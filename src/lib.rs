//! The [`Pantry`] is useful for temporarily storing for later use values that
//! might "decay" (become unusable) over time.
//!
//! Create a [`Pantry`] value and use its [`store`] function to store values
//! for later use.  A key is provided along with the value, so that the value
//! can be retrieved later using the same key.  A worker thread is spawned
//! which monitors the values and automatically drops any which have "decayed".
//! Values must implement the [`Perishable`] trait, whose [`perished`] function
//! asynchronously completes once the value has decayed.
//!
//! Use the [`fetch`] asynchronous function on the [`Pantry`] with a key to
//! retrieve a value previously stored using that key.  A value is only
//! returned if it was stored using the same key and has not decayed since it
//! was stored.
//!
//! Multiple values may have the same key, with the caveat that the values
//! stored under a given key may not be returned in the same order in which
//! they were stored.
//!
//! [`fetch`]: struct.Pantry.html#method.fetch
//! [`Pantry`]: struct.Pantry.html
//! [`Perishable`]: trait.Perishable.html
//! [`perished`]: trait.Perishable.html#method.perished
//! [`store`]: struct.Pantry.html#method.store

#![warn(clippy::pedantic)]
// This warning falsely triggers when `future::select_all` is used.
#![allow(clippy::mut_mut)]
#![warn(missing_docs)]

use async_trait::async_trait;
use futures::{
    channel::{
        mpsc,
        oneshot,
    },
    executor,
    future,
    Future,
    FutureExt,
    StreamExt,
};
use std::{
    cell::RefCell,
    collections::{
        HashMap,
        hash_map,
    },
    fmt::Debug,
    hash::Hash,
    pin::Pin,
    rc::Rc,
    thread,
};

type Sender<V> = oneshot::Sender<Option<V>>;
type Requester<V> = oneshot::Sender<Sender<V> >;
type Requestee<V> = oneshot::Receiver<Sender<V> >;

/// This is the trait that values must implement in order to be stored in the
/// [`Pantry`].
///
/// [`Pantry`]: struct.Pantry.html
#[async_trait]
pub trait Perishable: Send + 'static {
    /// This asynchronous function should complete once the value has "decayed"
    /// or become unusable or unsuitable for reuse.  The worker thread of the
    /// [`Pantry`] runs this future to completion, automatically dropping the
    /// value if it completes.
    ///
    /// Note that this is an asynchronous trait method.  Currently, the
    /// [`async_trait`] attribute from the [`async-trait`] crate is used
    /// to realize this specification.
    ///
    /// [`Pantry`]: struct.Pantry.html
    /// [`async_trait`]: https://docs.rs/async-trait/0.1.41/async_trait/index.html
    /// [`async-trait`]: https://docs.rs/async-trait/0.1.41/async_trait/
    async fn perished(&mut self);
}

async fn monitor_value<V>(
    mut value: V,
    receiver: Requestee<V>,
) -> bool
    where V: Perishable
{
    println!("Now monitoring parked value");
    futures::select!(
        _ = value.perished().fuse() => (),
        sender = receiver.fuse() => {
            println!("Received request to recycle value");
            if let Ok(sender) = sender {
                println!("Sending value to requester");
                // A failure here means the value requester gave up
                // waiting for the value.  It shouldn't happen since the
                // requestee should respond quickly, but it's still possible if
                // the requester is dumb and requested a value only to
                // immediately give up.  If that happens, silently discard the
                // value, as if we gave it to them only for them to drop
                // it.
                sender.send(Some(value)).unwrap_or(());
            } else {
                println!("No path back to requester!");
            }
        },
    );
    false
}

struct ParkedValuePool<V> {
    requesters: Vec<Requester<V>>,
    monitors: Vec<Pin<Box<dyn Future<Output=bool>>>>,
}

impl<V> ParkedValuePool<V>
    where V: Perishable
{
    fn add(
        &mut self,
        value: V,
    ) {
        let (sender, receiver) = oneshot::channel();
        self.requesters.push(sender);
        self.monitors.push(monitor_value(value, receiver).boxed());
    }

    fn new() -> Self {
        Self {
            requesters: Vec::new(),
            monitors: Vec::new(),
        }
    }

    fn remove(&mut self) -> Option<Requester<V>> {
        self.requesters.pop()
    }
}

struct ParkedValuePools<K, V> {
    pools: HashMap<K, ParkedValuePool<V>>,
    kick: Option<oneshot::Sender<()>>,
}

impl<K, V> ParkedValuePools<K, V> {
    fn new() -> Self {
        Self {
            pools: HashMap::new(),
            kick: None,
        }
    }
}

fn take_parked_value<K, V>(
    pools: &Rc<RefCell<ParkedValuePools<K, V>>>,
    key: K,
    value: V,
)
    where K: Eq + Hash,
        V: Perishable
{
    pools.borrow_mut().pools.entry(key)
        .or_insert_with(ParkedValuePool::new)
        .add(value);
    if let Some(kick) = pools.borrow_mut().kick.take() {
        println!("Kicking monitors");
        // It shouldn't be possible for the kick to fail, since the kick
        // receiver isn't dropped until either it receives the kick or
        // the worker is stopped, and this function is only called
        // by the worker.  So if it does fail, we want to know about it
        // since it would mean we have a bug.
        kick.send(()).unwrap();
    } else {
        println!("Cannot kick monitors");
    }
}

async fn handle_messages<K, V>(
    pools: Rc<RefCell<ParkedValuePools<K, V>>>,
    receiver: mpsc::UnboundedReceiver<WorkerMessage<K, V>>
)
    where K: Debug + Eq + Hash,
        V: Perishable
{
    receiver
        .take_while(|message| future::ready(
            !matches!(message, WorkerMessage::Stop)
        ))
        .for_each(|message| async {
            match message {
                WorkerMessage::Take{
                    key,
                    value,
                } => {
                    println!("Placing value to {:?} in pool", key);
                    take_parked_value(&pools, key, value);
                },
                WorkerMessage::Give{
                    key,
                    return_sender,
                } => {
                    println!("Asked to recycle value to {:?}", key);
                    // It's possible for this to fail if the user gave up on a
                    // transaction before the worker was able to recycle a
                    // value for use in sending the request.  If this
                    // happens, the value is simply dropped.
                    return_sender.send(
                        give_parked_value(&pools, key).await
                    ).unwrap_or(());
                },
                WorkerMessage::Stop => unreachable!()
            }
        }).await
}

async fn monitor_values<K, V>(
    pools: Rc<RefCell<ParkedValuePools<K, V>>>,
)
    where K: Eq + Hash
{
    let mut monitors = Vec::new();
    let mut needs_kick = true;
    loop {
        monitors.extend(
            pools.borrow_mut().pools.iter_mut()
                .flat_map(|(_, pool)| {
                    pool.requesters.retain(|requester|
                        if requester.is_canceled() {
                            println!("Stream requester canceled");
                            false
                        } else {
                            println!("Stream requester not canceled");
                            true
                        }
                    );
                    pool.monitors.drain(..)
                })
        );
        pools.borrow_mut().pools.retain(|_, pool|
            if pool.requesters.is_empty() {
                println!("Channel pool empty");
                false
            } else {
                println!("Channel pool not empty");
                true
            }
        );
        if needs_kick {
            println!("Setting up to kick");
            let (sender, receiver) = oneshot::channel();
            pools.borrow_mut().kick = Some(sender);
            let kick_future = async {
                // It shouldn't be possible for this to fail, since once the
                // kick is set up, it isn't dropped until the kick is sent.
                // The enclosing method holds a reference to `pools`
                // which holds the kick.  So if it does fail, we want to know
                // about it since it would mean we have a bug.
                receiver.await.unwrap();
                true
            }.boxed();
            monitors.push(kick_future);
        } else {
            println!("Already set up to kick");
        }
        println!("Waiting on monitors ({})", monitors.len());
        let (kicked, _, monitors_left) = future::select_all(
            monitors.into_iter()
        ).await;
        needs_kick = if kicked {
            println!("Monitors added");
            true
        } else {
            println!("Monitor completed");
            false
        };
        monitors = monitors_left;
    }
}

async fn give_parked_value<K, V>(
    pools: &Rc<RefCell<ParkedValuePools<K, V>>>,
    key: K,
) -> Option<V>
    where K: Eq + Hash,
        V: Perishable
{
    // Remove connection requester from the pools.
    let requester = match pools.borrow_mut().pools.entry(key) {
        hash_map::Entry::Occupied(mut entry) => {
            println!("Found a connection requester");
            let requester = entry.get_mut().remove();
            requester
        },
        hash_map::Entry::Vacant(_) => {
            println!("No connection requesters found");
            None
        },
    };

    // Now that the connection requester is removed from the pools,
    // we can communicate with its monitor safely to try to recycle
    // the connection it's holding.  We share the pools with the monitor,
    // so it's not safe to hold a mutable reference when yielding.
    if let Some(requester) = requester {
        let (sender, receiver) = oneshot::channel();
        println!("Requesting connection from monitor");
        // It's possible for this to fail if the connection was broken and we
        // managed to take the connection requester from the pools before the
        // worker got around to it.
        if requester.send(sender).is_err() {
            println!("Connection was broken");
            None
        } else {
            println!("Waiting for connection");
            // It's possible for this to fail if the connection is broken
            // while we're waiting for the monitor to receive our request
            // for the connection.
            let connection = receiver
                .await // <-- Make sure we're not holding the pools here!
                .unwrap_or(None);
            if connection.is_some() {
                println!("Got a connection");
            } else {
                println!("Connection was broken");
            }
            connection
        }
    } else {
        None
    }
}

async fn worker<K, V>(
    receiver: mpsc::UnboundedReceiver<WorkerMessage<K, V>>
)
    where K: Debug + Eq + Hash,
        V: Perishable
{
    println!("Worker started");
    let pools = Rc::new(RefCell::new(ParkedValuePools::new()));
    futures::select!(
        () = handle_messages(pools.clone(), receiver).fuse() => (),
        () = monitor_values(pools).fuse() => (),
    );
    println!("Worker stopping");
}

enum WorkerMessage<K, V> {
    Stop,
    Take{
        key: K,
        value: V,
    },
    Give{
        key: K,
        return_sender: Sender<V>,
    },
}

/// Each value of this type maintains a collection of stored values that might
/// "decay" or become unusable over time.  Values are added to the collection
/// by calling [`store`] and providing the value along with a key that can be
/// used to retrieve the value later.  Values added to the collection are
/// monitored by a worker thread and dropped if the futures returned by their
/// [`perished`] functions complete.
///
/// As long as the [`perished`] future remains uncompleted for a value, the
/// value remains held in the collection and can be retrieved calling [`fetch`]
/// with the same key used to store the value in the first place.
///
/// [`perished`]: trait.Perishable.html#method.perished
/// [`fetch`]: #method.fetch
/// [`store`]: #method.store
pub struct Pantry<K, V> {
    work_in: mpsc::UnboundedSender<WorkerMessage<K, V>>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl<K, V> Pantry<K, V>
    where K: Debug + Eq + Hash + Send + 'static,
        V: Perishable
{
    /// Create a new `Pantry` with no values in it.  This spawns the worker
    /// thread which monitors values added to it and detects when they should
    /// be dropped.
    #[must_use]
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded::<WorkerMessage<K, V>>();
        Self {
            work_in: sender,
            worker: Some(thread::spawn(|| executor::block_on(worker(receiver)))),
        }
    }

    /// Transfer ownership of the given value to the `Pantry`, associating with
    /// it the given key, which can be used later with [`fetch`] to transfer
    /// ownership of the value back out.
    ///
    /// [`fetch`]: #method.fetch
    pub fn store(
        &self,
        key: K,
        value: V,
    ) {
        // It shouldn't be possible for this to fail, since the worker holds
        // the receiver for this channel, and isn't dropped until the client
        // itself is dropped.  So if it does fail, we want to know about it
        // since it would mean we have a bug.
        self.work_in.unbounded_send(WorkerMessage::Take{
            key,
            value
        }).unwrap();
    }

    /// Attempt to retrieve the value previously stored with [`store`] using
    /// the given key.  If no value was stored with that key, or the
    /// [`perished`] future for the value stored with that key has completed,
    /// `None` is returned.
    ///
    /// This function is asynchronous because ownership must be completely
    /// transfered from the worker thread of the `Pantry`.
    ///
    /// [`perished`]: trait.Perishable.html#method.perished
    /// [`store`]: #method.store
    pub async fn fetch(
        &self,
        key: K,
    ) -> Option<V> {
        let (sender, receiver) = oneshot::channel();
        println!("Trying to fetch value with {:?}", key);
        // It shouldn't be possible for this to fail, since the worker holds
        // the receiver for this channel, and isn't dropped until the client
        // itself is dropped.  So if it does fail, we want to know about it
        // since it would mean we have a bug.
        self.work_in.unbounded_send(WorkerMessage::Give{
            key,
            return_sender: sender
        }).unwrap();
        // This can fail if the connection is broken before the monitor
        // receives our request for the connection.
        let connection = receiver.await.unwrap_or(None);
        if connection.is_some() {
            println!("Got a value");
        } else {
            println!("No value available");
        }
        connection
    }
}

impl<K, V> Default for Pantry<K, V>
    where K: Debug + Eq + Hash + Send + 'static,
        V: Perishable
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Drop for Pantry<K, V> {
    fn drop(&mut self) {
        println!("Sending stop message");
        // It shouldn't be possible for this to fail, since the worker holds
        // the receiver for this channel, and we haven't joined or dropped the
        // worker yet (we will a few lines later).  So if it does fail, we want
        // to know about it since it would mean we have a bug.
        self.work_in.unbounded_send(WorkerMessage::Stop).unwrap();
        println!("Joining worker thread");
        // This shouldn't fail unless the worker panics.  If it does, there's
        // no reason why we shouldn't panic as well.
        self.worker.take().unwrap().join().unwrap();
        println!("Worker thread joined");
    }
}
