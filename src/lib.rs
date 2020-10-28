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
    hash::Hash,
    pin::Pin,
    rc::Rc,
    thread,
};

type Sender<V> = oneshot::Sender<Option<V>>;
type Requester<V> = oneshot::Sender<Sender<V>>;
type Requestee<V> = oneshot::Receiver<Sender<V>>;

/// This is the trait that values must implement in order to be stored in the
/// [`Pantry`].  Note that since the trait has an asynchronous method,
/// currently the [`async_trait`] attribute from the [`async-trait`] crate must
/// be added to implementations.
///
/// # Examples
///
/// ```rust
/// # extern crate async_std;
/// # extern crate async_trait;
/// # extern crate pantry;
/// use async_trait::async_trait;
/// use pantry::Perishable;
///
/// struct SpyOrders(String);
///
/// #[async_trait]
/// impl Perishable for SpyOrders {
///     async fn perished(&mut self) {
///         // This message will self-destruct after
///         // sitting in the pantry for 5 seconds!
///         async_std::future::timeout(
///             std::time::Duration::from_secs(5),
///             futures::future::pending::<()>()
///         ).await.unwrap_or(())
///     }
/// }
/// ```
///
/// [`Pantry`]: struct.Pantry.html
/// [`async_trait`]: https://docs.rs/async-trait/0.1.41/async_trait/index.html
/// [`async-trait`]: https://docs.rs/async-trait/0.1.41/async_trait/
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
) -> MonitorKind
    where V: Perishable
{
    // The monitor completes if either of the following completes:
    futures::select!(
        // The value has perished.
        _ = value.perished().fuse() => (),

        // The value has been requested to be fetched back out,
        // or the requester for the value has been dropped.
        sender = receiver.fuse() => {
            if let Ok(sender) = sender {
                // A failure here means the value requester gave up
                // waiting for the value.  It shouldn't happen since the
                // requestee should respond quickly, but it's still possible if
                // the requester is dumb and requested a value only to
                // immediately give up.  If that happens, silently discard the
                // value, as if we gave it to them only for them to drop
                // it.
                sender.send(Some(value)).unwrap_or(());
            }
        },
    );

    // The output indicates that this isn't the "kick" future used to
    // wake the worker thread to collect more monitors.
    MonitorKind::Value
}

// This is the type of value returned by a completed monitor.  The
// `monitor_values` function uses this to tell the difference between value
// monitors and the special "kick" future it adds to ensure the task does not
// get stuck waiting when there are new monitors to add to its collection.
enum MonitorKind {
    Value,
    Kick,
}

struct ParkedValuePool<V> {
    requesters: Vec<Requester<V>>,
    monitors: Vec<Pin<Box<dyn Future<Output=MonitorKind>>>>,
}

impl<V> ParkedValuePool<V>
    where V: Perishable
{
    fn add(
        &mut self,
        value: V,
    ) {
        // Adding a value actually adds two different things:
        let (sender, receiver) = oneshot::channel();

        // First we store the oneshot sender as the "requester" we can
        // use later to retrieve the value from the monitor.
        self.requesters.push(sender);

        // Then we store a monitor, which is a future the worker thread
        // will try to drive to completion in order to fetch or drop
        // the value.  It holds the value as well as the receiver matching
        // the "requester", so that the value can be fetched back out of it.
        //
        // Note that the monitor will be taken out by the worker thread
        // the next time it loops.  The caller should "kick" the worker
        // thread if it's asleep.
        self.monitors.push(monitor_value(value, receiver).boxed());
    }

    fn new() -> Self {
        Self {
            requesters: Vec::new(),
            monitors: Vec::new(),
        }
    }

    fn remove(&mut self) -> Option<Requester<V>> {
        // To drop a value early, we need only drop the "requester" for it.
        // Doing so will wake the monitor since it "cancels" the oneshot.
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

async fn give_parked_value<K, V>(
    pools: &Rc<RefCell<ParkedValuePools<K, V>>>,
    key: K,
) -> Option<V>
    where K: Eq + Hash,
        V: Perishable
{
    // Remove requester from the pools.
    let requester = match pools.borrow_mut().pools.entry(key) {
        hash_map::Entry::Occupied(mut entry) => entry.get_mut().remove(),
        hash_map::Entry::Vacant(_) => None,
    };

    // Now that the requester is removed from the pools, we can communicate
    // with its monitor safely to try to fetch the value it's holding.  We
    // share the pools with the monitor, so it's not safe to hold a mutable
    // reference when yielding.
    if let Some(requester) = requester {
        let (sender, receiver) = oneshot::channel();
        // It's possible for this to fail if the value has perished and we
        // managed to take the  requester from the pools before the worker got
        // around to it.
        if requester.send(sender).is_err() {
            None
        } else {
            // It's possible for this to fail if the value has perished
            // while we're waiting for the monitor to receive our request
            // for the value.
            receiver
                .await // <-- Make sure we're not holding the pools here!
                .unwrap_or(None)
        }
    } else {
        None
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
    // First store the value and create the monitor for it.
    pools.borrow_mut().pools.entry(key)
        .or_insert_with(ParkedValuePool::new)
        .add(value);

    // If the worker thread is asleep, we will have a "kick" oneshot sender.
    // If so, take it and send to it to cause the worker thread to wake up
    // and collect the monitor to begin driving it to completion.
    if let Some(kick) = pools.borrow_mut().kick.take() {
        // It shouldn't be possible for the kick to fail, since the kick
        // receiver isn't dropped until either it receives the kick or
        // the worker is stopped, and this function is only called
        // by the worker.  So if it does fail, we want to know about it
        // since it would mean we have a bug.
        kick.send(()).unwrap();
    }
}

async fn handle_messages<K, V>(
    pools: Rc<RefCell<ParkedValuePools<K, V>>>,
    receiver: mpsc::UnboundedReceiver<WorkerMessage<K, V>>
)
    where K: Eq + Hash,
        V: Perishable
{
    // Drive to completion the stream of messages to the worker thread.
    receiver
        // The special `Stop` message completes the stream.
        .take_while(|message| future::ready(
            !matches!(message, WorkerMessage::Stop)
        ))

        // Handle messages other than `Stop`.
        .for_each(|message| async {
            match message {
                // We already handled `Stop` in the `take_while` above;
                // it causes the stream to end early so we won't get this far.
                WorkerMessage::Stop => unreachable!(),

                // Taking a value to store in the pantry is easy.
                WorkerMessage::Take{
                    key,
                    value,
                } => {
                    take_parked_value(&pools, key, value);
                },

                // Giving a value back out is more difficult.  The monitor
                // created for it when the value was stored owns the value.
                // Getting it back out requires that we signal the monitor to
                // pass back ownership.  Once we get it we deliver it back
                // through the oneshot sender provided with the `Give` message.
                // It's possible we have nothing to give back, so what we
                // send back is an `Option<V>` not a `V`.
                WorkerMessage::Give{
                    key,
                    return_sender,
                } => {
                    // It's possible for this to fail if the user gave up on a
                    // transaction before the worker was able to recycle a
                    // value for use in sending the request.  If this
                    // happens, the value is simply dropped.
                    return_sender.send(
                        give_parked_value(&pools, key).await
                    ).unwrap_or(());
                },
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
        // Add to our collection any monitors that have been created since the
        // last loop.  The first loop picks up any monitors created before the
        // worker thread actually started.
        monitors.extend(
            pools.borrow_mut().pools.iter_mut()
                .flat_map(|(_, pool)| {
                    pool.requesters.retain(|requester|
                        !requester.is_canceled()
                    );
                    pool.monitors.drain(..)
                })
        );

        // Drop any empty requester collections.
        pools.borrow_mut().pools.retain(|_, pool|
            !pool.requesters.is_empty()
        );

        // Add a special future we lovingly call the "kick", if we haven't
        // added it yet or if the last one completed.  This future completes if
        // a special "kick" oneshot is completed, which happens in
        // `take_parked_value` if the "kick" is set up by the time the worker
        // thread receives a new monitor to collect.
        //
        // If we didn't add this, the worker thread could get stuck if no
        // previously collected monitor completes (or if there were no previous
        // monitors collected) after monitors are added but not yet collected.
        if needs_kick {
            // Make a oneshot and store the sender as the "kick" which
            // `take_parked_value` will use to wake us up.
            let (sender, receiver) = oneshot::channel();
            pools.borrow_mut().kick = Some(sender);

            // Form the "kick" future which completes once the "kick" is made.
            let kick_future = async {
                // It shouldn't be possible for this to fail, since once the
                // kick is set up, it isn't dropped until the kick is sent.
                // The enclosing method holds a reference to `pools`
                // which holds the kick.  So if it does fail, we want to know
                // about it since it would mean we have a bug.
                receiver.await.unwrap();

                // The output indicates that this is the "kick" future used
                // to wake the worker thread to collect more monitors.
                MonitorKind::Kick
            }.boxed();

            // Add the "kick" future to our collection.
            monitors.push(kick_future);
        }

        // Wait until a monitor or the "kick" completes.
        let (monitor_kind, _, monitors_left) = future::select_all(
            monitors.into_iter()
        ).await;

        // If it was the "kick" future which completed, mark that we will
        // need to make a new "kick" for the next loop.
        needs_kick = matches!(monitor_kind, MonitorKind::Kick);

        // All incomplete futures go back to be collected next loop.
        monitors = monitors_left;
    }
}

async fn worker<K, V>(
    receiver: mpsc::UnboundedReceiver<WorkerMessage<K, V>>
)
    where K: Eq + Hash,
        V: Perishable
{
    // The worker thread makes the root storage for the monitors, which hold
    // stored values, and requesters, which are used to retrieve them.
    // These are shared between separate futures:
    let pools = Rc::new(RefCell::new(ParkedValuePools::new()));
    futures::select!(
        // Handle incoming messages to the worker thread, such as to stop,
        // store a new value, or try to retrieve back a value previously
        // stored.
        () = handle_messages(pools.clone(), receiver).fuse() => (),

        // Drive monitor futures to completion, which either deliver the values
        // back out or drop them if they have perished.
        () = monitor_values(pools).fuse() => (),
    );
}

enum WorkerMessage<K, V> {
    // This tells the worker thread to stop.
    Stop,

    // This takes a value and stores it in the pantry.
    Take {
        key: K,
        value: V,
    },

    // This requests that a previously-stored value be retrieved
    // and delivered back through the provided oneshot sender.
    Give {
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
/// value remains held in the collection and can be retrieved by calling
/// [`fetch`] with the same key used to store the value in the first place.
///
/// # Examples
///
/// ```rust
/// # extern crate async_std;
/// # extern crate async_trait;
/// # extern crate pantry;
/// use async_trait::async_trait;
/// use futures::executor::block_on;
/// use pantry::{Pantry, Perishable};
/// use std::time::Duration;
///
/// async fn delay_async(duration: Duration) {
///     async_std::future::timeout(
///         duration,
///         futures::future::pending::<()>()
///     ).await.unwrap_or(())
/// }
///
/// fn delay(duration: Duration) {
///     block_on(delay_async(duration));
/// }
///
/// struct SpyOrders(&'static str);
///
/// #[async_trait]
/// impl Perishable for SpyOrders {
///     async fn perished(&mut self) {
///         // This message will self-destruct after
///         // sitting in the pantry for 150 milliseconds!
///         delay_async(Duration::from_millis(150)).await
///     }
/// }
///
/// fn main() {
///     let pantry = Pantry::new();
///     let for_james = SpyOrders("Steal Dr. Evil's cat");
///     let for_jason = SpyOrders("Save the Queen");
///     let key = "spies";
///     pantry.store(key, for_james);
///     delay(Duration::from_millis(100));
///     pantry.store(key, for_jason);
///     delay(Duration::from_millis(100));
///     let value1 = block_on(async { pantry.fetch(key).await });
///     let value2 = block_on(async { pantry.fetch(key).await });
///     assert!(value1.is_some());
///     assert_eq!("Save the Queen", value1.unwrap().0);
///     assert!(value2.is_none());
/// }
/// ```
///
/// [`perished`]: trait.Perishable.html#method.perished
/// [`fetch`]: #method.fetch
/// [`store`]: #method.store
pub struct Pantry<K, V> {
    // This sender is used to deliver messages to the worker thread.
    work_in: mpsc::UnboundedSender<WorkerMessage<K, V>>,

    // This is our handle to join the worker thread when dropped.
    worker: Option<std::thread::JoinHandle<()>>,
}

impl<K, V> Pantry<K, V>
    where K: Eq + Hash + Send + 'static,
        V: Perishable
{
    /// Create a new `Pantry` with no values in it.  This spawns the worker
    /// thread which monitors values added to it and detects when they should
    /// be dropped.
    #[must_use]
    pub fn new() -> Self {
        // Make the channel used to communicate with the worker thread.
        let (sender, receiver) = mpsc::unbounded();

        // Store the sender end of the channel and spawn the worker thread,
        // giving it the receiver end.
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
        // Tell the worker thread to take the value and associate the key with
        // it.
        //
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

        // Tell the worker thread to give us a value matching the key.
        //
        // It shouldn't be possible for this to fail, since the worker holds
        // the receiver for this channel, and isn't dropped until the client
        // itself is dropped.  So if it does fail, we want to know about it
        // since it would mean we have a bug.
        self.work_in.unbounded_send(WorkerMessage::Give{
            key,
            return_sender: sender
        }).unwrap();

        // Wait for the worker thread to either give us the value back or tell
        // us it didn't have one to give us.
        //
        // This can fail if the value has perished before the monitor
        // receives our request for the value.
        receiver.await.unwrap_or(None)
    }
}

impl<K, V> Default for Pantry<K, V>
    where K: Eq + Hash + Send + 'static,
        V: Perishable
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Drop for Pantry<K, V> {
    fn drop(&mut self) {
        // Tell the worker thread to stop.
        //
        // It shouldn't be possible for this to fail, since the worker holds
        // the receiver for this channel, and we haven't joined or dropped the
        // worker yet (we will a few lines later).  So if it does fail, we want
        // to know about it since it would mean we have a bug.
        self.work_in.unbounded_send(WorkerMessage::Stop).unwrap();

        // Join the worker thread.
        //
        // This shouldn't fail unless the worker panics.  If it does, there's
        // no reason why we shouldn't panic as well.
        self.worker.take().unwrap().join().unwrap();
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    struct MockPerishable{
        num: usize,
        perish: Option<oneshot::Receiver<()>>,
        dropped: Option<oneshot::Sender<()>>,
    }

    impl MockPerishable {
        fn perishable(num: usize) -> (
            Self,
            oneshot::Sender<()>,
            oneshot::Receiver<()>,
        ) {
            let (perish_sender, perish_receiver) = oneshot::channel();
            let (dropped_sender, dropped_receiver) = oneshot::channel();
            let value = Self {
                num,
                perish: Some(perish_receiver),
                dropped: Some(dropped_sender),
            };
            (value, perish_sender, dropped_receiver)
        }
        fn not_perishable(num: usize) -> Self {
            Self {
                num,
                perish: None,
                dropped: None,
            }
        }
    }

    impl Drop for MockPerishable {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.take() {
                dropped.send(()).unwrap_or(());
            }
        }
    }

    #[async_trait]
    impl Perishable for MockPerishable {
        async fn perished(&mut self) {
            if let Some(perish) = self.perish.take() {
                perish.await.unwrap_or(());
            } else {
                futures::future::pending().await
            }
        }
    }

    #[test]
    fn store_then_fetch() {
        let pantry = Pantry::new();
        let value = MockPerishable::not_perishable(1337);
        let key = 42;
        pantry.store(key, value);
        let value = futures::executor::block_on(async {
            pantry.fetch(key).await
        });
        assert!(value.is_some());
        assert_eq!(1337, value.unwrap().num);
    }

    #[test]
    fn fetch_without_store() {
        let pantry: Pantry<usize, MockPerishable> = Pantry::new();
        let key = 42;
        let value = futures::executor::block_on(async {
            pantry.fetch(key).await
        });
        assert!(value.is_none());
    }

    #[test]
    fn store_then_double_fetch() {
        let pantry = Pantry::new();
        let value = MockPerishable::not_perishable(1337);
        let key = 42;
        pantry.store(key, value);
        let value = futures::executor::block_on(async {
            pantry.fetch(key).await
        });
        assert!(value.is_some());
        assert_eq!(1337, value.unwrap().num);
        let value = futures::executor::block_on(async {
            pantry.fetch(key).await
        });
        assert!(value.is_none());
    }

    #[test]
    fn double_store_then_double_fetch_same_key() {
        let pantry = Pantry::new();
        let value1 = MockPerishable::not_perishable(1337);
        let value2 = MockPerishable::not_perishable(85);
        let key = 42;
        pantry.store(key, value1);
        pantry.store(key, value2);
        let value1 = futures::executor::block_on(async {
            pantry.fetch(key).await
        });
        let value2 = futures::executor::block_on(async {
            pantry.fetch(key).await
        });
        assert!(value1.is_some());
        assert!(value2.is_some());
        assert!(matches!(
            (value1.unwrap().num, value2.unwrap().num),
            (1337, 85)
            | (85, 1337)
        ));
    }

    #[test]
    fn double_store_then_double_fetch_different_keys() {
        let pantry = Pantry::new();
        let value1 = MockPerishable::not_perishable(1337);
        let value2 = MockPerishable::not_perishable(85);
        let key1 = 42;
        let key2 = 33;
        pantry.store(key1, value1);
        pantry.store(key2, value2);
        let value1 = futures::executor::block_on(async {
            pantry.fetch(key1).await
        });
        let value2 = futures::executor::block_on(async {
            pantry.fetch(key2).await
        });
        assert!(value1.is_some());
        assert!(value2.is_some());
        assert!(matches!(
            (value1.unwrap().num, value2.unwrap().num),
            (1337, 85)
        ));
    }

    #[test]
    fn store_then_perish_then_fetch() {
        let pantry = Pantry::new();
        let (value, perish, dropped) = MockPerishable::perishable(1337);
        let key = 42;
        pantry.store(key, value);
        assert!(perish.send(()).is_ok());
        assert!(futures::executor::block_on(async {
            dropped.await
        }).is_ok());
        let value = futures::executor::block_on(async {
            pantry.fetch(key).await
        });
        assert!(value.is_none());
    }

    #[test]
    fn store_perishible_then_fetch_without_perish() {
        let pantry = Pantry::new();
        let (value, perish, dropped) = MockPerishable::perishable(1337);
        let key = 42;
        pantry.store(key, value);
        let value = futures::executor::block_on(async {
            pantry.fetch(key).await
        });
        assert!(dropped.now_or_never().is_none());
        drop(perish);
        assert!(value.is_some());
        assert_eq!(1337, value.unwrap().num);
    }

    #[test]
    fn double_store_then_one_perishes_then_double_fetch_same_key() {
        let pantry = Pantry::new();
        let (value1, perish, dropped) = MockPerishable::perishable(1337);
        let value2 = MockPerishable::not_perishable(85);
        let key = 42;
        pantry.store(key, value1);
        pantry.store(key, value2);
        assert!(perish.send(()).is_ok());
        assert!(futures::executor::block_on(async {
            dropped.await
        }).is_ok());
        let value1 = futures::executor::block_on(async {
            pantry.fetch(key).await
        });
        let value2 = futures::executor::block_on(async {
            pantry.fetch(key).await
        });
        assert!(value1.is_some());
        assert!(value2.is_none());
        assert_eq!(85, value1.unwrap().num);
    }

}
