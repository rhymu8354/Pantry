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
#![warn(missing_docs)]

use async_trait::async_trait;
use futures::{
    channel::{
        mpsc,
        oneshot::{
            self,
            Receiver,
            Sender,
        },
    },
    executor,
    future,
    future::LocalBoxFuture,
    FutureExt as _,
    StreamExt as _,
};
use std::{
    collections::{
        hash_map,
        HashMap,
    },
    hash::Hash,
    thread,
};

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
///             futures::future::pending::<()>(),
///         )
///         .await
///         .unwrap_or(())
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

// These are the types of message that may be sent to the worker thread
// of the pantry.
enum WorkerMessage<K, V> {
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

// This is the type of value returned by a completed monitor, indicating
// what (if any) work should be done as a consequence.
enum MonitorKind<K, V> {
    // This means the worker was sent a message.  The message receiver
    // is also provided back so that it can be used to await the next
    // message.
    Message {
        message: WorkerMessage<K, V>,
        receiver: mpsc::UnboundedReceiver<WorkerMessage<K, V>>,
    },

    // This means the message sender was closed, indicating the worker
    // thread should be joined.
    Stop,

    // This means a value was dropped or removed from the pantry.
    // No further work is required.
    Value,
}

// These types are used for the channels used to pass a value requester
// into a value monitor, so that the monitor can transfer the value back
// out to the requester.
type Requester<V> = Sender<Sender<V>>;
type Requestee<V> = Receiver<Sender<V>>;

// This is used to create a monitor which holds onto a value and watches
// to see if it perishes.  The monitor can also receive a request to pass
// the value back out before it perishes.
async fn monitor_value<K, V>(
    mut value: V,
    requestee: Requestee<V>,
) -> MonitorKind<K, V>
where
    V: Perishable,
{
    // The monitor completes if either of the following completes:
    #![allow(clippy::mut_mut)]
    futures::select!(
        // The value has perished.
        _ = value.perished().fuse() => {},

        // The value has been requested to be fetched back out,
        // or the requester for the value has been dropped.
        return_sender = requestee.fuse() => {
            // We should get a return sender, because we shouldn't drop the
            // requester before sending a return sender.
            //
            // A failure to send the value means the value requester gave up
            // waiting for the value.
            let _ = return_sender
                .expect("requester dropped before sending return sender")
                .send(value);
        },
    );

    // The output indicates that this future dealt with a value stored
    // in the pantry (by either dropping it or sending it back out).
    MonitorKind::Value
}

struct ParkedValuePool<K, V> {
    requesters: Vec<Requester<V>>,
    monitors: Vec<LocalBoxFuture<'static, MonitorKind<K, V>>>,
}

impl<K, V> ParkedValuePool<K, V>
where
    K: 'static,
    V: Perishable,
{
    fn add(
        &mut self,
        value: V,
    ) {
        // Adding a value actually adds two different things: a requester and a
        // monitor.
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
        // the next time it loops.
        self.monitors.push(monitor_value(value, receiver).boxed_local());
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

async fn await_next_message<K, V>(
    receiver: mpsc::UnboundedReceiver<WorkerMessage<K, V>>
) -> MonitorKind<K, V> {
    let (message, receiver) = receiver.into_future().await;
    message.map_or(MonitorKind::Stop, |message| MonitorKind::Message {
        message,
        receiver,
    })
}

async fn worker<K, V>(receiver: mpsc::UnboundedReceiver<WorkerMessage<K, V>>)
where
    K: Eq + Hash + 'static,
    V: Perishable,
{
    let mut pools: HashMap<K, ParkedValuePool<K, V>> = HashMap::new();
    let mut monitors = Vec::new();
    let mut receiver = Some(receiver);
    loop {
        // Add to our collection any monitors that have been created since the
        // last loop.  The first loop picks up any monitors created before the
        // worker thread actually started.
        monitors.extend(pools.iter_mut().flat_map(|(_, pool)| {
            pool.requesters.retain(|requester| !requester.is_canceled());
            pool.monitors.drain(..)
        }));

        // Add a special monitor to receive the next worker message,
        // if the receiver is idle.
        if let Some(receiver) = receiver.take() {
            monitors.push(await_next_message(receiver).boxed_local());
        }

        // Wait until a monitor completes.  If it indicates a message
        // received, handle the message.
        let (monitor_kind, _, monitors_left) =
            future::select_all(monitors.into_iter()).await;
        monitors = monitors_left;
        match monitor_kind {
            MonitorKind::Message {
                message,
                receiver: receiver_back,
            } => {
                receiver = Some(receiver_back);
                match message {
                    // Taking a value to store in the pantry is easy.
                    WorkerMessage::Take {
                        key,
                        value,
                    } => {
                        pools
                            .entry(key)
                            .or_insert_with(ParkedValuePool::new)
                            .add(value);
                    },

                    // Giving a value back out is more difficult.  The monitor
                    // created for it when the value was stored owns the value.
                    // Getting it back out requires that we signal the monitor
                    // to pass back ownership.  Once we get it we deliver it
                    // back through the oneshot sender provided with the `Give`
                    // message.  It's possible we have nothing to give back, so
                    // what we send back is an `Option<V>` not a `V`.
                    WorkerMessage::Give {
                        key,
                        return_sender,
                    } => {
                        // Attempt to remove a requester from the pools.  If we
                        // get a requestor, send the return sender through it
                        // for the monitor to use for returning the value
                        // back out of the pantry.
                        if let hash_map::Entry::Occupied(mut entry) =
                            pools.entry(key)
                        {
                            let pool = entry.get_mut();
                            if let Some(requester) = pool.remove() {
                                // It's possible for this to fail if the
                                // value perished during this loop.
                                let _ = requester.send(return_sender);
                            };
                            if pool.requesters.is_empty() {
                                entry.remove();
                            }
                        };
                    },
                }
            },
            MonitorKind::Stop => break,
            MonitorKind::Value => (),
        }
    }
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
/// use pantry::{
///     Pantry,
///     Perishable,
/// };
/// use std::time::Duration;
///
/// async fn delay_async(duration: Duration) {
///     async_std::future::timeout(duration, futures::future::pending::<()>())
///         .await
///         .unwrap_or(())
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
where
    K: Eq + Hash + Send + 'static,
    V: Perishable,
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
            worker: Some(thread::spawn(|| {
                executor::block_on(worker(receiver))
            })),
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
        self.work_in
            .unbounded_send(WorkerMessage::Take {
                key,
                value,
            })
            .expect("worker messager receiver dropped unexpectedly");
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
        self.work_in
            .unbounded_send(WorkerMessage::Give {
                key,
                return_sender: sender,
            })
            .expect("worker messager receiver dropped unexpectedly");

        // Wait for the worker thread to either give us the value back or tell
        // us (via error) that it didn't have one to give us.
        receiver.await.ok()
    }
}

impl<K, V> Default for Pantry<K, V>
where
    K: Eq + Hash + Send + 'static,
    V: Perishable,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Drop for Pantry<K, V> {
    fn drop(&mut self) {
        // Closing the worker message sender should cause the worker thread to
        // complete.
        self.work_in.close_channel();

        // Join the worker thread.
        //
        // This shouldn't fail unless the worker panics or we dropped ths
        // thread join handle.
        self.worker
            .take()
            .expect("worker thread join handle dropped unexpectedly")
            .join()
            .expect("worker thread could not be joined");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockPerishable {
        num: usize,
        perish: Option<Receiver<()>>,
        dropped: Option<Sender<()>>,
    }

    impl MockPerishable {
        fn perishable(num: usize) -> (Self, Sender<()>, Receiver<()>) {
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
        let value =
            futures::executor::block_on(async { pantry.fetch(key).await });
        assert!(value.is_some());
        assert_eq!(1337, value.unwrap().num);
    }

    #[test]
    fn fetch_without_store() {
        let pantry: Pantry<usize, MockPerishable> = Pantry::new();
        let key = 42;
        let value =
            futures::executor::block_on(async { pantry.fetch(key).await });
        assert!(value.is_none());
    }

    #[test]
    fn store_then_double_fetch() {
        let pantry = Pantry::new();
        let value = MockPerishable::not_perishable(1337);
        let key = 42;
        pantry.store(key, value);
        let value =
            futures::executor::block_on(async { pantry.fetch(key).await });
        assert!(value.is_some());
        assert_eq!(1337, value.unwrap().num);
        let value =
            futures::executor::block_on(async { pantry.fetch(key).await });
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
        let value1 =
            futures::executor::block_on(async { pantry.fetch(key).await });
        let value2 =
            futures::executor::block_on(async { pantry.fetch(key).await });
        assert!(value1.is_some());
        assert!(value2.is_some());
        assert!(matches!(
            (value1.unwrap().num, value2.unwrap().num),
            (1337, 85) | (85, 1337)
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
        let value1 =
            futures::executor::block_on(async { pantry.fetch(key1).await });
        let value2 =
            futures::executor::block_on(async { pantry.fetch(key2).await });
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
        assert!(futures::executor::block_on(async { dropped.await }).is_ok());
        let value =
            futures::executor::block_on(async { pantry.fetch(key).await });
        assert!(value.is_none());
    }

    #[test]
    fn store_perishible_then_fetch_without_perish() {
        let pantry = Pantry::new();
        let (value, perish, dropped) = MockPerishable::perishable(1337);
        let key = 42;
        pantry.store(key, value);
        let value =
            futures::executor::block_on(async { pantry.fetch(key).await });
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
        assert!(futures::executor::block_on(async { dropped.await }).is_ok());
        let value1 =
            futures::executor::block_on(async { pantry.fetch(key).await });
        let value2 =
            futures::executor::block_on(async { pantry.fetch(key).await });
        assert!(value1.is_some());
        assert!(value2.is_none());
        assert_eq!(85, value1.unwrap().num);
    }
}
