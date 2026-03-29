//! Runtime-agnostic channel types replacing tokio::sync::{mpsc, oneshot, watch, Notify}.
//!
//! Uses `flume` for mpsc, oneshot (bounded-1), watch, and notify.
#![allow(unused_imports, clippy::result_unit_err)]

use parking_lot::Mutex;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

// --- mpsc ---
// flume provides unbounded + bounded async-capable mpsc channels that work on any runtime.
pub use flume::{
    Receiver as MpscReceiver, Sender as MpscSender, bounded as mpsc_bounded,
    unbounded as mpsc_unbounded,
};

// --- oneshot ---
// Backed by flume bounded(1). Cross-thread wakeup works with monoio's !Send executor.

pub fn oneshot<T>() -> (OneshotSender<T>, OneshotReceiver<T>) {
    // Use flume bounded(1) instead of custom AtomicU8 state machine.
    // flume's cross-thread wakeup mechanism works with monoio's !Send executor
    // (proven: the Notify type uses flume bounded(1) and works cross-thread).
    // The custom oneshot's AtomicWaker doesn't reliably wake monoio tasks
    // from other threads, causing cross-shard write dispatch to hang.
    let (tx, rx) = flume::bounded(1);
    (
        OneshotSender { tx },
        OneshotReceiver {
            rx: Some(rx),
            recv_fut: None,
        },
    )
}

pub struct OneshotSender<T> {
    tx: flume::Sender<T>,
}

impl<T> OneshotSender<T> {
    pub fn send(self, value: T) -> Result<(), T> {
        self.tx.send(value).map_err(|e| e.into_inner())
    }
}

pub struct OneshotReceiver<T> {
    /// Receiver half — taken on first poll to avoid clone/race with try_recv.
    rx: Option<flume::Receiver<T>>,
    /// Cached recv future — persists across polls for correct waker registration.
    recv_fut: Option<Pin<Box<dyn Future<Output = Result<T, flume::RecvError>> + Send>>>,
}

impl<T: Send + 'static> OneshotReceiver<T> {
    pub async fn recv(self) -> Result<T, RecvError> {
        match self.rx {
            Some(rx) => rx.recv_async().await.map_err(|_| RecvError),
            None => Err(RecvError), // rx was taken by poll
        }
    }

    /// Alias for recv — flume's cross-thread wakeup works on all runtimes.
    pub async fn recv_polling(self) -> Result<T, RecvError> {
        self.recv().await
    }
}

impl<T: Send> OneshotReceiver<T> {
    /// Non-blocking try_recv for use with pending_wakers polling pattern.
    ///
    /// Returns `Ok(value)` if a value is available, `Err(TryRecvEmpty)` if not yet,
    /// or `Err(TryRecvDisconnected)` if the sender was dropped or rx was taken.
    pub fn try_recv(&self) -> Result<T, flume::TryRecvError> {
        match &self.rx {
            Some(rx) => rx.try_recv(),
            None => Err(flume::TryRecvError::Disconnected),
        }
    }

    /// Blocking receive for use from non-async contexts (e.g., OS thread watchers).
    pub fn recv_blocking(self) -> Result<T, RecvError> {
        match self.rx {
            Some(rx) => rx.recv().map_err(|_| RecvError),
            None => Err(RecvError),
        }
    }
}

impl<T: Send + 'static> Future for OneshotReceiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Lazily take the receiver on first poll — moved, not cloned.
        if self.recv_fut.is_none() {
            let rx = match self.rx.take() {
                Some(rx) => rx,
                None => return Poll::Ready(Err(RecvError)),
            };
            self.recv_fut = Some(Box::pin(rx.into_recv_async()));
        }
        match self.recv_fut.as_mut() {
            Some(fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(value)) => Poll::Ready(Ok(value)),
                Poll::Ready(Err(_)) => Poll::Ready(Err(RecvError)),
                Poll::Pending => Poll::Pending,
            },
            None => Poll::Ready(Err(RecvError)),
        }
    }
}

/// Error returned when the sender half of a oneshot channel is dropped without sending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "channel closed")
    }
}

impl std::error::Error for RecvError {}

// --- watch ---
// Simple watch channel using Arc<Mutex<T>> + flume for notification.
pub fn watch<T: Clone + Send + Sync + 'static>(initial: T) -> (WatchSender<T>, WatchReceiver<T>) {
    let shared = Arc::new(Mutex::new(initial));
    let (notify_tx, notify_rx) = flume::bounded::<()>(1);
    (
        WatchSender {
            shared: shared.clone(),
            notify_tx,
        },
        WatchReceiver { shared, notify_rx },
    )
}

#[derive(Clone)]
pub struct WatchSender<T> {
    shared: Arc<Mutex<T>>,
    notify_tx: flume::Sender<()>,
}
impl<T: Clone> WatchSender<T> {
    pub fn send(&self, value: T) -> Result<(), ()> {
        *self.shared.lock() = value;
        let _ = self.notify_tx.try_send(());
        Ok(())
    }
}

#[derive(Clone)]
pub struct WatchReceiver<T> {
    shared: Arc<Mutex<T>>,
    notify_rx: flume::Receiver<()>,
}
impl<T: Clone> WatchReceiver<T> {
    pub async fn changed(&mut self) -> Result<(), ()> {
        self.notify_rx.recv_async().await.map_err(|_| ())
    }
    pub fn borrow(&self) -> T {
        self.shared.lock().clone()
    }
}

// --- Notify ---
// Simple async notify using flume bounded(1) for signal coalescing.
pub struct Notify {
    tx: flume::Sender<()>,
    rx: flume::Receiver<()>,
}

impl Notify {
    pub fn new() -> Self {
        let (tx, rx) = flume::bounded(1);
        Self { tx, rx }
    }

    pub fn notify_one(&self) {
        let _ = self.tx.try_send(());
    }

    pub async fn notified(&self) {
        let _ = self.rx.recv_async().await;
    }
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use futures::executor::block_on;
    use futures::stream::FuturesUnordered;

    #[test]
    fn test_oneshot_send_then_recv() {
        let (tx, rx) = oneshot::<i32>();
        tx.send(42).unwrap();
        let val = block_on(rx.recv());
        assert_eq!(val.unwrap(), 42);
    }

    #[test]
    fn test_oneshot_sender_dropped_before_send() {
        let (tx, rx) = oneshot::<i32>();
        drop(tx);
        let result = block_on(rx.recv());
        assert_eq!(result, Err(RecvError));
    }

    #[test]
    fn test_oneshot_receiver_dropped_before_recv() {
        let (tx, rx) = oneshot::<i32>();
        drop(rx);
        let result = tx.send(42);
        assert_eq!(result, Err(42));
    }

    #[test]
    fn test_oneshot_future_ready_when_value_present() {
        let (tx, mut rx) = oneshot::<i32>();
        tx.send(99).unwrap();
        // Poll directly -- should be Ready immediately
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let pinned = Pin::new(&mut rx);
        match pinned.poll(&mut cx) {
            Poll::Ready(Ok(99)) => {}
            other => panic!("expected Ready(Ok(99)), got {:?}", other),
        }
    }

    #[test]
    fn test_oneshot_future_pending_when_empty() {
        let (_tx, mut rx) = oneshot::<i32>();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let pinned = Pin::new(&mut rx);
        match pinned.poll(&mut cx) {
            Poll::Pending => {}
            other => panic!("expected Pending, got {:?}", other),
        }
    }

    #[test]
    fn test_oneshot_concurrent_send_recv() {
        let (tx, rx) = oneshot::<i32>();
        let handle = std::thread::spawn(move || {
            tx.send(123).unwrap();
        });
        let val = block_on(rx.recv());
        assert_eq!(val.unwrap(), 123);
        handle.join().unwrap();
    }

    #[test]
    fn test_oneshot_in_futures_unordered() {
        let mut futs = FuturesUnordered::new();
        let mut senders = Vec::new();
        for i in 0..10 {
            let (tx, rx) = oneshot::<i32>();
            senders.push((i, tx));
            futs.push(rx);
        }
        // Send values
        for (i, tx) in senders {
            tx.send(i).unwrap();
        }
        // Collect all results
        let results: Vec<i32> = block_on(async {
            let mut results = Vec::new();
            while let Some(result) = futs.next().await {
                results.push(result.unwrap());
            }
            results
        });
        assert_eq!(results.len(), 10);
        let mut sorted = results.clone();
        sorted.sort();
        assert_eq!(sorted, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn test_oneshot_drop_value_when_not_received() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);
        DROP_COUNT.store(0, Ordering::SeqCst);

        #[derive(Debug)]
        struct DropCounter;
        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        let (tx, rx) = oneshot::<DropCounter>();
        tx.send(DropCounter).unwrap();
        drop(rx); // Drop receiver without reading
        // The value should be dropped exactly once (by OneshotInner::drop)
        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_mpsc_bounded() {
        let (tx, rx) = mpsc_bounded::<i32>(4);
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        assert_eq!(rx.recv().unwrap(), 1);
        assert_eq!(rx.recv().unwrap(), 2);
    }

    #[test]
    fn test_mpsc_unbounded() {
        let (tx, rx) = mpsc_unbounded::<i32>();
        tx.send(10).unwrap();
        tx.send(20).unwrap();
        assert_eq!(rx.recv().unwrap(), 10);
        assert_eq!(rx.recv().unwrap(), 20);
    }

    #[test]
    fn test_watch_initial_value() {
        let (_tx, rx) = watch(42);
        assert_eq!(rx.borrow(), 42);
    }

    #[test]
    fn test_watch_send_updates_value() {
        let (tx, rx) = watch(0);
        tx.send(99).unwrap();
        assert_eq!(rx.borrow(), 99);
    }

    #[test]
    fn test_notify_signal() {
        let notify = Notify::new();
        notify.notify_one();
        // Signal was sent, try_recv should succeed
        assert!(notify.rx.try_recv().is_ok());
    }

    #[test]
    fn test_notify_coalescing() {
        let notify = Notify::new();
        // Multiple notify_one calls coalesce (bounded(1))
        notify.notify_one();
        notify.notify_one(); // Should not block, just drops
        assert!(notify.rx.try_recv().is_ok());
        assert!(notify.rx.try_recv().is_err()); // Only one signal
    }
}
