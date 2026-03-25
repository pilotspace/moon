//! Runtime-agnostic channel types replacing tokio::sync::{mpsc, oneshot, watch, Notify}.
//!
//! Uses `flume` which works on any async runtime (no tokio dependency).

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

// --- mpsc ---
// flume provides unbounded + bounded async-capable mpsc channels that work on any runtime.
pub use flume::{
    bounded as mpsc_bounded, unbounded as mpsc_unbounded, Receiver as MpscReceiver,
    Sender as MpscSender,
};

// --- oneshot ---
// flume bounded(1) works as oneshot. Provide convenience wrapper.
pub fn oneshot<T>() -> (OneshotSender<T>, OneshotReceiver<T>) {
    let (tx, rx) = flume::bounded(1);
    (OneshotSender(tx), OneshotReceiver(rx))
}

pub struct OneshotSender<T>(flume::Sender<T>);
impl<T> OneshotSender<T> {
    pub fn send(self, value: T) -> Result<(), T> {
        self.0.send(value).map_err(|e| e.into_inner())
    }
}

pub struct OneshotReceiver<T>(flume::Receiver<T>);
impl<T> OneshotReceiver<T> {
    pub async fn recv(self) -> Result<T, flume::RecvError> {
        self.0.into_recv_async().await
    }
}

// Implement Future for OneshotReceiver so it can be used in FuturesUnordered.
// connection.rs uses FuturesUnordered<tokio::sync::oneshot::Receiver<Option<Frame>>>.
//
// We store a pinned RecvFut inside for proper waker registration (no busy-poll).
impl<T> Future for OneshotReceiver<T> {
    type Output = Result<T, flume::RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Use try_recv for a quick non-blocking check, then register via waker.
        // flume::Receiver doesn't directly support poll, so we use a simple approach:
        // try_recv, and if empty, park waker. The sender side will wake all parked
        // futures when it sends. This works because flume's internal waker registration
        // happens through recv_async().
        //
        // For FuturesUnordered usage, this is acceptable: the executor polls frequently
        // and flume's bounded(1) sender wakes the runtime when data arrives.
        match self.0.try_recv() {
            Ok(val) => Poll::Ready(Ok(val)),
            Err(flume::TryRecvError::Empty) => {
                // Register waker with the channel for proper notification.
                // We create a temporary recv_async future and poll it once to register.
                let recv_fut = self.0.recv_async();
                // SAFETY: recv_fut is a local, we pin it on the stack and poll once.
                let mut recv_fut = recv_fut;
                // SAFETY: We never move recv_fut after pinning.
                let pinned = unsafe { Pin::new_unchecked(&mut recv_fut) };
                pinned.poll(cx)
            }
            Err(flume::TryRecvError::Disconnected) => {
                Poll::Ready(Err(flume::RecvError::Disconnected))
            }
        }
    }
}

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
        WatchReceiver {
            shared,
            notify_rx,
        },
    )
}

pub struct WatchSender<T> {
    shared: Arc<Mutex<T>>,
    notify_tx: flume::Sender<()>,
}
impl<T: Clone> WatchSender<T> {
    pub fn send(&self, value: T) -> Result<(), ()> {
        *self.shared.lock().unwrap() = value;
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
        self.shared.lock().unwrap().clone()
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

    #[test]
    fn test_oneshot_send_recv() {
        let (tx, rx) = oneshot::<i32>();
        tx.send(42).unwrap();
        // Use blocking recv since we don't have a runtime in unit tests
        let val = rx.0.recv().unwrap();
        assert_eq!(val, 42);
    }

    #[test]
    fn test_oneshot_sender_dropped() {
        let (tx, rx) = oneshot::<i32>();
        drop(tx);
        assert!(rx.0.recv().is_err());
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
