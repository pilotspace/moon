//! Runtime-agnostic channel types replacing tokio::sync::{mpsc, oneshot, watch, Notify}.
//!
//! Uses `flume` for mpsc/watch/notify. Oneshot uses a lock-free AtomicU8 state machine.
#![allow(unused_imports, clippy::result_unit_err)]

use std::cell::UnsafeCell;
use std::future::Future;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use atomic_waker::AtomicWaker;

// --- mpsc ---
// flume provides unbounded + bounded async-capable mpsc channels that work on any runtime.
pub use flume::{
    bounded as mpsc_bounded, unbounded as mpsc_unbounded, Receiver as MpscReceiver,
    Sender as MpscSender,
};

// --- oneshot ---
// Lock-free oneshot using AtomicU8 state machine + UnsafeCell + AtomicWaker.
// Zero mutex contention -- eliminates 12% CPU overhead from flume's Mutex<VecDeque>.

const EMPTY: u8 = 0;
const VALUE: u8 = 1;
const CLOSED: u8 = 2;

struct OneshotInner<T> {
    state: AtomicU8,
    value: UnsafeCell<MaybeUninit<T>>,
    waker: AtomicWaker,
}

// SAFETY: The value is only written by sender (once) and read by receiver (once),
// with atomic state transitions providing the happens-before ordering.
// T: Send is required because value crosses thread boundaries.
unsafe impl<T: Send> Send for OneshotInner<T> {}
unsafe impl<T: Send> Sync for OneshotInner<T> {}

impl<T> Drop for OneshotInner<T> {
    fn drop(&mut self) {
        // If state is VALUE, the value was written but never read -- drop it.
        if *self.state.get_mut() == VALUE {
            unsafe {
                self.value.get_mut().assume_init_drop();
            }
        }
    }
}

pub fn oneshot<T>() -> (OneshotSender<T>, OneshotReceiver<T>) {
    let inner = Arc::new(OneshotInner {
        state: AtomicU8::new(EMPTY),
        value: UnsafeCell::new(MaybeUninit::uninit()),
        waker: AtomicWaker::new(),
    });
    (
        OneshotSender {
            inner: inner.clone(),
            sent: false,
        },
        OneshotReceiver {
            inner,
        },
    )
}

pub struct OneshotSender<T> {
    inner: Arc<OneshotInner<T>>,
    // Set to true when send() succeeds or handles CLOSED, to skip Drop's close logic.
    sent: bool,
}

impl<T> OneshotSender<T> {
    pub fn send(mut self, value: T) -> Result<(), T> {
        // Write value into the UnsafeCell.
        // SAFETY: Only the sender writes, and it does so exactly once (self is consumed).
        unsafe {
            (*self.inner.value.get()).write(value);
        }

        // Attempt state transition EMPTY -> VALUE.
        // Release ordering ensures the value write is visible to the receiver's Acquire load.
        match self.inner.state.compare_exchange(
            EMPTY,
            VALUE,
            Ordering::Release,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // Successfully stored value. Wake the receiver if it's waiting.
                self.inner.waker.wake();
                self.sent = true;
                Ok(())
            }
            Err(CLOSED) => {
                // Receiver was already dropped. Read back our value to return it.
                let value = unsafe { (*self.inner.value.get()).assume_init_read() };
                // Mark as EMPTY so OneshotInner::drop doesn't double-drop.
                self.inner.state.store(EMPTY, Ordering::Relaxed);
                self.sent = true;
                Err(value)
            }
            Err(_) => unreachable!("invalid oneshot state"),
        }
    }
}

impl<T> Drop for OneshotSender<T> {
    fn drop(&mut self) {
        if self.sent {
            return; // send() already handled the state transition.
        }
        // Sender dropped without sending. Transition EMPTY -> CLOSED.
        let prev = self.inner.state.swap(CLOSED, Ordering::Release);
        if prev == EMPTY {
            // Receiver may be waiting -- wake it so it sees CLOSED.
            self.inner.waker.wake();
        }
        // If prev == CLOSED, receiver already dropped -- nothing to do.
    }
}

pub struct OneshotReceiver<T> {
    inner: Arc<OneshotInner<T>>,
}

impl<T> OneshotReceiver<T> {
    pub async fn recv(self) -> Result<T, RecvError> {
        self.await
    }

    /// Blocking receive for use from non-async contexts (e.g., OS thread watchers).
    /// Spins with thread::yield_now until value arrives or sender is dropped.
    pub fn recv_blocking(self) -> Result<T, RecvError> {
        loop {
            let state = self.inner.state.load(Ordering::Acquire);
            match state {
                VALUE => {
                    let value = unsafe { (*self.inner.value.get()).assume_init_read() };
                    self.inner.state.store(CLOSED, Ordering::Relaxed);
                    // Prevent Drop from also closing
                    std::mem::forget(self);
                    return Ok(value);
                }
                CLOSED => {
                    std::mem::forget(self);
                    return Err(RecvError);
                }
                EMPTY => std::thread::yield_now(),
                _ => unreachable!(),
            }
        }
    }
}

impl<T> Future for OneshotReceiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Fast path: check state first.
        let state = self.inner.state.load(Ordering::Acquire);
        match state {
            VALUE => {
                // Value is ready. Read it out.
                // SAFETY: Sender wrote the value before the Release store of VALUE.
                // Our Acquire load ensures we see the write.
                let value = unsafe { (*self.inner.value.get()).assume_init_read() };
                // Transition to CLOSED so OneshotInner::drop doesn't double-drop.
                self.inner.state.store(CLOSED, Ordering::Relaxed);
                Poll::Ready(Ok(value))
            }
            CLOSED => Poll::Ready(Err(RecvError)),
            EMPTY => {
                // Register waker BEFORE re-checking state to avoid lost wakeups.
                self.inner.waker.register(cx.waker());

                // Re-check state after registering waker.
                // This handles the race where sender completes between our first
                // load and waker registration.
                let state = self.inner.state.load(Ordering::Acquire);
                match state {
                    VALUE => {
                        let value = unsafe { (*self.inner.value.get()).assume_init_read() };
                        self.inner.state.store(CLOSED, Ordering::Relaxed);
                        Poll::Ready(Ok(value))
                    }
                    CLOSED => Poll::Ready(Err(RecvError)),
                    EMPTY => Poll::Pending,
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }
}

impl<T> Drop for OneshotReceiver<T> {
    fn drop(&mut self) {
        // If EMPTY, transition to CLOSED so sender's send returns Err.
        // Use compare_exchange to avoid clobbering VALUE state.
        let _ = self.inner.state.compare_exchange(
            EMPTY,
            CLOSED,
            Ordering::Release,
            Ordering::Relaxed,
        );
        // If state was VALUE, the value remains in the inner and will be dropped
        // by OneshotInner::drop.
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
        WatchReceiver {
            shared,
            notify_rx,
        },
    )
}

#[derive(Clone)]
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
    use futures::executor::block_on;
    use futures::stream::FuturesUnordered;
    use futures::StreamExt;

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
        use std::sync::atomic::AtomicUsize;
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
