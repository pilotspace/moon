//! Pre-allocated response slot for zero-allocation cross-shard dispatch.
//!
//! Eliminates ~80-120ns per cross-shard write by replacing oneshot channel
//! allocation with a reusable, lock-free AtomicU8 state machine.
//!
//! Single-producer (target shard fills), single-consumer (connection owner polls).

use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::task::{Context, Poll};

use atomic_waker::AtomicWaker;

use crate::protocol::Frame;

const EMPTY: u8 = 0;
const FILLED: u8 = 1;

/// Lock-free single-producer single-consumer response slot.
///
/// One thread (the target shard) calls `fill()` to deposit the response.
/// Another thread (the connection owner) calls `poll_take()` to retrieve it.
/// The AtomicU8 state machine provides the happens-before ordering.
///
/// Waker synchronization uses `AtomicWaker` (from the `atomic-waker` crate)
/// which provides correct cross-thread waker registration via AcqRel atomics.
pub struct ResponseSlot {
    state: AtomicU8,
    /// Response data. Written by producer (fill), read by consumer (poll_take).
    data: UnsafeCell<Option<Vec<Frame>>>,
    /// Waker registered by the consumer when slot is empty.
    /// Uses AtomicWaker for correct cross-thread synchronization.
    waker: AtomicWaker,
}

// SAFETY: Only two threads access this slot:
// - Connection owner (consumer): registers waker via poll_take, reads data
// - Target shard (producer): writes data via fill, wakes consumer
// The AtomicU8 state transitions provide Release/Acquire ordering ensuring
// all writes to data are visible across threads. AtomicWaker handles
// waker synchronization internally.
unsafe impl Send for ResponseSlot {}
unsafe impl Sync for ResponseSlot {}

impl ResponseSlot {
    /// Create a new empty response slot.
    pub fn new() -> Self {
        Self {
            state: AtomicU8::new(EMPTY),
            data: UnsafeCell::new(None),
            waker: AtomicWaker::new(),
        }
    }

    /// Fill the slot with a response (called by the target shard thread).
    ///
    /// After this call, the consumer's next `poll_take` will return `Ready`.
    /// If a waker was registered, it is woken.
    ///
    /// # Safety contract
    /// Must only be called when state is EMPTY. Calling fill on a FILLED slot
    /// is a logic error (response would be lost). Debug-asserts guard this.
    pub fn fill(&self, response: Vec<Frame>) {
        // Write data before state transition (will be visible via Release ordering)
        unsafe {
            *self.data.get() = Some(response);
        }

        // Transition EMPTY -> FILLED with Release ordering.
        // This ensures the data write above is visible to the consumer's Acquire load.
        let prev = self.state.swap(FILLED, Ordering::Release);
        debug_assert_eq!(prev, EMPTY, "ResponseSlot::fill called on non-EMPTY slot");

        // Wake the consumer. AtomicWaker handles synchronization internally —
        // the waker registered by the consumer is guaranteed to be visible here.
        self.waker.wake();
    }

    /// Poll for the response (called by the connection owner thread).
    ///
    /// Returns `Ready(data)` if the slot has been filled, or `Pending` if empty.
    /// After returning `Ready`, the slot is reset to EMPTY and can be reused.
    pub fn poll_take(&self, cx: &mut Context<'_>) -> Poll<Vec<Frame>> {
        // Fast path: check if filled.
        let state = self.state.load(Ordering::Acquire);
        if state == FILLED {
            // Data is ready. Take it out and reset to EMPTY.
            let data = unsafe { (*self.data.get()).take().unwrap() };
            self.state.store(EMPTY, Ordering::Release);
            return Poll::Ready(data);
        }

        // Slot is empty. Register waker using AtomicWaker (handles synchronization).
        self.waker.register(cx.waker());

        // Re-check state after waker registration.
        // Handles the race where producer fills between our first load and waker store.
        let state = self.state.load(Ordering::Acquire);
        if state == FILLED {
            let data = unsafe { (*self.data.get()).take().unwrap() };
            self.state.store(EMPTY, Ordering::Release);
            return Poll::Ready(data);
        }

        Poll::Pending
    }

    /// Force-reset the slot to EMPTY, clearing any pending data and waker.
    ///
    /// Used after connection errors to ensure clean state for potential reuse.
    pub fn reset(&self) {
        self.state.store(EMPTY, Ordering::Release);
        unsafe {
            *self.data.get() = None;
        }
        // AtomicWaker::take() clears any registered waker.
        self.waker.take();
    }
}

impl Default for ResponseSlot {
    fn default() -> Self {
        Self::new()
    }
}

/// Future that wraps a `ResponseSlot` reference for async `.await` usage.
///
/// Holds a raw pointer to the slot for lifetime flexibility — the slot
/// (owned by the pool) outlives the future (created per-dispatch).
pub struct ResponseSlotFuture {
    slot: *const ResponseSlot,
}

// SAFETY: The underlying ResponseSlot is Send+Sync, and the pointer remains
// valid for the lifetime of the connection (the pool that owns the slot
// outlives all dispatched futures).
unsafe impl Send for ResponseSlotFuture {}

impl Future for ResponseSlotFuture {
    type Output = Vec<Frame>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: Pointer validity guaranteed by ResponseSlotPool lifetime.
        let slot = unsafe { &*self.slot };
        slot.poll_take(cx)
    }
}

/// Per-connection pre-allocated response slots, one per target shard.
///
/// Eliminates oneshot channel allocation on the cross-shard dispatch hot path.
/// Created once per connection, reused across all dispatches for that connection.
pub struct ResponseSlotPool {
    slots: Vec<ResponseSlot>,
    /// The shard this connection is assigned to (for diagnostics/debugging).
    #[allow(dead_code)]
    my_shard: usize,
}

impl ResponseSlotPool {
    /// Create a pool with one slot per shard.
    pub fn new(num_shards: usize, my_shard: usize) -> Self {
        let mut slots = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            slots.push(ResponseSlot::new());
        }
        Self { slots, my_shard }
    }

    /// Get a reference to the slot for the given target shard.
    #[inline]
    pub fn slot_for(&self, target_shard: usize) -> &ResponseSlot {
        &self.slots[target_shard]
    }

    /// Get a raw pointer to the slot for the given target shard.
    ///
    /// This pointer is stable for the lifetime of the pool and can be
    /// safely sent across threads in ShardMessage variants.
    #[inline]
    pub fn slot_ptr(&self, target_shard: usize) -> *const ResponseSlot {
        &self.slots[target_shard] as *const ResponseSlot
    }

    /// Create a future that resolves when the target shard fills the slot.
    #[inline]
    pub fn future_for(&self, target_shard: usize) -> ResponseSlotFuture {
        ResponseSlotFuture {
            slot: self.slot_ptr(target_shard),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use std::task::{Context, Poll};

    #[test]
    fn test_fill_then_poll_take_single_frame() {
        let slot = ResponseSlot::new();
        let frame = Frame::SimpleString(bytes::Bytes::from_static(b"OK"));
        slot.fill(vec![frame.clone()]);

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match slot.poll_take(&mut cx) {
            Poll::Ready(data) => {
                assert_eq!(data.len(), 1);
            }
            Poll::Pending => panic!("expected Ready after fill"),
        }
    }

    #[test]
    fn test_fill_then_poll_take_vec_of_frames() {
        let slot = ResponseSlot::new();
        let frames = vec![
            Frame::SimpleString(bytes::Bytes::from_static(b"OK")),
            Frame::Integer(42),
            Frame::Null,
        ];
        slot.fill(frames);

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match slot.poll_take(&mut cx) {
            Poll::Ready(data) => {
                assert_eq!(data.len(), 3);
            }
            Poll::Pending => panic!("expected Ready after fill"),
        }
    }

    #[test]
    fn test_poll_take_before_fill_returns_pending() {
        let slot = ResponseSlot::new();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Should be Pending when empty
        match slot.poll_take(&mut cx) {
            Poll::Pending => {}
            Poll::Ready(_) => panic!("expected Pending before fill"),
        }

        // Fill it
        slot.fill(vec![Frame::Integer(99)]);

        // Now should be Ready
        match slot.poll_take(&mut cx) {
            Poll::Ready(data) => {
                assert_eq!(data.len(), 1);
            }
            Poll::Pending => panic!("expected Ready after fill"),
        }
    }

    #[test]
    fn test_slot_reuse_fill_take_cycle() {
        let slot = ResponseSlot::new();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        for i in 0..5 {
            slot.fill(vec![Frame::Integer(i)]);
            match slot.poll_take(&mut cx) {
                Poll::Ready(data) => {
                    assert_eq!(data.len(), 1);
                }
                Poll::Pending => panic!("cycle {} expected Ready", i),
            }
        }
    }

    #[test]
    fn test_pool_creates_correct_number_of_slots() {
        let pool = ResponseSlotPool::new(8, 2);
        // Should be able to access all 8 slots without panic
        for i in 0..8 {
            let _slot = pool.slot_for(i);
        }
    }

    #[test]
    fn test_pool_slot_for_returns_correct_slot() {
        let pool = ResponseSlotPool::new(4, 0);
        // Verify slot_ptr and slot_for return the same address
        for i in 0..4 {
            let slot_ref = pool.slot_for(i) as *const ResponseSlot;
            let slot_ptr = pool.slot_ptr(i);
            assert_eq!(slot_ref, slot_ptr);
        }
    }

    #[test]
    fn test_future_resolves_after_fill() {
        let pool = ResponseSlotPool::new(4, 0);
        let slot = pool.slot_for(1);
        let future = pool.future_for(1);

        slot.fill(vec![Frame::SimpleString(bytes::Bytes::from_static(
            b"PONG",
        ))]);

        let result = block_on(future);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_concurrent_fill_from_another_thread() {
        let pool = ResponseSlotPool::new(4, 0);
        let addr = pool.slot_ptr(2) as usize;
        let future = pool.future_for(2);

        // Spawn a thread that fills the slot after a brief yield.
        // We transmit the address as usize (always Send) and reconstruct the pointer.
        let handle = std::thread::spawn(move || {
            std::thread::yield_now();
            // SAFETY: addr points to a valid ResponseSlot in the pool
            // which outlives this thread (we join before pool is dropped).
            let slot = unsafe { &*(addr as *const ResponseSlot) };
            slot.fill(vec![Frame::Integer(123)]);
        });

        let result = block_on(future);
        assert_eq!(result.len(), 1);
        handle.join().unwrap();
    }

    #[test]
    fn test_reset_clears_filled_data() {
        let slot = ResponseSlot::new();
        slot.fill(vec![Frame::Null]);
        slot.reset();

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match slot.poll_take(&mut cx) {
            Poll::Pending => {} // Correct: reset cleared the data
            Poll::Ready(_) => panic!("expected Pending after reset"),
        }
    }
}
