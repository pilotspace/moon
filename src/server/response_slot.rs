//! Pre-allocated response slot for zero-allocation cross-shard dispatch.
//!
//! Eliminates ~80-120ns per cross-shard write by replacing oneshot channel
//! allocation with a reusable, lock-free AtomicU8 state machine.
//!
//! Single-producer (target shard fills), single-consumer (connection owner polls).

use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU64, Ordering};
use std::task::{Context, Poll};

use atomic_waker::AtomicWaker;

use crate::protocol::Frame;

const EMPTY: u8 = 0;
const FILLED: u8 = 1;

// ── D1 park instrumentation (moon#416) ──────────────────────────────────
//
// `docs/internal/cross-shard-cost-model.md` puts a park at ~24.9 core-us and
// 85% of p=1 cost, but that figure was INFERRED by fitting a pipeline-depth
// sweep -- parks were never counted. These count them, because the decision
// "is cross-connection park batching worth building" turns entirely on two
// numbers that inference cannot supply: how often an await actually parks,
// and how many awaits sit parked simultaneously.
//
// Relaxed throughout: these are diagnostics, never read for control flow, and
// an exact total is not worth an ordering constraint on the dispatch path.

/// Every first poll of a cross-shard reply await.
static REMOTE_AWAITS: AtomicU64 = AtomicU64::new(0);
/// First polls that found the slot empty and therefore suspended. The reply had
/// not arrived yet, so this await costs a wake later.
static REMOTE_AWAITS_PARKED: AtomicU64 = AtomicU64::new(0);
/// Polls after the first that were STILL pending -- spurious wakes. Counted
/// apart from parks so they cannot inflate the number D1 is judged on.
static REMOTE_AWAIT_REPOLLS: AtomicU64 = AtomicU64::new(0);
/// Awaits currently parked. Signed so an unbalanced decrement shows up as a
/// negative reading instead of wrapping to ~1.8e19 and looking plausible.
static REMOTE_PARKED_INFLIGHT: AtomicI64 = AtomicI64::new(0);
/// Sum of the in-flight depth observed at each park. `sum / parked` is the mean
/// number of awaits parked at the moment of parking -- i.e. how many parks one
/// batched wake could have replaced. ~1 means D1 has nothing to batch.
static REMOTE_PARK_CONCURRENCY_SUM: AtomicU64 = AtomicU64::new(0);

/// Total cross-shard reply awaits (parked or not).
#[inline]
pub fn total_remote_awaits() -> u64 {
    REMOTE_AWAITS.load(Ordering::Relaxed)
}
/// Awaits that actually suspended. `parked / awaits` is the park rate.
#[inline]
pub fn total_remote_awaits_parked() -> u64 {
    REMOTE_AWAITS_PARKED.load(Ordering::Relaxed)
}
/// Spurious wakes: re-polled while still empty.
#[inline]
pub fn total_remote_await_repolls() -> u64 {
    REMOTE_AWAIT_REPOLLS.load(Ordering::Relaxed)
}
/// Awaits parked right now.
#[inline]
pub fn remote_parked_inflight() -> i64 {
    REMOTE_PARKED_INFLIGHT.load(Ordering::Relaxed)
}
/// Sum of park-time depth; divide by `total_remote_awaits_parked()` for the mean.
#[inline]
pub fn total_remote_park_concurrency_sum() -> u64 {
    REMOTE_PARK_CONCURRENCY_SUM.load(Ordering::Relaxed)
}

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
    /// Single-producer only (target shard thread).
    pub(crate) fn fill(&self, response: Vec<Frame>) {
        // Write data before state transition (will be visible via Release ordering)
        // SAFETY: Single-producer guarantee (only target shard calls fill). State is
        // EMPTY, so no consumer is reading the UnsafeCell. Data write completes before
        // the Release store to state, providing happens-before with consumer's Acquire load.
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

    /// Take the response iff the slot is FILLED, leaving it EMPTY; `None` if not yet
    /// filled. Does NOT touch the waker. The SINGLE unsafe `UnsafeCell` access for the
    /// whole slot lives here — `poll_take` and `try_take` both route through it, so
    /// neither introduces an unsafe block of its own. Single consumer (connection
    /// owner thread) only.
    #[inline]
    fn take_if_filled(&self) -> Option<Vec<Frame>> {
        if self.state.load(Ordering::Acquire) != FILLED {
            return None;
        }
        #[allow(clippy::unwrap_used)] // fill() always writes Some before the FILLED transition
        // SAFETY: Acquire load above confirmed FILLED (happens-after fill()'s Release store,
        // so the data write is visible); single-consumer slot, no concurrent read aliases it;
        // fill() always writes Some before the FILLED transition, so take() is Some.
        let data = unsafe { (*self.data.get()).take().unwrap() };
        self.state.store(EMPTY, Ordering::Release);
        Some(data)
    }

    /// Poll for the response (called by the connection owner thread).
    ///
    /// Returns `Ready(data)` if the slot has been filled, or `Pending` if empty.
    /// After returning `Ready`, the slot is reset to EMPTY and can be reused.
    pub(crate) fn poll_take(&self, cx: &mut Context<'_>) -> Poll<Vec<Frame>> {
        // Fast path: already filled.
        if let Some(data) = self.take_if_filled() {
            return Poll::Ready(data);
        }

        // Slot is empty. Register waker using AtomicWaker (handles synchronization).
        self.waker.register(cx.waker());

        // Re-check after registration: closes the race where the producer fills
        // between our first load and the waker store.
        if let Some(data) = self.take_if_filled() {
            return Poll::Ready(data);
        }

        Poll::Pending
    }

    /// Non-blocking take: returns the response iff the slot is already FILLED,
    /// WITHOUT registering a waker. For the reply-side idle-gated spin
    /// (xshard-read-fastpath C2): the caller is busy-polling, not parking, so it
    /// must not register a waker. Single consumer (connection owner thread).
    ///
    /// Live under BOTH runtimes: handler_sharded (tokio) and handler_monoio
    /// (L3b) reply paths spin on this before parking on `poll_take`.
    #[inline]
    pub(crate) fn try_take(&self) -> Option<Vec<Frame>> {
        // Same FILLED-confirmed take as poll_take's fast path, minus the waker
        // registration — the spin caller is busy-polling, not parking.
        self.take_if_filled()
    }

    /// Force-reset the slot to EMPTY, clearing any pending data and waker.
    ///
    /// Used after connection errors to ensure clean state for potential reuse.
    #[allow(dead_code)] // Used in tests and available for future error recovery paths
    pub(crate) fn reset(&self) {
        self.state.store(EMPTY, Ordering::Release);
        // SAFETY: reset() is called only by the connection owner (single consumer)
        // after storing EMPTY, so no producer is concurrently writing to the UnsafeCell.
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

/// Future that owns a shared handle to a `ResponseSlot` for async `.await` usage.
///
/// Holds an `Arc<ResponseSlot>` clone (not a borrow or raw pointer), so the slot
/// stays alive for as long as EITHER this future OR any in-flight `ResponseSlotPtr`
/// on a target shard references it. Abandoning this future (drop, panic-unwind,
/// shutdown break) can never dangle the target shard's late `fill()` — the refcount
/// keeps the slot allocated until the last handle drops. This is the structural
/// replacement for the old raw-pointer-into-stack-pool design (see PR review:
/// panic-unwind cross-shard reply UAF).
pub struct ResponseSlotFuture {
    slot: Arc<ResponseSlot>,
    /// False until the first poll, so a re-poll is never charged as a new await.
    polled: bool,
    /// True while this await is counted in `REMOTE_PARKED_INFLIGHT`. Drop uses it
    /// to release the gauge when a future is abandoned mid-park (shutdown break,
    /// panic-unwind) -- without that the mean depth drifts up forever.
    parked: bool,
}

impl Future for ResponseSlotFuture {
    type Output = Vec<Frame>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let first = !this.polled;
        this.polled = true;
        if first {
            REMOTE_AWAITS.fetch_add(1, Ordering::Relaxed);
        }
        match this.slot.poll_take(cx) {
            Poll::Pending => {
                if first {
                    this.parked = true;
                    REMOTE_AWAITS_PARKED.fetch_add(1, Ordering::Relaxed);
                    let depth = REMOTE_PARKED_INFLIGHT.fetch_add(1, Ordering::Relaxed) + 1;
                    REMOTE_PARK_CONCURRENCY_SUM.fetch_add(depth as u64, Ordering::Relaxed);
                } else {
                    REMOTE_AWAIT_REPOLLS.fetch_add(1, Ordering::Relaxed);
                }
                Poll::Pending
            }
            Poll::Ready(data) => {
                if this.parked {
                    this.parked = false;
                    REMOTE_PARKED_INFLIGHT.fetch_sub(1, Ordering::Relaxed);
                }
                Poll::Ready(data)
            }
        }
    }
}

impl Drop for ResponseSlotFuture {
    fn drop(&mut self) {
        // Abandoned while parked (shutdown break / panic-unwind). Releasing here
        // keeps the gauge honest; leaking it would make the mean depth -- the one
        // number D1 is judged on -- climb without bound.
        if self.parked {
            REMOTE_PARKED_INFLIGHT.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// Per-connection pre-allocated response slots, one per target shard.
///
/// Eliminates oneshot channel allocation on the cross-shard dispatch hot path.
/// Created once per connection, reused across all dispatches for that connection.
pub struct ResponseSlotPool {
    slots: Vec<Arc<ResponseSlot>>,
    /// The shard this connection is assigned to (for diagnostics/debugging).
    #[allow(dead_code)]
    my_shard: usize,
}

impl ResponseSlotPool {
    /// Create a pool with one slot per shard.
    pub fn new(num_shards: usize, my_shard: usize) -> Self {
        let mut slots = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            slots.push(Arc::new(ResponseSlot::new()));
        }
        Self { slots, my_shard }
    }

    /// Get a reference to the slot for the given target shard (for the reply-side
    /// spin `try_take()`; borrows the pool, no refcount bump).
    #[inline]
    pub fn slot_for(&self, target_shard: usize) -> &ResponseSlot {
        &self.slots[target_shard]
    }

    /// Clone the shared handle to the slot for the given target shard, to send
    /// cross-thread in a `ShardMessage`. One refcount bump per batch — the clone
    /// keeps the slot alive until BOTH this connection's pool AND the in-flight
    /// message drop, so the target shard's `fill()` can never dangle even if the
    /// connection task is dropped mid-flight (panic-unwind / shutdown).
    #[inline]
    pub fn slot_arc(&self, target_shard: usize) -> Arc<ResponseSlot> {
        Arc::clone(&self.slots[target_shard])
    }

    /// Create a future that resolves when the target shard fills the slot.
    ///
    /// The future OWNS an `Arc` clone of the slot, so it is drop-safe: abandoning
    /// it (shutdown break / panic-unwind) cannot dangle a concurrent `fill()`.
    #[inline]
    pub fn future_for(&self, target_shard: usize) -> ResponseSlotFuture {
        ResponseSlotFuture {
            slot: Arc::clone(&self.slots[target_shard]),
            polled: false,
            parked: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use std::task::{Context, Poll};

    /// The park counters are process-global and `cargo test` runs these in
    /// threads, so the two tests below would read each other's increments and
    /// fail on exact deltas. Serialise them rather than loosening the
    /// assertions -- an exact delta is the whole point of a counter test.
    static PARK_COUNTERS: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    /// D1 instrumentation (moon#416). The cost model says a park costs ~24.9
    /// core-us and is 85% of p=1 cost, but "parks" was INFERRED from a pipeline
    /// sweep, never counted. These pin the counter's meaning:
    ///
    ///   * a slot already filled when first polled did NOT park -- the reply beat
    ///     the await, which is the outcome D1 is trying to manufacture;
    ///   * a slot empty when first polled DID park;
    ///   * re-polls after a park are spurious wakes, counted separately, because
    ///     charging them as parks would inflate the very number D1 is judged on.
    #[test]
    fn a_filled_slot_does_not_park_and_an_empty_one_does() {
        let _serialised = PARK_COUNTERS.lock();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        let a0 = total_remote_awaits();
        let p0 = total_remote_awaits_parked();

        // Reply already present: an await, but NOT a park.
        let pool = ResponseSlotPool::new(2, 0);
        pool.slot_for(1).fill(vec![Frame::Integer(1)]);
        let mut ready = pool.future_for(1);
        assert!(matches!(Pin::new(&mut ready).poll(&mut cx), Poll::Ready(_)));
        assert_eq!(
            total_remote_awaits() - a0,
            1,
            "every first poll is one await"
        );
        assert_eq!(
            total_remote_awaits_parked() - p0,
            0,
            "a slot filled before the first poll must NOT be charged as a park"
        );

        // Slot empty: this is a park.
        let mut pending = pool.future_for(1);
        assert!(matches!(
            Pin::new(&mut pending).poll(&mut cx),
            Poll::Pending
        ));
        assert_eq!(total_remote_awaits() - a0, 2);
        assert_eq!(total_remote_awaits_parked() - p0, 1, "an empty slot parks");

        // A spurious re-poll while still empty is NOT a second park.
        assert!(matches!(
            Pin::new(&mut pending).poll(&mut cx),
            Poll::Pending
        ));
        assert_eq!(
            total_remote_awaits_parked() - p0,
            1,
            "re-polling a parked await must not double-count the park"
        );
    }

    /// The number D1 lives or dies on: how many awaits are parked at the same
    /// time. If it is ~1 there is nothing to batch and D1 is worthless; if it is
    /// ~N there are N parks where physics needs one.
    #[test]
    fn concurrency_sum_tracks_simultaneously_parked_awaits() {
        let _serialised = PARK_COUNTERS.lock();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let pool = ResponseSlotPool::new(4, 0);

        let p0 = total_remote_awaits_parked();
        let s0 = total_remote_park_concurrency_sum();

        // Park three awaits on three different targets, all outstanding at once.
        let mut f1 = pool.future_for(1);
        let mut f2 = pool.future_for(2);
        let mut f3 = pool.future_for(3);
        assert!(matches!(Pin::new(&mut f1).poll(&mut cx), Poll::Pending));
        assert!(matches!(Pin::new(&mut f2).poll(&mut cx), Poll::Pending));
        assert!(matches!(Pin::new(&mut f3).poll(&mut cx), Poll::Pending));

        assert_eq!(total_remote_awaits_parked() - p0, 3);
        // Observed depth at each park: 1, then 2, then 3.
        assert_eq!(
            total_remote_park_concurrency_sum() - s0,
            6,
            "sum of in-flight depth at park time must be 1+2+3"
        );

        // Completing them must release the in-flight count, or the mean drifts up
        // forever and the metric becomes fiction.
        for (f, t) in [(&mut f1, 1usize), (&mut f2, 2), (&mut f3, 3)] {
            pool.slot_for(t).fill(vec![Frame::Integer(t as i64)]);
            assert!(matches!(Pin::new(f).poll(&mut cx), Poll::Ready(_)));
        }
        assert_eq!(
            remote_parked_inflight(),
            0,
            "every completed await must decrement the in-flight gauge"
        );
    }

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
    fn test_pool_slot_arc_returns_correct_slot() {
        let pool = ResponseSlotPool::new(4, 0);
        // Verify slot_arc and slot_for reference the same slot object.
        for i in 0..4 {
            let slot_ref = pool.slot_for(i) as *const ResponseSlot;
            let slot_arc = pool.slot_arc(i);
            assert_eq!(slot_ref, Arc::as_ptr(&slot_arc));
        }
    }

    /// Drop-safety regression (PR review: panic-unwind cross-shard reply UAF).
    /// The slot handle sent cross-thread MUST outlive the connection's pool being
    /// dropped while a reply is still in flight — otherwise the target shard's late
    /// `fill()` is a use-after-free. With Arc-owned slots the allocation survives
    /// (strong_count >= 1) until the last handle drops; the raw-pointer design this
    /// replaced could not express this and would dangle. Provable without a
    /// sanitizer via `Arc::strong_count`.
    #[test]
    fn test_slot_arc_outlives_pool_drop_and_is_fillable() {
        let pool = ResponseSlotPool::new(4, 0);
        // Simulate the cross-thread send: clone the handle out of the pool.
        let in_flight = pool.slot_arc(2);
        assert_eq!(Arc::strong_count(&in_flight), 2, "pool + in-flight handle");

        // Connection task drops its pool (e.g. panic-unwind / shutdown) while the
        // message is still queued on the target shard.
        drop(pool);

        // The slot is still alive and safe to fill — no dangling pointer.
        assert_eq!(
            Arc::strong_count(&in_flight),
            1,
            "handle keeps the slot allocated after pool drop"
        );
        in_flight.fill(vec![Frame::Integer(7)]);
        assert_eq!(in_flight.try_take().map(|v| v.len()), Some(1));
    }

    #[test]
    fn test_future_resolves_after_fill() {
        // Polling a ResponseSlotFuture moves the process-global park counters,
        // so this must not run beside the tests that assert on them.
        let _serialised = PARK_COUNTERS.lock();
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
        // Same reason as above: this one actually parks, so it moves
        // REMOTE_AWAITS_PARKED and the in-flight depth as well.
        let _serialised = PARK_COUNTERS.lock();
        let pool = ResponseSlotPool::new(4, 0);
        // The cross-thread send is now an Arc clone (always Send) — no raw pointer.
        let in_flight = pool.slot_arc(2);
        let future = pool.future_for(2);

        // Spawn a thread that fills the slot after a brief yield.
        let handle = std::thread::spawn(move || {
            std::thread::yield_now();
            in_flight.fill(vec![Frame::Integer(123)]);
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
