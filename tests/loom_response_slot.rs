//! Loom model tests for ResponseSlot.
//!
//! Verifies that the lock-free SPSC ResponseSlot state machine
//! (EMPTY → fill → FILLED → poll_take → EMPTY) is correct under
//! all possible thread interleavings.
//!
//! Run with: cargo test --test loom_response_slot -- --nocapture
//! Note: loom exhaustive exploration can take 10-60s depending on depth.

#![allow(unexpected_cfgs)]

#[cfg(not(loom))]
mod non_loom {
    //! When not running under loom, we use std atomics and test the
    //! same logic with regular threads to validate the test structure.
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU8, Ordering};

    const EMPTY: u8 = 0;
    const FILLED: u8 = 1;

    /// Minimal reproduction of ResponseSlot's state machine for testing.
    struct SlotModel {
        state: AtomicU8,
        data: std::cell::UnsafeCell<Option<u64>>,
    }

    unsafe impl Send for SlotModel {}
    unsafe impl Sync for SlotModel {}

    impl SlotModel {
        fn new() -> Self {
            Self {
                state: AtomicU8::new(EMPTY),
                data: std::cell::UnsafeCell::new(None),
            }
        }

        fn fill(&self, value: u64) {
            unsafe { *self.data.get() = Some(value) };
            let prev = self.state.swap(FILLED, Ordering::Release);
            assert_eq!(prev, EMPTY, "fill on non-EMPTY slot");
        }

        fn try_take(&self) -> Option<u64> {
            let state = self.state.load(Ordering::Acquire);
            if state == FILLED {
                let data = unsafe { (*self.data.get()).take() };
                self.state.store(EMPTY, Ordering::Release);
                data
            } else {
                None
            }
        }

        fn reset(&self) {
            self.state.store(EMPTY, Ordering::Release);
            unsafe { *self.data.get() = None };
        }
    }

    #[test]
    fn fill_then_take() {
        let slot = Arc::new(SlotModel::new());
        let s2 = slot.clone();

        let producer = std::thread::spawn(move || {
            s2.fill(42);
        });

        producer.join().unwrap();

        let value = slot.try_take();
        assert_eq!(value, Some(42));
        assert_eq!(slot.try_take(), None); // slot now empty
    }

    #[test]
    fn fill_take_cycle() {
        let slot = Arc::new(SlotModel::new());

        for i in 0..100 {
            let s = slot.clone();
            let producer = std::thread::spawn(move || {
                s.fill(i);
            });
            producer.join().unwrap();

            let v = slot.try_take().unwrap();
            assert_eq!(v, i);
        }
    }

    #[test]
    fn reset_clears_data() {
        let slot = SlotModel::new();
        slot.fill(99);
        slot.reset();
        assert_eq!(slot.try_take(), None);
    }

    #[test]
    fn concurrent_fill_take() {
        let slot = Arc::new(SlotModel::new());
        let s2 = slot.clone();

        let producer = std::thread::spawn(move || {
            s2.fill(42);
        });

        // Spin until we get the value
        loop {
            if let Some(v) = slot.try_take() {
                assert_eq!(v, 42);
                break;
            }
            std::hint::spin_loop();
        }

        producer.join().unwrap();
    }
}

#[cfg(loom)]
mod loom_tests {
    use loom::cell::UnsafeCell;
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicU8, Ordering};
    use loom::thread;

    const EMPTY: u8 = 0;
    const FILLED: u8 = 1;

    /// Minimal ResponseSlot model using loom primitives.
    struct SlotModel {
        state: AtomicU8,
        data: UnsafeCell<Option<u64>>,
    }

    unsafe impl Send for SlotModel {}
    unsafe impl Sync for SlotModel {}

    impl SlotModel {
        fn new() -> Self {
            Self {
                state: AtomicU8::new(EMPTY),
                data: UnsafeCell::new(None),
            }
        }

        fn fill(&self, value: u64) {
            self.data.with_mut(|ptr| unsafe { *ptr = Some(value) });
            let prev = self.state.swap(FILLED, Ordering::Release);
            assert_eq!(prev, EMPTY, "fill on non-EMPTY slot");
        }

        fn try_take(&self) -> Option<u64> {
            let state = self.state.load(Ordering::Acquire);
            if state == FILLED {
                let data = self.data.with_mut(|ptr| unsafe { (*ptr).take() });
                self.state.store(EMPTY, Ordering::Release);
                data
            } else {
                None
            }
        }
    }

    /// Loom: producer fills, consumer takes — data is visible across threads.
    #[test]
    fn loom_fill_then_take() {
        loom::model(|| {
            let slot = Arc::new(SlotModel::new());
            let s2 = slot.clone();

            let producer = thread::spawn(move || {
                s2.fill(42);
            });

            producer.join().unwrap();

            let v = slot.try_take();
            assert_eq!(v, Some(42));
        });
    }

    /// Loom: concurrent producer fill + consumer poll under all interleavings.
    /// The consumer must eventually see the value with no data race.
    #[test]
    fn loom_concurrent_fill_take() {
        loom::model(|| {
            let slot = Arc::new(SlotModel::new());
            let s2 = slot.clone();

            let producer = thread::spawn(move || {
                s2.fill(42);
            });

            // Consumer attempts to take — may see EMPTY or FILLED
            let result = slot.try_take();

            producer.join().unwrap();

            if result.is_none() {
                // Producer hadn't filled yet — take now (must succeed)
                let v = slot.try_take();
                assert_eq!(v, Some(42));
            } else {
                assert_eq!(result, Some(42));
            }
        });
    }

    /// Loom: fill → take → fill → take cycle verifies reset-to-EMPTY works.
    #[test]
    fn loom_fill_take_refill() {
        loom::model(|| {
            let slot = Arc::new(SlotModel::new());
            let s2 = slot.clone();
            let s3 = slot.clone();

            // First cycle
            let p1 = thread::spawn(move || {
                s2.fill(1);
            });
            p1.join().unwrap();
            let v1 = slot.try_take();
            assert_eq!(v1, Some(1));

            // Second cycle (slot was reset to EMPTY by try_take)
            let p2 = thread::spawn(move || {
                s3.fill(2);
            });
            p2.join().unwrap();
            let v2 = slot.try_take();
            assert_eq!(v2, Some(2));
        });
    }
}

// ---------------------------------------------------------------------------
// moon#1299: the per-shard TXN isolation view (`transaction::isolation::
// Published`). One writer — the owning shard thread — STORES whole words,
// never a read-modify-write; other shards' FLUSHDB / FLUSHALL / SWAPDB and
// INFO only LOAD. `open_word` packs `open count (16 bits) | oldest begin ms
// (48 bits)` so a reader can never pair a count with another moment's
// timestamp. The model checks exactly that, and that the final state is
// visible once the writer is joined.
// ---------------------------------------------------------------------------

#[allow(dead_code)]
mod txn_isolation_view {
    pub const BEGIN_BITS: u32 = 48;
    pub const BEGIN_MASK: u64 = (1u64 << BEGIN_BITS) - 1;

    pub fn pack(count: u64, begin: u64) -> u64 {
        (count.min(u16::MAX as u64) << BEGIN_BITS) | (begin & BEGIN_MASK)
    }

    pub fn unpack(word: u64) -> (u64, u64) {
        (word >> BEGIN_BITS, word & BEGIN_MASK)
    }

    /// The states the writer publishes, in order: TXN 1 begins at 1000,
    /// TXN 2 begins at 2000, TXN 1 ends (oldest is now 2000), TXN 2 ends.
    pub const STATES: [(u64, u64); 5] = [(0, 0), (1, 1000), (2, 1000), (1, 2000), (0, 0)];
}

#[cfg(not(loom))]
mod txn_isolation_view_non_loom {
    use super::txn_isolation_view::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn packed_word_round_trips() {
        for (c, b) in STATES {
            assert_eq!(unpack(pack(c, b)), (c, b));
        }
    }

    #[test]
    fn readers_never_see_a_torn_open_word() {
        let open = Arc::new(AtomicU64::new(0));
        let held = Arc::new(AtomicU64::new(0));
        let (o, h) = (Arc::clone(&open), Arc::clone(&held));
        let writer = std::thread::spawn(move || {
            for _ in 0..10_000 {
                for (i, (c, b)) in STATES.iter().enumerate() {
                    h.store(if *c > 0 { 1 << i } else { 0 }, Ordering::Release);
                    o.store(pack(*c, *b), Ordering::Release);
                }
            }
        });
        for _ in 0..100_000 {
            let seen = unpack(open.load(Ordering::Acquire));
            assert!(STATES.contains(&seen), "torn view {seen:?}");
            let _ = held.load(Ordering::Acquire);
        }
        writer.join().unwrap();
        assert_eq!(unpack(open.load(Ordering::Acquire)), (0, 0));
        assert_eq!(held.load(Ordering::Acquire), 0);
    }
}

#[cfg(loom)]
mod txn_isolation_view_loom {
    use super::txn_isolation_view::*;
    use loom::sync::Arc;
    use loom::sync::atomic::{AtomicU64, Ordering};
    use loom::thread;

    #[test]
    fn loom_open_word_is_never_torn_and_final_state_is_visible() {
        loom::model(|| {
            let open = Arc::new(AtomicU64::new(0));
            let held = Arc::new(AtomicU64::new(0));
            let (o, h) = (Arc::clone(&open), Arc::clone(&held));
            let writer = thread::spawn(move || {
                for (c, b) in STATES.iter().skip(1) {
                    h.store(if *c > 0 { 1 << 3 } else { 0 }, Ordering::Release);
                    o.store(pack(*c, *b), Ordering::Release);
                }
            });
            // A concurrent reader (another shard's FLUSHDB / INFO).
            let seen = unpack(open.load(Ordering::Acquire));
            assert!(STATES.contains(&seen), "torn view {seen:?}");
            assert_eq!(seen.0 == 0, seen.1 == 0, "count and begin disagree");
            let _ = held.load(Ordering::Acquire);
            writer.join().unwrap();
            // Joined: the last publication (nothing open, nothing held).
            assert_eq!(unpack(open.load(Ordering::Acquire)), (0, 0));
            assert_eq!(held.load(Ordering::Acquire), 0);
        });
    }
}
