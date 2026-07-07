//! Loom model for the WAL v3 sync-agent watermark state machine
//! (`src/persistence/wal_v3/sync_agent.rs::SyncShared`).
//!
//! Verifies, under all interleavings of one publisher (agent thread) and one
//! waiter (shard thread in `wait_durable`):
//!   1. no lost wakeup — a waiter that saw `watermark < target` always gets
//!      woken by a publish/poison that happens after it checked, because the
//!      publisher takes the mutex before notifying and the waiter re-checks
//!      under the same mutex;
//!   2. the watermark is monotonic under fetch_max publishing;
//!   3. a poison is never missed by a parked waiter.
//!
//! Run with: RUSTFLAGS="--cfg loom" cargo test --release --test loom_wal_sync_agent
//! Without --cfg loom the same model runs once with std primitives as a
//! smoke test (mirrors tests/loom_response_slot.rs structure).

#![allow(unexpected_cfgs)]

#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(loom)]
use loom::sync::{Arc, Condvar, Mutex};

#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(not(loom))]
use std::sync::{Arc, Condvar, Mutex};

/// Mirror of `SyncShared` on loom-compatible primitives (std Condvar here,
/// parking_lot in production — same monitor semantics).
struct Shared {
    durable_lsn: AtomicU64,
    poisoned: AtomicBool,
    mutex: Mutex<()>,
    condvar: Condvar,
}

impl Shared {
    fn new() -> Self {
        Self {
            durable_lsn: AtomicU64::new(0),
            poisoned: AtomicBool::new(false),
            mutex: Mutex::new(()),
            condvar: Condvar::new(),
        }
    }

    fn publish(&self, lsn: u64) {
        self.durable_lsn.fetch_max(lsn, Ordering::Release);
        let _g = self.mutex.lock().unwrap();
        self.condvar.notify_all();
    }

    fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
        let _g = self.mutex.lock().unwrap();
        self.condvar.notify_all();
    }

    /// wait_watermark without the timeout escape hatch (timeout is a
    /// liveness bound, not part of the correctness argument).
    fn wait(&self, lsn: u64) -> Result<(), ()> {
        let mut guard = self.mutex.lock().unwrap();
        loop {
            if self.durable_lsn.load(Ordering::Acquire) >= lsn {
                return Ok(());
            }
            if self.poisoned.load(Ordering::Acquire) {
                return Err(());
            }
            guard = self.condvar.wait(guard).unwrap();
        }
    }
}

fn model_publish_wakes_waiter() {
    let shared = Arc::new(Shared::new());
    let publisher = {
        let shared = Arc::clone(&shared);
        thread_spawn(move || shared.publish(5))
    };
    let waiter = {
        let shared = Arc::clone(&shared);
        thread_spawn(move || shared.wait(5))
    };
    publisher.join().unwrap();
    let result = waiter.join().unwrap();
    assert_eq!(result, Ok(()), "waiter must observe the publish");
    assert!(shared.durable_lsn.load(Ordering::Acquire) >= 5);
}

fn model_poison_wakes_waiter() {
    let shared = Arc::new(Shared::new());
    let publisher = {
        let shared = Arc::clone(&shared);
        thread_spawn(move || shared.poison())
    };
    let waiter = {
        let shared = Arc::clone(&shared);
        thread_spawn(move || shared.wait(5))
    };
    publisher.join().unwrap();
    let result = waiter.join().unwrap();
    assert_eq!(result, Err(()), "waiter must observe the poison");
}

fn model_watermark_monotonic() {
    let shared = Arc::new(Shared::new());
    let a = {
        let shared = Arc::clone(&shared);
        thread_spawn(move || shared.publish(10))
    };
    let b = {
        let shared = Arc::clone(&shared);
        thread_spawn(move || shared.publish(3))
    };
    a.join().unwrap();
    b.join().unwrap();
    assert_eq!(
        shared.durable_lsn.load(Ordering::Acquire),
        10,
        "fetch_max publish must never regress the watermark"
    );
}

#[cfg(loom)]
fn thread_spawn<T: Send + 'static>(
    f: impl FnOnce() -> T + Send + 'static,
) -> loom::thread::JoinHandle<T> {
    loom::thread::spawn(f)
}

#[cfg(not(loom))]
fn thread_spawn<T: Send + 'static>(
    f: impl FnOnce() -> T + Send + 'static,
) -> std::thread::JoinHandle<T> {
    std::thread::spawn(f)
}

#[cfg(loom)]
mod loom_tests {
    use super::*;

    #[test]
    fn loom_publish_wakes_waiter() {
        loom::model(model_publish_wakes_waiter);
    }

    #[test]
    fn loom_poison_wakes_waiter() {
        loom::model(model_poison_wakes_waiter);
    }

    #[test]
    fn loom_watermark_monotonic() {
        loom::model(model_watermark_monotonic);
    }
}

#[cfg(not(loom))]
mod std_smoke {
    use super::*;

    #[test]
    fn smoke_publish_wakes_waiter() {
        for _ in 0..100 {
            model_publish_wakes_waiter();
        }
    }

    #[test]
    fn smoke_poison_wakes_waiter() {
        for _ in 0..100 {
            model_poison_wakes_waiter();
        }
    }

    #[test]
    fn smoke_watermark_monotonic() {
        for _ in 0..100 {
            model_watermark_monotonic();
        }
    }
}
