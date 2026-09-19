//! Loom model for the blocking-pop claim token (moon#1019 / moon#1023).
//!
//! The token is the REAL one: `src/blocking/claim.rs` is compiled into this
//! test crate through `#[path]`, and under `cfg(loom)` that file takes loom's
//! `Arc`/`AtomicU8` (review F6 on PR #1045 — the first cut modelled a
//! hand-copied twin, which proves nothing about the code that ships).
//!
//! Around it, each owner runs the waker's protocol: skip a settled waiter,
//! else POP, then `try_claim`, then send on a win or put the element back on a
//! loss (or on a failed send). The waiter settles when its wait ends without a
//! reply and, if a serve is already committed, takes it. The reply channel is
//! a model of the flume oneshot: a send into a CLOSED receiver fails, and
//! closing a receiver drops whatever it buffered.
//!
//! Verified under every interleaving:
//!
//!   1. at most one owner ever sends (exactly-once serve, moon#1019);
//!   2. `settle() == Dead` implies no owner ever sent, and none can afterwards
//!      — closing the receivers then drops nothing (moon#1023's drain is not
//!      needed);
//!   3. `settle() == Claimed` implies the waiter takes exactly one reply;
//!   4. conservation: elements in the keys + delivered + dropped == 2, and
//!      "dropped" is 0 whenever the waiter settles before it closes;
//!   5. a WON claim whose send FAILS (the receiver was closed without a
//!      settle — a connection task torn down abruptly) puts its element back.
//!
//! Run with: cargo rustc --release --test loom_blocking_claim -- --cfg loom
//! (then run the built test binary). `RUSTFLAGS="--cfg loom"` would apply the
//! cfg to every dependency too, and tokio's loom build of `hyper-util` does not
//! compile. Without `--cfg loom` the same models run repeatedly on std threads
//! as a smoke test (mirrors tests/loom_response_slot.rs).

#[path = "../src/blocking/claim.rs"]
#[allow(dead_code)]
mod claim;

use claim::{ClaimToken, Settled};

#[cfg(loom)]
use loom::sync::atomic::{AtomicUsize, Ordering};
#[cfg(loom)]
use loom::sync::{Arc, Mutex};

#[cfg(not(loom))]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(not(loom))]
use std::sync::{Arc, Mutex};

/// One owner's reply channel: `closed` once the waiter dropped its receiver.
#[derive(Default)]
struct Slot {
    closed: bool,
    value: Option<usize>,
}

struct World {
    token: ClaimToken,
    /// Elements in each owner's key (each owner is the only writer of its own).
    store: [AtomicUsize; 2],
    reply: [Mutex<Slot>; 2],
    /// How many owners' sends SUCCEEDED — invariant 1.
    sends: AtomicUsize,
    /// Elements lost with a closed receiver's buffer.
    dropped: AtomicUsize,
}

impl World {
    fn new() -> Self {
        World {
            token: ClaimToken::new(),
            store: [AtomicUsize::new(1), AtomicUsize::new(1)],
            reply: [Mutex::new(Slot::default()), Mutex::new(Slot::default())],
            sends: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
        }
    }

    /// `try_wake_*` + `deliver`, for owner `i`.
    fn owner_wake(&self, i: usize) {
        if !self.token.is_open() {
            return; // `is_settled`: skip without touching the datastore
        }
        // Pop first ...
        let had = self.store[i].load(Ordering::Relaxed);
        if had == 0 {
            return;
        }
        self.store[i].store(had - 1, Ordering::Relaxed);
        // ... claim second, with the element in hand ...
        let won = self.token.try_claim();
        // ... send on a win; a closed receiver fails the send.
        let sent = won && {
            let mut slot = self.reply[i].lock().unwrap();
            if slot.closed {
                false
            } else {
                slot.value = Some(i);
                true
            }
        };
        if sent {
            self.sends.fetch_add(1, Ordering::Relaxed);
        } else {
            // Lost claim, or a won claim whose send failed: put it back in
            // the same stretch.
            self.store[i].store(had, Ordering::Relaxed);
        }
    }

    /// Drop every receiver, losing whatever they buffered.
    fn close_receivers(&self) {
        for r in &self.reply {
            let mut slot = r.lock().unwrap();
            slot.closed = true;
            if slot.value.take().is_some() {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// `blocking_multikey::settle`, then drop the receivers.
    fn waiter_settle_then_close(&self) -> Option<usize> {
        let taken = match self.token.settle() {
            Settled::Dead => None,
            Settled::Claimed => loop {
                // The winner sends in the stretch it claimed in.
                if let Some(v) = self
                    .reply
                    .iter()
                    .find_map(|r| r.lock().unwrap().value.take())
                {
                    break Some(v);
                }
                thread_yield();
            },
        };
        self.close_receivers();
        taken
    }

    fn left(&self) -> usize {
        self.store.iter().map(|s| s.load(Ordering::Relaxed)).sum()
    }
}

#[cfg(loom)]
fn thread_yield() {
    loom::thread::yield_now();
}
#[cfg(not(loom))]
fn thread_yield() {
    std::thread::yield_now();
}

#[cfg(loom)]
fn spawn<F: FnOnce() + Send + 'static>(f: F) -> loom::thread::JoinHandle<()> {
    loom::thread::spawn(f)
}
#[cfg(not(loom))]
fn spawn<F: FnOnce() + Send + 'static>(f: F) -> std::thread::JoinHandle<()> {
    std::thread::spawn(f)
}

fn owners(w: &Arc<World>) -> Vec<impl FnOnce() -> Result<(), ()>> {
    (0..2)
        .map(|i| {
            let w = Arc::clone(w);
            let h = spawn(move || w.owner_wake(i));
            move || h.join().map_err(|_| ())
        })
        .collect()
}

/// Two owners serve while the waiter gives up: invariants 1–4.
fn model_two_owners_race_a_waiter_that_gives_up() {
    let w = Arc::new(World::new());
    let joins = owners(&w);
    let delivered = w.waiter_settle_then_close();
    for j in joins {
        j().unwrap();
    }
    let sends = w.sends.load(Ordering::Relaxed);
    assert!(sends <= 1, "two owners served one waiter");
    match delivered {
        None => assert_eq!(sends, 0, "settled Dead yet an owner sent"),
        Some(_) => assert_eq!(sends, 1),
    }
    // No owner can serve after the waiter settled.
    for i in 0..2 {
        w.owner_wake(i);
    }
    assert_eq!(
        w.sends.load(Ordering::Relaxed),
        sends,
        "a send after settle"
    );
    assert_eq!(
        w.dropped.load(Ordering::Relaxed),
        0,
        "a settled waiter dropped a buffered reply"
    );
    assert_eq!(
        w.left() + usize::from(delivered.is_some()),
        2,
        "an element was destroyed"
    );
}

/// The waiter is served normally (never settles): exactly one owner wins, and
/// the loser's element is back in its key.
fn model_two_owners_serve_a_live_waiter_once() {
    let w = Arc::new(World::new());
    for j in owners(&w) {
        j().unwrap();
    }
    assert_eq!(w.sends.load(Ordering::Relaxed), 1, "exactly one serve");
    assert_eq!(w.left(), 1, "the losing owner restored its element");
}

/// Invariant 5: the receivers are closed WITHOUT a settle, racing the owners.
/// A won claim may then fail its send; its element must go back. The only
/// element that can be lost is one already buffered when the receiver closed
/// — exactly the hazard `settle` exists to prevent (model 1 asserts it never
/// happens on the settled path).
fn model_a_won_claim_whose_send_fails_puts_the_element_back() {
    let w = Arc::new(World::new());
    let joins = owners(&w);
    w.close_receivers();
    for j in joins {
        j().unwrap();
    }
    let sends = w.sends.load(Ordering::Relaxed);
    let dropped = w.dropped.load(Ordering::Relaxed);
    assert!(sends <= 1, "two owners served one waiter");
    assert!(dropped <= sends, "an element vanished without a send");
    assert_eq!(
        w.left() + dropped,
        2,
        "a failed send did not put its element back"
    );
}

#[cfg(loom)]
#[test]
fn loom_claim_race_with_settle() {
    loom::model(model_two_owners_race_a_waiter_that_gives_up);
}

#[cfg(loom)]
#[test]
fn loom_claim_race_without_settle() {
    loom::model(model_two_owners_serve_a_live_waiter_once);
}

#[cfg(loom)]
#[test]
fn loom_won_claim_send_fails() {
    loom::model(model_a_won_claim_whose_send_fails_puts_the_element_back);
}

#[cfg(not(loom))]
#[test]
fn std_claim_race_with_settle() {
    for _ in 0..2_000 {
        model_two_owners_race_a_waiter_that_gives_up();
    }
}

#[cfg(not(loom))]
#[test]
fn std_claim_race_without_settle() {
    for _ in 0..2_000 {
        model_two_owners_serve_a_live_waiter_once();
    }
}

#[cfg(not(loom))]
#[test]
fn std_won_claim_send_fails() {
    for _ in 0..2_000 {
        model_a_won_claim_whose_send_fails_puts_the_element_back();
    }
}
