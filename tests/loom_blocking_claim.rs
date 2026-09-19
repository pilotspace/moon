//! Loom model for the blocking-pop claim token
//! (`src/blocking/claim.rs::ClaimToken`, moon#1019 / moon#1023).
//!
//! One waiter is registered on two owner shards, each holding one element in
//! the key it watches. Every owner runs the waker's protocol — skip a settled
//! waiter, else POP, then `try_claim`, then send on a win or put the element
//! back on a loss — while the waiter concurrently gives up (`settle`) and, if
//! a serve is already committed, takes it. Verified under every interleaving:
//!
//!   1. at most one owner ever sends (exactly-once serve, moon#1019);
//!   2. `settle() == Dead` implies no owner ever sent, and no owner can send
//!      afterwards (no drain needed, moon#1023);
//!   3. `settle() == Claimed` implies the waiter receives exactly one reply;
//!   4. conservation: elements left in the keys + elements delivered == the
//!      elements that were there — nothing is destroyed.
//!
//! Run with: cargo rustc --release --test loom_blocking_claim -- --cfg loom
//! (then run the built test binary). `RUSTFLAGS="--cfg loom"` would apply the
//! cfg to every dependency too, and tokio's loom build of `hyper-util` does not
//! compile.
//! Without --cfg loom the same model runs repeatedly on std threads as a
//! smoke test (mirrors tests/loom_response_slot.rs).

#![allow(unexpected_cfgs)]

#[cfg(loom)]
use loom::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
#[cfg(loom)]
use loom::sync::{Arc, Mutex};

#[cfg(not(loom))]
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
#[cfg(not(loom))]
use std::sync::{Arc, Mutex};

const WAITING: u8 = 0;
const CLAIMED: u8 = 1;
const DEAD: u8 = 2;

/// Mirror of `ClaimToken` — same states, same CASes, same orderings.
struct Token(AtomicU8);

impl Token {
    fn try_claim(&self) -> bool {
        self.0
            .compare_exchange(WAITING, CLAIMED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
    fn is_open(&self) -> bool {
        self.0.load(Ordering::Acquire) == WAITING
    }
    /// `true` = Dead, `false` = Claimed.
    fn settle(&self) -> bool {
        match self
            .0
            .compare_exchange(WAITING, DEAD, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => true,
            Err(CLAIMED) => false,
            Err(_) => true,
        }
    }
}

struct World {
    token: Token,
    /// Elements in each owner's key (each owner is the only writer of its own).
    store: [AtomicUsize; 2],
    /// Each owner's reply channel to the waiter (a flume oneshot in moon).
    reply: [Mutex<Option<usize>>; 2],
    /// How many owners ever sent — invariant 1.
    sends: AtomicUsize,
}

impl World {
    fn new() -> Self {
        World {
            token: Token(AtomicU8::new(WAITING)),
            store: [AtomicUsize::new(1), AtomicUsize::new(1)],
            reply: [Mutex::new(None), Mutex::new(None)],
            sends: AtomicUsize::new(0),
        }
    }

    /// `try_wake_list_waiter` + `deliver`, for owner `i`.
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
        // ... claim second, with the element in hand.
        if self.token.try_claim() {
            self.sends.fetch_add(1, Ordering::Relaxed);
            *self.reply[i].lock().unwrap() = Some(i);
        } else {
            // Lost: put it back in the same stretch.
            self.store[i].store(had, Ordering::Relaxed);
        }
    }

    /// `blocking_multikey::settle` for a wait that ended without a reply.
    /// Returns the committed reply, if one exists.
    fn waiter_settle(&self) -> Option<usize> {
        if self.token.settle() {
            // Dead: nothing was sent, nothing can be.
            return None;
        }
        // Claimed: the winner sends in the stretch it claimed in.
        loop {
            for r in &self.reply {
                if let Some(v) = r.lock().unwrap().take() {
                    return Some(v);
                }
            }
            thread_yield();
        }
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

/// Two owners serve while the waiter gives up.
fn model_two_owners_race_a_waiter_that_gives_up() {
    let w = Arc::new(World::new());
    let owners: Vec<_> = (0..2)
        .map(|i| {
            let w = Arc::clone(&w);
            spawn(move || w.owner_wake(i))
        })
        .collect();
    let delivered = w.waiter_settle();
    for o in owners {
        o.join().unwrap();
    }
    let sends = w.sends.load(Ordering::Relaxed);
    assert!(sends <= 1, "two owners served one waiter");
    match delivered {
        None => assert_eq!(sends, 0, "settled Dead yet an owner sent"),
        Some(_) => assert_eq!(sends, 1),
    }
    // No owner can send after the waiter settled Dead.
    for i in 0..2 {
        w.owner_wake(i);
    }
    assert_eq!(
        w.sends.load(Ordering::Relaxed),
        sends,
        "a send after settle"
    );
    // Leftover replies: only the one the waiter took may ever have existed.
    let leftover = w
        .reply
        .iter()
        .filter(|r| r.lock().unwrap().is_some())
        .count();
    assert_eq!(leftover, 0, "a reply was left behind in a receiver");
    let left: usize = w.store.iter().map(|s| s.load(Ordering::Relaxed)).sum();
    assert_eq!(
        left + usize::from(delivered.is_some()),
        2,
        "an element was destroyed"
    );
}

/// The waiter is served normally (never settles): exactly one owner wins,
/// and the loser's element is back in its key.
fn model_two_owners_serve_a_live_waiter_once() {
    let w = Arc::new(World::new());
    let owners: Vec<_> = (0..2)
        .map(|i| {
            let w = Arc::clone(&w);
            spawn(move || w.owner_wake(i))
        })
        .collect();
    for o in owners {
        o.join().unwrap();
    }
    assert_eq!(w.sends.load(Ordering::Relaxed), 1, "exactly one serve");
    let left: usize = w.store.iter().map(|s| s.load(Ordering::Relaxed)).sum();
    assert_eq!(left, 1, "the losing owner restored its element");
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
