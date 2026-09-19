//! One waiter, several shards, exactly one serve (moon#1019, moon#1023).
//!
//! A blocking pop whose keys live on more than one thread is registered on
//! each of them — its own shard for the keys it owns, one owner shard per
//! remote run of keys. Each registration can be woken independently, on its
//! own thread, and before this token existed each one simply served: two
//! owners with data both popped, and the client kept one reply while the
//! other element was dropped with its receiver.
//!
//! A [`ClaimToken`] is shared by every registration of one waiter. Serving is
//! a two-party race decided by one compare-and-swap on it:
//!
//! ```text
//!                 try_claim (a shard, element already popped)
//!     WAITING  ──────────────────────────────────────────────▶  CLAIMED
//!        │
//!        │  settle (the waiter: timeout, shutdown, vanished peer)
//!        ▼
//!      DEAD
//! ```
//!
//! Both terminal states are final, and each is entered by exactly one CAS
//! from `WAITING`, so exactly one of "a shard serves" and "the waiter gives
//! up" happens:
//!
//! * a shard pops FIRST and claims SECOND, with the element in hand. A lost
//!   claim puts the element back in the same synchronous stretch of that
//!   shard's event loop, so no other client can observe the round trip; a won
//!   claim sends the reply in that same stretch. There is no "claimed but
//!   nothing to send" state to release, which is what keeps the machine at
//!   three states and one CAS per side.
//! * the waiter, when its wait ends WITHOUT a reply, settles the token. `DEAD`
//!   means no shard has served and none ever will, so every receiver can be
//!   dropped as it is — this is what makes the post-cancel drain moon#1023
//!   asked for unnecessary. `CLAIMED` means a serve is committed and its reply
//!   is already in flight on exactly one receiver, so the waiter must take it
//!   (and deliver it, or, if its client is gone, log it as served) rather
//!   than drop it.
//!
//! A waiter whose every key lives on its own shard needs none of this — one
//! thread serves it, and `remove_wait` unregisters its siblings before the
//! next wake can run — so it carries no token and pays nothing.
//!
//! The model is in `tests/loom_blocking_claim.rs`, which compiles THIS file
//! (through `#[path]`) against loom's atomics. So this file must stay
//! self-contained: `std`/`loom` only, nothing from the rest of the crate.

#[cfg(loom)]
use loom::sync::Arc;
#[cfg(loom)]
use loom::sync::atomic::{AtomicU8, Ordering};
#[cfg(not(loom))]
use std::sync::Arc;
#[cfg(not(loom))]
use std::sync::atomic::{AtomicU8, Ordering};

const WAITING: u8 = 0;
const CLAIMED: u8 = 1;
const DEAD: u8 = 2;

/// Shared single-winner claim on one blocked waiter. Cheap to clone: one
/// `Arc` per waiter, cloned once per registration message.
#[derive(Clone, Debug)]
pub struct ClaimToken(Arc<AtomicU8>);

/// How a waiter's [`ClaimToken::settle`] resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settled {
    /// Nobody served the waiter and nobody ever will.
    Dead,
    /// A shard won the claim; its reply is (or is about to be) in one of the
    /// waiter's receivers and must be taken, not dropped.
    Claimed,
}

impl ClaimToken {
    /// A fresh token in `WAITING`.
    pub fn new() -> Self {
        ClaimToken(Arc::new(AtomicU8::new(WAITING)))
    }

    /// Try to become the one shard that serves this waiter.
    ///
    /// Call with the reply already built and any element already popped, and
    /// send the reply in the same synchronous stretch on success. On failure
    /// the caller must put back what it popped and treat the waiter as gone
    /// from this shard.
    #[must_use]
    pub fn try_claim(&self) -> bool {
        self.0
            .compare_exchange(WAITING, CLAIMED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Still unclaimed and alive — a shard may still serve it.
    ///
    /// Advisory: the answer can go stale the instant it is returned. It lets
    /// a waker skip a settled waiter WITHOUT touching the datastore (the
    /// common case); [`try_claim`](Self::try_claim) is the decision.
    pub fn is_open(&self) -> bool {
        self.0.load(Ordering::Acquire) == WAITING
    }

    /// The waiter gives up: close the token to every future claim, or learn
    /// that a shard already won it. Idempotent.
    #[must_use]
    pub fn settle(&self) -> Settled {
        match self
            .0
            .compare_exchange(WAITING, DEAD, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Settled::Dead,
            Err(CLAIMED) => Settled::Claimed,
            Err(_) => Settled::Dead,
        }
    }
}

impl Default for ClaimToken {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn first_claim_wins_and_the_rest_lose() {
        let t = ClaimToken::new();
        let peer = t.clone();
        assert!(t.is_open());
        assert!(t.try_claim());
        assert!(!peer.try_claim(), "a second shard must lose");
        assert!(!t.is_open());
        assert_eq!(peer.settle(), Settled::Claimed);
        assert_eq!(peer.settle(), Settled::Claimed, "settle is idempotent");
    }

    #[test]
    fn a_settled_waiter_can_never_be_claimed() {
        let t = ClaimToken::new();
        assert_eq!(t.settle(), Settled::Dead);
        assert!(!t.is_open());
        assert!(!t.clone().try_claim());
        assert_eq!(t.settle(), Settled::Dead, "settle is idempotent");
    }

    /// Hammer: many shards race one waiter that gives up concurrently.
    /// Exactly one side wins, every time.
    #[test]
    fn claim_and_settle_race_has_exactly_one_winner() {
        for _ in 0..2_000 {
            let t = ClaimToken::new();
            let shards: Vec<_> = (0..3)
                .map(|_| {
                    let t = t.clone();
                    std::thread::spawn(move || t.try_claim())
                })
                .collect();
            let settled = t.settle();
            let wins = shards
                .into_iter()
                .map(|h| h.join().unwrap_or(false))
                .filter(|w| *w)
                .count();
            match settled {
                Settled::Dead => assert_eq!(wins, 0),
                Settled::Claimed => assert_eq!(wins, 1),
            }
        }
    }
}
