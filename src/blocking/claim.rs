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
//!   (and deliver it, or restore its element) rather than drop it.
//!
//! A waiter whose every key lives on its own shard needs none of this — one
//! thread serves it, and `remove_wait` unregisters its siblings before the
//! next wake can run — so it carries no token and pays nothing.
//!
//! The model is in `tests/loom_blocking_claim.rs`.

use std::sync::Arc;
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

#[cfg(test)]
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

    // ---- the token as the wakers use it -------------------------------

    use crate::blocking::wakeup::{
        WakeUndo, restore_and_rewake, try_wake_list_waiter, try_wake_zset_waiter,
    };
    use crate::blocking::{BlockedCommand, BlockingRegistry, Direction, WaitEntry};
    use crate::protocol::Frame;
    use crate::runtime::channel::{self, OneshotReceiver};
    use crate::storage::Database;
    use bytes::Bytes;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn park(
        reg: &mut BlockingRegistry,
        key: &str,
        cmd: BlockedCommand,
        claim: Option<ClaimToken>,
    ) -> OneshotReceiver<Option<Frame>> {
        let (tx, rx) = channel::oneshot();
        let wait_id = reg.next_wait_id();
        reg.register(
            0,
            b(key),
            WaitEntry {
                wait_id,
                cmd,
                reply_tx: tx,
                deadline: None,
                claim,
            },
        );
        rx
    }

    fn list(db: &mut Database, key: &str) -> Vec<Bytes> {
        let now_ms = db.now_ms();
        match db.get_list_ref_if_alive(&b(key), now_ms) {
            Ok(Some(l)) => l.iter_bytes(),
            _ => Vec::new(),
        }
    }

    /// A shard that pops and then LOSES the claim — the race the token
    /// exists for, where the other shard wins between this shard's
    /// `is_settled` check and its `try_claim` — puts the element back in the
    /// same stretch. Driven through `restore` directly, since a single thread
    /// cannot interleave the two.
    #[test]
    fn a_lost_claim_after_the_pop_restores_the_list_in_order() {
        let mut db = Database::new();
        for v in ["A", "B", "C"] {
            db.list_push_back(&b("k"), b(v));
        }
        let front = db.list_pop_front(&b("k")).expect("A");
        WakeUndo::ListFront(smallvec::smallvec![front]).restore(&mut db, &b("k"));
        let back = db.list_pop_back(&b("k")).expect("C");
        WakeUndo::ListBack(smallvec::smallvec![back]).restore(&mut db, &b("k"));
        assert_eq!(list(&mut db, "k"), vec![b("A"), b("B"), b("C")]);
    }

    /// Waiters settled elsewhere are skipped WITHOUT a pop, and the push goes
    /// to the next live waiter — list and zset alike.
    #[test]
    fn wakers_skip_settled_waiters_without_consuming() {
        for family in ["list", "zset"] {
            let mut reg = BlockingRegistry::new(0);
            let mut db = Database::new();
            let claimed = ClaimToken::new();
            assert!(claimed.try_claim());
            let dead = ClaimToken::new();
            assert_eq!(dead.settle(), Settled::Dead);
            let (cmd_a, cmd_b, cmd_c) = if family == "list" {
                (
                    BlockedCommand::BLPop,
                    BlockedCommand::BLPop,
                    BlockedCommand::BLPop,
                )
            } else {
                (
                    BlockedCommand::BZPopMin,
                    BlockedCommand::BZPopMin,
                    BlockedCommand::BZPopMin,
                )
            };
            let rx_claimed = park(&mut reg, "k", cmd_a, Some(claimed));
            let rx_dead = park(&mut reg, "k", cmd_b, Some(dead));
            let rx_live = park(&mut reg, "k", cmd_c, Some(ClaimToken::new()));
            let woke = if family == "list" {
                db.list_push_back(&b("k"), b("v"));
                try_wake_list_waiter(&mut reg, &mut db, 0, &b("k"))
            } else {
                db.zset_restore(&b("k"), b("m"), 1.0);
                try_wake_zset_waiter(&mut reg, &mut db, 0, &b("k"))
            };
            assert!(woke, "{family}");
            assert!(rx_claimed.try_recv().ok().flatten().is_none(), "{family}");
            assert!(rx_dead.try_recv().ok().flatten().is_none(), "{family}");
            assert!(rx_live.try_recv().ok().flatten().is_some(), "{family}");
            assert!(
                !db.exists(b"k"),
                "{family}: exactly one element, served once"
            );
        }
    }

    /// moon#1023: every destructive reply rebuilds the undo that puts its
    /// element back where it came from — scores included, bit for bit.
    #[test]
    fn replies_rebuild_their_undo() {
        let k = b("k");
        let keys = [k.clone()];
        // BLPOP / BRPOP.
        let mut db = Database::new();
        db.list_push_back(&k, b("x"));
        let frame = Frame::Array(crate::framevec![
            Frame::BulkString(k.clone()),
            Frame::BulkString(b("v")),
        ]);
        let (key, undo) =
            WakeUndo::from_reply(&BlockedCommand::BLPop, &keys, &frame).expect("BLPOP");
        undo.restore(&mut db, &key);
        let (key, undo) =
            WakeUndo::from_reply(&BlockedCommand::BRPop, &keys, &frame).expect("BRPOP");
        undo.restore(&mut db, &key);
        assert_eq!(list(&mut db, "k"), vec![b("v"), b("x"), b("v")]);

        // BLMPOP LEFT, two elements: back in their original order.
        let mut db = Database::new();
        let frame = Frame::Array(crate::framevec![
            Frame::BulkString(k.clone()),
            Frame::Array(crate::framevec![
                Frame::BulkString(b("a")),
                Frame::BulkString(b("b"))
            ]),
        ]);
        let cmd = BlockedCommand::BLMPop {
            dir: Direction::Left,
            count: 2,
        };
        let (key, undo) = WakeUndo::from_reply(&cmd, &keys, &frame).expect("BLMPOP");
        undo.restore(&mut db, &key);
        assert_eq!(list(&mut db, "k"), vec![b("a"), b("b")]);

        // BZPOPMIN with a score that only round-trips if parsed exactly.
        let score = 0.1_f64 + 0.2_f64;
        let mut db = Database::new();
        let frame = Frame::Array(crate::framevec![
            Frame::BulkString(k.clone()),
            Frame::BulkString(b("m")),
            Frame::BulkString(crate::command::sorted_set::format_score_bytes(score)),
        ]);
        let (key, undo) =
            WakeUndo::from_reply(&BlockedCommand::BZPopMin, &keys, &frame).expect("BZPOPMIN");
        undo.restore(&mut db, &key);
        assert_eq!(db.zset_pop_min(&k), Some((b("m"), score)));

        // Errors and nulls removed nothing.
        assert!(WakeUndo::from_reply(&BlockedCommand::BLPop, &keys, &Frame::NullArray).is_none());
    }

    /// A reply-derived BLMOVE restore reverses the move only if the element
    /// is still where the move put it; otherwise the move stands (the element
    /// is in the destination — never lost).
    #[test]
    fn a_blmove_restore_never_takes_someone_elses_element() {
        let (src, dst) = (b("src"), b("dst"));
        let cmd = BlockedCommand::BLMove {
            destination: dst.clone(),
            wherefrom: Direction::Right,
            whereto: Direction::Left,
        };
        let frame = Frame::BulkString(b("moved"));

        let mut db = Database::new();
        db.list_push_front(&dst, b("moved"));
        let (key, undo) =
            WakeUndo::from_reply(&cmd, std::slice::from_ref(&src), &frame).expect("BLMOVE");
        assert_eq!(key, src);
        undo.restore(&mut db, &key);
        assert_eq!(list(&mut db, "src"), vec![b("moved")]);
        assert!(list(&mut db, "dst").is_empty());

        let mut db = Database::new();
        db.list_push_front(&dst, b("moved"));
        db.list_push_front(&dst, b("later"));
        let (key, undo) =
            WakeUndo::from_reply(&cmd, std::slice::from_ref(&src), &frame).expect("BLMOVE");
        undo.restore(&mut db, &key);
        assert!(list(&mut db, "src").is_empty(), "the move stands");
        assert_eq!(list(&mut db, "dst"), vec![b("later"), b("moved")]);
    }

    /// A restore re-offers the element to whoever else is parked on the key.
    #[test]
    fn a_restore_feeds_the_next_parked_waiter() {
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        let rx = park(&mut reg, "k", BlockedCommand::BLPop, None);
        let undo = WakeUndo::ListFront(smallvec::smallvec![b("v")]);
        assert!(restore_and_rewake(&mut reg, &mut db, 0, &b("k"), undo));
        assert!(rx.try_recv().ok().flatten().is_some());
        assert!(!db.exists(b"k"));
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
