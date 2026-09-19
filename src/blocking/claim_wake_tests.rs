//! The claim token as the wakers use it (moon#1019, moon#1023), and what a
//! waker owes a waiter it cannot serve: no answer, no lost element, no lost
//! TTL.
//!
//! Kept out of `claim.rs` on purpose: that file is compiled verbatim into
//! `tests/loom_blocking_claim.rs` (via `#[path]`), so it must not reach into
//! the rest of the crate.

use bytes::Bytes;

use crate::blocking::wakeup::{
    WakeUndo, deliver, expiry_of, try_wake_list_waiter, try_wake_zset_waiter,
};
use crate::blocking::{
    BlockedCommand, BlockingRegistry, ClaimToken, Direction, Settled, WaitEntry,
};
use crate::protocol::Frame;
use crate::runtime::channel::{self, OneshotReceiver};
use crate::storage::Database;

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

/// A waker run on a key that holds nothing — a key deleted in between, a
/// caller that cannot promise a push just happened — must answer NOBODY. It
/// used to pop the first parked waiter, find nothing to give it, and answer
/// it nil; for `BLPOP k 0` that is a reply redis never sends.
#[test]
fn a_wake_on_an_absent_key_answers_nobody() {
    for family in ["list", "zset"] {
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        let cmd = if family == "list" {
            BlockedCommand::BLPop
        } else {
            BlockedCommand::BZPopMin
        };
        let rx = park(&mut reg, "k", cmd, Some(ClaimToken::new()));
        let woke = if family == "list" {
            try_wake_list_waiter(&mut reg, &mut db, 0, &b("k"))
        } else {
            try_wake_zset_waiter(&mut reg, &mut db, 0, &b("k"))
        };
        assert!(!woke, "{family}");
        assert!(
            matches!(rx.try_recv(), Err(flume::TryRecvError::Empty)),
            "{family}: a parked waiter was answered from an absent key"
        );
        assert!(reg.has_waiters(0, &b("k")), "{family}: and stays parked");
    }
}

/// A waiter parked on a key that now holds ANOTHER type is not the waker's
/// to answer. `BZPOPMIN k 0` parks on an absent `k`; `RPUSH k x y` creates it
/// as a list; any later zset wake on `k` (a group registration or a
/// `BlockRegister` runs every waker whenever the key exists) must leave the
/// waiter parked — answering it nil is a reply redis never gives a
/// timeout-0 waiter. The same holds the other way round.
#[test]
fn a_waiter_of_another_family_is_left_parked_on_a_wrong_typed_key() {
    for family in ["zset waiter on a list", "list waiter on a zset"] {
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        let k = b("k");
        let (cmd, woke) = if family == "zset waiter on a list" {
            db.list_push_back(&k, b("x"));
            db.list_push_back(&k, b("y"));
            (BlockedCommand::BZPopMin, false)
        } else {
            db.zset_restore(&k, b("m"), 1.0);
            (BlockedCommand::BLPop, false)
        };
        let rx = park(&mut reg, "k", cmd, Some(ClaimToken::new()));
        let got = if family == "zset waiter on a list" {
            try_wake_zset_waiter(&mut reg, &mut db, 0, &k)
        } else {
            try_wake_list_waiter(&mut reg, &mut db, 0, &k)
        };
        assert_eq!(got, woke, "{family}");
        assert!(
            matches!(rx.try_recv(), Err(flume::TryRecvError::Empty)),
            "{family}: the waiter was answered from a key of another type"
        );
        assert!(reg.has_waiters(0, &k), "{family}: and must stay parked");
        if family == "zset waiter on a list" {
            assert_eq!(list(&mut db, "k"), vec![b("x"), b("y")], "{family}");
        }
    }
}

/// A queue can hold several waiters of the wrong family; the waker leaves
/// every one of them parked, in order, and does not spin.
#[test]
fn every_waiter_of_another_family_stays_parked_in_order() {
    let mut reg = BlockingRegistry::new(0);
    let mut db = Database::new();
    let k = b("k");
    db.list_push_back(&k, b("x"));
    let first = park(&mut reg, "k", BlockedCommand::BZPopMin, None);
    let second = park(&mut reg, "k", BlockedCommand::BZPopMax, None);
    assert!(!try_wake_zset_waiter(&mut reg, &mut db, 0, &k));
    assert!(matches!(first.try_recv(), Err(flume::TryRecvError::Empty)));
    assert!(matches!(second.try_recv(), Err(flume::TryRecvError::Empty)));
    let queued: Vec<bool> = reg
        .waiters_on(0, &k)
        .map(|q| {
            q.iter()
                .map(|e| matches!(e.cmd, BlockedCommand::BZPopMin))
                .collect()
        })
        .unwrap_or_default();
    assert_eq!(queued, vec![true, false], "both still queued, FIFO intact");
}

/// A shard pops for a waiter, the pop EMPTIES the key (removing
/// it), and then the shard loses the waiter's claim. The put-back must
/// recreate the key WITH its TTL, or the master keeps forever a key every
/// replica expires. Driven through the real lost-claim path, `deliver`.
#[test]
fn a_lost_claim_put_back_keeps_the_ttl() {
    for family in ["list", "zset"] {
        let mut db = Database::new();
        let k = b("k");
        let expires = db.now_ms() + 60_000;
        if family == "list" {
            db.list_push_back(&k, b("v"));
        } else {
            db.zset_restore(&k, b("m"), 1.0);
        }
        assert!(db.set_expiry(&k, expires));

        // What the waker does: read the TTL, pop, build the reply and undo.
        let ttl = expiry_of(&mut db, &k);
        let (frame, undo) = if family == "list" {
            let v = db.list_pop_front(&k).expect("v");
            (
                Frame::BulkString(v.clone()),
                WakeUndo::ListFront(smallvec::smallvec![v]),
            )
        } else {
            let (m, s) = db.zset_pop_min(&k).expect("m");
            (
                Frame::BulkString(m.clone()),
                WakeUndo::Zset(smallvec::smallvec![(m, s)]),
            )
        };
        assert!(!db.exists(&k), "{family}: the pop emptied the key");

        // Another shard has already won this waiter.
        let claim = ClaimToken::new();
        assert!(claim.clone().try_claim());
        let (tx, rx) = channel::oneshot();
        let served = deliver(&mut db, &k, tx, Some(&claim), frame, Some(undo), ttl);

        assert!(!served, "{family}: a lost claim serves nothing");
        assert!(rx.try_recv().is_err(), "{family}: and sends nothing");
        assert!(db.exists(&k), "{family}: the element is back");
        assert_eq!(
            expiry_of(&mut db, &k),
            expires,
            "{family}: the put-back dropped the TTL"
        );
    }
}

/// A shard WINS the claim but the send fails — the waiter's receiver is gone
/// (its connection task was torn down without settling). Nobody holds the
/// element, so `deliver` must put it back, TTL and all, exactly as for a lost
/// claim. The loom model (`tests/loom_blocking_claim.rs`) covers the same path
/// against the interleavings; this drives the real function.
#[test]
fn a_won_claim_whose_send_fails_puts_the_element_back() {
    let mut db = Database::new();
    let k = b("k");
    db.list_push_back(&k, b("v"));
    let expires = db.now_ms() + 60_000;
    assert!(db.set_expiry(&k, expires));
    let ttl = expiry_of(&mut db, &k);
    let v = db.list_pop_front(&k).expect("v");
    assert!(!db.exists(&k));

    let claim = ClaimToken::new();
    let (tx, rx) = channel::oneshot();
    drop(rx);
    let served = deliver(
        &mut db,
        &k,
        tx,
        Some(&claim),
        Frame::BulkString(v.clone()),
        Some(WakeUndo::ListFront(smallvec::smallvec![v])),
        ttl,
    );

    assert!(!served, "a failed send serves nobody");
    assert!(!claim.is_open(), "the claim was won");
    assert_eq!(list(&mut db, "k"), vec![b("v")], "the element is back");
    assert_eq!(expiry_of(&mut db, &k), expires, "with its TTL");
}

/// A key that SURVIVES the pop keeps its own TTL, and the put-back leaves it
/// alone rather than rewriting it.
#[test]
fn a_put_back_into_a_surviving_key_leaves_its_ttl_alone() {
    let mut db = Database::new();
    let k = b("k");
    db.list_push_back(&k, b("a"));
    db.list_push_back(&k, b("b"));
    let expires = db.now_ms() + 60_000;
    assert!(db.set_expiry(&k, expires));
    let ttl = expiry_of(&mut db, &k);
    let v = db.list_pop_front(&k).expect("a");
    WakeUndo::ListFront(smallvec::smallvec![v]).restore_keeping_ttl(&mut db, &k, ttl);
    assert_eq!(list(&mut db, "k"), vec![b("a"), b("b")]);
    assert_eq!(expiry_of(&mut db, &k), expires);
}

/// A shard that pops and then LOSES the claim puts the element back in the
/// same stretch, in its original position.
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

/// Waiters settled elsewhere are skipped WITHOUT a pop, and the push goes to
/// the next live waiter — list and zset alike.
#[test]
fn wakers_skip_settled_waiters_without_consuming() {
    for family in ["list", "zset"] {
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        let claimed = ClaimToken::new();
        assert!(claimed.try_claim());
        let dead = ClaimToken::new();
        assert_eq!(dead.settle(), Settled::Dead);
        let cmd = || {
            if family == "list" {
                BlockedCommand::BLPop
            } else {
                BlockedCommand::BZPopMin
            }
        };
        let rx_claimed = park(&mut reg, "k", cmd(), Some(claimed));
        let rx_dead = park(&mut reg, "k", cmd(), Some(dead));
        let rx_live = park(&mut reg, "k", cmd(), Some(ClaimToken::new()));
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

/// A BLMOVE put-back reverses the move only if the element is still where the
/// move put it; otherwise the move stands — never another client's element.
#[test]
fn a_blmove_put_back_never_takes_someone_elses_element() {
    let (src, dst) = (b("src"), b("dst"));
    let undo = || WakeUndo::Moved {
        destination: dst.clone(),
        wherefrom: Direction::Right,
        whereto: Direction::Left,
        value: b("moved"),
    };

    let mut db = Database::new();
    db.list_push_front(&dst, b("moved"));
    undo().restore(&mut db, &src);
    assert_eq!(list(&mut db, "src"), vec![b("moved")]);
    assert!(list(&mut db, "dst").is_empty());

    let mut db = Database::new();
    db.list_push_front(&dst, b("moved"));
    db.list_push_front(&dst, b("later"));
    undo().restore(&mut db, &src);
    assert!(list(&mut db, "src").is_empty(), "the move stands");
    assert_eq!(list(&mut db, "dst"), vec![b("later"), b("moved")]);
}
