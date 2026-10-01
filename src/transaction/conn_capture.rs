//! The undo capture of a connection's write inside an open `TXN`, shared by
//! both runtimes' generic write legs (moon#500, moon#1299, moon#1303).
//!
//! Before dispatch, [`capture_conn_write`]:
//! 1. refuses the write when another open transaction holds one of the keys
//!    it may write ([`isolation::check_write`], moon#1299) — nothing is
//!    captured and the transaction is poisoned (#499);
//! 2. records each written key's pre-image in the undo log, a write intent
//!    (read visibility, moon#807) and a hold (write isolation, moon#1299).
//!
//! Dispatch then runs inside the returned capture's [`OwnerScope`], so the
//! dispatch-level check admits this transaction's own keys.
//!
//! After dispatch, [`ConnWriteCapture::finish`] takes the capture back when
//! the write answered an error (moon#1303): an error reply means nothing was
//! written — the AOF's rule too (`serialize_effect_for_log` logs nothing for
//! one) — so its pre-images are truncated off the undo log, each key's
//! previous write intent is restored (`KvWriteIntents::record_write` returns
//! it) and the holds it created are released. Kept, `TXN.ABORT` restored a
//! stale pre-image over whatever another client wrote meanwhile
//! (`SET k v BADOPT`, `INCR` of a non-number, `WRONGTYPE`), and the key
//! stayed locked for a write that never happened. The script leg's twin is
//! `scripting::bridge::txn_capture::txn_undo_discard`.

use bytes::Bytes;
use smallvec::SmallVec;

use crate::protocol::Frame;
use crate::storage::Database;
use crate::transaction::isolation::{self, OwnerScope};
use crate::transaction::{CrossStoreTxn, KvWriteIntents, WriteIntent};

/// One key a connection write captured.
struct CapturedKey {
    db: usize,
    key: Bytes,
    /// The key's write intent before this write recorded its own.
    prev_intent: Option<WriteIntent>,
    /// This write created the transaction's hold on the key.
    newly_held: bool,
}

/// What one connection write inside a TXN captured; alive across its
/// dispatch (the [`OwnerScope`]).
#[must_use = "finish() takes an erroring write's capture back"]
pub(crate) struct ConnWriteCapture {
    undo_len: usize,
    keys: SmallVec<[CapturedKey; 4]>,
    _owner: OwnerScope,
}

/// Capture `cmd args`, about to run in database `sel_db` (`db`) inside
/// `txn`. `Err(reply)`: refused (another transaction holds a key it may
/// write); nothing was captured, `txn` is poisoned, and `reply` is the
/// client's answer.
pub(crate) fn capture_conn_write(
    txn: &mut CrossStoreTxn,
    intents: &mut KvWriteIntents,
    db: &mut Database,
    sel_db: usize,
    cmd: &[u8],
    args: &[Frame],
) -> Result<ConnWriteCapture, Frame> {
    let owner = OwnerScope::enter(txn.txn_id);
    if let Some(refused) = isolation::check_write(sel_db, cmd, args) {
        txn.record_rejected_op(cmd);
        return Err(refused);
    }
    let mut capture = ConnWriteCapture {
        undo_len: txn.kv_undo.len(),
        keys: SmallVec::new(),
        _owner: owner,
    };
    let (lsn, tid) = (txn.snapshot_lsn, txn.txn_id);
    // `pre` is the key's before-image, recorded in the undo log; a NEW hold
    // also keeps a copy for snapshots (moon#1300 — `isolation::hold`).
    let mut note = |key: Bytes, pre: &Option<crate::storage::entry::Entry>| {
        let prev_intent = intents.record_write(key.clone(), lsn, tid);
        let newly_held = isolation::hold(sel_db, &key, tid, || pre.clone());
        capture.keys.push(CapturedKey {
            db: sel_db,
            key,
            prev_intent,
            newly_held,
        });
    };
    if cmd.eq_ignore_ascii_case(b"DEL") || cmd.eq_ignore_ascii_case(b"UNLINK") {
        // A missing key's DEL writes nothing: no pre-image, no intent.
        for arg in args {
            if let Frame::BulkString(key) = arg
                && let pre @ Some(_) = db.get(key.as_ref()).cloned()
            {
                note(key.clone(), &pre);
                if let Some(old_entry) = pre {
                    txn.kv_undo.record_delete(sel_db, key.clone(), old_entry);
                }
            }
        }
    } else {
        // moon#500: every WRITE position of the shared key walker, not just
        // the primary key (`extract_primary_key` rolled back only the first
        // key of an `MSET` / `BITOP` / `SINTERSTORE`). Reads are NOT
        // captured: a read key would be held and hidden from other
        // transactions for nothing. An argv the walker cannot read falls
        // back to the primary key; a write-free one (`SORT src`) captures
        // nothing — see `conn_txn_capture_keys`.
        for key in crate::transaction::conn_txn_capture_keys(cmd, args) {
            let pre = db.get(key.as_ref()).cloned();
            note(key.clone(), &pre);
            match pre {
                None => txn.kv_undo.record_insert(sel_db, key),
                Some(entry) => txn.kv_undo.record_update(sel_db, key, entry),
            }
        }
    }
    Ok(capture)
}

impl ConnWriteCapture {
    /// After dispatch: an erroring write takes its capture back (moon#1303).
    /// Ends the owner scope either way.
    pub(crate) fn finish(
        self,
        reply_is_error: bool,
        txn: &mut CrossStoreTxn,
        intents: &mut KvWriteIntents,
    ) {
        if !reply_is_error {
            return;
        }
        txn.kv_undo.truncate(self.undo_len);
        // Newest first: a key captured twice by one write (`MSET k 1 k 2`)
        // ends at the intent it had before the write.
        for k in self.keys.into_iter().rev() {
            if k.newly_held {
                isolation::unhold(k.db, &k.key, txn.txn_id);
            }
            intents.restore(k.key, k.prev_intent);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frames(parts: &[&str]) -> Vec<Frame> {
        parts
            .iter()
            .map(|p| Frame::BulkString(Bytes::from(p.to_string())))
            .collect()
    }

    fn run(db: &mut Database, cmd: &str, parts: &[&str]) -> Frame {
        let mut sel = 0;
        match crate::command::dispatch(db, cmd.as_bytes(), &frames(parts), &mut sel, 16) {
            crate::command::DispatchResult::Response(f)
            | crate::command::DispatchResult::Quit(f) => f,
        }
    }

    /// Capture, dispatch, finish — the handlers' sequence.
    fn txn_write(
        txn: &mut CrossStoreTxn,
        intents: &mut KvWriteIntents,
        db: &mut Database,
        cmd: &str,
        parts: &[&str],
    ) -> Frame {
        let args = frames(parts);
        match capture_conn_write(txn, intents, db, 0, cmd.as_bytes(), &args) {
            Err(refused) => refused,
            Ok(capture) => {
                let reply = run(db, cmd, parts);
                capture.finish(matches!(reply, Frame::Error(_)), txn, intents);
                reply
            }
        }
    }

    fn on_fresh_thread(f: impl FnOnce() + Send + 'static) {
        std::thread::spawn(f).join().expect("test thread");
    }

    /// moon#1303: `SET k v BADOPT`, `INCR` of a non-number and a WRONGTYPE
    /// write each answer an error and keep nothing: no undo record, no
    /// intent, no hold.
    #[test]
    fn an_erroring_write_takes_its_capture_back() {
        on_fresh_thread(|| {
            let mut db = Database::new();
            run(&mut db, "SET", &["n", "abc"]);
            run(&mut db, "RPUSH", &["l", "x"]);
            let mut txn = CrossStoreTxn::new(5, 4, 0);
            isolation::txn_begin(5);
            let mut intents = KvWriteIntents::new();
            for (cmd, parts) in [
                ("SET", &["k", "v", "BADOPT"][..]),
                ("INCR", &["n"][..]),
                ("SET", &["l", "v", "GET"][..]),
                ("LPUSH", &["n", "x"][..]),
            ] {
                let reply = txn_write(&mut txn, &mut intents, &mut db, cmd, parts);
                assert!(
                    matches!(reply, Frame::Error(_)),
                    "{cmd} {parts:?}: {reply:?}"
                );
            }
            assert!(txn.kv_undo.is_empty(), "{:?}", txn.kv_undo);
            assert!(intents.is_empty());
            assert!(!isolation::any_held());
            assert!(!txn.is_dirty(), "a command error is not a guard refusal");
            isolation::txn_end(5);
        });
    }

    /// An erroring write of a key the transaction ALREADY wrote keeps the
    /// earlier capture, intent and hold.
    #[test]
    fn an_erroring_rewrite_keeps_the_earlier_capture() {
        on_fresh_thread(|| {
            let mut db = Database::new();
            let mut txn = CrossStoreTxn::new(9, 8, 0);
            isolation::txn_begin(9);
            let mut intents = KvWriteIntents::new();
            assert_eq!(
                txn_write(&mut txn, &mut intents, &mut db, "SET", &["k", "a"]),
                Frame::SimpleString(Bytes::from_static(b"OK"))
            );
            let reply = txn_write(&mut txn, &mut intents, &mut db, "INCR", &["k"]);
            assert!(matches!(reply, Frame::Error(_)));
            assert_eq!(txn.kv_undo.len(), 1);
            assert_eq!(intents.get(b"k").map(|i| i.txn_id), Some(9));
            assert!(isolation::is_held(0, b"k"));
            // A duplicate key in one erroring write restores the same state.
            let reply = txn_write(&mut txn, &mut intents, &mut db, "MSET", &["j", "1", "j"]);
            assert!(matches!(reply, Frame::Error(_)));
            assert!(intents.get(b"j").is_none());
            assert!(!isolation::is_held(0, b"j"));
            isolation::txn_end(9);
            assert!(!isolation::any_held());
        });
    }

    /// moon#1299: a key another transaction holds is refused before any
    /// capture, and the refusal poisons the writer's transaction.
    #[test]
    fn a_key_held_by_another_transaction_is_refused_uncaptured() {
        on_fresh_thread(|| {
            let mut db = Database::new();
            let mut a = CrossStoreTxn::new(1, 0, 0);
            let mut b = CrossStoreTxn::new(2, 1, 0);
            isolation::txn_begin(1);
            isolation::txn_begin(2);
            let mut intents = KvWriteIntents::new();
            txn_write(&mut a, &mut intents, &mut db, "SET", &["k", "a"]);
            let reply = txn_write(&mut b, &mut intents, &mut db, "SET", &["k", "b"]);
            assert!(isolation::is_conflict_reply(&reply), "{reply:?}");
            assert!(b.kv_undo.is_empty());
            assert!(b.is_dirty());
            assert_eq!(intents.get(b"k").map(|i| i.txn_id), Some(1));
            assert_eq!(
                run(&mut db, "GET", &["k"]),
                Frame::BulkString(Bytes::from("a"))
            );
            isolation::txn_end(1);
            isolation::txn_end(2);
        });
    }
}
