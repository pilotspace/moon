//! The undo capture of a connection's write inside an open `TXN`, shared by
//! both runtimes' generic write legs (moon#500, moon#1299, moon#1303).
//!
//! Before dispatch, [`capture_conn_write`]:
//! 1. refuses the write when another open transaction holds one of the keys
//!    it may write ([`isolation::check_write`], moon#1299) — nothing is
//!    captured and the transaction is poisoned (#499);
//! 2. records each written key's undo record, a write intent (read
//!    visibility, moon#807) and a hold (write isolation, moon#1299). The hold
//!    owns the key's pre-transaction image — the only copy (R2b W2): the
//!    undo log names the key (`UndoRecord::Held`) and the abort restores the
//!    hold's image ([`capture_key`]).
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
    let deleting = cmd.eq_ignore_ascii_case(b"DEL") || cmd.eq_ignore_ascii_case(b"UNLINK");
    if deleting {
        // A missing key's DEL writes nothing: no pre-image, no intent.
        for arg in args {
            if let Frame::BulkString(key) = arg
                && db.get(key.as_ref()).is_some()
            {
                capture_key(&mut capture, txn, intents, db, sel_db, key.clone(), true);
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
            capture_key(&mut capture, txn, intents, db, sel_db, key, false);
        }
    }
    Ok(capture)
}

/// Capture one key `cmd` may write (`deleting`: a `DEL` / `UNLINK` of a
/// present key): its write intent, its hold and its undo record.
///
/// R2b W2: ONE copy of the key's pre-transaction value. Its first write
/// clones the present value once and MOVES it into the new hold
/// (`isolation::hold`), which every snapshot serializes and the abort
/// restores from; the undo log records [`UndoRecord::Held`] (key and kind
/// only). A later write of a key the transaction already holds copies
/// nothing: the abort restores the first record only, and the commit's WAL
/// image needs just the key and kind. An absent key is an
/// [`UndoRecord::Insert`], as before.
///
/// [`UndoRecord::Held`]: crate::transaction::UndoRecord::Held
/// [`UndoRecord::Insert`]: crate::transaction::UndoRecord::Insert
fn capture_key(
    capture: &mut ConnWriteCapture,
    txn: &mut CrossStoreTxn,
    intents: &mut KvWriteIntents,
    db: &mut Database,
    sel_db: usize,
    key: Bytes,
    deleting: bool,
) {
    let (lsn, tid) = (txn.snapshot_lsn, txn.txn_id);
    let prev_intent = intents.record_write(key.clone(), lsn, tid);
    let newly_held = if isolation::holder(sel_db, &key) == Some(tid) {
        txn.kv_undo.record_held(sel_db, key.clone(), deleting);
        false
    } else {
        let pre = db.get(key.as_ref()).cloned();
        let present = pre.is_some();
        let newly = isolation::hold(sel_db, &key, tid, pre);
        if present {
            txn.kv_undo.record_held(sel_db, key.clone(), deleting);
        } else {
            txn.kv_undo.record_insert(sel_db, key.clone());
        }
        newly
    };
    capture.keys.push(CapturedKey {
        db: sel_db,
        key,
        prev_intent,
        newly_held,
    });
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

    /// R2b W2: a key's pre-transaction value is kept ONCE, by the hold. The
    /// undo log names the key (`Held`: key and kind, no image) on its first
    /// write and on every later one; the abort restores the hold's image.
    #[test]
    fn a_held_keys_image_is_kept_once_and_restored_from_the_hold() {
        use crate::transaction::UndoRecord;
        on_fresh_thread(|| {
            let mut db = Database::new();
            run(&mut db, "SET", &["k", "orig"]);
            let mut txn = CrossStoreTxn::new(7, 6, 0);
            isolation::txn_begin(7);
            let mut intents = KvWriteIntents::new();
            for (cmd, parts) in [
                ("SET", &["k", "a"][..]),
                ("APPEND", &["k", "b"][..]),
                ("DEL", &["k"][..]),
                ("SET", &["n", "new"][..]),
                ("SET", &["n", "again"][..]),
            ] {
                let reply = txn_write(&mut txn, &mut intents, &mut db, cmd, parts);
                assert!(!matches!(reply, Frame::Error(_)), "{cmd}: {reply:?}");
            }
            let kinds: Vec<_> = txn
                .kv_undo
                .records()
                .iter()
                .map(|r| match r {
                    UndoRecord::Held { key, deleted } => (key.clone(), "held", *deleted),
                    UndoRecord::Insert { key } => (key.clone(), "insert", false),
                    UndoRecord::Update { .. } | UndoRecord::Delete { .. } => {
                        panic!("a connection write copies no image into the undo log: {r:?}")
                    }
                })
                .collect();
            let (k, n) = (Bytes::from_static(b"k"), Bytes::from_static(b"n"));
            assert_eq!(
                kinds,
                vec![
                    (k.clone(), "held", false),
                    (k.clone(), "held", false),
                    (k.clone(), "held", true),
                    (n.clone(), "insert", false),
                    (n, "held", false),
                ]
            );
            let held = isolation::with_held_pre(0, b"k", |p| {
                p.and_then(|e| e.value.as_bytes().map(<[u8]>::to_vec))
            });
            assert_eq!(held.as_deref(), Some(&b"orig"[..]));
            // The abort: the first record of each key, `k` from the hold.
            let mut out = Vec::new();
            let log = std::mem::take(&mut txn.kv_undo);
            for (d, record) in crate::transaction::kv_compensation::first_per_key(log) {
                crate::transaction::kv_compensation::undo_one(&mut db, d, record, &mut out);
            }
            assert_eq!(
                run(&mut db, "GET", &["k"]),
                Frame::BulkString(Bytes::from("orig"))
            );
            assert!(db.get(b"n").is_none(), "the insert is undone");
            assert_eq!(out.len(), 2, "one compensating record per key");
            // The hold keeps its image until the release.
            assert!(isolation::is_held(0, b"k"));
            isolation::txn_end(7);
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
