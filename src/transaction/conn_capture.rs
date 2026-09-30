//! The undo capture of a connection's write inside an open `TXN`, shared by
//! both runtimes' generic write legs (moon#500, moon#1303).
//!
//! Before dispatch, [`capture_conn_write`] records each written key's
//! pre-image in the undo log and a write intent (read visibility, moon#807).
//!
//! After dispatch, [`ConnWriteCapture::finish`] takes the capture back when
//! the write answered an error (moon#1303): an error reply means nothing was
//! written — the AOF's rule too (`serialize_effect_for_log` logs nothing for
//! one) — so its pre-images are truncated off the undo log and each key's
//! previous write intent is restored (`KvWriteIntents::record_write` returns
//! it). Kept, `TXN.ABORT` restored a stale pre-image over whatever another
//! client wrote meanwhile (`SET k v BADOPT`, `INCR` of a non-number,
//! `WRONGTYPE`). The script leg's twin is
//! `scripting::bridge::txn_capture::txn_undo_discard`.

use bytes::Bytes;
use smallvec::SmallVec;

use crate::protocol::Frame;
use crate::storage::Database;
use crate::transaction::{CrossStoreTxn, KvWriteIntents, WriteIntent};

/// One key a connection write captured.
struct CapturedKey {
    key: Bytes,
    /// The key's write intent before this write recorded its own.
    prev_intent: Option<WriteIntent>,
}

/// What one connection write inside a TXN captured.
#[must_use = "finish() takes an erroring write's capture back"]
pub(crate) struct ConnWriteCapture {
    undo_len: usize,
    keys: SmallVec<[CapturedKey; 4]>,
}

/// Capture `cmd args`, about to run in database `sel_db` (`db`) inside
/// `txn`. `Err(reply)` is reserved for a refusal (none yet).
pub(crate) fn capture_conn_write(
    txn: &mut CrossStoreTxn,
    intents: &mut KvWriteIntents,
    db: &mut Database,
    sel_db: usize,
    cmd: &[u8],
    args: &[Frame],
) -> Result<ConnWriteCapture, Frame> {
    let mut capture = ConnWriteCapture {
        undo_len: txn.kv_undo.len(),
        keys: SmallVec::new(),
    };
    let (lsn, tid) = (txn.snapshot_lsn, txn.txn_id);
    let mut note = |key: Bytes| {
        let prev_intent = intents.record_write(key.clone(), lsn, tid);
        capture.keys.push(CapturedKey { key, prev_intent });
    };
    if cmd.eq_ignore_ascii_case(b"DEL") || cmd.eq_ignore_ascii_case(b"UNLINK") {
        // A missing key's DEL writes nothing: no pre-image, no intent.
        for arg in args {
            if let Frame::BulkString(key) = arg
                && let Some(old_entry) = db.get(key.as_ref()).cloned()
            {
                txn.kv_undo.record_delete(sel_db, key.clone(), old_entry);
                note(key.clone());
            }
        }
    } else {
        // moon#500: every WRITE position of the shared key walker, not just
        // the primary key (`extract_primary_key` rolled back only the first
        // key of an `MSET` / `BITOP` / `SINTERSTORE`). Reads are NOT
        // captured. An argv the walker cannot read falls back to the primary
        // key; a write-free one (`SORT src`) captures nothing — see
        // `conn_txn_capture_keys`.
        for key in crate::transaction::conn_txn_capture_keys(cmd, args) {
            match db.get(key.as_ref()).cloned() {
                None => txn.kv_undo.record_insert(sel_db, key.clone()),
                Some(entry) => txn.kv_undo.record_update(sel_db, key.clone(), entry),
            }
            note(key);
        }
    }
    Ok(capture)
}

impl ConnWriteCapture {
    /// After dispatch: an erroring write takes its capture back (moon#1303).
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

    /// moon#1303: `SET k v BADOPT`, `INCR` of a non-number and a WRONGTYPE
    /// write each answer an error and keep nothing: no undo record, no
    /// intent.
    #[test]
    fn an_erroring_write_takes_its_capture_back() {
        let mut db = Database::new();
        run(&mut db, "SET", &["n", "abc"]);
        run(&mut db, "RPUSH", &["l", "x"]);
        let mut txn = CrossStoreTxn::new(5, 4, 0);
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
        assert!(!txn.is_dirty(), "a command error is not a guard refusal");
    }

    /// An erroring write of a key the transaction ALREADY wrote keeps the
    /// earlier capture and intent.
    #[test]
    fn an_erroring_rewrite_keeps_the_earlier_capture() {
        let mut db = Database::new();
        let mut txn = CrossStoreTxn::new(9, 8, 0);
        let mut intents = KvWriteIntents::new();
        assert_eq!(
            txn_write(&mut txn, &mut intents, &mut db, "SET", &["k", "a"]),
            Frame::SimpleString(Bytes::from_static(b"OK"))
        );
        let reply = txn_write(&mut txn, &mut intents, &mut db, "INCR", &["k"]);
        assert!(matches!(reply, Frame::Error(_)));
        assert_eq!(txn.kv_undo.len(), 1);
        assert_eq!(intents.get(b"k").map(|i| i.txn_id), Some(9));
        // A duplicate key in one erroring write restores the same state.
        let reply = txn_write(&mut txn, &mut intents, &mut db, "MSET", &["j", "1", "j"]);
        assert!(matches!(reply, Frame::Error(_)));
        assert!(intents.get(b"j").is_none());
    }
}
