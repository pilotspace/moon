//! moon#894: a queued script's effect records, captured into the MULTI/EXEC
//! body's own record list.

use std::cell::RefCell;

use bytes::Bytes;

use crate::protocol::Frame;

/// One captured durability record: the db it executed in, and its serialized
/// command bytes (the same shape the MULTI/EXEC executor collects).
pub(crate) type CapturedEffect = (usize, Bytes);

thread_local! {
    /// moon#894: while a MULTI/EXEC body runs a queued script, that script's
    /// effect records land here instead of going straight to the AOF writer
    /// and the replication stream.
    ///
    /// Outside a transaction a script emits each effect the moment its
    /// `redis.call` succeeds, which is the right order because nothing else
    /// runs on the shard meanwhile. Inside `EXEC` the body's OTHER writes are
    /// collected and appended only after the whole body has run. A script
    /// emitting directly would therefore put its effects AHEAD of the writes
    /// queued before it: `SET k 1; EVAL "SET k 2"` would be logged `SET k 2;
    /// SET k 1`, and a restart or a replica would read `1` where the master
    /// answered `2`. Capturing puts the script's records into the body's list
    /// at the script's own position.
    ///
    /// `None` outside a capture. Only the synchronous executor arms it, and
    /// `capture_txn_effects` resets it on every exit path.
    static TXN_EFFECT_CAPTURE: RefCell<Option<Vec<CapturedEffect>>> = const { RefCell::new(None) };
}

/// Run `run` with this thread's script-effect emission diverted into a
/// buffer, and return that buffer in emission order (moon#894).
///
/// For the MULTI/EXEC executor only: `run` must be synchronous (no `.await`
/// can happen inside a closure), so no other connection's script can
/// interleave on this shard thread while the capture is armed.
pub(crate) fn capture_txn_effects<R>(run: impl FnOnce() -> R) -> (R, Vec<CapturedEffect>) {
    /// Disarms the capture even if `run` unwinds, so a panic cannot leave
    /// every later script on this thread writing into a dead buffer.
    struct Disarm;
    impl Drop for Disarm {
        fn drop(&mut self) {
            TXN_EFFECT_CAPTURE.with(|c| c.borrow_mut().take());
        }
    }
    TXN_EFFECT_CAPTURE.with(|c| *c.borrow_mut() = Some(Vec::new()));
    let disarm = Disarm;
    let out = run();
    let captured = TXN_EFFECT_CAPTURE
        .with(|c| c.borrow_mut().take())
        .unwrap_or_default();
    drop(disarm);
    (out, captured)
}

/// Record a script write effect into the armed capture. Returns `false`
/// (nothing done) when no capture is armed.
pub(super) fn capture_txn_effect(db_index: usize, cmd_and_args: &[Frame], reply: &Frame) -> bool {
    TXN_EFFECT_CAPTURE.with(|c| {
        let mut slot = c.borrow_mut();
        let Some(buf) = slot.as_mut() else {
            return false;
        };
        let frame = Frame::Array(crate::protocol::FrameVec::from_vec(cmd_and_args.to_vec()));
        // moon#825: frame AND reply, exactly as `record_effect_write` derives
        // it. No record means the reply proves nothing was written.
        for bytes in crate::persistence::aof::serialize_effect_for_log(&frame, reply) {
            buf.push((db_index, bytes));
        }
        true
    })
}

/// Record an eviction plain-drop `DEL` into the armed capture. Returns
/// `false` when no capture is armed.
#[cfg(feature = "runtime-monoio")]
pub(super) fn capture_txn_del(db_index: usize, key: &[u8]) -> bool {
    TXN_EFFECT_CAPTURE.with(|c| {
        let mut slot = c.borrow_mut();
        let Some(buf) = slot.as_mut() else {
            return false;
        };
        buf.push((db_index, crate::replication::reason_del::serialize_del(key)));
        true
    })
}

// ---------------------------------------------------------------------------
// moon#1285 (PR #1301 review): a script's writes inside an open TXN.
// ---------------------------------------------------------------------------

/// What a script captured for the cross-store transaction its connection has
/// open: the pre-images `TXN.ABORT` restores, and the keys the TXN must hold
/// write intents on.
///
/// A connection's own writes inside a TXN are undo-captured by the handler's
/// generic write leg, before dispatch. A script's writes never reach that
/// leg — the handler sees `EVAL`, and every inner `redis.call` runs through
/// the bridge — so `TXN.ABORT` restored nothing a script wrote, and neither
/// its keyspace nor its compensation records covered them. The bridge now
/// captures each written key's pre-image right before the write, with the
/// same key walker and the same insert / update / delete rule as the
/// connection leg, and the handler folds the result into the transaction
/// when the script returns.
#[derive(Debug, Default)]
pub(crate) struct ScriptTxnUndo {
    /// Pre-images, in capture order, each under its database.
    undo: crate::transaction::UndoLog,
    /// Every key the script wrote once, for the TXN's write intents.
    written: Vec<Bytes>,
    /// The first command refused because the TXN could not undo it; the
    /// handler poisons the transaction with it (#499).
    refused: Option<Bytes>,
    /// `(db, key)` already captured during this script: only a key's FIRST
    /// pre-image is ever restored (`kv_compensation::first_per_key`), so a
    /// loop rewriting one key clones its value once, not once per write.
    seen: std::collections::HashSet<(usize, Bytes)>,
}

impl ScriptTxnUndo {
    /// `(pre-images, written keys, first refused command)`.
    pub(crate) fn into_parts(self) -> (crate::transaction::UndoLog, Vec<Bytes>, Option<Bytes>) {
        (self.undo, self.written, self.refused)
    }
}

thread_local! {
    /// Armed only while a script runs for a connection with an open TXN.
    static TXN_UNDO_CAPTURE: RefCell<Option<ScriptTxnUndo>> = const { RefCell::new(None) };
}

/// Run `run` — one local script execution — with this thread's TXN undo
/// capture armed, and return what the script captured.
///
/// `run` is synchronous, so no other connection's script can run on this
/// shard thread while the capture is armed; a panic disarms it on unwind.
pub(crate) fn capture_txn_undo<R>(run: impl FnOnce() -> R) -> (R, ScriptTxnUndo) {
    struct Disarm;
    impl Drop for Disarm {
        fn drop(&mut self) {
            TXN_UNDO_CAPTURE.with(|c| c.borrow_mut().take());
        }
    }
    TXN_UNDO_CAPTURE.with(|c| *c.borrow_mut() = Some(ScriptTxnUndo::default()));
    let disarm = Disarm;
    let out = run();
    let captured = TXN_UNDO_CAPTURE
        .with(|c| c.borrow_mut().take())
        .unwrap_or_default();
    drop(disarm);
    (out, captured)
}

/// The keys `cmd` writes — the connection leg's rule: the shared walker's
/// WRITE positions, else the primary key.
fn txn_written_keys(cmd: &[u8], args: &[Frame]) -> smallvec::SmallVec<[Bytes; 4]> {
    let mut written = crate::tracking::invalidation::written_keys(cmd, args);
    if written.is_empty()
        && let Some(key) = crate::server::conn::shared::extract_primary_key(cmd, args)
    {
        written.push(key.clone());
    }
    written
}

/// Refuse a script write the TXN could not undo, before it has any effect:
/// `None` when no capture is armed or the write is capturable.
///
/// Refused, and the TXN poisoned:
/// - a write with no key to capture (`FLUSHDB`, `FLUSHALL`, `SWAPDB`, and any
///   argv the key walker cannot enumerate) — its pre-image is a whole
///   database or unknown;
/// - a second-database write (`MOVE`, `COPY ... DB n`) — refused on the
///   connection inside a TXN too, and the undo leg would have to capture the
///   destination database as well.
pub(super) fn txn_undo_refusal(
    cmd: &[u8],
    args: &[Frame],
    db_idx: usize,
    db_count: usize,
) -> Option<Frame> {
    TXN_UNDO_CAPTURE.with(|c| {
        let mut slot = c.borrow_mut();
        let capture = slot.as_mut()?;
        let two_db = matches!(
            crate::command::keyspace::move_cmd::resolve_two_db(cmd, args, db_idx, db_count),
            Some(Ok(_))
        );
        if !two_db && !txn_written_keys(cmd, args).is_empty() {
            return None;
        }
        if capture.refused.is_none() {
            capture.refused = Some(Bytes::copy_from_slice(cmd));
        }
        Some(Frame::Error(Bytes::from_static(
            crate::command::transaction::ERR_TXN_SCRIPT_NOT_UNDOABLE,
        )))
    })
}

/// Capture the pre-image of every key `cmd` is about to write into `db`
/// (database `db_idx`). No-op unless a capture is armed. Runs after the
/// eviction gate, right before the write — the connection leg's order.
///
/// The pre-image is a copy, as on the connection leg: the write mutates the
/// value in place, so there is nothing to move yet. The value the abort
/// REPLACES is handed to an armed snapshot by move (`kv_compensation`).
pub(super) fn txn_undo_capture(
    db: &mut crate::storage::Database,
    db_idx: usize,
    cmd: &[u8],
    args: &[Frame],
) {
    TXN_UNDO_CAPTURE.with(|c| {
        let mut slot = c.borrow_mut();
        let Some(capture) = slot.as_mut() else {
            return;
        };
        let is_delete = cmd.eq_ignore_ascii_case(b"DEL") || cmd.eq_ignore_ascii_case(b"UNLINK");
        for key in txn_written_keys(cmd, args) {
            if capture.seen.contains(&(db_idx, key.clone())) {
                continue;
            }
            // `peek`: an internal lookup, not client traffic (no LRU/LFU
            // touch); it still hides expired keys and promotes cold ones.
            match db.peek(&key).cloned() {
                // DEL of a missing key writes nothing — as on the connection.
                None if is_delete => continue,
                None => capture.undo.record_insert(db_idx, key.clone()),
                Some(old) if is_delete => capture.undo.record_delete(db_idx, key.clone(), old),
                Some(old) => capture.undo.record_update(db_idx, key.clone(), old),
            }
            capture.written.push(key.clone());
            capture.seen.insert((db_idx, key));
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Database;
    use crate::transaction::UndoRecord;

    fn bulk(s: &str) -> Frame {
        Frame::BulkString(Bytes::from(s.to_string()))
    }

    fn run(db: &mut Database, cmd: &str, args: &[&str]) {
        let args: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        let mut sel = 0;
        let _ = crate::command::dispatch(db, cmd.as_bytes(), &args, &mut sel, 16);
    }

    fn capture(db: &mut Database, cmd: &str, args: &[&str]) {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        txn_undo_capture(db, 0, cmd.as_bytes(), &frames);
        run(db, cmd, args);
    }

    /// Disarmed, the bridge hooks are inert: nothing refused, nothing held.
    #[test]
    fn disarmed_capture_is_a_no_op() {
        let mut db = Database::new();
        assert!(txn_undo_refusal(b"FLUSHDB", &[], 0, 16).is_none());
        txn_undo_capture(&mut db, 0, b"SET", &[bulk("k"), bulk("v")]);
        let ((), captured) = capture_txn_undo(|| ());
        let (undo, written, refused) = captured.into_parts();
        assert!(undo.is_empty() && written.is_empty() && refused.is_none());
    }

    /// Insert / update / delete, the connection leg's rule; a key written
    /// twice is captured once (its FIRST pre-image); a DEL of a missing key
    /// captures nothing.
    #[test]
    fn captures_first_pre_image_per_key() {
        let mut db = Database::new();
        run(&mut db, "SET", &["upd", "old"]);
        run(&mut db, "RPUSH", &["del", "a"]);
        let ((), captured) = capture_txn_undo(|| {
            capture(&mut db, "SET", &["upd", "x1"]);
            capture(&mut db, "SET", &["upd", "x2"]);
            capture(&mut db, "DEL", &["del", "missing"]);
            capture(&mut db, "SET", &["new", "n"]);
        });
        let (undo, written, refused) = captured.into_parts();
        assert!(refused.is_none());
        assert_eq!(
            written,
            vec![Bytes::from("upd"), Bytes::from("del"), Bytes::from("new")]
        );
        let kinds: Vec<(usize, &'static str, Bytes)> = undo
            .into_records_with_db()
            .map(|(db, r)| match r {
                UndoRecord::Insert { key } => (db, "insert", key),
                UndoRecord::Update { key, .. } => (db, "update", key),
                UndoRecord::Delete { key, .. } => (db, "delete", key),
            })
            .collect();
        assert_eq!(
            kinds,
            vec![
                (0, "update", Bytes::from("upd")),
                (0, "delete", Bytes::from("del")),
                (0, "insert", Bytes::from("new")),
            ]
        );
    }

    /// Keyless and second-database writes are refused while armed, and the
    /// first refused command is kept for the TXN's poisoning.
    #[test]
    fn uncapturable_writes_are_refused() {
        let (refusals, captured) = capture_txn_undo(|| {
            [
                txn_undo_refusal(b"FLUSHDB", &[], 0, 16).is_some(),
                txn_undo_refusal(b"FLUSHALL", &[], 0, 16).is_some(),
                txn_undo_refusal(b"MOVE", &[bulk("k"), bulk("3")], 0, 16).is_some(),
                txn_undo_refusal(
                    b"COPY",
                    &[bulk("a"), bulk("b"), bulk("DB"), bulk("2")],
                    0,
                    16,
                )
                .is_some(),
                // Same-db COPY and ordinary writes are capturable.
                txn_undo_refusal(b"COPY", &[bulk("a"), bulk("b")], 0, 16).is_some(),
                txn_undo_refusal(b"SET", &[bulk("k"), bulk("v")], 0, 16).is_some(),
            ]
        });
        assert_eq!(refusals, [true, true, true, true, false, false]);
        let (_, _, refused) = captured.into_parts();
        assert_eq!(refused, Some(Bytes::from_static(b"FLUSHDB")));
    }

    /// A panic inside the script disarms the capture.
    #[test]
    fn capture_disarms_on_unwind() {
        let r = std::panic::catch_unwind(|| capture_txn_undo(|| panic!("script panicked")));
        assert!(r.is_err());
        assert!(txn_undo_refusal(b"FLUSHDB", &[], 0, 16).is_none());
    }
}
