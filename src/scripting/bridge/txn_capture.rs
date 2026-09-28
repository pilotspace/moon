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
    /// How many writes were refused, each one counted against the TXN.
    refused_count: u32,
    /// `(db, key)` already captured during this script: only a key's FIRST
    /// pre-image is ever restored (`kv_compensation::first_per_key`), so a
    /// loop rewriting one key clones its value once, not once per write.
    seen: std::collections::HashSet<(usize, Bytes)>,
}

impl ScriptTxnUndo {
    /// `(pre-images, written keys, (first refused command, refusal count))`.
    pub(crate) fn into_parts(
        self,
    ) -> (
        crate::transaction::UndoLog,
        Vec<Bytes>,
        Option<(Bytes, u32)>,
    ) {
        let refused = self.refused.map(|cmd| (cmd, self.refused_count));
        (self.undo, self.written, refused)
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

/// Writes `command::dispatch` does NOT execute: a connection-level intercept
/// serves each of them (blocking pops, `FCALL`, `MQ`, `WS`, `FT.*`,
/// `GRAPH.*`, `FUNCTION`, `TEMPORAL.*`), so from a script they answer
/// `unknown command` and write nothing. Inside a TXN they are therefore
/// neither captured nor refused (moon#1285, PR #1301 review): capturing the
/// key their argv names — `BLPOP`'s list, `MQ`'s queue, or a graph / index
/// name taken as `args[0]` — made `TXN.ABORT` delete or restore another
/// client's key of that name, and the write intent hid it from other TXNs.
///
/// `script_undispatched_writes_match_dispatch` walks `COMMAND_META` and pins
/// this set to exactly the WRITE commands `dispatch` answers `unknown
/// command`, in both directions.
static SCRIPT_UNDISPATCHED_WRITES: phf::Set<&'static str> = phf::phf_set! {
    "BLPOP", "BRPOP", "BLMOVE", "BRPOPLPUSH", "BLMPOP", "BZPOPMIN", "BZPOPMAX",
    "BZMPOP", "FCALL", "FUNCTION", "MQ", "WS",
    "FT.CREATE", "FT.DROPINDEX", "FT.COMPACT", "FT.CONFIG",
    "GRAPH.CREATE", "GRAPH.ADDNODE", "GRAPH.ADDEDGE", "GRAPH.DELETE",
    "GRAPH.DROP", "GRAPH.QUERY",
    "TEMPORAL.SNAPSHOT_AT", "TEMPORAL.INVALIDATE",
};

/// Keyless writes `command::dispatch` EXECUTES from a script, whose pre-image
/// is a whole database: refused inside a TXN (moon#1285, PR #1301 review).
///
/// Any other `first_key: 0` write outside [`KeySpecClass::Movable`] and
/// outside [`SCRIPT_UNDISPATCHED_WRITES`] (`TXN`; `SELECT` is refused by the
/// bridge before this) answers an error from `dispatch` and writes nothing.
/// `keyless_script_writes_are_refused_or_inert` checks it for every such
/// registry entry.
///
/// [`KeySpecClass::Movable`]: crate::command::metadata::KeySpecClass::Movable
const KEYLESS_DISPATCHED_WRITES: [&[u8]; 3] = [b"FLUSHDB", b"FLUSHALL", b"SWAPDB"];

/// What the TXN undo capture does with one script write (moon#1285, PR #1301
/// review).
#[derive(Debug, PartialEq)]
enum TxnWritePlan {
    /// Capture these keys' pre-images and hold write intents on them.
    Capture(smallvec::SmallVec<[Bytes; 4]>),
    /// Nothing to capture: the bridge answers this argv with an error and
    /// writes nothing — a write only a connection-level intercept serves
    /// (`unknown command`, [`SCRIPT_UNDISPATCHED_WRITES`]), an argv the
    /// command's arity rejects (the arity error), a `MOVE` / `COPY ... DB`
    /// the two-db resolver rejects, or `TXN`. Runs exactly as outside a TXN.
    Inert,
    /// A write the TXN could not undo; refused before it runs.
    Refuse,
}

/// Plan one script write inside a TXN.
///
/// Inert first: a write `dispatch` does not execute
/// ([`SCRIPT_UNDISPATCHED_WRITES`]), an argv the arity rejects (shorter than
/// it, or longer than an exact one), or a two-db write the resolver rejects.
///
/// Captured: the shared walker's WRITE positions, else — for a command whose
/// registry entry names a first key (`first_key > 0`) — the primary key, the
/// connection leg's rule. The fallback is NEVER taken for a `first_key: 0`
/// command: its `args[0]` is a graph, an index or a subcommand literal, and
/// capturing a keyspace key of that name made `TXN.ABORT` delete or restore a
/// key the transaction never wrote (another client's).
///
/// Refused: a second-database write (`MOVE`, `COPY ... DB n`); a keyless
/// write `dispatch` executes ([`KEYLESS_DISPATCHED_WRITES`]); and an
/// arity-valid argv whose written keys cannot be enumerated — a malformed
/// movable-key command (`LMPOP 5 a b LEFT`), or a keyed one with no primary
/// key.
fn txn_write_plan(cmd: &[u8], args: &[Frame], db_idx: usize, db_count: usize) -> TxnWritePlan {
    use crate::command::metadata::{KeySpecClass, class_of, lookup};
    // `is_write` gates the caller, so an unregistered name cannot get here;
    // if one did, `dispatch` would answer `unknown command`.
    let Some(meta) = lookup(cmd) else {
        return TxnWritePlan::Inert;
    };
    if SCRIPT_UNDISPATCHED_WRITES.contains(meta.name) {
        return TxnWritePlan::Inert;
    }
    // Arity counts the command name; positive is exact, negative a minimum.
    // An argv the arity rejects is inert: a SHORT one for any command, and a
    // LONG one for an exact-arity command (`SETNX k v extra`) — dispatch
    // answers the arity error before it touches a key. Pinned for every
    // registered write, up to two arguments past an exact arity, by
    // `every_arity_rejected_write_argv_is_an_error_that_writes_nothing`.
    let given = args.len() + 1;
    let arity = usize::from(meta.arity.unsigned_abs());
    if given < arity || (meta.arity > 0 && given != arity) {
        return TxnWritePlan::Inert;
    }
    match crate::command::keyspace::move_cmd::resolve_two_db(cmd, args, db_idx, db_count) {
        Some(Ok(_)) => return TxnWritePlan::Refuse,
        // The bridge answers the resolver's error and writes nothing
        // (`MOVE k <same db>`, `COPY a b DB <junk>`).
        Some(Err(_)) => return TxnWritePlan::Inert,
        None => {}
    }
    let written = crate::tracking::invalidation::written_keys(cmd, args);
    if !written.is_empty() {
        return TxnWritePlan::Capture(written);
    }
    if meta.first_key > 0 {
        return match crate::server::conn::shared::extract_primary_key(cmd, args) {
            Some(key) => TxnWritePlan::Capture(smallvec::smallvec![key.clone()]),
            None => TxnWritePlan::Refuse,
        };
    }
    if KEYLESS_DISPATCHED_WRITES
        .iter()
        .any(|k| cmd.eq_ignore_ascii_case(k))
        || class_of(meta) == KeySpecClass::Movable
    {
        return TxnWritePlan::Refuse;
    }
    TxnWritePlan::Inert
}

/// Plan a script write against the armed TXN undo capture, before it has any
/// effect ([`txn_write_plan`]):
///
/// - `Ok(None)`: no capture is armed, or the write is inert — run it as
///   usual, capture nothing;
/// - `Ok(Some(keys))`: capture `keys` right before the write
///   ([`txn_undo_capture`]) — computed once, here;
/// - `Err(reply)`: refused. The reply is `ERR_TXN_SCRIPT_NOT_UNDOABLE`, and
///   every refusal counts against the TXN (#499), which may then not commit.
pub(super) fn txn_undo_plan(
    cmd: &[u8],
    args: &[Frame],
    db_idx: usize,
    db_count: usize,
) -> Result<Option<smallvec::SmallVec<[Bytes; 4]>>, Frame> {
    TXN_UNDO_CAPTURE.with(|c| {
        let mut slot = c.borrow_mut();
        let Some(capture) = slot.as_mut() else {
            return Ok(None);
        };
        match txn_write_plan(cmd, args, db_idx, db_count) {
            TxnWritePlan::Capture(keys) => Ok(Some(keys)),
            TxnWritePlan::Inert => Ok(None),
            TxnWritePlan::Refuse => {
                if capture.refused.is_none() {
                    capture.refused = Some(Bytes::copy_from_slice(cmd));
                }
                capture.refused_count = capture.refused_count.saturating_add(1);
                Err(Frame::Error(Bytes::from_static(
                    crate::command::transaction::ERR_TXN_SCRIPT_NOT_UNDOABLE,
                )))
            }
        }
    })
}

/// Capture the pre-image of each of `keys` — the keys [`txn_undo_plan`]
/// planned for `cmd` — which `cmd` is about to write into `db` (database
/// `db_idx`). No-op unless a capture is armed. Runs after the eviction gate,
/// right before the write — the connection leg's order.
///
/// The pre-image is a copy, as on the connection leg: the write mutates the
/// value in place, so there is nothing to move yet. The value the abort
/// REPLACES is handed to an armed snapshot by move (`kv_compensation`).
///
/// Returns where this write's captures begin (`None` when no capture is
/// armed), for [`txn_undo_discard`] should the write answer an error.
pub(super) fn txn_undo_capture(
    db: &mut crate::storage::Database,
    db_idx: usize,
    cmd: &[u8],
    keys: smallvec::SmallVec<[Bytes; 4]>,
) -> Option<TxnCaptureMark> {
    TXN_UNDO_CAPTURE.with(|c| {
        let mut slot = c.borrow_mut();
        let capture = slot.as_mut()?;
        let mark = TxnCaptureMark {
            db_idx,
            undo_len: capture.undo.len(),
            written_len: capture.written.len(),
        };
        let is_delete = cmd.eq_ignore_ascii_case(b"DEL") || cmd.eq_ignore_ascii_case(b"UNLINK");
        for key in keys {
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
        Some(mark)
    })
}

/// Where one script write's captures begin in the armed [`ScriptTxnUndo`].
#[derive(Debug)]
pub(super) struct TxnCaptureMark {
    db_idx: usize,
    undo_len: usize,
    written_len: usize,
}

/// Take back what [`txn_undo_capture`] captured for a write that then
/// answered an error, and so wrote nothing (moon#1285, PR #1301 review): its
/// pre-images, its write intents, and its `seen` entries — so a later
/// successful write of the same key captures that key's real pre-image.
///
/// A write's captures are always the tail of the log: nothing else is
/// captured between [`txn_undo_capture`] and the write's reply.
pub(super) fn txn_undo_discard(mark: TxnCaptureMark) {
    TXN_UNDO_CAPTURE.with(|c| {
        let mut slot = c.borrow_mut();
        let Some(capture) = slot.as_mut() else {
            return;
        };
        capture.undo.truncate(mark.undo_len);
        for key in capture.written.drain(mark.written_len..) {
            capture.seen.remove(&(mark.db_idx, key));
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

    fn frames(args: &[&str]) -> Vec<Frame> {
        args.iter().map(|a| bulk(a)).collect()
    }

    fn run(db: &mut Database, cmd: &str, args: &[&str]) -> Frame {
        let mut sel = 0;
        match crate::command::dispatch(db, cmd.as_bytes(), &frames(args), &mut sel, 16) {
            crate::command::DispatchResult::Response(f)
            | crate::command::DispatchResult::Quit(f) => f,
        }
    }

    /// The bridge's order: plan, then capture what was planned, then write —
    /// and take the captures back if the write answered an error.
    fn capture(db: &mut Database, cmd: &str, args: &[&str]) -> Frame {
        let mark = match txn_undo_plan(cmd.as_bytes(), &frames(args), 0, 16) {
            Err(refused) => return refused,
            Ok(Some(keys)) => txn_undo_capture(db, 0, cmd.as_bytes(), keys),
            Ok(None) => None,
        };
        let reply = bridge_reply(db, cmd, args);
        if let Some(mark) = mark
            && matches!(reply, Frame::Error(_))
        {
            txn_undo_discard(mark);
        }
        reply
    }

    fn refused(cmd: &str, args: &[&str]) -> bool {
        txn_undo_plan(cmd.as_bytes(), &frames(args), 0, 16).is_err()
    }

    /// What the bridge (`redis_call.rs`) answers for `cmd args`: `SELECT` is
    /// refused, a two-db resolver's error is the whole reply, anything else
    /// runs through `dispatch`. A resolved two-db op would write a second database, which
    /// no caller of this helper expects.
    fn bridge_reply(db: &mut Database, cmd: &str, args: &[&str]) -> Frame {
        use crate::command::keyspace::move_cmd::resolve_two_db;
        // The bridge refuses `SELECT` before any TXN planning.
        if cmd.eq_ignore_ascii_case("SELECT") {
            return Frame::Error(Bytes::from_static(b"ERR SELECT inside scripts"));
        }
        match resolve_two_db(cmd.as_bytes(), &frames(args), 0, 16) {
            Some(Err(reply)) => reply,
            Some(Ok(_)) => panic!("{cmd} {args:?} resolved a two-database write"),
            None => run(db, cmd, args),
        }
    }

    /// Argument fillers for the registry guards. `x` alone fails every
    /// numeric parse, so a handler could answer a parse error whatever the
    /// argument COUNT, hiding an arity check looser than the registry's; `1`
    /// and `0` are well-formed counts, indexes, scores, databases and TTLs.
    /// The seeded key is `x`, so a write to `1` or `0` shows in `DBSIZE`.
    const GUARD_FILLERS: [&str; 3] = ["x", "1", "0"];

    /// Disarmed, the bridge hooks are inert: nothing refused, nothing held.
    #[test]
    fn disarmed_capture_is_a_no_op() {
        let mut db = Database::new();
        assert_eq!(txn_undo_plan(b"FLUSHDB", &[], 0, 16), Ok(None));
        txn_undo_capture(&mut db, 0, b"SET", smallvec::smallvec![Bytes::from("k")]);
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
                refused("FLUSHDB", &[]),
                refused("FLUSHALL", &["ASYNC"]),
                refused("SWAPDB", &["0", "1"]),
                refused("MOVE", &["k", "3"]),
                refused("COPY", &["a", "b", "DB", "2"]),
                // A well-formed arity whose keys cannot be enumerated.
                refused("LMPOP", &["5", "a", "b", "LEFT"]),
                // Same-db COPY and ordinary writes are capturable.
                refused("COPY", &["a", "b"]),
                refused("SET", &["k", "v"]),
            ]
        });
        assert_eq!(refusals, [true, true, true, true, true, true, false, false]);
        let (_, _, refused) = captured.into_parts();
        assert_eq!(
            refused.map(|(cmd, _)| cmd),
            Some(Bytes::from_static(b"FLUSHDB"))
        );
    }

    /// PR #1301 review MINOR-1: a write that only a connection-level
    /// intercept serves (`GRAPH.*`, `FT.*`, `MQ`, `WS`, `FUNCTION`, the
    /// blocking pops) answers `unknown command` from a script and writes
    /// nothing — so it captures NOTHING. It used to capture `args[0]` (a
    /// graph, an index or a subcommand literal) or the key its argv names
    /// (`MQ`'s queue, `BLPOP`'s list) as a keyspace key: `TXN.ABORT` then
    /// deleted or restored another client's key of that name.
    #[test]
    fn keyless_writes_a_script_cannot_run_capture_nothing() {
        let mut db = Database::new();
        run(&mut db, "SET", &["users", "orig"]);
        let (plans, captured) = capture_txn_undo(|| {
            [
                ("GRAPH.QUERY", &["users", "CREATE (:N)"][..]),
                ("GRAPH.ADDNODE", &["users", "N"][..]),
                ("FT.DROPINDEX", &["users"][..]),
                ("FT.CREATE", &["users", "SCHEMA", "f", "TEXT"][..]),
                ("MQ", &["PUBLISH", "users", "m"][..]),
                ("WS", &["CREATE", "users"][..]),
                ("FUNCTION", &["FLUSH"][..]),
                ("BLPOP", &["users", "0"][..]),
                ("BZPOPMIN", &["users", "0"][..]),
            ]
            .map(|(cmd, args)| {
                let plan = txn_undo_plan(cmd.as_bytes(), &frames(args), 0, 16);
                capture(&mut db, cmd, args);
                (cmd, plan)
            })
        });
        for (cmd, plan) in plans {
            assert_eq!(plan, Ok(None), "{cmd} plans no capture");
        }
        let (undo, written, refused) = captured.into_parts();
        assert!(undo.is_empty(), "no pre-image captured");
        assert!(written.is_empty(), "no write intent");
        assert!(refused.is_none(), "nothing refused");
    }

    /// [`SCRIPT_UNDISPATCHED_WRITES`] is exactly the set of WRITE commands
    /// `dispatch` answers `unknown command` — in both directions, so a
    /// listed command that gains a `dispatch` arm (and would then write
    /// uncaptured) fails here, as does a new undispatched write left out.
    /// The name match in `dispatch` does not depend on the arguments.
    #[test]
    fn script_undispatched_writes_match_dispatch() {
        use crate::command::metadata::{COMMAND_META, CommandFlags};
        for (name, meta) in COMMAND_META.entries() {
            if !meta.flags.contains(CommandFlags::WRITE) {
                continue;
            }
            let n = usize::from(meta.arity.unsigned_abs())
                .saturating_sub(1)
                .max(1);
            let args = vec!["1"; n];
            let mut db = Database::new();
            let mut sel = 0;
            let unknown =
                crate::command::dispatch(&mut db, name.as_bytes(), &frames(&args), &mut sel, 16)
                    .is_unknown_command();
            assert_eq!(
                SCRIPT_UNDISPATCHED_WRITES.contains(name),
                unknown,
                "{name}: listed as undispatched iff dispatch answers `unknown command`"
            );
        }
        for name in SCRIPT_UNDISPATCHED_WRITES.iter() {
            assert!(
                crate::command::metadata::is_write(name.as_bytes()),
                "{name} is a registered write"
            );
        }
    }

    /// The registry guard for [`KEYLESS_DISPATCHED_WRITES`]: every WRITE
    /// entry of `COMMAND_META` with `first_key: 0` is either refused, or
    /// planned inert — and an inert one really is inert: `dispatch` answers
    /// it with an error and leaves the database untouched, for every argv
    /// length its arity allows (up to 4). A keyless write that gains a
    /// `dispatch` arm without joining the refusal list fails here.
    #[test]
    fn keyless_script_writes_are_refused_or_inert() {
        use crate::command::metadata::{COMMAND_META, CommandFlags, KeySpecClass, class_of};
        let mut checked = 0;
        for (name, meta) in COMMAND_META.entries() {
            if !meta.flags.contains(CommandFlags::WRITE) || meta.first_key > 0 {
                continue;
            }
            let min = usize::from(meta.arity.unsigned_abs()).saturating_sub(1);
            let max = if meta.arity >= 0 { min } else { min + 4 };
            for (n, filler) in (min..=max).flat_map(|n| GUARD_FILLERS.map(|f| (n, f))) {
                let args = vec![filler; n];
                let plan = txn_write_plan(name.as_bytes(), &frames(&args), 0, 16);
                let ((reply, mut db), captured) = capture_txn_undo(|| {
                    let mut db = Database::new();
                    run(&mut db, "SET", &["x", "keep"]);
                    let reply = capture(&mut db, name, &args);
                    (reply, db)
                });
                let unchanged = |db: &mut Database| {
                    assert_eq!(
                        run(db, "DBSIZE", &[]),
                        Frame::Integer(1),
                        "{name} {args:?} changed the keyspace"
                    );
                    assert_eq!(
                        run(db, "GET", &["x"]),
                        bulk("keep"),
                        "{name} {args:?} changed a key"
                    );
                };
                match plan {
                    TxnWritePlan::Refuse => {}
                    // Only a movable-key write (`LMPOP`, `ZMPOP`) names its
                    // keys; one that then answers an error keeps nothing.
                    TxnWritePlan::Capture(keys) => {
                        assert_eq!(
                            class_of(meta),
                            KeySpecClass::Movable,
                            "{name} {args:?} captured {keys:?}"
                        );
                        if matches!(reply, Frame::Error(_)) {
                            let (undo, written, _) = captured.into_parts();
                            assert!(
                                undo.is_empty() && written.is_empty(),
                                "{name} {args:?} answered {reply:?} but kept its capture"
                            );
                            unchanged(&mut db);
                        }
                    }
                    TxnWritePlan::Inert => {
                        assert!(
                            matches!(reply, Frame::Error(_)),
                            "{name} {args:?} is planned inert but dispatch answered {reply:?}"
                        );
                        unchanged(&mut db);
                    }
                }
                checked += 1;
            }
        }
        assert!(checked > 20, "the registry walk ran ({checked} argvs)");
        for cmd in KEYLESS_DISPATCHED_WRITES {
            let ((), captured) = capture_txn_undo(|| {
                let _ = txn_undo_plan(cmd, &frames(&["0", "1"]), 0, 16);
            });
            assert!(
                captured.into_parts().2.is_some(),
                "{} is refused",
                String::from_utf8_lossy(cmd)
            );
        }
    }

    /// PR #1301 review MINOR-2: a write whose argv is SHORTER than its arity
    /// (`redis.pcall('DEL')`, `redis.pcall('SET')`, `SET k`) is neither
    /// captured nor refused — `dispatch` answers the arity error, exactly as
    /// outside a TXN, and the TXN stays committable. `DEL` / `SET` used to be
    /// refused (no key to capture) and poisoned the TXN; `SET k` captured a
    /// key the failing command never wrote.
    #[test]
    fn short_argv_is_inert_and_answers_the_arity_error() {
        let mut db = Database::new();
        let (plans, captured) = capture_txn_undo(|| {
            [
                ("DEL", &[][..]),
                ("SET", &[][..]),
                ("SET", &["k"][..]),
                ("HSET", &["h", "f"][..]),
                ("MSET", &["k"][..]),
            ]
            .map(|(cmd, args)| {
                let plan = txn_undo_plan(cmd.as_bytes(), &frames(args), 0, 16);
                let reply = run(&mut db, cmd, args);
                (cmd, plan, reply)
            })
        });
        for (cmd, plan, reply) in plans {
            assert_eq!(plan, Ok(None), "{cmd}: neither captured nor refused");
            assert!(
                matches!(&reply, Frame::Error(e) if e.starts_with(b"ERR wrong number of arguments")),
                "{cmd}: dispatch answers the arity error: {reply:?}"
            );
        }
        let (undo, written, refused) = captured.into_parts();
        assert!(undo.is_empty() && written.is_empty());
        assert!(refused.is_none(), "the TXN is not poisoned");
    }

    /// PR #1301 review round 3: an exact-arity write given MORE arguments
    /// than its arity (`SETNX k v extra`, `HSETNX h f v x`) is an arity error
    /// that writes nothing, so it is inert like a short argv. It used to be
    /// captured and get a write intent: `TXN.ABORT` then restored the
    /// pre-image over a concurrent client's write, and the intent hid the key
    /// from other transactions. `MOVE k <same db>` likewise never runs (the
    /// bridge answers the resolver's error), so it captures nothing either.
    #[test]
    fn long_exact_arity_argv_is_inert_and_answers_the_arity_error() {
        let mut db = Database::new();
        run(&mut db, "SET", &["k", "orig"]);
        let (plans, captured) = capture_txn_undo(|| {
            [
                (
                    "SETNX",
                    &["k", "v", "extra"][..],
                    "wrong number of arguments",
                ),
                (
                    "HSETNX",
                    &["h", "f", "v", "x"][..],
                    "wrong number of arguments",
                ),
                ("INCRBY", &["n", "1", "1"][..], "wrong number of arguments"),
                ("RENAME", &["k", "b", "c"][..], "wrong number of arguments"),
                ("MOVE", &["k", "3", "x"][..], "wrong number of arguments"),
                (
                    "MOVE",
                    &["k", "0"][..],
                    "source and destination objects are the same",
                ),
            ]
            .map(|(cmd, args, err)| {
                let plan = txn_undo_plan(cmd.as_bytes(), &frames(args), 0, 16);
                let reply = bridge_reply(&mut db, cmd, args);
                (cmd, args, err, plan, reply)
            })
        });
        for (cmd, args, err, plan, reply) in plans {
            assert_eq!(
                plan,
                Ok(None),
                "{cmd} {args:?}: neither captured nor refused"
            );
            assert!(
                matches!(&reply, Frame::Error(e) if e.windows(err.len()).any(|w| w == err.as_bytes())),
                "{cmd} {args:?}: dispatch answers `{err}`: {reply:?}"
            );
        }
        let (undo, written, refused) = captured.into_parts();
        assert!(undo.is_empty() && written.is_empty(), "nothing captured");
        assert!(refused.is_none(), "the TXN is not poisoned");
        assert_eq!(run(&mut db, "DBSIZE", &[]), Frame::Integer(1));
        assert_eq!(run(&mut db, "GET", &["k"]), bulk("orig"));
    }

    /// PR #1301 review round 3: a write that answers an error wrote nothing,
    /// so what was captured for it is taken back — pre-image, write intent
    /// and `seen` entry. It used to be kept (`SET k v BADOPT`, `ZMPOP 1 k
    /// JUNK`, `INCR` of a non-number): the abort restored the pre-image over
    /// another client's write. A later successful write of the same key
    /// still captures the key's real pre-image.
    #[test]
    fn a_write_that_answers_an_error_keeps_no_capture() {
        let mut db = Database::new();
        run(&mut db, "SET", &["s", "abc"]);
        run(&mut db, "SET", &["k", "orig"]);
        let (replies, captured) = capture_txn_undo(|| {
            [
                capture(&mut db, "SET", &["k", "v", "BADOPT"]),
                capture(&mut db, "ZMPOP", &["1", "z", "JUNK"]),
                capture(&mut db, "INCR", &["s"]),
                capture(&mut db, "LPUSH", &["s", "x"]),
                // Succeeds: `k`'s pre-image is `orig`, captured once.
                capture(&mut db, "SET", &["k", "new"]),
                // Fails after `k` was captured: nothing more to take back.
                capture(&mut db, "SET", &["k", "v", "BADOPT"]),
            ]
        });
        for reply in &replies[..4] {
            assert!(matches!(reply, Frame::Error(_)), "{reply:?}");
        }
        assert!(matches!(replies[5], Frame::Error(_)));
        let (undo, written, refused) = captured.into_parts();
        assert!(refused.is_none());
        assert_eq!(written, vec![Bytes::from("k")]);
        let records: Vec<_> = undo.into_records_with_db().collect();
        assert_eq!(records.len(), 1, "{records:?}");
        match &records[0] {
            (0, UndoRecord::Update { key, old_entry }) => {
                assert_eq!(key, &Bytes::from("k"));
                let mut probe = Database::new();
                probe.set(b"k", old_entry.clone());
                assert_eq!(run(&mut probe, "GET", &["k"]), bulk("orig"));
            }
            other => panic!("expected k's update record, got {other:?}"),
        }
    }

    /// The guard behind "an arity-invalid argv is inert": for EVERY write in
    /// `COMMAND_META`, the bridge with fewer arguments than the arity — or,
    /// for an exact (positive) arity, one or two MORE — answers an error and
    /// leaves the keyspace untouched, and the plan is inert.
    #[test]
    fn every_arity_rejected_write_argv_is_an_error_that_writes_nothing() {
        use crate::command::metadata::{COMMAND_META, CommandFlags};
        let mut checked = 0;
        for (name, meta) in COMMAND_META.entries() {
            if !meta.flags.contains(CommandFlags::WRITE) {
                continue;
            }
            let min = usize::from(meta.arity.unsigned_abs()).saturating_sub(1);
            let long = if meta.arity > 0 {
                min + 1..min + 3
            } else {
                0..0
            };
            for (n, filler) in (0..min)
                .chain(long)
                .flat_map(|n| GUARD_FILLERS.map(|f| (n, f)))
            {
                let args = vec![filler; n];
                let ((plan, reply, mut db), _) = capture_txn_undo(|| {
                    let mut db = Database::new();
                    run(&mut db, "SET", &["x", "keep"]);
                    let plan = txn_undo_plan(name.as_bytes(), &frames(&args), 0, 16);
                    let reply = bridge_reply(&mut db, name, &args);
                    (plan, reply, db)
                });
                assert_eq!(plan, Ok(None), "{name} {args:?} is inert");
                assert!(
                    matches!(reply, Frame::Error(_)),
                    "{name} {args:?}: dispatch answered {reply:?}"
                );
                assert_eq!(
                    run(&mut db, "DBSIZE", &[]),
                    Frame::Integer(1),
                    "{name} {args:?}"
                );
                assert_eq!(run(&mut db, "GET", &["x"]), bulk("keep"), "{name} {args:?}");
                checked += 1;
            }
        }
        assert!(checked > 100, "the registry walk ran ({checked} argvs)");
    }

    /// Every refused write counts against the TXN, not just the first.
    #[test]
    fn every_refusal_is_counted() {
        let ((), captured) = capture_txn_undo(|| {
            assert!(refused("FLUSHDB", &[]));
            assert!(refused("MOVE", &["k", "3"]));
            assert!(!refused("SET", &["k", "v"]));
            assert!(refused("FLUSHALL", &[]));
        });
        let (_, _, refused) = captured.into_parts();
        assert_eq!(refused, Some((Bytes::from_static(b"FLUSHDB"), 3)));
    }

    /// A panic inside the script disarms the capture.
    #[test]
    fn capture_disarms_on_unwind() {
        let r = std::panic::catch_unwind(|| capture_txn_undo(|| panic!("script panicked")));
        assert!(r.is_err());
        assert_eq!(txn_undo_plan(b"FLUSHDB", &[], 0, 16), Ok(None));
    }
}
