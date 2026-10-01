//! Scripts inside an open cross-store transaction (moon#1285, PR #1301
//! review).
//!
//! `TXN.ABORT` restores what the transaction's undo log holds. A connection's
//! own writes reach that log through the generic write leg; a script's
//! writes run inside `EVAL` / `EVALSHA` / `FCALL`, through the Lua bridge, and
//! reached nothing — the abort left them in place and its compensation
//! records did not cover them, so they survived the abort live, after a
//! restart and on replicas.
//!
//! One policy for every script entry point on both runtimes:
//!
//! - a script that runs on THIS shard runs with the bridge's undo capture
//!   armed ([`run_local_script`]); its pre-images join the transaction's log
//!   and its keys the transaction's write intents, so the abort restores
//!   them and logs their compensation like any other TXN write. Writes the
//!   capture cannot undo are refused inside the script
//!   (`ERR_TXN_SCRIPT_NOT_UNDOABLE`) and poison the transaction;
//! - a read-write script whose keys live on ANOTHER shard is refused before
//!   it is routed ([`routed_script_refusal`]): the undo log is applied on
//!   the connection's shard only, which is why a TXN already refuses a
//!   cross-shard write. The `_RO` variants still route — they cannot write —
//!   and so does an `FCALL` of a function registered `no-writes`, which is
//!   routed AS `FCALL_RO` ([`routed_script_cmd`]) so the target shard runs
//!   it read-only whatever its own registry says by then.

use crate::protocol::Frame;
use crate::server::conn::core::ConnectionContext;
use crate::transaction::CrossStoreTxn;

/// True when `cmd_args` names a function this shard's registry holds with the
/// `no-writes` flag — the flag Redis runs `FCALL` read-only for, and the
/// flag [`crate::scripting::FunctionRegistry::call_function`] enforces.
/// `false` for an unknown function or a busy registry: the caller then
/// treats the call as read-write, the conservative side.
fn fcall_is_no_writes(cmd_args: &[Frame]) -> bool {
    let Some(Frame::BulkString(name)) = cmd_args.first() else {
        return false;
    };
    let slot = crate::scripting::shard_function_registry();
    let Ok(guard) = slot.try_borrow() else {
        return false;
    };
    guard
        .as_ref()
        .and_then(|reg| reg.lookup(name))
        .is_some_and(|(_, f)| f.flags & crate::scripting::functions::func_flags::NO_WRITES != 0)
}

/// The command a script is routed to another shard AS. Inside a TXN, an
/// `FCALL` of a `no-writes` function is routed as `FCALL_RO`: it passed
/// [`routed_script_refusal`] on that flag, and `FCALL_RO` makes the target
/// run it read-only even if a concurrent `FUNCTION LOAD REPLACE` redefined
/// the function as a writer before the routed call arrived. `cmd` otherwise.
pub(crate) fn routed_script_cmd<'a>(cmd: &'a [u8], cmd_args: &[Frame], txn_open: bool) -> &'a [u8] {
    if txn_open && cmd.eq_ignore_ascii_case(b"FCALL") && fcall_is_no_writes(cmd_args) {
        b"FCALL_RO"
    } else {
        cmd
    }
}

/// `Some(ERR_TXN_CROSS_SHARD)` when a TXN is open and `cmd` is a read-write
/// script (`EVAL`, `EVALSHA`, an `FCALL` of a function not registered
/// `no-writes`) whose keys all live on another shard.
/// The caller poisons the transaction (#499) and answers the error; nothing
/// ran. `None` otherwise — including a malformed argv or a genuinely
/// cross-shard key set, whose own replies (`route_script_elsewhere`, the
/// local handler) are unchanged.
pub(crate) fn routed_script_refusal(
    cmd: &[u8],
    cmd_args: &[Frame],
    txn_open: bool,
    ctx: &ConnectionContext,
) -> Option<Frame> {
    if !txn_open || ctx.num_shards <= 1 {
        return None;
    }
    // moon parses no `#!lua flags=` shebang for EVAL / EVALSHA, so only the
    // `_RO` variants and a `no-writes` function are known read-only here.
    let read_only = cmd.eq_ignore_ascii_case(b"EVAL_RO")
        || cmd.eq_ignore_ascii_case(b"EVALSHA_RO")
        || cmd.eq_ignore_ascii_case(b"FCALL_RO")
        || (cmd.eq_ignore_ascii_case(b"FCALL") && fcall_is_no_writes(cmd_args));
    if read_only {
        return None;
    }
    let (_, _, keys, _) = crate::scripting::parse_eval_args(cmd_args).ok()?;
    match crate::scripting::route_script_keys(&keys, ctx.shard_id, ctx.num_shards) {
        crate::scripting::ScriptRoute::Remote(_) => Some(Frame::Error(bytes::Bytes::from_static(
            crate::command::transaction::ERR_TXN_CROSS_SHARD,
        ))),
        crate::scripting::ScriptRoute::Local | crate::scripting::ScriptRoute::CrossShard => None,
    }
}

/// Run one LOCAL script execution. With a transaction open, the bridge's
/// undo capture is armed around `run`, and what it captured is folded into
/// `txn` before this returns — in the same synchronous stretch as the script,
/// so nothing can observe the writes without their undo records:
///
/// - the pre-images join `txn.kv_undo` at the script's position (the abort
///   restores each key's FIRST pre-image, so a key the transaction wrote
///   before the script keeps its pre-transaction state);
/// - every written key gets a write intent, as the connection leg records;
/// - each refused command counts against the transaction, which may then
///   not commit.
///
/// Without a transaction this is `run()`.
pub(crate) fn run_local_script<R>(txn: Option<&mut CrossStoreTxn>, run: impl FnOnce() -> R) -> R {
    let Some(txn) = txn else {
        return run();
    };
    // moon#1299: the script's writes run as this transaction's own — keys it
    // already holds pass the isolation check, keys another TXN holds do not.
    let owner = crate::transaction::isolation::OwnerScope::enter(txn.txn_id);
    let (out, captured) = crate::scripting::bridge::capture_txn_undo(run);
    drop(owner);
    let (undo, written, refused) = captured.into_parts();
    if let Some((cmd, count)) = refused {
        txn.record_rejected_ops(&cmd, count);
    }
    // ... and every key it captured is held until COMMIT / ABORT, like the
    // connection leg's (`transaction::conn_capture`).
    txn.kv_undo.append(hold_script_undo(txn.txn_id, undo));
    if !written.is_empty() {
        let (lsn, tid) = (txn.snapshot_lsn, txn.txn_id);
        crate::shard::slice::with_shard(|s| {
            for key in written {
                s.kv_write_intents.record_write(key, lsn, tid);
            }
        });
    }
    out
}

/// Hold every key a script captured for `txn_id` and return its undo records
/// as they join the transaction's log (moon#1300, R2b W2: ONE copy of each
/// pre-transaction image).
///
/// - A key's FIRST record in the transaction (no hold yet): its before-image
///   moves into the new hold, which every snapshot serializes and the abort
///   restores from — the record becomes `Held` (key and kind). An `Insert`
///   (the key was absent) stays as it is.
/// - A key the transaction already holds (an earlier write, or an earlier
///   record of this script): the abort restores that first record only, so
///   this record's copy is dropped — `Held`, key and kind only.
fn hold_script_undo(txn_id: u64, undo: crate::transaction::UndoLog) -> crate::transaction::UndoLog {
    use crate::transaction::UndoRecord;
    use crate::transaction::isolation;
    let mut out = crate::transaction::UndoLog::new();
    for (db, record) in undo.into_records_with_db() {
        let already =
            isolation::holder(db, crate::transaction::kv_compensation::record_key(&record))
                == Some(txn_id);
        match record {
            UndoRecord::Insert { key } => {
                if already {
                    out.record_held(db, key, false);
                } else {
                    isolation::hold(db, &key, txn_id, None);
                    out.record_insert(db, key);
                }
            }
            UndoRecord::Update { key, old_entry } => {
                if !already {
                    isolation::hold(db, &key, txn_id, Some(old_entry));
                }
                out.record_held(db, key, false);
            }
            UndoRecord::Delete { key, old_entry } => {
                if !already {
                    isolation::hold(db, &key, txn_id, Some(old_entry));
                }
                out.record_held(db, key, true);
            }
            UndoRecord::Held { key, deleted } => out.record_held(db, key, deleted),
        }
    }
    out
}
