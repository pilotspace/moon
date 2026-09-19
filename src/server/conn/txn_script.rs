//! Scripts queued inside `MULTI`/`EXEC` (moon#894).
//!
//! `EVAL`, `EVALSHA`, `EVAL_RO`, `EVALSHA_RO`, `FCALL` and `FCALL_RO` are
//! connection-layer intercepts outside a transaction. `cmd_dispatch` has no
//! scripting arm, so the transaction executor, which replays the queue through
//! `dispatch()`, answered each of them `ERR unknown command` at `EXEC` and ran
//! the rest of the body. The script's writes were silently dropped from an
//! otherwise successful transaction.
//!
//! This module lets the executor run them in place, synchronously, in the same
//! pass as the rest of the body. That matches Redis, where a script inside
//! `EXEC` runs atomically with everything around it:
//!
//! * **Where it runs.** Wherever the body runs. `analyze_txn_locality` already
//!   reads a script's declared keys through the shared key walker, so a body
//!   whose script keys live on another shard is routed there whole
//!   (`ShardMessage::TxnExecute`), and a body whose keys span shards is
//!   refused with `CROSSSLOT` before anything runs. These are the moon#247
//!   rules, with no script-specific exception.
//! * **What it may touch.** Exactly what the same script may touch outside
//!   `MULTI`. The bridge's own cross-shard and read-only rules apply
//!   unchanged, because the same script entry points run it.
//! * **Durability.** The script's effect records are captured
//!   ([`crate::scripting::bridge::capture_txn_effects`]) and spliced into the
//!   body's record list at the script's position. From there they reach the
//!   AOF, the WAL and the replication stream exactly like the body's other
//!   writes, in body order.
//! * **Flushes.** A `redis.call('FLUSHALL')` inside the script is completed on
//!   this shard by `run_and_complete`, as outside `MULTI`. The cross-shard
//!   half is returned for the caller to broadcast, with the same deferral the
//!   executor already uses for a queued `FLUSHALL`.

use std::cell::RefCell;
use std::rc::Rc;

use bytes::Bytes;

use crate::protocol::Frame;
use crate::scripting::bridge::CapturedEffect;
use crate::scripting::pending_flush::PendingFlush;

/// What the transaction executor needs to run a queued script.
///
/// Built by the caller only when the queue actually contains a script (see
/// [`queue_has_script`]), so an ordinary `EXEC` pays nothing for it.
pub(crate) struct TxnScripting<'a> {
    /// The executing shard's Lua VM.
    pub lua: &'a Rc<mlua::Lua>,
    /// The executing shard's script cache (`EVALSHA` looks up here).
    pub script_cache: &'a Rc<RefCell<crate::scripting::ScriptCache>>,
    /// The executing shard's function registry. The caller must have built
    /// it already (`FCALL` on a missing registry answers an error, not a
    /// panic).
    pub functions: &'a RefCell<Option<crate::scripting::FunctionRegistry>>,
    /// The ORIGINATING connection's identity. Every inner `redis.call` is
    /// authorized against it, whichever shard the body runs on (moon#569).
    pub script_acl: &'a crate::acl::ScriptAcl,
    pub num_shards: usize,
}

/// The script-execution commands the executor runs itself.
///
/// `SCRIPT` and `FUNCTION` are not here. They are connection-level
/// intercepts that fan out across shards, and they are filled in after the
/// body (moon#639/#697).
#[must_use]
pub(crate) fn is_txn_script(cmd: &[u8]) -> bool {
    const SCRIPTS: [&[u8]; 6] = [
        b"EVAL",
        b"EVALSHA",
        b"EVAL_RO",
        b"EVALSHA_RO",
        b"FCALL",
        b"FCALL_RO",
    ];
    SCRIPTS.iter().any(|s| cmd.eq_ignore_ascii_case(s))
}

/// Whether any queued command is a script, i.e. whether the caller must
/// build a [`TxnScripting`] for this `EXEC`.
#[must_use]
pub(crate) fn queue_has_script(queue: &[Frame]) -> bool {
    queue
        .iter()
        .filter_map(super::util::extract_command)
        .any(|(c, _)| is_txn_script(c))
}

/// The outcome of one queued script.
pub(crate) struct TxnScriptOutcome {
    /// The script's reply, for its slot in the `EXEC` array.
    pub reply: Frame,
    /// The durability records the script produced, in emission order.
    pub effects: Vec<CapturedEffect>,
    /// A flush the script performed. This shard's half is already done; the
    /// caller broadcasts the rest.
    pub flush: Option<PendingFlush>,
}

impl TxnScriptOutcome {
    fn refused(msg: &'static [u8]) -> Self {
        Self {
            reply: Frame::Error(Bytes::from_static(msg)),
            effects: Vec::new(),
            flush: None,
        }
    }
}

/// Run one queued script on this shard, in `db_idx`, synchronously.
///
/// Must not be called from inside a `with_shard` borrow: it takes its own.
pub(crate) fn run_txn_script(
    env: &TxnScripting<'_>,
    cmd: &[u8],
    args: &[Frame],
    db_idx: usize,
    shard_id: usize,
) -> TxnScriptOutcome {
    let is_fcall = cmd.eq_ignore_ascii_case(b"FCALL");
    let is_fcall_ro = cmd.eq_ignore_ascii_case(b"FCALL_RO");
    let is_body = cmd.eq_ignore_ascii_case(b"EVAL") || cmd.eq_ignore_ascii_case(b"EVAL_RO");
    let read_only = cmd.eq_ignore_ascii_case(b"EVAL_RO") || cmd.eq_ignore_ascii_case(b"EVALSHA_RO");

    // `try_borrow`, not `borrow`: this runs on the shard thread, where a panic
    // takes the whole process down. No `.await` separates this from any other
    // holder, so a conflict should be impossible, but refusing one command is
    // better than aborting the shard if that ever stops being true.
    let registry_guard = if is_fcall || is_fcall_ro {
        match env.functions.try_borrow() {
            Ok(g) => Some(g),
            Err(_) => {
                return TxnScriptOutcome::refused(b"ERR function registry busy on this shard");
            }
        }
    } else {
        None
    };
    let registry = match registry_guard.as_ref() {
        Some(g) => match g.as_ref() {
            Some(r) => Some(r),
            None => {
                return TxnScriptOutcome::refused(
                    b"ERR function registry unavailable on this shard",
                );
            }
        },
        None => None,
    };

    let ((reply, flush), effects) = crate::scripting::bridge::capture_txn_effects(|| {
        crate::shard::slice::with_shard(|s| {
            let db_count = s.databases.db_count();
            crate::scripting::pending_flush::run_and_complete(s, db_idx, |db| match registry {
                Some(reg) if is_fcall => crate::command::functions::handle_fcall(
                    reg,
                    args,
                    db,
                    shard_id,
                    env.num_shards,
                    db_idx,
                    db_count,
                    env.script_acl,
                ),
                Some(reg) => crate::command::functions::handle_fcall_ro(
                    reg,
                    args,
                    db,
                    shard_id,
                    env.num_shards,
                    db_idx,
                    db_count,
                    env.script_acl,
                ),
                None if is_body => crate::scripting::handle_eval(
                    env.lua,
                    env.script_cache,
                    args,
                    db,
                    shard_id,
                    env.num_shards,
                    db_idx,
                    db_count,
                    env.script_acl,
                    read_only,
                ),
                None => crate::scripting::handle_evalsha(
                    env.lua,
                    env.script_cache,
                    args,
                    db,
                    shard_id,
                    env.num_shards,
                    db_idx,
                    db_count,
                    env.script_acl,
                    read_only,
                ),
            })
        })
    });
    drop(registry_guard);
    // The moon#831 batch-barrier flag is how the live path learns a script
    // wrote. Here the captured `effects` carry that fact to the executor's own
    // barrier, so the flag is consumed and discarded. Leaving it armed would
    // make the NEXT live script on this thread think it wrote.
    let _ = crate::scripting::bridge::take_script_had_write();
    TxnScriptOutcome {
        reply,
        effects,
        flush,
    }
}

/// Publish every queued `EVAL`/`EVAL_RO` body to the other shards' script
/// caches before `EXEC` runs (moon#515).
///
/// Outside `MULTI` an `EVAL` caches its body server-wide, so a later `EVALSHA`
/// works whichever shard it reaches. The executor is synchronous and cannot
/// fan out, so the connection does it first. The fan-out is acked before the
/// body is sent, so a routed body finds its sha already cached on the owner.
pub(crate) async fn txn_script_prepass(
    ctx: &super::core::ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    queue: &[Frame],
) {
    if ctx.num_shards <= 1 {
        return;
    }
    for frame in queue {
        if let Some((cmd, args)) = super::util::extract_command(frame)
            && (cmd.eq_ignore_ascii_case(b"EVAL") || cmd.eq_ignore_ascii_case(b"EVAL_RO"))
        {
            super::shared::eval_script_fanout(ctx, shutdown, args).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_script_execution_command_is_claimed() {
        for c in [
            "EVAL",
            "evalsha",
            "Eval_Ro",
            "EVALSHA_RO",
            "fcall",
            "FCALL_RO",
        ] {
            assert!(is_txn_script(c.as_bytes()), "{c}");
        }
    }

    #[test]
    fn script_management_is_not_claimed() {
        // SCRIPT and FUNCTION are connection intercepts filled after the body.
        for c in ["SCRIPT", "FUNCTION", "EVALX", "FCALLS", "GET"] {
            assert!(!is_txn_script(c.as_bytes()), "{c}");
        }
    }

    #[test]
    fn queue_has_script_sees_a_script_anywhere_in_the_body() {
        let f = |parts: &[&'static str]| {
            Frame::Array(
                parts
                    .iter()
                    .map(|p| Frame::BulkString(Bytes::from_static(p.as_bytes())))
                    .collect::<Vec<_>>()
                    .into(),
            )
        };
        let plain = [f(&["SET", "a", "1"]), f(&["GET", "a"])];
        assert!(!queue_has_script(&plain));
        let mixed = [
            f(&["SET", "a", "1"]),
            f(&["EVAL", "return 1", "0"]),
            f(&["GET", "a"]),
        ];
        assert!(queue_has_script(&mixed));
    }
}
