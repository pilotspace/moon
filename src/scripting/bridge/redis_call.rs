//! `redis.call` / `redis.pcall`: the Lua-to-dispatch bridge, and a script's
//! two-database `MOVE` / `COPY` leg.

use std::cell::Cell;

use bytes::Bytes;
use mlua::prelude::*;

use crate::protocol::Frame;
use crate::storage::engine::StorageEngine;

use super::LuaEvictionCtx;
use super::script_state::{
    CURRENT_DB, CURRENT_DB_COUNT, CURRENT_DB_IDX, SCRIPT_ACL, SCRIPT_CALLER, SCRIPT_HAD_WRITE,
    SCRIPT_READ_ONLY,
};

/// Create a Lua function that bridges redis.call/redis.pcall to the Rust dispatch().
///
/// If `propagate_errors` is true (redis.call), Frame::Error results are raised as Lua errors.
/// If false (redis.pcall), errors are returned as {err = "..."} tables.
pub fn make_redis_call_fn(
    lua: &Lua,
    propagate_errors: bool,
    eviction_ctx: LuaEvictionCtx,
) -> mlua::Result<LuaFunction> {
    lua.create_function(move |lua, args: LuaMultiValue| {
        // Convert all Lua arguments to Frames
        // ARGUMENT conversion, not return-value conversion: Lua numbers
        // become bulk strings exactly as a wire client would send them
        // (see `lua_arg_to_frame`).
        let frames: Vec<Frame> = args
            .iter()
            .map(|v| crate::scripting::types::lua_arg_to_frame(lua, v))
            .collect::<mlua::Result<_>>()?;

        if frames.is_empty() {
            return Err(mlua::Error::RuntimeError(
                "ERR Please specify at least one argument for redis.call()".to_string(),
            ));
        }

        // Extract command name from first argument
        let cmd_bytes = match &frames[0] {
            Frame::BulkString(b) | Frame::SimpleString(b) => b.clone(),
            _ => {
                return Err(mlua::Error::RuntimeError(
                    "ERR Invalid command name".to_string(),
                ));
            }
        };

        // Access database via thread-local pointer (safe: single-threaded shard)
        let result = CURRENT_DB.with(|cell| {
            let ptr = cell.get() as *mut crate::storage::Database;
            if ptr.is_null() {
                return Err(mlua::Error::RuntimeError(
                    "ERR No database context".to_string(),
                ));
            }
            // SAFETY: Single-threaded shard guarantees exclusive access.
            // Pointer is valid for the entire script execution duration.
            let db = unsafe { &mut *ptr };
            let mut db_idx = CURRENT_DB_IDX.with(|c| c.get());
            let db_count = CURRENT_DB_COUNT.with(|c| c.get());

            // moon#569: authorize the INNER command against the caller's ACL.
            //
            // This is the whole fix, and it is placed FIRST — before the
            // SELECT intercept, before the read-only/replica gates, before
            // the eviction gate, before the COW pre-image capture and before
            // MONITOR is fed — so a denied command produces no side effect of
            // any kind, not even an observable one.
            //
            // Why it cannot be laundered: the check runs on the argv that is
            // about to be executed, at the one place every script-issued
            // command must pass through. It is therefore blind to HOW the
            // script produced that argv (literal, `..` concatenation, a
            // closure, a nested `pcall`, a loop) and to which entry point ran
            // the script (EVAL / EVALSHA / FCALL / FCALL_RO, local or routed
            // to another shard). Key extraction is the shared, fail-closed
            // `acl::keyspec` walker, so movable-key commands (LMPOP, ZMPOP,
            // SORT ... STORE, COPY, GEORADIUS ... STORE) are covered and
            // anything it cannot enumerate is DENIED rather than waved
            // through.
            if let Some(reason) = SCRIPT_ACL.with_borrow(|acl| acl.check(&cmd_bytes, &frames[1..]))
            {
                return Ok(Frame::Error(Bytes::from(crate::acl::script_acl_error(
                    &reason,
                ))));
            }

            // Wave A (task #34): `redis.call('SELECT', ...)` used to silently
            // corrupt state — the generic dispatch SELECT handler mutates
            // only the LOCAL `db_idx` below, but every write in this closure
            // still lands on `db`, the ONE `Database` this script execution
            // is pinned to (the `CURRENT_DB` thread-local is set once, by
            // `set_script_db`, before the script starts). A script that
            // called SELECT therefore kept writing the ORIGINAL db while
            // looking like it had switched. Fail loud instead of corrupting
            // a second db; a real multi-db-scripts feature is a follow-up.
            if cmd_bytes.eq_ignore_ascii_case(b"SELECT") {
                return Ok(Frame::Error(Bytes::from_static(
                    b"ERR SELECT inside scripts is not supported by moon yet",
                )));
            }

            // Reject non-readonly commands in read-only mode (FCALL_RO / EVAL_RO,
            // a `no-writes` function). Use positive allowlist (READONLY flag)
            // instead of negative blocklist (!WRITE) to also block PUBLISH and
            // other side-effecting commands. Redis rejects WRITE | MAY_REPLICATE;
            // moon's PUBLISH / SPUBLISH carry neither flag (MAY_REPLICATE also
            // drives AOF admission here), so a WRITE-only rule would let them
            // through where Redis refuses them.
            //
            // PR #1301 review round 3: an ordinary command error, as in Redis 7
            // (`scriptVerifyWriteCommandAllow`) — `redis.call` raises it below,
            // `redis.pcall` returns it as `{err = ...}` and the script goes on.
            // It used to be a Lua error even from `redis.pcall`.
            let cmd_is_readonly = crate::command::metadata::is_read(&cmd_bytes);
            let cmd_is_write = crate::command::metadata::is_write(&cmd_bytes);
            if SCRIPT_READ_ONLY.with(|c| c.get()) && !cmd_is_readonly {
                return Ok(Frame::Error(Bytes::from_static(
                    crate::scripting::ERR_RO_SCRIPT_WRITE,
                )));
            }
            // Task #38: reject a write attempted from a CLIENT-issued script
            // on a read-only replica, at the first offending `redis.call`/
            // `redis.pcall` — matching upstream Redis (a script that never
            // writes still runs on a replica; one that does is aborted mid-
            // script with `-READONLY`). This intentionally mirrors the exact
            // carve-outs `try_enforce_readonly` uses for the connection-level
            // gate (`server/conn/handler_monoio/dispatch.rs`) — commands that
            // are blanket-`WRITE`-flagged in `COMMAND_META` but carry
            // read-only subcommands. Master→replica Lua effect replication
            // (Wave A part 2, task #34) NEVER reaches this closure: replayed
            // effects are applied via `replication::apply::apply_local`,
            // which dispatches the inner command directly against storage
            // and never runs a Lua VM at all (see that module's doc comment)
            // — so this check cannot collide with, or block, replica apply.
            if cmd_is_write && eviction_ctx.is_replica() {
                let allowed_on_replica = if cmd_bytes.eq_ignore_ascii_case(b"WS") {
                    crate::command::workspace::is_ws_readonly_subcommand(&frames[1..])
                } else if cmd_bytes.eq_ignore_ascii_case(b"MQ") {
                    crate::command::mq::is_mq_readonly_subcommand(&frames[1..])
                } else {
                    #[cfg(feature = "graph")]
                    {
                        cmd_bytes.eq_ignore_ascii_case(b"GRAPH.QUERY")
                            && !crate::command::graph::is_cypher_write_query(&frames[1..])
                    }
                    #[cfg(not(feature = "graph"))]
                    {
                        false
                    }
                };
                if !allowed_on_replica {
                    return Ok(Frame::Error(Bytes::from_static(
                        b"READONLY You can't write against a read only replica.",
                    )));
                }
            }
            // moon#592: a two-key write issued from Lua against keys that do
            // not all belong to ONE shard. A script executes against the
            // single `Database` slice this thread owns, so the key it did not
            // route on — `RENAME`'s destination, a `*STORE`'s sources — is
            // read from and written to THIS shard's table under the right
            // name, where every normally-routed access is blind to it. The
            // script returned success and the data was gone.
            //
            // `route_script_keys` already refuses a script whose DECLARED keys
            // straddle shards. This closes the hole it cannot see: a
            // destination passed through `ARGV` (or built in Lua) is never
            // part of the routing decision at all, so
            // `EVAL "redis.call('RENAME', KEYS[1], ARGV[1])" 1 src dst` was
            // acked while destroying `src` and never creating `dst` —
            // reproduced at `--shards 4`.
            //
            // Refusal, not a hop, for the same reason as the connection-level
            // guard: it is decided from the key names before anything is
            // touched, so an aborted script leaves the keyspace untouched.
            //
            // NOT the same as "every key a script touches must be local" —
            // that stronger rule would also catch a single-key write to an
            // undeclared remote key, and is a separate decision with a much
            // wider blast radius (tracked as a follow-up).
            //
            // The SCRIPT variant of the guard (moon#1133) also claims a plain
            // `COPY`: on a connection it is coordinator-routed and correct
            // across shards, but a script has no coordinator, so an
            // undeclared remote destination was written into this slice.
            if let Some(err) = crate::server::conn::shared::script_cross_shard_rejection(
                &cmd_bytes,
                &frames[1..],
                crate::command::connection::shard_count(),
            ) {
                return Ok(err);
            }

            // Where this write's TXN undo captures begin: an error reply
            // takes them back (see `txn_undo_discard`).
            let mut txn_mark = None;
            if cmd_is_write {
                // moon#1285 (PR #1301 review): inside an open TXN, a write
                // TXN.ABORT could not undo is refused BEFORE any effect —
                // eviction, COW capture, MONITOR, the write itself — and the
                // keys to undo-capture are planned once, here. One
                // thread-local borrow when no TXN capture is armed.
                let txn_keys = match super::txn_capture::txn_undo_plan(
                    &cmd_bytes,
                    &frames[1..],
                    db_idx,
                    db_count,
                ) {
                    Ok(keys) => keys,
                    Err(refused) => return Ok(refused),
                };
                // Track writes for SCRIPT KILL safety check
                SCRIPT_HAD_WRITE.with(|c| c.set(true));
                // OOM eviction gate (M3): mirrors the connection handlers'
                // `run_write_eviction_gate` — without this, a write inside a
                // script could grow memory past `maxmemory` without limit
                // (EVAL/EVALSHA carry no WRITE command flag, so the
                // dispatch-level OOM check never sees them at all).
                if let Err(oom) = eviction_ctx.gate_command(&cmd_bytes, db, db_idx) {
                    return Ok(oom);
                }
                // moon#517: snapshot COW. `spsc_handler::cow_intercept` can
                // only run where the shard event loop's
                // `&mut Option<SnapshotState>` is in scope; a script runs on
                // a connection task (or inside the routed `Execute` arm) and
                // reaches the keyspace from HERE. Capturing at the
                // `redis.call` level is also the only level that sees a real
                // key — `EVAL <script> <numkeys> k`'s own `command[1]` is
                // the SCRIPT BODY, so wrapping the three `handle_eval` call
                // sites in `cow_intercept` would have stashed the script
                // text under a bogus key and still lost every pre-image.
                // Must run AFTER the eviction gate (which can itself mutate
                // the db) and BEFORE `execute_command` overwrites the value.
                // One thread-local `bool` load when no BGSAVE is in flight.
                crate::persistence::snapshot_cow::capture_command_pre_image(db, db_idx, &frames);
                // moon#1285 (PR #1301 review): the TXN's undo pre-images,
                // after the eviction gate and right before the write — the
                // connection leg's order. `None` outside a TXN.
                if let Some(keys) = txn_keys {
                    txn_mark = super::txn_capture::txn_undo_capture(db, db_idx, &cmd_bytes, keys);
                }
            }

            // MONITOR: a script-issued command is fed with the literal `lua`
            // in place of a peer address — measured against redis-server 8.6.1,
            // which emits `[0 lua] "SET" "lk" "v"` after the client's own
            // `[0 127.0.0.1:… ] "eval" …` line. This is the one hook site with
            // no connection behind it, so the handler-level hooks structurally
            // cannot cover it: without this call an operator watching a
            // script-driven workload sees every EVAL and none of its effects.
            //
            // Fed BEFORE execution, matching every other hook site, so ordering
            // is issue-order. Costs one `Relaxed` load per `redis.call` when no
            // monitor is attached.
            crate::monitor::feed_frames(db_idx, "lua", &cmd_bytes, &frames[1..]);

            // moon#1068: `MOVE` and `COPY ... DB n` write a SECOND database,
            // which `execute_command` (one `&mut Database`) cannot reach: MOVE
            // hit its "requires handler-level dispatch" arm, and COPY ignored
            // the DB clause and wrote the copy into THIS db while the effect
            // record below named db n — so replicas and replay put the key
            // where the master did not. Same resolver and cores as the
            // connection, MULTI and replay paths (`move_cmd::resolve_two_db`).
            // `None` = an ordinary command, including a same-db COPY.
            let two_db = crate::command::keyspace::move_cmd::resolve_two_db(
                &cmd_bytes,
                &frames[1..],
                db_idx,
                db_count,
            );
            let (frame, cross_db_write) = match two_db {
                None => (
                    db.execute_command(&cmd_bytes, &frames[1..], &mut db_idx, db_count),
                    None,
                ),
                Some(Err(reply)) => (reply, None),
                Some(Ok(op)) => {
                    let reply = run_two_db_op(db, db_idx, &op, &eviction_ctx);
                    let target = matches!(reply, Frame::Integer(1)).then(|| op.into_target());
                    (reply, Some(target))
                }
            };

            // moon#1285 (PR #1301 review): a write that answered an error
            // wrote nothing — the invariant the effect record relies on too
            // (`serialize_effect_for_log` logs nothing for an error reply) —
            // so the pre-images captured for it are taken back. Kept, the
            // abort would restore them over another client's writes
            // (`SET k v BADOPT`, `ZMPOP 1 k JUNK`), and their write intents
            // would hide the keys from other transactions.
            if let Some(mark) = txn_mark
                && matches!(frame, Frame::Error(_))
            {
                super::txn_capture::txn_undo_discard(mark);
                // moon#1299: a key another open TXN holds — a guard refusal.
                if crate::transaction::isolation::is_conflict_reply(&frame) {
                    super::txn_capture::txn_note_conflict(&cmd_bytes);
                }
            }

            // moon#685: a flush issued from Lua has to reach as far as the
            // same flush issued on the connection — `FLUSHDB` the selected
            // database on every shard, `FLUSHALL` every database on every
            // shard. This closure holds ONE `&mut Database` on ONE shard, so
            // it can do neither extra dimension: the keyspace half needs the
            // slice this borrow came out of, and the shard half needs to
            // `.await`. Record what was asked for; `pending_flush` finishes it
            // one frame up, where both are in scope again.
            //
            // Deferring is not a compromise on ordering: `redis.call('SELECT')`
            // is refused a few dozen lines above, and a script's writes are
            // pinned to this shard, so nothing it does between here and its
            // return can observe the databases still waiting to be cleared.
            //
            // Keyed on the command NAME rather than a flag passed down from
            // the caller — `FLUSHDB` must keep its single-database scope, and
            // a bool threaded through here is one inverted call from making it
            // wipe the server (moon#677 made the same choice for the same
            // reason).
            if !matches!(frame, Frame::Error(_)) {
                crate::scripting::pending_flush::arm(&cmd_bytes);
            }

            // Wave A part 2 (task #34): dual-plane (AOF + replication)
            // emission of the effect, immediately after each successful
            // write — not batched to script end, so a partially-failing
            // script still durably records its completed writes. Skipped
            // for FCALL_RO / EVAL_RO (`cmd_is_write` implies the read-only
            // gate above already passed, so a write here only happens in a
            // normal read-write script).
            match cross_db_write {
                // moon#1068: a two-database write is recorded only when it
                // wrote (`:1`), verbatim, under the SOURCE db — the rule every
                // live path follows and what redis logs. A replica and replay
                // apply it with the same cores, into the db it names. The
                // woken key is the one in the DESTINATION db, recorded after
                // the effect so the wake follows the log (moon#1056).
                Some(Some((dst_db, key))) => {
                    eviction_ctx.emit_effect(db_idx, &frames, &frame);
                    crate::blocking::wakeup::note_script_cross_db_write(dst_db, &key);
                }
                // `:0` (source missing, destination occupied) or an error:
                // nothing was written, so nothing is logged or woken.
                Some(None) => {}
                None if cmd_is_write && !matches!(frame, Frame::Error(_)) => {
                    eviction_ctx.emit_effect(db_idx, &frames, &frame);
                    // moon#1069: a key this write created may have a client
                    // blocked on it. Recorded, not served: the waiters see the
                    // script's result once it has finished, exactly as redis
                    // serves its ready keys after EVAL returns, and this
                    // closure has no registry in scope.
                    crate::blocking::wakeup::note_script_write(db_idx, &cmd_bytes, &frames[1..]);
                }
                None => {}
            }

            // moon#1089: CLIENT TRACKING sees every command a script runs, as
            // redis applies it inside `call()` — a write invalidates the keys
            // it modified, and a read registers its keys for the client that
            // ran the script, under that client's OPTIN/OPTOUT/CACHING state.
            // The connection handlers' own hooks cannot: they see `EVAL`, not
            // what it did. This is the one place every script command passes,
            // on whichever shard the script runs (the tracking table is
            // process-global). Recorded here without a lock and applied under
            // one lock when the script ends (`clear_script_db`). Flushes are
            // invalidated where the script's flush is completed
            // (`finish_script_flush`). One relaxed load per `redis.call` when
            // nobody is tracking.
            if crate::tracking::tracking_active() && !matches!(frame, Frame::Error(_)) {
                SCRIPT_CALLER
                    .with(Cell::get)
                    .after_script_command(&cmd_bytes, &frames[1..]);
            }

            Ok(frame)
        })?;

        // redis.call: propagate errors as Lua errors
        // redis.pcall: return errors as {err = "..."} tables (handled by frame_to_lua_value)
        if propagate_errors {
            if let Frame::Error(e) = &result {
                return Err(mlua::Error::RuntimeError(
                    String::from_utf8_lossy(e).to_string(),
                ));
            }
        }

        crate::scripting::types::frame_to_lua_value(lua, &result)
    })
}

/// moon#1068: run a script's `MOVE` / `COPY ... DB n` between `src` — the
/// database the script is pinned to, whose guard the caller already holds —
/// and the destination database `op` names, reached through
/// [`crate::shard::slice::with_second_shard_db`].
///
/// A `COPY` grows the destination, so it runs the same eviction gate as any
/// other write, against the destination (the connection path's rule, in
/// `spsc_two_db`); a `MOVE` is net-zero and the caller has already gated the
/// source. The destination's expiry clock is refreshed first, as the MULTI
/// executor does, so an expired destination key cannot block the write.
///
/// On a thread outside the registered database plane (unit-test slices) the
/// destination cannot be reached, and the command is refused rather than
/// applied to the one database in hand.
fn run_two_db_op(
    src: &mut crate::storage::Database,
    src_idx: usize,
    op: &crate::command::keyspace::move_cmd::TwoDbOp,
    eviction_ctx: &LuaEvictionCtx,
) -> Frame {
    use crate::command::keyspace::move_cmd::TwoDbOp;
    let dst_db = op.dst_db();
    crate::shard::slice::with_second_shard_db(dst_db, |dst| {
        dst.refresh_now();
        if matches!(op, TwoDbOp::Copy(_))
            && let Err(oom) = eviction_ctx.gate(dst, dst_db)
        {
            return oom;
        }
        op.apply(src, src_idx, dst)
    })
    .unwrap_or_else(|| {
        Frame::Error(Bytes::from_static(
            b"ERR MOVE/COPY could not lock its destination database",
        ))
    })
}
