//! moon#1264 (b), the connection side: a `FLUSHALL` with save points saves
//! the flushed dataset before its reply goes out, as redis's
//! `flushallCommand` does (see
//! [`crate::command::persistence::save_after_flushall`]).
//!
//! The save must start only once EVERY shard is flushed — a shard that
//! picked up the save epoch before its own flush would abort it (a FLUSHALL
//! fails an unfinished epoch, moon#1228) — so it runs where the connection
//! that issued the flush has finished fanning it out: a plain `FLUSHALL`
//! (both runtimes' write paths), an `EXEC` whose body ran one (local and
//! routed, both runtimes), and a script that called one
//! (`shared::finish_script_flush`, every script entry point). One helper per
//! shape keeps each call site to a single line.

use super::core::ConnectionContext;
use crate::command::persistence;
use crate::protocol::Frame;

/// After a typed flush reached every shard: `cmd` is the flush command, and
/// only `FLUSHALL` saves.
pub(crate) async fn after_flush(cmd: &[u8], ctx: &ConnectionContext) {
    if cmd.eq_ignore_ascii_case(b"FLUSHALL") {
        after_flushall(ctx).await;
    }
}

/// After a script's flush: `flushall_everywhere` when it was a `FLUSHALL`
/// and it reached every shard.
pub(crate) async fn after_script_flush(flushall_everywhere: bool, ctx: &ConnectionContext) {
    if flushall_everywhere {
        after_flushall(ctx).await;
    }
}

/// After a `FLUSHALL` reached every shard.
async fn after_flushall(ctx: &ConnectionContext) {
    let save = persistence::save_points_now(&ctx.runtime_config);
    persistence::save_after_flushall(&ctx.snapshot_trigger_tx, ctx.num_shards, save).await;
}

/// After an `EXEC` broadcast its body's flushes (`exec_flushes`: result
/// index, command, db).
pub(crate) async fn after_txn(exec_flushes: &[(usize, Frame, usize)], ctx: &ConnectionContext) {
    let save = persistence::save_points_now(&ctx.runtime_config);
    persistence::save_after_txn_flushes(
        exec_flushes,
        &ctx.snapshot_trigger_tx,
        ctx.num_shards,
        save,
    )
    .await;
}
