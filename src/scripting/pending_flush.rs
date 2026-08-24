//! moon#685: the parts of a script-issued flush the bridge cannot perform.
//!
//! `scripting::bridge` reaches the keyspace through a single `&mut Database`
//! (`db.execute_command`) on a single shard. A flush is the one command whose
//! reach is defined in terms of things that borrow does not contain:
//!
//! | issued as             | must clear                                  |
//! |-----------------------|---------------------------------------------|
//! | `FLUSHDB`             | the selected db, on **every shard**         |
//! | `FLUSHALL`            | **every** db, on **every shard**            |
//!
//! — and on each of those shards, the vector/text index CONTENTS and the
//! durable MQ streams alongside the keyspace, which is what keeps a flushed
//! hash from staying searchable as a ghost (R3, task #46).
//!
//! On the connection path both extra dimensions are already handled — the
//! keyspace half by `server_admin::flush_every_database` (moon#677) and the
//! shard half by `coordinator::coordinate_flush_broadcast` (D-2). Neither is
//! reachable from inside the bridge: the first needs the `&mut [Database]`
//! slice the script's own `&mut Database` is borrowed out of, and the second
//! needs to `.await`, inside a synchronous `redis.call` closure.
//!
//! So the bridge RECORDS what the script asked for and the caller finishes it
//! one frame up, where both are back in scope. The record is per-shard-thread
//! state — never shared — so a `Cell` is the entire mechanism: no atomics, and
//! nothing for loom to model. Same shape as [`crate::persistence::snapshot_cow`],
//! for the same reason.
//!
//! # Why callers go through [`run_and_complete`] instead of draining by hand
//!
//! There are TWELVE script entry points — `EVAL`, `EVALSHA`, `FCALL` and
//! `FCALL_RO`, each in `handler_monoio`, `handler_sharded` and `spsc_handler`.
//! moon#677's lesson was that a missing arm is invisible to CI, and a drain
//! each of twelve sites has to remember is that hazard with a wider mouth.
//! `run_and_complete` owns the borrow dance, so a site that uses it cannot
//! forget the keyspace half, and a site that does not use it does not get a
//! `&mut Database` at all.
//!
//! # What is still not covered
//!
//! The four **routed** entry points (`shard::spsc_handler`, reached when
//! `route_script_elsewhere` sends a script to the shard that owns its declared
//! keys) get the keyspace half and not the broadcast: they run inside
//! `handle_shard_message_shared`, which is synchronous and holds neither the
//! SPSC producers nor the notifiers a broadcast needs. A script that both
//! declares a key on another shard AND flushes therefore still clears only the
//! owner shard. Measured and tracked as moon#705 rather than papered over.

use std::cell::Cell;

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Database;

/// Which flush a script issued, and therefore how far it still has to reach.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum PendingFlush {
    /// `FLUSHDB` — the bridge cleared the selected database on this shard;
    /// every other shard's copy of that database is still full.
    Db,
    /// `FLUSHALL` — as above, plus the other fifteen databases on this shard,
    /// which [`run_and_complete`] clears.
    All,
}

thread_local! {
    /// What the script running on this shard thread flushed, if anything.
    /// One `Cell` load per `redis.call` that was not a flush.
    static PENDING: Cell<Option<PendingFlush>> = const { Cell::new(None) };
}

/// Record that a script issued a successful flush.
///
/// Keyed on the COMMAND NAME rather than on a flag the caller passes in:
/// `FLUSHDB` must keep its single-database scope, and a bool threaded down
/// here is one inverted call away from making it wipe the server. moon#677
/// made the same choice for the same reason.
pub(crate) fn arm(cmd: &[u8]) {
    let which = if cmd.eq_ignore_ascii_case(b"FLUSHALL") {
        PendingFlush::All
    } else if cmd.eq_ignore_ascii_case(b"FLUSHDB") {
        PendingFlush::Db
    } else {
        return;
    };
    // `All` subsumes `Db`, in either order: a script that calls both must
    // finish with the wider reach, not with whichever it happened to call
    // last.
    PENDING.with(|p| {
        if which == PendingFlush::All || p.get().is_none() {
            p.set(Some(which));
        }
    });
}

/// Take the record, leaving the thread clean for the next script.
fn take() -> Option<PendingFlush> {
    PENDING.with(|p| p.replace(None))
}

/// The command a remote shard has to run to match what the script did locally.
pub(crate) fn broadcast_frame(which: PendingFlush) -> Frame {
    let name: &'static [u8] = match which {
        PendingFlush::Db => b"FLUSHDB",
        PendingFlush::All => b"FLUSHALL",
    };
    // Shipped through `coordinate_flush_broadcast` as a `MultiExecute`, so
    // each remote shard runs it through its normal SPSC arm — dispatch, the
    // keyspace completion, per-shard AOF/WAL, and the vector/text/MQ hooks all
    // apply exactly as they do for a client-issued flush.
    Frame::Array(vec![Frame::BulkString(Bytes::from_static(name))].into())
}

/// Run a script entry point against `slice.databases[db_idx]` and complete
/// every LOCAL half of a flush it issued, reporting what is left for the caller
/// to broadcast.
///
/// Takes the whole `ShardSlice`, not just the databases, because "flush" means
/// three things on one shard and the keyspace is only the first:
///
///   * the databases `server_admin::flushall` could not reach (`FLUSHALL`),
///   * the vector and text index CONTENTS (`auto_flush_indexes` — R3; the
///     `FT.CREATE` definitions survive, matching restart semantics),
///   * the durable MQ streams (`auto_drop_mq_streams_on_flush` — task #46,
///     without which `replay_mq_wal` resurrects a flushed queue).
///
/// Doing the first without the other two would be worse than the bug this
/// module fixes: a flushed hash whose index entry survives is a searchable
/// ghost, and it is precisely the inconsistency between "the keys are gone"
/// and "the index still answers" that R3 exists to prevent. The typed path has
/// called all three together since R3; a remote shard reached by the broadcast
/// runs the whole set through its own SPSC arm. This is the local leg.
///
/// The pre-clear is not decoration: a previous script whose Lua error unwound
/// past its own completion would otherwise leave the record armed, and the
/// next, unrelated script on this shard thread would flush the whole server.
/// Clearing first makes the failure mode "a flush is dropped", never "a flush
/// happens that nobody asked for".
pub(crate) fn run_and_complete<R>(
    slice: &mut crate::shard::slice::ShardSlice,
    db_idx: usize,
    run: impl FnOnce(&mut Database) -> R,
) -> (R, Option<PendingFlush>) {
    let _ = take();
    let out = run(&mut slice.databases[db_idx]);
    let pending = take();
    if let Some(which) = pending {
        if which == PendingFlush::All {
            // `db_idx` is the database the bridge already cleared, so it is
            // skipped — exactly the contract `flush_every_database` documents.
            crate::command::server_admin::flush_every_database(&mut slice.databases, db_idx);
        }
        crate::shard::spsc_handler::auto_flush_indexes(
            &mut slice.vector_store,
            &mut slice.text_store,
            which == PendingFlush::Db,
            db_idx as u8,
        );
        crate::shard::mq_exec::auto_drop_mq_streams_on_flush(slice, db_idx);
    }
    (out, pending)
}
