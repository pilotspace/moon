//! Shared MOVE / `COPY ... DB n` two-database intercept for every
//! `ShardMessage` SPSC arm (Gap A).
//!
//! `MOVE` and `COPY ... DB n` each need two `&mut Database` borrows at once
//! (source + destination) — something the generic per-db write path
//! (`cmd_dispatch` / `key_extra::copy`) cannot provide, since it only ever
//! sees one `&mut Database`. The plain `Execute` arm in `spsc_handler.rs`
//! special-cased both commands ahead of the generic path; this module
//! extracts that logic into one helper so every other `ShardMessage` arm
//! (`MultiExecute`, `PipelineBatch`, `ExecuteSlotted`, `MultiExecuteSlotted`,
//! `PipelineBatchSlotted`) reuses it verbatim instead of silently falling
//! through to the single-db path — which, before this fix, meant COPY with
//! a `DB` clause silently performed a same-db copy (wrong-db data
//! corruption) and MOVE returned a loud-but-wrong "cross-db not supported"
//! error on every arm except the plain `Execute` one.

use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;

use crate::command::keyspace::move_cmd as ksmv;
use crate::config::RuntimeConfig;
use crate::protocol::Frame;
use crate::shard::shared_databases::ShardDatabases;
use crate::storage::Database;
use crate::storage::entry::CachedClock;
use crate::storage::tiered::spill_thread::SpillRequest;

/// Attempt the MOVE/`COPY ... DB n` two-database intercept for one command.
///
/// Returns `Some(response)` when `cmd` is `MOVE`, or `COPY` with a `DB`
/// clause targeting a database other than `db_idx` — the caller MUST treat
/// this as the command's full, final response: no generic dispatch, no COW
/// intercept, no auto-index hooks (mirrors the plain `Execute` arm's
/// pre-existing behavior, which returns before reaching any of those for
/// both commands — see the COW note in the Gap A commit body for why this
/// is intentional, not an oversight).
///
/// Returns `None` for anything else — not MOVE/COPY, or a same-db `COPY`
/// with no `DB` clause — so the caller falls through to the generic
/// single-db write path (`cmd_dispatch` → `key_extra::copy` for same-db
/// COPY).
///
/// Persistence (WAL/AOF) is deliberately NOT done here: the caller routes
/// the returned response through each arm's own per-command persistence
/// block, gated on `matches!(response, Frame::Integer(1))` — the same
/// condition the plain `Execute` arm already uses. This is STRICTER than
/// the batch arms' generic `!Error` persistence condition: a same-db
/// `MOVE`, or a `COPY` of a missing source key, returns `Integer(0)` and
/// must NOT persist.
#[allow(clippy::too_many_arguments)]
pub(crate) fn try_two_db_intercept(
    cmd: &[u8],
    args: &[Frame],
    databases: &mut [Database],
    db_idx: usize,
    db_count: usize,
    cached_clock: &CachedClock,
    evict_active: bool,
    shard_databases: &Arc<ShardDatabases>,
    shard_id: usize,
    runtime_config: &Arc<parking_lot::RwLock<RuntimeConfig>>,
    spill_sender: Option<&flume::Sender<SpillRequest>>,
    spill_file_id: &Rc<Cell<u64>>,
    disk_offload_dir: Option<&std::path::Path>,
) -> Option<Frame> {
    if cmd.eq_ignore_ascii_case(b"MOVE") {
        let response = match ksmv::parse_move_args(args, db_count) {
            Err(e) => e,
            Ok((_key, dst_db)) if dst_db == db_idx => Frame::Integer(0),
            Ok((key, dst_db)) => {
                // Refresh expiry clock on BOTH databases before the move so
                // an expired source key behaves as "not found" and an
                // expired destination key doesn't shadow the insert.
                ksmv::with_two_slice_dbs(databases, db_idx, dst_db, |src, dst| {
                    src.refresh_now_from_cache(cached_clock);
                    dst.refresh_now_from_cache(cached_clock);
                    ksmv::move_core(src, dst, &key)
                })
            }
        };
        return Some(response);
    }

    if cmd.eq_ignore_ascii_case(b"COPY") {
        // `?` here returns `None` from THIS function (not just the match) —
        // exactly the desired "no DB clause / same-db: fall through to
        // cmd_dispatch" behavior `parse_copy_db_args` documents.
        let copy_result = ksmv::parse_copy_db_args(args, db_idx, db_count)?;
        let response = match copy_result {
            Err(e) => e,
            Ok(ca) => ksmv::with_two_slice_dbs(databases, db_idx, ca.dst_db, |src, dst| {
                // Refresh expiry clock on BOTH dbs to mirror the single-db
                // write path: expired src/dst keys must resolve correctly
                // before copy_core inspects them.
                src.refresh_now_from_cache(cached_clock);
                dst.refresh_now_from_cache(cached_clock);
                // Unlike MOVE (net-zero — the key leaves src as it lands in
                // dst), cross-db COPY duplicates the value, so it must run
                // the same eviction gate as any other write. Gate on the
                // DESTINATION db (the one that grows) before copy_core.
                if evict_active {
                    if let Err(oom) = crate::shard::spsc_handler::spsc_eviction_gate(
                        dst,
                        ca.dst_db,
                        shard_databases,
                        shard_id,
                        runtime_config,
                        spill_sender,
                        spill_file_id,
                        disk_offload_dir,
                    ) {
                        return oom;
                    }
                }
                ksmv::copy_core(src, dst, &ca.src_key, &ca.dst_key, ca.replace)
            }),
        };
        return Some(response);
    }

    None
}
