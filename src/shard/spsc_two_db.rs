//! Shared MOVE / `COPY ... DB n` two-database intercept for every
//! `ShardMessage` SPSC arm (Gap A).
//!
//! `MOVE` and `COPY ... DB n` each need two `&mut Database` borrows at once
//! (source + destination) — something the generic per-db write path
//! (`cmd_dispatch` / `key_extra::copy`) cannot provide, since it only ever
//! sees one `&mut Database`. The plain `Execute` arm in `spsc_handler.rs`
//! special-cased both commands ahead of the generic path; this module
//! extracts that logic into one helper so every other `ShardMessage` arm
//! (`MultiExecute`, `PipelineBatchSlotted`; the dead `PipelineBatch`,
//! `ExecuteSlotted` and `MultiExecuteSlotted` arms were removed in moon#1198)
//! reuses it verbatim instead of silently falling
//! through to the single-db path — which, before this fix, meant COPY with
//! a `DB` clause silently performed a same-db copy (wrong-db data
//! corruption) and MOVE returned a loud-but-wrong "cross-db not supported"
//! error on every arm except the plain `Execute` one.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;

use crate::blocking::BlockingRegistry;
use crate::command::keyspace::move_cmd as ksmv;
use crate::config::RuntimeConfig;
use crate::protocol::Frame;
use crate::shard::db_plane::ShardDbSet;
use crate::shard::shared_databases::ShardDatabases;
use crate::storage::entry::CachedClock;
use crate::storage::tiered::spill_thread::SpillRequest;

/// What [`try_two_db_intercept`] did: the command's final response, and the
/// key it wrote into the OTHER database, if any.
///
/// moon#1069 serves the clients blocked on that key; moon#1056 makes the
/// CALLER do it, with [`wake_two_db_target`], AFTER it has logged the
/// command. A waiter served by that wake has its pop logged at the moment it
/// pops, so waking inside the intercept (before the arm's
/// `wal_append_and_fanout`) put the pop ahead of the MOVE that fed it, and
/// replay popped an empty key and then moved the element back in.
pub(crate) struct TwoDbOutcome {
    pub(crate) response: Frame,
    pub(crate) wake: Option<(usize, bytes::Bytes)>,
}

/// Serve the clients blocked on the key a MOVE / `COPY ... DB n` wrote into
/// `dst_db` — once the command is logged (see [`TwoDbOutcome`]). `wrote` is
/// "the command answered `:1`", read before any AOF error replaced the
/// reply: the key moved either way. A no-op unless someone waits there.
pub(crate) fn wake_two_db_target(
    blocking_registry: &RefCell<BlockingRegistry>,
    databases: &ShardDbSet,
    wake: Option<(usize, bytes::Bytes)>,
    wrote: bool,
) {
    let Some((dst_db, key)) = wake else {
        return;
    };
    if !wrote || !blocking_registry.borrow().has_waiters(dst_db, &key) {
        return;
    }
    let mut dst = databases.write(dst_db);
    crate::blocking::wakeup::wake_cross_db_write(
        blocking_registry,
        &mut dst,
        dst_db,
        &key,
        &Frame::Integer(1),
    );
}

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
    databases: &ShardDbSet,
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
) -> Option<TwoDbOutcome> {
    if cmd.eq_ignore_ascii_case(b"MOVE") {
        // `resolve_move` refuses `dst_db == db_idx` with redis's same-object
        // error (moon#1062), so `with_pair`'s distinct-index assert holds.
        let response = match ksmv::resolve_move(args, db_idx, db_count) {
            Err(e) => e,
            Ok((key, dst_db)) => {
                // Refresh expiry clock on BOTH databases before the move so
                // an expired source key behaves as "not found" and an
                // expired destination key doesn't shadow the insert.
                let reply = databases.with_pair(db_idx, dst_db, |src, dst| {
                    src.refresh_now_from_cache(cached_clock);
                    dst.refresh_now_from_cache(cached_clock);
                    ksmv::move_core(src, dst, &key)
                });
                // moon#1069: the key now exists in `dst_db`; the caller
                // wakes it once the MOVE is logged (moon#1056).
                return Some(TwoDbOutcome {
                    response: reply,
                    wake: Some((dst_db, key)),
                });
            }
        };
        return Some(TwoDbOutcome {
            response,
            wake: None,
        });
    }

    if cmd.eq_ignore_ascii_case(b"COPY") {
        // `?` here returns `None` from THIS function (not just the match) —
        // exactly the desired "no DB clause / same-db: fall through to
        // cmd_dispatch" behavior `parse_copy_db_args` documents.
        let copy_result = ksmv::parse_copy_db_args(args, db_idx, db_count)?;
        let (response, wake) = match copy_result {
            Err(e) => (e, None),
            Ok(ca) => databases.with_pair(db_idx, ca.dst_db, |src, dst| {
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
                    // task #34 (Wave A): a plain-dropped victim here (cross-db
                    // `COPY ... DB n` growing the destination past budget) is
                    // NOT wired to `record_reason_del` yet — this function
                    // deliberately does no WAL/AOF/replication I/O ("no
                    // persistence here" per the module doc), and threading
                    // those handles through for the comparatively rare
                    // cross-db COPY path is left as a follow-up alongside the
                    // other documented Wave-A gaps (db-quota eviction, Lua
                    // effects). A no-op sink preserves pre-#34 behavior.
                    if let Err(oom) = crate::shard::spsc_handler::spsc_eviction_gate(
                        dst,
                        ca.dst_db,
                        shard_databases,
                        shard_id,
                        runtime_config,
                        spill_sender,
                        spill_file_id,
                        disk_offload_dir,
                        &mut |_| {},
                    ) {
                        return (oom, None);
                    }
                }
                let reply = ksmv::copy_core(src, dst, &ca.src_key, &ca.dst_key, ca.replace);
                // moon#1069: the copy now exists in `ca.dst_db`; the caller
                // wakes it once the COPY is logged (moon#1056).
                (reply, Some((ca.dst_db, ca.dst_key.clone())))
            }),
        };
        return Some(TwoDbOutcome { response, wake });
    }

    None
}
