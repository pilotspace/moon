//! Persistence tick helpers for the shard event loop.
//!
//! Extracted from shard/mod.rs. Contains snapshot begin handling,
//! auto-save trigger checking, snapshot advance/finalize prep, and WAL flush.

use std::sync::Arc;

use tracing::info;

use crate::persistence::snapshot::SnapshotState;
use crate::runtime::channel;

use super::shared_databases::ShardDatabases;

/// Compute `allocator_overhead_bytes` = `max(0, RSS - tracked_sum)` (task
/// #58, LOW-1). `tracked_sum` is every subsystem this build can currently
/// account for: DashTable+entries, vector (mutable+immutable), text (FTS),
/// graph (CSR), Lua script cache, PageCache resident buffers, and the
/// replication backlog ring. Saturating: a stale/racing snapshot where
/// `tracked_sum` transiently exceeds `rss` (all reads are independent
/// Relaxed loads, at most one 100ms tick apart) clamps to 0 rather than
/// underflowing. Observability only -- the result is published for INFO
/// memory / Prometheus and is never read by eviction or budget-gating code.
#[allow(clippy::too_many_arguments)]
pub(crate) fn compute_allocator_overhead(
    rss_bytes: usize,
    dashtable_bytes: usize,
    vector_bytes: usize,
    text_bytes: usize,
    graph_bytes: usize,
    lua_bytes: usize,
    pagecache_bytes: usize,
    repl_backlog_bytes: usize,
) -> usize {
    let tracked_sum = dashtable_bytes
        + vector_bytes
        + text_bytes
        + graph_bytes
        + lua_bytes
        + pagecache_bytes
        + repl_backlog_bytes;
    rss_bytes.saturating_sub(tracked_sum)
}

/// Handle a pending SnapshotBegin that was collected from SPSC drain.
///
/// If a snapshot is already in progress, sends an error reply.
/// Otherwise, creates a new SnapshotState and stores the reply_tx.
pub(crate) fn handle_pending_snapshot(
    pending: Option<(
        u64,
        std::path::PathBuf,
        channel::OneshotSender<Result<(), String>>,
    )>,
    snapshot_state: &mut Option<SnapshotState>,
    snapshot_reply_tx: &mut Option<channel::OneshotSender<Result<(), String>>>,
    shard_databases: &Arc<ShardDatabases>,
    disk_offload_dir: Option<&std::path::Path>,
    shard_id: usize,
    wal_last_lsn: u64,
) {
    if let Some((epoch, snap_dir, reply_tx)) = pending {
        if snapshot_state.is_some() {
            let _ = reply_tx.send(Err("Snapshot already in progress".to_string()));
        } else {
            let snap_path = if let Some(offload) = disk_offload_dir {
                let shard_dir = offload.join(format!("shard-{}", shard_id));
                let _ = std::fs::create_dir_all(&shard_dir);
                shard_dir.join(format!("shard-{}.rrdshard", shard_id))
            } else {
                snap_dir.join(format!("shard-{}.rrdshard", shard_id))
            };
            let segment_counts = crate::shard::slice::with_shard(|s| {
                // One consistent snapshot of every db's segment layout: the
                // counts must all describe the same instant, or the bitmap
                // sized here would not match the keyspace the capture walks.
                s.databases.with_all_read(|dbs| {
                    dbs.iter()
                        .map(|db| db.data().segment_count())
                        .collect::<Vec<_>>()
                })
            });
            let db_count = shard_databases.db_count();
            let mut state = SnapshotState::new_from_metadata(
                shard_id as u16,
                epoch,
                db_count,
                segment_counts.clone(),
                snap_path,
            );
            // P3c — stamp the WAL LSN so PITR can pick this snapshot as a
            // valid replay base. 0 means "no WAL writer active" (e.g. pure
            // RDB mode) — header records 0, recovery falls back to full replay.
            if wal_last_lsn > 0 {
                state.set_last_lsn(wal_last_lsn);
            }
            // moon#1186: stream blocks to an off-shard-thread writer instead
            // of buffering the whole file for a write+fsync on this thread.
            start_snapshot_streaming(&mut state, shard_id);
            // moon#517: arm off-loop COW capture (Lua script writes) for
            // the life of this snapshot. See `persistence::snapshot_cow`.
            // moon#1186: with the layout, so written segments are skipped.
            crate::persistence::snapshot_cow::arm_with_layout(segment_counts);
            *snapshot_state = Some(state);
            *snapshot_reply_tx = Some(reply_tx);
        }
    }
}

/// Check the watch channel for auto-save snapshot triggers.
///
/// If the epoch has advanced and no snapshot is in progress, creates a new SnapshotState.
pub(crate) fn check_auto_save_trigger(
    snapshot_trigger_rx: &channel::WatchReceiver<u64>,
    last_snapshot_epoch: &mut u64,
    snapshot_state: &mut Option<SnapshotState>,
    shard_databases: &Arc<ShardDatabases>,
    persistence_dir: &Option<String>,
    disk_offload_dir: Option<&std::path::Path>,
    shard_id: usize,
    wal_last_lsn: u64,
) {
    let new_epoch = snapshot_trigger_rx.borrow();
    if new_epoch > *last_snapshot_epoch && snapshot_state.is_none() {
        *last_snapshot_epoch = new_epoch;
        // #366: consume the epoch but skip the save while the data directory
        // is missing — the snapshot temp file can't be created, and retrying
        // per save-point trigger just spams one doomed attempt per epoch.
        if crate::shard::disk_monitor::is_dir_lost() {
            tracing::warn!(
                "Shard {}: auto-save epoch {} skipped — data directory missing",
                shard_id,
                new_epoch
            );
            // moon#1230: this shard's part of the save is over, and it
            // failed. Unreported, the fan-in counter never reached zero:
            // `rdb_bgsave_in_progress:1` forever, every later BGSAVE
            // refused as "already in progress".
            crate::command::persistence::bgsave_shard_done(false);
            return;
        }
        let Some(dir) = persistence_dir else {
            // No persistence directory (`--appendonly no` without `--save`):
            // nowhere to write. Report the failure instead of leaving the
            // save in progress forever (moon#1230).
            tracing::warn!(
                "Shard {}: snapshot epoch {} not written — no persistence directory \
                 (--appendonly no and no --save); the save is reported failed",
                shard_id,
                new_epoch
            );
            crate::command::persistence::bgsave_shard_done(false);
            return;
        };
        // When disk-offload is enabled, write snapshot to the offload shard directory
        // so v3 recovery can find it alongside WAL v3 segments and manifest.
        let snap_path = if let Some(offload) = disk_offload_dir {
            let shard_dir = offload.join(format!("shard-{}", shard_id));
            let _ = std::fs::create_dir_all(&shard_dir);
            shard_dir.join(format!("shard-{}.rrdshard", shard_id))
        } else {
            std::path::PathBuf::from(dir).join(format!("shard-{}.rrdshard", shard_id))
        };
        let segment_counts = crate::shard::slice::with_shard(|s| {
            // One consistent snapshot of every db's segment layout: the
            // counts must all describe the same instant, or the bitmap
            // sized here would not match the keyspace the capture walks.
            s.databases.with_all_read(|dbs| {
                dbs.iter()
                    .map(|db| db.data().segment_count())
                    .collect::<Vec<_>>()
            })
        });
        let db_count = shard_databases.db_count();
        let mut state = SnapshotState::new_from_metadata(
            shard_id as u16,
            new_epoch,
            db_count,
            segment_counts.clone(),
            snap_path,
        );
        // P3c — stamp the WAL LSN before the header is written.
        if wal_last_lsn > 0 {
            state.set_last_lsn(wal_last_lsn);
        }
        // moon#1186: same streaming writer as the explicit-BGSAVE path.
        start_snapshot_streaming(&mut state, shard_id);
        // moon#517: same arming as the explicit-BGSAVE path above.
        crate::persistence::snapshot_cow::arm_with_layout(segment_counts);
        *snapshot_state = Some(state);
    }
}

/// Attach the off-shard-thread writer to a new snapshot (moon#1186). A
/// thread that cannot be spawned leaves the in-memory path in place; the
/// finalize still runs off-thread if a thread can be had by then.
fn start_snapshot_streaming(state: &mut SnapshotState, shard_id: usize) {
    if let Err(e) = state.start_streaming() {
        tracing::warn!(
            "Shard {}: snapshot writer thread unavailable ({}); buffering in memory",
            shard_id,
            e
        );
    }
}

/// Drive a complete snapshot's finalize (moon#1186): start it on the writer
/// thread (the EOF marker, CRC footer, fsync, rename and directory fsync all
/// run there) and poll its outcome WITHOUT blocking. Returns `Some(true)` /
/// `Some(false)` once the snapshot published / failed — the success or error
/// handler has already run — and `None` while the writer is still working.
pub(crate) fn drive_snapshot_finalize(
    snapshot_state: &mut Option<SnapshotState>,
    snapshot_reply_tx: &mut Option<channel::OneshotSender<Result<(), String>>>,
    shard_id: usize,
) -> Option<bool> {
    let snap = snapshot_state.as_mut()?;
    if !snap.finalize_started() {
        if let Err(e) = snap.begin_finalize() {
            finalize_snapshot_error(snapshot_state, snapshot_reply_tx, shard_id, &e.to_string());
            return Some(false);
        }
        // Every segment is written: no pre-image can be needed any more.
        crate::persistence::snapshot_cow::disarm();
    }
    match snapshot_state.as_ref()?.poll_finalize()? {
        Ok(()) => {
            finalize_snapshot_success(snapshot_state, snapshot_reply_tx, shard_id);
            Some(true)
        }
        Err(e) => {
            finalize_snapshot_error(snapshot_state, snapshot_reply_tx, shard_id, &e);
            Some(false)
        }
    }
}

/// Advance snapshot one segment and check if done (synchronous part).
///
/// Returns `true` if the snapshot is complete and ready for finalization
/// ([`drive_snapshot_finalize`]) — also when its writer thread already
/// failed, so the failure is reported instead of serializing the rest.
pub(crate) fn advance_snapshot_segment(
    snapshot_state: &mut Option<SnapshotState>,
    shard_databases: &Arc<ShardDatabases>,
    shard_id: usize,
) -> bool {
    let _ = shard_id; // E2 removes
    if let Some(snap) = snapshot_state {
        // moon#517: fold pre-images captured off this stack (Lua script
        // writes) into the snapshot BEFORE another segment is marked
        // serialized — the drain filters on the segment bitmap, so advancing
        // first would discard a pre-image that was still needed when it was
        // taken.
        crate::persistence::snapshot_cow::drain_into(snap);
        if snap.finalize_started() || snap.stream_failed() {
            return true;
        }
        // moon#1186: the writer thread is too far behind the disk — skip
        // this tick's segment rather than grow the in-flight backlog.
        if snap.stream_backlogged() {
            return false;
        }
        let current_db = snap.current_db_index();
        let db_count = shard_databases.db_count();
        if current_db < db_count {
            let done =
                crate::shard::slice::with_shard_db(current_db, |db| snap.advance_budgeted_db(db));
            // Captures from here on are filtered against the new cursor.
            crate::persistence::snapshot_cow::note_progress(snap.current_db_index(), snap.cursor());
            done
        } else {
            // All databases serialized, return true to trigger finalization
            true
        }
    } else {
        // Belt-and-braces: whatever path cleared `snapshot_state` (normal
        // finalize, an error finalize, or any future one), capture must not
        // stay armed against a snapshot that no longer exists — a queue
        // nobody drains would grow for the life of the shard.
        if crate::persistence::snapshot_cow::is_armed() {
            crate::persistence::snapshot_cow::disarm();
        }
        false
    }
}

/// Handle successful snapshot finalization: send reply.
///
/// WAL v2's per-snapshot `truncate_after_snapshot(epoch)` had no v3
/// equivalent -- v3 retention is LSN-driven (`WalWriterV3::recycle_aggressive`
/// / `recycle_segments_before`, invoked from autovacuum Pass C and the
/// checkpoint protocol) and runs independently of legacy RRDSHARD snapshot
/// epochs, so this handler no longer touches the WAL writer at all.
pub(crate) fn finalize_snapshot_success(
    snapshot_state: &mut Option<SnapshotState>,
    snapshot_reply_tx: &mut Option<channel::OneshotSender<Result<(), String>>>,
    shard_id: usize,
) {
    if let Some(snap) = snapshot_state.as_ref() {
        let epoch = snap.epoch;
        info!("Shard {}: snapshot epoch {} complete", shard_id, epoch);
        if let Some(tx) = snapshot_reply_tx.take() {
            let _ = tx.send(Ok(()));
        }
    }
    // moon#517: the file is closed — a pre-image has nowhere left to go.
    crate::persistence::snapshot_cow::disarm();
    *snapshot_state = None;
}

/// Handle failed snapshot finalization: send error reply.
pub(crate) fn finalize_snapshot_error(
    snapshot_state: &mut Option<SnapshotState>,
    snapshot_reply_tx: &mut Option<channel::OneshotSender<Result<(), String>>>,
    shard_id: usize,
    error: &str,
) {
    tracing::error!("Shard {}: snapshot finalize failed: {}", shard_id, error);
    if let Some(tx) = snapshot_reply_tx.take() {
        let _ = tx.send(Err(format!("finalize failed: {}", error)));
    }
    // moon#517: the file is closed — a pre-image has nowhere left to go.
    crate::persistence::snapshot_cow::disarm();
    *snapshot_state = None;
}

/// Flush WAL if buffer exceeds threshold (1ms tick -- write to page cache
/// only; durable sync is separate, see `timers::sync_wal_v3`).
///
/// Only active when the per-shard WAL writer was successfully initialized
/// (appendonly=yes; see the writer-creation block in `event_loop::run`).
pub(crate) fn flush_wal_v3_if_needed(
    wal_v3: &mut Option<crate::persistence::wal_v3::segment::WalWriterV3>,
) {
    // #366: while the data directory is missing, every flush that needs a
    // path operation (segment rotation) fails — at the 1ms tick cadence that
    // is a hot syscall+log error loop. The dir-lost latch already logged
    // loudly and refused new writes; skip until the directory heals.
    if crate::shard::disk_monitor::is_dir_lost() {
        return;
    }
    if let Some(wal) = wal_v3 {
        // Belt-and-suspenders for failure classes the latch can't see
        // (EACCES, a shallow heal missing nested dirs, …): a failed flush
        // arms a 1s backoff so no flush error can ever loop at tick cadence.
        if wal.flush_backing_off() {
            return;
        }
        if let Err(e) = wal.flush_if_needed() {
            wal.note_flush_failure();
            tracing::error!("WAL v3 flush failed (retrying in 1s): {}", e);
        } else {
            wal.clear_flush_backoff();
        }
    }
}

// ---------------------------------------------------------------------------
// Warm tier transition handler (disk-offload path)
// ---------------------------------------------------------------------------

/// Periodically check immutable segment ages/idle-time and trigger HOT->WARM
/// transitions.
///
/// Called from the event loop on a slower interval (e.g., every 10 seconds)
/// when disk-offload is enabled. Scans all VectorIndex segments, transitions
/// those older than `warm_after_secs` OR (WS3, when `idle_after_secs > 0`)
/// idle for at least `idle_after_secs` since their last search — whichever
/// threshold is reached first.
#[allow(clippy::too_many_arguments)]
pub(crate) fn check_warm_transitions(
    vector_store: &crate::vector::store::VectorStore,
    shard_dir: &std::path::Path,
    manifest: &mut ShardManifest,
    warm_after_secs: u64,
    idle_after_secs: u64,
    // The shard's one cold file_id counter (`file_id_seed::allocate_from`).
    spill_file_id: &std::cell::Cell<u64>,
    shard_id: usize,
    wal: &mut Option<WalWriterV3>,
) {
    // #366: warm-tier transitions write segment files under the offload dir
    // — doomed while the data directory is missing; skip until it heals.
    if crate::shard::disk_monitor::is_dir_lost() {
        return;
    }
    let count = crate::storage::tiered::file_id_seed::allocate_from(spill_file_id, |next| {
        vector_store.try_warm_transitions_all_idle(
            shard_dir,
            manifest,
            warm_after_secs,
            idle_after_secs,
            next,
            wal,
        )
    });
    if count > 0 {
        info!(
            "Shard {}: transitioned {} segment(s) to warm tier",
            shard_id, count
        );
    }
}

// ---------------------------------------------------------------------------
// Warm-segment mmap budget enforcement
// ---------------------------------------------------------------------------

/// 1 s tick: encode dirty text indexes (budgeted, duty-cycled) and hand the
/// bytes to the `.tpost` writer thread. No-op without the `text-index`
/// feature. See `docs/internal/text-postings-persistence.md`.
pub(crate) fn text_postings_tick(text_store: &mut crate::text::store::TextStore, shard_id: usize) {
    #[cfg(feature = "text-index")]
    {
        const TICK_BUDGET: std::time::Duration = std::time::Duration::from_millis(2);
        let n = text_store.persist_dirty_postings(TICK_BUDGET);
        if n > 0 {
            tracing::debug!(
                "Shard {}: queued {} text index(es) for .tpost write",
                shard_id,
                n
            );
        }
    }
    #[cfg(not(feature = "text-index"))]
    let _ = (text_store, shard_id);
}

/// Graceful shutdown: encode everything dirty and wait for the writer to
/// land it. A failure only costs the next boot a rebuild of the affected
/// indexes, so it is logged, never fatal.
pub(crate) fn persist_text_postings_on_shutdown(
    text_store: &mut crate::text::store::TextStore,
    shard_id: usize,
) {
    #[cfg(feature = "text-index")]
    {
        const SHUTDOWN_FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
        let started = std::time::Instant::now();
        match text_store.persist_all_postings_and_wait(SHUTDOWN_FLUSH_TIMEOUT) {
            Ok(0) => {}
            Ok(n) => info!(
                "Shard {}: persisted {} text index(es) to .tpost in {:.2?}",
                shard_id,
                n,
                started.elapsed()
            ),
            Err(e) => tracing::warn!(
                "Shard {}: text postings flush on shutdown failed ({e}); affected indexes rebuild on next boot",
                shard_id
            ),
        }
    }
    #[cfg(not(feature = "text-index"))]
    let _ = (text_store, shard_id);
}

/// Enforce the warm-segment resident-bytes budget across all vector indexes.
///
/// Called from the event loop on the warm-check timer (same 10s cadence as
/// `check_warm_transitions`). Registers any newly-added warm segments into
/// `budget`, then evicts LRU segments until resident bytes fall below the
/// configured limit.
///
/// The budget is per-shard and owned by the event loop; no locking is needed.
/// Eviction drops the `WarmSearchSegment` Arc from `SegmentList.warm` — the
/// on-disk .mpf files are preserved and the segment is reloaded transparently
/// on the next search.
pub(crate) fn enforce_warm_mmap_budget(
    vector_store: &crate::vector::store::VectorStore,
    budget: &mut crate::vector::persistence::mmap_budget::MmapBudget,
    shard_id: usize,
) {
    let total_evicted = vector_store.enforce_mmap_budget_all(budget);
    if total_evicted > 0 {
        info!(
            "Shard {}: mmap budget enforcer evicted {} warm segment(s) ({} B remaining)",
            shard_id,
            total_evicted,
            budget.current_resident_bytes(),
        );
    }
}

// ---------------------------------------------------------------------------
// Async spill completion polling (background pwrite thread)
// ---------------------------------------------------------------------------

/// Poll background spill thread for completed pwrite operations.
/// Run the eviction tick body shared between the tokio and monoio event
/// loops.
///
/// Drains background spill completions, runs the memory-pressure cascade if
/// enabled, otherwise falls back to plain `timers::run_eviction`. Every file
/// id it spills under is allocated from `spill_file_id`, the shard's one
/// counter (`file_id_seed::allocate_from`) — never from a copy of it.
///
/// Extracted from `event_loop.rs` so the file stays under the 1500-line cap
/// and so both runtime arms cannot drift.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_eviction_tick(
    spill_thread: Option<&crate::storage::tiered::spill_thread::SpillThread>,
    shard_manifest: &mut Option<crate::persistence::manifest::ShardManifest>,
    shard_databases: &std::sync::Arc<super::shared_databases::ShardDatabases>,
    shard_id: usize,
    server_config: &std::sync::Arc<crate::config::ServerConfig>,
    runtime_config: &std::sync::Arc<parking_lot::RwLock<crate::config::RuntimeConfig>>,
    page_cache: &Option<PageCache>,
    wal_v3_writer: &mut Option<crate::persistence::wal_v3::segment::WalWriterV3>,
    script_cache: &std::rc::Rc<std::cell::RefCell<crate::scripting::ScriptCache>>,
    // moon#506: the shard's ONE Lua VM slot (`event_loop`'s `lua_rc`, the same
    // cell `conn_accept` and `ShardLuaRuntime` fill). Sampled here rather than
    // at either event-loop tick site precisely because there are TWO tick sites
    // — a tokio `select!` arm and a monoio counter arm — and a publish written
    // into one of them is invisible on the other. This function is the body
    // they share, so the sample cannot drift per runtime.
    lua_vm_slot: &crate::scripting::ShardLuaSlot,
    spill_file_id: &std::rc::Rc<std::cell::Cell<u64>>,
    // task/issue #45: `<offload>/shard-{id}`, precomputed ONCE at shard init
    // (event_loop's `disk_offload_dir`) instead of a per-tick
    // `format!` + `join` allocation pair on this 100ms path. `None` iff
    // disk-offload is disabled — the same gate that nulls `spill_thread`
    // and `shard_manifest`, so every consumer below is already
    // Some-guarded.
    offload_shard_dir: Option<&std::path::Path>,
    // task #34 (Wave A): `record_reason_del` handles for plain-dropped
    // eviction victims — threaded straight through to `timers::run_eviction`
    // and `handle_memory_pressure`'s sync-spill-fallback branch.
    repl_backlog: &crate::replication::backlog::SharedBacklog,
    replica_txs: &mut Vec<crate::shard::dispatch::ReplicaFanout>,
    repl_state: &Option<crate::replication::state::OffsetHandle>,
    aof_pool: Option<&std::sync::Arc<crate::persistence::aof::AofWriterPool>>,
    wal_kv_log: bool,
) {
    if let Some(spill_t) = spill_thread {
        // moon#902: every published spill batch also logs a `MOON.SPILLED`
        // cut record to the AOF (AOF only — file ids are shard-local, so it
        // is never replicated).
        let mut marker_sink = ColdMarkerSink {
            aof_pool,
            wal_writer: wal_v3_writer.as_mut(),
            shard_id,
            wal_kv_log,
        };
        apply_spill_completions(
            spill_t,
            shard_manifest,
            shard_databases,
            shard_id,
            &mut marker_sink,
        );
    }

    // GAP-1: publish this shard's usage and refresh its elastic budget once
    // per 100ms tick. Siblings read the published snapshot on their own
    // ticks, so every budget is at most one tick stale — the same slack the
    // static scheme already has between eviction passes.
    // C5 / M4: publish vector/text/graph store memory for lock-free observers.
    // Uses the existing lock path (Wave E collapses to slice). Runs every tick
    // so Prometheus and MEMORY DOCTOR never see stale zero values for long.
    // A4 review (LOW): published BEFORE the KV publish + elastic recompute
    // below so the recompute's vector-aware donor/hot classification reads
    // THIS tick's vector figure, not last tick's (siblings' figures remain
    // ≤ 1 tick stale by design).
    let vector_resident_bytes = crate::shard::slice::with_shard(|s| {
        use std::sync::atomic::Ordering;
        let (mutable, immutable) = s.vector_store.resident_bytes();
        s.store_memory
            .vector
            .store(mutable + immutable, Ordering::Relaxed);
        // K4 (kernel-m2-brief-2026-07-12 stage 2): TextStore now has a real
        // resident_bytes() aggregate (posting lists, term dicts, FST
        // sidecars, TAG/NUMERIC indexes) -- this was hard-coded 0, making
        // FTS memory invisible to the elastic budget, MEMORY DOCTOR, and
        // Prometheus.
        s.store_memory
            .text
            .store(s.text_store.resident_bytes(), Ordering::Relaxed);
        #[cfg(feature = "graph")]
        {
            let graph_bytes = s.graph_store.resident_bytes();
            s.store_memory.graph.store(graph_bytes, Ordering::Relaxed);
        }
        #[cfg(not(feature = "graph"))]
        s.store_memory.graph.store(0, Ordering::Relaxed);
        // C4 (wave-5 hygiene): publish the shard's Lua script-cache byte
        // estimate alongside vector/text/graph so INFO/MEMORY DOCTOR and
        // Prometheus stop reporting a permanent zero for Lua memory.
        s.store_memory
            .lua
            .store(script_cache.borrow().resident_bytes(), Ordering::Relaxed);
        // moon#506: the script SOURCES published above are 48 bytes for
        // `return 1`; the VM that ran it holds ~25KB before it touches
        // anything, and unboundedly more once a script anchors tables in
        // `_G`. Publish the interpreter heap separately so `used_memory_lua`,
        // `moon_memory_bytes{kind="lua_scripts"}` and MEMORY DOCTOR report the
        // real footprint instead of the string length of the script text.
        // `None` = slot momentarily unsamplable: keep the previous value.
        if let Some(vm_bytes) = crate::scripting::vm_used_memory(lua_vm_slot) {
            s.store_memory.lua_vm.store(vm_bytes, Ordering::Relaxed);
        }
        // Return the vector resident total (HOT + WARM) so the pressure check
        // below can factor it in (memory-triggered vector offload, C).
        mutable + immutable
    });

    // task #58 (LOW-2): publish this shard's PageCache resident bytes
    // (actually-grown 4KB/64KB frame buffers, NOT the configured capacity --
    // see `PageCache::resident_buffer_bytes()`) alongside vector/text/graph/
    // lua above. Observability only: never read by eviction or budget gating.
    // `None` (disk-offload disabled) publishes 0, matching the disabled-
    // subsystem convention the other kinds already use.
    {
        use std::sync::atomic::Ordering;
        let pagecache_bytes = page_cache
            .as_ref()
            .map_or(0, PageCache::resident_buffer_bytes);
        shard_databases.store_memory_per_shard[shard_id]
            .pagecache
            .store(pagecache_bytes, Ordering::Relaxed);
    }

    // The shard's dead-slot ledger bytes, excluded from the pressure-cascade
    // trigger below.
    let cascade_ledger_bytes = {
        let rt = runtime_config.read();
        // C5 / Phase 3: compute per-shard KV memory via ShardSlice under one
        // batch of SHARED db guards (estimated_memory() is an O(1) accumulator
        // read, so the guards are held for a handful of loads and exclude
        // nobody — a shared guard never blocks a foreign reader, and this IS
        // the owner). Published unconditionally: MEMORY DOCTOR
        // and the Prometheus KV gauge read this atomic even when maxmemory is
        // unlimited — gating it on maxmemory > 0 left them at a permanent 0.
        //
        // K4: also charge each db's ColdIndex (disk-offload bookkeeping RAM
        // -- see storage::tiered::cold_index::ColdIndex::resident_bytes doc
        // comment). This is a per-db O(1) accumulator read, same complexity
        // class as estimated_memory() itself, so folding it in here does not
        // change this tick's cost -- and it is intentionally NOT folded into
        // Database::estimated_memory()/resident_bytes() themselves, which
        // stay untouched O(1) hot-path reads for the per-write eviction
        // pre-gate (inline_write_can_skip_eviction / evict_to_budget).
        //
        // `ci.resident_bytes()` includes the dead-slot ledger (moon#1215): it
        // is resident RAM, so `used_memory` and the elastic budget count it.
        // The pressure cascade does not (`ledger_bytes`, PR #1233 review): no
        // step of the cascade can free a ledger byte.
        let (used, ledger) = crate::shard::slice::with_shard(|s| {
            s.databases.with_all_read(|dbs| {
                dbs.iter().fold((0usize, 0usize), |(used, ledger), db| {
                    let ci = db.cold_index.as_ref();
                    (
                        used + db.estimated_memory() + ci.map_or(0, |ci| ci.resident_bytes()),
                        ledger + ci.map_or(0, |ci| ci.dead_slot_bytes()),
                    )
                })
            })
        });
        shard_databases.publish_memory(shard_id, used);
        // Elastic budgets only exist under a finite maxmemory cap.
        if rt.maxmemory > 0 {
            shard_databases.recompute_elastic_budget(shard_id, &rt);
        }
        ledger
    };

    // task #58 (LOW-1): publish `allocator_overhead_bytes` = RSS - tracked_sum
    // on this 100ms tick instead of only computing it on-demand (MEMORY
    // DOCTOR's existing on-demand formula in server_admin.rs is left as-is).
    // Only shard 0 performs this -- it needs a process-wide RSS read plus a
    // cross-shard sum, and every sibling's 100ms tick would otherwise repeat
    // the same syscall + O(num_shards) sum for an identical process-wide
    // number. Observability only: never read by eviction or budget gating.
    if shard_id == 0 {
        use std::sync::atomic::Ordering;
        let rss = crate::admin::metrics_setup::get_rss_bytes() as usize;
        let dashtable_bytes = shard_databases.read_memory_sum();
        let mut vector_bytes = 0usize;
        let mut text_bytes = 0usize;
        let mut graph_bytes = 0usize;
        let mut lua_bytes = 0usize;
        let mut pagecache_bytes = 0usize;
        for mem in shard_databases.store_memory_per_shard.iter() {
            vector_bytes += mem.vector.load(Ordering::Relaxed);
            text_bytes += mem.text.load(Ordering::Relaxed);
            graph_bytes += mem.graph.load(Ordering::Relaxed);
            // moon#506: script sources + interpreter heap. Both are real
            // allocations inside RSS, so both must be subtracted here or the
            // VM's ~25KB/shard is misattributed to allocator overhead.
            lua_bytes += mem.lua.load(Ordering::Relaxed) + mem.lua_vm.load(Ordering::Relaxed);
            pagecache_bytes += mem.pagecache.load(Ordering::Relaxed);
        }
        let repl_backlog_bytes = crate::admin::metrics_setup::get_global_repl_state_arc()
            .map(|state| state.read().backlog_resident_bytes())
            .unwrap_or(0);
        let allocator_overhead = compute_allocator_overhead(
            rss,
            dashtable_bytes,
            vector_bytes,
            text_bytes,
            graph_bytes,
            lua_bytes,
            pagecache_bytes,
            repl_backlog_bytes,
        );
        crate::admin::metrics_setup::update_allocator_overhead_bytes(allocator_overhead);
    }

    // Allocate through the shard's one counter: the cursor starts at the
    // counter's current value and is written back with `max` below.
    let mut cursor = spill_file_id.get();
    let next_file_id = &mut cursor;
    if server_config.disk_offload_enabled()
        && should_run_pressure_cascade(
            runtime_config,
            server_config,
            shard_databases,
            shard_id,
            vector_resident_bytes,
            cascade_ledger_bytes,
        )
    {
        handle_memory_pressure(
            page_cache,
            shard_databases,
            shard_id,
            runtime_config,
            shard_manifest,
            next_file_id,
            wal_v3_writer,
            spill_thread,
            offload_shard_dir,
            repl_backlog,
            replica_txs,
            repl_state,
            aof_pool,
            wal_kv_log,
        );
    } else {
        // task #45: give the tick eviction path the same write-then-durable
        // -then-drop discipline the interactive write-path gate and the
        // cascade's sync-spill fallback (step 3, above) already have. Only
        // built when there is BOTH a live disk-offload config AND a
        // `ShardManifest` (the durability backstop -- `--appendonly yes` or
        // `--save`; "spill is inert without one" is an existing, documented
        // rule, see `tests/cold_collection_visibility.rs`'s module doc) --
        // otherwise `spill_ctx` stays `None` and `run_eviction` falls back to
        // its pre-existing fail-close plain-drop (policy-aware: `noeviction`
        // still OOMs, an evicting policy still frees RAM, just with no cold
        // copy -- matches PR #273's fail-close discipline).
        let mut spill_ctx: Option<crate::storage::eviction::SpillContext<'_>> = None;
        if server_config.disk_offload_enabled()
            && let Some(shard_dir) = offload_shard_dir
            && let Some(ref mut manifest) = *shard_manifest
        {
            spill_ctx = Some(crate::storage::eviction::SpillContext {
                shard_dir,
                manifest,
                next_file_id,
                // Placeholder — `run_eviction` restamps per database (#139).
                db_index: 0,
            });
        }
        super::timers::run_eviction(
            shard_databases,
            shard_id,
            runtime_config,
            wal_v3_writer,
            repl_backlog,
            replica_txs,
            repl_state,
            aof_pool,
            wal_kv_log,
            spill_ctx.as_mut(),
        );
    }

    // Monotonic write-back: never lower the counter (moon#997 review).
    spill_file_id.set(spill_file_id.get().max(cursor));
}

/// Drain any final spill completions and shut down the spill thread.
///
/// Shared between the tokio and monoio shutdown arms in `event_loop.rs`.
pub(crate) fn drain_and_shutdown_spill(
    spill_thread: &mut Option<crate::storage::tiered::spill_thread::SpillThread>,
    shard_manifest: &mut Option<crate::persistence::manifest::ShardManifest>,
    shard_databases: &std::sync::Arc<super::shared_databases::ShardDatabases>,
    shard_id: usize,
    marker_sink: &mut ColdMarkerSink<'_>,
) {
    if let Some(spill_t) = spill_thread.as_ref() {
        apply_spill_completions(
            spill_t,
            shard_manifest,
            shard_databases,
            shard_id,
            marker_sink,
        );
    }
    if let Some(st) = spill_thread.take() {
        // shutdown() returns any completions from the thread's final buffer
        // flush that the drain above did not see; apply them so those cold keys
        // are not lost (file on disk but never recorded in the manifest).
        let leftover = st.shutdown();
        apply_completion_vec(leftover, shard_manifest, marker_sink);
        tracing::info!("Shard {}: spill background thread shut down", shard_id);
    }
}

/// For each successful completion: update manifest (ONE add_file+commit per
/// file) and ColdIndex (one insert per entry within that file).
///
/// Under the batching model each `SpillCompletion` covers ONE DataFile that
/// may contain many KV entries.  This makes manifest entries == #files, not
/// #keys, removing the ~70-entry inline-root cap.
pub(crate) fn apply_spill_completions(
    spill_thread: &crate::storage::tiered::spill_thread::SpillThread,
    shard_manifest: &mut Option<crate::persistence::manifest::ShardManifest>,
    shard_databases: &std::sync::Arc<super::shared_databases::ShardDatabases>,
    shard_id: usize,
    marker_sink: &mut ColdMarkerSink<'_>,
) {
    let _ = shard_databases; // E2 removes
    let _ = shard_id; // E2 removes
    let completions = spill_thread.drain_completions();
    apply_completion_vec(completions, shard_manifest, marker_sink);
}

/// moon#902: where a published spill batch's `MOON.SPILLED <file_id> key…`
/// cut record goes. The AOF leg is the one recovery depends on; the WAL leg
/// mirrors `wal_append_and_fanout`'s `--wal-kv-log` copy so a WAL-authority
/// replay sees the same cut. There is deliberately NO replication leg: cold
/// file ids are shard-local, and a replica applying a master's marker
/// against its own cold index could drop a hot key that a same-numbered but
/// unrelated file happens to back.
pub(crate) struct ColdMarkerSink<'a> {
    pub aof_pool: Option<&'a std::sync::Arc<crate::persistence::aof::AofWriterPool>>,
    pub wal_writer: Option<&'a mut crate::persistence::wal_v3::segment::WalWriterV3>,
    pub shard_id: usize,
    pub wal_kv_log: bool,
}

impl ColdMarkerSink<'_> {
    /// Log the cut record for `keys`, now spilled into `file_id`. Returns
    /// `false` when it could NOT be logged — the caller must then withdraw
    /// the publish (moon#1202); `true` when it was logged or no log exists.
    ///
    /// A refused marker is never retried later: by then a client write to one
    /// of the keys may already be in the log, and a marker replayed after it
    /// cuts that key back to this file's older value (see
    /// `replay_older_copy_tests::moon1202_a_marker_logged_after_a_later_write_would_lose_it`).
    ///
    /// The AOF leg goes first and the WAL leg only once it is in, so the two
    /// logs never disagree about a withdrawn spill. `budget` bounds how long
    /// this may block the shard thread on a full AOF channel; it is shared by
    /// every marker of one completion drain.
    fn emit(
        &mut self,
        db: usize,
        file_id: u64,
        keys: &[bytes::Bytes],
        budget: &mut std::time::Duration,
    ) -> bool {
        if keys.is_empty() {
            return true;
        }
        let wal_leg = self.wal_kv_log && self.wal_writer.is_some();
        if self.aof_pool.is_none() && !wal_leg {
            return true;
        }
        let data = crate::persistence::cold_records::serialize_spilled(file_id, keys);
        if let Some(pool) = self.aof_pool
            && let Err(refusal) =
                pool.try_send_append_bounded_or_refuse(self.shard_id, 0, db, data.clone(), budget)
        {
            tracing::warn!(
                shard_id = self.shard_id,
                file_id,
                keys = keys.len(),
                ?refusal,
                "MOON.SPILLED cut record refused by a saturated AOF writer; the spill is \
                 withdrawn and its keys stay in RAM until the next eviction pass"
            );
            return false;
        }
        if wal_leg && let Some(w) = self.wal_writer.as_deref_mut() {
            // moon#1039: the marker is db-scoped (replay applies it to the
            // selected db's cold index) — carry the db in the header.
            w.append_in_db(
                crate::persistence::wal_v3::record::WalRecordType::Command,
                db,
                &data,
            );
        }
        true
    }
}

/// Put one key of a spill batch that will NOT be published back into the hot
/// table from its in-flight payload (moon#893). Same guards as the
/// failed-pwrite branch of [`apply_completion_vec`]: only the newest request
/// for the key may re-insert, and a key re-created meanwhile is left alone.
fn rehydrate_unpublished_spill(
    entry: &crate::storage::tiered::spill_thread::SpillCompletionEntry,
    file_id: u64,
) {
    crate::shard::slice::with_shard_db(entry.db_index, |db| {
        if !db.spill_inflight_is_newest(&entry.key, entry.req_file_id) {
            // Deleted, overwritten, read-promoted or re-evicted in flight:
            // the newer state owns the key.
            crate::storage::tiered::spill_thread::record_spill_completion_superseded();
            return;
        }
        let payload = db.spill_inflight_payload(&entry.key, entry.req_file_id);
        db.spill_inflight_clear(&entry.key, entry.req_file_id);
        if db.get_version(&entry.key) != 0 {
            return;
        }
        match payload.and_then(|(vt, bytes, ttl)| {
            crate::storage::eviction::rehydrate_spill_payload(vt, &bytes, ttl)
        }) {
            Some(hot) => {
                db.set(&entry.key, hot);
                crate::storage::tiered::spill_thread::record_spill_failed_reinserted();
            }
            None => tracing::error!(
                file_id,
                key_len = entry.key.len(),
                "Spill completion refused AND its payload does not rehydrate — key lost \
                 until AOF-replay restart"
            ),
        }
    });
}

/// Apply a batch of spill completions: ONE manifest `add_file`+commit per file,
/// one `cold_index` insert per KV entry within it. Shared by the live drain
/// (`apply_spill_completions`) and the shutdown final-flush drain.
fn apply_completion_vec(
    completions: Vec<crate::storage::tiered::spill_thread::SpillCompletion>,
    shard_manifest: &mut Option<crate::persistence::manifest::ShardManifest>,
    marker_sink: &mut ColdMarkerSink<'_>,
) {
    if completions.is_empty() {
        return;
    }

    // Task #59: this runs on the shard event-loop thread, so it must not pay
    // for manifest fsyncs — one DEFERRED commit for the whole batch (below),
    // shipped to the manifest-sync thread. Correct because this path only
    // runs under `--appendonly yes` (evict_one_async_spill bails otherwise):
    // AOF replay + the orphan sweep reconstruct anything a lost manifest
    // commit would have recorded. Previously: one durable commit (up to 2
    // fsyncs) per flushed file, measured blocking the loop 1.0-2.1s per 8s
    // window under spill flood, single calls up to 1.0s.
    let mut manifest_dirty = false;
    // moon#1202: ONE backpressure budget for every `MOON.SPILLED` marker of
    // this drain, so N completions against a saturated writer stall the
    // shard thread for at most one bound, not N. Once it is spent, a marker
    // the channel cannot take right away withdraws its spill instead.
    let mut marker_budget = crate::persistence::aof::AOF_REASON_DEL_BACKPRESSURE_BOUND;
    for c in completions {
        if !c.success {
            // Deep-review F1: the hot entry was removed at evict time on the
            // promise this completion would land the key in the cold index.
            // A failed pwrite breaks that promise — without re-insert the key
            // exists in NEITHER plane and reads nil until an AOF-replay
            // restart (and permanently under --appendonly no sync paths).
            // Fail-closed: put the value back in RAM, mirroring the
            // enqueue-failure path which retains the hot value. Under a
            // persistently failing disk this re-arms eviction for the same
            // key — bounded per tick and loudly counted, which beats silent
            // wrong answers.
            if let Some(req) = c.failed_request {
                crate::shard::slice::with_shard_db(req.db_index, |db| {
                    // Deep-review P2 stale-shadow guard: if the key was
                    // re-created and re-evicted while this pwrite was in
                    // flight, a NEWER spill request supersedes this one —
                    // re-inserting this older payload would shadow the newer
                    // cold value in the hot plane (and the next eviction
                    // would spill the stale copy as authoritative).
                    let newest = db.spill_inflight_is_newest(&req.key, req.file_id);
                    db.spill_inflight_clear(&req.key, req.file_id);
                    if !newest {
                        tracing::warn!(
                            file_id = c.file_entry.file_id,
                            key_len = req.key.len(),
                            "Spill pwrite failed for a request that no longer owns this \
                             key; skipping re-insert (a newer spill is in flight or \
                             landed, or the key was deleted / overwritten / read-promoted \
                             while this write was in flight — #459)"
                        );
                        return;
                    }
                    if db.get_version(&req.key) != 0 {
                        // A newer write recreated the key while the spill was
                        // in flight; the failed payload is stale — drop it.
                        return;
                    }
                    match crate::storage::eviction::rehydrate_spill_payload(
                        req.value_type,
                        &req.value_bytes,
                        req.ttl_ms,
                    ) {
                        Some(entry) => {
                            db.set(&req.key, entry);
                            crate::storage::tiered::spill_thread::record_spill_failed_reinserted();
                            tracing::error!(
                                file_id = c.file_entry.file_id,
                                key_len = req.key.len(),
                                "Spill pwrite failed; evicted key re-inserted into hot table \
                                 (spill volume is failing writes)"
                            );
                        }
                        None => {
                            tracing::error!(
                                file_id = c.file_entry.file_id,
                                key_len = req.key.len(),
                                "Spill pwrite failed AND payload does not rehydrate — key lost \
                                 until AOF-replay restart"
                            );
                        }
                    }
                });
            } else {
                tracing::warn!(
                    file_id = c.file_entry.file_id,
                    "Spill pwrite failed on background thread (no payload carried back)"
                );
            }
            continue;
        }

        let file_id = c.file_entry.file_id;

        // RAM-only manifest update; durability handled once per batch below.
        if shard_manifest
            .as_ref()
            .is_some_and(|m| m.has_entry(file_id, c.file_entry.file_type))
        {
            // moon#893: the manifest already lists this id (Active, or
            // Tombstoned inside its retention window), so it does not
            // describe the file this batch wrote and nothing may point at
            // it. Unreachable while the file_id seed holds. The hot
            // entries were removed at enqueue: each key's only copy is its
            // in-flight payload, so put it back in RAM exactly as the
            // failed-pwrite branch above does, or it stays readable only
            // from `spill_inflight` until a restart.
            crate::storage::tiered::spill_thread::record_spill_completion_id_rejected();
            tracing::error!(
                file_id,
                keys = c.entries.len(),
                "Spill completion refused: the manifest already lists this file id \
                 (it was re-issued); re-inserting the batch's keys into the hot table"
            );
            for entry in &c.entries {
                rehydrate_unpublished_spill(entry, file_id);
            }
            continue;
        }

        // The keys this completion may publish, grouped per db (a file is
        // single-db by construction, so this is one group in practice).
        //
        // The in-flight record is the completion's AUTHORIZATION to publish,
        // not just a stale-shadow guard (#459). It is gone when the key was
        // deleted, overwritten, or read-promoted while the spill was in
        // flight; publishing anyway resurrects a deleted key — and because
        // the file reaches the manifest, the resurrection survives restart.
        // Measured pre-fix: 277 of 400 DELs undone this way. A superseded
        // key's record belongs to the newer request and is left for it.
        let mut groups: Vec<(
            usize,
            Vec<crate::storage::tiered::spill_thread::SpillCompletionEntry>,
        )> = Vec::new();
        // moon#1215: keys this file holds a slot for but that will NOT be
        // indexed from it (superseded, or withdrawn with the marker). If the
        // file is published for its other keys, those slots are on disk in a
        // listed file and a rebuild would index them: the cold index's
        // dead-slot ledger must know, so an AOF rewrite can keep them dead.
        let mut ghosts: Vec<(usize, bytes::Bytes, Option<u64>)> = Vec::new();
        for entry in c.entries {
            let publishable = crate::shard::slice::with_shard_db(entry.db_index, |db| {
                if !db.spill_inflight_is_newest(&entry.key, entry.req_file_id) {
                    crate::storage::tiered::spill_thread::record_spill_completion_superseded();
                    return false;
                }
                if db.cold_index.is_none() {
                    // No cold plane to publish into: retire the record.
                    db.spill_inflight_clear(&entry.key, entry.req_file_id);
                    return false;
                }
                true
            });
            if publishable {
                match groups.iter_mut().find(|(d, _)| *d == entry.db_index) {
                    Some((_, entries)) => entries.push(entry),
                    None => groups.push((entry.db_index, vec![entry])),
                }
            } else {
                ghosts.push((entry.db_index, entry.key, entry.ttl_ms));
            }
        }

        // moon#902 + moon#1202: each group's `MOON.SPILLED` cut record is
        // logged BEFORE its keys are published, in this same synchronous
        // section, so no client write to them can be logged in between. If the
        // AOF writer cannot take it, the group is not published at all: its
        // keys go back to RAM from their in-flight payloads. Publishing and
        // then dropping the marker (the pre-#1202 behaviour) left the log with
        // no cut for keys the cold plane now owns — value-correct on replay
        // (the log rebuilds them hot) but an acked-append-shaped hole in the
        // AOF status, and no way to log the cut later without reordering it
        // after a newer write.
        let mut published_any = false;
        let mut withdrawn_any = false;
        for (db_index, entries) in groups {
            let keys: Vec<bytes::Bytes> = entries.iter().map(|e| e.key.clone()).collect();
            if !marker_sink.emit(db_index, file_id, &keys, &mut marker_budget) {
                withdrawn_any = true;
                for entry in &entries {
                    rehydrate_unpublished_spill(entry, file_id);
                }
                ghosts.extend(entries.iter().map(|e| (db_index, e.key.clone(), e.ttl_ms)));
                continue;
            }
            published_any = true;
            crate::shard::slice::with_shard_db(db_index, |db| {
                for entry in entries {
                    // `ttl_ms` rides along from the `SpillCompletionEntry` so
                    // the proactive TTL sweep (R1, H-2) can judge expiry from
                    // the in-RAM index alone.
                    let location = crate::storage::tiered::cold_index::ColdLocation {
                        file_id,
                        page_idx: entry.page_idx,
                        slot_idx: entry.slot_idx,
                        ttl_ms: entry.ttl_ms,
                        value_type: entry.value_type,
                    };
                    if let Some(ref mut ci) = db.cold_index {
                        ci.insert(entry.key.clone(), location);
                    }
                    // Retire this request's record; a newer request's is left
                    // for its own completion.
                    db.spill_inflight_clear(&entry.key, entry.req_file_id);
                }
            });
        }
        if withdrawn_any {
            crate::storage::tiered::spill_thread::record_spill_completion_marker_withdrawn();
            if !published_any {
                // Nothing points at the file: leave it out of the manifest so
                // no restart can index it, and the startup orphan sweep
                // (unmanifested `heap-*.mpf`) reclaims it.
                continue;
            }
        }
        if let Some(ref mut manifest) = *shard_manifest {
            // Cannot refuse: `has_entry` was checked above and nothing in
            // between touches the manifest.
            if let Err(e) = manifest.add_file(c.file_entry) {
                tracing::error!(file_id, error = %e, "Spill completion: manifest add_file refused");
            } else {
                manifest_dirty = true;
                for (db_index, key, ttl_ms) in ghosts {
                    crate::shard::slice::with_shard_db(db_index, |db| {
                        if let Some(ci) = db.cold_index.as_mut() {
                            ci.note_dead_slot(file_id, key, ttl_ms);
                        }
                    });
                }
            }
        }
    }

    // ONE deferred commit for the whole drained batch (see the task #59 note
    // at the top of this function). Non-blocking when the manifest-sync
    // thread is attached; degrades to a synchronous persist otherwise.
    if manifest_dirty {
        if let Some(ref mut manifest) = *shard_manifest {
            if let Err(e) = manifest.commit_deferred() {
                tracing::warn!(
                    error = %e,
                    "Deferred manifest commit failed for spill completion batch"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Memory pressure cascade (design section 8.5)
// ---------------------------------------------------------------------------

/// Aggressive idle floor (seconds) used by the memory-pressure cascade to
/// offload idle vector segments to COLD early (C). Far below the normal
/// `--engine-offload-idle-secs` (default 3600s): once a shard is over its
/// memory budget, a segment untouched for a minute is worth shedding to reclaim
/// RAM rather than waiting out the full idle timeout. Actively-queried segments
/// (idle < this) stay resident; anything shed reloads on next touch.
const PRESSURE_OFFLOAD_IDLE_SECS: u64 = 60;

/// Check if memory usage exceeds the disk offload threshold.
///
/// Returns `true` when the pressure cascade should run. Uses actual
/// aggregate database memory estimate vs maxmemory * threshold.
/// `ledger_bytes`: this shard's cold dead-slot ledger (moon#1215), which the
/// published figure includes but no cascade step can free — page-cache
/// eviction, vector demotion and KV eviction all leave it untouched. Counting
/// it here fired the cascade on every tick once deletes left a large ledger
/// behind, pushing live data to disk for nothing (PR #1233 review). Write
/// admission charges it instead (`eviction::evict_to_budget`).
pub(crate) fn should_run_pressure_cascade(
    runtime_config: &std::sync::Arc<parking_lot::RwLock<crate::config::RuntimeConfig>>,
    server_config: &std::sync::Arc<crate::config::ServerConfig>,
    shard_databases: &std::sync::Arc<super::shared_databases::ShardDatabases>,
    shard_id: usize,
    vector_resident_bytes: usize,
    ledger_bytes: usize,
) -> bool {
    let rt = runtime_config.read();
    if rt.maxmemory == 0 {
        return false; // No memory limit set -- no pressure possible
    }
    // `used` is this shard's aggregate (across its DBs); compare against the
    // PER-SHARD budget so the cascade fires at maxmemory/num_shards per shard,
    // bounding aggregate RSS instead of the whole-instance cap per shard.
    // GAP-1: an elastic budget (idle siblings' donated headroom) widens the
    // threshold for hot shards; 0 means none published yet (static fallback).
    let budget = match shard_databases.elastic_budget(shard_id) {
        0 => rt.maxmemory_per_shard(),
        elastic => elastic.min(rt.maxmemory),
    };
    let threshold = (budget as f64 * server_config.disk_offload_threshold) as usize;
    // C5 / Phase 3: read the already-published per-shard KV memory (written
    // earlier this same tick by `run_eviction_tick`). Lock-free Relaxed load.
    //
    // Memory-triggered vector offload (C): add the shard's vector resident
    // bytes (HOT immutable + WARM, computed this same tick) so a vector-heavy
    // workload — the primary disk-offload use case, where KV is light but
    // vector segments are the RAM hog — actually triggers the cascade. Without
    // this, vector memory was invisible to every pressure mechanism and idle
    // segments only ever offloaded on the wall-clock idle timer.
    let used = shard_databases
        .published_shard_memory(shard_id)
        .saturating_sub(ledger_bytes)
        .saturating_add(vector_resident_bytes);
    used > threshold
}

/// Memory pressure cascade per MoonStore v2 design section 8.5.
///
/// Ordered response:
/// 1. **PageCache clock-sweep eviction** -- evict cold (unpinned, non-dirty) frames
/// 2. **Force-demote oldest HOT ImmutableSegments to WARM** (halved threshold)
/// 3. **KV eviction** -- existing LRU/LFU via `timers::run_eviction`
/// 4. **NoEviction policy** -- log OOM warning if cascade is exhausted
///
/// Called from eviction timer tick when `disk_offload_enabled` is true and
/// `should_run_pressure_cascade()` returns true.
#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_memory_pressure(
    page_cache: &Option<PageCache>,
    shard_databases: &std::sync::Arc<super::shared_databases::ShardDatabases>,
    shard_id: usize,
    runtime_config: &std::sync::Arc<parking_lot::RwLock<crate::config::RuntimeConfig>>,
    shard_manifest: &mut Option<ShardManifest>,
    next_file_id: &mut u64,
    wal_v3: &mut Option<crate::persistence::wal_v3::segment::WalWriterV3>,
    spill_thread: Option<&crate::storage::tiered::spill_thread::SpillThread>,
    // task/issue #45: see `run_eviction_tick`.
    offload_shard_dir: Option<&std::path::Path>,
    // task #34 (Wave A): see `run_eviction_tick`.
    repl_backlog: &crate::replication::backlog::SharedBacklog,
    replica_txs: &mut Vec<crate::shard::dispatch::ReplicaFanout>,
    repl_state: &Option<crate::replication::state::OffsetHandle>,
    aof_pool: Option<&std::sync::Arc<crate::persistence::aof::AofWriterPool>>,
    wal_kv_log: bool,
) {
    // Step 1: PageCache eviction -- evict up to 16 cold frames per tick.
    // This is the cheapest operation: no disk I/O, just invalidates cached pages.
    if let Some(ref pc) = *page_cache {
        let evicted = pc.evict_cold_frames(16);
        if evicted > 0 {
            tracing::debug!(
                "Shard {}: memory pressure step 1 -- evicted {} cold PageCache frame(s)",
                shard_id,
                evicted
            );
            return; // Pressure partially relieved; next tick will re-evaluate
        }
    }

    // Step 2: Force-offload idle vector segments straight to COLD (memory-
    // triggered early offload, C). Previously this demoted HOT->WARM, which
    // frees NO resident bytes (a WarmSearchSegment is a same-size heap copy of
    // the HOT segment). Under genuine memory pressure we instead shed the
    // segments that have been idle beyond an aggressive floor
    // (`PRESSURE_OFFLOAD_IDLE_SECS`, far below the normal
    // `--engine-offload-idle-secs`) all the way to COLD (`UnloadedSegment`
    // stub), which actually returns RAM — and stays reloadable-on-touch. We
    // pass `warm_after = u64::MAX` so the age-based HOT->WARM path stays
    // disabled here; only the idle->COLD path fires.
    if let Some(ref mut manifest) = *shard_manifest
        && let Some(shard_dir) = offload_shard_dir
    {
        let count = crate::shard::slice::with_shard(|s| {
            s.vector_store.try_warm_transitions_all_idle(
                shard_dir,
                manifest,
                u64::MAX,
                PRESSURE_OFFLOAD_IDLE_SECS,
                next_file_id,
                wal_v3,
            )
        });
        if count > 0 {
            tracing::info!(
                "Shard {}: memory pressure step 2 -- offloaded {} idle vector segment(s) to COLD",
                shard_id,
                count
            );
            return; // Freed RAM via cold offload; re-evaluate next tick
        }
    }

    // Step 3: KV eviction -- run existing LRU/LFU eviction, with spill-to-disk
    // when disk-offload is enabled (evicted entries written to KvLeaf DataFiles).
    // Compare this shard's aggregate (across its DBs) against the PER-SHARD
    // budget (maxmemory/num_shards) so the summed eviction across shards bounds
    // aggregate RSS at the whole-instance maxmemory.
    //
    // A3 review (MEDIUM): this used-term stays KV-only DELIBERATELY, unlike
    // `timers::run_eviction` (the disk-offload-off path), which adds the
    // shard's vector bytes. Inside the cascade the vector term has already
    // pulled its weight: it fired the trigger (`should_run_pressure_cascade`
    // is vector-inclusive) and step 2 sheds vector memory directly via
    // offload-to-COLD. Adding it here too would evict KV to pay for memory
    // that step 2 is already reclaiming more cheaply; the trigger refires
    // every 100ms tick, so the cascade converges with vectors shedding
    // first and KV eviction as the residual step.
    //
    // When a SpillThread is available, use the async path: entries are removed
    // from DashTable immediately (freeing RAM) and pwrite is deferred to the
    // background thread. Otherwise, fall back to synchronous spill.
    {
        let rt = runtime_config.read();
        if rt.maxmemory > 0 {
            // C5 / Phase 3: read the already-published per-shard KV memory
            // (written earlier this same tick). Lock-free Relaxed load.
            let total_mem = shard_databases.published_shard_memory(shard_id);
            // GAP-1: hot shards evict against their elastic budget (idle
            // siblings' donated headroom), not the static maxmemory/N.
            let budget = match shard_databases.elastic_budget(shard_id) {
                0 => rt.maxmemory_per_shard(),
                elastic => elastic.min(rt.maxmemory),
            };
            // moon#1221 review F1: bytes an UNLINK or expiry queued for lazy
            // free in ANY db are memory already released; reclaim them
            // across every db before the per-db loops below take a victim.
            let total_mem = if total_mem > budget {
                crate::storage::eviction::reclaim_lazy_free_in_shard(
                    shard_databases.db_count(),
                    total_mem,
                    &rt,
                    budget,
                )
            } else {
                total_mem
            };
            if total_mem > budget {
                let db_count = shard_databases.db_count();
                // #454 P2.8: ONE shared backpressure bound for this entire sweep
                // (per-key minting could stall the shard bound x victim-count).
                let mut reason_del_budget =
                    crate::persistence::aof::AOF_REASON_DEL_BACKPRESSURE_BOUND;
                // task/issue #45: `offload_shard_dir` is precomputed at shard
                // init. `spill_thread` and `shard_manifest` share its
                // disk-offload gate, so both branches below pair their
                // existing Some-guard with the dir's.

                if let (Some(spill_t), Some(shard_dir)) = (spill_thread, offload_shard_dir) {
                    // Async spill path: background thread does pwrite under
                    // `--appendonly yes` (AOF-backstopped fast path). Under
                    // `--appendonly no` there is no AOF backstop, so
                    // `evict_one_async_spill` needs `shard_manifest` to take
                    // the durable synchronous fallback instead of risking the
                    // crash window (see its doc comment in eviction.rs) --
                    // this is the ONE call site that has a manifest to give
                    // it (the inline per-connection write-path gate does
                    // not, and stays on the pre-fix fast-path-or-bail
                    // behavior).
                    let sender = spill_t.sender();
                    for i in 0..db_count {
                        crate::shard::slice::with_shard_db(i, |db| {
                            let _ = crate::storage::eviction::evict_to_budget(
                                db,
                                &rt,
                                crate::storage::eviction::EvictionRun::async_spill(
                                    &sender,
                                    shard_dir,
                                    next_file_id,
                                    i,
                                    shard_manifest.as_mut(),
                                )
                                .total(total_mem)
                                .budget(budget)
                                .report(
                                    // task #34 (Wave A): only the no-manifest,
                                    // `--appendonly no` plain-drop fallback
                                    // inside this function ever calls this sink
                                    // (the async-spill and durable-batch
                                    // branches leave a cold/AOF-recoverable
                                    // copy and never invoke it).
                                    &mut |key| {
                                        crate::replication::reason_del::record_reason_del(
                                            key,
                                            i,
                                            wal_v3,
                                            repl_backlog,
                                            replica_txs,
                                            repl_state,
                                            shard_id,
                                            aof_pool,
                                            wal_kv_log,
                                            &mut reason_del_budget,
                                        );
                                    },
                                ),
                            );
                        });
                    }
                    // Drop sender clone immediately to avoid shutdown deadlock
                    drop(sender);
                } else {
                    // Sync spill fallback
                    for i in 0..db_count {
                        crate::shard::slice::with_shard_db(i, |db| {
                            if let Some(ref mut manifest) = *shard_manifest
                                && let Some(shard_dir) = offload_shard_dir
                            {
                                let mut ctx = crate::storage::eviction::SpillContext {
                                    shard_dir,
                                    manifest,
                                    next_file_id,
                                    // #139: this fallback runs inside the
                                    // per-db loop — attribute to db `i`.
                                    db_index: i,
                                };
                                // Durable spill (manifest reachable): a
                                // STRING victim stays cold-readable, never a
                                // plain drop. But `evict_one_with_spill`'s
                                // spill body is string-only (task #34 review,
                                // defect 1) — a Hash/List/Set/ZSet victim
                                // picked here is a genuine plain-drop with no
                                // cold copy anywhere, and must still reach
                                // `record_reason_del`. Thread the real
                                // reporting sink (previously a hardcoded
                                // no-op here, which silently swallowed those
                                // emissions).
                                let _ = crate::storage::eviction::evict_to_budget(
                                    db,
                                    &rt,
                                    crate::storage::eviction::EvictionRun::sync_spill(Some(
                                        &mut ctx,
                                    ))
                                    .total(total_mem)
                                    .budget(budget)
                                    .report(&mut |key| {
                                        crate::replication::reason_del::record_reason_del(
                                            key,
                                            i,
                                            wal_v3,
                                            repl_backlog,
                                            replica_txs,
                                            repl_state,
                                            shard_id,
                                            aof_pool,
                                            wal_kv_log,
                                            &mut reason_del_budget,
                                        );
                                    }),
                                );
                            } else {
                                // No manifest reachable: this IS the plain
                                // -drop path (task #34, Wave A) — emit.
                                let _ = crate::storage::eviction::evict_to_budget(
                                    db,
                                    &rt,
                                    crate::storage::eviction::EvictionRun::plain()
                                        .total(total_mem)
                                        .budget(budget)
                                        .report(&mut |key| {
                                            crate::replication::reason_del::record_reason_del(
                                                key,
                                                i,
                                                wal_v3,
                                                repl_backlog,
                                                replica_txs,
                                                repl_state,
                                                shard_id,
                                                aof_pool,
                                                wal_kv_log,
                                                &mut reason_del_budget,
                                            );
                                        }),
                                );
                            }
                        });
                    }
                }
            }
        }
    }

    // Step 4: NoEviction policy check -- if we reached here with noeviction,
    // log a warning. The actual OOM rejection is handled inside evict_to_budget.
    {
        let rt = runtime_config.read();
        if rt.maxmemory_policy == "noeviction" {
            tracing::warn!(
                "Shard {}: memory pressure cascade exhausted; \
                 noeviction policy active, new writes may be rejected",
                shard_id
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Checkpoint protocol handlers (disk-offload path)
// ---------------------------------------------------------------------------

use super::checkpoint_heap_files::{
    HeapFilesDurability, heap_file_path, make_written_heap_files_durable,
    stop_waiting_on_vanished_heap_file,
};
use crate::persistence::checkpoint::{CheckpointAction, CheckpointManager};
use crate::persistence::control::ShardControlFile;
use crate::persistence::manifest::ShardManifest;
use crate::persistence::page_cache::PageCache;
use crate::persistence::wal_v3::record::WalRecordType;
use crate::persistence::wal_v3::segment::WalWriterV3;
use std::path::Path;

/// Build the per-checkpoint graph snapshot hook (2026-07 graph durability
/// P0, Bug B). Resolves the shard's `GraphStore` via the `with_shard`
/// thread-local at call time — the checkpoint always runs on the shard
/// thread. Returns `true` (checkpoint may proceed) for non-graph builds and
/// when there is nothing to snapshot.
pub(crate) fn graph_checkpoint_hook(
    persistence_dir: Option<&str>,
    shard_id: usize,
) -> impl FnMut(u64) -> bool + '_ {
    move |snapshot_lsn: u64| {
        #[cfg(feature = "graph")]
        {
            crate::shard::slice::with_shard(|s| {
                crate::graph::recovery::persist_graph_at_checkpoint(
                    &mut s.graph_store,
                    persistence_dir.map(std::path::Path::new),
                    shard_id,
                    snapshot_lsn,
                )
            })
        }
        #[cfg(not(feature = "graph"))]
        {
            let _ = (snapshot_lsn, persistence_dir, shard_id);
            true
        }
    }
}

/// Who forces a checkpoint, which decides whether the shard thread may
/// block on the off-loop heap data-file fsync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ForcedCheckpoint {
    /// Graceful shutdown: the shard serves no clients any more, so it waits
    /// for the outstanding fsync, bounded by `WAIT_DURABLE_TIMEOUT` counted
    /// from the helper's start.
    Shutdown,
    /// The P6 WAL-ceiling trigger, on the shard thread WHILE IT SERVES
    /// CLIENTS: never waits on the fsync. When Finalize reports it pending,
    /// the checkpoint is left to the periodic tick, which polls it.
    WalCeiling,
}

/// Force a checkpoint now, driving the state machine synchronously.
///
/// Callers: graceful shutdown (both runtimes' event loops,
/// [`ForcedCheckpoint::Shutdown`]) and the P6 WAL-ceiling trigger
/// ([`maybe_force_checkpoint_on_wal_overflow`],
/// [`ForcedCheckpoint::WalCeiling`]). BGSAVE does not come here: it only
/// `force_begin`s, and the periodic tick drives that checkpoint.
///
/// Calls `force_begin` to bypass trigger conditions, then drives the
/// checkpoint state machine in a tight loop until it completes, or — for
/// `WalCeiling` — until the heap data-file fsync is pending. No-op if a
/// checkpoint is already active (so a later trigger never starts a second
/// fsync helper).
#[allow(clippy::too_many_arguments)]
pub(crate) fn force_checkpoint(
    mode: ForcedCheckpoint,
    checkpoint_mgr: &mut CheckpointManager,
    page_cache: &PageCache,
    wal: &mut WalWriterV3,
    manifest: &mut ShardManifest,
    control: &mut ShardControlFile,
    control_path: &Path,
    shard_id: usize,
    tombstone_retain_epochs: u64,
    tombstone_retain_secs: u64,
    graph_save: &mut dyn FnMut(u64) -> bool,
) {
    if checkpoint_mgr.is_active() {
        tracing::warn!(
            "Shard {}: checkpoint already active, skipping force",
            shard_id
        );
        return;
    }
    let lsn = wal.current_lsn();
    let dirty = page_cache.dirty_page_count();
    if !checkpoint_mgr.force_begin(lsn, dirty) {
        return;
    }
    page_cache.arm_all_fpi_pending();
    // Drive checkpoint to completion synchronously (bounded tick loop: a
    // persistently failing Finalize — manifest commit or graph snapshot —
    // must not spin forever; the periodic tick path retries later).
    let mut ticks = 0u32;
    loop {
        if handle_checkpoint_tick(
            checkpoint_mgr,
            page_cache,
            wal,
            manifest,
            control,
            control_path,
            tombstone_retain_epochs,
            tombstone_retain_secs,
            graph_save,
        ) {
            break; // Finalize completed
        }
        // If Nothing returned and not active, we're done (empty checkpoint)
        if !checkpoint_mgr.is_active() {
            break;
        }
        ticks += 1;
        if ticks > 100_000 {
            tracing::error!(
                "Shard {}: forced checkpoint did not finalize after {} ticks; \
                 giving up (will retry on the periodic tick path)",
                shard_id,
                ticks
            );
            return;
        }
        // The heap data-file fsync runs off the shard thread and the periodic
        // tick only polls it; Finalize just reported it pending.
        if checkpoint_mgr.data_sync_busy() {
            match mode {
                // Serving clients: never block on the fsync. The periodic
                // tick polls it and finishes this checkpoint; until then the
                // published redo point does not move.
                ForcedCheckpoint::WalCeiling => {
                    info!(
                        "Shard {}: WAL-ceiling checkpoint: heap data-file fsync pending; \
                         the periodic tick finishes the checkpoint",
                        shard_id
                    );
                    return;
                }
                // No clients any more: wait for the one outstanding batch,
                // bounded by WAIT_DURABLE_TIMEOUT counted from the batch's
                // START, so a hung disk delays shutdown by that budget once.
                ForcedCheckpoint::Shutdown => {
                    if !checkpoint_mgr
                        .wait_data_sync(crate::persistence::wal_v3::segment::WAIT_DURABLE_TIMEOUT)
                    {
                        tracing::error!(
                            "Shard {}: shutdown checkpoint: heap data-file fsync still \
                             outstanding after {:?}; giving up (the redo point stays where \
                             it is and recovery replays the WAL from there)",
                            shard_id,
                            crate::persistence::wal_v3::segment::WAIT_DURABLE_TIMEOUT
                        );
                        return;
                    }
                }
            }
        }
    }
    info!("Shard {}: forced checkpoint complete", shard_id);
}

/// Check the trigger and begin a checkpoint if conditions are met.
///
/// Called every tick from the event loop when disk-offload is enabled.
/// No-op if a checkpoint is already in progress.
pub(crate) fn maybe_begin_checkpoint(
    checkpoint_mgr: &mut CheckpointManager,
    wal: &WalWriterV3,
    page_cache: &PageCache,
    wal_bytes_since_checkpoint: u64,
) {
    if checkpoint_mgr.is_active() {
        return;
    }
    if checkpoint_mgr
        .trigger()
        .should_checkpoint(wal_bytes_since_checkpoint)
    {
        let lsn = wal.current_lsn();
        let dirty = page_cache.dirty_page_count();
        checkpoint_mgr.begin(lsn, dirty);
        page_cache.arm_all_fpi_pending();
    }
}

/// P6: Ceiling-trigger — force a checkpoint + aggressive WAL recycle when
/// total on-disk WAL exceeds `max_wal_bytes` AND `max_checkpoint_lag_ms` has
/// elapsed since the last completed checkpoint.
///
/// The two-condition guard prevents thrashing: if a checkpoint finished 5ms
/// ago but WAL is still over max (e.g. very fast writers), we wait for
/// `max_checkpoint_lag_ms` before forcing another round. This also handles
/// the disk-full scenario: if `force_checkpoint` fails silently (manifest
/// commit error), the lag guard ensures we retry on the next tick interval
/// rather than spinning.
///
/// # Arguments
///
/// * `last_checkpoint_at` — `Instant` of the last completed checkpoint. The
///   caller is responsible for updating this when a checkpoint finalises.
/// * `max_checkpoint_lag_ms` — from `--wal-max-checkpoint-lag-ms` config.
///
/// Returns `true` if aggressive recycle was attempted (caller should reset
/// `last_checkpoint_at` and `wal_bytes_since_checkpoint`).
#[allow(clippy::too_many_arguments)]
pub(crate) fn maybe_force_checkpoint_on_wal_overflow(
    checkpoint_mgr: &mut CheckpointManager,
    wal: &mut WalWriterV3,
    page_cache: &PageCache,
    manifest: &mut ShardManifest,
    control: &mut ShardControlFile,
    control_path: &Path,
    shard_id: usize,
    last_checkpoint_at: std::time::Instant,
    max_checkpoint_lag_ms: u64,
    graph_save: &mut dyn FnMut(u64) -> bool,
) -> bool {
    // #366: no overflow scan while the data directory is missing — the
    // per-second directory read would warn-loop forever.
    if crate::shard::disk_monitor::is_dir_lost() {
        return false;
    }
    // Condition 1: total on-disk WAL exceeds the configured ceiling.
    let total_wal = match wal.stats() {
        Ok(s) => {
            // Wire P10 INFO metrics (P6 → RECL_WAL_*).
            use std::sync::atomic::Ordering::Relaxed;
            crate::command::info_reclamation::RECL_WAL_BYTES.store(s.total_bytes, Relaxed);
            crate::command::info_reclamation::RECL_WAL_SEGMENTS.store(s.total_segments, Relaxed);
            s.total_bytes
        }
        Err(e) => {
            tracing::warn!(
                "Shard {}: P6 WAL stats scan failed, skipping overflow check: {}",
                shard_id,
                e
            );
            return false;
        }
    };
    if total_wal <= wal.max_wal_bytes() {
        return false;
    }

    // Condition 2: enough time has elapsed since the last checkpoint to
    // avoid thrashing when the checkpoint just ran.
    //
    // #870 (D3): the lag alone is a thrash guard, not a schedule. When the
    // previous pass freed nothing — the whole eligible prefix holds
    // sole-copy plane history, or the floor did not move — running again
    // at base cadence is a fixed-rate no-op whose cost grows with the WAL.
    // Each such pass doubles the effective lag, capped at
    // `2^OVERFLOW_BACKOFF_MAX_SHIFT` (64× — 10 min 40 s at defaults); any
    // recycler that frees a segment resets it, and the regular checkpoint
    // path recycles on its own cadence regardless of this guard.
    let elapsed_ms = last_checkpoint_at.elapsed().as_millis() as u64;
    let effective_lag_ms =
        max_checkpoint_lag_ms.saturating_mul(wal.overflow_recycle_backoff_multiplier());
    if elapsed_ms < effective_lag_ms {
        tracing::debug!(
            "Shard {}: P6 WAL overflow ({} bytes) but lag guard active ({}/{}ms, {}x backoff), deferring",
            shard_id,
            total_wal,
            elapsed_ms,
            effective_lag_ms,
            wal.overflow_recycle_backoff_multiplier()
        );
        return false;
    }

    tracing::warn!(
        "Shard {}: P6 WAL ceiling trigger — {} bytes > max {} bytes, forcing checkpoint + aggressive recycle",
        shard_id,
        total_wal,
        wal.max_wal_bytes()
    );

    // Force a checkpoint (drives the state machine until it completes or its
    // heap data-file fsync is pending — this runs while the shard serves
    // clients, so it never waits on that fsync). If a checkpoint is already
    // active, force_checkpoint is a no-op — the in-progress checkpoint will
    // advance next tick and the recycle will run in handle_checkpoint_tick's
    // Finalize arm.
    force_checkpoint(
        ForcedCheckpoint::WalCeiling,
        checkpoint_mgr,
        page_cache,
        wal,
        manifest,
        control,
        control_path,
        shard_id,
        0, // tombstone_retain_epochs: no retention on emergency checkpoint
        0, // tombstone_retain_secs: no retention on emergency checkpoint
        graph_save,
    );

    // Aggressive recycle — bypass min_wal_bytes floor.
    // Use control.last_checkpoint_lsn (the LSN of the last *completed*
    // checkpoint) rather than wal.current_lsn()-1. If force_checkpoint above
    // was a no-op (checkpoint already active), left its checkpoint pending on
    // the data-file fsync, or failed silently, using the current WAL head
    // would be unsafe — we would recycle segments whose dirty pages have not
    // been made durable in their data files yet. The in-memory `control` is
    // the PUBLISHED redo point: Finalize only advances it together with a
    // successful control-file write (it restores the old values if that
    // write fails).
    //
    // Kernel M3 K2 review round 2 / P1-1: same min-across-planes floor as
    // every other recycle call site (Finalize, Pass C, VACUUM) — KV alone
    // is not enough, the graph engine's own snapshot floor must also cover
    // whatever this emergency path is about to recycle.
    let redo_lsn = control.last_checkpoint_lsn.min(control.graph_floor_lsn);
    match wal.recycle_aggressive(redo_lsn) {
        Ok(stats) if stats.segments_recycled > 0 => {
            tracing::info!(
                "Shard {}: P6 aggressive recycle freed {} segment(s), {} bytes",
                shard_id,
                stats.segments_recycled,
                stats.bytes_reclaimed,
            );
        }
        Ok(_) => {
            wal.note_overflow_recycle_freed_nothing();
            tracing::debug!(
                "Shard {}: P6 aggressive recycle: no segments eligible at redo_lsn={} \
                 (next pass after {}x lag)",
                shard_id,
                redo_lsn,
                wal.overflow_recycle_backoff_multiplier()
            );
        }
        Err(e) => {
            wal.note_overflow_recycle_freed_nothing();
            tracing::warn!(
                "Shard {}: P6 aggressive recycle failed: {} — disk may be full",
                shard_id,
                e
            );
        }
    }

    true
}

/// WAL payload of a FullPageImage record:
/// `file_id(8 LE) + page_offset(8 LE) + flag(1) + page_data`, where the flag is
/// `0x00` for an uncompressed image and `0x01` for an LZ4-compressed one.
fn full_page_image_payload(file_id: u64, page_offset: u64, data: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(17 + data.len());
    payload.extend_from_slice(&file_id.to_le_bytes());
    payload.extend_from_slice(&page_offset.to_le_bytes());
    if data.len() > 256 {
        let compressed = lz4_flex::compress_prepend_size(data);
        if compressed.len() < data.len() {
            payload.push(0x01);
            payload.extend_from_slice(&compressed);
            return payload;
        }
    }
    payload.push(0x00);
    payload.extend_from_slice(data);
    payload
}

#[cfg(test)]
thread_local! {
    static FAIL_NEXT_FPI: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Test hook: the next FullPageImage append on this thread fails.
#[cfg(test)]
pub(crate) fn fail_next_full_page_image() {
    FAIL_NEXT_FPI.with(|c| c.set(true));
}

#[cfg(test)]
fn take_full_page_image_fault() -> bool {
    FAIL_NEXT_FPI.with(|c| c.replace(false))
}

/// Handle one checkpoint tick. Called from the event loop every 1ms when
/// disk-offload is enabled.
///
/// Returns `true` if a finalize step was completed this tick.
///
/// The caller provides all I/O dependencies. The only work this tick hands
/// off is the heap data-file fsync, which runs on the manager's off-loop
/// helper; the tick never blocks on it.
///
/// After a successful manifest commit at the Finalize step, tombstone GC runs
/// with the configured two-axis retention policy. GC is in-memory only here;
/// the pruned state is committed on the **next** checkpoint's manifest commit.
/// This preserves crash safety: the current commit carries tombstones, and GC
/// results only reach disk after one additional dual-root swap.
pub(crate) fn handle_checkpoint_tick(
    checkpoint_mgr: &mut CheckpointManager,
    page_cache: &PageCache,
    wal: &mut WalWriterV3,
    manifest: &mut ShardManifest,
    control: &mut ShardControlFile,
    control_path: &Path,
    tombstone_retain_epochs: u64,
    tombstone_retain_secs: u64,
    graph_save: &mut dyn FnMut(u64) -> bool,
) -> bool {
    // #366: checkpoints are pure file work (page pwrite, manifest rename,
    // WAL recycle) — all doomed while the data directory is missing. Skip;
    // the CheckpointManager simply resumes when the latch clears.
    if crate::shard::disk_monitor::is_dir_lost() {
        return false;
    }
    match checkpoint_mgr.advance_tick() {
        CheckpointAction::Nothing => false,
        CheckpointAction::FlushPages(count) => {
            // Log-before-data for torn-page protection (#452), one WAL barrier
            // per batch: `flush_dirty_pages_with_fpi` appends the FullPageImage
            // of every FPI-pending page in the batch, then calls the barrier
            // below ONCE, then pwrites the pages. The barrier makes the WAL
            // durable through the batch's highest page LSN AND its highest FPI
            // LSN, so every page's change and image are on disk before any
            // page is overwritten in place — the order a wait per page gives,
            // for one fsync instead of one per page.
            //
            // The FPI append and the barrier are separate callbacks that both
            // need `wal`; they never run nested, so a `RefCell` hands the one
            // `&mut` to whichever runs.
            let wal_cell = std::cell::RefCell::new(&mut *wal);
            let wal_busy = || std::io::Error::other("checkpoint: WAL writer already borrowed");
            // Highest FPI LSN appended in this batch (0 = none).
            let batch_fpi_lsn = std::cell::Cell::new(0u64);
            let mut written_files: smallvec::SmallVec<[u64; 16]> = smallvec::SmallVec::new();
            let mut vanished_files: smallvec::SmallVec<[u64; 4]> = smallvec::SmallVec::new();
            let mut vanished_pages = 0usize;
            let mut fpi_records = 0usize;
            let shard_dir = control_path.parent().unwrap_or(Path::new("."));

            let outcome = page_cache.flush_dirty_pages_with_fpi(
                count,
                &mut |max_page_lsn| {
                    // HARD ordering invariant (log-before-data). Bounded wait
                    // on the off-loop WAL sync agent; Err writes no page of
                    // the batch and fails this checkpoint's flush (the pages
                    // stay dirty and are re-flushed before finalize).
                    let upto = max_page_lsn.max(batch_fpi_lsn.get());
                    let mut wal = wal_cell.try_borrow_mut().map_err(|_| wal_busy())?;
                    if wal.current_lsn() > upto {
                        wal.wait_durable(
                            upto,
                            crate::persistence::wal_v3::segment::WAIT_DURABLE_TIMEOUT,
                        )
                    } else {
                        Ok(())
                    }
                },
                &mut |file_id, page_offset, _is_large, data| {
                    #[cfg(test)]
                    if take_full_page_image_fault() {
                        return Err(std::io::Error::other("injected FPI failure"));
                    }
                    let payload = full_page_image_payload(file_id, page_offset, data);
                    let mut wal = wal_cell.try_borrow_mut().map_err(|_| wal_busy())?;
                    let lsn = wal.append(WalRecordType::FullPageImage, &payload);
                    batch_fpi_lsn.set(batch_fpi_lsn.get().max(lsn));
                    fpi_records += 1;
                    Ok(())
                },
                &mut |file_id, page_offset, is_large, data| {
                    // pwrite(2) dirty page to its DataFile at the correct offset.
                    // KV heap pages: {shard_dir}/data/heap-{file_id:06}.mpf
                    // Warm-tier .mpf pages are immutable and never dirtied, so
                    // only KV heap pages reach this path.
                    let page_size = if is_large {
                        crate::persistence::page::PAGE_64K
                    } else {
                        crate::persistence::page::PAGE_4K
                    };
                    let byte_offset = page_offset * page_size as u64;
                    // Never `create`: a heap file that is gone stays gone.
                    let r = std::fs::OpenOptions::new()
                        .write(true)
                        .open(heap_file_path(shard_dir, file_id))
                        .and_then(|file| crate::util::file_ext::write_at(&file, data, byte_offset));
                    match &r {
                        // Written, NOT yet durable: Finalize fsyncs the file
                        // before it may publish a redo point above this page.
                        Ok(()) => {
                            if !written_files.contains(&file_id) {
                                written_files.push(file_id);
                            }
                        }
                        // The heap file no longer exists: this page can never
                        // be written anywhere. Handled below — it is not a
                        // flush failure to retry.
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            vanished_pages += 1;
                            if !vanished_files.contains(&file_id) {
                                vanished_files.push(file_id);
                            }
                        }
                        Err(_) => {}
                    }
                    r
                },
            );

            for file_id in written_files {
                checkpoint_mgr.note_data_file_written(file_id);
            }
            for file_id in vanished_files {
                stop_waiting_on_vanished_heap_file(
                    checkpoint_mgr,
                    page_cache,
                    manifest,
                    file_id,
                    "page write",
                );
            }
            // Any page the batch did not write for a reason other than its
            // file being gone — its image, the WAL barrier or its write
            // failed — is still dirty: Finalize must re-flush it.
            if outcome.failed > vanished_pages {
                checkpoint_mgr.note_page_flush_failed();
            }
            if outcome.flushed > 0 {
                tracing::trace!(
                    "Checkpoint: flushed {} dirty pages (with FPI, {} FPI records)",
                    outcome.flushed,
                    fpi_records
                );
            }
            false
        }
        CheckpointAction::Finalize { redo_lsn } => {
            // Prod-hardening #13: a failed finalize leaves the state machine in
            // `Finalizing`, so the next 1ms tick re-enters this arm and
            // re-appends a WAL Checkpoint record (step 1) with no backoff.
            // Under a sustained failure (slow/degraded disk causing repeated
            // `wait_durable` timeouts, or a persistent `graph_save` failure)
            // this floods the WAL with a Checkpoint marker every millisecond,
            // and because `last_checkpoint_lsn` never advances,
            // `recycle_segments_before` never fires — WAL usage grows fastest
            // exactly during the disk-pressure incident. Gate re-attempts on an
            // exponential backoff so a stuck finalize retries on a bounded
            // schedule instead of hammering every tick.
            // `now` gates readiness only; failure branches below re-stamp
            // Instant::now() because wait_durable/commit/graph_save can take
            // longer than the backoff — arming from this pre-work timestamp
            // would put the retry deadline in the past (instant re-retry).
            let now = std::time::Instant::now();
            if !checkpoint_mgr.finalize_ready(now) {
                return false;
            }

            // 0. Every page change below `redo_lsn` must be durable in its heap
            //    file before the redo point is published (#452). Step 4 makes
            //    `redo_lsn` the replay start and step 6 recycles the WAL below
            //    it — including the FullPageImages — so a page that is only in
            //    the kernel's page cache at that instant rolls back on power
            //    loss with nothing left to redo it.
            if checkpoint_mgr.is_data_sync_poisoned() {
                tracing::error!(
                    "Checkpoint refused: a heap data-file fsync failed earlier on this \
                     shard, so pages it wrote cannot be proven durable. The redo point \
                     stays at {} and the WAL above it is retained; recovery replays \
                     from there. Restart the server once the disk is healthy.",
                    control.last_checkpoint_lsn
                );
                checkpoint_mgr.note_finalize_failed(std::time::Instant::now());
                return false;
            }
            //    A page that failed to flush is still dirty and was neither
            //    written nor imaged: flush it again before finalizing, keeping
            //    this checkpoint's redo point.
            if checkpoint_mgr.page_flush_failed() {
                tracing::warn!(
                    "Checkpoint: a dirty page failed to flush; re-flushing before \
                     publishing redo_lsn={}",
                    redo_lsn
                );
                checkpoint_mgr.restart_flush(page_cache.dirty_page_count());
                checkpoint_mgr.note_finalize_failed(std::time::Instant::now());
                return false;
            }
            //    Every heap file the flush wrote must be durable. The fsync
            //    runs off the shard thread; this tick only polls it.
            match make_written_heap_files_durable(
                checkpoint_mgr,
                page_cache,
                manifest,
                control_path.parent().unwrap_or(Path::new(".")),
            ) {
                HeapFilesDurability::Durable => {}
                HeapFilesDurability::Pending => return false,
                HeapFilesDurability::Failed => {
                    checkpoint_mgr.note_finalize_failed(std::time::Instant::now());
                    return false;
                }
            }

            // 1. Write WAL checkpoint record with redo_lsn payload
            let mut payload = [0u8; 8];
            payload.copy_from_slice(&redo_lsn.to_le_bytes());
            let ckpt_lsn = wal.append(WalRecordType::Checkpoint, &payload);

            // 2. HARD ordering invariant (WAL-before-manifest): the
            //    checkpoint record must be durable before the manifest
            //    commit publishes redo_lsn. Bounded wait on the off-loop
            //    sync agent; failure aborts finalize (retried next tick),
            //    so redo_lsn never advances past durability.
            if let Err(e) = wal.wait_durable(
                ckpt_lsn,
                crate::persistence::wal_v3::segment::WAIT_DURABLE_TIMEOUT,
            ) {
                tracing::error!("Checkpoint WAL flush failed: {}", e);
                checkpoint_mgr.note_finalize_failed(std::time::Instant::now());
                return false;
            }

            // 3. Commit manifest (atomic dual-root write)
            if let Err(e) = manifest.commit() {
                tracing::error!("Checkpoint manifest commit failed: {}", e);
                checkpoint_mgr.note_finalize_failed(std::time::Instant::now());
                return false;
            }

            // 3b. P1 — tombstone GC: physically prune tombstones that satisfy
            // the two-axis retention policy (epoch age + wall-clock age).
            // GC is in-memory only here; the pruned state reaches disk on the
            // NEXT manifest commit (safe: current root still carries tombstones).
            {
                let now = std::time::Instant::now();
                let pruned =
                    manifest.gc_tombstones(tombstone_retain_epochs, tombstone_retain_secs, now);
                if pruned > 0 {
                    tracing::info!(
                        "Manifest GC: pruned {} tombstone(s) \
                         (retain_epochs={}, retain_secs={})",
                        pruned,
                        tombstone_retain_epochs,
                        tombstone_retain_secs,
                    );
                }
                // Wire P10 INFO metrics (P1 → RECL_MANIFEST_*).
                use std::sync::atomic::Ordering::Relaxed;
                crate::command::info_reclamation::RECL_MANIFEST_ACTIVE
                    .store(manifest.active_entry_count() as u64, Relaxed);
                crate::command::info_reclamation::RECL_MANIFEST_TOMBSTONES
                    .store(manifest.tombstone_count() as u64, Relaxed);
            }

            // 3c. Graph snapshot (2026-07 durability P0, Bug B): every
            // graph WAL record at or below the floor this checkpoint commits
            // must be materialized on disk BEFORE the control-file update —
            // step 4 advances the replay floor and step 6 recycles the WAL
            // segments holding those records. `graph_save` receives the WAL
            // LSN the snapshot covers (current head: the shard thread runs
            // this between mutations, so no record can slip in). A `false`
            // return aborts the finalize; the checkpoint retries next tick
            // with the old floor still in force.
            //
            // `current_lsn()` is the NEXT-to-be-assigned LSN — the snapshot
            // covers records strictly below it, so the floor is one less
            // (the G5 crash test's first post-checkpoint record lands
            // exactly on `current_lsn()` and must NOT be skipped).
            //
            // Kernel M3 K2: capture this LSN into a local variable ONCE and
            // reuse it both as the arg to `graph_save` AND, below, as
            // `control.graph_floor_lsn` — never a second, independently
            // recomputed `wal.current_lsn()` call at the control-file write
            // site. Two computations of "now" a few lines apart is exactly
            // the silent-drift risk the brief's Risk #1 calls out: a future
            // refactor that moves one call earlier than the other would
            // desynchronize the mirror from what `persist_graph_at_checkpoint`
            // actually snapshotted, with nothing failing except a rare crash
            // test. Same variable, same tick, by construction.
            let graph_floor_lsn = wal.current_lsn().saturating_sub(1);
            if !graph_save(graph_floor_lsn) {
                tracing::error!("Checkpoint aborted: graph snapshot failed");
                checkpoint_mgr.note_finalize_failed(std::time::Instant::now());
                return false;
            }

            // 4. Update control file with new checkpoint LSN + the graph
            // floor mirror (K2). `graph_metadata.json` (written durably by
            // `graph_save` above, which just returned `true`) remains the
            // graph engine's own replay-skip authority; `graph_floor_lsn`
            // here is a recycle-decision mirror of that SAME value, so the
            // two can never disagree.
            //    The in-memory copy is what the WAL recyclers cut below (step 6
            //    and the P6 ceiling's emergency recycle), so it must only ever
            //    hold a PUBLISHED redo point: if the write fails, restore it.
            let published = (
                control.last_checkpoint_lsn,
                control.last_checkpoint_epoch,
                control.graph_floor_lsn,
            );
            control.last_checkpoint_lsn = redo_lsn;
            control.last_checkpoint_epoch = manifest.epoch();
            control.graph_floor_lsn = graph_floor_lsn;
            if let Err(e) = control.write(control_path) {
                (
                    control.last_checkpoint_lsn,
                    control.last_checkpoint_epoch,
                    control.graph_floor_lsn,
                ) = published;
                tracing::error!("Checkpoint control file update failed: {}", e);
                checkpoint_mgr.note_finalize_failed(std::time::Instant::now());
                return false;
            }

            // 5. Mark checkpoint complete (also clears the finalize backoff).
            checkpoint_mgr.complete();

            // 6. Recycle old WAL segments — kernel M3 K2's min-across-planes
            // floor. Only KV (`last_checkpoint_lsn`) and graph
            // (`graph_floor_lsn`) publish a real LSN floor this milestone;
            // ws/mq stay at the sentinel `0` and are DELIBERATELY excluded
            // from this min (see `ShardControlFile::ws_floor_lsn` doc +
            // brief §Stage 2's "min-across-planes" correction / Risk #2) —
            // folding them in would collapse the floor to `0` forever on
            // any shard that has ever seen a WS/MQ record, which is
            // strictly worse than today's per-segment
            // `segment_holds_plane_history` content scan (still applied
            // inside `recycle_segments_before`, orthogonally, as the
            // AND-gate for those planes). `redo_lsn == control.last_checkpoint_lsn`
            // here (just assigned above), so this is `min(redo_lsn,
            // graph_floor_lsn)` in practice — structurally `>= redo_lsn`
            // always held implicitly via begin()-vs-Finalize call-site
            // ordering before K2; this makes it an explicit, checked value
            // instead of relying on that ordering never drifting.
            let recycle_floor = control.last_checkpoint_lsn.min(control.graph_floor_lsn);
            match wal.recycle_segments_before(recycle_floor) {
                Ok(n) if n > 0 => {
                    tracing::info!("Checkpoint: recycled {} old WAL segment(s)", n);
                }
                Err(e) => {
                    tracing::warn!("WAL segment recycling failed: {}", e);
                }
                _ => {}
            }

            tracing::info!(
                "Checkpoint complete: redo_lsn={}, graph_floor_lsn={}, epoch={}",
                redo_lsn,
                graph_floor_lsn,
                manifest.epoch()
            );
            true
        }
    }
}

#[cfg(test)]
mod checkpoint_tick_tests;

#[cfg(test)]
mod cascade_ledger_tests;

#[cfg(test)]
mod fold_inflight_tests;

#[cfg(test)]
mod ghost_slot_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::checkpoint::CheckpointTrigger;
    use crate::persistence::wal_v3::record::{WalRecordType, read_wal_v3_record};
    use crate::persistence::wal_v3::segment::{
        DEFAULT_SEGMENT_SIZE, WAL_V3_HEADER_SIZE, WalBounds,
    };

    /// Count FullPageImage records in a raw WAL segment file.
    pub(super) fn count_fpi_records(raw_data: &[u8]) -> usize {
        let mut offset = WAL_V3_HEADER_SIZE;
        let mut fpi_count = 0usize;
        while offset + 4 <= raw_data.len() {
            let record_len =
                u32::from_le_bytes(raw_data[offset..offset + 4].try_into().unwrap()) as usize;
            if record_len < 20 || offset + record_len > raw_data.len() {
                break;
            }
            if let Some(record) = read_wal_v3_record(&raw_data[offset..]) {
                if record.record_type == WalRecordType::FullPageImage {
                    fpi_count += 1;
                }
            }
            offset += record_len;
        }
        fpi_count
    }

    #[test]
    fn test_checkpoint_tick_produces_fpi_wal_records() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        let wal_dir = shard_dir.join("wal-v3");
        let data_dir = shard_dir.join("data");
        std::fs::create_dir_all(&wal_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();

        // Create PageCache with 4 frames of 4KB, 0 of 64KB
        let page_cache = PageCache::new(4, 0);

        // Set up 2 frames: fetch pages to make them VALID, then mark dirty
        for i in 0..2usize {
            let handle = page_cache
                .fetch_page(1, i as u64, false, |buf| {
                    buf[0] = 0xDE;
                    buf[1] = (i as u8) + 1;
                    Ok(())
                })
                .unwrap();
            page_cache.unpin_page(handle);
            page_cache.mark_dirty(1, i as u64, (i + 1) as u64);
        }

        // Set FPI_PENDING on all valid frames (simulates checkpoint begin)
        page_cache.arm_all_fpi_pending();

        assert_eq!(
            page_cache.dirty_page_count(),
            2,
            "Should have 2 dirty pages"
        );

        // Create a dummy heap file (at least 8KB so pwrite succeeds for 2 pages)
        let heap_path = data_dir.join("heap-000001.mpf");
        std::fs::write(&heap_path, vec![0u8; 8192]).unwrap();

        // Create WAL writer
        let mut wal =
            WalWriterV3::new(0, &wal_dir, DEFAULT_SEGMENT_SIZE, WalBounds::DEFAULT).unwrap();

        // Create checkpoint manager and begin checkpoint with dirty_count=2
        let trigger = CheckpointTrigger::new(300, 256 * 1024 * 1024, 0.9);
        let mut checkpoint_mgr = CheckpointManager::new(trigger);
        checkpoint_mgr.begin(wal.current_lsn(), 2);

        // Create manifest and control file
        let manifest_path = shard_dir.join("manifest.dat");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut control = ShardControlFile::new([0u8; 16]);
        let control_path = ShardControlFile::control_path(&shard_dir, 0);
        control.write(&control_path).unwrap();

        // Drive checkpoint ticks until all pages are flushed.
        // pages_per_tick is 1 (2 dirty / 270000 ticks, clamped to 1), so we need
        // 2 ticks of FlushPages before reaching Finalize.
        let mut tick_count = 0;
        loop {
            let finalized = handle_checkpoint_tick(
                &mut checkpoint_mgr,
                &page_cache,
                &mut wal,
                &mut manifest,
                &mut control,
                &control_path,
                2,
                300,
                &mut |_| true,
            );
            tick_count += 1;
            if finalized || !checkpoint_mgr.is_active() {
                break;
            }
            assert!(
                tick_count < 5000,
                "Checkpoint should complete within 5000 ticks"
            );
            // The heap-file fsync completes off the shard thread; tick at
            // roughly the event loop's 1 ms cadence while it runs.
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        // Flush WAL to disk
        wal.flush_sync().unwrap();

        // Read back the WAL segment and count FullPageImage records
        let seg_path = wal_dir.join("000000000001.wal");
        let raw_data = std::fs::read(&seg_path).unwrap();
        let fpi_count = count_fpi_records(&raw_data);

        assert_eq!(fpi_count, 2, "Expected exactly 2 FPI WAL records");

        // Verify dirty pages were flushed (DIRTY cleared via public API)
        assert_eq!(
            page_cache.dirty_page_count(),
            0,
            "All dirty pages should be flushed"
        );
    }

    #[test]
    fn test_checkpoint_tick_no_fpi_when_flag_not_set() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        let wal_dir = shard_dir.join("wal-v3");
        let data_dir = shard_dir.join("data");
        std::fs::create_dir_all(&wal_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();

        // Create PageCache with 4 frames of 4KB, 0 of 64KB
        let page_cache = PageCache::new(4, 0);

        // Set up 2 frames: VALID + DIRTY only (NO FPI_PENDING)
        for i in 0..2usize {
            let handle = page_cache
                .fetch_page(1, i as u64, false, |buf| {
                    buf[0] = 0xAB;
                    Ok(())
                })
                .unwrap();
            page_cache.unpin_page(handle);
            page_cache.mark_dirty(1, i as u64, (i + 1) as u64);
        }
        // Do NOT call arm_all_fpi_pending -- no FPI_PENDING set

        // Create a dummy heap file
        let heap_path = data_dir.join("heap-000001.mpf");
        std::fs::write(&heap_path, vec![0u8; 8192]).unwrap();

        // Create WAL writer
        let mut wal =
            WalWriterV3::new(0, &wal_dir, DEFAULT_SEGMENT_SIZE, WalBounds::DEFAULT).unwrap();

        // Create checkpoint manager and begin
        let trigger = CheckpointTrigger::new(300, 256 * 1024 * 1024, 0.9);
        let mut checkpoint_mgr = CheckpointManager::new(trigger);
        checkpoint_mgr.begin(wal.current_lsn(), 2);

        // Create manifest and control file
        let manifest_path = shard_dir.join("manifest.dat");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut control = ShardControlFile::new([0u8; 16]);
        let control_path = ShardControlFile::control_path(&shard_dir, 0);
        control.write(&control_path).unwrap();

        // Drive checkpoint ticks until all pages are flushed.
        let mut tick_count = 0;
        loop {
            let finalized = handle_checkpoint_tick(
                &mut checkpoint_mgr,
                &page_cache,
                &mut wal,
                &mut manifest,
                &mut control,
                &control_path,
                2,
                300,
                &mut |_| true,
            );
            tick_count += 1;
            if finalized || !checkpoint_mgr.is_active() {
                break;
            }
            assert!(
                tick_count < 5000,
                "Checkpoint should complete within 5000 ticks"
            );
            // The heap-file fsync completes off the shard thread; tick at
            // roughly the event loop's 1 ms cadence while it runs.
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        // Flush WAL to disk
        wal.flush_sync().unwrap();

        // Read back and count FPI records -- should be 0
        let seg_path = wal_dir.join("000000000001.wal");
        let raw_data = std::fs::read(&seg_path).unwrap();
        let fpi_count = count_fpi_records(&raw_data);

        assert_eq!(
            fpi_count, 0,
            "Expected 0 FPI WAL records when FPI_PENDING not set"
        );

        // DIRTY should still be cleared (pages were flushed to disk)
        assert_eq!(
            page_cache.dirty_page_count(),
            0,
            "All dirty pages should be flushed even without FPI"
        );
    }

    // ──────────────────────────────────────────────────────────────────
    // P3c — snapshot LSN stamping
    // ──────────────────────────────────────────────────────────────────

    use crate::runtime::channel;
    use crate::shard::shared_databases::ShardDatabases;
    use crate::storage::Database;

    /// P3c — pending BGSAVE captures the WAL LSN into the new SnapshotState.
    #[test]
    fn test_handle_pending_snapshot_stamps_wal_lsn() {
        let tmp = tempfile::tempdir().unwrap();
        let snap_dir = tmp.path().to_path_buf();
        let dbs = vec![vec![Database::new()]];
        let (shared, mut inits) = ShardDatabases::new(dbs);
        crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(
            inits.remove(0),
        ));

        let (tx, _rx) = channel::oneshot::<Result<(), String>>();
        let mut snapshot_state: Option<SnapshotState> = None;
        let mut reply_tx: Option<channel::OneshotSender<Result<(), String>>> = None;

        handle_pending_snapshot(
            Some((7, snap_dir.clone(), tx)),
            &mut snapshot_state,
            &mut reply_tx,
            &shared,
            None,
            0,
            12_345,
        );

        let s = snapshot_state.as_ref().expect("snapshot state created");
        assert_eq!(s.last_lsn(), 12_345);
        assert_eq!(s.epoch, 7);
    }

    /// P3c — wal_last_lsn == 0 (no WAL writer) leaves last_lsn at 0 — the
    /// "unknown provenance" sentinel PITR conservatively skips.
    #[test]
    fn test_handle_pending_snapshot_zero_lsn_is_unknown() {
        let tmp = tempfile::tempdir().unwrap();
        let dbs = vec![vec![Database::new()]];
        let (shared, mut inits) = ShardDatabases::new(dbs);
        crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(
            inits.remove(0),
        ));

        let (tx, _rx) = channel::oneshot::<Result<(), String>>();
        let mut snapshot_state: Option<SnapshotState> = None;
        let mut reply_tx: Option<channel::OneshotSender<Result<(), String>>> = None;

        handle_pending_snapshot(
            Some((1, tmp.path().to_path_buf(), tx)),
            &mut snapshot_state,
            &mut reply_tx,
            &shared,
            None,
            0,
            0,
        );

        assert_eq!(snapshot_state.as_ref().unwrap().last_lsn(), 0);
    }

    /// P3c — auto-save trigger fires snapshot creation with stamped LSN.
    #[test]
    fn test_check_auto_save_trigger_stamps_wal_lsn() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_string_lossy().to_string();
        let dbs = vec![vec![Database::new()]];
        let (shared, mut inits) = ShardDatabases::new(dbs);
        crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(
            inits.remove(0),
        ));

        // Trigger goes from epoch 0 → 5; helper observes 5 > last(0) and
        // creates a snapshot state.
        let (trigger_tx, trigger_rx) = channel::watch::<u64>(0);
        let _ = trigger_tx.send(5);

        let mut last_epoch: u64 = 0;
        let mut snapshot_state: Option<SnapshotState> = None;

        check_auto_save_trigger(
            &trigger_rx,
            &mut last_epoch,
            &mut snapshot_state,
            &shared,
            &Some(dir),
            None,
            0,
            999,
        );

        let s = snapshot_state.as_ref().expect("auto-save created state");
        assert_eq!(s.last_lsn(), 999);
        assert_eq!(s.epoch, 5);
        assert_eq!(last_epoch, 5);
    }

    // ── WS3 priority 3: `--disk-offload-threshold` proactive spill trigger ──

    /// RED (pre-fix intent)/GREEN: `should_run_pressure_cascade` must fire
    /// exactly at `used_memory > threshold * per_shard_budget`, not only at
    /// `maxmemory` itself -- this is what makes disk-offload "proactive
    /// instead of edge-triggered" (WS3 priority 3). `--disk-offload-threshold`
    /// was previously documented as "parsed but not acted upon"; this test
    /// exercises the real call site (`run_eviction_tick` publishes
    /// `published_shard_memory` earlier in the same tick, then this function
    /// reads it back) end to end at the unit level, without spinning up a
    /// real server process.
    #[test]
    fn test_should_run_pressure_cascade_fires_at_threshold_not_maxmemory() {
        use clap::Parser;
        let dbs = vec![vec![Database::new()]];
        let (shared, _inits) = ShardDatabases::new(dbs);

        // 1 shard, 1 MiB maxmemory => per-shard budget is the whole 1 MiB.
        // disk_offload_threshold defaults to 0.85 (see config.rs).
        let rt = crate::config::RuntimeConfig {
            maxmemory: 1024 * 1024,
            num_shards: 1,
            ..Default::default()
        };
        let runtime_config = Arc::new(parking_lot::RwLock::new(rt));
        let server_config = Arc::new(crate::config::ServerConfig::parse_from::<[&str; 0], &str>(
            [],
        ));
        assert!((server_config.disk_offload_threshold - 0.85).abs() < f64::EPSILON);

        // Below the 85% threshold (e.g. 50%): must NOT trigger the cascade.
        shared.publish_memory(0, (1024 * 1024) / 2);
        assert!(
            !should_run_pressure_cascade(&runtime_config, &server_config, &shared, 0, 0, 0),
            "50% used_memory must stay below the 85% disk-offload-threshold"
        );

        // Cross the threshold (e.g. 90%), still well short of maxmemory
        // itself: this is the "proactive, not edge-triggered" case -- the
        // whole point of WS3 priority 3.
        shared.publish_memory(0, (1024 * 1024 * 90) / 100);
        assert!(
            should_run_pressure_cascade(&runtime_config, &server_config, &shared, 0, 0, 0),
            "90% used_memory must cross the 85% disk-offload-threshold and \
             trigger the pressure cascade well before maxmemory is reached"
        );

        // maxmemory == 0 (unset) must never trigger, regardless of usage.
        {
            let rt2 = crate::config::RuntimeConfig {
                maxmemory: 0,
                num_shards: 1,
                ..Default::default()
            };
            let runtime_config2 = Arc::new(parking_lot::RwLock::new(rt2));
            shared.publish_memory(0, usize::MAX / 2);
            assert!(
                !should_run_pressure_cascade(&runtime_config2, &server_config, &shared, 0, 0, 0),
                "no memory limit configured => no pressure possible"
            );
        }
    }

    /// C: vector resident bytes must count toward the pressure trigger — a
    /// vector-heavy shard with near-zero KV memory still fires the cascade
    /// (previously vector memory was invisible to every pressure mechanism).
    #[test]
    fn test_pressure_cascade_triggered_by_vector_memory_alone() {
        use clap::Parser;
        let dbs = vec![vec![Database::new()]];
        let (shared, _inits) = ShardDatabases::new(dbs);

        let rt = crate::config::RuntimeConfig {
            maxmemory: 1024 * 1024, // 1 MiB per-shard budget (1 shard)
            num_shards: 1,
            ..Default::default()
        };
        let runtime_config = Arc::new(parking_lot::RwLock::new(rt));
        let server_config = Arc::new(crate::config::ServerConfig::parse_from::<[&str; 0], &str>(
            [],
        ));

        // KV memory is trivial (well under the 85% threshold on its own)...
        shared.publish_memory(0, 1024);
        assert!(
            !should_run_pressure_cascade(&runtime_config, &server_config, &shared, 0, 0, 0),
            "KV alone is far below threshold => no cascade without vector accounting"
        );

        // ...but the shard is holding ~950 KiB of resident vector segments,
        // pushing total past the 85% (~892 KiB) threshold.
        let vec_bytes = (1024 * 1024 * 93) / 100;
        assert!(
            should_run_pressure_cascade(&runtime_config, &server_config, &shared, 0, vec_bytes, 0),
            "vector resident memory must contribute to the pressure trigger"
        );
    }

    // ── task #58 (LOW-1/LOW-2): allocator_overhead + pagecache accounting ──

    #[test]
    fn test_compute_allocator_overhead_subtracts_every_tracked_plane() {
        // RSS 1000, tracked planes sum to 400 (100 dashtable + 50 vector +
        // 40 text + 30 graph + 20 lua + 100 pagecache + 60 backlog) => 600
        // left over as allocator overhead.
        let overhead = compute_allocator_overhead(1000, 100, 50, 40, 30, 20, 100, 60);
        assert_eq!(overhead, 600);
    }

    #[test]
    fn test_compute_allocator_overhead_saturates_at_zero() {
        // Tracked sum exceeds RSS (stale cross-thread snapshot, e.g. a
        // pagecache figure a tick ahead of a shrinking RSS read) => clamp to
        // 0 instead of underflowing usize.
        let overhead = compute_allocator_overhead(100, 80, 0, 0, 0, 0, 50, 0);
        assert_eq!(overhead, 0);
    }

    #[test]
    fn test_compute_allocator_overhead_zero_tracked_equals_rss() {
        // A build with every subsystem disabled/empty attributes the whole
        // RSS to allocator overhead (matches MEMORY DOCTOR's existing
        // on-demand formula's degenerate case).
        let overhead = compute_allocator_overhead(2048, 0, 0, 0, 0, 0, 0, 0);
        assert_eq!(overhead, 2048);
    }

    #[test]
    fn test_pagecache_publishes_into_shard_store_memory() {
        // Wire-through test: a shard's ShardStoreMemory.pagecache atomic
        // round-trips through the same publisher `run_eviction_tick` uses
        // (store_memory_per_shard[shard_id].pagecache), and sums correctly
        // across shards the way INFO memory / the Prometheus 15s updater do.
        let dbs: Vec<Vec<crate::storage::Database>> = vec![
            vec![crate::storage::Database::new()],
            vec![crate::storage::Database::new()],
        ];
        let (shared, _inits) = ShardDatabases::new(dbs);

        shared.store_memory_per_shard[0]
            .pagecache
            .store(4096 * 10, std::sync::atomic::Ordering::Relaxed);
        shared.store_memory_per_shard[1]
            .pagecache
            .store(65536 * 3, std::sync::atomic::Ordering::Relaxed);

        let total: usize = shared
            .store_memory_per_shard
            .iter()
            .map(|m| m.pagecache.load(std::sync::atomic::Ordering::Relaxed))
            .sum();
        assert_eq!(total, 4096 * 10 + 65536 * 3);
    }

    /// moon#506 wire-through: the VM figure lands in its OWN atomic, separate
    /// from the script-source cache, and both sum across shards the way INFO
    /// memory / MEMORY DOCTOR / the Prometheus updater read them.
    #[test]
    fn test_lua_vm_and_script_cache_are_separate_published_atomics() {
        use std::sync::atomic::Ordering;
        let dbs: Vec<Vec<crate::storage::Database>> = vec![
            vec![crate::storage::Database::new()],
            vec![crate::storage::Database::new()],
        ];
        let (shared, _inits) = ShardDatabases::new(dbs);

        // What the two really look like in production: 48 bytes of script
        // text against a ~25KB interpreter heap.
        shared.store_memory_per_shard[0]
            .lua
            .store(48, Ordering::Relaxed);
        shared.store_memory_per_shard[0]
            .lua_vm
            .store(25_403, Ordering::Relaxed);
        shared.store_memory_per_shard[1]
            .lua_vm
            .store(24_165, Ordering::Relaxed);

        let vm_total: usize = shared
            .store_memory_per_shard
            .iter()
            .map(|m| m.lua_vm.load(Ordering::Relaxed))
            .sum();
        assert_eq!(vm_total, 25_403 + 24_165);

        let combined: usize = shared
            .store_memory_per_shard
            .iter()
            .map(|m| m.lua.load(Ordering::Relaxed) + m.lua_vm.load(Ordering::Relaxed))
            .sum();
        assert_eq!(combined, 48 + 25_403 + 24_165);
    }

    /// moon#506 root cause, pinned structurally: the shard periodic tick has
    /// TWO bodies in `event_loop.rs` — a tokio `select!` arm and a monoio
    /// counter arm — and the defect was a sample written into one of them.
    /// The VM sample must therefore live ONLY in `run_eviction_tick`, the body
    /// both arms call, never inline in a runtime-specific arm.
    /// moon#997 review, pinned structurally: a shard's cold file ids have ONE
    /// home, the shared `spill_file_id` counter.
    ///
    /// The event loop used to keep a second copy (`next_file_id`), re-synced
    /// with `max` once per tick. On tokio the cross-shard SPSC drain ran after
    /// that sync and advanced the counter; the eviction tick and warm
    /// transitions then allocated from the stale copy — re-issuing the drain's
    /// ids — and `run_eviction_tick` wrote it back with a plain `set`, moving
    /// the counter BACKWARDS so the next handler spill re-issued them again.
    /// The interleaving depends on tick timing, so this pins the two
    /// properties that make it impossible instead of racing for it:
    /// no second copy exists, and no write-back can lower the counter.
    #[test]
    fn test_cold_file_id_counter_has_one_home_and_never_moves_back() {
        let event_loop_src = include_str!("event_loop.rs");
        let copies: Vec<usize> = event_loop_src
            .match_indices("next_file_id")
            .filter(|(i, _)| !event_loop_src[*i..].starts_with("next_file_id_seed"))
            .map(|(i, _)| event_loop_src[..i].lines().count())
            .collect();
        assert!(
            copies.is_empty(),
            "event_loop.rs keeps a second cold file_id counter (`next_file_id`, lines \
             {copies:?}); allocate from `spill_file_id` via file_id_seed::allocate_from"
        );

        // Every write-back to the shared counter outside the seed must be
        // monotonic. Test modules are excluded (they quote the pattern).
        let sources = [
            (
                "shard/persistence_tick.rs",
                include_str!("persistence_tick.rs"),
            ),
            ("shard/spsc_handler.rs", include_str!("spsc_handler.rs")),
            ("shard/event_loop.rs", event_loop_src),
            (
                "server/conn/handler_monoio/mod.rs",
                include_str!("../server/conn/handler_monoio/mod.rs"),
            ),
            (
                "server/conn/handler_sharded/mod.rs",
                include_str!("../server/conn/handler_sharded/mod.rs"),
            ),
            (
                "scripting/bridge.rs",
                include_str!("../scripting/bridge.rs"),
            ),
        ];
        let mut lowering: Vec<String> = Vec::new();
        for (name, src) in sources {
            let body = src.split("\n#[cfg(test)]").next().unwrap_or(src);
            for (n, line) in body.lines().enumerate() {
                if line.contains("spill_file_id.set(")
                    && !line.contains(".max(")
                    && !line.contains("spill_file_id.set(spill_seed)")
                {
                    lowering.push(format!("{name}:{}: {}", n + 1, line.trim()));
                }
            }
        }
        assert!(
            lowering.is_empty(),
            "a write-back can move the shard's cold file_id counter backwards and \
             re-issue ids already on disk: {lowering:#?}"
        );
    }

    #[test]
    fn test_lua_vm_sample_lives_only_in_the_shared_tick_body() {
        let event_loop_src = include_str!("event_loop.rs");
        assert!(
            !event_loop_src.contains("lua_vm"),
            "event_loop.rs samples the Lua VM inline. Move it into \
             persistence_tick::run_eviction_tick — a publish written into one \
             runtime's tick arm is invisible on the other, which is exactly \
             how moon#506 shipped a monoio-blind figure."
        );
        assert_eq!(
            event_loop_src.matches("run_eviction_tick(").count(),
            2,
            "expected exactly two tick call sites (tokio select! arm + monoio \
             counter arm). If a third runtime arm was added, confirm it also \
             calls the shared body before updating this count."
        );
    }

    #[test]
    fn test_pagecache_resident_bytes_matches_frame_counts_formula() {
        // Task #58's stated formula: num 4k frames actually grown * 4096 +
        // num 64k frames actually grown * 65536. Exercise the real
        // PageCache::resident_buffer_bytes() (not a re-implementation) to
        // pin the contract this module's publish call relies on.
        let cache = PageCache::new(8, 4);
        assert_eq!(
            cache.resident_buffer_bytes(),
            0,
            "freshly constructed cache has no grown buffers"
        );

        // Growing one 4KB and one 64KB frame via fetch_page (miss path reads
        // through read_fn, which fills the buffer to a full page).
        let handle_small = cache
            .fetch_page(1, 0, false, |buf: &mut [u8]| -> std::io::Result<()> {
                buf.fill(0xAB);
                Ok(())
            })
            .expect("fetch 4KB page");
        cache.unpin_page(handle_small);
        let handle_large = cache
            .fetch_page(2, 0, true, |buf: &mut [u8]| -> std::io::Result<()> {
                buf.fill(0xCD);
                Ok(())
            })
            .expect("fetch 64KB page");
        cache.unpin_page(handle_large);

        assert_eq!(
            cache.resident_buffer_bytes(),
            4096 + 65536,
            "resident_buffer_bytes must equal grown-4KB*4096 + grown-64KB*65536"
        );
    }

    /// #870 — the lag guard must not re-arm at base cadence after an
    /// overflow pass that freed nothing. Establishes all three conditions
    /// the issue names (WAL over the ceiling, a completed checkpoint, plane
    /// records in every sealed segment), drives the real overflow path, and
    /// asserts on the scheduler's decisions and the scan counter — never on
    /// wall time (`last_checkpoint_at` is synthesized).
    #[test]
    fn test_870_wal_overflow_pass_backs_off_when_it_frees_nothing() {
        use std::time::{Duration, Instant};

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        let wal_dir = shard_dir.join("wal-v3");
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Condition 3: every sealed segment holds a sole-copy MQ record.
        let mut wal = WalWriterV3::new(0, &wal_dir, 512, WalBounds::new(0, 1024)).unwrap();
        // Condition 1: ceiling far below the WAL we are about to write.
        for i in 0..60 {
            wal.append(WalRecordType::MqCreate, b"mq-plane-payload-#870");
            if (i + 1) % 3 == 0 {
                wal.flush_sync().unwrap();
            }
        }
        wal.flush_sync().unwrap();
        let sealed = wal.current_segment_sequence() - 1;
        assert!(sealed >= 4, "need several sealed segments, got {sealed}");
        assert!(wal.stats().unwrap().total_bytes > wal.max_wal_bytes());

        let page_cache = PageCache::new(4, 0);
        let trigger = CheckpointTrigger::new(300, 256 * 1024 * 1024, 0.9);
        let mut checkpoint_mgr = CheckpointManager::new(trigger);
        let manifest_path = shard_dir.join("manifest.dat");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut control = ShardControlFile::new([0u8; 16]);
        let control_path = ShardControlFile::control_path(&shard_dir, 0);
        control.write(&control_path).unwrap();

        const LAG_MS: u64 = 10_000;
        let mut run =
            |wal: &mut WalWriterV3, control: &mut ShardControlFile, elapsed_ms: u64| -> bool {
                let last_checkpoint_at = Instant::now()
                    .checked_sub(Duration::from_millis(elapsed_ms))
                    .expect("Instant arithmetic");
                maybe_force_checkpoint_on_wal_overflow(
                    &mut checkpoint_mgr,
                    wal,
                    &page_cache,
                    &mut manifest,
                    control,
                    &control_path,
                    0,
                    last_checkpoint_at,
                    LAG_MS,
                    &mut |_| true,
                )
            };

        // Pass 1 — lag elapsed, pass runs, forces the checkpoint (condition
        // 2: `redo_lsn` now covers every sealed segment) and frees nothing.
        assert!(run(&mut wal, &mut control, LAG_MS + 1), "pass 1 must run");
        assert!(
            control.last_checkpoint_lsn > 0,
            "condition 2: a completed checkpoint"
        );
        assert_eq!(
            wal.plane_scans(),
            sealed,
            "pass 1 pays one cold scan per sealed segment"
        );
        for seq in 1..=sealed {
            assert!(
                crate::persistence::wal_v3::segment::WalSegment::segment_path(&wal_dir, seq)
                    .exists(),
                "plane-blocked segment {seq} must survive"
            );
        }

        // Pass 2 — the same base lag has elapsed again. The previous pass
        // freed nothing, so re-arming at base cadence is the #870 loop:
        // the guard must back off and DEFER this one.
        assert!(
            !run(&mut wal, &mut control, LAG_MS + 1),
            "#870: a pass that freed nothing re-armed the lag guard at base cadence"
        );
        // Pass 3 — the doubled lag has elapsed: the pass runs again (the
        // backoff is bounded, not a freeze) and still reads no sealed file.
        assert!(
            run(&mut wal, &mut control, 2 * LAG_MS + 1),
            "pass must run once the backed-off lag elapses"
        );
        assert_eq!(
            wal.plane_scans(),
            sealed,
            "#870: a later pass re-read immutable sealed segments"
        );
        // Pass 4 — quadrupled lag now required.
        assert!(
            !run(&mut wal, &mut control, 2 * LAG_MS + 1),
            "second no-op pass doubles the lag again"
        );
        assert!(run(&mut wal, &mut control, 4 * LAG_MS + 1));
    }

    /// moon#893 review: a spill completion whose file id the manifest already
    /// lists (Active, or Tombstoned inside its retention window) is refused by
    /// `ShardManifest::add_file`. The completion path used to `unwrap()` that
    /// — a panic on the shard thread. It must instead put every key back in
    /// the hot table from its in-flight payload (the hot entry was removed at
    /// enqueue, so `spill_inflight` holds the only copy) and publish nothing
    /// to the cold index, since the manifest does not describe that file.
    #[test]
    fn spill_completion_for_an_already_listed_id_rehydrates_its_keys() {
        use crate::persistence::kv_page::ValueType;
        use crate::persistence::manifest::{FileEntry, FileStatus, StorageTier};
        use crate::persistence::page::PageType;
        use crate::shard::slice::{ShardSlice, init_shard, test_support::make_init, with_shard_db};
        use crate::storage::db::PendingSpill;
        use crate::storage::tiered::spill_thread::{SpillCompletion, SpillCompletionEntry};

        for tombstoned in [false, true] {
            std::thread::spawn(move || {
                init_shard(ShardSlice::new(make_init(0, 1)));
                let tmp = tempfile::tempdir().unwrap();
                let entry = |id: u64| FileEntry {
                    file_id: id,
                    file_type: PageType::KvLeaf as u8,
                    status: FileStatus::Active,
                    tier: StorageTier::Hot,
                    page_size_log2: 12,
                    page_count: 1,
                    byte_size: 4096,
                    created_lsn: 0,
                    db_index: 0,
                    max_key_hash: 0,
                    last_modified_lsn: 0,
                };
                let mut manifest =
                    ShardManifest::create(&tmp.path().join("shard-0.manifest")).unwrap();
                manifest.add_file(entry(5)).unwrap();
                if tombstoned {
                    manifest.remove_file(5, PageType::KvLeaf);
                }
                let before = manifest.files().to_vec();

                let keys: Vec<bytes::Bytes> = (0..3)
                    .map(|i| bytes::Bytes::from(format!("k{i}")))
                    .collect();
                with_shard_db(0, |db| {
                    db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
                    for (i, k) in keys.iter().enumerate() {
                        db.spill_inflight_mark(
                            k.clone(),
                            PendingSpill {
                                req_id: 100 + i as u64,
                                value_type: ValueType::String,
                                value_bytes: bytes::Bytes::from(format!("v{i}")),
                                ttl_ms: None,
                            },
                        );
                    }
                });
                let completion = SpillCompletion {
                    file_entry: entry(5),
                    entries: keys
                        .iter()
                        .enumerate()
                        .map(|(i, k)| SpillCompletionEntry {
                            key: k.clone(),
                            db_index: 0,
                            page_idx: 0,
                            slot_idx: i as u16,
                            ttl_ms: None,
                            value_type: ValueType::String,
                            req_file_id: 100 + i as u64,
                        })
                        .collect(),
                    success: true,
                    failed_request: None,
                };
                let mut sink = ColdMarkerSink {
                    aof_pool: None,
                    wal_writer: None,
                    shard_id: 0,
                    wal_kv_log: false,
                };
                let mut shard_manifest = Some(manifest);
                apply_completion_vec(vec![completion], &mut shard_manifest, &mut sink);

                assert_eq!(
                    shard_manifest.as_ref().unwrap().files(),
                    &before[..],
                    "tombstoned={tombstoned}: the manifest keeps the entry it had"
                );
                with_shard_db(0, |db| {
                    assert!(
                        db.spill_inflight_is_empty(),
                        "tombstoned={tombstoned}: in-flight records must be retired"
                    );
                    for (i, k) in keys.iter().enumerate() {
                        assert!(
                            db.cold_index.as_ref().unwrap().lookup(k).is_none(),
                            "tombstoned={tombstoned}: {k:?} must not be published cold"
                        );
                        let e = db.data().get(k.as_ref()).unwrap_or_else(|| {
                            panic!("tombstoned={tombstoned}: {k:?} is in neither plane")
                        });
                        assert_eq!(e.value.as_bytes(), Some(format!("v{i}").as_bytes()),);
                    }
                });
            })
            .join()
            .unwrap_or_else(|e| {
                let msg = e
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_default();
                panic!("tombstoned={tombstoned}: {msg}")
            });
        }
    }

    /// moon#1202 harness: run one successful spill completion for `k0..k2`
    /// (file 5, in-flight payloads `v0..v2`) against an AOF pool whose
    /// writer channel has `capacity` slots, pre-filled with `prefill`
    /// records and never drained. Returns what the test asserts on.
    struct MarkerRun {
        /// The file id the manifest lists afterwards (5 or nothing).
        manifest_ids: Vec<u64>,
        /// Per key: `(published cold, hot value)`.
        planes: Vec<(bool, Option<Vec<u8>>)>,
        in_flight_left: bool,
        /// Whether the pool now reports a dropped acked append.
        missing_appends: bool,
        /// Every append the writer would have received, in order.
        logged: Vec<bytes::Bytes>,
    }

    fn run_marker_completion(capacity: usize, prefill: usize) -> MarkerRun {
        use crate::persistence::aof::{AofMessage, AofWriterPool};
        use crate::persistence::kv_page::ValueType;
        use crate::persistence::manifest::{FileEntry, FileStatus, StorageTier};
        use crate::persistence::page::PageType;
        use crate::shard::slice::{ShardSlice, init_shard, test_support::make_init, with_shard_db};
        use crate::storage::db::PendingSpill;
        use crate::storage::tiered::spill_thread::{SpillCompletion, SpillCompletionEntry};

        std::thread::spawn(move || {
            init_shard(ShardSlice::new(make_init(0, 1)));
            let tmp = tempfile::tempdir().unwrap();
            let manifest = ShardManifest::create(&tmp.path().join("shard-0.manifest")).unwrap();
            let keys: Vec<bytes::Bytes> = (0..3)
                .map(|i| bytes::Bytes::from(format!("k{i}")))
                .collect();
            with_shard_db(0, |db| {
                db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
                for (i, k) in keys.iter().enumerate() {
                    db.spill_inflight_mark(
                        k.clone(),
                        PendingSpill {
                            req_id: 100 + i as u64,
                            value_type: ValueType::String,
                            value_bytes: bytes::Bytes::from(format!("v{i}")),
                            ttl_ms: None,
                        },
                    );
                }
            });
            let completion = SpillCompletion {
                file_entry: FileEntry {
                    file_id: 5,
                    file_type: PageType::KvLeaf as u8,
                    status: FileStatus::Active,
                    tier: StorageTier::Hot,
                    page_size_log2: 12,
                    page_count: 1,
                    byte_size: 4096,
                    created_lsn: 0,
                    db_index: 0,
                    max_key_hash: 0,
                    last_modified_lsn: 0,
                },
                entries: keys
                    .iter()
                    .enumerate()
                    .map(|(i, k)| SpillCompletionEntry {
                        key: k.clone(),
                        db_index: 0,
                        page_idx: 0,
                        slot_idx: i as u16,
                        ttl_ms: None,
                        value_type: ValueType::String,
                        req_file_id: 100 + i as u64,
                    })
                    .collect(),
                success: true,
                failed_request: None,
            };

            let (tx, rx) = crate::runtime::channel::mpsc_bounded::<AofMessage>(capacity);
            let pool = AofWriterPool::top_level(tx);
            for _ in 0..prefill {
                assert!(pool.try_send_append(0, 1, 0, bytes::Bytes::from_static(b"filler")));
            }
            let mut sink = ColdMarkerSink {
                aof_pool: Some(&pool),
                wal_writer: None,
                shard_id: 0,
                wal_kv_log: false,
            };
            let mut shard_manifest = Some(manifest);
            apply_completion_vec(vec![completion], &mut shard_manifest, &mut sink);

            let manifest_ids = shard_manifest
                .as_ref()
                .unwrap()
                .files()
                .iter()
                .map(|f| f.file_id)
                .collect();
            let (planes, in_flight_left) = with_shard_db(0, |db| {
                let planes = keys
                    .iter()
                    .map(|k| {
                        let cold = db.cold_index.as_ref().unwrap().lookup(k).is_some();
                        let hot = db
                            .data()
                            .get(k.as_ref())
                            .and_then(|e| e.value.as_bytes().map(|b| b.to_vec()));
                        (cold, hot)
                    })
                    .collect();
                (planes, !db.spill_inflight_is_empty())
            });
            let mut logged = Vec::new();
            while let Ok(msg) = rx.try_recv() {
                if let AofMessage::Append { bytes, .. } = msg {
                    logged.push(bytes);
                }
            }
            MarkerRun {
                manifest_ids,
                planes,
                in_flight_left,
                missing_appends: pool.overflow_for(0).is_missing_appends(),
                logged,
            }
        })
        .join()
        .unwrap_or_else(|_| panic!("marker completion thread panicked"))
    }

    /// moon#1202: the writer channel stays full for the whole backpressure
    /// bound, so the `MOON.SPILLED` marker cannot be logged. It must never be
    /// dropped with the spill still published (the AOF then no longer cuts
    /// those keys, and a later retry could only land out of order): the
    /// publish is withdrawn instead. Every key goes back to the hot table
    /// from its in-flight payload, nothing is published cold, the manifest
    /// never names the file (the orphan sweep reclaims it), and the pool
    /// reports no lost append — nothing that was acknowledged is missing.
    #[test]
    fn a_marker_the_writer_cannot_take_withdraws_the_spill_instead_of_dropping() {
        let run = run_marker_completion(1, 1);
        assert_eq!(run.logged.len(), 1, "only the filler reached the writer");
        assert_eq!(
            run.planes,
            (0..3)
                .map(|i| (false, Some(format!("v{i}").into_bytes())))
                .collect::<Vec<_>>(),
            "every key must be back in RAM and none published cold"
        );
        assert!(!run.in_flight_left, "in-flight records must be retired");
        assert!(
            run.manifest_ids.is_empty(),
            "the manifest must not list a file no marker cuts"
        );
        assert!(
            !run.missing_appends,
            "a withdrawn marker is not a lost acked append"
        );
    }

    /// Control: with room in the channel the marker is logged and the keys
    /// are published cold, exactly as before.
    #[test]
    fn a_marker_the_writer_takes_publishes_the_spill() {
        let run = run_marker_completion(4, 0);
        let want = crate::persistence::cold_records::serialize_spilled(
            5,
            &[
                bytes::Bytes::from_static(b"k0"),
                bytes::Bytes::from_static(b"k1"),
                bytes::Bytes::from_static(b"k2"),
            ],
        );
        assert_eq!(run.logged, vec![want]);
        assert_eq!(run.planes, vec![(true, None); 3]);
        assert!(!run.in_flight_left);
        assert_eq!(run.manifest_ids, vec![5]);
        assert!(!run.missing_appends);
    }
}
