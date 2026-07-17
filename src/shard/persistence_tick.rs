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
            let (segment_counts, base_timestamps) = crate::shard::slice::with_shard(|s| {
                let mut seg_counts = Vec::with_capacity(s.databases.len());
                let mut base_ts = Vec::with_capacity(s.databases.len());
                for db in s.databases.iter() {
                    seg_counts.push(db.data().segment_count());
                    base_ts.push(db.base_timestamp());
                }
                (seg_counts, base_ts)
            });
            let db_count = shard_databases.db_count();
            let mut state = SnapshotState::new_from_metadata(
                shard_id as u16,
                epoch,
                db_count,
                segment_counts,
                base_timestamps,
                snap_path,
            );
            // P3c — stamp the WAL LSN so PITR can pick this snapshot as a
            // valid replay base. 0 means "no WAL writer active" (e.g. pure
            // RDB mode) — header records 0, recovery falls back to full replay.
            if wal_last_lsn > 0 {
                state.set_last_lsn(wal_last_lsn);
            }
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
        if let Some(dir) = persistence_dir {
            // When disk-offload is enabled, write snapshot to the offload shard directory
            // so v3 recovery can find it alongside WAL v3 segments and manifest.
            let snap_path = if let Some(offload) = disk_offload_dir {
                let shard_dir = offload.join(format!("shard-{}", shard_id));
                let _ = std::fs::create_dir_all(&shard_dir);
                shard_dir.join(format!("shard-{}.rrdshard", shard_id))
            } else {
                std::path::PathBuf::from(dir).join(format!("shard-{}.rrdshard", shard_id))
            };
            let (segment_counts, base_timestamps) = crate::shard::slice::with_shard(|s| {
                let mut seg_counts = Vec::with_capacity(s.databases.len());
                let mut base_ts = Vec::with_capacity(s.databases.len());
                for db in s.databases.iter() {
                    seg_counts.push(db.data().segment_count());
                    base_ts.push(db.base_timestamp());
                }
                (seg_counts, base_ts)
            });
            let db_count = shard_databases.db_count();
            let mut state = SnapshotState::new_from_metadata(
                shard_id as u16,
                new_epoch,
                db_count,
                segment_counts,
                base_timestamps,
                snap_path,
            );
            // P3c — stamp the WAL LSN before the header is written.
            if wal_last_lsn > 0 {
                state.set_last_lsn(wal_last_lsn);
            }
            *snapshot_state = Some(state);
        }
    }
}

/// Advance snapshot one segment and check if done (synchronous part).
///
/// Returns `true` if the snapshot is complete and ready for async finalization.
pub(crate) fn advance_snapshot_segment(
    snapshot_state: &mut Option<SnapshotState>,
    shard_databases: &Arc<ShardDatabases>,
    shard_id: usize,
) -> bool {
    let _ = shard_id; // E2 removes
    if let Some(snap) = snapshot_state {
        let current_db = snap.current_db_index();
        let db_count = shard_databases.db_count();
        if current_db < db_count {
            crate::shard::slice::with_shard_db(current_db, |db| snap.advance_one_segment_db(db))
        } else {
            // All databases serialized, return true to trigger finalization
            true
        }
    } else {
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
    if let Some(wal) = wal_v3 {
        if let Err(e) = wal.flush_if_needed() {
            tracing::error!("WAL v3 flush failed: {}", e);
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
    next_file_id: &mut u64,
    shard_id: usize,
    wal: &mut Option<WalWriterV3>,
) {
    let count = vector_store.try_warm_transitions_all_idle(
        shard_dir,
        manifest,
        warm_after_secs,
        idle_after_secs,
        next_file_id,
        wal,
    );
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
/// enabled, otherwise falls back to plain `timers::run_eviction`. Finally
/// publishes the latest `next_file_id` back to the shared `Rc<Cell>` so
/// connection handlers spawning fresh spills do not collide on file IDs.
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
    next_file_id: &mut u64,
    wal_v3_writer: &mut Option<crate::persistence::wal_v3::segment::WalWriterV3>,
    script_cache: &std::rc::Rc<std::cell::RefCell<crate::scripting::ScriptCache>>,
    spill_file_id: &std::rc::Rc<std::cell::Cell<u64>>,
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
        apply_spill_completions(spill_t, shard_manifest, shard_databases, shard_id);
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

    {
        let rt = runtime_config.read();
        // C5 / Phase 3: compute per-shard KV memory via ShardSlice without
        // lock acquisitions (avoids per-DB read locks; estimated_memory() is
        // an O(1) accumulator read). Published unconditionally: MEMORY DOCTOR
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
        // pre-gate (inline_write_can_skip_eviction / try_evict_if_needed).
        let used = crate::shard::slice::with_shard(|s| {
            s.databases
                .iter()
                .map(|db| {
                    db.estimated_memory()
                        + db.cold_index.as_ref().map_or(0, |ci| ci.resident_bytes())
                })
                .sum::<usize>()
        });
        shard_databases.publish_memory(shard_id, used);
        // Elastic budgets only exist under a finite maxmemory cap.
        if rt.maxmemory > 0 {
            shard_databases.recompute_elastic_budget(shard_id, &rt);
        }
    }

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
            lua_bytes += mem.lua.load(Ordering::Relaxed);
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

    if server_config.disk_offload_enabled()
        && should_run_pressure_cascade(
            runtime_config,
            server_config,
            shard_databases,
            shard_id,
            vector_resident_bytes,
        )
    {
        handle_memory_pressure(
            page_cache,
            shard_databases,
            shard_id,
            runtime_config,
            server_config,
            shard_manifest,
            next_file_id,
            wal_v3_writer,
            spill_thread,
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
        let eviction_shard_dir = server_config
            .effective_disk_offload_dir()
            .join(format!("shard-{}", shard_id));
        let mut spill_ctx: Option<crate::storage::eviction::SpillContext<'_>> = None;
        if server_config.disk_offload_enabled()
            && let Some(ref mut manifest) = *shard_manifest
        {
            spill_ctx = Some(crate::storage::eviction::SpillContext {
                shard_dir: &eviction_shard_dir,
                manifest,
                next_file_id,
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

    // Sync file ID back to the shared Cell so connection handlers see it.
    spill_file_id.set(*next_file_id);
}

/// Drain any final spill completions and shut down the spill thread.
///
/// Shared between the tokio and monoio shutdown arms in `event_loop.rs`.
pub(crate) fn drain_and_shutdown_spill(
    spill_thread: &mut Option<crate::storage::tiered::spill_thread::SpillThread>,
    shard_manifest: &mut Option<crate::persistence::manifest::ShardManifest>,
    shard_databases: &std::sync::Arc<super::shared_databases::ShardDatabases>,
    shard_id: usize,
) {
    if let Some(spill_t) = spill_thread.as_ref() {
        apply_spill_completions(spill_t, shard_manifest, shard_databases, shard_id);
    }
    if let Some(st) = spill_thread.take() {
        // shutdown() returns any completions from the thread's final buffer
        // flush that the drain above did not see; apply them so those cold keys
        // are not lost (file on disk but never recorded in the manifest).
        let leftover = st.shutdown();
        apply_completion_vec(leftover, shard_manifest);
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
) {
    let _ = shard_databases; // E2 removes
    let _ = shard_id; // E2 removes
    let completions = spill_thread.drain_completions();
    apply_completion_vec(completions, shard_manifest);
}

/// Apply a batch of spill completions: ONE manifest `add_file`+commit per file,
/// one `cold_index` insert per KV entry within it. Shared by the live drain
/// (`apply_spill_completions`) and the shutdown final-flush drain.
fn apply_completion_vec(
    completions: Vec<crate::storage::tiered::spill_thread::SpillCompletion>,
    shard_manifest: &mut Option<crate::persistence::manifest::ShardManifest>,
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
    for c in completions {
        if !c.success {
            tracing::warn!(
                file_id = c.file_entry.file_id,
                "Spill pwrite failed on background thread"
            );
            continue;
        }

        let file_id = c.file_entry.file_id;

        // RAM-only manifest update; durability handled once per batch below.
        if let Some(ref mut manifest) = *shard_manifest {
            manifest.add_file(c.file_entry);
            manifest_dirty = true;
        }

        // Insert one ColdIndex entry per KV within this file. `ttl_ms` rides
        // along from the `SpillCompletionEntry` so the proactive TTL sweep
        // (R1, H-2) can judge expiry from the in-RAM index alone.
        for entry in c.entries {
            let location = crate::storage::tiered::cold_index::ColdLocation {
                file_id,
                page_idx: entry.page_idx,
                slot_idx: entry.slot_idx,
                ttl_ms: entry.ttl_ms,
            };

            crate::shard::slice::with_shard_db(entry.db_index, |db| {
                if let Some(ref mut ci) = db.cold_index {
                    ci.insert(entry.key.clone(), location);
                }
            });
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
pub(crate) fn should_run_pressure_cascade(
    runtime_config: &std::sync::Arc<parking_lot::RwLock<crate::config::RuntimeConfig>>,
    server_config: &std::sync::Arc<crate::config::ServerConfig>,
    shard_databases: &std::sync::Arc<super::shared_databases::ShardDatabases>,
    shard_id: usize,
    vector_resident_bytes: usize,
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
    server_config: &std::sync::Arc<crate::config::ServerConfig>,
    shard_manifest: &mut Option<ShardManifest>,
    next_file_id: &mut u64,
    wal_v3: &mut Option<crate::persistence::wal_v3::segment::WalWriterV3>,
    spill_thread: Option<&crate::storage::tiered::spill_thread::SpillThread>,
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
    if let Some(ref mut manifest) = *shard_manifest {
        let shard_dir = server_config
            .effective_disk_offload_dir()
            .join(format!("shard-{}", shard_id));
        let count = crate::shard::slice::with_shard(|s| {
            s.vector_store.try_warm_transitions_all_idle(
                &shard_dir,
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
            if total_mem > budget {
                let db_count = shard_databases.db_count();
                let shard_dir = server_config
                    .effective_disk_offload_dir()
                    .join(format!("shard-{}", shard_id));

                if let Some(spill_t) = spill_thread {
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
                            let _ = crate::storage::eviction::try_evict_if_needed_async_spill_with_total_budget_reporting(
                                db,
                                &rt,
                                &sender,
                                &shard_dir,
                                next_file_id,
                                total_mem,
                                i,
                                budget,
                                shard_manifest.as_mut(),
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
                                    );
                                },
                            );
                        });
                    }
                    // Drop sender clone immediately to avoid shutdown deadlock
                    drop(sender);
                } else {
                    // Sync spill fallback
                    for i in 0..db_count {
                        crate::shard::slice::with_shard_db(i, |db| {
                            if let Some(ref mut manifest) = *shard_manifest {
                                let mut ctx = crate::storage::eviction::SpillContext {
                                    shard_dir: &shard_dir,
                                    manifest,
                                    next_file_id,
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
                                let _ = crate::storage::eviction::try_evict_if_needed_with_spill_and_total_budget_reporting(
                                    db,
                                    &rt,
                                    Some(&mut ctx),
                                    total_mem,
                                    budget,
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
                                        );
                                    },
                                );
                            } else {
                                // No manifest reachable: this IS the plain
                                // -drop path (task #34, Wave A) — emit.
                                let _ = crate::storage::eviction::try_evict_if_needed_with_spill_and_total_budget_reporting(
                                    db, &rt, None, total_mem, budget,
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
                                        );
                                    },
                                );
                            }
                        });
                    }
                }
            }
        }
    }

    // Step 4: NoEviction policy check -- if we reached here with noeviction,
    // log a warning. The actual OOM rejection is handled inside try_evict_if_needed.
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

/// Force a complete checkpoint synchronously (used by BGSAVE and shutdown).
///
/// Calls `force_begin` to bypass trigger conditions, then drives the
/// checkpoint state machine to completion in a tight loop. No-op if a
/// checkpoint is already active.
#[allow(clippy::too_many_arguments)]
pub(crate) fn force_checkpoint(
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
    let elapsed_ms = last_checkpoint_at.elapsed().as_millis() as u64;
    if elapsed_ms < max_checkpoint_lag_ms {
        tracing::debug!(
            "Shard {}: P6 WAL overflow ({} bytes) but lag guard active ({}/{}ms), deferring",
            shard_id,
            total_wal,
            elapsed_ms,
            max_checkpoint_lag_ms
        );
        return false;
    }

    tracing::warn!(
        "Shard {}: P6 WAL ceiling trigger — {} bytes > max {} bytes, forcing checkpoint + aggressive recycle",
        shard_id,
        total_wal,
        wal.max_wal_bytes()
    );

    // Force a synchronous checkpoint (drives the state machine to completion).
    // If checkpoint is already active, force_checkpoint is a no-op — the
    // in-progress checkpoint will advance next tick and the recycle will run
    // in handle_checkpoint_tick's Finalize arm.
    force_checkpoint(
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
    // was a no-op (checkpoint already active) or failed silently, using the
    // current WAL head would be unsafe — we would recycle segments whose dirty
    // pages have not been flushed to data files yet.
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
            tracing::debug!(
                "Shard {}: P6 aggressive recycle: no segments eligible at redo_lsn={}",
                shard_id,
                redo_lsn
            );
        }
        Err(e) => {
            tracing::warn!(
                "Shard {}: P6 aggressive recycle failed: {} — disk may be full",
                shard_id,
                e
            );
        }
    }

    true
}

/// Handle one checkpoint tick. Called from the event loop every 1ms when
/// disk-offload is enabled.
///
/// Returns `true` if a finalize step was completed this tick.
///
/// The caller provides all I/O dependencies — CheckpointManager itself is pure state.
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
    match checkpoint_mgr.advance_tick() {
        CheckpointAction::Nothing => false,
        CheckpointAction::FlushPages(count) => {
            // Collect FPI payloads during sweep, then append to WAL after.
            // This avoids dual-mutable-borrow of `wal` across closures.
            let mut fpi_payloads: Vec<Vec<u8>> = Vec::new();

            let flushed = page_cache.flush_dirty_pages_with_fpi(
                count,
                &mut |page_lsn| {
                    // HARD ordering invariant (log-before-data): the WAL must
                    // be durable past this page's LSN before the page pwrite.
                    // Bounded blocking wait on the off-loop sync agent; Err
                    // aborts this flush batch (checkpoint retries next tick).
                    if wal.current_lsn() > page_lsn {
                        wal.wait_durable(
                            page_lsn,
                            crate::persistence::wal_v3::segment::WAIT_DURABLE_TIMEOUT,
                        )
                    } else {
                        Ok(())
                    }
                },
                &mut |file_id, page_offset, _is_large, data| {
                    // Collect FPI payload for deferred WAL append.
                    // Payload format: file_id(8 LE) + page_offset(8 LE) + flag(1) + page_data
                    // Flag: 0x00 = uncompressed, 0x01 = LZ4-compressed
                    let mut payload = Vec::with_capacity(17 + data.len());
                    payload.extend_from_slice(&file_id.to_le_bytes());
                    payload.extend_from_slice(&page_offset.to_le_bytes());
                    if data.len() > 256 {
                        let compressed = lz4_flex::compress_prepend_size(data);
                        if compressed.len() < data.len() {
                            payload.push(0x01);
                            payload.extend_from_slice(&compressed);
                        } else {
                            payload.push(0x00);
                            payload.extend_from_slice(data);
                        }
                    } else {
                        payload.push(0x00);
                        payload.extend_from_slice(data);
                    }
                    fpi_payloads.push(payload);
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
                    let shard_dir = control_path.parent().unwrap_or(Path::new("."));
                    let file_path = shard_dir
                        .join("data")
                        .join(format!("heap-{:06}.mpf", file_id));
                    let file = std::fs::OpenOptions::new().write(true).open(&file_path)?;
                    crate::util::file_ext::write_at(&file, data, byte_offset)?;
                    Ok(())
                },
            );

            // Deferred FPI WAL append -- now safe since flush_dirty_pages_with_fpi
            // returned and the closures no longer borrow `wal`.
            for payload in &fpi_payloads {
                wal.append(WalRecordType::FullPageImage, payload);
            }

            if flushed > 0 {
                tracing::trace!(
                    "Checkpoint: flushed {} dirty pages (with FPI, {} FPI records)",
                    flushed,
                    fpi_payloads.len()
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
            control.last_checkpoint_lsn = redo_lsn;
            control.last_checkpoint_epoch = manifest.epoch();
            control.graph_floor_lsn = graph_floor_lsn;
            if let Err(e) = control.write(control_path) {
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
mod tests {
    use super::*;
    use crate::persistence::checkpoint::CheckpointTrigger;
    use crate::persistence::wal_v3::record::{WalRecordType, read_wal_v3_record};
    use crate::persistence::wal_v3::segment::{DEFAULT_SEGMENT_SIZE, WAL_V3_HEADER_SIZE};

    /// Count FullPageImage records in a raw WAL segment file.
    fn count_fpi_records(raw_data: &[u8]) -> usize {
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
        let mut wal = WalWriterV3::new(0, &wal_dir, DEFAULT_SEGMENT_SIZE).unwrap();

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
            // Safety: don't loop forever
            assert!(
                tick_count < 100,
                "Checkpoint should complete within 100 ticks"
            );
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
        let mut wal = WalWriterV3::new(0, &wal_dir, DEFAULT_SEGMENT_SIZE).unwrap();

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
                tick_count < 100,
                "Checkpoint should complete within 100 ticks"
            );
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
            !should_run_pressure_cascade(&runtime_config, &server_config, &shared, 0, 0),
            "50% used_memory must stay below the 85% disk-offload-threshold"
        );

        // Cross the threshold (e.g. 90%), still well short of maxmemory
        // itself: this is the "proactive, not edge-triggered" case -- the
        // whole point of WS3 priority 3.
        shared.publish_memory(0, (1024 * 1024 * 90) / 100);
        assert!(
            should_run_pressure_cascade(&runtime_config, &server_config, &shared, 0, 0),
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
                !should_run_pressure_cascade(&runtime_config2, &server_config, &shared, 0, 0),
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
            !should_run_pressure_cascade(&runtime_config, &server_config, &shared, 0, 0),
            "KV alone is far below threshold => no cascade without vector accounting"
        );

        // ...but the shard is holding ~950 KiB of resident vector segments,
        // pushing total past the 85% (~892 KiB) threshold.
        let vec_bytes = (1024 * 1024 * 93) / 100;
        assert!(
            should_run_pressure_cascade(&runtime_config, &server_config, &shared, 0, vec_bytes),
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
}
