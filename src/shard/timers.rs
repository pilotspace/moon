//! Timer arm body handlers for the shard event loop.
//!
//! Extracted from shard/mod.rs. Contains expiry, eviction, block timeout,
//! and WAL sync tick handlers.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::blocking::BlockingRegistry;
use crate::config::RuntimeConfig;

use super::shared_databases::ShardDatabases;

/// Run cooperative active expiry across all databases.
/// Shard 0 also updates the RSS gauge (once per expiry cycle, ~100ms).
///
/// task #34 (Wave A): the `record_reason_del` handles are threaded through
/// so every key the sweep actually removes gets a dual-plane (AOF +
/// replication) `DEL` record — mirrors the `ShardMessage::SwapDb` synthetic
/// -command pattern via `crate::replication::reason_del::record_reason_del`.
/// Zero-cost when neither an AOF pool nor a replica/backlog is wired: the
/// helper's own fast-path checks (`wal_fanout_has_work` inside
/// `wal_append_and_fanout`) skip the serialization + fan-out entirely.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_active_expiry(
    shard_databases: &Arc<ShardDatabases>,
    shard_id: usize,
    wal_writer: &mut Option<crate::persistence::wal_v3::segment::WalWriterV3>,
    repl_backlog: &crate::replication::backlog::SharedBacklog,
    replica_txs: &mut Vec<crate::shard::dispatch::ReplicaFanout>,
    repl_state: &Option<crate::replication::state::OffsetHandle>,
    aof_pool: Option<&Arc<crate::persistence::aof::AofWriterPool>>,
    wal_kv_log: bool,
    // #71b: when attached to a master, this node is a read-only replica and
    // must NOT run its own expiry deletion sweep. Redis semantics: the replica
    // keeps a logically-expired key resident (reads see it as gone via the
    // read-path `is_key_expired` check) until the master streams the
    // authoritative `DEL`/expire record, so both sides remove the key at the
    // same point in the replication stream. A replica running its own sweep
    // would delete early and diverge (and, on a promoted-then-demoted flap,
    // could even resurrect ordering hazards). The RSS gauge update below still
    // runs on either role.
    is_replica: bool,
) {
    if !is_replica {
        let db_count = shard_databases.db_count();
        // #454 P2.8: ONE shared backpressure bound for this entire sweep
        // (per-key minting could stall the shard bound x victim-count).
        let mut reason_del_budget = crate::persistence::aof::AOF_REASON_DEL_BACKPRESSURE_BOUND;
        for i in 0..db_count {
            crate::shard::slice::with_shard_db(i, |db| {
                crate::server::expiration::expire_cycle_direct(db, &mut |key| {
                    // Cache-invalidation consumers subscribe to this to drop
                    // their copy. Queued here and delivered by the shard
                    // loop's own drain — there is no connection to attribute
                    // an expiry to.
                    crate::notify::notify_keyspace_event(
                        crate::notify::NotifyFlags::EXPIRED,
                        "expired",
                        key,
                        i,
                    );
                    crate::replication::reason_del::record_reason_del(
                        key,
                        i,
                        wal_writer,
                        repl_backlog,
                        replica_txs,
                        repl_state,
                        shard_id,
                        aof_pool,
                        wal_kv_log,
                        &mut reason_del_budget,
                    );
                });
            });
        }
    }
    // Update RSS gauge on shard 0 only, once per second (not every 100ms tick).
    // Gated by a simple counter to reduce /proc/self/statm open/read/close churn.
    if shard_id == 0 {
        use std::sync::atomic::{AtomicU8, Ordering};
        static RSS_TICK: AtomicU8 = AtomicU8::new(0);
        let tick = RSS_TICK.fetch_add(1, Ordering::Relaxed);
        if tick.is_multiple_of(10) {
            let rss = crate::admin::metrics_setup::get_rss_bytes();
            if rss > 0 {
                crate::admin::metrics_setup::update_rss_bytes(rss);
            }
            // Republish the maxmemory footprint correction that
            // `evict_to_budget` reads on every write. This tick is where the
            // measurement is paid for — once a second, on one shard — instead
            // of three syscalls plus an instance-wide accounting sum per SET.
            //
            // On Linux the footprint IS this same `/proc/self/statm` read, so
            // reuse the value rather than paying for it twice. On macOS the
            // footprint is `phys_footprint` from `task_info`, a genuinely
            // different field — resident_size under-reports a swapped-out
            // process by ~20x — so it needs its own read.
            #[cfg(target_os = "linux")]
            let footprint = rss;
            #[cfg(not(target_os = "linux"))]
            let footprint = crate::admin::metrics_setup::process_footprint_bytes();
            // Unconditional, including when the read returned 0. A platform
            // with no footprint reader (Windows) must still tick the chore:
            // the refresh treats 0 as "unknown" and leaves the correction
            // neutral, and the counter it advances is what proves the chore
            // itself is alive. Guarding this call on `footprint > 0` made the
            // liveness signal indistinguishable from an unwired chore on every
            // platform that cannot measure.
            crate::admin::footprint::refresh_footprint_correction(footprint);
        }
    }
}

/// Run background eviction if maxmemory (or a per-db quota) is configured.
///
/// task #45: `spill` carries the same write-then-durable-then-drop
/// `SpillContext` the interactive write-path eviction gate and the memory-
/// pressure cascade's sync-spill fallback use. Before this fix the tick
/// NEVER received one — every collection (and, separately, every string —
/// see `evict_one_with_spill`'s task #45 fix) victim it picked was plain-
/// dropped even with `--disk-offload enable` and a durability backstop
/// (`--appendonly yes`/`--save`) present, producing the
/// `cold_collection_visibility` stable-ghost flake: a key whose hot copy this
/// path dropped without a cold copy stayed `EXISTS == 1` if some OTHER
/// eviction path's earlier (or later, via the async spill-completion race)
/// cold-index entry for the same key was still live, while every content
/// read on it came back empty forever. Threading a real `SpillContext`
/// through (built by the caller from `shard_manifest`/`shard_dir`/
/// `next_file_id` — `None` when no durability backstop exists, matching the
/// documented "spill is inert without one" rule) makes this path spill
/// exactly like every other eviction path, closing the race at its root
/// instead of papering over one instance of it.
///
/// task #34 (Wave A): plain-dropped victims (no `spill` context passed in —
/// `--disk-offload disable`, or no durability backstop) get a dual-plane
/// `DEL` record via the same `record_reason_del` handles `run_active_expiry`
/// uses.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_eviction(
    shard_databases: &Arc<ShardDatabases>,
    shard_id: usize,
    runtime_config: &Arc<parking_lot::RwLock<RuntimeConfig>>,
    wal_writer: &mut Option<crate::persistence::wal_v3::segment::WalWriterV3>,
    repl_backlog: &crate::replication::backlog::SharedBacklog,
    replica_txs: &mut Vec<crate::shard::dispatch::ReplicaFanout>,
    repl_state: &Option<crate::replication::state::OffsetHandle>,
    aof_pool: Option<&Arc<crate::persistence::aof::AofWriterPool>>,
    wal_kv_log: bool,
    mut spill: Option<&mut crate::storage::eviction::SpillContext<'_>>,
) {
    let rt = runtime_config.read();
    if rt.maxmemory > 0 {
        // GAP-1: enforce the elastic budget (0 = static fallback inside).
        let budget = shard_databases.elastic_budget(shard_id);
        // A3 (accounting spine, tiering-v2 D3): vector segment memory counts
        // toward maxmemory — it is data-plane memory. Published by the same
        // 100ms tick (`run_eviction_tick` stores it before calling us; lag
        // ≤ 1 tick, documented acceptable). One Relaxed load per tick, no
        // recompute on this path.
        //
        // REVIEW FIX (CRITICAL, perf+security reviews concurring): the
        // used-term is an AGGREGATE — Σ all dbs' KV + the shard's vector
        // bytes, computed ONCE — never the shard-wide vector term re-added
        // to each logical db's independent check. Per-db duplication both
        // under-detected (KV spread across dbs, no single db + vector over
        // budget ⇒ nothing evicted while aggregate RSS overran) and
        // over-evicted (vector term over budget ⇒ every db drained instead
        // of stopping at the aggregate target).
        //
        // Semantics when the un-evictable vector term alone exceeds the
        // budget: KV drains then errors OOM — the vector tier legitimately
        // owns that memory (shared-budget, same as one tenant's KV growth
        // evicting siblings under allkeys-lru); per-db quotas (WS5b) remain
        // the tenant-isolation mechanism. The pressure cascade (which CAN
        // shrink vectors via offload to COLD) fires earlier at
        // `--disk-offload-threshold` (0.85×budget), so with disk-offload
        // enabled vectors shed first.
        let vector_bytes = shard_databases.store_memory_per_shard[shard_id]
            .vector
            .load(std::sync::atomic::Ordering::Relaxed);
        let db_count = shard_databases.db_count();
        let mut kv_total: usize = 0;
        for i in 0..db_count {
            kv_total = kv_total.saturating_add(crate::shard::slice::with_shard_db(i, |db| {
                db.estimated_memory()
            }));
        }
        // Running aggregate: each db's eviction reduces it by what that db
        // actually freed; once it drops to the budget, remaining dbs see an
        // under-budget total and return immediately (no eviction).
        let mut remaining = kv_total.saturating_add(vector_bytes);
        // #454 P2.8: ONE shared backpressure bound for this entire sweep
        // (per-key minting could stall the shard bound x victim-count).
        let mut reason_del_budget = crate::persistence::aof::AOF_REASON_DEL_BACKPRESSURE_BOUND;
        for i in 0..db_count {
            crate::shard::slice::with_shard_db(i, |db| {
                let before = db.estimated_memory();
                // #139: the shared SpillContext must carry THIS iteration's
                // db so every spill file's manifest entry attributes its
                // victim to the database it actually came from.
                if let Some(ctx) = spill.as_deref_mut() {
                    ctx.db_index = i;
                }
                let _ = crate::storage::eviction::evict_to_budget(
                    db,
                    &rt,
                    crate::storage::eviction::EvictionRun::sync_spill(spill.as_deref_mut())
                        .total(remaining)
                        .budget(budget)
                        .report(&mut |key| {
                            crate::replication::reason_del::record_reason_del(
                                key,
                                i,
                                wal_writer,
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
                let freed = before.saturating_sub(db.estimated_memory());
                remaining = remaining.saturating_sub(freed);
            });
        }
    }
    // WS5b: proactive per-db quota sweep. This is what catches a quota'd db
    // that grew over budget WITHOUT a write to that db triggering the
    // on-write gate — e.g. `SWAPDB` moving an unbounded db's data into a
    // quota'd slot, or `MOVE` inserting into a quota'd target db (both
    // documented in docs/guides/isolation.md as lazily, not synchronously,
    // enforced). Runs even when `--maxmemory` is unset, gated by the
    // lock-free "any db quota configured?" atomic so an unconfigured
    // instance pays only one atomic load per tick, not a `Vec` scan.
    if crate::storage::db_quota::db_maxmemory_any_set() {
        let db_count = shard_databases.db_count();
        for i in 0..db_count {
            crate::shard::slice::with_shard_db(i, |db| {
                let _ = crate::storage::db_quota::check_db_maxmemory(db, i, &rt);
            });
        }
    }
}

/// Expire timed-out blocked clients.
///
/// Heap-driven since c10k W6: cost is O(due + stale-heap-entries), not
/// O(all blocked waiters), so the 100 Hz cadence is safe at 10k+ blocked
/// clients. The visit count is intentionally dropped here — it exists for
/// tests/observability at the registry layer.
pub(crate) fn expire_blocked_clients(blocking_rc: &Rc<RefCell<BlockingRegistry>>) {
    let now = std::time::Instant::now();
    let _ = blocking_rc.borrow_mut().expire_timed_out(now);
}

/// Checkpoint tick interval in milliseconds.
/// Same 1ms tick as WAL flush — checkpoint manager advances one tick per call.
#[allow(dead_code)]
pub const CHECKPOINT_TICK_MS: u64 = 1;

/// Warm tier transition check interval in milliseconds (10 seconds).
/// Infrequent enough to avoid overhead, responsive enough to catch aged segments.
pub const WARM_CHECK_INTERVAL_MS: u64 = 10_000;

/// Fire MQ triggers whose debounce window has elapsed.
///
/// Called from the event loop periodic tick at 100ms cadence (same interval
/// as expiry/eviction). For each trigger entry where `pending_fire_ms > 0`
/// and `pending_fire_ms <= now_ms`, fires the callback by publishing a
/// notification to the `mq:trigger:{queue_key}` pub/sub channel.
///
/// Triggers fire on the workspace home shard only (hash tag ensures
/// all workspace keys route to one shard, so the TriggerRegistry on
/// that shard is authoritative).
///
/// That is true of the KEY and says nothing about the SUBSCRIBER. A consumer
/// that subscribes to `mq:trigger:<queue>` lands on whichever shard accepted
/// its connection, and moon keeps one pub/sub registry per shard — so the
/// notification goes out through the same cross-shard fan-out `PUBLISH` uses,
/// not a bare local publish. Publishing locally only meant a consumer was
/// woken just when it happened to connect to the queue's home shard: right at
/// `--shards 1`, roughly a 1-in-N chance at `--shards N`, and silent either
/// way because the subscriber count is only `debug!`ged (moon#474).
pub(crate) fn fire_pending_mq_triggers(
    shard_databases: &Arc<ShardDatabases>,
    shard_id: usize,
    now_ms: u64,
    pubsub_registry: &Arc<parking_lot::RwLock<crate::pubsub::PubSubRegistry>>,
    remote_subscriber_map: &Arc<
        parking_lot::RwLock<crate::shard::remote_subscriber_map::RemoteSubscriberMap>,
    >,
    dispatch_tx: &std::cell::RefCell<Vec<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>>,
    notifiers: &[std::sync::Arc<crate::runtime::channel::Notify>],
) {
    // Collect keys of triggers ready to fire via with_shard (no lock needed on slice path).
    let ready_keys: Vec<bytes::Bytes> = crate::shard::slice::with_shard(|s| {
        let Some(ref mut reg) = s.trigger_registry else {
            return Vec::new();
        };
        reg.fire_ready(now_ms)
    });
    if ready_keys.is_empty() {
        return;
    }

    // Collect (channel, message) pairs via with_shard while not crossing await.
    let notifications: Vec<(bytes::Bytes, bytes::Bytes)> = crate::shard::slice::with_shard(|s| {
        let Some(ref mut reg) = s.trigger_registry else {
            return Vec::new();
        };
        let mut out = Vec::with_capacity(ready_keys.len());
        for key in &ready_keys {
            if let Some(entry) = reg.get(key) {
                let channel = {
                    let mut ch = Vec::with_capacity(11 + entry.queue_key.len());
                    ch.extend_from_slice(b"mq:trigger:");
                    ch.extend_from_slice(&entry.queue_key);
                    bytes::Bytes::from(ch)
                };
                out.push((channel, entry.callback_cmd.clone()));
            }
            reg.mark_fired(key, now_ms);
        }
        out
    });
    let _ = shard_databases; // shared handle no longer needed on this path

    // Publish outside with_shard — no re-entry. The fan-out serves local
    // subscribers directly and hands every OTHER shard holding a subscriber
    // for the channel one batched `NotifyPublish`, which is the only
    // cross-thread wake that reaches a subscriber's connection task.
    if tracing::enabled!(tracing::Level::DEBUG) {
        for (channel, _) in &notifications {
            tracing::debug!(
                "Shard {}: MQ trigger fired on channel {:?}",
                shard_id,
                String::from_utf8_lossy(channel),
            );
        }
    }
    crate::notify_fanout::publish_from_shard(
        shard_id,
        pubsub_registry,
        remote_subscriber_map,
        dispatch_tx,
        notifiers,
        notifications,
    );
}

/// Default interval for cold-tier orphan sweep (5 minutes).
pub const COLD_ORPHAN_SWEEP_INTERVAL_SECS: u64 = 300;

/// Run a cold-tier orphan sweep across all databases for a single shard.
///
/// Iterates each database's cold index and removes entries whose key is now
/// present in the hot DashTable (i.e. a hot write shadowed the spilled copy).
/// The corresponding DataFile and manifest entry are tombstoned atomically.
///
/// Also runs the proactive TTL-expiry sweep (R1, H-2:
/// `tmp/OFFLOAD-COMPRESSION-REVIEW.md`) in the same tick: cold entries whose
/// TTL has passed and which were never re-read (the on-read reclaim in
/// `cold_read.rs` only fires on an actual `GET`) previously leaked their
/// index entry and file refcount forever. Sharing this tick's cadence keeps
/// the plumbing (interval config, disk-offload gate, manifest handle) single
/// -sourced rather than adding a second timer for the same "sweep the cold
/// tier" concern.
///
/// Called from the shard event loop on a low-priority 5-minute timer (or the
/// interval configured via `--cold-orphan-sweep-interval-secs`).
///
/// # Concurrency
///
/// Each database is accessed via `with_shard_db`, which gives exclusive
/// access for the duration of the closure on this shard's own event-loop
/// thread (thread-per-core model — no other thread ever touches this
/// shard's databases). Spill, read-promotion, and eviction all run through
/// the same single-threaded exclusivity, preventing TOCTOU races with either
/// sweep.
pub(crate) fn run_cold_orphan_sweep(
    shard_databases: &Arc<super::shared_databases::ShardDatabases>,
    shard_id: usize,
    shard_dir: &std::path::Path,
    mut manifest: Option<&mut crate::persistence::manifest::ShardManifest>,
    now_ms: u64,
) {
    use crate::storage::tiered::cold_index::{MAX_EXPIRED_SWEEP_BATCH, SweepStats};

    let db_count = shard_databases.db_count();
    let mut total = SweepStats::default();
    let mut total_expired = SweepStats::default();

    for db_idx in 0..db_count {
        // Phase 1: collect orphan keys and pending-unlink flag inside with_shard_db.
        let (orphan_keys, has_pending_unlink) = crate::shard::slice::with_shard_db(db_idx, |db| {
            if db.cold_index.is_none() {
                return (Vec::new(), false);
            }
            let orphan_keys: Vec<bytes::Bytes> = db
                .cold_index
                .as_ref()
                .map(|ci| {
                    ci.iter()
                        .filter(|(key, _loc)| db.is_hot(key))
                        .map(|(k, _)| k.clone())
                        .collect()
                })
                .unwrap_or_default();
            let has_pending_unlink = db
                .cold_index
                .as_ref()
                .map(|ci| ci.has_pending_unlink())
                .unwrap_or(false);
            (orphan_keys, has_pending_unlink)
        });

        if !orphan_keys.is_empty() || has_pending_unlink {
            // Phase 2: delete files + update cold_index (orphan-shadow reclaim).
            let stats = crate::shard::slice::with_shard_db(db_idx, |db| {
                db.cold_index.as_mut().and_then(|ci| {
                    ci.sweep_known_orphans(orphan_keys, shard_dir, manifest.as_deref_mut())
                        .map_err(|e| {
                            tracing::error!(
                                shard = shard_id,
                                db = db_idx,
                                err = %e,
                                "cold_orphan_sweep: manifest commit error",
                            );
                            e
                        })
                        .ok()
                })
            });

            if let Some(s) = stats {
                total.entries_reclaimed += s.entries_reclaimed;
                total.bytes_reclaimed = total.bytes_reclaimed.saturating_add(s.bytes_reclaimed);
            }
        }

        // R1 / H-2: TTL-expiry sweep. Independent of the orphan-shadow state
        // above — a cold entry can expire and never be re-read regardless of
        // whether any OTHER key on this db is hot-shadowed or has a file
        // unlink pending, so this always runs (when the db has a cold index)
        // rather than being gated by the `continue` above.
        let expired_stats = crate::shard::slice::with_shard_db(db_idx, |db| {
            db.cold_index.as_mut().and_then(|ci| {
                ci.sweep_expired(
                    now_ms,
                    shard_dir,
                    manifest.as_deref_mut(),
                    MAX_EXPIRED_SWEEP_BATCH,
                )
                .map_err(|e| {
                    tracing::error!(
                        shard = shard_id,
                        db = db_idx,
                        err = %e,
                        "cold_expired_sweep: manifest commit error",
                    );
                    e
                })
                .ok()
            })
        });

        if let Some(s) = expired_stats {
            total_expired.entries_reclaimed += s.entries_reclaimed;
            total_expired.bytes_reclaimed = total_expired
                .bytes_reclaimed
                .saturating_add(s.bytes_reclaimed);
        }
    }

    // moon#656: publish this shard's cold-tier shape for `INFO MoonStore`.
    //
    // This is the ONE place in the codebase that already holds all three
    // inputs at once — every db's ColdIndex, the shard's manifest, and the
    // shared publisher — and it runs on a fixed timer whenever disk-offload
    // is enabled (the select arm is NOT gated on there being work to do), so
    // the numbers refresh even on a completely idle instance.
    //
    // Cost: the ColdIndex reads are O(1) accumulator loads; the manifest walk
    // is O(files) but happens once per sweep interval (60s default), not on
    // the 100ms memory tick, where an O(files) walk would cost milliseconds
    // of shard-thread time on a G2-sized cold tier.
    publish_cold_stats(shard_databases, shard_id, db_count, manifest.as_deref());

    if total.entries_reclaimed > 0 {
        tracing::info!(
            shard = shard_id,
            entries = total.entries_reclaimed,
            bytes = total.bytes_reclaimed,
            "cold_orphan_sweep: shard sweep complete",
        );
    }
    if total_expired.entries_reclaimed > 0 {
        tracing::info!(
            shard = shard_id,
            entries = total_expired.entries_reclaimed,
            bytes = total_expired.bytes_reclaimed,
            "cold_expired_sweep: shard sweep complete",
        );
    }
}

/// Publish one shard's cold-tier shape into the shared per-shard slot read by
/// `INFO MoonStore` (moon#656).
///
/// Split out of [`run_cold_orphan_sweep`] so the numbers it reports can be
/// asserted directly, without driving a whole sweep.
///
/// The manifest predicate — `status == Active && file_type == KvLeaf` — is the
/// same one [`crate::storage::tiered::cold_index::ColdIndex::rebuild_from_manifest`]
/// uses to decide what to re-index after a restart. Using a different predicate
/// here would report a cold tier the server would not reload.
pub(crate) fn publish_cold_stats(
    shard_databases: &Arc<super::shared_databases::ShardDatabases>,
    shard_id: usize,
    db_count: usize,
    manifest: Option<&crate::persistence::manifest::ShardManifest>,
) {
    use std::sync::atomic::Ordering;

    let Some(slot) = shard_databases.cold_per_shard.get(shard_id) else {
        return;
    };

    let (keys, index_bytes, files_referenced, files_pending_unlink) = (0..db_count).fold(
        (0usize, 0usize, 0usize, 0usize),
        |(k, b, fr, pu), db_idx| {
            crate::shard::slice::with_shard_db(db_idx, |db| match db.cold_index.as_ref() {
                Some(ci) => (
                    k.saturating_add(ci.len()),
                    b.saturating_add(ci.resident_bytes()),
                    fr.saturating_add(ci.referenced_file_count()),
                    pu.saturating_add(ci.pending_unlink_len()),
                ),
                None => (k, b, fr, pu),
            })
        },
    );

    let (disk_bytes, files) = manifest.map_or((0u64, 0usize), |m| {
        m.files()
            .iter()
            .filter(|e| {
                e.status == crate::persistence::manifest::FileStatus::Active
                    && e.file_type == crate::persistence::page::PageType::KvLeaf as u8
            })
            .fold((0u64, 0usize), |(bytes, n), e| {
                (bytes.saturating_add(e.byte_size), n + 1)
            })
    });

    slot.keys.store(keys, Ordering::Relaxed);
    slot.index_bytes.store(index_bytes, Ordering::Relaxed);
    slot.files_referenced
        .store(files_referenced, Ordering::Relaxed);
    slot.files_pending_unlink
        .store(files_pending_unlink, Ordering::Relaxed);
    slot.disk_bytes.store(disk_bytes, Ordering::Relaxed);
    slot.files.store(files, Ordering::Relaxed);
    slot.manifest_missing
        .store(manifest.is_none(), Ordering::Relaxed);
    if manifest.is_none() && keys > 0 {
        // Not merely a reporting gap. A shard serving cold keys with no
        // readable manifest cannot re-index them after a restart, because
        // `ColdIndex::rebuild_from_manifest` is the only thing that puts them
        // back — those keys are live now and gone on the next boot. The
        // missing INFO field is the least of it, so say so at 60s cadence
        // rather than silently reporting a zero.
        tracing::warn!(
            shard = shard_id,
            cold_keys = keys,
            "cold tier has keys but this shard has no readable manifest: on-disk \
             size is unreportable and these keys will NOT survive a restart",
        );
    }
    // Last, and `Release` rather than `Relaxed`: this flag is what allows INFO
    // to print the six values above, so it must not become visible before
    // them. With Relaxed on both sides a reader could see the flag on the
    // FIRST sweep and still read zeros — publishing a block that claims an
    // empty cold tier, which is exactly the misleading zero this whole change
    // exists to remove. Paired with the `Acquire` load in `read_cold_totals`.
    // Stale-by-one-sweep values remain fine and expected; zeros-with-flag do not.
    slot.published.store(true, Ordering::Release);
}

/// WAL v3 fsync on 1-second interval (mirrors v2 everysec pattern).
///
/// Flush buffered WAL v3 data to the page cache and hand the fsync to the
/// off-loop sync agent — the fsync itself no longer blocks the shard event
/// loop (measured 10–16 ms probe stalls per second on GCE pd with real WAL
/// bytes, tmp/WALV3-OFFLOOP-FSYNC.md §8). The everysec bound is now "sync
/// *initiated* every 1s, durable typically ms later" — the same window the
/// AOF everysec writers provide.
pub(crate) fn sync_wal_v3(wal_v3: &mut Option<crate::persistence::wal_v3::segment::WalWriterV3>) {
    if let Some(wal) = wal_v3 {
        if let Err(e) = wal.request_sync() {
            tracing::error!("WAL v3 sync failed: {}", e);
        }
    }
}

/// MVCC sweep tick: prune committed treemap + sweep zombie intents + update RECL_* metrics.
///
/// Called on the 1s timer (same cadence as WAL fsync). Takes mutable access to the
/// per-shard VectorStore so it can reach the `TransactionManager` and, when the `graph`
/// feature is enabled, the `GraphStore` for graph intent cleanup.
///
/// ## What this does
///
/// 1. `mark_old_snapshots_killed(now, threshold)` — MA2: kill snapshots older than
///    `old_snapshot_threshold_secs`. Skipped when threshold == 0 (disabled).
/// 2. `prune_committed(margin)` — evicts `committed` entries below
///    `oldest_snapshot - margin`, freeing RoaringTreemap memory.
/// 3. `sweep_zombies_mut()` — removes any write intents whose owner txn_id is
///    neither active nor committed (leaked by dropped-future or panic paths).
/// 4. Updates five RECL_MVCC_* atomics so `INFO` picks them up immediately,
///    including `RECL_MVCC_OLDEST_SNAPSHOT_AGE_SECS` (wired here for MA2).
/// 5. MA1: samples total immutable segment count and updates `RECL_SEGMENT_STALL_ACTIVE`
///    via `segment_stall::update_segment_stall`. Read commands continue; only
///    foreground writes are blocked when the threshold is exceeded.
///
/// ## Safety
///
/// Must be called from the shard thread only. `VectorStore` is `!Send`; no lock
/// is held across this call.
pub(crate) fn run_mvcc_sweep(
    vector_store: &mut crate::vector::store::VectorStore,
    #[cfg(feature = "graph")] graph_store: &mut crate::graph::store::GraphStore,
    prune_margin: u64,
    max_unflushed_immutable_segments: u64,
    old_snapshot_threshold_secs: u64,
) {
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    use crate::command::info_reclamation::{
        RECL_MVCC_ACTIVE, RECL_MVCC_COMMITTED, RECL_MVCC_OLDEST_SNAPSHOT_AGE_SECS,
        RECL_MVCC_OLDEST_SNAPSHOT_LAG, RECL_MVCC_ZOMBIES_SWEPT_TOTAL,
    };

    let now = Instant::now();
    let mgr = vector_store.txn_manager_mut();

    // 1. MA2: mark snapshots older than threshold as killed so oldest_snapshot advances.
    let newly_killed = if old_snapshot_threshold_secs > 0 {
        let threshold = Duration::from_secs(old_snapshot_threshold_secs);
        let count = mgr.mark_old_snapshots_killed(now, threshold);
        if count > 0 {
            tracing::warn!(
                count,
                threshold_secs = old_snapshot_threshold_secs,
                "mvcc_sweep: killed {} snapshot(s) older than threshold",
                count
            );
        }
        count
    } else {
        0
    };

    // 2. Prune committed treemap.
    let pruned = mgr.prune_committed(prune_margin);

    // 3. Sweep zombie vector write intents.
    let swept_vec = mgr.sweep_zombies_mut();

    // 4. Sweep zombie graph write intents (feature-gated).
    #[cfg(feature = "graph")]
    let swept_graph = {
        // GraphStore owns its own LSN counter but shares the same transaction
        // lifecycle model. Sweep the graph intents via the MVCC manager.
        let _ = graph_store; // referenced for future graph-txn sweep wiring
        mgr.sweep_graph_zombies_mut()
    };
    #[cfg(not(feature = "graph"))]
    let swept_graph = 0usize;

    let total_swept = swept_vec + swept_graph;
    let _ = pruned; // used for debug; not emitted separately (merged into committed count)

    // 5. Update RECL_MVCC_* atomics.
    let committed_len = mgr.committed_count();
    let active_len = mgr.active_count() as u64;
    let oldest_snap = mgr.oldest_snapshot();
    let current_lsn = mgr.current_lsn();
    let lag = current_lsn.saturating_sub(oldest_snap);

    // MA2: oldest_snapshot_age measures age of oldest NON-killed snapshot.
    // Killed snapshots are excluded so the metric reflects real GC pressure.
    let age_secs = mgr.oldest_snapshot_age(now).map_or(0, |d| d.as_secs());

    RECL_MVCC_COMMITTED.store(committed_len, Ordering::Relaxed);
    RECL_MVCC_ACTIVE.store(active_len, Ordering::Relaxed);
    RECL_MVCC_OLDEST_SNAPSHOT_LAG.store(lag, Ordering::Relaxed);
    RECL_MVCC_OLDEST_SNAPSHOT_AGE_SECS.store(age_secs, Ordering::Relaxed);
    if total_swept > 0 {
        RECL_MVCC_ZOMBIES_SWEPT_TOTAL.fetch_add(total_swept as u64, Ordering::Relaxed);
    }

    // 6. MA1: update segment-stall bit based on current immutable segment count.
    let imm_count = vector_store.total_immutable_segment_count();
    crate::shard::segment_stall::update_segment_stall(imm_count, max_unflushed_immutable_segments);

    if total_swept > 0 || pruned > 0 || newly_killed > 0 {
        tracing::debug!(
            pruned,
            swept_vec,
            swept_graph,
            newly_killed,
            committed_remaining = committed_len,
            oldest_snapshot_lag = lag,
            oldest_snapshot_age_secs = age_secs,
            imm_count,
            "mvcc_sweep: pruned committed + swept zombies + killed old snapshots",
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::db::Database;
    use bytes::Bytes;
    use std::sync::atomic::Ordering;

    /// No-replica/no-AOF `record_reason_del` handles for `run_eviction`
    /// unit tests below — these tests exercise the eviction budget math
    /// only, not task #34's dual-plane emission (covered end-to-end by
    /// `tests/replication_planes.rs`).
    struct NoOpReasonDelHandles {
        wal_writer: Option<crate::persistence::wal_v3::segment::WalWriterV3>,
        repl_backlog: crate::replication::backlog::SharedBacklog,
        replica_txs: Vec<crate::shard::dispatch::ReplicaFanout>,
        repl_state: Option<crate::replication::state::OffsetHandle>,
    }

    impl NoOpReasonDelHandles {
        fn new() -> Self {
            Self {
                wal_writer: None,
                repl_backlog: std::sync::Arc::new(parking_lot::Mutex::new(None)),
                replica_txs: Vec::new(),
                repl_state: None,
            }
        }
    }

    /// A3 review fix (CRITICAL, both adversarial reviews): the shard-wide
    /// vector term must gate an AGGREGATE check (Σ all dbs' KV + vector),
    /// not be re-added to every logical db's independent check. The per-db
    /// duplication had two failure modes:
    ///  1. under-detection — KV spread across dbs where no single
    ///     `db_i + vector` crosses budget ⇒ nothing evicts while true
    ///     aggregate RSS overruns (the very bug A3 claimed to fix);
    ///  2. over-eviction — a vector term over budget independently drained
    ///     EVERY db on the shard (multi-tenant blast radius), instead of
    ///     evicting only until the aggregate is back under budget.
    ///
    /// This test pins both: two dbs each individually under budget with the
    /// vector term, aggregate over — eviction must fire (kills mode 1) and
    /// must stop once the aggregate is satisfied, leaving db 1 untouched
    /// (kills mode 2).
    #[test]
    fn test_run_eviction_gates_on_aggregate_not_per_db() {
        let dbs = vec![vec![Database::new(), Database::new()]];
        let (shared, mut inits) = ShardDatabases::new(dbs);
        crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(
            inits.remove(0),
        ));

        // ~256 KiB of KV per db (64 keys × 4 KiB values).
        for db_idx in 0..2 {
            crate::shard::slice::with_shard_db(db_idx, |db| {
                for i in 0..64u32 {
                    db.set_string(
                        Bytes::from(format!("db{db_idx}:key:{i}")),
                        Bytes::from(vec![b'v'; 4096]),
                    );
                }
            });
        }

        let rt = RuntimeConfig {
            maxmemory: 1024 * 1024, // 1 MiB budget, 1 shard
            num_shards: 1,
            maxmemory_policy: "allkeys-lru".to_string(),
            ..Default::default()
        };
        let runtime_config = Arc::new(parking_lot::RwLock::new(rt));

        // 700 KiB vector term: each db alone is ~256K + 700K < 1 MiB (the
        // per-db check would pass ⇒ old code evicts NOTHING), but the
        // aggregate ~512K + 700K > 1 MiB must evict.
        shared.store_memory_per_shard[0]
            .vector
            .store(700 * 1024, Ordering::Relaxed);
        let mut h = NoOpReasonDelHandles::new();
        run_eviction(
            &shared,
            0,
            &runtime_config,
            &mut h.wal_writer,
            &h.repl_backlog,
            &mut h.replica_txs,
            &h.repl_state,
            None,
            false,
            None,
        );

        let len0 = crate::shard::slice::with_shard_db(0, |db| db.len());
        let len1 = crate::shard::slice::with_shard_db(1, |db| db.len());
        assert!(
            len0 < 64,
            "aggregate over budget must evict even when no single db crosses it (db0 {len0})"
        );
        // The overage (~190 KiB) is smaller than db0's ~256 KiB, so eviction
        // must satisfy the aggregate within db0 and never touch db1.
        assert_eq!(
            len1, 64,
            "eviction must stop at the aggregate target, not drain sibling dbs"
        );
    }

    /// A3 (accounting spine): published vector resident bytes must count
    /// toward the background maxmemory eviction check. A vector-heavy shard
    /// whose KV alone is under budget previously never evicted — vector RAM
    /// was invisible to `--maxmemory`, so a pure-vector workload could drive
    /// RSS to OOM while eviction reported "under budget". RED until
    /// `run_eviction` adds the published vector term to the used total.
    ///
    /// Note on semantics: when the un-evictable vector term alone exceeds
    /// the budget, KV on the shard fully drains then errors OOM — the vector
    /// tier legitimately owns that memory (shared-budget semantics, same as
    /// one tenant's KV growth evicting siblings under allkeys-lru), and the
    /// pressure cascade (which CAN shrink vectors via offload) fires earlier
    /// at 0.85×budget. Per-db tenant isolation remains the per-db quota
    /// mechanism (WS5b), not --maxmemory.
    #[test]
    fn test_run_eviction_counts_vector_memory() {
        let dbs = vec![vec![Database::new()]];
        let (shared, mut inits) = ShardDatabases::new(dbs);
        crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(
            inits.remove(0),
        ));

        crate::shard::slice::with_shard_db(0, |db| {
            for i in 0..100u32 {
                db.set_string(Bytes::from(format!("key:{i}")), Bytes::from(vec![b'v'; 64]));
            }
        });

        let rt = RuntimeConfig {
            maxmemory: 1024 * 1024,
            num_shards: 1,
            maxmemory_policy: "allkeys-lru".to_string(),
            ..Default::default()
        };
        let runtime_config = Arc::new(parking_lot::RwLock::new(rt));

        // KV alone (~tens of KB) is far under the 1 MiB budget: no eviction.
        let mut h = NoOpReasonDelHandles::new();
        run_eviction(
            &shared,
            0,
            &runtime_config,
            &mut h.wal_writer,
            &h.repl_backlog,
            &mut h.replica_txs,
            &h.repl_state,
            None,
            false,
            None,
        );
        let before = crate::shard::slice::with_shard_db(0, |db| db.len());
        assert_eq!(before, 100, "KV under budget must not evict");

        // 2 MiB of published vector-segment memory pushes the shard over
        // budget: KV must now be evicted (vector memory is data-plane memory).
        shared.store_memory_per_shard[0]
            .vector
            .store(2 * 1024 * 1024, Ordering::Relaxed);
        run_eviction(
            &shared,
            0,
            &runtime_config,
            &mut h.wal_writer,
            &h.repl_backlog,
            &mut h.replica_txs,
            &h.repl_state,
            None,
            false,
            None,
        );
        let after = crate::shard::slice::with_shard_db(0, |db| db.len());
        assert!(
            after < before,
            "vector bytes over budget must trigger KV eviction ({after} vs {before})"
        );
    }

    /// RED before the task #45 fix (`run_eviction` always passed `None` for
    /// `spill`, no matter what the caller had available): the tick eviction
    /// path plain-dropped a Hash victim even though a `SpillContext` (a real
    /// `ShardManifest` + `shard_dir` + `next_file_id`, exactly what
    /// `persistence_tick::run_eviction_tick` builds when disk-offload is
    /// enabled and a durability backstop is configured) was available. This
    /// is the deterministic, non-racy reproduction of the
    /// `cold_collection_visibility` stable-ghost flake's root cause: a
    /// single call, single victim, no server process, no timing race between
    /// two eviction paths -- just "does `run_eviction` use the `SpillContext`
    /// it was handed."
    ///
    /// GREEN after the fix: the Hash victim is durably spilled (a `.mpf` file
    /// exists under `shard_dir/data/` and `db.cold_index` has a live entry
    /// for the key), NOT plain-dropped -- so a subsequent `EXISTS`/`HGETALL`
    /// can find it via `promote_cold_if_present` instead of returning a
    /// stable ghost (`EXISTS == 1`, empty read).
    #[test]
    fn test_run_eviction_spills_collection_victim_when_spill_context_given() {
        use crate::storage::eviction::SpillContext;
        use crate::storage::tiered::cold_index::ColdIndex;

        let dbs = vec![vec![Database::new()]];
        let (shared, mut inits) = ShardDatabases::new(dbs);
        crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(
            inits.remove(0),
        ));

        // A real Hash key -- the exact shape `tests/cold_collection_visibility.rs`
        // probes (HSET f1/v1 f2/v2) -- plus enough filler so the shard is
        // over its 1-byte-effective budget and `allkeys-lru` is forced to
        // pick it as the (only) victim.
        crate::shard::slice::with_shard_db(0, |db| {
            db.cold_index = Some(ColdIndex::new());
            let h = db.get_or_create_hash(b"probehash:0").unwrap();
            h.insert(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"));
            h.insert(Bytes::from_static(b"f2"), Bytes::from_static(b"v2"));
        });

        let rt = RuntimeConfig {
            maxmemory: 1, // force eviction of the one key present
            num_shards: 1,
            maxmemory_policy: "allkeys-lru".to_string(),
            ..Default::default()
        };
        let runtime_config = Arc::new(parking_lot::RwLock::new(rt));

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest =
            crate::persistence::manifest::ShardManifest::create(&manifest_path).unwrap();
        let mut next_file_id = 1u64;
        let mut ctx = SpillContext {
            shard_dir: &shard_dir,
            manifest: &mut manifest,
            next_file_id: &mut next_file_id,
            db_index: 0,
        };

        let mut h = NoOpReasonDelHandles::new();
        run_eviction(
            &shared,
            0,
            &runtime_config,
            &mut h.wal_writer,
            &h.repl_backlog,
            &mut h.replica_txs,
            &h.repl_state,
            None,
            false,
            Some(&mut ctx),
        );

        let (len, has_cold_entry) = crate::shard::slice::with_shard_db(0, |db| {
            (
                db.len(),
                db.cold_index
                    .as_ref()
                    .is_some_and(|ci| ci.lookup(b"probehash:0").is_some()),
            )
        });
        assert_eq!(len, 0, "the hash victim must be evicted from hot RAM");
        assert!(
            shard_dir.join("data").exists(),
            "P0 (task #45): the tick eviction path must durably spill a \
             collection victim to disk when a SpillContext is available, not \
             plain-drop it"
        );
        assert!(
            has_cold_entry,
            "P0 (task #45): the spilled hash must be registered in \
             ColdIndex, or EXISTS/HGETALL can never find it again -- a \
             stable ghost"
        );
    }
}
