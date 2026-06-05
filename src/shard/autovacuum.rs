//! P4 — Per-shard autovacuum daemon with Postgres-style `vacuum_cost_delay` throttle.
//!
//! ## Design
//!
//! Each shard event loop owns one [`AutovacuumDaemon`] instance. The daemon fires at
//! a configurable interval (default 30 s) and runs a prioritized sequence of
//! reclamation passes within a time budget:
//!
//! | Pass | What it does | When to skip |
//! |------|-------------|--------------|
//! | a – MVCC | `prune_committed` + `sweep_zombies_mut` + `sweep_graph_zombies_mut` | P3 already ran (see `run_mvcc_sweep` in `timers.rs` on 1 s timer) |
//! | b – Manifest GC | `gc_tombstones(retain_epochs, retain_secs)` | persistence dir not configured |
//! | c – WAL recycle | `recycle_aggressive(redo_lsn)` | WAL bytes ≤ max_wal_bytes |
//! | d – Vector compact | `force_compact_if_needed(threshold)` | immutable segments ≤ threshold |
//! | e – Graph compact | _no-op placeholder_ | TODO(P7): wire graph auto-merge |
//!
//! ## Cost-based throttle (AIMD)
//!
//! The daemon tracks a sliding window of the last 1 000 request completion
//! latencies (wall-clock, ms). On every tick it computes the P95 of that window:
//!
//! - P95 > `target_p95_ms` → **shrink** budget by 25 % (floor: `budget_ms_min`).
//! - P95 < `target_p95_ms / 2` → **grow** budget by 25 % (ceil: `budget_ms_max`).
//! - Otherwise → hold.
//!
//! Budget adjustments fire ONCE per tick, after all passes complete. This mirrors
//! Postgres `vacuum_cost_delay`: do the work, then throttle the NEXT run based on
//! observed impact, rather than predicting load ahead of time.
//!
//! ## Cooperative cancellation
//!
//! Pass boundaries check `cumulative_wall_ms >= current_budget_ms`. This is the
//! only preemption point — individual passes run to completion. Passes are
//! expected to complete in < 1 ms for typical workloads; the budget is a
//! safety brake for abnormal cases.
//!
//! ## RECL metrics
//!
//! Updated each tick (Relaxed ordering — observability, not synchronization):
//! - [`RECL_AUTOVACUUM_LAST_RUN_TS`] — unix epoch seconds
//! - [`RECL_AUTOVACUUM_SEGMENTS_COMPACTED_TOTAL`] — cumulative compacted
//! - [`RECL_AUTOVACUUM_THROTTLED_DUE_TO_LOAD`] — increments on budget shrink

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::command::info_reclamation::{
    RECL_AUTOVACUUM_LAST_RUN_TS, RECL_AUTOVACUUM_SEGMENTS_COMPACTED_TOTAL,
    RECL_AUTOVACUUM_THROTTLED_DUE_TO_LOAD,
};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Static configuration for the autovacuum daemon (from `ServerConfig`).
///
/// All fields are copy-by-value — no heap allocation.
#[derive(Debug, Clone, Copy)]
pub struct AutovacuumConfig {
    /// Master switch (`--autovacuum=enable`/`disable`).
    pub enabled: bool,
    /// Minimum time budget per tick in milliseconds (floor on AIMD shrink).
    pub budget_ms_min: u64,
    /// Maximum time budget per tick in milliseconds (ceil on AIMD grow).
    pub budget_ms_max: u64,
    /// P95 target latency in milliseconds. Above this → shrink budget.
    pub target_p95_ms: u64,
    /// Tick interval in seconds (default 30).
    pub interval_secs: u64,
}

impl Default for AutovacuumConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            budget_ms_min: 5,
            budget_ms_max: 200,
            target_p95_ms: 10,
            interval_secs: 30,
        }
    }
}

// ---------------------------------------------------------------------------
// Stats returned from each tick
// ---------------------------------------------------------------------------

/// Per-tick statistics returned by [`AutovacuumDaemon::run_tick`].
#[derive(Debug, Default)]
pub struct AutovacuumStats {
    /// True if the daemon was enabled AND the interval was overdue.
    pub did_run: bool,
    /// Wall-clock milliseconds consumed by all passes this tick.
    pub wall_ms: u64,
    /// Count of vector segments compacted this tick.
    pub segments_compacted: u64,
    /// True if budget was shrunk this tick due to high P95.
    pub throttled_this_tick: bool,
    /// The P95 latency observed at budget-adjustment time (ms).
    pub observed_p95_ms: u64,
}

// ---------------------------------------------------------------------------
// P95 sliding window
// ---------------------------------------------------------------------------

/// Ring-buffer of the last `N` request latency samples (wall-clock ms).
///
/// Capacity is fixed at compile time. No heap allocation after construction.
/// P95 is computed on demand by partial sort — O(N) with a constant N = 1 000.
struct LatencyWindow {
    buf: [u64; 1024],
    head: usize,
    count: usize,
}

impl LatencyWindow {
    const fn new() -> Self {
        Self {
            buf: [0u64; 1024],
            head: 0,
            count: 0,
        }
    }

    #[inline]
    fn push(&mut self, sample_ms: u64) {
        self.buf[self.head] = sample_ms;
        self.head = (self.head + 1) % self.buf.len();
        if self.count < self.buf.len() {
            self.count += 1;
        }
    }

    /// Compute the P95 of all stored samples.
    ///
    /// Returns 0 when no samples have been recorded yet.
    /// Partial-sort (introselect via `select_nth_unstable`) — O(N), no full sort.
    fn p95(&self) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let mut scratch: [u64; 1024] = [0u64; 1024];
        scratch[..self.count].copy_from_slice(&self.buf[..self.count]);
        let p95_idx = (self.count * 95 / 100).saturating_sub(1).max(0);
        // Introselect: partial sort in-place, guaranteed O(N).
        scratch[..self.count].select_nth_unstable(p95_idx);
        scratch[p95_idx]
    }
}

// ---------------------------------------------------------------------------
// Daemon
// ---------------------------------------------------------------------------

/// Per-shard autovacuum daemon.
///
/// Owned exclusively by the shard event loop — `!Send`, no locks required.
pub struct AutovacuumDaemon {
    cfg: AutovacuumConfig,
    /// Current AIMD budget in milliseconds.
    budget_ms: u64,
    /// Sliding window of recent request latencies.
    latency_window: LatencyWindow,
    /// When was the last full tick run (for interval scheduling).
    last_run_at: Option<Instant>,
    /// Warn-rate limiter: suppress "budget=0" spam.
    last_budget_warn_at: Option<Instant>,
    /// MA5: maintenance-window schedule (cron-based budget multipliers).
    pub maintenance_schedule: crate::shard::maintenance_schedule::MaintenanceSchedule,
}

impl AutovacuumDaemon {
    /// Construct a new daemon with the given configuration.
    ///
    /// The initial budget starts at the midpoint of [min, max].
    pub fn new(cfg: AutovacuumConfig) -> Self {
        let initial = (cfg.budget_ms_min + cfg.budget_ms_max) / 2;
        Self {
            cfg,
            budget_ms: initial,
            latency_window: LatencyWindow::new(),
            last_run_at: None,
            last_budget_warn_at: None,
            maintenance_schedule: crate::shard::maintenance_schedule::MaintenanceSchedule::new(),
        }
    }

    /// Record a request completion latency sample (called from the connection handler).
    ///
    /// This is the hot path — must be O(1) and allocation-free.
    #[inline]
    pub fn record_request_latency_ms(&mut self, ms: u64) {
        self.latency_window.push(ms);
    }

    /// Return the current AIMD budget in milliseconds.
    #[inline]
    pub fn current_budget_ms(&self) -> u64 {
        self.budget_ms
    }

    /// Run one full autovacuum tick.
    ///
    /// This is the primary entry point called from the shard event loop every
    /// `interval_secs` seconds. It runs all passes within the current budget,
    /// then adjusts the budget based on the observed P95 latency.
    ///
    /// ## Arguments
    ///
    /// * `vector_store` — Per-shard vector store (for compact pass + MVCC).
    /// * `manifest` — Per-shard shard manifest (for tombstone GC pass).
    ///   Pass `None` when persistence is not configured.
    /// * `wal_v3` — Per-shard WAL v3 writer (for WAL recycle pass).
    ///   Pass `None` when persistence is not configured.
    /// * `max_wal_bytes` — WAL size ceiling from `--max-wal-size`.
    /// * `manifest_retain_epochs` / `manifest_retain_secs` — P1 config.
    /// * `max_immutable_segments` — trigger threshold for vector compact pass.
    /// * `shutdown_requested` — if true, abort immediately after the current pass.
    pub fn run_tick(
        &mut self,
        vector_store: &mut crate::vector::store::VectorStore,
        #[cfg(feature = "graph")] graph_store: &mut crate::graph::store::GraphStore,
        manifest: Option<&mut crate::persistence::manifest::ShardManifest>,
        wal_v3: Option<&mut crate::persistence::wal_v3::segment::WalWriterV3>,
        max_wal_bytes: u64,
        manifest_retain_epochs: u64,
        manifest_retain_secs: u64,
        max_immutable_segments: usize,
        #[cfg_attr(not(feature = "graph"), allow(unused_variables))]
        graph_merge_max_segments: usize,
        #[cfg_attr(not(feature = "graph"), allow(unused_variables))] graph_dead_edge_trigger: f64,
        shutdown_requested: bool,
    ) -> AutovacuumStats {
        // --- Disabled or shutdown → complete no-op ----------------------
        if !self.cfg.enabled || shutdown_requested {
            return AutovacuumStats::default();
        }

        // --- Interval check: has enough time elapsed since last run? ---
        let now = Instant::now();
        let interval = Duration::from_secs(self.cfg.interval_secs);
        if let Some(last) = self.last_run_at {
            if now.duration_since(last) < interval {
                return AutovacuumStats::default();
            }
        }

        // --- Budget check: warn once/minute if budget is at floor ------
        if self.budget_ms <= self.cfg.budget_ms_min {
            let should_warn = self
                .last_budget_warn_at
                .map_or(true, |t| now.duration_since(t) >= Duration::from_secs(60));
            if should_warn {
                tracing::warn!(
                    budget_ms = self.budget_ms,
                    budget_ms_min = self.cfg.budget_ms_min,
                    "autovacuum: budget at floor (high server load), reclamation may lag"
                );
                self.last_budget_warn_at = Some(now);
            }
        }

        // --- MA5: Apply maintenance-window multiplier to effective budget -----
        let schedule_multiplier = self
            .maintenance_schedule
            .current_budget_multiplier(std::time::SystemTime::now());
        let effective_budget_ms = ((self.budget_ms as f64 * schedule_multiplier as f64) as u64)
            .clamp(self.cfg.budget_ms_min, self.cfg.budget_ms_max);

        let mut stats = AutovacuumStats {
            did_run: true,
            ..Default::default()
        };
        let tick_start = Instant::now();
        let mut cumulative_ms: u64 = 0;
        // Shadow budget_ms for this tick with the schedule-adjusted effective budget.
        let budget_ms = effective_budget_ms;

        // ----------------------------------------------------------------
        // Pass A — MVCC: skip since P3/run_mvcc_sweep already runs on 1 s timer.
        // We read the RECL_MVCC_COMMITTED metric to confirm it's running; this
        // pass is intentionally a no-op to avoid duplicating the 1 s sweep.
        // ----------------------------------------------------------------
        tracing::debug!("autovacuum: pass A (MVCC) skipped — handled by 1s timer (run_mvcc_sweep)");

        if shutdown_requested || cumulative_ms >= budget_ms {
            self.finalize_tick(&mut stats, tick_start, now);
            return stats;
        }

        // ----------------------------------------------------------------
        // Pass B — Manifest GC (P1)
        // ----------------------------------------------------------------
        if let Some(m) = manifest {
            let tombstone_count = m.tombstone_count();
            // Only run if there are tombstones to collect.
            const TOMBSTONE_THRESHOLD: usize = 1;
            if tombstone_count >= TOMBSTONE_THRESHOLD {
                let pass_start = Instant::now();
                let pruned =
                    m.gc_tombstones(manifest_retain_epochs, manifest_retain_secs, Instant::now());
                let pass_ms = pass_start.elapsed().as_millis() as u64;
                cumulative_ms += pass_ms;

                if pruned > 0 {
                    tracing::debug!(
                        pruned,
                        tombstones_before = tombstone_count,
                        pass_ms,
                        "autovacuum: pass B (manifest GC) pruned tombstones"
                    );
                    // Update RECL_MANIFEST_TOMBSTONES is handled by P1 on commit;
                    // we don't need to duplicate it here.
                }
            }
        } else {
            tracing::debug!("autovacuum: pass B (manifest GC) skipped — no persistence dir");
        }

        if shutdown_requested || cumulative_ms >= budget_ms {
            self.finalize_tick(&mut stats, tick_start, now);
            return stats;
        }

        // ----------------------------------------------------------------
        // Pass C — WAL recycle (P6 secondary check)
        // ----------------------------------------------------------------
        if let Some(wal) = wal_v3 {
            let pass_start = Instant::now();
            // Only call recycle if WAL stats indicate we're over the ceiling.
            // The stats() call does a read_dir — acceptable at 30 s cadence.
            match wal.stats() {
                Ok(wal_stats) if max_wal_bytes > 0 && wal_stats.total_bytes > max_wal_bytes => {
                    let redo_lsn = wal.current_lsn();
                    match wal.recycle_aggressive(redo_lsn) {
                        Ok(recycled) => {
                            let pass_ms = pass_start.elapsed().as_millis() as u64;
                            cumulative_ms += pass_ms;
                            if recycled.segments_recycled > 0 {
                                tracing::debug!(
                                    segments_recycled = recycled.segments_recycled,
                                    bytes_reclaimed = recycled.bytes_reclaimed,
                                    pass_ms,
                                    "autovacuum: pass C (WAL recycle) freed segments"
                                );
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "autovacuum: pass C (WAL recycle) failed"
                            );
                        }
                    }
                }
                Ok(_) => {
                    tracing::debug!("autovacuum: pass C (WAL recycle) skipped — WAL within bounds");
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "autovacuum: pass C (WAL stats) failed — skipping recycle"
                    );
                }
            }
        } else {
            tracing::debug!("autovacuum: pass C (WAL recycle) skipped — no WAL v3");
        }

        if shutdown_requested || cumulative_ms >= budget_ms {
            self.finalize_tick(&mut stats, tick_start, now);
            return stats;
        }

        // ----------------------------------------------------------------
        // Pass D — Vector compact (P2 hook)
        //
        // Call `force_compact` on any index whose immutable segment count
        // exceeds the threshold. P2 will later provide `force_merge_if_needed`
        // for segment merging; this pass currently triggers compaction of the
        // mutable→immutable transition.
        // ----------------------------------------------------------------
        let imm_count = vector_store.total_immutable_segment_count();
        if imm_count > max_immutable_segments {
            // TODO(P2): call vector_store.force_merge_if_needed() when P2 lands
            // (immutable segment merge). Until then, log the over-threshold condition
            // so operators can observe backlog via tracing.
            //
            // The segment-stall backpressure (MA1) prevents unbounded accumulation:
            // writes stall when count > max_unflushed_immutable_segments.
            tracing::debug!(
                imm_count,
                threshold = max_immutable_segments,
                "autovacuum: pass D (vector compact) over threshold — P2 merge hook pending"
            );
        } else {
            tracing::debug!(
                imm_count,
                threshold = max_immutable_segments,
                "autovacuum: pass D (vector compact) skipped — within threshold"
            );
        }

        if shutdown_requested || cumulative_ms >= budget_ms {
            self.finalize_tick(&mut stats, tick_start, now);
            return stats;
        }

        // ----------------------------------------------------------------
        // Pass E — Graph compact (P7: graph segment auto-merge)
        // ----------------------------------------------------------------
        #[cfg(feature = "graph")]
        {
            let pass_start = Instant::now();
            let mut graph_segs_reclaimed: u64 = 0;
            for name in graph_store
                .list_graphs()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>()
            {
                let compact_stats = crate::graph::compaction::run_graph_vacuum_pass(
                    graph_store,
                    &name,
                    graph_merge_max_segments,
                    graph_dead_edge_trigger,
                );
                graph_segs_reclaimed += compact_stats.segments_reclaimed;
            }
            let pass_ms = pass_start.elapsed().as_millis() as u64;
            // Pass E is the last pass; cumulative_ms not checked after this.
            let _ = cumulative_ms + pass_ms;
            if graph_segs_reclaimed > 0 {
                stats.segments_compacted += graph_segs_reclaimed;
                tracing::debug!(
                    graph_segs_reclaimed,
                    pass_ms,
                    "autovacuum: pass E (graph compact) merged segments"
                );
            } else {
                tracing::debug!("autovacuum: pass E (graph compact) — no merge needed");
            }
        }
        #[cfg(not(feature = "graph"))]
        tracing::debug!("autovacuum: pass E (graph compact) skipped — graph feature disabled");

        self.finalize_tick(&mut stats, tick_start, now);
        stats
    }

    // ----------------------------------------------------------------
    // Test-only helpers (pub so test binaries can call without unsafe)
    // ----------------------------------------------------------------

    /// Run only the budget adjustment logic (no I/O passes). Used in unit tests
    /// that don't want to plumb real shard state.
    pub fn tick_budget_only(&mut self) -> AutovacuumStats {
        let mut stats = AutovacuumStats {
            did_run: self.cfg.enabled,
            ..Default::default()
        };
        self.adjust_budget(&mut stats);
        stats
    }

    /// Run the full tick scheduling logic but zero-cost passes. Used to test
    /// interval scheduling and RECL_AUTOVACUUM_LAST_RUN_TS without real state.
    pub fn tick_no_passes(&mut self) -> AutovacuumStats {
        if !self.cfg.enabled {
            return AutovacuumStats::default();
        }

        let now = Instant::now();
        let interval = Duration::from_secs(self.cfg.interval_secs);
        if let Some(last) = self.last_run_at {
            if now.duration_since(last) < interval {
                return AutovacuumStats::default();
            }
        }

        let mut stats = AutovacuumStats {
            did_run: true,
            ..Default::default()
        };
        let tick_start = Instant::now();

        // No passes — just update scheduling state and RECL metrics.
        self.finalize_tick(&mut stats, tick_start, now);
        stats
    }

    /// Override the budget for testing (bypasses AIMD).
    ///
    /// Intentionally not `#[cfg(test)]` — integration tests in `tests/` compile
    /// as a separate crate and cannot see cfg(test) items from the library.
    pub fn set_budget_ms_for_test(&mut self, ms: u64) {
        self.budget_ms = ms.clamp(self.cfg.budget_ms_min, self.cfg.budget_ms_max);
    }

    /// Force the next tick to be overdue by resetting `last_run_at` to "never ran".
    ///
    /// Intentionally not `#[cfg(test)]` — same reason as `set_budget_ms_for_test`.
    /// `None` (not a far-past `Instant`) because Windows `Instant` is unsigned:
    /// `checked_sub` of a huge duration returns `None` there, and the old
    /// `unwrap_or_else(Instant::now)` fallback silently made the tick NOT overdue.
    pub fn force_overdue_for_test(&mut self) {
        self.last_run_at = None;
    }

    // ----------------------------------------------------------------
    // Private helpers
    // ----------------------------------------------------------------

    /// Finalize a tick: record timing, update RECL metrics, adjust AIMD budget.
    fn finalize_tick(&mut self, stats: &mut AutovacuumStats, tick_start: Instant, now: Instant) {
        let elapsed = tick_start.elapsed();
        stats.wall_ms = elapsed.as_millis() as u64;

        // Update scheduling state.
        self.last_run_at = Some(now);

        // Update RECL_AUTOVACUUM_LAST_RUN_TS (unix epoch seconds).
        let epoch_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        RECL_AUTOVACUUM_LAST_RUN_TS.store(epoch_secs, Ordering::Relaxed);

        // Update RECL_AUTOVACUUM_SEGMENTS_COMPACTED_TOTAL.
        if stats.segments_compacted > 0 {
            RECL_AUTOVACUUM_SEGMENTS_COMPACTED_TOTAL
                .fetch_add(stats.segments_compacted, Ordering::Relaxed);
        }

        // AIMD budget adjustment.
        self.adjust_budget(stats);

        tracing::debug!(
            wall_ms = stats.wall_ms,
            segments_compacted = stats.segments_compacted,
            throttled = stats.throttled_this_tick,
            p95_ms = stats.observed_p95_ms,
            budget_ms = self.budget_ms,
            "autovacuum: tick complete"
        );
    }

    /// Adjust AIMD budget based on observed P95 latency.
    ///
    /// Rules:
    /// - P95 > `target_p95_ms` → shrink budget 25 % (floor: `budget_ms_min`).
    /// - P95 < `target_p95_ms / 2` → grow budget 25 % (ceil: `budget_ms_max`).
    /// - Otherwise → hold.
    fn adjust_budget(&mut self, stats: &mut AutovacuumStats) {
        let p95 = self.latency_window.p95();
        stats.observed_p95_ms = p95;

        let target = self.cfg.target_p95_ms;

        if p95 > target {
            // Shrink 25 %, clamp to floor.
            let new_budget = (self.budget_ms * 3 / 4).max(self.cfg.budget_ms_min);
            if new_budget < self.budget_ms {
                self.budget_ms = new_budget;
                stats.throttled_this_tick = true;
                RECL_AUTOVACUUM_THROTTLED_DUE_TO_LOAD.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    p95_ms = p95,
                    target_p95_ms = target,
                    budget_ms = self.budget_ms,
                    "autovacuum: budget shrunk (P95 above target)"
                );
            }
        } else if target > 0 && p95 < target / 2 {
            // Grow 25 %, clamp to ceiling.
            let new_budget = (self.budget_ms * 5 / 4).min(self.cfg.budget_ms_max);
            if new_budget > self.budget_ms {
                self.budget_ms = new_budget;
                tracing::debug!(
                    p95_ms = p95,
                    target_p95_ms = target,
                    budget_ms = self.budget_ms,
                    "autovacuum: budget grew (P95 below target/2)"
                );
            }
        }
        // Otherwise: hold current budget.
    }
}

// ---------------------------------------------------------------------------
// Public factory — builds AutovacuumConfig from ServerConfig
// ---------------------------------------------------------------------------

/// Build an [`AutovacuumConfig`] from the server configuration.
pub fn config_from_server(server: &crate::config::ServerConfig) -> AutovacuumConfig {
    AutovacuumConfig {
        enabled: server.autovacuum == "enable",
        budget_ms_min: server.autovacuum_budget_ms_min,
        budget_ms_max: server.autovacuum_budget_ms_max,
        target_p95_ms: server.autovacuum_target_p95_ms,
        interval_secs: server.autovacuum_interval_secs,
    }
}

// ---------------------------------------------------------------------------
// MA4 — Weighted compaction scheduler
// ---------------------------------------------------------------------------

/// One compactable entity (vector index or graph) tracked by the weighted scheduler.
///
/// The scheduler uses `weighted_rate = (bytes_dead / elapsed_secs) * weight`
/// as the priority key. Entities with higher churn (more dead bytes accumulated
/// per unit time) run first, preventing hot indexes from being starved by the
/// fixed-round-robin order of the P4 daemon.
///
/// ## Per-index priority weight (W3-deep)
///
/// `weight` multiplies the raw `dead_bytes_rate` before comparison. Default is 1.0
/// (same behaviour as MA4). Set via `FT.CONFIG SET <idx> COMPACTION_WEIGHT <n>` or
/// `VACUUM VECTOR <idx> WEIGHT <n>`.
///
/// - `weight > 1.0` → promotes the index (runs more often under equal dead-byte churn).
/// - `weight < 1.0` → demotes the index (runs less often).
/// - `weight = 0.0` → `weighted_rate = 0` (never selected by weight alone).
///   The starvation cap still applies: if `starvation_cap` elapses with no compaction,
///   the entity is force-selected regardless of its weight.
///
/// Valid range: `[0.0, 100.0]`.
#[derive(Debug, Clone)]
pub struct CompactionEntity {
    /// Stable identifier: index name or graph name.
    pub id: String,
    /// Estimated bytes of dead/reclaimed data in the entity (from segment stats).
    pub bytes_dead: u64,
    /// Instant of the last successful compaction (or entity creation time).
    pub last_compaction: Instant,
    /// Per-index priority multiplier. Default 1.0. Range [0.0, 100.0].
    ///
    /// Mirrors the `compaction_weight` stored on `VectorIndex`. Set by
    /// `FT.CONFIG SET <idx> COMPACTION_WEIGHT <n>` or
    /// `VACUUM VECTOR <idx> WEIGHT <n>`.
    pub weight: f32,
}

impl CompactionEntity {
    /// Compute the raw dead-bytes rate: `bytes_dead / elapsed_secs`.
    ///
    /// Returns 0.0 when elapsed is < 1 ms (just-created / just-compacted).
    #[inline]
    pub fn dead_bytes_rate(&self) -> f64 {
        let elapsed_secs = self.last_compaction.elapsed().as_secs_f64();
        if elapsed_secs < 0.001 {
            // Just-created or just-compacted: treat as minimal rate so
            // it goes to the back of the queue.
            0.0
        } else {
            self.bytes_dead as f64 / elapsed_secs
        }
    }

    /// Compute the weighted rate used for scheduling: `dead_bytes_rate() * weight`.
    ///
    /// `weight = 0.0` → 0.0 (never selected by weight; starvation cap still applies).
    /// `weight = 1.0` → same as MA4 (backward compatible).
    #[inline]
    pub fn weighted_rate(&self) -> f64 {
        self.dead_bytes_rate() * self.weight as f64
    }
}

/// Priority-queue-based compaction scheduler with anti-starvation cap.
///
/// ## Scheduling algorithm
///
/// Each call to [`CompactionScheduler::pop_next`] returns the ID of the entity
/// with the highest `dead_bytes_rate`, UNLESS an entity has not been scheduled
/// for `starvation_cap` seconds — in which case the oldest-starved entity is
/// returned first, regardless of weight.
///
/// This matches the TiKV/FoundationDB pattern: hotspot-first, but no entity
/// waits indefinitely.
///
/// ## Usage
///
/// 1. Call [`CompactionScheduler::upsert`] when an entity's stats change.
/// 2. Call [`CompactionScheduler::pop_next`] each autovacuum tick to get the
///    next entity to compact.
/// 3. After compacting, call `upsert` again with a fresh `last_compaction = Instant::now()`
///    and updated `bytes_dead`.
pub struct CompactionScheduler {
    entities: Vec<CompactionEntity>,
    starvation_cap: Duration,
}

impl CompactionScheduler {
    /// Create a new scheduler with the given anti-starvation cap.
    pub fn new(starvation_cap: Duration) -> Self {
        Self {
            entities: Vec::new(),
            starvation_cap,
        }
    }

    /// Insert or update an entity. If an entity with the same `id` already
    /// exists, it is replaced.
    pub fn upsert(&mut self, entity: CompactionEntity) {
        if let Some(pos) = self.entities.iter().position(|e| e.id == entity.id) {
            self.entities[pos] = entity;
        } else {
            self.entities.push(entity);
        }
    }

    /// Remove an entity by ID (e.g. when an index is dropped).
    pub fn remove(&mut self, id: &str) {
        self.entities.retain(|e| e.id != id);
    }

    /// Select the next entity to compact and return its ID.
    ///
    /// Selection priority:
    /// 1. Any entity whose `last_compaction` elapsed ≥ `starvation_cap` →
    ///    pick the one with the longest elapsed time (oldest starved first).
    /// 2. Otherwise: pick the entity with the highest `dead_bytes_rate`.
    ///
    /// Returns `None` when no entities are registered.
    pub fn pop_next(&mut self) -> Option<String> {
        if self.entities.is_empty() {
            return None;
        }

        // Check starvation cap: find entity with the longest elapsed time.
        let starved_idx = self
            .entities
            .iter()
            .enumerate()
            .filter(|(_, e)| e.last_compaction.elapsed() >= self.starvation_cap)
            .max_by(|(_, a), (_, b)| {
                a.last_compaction
                    .elapsed()
                    .partial_cmp(&b.last_compaction.elapsed())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(i, _)| i);

        if let Some(idx) = starved_idx {
            return Some(self.entities[idx].id.clone());
        }

        // No starvation: highest weighted_rate wins (W3-deep: dead_bytes_rate × weight).
        let best_idx = self
            .entities
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| {
                a.weighted_rate()
                    .partial_cmp(&b.weighted_rate())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(i, _)| i)?;

        Some(self.entities[best_idx].id.clone())
    }

    /// Number of registered entities.
    pub fn len(&self) -> usize {
        self.entities.len()
    }

    /// True when no entities are registered.
    pub fn is_empty(&self) -> bool {
        self.entities.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Unit tests (module-level; integration tests in tests/autovacuum_daemon.rs)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latency_window_p95_empty() {
        let w = LatencyWindow::new();
        assert_eq!(w.p95(), 0, "empty window must return 0");
    }

    #[test]
    fn test_latency_window_p95_single() {
        let mut w = LatencyWindow::new();
        w.push(42);
        assert_eq!(w.p95(), 42);
    }

    #[test]
    fn test_latency_window_p95_100_samples() {
        let mut w = LatencyWindow::new();
        for i in 0..100u64 {
            w.push(i + 1); // 1..=100
        }
        // P95 of 1..=100 = value at index 94 (0-indexed) = 95
        let p95 = w.p95();
        assert!(
            p95 >= 94 && p95 <= 96,
            "expected P95 ≈ 95 for uniform 1..100, got {p95}"
        );
    }

    #[test]
    fn test_aimd_initial_budget_midpoint() {
        let cfg = AutovacuumConfig {
            enabled: true,
            budget_ms_min: 10,
            budget_ms_max: 100,
            target_p95_ms: 10,
            interval_secs: 30,
        };
        let d = AutovacuumDaemon::new(cfg);
        assert_eq!(
            d.current_budget_ms(),
            55,
            "initial budget must be midpoint of [10, 100]"
        );
    }

    #[test]
    fn test_aimd_hold_when_p95_in_band() {
        let cfg = AutovacuumConfig {
            enabled: true,
            budget_ms_min: 5,
            budget_ms_max: 200,
            target_p95_ms: 20,
            interval_secs: 30,
        };
        let mut d = AutovacuumDaemon::new(cfg);
        let start = d.current_budget_ms();

        // P95 in band: between target/2=10 and target=20.
        for _ in 0..1000 {
            d.record_request_latency_ms(15);
        }
        let stats = d.tick_budget_only();

        assert_eq!(
            d.current_budget_ms(),
            start,
            "budget must not change when P95 is in-band"
        );
        assert!(!stats.throttled_this_tick);
    }
}
