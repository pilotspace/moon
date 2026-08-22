//! Background publishers: the ~1 Hz SSE metric feed (console feature) and the
//! per-subsystem `moon_memory_bytes` gauge updater.
//!
//! Split out of the former single-file `metrics_setup.rs` (moon#479, file-size
//! ceiling); cadence and gauge names are unchanged.

use std::sync::atomic::Ordering;

use metrics::gauge;

use crate::admin::metrics_setup::{
    METRICS_INITIALIZED, get_allocator_overhead_bytes, get_global_repl_state_arc,
    get_global_shard_databases, get_rss_bytes, update_rss_bytes,
};
// Only the console-gated SSE snapshot reads the raw connection gauge and the
// exact total-commands sum. Both imports must stay cfg'd: unconditionally
// importing them makes the default (no-console) build fail `unused_imports`.
#[cfg(feature = "console")]
use crate::admin::metrics_setup::{CONNECTED_CLIENTS, total_commands_sum};

// ── SSE metrics publisher (console feature) ────────────────────────────

/// Spawn a background task that publishes `MetricEvent` to the SSE
/// broadcast channel at ~1 Hz. Reads from existing atomic counters.
///
/// Must be called from within a tokio runtime context (the admin thread).
#[cfg(feature = "console")]
pub fn spawn_metrics_publisher() {
    use crate::admin::sse_stream::{MetricEvent, get_metrics_sender};

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(1000));
        let mut prev_ops: u64 = 0;
        let start = std::time::Instant::now();

        loop {
            interval.tick().await;

            let sender = match get_metrics_sender() {
                Some(s) => s,
                None => continue,
            };

            let total_ops = total_commands_sum();
            let ops_per_sec = total_ops.saturating_sub(prev_ops);
            prev_ops = total_ops;

            let event = MetricEvent {
                event: "server_stats",
                total_ops,
                ops_per_sec,
                total_memory: get_rss_bytes(),
                connected_clients: CONNECTED_CLIENTS.load(Ordering::Relaxed),
                uptime_seconds: start.elapsed().as_secs(),
                total_keys: 0,
            };

            // watch::send replaces the single stored value — no ring buffer,
            // no per-event allocation. Receivers see only the latest snapshot.
            let _ = sender.send(event);
        }
    });
}

// ── Per-subsystem memory gauge publisher ─────────────────────────────

/// Spawn a background task that updates the `moon_memory_bytes{kind=...}`
/// gauge every 15 seconds from the `resident_bytes()` accessors added in
/// Phase 190-01.
///
/// Must be called from within a tokio runtime context (the admin-http
/// thread). Requires `set_global_shard_databases()` to have been called
/// first — the function tolerates a missing handle by emitting 0 for all
/// subsystem kinds until the global is registered.
///
/// NOTE: This loop does NOT call `mallctl("epoch")` — see the documented
/// jemalloc leak at `get_rss_bytes()` (~1 MB / 20 s). `allocator_overhead`
/// is computed as `max(0, RSS − sum(other 6))`, the same formula MEMORY
/// DOCTOR uses.
pub fn spawn_moon_memory_publisher() {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));

        loop {
            interval.tick().await;

            if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
                continue;
            }

            update_moon_memory_bytes();
        }
    });
}

/// Collect per-subsystem resident bytes and emit all 7
/// `moon_memory_bytes{kind=...}` series plus `moon_rss_bytes`.
///
/// Called every 15 s by `spawn_moon_memory_publisher`. Lock-free: reads from
/// per-shard published atomics (C5 / M4). Figures lag at most one 100ms tick.
fn update_moon_memory_bytes() {
    use std::sync::atomic::Ordering;

    let rss = get_rss_bytes() as usize;

    let mut dashtable: usize = 0;
    let mut hnsw: usize = 0;
    let sealed: usize = 0; // combined into hnsw from vector atomic (C5)
    // K4 (kernel-m2-brief-2026-07-12 stage 2): text (FTS) resident bytes,
    // previously hard-coded 0 at the publish site.
    let mut text: usize = 0;
    let mut csr: usize = 0;
    let wal: usize = 0; // WalWriterV3 is stack-owned; not reachable here
    let mut backlog: usize = 0;
    let mut lua: usize = 0;
    // task #58 (LOW-2): per-shard PageCache resident buffer bytes.
    let mut pagecache: usize = 0;

    if let Some(shard_dbs) = get_global_shard_databases() {
        // KV memory: sum of per-shard published atomics. Lock-free.
        // C5 / M4: `read_memory_sum()` replaces per-shard `read_db(…)` locks.
        dashtable = shard_dbs.read_memory_sum();

        // Store memory: sum published per-shard vector/text/graph atomics.
        // Values are refreshed by each shard's 100ms tick (publish_store_memory).
        for mem in shard_dbs.store_memory_per_shard.iter() {
            hnsw += mem.vector.load(Ordering::Relaxed);
            text += mem.text.load(Ordering::Relaxed);
            // graph is cfg-gated at publish time; the atomic is always present.
            csr += mem.graph.load(Ordering::Relaxed);
            // C4 (wave-5 hygiene): Lua script-cache byte estimate, plus
            // (moon#506) the `mlua` interpreter heap that actually dominates
            // it — the gauge was previously a permanent ~48 bytes.
            lua += mem.lua.load(Ordering::Relaxed) + mem.lua_vm.load(Ordering::Relaxed);
            pagecache += mem.pagecache.load(Ordering::Relaxed);
        }
    }

    // Replication backlog via global state.
    if let Some(state) = get_global_repl_state_arc() {
        backlog = state.read().backlog_resident_bytes();
    }

    // task #58 (LOW-1): read the allocator-overhead figure sampled by shard
    // 0's 100ms tick (`persistence_tick::run_eviction_tick`) rather than
    // recomputing `rss - other_sum` here. This keeps INFO memory and
    // `/metrics` reading the SAME published number instead of two
    // independently-timed RSS snapshots drifting apart.
    let alloc_overhead = get_allocator_overhead_bytes();

    gauge!("moon_memory_bytes", "kind" => "dashtable").set(dashtable as f64);
    gauge!("moon_memory_bytes", "kind" => "hnsw").set(hnsw as f64);
    gauge!("moon_memory_bytes", "kind" => "text").set(text as f64);
    gauge!("moon_memory_bytes", "kind" => "csr").set(csr as f64);
    gauge!("moon_memory_bytes", "kind" => "wal").set(wal as f64);
    gauge!("moon_memory_bytes", "kind" => "sealed").set(sealed as f64);
    gauge!("moon_memory_bytes", "kind" => "replication_backlog").set(backlog as f64);
    gauge!("moon_memory_bytes", "kind" => "lua_scripts").set(lua as f64);
    gauge!("moon_memory_bytes", "kind" => "pagecache").set(pagecache as f64);
    gauge!("moon_memory_bytes", "kind" => "allocator_overhead").set(alloc_overhead as f64);

    // Task #56 (used_memory truthfulness) + finding #3 (parity delta): the
    // same logical ledger INFO's `used_memory` reports -- KV+ColdIndex +
    // vector/text/graph, PLUS the Lua script cache and replication backlog
    // (both already sampled above for their own `moon_memory_bytes{kind=...}`
    // series) to match real Redis's `used_memory` semantics -- published as
    // its own top-level gauge so `moon_used_memory_bytes / <maxmemory>` is a
    // meaningful alert expression. Still narrower than `moon_rss_bytes`,
    // which also carries allocator overhead and page cache (see
    // `logical_used_memory_bytes`'s doc comment for the full breakdown).
    gauge!("moon_used_memory_bytes").set((dashtable + hnsw + text + csr + lua + backlog) as f64);

    // moon#656: the KV cold tier's on-disk footprint. INFO answers "how big is
    // it right now" for a human at a prompt; an operator alerting on disk
    // growth needs a time series, so the same published numbers are emitted
    // here. Skipped entirely until a sweep has published — see
    // `ShardColdStats::published` for why a zero would be worse than absence.
    if let Some(shard_dbs) = get_global_shard_databases() {
        let cold = shard_dbs.read_cold_totals();
        if cold.published {
            gauge!("moon_cold_disk_bytes").set(cold.disk_bytes as f64);
            gauge!("moon_cold_keys").set(cold.keys as f64);
            gauge!("moon_cold_files", "state" => "referenced").set(cold.files_referenced as f64);
            gauge!("moon_cold_files", "state" => "dead")
                .set(cold.files.saturating_sub(cold.files_referenced) as f64);
            gauge!("moon_cold_files", "state" => "pending_unlink")
                .set(cold.files_pending_unlink as f64);
            gauge!("moon_memory_bytes", "kind" => "cold_index").set(cold.index_bytes as f64);
        }
    }

    // Update the existing RSS gauge in the same snapshot so the integration
    // test can compare moon_memory_bytes sum against moon_rss_bytes from the
    // same scrape (no drift between separate reads).
    update_rss_bytes(rss as u64);
}
