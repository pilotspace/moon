//! Process-global handles the INFO / MEMORY paths read from off-shard:
//! replication state, the shard database set, and the slowlog.
//!
//! Split out of the former single-file `metrics_setup.rs` (moon#479, file-size
//! ceiling); unchanged.

use std::sync::atomic::Ordering;

use crate::admin::metrics_setup::record_replication_lag;

// ── Global replication state (for INFO) ────────────────────────────────

static GLOBAL_REPL_STATE: once_cell::sync::OnceCell<
    std::sync::Arc<parking_lot::RwLock<crate::replication::state::ReplicationState>>,
> = once_cell::sync::OnceCell::new();

/// Register the global replication state for INFO queries.
pub fn set_global_repl_state(
    state: std::sync::Arc<parking_lot::RwLock<crate::replication::state::ReplicationState>>,
) {
    let _ = GLOBAL_REPL_STATE.set(state);
}

/// Get the raw global replication state Arc (for MEMORY DOCTOR backlog query).
/// Returns None before replication is initialized.
pub fn get_global_repl_state_arc()
-> Option<&'static std::sync::Arc<parking_lot::RwLock<crate::replication::state::ReplicationState>>>
{
    GLOBAL_REPL_STATE.get()
}

/// Get replication info for INFO command: (role, connected_slaves, master_repl_offset, repl_id).
/// Also updates the Prometheus replication lag gauge as a side-effect.
pub fn get_replication_info() -> (&'static str, usize, u64, String) {
    if let Some(state) = GLOBAL_REPL_STATE.get() {
        let guard = state.read();
        let role = match &guard.role {
            crate::replication::state::ReplicationRole::Master => "master",
            crate::replication::state::ReplicationRole::Replica { .. } => "slave",
        };
        let slaves = guard.replicas.len();
        let offset = guard.master_repl_offset.load(Ordering::Relaxed);
        let repl_id = guard.repl_id.clone();
        // Update Prometheus lag gauge: max lag across all replicas.
        if !guard.replicas.is_empty() {
            let max_lag_bytes = guard
                .replicas
                .iter()
                .map(|r| {
                    let ack: u64 = r
                        .ack_offsets
                        .iter()
                        .map(|a| a.load(Ordering::Relaxed))
                        .sum();
                    offset.saturating_sub(ack)
                })
                .max()
                .unwrap_or(0);
            record_replication_lag(max_lag_bytes, 0);
        }
        return (role, slaves, offset, repl_id);
    }
    ("master", 0, 0, "0".repeat(40))
}

// ── Global ShardDatabases (for MEMORY DOCTOR / Prometheus per-kind) ───

static GLOBAL_SHARD_DBS: once_cell::sync::OnceCell<
    std::sync::Weak<crate::shard::shared_databases::ShardDatabases>,
> = once_cell::sync::OnceCell::new();

/// Register the global ShardDatabases handle for admin commands.
/// Called once from main after ShardDatabases::new().
pub fn set_global_shard_databases(
    dbs: &std::sync::Arc<crate::shard::shared_databases::ShardDatabases>,
) {
    let _ = GLOBAL_SHARD_DBS.set(std::sync::Arc::downgrade(dbs));
}

/// Get the global ShardDatabases handle (returns None before server init
/// or after shutdown when the Arc has been dropped).
pub fn get_global_shard_databases()
-> Option<std::sync::Arc<crate::shard::shared_databases::ShardDatabases>> {
    GLOBAL_SHARD_DBS.get().and_then(|w| w.upgrade())
}

/// The instance-wide logical memory ledger reported as `INFO`'s
/// `used_memory` field and the `moon_used_memory_bytes` gauge.
///
/// Task #56 (used_memory truthfulness) + adversarial-review finding #3
/// (parity delta): this is NOT process RSS -- RSS also contains the binary
/// image, thread stacks, allocator arena fragmentation, and mmap'd
/// page-cache frames for cold-tier reads, none of which real Redis counts
/// in `used_memory` either. Reporting RSS as `used_memory` made every
/// disk-offload deployment look permanently over-budget (RSS-vs-ledger gap
/// of 150-500MB+ was misread as a leak in the G2 acceptance run) even when
/// the eviction system was correctly holding the real, gated ledger under
/// the cap. `used_memory_rss` / `used_memory_peak` still report the true
/// OS-level footprint alongside this field -- see `allocator_overhead_bytes`
/// and `pagecache_bytes` for the two largest components still legitimately
/// excluded, and `MEMORY DOCTOR` for the full per-subsystem breakdown.
///
/// This figure is DELIBERATELY WIDER than
/// `ShardDatabases::recompute_elastic_budget` (the formula `--maxmemory`
/// eviction actually gates on: KV DashTable + its ColdIndex overhead, plus
/// vector/text/graph resident bytes). It ALSO adds the Lua script cache and
/// the replication backlog ring, matching real Redis's `used_memory`
/// semantics: Redis's `used_memory` is "total allocator-attributed memory",
/// not "memory eviction can reclaim" -- Lua scripts (`SCRIPT FLUSH` is the
/// only reclaim path) and the replication backlog are real allocations
/// Redis counts there too, even though neither is "evictable data" in the
/// `--maxmemory` sense. Both terms are already tracked as O(1) accumulators
/// for the `moon_memory_bytes{kind="lua_scripts"}` /
/// `{kind="replication_backlog"}` gauges, so including them here is free --
/// no new instrumentation, just reusing the existing published totals. The
/// elastic budget / eviction gate is UNCHANGED by this: eviction still only
/// ever acts on KV+vector+text+graph, so a large Lua cache or replication
/// backlog can (correctly, matching Redis) push `used_memory` above what
/// eviction is actively bounding, without eviction trying to reclaim either.
///
/// O(num_shards) Relaxed atomic loads -- no lock, no allocation. Every term
/// is itself an O(1) accumulator maintained at its own mutation sites (the
/// `#297` cached-total pattern): this function only sums already-published
/// per-shard snapshots, exactly like `pagecache_bytes` already does at its
/// INFO call site.
#[must_use]
pub fn logical_used_memory_bytes() -> usize {
    let Some(shard_dbs) = get_global_shard_databases() else {
        return 0;
    };
    let dashtable_and_cold_index = shard_dbs.read_memory_sum();
    let store_total: usize = shard_dbs
        .store_memory_per_shard
        .iter()
        .map(|mem| {
            mem.vector.load(Ordering::Relaxed)
                + mem.text.load(Ordering::Relaxed)
                + mem.graph.load(Ordering::Relaxed)
                // moon#506: script sources AND the interpreter heap. The VM is
                // real heap the process holds; a script that anchors megabytes
                // of tables in `_G` has to be visible somewhere, and this is
                // the ledger operators watch.
                + mem.lua.load(Ordering::Relaxed)
                + mem.lua_vm.load(Ordering::Relaxed)
        })
        .sum();
    let replication_backlog = get_global_repl_state_arc()
        .map(|state| state.read().backlog_resident_bytes())
        .unwrap_or(0);
    dashtable_and_cold_index
        .saturating_add(store_total)
        .saturating_add(replication_backlog)
}

/// Bytes held by the per-shard `mlua` VMs, summed across shards — INFO's
/// `used_memory_lua` (moon#506).
///
/// Reads only `ShardStoreMemory::lua_vm`, the interpreter heap sampled by
/// `persistence_tick::run_eviction_tick`. Deliberately EXCLUDES
/// `ShardStoreMemory::lua` (the cached script sources): Redis reports those
/// under `used_memory_scripts`, and folding a 48-byte figure into a 25KB one
/// would just blur which of the two an operator is watching grow.
///
/// 0 before any shard builds a VM (moon initialises Lua lazily), which is the
/// truthful answer, not a missing sample.
#[must_use]
pub fn lua_vm_memory_bytes() -> usize {
    let Some(shard_dbs) = get_global_shard_databases() else {
        return 0;
    };
    shard_dbs
        .store_memory_per_shard
        .iter()
        .map(|mem| mem.lua_vm.load(Ordering::Relaxed))
        .sum()
}

// ── Global SLOWLOG ─────────────────────────────────────────────────────

/// Global slowlog instance accessible from any handler thread.
///
/// Initialized lazily with default thresholds. `init_global_slowlog` should
/// be called from main to apply user-configured values.
static GLOBAL_SLOWLOG: once_cell::sync::Lazy<crate::admin::slowlog::Slowlog> =
    once_cell::sync::Lazy::new(|| crate::admin::slowlog::Slowlog::new(128, 10_000));

/// Initialize the global slowlog with user-configured values.
///
/// Must be called before any command processing. If called after commands
/// have already been recorded, the old entries are lost (new instance).
/// In practice this is called once from main() before shards start.
pub fn init_global_slowlog(max_len: usize, threshold_us: u64) {
    // Force initialization of the Lazy with default, then reconfigure.
    // Since Slowlog fields are behind a Mutex, we just reset.
    let sl = global_slowlog();
    sl.reconfigure(max_len, threshold_us);
}

/// Get a reference to the global slowlog.
#[inline]
pub fn global_slowlog() -> &'static crate::admin::slowlog::Slowlog {
    &GLOBAL_SLOWLOG
}
