//! NUMA-aware thread pinning and memory placement.
//!
//! On Linux: pins the current thread to a specific CPU core via core_affinity,
//! and detects NUMA node topology from sysfs for diagnostics.
//! On macOS/other: all functions are no-ops (developer machine is macOS/aarch64).

/// Pin the current thread to the given core ID.
///
/// On Linux, uses `core_affinity::set_for_current`. On non-Linux, this is a no-op.
/// Called in the shard thread spawn closure BEFORE building the Tokio runtime,
/// ensuring all subsequent allocations go to the local NUMA node.
pub fn pin_to_core(core_id: usize) {
    #[cfg(target_os = "linux")]
    {
        use core_affinity::CoreId;
        let id = CoreId { id: core_id };
        if core_affinity::set_for_current(id) {
            tracing::info!("Shard {} pinned to core {}", core_id, core_id);
        } else {
            tracing::warn!("Shard {} failed to pin to core {}", core_id, core_id);
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = core_id;
        tracing::debug!(
            "Thread pinning not available on this platform (shard {})",
            core_id
        );
    }
}

/// Machine-wide logical CPU count, IGNORING the calling thread's affinity
/// mask. Cached after first call.
///
/// `std::thread::available_parallelism()` respects `sched_getaffinity` on
/// Linux: called from a core-pinned shard thread — or any thread spawned
/// from one, which inherits the single-core mask — it returns 1. That
/// silently serialized every "parallel" consumer sized from it (parallel
/// HNSW builds ran at exactly sequential speed; the background-compactor
/// pool sized itself to 1 worker). This reads the sysfs online-CPU list on
/// Linux (unaffected by affinity) and falls back to
/// `available_parallelism` elsewhere.
pub fn system_parallelism() -> usize {
    static CACHED: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        #[cfg(target_os = "linux")]
        if let Some(n) = std::fs::read_to_string("/sys/devices/system/cpu/online")
            .ok()
            .and_then(|s| count_cpulist(s.trim()))
        {
            return n;
        }
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1)
    })
}

/// Count the CPUs in a Linux cpulist string (e.g. `"0-3,8-11"` → 8).
#[cfg(target_os = "linux")]
fn count_cpulist(cpulist: &str) -> Option<usize> {
    let mut count = 0usize;
    for range_str in cpulist.split(',') {
        let range_str = range_str.trim();
        if range_str.is_empty() {
            continue;
        }
        if let Some((start_s, end_s)) = range_str.split_once('-') {
            let start = start_s.trim().parse::<usize>().ok()?;
            let end = end_s.trim().parse::<usize>().ok()?;
            count += end.checked_sub(start)? + 1;
        } else {
            range_str.parse::<usize>().ok()?;
            count += 1;
        }
    }
    (count > 0).then_some(count)
}

/// Pin the current (short-lived worker) thread to `core_id`, quietly.
///
/// Same mechanism as [`pin_to_core`] but logs at debug level without the
/// shard wording — used by parallel-build workers to ESCAPE the single-core
/// affinity mask they inherit when spawned from a pinned shard (or
/// background-compactor) thread. No-op on non-Linux.
pub fn pin_worker_to_core(core_id: usize) {
    #[cfg(target_os = "linux")]
    {
        use core_affinity::CoreId;
        if !core_affinity::set_for_current(CoreId { id: core_id }) {
            tracing::debug!("worker failed to pin to core {}", core_id);
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = core_id;
    }
}

// ── O5: auxiliary-thread affinity ───────────────────────────────────────────
//
// Shard threads pin to cores `0..num_shards` (see `pin_to_core`) and hold
// that pinning perfectly under load (0 cpu-migrations, measured). Everything
// else — the main accept/dispatch thread, manifest-sync-N, spill-N,
// moon-wal-sync-N, and the AOF writer / auto-save threads — is
// scheduler-placed. Several of these are spawned FROM an already-pinned
// shard thread (`ManifestSyncAgent::spawn`, `SpillThread::new`,
// `WalSyncAgent::spawn` all run inside the shard's own event loop), and Linux
// threads inherit their parent's affinity mask at `pthread_create` time — so
// without an explicit re-pin these auxiliary threads don't just "share" the
// shard core, they are BORN wearing that shard's exact single-core mask and
// can never leave it. That is the mechanism behind digest finding 6 (~12%
// steal observed on a shard core).
//
// The fix: when `shards < cores`, every auxiliary thread re-pins itself (as
// the first act of its closure, the same "escape the inherited mask" idiom
// `pin_worker_to_core` already uses for parallel HNSW build workers) to a
// core drawn round-robin from the non-shard remainder `shards..cores`. When
// `shards >= cores` there is no remainder — `aux_core_set` returns `None` and
// every call below becomes a no-op, deliberately: pinning to an empty set
// would fight the scheduler instead of helping it.
//
// EXCEPTION: the vector segment-reload pool (`moon-vec-reload-N`,
// src/vector/reload_pool.rs) is deliberately excluded from this fix. See the
// comment at its spawn site for why — short version: reloads are
// latency-critical and the reload storm at cold-start/crash-recovery wants
// every core while shard threads are still idle; confining it to the small
// non-shard remainder risks 3x worse cold-reload latency, the exact
// regression PR #237 (parallel HNSW / time-to-green) fixed.
//
// jemalloc's background-reclaim threads are NOT covered here: they are
// spawned internally by jemalloc itself (`background_thread:true` in
// `_rjem_malloc_conf`, see src/main.rs), with no Rust-side spawn hook or
// public API to redirect their affinity. Cross-thread `mallctl` affinity
// control does not exist in tikv-jemallocator/jemalloc's public surface.
// Left as a documented gap, not a silent omission.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Non-shard core IDs for `num_shards` shards (pinned to cores
/// `0..num_shards`) out of `total_cores` machine cores. `None` when there is
/// no remainder (`num_shards >= total_cores`) — pure function, unit-testable
/// without touching real system topology.
fn aux_core_set_for(num_shards: usize, total_cores: usize) -> Option<Vec<usize>> {
    if num_shards >= total_cores {
        return None;
    }
    Some((num_shards..total_cores).collect())
}

/// Non-shard core IDs for `num_shards` shards on this machine
/// (`system_parallelism()` cores). See [`aux_core_set_for`].
pub fn aux_core_set(num_shards: usize) -> Option<Vec<usize>> {
    aux_core_set_for(num_shards, system_parallelism())
}

/// `MOON_NO_AUX_PIN=1` disables auxiliary-thread pinning entirely — a bench
/// A/B knob and operator escape hatch, mirroring the posture of
/// `--io-busy-poll-us` / `MOON_XSHARD_SPIN_BUDGET`: measured opt-in behavior
/// stays overridable, never a silent, un-disable-able tax.
fn aux_pinning_disabled() -> bool {
    std::env::var_os("MOON_NO_AUX_PIN").is_some_and(|v| v == "1")
}

/// Process-wide non-shard core set, computed once. `None` means "don't pin"
/// (shards >= cores, `MOON_NO_AUX_PIN=1`, or [`init_aux_pinning`] was never
/// called — e.g. library-embedded tests that skip startup wiring).
static AUX_CORES: OnceLock<Option<Vec<usize>>> = OnceLock::new();

/// Round-robin cursor spreading auxiliary threads evenly across the
/// non-shard core set instead of piling every one of them onto its first
/// entry.
static AUX_RR: AtomicUsize = AtomicUsize::new(0);

/// Record the non-shard core set for this process. Call once at startup,
/// after `num_shards` is resolved and BEFORE spawning any auxiliary thread
/// (the AOF pool, the vector reload/search pools, and the shard threads
/// themselves — the last of which spawn manifest-sync/spill/wal-sync
/// threads internally). First call wins (idempotent), matching the
/// `OnceLock` init pattern used by `reload_pool::init_global` /
/// `search_pool::init_global` elsewhere in this module's callers.
///
/// Safe to call before any thread is spawned even though shard threads
/// inherit whatever affinity the calling thread has at spawn time: every
/// shard thread calls `pin_to_core` as the first act of its own closure,
/// unconditionally overriding whatever it inherited.
pub fn init_aux_pinning(num_shards: usize) {
    let _ = AUX_CORES.get_or_init(|| {
        if aux_pinning_disabled() {
            None
        } else {
            aux_core_set(num_shards)
        }
    });
}

/// Deterministic per-index pick from the process's non-shard core set, for
/// pools that place worker `i` on a stable core instead of using the shared
/// round-robin cursor (e.g. the background-compact pool). `None` when
/// pinning is off for any of the [`pin_current_aux_thread`] reasons —
/// callers fall back to their own heuristic.
pub fn aux_core_at(i: usize) -> Option<usize> {
    let Some(Some(cores)) = AUX_CORES.get() else {
        return None;
    };
    if cores.is_empty() {
        return None;
    }
    Some(cores[i % cores.len()])
}

/// Pin the CURRENT thread onto the process's non-shard core set (round-robin
/// across it). Call as the first statement of every auxiliary thread's
/// closure — main accept/dispatch, manifest-sync-N, spill-N,
/// moon-wal-sync-N, aof-writer[-N], auto-save, moon-heap-orphan-sweep-N,
/// moon-cold-read-N, moon-vec-snapshot-N, moon-vec-idx-gc. (The
/// background-compact pool `moon-vec-compact-N` uses [`aux_core_at`] with
/// its own fallback instead.) NOT called by the vector segment-reload pool
/// (`moon-vec-reload-N`) — see the comment at its spawn site in
/// src/vector/reload_pool.rs for why that pool stays scheduler-placed.
///
/// cpuset caveat: the core set comes from the machine-wide online-CPU list
/// (`system_parallelism`), so under a restrictive cgroup cpuset the chosen
/// core may be outside the allowed set — `set_for_current` then fails and
/// the thread simply KEEPS its inherited mask (debug-logged). Graceful:
/// never worse than the pre-pinning behavior.
///
/// No-op when [`init_aux_pinning`] was never called, `shards >= cores`, or
/// `MOON_NO_AUX_PIN=1` — in all three cases this deliberately leaves
/// whatever affinity the thread already has (inherited or default) alone.
pub fn pin_current_aux_thread(label: &str) {
    let Some(Some(cores)) = AUX_CORES.get() else {
        return;
    };
    // Defensive: aux_core_set_for never returns Some(vec![]), but a bare
    // index % 0 would panic if that invariant ever slipped.
    if cores.is_empty() {
        return;
    }
    let idx = AUX_RR.fetch_add(1, Ordering::Relaxed) % cores.len();
    let core_id = cores[idx];

    #[cfg(target_os = "linux")]
    {
        use core_affinity::CoreId;
        if core_affinity::set_for_current(CoreId { id: core_id }) {
            tracing::debug!("{label} pinned to non-shard core {core_id}");
        } else {
            tracing::debug!("{label} failed to pin to non-shard core {core_id}");
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        // No strict core-pinning equivalent on macOS (thread_policy_set is a
        // QoS hint, not affinity) — no-op by design, per CLAUDE.md's
        // Linux-only-code rule (compile guard + stub is sufficient).
        let _ = (label, core_id);
    }
}

/// Detect which NUMA node owns the given CPU core (Linux only).
///
/// Reads `/sys/devices/system/node/nodeN/cpulist` to find the node.
/// Returns `None` on non-Linux or if detection fails.
#[cfg(target_os = "linux")]
pub fn detect_numa_node(cpu_id: usize) -> Option<usize> {
    let node_dir = std::path::Path::new("/sys/devices/system/node");
    if !node_dir.exists() {
        return None;
    }
    let entries = std::fs::read_dir(node_dir).ok()?;
    for entry in entries {
        let entry = entry.ok()?;
        let name = entry.file_name();
        let name_str = name.to_str()?;
        if !name_str.starts_with("node") {
            continue;
        }
        let node_id: usize = name_str.strip_prefix("node")?.parse().ok()?;
        let cpulist_path = entry.path().join("cpulist");
        let cpulist = std::fs::read_to_string(&cpulist_path).ok()?;
        if cpu_in_cpulist(cpu_id, cpulist.trim()) {
            return Some(node_id);
        }
    }
    None
}

/// Parse a Linux cpulist string (e.g., "0-3,8-11") and check if cpu_id is in range.
#[cfg(target_os = "linux")]
fn cpu_in_cpulist(cpu_id: usize, cpulist: &str) -> bool {
    for range_str in cpulist.split(',') {
        let range_str = range_str.trim();
        if let Some((start_s, end_s)) = range_str.split_once('-') {
            if let (Ok(start), Ok(end)) = (start_s.parse::<usize>(), end_s.parse::<usize>()) {
                if cpu_id >= start && cpu_id <= end {
                    return true;
                }
            }
        } else if let Ok(single) = range_str.parse::<usize>() {
            if cpu_id == single {
                return true;
            }
        }
    }
    false
}

#[cfg(target_os = "linux")]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cpu_in_cpulist_single() {
        assert!(cpu_in_cpulist(0, "0"));
        assert!(cpu_in_cpulist(3, "3"));
        assert!(!cpu_in_cpulist(1, "0"));
    }

    #[test]
    fn test_cpu_in_cpulist_range() {
        assert!(cpu_in_cpulist(0, "0-3"));
        assert!(cpu_in_cpulist(2, "0-3"));
        assert!(cpu_in_cpulist(3, "0-3"));
        assert!(!cpu_in_cpulist(4, "0-3"));
    }

    #[test]
    fn test_cpu_in_cpulist_mixed() {
        assert!(cpu_in_cpulist(0, "0-3,8-11"));
        assert!(cpu_in_cpulist(9, "0-3,8-11"));
        assert!(!cpu_in_cpulist(5, "0-3,8-11"));
    }

    #[test]
    fn test_count_cpulist() {
        assert_eq!(count_cpulist("0"), Some(1));
        assert_eq!(count_cpulist("0-5"), Some(6));
        assert_eq!(count_cpulist("0-3,8-11"), Some(8));
        assert_eq!(count_cpulist("0-1, 4"), Some(3));
        assert_eq!(count_cpulist(""), None);
        assert_eq!(count_cpulist("garbage"), None);
        assert_eq!(count_cpulist("3-1"), None); // inverted range
    }

    #[test]
    fn test_system_parallelism_at_least_one() {
        assert!(system_parallelism() >= 1);
    }
}

// Not Linux-gated: pure arithmetic, no libc/core_affinity calls — must hold
// on macOS dev machines too.
#[cfg(test)]
mod aux_core_set_tests {
    use super::*;

    #[test]
    fn shards_less_than_cores_returns_remainder() {
        assert_eq!(aux_core_set_for(4, 6), Some(vec![4, 5]));
        assert_eq!(aux_core_set_for(1, 6), Some(vec![1, 2, 3, 4, 5]));
    }

    #[test]
    fn shards_equal_cores_is_noop() {
        assert_eq!(aux_core_set_for(4, 4), None);
        assert_eq!(aux_core_set_for(1, 1), None);
    }

    #[test]
    fn shards_greater_than_cores_is_noop() {
        assert_eq!(aux_core_set_for(6, 4), None);
    }

    #[test]
    fn single_core_machine_is_always_noop() {
        // shards=1, cores=1: the only case a 1-core box can be in (shards is
        // clamped to >=1 elsewhere) — must never pin to an empty set.
        assert_eq!(aux_core_set_for(1, 1), None);
    }

    #[test]
    fn zero_shards_edge_case() {
        // Defensive: num_shards=0 shouldn't happen in practice (main.rs
        // resolves auto-detect to >=1), but the function must not panic and
        // must return the full core range rather than treat 0 as "unbounded".
        assert_eq!(aux_core_set_for(0, 4), Some(vec![0, 1, 2, 3]));
    }

    #[test]
    fn zero_cores_edge_case_never_panics() {
        assert_eq!(aux_core_set_for(0, 0), None);
        assert_eq!(aux_core_set_for(4, 0), None);
    }
}
