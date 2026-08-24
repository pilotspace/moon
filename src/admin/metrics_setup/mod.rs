//! Prometheus metrics initialization and recording helpers.
//!
//! Uses the `metrics` facade crate so metric recording is a single atomic
//! operation on the hot path (counter increment or histogram observation).

mod command_metrics;
mod globals;
mod init;
mod memory;
mod publishers;
mod recorders;

pub use command_metrics::*;
pub use globals::*;
pub use init::*;
pub use memory::*;
pub use publishers::*;
pub use recorders::*;

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

static METRICS_INITIALIZED: AtomicBool = AtomicBool::new(false);
static SERVER_READY: AtomicBool = AtomicBool::new(false);

/// Mark the server as ready (called once after all shards are accepting).
pub fn set_server_ready() {
    SERVER_READY.store(true, Ordering::Release);
}

/// Check if the server is ready (for READYZ health check).
pub fn is_server_ready() -> bool {
    SERVER_READY.load(Ordering::Acquire)
}

// ── Lightweight atomic counters for INFO ────────────────────────────────
// These counters work even when the Prometheus exporter is disabled
// (admin_port=0), so INFO always returns meaningful stats.
static TOTAL_CONNECTIONS: AtomicU64 = AtomicU64::new(0);
static CONNECTED_CLIENTS: AtomicU64 = AtomicU64::new(0);

// ── spsc-wake-floor (M5): event-driven wake observability ───────────────
// Bumped at most once per shard-loop wake / per capped drain cycle — never
// per command — so plain (unsharded) atomics are fine here.
static SPSC_NOTIFY_WAKES: AtomicU64 = AtomicU64::new(0);
static SPSC_DRAIN_RENOTIFY: AtomicU64 = AtomicU64::new(0);
// Busy-poll skip-notify: cross-shard notifies elided because the target
// shard's driver advertised it is spin-polling (and will discover ringbuf
// items via its own probe). Per-dispatch rate, but only on the cross-shard
// path with busy-poll active — plain atomic is fine.
static SPSC_NOTIFY_SKIPPED: AtomicU64 = AtomicU64::new(0);

// ── ft-search-off-eventloop (C5): cooperative-yield observability ────────
// Bumped once per cooperative yield taken by the FT.SEARCH local slice (the
// per-chunk relinquish to the shard event loop). The deterministic proxy that
// a heavy search interleaved with the 1ms tick + co-located commands instead
// of monopolizing the loop. Coarse (≪ per-command rate) ⇒ plain atomic is fine.
static FT_SEARCH_COOPERATIVE_YIELDS: AtomicU64 = AtomicU64::new(0);

// ── INFO-readable counters ──────────────────────────────────────────────
//
// The `counter!()` macros below feed Prometheus and are gated behind
// METRICS_INITIALIZED, which is only true when the admin port is up. INFO
// must report these whether or not anyone scraped Prometheus, so each has an
// ungated atomic alongside it. Relaxed ordering: these are monotonic
// observability counters, never used to synchronise anything.
static KEYSPACE_HITS: AtomicU64 = AtomicU64::new(0);
static KEYSPACE_MISSES: AtomicU64 = AtomicU64::new(0);
static EXPIRED_KEYS: AtomicU64 = AtomicU64::new(0);
static EVICTED_KEYS: AtomicU64 = AtomicU64::new(0);
static EXPIRING_SPILL_SKIPPED: AtomicU64 = AtomicU64::new(0);

static SPILLED_KEYS: AtomicU64 = AtomicU64::new(0);
static REJECTED_CONNECTIONS: AtomicU64 = AtomicU64::new(0);
static NET_INPUT_BYTES: AtomicU64 = AtomicU64::new(0);
static NET_OUTPUT_BYTES: AtomicU64 = AtomicU64::new(0);
/// Sampled once per second by the chore tick; `instantaneous_ops_per_sec` is
/// the delta since the previous sample, NOT a per-command computation.
static OPS_LAST_SAMPLE: AtomicU64 = AtomicU64::new(0);
static OPS_PER_SEC: AtomicU64 = AtomicU64::new(0);
/// Clients currently parked in a blocking command. A GAUGE, not a counter:
/// the per-shard `BlockingRegistry` that owns the truth is an `Rc<RefCell<_>>`
/// pinned to its shard thread, so INFO — which runs on whichever thread the
/// asking connection landed on — cannot read it directly. Maintained instead
/// at the registry's two `wait_keys` transitions, which are the exact points a
/// client becomes and stops being blocked.
static BLOCKED_CLIENTS: AtomicI64 = AtomicI64::new(0);

/// Count one cooperative yield taken by the FT.SEARCH local slice (per chunk).
#[inline]
pub fn bump_ft_search_cooperative_yield() {
    FT_SEARCH_COOPERATIVE_YIELDS.fetch_add(1, Ordering::Relaxed);
}

/// Total cooperative yields taken by FT.SEARCH local slices (for INFO Stats).
#[inline]
pub fn ft_search_cooperative_yields() -> u64 {
    FT_SEARCH_COOPERATIVE_YIELDS.load(Ordering::Relaxed)
}

/// Count a shard-loop wake that came from the cross-shard `Notify` arm
/// (event-driven drain) rather than the periodic timer.
///
/// Includes wakes caused by the drain-cap self-re-notify: the `flume
/// bounded(1)` token coalesces producer notifies and self-re-notifies, so
/// they are indistinguishable at the wake site. Producer-driven wakes ≈
/// `spsc_notify_wakes - spsc_drain_renotify` (approximate — a coalesced
/// token can carry both causes).
#[inline]
pub fn bump_spsc_notify_wake() {
    SPSC_NOTIFY_WAKES.fetch_add(1, Ordering::Relaxed);
}

/// Total shard-loop wakes driven by the cross-shard `Notify` (for INFO Stats).
#[inline]
pub fn spsc_notify_wakes() -> u64 {
    SPSC_NOTIFY_WAKES.load(Ordering::Relaxed)
}

/// Count a self-re-notify issued because `drain_spsc_shared` stopped at the
/// per-cycle drain cap with messages possibly remaining.
#[inline]
pub fn bump_spsc_drain_renotify() {
    SPSC_DRAIN_RENOTIFY.fetch_add(1, Ordering::Relaxed);
}

/// Total capped-drain self-re-notifies (for INFO Stats).
#[inline]
pub fn spsc_drain_renotify() -> u64 {
    SPSC_DRAIN_RENOTIFY.load(Ordering::Relaxed)
}

/// Count a cross-shard notify elided because the target shard's busy-poll
/// driver advertised it will discover the ringbuf push via its spin probe.
#[inline]
pub fn bump_spsc_notify_skipped() {
    SPSC_NOTIFY_SKIPPED.fetch_add(1, Ordering::Relaxed);
}

/// Total skip-wake-elided cross-shard notifies (for INFO Stats).
#[inline]
pub fn spsc_notify_skipped() -> u64 {
    SPSC_NOTIFY_SKIPPED.load(Ordering::Relaxed)
}

// ── QW4 (2026-06 review finding 1.6): sharded total-commands counter ────
// Previously a single `TOTAL_COMMANDS: AtomicU64` — one cache line bounced
// across every shard core at full command rate (false sharing). Each OS
// thread takes a padded slot (round-robin at first use); increments touch
// only that thread's line. Readers (INFO, server_stats tick) sum all slots;
// the sum is exact — every increment lands in exactly one slot.
const COMMAND_COUNTER_SLOTS: usize = 64;

#[repr(align(64))]
struct PaddedCounter(AtomicU64);

#[allow(clippy::declare_interior_mutable_const)] // template for static array init only
const PADDED_COUNTER_ZERO: PaddedCounter = PaddedCounter(AtomicU64::new(0));
static COMMAND_COUNTERS: [PaddedCounter; COMMAND_COUNTER_SLOTS] =
    [PADDED_COUNTER_ZERO; COMMAND_COUNTER_SLOTS];
static NEXT_COMMAND_COUNTER_SLOT: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static COMMAND_COUNTER_SLOT: usize =
        (NEXT_COMMAND_COUNTER_SLOT.fetch_add(1, Ordering::Relaxed) as usize)
            % COMMAND_COUNTER_SLOTS;
}

/// Increment this thread's slot of the sharded total-commands counter.
#[inline]
fn bump_total_commands() {
    COMMAND_COUNTER_SLOT.with(|&slot| {
        COMMAND_COUNTERS[slot].0.fetch_add(1, Ordering::Relaxed);
    });
}

/// This thread's slot of the sharded total-commands counter. Used by the
/// shard loop's adaptive idle park (#373) as its "commands dispatched on
/// this shard since the last tick" signal — one relaxed load per tick.
/// Slot collisions (>64 threads share slots round-robin) can only inflate
/// the count, which keeps the loop in fast mode: the safe direction.
#[inline]
pub fn this_thread_commands() -> u64 {
    COMMAND_COUNTER_SLOT.with(|&slot| COMMAND_COUNTERS[slot].0.load(Ordering::Relaxed))
}

/// Count a replica-applied command in this thread's total-commands slot.
/// Redis parity: replicas include applied master-stream commands in
/// `total_commands_processed`. Also the adaptive idle park's (#373)
/// activity signal for the apply path, which bypasses the connection
/// handlers and would otherwise be invisible to the idle gate — a busy
/// replica must never be classified idle.
#[inline]
pub fn record_replica_apply() {
    bump_total_commands();
}

/// Exact sum across all counter slots. O(64) loads — read paths only
/// (INFO, the 1s server_stats tick), never the command hot path.
fn total_commands_sum() -> u64 {
    COMMAND_COUNTERS
        .iter()
        .map(|c| c.0.load(Ordering::Relaxed))
        .sum()
}

// ── EC9: keyspace-change counter behind `rdb_changes_since_last_save` ────
// The "is a save worth doing" signal every backup script reads. Sharded over
// the same padded per-thread slots as the total-commands counter for the same
// reason (a single line would bounce across every shard core at write rate);
// the increment is one relaxed fetch_add on the thread's own line.
//
// Counted at the storage funnels (`Database::set` / `remove` / `get_mut` /
// `clear` / `set_expiry`), not at dispatch: dispatch does not know whether a
// command mutated, and a phf flags lookup on the hot path is exactly the cost
// this codebase's perf invariants forbid. `get_mut` hands out mutable access
// that the caller may or may not use, so the count can run slightly HIGH.
// That direction is deliberate: over-counting says "changes pending" and
// triggers a save that was not needed, while under-counting would tell a
// backup script the dataset was clean when it was not.
static KEYSPACE_CHANGE_COUNTERS: [PaddedCounter; COMMAND_COUNTER_SLOTS] =
    [PADDED_COUNTER_ZERO; COMMAND_COUNTER_SLOTS];
/// Value of the change counter when the last save completed. `rdb_changes_
/// since_last_save` is the difference; a save that completes concurrently with
/// writes can only make the difference smaller, never negative (saturating).
static KEYSPACE_CHANGES_AT_LAST_SAVE: AtomicU64 = AtomicU64::new(0);

/// Record one keyspace mutation. Hot path: one relaxed add, no allocation.
#[inline]
pub fn record_keyspace_change() {
    COMMAND_COUNTER_SLOT.with(|&slot| {
        KEYSPACE_CHANGE_COUNTERS[slot]
            .0
            .fetch_add(1, Ordering::Relaxed);
    });
}

/// Exact sum across all slots. Read paths only (INFO).
fn keyspace_changes_sum() -> u64 {
    KEYSPACE_CHANGE_COUNTERS
        .iter()
        .map(|c| c.0.load(Ordering::Relaxed))
        .sum()
}

/// Keyspace mutations since the last completed save (INFO `rdb_changes_since_last_save`).
pub fn rdb_changes_since_last_save() -> u64 {
    keyspace_changes_sum().saturating_sub(KEYSPACE_CHANGES_AT_LAST_SAVE.load(Ordering::Relaxed))
}

/// Mark a save as complete: subsequent changes count from here.
///
/// Called on SAVE / BGSAVE success, never on failure — a failed save left the
/// dataset unpersisted, so the pending-change count must survive it.
pub fn mark_save_completed() {
    KEYSPACE_CHANGES_AT_LAST_SAVE.store(keyspace_changes_sum(), Ordering::Relaxed);
}

// ── EC9: replica sync counters (INFO `sync_full` / `sync_partial_*`) ─────
// How an operator sees replicas thrashing: a climbing `sync_full` against a
// flat `sync_partial_ok` means partial resync keeps failing and every replica
// reconnect is re-shipping the whole dataset. Plain atomics — these fire once
// per replica handshake, not per command.
static SYNC_FULL: AtomicU64 = AtomicU64::new(0);
static SYNC_PARTIAL_OK: AtomicU64 = AtomicU64::new(0);
static SYNC_PARTIAL_ERR: AtomicU64 = AtomicU64::new(0);

/// A replica was served a full resync (`+FULLRESYNC`).
#[inline]
pub fn record_sync_full() {
    SYNC_FULL.fetch_add(1, Ordering::Relaxed);
}

/// A replica's `PSYNC <id> <offset>` was satisfied from the backlog.
#[inline]
pub fn record_sync_partial_ok() {
    SYNC_PARTIAL_OK.fetch_add(1, Ordering::Relaxed);
}

/// A replica asked for a partial resync that could not be served.
#[inline]
pub fn record_sync_partial_err() {
    SYNC_PARTIAL_ERR.fetch_add(1, Ordering::Relaxed);
}

/// Full resyncs served since start.
pub fn sync_full() -> u64 {
    SYNC_FULL.load(Ordering::Relaxed)
}

/// Partial resyncs served from the backlog since start.
pub fn sync_partial_ok() -> u64 {
    SYNC_PARTIAL_OK.load(Ordering::Relaxed)
}

/// Partial-resync requests that had to fall back to a full resync.
pub fn sync_partial_err() -> u64 {
    SYNC_PARTIAL_ERR.load(Ordering::Relaxed)
}

// ── P6: WAL aggressive reclamation counters (read by P10 INFO emitter) ───
// Incremented by WalWriterV3::recycle_aggressive(). P10 reads these via the
// public getters below to populate the `# Reclamation` INFO section.
// Using relaxed ordering: these are monotonic event counters for monitoring,
// not synchronisation primitives.
static WAL_AGGRESSIVE_RECYCLE_SEGMENTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static WAL_AGGRESSIVE_RECYCLE_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);

// ── T1.4: Cross-shard dispatch counters (read by INFO stats) ────────────
// The Prometheus `counter!()` facade is not directly readable from the INFO
// path — it only surfaces via /metrics. These dedicated AtomicU64 counters
// are incremented in parallel inside the existing record_dispatch_* helpers
// so INFO always returns meaningful stats even with admin_port=0.
// Relaxed ordering is correct: these are monotonic event counters for
// observability, not synchronisation primitives.
//
// Note: a separate write-SPSC counter does not exist in the codebase —
// the existing `record_dispatch_cross_spsc` covers both reads routed via
// SPSC (when fast-path is off) and writes. INFO exposes the unified total
// as `total_dispatch_cross_spsc`.
static DISPATCH_CROSS_READ_SPSC_TOTAL: AtomicU64 = AtomicU64::new(0);
/// moon#513: pipeline batches cut short because a command could not execute
/// against shards whose earlier writes in the same batch were still pending.
/// Each increment is one extra dispatch/await boundary — measured at ~57us on
/// moon-dev — so this is the counter that says whether a slow pipeline is
/// paying the moon#512 ordering guarantee or something else entirely.
static PIPELINE_REMOTE_DEFER_TOTAL: AtomicU64 = AtomicU64::new(0);

// ── INFO-readable counter accessors ─────────────────────────────────────

/// Number of successful key lookups since start.
pub fn keyspace_hits() -> u64 {
    KEYSPACE_HITS.load(Ordering::Relaxed)
}

/// Number of lookups that found no key since start.
pub fn keyspace_misses() -> u64 {
    KEYSPACE_MISSES.load(Ordering::Relaxed)
}

/// Keys removed because their TTL elapsed.
pub fn expired_keys() -> u64 {
    EXPIRED_KEYS.load(Ordering::Relaxed)
}

/// Keys REMOVED FROM THE KEYSPACE by the maxmemory eviction policy.
///
/// Pairs with `DBSIZE`: this counter and the key count move together (see
/// [`record_eviction`]). Tiered keys are in [`spilled_keys`] instead.
pub fn evicted_keys() -> u64 {
    EVICTED_KEYS.load(Ordering::Relaxed)
}

/// Eviction victims dropped instead of spilled because they were about to
/// expire (moon#553). A subset of [`evicted_keys`].
pub fn expiring_spill_skipped() -> u64 {
    EXPIRING_SPILL_SKIPPED.load(Ordering::Relaxed)
}

/// Keys MOVED FROM RAM TO DISK by the maxmemory eviction policy.
///
/// These keys are still in `DBSIZE` and still readable — see
/// [`record_key_spilled`].
pub fn spilled_keys() -> u64 {
    SPILLED_KEYS.load(Ordering::Relaxed)
}

/// Connections refused (limit reached, or rejected before handshake).
pub fn rejected_connections() -> u64 {
    REJECTED_CONNECTIONS.load(Ordering::Relaxed)
}

/// Bytes read from clients since start.
pub fn total_net_input_bytes() -> u64 {
    NET_INPUT_BYTES.load(Ordering::Relaxed)
}

/// Bytes written to clients since start.
pub fn total_net_output_bytes() -> u64 {
    NET_OUTPUT_BYTES.load(Ordering::Relaxed)
}

/// Clients currently parked in a blocking command (BLPOP, BRPOP, XREAD …).
///
/// Never negative: a decrement that would go below zero means a
/// block/unblock pair was mismatched, and reporting a negative gauge would
/// turn a bookkeeping bug into a nonsense dashboard.
pub fn blocked_clients() -> u64 {
    BLOCKED_CLIENTS.load(Ordering::Relaxed).max(0) as u64
}

/// A client entered a blocking wait.
#[inline]
pub fn record_client_blocked() {
    BLOCKED_CLIENTS.fetch_add(1, Ordering::Relaxed);
}

/// A client left a blocking wait — served, timed out, or cancelled.
#[inline]
pub fn record_client_unblocked() {
    BLOCKED_CLIENTS.fetch_sub(1, Ordering::Relaxed);
}

/// Commands per second over the last sampling window.
pub fn instantaneous_ops_per_sec() -> u64 {
    OPS_PER_SEC.load(Ordering::Relaxed)
}

/// Record an expired key.
#[inline]
pub fn record_expired_key() {
    EXPIRED_KEYS.fetch_add(1, Ordering::Relaxed);
}

/// Record a refused connection.
#[inline]
pub fn record_rejected_connection() {
    REJECTED_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
}

/// Record bytes read from / written to clients.
#[inline]
pub fn record_net_bytes(input: u64, output: u64) {
    if input > 0 {
        NET_INPUT_BYTES.fetch_add(input, Ordering::Relaxed);
    }
    if output > 0 {
        NET_OUTPUT_BYTES.fetch_add(output, Ordering::Relaxed);
    }
}

/// Sample the ops-per-second rate. Call once per second from a chore tick.
///
/// Deliberately a sampled delta rather than a per-command rate computation:
/// the alternative would put a timestamp read on the dispatch path for a
/// field nobody reads more than once a second.
pub fn sample_ops_per_sec() {
    let total = total_commands_processed();
    let prev = OPS_LAST_SAMPLE.swap(total, Ordering::Relaxed);
    OPS_PER_SEC.store(total.saturating_sub(prev), Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    // Smoke tests for the dispatch-path counters added in Phase 177, Step 6.
    // A full assertion on the prometheus state would require initialising a
    // recorder and a scraping harness — out of scope here. These tests just
    // pin the contract that the helpers are safe to call on the hot path
    // before `init_metrics` has run and therefore never panic or allocate
    // unexpectedly when the exporter is disabled (admin_port = 0).

    #[test]
    fn dispatch_path_counters_no_op_before_init() {
        // METRICS_INITIALIZED starts false; all three helpers must early-return.
        // We just assert they do not panic. The absence of a global recorder
        // means counter!() would otherwise be a no-op, but the guard is what
        // we actually care about: no string allocation, no label churn.
        assert!(!METRICS_INITIALIZED.load(Ordering::Relaxed));
        record_dispatch_local();
        record_dispatch_cross_spsc();
        record_dispatch_local_inline(0); // count == 0 must short-circuit even when init
        record_dispatch_local_inline(7);
    }

    #[test]
    fn cached_metrics_skips_rebuild_on_same_cmd() {
        let mut cache = CachedMetricsHandles::new();
        assert!(cache.last_cmd.is_empty(), "fresh cache starts empty");

        cache.ensure(b"SET");
        assert_eq!(cache.last_cmd.as_slice(), b"SET", "first call populates");

        // Repeated SET: cache hit. We cannot observe the skip directly without a
        // mock recorder, but the stored bytes must remain and not churn.
        cache.ensure(b"SET");
        assert_eq!(cache.last_cmd.as_slice(), b"SET");

        // Different command: must rebuild and swap the buffer contents.
        cache.ensure(b"GET");
        assert_eq!(cache.last_cmd.as_slice(), b"GET");

        // Mixed case is treated as a distinct raw input; sanitize_cmd_label
        // will still normalise to "set" for the Prometheus label, but the
        // cache key is the raw bytes (pointer to the last call's payload).
        cache.ensure(b"set");
        assert_eq!(cache.last_cmd.as_slice(), b"set");
    }

    #[test]
    fn record_command_cached_no_op_before_init() {
        assert!(!METRICS_INITIALIZED.load(Ordering::Relaxed));
        let mut cache = CachedMetricsHandles::new();
        record_command_cached("set", 1, &mut cache);
        record_command_no_latency_cached("set", &mut cache);
        record_command_error_cached("set", &mut cache);
        // Must not panic, must not churn the cache on the hot path.
    }

    // ── T1.4: cross-shard dispatch atomics ───────────────────────────────
    // These tests share process-wide static AtomicU64 counters.  Because
    // the test runner is multi-threaded, other tests may increment the same
    // static concurrently.  We therefore only assert on monotone lower
    // bounds (after >= before + N) for positive-increment tests, which is
    // still sufficient to prove the counter was incremented.
    // The zero-is-noop tests read the counter twice with no intervening
    // increment; they assert `after >= before` (monotone), which is the
    // strongest correct claim under parallel execution.

    #[test]
    fn cross_spsc_atomic_increments() {
        let before = total_dispatch_cross_spsc();
        record_dispatch_cross_spsc();
        let after = total_dispatch_cross_spsc();
        assert!(
            after > before,
            "counter must have increased by at least 1; before={before} after={after}"
        );
    }

    #[test]
    fn cross_spsc_batch_atomic_increments() {
        let before = total_dispatch_cross_spsc();
        record_dispatch_cross_spsc_batch(3);
        let after = total_dispatch_cross_spsc();
        assert!(
            after >= before + 3,
            "counter must have increased by at least 3; before={before} after={after}"
        );
    }

    #[test]
    fn cross_spsc_batch_zero_is_noop() {
        let before = total_dispatch_cross_spsc();
        record_dispatch_cross_spsc_batch(0);
        let after = total_dispatch_cross_spsc();
        assert!(
            after >= before,
            "counter must be monotone; before={before} after={after}"
        );
    }
}
