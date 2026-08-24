//! Recording helpers for the non-command metric families: connections,
//! keyspace, eviction, persistence, shards, dispatch routing, vector search,
//! pub/sub, and replication.
//!
//! Split out of the former single-file `metrics_setup.rs` (moon#479, file-size
//! ceiling); every helper is unchanged.

use std::sync::atomic::Ordering;

use metrics::{counter, gauge, histogram};

use crate::admin::metrics_setup::{
    CONNECTED_CLIENTS, DISPATCH_CROSS_READ_SPSC_TOTAL, EVICTED_KEYS, EXPIRING_SPILL_SKIPPED,
    KEYSPACE_HITS, KEYSPACE_MISSES, METRICS_INITIALIZED, PIPELINE_REMOTE_DEFER_TOTAL, SPILLED_KEYS,
    TOTAL_CONNECTIONS, WAL_AGGRESSIVE_RECYCLE_BYTES_TOTAL, WAL_AGGRESSIVE_RECYCLE_SEGMENTS_TOTAL,
};

// ── Connection metrics ──────────────────────────────────────────────────

/// Record a new client connection.
#[inline]
pub fn record_connection_opened() {
    TOTAL_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
    CONNECTED_CLIENTS.fetch_add(1, Ordering::Relaxed);
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_connections_total").increment(1);
    gauge!("moon_connected_clients").increment(1.0);
}

/// Record a client disconnection.
#[inline]
pub fn record_connection_closed() {
    CONNECTED_CLIENTS.fetch_sub(1, Ordering::Relaxed);
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    gauge!("moon_connected_clients").decrement(1.0);
}

/// Current number of connected clients (for INFO command).
#[inline]
pub fn connected_clients() -> u64 {
    CONNECTED_CLIENTS.load(Ordering::Relaxed)
}

/// Adjust the per-shard connected-clients gauge (S-6 observability): the
/// only way to see SO_REUSEPORT imbalance or an affinity-funnel pile-up
/// without parsing CLIENT LIST. Called from client-registry
/// register/deregister — connect/disconnect rate, never per-command.
#[inline]
pub fn record_shard_connection_delta(shard: usize, delta: f64) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    gauge!("moon_shard_connected_clients", "shard" => shard.to_string()).increment(delta);
}

/// Record a cross-shard dispatch message dropped after the bounded
/// backpressure budget expired ([`PushOutcome::Backpressure`] give-up, R-1).
///
/// Fires only on the give-up path (target ring never drained for ~0.5s), so
/// the label allocation is off the hot path. A non-zero rate here means a
/// shard is wedged or persistently saturated — reply-carrying callers have
/// surfaced per-command errors, but this counter is the aggregate signal.
///
/// [`PushOutcome::Backpressure`]: crate::shard::dispatch::PushOutcome::Backpressure
#[inline]
pub fn record_xshard_backpressure_drop(target_shard: usize) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!(
        "moon_xshard_backpressure_drops_total",
        "target_shard" => target_shard.to_string()
    )
    .increment(1);
}

/// Try to open a connection if under the maxclients limit.
/// Returns true if the connection was accepted, false if at limit.
/// When maxclients is 0, the limit is disabled (unlimited).
#[inline]
pub fn try_accept_connection(maxclients: usize) -> bool {
    if maxclients == 0 {
        record_connection_opened();
        return true;
    }
    // CAS loop: only increment if under limit.
    // AcqRel on success ensures the counter increment is visible to other cores
    // before the connection handler runs (important on ARM/weak-memory archs).
    let mut current = CONNECTED_CLIENTS.load(Ordering::Acquire);
    loop {
        if current >= maxclients as u64 {
            return false;
        }
        match CONNECTED_CLIENTS.compare_exchange_weak(
            current,
            current + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                TOTAL_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
                if METRICS_INITIALIZED.load(Ordering::Relaxed) {
                    counter!("moon_connections_total").increment(1);
                    gauge!("moon_connected_clients").increment(1.0);
                }
                return true;
            }
            Err(actual) => current = actual,
        }
    }
}

// ── Keyspace metrics ────────────────────────────────────────────────────

/// Record keyspace hit/miss.
#[inline]
pub fn record_keyspace_hit() {
    KEYSPACE_HITS.fetch_add(1, Ordering::Relaxed);
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_keyspace_hits_total").increment(1);
}

#[inline]
pub fn record_keyspace_miss() {
    KEYSPACE_MISSES.fetch_add(1, Ordering::Relaxed);
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_keyspace_misses_total").increment(1);
}

// ── Eviction metrics ────────────────────────────────────────────────────

/// Record one key REMOVED FROM THE KEYSPACE to free memory.
///
/// Redis parity, and the moon#585 invariant: `evicted_keys` counts exactly
/// the keys `DBSIZE` stops counting. A key the tiering plane moved from RAM
/// to disk has NOT left the keyspace (moon#355 — it is still readable, still
/// in `DBSIZE`) and must be recorded with [`record_key_spilled`] instead. The
/// two must never be conflated: before moon#585 the durable batch spiller
/// counted every tiered key here, so a live instance reported 456,018
/// "evicted" keys while `DBSIZE` never moved.
#[inline]
pub fn record_eviction() {
    EVICTED_KEYS.fetch_add(1, Ordering::Relaxed);
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_evicted_keys_total").increment(1);
}

/// Record an eviction victim that was DROPPED rather than spilled to the
/// cold tier because its remaining TTL was under
/// [`crate::storage::eviction::SPILL_TTL_FLOOR_MS`] (moon#553).
///
/// Also counted by [`record_eviction`] — this counter answers "how much spill
/// IO did the TTL guard avoid", which is exactly what makes a benchmark of
/// the guard non-vacuous: a run where it reads 0 never exercised the guard.
#[inline]
pub fn record_expiring_spill_skipped() {
    EXPIRING_SPILL_SKIPPED.fetch_add(1, Ordering::Relaxed);
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_eviction_expiring_drop_total").increment(1);
}

/// Record one key MOVED FROM RAM TO DISK by the eviction sweep.
///
/// The tiering counterpart of [`record_eviction`]: memory was reclaimed but
/// the key is still in the keyspace, so `DBSIZE` deliberately does not move.
/// Operators watching a `--disk-offload enable` instance read this counter
/// (not `evicted_keys`) to see memory-pressure activity.
///
/// One relaxed `fetch_add` plus one relaxed load — the same shape as
/// [`record_eviction`], which already sits on the 100 ms eviction sweep. No
/// allocation, no lock.
#[inline]
pub fn record_key_spilled() {
    SPILLED_KEYS.fetch_add(1, Ordering::Relaxed);
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_spilled_keys_total").increment(1);
}

// ── Persistence metrics ─────────────────────────────────────────────────

/// Record an AOF fsync duration.
#[inline]
pub fn record_aof_fsync(duration_us: u64) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    histogram!("moon_aof_fsync_duration_microseconds").record(duration_us as f64);
}

/// Record a WAL segment rotation.
#[inline]
pub fn record_wal_rotation() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_wal_rotations_total").increment(1);
}

/// Record segments and bytes freed by P6 aggressive WAL recycle.
///
/// Called from [`WalWriterV3::recycle_aggressive`] after each successful
/// segment deletion. The increments are also reflected in the Prometheus
/// counter `moon_wal_aggressive_recycle_segments_total` when the exporter
/// is enabled; the atomics are always updated so P10 INFO works without
/// Prometheus.
#[inline]
pub fn record_wal_aggressive_recycle(segments: u64, bytes: u64) {
    WAL_AGGRESSIVE_RECYCLE_SEGMENTS_TOTAL.fetch_add(segments, Ordering::Relaxed);
    WAL_AGGRESSIVE_RECYCLE_BYTES_TOTAL.fetch_add(bytes, Ordering::Relaxed);
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_wal_aggressive_recycle_segments_total").increment(segments);
    counter!("moon_wal_aggressive_recycle_bytes_total").increment(bytes);
}

/// Return the total number of WAL segments freed by aggressive recycle since
/// process start. Used by P10 for the `reclamation_wal_segments` INFO field.
#[inline]
pub fn wal_aggressive_recycle_segments_total() -> u64 {
    WAL_AGGRESSIVE_RECYCLE_SEGMENTS_TOTAL.load(Ordering::Relaxed)
}

/// Return the total bytes freed by aggressive WAL recycle since process start.
/// Used by P10 for the `reclamation_wal_bytes` INFO field.
#[inline]
pub fn wal_aggressive_recycle_bytes_total() -> u64 {
    WAL_AGGRESSIVE_RECYCLE_BYTES_TOTAL.load(Ordering::Relaxed)
}

// ── Shard metrics ───────────────────────────────────────────────────────

/// Record SPSC queue drain batch size.
#[inline]
pub fn record_spsc_drain(shard_id: usize, count: u64) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    let shard = itoa::Buffer::new().format(shard_id).to_string();
    histogram!("moon_spsc_drain_batch_size", "shard" => shard).record(count as f64);
}

// ── Dispatch routing counters (Phase 177, Step 6) ───────────────────────
// Three-way split of the connection hot path so we can quantify what
// fraction of traffic is hitting the expensive cross-shard SPSC dispatch
// vs. the free local / shared-read fast paths. Ratio of these counters
// is the ground-truth signal for validating dispatch-layer optimizations
// (HotShardMessage split, outbox batching, waker relay fusion).

/// Command executed on the connection's own shard (no cross-thread hop).
#[inline]
pub fn record_dispatch_local() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_dispatch_path_total", "path" => "local").increment(1);
}

/// Batched variant of `record_dispatch_local`: one atomic increment per
/// caller instead of N. Used on the per-batch hot loop in the sharded and
/// monoio handlers to avoid per-command global-atomic cache-line bouncing
/// on `moon_dispatch_path_total{path="local"}`. Short-circuits on
/// `count == 0` so empty batches pay nothing.
#[inline]
pub fn record_dispatch_local_batch(count: u64) {
    if count == 0 || !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_dispatch_path_total", "path" => "local").increment(count);
}

/// Command deferred to cross-shard SPSC dispatch (the slow path).
/// Recorded when a command is enqueued into a `remote_groups` bucket that
/// will be flushed as a `PipelineBatchSlotted` message.
///
/// Note: covers both read and write commands routed via SPSC (no split
/// counter exists — all non-fast-path cross-shard traffic goes here).
#[inline]
pub fn record_dispatch_cross_spsc() {
    // Always increment the INFO-visible atomic (works even with admin_port=0).
    DISPATCH_CROSS_READ_SPSC_TOTAL.fetch_add(1, Ordering::Relaxed);
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_dispatch_path_total", "path" => "cross_spsc").increment(1);
}

/// Cross-shard fan-out message dropped after bounded retry (c10k E1/E3):
/// the target shard's SPSC ring stayed full through every backoff. `kind` is
/// `"publish"` (that shard's subscribers miss the message; PUBLISH count
/// under-reports) or `"script_load"` (that shard's script cache diverges —
/// EVALSHA there answers NOSCRIPT until the next SCRIPT LOAD).
#[inline]
pub fn record_xshard_fanout_drop(kind: &'static str) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_xshard_fanout_drop_total", "kind" => kind).increment(1);
}

/// Cross-shard reply await expired (c10k E4): the owner shard did not fill
/// the reply slot within `XSHARD_REPLY_TIMEOUT`. `kind` `"dispatch"` is
/// fatal for the connection (the reusable slot may be filled late);
/// `"publish"` degrades to an under-reported subscriber count.
#[inline]
pub fn record_xshard_reply_timeout(kind: &'static str) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_xshard_reply_timeout_total", "kind" => kind).increment(1);
}

/// A pipeline batch was cut short by the moon#507 ordering guard.
///
/// Recorded at the two sites that set `deferred_tail_from` for
/// `must_wait_for_pending_remote`: the command and the unconsumed tail move to
/// the next loop iteration so the pending `remote_groups` resolve first.
///
/// Exists because the cost is invisible otherwise. A client interleaving reads
/// between write groups at `--shards >= 2` pays one boundary per interleaving,
/// and nothing in `INFO` said so — the throughput just looked bad. It is also
/// what keeps a benchmark for moon#513 honest: a shape that claims to trigger
/// the guard has to be able to PROVE it did, and how often.
#[inline]
pub fn record_pipeline_remote_defer() {
    PIPELINE_REMOTE_DEFER_TOTAL.fetch_add(1, Ordering::Relaxed);
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_pipeline_remote_defer_total").increment(1);
}

/// Batched variant of `record_dispatch_cross_spsc`.
#[inline]
pub fn record_dispatch_cross_spsc_batch(count: u64) {
    if count == 0 {
        return;
    }
    // Always increment the INFO-visible atomic (works even with admin_port=0).
    DISPATCH_CROSS_READ_SPSC_TOTAL.fetch_add(count, Ordering::Relaxed);
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_dispatch_path_total", "path" => "cross_spsc").increment(count);
}

/// Command handled by the inline GET/SET fast path
/// (`try_inline_dispatch_loop` in `server/conn/blocking.rs`) — the hottest
/// local branch, which bypasses the standard frame-by-frame routing and
/// therefore the three counters above. Recorded in a single batch increment
/// per dispatch loop to keep the call site out of the per-command hot path.
#[inline]
pub fn record_dispatch_local_inline(count: u64) {
    if count == 0 || !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_dispatch_path_total", "path" => "local_inline").increment(count);
}

// ── Vector search metrics (v0.1.6) ─────────────────────────────────────

/// Record a cache hit for FT.CACHESEARCH.
#[inline]
pub fn record_cache_hit() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_cache_hits_total").increment(1);
}

/// Record a cache miss for FT.CACHESEARCH.
#[inline]
pub fn record_cache_miss() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_cache_misses_total").increment(1);
}

/// Update the document count gauge for a vector index.
/// Called after FT.CREATE, HSET auto-index, FT.DROPINDEX, and compaction.
#[inline]
pub fn update_vector_index_docs(index_name: &str, count: u64) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    // Sanitize index name: only allow alphanumeric + underscore + hyphen, max 64 chars.
    if index_name.len() > 64
        || !index_name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return;
    }
    let label = index_name.to_string();
    gauge!("moon_vector_index_docs", "index" => label).set(count as f64);
}

/// Update total vector memory usage gauge (bytes across all indexes).
#[inline]
pub fn update_vector_memory_bytes(bytes: u64) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    gauge!("moon_vector_memory_bytes").set(bytes as f64);
}

// ── Pub/Sub metrics ─────────────────────────────────────────────────────

/// Record a pub/sub message published.
#[inline]
pub fn record_pubsub_published() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_pubsub_messages_published_total").increment(1);
}

/// Record a slow subscriber drop.
#[inline]
pub fn record_pubsub_slow_drop() {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    counter!("moon_pubsub_slow_subscriber_drops_total").increment(1);
}

// ── Replication metrics ─────────────────────────────────────────────

/// Record replication lag (byte offset and time-based).
///
/// Called periodically when replication is active. When no replicas are
/// connected, the gauges remain at their last-set values (or zero).
#[inline]
pub fn record_replication_lag(bytes: u64, ms: u64) {
    if !METRICS_INITIALIZED.load(Ordering::Relaxed) {
        return;
    }
    gauge!("moon_replication_lag_bytes").set(bytes as f64);
    gauge!("moon_replication_lag_ms").set(ms as f64);
}
