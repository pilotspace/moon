use std::path::{Path, PathBuf};

use bytes::Bytes;
use rand::RngExt;
use smallvec::SmallVec;
use tracing::warn;

use crate::config::RuntimeConfig;
use crate::persistence::kv_page::{ValueType, entry_flags};
use crate::persistence::manifest::ShardManifest;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::compact_key::CompactKey;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::entry::lfu_decay;
use crate::storage::tiered::kv_serde;
use crate::storage::tiered::kv_spill;
use crate::storage::tiered::spill_thread::SpillRequest;

/// Maximum number of victim candidates we will sample in a single
/// `find_victim_*` call. This bounds the inline storage of the SmallVec
/// returned by `sample_random_keys` and matches a generous upper bound on
/// the user-tunable `maxmemory-samples` (Redis default 5; we accept up to 16).
const MAX_VICTIM_SAMPLES: usize = 16;

/// Reservoir-sample up to `samples` random keys from the database without
/// materializing the entire keyspace.
///
/// Algorithm: pick a random `Segment`, then reservoir-sample one slot inside
/// it (Algorithm R with reservoir size 1). Repeat until either `samples` keys
/// have been collected or the per-segment retry budget is exhausted (which
/// can happen if `volatile_only` is true and most segments contain no
/// volatile keys). The returned vector is bounded by `MAX_VICTIM_SAMPLES`.
///
/// Cost: each iteration touches one segment (≤ a few hundred slots), so the
/// total work per call is `O(samples × segment_capacity)` instead of
/// `O(total_keys)` — the previous implementation cloned every key in the
/// database into a `Vec<CompactKey>` per eviction loop iteration, which
/// dominated CPU cost on hot eviction.
fn sample_random_keys(
    db: &Database,
    samples: usize,
    volatile_only: bool,
) -> SmallVec<[CompactKey; MAX_VICTIM_SAMPLES]> {
    let table = db.data();
    let mut out: SmallVec<[CompactKey; MAX_VICTIM_SAMPLES]> = SmallVec::new();

    let seg_count = table.segment_count();
    if seg_count == 0 || table.is_empty() {
        return out;
    }
    let want = samples.min(MAX_VICTIM_SAMPLES);
    if want == 0 {
        return out;
    }

    let mut rng = rand::rng();
    // Per-segment retries: bounded so a sparse volatile keyspace cannot
    // turn this into an unbounded loop.
    let max_attempts = want.saturating_mul(8);
    let mut attempts = 0usize;

    while out.len() < want && attempts < max_attempts {
        attempts += 1;
        let seg_idx = rng.random_range(0..seg_count);
        let seg = table.segment(seg_idx);

        // Reservoir-sample one occupied slot from this segment with the
        // optional volatile filter applied. Algorithm R with k=1.
        let mut chosen: Option<&CompactKey> = None;
        let mut seen = 0u32;
        for (k, v) in seg.iter_occupied() {
            if volatile_only && !v.has_expiry() {
                continue;
            }
            seen += 1;
            if rng.random_range(0..seen) == 0 {
                chosen = Some(k);
            }
        }
        if let Some(k) = chosen {
            out.push(k.clone());
        }
    }

    out
}

/// Compare two LRU timestamps with u16 wraparound handling.
/// Uses signed-distance comparison: treats the 16-bit clock as circular.
/// Returns true if `a` is considered older (less recent) than `b`.
#[inline]
pub fn lru_is_older(a: u32, b: u32) -> bool {
    let a16 = a as i16;
    let b16 = b as i16;
    // Signed difference handles wraparound: if a was accessed before b,
    // (a16 - b16) is negative (a < b in time), meaning a is older.
    // Wraparound case: a=65400 (pre-wrap), b=100 (post-wrap):
    //   a16=-136, b16=100, -136-100=-236 < 0 → correctly identifies a as older.
    a16.wrapping_sub(b16) < 0
}

/// Sum used_memory across all databases for aggregate eviction decisions.
pub fn aggregate_used_memory(databases: &[Database]) -> usize {
    databases.iter().map(|db| db.estimated_memory()).sum()
}

/// Eviction policy variants matching Redis maxmemory-policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    NoEviction,
    AllKeysLru,
    AllKeysLfu,
    AllKeysRandom,
    VolatileLru,
    VolatileLfu,
    VolatileRandom,
    VolatileTtl,
}

impl EvictionPolicy {
    /// Parse a policy name string (case-insensitive) into an EvictionPolicy.
    ///
    /// Allocation-free: called per `try_evict_if_needed` invocation (i.e. per
    /// write command under memory pressure), so it must not build a lowercase
    /// `String` on every call.
    pub fn from_str(s: &str) -> Self {
        const TABLE: [(&str, EvictionPolicy); 7] = [
            ("allkeys-lru", EvictionPolicy::AllKeysLru),
            ("allkeys-lfu", EvictionPolicy::AllKeysLfu),
            ("allkeys-random", EvictionPolicy::AllKeysRandom),
            ("volatile-lru", EvictionPolicy::VolatileLru),
            ("volatile-lfu", EvictionPolicy::VolatileLfu),
            ("volatile-random", EvictionPolicy::VolatileRandom),
            ("volatile-ttl", EvictionPolicy::VolatileTtl),
        ];
        for (name, policy) in TABLE {
            if s.eq_ignore_ascii_case(name) {
                return policy;
            }
        }
        EvictionPolicy::NoEviction
    }

    /// Return the canonical string name for this policy.
    pub fn as_str(&self) -> &'static str {
        match self {
            EvictionPolicy::NoEviction => "noeviction",
            EvictionPolicy::AllKeysLru => "allkeys-lru",
            EvictionPolicy::AllKeysLfu => "allkeys-lfu",
            EvictionPolicy::AllKeysRandom => "allkeys-random",
            EvictionPolicy::VolatileLru => "volatile-lru",
            EvictionPolicy::VolatileLfu => "volatile-lfu",
            EvictionPolicy::VolatileRandom => "volatile-random",
            EvictionPolicy::VolatileTtl => "volatile-ttl",
        }
    }
}

/// OOM error frame returned when eviction cannot free enough memory.
fn oom_error() -> Frame {
    Frame::Error(Bytes::from_static(
        b"OOM command not allowed when used memory > 'maxmemory'",
    ))
}

/// Context for spilling evicted entries to disk instead of deleting them.
///
/// When provided to `try_evict_if_needed_with_spill`, evicted entries are
/// serialized to KvLeafPage DataFiles before being removed from RAM.
pub struct SpillContext<'a> {
    pub shard_dir: &'a Path,
    pub manifest: &'a mut ShardManifest,
    pub next_file_id: &'a mut u64,
}

/// Compute a shard's elastic memory budget from a snapshot of every shard's
/// published usage (GAP-1: hot-shard memory pooling).
///
/// `base` is the static per-shard budget (`maxmemory / num_shards`). Shards
/// under `base` donate their headroom; shards at-or-over `base` split the
/// donated surplus evenly. The aggregate invariant holds by construction:
///
/// ```text
/// Σ_hot (base + surplus/H) + Σ_under used_u
///   ≤ H·base + Σ_under (base − used_u) + Σ_under used_u  = N·base ≈ maxmemory
/// ```
///
/// so the instance-wide cap is never exceeded (modulo one tick of staleness
/// in the snapshot — the same bound the static scheme already has between
/// eviction ticks). An under-budget shard always keeps its full `base`, so
/// a shard whose load returns is never squeezed below its static share.
///
/// Returns `base` when pooling cannot help: unlimited memory, single shard,
/// this shard under budget, or no hot shard in the snapshot.
pub fn compute_elastic_budget(shard_id: usize, base: usize, used: &[usize]) -> usize {
    if base == 0 || used.len() <= 1 || shard_id >= used.len() {
        return base;
    }
    let mut surplus = 0usize;
    let mut hot = 0usize;
    for &u in used {
        if u < base {
            surplus += base - u;
        } else {
            hot += 1;
        }
    }
    if used[shard_id] < base || hot == 0 {
        return base;
    }
    base.saturating_add(surplus / hot)
}

/// Check if eviction is needed and attempt to free memory.
///
/// Returns Ok(()) if memory is within limits (or maxmemory is 0).
/// Returns Err(Frame) with OOM error if eviction fails to free enough memory.
pub fn try_evict_if_needed(db: &mut Database, config: &RuntimeConfig) -> Result<(), Frame> {
    try_evict_if_needed_with_spill(db, config, None)
}

/// Like [`try_evict_if_needed`] but enforcing an elastic per-shard budget
/// (`0` falls back to the static `maxmemory / num_shards`).
pub fn try_evict_if_needed_budget(
    db: &mut Database,
    config: &RuntimeConfig,
    budget_override: usize,
) -> Result<(), Frame> {
    try_evict_if_needed_with_spill_and_total_budget(
        db,
        config,
        None,
        db.estimated_memory(),
        budget_override,
    )
}

/// Check if eviction is needed, optionally spilling evicted entries to disk.
///
/// When `spill` is `Some`, evicted entries are written to a DataFile before
/// being removed from RAM. When `None`, behaves identically to
/// `try_evict_if_needed` (entries are simply deleted).
///
/// Spill failures are best-effort: if I/O fails, a warning is logged and the
/// entry is still removed from RAM.
pub fn try_evict_if_needed_with_spill(
    db: &mut Database,
    config: &RuntimeConfig,
    spill: Option<&mut SpillContext<'_>>,
) -> Result<(), Frame> {
    try_evict_if_needed_with_spill_and_total(db, config, spill, db.estimated_memory())
}

/// Eviction with explicit total_memory parameter (for aggregate checking).
///
/// When called from the memory pressure cascade, `total_memory` should be the
/// aggregate across all databases. When called from the connection handler,
/// pass `db.estimated_memory()` for single-DB behavior (Redis-compatible).
pub fn try_evict_if_needed_with_spill_and_total(
    db: &mut Database,
    config: &RuntimeConfig,
    spill: Option<&mut SpillContext<'_>>,
    total_memory: usize,
) -> Result<(), Frame> {
    try_evict_if_needed_with_spill_and_total_budget(db, config, spill, total_memory, 0)
}

/// Core sync eviction loop with an elastic budget override (GAP-1).
///
/// `budget_override == 0` keeps the static `maxmemory / num_shards`
/// threshold; a non-zero value (from
/// `ShardDatabases::recompute_elastic_budget`) lets a hot shard borrow
/// headroom donated by under-budget siblings before evicting.
pub fn try_evict_if_needed_with_spill_and_total_budget(
    db: &mut Database,
    config: &RuntimeConfig,
    mut spill: Option<&mut SpillContext<'_>>,
    total_memory: usize,
    budget_override: usize,
) -> Result<(), Frame> {
    if config.maxmemory == 0 {
        return Ok(());
    }

    let policy = EvictionPolicy::from_str(&config.maxmemory_policy);

    // Compare against the PER-SHARD budget, not the whole-instance maxmemory.
    // `maxmemory` is a whole-instance cap; each shard enforces independently, so
    // the threshold is `maxmemory / num_shards` (single shard ⇒ unchanged). This
    // is what bounds aggregate RSS in multishard mode. An elastic override
    // (capped at the instance maxmemory) widens the threshold for hot shards.
    let budget = if budget_override > 0 {
        budget_override.min(config.maxmemory)
    } else {
        config.maxmemory_per_shard()
    };
    let mut current_total = total_memory;
    while current_total > budget {
        if policy == EvictionPolicy::NoEviction {
            return Err(oom_error());
        }
        let before = db.estimated_memory();
        if !evict_one_with_spill(db, config, &policy, spill.as_deref_mut()) {
            return Err(oom_error());
        }
        let after = db.estimated_memory();
        current_total = current_total.saturating_sub(before.saturating_sub(after));
    }

    Ok(())
}

/// Seed value for a shard's spill `file_id` counter after recovery.
///
/// On restart, AOF/RDB replay re-populates the hot tier and the persistence
/// cascade re-evicts the excess. If the counter restarted at 1 it would mint
/// `heap-000001.mpf`, `heap-000257.mpf`, … — the *same names* recovery just
/// loaded — and atomically overwrite cold files the rebuilt `cold_index` still
/// references, silently corrupting cold read-through (the B-2 bug).
///
/// Scanning the on-disk `<shard_dir>/data/heap-NNNNNN.mpf` files (rather than
/// the manifest) is deliberate: a spill that wrote its `.mpf` but crashed
/// before the manifest commit leaves an orphan the manifest doesn't know about,
/// yet it still occupies a filename that must not be clobbered. The physical
/// files are the authority on what can be overwritten.
///
/// Returns `max(existing file_id) + 1`. Because each batch file is named after
/// its first request's `file_id` (gaps are harmless — recovery iterates
/// manifest entries, see `spill_thread`), any seed strictly above the current
/// max guarantees every future filename is new. Returns `1` when disk-offload
/// is off, the directory is absent, or it holds no heap files — identical to
/// the historical default, so a fresh server and the live hot path are
/// unchanged by this seeding.
#[must_use]
pub fn next_spill_file_id_seed(shard_dir: Option<&Path>) -> u64 {
    let Some(dir) = shard_dir else { return 1 };
    let data_dir = dir.join("data");
    let entries = match std::fs::read_dir(&data_dir) {
        Ok(e) => e,
        Err(e) => {
            // NotFound is the normal fresh-server case (data/ not created yet).
            // Anything else (permissions, I/O) is anomalous: fail safe to the
            // legacy default but surface it so a misseed can't hide silently.
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    dir = %data_dir.display(),
                    error = %e,
                    "spill file_id seed: could not scan cold dir; defaulting to 1"
                );
            }
            return 1;
        }
    };
    let mut max_id: Option<u64> = None;
    for entry in entries.flatten() {
        let name = entry.file_name();
        if let Some(id) = name
            .to_str()
            .and_then(|n| n.strip_prefix("heap-"))
            .and_then(|r| r.strip_suffix(".mpf"))
            .and_then(|s| s.parse::<u64>().ok())
        {
            max_id = Some(max_id.map_or(id, |m| m.max(id)));
        }
    }
    match max_id {
        Some(m) => m + 1,
        None => 1,
    }
}

/// Evict over-budget entries, spilling them to disk asynchronously via a
/// background `SpillThread` instead of doing a synchronous pwrite on the event
/// loop.
///
/// Per victim (`evict_one_async_spill`): serialize the key/value into a
/// `SpillRequest`, **`try_send` it to the spill channel BEFORE removing the
/// entry from the hot tier, and only remove from RAM once the send succeeds.**
/// This ordering is fail-safe: if the channel is full or disconnected the send
/// fails, the key stays in RAM, and the eviction loop bails out to retry on the
/// next tick — no acknowledged data is lost. The worst case under sustained
/// backpressure is keys remaining resident (eventually an OOM write-rejection
/// once at budget), which is correct behaviour, not data loss.
///
/// Callers must poll `SpillThread::drain_completions()` to apply the manifest
/// and `ColdIndex` updates produced by completed spills.
pub fn try_evict_if_needed_async_spill(
    db: &mut Database,
    config: &RuntimeConfig,
    sender: &flume::Sender<SpillRequest>,
    shard_dir: &Path,
    next_file_id: &mut u64,
    db_index: usize,
) -> Result<(), Frame> {
    try_evict_if_needed_async_spill_with_total(
        db,
        config,
        sender,
        shard_dir,
        next_file_id,
        db.estimated_memory(),
        db_index,
    )
}

/// Async spill eviction with explicit total_memory parameter.
pub fn try_evict_if_needed_async_spill_with_total(
    db: &mut Database,
    config: &RuntimeConfig,
    sender: &flume::Sender<SpillRequest>,
    shard_dir: &Path,
    next_file_id: &mut u64,
    total_memory: usize,
    db_index: usize,
) -> Result<(), Frame> {
    try_evict_if_needed_async_spill_with_total_budget(
        db,
        config,
        sender,
        shard_dir,
        next_file_id,
        total_memory,
        db_index,
        0,
    )
}

/// Like [`try_evict_if_needed_async_spill`] but enforcing an elastic
/// per-shard budget (`0` falls back to the static `maxmemory / num_shards`).
pub fn try_evict_if_needed_async_spill_budget(
    db: &mut Database,
    config: &RuntimeConfig,
    sender: &flume::Sender<SpillRequest>,
    shard_dir: &Path,
    next_file_id: &mut u64,
    db_index: usize,
    budget_override: usize,
) -> Result<(), Frame> {
    try_evict_if_needed_async_spill_with_total_budget(
        db,
        config,
        sender,
        shard_dir,
        next_file_id,
        db.estimated_memory(),
        db_index,
        budget_override,
    )
}

/// Async spill eviction with an elastic budget override (GAP-1; see
/// `try_evict_if_needed_with_spill_and_total_budget`).
#[allow(clippy::too_many_arguments)]
pub fn try_evict_if_needed_async_spill_with_total_budget(
    db: &mut Database,
    config: &RuntimeConfig,
    sender: &flume::Sender<SpillRequest>,
    shard_dir: &Path,
    next_file_id: &mut u64,
    total_memory: usize,
    db_index: usize,
    budget_override: usize,
) -> Result<(), Frame> {
    if config.maxmemory == 0 {
        return Ok(());
    }

    let policy = EvictionPolicy::from_str(&config.maxmemory_policy);

    // Per-shard budget (see `try_evict_if_needed_with_spill_and_total_budget`).
    let budget = if budget_override > 0 {
        budget_override.min(config.maxmemory)
    } else {
        config.maxmemory_per_shard()
    };
    let mut current_total = total_memory;
    while current_total > budget {
        if policy == EvictionPolicy::NoEviction {
            return Err(oom_error());
        }
        let before = db.estimated_memory();
        if !evict_one_async_spill(
            db,
            config,
            &policy,
            sender,
            shard_dir,
            next_file_id,
            db_index,
        ) {
            return Err(oom_error());
        }
        let after = db.estimated_memory();
        current_total = current_total.saturating_sub(before.saturating_sub(after));
    }

    Ok(())
}

/// Evict a single key via the async spill path.
///
/// Extracts the entry, removes it from DashTable (immediate RAM relief),
/// then sends a SpillRequest to the background thread for pwrite.
fn evict_one_async_spill(
    db: &mut Database,
    config: &RuntimeConfig,
    policy: &EvictionPolicy,
    sender: &flume::Sender<SpillRequest>,
    shard_dir: &Path,
    next_file_id: &mut u64,
    db_index: usize,
) -> bool {
    // Find victim key using same policy logic as sync path
    let victim = match policy {
        EvictionPolicy::NoEviction => None,
        EvictionPolicy::AllKeysLru => find_victim_lru(db, config.maxmemory_samples, false),
        EvictionPolicy::AllKeysLfu => {
            find_victim_lfu(db, config.maxmemory_samples, config.lfu_decay_time, false)
        }
        EvictionPolicy::AllKeysRandom => find_victim_random(db, false),
        EvictionPolicy::VolatileLru => find_victim_lru(db, config.maxmemory_samples, true),
        EvictionPolicy::VolatileLfu => {
            find_victim_lfu(db, config.maxmemory_samples, config.lfu_decay_time, true)
        }
        EvictionPolicy::VolatileRandom => find_victim_random(db, true),
        EvictionPolicy::VolatileTtl => find_victim_volatile_ttl(db, config.maxmemory_samples),
    };

    let key = match victim {
        Some(k) => k,
        None => return false,
    };

    // Build SpillRequest from the entry BEFORE removing it from DashTable.
    // This is CPU work only -- no I/O on the event loop.
    if let Some(entry) = db.data().get(key.as_bytes()) {
        let val_ref = entry.as_redis_value();

        // Determine value_type and serialize value bytes
        let collection_buf: Vec<u8>;
        let (value_type, value_bytes): (ValueType, &[u8]) = match val_ref {
            RedisValueRef::String(s) => (ValueType::String, s),
            ref other => {
                let vt = match other {
                    RedisValueRef::Hash(_)
                    | RedisValueRef::HashListpack(_)
                    | RedisValueRef::HashWithTtl { .. } => ValueType::Hash,
                    RedisValueRef::List(_) | RedisValueRef::ListListpack(_) => ValueType::List,
                    RedisValueRef::Set(_)
                    | RedisValueRef::SetListpack(_)
                    | RedisValueRef::SetIntset(_) => ValueType::Set,
                    RedisValueRef::SortedSet { .. }
                    | RedisValueRef::SortedSetBPTree { .. }
                    | RedisValueRef::SortedSetListpack(_) => ValueType::ZSet,
                    RedisValueRef::Stream(_) => ValueType::Stream,
                    RedisValueRef::String(_) => unreachable!(),
                };
                collection_buf = kv_serde::serialize_collection(other).unwrap_or_default();
                (vt, collection_buf.as_slice())
            }
        };

        // Determine flags and TTL
        let mut flags: u8 = 0;
        let ttl_ms = if entry.has_expiry() {
            flags |= entry_flags::HAS_TTL;
            Some(entry.expires_at_ms(0))
        } else {
            None
        };

        let file_id = *next_file_id;
        *next_file_id += 1;

        let req = SpillRequest {
            key: Bytes::copy_from_slice(key.as_bytes()),
            db_index,
            value_bytes: Bytes::copy_from_slice(value_bytes),
            value_type,
            flags,
            ttl_ms,
            file_id,
            shard_dir: PathBuf::from(shard_dir),
        };

        // CRITICAL: queue the spill BEFORE freeing RAM. If try_send fails
        // (channel full or disconnected) we MUST NOT remove the entry — that
        // would lose data because no completion will arrive and the file will
        // not exist. Bail out and let the next eviction tick retry.
        if sender.try_send(req).is_err() {
            return false;
        }

        // Now safe to free RAM. The bg thread holds the SpillRequest and will
        // produce a SpillCompletion that updates cold_index for this db_index.
        db.remove(key.as_bytes());

        // NOTE: we do NOT insert a tentative cold_index entry here.
        // Under batching the (page_idx, slot_idx) are unknown at evict time;
        // slot 0 would return a *different* key's value in the pre-flush window.
        // Accept a brief read-miss until the completion applies — the key is
        // safe: it is in the SpillRequest and will be registered once the bg
        // thread writes and the event loop processes the SpillCompletion.
        // AOF incr log is the durability backstop for the pre-flush window.
    } else {
        // Entry disappeared (race with expiry), just remove
        db.remove(key.as_bytes());
    }

    true
}

/// Evict a single key, optionally spilling to disk before removal.
fn evict_one_with_spill(
    db: &mut Database,
    config: &RuntimeConfig,
    policy: &EvictionPolicy,
    spill: Option<&mut SpillContext<'_>>,
) -> bool {
    // Find victim key using policy-specific sampling
    let victim = match policy {
        EvictionPolicy::NoEviction => None,
        EvictionPolicy::AllKeysLru => find_victim_lru(db, config.maxmemory_samples, false),
        EvictionPolicy::AllKeysLfu => {
            find_victim_lfu(db, config.maxmemory_samples, config.lfu_decay_time, false)
        }
        EvictionPolicy::AllKeysRandom => find_victim_random(db, false),
        EvictionPolicy::VolatileLru => find_victim_lru(db, config.maxmemory_samples, true),
        EvictionPolicy::VolatileLfu => {
            find_victim_lfu(db, config.maxmemory_samples, config.lfu_decay_time, true)
        }
        EvictionPolicy::VolatileRandom => find_victim_random(db, true),
        EvictionPolicy::VolatileTtl => find_victim_volatile_ttl(db, config.maxmemory_samples),
    };

    let key = match victim {
        Some(k) => k,
        None => return false,
    };

    // Spill to disk before removing, if context provided
    if let Some(ctx) = spill {
        if let Some(entry) = db.data().get(key.as_bytes()) {
            // Only spill string entries (collection types not yet supported)
            let is_string = matches!(entry.as_redis_value(), RedisValueRef::String(_));
            if is_string {
                if let Err(e) = kv_spill::spill_to_datafile(
                    ctx.shard_dir,
                    *ctx.next_file_id,
                    key.as_bytes(),
                    entry,
                    ctx.manifest,
                    None,
                ) {
                    warn!(
                        key = %String::from_utf8_lossy(key.as_bytes()),
                        error = %e,
                        "kv_spill: I/O error during spill, proceeding with eviction"
                    );
                } else {
                    *ctx.next_file_id += 1;
                }
            }
        }
    }

    db.remove(key.as_bytes());
    crate::admin::metrics_setup::record_eviction();
    true
}

// ── Victim selection helpers ───────────────────────────

/// Find the victim key with the oldest last_access from a random sample.
fn find_victim_lru(db: &Database, samples: usize, volatile_only: bool) -> Option<CompactKey> {
    let sampled = sample_random_keys(db, samples, volatile_only);
    if sampled.is_empty() {
        return None;
    }

    let mut oldest_key: Option<CompactKey> = None;
    let mut oldest_access: Option<u32> = None;

    for key in sampled.iter() {
        if let Some(entry) = db.data().get(key.as_bytes()) {
            let la = entry.last_access();
            match oldest_access {
                None => {
                    oldest_key = Some(key.clone());
                    oldest_access = Some(la);
                }
                Some(oldest) => {
                    if lru_is_older(la, oldest) {
                        oldest_key = Some(key.clone());
                        oldest_access = Some(la);
                    }
                }
            }
        }
    }

    oldest_key
}

/// Find the victim key with the lowest LFU counter from a random sample.
fn find_victim_lfu(
    db: &Database,
    samples: usize,
    lfu_decay_time: u64,
    volatile_only: bool,
) -> Option<CompactKey> {
    let sampled = sample_random_keys(db, samples, volatile_only);
    if sampled.is_empty() {
        return None;
    }

    let mut evict_key: Option<CompactKey> = None;
    let mut lowest_counter: Option<u8> = None;
    let mut oldest_access_for_tie: Option<u32> = None;

    for key in sampled.iter() {
        if let Some(entry) = db.data().get(key.as_bytes()) {
            let effective_counter =
                lfu_decay(entry.access_counter(), entry.last_access(), lfu_decay_time);

            let la = entry.last_access();
            let should_evict = match lowest_counter {
                None => true,
                Some(lowest) => {
                    effective_counter < lowest
                        || (effective_counter == lowest
                            && oldest_access_for_tie.map_or(true, |t| lru_is_older(la, t)))
                }
            };

            if should_evict {
                evict_key = Some(key.clone());
                lowest_counter = Some(effective_counter);
                oldest_access_for_tie = Some(la);
            }
        }
    }

    evict_key
}

/// Find a random victim key.
fn find_victim_random(db: &Database, volatile_only: bool) -> Option<CompactKey> {
    sample_random_keys(db, 1, volatile_only).into_iter().next()
}

/// Find the victim key with the soonest TTL expiration from a random sample.
fn find_victim_volatile_ttl(db: &Database, samples: usize) -> Option<CompactKey> {
    let sampled = sample_random_keys(db, samples, true);
    if sampled.is_empty() {
        return None;
    }

    let mut evict_key: Option<CompactKey> = None;
    let mut soonest_expiry: Option<u64> = None;

    for key in sampled.iter() {
        if let Some(entry) = db.data().get(key.as_bytes()) {
            if entry.has_expiry() {
                let exp = entry.expires_at_ms(db.base_timestamp());
                let should_evict = match soonest_expiry {
                    None => true,
                    Some(soonest) => exp < soonest,
                };
                if should_evict {
                    evict_key = Some(key.clone());
                    soonest_expiry = Some(exp);
                }
            }
        }
    }

    evict_key
}

#[cfg(test)]
mod tests {
    // Legacy wrappers used only in tests for backward-compatible assertions.
    fn evict_one_random(db: &mut super::Database, volatile_only: bool) -> bool {
        if let Some(key) = super::find_victim_random(db, volatile_only) {
            db.remove(key.as_bytes());
            true
        } else {
            false
        }
    }

    fn evict_one_volatile_ttl(db: &mut super::Database, samples: usize) -> bool {
        if let Some(key) = super::find_victim_volatile_ttl(db, samples) {
            db.remove(key.as_bytes());
            true
        } else {
            false
        }
    }

    use super::*;
    use crate::persistence::kv_page::read_datafile;
    use crate::persistence::manifest::ShardManifest;
    use crate::storage::entry::{Entry, current_secs, current_time_ms};

    // -----------------------------------------------------------------
    // compute_elastic_budget (GAP-1)
    // -----------------------------------------------------------------

    #[test]
    fn elastic_budget_balanced_load_keeps_base() {
        // Every shard at base — no surplus to borrow.
        assert_eq!(compute_elastic_budget(0, 100, &[100, 100, 100, 100]), 100);
    }

    #[test]
    fn elastic_budget_under_budget_shard_keeps_base() {
        // An under-budget shard never borrows (and never shrinks below base).
        assert_eq!(compute_elastic_budget(1, 100, &[100, 10, 100, 100]), 100);
    }

    #[test]
    fn elastic_budget_single_hot_shard_takes_all_surplus() {
        // Shard 0 hot, three idle siblings donate (100-10)*3 = 270.
        assert_eq!(compute_elastic_budget(0, 100, &[100, 10, 10, 10]), 370);
    }

    #[test]
    fn elastic_budget_two_hot_shards_split_surplus() {
        // Two hot shards split (100-10)*2 = 180 → +90 each.
        assert_eq!(compute_elastic_budget(0, 100, &[150, 10, 10, 120]), 190);
        assert_eq!(compute_elastic_budget(3, 100, &[150, 10, 10, 120]), 190);
    }

    #[test]
    fn elastic_budget_snapshot_aggregate_never_exceeds_total() {
        // Snapshot invariant: Σ_hot budget_i + Σ_under used_i ≤ N·base.
        // (A donor growing back is re-balanced on the next 100ms tick — the
        // hot shard's budget shrinks with the surplus and write-path
        // eviction pulls it back down, so transient overshoot is bounded by
        // one tick of donor write growth, same class as the static scheme's
        // between-tick slack.)
        let base = 100usize;
        for used in [
            vec![400, 0, 0, 0],
            vec![150, 150, 0, 0],
            vec![100, 100, 100, 100],
            vec![0, 0, 0, 0],
            vec![350, 99, 1, 0],
        ] {
            let n = used.len();
            let total: usize = (0..n)
                .map(|i| {
                    if used[i] >= base {
                        compute_elastic_budget(i, base, &used)
                    } else {
                        used[i]
                    }
                })
                .sum();
            assert!(
                total <= n * base,
                "snapshot aggregate {total} > cap {} for {used:?}",
                n * base
            );
        }
    }

    #[test]
    fn elastic_budget_degenerate_inputs_fall_back_to_base() {
        assert_eq!(compute_elastic_budget(0, 0, &[10, 10]), 0); // unlimited
        assert_eq!(compute_elastic_budget(0, 100, &[500]), 100); // single shard
        assert_eq!(compute_elastic_budget(7, 100, &[100, 0]), 100); // bad index
    }

    #[test]
    fn elastic_budget_override_widens_eviction_threshold() {
        // A db over the static budget but under the override must NOT evict;
        // override 0 must evict down to the static threshold.
        let mut db = Database::new();
        let mut config = make_config(20_000, "allkeys-lru");
        config.num_shards = 4; // static per-shard budget = 5000
        for i in 0..50 {
            let key = bytes::Bytes::from(format!("k{i}"));
            let entry = Entry::new_string(bytes::Bytes::from(vec![0u8; 100]));
            db.set(key, entry);
        }
        let used = db.estimated_memory();
        assert!(
            used > 5_000 && used < 20_000,
            "fixture must sit between static budget and maxmemory, got {used}"
        );

        // Elastic override above current usage: no eviction.
        let keys_before = db.len();
        try_evict_if_needed_with_spill_and_total_budget(&mut db, &config, None, used, used + 1024)
            .expect("within elastic budget must not OOM");
        assert_eq!(db.len(), keys_before, "no eviction under elastic budget");

        // Static budget (override 0): must evict below 5000 bytes.
        let used_now = db.estimated_memory();
        try_evict_if_needed_with_spill_and_total_budget(&mut db, &config, None, used_now, 0)
            .expect("eviction should succeed");
        assert!(
            db.estimated_memory() <= 5_000,
            "static budget must evict down to maxmemory/num_shards"
        );
        assert!(db.len() < keys_before);
    }

    #[test]
    fn elastic_budget_override_is_capped_at_instance_maxmemory() {
        // Override beyond maxmemory clamps: usage above maxmemory must evict.
        let mut db = Database::new();
        let config = make_config(500, "allkeys-lru");
        for i in 0..50 {
            let key = bytes::Bytes::from(format!("k{i}"));
            let entry = Entry::new_string(bytes::Bytes::from(vec![0u8; 100]));
            db.set(key, entry);
        }
        let used = db.estimated_memory();
        assert!(used > 500);
        try_evict_if_needed_with_spill_and_total_budget(&mut db, &config, None, used, usize::MAX)
            .expect("eviction should succeed");
        assert!(
            db.estimated_memory() <= 500,
            "override must clamp at instance maxmemory"
        );
    }

    fn make_config(maxmemory: usize, policy: &str) -> RuntimeConfig {
        RuntimeConfig {
            maxmemory,
            maxmemory_policy: policy.to_string(),
            maxmemory_samples: 5,
            lfu_log_factor: 10,
            lfu_decay_time: 1,
            save: None,
            appendonly: "no".to_string(),
            appendfsync: "everysec".to_string(),
            aclfile: None,
            dir: ".".to_string(),
            requirepass: None,
            protected_mode: "yes".to_string(),
            acllog_max_len: 128,
            client_pause_deadline_ms: 0,
            client_pause_write_only: false,
            lazyfree_threshold: 64,
            maxclients: 10000,
            timeout: 0,
            tcp_keepalive: 300,
            num_shards: 1,
        }
    }

    #[test]
    fn per_shard_budget_divides_maxmemory_at_enforcement() {
        // Regression for the multishard "zombie RAM" bug: maxmemory is a
        // whole-instance cap, but each shard enforces eviction independently.
        // Without per-shard division an N-shard server tolerates ~N×maxmemory.
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));
        db.set_string(Bytes::from_static(b"k2"), Bytes::from_static(b"v2"));
        db.set_string(Bytes::from_static(b"k3"), Bytes::from_static(b"v3"));
        db.set_string(Bytes::from_static(b"k4"), Bytes::from_static(b"v4"));
        let total = db.estimated_memory();
        assert!(total > 0);

        // Whole-instance cap == current memory, single shard: per-shard budget
        // equals `total`, so nothing is evicted.
        let mut cfg1 = make_config(total, "allkeys-lru");
        cfg1.num_shards = 1;
        assert!(try_evict_if_needed(&mut db, &cfg1).is_ok());
        assert_eq!(
            db.len(),
            4,
            "single shard: nothing evicted at budget == total"
        );

        // SAME whole-instance cap, 4 shards: per-shard budget = ceil(total/4) <<
        // total, so this shard MUST shed keys until it is under the per-shard
        // budget. Pre-fix (comparison vs raw maxmemory) this evicted nothing.
        let mut cfg4 = make_config(total, "allkeys-lru");
        cfg4.num_shards = 4;
        assert!(try_evict_if_needed(&mut db, &cfg4).is_ok());
        assert!(
            db.len() < 4,
            "4 shards: per-shard budget must force eviction (got len {})",
            db.len()
        );
        assert!(
            db.estimated_memory() <= cfg4.maxmemory_per_shard(),
            "evicted below the per-shard budget"
        );
    }

    #[test]
    fn test_eviction_policy_from_str() {
        assert_eq!(
            EvictionPolicy::from_str("noeviction"),
            EvictionPolicy::NoEviction
        );
        assert_eq!(
            EvictionPolicy::from_str("allkeys-lru"),
            EvictionPolicy::AllKeysLru
        );
        assert_eq!(
            EvictionPolicy::from_str("allkeys-lfu"),
            EvictionPolicy::AllKeysLfu
        );
        assert_eq!(
            EvictionPolicy::from_str("allkeys-random"),
            EvictionPolicy::AllKeysRandom
        );
        assert_eq!(
            EvictionPolicy::from_str("volatile-lru"),
            EvictionPolicy::VolatileLru
        );
        assert_eq!(
            EvictionPolicy::from_str("volatile-lfu"),
            EvictionPolicy::VolatileLfu
        );
        assert_eq!(
            EvictionPolicy::from_str("volatile-random"),
            EvictionPolicy::VolatileRandom
        );
        assert_eq!(
            EvictionPolicy::from_str("volatile-ttl"),
            EvictionPolicy::VolatileTtl
        );
        assert_eq!(
            EvictionPolicy::from_str("ALLKEYS-LRU"),
            EvictionPolicy::AllKeysLru
        );
        assert_eq!(
            EvictionPolicy::from_str("unknown"),
            EvictionPolicy::NoEviction
        );
    }

    #[test]
    fn test_noeviction_returns_oom() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"value"));
        let config = make_config(1, "noeviction");
        let result = try_evict_if_needed(&mut db, &config);
        assert!(result.is_err());
        match result.unwrap_err() {
            Frame::Error(msg) => {
                assert!(msg.starts_with(b"OOM"));
            }
            _ => panic!("Expected error frame"),
        }
    }

    #[test]
    fn test_no_eviction_when_unlimited() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"value"));
        let config = make_config(0, "allkeys-lru");
        assert!(try_evict_if_needed(&mut db, &config).is_ok());
    }

    #[test]
    fn test_no_eviction_when_under_limit() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"value"));
        let config = make_config(1_000_000, "allkeys-lru");
        assert!(try_evict_if_needed(&mut db, &config).is_ok());
        assert_eq!(db.len(), 1);
    }

    #[test]
    fn test_lru_evicts_oldest() {
        // `sample_random_keys` reservoir-samples `maxmemory_samples` victims
        // per eviction round using a non-deterministic RNG, so a single
        // eviction call over a tiny 3-key population is statistically flaky:
        // with probability ~(2/3)^5 ≈ 13% the oldest key is never sampled in
        // that round and a different key is evicted. We instead drive
        // eviction in a bounded loop, shrinking maxmemory after each round,
        // so "old" is eventually guaranteed to be sampled and picked
        // (worst case once the population shrinks to a single key).
        let mut db = Database::new();
        let mut entry1 = Entry::new_string(Bytes::from_static(b"val1"));
        entry1.set_last_access(current_secs() - 100);
        db.set(Bytes::from_static(b"old"), entry1);

        let mut entry2 = Entry::new_string(Bytes::from_static(b"val2"));
        entry2.set_last_access(current_secs() - 50);
        db.set(Bytes::from_static(b"medium"), entry2);

        let mut entry3 = Entry::new_string(Bytes::from_static(b"val3"));
        entry3.set_last_access(current_secs());
        db.set(Bytes::from_static(b"new"), entry3);

        // Drive eviction rounds until "old" is gone, bounded to prevent
        // infinite looping if the sampler is broken.
        for _ in 0..50 {
            if db.data().get(b"old" as &[u8]).is_none() {
                break;
            }
            let mem = db.estimated_memory();
            if mem == 0 {
                break;
            }
            let config = make_config(mem.saturating_sub(1), "allkeys-lru");
            let result = try_evict_if_needed(&mut db, &config);
            assert!(result.is_ok());
        }
        assert!(
            db.data().get(b"old" as &[u8]).is_none(),
            "LRU eviction failed to remove the oldest key within 50 rounds",
        );
    }

    #[test]
    fn test_allkeys_random_evicts() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));
        db.set_string(Bytes::from_static(b"k2"), Bytes::from_static(b"v2"));
        db.set_string(Bytes::from_static(b"k3"), Bytes::from_static(b"v3"));

        let config = make_config(1, "allkeys-random");
        let result = try_evict_if_needed(&mut db, &config);
        assert!(result.is_ok());
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn test_volatile_only_skips_persistent() {
        let mut db = Database::new();
        db.set_string(
            Bytes::from_static(b"persistent"),
            Bytes::from_static(b"value"),
        );
        let future_ms = current_time_ms() + 3_600_000;
        db.set_string_with_expiry(
            Bytes::from_static(b"volatile"),
            Bytes::from_static(b"value"),
            future_ms,
        );

        let result = evict_one_random(&mut db, true);
        assert!(result);
        assert_eq!(db.len(), 1);
        assert!(db.data().contains_key(b"persistent" as &[u8]));
    }

    #[test]
    fn test_volatile_ttl_evicts_soonest() {
        // `sample_random_keys` reservoir-samples across DashTable segments
        // with a non-deterministic RNG, so over a tiny 2-key population a
        // single eviction round can fail to sample `soon` and pick `later`
        // instead. Same flake pattern as `test_lru_evicts_oldest`: drive
        // eviction in a bounded loop so `soon` is guaranteed to be sampled
        // (worst case once the population shrinks to one key).
        let mut db = Database::new();
        let now_ms = current_time_ms();
        db.set_string_with_expiry(
            Bytes::from_static(b"soon"),
            Bytes::from_static(b"v"),
            now_ms + 10_000,
        );
        db.set_string_with_expiry(
            Bytes::from_static(b"later"),
            Bytes::from_static(b"v"),
            now_ms + 3_600_000,
        );

        let mut evictions = 0;
        for _ in 0..50 {
            if db.data().get(b"soon" as &[u8]).is_none() {
                break;
            }
            if !evict_one_volatile_ttl(&mut db, 5) {
                break;
            }
            evictions += 1;
            // Re-insert the wrongly-evicted `later` so we always have two
            // candidates until the algorithm picks `soon`. This proves the
            // sampler eventually selects the soonest-expiring key.
            if db.data().get(b"later" as &[u8]).is_none()
                && db.data().get(b"soon" as &[u8]).is_some()
            {
                db.set_string_with_expiry(
                    Bytes::from_static(b"later"),
                    Bytes::from_static(b"v"),
                    now_ms + 3_600_000,
                );
            }
        }
        assert!(
            db.data().get(b"soon" as &[u8]).is_none(),
            "expected `soon` evicted within bounded rounds (evictions={evictions})"
        );
    }

    #[test]
    fn test_policy_as_str() {
        assert_eq!(EvictionPolicy::NoEviction.as_str(), "noeviction");
        assert_eq!(EvictionPolicy::AllKeysLru.as_str(), "allkeys-lru");
        assert_eq!(EvictionPolicy::VolatileTtl.as_str(), "volatile-ttl");
    }

    #[test]
    fn test_evict_with_spill_creates_datafile() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut next_file_id = 1u64;

        let mut db = Database::new();
        db.set_string(
            Bytes::from_static(b"spill_key"),
            Bytes::from_static(b"spill_val"),
        );

        let config = make_config(1, "allkeys-lru");
        let mut ctx = SpillContext {
            shard_dir,
            manifest: &mut manifest,
            next_file_id: &mut next_file_id,
        };

        let result = try_evict_if_needed_with_spill(&mut db, &config, Some(&mut ctx));
        assert!(result.is_ok());
        assert_eq!(db.len(), 0);

        // Verify DataFile was created
        let file_path = shard_dir.join("data/heap-000001.mpf");
        assert!(file_path.exists(), "DataFile should have been created");

        // Verify contents
        let pages = read_datafile(&file_path).unwrap();
        assert_eq!(pages.len(), 1);
        let entry = pages[0].get(0).unwrap();
        assert_eq!(entry.key, b"spill_key");
        assert_eq!(entry.value, b"spill_val");

        // file_id should have been incremented
        assert_eq!(next_file_id, 2);
    }

    /// Fail-safe send-before-remove: when the async-spill request channel is
    /// FULL, eviction must NOT remove the victim from the hot tier (no data
    /// loss) and must surface OOM rather than pretend success. The live path
    /// (`evict_one_async_spill`) `try_send`s the `SpillRequest` BEFORE
    /// `db.remove`, so a failed send leaves the key resident for retry on the
    /// next eviction tick. This regression guard locks that ordering — the exact
    /// request-side data-loss scenario CodeRabbit raised on PR #144 (verified
    /// stale against the live code; this test keeps it that way).
    #[test]
    fn async_spill_full_channel_keeps_victim_in_ram_no_data_loss() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let mut next_file_id = 1u64;

        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));

        // Per-shard budget of 1 byte forces an eviction attempt for k1.
        let config = make_config(1, "allkeys-lru");

        // bounded(1) PRE-FILLED so the eviction's `try_send` observes a FULL
        // channel (the reviewer's backpressure scenario) rather than a
        // disconnect — both fail `try_send`, but full is the precise concern.
        let (tx, _rx) = flume::bounded::<SpillRequest>(1);
        tx.try_send(SpillRequest {
            key: Bytes::from_static(b"filler"),
            db_index: 0,
            value_bytes: Bytes::from_static(b"x"),
            value_type: ValueType::String,
            flags: 0,
            ttl_ms: None,
            file_id: 0,
            shard_dir: shard_dir.to_path_buf(),
        })
        .expect("filler occupies the single channel slot");

        let result =
            try_evict_if_needed_async_spill(&mut db, &config, &tx, shard_dir, &mut next_file_id, 0);

        // Could not enqueue the spill → OOM surfaced, never a false success.
        assert!(
            result.is_err(),
            "a full spill channel must surface OOM, never silently succeed"
        );
        // CRUCIAL: the victim is still resident — no acknowledged-write data loss.
        assert_eq!(
            db.len(),
            1,
            "victim must NOT be removed from RAM when the spill send fails"
        );
        assert!(
            db.data().get(&b"k1"[..]).is_some(),
            "k1 must remain in the hot tier for retry on the next eviction tick"
        );
    }

    #[test]
    fn test_evict_without_spill_unchanged() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));
        db.set_string(Bytes::from_static(b"k2"), Bytes::from_static(b"v2"));

        let config = make_config(1, "allkeys-random");
        let result = try_evict_if_needed_with_spill(&mut db, &config, None);
        assert!(result.is_ok());
        assert_eq!(db.len(), 0);
    }

    // ── B-2: post-crash cold-spill file_id seeding ──────────────────────────
    // The spill file_id counter must resume ABOVE every recovered `heap-*.mpf`
    // so post-restart re-eviction never overwrites a cold file the rebuilt
    // cold_index still points at (which silently corrupts cold read-through).

    /// `None` dir (disk-offload off) → seed 1 (historical default, no change).
    #[test]
    fn test_spill_seed_none_dir_is_one() {
        assert_eq!(next_spill_file_id_seed(None), 1);
    }

    /// Fresh server (dir absent / empty) → seed 1 == legacy behaviour, so the
    /// live read-through hot path cannot regress from this change.
    #[test]
    fn test_spill_seed_fresh_server_is_one() {
        let tmp = tempfile::tempdir().unwrap();
        // data/ does not exist yet
        assert_eq!(next_spill_file_id_seed(Some(tmp.path())), 1);
        // data/ exists but empty
        std::fs::create_dir_all(tmp.path().join("data")).unwrap();
        assert_eq!(next_spill_file_id_seed(Some(tmp.path())), 1);
    }

    /// With recovered heap files, seed = max(file_id) + 1 so the next batch's
    /// filename is strictly greater than any existing one (filenames are the
    /// first request id of each batch; gaps are harmless).
    #[test]
    fn test_spill_seed_resumes_above_max_recovered() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        for id in [1u64, 257, 513] {
            std::fs::write(data.join(format!("heap-{id:06}.mpf")), b"x").unwrap();
        }
        // non-heap files and bad names must be ignored
        std::fs::write(data.join("manifest.bin"), b"x").unwrap();
        std::fs::write(data.join("heap-notanum.mpf"), b"x").unwrap();
        std::fs::write(data.join("base-000999.rdb"), b"x").unwrap();
        assert_eq!(next_spill_file_id_seed(Some(tmp.path())), 514);
    }
}
