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
#[derive(Debug, Clone, PartialEq)]
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
    pub fn from_str(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "allkeys-lru" => EvictionPolicy::AllKeysLru,
            "allkeys-lfu" => EvictionPolicy::AllKeysLfu,
            "allkeys-random" => EvictionPolicy::AllKeysRandom,
            "volatile-lru" => EvictionPolicy::VolatileLru,
            "volatile-lfu" => EvictionPolicy::VolatileLfu,
            "volatile-random" => EvictionPolicy::VolatileRandom,
            "volatile-ttl" => EvictionPolicy::VolatileTtl,
            _ => EvictionPolicy::NoEviction,
        }
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

/// Check if eviction is needed and attempt to free memory.
///
/// Returns Ok(()) if memory is within limits (or maxmemory is 0).
/// Returns Err(Frame) with OOM error if eviction fails to free enough memory.
pub fn try_evict_if_needed(db: &mut Database, config: &RuntimeConfig) -> Result<(), Frame> {
    try_evict_if_needed_with_spill(db, config, None)
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
    mut spill: Option<&mut SpillContext<'_>>,
    total_memory: usize,
) -> Result<(), Frame> {
    if config.maxmemory == 0 {
        return Ok(());
    }

    let policy = EvictionPolicy::from_str(&config.maxmemory_policy);

    // Check aggregate memory (server-wide maxmemory limit per Redis semantics).
    // Evict from this DB until total memory drops below limit.
    let mut current_total = total_memory;
    while current_total > config.maxmemory {
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

/// Check if eviction is needed, spilling evicted entries asynchronously via
/// a background `SpillThread` instead of doing synchronous pwrite.
///
/// The async path: extracts key/value bytes, removes entry from DashTable
/// (freeing RAM immediately), then sends a `SpillRequest` to the background
/// thread. The pwrite is best-effort -- if the channel is full, the request
/// is dropped (entry already removed from RAM).
///
/// Callers must poll `SpillThread::drain_completions()` to apply manifest
/// and ColdIndex updates from completed spills.
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
    if config.maxmemory == 0 {
        return Ok(());
    }

    let policy = EvictionPolicy::from_str(&config.maxmemory_policy);

    let mut current_total = total_memory;
    while current_total > config.maxmemory {
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

/// Evict entries to bring memory under maxmemory, returning removed
/// (key, Entry) pairs for deferred spill OUTSIDE the write lock.
///
/// Inside the lock: only find_victim + db.remove (~600ns per eviction).
/// The caller extracts value bytes from the owned Entry after releasing
/// the lock, then sends SpillRequests to the background thread.
pub fn try_evict_deferred(
    db: &mut Database,
    config: &RuntimeConfig,
) -> Result<smallvec::SmallVec<[(Bytes, crate::storage::entry::Entry); 2]>, Frame> {
    if config.maxmemory == 0 {
        return Ok(smallvec::SmallVec::new());
    }

    let total_memory = db.estimated_memory();
    if total_memory <= config.maxmemory {
        return Ok(smallvec::SmallVec::new());
    }

    let policy = EvictionPolicy::from_str(&config.maxmemory_policy);
    let mut evicted = smallvec::SmallVec::new();
    let mut current_total = total_memory;

    while current_total > config.maxmemory {
        if policy == EvictionPolicy::NoEviction {
            return Err(oom_error());
        }

        let victim = find_victim_for_policy(db, config, &policy);
        let key = match victim {
            Some(k) => k,
            None => return Err(oom_error()),
        };

        let before = db.estimated_memory();
        let key_bytes = Bytes::copy_from_slice(key.as_bytes());
        if let Some(entry) = db.remove(key.as_bytes()) {
            evicted.push((key_bytes, entry));
        }
        let after = db.estimated_memory();
        current_total = current_total.saturating_sub(before.saturating_sub(after));
    }

    Ok(evicted)
}

/// Find a victim key using the given eviction policy.
fn find_victim_for_policy(
    db: &Database,
    config: &RuntimeConfig,
    policy: &EvictionPolicy,
) -> Option<CompactKey> {
    match policy {
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
    }
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
                    RedisValueRef::Hash(_) | RedisValueRef::HashListpack(_) => ValueType::Hash,
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

        // Insert a tentative cold_index entry so subsequent GETs in this DB
        // can resolve the key while the bg pwrite is in flight. The completion
        // handler in persistence_tick::apply_spill_completions will overwrite
        // this with the authoritative ColdLocation once pwrite finishes.
        if let Some(ref mut ci) = db.cold_index {
            ci.insert(
                Bytes::copy_from_slice(key.as_bytes()),
                crate::storage::tiered::cold_index::ColdLocation {
                    file_id,
                    slot_idx: 0,
                },
            );
        }
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
        }
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

        let result = evict_one_volatile_ttl(&mut db, 5);
        assert!(result);
        assert_eq!(db.len(), 1);
        assert!(db.data().get(b"soon" as &[u8]).is_none());
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
}
