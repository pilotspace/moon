use std::path::Path;

use bytes::Bytes;
use rand::seq::IndexedRandom;
use tracing::warn;

use crate::config::RuntimeConfig;
use crate::persistence::manifest::ShardManifest;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::compact_key::CompactKey;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::entry::lfu_decay;
use crate::storage::tiered::kv_spill;

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
    true
}

// ── Victim selection helpers ───────────────────────────

/// Collect candidate keys for eviction (all keys or volatile-only).
fn collect_candidate_keys(db: &Database, volatile_only: bool) -> Vec<CompactKey> {
    if volatile_only {
        db.data()
            .iter()
            .filter(|(_, e)| e.has_expiry())
            .map(|(k, _)| k.clone())
            .collect()
    } else {
        db.data().keys().cloned().collect()
    }
}

/// Find the victim key with the oldest last_access from a random sample.
fn find_victim_lru(db: &Database, samples: usize, volatile_only: bool) -> Option<CompactKey> {
    let keys = collect_candidate_keys(db, volatile_only);
    if keys.is_empty() {
        return None;
    }

    let mut rng = rand::rng();
    let sample_size = samples.min(keys.len());
    let sampled: Vec<&CompactKey> = keys.sample(&mut rng, sample_size).collect();

    let mut oldest_key: Option<CompactKey> = None;
    let mut oldest_access = None;

    for key in sampled {
        if let Some(entry) = db.data().get(key.as_bytes()) {
            let la = entry.last_access();
            match oldest_access {
                None => {
                    oldest_key = Some(key.clone());
                    oldest_access = Some(la);
                }
                Some(ref oldest) => {
                    if lru_is_older(la, *oldest) {
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
    let keys = collect_candidate_keys(db, volatile_only);
    if keys.is_empty() {
        return None;
    }

    let mut rng = rand::rng();
    let sample_size = samples.min(keys.len());
    let sampled: Vec<&CompactKey> = keys.sample(&mut rng, sample_size).collect();

    let mut evict_key: Option<CompactKey> = None;
    let mut lowest_counter: Option<u8> = None;
    let mut oldest_access_for_tie = None;

    for key in sampled {
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
    let keys = collect_candidate_keys(db, volatile_only);
    if keys.is_empty() {
        return None;
    }

    let mut rng = rand::rng();
    keys.choose(&mut rng).cloned()
}

/// Find the victim key with the soonest TTL expiration from a random sample.
fn find_victim_volatile_ttl(db: &Database, samples: usize) -> Option<CompactKey> {
    let keys: Vec<CompactKey> = db
        .data()
        .iter()
        .filter(|(_, e)| e.has_expiry())
        .map(|(k, _)| k.clone())
        .collect();

    if keys.is_empty() {
        return None;
    }

    let mut rng = rand::rng();
    let sample_size = samples.min(keys.len());
    let sampled: Vec<&CompactKey> = keys.sample(&mut rng, sample_size).collect();

    let mut evict_key: Option<CompactKey> = None;
    let mut soonest_expiry: Option<u64> = None;

    for key in sampled {
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

        let mem = db.estimated_memory();
        let config = make_config(mem - 1, "allkeys-lru");

        let result = try_evict_if_needed(&mut db, &config);
        assert!(result.is_ok());
        assert_eq!(db.len(), 2);
        assert!(db.data().get(b"old" as &[u8]).is_none());
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
        db.set_string(Bytes::from_static(b"spill_key"), Bytes::from_static(b"spill_val"));

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
