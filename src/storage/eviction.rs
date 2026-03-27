use bytes::Bytes;
use rand::seq::IndexedRandom;

use crate::config::RuntimeConfig;
use crate::protocol::Frame;
use crate::storage::compact_key::CompactKey;
use crate::storage::entry::lfu_decay;
use crate::storage::Database;

/// Compare two LRU timestamps with u16 wraparound handling.
/// Uses signed-distance comparison: treats the 16-bit clock as circular.
/// Returns true if `a` is considered older (less recent) than `b`.
#[inline]
pub fn lru_is_older(a: u32, b: u32) -> bool {
    let a16 = a as i16;
    let b16 = b as i16;
    // Signed difference handles wraparound: if b recently wrapped past 0
    // and a is near 65535, (a16 - b16) will be a large positive number,
    // meaning a is "older" (further in the past).
    a16.wrapping_sub(b16) > 0
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

/// Check if eviction is needed and attempt to free memory.
///
/// Returns Ok(()) if memory is within limits (or maxmemory is 0).
/// Returns Err(Frame) with OOM error if eviction fails to free enough memory.
pub fn try_evict_if_needed(db: &mut Database, config: &RuntimeConfig) -> Result<(), Frame> {
    if config.maxmemory == 0 {
        return Ok(());
    }

    let policy = EvictionPolicy::from_str(&config.maxmemory_policy);

    while db.estimated_memory() > config.maxmemory {
        if policy == EvictionPolicy::NoEviction {
            return Err(oom_error());
        }
        if !evict_one(db, config, &policy) {
            return Err(oom_error());
        }
    }

    Ok(())
}

/// Evict a single key according to the configured policy.
/// Returns true if a key was evicted, false if no eligible keys found.
fn evict_one(db: &mut Database, config: &RuntimeConfig, policy: &EvictionPolicy) -> bool {
    match policy {
        EvictionPolicy::NoEviction => false,
        EvictionPolicy::AllKeysLru => evict_one_lru(db, config.maxmemory_samples, false),
        EvictionPolicy::AllKeysLfu => {
            evict_one_lfu(db, config.maxmemory_samples, config.lfu_decay_time, false)
        }
        EvictionPolicy::AllKeysRandom => evict_one_random(db, false),
        EvictionPolicy::VolatileLru => evict_one_lru(db, config.maxmemory_samples, true),
        EvictionPolicy::VolatileLfu => {
            evict_one_lfu(db, config.maxmemory_samples, config.lfu_decay_time, true)
        }
        EvictionPolicy::VolatileRandom => evict_one_random(db, true),
        EvictionPolicy::VolatileTtl => evict_one_volatile_ttl(db, config.maxmemory_samples),
    }
}

/// Evict the key with the oldest last_access from a random sample.
fn evict_one_lru(db: &mut Database, samples: usize, volatile_only: bool) -> bool {
    let keys: Vec<CompactKey> = if volatile_only {
        db.data()
            .iter()
            .filter(|(_, e)| e.has_expiry())
            .map(|(k, _)| k.clone())
            .collect()
    } else {
        db.data().keys().cloned().collect()
    };

    if keys.is_empty() {
        return false;
    }

    let mut rng = rand::rng();
    let sample_size = samples.min(keys.len());
    let sampled: Vec<&CompactKey> = keys.choose_multiple(&mut rng, sample_size).collect();

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

    if let Some(key) = oldest_key {
        db.remove(key.as_bytes());
        true
    } else {
        false
    }
}

/// Evict the key with the lowest LFU counter (after decay) from a random sample.
fn evict_one_lfu(
    db: &mut Database,
    samples: usize,
    lfu_decay_time: u64,
    volatile_only: bool,
) -> bool {
    let keys: Vec<CompactKey> = if volatile_only {
        db.data()
            .iter()
            .filter(|(_, e)| e.has_expiry())
            .map(|(k, _)| k.clone())
            .collect()
    } else {
        db.data().keys().cloned().collect()
    };

    if keys.is_empty() {
        return false;
    }

    let mut rng = rand::rng();
    let sample_size = samples.min(keys.len());
    let sampled: Vec<&CompactKey> = keys.choose_multiple(&mut rng, sample_size).collect();

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
                            && oldest_access_for_tie
                                .map_or(true, |t| lru_is_older(la, t)))
                }
            };

            if should_evict {
                evict_key = Some(key.clone());
                lowest_counter = Some(effective_counter);
                oldest_access_for_tie = Some(la);
            }
        }
    }

    if let Some(key) = evict_key {
        db.remove(key.as_bytes());
        true
    } else {
        false
    }
}

/// Evict one random key.
fn evict_one_random(db: &mut Database, volatile_only: bool) -> bool {
    let keys: Vec<CompactKey> = if volatile_only {
        db.data()
            .iter()
            .filter(|(_, e)| e.has_expiry())
            .map(|(k, _)| k.clone())
            .collect()
    } else {
        db.data().keys().cloned().collect()
    };

    if keys.is_empty() {
        return false;
    }

    let mut rng = rand::rng();
    if let Some(key) = keys.choose(&mut rng) {
        db.remove(key.as_bytes());
        true
    } else {
        false
    }
}

/// Evict the key with the soonest TTL expiration from a random sample.
fn evict_one_volatile_ttl(db: &mut Database, samples: usize) -> bool {
    let keys: Vec<CompactKey> = db
        .data()
        .iter()
        .filter(|(_, e)| e.has_expiry())
        .map(|(k, _)| k.clone())
        .collect();

    if keys.is_empty() {
        return false;
    }

    let mut rng = rand::rng();
    let sample_size = samples.min(keys.len());
    let sampled: Vec<&CompactKey> = keys.choose_multiple(&mut rng, sample_size).collect();

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

    if let Some(key) = evict_key {
        db.remove(key.as_bytes());
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry::{current_secs, current_time_ms, Entry};

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
        assert_eq!(EvictionPolicy::from_str("noeviction"), EvictionPolicy::NoEviction);
        assert_eq!(EvictionPolicy::from_str("allkeys-lru"), EvictionPolicy::AllKeysLru);
        assert_eq!(EvictionPolicy::from_str("allkeys-lfu"), EvictionPolicy::AllKeysLfu);
        assert_eq!(EvictionPolicy::from_str("allkeys-random"), EvictionPolicy::AllKeysRandom);
        assert_eq!(EvictionPolicy::from_str("volatile-lru"), EvictionPolicy::VolatileLru);
        assert_eq!(EvictionPolicy::from_str("volatile-lfu"), EvictionPolicy::VolatileLfu);
        assert_eq!(EvictionPolicy::from_str("volatile-random"), EvictionPolicy::VolatileRandom);
        assert_eq!(EvictionPolicy::from_str("volatile-ttl"), EvictionPolicy::VolatileTtl);
        assert_eq!(EvictionPolicy::from_str("ALLKEYS-LRU"), EvictionPolicy::AllKeysLru);
        assert_eq!(EvictionPolicy::from_str("unknown"), EvictionPolicy::NoEviction);
    }

    #[test]
    fn test_noeviction_returns_oom() {
        let mut db = Database::new();
        // Set a key to use some memory
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"value"));
        // Configure very small maxmemory with noeviction
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
        assert_eq!(db.len(), 1); // Key should still be there
    }

    #[test]
    fn test_lru_evicts_oldest() {
        let mut db = Database::new();
        // Create entries with different last_access times
        let mut entry1 = Entry::new_string(Bytes::from_static(b"val1"));
        entry1.set_last_access(current_secs() - 100); // oldest
        db.set(Bytes::from_static(b"old"), entry1);

        let mut entry2 = Entry::new_string(Bytes::from_static(b"val2"));
        entry2.set_last_access(current_secs() - 50);
        db.set(Bytes::from_static(b"medium"), entry2);

        let mut entry3 = Entry::new_string(Bytes::from_static(b"val3"));
        entry3.set_last_access(current_secs()); // newest
        db.set(Bytes::from_static(b"new"), entry3);

        // Set maxmemory to allow only 2 entries (roughly)
        let mem = db.estimated_memory();
        // We want to trigger eviction of exactly 1 key
        let config = make_config(mem - 1, "allkeys-lru");

        let result = try_evict_if_needed(&mut db, &config);
        assert!(result.is_ok());
        // With samples=5 and only 3 keys, all are sampled -> oldest should be evicted
        assert_eq!(db.len(), 2);
        // "old" should have been evicted (oldest last_access)
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
        // Should have evicted keys until under limit (all of them since limit is 1 byte)
        assert!(result.is_ok());
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn test_volatile_only_skips_persistent() {
        let mut db = Database::new();
        // Persistent key (no TTL)
        db.set_string(Bytes::from_static(b"persistent"), Bytes::from_static(b"value"));
        // Volatile key (has TTL)
        let future_ms = current_time_ms() + 3_600_000;
        db.set_string_with_expiry(
            Bytes::from_static(b"volatile"),
            Bytes::from_static(b"value"),
            future_ms,
        );

        // With only 1 volatile key, volatile-random should evict it
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
        // "soon" should have been evicted (soonest expiry)
        assert!(db.data().get(b"soon" as &[u8]).is_none());
    }

    #[test]
    fn test_policy_as_str() {
        assert_eq!(EvictionPolicy::NoEviction.as_str(), "noeviction");
        assert_eq!(EvictionPolicy::AllKeysLru.as_str(), "allkeys-lru");
        assert_eq!(EvictionPolicy::VolatileTtl.as_str(), "volatile-ttl");
    }
}
