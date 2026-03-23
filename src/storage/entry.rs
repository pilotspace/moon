use bytes::Bytes;
use ordered_float::OrderedFloat;
use rand::Rng;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Return the current time as seconds since the Unix epoch, truncated to u32.
/// Wraps around in the year 2106 -- acceptable for LRU/LFU relative comparisons.
#[inline]
pub fn current_secs() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32
}

/// The type of value stored in a Redis key.
#[derive(Debug, Clone)]
pub enum RedisValue {
    String(Bytes),
    Hash(HashMap<Bytes, Bytes>),
    List(VecDeque<Bytes>),
    Set(HashSet<Bytes>),
    SortedSet {
        members: HashMap<Bytes, f64>,
        scores: BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
    },
}

impl RedisValue {
    /// Return the Redis type name string for this value.
    pub fn type_name(&self) -> &'static str {
        match self {
            RedisValue::String(_) => "string",
            RedisValue::Hash(_) => "hash",
            RedisValue::List(_) => "list",
            RedisValue::Set(_) => "set",
            RedisValue::SortedSet { .. } => "zset",
        }
    }

    /// Estimate memory usage of this value in bytes.
    pub fn estimate_memory(&self) -> usize {
        match self {
            RedisValue::String(b) => b.len(),
            RedisValue::Hash(map) => map
                .iter()
                .map(|(k, v)| k.len() + v.len() + 64)
                .sum(),
            RedisValue::List(list) => list
                .iter()
                .map(|elem| elem.len() + 24)
                .sum(),
            RedisValue::Set(set) => set
                .iter()
                .map(|member| member.len() + 24)
                .sum(),
            RedisValue::SortedSet { members, .. } => members
                .iter()
                .map(|(member, _)| member.len() + 80)
                .sum(),
        }
    }
}

/// LFU initial counter value (per Redis convention).
const LFU_INIT_VAL: u8 = 5;

/// Probabilistic logarithmic increment for LFU counter (Morris counter).
pub fn lfu_log_incr(counter: u8, lfu_log_factor: u8) -> u8 {
    if counter == 255 {
        return 255;
    }
    let r: f64 = rand::rng().random();
    let base_val = (counter as f64 - LFU_INIT_VAL as f64).max(0.0);
    let p = 1.0 / (base_val * lfu_log_factor as f64 + 1.0);
    if r < p {
        counter + 1
    } else {
        counter
    }
}

/// Time-based decay for LFU counter.
pub fn lfu_decay(counter: u8, last_access: u32, lfu_decay_time: u64) -> u8 {
    if lfu_decay_time == 0 {
        return counter;
    }
    let now = current_secs();
    let elapsed_secs = now.wrapping_sub(last_access) as u64;
    let elapsed_min = elapsed_secs / 60;
    let decay = (elapsed_min / lfu_decay_time) as u8;
    counter.saturating_sub(decay)
}

/// A single entry in the database, wrapping a value with expiration metadata.
#[derive(Debug, Clone)]
pub struct Entry {
    pub value: RedisValue,
    pub expires_at: Option<Instant>,
    /// Monotonically increasing version for WATCH support.
    pub version: u32,
    /// Last access time for LRU eviction (unix seconds, u32).
    pub last_access: u32,
    /// LFU access counter (Morris counter, initialized to LFU_INIT_VAL).
    pub access_counter: u8,
}

impl Entry {
    /// Create a new string entry with no expiration.
    pub fn new_string(value: Bytes) -> Entry {
        Entry {
            value: RedisValue::String(value),
            expires_at: None,
            version: 0,
            last_access: current_secs(),
            access_counter: LFU_INIT_VAL,
        }
    }

    /// Create a new string entry with an expiration time.
    pub fn new_string_with_expiry(value: Bytes, expires_at: Instant) -> Entry {
        Entry {
            value: RedisValue::String(value),
            expires_at: Some(expires_at),
            version: 0,
            last_access: current_secs(),
            access_counter: LFU_INIT_VAL,
        }
    }

    /// Create a new hash entry with an empty HashMap.
    pub fn new_hash() -> Entry {
        Entry {
            value: RedisValue::Hash(HashMap::new()),
            expires_at: None,
            version: 0,
            last_access: current_secs(),
            access_counter: LFU_INIT_VAL,
        }
    }

    /// Create a new list entry with an empty VecDeque.
    pub fn new_list() -> Entry {
        Entry {
            value: RedisValue::List(VecDeque::new()),
            expires_at: None,
            version: 0,
            last_access: current_secs(),
            access_counter: LFU_INIT_VAL,
        }
    }

    /// Create a new set entry with an empty HashSet.
    pub fn new_set() -> Entry {
        Entry {
            value: RedisValue::Set(HashSet::new()),
            expires_at: None,
            version: 0,
            last_access: current_secs(),
            access_counter: LFU_INIT_VAL,
        }
    }

    /// Create a new sorted set entry with empty members and scores.
    pub fn new_sorted_set() -> Entry {
        Entry {
            value: RedisValue::SortedSet {
                members: HashMap::new(),
                scores: BTreeMap::new(),
            },
            expires_at: None,
            version: 0,
            last_access: current_secs(),
            access_counter: LFU_INIT_VAL,
        }
    }

    /// Update last_access for LRU tracking.
    pub fn touch_lru(&mut self) {
        self.last_access = current_secs();
    }

    /// Update last_access and probabilistically increment LFU counter.
    pub fn touch_lfu(&mut self, lfu_log_factor: u8) {
        self.last_access = current_secs();
        self.access_counter = lfu_log_incr(self.access_counter, lfu_log_factor);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_string_no_expiry() {
        let entry = Entry::new_string(Bytes::from_static(b"hello"));
        assert!(entry.expires_at.is_none());
        matches!(entry.value, RedisValue::String(_));
        assert_eq!(entry.version, 0);
        assert_eq!(entry.access_counter, LFU_INIT_VAL);
    }

    #[test]
    fn test_new_string_with_expiry() {
        let exp = Instant::now() + std::time::Duration::from_secs(60);
        let entry = Entry::new_string_with_expiry(Bytes::from_static(b"hello"), exp);
        assert_eq!(entry.expires_at, Some(exp));
        assert_eq!(entry.version, 0);
        assert_eq!(entry.access_counter, LFU_INIT_VAL);
    }

    #[test]
    fn test_new_hash() {
        let entry = Entry::new_hash();
        assert!(entry.expires_at.is_none());
        assert!(matches!(entry.value, RedisValue::Hash(ref m) if m.is_empty()));
        assert_eq!(entry.value.type_name(), "hash");
        assert_eq!(entry.version, 0);
    }

    #[test]
    fn test_new_list() {
        let entry = Entry::new_list();
        assert!(entry.expires_at.is_none());
        assert!(matches!(entry.value, RedisValue::List(ref l) if l.is_empty()));
        assert_eq!(entry.value.type_name(), "list");
        assert_eq!(entry.version, 0);
    }

    #[test]
    fn test_new_set() {
        let entry = Entry::new_set();
        assert!(entry.expires_at.is_none());
        assert!(matches!(entry.value, RedisValue::Set(ref s) if s.is_empty()));
        assert_eq!(entry.value.type_name(), "set");
        assert_eq!(entry.version, 0);
    }

    #[test]
    fn test_new_sorted_set() {
        let entry = Entry::new_sorted_set();
        assert!(entry.expires_at.is_none());
        assert!(matches!(entry.value, RedisValue::SortedSet { ref members, ref scores } if members.is_empty() && scores.is_empty()));
        assert_eq!(entry.value.type_name(), "zset");
        assert_eq!(entry.version, 0);
    }

    #[test]
    fn test_type_name() {
        assert_eq!(RedisValue::String(Bytes::from_static(b"")).type_name(), "string");
        assert_eq!(RedisValue::Hash(HashMap::new()).type_name(), "hash");
        assert_eq!(RedisValue::List(VecDeque::new()).type_name(), "list");
        assert_eq!(RedisValue::Set(HashSet::new()).type_name(), "set");
        assert_eq!(RedisValue::SortedSet { members: HashMap::new(), scores: BTreeMap::new() }.type_name(), "zset");
    }

    #[test]
    fn test_estimate_memory_string() {
        let val = RedisValue::String(Bytes::from_static(b"hello"));
        assert_eq!(val.estimate_memory(), 5);
    }

    #[test]
    fn test_estimate_memory_hash() {
        let mut map = HashMap::new();
        map.insert(Bytes::from_static(b"key"), Bytes::from_static(b"val"));
        let val = RedisValue::Hash(map);
        // key(3) + val(3) + 64 = 70
        assert_eq!(val.estimate_memory(), 70);
    }

    #[test]
    fn test_estimate_memory_empty() {
        assert_eq!(RedisValue::Hash(HashMap::new()).estimate_memory(), 0);
        assert_eq!(RedisValue::List(VecDeque::new()).estimate_memory(), 0);
        assert_eq!(RedisValue::Set(HashSet::new()).estimate_memory(), 0);
    }

    #[test]
    fn test_lfu_log_incr_max() {
        assert_eq!(lfu_log_incr(255, 10), 255);
    }

    #[test]
    fn test_lfu_decay_zero_time() {
        assert_eq!(lfu_decay(100, current_secs(), 0), 100);
    }

    #[test]
    fn test_lfu_decay_recent() {
        // Just accessed, no decay expected
        assert_eq!(lfu_decay(100, current_secs(), 1), 100);
    }

    #[test]
    fn test_touch_lru() {
        let mut entry = Entry::new_string(Bytes::from_static(b"test"));
        let before = entry.last_access;
        entry.touch_lru();
        assert!(entry.last_access >= before);
    }
}
