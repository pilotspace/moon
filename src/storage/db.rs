use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use super::bptree::BPTree;
use super::compact_key::CompactKey;
use super::compact_value::{CompactValue, RedisValueRef};
use super::dashtable::DashTable;
use super::entry::{current_secs, current_time_ms, Entry, RedisValue};
use super::intset::Intset;
use super::listpack::Listpack;
use super::stream::Stream as StreamData;
use crate::protocol::Frame;

// ---------------------------------------------------------------------------
// Read-only Ref enums for immutable access to compact and full encodings
// ---------------------------------------------------------------------------

/// Read-only reference to a hash (full HashMap or compact Listpack).
pub enum HashRef<'a> {
    Map(&'a HashMap<Bytes, Bytes>),
    Listpack(&'a Listpack),
}

impl<'a> HashRef<'a> {
    /// Look up a single field. Linear scan for listpack, O(1) for HashMap.
    pub fn get_field(&self, field: &[u8]) -> Option<Bytes> {
        match self {
            HashRef::Map(map) => map.get(field).cloned(),
            HashRef::Listpack(lp) => {
                for (f, v) in lp.iter_pairs() {
                    if f.as_bytes() == field {
                        return Some(v.to_bytes());
                    }
                }
                None
            }
        }
    }

    /// Number of fields in the hash.
    pub fn len(&self) -> usize {
        match self {
            HashRef::Map(map) => map.len(),
            HashRef::Listpack(lp) => lp.len() / 2,
        }
    }

    /// Return all (field, value) pairs.
    pub fn entries(&self) -> Vec<(Bytes, Bytes)> {
        match self {
            HashRef::Map(map) => map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            HashRef::Listpack(lp) => lp
                .iter_pairs()
                .map(|(f, v)| (f.to_bytes(), v.to_bytes()))
                .collect(),
        }
    }
}

/// Read-only reference to a list (full VecDeque or compact Listpack).
pub enum ListRef<'a> {
    Deque(&'a VecDeque<Bytes>),
    Listpack(&'a Listpack),
}

impl<'a> ListRef<'a> {
    /// Number of elements.
    pub fn len(&self) -> usize {
        match self {
            ListRef::Deque(d) => d.len(),
            ListRef::Listpack(lp) => lp.len(),
        }
    }

    /// Get element at index.
    pub fn get(&self, index: usize) -> Option<Bytes> {
        match self {
            ListRef::Deque(d) => d.get(index).cloned(),
            ListRef::Listpack(lp) => lp.get_at(index).map(|e| e.to_bytes()),
        }
    }

    /// Get a range of elements [start..=end]. Caller must clamp bounds.
    pub fn range(&self, start: usize, end: usize) -> Vec<Bytes> {
        match self {
            ListRef::Deque(d) => {
                (start..=end).filter_map(|i| d.get(i).cloned()).collect()
            }
            ListRef::Listpack(lp) => {
                (start..=end).filter_map(|i| lp.get_at(i).map(|e| e.to_bytes())).collect()
            }
        }
    }

    /// Iterate all elements (for LPOS).
    pub fn iter_bytes(&self) -> Vec<Bytes> {
        match self {
            ListRef::Deque(d) => d.iter().cloned().collect(),
            ListRef::Listpack(lp) => lp.iter().map(|e| e.to_bytes()).collect(),
        }
    }
}

/// Read-only reference to a set (full HashSet, compact Listpack, or Intset).
pub enum SetRef<'a> {
    Hash(&'a HashSet<Bytes>),
    Listpack(&'a Listpack),
    Intset(&'a Intset),
}

impl<'a> SetRef<'a> {
    /// Number of members.
    pub fn len(&self) -> usize {
        match self {
            SetRef::Hash(s) => s.len(),
            SetRef::Listpack(lp) => lp.len(),
            SetRef::Intset(is) => is.len(),
        }
    }

    /// Check if member exists.
    pub fn contains(&self, member: &[u8]) -> bool {
        match self {
            SetRef::Hash(s) => s.contains(member),
            SetRef::Listpack(lp) => lp.find(member).is_some(),
            SetRef::Intset(is) => {
                if let Ok(s) = std::str::from_utf8(member) {
                    if let Ok(v) = s.parse::<i64>() {
                        return is.contains(v);
                    }
                }
                false
            }
        }
    }

    /// Return all members as Bytes.
    pub fn members(&self) -> Vec<Bytes> {
        match self {
            SetRef::Hash(s) => s.iter().cloned().collect(),
            SetRef::Listpack(lp) => lp.iter().map(|e| e.to_bytes()).collect(),
            SetRef::Intset(is) => is.iter().map(|v| Bytes::from(v.to_string())).collect(),
        }
    }

    /// Convert to an owned HashSet for set-algebra operations.
    pub fn to_hash_set(&self) -> HashSet<Bytes> {
        match self {
            SetRef::Hash(s) => (*s).clone(),
            SetRef::Listpack(lp) => lp.iter().map(|e| e.to_bytes()).collect(),
            SetRef::Intset(is) => is.iter().map(|v| Bytes::from(v.to_string())).collect(),
        }
    }
}

/// Read-only reference to a sorted set.
pub enum SortedSetRef<'a> {
    BPTree {
        tree: &'a BPTree,
        members: &'a HashMap<Bytes, f64>,
    },
    Listpack(&'a Listpack),
    #[allow(dead_code)]
    Legacy {
        members: &'a HashMap<Bytes, f64>,
        scores: &'a BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
    },
}

impl<'a> SortedSetRef<'a> {
    /// Number of members.
    pub fn len(&self) -> usize {
        match self {
            SortedSetRef::BPTree { members, .. } => members.len(),
            SortedSetRef::Listpack(lp) => lp.len() / 2,
            SortedSetRef::Legacy { members, .. } => members.len(),
        }
    }

    /// Get score for a member.
    pub fn score(&self, member: &[u8]) -> Option<f64> {
        match self {
            SortedSetRef::BPTree { members, .. } => members.get(member).copied(),
            SortedSetRef::Listpack(lp) => {
                for (m, s) in lp.iter_score_member_pairs() {
                    if m.as_bytes() == member {
                        return match s {
                            super::listpack::ListpackEntry::Integer(i) => Some(i as f64),
                            super::listpack::ListpackEntry::String(ref b) => {
                                std::str::from_utf8(b).ok().and_then(|s| s.parse().ok())
                            }
                        };
                    }
                }
                None
            }
            SortedSetRef::Legacy { members, .. } => members.get(member).copied(),
        }
    }

    /// Get all (member, score) pairs sorted by score then member.
    pub fn entries_sorted(&self) -> Vec<(Bytes, f64)> {
        match self {
            SortedSetRef::BPTree { tree, .. } => {
                tree.iter().map(|(score, member)| (member.clone(), score.0)).collect()
            }
            SortedSetRef::Listpack(lp) => {
                let mut pairs: Vec<(Bytes, f64)> = Vec::new();
                for (m, s) in lp.iter_score_member_pairs() {
                    let score = match s {
                        super::listpack::ListpackEntry::Integer(i) => i as f64,
                        super::listpack::ListpackEntry::String(ref b) => {
                            std::str::from_utf8(b).ok().and_then(|ss| ss.parse().ok()).unwrap_or(0.0)
                        }
                    };
                    pairs.push((m.to_bytes(), score));
                }
                pairs.sort_by(|a, b| {
                    OrderedFloat(a.1).cmp(&OrderedFloat(b.1)).then_with(|| a.0.cmp(&b.0))
                });
                pairs
            }
            SortedSetRef::Legacy { scores, .. } => {
                scores.keys().map(|(s, m)| (m.clone(), s.0)).collect()
            }
        }
    }

    /// Get members HashMap reference (for BPTree and Legacy variants).
    /// For listpack, returns None (callers should use entries_sorted or score).
    pub fn members_map(&self) -> Option<&'a HashMap<Bytes, f64>> {
        match self {
            SortedSetRef::BPTree { members, .. } => Some(members),
            SortedSetRef::Legacy { members, .. } => Some(members),
            SortedSetRef::Listpack(_) => None,
        }
    }

    /// Get BPTree reference (for BPTree variant only).
    pub fn bptree(&self) -> Option<&'a BPTree> {
        match self {
            SortedSetRef::BPTree { tree, .. } => Some(tree),
            _ => None,
        }
    }
}

/// Maximum number of entries in a listpack before upgrading to full encoding.
pub const LISTPACK_MAX_ENTRIES: usize = 128;
/// Maximum element size in bytes before upgrading a listpack to full encoding.
pub const LISTPACK_MAX_ELEMENT_SIZE: usize = 64;
/// Maximum number of entries in an intset before upgrading to full encoding.
#[allow(dead_code)]
const INTSET_MAX_ENTRIES: usize = 512;

/// Estimate per-entry overhead: key length + value memory + struct overhead.
fn entry_overhead(key: &[u8], entry: &Entry) -> usize {
    key.len() + entry.value.estimate_memory() + 128
}

/// An in-memory key-value database with lazy expiration.
///
/// Keys are `Bytes` (binary-safe). Values are `Entry` structs containing
/// a `CompactValue`, optional expiration (TTL delta), and packed metadata.
pub struct Database {
    data: DashTable<CompactKey, Entry>,
    used_memory: usize,
    /// Cached current time in epoch seconds; set once per batch to avoid
    /// repeated `SystemTime::now()` syscalls on every command.
    cached_now: u32,
    /// Cached current time in unix millis for expiry checks.
    cached_now_ms: u64,
    /// Base timestamp (epoch seconds) for TTL delta computation.
    /// Set once at database creation time and never changed, ensuring
    /// TTL deltas remain stable across the database lifetime.
    base_timestamp: u32,
}

impl Database {
    /// Create a new empty database.
    pub fn new() -> Self {
        Database {
            data: DashTable::new(),
            used_memory: 0,
            cached_now: current_secs(),
            cached_now_ms: current_time_ms(),
            base_timestamp: current_secs(),
        }
    }

    /// Update the cached timestamp. Call once per batch to amortize syscall cost.
    /// Does NOT update base_timestamp (it stays fixed for TTL delta stability).
    #[inline]
    pub fn refresh_now(&mut self) {
        self.cached_now = current_secs();
        self.cached_now_ms = current_time_ms();
    }

    /// Return the base timestamp for TTL delta computation.
    #[inline]
    pub fn base_timestamp(&self) -> u32 {
        self.base_timestamp
    }

    /// Return the cached current time (epoch seconds).
    #[inline]
    pub fn now(&self) -> u32 {
        self.cached_now
    }

    /// Return the cached current time (unix millis).
    #[inline]
    pub fn now_ms(&self) -> u64 {
        self.cached_now_ms
    }

    /// Estimated memory usage of all entries in this database.
    pub fn estimated_memory(&self) -> usize {
        self.used_memory
    }

    /// Get an entry by key, performing lazy expiration.
    ///
    /// Returns `None` if the key does not exist or has expired.
    /// Optimized: immutable lookup for expiry check, no LRU touch on reads
    /// (LRU is only needed when eviction is active; callers requiring LRU
    /// updates should use `get_mut()`). This reduces the hot path from
    /// get_mut+get (2 SIMD probes) to get+get (1 probe for non-expired keys
    /// since the compiler can often elide the second immutable lookup).
    pub fn get(&mut self, key: &[u8]) -> Option<&Entry> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        // Single immutable lookup: check existence + expiry
        let expired = self.data.get(key)
            .is_some_and(|e| e.is_expired_at(base_ts, now_ms));
        if expired {
            let removed = self.data.remove(key)?;
            self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &removed));
            return None;
        }
        // Return immutable ref (same slot, fast re-probe)
        self.data.get(key)
    }

    /// Get a mutable reference to an entry by key, performing lazy expiration and access tracking.
    ///
    /// Returns `None` if the key does not exist or has expired.
    /// Optimized: immutable check for expiry (rare path), then single get_mut
    /// for LRU touch + return. Reduces from 3 lookups to 2 for non-expired keys.
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut Entry> {
        let now = self.cached_now;
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        // Immutable check for expiry (avoids get_mut + remove + get_mut triple lookup)
        let expired = self.data.get(key)
            .is_some_and(|e| e.is_expired_at(base_ts, now_ms));
        if expired {
            let removed = self.data.remove(key)?;
            self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &removed));
            return None;
        }
        // Single get_mut: touch LRU + return
        let entry = self.data.get_mut(key)?;
        entry.set_last_access(now);
        Some(entry)
    }

    /// Insert or replace an entry, tracking memory and version.
    pub fn set(&mut self, key: Bytes, mut entry: Entry) {
        // If key exists, carry forward version+1 and subtract old memory
        if let Some(old_entry) = self.data.get(key.as_ref()) {
            let new_version = old_entry.version() + 1;
            entry.set_version(new_version);
            self.used_memory = self.used_memory.saturating_sub(entry_overhead(&key, old_entry));
        }
        self.used_memory += entry_overhead(&key, &entry);
        self.data.insert(CompactKey::from(key), entry);
    }

    /// Remove a key and return its entry. No expiry check needed (DEL removes regardless).
    pub fn remove(&mut self, key: &[u8]) -> Option<Entry> {
        if let Some(entry) = self.data.remove(key) {
            self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            Some(entry)
        } else {
            None
        }
    }

    /// Check if a key exists, performing lazy expiration.
    /// Optimized: single lookup via get instead of check_expired + contains_key.
    pub fn exists(&mut self, key: &[u8]) -> bool {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        match self.data.get(key) {
            None => false,
            Some(entry) => {
                if entry.is_expired_at(base_ts, now_ms) {
                    let removed = self.data.remove(key).unwrap();
                    self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &removed));
                    false
                } else {
                    true
                }
            }
        }
    }

    /// Number of entries (including potentially expired ones).
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Iterator over all keys (caller does glob filtering).
    pub fn keys(&self) -> impl Iterator<Item = &CompactKey> {
        self.data.keys()
    }

    /// Set or remove expiration on an existing key.
    ///
    /// Performs lazy expiry check first. Returns `false` if the key does not
    /// exist (or has already expired). Pass 0 to remove expiry.
    pub fn set_expiry(&mut self, key: &[u8], expires_at_ms: u64) -> bool {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
            return false;
        }
        match self.data.get_mut(key) {
            Some(entry) => {
                entry.set_expires_at_ms(base_ts, expires_at_ms);
                true
            }
            None => false,
        }
    }

    /// Count entries that have an expiration set.
    pub fn expires_count(&self) -> usize {
        self.data.values().filter(|e| e.has_expiry()).count()
    }

    /// Check if an entry is expired without requiring &mut self.
    fn check_expired(data: &DashTable<CompactKey, Entry>, key: &[u8], base_ts: u32, now_ms: u64) -> bool {
        data.get(key)
            .is_some_and(|e| e.is_expired_at(base_ts, now_ms))
    }

    /// Convenience: set a string value with no expiry.
    pub fn set_string(&mut self, key: Bytes, value: Bytes) {
        self.set(key, Entry::new_string(value));
    }

    /// Convenience: set a string value with an expiry (unix millis).
    pub fn set_string_with_expiry(&mut self, key: Bytes, value: Bytes, expires_at_ms: u64) {
        let base_ts = self.base_timestamp;
        self.set(key, Entry::new_string_with_expiry(value, expires_at_ms, base_ts));
    }

    /// Check if a single entry is expired.
    #[allow(dead_code)]
    fn is_expired(entry: &Entry, base_ts: u32) -> bool {
        entry.is_expired_at(base_ts, current_time_ms())
    }

    /// WRONGTYPE error frame.
    fn wrongtype_error() -> Frame {
        Frame::Error(Bytes::from_static(
            b"WRONGTYPE Operation against a key holding the wrong kind of value",
        ))
    }

    /// Get the version of a key. Returns 0 if not found. No expiry check (WATCH needs raw version).
    pub fn get_version(&self, key: &[u8]) -> u32 {
        self.data.get(key).map(|e| e.version()).unwrap_or(0)
    }

    /// Increment version of a key if it exists.
    pub fn increment_version(&mut self, key: &[u8]) {
        if let Some(entry) = self.data.get_mut(key) {
            entry.increment_version();
        }
    }

    /// Touch access time of a key for LRU tracking (for reads).
    pub fn touch_access(&mut self, key: &[u8]) {
        let now = self.cached_now;
        if let Some(entry) = self.data.get_mut(key) {
            entry.set_last_access(now);
        }
    }

    /// Mutable access to the data map (for eviction to remove keys directly).
    pub fn data_mut(&mut self) -> &mut DashTable<CompactKey, Entry> {
        &mut self.data
    }

    /// Get or create a hash entry. Returns mutable ref to inner HashMap.
    /// Returns Err(WRONGTYPE) if key exists with wrong type.
    ///
    /// New keys start with compact listpack encoding and are upgraded to
    /// full HashMap on first mutable access (eager upgrade).
    pub fn get_or_create_hash(
        &mut self,
        key: &[u8],
    ) -> Result<&mut HashMap<Bytes, Bytes>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        // Expire check
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            let entry = Entry::new_hash();
            let k = CompactKey::from(key);
            self.used_memory += entry_overhead(key, &entry);
            self.data.insert(k, entry);
        }
        let entry = self.data.get_mut(key).unwrap();
        // Upgrade compact listpack to full HashMap if needed
        if let Some(RedisValue::HashListpack(lp)) = entry.value.as_redis_value_mut() {
            let map = lp.to_hash_map();
            *entry.value.as_redis_value_mut().unwrap() = RedisValue::Hash(map);
        }
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::Hash(map)) => Ok(map),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Get a hash entry (read-only). Returns None if key missing, Err if wrong type.
    /// Upgrades compact encoding to full HashMap if found.
    pub fn get_hash(&mut self, key: &[u8]) -> Result<Option<&HashMap<Bytes, Bytes>>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        // Upgrade compact encoding if present
        if let Some(entry) = self.data.get_mut(key) {
            if let Some(RedisValue::HashListpack(lp)) = entry.value.as_redis_value_mut() {
                let map = lp.to_hash_map();
                *entry.value.as_redis_value_mut().unwrap() = RedisValue::Hash(map);
            }
        }
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::Hash(map) => Ok(Some(map)),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Get or create a list entry. Returns mutable ref to inner VecDeque.
    /// New keys start with full encoding. Upgrades compact listpack on access.
    pub fn get_or_create_list(
        &mut self,
        key: &[u8],
    ) -> Result<&mut VecDeque<Bytes>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            let entry = Entry::new_list();
            let k = CompactKey::from(key);
            self.used_memory += entry_overhead(key, &entry);
            self.data.insert(k, entry);
        }
        let entry = self.data.get_mut(key).unwrap();
        // Upgrade compact listpack to full VecDeque if needed
        if let Some(RedisValue::ListListpack(lp)) = entry.value.as_redis_value_mut() {
            let list = lp.to_vec_deque();
            *entry.value.as_redis_value_mut().unwrap() = RedisValue::List(list);
        }
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::List(list)) => Ok(list),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Get a list entry (read-only). Returns None if key missing, Err if wrong type.
    /// Upgrades compact encoding to full VecDeque if found.
    pub fn get_list(&mut self, key: &[u8]) -> Result<Option<&VecDeque<Bytes>>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        // Upgrade compact encoding if present
        if let Some(entry) = self.data.get_mut(key) {
            if let Some(RedisValue::ListListpack(lp)) = entry.value.as_redis_value_mut() {
                let list = lp.to_vec_deque();
                *entry.value.as_redis_value_mut().unwrap() = RedisValue::List(list);
            }
        }
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::List(list) => Ok(Some(list)),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Get or create a set entry. Returns mutable ref to inner HashSet.
    /// New keys start with full encoding. Upgrades compact encodings on access.
    pub fn get_or_create_set(
        &mut self,
        key: &[u8],
    ) -> Result<&mut HashSet<Bytes>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            let entry = Entry::new_set();
            let k = CompactKey::from(key);
            self.used_memory += entry_overhead(key, &entry);
            self.data.insert(k, entry);
        }
        let entry = self.data.get_mut(key).unwrap();
        // Upgrade compact encodings to full HashSet
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::SetListpack(lp)) => {
                let set = lp.to_hash_set();
                *entry.value.as_redis_value_mut().unwrap() = RedisValue::Set(set);
            }
            Some(RedisValue::SetIntset(is)) => {
                let set = is.to_hash_set();
                *entry.value.as_redis_value_mut().unwrap() = RedisValue::Set(set);
            }
            _ => {}
        }
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::Set(set)) => Ok(set),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Get a set entry (read-only). Returns None if key missing, Err if wrong type.
    /// Upgrades compact encodings to full HashSet if found.
    pub fn get_set(&mut self, key: &[u8]) -> Result<Option<&HashSet<Bytes>>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        // Upgrade compact encodings if present
        if let Some(entry) = self.data.get_mut(key) {
            match entry.value.as_redis_value_mut() {
                Some(RedisValue::SetListpack(lp)) => {
                    let set = lp.to_hash_set();
                    *entry.value.as_redis_value_mut().unwrap() = RedisValue::Set(set);
                }
                Some(RedisValue::SetIntset(is)) => {
                    let set = is.to_hash_set();
                    *entry.value.as_redis_value_mut().unwrap() = RedisValue::Set(set);
                }
                _ => {}
            }
        }
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::Set(set) => Ok(Some(set)),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Get or create an intset entry. Creates a new SetIntset if the key doesn't exist.
    /// Returns Err if the key exists but holds a non-set type.
    /// Returns Ok(None) if the key exists but is not an intset (caller should use get_or_create_set).
    /// Returns Ok(Some(&mut Intset)) if the key holds or was created as an intset.
    pub fn get_or_create_intset(
        &mut self,
        key: &[u8],
    ) -> Result<Option<&mut Intset>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            let entry = Entry::new_set_intset();
            let k = CompactKey::from(key);
            self.used_memory += entry_overhead(key, &entry);
            self.data.insert(k, entry);
        }
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::SetIntset(is)) => Ok(Some(is)),
            Some(RedisValue::Set(_)) | Some(RedisValue::SetListpack(_)) => Ok(None),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Upgrade an intset entry to a full HashSet and return mutable ref.
    /// Panics if the key doesn't exist or isn't a SetIntset.
    pub fn upgrade_intset_to_set(
        &mut self,
        key: &[u8],
    ) -> &mut HashSet<Bytes> {
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::SetIntset(is)) => {
                let set = is.to_hash_set();
                *entry.value.as_redis_value_mut().unwrap() = RedisValue::Set(set);
            }
            _ => {}
        }
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::Set(set)) => set,
            _ => unreachable!("upgrade_intset_to_set: expected Set after upgrade"),
        }
    }

    /// Get or create a hash entry as listpack. Creates new keys as HashListpack.
    /// Returns Ok(Some(&mut Listpack)) if the key is a HashListpack.
    /// Returns Ok(None) if the key already holds a full Hash (caller should fall through).
    /// Returns Err(WRONGTYPE) if the key holds a non-hash type.
    pub fn get_or_create_hash_listpack(
        &mut self,
        key: &[u8],
    ) -> Result<Option<&mut super::listpack::Listpack>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            let entry = Entry::new_hash_listpack();
            let k = CompactKey::from(key);
            self.used_memory += entry_overhead(key, &entry);
            self.data.insert(k, entry);
        }
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::HashListpack(lp)) => Ok(Some(lp)),
            Some(RedisValue::Hash(_)) => Ok(None),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Upgrade a HashListpack to full Hash. Returns mutable ref to the HashMap.
    /// Panics if the key doesn't exist or isn't a HashListpack.
    pub fn upgrade_hash_listpack_to_hash(
        &mut self,
        key: &[u8],
    ) -> &mut HashMap<Bytes, Bytes> {
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::HashListpack(lp)) => {
                let map = lp.to_hash_map();
                *entry.value.as_redis_value_mut().unwrap() = RedisValue::Hash(map);
            }
            _ => {}
        }
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::Hash(map)) => map,
            _ => unreachable!("upgrade_hash_listpack_to_hash: expected Hash after upgrade"),
        }
    }

    /// Get or create a list entry as listpack. Creates new keys as ListListpack.
    /// Returns Ok(Some(&mut Listpack)) if the key is a ListListpack.
    /// Returns Ok(None) if the key already holds a full List (caller should fall through).
    /// Returns Err(WRONGTYPE) if the key holds a non-list type.
    pub fn get_or_create_list_listpack(
        &mut self,
        key: &[u8],
    ) -> Result<Option<&mut super::listpack::Listpack>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            let entry = Entry::new_list_listpack();
            let k = CompactKey::from(key);
            self.used_memory += entry_overhead(key, &entry);
            self.data.insert(k, entry);
        }
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::ListListpack(lp)) => Ok(Some(lp)),
            Some(RedisValue::List(_)) => Ok(None),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Upgrade a ListListpack to full List. Returns mutable ref to the VecDeque.
    /// Panics if the key doesn't exist or isn't a ListListpack.
    pub fn upgrade_list_listpack_to_list(
        &mut self,
        key: &[u8],
    ) -> &mut VecDeque<Bytes> {
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::ListListpack(lp)) => {
                let list = lp.to_vec_deque();
                *entry.value.as_redis_value_mut().unwrap() = RedisValue::List(list);
            }
            _ => {}
        }
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::List(list)) => list,
            _ => unreachable!("upgrade_list_listpack_to_list: expected List after upgrade"),
        }
    }

    /// Get or create a sorted set entry. Returns mutable refs to both inner structures.
    ///
    /// New keys start with SortedSetBPTree encoding. Legacy SortedSet (BTreeMap)
    /// entries are upgraded to SortedSetBPTree on access for backward compatibility.
    pub fn get_or_create_sorted_set(
        &mut self,
        key: &[u8],
    ) -> Result<
        (
            &mut HashMap<Bytes, f64>,
            &mut BPTree,
        ),
        Frame,
    > {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            let entry = Entry::new_sorted_set_bptree();
            let k = CompactKey::from(key);
            self.used_memory += entry_overhead(key, &entry);
            self.data.insert(k, entry);
        }
        // Upgrade old SortedSet (BTreeMap) to SortedSetBPTree on access
        let entry = self.data.get_mut(key).unwrap();
        if let Some(RedisValue::SortedSet { members, scores }) = entry.value.as_redis_value_mut() {
            let mut tree = BPTree::new();
            let new_members = std::mem::take(members);
            let old_scores = std::mem::take(scores);
            for ((score, member), ()) in old_scores {
                tree.insert(score, member);
            }
            entry.value = CompactValue::from_redis_value(RedisValue::SortedSetBPTree {
                tree,
                members: new_members,
            });
        }
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::SortedSetBPTree { members, tree }) => Ok((members, tree)),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Get a sorted set entry (read-only). Returns None if key missing, Err if wrong type.
    pub fn get_sorted_set(
        &mut self,
        key: &[u8],
    ) -> Result<
        Option<(
            &HashMap<Bytes, f64>,
            &BPTree,
        )>,
        Frame,
    > {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        // Upgrade old SortedSet (BTreeMap) to SortedSetBPTree on access
        if let Some(entry) = self.data.get(key) {
            if matches!(entry.value.as_redis_value(), RedisValueRef::SortedSet { .. }) {
                let entry = self.data.get_mut(key).unwrap();
                if let Some(RedisValue::SortedSet { members, scores }) = entry.value.as_redis_value_mut() {
                    let mut tree = BPTree::new();
                    let new_members = std::mem::take(members);
                    let old_scores = std::mem::take(scores);
                    for ((score, member), ()) in old_scores {
                        tree.insert(score, member);
                    }
                    entry.value = CompactValue::from_redis_value(RedisValue::SortedSetBPTree {
                        tree,
                        members: new_members,
                    });
                }
            }
        }
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::SortedSetBPTree { tree, members } => Ok(Some((members, tree))),
                RedisValueRef::SortedSet { .. } => unreachable!("should have been upgraded"),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Collect keys that have an expiration set.
    pub fn keys_with_expiry(&self) -> Vec<CompactKey> {
        self.data
            .iter()
            .filter(|(_, e)| e.has_expiry())
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Check if a key exists and its expiry is in the past.
    pub fn is_key_expired(&self, key: &[u8]) -> bool {
        let now_ms = current_time_ms();
        let base_ts = self.base_timestamp;
        self.data
            .get(key)
            .is_some_and(|e| e.is_expired_at(base_ts, now_ms))
    }

    /// Read-only access to the data map (for SCAN iteration).
    pub fn data(&self) -> &DashTable<CompactKey, Entry> {
        &self.data
    }

    // ---- Read-only methods for RwLock read path ----
    // These take `now_ms` as a parameter and do NOT mutate state:
    // no expired-key removal, no LRU touch.

    /// Read-only get: checks expiry, returns None if expired, but does NOT
    /// remove expired keys or touch LRU. Used with RwLock read path.
    pub fn get_if_alive(&self, key: &[u8], now_ms: u64) -> Option<&Entry> {
        let base_ts = self.base_timestamp;
        let entry = self.data.get(key)?;
        if entry.is_expired_at(base_ts, now_ms) {
            return None;
        }
        Some(entry)
    }

    /// Read-only existence check: returns false if expired.
    pub fn exists_if_alive(&self, key: &[u8], now_ms: u64) -> bool {
        let base_ts = self.base_timestamp;
        self.data
            .get(key)
            .map(|e| !e.is_expired_at(base_ts, now_ms))
            .unwrap_or(false)
    }

    /// Read-only hash access. Returns None if key missing or expired, Err if wrong type.
    /// Compact encodings return None (caller falls through to mutable upgrade path).
    #[allow(dead_code)]
    pub fn get_hash_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<&HashMap<Bytes, Bytes>>, Frame> {
        let base_ts = self.base_timestamp;
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) if entry.is_expired_at(base_ts, now_ms) => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::Hash(map) => Ok(Some(map)),
                RedisValueRef::HashListpack(_) => Ok(None),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Read-only list access. Returns None if key missing or expired, Err if wrong type.
    /// Compact encodings return None (caller falls through to mutable upgrade path).
    #[allow(dead_code)]
    pub fn get_list_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<&VecDeque<Bytes>>, Frame> {
        let base_ts = self.base_timestamp;
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) if entry.is_expired_at(base_ts, now_ms) => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::List(list) => Ok(Some(list)),
                RedisValueRef::ListListpack(_) => Ok(None),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Read-only set access. Returns None if key missing or expired, Err if wrong type.
    /// Compact encodings return None (caller falls through to mutable upgrade path).
    #[allow(dead_code)]
    pub fn get_set_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<&HashSet<Bytes>>, Frame> {
        let base_ts = self.base_timestamp;
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) if entry.is_expired_at(base_ts, now_ms) => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::Set(set) => Ok(Some(set)),
                RedisValueRef::SetListpack(_) | RedisValueRef::SetIntset(_) => Ok(None),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Read-only sorted set access. Returns None if key missing or expired, Err if wrong type.
    /// Compact encodings return None (caller falls through to mutable upgrade path).
    #[allow(dead_code)]
    pub fn get_sorted_set_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<
        Option<(
            &HashMap<Bytes, f64>,
            &BPTree,
        )>,
        Frame,
    > {
        let base_ts = self.base_timestamp;
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) if entry.is_expired_at(base_ts, now_ms) => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::SortedSetBPTree { tree, members } => Ok(Some((members, tree))),
                RedisValueRef::SortedSet { .. }
                | RedisValueRef::SortedSetListpack(_) => Ok(None),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    // ---- Enum-based readonly accessors that handle compact encodings ----

    /// Read-only hash access via HashRef enum. Handles both HashMap and Listpack.
    pub fn get_hash_ref_if_alive(&self, key: &[u8], now_ms: u64) -> Result<Option<HashRef<'_>>, Frame> {
        let base_ts = self.base_timestamp;
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) if entry.is_expired_at(base_ts, now_ms) => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::Hash(map) => Ok(Some(HashRef::Map(map))),
                RedisValueRef::HashListpack(lp) => Ok(Some(HashRef::Listpack(lp))),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Read-only list access via ListRef enum. Handles both VecDeque and Listpack.
    pub fn get_list_ref_if_alive(&self, key: &[u8], now_ms: u64) -> Result<Option<ListRef<'_>>, Frame> {
        let base_ts = self.base_timestamp;
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) if entry.is_expired_at(base_ts, now_ms) => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::List(list) => Ok(Some(ListRef::Deque(list))),
                RedisValueRef::ListListpack(lp) => Ok(Some(ListRef::Listpack(lp))),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Read-only set access via SetRef enum. Handles HashSet, Listpack, and Intset.
    pub fn get_set_ref_if_alive(&self, key: &[u8], now_ms: u64) -> Result<Option<SetRef<'_>>, Frame> {
        let base_ts = self.base_timestamp;
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) if entry.is_expired_at(base_ts, now_ms) => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::Set(set) => Ok(Some(SetRef::Hash(set))),
                RedisValueRef::SetListpack(lp) => Ok(Some(SetRef::Listpack(lp))),
                RedisValueRef::SetIntset(is) => Ok(Some(SetRef::Intset(is))),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Read-only sorted set access via SortedSetRef enum. Handles BPTree, Listpack, and Legacy.
    pub fn get_sorted_set_ref_if_alive(&self, key: &[u8], now_ms: u64) -> Result<Option<SortedSetRef<'_>>, Frame> {
        let base_ts = self.base_timestamp;
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) if entry.is_expired_at(base_ts, now_ms) => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::SortedSetBPTree { tree, members } => {
                    Ok(Some(SortedSetRef::BPTree { tree, members }))
                }
                RedisValueRef::SortedSetListpack(lp) => Ok(Some(SortedSetRef::Listpack(lp))),
                RedisValueRef::SortedSet { members, scores } => {
                    Ok(Some(SortedSetRef::Legacy { members, scores }))
                }
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    // ---- Low-level helpers for blocking wakeup hooks ----

    /// Pop the front element from a list. Returns None if key missing/empty/wrong type.
    /// Removes the key if the list becomes empty. Handles compact listpack upgrade.
    pub fn list_pop_front(&mut self, key: &[u8]) -> Option<Bytes> {
        let list = self.get_or_create_list(key).ok()?;
        let val = list.pop_front()?;
        if list.is_empty() {
            self.remove(key);
        }
        Some(val)
    }

    /// Pop the back element from a list. Returns None if key missing/empty/wrong type.
    /// Removes the key if the list becomes empty. Handles compact listpack upgrade.
    pub fn list_pop_back(&mut self, key: &[u8]) -> Option<Bytes> {
        let list = self.get_or_create_list(key).ok()?;
        let val = list.pop_back()?;
        if list.is_empty() {
            self.remove(key);
        }
        Some(val)
    }

    /// Push an element to the front of a list. Creates the list if it does not exist.
    pub fn list_push_front(&mut self, key: &[u8], value: Bytes) {
        // get_or_create_list creates the key if missing
        if let Ok(list) = self.get_or_create_list(key) {
            list.push_front(value);
        }
    }

    /// Push an element to the back of a list. Creates the list if it does not exist.
    pub fn list_push_back(&mut self, key: &[u8], value: Bytes) {
        if let Ok(list) = self.get_or_create_list(key) {
            list.push_back(value);
        }
    }

    /// Pop the minimum element from a sorted set. Returns (member, score) or None.
    /// Removes the key if the sorted set becomes empty.
    pub fn zset_pop_min(&mut self, key: &[u8]) -> Option<(Bytes, f64)> {
        let (members, tree) = self.get_or_create_sorted_set(key).ok()?;
        let first = tree.iter().next().map(|(s, m)| (s, m.clone()))?;
        let (score, member) = first;
        tree.remove(score, &member);
        members.remove(&member);
        let result = (member, score.0);
        if members.is_empty() {
            self.remove(key);
        }
        Some(result)
    }

    /// Pop the maximum element from a sorted set. Returns (member, score) or None.
    /// Removes the key if the sorted set becomes empty.
    pub fn zset_pop_max(&mut self, key: &[u8]) -> Option<(Bytes, f64)> {
        let (members, tree) = self.get_or_create_sorted_set(key).ok()?;
        let last = tree.iter_rev().next().map(|(s, m)| (s, m.clone()))?;
        let (score, member) = last;
        tree.remove(score, &member);
        members.remove(&member);
        let result = (member, score.0);
        if members.is_empty() {
            self.remove(key);
        }
        Some(result)
    }

    /// Get or create a stream at the given key. Returns WRONGTYPE if key holds another type.
    pub fn get_or_create_stream(&mut self, key: &[u8]) -> Result<&mut StreamData, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            let entry = Entry::new_stream();
            let k = CompactKey::from(key);
            self.used_memory += entry_overhead(key, &entry);
            self.data.insert(k, entry);
        }
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::Stream(s)) => Ok(s.as_mut()),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Get a read-only reference to a stream. Returns Ok(None) if key doesn't exist.
    /// Returns WRONGTYPE error if key holds another type.
    pub fn get_stream(&mut self, key: &[u8]) -> Result<Option<&StreamData>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) => match entry.value.as_redis_value() {
                RedisValueRef::Stream(s) => Ok(Some(s)),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Get a mutable reference to an existing stream. Returns Ok(None) if key doesn't exist.
    pub fn get_stream_mut(&mut self, key: &[u8]) -> Result<Option<&mut StreamData>, Frame> {
        let now_ms = self.cached_now_ms;
        let base_ts = self.base_timestamp;
        if Self::check_expired(&self.data, key, base_ts, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        match self.data.get_mut(key) {
            None => Ok(None),
            Some(entry) => match entry.value.as_redis_value_mut() {
                Some(RedisValue::Stream(s)) => Ok(Some(s.as_mut())),
                Some(_) => Err(Self::wrongtype_error()),
                None => Err(Self::wrongtype_error()),
            },
        }
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ordered_float::OrderedFloat;

    #[test]
    fn test_set_and_get() {
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"key1"),
            Entry::new_string(Bytes::from_static(b"value1")),
        );
        let entry = db.get(b"key1").unwrap();
        match entry.value.as_redis_value() {
            RedisValueRef::String(v) => assert_eq!(v, b"value1"),
            _ => panic!("unexpected type"),
        }
    }

    #[test]
    fn test_get_missing_key() {
        let mut db = Database::new();
        assert!(db.get(b"nonexistent").is_none());
    }

    #[test]
    fn test_remove_key() {
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"key1"),
            Entry::new_string(Bytes::from_static(b"value1")),
        );
        let removed = db.remove(b"key1");
        assert!(removed.is_some());
        assert!(db.get(b"key1").is_none());
    }

    #[test]
    fn test_lazy_expiry() {
        let mut db = Database::new();
        // Set key with an expiry in the past
        let past_ms = current_time_ms() - 1000;
        let base_ts = db.base_timestamp();
        db.set(
            Bytes::from_static(b"expired"),
            Entry::new_string_with_expiry(Bytes::from_static(b"val"), past_ms, base_ts),
        );
        // get should return None and remove the key
        assert!(db.get(b"expired").is_none());
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn test_exists_with_expiry() {
        let mut db = Database::new();
        let past_ms = current_time_ms() - 1000;
        let base_ts = db.base_timestamp();
        db.set(
            Bytes::from_static(b"expired"),
            Entry::new_string_with_expiry(Bytes::from_static(b"val"), past_ms, base_ts),
        );
        assert!(!db.exists(b"expired"));
    }

    #[test]
    fn test_len_and_expires_count() {
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"k1"),
            Entry::new_string(Bytes::from_static(b"v1")),
        );
        let future_ms = current_time_ms() + 3_600_000;
        let base_ts = db.base_timestamp();
        db.set(
            Bytes::from_static(b"k2"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v2"), future_ms, base_ts),
        );
        assert_eq!(db.len(), 2);
        assert_eq!(db.expires_count(), 1);
    }

    #[test]
    fn test_is_expired() {
        let base_ts = current_secs();
        let past_ms = current_time_ms() - 1000;
        let entry = Entry::new_string_with_expiry(Bytes::from_static(b"v"), past_ms, base_ts);
        assert!(Database::is_expired(&entry, base_ts));

        let future_ms = current_time_ms() + 3_600_000;
        let entry = Entry::new_string_with_expiry(Bytes::from_static(b"v"), future_ms, base_ts);
        assert!(!Database::is_expired(&entry, base_ts));

        let entry = Entry::new_string(Bytes::from_static(b"v"));
        assert!(!Database::is_expired(&entry, base_ts));
    }

    #[test]
    fn test_get_mut() {
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"key"),
            Entry::new_string(Bytes::from_static(b"old")),
        );
        let entry = db.get_mut(b"key").unwrap();
        entry.set_string_value(Bytes::from_static(b"new"));
        let entry = db.get(b"key").unwrap();
        match entry.value.as_redis_value() {
            RedisValueRef::String(v) => assert_eq!(v, b"new"),
            _ => panic!("unexpected type"),
        }
    }

    // --- Type-checked helper tests ---

    #[test]
    fn test_get_or_create_hash() {
        let mut db = Database::new();
        let map = db.get_or_create_hash(b"myhash").unwrap();
        map.insert(Bytes::from_static(b"field"), Bytes::from_static(b"value"));
        let map = db.get_hash(b"myhash").unwrap().unwrap();
        assert_eq!(map.get(&Bytes::from_static(b"field")).unwrap().as_ref(), b"value");
    }

    #[test]
    fn test_hash_wrongtype() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k"), Bytes::from_static(b"v"));
        let result = db.get_or_create_hash(b"k");
        assert!(result.is_err());
        let result = db.get_hash(b"k");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_or_create_list() {
        let mut db = Database::new();
        let list = db.get_or_create_list(b"mylist").unwrap();
        list.push_back(Bytes::from_static(b"item"));
        let list = db.get_list(b"mylist").unwrap().unwrap();
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn test_get_or_create_set() {
        let mut db = Database::new();
        let set = db.get_or_create_set(b"myset").unwrap();
        set.insert(Bytes::from_static(b"member"));
        let set = db.get_set(b"myset").unwrap().unwrap();
        assert!(set.contains(&Bytes::from_static(b"member")));
    }

    #[test]
    fn test_get_or_create_sorted_set() {
        let mut db = Database::new();
        let (members, scores) = db.get_or_create_sorted_set(b"myzset").unwrap();
        members.insert(Bytes::from_static(b"a"), 1.0);
        scores.insert(OrderedFloat(1.0), Bytes::from_static(b"a"));
        let (members, scores) = db.get_sorted_set(b"myzset").unwrap().unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(scores.len(), 1);
    }

    #[test]
    fn test_get_hash_missing() {
        let mut db = Database::new();
        assert!(db.get_hash(b"missing").unwrap().is_none());
    }

    #[test]
    fn test_keys_with_expiry() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));
        let future_ms = current_time_ms() + 3_600_000;
        db.set_string_with_expiry(
            Bytes::from_static(b"k2"),
            Bytes::from_static(b"v2"),
            future_ms,
        );
        let keys = db.keys_with_expiry();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].as_ref(), b"k2");
    }

    #[test]
    fn test_is_key_expired() {
        let mut db = Database::new();
        let past_ms = current_time_ms() - 1000;
        let base_ts = db.base_timestamp();
        db.set(
            Bytes::from_static(b"expired"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v"), past_ms, base_ts),
        );
        assert!(db.is_key_expired(b"expired"));
        assert!(!db.is_key_expired(b"missing"));
    }

    #[test]
    fn test_data_accessor() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k"), Bytes::from_static(b"v"));
        assert_eq!(db.data().len(), 1);
    }

    #[test]
    fn test_used_memory_tracking() {
        let mut db = Database::new();
        assert_eq!(db.estimated_memory(), 0);
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"val"));
        assert!(db.estimated_memory() > 0);
        let mem_after_set = db.estimated_memory();
        db.remove(b"key");
        assert_eq!(db.estimated_memory(), 0);
        // Overwrite should not double-count
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"val"));
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"longer_value"));
        assert!(db.estimated_memory() > 0);
        // Should not equal 2x the original
        assert_ne!(db.estimated_memory(), mem_after_set * 2);
    }

    #[test]
    fn test_version_tracking() {
        let mut db = Database::new();
        assert_eq!(db.get_version(b"key"), 0);
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"v1"));
        assert_eq!(db.get_version(b"key"), 0); // first set, version 0
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"v2"));
        assert_eq!(db.get_version(b"key"), 1); // second set, version 0+1=1
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"v3"));
        assert_eq!(db.get_version(b"key"), 2);
    }

    #[test]
    fn test_increment_version() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"v"));
        assert_eq!(db.get_version(b"key"), 0);
        db.increment_version(b"key");
        assert_eq!(db.get_version(b"key"), 1);
        // non-existent key is a no-op
        db.increment_version(b"missing");
    }

    #[test]
    fn test_data_mut() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k"), Bytes::from_static(b"v"));
        assert_eq!(db.data_mut().len(), 1);
    }
}
