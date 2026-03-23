use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use super::entry::{current_secs, current_time_ms, Entry, RedisValue};
use crate::protocol::Frame;

/// Estimate per-entry overhead: key length + value memory + struct overhead.
fn entry_overhead(key: &[u8], entry: &Entry) -> usize {
    key.len() + entry.value.estimate_memory() + 128
}

/// An in-memory key-value database with lazy expiration.
///
/// Keys are `Bytes` (binary-safe). Values are `Entry` structs containing
/// a `RedisValue`, optional expiration, and creation timestamp.
pub struct Database {
    data: HashMap<Bytes, Entry>,
    used_memory: usize,
    /// Cached current time in epoch seconds; set once per batch to avoid
    /// repeated `SystemTime::now()` syscalls on every command.
    cached_now: u32,
    /// Cached current time in unix millis for expiry checks.
    cached_now_ms: u64,
}

impl Database {
    /// Create a new empty database.
    pub fn new() -> Self {
        Database {
            data: HashMap::new(),
            used_memory: 0,
            cached_now: current_secs(),
            cached_now_ms: current_time_ms(),
        }
    }

    /// Update the cached timestamp. Call once per batch to amortize syscall cost.
    #[inline]
    pub fn refresh_now(&mut self) {
        self.cached_now = current_secs();
        self.cached_now_ms = current_time_ms();
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

    /// Get an entry by key, performing lazy expiration and access tracking.
    ///
    /// Returns `None` if the key does not exist or has expired.
    /// Optimized: single get_mut for expiry check + LRU touch, then re-borrow as immutable.
    pub fn get(&mut self, key: &[u8]) -> Option<&Entry> {
        let now = self.cached_now;
        let now_ms = self.cached_now_ms;
        // Single mutable lookup: check expiry and touch access
        let entry = self.data.get_mut(key)?;
        if entry.is_expired_at(now_ms) {
            // Key expired -- remove it
            let removed = self.data.remove(key).unwrap();
            self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &removed));
            return None;
        }
        entry.set_last_access(now);
        // Re-borrow as immutable (second lookup, unavoidable without unsafe)
        self.data.get(key)
    }

    /// Get a mutable reference to an entry by key, performing lazy expiration and access tracking.
    ///
    /// Returns `None` if the key does not exist or has expired.
    /// Optimized: single get_mut for expiry check + LRU touch + return.
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut Entry> {
        let now = self.cached_now;
        let now_ms = self.cached_now_ms;
        // Single mutable lookup
        let entry = self.data.get_mut(key)?;
        if entry.is_expired_at(now_ms) {
            let removed = self.data.remove(key).unwrap();
            self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &removed));
            return None;
        }
        // Re-fetch after potential borrow issues (entry ref was invalidated by remove path)
        let entry = self.data.get_mut(key)?;
        entry.set_last_access(now);
        Some(entry)
    }

    /// Insert or replace an entry, tracking memory and version.
    pub fn set(&mut self, key: Bytes, mut entry: Entry) {
        // If key exists, carry forward version+1 and subtract old memory
        if let Some(old_entry) = self.data.get(&key) {
            let new_version = old_entry.version() + 1;
            entry.set_version(new_version);
            self.used_memory = self.used_memory.saturating_sub(entry_overhead(&key, old_entry));
        }
        self.used_memory += entry_overhead(&key, &entry);
        self.data.insert(key, entry);
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
        match self.data.get(key) {
            None => false,
            Some(entry) => {
                if entry.is_expired_at(now_ms) {
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
    pub fn keys(&self) -> impl Iterator<Item = &Bytes> {
        self.data.keys()
    }

    /// Set or remove expiration on an existing key.
    ///
    /// Performs lazy expiry check first. Returns `false` if the key does not
    /// exist (or has already expired). Pass 0 to remove expiry.
    pub fn set_expiry(&mut self, key: &[u8], expires_at_ms: u64) -> bool {
        let now_ms = self.cached_now_ms;
        if Self::check_expired(&self.data, key, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
            return false;
        }
        match self.data.get_mut(key) {
            Some(entry) => {
                entry.expires_at_ms = expires_at_ms;
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
    fn check_expired(data: &HashMap<Bytes, Entry>, key: &[u8], now_ms: u64) -> bool {
        data.get(key)
            .is_some_and(|e| e.is_expired_at(now_ms))
    }

    /// Convenience: set a string value with no expiry.
    pub fn set_string(&mut self, key: Bytes, value: Bytes) {
        self.set(key, Entry::new_string(value));
    }

    /// Convenience: set a string value with an expiry (unix millis).
    pub fn set_string_with_expiry(&mut self, key: Bytes, value: Bytes, expires_at_ms: u64) {
        self.set(key, Entry::new_string_with_expiry(value, expires_at_ms));
    }

    /// Check if a single entry is expired.
    fn is_expired(entry: &Entry) -> bool {
        entry.is_expired_at(current_time_ms())
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
    pub fn data_mut(&mut self) -> &mut HashMap<Bytes, Entry> {
        &mut self.data
    }

    /// Get or create a hash entry. Returns mutable ref to inner HashMap.
    /// Returns Err(WRONGTYPE) if key exists with wrong type.
    pub fn get_or_create_hash(
        &mut self,
        key: &[u8],
    ) -> Result<&mut HashMap<Bytes, Bytes>, Frame> {
        let now_ms = self.cached_now_ms;
        // Expire check
        if Self::check_expired(&self.data, key, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            let entry = Entry::new_hash();
            let k = Bytes::copy_from_slice(key);
            self.used_memory += entry_overhead(key, &entry);
            self.data.insert(k, entry);
        }
        match &mut self.data.get_mut(key).unwrap().value {
            RedisValue::Hash(map) => Ok(map),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Get a hash entry (read-only). Returns None if key missing, Err if wrong type.
    pub fn get_hash(&mut self, key: &[u8]) -> Result<Option<&HashMap<Bytes, Bytes>>, Frame> {
        let now_ms = self.cached_now_ms;
        if Self::check_expired(&self.data, key, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                RedisValue::Hash(map) => Ok(Some(map)),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Get or create a list entry. Returns mutable ref to inner VecDeque.
    pub fn get_or_create_list(
        &mut self,
        key: &[u8],
    ) -> Result<&mut VecDeque<Bytes>, Frame> {
        let now_ms = self.cached_now_ms;
        if Self::check_expired(&self.data, key, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            let entry = Entry::new_list();
            let k = Bytes::copy_from_slice(key);
            self.used_memory += entry_overhead(key, &entry);
            self.data.insert(k, entry);
        }
        match &mut self.data.get_mut(key).unwrap().value {
            RedisValue::List(list) => Ok(list),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Get a list entry (read-only). Returns None if key missing, Err if wrong type.
    pub fn get_list(&mut self, key: &[u8]) -> Result<Option<&VecDeque<Bytes>>, Frame> {
        let now_ms = self.cached_now_ms;
        if Self::check_expired(&self.data, key, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                RedisValue::List(list) => Ok(Some(list)),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Get or create a set entry. Returns mutable ref to inner HashSet.
    pub fn get_or_create_set(
        &mut self,
        key: &[u8],
    ) -> Result<&mut HashSet<Bytes>, Frame> {
        let now_ms = self.cached_now_ms;
        if Self::check_expired(&self.data, key, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            let entry = Entry::new_set();
            let k = Bytes::copy_from_slice(key);
            self.used_memory += entry_overhead(key, &entry);
            self.data.insert(k, entry);
        }
        match &mut self.data.get_mut(key).unwrap().value {
            RedisValue::Set(set) => Ok(set),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Get a set entry (read-only). Returns None if key missing, Err if wrong type.
    pub fn get_set(&mut self, key: &[u8]) -> Result<Option<&HashSet<Bytes>>, Frame> {
        let now_ms = self.cached_now_ms;
        if Self::check_expired(&self.data, key, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                RedisValue::Set(set) => Ok(Some(set)),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Get or create a sorted set entry. Returns mutable refs to both inner structures.
    pub fn get_or_create_sorted_set(
        &mut self,
        key: &[u8],
    ) -> Result<
        (
            &mut HashMap<Bytes, f64>,
            &mut BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
        ),
        Frame,
    > {
        let now_ms = self.cached_now_ms;
        if Self::check_expired(&self.data, key, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            let entry = Entry::new_sorted_set();
            let k = Bytes::copy_from_slice(key);
            self.used_memory += entry_overhead(key, &entry);
            self.data.insert(k, entry);
        }
        match &mut self.data.get_mut(key).unwrap().value {
            RedisValue::SortedSet { members, scores } => Ok((members, scores)),
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
            &BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
        )>,
        Frame,
    > {
        let now_ms = self.cached_now_ms;
        if Self::check_expired(&self.data, key, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                RedisValue::SortedSet { members, scores } => Ok(Some((members, scores))),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Collect keys that have an expiration set.
    pub fn keys_with_expiry(&self) -> Vec<Bytes> {
        self.data
            .iter()
            .filter(|(_, e)| e.has_expiry())
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Check if a key exists and its expiry is in the past.
    pub fn is_key_expired(&self, key: &[u8]) -> bool {
        let now_ms = current_time_ms();
        self.data
            .get(key)
            .is_some_and(|e| e.is_expired_at(now_ms))
    }

    /// Read-only access to the data map (for SCAN iteration).
    pub fn data(&self) -> &HashMap<Bytes, Entry> {
        &self.data
    }

    // ---- Read-only methods for RwLock read path ----
    // These take `now_ms` as a parameter and do NOT mutate state:
    // no expired-key removal, no LRU touch.

    /// Read-only get: checks expiry, returns None if expired, but does NOT
    /// remove expired keys or touch LRU. Used with RwLock read path.
    pub fn get_if_alive(&self, key: &[u8], now_ms: u64) -> Option<&Entry> {
        let entry = self.data.get(key)?;
        if entry.is_expired_at(now_ms) {
            return None;
        }
        Some(entry)
    }

    /// Read-only existence check: returns false if expired.
    pub fn exists_if_alive(&self, key: &[u8], now_ms: u64) -> bool {
        self.data
            .get(key)
            .map(|e| !e.is_expired_at(now_ms))
            .unwrap_or(false)
    }

    /// Read-only hash access. Returns None if key missing or expired, Err if wrong type.
    pub fn get_hash_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<&HashMap<Bytes, Bytes>>, Frame> {
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) if entry.is_expired_at(now_ms) => Ok(None),
            Some(entry) => match &entry.value {
                RedisValue::Hash(map) => Ok(Some(map)),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Read-only list access. Returns None if key missing or expired, Err if wrong type.
    pub fn get_list_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<&VecDeque<Bytes>>, Frame> {
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) if entry.is_expired_at(now_ms) => Ok(None),
            Some(entry) => match &entry.value {
                RedisValue::List(list) => Ok(Some(list)),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Read-only set access. Returns None if key missing or expired, Err if wrong type.
    pub fn get_set_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<&HashSet<Bytes>>, Frame> {
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) if entry.is_expired_at(now_ms) => Ok(None),
            Some(entry) => match &entry.value {
                RedisValue::Set(set) => Ok(Some(set)),
                _ => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Read-only sorted set access. Returns None if key missing or expired, Err if wrong type.
    pub fn get_sorted_set_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<
        Option<(
            &HashMap<Bytes, f64>,
            &BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
        )>,
        Frame,
    > {
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) if entry.is_expired_at(now_ms) => Ok(None),
            Some(entry) => match &entry.value {
                RedisValue::SortedSet { members, scores } => Ok(Some((members, scores))),
                _ => Err(Self::wrongtype_error()),
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
        match &entry.value {
            RedisValue::String(v) => assert_eq!(v.as_ref(), b"value1"),
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
        db.set(
            Bytes::from_static(b"expired"),
            Entry::new_string_with_expiry(Bytes::from_static(b"val"), past_ms),
        );
        // get should return None and remove the key
        assert!(db.get(b"expired").is_none());
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn test_exists_with_expiry() {
        let mut db = Database::new();
        let past_ms = current_time_ms() - 1000;
        db.set(
            Bytes::from_static(b"expired"),
            Entry::new_string_with_expiry(Bytes::from_static(b"val"), past_ms),
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
        db.set(
            Bytes::from_static(b"k2"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v2"), future_ms),
        );
        assert_eq!(db.len(), 2);
        assert_eq!(db.expires_count(), 1);
    }

    #[test]
    fn test_is_expired() {
        let past_ms = current_time_ms() - 1000;
        let entry = Entry::new_string_with_expiry(Bytes::from_static(b"v"), past_ms);
        assert!(Database::is_expired(&entry));

        let future_ms = current_time_ms() + 3_600_000;
        let entry = Entry::new_string_with_expiry(Bytes::from_static(b"v"), future_ms);
        assert!(!Database::is_expired(&entry));

        let entry = Entry::new_string(Bytes::from_static(b"v"));
        assert!(!Database::is_expired(&entry));
    }

    #[test]
    fn test_get_mut() {
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"key"),
            Entry::new_string(Bytes::from_static(b"old")),
        );
        let entry = db.get_mut(b"key").unwrap();
        entry.value = RedisValue::String(Bytes::from_static(b"new"));
        let entry = db.get(b"key").unwrap();
        match &entry.value {
            RedisValue::String(v) => assert_eq!(v.as_ref(), b"new"),
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
        scores.insert((OrderedFloat(1.0), Bytes::from_static(b"a")), ());
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
        db.set(
            Bytes::from_static(b"expired"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v"), past_ms),
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
