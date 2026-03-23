use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Instant;

use super::entry::{Entry, RedisValue};
use crate::protocol::Frame;

/// An in-memory key-value database with lazy expiration.
///
/// Keys are `Bytes` (binary-safe). Values are `Entry` structs containing
/// a `RedisValue`, optional expiration, and creation timestamp.
pub struct Database {
    data: HashMap<Bytes, Entry>,
}

impl Database {
    /// Create a new empty database.
    pub fn new() -> Self {
        Database {
            data: HashMap::new(),
        }
    }

    /// Get an entry by key, performing lazy expiration.
    ///
    /// Returns `None` if the key does not exist or has expired.
    pub fn get(&mut self, key: &[u8]) -> Option<&Entry> {
        if Self::check_expired(&self.data, key) {
            self.data.remove(key);
            return None;
        }
        self.data.get(key)
    }

    /// Get a mutable reference to an entry by key, performing lazy expiration.
    ///
    /// Returns `None` if the key does not exist or has expired.
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut Entry> {
        if Self::check_expired(&self.data, key) {
            self.data.remove(key);
            return None;
        }
        self.data.get_mut(key)
    }

    /// Insert or replace an entry.
    pub fn set(&mut self, key: Bytes, entry: Entry) {
        self.data.insert(key, entry);
    }

    /// Remove a key and return its entry. No expiry check needed (DEL removes regardless).
    pub fn remove(&mut self, key: &[u8]) -> Option<Entry> {
        self.data.remove(key)
    }

    /// Check if a key exists, performing lazy expiration.
    pub fn exists(&mut self, key: &[u8]) -> bool {
        if Self::check_expired(&self.data, key) {
            self.data.remove(key);
            return false;
        }
        self.data.contains_key(key)
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
    /// exist (or has already expired).
    pub fn set_expiry(&mut self, key: &[u8], expires_at: Option<Instant>) -> bool {
        if Self::check_expired(&self.data, key) {
            self.data.remove(key);
            return false;
        }
        match self.data.get_mut(key) {
            Some(entry) => {
                entry.expires_at = expires_at;
                true
            }
            None => false,
        }
    }

    /// Count entries that have an expiration set.
    pub fn expires_count(&self) -> usize {
        self.data.values().filter(|e| e.expires_at.is_some()).count()
    }

    /// Check if an entry is expired without requiring &mut self.
    fn check_expired(data: &HashMap<Bytes, Entry>, key: &[u8]) -> bool {
        data.get(key)
            .and_then(|e| e.expires_at)
            .is_some_and(|exp| Instant::now() >= exp)
    }

    /// Convenience: set a string value with no expiry.
    pub fn set_string(&mut self, key: Bytes, value: Bytes) {
        self.set(key, Entry::new_string(value));
    }

    /// Convenience: set a string value with an expiry.
    pub fn set_string_with_expiry(&mut self, key: Bytes, value: Bytes, expires_at: Instant) {
        self.set(key, Entry::new_string_with_expiry(value, expires_at));
    }

    /// Check if a single entry is expired.
    fn is_expired(entry: &Entry) -> bool {
        entry
            .expires_at
            .is_some_and(|exp| Instant::now() >= exp)
    }

    /// WRONGTYPE error frame.
    fn wrongtype_error() -> Frame {
        Frame::Error(Bytes::from_static(
            b"WRONGTYPE Operation against a key holding the wrong kind of value",
        ))
    }

    /// Get or create a hash entry. Returns mutable ref to inner HashMap.
    /// Returns Err(WRONGTYPE) if key exists with wrong type.
    pub fn get_or_create_hash(
        &mut self,
        key: &[u8],
    ) -> Result<&mut HashMap<Bytes, Bytes>, Frame> {
        // Expire check
        if Self::check_expired(&self.data, key) {
            self.data.remove(key);
        }
        if !self.data.contains_key(key) {
            self.data
                .insert(Bytes::copy_from_slice(key), Entry::new_hash());
        }
        match &mut self.data.get_mut(key).unwrap().value {
            RedisValue::Hash(map) => Ok(map),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Get a hash entry (read-only). Returns None if key missing, Err if wrong type.
    pub fn get_hash(&mut self, key: &[u8]) -> Result<Option<&HashMap<Bytes, Bytes>>, Frame> {
        if Self::check_expired(&self.data, key) {
            self.data.remove(key);
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
        if Self::check_expired(&self.data, key) {
            self.data.remove(key);
        }
        if !self.data.contains_key(key) {
            self.data
                .insert(Bytes::copy_from_slice(key), Entry::new_list());
        }
        match &mut self.data.get_mut(key).unwrap().value {
            RedisValue::List(list) => Ok(list),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Get a list entry (read-only). Returns None if key missing, Err if wrong type.
    pub fn get_list(&mut self, key: &[u8]) -> Result<Option<&VecDeque<Bytes>>, Frame> {
        if Self::check_expired(&self.data, key) {
            self.data.remove(key);
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
        if Self::check_expired(&self.data, key) {
            self.data.remove(key);
        }
        if !self.data.contains_key(key) {
            self.data
                .insert(Bytes::copy_from_slice(key), Entry::new_set());
        }
        match &mut self.data.get_mut(key).unwrap().value {
            RedisValue::Set(set) => Ok(set),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Get a set entry (read-only). Returns None if key missing, Err if wrong type.
    pub fn get_set(&mut self, key: &[u8]) -> Result<Option<&HashSet<Bytes>>, Frame> {
        if Self::check_expired(&self.data, key) {
            self.data.remove(key);
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
        if Self::check_expired(&self.data, key) {
            self.data.remove(key);
        }
        if !self.data.contains_key(key) {
            self.data
                .insert(Bytes::copy_from_slice(key), Entry::new_sorted_set());
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
        if Self::check_expired(&self.data, key) {
            self.data.remove(key);
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
            .filter(|(_, e)| e.expires_at.is_some())
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Check if a key exists and its expiry is in the past.
    pub fn is_key_expired(&self, key: &[u8]) -> bool {
        self.data
            .get(key)
            .and_then(|e| e.expires_at)
            .is_some_and(|exp| Instant::now() >= exp)
    }

    /// Read-only access to the data map (for SCAN iteration).
    pub fn data(&self) -> &HashMap<Bytes, Entry> {
        &self.data
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
    use std::time::Duration;

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
        let past = Instant::now() - Duration::from_secs(1);
        db.set(
            Bytes::from_static(b"expired"),
            Entry::new_string_with_expiry(Bytes::from_static(b"val"), past),
        );
        // get should return None and remove the key
        assert!(db.get(b"expired").is_none());
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn test_exists_with_expiry() {
        let mut db = Database::new();
        let past = Instant::now() - Duration::from_secs(1);
        db.set(
            Bytes::from_static(b"expired"),
            Entry::new_string_with_expiry(Bytes::from_static(b"val"), past),
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
        let future = Instant::now() + Duration::from_secs(3600);
        db.set(
            Bytes::from_static(b"k2"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v2"), future),
        );
        assert_eq!(db.len(), 2);
        assert_eq!(db.expires_count(), 1);
    }

    #[test]
    fn test_is_expired() {
        let past = Instant::now() - Duration::from_secs(1);
        let entry = Entry::new_string_with_expiry(Bytes::from_static(b"v"), past);
        assert!(Database::is_expired(&entry));

        let future = Instant::now() + Duration::from_secs(3600);
        let entry = Entry::new_string_with_expiry(Bytes::from_static(b"v"), future);
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
        let future = Instant::now() + Duration::from_secs(3600);
        db.set_string_with_expiry(
            Bytes::from_static(b"k2"),
            Bytes::from_static(b"v2"),
            future,
        );
        let keys = db.keys_with_expiry();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].as_ref(), b"k2");
    }

    #[test]
    fn test_is_key_expired() {
        let mut db = Database::new();
        let past = Instant::now() - Duration::from_secs(1);
        db.set(
            Bytes::from_static(b"expired"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v"), past),
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
}
