use bytes::Bytes;
use std::collections::HashMap;
use std::time::Instant;

use super::entry::Entry;

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
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry::RedisValue;
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
        }
    }
}
