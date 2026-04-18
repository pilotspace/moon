//! Per-shard durable queue metadata store.
//!
//! `DurableQueueRegistry` holds durable queue configurations for a single
//! shard. Thread-safety is provided by the caller (per-shard event loop or
//! `Mutex<Option<Box<DurableQueueRegistry>>>` on `ShardDatabases`).

use std::collections::HashMap;

use bytes::Bytes;

use super::DurableStreamConfig;

/// Per-shard durable queue registry.
///
/// Stores `DurableStreamConfig` entries keyed by queue key bytes.
/// Queues are expected to be few per shard, so HashMap is acceptable.
#[derive(Debug, Default)]
pub struct DurableQueueRegistry {
    entries: HashMap<Bytes, DurableStreamConfig>,
}

impl DurableQueueRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or replace a durable queue configuration.
    pub fn insert(&mut self, key: Bytes, config: DurableStreamConfig) {
        self.entries.insert(key, config);
    }

    /// Remove a durable queue by key, returning its config if it existed.
    pub fn remove(&mut self, key: &[u8]) -> Option<DurableStreamConfig> {
        self.entries.remove(key)
    }

    /// Look up a durable queue config by key.
    pub fn get(&self, key: &[u8]) -> Option<&DurableStreamConfig> {
        self.entries.get(key)
    }

    /// Iterate over all entries.
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &DurableStreamConfig)> {
        self.entries.iter()
    }

    /// Number of registered durable queues.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(key: &[u8]) -> DurableStreamConfig {
        DurableStreamConfig::new(Bytes::copy_from_slice(key), 3)
    }

    #[test]
    fn test_registry_new_is_empty() {
        let reg = DurableQueueRegistry::new();
        assert!(reg.is_empty());
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn test_registry_insert_and_get() {
        let mut reg = DurableQueueRegistry::new();
        let key = Bytes::from_static(b"orders");
        let config = make_config(b"orders");
        reg.insert(key, config);

        let found = reg.get(b"orders");
        assert!(found.is_some());
        let found = found.unwrap();
        assert_eq!(found.queue_key.as_ref(), b"orders");
        assert_eq!(found.max_delivery_count, 3);
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn test_registry_remove() {
        let mut reg = DurableQueueRegistry::new();
        let key = Bytes::from_static(b"tasks");
        reg.insert(key, make_config(b"tasks"));
        assert_eq!(reg.len(), 1);

        let removed = reg.remove(b"tasks");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().queue_key.as_ref(), b"tasks");
        assert_eq!(reg.len(), 0);
        assert!(reg.get(b"tasks").is_none());
    }

    #[test]
    fn test_registry_remove_nonexistent() {
        let mut reg = DurableQueueRegistry::new();
        assert!(reg.remove(b"nonexistent").is_none());
    }

    #[test]
    fn test_registry_iter() {
        let mut reg = DurableQueueRegistry::new();
        reg.insert(Bytes::from_static(b"q1"), make_config(b"q1"));
        reg.insert(Bytes::from_static(b"q2"), make_config(b"q2"));
        reg.insert(Bytes::from_static(b"q3"), make_config(b"q3"));

        let mut keys: Vec<&[u8]> = reg.iter().map(|(k, _)| k.as_ref()).collect();
        keys.sort();
        assert_eq!(keys, vec![&b"q1"[..], &b"q2"[..], &b"q3"[..]]);
    }

    #[test]
    fn test_registry_insert_replaces() {
        let mut reg = DurableQueueRegistry::new();
        let key = Bytes::from_static(b"q");
        reg.insert(key.clone(), DurableStreamConfig::new(key.clone(), 3));
        reg.insert(key.clone(), DurableStreamConfig::new(key.clone(), 10));

        assert_eq!(reg.len(), 1);
        assert_eq!(reg.get(b"q").unwrap().max_delivery_count, 10);
    }
}
