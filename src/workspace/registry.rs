//! Per-shard workspace metadata store.
//!
//! `WorkspaceRegistry` holds workspace metadata for a single shard.
//! Workspaces are expected to be few (tens, not thousands), so linear
//! scan for `get_by_name` is acceptable.

use std::collections::HashMap;

use bytes::Bytes;

use super::WorkspaceId;

/// Metadata for a single workspace.
#[derive(Debug, Clone)]
pub struct WorkspaceMetadata {
    /// Unique workspace identifier (UUID v7).
    pub id: WorkspaceId,
    /// Human-readable workspace name.
    pub name: Bytes,
    /// Creation timestamp (Unix milliseconds).
    pub created_at: i64,
}

/// Per-shard workspace metadata registry.
///
/// Stores workspace metadata in a `HashMap` keyed by `WorkspaceId`.
/// Thread-safety is provided by the caller (per-shard event loop or
/// `Mutex<Option<Box<WorkspaceRegistry>>>` on `ShardDatabases`).
#[derive(Debug, Default)]
pub struct WorkspaceRegistry {
    entries: HashMap<WorkspaceId, WorkspaceMetadata>,
}

impl WorkspaceRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or replace workspace metadata.
    pub fn insert(&mut self, id: WorkspaceId, meta: WorkspaceMetadata) {
        self.entries.insert(id, meta);
    }

    /// Remove a workspace by ID, returning its metadata if it existed.
    pub fn remove(&mut self, id: &WorkspaceId) -> Option<WorkspaceMetadata> {
        self.entries.remove(id)
    }

    /// Look up workspace metadata by ID.
    pub fn get(&self, id: &WorkspaceId) -> Option<&WorkspaceMetadata> {
        self.entries.get(id)
    }

    /// Look up workspace metadata by name (linear scan).
    ///
    /// Workspaces are expected to be few, so linear scan is acceptable.
    pub fn get_by_name(&self, name: &[u8]) -> Option<&WorkspaceMetadata> {
        self.entries
            .values()
            .find(|meta| meta.name.as_ref() == name)
    }

    /// Iterate over all workspace entries.
    pub fn iter(&self) -> impl Iterator<Item = (&WorkspaceId, &WorkspaceMetadata)> {
        self.entries.iter()
    }

    /// Number of registered workspaces.
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

    fn make_meta(id: WorkspaceId, name: &str) -> WorkspaceMetadata {
        WorkspaceMetadata {
            id,
            name: Bytes::from(name.to_owned()),
            created_at: 1_713_394_800_000,
        }
    }

    #[test]
    fn test_registry_insert_and_get() {
        let mut reg = WorkspaceRegistry::new();
        let id = WorkspaceId::new_v7();
        let meta = make_meta(id, "test-ws");
        reg.insert(id, meta);

        let found = reg.get(&id).unwrap();
        assert_eq!(found.id, id);
        assert_eq!(found.name.as_ref(), b"test-ws");
        assert_eq!(found.created_at, 1_713_394_800_000);
    }

    #[test]
    fn test_registry_remove() {
        let mut reg = WorkspaceRegistry::new();
        let id = WorkspaceId::new_v7();
        reg.insert(id, make_meta(id, "ws1"));
        assert_eq!(reg.len(), 1);

        let removed = reg.remove(&id);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().name.as_ref(), b"ws1");
        assert_eq!(reg.len(), 0);
        assert!(reg.get(&id).is_none());
    }

    #[test]
    fn test_registry_remove_nonexistent() {
        let mut reg = WorkspaceRegistry::new();
        let id = WorkspaceId::new_v7();
        assert!(reg.remove(&id).is_none());
    }

    #[test]
    fn test_registry_get_by_name() {
        let mut reg = WorkspaceRegistry::new();
        let id1 = WorkspaceId::new_v7();
        let id2 = WorkspaceId::new_v7();
        reg.insert(id1, make_meta(id1, "alpha"));
        reg.insert(id2, make_meta(id2, "beta"));

        let found = reg.get_by_name(b"beta").unwrap();
        assert_eq!(found.id, id2);

        assert!(reg.get_by_name(b"nonexistent").is_none());
    }

    #[test]
    fn test_registry_iter() {
        let mut reg = WorkspaceRegistry::new();
        let id1 = WorkspaceId::new_v7();
        let id2 = WorkspaceId::new_v7();
        reg.insert(id1, make_meta(id1, "ws1"));
        reg.insert(id2, make_meta(id2, "ws2"));

        let entries: Vec<_> = reg.iter().collect();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_registry_len_and_is_empty() {
        let mut reg = WorkspaceRegistry::new();
        assert!(reg.is_empty());
        assert_eq!(reg.len(), 0);

        let id = WorkspaceId::new_v7();
        reg.insert(id, make_meta(id, "ws"));
        assert!(!reg.is_empty());
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn test_registry_insert_replace() {
        let mut reg = WorkspaceRegistry::new();
        let id = WorkspaceId::new_v7();
        reg.insert(id, make_meta(id, "original"));
        reg.insert(id, make_meta(id, "replaced"));

        assert_eq!(reg.len(), 1);
        assert_eq!(reg.get(&id).unwrap().name.as_ref(), b"replaced");
    }
}
