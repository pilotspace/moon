//! GraphStore -- per-shard graph store with lazy initialization.
//!
//! No Arc, no Mutex -- fully owned by shard thread (same pattern as VectorStore).
//! Zero memory when no graphs exist.

use std::collections::HashMap;

use bytes::Bytes;

use crate::graph::segment::GraphSegmentHolder;

/// Errors from GraphStore operations.
#[derive(Debug, PartialEq, Eq)]
pub enum GraphStoreError {
    /// A graph with this name already exists.
    GraphAlreadyExists,
    /// No graph with this name was found.
    GraphNotFound,
}

/// A single named graph with its segment holder and configuration.
pub struct NamedGraph {
    /// Graph name.
    pub name: Bytes,
    /// Segment holder (ArcSwap-based, lock-free reads).
    pub segments: GraphSegmentHolder,
    /// Shard-local mutable write buffer. Writes go here; periodically
    /// published to `segments` ArcSwap for concurrent readers.
    /// Kept separate from ArcSwap to avoid Arc cloning on every mutation.
    pub write_buf: crate::graph::memgraph::MemGraph,
    /// Edge threshold for mutable segment freeze.
    pub edge_threshold: usize,
    /// LSN at which this graph was created.
    pub created_lsn: u64,
}

/// Per-shard graph store. No Arc, no Mutex -- fully owned by shard thread.
///
/// Lazy initialization: `graphs` is `None` until the first graph is created,
/// ensuring zero allocation overhead when no graphs exist.
pub struct GraphStore {
    graphs: Option<HashMap<Bytes, NamedGraph>>,
}

impl GraphStore {
    /// Create an empty GraphStore with zero allocation.
    pub fn new() -> Self {
        Self { graphs: None }
    }

    /// Create a new named graph. Lazily initializes the HashMap on first call.
    pub fn create_graph(
        &mut self,
        name: Bytes,
        edge_threshold: usize,
        lsn: u64,
    ) -> Result<(), GraphStoreError> {
        let map = self.graphs.get_or_insert_with(HashMap::new);
        if map.contains_key(&name) {
            return Err(GraphStoreError::GraphAlreadyExists);
        }
        map.insert(
            name.clone(),
            NamedGraph {
                name,
                segments: GraphSegmentHolder::new(edge_threshold),
                write_buf: crate::graph::memgraph::MemGraph::new(edge_threshold),
                edge_threshold,
                created_lsn: lsn,
            },
        );
        Ok(())
    }

    /// Drop a named graph. If the HashMap becomes empty, reclaim memory.
    pub fn drop_graph(&mut self, name: &[u8]) -> Result<(), GraphStoreError> {
        let Some(map) = self.graphs.as_mut() else {
            return Err(GraphStoreError::GraphNotFound);
        };
        if map.remove(name).is_none() {
            return Err(GraphStoreError::GraphNotFound);
        }
        if map.is_empty() {
            self.graphs = None;
        }
        Ok(())
    }

    /// Look up a named graph (immutable).
    pub fn get_graph(&self, name: &[u8]) -> Option<&NamedGraph> {
        self.graphs.as_ref()?.get(name)
    }

    /// Look up a named graph (mutable).
    pub fn get_graph_mut(&mut self, name: &[u8]) -> Option<&mut NamedGraph> {
        self.graphs.as_mut()?.get_mut(name)
    }

    /// List all graph names. Returns empty vec if no graphs exist.
    pub fn list_graphs(&self) -> Vec<&Bytes> {
        match &self.graphs {
            Some(map) => map.keys().collect(),
            None => Vec::new(),
        }
    }

    /// Number of graphs.
    pub fn graph_count(&self) -> usize {
        match &self.graphs {
            Some(map) => map.len(),
            None => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_allocates_nothing() {
        let store = GraphStore::new();
        assert!(store.graphs.is_none());
        assert_eq!(store.graph_count(), 0);
    }

    #[test]
    fn test_create_get_drop_lifecycle() {
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"social"), 64_000, 1)
            .expect("ok");
        assert_eq!(store.graph_count(), 1);

        let g = store.get_graph(b"social").expect("exists");
        assert_eq!(g.name, Bytes::from_static(b"social"));
        assert_eq!(g.edge_threshold, 64_000);
        assert_eq!(g.created_lsn, 1);

        store.drop_graph(b"social").expect("ok");
        assert_eq!(store.graph_count(), 0);
    }

    #[test]
    fn test_list_graphs() {
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"alpha"), 1000, 1)
            .expect("ok");
        store
            .create_graph(Bytes::from_static(b"beta"), 2000, 2)
            .expect("ok");

        let list = store.list_graphs();
        let mut names: Vec<&[u8]> = list.iter().map(|b| b.as_ref()).collect();
        names.sort();
        assert_eq!(names, vec![&b"alpha"[..], &b"beta"[..]]);
    }

    #[test]
    fn test_drop_last_graph_reclaims() {
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"only"), 1000, 1)
            .expect("ok");
        assert!(store.graphs.is_some());

        store.drop_graph(b"only").expect("ok");
        assert!(store.graphs.is_none());
    }

    #[test]
    fn test_create_duplicate_error() {
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"dup"), 1000, 1)
            .expect("ok");
        assert_eq!(
            store
                .create_graph(Bytes::from_static(b"dup"), 1000, 2)
                .unwrap_err(),
            GraphStoreError::GraphAlreadyExists
        );
    }

    #[test]
    fn test_drop_nonexistent_error() {
        let mut store = GraphStore::new();
        assert_eq!(
            store.drop_graph(b"nope").unwrap_err(),
            GraphStoreError::GraphNotFound
        );
    }

    #[test]
    fn test_get_graph_mut() {
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"mutable"), 1000, 1)
            .expect("ok");
        let g = store.get_graph_mut(b"mutable").expect("exists");
        g.edge_threshold = 2000;

        let g = store.get_graph(b"mutable").expect("exists");
        assert_eq!(g.edge_threshold, 2000);
    }
}
