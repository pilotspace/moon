//! GraphStore -- per-shard graph store with lazy initialization.
//!
//! No Arc, no Mutex -- fully owned by shard thread (same pattern as VectorStore).
//! Zero memory when no graphs exist.

use std::collections::HashMap;
use std::io;
use std::path::Path;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::graph::csr::CsrSegment;
use crate::graph::index::PropertyIndex;
use crate::graph::segment::GraphSegmentHolder;
use crate::graph::stats::GraphStats;

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
    /// Optional property indexes for cross-segment numeric range queries.
    /// Key is the property name dictionary ID.
    pub property_indexes: HashMap<u16, PropertyIndex>,
    /// Per-graph statistics for cost-based query planning.
    /// Updated incrementally on node/edge insert/delete.
    pub stats: GraphStats,
    /// Cypher plan cache: xxhash64 of query string → compiled PhysicalPlan.
    /// Wrapped in Mutex for interior mutability (reads insert cache misses).
    pub plan_cache: parking_lot::Mutex<crate::graph::cypher::planner::PlanCache>,
}

impl NamedGraph {
    /// Returns true if the mutable write buffer has exceeded the edge threshold
    /// and should be frozen + compacted into an immutable CSR segment.
    pub fn should_compact(&self) -> bool {
        self.write_buf.should_freeze()
    }

    /// Freeze the current MemGraph, convert to CSR, and push the new immutable
    /// segment into the segment holder. Replaces write_buf with a fresh MemGraph.
    ///
    /// Returns `true` if compaction succeeded, `false` if freeze/CSR build failed.
    pub fn freeze_and_compact(&mut self, lsn: u64) -> bool {
        // Freeze: drains all live nodes/edges from write_buf.
        let frozen = match self.write_buf.freeze() {
            Ok(f) => f,
            Err(_) => return false,
        };

        // Convert frozen MemGraph to a CSR segment.
        match CsrSegment::from_frozen(frozen, lsn) {
            Ok(csr) => {
                self.segments.add_immutable(csr);
                // Replace write_buf with a fresh empty MemGraph.
                self.write_buf =
                    crate::graph::memgraph::MemGraph::new(self.edge_threshold);
                true
            }
            Err(_) => {
                // CSR build failed -- write_buf is already frozen/drained.
                // Replace with fresh MemGraph so writes can continue.
                self.write_buf =
                    crate::graph::memgraph::MemGraph::new(self.edge_threshold);
                false
            }
        }
    }
}

/// Per-shard graph store. No Arc, no Mutex -- fully owned by shard thread.
///
/// Lazy initialization: `graphs` is `None` until the first graph is created,
/// ensuring zero allocation overhead when no graphs exist.
pub struct GraphStore {
    graphs: Option<HashMap<Bytes, NamedGraph>>,
    /// Monotonic LSN counter for MVCC versioning of graph mutations.
    next_lsn: u64,
    /// Pending WAL records produced by write handlers. Connection handlers
    /// drain this after dispatch and send bytes via `shard_databases.wal_append()`.
    pub(crate) wal_pending: Vec<Vec<u8>>,
}

impl GraphStore {
    /// Create an empty GraphStore with zero allocation.
    pub fn new() -> Self {
        Self {
            graphs: None,
            next_lsn: 0,
            wal_pending: Vec::new(),
        }
    }

    /// Allocate the next monotonic LSN for a graph mutation.
    pub fn allocate_lsn(&mut self) -> u64 {
        let lsn = self.next_lsn;
        self.next_lsn += 1;
        lsn
    }

    /// Drain all pending WAL records, returning them and leaving the vec empty.
    pub fn drain_wal(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.wal_pending)
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
                property_indexes: HashMap::new(),
                stats: GraphStats::new(),
                plan_cache: parking_lot::Mutex::new(
                    crate::graph::cypher::planner::PlanCache::new(1024),
                ),
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

    /// Serialize graph metadata (names, thresholds, LSNs) to a JSON file.
    ///
    /// This captures enough information to recreate the GraphStore structure
    /// on recovery. Actual graph data lives in CSR segment files and WAL.
    pub fn save_metadata(&self, path: &Path) -> io::Result<()> {
        let entries: Vec<GraphMetadataEntry> = match &self.graphs {
            Some(map) => map
                .values()
                .map(|g| GraphMetadataEntry {
                    name: String::from_utf8_lossy(&g.name).into_owned(),
                    edge_threshold: g.edge_threshold,
                    created_lsn: g.created_lsn,
                })
                .collect(),
            None => Vec::new(),
        };

        let meta = GraphStoreMetadata { graphs: entries };
        let json = serde_json::to_string_pretty(&meta)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        std::fs::write(path, json.as_bytes())
    }

    /// Load graph metadata from a JSON file and recreate graph shells.
    ///
    /// This creates empty NamedGraph entries (with fresh MemGraphs).
    /// CSR segments and WAL data are loaded separately during recovery.
    pub fn load_metadata(path: &Path) -> io::Result<Self> {
        let data = std::fs::read(path)?;
        let meta: GraphStoreMetadata = serde_json::from_slice(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let mut store = Self::new();
        for entry in &meta.graphs {
            let name = Bytes::from(entry.name.clone());
            // Ignore duplicate errors (shouldn't happen from valid metadata).
            let _ = store.create_graph(name, entry.edge_threshold, entry.created_lsn);
        }
        Ok(store)
    }
}

/// Serializable graph metadata entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GraphMetadataEntry {
    name: String,
    edge_threshold: usize,
    created_lsn: u64,
}

/// Top-level graph store metadata for JSON serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GraphStoreMetadata {
    graphs: Vec<GraphMetadataEntry>,
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

    // --- Phase 121: Metadata persistence tests ---

    #[test]
    fn test_save_load_metadata_roundtrip() {
        let dir = tempfile::TempDir::new().expect("tmpdir");
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"social"), 64_000, 10)
            .expect("ok");
        store
            .create_graph(Bytes::from_static(b"knowledge"), 32_000, 20)
            .expect("ok");

        let path = dir.path().join("graph_meta.json");
        store.save_metadata(&path).expect("save ok");

        let restored = GraphStore::load_metadata(&path).expect("load ok");
        assert_eq!(restored.graph_count(), 2);

        let g = restored.get_graph(b"social").expect("exists");
        assert_eq!(g.edge_threshold, 64_000);
        assert_eq!(g.created_lsn, 10);

        let g = restored.get_graph(b"knowledge").expect("exists");
        assert_eq!(g.edge_threshold, 32_000);
        assert_eq!(g.created_lsn, 20);
    }

    #[test]
    fn test_save_load_empty_store() {
        let dir = tempfile::TempDir::new().expect("tmpdir");
        let store = GraphStore::new();
        let path = dir.path().join("empty_meta.json");
        store.save_metadata(&path).expect("save ok");

        let restored = GraphStore::load_metadata(&path).expect("load ok");
        assert_eq!(restored.graph_count(), 0);
    }

    #[test]
    fn test_load_metadata_missing_file() {
        let result = GraphStore::load_metadata(std::path::Path::new("/nonexistent/meta.json"));
        assert!(result.is_err());
    }

    #[test]
    fn test_allocate_lsn_monotonic() {
        let mut store = GraphStore::new();
        assert_eq!(store.allocate_lsn(), 0);
        assert_eq!(store.allocate_lsn(), 1);
        assert_eq!(store.allocate_lsn(), 2);
    }

    #[test]
    fn test_drain_wal_returns_and_clears() {
        let mut store = GraphStore::new();
        store.wal_pending.push(vec![1, 2, 3]);
        store.wal_pending.push(vec![4, 5, 6]);
        let drained = store.drain_wal();
        assert_eq!(drained.len(), 2);
        assert!(store.wal_pending.is_empty());
    }
}
