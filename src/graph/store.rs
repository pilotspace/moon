//! GraphStore -- per-shard graph store with lazy initialization.
//!
//! No Arc, no Mutex -- fully owned by shard thread (same pattern as VectorStore).
//! Zero memory when no graphs exist.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::graph::csr::CsrSegment;
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
    /// Per-graph statistics for cost-based query planning.
    /// Updated incrementally on node/edge insert/delete.
    pub stats: GraphStats,
    /// Cypher plan cache: xxhash64 of query string → compiled PhysicalPlan.
    /// Wrapped in Mutex for interior mutability (reads insert cache misses).
    pub plan_cache: parking_lot::Mutex<crate::graph::cypher::planner::PlanCache>,
    /// Redis key → NodeKey mapping for O(1) lookup during graph expansion.
    /// Populated when a node has a `_key` property linking it to a Redis HASH key.
    pub key_to_node: HashMap<Bytes, crate::graph::types::NodeKey>,
}

impl NamedGraph {
    /// Returns true if the mutable write buffer has exceeded the edge threshold
    /// and should be frozen + compacted into an immutable CSR segment.
    pub fn should_compact(&self) -> bool {
        self.write_buf.should_freeze()
    }

    /// Register a Redis key → NodeKey mapping for graph expansion lookup.
    pub fn register_key(&mut self, key: Bytes, node: crate::graph::types::NodeKey) {
        self.key_to_node.insert(key, node);
    }

    /// Look up a NodeKey by Redis key. Returns `None` if no mapping exists.
    pub fn lookup_node_by_key(&self, key: &[u8]) -> Option<crate::graph::types::NodeKey> {
        self.key_to_node.get(key).copied()
    }

    /// Remove a Redis key mapping (e.g. when a node is deleted).
    pub fn unregister_key(&mut self, key: &[u8]) {
        self.key_to_node.remove(key);
    }

    /// Freeze the current MemGraph, convert to CSR, and push the new immutable
    /// segment into the segment holder. Thaws write_buf in place afterwards
    /// (id-allocation cursors are monotonic — see key-uniqueness note below).
    ///
    /// Returns `true` if compaction succeeded, `false` if freeze/CSR build failed.
    pub fn freeze_and_compact(&mut self, lsn: u64) -> bool {
        // Copy-up tombstones (dead write-buf nodes that shadow a frozen CSR
        // row) must survive the freeze: freeze() drops dead nodes, which
        // would RESURRECT the underlying frozen row. Collect them now and
        // re-materialize after the thaw.
        let dead_shadows: Vec<(crate::graph::types::NodeKey, u64)> = {
            let seg_list = self.segments.load();
            self.write_buf
                .iter_dead_nodes()
                .filter(|(k, _)| {
                    seg_list
                        .immutable
                        .iter()
                        .any(|s| s.lookup_node(*k).is_some())
                })
                .map(|(k, n)| (k, n.deleted_lsn))
                .collect()
        };

        // Freeze: drains all live nodes/edges from write_buf.
        let frozen = match self.write_buf.freeze() {
            Ok(f) => f,
            Err(_) => return false,
        };

        // Nothing freezable (e.g. the buffer holds only cross-tier delta
        // edges, which freeze() retains): skip — pushing an empty segment
        // per write attempt would churn the segment list.
        if frozen.nodes.is_empty() && frozen.edges.is_empty() {
            self.write_buf.thaw();
            self.restore_dead_shadows(&dead_shadows);
            return false;
        }

        // Convert frozen MemGraph to a CSR segment. Either way, thaw the
        // drained write_buf IN PLACE — the monotonic id cursors are never
        // reset, so post-freeze NodeKeys can never collide with the
        // external_ids of frozen CSR rows (a fresh MemGraph would restart
        // allocation at the same keys and alias new nodes onto frozen rows).
        let ok = match CsrSegment::from_frozen(frozen, lsn) {
            Ok(csr) => {
                self.segments.add_immutable(csr);
                true
            }
            // CSR build failed — write_buf is already drained; thaw so
            // writes can continue.
            Err(_) => false,
        };
        self.write_buf.thaw();
        self.restore_dead_shadows(&dead_shadows);
        ok
    }

    /// Re-materialize copy-up tombstones after a freeze drained them:
    /// copy the frozen row's data back into the write buffer, then re-delete
    /// it at its ORIGINAL deletion LSN (preserves MVCC time-travel).
    fn restore_dead_shadows(&mut self, dead_shadows: &[(crate::graph::types::NodeKey, u64)]) {
        if dead_shadows.is_empty() {
            return;
        }
        let seg_list = self.segments.load();
        for &(key, deleted_lsn) in dead_shadows {
            if copy_up_into(&mut self.write_buf, &seg_list.immutable, key) {
                self.write_buf.remove_node(key, deleted_lsn);
            }
        }
    }

    /// Copy a frozen node's data into the write buffer under the SAME key so
    /// it can be mutated in place ("copy-up"). No-op if the key is already
    /// resident. Returns `false` when the key is not in any segment or the
    /// frozen row is itself deleted.
    ///
    /// The shadow takes read precedence over the frozen row everywhere
    /// (`MergedNodeView` and `SegmentMergeReader` check the mutable tier
    /// first), so a later soft-delete of the shadow tombstones the frozen row.
    pub fn copy_up_node(&mut self, key: crate::graph::types::NodeKey) -> bool {
        if self.write_buf.get_node(key).is_some() {
            return true;
        }
        let seg_list = self.segments.load();
        copy_up_into(&mut self.write_buf, &seg_list.immutable, key)
    }
}

/// Shared copy-up core: materialize `key`'s newest frozen row (labels,
/// properties, embedding, MVCC stamps) into `write_buf` under the same key.
/// Used by [`NamedGraph::copy_up_node`] and WAL replay (which owns the
/// MemGraph outside the graph during replay).
pub(crate) fn copy_up_into(
    write_buf: &mut crate::graph::memgraph::MemGraph,
    segments: &[std::sync::Arc<crate::graph::csr::CsrStorage>],
    key: crate::graph::types::NodeKey,
) -> bool {
    use slotmap::Key;
    if write_buf.get_node(key).is_some() {
        return true;
    }
    // Newest segment wins (same precedence as MergedNodeView::segment_row;
    // the list is ordered newest-first).
    for seg in segments.iter() {
        let Some(row) = seg.lookup_node(key) else {
            continue;
        };
        let Some(meta) = seg.node_meta().get(row as usize) else {
            continue;
        };
        if meta.deleted_lsn != u64::MAX {
            return false;
        }
        let (created_lsn, valid_from, valid_to) =
            (meta.created_lsn, meta.valid_from, meta.valid_to);
        // Decode labels: bitmap (ids 0-31) + sparse overflow (ids >= 32).
        let mut labels: smallvec::SmallVec<[u16; 4]> = smallvec::SmallVec::new();
        let mut bm = meta.label_bitmap;
        while bm != 0 {
            labels.push(bm.trailing_zeros() as u16);
            bm &= bm - 1;
        }
        if let Some(extra) = seg.label_overflow().get(&row) {
            labels.extend_from_slice(extra);
        }
        let props = seg.node_properties(row);
        let embedding = seg.node_embedding(row);
        write_buf.add_node_with_id(key.data().as_ffi(), labels, props, embedding, created_lsn);
        if let Some(n) = write_buf.get_node_mut(key) {
            n.valid_from = valid_from;
            n.valid_to = valid_to;
        }
        return true;
    }
    false
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
    /// Monotonic freshness counter for the GRAPH engine on this shard.
    ///
    /// Bumped (Release) after every successful mutating operation: `create_graph`,
    /// `drop_graph`, and all graph mutations via `allocate_lsn` (the choke-point
    /// for every node/edge/property write). Exposed by `GRAPH.INFO` under
    /// `version_token`.
    ///
    /// Semantics:
    /// - Starts at 0 on shard boot; NOT restored from WAL (freshness hint only).
    /// - Monotonic within a single shard; no cross-shard atomicity.
    /// - Counter never wraps in practice (u64::MAX ≈ 1.8 × 10¹⁹ writes).
    /// - Failed writes do NOT bump the counter.
    version_token: AtomicU64,
}

impl GraphStore {
    /// Create an empty GraphStore with zero allocation.
    pub fn new() -> Self {
        Self {
            graphs: None,
            next_lsn: 0,
            wal_pending: Vec::new(),
            version_token: AtomicU64::new(0),
        }
    }

    /// Return the current GRAPH engine version token for this shard.
    ///
    /// Uses `Acquire` ordering so the caller observes all writes that preceded
    /// the most recent `bump_version` call on this shard.
    #[inline]
    pub fn version_token(&self) -> u64 {
        self.version_token.load(Ordering::Acquire)
    }

    /// Bump the GRAPH version token by 1 after a successful write.
    ///
    /// Uses `Release` ordering so that any subsequent `Acquire` load on any
    /// thread observes the completed write. Returns the new value.
    #[inline]
    pub fn bump_version(&self) -> u64 {
        self.version_token.fetch_add(1, Ordering::Release) + 1
    }

    /// Allocate the next monotonic LSN for a graph mutation.
    ///
    /// Pure counter increment — does NOT bump the version token. Callers are
    /// responsible for calling `bump_version()` after a successful write so that
    /// failed operations (e.g. duplicate GRAPH.CREATE before the actual insert)
    /// do not advance the freshness counter.
    pub fn allocate_lsn(&mut self) -> u64 {
        let lsn = self.next_lsn;
        self.next_lsn = self.next_lsn.saturating_add(1);
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
                stats: GraphStats::new(),
                plan_cache: parking_lot::Mutex::new(crate::graph::cypher::planner::PlanCache::new(
                    1024,
                )),
                key_to_node: HashMap::new(),
            },
        );
        // Bump version AFTER successful graph creation (monotonicity-on-success
        // contract). The caller allocates an LSN first, but that is a pure
        // counter increment — the version bump only fires here, on success.
        self.bump_version();
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
        // Bump version AFTER successful graph drop.
        self.bump_version();
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

    /// Iterate all named graphs mutably (for WAL replay).
    pub fn iter_graphs_mut(&mut self) -> impl Iterator<Item = &mut NamedGraph> {
        self.graphs.iter_mut().flat_map(|m| m.values_mut())
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

    /// Resident bytes across all named graphs on this shard.
    ///
    /// Sums mutable write-buffer (MemGraph slot maps) + immutable CSR segments
    /// (row_offsets + col_indices + edge/node metadata). O(graph_count * segment_count).
    pub fn resident_bytes(&self) -> usize {
        let map = match &self.graphs {
            Some(m) => m,
            None => return 0,
        };
        let mut total: usize = 0;
        for graph in map.values() {
            // Mutable write buffer.
            total += graph.write_buf.resident_bytes();
            // Immutable CSR segments.
            let snapshot = graph.segments.load();
            for csr in &snapshot.immutable {
                total += csr.resident_bytes();
            }
        }
        total
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
    fn test_freeze_and_compact_preserves_key_uniqueness() {
        use smallvec::smallvec;
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"g"), 4, 1)
            .expect("ok");
        let g = store.get_graph_mut(b"g").expect("g");

        let mut keys = Vec::new();
        for i in 0..5u64 {
            keys.push(g.write_buf.add_node(smallvec![0u16], smallvec![], None, i));
        }
        for w in keys.windows(2) {
            g.write_buf
                .add_edge(w[0], w[1], 0, 1.0, None, 10)
                .expect("edge");
        }
        assert!(g.freeze_and_compact(20), "compact succeeds");

        // A node added AFTER the freeze must get a key distinct from every
        // frozen key. A fresh SlotMap restarts allocation at the same
        // (index, generation) pairs, silently aliasing new nodes onto frozen
        // CSR rows (external_id collision across tiers).
        let new_key = g.write_buf.add_node(smallvec![0u16], smallvec![], None, 21);
        assert!(
            !keys.contains(&new_key),
            "post-freeze NodeKey collides with a frozen node's key"
        );
        let snap = g.segments.load();
        for seg in &snap.immutable {
            assert!(
                seg.lookup_node(new_key).is_none(),
                "frozen segment resolves a post-freeze key"
            );
        }
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
