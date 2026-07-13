//! CSR segment manifest -- per-graph JSON listing active segments.
//!
//! Each named graph has a manifest file that records which CSR segment files
//! are currently active, their frozen LSN, node/edge counts, and CRC32
//! checksums. On recovery, the manifest is loaded first, then each segment
//! file is mmap'd/read and CRC-validated.

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::graph::csr::CsrStorage;

/// Manifest entry for a single CSR segment file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentManifestEntry {
    /// Segment identifier -- the `created_lsn` of the CSR segment.
    pub segment_id: u64,
    /// Relative file path (e.g. `"graph_social/seg_100.csr"`).
    pub file_path: String,
    /// Number of nodes in this segment.
    pub node_count: u32,
    /// Number of edges in this segment.
    pub edge_count: u32,
    /// LSN at which this segment was frozen.
    pub frozen_lsn: u64,
    /// CRC32 checksum stored in the segment header.
    pub checksum: u64,
}

/// Per-graph manifest listing all active CSR segments.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphManifest {
    /// Graph name.
    pub graph_name: String,
    /// Active segment entries, newest first.
    pub segments: Vec<SegmentManifestEntry>,
    /// Unix timestamp (seconds) when this manifest was written.
    pub created_at: u64,
    /// Node-id allocation cursor (`MemGraph::id_cursors`) at save time.
    /// `0` (absent in pre-cursor manifests) means unknown — recovery then
    /// derives the floor from frozen external_ids + WAL replay alone.
    #[serde(default)]
    pub next_node_id: u64,
    /// Edge-id allocation cursor at save time (same contract).
    #[serde(default)]
    pub next_edge_id: u64,
}

impl GraphManifest {
    /// Build a manifest from live CSR storage segments.
    ///
    /// `base_dir` is the relative directory prefix for segment file paths
    /// (e.g. `"graph_social"`). Segment files are named `seg_{lsn}.csr`.
    /// `id_cursors` is the write buffer's `(next_node_id, next_edge_id)`.
    pub fn from_segments(
        graph_name: &str,
        segments: &[Arc<CsrStorage>],
        base_dir: &str,
        id_cursors: (u64, u64),
    ) -> Self {
        let entries: Vec<SegmentManifestEntry> = segments
            .iter()
            .map(|seg| SegmentManifestEntry {
                segment_id: seg.created_lsn(),
                file_path: format!("{base_dir}/seg_{}.csr", seg.created_lsn()),
                node_count: seg.header().node_count,
                edge_count: seg.header().edge_count,
                frozen_lsn: seg.created_lsn(),
                checksum: seg.header().checksum,
            })
            .collect();

        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            graph_name: graph_name.to_owned(),
            segments: entries,
            created_at,
            next_node_id: id_cursors.0,
            next_edge_id: id_cursors.1,
        }
    }

    /// Write this manifest as pretty-printed JSON to `path` atomically.
    ///
    /// Kernel M3 K2 review round 2 / P2-2 (behavior-equivalent refactor):
    /// routed through the shared `atomic_write_durable` helper
    /// (`src/persistence/atomic.rs`, K3) instead of a hand-rolled
    /// temp+fsync+rename+dir-fsync sequence — same durability contract,
    /// one fewer independent implementation of it to keep in sync.
    pub fn save(&self, path: &Path) -> io::Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        crate::persistence::atomic::atomic_write_durable(path, json.as_bytes())
            .map_err(|e| io::Error::other(e.to_string()))
    }

    /// Load a manifest from a JSON file at `path`.
    pub fn load(path: &Path) -> io::Result<Self> {
        let data = std::fs::read(path)?;
        serde_json::from_slice(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::csr::CsrSegment;
    use tempfile::TempDir;

    #[test]
    fn test_manifest_save_load_roundtrip() {
        let dir = TempDir::new().expect("tmpdir");
        let manifest = GraphManifest {
            graph_name: "social".to_owned(),
            segments: vec![
                SegmentManifestEntry {
                    segment_id: 100,
                    file_path: "graph_social/seg_100.csr".to_owned(),
                    node_count: 50,
                    edge_count: 200,
                    frozen_lsn: 100,
                    checksum: 12345,
                },
                SegmentManifestEntry {
                    segment_id: 200,
                    file_path: "graph_social/seg_200.csr".to_owned(),
                    node_count: 30,
                    edge_count: 80,
                    frozen_lsn: 200,
                    checksum: 67890,
                },
            ],
            created_at: 1700000000,
            next_node_id: (7 << 32) | 42,
            next_edge_id: (3 << 32) | 9,
        };

        let path = dir.path().join("manifest.json");
        manifest.save(&path).expect("save ok");
        let loaded = GraphManifest::load(&path).expect("load ok");
        assert_eq!(manifest, loaded);
    }

    #[test]
    fn test_manifest_from_segments() {
        use crate::graph::memgraph::MemGraph;
        use smallvec::smallvec;

        // Build a small CSR segment.
        let mut mg = MemGraph::new(100);
        let a = mg.add_node(smallvec![0], smallvec![], None, 1);
        let b = mg.add_node(smallvec![1], smallvec![], None, 1);
        mg.add_edge(a, b, 0, 1.0, None, 2).expect("ok");
        let frozen = mg.freeze().expect("ok");
        let csr = CsrSegment::from_frozen(frozen, 42).expect("ok");

        let segments: Vec<Arc<CsrStorage>> = vec![Arc::new(CsrStorage::from(csr))];
        let manifest =
            GraphManifest::from_segments("test_graph", &segments, "graph_test", mg.id_cursors());

        assert_eq!(manifest.graph_name, "test_graph");
        assert_eq!(manifest.next_node_id, mg.id_cursors().0);
        assert_eq!(manifest.next_edge_id, mg.id_cursors().1);
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(manifest.segments[0].segment_id, 42);
        assert_eq!(manifest.segments[0].node_count, 2);
        assert_eq!(manifest.segments[0].edge_count, 1);
        assert_eq!(manifest.segments[0].frozen_lsn, 42);
        assert!(manifest.segments[0].file_path.contains("seg_42.csr"));
    }

    #[test]
    fn test_manifest_load_missing_file() {
        let result = GraphManifest::load(Path::new("/nonexistent/manifest.json"));
        assert!(result.is_err());
    }

    #[test]
    fn test_manifest_load_invalid_json() {
        let dir = TempDir::new().expect("tmpdir");
        let path = dir.path().join("bad.json");
        std::fs::write(&path, b"not valid json").expect("write ok");
        let result = GraphManifest::load(&path);
        assert!(result.is_err());
    }

    #[test]
    fn test_manifest_empty_segments() {
        let dir = TempDir::new().expect("tmpdir");
        let manifest = GraphManifest {
            graph_name: "empty".to_owned(),
            segments: vec![],
            created_at: 0,
            next_node_id: 0,
            next_edge_id: 0,
        };
        let path = dir.path().join("manifest.json");
        manifest.save(&path).expect("save ok");
        let loaded = GraphManifest::load(&path).expect("load ok");
        assert_eq!(loaded.segments.len(), 0);
    }
}
