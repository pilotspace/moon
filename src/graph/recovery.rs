//! Graph persistence recovery -- load CSR segments from manifest, validate CRC32,
//! skip corrupted segments, and rebuild GraphStore.
//!
//! Recovery flow:
//! 1. Load graph metadata JSON (graph names, thresholds, LSNs)
//! 2. For each graph, load its manifest
//! 3. For each manifest segment, try to load and CRC32-validate the CSR file
//! 4. Corrupted segments are skipped with a warning log
//! 5. Build GraphStore with loaded immutable segments
//! 6. WAL replay fills MemGraph with uncommitted data (handled by replay.rs)

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::graph::csr::{CsrError, CsrStorage};
use crate::graph::manifest::GraphManifest;
use crate::graph::segment::GraphSegmentList;
use crate::graph::store::GraphStore;

/// Result of graph recovery for a single shard.
pub struct GraphRecoveryResult {
    /// The recovered GraphStore with loaded segments.
    pub store: GraphStore,
    /// Number of CSR segments successfully loaded.
    pub segments_loaded: usize,
    /// Number of CSR segments skipped due to corruption.
    pub segments_skipped: usize,
}

/// Directory layout constants.
const GRAPH_METADATA_FILE: &str = "graph_metadata.json";

/// Returns the manifest filename for a graph.
fn manifest_filename(graph_name: &str) -> String {
    format!("graph_{graph_name}/manifest.json")
}

/// Returns the graph data directory name.
fn graph_dir_name(graph_name: &str) -> String {
    format!("graph_{graph_name}")
}

/// Recover a GraphStore from persistence directory for a given shard.
///
/// The persistence directory layout:
/// ```text
/// {persistence_dir}/shard_{shard_id}/
///   graph_metadata.json
///   graph_{name}/
///     manifest.json
///     seg_{lsn}.csr
///     ...
/// ```
///
/// Returns `Ok(None)` if no graph metadata file exists (clean start).
pub fn recover_graph_store(
    persistence_dir: &Path,
    shard_id: usize,
) -> io::Result<Option<GraphRecoveryResult>> {
    let shard_dir = persistence_dir.join(format!("shard_{shard_id}"));
    let meta_path = shard_dir.join(GRAPH_METADATA_FILE);

    if !meta_path.exists() {
        return Ok(None);
    }

    let mut store = GraphStore::load_metadata(&meta_path)?;
    let mut total_loaded = 0usize;
    let mut total_skipped = 0usize;

    // For each graph, try to load its manifest and segments.
    let graph_names: Vec<String> = store
        .list_graphs()
        .iter()
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .collect();

    for graph_name in &graph_names {
        let manifest_path = shard_dir.join(manifest_filename(graph_name));
        if !manifest_path.exists() {
            tracing::warn!(
                graph = %graph_name,
                "graph manifest file not found, skipping segment load"
            );
            continue;
        }

        let manifest = match GraphManifest::load(&manifest_path) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(
                    graph = %graph_name,
                    error = %e,
                    "failed to load graph manifest, skipping"
                );
                continue;
            }
        };

        let mut loaded_segments: Vec<Arc<CsrStorage>> = Vec::new();

        for entry in &manifest.segments {
            let seg_path = shard_dir.join(&entry.file_path);
            match CsrStorage::from_file(&seg_path) {
                Ok(seg) => {
                    tracing::info!(
                        graph = %graph_name,
                        segment_id = entry.segment_id,
                        nodes = seg.node_count(),
                        edges = seg.edge_count(),
                        "loaded CSR segment via mmap"
                    );
                    loaded_segments.push(Arc::new(seg));
                    total_loaded += 1;
                }
                Err(CsrError::ChecksumMismatch { expected, actual }) => {
                    tracing::warn!(
                        graph = %graph_name,
                        segment_id = entry.segment_id,
                        path = %seg_path.display(),
                        expected_checksum = expected,
                        actual_checksum = actual,
                        "CSR segment CRC32 mismatch, skipping corrupted segment"
                    );
                    total_skipped += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        graph = %graph_name,
                        segment_id = entry.segment_id,
                        path = %seg_path.display(),
                        error = ?e,
                        "failed to load CSR segment, skipping"
                    );
                    total_skipped += 1;
                }
            }
        }

        // Inject loaded segments into the graph's segment holder.
        if let Some(graph) = store.get_graph_mut(graph_name.as_bytes()) {
            let current = graph.segments.load();
            graph.segments.swap(GraphSegmentList {
                mutable: current.mutable.clone(),
                immutable: loaded_segments,
            });
        }
    }

    Ok(Some(GraphRecoveryResult {
        store,
        segments_loaded: total_loaded,
        segments_skipped: total_skipped,
    }))
}

/// Save graph persistence data for a shard.
///
/// Writes metadata, manifests, and CSR segment files.
pub fn save_graph_store(
    store: &GraphStore,
    persistence_dir: &Path,
    shard_id: usize,
) -> io::Result<()> {
    let shard_dir = persistence_dir.join(format!("shard_{shard_id}"));
    std::fs::create_dir_all(&shard_dir)?;

    // Save metadata.
    let meta_path = shard_dir.join(GRAPH_METADATA_FILE);
    store.save_metadata(&meta_path)?;

    // For each graph, save manifest and CSR segment files.
    for graph_name_bytes in store.list_graphs() {
        let graph_name = String::from_utf8_lossy(graph_name_bytes);
        let graph = match store.get_graph(graph_name_bytes) {
            Some(g) => g,
            None => continue,
        };

        let graph_data_dir = shard_dir.join(graph_dir_name(&graph_name));
        std::fs::create_dir_all(&graph_data_dir)?;

        let segments = graph.segments.load();
        let base_dir = graph_dir_name(&graph_name);

        // Write each CSR segment file.
        for seg in &segments.immutable {
            let seg_filename = format!("seg_{}.csr", seg.created_lsn());
            let seg_path = graph_data_dir.join(&seg_filename);
            if !seg_path.exists() {
                seg.write_to_file(&seg_path)
                    .map_err(|e| io::Error::other(format!("failed to write CSR segment: {e:?}")))?;
            }
        }

        // Write manifest.
        let manifest = GraphManifest::from_segments(&graph_name, &segments.immutable, &base_dir);
        let manifest_path = graph_data_dir.join("manifest.json");
        manifest.save(&manifest_path)?;
    }

    Ok(())
}

/// Helper: build the shard persistence directory path.
pub fn shard_graph_dir(persistence_dir: &Path, shard_id: usize) -> PathBuf {
    persistence_dir.join(format!("shard_{shard_id}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use smallvec::smallvec;
    use tempfile::TempDir;

    use crate::graph::csr::CsrSegment;
    use crate::graph::memgraph::MemGraph;

    /// Build a small CSR segment for testing.
    fn make_csr(lsn: u64) -> CsrSegment {
        let mut mg = MemGraph::new(100);
        let a = mg.add_node(smallvec![0], smallvec![], None, 1);
        let b = mg.add_node(smallvec![1], smallvec![], None, 1);
        let c = mg.add_node(smallvec![2], smallvec![], None, 1);
        mg.add_edge(a, b, 0, 1.0, None, 2).expect("ok");
        mg.add_edge(b, c, 1, 2.0, None, 3).expect("ok");
        let frozen = mg.freeze().expect("ok");
        CsrSegment::from_frozen(frozen, lsn).expect("ok")
    }

    #[test]
    fn test_save_and_recover_roundtrip() {
        let dir = TempDir::new().expect("tmpdir");
        let shard_id = 0;

        // Build a GraphStore with one graph and one CSR segment.
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"social"), 64_000, 10)
            .expect("ok");

        // Add an immutable CSR segment.
        let csr = make_csr(100);
        let graph = store.get_graph_mut(b"social").expect("exists");
        graph.segments.add_immutable(csr);

        // Save.
        save_graph_store(&store, dir.path(), shard_id).expect("save ok");

        // Recover.
        let result = recover_graph_store(dir.path(), shard_id)
            .expect("io ok")
            .expect("result exists");

        assert_eq!(result.segments_loaded, 1);
        assert_eq!(result.segments_skipped, 0);
        assert_eq!(result.store.graph_count(), 1);

        let graph = result.store.get_graph(b"social").expect("exists");
        let segs = graph.segments.load();
        assert_eq!(segs.immutable.len(), 1);
        assert_eq!(segs.immutable[0].node_count(), 3);
        assert_eq!(segs.immutable[0].edge_count(), 2);
    }

    #[test]
    fn test_recover_with_corrupted_segment() {
        let dir = TempDir::new().expect("tmpdir");
        let shard_id = 0;

        // Build and save a store with two CSR segments.
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"test"), 64_000, 5)
            .expect("ok");

        let csr1 = make_csr(100);
        let csr2 = make_csr(200);
        let graph = store.get_graph_mut(b"test").expect("exists");
        graph.segments.add_immutable(csr1);
        graph.segments.add_immutable(csr2);

        save_graph_store(&store, dir.path(), shard_id).expect("save ok");

        // Corrupt one segment file.
        let seg_path = dir
            .path()
            .join(format!("shard_{shard_id}/graph_test/seg_100.csr"));
        let mut data = std::fs::read(&seg_path).expect("read ok");
        // Corrupt the checksum (offset 72).
        if data.len() > 73 {
            data[72] ^= 0xFF;
        }
        std::fs::write(&seg_path, &data).expect("write ok");

        // Recover -- should load 1 segment, skip 1 corrupted.
        let result = recover_graph_store(dir.path(), shard_id)
            .expect("io ok")
            .expect("result exists");

        assert_eq!(result.segments_loaded, 1);
        assert_eq!(result.segments_skipped, 1);

        let graph = result.store.get_graph(b"test").expect("exists");
        let segs = graph.segments.load();
        assert_eq!(segs.immutable.len(), 1);
        assert_eq!(segs.immutable[0].created_lsn(), 200);
    }

    #[test]
    fn test_recover_no_metadata() {
        let dir = TempDir::new().expect("tmpdir");
        // Create the shard dir but no metadata file.
        std::fs::create_dir_all(dir.path().join("shard_0")).expect("mkdir ok");

        let result = recover_graph_store(dir.path(), 0).expect("io ok");
        assert!(result.is_none());
    }

    #[test]
    fn test_recover_empty_store() {
        let dir = TempDir::new().expect("tmpdir");
        let shard_id = 0;

        let store = GraphStore::new();
        save_graph_store(&store, dir.path(), shard_id).expect("save ok");

        let result = recover_graph_store(dir.path(), shard_id)
            .expect("io ok")
            .expect("result exists");

        assert_eq!(result.store.graph_count(), 0);
        assert_eq!(result.segments_loaded, 0);
        assert_eq!(result.segments_skipped, 0);
    }

    #[test]
    fn test_save_multiple_graphs() {
        let dir = TempDir::new().expect("tmpdir");
        let shard_id = 0;

        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"graph_a"), 1000, 1)
            .expect("ok");
        store
            .create_graph(Bytes::from_static(b"graph_b"), 2000, 2)
            .expect("ok");

        let csr_a = make_csr(10);
        store
            .get_graph_mut(b"graph_a")
            .expect("exists")
            .segments
            .add_immutable(csr_a);

        let csr_b = make_csr(20);
        store
            .get_graph_mut(b"graph_b")
            .expect("exists")
            .segments
            .add_immutable(csr_b);

        save_graph_store(&store, dir.path(), shard_id).expect("save ok");

        let result = recover_graph_store(dir.path(), shard_id)
            .expect("io ok")
            .expect("result exists");

        assert_eq!(result.store.graph_count(), 2);
        assert_eq!(result.segments_loaded, 2);
        assert_eq!(result.segments_skipped, 0);
    }
}
