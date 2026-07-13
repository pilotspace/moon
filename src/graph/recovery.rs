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

use bytes::Bytes;

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
            // Validate path does not escape the shard directory (defense-in-depth).
            if !seg_path.starts_with(&shard_dir) {
                tracing::warn!(
                    graph = %graph_name,
                    path = %seg_path.display(),
                    "skipping segment with path traversal in manifest"
                );
                continue;
            }
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

        // Inject loaded segments into the graph's segment holder and restore
        // the write buffer's id-allocation floors: the manifest cursors
        // (authoritative when present; 0 in pre-cursor manifests) plus every
        // frozen external_id (covers manifests written before cursors
        // existed), so fresh post-recovery inserts can never alias a frozen
        // row or a WAL-replayed id.
        if let Some(graph) = store.get_graph_mut(graph_name.as_bytes()) {
            graph
                .write_buf
                .restore_id_cursors(manifest.next_node_id, manifest.next_edge_id);
            for seg in &loaded_segments {
                for nm in seg.node_meta() {
                    graph.write_buf.ensure_node_id_floor(nm.external_id);
                }
            }
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
/// Writes CSR segments + manifests (the **payload**) first, then
/// `graph_metadata.json` (the **reference** — it carries `snapshot_lsn`,
/// the WAL-replay floor recovery trusts) LAST.
///
/// This order is load-bearing (task #53 / kernel M3 K2 brief §1.2 root
/// cause): the OLD code wrote `graph_metadata.json` FIRST. A kill-9 in the
/// window between that write landing and the segment/manifest writes that
/// follow it durably advanced the WAL-replay floor (`snapshot_lsn`) past
/// data whose on-disk segments were still stale or absent — replay then
/// SKIPPED every record `<= snapshot_lsn` (trusting the published floor)
/// while the segments claiming to cover that range were never actually
/// written, permanently losing the entire batch since the prior checkpoint.
/// Reference-before-payload is exactly the "floor must never advance past
/// what is actually durable" violation this milestone's K2 register exists
/// to prevent structurally; this call site had it backwards internally.
///
/// Writing metadata last does NOT introduce a new double-apply risk on the
/// opposite crash window (segments/manifest durable, metadata still old):
/// WAL replay's node/edge insert path (`src/graph/replay.rs`'s
/// `node_present` cross-tier check) already probes loaded immutable
/// segments before re-inserting a WAL-logged id — it was built for the
/// "restart NodeKey aliasing" P0 and is exactly the redo-idempotency an
/// ARIES-style "payload before reference" ordering requires of its replay
/// path. A crash in this window only replays already-covered records
/// again; it never duplicates them.
pub fn save_graph_store(
    store: &GraphStore,
    persistence_dir: &Path,
    shard_id: usize,
) -> io::Result<()> {
    let shard_dir = persistence_dir.join(format!("shard_{shard_id}"));
    std::fs::create_dir_all(&shard_dir)?;

    // For each graph, save manifest and CSR segment files (payload) FIRST.
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

        // Write manifest (including the write buffer's id-allocation
        // cursors, so recovery resumes allocation past every id ever
        // handed out even if the WAL was truncated). Still payload-side —
        // it must land before metadata publishes the floor that assumes
        // these segments exist.
        let manifest = GraphManifest::from_segments(
            &graph_name,
            &segments.immutable,
            &base_dir,
            graph.write_buf.id_cursors(),
        );
        let manifest_path = graph_data_dir.join("manifest.json");
        manifest.save(&manifest_path)?;
    }

    // Save metadata (the reference / replay-skip floor) LAST — only once
    // every graph's segments + manifest for this snapshot are durably on
    // disk. A crash before this point leaves the OLD (safe, lower) floor
    // in place; the next checkpoint attempt simply retries with a fresh
    // snapshot_lsn.
    let meta_path = shard_dir.join(GRAPH_METADATA_FILE);
    store.save_metadata(&meta_path)?;

    Ok(())
}

/// Helper: build the shard persistence directory path.
pub fn shard_graph_dir(persistence_dir: &Path, shard_id: usize) -> PathBuf {
    persistence_dir.join(format!("shard_{shard_id}"))
}

/// Snapshot the graph store as part of a WAL v3 checkpoint (Bug B of the
/// 2026-07 durability P0: the checkpoint advances the WAL replay floor and
/// recycles segments, so every graph record it covers must be materialized
/// on disk FIRST or a crash loses the graph permanently).
///
/// Freezes each graph's write buffer into an immutable CSR segment (the
/// mutable tier is never serialized directly — freeze is the only path to
/// disk), stamps `snapshot_lsn` (the WAL LSN this snapshot covers; recovery
/// skips graph records at or below it), and persists segments + manifests +
/// metadata.
///
/// Returns `true` when the checkpoint may proceed (snapshot persisted, or
/// nothing to do). Returns `false` on save failure — the caller MUST abort
/// the checkpoint finalize so the control file keeps the old replay floor
/// and the WAL segments holding the graph records are not recycled.
///
/// Called on the shard thread between mutations (single-threaded event
/// loop), so the snapshot is a consistent cut: every record `<= snapshot_lsn`
/// is in the freeze, every later record is not.
pub fn persist_graph_at_checkpoint(
    store: &mut GraphStore,
    persistence_dir: Option<&Path>,
    shard_id: usize,
    snapshot_lsn: u64,
) -> bool {
    // Task #53 review round 2 / P0-1: `graph_count() == 0` must NOT be part
    // of this short-circuit. A GRAPH.DELETE that empties the graph map
    // marks the store dirty via the WAL drain but drives `graph_count()`
    // to 0 in the SAME tick — the old `!is_dirty() || graph_count() == 0`
    // condition then short-circuited on the `== 0` disjunct and skipped
    // `save_graph_store` entirely, leaving `graph_metadata.json` on disk
    // stale (still describing the graph as it existed before the delete).
    // Later checkpoints kept advancing `control.graph_floor_lsn` past the
    // WAL record holding the DELETE (nothing here ever re-checks
    // `is_dirty` once it's been wrongly treated as "nothing to persist"),
    // so recycle eventually freed the segment holding that DELETE — crash,
    // and recovery loads the stale pre-delete metadata with no WAL record
    // left to replay the deletion: the dropped graph resurrects. Dirty
    // alone is now the only gate; an empty-but-dirty store still calls
    // `save_graph_store` below, whose per-graph loop correctly no-ops on
    // zero graphs while `store.save_metadata` still rewrites
    // `graph_metadata.json` to reflect zero graphs at the new
    // `snapshot_lsn` — advancing the floor and clearing dirty only once
    // that fact is durable.
    if !store.is_dirty() {
        return true;
    }
    // No persistence dir (e.g. --appendonly no): graph writes carry no
    // durability contract; let the checkpoint proceed.
    let Some(dir) = persistence_dir else {
        return true;
    };

    // Freeze every write buffer so the on-disk segments cover the mutable
    // tier. Graphs whose buffer is empty (or holds only cross-tier delta
    // edges) skip the freeze — freeze_and_compact returns false without
    // pushing an empty segment.
    let names: Vec<Bytes> = store.list_graphs().into_iter().cloned().collect();
    for name in &names {
        let lsn = store.allocate_lsn();
        if let Some(graph) = store.get_graph_mut(name) {
            graph.freeze_and_compact(lsn);
        }
    }

    store.set_snapshot_lsn(snapshot_lsn);
    match save_graph_store(store, dir, shard_id) {
        Ok(()) => {
            store.clear_dirty();
            true
        }
        Err(e) => {
            tracing::error!(
                "Shard {shard_id}: checkpoint graph snapshot failed ({e}); \
                 aborting checkpoint finalize to keep the WAL replay floor"
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use slotmap::Key;
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

    /// P0-2 stable ids: recovery must restore the id-allocation cursors so
    /// post-restart inserts can never alias frozen external_ids — via the
    /// manifest cursors AND (for pre-cursor manifests) the frozen
    /// external_ids themselves.
    #[test]
    fn test_recover_restores_id_allocation_floor() {
        let dir = TempDir::new().expect("tmpdir");
        let shard_id = 0;

        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"g"), 64_000, 10)
            .expect("ok");
        let graph = store.get_graph_mut(b"g").expect("exists");

        // Simulate a pre-crash session: nodes allocated from the write
        // buffer, then frozen into a CSR segment.
        let a = graph.write_buf.add_node(smallvec![0], smallvec![], None, 1);
        let b = graph.write_buf.add_node(smallvec![1], smallvec![], None, 1);
        graph.write_buf.add_edge(a, b, 0, 1.0, None, 2).expect("ok");
        let frozen = graph.write_buf.freeze().expect("ok");
        let csr = CsrSegment::from_frozen(frozen, 100).expect("ok");
        graph.segments.add_immutable(csr);
        graph.write_buf.thaw();
        let saved_cursors = graph.write_buf.id_cursors();
        let max_frozen_id = a.data().as_ffi().max(b.data().as_ffi());

        save_graph_store(&store, dir.path(), shard_id).expect("save ok");

        let mut result = recover_graph_store(dir.path(), shard_id)
            .expect("io ok")
            .expect("result exists");
        let graph = result.store.get_graph_mut(b"g").expect("exists");

        // Cursors restored to at least the saved values.
        let (nn, ne) = graph.write_buf.id_cursors();
        assert!(
            nn >= saved_cursors.0,
            "node cursor {nn} < saved {}",
            saved_cursors.0
        );
        assert!(
            ne >= saved_cursors.1,
            "edge cursor {ne} < saved {}",
            saved_cursors.1
        );

        // A fresh insert must not alias any frozen row.
        let fresh = graph.write_buf.add_node(smallvec![9], smallvec![], None, 5);
        assert!(
            fresh.data().as_ffi() > max_frozen_id,
            "fresh id {} aliases frozen tier (max frozen {})",
            fresh.data().as_ffi(),
            max_frozen_id
        );

        // Pre-cursor manifest fallback: zero cursors, floor comes from the
        // frozen external_ids scan.
        let manifest_path = dir
            .path()
            .join(format!("shard_{shard_id}/graph_g/manifest.json"));
        let mut manifest = GraphManifest::load(&manifest_path).expect("load ok");
        manifest.next_node_id = 0;
        manifest.next_edge_id = 0;
        manifest.save(&manifest_path).expect("save ok");

        let mut result = recover_graph_store(dir.path(), shard_id)
            .expect("io ok")
            .expect("result exists");
        let graph = result.store.get_graph_mut(b"g").expect("exists");
        let fresh = graph.write_buf.add_node(smallvec![9], smallvec![], None, 5);
        assert!(
            fresh.data().as_ffi() > max_frozen_id,
            "pre-cursor fallback: fresh id {} aliases frozen tier",
            fresh.data().as_ffi()
        );
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
