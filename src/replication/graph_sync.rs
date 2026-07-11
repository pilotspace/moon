//! v0.7 graph-plane PSYNC snapshot: export / install a shard's `GraphStore`
//! as a moon-private RDB aux blob.
//!
//! The master freezes every graph's write buffer into an immutable CSR
//! segment (the same "freeze is the only serialization path" contract the
//! WAL-v3 checkpoint uses — see `persist_graph_at_checkpoint`) and ships the
//! segments' `to_bytes()` encodings. The replica recreates each graph and
//! injects the segments exactly like restart recovery does
//! (`recover_graph_store`), so node/edge ids, labels, properties, and
//! embeddings all survive with restart-equivalent fidelity. Post-snapshot
//! mutations arrive as deterministic GRAPH.* WAL records on the live stream.
//!
//! Blob format (version 1, little-endian):
//! ```text
//! [u8  version = 1]
//! [u32 graph_count]
//! per graph:
//!   [u16 name_len][name bytes]
//!   [u64 next_node_id][u64 next_edge_id]     (id-allocation cursors)
//!   [u32 segment_count]
//!   per segment: [u64 blob_len][CsrSegment::to_bytes payload]
//! ```

use bytes::Bytes;

use crate::graph::csr::{CsrSegment, CsrStorage};
use crate::graph::segment::GraphSegmentList;
use crate::graph::store::GraphStore;

const FORMAT_VERSION: u8 = 1;

/// Export the whole store as a snapshot blob. Freezes every graph's write
/// buffer first (same operation the checkpoint performs; runs on the shard
/// thread between mutations, so the cut is consistent). Always returns a
/// blob — an empty store encodes as `graph_count = 0`, which lets the
/// replica distinguish "master has no graphs" (authoritative: drop local
/// graphs) from "pre-graph-sync master" (aux absent entirely).
pub fn export_graph_store(store: &mut GraphStore) -> Vec<u8> {
    // Freeze all write buffers so the segments cover the mutable tier.
    let names: Vec<Bytes> = store.list_graphs().into_iter().cloned().collect();
    for name in &names {
        let lsn = store.allocate_lsn();
        if let Some(graph) = store.get_graph_mut(name) {
            graph.freeze_and_compact(lsn);
        }
    }

    let mut buf: Vec<u8> = Vec::with_capacity(64);
    buf.push(FORMAT_VERSION);
    buf.extend_from_slice(&(names.len() as u32).to_le_bytes());
    for name in &names {
        let Some(graph) = store.get_graph(name) else {
            // Unreachable (names collected above); keep counts honest anyway.
            buf.extend_from_slice(&[0u8; 2]);
            buf.extend_from_slice(&0u64.to_le_bytes());
            buf.extend_from_slice(&0u64.to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes());
            continue;
        };
        buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
        buf.extend_from_slice(name);
        let (next_node, next_edge) = graph.write_buf.id_cursors();
        buf.extend_from_slice(&next_node.to_le_bytes());
        buf.extend_from_slice(&next_edge.to_le_bytes());
        let segments = graph.segments.load();
        buf.extend_from_slice(&(segments.immutable.len() as u32).to_le_bytes());
        for seg in &segments.immutable {
            let blob = seg.to_bytes();
            buf.extend_from_slice(&(blob.len() as u64).to_le_bytes());
            buf.extend_from_slice(&blob);
        }
    }
    buf
}

/// Install a snapshot blob into `store`, replacing ALL local graph state
/// (authoritative, mirroring the keyspace-replace semantics of RDB load).
/// Returns the number of graphs installed, or `None` on a malformed blob
/// (store is left in whatever partial state was reached — the caller aborts
/// the sync and the replica retries with a fresh full resync).
pub fn install_graph_store(store: &mut GraphStore, blob: &[u8]) -> Option<usize> {
    let mut cur = Cursor { data: blob, pos: 0 };
    if cur.u8()? != FORMAT_VERSION {
        return None;
    }
    // Authoritative replace: drop everything local first.
    let local: Vec<Bytes> = store.list_graphs().into_iter().cloned().collect();
    for name in local {
        let _ = store.drop_graph(&name);
    }

    let graph_count = cur.u32()? as usize;
    for _ in 0..graph_count {
        let name_len = cur.u16()? as usize;
        let name = Bytes::copy_from_slice(cur.take(name_len)?);
        let next_node = cur.u64()?;
        let next_edge = cur.u64()?;
        let seg_count = cur.u32()? as usize;

        let mut segments: Vec<std::sync::Arc<CsrStorage>> = Vec::with_capacity(seg_count);
        for _ in 0..seg_count {
            let len = cur.u64()? as usize;
            let bytes = cur.take(len)?;
            match CsrSegment::from_bytes(bytes) {
                Ok(seg) => segments.push(std::sync::Arc::new(CsrStorage::Heap(seg))),
                Err(e) => {
                    tracing::warn!(
                        graph = %String::from_utf8_lossy(&name),
                        error = ?e,
                        "replica graph sync: corrupt CSR segment in snapshot — aborting install"
                    );
                    return None;
                }
            }
        }

        if store.create_graph(name.clone(), 64_000, 0).is_err() {
            return None;
        }
        let graph = store.get_graph_mut(&name)?;
        // Same floor-restore contract as `recover_graph_store`: manifest
        // cursors are authoritative, and every frozen external_id raises the
        // floor so later inserts can never alias a frozen row.
        graph.write_buf.restore_id_cursors(next_node, next_edge);
        for seg in &segments {
            for nm in seg.node_meta() {
                graph.write_buf.ensure_node_id_floor(nm.external_id);
            }
        }
        let current = graph.segments.load();
        graph.segments.swap(GraphSegmentList {
            mutable: current.mutable.clone(),
            immutable: segments,
        });
    }
    // The installed graphs exist only in memory; the next checkpoint must
    // re-materialize them (same contract as WAL replay).
    if graph_count > 0 {
        store.mark_dirty();
    }
    Some(graph_count)
}

/// Minimal bounds-checked reader over the blob.
struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        if end > self.data.len() {
            return None;
        }
        let s = &self.data[self.pos..end];
        self.pos = end;
        Some(s)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u16(&mut self) -> Option<u16> {
        #[allow(clippy::unwrap_used)] // take(2) guarantees the length
        Some(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Option<u32> {
        #[allow(clippy::unwrap_used)] // take(4) guarantees the length
        Some(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Option<u64> {
        #[allow(clippy::unwrap_used)] // take(8) guarantees the length
        Some(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::graph::graph_write::label_to_id;
    use smallvec::smallvec;

    fn seeded_store() -> GraphStore {
        let mut store = GraphStore::new();
        store
            .create_graph(Bytes::from_static(b"g1"), 64_000, 0)
            .unwrap();
        let lsn = store.allocate_lsn();
        let g = store.get_graph_mut(b"g1").unwrap();
        let person = label_to_id(b"Person");
        let name_key = label_to_id(b"name");
        let a = g.write_buf.add_node(
            smallvec![person],
            smallvec![(
                name_key,
                crate::graph::types::PropertyValue::String(Bytes::from_static(b"alice"))
            )],
            None,
            lsn,
        );
        let b = g.write_buf.add_node(
            smallvec![person],
            smallvec![(
                name_key,
                crate::graph::types::PropertyValue::String(Bytes::from_static(b"bob"))
            )],
            None,
            lsn,
        );
        g.write_buf
            .add_edge(a, b, label_to_id(b"KNOWS"), 1.0, None, lsn)
            .unwrap();
        store
    }

    #[test]
    fn export_install_round_trip_preserves_nodes_edges_props() {
        let mut master = seeded_store();
        let blob = export_graph_store(&mut master);

        let mut replica = GraphStore::new();
        // Pre-existing local graph must be dropped (authoritative replace).
        replica
            .create_graph(Bytes::from_static(b"stale"), 64_000, 0)
            .unwrap();
        let installed = install_graph_store(&mut replica, &blob).expect("valid blob");
        assert_eq!(installed, 1);
        assert!(replica.get_graph(b"stale").is_none(), "stale graph kept");

        let g = replica.get_graph(b"g1").expect("g1 installed");
        let segments = g.segments.load();
        let total_nodes: u32 = segments.immutable.iter().map(|s| s.node_count()).sum();
        let total_edges: u32 = segments.immutable.iter().map(|s| s.edge_count()).sum();
        assert_eq!(total_nodes, 2);
        assert_eq!(total_edges, 1);
        // Property fidelity through the freeze → blob → install pipeline.
        let name_key = label_to_id(b"name");
        let mut names: Vec<Bytes> = Vec::new();
        for seg in &segments.immutable {
            for row in 0..seg.node_count() {
                if let Some(crate::graph::types::PropertyValue::String(s)) = seg
                    .node_properties(row)
                    .iter()
                    .find(|(k, _)| *k == name_key)
                    .map(|(_, v)| v.clone())
                {
                    names.push(s);
                }
            }
        }
        names.sort();
        assert_eq!(
            names,
            vec![Bytes::from_static(b"alice"), Bytes::from_static(b"bob")]
        );
    }

    #[test]
    fn empty_store_round_trips_and_truncated_blob_rejected() {
        let mut empty = GraphStore::new();
        let blob = export_graph_store(&mut empty);
        let mut replica = seeded_store();
        assert_eq!(install_graph_store(&mut replica, &blob), Some(0));
        assert!(
            replica.get_graph(b"g1").is_none(),
            "empty master snapshot must drop replica-local graphs"
        );

        let mut master = seeded_store();
        let full = export_graph_store(&mut master);
        let mut target = GraphStore::new();
        assert_eq!(
            install_graph_store(&mut target, &full[..full.len() - 3]),
            None,
            "truncated blob must be rejected"
        );
    }
}
