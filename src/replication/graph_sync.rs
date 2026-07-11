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
//! Blob format (version 2, little-endian):
//! ```text
//! [u8  version = 2]
//! [u32 graph_count]
//! per graph:
//!   [u16 name_len][name bytes]
//!   [u64 next_node_id][u64 next_edge_id]     (id-allocation cursors)
//!   [u32 segment_count]
//!   per segment:
//!     [u64 blob_len][CsrSegment::to_bytes payload]
//!     [u32 deleted_edge_count][u32 edge_idx]*   (validity-overlay sidecar)
//!   [u32 delta_edge_count]                      (cross-tier write-buf edges)
//!   per delta edge:
//!     [u64 edge_ffi][u64 src_ffi][u64 dst_ffi][u16 edge_type][u64 created_lsn]
//!     [u32 stored_off][u32 rec_len][encode_edge_record payload]
//!   [u32 dead_shadow_count]                     (copy-up node tombstones)
//!   per shadow: [u64 node_ffi][u64 deleted_lsn]
//! ```
//!
//! The deleted-edge sidecar exists because the CSR byte format does NOT
//! serialize the validity bitmap (`to_bytes` writes `validity_bitmap_offset =
//! 0`; a load initializes all edges valid): soft-deleted edges live only in
//! the in-memory overlay until a physical compaction rewrites the segment.
//! That is fine self-referentially on the master (restart re-applies the
//! deletes from its WAL), but a replica has neither the overlay nor those WAL
//! records — without the sidecar, every edge soft-deleted on the master since
//! the segment was built would be RESURRECTED on the replica and served as
//! live from read queries (adversarial-review P1-5, both `Heap` and `Mmap`
//! variants).

use bytes::Bytes;

use crate::graph::csr::{CsrSegment, CsrStorage};
use crate::graph::segment::GraphSegmentList;
use crate::graph::store::GraphStore;

const FORMAT_VERSION: u8 = 2;

/// Export the whole store as a snapshot blob. Freezes every graph's write
/// buffer first (same operation the checkpoint performs; runs on the shard
/// thread between mutations, so the cut is consistent). Always returns a
/// blob — an empty store encodes as `graph_count = 0`, which lets the
/// replica distinguish "master has no graphs" (authoritative: drop local
/// graphs) from "pre-graph-sync master" (aux absent entirely).
pub fn export_graph_store(store: &mut GraphStore) -> Vec<u8> {
    // Freeze all write buffers so the segments cover the mutable tier.
    // Skip graphs whose write buffer is empty: this export runs INSIDE the
    // PSYNC handler's synchronous `with_shard` block, blocking the shard's
    // event loop — and reconnect-driven full resyncs can arrive repeatedly.
    // An unconditional freeze per resync would re-run the dead-shadow scan +
    // freeze/thaw machinery over every graph each time even when nothing
    // changed (adversarial-review P1-6).
    let names: Vec<Bytes> = store.list_graphs().into_iter().cloned().collect();
    for name in &names {
        let needs_freeze = store
            .get_graph(name)
            .is_some_and(|g| g.write_buf.node_count() > 0 || g.write_buf.edge_count() > 0);
        if !needs_freeze {
            continue;
        }
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
            buf.extend_from_slice(&0u32.to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes());
            continue;
        };
        if graph.write_buf.node_count() > 0 {
            // A successful freeze drains every live node; nodes still here
            // mean the freeze failed (CSR build error) and their data will
            // NOT be in the blob. Loud, not silent.
            tracing::warn!(
                graph = %String::from_utf8_lossy(name),
                live_nodes = graph.write_buf.node_count(),
                "graph snapshot export: write-buffer freeze failed — \
                 unfrozen nodes are missing from the replica snapshot"
            );
        }
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
            // Validity-overlay sidecar: `to_bytes` does not serialize the
            // validity bitmap, so soft-deleted edges exist only in memory —
            // ship the deleted set explicitly (see module doc, P1-5).
            let deleted: Vec<u32> = (0..seg.edge_count())
                .filter(|&i| !seg.is_valid(i))
                .collect();
            buf.extend_from_slice(&(deleted.len() as u32).to_le_bytes());
            for idx in deleted {
                buf.extend_from_slice(&idx.to_le_bytes());
            }
        }
        // Cross-tier delta edges: freeze() RETAINS edges with a frozen
        // endpoint in the write buffer (CsrSegment::from_frozen can't encode
        // them), so they are in NO segment — without this section every
        // cross-tier edge would silently vanish from the replica. The master
        // recovers them from its WAL on restart; the replica has no WAL.
        {
            use slotmap::Key;
            let delta: Vec<_> = graph.write_buf.iter_edges().collect();
            buf.extend_from_slice(&(delta.len() as u32).to_le_bytes());
            for (ek, e) in delta {
                buf.extend_from_slice(&ek.data().as_ffi().to_le_bytes());
                buf.extend_from_slice(&e.src.data().as_ffi().to_le_bytes());
                buf.extend_from_slice(&e.dst.data().as_ffi().to_le_bytes());
                buf.extend_from_slice(&e.edge_type.to_le_bytes());
                buf.extend_from_slice(&e.created_lsn.to_le_bytes());
                let mut rec: Vec<u8> = Vec::new();
                let stored_off = crate::graph::csr::props::encode_edge_record(
                    &mut rec,
                    e.weight,
                    e.properties.as_ref(),
                );
                buf.extend_from_slice(&stored_off.to_le_bytes());
                buf.extend_from_slice(&(rec.len() as u32).to_le_bytes());
                buf.extend_from_slice(&rec);
            }
        }
        // Copy-up tombstones: dead write-buf nodes shadowing a frozen CSR
        // row (a node DELETE against a frozen node). Same WAL-vs-no-WAL
        // asymmetry as delta edges — without this the deleted node
        // RESURRECTS on the replica. Same filter as `freeze_and_compact`'s
        // dead-shadow collection.
        {
            use slotmap::Key;
            let shadows: Vec<(u64, u64)> = graph
                .write_buf
                .iter_dead_nodes()
                .filter(|(k, _)| {
                    segments
                        .immutable
                        .iter()
                        .any(|s| s.lookup_node(*k).is_some())
                })
                .map(|(k, n)| (k.data().as_ffi(), n.deleted_lsn))
                .collect();
            buf.extend_from_slice(&(shadows.len() as u32).to_le_bytes());
            for (ffi, lsn) in shadows {
                buf.extend_from_slice(&ffi.to_le_bytes());
                buf.extend_from_slice(&lsn.to_le_bytes());
            }
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
            let mut seg = match CsrSegment::from_bytes(bytes) {
                Ok(seg) => seg,
                Err(e) => {
                    tracing::warn!(
                        graph = %String::from_utf8_lossy(&name),
                        error = ?e,
                        "replica graph sync: corrupt CSR segment in snapshot — aborting install"
                    );
                    return None;
                }
            };
            // Re-apply the master's validity overlay (soft-deleted edges are
            // not part of the CSR byte format — see module doc).
            let deleted = cur.u32()?;
            for _ in 0..deleted {
                let idx = cur.u32()?;
                if idx >= seg.edge_count() {
                    return None; // malformed sidecar
                }
                seg.mark_deleted(idx);
            }
            segments.push(std::sync::Arc::new(CsrStorage::Heap(seg)));
        }

        // Cross-tier delta edges + copy-up tombstones (parsed before the
        // graph mutations below so a truncated blob aborts cleanly first).
        let delta_count = cur.u32()? as usize;
        let mut delta_edges = Vec::with_capacity(delta_count);
        for _ in 0..delta_count {
            let ek = cur.u64()?;
            let src = cur.u64()?;
            let dst = cur.u64()?;
            let edge_type = cur.u16()?;
            let created_lsn = cur.u64()?;
            let stored_off = cur.u32()?;
            let rec_len = cur.u32()? as usize;
            let rec = cur.take(rec_len)?;
            let weight = crate::graph::csr::props::decode_edge_weight(rec, stored_off);
            let props = crate::graph::csr::props::decode_edge_props(rec, stored_off);
            let props = if props.is_empty() { None } else { Some(props) };
            delta_edges.push((ek, src, dst, edge_type, created_lsn, weight, props));
        }
        let shadow_count = cur.u32()? as usize;
        let mut shadows: Vec<(crate::graph::types::NodeKey, u64)> =
            Vec::with_capacity(shadow_count);
        for _ in 0..shadow_count {
            let ffi = cur.u64()?;
            let lsn = cur.u64()?;
            shadows.push((slotmap::KeyData::from_ffi(ffi).into(), lsn));
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
        // Cross-tier delta edges re-enter the write buffer under their
        // ORIGINAL edge ids (later streamed REMOVEEDGE records must resolve).
        for (ek, src, dst, edge_type, created_lsn, weight, props) in delta_edges {
            let src_key: crate::graph::types::NodeKey = slotmap::KeyData::from_ffi(src).into();
            let dst_key: crate::graph::types::NodeKey = slotmap::KeyData::from_ffi(dst).into();
            if graph
                .write_buf
                .add_edge_across_tiers_with_id(
                    ek,
                    src_key,
                    dst_key,
                    edge_type,
                    weight,
                    props,
                    created_lsn,
                )
                .is_err()
            {
                tracing::warn!(
                    graph = %String::from_utf8_lossy(&name),
                    edge = ek,
                    "replica graph sync: cross-tier delta edge failed to install"
                );
            }
        }
        // Copy-up tombstones LAST: they shadow rows in the just-installed
        // segments (restore_dead_shadows copy-ups from graph.segments).
        graph.restore_dead_shadows(&shadows);
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

    /// Adversarial-review P1-5 + the write-buf remainder gaps found while
    /// fixing it: (1) segment validity overlays (soft-deleted edges) are not
    /// part of the CSR byte format, (2) cross-tier delta edges are RETAINED
    /// by freeze() and live in no segment, (3) dead copy-up shadow nodes
    /// tombstone frozen rows from the write buffer. All three exist only in
    /// master memory + WAL — a replica must get them from the blob or it
    /// resurrects deleted data and silently loses cross-tier edges.
    #[test]
    fn validity_overlay_delta_edges_and_dead_shadows_survive_export() {
        use crate::graph::segment::GraphSegmentList;
        use slotmap::Key;

        let mut master = GraphStore::new();
        master
            .create_graph(Bytes::from_static(b"g1"), 64_000, 0)
            .unwrap();
        let lsn = master.allocate_lsn();
        let g = master.get_graph_mut(b"g1").unwrap();
        let person = label_to_id(b"Person");
        let a = g
            .write_buf
            .add_node(smallvec![person], smallvec![], None, lsn);
        let b = g
            .write_buf
            .add_node(smallvec![person], smallvec![], None, lsn);
        let c = g
            .write_buf
            .add_node(smallvec![person], smallvec![], None, lsn);
        g.write_buf
            .add_edge(a, b, label_to_id(b"KNOWS"), 1.0, None, lsn)
            .unwrap();
        assert!(g.freeze_and_compact(lsn + 1), "freeze must succeed");

        // (1) Soft-delete the frozen KNOWS edge — overlay only.
        {
            let segs = g.segments.load();
            assert_eq!(segs.immutable.len(), 1);
            let mut heap =
                CsrSegment::from_bytes(&segs.immutable[0].to_bytes()).expect("round-trip");
            heap.mark_deleted(0);
            g.segments.swap(GraphSegmentList {
                mutable: segs.mutable.clone(),
                immutable: vec![std::sync::Arc::new(CsrStorage::Heap(heap))],
            });
        }
        // (2) Cross-tier delta edges between frozen endpoints: one with
        // weight+props (record payload), one default-weight (empty record).
        let delta_ek = g
            .write_buf
            .add_edge_across_tiers(
                a,
                b,
                label_to_id(b"LIKES"),
                2.5,
                Some(smallvec![(
                    label_to_id(b"since"),
                    crate::graph::types::PropertyValue::Int(2020),
                )]),
                lsn + 2,
            )
            .expect("delta edge");
        g.write_buf
            .add_edge_across_tiers(b, a, label_to_id(b"SEES"), 1.0, None, lsn + 2)
            .expect("default-weight delta edge");
        // (3) Copy-up delete of frozen node c (dead shadow tombstone).
        assert!(g.copy_up_node(c), "copy-up of frozen c");
        assert!(g.write_buf.remove_node(c, lsn + 3), "shadow delete");

        let blob = export_graph_store(&mut master);
        let mut replica = GraphStore::new();
        assert_eq!(install_graph_store(&mut replica, &blob), Some(1));

        let rg = replica.get_graph(b"g1").expect("g1 installed");
        let segs = rg.segments.load();
        assert_eq!(segs.immutable.len(), 1);
        // (1) The frozen KNOWS edge stays deleted on the replica.
        assert!(
            !segs.immutable[0].is_valid(0),
            "segment validity overlay lost — deleted edge resurrected"
        );
        // (2) Both delta edges re-installed under their original ids.
        let delta: Vec<_> = rg.write_buf.iter_edges().collect();
        assert_eq!(delta.len(), 2, "cross-tier delta edges lost");
        let (ek0, e0) = delta
            .iter()
            .find(|(_, e)| e.edge_type == label_to_id(b"LIKES"))
            .expect("LIKES delta edge");
        assert_eq!(ek0.data().as_ffi(), delta_ek.data().as_ffi());
        assert_eq!(e0.weight, 2.5);
        assert_eq!(
            e0.properties.as_ref().and_then(|p| p
                .iter()
                .find(|(k, _)| *k == label_to_id(b"since"))
                .map(|(_, v)| v.clone())),
            Some(crate::graph::types::PropertyValue::Int(2020)),
        );
        // (3) Node c stays tombstoned (dead shadow re-materialized).
        let shadow = rg.write_buf.get_node(c).expect("shadow present");
        assert_ne!(shadow.deleted_lsn, u64::MAX, "shadow must be dead");
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
