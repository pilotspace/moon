//! MergedNodeView — unified node resolution across the mutable MemGraph tier
//! and the immutable CSR segment tier.
//!
//! `MemGraph::freeze` DRAINS nodes into a CSR segment (it does not copy), so
//! any read path that consults only the mutable tier silently loses every
//! frozen node. This view is the single seam read paths (Cypher NodeScan /
//! property eval, GRAPH.NEIGHBORS existence, GRAPH.HYBRID) go through to see
//! the whole graph.
//!
//! Tier invariant: a NodeKey lives in EXACTLY ONE tier, so lookups here need
//! no dedup -- but this is actively ENFORCED, not automatic, across the two
//! ways a mutable-tier `MemGraph` comes into being.
//!
//! In-process freeze (`NamedGraph::freeze_and_compact`) needs no extra work:
//! `freeze()` drains keys out of the slotmap and thaws `write_buf` IN PLACE
//! (same underlying slot maps), so slot reuse bumps the generation and a
//! drained key can never reappear in the mutable tier; `compact_segments`
//! swaps the segment list atomically, so within one loaded snapshot the
//! segments are pairwise disjoint too.
//!
//! Restart (`recover_graph_store` + WAL replay, `src/graph/recovery.rs` +
//! `src/graph/replay.rs`) is different: a FRESH `MemGraph` has no history
//! with the loaded CSR segments' generations, so its deterministic SlotMap
//! allocation (index 0, generation 1 first) could otherwise numerically
//! alias a persisted `external_id` -- silently shadowing a frozen node,
//! since lookups here check the mutable tier first. The invariant is
//! restored by two restart-only mechanisms: `recover_graph_store` seeds the
//! post-recovery mutable tier via `MemGraph::with_id_offset` with a
//! watermark one past the largest persisted `external_id` (so fresh keys
//! can never collide), and `replay.rs`'s AddNode replay loop skips
//! re-inserting any `node_id` already resident in a loaded segment (avoiding
//! double enumeration when un-truncated WAL history replays a node that is
//! already frozen). See `tests/graph_restart_id_aliasing.rs` for the
//! regression coverage.

use std::sync::Arc;

use roaring::RoaringBitmap;
use smallvec::SmallVec;

use crate::graph::csr::CsrStorage;
use crate::graph::memgraph::MemGraph;
use crate::graph::types::{NodeKey, PropertyMap, PropertyValue};
use crate::graph::visibility::{is_meta_visible, is_node_visible};

/// Borrowed view over both graph tiers. Cheap to construct (two references);
/// build one per query from `NamedGraph::write_buf` + a loaded segment guard.
pub struct MergedNodeView<'a> {
    memgraph: &'a MemGraph,
    segments: &'a [Arc<CsrStorage>],
}

impl<'a> MergedNodeView<'a> {
    pub fn new(memgraph: &'a MemGraph, segments: &'a [Arc<CsrStorage>]) -> Self {
        Self { memgraph, segments }
    }

    /// Locate a frozen node: `(segment, CSR row)`. Mutable-tier nodes return
    /// `None` — callers that care about both tiers check `memgraph` first.
    /// Newest segment wins on the (impossible-by-invariant) overlap.
    pub fn segment_row(&self, key: NodeKey) -> Option<(&'a CsrStorage, u32)> {
        for seg in self.segments.iter().rev() {
            if let Some(row) = seg.lookup_node(key) {
                return Some((seg.as_ref(), row));
            }
        }
        None
    }

    /// Does the node exist in ANY tier (ignoring MVCC/valid-time visibility)?
    pub fn contains(&self, key: NodeKey) -> bool {
        self.memgraph.get_node(key).is_some() || self.segment_row(key).is_some()
    }

    /// Node visibility across tiers.
    pub fn is_visible(
        &self,
        key: NodeKey,
        snapshot_lsn: u64,
        my_txn_id: u64,
        committed: &RoaringBitmap,
        valid_at: Option<i64>,
    ) -> bool {
        if let Some(node) = self.memgraph.get_node(key) {
            return is_node_visible(node, snapshot_lsn, my_txn_id, committed, valid_at);
        }
        if let Some((seg, row)) = self.segment_row(key) {
            if let Some(meta) = seg.node_meta().get(row as usize) {
                return is_meta_visible(meta, snapshot_lsn, my_txn_id, committed, valid_at);
            }
        }
        false
    }

    /// Single node property lookup across tiers (frozen side decodes one
    /// record from the v5 blob without materializing the whole map).
    pub fn property(&self, key: NodeKey, prop_id: u16) -> Option<PropertyValue> {
        if let Some(node) = self.memgraph.get_node(key) {
            return node
                .properties
                .iter()
                .find(|(pid, _)| *pid == prop_id)
                .map(|(_, v)| v.clone());
        }
        let (seg, row) = self.segment_row(key)?;
        seg.node_property(row, prop_id)
    }

    /// All node properties across tiers. `None` = node not found in any tier;
    /// an existing node with no properties yields an empty map.
    pub fn properties(&self, key: NodeKey) -> Option<PropertyMap> {
        if let Some(node) = self.memgraph.get_node(key) {
            return Some(node.properties.clone());
        }
        let (seg, row) = self.segment_row(key)?;
        Some(seg.node_properties(row))
    }

    /// Node embedding across tiers (`None` = node absent or no embedding).
    pub fn embedding(&self, key: NodeKey) -> Option<Vec<f32>> {
        if let Some(node) = self.memgraph.get_node(key) {
            return node.embedding.clone();
        }
        let (seg, row) = self.segment_row(key)?;
        seg.node_embedding(row)
    }

    /// Node labels across tiers. Frozen rows decode `label_bitmap` (ids 0-31)
    /// plus the sparse overflow map (ids >= 32).
    pub fn labels(&self, key: NodeKey) -> Option<SmallVec<[u16; 4]>> {
        if let Some(node) = self.memgraph.get_node(key) {
            return Some(node.labels.clone());
        }
        let (seg, row) = self.segment_row(key)?;
        let meta = seg.node_meta().get(row as usize)?;
        let mut out: SmallVec<[u16; 4]> = SmallVec::new();
        let mut bm = meta.label_bitmap;
        while bm != 0 {
            out.push(bm.trailing_zeros() as u16);
            bm &= bm - 1;
        }
        if let Some(extra) = seg.label_overflow().get(&row) {
            out.extend_from_slice(extra);
        }
        Some(out)
    }

    /// Enumerate every VISIBLE node key across both tiers, optionally
    /// restricted to a label. Frozen rows use the per-segment `LabelIndex`
    /// (which already folds in overflow labels); the mutable tier checks
    /// `labels` inline. Callback order: mutable tier first, then segments
    /// oldest-to-newest.
    pub fn for_each_visible_node(
        &self,
        label: Option<u16>,
        snapshot_lsn: u64,
        my_txn_id: u64,
        committed: &RoaringBitmap,
        valid_at: Option<i64>,
        mut f: impl FnMut(NodeKey),
    ) {
        for (key, node) in self.memgraph.iter_nodes() {
            if let Some(lid) = label {
                if !node.labels.contains(&lid) {
                    continue;
                }
            }
            if !is_node_visible(node, snapshot_lsn, my_txn_id, committed, valid_at) {
                continue;
            }
            f(key);
        }
        for seg in self.segments {
            let metas = seg.node_meta();
            match label {
                Some(lid) => {
                    let Some(bm) = seg.label_index().nodes_with_label(lid) else {
                        continue;
                    };
                    for row in bm {
                        let Some(meta) = metas.get(row as usize) else {
                            continue;
                        };
                        if !is_meta_visible(meta, snapshot_lsn, my_txn_id, committed, valid_at) {
                            continue;
                        }
                        f(NodeKey::from(slotmap::KeyData::from_ffi(meta.external_id)));
                    }
                }
                None => {
                    for meta in metas {
                        if !is_meta_visible(meta, snapshot_lsn, my_txn_id, committed, valid_at) {
                            continue;
                        }
                        f(NodeKey::from(slotmap::KeyData::from_ffi(meta.external_id)));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;

    use crate::graph::csr::CsrSegment;
    use crate::graph::memgraph::MemGraph;
    use crate::graph::types::PropertyValue;

    /// Two-tier fixture: nodes A,B frozen into a CSR segment, node C live in
    /// a fresh MemGraph. A carries label 1 + a property + an embedding.
    fn fixture() -> (MemGraph, Vec<Arc<CsrStorage>>, NodeKey, NodeKey, NodeKey) {
        let mut mg = MemGraph::new(64);
        let a = mg.add_node(
            smallvec![1u16],
            smallvec![(7u16, PropertyValue::Int(42))],
            Some(vec![1.0, 2.0]),
            1,
        );
        let b = mg.add_node(smallvec![2u16], SmallVec::new(), None, 2);
        mg.add_edge(a, b, 0, 1.0, None, 3).expect("edge");
        let seg = CsrSegment::from_frozen(mg.freeze().expect("freeze"), 3).expect("csr");
        mg.thaw();
        assert_eq!(mg.node_count(), 0, "freeze drains");
        let c = mg.add_node(smallvec![1u16], SmallVec::new(), None, 4);
        let segs: Vec<Arc<CsrStorage>> = vec![Arc::new(CsrStorage::Heap(seg))];
        (mg, segs, a, b, c)
    }

    #[test]
    fn test_view_contains_both_tiers() {
        let (mg, segs, a, b, c) = fixture();
        let view = MergedNodeView::new(&mg, &segs);
        assert!(view.contains(a));
        assert!(view.contains(b));
        assert!(view.contains(c));
        assert!(!view.contains(NodeKey::from(slotmap::KeyData::from_ffi(u64::MAX))));
    }

    #[test]
    fn test_view_property_and_embedding_from_frozen() {
        let (mg, segs, a, b, _c) = fixture();
        let view = MergedNodeView::new(&mg, &segs);
        assert_eq!(view.property(a, 7), Some(PropertyValue::Int(42)));
        assert_eq!(view.property(a, 8), None);
        assert_eq!(view.property(b, 7), None);
        assert_eq!(view.embedding(a), Some(vec![1.0, 2.0]));
        assert_eq!(view.embedding(b), None);
    }

    #[test]
    fn test_view_labels_from_frozen() {
        let (mg, segs, a, _b, c) = fixture();
        let view = MergedNodeView::new(&mg, &segs);
        assert_eq!(view.labels(a).expect("frozen labels").as_slice(), &[1u16]);
        assert_eq!(view.labels(c).expect("live labels").as_slice(), &[1u16]);
    }

    #[test]
    fn test_view_scan_merges_tiers_with_label_filter() {
        let (mg, segs, a, b, c) = fixture();
        let view = MergedNodeView::new(&mg, &segs);
        let committed = RoaringBitmap::new();

        let mut all = Vec::new();
        view.for_each_visible_node(None, 0, 0, &committed, None, |k| all.push(k));
        assert_eq!(all.len(), 3);
        assert!(all.contains(&a) && all.contains(&b) && all.contains(&c));

        let mut labeled = Vec::new();
        view.for_each_visible_node(Some(1), 0, 0, &committed, None, |k| labeled.push(k));
        assert_eq!(labeled.len(), 2);
        assert!(labeled.contains(&a) && labeled.contains(&c));
    }
}
