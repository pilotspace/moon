//! Steady-state tombstone bookkeeping of [`ImmutableSegment`] (moon#1066),
//! plus the row fixture the recovery keymap-gate tests share.

use std::sync::Arc;

use super::{ImmutableSegment, MvccHeader};
use crate::vector::aligned_buffer::AlignedBuffer;
use crate::vector::hnsw::graph::HnswGraph;
use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
use crate::vector::types::DistanceMetric;

impl ImmutableSegment {
    /// Test fixture: a segment whose MVCC rows are `(key_hash, global_id,
    /// delete_lsn)`, with an empty graph -- the tombstone bookkeeping and the
    /// recovery keymap gate never read the graph.
    pub(crate) fn with_test_rows(rows: &[(u64, u32, u64)]) -> Self {
        crate::vector::distance::init();
        let collection = Arc::new(CollectionMetadata::new(
            1,
            8,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
        ));
        let graph = HnswGraph::new(
            0,
            16,
            32,
            0,
            0,
            AlignedBuffer::new(0),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            68,
        );
        let mvcc: Vec<MvccHeader> = rows
            .iter()
            .enumerate()
            .map(|(i, &(key_hash, global_id, delete_lsn))| MvccHeader {
                internal_id: i as u32,
                global_id,
                key_hash,
                insert_lsn: 1,
                delete_lsn,
                hint_committed: 1,
            })
            .collect();
        let live = rows.iter().filter(|r| r.2 == 0).count() as u32;
        Self::new(
            graph,
            AlignedBuffer::new(0),
            Vec::new(),
            Vec::new(),
            16,
            Vec::new(),
            16,
            mvcc,
            collection,
            live,
            rows.len() as u32,
        )
    }
}

/// A steady-state tombstone is recorded and counted only by the segment
/// holding a live row for the key, exactly once.
#[test]
fn steady_state_tombstone_counts_only_the_segment_holding_the_key() {
    let seg = ImmutableSegment::with_test_rows(&[(10, 0, 0), (20, 1, 0), (30, 2, 0)]);
    assert_eq!(seg.live_count(), 3);

    assert_eq!(seg.mark_deleted_by_key_hash(99), 0, "not held here");
    assert_eq!(seg.live_count(), 3, "a foreign key is never counted");
    assert!(
        seg.tombstoned_key_hashes().is_empty(),
        "a foreign key is never recorded"
    );

    assert_eq!(seg.mark_deleted_by_key_hash(20), 1);
    assert_eq!(seg.live_count(), 2, "the held row is counted dead");
    assert_eq!(seg.mark_deleted_by_key_hash(20), 0, "idempotent");
    assert_eq!(seg.live_count(), 2);
    assert_eq!(
        seg.mvcc_live_count(),
        3,
        "persistence still sees the headers' count"
    );
    assert_eq!(
        seg.live_key_hashes().collect::<Vec<_>>(),
        vec![10, 30],
        "the tombstoned key is no longer live"
    );
}

/// A row an install-time tombstone already killed is not counted again.
#[test]
fn steady_state_tombstone_skips_rows_already_dead_at_install() {
    let seg = ImmutableSegment::with_test_rows(&[(10, 0, 0), (20, 1, 5)]);
    assert_eq!(seg.live_count(), 1);
    assert_eq!(seg.mark_deleted_by_key_hash(20), 0, "no live row for 20");
    assert_eq!(seg.live_count(), 1);
}
