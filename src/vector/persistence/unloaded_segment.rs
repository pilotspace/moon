//! `UnloadedSegment` -- true COLD tier: a WARM/HOT segment's on-disk `.mpf`
//! files with EVERY in-memory structure dropped (no TQ/SQ8 codes, no HNSW
//! graph, no f16 exact-rerank sidecar). Only a small stub remains resident:
//! segment id, collection metadata (needed to reload), a doc count (for
//! `FT.INFO`), whether it had a sidecar (for the coverage counter, without
//! reloading), and a `SegmentHandle` keeping the directory alive.
//!
//! This is the WS3-round-2 fix for the finding that `WarmSearchSegment`
//! (the pre-existing WARM tier) does not actually reduce RSS: it copies
//! every `.mpf` payload into owned buffers of the same size as the HOT
//! segment it replaces. `UnloadedSegment` holds none of that -- reload is a
//! synchronous [`UnloadedSegment::reload`] call (reusing
//! [`WarmSearchSegment::from_files`], the same function the normal WARM
//! tier and server-boot paths already use) on the first search that
//! touches the index after the idle window. Recall is unaffected: the
//! reload path is identical to the existing WARM-tier load, sidecar
//! included.

use std::path::Path;
use std::sync::Arc;

use crate::storage::tiered::SegmentHandle;
use crate::vector::persistence::warm_search::WarmSearchSegment;
use crate::vector::turbo_quant::collection::CollectionMetadata;

/// A fully-unloaded (COLD) segment stub. Everything needed to reload is
/// kept; everything that costs real memory (codes, graph, sidecar) is not.
pub struct UnloadedSegment {
    segment_id: u64,
    collection_meta: Arc<CollectionMetadata>,
    /// Doc count captured at unload time -- used by `FT.INFO`'s `num_docs`
    /// without requiring a reload. Mirrors `WarmSearchSegment::total_count`.
    doc_count: u32,
    /// Whether the segment had an exact-rerank f16 sidecar before being
    /// unloaded (`vectors.mpf` present) -- surfaced by `FT.INFO` without
    /// paying a reload just to answer the question.
    had_exact_rerank: bool,
    /// Whether codes.mpf should be mlocked on reload (carried over from
    /// the segment's original warm-tier construction args).
    mlock_codes: bool,
    /// Keeps the on-disk segment directory alive (not tombstoned) until
    /// this index/segment is explicitly dropped or flushed.
    handle: SegmentHandle,
}

impl UnloadedSegment {
    /// Build a stub from an already-materialized `WarmSearchSegment` that is
    /// about to be dropped. Captures everything needed to reload later, then
    /// the caller drops `warm` (or lets it go out of scope), freeing the
    /// codes/graph/sidecar buffers.
    pub fn from_warm(warm: &WarmSearchSegment, mlock_codes: bool) -> Self {
        Self {
            segment_id: warm.segment_id(),
            collection_meta: warm.collection_meta_arc(),
            doc_count: warm.total_count(),
            had_exact_rerank: warm.raw_f16().is_some(),
            mlock_codes,
            handle: warm.handle_clone(),
        }
    }

    #[inline]
    pub fn segment_id(&self) -> u64 {
        self.segment_id
    }

    /// Doc count captured at unload time (does not require a reload).
    #[inline]
    pub fn total_count(&self) -> u32 {
        self.doc_count
    }

    /// Whether the segment had an exact-rerank sidecar before unloading.
    #[inline]
    pub fn had_exact_rerank(&self) -> bool {
        self.had_exact_rerank
    }

    /// On-disk directory backing this stub.
    #[inline]
    pub fn segment_dir(&self) -> &Path {
        self.handle.segment_dir()
    }

    /// Mark the on-disk directory for deletion once all handles (this stub's
    /// and any reloaded segment's) are dropped. Used by DROPINDEX/FLUSHALL.
    pub fn mark_tombstoned(&self) {
        self.handle.mark_tombstoned();
    }

    /// Reload into a fully-materialized `WarmSearchSegment` -- the exact same
    /// path used for normal WARM-tier construction and server-boot segment
    /// recovery, so recall/exactness (including the f16 sidecar) is
    /// unaffected by having gone through the COLD tier.
    pub fn reload(&self) -> std::io::Result<WarmSearchSegment> {
        WarmSearchSegment::from_files(
            self.handle.segment_dir(),
            self.segment_id,
            self.collection_meta.clone(),
            self.handle.clone(),
            self.mlock_codes,
        )
    }

    /// Approximate resident bytes of the stub itself -- a handful of scalars
    /// plus an `Arc` bump, not the megabytes-to-gigabytes the segment held
    /// before unloading. Used for `FT.INFO`/observability, not enforcement.
    #[inline]
    pub fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::distance;
    use crate::vector::hnsw::graph::HnswGraph;
    use crate::vector::persistence::warm_segment::{
        write_codes_mpf, write_graph_mpf, write_mvcc_mpf,
    };
    use crate::vector::turbo_quant::collection::QuantizationConfig;
    use crate::vector::types::DistanceMetric;

    fn make_collection() -> Arc<CollectionMetadata> {
        Arc::new(CollectionMetadata::new(
            1,
            128,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
        ))
    }

    fn write_minimal_segment(seg_dir: &Path, seg_id: u64) {
        std::fs::create_dir_all(seg_dir).unwrap();
        let empty_graph = HnswGraph::new(
            0,
            16,
            32,
            0,
            0,
            crate::vector::aligned_buffer::AlignedBuffer::new(0),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            68,
        );
        let graph_bytes = empty_graph.to_bytes();
        write_codes_mpf(&seg_dir.join("codes.mpf"), seg_id, &[]).unwrap();
        write_graph_mpf(&seg_dir.join("graph.mpf"), seg_id, &graph_bytes).unwrap();
        write_mvcc_mpf(&seg_dir.join("mvcc.mpf"), seg_id, &[]).unwrap();
    }

    #[test]
    fn test_from_warm_captures_metadata_and_reload_round_trips() {
        distance::init();
        let tmp = tempfile::tempdir().unwrap();
        let seg_dir = tmp.path().join("seg-1");
        write_minimal_segment(&seg_dir, 1);
        let handle = SegmentHandle::new(1, seg_dir.clone());
        let warm =
            WarmSearchSegment::from_files(&seg_dir, 1, make_collection(), handle, false).unwrap();

        let stub = UnloadedSegment::from_warm(&warm, false);
        assert_eq!(stub.segment_id(), 1);
        assert_eq!(stub.total_count(), warm.total_count());
        assert!(!stub.had_exact_rerank());
        // Stub is tiny -- nowhere near the size of the segment it replaces.
        assert!(stub.resident_bytes() < 256);

        drop(warm);

        let reloaded = stub.reload().expect("reload from stub");
        assert_eq!(reloaded.segment_id(), 1);
        assert_eq!(reloaded.total_count(), stub.total_count());
    }

    #[test]
    fn test_mark_tombstoned_removes_directory_after_last_handle_drops() {
        distance::init();
        let tmp = tempfile::tempdir().unwrap();
        let seg_dir = tmp.path().join("seg-2");
        write_minimal_segment(&seg_dir, 2);
        let handle = SegmentHandle::new(2, seg_dir.clone());
        let warm =
            WarmSearchSegment::from_files(&seg_dir, 2, make_collection(), handle, false).unwrap();
        let stub = UnloadedSegment::from_warm(&warm, false);
        drop(warm);

        assert!(seg_dir.exists());
        stub.mark_tombstoned();
        drop(stub);
        assert!(
            !seg_dir.exists(),
            "directory removed once last handle drops"
        );
    }
}
