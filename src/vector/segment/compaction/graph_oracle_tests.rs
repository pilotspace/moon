//! moon#1213 item 4: which vectors `compact` builds the HNSW graph from.
//!
//! EXACT builds from its retained raw f32 vectors; it must not also decode
//! every TQ code into a centroid vector nothing reads. LIGHT (no raw f32)
//! still builds from the decoded vectors — the f16-sidecar oracle was
//! measured and not adopted (768d far-query recall dropped; see
//! WS11 NOTES/SUMMARY).

use std::sync::Arc;

use super::compact_path::DECODED_BUILD_ROWS;
use crate::vector::distance;
use crate::vector::segment::compaction::compact;
use crate::vector::segment::mutable::MutableSegment;
use crate::vector::test_support::EmbeddingLike;
use crate::vector::turbo_quant::collection::{BuildMode, CollectionMetadata, QuantizationConfig};
use crate::vector::types::DistanceMetric;

fn decoded_rows_for(mode: BuildMode, quant: QuantizationConfig) -> (usize, u32) {
    distance::init();
    crate::vector::turbo_quant::fwht::init_fwht();
    let dim = 48;
    let col = Arc::new(CollectionMetadata::with_build_mode(
        3,
        dim as u32,
        DistanceMetric::Cosine,
        quant,
        3,
        mode,
    ));
    let seg = MutableSegment::new(col.dimension, col.clone());
    for (i, v) in EmbeddingLike::new(dim, 8, 2).docs(150).iter().enumerate() {
        seg.append(i as u64, v, i as u64 + 1);
    }
    seg.mark_deleted(7, 999);
    DECODED_BUILD_ROWS.with(|c| c.set(0));
    let imm = compact(&seg.freeze(), &col, 9, None).expect("compact");
    (
        DECODED_BUILD_ROWS.with(std::cell::Cell::get),
        imm.live_count(),
    )
}

#[test]
fn exact_compaction_decodes_no_graph_oracle_vectors() {
    for quant in [QuantizationConfig::TurboQuant4, QuantizationConfig::Sq8] {
        // Red before the fix: EXACT decoded all 149 live vectors too.
        let (decoded, live) = decoded_rows_for(BuildMode::Exact, quant);
        assert_eq!(live, 149);
        assert_eq!(decoded, 0, "{quant:?}: EXACT builds from raw f32 only");
        // LIGHT has no raw vectors: the decoded oracle is its graph input.
        let (decoded, live) = decoded_rows_for(BuildMode::Light, quant);
        assert_eq!(
            decoded, live as usize,
            "{quant:?}: LIGHT decodes every live row"
        );
    }
}
