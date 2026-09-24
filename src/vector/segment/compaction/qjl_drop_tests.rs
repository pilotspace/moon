//! moon#1213 item 1: immutable segments carry no QJL data.
//!
//! Proof that nothing reads it: the QJL signs / residual norms were never
//! persisted (`segment_io` reloads every segment with EMPTY buffers), a fresh
//! compaction always emits a non-empty sub-centroid sign buffer (so the
//! `rerank_with_prod` fallback was gated off), and the only in-memory reader
//! left — a GraphUnion merge whose output lost both its signs and its f16
//! sidecar — scored with the vector norm where the residual norm belongs.
//! These tests pin the resulting contract: a segment in memory is the segment
//! a restart gives back, byte for byte and result for result.

use std::fs;
use std::sync::Arc;

use crate::vector::distance;
use crate::vector::hnsw::search::SearchScratch;
use crate::vector::persistence::segment_io::{SEGMENT_FORMAT_VERSION, read_immutable_segment};
use crate::vector::segment::compaction::{MergeMode, compact, merge_immutable};
use crate::vector::segment::immutable::ImmutableSegment;
use crate::vector::segment::mutable::MutableSegment;
use crate::vector::turbo_quant::collection::{BuildMode, CollectionMetadata, QuantizationConfig};
use crate::vector::types::DistanceMetric;

/// Clustered fixture (8 centres + noise), not isotropic Gaussian.
fn clustered(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut s = seed;
    let mut next = move || {
        s = s
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((s >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
    };
    let centers: Vec<Vec<f32>> = (0..8).map(|_| (0..dim).map(|_| next()).collect()).collect();
    (0..n)
        .map(|i| centers[i % 8].iter().map(|&c| c + 0.35 * next()).collect())
        .collect()
}

/// An EXACT collection whose QJL seed equals its collection id — the
/// invariant `segment_io` relies on to verify the metadata checksum on reload
/// (FT.CREATE always seeds with the collection id).
fn exact_collection(dim: usize, quant: QuantizationConfig) -> Arc<CollectionMetadata> {
    distance::init();
    crate::vector::turbo_quant::fwht::init_fwht();
    Arc::new(CollectionMetadata::with_build_mode(
        9,
        dim as u32,
        DistanceMetric::L2,
        quant,
        9,
        BuildMode::Exact,
    ))
}

fn frozen(
    col: &Arc<CollectionMetadata>,
    data: &[Vec<f32>],
    key_base: u64,
    dead: &[u32],
) -> MutableSegment {
    let seg = MutableSegment::new(col.dimension, col.clone());
    for (i, v) in data.iter().enumerate() {
        seg.append(key_base + i as u64, v, key_base + i as u64 + 1);
    }
    for &id in dead {
        seg.mark_deleted(id, 1_000);
    }
    seg
}

fn results(seg: &ImmutableSegment, qs: &[Vec<f32>]) -> Vec<Vec<(u32, u64, u32)>> {
    let mut scratch = SearchScratch::new(0, seg.collection_meta().padded_dimension);
    qs.iter()
        .map(|q| {
            seg.search(q, 10, 64, &mut scratch)
                .iter()
                .map(|r| (r.id.0, r.key_hash, r.distance.to_bits()))
                .collect()
        })
        .collect()
}

#[test]
fn fresh_exact_segment_holds_nothing_its_reloaded_twin_lacks() {
    // dim 100: not a multiple of 8 or 16 (QJL rows were ceil(100/8) = 13 B).
    for quant in [QuantizationConfig::TurboQuant4, QuantizationConfig::Sq8] {
        let dim = 100;
        let col = exact_collection(dim, quant);
        let data = clustered(320, dim, 11);
        // Dead rows inside the frozen window: HEAD's QJL buffer was sized by
        // live rows but merge derived its stride from total_count.
        let seg = frozen(&col, &data, 0, &[3, 17, 200]);
        let tmp = tempfile::tempdir().unwrap();
        let fresh = compact(&seg.freeze(), &col, 7, Some((tmp.path(), 1))).expect("compact");
        let (reloaded, _) = read_immutable_segment(tmp.path(), 1).expect("reload");

        // Red on HEAD for TQ4: the fresh EXACT segment carried
        // live·(8·13 + 4) bytes of QJL signs + residual norms that the
        // reloaded twin never had (both serve the sidecar from the map).
        assert_eq!(
            fresh.resident_bytes(),
            reloaded.resident_bytes(),
            "{quant:?}: in-memory segment must not hold state a restart drops"
        );
        let qs: Vec<Vec<f32>> = data.iter().step_by(29).cloned().collect();
        assert_eq!(results(&fresh, &qs), results(&reloaded, &qs), "{quant:?}");
    }
}

#[test]
fn merged_segment_without_signs_or_sidecar_scores_like_its_reloaded_twin() {
    // The one configuration in which HEAD read immutable QJL data: a
    // GraphUnion merge of a fresh EXACT segment with a pre-v2, pre-sidecar
    // source (no signs to recompute ⇒ the merge drops signs AND sidecar for
    // every row). HEAD then reranked with the TurboQuant_prod estimator in
    // memory — fed the vector norm as the "residual norm" — while the same
    // segment reloaded from disk (QJL never persisted) searched by ADC.
    let dim = 64;
    let col = exact_collection(dim, QuantizationConfig::TurboQuant4);
    let a = compact(
        &frozen(&col, &clustered(200, dim, 21), 0, &[]).freeze(),
        &col,
        3,
        None,
    )
    .expect("compact a");

    let legacy_dir = tempfile::tempdir().unwrap();
    compact(
        &frozen(&col, &clustered(180, dim, 22), 10_000, &[]).freeze(),
        &col,
        4,
        Some((legacy_dir.path(), 2)),
    )
    .expect("compact b");
    let dir = legacy_dir.path().join("segment-2");
    fs::remove_file(dir.join("sub_signs.bin")).unwrap();
    fs::remove_file(dir.join("raw_f16.bin")).unwrap();
    let meta = fs::read_to_string(dir.join("segment_meta.json")).unwrap();
    let v1 = meta.replace(
        &format!("\"version\": {SEGMENT_FORMAT_VERSION}"),
        "\"version\": 1",
    );
    assert_ne!(meta, v1, "downgrade helper must rewrite the version");
    fs::write(dir.join("segment_meta.json"), v1).unwrap();
    let (b, _) = read_immutable_segment(legacy_dir.path(), 2).expect("load legacy");
    assert!(b.sub_centroid_signs().is_empty() && b.raw_f16().is_none());

    let out = tempfile::tempdir().unwrap();
    let merged = merge_immutable(
        &[Arc::new(a), Arc::new(b)],
        &col,
        5,
        MergeMode::GraphUnion,
        0.0,
        Some((out.path(), 3)),
    )
    .expect("merge");
    assert!(merged.sub_centroid_signs().is_empty() && merged.raw_f16().is_none());
    let (reloaded, _) = read_immutable_segment(out.path(), 3).expect("reload merged");

    let qs = clustered(24, dim, 23);
    // Red on HEAD: the in-memory merged segment's distances came from the
    // QJL estimator, the reloaded one's from ADC.
    assert_eq!(results(&merged, &qs), results(&reloaded, &qs));
    assert_eq!(merged.resident_bytes(), reloaded.resident_bytes());
}
