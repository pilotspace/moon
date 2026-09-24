//! moon#1213 item 3: WARM (`.mpf`) segments keep the sub-centroid signs.
//!
//! HEAD wrote no signs at HOT->WARM, and `WarmSearchSegment` always searched
//! with an empty sign buffer: every segment silently moved from the 32-level
//! LUT to the 16-level one the moment it aged into WARM (and again after any
//! COLD->WARM reload). The signs now ride in `codes.mpf` after the codes,
//! announced by the `has_sub_signs` byte the VecCodes sub-header reserved.

use std::sync::Arc;

use bytes::Bytes;

use crate::persistence::manifest::ShardManifest;
use crate::storage::tiered::SegmentHandle;
use crate::vector::distance;
use crate::vector::hnsw::search::SearchScratch;
use crate::vector::persistence::segment_io::{read_immutable_segment, write_immutable_segment};
use crate::vector::persistence::warm_search::WarmSearchSegment;
use crate::vector::persistence::warm_segment::{
    codes_mpf_has_sub_signs, write_codes_mpf, write_codes_mpf_with_sub_signs, write_graph_mpf,
    write_mvcc_mpf,
};
use crate::vector::segment::SegmentList;
use crate::vector::segment::compaction::{MergeMode, compact};
use crate::vector::segment::immutable::ImmutableSegment;
use crate::vector::segment::mutable::MutableSegment;
use crate::vector::store::{IndexMeta, VectorStore};
use crate::vector::turbo_quant::collection::{BuildMode, CollectionMetadata, QuantizationConfig};
use crate::vector::turbo_quant::encoder::padded_dimension;
use crate::vector::types::{DistanceMetric, SearchResult};

/// Clustered fixture (16 centres + noise), not isotropic Gaussian.
fn clustered(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut s = seed;
    let mut next = move || {
        s = s
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((s >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
    };
    let centers: Vec<Vec<f32>> = (0..16)
        .map(|_| (0..dim).map(|_| next()).collect())
        .collect();
    (0..n)
        .map(|i| centers[i % 16].iter().map(|&c| c + 0.3 * next()).collect())
        .collect()
}

fn meta(dim: u32, quantization: QuantizationConfig, metric: DistanceMetric) -> IndexMeta {
    IndexMeta {
        name: Bytes::from_static(b"w"),
        dimension: dim,
        padded_dimension: padded_dimension(dim),
        metric,
        hnsw_m: 16,
        hnsw_ef_construction: 200,
        hnsw_ef_runtime: 0,
        compact_threshold: 0,
        source_field: Bytes::from_static(b"v"),
        key_prefixes: vec![Bytes::from_static(b"w:")],
        quantization,
        build_mode: BuildMode::Light,
        vector_fields: Vec::new(),
        schema_fields: Vec::new(),
        merge_mode: MergeMode::GraphUnion,
        keep_raw: false,
        db_index: 0,
        rerank_mult: 4,
        exact_beam: false,
    }
}

/// A compacted segment for `col`, WITHOUT its f16 sidecar so that search
/// distances are the beam's own (a sidecar rerank would overwrite them).
fn segment(col: &Arc<CollectionMetadata>, data: &[Vec<f32>]) -> ImmutableSegment {
    let seg = MutableSegment::new(col.dimension, col.clone());
    for (i, v) in data.iter().enumerate() {
        seg.append(1_000 + i as u64, v, i as u64 + 1);
    }
    compact(&seg.freeze(), col, 99, None)
        .expect("compact")
        .with_raw_f16(None)
}

type Hits = Vec<Vec<(u32, u64, u32)>>;

fn hits(r: &[SearchResult]) -> Vec<(u32, u64, u32)> {
    r.iter()
        .map(|h| (h.id.0, h.key_hash, h.distance.to_bits()))
        .collect()
}

/// Install the segment `build` makes as the only HOT segment of a fresh
/// one-index store, search it, move it to WARM through the store's own
/// transition, search again.
fn hot_then_warm(
    quant: QuantizationConfig,
    metric: DistanceMetric,
    dim: usize,
    build: impl FnOnce(&Arc<CollectionMetadata>, &[Vec<f32>]) -> ImmutableSegment,
) -> (
    Hits,
    Hits,
    Vec<u8>,
    Arc<WarmSearchSegment>,
    tempfile::TempDir,
) {
    distance::init();
    crate::vector::turbo_quant::fwht::init_fwht();
    let mut store = VectorStore::new();
    store
        .create_index(meta(dim as u32, quant, metric))
        .expect("create");
    let idx = store.get_index(b"w").expect("index");
    let data = clustered(600, dim, 5);
    let imm = Arc::new(build(&idx.collection, &data));
    let hot_signs = imm.sub_centroid_signs().to_vec();

    let queries = clustered(40, dim, 6);
    let mut scratch = SearchScratch::new(0, idx.collection.padded_dimension);
    let hot: Hits = queries
        .iter()
        .map(|q| hits(&imm.search(q, 10, 24, &mut scratch)))
        .collect();

    let old = idx.segments.load();
    idx.segments.swap(SegmentList {
        mutable: Arc::clone(&old.mutable),
        immutable: vec![imm],
        ivf: Vec::new(),
        warm: Vec::new(),
        unloaded: Vec::new(),
    });
    drop(old);

    let tmp = tempfile::tempdir().expect("tempdir");
    let mut manifest =
        ShardManifest::create(&tmp.path().join("shard-0.manifest")).expect("manifest");
    let mut next_file_id = 1u64;
    let moved =
        store.try_warm_transitions_all(tmp.path(), &mut manifest, 0, &mut next_file_id, &mut None);
    assert_eq!(moved, 1, "segment must go WARM");
    let idx = store.get_index(b"w").expect("index");
    let warm = Arc::clone(&idx.segments.load().warm[0]);
    let warm_hits: Hits = queries
        .iter()
        .map(|q| hits(&warm.search(q, 10, 24, &mut scratch)))
        .collect();
    (hot, warm_hits, hot_signs, warm, tmp)
}

#[test]
fn warm_segment_ranks_exactly_like_its_hot_source() {
    // dim 100 → padded 128: not a multiple of 16, sign rows of 16 bytes.
    for (dim, metric) in [(100usize, DistanceMetric::L2), (64, DistanceMetric::Cosine)] {
        let (hot, warm, hot_signs, warm_seg, _tmp) =
            hot_then_warm(QuantizationConfig::TurboQuant4, metric, dim, segment);
        assert!(!hot_signs.is_empty(), "fixture must carry real signs");
        // Red on HEAD: the WARM beam scored with the 16-level LUT, so every
        // distance (and some rankings) differed from the HOT segment's.
        assert_eq!(warm, hot, "{metric:?} {dim}d: WARM must rank like HOT");
        assert_eq!(warm_seg.sub_centroid_signs(), &hot_signs[..]);
    }
}

/// A TQ4 segment whose sign buffer is all zeros — the shape of an
/// insert-time placeholder (LIGHT TQ4A2 / SQ8) — via a persisted round trip.
fn zero_signed_segment(col: &Arc<CollectionMetadata>, data: &[Vec<f32>]) -> ImmutableSegment {
    let imm = segment(col, data);
    let tmp = tempfile::tempdir().expect("tempdir");
    write_immutable_segment(tmp.path(), 1, &imm, col).expect("persist");
    let signs = tmp.path().join("segment-1/sub_signs.bin");
    let len = std::fs::metadata(&signs).expect("signs").len() as usize;
    std::fs::write(&signs, vec![0u8; len]).expect("zero signs");
    let _ = std::fs::remove_file(tmp.path().join("segment-1/raw_f16.bin"));
    read_immutable_segment(tmp.path(), 1).expect("reload").0
}

#[test]
fn placeholder_sign_buffers_are_not_carried_to_warm() {
    // An all-zero buffer (LIGHT TQ4A2/SQ8 insert-time placeholders) would pin
    // every coordinate to the lower sub-bin; SQ8 never reads signs at all.
    // (TQ4A2 itself cannot be compacted in a debug build here: its beam trips
    // a pre-existing code-layout assertion that a separate fix owns.)
    for (quant, build) in [
        (
            QuantizationConfig::Sq8,
            segment as fn(&Arc<CollectionMetadata>, &[Vec<f32>]) -> ImmutableSegment,
        ),
        (QuantizationConfig::TurboQuant4, zero_signed_segment),
    ] {
        let (_hot, warm, hot_signs, warm_seg, tmp) =
            hot_then_warm(quant, DistanceMetric::L2, 64, build);
        assert!(!hot_signs.is_empty(), "{quant:?} fixture");
        assert!(hot_signs.iter().all(|&b| b == 0), "{quant:?} fixture");
        assert!(warm_seg.sub_centroid_signs().is_empty(), "{quant:?}");
        let codes = std::fs::read(tmp.path().join("vectors/segment-1/codes.mpf")).unwrap();
        assert!(
            !codes_mpf_has_sub_signs(&codes),
            "{quant:?}: flag must stay 0"
        );
        assert!(
            warm.iter().all(|h| !h.is_empty()),
            "{quant:?}: WARM still searches"
        );
    }
}

/// Write a WARM directory for `imm` by hand with `codes_writer`.
fn warm_dir(
    dir: &std::path::Path,
    imm: &ImmutableSegment,
    codes_writer: impl FnOnce(&std::path::Path, &[u8]),
) {
    std::fs::create_dir_all(dir).unwrap();
    codes_writer(&dir.join("codes.mpf"), imm.vectors_tq().as_slice());
    write_graph_mpf(
        &dir.join("graph.mpf"),
        7,
        &imm.graph().to_bytes_compressed(),
    )
    .unwrap();
    write_mvcc_mpf(&dir.join("mvcc.mpf"), 7, &imm.mvcc_raw_bytes()).unwrap();
}

fn open_warm(dir: &std::path::Path, col: &Arc<CollectionMetadata>) -> WarmSearchSegment {
    WarmSearchSegment::from_files(
        dir,
        7,
        col.clone(),
        SegmentHandle::new(7, dir.to_path_buf()),
        false,
    )
    .expect("open warm")
}

#[test]
fn legacy_warm_files_without_signs_load_and_search_with_the_16_level_lut() {
    // Backward compatibility: a WARM directory written before moon#1213 has
    // `has_sub_signs = 0` and codes only. It loads with no signs and ranks
    // exactly like its HOT source would without signs (16-level LUT).
    distance::init();
    crate::vector::turbo_quant::fwht::init_fwht();
    let dim = 96;
    let col = Arc::new(CollectionMetadata::new(
        3,
        dim as u32,
        DistanceMetric::L2,
        QuantizationConfig::TurboQuant4,
        3,
    ));
    let imm = segment(&col, &clustered(500, dim, 8));
    let tmp = tempfile::tempdir().unwrap();

    // The HOT twin without signs: persist, drop sub_signs.bin + sidecar.
    write_immutable_segment(tmp.path(), 1, &imm, &col).unwrap();
    let _ = std::fs::remove_file(tmp.path().join("segment-1/sub_signs.bin"));
    let _ = std::fs::remove_file(tmp.path().join("segment-1/raw_f16.bin"));
    let (hot16, _) = read_immutable_segment(tmp.path(), 1).unwrap();
    assert!(hot16.sub_centroid_signs().is_empty());

    let dir = tmp.path().join("legacy-warm");
    warm_dir(&dir, &imm, |p, codes| write_codes_mpf(p, 7, codes).unwrap());
    assert!(!codes_mpf_has_sub_signs(
        &std::fs::read(dir.join("codes.mpf")).unwrap()
    ));
    let warm = open_warm(&dir, &col);
    assert!(warm.sub_centroid_signs().is_empty());
    assert_eq!(warm.codes_data(), imm.vectors_tq().as_slice());

    let mut scratch = SearchScratch::new(0, col.padded_dimension);
    for q in clustered(20, dim, 9) {
        assert_eq!(
            hits(&warm.search(&q, 10, 24, &mut scratch)),
            hits(&hot16.search(&q, 10, 24, &mut scratch))
        );
    }
}

#[test]
fn flagged_codes_file_of_the_wrong_size_keeps_codes_and_drops_signs() {
    distance::init();
    crate::vector::turbo_quant::fwht::init_fwht();
    let dim = 64;
    let col = Arc::new(CollectionMetadata::new(
        4,
        dim as u32,
        DistanceMetric::L2,
        QuantizationConfig::TurboQuant4,
        4,
    ));
    let imm = segment(&col, &clustered(300, dim, 10));
    let signs = imm.sub_centroid_signs().to_vec();
    for cut in [1usize, signs.len() / 2] {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("warm");
        let short = &signs[..signs.len() - cut];
        warm_dir(&dir, &imm, |p, codes| {
            write_codes_mpf_with_sub_signs(p, 7, codes, short).unwrap()
        });
        assert!(codes_mpf_has_sub_signs(
            &std::fs::read(dir.join("codes.mpf")).unwrap()
        ));
        let warm = open_warm(&dir, &col);
        assert!(warm.sub_centroid_signs().is_empty(), "cut {cut}");
        assert_eq!(warm.codes_data(), imm.vectors_tq().as_slice(), "cut {cut}");
        let mut scratch = SearchScratch::new(0, col.padded_dimension);
        assert!(
            !warm
                .search(&clustered(1, dim, 11)[0], 5, 24, &mut scratch)
                .is_empty()
        );
    }

    // And the well-formed file round-trips the signs exactly.
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("warm");
    warm_dir(&dir, &imm, |p, codes| {
        write_codes_mpf_with_sub_signs(p, 7, codes, &signs).unwrap()
    });
    let warm = open_warm(&dir, &col);
    assert_eq!(warm.sub_centroid_signs(), &signs[..]);
    assert_eq!(warm.codes_data(), imm.vectors_tq().as_slice());
}

#[test]
fn flagged_all_zero_signs_are_a_placeholder_not_signs() {
    // Same rule as `segment_io`'s sub_signs.bin (PR #1221 review): an
    // all-zero buffer would pin every coordinate to the lower sub-bin.
    distance::init();
    crate::vector::turbo_quant::fwht::init_fwht();
    let dim = 64;
    let col = Arc::new(CollectionMetadata::new(
        6,
        dim as u32,
        DistanceMetric::L2,
        QuantizationConfig::TurboQuant4,
        6,
    ));
    let imm = segment(&col, &clustered(200, dim, 12));
    let zeros = vec![0u8; imm.sub_centroid_signs().len()];
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("warm");
    warm_dir(&dir, &imm, |p, codes| {
        write_codes_mpf_with_sub_signs(p, 7, codes, &zeros).unwrap()
    });
    let warm = open_warm(&dir, &col);
    assert!(warm.sub_centroid_signs().is_empty());
    assert_eq!(warm.codes_data(), imm.vectors_tq().as_slice());
}
