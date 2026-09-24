//! Sub-centroid signs across persistence and merge (moon#1193).
//!
//! HEAD wrote no `sub_signs.bin` and reloaded HOT segments with an EMPTY
//! sign buffer, so every restart silently moved all pre-existing data from
//! the 32-level sub-centroid LUT to the 16-level one; a GraphUnion merge that
//! included such a source zero-filled its signs (biased 32-level scores).

use std::fs;
use std::sync::Arc;

use crate::vector::distance;
use crate::vector::hnsw::search::SearchScratch;
use crate::vector::persistence::segment_io::{
    SEGMENT_FORMAT_VERSION, read_immutable_segment, write_immutable_segment,
};
use crate::vector::segment::compaction::{MergeMode, compact, merge_immutable};
use crate::vector::segment::immutable::ImmutableSegment;
use crate::vector::segment::mutable::MutableSegment;
use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
use crate::vector::types::DistanceMetric;

fn vecs(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut s = seed;
    let mut next = move || {
        s = s
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((s >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
    };
    let centers: Vec<Vec<f32>> = (0..8).map(|_| (0..dim).map(|_| next()).collect()).collect();
    (0..n)
        .map(|i| centers[i % 8].iter().map(|&c| c + 0.4 * next()).collect())
        .collect()
}

fn collection(dim: usize) -> Arc<CollectionMetadata> {
    distance::init();
    crate::vector::turbo_quant::fwht::init_fwht();
    Arc::new(CollectionMetadata::new(
        5,
        dim as u32,
        DistanceMetric::L2,
        QuantizationConfig::TurboQuant4,
        77,
    ))
}

/// A compacted LIGHT TQ4 segment (insert-time signs + f16 sidecar).
fn built_segment(
    col: &Arc<CollectionMetadata>,
    data: &[Vec<f32>],
    key_base: u64,
) -> ImmutableSegment {
    let seg = MutableSegment::new(col.dimension, col.clone());
    for (i, v) in data.iter().enumerate() {
        seg.append(key_base + i as u64, v, key_base + i as u64 + 1);
    }
    compact(&seg.freeze(), col, 4242, None).expect("compact")
}

fn search_all(seg: &ImmutableSegment, qs: &[Vec<f32>]) -> Vec<Vec<(u32, u32)>> {
    let mut scratch = SearchScratch::new(0, seg.collection_meta().padded_dimension);
    qs.iter()
        .map(|q| {
            seg.search(q, 10, 48, &mut scratch)
                .iter()
                .map(|r| (r.id.0, r.distance.to_bits()))
                .collect()
        })
        .collect()
}

#[test]
fn reloaded_segment_keeps_sub_centroid_signs_and_identical_results() {
    let dim = 96;
    let col = collection(dim);
    let data = vecs(400, dim, 1);
    let seg = built_segment(&col, &data, 0);
    assert!(
        !seg.sub_centroid_signs().is_empty(),
        "fixture must carry signs"
    );

    let tmp = tempfile::tempdir().unwrap();
    write_immutable_segment(tmp.path(), 1, &seg, &col).unwrap();
    let meta = fs::read_to_string(tmp.path().join("segment-1/segment_meta.json")).unwrap();
    assert!(
        meta.contains(&format!("\"version\": {SEGMENT_FORMAT_VERSION}")),
        "v2 meta expected: {meta}"
    );
    let (restored, _) = read_immutable_segment(tmp.path(), 1).unwrap();

    // Red on HEAD: the reloaded buffer was always empty.
    assert_eq!(restored.sub_centroid_signs(), seg.sub_centroid_signs());
    // Same beam (32-level LUT) + same sidecar rerank ⇒ bit-identical results.
    let qs: Vec<Vec<f32>> = data.iter().step_by(37).cloned().collect();
    assert_eq!(search_all(&restored, &qs), search_all(&seg, &qs));
}

#[test]
fn v1_directory_without_sub_signs_still_loads_and_searches() {
    // Backward compat: a segment directory written before format v2 has no
    // sub_signs.bin and `"version": 1`. It must load exactly as before —
    // empty signs, 16-level search, sidecar rerank intact.
    let dim = 64;
    let col = collection(dim);
    let data = vecs(300, dim, 2);
    let seg = built_segment(&col, &data, 0);
    let tmp = tempfile::tempdir().unwrap();
    write_immutable_segment(tmp.path(), 3, &seg, &col).unwrap();
    let dir = tmp.path().join("segment-3");
    fs::remove_file(dir.join("sub_signs.bin")).unwrap();
    let meta = fs::read_to_string(dir.join("segment_meta.json")).unwrap();
    let v1 = meta.replace(
        &format!("\"version\": {SEGMENT_FORMAT_VERSION}"),
        "\"version\": 1",
    );
    assert_ne!(meta, v1, "downgrade helper must rewrite the version");
    fs::write(dir.join("segment_meta.json"), v1).unwrap();

    let (restored, _) = read_immutable_segment(tmp.path(), 3).unwrap();
    assert!(restored.sub_centroid_signs().is_empty());
    assert!(restored.raw_f16().is_some());
    let mut scratch = SearchScratch::new(0, col.padded_dimension);
    let hits = restored.search(&data[5], 5, 48, &mut scratch);
    assert_eq!(
        hits.first().map(|r| r.id.0),
        Some(5),
        "self-query must hit itself"
    );
}

#[test]
fn wrong_sized_sub_signs_file_is_ignored_not_trusted() {
    // The beam indexes the sign buffer without per-read bounds checks, so a
    // truncated / padded file must be dropped at load.
    let dim = 64;
    let col = collection(dim);
    let data = vecs(200, dim, 3);
    let seg = built_segment(&col, &data, 0);
    for delta in [-1i64, 1, -64] {
        let tmp = tempfile::tempdir().unwrap();
        write_immutable_segment(tmp.path(), 9, &seg, &col).unwrap();
        let path = tmp.path().join("segment-9/sub_signs.bin");
        let mut bytes = fs::read(&path).unwrap();
        let new_len = (bytes.len() as i64 + delta) as usize;
        bytes.resize(new_len, 0xFF);
        fs::write(&path, bytes).unwrap();
        let (restored, _) = read_immutable_segment(tmp.path(), 9).unwrap();
        assert!(restored.sub_centroid_signs().is_empty(), "delta={delta}");
        let mut scratch = SearchScratch::new(0, col.padded_dimension);
        assert!(!restored.search(&data[0], 3, 32, &mut scratch).is_empty());
    }
}

#[test]
fn graph_union_merge_recomputes_missing_signs_from_sidecar_instead_of_zero_fill() {
    let dim = 64;
    let col = collection(dim);
    let a = built_segment(&col, &vecs(200, dim, 4), 0);
    let b_data = vecs(200, dim, 5);
    let b = built_segment(&col, &b_data, 10_000);

    // `b` as a pre-v2 reload: same codes + sidecar, no signs.
    let tmp = tempfile::tempdir().unwrap();
    write_immutable_segment(tmp.path(), 2, &b, &col).unwrap();
    fs::remove_file(tmp.path().join("segment-2/sub_signs.bin")).unwrap();
    let (b_reloaded, _) = read_immutable_segment(tmp.path(), 2).unwrap();
    assert!(b_reloaded.sub_centroid_signs().is_empty());

    let merged = merge_immutable(
        &[Arc::new(a), Arc::new(b_reloaded)],
        &col,
        99,
        MergeMode::GraphUnion,
        0.0,
        None,
    )
    .expect("merge");
    let bpv = merged.sub_sign_bytes_per_vec();
    let signs = merged.sub_centroid_signs();
    assert_eq!(signs.len(), merged.mvcc_headers().len() * bpv);

    // Every row that came from `b` must match `b`'s ORIGINAL signs up to
    // f16-rounding flips — HEAD zero-filled these rows.
    let b_signs_by_key: std::collections::HashMap<u64, &[u8]> = b
        .mvcc_headers()
        .iter()
        .enumerate()
        .map(|(pos, h)| {
            (
                h.key_hash,
                &b.sub_centroid_signs()[pos * bpv..(pos + 1) * bpv],
            )
        })
        .collect();
    let mut rows = 0usize;
    let mut flips = 0u32;
    for (pos, h) in merged.mvcc_headers().iter().enumerate() {
        if let Some(orig) = b_signs_by_key.get(&h.key_hash) {
            let got = &signs[pos * bpv..(pos + 1) * bpv];
            assert!(got.iter().any(|&x| x != 0), "row zero-filled");
            flips += got
                .iter()
                .zip(orig.iter())
                .map(|(x, y)| (x ^ y).count_ones())
                .sum::<u32>();
            rows += 1;
        }
    }
    assert_eq!(rows, 200);
    assert!(
        flips as usize <= rows * 2,
        "{flips} sign flips over {rows} rows — beyond f16 rounding"
    );
}

#[test]
fn merge_without_signs_or_sidecar_drops_signs_instead_of_zero_filling() {
    let dim = 64;
    let col = collection(dim);
    let a = built_segment(&col, &vecs(150, dim, 6), 0);
    // A source with neither signs nor a sidecar (pre-sidecar reload shape).
    let b = built_segment(&col, &vecs(150, dim, 7), 50_000);
    let tmp = tempfile::tempdir().unwrap();
    write_immutable_segment(tmp.path(), 4, &b, &col).unwrap();
    fs::remove_file(tmp.path().join("segment-4/sub_signs.bin")).unwrap();
    fs::remove_file(tmp.path().join("segment-4/raw_f16.bin")).unwrap();
    let (b_bare, _) = read_immutable_segment(tmp.path(), 4).unwrap();
    let merged = merge_immutable(
        &[Arc::new(a), Arc::new(b_bare)],
        &col,
        98,
        MergeMode::GraphUnion,
        0.0,
        None,
    )
    .expect("merge");
    assert!(
        merged.sub_centroid_signs().is_empty(),
        "partial signs must not be zero-filled"
    );
}
