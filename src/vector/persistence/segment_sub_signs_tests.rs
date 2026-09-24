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
use crate::vector::turbo_quant::collection::{BuildMode, CollectionMetadata, QuantizationConfig};
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

#[test]
fn persisted_compaction_serves_the_f16_sidecar_from_the_mapped_file() {
    // moon#1194 red test: compaction wrote raw_f16.bin and returned the
    // segment still heap-`Owned` (2*dim B/vector); only a restart mapped it.
    let dim = 96;
    let col = collection(dim);
    let data = vecs(300, dim, 11);
    let seg = MutableSegment::new(col.dimension, col.clone());
    for (i, v) in data.iter().enumerate() {
        seg.append(i as u64, v, i as u64 + 1);
    }
    let frozen = seg.freeze();
    let heap = compact(&frozen, &col, 4242, None).expect("compact");
    let tmp = tempfile::tempdir().unwrap();
    let persisted = compact(&frozen, &col, 4242, Some((tmp.path(), 12))).expect("compact+persist");
    assert!(!heap.raw_f16_is_mapped());
    assert!(
        persisted.raw_f16_is_mapped(),
        "sidecar must be mapped after persist"
    );
    assert_eq!(
        heap.resident_bytes() - persisted.resident_bytes(),
        300 * dim * 2,
        "exactly the sidecar leaves the heap"
    );
    assert_eq!(persisted.raw_f16(), heap.raw_f16());
    let qs: Vec<Vec<f32>> = data.iter().step_by(29).cloned().collect();
    assert_eq!(search_all(&persisted, &qs), search_all(&heap, &qs));
}

// ── moon#1221 review: only REAL signs are ever built, persisted or loaded ──
//
// The PR head persisted every sign buffer of the right length, so all-zero
// PLACEHOLDER buffers became `sub_signs.bin` as if real: SQ8 (never reads
// signs; reloaded n·padded/8 dead heap bytes where HEAD reloaded none) and
// every non-TQ4 LIGHT build (zero-filled at insert, then searched with the
// 32-level LUT and all-zero signs — the bias moon#1193 itself calls a bug —
// where HEAD's reload used the 16-level LUT). An EMPTY buffer is the one
// representation of "no signs": it selects the 16-level LUT in memory and
// after a reload alike.

fn collection_of(dim: usize, q: QuantizationConfig, mode: BuildMode) -> Arc<CollectionMetadata> {
    distance::init();
    crate::vector::turbo_quant::fwht::init_fwht();
    // seed == collection_id, as `VectorStore` creates collections: EXACT
    // reloads regenerate the QJL matrices from the id and the checksum
    // covers them.
    Arc::new(CollectionMetadata::with_build_mode(
        31,
        dim as u32,
        DistanceMetric::L2,
        q,
        31,
        mode,
    ))
}

/// Insert `data` into a mutable segment and compact it with persistence.
/// Returns the built segment, the bytes of `sub_signs.bin` (None when the
/// file was not written) and the segment reloaded from disk.
fn compact_persist_reload(
    col: &Arc<CollectionMetadata>,
    data: &[Vec<f32>],
    root: &std::path::Path,
    segment_id: u64,
) -> (ImmutableSegment, Option<Vec<u8>>, ImmutableSegment) {
    let seg = MutableSegment::new(col.dimension, col.clone());
    for (i, v) in data.iter().enumerate() {
        seg.append(i as u64 + 1, v, i as u64 + 1);
    }
    let built = compact(&seg.freeze(), col, 4242, Some((root, segment_id))).expect("compact");
    let file = fs::read(root.join(format!("segment-{segment_id}/sub_signs.bin"))).ok();
    let (reloaded, _) = read_immutable_segment(root, segment_id).expect("reload");
    (built, file, reloaded)
}

#[test]
fn sq8_segments_build_persist_and_reload_no_sub_signs() {
    // Red on the PR head: n·padded/8 zero bytes built, written to
    // sub_signs.bin and reloaded onto the heap for a quantizer that never
    // reads them (HEAD reloaded an empty buffer).
    for mode in [BuildMode::Light, BuildMode::Exact] {
        let col = collection_of(96, QuantizationConfig::Sq8, mode);
        let data = vecs(200, 96, 21);
        let tmp = tempfile::tempdir().unwrap();
        let (built, file, reloaded) = compact_persist_reload(&col, &data, tmp.path(), 1);
        assert!(built.sub_centroid_signs().is_empty(), "{mode:?}: built");
        assert_eq!(
            file.map(|f| f.len()),
            None,
            "{mode:?}: sub_signs.bin written"
        );
        assert!(
            reloaded.sub_centroid_signs().is_empty(),
            "{mode:?}: reloaded"
        );
        let mut scratch = SearchScratch::new(0, col.padded_dimension);
        assert_eq!(
            reloaded
                .search(&data[7], 1, 32, &mut scratch)
                .first()
                .map(|r| r.id.0),
            Some(7)
        );
    }
}

#[test]
fn non_tq4_light_segments_carry_no_placeholder_signs() {
    // TQ4A2 and TurboQuantProd4 have no insert-time sign encoder: the PR head
    // zero-filled their rows at insert, compacted the zeros, searched them
    // with the 32-level LUT and persisted them. Now: no signs anywhere, the
    // 16-level LUT before AND after a restart, identical results. The A2
    // dims include padded 8 and 32, whose HOT segments now take the budgeted
    // 16-level kernel fixed in the previous commit.
    for (q, dim) in [
        (QuantizationConfig::TurboQuant4A2, 6usize),
        (QuantizationConfig::TurboQuant4A2, 20),
        (QuantizationConfig::TurboQuant4A2, 96),
        (QuantizationConfig::TurboQuantProd4, 96),
    ] {
        let col = collection_of(dim, q, BuildMode::Light);
        let data = vecs(240, dim, 22);
        let tmp = tempfile::tempdir().unwrap();
        let (built, file, reloaded) = compact_persist_reload(&col, &data, tmp.path(), 2);
        assert!(built.sub_centroid_signs().is_empty(), "{q:?}/{dim}: built");
        assert_eq!(file.map(|f| f.len()), None, "{q:?}/{dim}: file written");
        assert!(reloaded.sub_centroid_signs().is_empty(), "{q:?}/{dim}");
        let qs: Vec<Vec<f32>> = data.iter().step_by(31).cloned().collect();
        let before = search_all(&built, &qs);
        assert!(before.iter().all(|r| !r.is_empty()));
        assert_eq!(search_all(&reloaded, &qs), before, "{q:?}/{dim}");
    }
}

#[test]
fn real_signs_persist_byte_identical() {
    // Real signs — TQ4 insert-time (LIGHT) and encoder-computed from raw f32
    // (EXACT, TQ4 and TQ4A2) — are written and reloaded byte for byte.
    for (q, mode) in [
        (QuantizationConfig::TurboQuant4, BuildMode::Light),
        (QuantizationConfig::TurboQuant4, BuildMode::Exact),
        (QuantizationConfig::TurboQuant4A2, BuildMode::Exact),
    ] {
        let col = collection_of(64, q, mode);
        let data = vecs(200, 64, 23);
        let tmp = tempfile::tempdir().unwrap();
        let (built, file, reloaded) = compact_persist_reload(&col, &data, tmp.path(), 3);
        let signs = built.sub_centroid_signs();
        assert_eq!(
            signs.len(),
            200 * built.sub_sign_bytes_per_vec(),
            "{q:?}/{mode:?}"
        );
        assert!(signs.iter().any(|&b| b != 0), "{q:?}/{mode:?}: placeholder");
        assert_eq!(file.as_deref(), Some(signs), "{q:?}/{mode:?}: file");
        assert_eq!(reloaded.sub_centroid_signs(), signs, "{q:?}/{mode:?}");
    }
}

#[test]
fn placeholder_sub_signs_files_are_ignored_at_load() {
    // A v2 directory written by the PR head before this fix can hold a
    // right-sized all-zero sub_signs.bin (SQ8, non-TQ4 LIGHT). Loading it
    // would put a zero placeholder on the 32-level path; it is ignored.
    // Red on the PR head: both reloads returned the zeros.
    let tmp = tempfile::tempdir().unwrap();
    for (id, q) in [
        (4u64, QuantizationConfig::Sq8),
        (5, QuantizationConfig::TurboQuant4),
        (6, QuantizationConfig::TurboQuant4A2),
    ] {
        let col = collection_of(64, q, BuildMode::Light);
        let data = vecs(120, 64, 24);
        let (built, _, _) = compact_persist_reload(&col, &data, tmp.path(), id);
        let bpv = built.sub_sign_bytes_per_vec();
        let path = tmp.path().join(format!("segment-{id}/sub_signs.bin"));
        fs::write(&path, vec![0u8; 120 * bpv]).unwrap();
        let (reloaded, _) = read_immutable_segment(tmp.path(), id).unwrap();
        assert!(reloaded.sub_centroid_signs().is_empty(), "{q:?}");
        assert!(path.exists(), "the reader never deletes data");
    }
}

/// Interleave `append` and `append_transactional` (every third row).
fn mixed_appends(col: &Arc<CollectionMetadata>, data: &[Vec<f32>]) -> MutableSegment {
    let seg = MutableSegment::new(col.dimension, col.clone());
    for (i, v) in data.iter().enumerate() {
        if i % 3 == 1 {
            seg.append_transactional(i as u64, v, i as u64 + 1, 9);
        } else {
            seg.append(i as u64, v, i as u64 + 1);
        }
    }
    seg
}

#[test]
fn mutable_segments_hold_no_sign_placeholder_for_non_tq4() {
    // Red on the PR head: SQ8 / TQ4A2 inserts zero-filled a sign row each,
    // and freeze handed the zeros to compaction.
    let dim = 64;
    let data = vecs(40, dim, 25);
    for q in [QuantizationConfig::Sq8, QuantizationConfig::TurboQuant4A2] {
        let col = collection_of(dim, q, BuildMode::Light);
        let seg = mixed_appends(&col, &data);
        assert!(seg.freeze().sub_centroid_signs.is_empty(), "{q:?}");
        // An empty buffer with `start > 0` must not panic the install path.
        let tail = seg.clone_suffix(7);
        assert_eq!(tail.len(), 33);
        assert!(tail.freeze().sub_centroid_signs.is_empty(), "{q:?}");
    }
}

#[test]
fn tq4_sign_rows_stay_aligned_across_both_append_paths() {
    // Red on the PR head: `append_transactional` pushed no sign row, so a
    // TQ4 segment mixing both paths held fewer rows than entries and
    // `freeze` panicked slicing `[..n·bpv]` (or, with the rows present,
    // entry k would have read another entry's signs).
    use crate::vector::turbo_quant::encoder::encode_tq_mse_scaled_with_signs;
    let dim = 64;
    let data = vecs(40, dim, 25);
    let col = collection_of(dim, QuantizationConfig::TurboQuant4, BuildMode::Light);
    let seg = mixed_appends(&col, &data);
    let mut work = vec![0.0f32; col.padded_dimension as usize];
    let expected: Vec<Vec<u8>> = data
        .iter()
        .map(|v| {
            encode_tq_mse_scaled_with_signs(
                v,
                col.fwht_sign_flips.as_slice(),
                col.codebook_boundaries_15(),
                col.codebook_16(),
                &mut work,
            )
            .signs
        })
        .collect();
    let frozen = seg.freeze();
    let bpv = frozen.sub_sign_bytes_per_vec;
    assert_eq!(frozen.sub_centroid_signs.len(), data.len() * bpv);
    for (i, row) in frozen.sub_centroid_signs.chunks_exact(bpv).enumerate() {
        assert_eq!(row, expected[i].as_slice(), "row {i} misaligned");
    }
    let tail = seg.clone_suffix(7).freeze();
    assert_eq!(
        tail.sub_centroid_signs.as_slice(),
        &frozen.sub_centroid_signs[7 * bpv..]
    );
}

#[test]
fn incomplete_insert_signs_are_dropped_at_compaction_not_zero_filled() {
    // A LIGHT TQ4 frozen segment whose sign buffer misses a row must not
    // ship a partially zero-filled buffer (red on the PR head: the missing
    // row stayed zero and the rest were used on the 32-level path).
    let dim = 64;
    let col = collection(dim);
    let seg = MutableSegment::new(col.dimension, col.clone());
    for (i, v) in vecs(120, dim, 26).iter().enumerate() {
        seg.append(i as u64, v, i as u64 + 1);
    }
    let mut frozen = seg.freeze();
    let bpv = frozen.sub_sign_bytes_per_vec;
    let full = frozen.sub_centroid_signs.len();
    frozen.sub_centroid_signs.truncate(full - bpv);
    let built = compact(&frozen, &col, 4242, None).expect("compact");
    assert!(built.sub_centroid_signs().is_empty());
}

#[test]
fn sq8_merge_carries_no_sign_buffer() {
    // Red on the PR head: `is_sq8 || all_have_signs` kept SQ8's zero buffer.
    let dim = 64;
    let col = collection_of(dim, QuantizationConfig::Sq8, BuildMode::Light);
    let build = |seed: u64, base: u64| {
        let seg = MutableSegment::new(col.dimension, col.clone());
        for (i, v) in vecs(120, dim, seed).iter().enumerate() {
            seg.append(base + i as u64, v, base + i as u64 + 1);
        }
        Arc::new(compact(&seg.freeze(), &col, 4242, None).expect("compact"))
    };
    let merged = merge_immutable(
        &[build(27, 0), build(28, 10_000)],
        &col,
        97,
        MergeMode::GraphUnion,
        0.0,
        None,
    )
    .expect("merge");
    assert_eq!(merged.mvcc_headers().len(), 240);
    assert!(merged.sub_centroid_signs().is_empty());
}
