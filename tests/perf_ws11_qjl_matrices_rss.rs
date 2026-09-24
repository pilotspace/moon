//! moon#1213 item 2: an EXACT collection must not hold its QJL matrices.
//!
//! HEAD materialized `8 · d² · 4` bytes of dense Gaussian QJL matrices in every
//! EXACT `CollectionMetadata` — 18.9 MB at 768d — once per index per shard
//! (FT.CREATE runs on every shard) and once more per reloaded EXACT segment
//! (`segment_io::read_immutable_segment` rebuilds a collection per segment).
//! After moon#1213 item 1 nothing reads them; only `metadata_checksum` hashes
//! them, and that is now streamed.
//!
//! Method: process `VmRSS` across (a) four `VectorStore`s — one per shard —
//! each creating two EXACT 768d indexes, and (b) reloading six persisted EXACT
//! 768d segments. No custom global allocator (would need `unsafe`); the HEAD
//! effect is 150+ MB so page granularity is noise. Linux-only
//! (`/proc/self/status`); one `#[test]` so no concurrent test perturbs RSS.

#![cfg(target_os = "linux")]

use std::sync::Arc;

use bytes::Bytes;
use moon::vector::persistence::segment_io::{read_immutable_segment, write_immutable_segment};
use moon::vector::segment::compaction::compact;
use moon::vector::segment::mutable::MutableSegment;
use moon::vector::store::{IndexMeta, VectorStore};
use moon::vector::turbo_quant::collection::{BuildMode, CollectionMetadata, QuantizationConfig};
use moon::vector::turbo_quant::encoder::padded_dimension;
use moon::vector::types::DistanceMetric;

const MB: f64 = 1024.0 * 1024.0;

fn rss() -> isize {
    let status = std::fs::read_to_string("/proc/self/status").expect("/proc/self/status");
    status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<isize>().ok())
        .expect("VmRSS")
        * 1024
}

fn exact_meta(name: &str, dim: u32) -> IndexMeta {
    IndexMeta {
        name: Bytes::copy_from_slice(name.as_bytes()),
        dimension: dim,
        padded_dimension: padded_dimension(dim),
        metric: DistanceMetric::Cosine,
        hnsw_m: 16,
        hnsw_ef_construction: 200,
        hnsw_ef_runtime: 0,
        compact_threshold: 1000,
        source_field: Bytes::from_static(b"v"),
        key_prefixes: vec![Bytes::copy_from_slice(format!("{name}:").as_bytes())],
        quantization: QuantizationConfig::TurboQuant4,
        build_mode: BuildMode::Exact,
        vector_fields: Vec::new(),
        schema_fields: Vec::new(),
        merge_mode: Default::default(),
        keep_raw: false,
        db_index: 0,
        rerank_mult: 4,
        exact_beam: false,
    }
}

#[test]
fn exact_collections_do_not_hold_qjl_matrices() {
    moon::vector::distance::init();
    let dim = 768u32;
    let per_matrix_set = 8.0 * (dim as f64) * (dim as f64) * 4.0; // 18.9 MB

    // (a) FT.CREATE on 4 shards, 2 EXACT indexes each.
    let before = rss();
    let mut shards: Vec<VectorStore> = (0..4).map(|_| VectorStore::new()).collect();
    for store in &mut shards {
        for name in ["ea", "eb"] {
            store.create_index(exact_meta(name, dim)).expect("create");
        }
    }
    let created = (rss() - before) as f64;
    eprintln!(
        "8 EXACT 768d indexes (4 shards x 2): RSS +{:.1} MB (HEAD holds {:.1} MB of QJL matrices)",
        created / MB,
        8.0 * per_matrix_set / MB
    );

    // (b) Six reloaded EXACT segments, each rebuilding its own collection.
    let col = Arc::new(CollectionMetadata::with_build_mode(
        7,
        dim,
        DistanceMetric::Cosine,
        QuantizationConfig::TurboQuant4,
        7,
        BuildMode::Exact,
    ));
    let tmp = tempfile::tempdir().expect("tempdir");
    for id in 0..6u64 {
        let seg = MutableSegment::new(dim, col.clone());
        for i in 0..40u64 {
            let v: Vec<f32> = (0..dim)
                .map(|j| (((i * 131 + j as u64 * 7 + id * 17) % 97) as f32) / 97.0 - 0.5)
                .collect();
            seg.append(id * 1000 + i, &v, i + 1);
        }
        let imm = compact(&seg.freeze(), &col, 11 + id, None).expect("compact");
        write_immutable_segment(tmp.path(), id, &imm, &col).expect("persist");
    }
    let before_reload = rss();
    let reloaded: Vec<_> = (0..6u64)
        .map(|id| read_immutable_segment(tmp.path(), id).expect("reload"))
        .collect();
    let reload = (rss() - before_reload) as f64;
    eprintln!(
        "6 reloaded EXACT 768d segments (40 vectors each): RSS +{:.1} MB (HEAD: +{:.1} MB of matrices)",
        reload / MB,
        6.0 * per_matrix_set / MB
    );

    // HEAD: ≥ 151 MB and ≥ 113 MB. Allow a few MB for index scaffolding.
    assert!(
        created < 12.0 * MB,
        "FT.CREATE x8 grew RSS by {:.1} MB",
        created / MB
    );
    assert!(
        reload < 12.0 * MB,
        "6 segment reloads grew RSS by {:.1} MB",
        reload / MB
    );
    // Keep everything alive until after the measurements.
    assert_eq!(reloaded.len(), 6);
    assert!(shards.iter().all(|s| s.index_names().len() == 2));
}
