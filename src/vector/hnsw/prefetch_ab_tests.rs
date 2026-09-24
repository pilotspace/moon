//! moon#1213 item 5: interleaved in-binary A/B of the beam's candidate
//! prefetch — HEAD's pattern (2 own-neighbour lines + 3 code lines) against
//! the whole code row + sign row — on compacted TQ4 segments (32-level LUT,
//! the production beam) over an embedding-shaped corpus.
//!
//! `#[ignore]`: a measurement, meaningful only in an optimized build:
//! `cargo test --profile release-fast --lib prefetch_ab -- --ignored --nocapture`.
//! Both arms run the same binary and the same queries, alternating per rep,
//! so drift on a shared box hits both.

use std::sync::Arc;
use std::time::Instant;

use crate::vector::distance;
use crate::vector::hnsw::graph::PREFETCH_LEGACY;
use crate::vector::hnsw::search::SearchScratch;
use crate::vector::segment::compaction::compact;
use crate::vector::segment::immutable::ImmutableSegment;
use crate::vector::segment::mutable::MutableSegment;
use crate::vector::test_support::EmbeddingLike;
use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
use crate::vector::types::DistanceMetric;

fn segment(dim: usize, n: usize) -> (ImmutableSegment, Vec<Vec<f32>>) {
    distance::init();
    crate::vector::turbo_quant::fwht::init_fwht();
    let col = Arc::new(CollectionMetadata::new(
        31,
        dim as u32,
        DistanceMetric::Cosine,
        QuantizationConfig::TurboQuant4,
        31,
    ));
    let mut g = EmbeddingLike::new(dim, 256, 17);
    let seg = MutableSegment::new(col.dimension, col.clone());
    for (i, v) in g.docs(n).iter().enumerate() {
        seg.append(i as u64, v, i as u64 + 1);
    }
    let imm = compact(&seg.freeze(), &col, 5, None).expect("compact");
    let queries = g.docs(400);
    (imm, queries)
}

/// ns per query for one arm.
fn run(seg: &ImmutableSegment, qs: &[Vec<f32>], legacy: bool, ef: usize) -> (f64, u64) {
    PREFETCH_LEGACY.with(|c| c.set(legacy));
    let mut scratch = SearchScratch::new(0, seg.collection_meta().padded_dimension);
    let mut check = 0u64;
    let t = Instant::now();
    for q in qs {
        for r in seg.search(q, 10, ef, &mut scratch) {
            check = check.wrapping_mul(31).wrapping_add(r.id.0 as u64);
        }
    }
    let ns = t.elapsed().as_nanos() as f64 / qs.len() as f64;
    PREFETCH_LEGACY.with(|c| c.set(false));
    (ns, check)
}

#[test]
#[ignore = "measurement: run in release-fast with --ignored --nocapture"]
fn prefetch_ab() {
    for (dim, n) in [(768usize, 40_000usize), (384, 40_000)] {
        let (seg, qs) = segment(dim, n);
        assert!(
            !seg.sub_centroid_signs().is_empty(),
            "32-level beam expected"
        );
        for ef in [64usize, 200] {
            // Warm both arms, then 7 alternating reps.
            let _ = run(&seg, &qs, true, ef);
            let _ = run(&seg, &qs, false, ef);
            let (mut old, mut new) = (Vec::new(), Vec::new());
            for rep in 0..7 {
                let order = if rep % 2 == 0 {
                    [true, false]
                } else {
                    [false, true]
                };
                let mut checks = [0u64; 2];
                for legacy in order {
                    let (ns, c) = run(&seg, &qs, legacy, ef);
                    checks[usize::from(legacy)] = c;
                    if legacy { old.push(ns) } else { new.push(ns) }
                }
                assert_eq!(checks[0], checks[1], "prefetch must not change results");
            }
            old.sort_by(f64::total_cmp);
            new.sort_by(f64::total_cmp);
            eprintln!(
                "PREFETCH {dim}d n={n} ef={ef}: HEAD median {:.0} ns/q (min {:.0}) | whole-row median {:.0} ns/q (min {:.0}) | new/old {:.3}  raw old={old:.0?} new={new:.0?}",
                old[3],
                old[0],
                new[3],
                new[0],
                new[3] / old[3]
            );
        }
    }
}
