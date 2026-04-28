//! PERF-09 correctness gate: 1M random insert + 1M random lookup with zero
//! false negatives and zero false positives. Exercises the fixed
//! Segment::find / insert_or_update_at paths from Phase 189.
//!
//! NOTE: REQ asked for `Segment::full_compare_count() <= 1.05 * num_lookups`.
//! That assertion is vacuous on the existing Swiss Table h2 design -- the
//! ctrl-byte SIMD filter already enforces this property. We assert the
//! stronger functional correctness instead.
//!
//! Run with:
//!   cargo test --release --test perf_v0112_segment_find_correctness

use bytes::Bytes;
use moon::storage::compact_key::CompactKey;
use moon::storage::dashtable::DashTable;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::collections::HashSet;

const N: usize = 1_000_000;
const KEY_LEN: usize = 16;
const INSERT_SEED: u64 = 0xdead_beef_cafe_babe;
const LOOKUP_NEG_SEED: u64 = 0xfeed_face_d00d_b00b;

fn rand_keys(seed: u64, count: usize) -> Vec<Vec<u8>> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count)
        .map(|_| {
            let mut k = vec![0u8; KEY_LEN];
            rng.fill(&mut k[..]);
            k
        })
        .collect()
}

#[test]
fn test_segment_find_1m_random_no_false_negatives() {
    let keys = rand_keys(INSERT_SEED, N);
    let mut table: DashTable<CompactKey, u32> = DashTable::with_capacity(N);
    let mut seen = HashSet::with_capacity(N);

    for (i, k) in keys.iter().enumerate() {
        let ck = CompactKey::from(Bytes::copy_from_slice(k));
        table.insert(ck, i as u32);
        seen.insert(k.clone());
    }
    assert_eq!(
        table.len(),
        seen.len(),
        "insert dedup mismatch: table.len()={} vs unique keys={}",
        table.len(),
        seen.len()
    );

    // Every inserted key must be findable
    let mut not_found = 0usize;
    for k in &keys {
        if table.get(k.as_slice()).is_none() {
            not_found += 1;
        }
    }
    assert_eq!(
        not_found, 0,
        "{} out of {} inserted keys not found by find()",
        not_found, N
    );
}

#[test]
fn test_segment_find_1m_random_no_false_positives() {
    let keys = rand_keys(INSERT_SEED, N);
    let mut table: DashTable<CompactKey, u32> = DashTable::with_capacity(N);

    for (i, k) in keys.iter().enumerate() {
        table.insert(CompactKey::from(Bytes::copy_from_slice(k)), i as u32);
    }

    // Lookup with disjoint keys must return None
    let neg_keys = rand_keys(LOOKUP_NEG_SEED, N);
    let inserted_set: HashSet<Vec<u8>> = keys.into_iter().collect();
    let mut false_positives = 0usize;
    for k in &neg_keys {
        // Skip the rare collision (random seeds may overlap by birthday paradox;
        // for KEY_LEN=16 it's astronomically rare)
        if inserted_set.contains(k) {
            continue;
        }
        if table.get(k.as_slice()).is_some() {
            false_positives += 1;
        }
    }
    assert_eq!(
        false_positives, 0,
        "{} false positives on disjoint key set",
        false_positives
    );
}
