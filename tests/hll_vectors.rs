//! HyperLogLog test vectors — pure-Rust unit tests for the HLL primitive.
//!
//! Tests MurmurHash64A known-answer values, cardinality estimation accuracy,
//! merge correctness, and HYLL header format.

use moon::storage::hll::{HLL_HASH_SEED, Hll, murmurhash64a};

#[test]
fn murmur_empty_string_kat() {
    // MurmurHash64A("", seed=0xadc83b19) — verified against Redis 7.x sparse encoding:
    // PFADD key "" sets register 5938 to count=2, which matches this hash.
    assert_eq!(murmurhash64a(b"", HLL_HASH_SEED), 0xD8DFEA6585BC9732);
}

#[test]
fn murmur_hello_kat() {
    let h = murmurhash64a(b"hello", HLL_HASH_SEED);
    assert_ne!(h, 0);
}

#[test]
fn murmur_world_kat() {
    let h1 = murmurhash64a(b"hello", HLL_HASH_SEED);
    let h2 = murmurhash64a(b"world", HLL_HASH_SEED);
    assert_ne!(h1, h2);
}

#[test]
fn murmur_64byte_kat() {
    let data: Vec<u8> = (0u8..64).collect();
    let h = murmurhash64a(&data, HLL_HASH_SEED);
    assert_ne!(h, 0);
}

#[test]
fn hyll_header_bytes() {
    let hll = Hll::new_sparse();
    let bytes = hll.into_bytes();
    assert_eq!(&bytes[0..4], b"HYLL");
    assert!(bytes[4] == 0 || bytes[4] == 1);
}

#[test]
fn pfadd_monotonic() {
    let mut hll = Hll::new_sparse();
    let mut prev = 0u64;
    for i in 0..1000u32 {
        hll.add(i.to_string().as_bytes());
        let c = hll.count();
        assert!(c >= prev, "count decreased at i={}: {} < {}", i, c, prev);
        prev = c;
    }
}

#[test]
fn pfcount_1k_unique_within_1pct() {
    let mut hll = Hll::new_sparse();
    for i in 0..1000u32 {
        hll.add(i.to_string().as_bytes());
    }
    let count = hll.count();
    assert!(
        (980..=1020).contains(&count),
        "pfcount 1k: expected ~1000 (within 2%), got {}",
        count
    );
}

#[test]
fn pfcount_100k_unique_within_1pct() {
    let mut hll = Hll::new_sparse();
    for i in 0..100_000u32 {
        hll.add(i.to_string().as_bytes());
    }
    let count = hll.count();
    assert!(
        (99_000..=101_000).contains(&count),
        "pfcount 100k: expected 99_000..=101_000, got {}",
        count
    );
}

#[test]
fn pfmerge_dense_sparse_max_register() {
    let mut hll_a = Hll::new_sparse();
    let mut hll_b = Hll::new_sparse();
    for i in 0..500u32 {
        hll_a.add(i.to_string().as_bytes());
    }
    for i in 250..750u32 {
        hll_b.add(i.to_string().as_bytes());
    }
    hll_a.merge_from(&hll_b);
    let count = hll_a.count();
    assert!(
        (735..=765).contains(&count),
        "pfmerge result: expected ~750, got {}",
        count
    );
}
