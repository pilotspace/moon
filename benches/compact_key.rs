//! Micro-benchmark: CompactKey inline (<=23B) vs heap (>23B) path performance.
//!
//! Measures creation throughput, as_bytes() access, hashing, equality comparison,
//! and cloning for both SSO-inline and heap-allocated key paths.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use rust_redis::storage::compact_key::CompactKey;

/// 10-byte key: fits inline (<=23 bytes).
fn make_inline_key(i: u64) -> CompactKey {
    let s = format!("key:{:06}", i); // "key:000000" = 10 bytes
    CompactKey::from(s.as_bytes())
}

/// 40-byte key: exceeds inline limit, heap-allocated.
fn make_heap_key(i: u64) -> CompactKey {
    let s = format!("long-key-prefix:{:025}", i); // 15 + 25 = 40 bytes
    CompactKey::from(s.as_bytes())
}

fn bench_compact_key(c: &mut Criterion) {
    // ─── Size verification ───
    assert_eq!(
        std::mem::size_of::<CompactKey>(),
        24,
        "CompactKey must be exactly 24 bytes"
    );
    // Bytes is 32 bytes in bytes 1.x; CompactKey saves 8 bytes per key vs Bytes.
    let bytes_size = std::mem::size_of::<bytes::Bytes>();
    assert!(
        bytes_size >= 24,
        "Bytes should be >= 24 bytes (actual: {bytes_size})"
    );

    // ─── 1. Inline path creation (10-byte keys) ───
    c.bench_function("compact_key/create_inline_10B", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let k = make_inline_key(i);
            i = i.wrapping_add(1);
            black_box(k);
        });
    });

    // ─── 2. Heap path creation (40-byte keys) ───
    c.bench_function("compact_key/create_heap_40B", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let k = make_heap_key(i);
            i = i.wrapping_add(1);
            black_box(k);
        });
    });

    // ─── 3. Mixed creation (50% inline / 50% heap) ───
    c.bench_function("compact_key/create_mixed_50_50", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let k = if i % 2 == 0 {
                make_inline_key(i)
            } else {
                make_heap_key(i)
            };
            i = i.wrapping_add(1);
            black_box(k);
        });
    });

    // ─── 4. Inline as_bytes() lookup ───
    let inline_keys: Vec<CompactKey> = (0..10_000).map(make_inline_key).collect();
    c.bench_function("compact_key/as_bytes_inline", |b| {
        b.iter(|| {
            for k in &inline_keys {
                black_box(k.as_bytes());
            }
        });
    });

    // ─── 5. Heap as_bytes() lookup ───
    let heap_keys: Vec<CompactKey> = (0..10_000).map(make_heap_key).collect();
    c.bench_function("compact_key/as_bytes_heap", |b| {
        b.iter(|| {
            for k in &heap_keys {
                black_box(k.as_bytes());
            }
        });
    });

    // ─── 6. Hash inline keys ───
    c.bench_function("compact_key/hash_inline", |b| {
        let key = make_inline_key(42);
        b.iter(|| {
            let mut h = DefaultHasher::new();
            black_box(&key).hash(&mut h);
            black_box(h.finish());
        });
    });

    // ─── 7. Hash heap keys ───
    c.bench_function("compact_key/hash_heap", |b| {
        let key = make_heap_key(42);
        b.iter(|| {
            let mut h = DefaultHasher::new();
            black_box(&key).hash(&mut h);
            black_box(h.finish());
        });
    });

    // ─── 8. Equality comparison (inline match) ───
    c.bench_function("compact_key/eq_inline_match", |b| {
        let a = make_inline_key(999);
        let a2 = make_inline_key(999);
        b.iter(|| {
            black_box(black_box(&a) == black_box(&a2));
        });
    });

    // ─── 9. Equality comparison (heap match) ───
    c.bench_function("compact_key/eq_heap_match", |b| {
        let a = make_heap_key(999);
        let a2 = make_heap_key(999);
        b.iter(|| {
            black_box(black_box(&a) == black_box(&a2));
        });
    });

    // ─── 10. Clone inline ───
    c.bench_function("compact_key/clone_inline", |b| {
        let key = make_inline_key(42);
        b.iter(|| {
            black_box(black_box(&key).clone());
        });
    });

    // ─── 11. Clone heap ───
    c.bench_function("compact_key/clone_heap", |b| {
        let key = make_heap_key(42);
        b.iter(|| {
            black_box(black_box(&key).clone());
        });
    });
}

criterion_group!(benches, bench_compact_key);
criterion_main!(benches);
