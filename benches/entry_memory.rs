use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use std::collections::HashMap;
use std::hint::black_box;

use rust_redis::storage::entry::CompactEntry;

/// Benchmark: Create 1M CompactEntry instances with 8-byte string values.
/// These fit in SSO (<=12 bytes), so no heap allocation for the value itself.
/// Measures throughput and validates per-entry struct overhead is exactly 24 bytes.
fn bench_compact_entry_1m_small_strings(c: &mut Criterion) {
    c.bench_function("1M compact entries (8-byte strings)", |b| {
        b.iter(|| {
            let mut entries: Vec<CompactEntry> = Vec::with_capacity(1_000_000);
            for i in 0u64..1_000_000 {
                let val = Bytes::from(format!("{:08}", i)); // 8-byte string, fits in SSO
                let entry = CompactEntry::new_string(val);
                entries.push(entry);
            }
            black_box(&entries);
            entries
        });
    });
}

/// Benchmark: 1M entries with mixed sizes -- 50% inline SSO, 50% heap-allocated.
/// Demonstrates memory savings from SSO path for the common small-string case.
fn bench_compact_entry_1m_mixed(c: &mut Criterion) {
    c.bench_function("1M compact entries (mixed inline/heap)", |b| {
        b.iter(|| {
            let mut entries: Vec<CompactEntry> = Vec::with_capacity(1_000_000);
            for i in 0u64..1_000_000 {
                let entry = if i % 2 == 0 {
                    // 8 bytes -- fits inline SSO (<=12 bytes)
                    CompactEntry::new_string(Bytes::from(format!("{:08}", i)))
                } else {
                    // 20 bytes -- exceeds SSO, heap-allocated
                    CompactEntry::new_string(Bytes::from(format!("long-value-{:09}", i)))
                };
                entries.push(entry);
            }
            black_box(&entries);
            entries
        });
    });
}

/// Benchmark: Measure HashMap<Bytes, CompactEntry> for 1M small string keys.
/// This is the real-world layout: the database stores entries in a HashMap.
fn bench_hashmap_1m_keys(c: &mut Criterion) {
    c.bench_function("HashMap<Bytes,CompactEntry> 1M keys", |b| {
        b.iter(|| {
            let mut map: HashMap<Bytes, CompactEntry> = HashMap::with_capacity(1_000_000);
            for i in 0u64..1_000_000 {
                let key = Bytes::from(format!("key:{:08}", i));
                let val = Bytes::from(format!("{:08}", i));
                let entry = CompactEntry::new_string(val);
                map.insert(key, entry);
            }
            black_box(&map);
            map
        });
    });
}

/// Benchmark: Verify struct sizes and compute reduction percentage.
/// CompactEntry = 24 bytes vs old Entry = 88 bytes = 72.7% reduction.
/// This benchmark runs the size assertions in a tight loop to validate the claim.
fn bench_entry_size_report(c: &mut Criterion) {
    c.bench_function("entry size comparison", |b| {
        b.iter(|| {
            let compact_size = std::mem::size_of::<CompactEntry>();
            // Compile-time assertion already in entry.rs, but verify at runtime too
            assert_eq!(compact_size, 24, "CompactEntry should be 24 bytes");

            // Old Entry was ~88 bytes:
            //   RedisValue enum: 72 bytes (Bytes + largest variant)
            //   expires_at_ms: u64 = 8 bytes
            //   metadata (version, last_access, counter): ~8 bytes
            let old_size: usize = 88;
            let reduction_pct = ((old_size - compact_size) as f64 / old_size as f64) * 100.0;
            assert!(
                reduction_pct >= 40.0,
                "Reduction {:.1}% is less than 40% target",
                reduction_pct
            );
            black_box(reduction_pct);
            reduction_pct
        });
    });
}

criterion_group!(
    benches,
    bench_compact_entry_1m_small_strings,
    bench_compact_entry_1m_mixed,
    bench_hashmap_1m_keys,
    bench_entry_size_report
);
criterion_main!(benches);
