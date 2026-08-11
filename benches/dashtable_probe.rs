//! DashTable point-lookup probe benchmark at cache-exceeding table sizes.
//!
//! The existing `get_hotpath` bench uses 10K keys — the whole table sits in
//! L2 and never exercises the miss-latency path that dominates production
//! (probe symbols = 18.65% of cycles on ARM / 11.09% on x86 at shards=1,
//! real-PMU GCE measurement, tmp/GCE-PMU-RESULTS.md). This bench builds
//! 100K- and 1M-key tables (the latter far exceeds L2 and typical L3
//! slices) and looks keys up in a fixed-seed shuffled order so the hardware
//! stride prefetcher cannot learn the walk.
//!
//! This harness rejected the O1 value-line-prefetch experiment (see the
//! NOTE in `segment/mod.rs`): the hit path MUST force a read through the
//! returned reference, or prefetch "wins" are artifacts — `black_box`
//! does not load through a `&V`, it only opaques the pointer.

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

use moon::storage::compact_key::CompactKey;
use moon::storage::dashtable::DashTable;
use moon::storage::entry::CompactEntry;

/// Fisher-Yates with a fixed-seed xorshift so runs are reproducible.
fn shuffled_indices(n: usize) -> Vec<usize> {
    let mut order: Vec<usize> = (0..n).collect();
    let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
    for i in (1..n).rev() {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        order.swap(i, (s as usize) % (i + 1));
    }
    order
}

fn bench_probe(c: &mut Criterion) {
    for &n in &[100_000usize, 1_000_000] {
        let keys: Vec<Bytes> = (0..n)
            .map(|i| Bytes::from(format!("key:{i:010}")))
            .collect();
        let miss_keys: Vec<Bytes> = (0..n)
            .map(|i| Bytes::from(format!("nay:{i:010}")))
            .collect();

        let mut table: DashTable<CompactKey, CompactEntry> = DashTable::new();
        for k in &keys {
            table.insert(
                CompactKey::from(k.clone()),
                CompactEntry::new_string(Bytes::from_static(b"0123456789abcdef")),
            );
        }

        let order = shuffled_indices(n);

        let mut group = c.benchmark_group("dashtable_probe");
        group.throughput(Throughput::Elements(1));

        let mut idx = 0usize;
        group.bench_function(BenchmarkId::new("get_hit", n), |b| {
            b.iter(|| {
                idx += 1;
                if idx == n {
                    idx = 0;
                }
                // `.map(|e| e.version())` forces a real load through the
                // returned reference — `get` alone returns a pointer into
                // `values[slot]` without dereferencing it, and `black_box`
                // does NOT force a read through a reference (disassembly-
                // verified: no load emitted). Every real caller (GET et al.)
                // reads the entry after lookup; the value-line prefetch can
                // only be measured if the harness does too.
                black_box(
                    table
                        .get(black_box(keys[order[idx]].as_ref()))
                        .map(|e| e.version()),
                )
            })
        });

        let mut midx = 0usize;
        group.bench_function(BenchmarkId::new("get_miss", n), |b| {
            b.iter(|| {
                midx += 1;
                if midx == n {
                    midx = 0;
                }
                black_box(table.get(black_box(miss_keys[order[midx]].as_ref())))
            })
        });

        group.finish();
    }
}

criterion_group!(benches, bench_probe);
criterion_main!(benches);
