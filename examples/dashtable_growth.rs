//! DashTable growth micro-bench (G1 lever 4): the cost of `insert_or_update`
//! as a function of table size, isolated from the server.
//!
//! Prints one CSV row per 100K-insert window: ns/insert in that window, the
//! number of `split_segment` calls it triggered, and the segment count. If a
//! per-split cost scales with the directory (the pre-L4 O(2^depth) scan did),
//! ns/insert in the growth phase rises with the table while
//! `splits_in_window` stays flat; a pre-sized table (`argv[2] = capacity`) is
//! the control that carries the same cache-miss growth with zero splits.
//!
//! ```text
//! cargo build --profile=release-with-debug --example dashtable_growth
//! target/release-with-debug/examples/dashtable_growth 2000000 0        # growth
//! target/release-with-debug/examples/dashtable_growth 2000000 3000000  # presized control
//! ```
//!
//! Numbers only count from a Linux host (CLAUDE.md); the reference run is in
//! tmp/perf-campaign/G1-ROOT-CAUSE.md §6.4.
use std::time::Instant;

use moon::storage::compact_key::CompactKey;
use moon::storage::dashtable::DashTable;

fn main() {
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000);
    let presize: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let window: usize = 100_000;
    let mut t: DashTable<CompactKey, u64> = if presize == 0 {
        DashTable::new()
    } else {
        DashTable::with_capacity(presize)
    };
    // 16-byte keys, the same shape as redis-benchmark's `key:__rand_int__`.
    let mut key = *b"key:000000000000";
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut prev_splits = t.split_count();
    println!("window,inserted,ns_per_insert,splits_in_window,segments,split_count");
    let total = Instant::now();
    let mut wstart = Instant::now();
    for i in 0..n {
        x = x
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let mut id = (x >> 24) % 1_000_000_000_000u64;
        for d in (4..16).rev() {
            key[d] = b'0' + (id % 10) as u8;
            id /= 10;
        }
        let _ = t.insert_or_update(CompactKey::from(&key[..]), |v: &mut u64| *v += 1, || 1u64);
        if (i + 1) % window == 0 {
            let el = wstart.elapsed().as_nanos() as f64 / window as f64;
            let sc = t.split_count();
            println!(
                "{},{},{:.1},{},{},{}",
                (i + 1) / window,
                i + 1,
                el,
                sc - prev_splits,
                t.segment_count(),
                sc
            );
            prev_splits = sc;
            wstart = Instant::now();
        }
    }
    println!(
        "total_ms={} len={} segments={} splits={}",
        total.elapsed().as_millis(),
        t.len(),
        t.segment_count(),
        t.split_count()
    );
}
