//! `DashTable` unit tests, moved verbatim out of `mod.rs` (moon#1226: the
//! module was 2,020 lines, over the 1,500-line rule). No test changed.

use super::segment::{LOAD_THRESHOLD, TOTAL_SLOTS};
use super::*;

/// moon#789: the deterministic gate for PERF-08 at the level
/// `Database::set` actually calls.
///
/// The segment-level twin of this test
/// (`segment::tests::test_insert_or_update_scans_fewer_groups_than_get_mut_plus_insert`)
/// pins `Segment::insert_or_update_at`. It would NOT notice
/// `DashTable::insert_or_update` being reimplemented as `get_mut` + `insert`
/// on top of a still-healthy segment helper — which is exactly the
/// regression PERF-08 exists to prevent. This one closes that gap.
///
/// Deterministic, arch-independent and load-independent, unlike the
/// wall-clock ratio in
/// `tests/perf_v0112_insert_or_update_single_probe.rs`.
#[test]
fn test_dashtable_insert_or_update_scans_fewer_groups_than_get_mut_plus_insert() {
    let key = b"probe_test_key".as_slice();

    // Legacy sequence on a miss: get_mut (a full probe) then insert (which
    // probes again to dedupe, then scans for a free slot).
    let mut legacy: DashTable<CompactKey, u32> = DashTable::new();
    let _ = segment::take_simd_probes();
    assert!(legacy.get_mut(key).is_none());
    legacy.insert(CompactKey::from(key), 7u32);
    let legacy_probes = segment::take_simd_probes();

    // Fused path on the same miss.
    let mut fused: DashTable<CompactKey, u32> = DashTable::new();
    let _ = segment::take_simd_probes();
    fused.insert_or_update(CompactKey::from(key), |v| *v = 7u32, || 7u32);
    let fused_probes = segment::take_simd_probes();

    // Both must actually have stored the key, or the counts describe two
    // different operations.
    assert_eq!(legacy.get(key), Some(&7u32));
    assert_eq!(fused.get(key), Some(&7u32));
    assert_eq!(legacy.len(), 1);
    assert_eq!(fused.len(), 1);

    assert!(
        fused_probes < legacy_probes,
        "DashTable::insert_or_update scanned {fused_probes} control-byte groups on a \
         miss; get_mut + insert scanned {legacy_probes}. The fused path must scan \
         strictly fewer — that is the whole of PERF-08. Equal or greater means the \
         single-probe fusion has regressed (moon#789)."
    );
}

/// moon#1159: the H2 fingerprint must carry bits the directory does not.
///
/// Every key in a segment of local depth `d` shares its top `d` hash bits
/// (that is what routes it there). Keys that share the top 10 bits are
/// therefore exactly the population of one depth-10 segment, and their
/// fingerprints are what `match_h2` has to tell apart. With H2 taken from
/// the top 7 bits they all carried ONE value; from bits 32..=38 they spread
/// over (almost) all 128.
#[test]
fn h2_fingerprint_is_independent_of_the_directory_bits() {
    const PREFIX_BITS: u32 = 10;
    const WANT: usize = 300;
    let target = hash_key(b"h2prefix:0") >> (64 - PREFIX_BITS);
    let mut fingerprints = std::collections::HashSet::new();
    let mut matched = 0usize;
    let mut i = 0u64;
    while matched < WANT {
        let k = format!("h2prefix:{i}");
        let hash = hash_key(k.as_bytes());
        if hash >> (64 - PREFIX_BITS) == target {
            fingerprints.insert(segment::h2(hash));
            matched += 1;
        }
        i += 1;
    }
    // 300 draws from 128 buckets leave ~116 distinct on average; 90 is a
    // generous floor that still fails loudly for a top-bit fingerprint
    // (which yields exactly 1).
    assert!(
        fingerprints.len() >= 90,
        "{WANT} keys sharing the top {PREFIX_BITS} hash bits (one depth-{PREFIX_BITS} \
         segment's worth) produced only {} distinct H2 fingerprints — H2 overlaps the \
         directory bits and the SIMD filter cannot separate keys inside a segment \
         (moon#1159)",
        fingerprints.len()
    );
}

/// moon#1159: the deterministic gate for the fingerprint's EFFECT — how
/// many full key compares a probe pays.
///
/// A 100K-key table sits at directory depth ~12, deep enough that a
/// fingerprint drawn from the directory bits is constant within every
/// segment. On HEAD `935c555` this measured ~6-8 compares per hit and
/// ~20+ per miss; a working 7-bit fingerprint needs ~1 per hit (the key
/// itself plus a 1/128 false-positive rate over the ~10 FULL slots ahead
/// of it) and ~0.2 per miss.
#[test]
fn h2_fingerprint_keeps_key_compares_near_one_per_hit() {
    const N: u32 = 100_000;
    let mut table: DashTable<CompactKey, u32> = DashTable::new();
    for i in 0..N {
        table.insert(CompactKey::from(format!("key:{i:08}")), i);
    }
    assert!(
        table.directory_depth() >= 10,
        "fixture must be deep enough for a top-bit fingerprint to collapse, got depth {}",
        table.directory_depth()
    );

    let _ = segment::take_key_compares();
    for i in 0..N {
        assert_eq!(table.get(format!("key:{i:08}").as_bytes()), Some(&i));
    }
    let hit_compares = segment::take_key_compares();
    for i in 0..N {
        assert!(table.get(format!("miss:{i:08}").as_bytes()).is_none());
    }
    let miss_compares = segment::take_key_compares();

    let per_hit = hit_compares as f64 / f64::from(N);
    let per_miss = miss_compares as f64 / f64::from(N);
    eprintln!("moon#1159 key compares: per_hit={per_hit:.4} per_miss={per_miss:.4}");
    assert!(
        per_hit <= 1.15,
        "mean key compares per HIT = {per_hit:.3} on a {N}-key table (want ~1.05); \
         the H2 fingerprint is not filtering (moon#1159)"
    );
    assert!(
        per_miss <= 0.5,
        "mean key compares per MISS = {per_miss:.3} on a {N}-key table (want ~0.2); \
         the H2 fingerprint is not filtering (moon#1159)"
    );
}

fn test_value(n: u32) -> String {
    format!("value_{}", n)
}

/// Structural cost per stored key, pinned so a layout change cannot silently
/// inflate RSS on a 100M-key dataset.
///
/// A `Segment<CompactKey, CompactEntry>` is the unit of allocation: one
/// cache line of control bytes, 8 bytes of metadata, then `TOTAL_SLOTS`
/// key slots and `TOTAL_SLOTS` value slots. Segments live in slab `Vec`s,
/// so there is no per-segment allocator rounding — the slab pays it once.
///
/// Divide by the achieved fill to get bytes/key:
///   * split at `LOAD_THRESHOLD` (54) leaves a segment holding 27..54 keys,
///     so the population averages ~40.5 -> ~85 bytes/key;
///   * `with_capacity` deliberately over-allocates one depth level, halving
///     the fill to ~27 keys/segment -> ~128 bytes/key.
///
/// Redis 7.0.15 for comparison, per key: `dictEntry` 24 B in jemalloc's
/// 32-byte class, plus ~12 B of `dictEntry*` bucket array (the table doubles
/// at load factor 1.0, so buckets/key averages ~1.5), plus a separately
/// allocated key `sds` — which moon does not pay at all for keys <= 23 bytes
/// because `CompactKey` inlines them into the slot counted here.
#[test]
fn segment_structural_cost_per_key_is_pinned() {
    use crate::storage::entry::CompactEntry;

    let seg = std::mem::size_of::<Segment<CompactKey, CompactEntry>>();

    assert_eq!(std::mem::size_of::<CompactKey>(), 24);
    assert_eq!(std::mem::size_of::<CompactEntry>(), 32);
    assert_eq!(TOTAL_SLOTS, 60);
    assert_eq!(LOAD_THRESHOLD, 54);

    // 64 (ctrl) + 8 (count/depth) + 1 (has_non_home_keys) + padding
    // + 60*24 (keys) + 60*32 (values), rounded to the 64-byte alignment.
    assert_eq!(
        seg, 3456,
        "Segment layout changed; recompute the per-key memory ledger"
    );

    // Bytes/key at the three fills that actually occur. A split halves a
    // segment, so live segments hold LOAD_THRESHOLD/2 .. LOAD_THRESHOLD
    // keys and the population mean is 3/4 of LOAD_THRESHOLD = 40.5.
    assert_eq!(
        seg / LOAD_THRESHOLD,
        64,
        "best case, at the split threshold"
    );
    assert_eq!(
        seg * 4 / (LOAD_THRESHOLD * 3),
        85,
        "organic fill, ~40.5 keys/segment"
    );
    assert_eq!(
        seg * 2 / LOAD_THRESHOLD,
        128,
        "--initial-keyspace-hint: with_capacity adds a depth level, halving fill"
    );
}

/// moon#1159 follow-up: an upsert keyed by a slice stores an owned key
/// ONLY when the key is new. `Database::set` used to build a
/// `CompactKey` per call, i.e. a heap block per overwrite of any key
/// longer than 23 bytes, dropped again as soon as the probe hit.
#[test]
fn insert_or_update_slice_builds_the_key_only_on_a_miss() {
    let long: &[u8] = b"a-key-that-is-definitely-longer-than-23-bytes";
    let mut t: DashTable<CompactKey, u32> = DashTable::new();
    let _ = take_upsert_key_builds();
    match t.insert_or_update_slice(long, |_| panic!("fresh key updated"), || 1) {
        InsertOrUpdate::Inserted(v) => assert_eq!(*v, 1),
        InsertOrUpdate::Updated(_) => panic!("fresh key reported Updated"),
    }
    assert_eq!(take_upsert_key_builds(), 1, "a miss stores exactly one key");
    for i in 0..100u32 {
        match t.insert_or_update_slice(long, |v| *v += 1, || unreachable!("hit ran make")) {
            InsertOrUpdate::Updated(v) => assert_eq!(*v, i + 2),
            InsertOrUpdate::Inserted(_) => panic!("existing key reported Inserted"),
        }
    }
    assert_eq!(
        take_upsert_key_builds(),
        0,
        "an overwrite built (and dropped) an owned key"
    );
    assert_eq!(t.get(long), Some(&101));
    assert_eq!(t.len(), 1);
}

/// Both upsert entry points, interleaved, across thousands of splits:
/// every key lands once, every overwrite hits, `len` stays exact.
#[test]
fn upsert_slice_and_owned_agree_across_splits() {
    let mut t: DashTable<CompactKey, u64> = DashTable::new();
    for i in 0..20_000u64 {
        let k = format!("upsert-agree-key-with-some-length-{i}");
        if i % 2 == 0 {
            t.insert_or_update_slice(k.as_bytes(), |_| panic!("fresh"), || i);
        } else {
            t.insert_or_update(CompactKey::from(k.as_bytes()), |_| panic!("fresh"), || i);
        }
    }
    assert_eq!(t.len(), 20_000);
    for i in 0..20_000u64 {
        let k = format!("upsert-agree-key-with-some-length-{i}");
        let hit = if i % 2 == 1 {
            t.insert_or_update_slice(k.as_bytes(), |v| *v += 1, || unreachable!())
        } else {
            t.insert_or_update(
                CompactKey::from(k.as_bytes()),
                |v| *v += 1,
                || unreachable!(),
            )
        };
        assert!(matches!(hit, InsertOrUpdate::Updated(v) if *v == i + 1));
    }
    assert_eq!(t.len(), 20_000);
    for i in 0..20_000u64 {
        let k = format!("upsert-agree-key-with-some-length-{i}");
        assert_eq!(t.get(k.as_bytes()), Some(&(i + 1)));
    }
}

#[test]
fn test_insert_or_update_survives_skewed_double_split() {
    // Keys sharing the top 12 hash bits route to the same segment for
    // every split up to depth 12, so one split cannot separate them.
    // Filling a segment past LOAD_THRESHOLD with such keys makes the
    // post-split retry inside insert_or_update hit NeedsSplit again —
    // the old code declared that unreachable and panicked (reproduced
    // live replaying a 219k-key WAL checkpoint on recovery).
    const SKEW_BITS: u32 = 12;
    let want = LOAD_THRESHOLD + 2;
    let target = hash_key(b"skew_0") >> (64 - SKEW_BITS);
    let mut keys = Vec::with_capacity(want);
    let mut i = 0u64;
    while keys.len() < want {
        let k = format!("skew_{i}");
        if hash_key(k.as_bytes()) >> (64 - SKEW_BITS) == target {
            keys.push(k);
        }
        i += 1;
    }

    let mut table: DashTable<CompactKey, String> = DashTable::new();
    for (n, k) in keys.iter().enumerate() {
        let mut updated = false;
        table.insert_or_update(
            CompactKey::from(k.clone()),
            |_| updated = true,
            || format!("v{n}"),
        );
        assert!(!updated, "unexpected in-place update for fresh key {k}");
    }

    assert_eq!(table.len(), want);
    for (n, k) in keys.iter().enumerate() {
        assert_eq!(
            table.get(k.as_bytes()),
            Some(&format!("v{n}")),
            "missing {k} after skewed splits"
        );
    }
}

#[test]
fn hash_page_empty_table_is_terminal() {
    let table: DashTable<CompactKey, String> = DashTable::new();
    let (page, more) = table.hash_page(0, 16, |_, _| true);
    assert!(page.is_empty());
    assert!(!more);
}

#[test]
fn hash_page_alive_filter_and_more_flag() {
    let mut table: DashTable<CompactKey, String> = DashTable::new();
    for i in 0..2000u32 {
        table.insert(CompactKey::from(format!("af_{i}")), test_value(i));
    }
    // Filter out every odd value: only evens may appear.
    let (page, _) = table.hash_page(0, 200, |_, v| {
        let n: u32 = v.trim_start_matches("value_").parse().unwrap();
        n % 2 == 0
    });
    assert!(!page.is_empty());
    for (_, k) in &page {
        let n: u32 = std::str::from_utf8(k.as_ref())
            .unwrap()
            .trim_start_matches("af_")
            .parse()
            .unwrap();
        assert_eq!(n % 2, 0, "alive filter leaked odd key {n}");
    }
    // A want larger than the table drains everything in one page.
    let (all, more) = table.hash_page(0, usize::MAX, |_, _| true);
    assert_eq!(all.len(), 2000);
    assert!(!more, "full drain must report no further segments");
}

#[test]
fn hash_page_drains_in_hash_order_under_split_churn() {
    // 4000 keys force many segment splits + directory doublings; paging
    // with concurrent inserts between pages exercises the split-safety
    // claim (cursor is a hash-space position, not a structure position).
    let mut table: DashTable<CompactKey, String> = DashTable::new();
    let mut original: Vec<String> = Vec::with_capacity(4000);
    for i in 0..4000u32 {
        let k = format!("hp_{i}");
        table.insert(CompactKey::from(k.clone()), test_value(i));
        original.push(k);
    }

    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut cursor = 0u64;
    let mut churn = 0u32;
    loop {
        let (page, more) = table.hash_page(cursor, 64, |_, _| true);
        let mut prev: Option<(u64, &[u8])> = None;
        for (h, k) in &page {
            assert!(*h >= cursor, "entry below cursor");
            assert_eq!(*h, hash_key(k.as_ref()), "stale hash in page");
            if let Some((ph, pk)) = prev {
                assert!(
                    (ph, pk) < (*h, k.as_ref()),
                    "page not ascending by (hash, key)"
                );
            }
            prev = Some((*h, k.as_ref()));
        }
        if page.is_empty() {
            assert!(!more, "empty page must be terminal");
            break;
        }
        for (_, k) in &page {
            assert!(
                seen.insert(k.as_ref().to_vec()),
                "duplicate key across pages: {:?}",
                String::from_utf8_lossy(k.as_ref())
            );
        }
        #[allow(clippy::unwrap_used)] // page verified non-empty above
        let last = page.last().unwrap().0;
        cursor = last + 1;
        if !more {
            break;
        }
        // Structural churn between pages: force splits mid-walk.
        for _ in 0..50 {
            table.insert(
                CompactKey::from(format!("churn_{churn}")),
                test_value(churn),
            );
            churn += 1;
        }
    }

    for k in &original {
        assert!(
            seen.contains(k.as_bytes()),
            "stable key {k} lost during split churn"
        );
    }
}

#[test]
fn test_new_empty() {
    let table: DashTable<CompactKey, String> = DashTable::new();
    assert_eq!(table.len(), 0);
    assert!(table.is_empty());
    assert_eq!(table.get(b"anything"), None);
}

#[test]
fn test_insert_and_get() {
    let mut table: DashTable<CompactKey, String> = DashTable::new();

    for i in 0..10 {
        let key = CompactKey::from(format!("key_{}", i));
        let val = test_value(i);
        assert_eq!(table.insert(key, val), None);
    }

    assert_eq!(table.len(), 10);

    for i in 0..10 {
        let key = format!("key_{}", i);
        let val = table.get(key.as_bytes());
        assert_eq!(val, Some(&test_value(i)), "Missing key_{}", i);
    }
}

#[test]
fn test_insert_replace() {
    let mut table: DashTable<CompactKey, String> = DashTable::new();
    let key = CompactKey::from("mykey");

    assert_eq!(table.insert(key.clone(), "first".into()), None);
    assert_eq!(table.len(), 1);

    let old = table.insert(key.clone(), "second".into());
    assert_eq!(old, Some("first".into()));
    assert_eq!(table.len(), 1);

    assert_eq!(table.get(b"mykey"), Some(&"second".to_string()));
}

#[test]
fn test_remove() {
    let mut table: DashTable<CompactKey, String> = DashTable::new();
    let key = CompactKey::from("remove_me");
    table.insert(key.clone(), "value".into());
    assert_eq!(table.len(), 1);

    let removed = table.remove(b"remove_me");
    assert_eq!(removed, Some("value".to_string()));
    assert_eq!(table.len(), 0);
    assert_eq!(table.get(b"remove_me"), None);

    assert_eq!(table.remove(b"remove_me"), None);
}

#[test]
fn test_contains_key() {
    let mut table: DashTable<CompactKey, String> = DashTable::new();
    let key = CompactKey::from("exists");
    table.insert(key.clone(), "yes".into());
    assert!(table.contains_key(b"exists"));
    assert!(!table.contains_key(b"nope"));

    table.remove(b"exists");
    assert!(!table.contains_key(b"exists"));
}

#[test]
fn test_keys_iter() {
    let mut table: DashTable<CompactKey, String> = DashTable::new();
    let mut expected_keys: Vec<String> = Vec::new();

    for i in 0..5 {
        let key = CompactKey::from(format!("k{}", i));
        expected_keys.push(format!("k{}", i));
        table.insert(key, test_value(i));
    }

    let mut actual_keys: Vec<String> = table
        .keys()
        .map(|k| String::from_utf8_lossy(k.as_bytes()).to_string())
        .collect();
    actual_keys.sort();
    expected_keys.sort();
    assert_eq!(actual_keys, expected_keys);
}

#[test]
fn test_iter() {
    let mut table: DashTable<CompactKey, String> = DashTable::new();
    for i in 0..8 {
        table.insert(CompactKey::from(format!("iter_{}", i)), test_value(i));
    }

    let count = table.iter().count();
    assert_eq!(count, 8);
    assert_eq!(count, table.len());
}

#[test]
fn test_iter_mut() {
    let mut table: DashTable<CompactKey, String> = DashTable::new();
    for i in 0..5 {
        table.insert(CompactKey::from(format!("mut_{}", i)), test_value(i));
    }

    for (_k, v) in table.iter_mut() {
        *v = format!("modified_{}", v);
    }

    for i in 0..5 {
        let key = format!("mut_{}", i);
        let val = table.get(key.as_bytes()).unwrap();
        assert!(val.starts_with("modified_"), "Value not modified: {}", val);
    }
}

#[test]
fn test_large_insert_triggers_split() {
    let mut table: DashTable<CompactKey, String> = DashTable::new();

    for i in 0..100 {
        let key = CompactKey::from(format!("large_{:04}", i));
        table.insert(key, test_value(i));
    }

    assert_eq!(table.len(), 100);

    for i in 0..100 {
        let key = format!("large_{:04}", i);
        let val = table.get(key.as_bytes());
        assert_eq!(val, Some(&test_value(i)), "Missing large_{:04}", i);
    }

    // Should have more than 1 segment after splits
    assert!(table.segment_count() > 1, "Expected splits to occur");
}

#[test]
fn test_1000_entries() {
    let mut table: DashTable<CompactKey, String> = DashTable::new();

    for i in 0..1000 {
        let key = CompactKey::from(format!("stress_{:06}", i));
        table.insert(key, test_value(i));
    }

    assert_eq!(table.len(), 1000);

    for i in 0..1000 {
        let key = format!("stress_{:06}", i);
        let val = table.get(key.as_bytes());
        assert_eq!(val, Some(&test_value(i)), "Missing stress_{:06}", i);
    }

    for i in 0..500 {
        let key = format!("stress_{:06}", i);
        let removed = table.remove(key.as_bytes());
        assert!(removed.is_some(), "Failed to remove stress_{:06}", i);
    }

    assert_eq!(table.len(), 500);

    for i in 500..1000 {
        let key = format!("stress_{:06}", i);
        let val = table.get(key.as_bytes());
        assert_eq!(
            val,
            Some(&test_value(i)),
            "Missing stress_{:06} after removes",
            i
        );
    }

    for i in 0..500 {
        let key = format!("stress_{:06}", i);
        assert_eq!(
            table.get(key.as_bytes()),
            None,
            "stress_{:06} should be removed",
            i
        );
    }
}

#[test]
fn test_memory_overhead() {
    // Verify structural overhead per entry is <= 16 bytes.
    // Segment overhead: 64 bytes ctrl + 8 bytes metadata = 72 bytes for 60 slots.
    // Per slot: 72 / 60 = 1.2 bytes. Well under 16.
    let ctrl_bytes = 64usize;
    let meta_bytes = 8usize; // count(4) + depth(4)
    let per_slot = (ctrl_bytes + meta_bytes) as f64 / TOTAL_SLOTS as f64;
    assert!(
        per_slot <= 16.0,
        "Per-slot overhead {:.1} exceeds 16 bytes",
        per_slot
    );
}

#[test]
fn test_with_capacity() {
    let table: DashTable<CompactKey, String> = DashTable::with_capacity(1000);
    assert_eq!(table.len(), 0);
    assert!(table.is_empty());
}

/// An EMPTY table must not pre-pay for segments it does not have.
///
/// moon boots `--databases 16` tables **per shard**, all empty, so every
/// reserved-but-unoccupied segment slot is multiplied by `16 x shards`
/// before a single key exists. At `size_of::<Segment<CompactKey,
/// CompactEntry>>() == 3,456 B`, the historical 16-slot first slab cost
/// 55,296 B per database — 93.75% of it for segments that would never
/// exist on an idle server — i.e. ~874 KB per shard.
#[test]
fn empty_table_does_not_over_reserve_segment_slots() {
    let table: DashTable<CompactKey, String> = DashTable::new();
    assert_eq!(table.segment_count(), 1, "an empty table holds one segment");
    assert!(
        table.reserved_segment_slots() <= 2,
        "an empty DashTable reserved {} segment slots for {} live segment(s); \
         every spare slot is a full Segment of dead weight, charged 16x per shard",
        table.reserved_segment_slots(),
        table.segment_count()
    );
}

/// Pre-sizing must reserve for the segments it actually creates, not round
/// up to a fixed slab size. `with_capacity(100)` needs 4 segments; a
/// 16-slot first slab reserves four times that.
#[test]
fn presized_table_does_not_over_reserve_segment_slots() {
    let table: DashTable<CompactKey, String> = DashTable::with_capacity(100);
    let live = table.segment_count();
    assert!(live > 0, "pre-sizing must allocate at least one segment");
    assert!(
        table.reserved_segment_slots() <= live * 2,
        "with_capacity(100) reserved {} segment slots for {live} live segments",
        table.reserved_segment_slots()
    );
}

/// Growth must stay pointer-stable: a slab is never reallocated, so a new
/// slab is pushed whenever the last one is full. Shrinking the first slab
/// must not break that — walk a table well past several slab boundaries
/// and confirm every key is still findable.
#[test]
fn growth_across_slab_boundaries_keeps_every_key() {
    let mut table: DashTable<CompactKey, u64> = DashTable::new();
    for i in 0..20_000u64 {
        table.insert(CompactKey::from(format!("growth:key:{i}")), i);
    }
    assert_eq!(table.len(), 20_000);
    assert!(
        table.segment_count() > 16,
        "the fixture must cross several slab boundaries, got {} segments",
        table.segment_count()
    );
    for i in 0..20_000u64 {
        let key = format!("growth:key:{i}");
        assert_eq!(
            table.get(key.as_bytes()),
            Some(&i),
            "key {i} lost across slab growth"
        );
    }
    assert!(
        table.reserved_segment_slots() < table.segment_count() * 2,
        "growth reserved {} slots for {} segments",
        table.reserved_segment_slots(),
        table.segment_count()
    );
}

#[test]
fn test_split_count_starts_at_zero() {
    let table: DashTable<CompactKey, String> = DashTable::new();
    assert_eq!(table.split_count(), 0);
}

#[test]
fn test_split_count_with_capacity_starts_at_zero() {
    // Pre-sized allocation must NOT count as splits.
    let table: DashTable<CompactKey, String> = DashTable::with_capacity(1_000_000);
    assert_eq!(table.split_count(), 0);
    assert!(
        table.segment_count() > 1,
        "with_capacity must allocate >1 segment for 1M hint"
    );
}

#[test]
fn test_split_count_grows_under_load_without_capacity() {
    let mut table: DashTable<CompactKey, String> = DashTable::new();
    for i in 0..2000 {
        table.insert(
            CompactKey::from(format!("split_count_{:06}", i)),
            format!("v_{}", i),
        );
    }
    assert!(
        table.split_count() > 0,
        "Expected splits after 2000 inserts on a default-sized table; got {}",
        table.split_count()
    );
}

#[test]
fn test_iter_empty() {
    let table: DashTable<CompactKey, String> = DashTable::new();
    assert_eq!(table.iter().count(), 0);
}

#[test]
fn test_iter_count_matches_len() {
    let mut table: DashTable<CompactKey, String> = DashTable::new();
    for i in 0..50 {
        table.insert(CompactKey::from(format!("cnt_{}", i)), test_value(i));
    }
    assert_eq!(table.iter().count(), table.len());
}

#[test]
fn test_iter_after_removes() {
    let mut table: DashTable<CompactKey, String> = DashTable::new();
    for i in 0..20 {
        table.insert(CompactKey::from(format!("rem_{}", i)), test_value(i));
    }
    for i in 0..10 {
        table.remove(format!("rem_{}", i).as_bytes());
    }
    assert_eq!(table.len(), 10);
    assert_eq!(table.iter().count(), 10);
}

#[test]
fn test_values_iter() {
    let mut table: DashTable<CompactKey, String> = DashTable::new();
    for i in 0..5 {
        table.insert(CompactKey::from(format!("v_{}", i)), test_value(i));
    }
    assert_eq!(table.values().count(), 5);
}

#[test]
fn test_directory_doubling() {
    // Insert enough to force multiple splits and directory doublings
    let mut table: DashTable<CompactKey, String> = DashTable::new();
    for i in 0..200 {
        table.insert(CompactKey::from(format!("dd_{:06}", i)), test_value(i));
    }

    // Directory should have grown
    assert!(table.directory.len() > 1);
    assert_eq!(table.len(), 200);

    // All entries retrievable
    for i in 0..200 {
        assert!(
            table.get(format!("dd_{:06}", i).as_bytes()).is_some(),
            "Missing dd_{:06}",
            i
        );
    }
}

#[test]
fn test_segment_iter_occupied() {
    use segment::Segment;

    let mut seg: Segment<CompactKey, String> = Segment::new(0);
    // Insert 5 entries using the segment's insert method
    for i in 0..5 {
        let key = CompactKey::from(format!("seg_key_{}", i));
        let val = format!("seg_val_{}", i);
        let hash = hash_key(key.as_ref());
        let h2_val = segment::h2(hash);
        let (ba, bb) = segment::home_buckets(hash);
        seg.insert(h2_val, key, val, ba, bb);
    }

    let occupied: Vec<_> = seg.iter_occupied().collect();
    assert_eq!(
        occupied.len(),
        5,
        "iter_occupied should yield exactly 5 pairs"
    );

    // Verify all keys are present
    let keys: Vec<String> = occupied
        .iter()
        .map(|(k, _)| String::from_utf8_lossy(k.as_bytes()).to_string())
        .collect();
    for i in 0..5 {
        let expected_key = format!("seg_key_{}", i);
        assert!(
            keys.contains(&expected_key),
            "Missing key: {}",
            expected_key
        );
    }
}

#[test]
fn test_segment_count_grows_after_split() {
    let mut table: DashTable<CompactKey, String> = DashTable::new();
    assert_eq!(table.segment_count(), 1);

    for i in 0..100 {
        table.insert(CompactKey::from(format!("sc_{:04}", i)), test_value(i));
    }

    assert!(
        table.segment_count() > 1,
        "segment_count should grow after splits, got {}",
        table.segment_count()
    );
}

#[test]
fn test_segment_index_for_hash_matches_get() {
    let mut table: DashTable<CompactKey, String> = DashTable::new();
    for i in 0..50 {
        table.insert(CompactKey::from(format!("si_{:04}", i)), test_value(i));
    }

    // For each key, verify segment_index_for_hash points to the segment containing it
    for i in 0..50 {
        let key = format!("si_{:04}", i);
        let hash = hash_key(key.as_bytes());
        let seg_idx = table.segment_index_for_hash(hash);
        let seg = table.segment(seg_idx);

        // The segment should contain this key in its iter_occupied
        let found = seg
            .iter_occupied()
            .any(|(k, _)| k.as_ref() == key.as_bytes());
        assert!(
            found,
            "Key {} not found in segment {} (segment_count={})",
            key,
            seg_idx,
            table.segment_count()
        );
    }
}

/// Regression test: insert followed by get_mut must always succeed.
///
/// This verifies the fix for the "overflow slot" bug where insert's
/// last-resort linear scan could place a key in a group that find()
/// didn't check (only group_a, group_b, and stash were searched).
#[test]
fn test_insert_then_get_mut_always_finds() {
    let mut table: DashTable<CompactKey, String> = DashTable::new();

    for i in 0..2000 {
        let key = CompactKey::from(format!("regress_{:06}", i));
        let val = test_value(i);
        table.insert(key, val);

        // Immediately verify the key is findable
        let lookup_key = format!("regress_{:06}", i);
        assert!(
            table.get_mut(lookup_key.as_bytes()).is_some(),
            "get_mut returned None immediately after insert for regress_{:06} (table len={})",
            i,
            table.len()
        );
    }

    // Verify all keys are still accessible
    for i in 0..2000 {
        let key = format!("regress_{:06}", i);
        assert!(
            table.get(key.as_bytes()).is_some(),
            "get returned None for regress_{:06}",
            i,
        );
    }
}

/// Every directory entry routing to a segment `S` must form ONE aligned
/// block of `2^(depth - S.depth)` consecutive slots. This is the
/// extendible-hashing invariant `split_segment`'s O(block) repoint (G1/L4)
/// relies on, checked here from first principles — without reference to
/// either the old scan or the new arithmetic.
fn assert_directory_block_invariant<V>(table: &DashTable<CompactKey, V>) {
    let depth = table.depth;
    assert_eq!(
        table.directory.len(),
        1usize << depth,
        "directory length is 2^depth"
    );
    // (first slot, last slot, slot count) per segment store index.
    let mut spans: std::collections::HashMap<usize, (usize, usize, usize)> =
        std::collections::HashMap::new();
    for (i, &seg) in table.directory.iter().enumerate() {
        let e = spans.entry(seg).or_insert((i, i, 0));
        e.1 = i;
        e.2 += 1;
    }
    assert_eq!(
        spans.len(),
        table.segment_count(),
        "every stored segment must be reachable from the directory"
    );
    for (&seg, &(first, last, count)) in &spans {
        let local = table.segments.get(seg).depth();
        assert!(
            local <= depth,
            "segment {seg}: local depth {local} > global depth {depth}"
        );
        let span = 1usize << (depth - local);
        assert_eq!(
            count, span,
            "segment {seg} (depth {local}): {count} slots, expected {span}"
        );
        assert_eq!(
            last - first + 1,
            span,
            "segment {seg}: slots {first}..={last} are not contiguous for span {span}"
        );
        assert_eq!(
            first % span,
            0,
            "segment {seg}: block start {first} not aligned to {span}"
        );
    }
}

/// 16-byte keys shaped like redis-benchmark's `key:__rand_int__`, the
/// same generator as `examples/dashtable_growth.rs`.
fn growth_key(x: &mut u64) -> CompactKey {
    *x = x
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let mut key = *b"key:000000000000";
    let mut id = (*x >> 24) % 1_000_000_000_000u64;
    for d in (4..16).rev() {
        key[d] = b'0' + (id % 10) as u8;
        id /= 10;
    }
    CompactKey::from(&key[..])
}

/// G1/L4 differential guard: across a 200K-key fill through the
/// production entry point (`insert_or_update`), the directory after EVERY
/// split must be byte-identical to what the pre-L4 full scan
/// (`repoint_by_full_scan`) would have produced. The comparison itself
/// runs inside `split_segment` under `cfg(test)`; this test proves it ran
/// once per split (`DIFFERENTIAL_CHECKS`), that the aligned-block
/// invariant holds at checkpoints, and that no key was lost.
///
/// Proven able to fail: dropping the `<< doublings` from `block_start`
/// panics on the first split that doubles the directory (see
/// tmp/perf-campaign/WAVE1-L4-L3A.md for the captured output).
#[test]
fn split_directory_repoint_matches_full_scan_across_200k_fill() {
    const N: u64 = 200_000;
    let checks_before = DIFFERENTIAL_CHECKS.with(|c| c.get());
    let mut table: DashTable<CompactKey, u64> = DashTable::new();
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut keys = Vec::with_capacity(N as usize);
    for i in 0..N {
        let key = growth_key(&mut x);
        keys.push(key.clone());
        let _ = table.insert_or_update(key, |v| *v = i, || i);
        if (i + 1) % 20_000 == 0 {
            assert_directory_block_invariant(&table);
        }
    }
    assert_directory_block_invariant(&table);
    let splits = table.split_count();
    assert!(
        splits >= 1_000,
        "fixture must split thousands of times, got {splits}"
    );
    assert_eq!(
        table.segment_count() as u64,
        splits + 1,
        "one new segment per split"
    );
    assert!(
        table.directory_depth() >= 10,
        "directory must have doubled repeatedly"
    );
    assert_eq!(
        DIFFERENTIAL_CHECKS.with(|c| c.get()) - checks_before,
        splits,
        "the full-scan comparison must have run on every split"
    );
    // Duplicate keys from the generator collapse into updates: len is the
    // distinct count, and every key must still route to its value.
    let mut distinct = std::collections::HashSet::new();
    let mut last_value = std::collections::HashMap::new();
    for (i, key) in keys.iter().enumerate() {
        distinct.insert(key.as_ref().to_vec());
        last_value.insert(key.as_ref().to_vec(), i as u64);
    }
    assert_eq!(table.len(), distinct.len());
    for (key, value) in &last_value {
        assert_eq!(table.get(key), Some(value), "key {key:?} lost or misrouted");
    }
}

/// The `with_capacity` path starts every segment at local depth ==
/// global depth, so the FIRST split of any segment doubles the directory
/// (`doublings == 1`) — the case where `block_start` must map `dir_idx`
/// through the doubling. Overfill a presized table and check the
/// invariant plus the per-split differential.
#[test]
fn split_after_presize_doubles_directory_and_keeps_block_invariant() {
    let checks_before = DIFFERENTIAL_CHECKS.with(|c| c.get());
    let mut table: DashTable<CompactKey, u64> = DashTable::with_capacity(10_000);
    let depth0 = table.directory_depth();
    assert_eq!(table.split_count(), 0);
    let mut x: u64 = 0x1234_5678_9ABC_DEF0;
    for i in 0..60_000u64 {
        let _ = table.insert_or_update(growth_key(&mut x), |v| *v = i, || i);
    }
    assert!(
        table.split_count() > 0,
        "overfilling a presized table must split"
    );
    assert!(
        table.directory_depth() > depth0,
        "the first split past presize doubles"
    );
    assert_directory_block_invariant(&table);
    assert_eq!(
        DIFFERENTIAL_CHECKS.with(|c| c.get()) - checks_before,
        table.split_count()
    );
}

/// `repoint_by_full_scan` is the oracle; pin its own semantics on a
/// hand-built directory so a broken oracle cannot silently agree with a
/// broken repoint. depth 3, segment 5 owns slots 4..8 (local depth 1),
/// splitting to local depth 2: slots 6 and 7 move.
#[test]
fn full_scan_oracle_moves_only_the_upper_half_of_the_block() {
    let dir = vec![0, 0, 1, 1, 5, 5, 5, 5];
    let out = repoint_by_full_scan(&dir, 3, 2, 5, 9);
    assert_eq!(out, vec![0, 0, 1, 1, 5, 5, 9, 9]);
}
