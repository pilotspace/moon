//! `ColdIndex` unit tests (moved out of `cold_index.rs` to keep that file
//! under the 1500-line cap; a child module, so private items stay in reach).

use super::*;

fn loc_in(file_id: u64, slot: u16) -> ColdLocation {
    ColdLocation {
        file_id,
        page_idx: 0,
        slot_idx: slot,
        ttl_ms: None,
        value_type: crate::persistence::kv_page::ValueType::String,
    }
}

/// moon#656: `INFO MoonStore` reports the cold tier's file-level shape,
/// and the two counters it needs are already maintained here — they were
/// simply not readable from outside this module.
#[test]
fn referenced_file_count_tracks_files_with_at_least_one_live_key() {
    let mut idx = ColdIndex::new();
    assert_eq!(
        idx.referenced_file_count(),
        0,
        "empty index references no file"
    );

    // Two keys co-located in one file, one key in another: two files,
    // three keys. A batched spill file holds up to FLUSH_ENTRY_CAP keys,
    // so file count and key count are genuinely independent numbers —
    // which is the whole point of reporting both.
    idx.insert(Bytes::from_static(b"a"), loc_in(7, 0));
    idx.insert(Bytes::from_static(b"b"), loc_in(7, 1));
    idx.insert(Bytes::from_static(b"c"), loc_in(9, 0));
    assert_eq!(idx.referenced_file_count(), 2);
    assert_eq!(idx.len(), 3);

    // Removing one of two co-located keys must NOT drop the file: its
    // sibling still lives there. This is the invariant whose violation
    // once collapsed cold read-through 200/200 -> 88/200.
    idx.remove(b"a");
    assert_eq!(idx.referenced_file_count(), 2, "file 7 still holds key b");

    idx.remove(b"b");
    assert_eq!(idx.referenced_file_count(), 1, "file 7 is now unreferenced");
}

/// The count reported as `cold_files_pending_unlink` — files whose last
/// live reference dropped and which the sweep has not yet unlinked.
#[test]
fn pending_unlink_len_counts_files_awaiting_the_sweep() {
    let mut idx = ColdIndex::new();
    assert_eq!(idx.pending_unlink_len(), 0);
    assert!(!idx.has_pending_unlink());

    idx.insert(Bytes::from_static(b"a"), loc_in(7, 0));
    idx.insert(Bytes::from_static(b"c"), loc_in(9, 0));
    assert_eq!(
        idx.pending_unlink_len(),
        0,
        "both files are still referenced"
    );

    idx.remove(b"a");
    assert_eq!(idx.pending_unlink_len(), 1, "file 7 dropped to zero refs");
    assert!(idx.has_pending_unlink());

    idx.remove(b"c");
    assert_eq!(idx.pending_unlink_len(), 2);

    // The existing boolean and the new count must never disagree — the
    // sweep trigger reads one and INFO reports the other.
    assert_eq!(idx.has_pending_unlink(), idx.pending_unlink_len() > 0);
}

#[test]
fn test_cold_index_insert_lookup_remove() {
    let mut idx = ColdIndex::new();
    let loc = ColdLocation {
        file_id: 1,
        page_idx: 0,
        slot_idx: 0,
        ttl_ms: None,
        value_type: crate::persistence::kv_page::ValueType::String,
    };
    idx.insert(Bytes::from_static(b"key1"), loc);
    assert_eq!(idx.len(), 1);
    let found = idx.lookup(b"key1").unwrap();
    assert_eq!(found.file_id, 1);
    assert_eq!(found.page_idx, 0);
    assert_eq!(found.slot_idx, 0);
    idx.remove(b"key1");
    assert!(idx.lookup(b"key1").is_none());
}

#[test]
fn range_from_is_hash_ordered_and_resumable() {
    // 200 entries paged via range_from must drain every key exactly
    // once in ascending (hash48, key) order — the #368 cold-plane
    // contract SCAN's merged page walk depends on.
    let mut idx = ColdIndex::new();
    let loc = ColdLocation {
        file_id: 1,
        page_idx: 0,
        slot_idx: 0,
        ttl_ms: None,
        value_type: crate::persistence::kv_page::ValueType::String,
    };
    for i in 0..200 {
        idx.insert(Bytes::from(format!("rk:{i}")), loc);
    }
    let mut seen: std::collections::HashSet<Bytes> = std::collections::HashSet::new();
    let mut cursor = 0u64;
    loop {
        let page: Vec<(u64, Bytes)> = idx
            .range_from(cursor)
            .take(16)
            .map(|(h, k, _)| (h, k.clone()))
            .collect();
        if page.is_empty() {
            break;
        }
        let mut prev: Option<(u64, &Bytes)> = None;
        for (h, k) in &page {
            assert_eq!(*h, scan_h48(k.as_ref()), "stored hash must match key");
            assert!(*h >= cursor, "entry below resume point");
            if let Some((ph, pk)) = prev {
                assert!((ph, pk) < (*h, k), "not ascending by (hash, key)");
            }
            prev = Some((*h, k));
            assert!(seen.insert(k.clone()), "duplicate across pages");
        }
        #[allow(clippy::unwrap_used)] // page verified non-empty above
        let last = page.last().unwrap().0;
        cursor = last + 1;
    }
    assert_eq!(seen.len(), 200, "every entry exactly once");
}

#[test]
fn lookup_and_remove_survive_equal_hash_range_probe() {
    // lookup/remove go through an equal-hash range probe now; two keys
    // in the index must stay independently addressable, and removing
    // one must not disturb the other.
    let mut idx = ColdIndex::new();
    let mk = |fid| ColdLocation {
        file_id: fid,
        page_idx: 0,
        slot_idx: 0,
        ttl_ms: None,
        value_type: crate::persistence::kv_page::ValueType::String,
    };
    idx.insert(Bytes::from_static(b"alpha"), mk(1));
    idx.insert(Bytes::from_static(b"beta"), mk(2));
    assert_eq!(idx.lookup(b"alpha").map(|l| l.file_id), Some(1));
    assert_eq!(idx.lookup(b"beta").map(|l| l.file_id), Some(2));
    assert!(!idx.remove(b"missing"));
    assert!(idx.remove(b"alpha"));
    assert!(idx.lookup(b"alpha").is_none());
    assert_eq!(idx.lookup(b"beta").map(|l| l.file_id), Some(2));
    assert_eq!(idx.len(), 1);
}

// ── K4 accounting spine: resident_bytes O(1) accumulator ─────────────

#[test]
fn resident_bytes_zero_when_empty() {
    assert_eq!(ColdIndex::new().resident_bytes(), 0);
}

#[test]
fn resident_bytes_grows_on_insert_shrinks_on_remove() {
    let mut idx = ColdIndex::new();
    let loc = ColdLocation {
        file_id: 1,
        page_idx: 0,
        slot_idx: 0,
        ttl_ms: None,
        value_type: crate::persistence::kv_page::ValueType::String,
    };
    idx.insert(Bytes::from_static(b"a_reasonably_long_key"), loc);
    let after_one = idx.resident_bytes();
    assert!(after_one > 0);

    idx.insert(Bytes::from_static(b"another_key"), loc);
    assert!(idx.resident_bytes() > after_one);

    // The index part shrinks on remove; the removed slot is still on
    // disk in file 1, so the dead-slot ledger now charges its key
    // (moon#1215) until the file is unlinked.
    let map_part = |idx: &ColdIndex| idx.resident_bytes() - idx.dead_slots().resident_bytes();
    idx.remove(b"another_key");
    assert_eq!(map_part(&idx), after_one);
    assert_eq!(idx.dead_slots().len(), 1);

    idx.remove(b"a_reasonably_long_key");
    assert_eq!(map_part(&idx), 0);
    assert_eq!(idx.resident_bytes(), idx.dead_slots().resident_bytes());
    assert!(idx.dead_slots().resident_bytes() > 0);
}

#[test]
fn resident_bytes_overwrite_same_key_does_not_double_count() {
    let mut idx = ColdIndex::new();
    let loc_a = ColdLocation {
        file_id: 1,
        page_idx: 0,
        slot_idx: 0,
        ttl_ms: None,
        value_type: crate::persistence::kv_page::ValueType::String,
    };
    let loc_b = ColdLocation {
        file_id: 2,
        page_idx: 0,
        slot_idx: 1,
        ttl_ms: None,
        value_type: crate::persistence::kv_page::ValueType::String,
    };
    idx.insert(Bytes::from_static(b"key1"), loc_a);
    let after_first = idx.resident_bytes();
    // Overwrite the SAME key with a different location (re-eviction to a
    // different file). Byte length is unchanged, so resident_bytes must
    // not grow.
    idx.insert(Bytes::from_static(b"key1"), loc_b);
    assert_eq!(
        idx.resident_bytes() - idx.dead_slots().resident_bytes(),
        after_first
    );
    // The superseded slot in file 1 is recorded as dead (moon#1215).
    assert!(idx.dead_slots().file_has_dead_slots(1));
}

#[test]
fn resident_bytes_zero_after_clear_all() {
    let mut idx = ColdIndex::new();
    let loc = ColdLocation {
        file_id: 1,
        page_idx: 0,
        slot_idx: 0,
        ttl_ms: None,
        value_type: crate::persistence::kv_page::ValueType::String,
    };
    idx.insert(Bytes::from_static(b"key1"), loc);
    idx.insert(Bytes::from_static(b"key2"), loc);
    assert!(idx.resident_bytes() > 0);
    idx.clear_all();
    assert_eq!(idx.len(), 0);
    // Only the dead-slot ledger remains charged: both slots stay on disk
    // until the sweep unlinks file 1 (moon#1215).
    assert_eq!(idx.resident_bytes(), idx.dead_slots().resident_bytes());
    assert_eq!(idx.dead_slots().len(), 2);
}

/// Create a shard dir with a `data/` subdir and a dummy heap-NNNNNN.mpf
/// file standing in for a batched multi-KV spill file.
fn make_shard_with_heap(file_ids: &[u64]) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    for &fid in file_ids {
        let p = data_dir.join(format!("heap-{:06}.mpf", fid));
        std::fs::write(&p, vec![0xABu8; 4096]).unwrap();
    }
    tmp
}

fn heap_path(shard_dir: &Path, file_id: u64) -> std::path::PathBuf {
    shard_dir
        .join("data")
        .join(format!("heap-{:06}.mpf", file_id))
}

/// REGRESSION (batch-file shared-deletion data loss): sweeping ONE orphan
/// key must NOT delete its `.mpf` while a co-located live key still
/// references the same file. Under spill batching a single file holds up
/// to 256 KVs; deleting it on one orphan key silently orphans the rest
/// (observed empirically as cold read-through 200/200 -> 88/200).
#[test]
fn test_sweep_retains_file_with_colocated_live_key() {
    let tmp = make_shard_with_heap(&[5]);
    let shard_dir = tmp.path();

    let mut ci = ColdIndex::new();
    // Two keys co-located in the SAME batched file (file_id = 5).
    ci.insert(
        Bytes::from_static(b"k_orphan"),
        ColdLocation {
            file_id: 5,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: None,
            value_type: crate::persistence::kv_page::ValueType::String,
        },
    );
    ci.insert(
        Bytes::from_static(b"k_live"),
        ColdLocation {
            file_id: 5,
            page_idx: 0,
            slot_idx: 1,
            ttl_ms: None,
            value_type: crate::persistence::kv_page::ValueType::String,
        },
    );

    let before_sweep = ci.resident_bytes();

    // Sweep ONLY the orphan key.
    ci.sweep_known_orphans(vec![Bytes::from_static(b"k_orphan")], shard_dir, None)
        .unwrap();

    // K4: resident_bytes must shrink by exactly the orphan's cost, via
    // the direct `self.map.remove` inside `sweep_known_orphans` (the
    // bypass site that does NOT go through the public `remove()`).
    assert!(
        ci.resident_bytes() < before_sweep,
        "sweeping an orphan must shrink resident_bytes"
    );

    // The co-located live key must remain resolvable AND its file present.
    assert!(
        ci.lookup(b"k_live").is_some(),
        "co-located live key dropped from cold index",
    );
    assert!(
        heap_path(shard_dir, 5).exists(),
        "DATA LOSS: file holding a live co-located key was deleted",
    );
    // The orphan entry itself is gone.
    assert!(ci.lookup(b"k_orphan").is_none(), "orphan entry not removed");
}

/// A batched file is unlinked only once its LAST live ref is removed.
#[test]
fn test_sweep_deletes_file_only_when_last_ref_removed() {
    let tmp = make_shard_with_heap(&[7]);
    let shard_dir = tmp.path();

    let mut ci = ColdIndex::new();
    ci.insert(
        Bytes::from_static(b"k1"),
        ColdLocation {
            file_id: 7,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: None,
            value_type: crate::persistence::kv_page::ValueType::String,
        },
    );
    ci.insert(
        Bytes::from_static(b"k2"),
        ColdLocation {
            file_id: 7,
            page_idx: 0,
            slot_idx: 1,
            ttl_ms: None,
            value_type: crate::persistence::kv_page::ValueType::String,
        },
    );

    // Sweep k1: one ref remains (k2) -> file MUST survive.
    ci.sweep_known_orphans(vec![Bytes::from_static(b"k1")], shard_dir, None)
        .unwrap();
    assert!(
        heap_path(shard_dir, 7).exists(),
        "file deleted while k2 still references it",
    );

    // Sweep k2: last ref removed -> file now reclaimed.
    ci.sweep_known_orphans(vec![Bytes::from_static(b"k2")], shard_dir, None)
        .unwrap();
    assert!(
        !heap_path(shard_dir, 7).exists(),
        "file not reclaimed after its last ref was swept",
    );
}

/// Re-eviction churn: `insert` overwriting a key's location (old file_id ->
/// new file_id) drops the old file to zero refs. The hot∩cold sweep can
/// never see this file (no key references it anymore), so the index must
/// enqueue it for reclamation and a subsequent sweep must unlink it.
#[test]
fn test_overwrite_reclaims_orphaned_old_file() {
    let tmp = make_shard_with_heap(&[10, 11]);
    let shard_dir = tmp.path();

    let mut ci = ColdIndex::new();
    ci.insert(
        Bytes::from_static(b"k"),
        ColdLocation {
            file_id: 10,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: None,
            value_type: crate::persistence::kv_page::ValueType::String,
        },
    );
    // Key re-spilled to a NEW file (re-eviction) -> file 10 orphaned.
    ci.insert(
        Bytes::from_static(b"k"),
        ColdLocation {
            file_id: 11,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: None,
            value_type: crate::persistence::kv_page::ValueType::String,
        },
    );

    // A sweep with NO orphan keys must still drain the pending unlink.
    ci.sweep_known_orphans(vec![], shard_dir, None).unwrap();

    assert!(
        !heap_path(shard_dir, 10).exists(),
        "orphaned old file (10) not reclaimed after overwrite",
    );
    assert!(
        heap_path(shard_dir, 11).exists(),
        "live file (11) wrongly deleted",
    );
    assert_eq!(ci.lookup(b"k").map(|l| l.file_id), Some(11));
}

// ── R1 / H-2: proactive TTL-expiry sweep ────────────────────────────────

/// RED->GREEN: a cold entry whose TTL has passed and which is NEVER
/// re-read (no GET touches it — the on-read reclaim path never fires)
/// must still be reclaimed by `sweep_expired`. This is the exact leak
/// described in tmp/OFFLOAD-COMPRESSION-REVIEW.md R1: before this fix,
/// nothing else in the system ever looks at a cold entry's TTL except a
/// read that never comes.
#[test]
fn test_sweep_expired_reclaims_never_read_entry() {
    let tmp = make_shard_with_heap(&[20]);
    let shard_dir = tmp.path();

    let mut ci = ColdIndex::new();
    ci.insert(
        Bytes::from_static(b"stale_session"),
        ColdLocation {
            file_id: 20,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: Some(1_000), // expires at t=1000ms
            value_type: crate::persistence::kv_page::ValueType::String,
        },
    );

    assert!(ci.resident_bytes() > 0, "insert must charge resident_bytes");

    // Sweep strictly after expiry. The key was never read.
    let stats = ci
        .sweep_expired(2_000, shard_dir, None, MAX_EXPIRED_SWEEP_BATCH)
        .unwrap();

    assert_eq!(
        stats.entries_reclaimed, 1,
        "sweep must reclaim the never-read expired entry"
    );
    // K4: the direct `self.map.remove` bypass inside `sweep_expired`
    // must also decrement resident_bytes.
    assert_eq!(
        ci.resident_bytes(),
        0,
        "resident_bytes must drop to 0 once the only entry expires"
    );
    assert!(
        stats.bytes_reclaimed > 0,
        "sweep must reclaim the backing file's bytes (last live ref)"
    );
    assert!(
        ci.lookup(b"stale_session").is_none(),
        "index entry must be gone after sweep"
    );
    assert!(
        !heap_path(shard_dir, 20).exists(),
        "backing DataFile must be unlinked once its last live ref expires"
    );
}

/// A cold entry with no TTL (`ttl_ms: None`) or a TTL still in the future
/// must NEVER be swept — only entries strictly past `now_ms` are expired.
#[test]
fn test_sweep_expired_ignores_live_entries() {
    let tmp = make_shard_with_heap(&[21]);
    let shard_dir = tmp.path();

    let mut ci = ColdIndex::new();
    ci.insert(
        Bytes::from_static(b"no_ttl"),
        ColdLocation {
            file_id: 21,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: None,
            value_type: crate::persistence::kv_page::ValueType::String,
        },
    );
    ci.insert(
        Bytes::from_static(b"future_ttl"),
        ColdLocation {
            file_id: 21,
            page_idx: 0,
            slot_idx: 1,
            ttl_ms: Some(5_000),
            value_type: crate::persistence::kv_page::ValueType::String,
        },
    );

    let stats = ci
        .sweep_expired(1_000, shard_dir, None, MAX_EXPIRED_SWEEP_BATCH)
        .unwrap();

    assert_eq!(stats.entries_reclaimed, 0, "no entry has expired yet");
    assert!(ci.lookup(b"no_ttl").is_some());
    assert!(ci.lookup(b"future_ttl").is_some());
    assert!(
        heap_path(shard_dir, 21).exists(),
        "file must survive when nothing has expired"
    );
}

/// Batch-file colocation must hold for the TTL sweep exactly as it does
/// for the orphan-shadow sweep: an expired key must NOT drag down a
/// co-located LIVE key's file. The file unlinks only once BOTH entries
/// are gone (mirrors `test_sweep_deletes_file_only_when_last_ref_removed`).
#[test]
fn test_sweep_expired_retains_file_with_colocated_live_key() {
    let tmp = make_shard_with_heap(&[22]);
    let shard_dir = tmp.path();

    let mut ci = ColdIndex::new();
    ci.insert(
        Bytes::from_static(b"expired"),
        ColdLocation {
            file_id: 22,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: Some(1_000),
            value_type: crate::persistence::kv_page::ValueType::String,
        },
    );
    ci.insert(
        Bytes::from_static(b"still_live"),
        ColdLocation {
            file_id: 22,
            page_idx: 0,
            slot_idx: 1,
            ttl_ms: Some(9_999_000),
            value_type: crate::persistence::kv_page::ValueType::String,
        },
    );

    // First sweep: only "expired" is past TTL.
    let stats1 = ci
        .sweep_expired(2_000, shard_dir, None, MAX_EXPIRED_SWEEP_BATCH)
        .unwrap();
    assert_eq!(stats1.entries_reclaimed, 1);
    assert!(ci.lookup(b"expired").is_none());
    assert!(
        ci.lookup(b"still_live").is_some(),
        "co-located live-TTL key must remain resolvable"
    );
    assert!(
        heap_path(shard_dir, 22).exists(),
        "DATA LOSS: file holding a live co-located key was deleted"
    );

    // Second sweep, now past the second key's TTL too: last ref drops,
    // file must finally be reclaimed.
    let stats2 = ci
        .sweep_expired(9_999_001, shard_dir, None, MAX_EXPIRED_SWEEP_BATCH)
        .unwrap();
    assert_eq!(stats2.entries_reclaimed, 1);
    assert!(
        !heap_path(shard_dir, 22).exists(),
        "file must be reclaimed once its last live ref also expires"
    );
}

/// Design-for-failure: `max_batch` bounds one call's work. With more
/// expired entries than the cap, a single call must reclaim at most
/// `max_batch` of them (never zero, never more) — the remainder is
/// picked up by a subsequent call, proving no per-tick unbounded stall.
#[test]
fn test_sweep_expired_respects_batch_cap() {
    let tmp = make_shard_with_heap(&[23]);
    let shard_dir = tmp.path();

    let mut ci = ColdIndex::new();
    for i in 0..10u16 {
        ci.insert(
            Bytes::from(format!("k{i}")),
            ColdLocation {
                file_id: 23,
                page_idx: 0,
                slot_idx: i,
                ttl_ms: Some(1_000),
                value_type: crate::persistence::kv_page::ValueType::String,
            },
        );
    }
    assert_eq!(ci.len(), 10);

    // Cap at 3 per call.
    let stats1 = ci.sweep_expired(2_000, shard_dir, None, 3).unwrap();
    assert_eq!(
        stats1.entries_reclaimed, 3,
        "one call must reclaim exactly max_batch entries when more are expired"
    );
    assert_eq!(ci.len(), 7, "the other 7 must remain for the next sweep");

    // Draining the rest across further capped calls must eventually
    // reach zero — no entry is permanently skipped by the cap.
    let mut total_reclaimed = stats1.entries_reclaimed;
    for _ in 0..10 {
        if ci.len() == 0 {
            break;
        }
        let s = ci.sweep_expired(2_000, shard_dir, None, 3).unwrap();
        total_reclaimed += s.entries_reclaimed;
    }
    assert_eq!(
        ci.len(),
        0,
        "all 10 expired entries must eventually reclaim"
    );
    assert_eq!(total_reclaimed, 10);
}

/// A sweep call with nothing expired must be a true no-op: zero entries
/// reclaimed, zero bytes, and it must not touch `pending_unlink` state
/// belonging to unrelated (non-TTL) reclamation paths.
#[test]
fn test_sweep_expired_noop_when_nothing_expired() {
    let tmp = make_shard_with_heap(&[24]);
    let shard_dir = tmp.path();

    let mut ci = ColdIndex::new();
    ci.insert(
        Bytes::from_static(b"k"),
        ColdLocation {
            file_id: 24,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: None,
            value_type: crate::persistence::kv_page::ValueType::String,
        },
    );

    let stats = ci
        .sweep_expired(1_000, shard_dir, None, MAX_EXPIRED_SWEEP_BATCH)
        .unwrap();
    assert_eq!(stats.entries_reclaimed, 0);
    assert_eq!(stats.bytes_reclaimed, 0);
    assert!(!ci.has_pending_unlink());
    assert_eq!(ci.len(), 1);
}
