use super::*;
use crate::vector::store::VectorFieldMeta;
use crate::vector::turbo_quant::collection::QuantizationConfig;
use crate::vector::types::DistanceMetric;

fn make_meta(name: &str, dim: u32, prefix: &str) -> IndexMeta {
    IndexMeta {
        name: Bytes::copy_from_slice(name.as_bytes()),
        dimension: dim,
        padded_dimension: dim,
        metric: DistanceMetric::L2,
        hnsw_m: 16,
        hnsw_ef_construction: 100,
        hnsw_ef_runtime: 0,
        compact_threshold: 0,
        source_field: Bytes::from_static(b"vec"),
        key_prefixes: vec![Bytes::copy_from_slice(prefix.as_bytes())],
        quantization: QuantizationConfig::Sq8,
        build_mode: crate::vector::turbo_quant::collection::BuildMode::Light,
        vector_fields: vec![VectorFieldMeta {
            field_name: Bytes::from_static(b"vec"),
            dimension: dim,
            padded_dimension: dim,
            metric: DistanceMetric::L2,
            quantization: QuantizationConfig::Sq8,
            build_mode: crate::vector::turbo_quant::collection::BuildMode::Light,
        }],
        schema_fields: Vec::new(),
        merge_mode: crate::vector::store::MergeMode::default(),
        keep_raw: false,
        db_index: 0,
        rerank_mult: 4,
        exact_beam: false,
    }
}

fn f32_blob(dim: usize, seed: u32) -> Bytes {
    let mut v = Vec::with_capacity(dim * 4);
    let mut s = seed;
    for _ in 0..dim {
        s = s.wrapping_mul(1664525).wrapping_add(1013904223);
        let f = (s as f32) / (u32::MAX as f32);
        v.extend_from_slice(&f.to_le_bytes());
    }
    Bytes::from(v)
}

/// Poll for `manifest.json` to appear (bounded, short interval) — the
/// background `global_snapshot_pool()` worker is a single shared thread
/// across every test in this binary, so its queue depth (hence latency)
/// is not under this test's control. Fails the assertion (returns
/// `None`) rather than hanging if the deadline is exceeded.
fn wait_for_manifest(idx_dir: &std::path::Path) -> Option<IndexManifest> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if let Some(m) = manifest::read_manifest_tolerant(idx_dir) {
            return Some(m);
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// No manifest on disk -> `create_index` behaves exactly like the plain
/// pre-B3 path (fresh collection_id, no recovered state).
#[test]
fn create_index_no_manifest_is_plain_fresh() {
    let tmp = tempfile::tempdir().unwrap();
    let mut store = VectorStore::new();
    store.set_persist_dir(tmp.path().to_path_buf());
    let mut state = RecoveryState::new();
    let meta = make_meta("idx", 8, "doc:");

    state.create_index(&mut store, tmp.path(), &meta);

    assert!(store.get_index(b"idx").is_some());
    assert!(
        !state.recovered_names.contains(&Bytes::from_static(b"idx")),
        "no manifest on disk -> index must NOT be marked recovered"
    );
}

/// Round-trip: build a store with a persist_dir, insert vectors, force a
/// synchronous compact + snapshot so manifest/keymap/segments land on
/// disk, then recover into a FRESH store. Verifies:
/// - `verified_unchanged == n` (dedup actually fires for every key, not
///   just "results happen to match" — a full re-encode would also leave
///   every key indexed and the allocator seeded, so this counter check
///   is the one assertion that actually pins dedup behavior).
/// - the mutable segment's global-id allocator is seeded above every
///   loaded global_id (a post-recovery insert never collides).
/// - a same-query HNSW search returns the identical top-1 global_id
///   before persist and after recovery — this is real end-to-end proof
///   that `suggested_ef` round-trips AND that dedup preserves the
///   original global_id (a silent re-encode of doc:5 would reassign it
///   a fresh id from the seeded allocator and this assertion would go
///   red, independently of `verified_unchanged`'s own count check).
///   NOTE (verified by a temporary experiment, then reverted): offsetting
///   `create_index_with_collection_id`'s pinned cid by 1000 left this
///   test green. Root cause, confirmed by reading `ImmutableSegment::
///   search` (immutable.rs:499): it passes `&self.collection_meta` — the
///   segment's OWN disk-reconstructed rotation/codebook data — into
///   `hnsw_search`, never the recreated index's collection object. After
///   `force_compact()` all data lives in that one self-contained
///   immutable segment, so a search hitting only pre-recovery data is
///   structurally unable to observe a wrong pin — true for every
///   quantization mode, not an SQ8-specific gap. The pin's actual
///   invariant is merge-compatibility of POST-recovery inserts (a future
///   re-compact/GraphUnion must stitch new codes over the old ones using
///   shared rotation) — untestable via search and out of scope for B3
///   test hardening (would need a full second compact+merge cycle).
///   Recorded here as a known, deliberate test gap rather than implied
///   coverage.
#[test]
fn recover_round_trip_dedups_unchanged_keys_and_seeds_allocator() {
    use crate::protocol::Frame;

    let tmp = tempfile::tempdir().unwrap();
    let dim = 8usize;

    // ---- Build + persist ----
    let mut store = VectorStore::new();
    store.set_persist_dir(tmp.path().to_path_buf());
    let meta = make_meta("idx", dim as u32, "doc:");
    store.create_index(meta.clone()).unwrap();

    let n = 20usize;
    for i in 0..n {
        let key = format!("doc:{i}");
        let blob = f32_blob(dim, i as u32 + 1);
        let args = vec![
            Frame::BulkString(Bytes::from(key.clone())),
            Frame::BulkString(Bytes::from_static(b"vec")),
            Frame::BulkString(blob),
        ];
        let _ = crate::shard::spsc_handler::auto_index_hset_public(
            &mut store,
            &mut TextStore::new(),
            key.as_bytes(),
            &args,
            0,
        );
    }

    {
        let idx = store.get_index_mut(b"idx").unwrap();
        // `force_compact` writes the segment directory INLINE (the
        // staged writer runs synchronously on this thread) but commits
        // the manifest/keymap via `persist_hook_after_install`, which
        // hands the job to the process-wide `global_snapshot_pool()`
        // (a single background worker THREAD, shared by every test in
        // this binary). Do NOT also build and run a second, manually
        // constructed `SnapshotJob` here: both would race the exact
        // same `manifest.json.tmp`/`keymap-1.bin.tmp` paths (fixed
        // names, not job-unique) and can stomp each other's fsync —
        // this was tried and is flaky. Instead just wait for the ONE
        // real job the production code path already submitted.
        idx.force_compact();
    }
    let idx_dir = manifest::index_persist_dir(tmp.path(), b"idx");
    let loaded_manifest = wait_for_manifest(&idx_dir)
        .expect("manifest must land within the timeout via global_snapshot_pool()");
    assert!(!loaded_manifest.segment_ids.is_empty());
    let max_loaded_global_id: u32 = {
        let idx = store.get_index(b"idx").unwrap();
        idx.key_hash_to_global_id.values().copied().max().unwrap()
    };

    // Pre-persist search baseline: query with doc:5's own vector (exact
    // match, distance 0) — nearest neighbor must be itself. Captured
    // AFTER force_compact() so both sides of the comparison search the
    // same shape of state (empty mutable + 1 immutable HNSW segment).
    let query_idx = 5usize;
    let query_key = format!("doc:{query_idx}");
    let query_blob_bytes = f32_blob(dim, query_idx as u32 + 1);
    let query_f32: Vec<f32> = query_blob_bytes
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
        .collect();
    let expected_global_id = {
        let idx = store.get_index(b"idx").unwrap();
        let kh = xxhash_rust::xxh64::xxh64(query_key.as_bytes(), 0);
        *idx.key_hash_to_global_id.get(&kh).unwrap()
    };
    let pre_results = store.search_index(b"idx", &query_f32, 1, 64).unwrap();
    assert_eq!(
        pre_results.first().copied(),
        Some(expected_global_id),
        "sanity: pre-persist search must return the exact-match vector as top-1"
    );

    // ---- Recover into a FRESH store ----
    let mut fresh = VectorStore::new();
    fresh.set_persist_dir(tmp.path().to_path_buf());
    let mut state = RecoveryState::new();
    state.create_index(&mut fresh, tmp.path(), &meta);
    assert!(state.recovered_names.contains(&Bytes::from_static(b"idx")));

    let counters = *state.counters.get(&Bytes::from_static(b"idx")).unwrap();
    assert_eq!(counters.loaded_segments, loaded_manifest.segment_ids.len());

    // Re-run the SAME HSETs through the dedup rescan — every one must
    // be recognized as unchanged (checksum match), never re-encoded.
    for i in 0..n {
        let key = format!("doc:{i}");
        let blob = f32_blob(dim, i as u32 + 1);
        let args = vec![
            Frame::BulkString(Bytes::from(key.clone())),
            Frame::BulkString(Bytes::from_static(b"vec")),
            Frame::BulkString(blob),
        ];
        let mut text_store = TextStore::new();
        state.reconcile_key(&mut fresh, &mut text_store, key.as_bytes(), &args, 0);
    }

    // The load-bearing assertion: every key must have taken the
    // metadata-only dedup path, not the full re-encode path. A full
    // re-encode of all n keys would ALSO leave every key indexed and the
    // allocator correctly seeded (checked below), so those checks alone
    // cannot distinguish "dedup worked" from "dedup is silently dead".
    let verified_unchanged = state
        .counters
        .get(&Bytes::from_static(b"idx"))
        .unwrap()
        .verified_unchanged;
    assert_eq!(
        verified_unchanged, n,
        "every unchanged key must dedup (metadata-only rebuild), not re-encode"
    );

    state.finish(&mut fresh, tmp.path());

    let final_counters = {
        // finish() consumed `state`; re-derive expectations from the
        // fresh store's live state instead.
        fresh.get_index(b"idx").unwrap().key_hash_to_global_id.len()
    };
    assert_eq!(
        final_counters, n,
        "all keys must still be indexed post-recovery"
    );

    // Post-recovery insert must get a global_id above every loaded one.
    let new_key = b"doc:new".to_vec();
    let new_blob = f32_blob(dim, 9999);
    let args = vec![
        Frame::BulkString(Bytes::from(new_key.clone())),
        Frame::BulkString(Bytes::from_static(b"vec")),
        Frame::BulkString(new_blob),
    ];
    let mut text_store = TextStore::new();
    let _ = crate::shard::spsc_handler::auto_index_hset_public(
        &mut fresh,
        &mut text_store,
        &new_key,
        &args,
        0,
    );
    let new_global_id = *fresh
        .get_index(b"idx")
        .unwrap()
        .key_hash_to_global_id
        .get(&xxhash_rust::xxh64::xxh64(&new_key, 0))
        .unwrap();
    assert!(
        new_global_id > max_loaded_global_id,
        "post-recovery insert global_id ({new_global_id}) must exceed every loaded \
             global_id ({max_loaded_global_id})"
    );

    // Post-recovery search must return the SAME top-1 as the
    // pre-persist baseline — this is what actually exercises
    // `suggested_ef` round-tripping and the collection_id/QJL-seed pin:
    // a mismatched seed scrambles HNSW beam search (wrong/garbage
    // distances), which a plain key-count check would never catch.
    let post_results = fresh.search_index(b"idx", &query_f32, 1, 64).unwrap();
    assert_eq!(
        post_results.first().copied(),
        Some(expected_global_id),
        "post-recovery search must return the same top-1 global_id as the pre-persist baseline"
    );
}

/// Deterministic regression test for the "phantom keymap entry"
/// silent-loss hole (found by `tests/crash_recovery_vector_durability.rs`
/// S1 flaking: post-crash `num_docs` 1950/2000 with `verified_unchanged
/// == 2000, re_indexed == 0`).
///
/// The durable keymap covers EVERY indexed key (mutable + immutable) at
/// snapshot-submit time; a kill -9 landing before the NEXT snapshot
/// (covering a just-frozen segment) leaves on-disk keymap ⊃ on-disk
/// segments. This test stages that state directly: persist n docs
/// normally, then rewrite the durable keymap with one EXTRA entry whose
/// checksum matches its AOF blob but whose doc is in NO segment. The
/// dedup rescan must RE-INDEX that key (making it searchable), never
/// "verify" it unchanged (which drops the doc silently).
#[test]
fn recover_reindexes_keymap_entries_not_backed_by_any_segment() {
    use crate::protocol::Frame;

    let tmp = tempfile::tempdir().unwrap();
    let dim = 8usize;

    // ---- Build + persist n real docs (same flow as the round-trip test) ----
    let mut store = VectorStore::new();
    store.set_persist_dir(tmp.path().to_path_buf());
    let meta = make_meta("idx", dim as u32, "doc:");
    store.create_index(meta.clone()).unwrap();

    let n = 8usize;
    for i in 0..n {
        let key = format!("doc:{i}");
        let blob = f32_blob(dim, i as u32 + 1);
        let args = vec![
            Frame::BulkString(Bytes::from(key.clone())),
            Frame::BulkString(Bytes::from_static(b"vec")),
            Frame::BulkString(blob),
        ];
        let _ = crate::shard::spsc_handler::auto_index_hset_public(
            &mut store,
            &mut TextStore::new(),
            key.as_bytes(),
            &args,
            0,
        );
    }
    {
        let idx = store.get_index_mut(b"idx").unwrap();
        idx.force_compact();
    }
    let idx_dir = manifest::index_persist_dir(tmp.path(), b"idx");
    let loaded_manifest = wait_for_manifest(&idx_dir)
        .expect("manifest must land within the timeout via global_snapshot_pool()");

    // ---- Stage the crash window: durable keymap ⊃ durable segments ----
    // The phantom's checksum MATCHES the blob the rescan will replay
    // (that is the crash-window signature: the key was mutable-resident
    // and fully checksummed when this keymap snapshot committed, but its
    // segment never reached disk).
    let phantom_key = b"doc:phantom".to_vec();
    let phantom_hash = xxhash_rust::xxh64::xxh64(&phantom_key, 0);
    let phantom_blob = f32_blob(dim, 4242);
    let max_gid = {
        let idx = store.get_index(b"idx").unwrap();
        idx.key_hash_to_global_id.values().copied().max().unwrap()
    };
    let mut entries = manifest::read_keymap_tolerant(&idx_dir, loaded_manifest.keymap_epoch)
        .expect("keymap must be readable");
    assert_eq!(entries.len(), n, "sanity: durable keymap covers all n docs");
    entries.push(manifest::KeymapEntry {
        key_hash: phantom_hash,
        global_id: max_gid + 1,
        vec_checksum: xxhash_rust::xxh64::xxh64(&phantom_blob, 0),
        key: Bytes::from(phantom_key.clone()),
    });
    manifest::write_keymap_atomic(&idx_dir, loaded_manifest.keymap_epoch, &entries)
        .expect("rewrite keymap with phantom entry");

    // ---- Recover into a FRESH store ----
    let mut fresh = VectorStore::new();
    fresh.set_persist_dir(tmp.path().to_path_buf());
    let mut state = RecoveryState::new();
    state.create_index(&mut fresh, tmp.path(), &meta);
    assert!(state.recovered_names.contains(&Bytes::from_static(b"idx")));

    // The phantom must NOT have been loaded into the recovered maps —
    // its doc exists in no loaded segment.
    assert!(
        fresh
            .get_index(b"idx")
            .unwrap()
            .key_hash_to_key
            .get(&phantom_hash)
            .is_none(),
        "keymap entry with no backing segment doc must be dropped at load"
    );

    // Rescan: replay all n real keys plus the phantom (its HSET is in
    // the AOF — the key genuinely exists in the keyspace).
    let mut text_store = TextStore::new();
    for i in 0..n {
        let key = format!("doc:{i}");
        let blob = f32_blob(dim, i as u32 + 1);
        let args = vec![
            Frame::BulkString(Bytes::from(key.clone())),
            Frame::BulkString(Bytes::from_static(b"vec")),
            Frame::BulkString(blob),
        ];
        state.reconcile_key(&mut fresh, &mut text_store, key.as_bytes(), &args, 0);
    }
    let phantom_args = vec![
        Frame::BulkString(Bytes::from(phantom_key.clone())),
        Frame::BulkString(Bytes::from_static(b"vec")),
        Frame::BulkString(phantom_blob.clone()),
    ];
    state.reconcile_key(&mut fresh, &mut text_store, &phantom_key, &phantom_args, 0);

    let counters = *state.counters.get(&Bytes::from_static(b"idx")).unwrap();
    assert_eq!(
        counters.verified_unchanged, n,
        "the n segment-backed keys dedup as unchanged"
    );
    assert_eq!(
        counters.re_indexed, 1,
        "the phantom key must take the full re-index path (checksum match \
             without a backing segment doc must never count as unchanged)"
    );

    state.finish(&mut fresh, tmp.path());

    // End-to-end: the phantom's document must actually be searchable —
    // exact-match query returns it as top-1 (before the fix the doc was
    // silently absent).
    let phantom_gid = *fresh
        .get_index(b"idx")
        .unwrap()
        .key_hash_to_global_id
        .get(&phantom_hash)
        .expect("phantom key must be indexed after the rescan");
    let query_f32: Vec<f32> = phantom_blob
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
        .collect();
    let results = fresh.search_index(b"idx", &query_f32, 1, 64).unwrap();
    assert_eq!(
        results.first().copied(),
        Some(phantom_gid),
        "phantom key's doc must be searchable post-recovery (was silently lost)"
    );
}

/// A segment whose directory is entirely missing (simulating a crash
/// that lost the whole segment, headers included) must not panic
/// recovery — falls back to abandoning this index's recovered state
/// (full rescan covers it).
#[test]
fn recover_corrupt_segment_with_unreadable_headers_falls_back_cleanly() {
    let tmp = tempfile::tempdir().unwrap();
    let idx_dir = manifest::index_persist_dir(tmp.path(), b"idx");
    std::fs::create_dir_all(&idx_dir).unwrap();

    // Manifest references segment-1, but NOTHING is written on disk for
    // it (not even mvcc_headers.bin) — the "headers also unreadable"
    // case.
    let m = IndexManifest {
        format_version: manifest::MANIFEST_FORMAT_VERSION,
        index_name_hex: manifest::index_name_hex(b"idx"),
        collection_id: 7,
        next_collection_id_floor: 8,
        next_segment_id: 2,
        next_global_id: 5,
        segment_ids: vec![1],
        keymap_epoch: 1,
    };
    manifest::write_manifest_atomic(&idx_dir, &m).unwrap();

    let mut store = VectorStore::new();
    store.set_persist_dir(tmp.path().to_path_buf());
    let meta = make_meta("idx", 8, "doc:");
    let mut state = RecoveryState::new();

    state.create_index(&mut store, tmp.path(), &meta);

    // Index exists (pinned cid) but is NOT marked recovered.
    assert!(store.get_index(b"idx").is_some());
    assert!(!state.recovered_names.contains(&Bytes::from_static(b"idx")));
}

/// Deletion probe (`finish()`'s `original_key_hashes` − `observed_key_hashes`
/// set difference): a key that existed at persist time but is never
/// observed during the rescan (deleted from the keyspace between
/// restarts) must be tombstoned — removed from the live index's
/// key_hash maps — even though it was never touched by `reconcile_key`.
#[test]
fn recover_finish_tombstones_keys_missing_from_rescan() {
    use crate::protocol::Frame;

    let tmp = tempfile::tempdir().unwrap();
    let dim = 8usize;

    let mut store = VectorStore::new();
    store.set_persist_dir(tmp.path().to_path_buf());
    let meta = make_meta("idx", dim as u32, "doc:");
    store.create_index(meta.clone()).unwrap();

    let n = 5usize;
    for i in 0..n {
        let key = format!("doc:{i}");
        let blob = f32_blob(dim, i as u32 + 1);
        let args = vec![
            Frame::BulkString(Bytes::from(key.clone())),
            Frame::BulkString(Bytes::from_static(b"vec")),
            Frame::BulkString(blob),
        ];
        let _ = crate::shard::spsc_handler::auto_index_hset_public(
            &mut store,
            &mut TextStore::new(),
            key.as_bytes(),
            &args,
            0,
        );
    }
    {
        let idx = store.get_index_mut(b"idx").unwrap();
        idx.force_compact();
    }
    let idx_dir = manifest::index_persist_dir(tmp.path(), b"idx");
    wait_for_manifest(&idx_dir)
        .expect("manifest must land within the timeout via global_snapshot_pool()");

    // Recover into a FRESH store, but only rescan n-1 of the n keys —
    // "doc:3" is simulated as deleted from the keyspace between restarts
    // (e.g. a DEL that happened while the server was down).
    let mut fresh = VectorStore::new();
    fresh.set_persist_dir(tmp.path().to_path_buf());
    let mut state = RecoveryState::new();
    state.create_index(&mut fresh, tmp.path(), &meta);
    assert!(state.recovered_names.contains(&Bytes::from_static(b"idx")));
    // Mirrors the production order (event_loop.rs): the deletion-probe
    // baseline is snapshotted after `create_index` (and, in production,
    // after `register_warm_segments` — there are no WARM segments here) and
    // before the rescan loop below.
    state.snapshot_recovered_baseline(&fresh);

    let missing_key = "doc:3";
    let missing_key_hash = xxhash_rust::xxh64::xxh64(missing_key.as_bytes(), 0);
    assert!(
        fresh
            .get_index(b"idx")
            .unwrap()
            .key_hash_to_global_id
            .contains_key(&missing_key_hash),
        "sanity: the to-be-deleted key must be present right after recovery, \
             before the rescan runs"
    );

    for i in 0..n {
        if i == 3 {
            continue; // never observed by reconcile_key — simulates a DEL
        }
        let key = format!("doc:{i}");
        let blob = f32_blob(dim, i as u32 + 1);
        let args = vec![
            Frame::BulkString(Bytes::from(key.clone())),
            Frame::BulkString(Bytes::from_static(b"vec")),
            Frame::BulkString(blob),
        ];
        let mut text_store = TextStore::new();
        state.reconcile_key(&mut fresh, &mut text_store, key.as_bytes(), &args, 0);
    }
    state.finish(&mut fresh, tmp.path());

    let idx = fresh.get_index(b"idx").unwrap();
    assert!(
        !idx.key_hash_to_global_id.contains_key(&missing_key_hash),
        "key never observed during the rescan must be tombstoned by finish()"
    );
    assert!(
        !idx.key_hash_to_key.contains_key(&missing_key_hash),
        "tombstoning must also prune the key_hash_to_key map"
    );
    assert_eq!(
        idx.key_hash_to_global_id.len(),
        n - 1,
        "the other n-1 keys (all observed) must remain indexed"
    );
}

#[cfg(test)]
#[path = "recover_v2_warm_tests.rs"]
mod warm_tests;
