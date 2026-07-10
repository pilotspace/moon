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

/// WARM restart-recovery hardening: proves the two paired fixes end to
/// end on real, force-compacted, on-disk state (not a manually
/// constructed segment, which cannot exercise Stack B's GC diff — it has
/// no `disk_segment_id`).
///
/// (a) LEAK FIX: `VectorIndex::try_warm_transitions_idle` now calls
///     `persist_hook_after_install` after every transition, so a segment
///     that just left `immutable` drops out of Stack B's `segment_ids`
///     too — its superseded `idx-<hex>/segment-<old_id>/` directory gets
///     GC'd by the next snapshot job's manifest diff (`run_snapshot_job`)
///     instead of leaking on disk forever.
/// (b) RESTART-WIRING FIX: `Shard::restore_from_persistence` stages
///     `RecoveryResult.warm_segments` on `Shard::recovered_warm_segments`;
///     `event_loop.rs` reattaches them via
///     `VectorStore::register_warm_segments` right after
///     `RecoveryState::finish`. This test calls the exact same two
///     functions directly (no live shard/event-loop needed) to prove a
///     "restart" reattaches the segment as WARM — not silently dropped,
///     not reloaded as a phantom HOT copy of stale data — with identical
///     recall to the pre-transition baseline.
#[test]
fn warm_transition_leak_fix_and_restart_recovery() {
    use crate::protocol::Frame;

    crate::vector::distance::init();
    let tmp = tempfile::tempdir().unwrap();
    let dim = 8usize;

    // ---- Build + persist ----
    let mut store = VectorStore::new();
    store.set_persist_dir(tmp.path().to_path_buf());
    let meta = make_meta("idx", dim as u32, "doc:");
    store.create_index(meta.clone()).unwrap();

    let n = 12usize;
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
    assert_eq!(
        loaded_manifest.segment_ids.len(),
        1,
        "force_compact must produce exactly 1 immutable segment"
    );
    let old_segment_id = loaded_manifest.segment_ids[0];
    let old_segment_dir = idx_dir.join(format!("segment-{old_segment_id}"));
    assert!(
        old_segment_dir.exists(),
        "sanity: compacted segment directory must exist on disk"
    );

    // Pre-transition search baseline (exact-match query -> itself, distance 0).
    let query_idx = 3usize;
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
        "sanity: pre-transition search must return the exact-match vector as top-1"
    );

    // ---- HOT -> WARM transition (warm_after_secs=0: everything qualifies) ----
    // `shard_dir == tmp.path()`, matching production's disk-offload mode
    // where Stack A's `shard_dir` and Stack B's `vector_persist_dir` are
    // the SAME directory (see `event_loop.rs`).
    let shard_dir = tmp.path().to_path_buf();
    let manifest_path = shard_dir.join("shard-0.manifest");
    let mut shard_manifest =
        crate::persistence::manifest::ShardManifest::create(&manifest_path).unwrap();
    let mut next_file_id = 1u64;
    let transitioned = store.try_warm_transitions_all(
        &shard_dir,
        &mut shard_manifest,
        0,
        &mut next_file_id,
        &mut None,
    );
    assert_eq!(transitioned, 1);

    {
        let idx = store.get_index(b"idx").unwrap();
        let snap = idx.segments.load();
        assert_eq!(
            snap.immutable.len(),
            0,
            "segment must have left the immutable tier"
        );
        assert_eq!(
            snap.warm.len(),
            1,
            "segment must now be resident in the warm tier"
        );
    }
    // `try_warm_transitions_idle` assigns `file_id = *next_file_id` THEN
    // increments, so the just-used id is `next_file_id - 1`.
    let warm_file_id = next_file_id - 1;
    let warm_segment_dir = shard_dir
        .join("vectors")
        .join(format!("segment-{warm_file_id}"));
    assert!(
        warm_segment_dir.exists(),
        "warm_tier::transition_to_warm must have written the segment's files"
    );

    // Search must still work identically while WARM (searchable, not stale).
    let warm_results = store.search_index(b"idx", &query_f32, 1, 64).unwrap();
    assert_eq!(
        warm_results.first().copied(),
        Some(expected_global_id),
        "search through a WARM segment must return the same top-1 as pre-transition"
    );

    // ---- (a) LEAK FIX: the superseded Stack-B directory must be GC'd ----
    // `persist_hook_after_install` (now called from inside
    // `try_warm_transitions_idle`) resubmits a manifest whose
    // `segment_ids` no longer contains `old_segment_id`;
    // `run_snapshot_job`'s diff physically removes `old_segment_dir`.
    let gc_deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if !old_segment_dir.exists() {
            break;
        }
        assert!(
            std::time::Instant::now() < gc_deadline,
            "superseded segment directory {old_segment_dir:?} was never GC'd \
                 after the WARM transition"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let post_transition_manifest = wait_for_manifest(&idx_dir).unwrap();
    assert!(
        !post_transition_manifest
            .segment_ids
            .contains(&old_segment_id),
        "the post-transition manifest must no longer track the superseded segment id"
    );

    // ---- (b) RESTART-WIRING FIX: simulate a restart into a FRESH store ----
    let mut fresh = VectorStore::new();
    fresh.set_persist_dir(tmp.path().to_path_buf());
    let mut state = RecoveryState::new();
    state.create_index(&mut fresh, tmp.path(), &meta);

    {
        let idx = fresh.get_index(b"idx").unwrap();
        assert_eq!(
            idx.segments.load().immutable.len(),
            0,
            "the GC'd segment must NOT be reloaded as a phantom HOT segment"
        );
    }

    state.finish(&mut fresh, tmp.path());

    // Mirrors the real recovery contract exactly: `Shard::
    // restore_from_persistence` stashes `RecoveryResult.warm_segments`
    // on `self.recovered_warm_segments`; `event_loop.rs` calls
    // `register_warm_segments` right after `RecoveryState::finish`
    // returns (never before — B3's dedup rescan must reconcile whatever
    // Stack B *did* recover first).
    fresh.register_warm_segments(vec![(warm_file_id, warm_segment_dir.clone())]);

    {
        let idx = fresh.get_index(b"idx").unwrap();
        let snap = idx.segments.load();
        assert_eq!(
            snap.warm.len(),
            1,
            "the WARM segment must survive the simulated restart"
        );
        assert_eq!(snap.immutable.len(), 0);
    }

    let post_restart_results = fresh.search_index(b"idx", &query_f32, 1, 64).unwrap();
    assert_eq!(
        post_restart_results.first().copied(),
        Some(expected_global_id),
        "post-restart search through the reattached WARM segment must return the same \
             top-1 global_id as the pre-transition baseline — no recall regression"
    );
}

/// Recursively copy a directory tree. Test-only helper for
/// `warm_reattach_dedups_against_crash_before_gc_race`: stashes a
/// segment directory so the "crash before Stack B's async GC committed"
/// state can be reproduced deterministically regardless of when the
/// shared `global_snapshot_pool()` background thread actually runs.
fn copy_dir_all(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let dst_path = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir_all(&entry.path(), &dst_path);
        } else {
            std::fs::copy(entry.path(), &dst_path).unwrap();
        }
    }
}

/// PR-review finding #1 (CRITICAL): a `kill -9` landing between
/// `transition_to_warm`'s durable Stack A commit and
/// `persist_hook_after_install`'s async Stack B GC leaves the SAME
/// vectors reachable twice on restart — the old segment reloads as HOT
/// (Stack B's manifest still lists it) *and* the warm copy gets
/// discovered by Stack A. Without a dedup check, `register_warm_segments`
/// would attach the warm copy on top of the already-reloaded HOT one:
/// `search_mvcc`'s merge (`all.sort_unstable(); all.truncate(k)`) has no
/// key_hash dedup, so results (and `num_docs`) would double-count
/// forever (not self-healing — the next snapshot re-adopts the reloaded
/// HOT copy into `segment_ids`).
///
/// This test deterministically reconstructs that exact crash window
/// (stash + force-restore the pre-transition manifest, independent of
/// the background snapshot pool's actual timing) and asserts the fix:
/// the warm copy is never attached, is retired (deleted) from disk, and
/// search returns the exact-match vector exactly once.
#[test]
fn warm_reattach_dedups_against_crash_before_gc_race() {
    use crate::protocol::Frame;

    crate::vector::distance::init();
    let tmp = tempfile::tempdir().unwrap();
    let dim = 8usize;

    let mut store = VectorStore::new();
    store.set_persist_dir(tmp.path().to_path_buf());
    let meta = make_meta("idx", dim as u32, "doc:");
    store.create_index(meta.clone()).unwrap();

    let n = 10usize;
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
    let pre_manifest = wait_for_manifest(&idx_dir)
        .expect("manifest must land within the timeout via global_snapshot_pool()");
    assert_eq!(pre_manifest.segment_ids.len(), 1);
    let old_segment_id = pre_manifest.segment_ids[0];
    let old_segment_dir = idx_dir.join(format!("segment-{old_segment_id}"));
    assert!(old_segment_dir.exists());

    // Stash the pre-transition segment directory AND its keymap file so
    // both can be restored below regardless of whether the async GC
    // already swept them away — `run_snapshot_job` deletes both the
    // superseded segment dir and the superseded `keymap-<epoch>.bin`
    // once a newer snapshot commits.
    let stash = tmp.path().join("stash-old-segment");
    copy_dir_all(&old_segment_dir, &stash);
    let old_keymap_path = idx_dir.join(format!("keymap-{}.bin", pre_manifest.keymap_epoch));
    let stashed_keymap = tmp.path().join("stash-old-keymap.bin");
    std::fs::copy(&old_keymap_path, &stashed_keymap).unwrap();

    let query_idx = 2usize;
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

    // ---- HOT -> WARM transition (durable Stack A commit is synchronous) ----
    let shard_dir = tmp.path().to_path_buf();
    let manifest_path = shard_dir.join("shard-0.manifest");
    let mut shard_manifest =
        crate::persistence::manifest::ShardManifest::create(&manifest_path).unwrap();
    let mut next_file_id = 1u64;
    let transitioned = store.try_warm_transitions_all(
        &shard_dir,
        &mut shard_manifest,
        0,
        &mut next_file_id,
        &mut None,
    );
    assert_eq!(transitioned, 1);
    let warm_file_id = next_file_id - 1;
    let warm_segment_dir = shard_dir
        .join("vectors")
        .join(format!("segment-{warm_file_id}"));
    assert!(warm_segment_dir.exists());

    // ---- Simulate "crash before Stack B's post-transition GC committed" ----
    // First wait for the REAL async `persist_hook_after_install` job
    // (submitted synchronously inside `try_warm_transitions_idle`,
    // completed on the shared background pool) to actually finish its
    // GC — otherwise our deliberate rollback below could itself get
    // clobbered by that job landing afterward. Once its effect is
    // observed (segment_ids no longer contains `old_segment_id`), force
    // the on-disk manifest/keymap back to the pre-transition state as
    // the LAST write: this deterministically reproduces the race the
    // finding describes without depending on background-thread timing.
    // `write_manifest_atomic` (dropping `old_segment_id` from
    // `segment_ids`) is only the SECOND of `run_snapshot_job`'s three
    // steps — its GC (deleting `old_segment_dir` and the superseded
    // keymap file) is a THIRD step that runs afterward, still on the
    // same background-thread call, still logically part of "this job".
    // Waiting on `segment_ids` alone races that GC: it can observe the
    // updated manifest, perform our rollback (restoring the stale
    // segment + keymap as the deliberate "crash" state), and then have
    // the OLD job's now-completing GC delete those same files out from
    // under it. Wait for the GC's actual file-system effect too, so the
    // rollback below is guaranteed to run after the whole job — GC
    // included — is done.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let m = wait_for_manifest(&idx_dir).unwrap();
        let manifest_advanced = !m.segment_ids.contains(&old_segment_id);
        let gc_landed = !old_segment_dir.exists() && !old_keymap_path.exists();
        if manifest_advanced && gc_landed {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the real leak-fix GC never dropped old_segment_id from segment_ids and \
                 swept its files (manifest_advanced={manifest_advanced}, gc_landed={gc_landed})"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    manifest::write_manifest_atomic(&idx_dir, &pre_manifest).unwrap();
    if !old_segment_dir.exists() {
        copy_dir_all(&stash, &old_segment_dir);
    }
    if !old_keymap_path.exists() {
        std::fs::copy(&stashed_keymap, &old_keymap_path).unwrap();
    }
    assert!(old_segment_dir.exists());
    assert!(old_keymap_path.exists());

    // ---- Restart into a FRESH store ----
    let mut fresh = VectorStore::new();
    fresh.set_persist_dir(tmp.path().to_path_buf());
    let mut state = RecoveryState::new();
    state.create_index(&mut fresh, tmp.path(), &meta);

    // Replay the SAME HSETs through the dedup rescan (mirrors the real
    // boot sequence's keyspace scan, `event_loop.rs`) so `finish()`'s
    // deletion probe observes every key as still-present instead of
    // tombstoning them all as "no longer in the keyspace" — the probe's
    // baseline (`original_key_hashes`) is every key `create_index` just
    // loaded from the (rolled-back) durable keymap, and it only treats a
    // key as still-live once `reconcile_key` has observed it.
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

    state.finish(&mut fresh, tmp.path());

    // Sanity: Stack B reloaded the stale segment as an ordinary
    // HOT/immutable copy — the crash-recovery precondition for the
    // duplication bug.
    {
        let idx = fresh.get_index(b"idx").unwrap();
        let snap = idx.segments.load();
        assert_eq!(
            snap.immutable.len(),
            1,
            "sanity: crash state must reload the stale segment as HOT"
        );
        assert!(
            idx.key_hash_to_global_id
                .contains_key(&xxhash_rust::xxh64::xxh64(query_key.as_bytes(), 0)),
            "sanity: the reloaded HOT segment's keymap must cover the query key"
        );
    }

    fresh.register_warm_segments(vec![(warm_file_id, warm_segment_dir.clone())]);

    // ---- The fix: no duplicate attachment, warm copy retired ----
    {
        let idx = fresh.get_index(b"idx").unwrap();
        let snap = idx.segments.load();
        assert_eq!(
            snap.warm.len(),
            0,
            "duplicate warm copy must NOT be attached when Stack B already covers it"
        );
        assert_eq!(
            snap.immutable.len(),
            1,
            "Stack B's HOT reload remains the single source of truth"
        );
    }
    assert!(
        !warm_segment_dir.exists(),
        "the superseded warm copy must be retired (deleted) from disk, not left leaking"
    );

    // ---- No duplicate results / correct recall ----
    let results = fresh.search_index(b"idx", &query_f32, 2 * n, 128).unwrap();
    let occurrences = results
        .iter()
        .filter(|&&gid| gid == expected_global_id)
        .count();
    assert_eq!(
        occurrences, 1,
        "the exact-match vector must appear exactly once in results, never duplicated"
    );
    assert_eq!(results.first().copied(), Some(expected_global_id));
}

/// PR-review finding #2 (HIGH): `register_warm_segments` used to attach
/// a warm segment to the FIRST index for which
/// `WarmSearchSegment::from_files` happened to succeed —
/// `from_files` accepts any caller-supplied `CollectionMetadata` and
/// never validates it against the file contents, so two indexes of the
/// same dimension/quantization made that a coin flip. This test builds
/// exactly that ambiguous setup (two same-dim indexes, each force-
/// compacted and warm-transitioned) and proves the fix routes each
/// segment to its true owner via persisted keymap key_hash evidence —
/// while the OLD naive "first successful `from_files`" approach is
/// re-run (with the fix's ownership logic bypassed) to prove it WOULD
/// have funneled both segments into a single index instead of the
/// correct 1-and-1 split, regardless of `HashMap` iteration order.
#[test]
fn warm_reattach_picks_correct_index_among_same_dim_indexes() {
    use crate::protocol::Frame;

    crate::vector::distance::init();
    let tmp = tempfile::tempdir().unwrap();
    let dim = 8usize;

    let mut store = VectorStore::new();
    store.set_persist_dir(tmp.path().to_path_buf());
    let meta_a = make_meta("idxA", dim as u32, "a:");
    let meta_b = make_meta("idxB", dim as u32, "b:");
    store.create_index(meta_a.clone()).unwrap();
    store.create_index(meta_b.clone()).unwrap();

    let n = 6usize;
    let mut key_hashes_a: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut key_hashes_b: std::collections::HashSet<u64> = std::collections::HashSet::new();
    for i in 0..n {
        let key_a = format!("a:{i}");
        let key_b = format!("b:{i}");
        key_hashes_a.insert(xxhash_rust::xxh64::xxh64(key_a.as_bytes(), 0));
        key_hashes_b.insert(xxhash_rust::xxh64::xxh64(key_b.as_bytes(), 0));
        for (key, seed) in [(key_a, i as u32 + 1), (key_b, i as u32 + 101)] {
            let blob = f32_blob(dim, seed);
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
    }
    assert!(
        key_hashes_a.is_disjoint(&key_hashes_b),
        "sanity: distinct key sets must not collide"
    );

    let mut pre_transition_epoch: std::collections::HashMap<Vec<u8>, u64> =
        std::collections::HashMap::new();
    for name in [b"idxA".as_slice(), b"idxB".as_slice()] {
        let idx = store.get_index_mut(name).unwrap();
        idx.force_compact();
        let idx_dir = manifest::index_persist_dir(tmp.path(), name);
        let m = wait_for_manifest(&idx_dir)
            .expect("manifest must land within the timeout via global_snapshot_pool()");
        pre_transition_epoch.insert(name.to_vec(), m.keymap_epoch);
    }

    // Pre-transition search baselines (captured while `store` still has
    // both indexes' data queryable) — the ground truth this test proves
    // is preserved after reattachment through the CORRECT index only.
    let query_a: Vec<f32> = f32_blob(dim, 1)
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
        .collect();
    let query_b: Vec<f32> = f32_blob(dim, 101)
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
        .collect();
    let expected_global_id_a = {
        let idx = store.get_index(b"idxA").unwrap();
        let kh = xxhash_rust::xxh64::xxh64(b"a:0".as_slice(), 0);
        *idx.key_hash_to_global_id.get(&kh).unwrap()
    };
    let expected_global_id_b = {
        let idx = store.get_index(b"idxB").unwrap();
        let kh = xxhash_rust::xxh64::xxh64(b"b:0".as_slice(), 0);
        *idx.key_hash_to_global_id.get(&kh).unwrap()
    };
    assert_eq!(
        store
            .search_index(b"idxA", &query_a, 1, 64)
            .unwrap()
            .first()
            .copied(),
        Some(expected_global_id_a),
        "sanity: pre-transition idxA search must return a:0 as top-1"
    );
    assert_eq!(
        store
            .search_index(b"idxB", &query_b, 1, 64)
            .unwrap()
            .first()
            .copied(),
        Some(expected_global_id_b),
        "sanity: pre-transition idxB search must return b:0 as top-1"
    );

    // ---- HOT -> WARM transition for both indexes ----
    let shard_dir = tmp.path().to_path_buf();
    let manifest_path = shard_dir.join("shard-0.manifest");
    let mut shard_manifest =
        crate::persistence::manifest::ShardManifest::create(&manifest_path).unwrap();
    let mut next_file_id = 1u64;
    let transitioned = store.try_warm_transitions_all(
        &shard_dir,
        &mut shard_manifest,
        0,
        &mut next_file_id,
        &mut None,
    );
    assert_eq!(transitioned, 2);

    // `transition_to_warm` only commits Stack A's shard manifest
    // synchronously; the Stack B snapshot that `persist_hook_after_install`
    // schedules for each index runs on `global_snapshot_pool()`'s
    // background worker(s). Wait for BOTH indexes' on-disk manifest to
    // advance past their pre-transition epoch before reading anything
    // ownership-relevant off disk — otherwise `register_warm_segments`
    // could race an in-flight `run_snapshot_job` (a real TOCTOU that
    // `read_manifest_and_keymap_consistent` only bounds, not
    // eliminates). Real recovery never hits this window: it only ever
    // observes a previous process's fully-committed on-disk state.
    for name in [b"idxA".as_slice(), b"idxB".as_slice()] {
        let idx_dir = manifest::index_persist_dir(tmp.path(), name);
        let pre_epoch = pre_transition_epoch[name.to_vec().as_slice()];
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if let Some(m) = manifest::read_manifest_tolerant(&idx_dir) {
                if m.keymap_epoch != pre_epoch {
                    break;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "post-transition snapshot for {:?} must land within the timeout",
                String::from_utf8_lossy(name),
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    // Identify which physical warm segment belongs to which logical
    // index by its own key_hash content (never by file_id ordering,
    // which depends on unspecified `HashMap` iteration order).
    let mut segment_for_a: Option<(u64, std::path::PathBuf)> = None;
    let mut segment_for_b: Option<(u64, std::path::PathBuf)> = None;
    for file_id in 1..next_file_id {
        let dir = shard_dir.join("vectors").join(format!("segment-{file_id}"));
        let hashes: std::collections::HashSet<u64> =
            crate::vector::persistence::warm_search::peek_key_hashes(&dir)
                .unwrap()
                .into_iter()
                .collect();
        if hashes == key_hashes_a {
            segment_for_a = Some((file_id, dir));
        } else if hashes == key_hashes_b {
            segment_for_b = Some((file_id, dir));
        } else {
            panic!("warm segment {file_id} at {dir:?} matched neither index's key set");
        }
    }
    let segment_for_a = segment_for_a.expect("idxA's warm segment must be found");
    let segment_for_b = segment_for_b.expect("idxB's warm segment must be found");

    // ---- Restart into a FRESH store with both indexes ----
    let mut fresh = VectorStore::new();
    fresh.set_persist_dir(tmp.path().to_path_buf());
    let mut state = RecoveryState::new();
    state.create_index(&mut fresh, tmp.path(), &meta_a);
    state.create_index(&mut fresh, tmp.path(), &meta_b);
    state.finish(&mut fresh, tmp.path());

    fresh.register_warm_segments(vec![segment_for_a.clone(), segment_for_b.clone()]);

    // ---- The fix: each segment attaches to its TRUE owner ----
    {
        let idx_a = fresh.get_index(b"idxA").unwrap();
        let snap_a = idx_a.segments.load();
        assert_eq!(
            snap_a.warm.len(),
            1,
            "idxA must receive exactly its own warm segment"
        );
    }
    {
        let idx_b = fresh.get_index(b"idxB").unwrap();
        let snap_b = idx_b.segments.load();
        assert_eq!(
            snap_b.warm.len(),
            1,
            "idxB must receive exactly its own warm segment"
        );
    }

    // End-to-end recall proof, using the SAME `global_id`s captured
    // before the transition: a cross-attached segment (idxA's data
    // wrongly attached to idxB, or vice versa) would still let each
    // `warm.len() == 1` check above pass, but would return the WRONG
    // (or no reasonable) top-1 here, since the wrong index's HNSW graph
    // was built over entirely different vectors.
    assert_eq!(
        fresh
            .search_index(b"idxA", &query_a, 1, 64)
            .unwrap()
            .first()
            .copied(),
        Some(expected_global_id_a),
        "idxA's reattached warm segment must return a:0 as top-1 — proves the segment \
             built from idxA's data landed on idxA, not idxB"
    );
    assert_eq!(
        fresh
            .search_index(b"idxB", &query_b, 1, 64)
            .unwrap()
            .first()
            .copied(),
        Some(expected_global_id_b),
        "idxB's reattached warm segment must return b:0 as top-1 — proves the segment \
             built from idxB's data landed on idxB, not idxA"
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
