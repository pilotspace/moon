use super::*;

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

    // Mirrors the real recovery contract exactly (PR review round 2,
    // commit 4): `Shard::restore_from_persistence` stashes
    // `RecoveryResult.warm_segments` on `self.recovered_warm_segments`;
    // `event_loop.rs` calls `register_warm_segments` right after the
    // `create_index` loop, BEFORE B3's dedup rescan (`reconcile_key`) and
    // BEFORE `RecoveryState::finish`. Attaching first lets
    // `register_warm_segments` populate `key_hash_to_key`/
    // `key_hash_to_global_id`/`key_hash_to_vec_checksum` for the WARM
    // segment's keys, which the rescan needs to treat them as
    // known/unchanged instead of re-encoding them into mutable (and which
    // `snapshot_recovered_baseline` below needs for the deletion probe to
    // cover WARM keys at all — see that method's docs).
    fresh.register_warm_segments(vec![(warm_file_id, warm_segment_dir.clone())]);
    state.snapshot_recovered_baseline(&fresh);

    // Replay the SAME HSETs through the dedup rescan (mirrors the real
    // boot sequence's keyspace scan, `event_loop.rs`, which always scans
    // every matching HASH key still in the keyspace) — nothing was deleted
    // while "down" in this scenario, so every key must be re-observed.
    // Without this, `finish()`'s deletion probe would see the WARM keys
    // `snapshot_recovered_baseline` just captured as never-observed and
    // wrongly tombstone all of them.
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
    // Ordering mirrors production exactly (PR review round 2, commit 4):
    // `create_index` -> `register_warm_segments` -> baseline snapshot ->
    // keyspace rescan (`reconcile_key`) -> `finish`. `register_warm_segments`
    // must run BEFORE the rescan, not after — see its own docs for why an
    // earlier ordering (attach after the rescan) defeated this very fix by
    // making the rescan re-index every WARM key into mutable first.
    let mut fresh = VectorStore::new();
    fresh.set_persist_dir(tmp.path().to_path_buf());
    let mut state = RecoveryState::new();
    state.create_index(&mut fresh, tmp.path(), &meta);

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
    state.snapshot_recovered_baseline(&fresh);

    // Replay the SAME HSETs through the dedup rescan (mirrors the real
    // boot sequence's keyspace scan, `event_loop.rs`) so `finish()`'s
    // deletion probe observes every key as still-present instead of
    // tombstoning them all as "no longer in the keyspace" — the probe's
    // baseline (`original_key_hashes`, now snapshotted by
    // `snapshot_recovered_baseline` right above) is every key that was
    // resident (HOT or, after the call above, cleanly-attached WARM) right
    // before the rescan, and it only treats a key as still-live once
    // `reconcile_key` has observed it.
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
    // Ordering mirrors production (PR review round 2, commit 4):
    // `register_warm_segments` runs right after both `create_index` calls,
    // before the keyspace rescan and `finish` — see
    // `VectorStore::register_warm_segments`'s docs.
    let mut fresh = VectorStore::new();
    fresh.set_persist_dir(tmp.path().to_path_buf());
    let mut state = RecoveryState::new();
    state.create_index(&mut fresh, tmp.path(), &meta_a);
    state.create_index(&mut fresh, tmp.path(), &meta_b);

    fresh.register_warm_segments(vec![segment_for_a.clone(), segment_for_b.clone()]);
    state.snapshot_recovered_baseline(&fresh);

    // Replay every HSET (both indexes) through the dedup rescan — nothing
    // was deleted while "down", so every key must be re-observed, or
    // `finish()`'s deletion probe would wrongly tombstone the WARM keys
    // `snapshot_recovered_baseline` just captured.
    for i in 0..n {
        for (key, seed) in [
            (format!("a:{i}"), i as u32 + 1),
            (format!("b:{i}"), i as u32 + 101),
        ] {
            let blob = f32_blob(dim, seed);
            let args = vec![
                Frame::BulkString(Bytes::from(key.clone())),
                Frame::BulkString(Bytes::from_static(b"vec")),
                Frame::BulkString(blob),
            ];
            let mut text_store = TextStore::new();
            state.reconcile_key(&mut fresh, &mut text_store, key.as_bytes(), &args, 0);
        }
    }

    state.finish(&mut fresh, tmp.path());

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

/// PR review round 2 (commit 4): the FULL production B3 sequence is
/// `create_index` -> `register_warm_segments` -> keyspace rescan
/// (`reconcile_key` over every still-live matching key) -> `finish`.
/// Commit 3's tests exercised `register_warm_segments` against a much
/// narrower slice of that sequence (either no rescan at all, or a rescan
/// whose keys were already "known" from a HOT reload for an unrelated
/// reason) and missed that running `register_warm_segments` AFTER the
/// rescan/`finish` — as `event_loop.rs` originally did — defeats the
/// WHOLE feature on every NORMAL restart, not just a crash:
/// `load_segments_and_keymap` never populates `key_hash_to_key`/
/// `key_hash_to_global_id` for WARM keys (see its `segment_resident`
/// gate, since nothing has attached the WARM segment to `idx.segments`
/// yet at that point), so a rescan running first sees every WARM key as
/// unknown and re-encodes it into the mutable segment; the duplication
/// check in `register_warm_segments` then sees those just-re-indexed
/// keys as "already covered" by what it thinks is a live HOT copy and
/// retires (permanently deletes) the WARM segment — RSS win gone, warm
/// vectors re-quantized, on every reboot.
///
/// This test drives the real sequence end to end, in order, and proves:
/// (a) the WARM segment attaches and is NOT retired,
/// (b) the rescan counts every still-live WARM key as `verified_unchanged`
///     (0 `re_indexed`), and none of them land in the mutable segment,
/// (c) search resolves each WARM doc's ORIGINAL key bytes via
///     `key_hash_to_key` — the exact map
///     `command::vector_search::hybrid::resolve_hybrid_doc_key` uses in
///     production — instead of falling through to a synthetic `vec:<id>`,
/// (d) no key_hash appears twice in search results,
/// (e) a WARM key whose underlying HASH was deleted while the server was
///     down (simulated: never observed by the rescan) is tombstoned by
///     `finish()`'s deletion probe (`snapshot_recovered_baseline` must
///     cover WARM keys, not just HOT/immutable ones, for this to work)
///     and is actually absent from a live search afterward.
///
/// (a), (b), and (c) are RED against the commit-3 ordering — verified by
/// hand: temporarily moving `register_warm_segments` back to run after
/// `finish()` (matching commit 3's `event_loop.rs`) turns this test's
/// warm-segment assertion in (a) into a "retired as duplicate" outcome
/// (0 warm segments attached, the segment directory deleted) and makes
/// (b)/(c) unreachable since there is no WARM segment left to check.
#[test]
fn warm_recovery_full_production_sequence_matches_real_boot_order() {
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
    let old_keymap_path = idx_dir.join(format!("keymap-{}.bin", pre_manifest.keymap_epoch));

    // ---- HOT -> WARM transition ----
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

    // Wait for the async Stack B snapshot the transition triggered to
    // FULLY settle (manifest advanced past the pre-transition epoch AND
    // the superseded segment/keymap actually swept) before reading
    // anything ownership-relevant off disk — this is a clean (non-crash)
    // restart scenario, so by the time we "reboot" below, this must have
    // long since committed. Same rationale/pattern as
    // `warm_reattach_dedups_against_crash_before_gc_race`.
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
            "post-transition snapshot never settled"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }

    // Pre-restart baseline for a key that survives the "outage" unchanged.
    let surviving_idx = 2usize;
    let surviving_key = format!("doc:{surviving_idx}");
    let surviving_blob = f32_blob(dim, surviving_idx as u32 + 1);
    let surviving_query: Vec<f32> = surviving_blob
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
        .collect();
    let surviving_key_hash = xxhash_rust::xxh64::xxh64(surviving_key.as_bytes(), 0);
    let expected_global_id = {
        let idx = store.get_index(b"idx").unwrap();
        *idx.key_hash_to_global_id.get(&surviving_key_hash).unwrap()
    };

    // The key we simulate a DEL-while-down for: simply never replayed
    // through the rescan below (its HASH is no longer in the keyspace).
    let deleted_idx = 5usize;
    let deleted_key_hash = xxhash_rust::xxh64::xxh64(format!("doc:{deleted_idx}").as_bytes(), 0);

    // ============ Simulate restart: the FULL production sequence ============
    let mut fresh = VectorStore::new();
    fresh.set_persist_dir(tmp.path().to_path_buf());
    let mut state = RecoveryState::new();
    state.create_index(&mut fresh, tmp.path(), &meta);

    fresh.register_warm_segments(vec![(warm_file_id, warm_segment_dir.clone())]);

    // (a) attached, not retired.
    {
        let idx = fresh.get_index(b"idx").unwrap();
        let snap = idx.segments.load();
        assert_eq!(
            snap.warm.len(),
            1,
            "(a) the WARM segment must attach on a normal restart, not be retired"
        );
        assert_eq!(snap.immutable.len(), 0);
    }
    assert!(
        warm_segment_dir.exists(),
        "(a) the warm segment's on-disk files must survive a clean attach"
    );

    state.snapshot_recovered_baseline(&fresh);

    // Keyspace rescan: replay every key EXCEPT `deleted_idx` (simulates a
    // DEL that happened while the server was down).
    for i in 0..n {
        if i == deleted_idx {
            continue;
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

    // (b) every still-live WARM key verified unchanged, none re-indexed —
    // and, concretely, none of them landed in the mutable segment (a
    // wrongly re-indexed key would; a verified-unchanged one never
    // touches mutable at all).
    {
        let c = *state.counters.get(&Bytes::from_static(b"idx")).unwrap();
        assert_eq!(
            c.verified_unchanged,
            n - 1,
            "(b) every still-live WARM key must be verified unchanged, not re-encoded"
        );
        assert_eq!(
            c.re_indexed, 0,
            "(b) no WARM key should be re-indexed into mutable when nothing actually changed"
        );
    }
    {
        let idx = fresh.get_index(b"idx").unwrap();
        assert_eq!(
            idx.segments.load().mutable.len(),
            0,
            "(b) no key should have been re-encoded into the mutable segment"
        );
    }

    state.finish(&mut fresh, tmp.path());

    // (e) delete-while-down: tombstoned by the deletion probe.
    {
        let idx = fresh.get_index(b"idx").unwrap();
        assert!(
            !idx.key_hash_to_global_id.contains_key(&deleted_key_hash),
            "(e) a WARM key deleted while the server was down must be tombstoned by \
             finish()'s deletion probe"
        );
    }

    // (c) search resolves the ORIGINAL key bytes via `key_hash_to_key` —
    // the same map `resolve_hybrid_doc_key` consults in production —
    // instead of falling through to a synthetic `vec:<id>`. (d) no
    // duplicate key_hash across results. (e, continued) the deleted key
    // never appears in results at all.
    {
        let idx = fresh.get_index_mut(b"idx").unwrap();
        let results = idx
            .segments
            .search(&surviving_query, 2 * n, 128, &mut idx.scratch);
        let hit = results
            .iter()
            .find(|r| r.id.0 == expected_global_id)
            .expect("(c) the surviving key's vector must be found via search");
        assert_ne!(
            hit.key_hash, 0,
            "(c) SearchResult.key_hash must be populated for a WARM-tier hit"
        );
        let resolved_key = idx.key_hash_to_key.get(&hit.key_hash);
        assert_eq!(
            resolved_key,
            Some(&Bytes::from(surviving_key.clone())),
            "(c) search must resolve the ORIGINAL key bytes for a WARM doc, not a \
             synthetic vec:<id>"
        );

        let mut seen = std::collections::HashSet::new();
        for r in &results {
            assert!(
                seen.insert(r.key_hash),
                "(d) duplicate key_hash {} in search results",
                r.key_hash
            );
        }
        assert!(
            !results.iter().any(|r| r.key_hash == deleted_key_hash),
            "(e) the deleted-while-down key must not appear in search results"
        );
    }
}
