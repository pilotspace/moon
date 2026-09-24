//! Unit tests for the segment-incremental snapshot engine (moved out of
//! `snapshot.rs` to keep that file under the 1,500-line limit).

use super::*;
use crate::storage::compact_value::RedisValueRef;
use ordered_float::OrderedFloat;
use tempfile::tempdir;

fn snap_path() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("dump.rrdshard");
    (dir, path)
}

/// P2 — v2 snapshot header round-trip: stamp last_lsn + created_at,
/// reload via the metadata peek API, and verify the fields survive.
#[test]
fn test_snapshot_v2_header_roundtrip() {
    let (_dir, path) = snap_path();
    let dbs = vec![Database::new()];

    // Save with last_lsn = 12345 stamped in.
    shard_snapshot_save_with_lsn(7, 42, 12345, &dbs, &path).unwrap();

    // Peek metadata only — must report the current write version (V3 since
    // phase 200 — adds the hash-field TTL trailer; preamble is unchanged
    // from V2) and the stamped LSN.
    let meta = read_snapshot_metadata(&path).expect("metadata read");
    assert_eq!(meta.version, SHARD_RDB_VERSION);
    assert_eq!(meta.shard_id, 7);
    assert_eq!(meta.epoch, 42);
    assert_eq!(meta.last_lsn, 12345);
    assert!(
        meta.created_at_unix_ms > 0,
        "v2 must stamp a non-zero created_at_unix_ms",
    );

    // Full load must still work.
    let mut loaded = vec![Database::new()];
    let _ = shard_snapshot_load(&mut loaded, &path).unwrap();
}

/// P2 — v1 backward compat: hand-build a v1 (24-byte minimum) snapshot
/// file and confirm both the loader and metadata-peek API accept it,
/// reporting last_lsn = 0 as the lossless fallback.
#[test]
fn test_v1_snapshot_loads_with_zero_lsn() {
    use crc32fast::Hasher;
    let (_dir, path) = snap_path();

    // Build the minimum-valid v1 file: preamble + eof + global_crc.
    let mut buf = Vec::new();
    buf.extend_from_slice(SHARD_RDB_MAGIC);
    buf.push(SHARD_RDB_VERSION_V1);
    buf.extend_from_slice(&3u16.to_le_bytes()); // shard_id
    buf.extend_from_slice(&99u64.to_le_bytes()); // epoch
    buf.push(EOF_MARKER);
    let mut hasher = Hasher::new();
    hasher.update(&buf);
    let crc = hasher.finalize();
    buf.extend_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, &buf).unwrap();

    // Metadata peek must succeed and synthesize last_lsn = 0.
    let meta = read_snapshot_metadata(&path).expect("v1 metadata read");
    assert_eq!(meta.version, SHARD_RDB_VERSION_V1);
    assert_eq!(meta.shard_id, 3);
    assert_eq!(meta.epoch, 99);
    assert_eq!(
        meta.last_lsn, 0,
        "v1 fallback must report last_lsn = 0 (forces full WAL replay)",
    );
    assert_eq!(meta.created_at_unix_ms, 0);

    // Full load on a (degenerate) v1 file must also succeed without error.
    let mut loaded = vec![Database::new()];
    let count = shard_snapshot_load(&mut loaded, &path).unwrap();
    assert_eq!(count, 0);
}

/// P2 — `set_last_lsn` must be observable via `last_lsn()` and reflected
/// in the on-disk header so P3 recovery can read it back without loading
/// the full payload.
#[test]
fn test_set_last_lsn_persists_in_header() {
    let (_dir, path) = snap_path();
    let dbs = vec![Database::new()];

    let mut state = SnapshotState::new(2, 1, &dbs, path.clone());
    state.set_last_lsn(987_654);
    assert_eq!(state.last_lsn(), 987_654);
    while !state.advance_one_segment(&dbs) {}
    state.finalize().unwrap();

    let meta = read_snapshot_metadata(&path).unwrap();
    assert_eq!(meta.last_lsn, 987_654);
}

#[test]
fn test_snapshot_round_trip_string() {
    let (_dir, path) = snap_path();
    let mut dbs = vec![Database::new()];
    dbs[0].set_string(b"k1", Bytes::from_static(b"v1"));
    dbs[0].set_string(b"k2", Bytes::from_static(b"v2"));
    dbs[0].set_string(b"k3", Bytes::from_static(b"v3"));

    shard_snapshot_save(0, 1, &dbs, &path).unwrap();

    let mut loaded = vec![Database::new()];
    let count = shard_snapshot_load(&mut loaded, &path).unwrap();
    assert_eq!(count, 3);
    for key in &[b"k1", b"k2", b"k3"] {
        let entry = loaded[0].get(*key).unwrap();
        match entry.value.as_redis_value() {
            RedisValueRef::String(_) => {}
            _ => panic!("Expected string for key {:?}", key),
        }
    }
}

#[test]
fn test_snapshot_round_trip_all_types() {
    let (_dir, path) = snap_path();
    let mut dbs = vec![Database::new()];

    // String
    dbs[0].set_string(b"str", Bytes::from_static(b"val"));
    // Hash
    {
        let map = dbs[0].get_or_create_hash(b"h").unwrap();
        map.insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
    }
    // List
    {
        let list = dbs[0].get_or_create_list(b"l").unwrap();
        list.push_back(Bytes::from_static(b"item"));
    }
    // Set
    {
        let set = dbs[0].get_or_create_set(b"s").unwrap();
        set.insert(Bytes::from_static(b"m"));
    }
    // Sorted set
    {
        let (members, tree) = dbs[0].get_or_create_sorted_set(b"z").unwrap();
        members.insert(Bytes::from_static(b"a"), 1.0);
        tree.insert(OrderedFloat(1.0), Bytes::from_static(b"a"));
    }

    shard_snapshot_save(0, 1, &dbs, &path).unwrap();

    let mut loaded = vec![Database::new()];
    let count = shard_snapshot_load(&mut loaded, &path).unwrap();
    assert_eq!(count, 5);
    assert_eq!(loaded[0].get(b"str").unwrap().value.type_name(), "string");
    assert_eq!(loaded[0].get(b"h").unwrap().value.type_name(), "hash");
    assert_eq!(loaded[0].get(b"l").unwrap().value.type_name(), "list");
    assert_eq!(loaded[0].get(b"s").unwrap().value.type_name(), "set");
    assert_eq!(loaded[0].get(b"z").unwrap().value.type_name(), "zset");
}

#[test]
fn test_snapshot_with_ttl() {
    let (_dir, path) = snap_path();
    let mut dbs = vec![Database::new()];

    // Key with future TTL
    let future_ms = current_time_ms() + 3_600_000;
    dbs[0].set_string_with_expiry(b"live", Bytes::from_static(b"yes"), future_ms);
    // Key with past TTL (should be skipped)
    let past_ms = current_time_ms() - 1000;
    dbs[0].set(
        b"dead",
        Entry::new_string_with_expiry(Bytes::from_static(b"no"), past_ms),
    );

    shard_snapshot_save(0, 1, &dbs, &path).unwrap();

    let mut loaded = vec![Database::new()];
    let count = shard_snapshot_load(&mut loaded, &path).unwrap();
    assert_eq!(count, 1);
    assert!(loaded[0].get(b"live").is_some());
    assert!(loaded[0].get(b"dead").is_none());
}

#[test]
fn test_snapshot_cow_captures_old_value() {
    let (_dir, path) = snap_path();
    let mut dbs = vec![Database::new()];
    // Insert enough entries across at least 2 segments
    for i in 0..100 {
        dbs[0].set_string(
            &Bytes::from(format!("cow_{:04}", i)),
            Bytes::from(format!("val_{:04}", i)),
        );
    }

    let seg_count = dbs[0].data().segment_count();
    assert!(seg_count > 1, "Need multiple segments for COW test");

    let mut state = SnapshotState::new(0, 1, &dbs, path.to_path_buf());

    // Advance past segment 0
    let done = state.advance_one_segment(&dbs);
    assert!(!done, "Should not be done after first segment");

    // Capture a COW entry for segment 1 (which hasn't been serialized yet)
    // Find a key that lives in segment 1
    let seg1 = dbs[0].data().segment(1);
    let (cow_key, cow_old_entry) = seg1.iter_occupied().next().unwrap();
    let cow_key = cow_key.clone();
    let cow_old_entry = cow_old_entry.clone();

    assert!(state.is_key_pending(0, cow_key.as_bytes()));
    state.capture_cow(0, cow_key.to_bytes(), Some(cow_old_entry));

    // Now overwrite the key in the live database (simulating a write during snapshot)
    dbs[0].set_string(cow_key.as_ref(), Bytes::from_static(b"NEW_VALUE"));

    // Continue advancing until done
    while !state.advance_one_segment(&dbs) {}
    state.finalize().unwrap();

    // Load and verify the COW captured the old value (not the new one)
    let mut loaded = vec![Database::new()];
    let _count = shard_snapshot_load(&mut loaded, &path).unwrap();
    let entry = loaded[0].get(cow_key.as_bytes()).unwrap();
    match entry.value.as_redis_value() {
        RedisValueRef::String(s) => {
            // The snapshot should have the OLD value from COW, not "NEW_VALUE"
            assert_ne!(
                s as &[u8], b"NEW_VALUE",
                "COW should have captured old value"
            );
        }
        _ => panic!("Expected string"),
    }
}

/// moon#517: a key written TWICE inside one snapshot epoch must keep the
/// pre-image taken before the FIRST write. `advance_segment_inner` writes
/// every overflow record for a segment in insertion order and load takes
/// the last one, so an un-deduped second capture silently published a
/// value that already contains the first write — which WAL replay then
/// applies a second time.
///
/// RED before the dedupe in `capture_cow`: the snapshot held "mid".
#[test]
fn test_snapshot_cow_keeps_the_first_pre_image() {
    let (_dir, path) = snap_path();
    let mut dbs = vec![Database::new()];
    for i in 0..100 {
        dbs[0].set_string(
            &Bytes::from(format!("cow_{:04}", i)),
            Bytes::from(format!("val_{:04}", i)),
        );
    }
    let mut state = SnapshotState::new(0, 1, &dbs, path.to_path_buf());

    let seg1 = dbs[0].data().segment(1);
    #[allow(clippy::unwrap_used)]
    let (key, first_entry) = seg1.iter_occupied().next().unwrap();
    let key = key.to_bytes();
    let first_entry = first_entry.clone();
    assert!(state.is_key_pending(0, &key));

    // Write #1: capture the epoch-start value, then mutate.
    state.capture_cow(0, key.clone(), Some(first_entry));
    dbs[0].set_string(&key, Bytes::from_static(b"mid"));

    // Write #2: a second capture of the SAME key, now holding "mid".
    #[allow(clippy::unwrap_used)]
    let second_entry = dbs[0].data().get(&key).unwrap().clone();
    state.capture_cow(0, key.clone(), Some(second_entry));
    dbs[0].set_string(&key, Bytes::from_static(b"NEW_VALUE"));

    while !state.advance_one_segment(&dbs) {}
    #[allow(clippy::unwrap_used)]
    state.finalize().unwrap();

    let mut loaded = vec![Database::new()];
    #[allow(clippy::unwrap_used)]
    let _count = shard_snapshot_load(&mut loaded, &path).unwrap();
    #[allow(clippy::unwrap_used)]
    let entry = loaded[0].get(&key).unwrap();
    match entry.value.as_redis_value() {
        RedisValueRef::String(s) => {
            assert_ne!(s as &[u8], b"mid", "second capture must not win");
            assert_ne!(s as &[u8], b"NEW_VALUE", "live value must not win");
        }
        _ => panic!("Expected string"),
    }
}

#[test]
fn test_snapshot_per_segment_crc32() {
    let (_dir, path) = snap_path();
    let mut dbs = vec![Database::new()];
    // Use a longer value so we can corrupt a data byte without hitting a tag
    dbs[0].set_string(b"testkey", Bytes::from_static(b"testvalue_long_enough"));

    shard_snapshot_save(0, 1, &dbs, &path).unwrap();

    let mut data = std::fs::read(&path).unwrap();
    // Layout: header(19) + DB_SELECTOR(1) + db_idx(1) + SEGMENT_BLOCK_MARKER(1)
    //       + seg_idx(4) + entry_count(4) + [entry_data...] + seg_crc(4) + EOF(1) + global_crc(4)
    // Entry data starts at offset 30. The entry:
    //   type_tag(1) + key_len(4) + "testkey"(7) + ttl(8) + val_len(4) + "testvalue_long_enough"(21) = 45 bytes
    // Value string bytes start at offset 30+1+4+7+8+4 = 54, end at 75
    // Corrupt a byte in the value string area (offset 60)
    let corrupt_offset = 60;
    assert!(
        corrupt_offset < data.len() - 8,
        "File too small for corruption test"
    );
    data[corrupt_offset] ^= 0xFF;

    // Recalculate the global CRC to isolate the segment CRC check
    let payload_len = data.len() - 4;
    let mut hasher = Hasher::new();
    hasher.update(&data[..payload_len]);
    let new_global_crc = hasher.finalize();
    data[payload_len..].copy_from_slice(&new_global_crc.to_le_bytes());
    std::fs::write(&path, &data).unwrap();

    let mut loaded = vec![Database::new()];
    // Per-segment CRC mismatch now uses log+skip recovery:
    // the corrupted segment is skipped but loading continues successfully.
    let result = shard_snapshot_load(&mut loaded, &path);
    assert!(
        result.is_ok(),
        "Per-segment CRC mismatch should log+skip, not hard-fail"
    );
    let count = result.unwrap();
    assert_eq!(
        count, 0,
        "Corrupted segment should be skipped, yielding 0 keys"
    );
}

#[test]
fn test_snapshot_multi_database() {
    let (_dir, path) = snap_path();
    let mut dbs = vec![Database::new(), Database::new()];
    dbs[0].set_string(b"db0_k", Bytes::from_static(b"v0"));
    dbs[1].set_string(b"db1_k", Bytes::from_static(b"v1"));

    shard_snapshot_save(0, 1, &dbs, &path).unwrap();

    let mut loaded = vec![Database::new(), Database::new()];
    let count = shard_snapshot_load(&mut loaded, &path).unwrap();
    assert_eq!(count, 2);
    assert!(loaded[0].get(b"db0_k").is_some());
    assert!(loaded[1].get(b"db1_k").is_some());
}

#[test]
fn test_snapshot_empty_database() {
    let (_dir, path) = snap_path();
    let dbs = vec![Database::new()];

    shard_snapshot_save(0, 1, &dbs, &path).unwrap();

    let mut loaded = vec![Database::new()];
    let count = shard_snapshot_load(&mut loaded, &path).unwrap();
    assert_eq!(count, 0);
    assert_eq!(loaded[0].len(), 0);
}

#[test]
fn test_advance_one_segment_yields_between_segments() {
    let mut dbs = vec![Database::new()];
    // Insert enough entries to have multiple segments
    for i in 0..100 {
        dbs[0].set_string(
            &Bytes::from(format!("yield_{:04}", i)),
            Bytes::from(format!("v_{:04}", i)),
        );
    }

    let seg_count = dbs[0].data().segment_count();
    assert!(seg_count > 1, "Need multiple segments, got {}", seg_count);

    let (_dir, path) = snap_path();
    let mut state = SnapshotState::new(0, 1, &dbs, path.to_path_buf());

    // First advance should return false (not done -- more segments to process)
    let done = state.advance_one_segment(&dbs);
    assert!(
        !done,
        "First advance should not complete with {} segments",
        seg_count
    );

    // Advance all remaining segments
    let mut advances = 1;
    while !state.advance_one_segment(&dbs) {
        advances += 1;
    }
    advances += 1; // count the final true-returning call... actually the last true call is when advance_one_segment returned true

    // Total advances should equal segment count
    assert_eq!(
        advances, seg_count,
        "Should need exactly {} advances, got {}",
        seg_count, advances
    );

    assert!(state.is_complete());
}

/// The event loop's finalize (moon#1186): begin, then poll without
/// blocking until the writer thread has published a loadable file.
#[test]
fn test_begin_and_poll_finalize_publishes_a_valid_file() {
    let (_dir, path) = snap_path();
    let mut dbs = vec![Database::new()];
    dbs[0].set_string(b"async_k1", Bytes::from_static(b"async_v1"));
    dbs[0].set_string(b"async_k2", Bytes::from_static(b"async_v2"));

    let mut state = SnapshotState::new(0, 1, &dbs, path.to_path_buf());
    while !state.advance_one_segment(&dbs) {}
    state.begin_finalize().unwrap();
    assert!(state.finalize_started());
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let outcome = loop {
        if let Some(r) = state.poll_finalize() {
            break r;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "finalize never completed"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    };
    outcome.unwrap();

    // Verify file was written and is loadable
    assert!(path.exists(), "Snapshot file should exist after finalize");
    assert!(
        !path.with_extension("rrdshard.tmp").exists(),
        "temp file must be renamed away"
    );
    let mut loaded = vec![Database::new()];
    let count = shard_snapshot_load(&mut loaded, &path).unwrap();
    assert_eq!(count, 2);
    assert!(loaded[0].get(b"async_k1").is_some());
    assert!(loaded[0].get(b"async_k2").is_some());
}

/// Locates the byte offset of the `entry_count` field within the first
/// segment block that actually carries entries (skipping empty
/// segments, which are still legally written for near-empty
/// databases). Mirrors the writer's known layout — see
/// `write_header_if_needed` / `advance_segment_inner` above.
fn find_entry_count_offset(data: &[u8]) -> usize {
    let version = data[8];
    let preamble_len = if version == SHARD_RDB_VERSION_V1 {
        19
    } else {
        35
    };
    let mut pos = preamble_len;
    loop {
        let tag = data[pos];
        pos += 1;
        match tag {
            EOF_MARKER => panic!("reached EOF before finding a non-empty segment"),
            DB_SELECTOR => pos += 1, // db_idx byte
            SEGMENT_BLOCK_MARKER => {
                let entry_count =
                    u32::from_le_bytes(data[pos + 4..pos + 8].try_into().expect("4 bytes"));
                if entry_count > 0 {
                    return pos + 4;
                }
                // Empty segment: seg_idx(4) + entry_count(4) + data(0) + crc(4).
                pos += 12;
            }
            other => panic!("unexpected tag byte {other:#x}"),
        }
    }
}

/// Security regression (untrusted-input DoS): a crafted/corrupt segment
/// block that lies about its `entry_count` (e.g. claims u32::MAX
/// entries while the file has only a handful of bytes left) must be
/// rejected before `Vec::with_capacity(entry_count as usize)` runs.
///
/// Before the fix, this allocation had no bound at all: a hostile or
/// corrupt snapshot file on the server-startup / replica-full-sync path
/// would drive a multi-gigabyte allocation before a single entry byte
/// was read, aborting the process (release builds use `panic =
/// "abort"`) or OOM-killing it.
#[test]
fn test_segment_entry_count_dos_rejected() {
    let (_dir, path) = snap_path();
    let mut dbs = vec![Database::new()];
    dbs[0].set_string(b"k", Bytes::from_static(b"v"));
    shard_snapshot_save(0, 1, &dbs, &path).unwrap();

    let mut data = std::fs::read(&path).unwrap();
    let entry_count_off = find_entry_count_offset(&data);
    data[entry_count_off..entry_count_off + 4].copy_from_slice(&u32::MAX.to_le_bytes());

    // Recompute the global CRC32 so the corruption is caught by the
    // entry_count bound check, not the outer whole-file checksum gate.
    let payload_len = data.len() - 4;
    let mut hasher = Hasher::new();
    hasher.update(&data[..payload_len]);
    let new_global_crc = hasher.finalize();
    data[payload_len..].copy_from_slice(&new_global_crc.to_le_bytes());
    std::fs::write(&path, &data).unwrap();

    let mut loaded = vec![Database::new()];
    let result = shard_snapshot_load(&mut loaded, &path);
    assert!(
        result.is_err(),
        "a lying entry_count must be rejected with a clean error, not drive an unbounded allocation"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("segment_entries") && err.contains("exceeds remaining data"),
        "expected the validate_count bounds-check error, got: {err}"
    );
}
