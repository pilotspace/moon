//! moon#1186 — the incremental snapshot streams to an off-shard-thread
//! writer, finalizes there, and keeps its copy-on-write overflow per segment.

use super::*;
use crate::storage::compact_value::RedisValueRef;

fn dataset() -> Vec<Database> {
    let mut dbs: Vec<Database> = (0..3).map(|_| Database::new()).collect();
    for i in 0..12000u32 {
        dbs[0].set_string(
            format!("k:{i:05}").as_bytes(),
            Bytes::from(format!("v-{}-{}", i, "p".repeat((i % 50) as usize))),
        );
    }
    // db 1 empty, db 2 small.
    for i in 0..40u32 {
        dbs[2].set_string(format!("d2:{i}").as_bytes(), Bytes::from(vec![b'z'; 300]));
    }
    dbs
}

fn wait_finalized(state: &SnapshotState) -> Result<(), String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        if let Some(r) = state.poll_finalize() {
            return r;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "finalize never completed"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// First key stored in segment `seg` of `db`.
fn key_in_segment(db: &Database, seg: usize) -> Bytes {
    db.data()
        .segment(seg)
        .iter_occupied()
        .next()
        .expect("segment has a key")
        .0
        .to_bytes()
}

/// HEAD `935c555`'s segment block encoder, verbatim (staging `Vec`, flat
/// overflow scan): the oracle for the in-place encoder.
fn head_segment_block(db: &Database, seg_idx: usize, overflow: &[(Bytes, Entry)]) -> Vec<u8> {
    let now_ms = current_time_ms();
    let segment = db.data().segment(seg_idx);
    let overflow_keys: HashSet<&[u8]> = overflow.iter().map(|(k, _)| k.as_ref()).collect();
    let mut segment_entries: Vec<u8> = Vec::new();
    let mut entry_count: u32 = 0;
    for (key, entry) in overflow {
        if entry.has_expiry() && entry.is_expired_at(now_ms) {
            continue;
        }
        rdb::write_entry(&mut segment_entries, key, entry).unwrap();
        entry_count += 1;
    }
    for (key, entry) in segment.iter_occupied() {
        if overflow_keys.contains(key.as_bytes()) {
            continue;
        }
        if entry.has_expiry() && entry.is_expired_at(now_ms) {
            continue;
        }
        rdb::write_entry(&mut segment_entries, key.as_bytes(), entry).unwrap();
        entry_count += 1;
    }
    let mut out = vec![SEGMENT_BLOCK_MARKER];
    out.extend_from_slice(&(seg_idx as u32).to_le_bytes());
    out.extend_from_slice(&entry_count.to_le_bytes());
    let mut hasher = Hasher::new();
    hasher.update(&segment_entries);
    out.extend_from_slice(&segment_entries);
    out.extend_from_slice(&hasher.finalize().to_le_bytes());
    out
}

/// The in-place segment encoder writes HEAD's exact block bytes, overflow
/// pre-images included.
#[test]
fn segment_block_bytes_match_head() {
    let dir = tempfile::tempdir().unwrap();
    let dbs = dataset();
    let mut state = SnapshotState::new(0, 1, &dbs, dir.path().join("s.rrdshard"));
    let victim = key_in_segment(&dbs[0], 0);
    let pre = dbs[0].data().get(&victim).unwrap().clone();
    state.capture_cow(0, victim.clone(), Some(pre.clone()));
    state.advance_one_segment(&dbs);
    // header (35) + db selector (2) precede the first block.
    let block = &state.output_buf[37..];
    assert_eq!(block, &head_segment_block(&dbs[0], 0, &[(victim, pre)])[..]);
}

/// A streamed snapshot (blocks handed to the writer thread as they fill,
/// footer + fsync + rename there) is byte-identical to the in-memory one.
#[test]
fn streamed_snapshot_is_byte_identical_to_in_memory_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let dbs = dataset();
    let mem_path = dir.path().join("mem.rrdshard");
    let str_path = dir.path().join("str.rrdshard");
    let mut mem = SnapshotState::new(3, 9, &dbs, mem_path.clone());
    let mut streamed = SnapshotState::new(3, 9, &dbs, str_path.clone());
    streamed.created_at_unix_ms = mem.created_at_unix_ms;
    mem.set_last_lsn(77);
    streamed.set_last_lsn(77);
    streamed.start_streaming().unwrap();

    // Same COW pre-images in pending segments of both.
    let count = dbs[0].data().segment_count();
    for seg in [1usize, count / 2, count - 1] {
        let k = key_in_segment(&dbs[0], seg);
        let e = dbs[0].data().get(&k).unwrap().clone();
        mem.capture_cow(0, k.clone(), Some(e.clone()));
        streamed.capture_cow(0, k, Some(e));
    }

    while !mem.advance_one_segment(&dbs) {}
    let mut peak_buf = 0usize;
    while !streamed.advance_one_segment(&dbs) {
        peak_buf = peak_buf.max(streamed.output_buf.len());
    }
    // Blocks leave the shard as they fill: the shard-side buffer never holds
    // more than a chunk plus one segment (it used to hold the whole file).
    assert!(
        peak_buf
            < crate::persistence::snapshot_stream::SNAPSHOT_STREAM_CHUNK + SNAPSHOT_STREAM_PROBE,
        "shard-side buffer grew to {peak_buf}"
    );
    mem.finalize().unwrap();
    streamed.begin_finalize().unwrap();
    wait_finalized(&streamed).unwrap();

    let a = std::fs::read(&mem_path).unwrap();
    let b = std::fs::read(&str_path).unwrap();
    assert!(a.len() > 2 * crate::persistence::snapshot_stream::SNAPSHOT_STREAM_CHUNK);
    assert_eq!(a, b, "streamed file differs from the in-memory file");
    assert!(!str_path.with_extension("rrdshard.tmp").exists());
    assert_eq!(streamed.stream_in_flight(), 0, "every block accounted for");

    let mut loaded: Vec<Database> = (0..3).map(|_| Database::new()).collect();
    assert_eq!(
        shard_snapshot_load(&mut loaded, &str_path).unwrap(),
        12040,
        "every key loads back"
    );
}

/// Output-buffer high-water mark while streaming: well under one chunk plus
/// one segment (the whole file used to sit here until finalize).
const SNAPSHOT_STREAM_PROBE: usize = 64 * 1024;

/// COW pre-images live per segment and are MOVED out when their segment is
/// written — no rescan of the whole overflow, no clone, and the dedupe set
/// shrinks with it.
#[test]
fn overflow_is_moved_out_per_segment() {
    let dir = tempfile::tempdir().unwrap();
    let mut dbs = dataset();
    let path = dir.path().join("cow.rrdshard");
    let mut state = SnapshotState::new(0, 1, &dbs, path.clone());
    let seg_count = dbs[0].data().segment_count();
    let mut victims = Vec::new();
    for seg in 0..seg_count {
        let k = key_in_segment(&dbs[0], seg);
        let e = dbs[0].data().get(&k).unwrap().clone();
        state.capture_cow(0, k.clone(), Some(e));
        victims.push(k);
    }
    assert_eq!(state.pending_pre_images(), seg_count);
    for k in &victims {
        dbs[0].set_string(k, Bytes::from_static(b"OVERWRITTEN"));
    }
    for done in 0..seg_count {
        state.advance_one_segment(&dbs);
        // One victim per segment: each advance moves exactly the written
        // segment's pre-image out (moon#1216 walks segments in hash order,
        // not store order, so which one is irrelevant).
        assert_eq!(
            state.pending_pre_images(),
            seg_count - done - 1,
            "advance {done} did not move its pre-image out"
        );
    }
    while !state.advance_one_segment(&dbs) {}
    state.finalize().unwrap();
    let mut loaded: Vec<Database> = (0..3).map(|_| Database::new()).collect();
    shard_snapshot_load(&mut loaded, &path).unwrap();
    for k in &victims {
        match loaded[0].get(k).unwrap().value.as_redis_value() {
            RedisValueRef::String(s) => assert_ne!(s as &[u8], b"OVERWRITTEN"),
            _ => panic!("expected string"),
        }
    }
}

/// A writer thread that cannot create its temp file fails the snapshot
/// loudly — the tick sees `stream_failed`, finalize reports the error, and
/// nothing is published.
#[test]
fn writer_failure_is_reported_not_published() {
    let dir = tempfile::tempdir().unwrap();
    let dbs = dataset();
    let path = dir.path().join("missing-dir").join("s.rrdshard");
    let mut state = SnapshotState::new(0, 1, &dbs, path.clone());
    state.start_streaming().unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !state.stream_failed() {
        assert!(
            std::time::Instant::now() < deadline,
            "failure never surfaced"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    state.begin_finalize().unwrap();
    assert!(wait_finalized(&state).is_err());
    assert!(!path.exists());
}

/// A snapshot dropped before finalize (shard exit, error) leaves no temp
/// file behind: the writer thread sees the channel close and deletes it.
#[test]
fn abandoned_snapshot_removes_its_temp_file() {
    let dir = tempfile::tempdir().unwrap();
    let dbs = dataset();
    let path = dir.path().join("s.rrdshard");
    let tmp = path.with_extension("rrdshard.tmp");
    let mut state = SnapshotState::new(0, 1, &dbs, path.clone());
    state.start_streaming().unwrap();
    for _ in 0..8 {
        state.advance_one_segment(&dbs);
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !tmp.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "temp file never created"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    drop(state);
    while tmp.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "temp file left behind"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert!(
        !path.exists(),
        "an abandoned snapshot must never be published"
    );
}
