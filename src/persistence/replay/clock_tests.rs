//! moon#1277 through the production flat-file replay (`aof::replay_aof`, the
//! tokio `--shards 1` layout; the multi-part and per-shard replays pin the
//! same clock from their incr file).

use std::time::{Duration, SystemTime};

use crate::persistence::replay::DispatchReplayEngine;
use crate::storage::Database;
use crate::storage::entry::current_time_ms;

fn resp(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Replay `records` from an `appendonly.aof` last modified at `mtime_ms`.
fn replay_written_at(records: &[&[&[u8]]], mtime_ms: u64) -> Database {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("appendonly.aof");
    let mut aof = Vec::new();
    for r in records {
        aof.extend_from_slice(&resp(r));
    }
    std::fs::write(&path, &aof).expect("write aof");
    std::fs::File::options()
        .write(true)
        .open(&path)
        .expect("open aof")
        .set_modified(SystemTime::UNIX_EPOCH + Duration::from_millis(mtime_ms))
        .expect("set mtime");
    let mut dbs = vec![Database::new()];
    crate::persistence::aof::replay_aof(&mut dbs, &path, &DispatchReplayEngine::new())
        .expect("replay");
    dbs.remove(0)
}

/// The raw hot entry (value, TTL), without any expiry judgment.
fn raw(db: &Database, key: &[u8]) -> Option<(Vec<u8>, u64)> {
    let e = db.data().get(key)?;
    Some((
        e.value
            .as_bytes_owned()
            .map(|b| b.to_vec())
            .unwrap_or_default(),
        e.expires_at_ms(),
    ))
}

/// moon#1277: `SET s v PXAT T` then `APPEND s x`, both written while `s` was
/// alive (the log's last write is before `T`); the restart comes after `T`.
/// The APPEND must land on `v` and keep `T` (so the key expires), not build
/// a new persistent `x`.
#[test]
fn an_rmw_logged_before_the_deadline_replays_onto_the_live_value() {
    let now = current_time_ms();
    let deadline = now - 2_000;
    let written = now - 5_000;
    let t = deadline.to_string();
    let db = replay_written_at(
        &[
            &[b"SET", b"s", b"v", b"PXAT", t.as_bytes()],
            &[b"APPEND", b"s", b"x"],
            &[b"SET", b"n", b"5", b"PXAT", t.as_bytes()],
            &[b"INCR", b"n"],
        ],
        written,
    );
    assert_eq!(
        (raw(&db, b"s"), raw(&db, b"n")),
        (
            Some((b"vx".to_vec(), deadline)),
            Some((b"6".to_vec(), deadline))
        ),
        "the RMWs must replay onto the value they were written against and keep its TTL \
         (a new persistent key comes back and never expires)"
    );
    assert_eq!(
        crate::persistence::replay::clock::pinned_replay_clock_ms(),
        None,
        "the pin ends with the replay"
    );
    assert!(
        db.now_ms() >= now,
        "the databases are handed back on the wall clock"
    );
}

/// The other side, unchanged: a write logged AFTER the key's deadline (the
/// log's last write is later than `T`) saw it expired live — `INCR` of an
/// expired counter started a fresh one — and must replay the same way.
#[test]
fn a_write_logged_after_the_deadline_still_sees_the_key_expired() {
    let now = current_time_ms();
    let deadline = now - 3_000;
    let t = deadline.to_string();
    let db = replay_written_at(
        &[
            &[b"SET", b"n", b"5", b"PXAT", t.as_bytes()],
            &[b"INCR", b"n"],
        ],
        now - 1_000,
    );
    assert_eq!(raw(&db, b"n"), Some((b"1".to_vec(), 0)));
}

fn touch(path: &std::path::Path, mtime_ms: u64) {
    std::fs::write(path, b"x").expect("write");
    std::fs::File::options()
        .write(true)
        .open(path)
        .expect("open")
        .set_modified(SystemTime::UNIX_EPOCH + Duration::from_millis(mtime_ms))
        .expect("set mtime");
}

/// The WAL passes pin to the newest `*.wal` segment; a modification time in
/// the future is not trusted past the wall clock.
#[test]
fn the_wal_pin_is_the_newest_segment_capped_at_the_wall_clock() {
    let now = current_time_ms();
    let dir = tempfile::tempdir().expect("tempdir");
    touch(&dir.path().join("000001.wal"), now - 9_000);
    touch(&dir.path().join("000002.wal"), now - 4_000);
    touch(&dir.path().join("checkpoint.meta"), now - 1_000);
    {
        let _pin = super::pin_replay_clock_to_wal_dir(dir.path());
        assert_eq!(super::pinned_replay_clock_ms(), Some(now - 4_000));
    }
    assert_eq!(super::pinned_replay_clock_ms(), None);
    touch(&dir.path().join("000003.wal"), now + 3_600_000);
    let _pin = super::pin_replay_clock_to_wal_dir(dir.path());
    let pinned = super::pinned_replay_clock_ms().expect("pinned");
    assert!(
        pinned >= now && pinned <= current_time_ms(),
        "a future mtime is capped at the wall clock: {pinned}"
    );
}

/// REVIEW-FINAL-P5B item 4 (b): with KV logs replayed over a snapshot
/// (`KvSources::SnapshotAndLogs`, here the legacy `appendonly.aof` of the
/// non-offload boot), the snapshot's loader skipped a key whose TTL passed
/// while the server was down, on the WALL clock — while the log replayed over
/// it judges on the pinned clock, to which the key was alive: `APPEND s x`
/// then built a new persistent `x`. Kept, it lands on `v` and keeps its TTL.
#[test]
fn a_snapshot_under_replayed_logs_keeps_a_key_the_log_saw_alive() {
    use crate::storage::entry::Entry;
    let dir = tempfile::tempdir().expect("tempdir");
    let deadline = current_time_ms() + 400;
    let mut image = vec![Database::new()];
    image[0].set(
        b"s",
        Entry::new_string_with_expiry(bytes::Bytes::from_static(b"v"), deadline),
    );
    crate::persistence::snapshot::shard_snapshot_save(
        0,
        1,
        &image,
        &dir.path().join("shard-0.rrdshard"),
    )
    .expect("save");
    let aof = dir.path().join("appendonly.aof");
    std::fs::write(&aof, resp(&[b"APPEND", b"s", b"x"])).expect("write aof");
    touch_mtime(&aof, deadline - 200);
    while current_time_ms() <= deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    let config = crate::config::RuntimeConfig::default(); // appendonly yes
    let mut shard = crate::shard::Shard::new(0, 1, 1, config);
    shard.restore_from_persistence(dir.path().to_str().expect("utf8"), None, false);
    assert_eq!(
        raw(&shard.databases[0], b"s"),
        Some((b"vx".to_vec(), deadline)),
        "the APPEND must land on the snapshot's value and keep its TTL"
    );
}

/// REVIEW-FINAL-P5B item 4 (a): the last-resort WAL replay (no AOF at all)
/// judges expiry by its newest segment's time too.
#[test]
fn the_last_resort_wal_replay_judges_by_the_log_time() {
    use crate::persistence::wal_v3::record::{WalRecordType, write_wal_v3_record};
    let dir = tempfile::tempdir().expect("tempdir");
    let now = current_time_ms();
    let deadline = now - 2_000;
    let t = deadline.to_string();
    let mut wal = vec![0u8; 64];
    wal[0..6].copy_from_slice(b"RRDWAL");
    wal[6] = 3;
    let set = resp(&[b"SET", b"s", b"v", b"PXAT", t.as_bytes()]);
    write_wal_v3_record(&mut wal, 1, WalRecordType::Command, &set);
    write_wal_v3_record(
        &mut wal,
        2,
        WalRecordType::Command,
        &resp(&[b"APPEND", b"s", b"x"]),
    );
    let segment = dir.path().join("000000000001.wal");
    std::fs::write(&segment, &wal).expect("write wal");
    touch_mtime(&segment, now - 5_000);
    let mut dbs = vec![Database::new()];
    crate::persistence::wal_v3::replay::replay_wal_v3_dir_commands(
        dir.path(),
        &mut dbs,
        &DispatchReplayEngine::new(),
    )
    .expect("replay");
    assert_eq!(raw(&dbs[0], b"s"), Some((b"vx".to_vec(), deadline)));
}

fn touch_mtime(path: &std::path::Path, mtime_ms: u64) {
    std::fs::File::options()
        .write(true)
        .open(path)
        .expect("open")
        .set_modified(SystemTime::UNIX_EPOCH + Duration::from_millis(mtime_ms))
        .expect("set mtime");
}
