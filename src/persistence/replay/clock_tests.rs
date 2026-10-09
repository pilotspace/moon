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
/// (`KvSources::SnapshotAndLogs` — since R2b review P1 only without an
/// `appendonly.aof` holding a record, here the non-offload boot's last-resort
/// WAL v3), the snapshot's loader skipped a key whose TTL passed while the
/// server was down, on the WALL clock — while the log replayed over it judges
/// on the pinned clock, to which the key was alive: `APPEND s x` then built a
/// new persistent `x`. Kept, it lands on `v` and keeps its TTL.
#[test]
fn a_snapshot_under_replayed_logs_keeps_a_key_the_log_saw_alive() {
    use crate::persistence::wal_v3::record::{WalRecordType, write_wal_v3_record};
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
    let wal_dir = dir.path().join("shard-0").join("wal-v3");
    std::fs::create_dir_all(&wal_dir).expect("wal dir");
    let mut wal = vec![0u8; 64];
    wal[0..6].copy_from_slice(b"RRDWAL");
    wal[6] = 3;
    write_wal_v3_record(
        &mut wal,
        1,
        WalRecordType::Command,
        &resp(&[b"APPEND", b"s", b"x"]),
    );
    let segment = wal_dir.join("000000000001.wal");
    std::fs::write(&segment, &wal).expect("write wal");
    touch_mtime(&segment, deadline - 200);
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

/// R2b review P1: a flat `appendonly.aof` holding a record is the only KV
/// source, so it is never replayed over a snapshot — a snapshot saved while it
/// was appended holds a prefix of it. The image's `s` is not loaded.
#[test]
fn a_flat_aof_is_not_replayed_over_the_snapshot() {
    use crate::storage::entry::Entry;
    let dir = tempfile::tempdir().expect("tempdir");
    let mut image = vec![Database::new()];
    image[0].set(b"s", Entry::new_string(bytes::Bytes::from_static(b"v")));
    crate::persistence::snapshot::shard_snapshot_save(
        0,
        1,
        &image,
        &dir.path().join("shard-0.rrdshard"),
    )
    .expect("save");
    std::fs::write(
        dir.path().join("appendonly.aof"),
        resp(&[b"APPEND", b"s", b"x"]),
    )
    .expect("write aof");
    let config = crate::config::RuntimeConfig::default(); // appendonly yes
    let mut shard = crate::shard::Shard::new(0, 1, 1, config);
    shard.restore_from_persistence(dir.path().to_str().expect("utf8"), None, false);
    assert_eq!(
        raw(&shard.databases[0], b"s").map(|(v, _)| v),
        Some(b"x".to_vec())
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

// ── R2 review of moon#1283: the positional foreign-segment rule ─────────────

/// The three production log readers.
#[derive(Clone, Copy, Debug)]
enum Layout {
    /// `aof::replay_aof`: the flat `appendonly.aof`.
    Flat,
    /// The multi-part top-level incr (bare RESP).
    Incr,
    /// The per-shard incr (`[u64 lsn][u32 len][RESP]`).
    Framed,
}

const LAYOUTS: [Layout; 3] = [Layout::Flat, Layout::Incr, Layout::Framed];

fn ts(ms: u64) -> Vec<u8> {
    crate::persistence::replay::pseudo::TsRecord::new(ms)
        .as_bytes()
        .to_vec()
}

fn close(ms: u64) -> Vec<u8> {
    crate::persistence::replay::pseudo::TsRecord::close(ms)
        .as_bytes()
        .to_vec()
}

fn cmd(parts: &[&str]) -> Vec<u8> {
    let parts: Vec<&[u8]> = parts.iter().map(|p| p.as_bytes()).collect();
    resp(&parts)
}

/// Replay `records` from a file of `layout` last modified at `mtime_ms`,
/// through the production reader. Returns the database and the file's path
/// (kept alive by the returned tempdir).
fn replay_layout(
    layout: Layout,
    records: &[Vec<u8>],
    mtime_ms: u64,
) -> (Database, std::path::PathBuf, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("log.aof");
    let mut bytes = Vec::new();
    for (i, r) in records.iter().enumerate() {
        if matches!(layout, Layout::Framed) {
            bytes.extend_from_slice(&(i as u64 + 1).to_le_bytes());
            bytes.extend_from_slice(&(r.len() as u32).to_le_bytes());
        }
        bytes.extend_from_slice(r);
    }
    std::fs::write(&path, &bytes).expect("write log");
    touch_mtime(&path, mtime_ms);
    let mut dbs = vec![Database::new()];
    let engine = DispatchReplayEngine::new();
    use crate::persistence::aof_manifest::shard_replay::fuzz;
    let replayed = match layout {
        Layout::Flat => crate::persistence::aof::replay_aof(&mut dbs, &path, &engine).is_ok(),
        Layout::Incr => fuzz::replay_resp_file(&mut dbs, &path, &engine).is_some(),
        Layout::Framed => fuzz::replay_framed_file(&mut dbs, &path, &engine).is_some(),
    };
    assert!(replayed, "{layout:?}: the log replays");
    assert_eq!(
        super::pinned_replay_clock_ms(),
        None,
        "the pin ends with it"
    );
    (dbs.remove(0), path, dir)
}

/// A downgrade in the MIDDLE of a file: this binary closed it cleanly
/// (`CLOSE` at `ta + 100`), an older binary appended with no stamp — it saw
/// `n` expire and `INCR` restarted it at 1 — and a later session of this
/// binary stamped `tb` before its first record. The segment is judged by
/// `tb`: `n` is `1` and persistent, as it was live. Judged by the stale
/// stamp (the pre-R1 replay) the `INCR` landed on `5` with the old deadline
/// and the key was lost; the file's mtime (much later) is irrelevant, and the
/// later session's own records keep their own stamps (`p`).
#[test]
fn a_segment_after_a_close_is_judged_by_the_next_stamp() {
    let now = current_time_ms();
    let ta = now - 600_000;
    let n_deadline = ta + 2_000;
    let tb = ta + 60_000;
    let p_deadline = tb + 50;
    for layout in LAYOUTS {
        let (db, path, _dir) = replay_layout(
            layout,
            &[
                ts(ta),
                cmd(&["SET", "n", "5", "PXAT", &n_deadline.to_string()]),
                close(ta + 100),
                // The older binary's segment: n had expired live.
                cmd(&["INCR", "n"]),
                // The later session of this binary.
                ts(tb),
                cmd(&["SET", "p", "5", "PXAT", &p_deadline.to_string()]),
                cmd(&["INCR", "p"]),
            ],
            now - 1_000,
        );
        assert_eq!(
            (raw(&db, b"n"), raw(&db, b"p")),
            (Some((b"1".to_vec(), 0)), Some((b"6".to_vec(), p_deadline))),
            "{layout:?}"
        );
        assert_eq!(
            super::take_open_foreign_segment(&path),
            None,
            "{layout:?}: a segment that ends at a stamp is nobody's business later"
        );
    }
}

/// A segment that runs to the END of the file (the re-upgrade boot itself)
/// is judged by the time the file was last written — `max(close, mtime
/// pin)` — and that judgment is handed to the writer that reopens the file.
/// A mtime moved BEFORE the close never judges earlier than the close.
#[test]
fn a_segment_at_the_end_of_the_file_is_judged_by_its_last_write() {
    let now = current_time_ms();
    let t = now - 600_000;
    let deadline = t + 2_000;
    for layout in LAYOUTS {
        let records = [
            ts(t),
            cmd(&["SET", "n", "5", "PXAT", &deadline.to_string()]),
            close(t + 10),
            cmd(&["INCR", "n"]),
        ];
        let appended = t + 30_000;
        let (db, path, _dir) = replay_layout(layout, &records, appended);
        assert_eq!(raw(&db, b"n"), Some((b"1".to_vec(), 0)), "{layout:?}");
        assert_eq!(
            super::take_open_foreign_segment(&path),
            Some(appended),
            "{layout:?}"
        );
        // Touched an hour back: the close is the floor.
        let (db, path, _dir) = replay_layout(layout, &records, t - 3_600_000);
        assert_eq!(
            raw(&db, b"n"),
            Some((b"6".to_vec(), deadline)),
            "{layout:?}: judged at the close, n was alive"
        );
        assert_eq!(super::take_open_foreign_segment(&path), Some(t + 10));
    }
}

/// Every graceful stop leaves a `CLOSE`; each opens its own (possibly empty)
/// segment, ended by the next stamp — a later session's stamp or another
/// `CLOSE`. Two restarts with no write between them leave two markers in a
/// row: an empty segment.
#[test]
fn multiple_close_markers_each_open_their_own_segment() {
    let now = current_time_ms();
    let t = now - 600_000;
    let (a, b) = (t + 1_000, t + 50_000);
    for layout in LAYOUTS {
        let (db, path, _dir) = replay_layout(
            layout,
            &[
                ts(t),
                cmd(&["SET", "a", "5", "PXAT", &a.to_string()]),
                cmd(&["SET", "b", "5", "PXAT", &b.to_string()]),
                close(t + 10),
                close(t + 20), // restarted, nothing written, stopped again
                // First foreign segment: judged by the next stamp (t + 5 s).
                cmd(&["INCR", "a"]),
                cmd(&["INCR", "b"]),
                ts(t + 5_000),
                close(t + 5_010),
                // Second foreign segment, ended by a CLOSE (t + 60 s): b too
                // had expired by then.
                cmd(&["INCR", "b"]),
                close(t + 60_000),
            ],
            now - 1_000,
        );
        assert_eq!(
            (raw(&db, b"a"), raw(&db, b"b")),
            (Some((b"1".to_vec(), 0)), Some((b"1".to_vec(), 0))),
            "{layout:?}"
        );
        assert_eq!(super::take_open_foreign_segment(&path), None);
    }
}

/// A `CLOSE` followed at once by a stamp is an empty segment: the records
/// after that stamp are this binary's, judged by it — and a `CLOSE` at the
/// very end of the file (a clean stop) opens nothing either.
#[test]
fn a_close_followed_by_a_stamp_is_an_empty_segment() {
    let now = current_time_ms();
    let t = now - 600_000;
    let deadline = t + 50;
    for layout in LAYOUTS {
        let (db, path, _dir) = replay_layout(
            layout,
            &[
                ts(t),
                cmd(&["SET", "n", "5", "PXAT", &deadline.to_string()]),
                close(t + 10),
                ts(t + 20),
                cmd(&["INCR", "n"]),
                close(t + 30),
            ],
            now - 1_000,
        );
        assert_eq!(
            raw(&db, b"n"),
            Some((b"6".to_vec(), deadline)),
            "{layout:?}"
        );
        assert_eq!(super::take_open_foreign_segment(&path), None);
    }
}

/// NEW-A (R2 review): the file's last clock tick is never re-judged by a
/// moved mtime. `SET k 10 PX 600; INCR k`, killed, then the file touched
/// forward an hour (a `cp`, a restore): with no `CLOSE` — or with the
/// `CLOSE` a clean stop leaves at the very end — nothing is foreign, so `k`
/// replays as 11 with its deadline (and has expired), never as a
/// persistent 1.
#[test]
fn a_moved_mtime_never_rejudges_the_last_clock_tick() {
    let now = current_time_ms();
    let t = now - 600_000;
    let deadline = t + 600;
    for layout in LAYOUTS {
        for clean in [false, true] {
            let mut records = vec![
                ts(t),
                cmd(&["SET", "k", "10", "PXAT", &deadline.to_string()]),
                cmd(&["INCR", "k"]),
            ];
            if clean {
                records.push(close(t + 100));
            }
            let (db, path, _dir) = replay_layout(layout, &records, now + 3_600_000);
            assert_eq!(
                raw(&db, b"k"),
                Some((b"11".to_vec(), deadline)),
                "{layout:?} clean {clean}"
            );
            assert_eq!(super::take_open_foreign_segment(&path), None);
        }
    }
}

/// A `CLOSE` read outside any replay scope (a live apply) changes nothing,
/// and one inside a scope with no file behind it (a WAL directory, a test
/// pin) judges what follows conservatively, as a segment at the end.
#[test]
fn a_close_outside_a_file_scope() {
    assert!(!super::observe_close(5));
    assert_eq!(super::pinned_replay_clock_ms(), None);
    {
        let _pin = super::pin_replay_clock_ms(9_000);
        assert!(super::observe_close(5));
        assert_eq!(super::pinned_replay_clock_ms(), Some(9_000));
        assert!(super::observe_log_ts(7));
        assert_eq!(super::pinned_replay_clock_ms(), Some(7));
    }
    assert_eq!(super::pinned_replay_clock_ms(), None);
}
