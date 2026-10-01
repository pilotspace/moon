//! moon#1297: the cold reclaim without an AOF — mostly-dead spill files are
//! compacted into new files, adopted once a snapshot that started after the
//! compaction has committed.
//!
//! Kill -9 at every point of a compaction's life (the `MOON_TEST_COLD_RECLAIM_*`
//! hooks, `storage::tiered::cold_reclaim::test_hooks`, plus
//! `MOON_TEST_SNAPSHOT_HOLD_FILE` to kill while the snapshot runs), then
//! restart and compare every key with what the LAST COMMITTED snapshot of
//! its shard says: never a key deleted before it (resurrected), never a
//! loss of a key it holds live (lost). Each shard's authority is read from
//! disk: its snapshot file changed since `S1` iff `S2` committed there.
//!
//! The shape (per case, `--appendonly no --save` with no auto-save):
//! 200 probes + 16,000 fillers spill under 8 MB; `S0`; DEL group A (i % 4 != 0:
//! 75% of every file dead); `S1`; release the compaction hold and wait for
//! compactions; DEL group B (i % 8 == 4: survivors whose compacted copies are
//! now dead); `S2` — the adopting snapshot — and the kill point. Group C
//! (i % 8 == 0) and the probes stay live throughout.
//!
//! The trailer of the adopting snapshot is checked exactly against the
//! compacted files on disk: a grave for every slot of a B key in them, none
//! for a C key; after adoption the next snapshot carries no grave of an
//! unlinked file.
//!
//! The automatic round a compaction may request (`ColdReclaim`) follows the
//! moon#1289 TXN rule: a round abandoned because a shard held a `TXN` write
//! at its start saves nothing and commits no compaction (the "TXN rule"
//! section below).
//!
//!   MOON_BIN=... [MOON_TEST_COLD_DEL_SHARDS=1] cargo test --test \
//!     cold_block_reclaim_no_aof_1297 -- --include-ignored --test-threads 1

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;
mod crash_recovery_cold_support;

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crash_recovery_cold_support::*;

/// Save points that never fire: every snapshot here is an explicit BGSAVE.
const SAVE: [&str; 2] = ["--save", "3600 100000000"];
/// `test_hooks::RECLAIM_CRASH_EXIT_CODE`.
const CRASH_EXIT: i32 = 87;

fn filler_key(i: usize) -> String {
    format!("filler:{i}")
}

/// Deleted before `S1`.
fn in_a(i: usize) -> bool {
    !i.is_multiple_of(4)
}

/// Deleted after the compaction, before `S2`.
fn in_b(i: usize) -> bool {
    i % 8 == 4
}

// ── RESP pipelining ──────────────────────────────────────────────────────────

#[derive(Debug)]
enum Reply {
    Status,
    // Read through `Debug`, in the panic that reports an unexpected reply.
    #[allow(dead_code)]
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
}

fn read_reply(r: &mut BufReader<std::net::TcpStream>) -> Reply {
    let mut line = String::new();
    r.read_line(&mut line).expect("reply line");
    let body = line.trim_end_matches("\r\n");
    match body.as_bytes().first() {
        Some(b'+') => Reply::Status,
        Some(b'-') => Reply::Error(body[1..].to_string()),
        Some(b':') => Reply::Int(body[1..].parse().expect("integer reply")),
        Some(b'$') => {
            let n: i64 = body[1..].parse().expect("bulk length");
            if n < 0 {
                return Reply::Bulk(None);
            }
            let mut buf = vec![0u8; n as usize + 2];
            r.read_exact(&mut buf).expect("bulk body");
            buf.truncate(n as usize);
            Reply::Bulk(Some(buf))
        }
        _ => panic!("unexpected reply line {line:?}"),
    }
}

/// Send `cmd key` for every key, pipelined in chunks, and return the replies
/// in order.
fn pipeline(port: u16, cmd: &str, keys: &[String]) -> Vec<Reply> {
    let stream = std::net::TcpStream::connect(format!("127.0.0.1:{port}")).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(60))).ok();
    let mut w = stream.try_clone().expect("clone");
    let mut r = BufReader::new(stream);
    let mut out = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(1_000) {
        let mut buf = Vec::with_capacity(chunk.len() * 32);
        for k in chunk {
            buf.extend_from_slice(
                format!("*2\r\n${}\r\n{cmd}\r\n${}\r\n{k}\r\n", cmd.len(), k.len()).as_bytes(),
            );
        }
        w.write_all(&buf).expect("pipeline write");
        for _ in chunk {
            out.push(read_reply(&mut r));
        }
    }
    out
}

fn del_keys(port: u16, keys: &[String]) -> usize {
    pipeline(port, "DEL", keys)
        .into_iter()
        .map(|r| match r {
            Reply::Int(n) => n as usize,
            other => panic!("DEL answered {other:?}"),
        })
        .sum()
}

fn get_all(port: u16, keys: &[String]) -> Vec<Option<Vec<u8>>> {
    pipeline(port, "GET", keys)
        .into_iter()
        .map(|r| match r {
            Reply::Bulk(v) => v,
            other => panic!("GET answered {other:?}"),
        })
        .collect()
}

// ── server helpers ───────────────────────────────────────────────────────────

fn bgsave_and_wait(port: u16) {
    let lastsave = || integer_reply(&redis_cmd(port, &["LASTSAVE"])).unwrap_or(0);
    let before = lastsave();
    std::thread::sleep(Duration::from_millis(1_100));
    redis_cmd(port, &["BGSAVE"]);
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let info = redis_cmd(port, &["INFO", "persistence"]);
        if info.contains("rdb_bgsave_in_progress:0") && lastsave() > before {
            assert!(
                info.contains("rdb_last_bgsave_status:ok"),
                "BGSAVE failed: {info}"
            );
            return;
        }
        assert!(Instant::now() < deadline, "BGSAVE never completed: {info}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// `BGSAVE` without waiting (the server may stop at a hook meanwhile).
fn bgsave_start(port: u16) {
    let _ = std::process::Command::new("redis-cli")
        .args(["-p", &port.to_string(), "BGSAVE"])
        .output();
}

/// Wait up to `secs` for the server to exit; its exit code.
fn wait_exit(server: &mut common::ServerGuard, secs: u64) -> Option<i32> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        if let Ok(Some(status)) = server.as_mut().try_wait() {
            return status.code();
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    None
}

fn shard_dir(dir: &Path, shard: usize) -> PathBuf {
    dir.join("off").join(format!("shard-{shard}"))
}

fn snapshot_path(dir: &Path, shard: usize) -> PathBuf {
    shard_dir(dir, shard).join(format!("shard-{shard}.rrdshard"))
}

/// Each shard's snapshot file contents, hashed (None: no file).
fn snapshot_ids(dir: &Path) -> Vec<Option<u64>> {
    use std::hash::{Hash, Hasher};
    (0..shards())
        .map(|s| {
            std::fs::read(snapshot_path(dir, s)).ok().map(|b| {
                let mut h = std::collections::hash_map::DefaultHasher::new();
                b.hash(&mut h);
                h.finish()
            })
        })
        .collect()
}

/// `heap-*.mpf` ids of one shard on disk.
fn heap_ids(dir: &Path, shard: usize) -> Vec<u64> {
    let Ok(rd) = std::fs::read_dir(shard_dir(dir, shard).join("data")) else {
        return Vec::new();
    };
    rd.flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.strip_prefix("heap-")?
                .strip_suffix(".mpf")?
                .parse()
                .ok()
        })
        .collect()
}

/// Every `(page, slot) -> key` of a spill file, read from disk.
fn file_slots(dir: &Path, shard: usize, file_id: u64) -> HashMap<(u32, u16), String> {
    use moon::persistence::kv_page::KvLeafPage;
    let path = shard_dir(dir, shard)
        .join("data")
        .join(format!("heap-{file_id:06}.mpf"));
    let raw = std::fs::read(&path).expect("read spill file");
    let mut out = HashMap::new();
    for (page, chunk) in raw.chunks_exact(4096).enumerate() {
        let mut buf = [0u8; 4096];
        buf.copy_from_slice(chunk);
        let Some(leaf) = KvLeafPage::from_bytes(buf) else {
            continue;
        };
        for slot in 0..leaf.slot_count() {
            if let Some(e) = leaf.get(slot) {
                out.insert(
                    (page as u32, slot),
                    String::from_utf8_lossy(&e.key).into_owned(),
                );
            }
        }
    }
    out
}

/// The cold-graves trailer of a shard's snapshot on disk.
fn trailer(dir: &Path, shard: usize) -> moon::persistence::snapshot::cold_graves::ColdGraves {
    let mut dbs: Vec<moon::storage::db::Database> = (0..16)
        .map(|_| moon::storage::db::Database::new())
        .collect();
    let mut expired = Vec::new();
    let mut graves = None;
    moon::persistence::snapshot::shard_snapshot_load_with_graves(
        &mut dbs,
        &snapshot_path(dir, shard),
        &mut expired,
        &mut graves,
    )
    .expect("load snapshot");
    graves.unwrap_or_default()
}

/// Active `KvLeaf` file ids the shard's manifest lists (server stopped).
fn listed(dir: &Path, shard: usize) -> HashSet<u64> {
    use moon::persistence::manifest::{FileStatus, ShardManifest};
    let m = ShardManifest::open(&shard_dir(dir, shard).join(format!("shard-{shard}.manifest")))
        .expect("open manifest");
    m.files()
        .iter()
        .filter(|f| {
            f.status == FileStatus::Active
                && f.file_type == moon::persistence::page::PageType::KvLeaf as u8
        })
        .map(|f| f.file_id)
        .collect()
}

// ── the kill points ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kill {
    /// `F'` written, not adopted (`compacted`).
    Compacted,
    /// The adopting snapshot is starting (`snapshot_start`).
    SnapshotStart,
    /// The adopting snapshot is running (held open, then kill -9).
    DuringSnapshot,
    /// It committed; `F'` not listed yet (`adopt_ready`).
    AdoptReady,
    /// `F'` durably listed; nothing re-pointed, `F` on disk (`listed`).
    Listed,
    /// `F` unlinked (`unlinked`).
    Unlinked,
    /// Adopted, and the next snapshot committed; then kill -9.
    AfterNextSnapshot,
}

impl Kill {
    fn hook(self) -> Option<&'static str> {
        match self {
            Kill::Compacted => Some("compacted"),
            Kill::SnapshotStart => Some("snapshot_start"),
            Kill::AdoptReady => Some("adopt_ready"),
            Kill::Listed => Some("listed"),
            Kill::Unlinked => Some("unlinked"),
            Kill::DuringSnapshot | Kill::AfterNextSnapshot => None,
        }
    }
}

/// Wait until compactions have started and stopped (the count stable for
/// 2 s, at most 60 s); the count.
fn wait_for_compactions(port: u16) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut stable_since = Instant::now();
    let mut last = 0;
    while Instant::now() < deadline {
        let c = info_u64(port, "cold_reclaim_compactions").unwrap_or(0);
        if c != last {
            last = c;
            stable_since = Instant::now();
        } else if c > 0 && stable_since.elapsed() >= Duration::from_secs(2) {
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    last
}

/// What a restart found wrong.
#[derive(Debug, Default)]
struct Outcome {
    resurrected: usize,
    lost: usize,
    wrong: Vec<String>,
}

fn run(kill: Kill) {
    let outcome = run_case(kill, false);
    assert!(outcome.wrong.is_empty(), "{kill:?}: {:?}", outcome.wrong);
}

/// One kill point. `sabotage`: remove every compacted file after the kill,
/// before the restart — the instrument check (its keys must count as lost).
fn run_case(kill: Kill, sabotage: bool) -> Outcome {
    let n = shards();
    let port = common::reserve_port();
    let dir = unique_dir(&format!("r1297-{kill:?}"));
    std::fs::create_dir_all(&dir).expect("create test dir");
    let hold = dir.join("reclaim.hold");
    std::fs::write(&hold, b"").expect("hold file");
    let snap_hold = dir.join("snapshot.hold");
    let hold_s = hold.to_string_lossy().to_string();
    let snap_hold_s = snap_hold.to_string_lossy().to_string();
    let mut envs: Vec<(&str, &str)> = vec![
        ("MOON_TEST_COLD_RECLAIM_HOLD_FILE", &hold_s),
        ("MOON_TEST_SNAPSHOT_HOLD_FILE", &snap_hold_s),
    ];
    if let Some(point) = kill.hook() {
        envs.push(("MOON_TEST_COLD_RECLAIM_CRASH", point));
    }
    let mut server = start_moon_with_env(port, &dir, 3600, "no", &SAVE, &envs);
    wait_for_port(port);

    let fillers: Vec<String> = (0..FILLER_COUNT).map(filler_key).collect();
    let a: Vec<String> = (0..FILLER_COUNT)
        .filter(|&i| in_a(i))
        .map(filler_key)
        .collect();
    let b: Vec<String> = (0..FILLER_COUNT)
        .filter(|&i| in_b(i))
        .map(filler_key)
        .collect();

    spill_probes(port, &dir);
    bgsave_and_wait(port); // S0
    let deleted_a = del_keys(port, &a);
    bgsave_and_wait(port); // S1: A's deletions are durable
    let s1 = snapshot_ids(&dir);
    let max_id_at_s1: Vec<u64> = (0..n)
        .map(|s| heap_ids(&dir, s).into_iter().max().unwrap_or(0))
        .collect();
    std::fs::remove_file(&hold).expect("release the compaction hold");

    let mut compactions = 0;
    let mut exit = None;
    if kill == Kill::Compacted {
        exit = wait_exit(&mut server, 60);
    } else {
        compactions = wait_for_compactions(port);
        assert!(
            compactions > 0,
            "precondition: no compaction started (deleted {deleted_a} A keys)"
        );
        // No further compaction from here: after the adoption a compacted
        // file is itself half dead (its B slots) and would be compacted
        // again, and that second adoption, after `S3`, would unlink files
        // `S3`'s trailer rightly names. One generation per case.
        std::fs::write(&hold, b"").expect("hold file");
        let deleted_b = del_keys(port, &b);
        assert!(deleted_b > 0, "precondition: no B key to delete");
        match kill {
            Kill::SnapshotStart | Kill::AdoptReady | Kill::Listed | Kill::Unlinked => {
                bgsave_start(port);
                exit = wait_exit(&mut server, 90);
            }
            Kill::DuringSnapshot => {
                std::fs::write(&snap_hold, b"").expect("snapshot hold");
                bgsave_start(port);
                let deadline = Instant::now() + Duration::from_secs(30);
                while !redis_cmd(port, &["INFO", "persistence"])
                    .contains("rdb_bgsave_in_progress:1")
                {
                    assert!(Instant::now() < deadline, "the snapshot never started");
                    std::thread::sleep(Duration::from_millis(50));
                }
                std::thread::sleep(Duration::from_millis(500));
                server.kill_now();
            }
            Kill::AfterNextSnapshot => {
                bgsave_and_wait(port); // S2
                let deadline = Instant::now() + Duration::from_secs(30);
                while info_u64(port, "cold_reclaim_files_unlinked").unwrap_or(0) == 0 {
                    assert!(Instant::now() < deadline, "nothing adopted after S2");
                    std::thread::sleep(Duration::from_millis(100));
                }
                std::thread::sleep(Duration::from_millis(500));
                bgsave_and_wait(port); // S3
                server.kill_now();
            }
            Kill::Compacted => unreachable!(),
        }
    }
    if kill.hook().is_some() {
        assert_eq!(
            exit,
            Some(CRASH_EXIT),
            "the {kill:?} hook never stopped the server"
        );
    }
    server.kill_now();
    wait_for_port_down(port);

    // Per shard: did the adopting snapshot commit before the kill?
    let after = snapshot_ids(&dir);
    let s2: Vec<bool> = (0..n).map(|s| after[s] != s1[s]).collect();
    match kill {
        Kill::Compacted | Kill::DuringSnapshot => assert!(
            s2.iter().all(|c| !c),
            "no snapshot may have committed after S1 here ({s2:?})"
        ),
        Kill::AdoptReady | Kill::Listed | Kill::Unlinked | Kill::AfterNextSnapshot => assert!(
            s2.iter().any(|c| *c),
            "the kill point comes after a committed snapshot ({s2:?})"
        ),
        Kill::SnapshotStart => {}
    }

    // The trailer of every snapshot committed after the compactions, against
    // the compacted files on disk: a grave for exactly the slots of B keys.
    let mut trailer_wrong = Vec::new();
    let mut compacted_graves = 0usize;
    for shard in (0..n).filter(|&s| s2[s]) {
        let graves = trailer(&dir, shard);
        let on_disk: HashSet<u64> = heap_ids(&dir, shard).into_iter().collect();
        let listed = listed(&dir, shard);
        // After the adoption, the NEXT snapshot forgets the unlinked old
        // files: every grave it carries is of a file listed and on disk. (The
        // adopting snapshot itself still names the old files it saw: they
        // were listed when it started, and a crash before their unlink
        // needs those graves.)
        if kill == Kill::AfterNextSnapshot {
            for (file_id, _) in graves.to_files() {
                if !on_disk.contains(&file_id) || !listed.contains(&file_id) {
                    trailer_wrong.push(format!(
                        "shard {shard}: a grave of file {file_id} after adoption (on disk {}, \
                         listed {})",
                        on_disk.contains(&file_id),
                        listed.contains(&file_id)
                    ));
                }
            }
        }
        for file_id in on_disk.iter().filter(|&&f| f > max_id_at_s1[shard]) {
            for ((page, slot), key) in file_slots(&dir, shard, *file_id) {
                let i: usize = key
                    .strip_prefix("filler:")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(usize::MAX);
                let dead = i != usize::MAX && in_b(i);
                let grave = graves.contains(*file_id, page, slot);
                compacted_graves += usize::from(grave);
                if dead != grave {
                    trailer_wrong.push(format!(
                        "shard {shard}: compacted file {file_id} slot ({page},{slot}) key {key}: \
                         grave {grave}, deleted before the snapshot {dead}"
                    ));
                }
            }
        }
    }

    if sabotage {
        // Every compacted file: on the shard whose hook stopped the process
        // the old files are gone, so nothing else holds those keys.
        let mut removed = 0;
        for (shard, &max_id) in max_id_at_s1.iter().enumerate() {
            for f in heap_ids(&dir, shard).into_iter().filter(|&f| f > max_id) {
                std::fs::remove_file(
                    shard_dir(&dir, shard)
                        .join("data")
                        .join(format!("heap-{f:06}.mpf")),
                )
                .expect("remove a compacted file");
                removed += 1;
            }
        }
        assert!(removed > 0, "no compacted file to remove");
    }
    let mut restarted = start_moon_alive_with(port, &dir, 3600, "no", &SAVE);
    let probe_keys: Vec<String> = (0..PROBE_COUNT).map(probe_key).collect();
    let pv = probe_value().into_bytes();
    let probes_lost = get_all(port, &probe_keys)
        .into_iter()
        .filter(|v| v.as_deref() != Some(pv.as_slice()))
        .count();
    let fv = "F".repeat(FILLER_VALUE_LEN).into_bytes();
    let (mut resurrected, mut lost, mut wrong_value) = (Vec::new(), Vec::new(), 0usize);
    for (i, v) in get_all(port, &fillers).into_iter().enumerate() {
        let key = filler_key(i);
        let shard = moon::shard::dispatch::key_to_shard(key.as_bytes(), n);
        let deleted = in_a(i) || (in_b(i) && s2[shard]);
        let live = !deleted;
        match (live, v) {
            (true, None) => lost.push(key),
            (true, Some(v)) if v != fv => wrong_value += 1,
            (false, Some(_)) => resurrected.push(key),
            _ => {}
        }
    }
    restarted.kill_now();
    wait_for_port_down(port);

    eprintln!(
        "{kill:?} (shards {n}): compactions {compactions}, S2 committed on {s2:?}, graves in \
         compacted files {compacted_graves}; after restart: {} resurrected, {} lost, {} wrong \
         value, {probes_lost} probes lost; trailer faults {}",
        resurrected.len(),
        lost.len(),
        wrong_value,
        trailer_wrong.len()
    );
    let mut wrong = Vec::new();
    if !resurrected.is_empty() {
        wrong.push(format!(
            "{} resurrected (first {:?})",
            resurrected.len(),
            &resurrected[..resurrected.len().min(5)]
        ));
    }
    if !lost.is_empty() || probes_lost > 0 || wrong_value > 0 {
        wrong.push(format!(
            "{} fillers lost (first {:?}), {probes_lost} probes lost, {wrong_value} wrong values",
            lost.len(),
            &lost[..lost.len().min(5)]
        ));
    }
    if !trailer_wrong.is_empty() {
        wrong.push(format!(
            "{} trailer faults (first {:?})",
            trailer_wrong.len(),
            &trailer_wrong[..trailer_wrong.len().min(5)]
        ));
    }
    if sabotage {
        // The instrument check expects its own damage: keep nothing.
        let _ = std::fs::remove_dir_all(&dir);
    } else {
        finish(&dir, &wrong);
    }
    Outcome {
        resurrected: resurrected.len(),
        lost: lost.len() + probes_lost + wrong_value,
        wrong,
    }
}

#[test]
#[ignore]
fn kill_after_compaction_before_adoption() {
    run(Kill::Compacted);
}

#[test]
#[ignore]
fn kill_as_the_adopting_snapshot_starts() {
    run(Kill::SnapshotStart);
}

#[test]
#[ignore]
fn kill_during_the_adopting_snapshot() {
    run(Kill::DuringSnapshot);
}

#[test]
#[ignore]
fn kill_after_the_snapshot_commits_before_listing() {
    run(Kill::AdoptReady);
}

#[test]
#[ignore]
fn kill_between_the_listing_and_the_unlink() {
    run(Kill::Listed);
}

#[test]
#[ignore]
fn kill_after_the_old_file_is_unlinked() {
    run(Kill::Unlinked);
}

#[test]
#[ignore]
fn kill_after_adoption_and_the_next_snapshot() {
    run(Kill::AfterNextSnapshot);
}

/// The instrument can fail (the loss half; the resurrection half was shown
/// by a mutant that left the compacted graves out of the trailer — see the
/// moon#1297 SUMMARY — and is pinned at unit level): after the old files
/// are unlinked, removing the compacted files must read as lost keys.
#[test]
#[ignore]
fn the_harness_counts_the_keys_of_a_removed_compacted_file_as_lost() {
    let outcome = run_case(Kill::Unlinked, true);
    assert!(
        outcome.lost > 0,
        "a compacted file was removed after its old file was unlinked, and the harness saw \
         no loss: {outcome:?}"
    );
    assert_eq!(outcome.resurrected, 0, "{outcome:?}");
}

// ── the moon#1289 TXN rule ───────────────────────────────────────────────────
//
// The snapshot a compaction waits for may be the AUTOMATIC one this shard
// requests (`SnapshotReason::ColdReclaim`). Like the held-file snapshot it
// must never contain an open `TXN`'s uncommitted writes: it waits while one
// is open, and each shard re-checks its own holds as it starts its part — one
// hold abandons the whole round (`persistence::snapshot_request::txn_round`).
// An abandoned round publishes nothing, so it must not commit a compaction
// either: the floor a compaction waits for moves only when a snapshot
// publishes (`snapshot_hold::note_snapshot_finished(true)`).
//
// The shape: S0, DEL A, S1; every shard is held at its start
// (`MOON_TEST_SNAPSHOT_START_HOLD_FILE`); compactions run and wait; the
// sweep requests the ColdReclaim round; a TXN writes `{t}k` and `{t}new`
// inside the window (after the request's pre-check, before any start); the
// starts are released. `MOON_TEST_COLD_RECLAIM_CRASH=adopt_ready` stops the
// server the moment any compaction is ready, i.e. the moment a snapshot that
// started after it committed.

const OK: &str = "+OK\r\n";

/// A connection with a `TXN` open that has written `{t}k = value`. A TXN
/// writes only on its connection's shard (#499) and a connection's shard is
/// not chosen by key, so at several shards reconnect until one lands there.
fn txn_on_the_keys_shard(port: u16, value: &str) -> common::Conn {
    for _ in 0..64 {
        let mut t = common::Conn::open(port);
        assert_eq!(t.send(&["TXN", "BEGIN"]), OK);
        let reply = t.send(&["SET", "{t}k", value]);
        if reply == OK {
            return t;
        }
        assert!(reply.contains("cross-shard"), "TXN SET answered {reply:?}");
        assert_eq!(t.send(&["TXN", "ABORT"]), OK);
    }
    panic!("no connection landed on the shard of {{t}}k in 64 tries");
}

fn saving(port: u16) -> bool {
    !redis_cmd(port, &["INFO", "persistence"]).contains("rdb_bgsave_in_progress:0")
}

/// Poll `cond` every 100 ms for up to `secs`, stopping early if the server
/// exits; the exit code if it did.
fn wait_or_exit(
    server: &mut common::ServerGuard,
    secs: u64,
    mut cond: impl FnMut() -> bool,
) -> Option<i32> {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        if let Ok(Some(status)) = server.as_mut().try_wait() {
            return Some(status.code().unwrap_or(-1));
        }
        if cond() {
            return None;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AfterAbandon {
    /// kill -9 with the TXN still open, compactions still pending.
    KillWithTxnOpen,
    /// `TXN ABORT`: the next sweep's round must publish and commit the
    /// compactions (the `adopt_ready` hook stops the server there).
    AbortAndRetry,
    /// `TXN ABORT`, then `SHUTDOWN` (its default save, a save rule being
    /// configured): the shutdown's save commits the pending compactions as
    /// the process stops (or a retried round does first, and `adopt_ready`
    /// stops it); the restart is that snapshot's point in time.
    AbortAndShutdown,
}

fn abandoned_reclaim_round(end: AfterAbandon) {
    let n = shards();
    let port = common::reserve_port();
    let dir = unique_dir(&format!("r1297-txn-{end:?}"));
    std::fs::create_dir_all(&dir).expect("create test dir");
    let hold = dir.join("reclaim.hold");
    std::fs::write(&hold, b"").expect("hold file");
    let start_hold = dir.join("start.hold");
    let hold_s = hold.to_string_lossy().to_string();
    let start_hold_s = start_hold.to_string_lossy().to_string();
    let envs = [
        ("MOON_TEST_COLD_RECLAIM_HOLD_FILE", hold_s.as_str()),
        ("MOON_TEST_SNAPSHOT_START_HOLD_FILE", start_hold_s.as_str()),
        ("MOON_TEST_COLD_RECLAIM_CRASH", "adopt_ready"),
    ];
    // A 1 s sweep: compactions waiting three sweeps request the round.
    let mut server = start_moon_with_env(port, &dir, 1, "no", &SAVE, &envs);
    wait_for_port(port);

    let fillers: Vec<String> = (0..FILLER_COUNT).map(filler_key).collect();
    let a: Vec<String> = (0..FILLER_COUNT)
        .filter(|&i| in_a(i))
        .map(filler_key)
        .collect();
    redis_set(port, "{t}k", "original");
    spill_probes(port, &dir);
    bgsave_and_wait(port); // S0
    del_keys(port, &a);
    bgsave_and_wait(port); // S1: A's deletions are durable
    let s1 = snapshot_ids(&dir);
    let lastsave_s1 = integer_reply(&redis_cmd(port, &["LASTSAVE"]));

    // Every shard now stops at the start of the next round.
    std::fs::write(&start_hold, b"").expect("start hold");
    std::fs::remove_file(&hold).expect("release the compaction hold");
    let compactions = wait_for_compactions(port);
    std::fs::write(&hold, b"").expect("hold file");
    assert!(compactions > 0, "precondition: no compaction started");
    assert_eq!(
        wait_or_exit(&mut server, 60, || {
            info_u64(port, "cold_reclaim_snapshots_requested").unwrap_or(0) >= 1 && saving(port)
        }),
        None,
        "precondition: the server stopped before the round was requested"
    );
    std::thread::sleep(Duration::from_millis(200));
    let pending = info_u64(port, "cold_reclaim_compactions_pending").unwrap_or(0);
    assert!(
        saving(port) && pending > 0,
        "precondition: the ColdReclaim round is requested and held at every shard's start \
         with compactions pending (pending {pending})"
    );
    assert_eq!(
        info_u64(port, "cold_held_release_snapshots_requested"),
        Some(0),
        "precondition: the round is the cold reclaim's, not the held-file trigger's"
    );

    // The TXN begins and writes AFTER the request's pre-check, BEFORE any
    // shard starts its part.
    let mut t = txn_on_the_keys_shard(port, "aborted");
    assert_eq!(t.send(&["SET", "{t}new", "inserted"]), OK);
    std::fs::remove_file(&start_hold).expect("release the starts");
    let mut stopped = wait_or_exit(&mut server, 60, || !saving(port));
    // Three more sweeps with the TXN open: the slot given back is asked for
    // again and deferred; nothing may adopt.
    if stopped.is_none() {
        stopped = wait_or_exit(&mut server, 4, || false);
    }
    let field = |f: &str| {
        if stopped.is_none() {
            info_u64(port, f)
        } else {
            None
        }
    };
    let abandoned = field("cold_reclaim_snapshots_abandoned_txn");
    let deferred = field("cold_reclaim_snapshots_deferred_txn");
    let unlinked = field("cold_reclaim_files_unlinked");
    let pending_after = field("cold_reclaim_compactions_pending");
    let lastsave = if stopped.is_none() {
        integer_reply(&redis_cmd(port, &["LASTSAVE"]))
    } else {
        lastsave_s1
    };
    let after_round = snapshot_ids(&dir);

    let mut retried_exit = None;
    if end != AfterAbandon::KillWithTxnOpen && stopped.is_none() {
        assert_eq!(t.send(&["TXN", "ABORT"]), OK);
        if end == AfterAbandon::AbortAndShutdown {
            let _ = std::process::Command::new("redis-cli")
                .args(["-p", &port.to_string(), "SHUTDOWN"])
                .output();
        }
        retried_exit = wait_exit(&mut server, 60);
    }
    drop(t);
    server.kill_now();
    wait_for_port_down(port);
    let committed: Vec<bool> = snapshot_ids(&dir)
        .iter()
        .zip(&s1)
        .map(|(now, s1)| now != s1)
        .collect();

    let mut restarted = start_moon_alive_with(port, &dir, 3600, "no", &SAVE);
    let k = redis_get(port, "{t}k");
    let new = redis_get(port, "{t}new");
    let probe_keys: Vec<String> = (0..PROBE_COUNT).map(probe_key).collect();
    let pv = probe_value().into_bytes();
    let probes_lost = get_all(port, &probe_keys)
        .into_iter()
        .filter(|v| v.as_deref() != Some(pv.as_slice()))
        .count();
    let fv = "F".repeat(FILLER_VALUE_LEN).into_bytes();
    let (mut resurrected, mut lost) = (0usize, 0usize);
    for (i, v) in get_all(port, &fillers).into_iter().enumerate() {
        match (in_a(i), v) {
            (true, Some(_)) => resurrected += 1,
            (false, None) => lost += 1,
            (false, Some(v)) if v != fv => lost += 1,
            _ => {}
        }
    }
    restarted.kill_now();
    wait_for_port_down(port);

    eprintln!(
        "{end:?} (shards {n}): compactions {compactions}, pending {pending} -> {pending_after:?}; \
         after the round: stopped {stopped:?}, abandoned {abandoned:?}, deferred {deferred:?}, \
         unlinked {unlinked:?}; retried exit {retried_exit:?}, committed {committed:?}; restart: \
         k {k:?}, new {new:?}, {resurrected} resurrected, {lost} lost, {probes_lost} probes lost"
    );
    let mut wrong = Vec::new();
    if let Some(code) = stopped {
        wrong.push(format!(
            "the server stopped (exit {code}) after the round that held a TXN write: a \
             compaction became ready, so that round committed"
        ));
    }
    if stopped.is_none() {
        if abandoned != Some(1) {
            wrong.push(format!(
                "cold_reclaim_snapshots_abandoned_txn {abandoned:?}, want 1"
            ));
        }
        if deferred.unwrap_or(0) == 0 {
            wrong.push("no request deferred while the TXN stayed open".to_string());
        }
        if unlinked != Some(0) || pending_after != Some(pending) {
            wrong.push(format!(
                "the abandoned round moved the compactions: unlinked {unlinked:?}, pending \
                 {pending} -> {pending_after:?}"
            ));
        }
        if after_round != s1 || lastsave != lastsave_s1 {
            wrong.push("the abandoned round published a snapshot (file or LASTSAVE)".to_string());
        }
    }
    match end {
        AfterAbandon::KillWithTxnOpen => {
            if committed.iter().any(|c| *c) {
                wrong.push(format!("a snapshot committed after S1: {committed:?}"));
            }
        }
        AfterAbandon::AbortAndRetry => {
            if stopped.is_none() && retried_exit != Some(CRASH_EXIT) {
                wrong.push(format!(
                    "the round retried after TXN ABORT did not commit the compactions \
                     (exit {retried_exit:?}, committed {committed:?})"
                ));
            }
        }
        AfterAbandon::AbortAndShutdown => {
            // Exit 0: the shutdown's save published on every shard. Exit
            // 87: a retried round committed first and one shard stopped at
            // `adopt_ready`, maybe before the others published.
            let ok = match retried_exit {
                Some(0) => committed.iter().all(|c| *c),
                Some(CRASH_EXIT) => committed.iter().any(|c| *c),
                _ => false,
            };
            if stopped.is_none() && !ok {
                wrong.push(format!(
                    "SHUTDOWN after the abandoned round: exit {retried_exit:?} (want 0 with \
                     every shard committed, or {CRASH_EXIT} if a retried round adopted \
                     first), committed {committed:?}"
                ));
            }
        }
    }
    if k.as_deref() != Some("original") || new.is_some() {
        wrong.push(format!(
            "the TXN's uncommitted writes reached a snapshot: after kill -9 GET {{t}}k -> {k:?} \
             (want original), GET {{t}}new -> {new:?} (want nil)"
        ));
    }
    if resurrected > 0 || lost > 0 || probes_lost > 0 {
        wrong.push(format!(
            "{resurrected} resurrected, {lost} fillers lost, {probes_lost} probes lost"
        ));
    }
    finish(&dir, &wrong);
    assert!(wrong.is_empty(), "{end:?}: {wrong:?}");
}

/// The round is abandoned and moves nothing; kill -9 with the TXN open and
/// the compactions pending restarts into S1 exactly.
#[test]
#[ignore]
fn an_abandoned_reclaim_round_neither_saves_the_txn_nor_commits_a_compaction() {
    abandoned_reclaim_round(AfterAbandon::KillWithTxnOpen);
}

/// The abandon is not a cancellation: once the TXN ends the next sweep's
/// round publishes and commits the compactions.
#[test]
#[ignore]
fn a_reclaim_round_retried_after_the_txn_ends_commits_the_compactions() {
    abandoned_reclaim_round(AfterAbandon::AbortAndRetry);
}

/// SHUTDOWN right after an abandoned round, compactions pending: its save
/// (not an automatic round) commits them as the process stops, and the
/// restart loses and resurrects nothing.
#[test]
#[ignore]
fn shutdown_after_an_abandoned_reclaim_round_keeps_every_key_exact() {
    abandoned_reclaim_round(AfterAbandon::AbortAndShutdown);
}

/// `CONFIG RESETSTAT` (the moon#1289 R1 rule: reset monotonic statistics,
/// never a gauge of live state): the reclaim's event counts go to zero, the
/// pending gauge keeps counting the compactions still waiting — before the
/// adopting snapshot and after it.
#[test]
#[ignore]
fn resetstat_zeroes_the_reclaim_statistics_and_keeps_the_pending_gauge() {
    let port = common::reserve_port();
    let dir = unique_dir("r1297-resetstat");
    std::fs::create_dir_all(&dir).expect("create test dir");
    let hold = dir.join("reclaim.hold");
    std::fs::write(&hold, b"").expect("hold file");
    let hold_s = hold.to_string_lossy().to_string();
    let envs = [("MOON_TEST_COLD_RECLAIM_HOLD_FILE", hold_s.as_str())];
    // No sweep, no save rule: nothing but the BGSAVE below commits.
    let mut server = start_moon_with_env(port, &dir, 3600, "no", &SAVE, &envs);
    wait_for_port(port);
    let a: Vec<String> = (0..FILLER_COUNT)
        .filter(|&i| in_a(i))
        .map(filler_key)
        .collect();
    spill_probes(port, &dir);
    bgsave_and_wait(port);
    del_keys(port, &a);
    bgsave_and_wait(port);
    std::fs::remove_file(&hold).expect("release the compaction hold");
    let compactions = wait_for_compactions(port);
    std::fs::write(&hold, b"").expect("hold file");
    let pending = info_u64(port, "cold_reclaim_compactions_pending").unwrap_or(0);
    assert!(
        compactions > 0 && pending > 0,
        "precondition: compactions recorded ({compactions}) and waiting ({pending})"
    );
    let stats = [
        "cold_reclaim_compactions",
        "cold_reclaim_files_unlinked",
        "cold_reclaim_bytes_unlinked",
        "cold_reclaim_snapshots_requested",
        "cold_reclaim_snapshots_deferred_txn",
        "cold_reclaim_snapshots_abandoned_txn",
    ];
    let mut wrong = Vec::new();
    let mut check = |when: &str, pending: u64| {
        redis_cmd(port, &["CONFIG", "RESETSTAT"]);
        for f in stats {
            let v = info_u64(port, f);
            if v != Some(0) {
                wrong.push(format!("{when}: {f} {v:?} after RESETSTAT, want 0"));
            }
        }
        let p = info_u64(port, "cold_reclaim_compactions_pending");
        if p != Some(pending) {
            wrong.push(format!(
                "{when}: the gauge cold_reclaim_compactions_pending {p:?} after RESETSTAT, \
                 want {pending}"
            ));
        }
    };
    check("pending", pending);

    // The adopting snapshot: the old files go, the counters count again.
    bgsave_and_wait(port);
    let deadline = Instant::now() + Duration::from_secs(30);
    while info_u64(port, "cold_reclaim_compactions_pending") != Some(0)
        || info_u64(port, "cold_reclaim_files_unlinked").unwrap_or(0) == 0
    {
        assert!(
            Instant::now() < deadline,
            "nothing adopted after the BGSAVE"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let unlinked = info_u64(port, "cold_reclaim_files_unlinked");
    check("adopted", 0);
    server.kill_now();
    wait_for_port_down(port);
    eprintln!("resetstat: compactions {compactions}, pending {pending}, unlinked {unlinked:?}");
    finish(&dir, &wrong);
    assert!(wrong.is_empty(), "{wrong:?}");
}

// ── disk held ────────────────────────────────────────────────────────────────

/// A value no spill compression shrinks: `len` hex digits of a xorshift
/// stream seeded by `seed` (the spill path compresses values, and a run of
/// one byte would measure the codec, not the files).
fn noise(seed: u64, len: usize) -> String {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut out = String::with_capacity(len + 16);
    while out.len() < len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.push_str(&format!("{x:016x}"));
    }
    out.truncate(len);
    out
}

/// Bytes of every `heap-*.mpf` under the offload dir.
fn heap_bytes(dir: &Path) -> u64 {
    fn walk(p: &Path, acc: &mut u64) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    walk(&path, acc);
                } else if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("heap-") && n.ends_with(".mpf"))
                {
                    *acc += e.metadata().map(|m| m.len()).unwrap_or(0);
                }
            }
        }
    }
    let mut acc = 0;
    walk(&dir.join("off"), &mut acc);
    acc
}

/// The headline: rounds of a write flood over `maxmemory` with DEL churn
/// (three of every four keys deleted) leave mostly-dead spill files. With
/// reclaim, the spill files hold at most twice the live keys' bytes once
/// the requested snapshots have committed the compactions; without it
/// every file stays whole (about four times).
#[test]
#[ignore]
fn disk_held_under_a_no_aof_flood_with_del_churn_is_bounded() {
    // Six rounds keep 14.4 MB of fillers live: well past the 8 MB budget, so
    // most of what stays live is cold, in mostly-dead files.
    const ROUNDS: usize = 6;
    let port = common::reserve_port();
    let dir = unique_dir("r1297-disk");
    std::fs::create_dir_all(&dir).expect("create test dir");
    // Sweep every second: compactions waiting for a snapshot request one
    // after 3 sweeps, at most one per 10 s (`held_release::spacing`).
    let mut server = start_moon_alive_with(port, &dir, 1, "no", &SAVE);
    let mut live: Vec<(String, String)> = Vec::new();
    for round in 0..ROUNDS {
        let keys: Vec<(String, String)> = (0..FILLER_COUNT)
            .map(|i| {
                let k = format!("r{round}:{i}");
                let v = noise((round * FILLER_COUNT + i) as u64, FILLER_VALUE_LEN);
                (k, v)
            })
            .collect();
        let stream = std::net::TcpStream::connect(format!("127.0.0.1:{port}")).expect("connect");
        stream.set_read_timeout(Some(Duration::from_secs(60))).ok();
        let mut w = stream.try_clone().expect("clone");
        let mut r = BufReader::new(stream);
        for chunk in keys.chunks(1_000) {
            let mut buf = Vec::new();
            for (k, v) in chunk {
                buf.extend_from_slice(
                    format!(
                        "*3\r\n$3\r\nSET\r\n${}\r\n{k}\r\n${}\r\n{v}\r\n",
                        k.len(),
                        v.len()
                    )
                    .as_bytes(),
                );
            }
            w.write_all(&buf).expect("SET write");
            for _ in chunk {
                match read_reply(&mut r) {
                    Reply::Status => {}
                    other => panic!("SET answered {other:?}"),
                }
            }
        }
        let dead: Vec<String> = keys
            .iter()
            .enumerate()
            .filter(|(i, _)| !i.is_multiple_of(4))
            .map(|(_, (k, _))| k.clone())
            .collect();
        del_keys(port, &dead);
        live.extend(keys.into_iter().step_by(4));
        std::thread::sleep(Duration::from_secs(2));
    }
    let live_bytes = (live.len() * FILLER_VALUE_LEN) as u64;
    let peak = heap_bytes(&dir);
    // The bound: 1.5x the value bytes of the live COLD keys (INFO
    // `cold_keys`, refreshed by every sweep). Page, slot and key overhead is
    // ~1.25x of it with no dead slot at all; mostly-dead files left whole
    // hold ~2.5x.
    let bound = || info_u64(port, "cold_keys").unwrap_or(0) * FILLER_VALUE_LEN as u64 * 3 / 2;
    // Let the reclaim compact, a requested snapshot commit it, and adopt.
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut held = heap_bytes(&dir);
    while Instant::now() < deadline && held > bound() {
        std::thread::sleep(Duration::from_secs(1));
        held = heap_bytes(&dir);
    }
    let bound = bound();
    let (compactions, unlinked, requested) = (
        info_u64(port, "cold_reclaim_compactions").unwrap_or(0),
        info_u64(port, "cold_reclaim_files_unlinked").unwrap_or(0),
        info_u64(port, "cold_reclaim_snapshots_requested").unwrap_or(0),
    );
    let (cold_keys, cold_files, grave_slots) = (
        info_u64(port, "cold_keys").unwrap_or(0),
        info_u64(port, "cold_files").unwrap_or(0),
        info_u64(port, "cold_grave_slots").unwrap_or(0),
    );
    let live_keys: Vec<String> = live.iter().map(|(k, _)| k.clone()).collect();
    let lost = get_all(port, &live_keys)
        .into_iter()
        .zip(&live)
        .filter(|(got, (_, v))| got.as_deref() != Some(v.as_bytes()))
        .count();
    server.kill_now();
    eprintln!(
        "disk held (shards {}): live filler bytes {live_bytes}, spill files {peak} after the \
         flood, {held} after settling ({:.2}x the live cold value bytes, bound 1.5x); \
         compactions {compactions}, files unlinked \
         {unlinked}, snapshots requested {requested}; cold keys {cold_keys} in {cold_files} \
         files with {grave_slots} dead slots; {lost} live keys unreadable",
        shards(),
        held as f64 * 1.5 / bound.max(1) as f64
    );
    let mut wrong = Vec::new();
    if held > bound {
        wrong.push(format!(
            "spill files hold {held} bytes after settling, over 1.5x the {cold_keys} live cold \
             keys' value bytes ({bound})"
        ));
    }
    if lost > 0 {
        wrong.push(format!("{lost} live keys unreadable"));
    }
    finish(&dir, &wrong);
    assert!(wrong.is_empty(), "{wrong:?}");
}
