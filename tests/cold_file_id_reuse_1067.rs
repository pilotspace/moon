//! A cold-tier file id must never be issued twice within one AOF generation
//! (moon#1067).
//!
//! The AOF writer appends to the same incr file across restarts, so one
//! generation (one `MOON.COLDCUT` head) spans every boot until a rewrite. Each
//! spill appends `MOON.SPILLED <file_id> key...`, and on replay that record
//! makes `file_id` readable for the REST of the generation. That is correct
//! only while an id names one file for the life of the generation.
//!
//! A restart resumes the id counter at one past the highest id the manifest
//! or the disk still holds. Tombstone GC (`gc_tombstones`) can prune the
//! manifest entry of a reclaimed file, and when every file is gone that makes
//! the counter restart below ids the generation already used. A re-issued id
//! is then authorised during replay by the OLD file's marker, long before the
//! new file's own marker. A write logged before its key was spilled into the
//! new file promotes the key's FINAL cold value and applies on top of it. The
//! promotion removes the cold entry, so the new marker no longer undoes it:
//! `RPUSH X a` once, `LRANGE X` reads `a a` after the next restart.
//!
//! The test drives exactly that sequence on a real server, with tombstone
//! retention set to zero so GC prunes on the next checkpoint. The control arm
//! (`default_retention_control`) keeps the default retention and must read
//! `a`. That is the proof that the failing arm fails because the id was
//! re-issued, not for some other reason.
//!
//! Run with (pin the binary you just built):
//!   MOON_BIN=target/release-fast/moon cargo test --profile release-fast \
//!     --test cold_file_id_reuse_1067

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#![allow(clippy::unwrap_used)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, find_moon_binary, server_stderr, spawn_listening_guarded};

const MAXMEMORY_BYTES: u64 = 512 * 1024;
const FILLER: usize = 1500;
const FILLER_LEN: usize = 2000;

struct Server {
    guard: ServerGuard,
    port: u16,
}

/// `prune_now`: tombstone retention zero, so each checkpoint (every second)
/// prunes the tombstones the previous one committed.
fn start(dir: &Path, prune_now: bool) -> Server {
    let retain_epochs = if prune_now { "0" } else { "2" };
    let retain_secs = if prune_now { "0" } else { "300" };
    let (guard, port) = spawn_listening_guarded(|port| {
        Command::new(find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--appendonly",
                "yes",
                "--disk-offload",
                "enable",
                "--maxmemory",
                &MAXMEMORY_BYTES.to_string(),
                "--maxmemory-policy",
                "allkeys-lru",
                "--maxmemory-samples",
                "200",
                "--disk-free-min-pct",
                "0",
                "--cold-orphan-sweep-interval-secs",
                "1",
                "--checkpoint-timeout",
                "1",
                "--manifest-tombstone-retain-epochs",
                retain_epochs,
                "--manifest-tombstone-retain-secs",
                retain_secs,
                "--dir",
            ])
            .arg(dir)
            .stdout(Stdio::null())
            .stderr(server_stderr(dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    await_loaded(port);
    Server { guard, port }
}

fn await_loaded(port: u16) {
    const DEADLINE: Duration = Duration::from_secs(120);
    let start = Instant::now();
    loop {
        let mut c = Conn::open(port);
        let info = c.send(&["INFO", "persistence"]);
        let read = c.send(&["EXISTS", "moon1067:probe"]);
        if info.contains("loading:0\r\n") && read.starts_with(':') {
            return;
        }
        assert!(
            start.elapsed() < DEADLINE,
            "server still loading after {DEADLINE:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

impl Server {
    fn conn(&self) -> Conn {
        Conn::open(self.port)
    }

    fn stop(mut self, crash: bool) {
        if crash {
            // Let the everysec AOF fsync cover the last write before the kill.
            std::thread::sleep(Duration::from_millis(1500));
            self.guard.kill_now();
            return;
        }
        use std::io::Write as _;
        let mut c = self.conn();
        let _ = c.sock.write_all(&common::encode(&["SHUTDOWN"]));
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if self.guard.as_mut().try_wait().unwrap().is_some() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "server did not exit within 30s of SHUTDOWN"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

fn heap_ids(dir: &Path) -> Vec<u64> {
    let mut ids: Vec<u64> = std::fs::read_dir(dir.join("shard-0").join("data"))
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().into_string().ok()?;
            name.strip_prefix("heap-")?
                .strip_suffix(".mpf")?
                .parse()
                .ok()
        })
        .collect();
    ids.sort_unstable();
    ids
}

fn fill(c: &mut Conn, prefix: &str) {
    let value = "f".repeat(FILLER_LEN);
    for chunk in (0..FILLER).collect::<Vec<_>>().chunks(50) {
        let keys: Vec<String> = chunk.iter().map(|i| format!("{prefix}:{i}")).collect();
        let cmds: Vec<[&str; 3]> = keys
            .iter()
            .map(|k| ["SET", k.as_str(), value.as_str()])
            .collect();
        let refs: Vec<&[&str]> = cmds.iter().map(|c| &c[..]).collect();
        let _ = c.pipeline(&refs);
    }
}

fn delete(c: &mut Conn, prefix: &str) {
    for chunk in (0..FILLER).collect::<Vec<_>>().chunks(50) {
        let keys: Vec<String> = chunk.iter().map(|i| format!("{prefix}:{i}")).collect();
        let cmds: Vec<[&str; 2]> = keys.iter().map(|k| ["DEL", k.as_str()]).collect();
        let refs: Vec<&[&str]> = cmds.iter().map(|c| &c[..]).collect();
        let _ = c.pipeline(&refs);
    }
}

fn wait_for(what: &str, secs: u64, mut ok: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !ok() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn fresh_dir(tag: &str) -> PathBuf {
    let d = common::unique_test_dir(&format!("moon-1067-{tag}"));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// Boot 1 spills and then reclaims every cold file; boot 2 writes `X` once
/// and spills it; boot 3 reads it. Returns `(LRANGE X after boot 3, the
/// heap ids boot 1 used, the heap ids boot 2 used)`.
fn run(tag: &str, prune_now: bool, crash: bool) -> (String, Vec<u64>, Vec<u64>) {
    let dir = fresh_dir(tag);

    let srv = start(&dir, prune_now);
    let mut c = srv.conn();
    fill(&mut c, "old");
    wait_for("boot 1 to spill", 30, || !heap_ids(&dir).is_empty());
    std::thread::sleep(Duration::from_millis(1000));
    let first_ids = heap_ids(&dir);
    delete(&mut c, "old");
    assert_eq!(c.send(&["DBSIZE"]), ":0\r\n");
    wait_for("boot 1's cold files to be reclaimed", 30, || {
        heap_ids(&dir).is_empty()
    });
    // Tombstoned on one checkpoint, pruned after it, persisted by the next.
    std::thread::sleep(Duration::from_millis(4000));
    drop(c);
    srv.stop(crash);

    let srv = start(&dir, prune_now);
    let mut c = srv.conn();
    assert_eq!(c.send(&["RPUSH", "X", "a"]), ":1\r\n");
    fill(&mut c, "new");
    wait_for("boot 2 to spill", 30, || !heap_ids(&dir).is_empty());
    std::thread::sleep(Duration::from_millis(1000));
    let second_ids = heap_ids(&dir);
    // Do not read X here: a read would promote it out of the cold tier.
    drop(c);
    srv.stop(crash);

    let srv = start(&dir, prune_now);
    let mut c = srv.conn();
    let got = c.send(&["LRANGE", "X", "0", "-1"]);
    drop(c);
    srv.stop(false);
    let _ = std::fs::remove_dir_all(&dir);
    (got, first_ids, second_ids)
}

fn assert_written_once(tag: &str, prune_now: bool, crash: bool) {
    let (got, first, second) = run(tag, prune_now, crash);
    assert!(!first.is_empty(), "instrument: boot 1 never spilled");
    assert!(!second.is_empty(), "instrument: boot 2 never spilled");
    let reused: Vec<u64> = second
        .iter()
        .copied()
        .filter(|id| first.contains(id))
        .collect();
    assert_eq!(
        got, "*1\r\n$1\r\na\r\n",
        "RPUSH X a ran once; after the restart LRANGE X read {got:?}. \
         Boot 1 used heap ids {first:?}, boot 2 used {second:?} (re-issued: {reused:?})"
    );
    assert!(
        reused.is_empty(),
        "boot 2 re-issued cold file ids boot 1 had used in the same AOF generation: {reused:?}"
    );
}

#[test]
fn pruned_tombstones_do_not_reissue_a_file_id_across_a_kill9() {
    assert_written_once("prune-kill9", true, true);
}

#[test]
fn pruned_tombstones_do_not_reissue_a_file_id_across_a_clean_restart() {
    assert_written_once("prune-clean", true, false);
}

/// Default retention keeps every tombstone past the restart, so the seed
/// cannot move backwards. This arm reads `a` on every version.
#[test]
fn default_retention_control() {
    assert_written_once("control", false, true);
}
