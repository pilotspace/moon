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
//! Every wait is on the condition it stands for (moon#1065): the fillers
//! re-send writes refused for AOF backpressure and read each pipeline within
//! the server's own bound ([`slow_host::pipeline_budget`], not a flat 20 s);
//! "spilled", "reclaimed" and "tombstones pruned" are polled on disk; and a
//! `SIGKILL` waits for the AOF to hold the last write instead of sleeping
//! 1.5 s. A timeout prints the server's INFO counters and `server.err`.
//!
//! Run with (pin the binary you just built):
//!   MOON_BIN=target/release-fast/moon cargo test --profile release-fast \
//!     --test cold_file_id_reuse_1067

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#![allow(clippy::unwrap_used)]

mod common;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::slow_host::{self, WriteStats};
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

    /// `crash`: `SIGKILL` once the AOF holds the last write `stats` saw
    /// applied — the everysec writer may still hold it in its channel, and a
    /// kill before it is written loses it (this used to sleep 1.5 s).
    fn stop(mut self, crash: bool, dir: &Path, stats: &WriteStats) {
        if crash {
            slow_host::wait_aof_holds_last(stats, self.port, dir);
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

/// `SET <prefix>:<i>` for every filler key, 50 to a pipeline. Each pipeline
/// is read within [`slow_host::pipeline_budget`] and every write refused for
/// AOF backpressure is re-sent (moon#1065); the replies used to be discarded
/// under a flat 20 s read budget, which one stall of the server exceeds.
fn fill(c: &mut Conn, dir: &Path, prefix: &str, stats: &mut WriteStats) {
    let value = "f".repeat(FILLER_LEN);
    let cmds: Vec<Vec<String>> = (0..FILLER)
        .map(|i| vec!["SET".to_string(), format!("{prefix}:{i}"), value.clone()])
        .collect();
    let deadline = Instant::now() + slow_host::CONDITION_DEADLINE;
    slow_host::write_all(c, &cmds, 50, stats, deadline, dir);
    eprintln!("fill {prefix}: {stats}");
}

/// `DEL` every filler key, as [`fill`] writes them.
fn delete(c: &mut Conn, dir: &Path, prefix: &str, stats: &mut WriteStats) {
    let cmds: Vec<Vec<String>> = (0..FILLER)
        .map(|i| vec!["DEL".to_string(), format!("{prefix}:{i}")])
        .collect();
    let deadline = Instant::now() + slow_host::CONDITION_DEADLINE;
    slow_host::write_all(c, &cmds, 50, stats, deadline, dir);
    eprintln!("delete {prefix}: {stats}");
}

/// The manifest on disk has settled after boot 1's files were reclaimed.
/// With `prune_now`: every tombstone of `ids` is pruned (tombstoned on one
/// checkpoint, pruned after it, persisted by the next — what a fixed 4 s
/// sleep stood for) except the manifest's highest id, which
/// `gc_tombstones` keeps as the id counter's high-water mark. Otherwise none
/// of `ids` is still Active.
fn manifest_settled(dir: &Path, ids: &BTreeSet<u64>, prune_now: bool) -> bool {
    use moon::persistence::manifest::{FileStatus, ShardManifest};
    let Ok(m) = ShardManifest::open(&dir.join("shard-0").join("shard-0.manifest")) else {
        return false;
    };
    let high_water = m.files().iter().map(|e| e.file_id).max();
    let mut ours = m.files().iter().filter(|e| ids.contains(&e.file_id));
    if prune_now {
        ours.all(|e| Some(e.file_id) == high_water)
    } else {
        ours.all(|e| e.status != FileStatus::Active)
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
/// The ids of every heap file seen on disk are accumulated while waiting
/// (they used to be sampled once, after a fixed 1 s sleep), so a spill that
/// lands late is still counted as used by its boot.
fn run(tag: &str, prune_now: bool, crash: bool) -> (String, Vec<u64>, Vec<u64>) {
    let dir = fresh_dir(tag);
    let within = slow_host::CONDITION_DEADLINE;

    let srv = start(&dir, prune_now);
    let mut c = srv.conn();
    let mut stats = WriteStats::default();
    fill(&mut c, &dir, "old", &mut stats);
    let mut first_ids = BTreeSet::new();
    slow_host::wait_until("boot 1 to spill", within, srv.port, &dir, || {
        first_ids.extend(heap_ids(&dir));
        !first_ids.is_empty()
    });
    // Every spill must land before its keys are deleted: that is the state
    // "boot 1 spills and then reclaims every cold file" means (it was a
    // fixed 1 s sleep). A DEL racing an in-flight spill is a different case.
    slow_host::wait_spill_idle("boot 1's spills to land", srv.port, &dir);
    first_ids.extend(heap_ids(&dir));
    delete(&mut c, &dir, "old", &mut stats);
    assert_eq!(c.send(&["DBSIZE"]), ":0\r\n");
    slow_host::wait_until(
        "boot 1's cold files to be reclaimed",
        within,
        srv.port,
        &dir,
        || {
            let now = heap_ids(&dir);
            first_ids.extend(now.iter().copied());
            now.is_empty()
        },
    );
    slow_host::wait_until(
        if prune_now {
            "boot 1's tombstones to be pruned from the manifest on disk"
        } else {
            "boot 1's files to be tombstoned in the manifest on disk"
        },
        within,
        srv.port,
        &dir,
        || manifest_settled(&dir, &first_ids, prune_now),
    );
    drop(c);
    srv.stop(crash, &dir, &stats);

    let srv = start(&dir, prune_now);
    let mut c = srv.conn();
    assert_eq!(c.send(&["RPUSH", "X", "a"]), ":1\r\n");
    let mut stats = WriteStats::default();
    fill(&mut c, &dir, "new", &mut stats);
    let mut second_ids = BTreeSet::new();
    slow_host::wait_until("boot 2 to spill", within, srv.port, &dir, || {
        second_ids.extend(heap_ids(&dir));
        !second_ids.is_empty()
    });
    slow_host::wait_spill_idle("boot 2's spills to land", srv.port, &dir);
    // Do not read X here: a read would promote it out of the cold tier.
    drop(c);
    srv.stop(crash, &dir, &stats);
    // Boot 2 is gone: whatever heap files it left are all it used.
    second_ids.extend(heap_ids(&dir));

    let srv = start(&dir, prune_now);
    let mut c = srv.conn();
    let got = c.send(&["LRANGE", "X", "0", "-1"]);
    drop(c);
    srv.stop(false, &dir, &WriteStats::default());
    let _ = std::fs::remove_dir_all(&dir);
    (
        got,
        first_ids.into_iter().collect(),
        second_ids.into_iter().collect(),
    )
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
