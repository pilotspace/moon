//! The boot crash-orphan sweep must not move a shard's cold file-id counter
//! backwards (moon#1114).
//!
//! A spill publishes its batch, then `apply_completion_vec` appends
//! `MOON.SPILLED <N> key...` to the AOF, and only afterwards runs ONE deferred
//! manifest commit for the batch. A crash in between leaves:
//!
//! - the AOF holding the marker for `N`,
//! - `data/heap-<N>.mpf` renamed into place,
//! - a manifest with no entry for `N`.
//!
//! The next boot classifies `heap-<N>.mpf` as a crash orphan and deletes it in
//! the background. Before the fix nothing recorded `N` anywhere once the file
//! was gone, so the boot after that resumed the counter at `N` and re-issued
//! it inside the same AOF generation. The old marker then authorised the NEW
//! file `N` early on replay, and a write logged before its key was spilled
//! into that file was applied on top of the value it had already produced:
//! `RPUSH X a` once, `LRANGE X` reads `a a` (the #1067 outcome).
//!
//! The crash window is injected as the durable state it leaves behind: boot 1
//! spills and is killed with SIGKILL after the everysec AOF fsync, then the
//! test rewrites the manifest without the highest spill id's entry — exactly
//! the commit the crash lost. Everything else (AOF, heap files, control file)
//! is what the real server wrote.
//!
//! Run with (pin the binary you just built):
//!   MOON_BIN=target/release-fast/moon cargo test --profile release-fast \
//!     --test cold_file_id_orphan_sweep_1114

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#![allow(clippy::unwrap_used)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, find_moon_binary, server_stderr, spawn_listening_guarded};
use moon::persistence::manifest::{FileStatus, ShardManifest};
use moon::persistence::page::PageType;

const MAXMEMORY_BYTES: u64 = 512 * 1024;
/// Boot 2 must allocate no file id: room for every key replay rebuilds hot.
const ROOMY_MAXMEMORY_BYTES: u64 = 256 * 1024 * 1024;
const FILLER: usize = 1500;
const FILLER_LEN: usize = 2000;

struct Server {
    guard: ServerGuard,
    port: u16,
}

fn start(dir: &Path, maxmemory: u64) -> Server {
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
                &maxmemory.to_string(),
                "--maxmemory-policy",
                "allkeys-lru",
                "--maxmemory-samples",
                "200",
                "--disk-free-min-pct",
                "0",
                "--checkpoint-timeout",
                "1",
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
        let read = c.send(&["EXISTS", "moon1114:probe"]);
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

fn shard_dir(dir: &Path) -> PathBuf {
    dir.join("shard-0")
}

fn manifest_path(dir: &Path) -> PathBuf {
    shard_dir(dir).join("shard-0.manifest")
}

fn heap_ids(dir: &Path) -> Vec<u64> {
    let mut ids: Vec<u64> = std::fs::read_dir(shard_dir(dir).join("data"))
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

/// Rewrite the manifest without the entries of `lost`: the state a crash
/// between their `MOON.SPILLED` markers and the deferred manifest commits
/// that would have listed them leaves behind. (The commits are asynchronous
/// on the manifest-sync thread, which coalesces queued snapshots, so under a
/// slow device a crash can lose every spill commit since the last durable
/// one.)
fn lose_manifest_commits_for(dir: &Path, lost: &[u64]) {
    let path = manifest_path(dir);
    let kept: Vec<_> = ShardManifest::open(&path)
        .unwrap()
        .files()
        .iter()
        .filter(|e| !lost.contains(&e.file_id))
        .cloned()
        .collect();
    std::fs::remove_file(&path).unwrap();
    let mut m = ShardManifest::create(&path).unwrap();
    for e in kept {
        m.add_file(e).unwrap();
    }
    m.commit().unwrap();
}

fn manifest_entry_status(dir: &Path, id: u64) -> Option<FileStatus> {
    ShardManifest::open(&manifest_path(dir))
        .unwrap()
        .files()
        .iter()
        .find(|e| e.file_id == id && e.file_type == PageType::KvLeaf as u8)
        .map(|e| e.status)
}

fn fresh_dir(tag: &str) -> PathBuf {
    let d = common::unique_test_dir(&format!("moon-1114-{tag}"));
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn orphan_sweep_does_not_reissue_a_marked_file_id() {
    let dir = fresh_dir("orphan");

    // Boot 1: spill (each batch logs its MOON.SPILLED marker), then crash.
    let srv = start(&dir, MAXMEMORY_BYTES);
    let mut c = srv.conn();
    fill(&mut c, "old");
    wait_for("boot 1 to spill", 30, || !heap_ids(&dir).is_empty());
    std::thread::sleep(Duration::from_millis(1000));
    drop(c);
    srv.stop(true);
    let first_ids = heap_ids(&dir);
    eprintln!("boot 1 heap ids: {first_ids:?}");
    let n = *first_ids.last().expect("instrument: boot 1 never spilled");
    // The crash window: every marker is in the AOF and every file on disk,
    // but none of the manifest commits that would list them survived. The
    // next boot resumes the counter at 1 once the files are gone, so boot 3's
    // first spill takes exactly an id boot 1's markers name.
    lose_manifest_commits_for(&dir, &first_ids);
    for id in &first_ids {
        assert_eq!(manifest_entry_status(&dir, *id), None);
    }

    // Boot 2: no spill. The boot sweep reclaims the orphans in the
    // background. Replay rebuilt boot 1's keys hot (their cold copies are in
    // no manifest); drop them so boot 3's first spill is `X`. Enough memory
    // that nothing spills: a spill here would list an id above boot 1's.
    let srv = start(&dir, ROOMY_MAXMEMORY_BYTES);
    let mut c = srv.conn();
    delete(&mut c, "old");
    assert_eq!(c.send(&["DBSIZE"]), ":0\r\n");
    drop(c);
    wait_for(
        "boot 2's orphan sweep to delete the unmanifested files",
        30,
        || !heap_ids(&dir).iter().any(|id| first_ids.contains(id)),
    );
    let boot2_ids = heap_ids(&dir);
    assert!(
        boot2_ids.iter().all(|id| *id < n),
        "instrument: boot 2 spilled (ids {boot2_ids:?}); the scenario needs a boot \
         that allocates nothing above {n}"
    );
    srv.stop(false);
    let recorded_after_sweep = manifest_entry_status(&dir, n);

    // Boot 3: write X once, spill it.
    let srv = start(&dir, MAXMEMORY_BYTES);
    let mut c = srv.conn();
    assert_eq!(c.send(&["RPUSH", "X", "a"]), ":1\r\n");
    fill(&mut c, "new");
    wait_for("boot 3 to spill", 30, || {
        heap_ids(&dir).iter().any(|id| !boot2_ids.contains(id))
    });
    std::thread::sleep(Duration::from_millis(1000));
    let third_ids: Vec<u64> = heap_ids(&dir)
        .into_iter()
        .filter(|id| !boot2_ids.contains(id))
        .collect();
    // Do not read X here: a read would promote it out of the cold tier.
    drop(c);
    srv.stop(true);

    // Boot 4: read it back.
    let srv = start(&dir, MAXMEMORY_BYTES);
    let mut c = srv.conn();
    let got = c.send(&["LRANGE", "X", "0", "-1"]);
    drop(c);
    srv.stop(false);
    let _ = std::fs::remove_dir_all(&dir);

    assert_eq!(
        got, "*1\r\n$1\r\na\r\n",
        "RPUSH X a ran once; after the restart LRANGE X read {got:?}. Boot 1's \
         orphaned id was {n}; boot 3 spilled into {third_ids:?}"
    );
    assert!(
        third_ids.iter().all(|id| *id > n),
        "boot 3 re-issued a file id at or below the orphaned {n}: {third_ids:?}"
    );
    assert_eq!(
        recorded_after_sweep,
        Some(FileStatus::Tombstone),
        "the sweep deleted heap-{n}.mpf without recording id {n} in the manifest"
    );
}
