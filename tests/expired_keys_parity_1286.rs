//! moon#1286 residuals (wave-2a R1 review): `INFO stats` `expired_keys` must
//! match redis 7.2.7 where the first fix left gaps.
//!
//! - **Replay (finding 2):** redis counts nothing while it loads. The AOF
//!   holds every reap the live server made (the active cycle's reason `DEL`s,
//!   a client's `DEL` of an expired key, a write over one) and each was
//!   counted once, live; replaying them must not count them again. Oracle,
//!   `--appendonly yes`: 50 active-expired + 500 `HSET` over expired + 500
//!   `DEL` of expired = 1050 before a restart, **0** after a graceful and a
//!   kill -9 restart. f766fc2: 1050 after both, s1 and s4.
//!
//! Expected values were captured from redis 7.2.7 with the same commands.
//!
//! `MOON_BIN=<moon> cargo test --test expired_keys_parity_1286 -- --include-ignored`

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

mod common;

use std::path::Path;
use std::time::{Duration, Instant};

use common::Conn;

fn spawn(dir: &Path, shards: usize, appendonly: bool) -> (common::ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let dir = dir.to_path_buf();
    let (guard, port) = common::spawn_listening_guarded(move |port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                dir.to_str().unwrap(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                if appendonly { "yes" } else { "no" },
                "--appendfsync",
                "always",
                "--save",
                "",
                "--disk-offload",
                "disable",
                "--maxmemory",
                "0",
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(mut c) = std::panic::catch_unwind(|| Conn::open(port))
            && c.send(&["INFO", "persistence"]).contains("loading:0\r\n")
        {
            break;
        }
        assert!(Instant::now() < deadline, "server on {port} never loaded");
        std::thread::sleep(Duration::from_millis(100));
    }
    (guard, port)
}

fn expired_keys(c: &mut Conn) -> u64 {
    let info = c.send(&["INFO", "stats"]);
    info.lines()
        .find_map(|l| l.strip_prefix("expired_keys:"))
        .unwrap_or_else(|| panic!("no expired_keys in INFO stats: {info}"))
        .trim()
        .parse()
        .unwrap()
}

fn dbsize(c: &mut Conn) -> i64 {
    c.send(&["DBSIZE"])
        .trim_start_matches(':')
        .trim()
        .parse()
        .unwrap()
}

/// Run `verb key args…` for `key` in `prefix0..prefixN`, one pipeline.
fn each(c: &mut Conn, n: usize, prefix: &str, verb: &[&str], tail: &[&str]) -> String {
    let keys: Vec<String> = (0..n).map(|i| format!("{prefix}{i}")).collect();
    let cmds: Vec<Vec<&str>> = keys
        .iter()
        .map(|k| {
            let mut v = verb.to_vec();
            v.push(k);
            v.extend_from_slice(tail);
            v
        })
        .collect();
    let refs: Vec<&[&str]> = cmds.iter().map(Vec::as_slice).collect();
    c.pipeline(&refs)
}

/// SIGTERM (a graceful shutdown) and reap.
fn graceful_stop(guard: &mut common::ServerGuard) {
    let pid = guard.id().to_string();
    let _ = std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status();
    if let Some(mut child) = guard.take() {
        let deadline = Instant::now() + Duration::from_secs(30);
        while child.try_wait().ok().flatten().is_none() {
            if Instant::now() >= deadline {
                let _ = child.kill();
                panic!("the server did not stop on SIGTERM");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

/// Wait until `expired_keys` reaches `want` (or 10 s), returning the last read.
fn wait_for(c: &mut Conn, want: u64) -> u64 {
    let start = Instant::now();
    loop {
        let n = expired_keys(c);
        if n >= want || start.elapsed() >= Duration::from_secs(10) {
            return n;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ---------------------------------------------------------------------------
// Finding 2: AOF replay
// ---------------------------------------------------------------------------

fn replay_does_not_recount(shards: usize) {
    let dir = common::unique_test_dir(&format!("moon-1286-replay-s{shards}"));
    let (mut guard, port) = spawn(&dir, shards, true);
    let mut c = Conn::open(port);
    // Reaped by the active cycle: reason DELs in the AOF.
    each(&mut c, 50, "a", &["SET"], &["v", "PX", "50"]);
    // Reaped by a write over the expired key.
    each(&mut c, 500, "h", &["SET"], &["v", "PX", "30"]);
    std::thread::sleep(Duration::from_millis(40));
    each(&mut c, 500, "h", &["HSET"], &["f", "v"]);
    // Reaped by a client's DEL.
    each(&mut c, 500, "d", &["SET"], &["v", "PX", "30"]);
    std::thread::sleep(Duration::from_millis(40));
    each(&mut c, 500, "d", &["DEL"], &[]);
    let before = wait_for(&mut c, 1050);
    assert_eq!(before, 1050, "s{shards}: live count (redis 7.2.7: 1050)");
    assert_eq!(dbsize(&mut c), 500);
    drop(c);
    graceful_stop(&mut guard);

    let (mut guard, port) = spawn(&dir, shards, true);
    let mut c = Conn::open(port);
    std::thread::sleep(Duration::from_millis(500));
    let after_graceful = expired_keys(&mut c);
    let size_graceful = dbsize(&mut c);
    drop(c);
    guard.kill_now();

    let (mut guard, port) = spawn(&dir, shards, true);
    let mut c = Conn::open(port);
    std::thread::sleep(Duration::from_millis(500));
    let after_kill = expired_keys(&mut c);
    let size_kill = dbsize(&mut c);
    guard.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(
        (size_graceful, size_kill),
        (500, 500),
        "s{shards}: the replay restores the hashes"
    );
    assert_eq!(
        (after_graceful, after_kill),
        (0, 0),
        "s{shards}: expired_keys after a graceful and a kill -9 restart (redis 7.2.7: 0, 0): \
         the AOF replay counted the logged reaps again"
    );
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn aof_replay_does_not_recount_expired_keys_1_shard() {
    replay_does_not_recount(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn aof_replay_does_not_recount_expired_keys_4_shards() {
    replay_does_not_recount(4);
}
