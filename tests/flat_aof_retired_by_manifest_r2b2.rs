//! R2b round 2 F2: once a manifest owns a dir's AOF, the flat
//! `appendonly.aof` must be retired in EVERY branch that creates one.
//!
//! tokio `--shards 1` opens `appendonly.aof` at its first boot (at least the
//! `MOON.COLDCUT` head). A monoio `--shards 1` boot of that dir replays it
//! into an empty keyspace and takes the "fresh boot" branch, which created
//! the manifest but left the flat file in place. A later tokio boot then took
//! the stale non-empty file as its only KV source (`KvSources::AofOnly`) and
//! came up EMPTY (reviewer: DBSIZE 0, the snapshot held 20), appending to
//! the stale file.
//!
//! Needs both runtimes' binaries (the cases FAIL without them — they used to
//! pass without running, R2b round 3 F-J):
//!
//! ```text
//! MOON_BIN_TOKIO=<tokio moon> MOON_BIN_MONOIO=<monoio moon> \
//!   cargo test --release --test flat_aof_retired_by_manifest_r2b2 -- --include-ignored
//! ```

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::Conn;

fn boot(bin: &Path, dir: &Path, shards: usize) -> (common::ServerGuard, u16) {
    common::spawn_listening_guarded(|port| {
        Command::new(bin)
            .args(["--port", &port.to_string(), "--shards", &shards.to_string()])
            .arg("--dir")
            .arg(dir)
            .args(["--disk-free-min-pct", "0", "--appendonly", "yes"])
            .env("MOON_DISK_FREE_MIN_PCT", "0")
            .stdout(Stdio::null())
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon")
    })
}

fn ready(port: u16) -> Conn {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            let mut c = Conn::open(port);
            if c.send(&["PING"]).starts_with("+PONG") {
                return c;
            }
        }
        assert!(Instant::now() < deadline, "moon never answered PING");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn bgsave(c: &mut Conn) {
    assert!(c.send(&["BGSAVE"]).starts_with('+'));
    let deadline = Instant::now() + Duration::from_secs(30);
    while !c
        .send(&["INFO", "persistence"])
        .contains("rdb_bgsave_in_progress:0")
    {
        assert!(Instant::now() < deadline, "BGSAVE never finished");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Both runtimes' binaries. Panics without them (R2b round 3 F-J: returning
/// `None` made the case pass without running).
fn bins() -> Option<(PathBuf, PathBuf)> {
    Some((
        common::required_runtime_bin("MOON_BIN_TOKIO"),
        common::required_runtime_bin("MOON_BIN_MONOIO"),
    ))
}

fn tokio_monoio_tokio(shards_mid: usize) {
    let Some((tokio, monoio)) = bins() else {
        eprintln!("SKIPPED: set MOON_BIN_TOKIO and MOON_BIN_MONOIO");
        return;
    };
    let dir = common::unique_test_dir(&format!("flat-retired-s{shards_mid}"));
    std::fs::create_dir_all(&dir).unwrap();
    // 1. tokio s1, fresh: opens appendonly.aof (its head).
    let (mut srv, port) = boot(&tokio, &dir, 1);
    drop(ready(port));
    srv.kill_now();
    assert!(
        dir.join("appendonly.aof").exists(),
        "tokio opened the flat AOF"
    );

    // 2. monoio boot of the same dir: writes, BGSAVE.
    let (mut srv, port) = boot(&monoio, &dir, shards_mid);
    let mut c = ready(port);
    for i in 0..20 {
        assert_eq!(c.send(&["SET", &format!("m{i}"), "v"]), "+OK\r\n");
    }
    bgsave(&mut c);
    drop(c);
    std::thread::sleep(Duration::from_millis(1200));
    srv.kill_now();
    assert!(
        !dir.join("appendonly.aof").exists(),
        "the manifest boot retired the flat AOF"
    );
    assert!(
        dir.join("appendonly.aof.legacy").exists(),
        "renamed, not deleted"
    );

    // 3. tokio s1 again: no stale flat AOF, so the snapshot is the base.
    if shards_mid == 1 {
        let (mut srv, port) = boot(&tokio, &dir, 1);
        let mut c = ready(port);
        assert_eq!(c.send(&["DBSIZE"]), ":20\r\n", "tokio kept the monoio data");
        drop(c);
        srv.kill_now();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN_TOKIO and MOON_BIN_MONOIO"]
fn a_monoio_fresh_boot_retires_a_head_only_flat_aof() {
    tokio_monoio_tokio(1);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN_TOKIO and MOON_BIN_MONOIO"]
fn a_multi_shard_fresh_boot_retires_the_flat_aof() {
    tokio_monoio_tokio(4);
}
