//! R2b round 2 F1: an `appendonly.aof` that is the boot's only KV source
//! (`KvSources::AofOnly`) and cannot be replayed must stop the boot.
//!
//! Before: an RDB preamble that does not load ("no valid EOF+CRC found")
//! logged one ERROR and the server booted EMPTY, served, and appended new
//! writes behind the unreadable bytes — so they were lost on the next boot
//! too (reviewer: 50 keys -> DBSIZE 0, and a later SET gone). redis 7.2.7
//! exits with status 1 on such a file. Now moon exits with status 1, names
//! the file and the remedy, and leaves the file byte-for-byte untouched.
//!
//! Both runtimes, v3 (disk-offload, the default) and v2
//! (`--disk-offload disable`) recovery paths. The dataset is saved under
//! `--appendonly no` so neither runtime has a manifest yet; the flat file is
//! then the legacy layout's authority on both.
//!
//! ```text
//! MOON_BIN=<moon> cargo test --release --test flat_aof_unreadable_refusal_r2b2 -- --include-ignored
//! ```

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::Conn;

fn boot(dir: &Path, extra: &[&str]) -> (common::ServerGuard, u16) {
    let bin = common::find_moon_binary();
    common::spawn_listening_guarded(|port| {
        Command::new(&bin)
            .args(["--port", &port.to_string(), "--shards", "1", "--dir"])
            .arg(dir)
            .args(["--disk-free-min-pct", "0"])
            .args(extra)
            .env("MOON_DISK_FREE_MIN_PCT", "0")
            .stdout(Stdio::null())
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (set MOON_BIN)")
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

fn refuses(extra: &[&str], name: &str) {
    let dir = common::unique_test_dir(&format!("aof-refusal-{name}"));
    std::fs::create_dir_all(&dir).unwrap();
    let mut no = vec!["--appendonly", "no"];
    no.extend_from_slice(extra);
    let (mut srv, port) = boot(&dir, &no);
    let mut c = ready(port);
    for i in 0..50 {
        assert_eq!(c.send(&["SET", &format!("k{i}"), "v"]), "+OK\r\n");
    }
    assert!(c.send(&["BGSAVE"]).starts_with('+'));
    let deadline = Instant::now() + Duration::from_secs(30);
    while !c
        .send(&["INFO", "persistence"])
        .contains("rdb_bgsave_in_progress:0")
    {
        assert!(Instant::now() < deadline, "save never finished");
        std::thread::sleep(Duration::from_millis(20));
    }
    drop(c);
    srv.kill_now();

    // An RDB preamble that does not load: the magic, then garbage.
    let aof = dir.join("appendonly.aof");
    let mut bytes = b"MOON".to_vec();
    bytes.extend_from_slice(&[0xff; 60]);
    std::fs::write(&aof, &bytes).unwrap();

    let bin = common::find_moon_binary();
    let mut child = Command::new(&bin)
        .args([
            "--port",
            &common::reserve_port().to_string(),
            "--shards",
            "1",
            "--dir",
        ])
        .arg(&dir)
        .args(["--disk-free-min-pct", "0", "--appendonly", "yes"])
        .args(extra)
        .env("MOON_DISK_FREE_MIN_PCT", "0")
        .stdout(Stdio::null())
        .stderr(common::server_stderr(&dir))
        .spawn()
        .expect("spawn moon");
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break s;
        }
        if Instant::now() >= deadline {
            common::sigkill(&mut child);
            panic!("{name}: moon booted (or hung) on an unreadable appendonly.aof");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(status.code(), Some(1), "{name}: exit status");
    let log = std::fs::read_to_string(dir.join("server.err")).unwrap_or_default();
    assert!(
        log.contains("refusing to start") && log.contains("appendonly.aof"),
        "{name}: the refusal names the file: {log:.2000}"
    );
    assert_eq!(
        std::fs::read(&aof).unwrap(),
        bytes,
        "{name}: nothing was appended to the unreadable file"
    );

    // The documented remedy: move it aside and boot from the snapshot.
    std::fs::rename(&aof, dir.join("appendonly.aof.bad")).unwrap();
    let mut yes = vec!["--appendonly", "yes"];
    yes.extend_from_slice(extra);
    let (mut srv, port) = boot(&dir, &yes);
    let mut c = ready(port);
    assert_eq!(c.send(&["DBSIZE"]), ":50\r\n", "{name}: the snapshot loads");
    srv.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn an_unreadable_flat_aof_stops_the_boot_v3_path() {
    refuses(&[], "v3");
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn an_unreadable_flat_aof_stops_the_boot_v2_path() {
    refuses(&["--disk-offload", "disable"], "v2");
}
