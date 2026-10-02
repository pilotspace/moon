//! R2b round 4: boot/replay fixes, against real servers.
//!
//! - **F2** — a write whose logged effect names more items than the client
//!   protocol's element cap (`SPOP s 1060000` was logged as ONE `SREM` of
//!   1,060,000 members) made the next boot refuse the AOF as "mid-file
//!   corruption" (and the truncation it advised dropped every later write).
//!   The effect is now logged in chunks and the replay parser has no element
//!   cap; the restart keeps the pop and the later write.
//!
//! ```text
//! MOON_BIN=<moon> cargo test --release --test aof_boot_r2b4 -- --include-ignored
//! ```

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::Conn;

fn boot(dir: &Path, shards: usize) -> (common::ServerGuard, u16) {
    std::fs::create_dir_all(dir).unwrap();
    let bin = common::find_moon_binary();
    common::spawn_listening_guarded(|port| {
        Command::new(&bin)
            .args(["--port", &port.to_string(), "--shards", &shards.to_string()])
            .arg("--dir")
            .arg(dir)
            .args([
                "--disk-free-min-pct",
                "0",
                "--appendonly",
                "yes",
                "--appendfsync",
                "always",
                "--maxmemory",
                "0",
            ])
            .env("MOON_DISK_FREE_MIN_PCT", "0")
            .stdout(Stdio::null())
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (set MOON_BIN)")
    })
}

fn ready(port: u16) -> Conn {
    let deadline = Instant::now() + Duration::from_secs(120);
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

/// F2: SADD 1.1M members, SPOP 1,060,000, SET after, kill -9, restart.
fn large_spop_survives_a_restart(shards: usize) {
    const MEMBERS: usize = 1_100_000;
    const POP: usize = 1_060_000;
    let dir = common::unique_test_dir(&format!("r2b4-spop-s{shards}"));
    let (mut srv, port) = boot(&dir, shards);
    let mut c = ready(port);
    let mut names: Vec<String> = Vec::with_capacity(10_002);
    for start in (0..MEMBERS).step_by(10_000) {
        names.clear();
        names.push("SADD".into());
        names.push("{s}set".into());
        names.extend((start..start + 10_000).map(|i| format!("m{i}")));
        let parts: Vec<&str> = names.iter().map(String::as_str).collect();
        assert_eq!(c.send(&parts), ":10000\r\n");
    }
    let popped = c.send_within(
        &["SPOP", "{s}set", &POP.to_string()],
        Duration::from_secs(120),
    );
    assert!(popped.starts_with(&format!("*{POP}\r\n")), "SPOP answered");
    assert_eq!(c.send(&["SET", "after", "1"]), "+OK\r\n");
    drop(c);
    srv.kill_now();

    let (mut srv, port) = boot(&dir, shards);
    let mut c = ready(port);
    assert_eq!(
        c.send(&["SCARD", "{s}set"]),
        format!(":{}\r\n", MEMBERS - POP),
        "s{shards}: the pop survives the restart"
    );
    assert_eq!(
        c.send(&["GET", "after"]),
        "$1\r\n1\r\n",
        "s{shards}: the write after it survives"
    );
    drop(c);
    srv.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn a_huge_spop_replays_after_a_restart_1_shard() {
    large_spop_survives_a_restart(1);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn a_huge_spop_replays_after_a_restart_4_shards() {
    large_spop_survives_a_restart(4);
}
