//! Adversarial review of moon#1285 (wave 1): without an AOF, a snapshot taken
//! while a `TXN` is open holds the transaction's UNCOMMITTED writes, and the
//! later `TXN ABORT` has nothing durable to compensate them with. A restart
//! from that snapshot brings the aborted writes back.
//!
//! WS27 made the abort durable through AOF compensating records
//! (`DEL` / `RESTORE … ABSTTL`). With `--appendonly no` there is no log to
//! append them to, and the snapshot is the durability authority
//! (CLAUDE.md, "Without an AOF the last snapshot is the durability
//! authority"). WS27's `snapshot::txn_abort_tests` asserts the image keeps
//! the uncommitted value by design (point-in-time, needed for the AOF fold's
//! exactly-once contract) — correct under an AOF, a resurrection without one.
//!
//! Pre-existing: red on ce65400 and on f7f1d96, both runtimes (Linux
//! container, not merge bar):
//!
//! ```text
//! SET k original ; TXN BEGIN ; SET k aborted ; SET new inserted ;
//! BGSAVE (another connection, completes) ; TXN ABORT -> +OK ; GET k -> original
//! kill -9 ; restart ; GET k -> aborted ; GET new -> inserted
//! ```
//!
//!   MOON_BIN=... cargo test --test review_w1_txn_abort_no_aof_snapshot_1285 -- --include-ignored --test-threads 1

mod common;

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::Conn;

const OK: &str = "+OK\r\n";

fn bulk(s: &str) -> String {
    format!("${}\r\n{s}\r\n", s.len())
}

fn start(dir: &std::path::Path) -> (common::ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let dir_s = dir.to_str().expect("utf8 dir").to_string();
    let (guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--dir",
                &dir_s,
                "--appendonly",
                "no",
                "--save",
                "",
                "--disk-free-min-pct",
                "0",
            ])
            .env("RUST_LOG", "moon=warn")
            .stdout(Stdio::null())
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (set MOON_BIN)")
    });
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(mut c) = std::panic::catch_unwind(|| Conn::open(port))
            && c.send(&["PING"]) == "+PONG\r\n"
        {
            break;
        }
        assert!(Instant::now() < deadline, "server on {port} never answered");
        std::thread::sleep(Duration::from_millis(100));
    }
    (guard, port)
}

fn bgsave_and_wait(c: &mut Conn) {
    let lastsave = |c: &mut Conn| {
        c.send(&["LASTSAVE"])
            .trim_start_matches(':')
            .trim()
            .parse::<i64>()
            .unwrap_or(0)
    };
    let before = lastsave(c);
    std::thread::sleep(Duration::from_millis(1_100));
    let _ = c.send(&["BGSAVE"]);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let info = c.send(&["INFO", "persistence"]);
        if info.contains("rdb_bgsave_in_progress:0") && lastsave(c) > before {
            assert!(info.contains("rdb_last_bgsave_status:ok"), "{info}");
            return;
        }
        assert!(Instant::now() < deadline, "BGSAVE never completed");
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn an_abort_after_a_mid_txn_snapshot_survives_a_restart_without_an_aof() {
    let dir = common::unique_test_dir("rv-w1-txn-noaof");
    let (mut guard, port) = start(&dir);
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["SET", "k", "original"]), OK);
    let mut t = Conn::open(port);
    assert_eq!(t.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(t.send(&["SET", "k", "aborted"]), OK);
    assert_eq!(t.send(&["SET", "new", "inserted"]), OK);
    bgsave_and_wait(&mut c);
    assert_eq!(t.send(&["TXN", "ABORT"]), OK);
    assert_eq!(
        c.send(&["GET", "k"]),
        bulk("original"),
        "live, after the abort"
    );
    assert_eq!(c.send(&["GET", "new"]), "$-1\r\n", "live, after the abort");
    drop(t);
    drop(c);
    guard.kill_now();

    let (mut guard, port) = start(&dir);
    let mut c = Conn::open(port);
    let k = c.send(&["GET", "k"]);
    let new = c.send(&["GET", "new"]);
    guard.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        k == bulk("original") && new == "$-1\r\n",
        "an aborted TXN came back after kill -9 + restart from a snapshot taken while it was \
         open: GET k -> {k:?} (want \"original\"), GET new -> {new:?} (want nil)"
    );
}
