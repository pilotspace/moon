//! R2b round 2 R1 (moon#1318): a promoted replica's restart keeps the
//! master's dataset, the stream it applied, and its own later writes.
//!
//! A replica's AOF is its only KV source at boot (R2b P1, redis's rule), but
//! nothing wrote the full sync's dataset or the applied master stream to it:
//! reviewer — monoio master 100 keys, tokio replica synced, BGSAVE,
//! `REPLICAOF NO ONE`, `SET promoted 1`, kill -9, restart -> DBSIZE 1
//! (monoio replicas too). Now the sync publishes a new AOF generation whose
//! base is the synced dataset (redis `restartAOFAfterSYNC`) and every applied
//! KV record is appended to the replica's own AOF.
//!
//! The master must be a monoio build (tokio has no master-side PSYNC):
//! `MOON_BIN_MONOIO` (the test FAILS without it — it used to pass without
//! running, R2b round 3 F-J). The replica is `MOON_BIN` — run once
//! per runtime. Replicas are single-shard (`replica_supported`, moon#406).
//!
//! ```text
//! MOON_BIN=<moon> MOON_BIN_MONOIO=<monoio moon> \
//!   cargo test --release --test promoted_replica_restart_r2b2 -- --include-ignored
//! ```

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::Conn;

fn boot(bin: &Path, dir: &Path) -> (common::ServerGuard, u16) {
    std::fs::create_dir_all(dir).unwrap();
    common::spawn_listening_guarded(|port| {
        Command::new(bin)
            .args(["--port", &port.to_string(), "--shards", "1", "--dir"])
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

fn wait_until(what: &str, mut ok: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ok() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The master's binary. Panics without `MOON_BIN_MONOIO` (R2b round 3 F-J:
/// returning `None` made the case pass without running).
fn master_bin() -> Option<PathBuf> {
    Some(common::required_runtime_bin("MOON_BIN_MONOIO"))
}

/// `with_stream`: the master also writes AFTER the sync (non-idempotent
/// INCR / RPUSH), so the applied stream must be in the replica's AOF.
fn promoted_restart(name: &str, with_stream: bool) {
    let Some(master) = master_bin() else {
        eprintln!("SKIPPED: set MOON_BIN_MONOIO (the master needs master-side PSYNC)");
        return;
    };
    let replica = common::find_moon_binary();
    let dm = common::unique_test_dir(&format!("promoted-{name}-m"));
    let dr = common::unique_test_dir(&format!("promoted-{name}-r"));
    let (mut msrv, mport) = boot(&master, &dm);
    let mut m = ready(mport);
    for i in 0..100 {
        assert_eq!(
            m.send(&["SET", &format!("k{i}"), &format!("v{i}")]),
            "+OK\r\n"
        );
    }

    let (mut rsrv, rport) = boot(&replica, &dr);
    let mut r = ready(rport);
    assert_eq!(
        r.send(&["REPLICAOF", "127.0.0.1", &mport.to_string()]),
        "+OK\r\n"
    );
    wait_until("the full sync", || r.send(&["DBSIZE"]) == ":100\r\n");
    let mut expected = 101;
    if with_stream {
        for _ in 0..10 {
            m.send(&["INCR", "c"]);
            m.send(&["RPUSH", "l", "x"]);
        }
        wait_until("the stream", || r.send(&["GET", "c"]) == "$2\r\n10\r\n");
        wait_until("the stream", || r.send(&["LLEN", "l"]) == ":10\r\n");
        expected += 2;
    }
    // The reviewer's shape: a snapshot on the replica, then the promotion.
    assert!(r.send(&["BGSAVE"]).starts_with('+'));
    wait_until("BGSAVE", || {
        r.send(&["INFO", "persistence"])
            .contains("rdb_bgsave_in_progress:0")
    });
    assert_eq!(r.send(&["REPLICAOF", "NO", "ONE"]), "+OK\r\n");
    assert_eq!(r.send(&["SET", "promoted", "1"]), "+OK\r\n");
    drop(m);
    msrv.kill_now();
    drop(r);
    std::thread::sleep(Duration::from_millis(1500));
    rsrv.kill_now();

    let (mut rsrv, rport) = boot(&replica, &dr);
    let mut r = ready(rport);
    assert_eq!(
        r.send(&["DBSIZE"]),
        format!(":{expected}\r\n"),
        "{name}: the promoted replica's restart keeps the master's data and its own write"
    );
    assert_eq!(r.send(&["GET", "k42"]), "$3\r\nv42\r\n");
    assert_eq!(r.send(&["GET", "promoted"]), "$1\r\n1\r\n");
    if with_stream {
        assert_eq!(
            r.send(&["GET", "c"]),
            "$2\r\n10\r\n",
            "{name}: INCR once each"
        );
        assert_eq!(r.send(&["LLEN", "l"]), ":10\r\n", "{name}: RPUSH once each");
    }
    drop(r);
    rsrv.kill_now();
    let _ = std::fs::remove_dir_all(&dm);
    let _ = std::fs::remove_dir_all(&dr);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN (replica) and MOON_BIN_MONOIO (master)"]
fn a_promoted_replica_restart_keeps_the_synced_dataset() {
    promoted_restart("sync", false);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN (replica) and MOON_BIN_MONOIO (master)"]
fn a_promoted_replica_restart_keeps_the_applied_stream_once() {
    promoted_restart("stream", true);
}
