//! R2b round 4 X1-DBL: a replica task replaced while it waits mid-batch.
//!
//! Since round 3 the stream loop may wait for room in the replica's AOF
//! writer between two records of a batch. A `REPLICAOF` arriving then
//! superseded the task, which exited without counting the records it had
//! already applied; the next task's `PSYNC` resumed at the batch's start
//! (`+CONTINUE`) and the master's re-sent prefix was applied — and logged —
//! twice (reviewer `dbl.py`: 20 000 `INCR`s gave 21 004). Also redis's
//! same-target no-op: `REPLICAOF <the current master>` changes nothing.
//!
//! The master is a monoio build (`MOON_BIN_MONOIO`; tokio has no master-side
//! PSYNC) — the tests are skipped, saying so, without it. The replica is
//! `MOON_BIN`: run once per runtime.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::io::Write;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::{Conn, encode};

fn boot(
    bin: &Path,
    dir: &Path,
    extra: &[&str],
    env: &[(&str, &str)],
) -> (common::ServerGuard, u16) {
    std::fs::create_dir_all(dir).unwrap();
    common::spawn_listening_guarded(|port| {
        let mut c = Command::new(bin);
        c.args(["--port", &port.to_string(), "--shards", "1", "--dir"])
            .arg(dir)
            .args(["--disk-free-min-pct", "0"])
            .args(extra)
            .env("MOON_DISK_FREE_MIN_PCT", "0")
            .stdout(Stdio::null())
            .stderr(common::server_stderr(dir));
        for (k, v) in env {
            c.env(k, v);
        }
        c.spawn().expect("spawn moon")
    })
}

fn ready(port: u16) -> Conn {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            let mut c = Conn::open(port);
            if c.send(&["PING"]).starts_with("+PONG") {
                return c;
            }
        }
        assert!(Instant::now() < deadline, "moon never answered PING");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_until(what: &str, secs: u64, mut ok: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !ok() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn master_bin() -> Option<PathBuf> {
    std::env::var_os("MOON_BIN_MONOIO").map(PathBuf::from)
}

fn bulk(v: &str) -> String {
    format!("${}\r\n{v}\r\n", v.len())
}

/// Pipeline `cmds` and read every reply.
fn pipeline(c: &mut Conn, cmds: &[Vec<String>]) {
    let mut out = Vec::new();
    for cmd in cmds {
        let parts: Vec<&str> = cmd.iter().map(String::as_str).collect();
        out.extend_from_slice(&encode(&parts));
    }
    c.sock.write_all(&out).unwrap();
    let reply = c.read_replies_within(cmds.len(), Duration::from_secs(120));
    assert!(
        !reply.contains("\r\n-") && !reply.starts_with('-'),
        "{reply:.200}"
    );
}

/// `GET key` once it has not changed for a second.
fn settled(c: &mut Conn, key: &str, secs: u64) -> String {
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut last = c.send(&["GET", key]);
    let mut since = Instant::now();
    while since.elapsed() < Duration::from_secs(1) {
        assert!(
            Instant::now() < deadline,
            "{key} never settled (last {last:?})"
        );
        std::thread::sleep(Duration::from_millis(100));
        let now = c.send(&["GET", key]);
        if now != last {
            last = now;
            since = Instant::now();
        }
    }
    last
}

// ── X1-DBL ────────────────────────────────────────────────────────────────

/// The reviewer's `dbl.py`: the replica's AOF fsync is held
/// (`appendfsync always` + `MOON_TEST_AOF_SYNC_GATE`), so its stream loop
/// parks for writer room mid-batch while the master runs `N` `INCR`s; a
/// `REPLICAOF` (naming the master as `reattach_host`) lands while it is
/// parked; the gate is released. `c` must end at exactly `N`, live and —
/// when `restart` — after a promotion and a restart from the replica's AOF.
fn incr_stream_with_a_replicaof_while_parked(name: &str, reattach_host: &str, restart: bool) {
    let Some(master) = master_bin() else {
        eprintln!("SKIPPED: set MOON_BIN_MONOIO (the master needs master-side PSYNC)");
        return;
    };
    const N: usize = 20_000;
    let replica = common::find_moon_binary();
    let dm = common::unique_test_dir(&format!("r2b4-{name}-m"));
    let dr = common::unique_test_dir(&format!("r2b4-{name}-r"));
    std::fs::create_dir_all(&dr).unwrap();
    let gate = dr.join("sync.gate");
    let gate_s = gate.to_string_lossy().to_string();
    let (mut msrv, mport) = boot(&master, &dm, &["--appendonly", "no"], &[]);
    let replica_args = [
        "--appendonly",
        "yes",
        "--appendfsync",
        "always",
        "--auto-aof-rewrite-percentage",
        "0",
    ];
    let (mut rsrv, rport) = boot(
        &replica,
        &dr,
        &replica_args,
        &[("MOON_TEST_AOF_SYNC_GATE", gate_s.as_str())],
    );
    let mut m = ready(mport);
    let mut r = ready(rport);
    assert_eq!(m.send(&["SET", "seed", "1"]), "+OK\r\n");
    assert_eq!(
        r.send(&["REPLICAOF", "127.0.0.1", &mport.to_string()]),
        "+OK\r\n"
    );
    wait_until("the full sync", 30, || r.send(&["DBSIZE"]) == ":1\r\n");
    std::thread::sleep(Duration::from_millis(1500)); // the post-sync rewrite

    std::fs::write(&gate, b"x").unwrap();
    for s in (0..N).step_by(1000) {
        let batch: Vec<Vec<String>> = (s..(s + 1000).min(N))
            .map(|_| vec!["INCR".into(), "c".into()])
            .collect();
        pipeline(&mut m, &batch);
    }
    assert_eq!(m.send(&["GET", "c"]), bulk(&N.to_string()));
    std::thread::sleep(Duration::from_millis(1000));
    let reattach = r.send(&["REPLICAOF", reattach_host, &mport.to_string()]);
    if reattach_host == "127.0.0.1" {
        assert_eq!(
            reattach, "+OK Already connected to specified master\r\n",
            "{name}: REPLICAOF naming the current master is a no-op, as in redis"
        );
    } else {
        assert_eq!(
            reattach, "+OK\r\n",
            "{name}: another name restarts the link"
        );
    }
    std::thread::sleep(Duration::from_millis(1000));
    std::fs::remove_file(&gate).unwrap();
    assert_eq!(
        settled(&mut r, "c", 120),
        bulk(&N.to_string()),
        "{name}: every INCR applied exactly once"
    );

    if restart {
        std::thread::sleep(Duration::from_millis(1500));
        assert_eq!(r.send(&["REPLICAOF", "NO", "ONE"]), "+OK\r\n");
        drop(r);
        std::thread::sleep(Duration::from_millis(1500));
        rsrv.kill_now();
        let (mut rsrv2, rport) = boot(&replica, &dr, &["--appendonly", "yes"], &[]);
        let mut r = ready(rport);
        assert_eq!(
            r.send(&["GET", "c"]),
            bulk(&N.to_string()),
            "{name}: the replica's AOF logged every INCR exactly once"
        );
        drop(r);
        rsrv2.kill_now();
    } else {
        drop(r);
        rsrv.kill_now();
    }
    drop(m);
    msrv.kill_now();
    let _ = std::fs::remove_dir_all(&dm);
    let _ = std::fs::remove_dir_all(&dr);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN (replica) and MOON_BIN_MONOIO (master)"]
fn replicaof_the_current_master_is_a_no_op_and_applies_nothing_twice() {
    incr_stream_with_a_replicaof_while_parked("same", "127.0.0.1", false);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN (replica) and MOON_BIN_MONOIO (master)"]
fn a_task_superseded_while_parked_resumes_after_what_it_applied() {
    incr_stream_with_a_replicaof_while_parked("alias", "localhost", true);
}
