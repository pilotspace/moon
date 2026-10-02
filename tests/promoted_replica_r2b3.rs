//! R2b round 3: a promoted replica's AOF after its master died inside a
//! transaction (F-A).
//!
//! A replica logs the master's stream, `MOON.TXN` markers included, into its
//! own AOF (moon#1318). When the master dies inside a `TXN`, the promotion
//! rolls the open block back in memory — and the AOF must end it at that
//! point too (`MOON.TXN RESET`), or the restart replays the local writes made
//! after the promotion against the dead block:
//!
//! 1. `INCR n` / `APPEND a X` after the promotion replayed on top of the
//!    master's uncommitted `SET a new; INCR n` (n = 2, a = newX);
//! 2. a local `TXN` got the same log id as the dead block, so its `END`
//!    committed the master's writes;
//! 3. a stream cut between `BEGIN` and `PAUSE` attributed every later plain
//!    local write to the dead block, rolled back at the end of the file.
//!
//! The master must be a monoio build (tokio has no master-side PSYNC):
//! `MOON_BIN_MONOIO` (skipped, saying so, without it). The replica is
//! `MOON_BIN`: run once per runtime.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
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
    let deadline = Instant::now() + Duration::from_secs(30);
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

/// The kill -9 lands right after the local writes were acknowledged, before
/// any rewrite could replace the AOF (the moon#1266 1A writes are in the
/// page cache before the reply).
fn crash_soon(srv: &mut common::ServerGuard) {
    std::thread::sleep(Duration::from_millis(100));
    srv.kill_now();
}

/// Cases 1 and 2: a real master dies inside an open `TXN`.
fn master_dies_inside_a_txn(name: &str, local_txn: bool) {
    let Some(master) = master_bin() else {
        eprintln!("SKIPPED: set MOON_BIN_MONOIO (the master needs master-side PSYNC)");
        return;
    };
    let replica = common::find_moon_binary();
    let dm = common::unique_test_dir(&format!("r2b3-{name}-m"));
    let dr = common::unique_test_dir(&format!("r2b3-{name}-r"));
    let (mut msrv, mport) = boot(&master, &dm, &["--appendonly", "no"], &[]);
    let (mut rsrv, rport) = boot(&replica, &dr, &["--appendonly", "yes"], &[]);
    let mut m = ready(mport);
    let mut r = ready(rport);
    assert_eq!(m.send(&["SET", "a", "old"]), "+OK\r\n");
    assert_eq!(
        r.send(&["REPLICAOF", "127.0.0.1", &mport.to_string()]),
        "+OK\r\n"
    );
    wait_until("the full sync", 30, || r.send(&["GET", "a"]) == bulk("old"));
    // The master's transaction: applied on the replica, never ended.
    assert_eq!(m.send(&["TXN", "BEGIN"]), "+OK\r\n");
    assert_eq!(m.send(&["SET", "a", "new"]), "+OK\r\n");
    assert_eq!(m.send(&["INCR", "n"]), ":1\r\n");
    wait_until("the TXN's records", 30, || {
        r.send(&["GET", "n"]) == bulk("1")
    });
    msrv.kill_now();
    drop(m);
    std::thread::sleep(Duration::from_millis(300));

    assert_eq!(r.send(&["REPLICAOF", "NO", "ONE"]), "+OK\r\n");
    assert_eq!(
        r.send(&["GET", "a"]),
        bulk("old"),
        "{name}: rolled back live"
    );
    if local_txn {
        assert_eq!(r.send(&["TXN", "BEGIN"]), "+OK\r\n");
        assert_eq!(r.send(&["SET", "loc", "1"]), "+OK\r\n");
        assert!(r.send(&["TXN", "COMMIT"]).starts_with('+'));
    }
    assert_eq!(r.send(&["INCR", "n"]), ":1\r\n");
    assert_eq!(r.send(&["APPEND", "a", "X"]), ":4\r\n");
    drop(r);
    crash_soon(&mut rsrv);

    let (mut rsrv, rport) = boot(&replica, &dr, &["--appendonly", "yes"], &[]);
    let mut r = ready(rport);
    assert_eq!(
        r.send(&["GET", "n"]),
        bulk("1"),
        "{name}: n as acknowledged"
    );
    assert_eq!(
        r.send(&["GET", "a"]),
        bulk("oldX"),
        "{name}: a as acknowledged"
    );
    if local_txn {
        assert_eq!(r.send(&["GET", "loc"]), bulk("1"), "{name}: the local TXN");
    }
    drop(r);
    rsrv.kill_now();
    let _ = std::fs::remove_dir_all(&dm);
    let _ = std::fs::remove_dir_all(&dr);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN (replica) and MOON_BIN_MONOIO (master)"]
fn local_writes_after_a_promotion_do_not_replay_against_the_dead_txn() {
    master_dies_inside_a_txn("plain", false);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN (replica) and MOON_BIN_MONOIO (master)"]
fn a_local_txn_after_a_promotion_does_not_commit_the_dead_txn() {
    master_dies_inside_a_txn("localtxn", true);
}

// ── Case 3: a fake master cuts the stream between BEGIN and PAUSE ─────────

fn read_line(r: &mut BufReader<TcpStream>) -> String {
    let mut l = String::new();
    r.read_line(&mut l).unwrap();
    l.trim_end().to_string()
}

/// A real master's full-sync RDB (PSYNC as a replica would).
fn capture_rdb(port: u16) -> Vec<u8> {
    let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut w = s.try_clone().unwrap();
    let mut r = BufReader::new(s);
    for cmd in [
        &["PING"][..],
        &["REPLCONF", "listening-port", "1"],
        &["REPLCONF", "capa", "eof", "capa", "psync2"],
    ] {
        w.write_all(&encode(cmd)).unwrap();
        read_line(&mut r);
    }
    w.write_all(&encode(&["PSYNC", "?", "-1"])).unwrap();
    assert!(read_line(&mut r).starts_with("+FULLRESYNC"));
    let len: usize = read_line(&mut r)[1..].parse().unwrap();
    let mut rdb = vec![0u8; len];
    r.read_exact(&mut rdb).unwrap();
    rdb
}

/// Serve one replica: the RDB, then `frames`, then hold the link and close.
fn fake_master(listener: TcpListener, rdb: Vec<u8>, frames: Vec<Vec<String>>, hold: Duration) {
    let (s, _) = listener.accept().unwrap();
    let mut w = s.try_clone().unwrap();
    let mut r = BufReader::new(s);
    loop {
        let n: usize = read_line(&mut r)[1..].parse().unwrap();
        let mut parts = Vec::new();
        for _ in 0..n {
            let len: usize = read_line(&mut r)[1..].parse().unwrap();
            let mut b = vec![0u8; len + 2];
            r.read_exact(&mut b).unwrap();
            parts.push(String::from_utf8_lossy(&b[..len]).to_uppercase());
        }
        match parts[0].as_str() {
            "PING" => w.write_all(b"+PONG\r\n").unwrap(),
            "REPLCONF" => w.write_all(b"+OK\r\n").unwrap(),
            "PSYNC" => {
                w.write_all(format!("+FULLRESYNC {} 0\r\n", "a".repeat(40)).as_bytes())
                    .unwrap();
                w.write_all(format!("${}\r\n", rdb.len()).as_bytes())
                    .unwrap();
                w.write_all(&rdb).unwrap();
                break;
            }
            _ => w.write_all(b"+OK\r\n").unwrap(),
        }
    }
    std::thread::sleep(Duration::from_millis(300));
    for f in &frames {
        let parts: Vec<&str> = f.iter().map(String::as_str).collect();
        w.write_all(&encode(&parts)).unwrap();
    }
    std::thread::sleep(hold);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN (replica) and MOON_BIN_MONOIO (master)"]
fn a_stream_cut_inside_a_txn_block_does_not_swallow_later_local_writes() {
    let Some(master) = master_bin() else {
        eprintln!("SKIPPED: set MOON_BIN_MONOIO (the master needs master-side PSYNC)");
        return;
    };
    let replica = common::find_moon_binary();
    let dm = common::unique_test_dir("r2b3-cut-m");
    let dr = common::unique_test_dir("r2b3-cut-r");
    let (mut msrv, mport) = boot(&master, &dm, &["--appendonly", "no"], &[]);
    let mut m = ready(mport);
    assert_eq!(m.send(&["SET", "a", "old"]), "+OK\r\n");
    assert_eq!(m.send(&["SET", "keep", "1"]), "+OK\r\n");
    let rdb = capture_rdb(mport);
    drop(m);
    msrv.kill_now();

    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let fport = listener.local_addr().unwrap().port();
    let frames: Vec<Vec<String>> = [
        vec!["SELECT", "0"],
        vec!["MOON.TXN", "BEGIN", "281474976710657"],
        vec!["SET", "a", "new"],
    ]
    .iter()
    .map(|f| f.iter().map(|s| s.to_string()).collect())
    .collect();
    let fm =
        std::thread::spawn(move || fake_master(listener, rdb, frames, Duration::from_millis(1500)));

    let (mut rsrv, rport) = boot(&replica, &dr, &["--appendonly", "yes"], &[]);
    let mut r = ready(rport);
    assert_eq!(
        r.send(&["REPLICAOF", "127.0.0.1", &fport.to_string()]),
        "+OK\r\n"
    );
    wait_until("the cut stream", 30, || {
        r.send(&["GET", "a"]) == bulk("new")
    });
    fm.join().unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(r.send(&["REPLICAOF", "NO", "ONE"]), "+OK\r\n");
    assert_eq!(r.send(&["GET", "a"]), bulk("old"), "rolled back live");
    assert_eq!(r.send(&["SET", "b", "acked"]), "+OK\r\n");
    assert_eq!(r.send(&["SET", "keep", "2"]), "+OK\r\n");
    drop(r);
    crash_soon(&mut rsrv);

    let (mut rsrv, rport) = boot(&replica, &dr, &["--appendonly", "yes"], &[]);
    let mut r = ready(rport);
    assert_eq!(
        r.send(&["GET", "a"]),
        bulk("old"),
        "the dead block stays rolled back"
    );
    assert_eq!(
        r.send(&["GET", "b"]),
        bulk("acked"),
        "a later local write survives"
    );
    assert_eq!(
        r.send(&["GET", "keep"]),
        bulk("2"),
        "a later local overwrite survives"
    );
    drop(r);
    rsrv.kill_now();
    let _ = std::fs::remove_dir_all(&dm);
    let _ = std::fs::remove_dir_all(&dr);
}

// ── X1: a stalled AOF writer throttles the link; nothing is dropped ───────

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

/// The replica's writer cannot fsync (`appendfsync always`, its fsync held
/// by `MOON_TEST_AOF_SYNC_GATE`) while the master streams 15k SETs. Before:
/// each logged record blocked the replica's shard thread up to 500 ms and
/// was then DROPPED from its AOF — the shard froze for seconds and the
/// promoted restart lost keys (reviewer `repl_stall.py`: 15-29). Now the
/// link waits for writer room (the shard keeps serving) and no record is
/// dropped.
#[test]
#[ignore = "spawns moon; set MOON_BIN (replica) and MOON_BIN_MONOIO (master)"]
fn a_stalled_replica_aof_writer_throttles_the_link_and_drops_nothing() {
    let Some(master) = master_bin() else {
        eprintln!("SKIPPED: set MOON_BIN_MONOIO (the master needs master-side PSYNC)");
        return;
    };
    const N: usize = 15_000;
    let replica = common::find_moon_binary();
    let dm = common::unique_test_dir("r2b3-stall-m");
    let dr = common::unique_test_dir("r2b3-stall-r");
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
    let pre: Vec<Vec<String>> = (0..1000)
        .map(|i| vec!["SET".into(), format!("pre{i}"), i.to_string()])
        .collect();
    pipeline(&mut m, &pre);
    assert_eq!(
        r.send(&["REPLICAOF", "127.0.0.1", &mport.to_string()]),
        "+OK\r\n"
    );
    wait_until("the full sync", 60, || r.send(&["DBSIZE"]) == ":1000\r\n");
    std::thread::sleep(Duration::from_millis(1500)); // the post-sync rewrite

    std::fs::write(&gate, b"x").unwrap();
    for s in (0..N).step_by(1000) {
        let batch: Vec<Vec<String>> = (s..(s + 1000).min(N))
            .map(|i| vec!["SET".into(), format!("k{i}"), i.to_string()])
            .collect();
        pipeline(&mut m, &batch);
    }
    // While the writer is held, the replica's shard keeps answering.
    let mut slowest = Duration::ZERO;
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(4) {
        let a = Instant::now();
        assert!(r.send(&["PING"]).starts_with("+PONG"));
        slowest = slowest.max(a.elapsed());
        std::thread::sleep(Duration::from_millis(100));
    }
    std::fs::remove_file(&gate).unwrap();
    let total = format!(":{}\r\n", N + 1000);
    wait_until("the replica to catch up", 120, || {
        r.send(&["DBSIZE"]) == total
    });
    eprintln!("slowest PING while the replica's AOF writer was held: {slowest:?}");
    assert!(
        slowest < Duration::from_millis(1500),
        "the replica's shard froze {slowest:?} while its AOF writer was held"
    );
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(r.send(&["REPLICAOF", "NO", "ONE"]), "+OK\r\n");
    assert_eq!(r.send(&["SET", "promoted", "1"]), "+OK\r\n");
    drop(m);
    msrv.kill_now();
    drop(r);
    std::thread::sleep(Duration::from_millis(1500));
    rsrv.kill_now();
    let log = std::fs::read_to_string(dr.join("server.err")).unwrap_or_default();
    assert!(
        !log.contains("NOT appended"),
        "a master-stream record was dropped from the replica's AOF"
    );

    let (mut rsrv, rport) = boot(&replica, &dr, &["--appendonly", "yes"], &[]);
    let mut r = ready(rport);
    wait_until("the restart", 60, || r.send(&["PING"]).starts_with("+PONG"));
    assert_eq!(
        r.send(&["DBSIZE"]),
        format!(":{}\r\n", N + 1001),
        "the promoted restart keeps every streamed key"
    );
    drop(r);
    rsrv.kill_now();
    let _ = std::fs::remove_dir_all(&dm);
    let _ = std::fs::remove_dir_all(&dr);
}
