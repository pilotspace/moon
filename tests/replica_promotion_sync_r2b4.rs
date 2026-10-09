//! R2b round 4 PROMO-RDB: a promotion while the full sync is in flight.
//!
//! A `REPLICAOF NO ONE` while the full-sync RDB was in flight promoted the
//! node; the old task then loaded the master's snapshot over it, wiping the
//! writes it acknowledged as a master (reviewer `slowsync.py`), and took
//! over its replication id when the `+FULLRESYNC` reply landed late.
//!
//! The RDB comes from a monoio master (`MOON_BIN_MONOIO`) — the tests are
//! skipped, saying so, without it; an in-test fake master then serves it
//! slowly. The replica is `MOON_BIN`: run once per runtime.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
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

fn master_bin() -> Option<PathBuf> {
    std::env::var_os("MOON_BIN_MONOIO").map(PathBuf::from)
}

fn bulk(v: &str) -> String {
    format!("${}\r\n{v}\r\n", v.len())
}

/// The value of `field` in `INFO replication`.
fn info_field(c: &mut Conn, field: &str) -> String {
    let info = c.send(&["INFO", "replication"]);
    info.split("\r\n")
        .find_map(|l| l.strip_prefix(field).and_then(|v| v.strip_prefix(':')))
        .unwrap_or_default()
        .to_string()
}

// ── PROMO-RDB ─────────────────────────────────────────────────────────────

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

/// Where the slow fake master stops until the test lets it go on.
#[derive(Clone, Copy, PartialEq)]
enum Stall {
    /// Before its `+FULLRESYNC` reply to PSYNC.
    BeforeFullResync,
    /// Halfway through the RDB bulk.
    MidRdb,
}

/// Serve one replica slowly: `reached` fires at `stall`, the sync goes on
/// when `resume` fires; the link is then held for a while and closed.
fn slow_fake_master(
    listener: TcpListener,
    rdb: Vec<u8>,
    stall: Stall,
    reached: mpsc::Sender<()>,
    resume: mpsc::Receiver<()>,
) {
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
            "PSYNC" => break,
            _ => w.write_all(b"+OK\r\n").unwrap(),
        }
    }
    if stall == Stall::BeforeFullResync {
        reached.send(()).unwrap();
        resume.recv().unwrap();
    }
    // A replica that gave up on this sync may have closed the link: the
    // writes after a stall are best-effort.
    let _ = w.write_all(format!("+FULLRESYNC {} 0\r\n", "f".repeat(40)).as_bytes());
    let _ = w.write_all(format!("${}\r\n", rdb.len()).as_bytes());
    let half = rdb.len() / 2;
    let _ = w.write_all(&rdb[..half]);
    if stall == Stall::MidRdb {
        reached.send(()).unwrap();
        resume.recv().unwrap();
    }
    let _ = w.write_all(&rdb[half..]);
    std::thread::sleep(Duration::from_millis(1500));
}

/// `REPLICAOF NO ONE` while the replica's sync is stalled at `stall`; the
/// promoted node acknowledges a write; the stalled sync then completes. The
/// node must stay the master it was promoted to: its acknowledged write
/// kept (live and after a restart from its AOF), the old master's data
/// never loaded, its own replication id kept.
fn promotion_during_a_stalled_sync(name: &str, stall: Stall) {
    let Some(master) = master_bin() else {
        eprintln!("SKIPPED: set MOON_BIN_MONOIO (the master needs master-side PSYNC)");
        return;
    };
    let replica = common::find_moon_binary();
    let dm = common::unique_test_dir(&format!("r2b4-{name}-m"));
    let dr = common::unique_test_dir(&format!("r2b4-{name}-r"));
    let (mut msrv, mport) = boot(&master, &dm, &["--appendonly", "no"], &[]);
    let mut m = ready(mport);
    assert_eq!(m.send(&["SET", "mkey", "master"]), "+OK\r\n");
    let rdb = capture_rdb(mport);
    drop(m);
    msrv.kill_now();

    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let fport = listener.local_addr().unwrap().port();
    let (reached_tx, reached_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let fm = std::thread::spawn(move || {
        slow_fake_master(listener, rdb, stall, reached_tx, resume_rx);
    });

    let (mut rsrv, rport) = boot(&replica, &dr, &["--appendonly", "yes"], &[]);
    let mut r = ready(rport);
    assert_eq!(r.send(&["SET", "pre", "1"]), "+OK\r\n");
    assert_eq!(
        r.send(&["REPLICAOF", "127.0.0.1", &fport.to_string()]),
        "+OK\r\n"
    );
    reached_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("the replica reached the stall");
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(r.send(&["REPLICAOF", "NO", "ONE"]), "+OK\r\n");
    assert_eq!(info_field(&mut r, "role"), "master");
    let replid = info_field(&mut r, "master_replid");
    assert_eq!(r.send(&["SET", "acked1", "1"]), "+OK\r\n");
    resume_tx.send(()).unwrap();
    fm.join().unwrap();
    std::thread::sleep(Duration::from_millis(500));

    assert_eq!(
        r.send(&["GET", "acked1"]),
        bulk("1"),
        "{name}: a write the promoted node acknowledged survives the late sync"
    );
    assert_eq!(
        r.send(&["GET", "pre"]),
        bulk("1"),
        "{name}: its own data stays"
    );
    assert_eq!(
        r.send(&["GET", "mkey"]),
        "$-1\r\n",
        "{name}: the old master's snapshot is never loaded"
    );
    assert_eq!(info_field(&mut r, "role"), "master");
    assert_eq!(
        info_field(&mut r, "master_replid"),
        replid,
        "{name}: the promoted node keeps its replication id"
    );
    drop(r);
    std::thread::sleep(Duration::from_millis(1500));
    rsrv.kill_now();

    let (mut rsrv, rport) = boot(&replica, &dr, &["--appendonly", "yes"], &[]);
    let mut r = ready(rport);
    assert_eq!(
        r.send(&["GET", "acked1"]),
        bulk("1"),
        "{name}: and after a restart from its AOF"
    );
    assert_eq!(r.send(&["GET", "mkey"]), "$-1\r\n");
    drop(r);
    rsrv.kill_now();
    let _ = std::fs::remove_dir_all(&dm);
    let _ = std::fs::remove_dir_all(&dr);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN (replica) and MOON_BIN_MONOIO (master)"]
fn a_promotion_during_the_rdb_transfer_is_not_wiped_by_the_snapshot() {
    promotion_during_a_stalled_sync("midrdb", Stall::MidRdb);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN (replica) and MOON_BIN_MONOIO (master)"]
fn a_promotion_before_the_fullresync_reply_keeps_the_node_a_master() {
    promotion_during_a_stalled_sync("prefullresync", Stall::BeforeFullResync);
}
