//! moon#1227 review (refs moon#1179): a pipeline longer than the 1024-frame
//! parse cap is answered in full.
//!
//! The batch parse stops at 1024 frames and leaves the rest in the read buffer.
//! That remainder must be parsed BEFORE the connection waits on the socket
//! again: the client has already sent it and is waiting for its replies, so a
//! read would wait forever. Direct reads into the read buffer (moon#1179 item 4)
//! made one read deliver more than 1024 frames routinely, where an 8 KiB rent
//! buffer rarely held that many — a fresh connection pipelining 2250 PINGs got
//! 2194 replies and then nothing.
//!
//! Every case drives a real server over a raw socket and counts COMPLETE
//! replies against a deadline, so a stranded tail fails fast with the count
//! instead of hanging the suite. Runs against whichever runtime the binary was
//! built with: the monoio handler by default, the tokio one under
//! `--no-default-features --features runtime-tokio,jemalloc`. Set `MOON_BIN`
//! to pin the server binary.

mod common;

use common::{Conn, encode, framed_len};

use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

/// The parse cap both connection handlers apply to one batch.
const CAP: usize = 1024;

/// How long a pipeline may take to be answered before the test calls it a hang.
const ANSWER_BUDGET: Duration = Duration::from_secs(15);

struct Moon {
    child: Child,
    port: u16,
    tmp_dir: std::path::PathBuf,
}

impl Drop for Moon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.tmp_dir);
    }
}

/// Port locks held for the life of the test process (see `claim`).
static CLAIMED: LazyLock<Mutex<Vec<moon::persistence::dir_lock::DirLock>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

/// Claim `port` against every other test process AND every other test thread
/// here, the way `common::reserve_port` does (an exclusive flock per port, the
/// same lock path, so the two schemes can never hand out one port twice), and
/// check that nothing is listening on it.
fn claim(port: u16) -> bool {
    let dir = std::env::temp_dir()
        .join("moon-test-ports")
        .join(port.to_string());
    if std::fs::create_dir_all(&dir).is_err() {
        return false;
    }
    let Ok(lock) = moon::persistence::dir_lock::acquire(&dir) else {
        return false;
    };
    if std::net::TcpListener::bind(("127.0.0.1", port)).is_err() {
        return false;
    }
    CLAIMED.lock().unwrap().push(lock);
    true
}

/// Candidate ports: this suite's window 7240–7249 first, then the shared
/// harness's reservation once the window is used up (a concurrent run of the
/// other runtime's leg, say).
fn next_port() -> u16 {
    (7240..=7249)
        .find(|&p| claim(p))
        .unwrap_or_else(common::reserve_port)
}

fn spawn_moon(shards: &str) -> Moon {
    let bin = common::find_moon_binary();
    for _attempt in 0..5 {
        let port = next_port();
        let tmp_dir = std::env::temp_dir().join(format!("moon-ws4-cap-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
        let child = Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                shards,
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                "--disk-free-min-pct",
                "0",
                "--dir",
                tmp_dir.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon");
        let mut moon = Moon {
            child,
            port,
            tmp_dir,
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(Some(_)) = moon.child.try_wait() {
                break; // died (a port race): next port
            }
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok()
                && Conn::open(port).send(&["PING"]) == "+PONG\r\n"
            {
                return moon;
            }
            assert!(Instant::now() < deadline, "moon never became ready");
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    panic!("moon failed to start on five ports");
}

/// Write `payload` in ONE write, then read until `want` complete replies have
/// arrived or [`ANSWER_BUDGET`] runs out. Returns the replies as text; panics
/// with the number of replies received when the server stops short.
fn send_and_collect(c: &mut Conn, payload: &[u8], want: usize) -> String {
    c.sock.write_all(payload).expect("write");
    let deadline = Instant::now() + ANSWER_BUDGET;
    c.sock
        .set_read_timeout(Some(Duration::from_millis(200)))
        .expect("read timeout");
    let mut got: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 65536];
    loop {
        if let Some(n) = framed_len(&got, want) {
            assert_eq!(n, got.len(), "more bytes than {want} replies");
            return String::from_utf8_lossy(&got).into_owned();
        }
        if Instant::now() >= deadline {
            panic!(
                "stranded pipeline: {}/{want} replies answered within {ANSWER_BUDGET:?}",
                complete_replies(&got)
            );
        }
        match c.sock.read(&mut chunk) {
            Ok(0) => panic!(
                "server closed after {}/{want} replies",
                complete_replies(&got)
            ),
            Ok(n) => got.extend_from_slice(&chunk[..n]),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(e) => panic!("read failed: {e}"),
        }
    }
}

/// Complete top-level replies at the front of `buf`.
fn complete_replies(buf: &[u8]) -> usize {
    let mut off = 0;
    let mut n = 0;
    while let Some(len) = framed_len(&buf[off..], 1) {
        off += len;
        n += 1;
    }
    n
}

fn pings(n: usize) -> Vec<u8> {
    encode(&["PING"]).repeat(n)
}

fn pongs(n: usize) -> String {
    "+PONG\r\n".repeat(n)
}

/// The review's first reproduction: a fresh connection, 2250 RESP PINGs in one
/// write. Pre-fix: 2194/2250 answered, then the connection waited on a read.
#[test]
fn fresh_connection_answers_a_2250_ping_pipeline() {
    let moon = spawn_moon("1");
    let mut c = Conn::open(moon.port);
    assert_eq!(send_and_collect(&mut c, &pings(2250), 2250), pongs(2250));
    // The connection is still in step afterwards.
    assert_eq!(c.send(&["PING"]), "+PONG\r\n");
}

/// A connection whose read buffer has already grown (a warm-up of 3000 PINGs
/// in groups of 750, each answered) takes a pipeline past the cap in ONE read.
/// Pre-fix: 1024/1100.
#[test]
fn warm_connection_answers_a_pipeline_past_the_cap() {
    let moon = spawn_moon("1");
    let mut c = Conn::open(moon.port);
    for _ in 0..4 {
        assert_eq!(send_and_collect(&mut c, &pings(750), 750), pongs(750));
    }
    assert_eq!(send_and_collect(&mut c, &pings(1100), 1100), pongs(1100));
    assert_eq!(c.send(&["PING"]), "+PONG\r\n");
}

/// A large PUBLISH grows the read buffer (its bulk is read in one hinted
/// read); the 1100 INCRs behind it arrive in one read. Pre-fix: 1024/1100.
/// The replies must also be in order: :1 .. :1100.
#[test]
fn incr_pipeline_after_a_large_publish_is_answered_in_order() {
    let moon = spawn_moon("1");
    let mut c = Conn::open(moon.port);
    let msg = "m".repeat(256 * 1024);
    assert_eq!(c.send(&["PUBLISH", "ch", &msg]), ":0\r\n");
    let n = 1100;
    let payload = encode(&["INCR", "ctr"]).repeat(n);
    let want: String = (1..=n).map(|i| format!(":{i}\r\n")).collect();
    assert_eq!(send_and_collect(&mut c, &payload, n), want);
    assert_eq!(
        c.send(&["GET", "ctr"]),
        format!("${}\r\n{n}\r\n", n.to_string().len())
    );
}

/// Inline (telnet) commands are the smallest frames there are: 1100 of them
/// fit in a single 8 KiB read. Pre-fix: 1024/1100.
#[test]
fn inline_ping_pipeline_past_the_cap_is_answered() {
    let moon = spawn_moon("1");
    let mut c = Conn::open(moon.port);
    let payload = b"PING\r\n".repeat(1100);
    assert_eq!(send_and_collect(&mut c, &payload, 1100), pongs(1100));
    assert_eq!(c.send(&["PING"]), "+PONG\r\n");
}

/// Exactly the cap, and one past it, on one connection: the boundary where
/// the parse loop stops with nothing, then with one frame, left over.
#[test]
fn pipelines_at_and_one_past_the_cap_are_answered() {
    let moon = spawn_moon("1");
    let mut c = Conn::open(moon.port);
    // Grow the buffer first so each pipeline lands in one read.
    assert_eq!(
        send_and_collect(&mut c, &pings(2 * CAP), 2 * CAP),
        pongs(2 * CAP)
    );
    for n in [CAP, CAP + 1, CAP, CAP + 1] {
        assert_eq!(send_and_collect(&mut c, &pings(n), n), pongs(n));
    }
    let payload = b"PING\r\n".repeat(CAP + 1);
    assert_eq!(send_and_collect(&mut c, &payload, CAP + 1), pongs(CAP + 1));
}

/// Past the cap WITH deferrals: over two shards, every `DBSIZE` behind a
/// remote `SET` cuts the batch and carries the parsed tail (moon#507 /
/// moon#1179 item 5), so the cap is reached with a carried tail at the front.
/// Every reply must be what in-order execution gives.
#[test]
fn pipeline_past_the_cap_with_deferrals_answers_in_order() {
    let moon = spawn_moon("2");
    let mut c = Conn::open(moon.port);
    assert_eq!(c.send(&["FLUSHALL"]), "+OK\r\n");
    let n = 1500usize;
    let mut payload = Vec::new();
    let mut want = String::new();
    for i in 0..n {
        payload.extend_from_slice(&encode(&["SET", &format!("cap:{i}"), "v"]));
        payload.extend_from_slice(&encode(&["DBSIZE"]));
        want.push_str("+OK\r\n");
        want.push_str(&format!(":{}\r\n", i + 1));
    }
    // Trailing PINGs, so the pipeline ends in plain frames past the cap too.
    payload.extend_from_slice(&pings(CAP + 100));
    want.push_str(&pongs(CAP + 100));
    assert_eq!(send_and_collect(&mut c, &payload, 2 * n + CAP + 100), want);
}
