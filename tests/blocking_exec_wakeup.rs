//! A producer executed inside `MULTI`/`EXEC` must wake a client blocked on the
//! key it wrote (moon#606).
//!
//! Measured against redis-server 8.6.1 and against moon, raw socket:
//!
//! ```text
//! BLPOP k 5   in one client, then   MULTI ; LPUSH k v ; EXEC   in another
//!   redis 8.6.1 :  woken at 0.514s   <- the push woke it
//!   moon        :  4.014s            <- its own timeout, not the push
//! ```
//!
//! The consumer is never wrong-answered, only late by its entire timeout — so
//! this reads as a slow queue or a flake rather than as a defect. Both halves
//! are asserted: the reply must be the ELEMENT (not the timeout's null) and it
//! must arrive well inside the budget. Asserting only the value would pass on
//! a server that merely timed out slowly enough to see the write; asserting
//! only the latency would pass on one that answered a premature null.
//!
//! `--shards 4` is deliberate. `EXEC` is routed to the shard that owns the
//! body's keys (`TxnLocality`), which is usually NOT the connection's own
//! shard, so the waking write and the parked waiter reach the registry by
//! different routes depending on placement. Each assertion loops over enough
//! distinct keys that both are exercised.

mod common;

use common::Conn;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Four shards: a key is remote ~75% of the time.
const SHARDS: &str = "4";
/// Distinct keys per assertion. At p(remote)=0.75 the chance that all 8 land
/// on the connection's own shard — and vacuously test one route — is under
/// 2e-5.
const TRIALS: usize = 8;
/// The blocked client's own timeout. Long enough that a woken reply and a
/// timed-out one are far apart in the measurement.
const BLOCK_SECS: &str = "5";
/// A reply later than this did not come from the write.
const WOKEN_WITHIN: Duration = Duration::from_secs(2);
/// How long a parked client is given to reach its blocked state before the
/// waking transaction is issued.
const SETTLE: Duration = Duration::from_millis(400);

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

fn spawn_moon(shards: &str) -> Moon {
    // `CARGO_BIN_EXE_moon` is the binary cargo built for THIS test run;
    // `common::find_moon_binary()` would fall back to `target/release/moon`,
    // whose provenance is unknown — a stale one turns a real failure green.
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-bxw-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
        Command::new(&bin)
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
                tmp_dir.to_str().unwrap_or("/tmp"),
            ])
            .stdout(Stdio::null())
            .stderr(
                std::fs::File::create(tmp_dir.join("moon.stderr")).expect("create moon stderr log"),
            )
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("moon-bxw-{port}"));
    let moon = Moon {
        child,
        port,
        tmp_dir,
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(mut c) = TcpStream::connect(("127.0.0.1", moon.port)) {
            let _ = c.set_read_timeout(Some(Duration::from_millis(500)));
            if c.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut buf = [0u8; 64];
                if let Ok(n) = c.read(&mut buf)
                    && n > 0
                    && buf.starts_with(b"+PONG")
                {
                    return moon;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let log = std::fs::read_to_string(moon.tmp_dir.join("moon.stderr")).unwrap_or_default();
    panic!("moon never became ready on port {port}\n--- stderr ---\n{log}");
}

/// Park `argv` on its own connection and hand back a join handle.
fn park(port: u16, argv: &[&str]) -> std::thread::JoinHandle<(Duration, String)> {
    let owned: Vec<String> = argv.iter().map(|s| (*s).to_string()).collect();
    std::thread::spawn(move || {
        let mut c = Conn::open(port);
        let start = Instant::now();
        let reply = c.send(&owned.iter().map(String::as_str).collect::<Vec<_>>());
        (start.elapsed(), reply)
    })
}

/// Run the producer `argv` inside `MULTI`/`EXEC` on a fresh connection.
fn exec_producer(port: u16, argv: &[&str]) -> String {
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["MULTI"]), "+OK\r\n", "MULTI was refused");
    let queued = c.send(argv);
    assert_eq!(queued, "+QUEUED\r\n", "{argv:?} was not queued: {queued}");
    c.send(&["EXEC"])
}

/// Park a waiter on each of `TRIALS` distinct keys, wake each with a
/// transaction, and report every trial whose reply was late or wrong.
#[track_caller]
fn each_trial(
    port: u16,
    tag: &str,
    expect: &str,
    mut waiter: impl FnMut(&str) -> Vec<String>,
    mut producer: impl FnMut(&str) -> Vec<String>,
) {
    let mut wrong: Vec<String> = Vec::new();
    for i in 0..TRIALS {
        let key = format!("{tag}{i}");
        let argv = waiter(&key);
        let handle = park(port, &argv.iter().map(String::as_str).collect::<Vec<_>>());
        std::thread::sleep(SETTLE);

        let pargv = producer(&key);
        let exec = exec_producer(port, &pargv.iter().map(String::as_str).collect::<Vec<_>>());
        assert!(
            exec.starts_with('*'),
            "EXEC of {pargv:?} did not commit: {exec:?}"
        );

        let (elapsed, reply) = handle.join().expect("waiter thread");
        if !reply.contains(expect) {
            wrong.push(format!("  {key}: reply {reply:?} after {elapsed:?}"));
        } else if elapsed > WOKEN_WITHIN {
            wrong.push(format!(
                "  {key}: correct reply but {elapsed:?} late — that is its own \
                 {BLOCK_SECS}s timeout expiring, not the EXEC waking it"
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {TRIALS} trials were not woken by the transaction:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

/// `MULTI ; LPUSH k v ; EXEC` must wake `BLPOP k`.
#[test]
fn bxw1_lpush_inside_multi_wakes_a_parked_blpop() {
    let m = spawn_moon(SHARDS);
    each_trial(
        m.port,
        "bxw1:",
        "payload",
        |k| vec!["BLPOP".into(), k.into(), BLOCK_SECS.into()],
        |k| vec!["LPUSH".into(), k.into(), "payload".into()],
    );
}

/// The zset waker has the same gap: `MULTI ; ZADD k s m ; EXEC` must wake
/// `BZPOPMIN k`.
#[test]
fn bxw2_zadd_inside_multi_wakes_a_parked_bzpopmin() {
    let m = spawn_moon(SHARDS);
    each_trial(
        m.port,
        "bxw2:",
        "member",
        |k| vec!["BZPOPMIN".into(), k.into(), BLOCK_SECS.into()],
        |k| vec!["ZADD".into(), k.into(), "1".into(), "member".into()],
    );
}

/// And the stream waker: `MULTI ; XADD s * f v ; EXEC` must wake a parked
/// `XREAD BLOCK`.
#[test]
fn bxw3_xadd_inside_multi_wakes_a_parked_xread() {
    let m = spawn_moon(SHARDS);
    // `$` binds at block time, so the stream must exist before the waiter
    // parks — otherwise it binds to nothing and the assertion would be about
    // binding, not about waking.
    let mut seed = Conn::open(m.port);
    for i in 0..TRIALS {
        let key = format!("bxw3:{i}");
        let reply = seed.send(&["XADD", &key, "1-1", "seed", "v"]);
        assert!(reply.starts_with('$'), "seed XADD failed: {reply:?}");
    }
    each_trial(
        m.port,
        "bxw3:",
        "woken",
        |k| {
            vec![
                "XREAD".into(),
                "BLOCK".into(),
                "5000".into(),
                "STREAMS".into(),
                k.into(),
                "$".into(),
            ]
        },
        |k| {
            vec![
                "XADD".into(),
                k.into(),
                "7-1".into(),
                "woken".into(),
                "v".into(),
            ]
        },
    );
}
