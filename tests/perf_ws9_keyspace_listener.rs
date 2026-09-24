//! WS9 / moon#1214 item 2: keyspace notifications must be delivered to a
//! subscriber that arrives AFTER the class was enabled, and the listener-count
//! gate that makes the no-subscriber case free must never wrongly suppress a
//! real listener's events. Also proves the gate's count survives churn
//! (subscribe/unsubscribe/disconnect) without stranding a live listener.
//!
//! Run alone with:
//!   MOON_BIN=$PWD/target/release/moon cargo test --test perf_ws9_keyspace_listener

#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

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
    let bin = common::find_moon_binary();
    let tmp_dir = common::unique_test_dir("ws9-keyspace");
    let dir = tmp_dir.clone();
    let (child, port) = common::spawn_listening(|port| {
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
                "--save",
                "",
                "--disk-free-min-pct",
                "0",
                "--dir",
                dir.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon")
    });
    Moon {
        child,
        port,
        tmp_dir,
    }
}

struct Conn(TcpStream);

impl Conn {
    fn open(port: u16) -> Self {
        let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
        Conn(s)
    }

    fn write(&mut self, parts: &[&str]) {
        let mut out = format!("*{}\r\n", parts.len());
        for p in parts {
            out.push_str(&format!("${}\r\n{p}\r\n", p.len()));
        }
        self.0.write_all(out.as_bytes()).expect("write");
    }

    fn send(&mut self, parts: &[&str]) -> String {
        self.write(parts);
        let mut buf = [0u8; 8192];
        let mut acc = Vec::new();
        self.0
            .set_read_timeout(Some(Duration::from_millis(400)))
            .unwrap();
        loop {
            match self.0.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => acc.extend_from_slice(&buf[..n]),
                Err(_) => break,
            }
        }
        self.0
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        String::from_utf8_lossy(&acc).into_owned()
    }

    /// Drain pmessages for `window`, returning (channel, payload) pairs.
    fn collect_pmessages(&mut self, window: Duration) -> Vec<(String, String)> {
        self.0
            .set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        let deadline = Instant::now() + window;
        let mut acc = Vec::new();
        let mut buf = [0u8; 16384];
        while Instant::now() < deadline {
            match self.0.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => acc.extend_from_slice(&buf[..n]),
                Err(_) => {}
            }
        }
        let text = String::from_utf8_lossy(&acc).into_owned();
        let lines: Vec<&str> = text
            .split("\r\n")
            .filter(|l| {
                !l.is_empty() && !l.starts_with('*') && !l.starts_with('$') && !l.starts_with('>')
            })
            .collect();
        let mut out = Vec::new();
        for (i, l) in lines.iter().enumerate() {
            if *l == "pmessage" && i + 3 < lines.len() {
                out.push((lines[i + 2].to_string(), lines[i + 3].to_string()));
            }
        }
        out
    }
}

fn wait_ready(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(mut c) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = c.set_read_timeout(Some(Duration::from_millis(500)));
            if c.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut buf = [0u8; 64];
                if let Ok(n) = c.read(&mut buf)
                    && n > 0
                    && buf.starts_with(b"+PONG")
                {
                    return;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("moon never became ready on {port}");
}

/// A late PSUBSCRIBE — established AFTER the class is enabled and after earlier
/// writes — still receives events for SUBSEQUENT writes. This is the gate's
/// correctness guarantee: raising the listener count from 0 must make every
/// following event flow, not just prove the no-listener fast path exists.
#[test]
fn late_psubscribe_still_receives_keyspace_events() {
    let m = spawn_moon("1");
    wait_ready(m.port);

    // Class enabled, but NOBODY subscribed yet — these writes take the free
    // (no-listener) path.
    let mut w = Conn::open(m.port);
    assert!(
        w.send(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
            .starts_with("+OK")
    );
    w.send(&["SET", "before1", "v"]);
    w.send(&["SET", "before2", "v"]);

    // A subscriber arrives now, then a fresh write happens.
    let mut sub = Conn::open(m.port);
    assert!(
        sub.send(&["PSUBSCRIBE", "__keyevent@0__:*"])
            .contains("psubscribe"),
        "PSUBSCRIBE not acknowledged"
    );
    w.send(&["SET", "after", "v"]);

    let msgs = sub.collect_pmessages(Duration::from_secs(2));
    assert!(
        msgs.contains(&("__keyevent@0__:set".into(), "after".into())),
        "a late subscriber must receive events for writes AFTER it subscribed; got {msgs:?}"
    );
}

/// The gate must keep working through subscription churn: after a subscriber
/// disconnects and a NEW one connects, keyspace events still flow. If the
/// listener count went negative or leaked to zero on disconnect, the second
/// subscriber would get silence.
#[test]
fn keyspace_events_survive_subscriber_churn() {
    let m = spawn_moon("1");
    wait_ready(m.port);
    let mut w = Conn::open(m.port);
    assert!(
        w.send(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
            .starts_with("+OK")
    );

    // Several subscribe / disconnect cycles: each drop must decrement the
    // count by exactly what its subscribe added, never below zero.
    for i in 0..5 {
        let mut s = Conn::open(m.port);
        assert!(
            s.send(&["PSUBSCRIBE", "__keyevent@0__:*"])
                .contains("psubscribe")
        );
        // s drops here (disconnect) at end of iteration.
        let _ = i;
        drop(s);
        std::thread::sleep(Duration::from_millis(50));
    }

    // A final live subscriber must still receive events.
    let mut sub = Conn::open(m.port);
    assert!(
        sub.send(&["PSUBSCRIBE", "__keyevent@0__:*"])
            .contains("psubscribe")
    );
    w.send(&["SET", "churn", "v"]);
    let msgs = sub.collect_pmessages(Duration::from_secs(2));
    assert!(
        msgs.contains(&("__keyevent@0__:set".into(), "churn".into())),
        "after churn, a live subscriber must still get events (count must not \
         have gone negative or stranded); got {msgs:?}"
    );
}

/// Cross-shard: a subscriber on one shard receives keyspace events for a write
/// whose key lives on another shard. Proves the global (process-wide) listener
/// count gates correctly regardless of which shard a listener or a write lands
/// on — a per-shard count would drop (N-1)/N of these.
#[test]
fn late_subscribe_receives_cross_shard_keyspace_events() {
    let m = spawn_moon("4");
    wait_ready(m.port);
    let mut w = Conn::open(m.port);
    assert!(
        w.send(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
            .starts_with("+OK")
    );

    let mut sub = Conn::open(m.port);
    assert!(
        sub.send(&["PSUBSCRIBE", "__keyevent@0__:*"])
            .contains("psubscribe")
    );

    // Write a spread of keys so at least one lands on a shard other than the
    // subscriber's.
    for i in 0..24 {
        let k = format!("xshard:{i}");
        w.send(&["SET", &k, "v"]);
    }
    let msgs = sub.collect_pmessages(Duration::from_secs(3));
    let sets = msgs
        .iter()
        .filter(|(ch, _)| ch == "__keyevent@0__:set")
        .count();
    assert!(
        sets >= 24,
        "a late subscriber must receive keyspace events for keys on every \
         shard; got {sets} set events: {msgs:?}"
    );
}
