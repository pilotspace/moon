//! An MQ trigger must reach its subscriber whatever shard that subscriber
//! landed on (moon#474).
//!
//! A trigger fires on the shard that owns the queue key — a hash tag routes
//! every workspace key to one shard, so the `TriggerRegistry` there is
//! authoritative. That is true of the KEY. It says nothing about the
//! SUBSCRIBER: a client that subscribes to `mq:trigger:<queue>` lands on
//! whichever shard accepted its connection, and moon keeps one pub/sub
//! registry PER shard.
//!
//! So a trigger delivered with a local-only publish reaches the consumer only
//! when the consumer happens to have been accepted by the queue's home shard —
//! about a 1-in-N chance at `--shards N`, and always true at `--shards 1`.
//! That is why this file spawns 4 shards and spreads the queues across them
//! from ONE subscriber connection: the subscriber sits on a single shard, so
//! most of these queues are necessarily remote to it.
//!
//! The failure is silent by construction. `publish_shared` returns a
//! subscriber count of 0, the trigger path only `tracing::debug!`s it, and the
//! consumer simply never wakes — no error, no metric.

mod common;

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Distinct names so they hash to different shards. Eight against four shards
/// leaves the subscriber's own shard owning a small minority of them.
const QUEUES: [&str; 8] = [
    "mqx:alpha",
    "mqx:beta",
    "mqx:gamma",
    "mqx:delta",
    "mqx:epsilon",
    "mqx:zeta",
    "mqx:eta",
    "mqx:theta",
];

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
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-mqtrigger-{port}"));
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
                tmp_dir.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(
                std::fs::File::create(tmp_dir.join("moon.stderr")).expect("create moon stderr log"),
            )
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("moon-mqtrigger-{port}"));
    let mut moon = Moon {
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
    let status = match moon.child.try_wait() {
        Ok(Some(s)) => format!("exited with {s}"),
        Ok(None) => "still running but never answered PING".to_string(),
        Err(e) => format!("status unavailable: {e}"),
    };
    let log = std::fs::read_to_string(moon.tmp_dir.join("moon.stderr")).unwrap_or_default();
    panic!("moon never became ready on port {port} ({status})\n--- stderr ---\n{log}");
}

struct Conn(TcpStream);

impl Conn {
    fn open(port: u16) -> Self {
        let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
        Conn(s)
    }

    fn send(&mut self, parts: &[&str]) -> String {
        let mut out = format!("*{}\r\n", parts.len());
        for p in parts {
            out.push_str(&format!("${}\r\n{p}\r\n", p.len()));
        }
        self.0.write_all(out.as_bytes()).expect("write");
        self.read_reply()
    }

    fn read_reply(&mut self) -> String {
        let mut buf = [0u8; 16384];
        let mut acc = Vec::new();
        loop {
            match self.0.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    acc.extend_from_slice(&buf[..n]);
                    self.0
                        .set_read_timeout(Some(Duration::from_millis(200)))
                        .unwrap();
                }
                Err(_) => break,
            }
        }
        self.0
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        String::from_utf8_lossy(&acc).into_owned()
    }

    /// Drain `message` pushes for the whole window.
    ///
    /// Waits out the window instead of returning on the first message: the
    /// point of the test is WHICH channels arrive, and returning early would
    /// report a partial set as if it were the final one.
    fn collect_messages(&mut self, window: Duration) -> Vec<(String, String)> {
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
        // RESP array: message / <channel> / <payload>
        let lines: Vec<&str> = text
            .split("\r\n")
            .filter(|l| !l.is_empty() && !l.starts_with('*') && !l.starts_with('$'))
            .collect();
        let mut out = Vec::new();
        for (i, l) in lines.iter().enumerate() {
            if *l == "message" && i + 2 < lines.len() {
                out.push((lines[i + 1].to_string(), lines[i + 2].to_string()));
            }
        }
        out
    }
}

/// Subscribe to every trigger channel on ONE connection, so every queue whose
/// home shard differs from this connection's shard is a cross-shard delivery.
fn subscriber(port: u16, channels: &[String]) -> Conn {
    let mut c = Conn::open(port);
    for ch in channels {
        let ack = c.send(&["SUBSCRIBE", ch]);
        assert!(
            ack.contains("subscribe"),
            "SUBSCRIBE {ch} not acknowledged: {ack:?}"
        );
    }
    c
}

/// With no workspace prefix the effective key is the raw key, so the channel
/// is exactly `mq:trigger:<queue>` (`src/shard/timers.rs`).
fn channel_for(queue: &str) -> String {
    format!("mq:trigger:{queue}")
}

// ---------------------------------------------------------------------------
// mqx1 — the case a single-shard test cannot catch.
// ---------------------------------------------------------------------------

#[test]
fn mqx1_a_fired_trigger_reaches_a_subscriber_on_any_shard() {
    let m = spawn_moon("4");

    let channels: Vec<String> = QUEUES.iter().map(|q| channel_for(q)).collect();
    // Subscribe BEFORE the push: the publisher consults the remote-subscriber
    // map, so a subscription that lands after the fire is not a fair test of
    // the fan-out.
    let mut sub = subscriber(m.port, &channels);

    let mut w = Conn::open(m.port);
    for q in QUEUES {
        let created = w.send(&["MQ", "CREATE", q]);
        assert!(created.starts_with("+OK"), "MQ CREATE {q}: {created:?}");
        let armed = w.send(&["MQ", "TRIGGER", q, "PUBLISH mqx:fired 1", "DEBOUNCE", "10"]);
        assert!(armed.starts_with("+OK"), "MQ TRIGGER {q}: {armed:?}");
    }
    for q in QUEUES {
        let pushed = w.send(&["MQ", "PUSH", q, "f", "v"]);
        assert!(!pushed.starts_with('-'), "MQ PUSH {q} errored: {pushed:?}");
    }

    let msgs = sub.collect_messages(Duration::from_secs(3));
    let got: HashSet<&str> = msgs.iter().map(|(ch, _)| ch.as_str()).collect();
    let missing: Vec<&String> = channels
        .iter()
        .filter(|c| !got.contains(c.as_str()))
        .collect();
    assert!(
        missing.is_empty(),
        "a fired MQ trigger must reach a subscriber on ANY shard, not only \
         one that happens to sit on the queue's home shard. The trigger timer \
         publishes into its own shard's registry, so at --shards 4 roughly \
         3/4 of these are dropped with no error and no metric. \
         missing: {missing:?} (got {got:?})"
    );
}

// ---------------------------------------------------------------------------
// mqx2 — the fan-out must not invent deliveries either.
// ---------------------------------------------------------------------------

/// A subscriber on an unrelated channel must receive nothing.
///
/// Without this, "deliver everywhere" could be satisfied by broadcasting every
/// trigger to every shard's every subscriber, which would pass mqx1 while
/// making `mq:trigger:` channels useless.
#[test]
fn mqx2_a_trigger_is_not_broadcast_to_unrelated_channels() {
    let m = spawn_moon("4");

    let mut bystander = subscriber(m.port, &[channel_for("mqx:not-this-queue")]);

    let mut w = Conn::open(m.port);
    for q in QUEUES {
        assert!(w.send(&["MQ", "CREATE", q]).starts_with("+OK"));
        assert!(
            w.send(&["MQ", "TRIGGER", q, "PUBLISH mqx:fired 1", "DEBOUNCE", "10"])
                .starts_with("+OK")
        );
        let pushed = w.send(&["MQ", "PUSH", q, "f", "v"]);
        assert!(!pushed.starts_with('-'), "MQ PUSH {q}: {pushed:?}");
    }

    let msgs = bystander.collect_messages(Duration::from_secs(2));
    assert!(
        msgs.is_empty(),
        "a subscriber to an unrelated trigger channel received {msgs:?}"
    );
}
