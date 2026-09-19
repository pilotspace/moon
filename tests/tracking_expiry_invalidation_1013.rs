//! moon#1013 — CLIENT TRACKING must invalidate a key that EXPIRES or is
//! EVICTED, not only one a command writes.
//!
//! Red on origin/main `3266d461`: every case below received ZERO pushes at
//! `--shards 1` and `--shards 4`. Oracle, measured against redis-server
//! 8.6.1 over a raw RESP3 socket: every case pushes `invalidate [key]`.
//!
//! | case                                    | redis 8.6.1 | moon pre-fix |
//! |-----------------------------------------|-------------|--------------|
//! | active expiry, default mode             | push        | none         |
//! | BCAST prefix, key expires               | push        | none         |
//! | NOLOOP client, its OWN key expires      | push        | none         |
//! | hash FIELD expires, hash survives       | push        | none         |
//! | allkeys-random eviction (offload off)   | push        | none         |
//!
//! Delivery accounting (see the 4-shard tracking-flake note in
//! `client_tracking_invalidation.rs`'s history): a push to an IDLE reader on
//! another shard is not guaranteed within a bounded read window, so each case
//! tracks N distinct keys, COUNTS the keys whose push arrived, and asserts a
//! MAJORITY. That cleanly separates the fix (all or nearly all) from the bug
//! (zero). Waits are deterministic `wait_for`s on the wire, never a bare
//! sleep race.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const N: usize = 6;

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

fn moon_binary() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("MOON_BIN") {
        return std::path::PathBuf::from(p);
    }
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

fn spawn_moon(shards: &str, extra: &[&str]) -> Moon {
    let bin = moon_binary();
    let dir_for = |port: u16| std::env::temp_dir().join(format!("moon-trk1013-{port}"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = dir_for(port);
        let _ = std::fs::create_dir_all(&tmp_dir);
        let mut args: Vec<String> = [
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
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        args.extend(extra.iter().map(|s| s.to_string()));
        Command::new(&bin)
            .args(&args)
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(tmp_dir.join("moon.stderr")).expect("create moon stderr"))
            .spawn()
            .expect("spawn moon")
    });
    let moon = Moon {
        child,
        port,
        tmp_dir: dir_for(port),
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let mut c = Resp::connect(moon.port);
        c.cmd(&["PING"]);
        if c.saw(b"+PONG") {
            return moon;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let log = std::fs::read_to_string(moon.tmp_dir.join("moon.stderr")).unwrap_or_default();
    panic!("moon (--shards {shards}) never answered PING\n--- stderr ---\n{log}");
}

/// Minimal RESP client over a blocking TcpStream (same shape as the one in
/// `client_tracking_invalidation.rs`).
struct Resp {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Resp {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        Self {
            stream,
            buf: Vec::new(),
        }
    }

    fn send(&mut self, args: &[&str]) {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
        }
        self.stream.write_all(&out).expect("write");
    }

    fn pump(&mut self, total: Duration) {
        let deadline = Instant::now() + total;
        let mut chunk = [0u8; 4096];
        while Instant::now() < deadline {
            match self.stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(_) => {}
            }
        }
    }

    fn cmd(&mut self, args: &[&str]) {
        self.send(args);
        self.pump(Duration::from_millis(150));
    }

    fn saw(&self, needle: &[u8]) -> bool {
        self.buf.windows(needle.len()).any(|w| w == needle)
    }

    fn clear(&mut self) {
        self.buf.clear();
    }
}

/// The exact RESP3 push redis sends for a one-key invalidation.
fn push_for(key: &str) -> Vec<u8> {
    format!(
        ">2\r\n$10\r\ninvalidate\r\n*1\r\n${}\r\n{key}\r\n",
        key.len()
    )
    .into_bytes()
}

fn tracking_client(port: u16, opts: &[&str]) -> Resp {
    let mut c = Resp::connect(port);
    c.cmd(&["HELLO", "3"]);
    assert!(c.saw(b"proto"), "HELLO 3 must answer");
    let mut args = vec!["CLIENT", "TRACKING", "ON"];
    args.extend_from_slice(opts);
    c.cmd(&args);
    assert!(
        c.saw(b"+OK"),
        "CLIENT TRACKING ON {opts:?} must be accepted"
    );
    c.clear();
    c
}

/// Wait until every key's push has arrived (or `timeout`), then return how
/// many did.
fn delivered(reader: &mut Resp, keys: &[String], timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    loop {
        let got = keys.iter().filter(|k| reader.saw(&push_for(k))).count();
        if got == keys.len() || Instant::now() >= deadline {
            return got;
        }
        reader.pump(Duration::from_millis(100));
    }
}

fn assert_majority(case: &str, shards: &str, got: usize, reader: &Resp) {
    assert!(
        got * 2 > N,
        "{case} at --shards {shards}: only {got}/{N} tracked keys got an \
         `invalidate` push (redis 8.6.1 pushes for every one). wire: {:?}",
        String::from_utf8_lossy(&reader.buf)
    );
}

fn keys(prefix: &str) -> Vec<String> {
    (0..N).map(|i| format!("{prefix}{i}")).collect()
}

// ── active expiry, default mode ─────────────────────────────────────────

fn active_expiry_default(shards: &str) {
    let m = spawn_moon(shards, &[]);
    let mut writer = Resp::connect(m.port);
    let mut reader = tracking_client(m.port, &[]);
    let ks = keys("te:act:");
    for k in &ks {
        writer.cmd(&["SET", k, "v", "PX", "800"]);
        assert!(writer.saw(b"+OK"));
        reader.cmd(&["GET", k]);
        assert!(reader.saw(b"$1\r\nv\r\n"), "tracked GET must hit");
    }
    // Deliberately NO `clear()` here: the early keys expire while later ones
    // are still being read, and clearing would discard their pushes. Keeping
    // the buffer is sound — `push_for` matches only the exact invalidate
    // frame, and no push can exist before the read that tracked the key.
    let got = delivered(&mut reader, &ks, Duration::from_secs(6));
    assert_majority("active expiry", shards, got, &reader);
}

#[test]
fn active_expiry_invalidates_1_shard() {
    active_expiry_default("1");
}

#[test]
fn active_expiry_invalidates_4_shards() {
    active_expiry_default("4");
}

// ── BCAST prefix ─────────────────────────────────────────────────────────

fn bcast_prefix_expiry(shards: &str) {
    let m = spawn_moon(shards, &[]);
    let mut writer = Resp::connect(m.port);
    let ks = keys("tb:");
    for k in &ks {
        // Long enough to outlive the loop plus the registration below: a key
        // that expires before the BCAST client registers is owed nothing.
        writer.cmd(&["SET", k, "v", "PX", "2500"]);
        assert!(writer.saw(b"+OK"));
    }
    // Registered AFTER the writes, so no write-triggered push can be
    // mistaken for the expiry one.
    let mut reader = tracking_client(m.port, &["BCAST", "PREFIX", "tb:"]);
    let got = delivered(&mut reader, &ks, Duration::from_secs(6));
    assert_majority("BCAST prefix expiry", shards, got, &reader);
}

#[test]
fn bcast_prefix_expiry_invalidates_1_shard() {
    bcast_prefix_expiry("1");
}

#[test]
fn bcast_prefix_expiry_invalidates_4_shards() {
    bcast_prefix_expiry("4");
}

// ── NOLOOP: an expiry is nobody's own write ──────────────────────────────

fn noloop_own_key_expiry(shards: &str) {
    let m = spawn_moon(shards, &[]);
    let mut reader = tracking_client(m.port, &["NOLOOP"]);
    let ks = keys("tn:");
    for k in &ks {
        reader.cmd(&["SET", k, "v", "PX", "800"]);
        reader.cmd(&["GET", k]);
    }
    // No `clear()`: see `active_expiry_default`.
    let got = delivered(&mut reader, &ks, Duration::from_secs(6));
    assert_majority("NOLOOP own-key expiry", shards, got, &reader);
}

#[test]
fn noloop_own_key_expiry_invalidates_1_shard() {
    noloop_own_key_expiry("1");
}

#[test]
fn noloop_own_key_expiry_invalidates_4_shards() {
    noloop_own_key_expiry("4");
}

// ── hash field expiry (hash survives) ────────────────────────────────────

fn hash_field_expiry(shards: &str) {
    let m = spawn_moon(shards, &[]);
    let mut writer = Resp::connect(m.port);
    let mut reader = tracking_client(m.port, &[]);
    let ks = keys("th:");
    for k in &ks {
        writer.cmd(&["HSET", k, "f", "v", "g", "w"]);
        writer.cmd(&["HPEXPIRE", k, "800", "FIELDS", "1", "f"]);
        reader.cmd(&["HGET", k, "g"]);
        assert!(reader.saw(b"$1\r\nw\r\n"), "tracked HGET must hit");
    }
    // No `clear()`: see `active_expiry_default`.
    // Nothing else touches the server from here: the db is IDLE, which is
    // the case the cached-clock advance exists for.
    let got = delivered(&mut reader, &ks, Duration::from_secs(6));
    assert_majority("hash field expiry", shards, got, &reader);
}

#[test]
fn hash_field_expiry_invalidates_1_shard() {
    hash_field_expiry("1");
}

#[test]
fn hash_field_expiry_invalidates_4_shards() {
    hash_field_expiry("4");
}

// ── eviction (plain drop: disk offload off) ──────────────────────────────

fn eviction(shards: &str) {
    // With disk offload on (the default) a victim SPILLS and stays readable
    // with the same value, which correctly needs no invalidation. Only a
    // plain drop removes the key.
    let m = spawn_moon(shards, &["--disk-offload", "disable"]);
    let mut writer = Resp::connect(m.port);
    let mut reader = tracking_client(m.port, &[]);
    let ks = keys("tev:");
    for k in &ks {
        writer.cmd(&["SET", k, "v"]);
        reader.cmd(&["GET", k]);
    }
    reader.clear();
    writer.cmd(&["CONFIG", "SET", "maxmemory-policy", "allkeys-random"]);
    let pad = "x".repeat(200);
    for i in 0..3000 {
        writer.send(&["SET", &format!("fill:{i}"), &pad]);
    }
    writer.pump(Duration::from_millis(800));
    writer.clear();
    writer.cmd(&["CONFIG", "SET", "maxmemory", "100000"]);
    assert!(writer.saw(b"+OK"), "CONFIG SET maxmemory must succeed");
    for i in 0..200 {
        writer.send(&["SET", &format!("trigger:{i}"), &pad]);
    }
    writer.pump(Duration::from_millis(800));
    // Only keys eviction actually removed are owed a push.
    let evicted: Vec<String> = ks
        .iter()
        .filter(|k| {
            let mut probe = Resp::connect(m.port);
            probe.cmd(&["EXISTS", k]);
            probe.saw(b":0\r\n")
        })
        .cloned()
        .collect();
    assert!(
        evicted.len() * 2 > N,
        "precondition: allkeys-random under maxmemory 100000 must evict most \
         tracked keys (evicted {}/{N})",
        evicted.len()
    );
    let got = delivered(&mut reader, &evicted, Duration::from_secs(4));
    assert!(
        got * 2 > evicted.len(),
        "eviction at --shards {shards}: only {got}/{} evicted tracked keys got \
         an `invalidate` push. wire: {:?}",
        evicted.len(),
        String::from_utf8_lossy(&reader.buf)
    );
}

#[test]
fn eviction_invalidates_1_shard() {
    eviction("1");
}

#[test]
fn eviction_invalidates_4_shards() {
    eviction("4");
}
