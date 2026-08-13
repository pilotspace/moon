//! INFO must answer the question the client actually asked.
//!
//! Measured against redis-server 8.6.1. Moon's `info()` takes `_args` and
//! discards it, so section selection does not exist:
//!
//! ```text
//! INFO replication
//!   redis: # Replication                                   (1 section)
//!   moon : # Server # Clients # Memory # Persistence # Vector # MoonStore
//!          # Reclamation # Stats # CPU # Replication # Commandstats
//!          # Keyspace # Replication                        (13, one twice)
//! ```
//!
//! A monitoring agent that scrapes `INFO replication` every second is paying
//! for the whole payload and parsing a duplicated header. Separately, Moon
//! exposes 61 fields where Redis exposes 213, and the missing ones are the
//! ones dashboards read: keyspace_hits, evicted_keys, maxmemory_policy.
//!
//! Raw sockets: redis-rs parses INFO into a map, which would hide both the
//! section ORDER and the duplicate-header bug under test.

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
    spawn_moon_in(shards, None)
}

/// `dir` lets a restart test reuse the SAME data dir, which is the only way
/// to prove run_id changes for reasons other than a fresh dataset.
fn spawn_moon_in(shards: &str, dir: Option<std::path::PathBuf>) -> Moon {
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let fixed_dir = dir.clone();
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = fixed_dir
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().join(format!("moon-infoobs-{port}")));
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
    let tmp_dir = dir.unwrap_or_else(|| std::env::temp_dir().join(format!("moon-infoobs-{port}")));
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
}

/// Section headers, in reply order. `INFO` is a bulk string whose payload is
/// CRLF-delimited, so the `$<len>` prefix line is skipped by the `#` filter.
fn headers(reply: &str) -> Vec<String> {
    reply
        .lines()
        .map(|l| l.trim_end_matches('\r'))
        .filter(|l| l.starts_with('#'))
        .map(|l| l.to_string())
        .collect()
}

fn field(reply: &str, name: &str) -> Option<String> {
    reply
        .lines()
        .map(|l| l.trim_end_matches('\r'))
        .find_map(|l| l.strip_prefix(&format!("{name}:")).map(|v| v.to_string()))
}

// ---------------------------------------------------------------------------
// io1 — the headline. This is why the task exists.
// ---------------------------------------------------------------------------

#[test]
fn io1_single_section_only() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let reply = c.send(&["INFO", "replication"]);
    let h = headers(&reply);
    assert_eq!(
        h,
        vec!["# Replication"],
        "INFO <section> must return ONLY that section — a monitoring agent \
         polling INFO replication should not pay for the whole payload. got {h:?}"
    );
}

#[test]
fn io2_section_case_insensitive() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let lower = headers(&c.send(&["INFO", "replication"]));
    let upper = headers(&c.send(&["INFO", "REPLICATION"]));
    assert_eq!(
        lower, upper,
        "section matching is case-insensitive in Redis; clients send both"
    );
    assert_eq!(upper, vec!["# Replication"]);
}

#[test]
fn io3_multiple_sections() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let h = headers(&c.send(&["INFO", "server", "clients"]));
    assert_eq!(
        h,
        vec!["# Server", "# Clients"],
        "INFO accepts several section names and returns exactly those, in the \
         server's canonical order — not the caller's. got {h:?}"
    );
}

#[test]
fn io4_unknown_section_is_empty() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let reply = c.send(&["INFO", "nosuchsection"]);
    assert!(
        !reply.starts_with('-'),
        "an unknown section is NOT an error in Redis — it is an empty bulk \
         string. Erroring here breaks clients that probe optional sections. got {reply:?}"
    );
    assert!(
        headers(&reply).is_empty(),
        "unknown section must yield no sections; got {:?}",
        headers(&reply)
    );
    // The connection must survive — an unknown section is not a protocol fault.
    assert!(c.send(&["PING"]).starts_with("+PONG"));
}

#[test]
fn io5_default_omits_commandstats() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let default = headers(&c.send(&["INFO"]));
    let all = headers(&c.send(&["INFO", "all"]));
    assert!(
        !default.iter().any(|h| h == "# Commandstats"),
        "Redis's default INFO omits Commandstats — it is per-command data that \
         grows with the command table and is only emitted on request. got {default:?}"
    );
    assert!(
        all.iter().any(|h| h == "# Commandstats"),
        "INFO all must include Commandstats; got {all:?}"
    );
}

#[test]
fn io6_no_duplicate_headers() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let h = headers(&c.send(&["INFO"]));
    let mut seen = std::collections::HashSet::new();
    let dups: Vec<&String> = h.iter().filter(|x| !seen.insert((*x).clone())).collect();
    assert!(
        dups.is_empty(),
        "every section header must appear at most once — a duplicate makes \
         naive INFO parsers (split on '#', build a map) silently keep only one \
         copy and drop the other's fields. duplicated: {dups:?} in {h:?}"
    );
}

#[test]
fn io7_repeated_section_emitted_once() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let h = headers(&c.send(&["INFO", "server", "server"]));
    assert_eq!(
        h,
        vec!["# Server"],
        "a repeated section name must not duplicate the section; got {h:?}"
    );
}

#[test]
fn io8_required_fields_present() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let reply = c.send(&["INFO", "all"]);
    // The fields clients, drivers and monitoring agents actually parse.
    for name in [
        "run_id",
        "redis_mode",
        "cluster_enabled",
        "process_id",
        "os",
        "arch_bits",
        "keyspace_hits",
        "keyspace_misses",
        "expired_keys",
        "evicted_keys",
        "rejected_connections",
        "maxmemory",
        "maxmemory_policy",
        "instantaneous_ops_per_sec",
        "blocked_clients",
        "pubsub_channels",
        "pubsub_patterns",
        "total_net_input_bytes",
        "total_net_output_bytes",
    ] {
        assert!(
            field(&reply, name).is_some(),
            "INFO is missing {name:?} — stock monitoring agents read it and \
             will either KeyError or silently report zero"
        );
    }
}

#[test]
fn io9_run_id_shape_and_restart() {
    let dir = std::env::temp_dir().join(format!("moon-infoobs-runid-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let first = {
        let m = spawn_moon_in("1", Some(dir.clone()));
        let mut c = Conn::open(m.port);
        let id = field(&c.send(&["INFO", "server"]), "run_id").expect("run_id present");
        assert_eq!(
            id.len(),
            40,
            "run_id is a 40-char hex string in Redis; got {id:?}"
        );
        assert!(
            id.bytes().all(|b| b.is_ascii_hexdigit()),
            "run_id must be hex; got {id:?}"
        );
        id
        // m dropped here -> server killed, and Drop also removes the dir, so
        // the second server below starts clean. That is fine for this test:
        // run_id must be per-PROCESS, and deriving it from dataset state would
        // be wrong regardless of whether the dataset survived.
    };

    // Fresh process: clients use run_id to detect that the server they are
    // talking to is not the one they were talking to.
    let m2 = spawn_moon_in("1", Some(dir.clone()));
    let mut c2 = Conn::open(m2.port);
    let second = field(&c2.send(&["INFO", "server"]), "run_id").expect("run_id present");
    assert_ne!(
        first, second,
        "run_id must differ across a restart — a stable one defeats every \
         client-side restart/failover detection that depends on it"
    );
}

#[test]
fn io10_keyspace_hit_miss_counters() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    c.send(&["SET", "io10key", "v"]);

    let before = c.send(&["INFO", "stats"]);
    let h0: u64 = field(&before, "keyspace_hits")
        .expect("keyspace_hits present")
        .parse()
        .expect("numeric");
    let m0: u64 = field(&before, "keyspace_misses")
        .expect("keyspace_misses present")
        .parse()
        .expect("numeric");

    c.send(&["GET", "io10key"]); // hit
    c.send(&["GET", "io10missing"]); // miss

    let after = c.send(&["INFO", "stats"]);
    let h1: u64 = field(&after, "keyspace_hits").unwrap().parse().unwrap();
    let m1: u64 = field(&after, "keyspace_misses").unwrap().parse().unwrap();

    assert_eq!(
        h1 - h0,
        1,
        "one existing-key GET must move keyspace_hits by exactly 1 \
         (hit-rate dashboards divide by these)"
    );
    assert_eq!(
        m1 - m0,
        1,
        "one missing-key GET must move keyspace_misses by exactly 1"
    );
}
