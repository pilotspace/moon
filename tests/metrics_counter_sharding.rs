//! moon#774: the per-command counters moved onto per-thread slots must still
//! give INFO the right number — with the exporter OFF and several shard
//! threads live.
//!
//! Every server here runs `--admin-port 0`. That is the deployment the fix is
//! *for*: `record_keyspace_hit` and `record_dispatch_cross_spsc` are ungated
//! on purpose because these fields back INFO, which must be right whether or
//! not anyone ever scraped Prometheus. If the fix had "solved" the hot-path
//! cost by gating the increment, every assertion below would read zero.
//!
//! `--shards 4` is load-bearing, not decoration. The slots are per OS THREAD,
//! so a single-shard run puts every increment in one slot and passes with a
//! summation that only ever reads slot 0.

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
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-mcs-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                shards,
                // The whole point: the exporter is OFF and INFO must still be
                // right (moon#774).
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
    let mut moon = Moon {
        child,
        port,
        tmp_dir: std::env::temp_dir().join(format!("moon-mcs-{port}")),
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
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        s.set_write_timeout(Some(Duration::from_secs(10))).unwrap();
        Conn(s)
    }

    fn encode(parts: &[&str]) -> String {
        let mut out = format!("*{}\r\n", parts.len());
        for p in parts {
            out.push_str(&format!("${}\r\n{p}\r\n", p.len()));
        }
        out
    }

    /// One command, one reply. Counts replies rather than draining on a
    /// timeout so a slow shard cannot silently truncate the read.
    fn send(&mut self, parts: &[&str]) -> String {
        self.pipeline(&[parts.to_vec()])
    }

    /// Write N commands as one pipeline and read exactly N replies back.
    ///
    /// Every command MUST take the batch dispatch path this test is about, so
    /// a shared connection is correct here — and a fresh connection per probe
    /// would be actively wrong: it would land each command on a fresh shard
    /// and hide the per-batch accumulator entirely.
    fn pipeline(&mut self, cmds: &[Vec<&str>]) -> String {
        let mut out = String::new();
        for c in cmds {
            out.push_str(&Self::encode(c));
        }
        self.0.write_all(out.as_bytes()).expect("write");
        self.read_replies(cmds.len())
    }

    fn read_replies(&mut self, want: usize) -> String {
        let mut acc: Vec<u8> = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut buf = [0u8; 65536];
        while count_replies(&acc) < want {
            if Instant::now() > deadline {
                panic!(
                    "timed out waiting for {want} replies; saw {} in {} bytes",
                    count_replies(&acc),
                    acc.len()
                );
            }
            match self.0.read(&mut buf) {
                Ok(0) => panic!("server closed the connection mid-pipeline"),
                Ok(n) => acc.extend_from_slice(&buf[..n]),
                Err(e) => panic!("read: {e}"),
            }
        }
        String::from_utf8_lossy(&acc).into_owned()
    }
}

/// Count complete top-level RESP replies in `buf`.
///
/// Only the reply shapes these tests provoke: `+`, `-`, `:`, `$` (with its
/// payload) and `*` (with its elements). Deliberately strict — a shape it does
/// not understand panics rather than silently under-counting into a timeout.
fn count_replies(buf: &[u8]) -> usize {
    let mut i = 0usize;
    let mut n = 0usize;
    while i < buf.len() {
        match parse_one(buf, i) {
            Some(next) => {
                i = next;
                n += 1;
            }
            None => break,
        }
    }
    n
}

fn line_end(buf: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i + 2);
        }
        i += 1;
    }
    None
}

fn parse_one(buf: &[u8], at: usize) -> Option<usize> {
    let tag = *buf.get(at)?;
    let after = line_end(buf, at)?;
    match tag {
        b'+' | b'-' | b':' | b',' | b'#' | b'_' => Some(after),
        b'$' | b'*' | b'%' | b'~' | b'>' => {
            let head = std::str::from_utf8(&buf[at + 1..after - 2]).ok()?;
            let len: i64 = head.parse().ok()?;
            if len < 0 {
                return Some(after);
            }
            if tag == b'$' {
                let end = after + len as usize + 2;
                if end <= buf.len() { Some(end) } else { None }
            } else {
                let mut i = after;
                let elems = if tag == b'%' { len * 2 } else { len };
                for _ in 0..elems {
                    i = parse_one(buf, i)?;
                }
                Some(i)
            }
        }
        other => panic!("unhandled RESP tag {:?} at {at}", other as char),
    }
}

fn field(info: &str, name: &str) -> u64 {
    for line in info.lines() {
        if let Some(rest) = line.strip_prefix(name)
            && rest.starts_with(':')
        {
            return rest[1..]
                .trim()
                .parse()
                .unwrap_or_else(|e| panic!("{name} not numeric: {rest:?} ({e})"));
        }
    }
    panic!("INFO has no `{name}` field:\n{info}");
}

/// The summation must be exact end to end, with four shard threads live and
/// the exporter off.
///
/// This is the assertion a broken read-time sum breaks and nothing else does:
/// the counter VALUE is what INFO publishes, and hit-rate dashboards divide by
/// it. Undercounting here reads as a cache that got worse.
#[test]
fn keyspace_hit_miss_totals_are_exact_across_four_shards() {
    const N: usize = 300;
    let m = spawn_moon("4");
    let mut c = Conn::open(m.port);

    // Distinct, untagged keys: at `--shards 4` these spread over all four
    // shard threads, so the increments land in several per-thread slots.
    let keys: Vec<String> = (0..N).map(|i| format!("mcs:hit:{i}")).collect();
    let missing: Vec<String> = (0..N).map(|i| format!("mcs:miss:{i}")).collect();

    let sets: Vec<Vec<&str>> = keys.iter().map(|k| vec!["SET", k.as_str(), "v"]).collect();
    c.pipeline(&sets);

    let before = c.send(&["INFO", "stats"]);
    let h0 = field(&before, "keyspace_hits");
    let m0 = field(&before, "keyspace_misses");

    let hits: Vec<Vec<&str>> = keys.iter().map(|k| vec!["GET", k.as_str()]).collect();
    c.pipeline(&hits);
    let misses: Vec<Vec<&str>> = missing.iter().map(|k| vec!["GET", k.as_str()]).collect();
    c.pipeline(&misses);

    let after = c.send(&["INFO", "stats"]);
    let h1 = field(&after, "keyspace_hits");
    let m1 = field(&after, "keyspace_misses");

    assert_eq!(
        h1 - h0,
        N as u64,
        "{N} existing-key GETs spread over 4 shard threads must move \
         keyspace_hits by exactly {N} — got {}. A per-thread counter whose \
         read-time sum misses a slot undercounts exactly here, and nowhere a \
         single-shard test would notice.",
        h1 - h0
    );
    assert_eq!(
        m1 - m0,
        N as u64,
        "{N} missing-key GETs must move keyspace_misses by exactly {N} — got {}",
        m1 - m0
    );
}

/// Batching the dispatch-path recorders must not change the total.
///
/// The realistic bug in a per-batch accumulator is not "it counts nothing" —
/// it is an accumulator that is never reset, which double-counts more with
/// every batch. So the bound is against the number of commands actually sent:
/// no path can claim more dispatches than there were commands.
#[test]
fn batched_dispatch_totals_never_exceed_the_commands_sent() {
    const BATCHES: usize = 20;
    const PER_BATCH: usize = 50;
    let m = spawn_moon("4");
    let mut c = Conn::open(m.port);

    let keys: Vec<String> = (0..PER_BATCH).map(|i| format!("mcs:disp:{i}")).collect();
    let sets: Vec<Vec<&str>> = keys.iter().map(|k| vec!["SET", k.as_str(), "v"]).collect();
    c.pipeline(&sets);

    let before = c.send(&["INFO", "stats"]);
    let spsc0 = field(&before, "total_dispatch_cross_spsc");
    let fast0 = field(&before, "total_dispatch_cross_read_fast");

    let batch: Vec<Vec<&str>> = keys.iter().map(|k| vec!["GET", k.as_str()]).collect();
    for _ in 0..BATCHES {
        c.pipeline(&batch);
    }

    let after = c.send(&["INFO", "stats"]);
    let spsc = field(&after, "total_dispatch_cross_spsc") - spsc0;
    let fast = field(&after, "total_dispatch_cross_read_fast") - fast0;

    // +1 for the trailing INFO, which is itself a dispatched command.
    let sent = (BATCHES * PER_BATCH + 1) as u64;

    assert!(
        spsc + fast <= sent,
        "cross-shard dispatch counters claim {spsc} SPSC + {fast} fast = {} \
         dispatches from {sent} commands. A per-batch accumulator that is not \
         reset between batches produces exactly this: the count grows with the \
         SQUARE of the batch index while every INFO field stays plausible.",
        spsc + fast
    );
    assert!(
        spsc + fast > 0,
        "no cross-shard dispatch was recorded at --shards 4, so this test \
         proved nothing about the batched recorders. Non-vacuity guard: the \
         bound above passes trivially when both counters stay at zero."
    );
}

/// One shard: the cross-shard counters must be zero, and the keyspace
/// counters must still be exact.
///
/// The complement of the four-shard case. Together they pin that the slot
/// scheme neither loses increments when threads fan out nor invents them when
/// there is only one.
#[test]
fn single_shard_records_no_cross_shard_dispatch() {
    const N: usize = 100;
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);

    let keys: Vec<String> = (0..N).map(|i| format!("mcs:one:{i}")).collect();
    let sets: Vec<Vec<&str>> = keys.iter().map(|k| vec!["SET", k.as_str(), "v"]).collect();
    c.pipeline(&sets);

    let before = c.send(&["INFO", "stats"]);
    let h0 = field(&before, "keyspace_hits");
    let spsc0 = field(&before, "total_dispatch_cross_spsc");

    let gets: Vec<Vec<&str>> = keys.iter().map(|k| vec!["GET", k.as_str()]).collect();
    c.pipeline(&gets);

    let after = c.send(&["INFO", "stats"]);
    assert_eq!(
        field(&after, "keyspace_hits") - h0,
        N as u64,
        "single-shard keyspace_hits must move by exactly {N}"
    );
    assert_eq!(
        field(&after, "total_dispatch_cross_spsc") - spsc0,
        0,
        "there is no other shard to dispatch to at --shards 1"
    );
}
