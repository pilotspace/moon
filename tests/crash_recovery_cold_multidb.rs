//! #139: cold recovery must re-attach spilled keys to the logical database
//! they were evicted from — not unconditionally db 0.
//!
//! Before the fix, `recover_shard_v3`'s cold-index rebuild merged every
//! recovered entry into `databases[0]`: a key spilled from `SELECT 1` was
//! unreachable from db 1 after restart (cold-read miss), and — because this
//! test deliberately uses the SAME key names in db 0 and db 1 — db 0's
//! index would hold whichever file's entry was inserted last, i.e. db 0
//! could serve db 1's VALUE. The fix attributes each spill file wholesale
//! via `FileEntry::db_index` (spill flush chunks never cross a db
//! boundary), and recovery attaches each rebuilt index to its own db.
//!
//! Setup mirrors `crash_recovery_disk_offload_no_aof`: offload WITHOUT AOF,
//! so the cold tier is the probes' ONLY durable copy and misattribution is
//! visible immediately (nothing replays the keys back into their dbs).
//! Probes land in db 0 AND db 1 under identical names with db-distinct
//! values; LRU filler in db 0 evicts them to the cold tier; SIGKILL;
//! restart; every readable probe must return ITS OWN db's value — any
//! cross-db value is an instant failure, and db 1 must recover at least
//! half its probes (async-tick durability slack, same convention as the
//! no-AOF suite).
//!
//! Run:
//!   cargo build --release
//!   cargo test --release --test crash_recovery_cold_multidb -- --ignored

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::Duration;

const PROBE_COUNT: usize = 60;
const PROBE_VALUE_LEN: usize = 2048;
const FILLER_COUNT: usize = 3000;
const FILLER_VALUE_LEN: usize = 2048;
/// 4 MiB, 1 shard: probes (2 dbs x 60 x 2 KiB = 240 KiB) + filler (~6 MiB)
/// blow well past the 0.85 spill threshold.
const MAXMEMORY_BYTES: usize = 4 * 1024 * 1024;
/// Async spill/manifest ticks must commit the probes before the SIGKILL
/// (same rationale as crash_recovery_disk_offload_no_aof's settle).
const SETTLE_BEFORE_KILL: u64 = 8;

fn unique_dir(suffix: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "moon-cold-multidb-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ))
}

fn start_moon(dir: &std::path::Path, maxmemory: usize) -> (Child, u16) {
    let off_dir = dir.join("off");
    std::fs::create_dir_all(&off_dir).expect("create off dir");
    common::spawn_listening(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--maxmemory",
                &maxmemory.to_string(),
                "--maxmemory-policy",
                "allkeys-lru",
                "--disk-offload",
                "enable",
                "--disk-offload-dir",
                off_dir.to_str().expect("off dir utf8"),
                "--appendonly",
                "no",
                "--cold-orphan-sweep-interval-secs",
                "60",
                "--dir",
            ])
            .arg(dir)
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("create stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("create stderr log"))
            .spawn()
            .expect("spawn moon (run `cargo build --release` first)")
    })
}

// Minimal RESP2 client (self-contained per-file convention; copied from
// tests/crash_recovery_spill_batch_kill9.rs).
struct Conn {
    s: TcpStream,
    buf: Vec<u8>,
    pos: usize,
}

impl Conn {
    fn open(port: u16) -> Self {
        let s = TcpStream::connect(format!("127.0.0.1:{port}")).expect("connect");
        s.set_read_timeout(Some(Duration::from_secs(30))).ok();
        s.set_write_timeout(Some(Duration::from_secs(30))).ok();
        Conn {
            s,
            buf: Vec::with_capacity(32 * 1024),
            pos: 0,
        }
    }

    fn cmd(&mut self, parts: &[&[u8]]) -> Option<Vec<u8>> {
        let mut req = Vec::with_capacity(64 + parts.iter().map(|p| p.len() + 16).sum::<usize>());
        req.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            req.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            req.extend_from_slice(p);
            req.extend_from_slice(b"\r\n");
        }
        self.s.write_all(&req).expect("write cmd");
        self.frame()
    }

    fn fill(&mut self) {
        let mut chunk = [0u8; 32 * 1024];
        let n = self.s.read(&mut chunk).expect("read from server");
        assert!(n > 0, "connection closed mid-frame");
        self.buf.extend_from_slice(&chunk[..n]);
    }

    fn line(&mut self) -> String {
        loop {
            if let Some(rel) = self.buf[self.pos..].windows(2).position(|w| w == b"\r\n") {
                let line =
                    String::from_utf8_lossy(&self.buf[self.pos..self.pos + rel]).into_owned();
                self.pos += rel + 2;
                return line;
            }
            self.fill();
        }
    }

    fn exact(&mut self, n: usize) -> Vec<u8> {
        while self.buf.len() - self.pos < n + 2 {
            self.fill();
        }
        let out = self.buf[self.pos..self.pos + n].to_vec();
        self.pos += n + 2;
        out
    }

    /// Returns the payload for bulk replies, Some(status bytes) for simple
    /// strings / integers, None for nil. Panics on protocol errors.
    fn frame(&mut self) -> Option<Vec<u8>> {
        if self.pos > 0 && self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        }
        let line = self.line();
        let (tag, rest) = line.split_at(1);
        match tag {
            "+" | ":" => Some(rest.as_bytes().to_vec()),
            "-" => panic!("server error reply: {rest}"),
            "$" => {
                let n: i64 = rest.parse().unwrap_or(-1);
                if n < 0 {
                    None
                } else {
                    Some(self.exact(n as usize))
                }
            }
            other => panic!("unexpected RESP tag {other:?} in line {line:?}"),
        }
    }
}

/// RAII guard: SIGKILLs the server process when dropped (safe on an
/// already-dead child). Same shape as crash_recovery_spill_batch_kill9.
struct ServerGuard(Child);

impl ServerGuard {
    fn sigkill(&mut self) {
        common::sigkill(&mut self.0);
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn probe_key(i: usize) -> String {
    format!("probe:{i:04}")
}

fn probe_value(db: usize, i: usize) -> Vec<u8> {
    let mut v = format!("db{db}:{i:04}:").into_bytes();
    v.resize(PROBE_VALUE_LEN, b'p');
    v
}

fn count_spill_files(dir: &std::path::Path) -> usize {
    let mut n = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p
                .file_name()
                .and_then(|f| f.to_str())
                .is_some_and(|f| f.starts_with("heap-") && f.ends_with(".mpf"))
            {
                n += 1;
            }
        }
    }
    n
}

#[test]
#[ignore = "real-server kill-9 test; needs a release moon binary (see file header)"]
fn cold_recovery_attributes_spilled_keys_to_their_database() {
    let dir = unique_dir("attrib");
    std::fs::create_dir_all(&dir).expect("create test dir");

    // Round 1: write probes into db0 AND db1 (same names, db-distinct
    // values), then LRU-filler to evict them into the cold tier.
    let (child, port) = start_moon(&dir, MAXMEMORY_BYTES);
    let mut guard = ServerGuard(child);
    {
        let mut c = Conn::open(port);
        for db in [0usize, 1usize] {
            assert_eq!(
                c.cmd(&[b"SELECT", db.to_string().as_bytes()]).as_deref(),
                Some(b"OK".as_ref())
            );
            for i in 0..PROBE_COUNT {
                let k = probe_key(i);
                let v = probe_value(db, i);
                assert_eq!(
                    c.cmd(&[b"SET", k.as_bytes(), &v]).as_deref(),
                    Some(b"OK".as_ref()),
                    "round-1 SET db{db} {k}"
                );
            }
        }
        // Filler in db0 pushes the (older) probes out through the LRU +
        // spill path. Values match probe size so victim selection is
        // size-uniform.
        assert_eq!(c.cmd(&[b"SELECT", b"0"]).as_deref(), Some(b"OK".as_ref()));
        let filler_v = vec![b'f'; FILLER_VALUE_LEN];
        for i in 0..FILLER_COUNT {
            let k = format!("filler:{i:06}");
            // Under allkeys-lru SET never OOMs; it evicts.
            let _ = c.cmd(&[b"SET", k.as_bytes(), &filler_v]);
        }
    }
    std::thread::sleep(Duration::from_secs(SETTLE_BEFORE_KILL));

    let spill_files = count_spill_files(&dir);
    assert!(
        spill_files > 0,
        "test premise broken: no spill files were written — eviction/offload \
         never engaged (check maxmemory/policy flags)"
    );
    guard.sigkill();

    // Round 2: generous maxmemory so recovery/promotion doesn't re-evict
    // mid-assertion (same convention as crash_recovery_spill_batch_kill9).
    let (child2, port2) = start_moon(&dir, 64 * 1024 * 1024);
    let mut guard2 = ServerGuard(child2);
    let mut c = Conn::open(port2);

    let mut db1_recovered = 0usize;
    for db in [0usize, 1usize] {
        assert_eq!(
            c.cmd(&[b"SELECT", db.to_string().as_bytes()]).as_deref(),
            Some(b"OK".as_ref())
        );
        for i in 0..PROBE_COUNT {
            let k = probe_key(i);
            match c.cmd(&[b"GET", k.as_bytes()]) {
                None => {} // not durable by kill time — tolerated (bounded below)
                Some(v) => {
                    let expect = probe_value(db, i);
                    // THE #139 assertion: a readable probe must carry ITS
                    // OWN db's value. Pre-fix, db0 could serve db1's value
                    // (same key name, misattributed cold entry) and db1
                    // answered nil for everything.
                    assert_eq!(
                        v, expect,
                        "db{db} {k}: recovered value belongs to the wrong database \
                         (cold-plane db attribution broken)"
                    );
                    if db == 1 {
                        db1_recovered += 1;
                    }
                }
            }
        }
    }
    assert!(
        db1_recovered >= PROBE_COUNT / 2,
        "db1 recovered only {db1_recovered}/{PROBE_COUNT} spilled probes — \
         SELECT>0 cold entries are not being re-attached (#139)"
    );

    guard2.sigkill();
    let _ = std::fs::remove_dir_all(&dir);
}
