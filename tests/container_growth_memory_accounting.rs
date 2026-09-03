//! WS6 — container-growth memory accounting integration test.
//!
//! `used_memory` was charged once at key creation for an EMPTY container
//! (`get_or_create_hash`/`_list`/`_set`/`_sorted_set` in `src/storage/db.rs`),
//! then handed out a raw `&mut` into the map/deque/set/tree. Every subsequent
//! mutation (HSET adding fields, LPUSH growing the deque, ...) was invisible
//! to `used_memory` — a real memory-safety hole against `--maxmemory` (and
//! the per-db quotas in `tests/db_maxmemory_quota.rs`), confirmed by
//! adversarial review 2026-07-08.
//!
//! RED (pre-fix): growing a single hash key past `--maxmemory` under
//! `noeviction` never hit the cap — every HSET kept succeeding because the
//! server only ever "saw" the empty-hash creation charge (~150 bytes), not
//! the megabytes of field data piled on afterward. Same for LPUSH growing a
//! single list key.
//!
//! GREEN (post-fix): growth is charged incrementally (O(1) per mutation —
//! see the WS6 accounting note above `entry_overhead` in
//! `src/storage/db.rs`), so the existing `--maxmemory` / `noeviction` gate
//! now sees the real, current size of a single growing container and
//! rejects further writes once the budget is crossed.
//!
//! Run alone with:
//!   MOON_BIN=$PWD/target/debug/moon cargo test --test container_growth_memory_accounting

#![allow(clippy::unwrap_used)]

mod common;

use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Binary resolution + server spawn (pattern: tests/db_maxmemory_quota.rs)
// ---------------------------------------------------------------------------

fn find_moon_binary() -> std::path::PathBuf {
    if let Ok(bin) = std::env::var("MOON_BIN") {
        let p = std::path::PathBuf::from(bin);
        if p.exists() {
            return p;
        }
    }
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

/// Fresh `--dir` under the OS temp dir (never the shared `/Volumes/Games`
/// checkout volume, which hovers near the 5% diskfull guard). Must be
/// portable: a hardcoded `/private/tmp` is macOS-only and fails
/// PermissionDenied on Linux CI runners.
fn test_tmpdir() -> tempfile::TempDir {
    let base = std::env::temp_dir().join("moon-container-growth-tests");
    std::fs::create_dir_all(&base).expect("create test tmp base dir");
    tempfile::Builder::new()
        .prefix("cgm-")
        .tempdir_in(&base)
        .expect("tempdir_in OS temp dir")
}

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        // kill() sends SIGKILL on all platforms via std::process::Child;
        // belt-and-suspenders backstop against a leaked busy-poller (see
        // gotcha_leaked_moon_busypoller_contaminates_xshard in project memory).
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Spawn moon with a small `--maxmemory` cap, `noeviction` policy, AOF and
/// disk-offload both disabled (raw in-memory write path, no WAL/spill noise
/// that could mask the accounting gap under test), single shard.
fn spawn_moon_maxmemory(dir: &std::path::Path, maxmemory_bytes: u64) -> (ServerGuard, u16) {
    let (child, port) = common::spawn_listening(|port| {
        Command::new(find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                "1",
                "--appendonly",
                "no",
                "--maxmemory",
                &maxmemory_bytes.to_string(),
                "--maxmemory-policy",
                "noeviction",
                "--disk-offload",
                "disable",
                // Guard against the diskfull write-pause (`MOONERR
                // diskfull`) firing mid-test on a nearly-full host volume
                // and masking the OOM assertions under test with an
                // unrelated error.
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard(child), port)
}

/// Spawn moon with the GLOBAL `--maxmemory` gate disabled (0 = unlimited) and
/// a single per-db `--db-maxmemory <db>:<bytes>` quota instead, `noeviction`
/// policy, AOF/disk-offload disabled. Isolates the db-quota gate
/// (`db_quota::check_db_maxmemory_for_command`) from the global gate
/// (`eviction::evict_to_budget`) so the shrink-only bypass is
/// exercised on each independently.
fn spawn_moon_db_maxmemory(dir: &std::path::Path, db_maxmemory_bytes: u64) -> (ServerGuard, u16) {
    let (child, port) = common::spawn_listening(|port| {
        Command::new(find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                "1",
                "--appendonly",
                "no",
                "--maxmemory",
                "0",
                "--maxmemory-policy",
                "noeviction",
                "--disk-offload",
                "disable",
                "--db-maxmemory",
                &format!("0:{db_maxmemory_bytes}"),
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard(child), port)
}

// ---------------------------------------------------------------------------
// Minimal RESP client (binary-safe args, full-frame parser) — same shape as
// tests/db_maxmemory_quota.rs's Client, duplicated rather than shared:
// integration test binaries in this repo are compiled independently.
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
enum V {
    Simple(String),
    Err(String),
    Bulk(Vec<u8>),
    Integer(i64),
    Null,
}

impl V {
    fn is_oom_error(&self) -> bool {
        matches!(self, V::Err(msg) if msg.to_uppercase().contains("OOM"))
    }
}

struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Client {
    fn connect(port: u16) -> Self {
        let addr = format!("127.0.0.1:{port}")
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();
        let start = Instant::now();
        let stream = loop {
            match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
                Ok(s) => break s,
                Err(_) if start.elapsed() < Duration::from_secs(30) => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => panic!("server never accepted on port {port}: {e}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let writer = stream.try_clone().unwrap();
        Client {
            reader: BufReader::new(stream),
            writer,
        }
    }

    fn encode(args: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    fn read_line(&mut self) -> String {
        let mut line = Vec::new();
        let mut b = [0u8; 1];
        loop {
            self.reader.read_exact(&mut b).expect("read byte");
            if b[0] == b'\n' {
                break;
            }
            if b[0] != b'\r' {
                line.push(b[0]);
            }
        }
        String::from_utf8_lossy(&line).into_owned()
    }

    fn parse(&mut self) -> V {
        let line = self.read_line();
        let (t, rest) = line.split_at(1);
        match t {
            "+" => V::Simple(rest.to_string()),
            "-" => V::Err(rest.to_string()),
            ":" => V::Integer(rest.parse().expect("integer reply")),
            "$" => {
                let n: i64 = rest.parse().expect("bulk len");
                if n < 0 {
                    return V::Null;
                }
                let mut buf = vec![0u8; n as usize + 2];
                self.reader.read_exact(&mut buf).expect("bulk body");
                buf.truncate(n as usize);
                V::Bulk(buf)
            }
            "*" => {
                let n: i64 = rest.parse().expect("arr len");
                if n < 0 {
                    return V::Null;
                }
                for _ in 0..n {
                    self.parse();
                }
                V::Null
            }
            other => panic!("unexpected RESP type {other:?} (line {line:?})"),
        }
    }

    fn cmd(&mut self, args: &[&[u8]]) -> V {
        self.writer.write_all(&Self::encode(args)).expect("send");
        self.parse()
    }

    fn try_ping(&mut self) -> std::io::Result<bool> {
        self.writer.write_all(b"*1\r\n$4\r\nPING\r\n")?;
        let mut buf = [0u8; 7];
        self.reader.read_exact(&mut buf)?;
        Ok(&buf == b"+PONG\r\n")
    }
}

fn wait_ready(port: u16) -> Client {
    let start = Instant::now();
    loop {
        let mut c = Client::connect(port);
        if let Ok(true) = c.try_ping() {
            return c;
        }
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "server never answered PING on port {port}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

// ---------------------------------------------------------------------------
// Case A: growing ONE hash key's fields past --maxmemory under noeviction
// must eventually reject further HSETs.
// ---------------------------------------------------------------------------

#[test]
fn test_hset_growth_on_single_key_hits_maxmemory() {
    let dir = test_tmpdir();
    // 64 KB budget; ~1 KB fields -> should bite well within 200 fields even
    // accounting for the ~150-byte empty-container baseline charge.
    let (_guard, port) = spawn_moon_maxmemory(dir.path(), 64 * 1024);
    let mut c = wait_ready(port);

    let value = vec![b'v'; 1024];
    let mut saw_oom = false;
    for i in 0..200 {
        let field = format!("field{i}");
        let r = c.cmd(&[b"HSET", b"biggrowinghash", field.as_bytes(), &value]);
        if r.is_oom_error() {
            saw_oom = true;
            break;
        }
        assert_eq!(
            r,
            V::Integer(1),
            "HSET of a brand-new field must return 1 (or OOM), got {r:?} at i={i}"
        );
    }

    assert!(
        saw_oom,
        "expected an OOM reply within 200 HSETs growing a single hash key \
         (64 KB cap, ~1 KB fields = ~200 KB of field data) -- growth of an \
         existing container is invisible to --maxmemory if this fails"
    );
}

// ---------------------------------------------------------------------------
// Case B: growing ONE list key via LPUSH past --maxmemory under noeviction
// must eventually reject further pushes.
// ---------------------------------------------------------------------------

#[test]
fn test_lpush_growth_on_single_key_hits_maxmemory() {
    let dir = test_tmpdir();
    let (_guard, port) = spawn_moon_maxmemory(dir.path(), 64 * 1024);
    let mut c = wait_ready(port);

    let value = vec![b'v'; 1024];
    let mut saw_oom = false;
    for _ in 0..200 {
        let r = c.cmd(&[b"LPUSH", b"biggrowinglist", &value]);
        if r.is_oom_error() {
            saw_oom = true;
            break;
        }
        match r {
            V::Integer(_) => {}
            other => panic!("expected Integer (new length) or OOM, got {other:?}"),
        }
    }

    assert!(
        saw_oom,
        "expected an OOM reply within 200 LPUSHes growing a single list key \
         (64 KB cap, ~1 KB elements = ~200 KB of element data) -- growth of \
         an existing container is invisible to --maxmemory if this fails"
    );
}

// ---------------------------------------------------------------------------
// Case C: deleting fields back down must credit the freed memory. Stays
// UNDER budget throughout (HDEL always succeeds well before OOM) so this
// isolates credit-on-delete correctness from the shrink-only gate bypass
// exercised end-to-end (grow past the cap, THEN delete) by
// `test_hdel_self_recovery_past_maxmemory_boundary` and
// `test_hdel_self_recovery_past_db_maxmemory_boundary` below.
// ---------------------------------------------------------------------------

#[test]
fn test_hdel_credits_memory_after_growth() {
    let dir = test_tmpdir();
    // Budget sized so growing ~100 KB of fields, deleting them all, then
    // writing another ~100 KB elsewhere fits ONLY if the deletes freed their
    // memory -- comfortably under the cap throughout (no OOM ever hit),
    // isolating credit-on-delete from the separate gate-classification gap
    // documented above.
    let (_guard, port) = spawn_moon_maxmemory(dir.path(), 256 * 1024);
    let mut c = wait_ready(port);

    let value = vec![b'v'; 1024];
    let mut fields_written: Vec<String> = Vec::new();
    for i in 0..100 {
        let field = format!("field{i}");
        let r = c.cmd(&[b"HSET", b"drainme", field.as_bytes(), &value]);
        assert_eq!(
            r,
            V::Integer(1),
            "setup: growing to ~100 KB must stay under the 256 KB budget, got {r:?} at i={i}"
        );
        fields_written.push(field);
    }

    // Delete every field written -- the hash's accounted memory should
    // return to (near-)empty. Every HDEL here runs well under budget.
    for field in &fields_written {
        let r = c.cmd(&[b"HDEL", b"drainme", field.as_bytes()]);
        assert_eq!(
            r,
            V::Integer(1),
            "HDEL of field {field} must succeed, got {r:?}"
        );
    }

    // A second, unrelated ~100 KB write must now succeed: if `drainme`'s
    // fields were NOT credited back on delete, the stale ~100 KB charge plus
    // this new ~100 KB write would exceed the 256 KB budget and OOM.
    let big_value = vec![b'w'; 100 * 1024];
    let r = c.cmd(&[b"SET", b"after-drain-probe", &big_value]);
    assert_eq!(
        r,
        V::Simple("OK".to_string()),
        "a ~100 KB write after fully draining a ~100 KB hash must succeed under a \
         256 KB budget -- got {r:?} (credit-on-delete broken: HDEL isn't freeing \
         the deleted fields' bytes)"
    );
}

// ---------------------------------------------------------------------------
// Case D: self-recovery PAST the noeviction boundary. Adversarial review
// (2026-07-08) found that once WS6's accounting fix makes container growth
// visible to the write-eviction gate, a NEW self-inflicted lockout becomes
// reachable: `run_write_eviction_gate` / `check_db_maxmemory_for_command`
// applied the noeviction reject to EVERY write command uniformly, so once a
// key crossed its cap, even a pure-shrink command like HDEL on that SAME key
// was also rejected -- a tenant wedges permanently with no self-recovery path
// short of FLUSHALL/restart. Fixed via a static shrink-only classification
// (`db_quota::is_shrink_only_command`) applied at all three connection
// handler call sites (handler_monoio, handler_sharded, handler_single x2).
//
// Both flavors of the gate (global `--maxmemory` and per-db
// `--db-maxmemory`) are covered here, sharing the identical shape:
//   1. grow a hash PAST the cap -> HSET must be rejected with OOM.
//   2. HDEL a field on that SAME (over-cap) key -> must SUCCEED (this is the
//      exact scenario the pre-fix code rejected).
//   3. HSET again, now under budget -> must succeed (proves the freed bytes
//      were both credited AND usable, not just "not rejected").
// ---------------------------------------------------------------------------

#[test]
fn test_hdel_self_recovery_past_maxmemory_boundary() {
    let dir = test_tmpdir();
    // Small budget so ~10 x 1 KB fields comfortably crosses it.
    let (_guard, port) = spawn_moon_maxmemory(dir.path(), 4 * 1024);
    let mut c = wait_ready(port);

    let value = vec![b'v'; 1024];
    let mut fields_written: Vec<String> = Vec::new();
    let mut saw_oom = false;
    for i in 0..40 {
        let field = format!("field{i}");
        let r = c.cmd(&[b"HSET", b"wedgeme", field.as_bytes(), &value]);
        if r.is_oom_error() {
            saw_oom = true;
            break;
        }
        assert_eq!(
            r,
            V::Integer(1),
            "HSET of a brand-new field must return 1 (or OOM), got {r:?} at i={i}"
        );
        fields_written.push(field);
    }
    assert!(
        saw_oom,
        "setup: expected an HSET to be rejected with OOM once `wedgeme` crosses \
         the 4 KB --maxmemory cap -- growth accounting must be broken if this fails"
    );
    assert!(
        !fields_written.is_empty(),
        "setup: at least one field must have been written before the cap hit"
    );

    // THE FIX UNDER TEST: HDEL on the same over-cap key must SUCCEED, not be
    // rejected by the noeviction gate alongside the growth-causing HSETs.
    let victim = &fields_written[0];
    let r = c.cmd(&[b"HDEL", b"wedgeme", victim.as_bytes()]);
    assert_eq!(
        r,
        V::Integer(1),
        "HDEL on an over---maxmemory-cap key must SUCCEED (shrink-only commands \
         must bypass the noeviction reject) -- got {r:?}. A tenant that grows a \
         key past the cap must retain a self-recovery path via HDEL/SREM/LPOP/etc."
    );

    // Drain the rest so the key is comfortably back under budget, then prove
    // a subsequent HSET succeeds again (the freed bytes are both credited and
    // usable, not merely "the reject was skipped once").
    for field in &fields_written[1..] {
        let r = c.cmd(&[b"HDEL", b"wedgeme", field.as_bytes()]);
        assert_eq!(
            r,
            V::Integer(1),
            "follow-up HDEL of field {field} must also succeed, got {r:?}"
        );
    }
    let r = c.cmd(&[b"HSET", b"wedgeme", b"recovered-field", &value]);
    assert_eq!(
        r,
        V::Integer(1),
        "HSET must succeed again once the key is back under budget after \
         draining -- got {r:?} (freed memory must be usable again, not just \
         the reject bypassed)"
    );
}

#[test]
fn test_hdel_self_recovery_past_db_maxmemory_boundary() {
    let dir = test_tmpdir();
    // Global --maxmemory is 0 (unlimited); only db 0's quota is small.
    let (_guard, port) = spawn_moon_db_maxmemory(dir.path(), 4 * 1024);
    let mut c = wait_ready(port);

    let value = vec![b'v'; 1024];
    let mut fields_written: Vec<String> = Vec::new();
    let mut saw_oom = false;
    for i in 0..40 {
        let field = format!("field{i}");
        let r = c.cmd(&[b"HSET", b"wedgeme", field.as_bytes(), &value]);
        if let V::Err(msg) = &r {
            assert!(
                msg.to_uppercase().contains("MOONERR")
                    && msg.to_lowercase().contains("db maxmemory"),
                "expected a db-maxmemory-flavored rejection, got {r:?}"
            );
            saw_oom = true;
            break;
        }
        assert_eq!(
            r,
            V::Integer(1),
            "HSET of a brand-new field must return 1 (or db-quota reject), got {r:?} at i={i}"
        );
        fields_written.push(field);
    }
    assert!(
        saw_oom,
        "setup: expected an HSET to be rejected once `wedgeme` crosses the \
         4 KB --db-maxmemory quota for db 0 -- got no rejection within 40 HSETs"
    );
    assert!(
        !fields_written.is_empty(),
        "setup: at least one field must have been written before the quota hit"
    );

    // THE FIX UNDER TEST: HDEL on the same over-quota key must SUCCEED under
    // the PER-DB gate too, not just the global --maxmemory gate covered by
    // `test_hdel_self_recovery_past_maxmemory_boundary` above.
    let victim = &fields_written[0];
    let r = c.cmd(&[b"HDEL", b"wedgeme", victim.as_bytes()]);
    assert_eq!(
        r,
        V::Integer(1),
        "HDEL on an over-db-maxmemory-quota key must SUCCEED (shrink-only \
         commands must bypass the db-quota reject too) -- got {r:?}"
    );

    for field in &fields_written[1..] {
        let r = c.cmd(&[b"HDEL", b"wedgeme", field.as_bytes()]);
        assert_eq!(
            r,
            V::Integer(1),
            "follow-up HDEL of field {field} must also succeed, got {r:?}"
        );
    }
    let r = c.cmd(&[b"HSET", b"wedgeme", b"recovered-field", &value]);
    assert_eq!(
        r,
        V::Integer(1),
        "HSET must succeed again once db 0 is back under its quota after \
         draining -- got {r:?}"
    );
}

// ---------------------------------------------------------------------------
// Case F (moon#788): the fixed cost of a container must be visible to
// `used_memory` through the real dispatch path, not just to a unit test.
//
// A sorted set is a `SortedSetBPTree`, and its node arena is a `Vec<Node>`
// where `Node` is an enum sized by `InternalNode` (784 B) and `Vec`'s minimum
// capacity for an element that size is 4 — so an EMPTY B+tree already owns a
// 3136-byte allocation. The estimator charged `tree.len() * 80`, per entry,
// which made that whole fixed cost invisible: a one-member zset billed 134 B.
// Measured on Linux at 50k keys the ledger was 15.1x below real RSS, i.e. an
// operator's `--maxmemory` gate could not fire before the OOM killer did.
//
// The floor below is deliberately far under the real arena (3136 B): it must
// hold on every 64-bit target without tracking the exact node layout, while
// still being an order of magnitude above the pre-fix 134 B.
// ---------------------------------------------------------------------------

/// Poll `INFO memory` until `want` accepts the value, or panic after 30s
/// naming what never happened.
fn poll_used_memory(c: &mut Client, want: impl Fn(u64) -> bool, what: &str) -> u64 {
    let start = Instant::now();
    loop {
        let v = used_memory(c);
        if want(v) {
            return v;
        }
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "{what}: used_memory stayed at {v} for 30s"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Read `used_memory` out of `INFO memory`.
fn used_memory(c: &mut Client) -> u64 {
    match c.cmd(&[b"INFO", b"memory"]) {
        V::Bulk(b) => {
            let text = String::from_utf8_lossy(&b);
            for line in text.lines() {
                if let Some(rest) = line.trim_end().strip_prefix("used_memory:") {
                    return rest.trim().parse().expect("used_memory is an integer");
                }
            }
            panic!("INFO memory carried no used_memory line:\n{text}");
        }
        other => panic!("INFO memory returned {other:?}"),
    }
}

#[test]
fn test_sorted_set_arena_is_visible_to_used_memory() {
    const KEYS: u64 = 500;
    /// Well under the 3136-byte arena a one-member B+tree really owns, and
    /// well over the 134 B the pre-fix estimator charged.
    const FLOOR_PER_KEY: u64 = 2048;

    let dir = test_tmpdir();
    // 512 MB: large enough that `noeviction` never fires for 500 tiny zsets,
    // so this measures the ledger and not the gate.
    let (_guard, port) = spawn_moon_maxmemory(dir.path(), 512 * 1024 * 1024);
    let mut c = wait_ready(port);

    let before = used_memory(&mut c);
    for i in 0..KEYS {
        let key = format!("z:{i:08}");
        let member = format!("m:{i:08}");
        let r = c.cmd(&[b"ZADD", key.as_bytes(), b"1", member.as_bytes()]);
        assert_eq!(r, V::Integer(1), "ZADD {key} must add one member");
    }
    // `INFO`'s used_memory sums a PUBLISHED per-shard atomic refreshed on the
    // periodic chore, not the live counter, so reading it immediately after
    // the last ZADD returns the pre-write figure. Poll rather than sleep a
    // fixed amount: an unpolled read here would report 0 B/key and fail this
    // test for a reason that has nothing to do with the accounting.
    let after = poll_used_memory(&mut c, |v| v > before, "growth after 500 ZADD");
    let per_key = (after - before) / KEYS;

    assert!(
        per_key >= FLOOR_PER_KEY,
        "a one-member sorted set was billed {per_key} B/key, but its B+tree \
         node arena alone is over 3 KB — the fixed cost of the container is \
         invisible to used_memory, so --maxmemory cannot bind (moon#788)"
    );

    // ...and removing the keys must give every byte back, or the ledger drifts
    // upward until the gate fires on memory nobody holds.
    for i in 0..KEYS {
        let key = format!("z:{i:08}");
        c.cmd(&[b"DEL", key.as_bytes()]);
    }
    // `used_memory` is deliberately wider than the KV ledger: it also carries
    // the Lua VM heap, which moon initialises lazily and samples on a chore,
    // so the idle figure steps between 0 and ~48 KB on its own. Assert the KV
    // growth came back rather than demanding an exact return to `before` — a
    // real leak here would be megabytes, not tens of kilobytes.
    let charged = after - before;
    let tolerance = charged / 20;
    poll_used_memory(
        &mut c,
        |v| v.saturating_sub(before) <= tolerance,
        &format!(
            "deleting every zset must return the {charged} B charged to within \
             {tolerance} B of the {before} B starting figure"
        ),
    );
}
