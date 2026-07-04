//! ADD milestone `v3-4-kv-correctness` — cross-shard coordinator LOCAL-leg WAL
//! durability (review Finding 1).
//!
//! The cross-shard coordinator splits a multi-key write (MSET / MSETNX) into a
//! LOCAL leg (keys owned by the connection's own shard, executed in-process) and
//! REMOTE legs (keys owned by other shards, executed via `ShardMessage::
//! MultiExecute`). The remote legs persist through `wal_append_and_fanout`; the
//! local leg historically executed the write in memory but NEVER appended it to
//! the owning shard's AOF. So a co-located MSET/MSETNX was durable when its owner
//! was a *remote* shard but silently **non-durable** when its owner was the
//! connection's *own* shard — the write survived until the next SIGKILL, then
//! vanished on restart.
//!
//! ## Why this is deterministic despite SO_REUSEPORT
//!
//! The connection lands on some shard `C` (SO_REUSEPORT picks it, we cannot
//! predict which). Each test writes ONE co-located group per shard (via a
//! `{hash-tag}` that provably routes to that shard). Whichever shard `C` is,
//! exactly ONE group is the LOCAL leg and the rest are remote. We assert ALL
//! groups survive a crash+restart:
//!   - pre-fix: the group whose owner == `C` is gone (local leg never persisted)
//!     → the assertion fails (RED) no matter which shard `C` turned out to be.
//!   - post-fix: the local leg persists via the same `aof_pool` path every local
//!     write uses → every group recovers (GREEN).
//!
//! All writes go through a SINGLE reused connection so `my_shard` (== `C`) is
//! stable across the whole write phase.
//!
//! Harness: `CARGO_BIN_EXE_moon` + a hand-rolled RESP2 client (no redis-cli), so
//! this runs as a live gate under both runtimes. Fsync policy + 1.5s quiescing
//! sleep before SIGKILL mirror the proven `crash_matrix_per_shard_aof.rs`
//! everysec pattern (the sleep drains the fire-and-forget remote-leg appends so
//! the ONLY thing a crash can drop pre-fix is the un-persisted local leg).
//!
//! Run alone with: cargo test --test coordinator_local_leg_durability

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use moon::shard::dispatch::key_to_shard;

// ---------------------------------------------------------------------------
// Harness (CARGO_BIN_EXE pattern, mirrors msetnx_cross_shard_reject.rs +
// crash_matrix_per_shard_aof.rs)
// ---------------------------------------------------------------------------

const SHARDS: u32 = 4;
/// Keys per co-located group (a multi-key MSET/MSETNX, not a single SET).
const GROUP_SIZE: usize = 3;

fn moon_binary() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    let p = l.local_addr().expect("local_addr").port();
    drop(l);
    p
}

/// Spawn moon with per-shard AOF enabled (`--appendonly yes`). `everysec` +
/// a 1.5s quiescing sleep before the kill gives 100% durability for everything
/// that was actually appended — so the only pre-fix loss is the local leg that
/// was never appended at all.
fn spawn_moon_aof(port: u16, dir: &std::path::Path, shards: u32) -> Child {
    Command::new(moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--shards",
            &shards.to_string(),
            "--appendonly",
            "yes",
            "--appendfsync",
            "everysec",
        ])
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
        .spawn()
        .expect("spawn moon (CARGO_BIN_EXE_moon)")
}

struct ServerGuard(Child);
impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// SIGKILL round-1 and reap it so the port + AOF file handles are released
/// before round-2 spawns. `Child::kill()` sends SIGKILL on Unix.
fn sigkill(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn connect(port: u16, deadline: Duration) -> TcpStream {
    let addr = format!("127.0.0.1:{port}")
        .to_socket_addrs()
        .expect("addr")
        .next()
        .expect("one addr");
    let start = Instant::now();
    loop {
        match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            Ok(s) => {
                s.set_read_timeout(Some(Duration::from_secs(5))).ok();
                s.set_write_timeout(Some(Duration::from_secs(5))).ok();
                return s;
            }
            Err(_) if start.elapsed() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!("server never accepted on {port}: {e}"),
        }
    }
}

fn wait_ready(port: u16) {
    let mut s = connect(port, Duration::from_secs(30));
    let start = Instant::now();
    loop {
        s.write_all(b"PING\r\n").expect("write PING");
        let mut buf = [0u8; 64];
        if let Ok(n) = s.read(&mut buf)
            && n > 0
            && buf[..n].windows(4).any(|w| w == b"PONG")
        {
            return;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "server accepted TCP but never answered PING"
        );
        std::thread::sleep(Duration::from_millis(100));
        s = connect(port, Duration::from_secs(5));
    }
}

// ---------------------------------------------------------------------------
// Minimal RESP2 client
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Resp>>),
}

struct Conn {
    s: TcpStream,
    buf: Vec<u8>,
    pos: usize,
}

impl Conn {
    fn open(port: u16) -> Self {
        Conn {
            s: connect(port, Duration::from_secs(10)),
            buf: Vec::with_capacity(16 * 1024),
            pos: 0,
        }
    }

    fn cmd(&mut self, parts: &[&str]) -> Resp {
        let mut req = Vec::with_capacity(64);
        req.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            req.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            req.extend_from_slice(p.as_bytes());
            req.extend_from_slice(b"\r\n");
        }
        self.s.write_all(&req).expect("write cmd");
        self.frame()
    }

    fn fill(&mut self) {
        let mut chunk = [0u8; 16 * 1024];
        let n = self.s.read(&mut chunk).expect("read");
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

    fn frame(&mut self) -> Resp {
        if self.pos > 0 && self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        }
        let line = self.line();
        let (tag, rest) = line.split_at(1);
        match tag {
            "+" => Resp::Simple(rest.to_string()),
            "-" => Resp::Error(rest.to_string()),
            ":" => Resp::Int(rest.parse().unwrap_or(0)),
            "$" => {
                let n: i64 = rest.parse().unwrap_or(-1);
                if n < 0 {
                    Resp::Bulk(None)
                } else {
                    Resp::Bulk(Some(self.exact(n as usize)))
                }
            }
            "*" => {
                let n: i64 = rest.parse().unwrap_or(-1);
                if n < 0 {
                    Resp::Array(None)
                } else {
                    let mut items = Vec::with_capacity(n as usize);
                    for _ in 0..n {
                        items.push(self.frame());
                    }
                    Resp::Array(Some(items))
                }
            }
            other => panic!("unexpected RESP tag {other:?} (line {line:?})"),
        }
    }
}

/// Find, for each shard, a `{hash-tag}` value whose tagged keys provably route
/// to that shard (via moon's own `key_to_shard`, so co-location is proven, not
/// assumed). `out[s]` is a tag whose `{tag}...` keys land on shard `s`.
fn tags_per_shard(num_shards: usize) -> Vec<String> {
    let mut out: Vec<Option<String>> = vec![None; num_shards];
    let mut found = 0;
    for i in 0..1_000_000 {
        let tag = format!("s{i}");
        // Only the tag inside `{}` is hashed, so any suffix routes identically.
        let probe = format!("{{{}}}k", tag);
        let s = key_to_shard(probe.as_bytes(), num_shards);
        if out[s].is_none() {
            out[s] = Some(tag);
            found += 1;
            if found == num_shards {
                break;
            }
        }
    }
    out.into_iter()
        .map(|o| o.expect("found a tag for every shard"))
        .collect()
}

/// The `(key, value)` pairs of the co-located group on shard `s`.
fn group_pairs(tag: &str) -> Vec<(String, String)> {
    (0..GROUP_SIZE)
        .map(|j| (format!("{{{}}}:k{}", tag, j), format!("{}-v{}", tag, j)))
        .collect()
}

/// A moon `MOONERR diskfull` write-pause reply. When the data volume dips below
/// the 5%-free guard, writes are refused and this durability test cannot run —
/// callers SKIP (not fail), so a full dev/CI disk doesn't masquerade as a
/// regression. On a healthy host (ample `/tmp`, as on CI) this never fires.
fn is_diskfull(r: &Resp) -> bool {
    matches!(r, Resp::Error(m) if m.contains("diskfull"))
}

/// Kill round-1, restart, and assert EVERY written pair recovered. The failure
/// message identifies the missing group so a RED run points straight at the
/// un-persisted local leg.
fn assert_all_survive_restart(
    port: u16,
    dir: &std::path::Path,
    child1: Child,
    expected: &[(String, String)],
    what: &str,
) {
    // > 1s so the everysec fsync window flushed every append that WAS made.
    std::thread::sleep(Duration::from_millis(1500));
    sigkill(child1);

    let _guard = ServerGuard(spawn_moon_aof(port, dir, SHARDS));
    wait_ready(port);

    let mut c = Conn::open(port);
    let mut missing: Vec<String> = Vec::new();
    let mut mismatched: Vec<String> = Vec::new();
    for (k, v) in expected {
        match c.cmd(&["GET", k]) {
            Resp::Bulk(Some(got)) if got == v.as_bytes() => {}
            Resp::Bulk(Some(got)) => mismatched.push(format!(
                "{k}: want={v} got={}",
                String::from_utf8_lossy(&got)
            )),
            Resp::Bulk(None) => missing.push(k.clone()),
            other => panic!("unexpected GET reply for {k}: {other:?}"),
        }
    }

    assert!(
        missing.is_empty() && mismatched.is_empty(),
        "{what}: coordinator LOCAL leg lost writes across restart — {} missing, {} mismatched. \
         Missing (the co-located group whose owner == the connection's own shard, whose local \
         leg never hit the AOF): {:?}; mismatched: {:?}",
        missing.len(),
        mismatched.len(),
        missing.iter().take(GROUP_SIZE + 1).collect::<Vec<_>>(),
        mismatched.iter().take(5).collect::<Vec<_>>(),
    );
}

// ---------------------------------------------------------------------------
// MSET — fast path (all keys of a group co-located on one shard).
// The group whose owner == the connection's shard takes the `groups.len() == 1
// && contains_key(&my_shard)` fast path → `string::mset(db, args)` with no AOF.
// ---------------------------------------------------------------------------

#[test]
fn mset_colocated_local_leg_persists_across_restart() {
    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");
    let child1 = spawn_moon_aof(port, dir.path(), SHARDS);
    wait_ready(port);

    let tags = tags_per_shard(SHARDS as usize);
    let mut expected: Vec<(String, String)> = Vec::new();

    // ONE connection for the whole write phase → my_shard is stable.
    let mut c = Conn::open(port);
    for tag in &tags {
        let pairs = group_pairs(tag);
        let mut argv: Vec<&str> = vec!["MSET"];
        for (k, v) in &pairs {
            argv.push(k);
            argv.push(v);
        }
        let resp = c.cmd(&argv);
        if is_diskfull(&resp) {
            eprintln!(
                "SKIP mset_colocated: MOONERR diskfull — durability untestable on a \
                 <5%-free filesystem; needs ample /tmp (CI has it)"
            );
            return;
        }
        assert_eq!(
            resp,
            Resp::Simple("OK".to_string()),
            "co-located MSET on tag {tag} should return OK"
        );
        expected.extend(pairs);
    }
    drop(c);

    assert_all_survive_restart(
        port,
        dir.path(),
        child1,
        &expected,
        "MSET co-located fast-path",
    );
}

// ---------------------------------------------------------------------------
// MSET — scatter path (ONE MSET spanning every shard). The slice owned by the
// connection's shard is the local leg (`db.set_string` per key, no AOF); the
// other slices scatter remotely (persisted).
// ---------------------------------------------------------------------------

#[test]
fn mset_scatter_local_slice_persists_across_restart() {
    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");
    let child1 = spawn_moon_aof(port, dir.path(), SHARDS);
    wait_ready(port);

    let tags = tags_per_shard(SHARDS as usize);
    // One key per shard, all in a single MSET → guaranteed multi-shard scatter
    // with a local slice on whichever shard the connection landed on.
    let expected: Vec<(String, String)> = tags
        .iter()
        .map(|tag| (format!("{{{}}}:scatter", tag), format!("{}-scatter", tag)))
        .collect();

    let mut argv: Vec<&str> = vec!["MSET"];
    for (k, v) in &expected {
        argv.push(k);
        argv.push(v);
    }
    let mut c = Conn::open(port);
    let resp = c.cmd(&argv);
    if is_diskfull(&resp) {
        eprintln!(
            "SKIP mset_scatter: MOONERR diskfull — durability untestable on a \
             <5%-free filesystem; needs ample /tmp (CI has it)"
        );
        return;
    }
    assert_eq!(
        resp,
        Resp::Simple("OK".to_string()),
        "scatter MSET should return OK"
    );
    drop(c);

    assert_all_survive_restart(
        port,
        dir.path(),
        child1,
        &expected,
        "MSET scatter local-slice",
    );
}

// ---------------------------------------------------------------------------
// MSETNX — co-located (all-new → :1). The group whose owner == the connection's
// shard runs via `run_on_owner` → `run_local` with no AOF append.
// ---------------------------------------------------------------------------

#[test]
fn msetnx_colocated_local_leg_persists_across_restart() {
    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");
    let child1 = spawn_moon_aof(port, dir.path(), SHARDS);
    wait_ready(port);

    let tags = tags_per_shard(SHARDS as usize);
    let mut expected: Vec<(String, String)> = Vec::new();

    let mut c = Conn::open(port);
    for tag in &tags {
        let pairs = group_pairs(tag);
        let mut argv: Vec<&str> = vec!["MSETNX"];
        for (k, v) in &pairs {
            argv.push(k);
            argv.push(v);
        }
        let resp = c.cmd(&argv);
        if is_diskfull(&resp) {
            eprintln!(
                "SKIP msetnx_colocated: MOONERR diskfull — durability untestable on a \
                 <5%-free filesystem; needs ample /tmp (CI has it)"
            );
            return;
        }
        assert_eq!(
            resp,
            Resp::Int(1),
            "all-new co-located MSETNX on tag {tag} should return 1"
        );
        expected.extend(pairs);
    }
    drop(c);

    assert_all_survive_restart(
        port,
        dir.path(),
        child1,
        &expected,
        "MSETNX co-located local leg",
    );
}
