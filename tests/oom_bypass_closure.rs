//! `--maxmemory` + eviction is enforced ONLY in the four connection handlers
//! (see `run_write_eviction_gate` in `src/server/conn/handler_monoio/mod.rs`).
//! Two write paths never ran that check, so memory could be driven past
//! `maxmemory` without limit:
//!
//! 1. Cross-shard SPSC write legs (`src/shard/spsc_handler.rs`): `Execute`,
//!    `PipelineBatch`, `MultiExecute` (+ their `*Slotted` variants) executed
//!    write commands against the TARGET shard's `&mut Database` directly,
//!    bypassing the connection handler's eviction gate entirely.
//! 2. Lua `redis.call` writes (`src/scripting/bridge.rs`): EVAL/EVALSHA carry
//!    no WRITE command flag (`src/command/metadata.rs`), so the dispatch-level
//!    OOM check never saw them, and the bridge itself never ran the check
//!    before executing a write inside a script.
//!
//! Wire-level on purpose: store-level tests cannot catch dispatch wiring —
//! see `tests/vector_del_unindex.rs` for the same rationale.
//!
//! Case B deliberately does NOT use MSET: `coordinate_mset`'s cross-shard
//! scatter path (`src/shard/coordinator.rs`) discards every remote leg's
//! reply and hardcodes `+OK` regardless of leg outcome — a separate,
//! pre-existing coordinator bug, independent of the SPSC eviction gate this
//! suite targets. A pipeline of individually-routed `SET` commands exercises
//! the exact same SPSC arms (`ExecuteSlotted`/`PipelineBatchSlotted`) while
//! faithfully propagating each remote leg's real reply back to the client,
//! cleanly isolating the SPSC-side bypass from that separate bug.
//!
//! Run alone with:
//!   MOON_BIN=$PWD/target/release/moon cargo test --test oom_bypass_closure

#![allow(clippy::unwrap_used)]

use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Binary resolution + server spawn (pattern: tests/vector_del_unindex.rs)
// ---------------------------------------------------------------------------

fn find_moon_binary() -> std::path::PathBuf {
    if let Ok(bin) = std::env::var("MOON_BIN") {
        let p = std::path::PathBuf::from(bin);
        if p.exists() {
            return p;
        }
    }
    let manifest = env!("CARGO_MANIFEST_DIR");
    let release = std::path::PathBuf::from(format!("{manifest}/target/release/moon"));
    if release.exists() {
        return release;
    }
    let debug = std::path::PathBuf::from(format!("{manifest}/target/debug/moon"));
    if debug.exists() {
        return debug;
    }
    panic!("No moon binary found. Build first or set MOON_BIN=/path/to/moon.");
}

/// Ports below 20000 collide with other services in CI/dev; pick a free one
/// above that floor instead of a fixed low port.
fn free_port() -> u16 {
    loop {
        let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
        let p = l.local_addr().expect("local_addr").port();
        drop(l);
        if p >= 20000 {
            return p;
        }
    }
}

/// `tempfile::tempdir()` defaults to `$TMPDIR`, which on macOS lives on the
/// root volume group — observed at ~95% full in dev environments, well past
/// Moon's 5%-free diskfull write-pause guard (`MOONERR diskfull`). Root the
/// test's scratch dirs under the repo's own volume instead, which has ample
/// headroom (see gotcha_vm_diskfull_shared_volume in project memory for the
/// VM-side analog of this trap).
fn test_tmpdir() -> tempfile::TempDir {
    let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/oom-test-tmp");
    std::fs::create_dir_all(&base).expect("create oom-test-tmp base dir");
    tempfile::Builder::new()
        .prefix("oom-bypass-")
        .tempdir_in(&base)
        .expect("tempdir_in target/oom-test-tmp")
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

/// Spawn moon with a tiny `--maxmemory` + `noeviction` policy so writes OOM
/// deterministically once the budget is exceeded.
fn spawn_moon_oom(
    port: u16,
    dir: &std::path::Path,
    shards: u32,
    maxmemory_bytes: u64,
) -> ServerGuard {
    let child = Command::new(find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--shards",
            &shards.to_string(),
            "--appendonly",
            "no",
            "--maxmemory",
            &maxmemory_bytes.to_string(),
            "--maxmemory-policy",
            "noeviction",
        ])
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
        .spawn()
        .expect("spawn moon");
    ServerGuard(child)
}

// ---------------------------------------------------------------------------
// Minimal RESP client (binary-safe args, full-frame parser)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
enum V {
    Simple(String),
    Err(String),
    Int(i64),
    Bulk(Vec<u8>),
    Arr(Vec<V>),
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
            ":" => V::Int(rest.parse().expect("int")),
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
                V::Arr((0..n).map(|_| self.parse()).collect())
            }
            other => panic!("unexpected RESP type {other:?} (line {line:?})"),
        }
    }

    fn cmd(&mut self, args: &[&[u8]]) -> V {
        self.writer.write_all(&Self::encode(args)).expect("send");
        self.parse()
    }

    /// Send all commands in ONE write (a wire pipeline), then read all replies.
    fn pipeline(&mut self, cmds: &[Vec<Vec<u8>>]) -> Vec<V> {
        let mut buf = Vec::new();
        for c in cmds {
            let refs: Vec<&[u8]> = c.iter().map(|a| a.as_slice()).collect();
            buf.extend_from_slice(&Self::encode(&refs));
        }
        self.writer.write_all(&buf).expect("send pipeline");
        cmds.iter().map(|_| self.parse()).collect()
    }

    /// Fallible PING for the readiness probe: a connection accepted while the
    /// server is still bringing up its per-shard SO_REUSEPORT listeners can be
    /// RESET mid-read — that must retry with a fresh connection, not panic.
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
        // Any I/O error (or a non-PONG answer, which would desync the framing)
        // drops this connection and probes again on a new one.
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

fn blob(size: usize, fill: u8) -> Vec<u8> {
    vec![fill; size]
}

// ---------------------------------------------------------------------------
// Case A: direct SET control. Sanity check that the pre-existing local write
// path enforces maxmemory (proves the test's OOM plumbing/assertions work
// before we lean on them for the cross-shard/Lua cases below).
// ---------------------------------------------------------------------------

#[test]
fn test_case_a_direct_set_control_oom() {
    let dir = test_tmpdir();
    let port = free_port();
    const MAXMEMORY: u64 = 2 * 1024 * 1024; // 2MB
    let _guard = spawn_moon_oom(port, dir.path(), 1, MAXMEMORY);
    let mut c = wait_ready(port);

    let value = blob(64 * 1024, b'a'); // 64KB per key
    const MAX_ITERS: usize = 200; // ~32 expected before OOM; generous margin.

    let mut oomed = false;
    for i in 0..MAX_ITERS {
        let key = format!("a:{i}");
        let r = c.cmd(&[b"SET", key.as_bytes(), &value]);
        if r.is_oom_error() {
            oomed = true;
            break;
        }
        assert_eq!(
            r,
            V::Simple("OK".into()),
            "SET a:{i} unexpected reply {r:?}"
        );
    }
    assert!(
        oomed,
        "control case: direct SET never hit OOM within {MAX_ITERS} iterations \
         at maxmemory={MAXMEMORY} — the handler-level eviction gate itself is \
         broken, which would invalidate cases B/C below."
    );
}

// ---------------------------------------------------------------------------
// Case B: cross-shard SPSC write legs. One wire pipeline of many
// individually-keyed SET commands (no hash tags) at shards=4 — the majority
// route to a shard other than the one this connection is pinned to, via the
// SPSC ExecuteSlotted/PipelineBatchSlotted arms in spsc_handler.rs.
//
// Before the M2 fix, those arms executed writes against the target shard's
// Database with NO eviction check at all, so remote legs kept returning +OK
// far past the point where a same-shard write would already OOM. This
// asserts the OOM error rate over a large oversubscribed pipeline, which is
// robust to elastic-budget redistribution (GAP-1) without pinning an exact
// byte threshold.
// ---------------------------------------------------------------------------

#[test]
fn test_case_b_cross_shard_pipeline_oom() {
    let dir = test_tmpdir();
    let port = free_port();
    const MAXMEMORY: u64 = 2 * 1024 * 1024; // 2MB whole-instance cap
    let _guard = spawn_moon_oom(port, dir.path(), 4, MAXMEMORY);
    let mut c = wait_ready(port);

    const N: usize = 3000;
    const VALUE_SIZE: usize = 4096; // 4KB — total attempted ~12MB vs 2MB cap.
    let value = blob(VALUE_SIZE, b'b');

    let cmds: Vec<Vec<Vec<u8>>> = (0..N)
        .map(|i| {
            vec![
                b"SET".to_vec(),
                format!("b:{i}").into_bytes(),
                value.clone(),
            ]
        })
        .collect();
    let replies = c.pipeline(&cmds);
    assert_eq!(replies.len(), N);

    let oom_count = replies.iter().filter(|r| r.is_oom_error()).count();
    let ok_count = replies
        .iter()
        .filter(|r| matches!(r, V::Simple(s) if s == "OK"))
        .count();
    let other_count = N - oom_count - ok_count;

    // With eviction enforced across ALL 4 shards, each shard accepts writes
    // until its share of the 2MB cap is used, then OOMs the rest — but the
    // exact OK count is timing-sensitive: the GAP-1 elastic budget
    // redistributes per-shard caps on a 100ms snapshot tick, so early writes
    // can land before budgets tighten (observed ~500-1100 OKs of 3000 across
    // machines). Without the fix, only the ~1/4 of keys landing on THIS
    // connection's own local shard can ever OOM (the pre-existing,
    // already-correct local path) — the other ~3/4 (remote legs) return +OK
    // unconditionally, bounding the pre-fix OOM count at roughly N/4 (~750).
    // N/2 sits with wide margin above that ceiling and below the post-fix
    // floor (~1900+ observed), so the assertion discriminates the bypass
    // without being sensitive to eviction-tick timing.
    assert!(
        oom_count >= N / 2,
        "cross-shard SPSC bypass: expected the large majority of {N} \
         oversubscribed writes to OOM once every shard's budget is \
         exhausted, got only {oom_count} OOM / {ok_count} OK / \
         {other_count} other — remote-shard legs are bypassing the \
         eviction gate"
    );
}

// ---------------------------------------------------------------------------
// Case C: Lua redis.call writes. EVAL/EVALSHA carry no WRITE command flag,
// so the dispatch-level check never saw them; the bridge itself never ran
// the eviction check before executing a write inside a script. A tight loop
// of redis.call('SET', ...) must be denied with an OOM error once the
// script's writes exceed maxmemory, not silently write past the cap.
// ---------------------------------------------------------------------------

#[test]
fn test_case_c_lua_redis_call_write_oom() {
    let dir = test_tmpdir();
    let port = free_port();
    const MAXMEMORY: u64 = 2 * 1024 * 1024; // 2MB
    let _guard = spawn_moon_oom(port, dir.path(), 1, MAXMEMORY);
    let mut c = wait_ready(port);

    // 2000 * 8KB = ~16MB attempted vs a 2MB cap — comfortably oversubscribed.
    let value = blob(8 * 1024, b'c');
    let script = b"for i=1,2000 do redis.call('SET', KEYS[1]..i, ARGV[1]) end return 'DONE'";

    let r = c.cmd(&[b"EVAL", script, b"1", b"lk:", &value]);

    match &r {
        V::Err(msg) => {
            assert!(
                msg.to_uppercase().contains("OOM"),
                "EVAL errored but not with OOM: {msg:?}"
            );
        }
        other => panic!(
            "Lua redis.call bypass: EVAL completed as {other:?} instead of \
             hitting OOM — writes inside a script are not gated by \
             --maxmemory"
        ),
    }
}

// ---------------------------------------------------------------------------
// Case D: read-only Lua script under memory pressure must NOT be blocked.
// The M2 fix gates redis.call inside `if cmd_is_write { ... }` in
// scripting/bridge.rs — this locks that invariant against a future
// over-broad gate (e.g. one that checks eviction unconditionally for every
// redis.call regardless of the inner command's write flag).
// ---------------------------------------------------------------------------

#[test]
fn test_case_d_readonly_eval_not_blocked_at_oom() {
    let dir = test_tmpdir();
    let port = free_port();
    const MAXMEMORY: u64 = 2 * 1024 * 1024; // 2MB
    let _guard = spawn_moon_oom(port, dir.path(), 1, MAXMEMORY);
    let mut c = wait_ready(port);

    // Drive the instance past its cap first (reuses case A's control setup).
    let value = blob(64 * 1024, b'd');
    let mut set_up_oom = false;
    for i in 0..200 {
        let key = format!("d:{i}");
        let r = c.cmd(&[b"SET", key.as_bytes(), &value]);
        if r.is_oom_error() {
            set_up_oom = true;
            break;
        }
    }
    assert!(
        set_up_oom,
        "setup: instance never reached OOM to test against"
    );

    // A key written before the cap was hit must still be readable via a
    // read-only script — the eviction gate must not fire for reads.
    let script = b"return redis.call('GET', KEYS[1])";
    let r = c.cmd(&[b"EVAL", script, b"1", b"d:0"]);
    assert_eq!(
        r,
        V::Bulk(value),
        "read-only EVAL was blocked (or returned the wrong value) while the \
         instance is over maxmemory — the eviction gate must only apply to \
         writes inside a script, not reads"
    );
}

// ---------------------------------------------------------------------------
// Case E: cross-db `COPY ... DB n` under memory pressure. `COPY` carries the
// generic `W` (write) flag (src/command/metadata.rs), so on the cross-shard
// dispatch arms handler_monoio actually uses for remote writes
// (`ExecuteSlotted`/`PipelineBatchSlotted`) it hits the SAME generic
// `is_write` eviction gate cases B/C already exercise — there is no
// COPY-specific bypass on that path. This case is a targeted regression
// check that COPY specifically (not just SET) drives that gate to a real
// OOM under cross-shard load, and locks in the dest-db gate committed in
// spsc_handler.rs's plain `Execute` arm (used by the single-key remote
// dispatch path) as a defensive duplicate.
//
// NOTE — separate, pre-existing, out-of-scope bug found while writing this
// case: cross-shard remote dispatch of `COPY ... DB n` silently ignores the
// DB clause and performs a same-db copy instead (the MOVE/COPY two-database
// intercept in spsc_handler.rs only exists in the plain `Execute` arm, not
// `ExecuteSlotted`/`PipelineBatchSlotted`, so remote COPY falls through to
// the generic single-db `key_extra::copy`). This is a data-correctness bug,
// independent of eviction — confirmed via manual probe (4/20 remote COPYs
// landed in the wrong db). Distinct dst keys below deliberately still land
// in db 0 for the ~3/4 of keys dispatched remotely; this case asserts only
// on the OOM behavior, not on which db the copy actually landed in.
// ---------------------------------------------------------------------------

#[test]
fn test_case_e_cross_db_copy_oom() {
    let dir = test_tmpdir();
    let port = free_port();
    const MAXMEMORY: u64 = 2 * 1024 * 1024; // 2MB whole-instance cap
    let _guard = spawn_moon_oom(port, dir.path(), 4, MAXMEMORY);
    let mut c = wait_ready(port);

    const N: usize = 300;
    const VALUE_SIZE: usize = 4096; // 4KB — N*VALUE_SIZE ~= 1.2MB, well under the 2MB cap.
    let value = blob(VALUE_SIZE, b'e');

    // Phase 1: SET N keys in db 0 (default), spread across all 4 shards.
    // Comfortably under budget — every SET must succeed.
    let set_cmds: Vec<Vec<Vec<u8>>> = (0..N)
        .map(|i| {
            vec![
                b"SET".to_vec(),
                format!("e:{i}").into_bytes(),
                value.clone(),
            ]
        })
        .collect();
    let set_replies = c.pipeline(&set_cmds);
    let set_ok = set_replies
        .iter()
        .filter(|r| matches!(r, V::Simple(s) if s == "OK"))
        .count();
    assert_eq!(
        set_ok, N,
        "setup: not all {N} initial SETs succeeded under budget (got {set_ok} OK) \
         — the phase-1 sizing assumption is wrong, adjust before trusting phase 2"
    );

    // Phase 2: COPY each key to a DISTINCT dst key (src != dst — required,
    // Redis rejects same-key COPY regardless of db) — doubles the value's
    // footprint, pushing per-shard usage past the cap.
    let copy_cmds: Vec<Vec<Vec<u8>>> = (0..N)
        .map(|i| {
            let src = format!("e:{i}").into_bytes();
            let dst = format!("e:{i}:c").into_bytes();
            vec![b"COPY".to_vec(), src, dst, b"DB".to_vec(), b"1".to_vec()]
        })
        .collect();
    let copy_replies = c.pipeline(&copy_cmds);
    assert_eq!(copy_replies.len(), N);

    let oom_count = copy_replies.iter().filter(|r| r.is_oom_error()).count();
    let ok_count = copy_replies
        .iter()
        .filter(|r| matches!(r, V::Int(1)))
        .count();
    let other_count = N - oom_count - ok_count;

    // Threshold picked from observed data, same methodology as case B:
    // pre-fix (git-stashed) run = 0/300 OOM; post-fix = 104/300. N/10 (30)
    // sits safely above the RED floor and safely below the GREEN observed
    // count, robust to elastic-budget (GAP-1) variance across CI machines.
    assert!(
        oom_count >= N / 10,
        "cross-shard COPY bypass: expected a substantial share of {N} COPYs \
         routed via cross-shard SPSC to OOM once their shard's budget is \
         exhausted, got only {oom_count} OOM / {ok_count} OK / \
         {other_count} other — COPY is not hitting the generic eviction \
         gate on the cross-shard write path"
    );
}
