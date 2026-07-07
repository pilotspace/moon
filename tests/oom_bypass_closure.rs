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
    // Fall back to the binary cargo built for THIS test run: compile-time
    // path with the right profile, CARGO_TARGET_DIR, and .exe suffix on
    // Windows (the old target/{release,debug}/moon probing found nothing on
    // Windows and could pick a stale release binary).
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
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
// Case E: cross-db `COPY ... DB n` under memory pressure. This locks in the
// destination-db eviction gate in `src/shard/spsc_two_db.rs`'s
// `try_two_db_intercept` (Gap A), which every `ShardMessage` SPSC arm now
// calls (previously only the plain `Execute` arm had a two-db intercept at
// all; the other arms — including `PipelineBatchSlotted`, the one real
// client traffic uses — silently ignored the `DB` clause and performed a
// same-db copy, a data-correctness bug, not an eviction one; see the CHANGELOG
// and the Gap A commit body). Moon's eviction gate is per-DATABASE (whichever
// db a write targets — `db.estimated_memory()` alone, not a shard-wide
// aggregate across all 16 dbs; see `try_evict_if_needed_budget`'s doc
// comment), consistently across every write path including this one — so
// db 1 (the COPY destination) needs its OWN pre-existing memory pressure to
// demonstrate the gate; ballast keys below give it that.
//
// Revision note: this case originally pre-loaded ONLY db 0, then asserted
// OOM on cross-db COPYs. That passed only because of the (now-fixed) Gap A
// bug: pre-fix, COPY's `DB 1` clause was silently ignored and the copy
// landed in db 0 (already loaded from phase 1), so the single-db gate
// tripped as a side effect of the routing bug, not because the destination
// db was actually under pressure. Post-fix, COPY correctly writes into db 1,
// which started empty, so the exact same setup produced 0/300 OOM — a
// regression alarm that traced back to a stale test premise, not a
// production bug (confirmed: temporarily stashing only the `if evict_active
// { spsc_eviction_gate(dst, ...) }` block inside `try_two_db_intercept`,
// while keeping the destination-db routing fix, reproduces RED here — see
// the Gap A commit body). Phase 1b below gives db 1 the same starting
// footprint as db 0 so the destination-db gate has something to trip on.
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

    // Phase 1b: ballast — SET the SAME N keys (same names, so they hash to
    // the SAME shards as their phase-1 counterparts) into db 1 too, so the
    // COPY destination starts with the same per-shard memory footprint as
    // the source db. Required because Moon's eviction gate is per-DATABASE
    // (see the module doc above) — without this, phase 2's copies land in a
    // near-empty db 1 and never cross budget, regardless of whether the
    // destination-db gate is wired correctly.
    assert_eq!(
        c.cmd(&[b"SELECT", b"1"]),
        V::Simple("OK".into()),
        "SELECT 1 (ballast) failed"
    );
    let ballast_replies = c.pipeline(&set_cmds);
    let ballast_ok = ballast_replies
        .iter()
        .filter(|r| matches!(r, V::Simple(s) if s == "OK"))
        .count();
    assert_eq!(
        ballast_ok, N,
        "ballast: not all {N} db-1 SETs succeeded under budget (got {ballast_ok} OK) \
         — the phase-1b sizing assumption is wrong, adjust before trusting phase 2"
    );
    assert_eq!(
        c.cmd(&[b"SELECT", b"0"]),
        V::Simple("OK".into()),
        "SELECT 0 (back for phase 2) failed"
    );

    // Phase 2: escalating rounds of COPYs, each round to FRESH dst keys
    // (src != dst — required, Redis rejects same-key COPY regardless of db),
    // until the destination db's per-shard footprint provably exceeds ANY
    // possible budget.
    //
    // Why rounds instead of the original single-shot statistical assert:
    // one round of 300×4KB (~300KB/shard) sits inside the slack that GAP-1's
    // elastic budget + its 100ms-stale usage snapshots can grant, so the
    // single-shot OOM share was timing-sensitive — 76/300 locally, 28/300 on
    // slow GitHub runners after one deflake (N/10 → N/30), and finally
    // 0/300 EXACTLY on both CI platforms (post-merge main runs of PR
    // #217/#218/#219 all red). Escalation removes the timing dependence via
    // an absolute ceiling: `compute_elastic_budget` can never grant a shard
    // more than `base + surplus ≤ maxmemory` (2MB here), and the gate is
    // per-DATABASE (`db.estimated_memory()` vs that budget) under
    // `noeviction` — so once db 1's per-shard usage passes 2MB, every
    // further gate-checked COPY into it MUST OOM, on any runner speed. 8
    // rounds × 300 × 4KB ≈ 9.6MB into db 1 (~2.4MB/shard, margin over the
    // 2MB ceiling even for the luckiest shard of the hash spread).
    //
    // The RED floor is unchanged and still exact: with the Gap A gate block
    // stashed, COPY never consults the eviction gate at ANY pressure, so
    // the cumulative OOM count stays 0 through all rounds (re-verified
    // red/green methodology in the Gap A commit body).
    const ROUNDS: usize = 8;
    let mut oom_count = 0usize;
    let mut ok_count = 0usize;
    let mut other_count = 0usize;
    for round in 0..ROUNDS {
        let copy_cmds: Vec<Vec<Vec<u8>>> = (0..N)
            .map(|i| {
                let src = format!("e:{i}").into_bytes();
                let dst = format!("e:{i}:c{round}").into_bytes();
                vec![b"COPY".to_vec(), src, dst, b"DB".to_vec(), b"1".to_vec()]
            })
            .collect();
        let copy_replies = c.pipeline(&copy_cmds);
        assert_eq!(copy_replies.len(), N);
        oom_count += copy_replies.iter().filter(|r| r.is_oom_error()).count();
        ok_count += copy_replies
            .iter()
            .filter(|r| matches!(r, V::Int(1)))
            .count();
        other_count = (round + 1) * N - oom_count - ok_count;
        if oom_count >= N / 30 {
            break; // gate demonstrably firing — no need to keep escalating
        }
    }

    assert!(
        oom_count >= N / 30,
        "cross-shard COPY bypass: expected COPYs routed via cross-shard SPSC \
         to OOM once the destination db's usage exceeds every possible \
         budget (≈{}KB/shard vs the {}KB absolute budget ceiling after \
         {ROUNDS} rounds), got only {oom_count} OOM / {ok_count} OK / \
         {other_count} other — COPY is not hitting the generic eviction \
         gate on the cross-shard write path",
        (N / 4 * VALUE_SIZE * (ROUNDS + 1)) / 1024,
        MAXMEMORY / 1024,
    );
}

// ---------------------------------------------------------------------------
// Case F: CONFIG SET maxmemory publishes/un-publishes the process-global
// atomic (Gap C — `crate::storage::eviction::{publish_maxmemory,
// maxmemory_is_set}`) that the cross-shard SPSC drain (`spsc_handler.rs`)
// and the Lua eviction bridge (`scripting/bridge.rs`) now read instead of a
// per-drain-cycle `RuntimeConfig` lock read / per-script generation Cell.
// Server starts WITHOUT --maxmemory (atomic unset at startup, exercising the
// startup-publish call site's absence-of-effect) so this test isolates the
// CONFIG SET write site specifically. Runs at shards=4 with the same
// cross-shard oversubscribed-pipeline shape as case B, so the OOM assertion
// specifically proves the SPSC arms observe the freshly-published atomic —
// not just the pre-existing local/inline fast path.
// ---------------------------------------------------------------------------

fn spawn_moon_no_maxmemory(port: u16, dir: &std::path::Path, shards: u32) -> ServerGuard {
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
            // Disk offload defaults to `enable`, which gives every shard's
            // SPSC drain a `spill_sender.is_some() == true` regardless of
            // maxmemory — that ORs into `evict_active` and would mask
            // whether the CONFIG SET call site actually published the
            // atomic under test. Disable it so `evict_active` here is driven
            // solely by `maxmemory_is_set()`.
            "--disk-offload",
            "disable",
            // Explicit `0`, NOT omitted: omitting --maxmemory triggers the
            // config auto-guardrail (config.rs ~1044-1101), which caps
            // maxmemory at a nonzero fraction of detected system RAM — that
            // nonzero value would make the STARTUP publish_maxmemory call
            // (main.rs) publish `maxmemory_is_set() == true` immediately,
            // making phase 2's OOM assertion pass regardless of whether the
            // CONFIG SET call site publishes anything at all. `0` is the
            // explicit, Redis-compatible "unlimited" escape hatch — it
            // starts the atomic definitively unset.
            "--maxmemory",
            "0",
        ])
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
        .spawn()
        .expect("spawn moon");
    ServerGuard(child)
}

#[test]
fn test_case_f_config_set_maxmemory_publishes_atomic() {
    let dir = test_tmpdir();
    let port = free_port();
    let _guard = spawn_moon_no_maxmemory(port, dir.path(), 4);
    let mut c = wait_ready(port);

    const N: usize = 3000;
    const VALUE_SIZE: usize = 4096; // 4KB, same shape as case B.
    let value = blob(VALUE_SIZE, b'f');

    // Phase 1: with no maxmemory ever configured (atomic unset at startup —
    // no --maxmemory flag was passed), a large cross-shard pipeline must all
    // succeed. Establishes the baseline before enabling the cap.
    let baseline_cmds: Vec<Vec<Vec<u8>>> = (0..N)
        .map(|i| {
            vec![
                b"SET".to_vec(),
                format!("f0:{i}").into_bytes(),
                value.clone(),
            ]
        })
        .collect();
    let baseline_replies = c.pipeline(&baseline_cmds);
    let baseline_ok = baseline_replies
        .iter()
        .filter(|r| matches!(r, V::Simple(s) if s == "OK"))
        .count();
    assert_eq!(
        baseline_ok, N,
        "setup: writes should all succeed before maxmemory is ever configured"
    );

    // Phase 2: CONFIG SET maxmemory-policy + a tiny maxmemory (no restart).
    // If `publish_maxmemory` at the CONFIG SET call site
    // (src/command/config.rs) is missing or wrong, `maxmemory_is_set()`
    // stays false and the SPSC drain's per-cycle `evict_active` snapshot
    // never turns on — remote legs would keep returning +OK regardless of
    // the now-configured cap.
    let r = c.cmd(&[b"CONFIG", b"SET", b"maxmemory-policy", b"noeviction"]);
    assert_eq!(
        r,
        V::Simple("OK".into()),
        "CONFIG SET maxmemory-policy failed: {r:?}"
    );
    const MAXMEMORY: &[u8] = b"2097152"; // 2MB whole-instance cap, same as cases B/E.
    let r = c.cmd(&[b"CONFIG", b"SET", b"maxmemory", MAXMEMORY]);
    assert_eq!(
        r,
        V::Simple("OK".into()),
        "CONFIG SET maxmemory failed: {r:?}"
    );

    let capped_cmds: Vec<Vec<Vec<u8>>> = (0..N)
        .map(|i| {
            vec![
                b"SET".to_vec(),
                format!("f1:{i}").into_bytes(),
                value.clone(),
            ]
        })
        .collect();
    let capped_replies = c.pipeline(&capped_cmds);
    let oom_count = capped_replies.iter().filter(|r| r.is_oom_error()).count();
    let ok_count = capped_replies
        .iter()
        .filter(|r| matches!(r, V::Simple(s) if s == "OK"))
        .count();
    assert!(
        oom_count >= N / 2,
        "CONFIG SET maxmemory publish: expected the large majority of {N} \
         oversubscribed cross-shard writes to OOM after CONFIG SET maxmemory \
         {MAXMEMORY:?}, got only {oom_count} OOM / {ok_count} OK — the \
         process-global maxmemory_is_set() atomic was not published at the \
         CONFIG SET call site (Gap C)"
    );

    // Phase 3: CONFIG SET maxmemory 0 (un-publish) — writes must succeed
    // again, proving the atomic flips back off and doesn't latch "active"
    // forever.
    let r = c.cmd(&[b"CONFIG", b"SET", b"maxmemory", b"0"]);
    assert_eq!(
        r,
        V::Simple("OK".into()),
        "CONFIG SET maxmemory 0 failed: {r:?}"
    );

    let unpublish_cmds: Vec<Vec<Vec<u8>>> = (0..N)
        .map(|i| {
            vec![
                b"SET".to_vec(),
                format!("f2:{i}").into_bytes(),
                value.clone(),
            ]
        })
        .collect();
    let unpublish_replies = c.pipeline(&unpublish_cmds);
    let unpublish_ok = unpublish_replies
        .iter()
        .filter(|r| matches!(r, V::Simple(s) if s == "OK"))
        .count();
    assert_eq!(
        unpublish_ok, N,
        "CONFIG SET maxmemory 0 un-publish: expected all {N} writes to \
         succeed once maxmemory is cleared, got only {unpublish_ok} OK — \
         the maxmemory_is_set() atomic did not flip back off"
    );
}

// ---------------------------------------------------------------------------
// Case G: FCALL-internal writes (Lua FUNCTION library). `FUNCTION LOAD`
// creates its own per-library sandboxed Lua VM (src/scripting/functions.rs),
// separate from the shard's shared EVAL/EVALSHA VM. That per-library VM
// used to register redis.call/pcall with `LuaEvictionCtx::disabled()`
// unconditionally, regardless of the EVAL/EVALSHA fix in case C — a
// FUNCTION whose body writes in a loop could grow memory past `maxmemory`
// without limit.
// ---------------------------------------------------------------------------

#[test]
fn test_case_g_fcall_internal_write_oom() {
    let dir = test_tmpdir();
    let port = free_port();
    const MAXMEMORY: u64 = 2 * 1024 * 1024; // 2MB
    let _guard = spawn_moon_oom(port, dir.path(), 1, MAXMEMORY);
    let mut c = wait_ready(port);

    // Registered functions are called with zero Lua args (`call_function` in
    // src/scripting/functions.rs invokes `registered.call(())`) — like a
    // plain EVAL body, they read `KEYS`/`ARGV` as globals, not parameters.
    let lib_body: &[u8] = b"#!lua name=oomlib\n\
        local function write_loop()\n\
          local val = ARGV[1]\n\
          for i = 1, 2000 do\n\
            redis.call('SET', 'gk:' .. i, val)\n\
          end\n\
          return 'DONE'\n\
        end\n\
        redis.register_function('write_loop', write_loop)";

    let load_reply = c.cmd(&[b"FUNCTION", b"LOAD", lib_body]);
    assert_eq!(
        load_reply,
        V::Bulk(b"oomlib".to_vec()),
        "FUNCTION LOAD failed: {load_reply:?}"
    );

    // 2000 * 8KB = ~16MB attempted vs a 2MB cap — comfortably oversubscribed
    // (same shape as case C's EVAL loop).
    let value = blob(8 * 1024, b'g');
    let r = c.cmd(&[b"FCALL", b"write_loop", b"0", &value]);

    match &r {
        V::Err(msg) => {
            assert!(
                msg.to_uppercase().contains("OOM"),
                "FCALL errored but not with OOM: {msg:?}"
            );
        }
        other => panic!(
            "FCALL-internal write bypass: FCALL completed as {other:?} \
             instead of hitting OOM — writes inside a FUNCTION are not \
             gated by --maxmemory"
        ),
    }
}

// ---------------------------------------------------------------------------
// Case H: control — FCALL with no maxmemory configured must succeed. Locks
// the write-only/OOM-only scope of the FCALL gate against a future
// over-broad check (mirrors case D for EVAL).
// ---------------------------------------------------------------------------

#[test]
fn test_case_h_fcall_no_maxmemory_succeeds() {
    let dir = test_tmpdir();
    let port = free_port();
    // Reuse spawn_moon_no_maxmemory (Gap C's case F helper) so this
    // genuinely has no cap — spawn_moon_oom always sets one.
    let _guard = spawn_moon_no_maxmemory(port, dir.path(), 1);
    let mut c = wait_ready(port);

    let lib_body: &[u8] = b"#!lua name=oklib\n\
        local function write_loop()\n\
          local val = ARGV[1]\n\
          for i = 1, 50 do\n\
            redis.call('SET', 'hk:' .. i, val)\n\
          end\n\
          return 'DONE'\n\
        end\n\
        redis.register_function('write_loop', write_loop)";

    let load_reply = c.cmd(&[b"FUNCTION", b"LOAD", lib_body]);
    assert_eq!(
        load_reply,
        V::Bulk(b"oklib".to_vec()),
        "FUNCTION LOAD failed: {load_reply:?}"
    );

    let value = blob(1024, b'h');
    let r = c.cmd(&[b"FCALL", b"write_loop", b"0", &value]);
    assert_eq!(
        r,
        V::Bulk(b"DONE".to_vec()),
        "FCALL without maxmemory configured should succeed, got {r:?}"
    );
}
