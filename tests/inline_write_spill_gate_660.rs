//! moon#660: the inline write fast path vs. a live `spill_sender`.
//!
//! ## What changed and why these tests exist
//!
//! `can_inline_writes` (`src/server/conn/handler_monoio/mod.rs`) used to carry
//! the term `ctx.spill_sender.is_none()`. `--disk-offload` defaults to
//! `enable`, which spawns a per-shard `SpillThread` and hands EVERY connection
//! a live sender, so that term was false out of the box: inline writes never
//! ran in the default configuration.
//!
//! The term was a CONFIG predicate standing in for a STATE one. What a live
//! sender actually changes is EVICTION ROUTING, and only that:
//!
//! | path                                             | sink                       | victim |
//! |--------------------------------------------------|----------------------------|--------|
//! | generic (`run_write_eviction_gate`), sender live  | `EvictionRun::async_spill` | SPILLED (`appendonly yes`) |
//! | inline (`try_inline_dispatch`)                    | `EvictionRun::plain`       | DROPPED |
//!
//! A plain `SET k v` has no other divergence: `string::set`'s `args.len() == 2`
//! fast path and the inline path build the same `Entry`, queue the same `set`
//! keyspace notification, and call the SAME `Database::set` — which is where
//! every cold-tier obligation lives (`spill_inflight_forget` retires an
//! in-flight spill payload, #459; the `Updated` arm drops a stale `cold_index`
//! shadow, task #56). Neither path consults or promotes the cold tier on a
//! write.
//!
//! So the invariant enforced by the fix is:
//!
//! > the inline write path may run only when it will produce the same
//! > observable state transition as generic dispatch — i.e. only when
//! > EVICTION PROVABLY WILL NOT FIRE on this write.
//!
//! answered per-write by the lock-free `inline_write_can_skip_eviction`
//! pre-gate, which the inline path now evaluates BEFORE it consumes the
//! command bytes from `read_buf`, so that "not handled, fall back" is a real
//! option rather than a half-executed write.
//!
//! ## The discriminator these tests rest on
//!
//! `INFO stats` reports two counters that are NEVER both incremented for the
//! same victim (`src/admin/metrics_setup/recorders.rs`, moon#585):
//!
//!   * `evicted_keys`  — the key LEFT the keyspace. Only
//!     `evict_one_with_spill(.., None, ..)` records it, which is exactly what
//!     `EvictionRun::plain` — the inline path's only sink — resolves to.
//!   * `spilled_keys`  — the key MOVED TO DISK and is still readable. Only
//!     `evict_one_async_spill` / `evict_batch_durable` record it, which is
//!     what `EvictionRun::async_spill` resolves to under `--appendonly yes`.
//!
//! A plain-drop where a spill was required is therefore not merely "a key went
//! missing" — it is visible as `evicted_keys` climbing while `spilled_keys`
//! does not.
//!
//! ## Reddening mutations
//!
//! Each test names its own; none of them is assumed. Every mutation below was
//! applied, observed, and reverted:
//!
//! | mutation | goes red |
//! |----------|----------|
//! | delete the `if needs_eviction && spill_sender_active { return 0; }` bail-out in `blocking.rs` | G1 shards=1 (`evicted_keys=1093, spilled_keys=0`), G2 shards=1 and 4 |
//! | restore `ctx.spill_sender.is_none()` to `can_inline_writes` (the pre-fix gate) | G2 (no connection inlines at all), and all four G4 tests at their vacuity CONTROL |
//! | gut `Database::remove_cold_only` (`storage/db/kv_ops.rs`) | G3 |
//! | drop `&& !monitored` from `can_inline_writes` | G4 monitor |
//! | drop `&& !conn.in_cross_txn()` from `can_inline_writes` | G6 both (counter `2 -> 3`; `GET k` answers `"modified"` after `TXN ABORT`) |
//! | delete the `is_any_write_stall_active()` bail-out in `blocking.rs` | NOT this file — `mem_watchdog` cases A and B, and `compaction_escape_hatch_718` (merge-base green, branch red) |
//! | delete the `pause_possibly_active()` bail-out | NOT this file — `server::conn::tests::test_inline_set_stands_down_under_client_pause` |
//! | delete the `loading::is_loading()` bail-out | NOT this file — `server::conn::tests::test_inline_set_stands_down_while_loading` |
//!
//! The write-stall row is deliberately guarded OUTSIDE this file. The refusal
//! it protects is produced by `segment_stall::stall_refusal`, whose exemptions
//! (moon#718's escape hatch) and three sources already have dedicated suites;
//! re-asserting them here would duplicate that coverage and drift from it. The
//! row is recorded so the mutation ledger stays a complete index of what was
//! proved, not only of what this file proves.
//!
//! The first of those is the precise WRONG fix this file exists to guard
//! against — widening the gate without giving the inline path a way to stand
//! down. It is silent-data-loss class: the client still receives `+OK`.
//!
//! Two things this file does NOT claim. G1 at shards=4 does not redden under
//! the bail-out mutation: most writes there are cross-shard and take the
//! generic (spilling) path anyway, so the counters stay healthy. It is a
//! coverage companion; **G1 shards=1 and G2 shards=4 carry the safety proof.**
//! And G3's restart assertion alone does not discriminate the cold plane —
//! under `--appendonly yes` the AOF replays `SET; SET; DEL` and the key dies
//! regardless, which is why G3 asserts the LIVE state first.
//!
//! Run with:
//!   cargo build --release
//!   MOON_BIN=$PWD/target/release/moon cargo test --release \
//!     --test inline_write_spill_gate_660

#![allow(clippy::unwrap_used)]
// The inline dispatch path exists ONLY in the monoio connection handler:
// `record_dispatch_local_inline` has exactly one production call site,
// `src/server/conn/handler_monoio/mod.rs`. Under a `runtime-tokio` build the
// `local_inline` counter is therefore permanently 0, and every CONTROL block
// in this file (`after > before`) would fail — not because the gate regressed
// but because the path under test is not compiled in. The whole suite is
// gated rather than each assertion: a test that cannot observe the mechanism
// is not a weaker guard, it is a false one.
#![cfg(feature = "runtime-monoio")]

mod common;

use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Fixture sizing
// ---------------------------------------------------------------------------

/// Probe keys, written FIRST so `allkeys-lru` makes them the victims.
const PROBE_COUNT: usize = 200;
const PROBE_VALUE_LEN: usize = 500;
/// Filler written after the probes to drive the shard past `maxmemory`.
const FILLER_COUNT: usize = 12_000;
const FILLER_VALUE_LEN: usize = 600;
/// 8 MiB instance-wide. Probes (~100 KiB) + filler (~7.2 MiB) crosses it at
/// shards=1; at shards=4 each shard's 2 MiB split is crossed sooner still.
const MAXMEMORY_BYTES: u64 = 8 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Binary resolution + server spawn (pattern: tests/oom_bypass_closure.rs)
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

/// `$TMPDIR` on macOS lives on the root volume group, observed at ~95% full in
/// dev environments — well past moon's 5%-free diskfull write-pause guard.
/// Root scratch under the repo's own volume instead.
fn test_tmpdir() -> tempfile::TempDir {
    let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/i660-test-tmp");
    std::fs::create_dir_all(&base).expect("create i660-test-tmp base dir");
    tempfile::Builder::new()
        .prefix("i660-")
        .tempdir_in(&base)
        .expect("tempdir_in target/i660-test-tmp")
}

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        // SIGKILL, not SIGTERM: moon's SIGTERM + SO_REUSEPORT teardown can hang
        // a bench/test harness (gotcha_moon_sigterm_reuseport_bench_hang).
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// How the server under test is configured. Every field here is load-bearing
/// for at least one test, so they are named rather than positional.
struct Cfg {
    shards: u32,
    /// `"yes"` puts the generic gate on the ASYNC SPILL branch — the only
    /// configuration in which the inline path's `plain` sink actually
    /// diverges from it. Under `"no"` the generic gate has no `ShardManifest`
    /// handle either and plain-drops identically (see the
    /// `EvictionSink::AsyncSpill` doc and
    /// `tests/crash_recovery_disk_offload_no_aof.rs`'s task #44 section), so a
    /// test of the routing divergence MUST use `"yes"`.
    appendonly: &'static str,
    /// `0` = start with no cap (inline writes eligible); tests that need
    /// pressure publish one at runtime with `CONFIG SET maxmemory`.
    maxmemory: u64,
    admin_port: u16,
}

fn spawn_moon(dir: &std::path::Path, cfg: &Cfg) -> (ServerGuard, u16) {
    let off_dir = dir.join("off");
    std::fs::create_dir_all(&off_dir).expect("create off dir");
    let (child, port) = common::spawn_listening(|port| {
        Command::new(find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &cfg.shards.to_string(),
                // The whole point: disk offload at its DEFAULT, so every
                // connection has a live `spill_sender`.
                "--disk-offload",
                "enable",
                "--disk-offload-dir",
                &off_dir.to_string_lossy(),
                "--appendonly",
                cfg.appendonly,
                // NOT `always`: `try_inline_dispatch` refuses to inline any
                // write under `appendfsync=always` (it cannot await the
                // writer's ack), which would make every test here vacuous.
                "--appendfsync",
                "everysec",
                "--maxmemory",
                &cfg.maxmemory.to_string(),
                "--maxmemory-policy",
                "allkeys-lru",
                // Under test is the eviction gate, not the disk guard; a
                // near-full dev volume would otherwise shadow every write
                // with `MOONERR diskfull`.
                "--disk-free-min-pct",
                "0",
                "--protected-mode",
                "no",
                "--admin-port",
                &cfg.admin_port.to_string(),
            ])
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
            .spawn()
            .expect("spawn moon (run `cargo build --release` first)")
    });
    (ServerGuard(child), port)
}

// ---------------------------------------------------------------------------
// Minimal RESP client (binary-safe, full-frame parser)
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
                Err(e) => {
                    assert!(
                        start.elapsed() < Duration::from_secs(20),
                        "moon on port {port} never accepted a connection: {e}"
                    );
                    std::thread::sleep(Duration::from_millis(50));
                }
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
            // RESP3 map — `HELLO 3` answers with one, and the tracking test
            // must speak RESP3 to turn CLIENT TRACKING on without a REDIRECT.
            "%" => {
                let n: i64 = rest.parse().expect("map len");
                V::Arr((0..n * 2).map(|_| self.parse()).collect())
            }
            "_" => V::Null,
            other => panic!("unexpected RESP type {other:?} (line {line:?})"),
        }
    }

    fn cmd(&mut self, args: &[&[u8]]) -> V {
        self.writer.write_all(&Self::encode(args)).expect("send");
        self.parse()
    }

    /// Send `cmds` in ONE write (a wire pipeline), then read every reply.
    /// Pipelining matters: `try_inline_dispatch_loop` drains a whole read
    /// buffer, so this is the shape that actually exercises the inline path
    /// the way `redis-benchmark -P` does.
    fn pipeline(&mut self, cmds: &[Vec<Vec<u8>>]) -> Vec<V> {
        let mut buf = Vec::new();
        for c in cmds {
            let refs: Vec<&[u8]> = c.iter().map(|a| a.as_slice()).collect();
            buf.extend_from_slice(&Self::encode(&refs));
        }
        self.writer.write_all(&buf).expect("send pipeline");
        cmds.iter().map(|_| self.parse()).collect()
    }

    fn info_stats(&mut self) -> String {
        match self.cmd(&[b"INFO", b"stats"]) {
            V::Bulk(b) => String::from_utf8_lossy(&b).into_owned(),
            other => panic!("INFO stats returned {other:?}"),
        }
    }
}

/// Pull `field:<u64>` out of an `INFO` payload.
fn info_field(info: &str, field: &str) -> u64 {
    for line in info.lines() {
        if let Some(v) = line
            .strip_prefix(field)
            .and_then(|rest| rest.strip_prefix(':'))
        {
            return v
                .trim()
                .parse()
                .unwrap_or_else(|e| panic!("INFO field {field} value {v:?} did not parse: {e}"));
        }
    }
    panic!("INFO payload has no field {field:?}; payload was:\n{info}");
}

/// Scrape `moon_dispatch_path_total{path="local_inline"}` off the admin port.
///
/// This is THE mechanism check. It is the only signal that distinguishes "the
/// inline path ran" from "generic dispatch produced an identical answer" —
/// every other observable is identical by design, which is the whole premise
/// of the fix.
fn local_inline_count(admin_port: u16) -> u64 {
    let body = http_get(admin_port, "/metrics");
    for line in body.lines() {
        if line.starts_with("moon_dispatch_path_total")
            && line.contains("local_inline")
            && !line.starts_with('#')
        {
            let n = line
                .rsplit(' ')
                .next()
                .expect("metric line has a value field");
            // Prometheus renders counters as floats ("12345" or "12345.0").
            return n
                .trim()
                .parse::<f64>()
                .unwrap_or_else(|e| panic!("could not parse local_inline value {n:?}: {e}"))
                as u64;
        }
    }
    // Absent counter == never incremented (metrics-rs does not emit an
    // untouched counter). That is a legitimate zero, not a scrape failure —
    // but only if the endpoint answered at all, which `http_get` asserts.
    0
}

fn http_get(port: u16, path: &str) -> String {
    let addr = format!("127.0.0.1:{port}")
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();
    let start = Instant::now();
    let mut stream = loop {
        match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            Ok(s) => break s,
            Err(e) => {
                assert!(
                    start.elapsed() < Duration::from_secs(20),
                    "admin port {port} never accepted a connection: {e}"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    stream
        .write_all(format!("GET {path} HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n").as_bytes())
        .expect("send admin request");
    let mut body = Vec::new();
    stream.read_to_end(&mut body).expect("read admin response");
    let text = String::from_utf8_lossy(&body).into_owned();
    assert!(
        text.contains("200"),
        "admin {path} did not answer 200; response was:\n{text}"
    );
    text
}

// ---------------------------------------------------------------------------
// Workload helpers
// ---------------------------------------------------------------------------

/// Accept a filler write's reply.
///
/// `+OK` is the expected answer. `-MOONERR AOF backpressure` is ALSO accepted
/// here, and only here — never for a key under test.
///
/// ## Why, and what it is telling you
///
/// This is a real, measured consequence of moon#660, not a fixture wart. The
/// inline write path is ~1.8x faster than generic dispatch (measured on
/// Linux), and on an `--appendonly yes` server a burst of pipelined writes can
/// now outrun the AOF writer's channel. `try_inline_dispatch` handles that
/// fail-loud by design (PR #211): it retries up to
/// `AOF_SPSC_BACKPRESSURE_BOUND` and then answers
/// `-MOONERR AOF backpressure: write applied in memory but not queued for
/// persistence` rather than a lying `+OK`. Observed here on roughly 1 run in 3
/// at `--shards 1`, and once as an aborted `redis-benchmark` leg on the Linux
/// bench host.
///
/// It is NOT data loss and NOT silent: the client is told. Accepting it for
/// FILLER — whose only job is to occupy memory, and whose durability nothing
/// in this file depends on — keeps these tests measuring the eviction gate
/// instead of the AOF writer's capacity.
///
/// Crucially this does not weaken the "no lost write" argument in
/// `bail_out_body`: that argument needs a REPLY per command, proving the
/// command was executed rather than swallowed by a bail-out that had already
/// consumed the bytes. An error is a reply.
///
/// `-OOM` is deliberately NOT accepted: `allkeys-lru` reclaiming instead of
/// rejecting is a property these tests do check.
fn assert_filler_accepted(r: &V, what: &str) {
    match r {
        V::Simple(s) if s == "OK" => {}
        V::Err(msg) if msg.contains("AOF backpressure") => {}
        other => {
            panic!("{what}: expected +OK (or the fail-loud AOF-backpressure error), got {other:?}")
        }
    }
}

fn probe_key(i: usize) -> Vec<u8> {
    format!("probe:{i:06}").into_bytes()
}

/// Plain `SET k v` — exactly `*3`, no options. This is the ONLY write shape
/// `try_inline_dispatch` accepts; anything richer (EX/NX/GET) falls out of the
/// fast path on its own and would make these tests vacuous.
fn set_cmd(key: Vec<u8>, len: usize) -> Vec<Vec<u8>> {
    vec![b"SET".to_vec(), key, vec![b'v'; len]]
}

fn write_probes(c: &mut Client) {
    for chunk in (0..PROBE_COUNT).collect::<Vec<_>>().chunks(50) {
        let cmds: Vec<Vec<Vec<u8>>> = chunk
            .iter()
            .map(|&i| set_cmd(probe_key(i), PROBE_VALUE_LEN))
            .collect();
        for r in c.pipeline(&cmds) {
            assert_eq!(r, V::Simple("OK".into()), "probe SET should succeed");
        }
    }
}

fn write_filler(c: &mut Client) {
    for chunk in (0..FILLER_COUNT).collect::<Vec<_>>().chunks(100) {
        let cmds: Vec<Vec<Vec<u8>>> = chunk
            .iter()
            .map(|&i| set_cmd(format!("filler:{i:06}").into_bytes(), FILLER_VALUE_LEN))
            .collect();
        for r in c.pipeline(&cmds) {
            // `allkeys-lru` must never answer -OOM: it reclaims instead.
            assert_filler_accepted(&r, "filler SET");
        }
    }
}

fn readable_probes(c: &mut Client) -> usize {
    let mut found = 0;
    for chunk in (0..PROBE_COUNT).collect::<Vec<_>>().chunks(50) {
        let cmds: Vec<Vec<Vec<u8>>> = chunk
            .iter()
            .map(|&i| vec![b"GET".to_vec(), probe_key(i)])
            .collect();
        for r in c.pipeline(&cmds) {
            if let V::Bulk(b) = r {
                assert_eq!(b.len(), PROBE_VALUE_LEN, "probe value truncated");
                found += 1;
            }
        }
    }
    found
}

// ===========================================================================
// GROUP 1 — SAFETY. Under pressure with a live spill sender, a write must
// still SPILL, never plain-drop.
// ===========================================================================

/// This is the whole safety case. With `--disk-offload enable` (default) and
/// `--appendonly yes`, generic dispatch routes eviction victims to the
/// `SpillThread` and they stay readable. If the inline path answers those
/// writes instead, it substitutes `EvictionRun::plain` — the victims are
/// DELETED and the client is told `+OK` regardless.
///
/// Three assertions, in increasing strength:
///   1. non-vacuity — tiering actually ran (`spilled_keys > 0`);
///   2. routing — drops are the rare exception, not the rule;
///   3. the user-visible invariant — NO probe was lost.
///
/// Reddening mutation: delete the `if needs_eviction && spill_sender_active
/// { return 0; }` bail-out in `src/server/conn/blocking.rs`. Every inline SET
/// then resolves eviction with the `plain` sink; `spilled_keys` collapses,
/// `evicted_keys` climbs, and `readable_probes` falls far below `PROBE_COUNT`.
fn spill_not_drop_body(shards: u32) {
    let dir = test_tmpdir();
    let admin_port = common::reserve_port();
    let cfg = Cfg {
        shards,
        appendonly: "yes",
        maxmemory: MAXMEMORY_BYTES,
        admin_port,
    };
    let (_guard, port) = spawn_moon(dir.path(), &cfg);
    let mut c = Client::connect(port);

    write_probes(&mut c);
    write_filler(&mut c);

    let info = c.info_stats();
    let evicted = info_field(&info, "evicted_keys");
    let spilled = info_field(&info, "spilled_keys");

    // (1) Non-vacuity. If neither counter moved, the fixture never created
    //     memory pressure and the rest of this test proves nothing — the
    //     `gotcha_vacuous_benchmark_never_fires_guard` failure mode.
    assert!(
        evicted + spilled > 0,
        "shards={shards}: fixture created NO memory pressure \
         (evicted_keys={evicted}, spilled_keys={spilled}) — the test would be \
         vacuous; raise FILLER_COUNT or lower MAXMEMORY_BYTES"
    );
    assert!(
        spilled > 0,
        "shards={shards}: memory pressure fired but NOTHING was tiered \
         (evicted_keys={evicted}, spilled_keys={spilled}). Under \
         --disk-offload enable + --appendonly yes every write-path victim must \
         reach the SpillThread; a zero here means the inline fast path \
         resolved eviction with EvictionRun::plain and DELETED them"
    );

    // (2) Routing. A handful of plain drops are legitimate (a victim racing
    //     lazy expiry, moon#553's TTL floor), but they must be the exception.
    assert!(
        evicted * 10 < spilled,
        "shards={shards}: plain drops dominate tiering \
         (evicted_keys={evicted}, spilled_keys={spilled}) — victims are being \
         deleted where they should be spilled"
    );

    // (3) The invariant a user can see. Spilling never removes a key from the
    //     keyspace, so every probe must still answer — from hot RAM, from the
    //     in-flight spill plane, or from the cold index.
    let found = readable_probes(&mut c);
    assert_eq!(
        found,
        PROBE_COUNT,
        "shards={shards}: {} of {PROBE_COUNT} probes were LOST across eviction \
         (evicted_keys={evicted}, spilled_keys={spilled}). A spilled key never \
         leaves the keyspace; a plain-dropped one does.",
        PROBE_COUNT - found
    );
}

#[test]
fn g1_spill_not_drop_under_pressure_shards1() {
    spill_not_drop_body(1);
}

#[test]
fn g1_spill_not_drop_under_pressure_shards4() {
    spill_not_drop_body(4);
}

// ===========================================================================
// GROUP 2 — THE BAIL-OUT IS EXERCISED (both directions).
// ===========================================================================

/// One test, two claims, because they are the two halves of the same change
/// and testing either alone is misleading:
///
///   * phase A — the gate WIDENED: with a live `spill_sender` and no memory
///     pressure, plain SETs now take the inline path. Pre-#660 this was
///     structurally impossible (`spill_sender.is_none()` was false out of the
///     box), so a non-zero delta here is the perf fix itself.
///   * phase B — the bail-out FIRES: once that shard is genuinely over budget,
///     the inline counter must go flat. Without this half, phase A alone would
///     also pass on the wrong (unsafe) fix.
///
/// This addresses the "prove the condition is not permanently true" clause:
/// the same binary, same connection, same command shape is shown inlining and
/// then NOT inlining, with only live memory pressure changing between them.
///
/// ## Why every key carries a `{t}` hash tag
///
/// Memory pressure is PER SHARD (`elastic_budget(shard_id)` against that
/// shard's `estimated_memory()`), and so is the bail-out. Spraying keys across
/// four shards drives only some of them over budget, and writes to the others
/// go on inlining — correctly, because on an under-budget shard eviction
/// provably cannot fire, which is precisely the invariant. A first draft of
/// this test asserted a flat counter across all shards and failed with 33
/// inlined writes at shards=4 for exactly that reason: the assertion was
/// wrong, not the code.
///
/// `{t}` co-locates every key here on ONE shard (`key_to_shard` hashes only
/// the tag, outside cluster mode too), so "the shard under test is over
/// budget" is unambiguous and the flat-counter assertion is exact at any
/// shard count.
///
/// Reddening mutations:
///   * phase A goes red by restoring `ctx.spill_sender.is_none()` to
///     `can_inline_writes` (the pre-#660 gate) — no connection ever inlines
///     and `connection_on_tag_shard` exhausts its attempts.
///   * phase B goes red by deleting the `return 0` bail-out in
///     `src/server/conn/blocking.rs` — the inline counter keeps climbing
///     under pressure.
fn tagged_key(tag: &str, prefix: &str, i: usize) -> Vec<u8> {
    format!("{{{tag}}}:{prefix}:{i:06}").into_bytes()
}

/// A connection paired with a hash tag that routes to THAT connection's own
/// shard.
struct Pinned {
    c: Client,
    tag: String,
}

/// Phase-A probes written per tag attempt.
const G2_PROBE_COUNT: usize = 40;

/// Find a hash tag whose keys are owned by this connection's shard, proving it
/// by observing `local_inline` move.
///
/// Searching TAGS rather than CONNECTIONS is deliberate. `try_inline_dispatch`
/// bails when `key_to_shard(key) != shard_id`, so the pair (connection, tag)
/// has to agree — and only one half of that pair is under the test's control.
/// A first draft opened 40 connections hoping one would land on the shard
/// owning a fixed `{t}`; all 40 missed, because which shard accepts a
/// connection is the kernel's decision (SO_REUSEPORT 4-tuple hashing) and is
/// not uniform for sequential connections from one client process. Which shard
/// owns a TAG, by contrast, is a pure function this test can enumerate.
///
/// This doubles as phase A: a (connection, tag) pair that inlines at all is
/// proof the `spill_sender` term is gone from `can_inline_writes`.
fn pin_to_local_shard(port: u16, admin_port: u16, shards: u32) -> Pinned {
    let mut c = Client::connect(port);
    for n in 0..24u32 {
        let tag = format!("s{n}");
        let before = local_inline_count(admin_port);
        for i in 0..G2_PROBE_COUNT {
            assert_eq!(
                c.cmd(&[
                    b"SET",
                    &tagged_key(&tag, "probe", i),
                    &vec![b'v'; PROBE_VALUE_LEN]
                ]),
                V::Simple("OK".into()),
                "tagged probe SET should succeed"
            );
        }
        if local_inline_count(admin_port) > before {
            return Pinned { c, tag };
        }
    }
    panic!(
        "shards={shards}: 24 candidate hash tags, none inlined a single plain \
         SET with a live spill_sender and no memory pressure. With 24 tags over \
         {shards} shards at least one must route to this connection's own \
         shard, so this is the pre-#660 gate (`spill_sender.is_none()`), i.e. \
         the 1.86x regression — not a routing miss."
    );
}

/// Hard cap on filler writes while waiting for tiering to start.
const G2_FILLER_CAP: usize = 40_000;
/// Writes in the measurement window (see `bail_out_body`).
const G2_WINDOW: usize = 2_000;

fn write_tagged_chunk(c: &mut Client, tag: &str, from: usize, count: usize) {
    let cmds: Vec<Vec<Vec<u8>>> = (from..from + count)
        .map(|i| set_cmd(tagged_key(tag, "filler", i), FILLER_VALUE_LEN))
        .collect();
    for r in c.pipeline(&cmds) {
        assert_filler_accepted(&r, "tagged filler SET");
    }
}

fn bail_out_body(shards: u32) {
    let dir = test_tmpdir();
    let admin_port = common::reserve_port();
    let cfg = Cfg {
        shards,
        appendonly: "yes",
        maxmemory: MAXMEMORY_BYTES,
        admin_port,
    };
    let (_guard, port) = spawn_moon(dir.path(), &cfg);

    // ---- phase A: no pressure, live spill sender -> inline runs ----
    let Pinned { mut c, tag } = pin_to_local_shard(port, admin_port, shards);

    // ---- phase B: write until this shard starts tiering ----
    let mut written = 0usize;
    let mut spilled = 0u64;
    while spilled == 0 && written < G2_FILLER_CAP {
        write_tagged_chunk(&mut c, &tag, written, 500);
        written += 500;
        spilled = info_field(&c.info_stats(), "spilled_keys");
    }
    assert!(
        spilled > 0,
        "shards={shards}: {written} tagged filler writes started no tiering \
         (spilled_keys=0); the measurement below would be vacuous"
    );

    // ---- the measurement: a WINDOW that validates its own preconditions ----
    //
    // Two earlier designs failed here, and both failures are the reason this
    // one is shaped the way it is:
    //
    //   * "establish a steady state, then measure" assumed the shard would
    //     STAY over budget. It did not — the elastic budget (GAP-1) lets this
    //     shard borrow the idle siblings' headroom up to the whole instance
    //     `maxmemory`, so a fixed filler sized against `maxmemory / shards`
    //     never crossed the real threshold and 196 of 200 measurement writes
    //     inlined, correctly.
    //   * sizing the filler against the INSTANCE cap instead did cross it, and
    //     then saturated the spill thread: `allkeys-lru` answered `-OOM`
    //     reproducibly, because bytes handed to the spill thread stay counted
    //     as resident (moon#466) until their completions land.
    //
    // So this does not try to hold a steady state at all. It measures a window
    // and PROVES from inside the window that eviction fired during it:
    // `spilled_keys` must climb across the window, and the only traffic on
    // this server is this connection's tagged writes, which all land on this
    // one shard. Over that same window not one write may inline.
    //
    // The `+OK` assertions inside `write_tagged_chunk` are not decoration:
    // they are the proof that the bail-out did not CONSUME the command.
    // `try_inline_dispatch` bails BEFORE `read_buf.split_to()`, so generic
    // dispatch re-parses the same bytes and answers. Had the bail been placed
    // after the split, these writes would have been swallowed with no reply —
    // the client would block for replies that never come and the test would
    // hang on its socket timeout. A silent write loss is observable here as a
    // hang, a wrong one as a mismatch; neither can pass.
    let inline_before = local_inline_count(admin_port);
    let spilled_before = spilled;
    let evicted_before = info_field(&c.info_stats(), "evicted_keys");
    // The window RUNS UNTIL it has proved its own precondition, rather than
    // writing a fixed count and hoping. A fixed `G2_WINDOW` was enough to make
    // this shard tier on macOS and NOT enough on the Linux CI host (measured:
    // `spilled_keys` 351 -> 351, and the non-vacuity assertion below correctly
    // refused to report a pass). Sizing a fixed window to the slowest platform
    // would just make it slow everywhere and still be a guess.
    let mut w = 0usize;
    let mut spilled_after = spilled_before;
    while w < G2_FILLER_CAP {
        write_tagged_chunk(&mut c, &tag, written + w, 500);
        w += 500;
        spilled_after = info_field(&c.info_stats(), "spilled_keys");
        if w >= G2_WINDOW && spilled_after > spilled_before {
            break;
        }
    }
    let inline_after = local_inline_count(admin_port);
    let evicted_after = info_field(&c.info_stats(), "evicted_keys");

    assert!(
        spilled_after > spilled_before,
        "shards={shards}: no key was tiered during the measurement window \
         ({spilled_before} -> {spilled_after}), so the window was not under \
         eviction pressure and the flat-counter assertion below proves nothing"
    );
    // THE SAFETY PROPERTY. What must never happen is a victim DROPPED where it
    // should have been SPILLED — that is the silent data loss this whole file
    // exists to guard, and it is what deleting the bail-out produces
    // (measured: `evicted_keys` 1093, `spilled_keys` flat at 0).
    let evicted_delta = evicted_after - evicted_before;
    let spilled_delta = spilled_after - spilled_before;
    assert!(
        evicted_delta * 10 < spilled_delta,
        "shards={shards}: plain drops dominate tiering across the window \
         (evicted_keys {evicted_before} -> {evicted_after}, spilled_keys \
         {spilled_before} -> {spilled_after}) — victims are being DELETED \
         where a live spill sender requires them to be SPILLED. This is the \
         bail-out failing to stand the inline path down."
    );

    // The bail-out's OWN behaviour, stated as what it actually guarantees.
    //
    // An earlier version asserted `inline_after == inline_before` and, in the
    // same breath, blamed any slip on victims being "plain-dropped instead of
    // spilled". Both halves were wrong, and the Linux CI leg caught it:
    // 5 of 2000 writes inlined during a window in which eviction fired.
    //
    // The mechanism cannot promise zero. `inline_write_can_skip_eviction`
    // reads PUBLISHED hints — `MAXMEMORY_HINT`, `MAXMEMORY_PER_SHARD_HINT`,
    // the once-a-second footprint correction — and an `elastic_budget`
    // refreshed on a 100 ms tick. During rapid growth those lag the live
    // figure, so a write can be told "no pressure" while the shard is in fact
    // over budget.
    //
    // What such a write does is SKIP eviction, not resolve it: the bail is
    // `needs_eviction && spill_sender_active`, so a stale `needs_eviction =
    // false` means the eviction block never runs and no `EvictionRun::plain`
    // is ever built. The cost is a deferred eviction and a transient overshoot
    // that the next write — with refreshed hints — corrects. It is NOT a drop,
    // which is why the assertion above is the one carrying the safety claim.
    //
    // So this bounds the slip instead of forbidding it. A regression that
    // genuinely disabled the bail-out does not slip 0.25%; it inlines the
    // whole window, which this still catches by two orders of magnitude.
    let inline_delta = inline_after - inline_before;
    let slip_ceiling = (w as u64) / 100; // 1% of the writes actually issued
    assert!(
        inline_delta <= slip_ceiling,
        "shards={shards}: {inline_delta} of {w} writes inlined during a window \
         in which eviction demonstrably fired ({inline_before} -> \
         {inline_after}, spilled {spilled_before} -> {spilled_after}). \
         Published-hint staleness explains a slip of a few writes; more than \
         {slip_ceiling} means the bail-out is not firing at all."
    );

    // The safety consequence, restated end-to-end: nothing was lost while the
    // fallback was carrying the load.
    let mut found = 0;
    for i in 0..G2_PROBE_COUNT {
        if let V::Bulk(b) = c.cmd(&[b"GET", &tagged_key(&tag, "probe", i)]) {
            assert_eq!(b.len(), PROBE_VALUE_LEN, "probe value truncated");
            found += 1;
        }
    }
    assert_eq!(
        found,
        G2_PROBE_COUNT,
        "shards={shards}: {} of {G2_PROBE_COUNT} phase-A probes were LOST \
         while the inline path was standing down",
        G2_PROBE_COUNT - found
    );
}

#[test]
fn g2_bail_out_fires_under_pressure_shards1() {
    bail_out_body(1);
}

#[test]
fn g2_bail_out_fires_under_pressure_shards4() {
    bail_out_body(4);
}

// ===========================================================================
// GROUP 3 — NO RESURRECTION (#212 / #213 / #459 class).
// ===========================================================================

/// The scary one. A DEL of a key that has a cold-tier copy must be FINAL.
///
/// The hazard this exercises is specific to widening the inline gate: an
/// inline `SET` writes straight into the hot table via `Database::set`. If the
/// key already has a cold copy, the delete has to reach BOTH planes —
/// `Database::remove_cold_only` drops the `cold_index` entry AND retires any
/// in-flight spill record, the latter being the load-bearing half (#459): the
/// in-flight record is the spill completion's authorisation to publish into
/// `cold_index`, so without it a DEL issued during the spill window is UNDONE
/// when the spill lands.
///
/// ## Why this asserts LIVE first and across a restart second
///
/// A first draft asserted only the post-restart state, and gutting
/// `remove_cold_only` did not turn it red: under `--appendonly yes` the AOF
/// replays `SET v1; SET v2; DEL` and the key ends up dead however the cold
/// plane behaved. The restart assertion is therefore a real end-to-end
/// guarantee but a BLUNT instrument for this seam — AOF replay masks it. The
/// LIVE assertion has no such backstop: `GET`/`EXISTS` consult the hot plane,
/// the in-flight plane and `cold_index` directly, so a cold copy that outlived
/// its DEL is visible immediately.
///
/// ## Why 200 keys and not one
///
/// Which keys `allkeys-lru` picks is sampled, so no single key is guaranteed
/// to be cold at the moment of the DEL. Over 200 probes, some certainly are —
/// and the test states its own ground truth (`spilled_keys`) rather than
/// assuming it.
///
/// Reddening mutation: gut `Database::remove_cold_only`
/// (`src/storage/db/kv_ops.rs`) to a no-op — the delete then reaches only the
/// hot plane, and every probe that was cold answers its old value from the
/// cold read-through on the very next `GET`.
#[test]
fn g3_del_of_spilled_keys_stays_dead_live_and_across_restart() {
    let dir = test_tmpdir();
    let admin_port = common::reserve_port();
    let cfg = Cfg {
        shards: 1,
        appendonly: "yes",
        maxmemory: MAXMEMORY_BYTES,
        admin_port,
    };
    let (mut guard, port) = spawn_moon(dir.path(), &cfg);
    let mut c = Client::connect(port);

    // (1) probes v1, (2) filler to push them out of RAM.
    write_probes(&mut c);
    write_filler(&mut c);
    let spilled = info_field(&c.info_stats(), "spilled_keys");
    assert!(
        spilled > 0,
        "nothing was tiered, so this test never reaches the cold-plane hazard \
         it exists to guard (spilled_keys={spilled})"
    );

    // (3) overwrite HALF of them: a hot value landing over a cold copy, which
    //     is the `Database::set` `Updated`-arm / `spill_inflight_forget` seam.
    for i in (0..PROBE_COUNT).step_by(2) {
        assert_eq!(
            c.cmd(&[b"SET", &probe_key(i), &vec![b'2'; PROBE_VALUE_LEN]]),
            V::Simple("OK".into()),
            "overwrite SET should succeed"
        );
    }

    // Ground truth: every probe is still retrievable right before the DEL, so
    // a nil AFTER it can only mean the DEL worked — never that the key had
    // already been plain-dropped.
    assert_eq!(
        readable_probes(&mut c),
        PROBE_COUNT,
        "probes were lost BEFORE the DEL; the delete assertions below would \
         pass for the wrong reason"
    );

    // (4) DEL every probe. This must be final on BOTH planes.
    for i in 0..PROBE_COUNT {
        assert_eq!(
            c.cmd(&[b"DEL", &probe_key(i)]),
            V::Int(1),
            "DEL of probe {i} should remove exactly one key"
        );
    }

    // (5) LIVE assertion — no AOF backstop involved. `GET` consults hot, then
    //     the in-flight spill plane, then `cold_index`.
    let alive = readable_probes(&mut c);
    assert_eq!(
        alive, 0,
        "RESURRECTION (live): {alive} of {PROBE_COUNT} DEL'd keys still answer \
         a value. A cold copy outlived its delete — the #212/#213/#459 class."
    );
    for i in 0..PROBE_COUNT {
        assert_eq!(
            c.cmd(&[b"EXISTS", &probe_key(i)]),
            V::Int(0),
            "EXISTS says probe {i} is still present after DEL"
        );
    }

    // Let `appendfsync everysec` and the spill/manifest ticks settle, so the
    // crash below tests recovery ordering rather than a 1-second AOF window.
    std::thread::sleep(Duration::from_secs(3));

    // (6) hard crash, (7) restart on the same dir.
    drop(c);
    guard.0.kill().expect("SIGKILL moon");
    guard.0.wait().expect("reap moon");
    drop(guard);
    common::wait_for_port_down(port);

    let restart = Command::new(find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.path().to_string_lossy(),
            "--shards",
            "1",
            "--disk-offload",
            "enable",
            "--disk-offload-dir",
            &dir.path().join("off").to_string_lossy(),
            "--appendonly",
            "yes",
            "--appendfsync",
            "everysec",
            "--maxmemory",
            &MAXMEMORY_BYTES.to_string(),
            "--maxmemory-policy",
            "allkeys-lru",
            "--disk-free-min-pct",
            "0",
            "--protected-mode",
            "no",
        ])
        .stdout(std::fs::File::create(dir.path().join("moon.restart.stdout.log")).unwrap())
        .stderr(std::fs::File::create(dir.path().join("moon.restart.stderr.log")).unwrap())
        .spawn()
        .expect("restart moon");
    let _restart_guard = ServerGuard(restart);

    let mut c2 = Client::connect(port);
    // (8) end-to-end: the cold rebuild must not hand any of them back.
    let revived = readable_probes(&mut c2);
    assert_eq!(
        revived, 0,
        "RESURRECTION (restart): {revived} of {PROBE_COUNT} DEL'd keys came \
         back after a crash + cold rebuild."
    );
}

// ===========================================================================
// GROUP 4 — EVERY OTHER GATE TERM STILL HOLDS.
// ===========================================================================
//
// This change removes exactly ONE term from `can_inline_writes`
// (`ctx.spill_sender.is_none()`). These tests pin the remaining terms so a
// future edit cannot widen them by accident — each drives plain SETs that
// WOULD inline, under a condition that must suppress inlining, and asserts the
// `local_inline` counter does not move.
//
// Reddening mutation for all of them: delete the corresponding term from
// `can_inline_writes` in `src/server/conn/handler_monoio/mod.rs`. Each test
// then sees the counter climb.

/// Shared body: run `setup` on a dedicated connection, then issue plain SETs
/// on THAT SAME connection and require zero inlining.
///
/// The SETs must go down the connection the condition applies to — a fresh
/// connection per probe would silently test the wrong thing
/// (gotcha_fresh_connection_per_probe_hides_shard_local_bugs).
fn assert_no_inline_with(label: &str, setup: impl FnOnce(u16, &mut Client) -> Vec<Client>) {
    let dir = test_tmpdir();
    let admin_port = common::reserve_port();
    let cfg = Cfg {
        shards: 1,
        appendonly: "yes",
        maxmemory: 0,
        admin_port,
    };
    let (_guard, port) = spawn_moon(dir.path(), &cfg);

    // Control first: prove that on THIS server, in THIS configuration, a plain
    // SET on an unencumbered connection DOES inline. Without this the test
    // could pass because inlining never happens at all.
    {
        let mut control = Client::connect(port);
        let before = local_inline_count(admin_port);
        for i in 0..50 {
            control.cmd(&[b"SET", format!("ctl:{i}").as_bytes(), b"v"]);
        }
        let after = local_inline_count(admin_port);
        assert!(
            after > before,
            "{label}: CONTROL failed — plain SET did not inline even on an \
             unencumbered connection ({before} -> {after}). The suppression \
             assertion below would be vacuous."
        );
    }

    let mut c = Client::connect(port);
    // Aux connections (a MONITOR feed, a tracking client) are returned rather
    // than dropped: several of these gate terms are PROCESS-GLOBAL
    // (`monitor::any_attached`, `tracking::tracking_active`), so the condition
    // exists only while the other connection is still open.
    let _aux = setup(port, &mut c);
    let before = local_inline_count(admin_port);
    for i in 0..50 {
        c.cmd(&[b"SET", format!("{label}:{i}").as_bytes(), b"v"]);
    }
    let after = local_inline_count(admin_port);
    assert_eq!(
        after,
        before,
        "{label}: {} writes were inlined while this condition was active. The \
         inline path skips the side effect this term exists to preserve.",
        after - before
    );
}

#[test]
fn g4_multi_still_suppresses_inline() {
    // Inside MULTI a command must be QUEUED. The inline path would ANSWER it,
    // and EXEC would then omit it entirely — see tests/multi_queues_inline_get.rs.
    assert_no_inline_with("multi", |_port, c| {
        assert_eq!(c.cmd(&[b"MULTI"]), V::Simple("OK".into()));
        Vec::new()
    });
    // NOTE: the SETs above are answered `+QUEUED`; `assert_no_inline_with`
    // does not inspect the replies, only the dispatch counter, which is
    // exactly the claim under test.
}

#[test]
fn g4_client_tracking_still_suppresses_inline() {
    // The inline path never registers an invalidation, so a tracking client's
    // cache would never be told its key changed.
    // Enabled on THIS connection (covers `conn.tracking_state.enabled`) after a
    // RESP3 handshake, which is what lets `CLIENT TRACKING ON` work without a
    // REDIRECT target.
    assert_no_inline_with("tracking", |_port, c| {
        match c.cmd(&[b"HELLO", b"3"]) {
            V::Arr(_) => {}
            other => panic!("HELLO 3 should answer a map, got {other:?}"),
        }
        assert_eq!(
            c.cmd(&[b"CLIENT", b"TRACKING", b"ON"]),
            V::Simple("OK".into()),
            "CLIENT TRACKING ON should succeed on a RESP3 connection"
        );
        Vec::new()
    });
}

#[test]
fn g4_monitor_still_suppresses_inline() {
    // The inline path answers from the read buffer and never sees `peer_addr`,
    // so it cannot format a MONITOR feed line. The suppression is what makes
    // the feed correct by construction.
    //
    // MONITOR is attached on a SEPARATE connection on purpose: the gate term
    // is `crate::monitor::any_attached()`, process-global, not per-connection.
    assert_no_inline_with("monitor", |port, _c| {
        let mut mon = Client::connect(port);
        assert_eq!(
            mon.cmd(&[b"MONITOR"]),
            V::Simple("OK".into()),
            "MONITOR should be accepted"
        );
        // Returned, not dropped: `monitor::any_attached()` is false again the
        // moment this connection closes, and the assertion below would then
        // be testing nothing.
        vec![mon]
    });
}

#[test]
fn g4_restricted_acl_still_suppresses_inline() {
    // Ungated, the inline path is an ACL bypass: a `-@all` user writes any key
    // by name. See tests/acl_inline_read_enforcement.rs for the read half.
    assert_no_inline_with("acl", |_port, c| {
        assert_eq!(
            c.cmd(&[
                b"ACL",
                b"SETUSER",
                b"limited",
                b"on",
                b">pw",
                b"~limited:*",
                b"+@all",
            ]),
            V::Simple("OK".into()),
            "ACL SETUSER should succeed"
        );
        assert_eq!(
            c.cmd(&[b"AUTH", b"limited", b"pw"]),
            V::Simple("OK".into()),
            "AUTH as the restricted user should succeed"
        );
        Vec::new()
    });
}

// ===========================================================================
// GROUP 5 — THE TWO TERMS THIS CHANGE MADE REACHABLE FOR THE FIRST TIME.
// ===========================================================================
//
// `can_inline_writes` is a conjunction, and before moon#660 one of its terms
// (`ctx.spill_sender.is_none()`) was FALSE in the shipped default, because
// `--disk-offload` defaults to `enable`. The conjunction was therefore false
// out of the box and the inline write path never ran at all — which means
// every OTHER term in it, including `!is_replica` and `!fanout_hint_active()`,
// was effectively dead code in the default configuration. Reaching them
// required an operator to pass `--disk-offload disable` explicitly.
//
// This change makes the inline write path reachable by default. That does not
// alter those two terms, but it promotes them from unreachable to
// load-bearing, so they need coverage that never previously existed.
//
// This is not hypothetical. The task #34 comment immediately above
// `can_inline_writes` records that the fan-out gap already produced silent
// data loss once, in exactly the configuration that could reach it:
//
//   "Before this gate, a master with `--disk-offload disable` and an attached
//    replica silently dropped every plain `SET` from the replication stream
//    (verified: a lone `SET foo bar` never reached the replica)."
//
// Acked write, absent data — the same class as G3's resurrection. After this
// change the DEFAULT configuration reaches that code, so both terms are
// pinned here.

/// Poll `f` until it returns true or `timeout` elapses.
fn wait_until<F: FnMut() -> bool>(timeout: Duration, mut f: F) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if f() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn info_section(c: &mut Client, section: &str) -> String {
    match c.cmd(&[b"INFO", section.as_bytes()]) {
        V::Bulk(b) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("INFO {section} did not answer a bulk string: {other:?}"),
    }
}

/// A master and a replica, each with its own admin port.
struct Pair {
    _mdir: tempfile::TempDir,
    _rdir: tempfile::TempDir,
    _mguard: ServerGuard,
    _rguard: ServerGuard,
    m_port: u16,
    m_admin: u16,
    r_port: u16,
    r_admin: u16,
}

/// Spawn master + replica, both with disk offload at its DEFAULT (`enable`) —
/// the whole point is the configuration that ships, not `disable`, which is
/// the only one that could reach these terms before moon#660.
///
/// Attaching is deferred to the caller so a CONTROL measurement can be taken
/// on a server that is not yet a replica / not yet a fan-out master.
fn spawn_pair() -> Pair {
    let mdir = test_tmpdir();
    let rdir = test_tmpdir();
    let m_admin = common::reserve_port();
    let r_admin = common::reserve_port();
    // `appendonly no`: GROUP 5 is about replication fan-out, not the eviction
    // routing divergence, so the AOF is dead weight here. `maxmemory 0` keeps
    // the eviction pre-gate permanently satisfied, so the ONLY thing that can
    // suppress inlining in these tests is the gate term under test.
    let (mguard, m_port) = spawn_moon(
        mdir.path(),
        &Cfg {
            shards: 1,
            appendonly: "no",
            maxmemory: 0,
            admin_port: m_admin,
        },
    );
    let (rguard, r_port) = spawn_moon(
        rdir.path(),
        &Cfg {
            shards: 1,
            appendonly: "no",
            maxmemory: 0,
            admin_port: r_admin,
        },
    );
    Pair {
        _mdir: mdir,
        _rdir: rdir,
        _mguard: mguard,
        _rguard: rguard,
        m_port,
        m_admin,
        r_port,
        r_admin,
    }
}

fn attach_replica(p: &Pair) {
    let mut r = Client::connect(p.r_port);
    assert_eq!(
        r.cmd(&[b"REPLICAOF", b"127.0.0.1", p.m_port.to_string().as_bytes()]),
        V::Simple("OK".into()),
        "REPLICAOF should be accepted"
    );
    assert!(
        wait_until(Duration::from_secs(20), || {
            info_section(&mut r, "replication").contains("master_link_status:up")
        }),
        "replica link never came up"
    );
}

/// Plain SETs per measurement window. Small: the assertion is on a counter
/// delta, not on throughput.
const G5_WRITES: usize = 50;

/// Term: `!crate::replication::state::fanout_hint_active()`.
///
/// Reddening mutation: delete that term from `can_inline_writes` in
/// `src/server/conn/handler_monoio/mod.rs`. The counter then climbs by
/// `G5_WRITES` after the replica attaches, and the delivery assertion fails
/// with the keys missing on the replica — the exact task #34 symptom.
///
/// The CONTROL block is also the positive demonstration of moon#660 itself:
/// on an unmodified default-configuration master, plain `SET` inlines. Before
/// this change that assertion would have failed, because `--disk-offload
/// enable` (the default) held `can_inline_writes` false.
#[test]
fn g5_attached_replica_suppresses_inline_and_writes_still_replicate() {
    let p = spawn_pair();
    let mut m = Client::connect(p.m_port);

    // CONTROL — no replica yet, so `fanout_hint_active()` is false.
    {
        let before = local_inline_count(p.m_admin);
        for i in 0..G5_WRITES {
            assert_eq!(
                m.cmd(&[b"SET", format!("ctl:{i}").as_bytes(), b"v"]),
                V::Simple("OK".into())
            );
        }
        let after = local_inline_count(p.m_admin);
        assert!(
            after > before,
            "CONTROL failed — plain SET did not inline on a default-configuration \
             master with NO replica attached ({before} -> {after}). Either moon#660 \
             regressed or this binary predates it; the suppression assertion below \
             would be vacuous."
        );
    }

    attach_replica(&p);

    // The sticky hint is now set for the life of the process (never cleared
    // once true), so the assertions are ordered around that rather than
    // expecting a detach to restore inlining.
    let before = local_inline_count(p.m_admin);
    for i in 0..G5_WRITES {
        assert_eq!(
            m.cmd(&[
                b"SET",
                format!("rep:{i}").as_bytes(),
                format!("v{i}").as_bytes()
            ]),
            V::Simple("OK".into())
        );
    }
    let after = local_inline_count(p.m_admin);

    // CONSEQUENCE FIRST, mechanism second. The counter delta below is the
    // cheaper and more specific signal, but it is not the thing that matters:
    // what matters is that the write ARRIVES. Asserting the data first means
    // a regression reports itself as "the replica never got these keys"
    // rather than as a counter number the reader has to interpret — and it
    // proves the term prevents data loss, not merely that it changes a
    // dispatch path. (Verified: with the fan-out term deleted, this is the
    // assertion that fires.)
    let mut r = Client::connect(p.r_port);
    assert!(
        wait_until(Duration::from_secs(20), || {
            (0..G5_WRITES).all(|i| {
                r.cmd(&[b"GET", format!("rep:{i}").as_bytes()])
                    == V::Bulk(format!("v{i}").into_bytes())
            })
        }),
        "replica did not receive every plain SET issued after it attached — \
         these writes were acked to the client and then dropped from the \
         replication stream (task #34's silent data loss)"
    );

    assert_eq!(
        after,
        before,
        "{} writes were inlined AFTER a replica attached. The inline path does not \
         feed the replication backlog, so each one is a write the replica will never \
         see (task #34).",
        after - before
    );
}

/// Term: `!is_replica` (the lock-free `ctx.is_replica_mirror` load).
///
/// Reddening mutation: delete `&& !is_replica` from `can_inline_writes`. A
/// client `SET` against the read-only replica then answers `+OK` and the write
/// LANDS, diverging the replica from its master.
///
/// This is the sharpest of the six terms, because `try_inline_dispatch` has NO
/// read-only guard of its own — grepping READONLY in
/// `src/server/conn/blocking.rs` returns nothing. The `-READONLY` error is
/// produced exclusively by generic dispatch (`handler_monoio/dispatch.rs:944`),
/// so `!is_replica` is not one of several defences; it is the only one.
#[test]
fn g5_replica_refuses_client_writes_and_never_inlines() {
    let p = spawn_pair();

    // CONTROL — before REPLICAOF this server is an ordinary master, so plain
    // SET must inline on it. Proves the counter is live on THIS process.
    {
        let mut pre = Client::connect(p.r_port);
        let before = local_inline_count(p.r_admin);
        for i in 0..G5_WRITES {
            assert_eq!(
                pre.cmd(&[b"SET", format!("ctl:{i}").as_bytes(), b"v"]),
                V::Simple("OK".into())
            );
        }
        let after = local_inline_count(p.r_admin);
        assert!(
            after > before,
            "CONTROL failed — plain SET did not inline on this server while it was \
             still a master ({before} -> {after}); the assertion below would be vacuous."
        );
    }

    attach_replica(&p);

    // Fresh connection: the full resync flushed the keyspace, and a client
    // that started life talking to a replica is the realistic shape.
    let mut c = Client::connect(p.r_port);
    let before = local_inline_count(p.r_admin);
    for i in 0..G5_WRITES {
        match c.cmd(&[b"SET", format!("ro:{i}").as_bytes(), b"v"]) {
            V::Err(msg) if msg.starts_with("READONLY") => {}
            other => panic!(
                "SET ro:{i} against a read-only replica answered {other:?}, expected a \
                 -READONLY error. The inline path has no read-only guard, so `+OK` here \
                 means the write LANDED and the replica has silently diverged."
            ),
        }
    }
    let after = local_inline_count(p.r_admin);
    assert_eq!(
        after,
        before,
        "{} client writes were inlined on a REPLICA. `try_inline_dispatch` never \
         consults the read-only gate, so each one bypassed it.",
        after - before
    );

    // Belt and braces: nothing landed.
    for i in 0..G5_WRITES {
        assert_eq!(
            c.cmd(&[b"GET", format!("ro:{i}").as_bytes()]),
            V::Null,
            "ro:{i} exists on the replica — a client write was applied despite the \
             -READONLY reply"
        );
    }
}

// ---------------------------------------------------------------------------
// G6 — the cross-store transaction term (`!conn.in_cross_txn()`)
// ---------------------------------------------------------------------------
//
// Eviction routing was NOT the only divergence widening this gate exposed.
// Inside an open `TXN` the GENERIC write leg captures an undo record
// (`txn.kv_undo.record_insert` / `record_update`) and a write intent
// (`s.kv_write_intents.record_write`) before dispatching.
// `try_inline_dispatch` does neither — grepping `cross_txn`, `kv_undo` and
// `write_intent` in `src/server/conn/blocking.rs` returns nothing.
//
// Without the undo record `TXN ABORT` restores nothing
// (`transaction/abort.rs` replays `UndoRecord::Update`); without the write
// intent the MVCC snapshot-visibility filter cannot hide the uncommitted
// value from a foreign transaction's reads.
//
// This is INTRODUCED by moon#660, not merely exposed by it: before the change
// `--disk-offload enable` held `can_inline_writes` false in the shipped
// default, so the in-TXN SET went generic and the term was never needed.
//
// Reddening mutation (applied, observed, reverted): delete
// `&& !conn.in_cross_txn()` from `can_inline_writes`. Measured on the release
// binary, `--shards 1`, stock config, ONE connection:
//
//     command          fixed        mutated
//     SET k original   +OK          +OK
//     TXN BEGIN        +OK          +OK
//     SET k modified   +OK          +OK   (local_inline +1: INLINED)
//     TXN ABORT        +OK          +OK   (undo log empty)
//     GET k            "original"   "modified"   <-- rollback silently lost
//
// Both tests below flip on that mutation: the counter test on the inline
// count, the contract test on the value `GET` answers.

#[test]
fn g6_cross_txn_still_suppresses_inline() {
    assert_no_inline_with("cross_txn", |_port, c| {
        assert_eq!(
            c.cmd(&[b"TXN", b"BEGIN"]),
            V::Simple("OK".into()),
            "TXN BEGIN should open a cross-store transaction"
        );
        Vec::new()
    });
}

#[test]
fn g6_inline_write_inside_txn_is_rolled_back_by_abort() {
    let dir = test_tmpdir();
    let admin_port = common::reserve_port();
    let cfg = Cfg {
        shards: 1,
        appendonly: "yes",
        maxmemory: 0,
        admin_port,
    };
    let (_guard, port) = spawn_moon(dir.path(), &cfg);

    // ONE connection throughout: `active_cross_txn` is per-connection state,
    // and a fresh connection per command would leave the TXN on a dead socket
    // and silently test nothing.
    let mut c = Client::connect(port);

    assert_eq!(c.cmd(&[b"SET", b"k", b"original"]), V::Simple("OK".into()));

    // CONTROL: a plain SET on this connection, OUTSIDE the transaction, must
    // inline on this server in this configuration. Without this the rollback
    // assertion below could pass merely because nothing ever inlines.
    let ctl_before = local_inline_count(admin_port);
    c.cmd(&[b"SET", b"ctl", b"v"]);
    let ctl_after = local_inline_count(admin_port);
    assert!(
        ctl_after > ctl_before,
        "CONTROL failed — a plain SET outside the transaction did not inline \
         ({ctl_before} -> {ctl_after}); the assertions below would be vacuous"
    );

    assert_eq!(c.cmd(&[b"TXN", b"BEGIN"]), V::Simple("OK".into()));

    let before = local_inline_count(admin_port);
    assert_eq!(c.cmd(&[b"SET", b"k", b"modified"]), V::Simple("OK".into()));
    let after = local_inline_count(admin_port);
    assert_eq!(
        after, before,
        "a SET inside an open TXN was INLINED ({before} -> {after}), so no undo \
         record was captured and TXN ABORT cannot roll it back"
    );

    assert_eq!(c.cmd(&[b"TXN", b"ABORT"]), V::Simple("OK".into()));

    // The contract a user can see, independent of any counter.
    assert_eq!(
        c.cmd(&[b"GET", b"k"]),
        V::Bulk(b"original".to_vec()),
        "TXN ABORT did not restore the pre-transaction value — the in-TXN SET \
         bypassed undo capture via the inline write path"
    );
}
