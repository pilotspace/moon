//! moon#660 step 2: the hot/cold/WAL reconciliation invariant, as a PROPERTY.
//!
//! ## Why this file exists
//!
//! Disk offload is a two-source-of-truth durability path. Spilled segments are
//! independently self-durable and recover on their own, so the hazard is not a
//! double-write conflict with the WAL — it is RECONCILIATION. Recovery runs
//! Phase 3 (rebuild `cold_index` from the manifest) and then Phase 4 (WAL
//! replay on top, hot shadowing cold), and every bug found in that seam so far
//! has been silent-data-loss class:
//!
//!   * DEL/FLUSH resurrection + expired-cold leak (#212)
//!   * BITOP/COPY/DEL/UNLINK resurrection (#213)
//!   * a spill completion resurrecting a DEL'd key (#459)
//!
//! Every one of those was found by soak or adversarial review, and every one
//! was then pinned by an EXAMPLE — a hand-written sequence reproducing that
//! specific bug. None of them was found by, or is protected by, a proof that
//! the invariant holds in general. #660 records that gap as the one piece of
//! work worth doing regardless of what happens to the `--disk-offload`
//! default. This is that piece.
//!
//! ## The invariant
//!
//! For a keyspace driven by an arbitrary sequence of writes, deletes and
//! expiries, with the cold tier live and under enough memory pressure to
//! actually tier keys:
//!
//! > the server's answer for every key equals the MODEL's answer — both while
//! > running, and again after a `SIGKILL` and a full Phase-3/Phase-4 recovery.
//!
//! Three failure shapes fall out of that one statement, and they are named
//! individually in the assertions because they are the three that have
//! actually shipped:
//!
//!   * **resurrection** — a deleted key answers a value (a cold copy outlived
//!     its delete, or a spill completion landed after it).
//!   * **expired-cold leak** — a key whose TTL passed answers a value.
//!   * **lost write** — a live key answers nil, or an older value.
//!
//! ## Why hand-rolled generation and not proptest
//!
//! The tree has no `proptest` dependency and this does not add one: a
//! durability default is not the place to also widen the supply chain. The
//! generator here is a seeded xorshift, the seed is printed on every failure,
//! and `MOON_660_SEEDS` re-runs any seed on its own — which is the part of
//! proptest that matters here (reproducibility), without the part that does
//! not (shrinking, which a 40-op sequence barely needs).
//!
//! Run with:
//!   cargo build --release
//!   MOON_BIN=$PWD/target/release/moon cargo test --release \
//!     --test cold_reconciliation_property_660
//!
//! ## moon#965 — what the instrument in this file is for
//!
//! This test is red on hosted ubuntu and Windows CI and green on macOS-latest
//! and the self-hosted arm64 VM. The runtime is the SAME (tokio) on a failing
//! and a passing leg, so the discriminator is not the runtime — the hosted
//! runners have slow network-backed storage and the two green hosts have local
//! NVMe. Every red run so far was RETRIED on a fresh temp dir, destroying the
//! AOF, the manifest and the server log that would have diagnosed it.
//!
//! Two hypotheses had to be told apart, and the pre-#965 `check()` — which
//! panicked on the FIRST divergent key — could not:
//!
//!   * **TAIL LOSS** — every write the server still has was issued BEFORE every
//!     write it lost. One cut. That is a truncated append log.
//!   * **COLD PLANE** — one key stale while OTHER keys correctly hold LATER
//!     writes. No cut exists. That is the Phase-3/Phase-4 shadowing seam.
//!
//! `check()` now collects every divergence and names the verdict. Three things
//! were measured while building it, recorded here because each cost time:
//!
//! **1. "29 versions stale" is false.** moon#965 reports `prop:key:004`
//! answering `v4-7` where `v4-36` was expected and reads 36 - 7 as a version
//! gap. It is a gap in SEQUENCE INDICES. Key 4 is touched exactly three times
//! in seed 2 — step 3 (a `COPY` that no-ops on an absent source), step 7 and
//! step 36 — so `v4-7` is the IMMEDIATELY PRECEDING write, ONE behind. The
//! report prints both numbers side by side (`dSTEPS` vs `dWRITES`) so the
//! mistake cannot be made again.
//!
//! **2. H1 (acked appends dropped at enqueue) is REFUTED for this workload.**
//! `AofWriterPool::try_send_append` is fire-and-forget and drops on a full
//! channel after the client has been acked — but it has NO production call
//! site: every caller outside `aof/pool.rs` is a unit test. The real write
//! paths are `send_append_bounded_blocking` and `try_send_append_durable`,
//! which block and then fail LOUD. Measured with `MOON_TEST_AOF_CHANNEL_CAP=7`
//! (see below): 20,000 pipelined 1 KiB `SET`s all answered `+OK`, the AOF grew
//! to 21,588,162 bytes — every record landed — and `aof_backpressure_dropped`
//! stayed 0. Throughput collapsed to ~2.2k ops/s, which is the shape of
//! backpressure, not of dropping.
//!
//! **3. The one line that decides the question was being thrown away.**
//! `tracing_subscriber::fmt()` in `src/main.rs` writes to **stdout** at
//! `moon=info`, and this harness spawned with `Stdio::null()` for stdout. So
//! `AOF incr truncated tail: N bytes at offset O` — plus every AOF-drop warning
//! and the moon#875 cold-recovery lines — was discarded on every run this file
//! has ever made. `server_stdout` now captures it to `server.out`. Note the
//! needle: with an `appendonlydir/` manifest the live replay is
//! `persistence::aof_manifest::shard_replay` and it says `AOF incr truncated
//! tail`; `AOF truncated` in `persistence::aof` belongs to the legacy
//! single-file path and never fires here.
//!
//! ### Reddening mutation (applied, observed, reverted)
//!
//! Truncate the biggest `*.aof` to 88% of its length immediately before
//! `crash_and_restart()` — a simulated lost tail with a known answer. Seed 2,
//! macOS arm64, release:
//!
//! ```text
//!   KEY              KIND            WANT STEP   GOT STEP   dSTEPS  dWRITES
//!   prop:key:004     STALE VALUE            36          7       29        1
//!   prop:key:000     STALE VALUE            38         35        3        2
//!   prop:key:009     LOST WRITE             39        nil        -        -
//!
//!   latest step the server got RIGHT (s_kept): 34
//!   earliest step the server got WRONG (s_lost): 36
//!   VERDICT: TAIL LOSS ... a single cut between step 34 and step 36
//!   AOF bytes before SIGKILL: 10755890   after restart: 9465183
//!   WARN shard_replay: AOF incr truncated tail: 1031 bytes at offset 9464152
//! ```
//!
//! That reproduces moon#965's reported signature EXACTLY — same key, same
//! expected value, same observed value, same 29 — from a log that is known to
//! have lost its tail and from nothing else. It does not prove #965 is tail
//! loss; it proves the instrument would say so if it is, and would say
//! COLD PLANE if it is not.
//!
//! ### Knobs
//!
//! * `MOON_TEST_AOF_CHANNEL_CAP=N` — cap the AOF writer channel (production
//!   10,000 when unset, in every profile). Forces the backpressure path.
//!   With it set, the LIVE assertion INVERTS and demands a non-zero drop
//!   counter: a provoked run that drops nothing has tested nothing.
//! * `MOON_660_SETTLE_MS=N` — the pre-SIGKILL drain window (default 3000).
//!   0 removes it. Green on this host at 0, which bounds but does not refute
//!   the undrained-channel hypothesis on a slow disk.
//! * `MOON_660_EVIDENCE_DIR=<path>` — on failure, copy the server `--dir` and
//!   the divergence report there. CI uploads it as an artifact.

#![allow(clippy::unwrap_used)]

mod common;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, find_moon_binary, wait_for_port_down};

/// Small enough that the filler below reliably crosses it, large enough that
/// the server is not answering `-OOM` to the operations under test.
const MAXMEMORY_BYTES: usize = 8 * 1024 * 1024;
/// Value size for keys under test — comfortably past `CompactValue`'s 12-byte
/// inline limit, so every one of them is a heap value that can actually tier.
const VALUE_LEN: usize = 256;
/// Filler written between operations to create the memory pressure that moves
/// keys to the cold tier. Without pressure nothing spills and the whole file
/// is vacuous, which the `spilled_keys` assertion at the end refuses to allow.
///
/// Sized by measurement, not by guess. The first version wrote 120 x 1 KiB per
/// step — about 4.9 MiB across the whole sequence, comfortably UNDER the 8 MiB
/// cap — so nothing ever tiered and the non-vacuity assertion refused the run
/// (`spilled_keys summed to 0`). 400 x 1 KiB then put ~16 MiB through an 8 MiB
/// budget and the server started answering `-OOM` to the operations under
/// test: bytes handed to the spill thread stay counted as resident until their
/// completions land (moon#466), so `allkeys-lru` runs out of headroom faster
/// than the arithmetic suggests. 250 x 1 KiB (~10 MiB through 8 MiB) crosses
/// the budget without saturating the spill thread — and `apply_write` below
/// makes the test correct under `-OOM` anyway, so this is a tuning knob for
/// COVERAGE, never for correctness.
const FILLER_PER_STEP: usize = 250;
const FILLER_VALUE_LEN: usize = 1024;
/// Operations per sequence. Long enough for keys to be written, tiered,
/// deleted and re-written several times over.
const OPS: usize = 40;
/// Distinct keys the sequence draws from. Deliberately small relative to
/// `OPS`, so the same key is repeatedly overwritten, deleted and resurrected —
/// that collision is where the seam bugs live.
const KEYSPACE: usize = 12;
/// TTL applied by `Op::SetVolatile`, in milliseconds. Long enough to survive
/// being written and tiered, short enough that the sequence outruns it.
const VOLATILE_TTL_MS: u64 = 300;

// ===========================================================================
// The model.
// ===========================================================================

/// What the keyspace SHOULD contain. Deliberately the dumbest possible
/// structure: if the model needed to know about tiers, spills or replay order
/// to stay correct, it would be re-implementing the thing under test and would
/// agree with it for the same wrong reasons.
#[derive(Default)]
struct Model {
    live: HashMap<String, String>,
    /// Keys given a TTL, with the instant they expire. Kept separately because
    /// "is it gone yet" is a question about wall-clock, resolved once at the
    /// point of assertion rather than guessed at during generation.
    volatile: HashMap<String, Instant>,
    /// Every key the sequence has ever touched — the assertion sweep covers
    /// these, not just the survivors, because a resurrection is by definition
    /// a key the model believes is GONE.
    seen: Vec<String>,
    /// Step index of the most recent operation that CHANGED this key's state.
    ///
    /// moon#965 needs this and nothing else can supply it. A divergence report
    /// that only names the key cannot say whether the keys the server got
    /// RIGHT were all written before the keys it got WRONG — which is the one
    /// question that separates a truncated log from a cold-plane shadow.
    last_change_step: HashMap<String, usize>,
    /// Every state-changing op per key, in order: `(step, Some(value))` for a
    /// write, `(step, None)` for a delete or a flush.
    ///
    /// This is what makes "N versions stale" a MEASURED number rather than a
    /// guess. moon#965 was filed claiming `prop:key:004` came back "29
    /// versions stale" because `v4-7` was observed where `v4-36` was expected
    /// — but 36 - 7 is a difference of STEP INDICES, and steps 8..35 never
    /// touched key 4. The history below reports the position difference (ONE
    /// write behind), which points at a truncated log, not at shadowing.
    history: HashMap<String, Vec<(usize, Option<String>)>>,
}

impl Model {
    fn touch(&mut self, k: &str) {
        if !self.seen.iter().any(|s| s == k) {
            self.seen.push(k.to_string());
        }
    }

    /// Record a state change for the divergence report. Called by every
    /// mutating method below, never by the assertion path.
    fn record(&mut self, k: &str, step: usize, v: Option<&str>) {
        self.last_change_step.insert(k.to_string(), step);
        self.history
            .entry(k.to_string())
            .or_default()
            .push((step, v.map(str::to_string)));
    }

    /// How many of this key's OWN state changes separate `from_step` from
    /// `to_step` — the honest answer to "how stale is this value".
    fn writes_between(&self, k: &str, from_step: usize, to_step: usize) -> Option<usize> {
        let h = self.history.get(k)?;
        let a = h.iter().position(|(s, _)| *s == from_step)?;
        let b = h.iter().position(|(s, _)| *s == to_step)?;
        Some(b.abs_diff(a))
    }

    fn set(&mut self, k: &str, v: &str, step: usize) {
        self.touch(k);
        self.live.insert(k.to_string(), v.to_string());
        // A plain SET clears any TTL — the `#553` shape, and the reason
        // `SetVolatile` then `Set` must not leave the key expiring.
        self.volatile.remove(k);
        self.record(k, step, Some(v));
    }

    fn set_volatile(&mut self, k: &str, v: &str, ttl: Duration, step: usize) {
        self.touch(k);
        self.live.insert(k.to_string(), v.to_string());
        self.volatile.insert(k.to_string(), Instant::now() + ttl);
        self.record(k, step, Some(v));
    }

    fn del(&mut self, k: &str, step: usize) {
        self.touch(k);
        self.live.remove(k);
        self.volatile.remove(k);
        self.record(k, step, None);
    }

    fn copy(&mut self, src: &str, dst: &str, step: usize) -> bool {
        self.touch(src);
        self.touch(dst);
        // Redis `COPY` without REPLACE refuses when the destination exists.
        if self.get(dst).is_some() {
            return false;
        }
        match self.get(src) {
            Some(v) => {
                self.live.insert(dst.to_string(), v.clone());
                // COPY carries the TTL across; the sequence only ever copies
                // to a fresh destination, so mirroring it is enough.
                if let Some(&at) = self.volatile.get(src) {
                    self.volatile.insert(dst.to_string(), at);
                }
                self.record(dst, step, Some(&v));
                true
            }
            None => false,
        }
    }

    fn flush(&mut self, step: usize) {
        let gone: Vec<String> = self.live.keys().cloned().collect();
        self.live.clear();
        self.volatile.clear();
        for k in gone {
            self.record(&k, step, None);
        }
    }

    /// The model's answer for a key, resolved against the clock NOW. A key
    /// whose TTL has passed reads as absent even though the entry is still in
    /// `live` — which is exactly the "expired-cold leak" question.
    fn get(&self, k: &str) -> Option<String> {
        let v = self.live.get(k)?;
        if let Some(&at) = self.volatile.get(k)
            && Instant::now() >= at
        {
            return None;
        }
        Some(v.clone())
    }

    /// True when the key's expiry is close enough that the model and the
    /// server could legitimately disagree about which side of it they are on.
    /// Such a key is SKIPPED rather than asserted, because a race between the
    /// model's clock and the server's is not the property under test.
    fn near_expiry(&self, k: &str) -> bool {
        match self.volatile.get(k) {
            Some(&at) => {
                let now = Instant::now();
                let margin = Duration::from_millis(250);
                at > now - margin && at < now + margin
            }
            None => false,
        }
    }
}

// ===========================================================================
// Generation.
// ===========================================================================

/// xorshift64*. Not cryptographic and not trying to be — it needs to be
/// reproducible from a seed and spread over a small op space, nothing more.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % (n as u64)) as usize
    }
}

#[derive(Debug)]
enum Op {
    Set(usize),
    SetVolatile(usize),
    Del(usize),
    Unlink(usize),
    Copy(usize, usize),
    FlushDb,
}

fn gen_op(rng: &mut Rng) -> Op {
    // Weighted so writes dominate and FLUSHDB stays rare — a sequence that
    // flushes every few steps never accumulates enough cold state to be
    // interesting.
    match rng.below(100) {
        0..=39 => Op::Set(rng.below(KEYSPACE)),
        40..=54 => Op::SetVolatile(rng.below(KEYSPACE)),
        55..=74 => Op::Del(rng.below(KEYSPACE)),
        75..=87 => Op::Unlink(rng.below(KEYSPACE)),
        88..=97 => Op::Copy(rng.below(KEYSPACE), rng.below(KEYSPACE)),
        _ => Op::FlushDb,
    }
}

// ===========================================================================
// Server harness.
// ===========================================================================

struct Server {
    guard: ServerGuard,
    port: u16,
    dir: std::path::PathBuf,
}

fn moon_args(dir: &std::path::Path, port: u16) -> Vec<String> {
    vec![
        "--port".into(),
        port.to_string(),
        "--dir".into(),
        dir.to_string_lossy().into_owned(),
        "--shards".into(),
        "1".into(),
        // The tier under test, explicitly ON — this file must keep testing it
        // after #660 made it opt-in.
        "--disk-offload".into(),
        "enable".into(),
        "--disk-offload-dir".into(),
        dir.join("off").to_string_lossy().into_owned(),
        // The durability backstop. Without it `disk_offload_spill_inert`
        // holds, victims are DROPPED rather than spilled, and no cold state
        // ever exists to reconcile.
        "--appendonly".into(),
        "yes".into(),
        "--appendfsync".into(),
        "everysec".into(),
        "--maxmemory".into(),
        MAXMEMORY_BYTES.to_string(),
        "--maxmemory-policy".into(),
        "allkeys-lru".into(),
        // Under test is reconciliation, not the disk guard; a near-full dev
        // volume would otherwise shadow every write with `MOONERR diskfull`.
        "--disk-free-min-pct".into(),
        "0".into(),
        "--protected-mode".into(),
        "no".into(),
    ]
}

/// stdout sink for a spawned `moon` (moon#965).
///
/// This is NOT cosmetic. `tracing_subscriber::fmt()` in `src/main.rs` writes to
/// **stdout**, and the default filter is `moon=info` — so every `warn!` the
/// server emits lands there, including the three lines that decide this
/// investigation:
///
///   * `AOF truncated: N unparseable bytes at offset O (end of file)`
///   * `AOF append dropped for shard S (lsn L): channel full`
///   * `cold recovery: spill file … (moon#875)`
///
/// This harness spawned with `Stdio::null()` for stdout, so all three were
/// discarded on every run this test has ever made, red or green. Measured
/// while building this instrument: a deliberately truncated AOF produced a
/// textbook TAIL LOSS verdict and `server.err said NOTHING about a truncated
/// or corrupt log` — because the server HAD said it, into `/dev/null`.
fn server_stdout(dir: &std::path::Path) -> std::process::Stdio {
    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("server.out"))
    {
        Ok(f) => std::process::Stdio::from(f),
        Err(_) => std::process::Stdio::null(),
    }
}

fn spawn(dir: &std::path::Path) -> Server {
    std::fs::create_dir_all(dir.join("off")).expect("create offload dir");
    let (guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(find_moon_binary())
            .args(moon_args(dir, port))
            .stdout(server_stdout(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (run `cargo build --release` first)")
    });
    assert!(
        serving(port),
        "moon never answered PING on port {port} after start-up"
    );
    Server {
        guard,
        port,
        dir: dir.to_path_buf(),
    }
}

/// Poll until a real `+PONG` comes back — NOT merely until `connect` succeeds.
/// moon's client listeners use `SO_REUSEPORT`, so accepting proves neither
/// that the server is serving nor even that the peer is the process just
/// spawned.
fn serving(port: u16) -> bool {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(mut s) = TcpStream::connect_timeout(
            &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
            Duration::from_millis(200),
        ) {
            let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
            let mut buf = [0u8; 7];
            if s.write_all(b"PING\r\n").is_ok()
                && s.read_exact(&mut buf).is_ok()
                && buf.starts_with(b"+PONG")
            {
                return true;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

impl Server {
    /// `SIGKILL` and restart on the same `--dir`, which is what drives Phase 3
    /// (cold_index rebuild from the manifest) and Phase 4 (WAL replay on top).
    /// A graceful shutdown would let the server tidy up and would not exercise
    /// the seam at all.
    fn crash_and_restart(self) -> Server {
        let Server {
            mut guard,
            port,
            dir,
        } = self;
        // `kill_now` reaps, which a same-dir restart requires: the new server
        // must be able to take the dir lock the corpse would otherwise hold.
        guard.kill_now();
        drop(guard);
        wait_for_port_down(port);

        let child = Command::new(find_moon_binary())
            .args(moon_args(&dir, port))
            .stdout(server_stdout(&dir))
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("restart moon");
        let guard = ServerGuard::new(child);
        assert!(
            serving(port),
            "restarted moon on port {port} never answered PING; every \
             assertion below would be measuring a server that is not up"
        );
        Server { guard, port, dir }
    }
}

// ===========================================================================
// RESP helpers.
// ===========================================================================

/// `Some(value)` for a bulk reply, `None` for a null. Anything else is a bug
/// in the harness or an error reply, and must not be silently read as "absent"
/// — that would turn every server error into a passing "key is gone".
fn parse_get(raw: &str) -> Option<String> {
    if raw.starts_with("$-1") || raw.starts_with("_\r\n") {
        return None;
    }
    assert!(
        raw.starts_with('$'),
        "GET answered neither a bulk string nor a null: {raw:?}"
    );
    let body = raw.split_once("\r\n").map(|x| x.1).expect("bulk body");
    Some(body.trim_end_matches("\r\n").to_string())
}

/// Did the server ACCEPT the command, i.e. may the model apply it?
///
/// Under real memory pressure `-OOM` and the AOF writer's fail-loud
/// backpressure error are legitimate answers, and both mean the write did NOT
/// happen. A model that applied the op regardless would then diverge from a
/// server that is behaving perfectly, and the test would report a LOST WRITE
/// against its own bookkeeping.
///
/// This is what makes the file a property test rather than a tuned fixture:
/// correctness no longer depends on choosing a filler size that never trips
/// the cap. Any OTHER error is still a hard failure — swallowing them would
/// turn every genuine server error into a silent "the model skipped that one".
fn accepted(reply: &str, what: &str) -> bool {
    if reply.starts_with('-') {
        assert!(
            reply.contains("OOM") || reply.contains("backpressure"),
            "{what} answered an unexpected error: {reply:?}"
        );
        return false;
    }
    true
}

/// The AOF-writer channel cap this run deliberately forced on the server, if
/// any (moon#965). `None` is every ordinary run, local or CI.
fn forced_channel_cap() -> Option<String> {
    std::env::var("MOON_TEST_AOF_CHANNEL_CAP")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn info_field(info: &str, field: &str) -> u64 {
    info.lines()
        .find_map(|l| l.strip_prefix(field).and_then(|r| r.strip_prefix(':')))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0)
}

// ===========================================================================
// The property.
// ===========================================================================

fn value_for(key_idx: usize, step: usize) -> String {
    // Distinct per (key, step) so a stale value is distinguishable from the
    // current one — a test whose values all look alike cannot tell a lost
    // write from a resurrected older copy.
    let head = format!("v{key_idx}-{step}-");
    let mut s = String::with_capacity(VALUE_LEN);
    s.push_str(&head);
    while s.len() < VALUE_LEN {
        s.push('x');
    }
    s
}

fn key_name(i: usize) -> String {
    format!("prop:key:{i:03}")
}

/// Drive one seeded sequence, then assert the invariant live and again across
/// a crash. Returns the number of keys that actually tiered, so the caller can
/// refuse a vacuous pass.
fn run_sequence(seed: u64) -> u64 {
    let dir = common::unique_test_dir(&format!("cold-recon-660-{seed}"));
    let server = spawn(&dir);
    let mut c = Conn::open(server.port);
    let mut model = Model::default();
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1));

    for step in 0..OPS {
        match gen_op(&mut rng) {
            Op::Set(k) => {
                let (key, val) = (key_name(k), value_for(k, step));
                if accepted(&c.send(&["SET", &key, &val]), "SET") {
                    model.set(&key, &val, step);
                }
            }
            Op::SetVolatile(k) => {
                let (key, val) = (key_name(k), value_for(k, step));
                let r = c.send(&["SET", &key, &val, "PX", &VOLATILE_TTL_MS.to_string()]);
                if accepted(&r, "SET PX") {
                    model.set_volatile(&key, &val, Duration::from_millis(VOLATILE_TTL_MS), step);
                }
            }
            Op::Del(k) => {
                let key = key_name(k);
                if accepted(&c.send(&["DEL", &key]), "DEL") {
                    model.del(&key, step);
                }
            }
            Op::Unlink(k) => {
                let key = key_name(k);
                if accepted(&c.send(&["UNLINK", &key]), "UNLINK") {
                    model.del(&key, step);
                }
            }
            Op::Copy(a, b) if a != b => {
                let (src, dst) = (key_name(a), key_name(b));
                let reply = c.send(&["COPY", &src, &dst]);
                if accepted(&reply, "COPY") {
                    // The model predicts the outcome INDEPENDENTLY; the
                    // server's `:0`/`:1` then has to agree with it. Letting the
                    // model simply follow the reply would make this arm
                    // self-fulfilling.
                    // Whether either side carries a TTL is decided BEFORE the
                    // copy mutates the model.
                    //
                    // The prediction is only asserted when neither does. A
                    // 300 ms TTL is short enough that the model's
                    // `Instant::now()` and the server's own expiry evaluation
                    // can legitimately land on opposite sides of it — measured:
                    // this arm failed on seed 2 with `model predicted
                    // copied=true, server answered ":0"` while the whole suite
                    // ran alongside other test binaries, and seed 2 replayed
                    // ALONE passes. That is a race between two clocks, not a
                    // property of `COPY`, and asserting through it would make
                    // this file fail under load for a reason it does not claim
                    // to test. The `check()` sweep skips near-expiry keys for
                    // exactly the same reason.
                    //
                    // Nothing valuable is lost: a cold source is a long-lived,
                    // LRU-evicted key, which is precisely the non-volatile
                    // case still covered here. If `COPY` ever stopped
                    // consulting the cold tier (the moon#610 class), this arm
                    // is where it would show up.
                    let racy =
                        model.volatile.contains_key(&src) || model.volatile.contains_key(&dst);
                    let predicted = model.copy(&src, &dst, step);
                    let observed = reply.starts_with(":1");
                    if !racy && predicted != observed {
                        // Ask the server what IT thinks, only on the failure
                        // path so the happy path perturbs nothing.
                        let ex_src = c.send(&["EXISTS", &src]);
                        let pttl_src = c.send(&["PTTL", &src]);
                        let ex_dst = c.send(&["EXISTS", &dst]);
                        panic!(
                            "COPY {src} -> {dst} (seed {seed}): model predicted \
                             copied={predicted}, server answered {reply:?}. \
                             Server view: EXISTS src={ex_src:?} PTTL \
                             src={pttl_src:?} EXISTS dst={ex_dst:?}. Neither \
                             key carries a TTL, so this is NOT an expiry race: \
                             a source the server cannot see while the model can \
                             means COPY is not consulting the cold tier (the \
                             moon#610 class)."
                        );
                    }
                }
            }
            Op::Copy(..) => {}
            Op::FlushDb => {
                if accepted(&c.send(&["FLUSHDB"]), "FLUSHDB") {
                    model.flush(step);
                }
            }
        }

        // Pressure. This is what moves the keys above into the cold tier; the
        // sequence is uninteresting without it.
        let filler: Vec<Vec<String>> = (0..FILLER_PER_STEP)
            .map(|i| {
                vec![
                    "SET".to_string(),
                    format!("filler:{step:03}:{i:04}"),
                    "f".repeat(FILLER_VALUE_LEN),
                ]
            })
            .collect();
        for cmd in &filler {
            let parts: Vec<&str> = cmd.iter().map(String::as_str).collect();
            let r = c.send(&parts);
            // The AOF writer's fail-loud backpressure reply is a legitimate
            // answer under this much pressure, and it is not what is under
            // test. What must never happen is a filler write being counted as
            // a key under test, which it is not.
            assert!(
                r == "+OK\r\n" || r.contains("backpressure") || r.contains("OOM"),
                "filler SET answered {r:?}"
            );
        }
    }

    // Outrun every TTL the sequence handed out, so "expired" is unambiguous
    // for both sides rather than a race.
    std::thread::sleep(Duration::from_millis(VOLATILE_TTL_MS * 3));

    let spilled = info_field(&c.send(&["INFO", "stats"]), "spilled_keys");

    // ---- the instrument, asserted at the LIVE checkpoint (moon#965) ----
    //
    // `AofWriterPool::try_send_append` is fire-and-forget under `everysec`: a
    // full writer channel counts the drop here and logs it — AFTER the client
    // was told `+OK`. If that has happened even once, the log on disk is
    // missing writes the client was told succeeded, and EVERY assertion on the
    // far side of the crash is measuring that, not reconciliation. The counter
    // must be read BEFORE the crash: a restarted process resets it to zero and
    // the evidence is gone.
    let dropped_live = info_field(
        &c.send(&["INFO", "persistence"]),
        "aof_backpressure_dropped",
    );
    let mut ev = Evidence {
        dir: dir.clone(),
        dropped_live,
        ..Evidence::default()
    };

    check(&mut c, &model, seed, "LIVE", &ev);

    match forced_channel_cap() {
        // Unprovoked: a drop here means the server lost writes it had already
        // acked, and nothing measured after the crash means what it says.
        None => assert_eq!(
            dropped_live,
            0,
            "LIVE (seed {seed}): the server dropped {dropped_live} acked AOF \
             appends (aof_backpressure_dropped). Every assertion after the \
             crash is then measuring a log missing writes the client was told \
             succeeded, not a reconciliation bug. This is moon#965 hypothesis \
             H1 — `AofWriterPool::try_send_append` returning false on \
             `TrySendError::Full` under `appendfsync=everysec`. Server dir: {}",
            dir.display()
        ),
        // Provoked by `MOON_TEST_AOF_CHANNEL_CAP`. The assertion INVERTS: the
        // run exists to reach H1 deliberately, so a zero counter means the
        // knob never reached the server and the experiment proved nothing.
        // Confirming it, the run then continues into the crash — the whole
        // point is to see what the divergence table says when the tail of the
        // log is known to be missing.
        Some(cap) => {
            assert!(
                dropped_live > 0,
                "LIVE (seed {seed}): MOON_TEST_AOF_CHANNEL_CAP={cap} was set, \
                 but aof_backpressure_dropped is 0 — the cap never reached the \
                 server (wrong binary? MOON_BIN stale?) or `everysec` does not \
                 take the fire-and-forget path at all. Either way this run \
                 tests NOTHING about moon#965 H1, and a green result here must \
                 not be read as evidence. Server dir: {}",
                dir.display()
            );
            eprintln!(
                "moon#965 H1 PROVOKED (seed {seed}, cap {cap}): the server \
                 acked and then dropped {dropped_live} AOF appends. Continuing \
                 into the crash so the divergence table shows the shape a lost \
                 log tail produces."
            );
        }
    }

    // ---- the reconciliation itself ----
    drop(c);
    // Let `appendfsync everysec` and the spill/manifest ticks settle, so the
    // crash tests recovery ORDERING rather than a one-second AOF window.
    //
    // A KNOB since moon#965 (default unchanged at 3000 ms). The flume writer
    // channel holds up to `MOON_TEST_AOF_CHANNEL_CAP` (10,000) messages, and
    // this sleep is the ONLY thing that drains them before the SIGKILL — but
    // only if the writer task is actually scheduled during it. Setting this to
    // 0 removes the drain window entirely and turns hypothesis H2 into a
    // deterministic experiment instead of a property of the CI host's disk.
    let settle_ms: u64 = std::env::var("MOON_660_SETTLE_MS")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(3000);
    std::thread::sleep(Duration::from_millis(settle_ms));

    // The AOF as it stands at the instant of the crash. Compared with the
    // post-restart inventory below, this separates "the writer never landed
    // the bytes" from "recovery did not read the bytes that are there".
    ev.aof_before_kill = aof_inventory(&dir);

    let server = server.crash_and_restart();
    let mut c = Conn::open(server.port);

    ev.aof_after_restart = aof_inventory(&dir);
    ev.dropped_after_restart = Some(info_field(
        &c.send(&["INFO", "persistence"]),
        "aof_backpressure_dropped",
    ));

    check(
        &mut c,
        &model,
        seed,
        "AFTER CRASH + PHASE-3/PHASE-4 RECOVERY",
        &ev,
    );

    drop(c);
    drop(server);
    spilled
}

// ===========================================================================
// The divergence report (moon#965).
// ===========================================================================
//
// The pre-#965 `check()` panicked on the FIRST key that disagreed. That is one
// sample of a population, and it cost weeks: moon#965 reports `prop:key:004`
// answering `v4-7` where `v4-36` was expected, and nothing in that message
// says whether ANY OTHER key also disagreed — which is the whole question.
//
//   * every divergent key pinned at a step <= S, every correct key last
//     written at a step <= S too, and nothing correct after S
//                                        => the log lost its TAIL
//   * one key stale while other keys correctly hold LATER writes
//                                        => the cold plane shadowed it
//
// Those two have disjoint fixes and the old message could not tell them apart,
// so this collects every divergence, prints the table, and names the verdict.

/// Step index parsed out of a value minted by [`value_for`] (`v{key}-{step}-…`).
fn step_of(v: &str) -> Option<usize> {
    let rest = v.strip_prefix('v')?;
    let mut parts = rest.split('-');
    let _key = parts.next()?;
    parts.next()?.parse().ok()
}

fn head(v: &str) -> String {
    v[..v.len().min(20)].to_string()
}

struct Divergence {
    key: String,
    kind: &'static str,
    want: Option<String>,
    got: Option<String>,
    /// Step of the model op that last CHANGED this key.
    want_step: Option<usize>,
    /// Step the server's answer was minted at, when it answered a value.
    got_step: Option<usize>,
    /// Distance in this key's OWN history, in state changes.
    writes_behind: Option<usize>,
}

/// What the instrument recorded outside the keyspace sweep: the AOF on disk on
/// both sides of the crash, the acked-append drop counter, and whatever the
/// server itself said about a truncated log.
#[derive(Default)]
struct Evidence {
    dir: std::path::PathBuf,
    dropped_live: u64,
    dropped_after_restart: Option<u64>,
    aof_before_kill: Vec<(String, u64)>,
    aof_after_restart: Vec<(String, u64)>,
}

/// Every `*.aof` under `dir`, with its length. Layout-agnostic on purpose:
/// TopLevel writes `appendonly.aof` in `--dir`, PerShard writes an
/// `appendonlydir/`, and a rewrite adds generations. The question this answers
/// is "how many bytes did the writer actually land", and that is the sum.
fn aof_inventory(dir: &std::path::Path) -> Vec<(String, u64)> {
    fn walk(d: &std::path::Path, root: &std::path::Path, out: &mut Vec<(String, u64)>) {
        let Ok(rd) = std::fs::read_dir(d) else {
            return;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, root, out);
            } else if p.extension().is_some_and(|x| x == "aof")
                && let Ok(m) = std::fs::metadata(&p)
            {
                let rel = p.strip_prefix(root).unwrap_or(&p).to_string_lossy();
                out.push((rel.into_owned(), m.len()));
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

fn aof_total(inv: &[(String, u64)]) -> u64 {
    inv.iter().map(|(_, n)| *n).sum()
}

/// The server's own durability narrative for this run: how much of the AOF it
/// replayed, whether it found a short tail, and what the cold plane did.
///
/// The needles matter, and getting them wrong costs the whole line. The
/// `AOF truncated: N unparseable bytes at offset O (end of file)` message in
/// `src/persistence/aof/mod.rs` belongs to the LEGACY single-file replay. With
/// an `appendonlydir/` manifest — which is what this test produces and what
/// ships — the live path is `persistence::aof_manifest::shard_replay`, and it
/// says `AOF incr truncated tail: N bytes at offset O (treating as crash-time
/// EOF)`. Measured against a deliberately truncated log while building this
/// instrument: a filter looking only for `AOF truncated` reported "the server
/// said NOTHING" while the server had in fact named the exact byte offset.
/// Both spellings are matched below, case-insensitively on `truncat`.
fn server_err_durability_lines(dir: &std::path::Path) -> Vec<String> {
    // BOTH streams. `tracing` writes to stdout (`server.out`); a panic, the
    // jemalloc preamble and the startup refusals go to stderr (`server.err`).
    // Reading only one of them is how the `AOF truncated` line stayed
    // invisible — see [`server_stdout`].
    let mut out = Vec::new();
    for name in ["server.out", "server.err"] {
        let Ok(txt) = std::fs::read_to_string(dir.join(name)) else {
            continue;
        };
        out.extend(
            txt.lines()
                .filter(|l| {
                    let low = l.to_ascii_lowercase();
                    low.contains("truncat")
                        || low.contains("corrupt")
                        || l.contains("AOF append dropped")
                        || l.contains("AOF append LOST")
                        || l.contains("AOF incr replayed")
                        || l.contains("AOF multi-part loaded")
                        || l.contains("cold recovery:")
                        || l.contains("cold-plane reconcile")
                        || l.contains("rebuilt cold index")
                        || l.contains("manifest recovered")
                })
                .map(|l| format!("[{name}] {l}")),
        );
    }
    out
}

/// Copy the whole server `--dir` plus `report` into `MOON_660_EVIDENCE_DIR`, so
/// CI can upload the AOF, the manifest and `server.err` that diagnose the run.
/// A no-op when the variable is unset, which is every local run.
fn preserve_evidence(dir: &std::path::Path, seed: u64, phase: &str, report: &str) {
    let Ok(root) = std::env::var("MOON_660_EVIDENCE_DIR") else {
        return;
    };
    if root.trim().is_empty() {
        return;
    }
    let slug: String = phase
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    let dest = std::path::Path::new(&root).join(format!("seed-{seed}-{slug}"));
    let _ = std::fs::create_dir_all(&dest);
    let _ = std::fs::write(dest.join("divergence-report.txt"), report);

    fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
        let _ = std::fs::create_dir_all(to);
        let Ok(rd) = std::fs::read_dir(from) else {
            return;
        };
        for e in rd.flatten() {
            let p = e.path();
            let Some(name) = p.file_name() else { continue };
            if p.is_dir() {
                copy_tree(&p, &to.join(name));
            } else {
                let _ = std::fs::copy(&p, to.join(name));
            }
        }
    }
    copy_tree(dir, &dest.join("moon-dir"));
    eprintln!("moon#965 evidence preserved under {}", dest.display());
}

/// The invariant, stated once and applied identically on both sides of the
/// crash. Every key the sequence ever touched is swept — not just survivors —
/// because a resurrection is by definition a key the model believes is gone.
///
/// Unlike the pre-#965 version this does NOT stop at the first divergence: the
/// shape of the whole set is the diagnosis.
fn check(c: &mut Conn, model: &Model, seed: u64, phase: &str, ev: &Evidence) {
    let mut divergences: Vec<Divergence> = Vec::new();
    // Keys the sweep actually asserted on AND found correct. `s_kept` below is
    // a statement about these and only these — a skipped near-expiry key
    // proves nothing either way.
    let mut agreed_steps: Vec<(String, usize)> = Vec::new();

    for key in &model.seen {
        if model.near_expiry(key) {
            continue;
        }
        let got = parse_get(&c.send(&["GET", key]));
        let want = model.get(key);
        let want_step = model.last_change_step.get(key).copied();
        let got_step = got.as_deref().and_then(step_of);
        let writes_behind = match (want_step, got_step) {
            (Some(w), Some(g)) => model.writes_between(key, g, w),
            _ => None,
        };
        let kind = match (&want, &got) {
            (None, Some(_)) => "RESURRECTION",
            (Some(_), None) => "LOST WRITE",
            (Some(w), Some(g)) if w != g => "STALE VALUE",
            _ => {
                if let Some(w) = want_step {
                    agreed_steps.push((key.clone(), w));
                }
                continue;
            }
        };
        divergences.push(Divergence {
            key: key.clone(),
            kind,
            want: want.clone(),
            got: got.clone(),
            want_step,
            got_step,
            writes_behind,
        });
    }

    if divergences.is_empty() {
        return;
    }

    let report = render_report(model, seed, phase, ev, &divergences, &agreed_steps);
    preserve_evidence(&ev.dir, seed, phase, &report);
    panic!("{report}");
}

/// Turn the collected divergences into the one thing a red CI line has to
/// carry: which of the two mechanisms is at work.
fn render_report(
    model: &Model,
    seed: u64,
    phase: &str,
    ev: &Evidence,
    divergences: &[Divergence],
    agreed_steps: &[(String, usize)],
) -> String {
    use std::fmt::Write as _;

    // The earliest model change the server failed to reflect.
    let s_lost = divergences.iter().filter_map(|d| d.want_step).min();
    // The latest model change the server DID reflect correctly.
    let s_kept = agreed_steps.iter().map(|(_, s)| *s).max();
    // A truncated log also means every surviving stale answer predates the
    // cut; a shadowed answer need not.
    let stale_all_before_cut = match s_lost {
        Some(lost) => divergences
            .iter()
            .all(|d| d.got_step.is_none_or(|g| g < lost)),
        None => false,
    };
    let clean_cut = matches!((s_kept, s_lost), (Some(k), Some(l)) if k < l)
        || (s_kept.is_none() && s_lost.is_some());
    let tail_loss = clean_cut && stale_all_before_cut;

    let mut r = String::new();
    let _ = writeln!(
        r,
        "\n{phase}: {} DIVERGENCE(S) (seed {seed}) — moon#965 instrument\n",
        divergences.len()
    );

    let _ = writeln!(
        r,
        "  {:<16} {:<14} {:>10} {:>10} {:>8} {:>8}  WANT -> GOT",
        "KEY", "KIND", "WANT STEP", "GOT STEP", "dSTEPS", "dWRITES"
    );
    for d in divergences {
        let ws = d.want_step.map_or("-".to_string(), |v| v.to_string());
        let gs = d.got_step.map_or("nil".to_string(), |v| v.to_string());
        let dsteps = match (d.want_step, d.got_step) {
            (Some(w), Some(g)) => w.abs_diff(g).to_string(),
            _ => "-".to_string(),
        };
        let dwrites = d.writes_behind.map_or("-".to_string(), |v| v.to_string());
        let _ = writeln!(
            r,
            "  {:<16} {:<14} {:>10} {:>10} {:>8} {:>8}  {:?} -> {:?}",
            d.key,
            d.kind,
            ws,
            gs,
            dsteps,
            dwrites,
            d.want.as_deref().map(head),
            d.got.as_deref().map(head),
        );
    }

    let _ = writeln!(
        r,
        "\n  dSTEPS is a difference of SEQUENCE INDICES and means nothing on its own —\n  \
         steps in between need never have touched the key. dWRITES is the distance in\n  \
         the key's OWN history and is the number that matters. moon#965 was filed on\n  \
         dSTEPS=29; dWRITES for that same case is 1, i.e. the IMMEDIATELY PRECEDING\n  \
         write survived and the last one did not."
    );

    let _ = writeln!(
        r,
        "\n  latest step the server got RIGHT (s_kept): {}\n  \
           earliest step the server got WRONG (s_lost): {}\n  \
           every stale answer predates s_lost: {}",
        s_kept.map_or("(none — nothing agreed)".to_string(), |v| v.to_string()),
        s_lost.map_or("(unknown)".to_string(), |v| v.to_string()),
        stale_all_before_cut
    );

    if tail_loss {
        let _ = writeln!(
            r,
            "\n  VERDICT: TAIL LOSS. Every write the server still has was issued BEFORE\n  \
             every write it lost — a single cut between step {} and step {}. That is what\n  \
             a truncated append log looks like; no cold-plane mechanism produces it,\n  \
             because replay runs over a cleared keyspace and the later record would land\n  \
             on `InsertOrUpdate::Updated`, which unconditionally removes the cold entry.\n  \
             Look at H1 (acked appends dropped at enqueue — check aof_backpressure_dropped\n  \
             below) and H2 (the writer channel undrained at SIGKILL).",
            s_kept.map_or("start".to_string(), |v| v.to_string()),
            s_lost.map_or("?".to_string(), |v| v.to_string()),
        );
    } else {
        let _ = writeln!(
            r,
            "\n  VERDICT: COLD PLANE / SHADOWING. There is NO single cut: the server\n  \
             correctly holds writes issued at or after step {} while still answering a\n  \
             stale or resurrected value for a key changed at step {}. A truncated log\n  \
             cannot do that. Look at the Phase-3/Phase-4 seam — cold_index rebuild,\n  \
             replay_cold_spilled, finish_replay_cold_reconcile, demote_replayed_cold_shadows.",
            s_kept.map_or("?".to_string(), |v| v.to_string()),
            s_lost.map_or("?".to_string(), |v| v.to_string()),
        );
    }

    let _ = writeln!(
        r,
        "\n  AOF / DURABILITY EVIDENCE\n  \
           aof_backpressure_dropped (LIVE, pre-crash):  {}\n  \
           aof_backpressure_dropped (after restart):    {}\n  \
           AOF bytes before SIGKILL:                    {} in {:?}\n  \
           AOF bytes after restart:                     {} in {:?}",
        ev.dropped_live,
        ev.dropped_after_restart
            .map_or("(not sampled)".to_string(), |v| v.to_string()),
        aof_total(&ev.aof_before_kill),
        ev.aof_before_kill,
        aof_total(&ev.aof_after_restart),
        ev.aof_after_restart,
    );

    let lines = server_err_durability_lines(&ev.dir);
    if lines.is_empty() {
        let _ = writeln!(
            r,
            "  server.out/server.err said NOTHING about a truncated or corrupt log."
        );
    } else {
        let _ = writeln!(r, "  server log (durability lines, last 40):");
        for l in lines.iter().rev().take(40).rev() {
            let _ = writeln!(r, "    {l}");
        }
    }

    let _ = writeln!(
        r,
        "\n  HISTORY of the divergent keys (step -> value, `DEL` for a removal):"
    );
    for d in divergences {
        let h = model.history.get(&d.key).map_or(String::new(), |v| {
            v.iter()
                .map(|(s, val)| match val {
                    Some(x) => format!("{s}:{}", head(x)),
                    None => format!("{s}:DEL"),
                })
                .collect::<Vec<_>>()
                .join(" ")
        });
        let _ = writeln!(r, "    {:<16} {h}", d.key);
    }

    let _ = writeln!(
        r,
        "\n  Reproduce alone:   MOON_660_SEEDS={seed} cargo test --release \\\n    \
           --test cold_reconciliation_property_660\n  \
           Force H1 (acked appends dropped at enqueue):\n    \
           MOON_TEST_AOF_CHANNEL_CAP=1 MOON_660_SEEDS={seed} ...\n  \
           Force H2 (no drain window before SIGKILL):\n    \
           MOON_660_SETTLE_MS=0 MOON_660_SEEDS={seed} ...\n  \
           Server dir (AOF, manifest, server.err): {}",
        ev.dir.display()
    );
    r
}

/// The property, over several seeds.
///
/// Reddening mutation (applied, observed, reverted): gut
/// `Database::remove_cold_only` (`src/storage/db/kv_ops.rs`) to a no-op —
/// deletes then reach only the hot plane and the RESURRECTION arm fires:
///
///     LIVE: RESURRECTION / EXPIRED-COLD LEAK (seed 5): prop:key:004 is
///     absent in the model but the server answered 256 bytes starting
///     "v4-19-xxxxxxxxxx"
///
/// `spill_inflight_forget` (#459) and the `Updated` arm of `Database::set`
/// (task #56) are the other two seams this sweeps.
///
/// One honest note on the seed count: that mutation was caught on **seed 5**,
/// not seed 1 — seeds 1-4 completed clean. Whether any single sequence
/// happens to delete a key while it is cold is exactly the sampling the
/// `allkeys-lru` victim choice decides, which is why this runs a SWEEP and
/// why shrinking the default seed list to save wall-clock would quietly cost
/// most of the file's power.
#[test]
fn cold_reconciliation_holds_across_crash_for_every_seed() {
    let seeds: Vec<u64> = match std::env::var("MOON_660_SEEDS") {
        Ok(s) => s
            .split(',')
            .filter_map(|t| t.trim().parse().ok())
            .collect::<Vec<_>>(),
        Err(_) => (1..=6).collect(),
    };
    assert!(!seeds.is_empty(), "MOON_660_SEEDS parsed to nothing");

    let mut total_spilled = 0u64;
    for seed in &seeds {
        total_spilled += run_sequence(*seed);
    }

    // NON-VACUITY. If nothing ever tiered, every assertion above was a
    // statement about the hot plane alone and this file proved nothing about
    // reconciliation — the `gotcha_vacuous_benchmark_never_fires_guard`
    // failure mode, which is exactly how a property test rots into decoration.
    assert!(
        total_spilled > 0,
        "no key was tiered across {} seeds (spilled_keys summed to 0), so the \
         cold plane was never populated and the reconciliation invariant was \
         never exercised; raise FILLER_PER_STEP or lower MAXMEMORY_BYTES",
        seeds.len()
    );
}
