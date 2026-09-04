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
}

impl Model {
    fn touch(&mut self, k: &str) {
        if !self.seen.iter().any(|s| s == k) {
            self.seen.push(k.to_string());
        }
    }

    fn set(&mut self, k: &str, v: &str) {
        self.touch(k);
        self.live.insert(k.to_string(), v.to_string());
        // A plain SET clears any TTL — the `#553` shape, and the reason
        // `SetVolatile` then `Set` must not leave the key expiring.
        self.volatile.remove(k);
    }

    fn set_volatile(&mut self, k: &str, v: &str, ttl: Duration) {
        self.touch(k);
        self.live.insert(k.to_string(), v.to_string());
        self.volatile.insert(k.to_string(), Instant::now() + ttl);
    }

    fn del(&mut self, k: &str) {
        self.touch(k);
        self.live.remove(k);
        self.volatile.remove(k);
    }

    fn copy(&mut self, src: &str, dst: &str) -> bool {
        self.touch(src);
        self.touch(dst);
        // Redis `COPY` without REPLACE refuses when the destination exists.
        if self.get(dst).is_some() {
            return false;
        }
        match self.get(src) {
            Some(v) => {
                self.live.insert(dst.to_string(), v);
                // COPY carries the TTL across; the sequence only ever copies
                // to a fresh destination, so mirroring it is enough.
                if let Some(&at) = self.volatile.get(src) {
                    self.volatile.insert(dst.to_string(), at);
                }
                true
            }
            None => false,
        }
    }

    fn flush(&mut self) {
        self.live.clear();
        self.volatile.clear();
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

fn spawn(dir: &std::path::Path) -> Server {
    std::fs::create_dir_all(dir.join("off")).expect("create offload dir");
    let (guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(find_moon_binary())
            .args(moon_args(dir, port))
            .stdout(std::process::Stdio::null())
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
            .stdout(std::process::Stdio::null())
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
                    model.set(&key, &val);
                }
            }
            Op::SetVolatile(k) => {
                let (key, val) = (key_name(k), value_for(k, step));
                let r = c.send(&["SET", &key, &val, "PX", &VOLATILE_TTL_MS.to_string()]);
                if accepted(&r, "SET PX") {
                    model.set_volatile(&key, &val, Duration::from_millis(VOLATILE_TTL_MS));
                }
            }
            Op::Del(k) => {
                let key = key_name(k);
                if accepted(&c.send(&["DEL", &key]), "DEL") {
                    model.del(&key);
                }
            }
            Op::Unlink(k) => {
                let key = key_name(k);
                if accepted(&c.send(&["UNLINK", &key]), "UNLINK") {
                    model.del(&key);
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
                    let predicted = model.copy(&src, &dst);
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
                    model.flush();
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

    check(&mut c, &model, seed, "LIVE");

    // ---- the reconciliation itself ----
    drop(c);
    // Let `appendfsync everysec` and the spill/manifest ticks settle, so the
    // crash tests recovery ORDERING rather than a one-second AOF window.
    std::thread::sleep(Duration::from_secs(3));
    let server = server.crash_and_restart();
    let mut c = Conn::open(server.port);

    check(
        &mut c,
        &model,
        seed,
        "AFTER CRASH + PHASE-3/PHASE-4 RECOVERY",
    );

    drop(c);
    drop(server);
    spilled
}

/// The invariant, stated once and applied identically on both sides of the
/// crash. Every key the sequence ever touched is swept — not just survivors —
/// because a resurrection is by definition a key the model believes is gone.
fn check(c: &mut Conn, model: &Model, seed: u64, phase: &str) {
    for key in &model.seen {
        if model.near_expiry(key) {
            continue;
        }
        let got = parse_get(&c.send(&["GET", key]));
        let want = model.get(key);
        match (&want, &got) {
            (None, Some(v)) => panic!(
                "{phase}: RESURRECTION / EXPIRED-COLD LEAK (seed {seed}): {key} \
                 is absent in the model but the server answered {} bytes \
                 starting {:?}. A cold copy outlived its delete or its TTL — \
                 the #212/#213/#459 class. Re-run this case alone with \
                 MOON_660_SEEDS={seed}",
                v.len(),
                &v[..v.len().min(16)]
            ),
            (Some(w), None) => panic!(
                "{phase}: LOST WRITE (seed {seed}): {key} should hold {:?} but \
                 the server answered nil. Re-run with MOON_660_SEEDS={seed}",
                &w[..w.len().min(16)]
            ),
            (Some(w), Some(g)) if w != g => panic!(
                "{phase}: STALE VALUE (seed {seed}): {key} should hold {:?} but \
                 the server answered {:?} — an older copy shadowed the current \
                 one. Re-run with MOON_660_SEEDS={seed}",
                &w[..w.len().min(16)],
                &g[..g.len().min(16)]
            ),
            _ => {}
        }
    }
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
