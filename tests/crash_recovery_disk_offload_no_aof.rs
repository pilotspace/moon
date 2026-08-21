//! CRASH-COLD-NOAOF: disk-offload cold recovery WITHOUT AOF (#22 regression).
//!
//! Reproduces and guards the #22 durability bug: with `--appendonly no` and
//! `--disk-offload enable`, keys that DID make it durably to a manifest-active
//! `heap-*.mpf` file must survive a hard crash and be served via cold
//! read-through after restart — driven PURELY by the per-shard manifest, with
//! NO AOF replay (there is no AOF under `appendonly no`).
//!
//! Root cause (main.rs): `persistence_dir` was derived as `Some(..)` only when
//! `appendonly == "yes" || save.is_some()`. Under `appendonly no` it was `None`,
//! and the shard-construction closure gated `restore_from_persistence` behind
//! `if let Some(ref dir) = persistence_dir { .. }` — so the v3 cold rebuild was
//! skipped entirely even though `disk_offload_base` was `Some`. Cold keys
//! recovered 0/200. The fix fires recovery when
//! `persistence_dir.is_some() || disk_offload_base.is_some()`.
//!
//! ## task #44: why this no longer floors on `PROBE_COUNT * 65%`
//!
//! The original version of this test forced eviction by blasting `FILLER_COUNT`
//! writes down one connection and asserted `post >= 65% of PROBE_COUNT`,
//! assuming most LRU-evicted probes land durably in a `heap-*.mpf` file. That
//! assumption predates `disk_offload_spill_inert()` / PR #273 (`fix(storage):
//! policy-aware eviction fail-close for disk-offload without a durability
//! backstop`) and the write-path gate documented at
//! `run_write_eviction_gate` (`src/server/conn/handler_monoio/mod.rs`): under
//! `--appendonly no` (no AOF backstop) the PER-CONNECTION write-path eviction
//! gate — the path every plain `SET`/`HSET` past `maxmemory` actually takes —
//! has NO `ShardManifest` handle and therefore PLAIN-DROPS victims (Redis
//! "pure cache, no durability" semantics for `allkeys-*`/`volatile-*`; see
//! `config::disk_offload_spill_inert` and the boot-time WARN it drives, plus
//! `docs/PRODUCTION-CONTRACT.md`'s CRASH-02 note on task #57, which made
//! exactly this drop-not-OOM behavior the *documented fix*, not a bug). Only
//! the periodic memory-pressure tick (`shard/persistence_tick.rs`, the one
//! call site that DOES hold the manifest) durably spills under `appendonly
//! no`, and a single busy connection's synchronous per-write eviction reacts
//! before that tick ever observes a sustained over-budget shard — so a write
//! burst like `write_filler` durably spills only a small, load-sensitive
//! trickle of the evicted keys (observed 1-51/200 across runs), not a
//! majority. That is intentional, tested (see
//! `async_spill_no_aof_backstop_no_manifest_allkeys_lru_evicts_pure_cache` in
//! `storage::eviction`), and orthogonal to the #22 recovery-gate bug this file
//! guards.
//!
//! So instead of hoping eviction throughput clears a fixed floor, this test
//! establishes GROUND TRUTH directly from the on-disk manifest + `heap-*.mpf`
//! files right before the kill (whatever a shard's manifest marks `Active`
//! KvLeaf at that instant is, by the disk-offload contract, durably spilled)
//! and asserts recovery returns AT LEAST that many probes — i.e., "every key
//! that really did reach durable cold storage survives the crash", which is
//! the actual #22 regression signal, decoupled from how many probes eviction
//! happened to spill instead of drop.
//!
//! Discriminating signal (RED vs GREEN):
//!   * `heap-*.mpf` files + manifest `Active` KvLeaf entries prove specific
//!     probes were durably spilled to cold BEFORE the kill (ground truth,
//!     read directly via `moon::persistence::{manifest,kv_page}`).
//!   * POST-crash read-through must recover AT LEAST that many probes.
//!
//!   RED (pre-#22-fix binary): ground truth > 0, POST == ~0 (recovery skipped
//!   entirely — the persistence_dir gate regressed).
//!   GREEN (post-#22-fix): POST >= ground truth.
//!
//! Because there is NO AOF under `appendonly no`, a non-zero POST count can ONLY
//! come from the cold-manifest recovery path the #22 fix enables.
//!
//! NOTE: we deliberately do NOT read the probes back PRE-crash. A cold GET
//! PROMOTES the key back to the hot tier (db.rs cold_read_through), and hot keys
//! are NOT durable under `appendonly no` — a pre-crash read-through would pull
//! probes out of cold and lose them on crash, corrupting the measurement.
//!
//! Run with (monoio default — matches CI):
//!   cargo build --release
//!   cargo test --release --test crash_recovery_disk_offload_no_aof -- --ignored
//!
//! tokio runtime:
//!   cargo build --release --no-default-features \
//!     --features runtime-tokio,jemalloc,graph,text-index
//!   cargo test --release --no-default-features \
//!     --features runtime-tokio,jemalloc,graph,text-index \
//!     --test crash_recovery_disk_offload_no_aof -- --ignored
//!
//! Requires: built release binary, `redis-cli` on PATH.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::collections::HashSet;
use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use moon::persistence::kv_page::read_datafile;
use moon::persistence::manifest::{FileStatus, ShardManifest};
use moon::persistence::page::PageType;

const PROBE_COUNT: usize = 200;
const PROBE_VALUE_LEN: usize = 500;
/// Filler keys written after the probes to drive memory past the disk-offload
/// threshold, forcing the (older) probe keys to be evicted to the cold tier.
const FILLER_COUNT: usize = 16_000;
const FILLER_VALUE_LEN: usize = 600;
/// 8 MiB total across 4 shards (2 MiB/shard). disk-offload spills at
/// 0.85 × maxmemory; probes (~100 KiB) + filler (~9.6 MiB) >> threshold.
const MAXMEMORY_BYTES: usize = 8 * 1024 * 1024;
const SHARDS: usize = 4;
/// Seconds to let the async spill/manifest ticks drain before the SIGKILL. Under
/// `appendonly no` only tick-committed cold keys are durable — a longer settle
/// gives the periodic memory-pressure cascade (`shard/persistence_tick.rs`, the
/// one call site with a `ShardManifest` handle under no-AOF; see the module
/// doc's task #44 section) more chances to catch a transient over-budget shard
/// before it's kept in ground truth by `count_durable_probe_entries` below.
const SETTLE_BEFORE_KILL: u64 = 8;

fn unique_dir(suffix: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "moon-cold-noaof-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ))
}

fn start_moon(port: u16, dir: &std::path::Path) -> Child {
    let off_dir = dir.join("off");
    std::fs::create_dir_all(&off_dir).expect("create off dir");
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            &SHARDS.to_string(),
            "--maxmemory",
            &MAXMEMORY_BYTES.to_string(),
            "--maxmemory-policy",
            "allkeys-lru",
            "--disk-offload",
            "enable",
            "--disk-offload-dir",
            off_dir.to_str().expect("off dir utf8"),
            // The bug under test: NO AOF. Cold recovery must work anyway.
            "--appendonly",
            "no",
            "--cold-orphan-sweep-interval-secs",
            "60",
            "--dir",
        ])
        .arg(dir)
        // Captured to a log file so a CI flake produces a real diagnostic
        // (see feedback_silenced_child_stdio_flake — never Stdio::null()).
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("create moon stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("create moon stderr log"))
        .spawn()
        .expect("spawn moon (run `cargo build --release` with default features first)")
}

fn wait_for_port(port: u16) {
    for _ in 0..80 {
        if std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            std::thread::sleep(Duration::from_millis(200));
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("moon did not start within 8s on port {}", port);
}

/// Wait until the previous server on `port` is FULLY down before restarting on
/// it. The crash test restarts on the SAME port immediately after a SIGKILL;
/// without this, round 2 races round 1's listener teardown and hits "Address
/// already in use", and round 1's lingering per-shard SO_REUSEPORT listeners
/// keep load-balancing some connections — so the post-crash probes read 0 even
/// though recovery itself fully succeeded (rebuilt the cold index on every
/// shard). That presented as a tokio-specific flake purely because of OS
/// port-release timing, not a recovery defect.
///
/// IMPORTANT: a bind-based "is it free?" check is useless here — moon binds with
/// SO_REUSEPORT, so a plain `TcpListener::bind` SUCCEEDS even while round 1 still
/// holds the port. The reliable signal is the opposite: poll until a `connect`
/// is REFUSED, meaning no listener (round 1's) is accepting any more.
fn wait_for_port_down(port: u16) {
    let addr = format!("127.0.0.1:{}", port);
    let mut consecutive_refused = 0;
    for _ in 0..120 {
        match std::net::TcpStream::connect_timeout(
            &addr.parse().expect("addr"),
            Duration::from_millis(100),
        ) {
            Ok(_) => {
                // Something is still accepting (round 1 not fully gone yet).
                consecutive_refused = 0;
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(_) => {
                // Require two consecutive refusals to avoid a transient gap
                // between two of round 1's per-shard listeners.
                consecutive_refused += 1;
                if consecutive_refused >= 2 {
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
    // Fall through after ~12s: let the restart proceed and surface its own error.
}

/// Restart attempts for round 2. The central tokio listener plain-binds the
/// port; on a rapid SIGKILL→restart it can race the dying round-1 process's
/// socket teardown, hit `Address already in use`, and self-terminate
/// (`Listener error` → `Server shut down`). That is a transient OS port-release
/// race on fast restart, NOT a recovery defect — the attempt that DOES bind
/// recovers the cold tier fully. Detect the self-terminated restart and retry,
/// bounded. The recovery assertion (`post >= RECOVERY_FLOOR`) still has to pass
/// on the attempt that binds, so retrying works around the race WITHOUT
/// weakening the durability signal.
const RESTART_ATTEMPTS: usize = 6;

/// Start moon and return a child that is BOTH alive and accepting on `port`.
/// Retries if the freshly-spawned server self-terminates on a transient rebind
/// EADDRINUSE (see `RESTART_ATTEMPTS`). Panics only if every attempt fails to
/// come up — that would be a real start failure, not the benign rebind race.
fn start_moon_alive(port: u16, dir: &std::path::Path) -> Child {
    for attempt in 1..=RESTART_ATTEMPTS {
        let mut child = start_moon(port, dir);
        let mut up = false;
        // Poll up to ~8s for the server to either accept or self-terminate.
        for _ in 0..80 {
            // Did the server exit on its own (rebind EADDRINUSE self-shutdown)?
            if let Ok(Some(_status)) = child.try_wait() {
                break; // self-terminated — fall through to retry
            }
            if std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
                std::thread::sleep(Duration::from_millis(200));
                up = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        if up {
            return child;
        }
        // Reap (no-op if already self-terminated) and back off so the kernel
        // finishes releasing the port before the next rebind attempt.
        let _ = child.kill();
        let _ = child.wait();
        if attempt < RESTART_ATTEMPTS {
            std::thread::sleep(Duration::from_millis(300));
        }
    }
    panic!(
        "moon failed to start+serve on port {} after {} attempts \
         (not the rebind race — a real start failure)",
        port, RESTART_ATTEMPTS
    );
}

fn probe_key(i: usize) -> String {
    format!("probe:{}", i)
}

fn redis_set(port: u16, key: &str, value: &str) {
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "SET", key, value])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("redis-cli SET");
    assert!(
        out.status.success(),
        "redis-cli SET {} failed: {}",
        key,
        String::from_utf8_lossy(&out.stderr)
    );
}

fn redis_get(port: u16, key: &str) -> Option<String> {
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "GET", key])
        .output()
        .expect("redis-cli GET");
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if s.is_empty() || s == "(nil)" {
        None
    } else {
        Some(s)
    }
}

/// Push `FILLER_COUNT` keys past the disk-offload threshold via ONE `MSET`
/// (task #44).
///
/// The old version pipelined `FILLER_COUNT` individual `SET`s. Under the
/// current write-path eviction gate (see the module doc's task #44 section)
/// EVERY individual write command re-checks `maxmemory` and evicts
/// synchronously through the manifest-less, plain-drop fallback — so a
/// pipelined-`SET` burst never lets a shard's memory sit over budget long
/// enough for the periodic, manifest-backed memory-pressure tick
/// (`persistence_tick.rs`) to be the one that reclaims it, and since LRU
/// always picks the OLDEST key (the probes, written first) that plain-drop
/// path reliably erases the very keys this test needs to end up durably on
/// disk (ground truth was empirically 0/200 with the old design).
///
/// `MSET`'s handler (`string::mset`) inserts all pairs directly with no
/// per-key eviction check, and the write-path gate that DOES run checks
/// memory exactly ONCE, using the PRE-`MSET` total — so a single giant
/// `MSET` can leave a shard far over budget with no further write command
/// pending to re-trigger the plain-drop gate. As long as this connection
/// sends no more writes after the `MSET` returns, the ONLY thing left to
/// reclaim that overshoot is the periodic tick — which reaches
/// `evict_batch_durable_no_aof` (the manifest-backed, actually-durable path)
/// and, being the same LRU policy, sheds the probes (globally the oldest
/// keys, written and settled well before this `MSET`) before it ever reaches
/// into the filler.
fn write_filler(port: u16) {
    use std::io::{BufRead, BufReader};

    let mut stream =
        std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).expect("connect for filler");
    stream.set_write_timeout(Some(Duration::from_secs(60))).ok();
    stream.set_read_timeout(Some(Duration::from_secs(60))).ok();
    let val = "F".repeat(FILLER_VALUE_LEN);

    let total_args = 1 + 2 * FILLER_COUNT;
    let mut buf: Vec<u8> = Vec::with_capacity(FILLER_COUNT * (FILLER_VALUE_LEN + 32));
    buf.extend_from_slice(format!("*{}\r\n$4\r\nMSET\r\n", total_args).as_bytes());
    for i in 0..FILLER_COUNT {
        let key = format!("filler:{}", i);
        buf.extend_from_slice(
            format!("${}\r\n{}\r\n${}\r\n{}\r\n", key.len(), key, val.len(), val).as_bytes(),
        );
    }
    stream.write_all(&buf).expect("filler MSET write");

    // Read the `+OK\r\n` reply so the caller's subsequent settle-sleep starts
    // only once the server has actually finished applying every key (all
    // shard legs of `coordinate_mset` completed) — otherwise the settle
    // window could start before the write even lands.
    let mut reply = String::new();
    let mut reader = BufReader::new(&stream);
    reader
        .read_line(&mut reply)
        .expect("filler MSET reply read");
    assert!(
        reply.starts_with('+'),
        "filler MSET must succeed, got: {}",
        reply
    );
}

fn count_heap_files(dir: &std::path::Path) -> usize {
    let off = dir.join("off");
    fn walk(p: &std::path::Path, acc: &mut usize) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    walk(&path, acc);
                } else if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("heap-") && n.ends_with(".mpf"))
                    .unwrap_or(false)
                {
                    *acc += 1;
                }
            }
        }
    }
    let mut acc = 0;
    walk(&off, &mut acc);
    acc
}

fn count_probes_readable(port: u16) -> usize {
    (0..PROBE_COUNT)
        .filter(|&i| redis_get(port, &probe_key(i)).is_some())
        .count()
}

/// Ground truth (task #44): read each shard's `ShardManifest` + `Active`
/// `KvLeaf` `heap-*.mpf` DataFiles directly (best-effort, exactly the way
/// `persistence::recovery::recover_shard_v3_pitr` reads them at boot) and
/// count the DISTINCT `probe:*` keys that were durably on disk at the moment
/// this is called. Call it right before the SIGKILL — nothing else writes
/// after `SETTLE_BEFORE_KILL`, so this is a faithful snapshot of what the
/// crash lands on. Errors (a manifest/DataFile caught mid-write) are
/// swallowed and simply under-count — safe, since under-counting only makes
/// the recovery assertion easier to satisfy, never harder.
fn count_durable_probe_entries(dir: &std::path::Path) -> usize {
    let off = dir.join("off");
    let mut probe_keys: HashSet<Vec<u8>> = HashSet::new();
    let Ok(shard_dirs) = std::fs::read_dir(&off) else {
        return 0;
    };
    for shard_entry in shard_dirs.flatten() {
        let shard_dir = shard_entry.path();
        if !shard_dir.is_dir() {
            continue;
        }
        let Some(shard_name) = shard_dir.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let manifest_path = shard_dir.join(format!("{shard_name}.manifest"));
        if !manifest_path.exists() {
            continue;
        }
        let Ok(manifest) = ShardManifest::open(&manifest_path) else {
            continue;
        };
        let data_dir = shard_dir.join("data");
        for file_entry in manifest.files() {
            if file_entry.status != FileStatus::Active
                || file_entry.file_type != PageType::KvLeaf as u8
            {
                continue;
            }
            let heap_path = data_dir.join(format!("heap-{:06}.mpf", file_entry.file_id));
            let Ok(pages) = read_datafile(&heap_path) else {
                continue;
            };
            for page in &pages {
                for slot_idx in 0..page.slot_count() {
                    if let Some(kv_entry) = page.get(slot_idx)
                        && kv_entry.key.starts_with(b"probe:")
                    {
                        probe_keys.insert(kv_entry.key);
                    }
                }
            }
        }
    }
    probe_keys.len()
}

#[cfg(unix)]
fn sigkill(child: &mut Child) {
    let pid = child.id() as i32;
    unsafe {
        libc::kill(pid, libc::SIGKILL);
    }
    let _ = child.wait();
}

#[cfg(not(unix))]
fn sigkill(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// #22: cold keys spilled under `--appendonly no --disk-offload enable` must
/// recover after a SIGKILL crash via the per-shard manifest (no AOF).
#[test]
#[ignore] // Requires built release binary + redis-cli; run explicitly.
fn cold_keys_recover_after_crash_without_aof() {
    let port = common::reserve_port();
    let dir = unique_dir("c22");
    std::fs::create_dir_all(&dir).expect("create test dir");

    // -- Round 1: populate + force cold spill -------------------------------
    let mut child = start_moon(port, &dir);
    wait_for_port(port);

    let probe_val = "P".repeat(PROBE_VALUE_LEN);
    for i in 0..PROBE_COUNT {
        redis_set(port, &probe_key(i), &probe_val);
    }
    // Filler evicts the older probes to the cold tier.
    write_filler(port);
    // Let the async spill thread drain its queue and commit manifests. Under
    // `appendonly no` a cold key is only crash-durable once its eviction-tick
    // manifest commit lands, so give the ticks ample time before the kill.
    std::thread::sleep(Duration::from_secs(SETTLE_BEFORE_KILL));

    // NOTE: do NOT read the probes here — a cold GET promotes the key back to
    // hot, and hot is not durable under `appendonly no`, which would corrupt
    // the POST measurement. `heap_files > 0` is the spill-happened proof.
    let heap_files = count_heap_files(&dir);
    // Ground truth (task #44): exactly which probes are durably on disk RIGHT
    // NOW, read the same way `restore_from_persistence`'s v3 path will read
    // them after the kill. See the module doc's task #44 section for why this
    // replaces the old fixed `RECOVERY_FLOOR` — eviction throughput under
    // `--appendonly no` is a separate, already-fixed concern (PR #273 /
    // task #57's intentional plain-drop), not this file's regression signal.
    let durable_probes = count_durable_probe_entries(&dir);

    // SIGKILL — hard crash, no graceful drain.
    sigkill(&mut child);
    // Wait until round 1 is fully down (port no longer accepting) before
    // restarting on it — otherwise round 2 races round 1's SO_REUSEPORT listener
    // teardown and the probes read round 1's empty post-eviction hot tier as 0
    // despite a fully successful recovery. This was the entire source of the
    // apparent tokio flake.
    wait_for_port_down(port);

    // -- Round 2: recover ---------------------------------------------------
    // start_moon_alive retries the rebind if the central listener loses the
    // port-release race and self-terminates (transient EADDRINUSE on fast
    // restart) — the recovery assertion below still has to pass on the attempt
    // that binds, so this does not weaken the durability signal.
    let mut child2 = start_moon_alive(port, &dir);
    // Give recovery a beat to re-attach the cold index before probing.
    std::thread::sleep(Duration::from_secs(1));

    let post = count_probes_readable(port);

    sigkill(&mut child2);

    eprintln!(
        "cold_keys_recover_after_crash_without_aof: heap_files={} durable_probes(ground truth)={} \
         post(recovered)={} probe_count={}",
        heap_files, durable_probes, post, PROBE_COUNT
    );

    // Setup sanity: if no probe ever reached durable cold storage, the test
    // exercised nothing — fail loud (a maxmemory/eviction misconfig or a
    // pathologically unlucky tick timing, not a recovery pass/fail signal).
    assert!(
        durable_probes > 0,
        "test setup: expected at least one probe durably spilled (heap-*.mpf, manifest \
         Active KvLeaf) but found 0 — either eviction never fired (maxmemory/threshold too \
         high) or the periodic memory-pressure tick (persistence_tick.rs) never got a chance \
         to run before write_filler finished. heap_files={}. post={}",
        heap_files,
        post
    );

    // The actual #22 assertion: every durably-spilled probe recovers WITHOUT
    // AOF. Clean up ONLY on success so a failure keeps moon.std*.log for
    // diagnosis (matches the crash_matrix convention: cleanup after the assert).
    let recovered = post >= durable_probes;
    if recovered {
        let _ = std::fs::remove_dir_all(&dir);
    }
    assert!(
        recovered,
        "#22 REGRESSION: post-crash cold read-through {}/{} (ground truth: {} probes were \
         durably on disk before the kill). heap_files={}. Logs kept at {} for diagnosis. A \
         POST below the durably-spilled ground truth means restore_from_persistence was \
         skipped (or lossy) under `appendonly no` — the persistence_dir gate regressed \
         (recovery must fire on disk_offload_base.is_some()).",
        post,
        PROBE_COUNT,
        durable_probes,
        heap_files,
        dir.display()
    );
}
