//! CRASH-COLD-NOAOF: disk-offload cold recovery WITHOUT AOF (#22 regression).
//!
//! Reproduces and guards the #22 durability bug: with `--appendonly no` and
//! `--disk-offload enable`, cold (spilled-to-disk) keys must survive a hard
//! crash and be served via cold read-through after restart — driven PURELY by
//! the per-shard manifest, with NO AOF replay (there is no AOF under
//! `appendonly no`).
//!
//! Root cause (main.rs): `persistence_dir` was derived as `Some(..)` only when
//! `appendonly == "yes" || save.is_some()`. Under `appendonly no` it was `None`,
//! and the shard-construction closure gated `restore_from_persistence` behind
//! `if let Some(ref dir) = persistence_dir { .. }` — so the v3 cold rebuild was
//! skipped entirely even though `disk_offload_base` was `Some`. Cold keys
//! recovered 0/200. The fix fires recovery when
//! `persistence_dir.is_some() || disk_offload_base.is_some()`.
//!
//! Discriminating signal (RED vs GREEN):
//!   * `heap-*.mpf` files on disk prove the probes were durably spilled to cold.
//!   * POST-crash read-through proves recovery re-attached the cold index.
//!   RED (pre-#22-fix binary): heap files present, POST == ~0 (recovery skipped).
//!   GREEN (post-#22-fix):     heap files present, POST == ~PROBE_COUNT.
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

use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

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
/// Post-crash recovery floor. This test catches the *categorical* #22
/// regression: when recovery is skipped the cold tier recovers a hard 0; when
/// it fires it recovers most of the cold probes. The spread is intrinsic to
/// crashing under `appendonly no`, where a cold key only survives once its
/// eviction-tick manifest commit has landed (no per-write durability without
/// AOF) — and it WIDENS under CPU contention (4 shards on a 6-core CI box
/// running the filler load + harness), where the spill/manifest threads drain
/// fewer ticks before the kill. Observed tail under that contention dipped to
/// 143/200 (still 143× above the RED 0 — recovery plainly fired). So the floor
/// is 65%: robustly inside the legitimate GREEN band, 130× above the RED 0, and
/// no longer clipping the load-induced lower tail. The `settle` sleep before the
/// kill is sized in tandem (see SETTLE_BEFORE_KILL) to drain most ticks first.
const RECOVERY_FLOOR: usize = (PROBE_COUNT * 65) / 100;
/// Seconds to let the async spill/manifest ticks drain before the SIGKILL. Under
/// `appendonly no` only tick-committed cold keys survive a crash; a longer settle
/// lands more commits and tightens the recovery distribution upward. Sized for
/// the contended multishard case (8s) — raising it trades test time for a higher,
/// tighter GREEN band.
const SETTLE_BEFORE_KILL: u64 = 8;

fn unique_port() -> u16 {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind to port 0");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

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
    Command::new("./target/release/moon")
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

/// Write `FILLER_COUNT` distinct keys via pipelined TCP SETs to push memory
/// past the disk-offload threshold and evict the (older) probe keys to cold.
fn write_filler(port: u16) {
    let mut stream =
        std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).expect("connect for filler");
    stream.set_write_timeout(Some(Duration::from_secs(30))).ok();
    let val = "F".repeat(FILLER_VALUE_LEN);
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    for i in 0..FILLER_COUNT {
        let key = format!("filler:{}", i);
        let cmd = format!(
            "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            key.len(),
            key,
            val.len(),
            val
        );
        buf.extend_from_slice(cmd.as_bytes());
        // Flush in ~64 KiB chunks so the server applies eviction incrementally
        // rather than receiving one giant burst that overruns the spill queue.
        if buf.len() >= 64 * 1024 {
            stream.write_all(&buf).expect("filler write");
            buf.clear();
        }
    }
    if !buf.is_empty() {
        stream.write_all(&buf).expect("filler tail write");
    }
    stream.flush().ok();
    // Drain is unnecessary for correctness here; closing the stream is fine.
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
    let port = unique_port();
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

    // Setup sanity: if nothing spilled, the test exercised nothing — fail loud
    // (a maxmemory/eviction misconfig, not a recovery pass).
    assert!(
        heap_files > 0,
        "test setup: expected cold spill (heap-*.mpf) but found 0 — eviction never fired \
         (maxmemory/threshold too high). post={}",
        post
    );

    // The actual #22 assertion: cold keys recovered WITHOUT AOF.
    // Clean up ONLY on success so a failure keeps moon.std*.log for diagnosis
    // (matches the crash_matrix convention: cleanup after the assert).
    let recovered = post >= RECOVERY_FLOOR;
    if recovered {
        let _ = std::fs::remove_dir_all(&dir);
    }
    assert!(
        recovered,
        "#22 REGRESSION: post-crash cold read-through {}/{} (floor {}). heap_files={}. \
         Logs kept at {} for diagnosis. A POST near 0 with cold files on disk means \
         restore_from_persistence was skipped under `appendonly no` — the persistence_dir \
         gate regressed (recovery must fire on disk_offload_base.is_some()).",
        post,
        PROBE_COUNT,
        RECOVERY_FLOOR,
        heap_files,
        dir.display()
    );
}
