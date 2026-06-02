//! CRASH-02 (F6): per-shard BGREWRITEAOF crash-recovery matrix.
//!
//! Validates that multi-shard `BGREWRITEAOF` (per-shard fan-out compaction,
//! `--experimental-per-shard-rewrite`) is crash-safe: a rewrite that straddles
//! a write stream, followed by SIGKILL, must recover to EXACTLY the acked
//! state — no dropped writes, no double-applied writes.
//!
//! Why INCR: it is non-idempotent. If the rewrite leaves a pre-snapshot INCR
//! in BOTH the new base (snapshot) AND a replayed incr, recovery double-applies
//! it (count > expected). If the rewrite drops an in-flight INCR, recovery
//! under-counts (count < expected). A plain SET would mask both — the final
//! value would look correct regardless. The exact-count assertion is the whole
//! point.
//!
//! This is the acceptance gate for F6 monoio. The historical naive multi-shard
//! BGREWRITEAOF lost ~38% of keys on restart (main.rs gate, 2026-05-26); these
//! tests must show 0% loss and 0% duplication.
//!
//! Run (monoio default — matches production + CI):
//!   cargo build --release
//!   cargo test --release --test crash_matrix_per_shard_bgrewriteaof -- --ignored
//!
//! Requires: built release binary (default features = runtime-monoio) and
//! `redis-cli` on PATH. Per-shard BGREWRITEAOF is monoio-only (the fold uses
//! synchronous std::fs IO); the tokio build refuses it at the command handler.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

use std::process::{Child, Command, Stdio};
use std::time::Duration;

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
        "moon-crash-bgrewriteaof-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ))
}

fn start_moon(port: u16, dir: &std::path::Path) -> Child {
    Command::new("./target/release/moon")
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "2",
            "--appendonly",
            "yes",
            "--appendfsync",
            "everysec",
            // F6: open the gate so BGREWRITEAOF routes to the per-shard
            // fan-out coordinator instead of the refusal error.
            "--experimental-per-shard-rewrite",
            "--dir",
        ])
        .arg(dir)
        // Pipe child stdio to log files — a silent connection-refused flake
        // would otherwise hide the real startup error (see
        // feedback_silenced_child_stdio_flake).
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

/// One `INCR key`, returning the new value (or -1 on failure).
fn redis_incr(port: u16, key: &str) -> i64 {
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "INCR", key])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("redis-cli INCR");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or(-1)
}

fn redis_get_i64(port: u16, key: &str) -> i64 {
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "GET", key])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("redis-cli GET");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or(-1)
}

/// Issue BGREWRITEAOF; returns the raw reply string. Asserts it is NOT a gate
/// refusal (the gate must be open under --experimental-per-shard-rewrite).
fn bgrewriteaof(port: u16) -> String {
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "BGREWRITEAOF"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("redis-cli BGREWRITEAOF");
    let reply = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert!(
        !reply.to_lowercase().contains("not yet supported")
            && !reply.to_lowercase().contains("not supported")
            && !reply.to_lowercase().contains("shards 1"),
        "BGREWRITEAOF was refused (gate still closed?): {:?}. \
         Expected the per-shard fan-out to start.",
        reply
    );
    reply
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

/// Count base RDB files under `appendonlydir/shard-*/` whose seq > 1. The
/// initial generation is seq=1; a successful per-shard rewrite advances to
/// seq=2, so a seq>1 base proves the compaction physically ran (not a silent
/// no-op that would let a buggy "rewrite did nothing" still pass the data
/// assertions).
fn compacted_base_exists(dir: &std::path::Path) -> bool {
    let aof_dir = dir.join("appendonlydir");
    let Ok(shards) = std::fs::read_dir(&aof_dir) else {
        return false;
    };
    for shard in shards.flatten() {
        let p = shard.path();
        if !p.is_dir() {
            continue;
        }
        let Ok(files) = std::fs::read_dir(&p) else {
            continue;
        };
        for f in files.flatten() {
            let name = f.file_name().to_string_lossy().to_string();
            // Base files look like `moon.aof.<seq>.base.rdb`. Extract <seq>.
            if let Some(rest) = name.strip_prefix("moon.aof.") {
                if let Some(seq_str) = rest.strip_suffix(".base.rdb") {
                    if seq_str.parse::<u64>().map(|s| s > 1).unwrap_or(false) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// CRASH-02-STRADDLE: a rewrite issued mid-write-stream must not drop or
/// double-apply any acked INCR.
///
/// Two counters, one per shard (`{a}` → one shard, `{b}` → the other), each
/// INCR'd `N` times. A BGREWRITEAOF is fired in the middle of the stream so the
/// rewrite snapshot straddles the writes: some INCRs land in the old incr
/// (folded into the new base), some in the new incr. After all INCRs are acked
/// we quiesce >1.5s (everysec flush) and SIGKILL, then assert each counter
/// recovers to EXACTLY N.
#[test]
#[ignore] // Requires built release binary + redis-cli; run explicitly.
fn bgrewriteaof_straddle_crash_recovers_exact() {
    const N: i64 = 500;

    let port = unique_port();
    let dir = unique_dir("straddle");
    std::fs::create_dir_all(&dir).expect("create test dir");

    // -- Round 1 --------------------------------------------------------
    let mut child = start_moon(port, &dir);
    wait_for_port(port);

    let key_a = "cnt:{a}";
    let key_b = "cnt:{b}";

    let mut started_reply = String::new();
    for i in 0..N {
        redis_incr(port, key_a);
        redis_incr(port, key_b);
        // Fire the rewrite once, roughly mid-stream, so the snapshot
        // boundary falls inside the write sequence.
        if i == N / 2 {
            started_reply = bgrewriteaof(port);
        }
    }
    assert!(
        !started_reply.is_empty(),
        "BGREWRITEAOF was never issued — test bug"
    );

    // Give the fan-out time to complete its fold + commit, then quiesce so the
    // everysec window flushes every acked INCR to durable storage.
    std::thread::sleep(Duration::from_millis(2000));

    // The compacted base must physically exist (rewrite was not a no-op).
    assert!(
        compacted_base_exists(&dir),
        "no seq>1 base RDB found under {}/appendonlydir — the per-shard \
         rewrite did not produce a compacted base (silent no-op?)",
        dir.display()
    );

    sigkill(&mut child);

    // -- Round 2 (recovery) ---------------------------------------------
    let mut child2 = start_moon(port, &dir);
    wait_for_port(port);

    let got_a = redis_get_i64(port, key_a);
    let got_b = redis_get_i64(port, key_b);

    sigkill(&mut child2);

    assert_eq!(
        got_a, N,
        "CRASH-02-STRADDLE: {} recovered to {} (expected {}). \
         <{} = dropped writes across the rewrite boundary; \
         >{} = double-applied pre-snapshot INCRs (incr replayed on top of a \
         base that already contains them).",
        key_a, got_a, N, N, N
    );
    assert_eq!(
        got_b, N,
        "CRASH-02-STRADDLE: {} recovered to {} (expected {}). \
         <{} = dropped writes; >{} = double-applied INCRs.",
        key_b, got_b, N, N, N
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// CRASH-02-COMPOSE: base + post-rewrite incr must compose exactly once.
///
/// INCR a counter `pre` times, BGREWRITEAOF (folds the `pre` INCRs into the new
/// base), wait for the fold to settle, then INCR `post` more times (these land
/// only in the new incr). After crash+recovery the counter must equal
/// `pre + post` — proving the new base (snapshot of the pre INCRs) and the new
/// incr (post INCRs) replay together without dropping or double-counting the
/// boundary.
#[test]
#[ignore] // Requires built release binary + redis-cli; run explicitly.
fn bgrewriteaof_base_plus_incr_recovers_exact() {
    const PRE: i64 = 300;
    const POST: i64 = 200;

    let port = unique_port().saturating_add(1);
    let dir = unique_dir("compose");
    std::fs::create_dir_all(&dir).expect("create test dir");

    // -- Round 1 --------------------------------------------------------
    let mut child = start_moon(port, &dir);
    wait_for_port(port);

    let key = "compose:{a}";

    for _ in 0..PRE {
        redis_incr(port, key);
    }

    let reply = bgrewriteaof(port);
    assert!(!reply.is_empty(), "BGREWRITEAOF returned empty reply");

    // Let the fold complete + writers reopen to the new incr before the POST
    // INCRs, so POST writes land cleanly in the new generation's incr.
    std::thread::sleep(Duration::from_millis(1500));

    assert!(
        compacted_base_exists(&dir),
        "no seq>1 base RDB found — rewrite did not compact"
    );

    for _ in 0..POST {
        redis_incr(port, key);
    }

    // Quiesce so the everysec window flushes the POST INCRs.
    std::thread::sleep(Duration::from_millis(1500));

    sigkill(&mut child);

    // -- Round 2 (recovery) ---------------------------------------------
    let mut child2 = start_moon(port, &dir);
    wait_for_port(port);

    let got = redis_get_i64(port, key);

    sigkill(&mut child2);

    assert_eq!(
        got,
        PRE + POST,
        "CRASH-02-COMPOSE: {} recovered to {} (expected {}+{}={}). \
         A value of {} means the pre-rewrite INCRs were double-applied \
         (base + replayed old incr); a value of {} means the post-rewrite \
         INCRs in the new incr were lost.",
        key,
        got,
        PRE,
        POST,
        PRE + POST,
        PRE + PRE + POST,
        PRE,
    );

    let _ = std::fs::remove_dir_all(&dir);
}
