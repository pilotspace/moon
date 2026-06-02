//! CRASH-01-LITE: per-shard AOF crash-recovery matrix (RFC step 8).
//!
//! Boots a multi-shard moon with `--appendonly yes`, drives a write load,
//! kills the process with SIGKILL, restarts, and asserts the recovered
//! state matches what was on disk pre-crash. Validates the full
//! step 2-5 pipeline end-to-end:
//!
//!   handler write → AOF channel → per-shard writer → fsync
//!     → SIGKILL → restart → PerShard manifest load → replay_per_shard
//!     → ordered merge (empty today) → server accepts client traffic
//!
//! Test matrix (subset of RFC § 7 — "LITE" defers cross-shard TXN and
//! BGREWRITEAOF interleaving to step 9 + future steps):
//!   - `--shards 2 --appendonly yes --appendfsync everysec` + SIGKILL
//!     → ≥99% recover (everysec fsync window allows ≤1s loss).
//!   - `--shards 2 --appendonly yes --appendfsync always` + SIGKILL
//!     → 100% recover (every +OK must observe an fsync; H1 closure).
//!
//! Run with (monoio default — matches CI):
//!   cargo build --release
//!   cargo test --release --test crash_matrix_per_shard_aof -- --ignored
//!
//! Requires: built release binary, `redis-cli` on PATH.
//! Crash-recovery is now validated on BOTH runtimes. The PerShard AOF manifest
//! init (initialize_multi) + replay_per_shard run under runtime-monoio AND
//! runtime-tokio as of the tokio multi-part enablement; the tokio per-shard
//! writer writes the byte-identical framed incr format. To validate tokio:
//!   cargo build --release --no-default-features \
//!     --features runtime-tokio,jemalloc,graph,text-index
//!   cargo test --release --no-default-features \
//!     --features runtime-tokio,jemalloc,graph,text-index \
//!     --test crash_matrix_per_shard_aof -- --ignored

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

use std::process::{Child, Command, Stdio};
use std::time::Duration;

const KEY_COUNT: usize = 200;

fn unique_port() -> u16 {
    // Ask the OS to assign an available ephemeral port by binding to 0.
    // The socket is immediately dropped after reading the port — there is a
    // brief TOCTOU window, but it is far safer than the previous pid-modulo
    // scheme which collides when multiple cargo test processes run in parallel
    // (e.g., CI --test-threads > 1 across feature flag matrix jobs).
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
        "moon-crash-matrix-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ))
}

fn start_moon(port: u16, dir: &std::path::Path) -> Child {
    start_moon_with_fsync(port, dir, "everysec")
}

fn start_moon_with_fsync(port: u16, dir: &std::path::Path, fsync: &str) -> Child {
    Command::new("./target/release/moon")
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "2",
            "--appendonly",
            "yes",
            "--appendfsync",
            fsync,
            // No `--unsafe-multishard-aof` — step 9 lifted the gate; this
            // test now validates that the default `--shards 2 --appendonly
            // yes` launch is crash-safe out of the box.
            "--dir",
        ])
        .arg(dir)
        // Captured to a log file so a CI flake produces a real diagnostic
        // rather than the silent "connection refused" symptom the project
        // already paid for once (see feedback_silenced_child_stdio_flake).
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

fn redis_set(port: u16, key: &str, value: &str) {
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "SET", key, value])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("redis-cli SET");
    assert!(
        out.status.success(),
        "redis-cli SET {} {} failed: {}",
        key,
        value,
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

/// Send N RPUSH commands to `key` in a single pipelined TCP write and drain
/// all N integer responses.  Useful for double-write detection because
/// RPUSH is non-idempotent: N pushes → LLEN N; double-replay → LLEN 2*N.
fn pipeline_rpush(port: u16, key: &str, n: usize) {
    use std::io::{BufRead, BufReader, Write};

    let mut stream =
        std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).expect("connect for pipeline");
    stream.set_read_timeout(Some(Duration::from_secs(10))).ok();

    // Build one TCP segment with N RPUSH commands (pipeline).
    let mut buf: Vec<u8> = Vec::with_capacity(n * 64);
    for i in 0..n {
        let val = format!("v{}", i);
        let cmd = format!(
            "*3\r\n$5\r\nRPUSH\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            key.len(),
            key,
            val.len(),
            val
        );
        buf.extend_from_slice(cmd.as_bytes());
    }
    stream.write_all(&buf).expect("pipeline write");
    stream.flush().ok();

    // Drain all N responses — each is `:N\r\n` (integer reply).
    let mut reader = BufReader::new(&stream);
    let mut line = String::new();
    for _ in 0..n {
        line.clear();
        let _ = reader.read_line(&mut line);
    }
}

/// Query LLEN for `key`; returns -1 on parse failure.
fn redis_llen(port: u16, key: &str) -> i64 {
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "LLEN", key])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("redis-cli LLEN");
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    // redis-cli returns the integer as a bare decimal string (no `:` prefix).
    s.parse().unwrap_or(-1)
}

/// SIGKILL via `kill -9` (Child::kill on Unix already sends SIGKILL but
/// being explicit here documents intent and survives stdlib changes).
#[cfg(unix)]
fn sigkill(child: &mut Child) {
    let pid = child.id() as i32;
    unsafe {
        libc::kill(pid, libc::SIGKILL);
    }
    // Wait for the kernel to reap the process so its file handles are
    // released and the next spawn can lock the AOF files.
    let _ = child.wait();
}

#[cfg(not(unix))]
fn sigkill(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[test]
#[ignore] // Requires built release binary + redis-cli; run explicitly.
fn crash_01_lite_per_shard_aof_recovers_after_sigkill() {
    let port = unique_port();
    let dir = unique_dir("crash01");
    std::fs::create_dir_all(&dir).expect("create test dir");

    // -- Round 1 --------------------------------------------------------
    let mut child = start_moon(port, &dir);
    wait_for_port(port);

    // Write KEY_COUNT keys. Use hash tags to deterministically spread
    // across both shards (half on each) — confirms per-shard files are
    // populated.
    let mut expected: std::collections::HashMap<String, String> =
        std::collections::HashMap::with_capacity(KEY_COUNT);
    for i in 0..KEY_COUNT {
        // Alternate hash tag → shard partition.
        let tag = if i % 2 == 0 { "a" } else { "b" };
        let key = format!("crash:{{{}}}:{}", tag, i);
        let value = format!("v-{}", i);
        redis_set(port, &key, &value);
        expected.insert(key, value);
    }

    // Wait > 1s so the everysec fsync window definitely flushed every
    // entry to durable storage.
    std::thread::sleep(Duration::from_millis(1500));

    // SIGKILL — no graceful shutdown, no chance for in-flight buffers
    // to drain.
    sigkill(&mut child);

    // -- Round 2 (recovery) ---------------------------------------------
    let mut child2 = start_moon(port, &dir);
    wait_for_port(port);

    // Verify every key recovered. The everysec contract permits up to 1s
    // of loss, but we slept 1.5s before the kill so we should see 100%.
    let mut missing: Vec<String> = Vec::new();
    let mut mismatched: Vec<String> = Vec::new();
    for (key, want) in &expected {
        match redis_get(port, key) {
            None => missing.push(key.clone()),
            Some(got) if got != *want => {
                mismatched.push(format!("{}: want={} got={}", key, want, got))
            }
            Some(_) => {}
        }
    }

    // Cleanup before any failure assertion so the temp dir isn't leaked
    // when the assertion fires.
    sigkill(&mut child2);

    assert!(
        missing.is_empty() && mismatched.is_empty(),
        "CRASH-01-LITE: {} missing, {} mismatched. Sample missing: {:?}, sample mismatched: {:?}",
        missing.len(),
        mismatched.len(),
        missing.iter().take(5).collect::<Vec<_>>(),
        mismatched.iter().take(5).collect::<Vec<_>>(),
    );

    // Successful run — clean up the temp dir.
    let _ = std::fs::remove_dir_all(&dir);
}

/// CRASH-01-LITE-ALWAYS: H1 (in-flight loss) closure.
///
/// Under `appendfsync=always` the writer must fsync before the +OK is
/// observable by the client. The integration plumbs `AppendSync` through
/// the handlers (handler_monoio / handler_sharded) via
/// `AofWriterPool::try_send_append_durable`, which awaits the writer's
/// ack and converts AOF failure into `Frame::Error` so the client never
/// sees +OK for an entry that was not durable.
///
/// This test SIGKILLs the server **without** any quiescing sleep — every
/// +OK observed by the client must therefore be backed by an fsync on
/// disk. Any data loss here proves the handler→writer ack handshake is
/// broken.
#[test]
#[ignore] // Requires built release binary + redis-cli; run explicitly.
fn crash_01_lite_always_per_shard_aof_recovers_after_sigkill() {
    // Offset port so this test never collides with the everysec test
    // when both run on the same dev host.
    let port = unique_port().saturating_add(1);
    let dir = unique_dir("crash01-always");
    std::fs::create_dir_all(&dir).expect("create test dir");

    // -- Round 1 --------------------------------------------------------
    let mut child = start_moon_with_fsync(port, &dir, "always");
    wait_for_port(port);

    let mut expected: std::collections::HashMap<String, String> =
        std::collections::HashMap::with_capacity(KEY_COUNT);
    for i in 0..KEY_COUNT {
        let tag = if i % 2 == 0 { "a" } else { "b" };
        let key = format!("crash:{{{}}}:{}", tag, i);
        let value = format!("v-{}", i);
        // SET only returns +OK after the writer fsyncs under Always.
        redis_set(port, &key, &value);
        expected.insert(key, value);
    }

    // NO quiescing sleep — H1 contract is that each +OK already saw fsync.
    sigkill(&mut child);

    // -- Round 2 (recovery) ---------------------------------------------
    let mut child2 = start_moon_with_fsync(port, &dir, "always");
    wait_for_port(port);

    let mut missing: Vec<String> = Vec::new();
    let mut mismatched: Vec<String> = Vec::new();
    for (key, want) in &expected {
        match redis_get(port, key) {
            None => missing.push(key.clone()),
            Some(got) if got != *want => {
                mismatched.push(format!("{}: want={} got={}", key, want, got))
            }
            Some(_) => {}
        }
    }

    sigkill(&mut child2);

    assert!(
        missing.is_empty() && mismatched.is_empty(),
        "CRASH-01-LITE-ALWAYS: {} missing, {} mismatched. Sample missing: {:?}, sample mismatched: {:?}",
        missing.len(),
        mismatched.len(),
        missing.iter().take(5).collect::<Vec<_>>(),
        mismatched.iter().take(5).collect::<Vec<_>>(),
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// PIPELINE-DOUBLE-WRITE: FIX-W1-2 discriminating regression test.
///
/// Before the fix, `wal_append_and_fanout` was called with `aof_pool` inside
/// the `PipelineBatch` / `PipelineBatchSlotted` SPSC arms.  The connection-
/// handler coordinator **also** writes the AOF entry after collecting each
/// shard response (handler_sharded/mod.rs:1703 / handler_monoio/mod.rs:2004).
/// The net effect was every cross-shard pipelined command written TWICE to
/// the target shard's AOF file.  On recovery, the replay doubled the logical
/// effect of every write.
///
/// `RPUSH` is non-idempotent: N pushes → LLEN N.
/// Double-replay → LLEN 2*N.  This makes over-count directly observable
/// after crash+recovery — no AOF-binary inspection needed.
///
/// Shard routing (CRC16 mod 2):
///   `{a}` → shard 1  |  `{b}` → shard 0
///
/// A connection assigned to shard 0 routes `{a}` commands cross-shard
/// (PipelineBatchSlotted).  A connection on shard 1 routes `{b}` commands
/// cross-shard.  By pipelining BOTH sets in one call this test exercises the
/// PipelineBatchSlotted path regardless of which shard the OS assigns to the
/// connection (SO_REUSEPORT is non-deterministic at test time).
///
/// **Red state (commit before 124cae2):** LLEN list{a} and/or list{b} == 2*N
///   — the SPSC-side duplicate write survives the fsync window and is replayed.
/// **Green state (post-fix):** both LLENs == N exactly.
#[test]
#[ignore] // Requires built release binary + redis-cli; run explicitly.
fn pipeline_batch_no_double_write_after_crash_recovery() {
    const N: usize = 20;

    let port = unique_port().saturating_add(2);
    let dir = unique_dir("pipeline-dbl");
    std::fs::create_dir_all(&dir).expect("create test dir");

    // -- Round 1 --------------------------------------------------------
    let mut child = start_moon(port, &dir);
    wait_for_port(port);

    // Pipeline N RPUSHes to list{a} (→ shard 1) and N to list{b} (→ shard 0)
    // in two separate pipelined bursts.  Each list should end up with exactly
    // N elements after a crash+recovery cycle.
    pipeline_rpush(port, "list{a}", N);
    pipeline_rpush(port, "list{b}", N);

    // Wait > 1s so the everysec fsync window flushed all entries.
    std::thread::sleep(Duration::from_millis(1500));

    sigkill(&mut child);

    // -- Round 2 (recovery) ---------------------------------------------
    let mut child2 = start_moon(port, &dir);
    wait_for_port(port);

    let llen_a = redis_llen(port, "list{a}");
    let llen_b = redis_llen(port, "list{b}");

    sigkill(&mut child2);

    // Failure message identifies which list was doubled and the expected count
    // so the root cause is unambiguous in CI output.
    assert_eq!(
        llen_a,
        N as i64,
        "PIPELINE-DOUBLE-WRITE: list{{a}} LLEN after crash+recovery = {} (expected {}). \
         A value of {} indicates the PipelineBatchSlotted SPSC arm is still passing \
         aof_pool to wal_append_and_fanout, causing duplicate AOF entries (FIX-W1-2).",
        llen_a,
        N,
        2 * N as i64,
    );
    assert_eq!(
        llen_b,
        N as i64,
        "PIPELINE-DOUBLE-WRITE: list{{b}} LLEN after crash+recovery = {} (expected {}). \
         A value of {} indicates the PipelineBatchSlotted SPSC arm is still passing \
         aof_pool to wal_append_and_fanout, causing duplicate AOF entries (FIX-W1-2).",
        llen_b,
        N,
        2 * N as i64,
    );

    let _ = std::fs::remove_dir_all(&dir);
}
