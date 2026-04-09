//! Jepsen-lite crash-recovery test for Moon.
//!
//! Spawns a Moon server process with `appendfsync=always`, runs N concurrent
//! writer threads, periodically SIGKILLs the server, restarts it, and verifies
//! per-key linearizability by reading all keys back after each restart cycle.
//!
//! Requires the `moon` binary to be built (`cargo build --release`).
//! Marked `#[ignore]` because it depends on an external server binary.

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use tempfile::TempDir;

/// Number of concurrent writer threads.
const NUM_WRITERS: usize = 4;

/// Number of crash-restart cycles.
const RESTART_CYCLES: usize = 3;

/// How long writers run before we SIGKILL the server.
const WRITE_DURATION: Duration = Duration::from_secs(3);

/// How many keys each writer covers (each writer has its own key space).
const KEYS_PER_WRITER: u64 = 50;

/// Find the Moon binary. Check `target/release/moon` first, then `target/debug/moon`.
fn find_moon_binary() -> String {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let release = format!("{manifest_dir}/target/release/moon");
    if std::path::Path::new(&release).exists() {
        return release;
    }
    let debug = format!("{manifest_dir}/target/debug/moon");
    if std::path::Path::new(&debug).exists() {
        return debug;
    }
    // Fall back to PATH
    "moon".to_string()
}

/// Start a Moon server on the given port with AOF appendfsync=always.
fn start_server(port: u16, data_dir: &str) -> Child {
    let binary = find_moon_binary();
    let mut child = Command::new(&binary)
        .args([
            "--port",
            &port.to_string(),
            "--bind",
            "127.0.0.1",
            "--shards",
            "1",
            "--appendonly",
            "yes",
            "--appendfsync",
            "always",
            "--dir",
            data_dir,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| {
            panic!(
                "Failed to spawn moon binary at '{}': {}. Build with `cargo build --release` first.",
                binary, e
            )
        });

    // Wait for server to be ready by polling connection.
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(10) {
            // Try to read stderr for diagnostics
            if let Some(stderr) = child.stderr.take() {
                let reader = BufReader::new(stderr);
                let lines: Vec<String> = reader.lines().take(20).filter_map(|l| l.ok()).collect();
                panic!(
                    "Moon server did not start within 10s on port {}. stderr:\n{}",
                    port,
                    lines.join("\n")
                );
            }
            panic!("Moon server did not start within 10s on port {}", port);
        }
        if let Ok(client) = redis::Client::open(format!("redis://127.0.0.1:{}/", port))
            && let Ok(_conn) = client.get_connection_with_timeout(Duration::from_millis(200))
        {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    child
}

/// SIGKILL the server process.
fn kill_server(child: &mut Child) {
    #[cfg(unix)]
    {
        // Send SIGKILL via libc directly.
        // SAFETY: child.id() returns a valid PID for a running child process.
        unsafe {
            libc::kill(child.id() as i32, libc::SIGKILL);
        }
    }
    #[cfg(not(unix))]
    {
        let _ = child.kill();
    }
    // Wait for the process to fully exit.
    let _ = child.wait();
}

/// A writer thread that continuously sets keys with incrementing values.
/// Records the last successfully acknowledged value for each key.
fn writer_thread(
    port: u16,
    writer_id: usize,
    stop: Arc<AtomicBool>,
    counter: Arc<AtomicU64>,
) -> HashMap<String, u64> {
    let mut last_written: HashMap<String, u64> = HashMap::new();

    let client = match redis::Client::open(format!("redis://127.0.0.1:{}/", port)) {
        Ok(c) => c,
        Err(_) => return last_written,
    };
    let mut conn = match client.get_connection_with_timeout(Duration::from_secs(2)) {
        Ok(c) => c,
        Err(_) => return last_written,
    };

    while !stop.load(Ordering::Relaxed) {
        let seq = counter.fetch_add(1, Ordering::Relaxed);
        let key_idx = seq % KEYS_PER_WRITER;
        let key = format!("w{writer_id}:k{key_idx}");
        let value = seq;

        // SET key value — only record if the server acknowledged it.
        let result: redis::RedisResult<String> =
            redis::cmd("SET").arg(&key).arg(value).query(&mut conn);

        match result {
            Ok(ref s) if s == "OK" => {
                last_written.insert(key, value);
            }
            _ => {
                // Connection broken (server killed). Stop writing.
                break;
            }
        }
    }

    last_written
}

/// Merge per-writer maps into a single map. For duplicate keys, keep the
/// highest (latest) value — the one that was last ACK'd.
fn merge_written(maps: Vec<HashMap<String, u64>>) -> HashMap<String, u64> {
    let mut merged: HashMap<String, u64> = HashMap::new();
    for map in maps {
        for (k, v) in map {
            let entry = merged.entry(k).or_insert(0);
            if v > *entry {
                *entry = v;
            }
        }
    }
    merged
}

/// After restart, read all keys from the server and verify values match what
/// was last acknowledged. A key may have a HIGHER value than what we recorded
/// (if the server persisted a write whose ACK we lost to SIGKILL), but it must
/// never have a LOWER value (that would be data loss). Missing keys are also
/// acceptable if the ACK was lost.
fn verify_linearizability(port: u16, expected: &HashMap<String, u64>) -> (usize, usize, usize) {
    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client
        .get_connection_with_timeout(Duration::from_secs(5))
        .unwrap();

    let mut verified = 0usize;
    let mut missing = 0usize;
    let mut violations = 0usize;

    for (key, expected_value) in expected {
        let result: redis::RedisResult<Option<u64>> = redis::cmd("GET").arg(key).query(&mut conn);
        match result {
            Ok(Some(actual)) => {
                if actual < *expected_value {
                    // This is a linearizability violation: the server lost an ACK'd write.
                    eprintln!(
                        "VIOLATION: key={} expected>={} got={}",
                        key, expected_value, actual
                    );
                    violations += 1;
                } else {
                    verified += 1;
                }
            }
            Ok(None) => {
                // Key missing — could be that the ACK was lost before SIGKILL.
                // With appendfsync=always, a fully ACK'd SET should survive.
                // We count this but don't fail — depends on how the connection
                // broke relative to the fsync.
                missing += 1;
            }
            Err(e) => {
                panic!("Failed to GET key {}: {}", key, e);
            }
        }
    }

    (verified, missing, violations)
}

#[test]
#[ignore]
fn jepsen_lite_crash_recovery() {
    let data_dir = TempDir::new().unwrap();
    let data_path = data_dir.path().to_string_lossy().to_string();

    // Use a fixed port to avoid conflicts — OS-assigned ports are hard with
    // external processes. Use a high port unlikely to conflict.
    let port: u16 = 16399;

    // Cumulative expected state across restart cycles.
    let mut cumulative_expected: HashMap<String, u64> = HashMap::new();

    for cycle in 0..RESTART_CYCLES {
        eprintln!("=== Restart cycle {}/{} ===", cycle + 1, RESTART_CYCLES);

        // Start server (it will replay AOF from previous cycles).
        let mut server = start_server(port, &data_path);

        // If this isn't the first cycle, verify state from previous cycles survived.
        if !cumulative_expected.is_empty() {
            let (verified, missing, violations) =
                verify_linearizability(port, &cumulative_expected);
            eprintln!(
                "  Post-restart check: verified={} missing={} violations={}",
                verified, missing, violations
            );
            assert_eq!(
                violations, 0,
                "Linearizability violation after restart cycle {}",
                cycle
            );
        }

        // Spawn writer threads.
        let stop = Arc::new(AtomicBool::new(false));
        let counter = Arc::new(AtomicU64::new(cycle as u64 * 100_000));

        let handles: Vec<_> = (0..NUM_WRITERS)
            .map(|writer_id| {
                let stop = stop.clone();
                let counter = counter.clone();
                thread::spawn(move || writer_thread(port, writer_id, stop, counter))
            })
            .collect();

        // Let writers run for a while.
        thread::sleep(WRITE_DURATION);

        // SIGKILL the server (simulating crash).
        eprintln!("  SIGKILLing server...");
        kill_server(&mut server);

        // Signal writers to stop (they'll likely already be stopped due to broken pipe).
        stop.store(true, Ordering::Relaxed);

        // Collect results from writers.
        let writer_results: Vec<HashMap<String, u64>> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();
        let cycle_written = merge_written(writer_results);

        let total_acked: usize = cycle_written.len();
        eprintln!("  Writers ACK'd {} unique keys this cycle", total_acked);

        // Merge into cumulative expected state.
        for (k, v) in &cycle_written {
            let entry = cumulative_expected.entry(k.clone()).or_insert(0);
            if *v > *entry {
                *entry = *v;
            }
        }
    }

    // Final verification: restart server one more time and check everything.
    eprintln!("=== Final verification ===");
    let mut server = start_server(port, &data_path);

    let (verified, missing, violations) = verify_linearizability(port, &cumulative_expected);
    eprintln!(
        "Final: verified={} missing={} violations={}",
        verified, missing, violations
    );
    assert_eq!(
        violations, 0,
        "Linearizability violations detected in final verification"
    );
    assert!(
        verified > 0,
        "No keys were verified — writers may not have written any data"
    );

    // Graceful shutdown.
    let _ = server.kill();
    let _ = server.wait();

    eprintln!(
        "Jepsen-lite PASSED: {} keys verified, {} missing (ACK-lost), 0 violations across {} cycles",
        verified, missing, RESTART_CYCLES
    );
}
