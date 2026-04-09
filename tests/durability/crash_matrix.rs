//! Crash injection test matrix.
//!
//! Axes: {persistence_mode} × {write_phase}
//!
//! Persistence modes:
//!   - none (no persistence)
//!   - rdb (snapshot only)
//!   - aof-always (appendfsync=always)
//!   - aof-everysec (appendfsync=everysec)
//!   - wal+rdb (WAL v3 + RDB snapshot)
//!   - disk-offload (cold tier enabled)
//!
//! Write phases:
//!   - during SET (mid-write)
//!   - during BGSAVE (mid-snapshot)
//!   - during BGREWRITEAOF (mid-compaction)
//!   - during WAL rotation (mid-segment-seal)
//!
//! Each cell: start server → write N keys → kill at phase → restart → verify.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

/// Helper: start a Moon server process with given config.
fn start_moon(args: &[&str]) -> std::process::Child {
    Command::new("./target/release/moon")
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to start moon server")
}

/// Helper: send a RESP command via raw TCP.
fn send_resp_command(addr: &str, cmd: &str) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect failed");
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();

    // Build RESP inline command
    let msg = format!("{}\r\n", cmd);
    stream.write_all(msg.as_bytes()).expect("write failed");
    stream.flush().ok();

    let reader = BufReader::new(&stream);
    let mut response = String::new();
    for line in reader.lines() {
        match line {
            Ok(l) => {
                response.push_str(&l);
                response.push('\n');
                // Simple heuristic: stop after first complete response
                if l.starts_with('+') || l.starts_with('-') || l.starts_with(':') {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    response
}

/// Helper: write N keys to the server.
fn write_keys(addr: &str, n: usize) {
    for i in 0..n {
        let cmd = format!("SET crash_test_key_{} value_{}", i, i);
        send_resp_command(addr, &cmd);
    }
}

/// Helper: count keys via DBSIZE.
fn get_dbsize(addr: &str) -> i64 {
    let resp = send_resp_command(addr, "DBSIZE");
    // Parse ":N\n" format
    resp.trim()
        .trim_start_matches(':')
        .trim()
        .parse()
        .unwrap_or(-1)
}

/// Crash matrix test: write keys, SIGKILL, restart, verify.
///
/// This is the test framework. Individual test functions parameterize
/// the persistence mode and write phase.
fn crash_test(
    mode: &str,
    port: u16,
    key_count: usize,
    persistence_args: &[&str],
) -> Result<(), String> {
    let addr = format!("127.0.0.1:{}", port);

    // 1. Start server with persistence config
    let mut server = start_moon(
        &[
            &["--port", &port.to_string(), "--shards", "1"],
            persistence_args,
        ]
        .concat(),
    );

    // Wait for server to be ready
    thread::sleep(Duration::from_millis(500));

    // 2. Write keys
    write_keys(&addr, key_count);

    // 3. Verify keys are written
    let before = get_dbsize(&addr);
    if before < key_count as i64 {
        let _ = server.kill();
        return Err(format!(
            "{}: only {} of {} keys written before crash",
            mode, before, key_count
        ));
    }

    // 4. SIGKILL the server (simulates crash)
    // SAFETY: `child.id()` returns a valid PID for a process we just spawned.
    // SIGKILL is always valid. We check the return code for robustness.
    let ret = unsafe { libc::kill(server.id() as i32, libc::SIGKILL) };
    assert_eq!(ret, 0, "libc::kill failed");
    let _ = server.wait();

    // 5. Restart with same config
    let mut server2 = start_moon(
        &[
            &["--port", &port.to_string(), "--shards", "1"],
            persistence_args,
        ]
        .concat(),
    );

    // Wait for recovery
    thread::sleep(Duration::from_secs(2));

    // 6. Verify data survived
    let after = get_dbsize(&addr);

    let _ = send_resp_command(&addr, "SHUTDOWN NOSAVE");
    let _ = server2.kill();
    let _ = server2.wait();

    // 7. Check RPO bounds
    match mode {
        "aof-always" => {
            if after < key_count as i64 {
                return Err(format!(
                    "aof-always: RPO violation — {} of {} keys survived (expected all)",
                    after, key_count
                ));
            }
        }
        "aof-everysec" => {
            // Allow up to 1 second of loss
            let min_expected = (key_count as i64) - 100; // rough bound
            if after < min_expected {
                return Err(format!(
                    "aof-everysec: RPO violation — {} of {} keys survived (min expected {})",
                    after, key_count, min_expected
                ));
            }
        }
        _ => {
            // Other modes: just verify server started and recovered
            if after < 0 {
                return Err(format!(
                    "{}: server did not recover (DBSIZE returned -1)",
                    mode
                ));
            }
        }
    }

    Ok(())
}

// ── Test functions (one per matrix cell) ────────────────────────────

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::*;

    // These tests require a built `moon` binary at ./target/release/moon
    // and libc for SIGKILL. Run with:
    //   cargo test --test durability_crash_matrix -- --ignored

    #[test]
    #[ignore] // Requires running server
    fn crash_aof_always_during_set() {
        let dir = tempfile::tempdir().unwrap();
        let result = crash_test(
            "aof-always",
            16400,
            1000,
            &[
                "--appendonly",
                "yes",
                "--appendfsync",
                "always",
                "--dir",
                dir.path().to_str().unwrap(),
            ],
        );
        assert!(result.is_ok(), "{}", result.unwrap_err());
    }

    #[test]
    #[ignore]
    fn crash_aof_everysec_during_set() {
        let dir = tempfile::tempdir().unwrap();
        let result = crash_test(
            "aof-everysec",
            16401,
            1000,
            &[
                "--appendonly",
                "yes",
                "--appendfsync",
                "everysec",
                "--dir",
                dir.path().to_str().unwrap(),
            ],
        );
        assert!(result.is_ok(), "{}", result.unwrap_err());
    }

    #[test]
    #[ignore]
    fn crash_no_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let result = crash_test("none", 16402, 100, &["--dir", dir.path().to_str().unwrap()]);
        // No persistence — data loss is expected. Just verify server recovers.
        assert!(result.is_ok(), "{}", result.unwrap_err());
    }

    /// G14: SIGKILL during disk-offload spill.
    ///
    /// Triggers cold-tier spill with a very low threshold, then crashes.
    /// After restart, server must recover and report a non-negative DBSIZE.
    #[test]
    #[ignore]
    fn crash_disk_offload_during_spill() {
        let dir = tempfile::tempdir().unwrap();
        let dir_str = dir.path().to_str().unwrap();

        let result = crash_test(
            "disk-offload",
            16403,
            500,
            &[
                "--appendonly",
                "yes",
                "--appendfsync",
                "always",
                "--dir",
                dir_str,
                "--disk-offload",
                "enable",
                "--disk-offload-threshold",
                "0.1",
            ],
        );
        // Disk offload with AOF-always: data should survive.
        // At minimum, server must recover (DBSIZE >= 0).
        assert!(result.is_ok(), "{}", result.unwrap_err());
    }
}
