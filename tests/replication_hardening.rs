//! Replication hardening tests for PSYNC2.
//!
//! Tests partial resync, full resync, network partition recovery,
//! replica kill-restart, and replica promotion paths.
//!
//! Run: cargo test --test replication_hardening -- --ignored
//! Requires: a built moon binary — set MOON_BIN, or default
//! ./target/release/moon (⚠ on a shared macOS/Linux checkout the default may
//! be the other platform's binary; always pin MOON_BIN, repo harness rule).

mod common;

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

fn moon_bin() -> std::path::PathBuf {
    common::find_moon_binary()
}

fn start_moon(port: u16, dir: &str, extra: &[&str]) -> Guard {
    start_moon_shards(port, dir, 1, extra)
}

/// Same as [`start_moon`] but with a caller-supplied `--shards` count — the
/// soak harness (scripts/soak-replication-24h.sh) runs a MULTI-shard master
/// (`--master-shards 4` default) against a single-shard replica (replica-side
/// streaming replication is single-shard only; see
/// `src/replication/replica.rs`), so a master-restart repro needs to match
/// that shard topology, not the single-shard default `start_moon` uses.
fn start_moon_shards(port: u16, dir: &str, shards: u32, extra: &[&str]) -> Guard {
    Guard(
        Command::new(moon_bin())
            .args(
                [
                    &[
                        "--port",
                        &port.to_string(),
                        "--shards",
                        &shards.to_string(),
                        "--dir",
                        dir,
                    ][..],
                    extra,
                ]
                .concat(),
            )
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Failed to start moon"),
    )
}

/// Kill-on-drop server guard.
///
/// Teardown is SIGKILL-based, NOT `SHUTDOWN NOSAVE`: moon has no working
/// SHUTDOWN command on the production path (the dispatch arm is an error stub
/// and the connection handlers never intercept it — see task #27), so a
/// graceful-shutdown cleanup blocks `Child::wait` forever. Kill-on-drop also
/// survives assert panics, so a failed test no longer leaks servers that hold
/// ports and wedge later tests. (Repo harness rule: always kill -9 moon.)
struct Guard(std::process::Child);

impl Drop for Guard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn send_cmd(addr: &str, cmd: &str) -> String {
    send_cmd_with_timeout(addr, cmd, Duration::from_secs(5))
}

/// Same as [`send_cmd`] but with a caller-supplied read timeout. Needed for
/// commands like `WAIT <n> <timeout_ms>` that legitimately block server-side
/// for up to `timeout_ms` — the client-side read timeout must exceed that or
/// we time out locally before the server ever replies.
fn send_cmd_with_timeout(addr: &str, cmd: &str, read_timeout: Duration) -> String {
    let Ok(mut stream) = TcpStream::connect(addr) else {
        return String::new();
    };
    stream.set_read_timeout(Some(read_timeout)).ok();
    stream
        .write_all(format!("{}\r\n", cmd).as_bytes())
        .expect("write");
    stream.flush().ok();

    let mut reader = BufReader::new(&stream);
    let mut resp = String::new();
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                let trimmed = line.trim_end_matches("\r\n").trim_end_matches('\n');
                resp.push_str(trimmed);
                resp.push('\n');
                if trimmed.starts_with('+') || trimmed.starts_with('-') || trimmed.starts_with(':')
                {
                    break;
                }
                // Bulk string: $N header — read N bytes + CRLF
                if let Some(after_dollar) = trimmed.strip_prefix('$') {
                    let len: i64 = after_dollar.trim().parse().unwrap_or(-1);
                    if len < 0 {
                        break; // $-1 = nil
                    }
                    let mut buf = vec![0u8; (len as usize) + 2]; // +2 for \r\n
                    if std::io::Read::read_exact(&mut reader, &mut buf).is_ok() {
                        let data = String::from_utf8_lossy(&buf[..len as usize]);
                        resp.push_str(&data);
                        resp.push('\n');
                    }
                    break;
                }
            }
        }
    }
    resp
}

fn dbsize(addr: &str) -> i64 {
    let resp = send_cmd(addr, "DBSIZE");
    if resp.is_empty() {
        panic!("dbsize: failed to connect to {addr}");
    }
    resp.trim()
        .trim_start_matches(':')
        .trim()
        .parse()
        .unwrap_or(-1)
}

fn write_keys(addr: &str, prefix: &str, n: usize) {
    for i in 0..n {
        send_cmd(addr, &format!("SET {}_{} value_{}", prefix, i, i));
    }
}

/// Issue `WAIT <num_required> <timeout_ms>` against `addr` and return the
/// acked replica count. The client-side read timeout is `timeout_ms` plus a
/// generous buffer so we never race the server's own WAIT deadline.
fn wait_cmd(addr: &str, num_required: u64, timeout_ms: u64) -> i64 {
    let resp = send_cmd_with_timeout(
        addr,
        &format!("WAIT {} {}", num_required, timeout_ms),
        Duration::from_millis(timeout_ms) + Duration::from_secs(5),
    );
    resp.trim()
        .trim_start_matches(':')
        .trim()
        .parse()
        .unwrap_or(-1)
}

/// Poll `INFO` on a replica until `master_link_status:up` appears (data-plane
/// PSYNC streaming established) or `deadline` elapses. Returns whether it came up.
fn wait_link_up(replica_addr: &str, deadline: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        let info = send_cmd(replica_addr, "INFO replication");
        if info.contains("master_link_status:up") {
            return true;
        }
        thread::sleep(Duration::from_millis(200));
    }
    false
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::*;

    /// REPL-01: Partial resync after replica reconnect within backlog window.
    #[test]
    #[ignore]
    fn partial_resync_within_backlog() {
        let master_dir = tempfile::tempdir().unwrap();
        let replica_dir = tempfile::tempdir().unwrap();

        let _master = start_moon(16600, master_dir.path().to_str().unwrap(), &[]);
        thread::sleep(Duration::from_millis(500));

        // Write initial data
        write_keys("127.0.0.1:16600", "repl", 100);

        // Start replica
        let mut replica = start_moon(16601, replica_dir.path().to_str().unwrap(), &[]);
        thread::sleep(Duration::from_millis(500));

        // Configure replication
        send_cmd("127.0.0.1:16601", "REPLICAOF 127.0.0.1 16600");
        thread::sleep(Duration::from_secs(2));

        // Verify initial sync
        let replica_size = dbsize("127.0.0.1:16601");
        assert!(
            replica_size >= 90,
            "Replica should have most keys after initial sync, got {}",
            replica_size
        );

        // Kill replica
        // SAFETY: `child.id()` returns a valid PID for a process we just spawned.
        // SIGKILL is always valid. We check the return code for robustness.
        let ret = unsafe { libc::kill(replica.0.id() as i32, libc::SIGKILL) };
        assert_eq!(ret, 0, "libc::kill failed");
        let _ = replica.0.wait();

        // Write more data while replica is down (within backlog)
        write_keys("127.0.0.1:16600", "new", 50);

        // Restart replica — should partial resync
        let _replica2 = start_moon(16601, replica_dir.path().to_str().unwrap(), &[]);
        thread::sleep(Duration::from_millis(500));
        send_cmd("127.0.0.1:16601", "REPLICAOF 127.0.0.1 16600");
        thread::sleep(Duration::from_secs(3));

        let final_size = dbsize("127.0.0.1:16601");
        let master_size = dbsize("127.0.0.1:16600");

        assert_eq!(
            final_size, master_size,
            "Replica should match master after partial resync: replica={}, master={}",
            final_size, master_size
        );
    }

    /// REPL-04: Replica kill-9 + restart yields data parity vs master.
    #[test]
    #[ignore]
    fn replica_kill_restart_parity() {
        let master_dir = tempfile::tempdir().unwrap();
        let replica_dir = tempfile::tempdir().unwrap();

        let _master = start_moon(16610, master_dir.path().to_str().unwrap(), &[]);
        thread::sleep(Duration::from_millis(500));

        write_keys("127.0.0.1:16610", "kill_test", 200);

        let mut replica = start_moon(16611, replica_dir.path().to_str().unwrap(), &[]);
        thread::sleep(Duration::from_millis(500));
        send_cmd("127.0.0.1:16611", "REPLICAOF 127.0.0.1 16610");
        thread::sleep(Duration::from_secs(3));

        // Kill replica with SIGKILL
        // SAFETY: `child.id()` returns a valid PID for a process we just spawned.
        // SIGKILL is always valid. We check the return code for robustness.
        let ret = unsafe { libc::kill(replica.0.id() as i32, libc::SIGKILL) };
        assert_eq!(ret, 0, "libc::kill failed");
        let _ = replica.0.wait();

        // Write more data
        write_keys("127.0.0.1:16610", "post_kill", 100);

        // Restart replica
        let _replica2 = start_moon(16611, replica_dir.path().to_str().unwrap(), &[]);
        thread::sleep(Duration::from_millis(500));
        send_cmd("127.0.0.1:16611", "REPLICAOF 127.0.0.1 16610");
        thread::sleep(Duration::from_secs(3));

        let master_size = dbsize("127.0.0.1:16610");
        let replica_size = dbsize("127.0.0.1:16611");

        assert_eq!(
            replica_size, master_size,
            "Replica should match master after kill-restart: replica={}, master={}",
            replica_size, master_size
        );
    }

    /// REPL-06: Replica promotion via REPLICAOF NO ONE.
    #[test]
    #[ignore]
    fn replica_promotion() {
        let master_dir = tempfile::tempdir().unwrap();
        let replica_dir = tempfile::tempdir().unwrap();

        let _master = start_moon(16620, master_dir.path().to_str().unwrap(), &[]);
        thread::sleep(Duration::from_millis(500));

        write_keys("127.0.0.1:16620", "promo", 100);

        let _replica = start_moon(16621, replica_dir.path().to_str().unwrap(), &[]);
        thread::sleep(Duration::from_millis(500));
        send_cmd("127.0.0.1:16621", "REPLICAOF 127.0.0.1 16620");
        thread::sleep(Duration::from_secs(2));

        // Promote replica
        let result = send_cmd("127.0.0.1:16621", "REPLICAOF NO ONE");
        assert!(
            result.contains("+OK"),
            "REPLICAOF NO ONE should return OK, got: {}",
            result.trim()
        );

        // Verify promoted replica accepts writes
        send_cmd("127.0.0.1:16621", "SET promoted_key promoted_value");
        let get_result = send_cmd("127.0.0.1:16621", "GET promoted_key");
        assert!(
            get_result.contains("promoted_value"),
            "Promoted replica should accept writes"
        );
    }

    /// G17: Full resync when replica offset falls outside backlog window.
    ///
    /// Uses a tiny backlog so that writes during disconnect overflow it,
    /// forcing a full resync on reconnect.
    #[test]
    #[ignore]
    fn full_resync_outside_backlog() {
        let master_dir = tempfile::tempdir().unwrap();
        let replica_dir = tempfile::tempdir().unwrap();

        // Start master with a tiny replication backlog (1KB)
        let _master = start_moon(
            16630,
            master_dir.path().to_str().unwrap(),
            &["--repl-backlog-size", "1024"],
        );
        thread::sleep(Duration::from_millis(500));

        // Write initial data
        write_keys("127.0.0.1:16630", "fullresync", 100);

        // Start replica and sync
        let mut replica = start_moon(16631, replica_dir.path().to_str().unwrap(), &[]);
        thread::sleep(Duration::from_millis(500));
        send_cmd("127.0.0.1:16631", "REPLICAOF 127.0.0.1 16630");
        thread::sleep(Duration::from_secs(2));

        let synced = dbsize("127.0.0.1:16631");
        assert!(
            synced >= 90,
            "Replica should have most keys after initial sync, got {}",
            synced
        );

        // Disconnect replica
        // SAFETY: `child.id()` returns a valid PID for a process we just spawned.
        // SIGKILL is always valid. We check the return code for robustness.
        let ret = unsafe { libc::kill(replica.0.id() as i32, libc::SIGKILL) };
        assert_eq!(ret, 0, "libc::kill failed");
        let _ = replica.0.wait();

        // Write enough to overflow the 1KB backlog
        write_keys("127.0.0.1:16630", "overflow", 500);

        // Reconnect replica — should trigger full resync
        let _replica2 = start_moon(16631, replica_dir.path().to_str().unwrap(), &[]);
        thread::sleep(Duration::from_millis(500));
        send_cmd("127.0.0.1:16631", "REPLICAOF 127.0.0.1 16630");
        thread::sleep(Duration::from_secs(4));

        let master_size = dbsize("127.0.0.1:16630");
        let replica_size = dbsize("127.0.0.1:16631");

        assert_eq!(
            replica_size, master_size,
            "Replica should match master after full resync: replica={}, master={}",
            replica_size, master_size
        );
    }

    /// G18: Network partition recovery via REPLICAOF NO ONE / REPLICAOF <master>.
    ///
    /// Simulates a network partition by detaching the replica, writing to master
    /// during the "partition", then re-attaching. Verifies the replica catches up.
    #[test]
    #[ignore]
    fn network_partition_recovery() {
        let master_dir = tempfile::tempdir().unwrap();
        let replica_dir = tempfile::tempdir().unwrap();

        let _master = start_moon(16640, master_dir.path().to_str().unwrap(), &[]);
        thread::sleep(Duration::from_millis(500));

        // Initial data
        write_keys("127.0.0.1:16640", "partition", 100);

        let _replica = start_moon(16641, replica_dir.path().to_str().unwrap(), &[]);
        thread::sleep(Duration::from_millis(500));
        send_cmd("127.0.0.1:16641", "REPLICAOF 127.0.0.1 16640");
        thread::sleep(Duration::from_secs(2));

        let synced = dbsize("127.0.0.1:16641");
        assert!(
            synced >= 90,
            "Replica should sync initial data, got {}",
            synced
        );

        // Simulate partition: detach replica
        send_cmd("127.0.0.1:16641", "REPLICAOF NO ONE");
        thread::sleep(Duration::from_millis(500));

        // Write to master during "partition"
        write_keys("127.0.0.1:16640", "during_partition", 200);

        // Restore connection: re-attach replica to master
        send_cmd("127.0.0.1:16641", "REPLICAOF 127.0.0.1 16640");
        thread::sleep(Duration::from_secs(3));

        let master_size = dbsize("127.0.0.1:16640");
        let replica_size = dbsize("127.0.0.1:16641");

        assert_eq!(
            replica_size, master_size,
            "Replica should catch up after partition: replica={}, master={}",
            replica_size, master_size
        );
    }

    /// task #67 (P0, v0.7.0 soak): master kill-9 + restart must not wedge
    /// `REPLCONF ACK` registration on the master side. The soak harness
    /// (PR #327, `scripts/soak-replication-24h.sh`) reproduced this twice:
    /// after killing and restarting the master on the same port/dir, the
    /// replica's reconnect + data-plane streaming recovers fine, but
    /// `WAIT 1 <timeout>` on the restarted master times out for minutes —
    /// the replica keeps sending `REPLCONF ACK`, but the master stops
    /// recording them — until the *replica* process itself is restarted.
    ///
    /// Matches the soak's shard topology: a MULTI-shard master
    /// (`--master-shards 4` default) against a single-shard replica —
    /// `handle_psync_inline_multi_shard` is a materially different code path
    /// from the single-shard `handle_psync_inline_single_shard` (always full
    /// resync; per-shard offset capture instead of the seeded master-axis
    /// total), so a single-shard repro does not exercise the same code.
    ///
    /// This test asserts the CONTROL plane (WAIT/ACK) specifically, not just
    /// data parity: a data-only assertion (as in `replica_kill_restart_parity`)
    /// would pass even with this bug, since the write still arrives — WAIT is
    /// what actually wedges.
    #[test]
    #[ignore]
    fn master_kill_restart_wait_acks() {
        let master_dir = tempfile::tempdir().unwrap();
        let replica_dir = tempfile::tempdir().unwrap();
        let master_port = 16650u16;
        let replica_port = 16651u16;
        let master_addr = format!("127.0.0.1:{master_port}");
        let replica_addr = format!("127.0.0.1:{replica_port}");

        let mut master =
            start_moon_shards(master_port, master_dir.path().to_str().unwrap(), 4, &[]);
        thread::sleep(Duration::from_millis(500));

        // Some pre-crash write history so the restarted master's AOF replay
        // seeds a non-trivial master_repl_offset (RFC §2 Rule 3) — the
        // realistic soak scenario, not a freshly-booted empty master.
        write_keys(&master_addr, "pre_kill", 50);

        let _replica = start_moon(replica_port, replica_dir.path().to_str().unwrap(), &[]);
        thread::sleep(Duration::from_millis(500));
        send_cmd(&replica_addr, &format!("REPLICAOF 127.0.0.1 {master_port}"));
        assert!(
            wait_link_up(&replica_addr, Duration::from_secs(15)),
            "initial sync must complete before the test proceeds"
        );

        // Sanity: WAIT resolves before any restart.
        send_cmd(&master_addr, "SET sanity_key sanity_value");
        let acked = wait_cmd(&master_addr, 1, 2000);
        assert_eq!(acked, 1, "WAIT must succeed before master restart (sanity)");

        // Kill -9 the MASTER, then restart it on the SAME port + SAME dir
        // (AOF-backed recovery — appendonly defaults to yes).
        // SAFETY: `child.id()` returns a valid PID for a process we just
        // spawned; SIGKILL is always a valid signal.
        let ret = unsafe { libc::kill(master.0.id() as i32, libc::SIGKILL) };
        assert_eq!(ret, 0, "libc::kill failed");
        let _ = master.0.wait();

        let mut master2 =
            start_moon_shards(master_port, master_dir.path().to_str().unwrap(), 4, &[]);
        thread::sleep(Duration::from_millis(700));

        // Data plane: the replica's reconnect loop must re-establish PSYNC
        // streaming against the restarted master.
        assert!(
            wait_link_up(&replica_addr, Duration::from_secs(30)),
            "replica must reconnect to the restarted master"
        );

        // Confirm the data plane is actually flowing post-restart.
        send_cmd(&master_addr, "SET post_restart_key post_restart_value");
        let mut replicated = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        while std::time::Instant::now() < deadline {
            let v = send_cmd(&replica_addr, "GET post_restart_key");
            if v.contains("post_restart_value") {
                replicated = true;
                break;
            }
            thread::sleep(Duration::from_millis(200));
        }
        assert!(
            replicated,
            "data plane must keep replicating after master restart"
        );

        // Control plane: WAIT must resolve to 1 within a bounded timeout —
        // this is what task #67 says wedges for minutes.
        let acked = wait_cmd(&master_addr, 1, 5000);
        assert_eq!(
            acked,
            1,
            "task #67: WAIT 1 must succeed after master kill-9+restart — \
             REPLCONF ACK stopped registering on the restarted master \
             (replica ACKs: {:?})",
            send_cmd(&master_addr, "INFO replication")
        );

        let _ = master2.0.kill();
        let _ = master2.0.wait();
    }
}
