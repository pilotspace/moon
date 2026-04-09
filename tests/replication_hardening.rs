//! Replication hardening tests for PSYNC2.
//!
//! Tests partial resync, full resync, network partition recovery,
//! replica kill-restart, and replica promotion paths.
//!
//! Run: cargo test --test replication_hardening -- --ignored
//! Requires: built moon binary at ./target/release/moon

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

fn start_moon(port: u16, dir: &str, extra: &[&str]) -> std::process::Child {
    Command::new("./target/release/moon")
        .args(
            &[
                &["--port", &port.to_string(), "--shards", "1", "--dir", dir][..],
                extra,
            ]
            .concat(),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to start moon")
}

fn send_cmd(addr: &str, cmd: &str) -> String {
    let Ok(mut stream) = TcpStream::connect(addr) else {
        return String::new();
    };
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    stream
        .write_all(format!("{}\r\n", cmd).as_bytes())
        .expect("write");
    stream.flush().ok();

    let reader = BufReader::new(&stream);
    let mut resp = String::new();
    for line in reader.lines() {
        match line {
            Ok(l) => {
                resp.push_str(&l);
                resp.push('\n');
                if l.starts_with('+') || l.starts_with('-') || l.starts_with(':') {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    resp
}

fn dbsize(addr: &str) -> i64 {
    send_cmd(addr, "DBSIZE")
        .trim()
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

#[cfg(test)]
mod tests {
    use super::*;

    /// REPL-01: Partial resync after replica reconnect within backlog window.
    #[test]
    #[ignore]
    fn partial_resync_within_backlog() {
        let master_dir = tempfile::tempdir().unwrap();
        let replica_dir = tempfile::tempdir().unwrap();

        let mut master = start_moon(16600, master_dir.path().to_str().unwrap(), &[]);
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
        unsafe { libc::kill(replica.id() as i32, libc::SIGKILL) };
        let _ = replica.wait();

        // Write more data while replica is down (within backlog)
        write_keys("127.0.0.1:16600", "new", 50);

        // Restart replica — should partial resync
        let mut replica2 = start_moon(16601, replica_dir.path().to_str().unwrap(), &[]);
        thread::sleep(Duration::from_millis(500));
        send_cmd("127.0.0.1:16601", "REPLICAOF 127.0.0.1 16600");
        thread::sleep(Duration::from_secs(3));

        let final_size = dbsize("127.0.0.1:16601");
        let master_size = dbsize("127.0.0.1:16600");

        // Cleanup
        send_cmd("127.0.0.1:16600", "SHUTDOWN NOSAVE");
        send_cmd("127.0.0.1:16601", "SHUTDOWN NOSAVE");
        let _ = master.wait();
        let _ = replica2.wait();

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

        let mut master = start_moon(16610, master_dir.path().to_str().unwrap(), &[]);
        thread::sleep(Duration::from_millis(500));

        write_keys("127.0.0.1:16610", "kill_test", 200);

        let mut replica = start_moon(16611, replica_dir.path().to_str().unwrap(), &[]);
        thread::sleep(Duration::from_millis(500));
        send_cmd("127.0.0.1:16611", "REPLICAOF 127.0.0.1 16610");
        thread::sleep(Duration::from_secs(3));

        // Kill replica with SIGKILL
        unsafe { libc::kill(replica.id() as i32, libc::SIGKILL) };
        let _ = replica.wait();

        // Write more data
        write_keys("127.0.0.1:16610", "post_kill", 100);

        // Restart replica
        let mut replica2 = start_moon(16611, replica_dir.path().to_str().unwrap(), &[]);
        thread::sleep(Duration::from_millis(500));
        send_cmd("127.0.0.1:16611", "REPLICAOF 127.0.0.1 16610");
        thread::sleep(Duration::from_secs(3));

        let master_size = dbsize("127.0.0.1:16610");
        let replica_size = dbsize("127.0.0.1:16611");

        send_cmd("127.0.0.1:16610", "SHUTDOWN NOSAVE");
        send_cmd("127.0.0.1:16611", "SHUTDOWN NOSAVE");
        let _ = master.wait();
        let _ = replica2.wait();

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

        let mut master = start_moon(16620, master_dir.path().to_str().unwrap(), &[]);
        thread::sleep(Duration::from_millis(500));

        write_keys("127.0.0.1:16620", "promo", 100);

        let mut replica = start_moon(16621, replica_dir.path().to_str().unwrap(), &[]);
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

        send_cmd("127.0.0.1:16620", "SHUTDOWN NOSAVE");
        send_cmd("127.0.0.1:16621", "SHUTDOWN NOSAVE");
        let _ = master.wait();
        let _ = replica.wait();
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
        let mut master = start_moon(
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
        unsafe { libc::kill(replica.id() as i32, libc::SIGKILL) };
        let _ = replica.wait();

        // Write enough to overflow the 1KB backlog
        write_keys("127.0.0.1:16630", "overflow", 500);

        // Reconnect replica — should trigger full resync
        let mut replica2 = start_moon(16631, replica_dir.path().to_str().unwrap(), &[]);
        thread::sleep(Duration::from_millis(500));
        send_cmd("127.0.0.1:16631", "REPLICAOF 127.0.0.1 16630");
        thread::sleep(Duration::from_secs(4));

        let master_size = dbsize("127.0.0.1:16630");
        let replica_size = dbsize("127.0.0.1:16631");

        // Cleanup
        send_cmd("127.0.0.1:16630", "SHUTDOWN NOSAVE");
        send_cmd("127.0.0.1:16631", "SHUTDOWN NOSAVE");
        let _ = master.wait();
        let _ = replica2.wait();

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

        let mut master = start_moon(16640, master_dir.path().to_str().unwrap(), &[]);
        thread::sleep(Duration::from_millis(500));

        // Initial data
        write_keys("127.0.0.1:16640", "partition", 100);

        let mut replica = start_moon(16641, replica_dir.path().to_str().unwrap(), &[]);
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

        // Cleanup
        send_cmd("127.0.0.1:16640", "SHUTDOWN NOSAVE");
        send_cmd("127.0.0.1:16641", "SHUTDOWN NOSAVE");
        let _ = master.wait();
        let _ = replica.wait();

        assert_eq!(
            replica_size, master_size,
            "Replica should catch up after partition: replica={}, master={}",
            replica_size, master_size
        );
    }
}
