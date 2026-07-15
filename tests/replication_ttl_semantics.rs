//! v0.7.1 (#71): replica TTL semantics — relative-expiry commands are rewritten
//! to absolute deadlines before replication (#71a), and a replica does not run
//! its own active-expiry deletion sweep (#71b): it converges to the master's
//! authoritative expiry decision.
//!
//! Black-box tests over two real `moon` processes. `#[ignore]`d like the other
//! replication suites — they need a prebuilt binary:
//!
//! ```text
//! MOON_BIN=./target/release/moon \
//!   cargo test --test replication_ttl_semantics -- --ignored --nocapture
//! ```
//!
//! **#71a discriminator:** a replica applying a *relative* `EXPIRE k 100000`
//! verbatim would restart the countdown at apply time, so its `PEXPIRETIME`
//! (absolute expiry, ms) would differ from the master's by the apply delay.
//! With the master-side rewrite to `PEXPIREAT k <abs>`, both sides carry the
//! **identical** absolute deadline — asserted here by exact equality.

use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

fn moon_bin() -> String {
    std::env::var("MOON_BIN").unwrap_or_else(|_| "./target/release/moon".to_string())
}

fn start_moon(port: u16, dir: &str) -> Child {
    Command::new(moon_bin())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--dir",
            dir,
            "--appendonly",
            "no",
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to start moon (set MOON_BIN to a built binary)")
}

/// Send one inline command and return the raw reply (one logical RESP reply).
fn send_cmd(addr: &str, cmd: &str) -> String {
    let Ok(mut stream) = TcpStream::connect(addr) else {
        return String::new();
    };
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    stream
        .write_all(format!("{}\r\n", cmd).as_bytes())
        .expect("write");
    stream.flush().ok();

    let mut reader = BufReader::new(&stream);
    let mut out = String::new();
    let mut line = String::new();
    use std::io::BufRead;
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                let trimmed = line.trim_end_matches("\r\n").trim_end_matches('\n');
                out.push_str(trimmed);
                out.push('\n');
                if trimmed.starts_with('+') || trimmed.starts_with('-') || trimmed.starts_with(':')
                {
                    break;
                }
                if let Some(rest) = trimmed.strip_prefix('$') {
                    let len: i64 = rest.trim().parse().unwrap_or(-1);
                    if len < 0 {
                        break; // $-1 nil
                    }
                    let mut buf = vec![0u8; (len as usize) + 2];
                    if reader.read_exact(&mut buf).is_ok() {
                        out.push_str(&String::from_utf8_lossy(&buf[..len as usize]));
                        out.push('\n');
                    }
                    break;
                }
            }
        }
    }
    out
}

/// Parse the integer reply (`:<n>`) of a command like `PEXPIRETIME`/`DBSIZE`.
fn send_int(addr: &str, cmd: &str) -> Option<i64> {
    let reply = send_cmd(addr, cmd);
    reply.lines().find_map(|l| {
        l.strip_prefix(':')
            .and_then(|n| n.trim().parse::<i64>().ok())
    })
}

fn get(addr: &str, key: &str) -> Option<String> {
    let reply = send_cmd(addr, &format!("GET {}", key));
    let mut lines = reply.lines();
    match lines.next() {
        Some(h) if h.starts_with('$') => {
            if h.starts_with("$-1") {
                None
            } else {
                lines.next().map(|s| s.to_string())
            }
        }
        _ => None,
    }
}

fn wait_until<F: Fn() -> bool>(timeout: Duration, f: F) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if f() {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    f()
}

struct Killer(Child);
impl Drop for Killer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn ready(addr: &str) -> bool {
    send_cmd(addr, "PING").starts_with("+PONG")
}

/// #71a: `EXPIRE`/`SETEX`/`SET ... EX` (relative) replicate as an absolute
/// deadline, so the replica's `PEXPIRETIME` exactly equals the master's.
#[test]
#[ignore = "needs prebuilt MOON_BIN; run with --ignored"]
fn relative_ttl_replicates_as_identical_absolute_deadline() {
    let master_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let master_addr = "127.0.0.1:16760";
    let replica_addr = "127.0.0.1:16761";

    let _master = Killer(start_moon(16760, master_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || ready(master_addr)),
        "master never became ready"
    );
    let _replica = Killer(start_moon(16761, replica_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || ready(replica_addr)),
        "replica never became ready"
    );
    send_cmd(replica_addr, "REPLICAOF 127.0.0.1 16760");
    assert!(
        wait_until(Duration::from_secs(10), || send_cmd(
            replica_addr,
            "INFO replication"
        )
        .contains("master_link_status:up")),
        "replica link never came up"
    );

    // Three relative-TTL forms; a big TTL keeps the key alive for the whole test.
    send_cmd(master_addr, "SET k1 v1");
    send_cmd(master_addr, "EXPIRE k1 100000"); // EXPIRE (seconds)
    send_cmd(master_addr, "SETEX k2 100000 v2"); // SETEX
    send_cmd(master_addr, "SET k3 v3 EX 100000"); // SET ... EX

    for key in ["k1", "k2", "k3"] {
        assert!(
            wait_until(Duration::from_secs(10), || get(replica_addr, key).is_some()),
            "replica never received {key}"
        );
        let master_pt = send_int(master_addr, &format!("PEXPIRETIME {}", key));
        let replica_pt = send_int(replica_addr, &format!("PEXPIRETIME {}", key));
        assert!(
            master_pt.is_some() && master_pt.unwrap() > 0,
            "master {key} has no absolute expiry: {master_pt:?}"
        );
        assert_eq!(
            master_pt, replica_pt,
            "{key}: replica absolute deadline must EXACTLY match master's \
             (verbatim relative replication would drift by the apply delay). \
             master={master_pt:?} replica={replica_pt:?}"
        );
    }
}

/// #71a + #71b: a relative TTL that actually fires — the key expires at the
/// same absolute instant on both nodes, the master streams the authoritative
/// removal, and the replica converges (it does not need its own sweep, and does
/// not diverge by expiring early or late).
#[test]
#[ignore = "needs prebuilt MOON_BIN; run with --ignored"]
fn expiring_key_converges_via_master_decision() {
    let master_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let master_addr = "127.0.0.1:16762";
    let replica_addr = "127.0.0.1:16763";

    let _master = Killer(start_moon(16762, master_dir.path().to_str().unwrap()));
    assert!(wait_until(Duration::from_secs(5), || ready(master_addr)));
    let _replica = Killer(start_moon(16763, replica_dir.path().to_str().unwrap()));
    assert!(wait_until(Duration::from_secs(5), || ready(replica_addr)));
    send_cmd(replica_addr, "REPLICAOF 127.0.0.1 16762");
    assert!(
        wait_until(Duration::from_secs(10), || send_cmd(
            replica_addr,
            "INFO replication"
        )
        .contains("master_link_status:up")),
        "replica link never came up"
    );

    // A key that lives long enough to reach the replica, then expires.
    send_cmd(master_addr, "SET doomed v PX 1500");
    assert!(
        wait_until(Duration::from_secs(5), || get(replica_addr, "doomed")
            .is_some()),
        "replica never received the key before expiry"
    );
    // Both carry the same absolute deadline.
    assert_eq!(
        send_int(master_addr, "PEXPIRETIME doomed"),
        send_int(replica_addr, "PEXPIRETIME doomed"),
        "absolute deadline diverged between master and replica"
    );

    // After the deadline, the master expires it and streams the removal; the
    // replica converges to the key being gone (logical + physical).
    assert!(
        wait_until(Duration::from_secs(8), || get(replica_addr, "doomed")
            .is_none()),
        "replica never converged to the master's expiry decision"
    );
    // Master is also gone — no divergence.
    assert!(
        get(master_addr, "doomed").is_none(),
        "master should have expired the key"
    );
}
