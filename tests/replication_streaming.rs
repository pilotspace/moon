//! R0 acceptance: single-shard streaming replication actually APPLIES the
//! master's snapshot AND its live command stream on the replica.
//!
//! These are black-box tests over two real `moon` processes. They are
//! `#[ignore]`d (like `replication_hardening.rs`) because they need a prebuilt
//! binary; run them explicitly:
//!
//! ```text
//! MOON_BIN=./target/release/moon \
//!   cargo test --test replication_streaming -- --ignored --nocapture
//! ```
//!
//! Before R0 the replica discarded both the FULLRESYNC snapshot and the live
//! stream (`buf.clear()`), so a freshly-attached replica reported `DBSIZE 0`.
//! These tests lock in the fix.

use std::io::{BufRead, BufReader, Read, Write};
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
            // /Volumes/Games hovers near the 5% diskfull guard; disable it so a
            // low-free-space dev host does not turn writes into MOONERR diskfull.
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

/// Read exactly one RESP reply from `reader`, returned as text (bulk bodies on
/// their own line; `$-1` nil yields an empty trailing line).
fn read_one_reply<R: BufRead>(reader: &mut R) -> String {
    let mut out = String::new();
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                let trimmed = line.trim_end_matches("\r\n").trim_end_matches('\n');
                if trimmed.starts_with('+') || trimmed.starts_with('-') || trimmed.starts_with(':')
                {
                    out.push_str(trimmed);
                    break;
                }
                if let Some(rest) = trimmed.strip_prefix('$') {
                    let len: i64 = rest.trim().parse().unwrap_or(-1);
                    if len < 0 {
                        break;
                    }
                    let mut buf = vec![0u8; (len as usize) + 2];
                    if reader.read_exact(&mut buf).is_ok() {
                        out.push_str(&String::from_utf8_lossy(&buf[..len as usize]));
                    }
                    break;
                }
                // Ignore array/other headers for these simple sequences.
            }
        }
    }
    out
}

/// Run a sequence of commands on ONE connection (so `SELECT` persists) and
/// return the LAST reply as text.
fn send_seq(addr: &str, cmds: &[&str]) -> String {
    let Ok(mut stream) = TcpStream::connect(addr) else {
        return String::new();
    };
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    for c in cmds {
        if stream.write_all(format!("{}\r\n", c).as_bytes()).is_err() {
            return String::new();
        }
    }
    stream.flush().ok();
    let mut reader = BufReader::new(&stream);
    let mut last = String::new();
    for _ in 0..cmds.len() {
        last = read_one_reply(&mut reader);
    }
    last
}

/// GET `key` in logical db `db` (SELECT + GET on one connection).
fn get_in_db(addr: &str, db: usize, key: &str) -> Option<String> {
    let v = send_seq(addr, &[&format!("SELECT {}", db), &format!("GET {}", key)]);
    let v = v.trim().to_string();
    if v.is_empty() { None } else { Some(v) }
}

fn dbsize(addr: &str) -> i64 {
    let resp = send_cmd(addr, "DBSIZE");
    resp.trim()
        .trim_start_matches(':')
        .trim()
        .parse()
        .unwrap_or(-1)
}

fn get(addr: &str, key: &str) -> Option<String> {
    let resp = send_cmd(addr, &format!("GET {}", key));
    let v = resp.lines().nth(1).map(|s| s.to_string());
    v.filter(|s| !s.is_empty())
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

/// REPL-STREAM-01: replica applies the initial snapshot AND subsequent live
/// writes streamed from the master.
#[test]
#[ignore]
fn replica_applies_snapshot_and_live_stream() {
    let master_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();

    let master_addr = "127.0.0.1:16700";
    let replica_addr = "127.0.0.1:16701";

    let _master = Killer(start_moon(16700, master_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(master_addr, "PING")
            .starts_with("+PONG")),
        "master never became ready"
    );

    // Pre-connect state → must arrive via the FULLRESYNC snapshot.
    send_cmd(master_addr, "SET before1 alpha");
    send_cmd(master_addr, "SET before2 beta");

    let _replica = Killer(start_moon(16701, replica_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(replica_addr, "PING")
            .starts_with("+PONG")),
        "replica never became ready"
    );

    send_cmd(replica_addr, &format!("REPLICAOF 127.0.0.1 {}", 16700));

    // Snapshot must land: both pre-connect keys visible on the replica.
    let synced = wait_until(Duration::from_secs(10), || dbsize(replica_addr) >= 2);
    assert!(
        synced,
        "replica did not load the FULLRESYNC snapshot (dbsize={})",
        dbsize(replica_addr)
    );
    assert_eq!(get(replica_addr, "before1").as_deref(), Some("alpha"));
    assert_eq!(get(replica_addr, "before2").as_deref(), Some("beta"));

    // Post-connect write → must arrive via the LIVE stream.
    send_cmd(master_addr, "SET after1 gamma");
    let streamed = wait_until(Duration::from_secs(10), || {
        get(replica_addr, "after1").as_deref() == Some("gamma")
    });
    assert!(
        streamed,
        "replica did not apply the live-streamed write (after1={:?})",
        get(replica_addr, "after1")
    );

    // A live DEL must also propagate (delete, not just set).
    send_cmd(master_addr, "DEL before1");
    let deleted = wait_until(Duration::from_secs(10), || {
        get(replica_addr, "before1").is_none()
    });
    assert!(deleted, "replica did not apply the live-streamed DEL");

    // Confirm the replica reports replica role while still attached.
    let info = send_cmd(replica_addr, "INFO replication");
    assert!(
        info.contains("role:slave") || info.contains("role:replica"),
        "replica INFO should report replica role, got:\n{}",
        info
    );

    // MOVE and cross-db COPY are intercepted BEFORE generic dispatch on the
    // master (two-db path); the replica must mirror that intercept or they
    // silently diverge (generic dispatch errors on MOVE / mis-targets COPY..DB).
    // Their destination db is a command ARGUMENT (self-describing), so they
    // replicate even in db0-scoped R0.
    send_cmd(master_addr, "SET mvkey moved");
    send_cmd(master_addr, "MOVE mvkey 1"); // db0 -> db1
    send_cmd(master_addr, "SET cpkey orig");
    send_cmd(master_addr, "COPY cpkey cpkey2 DB 1");
    send_cmd(master_addr, "SET twodb_done 1"); // in-order stream sentinel (db0)

    // The stream is applied in order, so once the sentinel is visible on the
    // replica, MOVE + COPY have already been applied. (A bare is_none() check on
    // mvkey would false-pass immediately, since mvkey starts absent.)
    let sentinel = wait_until(Duration::from_secs(10), || {
        get(replica_addr, "twodb_done").as_deref() == Some("1")
    });
    assert!(sentinel, "two-db ordering sentinel never replicated");
    // MOVE's db0 side-effect (key left db0) is observable WITHOUT SELECT —
    // SELECT is (wrongly) rejected on a read-only replica, task #23.
    assert!(
        get(replica_addr, "mvkey").is_none(),
        "MOVE did not remove mvkey from replica db0"
    );
    // COPY leaves the source key in db0.
    assert_eq!(get(replica_addr, "cpkey").as_deref(), Some("orig"));

    // Promote the replica so the read-only guard lifts and SELECT works — then
    // confirm both two-db writes landed in db1.
    assert!(
        send_cmd(replica_addr, "REPLICAOF NO ONE").starts_with("+OK"),
        "REPLICAOF NO ONE should succeed"
    );
    thread::sleep(Duration::from_millis(300));
    assert_eq!(
        get_in_db(replica_addr, 1, "mvkey").as_deref(),
        Some("moved"),
        "MOVE did not land in replica db1"
    );
    assert_eq!(
        get_in_db(replica_addr, 1, "cpkey2").as_deref(),
        Some("orig"),
        "cross-db COPY did not land in replica db1"
    );
}
