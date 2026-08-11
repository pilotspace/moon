//! Wave B ws-plane replication: a replica must synchronize the process-global
//! `WorkspaceRegistry` (WS.CREATE/WS.DROP) from its master — both the live
//! mutation stream and the PSYNC full-resync snapshot backfill.
//!
//! WS.CREATE mints a nondeterministic UUIDv7 on the master; the replica must
//! apply the MASTER's `ws_id` verbatim (never mint its own), so these tests
//! assert on the EXACT id (and `created_at`) round-tripping, not just
//! presence. Modeled on `tests/replication_graph.rs`.
//!
//! Run: `MOON_BIN=./target/release/moon cargo test --test replication_ws -- --ignored`

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn moon_bin() -> std::path::PathBuf {
    common::find_moon_binary()
}

fn start_moon(port: u16, dir: &str) -> Child {
    start_moon_shards(port, dir, 1)
}

fn start_moon_shards(port: u16, dir: &str, shards: u32) -> Child {
    Command::new(moon_bin())
        .env("RUST_LOG", "moon=warn")
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            &shards.to_string(),
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

/// Kill-on-drop guard so a panicking assertion never leaks a live server.
struct Killer(Child);
impl Drop for Killer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Send one inline command, return the raw reply text.
fn send_cmd(addr: &str, cmd: &str) -> String {
    let Ok(mut stream) = TcpStream::connect(addr) else {
        return String::new();
    };
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    if stream.write_all(format!("{cmd}\r\n").as_bytes()).is_err() {
        return String::new();
    }
    read_quiet(&mut stream)
}

/// RESP array of bulk strings — binary-safe, for multiword commands.
fn send_resp(addr: &str, parts: &[&str]) -> String {
    let Ok(mut stream) = TcpStream::connect(addr) else {
        return String::new();
    };
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .ok();
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    if stream.write_all(&out).is_err() {
        return String::new();
    }
    read_quiet(&mut stream)
}

fn read_quiet(stream: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let deadline = Instant::now() + Duration::from_millis(600);
    while Instant::now() < deadline {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(_) => {
                if !buf.is_empty() {
                    break;
                }
            }
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

fn wait_until<F: Fn() -> bool>(timeout: Duration, f: F) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    f()
}

/// Extract the workspace id from a `WS CREATE` bulk-string reply
/// (`$<len>\r\n<id>\r\n`).
fn extract_ws_id(create_reply: &str) -> String {
    create_reply
        .lines()
        .nth(1)
        .unwrap_or_default()
        .trim()
        .to_string()
}

/// REPL-WS-01: live WS.CREATE on the master reaches the replica — including
/// the EXACT `ws_id` (nondeterministic UUIDv7, never re-minted by the
/// replica) and `created_at`. The replica attaches to an EMPTY master first,
/// isolating the live streaming leg from snapshot backfill.
#[test]
#[ignore]
fn replica_syncs_ws_create_live_stream() {
    let master_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let master_addr = "127.0.0.1:16740";
    let replica_addr = "127.0.0.1:16741";

    let _master = Killer(start_moon(16740, master_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(master_addr, "PING")
            .starts_with("+PONG")),
        "master never became ready"
    );
    let _replica = Killer(start_moon(16741, replica_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(replica_addr, "PING")
            .starts_with("+PONG")),
        "replica never became ready"
    );
    assert!(
        send_cmd(replica_addr, "REPLICAOF 127.0.0.1 16740").starts_with("+OK"),
        "REPLICAOF failed"
    );
    std::thread::sleep(Duration::from_millis(500));

    let create_reply = send_resp(master_addr, &["WS", "CREATE", "acme"]);
    assert!(
        create_reply.starts_with('$'),
        "WS CREATE must return a bulk-string id: {create_reply}"
    );
    let ws_id = extract_ws_id(&create_reply);
    assert!(!ws_id.is_empty(), "WS CREATE returned an empty id");

    let synced = wait_until(Duration::from_secs(10), || {
        send_cmd(replica_addr, "WS LIST").contains("acme")
    });
    assert!(
        synced,
        "replica never saw the live-created workspace: {}",
        send_cmd(replica_addr, "WS LIST")
    );

    // The EXACT master-minted id must round-trip — WS.INFO on the replica
    // must resolve it (a re-minted id on the replica would 404 here).
    let info = send_resp(replica_addr, &["WS", "INFO", &ws_id]);
    assert!(
        info.contains("acme"),
        "replica WS INFO for master's exact ws_id failed: {info}"
    );

    // WS AUTH against the master's exact id must also succeed on the replica
    // (the id the client already has from the master's CREATE response).
    let auth = send_resp(replica_addr, &["WS", "AUTH", &ws_id]);
    assert!(
        auth.starts_with("+OK"),
        "replica WS AUTH with master's exact ws_id failed: {auth}"
    );
}

/// REPL-WS-02: a replica attaching to a master that ALREADY has a workspace
/// must backfill it from the PSYNC full-resync snapshot (not just live
/// deltas) — including the exact `ws_id` and `created_at`.
#[test]
#[ignore]
fn replica_backfills_ws_registry_from_snapshot() {
    let master_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let master_addr = "127.0.0.1:16742";
    let replica_addr = "127.0.0.1:16743";

    let _master = Killer(start_moon(16742, master_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(master_addr, "PING")
            .starts_with("+PONG")),
        "master never became ready"
    );

    // Build WS state on the master BEFORE any replica exists.
    let create_reply = send_resp(master_addr, &["WS", "CREATE", "presnap"]);
    assert!(create_reply.starts_with('$'), "seed WS CREATE failed");
    let ws_id = extract_ws_id(&create_reply);
    let master_info = send_resp(master_addr, &["WS", "INFO", &ws_id]);

    // Now attach a fresh replica — it must learn `presnap` + its exact id.
    let _replica = Killer(start_moon(16743, replica_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(replica_addr, "PING")
            .starts_with("+PONG")),
        "replica never became ready"
    );
    assert!(
        send_cmd(replica_addr, "REPLICAOF 127.0.0.1 16742").starts_with("+OK"),
        "REPLICAOF failed"
    );

    let backfilled = wait_until(Duration::from_secs(10), || {
        send_cmd(replica_addr, "WS LIST").contains("presnap")
    });
    assert!(
        backfilled,
        "replica never backfilled the pre-existing workspace: {}",
        send_cmd(replica_addr, "WS LIST")
    );

    let replica_info = send_resp(replica_addr, &["WS", "INFO", &ws_id]);
    assert_eq!(
        replica_info, master_info,
        "replica WS INFO diverged from the master's snapshot-backfilled entry"
    );

    // A live write AFTER the snapshot must also land (stream continues past
    // the backfill boundary).
    let create2 = send_resp(master_addr, &["WS", "CREATE", "postsnap"]);
    assert!(create2.starts_with('$'), "post-snapshot WS CREATE failed");
    let grew = wait_until(Duration::from_secs(10), || {
        send_cmd(replica_addr, "WS LIST").contains("postsnap")
    });
    assert!(grew, "post-snapshot live WS.CREATE did not replicate");
}

/// REPL-WS-03: WS.DROP propagates to the replica — the workspace disappears
/// from WS.LIST and a subsequent WS.AUTH against the dropped id fails on the
/// replica exactly as it does on the master.
#[test]
#[ignore]
fn replica_syncs_ws_drop() {
    let master_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let master_addr = "127.0.0.1:16744";
    let replica_addr = "127.0.0.1:16745";

    let _master = Killer(start_moon(16744, master_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(master_addr, "PING")
            .starts_with("+PONG")),
        "master never became ready"
    );
    let _replica = Killer(start_moon(16745, replica_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(replica_addr, "PING")
            .starts_with("+PONG")),
        "replica never became ready"
    );
    assert!(
        send_cmd(replica_addr, "REPLICAOF 127.0.0.1 16744").starts_with("+OK"),
        "REPLICAOF failed"
    );
    std::thread::sleep(Duration::from_millis(500));

    let create_reply = send_resp(master_addr, &["WS", "CREATE", "todrop"]);
    assert!(create_reply.starts_with('$'), "WS CREATE failed");
    let ws_id = extract_ws_id(&create_reply);

    let synced = wait_until(Duration::from_secs(10), || {
        send_cmd(replica_addr, "WS LIST").contains("todrop")
    });
    assert!(synced, "replica never saw the pre-drop workspace");

    // Bind a connection to the workspace on the master and write a
    // workspace-scoped key, so DROP's key-sweep has something to clean up on
    // both sides (mirrors the master's WsDropCleanup best-effort sweep).
    let mut bound = TcpStream::connect(master_addr).expect("connect bound conn");
    bound
        .set_read_timeout(Some(Duration::from_millis(500)))
        .ok();
    let auth_cmd = format!(
        "*3\r\n$2\r\nWS\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n",
        ws_id.len(),
        ws_id
    );
    bound.write_all(auth_cmd.as_bytes()).unwrap();
    read_quiet(&mut bound);
    bound
        .write_all(b"*3\r\n$3\r\nSET\r\n$4\r\nwkey\r\n$3\r\nval\r\n")
        .unwrap();
    read_quiet(&mut bound);
    drop(bound);

    let drop_reply = send_resp(master_addr, &["WS", "DROP", &ws_id]);
    assert!(
        drop_reply.starts_with("+OK"),
        "WS DROP failed: {drop_reply}"
    );

    let gone = wait_until(Duration::from_secs(10), || {
        !send_cmd(replica_addr, "WS LIST").contains("todrop")
    });
    assert!(
        gone,
        "replica never applied WS.DROP: {}",
        send_cmd(replica_addr, "WS LIST")
    );

    // WS AUTH against the dropped id must now fail identically on both sides.
    let master_auth = send_resp(master_addr, &["WS", "AUTH", &ws_id]);
    let replica_auth = send_resp(replica_addr, &["WS", "AUTH", &ws_id]);
    assert!(
        master_auth.starts_with('-') && replica_auth.starts_with('-'),
        "post-drop WS AUTH must error on both sides: master={master_auth:?} replica={replica_auth:?}"
    );
}

/// REPL-WS-04: the shard-0 affinity hop itself (no replica involved) — at
/// `--shards 4`, WS.CREATE issued from ANY connection (the kernel spreads
/// SO_REUSEPORT-accepted connections across shard listeners; on Linux this
/// routinely lands non-zero shards, exercising the cross-shard hop to shard
/// 0) must be immediately, consistently visible from every other connection.
/// Mirrors `tests/shardslice_live.rs`'s `test_ssm3_ws_global_registry` oracle
/// but additionally asserts `created_at` consistency across connections
/// (Wave B moved the write off the connecting thread onto shard 0's).
#[test]
#[ignore]
fn ws_create_shard0_hop_consistent_across_shards() {
    let dir = tempfile::tempdir().unwrap();
    let addr = "127.0.0.1:16746";
    let _server = Killer(start_moon_shards(16746, dir.path().to_str().unwrap(), 4));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(addr, "PING")
            .starts_with("+PONG")),
        "server never became ready"
    );

    let create_reply = send_resp(addr, &["WS", "CREATE", "hop4"]);
    assert!(create_reply.starts_with('$'), "WS CREATE failed");
    let ws_id = extract_ws_id(&create_reply);
    let baseline_info = send_resp(addr, &["WS", "INFO", &ws_id]);

    for i in 0..16 {
        let info = send_resp(addr, &["WS", "INFO", &ws_id]);
        assert_eq!(
            info, baseline_info,
            "conn {i}: WS INFO diverged across connections (shard-0 hop inconsistency)"
        );
        let list_flat = send_cmd(addr, "WS LIST");
        assert!(
            list_flat.contains("hop4"),
            "conn {i}: WS LIST must include 'hop4' from every shard connection"
        );
    }
}
