//! v0.7 graph-plane replication: a replica must synchronize graph data
//! (nodes, edges, labels, properties) from its master — both the live
//! GRAPH.* mutation stream and the PSYNC full-resync snapshot backfill.
//!
//! Single-shard master scope (matches R0/R0.5). Graph mutations are streamed
//! as their DETERMINISTIC, id-pinned WAL records (GRAPH.ADDNODE <g> <id> …,
//! GRAPH.ADDEDGE <g> <eid> <src> <dst> …) — the master's WAL layer already
//! rewrites non-deterministic Cypher into these, so the replica replays them
//! into an identical graph without re-allocating ids.
//!
//! Run: `MOON_BIN=./target/release/moon cargo test --test replication_graph -- --ignored`

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn moon_bin() -> String {
    std::env::var("MOON_BIN").unwrap_or_else(|_| "./target/release/moon".to_string())
}

fn start_moon(port: u16, dir: &str) -> Child {
    start_moon_logged(port, dir, None)
}

/// Like `start_moon`, optionally teeing stderr (tracing output) to a file so
/// tests can assert stream-health properties — e.g. that the replica held ONE
/// live connection instead of masking a dead stream behind a 0.5s
/// reconnect/full-resync loop (the exact failure mode that hid the missing
/// self-SPSC producer for months).
fn start_moon_logged(port: u16, dir: &str, stderr_log: Option<&std::path::Path>) -> Child {
    let stderr = match stderr_log {
        Some(p) => Stdio::from(std::fs::File::create(p).expect("create log file")),
        None => Stdio::null(),
    };
    Command::new(moon_bin())
        .env("RUST_LOG", "moon=warn")
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
        .stderr(stderr)
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

/// RESP array of bulk strings — binary-safe, for multiword graph commands.
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

/// REPL-GRAPH-01: live GRAPH.* mutations on the master reach the replica.
/// The replica attaches to an EMPTY master, then every subsequent graph write
/// must appear — isolating the live streaming leg from snapshot backfill.
#[test]
#[ignore]
fn replica_syncs_graph_live_stream() {
    let master_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let master_addr = "127.0.0.1:16730";
    let replica_addr = "127.0.0.1:16731";

    let replica_log = replica_dir.path().join("replica-stderr.log");

    let _master = Killer(start_moon(16730, master_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(master_addr, "PING")
            .starts_with("+PONG")),
        "master never became ready"
    );
    let _replica = Killer(start_moon_logged(
        16731,
        replica_dir.path().to_str().unwrap(),
        Some(&replica_log),
    ));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(replica_addr, "PING")
            .starts_with("+PONG")),
        "replica never became ready"
    );
    assert!(
        send_cmd(replica_addr, "REPLICAOF 127.0.0.1 16730").starts_with("+OK"),
        "REPLICAOF failed"
    );
    // Let the handshake settle before the first write.
    std::thread::sleep(Duration::from_millis(500));

    // 1. Live graph creation + node inserts stream to the replica.
    assert!(send_cmd(master_addr, "GRAPH.CREATE g1").contains("OK"));
    let a = send_resp(
        master_addr,
        &["GRAPH.ADDNODE", "g1", "Person", "name", "alice"],
    );
    let b = send_resp(
        master_addr,
        &["GRAPH.ADDNODE", "g1", "Person", "name", "bob"],
    );
    assert!(a.starts_with(':'), "ADDNODE alice must return an id: {a}");
    assert!(b.starts_with(':'), "ADDNODE bob must return an id: {b}");

    let counted = wait_until(Duration::from_secs(10), || {
        let r = send_resp(
            replica_addr,
            &["GRAPH.QUERY", "g1", "MATCH (n:Person) RETURN count(n)"],
        );
        r.contains('2')
    });
    assert!(
        counted,
        "replica never saw the 2 streamed Person nodes: {}",
        send_resp(
            replica_addr,
            &["GRAPH.QUERY", "g1", "MATCH (n:Person) RETURN count(n)"]
        )
    );

    // 2. Node PROPERTIES survive replication (not just node count).
    let names = send_resp(
        replica_addr,
        &["GRAPH.QUERY", "g1", "MATCH (n:Person) RETURN n.name"],
    );
    assert!(
        names.contains("alice") && names.contains("bob"),
        "replica node properties diverged: {names}"
    );

    // 3. A brand-new graph created live shows up in the replica's GRAPH.LIST.
    assert!(send_cmd(master_addr, "GRAPH.CREATE g2").contains("OK"));
    let listed = wait_until(Duration::from_secs(10), || {
        send_cmd(replica_addr, "GRAPH.LIST").contains("g2")
    });
    assert!(
        listed,
        "replica GRAPH.LIST never listed live-created g2: {}",
        send_cmd(replica_addr, "GRAPH.LIST")
    );

    // 4. STREAM-HEALTH: everything above must have arrived on ONE live
    // stream. A reconnect/full-resync loop can deliver the same data via
    // repeated snapshots (each FULLRESYNC RDB carries the graph aux) and
    // silently mask a dead live stream — the exact failure mode that hid the
    // missing self-SPSC producer behind green tests.
    let log = std::fs::read_to_string(&replica_log).unwrap_or_default();
    let reconnects = log.matches("reconnecting").count();
    assert_eq!(
        reconnects, 0,
        "replica reconnected {reconnects}x — live stream did not hold:\n{log}"
    );
}

/// REPL-GRAPH-02: a replica attaching to a master that ALREADY has graph data
/// must backfill it from the PSYNC full-resync snapshot (not just live deltas).
#[test]
#[ignore]
fn replica_backfills_graph_from_snapshot() {
    let master_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let master_addr = "127.0.0.1:16732";
    let replica_addr = "127.0.0.1:16733";

    let _master = Killer(start_moon(16732, master_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(master_addr, "PING")
            .starts_with("+PONG")),
        "master never became ready"
    );

    // Build graph state on the master BEFORE any replica exists.
    assert!(send_cmd(master_addr, "GRAPH.CREATE gsnap").contains("OK"));
    for name in ["alice", "bob", "carol"] {
        let r = send_resp(
            master_addr,
            &["GRAPH.ADDNODE", "gsnap", "Person", "name", name],
        );
        assert!(r.starts_with(':'), "seed ADDNODE {name} failed: {r}");
    }

    // Now attach a fresh replica — it must learn gsnap + its 3 nodes.
    let _replica = Killer(start_moon(16733, replica_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(replica_addr, "PING")
            .starts_with("+PONG")),
        "replica never became ready"
    );
    assert!(
        send_cmd(replica_addr, "REPLICAOF 127.0.0.1 16732").starts_with("+OK"),
        "REPLICAOF failed"
    );

    let backfilled = wait_until(Duration::from_secs(10), || {
        send_resp(
            replica_addr,
            &["GRAPH.QUERY", "gsnap", "MATCH (n:Person) RETURN count(n)"],
        )
        .contains('3')
    });
    assert!(
        backfilled,
        "replica never backfilled the 3 pre-existing snapshot nodes: {}",
        send_resp(
            replica_addr,
            &["GRAPH.QUERY", "gsnap", "MATCH (n:Person) RETURN count(n)"]
        )
    );

    // A live write AFTER the snapshot must also land (stream continues past
    // the backfill boundary).
    let r = send_resp(
        master_addr,
        &["GRAPH.ADDNODE", "gsnap", "Person", "name", "dave"],
    );
    assert!(r.starts_with(':'), "post-snapshot ADDNODE failed: {r}");
    let grew = wait_until(Duration::from_secs(10), || {
        send_resp(
            replica_addr,
            &["GRAPH.QUERY", "gsnap", "MATCH (n:Person) RETURN count(n)"],
        )
        .contains('4')
    });
    assert!(grew, "post-snapshot live node did not replicate");
}

/// REPL-GRAPH-03 (adversarial round-2 finding B): TEMPORAL.INVALIDATE mutates
/// graph state (`valid_to`) and must reach the replica — streamed as the
/// deterministic wall-clock-pinned `TEMPORAL.INVALIDATE-AT` form so master
/// and replica agree on the exact `valid_to`. Observable only through an
/// explicit `VALID_AT` filter: far-future is inside the default
/// `valid_to = i64::MAX` before invalidation and outside `valid_to = now`
/// after it.
#[test]
#[ignore]
fn replica_applies_temporal_invalidate() {
    // ~ year 2255 in Unix ms: beyond "now", far short of i64::MAX.
    const FAR_FUTURE_MS: &str = "9000000000000";

    let master_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let master_addr = "127.0.0.1:16660";
    let replica_addr = "127.0.0.1:16661";

    let _master = Killer(start_moon(16660, master_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(master_addr, "PING")
            .starts_with("+PONG")),
        "master never became ready"
    );
    let _replica = Killer(start_moon(16661, replica_dir.path().to_str().unwrap()));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(replica_addr, "PING")
            .starts_with("+PONG")),
        "replica never became ready"
    );
    assert!(
        send_cmd(replica_addr, "REPLICAOF 127.0.0.1 16660").starts_with("+OK"),
        "REPLICAOF failed"
    );
    std::thread::sleep(Duration::from_millis(500));

    assert!(send_cmd(master_addr, "GRAPH.CREATE tg").contains("OK"));
    let reply = send_resp(
        master_addr,
        &["GRAPH.ADDNODE", "tg", "Person", "name", "alice"],
    );
    assert!(reply.starts_with(':'), "ADDNODE must return an id: {reply}");
    let node_id = reply.trim_start_matches(':').trim().to_string();

    let visible_query = [
        "GRAPH.QUERY",
        "tg",
        "MATCH (n:Person) RETURN n.name",
        "VALID_AT",
        FAR_FUTURE_MS,
    ];
    assert!(
        wait_until(Duration::from_secs(10), || send_resp(
            replica_addr,
            &visible_query
        )
        .contains("alice")),
        "node never became visible on the replica before invalidation"
    );

    let inv = send_resp(
        master_addr,
        &["TEMPORAL.INVALIDATE", &node_id, "NODE", "tg"],
    );
    assert!(inv.starts_with("+OK"), "TEMPORAL.INVALIDATE failed: {inv}");
    // Master-side sanity: invalidation is observable at VALID_AT far-future.
    assert!(
        !send_resp(master_addr, &visible_query).contains("alice"),
        "master itself still shows the node past its valid_to"
    );

    // The replica must converge to the same temporal visibility.
    assert!(
        wait_until(Duration::from_secs(10), || !send_resp(
            replica_addr,
            &visible_query
        )
        .contains("alice")),
        "TEMPORAL.INVALIDATE never applied on the replica (valid_to diverged): {}",
        send_resp(replica_addr, &visible_query)
    );
}
