//! moon#1302: graph (and MQ) WAL records past the per-shard 4096-slot WAL
//! append channel were acknowledged but lost on kill -9.
//!
//! Graph and MQ writes reach WAL-v3 through a bounded per-shard channel that
//! the shard's own event loop drains on its 1 ms tick. The tick cannot run
//! while one connection's read batch executes, so any single command (or
//! pipelined batch) that emitted more records than the channel's free slots
//! had the rest dropped by an unchecked `try_send`:
//!
//! ```text
//! GRAPH.QUERY g "CREATE (:N {i:0}), … (:N {i:5999})"  -> 6000 nodes created
//! kill -9, restart                                    -> 4096 nodes
//! ```
//!
//! The rollback side (PR #1301) answered `MOONERR WAL backpressure` instead
//! of `+OK` for a TXN whose rollback did not fit, deterministically for more
//! than 4096 records and for a 2,500-node TXN aborted in the same pipelined
//! batch that wrote it; its replica already held the full rollback.
//!
//! Every test runs a real server (`MOON_BIN` pinned), SIGKILLs it after the
//! reply and restarts it on the same directory:
//!
//! ```text
//! MOON_BIN=/path/to/moon cargo test --test graph_wal_append_1302 \
//!     -- --include-ignored --test-threads 1
//! ```
//!
//! Graph tests SKIP on a binary without the `graph` feature (the tokio leg
//! builds without default features), detected by `GRAPH.CREATE` answering
//! "unknown command". The replica test needs master-side PSYNC (monoio). The
//! MQ tests run on both runtimes.

mod common;

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::Conn;

const OK: &str = "+OK\r\n";

/// More records than the per-shard WAL append channel holds (4096 slots).
const BIG: usize = 6000;

/// The pipelined TXN of the moon#1302 review: its forward records plus its
/// rollback records exceed the channel within one batch, while the TXN itself
/// is well under 4096.
const PIPELINED: usize = 2500;

struct Server {
    guard: common::ServerGuard,
    port: u16,
}

fn start(dir: &std::path::Path, shards: usize) -> Server {
    let bin = common::find_moon_binary();
    let dir_s = dir.to_str().expect("utf8 dir").to_string();
    let (guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &shards.to_string(),
                "--dir",
                &dir_s,
                "--appendonly",
                "yes",
                "--appendfsync",
                "always",
                "--disk-free-min-pct",
                "0",
            ])
            .env("RUST_LOG", "moon=warn")
            .env("MOON_DISK_FREE_MIN_PCT", "0")
            .stdout(Stdio::null())
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (set MOON_BIN)")
    });
    wait_ready(port);
    Server { guard, port }
}

/// PING until `+PONG` — a restarting server answers `-LOADING` first.
fn wait_ready(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Ok(mut s) = std::net::TcpStream::connect(("127.0.0.1", port)) {
            use std::io::{Read, Write};
            let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
            if s.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut buf = [0u8; 64];
                if let Ok(n) = s.read(&mut buf)
                    && buf[..n].starts_with(b"+PONG")
                {
                    return;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("server on {port} never answered PONG");
}

fn restart(mut server: Server, dir: &std::path::Path, shards: usize) -> Server {
    server.guard.kill_now();
    start(dir, shards)
}

/// Graph and MQ records reach WAL-v3 on the shard's 1 ms tick, which does not
/// gate replies (`--appendfsync always` fsyncs off-loop right after). Wait
/// the tick out so the kill tests the records, not the tick.
fn let_wal_v3_flush() {
    std::thread::sleep(Duration::from_millis(300));
}

fn has_graph(c: &mut Conn) -> bool {
    let r = c.send(&["GRAPH.CREATE", "__probe_graph"]);
    if r.to_ascii_lowercase().contains("unknown command") {
        return false;
    }
    let _ = c.send(&["GRAPH.DELETE", "__probe_graph"]);
    true
}

/// A hash tag this connection's shard owns (a TXN refuses a KV write to
/// another shard's key). Probed with a throwaway transaction.
fn local_tag(c: &mut Conn) -> String {
    for i in 0..512 {
        let tag = format!("t{i}");
        assert_eq!(c.send(&["TXN", "BEGIN"]), OK);
        let r = c.send(&["SET", &format!("{{{tag}}}:probe"), "1"]);
        assert_eq!(c.send(&["TXN", "ABORT"]), OK);
        if r == OK {
            assert_eq!(c.send(&["DEL", &format!("{{{tag}}}:probe")]), ":0\r\n");
            return tag;
        }
    }
    panic!("no hash tag is local to this connection's shard");
}

/// A hash tag this connection's shard does NOT own; `None` at `--shards 1`.
fn remote_tag(c: &mut Conn) -> Option<String> {
    for i in 0..512 {
        let tag = format!("r{i}");
        assert_eq!(c.send(&["TXN", "BEGIN"]), OK);
        let r = c.send(&["SET", &format!("{{{tag}}}:probe"), "1"]);
        let _ = c.send(&["TXN", "ABORT"]);
        if r.contains("cross-shard") {
            return Some(tag);
        }
        if r == OK {
            assert_eq!(c.send(&["DEL", &format!("{{{tag}}}:probe")]), ":0\r\n");
        }
    }
    None
}

/// `MATCH (n:N) RETURN count(n)` on `graph`.
fn node_count(c: &mut Conn, graph: &str) -> i64 {
    let r = c.send(&["GRAPH.QUERY", graph, "MATCH (n:N) RETURN count(n)"]);
    r.split("\r\n:")
        .nth(1)
        .and_then(|t| t.split("\r\n").next())
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("no count in {r:?}"))
}

/// `section` field `name` as an integer; 0 when absent.
fn info_int(c: &mut Conn, section: &str, name: &str) -> i64 {
    let info = c.send(&["INFO", section]);
    let prefix = format!("{name}:");
    info.lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0)
}

/// The graphs a `--shards` run exercises: the connection's own shard (the
/// handler's local leg) and, at `--shards > 1`, another shard's (the owner's
/// SPSC leg).
fn graph_names(c: &mut Conn, shards: usize) -> Vec<String> {
    let mut v = vec![format!("{{{}}}g", local_tag(c))];
    if shards > 1 {
        v.push(format!(
            "{{{}}}g",
            remote_tag(c).expect("--shards > 1 has a remote hash tag")
        ));
    }
    v
}

fn assert_no_error(replies: &str) {
    assert!(
        !replies.starts_with('-') && !replies.contains("\r\n-"),
        "an error reply in the batch: {}",
        &replies[..replies.len().min(400)]
    );
}

// ---------------------------------------------------------------------------
// Forward: one Cypher CREATE of 6000 nodes
// ---------------------------------------------------------------------------

fn one_create_case(shards: usize) {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = start(dir.path(), shards);
    let mut c = Conn::open(server.port);
    if !has_graph(&mut c) {
        eprintln!("SKIP: this binary has no `graph` feature (tokio leg)");
        return;
    }
    let graphs = graph_names(&mut c, shards);
    let query = format!(
        "CREATE {}",
        (0..BIG)
            .map(|i| format!("(:N {{i: {i}}})"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    for g in &graphs {
        assert_eq!(c.send(&["GRAPH.CREATE", g]), OK);
        let r = c.send(&["GRAPH.QUERY", g, &query]);
        assert!(
            r.contains(&format!("Nodes created: {BIG}")),
            "{g}: the CREATE must create {BIG} nodes: {:?}",
            &r[..r.len().min(300)]
        );
        assert_eq!(node_count(&mut c, g), BIG as i64, "{g}: live");
    }
    drop(c);
    let_wal_v3_flush();
    let server = restart(server, dir.path(), shards);
    let mut c = Conn::open(server.port);
    for g in &graphs {
        let after = node_count(&mut c, g);
        assert_eq!(
            after, BIG as i64,
            "{g}: the CREATE answered OK for {BIG} nodes, yet {after} survived kill -9"
        );
    }
}

#[test]
#[ignore = "spawns a real server; set MOON_BIN"]
fn one_cypher_create_of_6000_nodes_survives_kill9_shards_1() {
    one_create_case(1);
}

#[test]
#[ignore = "spawns a real server; set MOON_BIN"]
fn one_cypher_create_of_6000_nodes_survives_kill9_shards_4() {
    one_create_case(4);
}

// ---------------------------------------------------------------------------
// Rollback: 2,500 pipelined GRAPH.ADDNODE + TXN ABORT in ONE batch
// ---------------------------------------------------------------------------

fn pipelined_abort_case(shards: usize) {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = start(dir.path(), shards);
    let mut c = Conn::open(server.port);
    if !has_graph(&mut c) {
        eprintln!("SKIP: this binary has no `graph` feature (tokio leg)");
        return;
    }
    let graphs = graph_names(&mut c, shards);
    let ids: Vec<String> = (0..PIPELINED).map(|i| i.to_string()).collect();
    for g in &graphs {
        assert_eq!(c.send(&["GRAPH.CREATE", g]), OK);
        let mut cmds: Vec<Vec<&str>> = vec![vec!["TXN", "BEGIN"]];
        cmds.extend(
            ids.iter()
                .map(|i| vec!["GRAPH.ADDNODE", g.as_str(), "N", "i", i.as_str()]),
        );
        cmds.push(vec!["TXN", "ABORT"]);
        let refs: Vec<&[&str]> = cmds.iter().map(Vec::as_slice).collect();
        let replies = c.pipeline(&refs);
        assert!(replies.starts_with(OK), "TXN BEGIN: {replies:.200}");
        assert!(
            replies.ends_with(&format!("\r\n{OK}")),
            "{g}: the abort of a {PIPELINED}-node TXN pipelined with its writes must \
             answer +OK; tail: {:?}",
            &replies[replies.len().saturating_sub(300)..]
        );
        assert_no_error(&replies);
        assert_eq!(node_count(&mut c, g), 0, "{g}: live, after the abort");
    }
    drop(c);
    let_wal_v3_flush();
    let server = restart(server, dir.path(), shards);
    let mut c = Conn::open(server.port);
    for g in &graphs {
        let after = node_count(&mut c, g);
        assert_eq!(
            after, 0,
            "{g}: TXN ABORT answered +OK, yet {after} aborted nodes came back after kill -9"
        );
    }
}

/// The same TXN written by ONE Cypher `CREATE` of 2,500 nodes, pipelined with
/// its `TXN ABORT`: the forward records and the rollback records are emitted
/// with no tick in between, deterministically (the `GRAPH.ADDNODE` shape above
/// is split into 1024-frame batches, `MAX_BATCH_FRAMES`, and the tick can
/// drain between them).
fn pipelined_create_abort_case(shards: usize) {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = start(dir.path(), shards);
    let mut c = Conn::open(server.port);
    if !has_graph(&mut c) {
        eprintln!("SKIP: this binary has no `graph` feature (tokio leg)");
        return;
    }
    // A Cypher write inside a TXN must be local to the connection's shard
    // (a remote one is refused as cross-shard, unlike `GRAPH.ADDNODE`).
    let graphs = [format!("{{{}}}g", local_tag(&mut c))];
    let query = format!(
        "CREATE {}",
        (0..PIPELINED)
            .map(|i| format!("(:N {{i: {i}}})"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    for g in &graphs {
        assert_eq!(c.send(&["GRAPH.CREATE", g]), OK);
        let replies = c.pipeline(&[
            &["TXN", "BEGIN"],
            &["GRAPH.QUERY", g.as_str(), query.as_str()],
            &["TXN", "ABORT"],
        ]);
        assert!(
            replies.contains(&format!("Nodes created: {PIPELINED}")),
            "{g}: {:.300}",
            replies
        );
        assert!(
            replies.ends_with(&format!("\r\n{OK}")),
            "{g}: the abort of a {PIPELINED}-node TXN pipelined with its CREATE must \
             answer +OK; tail: {:?}",
            &replies[replies.len().saturating_sub(300)..]
        );
        assert_eq!(node_count(&mut c, g), 0, "{g}: live, after the abort");
    }
    drop(c);
    let_wal_v3_flush();
    let server = restart(server, dir.path(), shards);
    let mut c = Conn::open(server.port);
    for g in &graphs {
        let after = node_count(&mut c, g);
        assert_eq!(
            after, 0,
            "{g}: TXN ABORT answered +OK, yet {after} aborted nodes came back after kill -9"
        );
    }
}

#[test]
#[ignore = "spawns a real server; set MOON_BIN"]
fn pipelined_2500_node_create_and_abort_is_ok_and_stays_aborted_shards_1() {
    pipelined_create_abort_case(1);
}

#[test]
#[ignore = "spawns a real server; set MOON_BIN"]
fn pipelined_2500_node_create_and_abort_is_ok_and_stays_aborted_shards_4() {
    pipelined_create_abort_case(4);
}

#[test]
#[ignore = "spawns a real server; set MOON_BIN"]
fn pipelined_2500_addnode_and_abort_is_ok_and_stays_aborted_shards_1() {
    pipelined_abort_case(1);
}

#[test]
#[ignore = "spawns a real server; set MOON_BIN"]
fn pipelined_2500_addnode_and_abort_is_ok_and_stays_aborted_shards_4() {
    pipelined_abort_case(4);
}

// ---------------------------------------------------------------------------
// Rollback: a TXN with more than 4096 graph rollback records
// ---------------------------------------------------------------------------

/// Writes `BIG` nodes inside an open TXN on `g`, in chunks with a pause so the
/// forward records drain: this is about the rollback's records.
fn txn_write_big(c: &mut Conn, g: &str) {
    let ids: Vec<String> = (0..BIG).map(|i| i.to_string()).collect();
    for chunk in ids.chunks(200) {
        let cmds: Vec<Vec<&str>> = chunk
            .iter()
            .map(|i| vec!["GRAPH.ADDNODE", g, "N", "i", i.as_str()])
            .collect();
        let refs: Vec<&[&str]> = cmds.iter().map(Vec::as_slice).collect();
        assert_no_error(&c.pipeline(&refs));
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn big_rollback_case(shards: usize) {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = start(dir.path(), shards);
    let mut c = Conn::open(server.port);
    if !has_graph(&mut c) {
        eprintln!("SKIP: this binary has no `graph` feature (tokio leg)");
        return;
    }
    let graphs = graph_names(&mut c, shards);
    for g in &graphs {
        assert_eq!(c.send(&["GRAPH.CREATE", g]), OK);
        assert_eq!(
            c.send(&["GRAPH.ADDNODE", g, "N", "i", "kept"]).as_bytes()[0],
            b':'
        );
        assert_eq!(c.send(&["TXN", "BEGIN"]), OK);
        txn_write_big(&mut c, g);
        assert_eq!(node_count(&mut c, g), BIG as i64 + 1);
        let abort = c.send(&["TXN", "ABORT"]);
        assert_eq!(
            abort, OK,
            "{g}: a TXN with {BIG} graph rollback records must abort with +OK"
        );
        assert_eq!(node_count(&mut c, g), 1, "{g}: live, after the abort");
    }
    assert_eq!(
        info_int(&mut c, "persistence", "txn_rollback_wal_dropped"),
        0
    );
    drop(c);
    let_wal_v3_flush();
    let server = restart(server, dir.path(), shards);
    let mut c = Conn::open(server.port);
    for g in &graphs {
        let after = node_count(&mut c, g);
        assert_eq!(
            after, 1,
            "{g}: after kill -9 only the node written outside the TXN may exist, got {after}"
        );
    }
}

#[test]
#[ignore = "spawns a real server; set MOON_BIN"]
fn txn_with_6000_graph_rollback_records_aborts_ok_and_survives_kill9_shards_1() {
    big_rollback_case(1);
}

#[test]
#[ignore = "spawns a real server; set MOON_BIN"]
fn txn_with_6000_graph_rollback_records_aborts_ok_and_survives_kill9_shards_4() {
    big_rollback_case(4);
}

// ---------------------------------------------------------------------------
// Replica: master and replica agree after a master restart that follows a
// large abort (moon#1302 review item 3; --shards 1, monoio master)
// ---------------------------------------------------------------------------

fn wait_for(what: &str, timeout: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
#[ignore = "spawns real servers; set MOON_BIN to a monoio build"]
fn master_and_replica_agree_after_a_large_abort_and_master_restart() {
    let mdir = tempfile::tempdir().expect("tempdir");
    let rdir = tempfile::tempdir().expect("tempdir");
    let master = start(mdir.path(), 1);
    let replica = start(rdir.path(), 1);
    let mut m = Conn::open(master.port);
    if !has_graph(&mut m) {
        eprintln!("SKIP: no `graph` feature — the tokio leg has no master-side PSYNC");
        return;
    }
    let mut r = Conn::open(replica.port);
    assert_eq!(
        r.send(&["REPLICAOF", "127.0.0.1", &master.port.to_string()]),
        OK
    );
    wait_for("the replication link", Duration::from_secs(20), || {
        r.send(&["INFO", "replication"])
            .contains("master_link_status:up")
    });
    let g = "g";
    assert_eq!(m.send(&["GRAPH.CREATE", g]), OK);
    assert_eq!(
        m.send(&["GRAPH.ADDNODE", g, "N", "i", "kept"]).as_bytes()[0],
        b':'
    );
    assert_eq!(m.send(&["TXN", "BEGIN"]), OK);
    txn_write_big(&mut m, g);
    let abort = m.send(&["TXN", "ABORT"]);
    assert_eq!(m.send(&["SET", "after-abort", "1"]), OK);
    wait_for(
        "the post-abort marker on the replica",
        Duration::from_secs(30),
        || r.send(&["GET", "after-abort"]) == "$1\r\n1\r\n",
    );
    let replica_nodes = node_count(&mut r, g);
    let master_live = node_count(&mut m, g);
    drop(r);
    drop(replica);
    drop(m);
    let_wal_v3_flush();
    let master = restart(master, mdir.path(), 1);
    let mut m = Conn::open(master.port);
    let master_after = node_count(&mut m, g);
    eprintln!(
        "abort={abort:?} master live={master_live} replica={replica_nodes} \
         master after restart={master_after}"
    );
    assert_eq!(master_live, 1);
    assert_eq!(
        master_after, replica_nodes,
        "after a master restart the master holds {master_after} nodes and its replica \
         {replica_nodes} (the master's abort answered {abort:?})"
    );
    assert_eq!(abort, OK, "the abort of a {BIG}-node TXN must answer +OK");
    assert_eq!(master_after, 1);
}

// ---------------------------------------------------------------------------
// MQ: a TXN.COMMIT that materializes more than 4096 publishes (one MqPush WAL
// record each, in one synchronous stretch)
// ---------------------------------------------------------------------------

fn mq_burst_case(shards: usize) {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = start(dir.path(), shards);
    let mut c = Conn::open(server.port);
    let q = format!("{{{}}}q", local_tag(&mut c));
    assert_eq!(c.send(&["MQ", "CREATE", &q]), OK);
    assert_eq!(c.send(&["TXN", "BEGIN"]), OK);
    let ids: Vec<String> = (0..BIG).map(|i| i.to_string()).collect();
    for chunk in ids.chunks(500) {
        let cmds: Vec<Vec<&str>> = chunk
            .iter()
            .map(|i| vec!["MQ", "PUBLISH", q.as_str(), "i", i.as_str()])
            .collect();
        let refs: Vec<&[&str]> = cmds.iter().map(Vec::as_slice).collect();
        assert_no_error(&c.pipeline(&refs));
    }
    assert_eq!(c.send(&["TXN", "COMMIT"]), OK);
    let live = c.send(&["XLEN", &q]);
    assert_eq!(live, format!(":{BIG}\r\n"), "live, after the commit");
    let dropped = info_int(
        &mut c,
        "reclamation",
        "reclamation_wal_append_channel_dropped_total",
    );
    drop(c);
    let_wal_v3_flush();
    let server = restart(server, dir.path(), shards);
    let mut c = Conn::open(server.port);
    let after = c.send(&["XLEN", &q]);
    assert_eq!(
        after,
        format!(":{BIG}\r\n"),
        "TXN COMMIT answered +OK for {BIG} publishes (WAL records dropped: {dropped}), \
         yet the queue holds {after:?} after kill -9"
    );
    assert_eq!(dropped, 0, "no MQ WAL record may be dropped");
}

#[test]
#[ignore = "spawns a real server; set MOON_BIN"]
fn mq_txn_commit_of_6000_publishes_survives_kill9_shards_1() {
    mq_burst_case(1);
}

#[test]
#[ignore = "spawns a real server; set MOON_BIN"]
fn mq_txn_commit_of_6000_publishes_survives_kill9_shards_4() {
    mq_burst_case(4);
}
