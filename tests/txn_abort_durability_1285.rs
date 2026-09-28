//! moon#1285 / moon#1185 (option b): `TXN.ABORT` must survive a crash and
//! reach replicas.
//!
//! A cross-store transaction applies its writes to the live stores as they
//! run, and those writes reach the AOF, the WAL and the replication stream
//! at that moment. `TXN.ABORT` used to rewind memory only, so:
//!
//! ```text
//! SET k original ; TXN BEGIN ; SET k aborted ; TXN ABORT ; GET k -> original
//! kill -9, restart with --appendonly yes                ; GET k -> aborted
//! ```
//!
//! on both runtimes, and a replica kept `aborted` forever. The same held for
//! the graph plane (its rollback logged nothing, although the forward writes
//! were WAL-logged) and the vector index was wrong even live: an in-TXN
//! `HSET` tombstones the key's previous vector and an in-TXN `DEL` its
//! document, and the abort restored neither.
//!
//! Every test runs a real server (`MOON_BIN` pinned), SIGKILLs it after the
//! abort's `+OK` (`--appendfsync always`: the reply means the abort is on
//! disk) and restarts it on the same directory.
//!
//! ```text
//! MOON_BIN=/path/to/moon cargo test --test txn_abort_durability_1285 \
//!     -- --include-ignored --test-threads 1
//! ```
//!
//! Coverage: KV insert / update / delete, key TTL, per-field hash TTL, two
//! databases (one reached by a `SELECT` inside the transaction), `--shards 1`
//! and `4`, both runtimes; vector (both runtimes); graph and the replica leg
//! only where the binary has the `graph` feature (the monoio build — the
//! tokio leg has neither graph nor master-side PSYNC), detected by
//! `GRAPH.CREATE` answering "unknown command". MQ is an audit: `MQ PUBLISH`
//! intents are held until commit, so an abort has nothing to undo.
//!
//! PR #1301 review: a graph rollback whose WAL records overflow the shard's
//! WAL append channel (local leg at `--shards 1`, remote owner leg at `4`)
//! must answer the refusal, never `+OK` followed by resurrected writes.

mod common;

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::Conn;

const OK: &str = "+OK\r\n";

fn bulk(s: &str) -> String {
    format!("${}\r\n{s}\r\n", s.len())
}

fn int(n: i64) -> String {
    format!(":{n}\r\n")
}

/// A running server plus the directory it persists into.
struct Server {
    guard: common::ServerGuard,
    port: u16,
}

fn start(dir: &std::path::Path, shards: usize, appendonly: bool) -> Server {
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
                if appendonly { "yes" } else { "no" },
                "--appendfsync",
                "always",
                "--disk-free-min-pct",
                "0",
            ])
            .env("RUST_LOG", "moon=warn")
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
    start(dir, shards, true)
}

/// Graph and MQ records go to the WAL-v3 writer, which flushes on its 1 ms
/// tick — not behind the AOF's `always` barrier. The forward writes have the
/// same window; wait it out so the kill tests the records, not the tick.
fn let_wal_v3_flush() {
    std::thread::sleep(Duration::from_millis(200));
}

/// A hash tag whose keys this connection's shard owns: a `TXN` refuses a
/// write to another shard's key. Probed with a throwaway transaction.
fn local_tag(c: &mut Conn) -> String {
    for i in 0..512 {
        let tag = format!("t{i}");
        assert_eq!(c.send(&["TXN", "BEGIN"]), OK);
        let r = c.send(&["SET", &format!("{{{tag}}}:probe"), "1"]);
        assert_eq!(c.send(&["TXN", "ABORT"]), OK);
        if r == OK {
            assert_eq!(c.send(&["DEL", &format!("{{{tag}}}:probe")]), int(0));
            return tag;
        }
    }
    panic!("no hash tag is local to this connection's shard");
}

fn has_graph(c: &mut Conn) -> bool {
    let r = c.send(&["GRAPH.CREATE", "__probe_graph"]);
    if r.to_ascii_lowercase().contains("unknown command") {
        return false;
    }
    let _ = c.send(&["GRAPH.DELETE", "__probe_graph"]);
    true
}

// ---------------------------------------------------------------------------
// KV
// ---------------------------------------------------------------------------

struct KvKeys {
    upd: String,
    ttl: String,
    del: String,
    hash: String,
    new: String,
    d5: String,
    new5: String,
}

impl KvKeys {
    fn new(tag: &str) -> Self {
        let k = |n: &str| format!("{{{tag}}}:{n}");
        KvKeys {
            upd: k("upd"),
            ttl: k("ttl"),
            del: k("del"),
            hash: k("hash"),
            new: k("new"),
            d5: k("d5"),
            new5: k("new5"),
        }
    }
}

/// The pre-transaction state, in db 3 and db 5.
fn kv_seed(c: &mut Conn, k: &KvKeys) {
    assert_eq!(c.send(&["SELECT", "3"]), OK);
    assert_eq!(c.send(&["SET", &k.upd, "original"]), OK);
    assert_eq!(c.send(&["SET", &k.ttl, "keep", "PX", "600000"]), OK);
    assert_eq!(c.send(&["RPUSH", &k.del, "a", "b", "c"]), int(3));
    assert_eq!(c.send(&["HSET", &k.hash, "f1", "v1", "f2", "v2"]), int(2));
    assert_eq!(
        c.send(&["HPEXPIRE", &k.hash, "600000", "FIELDS", "1", "f1"]),
        "*1\r\n:1\r\n"
    );
    assert_eq!(c.send(&["SELECT", "5"]), OK);
    assert_eq!(c.send(&["SET", &k.d5, "five"]), OK);
    assert_eq!(c.send(&["SELECT", "3"]), OK);
}

/// Insert, update (twice), TTL drop, delete, hash rewrite — and, with
/// `select_inside`, an update and an insert in db 5 reached by a `SELECT`
/// inside the transaction.
fn kv_txn_then_abort(c: &mut Conn, k: &KvKeys, select_inside: bool) {
    assert_eq!(c.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(c.send(&["SET", &k.upd, "aborted"]), OK);
    assert_eq!(c.send(&["SET", &k.upd, "aborted-again"]), OK);
    assert_eq!(c.send(&["SET", &k.ttl, "changed"]), OK);
    assert_eq!(c.send(&["DEL", &k.del]), int(1));
    assert_eq!(c.send(&["SET", &k.new, "inserted", "EX", "100"]), OK);
    assert_eq!(c.send(&["HSET", &k.hash, "f1", "X", "f3", "Y"]), int(1));
    if select_inside {
        assert_eq!(c.send(&["SELECT", "5"]), OK);
        assert_eq!(c.send(&["SET", &k.d5, "changed"]), OK);
        assert_eq!(c.send(&["SET", &k.new5, "x"]), OK);
        assert_eq!(c.send(&["SELECT", "3"]), OK);
    }
    assert_eq!(c.send(&["GET", &k.upd]), bulk("aborted-again"));
    assert_eq!(c.send(&["TXN", "ABORT"]), OK);
}

/// The aborted-to state: exactly the seed.
fn kv_assert_seed(c: &mut Conn, k: &KvKeys, when: &str) {
    assert_eq!(c.send(&["SELECT", "3"]), OK);
    assert_eq!(
        c.send(&["GET", &k.upd]),
        bulk("original"),
        "{when}: update undone"
    );
    assert_eq!(
        c.send(&["GET", &k.ttl]),
        bulk("keep"),
        "{when}: TTL key value"
    );
    let pttl = c.send(&["PTTL", &k.ttl]);
    let ms: i64 = pttl
        .trim_start_matches(':')
        .trim_end()
        .parse()
        .unwrap_or(-9);
    assert!(
        ms > 0 && ms <= 600_000,
        "{when}: the key TTL must come back with the value, got {pttl:?}"
    );
    assert_eq!(
        c.send(&["LRANGE", &k.del, "0", "-1"]),
        "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n",
        "{when}: delete undone"
    );
    assert_eq!(c.send(&["EXISTS", &k.new]), int(0), "{when}: insert undone");
    assert_eq!(c.send(&["HLEN", &k.hash]), int(2), "{when}: hash fields");
    assert_eq!(
        c.send(&["HGET", &k.hash, "f1"]),
        bulk("v1"),
        "{when}: hash value"
    );
    let fttl = c.send(&["HPTTL", &k.hash, "FIELDS", "2", "f1", "f2"]);
    assert!(
        fttl.starts_with("*2\r\n:") && fttl.ends_with(":-1\r\n") && !fttl.contains(":-2"),
        "{when}: f1 keeps its field TTL, f2 has none: {fttl:?}"
    );
    assert_eq!(c.send(&["SELECT", "5"]), OK);
    assert_eq!(
        c.send(&["GET", &k.d5]),
        bulk("five"),
        "{when}: db5 update undone"
    );
    assert_eq!(
        c.send(&["EXISTS", &k.new5]),
        int(0),
        "{when}: db5 insert undone"
    );
    assert_eq!(c.send(&["SELECT", "3"]), OK);
}

fn kv_case(shards: usize, select_inside: bool) {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = start(dir.path(), shards, true);
    let mut c = Conn::open(server.port);
    let tag = local_tag(&mut c);
    let k = KvKeys::new(&tag);
    kv_seed(&mut c, &k);
    kv_txn_then_abort(&mut c, &k, select_inside);
    kv_assert_seed(&mut c, &k, "live, after TXN.ABORT");
    drop(c);

    // The abort's +OK was acked under appendfsync=always: crash now.
    let server = restart(server, dir.path(), shards);
    let mut c = Conn::open(server.port);
    kv_assert_seed(
        &mut c,
        &k,
        &format!("after kill -9 + restart (shards={shards})"),
    );
}

/// The moon#1285 repro, widened: every KV undo kind, TTLs, db 3.
#[test]
#[ignore = "spawns a real server; set MOON_BIN"]
fn kv_abort_survives_restart_shards_1() {
    kv_case(1, false);
}

#[test]
#[ignore = "spawns a real server; set MOON_BIN"]
fn kv_abort_survives_restart_shards_4() {
    kv_case(4, false);
}

/// A `SELECT` inside the transaction: the abort used to replay every undo
/// record into the database selected AT ABORT time, so the db 5 writes were
/// never undone (live) and their compensation would have named the wrong db.
#[test]
#[ignore = "spawns a real server; set MOON_BIN"]
fn kv_abort_with_select_inside_the_txn_shards_1() {
    kv_case(1, true);
}

#[test]
#[ignore = "spawns a real server; set MOON_BIN"]
fn kv_abort_with_select_inside_the_txn_shards_4() {
    kv_case(4, true);
}

/// Disconnect cleanup is the third abort path: a client that dies inside a
/// transaction must not leave its writes to be replayed.
#[test]
#[ignore = "spawns a real server; set MOON_BIN"]
fn kv_disconnect_abort_survives_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = start(dir.path(), 1, true);
    let mut c = Conn::open(server.port);
    assert_eq!(c.send(&["SET", "dk", "original"]), OK);
    assert_eq!(c.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(c.send(&["SET", "dk", "aborted"]), OK);
    assert_eq!(c.send(&["SET", "dnew", "x"]), OK);
    drop(c);
    // The rollback runs when the server notices the disconnect.
    let mut probe = Conn::open(server.port);
    let deadline = Instant::now() + Duration::from_secs(10);
    while probe.send(&["GET", "dk"]) != bulk("original") {
        assert!(Instant::now() < deadline, "disconnect never rolled back");
        std::thread::sleep(Duration::from_millis(50));
    }
    // A write after the rollback goes through the same AOF writer and the
    // same fsync barrier, so once it is acked the rollback's records are too.
    assert_eq!(probe.send(&["SET", "barrier", "1"]), OK);
    drop(probe);
    let server = restart(server, dir.path(), 1);
    let mut c = Conn::open(server.port);
    assert_eq!(c.send(&["GET", "dk"]), bulk("original"));
    assert_eq!(c.send(&["EXISTS", "dnew"]), int(0));
}

// ---------------------------------------------------------------------------
// Vector
// ---------------------------------------------------------------------------

fn vec4(a: f32, b: f32, c: f32, d: f32) -> Vec<u8> {
    [a, b, c, d].iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// `parts` with binary-safe bulk strings.
fn send_bin(c: &mut Conn, parts: &[&[u8]]) -> String {
    use std::io::Write;
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    c.sock.write_all(&out).expect("write");
    c.read_replies(1)
}

fn knn(c: &mut Conn) -> String {
    let q = vec4(1.0, 0.0, 0.0, 0.0);
    send_bin(
        c,
        &[
            b"FT.SEARCH",
            b"idx",
            b"*=>[KNN 5 @vec $q]",
            b"PARAMS",
            b"2",
            b"q",
            &q,
            b"DIALECT",
            b"2",
        ],
    )
}

/// The index holds exactly doc:1 (at its ORIGINAL vector, the nearest to the
/// query) and doc:2 — not the aborted doc:3, not doc:1's aborted vector.
fn vec_assert_seed(c: &mut Conn, when: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let r = knn(c);
        let ok = r.starts_with("*5\r\n:2\r\n")
            && !r.contains("doc:3")
            && r.find("doc:1")
                .zip(r.find("doc:2"))
                .is_some_and(|(a, b)| a < b);
        if ok {
            return;
        }
        // A restarted server reconciles its indexes in the background.
        if Instant::now() >= deadline {
            panic!(
                "{when}: FT.SEARCH must return doc:1 (original vector, nearest) then \
                 doc:2, and nothing else: {r:?}"
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn vec_seed(c: &mut Conn) {
    assert_eq!(
        c.send(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "vec",
            "VECTOR",
            "HNSW",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            "4",
            "DISTANCE_METRIC",
            "L2",
        ]),
        OK
    );
    let v1 = vec4(1.0, 0.0, 0.0, 0.0);
    let v2 = vec4(0.0, 1.0, 0.0, 0.0);
    assert_eq!(
        send_bin(c, &[b"HSET", b"doc:1", b"vec", &v1, b"tag", b"old"]),
        int(2)
    );
    assert_eq!(send_bin(c, &[b"HSET", b"doc:2", b"vec", &v2]), int(1));
}

fn vec_txn_then_abort(c: &mut Conn) {
    assert_eq!(c.send(&["TXN", "BEGIN"]), OK);
    let far = vec4(5.0, 5.0, 0.0, 0.0);
    let near = vec4(1.0, 0.0, 0.0, 0.0);
    assert_eq!(
        send_bin(c, &[b"HSET", b"doc:1", b"vec", &far, b"tag", b"new"]),
        int(0)
    );
    assert_eq!(send_bin(c, &[b"HSET", b"doc:3", b"vec", &near]), int(1));
    assert_eq!(c.send(&["DEL", "doc:2"]), int(1));
    assert_eq!(c.send(&["TXN", "ABORT"]), OK);
}

#[test]
#[ignore = "spawns a real server; set MOON_BIN"]
fn vector_abort_is_live_correct_and_survives_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = start(dir.path(), 1, true);
    let mut c = Conn::open(server.port);
    vec_seed(&mut c);
    vec_assert_seed(&mut c, "before the transaction");
    vec_txn_then_abort(&mut c);
    assert_eq!(c.send(&["HGET", "doc:1", "tag"]), bulk("old"));
    vec_assert_seed(&mut c, "live, after TXN.ABORT");
    drop(c);
    let server = restart(server, dir.path(), 1);
    let mut c = Conn::open(server.port);
    assert_eq!(c.send(&["HGET", "doc:1", "tag"]), bulk("old"));
    assert_eq!(c.send(&["EXISTS", "doc:3"]), int(0));
    vec_assert_seed(&mut c, "after kill -9 + restart");
}

// ---------------------------------------------------------------------------
// Graph
// ---------------------------------------------------------------------------

const GRAPH_ROWS: &str = "MATCH (n:P) RETURN n.id, n.name, n.extra ORDER BY n.id";

fn graph_seed(c: &mut Conn) {
    assert_eq!(c.send(&["GRAPH.CREATE", "g"]), OK);
    let q = "CREATE (:P {id: 1, name: 'keep'})-[:R]->(:P {id: 2, name: 'del'})";
    let r = c.send(&["GRAPH.QUERY", "g", q]);
    assert!(!r.starts_with('-'), "{q}: {r}");
}

fn graph_txn_then_abort(c: &mut Conn) {
    assert_eq!(c.send(&["TXN", "BEGIN"]), OK);
    for q in [
        "CREATE (:P {id: 3, name: 'aborted'})",
        "MATCH (n:P {id: 1}) SET n.name = 'changed'",
        "MATCH (n:P {id: 1}) SET n.extra = 7",
        "MATCH (n:P {id: 2}) DELETE n",
    ] {
        let r = c.send(&["GRAPH.QUERY", "g", q]);
        assert!(!r.starts_with('-'), "{q}: {r}");
    }
    assert_eq!(c.send(&["TXN", "ABORT"]), OK);
}

fn graph_assert_seed(c: &mut Conn, when: &str) {
    let rows = c.send(&["GRAPH.QUERY", "g", GRAPH_ROWS]);
    let want_rows = "*2\r\n*3\r\n:1\r\n$4\r\nkeep\r\n$-1\r\n*3\r\n:2\r\n$3\r\ndel\r\n$-1\r\n";
    assert!(
        rows.contains(want_rows),
        "{when}: graph must hold exactly nodes 1 (name keep, no extra) and 2: {rows:?}"
    );
    let edges = c.send(&[
        "GRAPH.QUERY",
        "g",
        "MATCH (a:P)-[:R]->(b:P) RETURN a.id, b.id",
    ]);
    assert!(
        edges.contains("*1\r\n*2\r\n:1\r\n:2\r\n"),
        "{when}: the edge 1->2 the DELETE cascaded to must be back: {edges:?}"
    );
}

#[test]
#[ignore = "spawns a real server; set MOON_BIN"]
fn graph_abort_survives_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = start(dir.path(), 1, true);
    let mut c = Conn::open(server.port);
    if !has_graph(&mut c) {
        eprintln!("SKIP: this binary has no `graph` feature (tokio leg)");
        return;
    }
    graph_seed(&mut c);
    graph_assert_seed(&mut c, "before the transaction");
    graph_txn_then_abort(&mut c);
    graph_assert_seed(&mut c, "live, after TXN.ABORT");
    drop(c);
    let_wal_v3_flush();
    let server = restart(server, dir.path(), 1);
    let mut c = Conn::open(server.port);
    graph_assert_seed(&mut c, "after kill -9 + restart");
}

// ---------------------------------------------------------------------------
// Graph: a rollback record the WAL channel refuses (PR #1301 review)
// ---------------------------------------------------------------------------

/// More graph entities than the per-shard WAL append channel holds (4096
/// slots, `shard::event_loop`). The forward writes reach the channel a batch
/// at a time and drain on the 1 ms tick; the rollback emits one record per
/// entity in ONE synchronous stretch, so the channel overflows mid-rollback.
const OVERFLOW_NODES: usize = 6000;

/// `INFO persistence` field `name` as an integer; 0 when the field is absent
/// (a binary that predates it), so the unfixed code fails on the restart
/// invariant — the real defect — rather than on a missing counter.
fn info_int(c: &mut Conn, name: &str) -> i64 {
    let info = c.send(&["INFO", "persistence"]);
    let prefix = format!("{name}:");
    info.lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0)
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

/// A hash tag this connection's shard does NOT own: a `TXN` refuses a write
/// to it. `None` at `--shards 1`.
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
            assert_eq!(c.send(&["DEL", &format!("{{{tag}}}:probe")]), int(0));
        }
    }
    None
}

/// The invariant: a `TXN.ABORT` that answered `+OK` must not come back after
/// a kill -9. On the unfixed code the rollback's records past the channel's
/// capacity were dropped silently: `+OK`, 0 nodes live, 1904 after restart.
/// Fixed, an abort whose records do not all fit answers the WAL refusal and
/// counts the dropped records (the rollback is still applied in memory); one
/// whose records fit answers `+OK` and stays aborted.
fn graph_rollback_overflow_case(shards: usize, remote: bool) {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = start(dir.path(), shards, true);
    let mut c = Conn::open(server.port);
    if !has_graph(&mut c) {
        eprintln!("SKIP: this binary has no `graph` feature (tokio leg)");
        return;
    }
    let tag = if remote {
        remote_tag(&mut c).expect("--shards > 1 has a remote hash tag")
    } else {
        local_tag(&mut c)
    };
    let graph = format!("{{{tag}}}g");
    assert_eq!(c.send(&["GRAPH.CREATE", &graph]), OK);
    let dropped_before = info_int(&mut c, "txn_rollback_wal_dropped");

    assert_eq!(c.send(&["TXN", "BEGIN"]), OK);
    let ids: Vec<String> = (0..OVERFLOW_NODES).map(|i| i.to_string()).collect();
    for chunk in ids.chunks(200) {
        let cmds: Vec<Vec<&str>> = chunk
            .iter()
            .map(|i| vec!["GRAPH.ADDNODE", graph.as_str(), "N", "i", i.as_str()])
            .collect();
        let refs: Vec<&[&str]> = cmds.iter().map(Vec::as_slice).collect();
        let replies = c.pipeline(&refs);
        assert!(
            !replies.contains("\r\n-") && !replies.starts_with('-'),
            "{replies}"
        );
        // Let the forward records drain: this test is about the rollback's.
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(node_count(&mut c, &graph), OVERFLOW_NODES as i64);
    let abort = c.send(&["TXN", "ABORT"]);
    assert_eq!(
        node_count(&mut c, &graph),
        0,
        "the rollback is applied in memory whatever its reply ({abort:?})"
    );
    let dropped_after = info_int(&mut c, "txn_rollback_wal_dropped");
    if abort == OK {
        assert_eq!(dropped_after, dropped_before, "+OK with dropped records");
    } else {
        assert!(
            abort.starts_with("-MOONERR WAL backpressure"),
            "a refused rollback answers the WAL refusal: {abort:?}"
        );
        assert!(
            dropped_after > dropped_before,
            "a refused rollback record is counted in INFO txn_rollback_wal_dropped"
        );
    }
    eprintln!(
        "shards={shards} remote={remote}: TXN.ABORT -> {abort:?}, \
         txn_rollback_wal_dropped {dropped_before} -> {dropped_after}"
    );
    drop(c);
    let_wal_v3_flush();
    let server = restart(server, dir.path(), shards);
    let mut c = Conn::open(server.port);
    let after = node_count(&mut c, &graph);
    if abort == OK {
        assert_eq!(
            after, 0,
            "TXN.ABORT answered +OK, yet {after} aborted graph nodes came back after kill -9"
        );
    }
}

#[test]
#[ignore = "spawns a real server; set MOON_BIN"]
fn graph_rollback_wal_overflow_is_never_acked_shards_1() {
    graph_rollback_overflow_case(1, false);
}

/// The remote leg: the graph lives on another shard, whose
/// `ShardMessage::GraphRollback` handler appends the records.
#[test]
#[ignore = "spawns a real server; set MOON_BIN"]
fn graph_remote_rollback_wal_overflow_is_never_acked_shards_4() {
    graph_rollback_overflow_case(4, true);
}

// ---------------------------------------------------------------------------
// MQ (audit)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "spawns a real server; set MOON_BIN"]
fn mq_publish_intents_do_not_leak_on_abort() {
    let dir = tempfile::tempdir().expect("tempdir");
    let server = start(dir.path(), 1, true);
    let mut c = Conn::open(server.port);
    assert_eq!(c.send(&["MQ", "CREATE", "q"]), OK);
    assert_eq!(c.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(c.send(&["MQ", "PUBLISH", "q", "a", "1"]), "+QUEUED\r\n");
    assert_eq!(c.send(&["TXN", "ABORT"]), OK);
    assert_eq!(c.send(&["XLEN", "q"]), int(0), "aborted publish leaked");
    // Control: a committed publish lands.
    assert_eq!(c.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(c.send(&["MQ", "PUBLISH", "q", "b", "2"]), "+QUEUED\r\n");
    assert_eq!(c.send(&["TXN", "COMMIT"]), OK);
    assert_eq!(c.send(&["XLEN", "q"]), int(1));
    drop(c);
    let_wal_v3_flush();
    let server = restart(server, dir.path(), 1);
    let mut c = Conn::open(server.port);
    assert_eq!(
        c.send(&["XLEN", "q"]),
        int(1),
        "only the committed message survives a restart"
    );
}

// ---------------------------------------------------------------------------
// Replica (monoio master: master-side PSYNC is monoio-only)
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
fn replica_converges_to_the_aborted_to_state() {
    let mdir = tempfile::tempdir().expect("tempdir");
    let rdir = tempfile::tempdir().expect("tempdir");
    let master = start(mdir.path(), 1, false);
    let replica = start(rdir.path(), 1, false);
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

    let tag = local_tag(&mut m);
    let k = KvKeys::new(&tag);
    kv_seed(&mut m, &k);
    assert_eq!(m.send(&["SELECT", "0"]), OK);
    vec_seed(&mut m);
    graph_seed(&mut m);
    assert_eq!(m.send(&["SET", "seeded", "1"]), OK);
    wait_for("the seed on the replica", Duration::from_secs(20), || {
        r.send(&["GET", "seeded"]) == bulk("1")
    });
    vec_assert_seed(&mut r, "replica, before the transaction");
    graph_assert_seed(&mut r, "replica, before the transaction");

    assert_eq!(m.send(&["SELECT", "3"]), OK);
    kv_txn_then_abort(&mut m, &k, true);
    assert_eq!(m.send(&["SELECT", "0"]), OK);
    vec_txn_then_abort(&mut m);
    graph_txn_then_abort(&mut m);
    // A write after every abort: once the replica has it, it has applied
    // everything the master streamed before it.
    assert_eq!(m.send(&["SET", "after-abort", "1"]), OK);
    wait_for(
        "the post-abort marker on the replica",
        Duration::from_secs(20),
        || r.send(&["GET", "after-abort"]) == bulk("1"),
    );

    kv_assert_seed(&mut r, &k, "replica");
    assert_eq!(r.send(&["SELECT", "0"]), OK);
    assert_eq!(
        r.send(&["HGET", "doc:1", "tag"]),
        bulk("old"),
        "replica doc:1"
    );
    assert_eq!(r.send(&["EXISTS", "doc:3"]), int(0), "replica doc:3");
    vec_assert_seed(&mut r, "replica, after the master's TXN.ABORT");
    graph_assert_seed(&mut r, "replica, after the master's TXN.ABORT");
    drop(replica);
    drop(master);
}
