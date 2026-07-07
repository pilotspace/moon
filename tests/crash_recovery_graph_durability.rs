//! W2-9: graph-engine kill-9 crash-recovery integration tests.
//!
//! Under SIGKILL the graph engine's durability rests ENTIRELY on graph WAL
//! command replay: `save_graph_store` (CSR segments + metadata + manifest)
//! runs only on graceful shutdown (`src/shard/event_loop.rs` shutdown branch),
//! so a crashed server boots with `recover_graph_store → Ok(None)` and
//! rebuilds every graph by re-executing `shard-<N>.wal` records through
//! `DispatchReplayEngine::replay_graph_commands`. These tests prove that path
//! end to end against REAL server processes (model: the vector engine's B4
//! suite, `tests/crash_recovery_vector_durability.rs`).
//!
//! Three scenarios, one `#[test]` each:
//!   G1 mutable-tier replay: ADDNODE/ADDEDGE + Cypher CREATE/SET/DELETE
//!      survive kill -9; external node handles are STABLE across replay
//!      (W2-1) so pre-crash handles keep working (GRAPH.NEIGHBORS, RETURN n).
//!   G2 freeze-threshold crossing: >64_000 edges froze a CSR segment before
//!      the crash; replay re-executes all inserts (re-freezing along the way)
//!      and every query answer matches its pre-crash value.
//!   G3 double-crash idempotence: crash → recover → crash again with NO new
//!      writes → recover; second recovery must equal the first (catches
//!      replay-time WAL re-append / double-apply bugs).
//!
//! Crash-point determinism: kill -9 preserves the OS page cache, so a WAL
//! record is durable for these tests as soon as the writer's 1ms flush tick
//! has written it to the fd. Each scenario writes a unique MARKER property
//! value last and polls the on-disk WAL for those bytes before killing —
//! a bounded condition-based wait, never a fixed sleep as synchronization.
//!
//! Run with (monoio default — matches CI):
//!   cargo build --release
//!   cargo test --release --test crash_recovery_graph_durability -- --ignored
//!
//! Requires: built release binary (`MOON_BIN` env var honored, falls back to
//! `target/release/moon` then `target/debug/moon`).

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#![allow(clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Binary resolution (pattern: tests/crash_recovery_vector_durability.rs)
// ---------------------------------------------------------------------------

fn find_moon_binary() -> PathBuf {
    if let Ok(bin) = std::env::var("MOON_BIN") {
        let p = PathBuf::from(bin);
        if p.exists() {
            return p;
        }
    }
    let manifest = env!("CARGO_MANIFEST_DIR");
    let release = PathBuf::from(format!("{manifest}/target/release/moon"));
    if release.exists() {
        return release;
    }
    let debug = PathBuf::from(format!("{manifest}/target/debug/moon"));
    if debug.exists() {
        return debug;
    }
    panic!(
        "No moon binary found. Build with `cargo build --release` or set \
         MOON_BIN=/path/to/moon."
    );
}

fn unique_port() -> u16 {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind :0");
    let port = listener.local_addr().expect("local_addr").port();
    drop(listener);
    port
}

fn unique_dir(suffix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "moon-graphdur-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ))
}

// ---------------------------------------------------------------------------
// Server spawn / crash / restart harness
// ---------------------------------------------------------------------------

/// Kills the wrapped child on drop (SIGKILL + waitpid), plus a best-effort
/// `pkill -9 -f <dir>` backstop keyed on this test's unique temp dir path
/// (documented gotcha: SIGTERM+SO_REUSEPORT hangs — always kill -9).
struct ServerGuard {
    child: Child,
    dir_marker: String,
}

impl ServerGuard {
    fn new(child: Child, dir: &Path) -> Self {
        Self {
            child,
            dir_marker: dir.to_string_lossy().into_owned(),
        }
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            sigkill(&mut self.child);
        } else {
            let _ = self.child.wait();
        }
        let _ = Command::new("pkill")
            .args(["-9", "-f"])
            .arg(&self.dir_marker)
            .output();
    }
}

#[cfg(unix)]
fn sigkill(child: &mut Child) {
    let pid = child.id() as i32;
    unsafe {
        libc::kill(pid, libc::SIGKILL);
    }
    let _ = child.wait();
}

#[cfg(not(unix))]
fn sigkill(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// Wait until nothing is accepting on `port` (SO_REUSEPORT makes a plain
/// bind-check useless; two consecutive refused connects = down).
fn wait_for_port_down(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    let mut consecutive_refused = 0;
    for _ in 0..120 {
        match TcpStream::connect_timeout(&addr.parse().expect("addr"), Duration::from_millis(100)) {
            Ok(_) => {
                consecutive_refused = 0;
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(_) => {
                consecutive_refused += 1;
                if consecutive_refused >= 2 {
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

/// Spawn moon with `--appendonly yes --shards 1`, logs captured per test.
/// `--disk-free-min-pct 0`: the dev volume routinely sits near Moon's 5%
/// diskfull guard; durability here is proven by kill -9 + replay, not by
/// the free-space monitor.
fn spawn_moon(port: u16, dir: &Path) -> ServerGuard {
    std::fs::create_dir_all(dir).expect("create test dir");
    let child = Command::new(find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--appendonly",
            "yes",
            "--disk-offload",
            "disable",
            "--disk-free-min-pct",
            "0",
            "--dir",
        ])
        .arg(dir)
        .env("RUST_LOG", "moon=info")
        .stdout(
            std::fs::File::options()
                .append(true)
                .create(true)
                .open(dir.join("moon.stdout.log"))
                .expect("stdout log"),
        )
        .stderr(
            std::fs::File::options()
                .append(true)
                .create(true)
                .open(dir.join("moon.stderr.log"))
                .expect("stderr log"),
        )
        .spawn()
        .expect("spawn moon (run `cargo build --release` first, or set MOON_BIN)");
    ServerGuard::new(child, dir)
}

/// Restart attempts — a rapid SIGKILL→rebind on the same port can lose a
/// transient EADDRINUSE race (OS timing artifact, not a recovery defect).
const RESTART_ATTEMPTS: usize = 6;

fn start_moon_alive(port: u16, dir: &Path) -> ServerGuard {
    for attempt in 1..=RESTART_ATTEMPTS {
        let mut guard = spawn_moon(port, dir);
        let mut up = false;
        for _ in 0..300 {
            if let Ok(Some(_status)) = guard.child.try_wait() {
                break; // self-terminated (rebind race) — retry
            }
            if TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
                up = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        if up {
            return guard;
        }
        drop(guard);
        if attempt < RESTART_ATTEMPTS {
            std::thread::sleep(Duration::from_millis(300));
        }
    }
    panic!("moon failed to start+serve on port {port} after {RESTART_ATTEMPTS} attempts");
}

/// Crash the server (SIGKILL) and wait until the port is actually down.
fn crash(mut guard: ServerGuard, port: u16) {
    sigkill(&mut guard.child);
    drop(guard); // idempotent (already reaped) + pkill backstop
    wait_for_port_down(port);
}

/// Poll `<dir>/shard-0.wal` until `needle` appears in its bytes — the
/// bounded, condition-based "my last write reached the WAL fd" wait that
/// makes the kill -9 point deterministic (see module doc).
fn wait_for_wal_bytes(dir: &Path, needle: &[u8]) {
    let wal = dir.join("shard-0.wal");
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(bytes) = std::fs::read(&wal) {
            if bytes.windows(needle.len()).any(|w| w == needle) {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "WAL at {} never contained marker {:?} — graph writes are not \
             reaching the shard WAL",
            wal.display(),
            String::from_utf8_lossy(needle),
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ---------------------------------------------------------------------------
// Minimal RESP client with pipelining
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Clone)]
enum V {
    Simple(String),
    Err(String),
    Int(i64),
    Bulk(Vec<u8>),
    Arr(Vec<V>),
    Null,
}

struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Client {
    fn connect(port: u16) -> Self {
        let addr = format!("127.0.0.1:{port}")
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();
        let start = Instant::now();
        let stream = loop {
            match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
                Ok(s) => break s,
                Err(_) if start.elapsed() < Duration::from_secs(30) => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => panic!("server never accepted on port {port}: {e}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let writer = stream.try_clone().unwrap();
        Client {
            reader: BufReader::new(stream),
            writer,
        }
    }

    fn encode(args: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    fn cmd(&mut self, args: &[&[u8]]) -> V {
        self.writer.write_all(&Self::encode(args)).expect("write");
        self.parse()
    }

    /// Pipeline: send every command, then read every reply (G2 bulk load).
    fn pipeline(&mut self, cmds: &[Vec<Vec<u8>>]) -> Vec<V> {
        let mut buf = Vec::new();
        for c in cmds {
            let refs: Vec<&[u8]> = c.iter().map(|a| a.as_slice()).collect();
            buf.extend_from_slice(&Self::encode(&refs));
        }
        self.writer.write_all(&buf).expect("write pipeline");
        cmds.iter().map(|_| self.parse()).collect()
    }

    fn read_line(&mut self) -> String {
        let mut line = Vec::new();
        let mut b = [0u8; 1];
        loop {
            self.reader.read_exact(&mut b).expect("read byte");
            if b[0] == b'\n' {
                break;
            }
            if b[0] != b'\r' {
                line.push(b[0]);
            }
        }
        String::from_utf8_lossy(&line).into_owned()
    }

    fn parse(&mut self) -> V {
        let line = self.read_line();
        let (t, rest) = line.split_at(1);
        match t {
            "+" => V::Simple(rest.to_string()),
            "-" => V::Err(rest.to_string()),
            ":" => V::Int(rest.parse().expect("int")),
            "$" => {
                let n: i64 = rest.parse().expect("bulk len");
                if n < 0 {
                    return V::Null;
                }
                let mut buf = vec![0u8; n as usize + 2];
                self.reader.read_exact(&mut buf).expect("bulk body");
                buf.truncate(n as usize);
                V::Bulk(buf)
            }
            "*" => {
                let n: i64 = rest.parse().expect("arr len");
                if n < 0 {
                    return V::Null;
                }
                (0..n).map(|_| self.parse()).collect::<Vec<_>>().into()
            }
            "%" => {
                // RESP3 map: read 2n values, flatten into an Arr.
                let n: i64 = rest.parse().expect("map len");
                (0..n * 2).map(|_| self.parse()).collect::<Vec<_>>().into()
            }
            "_" => V::Null,
            other => panic!("unexpected RESP type byte {other:?} (line {line:?})"),
        }
    }
}

impl From<Vec<V>> for V {
    fn from(v: Vec<V>) -> Self {
        V::Arr(v)
    }
}

// ---------------------------------------------------------------------------
// Graph helpers
// ---------------------------------------------------------------------------

const G: &[u8] = b"crashg";

fn addnode(c: &mut Client, label: &str, id: u64) -> i64 {
    match c.cmd(&[
        b"GRAPH.ADDNODE",
        G,
        label.as_bytes(),
        b"id",
        id.to_string().as_bytes(),
    ]) {
        V::Int(handle) => handle,
        other => panic!("ADDNODE {label} id={id} failed: {other:?}"),
    }
}

fn addedge(c: &mut Client, src: i64, dst: i64) {
    match c.cmd(&[
        b"GRAPH.ADDEDGE",
        G,
        src.to_string().as_bytes(),
        dst.to_string().as_bytes(),
        b"E",
    ]) {
        V::Int(_) => {}
        other => panic!("ADDEDGE {src}->{dst} failed: {other:?}"),
    }
}

fn query(c: &mut Client, cypher: &str) -> V {
    c.cmd(&[b"GRAPH.QUERY", G, cypher.as_bytes()])
}

/// Rows array from a GRAPH.QUERY reply (`[headers, rows, stats]`), flattened
/// to one V per single-column row.
fn single_column(c: &mut Client, cypher: &str) -> Vec<V> {
    match query(c, cypher) {
        V::Arr(outer) => match outer.get(1) {
            Some(V::Arr(rows)) => rows
                .iter()
                .map(|r| match r {
                    V::Arr(cells) => cells.first().cloned().expect("row has a cell"),
                    other => panic!("malformed row: {other:?}"),
                })
                .collect(),
            other => panic!("malformed query reply, rows slot = {other:?}"),
        },
        V::Err(e) => panic!("query {cypher:?} errored: {e}"),
        other => panic!("malformed query reply: {other:?}"),
    }
}

/// A node cell from `RETURN n` arrives as `Bulk("node:<ext_id>")` over the
/// wire (`value_to_frame`); extract the external handle.
fn node_handle(v: &V) -> i64 {
    match v {
        V::Int(h) => *h,
        V::Bulk(b) => {
            let s = String::from_utf8_lossy(b);
            s.strip_prefix("node:")
                .unwrap_or_else(|| panic!("expected node:<id> cell, got {s:?}"))
                .parse()
                .expect("handle int")
        }
        other => panic!("expected node cell, got {other:?}"),
    }
}

/// Sorted set of integer cells from a single-column query (id sets).
fn int_set(c: &mut Client, cypher: &str) -> BTreeSet<i64> {
    single_column(c, cypher)
        .into_iter()
        .map(|v| match v {
            V::Int(i) => i,
            V::Bulk(b) => String::from_utf8_lossy(&b).parse().expect("int cell"),
            other => panic!("non-integer cell: {other:?}"),
        })
        .collect()
}

/// Write the scenario's last record and block until it is on disk in the
/// shard WAL, so kill -9 cannot race the 1ms flush tick.
fn write_marker_and_sync(c: &mut Client, dir: &Path, marker: &str) {
    match c.cmd(&[b"GRAPH.ADDNODE", G, b"Marker", b"tag", marker.as_bytes()]) {
        V::Int(_) => {}
        other => panic!("marker ADDNODE failed: {other:?}"),
    }
    wait_for_wal_bytes(dir, marker.as_bytes());
}

// ---------------------------------------------------------------------------
// G1: mutable-tier WAL replay — commands + Cypher writes + stable handles
// ---------------------------------------------------------------------------

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn g1_mutable_tier_survives_kill9_with_stable_handles() {
    let port = unique_port();
    let dir = unique_dir("g1");
    let guard = start_moon_alive(port, &dir);
    let mut c = Client::connect(port);

    assert_eq!(
        c.cmd(&[b"GRAPH.CREATE", G]),
        V::Simple("OK".into()),
        "GRAPH.CREATE"
    );

    // 6 nodes in a ring (6 edges — far below the 64K freeze threshold).
    let handles: Vec<i64> = (0..6).map(|i| addnode(&mut c, "N", i)).collect();
    for i in 0..6 {
        addedge(&mut c, handles[i], handles[(i + 1) % 6]);
    }

    // Cypher write path (execute_write_plan → WAL fan-out): create, mutate,
    // delete.
    for q in [
        "CREATE (n:W {id: 100})",
        "MATCH (n:N {id: 3}) SET n.score = 42",
        "MATCH (n:N {id: 5}) DELETE n",
    ] {
        if let V::Err(e) = query(&mut c, q) {
            panic!("write query {q:?} errored: {e}");
        }
    }

    // Pre-crash ground truth.
    let pre_ids = int_set(&mut c, "MATCH (n:N) RETURN n.id");
    assert_eq!(
        pre_ids,
        (0..5).collect(),
        "pre-crash: ids 0..4 live, 5 deleted"
    );
    let pre_handle_2 = match single_column(&mut c, "MATCH (a:N {id: 2}) RETURN a").as_slice() {
        [cell] => node_handle(cell),
        other => panic!("handle query returned {other:?}"),
    };
    assert_eq!(
        pre_handle_2, handles[2],
        "RETURN n yields the ADDNODE handle"
    );

    write_marker_and_sync(&mut c, &dir, "W29-G1-MARKER");
    crash(guard, port);

    // Recover and re-assert everything.
    let guard = start_moon_alive(port, &dir);
    let mut c = Client::connect(port);

    assert_eq!(
        int_set(&mut c, "MATCH (n:N) RETURN n.id"),
        (0..5).collect(),
        "post-crash: DELETE must persist, ids 0..4 must replay"
    );
    assert_eq!(
        int_set(&mut c, "MATCH (n:W) RETURN n.id"),
        BTreeSet::from([100]),
        "post-crash: Cypher CREATE must replay"
    );
    assert_eq!(
        int_set(&mut c, "MATCH (n:N {id: 3}) RETURN n.score"),
        BTreeSet::from([42]),
        "post-crash: Cypher SET must replay"
    );

    // W2-1 e2e: external handles are stable across WAL replay — the handle
    // recorded BEFORE the crash still resolves to the same node.
    let post_handle_2 = match single_column(&mut c, "MATCH (a:N {id: 2}) RETURN a").as_slice() {
        [cell] => node_handle(cell),
        other => panic!("handle query returned {other:?}"),
    };
    assert_eq!(
        post_handle_2, pre_handle_2,
        "external node handle must survive WAL replay (W2-1)"
    );

    // The pre-crash handle works as a command argument too: node 2's ring
    // neighbors are 1 and 3 (node 5 is deleted; 2 is not adjacent to it).
    match c.cmd(&[b"GRAPH.NEIGHBORS", G, pre_handle_2.to_string().as_bytes()]) {
        V::Arr(items) => assert!(
            !items.is_empty(),
            "GRAPH.NEIGHBORS on a pre-crash handle must see ring edges"
        ),
        other => panic!("GRAPH.NEIGHBORS failed: {other:?}"),
    }

    // Post-recovery writes still work and allocate FRESH ids (no aliasing).
    let new_handle = addnode(&mut c, "N", 900);
    assert!(
        !handles.contains(&new_handle),
        "post-recovery ADDNODE must not alias a replayed handle \
         (got {new_handle}, pre-crash handles {handles:?})"
    );

    drop(guard);
}

// ---------------------------------------------------------------------------
// G2: freeze-threshold crossing — frozen tier rebuilt by replay
// ---------------------------------------------------------------------------

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn g2_frozen_tier_rebuilt_from_wal_replay() {
    // GRAPH.CREATE hardcodes edge_threshold = 64_000 (graph_write.rs); the
    // load below crosses it so freeze() fired before the crash. kill -9
    // never persists CSR segments, so recovery must re-execute all inserts
    // (re-freezing along the way) and reach the same answers.
    const NODES: u64 = 300;
    const EDGES: usize = 64_100;

    let port = unique_port();
    let dir = unique_dir("g2");
    let guard = start_moon_alive(port, &dir);
    let mut c = Client::connect(port);

    assert_eq!(c.cmd(&[b"GRAPH.CREATE", G]), V::Simple("OK".into()));

    // Nodes (pipelined), handles harvested from the replies.
    let node_cmds: Vec<Vec<Vec<u8>>> = (0..NODES)
        .map(|i| {
            vec![
                b"GRAPH.ADDNODE".to_vec(),
                G.to_vec(),
                b"N".to_vec(),
                b"id".to_vec(),
                i.to_string().into_bytes(),
            ]
        })
        .collect();
    let handles: Vec<i64> = c
        .pipeline(&node_cmds)
        .into_iter()
        .map(|v| match v {
            V::Int(h) => h,
            other => panic!("pipelined ADDNODE failed: {other:?}"),
        })
        .collect();

    // Edges: deterministic pseudo-random pairs, pipelined in chunks.
    let edge_cmds: Vec<Vec<Vec<u8>>> = (0..EDGES)
        .map(|k| {
            let src = handles[k % NODES as usize];
            let dst = handles[(k * 7 + 1) % NODES as usize];
            vec![
                b"GRAPH.ADDEDGE".to_vec(),
                G.to_vec(),
                src.to_string().into_bytes(),
                dst.to_string().into_bytes(),
                b"E".to_vec(),
            ]
        })
        .collect();
    for chunk in edge_cmds.chunks(1000) {
        for r in c.pipeline(chunk) {
            match r {
                V::Int(_) => {}
                other => panic!("pipelined ADDEDGE failed: {other:?}"),
            }
        }
    }

    // Pre-crash ground truth: full id set + one node's out-neighborhood.
    let pre_ids = int_set(&mut c, "MATCH (n:N) RETURN n.id");
    assert_eq!(pre_ids.len(), NODES as usize, "pre-crash node count");
    let probe = "MATCH (a:N {id: 7})-[:E]->(b) RETURN b.id";
    let pre_probe = int_set(&mut c, probe);
    assert!(!pre_probe.is_empty(), "probe node must have out-edges");

    write_marker_and_sync(&mut c, &dir, "W29-G2-MARKER");
    crash(guard, port);

    // Recovery replays >64K graph commands before serving — start_moon_alive
    // polls the port, and boot only accepts after Phase B replay.
    let guard = start_moon_alive(port, &dir);
    let mut c = Client::connect(port);

    assert_eq!(
        int_set(&mut c, "MATCH (n:N) RETURN n.id"),
        pre_ids,
        "post-crash node id set must match pre-crash exactly"
    );
    assert_eq!(
        int_set(&mut c, probe),
        pre_probe,
        "post-crash traversal answers must match pre-crash (frozen tier \
         rebuilt by replay)"
    );

    // Spot-check handle stability at scale (W2-1): first/middle/last.
    for idx in [0usize, NODES as usize / 2, NODES as usize - 1] {
        let q = format!("MATCH (a:N {{id: {idx}}}) RETURN a");
        match single_column(&mut c, &q).as_slice() {
            [cell] => assert_eq!(
                node_handle(cell),
                handles[idx],
                "handle for id={idx} must survive replay"
            ),
            other => panic!("handle query {q:?} returned {other:?}"),
        }
    }

    drop(guard);
}

// ---------------------------------------------------------------------------
// G3: double-crash idempotence — replay must not re-log or double-apply
// ---------------------------------------------------------------------------

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn g3_double_crash_recovery_is_idempotent() {
    let port = unique_port();
    let dir = unique_dir("g3");
    let guard = start_moon_alive(port, &dir);
    let mut c = Client::connect(port);

    assert_eq!(c.cmd(&[b"GRAPH.CREATE", G]), V::Simple("OK".into()));
    let handles: Vec<i64> = (0..4).map(|i| addnode(&mut c, "N", i)).collect();
    addedge(&mut c, handles[0], handles[1]);
    addedge(&mut c, handles[1], handles[2]);
    if let V::Err(e) = query(&mut c, "MATCH (n:N {id: 0}) SET n.rank = 7") {
        panic!("SET errored: {e}");
    }
    // Assert the SET pre-crash so a post-recovery miss is unambiguously a
    // replay defect, not a harness/query mistake.
    assert_eq!(
        int_set(&mut c, "MATCH (n:N {id: 0}) RETURN n.rank"),
        BTreeSet::from([7]),
        "pre-crash rank"
    );

    write_marker_and_sync(&mut c, &dir, "W29-G3-MARKER");
    crash(guard, port);

    // First recovery: verify, then crash again WITHOUT writing anything.
    let guard = start_moon_alive(port, &dir);
    let mut c = Client::connect(port);
    let first_ids = int_set(&mut c, "MATCH (n:N) RETURN n.id");
    assert_eq!(first_ids, (0..4).collect(), "first recovery id set");
    let first_rank = int_set(&mut c, "MATCH (n:N {id: 0}) RETURN n.rank");
    assert_eq!(first_rank, BTreeSet::from([7]), "first recovery rank");
    let wal_len_after_first = std::fs::metadata(dir.join("shard-0.wal"))
        .map(|m| m.len())
        .unwrap_or(0);
    crash(guard, port);

    // Second recovery: identical answers, and replay must not have grown the
    // WAL with duplicated records (re-logging replayed commands would double
    // the graph on every restart).
    let guard = start_moon_alive(port, &dir);
    let mut c = Client::connect(port);
    assert_eq!(
        int_set(&mut c, "MATCH (n:N) RETURN n.id"),
        first_ids,
        "second recovery must equal the first (idempotent replay)"
    );
    assert_eq!(
        int_set(&mut c, "MATCH (n:N {id: 0}) RETURN n.rank"),
        BTreeSet::from([7]),
        "second recovery rank"
    );
    assert_eq!(
        single_column(&mut c, "MATCH (n:N {id: 1}) RETURN n").len(),
        1,
        "no duplicated nodes after double recovery"
    );
    let wal_len_after_second = std::fs::metadata(dir.join("shard-0.wal"))
        .map(|m| m.len())
        .unwrap_or(0);
    assert_eq!(
        wal_len_after_second, wal_len_after_first,
        "recovery with no new writes must not append to the graph WAL"
    );

    drop(guard);
}
