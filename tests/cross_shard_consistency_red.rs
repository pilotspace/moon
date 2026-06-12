//! ADD task `consistency-dispatch-gaps` — contract v3 failing-first suite.
//!
//! RED tests (fail until the cross-shard COPY/BITOP coordinators and the
//! GRAPH.* owner-shard routing land — frozen contract §3 v3):
//!   - cdg6a_bitop_cross_shard — BITOP AND/OR/XOR/NOT with sources and dest
//!     PROVABLY on different shards (keys picked via moon's own key_to_shard)
//!     computes the Redis-exact result, readable from dest's owning shard.
//!     On main: BITOP routes by args[0] (the literal "AND"), reads sources on
//!     an arbitrary shard, writes dest there too — Int(0) and a missing dest.
//!   - cdg6b_copy_cross_shard — COPY src dst across shards copies the string
//!     value + TTL, honours no-REPLACE (Int(0), dst untouched) and REPLACE;
//!     co-located non-string COPY (hash tags) keeps full-type fidelity.
//!     On main: routed by src, dst written into src's shard — GET dst = nil.
//!   - cdg6c_graph_routing_connection_independent — every one of N fresh
//!     connections can GRAPH.ADDNODE into one graph, and a final fresh
//!     connection sees all N nodes. On main the graph store is per-shard and
//!     commands execute on the CONNECTION's shard: whichever connections the
//!     kernel lands elsewhere get "ERR graph not found" (red on Linux
//!     SO_REUSEPORT spread; macOS may pin all conns to one shard and pass
//!     vacuously — red state for this case is established on the Linux VM).
//!
//! Run alone with: cargo test --test cross_shard_consistency_red

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use moon::shard::dispatch::key_to_shard;

// ---------------------------------------------------------------------------
// Harness (CARGO_BIN_EXE pattern, mirrors wire_reachability_red.rs)
// ---------------------------------------------------------------------------

fn moon_binary() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    let p = l.local_addr().expect("local_addr").port();
    drop(l);
    p
}

fn spawn_moon(port: u16, dir: &std::path::Path, shards: u32) -> Child {
    Command::new(moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--shards",
            &shards.to_string(),
        ])
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
        .spawn()
        .expect("spawn moon (CARGO_BIN_EXE_moon)")
}

struct ServerGuard(Child);
impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn connect(port: u16, deadline: Duration) -> TcpStream {
    let addr = format!("127.0.0.1:{port}")
        .to_socket_addrs()
        .expect("addr")
        .next()
        .expect("one addr");
    let start = Instant::now();
    loop {
        match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            Ok(s) => {
                s.set_read_timeout(Some(Duration::from_secs(5))).ok();
                s.set_write_timeout(Some(Duration::from_secs(5))).ok();
                return s;
            }
            Err(_) if start.elapsed() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!("server never accepted on {port}: {e}"),
        }
    }
}

fn wait_ready(port: u16) -> TcpStream {
    let mut s = connect(port, Duration::from_secs(30));
    let start = Instant::now();
    loop {
        s.write_all(b"PING\r\n").expect("write PING");
        let mut buf = [0u8; 64];
        if let Ok(n) = s.read(&mut buf) {
            if n > 0 && buf[..n].windows(4).any(|w| w == b"PONG") {
                return s;
            }
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "server accepted TCP but never answered PING"
        );
        std::thread::sleep(Duration::from_millis(100));
        s = connect(port, Duration::from_secs(5));
    }
}

// ---------------------------------------------------------------------------
// Minimal RESP2 reader (same shape as wire_reachability_red.rs)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Resp>>),
}

impl Resp {
    fn flat(&self) -> String {
        match self {
            Resp::Simple(s) | Resp::Error(s) => s.clone(),
            Resp::Int(i) => i.to_string(),
            Resp::Bulk(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            Resp::Bulk(None) => "<nil>".into(),
            Resp::Array(Some(items)) => items.iter().map(Resp::flat).collect::<Vec<_>>().join(" "),
            Resp::Array(None) => "<nil-array>".into(),
        }
    }
}

struct Conn {
    s: TcpStream,
    buf: Vec<u8>,
    pos: usize,
}

impl Conn {
    fn new(s: TcpStream) -> Self {
        Conn {
            s,
            buf: Vec::with_capacity(16 * 1024),
            pos: 0,
        }
    }

    fn open(port: u16) -> Self {
        Conn::new(connect(port, Duration::from_secs(10)))
    }

    fn cmd(&mut self, parts: &[&[u8]]) -> Resp {
        let mut req = Vec::with_capacity(64);
        req.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            req.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            req.extend_from_slice(p);
            req.extend_from_slice(b"\r\n");
        }
        self.s.write_all(&req).expect("write cmd");
        self.frame()
    }

    fn cmd_s(&mut self, parts: &[&str]) -> Resp {
        let v: Vec<&[u8]> = parts.iter().map(|p| p.as_bytes()).collect();
        self.cmd(&v)
    }

    fn fill(&mut self) {
        let mut chunk = [0u8; 16 * 1024];
        let n = self.s.read(&mut chunk).expect("read");
        assert!(n > 0, "connection closed mid-frame");
        self.buf.extend_from_slice(&chunk[..n]);
    }

    fn line(&mut self) -> String {
        loop {
            if let Some(rel) = self.buf[self.pos..].windows(2).position(|w| w == b"\r\n") {
                let line =
                    String::from_utf8_lossy(&self.buf[self.pos..self.pos + rel]).into_owned();
                self.pos += rel + 2;
                return line;
            }
            self.fill();
        }
    }

    fn exact(&mut self, n: usize) -> Vec<u8> {
        while self.buf.len() - self.pos < n + 2 {
            self.fill();
        }
        let out = self.buf[self.pos..self.pos + n].to_vec();
        self.pos += n + 2;
        out
    }

    fn frame(&mut self) -> Resp {
        if self.pos > 0 && self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        }
        let line = self.line();
        let (tag, rest) = line.split_at(1);
        match tag {
            "+" => Resp::Simple(rest.to_string()),
            "-" => Resp::Error(rest.to_string()),
            ":" => Resp::Int(rest.parse().unwrap_or(0)),
            "$" => {
                let n: i64 = rest.parse().unwrap_or(-1);
                if n < 0 {
                    Resp::Bulk(None)
                } else {
                    Resp::Bulk(Some(self.exact(n as usize)))
                }
            }
            "*" => {
                let n: i64 = rest.parse().unwrap_or(-1);
                if n < 0 {
                    Resp::Array(None)
                } else {
                    let mut items = Vec::with_capacity(n as usize);
                    for _ in 0..n {
                        items.push(self.frame());
                    }
                    Resp::Array(Some(items))
                }
            }
            other => panic!("unexpected RESP tag {other:?} (line {line:?})"),
        }
    }
}

// ---------------------------------------------------------------------------
// Key picking: provably-cross-shard names via moon's own hash
// ---------------------------------------------------------------------------

const SHARDS: u32 = 4;

/// Return one key per shard (index = shard id), generated deterministically.
fn keys_per_shard(prefix: &str, num_shards: usize) -> Vec<String> {
    let mut out: Vec<Option<String>> = vec![None; num_shards];
    let mut found = 0;
    for i in 0..10_000 {
        let k = format!("{prefix}{i}");
        let s = key_to_shard(k.as_bytes(), num_shards);
        if out[s].is_none() {
            out[s] = Some(k);
            found += 1;
            if found == num_shards {
                break;
            }
        }
    }
    out.into_iter()
        .map(|o| o.expect("found a key for every shard"))
        .collect()
}

// ---------------------------------------------------------------------------
// cdg6a — BITOP across shards
// ---------------------------------------------------------------------------

#[test]
fn cdg6a_bitop_cross_shard() {
    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");
    let _guard = ServerGuard(spawn_moon(port, dir.path(), SHARDS));
    drop(wait_ready(port));

    let ks = keys_per_shard("cdg6a:", SHARDS as usize);
    // sources on shards 0 and 1, dest on shard 2 — all distinct by construction
    let (s1, s2, dest) = (&ks[0], &ks[1], &ks[2]);

    let mut c = Conn::open(port);
    assert_eq!(c.cmd_s(&["SET", s1, "abc"]), Resp::Simple("OK".into()));
    assert_eq!(c.cmd_s(&["SET", s2, "abd"]), Resp::Simple("OK".into()));

    // AND: byte-wise; equal length, no padding involved
    let r = c.cmd_s(&["BITOP", "AND", dest, s1, s2]);
    assert_eq!(r, Resp::Int(3), "BITOP AND result length (got {r:?})");
    let v = c.cmd_s(&["GET", dest]);
    assert_eq!(
        v,
        Resp::Bulk(Some(b"ab`".to_vec())),
        "AND of abc/abd (c&d = `)"
    );

    // OR
    assert_eq!(c.cmd_s(&["BITOP", "OR", dest, s1, s2]), Resp::Int(3));
    assert_eq!(c.cmd_s(&["GET", dest]), Resp::Bulk(Some(b"abg".to_vec())));

    // XOR
    assert_eq!(c.cmd_s(&["BITOP", "XOR", dest, s1, s2]), Resp::Int(3));
    assert_eq!(
        c.cmd_s(&["GET", dest]),
        Resp::Bulk(Some(vec![0x00, 0x00, 0x07]))
    );

    // NOT (single source on a different shard than dest)
    assert_eq!(c.cmd_s(&["BITOP", "NOT", dest, s1]), Resp::Int(3));
    assert_eq!(
        c.cmd_s(&["GET", dest]),
        Resp::Bulk(Some(vec![!b'a', !b'b', !b'c']))
    );

    // NOT arity error preserved
    let e = c.cmd_s(&["BITOP", "NOT", dest, s1, s2]);
    assert!(
        matches!(&e, Resp::Error(m) if m.contains("NOT")),
        "BITOP NOT arity error (got {e:?})"
    );

    // Unequal lengths: shorter source zero-padded to the longest
    assert_eq!(c.cmd_s(&["SET", s2, "abcdef"]), Resp::Simple("OK".into()));
    assert_eq!(c.cmd_s(&["BITOP", "OR", dest, s1, s2]), Resp::Int(6));
    assert_eq!(
        c.cmd_s(&["GET", dest]),
        Resp::Bulk(Some(b"abcdef".to_vec()))
    );

    // All sources missing: dest deleted, Int(0)
    let m1 = &ks[3]; // shard 3, never written
    assert_eq!(
        c.cmd_s(&["BITOP", "AND", dest, m1, "cdg6a:missing:zz"]),
        Resp::Int(0)
    );
    assert_eq!(c.cmd_s(&["GET", dest]), Resp::Bulk(None), "dest deleted");

    // WRONGTYPE propagates from a cross-shard source
    assert_eq!(c.cmd_s(&["RPUSH", m1, "x"]), Resp::Int(1));
    let e = c.cmd_s(&["BITOP", "AND", dest, s1, m1]);
    assert!(
        matches!(&e, Resp::Error(m) if m.starts_with("WRONGTYPE")),
        "wrongtype source (got {e:?})"
    );
}

// ---------------------------------------------------------------------------
// cdg6b — COPY across shards
// ---------------------------------------------------------------------------

#[test]
fn cdg6b_copy_cross_shard() {
    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");
    let _guard = ServerGuard(spawn_moon(port, dir.path(), SHARDS));
    drop(wait_ready(port));

    let ks = keys_per_shard("cdg6b:", SHARDS as usize);
    let (src, dst, dst2) = (&ks[0], &ks[1], &ks[2]);

    let mut c = Conn::open(port);
    assert_eq!(
        c.cmd_s(&["SET", src, "copy_value"]),
        Resp::Simple("OK".into())
    );
    assert_eq!(c.cmd_s(&["PEXPIRE", src, "500000"]), Resp::Int(1));

    // Plain cross-shard COPY: value lands on dst's shard, TTL preserved
    let r = c.cmd_s(&["COPY", src, dst]);
    assert_eq!(r, Resp::Int(1), "COPY src dst (got {r:?})");
    assert_eq!(
        c.cmd_s(&["GET", dst]),
        Resp::Bulk(Some(b"copy_value".to_vec())),
        "dst readable on its own shard"
    );
    match c.cmd_s(&["PTTL", dst]) {
        Resp::Int(ttl) => assert!(ttl > 0 && ttl <= 500_000, "COPY preserves TTL (PTTL={ttl})"),
        other => panic!("PTTL reply {other:?}"),
    }

    // Existing destination without REPLACE → Int(0), dst untouched
    assert_eq!(c.cmd_s(&["SET", dst2, "old"]), Resp::Simple("OK".into()));
    assert_eq!(c.cmd_s(&["COPY", src, dst2]), Resp::Int(0));
    assert_eq!(c.cmd_s(&["GET", dst2]), Resp::Bulk(Some(b"old".to_vec())));

    // REPLACE overwrites
    assert_eq!(c.cmd_s(&["COPY", src, dst2, "REPLACE"]), Resp::Int(1));
    assert_eq!(
        c.cmd_s(&["GET", dst2]),
        Resp::Bulk(Some(b"copy_value".to_vec()))
    );

    // Missing source → Int(0)
    assert_eq!(c.cmd_s(&["COPY", "cdg6b:nope", dst2]), Resp::Int(0));

    // Co-located (hash-tagged) non-string COPY keeps full fidelity
    assert_eq!(c.cmd_s(&["RPUSH", "{cdg6b}list", "a", "b"]), Resp::Int(2));
    assert_eq!(
        c.cmd_s(&["COPY", "{cdg6b}list", "{cdg6b}list2"]),
        Resp::Int(1)
    );
    assert_eq!(
        c.cmd_s(&["LRANGE", "{cdg6b}list2", "0", "-1"]).flat(),
        "a b"
    );

    // Cross-shard NON-string COPY: explicit error, not silent corruption.
    // Find a list key on a different shard than its copy target.
    let lsrc = &ks[3];
    assert_eq!(c.cmd_s(&["DEL", lsrc]), Resp::Int(0));
    assert_eq!(c.cmd_s(&["RPUSH", lsrc, "x"]), Resp::Int(1));
    let e = c.cmd_s(&["COPY", lsrc, dst2, "REPLACE"]);
    assert!(
        matches!(&e, Resp::Error(_)),
        "cross-shard non-string COPY must error explicitly (got {e:?})"
    );
    // and the destination must be untouched by the failed copy
    assert_eq!(
        c.cmd_s(&["GET", dst2]),
        Resp::Bulk(Some(b"copy_value".to_vec()))
    );
}

// ---------------------------------------------------------------------------
// cdg6c — GRAPH commands are connection-independent
// ---------------------------------------------------------------------------

#[test]
fn cdg6c_graph_routing_connection_independent() {
    if !cfg!(feature = "graph") {
        eprintln!("skip: graph feature compiled out (tokio CI feature set)");
        return;
    }
    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");
    let _guard = ServerGuard(spawn_moon(port, dir.path(), SHARDS));
    drop(wait_ready(port));

    let mut c0 = Conn::open(port);
    assert_eq!(
        c0.cmd_s(&["GRAPH.CREATE", "cdg6graph"]),
        Resp::Simple("OK".into())
    );

    // N fresh connections — the kernel spreads them across shard listeners
    // (Linux SO_REUSEPORT). Every single one must reach the graph.
    const N: usize = 12;
    for i in 0..N {
        let mut c = Conn::open(port);
        let r = c.cmd_s(&["GRAPH.ADDNODE", "cdg6graph", ":CdgLabel"]);
        assert!(
            matches!(r, Resp::Int(_)),
            "conn {i}: GRAPH.ADDNODE must succeed from ANY connection (got {r:?})"
        );
    }

    // A final fresh connection sees all N nodes.
    let mut c = Conn::open(port);
    let info = c.cmd_s(&["GRAPH.INFO", "cdg6graph"]).flat();
    assert!(
        info.contains(&N.to_string()),
        "GRAPH.INFO must report {N} nodes from any connection (got: {info})"
    );
}

/// Pull `node_count` out of a GRAPH.INFO reply (RESP2 flattens the map to
/// an array of alternating key/value items).
fn graph_node_count(c: &mut Conn, graph: &str) -> i64 {
    let r = c.cmd_s(&["GRAPH.INFO", graph]);
    let items = match r {
        Resp::Array(Some(items)) => items,
        other => panic!("GRAPH.INFO reply {other:?}"),
    };
    for pair in items.chunks(2) {
        if pair.len() == 2 && pair[0].flat() == "node_count" {
            if let Resp::Int(n) = pair[1] {
                return n;
            }
            return pair[1].flat().parse().expect("node_count value");
        }
    }
    panic!("node_count missing from GRAPH.INFO reply");
}

// ---------------------------------------------------------------------------
// cdg6d — TEMPORAL.INVALIDATE is connection-independent
// ---------------------------------------------------------------------------

#[test]
fn cdg6d_temporal_invalidate_cross_connection() {
    if !cfg!(feature = "graph") {
        eprintln!("skip: graph feature compiled out (tokio CI feature set)");
        return;
    }
    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");
    let _guard = ServerGuard(spawn_moon(port, dir.path(), SHARDS));
    drop(wait_ready(port));

    let mut c0 = Conn::open(port);
    assert_eq!(
        c0.cmd_s(&["GRAPH.CREATE", "cdg6dgraph"]),
        Resp::Simple("OK".into())
    );
    let node_id = match c0.cmd_s(&["GRAPH.ADDNODE", "cdg6dgraph", ":CdgLabel"]) {
        Resp::Int(id) => id.to_string(),
        other => panic!("ADDNODE reply {other:?}"),
    };

    // TEMPORAL.INVALIDATE mutates the graph store, so it must reach the
    // graph's owner shard no matter which shard the connection landed on.
    // On main it executes on the CONNECTION's shard — non-owner connections
    // answer "ERR graph not found".
    for i in 0..12 {
        let mut c = Conn::open(port);
        let r = c.cmd_s(&["TEMPORAL.INVALIDATE", &node_id, "NODE", "cdg6dgraph"]);
        assert_eq!(
            r,
            Resp::Simple("OK".into()),
            "conn {i}: TEMPORAL.INVALIDATE must succeed from ANY connection"
        );
    }
}

// ---------------------------------------------------------------------------
// cdg6e — TXN.ABORT rolls back graph writes routed to other shards
// ---------------------------------------------------------------------------

#[test]
fn cdg6e_txn_abort_rolls_back_routed_graph_write() {
    if !cfg!(feature = "graph") {
        eprintln!("skip: graph feature compiled out (tokio CI feature set)");
        return;
    }
    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");
    let _guard = ServerGuard(spawn_moon(port, dir.path(), SHARDS));
    drop(wait_ready(port));

    let mut c0 = Conn::open(port);
    assert_eq!(
        c0.cmd_s(&["GRAPH.CREATE", "cdg6egraph"]),
        Resp::Simple("OK".into())
    );
    assert!(matches!(
        c0.cmd_s(&["GRAPH.ADDNODE", "cdg6egraph", ":Base"]),
        Resp::Int(_)
    ));
    let baseline = graph_node_count(&mut c0, "cdg6egraph");
    assert_eq!(baseline, 1, "baseline committed node");

    // From many fresh connections (the kernel spreads them across shard
    // listeners on Linux): add a node inside a TXN, then abort. The routed
    // ADDNODE's intent must be captured from the response id and the abort
    // must remove the node on the graph's OWNER shard, not the conn's shard.
    for i in 0..12 {
        let mut c = Conn::open(port);
        assert_eq!(
            c.cmd_s(&["TXN", "BEGIN"]),
            Resp::Simple("OK".into()),
            "conn {i}: TXN BEGIN"
        );
        let r = c.cmd_s(&["GRAPH.ADDNODE", "cdg6egraph", ":Aborted"]);
        assert!(
            matches!(r, Resp::Int(_)),
            "conn {i}: in-txn ADDNODE must succeed from ANY connection (got {r:?})"
        );
        assert_eq!(
            c.cmd_s(&["TXN", "ABORT"]),
            Resp::Simple("OK".into()),
            "conn {i}: TXN ABORT"
        );
    }

    // All 12 aborted adds must be gone, from any connection's view.
    let mut c = Conn::open(port);
    let after = graph_node_count(&mut c, "cdg6egraph");
    assert_eq!(
        after, baseline,
        "TXN.ABORT must roll back routed graph writes (still-visible nodes leak)"
    );
}

// ---------------------------------------------------------------------------
// cdg6f — workspaces are connection-independent (contract v4)
// ---------------------------------------------------------------------------

#[test]
fn cdg6f_workspace_cross_connection() {
    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");
    let _guard = ServerGuard(spawn_moon(port, dir.path(), SHARDS));
    drop(wait_ready(port));

    let mut c0 = Conn::open(port);
    let ws_id = match c0.cmd_s(&["WS", "CREATE", "cdg6fws"]) {
        Resp::Bulk(Some(id)) => String::from_utf8(id).expect("ws id utf8"),
        other => panic!("WS CREATE reply {other:?}"),
    };

    // The registry must be visible from EVERY connection, whichever shard
    // the kernel landed it on. On main the registry is per-shard: AUTH from
    // a non-creating shard answers "ERR workspace not found".
    for i in 0..12 {
        let mut c = Conn::open(port);
        let r = c.cmd_s(&["WS", "AUTH", &ws_id]);
        assert_eq!(
            r,
            Resp::Simple("OK".into()),
            "conn {i}: WS AUTH must succeed from ANY connection"
        );
        let list = c.cmd_s(&["WS", "LIST"]).flat();
        assert!(
            list.contains("cdg6fws"),
            "conn {i}: WS LIST must include the workspace (got: {list})"
        );
    }

    // Bound writes are scoped: SET inside the workspace, visible to a bound
    // GET on the same connection; invisible to an unbound connection.
    let mut bound = Conn::open(port);
    assert_eq!(
        bound.cmd_s(&["WS", "AUTH", &ws_id]),
        Resp::Simple("OK".into())
    );
    assert_eq!(
        bound.cmd_s(&["SET", "cdg6f:key", "wsval"]),
        Resp::Simple("OK".into())
    );
    assert_eq!(
        bound.cmd_s(&["GET", "cdg6f:key"]),
        Resp::Bulk(Some(b"wsval".to_vec()))
    );
    let mut unbound = Conn::open(port);
    assert_eq!(
        unbound.cmd_s(&["GET", "cdg6f:key"]),
        Resp::Bulk(None),
        "workspace keys must be invisible to unbound connections"
    );
}

// ---------------------------------------------------------------------------
// cdg6g — MQ queues are connection-independent (contract v4)
// ---------------------------------------------------------------------------

#[test]
fn cdg6g_mq_cross_connection() {
    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");
    let _guard = ServerGuard(spawn_moon(port, dir.path(), SHARDS));
    drop(wait_ready(port));

    let mut c0 = Conn::open(port);
    assert_eq!(
        c0.cmd_s(&["MQ", "CREATE", "cdg6gq", "MAXDELIVERY", "3"]),
        Resp::Simple("OK".into())
    );

    // PUSH from fresh connections (kernel spreads them across shards on
    // Linux). On main the stream lives in the CONNECTION's shard db, so a
    // non-creating shard answers "queue is not durable / not found".
    for i in 0..6 {
        let mut c = Conn::open(port);
        let r = c.cmd(&[b"MQ", b"PUSH", b"cdg6gq", format!("f{i}").as_bytes(), b"v"]);
        assert!(
            matches!(&r, Resp::Bulk(Some(_))),
            "conn {i}: MQ PUSH must succeed from ANY connection (got {r:?})"
        );
    }

    // POP from another fresh connection must see all 6 messages.
    let mut c = Conn::open(port);
    let popped = c.cmd_s(&["MQ", "POP", "cdg6gq", "COUNT", "6"]).flat();
    for i in 0..6 {
        assert!(
            popped.contains(&format!("f{i}")),
            "MQ POP must return message f{i} pushed from another connection (got: {popped})"
        );
    }

    // DLQ routing: MAXDELIVERY 1 → first un-ACKed POP dead-letters the
    // message; DLQLEN from yet another connection reports it.
    let mut c1 = Conn::open(port);
    assert_eq!(
        c1.cmd_s(&["MQ", "CREATE", "cdg6gdlq", "MAXDELIVERY", "1"]),
        Resp::Simple("OK".into())
    );
    let mut c2 = Conn::open(port);
    assert!(matches!(
        c2.cmd_s(&["MQ", "PUSH", "cdg6gdlq", "df", "dv"]),
        Resp::Bulk(Some(_))
    ));
    let mut c3 = Conn::open(port);
    let _ = c3.cmd_s(&["MQ", "POP", "cdg6gdlq"]);
    let mut c4 = Conn::open(port);
    assert_eq!(
        c4.cmd_s(&["MQ", "DLQLEN", "cdg6gdlq"]),
        Resp::Int(1),
        "DLQLEN must report the dead-lettered message from any connection"
    );
}
