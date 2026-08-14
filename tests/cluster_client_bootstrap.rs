//! Cluster client bootstrap: discovery, routing, replica reads, honest health.
//!
//! Measured against redis-server 8.6.1 (3 masters + 1 replica, then a killed
//! master, 2026-08-14) over raw sockets. Raw sockets because the RESP2/RESP3
//! shape difference IS part of the contract — `CLUSTER SHARDS` returns flat
//! arrays under RESP2 and Maps under RESP3, and every client library
//! normalises that away before a test could see it.
//!
//! One connection per probe where the reply interleaves with anything else:
//! a shared socket desynchronises and silently returns the previous command's
//! bytes. That produced two wrong readings while measuring the oracle.
//!
//! The load-bearing test is `cb1`: until peer slot ownership actually
//! propagates, every other surface here can only report the local node's view,
//! and a client acting on that view writes keys to nodes that do not own them
//! (issue #485).

mod common;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

// ── harness ────────────────────────────────────────────────────────────────

/// Reserve ports whose cluster-bus sibling (`port + 10000`) is also free — the
/// bus binds that unconditionally in cluster mode. Same approach as
/// `cluster_formation.rs`: the ephemeral range would overflow past 65535.
fn reserve_cluster_ports(n: usize) -> Vec<u16> {
    static NEXT_RANGE: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);
    let range = NEXT_RANGE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let start = 22000 + (std::process::id() as u16 % 900) * 5 + range * 300;
    let mut held: Vec<TcpListener> = Vec::new();
    let mut ports = Vec::new();
    for candidate in (start..40000).step_by(3) {
        let Ok(l) = TcpListener::bind(("127.0.0.1", candidate)) else {
            continue;
        };
        let Ok(bus) = TcpListener::bind(("127.0.0.1", candidate + 10000)) else {
            continue;
        };
        held.push(l);
        held.push(bus);
        ports.push(candidate);
        if ports.len() == n {
            return ports;
        }
    }
    panic!("could not reserve {n} ports with free +10000 siblings");
}

/// Kills the whole fleet on drop, so a failed assertion never leaks servers.
/// A leaked moon whose `--dir` is then removed spins CPU indefinitely.
struct Fleet(Vec<Option<Child>>);

impl Fleet {
    /// SIGKILL one node and leave the slot empty — used to drive a real
    /// failure rather than simulating one.
    fn kill(&mut self, idx: usize) {
        if let Some(mut c) = self.0[idx].take() {
            common::sigkill(&mut c);
        }
    }
}

impl Drop for Fleet {
    fn drop(&mut self) {
        for slot in &mut self.0 {
            if let Some(c) = slot.as_mut() {
                common::sigkill(c);
            }
        }
    }
}

fn spawn_node(dir: &std::path::Path, port: u16, extra: &[&str]) -> Child {
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--dir",
            dir.to_str().unwrap(),
            "--disk-free-min-pct",
            "0",
            "--appendonly",
            "no",
            "--cluster-enabled",
            // Short, so a failure test does not wait out Redis's 15s default.
            "--cluster-node-timeout",
            "1000",
        ])
        .args(extra)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn moon")
}

// ── a small RESP reader (RESP2 + RESP3) ────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Val {
    Str(String),
    Int(i64),
    Arr(Vec<Val>),
    /// RESP3 `%` — kept DISTINCT from Arr on purpose: the whole point of the
    /// RESP3 clause is that a Map is not an Array.
    Map(Vec<(Val, Val)>),
    Nil,
    Err(String),
}

impl Val {
    fn as_str(&self) -> &str {
        match self {
            Val::Str(s) => s,
            _ => panic!("expected string, got {self:?}"),
        }
    }
    fn arr(&self) -> &[Val] {
        match self {
            Val::Arr(v) => v,
            _ => panic!("expected array, got {self:?}"),
        }
    }
    /// Field lookup that works for BOTH wire shapes, so a test can assert on
    /// content without caring which protocol produced it.
    fn field(&self, key: &str) -> Option<&Val> {
        match self {
            Val::Map(pairs) => pairs
                .iter()
                .find(|(k, _)| matches!(k, Val::Str(s) if s == key))
                .map(|(_, v)| v),
            Val::Arr(items) => items
                .chunks(2)
                .find(|c| c.len() == 2 && matches!(&c[0], Val::Str(s) if s == key))
                .map(|c| &c[1]),
            _ => None,
        }
    }
    fn keys(&self) -> Vec<String> {
        match self {
            Val::Map(pairs) => pairs.iter().map(|(k, _)| k.as_str().to_string()).collect(),
            Val::Arr(items) => items
                .chunks(2)
                .filter(|c| c.len() == 2)
                .map(|c| c[0].as_str().to_string())
                .collect(),
            _ => vec![],
        }
    }
}

struct Conn {
    s: TcpStream,
    buf: Vec<u8>,
}

impl Conn {
    fn open(port: u16) -> Self {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
                s.set_read_timeout(Some(Duration::from_secs(15)))
                    .expect("read timeout");
                return Conn { s, buf: Vec::new() };
            }
            assert!(Instant::now() < deadline, "connect to :{port} kept failing");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn ready(port: u16) -> Self {
        let mut c = Conn::open(port);
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Val::Str(s) = c.cmd(&["PING"])
                && s == "PONG"
            {
                return c;
            }
            assert!(
                Instant::now() < deadline,
                "node :{port} never answered PING"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn cmd(&mut self, parts: &[&str]) -> Val {
        let mut out = format!("*{}\r\n", parts.len()).into_bytes();
        for p in parts {
            out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            out.extend_from_slice(p.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        self.s.write_all(&out).expect("write");
        self.read_value()
    }

    fn read_value(&mut self) -> Val {
        loop {
            let mut cur = 0usize;
            if let Some(v) = parse(&self.buf, &mut cur) {
                self.buf.drain(..cur);
                return v;
            }
            let mut chunk = [0u8; 65536];
            let n = self.s.read(&mut chunk).expect("read reply");
            assert!(n > 0, "connection closed mid-reply");
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }
}

fn line_end(b: &[u8], from: usize) -> Option<usize> {
    b[from..]
        .windows(2)
        .position(|w| w == b"\r\n")
        .map(|p| from + p)
}

fn parse(b: &[u8], cur: &mut usize) -> Option<Val> {
    if *cur >= b.len() {
        return None;
    }
    let tag = b[*cur];
    let eol = line_end(b, *cur + 1)?;
    let head = std::str::from_utf8(&b[*cur + 1..eol]).ok()?.to_string();
    let after = eol + 2;
    match tag {
        b'+' => {
            *cur = after;
            Some(Val::Str(head))
        }
        b'-' => {
            *cur = after;
            Some(Val::Err(head))
        }
        b':' => {
            *cur = after;
            Some(Val::Int(head.parse().ok()?))
        }
        b'$' => {
            let len: i64 = head.parse().ok()?;
            if len < 0 {
                *cur = after;
                return Some(Val::Nil);
            }
            let end = after + len as usize;
            if b.len() < end + 2 {
                return None;
            }
            *cur = end + 2;
            Some(Val::Str(
                String::from_utf8_lossy(&b[after..end]).into_owned(),
            ))
        }
        b'*' | b'~' | b'>' => {
            let n: i64 = head.parse().ok()?;
            if n < 0 {
                *cur = after;
                return Some(Val::Nil);
            }
            let mut c = after;
            let mut items = Vec::new();
            for _ in 0..n {
                items.push(parse(b, &mut c)?);
            }
            *cur = c;
            Some(Val::Arr(items))
        }
        b'%' => {
            let n: i64 = head.parse().ok()?;
            let mut c = after;
            let mut pairs = Vec::new();
            for _ in 0..n {
                let k = parse(b, &mut c)?;
                let v = parse(b, &mut c)?;
                pairs.push((k, v));
            }
            *cur = c;
            Some(Val::Map(pairs))
        }
        b'_' => {
            *cur = after;
            Some(Val::Nil)
        }
        _ => {
            *cur = after;
            Some(Val::Str(head))
        }
    }
}

// ── cluster formation ──────────────────────────────────────────────────────

struct Cluster {
    _fleet: Fleet,
    _dirs: Vec<tempfile::TempDir>,
    ports: Vec<u16>,
    ids: Vec<String>,
}

impl Cluster {
    fn conn(&self, idx: usize) -> Conn {
        Conn::ready(self.ports[idx])
    }
}

fn info_field(c: &mut Conn, cmd: &[&str], key: &str) -> Option<String> {
    match c.cmd(cmd) {
        Val::Str(s) => s
            .lines()
            .find_map(|l| l.strip_prefix(&format!("{key}:")))
            .map(|v| v.trim().to_string()),
        _ => None,
    }
}

/// Three masters, MET, with all 16384 slots assigned in equal thirds.
///
/// Deliberately does NOT wait for slot convergence — that is what `cb1` is
/// for. Waiting here would turn a propagation failure into a harness timeout
/// and hide which assertion actually broke.
fn form_cluster(n: usize) -> Cluster {
    let dirs: Vec<tempfile::TempDir> = (0..n).map(|_| tempfile::tempdir().unwrap()).collect();
    let ports = reserve_cluster_ports(n);
    let fleet = Fleet(
        dirs.iter()
            .zip(&ports)
            .map(|(d, &p)| Some(spawn_node(d.path(), p, &[])))
            .collect(),
    );

    let mut conns: Vec<Conn> = ports.iter().map(|&p| Conn::ready(p)).collect();
    let ids: Vec<String> = conns
        .iter_mut()
        .map(|c| c.cmd(&["CLUSTER", "MYID"]).as_str().to_string())
        .collect();
    for id in &ids {
        assert_eq!(id.len(), 40, "CLUSTER MYID must be 40 hex chars");
    }

    for (i, &p) in ports.iter().enumerate().skip(1) {
        let r = conns[0].cmd(&["CLUSTER", "MEET", "127.0.0.1", &p.to_string()]);
        assert_eq!(r, Val::Str("OK".into()), "MEET node {i} failed: {r:?}");
    }

    // Mesh convergence (known_nodes) works today; slot convergence is the
    // thing under test.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let all = conns.iter_mut().all(|c| {
            info_field(c, &["CLUSTER", "INFO"], "cluster_known_nodes")
                .and_then(|v| v.parse::<usize>().ok())
                == Some(n)
        });
        if all {
            break;
        }
        assert!(Instant::now() < deadline, "cluster mesh never formed");
        std::thread::sleep(Duration::from_millis(200));
    }

    // Assign slots in equal thirds. ADDSLOTS takes individual slots, so this
    // is chunked to keep each command a sane size.
    let per = 16384 / n;
    for (i, c) in conns.iter_mut().enumerate() {
        let lo = i * per;
        let hi = if i == n - 1 { 16383 } else { (i + 1) * per - 1 };
        for chunk_start in (lo..=hi).step_by(2000) {
            let chunk_end = (chunk_start + 1999).min(hi);
            let slots: Vec<String> = (chunk_start..=chunk_end).map(|s| s.to_string()).collect();
            let mut args: Vec<&str> = vec!["CLUSTER", "ADDSLOTS"];
            args.extend(slots.iter().map(|s| s.as_str()));
            let r = c.cmd(&args);
            assert_eq!(r, Val::Str("OK".into()), "ADDSLOTS on node {i}: {r:?}");
        }
    }

    Cluster {
        _fleet: fleet,
        _dirs: dirs,
        ports,
        ids,
    }
}

/// Wait until every node agrees the whole keyspace is covered, or give up.
fn await_slot_convergence(cl: &Cluster, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let all = (0..cl.ports.len()).all(|i| {
            let mut c = cl.conn(i);
            info_field(&mut c, &["CLUSTER", "INFO"], "cluster_slots_assigned")
                .and_then(|v| v.parse::<usize>().ok())
                == Some(16384)
        });
        if all {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// Which node index owns this key's slot, per the assignment `form_cluster` made.
fn owner_of(cl: &Cluster, key: &str) -> usize {
    let mut c = cl.conn(0);
    let slot = match c.cmd(&["CLUSTER", "KEYSLOT", key]) {
        Val::Int(i) => i as usize,
        other => panic!("CLUSTER KEYSLOT returned {other:?}"),
    };
    let per = 16384 / cl.ports.len();
    (slot / per).min(cl.ports.len() - 1)
}

// ── M1: ownership converges without an epoch bump ──────────────────────────

#[test]
fn cb1_slot_ownership_converges_without_an_epoch_bump() {
    // The whole task rests on this. Gossip carries a 2048-byte slot bitmap,
    // but merges it only under `msg.config_epoch > entry.epoch`; ADDSLOTS
    // never bumps the epoch, so every node sits at 0, `0 > 0` is false, and
    // peer ownership never merges — permanently. See #485.
    let cl = form_cluster(3);
    assert!(
        await_slot_convergence(&cl, Duration::from_secs(30)),
        "every node must converge on cluster_slots_assigned:16384; a node that \
         only counts its own slots cannot answer CLUSTER SHARDS, cannot emit \
         MOVED, and cannot tell whether the keyspace is covered"
    );
}

#[test]
fn cb2_peer_slot_ranges_are_visible_in_cluster_nodes() {
    // The same defect seen through the other window a client may read.
    let cl = form_cluster(3);
    assert!(await_slot_convergence(&cl, Duration::from_secs(30)));

    let mut c = cl.conn(0);
    let nodes = c.cmd(&["CLUSTER", "NODES"]).as_str().to_string();
    for (i, id) in cl.ids.iter().enumerate() {
        let row = nodes
            .lines()
            .find(|l| l.starts_with(id.as_str()))
            .unwrap_or_else(|| panic!("node {i} ({id}) missing from CLUSTER NODES:\n{nodes}"));
        assert!(
            row.split_whitespace()
                .any(|t| t.contains('-') && t.chars().next().is_some_and(|ch| ch.is_ascii_digit())),
            "node {i} row carries no slot range — peers show `connected -` when \
             ownership never propagated:\n{row}"
        );
    }
}

// ── M2 / M3: routing ───────────────────────────────────────────────────────

#[test]
fn cb3_key_for_another_nodes_slot_is_redirected_not_served() {
    // Measured on redis-server: MOVED <slot> <host>:<port>. Measured on Moon
    // today: `OK`, stored on the wrong node, invisible to the owner (#485).
    let cl = form_cluster(3);
    assert!(await_slot_convergence(&cl, Duration::from_secs(30)));

    let key = "foo";
    let owner = owner_of(&cl, key);
    let other = (owner + 1) % cl.ports.len();

    let mut c = cl.conn(other);
    let r = c.cmd(&["SET", key, "written-to-the-wrong-node"]);
    match &r {
        Val::Err(e) => assert!(e.starts_with("MOVED "), "expected MOVED, got error {e:?}"),
        other => panic!("a key owned by another node must be redirected, got {other:?}"),
    }

    // And the write must NOT have landed locally — a MOVED that still stored
    // the value would be the same divergence with a better error message.
    let mut oc = cl.conn(owner);
    assert_eq!(
        oc.cmd(&["GET", key]),
        Val::Nil,
        "the redirected write must not have been applied anywhere"
    );
}

#[test]
fn cb4_lone_node_with_no_slots_still_serves_locally() {
    // The bootstrap case `route_slot`'s fallback exists for. This one is GREEN
    // before the fix and must stay green after: narrowing the fallback must not
    // break a single node that has never been told about any slots.
    let dir = tempfile::tempdir().unwrap();
    let port = reserve_cluster_ports(1)[0];
    let _fleet = Fleet(vec![Some(spawn_node(dir.path(), port, &[]))]);
    let mut c = Conn::ready(port);
    assert_eq!(c.cmd(&["SET", "k", "v"]), Val::Str("OK".into()));
    assert_eq!(c.cmd(&["GET", "k"]), Val::Str("v".into()));
}

#[test]
fn cb21_an_unclaimed_slot_says_hash_slot_not_served_not_cluster_is_down() {
    // Redis has TWO CLUSTERDOWN messages and a client can distinguish them:
    // `Hash slot not served` means THIS slot has no owner (the cluster may be
    // perfectly healthy otherwise), while `The cluster is down` means
    // cluster_state is fail. Conflating them tells an operator the whole
    // cluster is down when one slot lost its owner.
    //
    // Measured against redis-server (3 masters, cluster-require-full-coverage
    // no, CLUSTER DELSLOTS 12182 on the owner, then GET and SET of a key
    // hashing to 12182 on that same node): both answered
    //   CLUSTERDOWN Hash slot not served
    // and cluster_state stayed `ok`, confirming the per-slot reading.
    let cl = form_cluster(3);
    assert!(await_slot_convergence(&cl, Duration::from_secs(30)));

    let key = "foo";
    let owner = owner_of(&cl, key);
    let slot = 12182; // CRC16("foo") & 16383

    let mut oc = cl.conn(owner);
    assert_eq!(
        oc.cmd(&["CLUSTER", "DELSLOTS", &slot.to_string()]),
        Val::Str("OK".into()),
        "DELSLOTS on the owner must succeed to create the coverage hole"
    );

    // Ask the EX-OWNER, which is the node that knows nobody owns the slot. A
    // peer that has not yet learned of the DELSLOTS still answers MOVED, so
    // this assertion only means anything against the ex-owner itself.
    for cmd in [
        vec!["GET", key],
        vec!["SET", key, "must-not-be-served-anywhere"],
    ] {
        match oc.cmd(&cmd) {
            Val::Err(e) => assert_eq!(
                e, "CLUSTERDOWN Hash slot not served",
                "{cmd:?} on a slot nobody owns must name the SLOT, not the cluster"
            ),
            other => panic!(
                "{cmd:?} on an unowned slot must be refused, got {other:?} — \
                 serving it is #485 with extra steps"
            ),
        }
    }
}

// ── M4-M9: CLUSTER SHARDS ──────────────────────────────────────────────────

fn shards(c: &mut Conn) -> Vec<Val> {
    match c.cmd(&["CLUSTER", "SHARDS"]) {
        Val::Arr(v) => v,
        other => panic!("CLUSTER SHARDS must return an Array at the top level, got {other:?}"),
    }
}

#[test]
#[ignore = "red until batch 4 (CLUSTER SHARDS) lands; see .add/tasks/cluster-client-bootstrap TASK.md §5"]
fn cb5_cluster_shards_reports_every_shard_cluster_wide() {
    let cl = form_cluster(3);
    assert!(await_slot_convergence(&cl, Duration::from_secs(30)));

    for i in 0..3 {
        let mut c = cl.conn(i);
        let sh = shards(&mut c);
        assert_eq!(
            sh.len(),
            3,
            "node {i} must report all three shards, not just its own"
        );
        // Every slot accounted for exactly once, across all shards.
        let mut covered = 0usize;
        for s in &sh {
            let slots = s
                .field("slots")
                .expect("shard has a slots field")
                .arr()
                .to_vec();
            assert!(
                slots.len() % 2 == 0,
                "slots is a flat array of integer PAIRS, got {slots:?}"
            );
            for pair in slots.chunks(2) {
                let (Val::Int(lo), Val::Int(hi)) = (&pair[0], &pair[1]) else {
                    panic!("slot bounds are integers, got {pair:?}");
                };
                covered += (hi - lo + 1) as usize;
            }
        }
        assert_eq!(covered, 16384, "node {i} must account for every slot");
    }
}

#[test]
#[ignore = "red until batch 4 (CLUSTER SHARDS) lands; see .add/tasks/cluster-client-bootstrap TASK.md §5"]
fn cb6_shard_and_node_entries_are_maps_under_resp3_and_arrays_under_resp2() {
    // Measured: the top level stays an Array in BOTH protocols; only the shard
    // and node entries change shape. Copying the RESP2 form into RESP3 is the
    // easy mistake, and no content assertion would catch it.
    let cl = form_cluster(3);
    assert!(await_slot_convergence(&cl, Duration::from_secs(30)));

    let mut c2 = cl.conn(0);
    let s2 = shards(&mut c2);
    assert!(
        matches!(s2[0], Val::Arr(_)),
        "under RESP2 a shard entry is a flat Array, got {:?}",
        s2[0]
    );
    let nodes2 = s2[0].field("nodes").unwrap().arr().to_vec();
    assert!(
        matches!(nodes2[0], Val::Arr(_)),
        "under RESP2 a node entry is a flat Array, got {:?}",
        nodes2[0]
    );

    let mut c3 = cl.conn(0);
    assert!(!matches!(c3.cmd(&["HELLO", "3"]), Val::Err(_)));
    let s3 = shards(&mut c3);
    assert!(
        matches!(s3[0], Val::Map(_)),
        "under RESP3 a shard entry is a Map, got {:?}",
        s3[0]
    );
    let nodes3 = s3[0].field("nodes").unwrap().arr().to_vec();
    assert!(
        matches!(nodes3[0], Val::Map(_)),
        "under RESP3 a node entry is a Map, got {:?}",
        nodes3[0]
    );
}

#[test]
#[ignore = "red until batch 4 (CLUSTER SHARDS) lands; see .add/tasks/cluster-client-bootstrap TASK.md §5"]
fn cb7_node_entry_carries_exactly_the_seven_measured_fields() {
    let cl = form_cluster(3);
    assert!(await_slot_convergence(&cl, Duration::from_secs(30)));
    let mut c = cl.conn(0);
    let sh = shards(&mut c);
    let node = sh[0].field("nodes").unwrap().arr()[0].clone();
    let keys = node.keys();
    assert_eq!(
        keys,
        vec![
            "id",
            "port",
            "ip",
            "endpoint",
            "role",
            "replication-offset",
            "health"
        ],
        "field set and ORDER are both measured; a client may read positionally \
         under RESP2"
    );
    assert_eq!(node.field("id").unwrap().as_str().len(), 40);
    assert!(matches!(node.field("port"), Some(Val::Int(_))));
    assert!(matches!(
        node.field("replication-offset"),
        Some(Val::Int(_))
    ));
    assert_eq!(node.field("role").unwrap().as_str(), "master");
    assert_eq!(node.field("health").unwrap().as_str(), "online");
}

#[test]
#[ignore = "red until batch 4 (CLUSTER SHARDS) lands; see .add/tasks/cluster-client-bootstrap TASK.md §5"]
fn cb8_a_master_and_its_replica_share_one_shard_entry() {
    let cl = form_cluster(3);
    assert!(await_slot_convergence(&cl, Duration::from_secs(30)));

    // A fourth node, replicating node 0.
    let dir = tempfile::tempdir().unwrap();
    let rport = reserve_cluster_ports(1)[0];
    let _replica = Fleet(vec![Some(spawn_node(dir.path(), rport, &[]))]);
    let mut rc = Conn::ready(rport);
    let mut c0 = cl.conn(0);
    assert_eq!(
        c0.cmd(&["CLUSTER", "MEET", "127.0.0.1", &rport.to_string()]),
        Val::Str("OK".into())
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let r = rc.cmd(&["CLUSTER", "REPLICATE", &cl.ids[0]]);
        if r == Val::Str("OK".into()) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "CLUSTER REPLICATE kept failing: {r:?}"
        );
        std::thread::sleep(Duration::from_millis(300));
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut c = cl.conn(1);
        let sh = shards(&mut c);
        let with_two = sh
            .iter()
            .find(|s| s.field("nodes").map(|n| n.arr().len()) == Some(2));
        if let Some(shard) = with_two {
            assert_eq!(
                sh.len(),
                3,
                "the shard COUNT must stay 3 — a replica joins a shard, it does \
                 not create one"
            );
            let nodes = shard.field("nodes").unwrap().arr().to_vec();
            assert_eq!(
                nodes[0].field("role").unwrap().as_str(),
                "master",
                "master is listed first"
            );
            assert_eq!(nodes[1].field("role").unwrap().as_str(), "replica");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "no shard ever reported two nodes: {sh:?}"
        );
        std::thread::sleep(Duration::from_millis(300));
    }
}

#[test]
#[ignore = "red until batch 4 (CLUSTER SHARDS) lands; see .add/tasks/cluster-client-bootstrap TASK.md §5"]
fn cb9_a_failed_node_is_reported_with_health_fail_and_empty_slots() {
    // Measured: the dead node stays listed as `role: master, health: fail`, and
    // its shard's slots array becomes empty. Reporting `role` and `health`
    // together is impossible while NodeFlags conflates them (M19).
    let cl = form_cluster(3);
    assert!(await_slot_convergence(&cl, Duration::from_secs(30)));
    let dead_id = cl.ids[2].clone();
    let mut fleet_ref = cl;
    fleet_ref._fleet.kill(2);

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut c = fleet_ref.conn(0);
        let sh = shards(&mut c);
        let dead = sh.iter().find_map(|s| {
            s.field("nodes")?
                .arr()
                .iter()
                .find(|n| {
                    n.field("id")
                        .map(|i| i.as_str() == dead_id)
                        .unwrap_or(false)
                })
                .map(|n| (s.clone(), n.clone()))
        });
        if let Some((shard, node)) = dead
            && node.field("health").map(|h| h.as_str() == "fail") == Some(true)
        {
            assert_eq!(
                node.field("role").unwrap().as_str(),
                "master",
                "a dead master keeps its ROLE — health and role are independent \
                 axes (M19); the single NodeFlags enum cannot express this"
            );
            assert!(
                shard.field("slots").unwrap().arr().is_empty(),
                "a shard with no live owner reports an EMPTY slots array"
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the failed node never reached health:fail in CLUSTER SHARDS"
        );
        std::thread::sleep(Duration::from_millis(300));
    }
}

#[test]
#[ignore = "red until batch 4 (CLUSTER MYSHARDID) lands; see .add/tasks/cluster-client-bootstrap TASK.md §5"]
fn cb10_myshardid_is_stable_and_identical_within_a_shard() {
    let cl = form_cluster(3);
    assert!(await_slot_convergence(&cl, Duration::from_secs(30)));
    let mut c = cl.conn(0);
    let first = c.cmd(&["CLUSTER", "MYSHARDID"]);
    let id = match &first {
        Val::Str(s) => s.clone(),
        other => panic!("CLUSTER MYSHARDID must return a bulk string, got {other:?}"),
    };
    assert_eq!(id.len(), 40, "shard id is 40 hex chars, got {id:?}");
    assert_eq!(
        c.cmd(&["CLUSTER", "MYSHARDID"]),
        first,
        "stable across calls"
    );

    // Distinct shards must not share an id.
    let mut c1 = cl.conn(1);
    assert_ne!(
        c1.cmd(&["CLUSTER", "MYSHARDID"]).as_str(),
        id,
        "different shards have different ids"
    );
}

// ── M10-M13: READONLY / READWRITE ──────────────────────────────────────────

#[test]
#[ignore = "red until batch 5 (READONLY/READWRITE) lands; see .add/tasks/cluster-client-bootstrap TASK.md §5"]
fn cb11_readonly_and_readwrite_are_accepted_on_a_cluster_instance() {
    let dir = tempfile::tempdir().unwrap();
    let port = reserve_cluster_ports(1)[0];
    let _fleet = Fleet(vec![Some(spawn_node(dir.path(), port, &[]))]);
    let mut c = Conn::ready(port);
    assert_eq!(c.cmd(&["READONLY"]), Val::Str("OK".into()));
    assert_eq!(c.cmd(&["READWRITE"]), Val::Str("OK".into()));
}

#[test]
#[ignore = "red until batch 5 (READONLY/READWRITE) lands; see .add/tasks/cluster-client-bootstrap TASK.md §5"]
fn cb12_readonly_serves_replica_reads_but_never_replica_writes() {
    // The measured asymmetry, and the reason READONLY is not just a flag that
    // returns +OK: reads are served locally, writes still redirect.
    let cl = form_cluster(3);
    assert!(await_slot_convergence(&cl, Duration::from_secs(30)));

    let dir = tempfile::tempdir().unwrap();
    let rport = reserve_cluster_ports(1)[0];
    let _replica = Fleet(vec![Some(spawn_node(dir.path(), rport, &[]))]);
    let mut rc = Conn::ready(rport);
    let mut c0 = cl.conn(0);
    assert_eq!(
        c0.cmd(&["CLUSTER", "MEET", "127.0.0.1", &rport.to_string()]),
        Val::Str("OK".into())
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while rc.cmd(&["CLUSTER", "REPLICATE", &cl.ids[0]]) != Val::Str("OK".into()) {
        assert!(Instant::now() < deadline, "CLUSTER REPLICATE kept failing");
        std::thread::sleep(Duration::from_millis(300));
    }

    // A key owned by node 0, written through its master.
    let key = (0..)
        .map(|i| format!("k{i}"))
        .find(|k| owner_of(&cl, k) == 0)
        .expect("a key in node 0's range");
    assert_eq!(c0.cmd(&["SET", &key, "v1"]), Val::Str("OK".into()));

    // Without READONLY the replica redirects.
    match rc.cmd(&["GET", &key]) {
        Val::Err(e) => assert!(e.starts_with("MOVED "), "expected MOVED, got {e:?}"),
        other => panic!("a replica must redirect reads until READONLY, got {other:?}"),
    }

    assert_eq!(rc.cmd(&["READONLY"]), Val::Str("OK".into()));
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match rc.cmd(&["GET", &key]) {
            Val::Str(s) if s == "v1" => break,
            other => {
                assert!(
                    Instant::now() < deadline,
                    "READONLY must let the replica serve the read, got {other:?}"
                );
                std::thread::sleep(Duration::from_millis(300));
            }
        }
    }

    // Writes are still redirected — replica reads never become replica writes.
    match rc.cmd(&["SET", &key, "v2"]) {
        Val::Err(e) => assert!(
            e.starts_with("MOVED "),
            "a WRITE on a READONLY replica must still redirect, got {e:?}"
        ),
        other => panic!("replica accepted a write under READONLY: {other:?}"),
    }

    // And READWRITE restores redirection for reads.
    assert_eq!(rc.cmd(&["READWRITE"]), Val::Str("OK".into()));
    match rc.cmd(&["GET", &key]) {
        Val::Err(e) => assert!(e.starts_with("MOVED "), "expected MOVED, got {e:?}"),
        other => panic!("READWRITE must restore redirection, got {other:?}"),
    }
}

// ── M14-M16: honest health, fail-closed ────────────────────────────────────

#[test]
fn cb15_cluster_state_and_slot_counters_reflect_reality() {
    let cl = form_cluster(3);
    assert!(await_slot_convergence(&cl, Duration::from_secs(30)));

    let mut c = cl.conn(0);
    assert_eq!(
        info_field(&mut c, &["CLUSTER", "INFO"], "cluster_state").as_deref(),
        Some("ok"),
        "a fully covered cluster is ok"
    );
    assert_eq!(
        info_field(&mut c, &["CLUSTER", "INFO"], "cluster_slots_ok").as_deref(),
        Some("16384")
    );

    let mut cl = cl;
    cl._fleet.kill(2);

    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let mut c = cl.conn(0);
        let state = info_field(&mut c, &["CLUSTER", "INFO"], "cluster_state");
        if state.as_deref() == Some("fail") {
            let fail_ct: usize = info_field(&mut c, &["CLUSTER", "INFO"], "cluster_slots_fail")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            assert!(
                fail_ct > 0,
                "cluster_slots_fail must be a real count, not the hardcoded 0"
            );
            let ok_ct: usize = info_field(&mut c, &["CLUSTER", "INFO"], "cluster_slots_ok")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            assert!(
                ok_ct < 16384,
                "cluster_slots_ok must drop when a shard loses its owner"
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "cluster_state never became fail after a master died — it is a \
             constant today (ClusterStatus is assigned only in its initialiser)"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

#[test]
fn cb16_a_degraded_cluster_refuses_keyspace_traffic_even_for_local_slots() {
    // ⚠ freeze flag 2: measured on redis-server — CLUSTERDOWN is returned even
    // for a key in a slot the receiving node OWNS and could serve. A
    // partially-visible keyspace is worse than a refusal.
    let cl = form_cluster(3);
    assert!(await_slot_convergence(&cl, Duration::from_secs(30)));

    let local_key = (0..)
        .map(|i| format!("k{i}"))
        .find(|k| owner_of(&cl, k) == 0)
        .expect("a key node 0 owns");

    let mut cl = cl;
    cl._fleet.kill(2);

    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let mut c = cl.conn(0);
        if let Val::Err(e) = c.cmd(&["GET", &local_key])
            && e.starts_with("CLUSTERDOWN")
        {
            assert_eq!(e, "CLUSTERDOWN The cluster is down", "verbatim Redis text");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "a degraded cluster must refuse even a locally-owned key; serving \
             it hands the client a partial view it cannot detect"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

// ── M17 / M18: a client can tell what it is talking to ─────────────────────

#[test]
#[ignore = "red until batch 6 (INFO identity) lands; see .add/tasks/cluster-client-bootstrap TASK.md §5"]
fn cb17_info_reports_cluster_mode_honestly() {
    let dir = tempfile::tempdir().unwrap();
    let port = reserve_cluster_ports(1)[0];
    let _fleet = Fleet(vec![Some(spawn_node(dir.path(), port, &[]))]);
    let mut c = Conn::ready(port);
    assert_eq!(
        info_field(&mut c, &["INFO"], "redis_mode").as_deref(),
        Some("cluster"),
        "an SDK branches on redis_mode BEFORE it ever calls CLUSTER SHARDS — a \
         hardcoded `standalone` makes the cluster undiscoverable"
    );
    assert_eq!(
        info_field(&mut c, &["INFO"], "cluster_enabled").as_deref(),
        Some("1")
    );
}

#[test]
fn cb18_cluster_info_does_not_emit_cluster_enabled() {
    // Measured: redis-server reports cluster_enabled in INFO, never in
    // CLUSTER INFO. Moon has it in exactly the wrong one of the two.
    let dir = tempfile::tempdir().unwrap();
    let port = reserve_cluster_ports(1)[0];
    let _fleet = Fleet(vec![Some(spawn_node(dir.path(), port, &[]))]);
    let mut c = Conn::ready(port);
    let body = c.cmd(&["CLUSTER", "INFO"]).as_str().to_string();
    assert!(
        !body.contains("cluster_enabled"),
        "CLUSTER INFO must not carry cluster_enabled:\n{body}"
    );
}

// ── rejections ─────────────────────────────────────────────────────────────

#[test]
#[ignore = "red until batch 4/5 (argument rejections) lands; see .add/tasks/cluster-client-bootstrap TASK.md §5"]
fn cb19_argument_rejections_are_verbatim() {
    let dir = tempfile::tempdir().unwrap();
    let port = reserve_cluster_ports(1)[0];
    let _fleet = Fleet(vec![Some(spawn_node(dir.path(), port, &[]))]);
    let mut c = Conn::ready(port);

    match c.cmd(&["CLUSTER", "SHARDS", "extra"]) {
        Val::Err(e) => assert!(
            e.contains("Unknown subcommand or wrong number of arguments"),
            "got {e:?}"
        ),
        other => panic!("CLUSTER SHARDS takes no arguments, got {other:?}"),
    }
    for verb in ["READONLY", "READWRITE"] {
        match c.cmd(&[verb, "extra"]) {
            Val::Err(e) => assert_eq!(
                e,
                format!(
                    "ERR wrong number of arguments for '{}' command",
                    verb.to_lowercase()
                )
            ),
            other => panic!("{verb} takes no arguments, got {other:?}"),
        }
    }
}

#[test]
#[ignore = "red until batch 4/5 (standalone refusals) lands; see .add/tasks/cluster-client-bootstrap TASK.md §5"]
fn cb20_cluster_verbs_are_refused_on_a_standalone_instance() {
    let dir = tempfile::tempdir().unwrap();
    let (child, port) = common::spawn_listening(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--dir",
                dir.path().to_str().unwrap(),
                "--disk-free-min-pct",
                "0",
                "--appendonly",
                "no",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    });
    let _fleet = Fleet(vec![Some(child)]);
    let mut c = Conn::ready(port);

    for cmd in [
        vec!["CLUSTER", "SHARDS"],
        vec!["READONLY"],
        vec!["READWRITE"],
    ] {
        match c.cmd(&cmd) {
            Val::Err(e) => assert_eq!(
                e, "ERR This instance has cluster support disabled",
                "{cmd:?} on a standalone instance"
            ),
            other => panic!("{cmd:?} must be refused on a standalone node, got {other:?}"),
        }
    }
    // And the standalone identity is reported honestly too.
    assert_eq!(
        info_field(&mut c, &["INFO"], "redis_mode").as_deref(),
        Some("standalone")
    );
    assert_eq!(
        info_field(&mut c, &["INFO"], "cluster_enabled").as_deref(),
        Some("0")
    );
    let _ = HashMap::<u8, u8>::new();
}
