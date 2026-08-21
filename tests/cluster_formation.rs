//! v0.9 W0/C-1 (#405): the cluster control plane must run on the DEFAULT
//! runtime. A 3-node cluster started with `--cluster-enabled` has to form via
//! CLUSTER MEET + gossip regardless of which runtime the binary was built
//! with — before C-1, the monoio startup path never spawned the cluster bus
//! or the gossip ticker, so MEET wrote local state that no peer ever learned
//! about.
//!
//! The load-bearing assertion is on nodes 2 and 3: node 1 knows all three
//! from its own MEET commands, but nodes 2/3 only reach
//! `cluster_known_nodes:3` when the bus carries node 1's gossip pings to
//! them (node 2 learns of node 3 — and vice versa — exclusively through
//! gossip payloads).

mod common;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

/// Kills the whole fleet on drop so a failed assertion never leaks servers
/// (leaked-moon gotcha: orphans spin CPU and poison later benches).
struct Fleet(Vec<Child>);

impl Drop for Fleet {
    fn drop(&mut self) {
        for child in &mut self.0 {
            common::sigkill(child);
        }
    }
}

fn spawn_cluster_node(dir: &std::path::Path, port: u16, extra: &[&str]) -> Child {
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
            "--cluster-enabled",
        ])
        .args(extra)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn moon")
}

fn connect_retry(port: u16) -> TcpStream {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(s) => return s,
            Err(e) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "connect to 127.0.0.1:{port} kept failing: {e}"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

/// Read one full RESP reply (simple line or bulk string).
fn command_reply(stream: &mut TcpStream, cmd: &str) -> String {
    stream.write_all(cmd.as_bytes()).expect("write cmd");
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("set timeout");
    let mut buf = Vec::new();
    let mut chunk = [0u8; 65536];
    loop {
        let n = stream.read(&mut chunk).expect("read reply");
        assert!(n > 0, "connection closed mid-reply");
        buf.extend_from_slice(&chunk[..n]);
        if buf.starts_with(b"$") {
            if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                let len: usize = std::str::from_utf8(&buf[1..pos - 1])
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                if buf.len() >= pos + 1 + len + 2 {
                    break;
                }
            }
        } else if buf.ends_with(b"\r\n") {
            break;
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

fn known_nodes(stream: &mut TcpStream) -> usize {
    let info = command_reply(stream, "CLUSTER INFO\r\n");
    info.lines()
        .find_map(|l| l.strip_prefix("cluster_known_nodes:"))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("no cluster_known_nodes in reply: {info}"))
}

fn myid(stream: &mut TcpStream) -> String {
    let reply = command_reply(stream, "CLUSTER MYID\r\n");
    let id = reply
        .lines()
        .nth(1)
        .expect("CLUSTER MYID bulk payload")
        .trim()
        .to_string();
    assert_eq!(id.len(), 40, "unexpected MYID reply: {reply}");
    id
}

/// Spawn a 3-node fleet, MEET nodes 2/3 from node 1, and wait for the mesh
/// to complete: every node's CLUSTER NODES must list all three REAL node ids.
///
/// Identity convergence (not just `cluster_known_nodes:3`) is the criterion:
/// a rumor can adopt a peer under a not-yet-resolved placeholder id at the
/// right address, which satisfies the count while the real id is still only
/// resolvable by direct handshake.
fn form_three_node_cluster(
    dirs: &[tempfile::TempDir],
    extra: &[&str],
) -> (Fleet, Vec<TcpStream>, Vec<u16>, Vec<String>) {
    let (children, ports): (Vec<Child>, Vec<u16>) = dirs
        .iter()
        .map(|d| common::spawn_listening_cluster(|p| spawn_cluster_node(d.path(), p, extra)))
        .unzip();
    let fleet = Fleet(children);

    let mut conns: Vec<TcpStream> = ports.iter().map(|&p| connect_retry(p)).collect();
    for c in &mut conns {
        let pong = command_reply(c, "PING\r\n");
        assert_eq!(pong, "+PONG\r\n");
    }

    // Meet nodes 2 and 3 into the cluster from node 1 only. Node 2 and node 3
    // never hear about each other directly — only gossip can complete the mesh.
    for &p in &ports[1..] {
        let r = command_reply(&mut conns[0], &format!("CLUSTER MEET 127.0.0.1 {p}\r\n"));
        assert!(r.starts_with("+OK"), "CLUSTER MEET failed: {r}");
    }

    let ids: Vec<String> = conns.iter_mut().map(myid).collect();

    // Gossip ticks every 100ms; give a loaded runner plenty of slack.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let resolved: Vec<usize> = conns
            .iter_mut()
            .map(|c| {
                let nodes = command_reply(c, "CLUSTER NODES\r\n");
                ids.iter()
                    .filter(|id| nodes.lines().any(|l| l.starts_with(id.as_str())))
                    .count()
            })
            .collect();
        if resolved.iter().all(|&n| n == 3) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "cluster never formed: real ids resolved per node = {resolved:?} \
             (expected [3, 3, 3]; nodes 2/3 stuck below 3 means the cluster \
             bus / gossip ticker is not running on this runtime)"
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    // Identity convergence implies exact membership: any placeholder at one
    // of the three addresses is retired by the direct handshake that
    // resolved the real id there.
    let counts: Vec<usize> = conns.iter_mut().map(known_nodes).collect();
    assert_eq!(counts, vec![3, 3, 3], "phantom entries survived formation");

    (fleet, conns, ports, ids)
}

#[test]
fn three_node_cluster_forms_via_meet_and_gossip() {
    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let (fleet, mut conns, ports, _ids) = form_three_node_cluster(&dirs, &[]);

    // Bus traffic must actually have flowed on the seed node.
    let info = command_reply(&mut conns[0], "CLUSTER INFO\r\n");
    let sent: u64 = info
        .lines()
        .find_map(|l| l.strip_prefix("cluster_stats_messages_sent:"))
        .and_then(|v| v.trim().parse().ok())
        .expect("cluster_stats_messages_sent present");
    assert!(sent > 0, "no cluster bus messages were sent: {info}");

    // MEET is idempotent by address: repeating one must not stack a fresh
    // placeholder (the placeholder id is random per call).
    let r = command_reply(
        &mut conns[0],
        &format!("CLUSTER MEET 127.0.0.1 {}\r\n", ports[1]),
    );
    assert!(r.starts_with("+OK"), "repeat MEET failed: {r}");
    assert_eq!(
        known_nodes(&mut conns[0]),
        3,
        "repeat MEET stacked a placeholder"
    );

    // MEET-ing our own advertised address is refused.
    let r = command_reply(
        &mut conns[0],
        &format!("CLUSTER MEET 127.0.0.1 {}\r\n", ports[0]),
    );
    assert!(r.starts_with("-ERR"), "self-MEET must error: {r}");

    drop(fleet);
}

/// A cluster node whose bus port (port + 10000) is already taken must abort
/// loudly at startup — not serve clients while invisible to every peer.
#[test]
fn occupied_bus_port_aborts_startup() {
    let port = common::reserve_cluster_port();
    let _bus_blocker = TcpListener::bind(("127.0.0.1", port + 10000)).expect("occupy bus port");
    let dir = tempfile::tempdir().expect("tempdir");
    let mut child = spawn_cluster_node(dir.path(), port, &[]);

    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        match child.try_wait().expect("try_wait") {
            Some(status) => {
                assert!(
                    !status.success(),
                    "startup must abort with a nonzero status, got {status}"
                );
                break;
            }
            None => {
                if std::time::Instant::now() >= deadline {
                    common::sigkill(&mut child);
                    panic!("node kept running with an occupied cluster bus port");
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

/// Failure detection through the control plane: killing one node of a formed
/// 3-node cluster must get it flagged (`fail?`/`fail`) by both survivors within the
/// node timeout. Hard FAIL needs quorum ≥ 2 EXTERNAL reporters, which two
/// survivors of three masters cannot reach — full FAIL/election e2e lands
/// with C-3's replica legs.
#[test]
fn killed_node_is_flagged_by_survivors() {
    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    let (mut fleet, mut conns, _ports, ids) =
        form_three_node_cluster(&dirs, &["--cluster-node-timeout", "3000"]);

    // Node 3's id — formation guarantees both survivors already resolved it.
    let victim_id = ids[2].clone();

    common::sigkill(&mut fleet.0[2]);

    // node_timeout is 3s and gossip ticks every 100ms; poll each survivor's
    // CLUSTER NODES for the victim's flags token to become `fail?` (PFAIL) or
    // `fail` (FAIL, once quorum confirms). Those two spellings are Redis's own,
    // measured against redis-server 3-node cluster + SHUTDOWN NOSAVE 2026-08:
    // the victim's flags column went `master` -> `master,fail?` -> `master,fail`.
    // Moon previously rendered PFAIL as `pfail`, which no real client parses.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    'outer: loop {
        let mut flagged = 0;
        for c in conns.iter_mut().take(2) {
            let nodes = command_reply(c, "CLUSTER NODES\r\n");
            let flags = nodes
                .lines()
                .find(|l| l.starts_with(&victim_id))
                .and_then(|l| l.split_whitespace().nth(2))
                .unwrap_or("");
            if flags.split(',').any(|f| f == "fail?" || f == "fail") {
                flagged += 1;
            }
        }
        if flagged == 2 {
            break 'outer;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "survivors never flagged the killed node ({flagged}/2 saw fail?/fail)"
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    drop(fleet);
}
