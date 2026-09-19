//! moon#1015 — a multi-shard node must REFUSE to become a replica, not ack it.
//!
//! Streaming replication applies into a single shard only; multi-shard
//! replicas are moon#406. Before this fix `REPLICAOF host port` (and
//! `CLUSTER REPLICATE`) on a `--shards > 1` node replied `+OK`, flipped the
//! node to a read-only replica, killed any running replica task, and then the
//! replica task refused to start — logging the reason where no client could
//! see it. The node was left refusing every write while holding no data.
//!
//! The contract pinned here, per command:
//!   * the reply is an error naming `--shards 1` and moon#406;
//!   * `INFO replication` still reports `role:master`;
//!   * the node still accepts writes.
//!
//! Plus two controls, so the gate cannot pass by refusing everything:
//! `REPLICAOF NO ONE` stays allowed on a multi-shard node, and a
//! single-shard node still accepts `REPLICAOF host port`.
//!
//! Pin the binary under test: `MOON_BIN=/path/to/moon cargo test --test
//! replicaof_multishard_refusal_1015`.

mod common;

use std::process::{Child, Command};
use std::time::{Duration, Instant};

use common::Conn;

fn spawn_moon(dir: &std::path::Path, port: u16, shards: usize, extra: &[&str]) -> Child {
    std::fs::create_dir_all(dir).expect("create --dir");
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            &shards.to_string(),
            "--dir",
            dir.to_str().expect("utf-8 dir"),
            "--appendonly",
            "no",
            "--disk-free-min-pct",
            "0",
        ])
        .args(extra)
        .stdout(std::process::Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon (set MOON_BIN to a built binary)")
}

fn start(prefix: &str, shards: usize) -> (common::ServerGuard, u16) {
    let dir = common::unique_test_dir(prefix);
    common::spawn_listening_guarded(|p| spawn_moon(&dir, p, shards, &[]))
}

fn start_cluster(prefix: &str, shards: usize) -> (common::ServerGuard, u16) {
    let dir = common::unique_test_dir(prefix);
    let (child, port) = common::spawn_listening_cluster(|p| {
        spawn_moon(
            &dir,
            p,
            shards,
            &["--cluster-enabled", "--cluster-node-timeout", "1000"],
        )
    });
    (common::ServerGuard::new(child), port)
}

/// The role line of `INFO replication`, e.g. `role:master`.
fn role(port: u16) -> String {
    let info = Conn::open(port).send(&["INFO", "replication"]);
    info.lines()
        .find(|l| l.starts_with("role:"))
        .unwrap_or("<no role line>")
        .to_string()
}

/// Assert the moon#1015 refusal shape for a replica-start reply.
fn assert_refused(what: &str, reply: &str) {
    assert!(
        reply.starts_with("-ERR"),
        "{what} on a multi-shard node must be REFUSED, not acknowledged; got {reply:?}"
    );
    assert!(
        reply.contains("--shards 1") && reply.contains("406"),
        "{what} refusal must name `--shards 1` and moon#406; got {reply:?}"
    );
}

/// Assert the node is still a writable master after a refused replica start.
fn assert_still_master_and_writable(what: &str, port: u16) {
    assert_eq!(
        role(port),
        "role:master",
        "{what} was refused, so the node must still be a master"
    );
    let mut c = Conn::open(port);
    assert_eq!(
        c.send(&["SET", "after-refusal", "v"]),
        "+OK\r\n",
        "{what} was refused, so the node must still accept writes"
    );
    assert_eq!(c.send(&["GET", "after-refusal"]), "$1\r\nv\r\n");
}

#[test]
fn replicaof_on_multishard_node_is_refused_and_node_stays_master() {
    // A live master, so "never syncs" cannot be blamed on a dead target.
    let (_master, master_port) = start("moon-1015-master", 1);
    Conn::open(master_port).send(&["SET", "k", "fromMaster"]);

    let (_node, port) = start("moon-1015-node", 2);
    let mp = master_port.to_string();

    for verb in ["REPLICAOF", "SLAVEOF"] {
        let reply = Conn::open(port).send(&[verb, "127.0.0.1", &mp]);
        assert_refused(verb, &reply);
        assert_still_master_and_writable(verb, port);
    }
}

#[test]
fn replicaof_no_one_stays_allowed_on_multishard_node() {
    let (_node, port) = start("moon-1015-noone", 2);
    assert_eq!(
        Conn::open(port).send(&["REPLICAOF", "NO", "ONE"]),
        "+OK\r\n",
        "REPLICAOF NO ONE must not be caught by the multi-shard gate"
    );
    assert_still_master_and_writable("REPLICAOF NO ONE", port);
}

#[test]
fn replicaof_argument_errors_win_over_the_multishard_gate() {
    // Arity and port validation still answer first — the gate only replaces
    // what would have been an acknowledged replica start.
    let (_node, port) = start("moon-1015-args", 2);
    let mut c = Conn::open(port);
    let arity = c.send(&["REPLICAOF", "127.0.0.1"]);
    assert!(
        arity.starts_with("-ERR wrong number of arguments"),
        "got {arity:?}"
    );
    let bad_port = c.send(&["REPLICAOF", "127.0.0.1", "notaport"]);
    assert!(
        bad_port.starts_with("-ERR value is not an integer"),
        "got {bad_port:?}"
    );
    assert_eq!(role(port), "role:master");
}

#[test]
fn replicaof_on_single_shard_node_is_still_accepted() {
    // Control: the gate must not over-refuse the supported configuration.
    let (_master, master_port) = start("moon-1015-ctl-master", 1);
    let (_node, port) = start("moon-1015-ctl-node", 1);
    let reply = Conn::open(port).send(&["REPLICAOF", "127.0.0.1", &master_port.to_string()]);
    assert_eq!(
        reply, "+OK\r\n",
        "single-shard REPLICAOF must still be accepted"
    );
    assert_eq!(role(port), "role:slave");
    // Detach so the test does not leave a streaming link behind.
    assert_eq!(
        Conn::open(port).send(&["REPLICAOF", "NO", "ONE"]),
        "+OK\r\n"
    );
}

#[test]
fn cluster_replicate_on_multishard_node_is_refused_and_node_stays_master() {
    let (_master, master_port) = start_cluster("moon-1015-cl-master", 1);
    let (_node, port) = start_cluster("moon-1015-cl-node", 2);

    let master_id = Conn::open(master_port).send(&["CLUSTER", "MYID"]);
    // `$40\r\n<id>\r\n`
    let master_id = master_id
        .lines()
        .nth(1)
        .expect("CLUSTER MYID bulk body")
        .to_string();
    assert_eq!(master_id.len(), 40, "CLUSTER MYID: {master_id:?}");

    assert_eq!(
        Conn::open(port).send(&["CLUSTER", "MEET", "127.0.0.1", &master_port.to_string()]),
        "+OK\r\n"
    );
    // Wait until the would-be replica KNOWS the master, so the pre-fix build
    // takes the success path (`+OK` + relabel) rather than `Unknown node`.
    let deadline = Instant::now() + Duration::from_secs(20);
    while !Conn::open(port)
        .send(&["CLUSTER", "NODES"])
        .contains(&master_id)
    {
        assert!(
            Instant::now() < deadline,
            "the node never learned the master's id via gossip"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    let reply = Conn::open(port).send(&["CLUSTER", "REPLICATE", &master_id]);
    assert_refused("CLUSTER REPLICATE", &reply);
    assert_eq!(role(port), "role:master");

    // The cluster view must not have been relabelled either.
    let nodes = Conn::open(port).send(&["CLUSTER", "NODES"]);
    let me = nodes
        .lines()
        .find(|l| l.contains("myself"))
        .unwrap_or_else(|| panic!("no myself line in CLUSTER NODES: {nodes:?}"));
    assert!(
        me.contains("master") && !me.contains("slave"),
        "a refused CLUSTER REPLICATE must leave this node a master: {me:?}"
    );
}
