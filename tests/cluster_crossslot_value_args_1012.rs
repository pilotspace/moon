//! moon#1012: in cluster mode the `CROSSSLOT` pre-check slot-hashed every
//! argument of a multi-key command as if it were a key, so `MSET`/`MSETNX`
//! were refused whenever a VALUE hashed to another slot.
//!
//! Every expected reply below is the raw wire reply of redis-server 8.6.1 in
//! cluster mode, one node holding all 16384 slots, measured over a raw socket:
//!
//! ```text
//! MSET {t}a x {t}b y              redis +OK        moon (pre-fix) -CROSSSLOT
//! MSETNX {u}a x {u}b y            redis :1         moon (pre-fix) -CROSSSLOT
//! MSET {t}a {other}               redis +OK        moon (pre-fix) -CROSSSLOT
//! MSET {t}a x {other}b y          redis -CROSSSLOT moon           -CROSSSLOT
//! ```
//!
//! The suite runs a real server binary — the cluster pre-check lives in the
//! connection handlers, which only a live connection reaches — at `--shards 1`
//! and `--shards 4`, on whichever runtime the binary was built with.

mod common;

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use common::Conn;

const CROSSSLOT: &str = "-CROSSSLOT Keys in request don't hash to the same slot\r\n";

/// Kills the node and removes its directory on drop, so a failed assertion
/// never leaks a server.
struct Node {
    child: Child,
    dir: PathBuf,
}

impl Drop for Node {
    fn drop(&mut self) {
        common::sigkill(&mut self.child);
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn spawn_node(shards: &str) -> (Node, u16) {
    let bin = common::find_moon_binary();
    let dir = common::unique_test_dir("crossslot-1012");
    std::fs::create_dir_all(&dir).expect("create --dir");
    let dir_for_spawn = dir.clone();
    // A cluster node also binds port+10000 for its bus, so the port must come
    // from the reserved cluster range.
    let (child, port) = common::spawn_listening_cluster(move |port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                shards,
                "--dir",
                dir_for_spawn.to_str().expect("utf-8 dir"),
                "--disk-free-min-pct",
                "0",
                "--appendonly",
                "no",
                "--admin-port",
                "0",
                "--cluster-enabled",
            ])
            .stdout(Stdio::null())
            .stderr(common::server_stderr(&dir_for_spawn))
            .spawn()
            .expect("spawn moon")
    });
    (Node { child, dir }, port)
}

/// Give this node every slot, then wait until it reports `cluster_state:ok`,
/// so a refusal below can only be the pre-check under test and never a
/// `CLUSTERDOWN` for an unserved slot.
fn own_every_slot(conn: &mut Conn) {
    for start in (0u16..16384).step_by(1024) {
        let slots: Vec<String> = (start..(start + 1024).min(16384))
            .map(|s| s.to_string())
            .collect();
        let mut argv: Vec<&str> = vec!["CLUSTER", "ADDSLOTS"];
        argv.extend(slots.iter().map(String::as_str));
        let r = conn.send(&argv);
        assert_eq!(r, "+OK\r\n", "ADDSLOTS {start}..: {r:?}");
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if conn.send(&["CLUSTER", "INFO"]).contains("cluster_state:ok") {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "cluster_state never became ok after claiming all 16384 slots"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn keyslot(conn: &mut Conn, key: &str) -> String {
    conn.send(&["CLUSTER", "KEYSLOT", key])
}

fn run(shards: &str) {
    let (_node, port) = spawn_node(shards);
    let mut c = Conn::open(port);
    own_every_slot(&mut c);

    // The precondition every row rests on: `{t}` and `{other}` really are in
    // different slots, and so is the plain value `x`. Without this a green
    // run could be a hash coincidence.
    let t = keyslot(&mut c, "{t}");
    assert_ne!(
        t,
        keyslot(&mut c, "{other}"),
        "{{t}} and {{other}} share a slot"
    );
    assert_ne!(t, keyslot(&mut c, "x"), "{{t}} and x share a slot");
    assert_eq!(t, keyslot(&mut c, "{t}a"), "a hash tag must pin the slot");

    // (argv, redis-server 8.6.1's raw reply)
    let rows: &[(&[&str], &str)] = &[
        // The reported bug: keys in one slot, values anywhere.
        (&["MSET", "{t}a", "x", "{t}b", "y"], "+OK\r\n"),
        (&["MSETNX", "{u}a", "x", "{u}b", "y"], ":1\r\n"),
        (&["MSET", "{t}c", "{other}v"], "+OK\r\n"),
        (
            &["MSETNX", "{t}d", "{other}v", "{t}e", "{other}w"],
            ":1\r\n",
        ),
        // Values that happen to share the tag: always passed, must still.
        (&["MSET", "{t}f", "{t}1", "{t}g", "{t}2"], "+OK\r\n"),
        // Keys genuinely in two slots: still refused, whatever the values.
        (&["MSET", "{t}a", "{t}v", "{other}b", "{t}w"], CROSSSLOT),
        (&["MSETNX", "{t}h", "x", "{other}i", "y"], CROSSSLOT),
        // The rest of the pre-checked family keeps its verdicts.
        (&["MGET", "{t}a", "{other}b"], CROSSSLOT),
        (&["DEL", "{t}z1", "{other}z2"], CROSSSLOT),
        (&["BITOP", "AND", "{t}dst", "{t}s1", "{t}s2"], ":0\r\n"),
        (&["BITOP", "AND", "{t}dst", "{other}s1"], CROSSSLOT),
        (&["COPY", "{t}nosrc", "{t}dst2", "REPLACE"], ":0\r\n"),
        (&["COPY", "{t}a", "{other}dst"], CROSSSLOT),
    ];
    for (argv, want) in rows {
        let got = c.send(argv);
        assert_eq!(
            &got,
            want,
            "shards={shards}: {} — redis-server 8.6.1 answers {want:?}",
            argv.join(" ")
        );
    }

    // The accepted writes really landed, with the values as sent.
    assert_eq!(
        c.send(&["MGET", "{t}a", "{t}b", "{t}c"]),
        "*3\r\n$1\r\nx\r\n$1\r\ny\r\n$8\r\n{other}v\r\n",
        "shards={shards}: MSET must store the values it was given"
    );
    assert_eq!(
        c.send(&["MGET", "{u}a", "{u}b"]),
        "*2\r\n$1\r\nx\r\n$1\r\ny\r\n",
        "shards={shards}: MSETNX must store the values it was given"
    );
}

#[test]
fn mset_values_are_not_slot_checked_1_shard() {
    run("1");
}

#[test]
fn mset_values_are_not_slot_checked_4_shards() {
    run("4");
}
