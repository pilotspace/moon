//! moon#1286 R2 (review N2): a replica applying its master's stream never
//! deletes a key because a deadline looks past on ITS clock — redis's
//! `checkAlreadyExpired` is false on a replica — and the master's own
//! immediate delete of a past deadline reaches the replica as `DEL`.
//!
//! Before: F4 (delete at once on an absolute deadline already past) also ran
//! on the replica, so a replica applying the stream late deleted keys its
//! master kept and published `del` for them (reviewer's `replag.sh`: the
//! replica SIGSTOPped 0.4 s over `PEXPIREAT k now+100` then `PERSIST k`
//! ended with DBSIZE 1 and `del:b`, `del:c`; redis 7.2.7's replica keeps all
//! three).
//!
//! The replica's own-clock view of an expired key on master-stream lookups
//! (the `PERSIST` that follows misses a key it sees as expired) is the older
//! divergence this does not close: the keys stay resident (DBSIZE 3), but
//! their past deadline hides them on the replica.
//!
//! Master-side PSYNC is monoio-only: run with a monoio `MOON_BIN`.
//! `MOON_BIN=<moon> cargo test --test replica_past_deadline_1286 -- --include-ignored`

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::{Conn, ServerGuard, find_moon_binary, server_stderr, spawn_listening_guarded};

fn start(dir: &Path) -> (ServerGuard, u16) {
    std::fs::create_dir_all(dir).unwrap();
    let dir_arg = dir.to_path_buf();
    spawn_listening_guarded(|port| {
        Command::new(find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--appendonly",
                "no",
                "--disk-free-min-pct",
                "0",
                "--dir",
            ])
            .arg(&dir_arg)
            .stdout(Stdio::null())
            .stderr(server_stderr(&dir_arg))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    let start = std::time::Instant::now();
    while !cond() {
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn int(reply: &str) -> i64 {
    reply
        .trim()
        .strip_prefix(':')
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("not an integer reply: {reply:?}"))
}

struct Pair {
    _master_guard: ServerGuard,
    replica_guard: ServerGuard,
    master: u16,
    replica: u16,
    dirs: [std::path::PathBuf; 2],
}

fn pair(tag: &str) -> Pair {
    let dm = common::unique_test_dir(&format!("moon-1286-n2-{tag}-m"));
    let ds = common::unique_test_dir(&format!("moon-1286-n2-{tag}-s"));
    let (mg, master) = start(&dm);
    let (rg, replica) = start(&ds);
    let reply = Conn::open(replica).send(&["REPLICAOF", "127.0.0.1", &master.to_string()]);
    assert!(reply.starts_with('+'), "REPLICAOF answered {reply:?}");
    wait_for("the replica link", || {
        Conn::open(replica)
            .send(&["INFO", "replication"])
            .contains("master_link_status:up")
    });
    Pair {
        _master_guard: mg,
        replica_guard: rg,
        master,
        replica,
        dirs: [dm, ds],
    }
}

impl Pair {
    /// Wait until the replica has applied everything the master sent.
    fn settle(&self) {
        let marker = format!("marker-{}", now_ms());
        assert_eq!(
            Conn::open(self.master).send(&["SET", "settle", &marker]),
            "+OK\r\n"
        );
        let want = format!("${}\r\n{marker}\r\n", marker.len());
        wait_for("the replica to apply the stream", || {
            Conn::open(self.replica).send(&["GET", "settle"]) == want
        });
    }

    fn signal(&self, sig: &str) {
        let status = Command::new("kill")
            .args([sig, &self.replica_guard.id().to_string()])
            .status()
            .unwrap();
        assert!(status.success(), "kill {sig}");
    }
}

impl Drop for Pair {
    fn drop(&mut self) {
        self.signal("-CONT");
        for d in &self.dirs {
            let _ = std::fs::remove_dir_all(d);
        }
    }
}

/// Every keyevent the replica publishes while `run` runs, as text.
fn replica_keyevents(port: u16, run: impl FnOnce()) -> String {
    let mut c = Conn::open(port);
    assert_eq!(
        c.send(&["CONFIG", "SET", "notify-keyspace-events", "KEA"]),
        "+OK\r\n"
    );
    let mut sub = Conn::open(port);
    sub.sock
        .write_all(&common::encode(&["PSUBSCRIBE", "__keyevent@0__:*"]))
        .unwrap();
    run();
    sub.sock
        .set_read_timeout(Some(Duration::from_millis(300)))
        .unwrap();
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    while let Ok(n) = sub.sock.read(&mut buf) {
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n]);
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The reviewer's `replag.sh`: the replica applies the stream 0.4 s late, so
/// deadlines that were 100 ms in the FUTURE on the master are past on the
/// replica when it applies them — then the master keeps every key.
#[test]
#[ignore = "real-server suite: MOON_BIN pinned (monoio: master PSYNC)"]
fn a_lagging_replica_does_not_delete_what_its_master_kept() {
    let p = pair("lag");
    let mut m = Conn::open(p.master);
    for k in ["a", "b", "c"] {
        assert_eq!(m.send(&["SET", k, "1"]), "+OK\r\n");
    }
    p.settle();
    let events = replica_keyevents(p.replica, || {
        p.signal("-STOP");
        let soon = (now_ms() + 100).to_string();
        assert_eq!(int(&m.send(&["PEXPIREAT", "a", &soon])), 1);
        assert_eq!(int(&m.send(&["PERSIST", "a"])), 1);
        assert_eq!(int(&m.send(&["PEXPIREAT", "b", &soon])), 1);
        assert_eq!(int(&m.send(&["PEXPIRE", "b", "100000"])), 1);
        assert_eq!(m.send(&["GETEX", "c", "PXAT", &soon]), "$1\r\n1\r\n");
        assert_eq!(int(&m.send(&["PERSIST", "c"])), 1);
        // Well past the 100 ms deadlines before the replica reads a byte.
        std::thread::sleep(Duration::from_millis(1000));
        p.signal("-CONT");
        p.settle();
    });
    assert_eq!(int(&m.send(&["DBSIZE"])), 4, "master: a b c settle");
    let dbsize = int(&Conn::open(p.replica).send(&["DBSIZE"]));
    assert!(
        !events.contains(":del\r\n"),
        "the replica published `del` for keys its master kept: {events:?}"
    );
    assert_eq!(
        dbsize, 4,
        "the replica deleted keys its master kept (redis's replica keeps them)"
    );
}

/// The other half: a deadline already past ON THE MASTER deletes there at
/// once, and the replica — which no longer decides that by itself — must be
/// told. Without the master's `DEL` it would keep an invisible key forever
/// (a replica runs no expiry of its own).
#[test]
#[ignore = "real-server suite: MOON_BIN pinned (monoio: master PSYNC)"]
fn a_past_deadline_on_the_master_deletes_on_the_replica_too() {
    let p = pair("past");
    let mut m = Conn::open(p.master);
    for k in ["d", "e", "keep"] {
        assert_eq!(m.send(&["SET", k, "1"]), "+OK\r\n");
    }
    p.settle();
    let past = (now_ms() - 60_000).to_string();
    let past_s = (now_ms() / 1000 - 60).to_string();
    assert_eq!(int(&m.send(&["PEXPIREAT", "d", &past])), 1);
    assert_eq!(m.send(&["GETEX", "e", "EXAT", &past_s]), "$1\r\n1\r\n");
    p.settle();
    let mut s = Conn::open(p.replica);
    assert_eq!(int(&m.send(&["DBSIZE"])), 2, "master: keep + settle");
    assert_eq!(
        int(&s.send(&["DBSIZE"])),
        2,
        "the replica kept a key its master deleted for a past deadline"
    );
    assert_eq!(int(&s.send(&["EXISTS", "d", "e"])), 0);
}
