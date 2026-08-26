//! A flush issued by a ROUTED script must reach every shard (moon#705).
//!
//! moon#685 gave a script-issued `FLUSHDB`/`FLUSHALL` both dimensions the Lua
//! bridge cannot reach on its own — every database, and every shard. The
//! second half landed at eight of the twelve script entry points. The four in
//! `shard/spsc_handler.rs` — the arms `route_script_elsewhere` sends a script
//! to when it declares a key another shard owns — dropped the broadcast on the
//! floor, so the flush cleared only the OWNER shard while the script still
//! answered `+OK`.
//!
//! Measured on the pre-fix binary at `--shards 4`, 12 keys re-seeded before
//! each row, the only variable being the script's DECLARED key:
//!
//! ```text
//!   route-a  reply=OK  dbsize 12 -> 9
//!   route-d  reply=OK  dbsize 12 -> 0     (that key happened to be local)
//!   route-e  reply=OK  dbsize 12 -> 10
//! ```
//!
//! Which answer a caller gets depends on where a key the script never touches
//! happens to hash — an implementation detail no client can see. So this test
//! does not sample: every declared key runs through the same assertion, and a
//! build that fixes only the local path fails on the first key that routes.
//!
//! The declared key is deliberately a key the script never touches. That is
//! the whole point: it decides only WHERE the script runs, and where a script
//! runs must not decide how much of the keyspace its flush reaches.

mod common;

use std::process::{Child, Command};

const SHARDS: usize = 4;
const SEEDED: usize = 12;

/// Declared keys, spread so that at least one lands on each shard.
///
/// Correctness does not depend on the spread — every key is asserted
/// identically, and one that routes locally is a legitimate case that passed
/// before this fix too. The spread only decides how fast a regression shows.
const KEYS: &[&str] = &[
    "route-a", "route-b", "route-c", "route-d", "route-e", "route-f", "route-g", "route-h",
];

fn spawn_on(port: u16, dir: &std::path::Path) -> Child {
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            &SHARDS.to_string(),
            "--appendonly",
            "no",
            "--disk-free-min-pct",
            "0",
            "--dir",
        ])
        .arg(dir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn moon")
}

/// One shared connection for the whole run.
///
/// Deliberately NOT a fresh connection per probe: the connection's own shard
/// is what decides whether a given declared key routes, so reconnecting
/// between rows would re-roll the very variable under test.
struct Cli {
    conn: redis::Connection,
}

impl Cli {
    /// `spawn_listening_guarded` already proved the server ACCEPTS; this
    /// retries only the RESP handshake, which can still land a moment early.
    fn open(port: u16) -> Self {
        let client =
            redis::Client::open(format!("redis://127.0.0.1:{port}/")).expect("redis client");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if let Ok(mut conn) = client.get_connection()
                && redis::cmd("PING").query::<String>(&mut conn).is_ok()
            {
                return Self { conn };
            }
            assert!(
                std::time::Instant::now() < deadline,
                "server on port {port} never answered PING"
            );
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    fn cmd(&mut self, args: &[&str]) -> redis::Value {
        let mut c = redis::cmd(args[0]);
        for a in &args[1..] {
            c.arg(*a);
        }
        c.query(&mut self.conn).expect("command")
    }

    fn dbsize(&mut self) -> i64 {
        match self.cmd(&["DBSIZE"]) {
            redis::Value::Int(n) => n,
            other => panic!("DBSIZE answered {other:?}"),
        }
    }

    /// Wipe every database, then re-seed `SEEDED` keys into db0.
    ///
    /// The wipe goes through a TYPED `FLUSHALL`, which has broadcast since
    /// D-2 — using the script path to set up a test of the script path would
    /// make a failure unreadable.
    fn reseed(&mut self) {
        self.cmd(&["FLUSHALL"]);
        for i in 0..SEEDED {
            self.cmd(&["SET", &format!("seed:{i}"), "v"]);
        }
        assert_eq!(
            self.dbsize(),
            SEEDED as i64,
            "the fixture must start from a full keyspace, or an unflushed shard \
             cannot be told from an empty one"
        );
    }
}

/// Run `body` as a script that declares `key`, and assert the keyspace is empty
/// afterwards.
fn assert_flush_reaches_every_shard(c: &mut Cli, what: &str, key: &str, invoke: &[&str]) {
    c.reseed();
    let reply = c.cmd(invoke);
    let after = c.dbsize();
    assert_eq!(
        after, 0,
        "{what} declaring {key:?}: the script answered {reply:?} but {after} of {SEEDED} keys \
         survived. A flush that reports success must have reached every shard — where the \
         script happened to run is not something the client can see or control."
    );
}

#[test]
fn moon705_a_routed_script_flush_reaches_every_shard() {
    let dir = tempfile::Builder::new()
        .prefix("moon-705-")
        .tempdir()
        .expect("tempdir");
    let (mut guard, port) = common::spawn_listening_guarded(|p| spawn_on(p, dir.path()));
    let mut c = Cli::open(port);

    for key in KEYS {
        // EVAL — the arm the issue measured.
        assert_flush_reaches_every_shard(
            &mut c,
            "EVAL FLUSHALL",
            key,
            &["EVAL", "return redis.call('FLUSHALL')", "1", key],
        );
        assert_flush_reaches_every_shard(
            &mut c,
            "EVAL FLUSHDB",
            key,
            &["EVAL", "return redis.call('FLUSHDB')", "1", key],
        );
    }

    // EVALSHA reaches the SAME arm as EVAL, by a different door: it carries a
    // sha where EVAL carries a body, and a fix applied to only one of the two
    // would pass every row above.
    let sha = match c.cmd(&["SCRIPT", "LOAD", "return redis.call('FLUSHALL')"]) {
        redis::Value::BulkString(b) => String::from_utf8(b).expect("sha is ascii"),
        other => panic!("SCRIPT LOAD answered {other:?}"),
    };
    for key in KEYS {
        assert_flush_reaches_every_shard(
            &mut c,
            "EVALSHA FLUSHALL",
            key,
            &["EVALSHA", &sha, "1", key],
        );
    }

    // FCALL is the second of the two routed sites, and it is a separate
    // `with_shard` block — fixing the EVAL one does not fix this.
    c.cmd(&[
        "FUNCTION",
        "LOAD",
        "#!lua name=flush705\nredis.register_function('f705', function(keys, args) \
         return redis.call('FLUSHALL') end)",
    ]);
    for key in KEYS {
        assert_flush_reaches_every_shard(
            &mut c,
            "FCALL FLUSHALL",
            key,
            &["FCALL", "f705", "1", key],
        );
    }

    guard.kill_now();
}

/// The same defect, reached through MULTI instead of a script.
///
/// `broadcast_txn_flushes` had no sender parameter at all: it passed the shard
/// that RAN the body as `coordinate_flush_broadcast`'s `my_shard`, which is
/// both the skip AND the SPSC source. For a LOCAL transaction the two coincide
/// and it was correct; for a ROUTED one it sent from a shard the caller is not
/// running on and skipped the wrong leg. Found while fixing moon#705 — the two
/// share the one helper — so it is asserted here rather than left to be
/// rediscovered.
#[test]
fn moon705_a_routed_transaction_flush_reaches_every_shard() {
    let dir = tempfile::Builder::new()
        .prefix("moon-705-txn-")
        .tempdir()
        .expect("tempdir");
    let (mut guard, port) = common::spawn_listening_guarded(|p| spawn_on(p, dir.path()));
    let mut c = Cli::open(port);

    for key in KEYS {
        c.reseed();
        // A transaction whose only key lives on ONE shard is what gets routed
        // there wholesale; the FLUSHALL beside it is keyless and rides along.
        c.cmd(&["MULTI"]);
        c.cmd(&["SET", key, "1"]);
        c.cmd(&["FLUSHALL"]);
        let reply = c.cmd(&["EXEC"]);
        let after = c.dbsize();
        assert_eq!(
            after, 0,
            "MULTI declaring {key:?}: EXEC answered {reply:?} but {after} keys survived. \
             A flush inside a transaction that reports success must have reached every \
             shard, exactly as the same flush typed on the connection does."
        );
    }

    guard.kill_now();
}
