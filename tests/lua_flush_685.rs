//! A flush issued from Lua must reach as far as the same flush typed on the
//! connection — moon#685.
//!
//! moon#677 fixed `FLUSHALL` on the connection, `MULTI`/`EXEC`, cross-shard,
//! AOF-replay and replication paths and deliberately left the Lua path out:
//! the fix there is structural, not a missing call. So `FLUSHALL` meant two
//! different things depending on which door it came through.
//!
//! Measured against the pre-fix binary:
//!
//! ```text
//! --shards 1, five databases seeded with one key each, from db0
//!   plain FLUSHALL:                             db0=0 db1=0 db3=0 db7=0 db15=0
//!   EVAL "return redis.call('FLUSHALL')" 0:     db0=0 db1=1 db3=1 db7=1 db15=1
//!
//! --shards 4, db3 seeded with 40 keys
//!   EVAL "return redis.call('FLUSHDB')"  0:     40 -> 29   (one shard of four)
//!   EVAL "return redis.call('FLUSHALL')" 0:     29 -> 29   (nothing more)
//! ```
//!
//! Two independent dimensions were missing, and only the first is what the
//! issue reported:
//!
//!   * **databases.** The bridge holds one `&mut Database`, so `FLUSHALL`
//!     could not mean more than `FLUSHDB`.
//!   * **shards.** `FLUSHDB`/`FLUSHALL` are keyless, so a script clears only
//!     the shard it runs on. The typed command has broadcast since D-2. This
//!     one also affected `FLUSHDB`, which the issue calls already correct —
//!     it was, in the single-shard sense only.
//!
//! Redis clears everything either way, so both are parity gaps as well as
//! internal inconsistencies.
//!
//! # Why the counter-test matters as much as the fix
//!
//! `FLUSHDB` must keep its single-DATABASE scope while gaining whole-server
//! shard reach. The hazard in "make FLUSHALL see every database" is doing it
//! with a caller-passed flag that one inverted call turns `FLUSHDB` into a
//! server wipe — which is why the completion keys on the COMMAND NAME
//! (moon#677 made the same choice). `lfa4` is the test that catches an
//! inversion, and it is the reason this file is not just `lfa1`.

mod common;

use common::Conn;
use moon::shard::dispatch::key_to_shard;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Databases sampled. Not all sixteen: enough to catch "only the selected one"
/// (the bug) and "only db0" (the obvious wrong fix), including the last index
/// so an off-by-one in the loop bound cannot hide.
const SAMPLED: [u32; 5] = [0, 1, 3, 7, 15];

struct Moon {
    child: Child,
    port: u16,
    tmp_dir: std::path::PathBuf,
}

impl Drop for Moon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.tmp_dir);
    }
}

fn spawn_moon(shards: usize) -> Moon {
    spawn_moon_with_aof(shards, "no")
}

fn spawn_moon_with_aof(shards: usize, appendonly: &str) -> Moon {
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-lfa685-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &shards.to_string(),
                "--admin-port",
                "0",
                "--appendonly",
                appendonly,
                "--disk-free-min-pct",
                "0",
                "--dir",
                tmp_dir.to_str().unwrap_or("/tmp"),
            ])
            .stdout(Stdio::null())
            .stderr(
                std::fs::File::create(tmp_dir.join("moon.stderr")).expect("create moon stderr log"),
            )
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("moon-lfa685-{port}"));
    let moon = Moon {
        child,
        port,
        tmp_dir,
    };
    await_ready(moon.port, Duration::from_secs(30));
    moon
}

fn await_ready(port: u16, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        if let Ok(mut c) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = c.set_read_timeout(Some(Duration::from_millis(500)));
            if c.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut buf = [0u8; 64];
                if let Ok(n) = c.read(&mut buf)
                    && n > 0
                    && buf.starts_with(b"+PONG")
                {
                    return true;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

/// One key per (database, shard) pair, named so that moon's OWN `key_to_shard`
/// puts it where this function claims.
///
/// Not "seed N keys and hope they spread": at `--shards 4` a random handful can
/// land on three shards of four, and a test that flushes only the connection's
/// shard would then pass by luck. Every shard is proven occupied before the
/// flush, so a fix that reaches only the local one cannot go green.
fn seed(c: &mut Conn, shards: usize) {
    for db in SAMPLED {
        assert_eq!(c.send(&["SELECT", &db.to_string()]), "+OK\r\n");
        for key in shard_spanning_keys(db, shards) {
            assert_eq!(c.send(&["SET", &key, "v"]), "+OK\r\n");
        }
    }
    let seeded = sizes(c);
    assert!(
        seeded.iter().all(|&(_, n)| n == shards as i64),
        "the seed itself did not take (shards={shards}): {seeded:?}"
    );
}

fn shard_spanning_keys(db: u32, shards: usize) -> Vec<String> {
    let mut out: Vec<Option<String>> = vec![None; shards];
    let mut n = 0u32;
    while out.iter().any(Option::is_none) {
        let candidate = format!("lfa:{db}:{n}");
        let slot = key_to_shard(candidate.as_bytes(), shards);
        if out[slot].is_none() {
            out[slot] = Some(candidate);
        }
        n += 1;
        assert!(n < 10_000, "could not cover {shards} shards from db{db}");
    }
    out.into_iter().flatten().collect()
}

fn sizes(c: &mut Conn) -> Vec<(u32, i64)> {
    SAMPLED
        .iter()
        .map(|&db| {
            assert_eq!(c.send(&["SELECT", &db.to_string()]), "+OK\r\n");
            let reply = c.send(&["DBSIZE"]);
            let n = reply
                .strip_prefix(':')
                .and_then(|r| r.trim_end_matches("\r\n").parse::<i64>().ok())
                .unwrap_or_else(|| panic!("DBSIZE on db{db} answered {reply:?}"));
            (db, n)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The defect
// ---------------------------------------------------------------------------

#[test]
fn lfa1_script_flushall_clears_every_database_on_every_shard() {
    for shards in [1usize, 4] {
        let m = spawn_moon(shards);
        let mut c = Conn::open(m.port);
        seed(&mut c, shards);

        assert_eq!(c.send(&["SELECT", "0"]), "+OK\r\n");
        assert_eq!(
            c.send(&["EVAL", "return redis.call('FLUSHALL')", "0"]),
            "+OK\r\n"
        );

        let after = sizes(&mut c);
        assert!(
            after.iter().all(|&(_, n)| n == 0),
            "moon#685 — a script-issued FLUSHALL left keys behind (shards={shards}): {after:?}"
        );
    }
}

/// The same, issued from a database that is not `0` — so a fix that clears
/// "db0 plus the selected one" cannot pass.
#[test]
fn lfa2_script_flushall_from_a_nonzero_database_clears_every_database() {
    let m = spawn_moon(4);
    let mut c = Conn::open(m.port);
    seed(&mut c, 4);

    assert_eq!(c.send(&["SELECT", "7"]), "+OK\r\n");
    assert_eq!(
        c.send(&["EVAL", "return redis.call('FLUSHALL')", "0"]),
        "+OK\r\n"
    );

    let after = sizes(&mut c);
    assert!(
        after.iter().all(|&(_, n)| n == 0),
        "moon#685 — FLUSHALL from a script running in db7 left databases behind: {after:?}"
    );
}

/// `EVALSHA` reaches the bridge through its own call site in every handler, and
/// moon#677's lesson was that a missing arm is invisible to CI. Pinned
/// separately rather than trusted to share `EVAL`'s path.
#[test]
fn lfa3_evalsha_clears_every_database_too() {
    let m = spawn_moon(4);
    let mut c = Conn::open(m.port);
    seed(&mut c, 4);

    let load = c.send(&["SCRIPT", "LOAD", "return redis.call('FLUSHALL')"]);
    let sha = load
        .strip_prefix('$')
        .and_then(|r| r.split_once("\r\n"))
        .map(|(_, rest)| rest.trim_end_matches("\r\n").to_string())
        .unwrap_or_else(|| panic!("SCRIPT LOAD answered {load:?}"));

    assert_eq!(c.send(&["SELECT", "0"]), "+OK\r\n");
    assert_eq!(c.send(&["EVALSHA", &sha, "0"]), "+OK\r\n");

    let after = sizes(&mut c);
    assert!(
        after.iter().all(|&(_, n)| n == 0),
        "moon#685 — EVALSHA's FLUSHALL left keys behind: {after:?}"
    );
}

/// `FCALL` is the fourth script family and the issue does not mention it: a
/// FUNCTION body reaches `redis.call` through the same bridge, so it carried
/// the same bug through four more call sites.
#[test]
fn lfa6_fcall_clears_every_database_too() {
    let m = spawn_moon(4);
    let mut c = Conn::open(m.port);

    let lib = "#!lua name=lfa685\n\
               redis.register_function('lfa_flushall', function() \
               return redis.call('FLUSHALL') end)";
    let loaded = c.send(&["FUNCTION", "LOAD", lib]);
    assert!(
        !loaded.starts_with('-'),
        "FUNCTION LOAD answered {loaded:?}"
    );

    seed(&mut c, 4);
    assert_eq!(c.send(&["SELECT", "0"]), "+OK\r\n");
    assert_eq!(c.send(&["FCALL", "lfa_flushall", "0"]), "+OK\r\n");

    let after = sizes(&mut c);
    assert!(
        after.iter().all(|&(_, n)| n == 0),
        "moon#685 — FCALL's FLUSHALL left keys behind: {after:?}"
    );
}

/// A flush clears the vector/text index CONTENTS along with the keys, and a
/// script's flush is not exempt.
///
/// This is the half that makes "reach every database" safe to add rather than
/// harmful. Clearing the keyspace while the index still answers produces a
/// searchable ghost — `FT.SEARCH` returning documents whose hashes no longer
/// exist — which is precisely the inconsistency the R3 hook exists to prevent,
/// and the completion would have widened it from one database to all sixteen
/// on every shard.
///
/// The `FT.CREATE` DEFINITION must survive, matching restart semantics: a flush
/// empties an index, it does not drop it.
#[test]
fn lfa7_script_flush_clears_index_contents_but_keeps_the_definition() {
    let m = spawn_moon(1);
    let mut c = Conn::open(m.port);

    let created = c.send(&[
        "FT.CREATE",
        "tidx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "body",
        "TEXT",
    ]);
    // CI's tokio leg builds `--no-default-features --features
    // runtime-tokio,jemalloc`, which drops `text-index` — so this case cannot
    // run there. Skipping on the FEATURE FLAG alone would let a real
    // `FT.CREATE` regression skip the test silently on the default build, so
    // the two have to agree: without the feature the server must SAY so, and
    // with it the create must succeed. Neither side can go vacuous.
    if !cfg!(feature = "text-index") {
        assert!(
            created.contains("text-index feature"),
            "built without text-index, so FT.CREATE must refuse for that reason \
             — answered {created:?}"
        );
        eprintln!(
            "SKIP lfa7: this build has no text-index feature (CI's tokio leg \
             drops default features); the index half is covered on every other leg"
        );
        return;
    }
    assert_eq!(created, "+OK\r\n");
    for n in 1..=5 {
        let reply = c.send(&["HSET", &format!("doc:{n}"), "body", "alpha beta gamma"]);
        assert!(!reply.starts_with('-'), "HSET answered {reply:?}");
    }
    std::thread::sleep(Duration::from_millis(500));

    let before = c.send(&["FT.SEARCH", "tidx", "alpha"]);
    assert!(
        before.starts_with("*11\r\n") || before.contains("doc:"),
        "the seed must be searchable before the flush (got {before:?})"
    );

    assert_eq!(
        c.send(&["EVAL", "return redis.call('FLUSHALL')", "0"]),
        "+OK\r\n"
    );
    std::thread::sleep(Duration::from_millis(300));

    assert_eq!(c.send(&["DBSIZE"]), ":0\r\n", "the keyspace must be empty");
    let after = c.send(&["FT.SEARCH", "tidx", "alpha"]);
    assert!(
        after.starts_with("*1\r\n:0\r\n") || after.starts_with(":0\r\n"),
        "moon#685 — the keys are gone but the index still answers: a flushed \
         hash left a searchable ghost (got {after:?})"
    );

    let info = c.send(&["FT.INFO", "tidx"]);
    assert!(
        !info.starts_with('-'),
        "a flush empties an index, it does not drop it — FT.INFO answered {info:?}"
    );
}

// ---------------------------------------------------------------------------
// The counter-test — the half that keeps the fix from being destructive
// ---------------------------------------------------------------------------

/// `FLUSHDB` from Lua must gain whole-SERVER reach without gaining whole-
/// SERVER scope: every shard's copy of db3, and nothing else.
///
/// The hazard in "make FLUSHALL see every database" is implementing it with a
/// caller-passed flag: one inverted call and `FLUSHDB` wipes the server. This
/// is the test that catches that, which is why the completion keys on the
/// COMMAND NAME instead.
#[test]
fn lfa4_script_flushdb_clears_its_own_database_on_every_shard_and_no_other() {
    let m = spawn_moon(4);
    let mut c = Conn::open(m.port);
    seed(&mut c, 4);

    assert_eq!(c.send(&["SELECT", "3"]), "+OK\r\n");
    assert_eq!(
        c.send(&["EVAL", "return redis.call('FLUSHDB')", "0"]),
        "+OK\r\n"
    );

    let after = sizes(&mut c);
    let expected: Vec<(u32, i64)> = SAMPLED
        .iter()
        .map(|&db| (db, if db == 3 { 0 } else { 4 }))
        .collect();
    assert_eq!(
        after, expected,
        "moon#685 — a script-issued FLUSHDB must clear db3 on ALL FOUR shards \
         and leave every other database untouched"
    );
}

// ---------------------------------------------------------------------------
// The plane the bug made disagree with itself
// ---------------------------------------------------------------------------

/// Live state and recovered state must agree about what a script's `FLUSHALL`
/// did.
///
/// Measured against the pre-fix binary, `--appendonly yes`, five databases
/// seeded, one script-issued `FLUSHALL`, then a `kill -9` and a restart:
///
/// ```text
/// live:              db0=0 db1=1 db3=1 db7=1 db15=1
/// after AOF replay:  db0=0 db1=0 db3=0 db7=0 db15=0
/// ```
///
/// The AOF plane was already RIGHT — the record says `FLUSHALL` and
/// `persistence::replay` completes it across the set — so the bug was not a
/// missing effect but a live server that disagreed with its own log. A restart
/// silently deleted four databases that were readable a moment earlier, and by
/// the same mechanism (`replication::apply` calls the same completion) a
/// primary and its replica diverged on any script that called `FLUSHALL`.
///
/// This pins the agreement rather than either side alone: whichever side a
/// future change breaks, the two stop matching.
#[test]
fn lfa5_live_state_and_recovered_state_agree() {
    let m = spawn_moon_with_aof(1, "yes");
    let port = m.port;
    let dir = m.tmp_dir.clone();

    let mut c = Conn::open(port);
    seed(&mut c, 1);
    assert_eq!(c.send(&["SELECT", "0"]), "+OK\r\n");
    assert_eq!(
        c.send(&["EVAL", "return redis.call('FLUSHALL')", "0"]),
        "+OK\r\n"
    );
    let live = sizes(&mut c);
    drop(c);

    // Past the `everysec` fsync, then a hard kill: a clean shutdown would prove
    // less, since it flushes on the way out.
    std::thread::sleep(Duration::from_millis(2100));
    let mut m = m;
    let _ = m.child.kill();
    let _ = m.child.wait();

    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let mut restarted = Command::new(&bin)
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--admin-port",
            "0",
            "--appendonly",
            "yes",
            "--disk-free-min-pct",
            "0",
            "--dir",
            dir.to_str().unwrap_or("/tmp"),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("respawn moon");

    assert!(
        await_ready(port, Duration::from_secs(45)),
        "moon did not come back up on port {port}"
    );

    let mut c = Conn::open(port);
    let recovered = sizes(&mut c);
    drop(c);
    let _ = restarted.kill();
    let _ = restarted.wait();

    assert_eq!(
        live, recovered,
        "moon#685 — the live server and its own AOF disagreed about a \
         script-issued FLUSHALL: a restart deleted databases that were readable \
         before it (and a replica diverged from its primary the same way)"
    );
    assert!(
        live.iter().all(|&(_, n)| n == 0),
        "and both sides must be EMPTY, not merely equal: {live:?}"
    );
}
