//! End-to-end tests for CMD-01..CMD-05 (Phase 137 Plan 01).
//!
//! Spawns the release moon binary on a free TCP port and shells out to
//! `redis-cli` to exercise every new admin command. Skips gracefully when
//! the release binary or `redis-cli` is missing (e.g. when `cargo test`
//! runs on macOS without first building via OrbStack).
//!
//! Run with:
//!   cargo test --release --test cmd_flush_dbsize_debug_memory

mod common;

use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn redis_cli_available() -> bool {
    Command::new("redis-cli")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn release_binary() -> std::path::PathBuf {
    // MOON_BIN-aware: the hardcoded target/release/moon fallback inside
    // find_moon_binary is a stale-binary trap on shared checkouts (VM runs
    // exec a host Mach-O via OrbStack's proxy and never accept in-VM).
    common::find_moon_binary()
}

/// Running moon instance. Auto-killed on drop.
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

/// Start moon on a fresh port. Returns `None` if the binary is missing,
/// `redis-cli` is missing, or the process never accepts connections.
fn spawn_moon() -> Option<Moon> {
    spawn_moon_shards(1)
}

/// Same, with an explicit shard count. moon#677 needs both: the bug is
/// present at `--shards 1` (so it is not a routing problem) AND has to stay
/// fixed at `--shards 4`, where the flush reaches the other shards through
/// `coordinate_flush_broadcast` rather than the local path.
fn spawn_moon_shards(shards: usize) -> Option<Moon> {
    if !redis_cli_available() {
        eprintln!("skipping: redis-cli not in PATH");
        return None;
    }
    let bin = release_binary();
    if !bin.exists() {
        eprintln!(
            "skipping: {} not built. Run `cargo build --release` first.",
            bin.display()
        );
        return None;
    }
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-test-{port}"));
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
                "no",
                "--dir",
                tmp_dir.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    });
    // Same directory formula the closure used for the winning attempt.
    let tmp_dir = std::env::temp_dir().join(format!("moon-test-{port}"));
    let moon = Moon {
        child,
        port,
        tmp_dir,
    };

    // Wait up to ~5s for PING to succeed.
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if redis_cli(moon.port, &["PING"])
            .map(|out| out.trim() == "PONG")
            .unwrap_or(false)
        {
            return Some(moon);
        }
        thread::sleep(Duration::from_millis(100));
    }
    eprintln!("skipping: moon did not respond to PING within 5s on port {port}");
    None
}

/// Run `redis-cli -p <port> <args...>` and return stdout as String.
fn redis_cli(port: u16, args: &[&str]) -> Option<String> {
    let output = Command::new("redis-cli")
        .args(["-p", &port.to_string()])
        .args(args)
        .output()
        .ok()?;
    Some(String::from_utf8_lossy(&output.stdout).into_owned())
}

// ---------------------------------------------------------------------------
// CMD-01 + CMD-02: FLUSHALL / FLUSHDB / DBSIZE
// ---------------------------------------------------------------------------

#[test]
fn cmd_flushall_flushdb_dbsize_roundtrip() {
    let Some(m) = spawn_moon() else { return };

    assert_eq!(
        redis_cli(m.port, &["SET", "k1", "v1"]).unwrap().trim(),
        "OK"
    );
    assert_eq!(
        redis_cli(m.port, &["SET", "k2", "v2"]).unwrap().trim(),
        "OK"
    );
    assert_eq!(redis_cli(m.port, &["DBSIZE"]).unwrap().trim(), "2");

    assert_eq!(redis_cli(m.port, &["FLUSHDB"]).unwrap().trim(), "OK");
    assert_eq!(redis_cli(m.port, &["DBSIZE"]).unwrap().trim(), "0");

    assert_eq!(
        redis_cli(m.port, &["SET", "k3", "v3"]).unwrap().trim(),
        "OK"
    );
    assert_eq!(redis_cli(m.port, &["DBSIZE"]).unwrap().trim(), "1");
    assert_eq!(redis_cli(m.port, &["FLUSHALL"]).unwrap().trim(), "OK");
    assert_eq!(redis_cli(m.port, &["DBSIZE"]).unwrap().trim(), "0");

    // ASYNC / SYNC qualifiers accepted for compatibility.
    assert_eq!(
        redis_cli(m.port, &["FLUSHALL", "ASYNC"]).unwrap().trim(),
        "OK"
    );
    assert_eq!(
        redis_cli(m.port, &["FLUSHDB", "SYNC"]).unwrap().trim(),
        "OK"
    );

    // Garbage qualifier should error (syntax error).
    let err = redis_cli(m.port, &["FLUSHDB", "BANANAS"]).unwrap();
    assert!(
        err.to_uppercase().contains("ERR") && err.to_uppercase().contains("SYNTAX"),
        "expected syntax error, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// CMD-03: DEBUG OBJECT
// ---------------------------------------------------------------------------

#[test]
fn cmd_debug_object_returns_redis_format() {
    let Some(m) = spawn_moon() else { return };

    assert_eq!(
        redis_cli(m.port, &["SET", "dk", "hello"]).unwrap().trim(),
        "OK"
    );

    let out = redis_cli(m.port, &["DEBUG", "OBJECT", "dk"]).unwrap();
    assert!(out.contains("encoding:"), "missing encoding: in {out}");
    assert!(out.contains("refcount:1"), "missing refcount:1 in {out}");
    assert!(
        out.contains("serializedlength:"),
        "missing serializedlength in {out}"
    );

    let err = redis_cli(m.port, &["DEBUG", "OBJECT", "nonexistent"]).unwrap();
    assert!(
        err.to_uppercase().contains("NO SUCH KEY"),
        "expected ERR no such key, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// CMD-05: DEBUG SLEEP
// ---------------------------------------------------------------------------

#[test]
fn cmd_debug_sleep_blocks_expected_duration() {
    let Some(m) = spawn_moon() else { return };

    let start = Instant::now();
    assert_eq!(
        redis_cli(m.port, &["DEBUG", "SLEEP", "0.25"])
            .unwrap()
            .trim(),
        "OK"
    );
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(220),
        "DEBUG SLEEP 0.25 returned too fast: {elapsed:?}"
    );
    // Sanity cap — should not take >5s for a 0.25s sleep.
    assert!(
        elapsed < Duration::from_secs(5),
        "DEBUG SLEEP 0.25 took too long: {elapsed:?}"
    );

    // Zero sleep returns immediately.
    let start = Instant::now();
    assert_eq!(
        redis_cli(m.port, &["DEBUG", "SLEEP", "0"]).unwrap().trim(),
        "OK"
    );
    assert!(start.elapsed() < Duration::from_millis(500));
}

// ---------------------------------------------------------------------------
// CMD-04: MEMORY USAGE
// ---------------------------------------------------------------------------

#[test]
fn cmd_memory_usage_returns_integer_or_nil() {
    let Some(m) = spawn_moon() else { return };

    assert_eq!(
        redis_cli(m.port, &["SET", "mk", "abcdefghij"])
            .unwrap()
            .trim(),
        "OK"
    );

    let out = redis_cli(m.port, &["MEMORY", "USAGE", "mk"]).unwrap();
    let n: i64 = out
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("expected integer, got: {out:?}"));
    assert!(n >= 10, "MEMORY USAGE expected >=10, got {n}");

    let miss = redis_cli(m.port, &["MEMORY", "USAGE", "nonexistent"]).unwrap();
    let trimmed = miss.trim();
    assert!(
        trimmed.is_empty() || trimmed == "(nil)",
        "expected nil for missing key, got: {miss:?}"
    );

    // SAMPLES flag is accepted as a no-op.
    let out = redis_cli(m.port, &["MEMORY", "USAGE", "mk", "SAMPLES", "5"]).unwrap();
    let n: i64 = out
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("expected integer, got: {out:?}"));
    assert!(n >= 10);
}

// ---------------------------------------------------------------------------
// moon#677: FLUSHALL means EVERY database
// ---------------------------------------------------------------------------

/// The databases these tests populate. Not 0..16 — the point is to prove the
/// flush reaches databases the connection never selected, and a sparse set
/// makes a partial fix (say, "db0 and db1") visible instead of accidentally
/// passing.
const PROBE_DBS: [&str; 5] = ["0", "1", "3", "7", "15"];

/// `redis-cli -n <db>` picks the database for that invocation. Every call
/// here is its own connection, which is exactly why the db is passed as a
/// flag rather than as a preceding `SELECT`: a `SELECT` sent through a
/// separate `redis-cli` process would apply to a connection that closes
/// before the next command is written.
fn cli_db(port: u16, db: &str, args: &[&str]) -> String {
    let output = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "-n", db])
        .args(args)
        .output()
        .expect("redis-cli");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn seed_probe_dbs(port: u16) {
    for db in PROBE_DBS {
        assert_eq!(
            cli_db(port, db, &["SET", &format!("k{db}"), "v"]),
            "OK",
            "seeding db{db} failed"
        );
        assert_eq!(cli_db(port, db, &["DBSIZE"]), "1", "db{db} not seeded");
    }
}

fn assert_flushall_emptied_everything(port: u16, shards: usize) {
    let survivors: Vec<String> = PROBE_DBS
        .iter()
        .map(|db| (db, cli_db(port, db, &["DBSIZE"])))
        .filter(|(_, size)| size != "0")
        .map(|(db, size)| format!("db{db}={size}"))
        .collect();
    assert!(
        survivors.is_empty(),
        "FLUSHALL (shards={shards}) left {} database(s) populated: {}. \
         An operator who runs FLUSHALL believes the instance is empty.",
        survivors.len(),
        survivors.join(" ")
    );
}

#[test]
fn flushall_clears_every_database_not_only_the_selected_one() {
    let Some(m) = spawn_moon() else { return };
    seed_probe_dbs(m.port);

    // Issued from db0, the database the connection selected.
    assert_eq!(cli_db(m.port, "0", &["FLUSHALL"]), "OK");
    assert_flushall_emptied_everything(m.port, 1);
}

#[test]
fn flushall_from_a_non_zero_database_also_clears_db0() {
    let Some(m) = spawn_moon() else { return };
    seed_probe_dbs(m.port);

    // The mirror case: a fix that special-cased "also clear db0" rather than
    // clearing every database would pass the test above and fail this one.
    assert_eq!(cli_db(m.port, "7", &["FLUSHALL"]), "OK");
    assert_flushall_emptied_everything(m.port, 1);
}

#[test]
fn flushall_clears_every_database_across_shards() {
    let Some(m) = spawn_moon_shards(4) else {
        return;
    };
    seed_probe_dbs(m.port);

    assert_eq!(cli_db(m.port, "0", &["FLUSHALL"]), "OK");
    assert_flushall_emptied_everything(m.port, 4);
}

#[test]
fn flushdb_still_clears_only_the_selected_database() {
    let Some(m) = spawn_moon() else { return };
    seed_probe_dbs(m.port);

    // The counter-test for the fix above: FLUSHDB must NOT grow into
    // FLUSHALL. Without this, "clear every database" passes both commands'
    // tests and silently makes FLUSHDB destructive.
    assert_eq!(cli_db(m.port, "3", &["FLUSHDB"]), "OK");
    assert_eq!(cli_db(m.port, "3", &["DBSIZE"]), "0");
    for db in PROBE_DBS.iter().filter(|d| **d != "3") {
        assert_eq!(
            cli_db(m.port, db, &["DBSIZE"]),
            "1",
            "FLUSHDB in db3 wrongly cleared db{db}"
        );
    }
}

#[test]
fn info_keyspace_agrees_with_flushall() {
    let Some(m) = spawn_moon() else { return };
    seed_probe_dbs(m.port);
    assert_eq!(cli_db(m.port, "0", &["FLUSHALL"]), "OK");

    // DBSIZE is per-connection-db; INFO keyspace is the whole-instance view,
    // and it is what an operator actually looks at after a flush. Both have
    // to agree that nothing is left.
    let info = cli_db(m.port, "0", &["INFO", "keyspace"]);
    let leftovers: Vec<&str> = info
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with("db") && l.contains("keys="))
        .filter(|l| !l.contains("keys=0"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "INFO keyspace still lists populated databases after FLUSHALL: {leftovers:?}"
    );
}

/// A FLUSHALL that a restart undoes is not a flush. This is the AOF-replay
/// leg: the record is logged with the writer's selected db, so a replayer
/// that hands it to `command::dispatch` clears that one database and restores
/// the other fifteen from the log.
///
/// SIGKILL rather than SHUTDOWN on purpose — a clean shutdown can paper over
/// a replay bug by rewriting the log from live state.
#[test]
fn flushall_survives_a_restart() {
    if !redis_cli_available() {
        eprintln!("skipping: redis-cli not in PATH");
        return;
    }
    let bin = release_binary();
    if !bin.exists() {
        eprintln!("skipping: {} not built", bin.display());
        return;
    }

    let dir = std::env::temp_dir().join(format!("moon-flushall-restart-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::create_dir_all(&dir);

    let spawn = |port: u16| {
        Command::new(&bin)
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
                dir.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    };

    let (mut child, port) = common::spawn_listening(spawn);
    let ready = |port: u16| {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if redis_cli(port, &["PING"])
                .map(|o| o.trim() == "PONG")
                .unwrap_or(false)
            {
                return true;
            }
            thread::sleep(Duration::from_millis(100));
        }
        false
    };
    if !ready(port) {
        common::sigkill(&mut child);
        let _ = std::fs::remove_dir_all(&dir);
        eprintln!("skipping: moon did not become ready on {port}");
        return;
    }

    seed_probe_dbs(port);
    assert_eq!(cli_db(port, "0", &["FLUSHALL"]), "OK");
    // Give the 1ms-tick WAL buffer a moment to reach the log before SIGKILL.
    thread::sleep(Duration::from_millis(300));

    common::sigkill(&mut child);
    common::wait_for_port_down(port);

    let mut child2 = spawn(port);
    let restarted = ready(port);
    let outcome = if restarted {
        PROBE_DBS
            .iter()
            .map(|db| (db, cli_db(port, db, &["DBSIZE"])))
            .filter(|(_, size)| size != "0")
            .map(|(db, size)| format!("db{db}={size}"))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    common::sigkill(&mut child2);
    let _ = std::fs::remove_dir_all(&dir);

    assert!(restarted, "moon did not come back up after SIGKILL");
    assert!(
        outcome.is_empty(),
        "restart resurrected {} database(s) a FLUSHALL had emptied: {}",
        outcome.len(),
        outcome.join(" ")
    );
}

/// A queued `FLUSHALL` is a different code path from an immediate one — `EXEC`
/// replays the body through `execute_transaction`, and the post-EXEC hooks are
/// a separate block from the live write path's. It had no coverage at all
/// until CodeRabbit pointed at moon#677's first cut of it, where the keyspace
/// clear had been placed inside a `vector_store`-gated loop.
///
/// One connection for the whole exchange: `MULTI` state is connection-scoped,
/// so the per-invocation `redis-cli` the other tests use would send `MULTI`,
/// `FLUSHALL` and `EXEC` down three unrelated connections and assert nothing.
#[test]
fn flushall_inside_multi_exec_clears_every_database() {
    let Some(m) = spawn_moon() else { return };
    seed_probe_dbs(m.port);

    let out = cli_pipe(m.port, "SELECT 0\nMULTI\nFLUSHALL\nEXEC\n");
    assert!(
        out.contains("OK"),
        "MULTI/FLUSHALL/EXEC did not succeed: {out:?}"
    );

    assert_flushall_emptied_everything(m.port, 1);
}

/// Feed several commands to one `redis-cli` process — and therefore one
/// connection — via stdin.
fn cli_pipe(port: u16, script: &str) -> String {
    use std::io::Write as _;
    let mut child = Command::new("redis-cli")
        .args(["-p", &port.to_string()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("redis-cli");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(script.as_bytes())
        .expect("write");
    let out = child.wait_with_output().expect("wait");
    String::from_utf8_lossy(&out.stdout).into_owned()
}
