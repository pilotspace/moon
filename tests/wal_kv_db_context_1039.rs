//! moon#1039: WAL v3 KV records must replay into the db they were written in.
//!
//! Under `--wal-kv-log on`, a KV write that executes on a shard thread
//! (a cross-shard write, an active-expiry reason-DEL) is appended to the
//! shard's WAL v3 as a bare RESP command. Before the fix that record carried
//! no db context and Phase-4 replay started every shard at db 0, so a
//! restart that recovered KV from the WAL:
//!
//! - put every db 1-15 write into db 0, and
//! - replayed a DEL logged for db 3 against db 0, deleting db 0's
//!   same-named key.
//!
//! Phase 4 is the KV authority whenever no multi-part AOF manifest is on
//! disk; these tests reach it by removing `appendonlydir/` between the
//! SIGKILL and the restart. Each test also proves it is not vacuous: the WAL
//! must actually have replayed KV commands on the restart.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::io::Read as _;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn log_sink(dir: &Path, name: &str) -> Stdio {
    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join(name))
    {
        Ok(f) => Stdio::from(f),
        Err(_) => Stdio::null(),
    }
}

fn spawn(dir: &Path, shards: usize, extra: &[&str]) -> (common::ServerGuard, u16) {
    std::fs::create_dir_all(dir).expect("create test dir");
    common::spawn_listening_guarded(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "yes",
                "--wal-kv-log",
                "on",
                "--disk-free-min-pct",
                "0",
                "--dir",
            ])
            .arg(dir)
            .args(extra)
            .env("RUST_LOG", "moon=info")
            .stdout(log_sink(dir, "server.out"))
            .stderr(log_sink(dir, "server.err"))
            .spawn()
            .expect("spawn moon")
    })
}

fn read_log(dir: &Path) -> String {
    let mut s = String::new();
    for name in ["server.out", "server.err"] {
        if let Ok(mut f) = std::fs::File::open(dir.join(name)) {
            let _ = f.read_to_string(&mut s);
        }
    }
    s
}

fn wait_for_log(dir: &Path, needle: &str, deadline: Duration) -> bool {
    let until = Instant::now() + deadline;
    loop {
        if read_log(dir).contains(needle) {
            return true;
        }
        if Instant::now() >= until {
            return false;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Sum of `cmds=` over every shard's "WAL v3 replay complete" line logged
/// AFTER byte offset `from` of the log (i.e. by the restarted server).
fn wal_cmds_replayed_since(dir: &Path, from: usize) -> usize {
    let log = read_log(dir);
    log.get(from..)
        .unwrap_or("")
        .lines()
        .filter(|l| l.contains("WAL v3 replay complete"))
        .filter_map(|l| {
            let rest = l.split("cmds=").nth(1)?;
            rest.split(|c: char| !c.is_ascii_digit())
                .next()?
                .parse::<usize>()
                .ok()
        })
        .sum()
}

/// Sum of the KV commands the last-resort WAL fallback (no AOF, disk-offload
/// off: `replay_wal_v3_dir_commands`) reports it applied, over every shard's
/// "applied N KV command(s)" line logged AFTER byte offset `from`.
fn wal_last_resort_applied_since(dir: &Path, from: usize) -> usize {
    let log = read_log(dir);
    log.get(from..)
        .unwrap_or("")
        .lines()
        .filter(|l| l.contains("LAST-RESORT"))
        .filter_map(|l| {
            let rest = l.split("applied ").nth(1)?;
            rest.split(|c: char| !c.is_ascii_digit())
                .next()?
                .parse::<usize>()
                .ok()
        })
        .sum()
}

/// SIGKILL and restart on `dir`. With `drop_aof`, remove the AOF first so
/// the WAL is the KV authority on every runtime.
fn crash_and_restart(
    server: &mut common::ServerGuard,
    dir: &Path,
    shards: usize,
    drop_aof: bool,
    extra: &[&str],
) -> (common::ServerGuard, u16, usize) {
    // The WAL buffer reaches the OS at 4 KiB or on the 1 s sync cadence,
    // whichever first; these tests write far less than 4 KiB, so wait out
    // the 1 s cadence. After that the page-cache copy survives SIGKILL.
    std::thread::sleep(Duration::from_millis(1600));
    server.kill_now();
    // monoio (and tokio at --shards > 1) writes the multi-part AOF under
    // `appendonlydir/`; tokio at --shards 1 writes the top-level
    // `appendonly.aof`. Remove whichever this build wrote.
    let aof_dir = dir.join("appendonlydir");
    let aof_file = dir.join("appendonly.aof");
    assert!(
        aof_dir.exists() || aof_file.exists(),
        "precondition: an AOF exists"
    );
    if drop_aof {
        let _ = std::fs::remove_dir_all(&aof_dir);
        let _ = std::fs::remove_file(&aof_file);
    }
    let log_mark = read_log(dir).len();
    let (guard, port) = spawn(dir, shards, extra);
    (guard, port, log_mark)
}

fn keys_in(conn: &mut common::Conn, db: usize) -> Vec<String> {
    conn.send(&["SELECT", &db.to_string()]);
    let reply = conn.send(&["KEYS", "*"]);
    reply
        .lines()
        .filter(|l| !l.starts_with('*') && !l.starts_with('$'))
        .map(str::to_owned)
        .collect()
}

/// Cross-shard writes at `--shards 4` across several dbs: every key the WAL
/// recovers must come back in the db it was written to, and none may leak
/// into another db.
#[test]
fn cross_shard_wal_writes_recover_into_their_own_db_4_shards() {
    cross_shard_case("moon-1039-s4", &[], wal_cmds_replayed_since);
}

/// The same with `--disk-offload disable`. With the AOF gone, boot takes
/// the last-resort WAL fallback (`replay_wal_v3_dir_commands`), not Phase 4.
/// That path decodes the same records and must honour their db the same way
/// (moon#1026 made it apply them at all).
#[test]
fn cross_shard_wal_writes_recover_into_their_own_db_last_resort_4_shards() {
    cross_shard_case(
        "moon-1039-s4-lr",
        &["--disk-offload", "disable"],
        wal_last_resort_applied_since,
    );
}

fn cross_shard_case(name: &str, extra: &[&str], replayed_since: fn(&Path, usize) -> usize) {
    const DBS: [usize; 4] = [0, 3, 7, 15];
    const PER_DB: usize = 40;
    let dir = common::unique_test_dir(name);
    let (mut server, port) = spawn(&dir, 4, extra);
    {
        // PIPELINED: a pipelined batch is what fans the foreign-owned SETs
        // out to their owner shards' threads (`PipelineBatchSlotted`), and a write
        // that executes on a shard thread is what `--wal-kv-log` logs. One
        // unpipelined SET at a time never reaches the WAL (see the
        // non-vacuity check below).
        let mut c = common::Conn::open(port);
        for db in DBS {
            let db_s = db.to_string();
            let kvs: Vec<(String, String)> = (0..PER_DB)
                .map(|i| (format!("k{db}_{i}"), format!("v{db}_{i}")))
                .collect();
            let mut cmds: Vec<Vec<&str>> = vec![vec!["SELECT", &db_s]];
            cmds.extend(kvs.iter().map(|(k, v)| vec!["SET", k.as_str(), v.as_str()]));
            let refs: Vec<&[&str]> = cmds.iter().map(Vec::as_slice).collect();
            let replies = c.pipeline(&refs);
            assert_eq!(
                replies.matches("+OK").count(),
                PER_DB + 1,
                "SELECT {db} + {PER_DB} SETs: {replies:?}"
            );
        }
    }
    let (_restarted, port, log_mark) = crash_and_restart(&mut server, &dir, 4, true, extra);
    let replayed = replayed_since(&dir, log_mark);
    assert!(
        replayed > 0,
        "the WAL replayed no KV commands on restart — this test proved nothing.\n{}",
        read_log(&dir)
    );

    let mut c = common::Conn::open(port);
    let mut misplaced = Vec::new();
    let mut recovered_per_db = [0usize; 16];
    for (db, recovered) in recovered_per_db.iter_mut().enumerate() {
        for key in keys_in(&mut c, db) {
            let written_db: usize = key
                .strip_prefix('k')
                .and_then(|r| r.split('_').next())
                .and_then(|d| d.parse().ok())
                .unwrap_or(usize::MAX);
            if written_db != db {
                misplaced.push(format!(
                    "{key} (written to db {written_db}) found in db {db}"
                ));
            } else {
                *recovered += 1;
                c.send(&["SELECT", &db.to_string()]);
                let v = c.send(&["GET", &key]);
                let want = key.replacen('k', "v", 1);
                assert!(v.contains(&want), "{key} in db {db}: got {v:?}");
            }
        }
    }
    assert!(
        misplaced.is_empty(),
        "cross-db corruption after WAL replay: {misplaced:#?}"
    );
    for db in [3usize, 7, 15] {
        assert!(
            recovered_per_db[db] > 0,
            "no db-{db} key recovered at all (recovered per db: {recovered_per_db:?}) — \
             the WAL carried none, so this test proved nothing for db {db}"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// `--shards 1`: an active-expiry DEL logged for db 3 must not delete db 0's
/// same-named key on WAL replay.
///
/// `drop_aof = false` is the untouched-directory case. On the tokio runtime
/// at `--shards 1` the top-level `appendonly.aof` is not the KV authority, so
/// Phase 4 replays the WAL anyway and the bug fired on a plain SIGKILL +
/// restart. On monoio the multi-part AOF is the authority and Phase 4 skips
/// the WAL's KV records, so that variant must simply stay correct there.
fn expiry_del_case(drop_aof: bool) {
    let dir = common::unique_test_dir("moon-1039-s1");
    let (mut server, port) = spawn(&dir, 1, &[]);
    {
        let mut c = common::Conn::open(port);
        assert!(c.send(&["SET", "k", "keep"]).starts_with("+OK"));
        // Put db 0's `k` in the snapshot Phase 2 loads; the WAL tail after
        // the snapshot then holds only the expiry DEL below.
        let r = c.send(&["BGSAVE"]);
        assert!(r.starts_with('+'), "BGSAVE: {r:?}");
        assert!(
            wait_for_log(&dir, "snapshot epoch 1 complete", Duration::from_secs(15)),
            "snapshot never completed:\n{}",
            read_log(&dir)
        );
        assert!(c.send(&["SELECT", "3"]).starts_with("+OK"));
        assert!(c.send(&["SET", "k", "gone", "PX", "50"]).starts_with("+OK"));
    }
    // Let ACTIVE expiry (not a lazy read) reap db 3's `k`: that path logs the
    // reason-DEL to the WAL. Never touch db 3's `k` until after the restart.
    std::thread::sleep(Duration::from_millis(1500));
    let (_restarted, port, log_mark) = crash_and_restart(&mut server, &dir, 1, drop_aof, &[]);
    let wal_is_kv_authority = drop_aof || !cfg!(feature = "runtime-monoio");
    if wal_is_kv_authority {
        let replayed = wal_cmds_replayed_since(&dir, log_mark);
        assert!(
            replayed > 0,
            "the WAL replayed no KV commands (the expiry DEL was never logged) — \
             this test proved nothing.\n{}",
            read_log(&dir)
        );
    }
    let mut c = common::Conn::open(port);
    let v = c.send(&["GET", "k"]);
    assert!(
        v.contains("keep"),
        "db 0's `k` was deleted by a DEL logged for db 3: GET k -> {v:?}"
    );
    assert!(c.send(&["SELECT", "3"]).starts_with("+OK"));
    let e = c.send(&["EXISTS", "k"]);
    assert_eq!(e.trim(), ":0", "db 3's `k` expired before the crash");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn expiry_del_in_db3_spares_db0_key_on_wal_replay_1_shard() {
    expiry_del_case(true);
}

#[test]
fn expiry_del_in_db3_spares_db0_key_after_plain_restart_1_shard() {
    expiry_del_case(false);
}
