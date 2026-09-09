//! moon#863 — a replica must hold the master's data in the SAME encoding.
//!
//! The unit-level guard lives in `tests/rdb_wire_compact_encodings_863.rs`;
//! this suite proves the property end to end over a REAL FULLRESYNC, which is
//! the path the issue is actually about: `replication::apply::load_snapshot`
//! feeds the snapshot straight to `redis_rdb::load_rdb`, so before the fix a
//! replica flattened every compact encoding and held the master's data in the
//! expensive form until each key was rewritten. `OBJECT ENCODING` on the
//! replica disagreed with the master's, which is client-visible.
//!
//! Run (integration tests here are `#[ignore]`d, per this repo's convention
//! for suites that spawn real servers):
//!
//! ```text
//! MOON_BIN=./target/release/moon \
//!   cargo test --test replication_encoding_parity_863 -- --ignored --nocapture
//! ```
//!
//! Server dirs are `tempfile::tempdir()` PLUS `--disk-free-min-pct 0` on every
//! spawn, matching `tests/replication_planes.rs` — without that flag the
//! disk-free guard silently guts the kill -9 leg.
//!
//! Footprint: the kill -9 leg deliberately builds a ~240 MB master so the
//! snapshot cannot finish transferring inside the kill window — it ASSERTS the
//! kill landed mid-resync and tells you to raise `BULK`/`PAD` if a faster host
//! outruns it, rather than going green having tested nothing. Budget ~600 MB
//! across the two processes for that test.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

fn moon_bin() -> std::path::PathBuf {
    common::find_moon_binary()
}

fn start_moon(port: u16, dir: &str, shards: usize, extra: &[&str]) -> Child {
    let port_s = port.to_string();
    let shards_s = shards.to_string();
    let mut full: Vec<&str> = vec![
        "--port",
        &port_s,
        "--shards",
        &shards_s,
        "--dir",
        dir,
        "--disk-free-min-pct",
        "0",
        "--appendonly",
        "no",
    ];
    full.extend_from_slice(extra);
    Command::new(moon_bin())
        .args(&full)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to start moon (set MOON_BIN to a built binary)")
}

fn read_one_reply<R: BufRead>(reader: &mut R) -> String {
    let mut out = String::new();
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {
                let trimmed = line.trim_end_matches("\r\n").trim_end_matches('\n');
                if trimmed.starts_with('+') || trimmed.starts_with('-') || trimmed.starts_with(':')
                {
                    out.push_str(trimmed);
                    break;
                }
                if let Some(rest) = trimmed.strip_prefix('$') {
                    let len: i64 = rest.trim().parse().unwrap_or(-1);
                    if len < 0 {
                        break;
                    }
                    let mut buf = vec![0u8; (len as usize) + 2];
                    if reader.read_exact(&mut buf).is_ok() {
                        out.push_str(&String::from_utf8_lossy(&buf[..len as usize]));
                    }
                    break;
                }
            }
        }
    }
    out
}

/// One inline command on a fresh connection. Fresh-per-probe is safe here:
/// nothing in this suite blocks, and every assertion is a whole-key property
/// (`OBJECT ENCODING`, `LLEN`) that does not depend on connection state.
fn send_cmd(addr: &str, cmd: &str) -> String {
    let Ok(mut stream) = TcpStream::connect(addr) else {
        return String::new();
    };
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    if stream.write_all(format!("{}\r\n", cmd).as_bytes()).is_err() {
        return String::new();
    }
    stream.flush().ok();
    let mut reader = BufReader::new(&stream);
    read_one_reply(&mut reader)
}

/// Pipelined inline burst on one connection; every reply drained in order.
fn pipeline(addr: &str, cmds: &[String]) {
    let Ok(mut stream) = TcpStream::connect(addr) else {
        panic!("pipeline: connect failed to {addr}");
    };
    stream.set_read_timeout(Some(Duration::from_secs(20))).ok();
    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
    let mut buf = String::new();
    for c in cmds {
        buf.push_str(c);
        buf.push_str("\r\n");
    }
    stream.write_all(buf.as_bytes()).expect("write");
    stream.flush().ok();
    for _ in cmds {
        read_one_reply(&mut reader);
    }
}

fn wait_until<F: Fn() -> bool>(timeout: Duration, f: F) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if f() {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

fn await_ready(addr: &str) {
    assert!(
        wait_until(Duration::from_secs(20), || send_cmd(addr, "PING")
            .starts_with("+PONG")),
        "server at {addr} did not become ready"
    );
}

fn await_link_up(replica_addr: &str) {
    assert!(
        wait_until(Duration::from_secs(30), || send_cmd(
            replica_addr,
            "INFO replication"
        )
        .contains("master_link_status:up")),
        "replica {replica_addr} link did not come up"
    );
}

fn dbsize(addr: &str) -> i64 {
    send_cmd(addr, "DBSIZE")
        .strip_prefix(':')
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(-1)
}

fn encoding(addr: &str, key: &str) -> String {
    send_cmd(addr, &format!("OBJECT ENCODING {key}"))
}

fn integer_reply(addr: &str, cmd: &str) -> i64 {
    send_cmd(addr, cmd)
        .strip_prefix(':')
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(-1)
}

struct Guard(Vec<Child>);
impl Drop for Guard {
    fn drop(&mut self) {
        for c in &mut self.0 {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn spawn_into(guard: &mut Guard, dir: &str, shards: usize, extra: &[&str]) -> u16 {
    let (child, port) = common::spawn_listening(|port| start_moon(port, dir, shards, extra));
    guard.0.push(child);
    port
}

/// SAFETY: `pid` is a live child PID this test spawned itself; SIGKILL is
/// always a valid signal to send. Mirrors `tests/replication_planes.rs`.
fn sigkill(child: &mut Child) {
    #[cfg(unix)]
    {
        // SAFETY: see doc comment above.
        let ret = unsafe { libc::kill(child.id() as i32, libc::SIGKILL) };
        assert_eq!(ret, 0, "libc::kill failed");
        let _ = child.wait();
    }
    #[cfg(not(unix))]
    {
        let _ = child.kill();
        let _ = child.wait();
    }
}

/// One key per compact encoding, all comfortably below the listpack limits
/// (`LISTPACK_MAX_ENTRIES` = 128, `LISTPACK_MAX_ELEMENT_SIZE` = 64 B).
/// `si` is all-integer on purpose — that is the intset row.
const COMPACT_KEYS: [&str; 5] = ["si", "ss", "h", "l", "z"];

fn write_compact_dataset(master: &str) {
    pipeline(
        master,
        &[
            "SADD si 1 2 3".to_string(),
            "SADD ss x y z".to_string(),
            "HSET h f1 v1 f2 v2".to_string(),
            "RPUSH l a b c".to_string(),
            "ZADD z 1.5 alice 2.5 bob".to_string(),
        ],
    );
}

// ===========================================================================
// 1. Encoding parity across a real FULLRESYNC.
// ===========================================================================

fn run_resync_encoding_parity(shards: usize) {
    let mdir = tempfile::tempdir().expect("mdir");
    let rdir = tempfile::tempdir().expect("rdir");
    let mut guard = Guard(vec![]);
    let master_port = spawn_into(&mut guard, mdir.path().to_str().unwrap(), shards, &[]);
    // The replica is ALWAYS single-shard: multi-shard replicas are a v0.9
    // item, and every multi-shard-master suite in this repo
    // (`tests/replication_multishard.rs`) pairs `--shards N` with a 1-shard
    // replica. A 4-shard replica simply never brings its link up.
    let replica_port = spawn_into(&mut guard, rdir.path().to_str().unwrap(), 1, &[]);
    let m = format!("127.0.0.1:{master_port}");
    let r = format!("127.0.0.1:{replica_port}");
    await_ready(&m);
    await_ready(&r);

    // Write BEFORE attaching, so the data can only reach the replica through
    // the FULLRESYNC snapshot — not through the live command stream, which
    // would build each container with the same command path the master used
    // and hide the codec entirely.
    write_compact_dataset(&m);

    let master_enc: Vec<String> = COMPACT_KEYS.iter().map(|k| encoding(&m, k)).collect();
    assert_eq!(
        master_enc,
        vec!["intset", "listpack", "listpack", "listpack", "listpack"],
        "setup invariant broken: the master is not holding the compact forms \
         (SADD/ZADD listpack encodings landed in moon#877/#878)"
    );

    assert!(
        send_cmd(&r, &format!("REPLICAOF 127.0.0.1 {master_port}")).starts_with("+OK"),
        "REPLICAOF refused"
    );
    await_link_up(&r);
    assert!(
        wait_until(Duration::from_secs(20), || dbsize(&r)
            == COMPACT_KEYS.len() as i64),
        "replica never received the {} keys (dbsize={})",
        COMPACT_KEYS.len(),
        dbsize(&r)
    );

    let mismatches: Vec<String> = COMPACT_KEYS
        .iter()
        .zip(&master_enc)
        .filter_map(|(key, want)| {
            let got = encoding(&r, key);
            (&got != want).then(|| format!("{key}: master={want} replica={got}"))
        })
        .collect();
    assert!(
        mismatches.is_empty(),
        "moon#863 (shards={shards}): the replica flattened compact encodings across \
         FULLRESYNC — it holds the master's data in the expensive form: {mismatches:?}"
    );

    // Content parity, because an encoding that matches but has lost data is
    // worse than one that does not match.
    assert_eq!(integer_reply(&r, "SCARD si"), 3, "si content");
    assert_eq!(integer_reply(&r, "SCARD ss"), 3, "ss content");
    assert_eq!(integer_reply(&r, "HLEN h"), 2, "h content");
    assert_eq!(integer_reply(&r, "LLEN l"), 3, "l content");
    assert_eq!(integer_reply(&r, "ZCARD z"), 2, "z content");
    assert_eq!(send_cmd(&r, "LRANGE l 0 0"), "a", "list order preserved");
    assert_eq!(send_cmd(&r, "ZSCORE z bob"), "2.5", "zset score preserved");
}

#[test]
#[ignore]
fn resync_encoding_parity_single_shard() {
    run_resync_encoding_parity(1);
}

#[test]
#[ignore]
fn resync_encoding_parity_four_shards() {
    run_resync_encoding_parity(4);
}

// ===========================================================================
// 2. Above the thresholds: full form on BOTH sides, and nothing truncated.
//    The moon#866 guard at the replication level — a compaction step that
//    silently dropped elements would leave this suite's encodings correct.
// ===========================================================================

#[test]
#[ignore]
fn resync_does_not_truncate_above_threshold_containers() {
    const HUGE: usize = 5000;
    let mdir = tempfile::tempdir().expect("mdir");
    let rdir = tempfile::tempdir().expect("rdir");
    let mut guard = Guard(vec![]);
    let master_port = spawn_into(&mut guard, mdir.path().to_str().unwrap(), 1, &[]);
    let replica_port = spawn_into(&mut guard, rdir.path().to_str().unwrap(), 1, &[]);
    let m = format!("127.0.0.1:{master_port}");
    let r = format!("127.0.0.1:{replica_port}");
    await_ready(&m);
    await_ready(&r);

    for chunk in (0..HUGE).collect::<Vec<_>>().chunks(500) {
        let cmds: Vec<String> = chunk
            .iter()
            .flat_map(|i| {
                [
                    format!("RPUSH bigl e{i}"),
                    format!("SADD bigs m{i}"),
                    format!("HSET bigh f{i} v{i}"),
                    format!("ZADD bigz {i} z{i}"),
                ]
            })
            .collect();
        pipeline(&m, &cmds);
    }

    let master_enc: Vec<String> = ["bigl", "bigs", "bigh", "bigz"]
        .iter()
        .map(|k| encoding(&m, k))
        .collect();
    assert_eq!(
        master_enc,
        vec!["linkedlist", "hashtable", "hashtable", "skiplist"],
        "setup invariant broken: {HUGE} elements should be past every listpack threshold"
    );

    assert!(send_cmd(&r, &format!("REPLICAOF 127.0.0.1 {master_port}")).starts_with("+OK"));
    await_link_up(&r);
    assert!(
        wait_until(Duration::from_secs(60), || dbsize(&r) == 4),
        "replica never received the 4 keys (dbsize={})",
        dbsize(&r)
    );

    for (key, want) in ["bigl", "bigs", "bigh", "bigz"].iter().zip(&master_enc) {
        assert_eq!(
            &encoding(&r, key),
            want,
            "{key} must stay in the full form on the replica too"
        );
    }
    for (cmd, what) in [
        ("LLEN bigl", "list"),
        ("SCARD bigs", "set"),
        ("HLEN bigh", "hash"),
        ("ZCARD bigz", "zset"),
    ] {
        assert!(
            wait_until(Duration::from_secs(60), || integer_reply(&r, cmd)
                == HUGE as i64),
            "moon#866 class: the {what} lost elements across FULLRESYNC ({cmd} = {}, want {HUGE})",
            integer_reply(&r, cmd)
        );
    }
}

// ===========================================================================
// 3. kill -9 the replica MID-RESYNC. It must come back, re-sync from scratch,
//    and land on the same encodings — a half-applied snapshot must never
//    survive as state.
// ===========================================================================

#[test]
#[ignore]
fn replica_killed_mid_resync_recovers_with_matching_encodings() {
    // Ballast sized so the snapshot (~240 MB) cannot transfer and apply inside
    // the short sleep below — the leg is worthless if the kill lands after the
    // resync already finished, so that is asserted, not assumed.
    const BULK: usize = 60_000;
    const PAD: usize = 4096;
    let mdir = tempfile::tempdir().expect("mdir");
    let rdir = tempfile::tempdir().expect("rdir");
    let mut guard = Guard(vec![]);
    let master_port = spawn_into(&mut guard, mdir.path().to_str().unwrap(), 1, &[]);
    let m = format!("127.0.0.1:{master_port}");
    await_ready(&m);

    // Ballast so the snapshot takes long enough for the SIGKILL to land while
    // the resync is still in flight, plus the five compact keys we assert on.
    for chunk in (0..BULK).collect::<Vec<_>>().chunks(1000) {
        let cmds: Vec<String> = chunk
            .iter()
            .map(|i| format!("SET pad:{i} {}", "v".repeat(PAD)))
            .collect();
        pipeline(&m, &cmds);
    }
    write_compact_dataset(&m);
    let master_enc: Vec<String> = COMPACT_KEYS.iter().map(|k| encoding(&m, k)).collect();
    assert_eq!(
        master_enc,
        vec!["intset", "listpack", "listpack", "listpack", "listpack"],
        "setup invariant broken: the master is not holding the compact forms"
    );
    let expected_keys = (BULK + COMPACT_KEYS.len()) as i64;
    assert_eq!(dbsize(&m), expected_keys, "master dataset");

    // Replica generation 1: attach, then SIGKILL without waiting for the link.
    let rdir_path = rdir.path().to_str().unwrap().to_string();
    let replica_port = spawn_into(&mut guard, &rdir_path, 1, &[]);
    let r = format!("127.0.0.1:{replica_port}");
    await_ready(&r);
    assert!(send_cmd(&r, &format!("REPLICAOF 127.0.0.1 {master_port}")).starts_with("+OK"));
    // Kill while the resync is genuinely IN FLIGHT. `load_snapshot` applies
    // the payload in one go, so a replica that has not reached the master's
    // key count has not finished — and a replica too busy to answer at all
    // (dbsize == -1) certainly has not either. Asserted, because a leg that
    // silently degenerated into "kill an already-synced replica" would still
    // go green and prove nothing.
    thread::sleep(Duration::from_millis(15));
    let progress = dbsize(&r);
    assert!(
        progress < expected_keys,
        "the replica finished its resync before the SIGKILL (dbsize={progress}, \
         want < {expected_keys}) — this leg did not test a mid-resync kill; \
         raise BULK/PAD"
    );
    println!("kill -9 at replica dbsize={progress} of {expected_keys}");
    let mut gen1 = guard.0.pop().expect("replica child");
    sigkill(&mut gen1);

    // Generation 2: same dir, same port on purpose — a restart, not a new node.
    // Push into the guard IMMEDIATELY, before any panic-able wait can leak it:
    // moon binds with SO_REUSEPORT, so a leaked server SHARES the port with
    // the next run's and splits its connections.
    guard.0.push(start_moon(replica_port, &rdir_path, 1, &[]));
    await_ready(&r);
    assert!(send_cmd(&r, &format!("REPLICAOF 127.0.0.1 {master_port}")).starts_with("+OK"));
    await_link_up(&r);
    assert!(
        wait_until(Duration::from_secs(120), || dbsize(&r) == expected_keys),
        "replica did not re-sync the full dataset after a mid-resync kill -9 \
         (dbsize={}, want {expected_keys})",
        dbsize(&r)
    );

    let mismatches: Vec<String> = COMPACT_KEYS
        .iter()
        .zip(&master_enc)
        .filter_map(|(key, want)| {
            let got = encoding(&r, key);
            (&got != want).then(|| format!("{key}: master={want} replica={got}"))
        })
        .collect();
    assert!(
        mismatches.is_empty(),
        "after a kill -9 mid-resync the replica came back with the wrong \
         encodings: {mismatches:?}"
    );
    assert_eq!(integer_reply(&r, "SCARD si"), 3);
    assert_eq!(integer_reply(&r, "ZCARD z"), 2);
}
