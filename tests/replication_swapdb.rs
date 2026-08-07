//! #386 — streamed SWAPDB must be applied by replicas, exactly once.
//!
//! Two independent defects combine here:
//!
//! 1. **Replica no-op:** `replication/apply.rs::apply_local` had no SWAPDB
//!    intercept — the record fell through to generic dispatch, which
//!    hard-errors ("SWAPDB must be issued at the connection handler level"),
//!    and `warn_on_error` only logs. Every streamed SWAPDB silently no-op'd.
//!
//! 2. **Wire multiplicity:** a multi-shard master's SWAPDB used to reach the
//!    replication plane once per REMOTE leg (the SwapDb SPSC arm's
//!    `wal_append_and_fanout`) and never for the coordinator's local leg.
//!    Today's replica applies the merged wire as ONE stream, record by
//!    record — N-1 emissions would swap N-1 times, so the net effect
//!    depended on the master's shard-count parity (shards=3 → two swaps →
//!    net NO-OP). The wire contract is now: exactly ONE SWAPDB record per
//!    client SWAPDB (emitted by the coordinator after its durability gate +
//!    local swap); remote legs keep their AOF/WAL writes (per-shard
//!    recovery needs them) but stay OFF the replication plane.
//!
//! The multi-shard scenarios run at shards=3 AND shards=4 deliberately:
//! shards=4 (3 remote legs, odd) would accidentally pass under a
//! replica-apply-only fix, while shards=3 (2 remote legs, even) nets to
//! no-op and exposes the multiplicity defect.
//!
//! (When v0.9's #406 lands per-shard demuxed multi-shard replicas, this
//! contract must flip to per-shard emission + per-stream apply — see #386.)

mod common;

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

fn moon_bin() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("MOON_BIN") {
        return std::path::PathBuf::from(p);
    }
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
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
        "--databases",
        "4",
    ];
    full.extend_from_slice(extra);
    Command::new(moon_bin())
        .args(&full)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to start moon (set MOON_BIN to a built binary)")
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

fn read_one_reply<R: BufRead>(reader: &mut R) -> String {
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => return String::new(),
            Ok(_) => {
                let trimmed = line.trim_end_matches("\r\n").trim_end_matches('\n');
                if trimmed.starts_with('+') || trimmed.starts_with('-') || trimmed.starts_with(':')
                {
                    return trimmed.to_string();
                }
                if let Some(rest) = trimmed.strip_prefix('$') {
                    let len: i64 = rest.trim().parse().unwrap_or(-1);
                    if len < 0 {
                        return String::new(); // nil
                    }
                    let mut buf = vec![0u8; (len as usize) + 2];
                    let mut out = String::new();
                    if reader.read_exact(&mut buf).is_ok() {
                        out.push_str(&String::from_utf8_lossy(&buf[..len as usize]));
                    }
                    return out;
                }
                // array/other headers not needed here
            }
        }
    }
}

/// One connection, several commands in order — SELECT context persists,
/// which per-command connections cannot give us.
fn session_cmds(addr: &str, cmds: &[&str]) -> Vec<String> {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut replies = Vec::with_capacity(cmds.len());
    for cmd in cmds {
        stream
            .write_all(format!("{cmd}\r\n").as_bytes())
            .expect("write");
        stream.flush().ok();
        let mut reader = BufReader::new(&stream);
        replies.push(read_one_reply(&mut reader));
    }
    replies
}

fn send_cmd(addr: &str, cmd: &str) -> String {
    session_cmds(addr, &[cmd]).pop().unwrap_or_default()
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
        wait_until(Duration::from_secs(15), || send_cmd(addr, "PING")
            .starts_with("+PONG")),
        "server at {addr} did not become ready"
    );
}

fn await_link_up(replica_addr: &str) {
    assert!(
        wait_until(Duration::from_secs(15), || send_cmd(
            replica_addr,
            "INFO replication"
        )
        .contains("master_link_status:up")),
        "replica {replica_addr} link did not come up"
    );
}

/// GET `key` in logical db `db` on `addr` via one session.
fn get_in_db(addr: &str, db: usize, key: &str) -> String {
    session_cmds(addr, &[&format!("SELECT {db}"), &format!("GET {key}")])
        .pop()
        .unwrap_or_default()
}

/// Master (N shards) + replica (1 shard): one client SWAPDB must produce
/// exactly one logical swap on the replica.
fn run_swapdb_replication(master_shards: usize) {
    let mdir = tempfile::tempdir().expect("mdir");
    let rdir = tempfile::tempdir().expect("rdir");
    let mut guard = Guard(vec![]);
    let master_port = spawn_into(
        &mut guard,
        mdir.path().to_str().unwrap(),
        master_shards,
        &["--appendonly", "no"],
    );
    let replica_port = spawn_into(
        &mut guard,
        rdir.path().to_str().unwrap(),
        1,
        &["--appendonly", "no"],
    );
    let m = format!("127.0.0.1:{master_port}");
    let r = format!("127.0.0.1:{replica_port}");
    await_ready(&m);
    await_ready(&r);

    assert!(send_cmd(&r, &format!("REPLICAOF 127.0.0.1 {master_port}")).starts_with("+OK"));
    await_link_up(&r);

    // Enough keys in db0 that every master shard owns at least one, plus a
    // db1 sentinel — the swap must move ALL of them, including keys owned by
    // the coordinator shard (the leg that used to skip the repl plane).
    let mut db0_cmds: Vec<String> = vec!["SELECT 0".into()];
    for i in 0..16 {
        db0_cmds.push(format!("SET swap:key:{i} before-{i}"));
    }
    let refs: Vec<&str> = db0_cmds.iter().map(String::as_str).collect();
    for reply in session_cmds(&m, &refs).into_iter().skip(1) {
        assert!(reply.starts_with("+OK"), "master SET failed: {reply}");
    }
    let one_replies = session_cmds(&m, &["SELECT 1", "SET swap:one only-in-db1"]);
    assert!(
        one_replies[1].starts_with("+OK"),
        "db1 SET: {one_replies:?}"
    );

    // All writes visible on the replica before the swap.
    assert!(
        wait_until(Duration::from_secs(10), || {
            get_in_db(&r, 0, "swap:key:15") == "before-15"
                && get_in_db(&r, 1, "swap:one") == "only-in-db1"
        }),
        "replica did not catch up pre-swap (db0 k15={:?}, db1 one={:?})",
        get_in_db(&r, 0, "swap:key:15"),
        get_in_db(&r, 1, "swap:one"),
    );

    // The operation under test.
    let swap_reply = send_cmd(&m, "SWAPDB 0 1");
    assert!(
        swap_reply.starts_with("+OK"),
        "[shards={master_shards}] SWAPDB on master failed: {swap_reply}"
    );

    // Master sanity: db0 now holds only the db1 sentinel; db1 holds the 16 keys.
    assert_eq!(get_in_db(&m, 0, "swap:one"), "only-in-db1");
    assert_eq!(get_in_db(&m, 1, "swap:key:0"), "before-0");
    assert_eq!(
        get_in_db(&m, 0, "swap:key:0"),
        "",
        "master db0 must not keep swapped key"
    );

    // THE #386 ASSERTION: the replica must converge to the swapped state —
    // every db0 key (whatever master shard owned it) now answers from db1,
    // and the db1 sentinel answers from db0. A no-op (defect 1, or even
    // emission-count under defect 2) leaves keys in their old dbs.
    assert!(
        wait_until(Duration::from_secs(10), || {
            get_in_db(&r, 1, "swap:key:0") == "before-0"
                && get_in_db(&r, 1, "swap:key:15") == "before-15"
                && get_in_db(&r, 0, "swap:one") == "only-in-db1"
                && get_in_db(&r, 0, "swap:key:0").is_empty()
        }),
        "[shards={master_shards}] replica did not apply SWAPDB exactly once: \
         db1 k0={:?} db1 k15={:?} db0 one={:?} db0 k0={:?}",
        get_in_db(&r, 1, "swap:key:0"),
        get_in_db(&r, 1, "swap:key:15"),
        get_in_db(&r, 0, "swap:one"),
        get_in_db(&r, 0, "swap:key:0"),
    );

    // Writes AFTER the swap still replicate into the right (post-swap) dbs.
    let post = session_cmds(&m, &["SELECT 0", "SET swap:after post-swap-value"]);
    assert!(post[1].starts_with("+OK"), "post-swap SET: {post:?}");
    assert!(
        wait_until(Duration::from_secs(10), || get_in_db(&r, 0, "swap:after")
            == "post-swap-value"),
        "[shards={master_shards}] post-swap write did not replicate into db0"
    );
}

// `#[ignore]`d like the other replication suites (`replication_streaming.rs`,
// `replication_hardening.rs`): they spawn two real `moon` processes, and
// PSYNC-as-master is monoio-only ("-ERR PSYNC requires runtime-monoio on the
// master"), so they can never pass under the CI tokio job. Run explicitly:
//   MOON_BIN=$PWD/target/release/moon cargo test --release \
//     --test replication_swapdb -- --include-ignored

#[test]
#[ignore] // Requires monoio release binary + real replication link; run explicitly.
fn swapdb_replicates_single_shard_master() {
    run_swapdb_replication(1);
}

/// shards=3: TWO remote legs — under the old per-remote-leg emission a
/// replica-apply-only fix nets to NO-OP (even swap count). The multiplicity
/// half of the defect.
#[test]
#[ignore] // Requires monoio release binary + real replication link; run explicitly.
fn swapdb_replicates_three_shard_master() {
    run_swapdb_replication(3);
}

/// shards=4: THREE remote legs — odd count would accidentally pass an
/// apply-only fix; this leg pins the coordinator-local-leg + exactly-once
/// contract instead (with the fix, exactly one record regardless of shards).
#[test]
#[ignore] // Requires monoio release binary + real replication link; run explicitly.
fn swapdb_replicates_four_shard_master() {
    run_swapdb_replication(4);
}
