//! moon#677 — a streamed `FLUSHALL` must empty EVERY database on the replica.
//!
//! `replication/apply.rs` handled `FLUSHDB` and `FLUSHALL` with one arm that
//! dispatched the record against the single database it was attributed to.
//! For `FLUSHDB` that is right. For `FLUSHALL` it left the master empty and
//! the replica holding every database the client never selected — divergence
//! that stays invisible until somebody `SELECT`s one of them, which is why no
//! existing test saw it (they all live in db0, where the two behaviours are
//! identical).
//!
//! The counter-test matters as much as the fix: a replica that "fixed" this
//! by clearing every database on `FLUSHDB` too would pass the first test and
//! destroy fifteen databases per `FLUSHDB`.
//!
//! `#[ignore]`d like every other replication suite here: these spawn two real
//! `moon` processes and PSYNC-as-master is monoio-only, so they can never
//! pass under the CI tokio job. Run explicitly:
//!   MOON_BIN=$PWD/target/release/moon cargo test --release \
//!     --test replication_flushall -- --include-ignored

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

/// `start_moon` passes `--databases 4`, so these are every database the
/// servers have. Using all of them is deliberate: the bug is "only the
/// selected one survives the flush", and a probe set that stopped at db1
/// could not tell a partial fix from a complete one.
const PROBE_DBS: [usize; 4] = [0, 1, 2, 3];

fn seed_all_dbs(addr: &str, tag: &str) {
    for db in PROBE_DBS {
        let replies = session_cmds(
            addr,
            &[
                &format!("SELECT {db}"),
                &format!("SET flush:k{db} {tag}-{db}"),
            ],
        );
        assert!(
            replies[1].starts_with("+OK"),
            "seeding db{db} on {addr} failed: {replies:?}"
        );
    }
}

/// Every probe database's key as `addr` currently answers it, for assertion
/// messages. Empty answers are kept in the rendering — "db1=\"\"" is the
/// information a failure needs.
fn db_snapshot(addr: &str) -> Vec<String> {
    PROBE_DBS
        .iter()
        .map(|db| format!("db{db}={:?}", get_in_db(addr, *db, &format!("flush:k{db}"))))
        .collect()
}

/// Master (N shards) + replica (1 shard): one client FLUSHALL must leave the
/// replica with no keys in ANY database.
fn run_flushall_replication(master_shards: usize) {
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

    seed_all_dbs(&m, "before");
    assert!(
        wait_until(Duration::from_secs(10), || {
            PROBE_DBS
                .iter()
                .all(|db| get_in_db(&r, *db, &format!("flush:k{db}")) == format!("before-{db}"))
        }),
        "[shards={master_shards}] replica did not catch up pre-flush: {:?}",
        db_snapshot(&r)
    );

    // The operation under test, issued from db0.
    let reply = send_cmd(&m, "FLUSHALL");
    assert!(
        reply.starts_with("+OK"),
        "[shards={master_shards}] FLUSHALL on master failed: {reply}"
    );

    // Master sanity first: if the master itself kept databases, the replica
    // assertion below would be measuring the wrong defect.
    for db in PROBE_DBS {
        assert_eq!(
            get_in_db(&m, db, &format!("flush:k{db}")),
            "",
            "[shards={master_shards}] master kept db{db} after FLUSHALL"
        );
    }

    assert!(
        wait_until(Duration::from_secs(10), || {
            PROBE_DBS
                .iter()
                .all(|db| get_in_db(&r, *db, &format!("flush:k{db}")).is_empty())
        }),
        "[shards={master_shards}] replica kept databases a FLUSHALL emptied on the \
         master: {:?}",
        db_snapshot(&r)
    );

    // Writes after the flush still replicate, into the database they were
    // written to — a flush that broke the stream would also pass the
    // emptiness check above.
    let post = session_cmds(&m, &["SELECT 2", "SET flush:after post-flush"]);
    assert!(post[1].starts_with("+OK"), "post-flush SET: {post:?}");
    assert!(
        wait_until(Duration::from_secs(10), || get_in_db(&r, 2, "flush:after")
            == "post-flush"),
        "[shards={master_shards}] post-flush write did not replicate into db2"
    );
}

#[test]
#[ignore] // Requires monoio release binary + real replication link; run explicitly.
fn flushall_replicates_to_every_database_single_shard_master() {
    run_flushall_replication(1);
}

#[test]
#[ignore] // Requires monoio release binary + real replication link; run explicitly.
fn flushall_replicates_to_every_database_four_shard_master() {
    run_flushall_replication(4);
}

/// The counter-test. A `FLUSHDB` on the master must clear exactly one
/// database on the replica.
#[test]
#[ignore] // Requires monoio release binary + real replication link; run explicitly.
fn flushdb_still_replicates_as_a_single_database_flush() {
    let mdir = tempfile::tempdir().expect("mdir");
    let rdir = tempfile::tempdir().expect("rdir");
    let mut guard = Guard(vec![]);
    let master_port = spawn_into(
        &mut guard,
        mdir.path().to_str().unwrap(),
        1,
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

    seed_all_dbs(&m, "before");
    assert!(
        wait_until(Duration::from_secs(10), || {
            PROBE_DBS
                .iter()
                .all(|db| get_in_db(&r, *db, &format!("flush:k{db}")) == format!("before-{db}"))
        }),
        "replica did not catch up pre-flush: {:?}",
        db_snapshot(&r)
    );

    let replies = session_cmds(&m, &["SELECT 2", "FLUSHDB"]);
    assert!(replies[1].starts_with("+OK"), "FLUSHDB: {replies:?}");

    assert!(
        wait_until(Duration::from_secs(10), || get_in_db(&r, 2, "flush:k2")
            .is_empty()),
        "replica did not apply FLUSHDB to db2"
    );
    for db in PROBE_DBS.iter().filter(|d| **d != 2) {
        assert_eq!(
            get_in_db(&r, *db, &format!("flush:k{db}")),
            format!("before-{db}"),
            "FLUSHDB in db2 wrongly cleared db{db} on the replica"
        );
    }
}
