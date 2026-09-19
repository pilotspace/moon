//! moon#1034: `REPLICAOF <hostname> <port>` must resolve the host as redis
//! does, and must never panic the replica task.
//!
//! The monoio `run_replica_task` parsed `"{host}:{port}"` as a `SocketAddr`
//! with `.expect("invalid master address")`. `SocketAddr::from_str` accepts IP
//! literals only, so `REPLICAOF localhost 6379`, or any DNS name, panicked
//! inside a task spawned on the shard thread, after the command had already
//! acked `+OK` and flipped the node to a read-only replica.
//!
//! Contract pinned here (all against real server processes, `--shards 1`):
//!   * `REPLICAOF localhost <port>` syncs a key from a live master. `localhost`
//!     resolves to `::1` and `127.0.0.1` on common hosts while moon binds
//!     `127.0.0.1`, so this also exercises trying every resolved address.
//!   * An unresolvable host keeps the node alive and answering, reports
//!     `role:slave` + `master_link_status:down`, and `REPLICAOF NO ONE`
//!     still promotes it back to a writable master.
//!   * Re-pointing from an unresolvable host to `localhost` syncs: the stuck
//!     task is superseded, not left in charge.
//!
//! Replication integration tests are `#[ignore]`d in this repo; run with
//! `MOON_BIN=/path/to/moon cargo test --test replicaof_hostname_1034 -- --ignored`.
//! PSYNC-as-master is monoio-only: to exercise a tokio replica, also set
//! `MOON_MASTER_BIN` to a monoio build.

mod common;

use std::process::{Child, Command};
use std::time::{Duration, Instant};

use common::Conn;

/// `.invalid` is reserved by RFC 6761 and never resolves.
const UNRESOLVABLE: &str = "moon-1034-no-such-master.invalid";

fn spawn_moon(bin: &std::path::Path, dir: &std::path::Path, port: u16) -> Child {
    std::fs::create_dir_all(dir).expect("create --dir");
    Command::new(bin)
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--dir",
            dir.to_str().expect("utf-8 dir"),
            "--appendonly",
            "no",
            "--disk-free-min-pct",
            "0",
        ])
        .env("RUST_LOG", "moon=info")
        .stdout(common::server_stderr(dir))
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon (set MOON_BIN to a built binary)")
}

/// The node under test (`MOON_BIN`, via `find_moon_binary`).
fn start(prefix: &str) -> (common::ServerGuard, u16, std::path::PathBuf) {
    let dir = common::unique_test_dir(prefix);
    let bin = common::find_moon_binary();
    let (guard, port) = common::spawn_listening_guarded(|p| spawn_moon(&bin, &dir, p));
    (guard, port, dir)
}

/// The master. PSYNC-as-master is monoio-only, so a tokio replica
/// (`MOON_BIN`) is exercised against a monoio master named by
/// `MOON_MASTER_BIN`. Defaults to the node under test.
fn start_master(prefix: &str) -> (common::ServerGuard, u16, std::path::PathBuf) {
    let dir = common::unique_test_dir(prefix);
    let bin = std::env::var_os("MOON_MASTER_BIN")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(common::find_moon_binary);
    let (guard, port) = common::spawn_listening_guarded(|p| spawn_moon(&bin, &dir, p));
    (guard, port, dir)
}

/// One `field:value` line of `INFO replication`.
fn info_field(port: u16, field: &str) -> String {
    let info = Conn::open(port).send(&["INFO", "replication"]);
    info.lines()
        .find(|l| l.starts_with(field))
        .map(|l| l.trim_end().to_string())
        .unwrap_or_else(|| format!("<no {field} line in {info:?}>"))
}

/// Server log, for failure messages: a panicked task shows up here.
fn server_log(dir: &std::path::Path) -> String {
    let log = std::fs::read_to_string(dir.join("server.err")).unwrap_or_default();
    let tail: Vec<&str> = log.lines().rev().take(25).collect();
    tail.into_iter().rev().collect::<Vec<_>>().join("\n")
}

fn assert_alive(guard: &mut common::ServerGuard, dir: &std::path::Path, what: &str) {
    if let Ok(Some(status)) = guard.as_mut().try_wait() {
        panic!(
            "{what}: the replica process exited ({status}); log tail:\n{}",
            server_log(dir)
        );
    }
    assert!(
        !std::fs::read_to_string(dir.join("server.err"))
            .unwrap_or_default()
            .contains("panicked"),
        "{what}: a task panicked; log tail:\n{}",
        server_log(dir)
    );
}

/// Poll `GET key` on `port` until it answers `want` or `deadline` passes.
fn wait_for_value(port: u16, key: &str, want: &str, deadline: Duration) -> Option<String> {
    let expect = format!("${}\r\n{want}\r\n", want.len());
    let start = Instant::now();
    let mut last = None;
    while start.elapsed() < deadline {
        let got = Conn::open(port).send(&["GET", key]);
        if got == expect {
            return None;
        }
        last = Some(got);
        std::thread::sleep(Duration::from_millis(100));
    }
    last
}

#[test]
#[ignore = "replication integration test (needs a monoio master): run with -- --ignored"]
fn replicaof_localhost_syncs_a_key_and_does_not_panic() {
    let (_master, master_port, _mdir) = start_master("moon-1034-master");
    assert_eq!(
        Conn::open(master_port).send(&["SET", "k1034", "fromMaster"]),
        "+OK\r\n"
    );

    let (mut replica, port, dir) = start("moon-1034-replica");
    let mp = master_port.to_string();
    assert_eq!(
        Conn::open(port).send(&["REPLICAOF", "localhost", &mp]),
        "+OK\r\n"
    );

    let miss = wait_for_value(port, "k1034", "fromMaster", Duration::from_secs(15));
    assert_alive(&mut replica, &dir, "REPLICAOF localhost");
    assert!(
        miss.is_none(),
        "REPLICAOF localhost {mp} never synced k1034 (last GET {miss:?}); log tail:\n{}",
        server_log(&dir)
    );
    assert_eq!(
        info_field(port, "master_link_status:"),
        "master_link_status:up"
    );

    // A write after the link is up streams too, not just the snapshot.
    assert_eq!(
        Conn::open(master_port).send(&["SET", "k1034b", "streamed"]),
        "+OK\r\n"
    );
    let miss = wait_for_value(port, "k1034b", "streamed", Duration::from_secs(10));
    assert!(
        miss.is_none(),
        "streamed write never arrived (last GET {miss:?})"
    );
    assert_alive(&mut replica, &dir, "REPLICAOF localhost (streaming)");
}

#[test]
#[ignore = "replication integration test: run with -- --ignored"]
fn replicaof_unresolvable_host_retries_with_link_down_and_never_panics() {
    let (mut node, port, dir) = start("moon-1034-unresolvable");
    assert_eq!(
        Conn::open(port).send(&["REPLICAOF", UNRESOLVABLE, "6379"]),
        "+OK\r\n",
        "redis accepts any host at REPLICAOF and resolves it in the connect loop"
    );

    // Long enough for several resolve attempts with backoff.
    std::thread::sleep(Duration::from_millis(2500));
    assert_alive(&mut node, &dir, "REPLICAOF <unresolvable>");
    assert_eq!(info_field(port, "role:"), "role:slave");
    assert_eq!(
        info_field(port, "master_link_status:"),
        "master_link_status:down"
    );
    assert_eq!(Conn::open(port).send(&["PING"]), "+PONG\r\n");

    // Promotion still works while the task is retrying.
    assert_eq!(
        Conn::open(port).send(&["REPLICAOF", "NO", "ONE"]),
        "+OK\r\n"
    );
    assert_eq!(info_field(port, "role:"), "role:master");
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["SET", "after", "v"]), "+OK\r\n");
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(
        info_field(port, "role:"),
        "role:master",
        "a superseded resolve loop must not flip the node back to a replica"
    );
    assert_alive(&mut node, &dir, "REPLICAOF NO ONE after <unresolvable>");
}

#[test]
#[ignore = "replication integration test (needs a monoio master): run with -- --ignored"]
fn replicaof_retarget_from_unresolvable_to_localhost_syncs() {
    let (_master, master_port, _mdir) = start_master("moon-1034-retarget-master");
    assert_eq!(
        Conn::open(master_port).send(&["SET", "kr", "v-retarget"]),
        "+OK\r\n"
    );

    let (mut replica, port, dir) = start("moon-1034-retarget-replica");
    assert_eq!(
        Conn::open(port).send(&["REPLICAOF", UNRESOLVABLE, "6379"]),
        "+OK\r\n"
    );
    std::thread::sleep(Duration::from_millis(1200));
    let mp = master_port.to_string();
    assert_eq!(
        Conn::open(port).send(&["REPLICAOF", "localhost", &mp]),
        "+OK\r\n"
    );
    let miss = wait_for_value(port, "kr", "v-retarget", Duration::from_secs(15));
    assert_alive(&mut replica, &dir, "re-point to localhost");
    assert!(
        miss.is_none(),
        "re-pointing from an unresolvable host to localhost never synced (last GET {miss:?}); \
         log tail:\n{}",
        server_log(&dir)
    );
    assert_eq!(
        info_field(port, "master_host:"),
        "master_host:localhost",
        "INFO reports the host as the operator typed it"
    );
}
