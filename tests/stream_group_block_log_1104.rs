//! moon#1104 — a blocking `XREADGROUP` that delivers entries is a write, and
//! it must survive `kill -9` exactly like the same read without `BLOCK`.
//!
//! A consumer-group read moves every entry it delivers into the group's
//! pending list (PEL) and advances the group's last-delivered id. Without
//! `BLOCK` it goes through the ordinary write exit and is logged. With
//! `BLOCK` it goes through the blocking intercept — served at once from the
//! client's own shard, or later by the wake on the stream's owner — and
//! nothing logged it: after a restart the PEL was empty, the cursor was back,
//! and the next `>` reader was handed the same entries a second time.
//!
//! redis-server 8.6.1 on the same sequence (`appendfsync always`, restart on
//! the same dir) keeps the PEL, the cursor and the NOACK consumer: its AOF
//! holds one `XCLAIM ... FORCE JUSTID LASTID` per delivered entry and an
//! `XGROUP SETID` (plus `XGROUP CREATECONSUMER` for a NOACK read that created
//! its consumer). Moon now logs the same records, on the shard that served
//! the read, in the read's own synchronous stretch.
//!
//! Every row reads ONE stream on ONE waiter connection. At `--shards 4` each
//! row is repeated with its stream on each shard, so at least three of the
//! four owners are not the waiter's shard (the owner's wake serves them) and
//! one is (the immediate read on the waiter's own shard serves it).
//!
//! ```text
//! MOON_BIN=/path/to/moon cargo test --test stream_group_block_log_1104
//! ```

mod common;

use std::process::Child;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use common::{Conn, spawn_listening_guarded, unique_test_dir};
use moon::shard::dispatch::key_to_shard;

fn spawn(port: u16, dir: &std::path::Path, shards: usize) -> Child {
    std::process::Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--shards",
            &shards.to_string(),
            "--appendonly",
            "yes",
            "--appendfsync",
            "always",
            // Below the ~5%-free guard every write answers `MOONERR diskfull`.
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon (set MOON_BIN to a built binary)")
}

fn key_on(prefix: &str, shard: usize, shards: usize) -> String {
    (0..10_000)
        .map(|i| format!("{prefix}{i}"))
        .find(|k| key_to_shard(k.as_bytes(), shards) == shard)
        .expect("some key hashes to every shard")
}

fn blocked_clients(port: u16) -> u32 {
    let mut c = Conn::open(port);
    c.send(&["INFO", "clients"])
        .lines()
        .find_map(|l| l.trim().strip_prefix("blocked_clients:")?.parse().ok())
        .unwrap_or(0)
}

/// Wait until the server reports a parked client, then a little longer so a
/// registration on a REMOTE owner has landed too.
fn await_blocked(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while blocked_clients(port) < 1 {
        assert!(Instant::now() < deadline, "the reader never parked");
        std::thread::sleep(Duration::from_millis(5));
    }
    std::thread::sleep(Duration::from_millis(50));
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    /// The entry is there before the read: served at once.
    Immediate,
    /// The read parks and an `XADD` serves it.
    Parked,
    /// As `Parked`, with `NOACK` and a consumer the read creates.
    ParkedNoack,
}

struct Row {
    label: String,
    key: String,
    mode: Mode,
}

fn rows(shards: usize) -> Vec<Row> {
    let mut out = Vec::new();
    for s in 0..shards {
        for mode in [Mode::Immediate, Mode::Parked, Mode::ParkedNoack] {
            let key = key_on(&format!("k1104:{mode:?}:"), s, shards);
            out.push(Row {
                label: format!("{mode:?} owner={s}"),
                key,
                mode,
            });
        }
    }
    out
}

fn read_cmd(row: &Row) -> Vec<String> {
    let mut v: Vec<String> = ["XREADGROUP", "GROUP", "g", "c"]
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    if matches!(row.mode, Mode::ParkedNoack) {
        v.push("NOACK".into());
    }
    for s in ["BLOCK", "5000", "STREAMS", &row.key, ">"] {
        v.push(s.to_string());
    }
    v
}

fn refs(v: &[String]) -> Vec<&str> {
    v.iter().map(String::as_str).collect()
}

/// Serve every row's read on ONE waiter connection, in order; a parked row is
/// fed its entry by an `XADD` from another connection once it has parked.
fn serve_all(port: u16, rows: &[Row]) {
    let mut c = Conn::open(port);
    for row in rows {
        assert!(
            c.send(&["XGROUP", "CREATE", &row.key, "g", "$", "MKSTREAM"])
                .starts_with("+OK")
        );
        if matches!(row.mode, Mode::Immediate) {
            assert!(c.send(&["XADD", &row.key, "1-1", "f", "v"]).contains("1-1"));
        }
    }
    let reads: Vec<(Vec<String>, bool)> = rows
        .iter()
        .map(|r| (read_cmd(r), matches!(r.mode, Mode::Immediate)))
        .collect();
    let (go_tx, go_rx) = mpsc::channel::<bool>();
    let waiter = std::thread::spawn(move || {
        let mut w = Conn::open(port);
        let mut replies = Vec::new();
        for (cmd, immediate) in &reads {
            go_tx.send(*immediate).expect("main thread gone");
            replies.push(w.send(&refs(cmd)));
        }
        replies
    });
    for row in rows {
        let immediate = go_rx.recv().expect("waiter thread died");
        if !immediate {
            await_blocked(port);
            assert!(c.send(&["XADD", &row.key, "1-1", "f", "v"]).contains("1-1"));
        }
    }
    let replies = waiter.join().expect("waiter thread");
    for (row, reply) in rows.iter().zip(&replies) {
        assert!(
            reply.contains("1-1") && !reply.starts_with('-'),
            "{}: the read did not deliver 1-1 ({reply:?}) — the row would prove nothing",
            row.label
        );
    }
}

/// The group state a row's read left: the pending summary, the group's
/// cursor/consumer count, and the consumer that owns the entry with its
/// delivery count.
fn probe(c: &mut Conn, row: &Row) -> Vec<String> {
    let mut groups = c.send(&["XINFO", "GROUPS", &row.key]);
    // `lag` is the one field of XINFO GROUPS that is not a function of the
    // group alone; the rest is compared verbatim.
    if let Some(i) = groups.find("lag\r\n") {
        groups.truncate(i);
    }
    // Each detail entry is `[id, consumer, idle, delivery-count]`; the idle
    // time moves while we look, so it is dropped and the count kept.
    let raw = c.send(&["XPENDING", &row.key, "g", "-", "+", "10"]);
    let mut ints_in_entry = 0;
    let mut kept = Vec::new();
    for line in raw.split("\r\n") {
        if line == "*4" {
            ints_in_entry = 0;
        }
        if line.starts_with(':') {
            ints_in_entry += 1;
            if ints_in_entry == 1 {
                continue;
            }
        }
        kept.push(line);
    }
    let detail = kept.join("|");
    vec![
        c.send(&["XPENDING", &row.key, "g"]),
        groups,
        detail,
        c.send(&["XINFO", "CONSUMERS", &row.key, "g"])
            .split("\r\n")
            .filter(|l| !l.starts_with(':'))
            .collect::<Vec<_>>()
            .join("|"),
    ]
}

fn expected_pending(row: &Row) -> String {
    match row.mode {
        Mode::ParkedNoack => "*4\r\n:0\r\n$-1\r\n$-1\r\n*-1\r\n".to_string(),
        _ => "*4\r\n:1\r\n$3\r\n1-1\r\n$3\r\n1-1\r\n*1\r\n*2\r\n$1\r\nc\r\n$1\r\n1\r\n".to_string(),
    }
}

fn run(shards: usize) {
    let dir = unique_test_dir(&format!("stream_group_block_1104_s{shards}"));
    let (mut guard, port) = spawn_listening_guarded(|p| spawn(p, &dir, shards));
    let rows = rows(shards);
    serve_all(port, &rows);

    let mut c = Conn::open(port);
    let before: Vec<Vec<String>> = rows.iter().map(|r| probe(&mut c, r)).collect();
    for (row, got) in rows.iter().zip(&before) {
        let want = expected_pending(row);
        assert!(
            got[0] == want
                || matches!(row.mode, Mode::ParkedNoack) && got[0].starts_with("*4\r\n:0"),
            "{}: XPENDING before the kill: {:?}",
            row.label,
            got[0]
        );
    }
    assert!(c.send(&["SET", "k1104:control", "1"]).starts_with("+OK"));
    drop(c);
    guard.kill_now();
    common::wait_for_port_down(port);

    let (_g2, port2) = spawn_listening_guarded(|p| spawn(p, &dir, shards));
    let mut c = Conn::open(port2);
    assert_eq!(
        c.send(&["GET", "k1104:control"]),
        "$1\r\n1\r\n",
        "the control write did not survive the restart — the AOF itself is broken"
    );
    let mut changed = Vec::new();
    for (row, was) in rows.iter().zip(&before) {
        let now = probe(&mut c, row);
        if &now != was {
            changed.push(format!(
                "{}: before={:?}\n      after ={:?}",
                row.label, was, now
            ));
        }
        // And the user-visible consequence: the entry is not handed out again.
        let again = c.send(&["XREADGROUP", "GROUP", "g", "c2", "STREAMS", &row.key, ">"]);
        if again != "*-1\r\n" {
            changed.push(format!(
                "{}: a second `>` read after the restart was handed {again:?}",
                row.label
            ));
        }
    }
    assert!(
        changed.is_empty(),
        "{} of {} rows lost their group read across kill -9 + restart:\n  {}",
        rows.iter()
            .filter(|r| changed
                .iter()
                .any(|c| c.starts_with(&format!("{}:", r.label))))
            .count(),
        rows.len(),
        changed.join("\n  ")
    );
}

#[test]
fn blocking_group_reads_survive_kill9_one_shard() {
    run(1);
}

#[test]
fn blocking_group_reads_survive_kill9_four_shards() {
    run(4);
}

/// The same reads against a live replica: the records reach the replication
/// stream from the shard that served each read, and a replica applying them
/// holds the master's PEL, cursor and consumers. `#[ignore]`d like every
/// replication suite (master-side replication exists on the monoio runtime
/// only):
///
/// ```text
/// MOON_BIN=... cargo test --test stream_group_block_log_1104 -- --include-ignored
/// ```
#[test]
#[ignore = "replication: monoio master, run with --include-ignored"]
fn a_replica_holds_the_masters_group_state() {
    fn spawn_plain(port: u16, dir: &std::path::Path, shards: usize) -> Child {
        std::process::Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (set MOON_BIN to a built binary)")
    }

    for shards in [1, 4] {
        let mdir = unique_test_dir("stream_group_block_1104_master");
        let rdir = unique_test_dir("stream_group_block_1104_replica");
        let (_m, mport) = spawn_listening_guarded(|p| spawn_plain(p, &mdir, shards));
        let (_r, rport) = spawn_listening_guarded(|p| spawn_plain(p, &rdir, 1));
        let mut r = Conn::open(rport);
        assert!(
            r.send(&["REPLICAOF", "127.0.0.1", &mport.to_string()])
                .starts_with("+OK")
        );
        let deadline = Instant::now() + Duration::from_secs(20);
        while !r
            .send(&["INFO", "replication"])
            .contains("master_link_status:up")
        {
            assert!(Instant::now() < deadline, "replica link never came up");
            std::thread::sleep(Duration::from_millis(100));
        }

        let rows = rows(shards);
        serve_all(mport, &rows);
        let mut m = Conn::open(mport);
        let on_master: Vec<Vec<String>> = rows.iter().map(|row| probe(&mut m, row)).collect();

        // Each shard feeds the replica its own stream, so poll until it
        // agrees; a replica that missed a record never converges.
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut on_replica: Vec<Vec<String>> = rows.iter().map(|row| probe(&mut r, row)).collect();
        while on_replica != on_master && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(100));
            on_replica = rows.iter().map(|row| probe(&mut r, row)).collect();
        }
        let diverged: Vec<String> = rows
            .iter()
            .zip(on_master.iter().zip(&on_replica))
            .filter(|(_, (m, r))| m != r)
            .map(|(row, (m, r))| format!("{}: master={m:?}\n      replica={r:?}", row.label))
            .collect();
        assert!(
            diverged.is_empty(),
            "shards={shards}: the replica does not hold the master's group state:\n  {}",
            diverged.join("\n  ")
        );
    }
}
