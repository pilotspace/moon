//! moon#1056 / moon#1097 — a served blocking pop is logged by the shard that
//! POPPED it, at the moment it popped, in the same order as that shard's
//! other writes.
//!
//! ## The two defects
//!
//! Since moon#827 a blocking pop propagates a synthesised record (`BLPOP` ->
//! `LPOP`, ...). It was written by the WAITER's connection task after the
//! reply had reached it:
//!
//! * **Wrong file.** At `--shards > 1` a waiter parks on the shard that owns
//!   its key, which is usually not its own. The record went to the waiter's
//!   own shard AOF; per-shard replay routes a record by the file it sits in,
//!   so it was dropped, and the element the client had already received came
//!   back after `kill -9`.
//! * **Wrong place in the order** (`--shards 1` too). The record was written
//!   after the owner had moved on to later writes. A later write to the same
//!   key could be logged BEFORE the pop that preceded it, and replay applied
//!   them the other way round. The replication stream carried the same
//!   inversion to every replica.
//!
//! ## What is asserted
//!
//! The user-visible fact, as in `blocking_pop_propagation_827.rs`: after a
//! restart (or on a replica) the keyspace is the one the clients were told
//! about. Every row pushes more than it pops, and every ordering row is shaped
//! so that the two orders give DIFFERENT states, so a wrong order cannot pass
//! by accident.
//!
//! * [`cross_shard_pops_survive_kill9`] — every blocking pop, each parked on a
//!   key owned by each of the four shards in turn, waiter on one fixed
//!   connection (so at least three of four owners are not its shard);
//!   `appendfsync always`, `kill -9`, restart on the same `--dir`.
//!   `BLMOVE`/`BRPOPLPUSH` rows check the source AND the destination.
//! * [`a_pop_is_logged_before_the_writes_that_follow_it`] — `--shards 1`: the
//!   write that wakes the waiter and a later, order-sensitive write to the
//!   same key arrive in ONE pipelined batch, so the waiter cannot run between
//!   them. Covers a plain write, `EXEC`, and `MOVE` as the waking write.
//! * [`replica_receives_each_pop_once_in_owner_order`] — the same shape
//!   against a live replica (`#[ignore]`d like every replication suite;
//!   master-side replication exists on the monoio runtime only).
//!
//! ```text
//! MOON_BIN=/path/to/moon cargo test --test blocking_pop_owner_log_1056 -- --include-ignored
//! ```

mod common;

use std::process::Child;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use common::{Conn, spawn_listening_guarded, unique_test_dir};
use moon::shard::dispatch::key_to_shard;

const SHARDS: usize = 4;

fn spawn(port: u16, dir: &std::path::Path, shards: usize, fsync: &str) -> Child {
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
            fsync,
            // Below the ~5%-free guard every write answers `MOONERR diskfull`
            // and nothing here would run.
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon (set MOON_BIN to a built binary)")
}

/// The first `{prefix}{i}` owned by `shard`, found with the server's own
/// `key_to_shard` rather than trusted to a literal.
fn key_on(prefix: &str, shard: usize, shards: usize) -> String {
    (0..10_000)
        .map(|i| format!("{prefix}{i}"))
        .find(|k| key_to_shard(k.as_bytes(), shards) == shard)
        .expect("some key hashes to every shard")
}

/// A `{tag}` owned by `shard`, so `{tag}:src` and `{tag}:dst` are co-located
/// there (a move across shards is refused, moon#570).
fn tag_on(shard: usize, shards: usize) -> String {
    (0..10_000)
        .map(|i| format!("{{t1056x{i}}}"))
        .find(|t| key_to_shard(t.as_bytes(), shards) == shard)
        .expect("some tag hashes to every shard")
}

fn blocked_clients(port: u16) -> u32 {
    let mut c = Conn::open(port);
    c.send(&["INFO", "clients"])
        .lines()
        .find_map(|l| l.trim().strip_prefix("blocked_clients:")?.parse().ok())
        .unwrap_or(0)
}

/// Wait until the server itself reports `n` parked clients, then a little
/// longer so a registration on a REMOTE owner has landed too. Either order is
/// a pop the owner serves, so a race here only changes WHICH owner-side path
/// runs (a wake, or the registration's immediate serve), never the result.
fn await_blocked(port: u16, n: u32) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while blocked_clients(port) < n {
        assert!(Instant::now() < deadline, "the waiter never parked");
        std::thread::sleep(Duration::from_millis(5));
    }
    std::thread::sleep(Duration::from_millis(50));
}

/// A reply that is an error or a null: the pop did not happen, and the row
/// would prove nothing.
fn is_miss(reply: &str) -> bool {
    reply.starts_with('-') || reply.starts_with("*-1") || reply.starts_with("$-1")
}

fn owned(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|s| (*s).to_string()).collect()
}

fn refs(v: &[String]) -> Vec<&str> {
    v.iter().map(String::as_str).collect()
}

/// One parked pop: `writes` wake it (sent as ONE pipelined batch), `probes`
/// read back what it left, and `expect` is what each probe must answer before
/// the kill.
struct Row {
    label: String,
    pop: Vec<String>,
    writes: Vec<Vec<String>>,
    probes: Vec<Vec<String>>,
    expect: Vec<String>,
}

/// RESP array of bulk strings, for `LRANGE`/`ZRANGE` expectations.
fn arr(items: &[&str]) -> String {
    let mut s = format!("*{}\r\n", items.len());
    for i in items {
        s.push_str(&format!("${}\r\n{i}\r\n", i.len()));
    }
    s
}

/// Park every row's pop on ONE waiter connection, in order, waking each with
/// its writes on a second connection. Returns nothing: every reply is
/// asserted to be a real pop here.
fn park_and_wake(port: u16, rows: &[Row]) {
    let pops: Vec<Vec<String>> = rows.iter().map(|r| r.pop.clone()).collect();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let waiter = std::thread::spawn(move || {
        let mut w = Conn::open(port);
        let mut replies = Vec::new();
        for pop in &pops {
            go_tx.send(()).expect("main thread gone");
            replies.push(w.send(&refs(pop)));
        }
        replies
    });
    let mut c = Conn::open(port);
    for row in rows {
        go_rx.recv().expect("waiter thread died");
        await_blocked(port, 1);
        let batch: Vec<Vec<&str>> = row.writes.iter().map(|w| refs(w)).collect();
        let batch_refs: Vec<&[&str]> = batch.iter().map(Vec::as_slice).collect();
        let _ = c.pipeline(&batch_refs);
    }
    let replies = waiter.join().expect("waiter thread");
    for (row, reply) in rows.iter().zip(&replies) {
        assert!(
            !is_miss(reply),
            "{}: the parked pop was not served ({reply:?}) — this row would prove nothing",
            row.label
        );
    }
}

/// The state every row's probes answer, in row order.
fn probe_all(c: &mut Conn, rows: &[Row]) -> Vec<String> {
    rows.iter()
        .flat_map(|r| r.probes.iter())
        .map(|p| c.send(&refs(p)))
        .collect()
}

/// Run `rows`, kill -9, restart on the same dir, and report every probe that
/// changed across the restart.
fn run_kill9(prefix: &str, shards: usize, fsync: &str, rows: &[Row]) {
    let dir = unique_test_dir(prefix);
    let (mut guard, port) = spawn_listening_guarded(|p| spawn(p, &dir, shards, fsync));

    park_and_wake(port, rows);

    let mut c = Conn::open(port);
    let before = probe_all(&mut c, rows);
    let expected: Vec<&String> = rows.iter().flat_map(|r| r.expect.iter()).collect();
    let labels: Vec<String> = rows
        .iter()
        .flat_map(|r| {
            r.probes
                .iter()
                .map(move |p| format!("{} {}", r.label, p.join(" ")))
        })
        .collect();
    for ((label, got), want) in labels.iter().zip(&before).zip(&expected) {
        assert_eq!(
            got, *want,
            "{label}: the pop did not take effect before the kill"
        );
    }

    // A control write that must survive: if the AOF itself were broken,
    // every row below would "pass" by losing nothing.
    assert!(c.send(&["SET", "k1056:control", "1"]).starts_with("+OK"));
    if fsync != "always" {
        std::thread::sleep(Duration::from_millis(1500));
    }
    drop(c);
    guard.kill_now();
    common::wait_for_port_down(port);

    let (_g2, port2) = spawn_listening_guarded(|p| spawn(p, &dir, shards, fsync));
    let mut c = Conn::open(port2);
    assert_eq!(
        c.send(&["GET", "k1056:control"]),
        "$1\r\n1\r\n",
        "the control write did not survive the restart — the AOF itself is broken"
    );
    let after = probe_all(&mut c, rows);
    let changed: Vec<String> = labels
        .iter()
        .zip(before.iter().zip(&after))
        .filter(|(_, (b, a))| b != a)
        .map(|(l, (b, a))| format!("{l}: before={b:?} after={a:?}"))
        .collect();
    assert!(
        changed.is_empty(),
        "{} of {} probes changed across kill -9 + restart — a pop the client \
         was acknowledged was undone or re-applied:\n  {}",
        changed.len(),
        labels.len(),
        changed.join("\n  ")
    );
}

/// Every blocking pop, parked on a key owned by each shard in turn.
fn cross_shard_rows() -> Vec<Row> {
    let mut rows = Vec::new();
    for s in 0..SHARDS {
        let l = key_on("k1056:l:", s, SHARDS);
        rows.push(Row {
            label: format!("BLPOP owner={s}"),
            pop: owned(&["BLPOP", &l, "10"]),
            writes: vec![owned(&["RPUSH", &l, "a", "b"])],
            probes: vec![owned(&["LRANGE", &l, "0", "-1"])],
            expect: vec![arr(&["b"])],
        });
        let r = key_on("k1056:r:", s, SHARDS);
        rows.push(Row {
            label: format!("BRPOP owner={s}"),
            pop: owned(&["BRPOP", &r, "10"]),
            writes: vec![owned(&["RPUSH", &r, "a", "b"])],
            probes: vec![owned(&["LRANGE", &r, "0", "-1"])],
            expect: vec![arr(&["a"])],
        });
        let m = key_on("k1056:m:", s, SHARDS);
        rows.push(Row {
            label: format!("BLMPOP COUNT 2 owner={s}"),
            pop: owned(&["BLMPOP", "10", "1", &m, "LEFT", "COUNT", "2"]),
            writes: vec![owned(&["RPUSH", &m, "a", "b", "c"])],
            probes: vec![owned(&["LRANGE", &m, "0", "-1"])],
            expect: vec![arr(&["c"])],
        });
        let zmin = key_on("k1056:zmin:", s, SHARDS);
        rows.push(Row {
            label: format!("BZPOPMIN owner={s}"),
            pop: owned(&["BZPOPMIN", &zmin, "10"]),
            writes: vec![owned(&["ZADD", &zmin, "1", "m1", "2", "m2"])],
            probes: vec![owned(&["ZRANGE", &zmin, "0", "-1"])],
            expect: vec![arr(&["m2"])],
        });
        let zmax = key_on("k1056:zmax:", s, SHARDS);
        rows.push(Row {
            label: format!("BZPOPMAX owner={s}"),
            pop: owned(&["BZPOPMAX", &zmax, "10"]),
            writes: vec![owned(&["ZADD", &zmax, "1", "m1", "2", "m2"])],
            probes: vec![owned(&["ZRANGE", &zmax, "0", "-1"])],
            expect: vec![arr(&["m1"])],
        });
        let zm = key_on("k1056:zm:", s, SHARDS);
        rows.push(Row {
            label: format!("BZMPOP MIN owner={s}"),
            pop: owned(&["BZMPOP", "10", "1", &zm, "MIN"]),
            writes: vec![owned(&["ZADD", &zm, "1", "m1", "2", "m2"])],
            probes: vec![owned(&["ZRANGE", &zm, "0", "-1"])],
            expect: vec![arr(&["m2"])],
        });
        // The move family: source AND destination, both on the owner.
        let t = tag_on(s, SHARDS);
        let (src, dst) = (format!("{t}:mv:src"), format!("{t}:mv:dst"));
        rows.push(Row {
            label: format!("BLMOVE owner={s}"),
            pop: owned(&["BLMOVE", &src, &dst, "LEFT", "RIGHT", "10"]),
            writes: vec![owned(&["RPUSH", &src, "a", "b"])],
            probes: vec![
                owned(&["LRANGE", &src, "0", "-1"]),
                owned(&["LRANGE", &dst, "0", "-1"]),
            ],
            expect: vec![arr(&["b"]), arr(&["a"])],
        });
        let (src, dst) = (format!("{t}:rpl:src"), format!("{t}:rpl:dst"));
        rows.push(Row {
            label: format!("BRPOPLPUSH owner={s}"),
            pop: owned(&["BRPOPLPUSH", &src, &dst, "10"]),
            writes: vec![owned(&["RPUSH", &src, "a", "b"])],
            probes: vec![
                owned(&["LRANGE", &src, "0", "-1"]),
                owned(&["LRANGE", &dst, "0", "-1"]),
            ],
            expect: vec![arr(&["a"]), arr(&["b"])],
        });
        // A waiter parked on keys of TWO owners (the claim-token path,
        // moon#1019): the second key's owner serves it.
        let first = key_on("k1056:span:a:", s, SHARDS);
        let second = key_on("k1056:span:b:", (s + 1) % SHARDS, SHARDS);
        rows.push(Row {
            label: format!("BLPOP spanning owner={}", (s + 1) % SHARDS),
            pop: owned(&["BLPOP", &first, &second, "10"]),
            writes: vec![owned(&["RPUSH", &second, "a", "b"])],
            probes: vec![
                owned(&["LRANGE", &first, "0", "-1"]),
                owned(&["LRANGE", &second, "0", "-1"]),
            ],
            expect: vec![arr(&[]), arr(&["b"])],
        });
    }
    rows
}

/// moon#1056 (a) and (c): a pop served for a waiter on another shard must be
/// in the OWNER's AOF. Pre-fix every row owned by a shard other than the
/// waiter's came back after the restart.
#[test]
fn cross_shard_pops_survive_kill9() {
    run_kill9(
        "blocking_pop_1056_xshard",
        SHARDS,
        "always",
        &cross_shard_rows(),
    );
}

/// Rows whose waking write and a LATER write to the same key arrive in one
/// pipelined batch. The two orders of "pop" and "later write" leave different
/// states, so replay in the wrong order cannot match.
fn ordering_rows() -> Vec<Row> {
    vec![
        // [a,b] -> pop a -> [b] -> LPUSH x -> [x,b].
        // Logged pop-after-LPUSH replays as [x,a,b] -> [a,b].
        Row {
            label: "BLPOP then LPUSH".into(),
            pop: owned(&["BLPOP", "k1056:o:l", "10"]),
            writes: vec![
                owned(&["RPUSH", "k1056:o:l", "a", "b"]),
                owned(&["LPUSH", "k1056:o:l", "x"]),
            ],
            probes: vec![owned(&["LRANGE", "k1056:o:l", "0", "-1"])],
            expect: vec![arr(&["x", "b"])],
        },
        // {a:1,b:2} -> pop a -> {b} -> ZADD x:0 -> {x,b}.
        // Logged late: {x,a,b} -> ZPOPMIN pops x -> {a,b}.
        Row {
            label: "BZPOPMIN then ZADD".into(),
            pop: owned(&["BZPOPMIN", "k1056:o:z", "10"]),
            writes: vec![
                owned(&["ZADD", "k1056:o:z", "1", "a", "2", "b"]),
                owned(&["ZADD", "k1056:o:z", "0", "x"]),
            ],
            probes: vec![owned(&["ZRANGE", "k1056:o:z", "0", "-1"])],
            expect: vec![arr(&["x", "b"])],
        },
        // src [a,b] -> move a -> src [b], dst [a] -> LPUSH src x -> [x,b].
        // Logged late: src [x,a,b] -> move x -> src [a,b], dst [x].
        Row {
            label: "BLMOVE then LPUSH".into(),
            pop: owned(&[
                "BLMOVE",
                "k1056:o:src",
                "k1056:o:dst",
                "LEFT",
                "RIGHT",
                "10",
            ]),
            writes: vec![
                owned(&["RPUSH", "k1056:o:src", "a", "b"]),
                owned(&["LPUSH", "k1056:o:src", "x"]),
            ],
            probes: vec![
                owned(&["LRANGE", "k1056:o:src", "0", "-1"]),
                owned(&["LRANGE", "k1056:o:dst", "0", "-1"]),
            ],
            expect: vec![arr(&["x", "b"]), arr(&["a"])],
        },
        // The waking write is an EXEC body.
        Row {
            label: "EXEC wakes, then LPUSH".into(),
            pop: owned(&["BLPOP", "k1056:o:tx", "10"]),
            writes: vec![
                owned(&["MULTI"]),
                owned(&["RPUSH", "k1056:o:tx", "a", "b"]),
                owned(&["EXEC"]),
                owned(&["LPUSH", "k1056:o:tx", "x"]),
            ],
            probes: vec![owned(&["LRANGE", "k1056:o:tx", "0", "-1"])],
            expect: vec![arr(&["x", "b"])],
        },
        // The waking write is a MOVE into the waiter's db (the waiter is in
        // db 0; the list is built in db 1 and moved over).
        Row {
            label: "MOVE wakes, then LPUSH".into(),
            pop: owned(&["BLPOP", "k1056:o:mv", "10"]),
            writes: vec![
                owned(&["SELECT", "1"]),
                owned(&["RPUSH", "k1056:o:mv", "a", "b"]),
                owned(&["MOVE", "k1056:o:mv", "0"]),
                owned(&["SELECT", "0"]),
                owned(&["LPUSH", "k1056:o:mv", "x"]),
            ],
            probes: vec![owned(&["LRANGE", "k1056:o:mv", "0", "-1"])],
            expect: vec![arr(&["x", "b"])],
        },
    ]
}

/// moon#1056 (b): the pop is logged at its place in the owner's order —
/// before a later write to the same key — at `--shards 1`, where the waiter
/// and the owner are the same shard.
#[test]
fn a_pop_is_logged_before_the_writes_that_follow_it() {
    run_kill9("blocking_pop_1056_order", 1, "everysec", &ordering_rows());
}

/// moon#1056 (d): a replica applies each served pop exactly once and in the
/// owner's order. Same batch shape as the ordering rows, then `WAIT` and a
/// state comparison: a missing pop leaves `[x,a,b]`, a duplicated one `[b]`,
/// a misordered one `[a,b]`.
#[cfg(feature = "runtime-monoio")]
#[test]
#[ignore]
fn replica_receives_each_pop_once_in_owner_order() {
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

    for shards in [1, SHARDS] {
        let mdir = unique_test_dir("blocking_pop_1056_master");
        let rdir = unique_test_dir("blocking_pop_1056_replica");
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

        // One key per shard (all on one shard at --shards 1), each fed and
        // then overwritten in one batch while a waiter is parked on it.
        let rows: Vec<Row> = (0..SHARDS)
            .map(|s| {
                let k = key_on("k1056:repl:", s % shards, shards);
                Row {
                    label: format!("shards={shards} BLPOP {k}"),
                    pop: owned(&["BLPOP", &k, "10"]),
                    writes: vec![owned(&["RPUSH", &k, "a", "b"]), owned(&["LPUSH", &k, "x"])],
                    probes: vec![owned(&["LRANGE", &k, "0", "-1"])],
                    expect: vec![arr(&["x", "b"])],
                }
            })
            // A key can repeat at --shards 1; keep each once.
            .fold(Vec::new(), |mut acc: Vec<Row>, row| {
                if !acc.iter().any(|a| a.pop == row.pop) {
                    acc.push(row);
                }
                acc
            });
        park_and_wake(mport, &rows);

        let mut m = Conn::open(mport);
        let on_master = probe_all(&mut m, &rows);
        for (row, mv) in rows.iter().zip(&on_master) {
            assert_eq!(mv, &row.expect[0], "{}: master state", row.label);
        }
        // Each shard feeds the replica its own stream, so no single fence
        // proves every stream has drained: poll the replica until it agrees
        // with the master, and report what it held if it never does. A
        // replica that applied a pop twice, never, or out of order never
        // converges — nothing after these writes can repair it.
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut on_replica = probe_all(&mut r, &rows);
        while on_replica != on_master && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(100));
            on_replica = probe_all(&mut r, &rows);
        }
        let diverged: Vec<String> = rows
            .iter()
            .zip(on_master.iter().zip(&on_replica))
            .filter(|(_, (mv, rv))| mv != rv)
            .map(|(row, (mv, rv))| format!("{}: master={mv:?} replica={rv:?}", row.label))
            .collect();
        assert!(
            diverged.is_empty(),
            "the replica diverged from the master — a served pop reached it \
             missing, twice, or out of order:\n  {}",
            diverged.join("\n  ")
        );
    }
}
