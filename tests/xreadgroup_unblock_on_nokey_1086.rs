//! moon#1086 — a parked `XREADGROUP ... BLOCK` whose stream is deleted,
//! retyped or loses its group is answered at once, with redis's error.
//!
//! Redis marks a group reader `unblock_on_nokey`: any write that deletes its
//! key or changes its type — and the destruction of its group — re-runs the
//! read, which answers the error it now gets. Measured against redis-server
//! 8.6.1 (a reader parked on `{x}s`, the write about 0.3 s later):
//!
//! ```text
//! DEL / UNLINK / RENAME away / MOVE / SWAPDB / FLUSHDB / FLUSHALL /
//! XGROUP DESTROY / PEXPIRE 1      -> 0.3 s  -NOGROUP No such key '{x}s' or
//!                                          consumer group 'g' in XREADGROUP
//!                                          with GROUP option
//! SET {x}s str                    -> 0.3 s  -WRONGTYPE Operation against a
//!                                          key holding the wrong kind of value
//! XGROUP DELCONSUMER / XTRIM /
//! a write to another key          -> parked until its own timeout
//! ```
//!
//! Moon answered nil at the reader's own timeout in every row. A plain
//! `XREAD` stays parked on `DEL`, as in redis.
//!
//! ```text
//! MOON_BIN=/path/to/moon cargo test --test xreadgroup_unblock_on_nokey_1086
//! ```

mod common;

use std::process::Child;
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
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon (set MOON_BIN to a built binary)")
}

/// A `{tag}` owned by `shard`, so `{tag}s` and `{tag}t` live together there.
fn tag_on(shard: usize, shards: usize) -> String {
    (0..10_000)
        .map(|i| format!("{{u1086x{i}}}"))
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

fn await_blocked(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while blocked_clients(port) < 1 {
        assert!(Instant::now() < deadline, "the reader never parked");
        std::thread::sleep(Duration::from_millis(5));
    }
    // A registration on a REMOTE owner lands a moment after the gauge moves.
    std::thread::sleep(Duration::from_millis(50));
}

fn nogroup(key: &str) -> String {
    format!(
        "-NOGROUP No such key '{key}' or consumer group 'g' in XREADGROUP with GROUP option\r\n"
    )
}

const WRONGTYPE: &str = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

/// Park `read` on its own connection, run `writes` (one pipelined batch) once
/// it has parked, and return the reader's reply and how long after the
/// writes it came.
fn park_then(port: u16, read: Vec<String>, writes: &[Vec<String>]) -> (String, Duration) {
    let reader = std::thread::spawn(move || {
        let mut w = Conn::open(port);
        let refs: Vec<&str> = read.iter().map(String::as_str).collect();
        let reply = w.send(&refs);
        (reply, Instant::now())
    });
    await_blocked(port);
    let mut c = Conn::open(port);
    let batch: Vec<Vec<&str>> = writes
        .iter()
        .map(|w| w.iter().map(String::as_str).collect())
        .collect();
    let batch_refs: Vec<&[&str]> = batch.iter().map(Vec::as_slice).collect();
    let sent = Instant::now();
    let _ = c.pipeline(&batch_refs);
    let (reply, at) = reader.join().expect("reader thread");
    (reply, at.saturating_duration_since(sent))
}

fn cmd(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|s| (*s).to_string()).collect()
}

struct Case {
    label: &'static str,
    writes: fn(&str, &str) -> Vec<Vec<String>>,
    /// `None`: the reader must stay parked (and time out with nil).
    error: Option<fn(&str) -> String>,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            label: "DEL",
            writes: |s, _| vec![cmd(&["DEL", s])],
            error: Some(nogroup),
        },
        Case {
            label: "UNLINK",
            writes: |s, _| vec![cmd(&["UNLINK", s])],
            error: Some(nogroup),
        },
        Case {
            label: "SET over the stream",
            writes: |s, _| vec![cmd(&["SET", s, "str"])],
            error: Some(|_| WRONGTYPE.to_string()),
        },
        Case {
            label: "RENAME away",
            writes: |s, t| vec![cmd(&["RENAME", s, t])],
            error: Some(nogroup),
        },
        Case {
            label: "RENAME a list onto it",
            writes: |s, t| vec![cmd(&["RPUSH", t, "x"]), cmd(&["RENAME", t, s])],
            error: Some(|_| WRONGTYPE.to_string()),
        },
        Case {
            label: "XGROUP DESTROY",
            writes: |s, _| vec![cmd(&["XGROUP", "DESTROY", s, "g"])],
            error: Some(nogroup),
        },
        Case {
            label: "DEL inside MULTI/EXEC",
            writes: |s, _| vec![cmd(&["MULTI"]), cmd(&["DEL", s]), cmd(&["EXEC"])],
            error: Some(nogroup),
        },
        Case {
            label: "DEL from a script",
            writes: |s, _| vec![cmd(&["EVAL", "return redis.call('DEL', KEYS[1])", "1", s])],
            error: Some(nogroup),
        },
        Case {
            label: "MOVE to another db",
            writes: |s, _| vec![cmd(&["MOVE", s, "1"])],
            error: Some(nogroup),
        },
        Case {
            label: "SWAPDB with an empty db",
            writes: |_, _| vec![cmd(&["SWAPDB", "0", "1"])],
            error: Some(nogroup),
        },
        Case {
            label: "FLUSHDB",
            writes: |_, _| vec![cmd(&["FLUSHDB"])],
            error: Some(nogroup),
        },
        Case {
            label: "FLUSHALL",
            writes: |_, _| vec![cmd(&["FLUSHALL"])],
            error: Some(nogroup),
        },
        Case {
            label: "PEXPIRE 1",
            writes: |s, _| vec![cmd(&["PEXPIRE", s, "1"])],
            error: Some(nogroup),
        },
        Case {
            label: "XGROUP DELCONSUMER",
            writes: |s, _| vec![cmd(&["XGROUP", "DELCONSUMER", s, "g", "c"])],
            error: None,
        },
        Case {
            label: "XTRIM",
            writes: |s, _| vec![cmd(&["XTRIM", s, "MAXLEN", "0"])],
            error: None,
        },
        Case {
            label: "a write to another key",
            writes: |_, t| vec![cmd(&["SET", t, "x"])],
            error: None,
        },
    ]
}

fn run(shards: usize) {
    let dir = unique_test_dir(&format!("xreadgroup_nokey_1086_s{shards}"));
    let (_guard, port) = spawn_listening_guarded(|p| spawn(p, &dir, shards));
    let mut c = Conn::open(port);
    let mut failures = Vec::new();
    for owner in 0..shards {
        let tag = tag_on(owner, shards);
        let (s, t) = (format!("{tag}s"), format!("{tag}t"));
        for case in cases() {
            // One fresh keyspace per row: FLUSHALL is a row.
            assert!(c.send(&["FLUSHALL"]).starts_with("+OK"));
            assert!(
                c.send(&["XGROUP", "CREATE", &s, "g", "$", "MKSTREAM"])
                    .starts_with("+OK")
            );
            let block = if case.error.is_some() { "5000" } else { "800" };
            let read = cmd(&[
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "BLOCK",
                block,
                "STREAMS",
                &s,
                ">",
            ]);
            let (reply, after) = park_then(port, read, &(case.writes)(&s, &t));
            let label = format!("shards={shards} owner={owner} {}", case.label);
            match case.error {
                Some(want) => {
                    let want = want(&s);
                    if reply != want || after > Duration::from_millis(500) {
                        failures.push(format!(
                            "{label}: got {reply:?} {:.0?} after the write, want {want:?} within 500ms",
                            after
                        ));
                    }
                }
                None => {
                    if reply != "*-1\r\n" {
                        failures.push(format!(
                            "{label}: the reader must stay parked until its own timeout, got {reply:?}"
                        ));
                    }
                }
            }
        }
        // A plain XREAD is not unblocked by a deletion, in redis or here.
        assert!(c.send(&["FLUSHALL"]).starts_with("+OK"));
        assert!(c.send(&["XADD", &s, "1-1", "f", "v"]).contains("1-1"));
        let read = cmd(&["XREAD", "BLOCK", "800", "STREAMS", &s, "$"]);
        let (reply, _) = park_then(port, read, &[cmd(&["DEL", &s])]);
        if reply != "*-1\r\n" {
            failures.push(format!(
                "shards={shards} owner={owner} XREAD + DEL: must stay parked, got {reply:?}"
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} rows differ from redis-server 8.6.1:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}

#[test]
fn a_parked_group_reader_is_answered_when_its_stream_goes_one_shard() {
    run(1);
}

#[test]
fn a_parked_group_reader_is_answered_when_its_stream_goes_four_shards() {
    run(4);
}

/// The same texts when the read is issued against a missing key or group —
/// immediate, with or without `BLOCK` (moon answered `ERR The XREADGROUP
/// subcommand requires the key to exist.` and `NOGROUP No such consumer group
/// for key name`).
#[test]
fn a_read_of_a_missing_key_or_group_answers_redis_text() {
    let dir = unique_test_dir("xreadgroup_nokey_1086_immediate");
    let (_guard, port) = spawn_listening_guarded(|p| spawn(p, &dir, 4));
    let mut c = Conn::open(port);
    for owner in 0..4 {
        let tag = tag_on(owner, 4);
        let (s, missing) = (format!("{tag}s"), format!("{tag}missing"));
        assert!(
            c.send(&["XGROUP", "CREATE", &s, "g", "$", "MKSTREAM"])
                .starts_with("+OK")
        );
        for block in [None, Some("300")] {
            let mut read = vec!["XREADGROUP", "GROUP", "g", "c"];
            if let Some(ms) = block {
                read.extend(["BLOCK", ms]);
            }
            let mut a = read.clone();
            a.extend(["STREAMS", missing.as_str(), ">"]);
            assert_eq!(
                c.send(&a),
                nogroup(&missing),
                "owner={owner} block={block:?}"
            );
            let mut b = vec!["XREADGROUP", "GROUP", "nog", "c"];
            if let Some(ms) = block {
                b.extend(["BLOCK", ms]);
            }
            b.extend(["STREAMS", s.as_str(), ">"]);
            assert_eq!(
                c.send(&b),
                format!(
                    "-NOGROUP No such key '{s}' or consumer group 'nog' in XREADGROUP with GROUP option\r\n"
                ),
                "owner={owner} block={block:?}"
            );
        }
    }
}
