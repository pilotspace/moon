//! moon#825 — a non-deterministic write must propagate its EFFECT, not itself.
//!
//! ## The bug this file exists to keep closed
//!
//! `SPOP` and `XADD key *` were written to the AOF (and streamed to replicas)
//! verbatim. Replay re-rolls the RNG and re-reads the clock, so the recovered
//! state is not the state the client was told about — and nothing errors,
//! nothing is logged, the AOF looks healthy:
//!
//! ```text
//! SADD sp a b c d e f g h
//! SPOP sp                -> "f"
//! SMEMBERS sp            -> a b c d e g h
//! [restart]
//! SMEMBERS sp            -> a b c d f g h    <- f is back, e is gone
//! ```
//!
//! The same class covers every write whose result is a function of the RNG,
//! the clock, or a condition evaluated against the clock. Enumerated from the
//! state writers rather than from the two reported names, the affected forms
//! were: `SPOP` (single and `count`), `XADD` with `*` or `ms-*`, `EXPIRE` /
//! `PEXPIRE` carrying a `NX|XX|GT|LT` flag (the frame-only expire rewrite
//! bails on the fourth argument), `HEXPIRE` / `HPEXPIRE` / `HGETEX EX|PX`
//! (relative hash-field deadlines, never rewritten), `RESTORE` with a
//! relative TTL — and, orthogonally, EVERY relative-expiry command on the
//! cross-shard, `MULTI`/`EXEC` and Lua paths, whose append sites bypassed the
//! expire rewrite entirely.
//!
//! ## What these tests prove
//!
//! Each row performs the write through the primary, records what the client
//! was told (and the state it can observe), restarts the server from its AOF,
//! and compares. `DEBUG DIGEST` over the whole keyspace is the aggregate
//! oracle; the per-row probes are what turn a digest mismatch into a named
//! command. The TTL rows probe `PEXPIRETIME` / `HPEXPIRETIME` directly, since
//! the digest hashes only the *presence* of an expiry, never its deadline.
//!
//! `SPOP` rows pop from 64-member sets so that a verbatim replay landing on
//! the same member by luck is a 1-in-64 (single) or 1-in-C(64,32) (count)
//! event per row; the assertion is over all rows together.
//!
//! One run per topology: `--shards 1` (the local write leg) and `--shards 4`
//! (the owner-shard SPSC arm, which is a different append site).
//!
//! Run alone with: cargo test --test nondeterministic_propagation_825

mod common;

use std::process::Child;
use std::time::Duration;

use common::{Conn, spawn_listening_guarded, unique_test_dir};

fn spawn(port: u16, dir: &std::path::Path, shards: &str) -> Child {
    std::process::Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--shards",
            shards,
            "--appendonly",
            "yes",
            "--appendfsync",
            "everysec",
            // Same reason as every other durability suite: below the ~5%-free
            // threshold every write answers `MOONERR diskfull` and the
            // assertions never run.
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon")
}

/// One non-deterministic write.
///
/// `setup` builds the state, `write` is the sequence of commands ending in the
/// write under test (the last reply is checked for a miss), and every probe
/// must answer the same before and after a restart.
struct Row {
    label: &'static str,
    setup: Vec<Vec<String>>,
    write: Vec<Vec<String>>,
    probes: Vec<Vec<String>>,
}

fn s(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|p| (*p).to_string()).collect()
}

/// `SADD key m00 … m63`.
fn sadd64(key: &str) -> Vec<String> {
    let mut v = vec!["SADD".to_string(), key.to_string()];
    v.extend((0..64).map(|i| format!("m{i:02}")));
    v
}

fn rows() -> Vec<Row> {
    let mut rows = Vec::new();

    for i in 0..4 {
        let key = format!("k825:spop{i}");
        rows.push(Row {
            label: "SPOP key",
            setup: vec![sadd64(&key)],
            write: vec![s(&["SPOP", &key])],
            probes: vec![s(&["SMEMBERS", &key])],
        });
    }
    for i in 0..2 {
        let key = format!("k825:spopc{i}");
        rows.push(Row {
            label: "SPOP key count",
            setup: vec![sadd64(&key)],
            write: vec![s(&["SPOP", &key, "32"])],
            probes: vec![s(&["SMEMBERS", &key])],
        });
    }
    for i in 0..2 {
        let key = format!("k825:xadd{i}");
        rows.push(Row {
            label: "XADD key *",
            setup: vec![],
            write: vec![s(&["XADD", &key, "*", "f", "v"])],
            probes: vec![s(&["XRANGE", &key, "-", "+"])],
        });
    }
    // Control: a partial ID is deterministic given the replayed prefix. It
    // must survive with or without the fix.
    rows.push(Row {
        label: "XADD key ms-* (control)",
        setup: vec![],
        write: vec![s(&["XADD", "k825:xaddp", "1000-*", "f", "v"])],
        probes: vec![s(&["XRANGE", "k825:xaddp", "-", "+"])],
    });
    rows.push(Row {
        label: "EXPIRE key ttl NX",
        setup: vec![s(&["SET", "k825:enx", "v"])],
        write: vec![s(&["EXPIRE", "k825:enx", "100000", "NX"])],
        probes: vec![s(&["PEXPIRETIME", "k825:enx"])],
    });
    rows.push(Row {
        label: "PEXPIRE key ttl GT",
        setup: vec![
            s(&["SET", "k825:pgt", "v"]),
            s(&["EXPIRE", "k825:pgt", "100000"]),
        ],
        write: vec![s(&["PEXPIRE", "k825:pgt", "200000000", "GT"])],
        probes: vec![s(&["PEXPIRETIME", "k825:pgt"])],
    });
    rows.push(Row {
        label: "HEXPIRE key ttl FIELDS",
        setup: vec![s(&["HSET", "k825:hx", "f1", "v", "f2", "v"])],
        write: vec![s(&[
            "HEXPIRE", "k825:hx", "100000", "FIELDS", "2", "f1", "f2",
        ])],
        probes: vec![s(&["HPEXPIRETIME", "k825:hx", "FIELDS", "2", "f1", "f2"])],
    });
    rows.push(Row {
        label: "HGETEX key EX ttl FIELDS",
        setup: vec![s(&["HSET", "k825:hg", "f", "v"])],
        write: vec![s(&[
            "HGETEX", "k825:hg", "EX", "100000", "FIELDS", "1", "f",
        ])],
        probes: vec![s(&["HPEXPIRETIME", "k825:hg", "FIELDS", "1", "f"])],
    });
    // RESTORE carries a binary payload; DUMP it inside the same script so the
    // bytes never cross this test's text-only connection helper. The write
    // itself is the inner RESTORE, recorded by the script's effect plane.
    rows.push(Row {
        label: "RESTORE key ttl payload (via EVAL)",
        setup: vec![s(&["SET", "k825:{r}:src", "v"])],
        write: vec![s(&[
            "EVAL",
            "redis.call('RESTORE', KEYS[1], ARGV[1], redis.call('DUMP', KEYS[2])) return 1",
            "2",
            "k825:{r}:dst",
            "k825:{r}:src",
            "100000",
        ])],
        probes: vec![
            s(&["PEXPIRETIME", "k825:{r}:dst"]),
            s(&["GET", "k825:{r}:dst"]),
        ],
    });
    rows.push(Row {
        label: "EVAL redis.call('SPOP')",
        setup: vec![sadd64("k825:lspop")],
        write: vec![s(&[
            "EVAL",
            "return redis.call('SPOP', KEYS[1])",
            "1",
            "k825:lspop",
        ])],
        probes: vec![s(&["SMEMBERS", "k825:lspop"])],
    });
    rows.push(Row {
        label: "EVAL redis.call('XADD', '*')",
        setup: vec![],
        write: vec![s(&[
            "EVAL",
            "return redis.call('XADD', KEYS[1], '*', 'f', 'v')",
            "1",
            "k825:lxadd",
        ])],
        probes: vec![s(&["XRANGE", "k825:lxadd", "-", "+"])],
    });
    rows.push(Row {
        label: "MULTI / SPOP / XADD * / EXEC",
        setup: vec![sadd64("k825:{m}:spop")],
        write: vec![
            s(&["MULTI"]),
            s(&["SPOP", "k825:{m}:spop"]),
            s(&["XADD", "k825:{m}:xadd", "*", "f", "v"]),
            s(&["EXEC"]),
        ],
        probes: vec![
            s(&["SMEMBERS", "k825:{m}:spop"]),
            s(&["XRANGE", "k825:{m}:xadd", "-", "+"]),
        ],
    });
    rows
}

/// A reply that is a RESP error or a null — the write did not happen, and
/// the row would prove nothing.
fn is_miss(reply: &str) -> bool {
    reply.starts_with('-') || reply.starts_with("*-1") || reply.starts_with("$-1")
}

/// Parse a flat RESP2 array of bulk strings; `None` for any other shape.
fn flat_bulk_array(reply: &str) -> Option<Vec<String>> {
    let mut lines = reply.split("\r\n");
    let head = lines.next()?;
    let n: usize = head.strip_prefix('*')?.parse().ok()?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let len = lines.next()?.strip_prefix('$')?;
        let _: usize = len.parse().ok()?;
        out.push(lines.next()?.to_string());
    }
    Some(out)
}

/// Canonical form of a probe reply: a flat set-shaped array is sorted (set
/// iteration order is not part of the contract), anything else is compared
/// byte-for-byte.
fn canon(reply: &str) -> String {
    match flat_bulk_array(reply) {
        Some(mut members) => {
            members.sort();
            members.join(",")
        }
        None => reply.to_string(),
    }
}

fn send(c: &mut Conn, cmd: &[String]) -> String {
    let refs: Vec<&str> = cmd.iter().map(String::as_str).collect();
    c.send(&refs)
}

fn run(shards: &str) {
    let dir = unique_test_dir(&format!("nondet_825_s{shards}"));
    let (mut guard, port) = spawn_listening_guarded(|p| spawn(p, &dir, shards));

    let rows = rows();
    let mut c = Conn::open(port);
    for row in &rows {
        for cmd in &row.setup {
            let r = send(&mut c, cmd);
            assert!(!is_miss(&r), "{}: setup {cmd:?} failed: {r:?}", row.label);
        }
        let mut last = String::new();
        for cmd in &row.write {
            last = send(&mut c, cmd);
        }
        assert!(
            !is_miss(&last),
            "{}: the write itself failed ({last:?}) — this row would prove nothing",
            row.label
        );
    }

    // What the client can observe, captured BEFORE the restart.
    let before: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.probes.iter().map(|p| canon(&send(&mut c, p))).collect())
        .collect();
    // A control write that MUST survive. If the AOF itself is broken, every
    // row below would "pass" by losing nothing that was ever there.
    send(&mut c, &s(&["SET", "k825:control", "1"]));

    // The whole-keyspace digest, taken AFTER the last write so the recovered
    // keyspace has the same key set to hash. `+<40 hex>\r\n` — a
    // SimpleString; anything else is an error or an absent command, and the
    // aggregate oracle below would be vacuous.
    let digest_before = send(&mut c, &s(&["DEBUG", "DIGEST"]));
    assert!(
        digest_before.starts_with('+') && digest_before.trim().len() == 41,
        "DEBUG DIGEST unavailable: {digest_before:?}"
    );
    std::thread::sleep(Duration::from_millis(1500));

    drop(c);
    guard.kill_now();
    common::wait_for_port_down(port);
    let (_g2, port2) = spawn_listening_guarded(|p| spawn(p, &dir, shards));
    let mut c = Conn::open(port2);
    assert_eq!(
        send(&mut c, &s(&["GET", "k825:control"])),
        "$1\r\n1\r\n",
        "the control write did not survive the restart — the AOF itself is \
         broken, so nothing else this test says is meaningful"
    );

    let mut lost = Vec::new();
    for (row, was) in rows.iter().zip(&before) {
        for (probe, was_one) in row.probes.iter().zip(was) {
            let now = canon(&send(&mut c, probe));
            if &now != was_one {
                lost.push(format!(
                    "{} [{}]: before={was_one:?} after={now:?}",
                    row.label,
                    probe.join(" ")
                ));
            }
        }
    }
    let digest_after = send(&mut c, &s(&["DEBUG", "DIGEST"]));
    assert!(
        lost.is_empty() && digest_after == digest_before,
        "{} probes over {} non-deterministic writes replayed to a different state than \
         the one the client was told about (shards={shards}); DEBUG DIGEST before={} \
         after={}:\n  {}",
        lost.len(),
        rows.len(),
        digest_before.trim(),
        digest_after.trim(),
        lost.join("\n  ")
    );
}

#[test]
fn nondeterministic_writes_replay_to_the_acknowledged_state_single_shard() {
    run("1");
}

#[test]
fn nondeterministic_writes_replay_to_the_acknowledged_state_four_shards() {
    run("4");
}
