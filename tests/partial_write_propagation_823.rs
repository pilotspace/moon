//! moon#823 — a command must not write anything it cannot propagate.
//!
//! ## The bug this file exists to keep closed
//!
//! `redis.call`/`redis.pcall` converted a Lua `nil`, boolean or table argument
//! into `Frame::Null` / `Frame::Integer` and handed it straight to `dispatch`.
//! No wire client can produce those shapes in an argv, so command parsers do
//! not expect them: `extract_bytes` returns `None`, and HSET, HMSET, LPUSH,
//! RPUSH, LPUSHX, RPUSHX, ZREM, MSET and MSETNX each discover that from INSIDE
//! their mutation loop and return an arity error having already written part of
//! the command.
//!
//! That alone would be a memory-ledger drift (moon#814's class). It is worse
//! than that, because propagation is gated on the reply not being an error
//! (`!matches!(resp, Frame::Error(_))`): the partial write is applied on the
//! master and is **never appended to the AOF and never sent to a replica**.
//! Measured before the fix, single shard, `--appendonly yes`:
//!
//! ```text
//! EVAL "return redis.pcall('LPUSH','mylist','a','b',true)" 0
//!   -> ERR wrong number of arguments for 'lpush' command
//! LLEN mylist = 2          <- the error was a lie; two elements are resident
//! [restart]
//! LLEN mylist = 0          <- gone. An unrelated SET in the same session survived.
//! ```
//!
//! Silent data loss across restart and permanent replica divergence, driveable
//! by any client that can `EVAL`. Real Redis 8.6.1 refuses the call outright:
//! `ERR Lua redis lib command arguments must be strings or integers`, writing
//! nothing.
//!
//! ## Two triggers, not one
//!
//! `EVAL` is the trigger that exposed this, but it is not the only one.
//! `validate_frame` (`src/protocol/parse.rs`) accepts `+`, `-`, `(`, `:`, `,`
//! and `#` as top-level array elements, so a RAW WIRE client can put an
//! integer frame where a bulk string belongs and reach the same code with no
//! Lua anywhere. Verified: `*5\\r\\n$4\\r\\nMSET\\r\\n$3\\r\\nwa1\\r\\n$1\\r\\n1\\r\\n$3\\r\\nwb1\\r\\n:7\\r\\n`
//! reaches `mset`, where real Redis answers
//! `ERR Protocol error: expected '$', got ':'` and closes the connection.
//! That protocol-layer divergence is filed separately; it is why the
//! per-command validation below matters independently of the Lua boundary.
//!
//! ## And a trigger that needs no odd frame at all
//!
//! `XADD` reaches the same shape from plain `redis-cli`: it calls
//! `db.get_or_create_stream(key)` and only THEN parses the ID, so every
//! malformed or out-of-order ID creates and charges an empty stream that is
//! never logged. `xadd_does_not_create_the_key_when_the_id_is_rejected`
//! covers it.
//!
//! ## What each test proves
//!
//! * `every_partial_write_command_refuses_a_non_string_argument` — the reply is
//!   Redis's boundary error and **nothing is written**, for all nine commands.
//!   Pre-fix this fails on the keyspace assertion, not the message one: the old
//!   code answered an arity error and left the partial write behind.
//! * `a_refused_script_write_does_not_reappear_or_vanish_across_restart` — the
//!   durability half. It is the assertion that would have caught the bug as
//!   DATA LOSS rather than as a wrong error string.
//!
//! Both run under either runtime; the boundary is runtime-independent.
//!
//! Run alone with: cargo test --test partial_write_propagation_823

mod common;

use std::process::Child;
use std::time::Duration;

use common::{Conn, spawn_listening_guarded, unique_test_dir};

/// Redis's exact refusal, verified against Redis 8.6.1.
const BOUNDARY_ERR: &str = "Lua redis lib command arguments must be strings or integers";

fn spawn(port: u16, dir: &std::path::Path) -> Child {
    std::process::Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--shards",
            "1",
            "--appendonly",
            "yes",
            "--appendfsync",
            "everysec",
            // ~10 other durability suites do the same: without it, every write
            // on a host below the ~5%-free threshold answers `MOONERR
            // diskfull` and the assertions below never run. A guard that stops
            // running on a full disk is not a guard.
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon")
}

/// `(command, the Lua that calls it with a bad argument, a probe, the reply
/// that probe must give when NOTHING was written)`.
type Case = (
    &'static str,
    &'static str,
    &'static [&'static str],
    &'static str,
);

/// As [`Case`], with the setup commands that must build the key first.
type PrebuiltCase = (
    &'static str,
    &'static [&'static [&'static str]],
    &'static str,
    &'static [&'static str],
    &'static str,
);

///
/// Every script puts at least one GOOD argument before the bad one, so a
/// command that validates lazily has already mutated by the time it refuses.
/// A script whose bad argument came first would pass even against the old
/// code, and would be a vacuous test.
const CASES: &[Case] = &[
    (
        "LPUSH",
        "return redis.pcall('LPUSH', KEYS[1], 'a', 'b', true)",
        &["LLEN", "k823:lpush"],
        ":0\r\n",
    ),
    (
        "RPUSH",
        "return redis.pcall('RPUSH', KEYS[1], 'a', 'b', true)",
        &["LLEN", "k823:rpush"],
        ":0\r\n",
    ),
    (
        "HSET",
        "return redis.pcall('HSET', KEYS[1], 'f1', 'v1', 'f2', true)",
        &["HLEN", "k823:hset"],
        ":0\r\n",
    ),
    (
        "HMSET",
        "return redis.pcall('HMSET', KEYS[1], 'f1', 'v1', 'f2', true)",
        &["HLEN", "k823:hmset"],
        ":0\r\n",
    ),
    (
        "MSET",
        "return redis.pcall('MSET', KEYS[1], 'v1', 'k823:mset:b', true)",
        &["EXISTS", "k823:mset"],
        ":0\r\n",
    ),
    (
        "MSETNX",
        "return redis.pcall('MSETNX', KEYS[1], 'v1', 'k823:msetnx:b', true)",
        &["EXISTS", "k823:msetnx"],
        ":0\r\n",
    ),
];

/// Commands that need the key to exist first: the bug strands a PARTIAL
/// mutation of an existing value, so the probe asserts the value is untouched.
const CASES_PREBUILT: &[PrebuiltCase] = &[
    (
        "ZREM",
        &[&["ZADD", "k823:zrem", "1", "a", "2", "b", "3", "c"]],
        "return redis.pcall('ZREM', KEYS[1], 'a', 'b', true)",
        &["ZCARD", "k823:zrem"],
        ":3\r\n",
    ),
    (
        "LPUSHX",
        &[&["RPUSH", "k823:lpushx", "seed"]],
        "return redis.pcall('LPUSHX', KEYS[1], 'a', 'b', true)",
        &["LLEN", "k823:lpushx"],
        ":1\r\n",
    ),
    (
        "RPUSHX",
        &[&["RPUSH", "k823:rpushx", "seed"]],
        "return redis.pcall('RPUSHX', KEYS[1], 'a', 'b', true)",
        &["LLEN", "k823:rpushx"],
        ":1\r\n",
    ),
];

#[test]
fn every_partial_write_command_refuses_a_non_string_argument() {
    let dir = unique_test_dir("lua-arg-823-refuse");
    std::fs::create_dir_all(&dir).expect("mkdir");
    let (_guard, port) = spawn_listening_guarded(|p| spawn(p, &dir));
    // One connection throughout: a fresh connection per probe can land on a
    // different shard and hide a shard-local difference (moon#629).
    let mut c = Conn::open(port);

    let mut failures = Vec::new();

    for (name, script, probe, want) in CASES {
        let key = probe[1];
        let reply = c.send(&["EVAL", script, "1", key]);
        if !reply.contains(BOUNDARY_ERR) {
            failures.push(format!(
                "{name}: expected the boundary refusal, got {reply:?}"
            ));
        }
        let got = c.send(probe);
        if got != *want {
            failures.push(format!(
                "{name}: refused the call but WROTE anyway — {} answered {got:?}, want {want:?}",
                probe.join(" ")
            ));
        }
    }

    for (name, setup, script, probe, want) in CASES_PREBUILT {
        for s in *setup {
            c.send(s);
        }
        let key = probe[1];
        let reply = c.send(&["EVAL", script, "1", key]);
        if !reply.contains(BOUNDARY_ERR) {
            failures.push(format!(
                "{name}: expected the boundary refusal, got {reply:?}"
            ));
        }
        let got = c.send(probe);
        if got != *want {
            failures.push(format!(
                "{name}: refused the call but MUTATED anyway — {} answered {got:?}, want {want:?}",
                probe.join(" ")
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "a non-string Lua argument reached the keyspace:\n  {}",
        failures.join("\n  ")
    );
}

#[test]
fn a_refused_script_write_does_not_reappear_or_vanish_across_restart() {
    let dir = unique_test_dir("lua-arg-823-restart");
    std::fs::create_dir_all(&dir).expect("mkdir");
    let (mut guard, port) = spawn_listening_guarded(|p| spawn(p, &dir));

    // The property under test is NOT "the keyspace matches some expected
    // value after recovery" — an earlier draft asserted that and PASSED
    // against the unfixed code, because the pre-fix partial write was
    // invisible on disk and the post-restart state happened to equal what a
    // correct server would show. The bug is a divergence between what the
    // master serves and what it can recover, so the assertion has to compare
    // BEFORE against AFTER. Verified: with the fix reverted this fails on
    // seven commands; with the fix it passes.
    let mut probes: Vec<(&str, Vec<&str>)> = Vec::new();
    for (name, _, probe, _) in CASES {
        probes.push((name, probe.to_vec()));
    }
    for (name, _, _, probe, _) in CASES_PREBUILT {
        probes.push((name, probe.to_vec()));
    }

    let before: Vec<String> = {
        let mut c = Conn::open(port);
        // A control write that MUST survive. Without it a restart that lost
        // everything would read as a pass.
        assert_eq!(c.send(&["SET", "k823:control", "1"]), "+OK\r\n");

        for (_, script, probe, _) in CASES {
            c.send(&["EVAL", script, "1", probe[1]]);
        }
        for (_, setup, script, probe, _) in CASES_PREBUILT {
            for s in *setup {
                c.send(s);
            }
            c.send(&["EVAL", script, "1", probe[1]]);
        }
        // Let `everysec` drain, so anything that WAS appended is on disk and
        // the only thing a crash can drop is something never appended at all.
        std::thread::sleep(Duration::from_millis(1500));
        probes.iter().map(|(_, p)| c.send(p)).collect()
    };

    guard.kill_now();
    common::wait_for_port_down(port);

    let (_guard2, port2) = spawn_listening_guarded(|p| spawn(p, &dir));
    let mut c = Conn::open(port2);

    assert_eq!(
        c.send(&["GET", "k823:control"]),
        "$1\r\n1\r\n",
        "the control write did not survive the restart — the AOF itself is \
         broken, so nothing else this test says is meaningful"
    );

    let mut failures = Vec::new();
    for (i, (name, probe)) in probes.iter().enumerate() {
        let after = c.send(probe);
        if after != before[i] {
            failures.push(format!(
                "{name}: {} answered {:?} before the restart and {after:?} after — \
                 the master served state it had never persisted",
                probe.join(" "),
                before[i]
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "a refused script write left the master diverged from its own AOF:\n  {}",
        failures.join("\n  ")
    );
}

/// Every `XADD` ID-rejection path used to create and charge the stream first.
///
/// `db.get_or_create_stream(key)` ran BEFORE the ID was parsed, so a malformed
/// or out-of-order ID left an empty stream in the keyspace with a birth version
/// burned and an `entry_overhead` charged — and, because propagation is gated
/// on the reply not being an error, never written to the AOF. Unlike the rest
/// of this file it needs no Lua and no odd wire frame: plain `redis-cli`
/// reaches it.
///
/// Real Redis 8.6.1 parses the ID first and creates nothing:
/// `XADD s1 bogus f v` -> error, `EXISTS s1` = 0, `DBSIZE` = 0.
///
/// All five shapes below were measured creating the key before the fix.
#[test]
fn xadd_does_not_create_the_key_when_the_id_is_rejected() {
    let dir = unique_test_dir("xadd-823-nocreate");
    std::fs::create_dir_all(&dir).expect("mkdir");
    let (_guard, port) = spawn_listening_guarded(|p| spawn(p, &dir));
    let mut c = Conn::open(port);

    let mut failures = Vec::new();

    // A fresh key per shape, so one failure cannot mask another.
    for (i, bad_id) in ["bogus", "0-0", "1-1-1", "abc-1", "-5"].iter().enumerate() {
        let key = format!("x823:{i}");
        let reply = c.send(&["XADD", &key, bad_id, "f", "v"]);
        if !reply.starts_with('-') {
            failures.push(format!("XADD {bad_id}: expected an error, got {reply:?}"));
        }
        let exists = c.send(&["EXISTS", &key]);
        if exists != ":0\r\n" {
            failures.push(format!(
                "XADD {bad_id}: rejected the ID but CREATED {key} — EXISTS answered {exists:?}"
            ));
        }
    }

    // An out-of-order ID against an EXISTING stream must leave it untouched.
    // This path never created a key (the stream was already there), but it is
    // the case a fix could plausibly break, so it is pinned.
    assert!(
        c.send(&["XADD", "x823:ex", "5-5", "f", "v"])
            .starts_with('$')
    );
    let reply = c.send(&["XADD", "x823:ex", "1-1", "f", "v"]);
    if !reply.contains("equal or smaller") {
        failures.push(format!("XADD 1-1 on an existing stream: got {reply:?}"));
    }
    let len = c.send(&["XLEN", "x823:ex"]);
    if len != ":1\r\n" {
        failures.push(format!("XADD 1-1 changed XLEN: {len:?}"));
    }

    // And a VALID id must still work — a fix that rejects everything passes
    // every assertion above.
    assert!(
        c.send(&["XADD", "x823:ok", "1-1", "f", "v"])
            .starts_with('$'),
        "a valid explicit ID must still be accepted"
    );
    assert_eq!(c.send(&["XLEN", "x823:ok"]), ":1\r\n");
    assert!(
        c.send(&["XADD", "x823:auto", "*", "f", "v"])
            .starts_with('$'),
        "auto-generated IDs must still work"
    );
    assert!(
        c.send(&["XADD", "x823:seq", "7-*", "f", "v"])
            .starts_with('$'),
        "the ms-* auto-sequence form must still work"
    );

    assert!(
        failures.is_empty(),
        "XADD created or mutated state it could not propagate:\n  {}",
        failures.join("\n  ")
    );
}
