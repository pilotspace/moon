//! L4 shared read plane (#513) — the guard-recursion hazard the compiler
//! cannot see.
//!
//! Moving `ShardSlice.databases` behind per-(shard, db) `RwLock`s replaced a
//! `RefCell` borrow with a real lock. Two things follow that no `cargo check`
//! and no unit test catches:
//!
//! 1. The old `&mut s.databases[i]` borrow ended silently under NLL. A guard
//!    does not — it lives to end of scope. Where the write path later calls
//!    `with_all` (FLUSHALL) or re-acquires the same index (`wake_producer`
//!    after a SELECT), a missing `drop` is a **runtime** panic,
//!    `"db guard held recursively"`, not a compile error.
//! 2. That panic fires on essentially every write command, so its absence is
//!    the thing worth pinning — and it is only reachable over a real socket
//!    against a real server, because the panic happens on the shard thread.
//!
//! **Why these commands and not `SET`.** A plain `SET` is served by
//! `try_inline_dispatch` (`src/server/conn/blocking.rs`), which is a different
//! dispatch path that never reaches the write-path arm this test exists to
//! cover. `HSET` and `SET … EX` are not inline-eligible, so they take the full
//! `handler_monoio` write path. A version of this test written with bare `SET`
//! passes against a server that panics on every hash write — it would be
//! green and worthless.
//!
//! Multi-shard on purpose: `--shards 4` means these keys land on different
//! shards, so the cross-shard SPSC executor (`spsc_handler`) is exercised too.

mod common;

use std::process::Command;

use common::{Conn, find_moon_binary, server_stderr, spawn_listening_guarded, unique_test_dir};

fn assert_ok(reply: &str, what: &str) {
    assert!(
        !reply.contains("ERR") && !reply.is_empty(),
        "{what} must succeed, got: {reply:?}"
    );
}

#[test]
fn write_path_survives_every_command_family_that_reacquires_its_db() {
    let dir = unique_test_dir("l4-guard-recursion");
    let bin = find_moon_binary();

    let (_guard, port) = spawn_listening_guarded(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "4",
                "--dir",
                dir.to_str().expect("utf8 dir"),
                "--appendonly",
                "no",
            ])
            .stderr(server_stderr(&dir))
            .spawn()
            .expect("moon spawns")
    });

    let mut c = Conn::open(port);

    // ── the non-inline write path, repeatedly, on one db ──────────────────
    // If the write path leaked its guard, the FIRST of these panics the shard
    // thread and every later reply times out.
    for i in 0..64 {
        let key = format!("h:{i}");
        let field = format!("f{i}");
        assert_ok(&c.send(&["HSET", &key, &field, "v"]), "HSET");
        assert_ok(&c.send(&["HGET", &key, &field]), "HGET");
    }

    // SET with an expiry — also non-inline, and it touches the expiry index.
    for i in 0..64 {
        assert_ok(
            &c.send(&["SET", &format!("e:{i}"), "v", "EX", "600"]),
            "SET EX",
        );
    }

    // ── SELECT then write: `wake_producer` re-acquires the selected db ────
    // This is the interaction that fires whenever the new db equals the one
    // the write guard was taken on.
    for db in ["0", "1", "2", "0"] {
        assert_ok(&c.send(&["SELECT", db]), "SELECT");
        assert_ok(&c.send(&["HSET", "sel", "f", "v"]), "HSET after SELECT");
    }
    assert_ok(&c.send(&["SELECT", "0"]), "SELECT back to 0");

    // ── a pipelined batch: several writes settle in ONE batch ─────────────
    let replies = c.pipeline(&[
        &["HSET", "p:a", "f", "1"],
        &["HSET", "p:b", "f", "2"],
        &["SET", "p:c", "3", "EX", "600"],
        &["HGET", "p:a", "f"],
    ]);
    assert!(
        !replies.contains("ERR"),
        "pipelined writes must all succeed: {replies:?}"
    );

    // ── FLUSHALL: the write path drops its guard, then `with_all` takes all
    // sixteen. A leaked guard makes this the panic site.
    assert_ok(&c.send(&["FLUSHALL"]), "FLUSHALL");

    // moon#677: FLUSHALL must clear EVERY database, not just the selected one.
    for db in ["0", "1", "2"] {
        assert_ok(&c.send(&["SELECT", db]), "SELECT");
        let n = c.send(&["DBSIZE"]);
        assert!(
            n.starts_with(":0"),
            "db {db} must be empty after FLUSHALL, got {n:?}"
        );
    }
    assert_ok(&c.send(&["SELECT", "0"]), "SELECT back to 0");

    // ── the server is still alive and answering ───────────────────────────
    assert_ok(&c.send(&["HSET", "after", "f", "v"]), "HSET after FLUSHALL");
    let pong = c.send(&["PING"]);
    assert!(pong.contains("PONG"), "server must still answer: {pong:?}");
}

#[test]
fn two_db_commands_hold_both_guards_without_deadlocking() {
    // MOVE and cross-db COPY take TWO write guards. `with_pair` acquires
    // ascending — the module's single deadlock rule. A hand-rolled order, or
    // a same-index pair reaching `with_pair`, hangs the shard thread rather
    // than returning an error, so this test's real assertion is that it
    // terminates at all.
    let dir = unique_test_dir("l4-two-db");
    let bin = find_moon_binary();

    let (_guard, port) = spawn_listening_guarded(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "4",
                "--dir",
                dir.to_str().expect("utf8 dir"),
                "--appendonly",
                "no",
            ])
            .stderr(server_stderr(&dir))
            .spawn()
            .expect("moon spawns")
    });

    let mut c = Conn::open(port);

    assert_ok(&c.send(&["SET", "mk", "mv"]), "SET");
    // ascending pair (0 -> 3)
    assert_ok(&c.send(&["MOVE", "mk", "3"]), "MOVE 0->3");
    // descending pair (3 -> 1): must still acquire ascending internally
    assert_ok(&c.send(&["SELECT", "3"]), "SELECT 3");
    assert_ok(&c.send(&["MOVE", "mk", "1"]), "MOVE 3->1");

    // same-db MOVE must short-circuit BEFORE `with_pair`, whose distinct-index
    // assert would otherwise panic the shard thread.
    assert_ok(&c.send(&["SELECT", "1"]), "SELECT 1");
    let same = c.send(&["MOVE", "mk", "1"]);
    assert!(
        same.starts_with(":0"),
        "same-db MOVE returns 0, never panics: {same:?}"
    );

    // cross-db COPY, both orders
    assert_ok(&c.send(&["COPY", "mk", "ck", "DB", "2"]), "COPY 1->2");
    assert_ok(&c.send(&["SELECT", "2"]), "SELECT 2");
    assert_ok(&c.send(&["COPY", "ck", "ck2", "DB", "0"]), "COPY 2->0");

    // same-db COPY: routed away from `with_pair` by parse_copy_db_args
    let sd = c.send(&["COPY", "ck", "ck3", "DB", "2"]);
    assert!(
        !sd.contains("ERR"),
        "same-db COPY must not error or panic: {sd:?}"
    );

    let pong = c.send(&["PING"]);
    assert!(pong.contains("PONG"), "server must still answer: {pong:?}");
}
