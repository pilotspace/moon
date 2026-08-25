//! #499 on the SHIPPED runtime — `TXN.COMMIT` must not report OK after the
//! cross-shard guard rejected ops in the transaction body.
//!
//! `tests/txn_partial_reject.rs` covers `handler_sharded` (the tokio path CI
//! runs per-PR). The monoio handler carries its own copy of both the guard and
//! the commit handler, and the two have drifted before — this suite spawns the
//! real `moon` binary (built with default features, i.e. monoio) so the
//! intercept order actually shipped is the one under test.
//!
//! Run:
//!   cargo test --test txn_partial_reject_monoio -- --test-threads=1
#![cfg(feature = "runtime-monoio")]

mod common;

use moon::shard::dispatch::key_to_shard;
use std::process::{Child, Command, Stdio};

const SHARDS: usize = 4;

fn spawn_moon(dir: &std::path::Path) -> (common::ServerGuard, u16) {
    let bin = common::find_moon_binary();
    common::spawn_listening_guarded(|port| {
        Command::new(&bin)
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--shards",
                &SHARDS.to_string(),
                "--appendonly",
                "no",
                "--dir",
                &dir.to_string_lossy(),
                // Dev volumes routinely sit under the 5% default; without this
                // every write answers `MOONERR diskfull` and the suite would
                // "pass" without ever exercising the guard.
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    })
}

/// One key per shard: whichever shard the connection landed on, exactly one is
/// shard-local (accepted) and the other three trip the cross-shard TXN guard.
fn one_key_per_shard(prefix: &str) -> Vec<String> {
    let mut by_shard: Vec<Option<String>> = vec![None; SHARDS];
    for i in 0..10_000 {
        let key = format!("{prefix}:{i}");
        let shard = key_to_shard(key.as_bytes(), SHARDS);
        if by_shard[shard].is_none() {
            by_shard[shard] = Some(key);
        }
        if by_shard.iter().all(Option::is_some) {
            break;
        }
    }
    by_shard
        .into_iter()
        .map(|k| k.expect("every shard must be reachable by some key"))
        .collect()
}

#[test]
fn monoio_txn_commit_aborts_when_ops_were_rejected() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (mut child, port) = spawn_moon(tmp.path());
    let mut conn = common::Conn::open(port);

    assert_eq!(conn.send(&["PING"]), "+PONG\r\n");

    let keys = one_key_per_shard("txn499m:mixed");
    assert_eq!(conn.send(&["TXN", "BEGIN"]), "+OK\r\n");

    let mut accepted = 0usize;
    let mut rejected = 0usize;
    for key in &keys {
        let reply = conn.send(&["SET", key, "v"]);
        if reply == "+OK\r\n" {
            accepted += 1;
        } else {
            assert!(
                reply.contains("cross-shard"),
                "unexpected reply for {key}: {reply:?}"
            );
            rejected += 1;
        }
    }
    assert_eq!(accepted, 1, "exactly one key is shard-local");
    assert_eq!(rejected, SHARDS - 1);

    let commit = conn.send(&["TXN", "COMMIT"]);
    assert!(
        commit.starts_with("-EXECABORT"),
        "TXN.COMMIT must abort loudly after rejected ops, got: {commit:?}"
    );
    assert!(
        commit.contains("rolled back and NOT committed"),
        "COMMIT error must state the transaction was rolled back, got: {commit:?}"
    );

    // Abort-all: the accepted sibling write is rolled back too.
    for key in &keys {
        assert_eq!(
            conn.send(&["GET", key]),
            "$-1\r\n",
            "{key} must not exist after an aborted commit"
        );
    }

    // Transaction discarded — the connection is no longer in a TXN.
    let again = conn.send(&["TXN", "COMMIT"]);
    assert!(
        again.contains("not in a cross-store transaction"),
        "expected not-in-txn error, got: {again:?}"
    );

    child.kill_now();
}

/// Regression on the shipped runtime: a transaction with no rejected ops still
/// commits `+OK` and applies.
#[test]
fn monoio_txn_commit_ok_when_no_op_was_rejected() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (mut child, port) = spawn_moon(tmp.path());
    let mut conn = common::Conn::open(port);

    assert_eq!(conn.send(&["PING"]), "+PONG\r\n");

    let probe_keys = one_key_per_shard("txn499m:probe");

    // Bounded retry: connection migration is inhibited *during* a TXN, not
    // between two, so the probe's answer can go stale.
    let mut committed = false;
    for _ in 0..4 {
        assert_eq!(conn.send(&["TXN", "BEGIN"]), "+OK\r\n");
        let mut local: Option<usize> = None;
        for key in &probe_keys {
            if conn.send(&["SET", key, "probe"]) == "+OK\r\n" {
                local = Some(key_to_shard(key.as_bytes(), SHARDS));
            }
        }
        assert_eq!(conn.send(&["TXN", "ABORT"]), "+OK\r\n");
        let local = local.expect("one probe key must be shard-local");

        let keys: Vec<String> = (0..10_000)
            .map(|i| format!("txn499m:ok:{i}"))
            .filter(|k| key_to_shard(k.as_bytes(), SHARDS) == local)
            .take(2)
            .collect();
        assert_eq!(keys.len(), 2);

        assert_eq!(conn.send(&["TXN", "BEGIN"]), "+OK\r\n");
        let a = conn.send(&["SET", &keys[0], "a"]);
        let b = conn.send(&["SET", &keys[1], "b"]);
        if a != "+OK\r\n" || b != "+OK\r\n" {
            let _ = conn.send(&["TXN", "ABORT"]);
            continue;
        }
        assert_eq!(
            conn.send(&["TXN", "COMMIT"]),
            "+OK\r\n",
            "a fully-accepted TXN must still commit OK"
        );
        assert_eq!(conn.send(&["GET", &keys[0]]), "$1\r\na\r\n");
        assert_eq!(conn.send(&["GET", &keys[1]]), "$1\r\nb\r\n");
        committed = true;
        break;
    }
    assert!(committed, "clean multi-shard TXN never got to commit");

    for key in &probe_keys {
        assert_eq!(conn.send(&["GET", key]), "$-1\r\n", "{key} must not exist");
    }

    child.kill_now();
}
