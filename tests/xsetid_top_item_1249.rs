//! moon#1249: `XSETID` below the stream's top item must be refused, and a
//! later `XADD *` must never overwrite an existing entry.
//!
//! redis 7.0.15 answers `ERR The ID specified in XSETID is smaller than the
//! target stream top item`. moon answered `+OK` and lowered `last_id`, so the
//! next `XADD *` re-issued an ID already in the stream: the entry was
//! OVERWRITTEN and XLEN counted it twice (pre-existing, also on ae21476; found
//! by the part 3b memory review).
//!
//! Every dispatch path runs the one `xsetid` body; this drives the plain
//! command, MULTI/EXEC and Lua `redis.call` at `--shards 1` and `--shards 4`.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test xsetid_top_item_1249`.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::{Conn, ServerGuard};

const REFUSED: &str =
    "-ERR The ID specified in XSETID is smaller than the target stream top item\r\n";

fn spawn(shards: usize) -> (ServerGuard, u16, std::path::PathBuf) {
    let dir = common::unique_test_dir(&format!("xsetid-1249-s{shards}"));
    std::fs::create_dir_all(&dir).unwrap();
    let bin = common::find_moon_binary();
    let d = dir.clone();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &d.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
                "--save",
                "",
                "--disk-offload",
                "disable",
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(common::server_stderr(&d))
            .stderr(common::server_stderr(&d))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard::new(child), port, dir)
}

/// Seed `key` with one entry at `99999999999999-5` (a far-future ms, so
/// `XADD *` stays on that ms and only the sequence moves).
fn seed(c: &mut Conn, key: &str) {
    let r = c.send(&["XADD", key, "99999999999999-5", "f", "orig5"]);
    assert!(r.contains("99999999999999-5"), "XADD: {r:?}");
}

/// Five `XADD *` after the refused XSETID; the original must survive and
/// every entry be counted once.
fn assert_no_overwrite(c: &mut Conn, key: &str, how: &str) {
    for _ in 0..5 {
        let r = c.send(&["XADD", key, "*", "f", "new"]);
        assert!(r.starts_with('$'), "{how}: XADD * answered {r:?}");
    }
    let range = c.send(&["XRANGE", key, "-", "+"]);
    let xlen = c.send(&["XLEN", key]);
    assert!(
        range.contains("orig5"),
        "{how}: an XADD * overwrote entry 99999999999999-5 (XLEN {})",
        xlen.trim_end()
    );
    assert_eq!(xlen, ":6\r\n", "{how}: XLEN counts each entry once");
    assert_eq!(
        range.matches("99999999999999-5\r\n").count(),
        1,
        "{how}: the ID appears once"
    );
}

fn check(shards: usize) {
    let (guard, port, dir) = spawn(shards);
    let mut c = Conn::open(port);

    // Plain command: the reviewer's reproduction.
    seed(&mut c, "{s}plain");
    assert_eq!(
        c.send(&["XSETID", "{s}plain", "99999999999999-0"]),
        REFUSED,
        "--shards {shards}: plain XSETID below the top item"
    );
    assert_no_overwrite(&mut c, "{s}plain", "plain");

    // MULTI/EXEC.
    seed(&mut c, "{s}multi");
    assert_eq!(c.send(&["MULTI"]), "+OK\r\n");
    assert_eq!(
        c.send(&["XSETID", "{s}multi", "99999999999999-0"]),
        "+QUEUED\r\n"
    );
    let exec = c.send(&["EXEC"]);
    assert_eq!(
        exec,
        format!("*1\r\n{REFUSED}"),
        "--shards {shards}: XSETID inside MULTI/EXEC"
    );
    assert_no_overwrite(&mut c, "{s}multi", "MULTI");

    // Lua.
    seed(&mut c, "{s}lua");
    let r = c.send(&[
        "EVAL",
        "return redis.pcall('XSETID', KEYS[1], ARGV[1])",
        "1",
        "{s}lua",
        "99999999999999-0",
    ]);
    assert!(
        r.contains("smaller than the target stream top item"),
        "--shards {shards}: XSETID through redis.pcall answered {r:?}"
    );
    assert_no_overwrite(&mut c, "{s}lua", "Lua");

    // At or above the top item is still accepted, like redis.
    assert_eq!(
        c.send(&["XSETID", "{s}plain", "99999999999999-99"]),
        "+OK\r\n"
    );
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn xsetid_below_the_top_entry_never_lets_xadd_overwrite_an_entry_shards_1() {
    check(1);
}

#[test]
fn xsetid_below_the_top_entry_never_lets_xadd_overwrite_an_entry_shards_4() {
    check(4);
}
