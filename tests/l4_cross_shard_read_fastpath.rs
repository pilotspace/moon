//! L4 S4 — the cross-shard shared-read fast path.
//!
//! A foreign shard's read is served on the calling thread under a shared guard
//! on the owner's database, instead of hopping through the SPSC channel. Three
//! things need pinning, and none of them is visible to `cargo check`:
//!
//! 1. **The path actually fires.** Every gate (`is_dispatch_read_supported`,
//!    `single_owner_shard`, `is_multi_key_command`, `db.is_hot`, `try_read`)
//!    is a chance to silently never take it. A version of this test that only
//!    asserted "GET returns the right value" passes with the fast path dead —
//!    green and worthless — so it asserts the COUNTER moved.
//! 2. **It answers identically to the SPSC path it replaces.** Same values,
//!    same misses.
//! 3. **It does not break read-your-own-writes.** Serving a read locally lets
//!    it overtake this connection's own queued write, which is the moon#507 /
//!    moon#512 write-loss class. The `pending_mask` gate exists for exactly
//!    this, and the pipelined case below is what fails if it is dropped.
//!
//! `--shards 4`, and the keys are plain `k:N` so they scatter across all four:
//! with one connection pinned to one shard, ~3/4 of the reads are foreign.
//!
//! **monoio only.** The fast path is implemented in `handler_monoio`, the
//! runtime that ships; `handler_sharded` (the tokio leg) still routes every
//! cross-shard read through SPSC. Left ungated, test 1 fails on tokio — the
//! counter cannot move — and tests 2 and 3 would pass *vacuously*, which is
//! worse than skipping: they would report the ordering guard as verified on a
//! runtime where the path they guard never executes. The tokio twin is S5.
#![cfg(not(feature = "runtime-tokio"))]

mod common;

use std::process::Command;

use common::{Conn, find_moon_binary, server_stderr, spawn_listening_guarded, unique_test_dir};

/// Read one `INFO stats` counter. Returns 0 when the field is absent so a
/// missing field fails as "never fired" rather than panicking somewhere less
/// informative.
fn stat(c: &mut Conn, field: &str) -> u64 {
    let info = c.send(&["INFO", "stats"]);
    for line in info.lines() {
        let line = line.trim_end_matches('\r');
        if let Some(rest) = line.strip_prefix(field)
            && let Some(v) = rest.strip_prefix(':')
        {
            return v.trim().parse().unwrap_or(0);
        }
    }
    0
}

fn spawn(dir: &std::path::Path, fastpath: &str) -> (common::ServerGuard, u16) {
    let bin = find_moon_binary();
    spawn_listening_guarded(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "4",
                "--cross-shard-fast-path",
                fastpath,
                "--dir",
                dir.to_str().expect("utf8 dir"),
                "--appendonly",
                "no",
            ])
            .stderr(server_stderr(dir))
            .spawn()
            .expect("moon spawns")
    })
}

#[test]
fn cross_shard_reads_take_the_fast_path_and_answer_like_the_slow_one() {
    let dir = unique_test_dir("l4-fastpath-on");
    let (_guard, port) = spawn(&dir, "on");
    let mut c = Conn::open(port);

    const N: usize = 200;
    for i in 0..N {
        let reply = c.send(&["SET", &format!("k:{i}"), &format!("v{i}")]);
        assert!(reply.contains("OK"), "SET k:{i} failed: {reply:?}");
    }

    let before = stat(&mut c, "total_dispatch_cross_read_fast");

    // Every value must be right REGARDLESS of which path served it.
    for i in 0..N {
        let reply = c.send(&["GET", &format!("k:{i}")]);
        assert!(
            reply.contains(&format!("v{i}")),
            "GET k:{i} must return v{i}, got {reply:?}"
        );
    }

    let after = stat(&mut c, "total_dispatch_cross_read_fast");
    assert!(
        after > before,
        "the fast path never fired: total_dispatch_cross_read_fast stayed at {before}. \
         With 4 shards and {N} scattered keys, roughly 3/4 of these reads are foreign, \
         so a zero here means a gate is rejecting everything."
    );

    // A miss must also answer correctly on the fast path.
    let miss = c.send(&["GET", "k:definitely-absent"]);
    assert!(
        miss.starts_with("$-1") || miss.starts_with("_\r\n"),
        "a cross-shard miss must be a null reply, got {miss:?}"
    );
}

#[test]
fn the_fast_path_never_overtakes_this_connections_own_queued_write() {
    let dir = unique_test_dir("l4-fastpath-order");
    let (_guard, port) = spawn(&dir, "on");
    let mut c = Conn::open(port);

    // SET then GET the SAME key inside ONE pipeline, 64 keys over 4 shards.
    // The SET is a cross-shard write and sets this target's pending bit; if the
    // GET were allowed onto the fast path it would read the owner's database
    // before the queued SET landed and answer nil. That is moon#507 exactly.
    for i in 0..64 {
        let key = format!("own:{i}");
        let val = format!("w{i}");
        let replies = c.pipeline(&[&["SET", &key, &val], &["GET", &key]]);
        assert!(
            replies.contains(&val),
            "read-your-own-writes violated for {key}: a GET pipelined behind its \
             own cross-shard SET returned {replies:?} instead of {val}"
        );
    }
}

#[test]
fn the_fast_path_stays_dark_when_the_flag_is_off() {
    let dir = unique_test_dir("l4-fastpath-off");
    let (_guard, port) = spawn(&dir, "off");
    let mut c = Conn::open(port);

    for i in 0..100 {
        c.send(&["SET", &format!("k:{i}"), &format!("v{i}")]);
    }
    for i in 0..100 {
        let reply = c.send(&["GET", &format!("k:{i}")]);
        assert!(reply.contains(&format!("v{i}")), "GET k:{i} -> {reply:?}");
    }

    assert_eq!(
        stat(&mut c, "total_dispatch_cross_read_fast"),
        0,
        "off must mean off: no read may take the fast path"
    );
    assert!(
        stat(&mut c, "total_dispatch_cross_spsc") > 0,
        "with the fast path off, cross-shard reads must still be going somewhere"
    );
}
