//! A restart must not flatten compact encodings.
//!
//! The wire format collapses every encoding of a type into ONE `ValueType`
//! (`Set | SetListpack | SetIntset -> ValueType::Set`), so decode had no tag to
//! tell it which form to rebuild and always produced the full one. Measured on
//! the unmodified binary: after a single restart a hash went
//! `listpack -> hashtable`, a list `listpack -> linkedlist`, and an all-integer
//! set `intset -> hashtable`, while redis 8.6.1 preserves all three across
//! `DEBUG RELOAD`.
//!
//! That made every container memory figure in BENCHMARK.md a fresh-server best
//! case: the SADD-listpack win (978.2 -> 404.5 B/key on Linux) reverts entirely
//! after one restart. This test is the regression gate.
//!
//! Run:
//!   cargo build --release
//!   cargo test --release --test restart_preserves_compact_encoding -- --ignored

mod common;

use std::process::{Child, Command};
use std::time::Duration;

fn start_moon(dir: &std::path::Path) -> (Child, u16) {
    common::spawn_listening(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--dir",
                dir.to_str().expect("utf8 dir"),
                "--appendonly",
                "yes",
                // The diskfull guard trips on a shared volume with little free
                // space and would abort startup before the probe ever runs.
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon")
    })
}

/// `OBJECT ENCODING <key>`, via a fresh connection each time.
fn encoding(port: u16, key: &str) -> String {
    let mut c = common::Conn::open(port);
    let raw = c.send(&["OBJECT", "ENCODING", key]);
    raw.trim_start_matches('$')
        .lines()
        .nth(1)
        .unwrap_or("")
        .trim()
        .to_string()
}

#[test]
#[ignore = "spawns a real server and restarts it; run with --ignored"]
fn compact_encodings_survive_a_restart() {
    let dir = common::unique_test_dir("restart-encoding");
    std::fs::create_dir_all(&dir).expect("create dir");

    let (child, port) = start_moon(&dir);
    let mut guard = common::ServerGuard::new(child);

    {
        let mut c = common::Conn::open(port);
        // Every one of these is far below LISTPACK_MAX_ENTRIES (128) and
        // LISTPACK_MAX_ELEMENT_SIZE (64), so all four must be compact.
        c.send(&["HSET", "h", "f1", "v1", "f2", "v2", "f3", "v3"]);
        c.send(&["RPUSH", "l", "a", "b", "c", "d", "e"]);
        c.send(&["SADD", "s", "alpha", "beta", "gamma"]);
        c.send(&["SADD", "si", "1", "2", "3"]);
        // A hash big enough to stay a hashtable — the negative control. Without
        // it a bug that compacts EVERYTHING would pass this test.
        let mut big: Vec<String> = vec!["HSET".into(), "hbig".into()];
        for i in 0..200 {
            big.push(format!("f{i:04}"));
            big.push("v".into());
        }
        let refs: Vec<&str> = big.iter().map(|s| s.as_str()).collect();
        c.send(&refs);
    }

    // Record the pre-restart encodings. Note `s` (a small *string* set) is a
    // hashtable on main and a listpack on `perf/set-listpack-encoding`; this
    // test must pass on both, so its precondition accepts either. What the
    // restart guarantee actually claims is asserted *after* the restart, in
    // absolute terms -- see `want` below.
    let before = [
        ("h", encoding(port, "h")),
        ("l", encoding(port, "l")),
        ("s", encoding(port, "s")),
        ("si", encoding(port, "si")),
        ("hbig", encoding(port, "hbig")),
    ];
    assert_eq!(
        before[0].1, "listpack",
        "precondition: small hash is a listpack"
    );
    assert_eq!(
        before[1].1, "listpack",
        "precondition: small list is a listpack"
    );
    assert!(
        before[2].1 == "listpack" || before[2].1 == "hashtable",
        "precondition: small string set is listpack or hashtable, got {}",
        before[2].1
    );
    assert_eq!(
        before[3].1, "intset",
        "precondition: all-integer set is an intset"
    );
    assert_eq!(
        before[4].1, "hashtable",
        "precondition: a 200-field hash is a hashtable"
    );

    // Flush to disk, then take the server down cleanly and bring it back on the
    // same --dir. This is the exact sequence that used to flatten everything.
    {
        let mut c = common::Conn::open(port);
        c.send(&["BGREWRITEAOF"]);
    }
    std::thread::sleep(Duration::from_secs(3));
    guard.kill_now();
    common::wait_for_port_down(port);

    let (child2, port2) = start_moon(&dir);
    let mut guard2 = common::ServerGuard::new(child2);

    // The guarantee, stated absolutely: a restart must leave every small
    // container in its compact encoding, and must NOT compact the big one.
    // Asserting the target encoding (rather than before == after) is what makes
    // this meaningful on main, where `s` legitimately *gains* a listpack here.
    let want = [
        ("h", "listpack"),
        ("l", "listpack"),
        ("s", "listpack"),
        ("si", "intset"),
        ("hbig", "hashtable"),
    ];
    let mut failures = Vec::new();
    for ((key, expected), (_, was)) in want.iter().zip(before.iter()) {
        let now = encoding(port2, key);
        if &now != expected {
            failures.push(format!(
                "{key}: was {was}, after restart {now}, want {expected}"
            ));
        }
    }
    guard2.kill_now();
    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        failures.is_empty(),
        "restart did not leave these in their compact encoding (redis preserves \
         all of them across DEBUG RELOAD): {}",
        failures.join(", ")
    );
    let _ = port2;
}
