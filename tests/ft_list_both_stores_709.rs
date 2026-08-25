//! moon#709 — `FT._LIST` must enumerate BOTH index stores.
//!
//! An index whose schema carries no `VECTOR` field lives only in the
//! `TextStore`. `FT._LIST` enumerated the vector store alone, so such an index
//! was invisible to it even though `FT.INFO` and `FT.SEARCH` both work on it.
//! `FT._LIST` is how tools and the Moon Console discover indexes, so a
//! TEXT-only index could not be listed, inspected in a UI, or picked up by
//! anything that enumerates before acting — and it silently broke any harness
//! that used `FT._LIST` to verify index creation. That is how the bug was
//! found: 50 successful `FT.CREATE`s reported as "built 0 indexes".
//!
//! The unit test in `src/command/vector_search/tests.rs` covers the union and
//! the both-stores de-duplication. This suite exists for the half a lib test
//! cannot reach: the set must survive a restart, which means the TEXT-only
//! index has to come back from its sidecar and re-register.
#![cfg(feature = "text-index")]

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

mod common;

const VECTOR_ONLY: &[&str] = &[
    "vec",
    "ON",
    "HASH",
    "PREFIX",
    "1",
    "v0:",
    "SCHEMA",
    "emb",
    "VECTOR",
    "HNSW",
    "6",
    "TYPE",
    "FLOAT32",
    "DIM",
    "4",
    "DISTANCE_METRIC",
    "L2",
];
const TEXT_ONLY: &[&str] = &[
    "txt", "ON", "HASH", "PREFIX", "1", "p0:", "SCHEMA", "body", "TEXT",
];
const MIXED: &[&str] = &[
    "both",
    "ON",
    "HASH",
    "PREFIX",
    "1",
    "m0:",
    "SCHEMA",
    "body",
    "TEXT",
    "emb",
    "VECTOR",
    "HNSW",
    "6",
    "TYPE",
    "FLOAT32",
    "DIM",
    "4",
    "DISTANCE_METRIC",
    "L2",
];

fn spawn_on(port: u16, dir: &std::path::Path) -> Child {
    Command::new(common::find_moon_binary())
        .args(["--port", &port.to_string(), "--shards", "1", "--dir"])
        .arg(dir)
        .args(["--disk-free-min-pct", "0"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn moon — run `cargo build --release` first")
}

fn await_ready(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Ok(sock) = std::net::TcpStream::connect(("127.0.0.1", port)) {
            drop(sock);
            let mut c = common::Conn::open(port);
            if c.send(&["PING"]).contains("PONG") {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("server never became ready on :{port} after restart");
}

/// Sorted index names parsed out of an `FT._LIST` reply, so the assertion does
/// not depend on either store's hashing order. Bulk payload lines are the ones
/// that carry no RESP type prefix.
fn ft_list(c: &mut common::Conn) -> Vec<String> {
    let reply = c.send(&["FT._LIST"]);
    let mut names: Vec<String> = reply
        .split("\r\n")
        .filter(|l| !l.is_empty())
        .filter(|l| !l.starts_with(['*', '$', '%', '~', '-', '+', ':', '_']))
        .map(str::to_string)
        .collect();
    names.sort();
    names
}

#[test]
fn ft_list_returns_text_only_and_mixed_indexes_and_survives_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut guard, port) = common::spawn_listening_guarded(|p| spawn_on(p, dir.path()));
    let mut c = common::Conn::open(port);

    for schema in [VECTOR_ONLY, TEXT_ONLY, MIXED] {
        let mut argv = vec!["FT.CREATE"];
        argv.extend_from_slice(schema);
        let reply = c.send(&argv);
        assert!(
            reply.contains("OK"),
            "FT.CREATE {} must succeed, got {reply:?}",
            schema[0]
        );
    }

    // Guard the premise: `txt` must really be a working TEXT-only index. If
    // FT.CREATE quietly registered it in the vector store too, the assertion
    // below would pass without exercising the union under test at all.
    let info = c.send(&["FT.INFO", "txt"]);
    assert!(
        info.contains("txt"),
        "premise: FT.INFO must work on a TEXT-only index, got {info:?}"
    );
    c.send(&["HSET", "p0:1", "body", "hello world alpha"]);
    let hit = c.send(&["FT.SEARCH", "txt", "hello"]);
    assert!(
        hit.contains("p0:1"),
        "premise: FT.SEARCH must work on a TEXT-only index, got {hit:?}"
    );

    let expected = vec!["both".to_string(), "txt".to_string(), "vec".to_string()];
    assert_eq!(
        ft_list(&mut c),
        expected,
        "FT._LIST must list every index exactly once, across both stores"
    );

    // Restart on the SAME dir: the TEXT-only index has to come back from its
    // sidecar and re-register, or it drops out of the list again.
    drop(c);
    guard.kill_now();
    common::wait_for_port_down(port);
    let mut guard2 = common::ServerGuard::new(spawn_on(port, dir.path()));
    await_ready(port);
    let mut c2 = common::Conn::open(port);

    assert_eq!(
        ft_list(&mut c2),
        expected,
        "FT._LIST must still list every index after a restart"
    );

    drop(c2);
    guard2.kill_now();
}
