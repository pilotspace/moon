//! TAG and NUMERIC field definitions must survive a restart (moon#880).
//!
//! ## The bug
//!
//! `text-indexes.meta` only ever persisted the TEXT fields of an `FT.CREATE`
//! schema: `TextStore::collect_index_metas` dropped `tag_fields` and
//! `numeric_fields`, the serializer had no slot for them, and both restore
//! paths (boot-time sidecar recovery, replica full-sync snapshot) rebuilt the
//! index with `TextIndex::new`, which takes TEXT fields only. After every
//! reboot `FT.SEARCH ix '@cat:{a}'` answered `unknown_field` while TEXT
//! queries kept working — the index looked healthy with a third of its
//! schema gone, and recovery logged success.
//!
//! ## What this test does
//!
//! Over the wire, against a real server: create a TEXT + TAG + NUMERIC index,
//! write matching hashes, confirm all three field types answer, SIGKILL the
//! server, restart it on the same `--dir`, wait for the boot-time HASH rescan
//! (TEXT is the control), and assert the TAG and NUMERIC queries still find
//! the document. SIGKILL is deliberate: the sidecar is written at `FT.CREATE`
//! and `appendfsync always` makes the HSETs durable, so a graceful shutdown
//! is not part of the contract being tested.
#![cfg(feature = "text-index")]

mod common;

use std::io::Write;
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

fn spawn_on(port: u16, dir: &std::path::Path) -> Child {
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--appendonly",
            "yes",
            "--appendfsync",
            "always",
            "--disk-free-min-pct",
            "0",
            "--dir",
        ])
        .arg(dir)
        .stdout(std::process::Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon (run `cargo build` first)")
}

/// Block until the port answers `PING`, then hand back a framed connection.
fn wait_ready(port: u16) -> common::Conn {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(Instant::now() < deadline, "server never answered PING");
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
            s.set_read_timeout(Some(Duration::from_secs(2))).ok();
            let mut buf = [0u8; 16];
            if s.write_all(b"PING\r\n").is_ok()
                && let Ok(n) = std::io::Read::read(&mut s, &mut buf)
                && buf[..n].starts_with(b"+PONG")
            {
                return common::Conn::open(port);
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The boot-time rescan re-indexes hashes asynchronously after the port
/// opens; poll the TEXT query (unaffected by the bug) until `doc:1` is back.
fn wait_for_reindex(c: &mut common::Conn) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if c.send(&["FT.SEARCH", "ix", "hello"]).contains("doc:1") {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "TEXT query never found doc:1 after restart — rescan did not run"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn tag_and_numeric_fields_survive_a_restart() {
    let dir = common::unique_test_dir("moon-text-meta-tag-numeric");
    std::fs::create_dir_all(&dir).expect("create test dir");
    let port = common::reserve_port();

    let mut guard = common::ServerGuard::new(spawn_on(port, &dir));
    let mut c = wait_ready(port);
    assert_eq!(
        c.send(&[
            "FT.CREATE",
            "ix",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
            "cat",
            "TAG",
            "price",
            "NUMERIC",
        ]),
        "+OK\r\n"
    );
    c.send(&[
        "HSET",
        "doc:1",
        "title",
        "hello world",
        "cat",
        "a",
        "price",
        "10",
    ]);
    c.send(&[
        "HSET",
        "doc:2",
        "title",
        "goodbye moon",
        "cat",
        "b",
        "price",
        "20",
    ]);

    // Pre-restart: all three field types answer. If this fails the bug under
    // test is not what broke.
    for q in ["hello", "@cat:{a}", "@price:[5 15]"] {
        let r = c.send(&["FT.SEARCH", "ix", q]);
        assert!(
            r.contains("doc:1"),
            "pre-restart {q} did not find doc:1: {r:?}"
        );
    }
    drop(c);

    guard.kill_now();
    common::wait_for_port_down(port);

    let _guard = common::ServerGuard::new(spawn_on(port, &dir));
    let mut c = wait_ready(port);
    wait_for_reindex(&mut c);

    let tag = c.send(&["FT.SEARCH", "ix", "@cat:{a}"]);
    let num = c.send(&["FT.SEARCH", "ix", "@price:[5 15]"]);
    assert!(
        tag.contains("doc:1"),
        "TAG field definition lost across restart: {tag:?}"
    );
    assert!(
        num.contains("doc:1"),
        "NUMERIC field definition lost across restart: {num:?}"
    );
    // And the other document is still excluded — the field is real, not a
    // match-all fallback.
    assert!(
        !tag.contains("doc:2"),
        "TAG filter stopped filtering: {tag:?}"
    );
    assert!(
        !num.contains("doc:2"),
        "NUMERIC filter stopped filtering: {num:?}"
    );
}
