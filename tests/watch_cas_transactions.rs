//! ADD task `watch-cas-transactions` — failing-first suite.
//!
//! WATCH is optimistic locking: after `WATCH k`, if anyone writes `k` before
//! `EXEC`, the transaction must abort. Every client library's CAS loop —
//! redis-py `pipeline().watch()`, go-redis `TxPipelined`, Lettuce — is built on
//! exactly that, and nothing else in Moon's test tree asserts it.
//!
//! Measured on `main` @8b1153b4, default (monoio) build, both shards=1 and
//! shards=4, RESP and inline: `WATCH` and `UNWATCH` reply
//! `-ERR unknown command`. WATCH exists only in `handler_single.rs` (the
//! EMBEDDED path) and the CAS check only in `execute_transaction`, which is
//! `#[cfg(feature = "runtime-tokio")]`. The two production paths route through
//! `execute_transaction_sharded`, whose signature has no `watched_keys`
//! parameter at all — the check is not skipped, it is structurally absent. So a
//! transaction that declared a dependency on `k` commits over a conflicting
//! write and silently clobbers it.
//!
//! Expected RED on main:
//!   wc1  conflicting write does not abort EXEC          (the headline)
//!   wc3  watch on an absent key does not abort
//!   wc4  UNWATCH errors
//!   wc5  watches are never set, so nothing to clear
//!   wc6  delete+recreate does not abort (the ABA hole)
//!   wc8  WATCH arity is not enforced (unknown command instead)
//!   wc9  WATCH inside MULTI is not refused
//!   wc10 cross-shard watch is not classified
//!
//! Two are GREEN on main, both deliberately:
//!   wc2  a clean EXEC still commits — the behavior the build must not break.
//!   wc7  the paths agree. It passes today because shards=1 and shards=4 are
//!        equally broken, which is a real (if bleak) agreement. Its job is to
//!        stop a fix that lands on ONE production path — the failure mode that
//!        made the #457 inline-GET ACL bypass invisible — so it is worth
//!        keeping even though it cannot fail for the headline reason.
//!
//! Run alone with: cargo test --test watch_cas_transactions

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn spawn_moon(dir: &std::path::Path, shards: u32) -> (Child, u16) {
    common::spawn_listening(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
                // The shared /Volumes checkout hovers near the 5% diskfull
                // guard; a tripped guard turns every write into MOONERR and
                // would fail this suite for an unrelated reason.
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
            .spawn()
            .expect("spawn moon")
    })
}

struct ServerGuard(Child);
impl Drop for ServerGuard {
    fn drop(&mut self) {
        common::sigkill(&mut self.0);
    }
}

/// Connect and return only once the server answered a PING on THIS socket.
/// The listener can accept before the shard behind it serves; under a fully
/// parallel run that first connection comes back RST. Setup-only on purpose —
/// a reset inside a test body still panics, because there it is a finding.
fn connect_ready(port: u16) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(mut s) = TcpStream::connect(format!("127.0.0.1:{port}")) {
            s.set_read_timeout(Some(Duration::from_secs(10))).ok();
            s.set_write_timeout(Some(Duration::from_secs(10))).ok();
            if s.write_all(b"PING\r\n").is_ok() {
                let mut buf = [0u8; 64];
                if let Ok(n) = s.read(&mut buf)
                    && n > 0
                    && buf[..n].windows(4).any(|w| w == b"PONG")
                {
                    return s;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "server on {port} never answered PING"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// One command, one raw reply. Raw bytes on purpose: the abort signal IS the
/// type byte (`*-1` / `_`), so a reader that renders replies to text would hide
/// the very thing under test.
fn cmd(s: &mut TcpStream, args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
    }
    s.write_all(&out).expect("write command");
    read_reply(s)
}

fn read_reply(s: &mut TcpStream) -> Vec<u8> {
    // One reply per command here, and every reply this suite sees is small, so
    // a single bounded read with a short settle is enough and keeps the
    // assertions on raw bytes.
    std::thread::sleep(Duration::from_millis(60));
    let mut buf = vec![0u8; 65536];
    match s.read(&mut buf) {
        Ok(n) => buf[..n].to_vec(),
        Err(e) => panic!("read reply: {e}"),
    }
}

fn is_null(reply: &[u8]) -> bool {
    reply.starts_with(b"*-1\r\n") || reply.starts_with(b"$-1\r\n") || reply.starts_with(b"_\r\n")
}

fn text(reply: &[u8]) -> String {
    String::from_utf8_lossy(reply).into_owned()
}

/// Two keys that provably land on different shards, discovered by ASKING the
/// server rather than reimplementing `key_to_shard` in the test: a MULTI body
/// spanning shards already answers CROSSSLOT today, so that reply is the oracle.
fn find_cross_shard_pair(port: u16) -> Option<(String, String)> {
    let mut s = connect_ready(port);
    for i in 0..64 {
        let (a, b) = (format!("wc:a{i}"), format!("wc:b{i}"));
        cmd(&mut s, &["MULTI"]);
        cmd(&mut s, &["GET", &a]);
        cmd(&mut s, &["GET", &b]);
        let r = cmd(&mut s, &["EXEC"]);
        if text(&r).contains("CROSSSLOT") {
            return Some((a, b));
        }
        cmd(&mut s, &["DISCARD"]);
    }
    None
}

/// Fresh server + a temp dir that dies with the test. Every test gets its own
/// `--dir`: an empty/reused dir silently reloads another test's state.
fn server(shards: u32) -> (ServerGuard, u16, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), shards);
    (ServerGuard(child), port, dir)
}

// ---------------------------------------------------------------------------
// wc1 — THE headline: a conflicting write must abort the transaction
// ---------------------------------------------------------------------------

#[test]
fn wc1_a_conflicting_write_aborts_the_transaction() {
    for shards in [1u32, 4] {
        let (_g, port, _d) = server(shards);
        let (mut a, mut b) = (connect_ready(port), connect_ready(port));

        cmd(&mut a, &["SET", "k", "v0"]);
        let watch = cmd(&mut a, &["WATCH", "k"]);
        assert!(
            watch.starts_with(b"+OK"),
            "shards={shards}: WATCH must be accepted, got {:?}",
            text(&watch)
        );

        cmd(&mut a, &["MULTI"]);
        cmd(&mut a, &["SET", "k", "from-A"]);
        // The conflict, from a genuinely separate connection.
        cmd(&mut b, &["SET", "k", "from-B"]);

        let exec = cmd(&mut a, &["EXEC"]);
        assert!(
            is_null(&exec),
            "shards={shards}: EXEC must abort after a conflicting write to a watched key, \
             got {:?}",
            text(&exec)
        );

        // The abort is only real if the queued write did not land.
        let got = cmd(&mut b, &["GET", "k"]);
        assert!(
            text(&got).contains("from-B"),
            "shards={shards}: aborted transaction still wrote — k should hold the conflicting \
             value from-B, got {:?}",
            text(&got)
        );
    }
}

// ---------------------------------------------------------------------------
// wc2 — the pin: no conflict must still commit (expected green today)
// ---------------------------------------------------------------------------

#[test]
fn wc2_a_clean_transaction_still_commits() {
    for shards in [1u32, 4] {
        let (_g, port, _d) = server(shards);
        let mut a = connect_ready(port);

        cmd(&mut a, &["SET", "k", "v0"]);
        cmd(&mut a, &["WATCH", "k"]);
        cmd(&mut a, &["MULTI"]);
        cmd(&mut a, &["SET", "k", "from-A"]);
        let exec = cmd(&mut a, &["EXEC"]);

        assert!(
            !is_null(&exec),
            "shards={shards}: EXEC must COMMIT when nothing wrote the watched key, got a \
             null abort. An over-eager CAS check is as broken as a missing one."
        );
        let got = cmd(&mut a, &["GET", "k"]);
        assert!(
            text(&got).contains("from-A"),
            "shards={shards}: committed transaction did not apply, got {:?}",
            text(&got)
        );
    }
}

// ---------------------------------------------------------------------------
// wc3 — a watch on an absent key is still a dependency
// ---------------------------------------------------------------------------

#[test]
fn wc3_watching_an_absent_key_aborts_when_it_appears() {
    let (_g, port, _d) = server(1);
    let (mut a, mut b) = (connect_ready(port), connect_ready(port));

    cmd(&mut a, &["DEL", "absent"]);
    cmd(&mut a, &["WATCH", "absent"]);
    cmd(&mut a, &["MULTI"]);
    cmd(&mut a, &["SET", "sentinel", "written"]);
    cmd(&mut b, &["SET", "absent", "now-exists"]);

    let exec = cmd(&mut a, &["EXEC"]);
    assert!(
        is_null(&exec),
        "EXEC must abort when a watched key that did not exist was created, got {:?}",
        text(&exec)
    );
    let sentinel = cmd(&mut b, &["EXISTS", "sentinel"]);
    assert!(
        text(&sentinel).contains(":0"),
        "aborted transaction still ran its body — sentinel should not exist, got {:?}",
        text(&sentinel)
    );
}

// ---------------------------------------------------------------------------
// wc4 — UNWATCH releases the dependency
// ---------------------------------------------------------------------------

#[test]
fn wc4_unwatch_releases_the_dependency() {
    let (_g, port, _d) = server(1);
    let (mut a, mut b) = (connect_ready(port), connect_ready(port));

    cmd(&mut a, &["SET", "k", "v0"]);
    cmd(&mut a, &["WATCH", "k"]);
    let un = cmd(&mut a, &["UNWATCH"]);
    assert!(
        un.starts_with(b"+OK"),
        "UNWATCH must reply +OK, got {:?}",
        text(&un)
    );

    cmd(&mut a, &["MULTI"]);
    cmd(&mut a, &["SET", "k", "from-A"]);
    cmd(&mut b, &["SET", "k", "from-B"]);

    let exec = cmd(&mut a, &["EXEC"]);
    assert!(
        !is_null(&exec),
        "after UNWATCH the conflicting write must NOT abort the transaction, got {:?}",
        text(&exec)
    );
    let got = cmd(&mut a, &["GET", "k"]);
    assert!(
        text(&got).contains("from-A"),
        "unwatched transaction should have committed, got {:?}",
        text(&got)
    );
}

// ---------------------------------------------------------------------------
// wc5 — EXEC clears watches on BOTH outcomes
// ---------------------------------------------------------------------------

#[test]
fn wc5_exec_clears_watches_on_both_outcomes() {
    let (_g, port, _d) = server(1);
    let (mut a, mut b) = (connect_ready(port), connect_ready(port));

    // Cycle 1: force an abort.
    cmd(&mut a, &["SET", "k", "v0"]);
    cmd(&mut a, &["WATCH", "k"]);
    cmd(&mut a, &["MULTI"]);
    cmd(&mut a, &["SET", "k", "v1"]);
    cmd(&mut b, &["SET", "k", "conflict"]);
    let first = cmd(&mut a, &["EXEC"]);
    assert!(is_null(&first), "setup: first EXEC should abort");

    // Cycle 2: no new WATCH. A watch surviving the abort would wrongly abort
    // this one too — a stale dependency is how a CAS loop livelocks.
    cmd(&mut a, &["MULTI"]);
    cmd(&mut a, &["SET", "k", "v2"]);
    let second = cmd(&mut a, &["EXEC"]);
    assert!(
        !is_null(&second),
        "the aborted EXEC must have cleared its watches; the next transaction \
         aborted with no WATCH of its own, got {:?}",
        text(&second)
    );
    let got = cmd(&mut a, &["GET", "k"]);
    assert!(
        text(&got).contains("v2"),
        "second transaction should have committed, got {:?}",
        text(&got)
    );
}

// ---------------------------------------------------------------------------
// wc6 — the ABA hole: delete + recreate is a conflict
// ---------------------------------------------------------------------------

#[test]
fn wc6_delete_and_recreate_is_a_conflict() {
    let (_g, port, _d) = server(1);
    let (mut a, mut b) = (connect_ready(port), connect_ready(port));

    // Versions are per-entry, start at INITIAL_VERSION = 1, and die with the
    // entry (src/storage/entry.rs:332). So DEL + re-SET returns k to version 1
    // — the same token WATCH recorded — and a version-only check commits where
    // Redis aborts. The key was destroyed and recreated; that IS a conflict.
    cmd(&mut a, &["SET", "k", "v0"]);
    cmd(&mut a, &["WATCH", "k"]);
    cmd(&mut a, &["MULTI"]);
    cmd(&mut a, &["SET", "sentinel", "written"]);

    cmd(&mut b, &["DEL", "k"]);
    cmd(&mut b, &["SET", "k", "v0"]);

    let exec = cmd(&mut a, &["EXEC"]);
    assert!(
        is_null(&exec),
        "EXEC must abort after a watched key was deleted and recreated (ABA): the entry \
         version resets to INITIAL_VERSION, so a version-only check cannot see it. Got {:?}",
        text(&exec)
    );
    let sentinel = cmd(&mut b, &["EXISTS", "sentinel"]);
    assert!(
        text(&sentinel).contains(":0"),
        "aborted transaction still ran its body, got {:?}",
        text(&sentinel)
    );
}

// ---------------------------------------------------------------------------
// wc7 — every dispatch path agrees
// ---------------------------------------------------------------------------

#[test]
fn wc7_all_dispatch_paths_agree() {
    // Same sequence, both shard counts. The reply must be byte-identical:
    // WATCH lives only in the embedded path today, so this is where a
    // production-path-only gap shows up as a difference rather than a guess.
    let mut seen: Vec<(u32, String, String)> = Vec::new();
    for shards in [1u32, 4] {
        let (_g, port, _d) = server(shards);
        let (mut a, mut b) = (connect_ready(port), connect_ready(port));

        cmd(&mut a, &["SET", "k", "v0"]);
        let watch = text(&cmd(&mut a, &["WATCH", "k"]));
        cmd(&mut a, &["MULTI"]);
        cmd(&mut a, &["SET", "k", "from-A"]);
        cmd(&mut b, &["SET", "k", "from-B"]);
        let exec = text(&cmd(&mut a, &["EXEC"]));
        seen.push((shards, watch, exec));
    }
    let (s0, w0, e0) = &seen[0];
    for (s, w, e) in &seen[1..] {
        assert_eq!(
            w, w0,
            "WATCH reply differs between shards={s0} and shards={s}: {w0:?} vs {w:?}"
        );
        assert_eq!(
            e, e0,
            "EXEC reply differs between shards={s0} and shards={s}: {e0:?} vs {e:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// wc8 / wc9 / wc10 — the rejections
// ---------------------------------------------------------------------------

#[test]
fn wc8_watch_without_keys_is_an_arity_error() {
    let (_g, port, _d) = server(1);
    let mut a = connect_ready(port);

    let r = text(&cmd(&mut a, &["WATCH"]));
    assert!(
        r.contains("wrong number of arguments"),
        "bare WATCH must be an arity error, got {r:?}"
    );

    // And it must not have half-registered anything: a later transaction with
    // no watch of its own still commits.
    cmd(&mut a, &["MULTI"]);
    cmd(&mut a, &["SET", "k", "v1"]);
    let exec = cmd(&mut a, &["EXEC"]);
    assert!(
        !is_null(&exec),
        "a rejected WATCH must leave no dependency behind, got {:?}",
        text(&exec)
    );
}

#[test]
fn wc9_watch_inside_multi_is_refused() {
    let (_g, port, _d) = server(1);
    let mut a = connect_ready(port);

    cmd(&mut a, &["MULTI"]);
    let r = text(&cmd(&mut a, &["WATCH", "k"]));
    assert!(
        r.contains("WATCH inside MULTI"),
        "WATCH inside MULTI must be refused, got {r:?}"
    );

    // It must be refused, not QUEUED: the body is one command, so EXEC returns
    // exactly one reply. A queued WATCH would make it two.
    cmd(&mut a, &["SET", "k", "v1"]);
    let exec = text(&cmd(&mut a, &["EXEC"]));
    assert!(
        exec.starts_with("*1\r\n"),
        "a refused WATCH must not be queued — EXEC should return exactly 1 reply, got {exec:?}"
    );
}

#[test]
fn wc10_a_cross_shard_watch_is_refused() {
    let (_g, port, _d) = server(4);
    let Some((ka, kb)) = find_cross_shard_pair(port) else {
        panic!(
            "no cross-shard key pair found in 64 tries at shards=4 — the oracle (a MULTI body spanning shards answering CROSSSLOT) did not fire, so this test cannot prove anything"
        );
    };

    let mut a = connect_ready(port);
    cmd(&mut a, &["SET", &ka, "v0"]);
    cmd(&mut a, &["SET", &kb, "v0"]);
    cmd(&mut a, &["WATCH", &ka, &kb]);
    cmd(&mut a, &["MULTI"]);
    cmd(&mut a, &["SET", &ka, "from-A"]);
    let exec = text(&cmd(&mut a, &["EXEC"]));

    assert!(
        exec.contains("CROSSSLOT"),
        "watching keys on different shards must be refused loudly — the body commits under one \
         shard's lock, so a version on another shard cannot be validated atomically. Got {exec:?}"
    );
    let got = text(&cmd(&mut a, &["GET", &ka]));
    assert!(
        got.contains("v0"),
        "a CROSSSLOT-refused transaction must not have run its body, got {got:?}"
    );
}
