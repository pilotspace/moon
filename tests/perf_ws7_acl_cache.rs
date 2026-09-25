//! WS7 / moon#1165: an ACL mutation must not pin existing connections on the
//! slow path for life — and must still bite immediately (fail CLOSED).
//!
//! The review measured GET −48.6% on existing connections after one
//! `ACL SETUSER probe on nopass +ping` (a user nobody is connected as): the
//! per-connection "unrestricted" cache went stale and was never refreshed, so
//! every later GET left the inline path for the locked per-command check.
//!
//! Red on `ae21476` (`MOON_BIN=<baseline>`): the inline counter stays flat
//! after the mutation. The fail-closed halves pass on both — they are the
//! security floor the refresh must not lower.
//!
//! Run: `MOON_BIN=/path/to/moon cargo test --test perf_ws7_acl_cache`

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;
mod perf_ws7_support;

use perf_ws7_support as ws7;

#[cfg(feature = "runtime-monoio")]
const N: usize = 50;

#[cfg(feature = "runtime-monoio")]
fn get_n(c: &mut common::Conn, key: &str, want: &str) {
    for _ in 0..N {
        assert_eq!(c.send(&["GET", key]), want);
    }
}

/// An unrestricted connection that predates an unrelated `ACL SETUSER` is
/// back on the inline GET path after it (monoio: the only handler with one).
#[cfg(feature = "runtime-monoio")]
#[test]
fn existing_connection_returns_to_inline_path_after_acl_setuser() {
    let srv = ws7::spawn("ws7-acl", "1", &[]);
    let mut c = ws7::conn(srv.port);
    assert_eq!(c.send(&["SET", "k", "v"]), "+OK\r\n");

    let before = ws7::local_inline(srv.admin_port);
    get_n(&mut c, "k", "$1\r\nv\r\n");
    let warm = ws7::local_inline(srv.admin_port) - before;
    assert!(
        warm >= N as u64,
        "precondition: {N} GETs on a fresh default connection must be \
         inlined, got {warm}"
    );

    // A mutation for a user nobody is connected as.
    let mut admin = ws7::conn(srv.port);
    assert_eq!(
        admin.send(&["ACL", "SETUSER", "probe", "on", "nopass", "+ping"]),
        "+OK\r\n"
    );

    let before = ws7::local_inline(srv.admin_port);
    get_n(&mut c, "k", "$1\r\nv\r\n");
    let after = ws7::local_inline(srv.admin_port) - before;
    assert!(
        after >= N as u64,
        "after an unrelated ACL SETUSER the existing default connection's \
         {N} GETs were inlined {after} times — the stale ACL cache was never \
         refreshed, so the connection is stuck on the locked slow path"
    );
}

/// Fail-closed floor, unrestricted -> restricted: revoking `get` from the
/// user an existing (fast-path) connection runs as bites on its very next
/// command — pipelined or not — and granting it back restores service.
fn revocation_applies_immediately(shards: &str) {
    let srv = ws7::spawn("ws7-acl-rev", shards, &[]);
    let mut c = ws7::conn(srv.port);
    let mut admin = ws7::conn(srv.port);
    // Keep an admin path that survives revoking commands from `default`.
    assert_eq!(
        admin.send(&["ACL", "SETUSER", "root", "on", ">pw", "~*", "&*", "+@all"]),
        "+OK\r\n"
    );
    assert_eq!(admin.send(&["AUTH", "root", "pw"]), "+OK\r\n");
    for i in 0..8 {
        let k = format!("k{i}");
        assert_eq!(c.send(&["SET", &k, "v"]), "+OK\r\n");
        assert_eq!(c.send(&["GET", &k]), "$1\r\nv\r\n");
    }

    assert_eq!(
        admin.send(&["ACL", "SETUSER", "default", "-get"]),
        "+OK\r\n"
    );
    for i in 0..8 {
        let k = format!("k{i}");
        let r = c.send(&["GET", &k]);
        assert!(
            r.starts_with("-NOPERM"),
            "revoked GET on {k} ran on an existing default connection: {r:?}"
        );
    }
    // Pipelined, and mixed with a still-permitted command.
    let r = c.pipeline(&[&["GET", "k0"], &["SET", "k0", "w"], &["GET", "k1"]]);
    let lines: Vec<&str> = r.split("\r\n").collect();
    assert!(lines[0].starts_with("-NOPERM"), "pipelined GET ran: {r:?}");
    assert_eq!(lines[1], "+OK", "SET is still permitted: {r:?}");
    assert!(lines[2].starts_with("-NOPERM"), "pipelined GET ran: {r:?}");

    assert_eq!(
        admin.send(&["ACL", "SETUSER", "default", "+get"]),
        "+OK\r\n"
    );
    assert_eq!(c.send(&["GET", "k0"]), "$1\r\nw\r\n");

    // DELUSER of the connection's own user: denied, never promoted.
    assert_eq!(
        admin.send(&["ACL", "SETUSER", "alice", "on", ">a", "~*", "+@all"]),
        "+OK\r\n"
    );
    let mut a = ws7::conn(srv.port);
    assert_eq!(a.send(&["AUTH", "alice", "a"]), "+OK\r\n");
    assert_eq!(a.send(&["GET", "k0"]), "$1\r\nw\r\n");
    assert_eq!(admin.send(&["ACL", "DELUSER", "alice"]), ":1\r\n");
    // moon, like redis, disconnects a deleted user's sessions; a session that
    // is still open must at least be refused. Either way, never served.
    match send_or_closed(&mut a, &["GET", "k0"]) {
        None => {}
        Some(r) => assert!(
            r.starts_with("-NOPERM"),
            "a deleted user's live connection still ran GET: {r:?}"
        ),
    }
}

/// `Some(reply)`, or `None` when the server closed the connection instead.
fn send_or_closed(c: &mut common::Conn, parts: &[&str]) -> Option<String> {
    use std::io::{Read, Write};
    if c.sock.write_all(&common::encode(parts)).is_err() {
        return None;
    }
    let mut buf = [0u8; 4096];
    match c.sock.read(&mut buf) {
        Ok(0) | Err(_) => None,
        Ok(n) => Some(String::from_utf8_lossy(&buf[..n]).into_owned()),
    }
}

#[test]
fn unrestricted_to_restricted_revocation_applies_immediately_1_shard() {
    revocation_applies_immediately("1");
}

#[test]
fn unrestricted_to_restricted_revocation_applies_immediately_2_shards() {
    revocation_applies_immediately("2");
}

/// Fail-closed floor, restricted -> more restricted: a restricted user's
/// revocation bites on its next command, and a grant back applies too.
#[test]
fn restricted_user_revocation_applies_immediately() {
    let srv = ws7::spawn("ws7-acl-restr", "2", &[]);
    let mut admin = ws7::conn(srv.port);
    assert_eq!(admin.send(&["SET", "app:1", "x"]), "+OK\r\n");
    assert_eq!(
        admin.send(&[
            "ACL", "SETUSER", "bob", "on", ">b", "~app:*", "+get", "+set", "+auth"
        ]),
        "+OK\r\n"
    );
    let mut b = ws7::conn(srv.port);
    assert_eq!(b.send(&["AUTH", "bob", "b"]), "+OK\r\n");
    assert_eq!(b.send(&["GET", "app:1"]), "$1\r\nx\r\n");
    let r = b.send(&["GET", "other"]);
    assert!(r.starts_with("-NOPERM"), "key pattern not enforced: {r:?}");

    assert_eq!(admin.send(&["ACL", "SETUSER", "bob", "-get"]), "+OK\r\n");
    let r = b.send(&["GET", "app:1"]);
    assert!(r.starts_with("-NOPERM"), "bob's revoked GET ran: {r:?}");
    assert_eq!(b.send(&["SET", "app:2", "y"]), "+OK\r\n");

    assert_eq!(
        admin.send(&["ACL", "SETUSER", "bob", "resetkeys", "~none:*"]),
        "+OK\r\n"
    );
    let r = b.send(&["SET", "app:3", "z"]);
    assert!(
        r.starts_with("-NOPERM"),
        "bob's revoked key pattern still applies: {r:?}"
    );

    assert_eq!(
        admin.send(&["ACL", "SETUSER", "bob", "+get", "~app:*"]),
        "+OK\r\n"
    );
    assert_eq!(b.send(&["GET", "app:2"]), "$1\r\ny\r\n");
}

/// CLAUDE.md "parking_lot only": the process-wide `AclTable` lock was a
/// `std::sync::RwLock` (moon#1165), with a poison `unwrap()` at every site.
/// Scans `src/` (or `MOON_SRC_ROOT`, to record the red run on an extracted
/// `ae21476` tree) for either coming back.
#[test]
fn acl_table_lock_is_parking_lot() {
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for e in std::fs::read_dir(dir).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().is_some_and(|x| x == "rs") {
                out.push(p);
            }
        }
    }
    let root =
        std::env::var("MOON_SRC_ROOT").unwrap_or_else(|_| env!("CARGO_MANIFEST_DIR").to_string());
    let mut files = Vec::new();
    walk(&std::path::Path::new(&root).join("src"), &mut files);
    let mut hits = Vec::new();
    for f in &files {
        let src = std::fs::read_to_string(f).unwrap();
        for (i, line) in src.lines().enumerate() {
            let code = line.split("//").next().unwrap_or("");
            let std_lock = code.contains("std::sync::RwLock") || code.contains("StdRwLock");
            let poison_unwrap = code.contains("acl_table.read().unwrap()")
                || code.contains("acl_table.write().unwrap()");
            if (std_lock && (code.contains("AclTable") || code.contains("type StdRwLock")))
                || poison_unwrap
            {
                hits.push(format!("{}:{}: {}", f.display(), i + 1, line.trim()));
            }
        }
    }
    assert!(
        hits.is_empty(),
        "the AclTable lock must be parking_lot::RwLock (no poison unwraps); \
         {} std::sync::RwLock sites remain:\n{}",
        hits.len(),
        hits.join("\n")
    );
}
