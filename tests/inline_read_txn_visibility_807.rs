//! moon#807: the inline `GET` fast path vs. the MVCC snapshot-visibility filter.
//!
//! ## The defect
//!
//! Inside an open cross-store transaction the GENERIC read leg runs the
//! write-intent visibility filter before dispatching — the `if
//! conn.in_cross_txn()` block ahead of `dispatch_read` in
//! `src/server/conn/handler_monoio/mod.rs` (and its twin in
//! `handler_sharded/mod.rs`), which calls `KvWriteIntents::is_key_visible`
//! and answers `Null` for a key that carries another transaction's
//! UNCOMMITTED intent. `try_inline_dispatch` (`src/server/conn/blocking.rs`)
//! never consults `kv_write_intents`, and `can_inline_reads` carried no
//! `in_cross_txn` term — so a connection that was itself inside a `TXN`
//! answered a plain `GET` from the fast path and saw the foreign
//! transaction's uncommitted value. The two dispatch paths disagreed on the
//! same key for the same reader.
//!
//! Measured on `origin/main` @ `b04e8990`, `--shards 1`, release-fast binary:
//!
//!     A:  SET k original      +OK
//!     A:  TXN BEGIN           +OK
//!     A:  SET k modified      +OK           (uncommitted)
//!     B:  TXN BEGIN           +OK
//!     B:  GET k  (RESP)       "modified"    local_inline +1 — INLINED, dirty read
//!     B:  GET k  (text form)  (nil)         generic dispatch — the filter ran
//!
//! ## The fix
//!
//! `can_inline_reads` stands down while THIS connection is inside a `TXN`
//! (`&& !conn.in_cross_txn()`), the same term `can_inline_writes` already
//! carries (moon#660). The filter needs the reader's snapshot LSN and txn id
//! plus the committed-set snapshot from the vector store's txn manager;
//! none of that belongs in the hottest function in the codebase, and the
//! generic leg already does it right. Cost: one `Option::is_some()` on a
//! connection field per batch, evaluated in the same prologue that already
//! loads it for the write gate.
//!
//! ## How the two paths are told apart
//!
//! `try_inline_dispatch` parses ONLY the canonical RESP array form
//! (`*2\r\n$3\r\nGET\r\n...`); the plain-text inline protocol (`GET k\r\n`,
//! what telnet sends) fails its first byte check and goes through generic
//! dispatch. Both framings are legal on the same connection, so a single
//! reader can be asked the same question through both paths back to back —
//! which matters here because `active_cross_txn` is per-connection state and
//! a fresh connection per probe would silently test a different reader.
//! `moon_dispatch_path_total{path="local_inline"}` on the admin port then
//! confirms which path actually ran.
//!
//! ## What this file does NOT claim
//!
//! A reader that is NOT in a transaction sees the uncommitted value on BOTH
//! paths: `KvWriteIntents` is documented as "non-transactional operations
//! bypass this entirely", and the generic filter is gated on the READER's
//! `in_cross_txn()`. That is the engine's isolation contract for plain
//! clients, not an inline bypass, and the parity test below pins only that
//! the two paths agree on it. Whether the filter should answer the
//! before-image instead of `Null` for a foreign-txn reader is likewise a
//! design question about the generic filter, not about the fast path.
//!
//! Run with:
//!   cargo build --release
//!   MOON_BIN=$PWD/target/release/moon cargo test --release \
//!     --test inline_read_txn_visibility_807

#![allow(clippy::unwrap_used)]
// The inline dispatch path exists ONLY in the monoio connection handler; under
// `runtime-tokio` every GET is generic and the CONTROL below (a plain GET must
// inline) would fail for a reason unrelated to the gate.
#![cfg(feature = "runtime-monoio")]

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Server spawn
// ---------------------------------------------------------------------------

fn find_moon_binary() -> std::path::PathBuf {
    if let Ok(bin) = std::env::var("MOON_BIN") {
        let p = std::path::PathBuf::from(bin);
        if p.exists() {
            return p;
        }
    }
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        // SIGKILL, not SIGTERM: moon's SIGTERM + SO_REUSEPORT teardown can
        // hang a harness (gotcha_moon_sigterm_reuseport_bench_hang).
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Spawn a single-shard moon and return `(guard, client_port, admin_port)`,
/// verified SERVING on both planes (`+PONG` and a `/metrics` body), retrying
/// the whole pair on fresh ports otherwise — a held admin port makes moon
/// exit(1) at start-up and `spawn_listening` retries the client port only.
fn spawn_moon(dir: &std::path::Path) -> (ServerGuard, u16, u16) {
    const ATTEMPTS: usize = 3;
    for attempt in 1..=ATTEMPTS {
        let admin_port = common::reserve_port();
        let (child, port) = common::spawn_listening(|port| {
            Command::new(find_moon_binary())
                .args([
                    "--port",
                    &port.to_string(),
                    "--dir",
                    &dir.to_string_lossy(),
                    "--shards",
                    "1",
                    "--appendonly",
                    "no",
                    // Under test is the read gate, not the disk guard; a
                    // near-full dev volume would otherwise refuse the SETs.
                    "--disk-free-min-pct",
                    "0",
                    "--protected-mode",
                    "no",
                    "--admin-port",
                    &admin_port.to_string(),
                ])
                .stdout(std::process::Stdio::null())
                .stderr(common::server_stderr(dir))
                .spawn()
                .expect("spawn moon (run `cargo build --release` first)")
        });
        let guard = ServerGuard(child);
        if pong(port) && !http_get(admin_port, "/metrics").is_empty() {
            return (guard, port, admin_port);
        }
        eprintln!(
            "spawn_moon: attempt {attempt}/{ATTEMPTS} came up dead \
             (client {port}, admin {admin_port}); retrying on fresh ports"
        );
        drop(guard);
    }
    panic!(
        "moon never came up serving after {ATTEMPTS} attempts; stderr:\n{}",
        std::fs::read_to_string(dir.join("server.err")).unwrap_or_default()
    );
}

fn pong(port: u16) -> bool {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if let Ok(mut s) = TcpStream::connect_timeout(
            &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
            Duration::from_millis(200),
        ) {
            let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
            let mut buf = [0u8; 7];
            if s.write_all(b"PING\r\n").is_ok()
                && s.read_exact(&mut buf).is_ok()
                && buf.starts_with(b"+PONG")
            {
                return true;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

fn http_get(port: u16, path: &str) -> String {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let start = Instant::now();
    let mut stream = loop {
        match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            Ok(s) => break s,
            Err(e) => {
                if start.elapsed() > Duration::from_secs(20) {
                    eprintln!("admin port {port} never accepted a connection: {e}");
                    return String::new();
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    stream
        .write_all(format!("GET {path} HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n").as_bytes())
        .expect("send admin request");
    let mut body = Vec::new();
    stream.read_to_end(&mut body).expect("read admin response");
    let text = String::from_utf8_lossy(&body).into_owned();
    if !text.contains("200") {
        return String::new();
    }
    text
}

/// Scrape `moon_dispatch_path_total{path="local_inline"}` off the admin port.
/// This is the mechanism check: the only signal that separates "the inline
/// path ran" from "generic dispatch produced the same answer".
fn local_inline_count(admin_port: u16) -> u64 {
    let body = http_get(admin_port, "/metrics");
    assert!(!body.is_empty(), "admin /metrics did not answer");
    for line in body.lines() {
        if line.starts_with("moon_dispatch_path_total")
            && line.contains("local_inline")
            && !line.starts_with('#')
        {
            let n = line.rsplit(' ').next().expect("metric value field");
            return n
                .trim()
                .parse::<f64>()
                .unwrap_or_else(|e| panic!("could not parse local_inline value {n:?}: {e}"))
                as u64;
        }
    }
    // metrics-rs does not emit an untouched counter: absent == never bumped.
    0
}

// ---------------------------------------------------------------------------
// Minimal RESP client that can speak BOTH framings on one connection
// ---------------------------------------------------------------------------

#[derive(PartialEq, Clone)]
enum V {
    Simple(String),
    Err(String),
    Bulk(Vec<u8>),
    Null,
    Other(String),
}

// Hand-written so a failure prints `"modified"`, not eight byte values.
impl std::fmt::Debug for V {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            V::Simple(s) => write!(f, "+{s}"),
            V::Err(s) => write!(f, "-{s}"),
            V::Bulk(b) => write!(f, "{:?}", String::from_utf8_lossy(b)),
            V::Null => write!(f, "(nil)"),
            V::Other(s) => write!(f, "{s:?}"),
        }
    }
}

struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Client {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let writer = stream.try_clone().unwrap();
        Client {
            reader: BufReader::new(stream),
            writer,
        }
    }

    /// Canonical RESP array — the ONLY framing `try_inline_dispatch` parses.
    fn resp(&mut self, args: &[&str]) -> V {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
        }
        self.writer.write_all(&out).expect("write");
        self.parse()
    }

    /// Plain-text inline protocol (`GET k\r\n`): first byte is not `*`, so
    /// the fast path declines it and it is answered by generic dispatch.
    fn text(&mut self, line: &str) -> V {
        self.writer
            .write_all(format!("{line}\r\n").as_bytes())
            .expect("write");
        self.parse()
    }

    fn parse(&mut self) -> V {
        let mut line = String::new();
        let n = self.reader.read_line(&mut line).expect("read reply line");
        assert!(n > 0, "server closed the connection mid-reply");
        let line = line.trim_end_matches(['\r', '\n']).to_string();
        let (t, rest) = line.split_at(1);
        match t {
            "+" => V::Simple(rest.to_string()),
            "-" => V::Err(rest.to_string()),
            "$" => {
                let n: i64 = rest.parse().expect("bulk len");
                if n < 0 {
                    return V::Null;
                }
                let mut buf = vec![0u8; n as usize + 2];
                self.reader.read_exact(&mut buf).expect("bulk body");
                buf.truncate(n as usize);
                V::Bulk(buf)
            }
            "_" => V::Null,
            _ => V::Other(line),
        }
    }
}

fn bulk(s: &str) -> V {
    V::Bulk(s.as_bytes().to_vec())
}

/// Ask `reader` for `key` through the fast-path framing and report whether
/// the inline counter moved.
fn get_inline(reader: &mut Client, admin_port: u16, key: &str) -> (V, bool) {
    let before = local_inline_count(admin_port);
    let v = reader.resp(&["GET", key]);
    let after = local_inline_count(admin_port);
    (v, after > before)
}

/// Fixture shared by every test: `A` holds an OPEN transaction with an
/// uncommitted `SET k modified` over a committed `original`.
fn open_foreign_txn(port: u16) -> Client {
    let mut a = Client::connect(port);
    assert_eq!(a.resp(&["SET", "k", "original"]), V::Simple("OK".into()));
    assert_eq!(a.resp(&["TXN", "BEGIN"]), V::Simple("OK".into()));
    assert_eq!(a.resp(&["SET", "k", "modified"]), V::Simple("OK".into()));
    a
}

// ---------------------------------------------------------------------------
// The guard
// ---------------------------------------------------------------------------

/// A reader inside its own `TXN` must get the SAME answer from the fast path
/// as from generic dispatch, and never the foreign transaction's uncommitted
/// value.
///
/// Red on `origin/main` @ `b04e8990`: the RESP `GET` inlines and answers
/// `"modified"`; the text-form `GET` on the same connection answers `(nil)`.
#[test]
fn foreign_txn_reader_inline_get_agrees_with_generic_dispatch() {
    let dir = common::unique_test_dir("i807-txn-reader");
    std::fs::create_dir_all(&dir).unwrap();
    let (_guard, port, admin_port) = spawn_moon(&dir);

    let mut a = open_foreign_txn(port);

    // CONTROL: on this server a plain GET from a connection OUTSIDE any
    // transaction inlines. Without this, "the counter did not move" below
    // would also be true of a build where nothing ever inlines.
    let mut ctl = Client::connect(port);
    let (_, ctl_inlined) = get_inline(&mut ctl, admin_port, "k");
    assert!(
        ctl_inlined,
        "CONTROL failed — a plain GET outside any transaction did not inline; \
         the assertions below would be vacuous"
    );

    // ONE reader connection throughout: `active_cross_txn` is per-connection
    // state, so the fast-path and generic answers must come from the SAME
    // reader or they are answers to different questions.
    let mut b = Client::connect(port);
    assert_eq!(b.resp(&["TXN", "BEGIN"]), V::Simple("OK".into()));

    let generic = b.text("GET k");
    let (fast, inlined) = get_inline(&mut b, admin_port, "k");

    assert_ne!(
        fast,
        bulk("modified"),
        "DIRTY READ: a reader inside its own TXN saw connection A's UNCOMMITTED \
         value through the inline GET path (generic dispatch answered {generic:?})"
    );
    assert_eq!(
        fast, generic,
        "the inline path and generic dispatch disagree for the same reader on \
         the same key: inline={fast:?} generic={generic:?}"
    );
    assert!(
        !inlined,
        "a GET inside an open TXN was INLINED — the fast path cannot run the \
         write-intent visibility filter, so it must stand down to generic dispatch"
    );

    // Release: once A aborts, the intent is gone and BOTH paths answer the
    // committed value. The reader is still inside its own TXN, so this also
    // pins that the generic leg's filter lets a released key through.
    assert_eq!(a.resp(&["TXN", "ABORT"]), V::Simple("OK".into()));
    assert_eq!(b.text("GET k"), bulk("original"));
    assert_eq!(b.resp(&["GET", "k"]), bulk("original"));

    // And a committed foreign write is visible to both.
    assert_eq!(a.resp(&["TXN", "BEGIN"]), V::Simple("OK".into()));
    assert_eq!(a.resp(&["SET", "k", "committed"]), V::Simple("OK".into()));
    assert_eq!(a.resp(&["TXN", "COMMIT"]), V::Simple("OK".into()));
    assert_eq!(b.text("GET k"), bulk("committed"));
    assert_eq!(b.resp(&["GET", "k"]), bulk("committed"));
}

/// Leaving the transaction hands the connection back to the fast path — the
/// stand-down must be scoped to the TXN, not to the connection's lifetime.
#[test]
fn reader_inlines_again_after_its_txn_ends() {
    let dir = common::unique_test_dir("i807-after-txn");
    std::fs::create_dir_all(&dir).unwrap();
    let (_guard, port, admin_port) = spawn_moon(&dir);

    let mut b = Client::connect(port);
    assert_eq!(b.resp(&["SET", "k", "v"]), V::Simple("OK".into()));
    assert_eq!(b.resp(&["TXN", "BEGIN"]), V::Simple("OK".into()));
    let (_, inlined_in_txn) = get_inline(&mut b, admin_port, "k");
    assert_eq!(b.resp(&["TXN", "COMMIT"]), V::Simple("OK".into()));
    let (v, inlined_after) = get_inline(&mut b, admin_port, "k");

    assert!(!inlined_in_txn, "a GET inside an open TXN must not inline");
    assert!(
        inlined_after,
        "a GET after TXN COMMIT did not inline — the stand-down outlived the transaction"
    );
    assert_eq!(v, bulk("v"));

    assert_eq!(b.resp(&["TXN", "BEGIN"]), V::Simple("OK".into()));
    assert_eq!(b.resp(&["TXN", "ABORT"]), V::Simple("OK".into()));
    let (_, inlined_after_abort) = get_inline(&mut b, admin_port, "k");
    assert!(
        inlined_after_abort,
        "a GET after TXN ABORT did not inline — the stand-down outlived the transaction"
    );
}

// ---------------------------------------------------------------------------
// Parity for the shape the issue was filed with
// ---------------------------------------------------------------------------

/// A reader OUTSIDE any transaction. The generic leg's filter is gated on the
/// reader's `in_cross_txn()`, so this reader is filtered on neither path; the
/// contract pinned here is only that the two paths AGREE (whatever the engine
/// answers plain clients for an in-flight foreign write, it must not depend
/// on which parser happened to accept the bytes). Whether plain clients
/// should see uncommitted transactional writes at all is the generic filter's
/// design question, not the fast path's — see the module doc.
#[test]
fn non_txn_reader_inline_get_agrees_with_generic_dispatch() {
    let dir = common::unique_test_dir("i807-plain-reader");
    std::fs::create_dir_all(&dir).unwrap();
    let (_guard, port, admin_port) = spawn_moon(&dir);

    let mut a = open_foreign_txn(port);

    let mut b = Client::connect(port);
    let generic = b.text("GET k");
    let (fast, inlined) = get_inline(&mut b, admin_port, "k");
    assert!(
        inlined,
        "CONTROL failed — a plain GET outside any transaction did not inline"
    );
    assert_eq!(
        fast, generic,
        "the inline path and generic dispatch disagree for a non-TXN reader: \
         inline={fast:?} generic={generic:?}"
    );

    assert_eq!(a.resp(&["TXN", "ABORT"]), V::Simple("OK".into()));
    assert_eq!(b.text("GET k"), bulk("original"));
    assert_eq!(b.resp(&["GET", "k"]), bulk("original"));
}
