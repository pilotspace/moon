//! moon#476: a restarting server must tell clients it is loading, not hang.
//!
//! Measured before this contract existed (moon-dev, 83,828 keys):
//!
//! ```text
//! connect() succeeded at     148.2 ms
//! PING sent at               148.3 ms
//! first byte back at        1376.6 ms   -> 1228 ms of silence, then +PONG
//! ```
//!
//! The per-shard listener is created and its accept task spawned BEFORE index
//! recovery, but recovery is fully synchronous, so on a single-threaded shard
//! runtime neither the accept task nor the event loop is ever scheduled until
//! it returns. The kernel completes the handshake from the backlog by itself,
//! which is why `connect()` succeeds — the client believes it holds a healthy
//! connection and then waits out the whole recovery with no way to tell that
//! state from a wedged process.
//!
//! The contract pinned here is Redis's: while the dataset is loading, data
//! commands are REFUSED with `-LOADING` and the connection stays live, so a
//! client can fail over immediately instead of blocking. `PING` still answers,
//! which is what proves the event loop is actually running rather than the
//! reply merely arriving after recovery finished.
//!
//! Note the key counts: recovery must take long enough to sample. If a future
//! change makes recovery so fast the window closes, this test fails LOUDLY
//! (never observed -LOADING) rather than passing vacuously.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const KEYS: usize = 60_000;

fn unique_dir(suffix: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "moon-loading-476-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ))
}

fn enc(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Read one RESP reply's first line. Enough to classify `+OK` / `-LOADING` /
/// `$<n>` — this test only ever asks "which kind of reply is this?".
fn read_line(s: &mut TcpStream) -> std::io::Result<String> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = s.read(&mut byte)?;
        if n == 0 {
            return Err(std::io::Error::other("closed"));
        }
        if byte[0] == b'\n' {
            break;
        }
        if byte[0] != b'\r' {
            buf.push(byte[0]);
        }
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

fn spawn(dir: &std::path::Path, port: u16) -> Child {
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--dir",
            dir.to_str().expect("dir utf8"),
            "--appendonly",
            "yes",
            // Crash-harness convention: the diskfull guard would otherwise
            // refuse writes on a nearly-full volume and gut the test.
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn moon")
}

/// Kills the server when it leaves scope — including when the scope is left by
/// an unwinding panic.
///
/// A test that calls `kill(child)` on its last line kills nothing when an
/// earlier `assert!` fires: the panic unwinds straight past it and the server
/// is reparented to init. Measured 2026-08-25, after a round of deliberately
/// failing mutation runs of this very file: five orphans, each burning ~170%
/// CPU (834% combined) for 7.5 hours, answering nothing on their ports and
/// spending the time in syscalls (13:42 system vs 0:09 user). The cost is not
/// just the cores — every wall-clock-sensitive test that ran afterwards did so
/// on a machine under invisible load.
///
/// Deliberately kills rather than asking politely: these are throwaway data
/// dirs under `/tmp`, and a hung server (the exact thing this file tests for)
/// would ignore a graceful shutdown.
struct ServerGuard(Option<Child>);

impl ServerGuard {
    fn new(c: Child) -> Self {
        Self(Some(c))
    }

    /// Kill now, when the test needs the server *gone* rather than merely
    /// doomed — here, so the store is closed before it is reopened.
    fn kill_now(mut self) {
        self.terminate();
    }

    fn terminate(&mut self) {
        if let Some(mut c) = self.0.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        self.terminate();
    }
}

/// Build a store with a TEXT index over `KEYS` hashes, so the restart has real
/// index-reconcile work to do, and leave it on disk.
fn build_store(dir: &std::path::Path, port: u16) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let child = ServerGuard::new(spawn(dir, port));
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut s = loop {
        assert!(Instant::now() < deadline, "build server never accepted");
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
            let mut s = s;
            if s.write_all(&enc(&[b"PING"])).is_ok()
                && read_line(&mut s).map(|l| l == "+PONG").unwrap_or(false)
            {
                break s;
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    };

    // A VECTOR index, not TEXT: `text-index` is absent from the tokio CI
    // feature set (`--no-default-features --features runtime-tokio,jemalloc`),
    // where a TEXT schema is refused with "TEXT fields require the text-index
    // feature". Vector indexes are unconditional, so this test runs on every
    // leg instead of silently skipping the one the PR gate uses.
    s.write_all(&enc(&[
        b"FT.CREATE",
        b"px",
        b"ON",
        b"HASH",
        b"PREFIX",
        b"1",
        b"px:",
        b"SCHEMA",
        b"emb",
        b"VECTOR",
        b"HNSW",
        b"6",
        b"TYPE",
        b"FLOAT32",
        b"DIM",
        b"8",
        b"DISTANCE_METRIC",
        b"L2",
    ]))
    .expect("write FT.CREATE");
    assert_eq!(read_line(&mut s).expect("FT.CREATE reply"), "+OK");

    // Pipelined in batches so the build is a couple of seconds, not minutes.
    for base in (0..KEYS).step_by(2_000) {
        let mut batch = Vec::new();
        let hi = (base + 2_000).min(KEYS);
        for j in base..hi {
            let key = format!("px:{j}");
            let body = format!("document number {j} alpha beta term{j}");
            let mut emb = Vec::with_capacity(32);
            for k in 0..8u32 {
                emb.extend_from_slice(&(((j as u32 + k) % 11) as f32 * 0.25).to_le_bytes());
            }
            batch.extend_from_slice(&enc(&[
                b"HSET",
                key.as_bytes(),
                b"body",
                body.as_bytes(),
                b"emb",
                &emb,
            ]));
        }
        s.write_all(&batch).expect("write batch");
        for _ in base..hi {
            read_line(&mut s).expect("HSET reply");
        }
    }

    s.write_all(&enc(&[b"BGREWRITEAOF"]))
        .expect("write bgrewrite");
    let _ = read_line(&mut s);
    std::thread::sleep(Duration::from_millis(500));
    drop(s);
    child.kill_now();
}

/// moon#476 acceptance: while indexes rebuild, data commands say `-LOADING`
/// and the connection stays live enough to answer `PING`.
#[test]
fn lst476_loading_is_reported_not_hidden_behind_a_hang() {
    let dir = unique_dir("gate");
    // Build the store on its own port FIRST. `spawn_listening` may retry its
    // closure on a different port, and a second build against the same dir
    // would hit "Index already exists" -- so only the restart goes through it.
    build_store(&dir, common::reserve_port());
    let (child, port) = common::spawn_listening(|p| spawn(&dir, p));
    let child = ServerGuard::new(child);

    // Connect as early as the kernel allows and sample continuously. Both
    // replies come from ONE connection so a -LOADING and a +PONG observed
    // together prove the same live connection is being served.
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut s = loop {
        assert!(
            Instant::now() < deadline,
            "restart never accepted a connection"
        );
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)) {
            break s;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    s.set_read_timeout(Some(Duration::from_secs(120)))
        .expect("set read timeout");

    let mut saw_loading = false;
    let mut ping_answered_while_loading = false;
    let mut settled_value = None;

    let t0 = Instant::now();
    let mut first_reply_ms = None;
    while Instant::now() - t0 < Duration::from_secs(120) {
        s.write_all(&enc(&[b"HGET", b"px:1", b"body"]))
            .expect("write HGET");
        let reply = read_line(&mut s).expect("HGET reply");
        first_reply_ms.get_or_insert_with(|| (Instant::now() - t0).as_millis());
        if reply.starts_with("-LOADING") {
            saw_loading = true;
            // Same connection, immediately: a live event loop must still
            // answer PING. If this hangs, nothing is being served and the
            // -LOADING above would have been the only thing working.
            s.write_all(&enc(&[b"PING"])).expect("write PING");
            if read_line(&mut s).expect("PING reply") == "+PONG" {
                ping_answered_while_loading = true;
            }
        } else {
            settled_value = Some(reply);
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }

    // The two ways this can fail look identical unless the timing is reported:
    // a first reply that took hundreds of ms means the window WAS open and the
    // client was hung through it (gate missing); a first reply that came back
    // instantly means recovery outran the sampler (window closed).
    let first_ms = first_reply_ms.unwrap_or(0);
    assert!(
        saw_loading,
        "never observed -LOADING during a {KEYS}-key index recovery; the first \
         reply took {first_ms} ms. If that is large, the client was HUNG through \
         the window and the -LOADING gate is missing. If it is ~0, recovery \
         outran the sampler — raise KEYS rather than deleting this assertion, \
         since a vacuous pass here hides exactly the hang this test exists to \
         catch."
    );
    assert!(
        ping_answered_while_loading,
        "-LOADING was returned but PING did not answer on the same connection: \
         the loading gate is in place without the event loop actually running, \
         which is still a hang for every other client."
    );
    // Length computed, not hardcoded: the bulk header must match the body
    // this test actually wrote.
    let expected = format!("${}", "document number 1 alpha beta term1".len());
    assert_eq!(
        settled_value.as_deref(),
        Some(expected.as_str()),
        "after recovery the value must be served normally"
    );

    child.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A panicking test must not leave its server behind.
///
/// This is the regression test for how this file produced five 170%-CPU
/// orphans in one afternoon: the kill lived on the last line of the test, and
/// a failing assert never reached it. The check is on the PROCESS, not on the
/// port — a dead server's port frees up either way, so a connect-refused proves
/// nothing about whether anything is still running.
#[test]
fn lst476_a_panicking_test_does_not_orphan_its_server() {
    let dir = std::env::temp_dir().join(format!(
        "moon-loading-476-{}-orphan-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("mkdir");

    let port = common::reserve_port();
    let child = spawn(&dir, port);
    let pid = child.id();
    let guard = ServerGuard::new(child);

    // Exactly the shape of a failing assertion mid-test.
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _guard = guard;
        panic!("a test failing before its explicit kill");
    }));
    assert!(outcome.is_err(), "the panic must actually have happened");

    // `kill` + `wait` are synchronous, so by the time the unwind completes the
    // child is reaped and its pid no longer names a live process.
    let alive = std::process::Command::new("ps")
        .args(["-p", &pid.to_string()])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).lines().count() > 1)
        .unwrap_or(false);
    assert!(
        !alive,
        "server pid {pid} survived a panicking test — this is how orphans that \
         spin at ~170% CPU for hours get created"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
