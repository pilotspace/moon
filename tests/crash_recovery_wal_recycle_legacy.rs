//! Task #43 (P1 data loss): autovacuum Pass C (`src/shard/autovacuum.rs`)
//! calls `WalWriterV3::recycle_aggressive(redo_lsn = wal.current_lsn())`
//! whenever total on-disk WAL bytes exceed `--max-wal-size`, REGARDLESS of
//! whether disk-offload (the only mode with a periodic checkpoint/graph
//! snapshot protocol) is enabled.
//!
//! In legacy mode (`--disk-offload disable`) there is no periodic plane
//! snapshot: `save_graph_store` runs only on graceful shutdown
//! (`src/shard/event_loop.rs` shutdown branch), and the workspace/MQ/temporal
//! registries have NO snapshot format at all — `replay_workspace_wal` /
//! `replay_mq_wal` / `replay_temporal_wal` rebuild them purely by re-scanning
//! the WAL. `current_lsn()` is the LSN *about to be assigned* (i.e. "assume
//! everything is durable already"), so `recycle_aggressive` deletes every
//! sealed segment once the ceiling is breached — including segments whose
//! records have no other durable form. A kill -9 after that recycle loses
//! that data permanently, even though replay (task #42, PR #286) now works
//! correctly for the records that remain.
//!
//! This test proves the loss end to end against a REAL server process
//! (pattern: `tests/crash_recovery_graph_durability.rs` G1/G4/G5): tiny
//! `--wal-segment-size` / `--max-wal-size` force several Pass C recycle
//! passes within seconds via a fast `--autovacuum-interval-secs`.
//!
//! Two independent assertions, deliberately kept separate:
//!
//! 1. **Byte-level survival** (the assertion scoped exactly to task #43):
//!    after Pass C has had several chances to run, the WAL bytes for the
//!    earliest graph write AND the earliest workspace write must still be
//!    present on disk somewhere under `wal-v3/`. This is what
//!    `recycle_aggressive` vs. "skip and warn" actually controls, and it is
//!    provable without depending on any replay/reconstruction logic.
//! 2. **Round-trip survival** (kill -9 + restart): the workspace must still
//!    be listed by `WS LIST`. `replay_workspace_wal` rebuilds the registry
//!    purely from the WAL (no snapshot format exists for it — the exact
//!    plane the task's design guidance called out), so this is an
//!    end-to-end proof that legacy mode no longer loses it.
//!
//! Deliberately NOT asserted: full graph reconstruction via `GRAPH.QUERY`
//! after restart. Probing independently (see task notes) showed that graph
//! command replay in legacy (non-disk-offload) mode does not reconstruct
//! the graph even with default `--max-wal-size` and zero Pass C recycling —
//! a separate, pre-existing gap unrelated to WAL recycling (the WAL bytes
//! for the graph write are provably still on disk; the replay pipeline
//! just doesn't consume them into a `GraphStore` in this mode). Assertion 1
//! above still proves task #43 is fixed for the graph plane's data at the
//! byte level, which is the layer Pass C operates on.
//!
//! Run with (monoio default — matches CI):
//!   cargo build --release
//!   cargo test --release --test crash_recovery_wal_recycle_legacy -- --ignored

#![allow(clippy::unwrap_used)]

use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Binary resolution (pattern: crash_recovery_graph_durability.rs)
// ---------------------------------------------------------------------------

fn find_moon_binary() -> PathBuf {
    if let Ok(bin) = std::env::var("MOON_BIN") {
        let p = PathBuf::from(bin);
        if p.exists() {
            return p;
        }
    }
    let manifest = env!("CARGO_MANIFEST_DIR");
    let release = PathBuf::from(format!("{manifest}/target/release/moon"));
    if release.exists() {
        return release;
    }
    let debug = PathBuf::from(format!("{manifest}/target/debug/moon"));
    if debug.exists() {
        return debug;
    }
    panic!(
        "No moon binary found. Build with `cargo build --release` or set \
         MOON_BIN=/path/to/moon."
    );
}

fn unique_port() -> u16 {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind :0");
    let port = listener.local_addr().expect("local_addr").port();
    drop(listener);
    port
}

fn unique_dir(suffix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "moon-walrecycle-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ))
}

// ---------------------------------------------------------------------------
// Server spawn / crash / restart harness (pattern: crash_recovery_graph_durability.rs)
// ---------------------------------------------------------------------------

struct ServerGuard {
    child: Child,
    dir_marker: String,
}

impl ServerGuard {
    fn new(child: Child, dir: &Path) -> Self {
        Self {
            child,
            dir_marker: dir.to_string_lossy().into_owned(),
        }
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            sigkill(&mut self.child);
        } else {
            let _ = self.child.wait();
        }
        let _ = Command::new("pkill")
            .args(["-9", "-f"])
            .arg(&self.dir_marker)
            .output();
    }
}

#[cfg(unix)]
fn sigkill(child: &mut Child) {
    let pid = child.id() as i32;
    // SAFETY: `pid` is the live child's own process id (owned `Child`, not yet
    // waited), so the SIGKILL targets exactly the server this guard spawned;
    // `libc::kill` has no memory-safety preconditions beyond a valid signal.
    unsafe {
        libc::kill(pid, libc::SIGKILL);
    }
    let _ = child.wait();
}

#[cfg(not(unix))]
fn sigkill(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// Wait until nothing is accepting on `port` (SO_REUSEPORT makes a plain
/// bind-check useless; two consecutive refused connects = down).
fn wait_for_port_down(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    let mut consecutive_refused = 0;
    for _ in 0..120 {
        match TcpStream::connect_timeout(&addr.parse().expect("addr"), Duration::from_millis(100)) {
            Ok(_) => {
                consecutive_refused = 0;
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(_) => {
                consecutive_refused += 1;
                if consecutive_refused >= 2 {
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

/// Spawn moon in LEGACY mode (`--disk-offload disable` — no checkpoint
/// protocol, no periodic graph snapshot) with a tiny WAL ceiling so Pass C
/// fires within seconds instead of needing 256 MB of writes.
fn spawn_moon_legacy_fast_recycle(port: u16, dir: &Path) -> ServerGuard {
    std::fs::create_dir_all(dir).expect("create test dir");
    let child = Command::new(find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--appendonly",
            "yes",
            "--disk-offload",
            "disable",
            "--disk-free-min-pct",
            "0",
            "--wal-segment-size",
            "32kb",
            "--max-wal-size",
            "96kb",
            "--autovacuum-interval-secs",
            "1",
            "--dir",
        ])
        .arg(dir)
        .env("RUST_LOG", "moon=info")
        .stdout(
            std::fs::File::options()
                .append(true)
                .create(true)
                .open(dir.join("moon.stdout.log"))
                .expect("stdout log"),
        )
        .stderr(
            std::fs::File::options()
                .append(true)
                .create(true)
                .open(dir.join("moon.stderr.log"))
                .expect("stderr log"),
        )
        .spawn()
        .expect("spawn moon (run `cargo build --release` first, or set MOON_BIN)");
    ServerGuard::new(child, dir)
}

const RESTART_ATTEMPTS: usize = 6;

fn start_moon_alive(port: u16, dir: &Path) -> ServerGuard {
    for attempt in 1..=RESTART_ATTEMPTS {
        let mut guard = spawn_moon_legacy_fast_recycle(port, dir);
        let mut up = false;
        for _ in 0..300 {
            if let Ok(Some(_status)) = guard.child.try_wait() {
                break; // self-terminated (rebind race) — retry
            }
            if TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
                up = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        if up {
            return guard;
        }
        drop(guard);
        if attempt < RESTART_ATTEMPTS {
            std::thread::sleep(Duration::from_millis(300));
        }
    }
    panic!("moon failed to start+serve on port {port} after {RESTART_ATTEMPTS} attempts");
}

fn crash(mut guard: ServerGuard, port: u16) {
    sigkill(&mut guard.child);
    drop(guard); // idempotent (already reaped) + pkill backstop
    wait_for_port_down(port);
}

/// Poll every `*.wal` file under `<dir>/shard-0/wal-v3/` until `needle`
/// appears somewhere on disk — the bounded, condition-based "my last write
/// reached the WAL fd" wait that makes the recycle race deterministic.
fn wait_for_wal_v3_bytes(dir: &Path, needle: &[u8]) {
    let wal_dir = dir.join("shard-0").join("wal-v3");
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(entries) = std::fs::read_dir(&wal_dir) {
            for entry in entries.flatten() {
                if let Ok(bytes) = std::fs::read(entry.path())
                    && bytes.windows(needle.len()).any(|w| w == needle)
                {
                    return;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "WAL v3 dir {} never contained marker {:?} — writes are not \
             reaching the v3 WAL",
            wal_dir.display(),
            String::from_utf8_lossy(needle),
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Total bytes across every `*.wal` segment under `<dir>/shard-0/wal-v3/`.
fn wal_v3_total_bytes(dir: &Path) -> u64 {
    let wal_dir = dir.join("shard-0").join("wal-v3");
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(&wal_dir) {
        for entry in entries.flatten() {
            total += std::fs::metadata(entry.path())
                .map(|m| m.len())
                .unwrap_or(0);
        }
    }
    total
}

/// One-shot (non-blocking) check: does `needle` appear in any `*.wal`
/// segment under `<dir>/shard-0/wal-v3/` right now?
fn wal_v3_contains_bytes(dir: &Path, needle: &[u8]) -> bool {
    let wal_dir = dir.join("shard-0").join("wal-v3");
    if let Ok(entries) = std::fs::read_dir(&wal_dir) {
        for entry in entries.flatten() {
            if let Ok(bytes) = std::fs::read(entry.path())
                && bytes.windows(needle.len()).any(|w| w == needle)
            {
                return true;
            }
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Minimal RESP client (pattern: crash_recovery_graph_durability.rs)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Clone)]
enum V {
    Simple(String),
    Err(String),
    Int(i64),
    Bulk(Vec<u8>),
    Arr(Vec<V>),
    Null,
}

struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Client {
    fn connect(port: u16) -> Self {
        let addr = format!("127.0.0.1:{port}")
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();
        let start = Instant::now();
        let stream = loop {
            match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
                Ok(s) => break s,
                Err(_) if start.elapsed() < Duration::from_secs(30) => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => panic!("server never accepted on port {port}: {e}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let writer = stream.try_clone().unwrap();
        Client {
            reader: BufReader::new(stream),
            writer,
        }
    }

    fn encode(args: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    fn cmd(&mut self, args: &[&[u8]]) -> V {
        self.writer.write_all(&Self::encode(args)).expect("write");
        self.parse()
    }

    fn read_line(&mut self) -> String {
        let mut line = Vec::new();
        let mut b = [0u8; 1];
        loop {
            self.reader.read_exact(&mut b).expect("read byte");
            if b[0] == b'\n' {
                break;
            }
            if b[0] != b'\r' {
                line.push(b[0]);
            }
        }
        String::from_utf8_lossy(&line).into_owned()
    }

    fn parse(&mut self) -> V {
        let line = self.read_line();
        let (t, rest) = line.split_at(1);
        match t {
            "+" => V::Simple(rest.to_string()),
            "-" => V::Err(rest.to_string()),
            ":" => V::Int(rest.parse().expect("int")),
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
            "*" => {
                let n: i64 = rest.parse().expect("arr len");
                if n < 0 {
                    return V::Null;
                }
                (0..n).map(|_| self.parse()).collect::<Vec<_>>().into()
            }
            "%" => {
                let n: i64 = rest.parse().expect("map len");
                (0..n * 2).map(|_| self.parse()).collect::<Vec<_>>().into()
            }
            "_" => V::Null,
            other => panic!("unexpected RESP type byte {other:?} (line {line:?})"),
        }
    }
}

impl From<Vec<V>> for V {
    fn from(v: Vec<V>) -> Self {
        V::Arr(v)
    }
}

const G: &[u8] = b"walrecycleg";

// ---------------------------------------------------------------------------
// Task #43: legacy-mode Pass C must not lose the sole durable copy of
// graph / workspace history.
// ---------------------------------------------------------------------------

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn t43_legacy_mode_pass_c_must_not_lose_ws_and_graph_history() {
    let port = unique_port();
    let dir = unique_dir("t43");
    let guard = start_moon_alive(port, &dir);
    let mut c = Client::connect(port);

    // 1. Earliest writes: a workspace (registry has NO snapshot format —
    //    replay_workspace_wal rebuilds it purely from the WAL) and a graph
    //    node with a unique marker property. Both land in the FIRST WAL
    //    segment, which — once the ceiling is breached below — is the
    //    first one Pass C's `recycle_aggressive` deletes.
    let ws_id = match c.cmd(&[b"WS", b"CREATE", b"legacy-ws-t43"]) {
        V::Bulk(id) => id,
        other => panic!("WS CREATE failed: {other:?}"),
    };

    assert_eq!(
        c.cmd(&[b"GRAPH.CREATE", G]),
        V::Simple("OK".into()),
        "GRAPH.CREATE"
    );
    match c.cmd(&[b"GRAPH.ADDNODE", G, b"Early", b"tag", b"EARLY-MARKER-T43"]) {
        V::Int(_) => {}
        other => panic!("early marker ADDNODE failed: {other:?}"),
    }
    wait_for_wal_v3_bytes(&dir, b"EARLY-MARKER-T43");
    wait_for_wal_v3_bytes(&dir, b"legacy-ws-t43");

    // Sanity: the workspace is visible right now, pre-recycle.
    match c.cmd(&[b"WS", b"LIST"]) {
        V::Arr(entries) => assert!(
            entries.iter().any(|e| matches!(e, V::Arr(cells)
                if cells.first() == Some(&V::Bulk(ws_id.clone())))),
            "workspace not visible immediately after WS CREATE: {entries:?}"
        ),
        other => panic!("WS LIST failed: {other:?}"),
    }

    // 2. Blow past `--max-wal-size 96kb` with padded ADDNODE writes so the
    //    early segment rotates out and becomes eligible for recycling.
    //    2KB padding * 300 nodes ~= 600KB, several multiples of the 96KB
    //    ceiling and the 32KB segment size (~18+ rotations).
    let padding = "x".repeat(2048);
    for i in 0..300u64 {
        match c.cmd(&[
            b"GRAPH.ADDNODE",
            G,
            b"Bulk",
            b"pad",
            padding.as_bytes(),
            b"id",
            i.to_string().as_bytes(),
        ]) {
            V::Int(_) => {}
            other => panic!("bulk ADDNODE {i} failed: {other:?}"),
        }
    }

    // Confirm we actually forced growth past the ceiling (test-setup sanity,
    // not the bug assertion itself).
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if wal_v3_total_bytes(&dir) > 96 * 1024 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "WAL never exceeded the 96KB ceiling — test setup did not \
             generate enough volume to exercise Pass C"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // 3. Give the 1s autovacuum tick several chances to run Pass C.
    std::thread::sleep(Duration::from_secs(5));

    // 4. Assertion 1 (byte-level, scoped exactly to task #43): the earliest
    //    WS + graph records must still be on disk somewhere under wal-v3/.
    //    This is what `recycle_aggressive` vs. "skip and warn" controls,
    //    independent of any replay/reconstruction correctness.
    assert!(
        wal_v3_contains_bytes(&dir, b"legacy-ws-t43"),
        "task #43 REGRESSION: the WorkspaceCreate record for \
         'legacy-ws-t43' was deleted from every on-disk WAL segment — Pass \
         C recycled it in legacy mode with no checkpoint/snapshot floor, \
         and the workspace registry has no other durable form"
    );
    assert!(
        wal_v3_contains_bytes(&dir, b"EARLY-MARKER-T43"),
        "task #43 REGRESSION: the earliest graph node's WAL record was \
         deleted from every on-disk WAL segment — Pass C recycled it in \
         legacy mode with no checkpoint/graph-snapshot floor"
    );

    // 5. Kill -9 — NO graceful shutdown, so `save_graph_store` never runs
    //    and the WAL is the only durable form of the early writes.
    crash(guard, port);

    // 6. Assertion 2 (round-trip): restart and prove the workspace registry
    //    — which replays purely from the WAL, no snapshot format exists —
    //    survived. (Full graph reconstruction via GRAPH.QUERY is
    //    deliberately not asserted here; see the module doc comment.)
    let guard = start_moon_alive(port, &dir);
    let mut c = Client::connect(port);

    let ws_list = match c.cmd(&[b"WS", b"LIST"]) {
        V::Arr(entries) => entries,
        other => panic!("WS LIST failed: {other:?}"),
    };
    let ws_survived = ws_list.iter().any(|entry| match entry {
        V::Arr(cells) => cells.first() == Some(&V::Bulk(ws_id.clone())),
        _ => false,
    });
    assert!(
        ws_survived,
        "task #43 REGRESSION: workspace 'legacy-ws-t43' (id {:?}) did not \
         survive kill-9 — Pass C recycled the WAL segment holding its \
         WorkspaceCreate record, and the workspace registry has no other \
         durable form. WS LIST after recovery: {ws_list:?}",
        String::from_utf8_lossy(&ws_id),
    );

    drop(guard);
}
