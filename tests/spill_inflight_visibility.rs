//! Issue #459: a key whose async spill is IN FLIGHT must stay fully visible.
//!
//! `evict_one_async_spill` removes the hot entry as soon as the `SpillRequest`
//! is queued, and the key is only registered in `cold_index` when the
//! completion lands. In between, the key exists in NO plane the database
//! consults — `spill_inflight` is a supersession guard read solely by the
//! completion path, never by reads, deletes, or `logical_len`.
//!
//! The eviction code names the window and accepts it:
//!
//! > Accept a brief read-miss until the completion applies — the key is
//! > safe: it is in the SpillRequest and will be registered once the bg
//! > thread writes [...] AOF incr log is the durability backstop.
//!
//! AOF does backstop *durability*. It does not backstop *visibility*, and a
//! read-miss is not a latency artifact — it is a wrong answer to a client.
//! Measured on `origin/main` @4c9bd2c5 with the config below (400 × 4 KiB
//! against a 512 KiB cap), all three faults reproduce:
//!
//!   * `DBSIZE` answered 124 immediately after the writes were acked, then
//!     climbed to 400 over ~3s with no further writes (this is the
//!     `live 373 / recovered 400` that filed #459);
//!   * `GET k0` → nil and `EXISTS k0` → 0, then 250ms later `GET k0` → 4096
//!     bytes with no intervening write;
//!   * 277 of 400 `DEL`s answered `:0`, and after the queue drained all 277
//!     keys read back — the deletes were acknowledged and then undone. The
//!     completion path inserts into `cold_index` unconditionally, so those
//!     resurrections reach the manifest and survive restart.
//!
//! Wire-level on purpose: the window is a property of the live
//! evict → spill-thread → completion pipeline and cannot be built in a unit
//! test. Scoped to `--disk-offload enable` (opt-in), 1 shard, so every key
//! races the one spill queue.
//!
//! Run with (monoio default — matches the shipped runtime):
//!   cargo build --release
//!   MOON_BIN=$PWD/target/release/moon cargo test --release \
//!     --test spill_inflight_visibility

#![allow(clippy::unwrap_used)]

mod common;

use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use common::find_moon_binary;

// ---------------------------------------------------------------------------
// Server (pattern: tests/dbsize_offload_logical.rs)
// ---------------------------------------------------------------------------

fn test_tmpdir() -> tempfile::TempDir {
    let base =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/spill-459-test-tmp");
    std::fs::create_dir_all(&base).expect("create spill-459-test-tmp base dir");
    tempfile::Builder::new()
        .prefix("spill-459-")
        .tempdir_in(&base)
        .expect("tempdir_in target/spill-459-test-tmp")
}

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

const MAXMEMORY_BYTES: u64 = 512 * 1024; // 512 KiB — forces spill fast.
const N_KEYS: usize = 400;
const VAL_SIZE: usize = 4096; // 400 × 4KiB ≈ 1.6 MiB » 512 KiB cap → heavy spill.

fn spawn_moon_offload(dir: &std::path::Path) -> (ServerGuard, u16) {
    let (child, port) = common::spawn_listening(|port| {
        Command::new(find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                // 1 shard: every key contends the same spill queue, so the
                // in-flight window is wide and the race is not sharded away.
                "--shards",
                "1",
                // Spill is INERT without a durability backstop — the
                // async-spill path bails unless a ShardManifest exists,
                // which needs --appendonly yes.
                "--appendonly",
                "yes",
                "--disk-offload",
                "enable",
                "--maxmemory",
                &MAXMEMORY_BYTES.to_string(),
                "--maxmemory-policy",
                "allkeys-lru",
                "--maxmemory-samples",
                "200",
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard(child), port)
}

// ---------------------------------------------------------------------------
// Minimal RESP client (pattern: tests/dbsize_offload_logical.rs)
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
    fn try_connect(port: u16, window: Duration) -> Option<Client> {
        let addr = ("127.0.0.1", port)
            .to_socket_addrs()
            .ok()?
            .next()
            .expect("resolve loopback");
        let start = Instant::now();
        let stream = loop {
            match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
                Ok(s) => break s,
                Err(_) if start.elapsed() < window => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(_) => return None,
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let writer = stream.try_clone().unwrap();
        Some(Client {
            reader: BufReader::new(stream),
            writer,
        })
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
                V::Arr((0..n).map(|_| self.parse()).collect())
            }
            other => panic!("unexpected RESP type {other:?} (line {line:?})"),
        }
    }

    fn cmd(&mut self, args: &[&[u8]]) -> V {
        self.writer.write_all(&Self::encode(args)).expect("send");
        self.parse()
    }

    /// Send every command, THEN read every reply. Returning from this means
    /// the server acknowledged all of them.
    fn pipeline(&mut self, cmds: &[Vec<Vec<u8>>]) -> Vec<V> {
        let mut out = Vec::with_capacity(cmds.len());
        for c in cmds {
            let refs: Vec<&[u8]> = c.iter().map(|a| a.as_slice()).collect();
            self.writer.write_all(&Self::encode(&refs)).expect("send");
        }
        for _ in cmds {
            out.push(self.parse());
        }
        out
    }

    fn try_ping(&mut self) -> std::io::Result<bool> {
        self.writer.write_all(b"*1\r\n$4\r\nPING\r\n")?;
        let mut buf = [0u8; 7];
        self.reader.read_exact(&mut buf)?;
        Ok(&buf == b"+PONG\r\n")
    }
}

fn readiness_deadline() -> Duration {
    if std::env::var_os("CI").is_some() {
        Duration::from_secs(120)
    } else {
        Duration::from_secs(30)
    }
}

fn wait_ready(guard: &mut ServerGuard, dir: &std::path::Path, port: u16) -> Client {
    let deadline = Instant::now() + readiness_deadline();
    loop {
        if let Ok(Some(status)) = guard.0.try_wait() {
            let tail = std::fs::read_to_string(dir.join("moon.stderr.log"))
                .unwrap_or_else(|e| format!("<unreadable: {e}>"));
            panic!("moon exited {status} before ready; stderr tail:\n{tail}");
        }
        if let Some(mut c) = Client::try_connect(port, Duration::from_secs(2))
            && c.try_ping().unwrap_or(false)
        {
            return c;
        }
        assert!(
            Instant::now() < deadline,
            "moon never answered PING on port {port}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn key_name(i: usize) -> Vec<u8> {
    format!("k{i}").into_bytes()
}

/// Write every key and read every reply, so all writes are ACKED on return.
/// Whatever the server does with them afterwards, it already promised the
/// client these keys exist.
fn load_keys(c: &mut Client) {
    let val = vec![b'x'; VAL_SIZE];
    let cmds: Vec<Vec<Vec<u8>>> = (0..N_KEYS)
        .map(|i| vec![b"SET".to_vec(), key_name(i), val.clone()])
        .collect();
    for (i, r) in c.pipeline(&cmds).into_iter().enumerate() {
        // A refused SET is an ENVIRONMENT failure, not a #459 finding: on a
        // slow disk the spill queue backs up, `try_send` fails in
        // `evict_one_async_spill`, and `evict_to_budget` surfaces OOM. Say so
        // explicitly so the run is not misread as the defect reappearing.
        if let V::Err(ref e) = r {
            panic!(
                "SET k{i} was refused ({e}). The spill queue could not keep up with the \
                 write burst, so this run cannot say anything about #459 — it never \
                 reached the in-flight window. Re-run on a less loaded machine."
            );
        }
        assert_eq!(r, V::Simple("OK".into()), "SET k{i} was not acknowledged");
    }
}

/// One `INFO persistence` counter, or `None` when the field is absent (the
/// pre-fix binary has no `spill_completion_superseded`, and saying "absent"
/// beats defaulting to 0 and silently satisfying a guard).
fn info_counter(c: &mut Client, field: &str) -> Option<u64> {
    let V::Bulk(body) = c.cmd(&[b"INFO", b"persistence"]) else {
        return None;
    };
    let text = String::from_utf8_lossy(&body);
    let prefix = format!("{field}:");
    text.lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .and_then(|v| v.trim().parse().ok())
}

/// Total spill work the server reports having COMPLETED.
///
/// `spill_batches_flushed` counts batches the background thread wrote;
/// `spill_completion_superseded` counts completions the event loop applied
/// and refused to publish. Together they move whenever the pipeline makes
/// progress, which `DBSIZE` alone does not.
fn spill_progress(c: &mut Client) -> u64 {
    info_counter(c, "spill_batches_flushed").unwrap_or(0)
        + info_counter(c, "spill_completion_superseded").unwrap_or(0)
}

/// Wait for the spill pipeline — queue AND completion application — to go
/// quiet.
///
/// Gating on a stable `DBSIZE` alone is NOT sufficient, and is unsound in the
/// direction that matters. After the deletion test's `DEL` sweep every key is
/// either deleted or in-flight-invisible, so `DBSIZE` reads 0 immediately
/// while completions are still queued; a stability gate can return before any
/// completion is applied, and a completion that republishes a deleted key
/// then lands AFTER the assertions have run. That is a FALSE PASS hiding the
/// exact P0 this file exists to catch. (It happened to catch it anyway,
/// because completions land inside the poll's ~1s floor — luck, not a
/// guarantee.)
///
/// So gate on server-reported spill PROGRESS as well: hold until neither the
/// progress counters nor `DBSIZE` have moved for several consecutive samples.
/// A fixed sleep is avoided for the same reason as before — too short fails
/// on a slow disk for a reason that is not the defect.
fn drain(c: &mut Client) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = (i64::MIN, u64::MAX);
    let mut stable = 0;
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(250));
        let V::Int(n) = c.cmd(&[b"DBSIZE"]) else {
            continue;
        };
        let now = (n, spill_progress(c));
        if now == last {
            stable += 1;
            // Six consecutive identical samples ≈ 1.5s with BOTH signals
            // still. The spill thread flushes on a sub-second cadence, so a
            // pipeline with anything left in it moves one of them.
            if stable >= 6 {
                return;
            }
        } else {
            stable = 0;
            last = now;
        }
    }
}

// ---------------------------------------------------------------------------
// Fault 1 — a live key must never be denied
// ---------------------------------------------------------------------------

/// A key that answers nil and then answers a value on retry, with no write
/// in between, was denied while it existed. Retry-and-compare is the
/// unambiguous form: it needs no knowledge of which keys spilled, and it
/// cannot be explained by legitimate `allkeys-lru` eviction — an evicted key
/// stays gone, it does not come back on its own.
#[test]
fn a_key_that_answers_nil_must_not_come_back_on_a_bare_retry() {
    let dir = test_tmpdir();
    let (mut guard, port) = spawn_moon_offload(dir.path());
    let mut c = wait_ready(&mut guard, dir.path(), port);

    load_keys(&mut c);

    let mut liars = Vec::new();
    for i in 0..N_KEYS {
        let k = key_name(i);
        if c.cmd(&[b"GET", &k]) != V::Null {
            continue;
        }
        let existed = c.cmd(&[b"EXISTS", &k]);
        std::thread::sleep(Duration::from_millis(250));
        if let V::Bulk(v) = c.cmd(&[b"GET", &k]) {
            liars.push((i, existed.clone(), v.len()));
        }
    }

    assert!(
        liars.is_empty(),
        "{} key(s) answered nil and then returned a value on a bare retry, \
         with no write in between — the server denied a key it was holding. \
         First offenders (key, EXISTS at the denial, bytes on retry): {:?}",
        liars.len(),
        &liars[..liars.len().min(5)]
    );
}

// ---------------------------------------------------------------------------
// Fault 2 — an acknowledged DEL must be final
// ---------------------------------------------------------------------------

/// The severe one. DEL every key while spills are still in flight, then let
/// the queue drain. A key that reads back afterwards was resurrected by the
/// completion path, which inserts into `cold_index` unconditionally — so the
/// resurrection is committed to the manifest, not transient.
///
/// Asserted on readability rather than on the DEL return codes: a DEL that
/// answers `:0` for a live key is the same defect, but a client that deleted
/// data and got it back is the harm worth pinning.
#[test]
fn a_deleted_key_must_not_come_back_after_the_spill_queue_drains() {
    let dir = test_tmpdir();
    let (mut guard, port) = spawn_moon_offload(dir.path());
    let mut c = wait_ready(&mut guard, dir.path(), port);

    load_keys(&mut c);

    // Immediately, while spills are still queued.
    let dels: Vec<Vec<Vec<u8>>> = (0..N_KEYS)
        .map(|i| vec![b"DEL".to_vec(), key_name(i)])
        .collect();
    let denied = c
        .pipeline(&dels)
        .into_iter()
        .filter(|r| *r == V::Int(0))
        .count();

    drain(&mut c);

    let gets: Vec<Vec<Vec<u8>>> = (0..N_KEYS)
        .map(|i| vec![b"GET".to_vec(), key_name(i)])
        .collect();
    let resurrected: Vec<usize> = c
        .pipeline(&gets)
        .into_iter()
        .enumerate()
        .filter_map(|(i, r)| (r != V::Null).then_some(i))
        .collect();

    let dbsize = c.cmd(&[b"DBSIZE"]);

    // Vacuity guard. "Nothing came back" is only meaningful if the spill
    // completions actually ARRIVED and were refused — otherwise a run where
    // the queue never drained, or never spilled at all, passes while proving
    // nothing. `spill_completion_superseded` counts completions the event
    // loop applied and declined to publish because DEL had retired their
    // in-flight record, which is precisely the mechanism under test.
    let superseded = info_counter(&mut c, "spill_completion_superseded");
    assert!(
        resurrected.is_empty(),
        "{} key(s) were readable again after DEL + drain (DBSIZE={:?}); \
         {denied} DEL(s) had answered :0. A key deleted by an acknowledged \
         command came back on its own. First: {:?}",
        resurrected.len(),
        dbsize,
        &resurrected[..resurrected.len().min(8)]
    );
    assert_eq!(
        dbsize,
        V::Int(0),
        "every key was deleted, so DBSIZE must be 0"
    );
    assert!(
        superseded.is_some_and(|n| n > 0),
        "the run proved nothing: spill_completion_superseded={superseded:?}, so no \
         completion was observed arriving and being refused. Either the DELs never \
         raced an in-flight spill (nothing spilled, or the queue drained first), or \
         the drain returned early. A green result here would not mean deletes are \
         final — it would mean the window was never entered."
    );
}

// ---------------------------------------------------------------------------
// Fault 3 — the counter must not under-report acked writes
// ---------------------------------------------------------------------------

/// The symptom that filed #459. Every SET was acknowledged before DBSIZE is
/// asked, so no key may be missing from the count: `logical_len` sums hot +
/// cold and an in-flight key is in neither.
///
/// Read twice with a drain between: a count that RISES with no writes in
/// between is proof the first answer was wrong, and distinguishes this from
/// legitimate eviction (which can only lower a count, never raise it).
#[test]
fn dbsize_must_not_undercount_keys_whose_spill_is_in_flight() {
    let dir = test_tmpdir();
    let (mut guard, port) = spawn_moon_offload(dir.path());
    let mut c = wait_ready(&mut guard, dir.path(), port);

    load_keys(&mut c);

    let immediate = c.cmd(&[b"DBSIZE"]);
    drain(&mut c);
    let settled = c.cmd(&[b"DBSIZE"]);

    let (V::Int(immediate), V::Int(settled)) = (immediate.clone(), settled.clone()) else {
        panic!("DBSIZE must answer an integer, got {immediate:?} / {settled:?}");
    };

    assert!(
        immediate >= settled,
        "DBSIZE rose from {immediate} to {settled} with no writes in between — \
         the first answer omitted {} key(s) whose spill was still in flight. \
         Eviction can only lower a key count; nothing legitimate raises one.",
        settled - immediate
    );
    assert_eq!(
        immediate, N_KEYS as i64,
        "all {N_KEYS} SETs were acknowledged before DBSIZE was asked, so every \
         key must be counted (settled count was {settled})"
    );
}
