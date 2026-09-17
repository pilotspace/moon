//! `CLIENT TRACKING` invalidation must cover movablekeys commands (#582).
//!
//! A movablekeys command is one whose keys are not at a fixed argument
//! position; those carry `first_key: 0` in `COMMAND_META`, mirroring redis's
//! own table. `tracking::invalidation::command_keys` read that as "this
//! command has no keys" and returned an empty list, so:
//!
//!   * a movablekeys **read** (`SINTERCARD`, `ZDIFF`, `XREAD`, ...) never
//!     registered the client, which then cached a value it would never be told
//!     about; and
//!   * a movablekeys **write** (`LMPOP`, `ZMPOP`, ...) never pushed an
//!     `invalidate`, so every tracker's copy went stale **permanently**.
//!
//! Client-side caching is a correctness contract — the client may serve its
//! cached value until told otherwise — so a missed invalidation is unbounded
//! wrong data, and it is invisible to the client. That is why this is pinned
//! on the wire and not only as a unit test of the extractor.
//!
//! Every case below was measured against `redis-server 8.0.5`, which fires an
//! invalidation for all of them; the `{hash}`-tagged read rows were re-measured
//! against `redis-server 8.6.1` (`SINTERCARD 2 {s}1 {s}2` -> `:1`, then
//! `SADD {s}1 y` pushes `invalidate {s}1`).
//!
//! Each case runs beside a **control** that differs only in using a
//! fixed-position command. The controls are not decoration: without them, a
//! harness that never delivers pushes at all would make every assertion pass
//! after an inverted fix, or fail for reasons unrelated to key extraction.
//!
//! ## Why the multi-key read rows are `{hash}`-tagged (moon#962)
//!
//! Until moon#962 a multi-key read whose keys lived on several shards was
//! executed on the FIRST key's shard alone, and every other key read as
//! absent. `SINTERCARD 2 s1 s2` with `x` in both answered `0` at `--shards 4`
//! (true answer `1`) — and this file was **green on that wrong answer**: the
//! read "succeeded", the client registered, the assertion passed. moon#962
//! turns that into a `CROSSSLOT` refusal, which registers nothing, so the
//! untagged rows failed for the right reason.
//!
//! The read rows are therefore co-located with a `{hash}` tag — the read
//! genuinely executes at every shard count and the assertion means what it
//! says — and `spanning_movablekeys_read_is_refused_not_answered_from_one_shard`
//! pins the refusal itself, so a regression that quietly restored the one-shard
//! answer cannot make this file green again. [`invalidation_reaches_tracker`]
//! additionally refuses to count an errored read as "registered".

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use moon::shard::dispatch::key_to_shard;

/// Shard count the spanning case is pinned at: what moon#962 measured at, and
/// enough that a two-key set can be placed on different shards.
const SPAN_SHARDS: usize = 4;

struct Moon {
    child: Child,
    port: u16,
    tmp_dir: std::path::PathBuf,
}

impl Drop for Moon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.tmp_dir);
    }
}

fn spawn_moon(shards: &str) -> Moon {
    // `find_moon_binary`, not a bare `CARGO_BIN_EXE_moon`: `MOON_BIN` must be
    // able to point this suite at a control binary, or a red/green A/B of the
    // rows below is impossible.
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-track-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                shards,
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                "--disk-free-min-pct",
                "0",
                "--dir",
                tmp_dir.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(
                std::fs::File::create(tmp_dir.join("moon.stderr")).expect("create moon stderr log"),
            )
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("moon-track-{port}"));
    let mut moon = Moon {
        child,
        port,
        tmp_dir,
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(mut c) = TcpStream::connect(("127.0.0.1", moon.port)) {
            let _ = c.set_read_timeout(Some(Duration::from_millis(500)));
            if c.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut buf = [0u8; 64];
                if let Ok(n) = c.read(&mut buf)
                    && n > 0
                    && buf.starts_with(b"+PONG")
                {
                    return moon;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let status = match moon.child.try_wait() {
        Ok(Some(s)) => format!("exited with {s}"),
        Ok(None) => "still running but never answered PING".to_string(),
        Err(e) => format!("status unavailable: {e}"),
    };
    let log = std::fs::read_to_string(moon.tmp_dir.join("moon.stderr")).unwrap_or_default();
    panic!("moon never became ready on port {port} ({status})\n--- stderr ---\n{log}");
}

fn encode(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
    }
    out
}

struct Conn(TcpStream);

impl Conn {
    fn open(port: u16) -> Self {
        let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        s.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut c = Conn(s);
        c.call(&["HELLO", "3"]);
        c
    }

    /// Send a command and drain until the peer is quiet for `wait`.
    fn drain(&mut self, wait: Duration) -> Vec<u8> {
        self.0.set_read_timeout(Some(wait)).unwrap();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 16384];
        loop {
            match self.0.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(_) => break,
            }
        }
        buf
    }

    fn call(&mut self, args: &[&str]) -> Vec<u8> {
        self.0.write_all(&encode(args)).expect("write");
        self.drain(Duration::from_millis(350))
    }

    /// Wait for an `invalidate` push naming `key`, up to `budget`.
    fn awaits_invalidate(&mut self, key: &str, budget: Duration) -> bool {
        let deadline = Instant::now() + budget;
        let mut seen = Vec::new();
        while Instant::now() < deadline {
            seen.extend_from_slice(&self.drain(Duration::from_millis(250)));
            let text = String::from_utf8_lossy(&seen);
            if text.contains("invalidate") && text.contains(key) {
                return true;
            }
        }
        false
    }
}

fn tracking_client(port: u16) -> Conn {
    let mut c = Conn::open(port);
    let reply = c.call(&["CLIENT", "TRACKING", "ON"]);
    assert!(
        String::from_utf8_lossy(&reply).contains("OK"),
        "CLIENT TRACKING ON failed: {:?}",
        String::from_utf8_lossy(&reply)
    );
    c
}

/// One tracked-read / foreign-write pair: does the tracking client get told?
///
/// Panics if the tracked read itself errors. A read that was refused never
/// executed, so whether the client "registered" afterwards proves nothing
/// about key extraction — and naming the reply here is what turns a bare
/// "did not register" into a diagnosis (moon#962 was first seen as exactly
/// that bare message).
fn invalidation_reaches_tracker(
    port: u16,
    setup: &[&[&str]],
    tracked_read: &[&str],
    foreign_write: &[&str],
    key: &str,
) -> bool {
    let mut w = Conn::open(port);
    for c in setup {
        w.call(c);
    }

    let mut tracker = tracking_client(port);
    let read_reply = tracker.call(tracked_read);
    assert!(
        !read_reply.starts_with(b"-"),
        "tracked read {tracked_read:?} errored, so registration cannot be judged: {:?}",
        String::from_utf8_lossy(&read_reply)
    );

    let mut other = Conn::open(port);
    other.call(foreign_write);

    tracker.awaits_invalidate(key, Duration::from_secs(3))
}

/// A key on a DIFFERENT shard from `anchor` at [`SPAN_SHARDS`], found with the
/// routing hash the server itself uses — never a literal that happens to land
/// right at one shard count.
fn key_on_another_shard(anchor: &str, prefix: &str) -> String {
    let owner = key_to_shard(anchor.as_bytes(), SPAN_SHARDS);
    (0..1000)
        .map(|i| format!("{prefix}{i}"))
        .find(|k| key_to_shard(k.as_bytes(), SPAN_SHARDS) != owner)
        .expect("a key on another shard must exist within 1000 candidates")
}

/// A movablekeys READ must register the client, and a movablekeys WRITE must
/// push. Controls in the same shape prove the harness delivers pushes at all.
#[test]
fn movablekeys_reads_and_writes_reach_tracking_clients() {
    for shards in ["1", "4"] {
        let moon = spawn_moon(shards);
        let p = moon.port;

        // ── controls: fixed-position commands, known-good before #582 ─────
        assert!(
            invalidation_reaches_tracker(
                p,
                &[&["DEL", "cs"], &["SADD", "cs", "x"]],
                &["SMEMBERS", "cs"],
                &["SADD", "cs", "y"],
                "cs",
            ),
            "CONTROL SMEMBERS/SADD did not deliver at shards={shards} — the harness is \
             broken, so the assertions below would be meaningless"
        );
        assert!(
            invalidation_reaches_tracker(
                p,
                &[&["DEL", "cl"], &["RPUSH", "cl", "a", "b"]],
                &["LRANGE", "cl", "0", "-1"],
                &["LPOP", "cl"],
                "cl",
            ),
            "CONTROL LRANGE/LPOP did not deliver at shards={shards}"
        );

        // ── READ side: a movablekeys read must register the client ────────
        //
        // Two keys, co-located by `{hash}` tag so the read EXECUTES at every
        // shard count (moon#962: an untagged pair that spans shards is refused
        // with CROSSSLOT, and a refused read registers nothing). The spanning
        // form is pinned separately below.
        assert!(
            invalidation_reaches_tracker(
                p,
                &[
                    &["DEL", "{s}1", "{s}2"],
                    &["SADD", "{s}1", "x"],
                    &["SADD", "{s}2", "x"]
                ],
                &["SINTERCARD", "2", "{s}1", "{s}2"],
                &["SADD", "{s}1", "y"],
                "{s}1",
            ),
            "SINTERCARD read did not register the tracking client (shards={shards})"
        );
        assert!(
            invalidation_reaches_tracker(
                p,
                &[
                    &["DEL", "{z}1", "{z}2"],
                    &["ZADD", "{z}1", "1", "a"],
                    &["ZADD", "{z}2", "1", "b"]
                ],
                &["ZDIFF", "2", "{z}1", "{z}2"],
                &["ZADD", "{z}1", "2", "c"],
                "{z}1",
            ),
            "ZDIFF read did not register the tracking client (shards={shards})"
        );

        // ── WRITE side: a movablekeys write must push an invalidation ─────
        //
        // Single-key (`numkeys 1`) on purpose: one key names one shard, so
        // these rows are outside moon#962's refusal at every shard count and
        // isolate the #582 extractor defect from routing.
        assert!(
            invalidation_reaches_tracker(
                p,
                &[&["DEL", "ml"], &["RPUSH", "ml", "a", "b", "c"]],
                &["LRANGE", "ml", "0", "-1"],
                &["LMPOP", "1", "ml", "LEFT"],
                "ml",
            ),
            "LMPOP write did not invalidate the key it popped (shards={shards})"
        );
        assert!(
            invalidation_reaches_tracker(
                p,
                &[&["DEL", "mz"], &["ZADD", "mz", "1", "a", "2", "b"]],
                &["ZRANGE", "mz", "0", "-1"],
                &["ZMPOP", "1", "mz", "MIN"],
                "mz",
            ),
            "ZMPOP write did not invalidate the key it popped (shards={shards})"
        );
        // Two keys, co-located by tag, with the FIRST one empty so the pop
        // lands on the SECOND: the invalidation must name the key that was
        // actually popped, not the routing key. Measured against redis 8.6.1
        // (`LMPOP 2 {m}a {m}b LEFT` -> `{m}b B1`, push `invalidate {m}b`).
        assert!(
            invalidation_reaches_tracker(
                p,
                &[&["DEL", "{m}a", "{m}b"], &["RPUSH", "{m}b", "B1", "B2"]],
                &["LRANGE", "{m}b", "0", "-1"],
                &["LMPOP", "2", "{m}a", "{m}b", "LEFT"],
                "{m}b",
            ),
            "LMPOP over two keys did not invalidate the SECOND key it popped \
             (shards={shards})"
        );
        assert!(
            invalidation_reaches_tracker(
                p,
                &[
                    &["DEL", "{q}a", "{q}b"],
                    &["ZADD", "{q}b", "1", "x", "2", "y"]
                ],
                &["ZRANGE", "{q}b", "0", "-1"],
                &["ZMPOP", "2", "{q}a", "{q}b", "MIN"],
                "{q}b",
            ),
            "ZMPOP over two keys did not invalidate the SECOND key it popped \
             (shards={shards})"
        );
    }
}

/// moon#962: a movablekeys read whose keys SPAN shards is refused with
/// `CROSSSLOT` — never answered from one shard's slice. Before the fix the same
/// `SINTERCARD` answered `:0` at `--shards 4` (true answer `1`), the client
/// registered, and this file was green on the wrong answer.
///
/// The second key is SEARCHED for on a different shard with the server's own
/// routing hash, so the pair spans by construction, not by luck. The same argv
/// at `--shards 1` — where no boundary exists — must still answer `:1` and
/// still register the client, which keeps the pre-#962 untagged shape of the
/// #582 assertion alive on the one leg where it was ever true.
#[test]
fn spanning_movablekeys_read_is_refused_not_answered_from_one_shard() {
    let a = "span:a";
    let b = key_on_another_shard(a, "span:b");
    assert_ne!(
        key_to_shard(a.as_bytes(), SPAN_SHARDS),
        key_to_shard(b.as_bytes(), SPAN_SHARDS),
        "the pair must span shards for this test to mean anything"
    );

    // ── shards=4: refused, and refused BEFORE reading (the reply is an error,
    //    not a number) ────────────────────────────────────────────────────────
    {
        let moon = spawn_moon(&SPAN_SHARDS.to_string());
        let mut c = Conn::open(moon.port);
        c.call(&["DEL", a, &b]);
        c.call(&["SADD", a, "x"]);
        c.call(&["SADD", &b, "x"]);
        let reply = c.call(&["SINTERCARD", "2", a, &b]);
        let text = String::from_utf8_lossy(&reply);
        assert!(
            text.starts_with("-CROSSSLOT"),
            "SINTERCARD over a spanning key set at shards={SPAN_SHARDS} must be refused \
             with CROSSSLOT, got {text:?} (`:0` is the pre-#962 one-shard wrong answer; \
             `:1` would mean a merge now exists and this pin should move to \
             tests/multikey_read_cross_shard.rs)"
        );
    }

    // ── shards=1: the same argv answers correctly and registers ───────────
    {
        let moon = spawn_moon("1");
        let mut c = Conn::open(moon.port);
        c.call(&["DEL", a, &b]);
        c.call(&["SADD", a, "x"]);
        c.call(&["SADD", &b, "x"]);
        let reply = c.call(&["SINTERCARD", "2", a, &b]);
        assert_eq!(
            reply,
            b":1\r\n",
            "at shards=1 there is no boundary to cross: got {:?}",
            String::from_utf8_lossy(&reply)
        );
        assert!(
            invalidation_reaches_tracker(
                moon.port,
                &[&["DEL", a, &b], &["SADD", a, "x"], &["SADD", &b, "x"]],
                &["SINTERCARD", "2", a, &b],
                &["SADD", a, "y"],
                a,
            ),
            "untagged SINTERCARD read did not register the tracking client at shards=1"
        );
    }
}

/// `SORT src STORE dst` WRITES `dst`, but its registry spec names `src`
/// (`first_key = 1`). Moon invalidated the key it had not written and missed
/// the one it had — so a tracker of `dst` was never told, which is the shape
/// this test pins.
#[test]
fn sort_store_invalidates_the_destination_it_writes() {
    for shards in ["1", "4"] {
        let moon = spawn_moon(shards);
        assert!(
            invalidation_reaches_tracker(
                moon.port,
                &[
                    &["DEL", "{sort}src", "{sort}dst"],
                    &["RPUSH", "{sort}src", "b", "a"],
                    &["RPUSH", "{sort}dst", "stale"],
                ],
                &["LRANGE", "{sort}dst", "0", "-1"],
                &["SORT", "{sort}src", "ALPHA", "STORE", "{sort}dst"],
                "{sort}dst",
            ),
            "SORT ... STORE did not invalidate its DESTINATION (shards={shards})"
        );
    }
}
