//! A transaction must be all-or-nothing with respect to QUEUE-TIME faults.
//!
//! Measured against redis-server 8.6.1. The headline is a data-correctness
//! bug, not a compatibility nit:
//!
//! ```text
//! MULTI / NOSUCHCMD / SET k v / EXEC
//!   redis: -ERR unknown command … / +QUEUED / -EXECABORT      -> k is UNSET
//!   moon : -ERR unknown command … / +QUEUED / *1 +OK          -> k is SET
//! ```
//!
//! Moon has no queue-time validation, so nothing marks the transaction dirty
//! and EXEC happily runs the half that parsed. Redis checks each command's
//! existence and arity as it is QUEUED, latches a dirty flag on failure, and
//! refuses the whole block at EXEC.
//!
//! Raw sockets throughout: redis-rs normalises away the very distinctions
//! under test (a Null Array vs a Null Bulk, `+QUEUED` vs an error reply).

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const EXECABORT: &str = "EXECABORT Transaction discarded because of previous errors.";

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
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-multiexec-{port}"));
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
    let tmp_dir = std::env::temp_dir().join(format!("moon-multiexec-{port}"));
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

/// A connection that sends one command at a time and returns each raw reply,
/// so command N's answer is never conflated with command N+1's.
struct Conn(TcpStream);

impl Conn {
    fn open(port: u16) -> Self {
        let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
        Conn(s)
    }

    /// Encode as a RESP array. Inline would work for most of these, but the
    /// inline path is a DIFFERENT dispatch path in Moon, and a MULTI fix that
    /// only lands on one of them is not a fix.
    fn send(&mut self, parts: &[&str]) -> String {
        let mut out = format!("*{}\r\n", parts.len());
        for p in parts {
            out.push_str(&format!("${}\r\n{p}\r\n", p.len()));
        }
        self.0.write_all(out.as_bytes()).expect("write");
        self.read_reply()
    }

    /// Send raw bytes with no encoding — for the malformed-frame case.
    fn send_raw(&mut self, bytes: &[u8]) -> String {
        let _ = self.0.write_all(bytes);
        self.read_reply()
    }

    fn read_reply(&mut self) -> String {
        let mut buf = [0u8; 8192];
        let mut acc = Vec::new();
        loop {
            match self.0.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    acc.extend_from_slice(&buf[..n]);
                    // One reply per send, so the first complete line (or the
                    // whole array) is enough; a short second read drains any
                    // trailing segment of a multi-line array.
                    self.0
                        .set_read_timeout(Some(Duration::from_millis(200)))
                        .unwrap();
                }
                Err(_) => break,
            }
        }
        self.0
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        String::from_utf8_lossy(&acc).into_owned()
    }
}

// ---------------------------------------------------------------------------
// me1 — the data-loss case. This is why the task exists.
// ---------------------------------------------------------------------------

#[test]
fn me1_unknown_command_aborts_and_applies_nothing() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    assert!(c.send(&["MULTI"]).starts_with("+OK"));

    let bad = c.send(&["NOSUCHCMD", "a", "b"]);
    assert!(
        bad.contains("unknown command"),
        "queueing an unknown command must reply the error AT QUEUE TIME; got {bad:?}"
    );

    let queued = c.send(&["SET", "me1key", "v"]);
    assert!(
        queued.starts_with("+QUEUED"),
        "a VALID command after the bad one still queues normally; got {queued:?}"
    );

    let exec = c.send(&["EXEC"]);
    assert!(
        exec.contains(EXECABORT),
        "EXEC on a poisoned transaction must refuse the whole block; got {exec:?}"
    );

    // The assertion that makes this a correctness bug rather than a wording
    // nit: on Moon today the SET actually lands.
    let mut probe = Conn::open(m.port);
    let got = probe.send(&["GET", "me1key"]);
    assert!(
        got.starts_with("$-1"),
        "an aborted transaction must apply NOTHING. me1key should not exist; \
         GET returned {got:?}"
    );
}

// ---------------------------------------------------------------------------
// me2 — arity is checked at queue time too.
// ---------------------------------------------------------------------------

#[test]
fn me2_wrong_arity_aborts() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    c.send(&["MULTI"]);
    let bad = c.send(&["GET"]);
    assert!(
        bad.contains("wrong number of arguments"),
        "GET with no key cannot ever run; it must be refused at queue time. got {bad:?}"
    );
    let exec = c.send(&["EXEC"]);
    assert!(exec.contains(EXECABORT), "got {exec:?}");
}

// ---------------------------------------------------------------------------
// me3 — the dirty flag must not outlive its transaction.
// ---------------------------------------------------------------------------

#[test]
fn me3_discard_clears_the_dirty_flag() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    c.send(&["MULTI"]);
    c.send(&["NOSUCHCMD"]);
    assert!(c.send(&["DISCARD"]).starts_with("+OK"));

    // A leaked flag would silently abort this innocent transaction — a worse
    // bug than the one being fixed, so it gets its own test.
    c.send(&["MULTI"]);
    assert!(c.send(&["SET", "me3key", "v"]).starts_with("+QUEUED"));
    let exec = c.send(&["EXEC"]);
    assert!(
        !exec.contains("EXECABORT") && exec.contains("+OK"),
        "a fresh MULTI after a DISCARDed poisoned one must run normally; got {exec:?}"
    );
    assert!(c.send(&["GET", "me3key"]).contains('v'));
}

// ---------------------------------------------------------------------------
// me4 — SUBSCRIBE must be refused, not EXECUTED. Moon subscribes for real.
// ---------------------------------------------------------------------------

#[test]
fn me4_subscribe_is_refused_at_queue_time_not_executed() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    c.send(&["MULTI"]);
    let r = c.send(&["SUBSCRIBE", "ch"]);
    assert!(
        !r.contains("subscribe") || r.contains("not allowed"),
        "SUBSCRIBE inside MULTI must be REFUSED, not run. A reply containing a \
         real subscribe confirmation means the connection entered subscriber \
         mode inside a transaction. got {r:?}"
    );
    assert!(
        r.contains("not allowed in transactions"),
        "expected Redis's refusal text; got {r:?}"
    );
}

// ---------------------------------------------------------------------------
// me5 — WATCH has its own distinct refusal.
// ---------------------------------------------------------------------------

#[test]
fn me5_watch_inside_multi_is_refused() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    c.send(&["MULTI"]);
    let r = c.send(&["WATCH", "k"]);
    assert!(r.contains("WATCH inside MULTI is not allowed"), "got {r:?}");
}

// ---------------------------------------------------------------------------
// me6 — a malformed frame inside MULTI still names itself (protocol contract).
// ---------------------------------------------------------------------------

#[test]
fn me6_bad_frame_inside_multi_names_itself_before_closing() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    c.send(&["MULTI"]);
    let r = c.send_raw(b"*2\r\n$3\r\nGET\r\n$abc\r\nk\r\n");
    assert!(
        r.contains("Protocol error: invalid bulk length"),
        "a malformed frame inside MULTI follows the same protocol-error \
         contract as anywhere else: name it, THEN close. Moon closes bare \
         today. got {r:?}"
    );
}

// ---------------------------------------------------------------------------
// me7 — the null TYPE matters to statically-typed clients.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "needs Frame::NullArray — RESP2 `*-1` is inexpressible in Moon today; \
            filed as its own task, see the comment below"]
fn me7_blpop_in_multi_returns_null_array_not_null_bulk() {
    // IGNORED, not deleted, and not weakened: the divergence is real and
    // measured (Moon `*1\r\n$-1\r\n`, Redis `*1\r\n*-1\r\n`).
    //
    // It cannot be fixed here. `Frame` has no null-array variant at all —
    // `Frame::Null` serialises to `$-1` in RESP2, full stop — so this is not a
    // MULTI bug: a plain `BLPOP key 1` that times out OUTSIDE a transaction
    // returns the wrong type too, as does every command whose null shape is an
    // array. The fix is a `Frame::NullArray` variant threaded through the ~14
    // `Frame::Null` arms in `serialize.rs` / `resp3.rs`, which touches every
    // reply path and deserves its own contract and review.
    //
    // Un-ignore this test in that task; it is already the right assertion.

    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    c.send(&["MULTI"]);
    assert!(c.send(&["BLPOP", "me7missing", "0"]).starts_with("+QUEUED"));
    let exec = c.send(&["EXEC"]);
    assert!(
        exec.contains("*-1"),
        "a blocking command that finds nothing inside MULTI takes its \
         zero-timeout path and returns a Null ARRAY (`*-1`). Moon returns a \
         Null BULK (`$-1`), which a statically-typed client decodes as the \
         wrong type. got {exec:?}"
    );
    assert!(
        !exec.contains("$-1"),
        "the reply must not be a Null Bulk; got {exec:?}"
    );
}

// ---------------------------------------------------------------------------
// me8 — a RUNTIME error is data, not a queue-time fault. Must NOT abort.
// ---------------------------------------------------------------------------

#[test]
fn me8_wrongtype_at_execution_does_not_abort_the_block() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    c.send(&["LPUSH", "me8list", "v"]);
    c.send(&["MULTI"]);
    c.send(&["GET", "me8list"]);
    c.send(&["SET", "me8other", "v"]);
    let exec = c.send(&["EXEC"]);
    assert!(
        exec.contains("WRONGTYPE"),
        "the type error is one ELEMENT of the EXEC array; got {exec:?}"
    );
    assert!(
        !exec.contains("EXECABORT"),
        "a runtime error must NOT abort the transaction — only queue-time \
         faults do. got {exec:?}"
    );
    assert!(
        c.send(&["GET", "me8other"]).contains('v'),
        "the sibling command must still have applied"
    );
}

// ---------------------------------------------------------------------------
// me9 — the cases already matching Redis must stay matching.
// ---------------------------------------------------------------------------

#[test]
fn me9_already_matching_behavior_is_unchanged() {
    let m = spawn_moon("1");

    let mut c = Conn::open(m.port);
    c.send(&["MULTI"]);
    assert!(
        c.send(&["MULTI"]).contains("MULTI calls can not be nested"),
        "nested MULTI"
    );
    c.send(&["DISCARD"]);

    let mut c = Conn::open(m.port);
    assert!(
        c.send(&["EXEC"]).contains("EXEC without MULTI"),
        "EXEC without MULTI"
    );
    assert!(
        c.send(&["DISCARD"]).contains("DISCARD without MULTI"),
        "DISCARD without MULTI"
    );

    // Empty MULTI/EXEC is an empty array, not an error.
    let mut c = Conn::open(m.port);
    c.send(&["MULTI"]);
    assert!(c.send(&["EXEC"]).starts_with("*0"), "empty MULTI/EXEC");

    // SELECT inside MULTI queues and applies.
    let mut c = Conn::open(m.port);
    c.send(&["MULTI"]);
    assert!(c.send(&["SELECT", "3"]).starts_with("+QUEUED"));
    assert!(c.send(&["EXEC"]).contains("+OK"), "SELECT inside MULTI");

    // RESET inside MULTI abandons the transaction.
    let mut c = Conn::open(m.port);
    c.send(&["MULTI"]);
    c.send(&["SET", "me9key", "v"]);
    assert!(c.send(&["RESET"]).starts_with("+RESET"));
    assert!(
        c.send(&["EXEC"]).contains("EXEC without MULTI"),
        "RESET must have left MULTI state"
    );
}

// ---------------------------------------------------------------------------
// me10 — the §1 ⚠ mitigation. A command that dispatches must also queue.
// ---------------------------------------------------------------------------

#[test]
fn me10_every_dispatchable_command_is_queueable() {
    // The risk this fix introduces: queue-time validation consults
    // COMMAND_META, but Moon has THREE dispatch paths. A command reachable
    // through dispatch but MISSING from the table would now be rejected
    // inside MULTI while still working outside it — a regression strictly
    // worse than the bug being fixed.
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);

    // One representative command per dispatch path, each with valid arity.
    let representatives: [&[&str]; 6] = [
        &["SET", "me10k", "v"],       // write path: dispatch
        &["GET", "me10k"],            // read path: dispatch_read + inline fast path
        &["HSET", "me10h", "f", "v"], // write, non-inline-eligible
        &["TTL", "me10k"],            // read, non-inline
        &["PING"],                    // no-key command
        &["INFO", "server"],          // admin-ish, optional arg
    ];
    for cmd in representatives {
        c.send(&["MULTI"]);
        let r = c.send(cmd);
        assert!(
            r.starts_with("+QUEUED"),
            "{cmd:?} dispatches fine outside MULTI, so it MUST queue inside \
             one. A non-QUEUED reply means COMMAND_META disagrees with \
             dispatch. got {r:?}"
        );
        let exec = c.send(&["EXEC"]);
        assert!(
            !exec.contains("EXECABORT"),
            "{cmd:?} queued but the transaction was poisoned anyway: {exec:?}"
        );
        // The assertion this test was MISSING. `+QUEUED` plus "no EXECABORT"
        // says the transaction was not poisoned — it says nothing about
        // whether the command actually RAN. It did not: `execute_transaction`
        // replays the queue through `dispatch()`, and a connection-level
        // intercept never reaches dispatch, so it came back as
        // `-ERR unknown command` INSIDE the EXEC array while both assertions
        // above stayed happy. That is how a regression shipped past a green
        // test — the check verified the signal expected, not the behaviour
        // claimed.
        assert!(
            !exec.contains("unknown command"),
            "{cmd:?} queued but could not be EXECUTED — a queued command that \
             cannot run is worse than one that never queued, because the client \
             gets an error where it used to get data. got {exec:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// me10b — the regression class the ⚠ assumption named, pinned.
// ---------------------------------------------------------------------------

#[test]
fn me10b_unregistered_dotted_commands_still_queue() {
    // Queue-time validation reads COMMAND_META, which does NOT cover every
    // dotted extension family Moon dispatches. A dotted name missing from the
    // table must fall through to queueing, never be called "unknown" — that
    // would break a command that works fine outside a transaction.
    //
    // GRAPH.QUERY is IN the table, so it proves registered dotted names are
    // unaffected; the synthetic name proves unregistered ones are not
    // rejected at queue time. Whether the synthetic one errors at EXEC is not
    // this test's business — only that it does not poison the transaction.
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    c.send(&["MULTI"]);
    let r = c.send(&["SOMEFUTURE.CMD", "arg"]);
    assert!(
        r.starts_with("+QUEUED"),
        "an unregistered DOTTED command must queue, not be rejected as          unknown — otherwise adding a dotted family without a COMMAND_META          entry silently makes it unusable inside MULTI. got {r:?}"
    );
    let exec = c.send(&["EXEC"]);
    assert!(
        !exec.contains("EXECABORT"),
        "an unregistered dotted command must not poison the transaction; got {exec:?}"
    );

    // A NON-dotted unknown name is still caught — the carve-out must not have
    // disabled the check that me1 depends on.
    let mut c2 = Conn::open(m.port);
    c2.send(&["MULTI"]);
    assert!(
        c2.send(&["NOSUCHCMD"]).contains("unknown command"),
        "the dotted carve-out must not weaken the plain unknown-command check"
    );
}

// ---------------------------------------------------------------------------
// me10c — intercept-only commands must not be queued into a guaranteed error.
// ---------------------------------------------------------------------------

#[test]
fn me10c_intercept_only_commands_are_not_queued_into_an_error() {
    // LATENCY is deliberately NOT in this list: it errors outside a
    // transaction too (Moon does not implement it), so including it would have
    // hidden an unimplemented command behind a transaction exemption.
    //
    // These reach a connection-level intercept and never `dispatch()`, so the
    // transaction executor cannot replay them. Redis queues and runs them all;
    // Moon cannot yet, so it keeps executing them immediately — the
    // pre-existing divergence the compat manifest waives.
    //
    // What must NEVER happen is the middle state: queueing them into an
    // `-ERR unknown command` inside EXEC. That turns data into an error, which
    // is strictly worse than the divergence it replaced.
    let m = spawn_moon("1");
    for cmd in [
        &["CONFIG", "GET", "maxmemory"][..],
        &["CLIENT", "GETNAME"][..],
        &["ACL", "WHOAMI"][..],
        &["CLUSTER", "INFO"][..],
        &["SCRIPT", "EXISTS", "deadbeef"][..],
        &["WAIT", "0", "0"][..],
        // PUBSUB is the one that matters most here: it works outside MULTI
        // (`*0`) but is ABSENT from COMMAND_META, so queue-time validation
        // would call it unknown and poison the transaction. It has no dot,
        // so the dotted carve-out does not cover it.
        &["PUBSUB", "CHANNELS"][..],
    ] {
        let mut c = Conn::open(m.port);
        c.send(&["MULTI"]);
        let queued = c.send(cmd);
        assert!(
            !queued.contains("unknown command"),
            "{cmd:?} works outside MULTI, so it must not be rejected as unknown \
             inside one. got {queued:?}"
        );
        let exec = c.send(&["EXEC"]);
        assert!(
            !exec.contains("unknown command") && !exec.contains("EXECABORT"),
            "{cmd:?} must not end up as an error inside EXEC. got {exec:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// me11 — shard count must not change transaction semantics.
// ---------------------------------------------------------------------------

#[test]
fn me11_queue_semantics_hold_at_four_shards() {
    let m = spawn_moon("4");
    let mut c = Conn::open(m.port);
    c.send(&["MULTI"]);
    c.send(&["NOSUCHCMD"]);
    // Deliberately a key that hashes elsewhere than the connection's shard.
    c.send(&["SET", "me11:crossshard:key", "v"]);
    let exec = c.send(&["EXEC"]);
    assert!(
        exec.contains(EXECABORT),
        "the dirty flag is connection-local, so shard count cannot matter; got {exec:?}"
    );
    let mut probe = Conn::open(m.port);
    assert!(
        probe
            .send(&["GET", "me11:crossshard:key"])
            .starts_with("$-1"),
        "a cross-shard write in an aborted transaction must not apply either"
    );
}
