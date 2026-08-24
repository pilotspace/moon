//! Container subcommands: unknown ones must be refused with Redis's shape, and
//! refused at QUEUE time inside `MULTI` — moon#670.
//!
//! Two defects, one table.
//!
//! 1. **Queue-time validation.** Redis validates a container's *subcommand*
//!    before storing the command in the transaction: an unknown subcommand is
//!    refused on the `MULTI` connection immediately and the transaction is
//!    poisoned, so `EXEC` answers `-EXECABORT`. Moon replied `+QUEUED` and only
//!    noticed at `EXEC`, so the transaction RAN and the client got a one-element
//!    array holding the error. A client that treats `+QUEUED` as "this command
//!    is valid" — which is exactly what Redis guarantees — then applies a
//!    partial result.
//!
//! 2. **Error text.** Redis's shape is uniformly
//!    `ERR unknown subcommand '<as sent>'. Try <CONTAINER> HELP.` Moon had ten
//!    different spellings, including one that differed only in the case of a
//!    single letter (`COMMAND` said `Unknown`) and two that never named the
//!    offending subcommand at all (`SLOWLOG`, `XGROUP` — the latter reported a
//!    literal `'UNKNOWN'`).
//!
//! **Measured, not recalled.** Every expectation below came from probing
//! `redis-server 8.6.1` on 2026-08-24, one fresh connection per case (`MULTI`
//! state is per-connection, and a desynced socket turns the whole table into
//! fiction). The issue reported 6 containers; the sweep found **14**.
//!
//! Three containers are deliberately NOT gated at queue time, and each exclusion
//! is fenced by a test rather than left to a comment. (`FUNCTION` was a fourth
//! until moon#697 made its executor dispatch; `csp6` now asserts it IS gated.)
//!
//!   * `CLUSTER` — with cluster support disabled Moon answers *every* CLUSTER
//!     subcommand, bogus ones included, with "This instance has cluster support
//!     disabled". Dispatch therefore never says "unknown subcommand", so the
//!     queue gate must not either, or a queued-vs-dispatched divergence opens up
//!     in the other direction (`csp7`).
//!   * `DEBUG` — Redis refuses `DEBUG` outright unless `enable-debug-command`
//!     is set, so there is no oracle reply to match.
//!   * `LATENCY` — an unknown *command* on Moon (moon#632), already caught by
//!     the existing command-existence half of the queue gate.

mod common;

use common::Conn;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

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
        let tmp_dir = std::env::temp_dir().join(format!("moon-csp670-{port}"));
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
                tmp_dir.to_str().unwrap_or("/tmp"),
            ])
            .stdout(Stdio::null())
            .stderr(
                std::fs::File::create(tmp_dir.join("moon.stderr")).expect("create moon stderr log"),
            )
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("moon-csp670-{port}"));
    let moon = Moon {
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
    let log = std::fs::read_to_string(moon.tmp_dir.join("moon.stderr")).unwrap_or_default();
    panic!("moon never became ready on port {port}\n--- stderr ---\n{log}");
}

/// The containers whose unknown-subcommand rejection is gated at queue time.
///
/// This is Moon's own set, not Redis's, exactly as the existing command-name
/// half of the gate uses Moon's `COMMAND_META`: a name Moon does not dispatch
/// is not queueable, whether or not Redis has it.
const GATED: &[&str] = &[
    // FUNCTION joined in moon#697, once its EXEC executor stopped reporting an
    // unknown command for every subcommand.
    "ACL", "CLIENT", "COMMAND", "CONFIG", "FUNCTION", "MEMORY", "MODULE", "OBJECT", "PUBSUB",
    "SCRIPT", "SLOWLOG", "XGROUP", "XINFO",
];

/// The containers whose unknown-subcommand ERROR TEXT must match Redis.
///
/// A superset of [`GATED`]: `FUNCTION`'s top-level text is in scope even though
/// its queue-time gating is blocked on moon#697.
const TEXT_CONTAINERS: &[&str] = &[
    "ACL", "CLIENT", "COMMAND", "CONFIG", "FUNCTION", "MEMORY", "MODULE", "OBJECT", "PUBSUB",
    "SCRIPT", "SLOWLOG", "XGROUP", "XINFO",
];

fn unknown_sub(container: &str, sub: &str) -> String {
    format!("-ERR unknown subcommand '{sub}'. Try {container} HELP.\r\n")
}

const EXECABORT: &str = "-EXECABORT Transaction discarded because of previous errors.\r\n";

// ---------------------------------------------------------------------------
// Defect 2 — the error text
// ---------------------------------------------------------------------------

#[test]
fn csp1_every_container_refuses_a_bogus_subcommand_with_redis_shape() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    for container in TEXT_CONTAINERS {
        // `OBJECT BOGUS` alone used to answer an ARITY error rather than an
        // unknown-subcommand error, so the bogus name is sent WITH a plausible
        // argument as well — an arity error would read as "the subcommand
        // exists" and hide the defect.
        for argv in [
            vec![*container, "BOGUS"],
            vec![*container, "BOGUS", "nokey670"],
        ] {
            let got = c.send(&argv);
            assert_eq!(
                got,
                unknown_sub(container, "BOGUS"),
                "\n  command : {argv:?}\n  why     : moon#670 — redis 8.6.1 answers this exact \
                 string for every container; Moon had ten spellings"
            );
        }
    }
}

#[test]
fn csp2_the_echoed_subcommand_keeps_the_case_the_client_sent() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    // Redis echoes the subcommand VERBATIM here — unlike the arity error, which
    // lower-cases the command name (moon#491). Getting this backwards would look
    // like a fix and break the string a driver author greps for.
    assert_eq!(
        c.send(&["CONFIG", "MiXeD"]),
        unknown_sub("CONFIG", "MiXeD"),
        "moon#670 — the subcommand is echoed as sent, not normalised"
    );
}

#[test]
fn csp3_an_echoed_subcommand_cannot_inject_a_second_reply() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    // The subcommand is USER INPUT echoed into a RESP error, and `serialize_frame`
    // writes an error's payload raw before terminating it with CRLF. A subcommand
    // arrives as a bulk string, so it may legally contain CR, LF and NUL — which
    // would end the error frame early and let the client read the rest as a
    // second, attacker-chosen reply.
    let hostile = "a\r\n-INJECTED\r\n\u{7}b";
    let got = c.send(&["CONFIG", hostile]);
    // The payload surviving as TEXT is fine and even desirable — the client
    // should see what it sent. What must not survive is its framing: exactly one
    // CRLF, at the very end, and no other control byte anywhere in between.
    // Assert that, not the absence of the word, or the test passes for the wrong
    // reason the moment someone strips the text instead of the bytes.
    assert!(
        got.ends_with("\r\n") && got.matches("\r\n").count() == 1,
        "moon#670 — exactly one frame, terminated once; got {got:?}"
    );
    assert!(
        !got[..got.len() - 2].bytes().any(|b| b < 0x20 || b == 0x7f),
        "moon#670 — no control byte may reach the wire inside the frame; got {got:?}"
    );
    // The connection must still be usable: an injected frame would leave the
    // client one reply out of step for the rest of the session.
    assert_eq!(c.send(&["PING"]), "+PONG\r\n");
}

// ---------------------------------------------------------------------------
// Defect 1 — queue-time validation
// ---------------------------------------------------------------------------

#[test]
fn csp4_a_bogus_subcommand_is_refused_at_queue_time_and_poisons_the_transaction() {
    for shards in ["1", "4"] {
        let m = spawn_moon(shards);
        for container in GATED {
            // One connection per case: `MULTI` is per-connection state, and a
            // transaction left open would contaminate the next container.
            let mut c = Conn::open(m.port);
            assert_eq!(c.send(&["MULTI"]), "+OK\r\n");
            assert_eq!(
                c.send(&[container, "BOGUS"]),
                unknown_sub(container, "BOGUS"),
                "moon#670 (shards={shards}) — {container} BOGUS must be refused INSTEAD of \
                 being queued; Moon used to reply +QUEUED"
            );
            assert_eq!(
                c.send(&["EXEC"]),
                EXECABORT,
                "moon#670 (shards={shards}) — the transaction must be poisoned, so the valid \
                 half of a mistyped transaction never runs"
            );
        }
    }
}

#[test]
fn csp5_a_known_subcommand_still_queues_and_runs() {
    // The discriminator for the widening direction. A gate that rejected every
    // container subcommand would pass csp4 and be catastrophically wrong; this
    // is what stops that.
    for shards in ["1", "4"] {
        let m = spawn_moon(shards);
        let mut c = Conn::open(m.port);
        assert_eq!(c.send(&["MULTI"]), "+OK\r\n");
        for argv in [
            vec!["ACL", "WHOAMI"],
            vec!["PUBSUB", "NUMPAT"],
            vec!["CONFIG", "GET", "maxmemory"],
            vec!["MEMORY", "USAGE", "nokey670"],
            vec!["OBJECT", "ENCODING", "nokey670"],
            vec!["COMMAND", "COUNT"],
            vec!["SCRIPT", "EXISTS", "abc"],
            vec!["SLOWLOG", "LEN"],
        ] {
            assert_eq!(
                c.send(&argv),
                "+QUEUED\r\n",
                "moon#670 (shards={shards}) — {argv:?} is a real subcommand and must still queue"
            );
        }
        let exec = c.send(&["EXEC"]);
        assert!(
            exec.starts_with("*8\r\n"),
            "moon#670 (shards={shards}) — all eight must have run; got {exec:?}"
        );
        assert!(
            !exec.contains("unknown subcommand"),
            "moon#670 (shards={shards}) — no real subcommand may be reported unknown; got {exec:?}"
        );
    }
}

#[test]
fn csp6_function_is_gated_like_every_other_container() {
    // Was a known-red fence for moon#697, which is now fixed: `FUNCTION` was the
    // one container whose EXEC executor answered `ERR unknown command
    // 'FUNCTION'` for EVERY subcommand, so there was no notion of a "known
    // FUNCTION subcommand" for the gate to agree with.
    //
    // moon#697 added FUNCTION to `is_txn_connection_intercept`, so EXEC now
    // dispatches it and "queueable iff dispatchable" holds. FUNCTION therefore
    // moved into [`GATED`], and this asserts the behaviour rather than fencing
    // its absence. Measured: redis-server 8.6.1 refuses `FUNCTION BOGUS` at
    // queue time and answers `-EXECABORT`, exactly like CONFIG and OBJECT.
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    assert_eq!(c.send(&["MULTI"]), "+OK\r\n");
    assert_eq!(
        c.send(&["FUNCTION", "BOGUS"]),
        unknown_sub("FUNCTION", "BOGUS"),
        "moon#697 — FUNCTION is gated at queue time now"
    );
    assert_eq!(c.send(&["EXEC"]), EXECABORT);
}

#[test]
fn csp7_cluster_is_not_gated_while_cluster_support_is_disabled() {
    // Regression fence for the OTHER direction. With cluster disabled, Moon
    // answers every CLUSTER subcommand — bogus included — with "cluster support
    // disabled". Dispatch therefore never reports an unknown subcommand, so a
    // gate that rejected `CLUSTER BOGUS` would refuse to queue something
    // dispatch would happily have executed, breaking the queueable-iff-
    // dispatchable invariant the queue gate is built on.
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    assert_eq!(c.send(&["MULTI"]), "+OK\r\n");
    assert_eq!(
        c.send(&["CLUSTER", "BOGUS"]),
        "+QUEUED\r\n",
        "moon#670 — CLUSTER must stay ungated while cluster support is disabled"
    );
    let exec = c.send(&["EXEC"]);
    assert!(
        exec.starts_with("*1\r\n") && exec.contains("cluster support disabled"),
        "moon#670 — the transaction must run and CLUSTER must give its own answer; got {exec:?}"
    );
}
