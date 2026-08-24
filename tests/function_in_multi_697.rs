//! `FUNCTION` inside `MULTI` must dispatch, not report an unknown command — moon#697.
//!
//! Every `FUNCTION` subcommand — valid or not — was queued by `MULTI` and then
//! answered at `EXEC` with `ERR unknown command 'FUNCTION', with args beginning
//! with: `. Outside a transaction the same commands worked. `FUNCTION` was the
//! ONLY container with this behaviour; the other twelve all executed correctly
//! inside `MULTI`.
//!
//! # Why it is worse than wrong text
//!
//! The `MULTI` queue gate's safety argument is *queueable iff dispatchable*: it
//! reads the same `COMMAND_META` dispatch reads, so a command cannot become
//! queueable-but-undispatchable. `FUNCTION` broke that invariant — `COMMAND_META`
//! has it, so the gate queued it, but the `EXEC` executor could not reach the
//! handler and fell through to the keyspace `dispatch()`, which has no FUNCTION
//! arm. The fallback then *lied about the command existing* rather than failing
//! loudly.
//!
//! # Root cause
//!
//! `shared::is_txn_connection_intercept` listed nine connection-level intercepts
//! and `FUNCTION` was not one of them, so the executor left no
//! `TXN_INTERCEPT_PLACEHOLDER` for the connection-owning caller to fill.
//!
//! # The half that is not "add a name to a list"
//!
//! The live path follows `handle_function` with `function_fanout_op` +
//! `function_registry_fanout`, and REPLACES the local reply when a leg did not
//! reach every shard. The registry is per-SHARD-THREAD (its `RefCell` is shared
//! with that thread's SPSC drain loop, which applies inbound fan-outs), so an
//! EXEC-side intercept that skipped the fan-out would apply `FUNCTION LOAD` to
//! one shard and answer `+OK` about it. `fim4` is the test that would catch
//! that, and it only bites at `--shards 4`.

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
        let tmp_dir = std::env::temp_dir().join(format!("moon-fim697-{port}"));
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
    let tmp_dir = std::env::temp_dir().join(format!("moon-fim697-{port}"));
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

const LIB: &str =
    "#!lua name=fim697lib\nredis.register_function('fim697fn', function() return 7 end)";

// ---------------------------------------------------------------------------
// The defect
// ---------------------------------------------------------------------------

#[test]
fn fim1_function_list_inside_multi_dispatches() {
    for shards in ["1", "4"] {
        let m = spawn_moon(shards);
        let mut c = Conn::open(m.port);
        assert_eq!(c.send(&["MULTI"]), "+OK\r\n");
        assert_eq!(c.send(&["FUNCTION", "LIST"]), "+QUEUED\r\n");
        let exec = c.send(&["EXEC"]);
        assert_eq!(
            exec, "*1\r\n*0\r\n",
            "moon#697 — FUNCTION LIST inside MULTI must answer the empty library list \
             like Redis, not `unknown command` (shards={shards})"
        );
    }
}

/// The reply inside `MULTI` must equal the reply outside it.
///
/// Pinning them against EACH OTHER rather than against a literal is what makes
/// this survive a future change to the deferred-subcommand wording: the property
/// is "the transaction does not change the answer", not any one string.
#[test]
fn fim2_in_multi_replies_match_top_level_replies() {
    let m = spawn_moon("1");
    // `FUNCTION BOGUS` is deliberately absent: it is refused at QUEUE time now,
    // so it never reaches EXEC and cannot be compared this way. `fim3` owns it,
    // and matches the measured oracle — redis-server 8.6.1 answers
    // `-ERR unknown subcommand 'BOGUS'` at queue time and `-EXECABORT` at EXEC,
    // exactly as it does for CONFIG and OBJECT. (moon#697's issue table claims
    // Redis answers `*1` + the error instead; re-measured on 2026-08-24, it does
    // not — see the correction posted on the issue.)
    for argv in [
        vec!["FUNCTION", "LIST"],
        vec!["FUNCTION", "DUMP"],
        vec!["FUNCTION", "STATS"],
        vec!["FUNCTION", "HELP"],
    ] {
        // Fresh connections: MULTI state is per-connection, and one poisoned
        // transaction would make every later row report EXECABORT.
        let mut top = Conn::open(m.port);
        let direct = top.send(&argv);

        let mut txn = Conn::open(m.port);
        assert_eq!(txn.send(&["MULTI"]), "+OK\r\n");
        let queued = txn.send(&argv);
        assert_eq!(queued, "+QUEUED\r\n", "{argv:?} must queue (moon#697/#670)");
        let exec = txn.send(&["EXEC"]);

        assert_eq!(
            exec,
            format!("*1\r\n{direct}"),
            "moon#697 — {argv:?} answered differently inside MULTI than outside it"
        );
    }
}

/// A bogus subcommand must now be refused at QUEUE time like the other twelve.
///
/// This is what `csp6` in the moon#670 suite was the known-red fence for: once
/// FUNCTION dispatches inside MULTI, the queue gate's "queueable iff
/// dispatchable" invariant holds for it too, so it joins the gate.
#[test]
fn fim3_bogus_subcommand_is_refused_at_queue_time_and_aborts() {
    for shards in ["1", "4"] {
        let m = spawn_moon(shards);
        let mut c = Conn::open(m.port);
        assert_eq!(c.send(&["MULTI"]), "+OK\r\n");
        assert_eq!(
            c.send(&["FUNCTION", "BOGUS"]),
            "-ERR unknown subcommand 'BOGUS'. Try FUNCTION HELP.\r\n",
            "moon#697 closes the moon#670 exclusion (shards={shards})"
        );
        assert_eq!(
            c.send(&["EXEC"]),
            "-EXECABORT Transaction discarded because of previous errors.\r\n"
        );
    }
}

/// `FUNCTION LOAD` inside `MULTI` must reach EVERY shard, not just one.
///
/// The registry is per-shard-thread and made global by an explicit fan-out that
/// the live path runs after `handle_function`. An EXEC-side intercept that
/// dispatched but skipped the fan-out would pass `fim1`/`fim2` and still leave
/// the library on a single shard — visible only to connections that landed
/// there. Reading it back over several fresh connections is what exposes that,
/// and only at `--shards 4`.
#[test]
fn fim4_function_load_inside_multi_fans_out_to_every_shard() {
    let m = spawn_moon("4");
    let mut c = Conn::open(m.port);
    assert_eq!(c.send(&["MULTI"]), "+OK\r\n");
    assert_eq!(c.send(&["FUNCTION", "LOAD", LIB]), "+QUEUED\r\n");
    let exec = c.send(&["EXEC"]);
    assert!(
        exec.starts_with("*1\r\n") && exec.contains("fim697lib"),
        "FUNCTION LOAD inside MULTI must report the library name; got {exec:?}"
    );

    // Several FRESH connections: each may land on a different shard thread, and
    // a load that reached only one would show up as an empty list on some of
    // them. Twelve is well past the point where four shards all get hit.
    for probe in 0..12 {
        let mut p = Conn::open(m.port);
        let listed = p.send(&["FUNCTION", "LIST"]);
        assert!(
            listed.contains("fim697lib"),
            "moon#697 — a library loaded inside MULTI is missing on probe {probe}: the \
             EXEC-side intercept dispatched but skipped the cross-shard fan-out.\n  got: {listed:?}"
        );
    }
}
