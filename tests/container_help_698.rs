//! `<CONTAINER> HELP` must answer Redis's shape on every container — moon#698.
//!
//! Redis gives every container command a `HELP` subcommand. Moon served it on
//! five and refused it on eight, each with a different error — two of them an
//! *arity* error, which reads to a client as "the subcommand exists but you
//! called it wrong", and three (`CONFIG`, `COMMAND`, `FUNCTION`) with a message
//! that told the client to run the exact command it had just refused.
//!
//! **Measured, not recalled.** The expected shape came from sweeping all 13
//! containers against `redis-server 8.6.1` on 2026-08-24 reading RESP *types*,
//! not just "an array came back". Every Redis help reply is uniform:
//!
//!   * an array whose elements are all **simple** strings — never bulk strings;
//!   * opening with `<CONTAINER> <subcommand> [<arg> [value] [opt] ...]. Subcommands are:`;
//!   * closing with `HELP` and `    Print this help.`
//!
//! That type detail is the half a type-blind probe misses: judged only on "an
//! array came back", `OBJECT`/`MEMORY`/`SLOWLOG` looked correct while emitting
//! bulk strings and no header line. Only `ACL` and `MODULE` actually matched.
//!
//! The body advertises what **Moon** dispatches, not what Redis dispatches, and
//! `ch2` ties the two tables together so they cannot drift: copying Redis's own
//! help text would have advertised four `CLIENT KILL` filters Moon silently
//! ignores (`parse_kill_args` supports `ID`/`ADDR`/`USER` and the legacy
//! `addr:port` only).

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
        let tmp_dir = std::env::temp_dir().join(format!("moon-ch698-{port}"));
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
    let tmp_dir = std::env::temp_dir().join(format!("moon-ch698-{port}"));
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

/// Every container that must answer `HELP`.
///
/// `CLUSTER` is excluded and fenced by `ch5`: with cluster support disabled Moon
/// answers *every* CLUSTER subcommand — `HELP` included — with "This instance
/// has cluster support disabled", which is also what Redis does when built
/// without cluster support. `DEBUG` is excluded because Redis refuses `DEBUG`
/// outright unless `enable-debug-command` is set, so there is no oracle reply to
/// match; its help stays Moon's own convention.
const CONTAINERS: &[&str] = &[
    "ACL", "CLIENT", "COMMAND", "CONFIG", "FUNCTION", "MEMORY", "MODULE", "OBJECT", "PUBSUB",
    "SCRIPT", "SLOWLOG", "XGROUP", "XINFO",
];

/// Split a RESP array reply into `(type_byte, payload)` pairs.
///
/// Deliberately refuses anything that is not a flat array of line-terminated
/// items: a help reply that came back as an error, or with a nested container,
/// must fail loudly here rather than silently compare as an empty list.
fn parse_flat_array(raw: &str) -> Vec<(char, String)> {
    let mut lines = raw.split_inclusive("\r\n");
    let head = lines.next().unwrap_or_default();
    let head_ty = head.chars().next().unwrap_or('?');
    assert_eq!(
        head_ty, '*',
        "expected an array reply, got {head_ty:?}: {raw:?}"
    );
    let n: usize = head[1..]
        .trim_end()
        .parse()
        .unwrap_or_else(|e| panic!("bad array length in {raw:?}: {e}"));
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let line = lines
            .next()
            .unwrap_or_else(|| panic!("short array: {raw:?}"));
        let ty = line.chars().next().unwrap_or('?');
        match ty {
            '+' | '-' => out.push((ty, line[1..].trim_end_matches("\r\n").to_string())),
            '$' => {
                // A bulk string's payload is on the FOLLOWING line. Reading it
                // is what lets the assertion report "bulk, not simple" with the
                // offending text rather than a parse panic.
                let body = lines
                    .next()
                    .unwrap_or_else(|| panic!("short bulk: {raw:?}"));
                out.push((ty, body.trim_end_matches("\r\n").to_string()));
            }
            other => panic!("help arrays are flat; got element type {other:?} in {raw:?}"),
        }
    }
    out
}

fn help_lines(c: &mut Conn, container: &str) -> Vec<(char, String)> {
    parse_flat_array(&c.send(&[container, "HELP"]))
}

// ---------------------------------------------------------------------------
// The shape
// ---------------------------------------------------------------------------

#[test]
fn ch1_every_container_answers_help_with_redis_shape() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    for container in CONTAINERS {
        let items = help_lines(&mut c, container);
        assert!(
            items.len() >= 3,
            "{container} HELP must carry a header, at least one body line and the HELP footer; \
             got {items:?}"
        );

        // Element TYPE is the half a type-blind probe misses. Redis emits simple
        // strings; Moon emitted bulk strings on three of the five containers
        // that already "worked".
        let wrong: Vec<&(char, String)> = items.iter().filter(|(ty, _)| *ty != '+').collect();
        assert!(
            wrong.is_empty(),
            "{container} HELP must be simple strings like Redis, not bulk; offenders: {wrong:?}"
        );

        assert_eq!(
            items[0].1,
            format!("{container} <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
            "{container} HELP header must match Redis verbatim"
        );
        let tail: Vec<&str> = items[items.len() - 2..]
            .iter()
            .map(|(_, s)| s.as_str())
            .collect();
        assert_eq!(
            tail,
            vec!["HELP", "    Print this help."],
            "{container} HELP must close with Redis's HELP footer"
        );
    }
}

// ---------------------------------------------------------------------------
// The drift guard
// ---------------------------------------------------------------------------

#[test]
fn ch2_help_advertises_every_subcommand_moon_actually_dispatches() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    for container in CONTAINERS {
        let items = help_lines(&mut c, container);
        let body: Vec<&str> = items.iter().map(|(_, s)| s.as_str()).collect();
        let Some(subs) = moon::command::metadata::SUBCOMMAND_META.get(container) else {
            panic!("{container} has no SUBCOMMAND_META entry — the sweep list went stale");
        };
        for sub in subs.iter() {
            // A subcommand's name must open one of the body lines. Matching on
            // "appears somewhere" would pass on a prose mention, which is not
            // the same promise.
            assert!(
                body.iter().any(|l| l.starts_with(sub.name)),
                "{container} HELP does not advertise {} — help text and \
                 SUBCOMMAND_META must not drift (moon#698)\n  body: {body:#?}",
                sub.name
            );
        }
    }
}

/// The converse of `ch2`: help must not advertise what Moon will refuse.
///
/// `FUNCTION DUMP`/`RESTORE`/`STATS` are *recognised* names that answer "not
/// supported in this release", so a client reading them out of `FUNCTION HELP`
/// would be told about three subcommands it cannot use.
#[test]
fn ch3_help_does_not_advertise_subcommands_moon_refuses() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    for (container, absent) in [
        ("FUNCTION", &["DUMP", "RESTORE", "STATS", "KILL"][..]),
        ("SCRIPT", &["KILL", "DEBUG"][..]),
        ("COMMAND", &["GETKEYSANDFLAGS"][..]),
        ("CLIENT", &["CACHING", "GETREDIR", "REPLY", "SETINFO"][..]),
    ] {
        let items = help_lines(&mut c, container);
        for name in absent {
            assert!(
                !items.iter().any(|(_, l)| l.starts_with(name)),
                "{container} HELP advertises {name}, which Moon does not dispatch (moon#698)"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// The moon#670 consequence this fix removes
// ---------------------------------------------------------------------------

#[test]
fn ch4_help_queues_and_runs_inside_multi() {
    for shards in ["1", "4"] {
        let m = spawn_moon(shards);
        for container in CONTAINERS {
            // moon#697: inside MULTI, Moon's executor answers `ERR unknown
            // command 'FUNCTION'` for EVERY FUNCTION subcommand, valid ones
            // included — a dispatch defect that has nothing to do with HELP.
            // Fenced by `ch7` rather than silently skipped, so this exclusion
            // fails loudly the moment #697 is fixed.
            if *container == "FUNCTION" {
                continue;
            }
            // One FRESH connection per case: MULTI state is per-connection, and
            // a poisoned transaction left behind by a previous case would make
            // every later row report EXECABORT for the wrong reason.
            let mut c = Conn::open(m.port);
            assert_eq!(c.send(&["MULTI"]), "+OK\r\n");
            assert_eq!(
                c.send(&[container, "HELP"]),
                "+QUEUED\r\n",
                "{container} HELP must QUEUE at shards={shards}; moon#670's gate refuses any \
                 subcommand absent from SUBCOMMAND_META, so this is what the new entry buys"
            );
            let exec = c.send(&["EXEC"]);
            assert!(
                exec.starts_with("*1\r\n*"),
                "{container} HELP inside MULTI must EXEC into a one-element array holding the \
                 help array at shards={shards}; got {exec:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Fences — what must NOT change
// ---------------------------------------------------------------------------

#[test]
fn ch5_cluster_help_stays_masked_by_the_cluster_disabled_reply() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    // Not an oversight: with cluster support off, dispatch answers this for
    // EVERY CLUSTER subcommand. Serving a help array here would make CLUSTER the
    // one container whose HELP disagrees with what the rest of the container
    // does.
    assert_eq!(
        c.send(&["CLUSTER", "HELP"]),
        "-ERR This instance has cluster support disabled\r\n"
    );
}

#[test]
fn ch6_a_bogus_subcommand_is_still_refused_and_still_aborts_the_transaction() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    // moon#670's guarantee must survive adding HELP: the fix adds ONE name to
    // each container's table, and every other name stays unknown.
    assert_eq!(
        c.send(&["CONFIG", "HELPME"]),
        "-ERR unknown subcommand 'HELPME'. Try CONFIG HELP.\r\n",
        "a name that merely starts with HELP is not HELP"
    );
    let mut t = Conn::open(m.port);
    assert_eq!(t.send(&["MULTI"]), "+OK\r\n");
    assert_eq!(
        t.send(&["XINFO", "BOGUS", "k"]),
        "-ERR unknown subcommand 'BOGUS'. Try XINFO HELP.\r\n"
    );
    assert_eq!(
        t.send(&["EXEC"]),
        "-EXECABORT Transaction discarded because of previous errors.\r\n"
    );
}

/// The fence for `ch4`'s one exclusion.
///
/// If this test starts failing, moon#697 has been fixed and `FUNCTION` must be
/// removed from `ch4`'s skip list — an exclusion that outlives its cause is how
/// a suite quietly stops covering something.
#[test]
fn ch7_function_inside_multi_still_hits_moon_697() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    assert_eq!(c.send(&["MULTI"]), "+OK\r\n");
    assert_eq!(c.send(&["FUNCTION", "HELP"]), "+QUEUED\r\n");
    let exec = c.send(&["EXEC"]);
    assert!(
        exec.contains("unknown command 'FUNCTION'"),
        "moon#697 appears fixed — FUNCTION now dispatches inside MULTI, so drop the \
         FUNCTION skip in ch4 and let it assert the help array like every other \
         container; got {exec:?}"
    );
}
