//! moon#827 — a blocking pop that succeeds must reach the durability planes.
//!
//! ## The bug this file exists to keep closed
//!
//! A blocking pop that actually pops mutates the keyspace and acks the client,
//! but outside `MULTI` it writes **nothing** to the AOF and sends nothing to a
//! replica. The pop is undone by the next restart and never happens on a
//! replica. Measured before the fix, single shard, `--appendonly yes`, all
//! eight commands on both the immediately-satisfiable and the
//! parked-then-woken path — sixteen cases, sixteen losses. The incremental AOF
//! held the sixteen setup `RPUSH`/`ZADD` records and not one pop.
//!
//! ```text
//! RPUSH q a b c
//! BLPOP q 0        -> "q","a"      LRANGE q 0 -1 = b,c
//! [restart]
//!                                  LRANGE q 0 -1 = a,b,c   <- "a" is back
//! ```
//!
//! For the overwhelmingly common use — a queue consumer on `BLPOP` — this
//! redelivers every message already consumed, after a master restart or on
//! failover.
//!
//! ## Why the verbatim command is not the fix
//!
//! The record cannot be the blocking command itself: a replica applying a
//! literal `BLPOP` would park its apply loop, and an AOF replaying one would
//! stall recovery. The propagated record has to be the SYNTHESISED
//! non-blocking sibling (`BLPOP` -> `LPOP`, `BZPOPMIN` -> `ZPOPMIN`, …),
//! scoped to the key that actually served — which the multi-key form of the
//! command is not.
//!
//! moon already knows this and already does it, but only for the queued-in-
//! `MULTI` path (`src/server/conn/blocking_txn.rs`, reached from
//! `shared.rs`). The A/B that localised the bug — same server, same command,
//! only `MULTI` differs:
//!
//! ```text
//! BLPOP standalone 0            -> AOF: (nothing)
//! MULTI / BLPOP intxn 0 / EXEC  -> AOF: *2 $4 LPOP $5 intxn
//! ```
//!
//! ## What these tests prove
//!
//! Both assert the **user-visible** fact rather than the AOF bytes: after a
//! restart, an element the server already handed to a client must not come
//! back. That is the assertion that fails as DATA LOSS rather than as a
//! missing log record, and it holds whichever sibling the fix chooses to
//! synthesise.
//!
//! The two paths are separate code — the immediate path returns from the pop
//! helper inline, the parked path completes through the wakeup machinery — so
//! a fix to one does not imply the other. Both are covered.
//!
//! Run alone with: cargo test --test blocking_pop_propagation_827

mod common;

use std::process::Child;
use std::time::Duration;

use common::{Conn, spawn_listening_guarded, unique_test_dir};

fn spawn(port: u16, dir: &std::path::Path) -> Child {
    std::process::Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--shards",
            "1",
            "--appendonly",
            "yes",
            "--appendfsync",
            "everysec",
            // Same reason as every other durability suite: below the ~5%-free
            // threshold every write answers `MOONERR diskfull` and the
            // assertions never run. A guard that stops running on a full disk
            // is not a guard.
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon")
}

/// One blocking-pop case.
///
/// `setup` builds the collection, `pop` is the blocking command, and `probe`
/// must answer `after` once the pop has happened — both before and after a
/// restart. `label` names the row in a failure.
struct Case {
    label: &'static str,
    setup: &'static [&'static [&'static str]],
    pop: &'static [&'static str],
    probe: &'static [&'static str],
    after: &'static str,
}

/// All eight blocking pops.
///
/// Every case pushes MORE than it pops, so the probe distinguishes "the pop
/// was replayed" from "the key never existed" — a case that emptied its
/// collection would answer the same way whether the pop or the setup was the
/// thing that went missing, and would be a vacuous row.
///
/// Timeouts are non-zero so a regression that stops waking the waiter fails as
/// a timeout rather than hanging the suite forever.
const CASES: &[Case] = &[
    Case {
        label: "BLPOP",
        setup: &[&["RPUSH", "k827:blpop", "a", "b"]],
        pop: &["BLPOP", "k827:blpop", "5"],
        probe: &["LRANGE", "k827:blpop", "0", "-1"],
        after: "*1\r\n$1\r\nb\r\n",
    },
    Case {
        label: "BRPOP",
        setup: &[&["RPUSH", "k827:brpop", "a", "b"]],
        pop: &["BRPOP", "k827:brpop", "5"],
        probe: &["LRANGE", "k827:brpop", "0", "-1"],
        after: "*1\r\n$1\r\na\r\n",
    },
    Case {
        label: "BLMOVE",
        setup: &[&["RPUSH", "k827:blmove:s", "a", "b"]],
        pop: &[
            "BLMOVE",
            "k827:blmove:s",
            "k827:blmove:d",
            "LEFT",
            "RIGHT",
            "5",
        ],
        probe: &["LRANGE", "k827:blmove:s", "0", "-1"],
        after: "*1\r\n$1\r\nb\r\n",
    },
    Case {
        label: "BRPOPLPUSH",
        setup: &[&["RPUSH", "k827:brpoplpush:s", "a", "b"]],
        pop: &["BRPOPLPUSH", "k827:brpoplpush:s", "k827:brpoplpush:d", "5"],
        probe: &["LRANGE", "k827:brpoplpush:s", "0", "-1"],
        after: "*1\r\n$1\r\na\r\n",
    },
    Case {
        label: "BZPOPMIN",
        setup: &[&["ZADD", "k827:bzpopmin", "1", "m1", "2", "m2"]],
        pop: &["BZPOPMIN", "k827:bzpopmin", "5"],
        probe: &["ZRANGE", "k827:bzpopmin", "0", "-1"],
        after: "*1\r\n$2\r\nm2\r\n",
    },
    Case {
        label: "BZPOPMAX",
        setup: &[&["ZADD", "k827:bzpopmax", "1", "m1", "2", "m2"]],
        pop: &["BZPOPMAX", "k827:bzpopmax", "5"],
        probe: &["ZRANGE", "k827:bzpopmax", "0", "-1"],
        after: "*1\r\n$2\r\nm1\r\n",
    },
    Case {
        label: "BLMPOP",
        setup: &[&["RPUSH", "k827:blmpop", "a", "b"]],
        pop: &["BLMPOP", "5", "1", "k827:blmpop", "LEFT"],
        probe: &["LRANGE", "k827:blmpop", "0", "-1"],
        after: "*1\r\n$1\r\nb\r\n",
    },
    Case {
        label: "BZMPOP",
        setup: &[&["ZADD", "k827:bzmpop", "1", "m1", "2", "m2"]],
        pop: &["BZMPOP", "5", "1", "k827:bzmpop", "MIN"],
        probe: &["ZRANGE", "k827:bzmpop", "0", "-1"],
        after: "*1\r\n$2\r\nm2\r\n",
    },
];

/// A reply that is a RESP error or a null — i.e. the pop did not happen.
///
/// Guards against the vacuous version of this suite: if the pop silently
/// failed, the post-restart state would trivially match a state that was never
/// mutated, and every row would pass.
fn is_miss(reply: &str) -> bool {
    reply.starts_with('-') || reply.starts_with("*-1") || reply.starts_with("$-1")
}

#[test]
fn every_blocking_pop_survives_restart_when_satisfied_immediately() {
    let dir = unique_test_dir("blocking_pop_827_immediate");
    let (mut guard, port) = spawn_listening_guarded(|p| spawn(p, &dir));

    let mut c = Conn::open(port);
    for case in CASES {
        for cmd in case.setup {
            c.send(cmd);
        }
        let reply = c.send(case.pop);
        assert!(
            !is_miss(&reply),
            "{}: the pop itself failed ({reply:?}) — this row would prove nothing",
            case.label
        );
    }

    // The state the client was told about, captured BEFORE the kill. Comparing
    // the recovered state against these rather than against a literal is what
    // makes a mutation of the fix fail here: if the pop never happened at all,
    // the pre-kill assertion above catches it instead.
    let before: Vec<String> = CASES.iter().map(|c2| c.send(c2.probe)).collect();
    for (case, got) in CASES.iter().zip(&before) {
        assert_eq!(
            got, case.after,
            "{}: pop did not take effect before restart",
            case.label
        );
    }

    // A control write that MUST survive. If the AOF itself is broken, every
    // row below would "pass" by losing nothing that was ever there.
    c.send(&["SET", "k827:control", "1"]);
    std::thread::sleep(Duration::from_millis(1500));

    drop(c);
    guard.kill_now();
    common::wait_for_port_down(port);
    let (_g2, port2) = spawn_listening_guarded(|p| spawn(p, &dir));
    let mut c = Conn::open(port2);
    assert_eq!(
        c.send(&["GET", "k827:control"]),
        "$1\r\n1\r\n",
        "the control write did not survive the restart — the AOF itself is \
         broken, so nothing else this test says is meaningful"
    );

    let mut lost = Vec::new();
    for (case, was) in CASES.iter().zip(&before) {
        let now = c.send(case.probe);
        if &now != was {
            lost.push(format!("{}: before={was:?} after={now:?}", case.label));
        }
    }
    assert!(
        lost.is_empty(),
        "{} of {} blocking pops were undone by the restart — the write was acked \
         to the client but never propagated:\n  {}",
        lost.len(),
        CASES.len(),
        lost.join("\n  ")
    );
}

#[test]
fn every_blocking_pop_survives_restart_when_parked_then_woken() {
    let dir = unique_test_dir("blocking_pop_827_parked");
    let (mut guard, port) = spawn_listening_guarded(|p| spawn(p, &dir));

    // Park each pop on an EMPTY key, then push what wakes it. This is the
    // other code path: the pop completes inside the wakeup machinery rather
    // than returning inline from the dispatch call.
    for case in CASES {
        let pop: Vec<String> = case.pop.iter().map(|s| (*s).to_string()).collect();
        let waiter = std::thread::spawn(move || {
            let mut w = Conn::open(port);
            let refs: Vec<&str> = pop.iter().map(String::as_str).collect();
            w.send(&refs)
        });

        // Give the waiter time to reach its parked state before the write that
        // wakes it. Too short and the push races ahead of the registration,
        // which the server must also handle but which exercises the IMMEDIATE
        // path — the one the other test already covers.
        std::thread::sleep(Duration::from_millis(400));

        let mut c = Conn::open(port);
        for cmd in case.setup {
            c.send(cmd);
        }

        let reply = waiter.join().expect("waiter thread");
        assert!(
            !is_miss(&reply),
            "{}: the parked pop was never served ({reply:?}) — this row would \
             prove nothing",
            case.label
        );
    }

    let mut c = Conn::open(port);
    let before: Vec<String> = CASES.iter().map(|c2| c.send(c2.probe)).collect();
    for (case, got) in CASES.iter().zip(&before) {
        assert_eq!(
            got, case.after,
            "{}: parked pop did not take effect before restart",
            case.label
        );
    }

    c.send(&["SET", "k827:control", "1"]);
    std::thread::sleep(Duration::from_millis(1500));

    drop(c);
    guard.kill_now();
    common::wait_for_port_down(port);
    let (_g2, port2) = spawn_listening_guarded(|p| spawn(p, &dir));
    let mut c = Conn::open(port2);
    assert_eq!(
        c.send(&["GET", "k827:control"]),
        "$1\r\n1\r\n",
        "the control write did not survive the restart — the AOF itself is \
         broken, so nothing else this test says is meaningful"
    );

    let mut lost = Vec::new();
    for (case, was) in CASES.iter().zip(&before) {
        let now = c.send(case.probe);
        if &now != was {
            lost.push(format!("{}: before={was:?} after={now:?}", case.label));
        }
    }
    assert!(
        lost.is_empty(),
        "{} of {} parked-then-woken blocking pops were undone by the restart:\n  {}",
        lost.len(),
        CASES.len(),
        lost.join("\n  ")
    );
}
