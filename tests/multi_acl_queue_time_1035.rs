//! moon#1035: a command the user's ACL denies, sent inside `MULTI`, must
//! poison the transaction so `EXEC` applies NOTHING.
//!
//! Before the fix moon answered the denied command `NOPERM`, did not queue
//! it, and then `EXEC` applied the rest of the transaction. Every expectation
//! below was measured against redis-server 8.6.1 over a raw socket:
//!
//! ```text
//! MULTI / SET mx 1 / FLUSHALL / SET my 1 / EXEC        (user: +@all -flushall)
//!   redis : +OK +QUEUED -NOPERM +QUEUED -EXECABORT     mx, my unset
//!   moon  : +OK +QUEUED -NOPERM +QUEUED *2 +OK +OK     mx, my SET
//! ```
//!
//! The same holds for a denied KEY (`~ok:*` user writing `secret`) and a
//! denied CHANNEL (`&allowed` user publishing to `secret`); redis flags the
//! transaction for every ACL rejection, whatever its source.
//!
//! Runs a real server binary at `--shards 1` and `--shards 4` — the queue gate
//! lives in the connection handlers, and at `--shards 4` a single-owner body
//! is shipped to its owner shard (moon#247), a path the poison must precede.
//! Which RUNTIME is exercised is whichever the binary was built with; CI runs
//! this file on both.

mod common;

use std::process::{Command, Stdio};

use common::Conn;

const SHARD_COUNTS: [&str; 2] = ["1", "4"];

const EXECABORT: &str = "-EXECABORT Transaction discarded because of previous errors.\r\n";
/// redis-server 8.6.1's text, byte for byte; moon's matches.
const NOPERM_FLUSHALL: &str = "-NOPERM User u has no permissions to run the 'flushall' command\r\n";

struct Moon {
    _guard: common::ServerGuard,
    port: u16,
    dir: std::path::PathBuf,
}

impl Drop for Moon {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn spawn_moon(shards: &str) -> Moon {
    // `find_moon_binary` honours MOON_BIN, which is how this suite is run
    // against a pre-fix binary to show it red.
    let bin = common::find_moon_binary();
    let dir_cell = std::cell::RefCell::new(std::path::PathBuf::new());
    let (guard, port) = common::spawn_listening_guarded(|port| {
        let dir = common::unique_test_dir(&format!("multi-acl-1035-{shards}-{port}"));
        let _ = std::fs::create_dir_all(&dir);
        *dir_cell.borrow_mut() = dir.clone();
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
                dir.to_str().expect("utf8 dir"),
            ])
            .stdout(Stdio::null())
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon")
    });
    let dir = dir_cell.into_inner();
    // Prove the listener is OURS, not a stray redis-server on a reused port.
    let info = Conn::open(port).send(&["INFO", "server"]);
    assert!(
        info.contains("moon_version"),
        "port {port} is held by something that is not moon: {info:?}"
    );
    let mut admin = Conn::open(port);
    for rule in [
        // Command denial (and, once moon enforces it, a subcommand denial).
        &[
            "ACL",
            "SETUSER",
            "u",
            "reset",
            "on",
            "nopass",
            "~*",
            "&*",
            "+@all",
            "-flushall",
            "-config|set",
        ][..],
        // Key-pattern denial.
        &[
            "ACL", "SETUSER", "k", "reset", "on", "nopass", "~ok:*", "&*", "+@all",
        ][..],
        // Channel-pattern denial.
        &[
            "ACL",
            "SETUSER",
            "c",
            "reset",
            "on",
            "nopass",
            "~*",
            "resetchannels",
            "&allowed",
            "+@all",
        ][..],
    ] {
        let r = admin.send(rule);
        assert_eq!(r, "+OK\r\n", "{rule:?}");
    }
    Moon {
        _guard: guard,
        port,
        dir,
    }
}

/// A fresh connection authenticated as `user` (every test user is `nopass`).
fn login(port: u16, user: &str) -> Conn {
    let mut c = Conn::open(port);
    let r = c.send(&["AUTH", user, "x"]);
    assert_eq!(r, "+OK\r\n", "AUTH {user}");
    c
}

fn get(port: u16, key: &str) -> String {
    Conn::open(port).send(&["GET", key])
}

/// Send each command on its own round trip and return the raw replies.
fn each(c: &mut Conn, cmds: &[&[&str]]) -> Vec<String> {
    cmds.iter().map(|argv| c.send(argv)).collect()
}

fn denied_command_poisons_txn(port: u16, shards: &str) {
    let mut u = login(port, "u");
    let got = each(
        &mut u,
        &[
            &["MULTI"],
            &["SET", "mx", "1"],
            &["FLUSHALL"],
            &["SET", "my", "1"],
            &["EXEC"],
        ],
    );
    assert_eq!(
        got,
        [
            "+OK\r\n",
            "+QUEUED\r\n",
            NOPERM_FLUSHALL,
            "+QUEUED\r\n",
            EXECABORT
        ],
        "shards={shards}: a denied command must poison the transaction"
    );
    assert_eq!(
        get(port, "mx"),
        "$-1\r\n",
        "shards={shards}: nothing may apply"
    );
    assert_eq!(
        get(port, "my"),
        "$-1\r\n",
        "shards={shards}: nothing may apply"
    );

    // The refusal logs what the same refusal outside MULTI logs. moon keeps
    // the log per connection, so ask the refused connection.
    let log = u.send(&["ACL", "LOG", "1"]);
    let mut top = login(port, "u");
    assert_eq!(
        top.send(&["FLUSHALL"]),
        NOPERM_FLUSHALL,
        "top-level control"
    );
    let top_log = top.send(&["ACL", "LOG", "1"]);
    for field in ["command", "flushall", "\r\nu\r\n"] {
        assert!(
            log.contains(field) && top_log.contains(field),
            "shards={shards}: ACL LOG entry must carry {field:?} in MULTI and out.\n\
             in MULTI: {log:?}\ntop-level: {top_log:?}"
        );
    }
}

fn denied_command_in_one_pipelined_write(port: u16, shards: &str) {
    let mut u = login(port, "u");
    let got = u.pipeline(&[
        &["MULTI"],
        &["SET", "px", "1"],
        &["FLUSHALL"],
        &["SET", "py", "1"],
        &["EXEC"],
    ]);
    let want = [
        "+OK\r\n",
        "+QUEUED\r\n",
        NOPERM_FLUSHALL,
        "+QUEUED\r\n",
        EXECABORT,
    ]
    .concat();
    assert_eq!(got, want, "shards={shards}: pipelined MULTI must abort too");
    assert_eq!(get(port, "px"), "$-1\r\n", "shards={shards}");
    assert_eq!(get(port, "py"), "$-1\r\n", "shards={shards}");
}

fn watch_is_cleared_by_the_abort(port: u16, shards: &str) {
    let mut u = login(port, "u");
    let got = each(
        &mut u,
        &[
            &["WATCH", "w"],
            &["MULTI"],
            &["SET", "w", "1"],
            &["FLUSHALL"],
            &["EXEC"],
        ],
    );
    assert_eq!(
        got,
        [
            "+OK\r\n",
            "+OK\r\n",
            "+QUEUED\r\n",
            NOPERM_FLUSHALL,
            EXECABORT
        ],
        "shards={shards}: WATCH + denied command"
    );
    assert_eq!(get(port, "w"), "$-1\r\n", "shards={shards}");
    // The aborted EXEC unwatched `w`: touching it now must not abort the NEXT
    // transaction (redis: `*1 +OK`).
    assert_eq!(Conn::open(port).send(&["SET", "w", "other"]), "+OK\r\n");
    let got = each(&mut u, &[&["MULTI"], &["SET", "w2", "1"], &["EXEC"]]);
    assert_eq!(
        got,
        ["+OK\r\n", "+QUEUED\r\n", "*1\r\n+OK\r\n"],
        "shards={shards}: a stale watch survived the abort"
    );
}

fn discard_clears_the_poison(port: u16, shards: &str) {
    let mut u = login(port, "u");
    let got = each(
        &mut u,
        &[
            &["MULTI"],
            &["SET", "d", "1"],
            &["FLUSHALL"],
            &["DISCARD"],
            &["EXEC"],
            &["MULTI"],
            &["SET", "d2", "1"],
            &["EXEC"],
        ],
    );
    assert_eq!(
        got,
        [
            "+OK\r\n",
            "+QUEUED\r\n",
            NOPERM_FLUSHALL,
            "+OK\r\n",
            "-ERR EXEC without MULTI\r\n",
            "+OK\r\n",
            "+QUEUED\r\n",
            "*1\r\n+OK\r\n",
        ],
        "shards={shards}: DISCARD must clear the poison, or the next MULTI aborts"
    );
    assert_eq!(get(port, "d"), "$-1\r\n", "shards={shards}");
    assert_eq!(get(port, "d2"), "$1\r\n1\r\n", "shards={shards}");
}

/// A denied KEY poisons the transaction exactly as a denied command does.
/// moon's `NOPERM` text for a key differs from redis's (a separate, older
/// divergence), so the reply is pinned to what moon answers for the same
/// command OUTSIDE a transaction rather than to redis's string.
fn denied_key_poisons_txn(port: u16, shards: &str) {
    let top = login(port, "k").send(&["SET", "secret", "1"]);
    assert!(top.starts_with("-NOPERM"), "control: {top:?}");
    let mut k = login(port, "k");
    let got = each(
        &mut k,
        &[
            &["MULTI"],
            &["SET", "ok:a", "1"],
            &["SET", "secret", "1"],
            &["EXEC"],
        ],
    );
    assert_eq!(
        got,
        ["+OK\r\n", "+QUEUED\r\n", top.as_str(), EXECABORT],
        "shards={shards}: a denied key must poison the transaction"
    );
    assert_eq!(get(port, "ok:a"), "$-1\r\n", "shards={shards}");
}

/// A denied CHANNEL poisons it too. Before the fix `PUBLISH secret` was
/// QUEUED and refused only inside EXEC's reply, after the `SET` had applied.
fn denied_channel_poisons_txn(port: u16, shards: &str) {
    let top = login(port, "c").send(&["PUBLISH", "secret", "x"]);
    assert!(top.starts_with("-NOPERM"), "control: {top:?}");
    for verb in ["PUBLISH", "SPUBLISH"] {
        let key = format!("chan-{verb}");
        let mut c = login(port, "c");
        let got = each(
            &mut c,
            &[
                &["MULTI"],
                &["SET", &key, "1"],
                &[verb, "secret", "x"],
                &["EXEC"],
            ],
        );
        assert_eq!(
            got,
            ["+OK\r\n", "+QUEUED\r\n", top.as_str(), EXECABORT],
            "shards={shards}: {verb} to a denied channel must poison the transaction"
        );
        assert_eq!(get(port, &key), "$-1\r\n", "shards={shards}: {verb}");
    }
    // Positive control: a permitted channel still queues and runs.
    let mut c = login(port, "c");
    let got = each(
        &mut c,
        &[
            &["MULTI"],
            &["SET", "chan-ok", "1"],
            &["PUBLISH", "allowed", "x"],
            &["EXEC"],
        ],
    );
    assert_eq!(
        got,
        [
            "+OK\r\n",
            "+QUEUED\r\n",
            "+QUEUED\r\n",
            "*2\r\n+OK\r\n:0\r\n"
        ],
        "shards={shards}: a permitted PUBLISH must still run"
    );
}

/// At `--shards 4` a body whose keys all live on ONE other shard is shipped
/// there whole (moon#247). Six hash tags so at least one lands off the
/// connection's shard; the poison is checked before any routing.
fn owner_routed_body_still_poisons(port: u16, shards: &str) {
    for tag in ["a", "b", "c", "d", "e", "f"] {
        let key = format!("{{{tag}}}k");
        let mut u = login(port, "u");
        let got = each(
            &mut u,
            &[&["MULTI"], &["SET", &key, "1"], &["FLUSHALL"], &["EXEC"]],
        );
        assert_eq!(
            got,
            ["+OK\r\n", "+QUEUED\r\n", NOPERM_FLUSHALL, EXECABORT],
            "shards={shards}: {key}"
        );
        assert_eq!(get(port, &key), "$-1\r\n", "shards={shards}: {key}");
    }
}

/// The subcommand case composes rather than being special-cased: the queue
/// gate refuses exactly what the top-level gate refuses, through the same
/// `check_command_permission(user, cmd, args)`. redis 8.6.1 denies
/// `CONFIG SET` to a `-config|set` user; while moon does not yet enforce
/// per-subcommand rules this pins the invariant ("inside MULTI == outside"),
/// and it becomes the redis-parity assertion the moment it does.
fn subcommand_verdict_matches_top_level(port: u16, shards: &str) {
    let argv: &[&str] = &["CONFIG", "SET", "maxmemory-policy", "noeviction"];
    let top = login(port, "u").send(argv);
    let mut u = login(port, "u");
    let got = each(&mut u, &[&["MULTI"], &["SET", "s", "1"], argv, &["EXEC"]]);
    if top.starts_with("-NOPERM") {
        assert_eq!(
            got,
            ["+OK\r\n", "+QUEUED\r\n", top.as_str(), EXECABORT],
            "shards={shards}: a denied subcommand must poison the transaction"
        );
        assert_eq!(get(port, "s"), "$-1\r\n", "shards={shards}");
    } else {
        eprintln!("shards={shards}: CONFIG SET not denied at top level ({top:?}); invariant only");
        assert_eq!(
            got,
            [
                "+OK\r\n",
                "+QUEUED\r\n",
                "+QUEUED\r\n",
                "*2\r\n+OK\r\n+OK\r\n"
            ],
            "shards={shards}: permitted outside MULTI, so it must queue and run inside"
        );
    }
}

fn permitted_transaction_still_commits(port: u16, shards: &str) {
    let mut u = login(port, "u");
    let got = each(
        &mut u,
        &[&["MULTI"], &["SET", "pc", "1"], &["INCR", "pc"], &["EXEC"]],
    );
    assert_eq!(
        got,
        [
            "+OK\r\n",
            "+QUEUED\r\n",
            "+QUEUED\r\n",
            "*2\r\n+OK\r\n:2\r\n"
        ],
        "shards={shards}: positive control"
    );
}

#[test]
fn multi_acl_refusal_poisons_txn_1035() {
    for shards in SHARD_COUNTS {
        let moon = spawn_moon(shards);
        let port = moon.port;
        denied_command_poisons_txn(port, shards);
        denied_command_in_one_pipelined_write(port, shards);
        watch_is_cleared_by_the_abort(port, shards);
        discard_clears_the_poison(port, shards);
        denied_key_poisons_txn(port, shards);
        denied_channel_poisons_txn(port, shards);
        owner_routed_body_still_poisons(port, shards);
        subcommand_verdict_matches_top_level(port, shards);
        permitted_transaction_still_commits(port, shards);
    }
}
