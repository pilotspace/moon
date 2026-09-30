//! moon#1299 + moon#1303: a key an open cross-store `TXN` wrote is HELD until
//! the transaction commits or aborts.
//!
//! Before: another client's write to such a key was acknowledged, and the
//! transaction's `TXN ABORT` then restored its pre-image over it — the
//! acknowledged write was lost live, after a restart, and on replicas (the
//! compensation is logged since moon#1285). The fix refuses the other
//! client's write with `-TXNCONFLICT key held by an open transaction`
//! (`-TXNCONFLICT database has keys held by an open transaction` for
//! `FLUSHDB` / `FLUSHALL` / `SWAPDB`), on every write path: plain commands
//! (inline and generic), MULTI/EXEC bodies, scripts, blocking pops, and at
//! `--shards 4` the routed legs.
//!
//! moon#1303: a TXN write that answered an error (`SET k v BADOPT`, `INCR`
//! of a non-number, WRONGTYPE) wrote nothing, so it takes its undo capture
//! (and its hold) back — another client may then write the key, and the
//! abort does not restore over it.
//!
//! Each test runs at `--shards 1` and `--shards 4`, on whichever runtime
//! `MOON_BIN` was built with (run it once per runtime):
//!
//!   MOON_DISK_FREE_MIN_PCT=0 MOON_BIN=... cargo test --test txn_isolation_1299 -- --include-ignored

mod common;

use std::io::Write as _;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::Conn;

const OK: &str = "+OK\r\n";
const CONFLICT: &str = "-TXNCONFLICT";

fn bulk(s: &str) -> String {
    format!("${}\r\n{s}\r\n", s.len())
}

fn spawn(shards: usize, extra: &[&str]) -> (common::ServerGuard, u16, std::path::PathBuf) {
    let bin = common::find_moon_binary();
    let dir = common::unique_test_dir(&format!("txn-iso-{shards}"));
    let dir_s = dir.to_str().expect("utf8 dir").to_string();
    let extra: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
    let (guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(&bin)
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--shards",
                &shards.to_string(),
                "--dir",
                &dir_s,
                "--appendonly",
                "no",
                "--save",
                "",
                "--disk-free-min-pct",
                "0",
            ])
            .args(&extra)
            .env("RUST_LOG", "moon=warn")
            .stdout(Stdio::null())
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon (set MOON_BIN)")
    });
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(mut c) = std::panic::catch_unwind(|| Conn::open(port))
            && c.send(&["PING"]) == "+PONG\r\n"
        {
            break;
        }
        assert!(Instant::now() < deadline, "server on {port} never answered");
        std::thread::sleep(Duration::from_millis(100));
    }
    (guard, port, dir)
}

/// A hash tag whose keys live on `txn`'s own shard: a TXN refuses writes that
/// route elsewhere (#499), so every key a test's transaction writes carries it.
fn local_tag(txn: &mut Conn) -> String {
    for i in 0..256 {
        let tag = format!("{{t{i}}}");
        assert_eq!(txn.send(&["TXN", "BEGIN"]), OK);
        let probe = format!("{tag}probe");
        let wrote = txn.send(&["SET", &probe, "x"]);
        assert!(
            wrote == OK || wrote.contains("cross-shard"),
            "probe write: {wrote:?}"
        );
        assert_eq!(txn.send(&["TXN", "ABORT"]), OK);
        if wrote == OK {
            return tag;
        }
    }
    panic!("no hash tag routes to the transaction's shard");
}

/// Several "other clients": at `--shards 4` they land on several shards, so
/// both the local and the routed (SPSC) write legs are exercised.
fn others(port: u16) -> Vec<Conn> {
    (0..8).map(|_| Conn::open(port)).collect()
}

fn assert_refused(reply: &str, what: &str) {
    assert!(
        reply.starts_with(CONFLICT) || reply.contains("TXNCONFLICT"),
        "{what}: another client's write to a key an open TXN holds must be refused \
         with TXNCONFLICT, got {reply:?}"
    );
}

/// No reply arrives on `c` within `d` (a blocked client stays parked).
fn silent_for(c: &mut Conn, d: Duration) -> bool {
    c.sock.set_read_timeout(Some(d)).expect("timeout");
    let mut b = [0u8; 1];
    let silent = match c.sock.peek(&mut b) {
        Ok(n) => n == 0,
        Err(e) => matches!(
            e.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        ),
    };
    c.sock
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("timeout");
    silent
}

fn info_field(c: &mut Conn, field: &str) -> Option<u64> {
    let info = c.send(&["INFO", "stats"]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .and_then(|v| v.trim().parse().ok())
}

fn cleanup(mut guard: common::ServerGuard, dir: std::path::PathBuf) {
    guard.kill_now();
    let _ = std::fs::remove_dir_all(dir);
}

// ---------------------------------------------------------------------------
// The issue's repro, every write path, ABORT and COMMIT
// ---------------------------------------------------------------------------

fn foreign_writes_are_refused_and_abort_restores(shards: usize) {
    let (guard, port, dir) = spawn(shards, &[]);
    let mut a = Conn::open(port);
    let tag = local_tag(&mut a);
    let k = format!("{tag}k");
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["SET", &k, "orig"]), OK);

    assert_eq!(a.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(a.send(&["SET", &k, "txn"]), OK);
    for (i, b) in others(port).iter_mut().enumerate() {
        let what = format!("shards={shards} client {i}");
        assert_refused(&b.send(&["SET", &k, "other"]), &format!("{what} SET"));
        assert_refused(&b.send(&["APPEND", &k, "x"]), &format!("{what} APPEND"));
        assert_refused(&b.send(&["DEL", &k]), &format!("{what} DEL"));
        assert_refused(&b.send(&["EXPIRE", &k, "100"]), &format!("{what} EXPIRE"));
        assert_refused(
            &b.send(&["MSET", &format!("{tag}free"), "1", &k, "2"]),
            &format!("{what} MSET"),
        );
        // A script's write raises the refusal inside the script.
        let eval = b.send(&["EVAL", "return redis.call('SET', KEYS[1], 'lua')", "1", &k]);
        assert_refused(&eval, &format!("{what} EVAL"));
        // A MULTI/EXEC body: the queued write's element is the refusal.
        assert_eq!(b.send(&["MULTI"]), OK);
        assert_eq!(b.send(&["SET", &k, "multi"]), "+QUEUED\r\n");
        let exec = b.send(&["EXEC"]);
        assert!(exec.contains("TXNCONFLICT"), "{what} EXEC: {exec:?}");
        // Reads are not refused: they see the uncommitted value (the engine's
        // isolation contract for plain clients is unchanged).
        assert_eq!(b.send(&["GET", &k]), bulk("txn"), "{what} GET");
    }
    assert_eq!(a.send(&["TXN", "ABORT"]), OK);
    assert_eq!(
        c.send(&["GET", &k]),
        bulk("orig"),
        "shards={shards}: abort restores"
    );
    // Released: anyone writes again.
    assert_eq!(c.send(&["SET", &k, "after"]), OK);
    assert_eq!(c.send(&["GET", &k]), bulk("after"));
    cleanup(guard, dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn foreign_writes_are_refused_and_abort_restores_1_shard() {
    foreign_writes_are_refused_and_abort_restores(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn foreign_writes_are_refused_and_abort_restores_4_shards() {
    foreign_writes_are_refused_and_abort_restores(4);
}

fn commit_keeps_the_txn_value_and_releases(shards: usize) {
    let (guard, port, dir) = spawn(shards, &[]);
    let mut a = Conn::open(port);
    let tag = local_tag(&mut a);
    let k = format!("{tag}k");
    let fresh = format!("{tag}fresh");
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["SET", &k, "orig"]), OK);
    assert_eq!(a.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(a.send(&["SET", &k, "txn"]), OK);
    assert_eq!(a.send(&["SET", &fresh, "new"]), OK);
    assert_refused(&c.send(&["SET", &k, "other"]), "SET before commit");
    assert_refused(&c.send(&["SET", &fresh, "other"]), "SET of a TXN insert");
    // Another transaction is refused too, and the refusal poisons it.
    let mut t2 = Conn::open(port);
    assert_eq!(t2.send(&["TXN", "BEGIN"]), OK);
    let t2_write = t2.send(&["SET", &k, "t2"]);
    if !t2_write.contains("cross-shard") {
        assert_refused(&t2_write, "a second TXN");
    }
    let t2_commit = t2.send(&["TXN", "COMMIT"]);
    assert!(
        t2_commit.starts_with('-'),
        "a refused TXN may not commit: {t2_commit:?}"
    );
    assert_eq!(a.send(&["TXN", "COMMIT"]), OK);
    assert_eq!(
        c.send(&["GET", &k]),
        bulk("txn"),
        "shards={shards}: commit keeps"
    );
    assert_eq!(c.send(&["GET", &fresh]), bulk("new"));
    assert_eq!(c.send(&["SET", &k, "after"]), OK, "released at commit");
    cleanup(guard, dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn commit_keeps_the_txn_value_and_releases_1_shard() {
    commit_keeps_the_txn_value_and_releases(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn commit_keeps_the_txn_value_and_releases_4_shards() {
    commit_keeps_the_txn_value_and_releases(4);
}

/// A transaction ended by its client disconnecting releases its keys.
fn disconnect_releases(shards: usize) {
    let (guard, port, dir) = spawn(shards, &[]);
    let mut a = Conn::open(port);
    let tag = local_tag(&mut a);
    let k = format!("{tag}k");
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["SET", &k, "orig"]), OK);
    assert_eq!(a.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(a.send(&["SET", &k, "txn"]), OK);
    drop(a);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let r = c.send(&["SET", &k, "after"]);
        if r == OK {
            break;
        }
        assert_refused(&r, "before the disconnect is noticed");
        assert!(
            Instant::now() < deadline,
            "a disconnected TXN never released {k}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(c.send(&["GET", &k]), bulk("after"));
    cleanup(guard, dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn disconnect_releases_1_shard() {
    disconnect_releases(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn disconnect_releases_4_shards() {
    disconnect_releases(4);
}

// ---------------------------------------------------------------------------
// FLUSHDB / FLUSHALL / SWAPDB
// ---------------------------------------------------------------------------

fn whole_db_writes_are_refused_while_a_txn_holds(shards: usize) {
    let (guard, port, dir) = spawn(shards, &[]);
    let mut a = Conn::open(port);
    let tag = local_tag(&mut a);
    let k = format!("{tag}k");
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["SET", &k, "orig"]), OK);
    assert_eq!(c.send(&["SET", "unrelated", "u"]), OK);
    assert_eq!(a.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(a.send(&["SET", &k, "txn"]), OK);
    for (i, b) in others(port).iter_mut().enumerate() {
        let what = format!("shards={shards} client {i}");
        assert_refused(&b.send(&["FLUSHDB"]), &format!("{what} FLUSHDB"));
        assert_refused(&b.send(&["FLUSHALL"]), &format!("{what} FLUSHALL"));
        assert_refused(
            &b.send(&["SWAPDB", "0", "1"]),
            &format!("{what} SWAPDB 0 1"),
        );
        assert_refused(
            &b.send(&["SWAPDB", "2", "0"]),
            &format!("{what} SWAPDB 2 0"),
        );
        // A db the TXN holds nothing in is not refused.
        assert_eq!(b.send(&["SELECT", "3"]), OK);
        assert_eq!(b.send(&["FLUSHDB"]), OK, "{what} FLUSHDB of db 3");
        assert_eq!(b.send(&["SWAPDB", "3", "4"]), OK, "{what} SWAPDB 3 4");
    }
    // Nothing was cleared: the unrelated key survived every refused flush.
    assert_eq!(c.send(&["GET", "unrelated"]), bulk("u"));
    assert_eq!(a.send(&["TXN", "ABORT"]), OK);
    assert_eq!(c.send(&["GET", &k]), bulk("orig"));
    assert_eq!(c.send(&["FLUSHALL"]), OK, "released at abort");
    assert_eq!(c.send(&["GET", &k]), "$-1\r\n");
    cleanup(guard, dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn whole_db_writes_are_refused_while_a_txn_holds_1_shard() {
    whole_db_writes_are_refused_while_a_txn_holds(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn whole_db_writes_are_refused_while_a_txn_holds_4_shards() {
    whole_db_writes_are_refused_while_a_txn_holds(4);
}

// ---------------------------------------------------------------------------
// The PR #1301 review over-capture shapes (recorded on moon#1299)
// ---------------------------------------------------------------------------

/// Run `txn_cmds` inside a TXN, then have another client write `victim`
/// with `foreign`; abort. The other client's write is either refused or
/// kept — never overwritten by the abort.
fn shape(
    a: &mut Conn,
    port: u16,
    setup: &[&[&str]],
    txn_cmds: &[&[&str]],
    foreign: &[&str],
    read: &[&str],
) -> Option<String> {
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["FLUSHALL"]), OK);
    for cmd in setup {
        let r = c.send(cmd);
        assert!(!r.starts_with('-'), "setup {cmd:?}: {r:?}");
    }
    assert_eq!(a.send(&["TXN", "BEGIN"]), OK);
    for cmd in txn_cmds {
        let r = a.send(cmd);
        assert!(
            !r.contains("cross-shard"),
            "{cmd:?} must stay on the TXN's shard: {r:?}"
        );
    }
    let wrote = c.send(foreign);
    let after_write = c.send(read);
    assert_eq!(a.send(&["TXN", "ABORT"]), OK);
    let after_abort = c.send(read);
    if wrote.contains("TXNCONFLICT") {
        return None;
    }
    assert!(!wrote.starts_with('-'), "{foreign:?}: {wrote:?}");
    (after_abort != after_write).then(|| {
        format!(
            "TXN {txn_cmds:?} ABORT overwrote another client's acknowledged {foreign:?} \
             ({read:?} was {after_write:?} before the abort, {after_abort:?} after)"
        )
    })
}

fn over_capture_shapes_never_clobber(shards: usize) {
    let (guard, port, dir) = spawn(shards, &[]);
    // The TXN connection: `local_tag` found a tag on ITS shard.
    let mut t = Conn::open(port);
    let tag = local_tag(&mut t);
    let a = format!("{tag}a");
    let b = format!("{tag}b");
    let dst = format!("{tag}dst");
    let g = format!("{tag}g");
    let x = format!("{tag}x");
    let mut failures: Vec<String> = Vec::new();
    // 1. LMPOP / ZMPOP capture every candidate key.
    failures.extend(shape(
        &mut t,
        port,
        &[&["RPUSH", &a, "1", "2"]],
        &[&["LMPOP", "2", &a, &b, "LEFT"]],
        &["RPUSH", &b, "other"],
        &["LRANGE", &b, "0", "-1"],
    ));
    failures.extend(shape(
        &mut t,
        port,
        &[&["ZADD", &a, "1", "m"]],
        &[&["ZMPOP", "2", &a, &b, "MIN"]],
        &["ZADD", &b, "5", "other"],
        &["ZRANGE", &b, "0", "-1"],
    ));
    // 2. Writes that succeed and change nothing.
    failures.extend(shape(
        &mut t,
        port,
        &[&["SET", &a, "orig"]],
        &[&["SETNX", &a, "nx"]],
        &["SET", &a, "other"],
        &["GET", &a],
    ));
    failures.extend(shape(
        &mut t,
        port,
        &[&["SET", &a, "orig"]],
        &[&["SET", &a, "nx", "NX"]],
        &["SET", &a, "other"],
        &["GET", &a],
    ));
    failures.extend(shape(
        &mut t,
        port,
        &[&["SET", &a, "src"], &["SET", &dst, "orig"]],
        &[&["COPY", &a, &dst]],
        &["SET", &dst, "other"],
        &["GET", &dst],
    ));
    failures.extend(shape(
        &mut t,
        port,
        &[&["SET", &a, "src"], &["SET", &dst, "orig"]],
        &[&["RENAMENX", &a, &dst]],
        &["SET", &dst, "other"],
        &["GET", &dst],
    ));
    failures.extend(shape(
        &mut t,
        port,
        &[],
        &[&["LPUSHX", &a, "v"]],
        &["RPUSH", &a, "other"],
        &["LRANGE", &a, "0", "-1"],
    ));
    failures.extend(shape(
        &mut t,
        port,
        &[&["SADD", &a, "m1"], &["SADD", &dst, "d1"]],
        &[&["SMOVE", &a, &dst, "missing"]],
        &["SADD", &dst, "other"],
        &["SMEMBERS", &dst],
    ));
    // 3. Key-walker corner cases: a member literally named STORE, XGROUP HELP.
    failures.extend(shape(
        &mut t,
        port,
        &[&["GEOADD", &g, "13.36", "38.11", "STORE"]],
        &[&["GEORADIUSBYMEMBER", &g, "STORE", "100", "km"]],
        &["SET", "100", "other"],
        &["GET", "100"],
    ));
    failures.extend(shape(
        &mut t,
        port,
        &[],
        &[&["XGROUP", "HELP", &x]],
        &["SET", &x, "other"],
        &["GET", &x],
    ));
    assert!(
        failures.is_empty(),
        "shards={shards}: {} over-capture shape(s) lost another client's write:\n{}",
        failures.len(),
        failures.join("\n")
    );
    cleanup(guard, dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn over_capture_shapes_never_clobber_1_shard() {
    over_capture_shapes_never_clobber(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn over_capture_shapes_never_clobber_4_shards() {
    over_capture_shapes_never_clobber(4);
}

// ---------------------------------------------------------------------------
// moon#1303: an erroring connection write keeps no capture
// ---------------------------------------------------------------------------

fn an_erroring_write_holds_nothing(shards: usize) {
    let (guard, port, dir) = spawn(shards, &[]);
    let mut a = Conn::open(port);
    let tag = local_tag(&mut a);
    let k = format!("{tag}k");
    let n = format!("{tag}n");
    let l = format!("{tag}l");
    let mut c = Conn::open(port);
    for (setup, bad, key) in [
        (
            vec!["SET", k.as_str(), "orig"],
            vec!["SET", k.as_str(), "v", "BADOPT"],
            &k,
        ),
        (vec!["SET", n.as_str(), "abc"], vec!["INCR", n.as_str()], &n),
        (
            vec!["RPUSH", l.as_str(), "x"],
            vec!["SET", l.as_str(), "v", "GET"],
            &l,
        ),
        (
            vec!["SET", n.as_str(), "abc"],
            vec!["LPUSH", n.as_str(), "x"],
            &n,
        ),
    ] {
        assert!(!c.send(&["DEL", key]).starts_with('-'));
        assert!(!c.send(&setup).starts_with('-'));
        assert_eq!(a.send(&["TXN", "BEGIN"]), OK);
        let err = a.send(&bad);
        assert!(
            err.starts_with('-'),
            "{bad:?} must answer an error: {err:?}"
        );
        let other = c.send(&["SET", key, "fresh"]);
        assert_eq!(
            other, OK,
            "shards={shards}: {bad:?} answered an error and wrote nothing, so it must not \
             leave {key} locked"
        );
        assert_eq!(a.send(&["TXN", "ABORT"]), OK);
        assert_eq!(
            c.send(&["GET", key]),
            bulk("fresh"),
            "shards={shards}: TXN ABORT restored the pre-image {bad:?} captured over \
             another client's write"
        );
    }
    cleanup(guard, dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn an_erroring_write_holds_nothing_1_shard() {
    an_erroring_write_holds_nothing(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn an_erroring_write_holds_nothing_4_shards() {
    an_erroring_write_holds_nothing(4);
}

// ---------------------------------------------------------------------------
// Blocking pops, expiry, eviction, INFO
// ---------------------------------------------------------------------------

fn blocked_clients_do_not_pop_a_held_key(shards: usize) {
    let (guard, port, dir) = spawn(shards, &[]);
    let mut a = Conn::open(port);
    let tag = local_tag(&mut a);
    let l = format!("{tag}l");
    let full = format!("{tag}full");
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["RPUSH", &full, "f"]), ":1\r\n");

    // A client parked on an empty list the TXN then pushes to stays parked
    // while the TXN is open, and is served once it commits.
    let mut blocked = Conn::open(port);
    blocked
        .sock
        .write_all(&common::encode(&["BLPOP", &l, "0"]))
        .expect("write");
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(a.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(a.send(&["RPUSH", &l, "x"]), ":1\r\n");
    assert_eq!(a.send(&["RPUSH", &full, "t"]), ":2\r\n");
    assert!(
        silent_for(&mut blocked, Duration::from_millis(400)),
        "shards={shards}: a blocked client popped a key an open TXN holds: {:?}",
        blocked.read_replies_within(1, Duration::from_secs(1))
    );
    // A blocking pop on a held key that has data: served at once on the
    // key's own shard it is refused like any write; registered from another
    // shard it parks (the owner's waker will not pop a held key) and is
    // served after the commit. Either way it never pops under the TXN.
    let mut imm = Conn::open(port);
    imm.sock
        .write_all(&common::encode(&["BLPOP", &full, "0"]))
        .expect("write");
    let imm_refused = !silent_for(&mut imm, Duration::from_millis(400));
    if imm_refused {
        assert_refused(
            &imm.read_replies_within(1, Duration::from_secs(1)),
            "immediate BLPOP",
        );
    }
    assert_eq!(
        c.send(&["LRANGE", &full, "0", "-1"]),
        format!("*2\r\n{}{}", bulk("f"), bulk("t")),
        "shards={shards}: a blocking pop took an element of a held list"
    );
    assert_eq!(a.send(&["TXN", "COMMIT"]), OK);
    let served = blocked.read_replies_within(1, Duration::from_secs(5));
    assert_eq!(
        served,
        format!("*2\r\n{}{}", bulk(&l), bulk("x")),
        "shards={shards}"
    );
    if !imm_refused {
        assert_eq!(
            imm.read_replies_within(1, Duration::from_secs(5)),
            format!("*2\r\n{}{}", bulk(&full), bulk("f")),
            "shards={shards}: the parked pop is served once the key is released"
        );
    }
    cleanup(guard, dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn blocked_clients_do_not_pop_a_held_key_1_shard() {
    blocked_clients_do_not_pop_a_held_key(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn blocked_clients_do_not_pop_a_held_key_4_shards() {
    blocked_clients_do_not_pop_a_held_key(4);
}

fn dbsize(c: &mut Conn) -> u64 {
    c.send(&["DBSIZE"])
        .trim_start_matches(':')
        .trim()
        .parse()
        .expect("DBSIZE")
}

/// A held key whose TTL passes is not reaped by active expiry while the TXN
/// is open; ABORT restores the pre-image, and a committed expired key is
/// reaped once released.
fn active_expiry_skips_held_keys(shards: usize) {
    let (guard, port, dir) = spawn(shards, &[]);
    let mut a = Conn::open(port);
    let tag = local_tag(&mut a);
    let k = format!("{tag}k");
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["FLUSHALL"]), OK);
    assert_eq!(c.send(&["SET", &k, "orig"]), OK);
    assert_eq!(a.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(a.send(&["SET", &k, "txn", "PX", "100"]), OK);
    std::thread::sleep(Duration::from_millis(700));
    assert_eq!(
        dbsize(&mut c),
        1,
        "shards={shards}: active expiry reaped a key an open TXN holds"
    );
    assert_eq!(a.send(&["TXN", "ABORT"]), OK);
    assert_eq!(c.send(&["GET", &k]), bulk("orig"));
    assert_eq!(c.send(&["PTTL", &k]), ":-1\r\n");

    // COMMIT of an expired value: reaped after the release.
    assert_eq!(a.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(a.send(&["SET", &k, "txn", "PX", "100"]), OK);
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(dbsize(&mut c), 1, "shards={shards}: reaped while held");
    assert_eq!(a.send(&["TXN", "COMMIT"]), OK);
    let deadline = Instant::now() + Duration::from_secs(5);
    while dbsize(&mut c) != 0 {
        assert!(
            Instant::now() < deadline,
            "the released expired key was never reaped"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    cleanup(guard, dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn active_expiry_skips_held_keys_1_shard() {
    active_expiry_skips_held_keys(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn active_expiry_skips_held_keys_4_shards() {
    active_expiry_skips_held_keys(4);
}

/// An `allkeys-lru` eviction flood does not evict a key an open TXN holds.
fn eviction_skips_held_keys(shards: usize) {
    let (guard, port, dir) = spawn(
        shards,
        &[
            "--maxmemory",
            "8mb",
            "--maxmemory-policy",
            "allkeys-lru",
            "--disk-offload",
            "disable",
        ],
    );
    let mut a = Conn::open(port);
    let tag = local_tag(&mut a);
    let held = format!("{tag}held");
    let early = format!("{tag}early");
    let mut c = Conn::open(port);
    assert_eq!(c.send(&["SET", &early, "e"]), OK);
    assert_eq!(a.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(a.send(&["SET", &held, "v"]), OK);
    let value = "x".repeat(1024);
    for round in 0..40 {
        let mut cmds: Vec<Vec<String>> = Vec::new();
        for i in 0..500 {
            cmds.push(vec![
                "SET".into(),
                format!("{tag}flood:{round}:{i}"),
                value.clone(),
            ]);
        }
        let refs: Vec<Vec<&str>> = cmds
            .iter()
            .map(|c| c.iter().map(String::as_str).collect())
            .collect();
        let slices: Vec<&[&str]> = refs.iter().map(Vec::as_slice).collect();
        let _ = c.pipeline(&slices);
    }
    let evicted = info_field(&mut c, "evicted_keys").unwrap_or(0);
    assert!(
        evicted > 0,
        "shards={shards}: the flood evicted nothing — test is inert"
    );
    assert_eq!(
        c.send(&["EXISTS", &held]),
        ":1\r\n",
        "shards={shards}: eviction removed a key an open TXN holds ({evicted} evicted)"
    );
    assert_eq!(a.send(&["TXN", "COMMIT"]), OK);
    assert_eq!(c.send(&["GET", &held]), bulk("v"));
    cleanup(guard, dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn eviction_skips_held_keys_1_shard() {
    eviction_skips_held_keys(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn eviction_skips_held_keys_4_shards() {
    eviction_skips_held_keys(4);
}

fn info_reports_open_transactions(shards: usize) {
    let (guard, port, dir) = spawn(shards, &[]);
    let mut a = Conn::open(port);
    let tag = local_tag(&mut a);
    let mut c = Conn::open(port);
    assert_eq!(info_field(&mut c, "txn_open"), Some(0));
    assert_eq!(a.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(a.send(&["SET", &format!("{tag}k"), "v"]), OK);
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(info_field(&mut c, "txn_open"), Some(1));
    assert!(info_field(&mut c, "txn_oldest_age_ms").is_some_and(|ms| ms >= 250));
    assert_eq!(info_field(&mut c, "txn_held_keys"), Some(1));
    let before = info_field(&mut c, "txn_conflicts_refused").unwrap_or(0);
    assert_refused(&c.send(&["SET", &format!("{tag}k"), "x"]), "SET");
    assert_eq!(
        info_field(&mut c, "txn_conflicts_refused"),
        Some(before + 1)
    );
    assert_eq!(a.send(&["TXN", "COMMIT"]), OK);
    assert_eq!(info_field(&mut c, "txn_open"), Some(0));
    assert_eq!(info_field(&mut c, "txn_oldest_age_ms"), Some(0));
    assert_eq!(info_field(&mut c, "txn_held_keys"), Some(0));
    cleanup(guard, dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn info_reports_open_transactions_1_shard() {
    info_reports_open_transactions(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn info_reports_open_transactions_4_shards() {
    info_reports_open_transactions(4);
}
