//! moon#1299 R1 review fixes (area 1): the smaller findings beside F1.
//!
//! - F2: a cross-shard `DEL` / `UNLINK` whose LOCAL slice is refused
//!   (`-TXNCONFLICT`) answered a success count of the remote legs, hiding
//!   keys that were never deleted.
//! - F3: `INFO txn_held_keys` was published only when a database's held
//!   count went 0↔1 (it read 1 for five held keys).
//! - RESET left an open `TXN` (and its key holds) in place; redis's RESET
//!   discards the connection's MULTI state, and now so does moon's for TXN.
//! - A cross-shard `MSET` / `DEL` / `UNLINK` refused on the held key's leg
//!   while other legs were applied answered the bare `-TXNCONFLICT`; it now
//!   says the command was partially executed (as the AOF-backpressure
//!   refusal already did).
//!
//! Each test runs on whichever runtime `MOON_BIN` was built with; run the
//! suite once per runtime:
//!
//!   MOON_DISK_FREE_MIN_PCT=0 MOON_BIN=... cargo test --test review_r1_txn_isolation_1299 -- --include-ignored

mod common;

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::Conn;

const OK: &str = "+OK\r\n";
const CONFLICT: &str = "-TXNCONFLICT";

fn spawn(shards: usize, extra: &[&str]) -> (common::ServerGuard, u16, std::path::PathBuf) {
    let bin = common::find_moon_binary();
    let dir = common::unique_test_dir(&format!("txn-r1-{shards}"));
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

fn cleanup(mut guard: common::ServerGuard, dir: std::path::PathBuf) {
    guard.kill_now();
    let _ = std::fs::remove_dir_all(dir);
}

/// Is `key` on `c`'s own shard? A TXN refuses a write that routes to another
/// shard (#499), so a TXN probe answers exactly that. Leaves no trace.
fn is_local(c: &mut Conn, key: &str) -> bool {
    assert_eq!(c.send(&["TXN", "BEGIN"]), OK);
    let wrote = c.send(&["SET", key, "probe"]);
    assert_eq!(c.send(&["TXN", "ABORT"]), OK);
    wrote == OK
}

/// The first `{<prefix><i>}<suffix>` key for which `want(key)` holds.
fn find_key(prefix: &str, suffix: &str, mut want: impl FnMut(&str) -> bool) -> String {
    (0..256)
        .map(|i| format!("{{{prefix}{i}}}{suffix}"))
        .find(|k| want(k))
        .expect("no such key within 256 hash tags")
}

/// A fresh connection for which `want(conn)` holds (the server spreads
/// connections over its shards).
fn find_conn(port: u16, mut want: impl FnMut(&mut Conn) -> bool) -> Conn {
    for _ in 0..128 {
        let mut c = Conn::open(port);
        if want(&mut c) {
            return c;
        }
    }
    panic!("no connection with the wanted shard within 128 tries");
}

fn exists(c: &mut Conn, key: &str) -> String {
    c.send(&["EXISTS", key])
}

// ---------------------------------------------------------------------------
// F2: a refused local slice of a cross-shard DEL / UNLINK
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn cross_shard_del_with_a_refused_local_slice_answers_the_conflict() {
    let (guard, port, dir) = spawn(4, &[]);
    let mut a = Conn::open(port);
    let held = find_key("h", "held", |k| is_local(&mut a, k));
    // Same hash tag: same shard as the held key.
    let tag_end = held.find('}').expect("tag") + 1;
    let local_free = format!("{}free", &held[..tag_end]);
    // B runs on the held key's shard; C on another one.
    let mut b = find_conn(port, |c| is_local(c, &held));
    let mut c = find_conn(port, |c| !is_local(c, &held));
    let remote = find_key("r", "x", |k| !is_local(&mut b, k));
    let mut w = Conn::open(port);
    for k in [&held, &local_free, &remote] {
        assert_eq!(w.send(&["SET", k, "orig"]), OK);
    }
    assert_eq!(a.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(a.send(&["SET", &held, "txn"]), OK);

    for cmd in ["DEL", "UNLINK"] {
        for k in [&local_free, &remote] {
            assert_eq!(w.send(&["SET", k, "orig"]), OK);
        }
        // B: the held key's slice is B's LOCAL leg.
        let reply = b.send(&[cmd, &remote, &held, &local_free]);
        assert!(
            reply.starts_with(CONFLICT),
            "{cmd} remote held local_free from the held key's shard must answer \
             -TXNCONFLICT (its local slice was refused), got {reply:?}"
        );
        assert_eq!(
            exists(&mut w, &held),
            ":1\r\n",
            "{cmd}: the held key is not deleted"
        );
        assert_eq!(
            exists(&mut w, &local_free),
            ":1\r\n",
            "{cmd}: the refused slice's other key is not deleted either"
        );

        // C: the same slice is a REMOTE leg — the answer must agree.
        assert_eq!(w.send(&["SET", &local_free, "orig"]), OK);
        let reply = c.send(&[cmd, &held, &local_free]);
        assert!(
            reply.starts_with(CONFLICT),
            "{cmd} held local_free from another shard must answer -TXNCONFLICT, got {reply:?}"
        );
    }

    assert_eq!(a.send(&["TXN", "ABORT"]), OK);
    // Released: the same DEL now deletes everything it names.
    assert_eq!(w.send(&["SET", &remote, "orig"]), OK);
    assert_eq!(b.send(&["DEL", &remote, &held, &local_free]), ":3\r\n");
    cleanup(guard, dir);
}

// ---------------------------------------------------------------------------
// F3: INFO txn_held_keys counts every held key
// ---------------------------------------------------------------------------

fn info_field(c: &mut Conn, field: &str) -> Option<u64> {
    let info = c.send(&["INFO", "stats"]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .and_then(|v| v.trim().parse().ok())
}

#[test]
#[ignore]
fn info_txn_held_keys_counts_every_held_key() {
    let (guard, port, dir) = spawn(1, &[]);
    let mut a = Conn::open(port);
    let mut w = Conn::open(port);
    for end in ["COMMIT", "ABORT"] {
        assert_eq!(a.send(&["TXN", "BEGIN"]), OK);
        for i in 0..5 {
            assert_eq!(a.send(&["SET", &format!("k{i}"), "v"]), OK);
        }
        assert_eq!(info_field(&mut w, "txn_held_keys"), Some(5), "5 SETs");
        for i in 5..7 {
            assert_eq!(a.send(&["SET", &format!("k{i}"), "v"]), OK);
        }
        assert_eq!(info_field(&mut w, "txn_held_keys"), Some(7), "7 SETs");
        // A rewrite of a held key holds nothing new.
        assert_eq!(a.send(&["SET", "k0", "v2"]), OK);
        assert_eq!(info_field(&mut w, "txn_held_keys"), Some(7), "rewrite");
        // Another database.
        assert_eq!(a.send(&["SELECT", "2"]), OK);
        assert_eq!(a.send(&["SET", "z", "v"]), OK);
        assert_eq!(a.send(&["SELECT", "0"]), OK);
        assert_eq!(info_field(&mut w, "txn_held_keys"), Some(8), "+1 in db 2");
        // moon#1303: an erroring write takes its new hold back.
        assert!(a.send(&["INCR", "k0"]).starts_with("-ERR"));
        assert!(a.send(&["SET", "n", "v", "BADOPT"]).starts_with("-ERR"));
        assert_eq!(
            info_field(&mut w, "txn_held_keys"),
            Some(8),
            "erroring writes"
        );
        assert_eq!(a.send(&["TXN", end]), OK);
        assert_eq!(
            info_field(&mut w, "txn_held_keys"),
            Some(0),
            "after TXN {end}"
        );
        assert_eq!(info_field(&mut w, "txn_open"), Some(0), "after TXN {end}");
    }
    cleanup(guard, dir);
}

// ---------------------------------------------------------------------------
// RESET ends an open TXN (redis RESET discards MULTI state)
// ---------------------------------------------------------------------------

fn bulk(s: &str) -> String {
    format!("${}\r\n{s}\r\n", s.len())
}

#[test]
#[ignore]
fn reset_ends_the_open_txn_and_releases_its_keys() {
    let (guard, port, dir) = spawn(1, &[]);
    let mut a = Conn::open(port);
    let mut w = Conn::open(port);
    assert_eq!(w.send(&["SET", "k", "orig"]), OK);

    // RESET pipelined between the TXN's write and a read: the rollback is
    // applied before the next command runs.
    let replies = a.pipeline(&[
        &["TXN", "BEGIN"],
        &["SET", "k", "txnval"],
        &["RESET"],
        &["GET", "k"],
    ]);
    assert_eq!(
        replies,
        format!("{OK}{OK}+RESET\r\n{}", bulk("orig")),
        "RESET must roll the open TXN back before the next command"
    );
    assert_eq!(w.send(&["GET", "k"]), bulk("orig"));
    assert_eq!(info_field(&mut w, "txn_open"), Some(0));
    assert_eq!(info_field(&mut w, "txn_held_keys"), Some(0));
    assert_eq!(w.send(&["SET", "k", "other"]), OK, "released by RESET");
    assert_eq!(w.send(&["FLUSHALL"]), OK);
    // The transaction is gone: nothing left to commit.
    let commit = a.send(&["TXN", "COMMIT"]);
    assert!(
        commit.starts_with("-"),
        "TXN.COMMIT after RESET: {commit:?}"
    );
    assert_eq!(w.send(&["GET", "k"]), "$-1\r\n");

    // From subscriber mode too (RESET is the sanctioned way out of it).
    assert_eq!(w.send(&["SET", "k", "orig"]), OK);
    assert_eq!(a.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(a.send(&["SET", "k", "txnval"]), OK);
    assert!(a.send(&["SUBSCRIBE", "ch"]).contains("subscribe"));
    assert_eq!(a.send(&["RESET"]), "+RESET\r\n");
    assert_eq!(w.send(&["GET", "k"]), bulk("orig"));
    assert_eq!(info_field(&mut w, "txn_open"), Some(0));
    assert_eq!(w.send(&["SET", "k", "other"]), OK);

    // A RESET refused for its arity changes nothing, the TXN included.
    assert_eq!(a.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(a.send(&["SET", "k", "txn2"]), OK);
    assert!(a.send(&["RESET", "extra"]).starts_with("-ERR wrong number"));
    assert_eq!(info_field(&mut w, "txn_open"), Some(1));
    assert_eq!(a.send(&["TXN", "COMMIT"]), OK);
    assert_eq!(w.send(&["GET", "k"]), bulk("txn2"));
    cleanup(guard, dir);
}

// ---------------------------------------------------------------------------
// A cross-shard write refused on one leg says it was partially executed
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn partially_applied_cross_shard_writes_say_so() {
    let (guard, port, dir) = spawn(4, &[]);
    let mut a = Conn::open(port);
    let held = find_key("h", "held", |k| is_local(&mut a, k));
    let tag_end = held.find('}').expect("tag") + 1;
    let local_free = format!("{}free", &held[..tag_end]);
    let mut b = find_conn(port, |c| is_local(c, &held));
    let remote = find_key("r", "x", |k| !is_local(&mut b, k));
    let mut w = Conn::open(port);
    for k in [&held, &local_free, &remote] {
        assert_eq!(w.send(&["SET", k, "orig"]), OK);
    }
    assert_eq!(a.send(&["TXN", "BEGIN"]), OK);
    assert_eq!(a.send(&["SET", &held, "txn"]), OK);

    // MSET: the remote leg is applied, the held key's leg is refused.
    let reply = b.send(&["MSET", &remote, "B", &held, "B", &local_free, "B"]);
    assert!(
        reply.starts_with(CONFLICT) && reply.contains("partially executed"),
        "a cross-shard MSET refused on one leg while another was applied must say \
         it was partially executed, got {reply:?}"
    );
    assert_eq!(
        w.send(&["GET", &remote]),
        "$1\r\nB\r\n",
        "remote leg applied"
    );
    assert_eq!(
        w.send(&["GET", &local_free]),
        "$4\r\norig\r\n",
        "refused leg unapplied"
    );

    // DEL (F2): the same shape.
    let reply = b.send(&["DEL", &remote, &held, &local_free]);
    assert!(
        reply.starts_with(CONFLICT) && reply.contains("partially executed"),
        "DEL: {reply:?}"
    );
    assert_eq!(exists(&mut w, &remote), ":0\r\n", "remote leg applied");

    // Nothing else ran: the plain refusal, no "partially".
    let reply = b.send(&["MSET", &held, "B", &local_free, "B"]);
    assert!(
        reply.starts_with(CONFLICT) && !reply.contains("partially"),
        "a refusal with no other part applied is the plain one, got {reply:?}"
    );
    assert_eq!(a.send(&["TXN", "ABORT"]), OK);
    cleanup(guard, dir);
}
