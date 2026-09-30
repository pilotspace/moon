//! moon#1299 R1 review fixes (area 1): the smaller findings beside F1.
//!
//! - F2: a cross-shard `DEL` / `UNLINK` whose LOCAL slice is refused
//!   (`-TXNCONFLICT`) answered a success count of the remote legs, hiding
//!   keys that were never deleted.
//! - F3: `INFO txn_held_keys` was published only when a database's held
//!   count went 0↔1 (it read 1 for five held keys).
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
