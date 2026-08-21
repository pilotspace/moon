//! moon#629 red/green: `RANDOMKEY` must sample the WHOLE keyspace on a
//! multi-shard server, not just the shard the connection happens to sit on.
//!
//! Before the fix `RANDOMKEY` was absent from the cross-shard coordinator, so
//! it fell through to a local-shard lookup. Two user-visible defects followed
//! on `--shards 4`, both reproduced by the tests here:
//!
//!   * `RANDOMKEY` answered **Null while `DBSIZE` reported keys** — whenever
//!     the serving shard owned none of them. With one key in the db that is
//!     deterministic: 3 connections in 4 never see it.
//!   * When it did answer, it could only ever name a key from that one shard,
//!     so repeated draws returned the same handful of names forever.
//!
//! Every draw runs on ONE connection deliberately. A fresh `redis-cli` per
//! draw opens a fresh connection, which SO_REUSEPORT spreads across shards —
//! that spread MASKS the bug, which is why it went unnoticed. The `Conn`
//! helper keeps a single socket for the whole run, exactly like a real client.
//!
//! Run with:
//!   cargo test --release --test randomkey_cross_shard

mod common;

use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use common::Conn;

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

fn spawn_moon(shards: &str) -> Option<Moon> {
    let bin = common::find_moon_binary();
    if !bin.exists() {
        eprintln!(
            "skipping: {} not built. Run `cargo build --release` first.",
            bin.display()
        );
        return None;
    }
    let tag = format!("moon-randomkey-{shards}");
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("{tag}-{port}"));
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
                "--dir",
                tmp_dir.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("{tag}-{port}"));
    let moon = Moon {
        child,
        port,
        tmp_dir,
    };

    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        let ok = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Conn::open(moon.port).send(&["PING"]).contains("PONG")
        }))
        .unwrap_or(false);
        if ok {
            return Some(moon);
        }
        thread::sleep(Duration::from_millis(100));
    }
    eprintln!("skipping: moon did not answer PING within 15s on port {port}");
    None
}

/// The bulk-string payload of a RESP reply, or `None` for a null reply.
///
/// Deliberately distinguishes the two: "Null while the db is non-empty" IS the
/// defect, so a helper that folded null into an empty string would erase it.
fn bulk(reply: &str) -> Option<String> {
    if reply.starts_with("$-1") || reply.starts_with("_\r\n") {
        return None;
    }
    let mut lines = reply.split("\r\n");
    let header = lines.next()?;
    if !header.starts_with('$') {
        panic!("expected a bulk string or null, got {reply:?}");
    }
    Some(lines.next()?.to_string())
}

fn seed(conn: &mut Conn, prefix: &str, n: usize) {
    for i in 0..n {
        let key = format!("{prefix}:{i}");
        let r = conn.send(&["SET", &key, "v"]);
        assert!(r.contains("OK"), "seed SET {key} -> {r:?}");
    }
}

fn dbsize(conn: &mut Conn) -> i64 {
    let r = conn.send(&["DBSIZE"]);
    r.trim_start_matches(':')
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("DBSIZE -> {r:?}"))
}

/// rk1: the deterministic form. One key in the db, one connection, 20 draws.
/// Redis answers the key 20/20. Moon answered Null 20/20 whenever the
/// connection's shard was not the key's owner.
#[test]
fn rk1_never_null_while_the_db_is_not_empty() {
    let Some(m) = spawn_moon("4") else { return };
    let mut c = Conn::open(m.port);

    assert!(c.send(&["FLUSHALL"]).contains("OK"));
    assert!(c.send(&["SET", "solo", "v"]).contains("OK"));
    assert_eq!(dbsize(&mut c), 1, "precondition: exactly one key in the db");

    for draw in 0..20 {
        let got = bulk(&c.send(&["RANDOMKEY"]));
        assert_eq!(
            got.as_deref(),
            Some("solo"),
            "draw {draw}: RANDOMKEY must name the only key in the db, not Null"
        );
    }
}

/// rk2: the distribution. 64 keys spread over 4 shards; 300 draws on ONE
/// connection must reach far more of the keyspace than any single shard owns
/// (~16 keys). Coupon-collector puts the fixed server near 63 distinct.
#[test]
fn rk2_samples_the_whole_keyspace_not_one_shard() {
    let Some(m) = spawn_moon("4") else { return };
    let mut c = Conn::open(m.port);

    assert!(c.send(&["FLUSHALL"]).contains("OK"));
    seed(&mut c, "rk", 64);
    assert_eq!(dbsize(&mut c), 64);

    let mut hits: HashMap<String, usize> = HashMap::new();
    for draw in 0..300 {
        let got = bulk(&c.send(&["RANDOMKEY"]));
        let key = got.unwrap_or_else(|| panic!("draw {draw}: Null with 64 keys present"));
        assert!(
            key.starts_with("rk:"),
            "draw {draw}: RANDOMKEY named {key:?}, which was never seeded"
        );
        *hits.entry(key).or_default() += 1;
    }

    // A single shard owns roughly 16 of the 64. The pre-fix server could not
    // exceed its own shard's share however many times it was asked; 40 is far
    // above any plausible hash imbalance and far below the ~63 a correct
    // server reaches.
    assert!(
        hits.len() >= 40,
        "RANDOMKEY reached only {} distinct keys in 300 draws — it is sampling \
         one shard, not the keyspace (hits: {:?})",
        hits.len(),
        {
            let mut v: Vec<_> = hits.iter().map(|(k, n)| (k.clone(), *n)).collect();
            v.sort();
            v.truncate(8);
            v
        }
    );
}

/// rk3: the other half of the contract — Null is CORRECT on an empty db, and
/// the fix must not turn it into a fabricated key or an error.
#[test]
fn rk3_empty_db_still_answers_null() {
    let Some(m) = spawn_moon("4") else { return };
    let mut c = Conn::open(m.port);

    assert!(c.send(&["FLUSHALL"]).contains("OK"));
    assert_eq!(dbsize(&mut c), 0);
    for draw in 0..5 {
        let r = c.send(&["RANDOMKEY"]);
        assert_eq!(bulk(&r), None, "draw {draw}: empty db must answer Null, got {r:?}");
    }
}

/// rk4: a key in another db must not leak into this one's draw. The
/// coordinator fans out per selected db; a fan-out that forgot `db_index`
/// would surface here and nowhere else.
#[test]
fn rk4_draws_only_from_the_selected_db() {
    let Some(m) = spawn_moon("4") else { return };
    let mut c = Conn::open(m.port);

    assert!(c.send(&["FLUSHALL"]).contains("OK"));
    assert!(c.send(&["SELECT", "1"]).contains("OK"));
    seed(&mut c, "one", 16);
    assert!(c.send(&["SELECT", "0"]).contains("OK"));
    assert_eq!(dbsize(&mut c), 0, "db 0 must be empty");

    for draw in 0..10 {
        let r = c.send(&["RANDOMKEY"]);
        assert_eq!(bulk(&r), None, "draw {draw}: db 0 is empty, got {r:?}");
    }

    assert!(c.send(&["SELECT", "1"]).contains("OK"));
    for draw in 0..20 {
        let got = bulk(&c.send(&["RANDOMKEY"]));
        let key = got.unwrap_or_else(|| panic!("draw {draw}: Null with 16 keys in db 1"));
        assert!(key.starts_with("one:"), "db 1 draw named {key:?}");
    }
}

/// rk5: single-shard servers were always correct. Pin it, so the coordinator
/// path added for #629 cannot regress the case that already worked.
#[test]
fn rk5_single_shard_is_unchanged() {
    let Some(m) = spawn_moon("1") else { return };
    let mut c = Conn::open(m.port);

    assert!(c.send(&["FLUSHALL"]).contains("OK"));
    assert_eq!(bulk(&c.send(&["RANDOMKEY"])), None);
    assert!(c.send(&["SET", "solo", "v"]).contains("OK"));
    for _ in 0..10 {
        assert_eq!(bulk(&c.send(&["RANDOMKEY"])).as_deref(), Some("solo"));
    }
}
