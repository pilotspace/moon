//! Commands whose first argument is not their key must still route by the key
//! — moon#533, moon#534.
//!
//! `extract_primary_key` (`src/server/conn/shared.rs`) special-cases the
//! commands whose key is not `args[0]` and falls through to `args[0]` for
//! everything else. `LMPOP`, `ZMPOP` and `SINTERCARD` take `numkeys` first and
//! `XREADGROUP` takes the literal token `GROUP`, so each hashed a constant and
//! pinned every invocation to one shard. A key that shard did not own read as
//! absent.
//!
//! Two rules govern every assertion here, both learned by having the earlier
//! instrument miss this class entirely:
//!
//!   1. **Probe populated keys.** On an absent key a mis-routed command and a
//!      correctly routed one return the same bytes, so an absent-key probe
//!      cannot see the defect. The `--shards 4` oracle in moon#482 probed
//!      `LMPOP`/`ZMPOP` on absent keys and reported them clean while they were
//!      broken.
//!   2. **Probe many keys.** A constant route is still correct for ~1/N of
//!      keys, so a single-key test at `--shards 4` passes a quarter of the
//!      time — it reads as a flake rather than as a bug.
//!
//! `--shards 4` is the point of the file; a single-shard run has no routing and
//! cannot fail these.

mod common;

use common::Conn;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Enough keys that a constant route cannot pass by luck. With 4 shards a
/// wrong route serves ~1/4 of keys, so 12 keys make a false green about
/// 1-in-16-million rather than 1-in-4.
const KEYS: usize = 12;

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
        let tmp_dir = std::env::temp_dir().join(format!("moon-shardroute-{port}"));
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
    let tmp_dir = std::env::temp_dir().join(format!("moon-shardroute-{port}"));
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

/// Run `probe` against `KEYS` distinct keys, each prepared by `setup`, and
/// report EVERY key whose reply does not satisfy `is_hit`.
///
/// Reporting all failures rather than the first matters here: the count is the
/// diagnosis. "11 of 12 wrong" is a constant route; "1 of 12 wrong" is
/// something else entirely, and a `assert!` on the first failure would make
/// those two look identical.
#[track_caller]
fn assert_every_key_routes(
    c: &mut Conn,
    label: &str,
    prefix: &str,
    setup: impl Fn(&mut Conn, &str),
    probe: impl Fn(&mut Conn, &str) -> String,
    is_hit: impl Fn(&str) -> bool,
) {
    let mut wrong = Vec::new();
    for i in 0..KEYS {
        let key = format!("{prefix}{i}");
        setup(c, &key);
        // The setup must actually have landed, or a "miss" below would prove
        // nothing about routing. EXISTS routes by args[0] and is known good.
        let exists = c.send(&["EXISTS", &key]);
        assert_eq!(
            exists, ":1\r\n",
            "{label}: setup did not create {key} (EXISTS said {exists:?}) — \
             the probe below would be vacuous"
        );
        let got = probe(c, &key);
        if !is_hit(&got) {
            wrong.push(format!("  {key} -> {got:?}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "moon#533/#534 — {label} answered a MISS for {} of {} keys that exist. \
         A command routed by hashing a literal argument lands on one fixed \
         shard and reports every other shard's keys as absent:\n{}",
        wrong.len(),
        KEYS,
        wrong.join("\n")
    );
}

#[test]
fn srp1_lmpop_routes_by_its_key_not_by_numkeys() {
    let m = spawn_moon("4");
    let mut c = Conn::open(m.port);
    assert_every_key_routes(
        &mut c,
        "LMPOP",
        "srp:lmpop:",
        |c, k| {
            c.send(&["RPUSH", k, "v1"]);
        },
        |c, k| c.send(&["LMPOP", "1", k, "LEFT"]),
        // A hit names the key it popped from; the miss is the null array.
        |r| r.starts_with('*') && r.contains("v1"),
    );
}

#[test]
fn srp2_zmpop_routes_by_its_key_not_by_numkeys() {
    let m = spawn_moon("4");
    let mut c = Conn::open(m.port);
    assert_every_key_routes(
        &mut c,
        "ZMPOP",
        "srp:zmpop:",
        |c, k| {
            c.send(&["ZADD", k, "1", "m"]);
        },
        |c, k| c.send(&["ZMPOP", "1", k, "MIN"]),
        |r| r.starts_with('*') && r.contains('m'),
    );
}

#[test]
fn srp3_sintercard_routes_by_its_key_not_by_numkeys() {
    let m = spawn_moon("4");
    let mut c = Conn::open(m.port);
    assert_every_key_routes(
        &mut c,
        "SINTERCARD",
        "srp:sinter:",
        |c, k| {
            c.send(&["SADD", k, "a", "b"]);
        },
        |c, k| c.send(&["SINTERCARD", "1", k]),
        // The mis-routed answer is `:0` — indistinguishable from an empty set,
        // which is exactly why this went unnoticed.
        |r| r == ":2\r\n",
    );
}

#[test]
fn srp4_xreadgroup_routes_by_its_key_not_by_the_group_token() {
    let m = spawn_moon("4");
    let mut c = Conn::open(m.port);
    assert_every_key_routes(
        &mut c,
        "XREADGROUP",
        "srp:xrg:",
        |c, k| {
            c.send(&["XADD", k, "1-1", "f", "v"]);
            c.send(&["XGROUP", "CREATE", k, "g", "0"]);
        },
        |c, k| {
            c.send(&[
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "COUNT",
                "1",
                "STREAMS",
                k,
                ">",
            ])
        },
        // Mis-routed, XREADGROUP does not answer a null — it errors with
        // "requires the key to exist", which is at least loud.
        |r| r.starts_with('*') && r.contains("1-1"),
    );
}

#[test]
fn srp5_the_already_correct_commands_do_not_move() {
    // The fence. An audit of this class can fail in both directions: adding a
    // routing arm to a command whose key really IS args[0] would break it.
    // These four already route correctly and must still do so.
    let m = spawn_moon("4");
    let mut c = Conn::open(m.port);

    assert_every_key_routes(
        &mut c,
        "ZDIFF (has an arm already)",
        "srp:zdiff:",
        |c, k| {
            c.send(&["ZADD", k, "1", "m"]);
        },
        |c, k| c.send(&["ZDIFF", "1", k]),
        |r| r.starts_with('*') && r.contains('m'),
    );
    assert_every_key_routes(
        &mut c,
        "XREAD (has an arm already)",
        "srp:xread:",
        |c, k| {
            c.send(&["XADD", k, "1-1", "f", "v"]);
        },
        |c, k| c.send(&["XREAD", "COUNT", "1", "STREAMS", k, "0-0"]),
        |r| r.starts_with('*') && r.contains("1-1"),
    );
    assert_every_key_routes(
        &mut c,
        "MEMORY USAGE (moon#511's arm)",
        "srp:memusage:",
        |c, k| {
            c.send(&["SET", k, "v"]);
        },
        |c, k| c.send(&["MEMORY", "USAGE", k]),
        |r| r.starts_with(':'),
    );
    assert_every_key_routes(
        &mut c,
        "LPOP (key really is args[0])",
        "srp:lpop:",
        |c, k| {
            c.send(&["RPUSH", k, "v1"]);
        },
        |c, k| c.send(&["LPOP", k]),
        |r| r.contains("v1"),
    );
}

#[test]
fn srp6_multi_key_forms_still_reach_every_named_key() {
    // `numkeys > 1` is the form the fix must not get wrong: routing to the
    // FIRST key is correct only because the coordinator rejects or co-locates
    // the rest. A form naming two keys on different shards must give a
    // deterministic answer, never a silent partial one.
    let m = spawn_moon("4");
    let mut c = Conn::open(m.port);

    // Hash tags co-locate, so this pair is guaranteed same-shard and must work.
    for i in 0..KEYS {
        let a = format!("srp:mk:{{t{i}}}:a");
        let b = format!("srp:mk:{{t{i}}}:b");
        c.send(&["RPUSH", &b, "v1"]);
        let got = c.send(&["LMPOP", "2", &a, &b, "LEFT"]);
        assert!(
            got.starts_with('*') && got.contains("v1"),
            "LMPOP over two co-located keys must find the populated one, got {got:?}"
        );
    }
}
