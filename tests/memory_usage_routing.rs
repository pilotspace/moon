//! `MEMORY USAGE <key>` must route by the KEY, not by the literal "USAGE".
//! (moon#511)
//!
//! `extract_primary_key` treats `args[0]` as the routing key for anything not
//! in its keyless list. `MEMORY` is not in that list and had no subcommand arm
//! of its own, so every `MEMORY USAGE` hashed the string `"USAGE"` — one fixed
//! shard, whatever the key — and then read that shard's slice for a key it
//! does not own. The key reports as absent unless it happens to live on the
//! same shard the word "USAGE" hashes to.
//!
//! Measured against the unfixed build at `--shards 4`: 22 of 24 existing keys
//! reported absent. The rate is `1 - 1/shards`, the signature of "always lands
//! on one fixed shard regardless of the key" — not of a race.
//!
//! The same class of bug has been fixed for `OBJECT`, `XGROUP`, `XINFO`,
//! `BITOP`, `ZDIFF`/`ZINTER`/`ZUNION`/`ZINTERCARD` and `XREAD`; each needed an
//! explicit arm because the subcommand or a count literal sits at `args[0]`.
//!
//! Every assertion loops over key placements: a single trial samples one
//! placement and passes by luck ~25% of the time at four shards.

mod common;

use common::Conn;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Four shards: a key is remote ~75% of the time, so a build that only ever
/// reads one fixed shard cannot pass by luck.
const SHARDS: &str = "4";
/// Distinct keys per assertion. At p(remote)=0.75 the chance all 12 land on
/// the one shard "USAGE" hashes to — and vacuously pass — is under 1e-7.
const TRIALS: usize = 12;

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
        let tmp_dir = std::env::temp_dir().join(format!("moon-memroute-{port}"));
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
                tmp_dir.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(
                std::fs::File::create(tmp_dir.join("moon.stderr")).expect("create moon stderr log"),
            )
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("moon-memroute-{port}"));
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

/// Run `body` once per key placement on a FRESH connection.
///
/// Fresh connections on purpose: the connection's own shard is half of what
/// decides whether a key is local, so reusing one connection would sample a
/// single placement TRIALS times instead of the distribution.
fn each_trial(
    port: u16,
    tag: &str,
    why_ctx: &str,
    mut body: impl FnMut(&mut Conn, &str) -> Option<String>,
) {
    let mut wrong: Vec<String> = Vec::new();
    for i in 0..TRIALS {
        let mut c = Conn::open(port);
        let key = format!("{{{tag}{i}}}k");
        if let Some(why) = body(&mut c, &key) {
            wrong.push(format!("  key {key}: {why}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "{}/{} key placements wrong ({why_ctx}):\n{}",
        wrong.len(),
        TRIALS,
        wrong.join("\n")
    );
}

/// `:<n>` with n > 0 — the shape of a real MEMORY USAGE answer.
fn is_positive_integer(reply: &str) -> bool {
    reply
        .strip_prefix(':')
        .and_then(|r| r.trim_end().parse::<i64>().ok())
        .is_some_and(|n| n > 0)
}

#[test]
fn mur1_memory_usage_reports_an_existing_key() {
    let m = spawn_moon(SHARDS);
    each_trial(
        m.port,
        "mu1",
        "moon#511 — MEMORY USAGE must route by the KEY, not by the literal \
         \"USAGE\"; a null here means it read a shard that does not own the key",
        |c, key| {
            let set = c.send(&["SET", key, "hello-value"]);
            if !set.starts_with("+OK") {
                return Some(format!("SET did not ack: {set:?}"));
            }
            // Prove the key is readable through the normal path first, so a
            // failure below is about MEMORY's routing and not about the SET.
            let get = c.send(&["GET", key]);
            if !get.contains("hello-value") {
                return Some(format!("GET could not read back the key: {get:?}"));
            }
            let usage = c.send(&["MEMORY", "USAGE", key]);
            if is_positive_integer(&usage) {
                None
            } else {
                Some(format!("MEMORY USAGE replied {usage:?}, want :<bytes>"))
            }
        },
    );
}

#[test]
fn mur2_memory_usage_of_a_missing_key_is_null() {
    // The negative case must stay negative: a fix that routes correctly but
    // reports a size for keys that do not exist would pass mur1 and be worse.
    let m = spawn_moon(SHARDS);
    each_trial(
        m.port,
        "mu2",
        "moon#511 — a key that was never set must report null, not a size",
        |c, key| {
            let usage = c.send(&["MEMORY", "USAGE", key]);
            if usage.starts_with("$-1") || usage.starts_with("_") {
                None
            } else {
                Some(format!("MEMORY USAGE of an absent key replied {usage:?}"))
            }
        },
    );
}

#[test]
fn mur3_memory_usage_honours_samples_after_the_key() {
    // `MEMORY USAGE key SAMPLES n` — the key is still args[1]; trailing
    // options must not shift routing.
    let m = spawn_moon(SHARDS);
    each_trial(
        m.port,
        "mu3",
        "moon#511 — SAMPLES after the key must not change which shard is asked",
        |c, key| {
            let set = c.send(&["SET", key, "hello-value"]);
            if !set.starts_with("+OK") {
                return Some(format!("SET did not ack: {set:?}"));
            }
            let usage = c.send(&["MEMORY", "USAGE", key, "SAMPLES", "0"]);
            if is_positive_integer(&usage) {
                None
            } else {
                Some(format!("MEMORY USAGE ... SAMPLES 0 replied {usage:?}"))
            }
        },
    );
}

#[test]
fn mur4_keyless_memory_subcommands_still_answer() {
    // MEMORY DOCTOR/STATS take no key. Whatever the routing fix does, it must
    // not turn them into "no such key" or an error.
    let m = spawn_moon(SHARDS);
    let mut c = Conn::open(m.port);

    let doctor = c.send(&["MEMORY", "DOCTOR"]);
    assert!(
        !doctor.starts_with('-'),
        "MEMORY DOCTOR must still answer, got {doctor:?}"
    );

    let stats = c.send(&["MEMORY", "STATS"]);
    assert!(
        !stats.starts_with('-'),
        "MEMORY STATS must still answer, got {stats:?}"
    );
}
