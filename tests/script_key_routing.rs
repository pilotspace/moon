//! A script whose keys all live on ONE shard must run there. (moon#508)
//!
//! Filed as "EVALSHA of a single-key script fails with CROSSSLOT", with the
//! guess that a 1-element key list was being mis-folded. It is not: the check
//! (`scripting::validate_keys_same_shard`) requires every key to hash to the
//! shard the CONNECTION happens to be on. One key cannot cross slots, but it
//! very easily lives on another shard — so `CROSSSLOT` was standing in for
//! "I cannot run this here", and the script never got routed anywhere else.
//!
//! Measured against the unfixed build at `--shards 4`, 8 distinct keys on
//! fresh connections: 7 of 8 rejected. Only the key that happened to land on
//! the connection's own shard ran. `numkeys=0` always worked, which is why the
//! defect reads as intermittent rather than total.
//!
//! This breaks `redis.lock.Lock.release()` — implemented as a single-key
//! EVALSHA — and through redis-py's `Script.__call__` wrapper the caller sees
//! a `NoScriptError` followed by a cross-slot error, neither of which names
//! the real cause.
//!
//! Every assertion here is written so the FIXED state is the passing state,
//! and each loops over key placements: a single trial only samples one
//! placement, and at `--shards 4` a lucky key passes ~25% of the time.

mod common;

use common::Conn;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Four shards: a key is remote ~75% of the time, so a build that only ever
/// runs scripts locally cannot pass by luck.
const SHARDS: &str = "4";
/// Distinct keys per assertion. At p(remote)=0.75 the chance that all 12 land
/// on the connection's own shard — and vacuously pass — is under 1e-7.
const TRIALS: usize = 12;

/// `return redis.call('get', KEYS[1])` — the shape of every single-key script
/// a lock or cache wrapper issues.
const GET_SCRIPT: &str = "return redis.call('get',KEYS[1])";

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
        let tmp_dir = std::env::temp_dir().join(format!("moon-scriptroute-{port}"));
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
    let tmp_dir = std::env::temp_dir().join(format!("moon-scriptroute-{port}"));
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

/// Run `body` once per key placement on a FRESH connection, collecting the
/// trials that came out wrong.
///
/// Fresh connections on purpose: which shard a connection lands on is what
/// decides whether its key is "local", so reusing one connection would sample
/// a single placement TRIALS times instead of the distribution.
fn each_trial(port: u16, tag: &str, mut body: impl FnMut(&mut Conn, &str) -> Option<String>) {
    let mut wrong: Vec<String> = Vec::new();
    for i in 0..TRIALS {
        let mut c = Conn::open(port);
        let key = format!("{tag}{i}");
        if let Some(why) = body(&mut c, &key) {
            wrong.push(format!("  key {key}: {why}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "{}/{} key placements wrong (moon#508 — a script whose keys all live \
         on one shard must be ROUTED there, not rejected):\n{}",
        wrong.len(),
        TRIALS,
        wrong.join("\n")
    );
}

/// A single-key EVAL must run wherever its key lives.
#[test]
fn skr1_single_key_eval_runs_on_the_keys_shard() {
    let m = spawn_moon(SHARDS);
    each_trial(m.port, "skr1", |c, k| {
        c.send(&["SET", k, "v"]);
        let r = c.send(&["EVAL", GET_SCRIPT, "1", k]);
        (r != "$1\r\nv\r\n").then(|| format!("EVAL replied {r:?}, expected the value"))
    });
}

/// The reported symptom: EVALSHA of a single-key script, the shape
/// `redis.lock.Lock.release()` issues.
#[test]
fn skr2_single_key_evalsha_runs_on_the_keys_shard() {
    let m = spawn_moon(SHARDS);
    let mut loader = Conn::open(m.port);
    let load = loader.send(&["SCRIPT", "LOAD", GET_SCRIPT]);
    let sha = load
        .rsplit("\r\n")
        .find(|s| s.len() == 40)
        .unwrap_or_else(|| panic!("SCRIPT LOAD did not return a sha: {load:?}"))
        .to_string();

    each_trial(m.port, "skr2", |c, k| {
        c.send(&["SET", k, "v"]);
        // SCRIPT EXISTS proves the cache is not the problem: a NOSCRIPT here
        // would be a different defect (a load fan-out that missed a shard).
        let ex = c.send(&["SCRIPT", "EXISTS", &sha]);
        if !ex.contains(":1") {
            return Some(format!("SCRIPT EXISTS replied {ex:?} — script not cached"));
        }
        let r = c.send(&["EVALSHA", &sha, "1", k]);
        (r != "$1\r\nv\r\n").then(|| format!("EVALSHA replied {r:?}, expected the value"))
    });
}

/// A script must be able to WRITE the key it was routed for, not just read it
/// — otherwise routing could be faked by answering reads from the wrong shard.
#[test]
fn skr3_single_key_script_writes_land_on_the_keys_shard() {
    let m = spawn_moon(SHARDS);
    each_trial(m.port, "skr3", |c, k| {
        c.send(&["DEL", k]);
        let set = c.send(&[
            "EVAL",
            "return redis.call('set',KEYS[1],ARGV[1])",
            "1",
            k,
            "w",
        ]);
        if !set.contains("OK") {
            return Some(format!("script SET replied {set:?}"));
        }
        // Read it back through the NORMAL path: if the script wrote to the
        // wrong shard's database, a plain GET (which routes correctly) misses.
        let got = c.send(&["GET", k]);
        (got != "$1\r\nw\r\n").then(|| format!("GET after script SET replied {got:?}"))
    });
}

/// `numkeys=0` has no key to route by and must keep working — it is the case
/// that masked the defect, since it always succeeded.
#[test]
fn skr4_keyless_script_still_runs() {
    let m = spawn_moon(SHARDS);
    each_trial(m.port, "skr4", |c, _k| {
        let r = c.send(&["EVAL", "return 1+1", "0"]);
        (r != ":2\r\n").then(|| format!("keyless EVAL replied {r:?}"))
    });
}

/// The guard must SURVIVE: keys that genuinely span shards still have to be
/// rejected, because a script runs against one shard's database and cannot
/// reach another's.
///
/// This is the assertion that stops the fix from being "delete the check".
#[test]
fn skr5_genuinely_cross_shard_keys_are_still_rejected() {
    let m = spawn_moon(SHARDS);
    // Hash tags force co-location, so `{a}k` and `{b}k` land wherever their
    // TAG hashes — over many distinct tags, some pairs must differ.
    let mut rejected = 0usize;
    let mut accepted = 0usize;
    for i in 0..24 {
        let mut c = Conn::open(m.port);
        let k1 = format!("{{x{i}}}one");
        let k2 = format!("{{y{i}}}two");
        c.send(&["SET", &k1, "1"]);
        c.send(&["SET", &k2, "2"]);
        let r = c.send(&[
            "EVAL",
            "return {redis.call('get',KEYS[1]),redis.call('get',KEYS[2])}",
            "2",
            &k1,
            &k2,
        ]);
        if r.contains("CROSSSLOT") {
            rejected += 1;
        } else if r.contains('1') && r.contains('2') {
            accepted += 1;
        } else {
            panic!("2-key script replied neither a value pair nor CROSSSLOT: {r:?}");
        }
    }
    assert!(
        rejected > 0,
        "no 2-key script was rejected across 24 tag pairs — the cross-shard \
         guard is gone, so a script can now silently read another shard's \
         (empty) view of a key. Fixing #508 must ROUTE same-shard scripts, \
         not delete the check."
    );
    assert!(
        accepted > 0,
        "every 2-key script was rejected across 24 tag pairs — pairs that DO \
         co-locate must still run, or this is just the old bug with extra steps"
    );
}
