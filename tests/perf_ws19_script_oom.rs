//! moon#1241 against a real server: inside a script, a command that can only
//! shrink memory (DEL, UNLINK, ...) is not refused for memory on an
//! over-budget shard — as on the connection path and the routed leg, and as
//! redis allows (those commands are not `denyoom`). A growing command in a
//! script is still refused.
//!
//! Replies, measured against redis-server 7.0.15 (`noeviction`, over
//! maxmemory):
//! - `EVAL "return redis.call('DEL', KEYS[1])" 1 k` -> `:1`
//! - `EVAL "return redis.pcall('UNLINK', KEYS[1])" 1 k` -> `:1`
//! - `EVAL "return redis.call('SET', KEYS[1], 'v')" 1 k` -> `-OOM ...`
//! - `FCALL` of a function registered WITHOUT `allow-oom` -> `-OOM ...`, even
//!   if it only deletes: redis refuses the whole call.
//! - `FCALL` of a function registered with `flags={'allow-oom'}` that deletes
//!   -> `:1`.
//! - `FCALL` of a function registered with `flags={'allow-oom'}` that SETs
//!   -> `+OK`: redis runs ANY command in an `allow-oom` function
//!   (`SCRIPT_ALLOW_OOM`; PR #1268 review — moon refused it).
//!
//! `script_oom_matches_redis_7_0_15` (ignored: needs `redis-server` on PATH)
//! runs the same checks against redis-server itself.
//!
//! Before the fix moon answered `-OOM` to all five (the script gate had no
//! command in scope). Runs at `--shards 1` and 4; all keys share a hash tag,
//! so every call runs on the one shard that is over budget.

#![allow(clippy::unwrap_used)]

mod common;

use common::{Conn, ServerGuard};

const TAG: &str = "{oom}";

fn spawn(dir: &std::path::Path, shards: usize) -> (ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
                "--save",
                "",
                "--disk-offload",
                "disable",
                "--maxmemory",
                "4mb",
                "--maxmemory-policy",
                "noeviction",
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    (ServerGuard::new(child), port)
}

/// SET 64 KiB values under the tag until the shard refuses even a tiny
/// write for memory. Returns the keys written (all still there:
/// `noeviction`).
///
/// redis counts the query buffer a value arrives in, so near the limit it
/// refuses the 64 KiB SET while tiny commands still pass. A refused big SET
/// is therefore followed by a tiny command that grows the dataset by 64 KiB
/// (`SETRANGE pad 65535 x`), which carries the dataset itself over the limit
/// on either server.
fn fill_until_oom(c: &mut Conn, next: &mut usize) -> Vec<String> {
    let value = "x".repeat(64 * 1024);
    let mut written = Vec::new();
    for _ in 0..2_000 {
        let probe = c.send(&["SET", &format!("{TAG}:probe"), "v"]);
        if probe.starts_with("-OOM") {
            return written;
        }
        assert!(probe.starts_with("+OK"), "SET probe: {probe}");
        let key = format!("{TAG}:big:{next}");
        *next += 1;
        let reply = c.send(&["SET", &key, &value]);
        if reply.starts_with("+OK") {
            written.push(key);
        } else {
            assert!(reply.starts_with("-OOM"), "SET {key}: {reply}");
            let pad = format!("{TAG}:pad:{next}");
            *next += 1;
            let grown = c.send(&["SETRANGE", &pad, "65535", "x"]);
            if grown.starts_with(':') {
                written.push(pad);
            } else {
                assert!(grown.starts_with("-OOM"), "SETRANGE {pad}: {grown}");
            }
        }
    }
    panic!("the shard never refused a write under noeviction with --maxmemory 4mb");
}

/// A key the tag's shard holds, with the shard over budget right now.
fn over_budget_victim(c: &mut Conn, next: &mut usize, keys: &mut Vec<String>) -> String {
    keys.extend(fill_until_oom(c, next));
    keys.pop().expect("a key to delete")
}

/// Every check on the server at `port`; the ones whose reply differs from
/// redis 7.0.15's.
fn script_oom_mismatches(port: u16) -> Vec<String> {
    let mut c = Conn::open(port);
    let lib = "#!lua name=ws19oom\n\
        redis.register_function('fdel', function(keys, args) return redis.call('DEL', keys[1]) end)\n\
        redis.register_function{function_name='fdel_oom', \
          callback=function(keys, args) return redis.call('DEL', keys[1]) end, flags={'allow-oom'}}\n\
        redis.register_function{function_name='fset_oom', \
          callback=function(keys, args) return redis.call('SET', keys[1], 'v') end, flags={'allow-oom'}}";
    let loaded = c.send(&["FUNCTION", "LOAD", lib]);
    assert!(loaded.contains("ws19oom"), "FUNCTION LOAD: {loaded}");

    let mut next = 0usize;
    let mut keys = Vec::new();
    let mut wrong: Vec<String> = Vec::new();
    let mut check = |what: &str, reply: String, ok: bool| {
        if !ok {
            wrong.push(format!("{what} answered {:?}", reply.trim()));
        }
    };

    // Refused, as redis refuses them.
    let victim = over_budget_victim(&mut c, &mut next, &mut keys);
    let reply = c.send(&[
        "EVAL",
        "return redis.call('SET', KEYS[1], 'v')",
        "1",
        &format!("{TAG}:new"),
    ]);
    let ok = reply.starts_with("-OOM");
    check("script SET over budget", reply, ok);
    let reply = c.send(&["FCALL", "fdel", "1", &victim]);
    let ok = reply.starts_with("-OOM");
    check("FCALL of a DEL-only function without allow-oom", reply, ok);
    keys.push(victim);

    // Allowed.
    let victim = over_budget_victim(&mut c, &mut next, &mut keys);
    let reply = c.send(&["EVAL", "return redis.call('DEL', KEYS[1])", "1", &victim]);
    let ok = reply.trim() == ":1";
    check("script DEL over budget", reply, ok);

    let victim = over_budget_victim(&mut c, &mut next, &mut keys);
    let reply = c.send(&[
        "EVAL",
        "return redis.pcall('UNLINK', KEYS[1])",
        "1",
        &victim,
    ]);
    let ok = reply.trim() == ":1";
    check("script UNLINK (pcall) over budget", reply, ok);

    let victim = over_budget_victim(&mut c, &mut next, &mut keys);
    let reply = c.send(&["FCALL", "fdel_oom", "1", &victim]);
    let ok = reply.trim() == ":1";
    check("FCALL of an allow-oom DEL function over budget", reply, ok);

    let _ = over_budget_victim(&mut c, &mut next, &mut keys);
    let reply = c.send(&["FCALL", "fset_oom", "1", &format!("{TAG}:grown")]);
    let ok = reply.starts_with("+OK");
    check("FCALL of an allow-oom SET function over budget", reply, ok);
    wrong
}

fn script_oom(shards: usize) {
    let dir = common::unique_test_dir(&format!("ws19-1241-s{shards}"));
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = spawn(&dir, shards);
    let wrong = script_oom_mismatches(port);
    server.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(wrong.is_empty(), "--shards {shards}: {wrong:#?}");
}

/// The oracle: the same checks against redis-server 7.0.15, `noeviction`
/// over a 4 MB `maxmemory`.
#[test]
#[ignore] // Needs redis-server (7.0.15) on PATH; run explicitly.
fn script_oom_matches_redis_7_0_15() {
    let dir = common::unique_test_dir("ws19-1241-oracle");
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = common::spawn_listening_guarded(|port| {
        std::process::Command::new("redis-server")
            .args([
                "--port",
                &port.to_string(),
                "--save",
                "",
                "--appendonly",
                "no",
            ])
            .args(["--maxmemory", "4mb", "--maxmemory-policy", "noeviction"])
            .arg("--dir")
            .arg(&dir)
            .stdout(common::server_stderr(&dir))
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn redis-server (on PATH)")
    });
    let wrong = script_oom_mismatches(port);
    server.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(wrong.is_empty(), "redis-server disagrees: {wrong:#?}");
}

#[test]
fn shrink_only_script_commands_over_budget_one_shard() {
    script_oom(1);
}

#[test]
fn shrink_only_script_commands_over_budget_four_shards() {
    script_oom(4);
}

// ── The per-database quota is not maxmemory (PR #1268 review) ──────────────

/// `allow-oom` is redis's flag against `maxmemory`; moon's per-database
/// `--db-maxmemory` quota is an additive tenant cap redis does not have, so
/// an `allow-oom` function must not write past it. Under `noeviction` with db
/// 1 over its quota: an `allow-oom` SET gets the quota error, while a
/// shrink-only command — in an `allow-oom` function or an EVAL — is never
/// refused by the quota (a tenant must be able to delete its way back).
#[test]
fn an_allow_oom_function_does_not_write_past_a_db_quota() {
    let dir = common::unique_test_dir("ws19-1241-quota");
    std::fs::create_dir_all(&dir).unwrap();
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args(["--port", &port.to_string(), "--dir", &dir.to_string_lossy()])
            .args(["--shards", "1", "--appendonly", "no", "--save", ""])
            .args(["--disk-offload", "disable", "--disk-free-min-pct", "0"])
            .args(["--maxmemory", "0", "--maxmemory-policy", "noeviction"])
            .args(["--db-maxmemory", "1:1mb"])
            .stdout(common::server_stderr(&dir))
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    let mut server = ServerGuard::new(child);
    let mut c = Conn::open(port);
    let lib = "#!lua name=ws19quota\n\
        redis.register_function{function_name='qset', \
          callback=function(keys, args) return redis.call('SET', keys[1], 'v') end, flags={'allow-oom'}}\n\
        redis.register_function{function_name='qdel', \
          callback=function(keys, args) return redis.call('DEL', keys[1]) end, flags={'allow-oom'}}";
    let loaded = c.send(&["FUNCTION", "LOAD", lib]);
    assert!(loaded.contains("ws19quota"), "FUNCTION LOAD: {loaded}");
    assert!(c.send(&["SELECT", "1"]).starts_with("+OK"));
    let value = "x".repeat(16 * 1024);
    let mut keys = Vec::new();
    for i in 0..1_000 {
        let key = format!("q:{i}");
        let reply = c.send(&["SET", &key, &value]);
        if !reply.starts_with("+OK") {
            assert!(
                reply.contains("db maxmemory exceeded"),
                "SET {key}: {reply}"
            );
            break;
        }
        keys.push(key);
    }
    assert!(
        c.send(&["SET", "probe", "v"])
            .contains("db maxmemory exceeded"),
        "fixture: db 1 over its quota"
    );
    let set = c.send(&["FCALL", "qset", "1", "grown"]);
    let del_fn = c.send(&["FCALL", "qdel", "1", &keys.pop().unwrap()]);
    let del_eval = c.send(&[
        "EVAL",
        "return redis.call('DEL', KEYS[1])",
        "1",
        &keys.pop().unwrap(),
    ]);
    server.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        set.contains("db maxmemory exceeded"),
        "an allow-oom SET past db 1's quota answered {set:?}"
    );
    assert_eq!(del_fn.trim(), ":1", "an allow-oom DEL over the quota");
    assert_eq!(del_eval.trim(), ":1", "an EVAL DEL over the quota");
}
