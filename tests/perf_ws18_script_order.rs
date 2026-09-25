//! moon#1235: a `SCRIPT LOAD` racing a `SCRIPT FLUSH` (and a `FUNCTION LOAD`
//! racing a `FUNCTION FLUSH`) from connections on different shards must leave
//! EVERY shard agreeing once both replies are in — the script (library) is
//! either loaded everywhere or gone everywhere. redis has one cache, so its
//! answer never depends on which shard a later call lands on.
//!
//! Each trial releases the two commands through a barrier on two connections,
//! waits for both replies, then asks every shard: `EVALSHA` / `FCALL` over one
//! key owned by each shard (a routed call runs against the OWNER's cache), and
//! `SCRIPT EXISTS` from 16 connections. A trial is MIXED when the shards
//! disagree. 0 mixed trials is the bar.
//!
//! Red on `d155cd6` / `ae21476`: each origin applied its op locally first and
//! then replayed it on its own rings, so two concurrent mutations from two
//! shards reached the others in different orders (the review measured 121 of
//! 400 SCRIPT trials and 96 of 200 FUNCTION trials mixed at `--shards 4`).
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws18_script_order`.

#![allow(clippy::unwrap_used)]

mod common;

use std::sync::{Arc, Barrier};

use common::{Conn, ServerGuard};

const TRIALS: usize = 200;
const PROBE_CONNS: usize = 16;

fn spawn(dir: &std::path::Path, shards: usize) -> (ServerGuard, u16) {
    spawn_env(dir, shards, &[])
}

fn spawn_env(dir: &std::path::Path, shards: usize, env: &[(&str, &str)]) -> (ServerGuard, u16) {
    std::fs::create_dir_all(dir).expect("create test dir");
    let bin = common::find_moon_binary();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .envs(env.iter().copied())
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
                "0",
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard::new(child), port)
}

/// One key owned by each shard, in shard order.
fn key_per_shard(shards: usize) -> Vec<String> {
    let mut out: Vec<Option<String>> = vec![None; shards];
    let mut i = 0usize;
    while out.iter().any(Option::is_none) {
        let k = format!("ws18:order:{i}");
        let s = moon::shard::dispatch::key_to_shard(k.as_bytes(), shards);
        if out[s].is_none() {
            out[s] = Some(k);
        }
        i += 1;
    }
    out.into_iter().map(Option::unwrap).collect()
}

fn sha1_hex(body: &str) -> String {
    sha1_smol::Sha1::from(body.as_bytes()).hexdigest()
}

/// Run `a` and `b` on their own connections, released together, and return
/// both replies.
fn race(a: &mut Conn, a_cmd: &[&str], b: &mut Conn, b_cmd: &[&str]) -> (String, String) {
    let barrier = Arc::new(Barrier::new(2));
    std::thread::scope(|s| {
        let ba = Arc::clone(&barrier);
        let ha = s.spawn(move || {
            ba.wait();
            a.send(a_cmd)
        });
        let bb = Arc::clone(&barrier);
        let hb = s.spawn(move || {
            bb.wait();
            b.send(b_cmd)
        });
        (ha.join().unwrap(), hb.join().unwrap())
    })
}

/// Connections spread over the shards (placement is the kernel's), so a
/// racing pair usually has its two origins on different shards.
fn open_pool(port: u16, n: usize) -> Vec<Conn> {
    (0..n).map(|_| Conn::open(port)).collect()
}

fn scripts(shards: usize) {
    let dir = common::unique_test_dir(&format!("ws18-script-order-s{shards}"));
    let (guard, port) = spawn(&dir, shards);
    let keys = key_per_shard(shards);
    let mut loaders = open_pool(port, 4);
    let mut flushers = open_pool(port, 4);
    let mut probes = open_pool(port, PROBE_CONNS);
    let mut mixed = Vec::new();
    for t in 0..TRIALS {
        let body = format!("return {t}");
        let sha = sha1_hex(&body);
        let (la, fb) = (t % loaders.len(), (t / loaders.len()) % flushers.len());
        let (load, flush) = race(
            &mut loaders[la],
            &["SCRIPT", "LOAD", &body],
            &mut flushers[fb],
            &["SCRIPT", "FLUSH"],
        );
        assert_eq!(load, format!("$40\r\n{sha}\r\n"), "trial {t}: SCRIPT LOAD");
        assert_eq!(flush, "+OK\r\n", "trial {t}: SCRIPT FLUSH");
        // Every shard's cache, through a call routed to a key it owns.
        let mut view = String::new();
        for k in &keys {
            let r = probes[0].send(&["EVALSHA", &sha, "1", k]);
            view.push(if r.starts_with("-NOSCRIPT") { '0' } else { '1' });
        }
        view.push('|');
        for p in probes.iter_mut() {
            let r = p.send(&["SCRIPT", "EXISTS", &sha]);
            view.push(if r == "*1\r\n:1\r\n" { '1' } else { '0' });
        }
        if view.contains('0') && view.contains('1') {
            mixed.push(format!("trial {t}: {view}"));
        }
    }
    assert!(
        mixed.is_empty(),
        "--shards {shards}: {} of {TRIALS} SCRIPT LOAD/FLUSH trials left the shards \
         disagreeing (EVALSHA per shard | SCRIPT EXISTS per connection): {:?}",
        mixed.len(),
        &mixed[..mixed.len().min(8)]
    );
    drop(probes);
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

fn functions(shards: usize) {
    let dir = common::unique_test_dir(&format!("ws18-function-order-s{shards}"));
    let (guard, port) = spawn(&dir, shards);
    let keys = key_per_shard(shards);
    let mut loaders = open_pool(port, 4);
    let mut flushers = open_pool(port, 4);
    let mut probe = Conn::open(port);
    let mut mixed = Vec::new();
    for t in 0..TRIALS {
        let body = format!(
            "#!lua name=ws18lib{t}\nredis.register_function('ws18f{t}', \
             function(keys, args) return {t} end)"
        );
        let func = format!("ws18f{t}");
        let (la, fb) = (t % loaders.len(), (t / loaders.len()) % flushers.len());
        let (load, flush) = race(
            &mut loaders[la],
            &["FUNCTION", "LOAD", &body],
            &mut flushers[fb],
            &["FUNCTION", "FLUSH"],
        );
        let lib = format!("ws18lib{t}");
        assert_eq!(
            load,
            format!("${}\r\n{lib}\r\n", lib.len()),
            "trial {t}: FUNCTION LOAD"
        );
        assert_eq!(flush, "+OK\r\n", "trial {t}: FUNCTION FLUSH");
        let mut view = String::new();
        for k in &keys {
            let r = probe.send(&["FCALL", &func, "1", k]);
            view.push(if r.starts_with(':') { '1' } else { '0' });
        }
        if view.contains('0') && view.contains('1') {
            mixed.push(format!("trial {t}: {view}"));
        }
    }
    assert!(
        mixed.is_empty(),
        "--shards {shards}: {} of {TRIALS} FUNCTION LOAD/FLUSH trials left the shards \
         disagreeing (FCALL per shard): {:?}",
        mixed.len(),
        &mixed[..mixed.len().min(8)]
    );
    drop(probe);
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn script_load_vs_flush_converges_at_shards_2() {
    scripts(2);
}

#[test]
fn script_load_vs_flush_converges_at_shards_4() {
    scripts(4);
}

#[test]
fn function_load_vs_flush_converges_at_shards_2() {
    functions(2);
}

#[test]
fn function_load_vs_flush_converges_at_shards_4() {
    functions(4);
}

/// moon#567: a `SCRIPT LOAD` whose replay did not reach every shard must say
/// so, as `SCRIPT FLUSH` and `FUNCTION LOAD` already do. It used to answer the
/// sha over shards that never got the body, so the client's next `EVALSHA`
/// met `NOSCRIPT` for a sha the server had just returned (the redis-py
/// `Lock.release()` failure). Every peer is wedged so the outcome does not
/// depend on which shard the connection lands on (see
/// `script_function_fanout::sff12`).
///
/// Red on `d155cd6` / `ae21476`: the reply was `$40\r\n<sha>`.
#[test]
fn script_load_partial_fanout_is_reported_not_swallowed() {
    let dir = common::unique_test_dir("ws18-script-load-partial");
    let (guard, port) = spawn_env(&dir, 4, &[("MOON_TEST_DROP_FANOUT_TO_SHARD", "0,1,2,3")]);
    let r = Conn::open(port).send(&["SCRIPT", "LOAD", "return 'ws18-partial'"]);
    assert_eq!(
        r,
        "-MOONERR partialfanout SCRIPT LOAD applied on 1 of 4 shards; re-issue it to converge\r\n",
        "a SCRIPT LOAD the server could not publish must not answer the sha"
    );
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}
