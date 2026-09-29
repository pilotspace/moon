//! Adversarial review of moon#1290 (wave 1): a ROUTED script still
//! plain-drops its eviction victims under `--appendonly no --disk-offload
//! enable`.
//!
//! moon#1290 N6 gave every connection write gate and the SPSC write gate the
//! shard's `ShardManifest`, so a no-AOF victim is tiered durably instead of
//! dropped. The script bridge borrows it through `shard::manifest_cell`,
//! which answers `None` while the event loop holds the cell — and the loop
//! holds it for the whole SPSC drain (`event_loop.rs`: `&mut
//! shard_manifest.borrow_mut()` passed to `drain_spsc_shared`). An `EVAL`
//! whose key another shard owns is executed by that shard INSIDE the drain
//! (`ShardMessage::Execute` with a `script_acl`), so its bridge gate takes the
//! no-manifest plain-drop path: acknowledged keys vanish (`evicted_keys` > 0,
//! `DBSIZE` short). WS28 SUMMARY residual 5 names the path but says it "was
//! not seen in the repro"; this sees it every run.
//!
//! Measured (Linux container, not merge bar), 16,000 x 600 B writes at
//! `--shards 4`, 8 MB `allkeys-lru`:
//!
//! | binary | SET | EVAL SET |
//! |---|---|---|
//! | f7f1d96 monoio | evicted 0, DBSIZE 16000 | evicted 1793, DBSIZE 14207 |
//! | f7f1d96 tokio  | evicted 0, DBSIZE 16000 | evicted 1398, DBSIZE 14602 |
//! | ce65400 monoio | (#1290 base)            | evicted 4057, DBSIZE 11943 |
//!
//! MULTI/EXEC and MSET across shards lose nothing (the SPSC gate has the
//! manifest); only the script bridge inside the drain does.
//!
//!   MOON_BIN=... cargo test --test review_w1_routed_eval_tiering_1290 -- --include-ignored --test-threads 1 --nocapture

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;
mod crash_recovery_cold_support;

use std::io::{Read, Write};
use std::time::Duration;

use crash_recovery_cold_support::*;

const SAVE: [&str; 2] = ["--save", "3600 100000000"];
const WRITES: usize = 16_000;
const SCRIPT: &str = "return redis.call('SET', KEYS[1], ARGV[1])";

fn resp(parts: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n{}\r\n", p.len(), p).as_bytes());
    }
    out
}

/// How each write reaches the owner shard's script bridge.
#[derive(Clone, Copy)]
enum Via {
    /// `EVAL` routed as `ShardMessage::Execute`.
    Eval,
    /// `MULTI / EVAL / EXEC`, the body routed as `ShardMessage::TxnExecute`.
    MultiEval,
    /// `FCALL` routed as `ShardMessage::Execute`.
    Fcall,
}

/// Pipeline `WRITES` script writes (one key each) and drain every reply.
fn pipelined_writes(port: u16, via: Via) -> usize {
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(120))).ok();
    let val = "E".repeat(600);
    let mut errors = 0usize;
    for chunk in (0..WRITES).collect::<Vec<_>>().chunks(500) {
        let mut buf = Vec::with_capacity(chunk.len() * 700);
        for i in chunk {
            let key = format!("eval:{i}");
            match via {
                Via::Eval => buf.extend_from_slice(&resp(&["EVAL", SCRIPT, "1", &key, &val])),
                Via::MultiEval => {
                    buf.extend_from_slice(&resp(&["MULTI"]));
                    buf.extend_from_slice(&resp(&["EVAL", SCRIPT, "1", &key, &val]));
                    buf.extend_from_slice(&resp(&["EXEC"]));
                }
                Via::Fcall => buf.extend_from_slice(&resp(&["FCALL", "ws30set", "1", &key, &val])),
            }
        }
        s.write_all(&buf).expect("write");
        // +OK / +QUEUED / *1 / +OK per transaction; one line per script.
        let per = match via {
            Via::MultiEval => 4,
            Via::Eval | Via::Fcall => 1,
        };
        let mut got = Vec::new();
        let mut lines = 0usize;
        let mut tmp = [0u8; 65536];
        while lines < chunk.len() * per {
            let n = s.read(&mut tmp).expect("read");
            assert!(n > 0, "server closed the connection");
            got.extend_from_slice(&tmp[..n]);
            lines = got.windows(2).filter(|w| w == b"\r\n").count();
        }
        errors += got
            .split(|&b| b == b'\n')
            .filter(|l| l.first() == Some(&b'-'))
            .count();
    }
    errors
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn routed_eval_tiers_every_victim_without_an_aof() {
    run_routed(Via::Eval, "rv-w1-eval-1290");
}

/// WS30 sibling: a script queued in MULTI whose body is routed to the owner
/// shard (`TxnExecute`) runs inside the same drain. Before the lend it
/// plain-dropped 1868 (monoio) / 1989 (tokio) of 16000 at `--shards 4`.
#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn routed_multi_eval_tiers_every_victim_without_an_aof() {
    run_routed(Via::MultiEval, "rv-w1-multi-eval-1290");
}

/// WS30 sibling: a routed `FCALL` (the Functions arm of the same drain).
#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn routed_fcall_tiers_every_victim_without_an_aof() {
    run_routed(Via::Fcall, "rv-w1-fcall-1290");
}

fn run_routed(via: Via, label: &str) {
    let port = common::reserve_port();
    let dir = unique_dir(label);
    std::fs::create_dir_all(&dir).expect("create test dir");
    let mut server = start_moon_alive_with(port, &dir, 3600, "no", &SAVE);
    if let Via::Fcall = via {
        let lib = "#!lua name=ws30lib\nredis.register_function('ws30set', \
                   function(keys, args) return redis.call('SET', keys[1], args[1]) end)";
        let reply = redis_cmd(port, &["FUNCTION", "LOAD", lib]);
        assert!(reply.contains("ws30lib"), "FUNCTION LOAD: {reply}");
    }
    let errors = pipelined_writes(port, via);
    std::thread::sleep(Duration::from_secs(2));
    let evicted = info_u64(port, "evicted_keys").unwrap_or(u64::MAX);
    let spilled = info_u64(port, "spilled_keys").unwrap_or(0);
    let degraded = info_u64(port, "spill_thread_degraded").unwrap_or(0);
    let live = integer_reply(&redis_cmd(port, &["DBSIZE"])).unwrap_or(-1);
    server.kill_now();
    wait_for_port_down(port);
    eprintln!(
        "shards {}: EVAL errors {errors}, evicted_keys {evicted}, spilled_keys {spilled}, \
         spill_thread_degraded {degraded}, DBSIZE {live}/{WRITES}",
        shards()
    );
    let mut wrong = Vec::new();
    if errors != 0 {
        wrong.push(format!("precondition: {errors} EVALs answered an error"));
    }
    if degraded != 0 {
        wrong.push(format!("precondition: spill thread degraded ({degraded})"));
    }
    if evicted != 0 || live != WRITES as i64 {
        wrong.push(format!(
            "{evicted} acknowledged keys plain-dropped by a routed script's eviction gate \
             (DBSIZE {live}, want {WRITES})"
        ));
    }
    finish(&dir, &wrong);
    assert!(wrong.is_empty(), "{wrong:?}");
}
