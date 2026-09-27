//! Review round 3 (cc26019, round-2b MAJOR-3's missed sibling): an eviction
//! a Lua script's `redis.call` triggers must reach the AOF as a `DEL`, or the
//! evicted keys come back on the next restart.
//!
//! cc26019 wired `record_reason_del_conn` into the two tokio connection write
//! gates and made it available on both runtimes, but the third gate — the
//! script bridge's (`scripting/bridge.rs`, `LuaEvictionCtx`) — still has
//! `#[cfg(not(feature = "runtime-monoio"))] { let _ = key; }` as its
//! plain-drop sink. Under `runtime-tokio`, every victim a script's write
//! evicts is dropped without a `DEL`, and the AOF replays it back. redis
//! propagates every eviction as a `DEL`.
//!
//! Red on 4e06039 tokio (29,169 of 29,175 evicted keys back); monoio passes.
//!
//! Run: MOON_BIN=<bin> cargo test --test review_r3_lua_eviction_aof -- --ignored

#![allow(clippy::unwrap_used)]

mod common;

use std::path::Path;
use std::process::Command;
use std::time::Duration;

use common::Conn;

const KEYS: usize = 40_000;
const BATCH: usize = 500;
const SCRIPT: &str = "for i=tonumber(ARGV[1]),tonumber(ARGV[2]) do \
                      redis.call('SET','k:'..i, string.rep('x',600)) end return 1";

fn start(dir: &Path, maxmemory: &str) -> (common::ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let dir = dir.to_path_buf();
    let maxmemory = maxmemory.to_string();
    let (child, port) = common::spawn_listening(move |port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--maxmemory",
                &maxmemory,
                "--maxmemory-policy",
                "allkeys-lru",
                "--disk-offload",
                "disable",
                "--appendonly",
                "yes",
                "--save",
                "",
                "--dir",
                dir.to_str().unwrap(),
            ])
            .stdout(common::server_stderr(&dir))
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon")
    });
    (common::ServerGuard::new(child), port)
}

fn is_nil(c: &mut Conn, i: usize) -> bool {
    c.send(&["GET", &format!("k:{i}")]).starts_with("$-1")
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn evictions_a_script_triggers_are_not_resurrected_by_the_aof() {
    let dir = common::unique_test_dir("r3-lua-evict-aof");
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = start(&dir, "8388608");
    let mut c = Conn::open(port);
    for lo in (0..KEYS).step_by(BATCH) {
        let (a, b) = (lo.to_string(), (lo + BATCH - 1).to_string());
        let r = c.send(&["EVAL", SCRIPT, "0", &a, &b]);
        assert!(r.starts_with(':'), "EVAL reply: {r}");
    }
    let nil_live: Vec<usize> = (0..KEYS).filter(|&i| is_nil(&mut c, i)).collect();
    eprintln!("{} of {KEYS} keys read nil live", nil_live.len());
    assert!(!nil_live.is_empty(), "fixture: nothing was evicted");

    // everysec: let the tail reach the disk, then restart without maxmemory.
    std::thread::sleep(Duration::from_millis(2_500));
    server.kill_now();
    common::wait_for_port_down(port);
    drop(c);
    let (_server2, port2) = start(&dir, "0");
    let mut c2 = Conn::open(port2);
    let resurrected = nil_live.iter().filter(|&&i| !is_nil(&mut c2, i)).count();
    assert_eq!(
        resurrected,
        0,
        "{resurrected} of {} keys a script's writes EVICTED came back after the restart \
         (the script bridge's plain-drop sink logged no DEL); dir {}",
        nil_live.len(),
        dir.display()
    );
    let _ = std::fs::remove_dir_all(&dir);
}
