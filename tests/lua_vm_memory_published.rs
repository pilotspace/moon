//! moon#506 — the shard's Lua VM footprint must be observable.
//!
//! # What this suite pins
//!
//! `INFO memory` must carry `used_memory_lua`, and that number must be the
//! `mlua` VM that actually executed the script — not the script-source cache,
//! which is three orders of magnitude smaller (48 bytes for `return 1`: a
//! 40-char SHA1 key plus an 8-byte body) and was the only thing
//! `ShardStoreMemory::lua` ever carried.
//!
//! # Why it is an integration test and not a unit test
//!
//! The defect was never in the VM. It was in WHERE the sample is taken: the
//! shard periodic tick exists twice in `event_loop.rs` — a tokio `select!` arm
//! and a monoio counter-based arm — and a publish added to one of them is
//! invisible on the other. Only a real server, running the runtime this build
//! ships, proves the sample site is on the path that actually executes.
//!
//! Runs against whichever runtime the crate was built with, so the default
//! (monoio) build covers the runtime that ships. Note that the per-PR CI gate
//! does not run integration tests (they need the `ci-full` label), so this
//! suite's monoio coverage comes from `scripts/ci-local.sh` and the main-push
//! dispatch matrix.

mod common;

use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::Duration;

/// Two shard ticks (100ms each) plus slack — the publish happens on the 100ms
/// eviction tick, so a probe fired immediately after EVAL would race it.
const TICK_SETTLE: Duration = Duration::from_millis(400);

/// A sandboxed VM with `setup_sandbox` + `register_redis_api` applied measures
/// ~24KB. Anything at or below this floor is the script cache (or a zero),
/// which is precisely the wrong-VM/wrong-source symptom moon#506 describes.
const SANE_VM_FLOOR: u64 = 4096;

struct Moon {
    child: Child,
    port: u16,
    dir: std::path::PathBuf,
}

impl Drop for Moon {
    fn drop(&mut self) {
        common::sigkill(&mut self.child);
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn spawn_moon() -> Moon {
    let bin = common::find_moon_binary();
    // Not pid+timestamp: both #[test]s here spawn concurrently, and macOS
    // clocks tick in microseconds, so that shape handed BOTH of them the same
    // --dir often enough to fail ~3% of loaded runs (moon#741).
    let dir = common::unique_test_dir("moon-506-lua");
    std::fs::create_dir_all(&dir).expect("create test dir");
    let dir_arg = dir.clone();
    let (child, port) = common::spawn_listening(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--dir",
                &dir_arg.display().to_string(),
                "--appendonly",
                "no",
            ])
            .stdout(Stdio::null())
            // `spawn_listening` tells you to read the server stderr log in the
            // test's --dir when a child dies before accepting. Discarding
            // stderr made that instruction unfollowable, which is exactly the
            // state this suite was in the one time it failed on macOS. Append
            // (not truncate): the closure runs once per respawn attempt, and
            // the first attempt's reason is the one worth keeping.
            .stderr(common::server_stderr(&dir_arg))
            .spawn()
            .expect("spawn moon")
    });
    Moon { child, port, dir }
}

/// Pull one `field:<int>` line out of an INFO payload.
fn info_field(info: &str, field: &str) -> Option<u64> {
    info.lines().find_map(|l| {
        l.strip_prefix(field)?
            .strip_prefix(':')?
            .trim()
            .parse()
            .ok()
    })
}

#[test]
fn info_memory_reports_the_lua_vm_that_executes_scripts() {
    let moon = spawn_moon();
    let mut conn = common::Conn::open(moon.port);
    assert!(conn.send(&["PING"]).contains("PONG"), "server not ready");

    // The VM is built at connection accept, so it already exists here; the
    // publish still has to reach the atomic via the shard tick.
    sleep(TICK_SETTLE);

    assert_eq!(conn.send(&["EVAL", "return 1", "0"]), ":1\r\n");
    sleep(TICK_SETTLE);

    let info = conn.send(&["INFO", "memory"]);
    let lua = info_field(&info, "used_memory_lua")
        .unwrap_or_else(|| panic!("INFO memory has no `used_memory_lua` field. Payload:\n{info}"));

    assert!(
        lua > SANE_VM_FLOOR,
        "used_memory_lua = {lua}; a sandboxed VM with the redis API registered \
         cannot be that small. A value in the tens of bytes means the published \
         number is the script-source cache, not the VM (moon#506)."
    );
}

#[test]
fn used_memory_lua_tracks_real_lua_allocation() {
    let moon = spawn_moon();
    let mut conn = common::Conn::open(moon.port);
    assert!(conn.send(&["PING"]).contains("PONG"), "server not ready");
    sleep(TICK_SETTLE);

    assert_eq!(conn.send(&["EVAL", "return 1", "0"]), ":1\r\n");
    sleep(TICK_SETTLE);
    let before = info_field(&conn.send(&["INFO", "memory"]), "used_memory_lua")
        .expect("used_memory_lua present");

    // Anchor the table in _G so the collector cannot reclaim it before the
    // next tick samples the VM. ~200k short strings ≈ 4MB of Lua heap.
    assert_eq!(
        conn.send(&[
            "EVAL",
            "local t = {} for i = 1, 200000 do t[i] = string.rep('x', 6) end _G.MOON506 = t return 1",
            "0",
        ]),
        ":1\r\n"
    );
    sleep(TICK_SETTLE);
    let after = info_field(&conn.send(&["INFO", "memory"]), "used_memory_lua")
        .expect("used_memory_lua present");

    assert!(
        after > before + 1_000_000,
        "used_memory_lua went {before} -> {after} across a script that anchors \
         several MB of Lua tables in _G. The published figure is not the \
         executing VM (moon#506)."
    );

    // used_memory must carry the same growth: the Lua heap is real RSS and an
    // operator watching a runaway script has to see it somewhere.
    let used =
        info_field(&conn.send(&["INFO", "memory"]), "used_memory").expect("used_memory present");
    assert!(
        used >= after,
        "used_memory ({used}) must include the Lua VM footprint ({after})"
    );
}
