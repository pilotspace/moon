//! moon#1298 (prototype): `volatile-ttl` eviction against a real server must
//! evict the SMALLEST remaining TTL first, with the expiry index on the
//! sorted set (default) AND on the time-bucket wheel (`MOON_EXPIRY_WHEEL=1`).
//!
//! Setup: `--shards 1`, no AOF. 1500 volatile keys `vt:{i}` with TTLs that
//! ascend with `i` (so `vt:0` is the nearest), 100 persistent keys, then
//! `vt:0` is retargeted to the FARTHEST TTL (an eager unindex + reindex). The
//! policy is switched to `volatile-ttl` and `maxmemory` lowered below usage;
//! a burst of non-volatile writes forces eviction. What was evicted must be a
//! contiguous prefix of the TTL order (minus the retargeted key), and no
//! persistent key may be evicted.
//!
//! `MOON_BIN=<moon> cargo test --test expiry_wheel_volatile_ttl_1298 -- --include-ignored`

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

mod common;

use std::path::Path;
use std::process::Command;

use common::Conn;

const VOLATILE: usize = 1_500;
const PERSISTENT: usize = 100;

fn spawn(dir: &Path, wheel: bool) -> (common::ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let dir = dir.to_path_buf();
    common::spawn_listening_guarded(move |port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                dir.to_str().unwrap(),
                "--shards",
                "1",
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
            .env("MOON_DISK_FREE_MIN_PCT", "0")
            .env("MOON_EXPIRY_WHEEL", if wheel { "1" } else { "0" })
            .stdout(common::server_stderr(&dir))
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    })
}

fn info_u64(c: &mut Conn, section: &str, field: &str) -> u64 {
    let info = c.send(&["INFO", section]);
    info.lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("INFO {section} has no numeric {field}"))
}

fn exists(c: &mut Conn, key: &str) -> bool {
    c.send(&["EXISTS", key]) == ":1\r\n"
}

fn run(wheel: bool) {
    let dir = common::unique_test_dir(if wheel { "w1298w" } else { "w1298t" });
    let (_guard, port) = spawn(&dir, wheel);
    let mut c = Conn::open(port);
    let value = "x".repeat(1024);
    // Volatile keys, TTL ascending with the index; 1 s apart so several share
    // no bucket and some pairs of neighbours do not straddle one either.
    for chunk in (0..VOLATILE).collect::<Vec<_>>().chunks(250) {
        let keys: Vec<String> = chunk.iter().map(|i| format!("vt:{i}")).collect();
        let ttls: Vec<String> = chunk
            .iter()
            .map(|i| (1_000_000 + i * 3).to_string())
            .collect();
        let cmds: Vec<[&str; 5]> = keys
            .iter()
            .zip(&ttls)
            .map(|(k, t)| ["SET", k.as_str(), value.as_str(), "EX", t.as_str()])
            .collect();
        let refs: Vec<&[&str]> = cmds.iter().map(|c| c.as_slice()).collect();
        let replies = c.pipeline(&refs);
        assert_eq!(replies.matches("+OK").count(), chunk.len());
    }
    for i in 0..PERSISTENT {
        assert_eq!(c.send(&["SET", &format!("plain:{i}"), &value]), "+OK\r\n");
    }
    // The nearest key becomes the FARTHEST: it must survive the eviction.
    assert_eq!(c.send(&["EXPIRE", "vt:0", "9000000"]), ":1\r\n");
    assert_eq!(
        c.send(&["CONFIG", "SET", "maxmemory-policy", "volatile-ttl"]),
        "+OK\r\n"
    );
    let used = info_u64(&mut c, "memory", "used_memory");
    // Room for roughly 1100 of the 1600 keys: ~500 evictions.
    let cap = (used * 7 / 10).to_string();
    assert_eq!(c.send(&["CONFIG", "SET", "maxmemory", &cap]), "+OK\r\n");
    for j in 0..200 {
        let r = c.send(&["SET", &format!("trigger:{j}"), &value]);
        assert!(
            r == "+OK\r\n" || r.starts_with("-OOM"),
            "unexpected reply {r:?}"
        );
    }
    let evicted = info_u64(&mut c, "stats", "evicted_keys");
    assert!(evicted >= 100, "vacuity: only {evicted} keys were evicted");
    // Present/absent pattern over vt:1.. in TTL order.
    let present: Vec<bool> = (1..VOLATILE)
        .map(|i| exists(&mut c, &format!("vt:{i}")))
        .collect();
    let first_present = present.iter().position(|p| *p).unwrap_or(present.len());
    assert!(
        first_present >= 100,
        "vacuity: the cut is at {first_present}"
    );
    assert!(
        present[first_present..].iter().all(|p| *p),
        "volatile-ttl evicted a key with a LATER TTL while an earlier one \
         survived (wheel={wheel}); first survivor vt:{}",
        first_present + 1
    );
    assert!(
        exists(&mut c, "vt:0"),
        "the retargeted (farthest) key was evicted"
    );
    for i in 0..PERSISTENT {
        assert!(
            exists(&mut c, &format!("plain:{i}")),
            "volatile-ttl evicted a persistent key (wheel={wheel})"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn volatile_ttl_evicts_the_nearest_deadline_first_sorted_set() {
    run(false);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn volatile_ttl_evicts_the_nearest_deadline_first_wheel() {
    run(true);
}
