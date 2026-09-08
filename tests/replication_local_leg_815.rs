//! moon#815: the coordinator's LOCAL legs (the slice of a multi-key write that
//! the connection's own shard owns — `MSET`/`MSETNX` co-located or scattered,
//! `DEL`/`UNLINK`, `COPY`, `BITOP`) reached the AOF but never the replica,
//! while `issue_append_lsn` still advanced `master_repl_offset` for them.
//!
//! Two symptoms, one root cause, both asserted here:
//!
//! * `--appendonly yes`: the offset counts bytes no replica can ever receive,
//!   so `WAIT 1 <t>` answers `:0` forever once a local leg has run.
//! * `--appendonly no`: `persist_local_leg` short-circuits on the missing AOF
//!   pool, so the leg neither advances the offset NOR replicates — `WAIT`
//!   answers `:1` while the replica silently diverges.
//!
//! Black-box over real `moon` processes; `#[ignore]`d like every other
//! replication suite (they need a built binary and two live servers):
//!
//! ```text
//! MOON_BIN=./target/release/moon \
//!   cargo test --test replication_local_leg_815 -- --ignored --nocapture
//! ```

mod common;

use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

use common::Conn;

/// Enough co-located groups that, whatever shard the client connection lands
/// on, several groups are owned by that shard and take the local leg.
const GROUPS: usize = 24;

fn start_moon(port: u16, dir: &std::path::Path, shards: usize, appendonly: bool) -> Child {
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            &shards.to_string(),
            "--dir",
            dir.to_str().expect("utf8 dir"),
            "--appendonly",
            if appendonly { "yes" } else { "no" },
            "--appendfsync",
            "everysec",
            // /Volumes/Games hovers near the 5% diskfull guard; disable it so
            // a low-free-space dev host does not turn writes into MOONERR.
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon (set MOON_BIN to a built binary)")
}

fn wait_until(timeout: Duration, mut f: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

/// `master_repl_offset` and `slave0`'s `offset=` from `INFO replication`.
fn master_and_replica_offsets(info: &str) -> (u64, u64) {
    let mut master = 0;
    let mut replica = 0;
    for line in info.lines() {
        let line = line.trim();
        if let Some(v) = line.strip_prefix("master_repl_offset:") {
            master = v.trim().parse().unwrap_or(0);
        }
        if line.starts_with("slave0:") {
            for field in line.split(',') {
                if let Some(v) = field.trim().strip_prefix("offset=") {
                    replica = v.trim().parse().unwrap_or(0);
                }
            }
        }
    }
    (master, replica)
}

fn bulk(reply: &str) -> Option<String> {
    let mut lines = reply.split("\r\n");
    let head = lines.next()?;
    if head == "$-1" {
        return None;
    }
    head.strip_prefix('$')?;
    lines.next().map(str::to_owned)
}

fn scenario(shards: usize, appendonly: bool) {
    let mdir = common::unique_test_dir("moon-815-master");
    let rdir = common::unique_test_dir("moon-815-replica");
    std::fs::create_dir_all(&mdir).expect("mdir");
    std::fs::create_dir_all(&rdir).expect("rdir");
    let (mut master, mport) =
        common::spawn_listening_guarded(|p| start_moon(p, &mdir, shards, appendonly));
    let (mut replica, rport) = common::spawn_listening_guarded(|p| start_moon(p, &rdir, 1, false));

    let mut m = Conn::open(mport);
    let mut r = Conn::open(rport);
    assert!(
        r.send(&["REPLICAOF", "127.0.0.1", &mport.to_string()])
            .starts_with("+OK"),
        "REPLICAOF refused"
    );
    assert!(
        wait_until(Duration::from_secs(20), || r
            .send(&["INFO", "replication"])
            .contains("master_link_status:up")),
        "replica link never came up (shards={shards} appendonly={appendonly})"
    );

    // Baseline: an ordinary single-key write is acknowledged by the replica.
    assert!(m.send(&["SET", "fence:0", "start"]).starts_with("+OK"));
    assert_eq!(
        m.send(&["WAIT", "1", "3000"]).trim(),
        ":1",
        "WAIT baseline before any local leg (shards={shards} appendonly={appendonly})"
    );

    // The writes under test, all on ONE connection so a fixed shard owns the
    // local legs. Every group is co-located by hash tag; across GROUPS tags
    // every shard owns several, so the connection's own shard takes the
    // in-process leg for its share of them.
    for t in 0..GROUPS {
        let a = format!("{{t{t}}}:a");
        let b = format!("{{t{t}}}:b");
        let c = format!("{{t{t}}}:c");
        let zz = format!("{{t{t}}}:zz");
        assert!(
            m.send(&["MSET", &a, "va", &b, "vb"]).starts_with("+OK"),
            "MSET group {t}"
        );
        // `c` arrives via the single-key path (replicated on main), so a
        // multi-key DEL that misses the replica is observable as `c`
        // surviving there — a DEL of a never-replicated key would not be.
        assert!(m.send(&["SET", &c, "vc"]).starts_with("+OK"));
        assert_eq!(m.send(&["DEL", &c, &zz]).trim(), ":1", "DEL group {t}");
    }
    // A scattered MSET: keys spread over every shard, so exactly one slice is
    // the connection's local leg and the rest travel the SPSC path.
    let scattered: Vec<String> = (0..GROUPS)
        .flat_map(|t| [format!("{{t{t}}}:s"), format!("s{t}")])
        .collect();
    let mut mset: Vec<&str> = vec!["MSET"];
    mset.extend(scattered.iter().map(String::as_str));
    assert!(m.send(&mset).starts_with("+OK"), "scattered MSET");
    assert!(m.send(&["SET", "fence:1", "end"]).starts_with("+OK"));

    // moon#815 (appendonly=yes): `:0` forever — the offset counts the local
    // legs' bytes and the replica can never ACK them.
    let w = m.send(&["WAIT", "1", "3000"]);
    assert_eq!(
        w.trim(),
        ":1",
        "WAIT after coordinator local legs (shards={shards} appendonly={appendonly}): {w:?}"
    );

    // The link is healthy — the fence written AFTER the legs arrives — so
    // whatever is missing below was dropped, not delayed.
    assert!(
        wait_until(Duration::from_secs(20), || bulk(
            &r.send(&["GET", "fence:1"])
        )
        .as_deref()
            == Some("end")),
        "fence never reached the replica (shards={shards} appendonly={appendonly})"
    );
    let mut missing: Vec<String> = Vec::new();
    for t in 0..GROUPS {
        let a = format!("{{t{t}}}:a");
        let c = format!("{{t{t}}}:c");
        let s = format!("{{t{t}}}:s");
        if bulk(&r.send(&["GET", &a])).as_deref() != Some("va") {
            missing.push(format!("{a} (co-located MSET)"));
        }
        if bulk(&r.send(&["GET", &c])).is_some() {
            missing.push(format!("{c} (DEL not applied)"));
        }
        if bulk(&r.send(&["GET", &s])).as_deref() != Some(&format!("s{t}")) {
            missing.push(format!("{s} (scattered MSET slice)"));
        }
    }
    assert!(
        missing.is_empty(),
        "moon#815: {} replica divergences after coordinator local legs \
         (shards={shards} appendonly={appendonly}): {missing:?}",
        missing.len()
    );

    // And the offsets agree: every byte the master counted reached the replica.
    let info = m.send(&["INFO", "replication"]);
    let (master_off, replica_off) = master_and_replica_offsets(&info);
    assert!(
        master_off > 0 && master_off == replica_off,
        "master_repl_offset {master_off} != slave0 offset {replica_off} \
         (shards={shards} appendonly={appendonly})"
    );

    replica.kill_now();
    master.kill_now();
    let _ = std::fs::remove_dir_all(&mdir);
    let _ = std::fs::remove_dir_all(&rdir);
}

/// Control: at `--shards 1` the monoio handler never enters the coordinator,
/// so this passes before and after the fix. It stays here because the
/// self-SPSC gap (no self-loop at shards=1) has masked a dead live stream
/// before — a green here proves the harness itself, not the fix.
#[test]
#[ignore]
fn local_legs_replicate_shards1_appendonly_yes() {
    scenario(1, true);
}

#[test]
#[ignore]
fn local_legs_replicate_shards1_appendonly_no() {
    scenario(1, false);
}

/// RED on main: `WAIT 1 3000` answers `:0` and the local legs' keys are
/// missing on the replica.
#[test]
#[ignore]
fn local_legs_replicate_shards4_appendonly_yes() {
    scenario(4, true);
}

/// RED on main: `WAIT` answers `:1` (nothing advanced the offset) while the
/// local legs' keys are missing on the replica.
#[test]
#[ignore]
fn local_legs_replicate_shards4_appendonly_no() {
    scenario(4, false);
}
