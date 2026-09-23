//! moon#1130 — a plain (non-blocking) `XREADGROUP`, and `XCLAIM` /
//! `XAUTOCLAIM`, must be logged as the EFFECT they had, never re-run.
//!
//! These commands decide what they do from the clock: a read stamps every
//! entry it delivers with the delivery time, and a claim takes only what has
//! been idle for `min-idle-time`. Logged verbatim, a replay (a restart, or a
//! replica applying the stream) re-decided both at REPLAY time: every pending
//! entry's idle time restarted from zero, and a claim that followed found the
//! entry only a few ms idle and took nothing — `XPENDING` after the restart
//! named the old owner again.
//!
//! Redis propagates each of them as `XCLAIM ... 0 <ids> TIME <ms> ... FORCE`
//! (min-idle 0: nothing is re-decided), plus the group's cursor. Moon now
//! does the same, from the reply the command answered.
//!
//! ```text
//! MOON_BIN=/path/to/moon cargo test --test stream_group_plain_log_1130
//! ```

mod common;

use std::process::Child;
use std::time::{Duration, Instant};

use common::{Conn, spawn_listening_guarded, unique_test_dir};

fn spawn(port: u16, dir: &std::path::Path, shards: usize, aof: bool) -> Child {
    let mut args = vec![
        "--port".to_string(),
        port.to_string(),
        "--dir".to_string(),
        dir.to_string_lossy().into_owned(),
        "--shards".to_string(),
        shards.to_string(),
        // Below the ~5%-free guard every write answers `MOONERR diskfull`.
        "--disk-free-min-pct".to_string(),
        "0".to_string(),
    ];
    if aof {
        args.extend(["--appendonly", "yes", "--appendfsync", "always"].map(String::from));
    } else {
        args.extend(["--appendonly", "no"].map(String::from));
    }
    std::process::Command::new(common::find_moon_binary())
        .args(&args)
        .stdout(std::process::Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon (set MOON_BIN to a built binary)")
}

/// One pending entry: `(id, consumer, idle-ms, delivery-count)`.
type Pending = (String, String, u64, u64);

/// `XPENDING key g - + 100`, parsed.
fn pending(c: &mut Conn, key: &str) -> Vec<Pending> {
    let raw = c.send(&["XPENDING", key, "g", "-", "+", "100"]);
    let lines: Vec<&str> = raw.split("\r\n").collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        if lines[i] == "*4" && i + 6 < lines.len() {
            // *4 / $n / id / $n / consumer / :idle / :count
            let id = lines[i + 2].to_string();
            let consumer = lines[i + 4].to_string();
            let idle = lines[i + 5].trim_start_matches(':').parse().unwrap_or(0);
            let count = lines[i + 6].trim_start_matches(':').parse().unwrap_or(0);
            out.push((id, consumer, idle, count));
            i += 7;
        } else {
            i += 1;
        }
    }
    out
}

/// The group's state minus the clock: cursor + consumer names + PEL
/// ownership and counts.
fn group_state(c: &mut Conn, key: &str) -> String {
    let mut groups = c.send(&["XINFO", "GROUPS", key]);
    if let Some(i) = groups.find("lag\r\n") {
        groups.truncate(i);
    }
    // Consumer names only: each follows its `name` field. The seen/idle
    // fields move with the clock.
    let raw = c.send(&["XINFO", "CONSUMERS", key, "g"]);
    let lines: Vec<&str> = raw.split("\r\n").collect();
    let mut consumers = Vec::new();
    for i in 0..lines.len() {
        if lines[i] == "name" && i + 2 < lines.len() {
            consumers.push(lines[i + 2].to_string());
        }
    }
    consumers.sort();
    let pel: Vec<String> = pending(c, key)
        .into_iter()
        .map(|(id, who, _, n)| format!("{id}:{who}:{n}"))
        .collect();
    format!("groups={groups:?} consumers={consumers:?} pel={pel:?}")
}

fn ok(reply: &str, what: &str) {
    assert!(!reply.starts_with('-'), "{what}: {reply:?}");
}

/// Every row's key, and the setup that ran on it before the kill.
struct Scenario {
    keys: Vec<String>,
}

/// Build every case on the live server. `{t}` co-locates the multi-stream
/// pair on one shard.
fn build(c: &mut Conn) -> Scenario {
    let keys: Vec<String> = [
        "p1130:read",
        "p1130:claim",
        "p1130:claim-refused",
        "p1130:autoclaim",
        "p1130:noack",
        "{p1130m}:a",
        "{p1130m}:b",
        "p1130:history",
        "p1130:multi",
        "p1130:lua",
    ]
    .iter()
    .map(|s| (*s).to_string())
    .collect();
    for k in &keys {
        ok(
            &c.send(&["XGROUP", "CREATE", k, "g", "$", "MKSTREAM"]),
            "XGROUP CREATE",
        );
        ok(&c.send(&["XADD", k, "1-1", "f", "v"]), "XADD 1-1");
        ok(&c.send(&["XADD", k, "1-2", "f", "v"]), "XADD 1-2");
    }
    // (a) a plain read, two entries.
    ok(
        &c.send(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "STREAMS",
            "p1130:read",
            ">",
        ]),
        "plain read",
    );
    // (b) read by c1, then claimed by c2 after it has been idle.
    for k in [
        "p1130:claim",
        "p1130:claim-refused",
        "p1130:autoclaim",
        "p1130:history",
    ] {
        let r = c.send(&["XREADGROUP", "GROUP", "g", "c1", "STREAMS", k, ">"]);
        assert!(r.contains("1-1") && r.contains("1-2"), "{k}: read {r:?}");
    }
    // The same read queued in MULTI, and issued from a script: both
    // executors log their own records.
    let r = c.pipeline(&[
        &["MULTI"],
        &[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "STREAMS",
            "p1130:multi",
            ">",
        ],
        &["EXEC"],
    ]);
    assert!(r.contains("1-2") && !r.contains("-ERR"), "MULTI read {r:?}");
    let r = c.send(&[
        "EVAL",
        "return redis.call('XREADGROUP','GROUP','g','c1','STREAMS',KEYS[1],'>')",
        "1",
        "p1130:lua",
    ]);
    assert!(
        r.contains("1-2") && !r.starts_with('-'),
        "script read {r:?}"
    );
    // The refused claim's consumer exists up front: a claim that takes
    // nothing still creates its consumer, and redis 8.6.1 does not log that
    // either (its AOF has no record for it).
    ok(
        &c.send(&["XGROUP", "CREATECONSUMER", "p1130:claim-refused", "g", "c2"]),
        "CREATECONSUMER",
    );
    // (c) NOACK: no PEL, but the cursor and the consumer.
    ok(
        &c.send(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "cn",
            "NOACK",
            "STREAMS",
            "p1130:noack",
            ">",
        ]),
        "noack read",
    );
    // (d) one read over two streams, COUNT 1 each.
    let r = c.send(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "cm",
        "COUNT",
        "1",
        "STREAMS",
        "{p1130m}:a",
        "{p1130m}:b",
        ">",
        ">",
    ]);
    assert!(
        r.contains("{p1130m}:a") && r.contains("{p1130m}:b"),
        "multi read {r:?}"
    );
    // The autoclaim case also has an entry deleted under its PEL.
    ok(&c.send(&["XDEL", "p1130:autoclaim", "1-2"]), "XDEL");

    std::thread::sleep(Duration::from_millis(1300));

    let r = c.send(&["XCLAIM", "p1130:claim", "g", "c2", "1000", "1-1", "JUSTID"]);
    assert!(r.contains("1-1"), "XCLAIM claimed nothing: {r:?}");
    // Not idle long enough: claims nothing, and must stay that way.
    let r = c.send(&["XCLAIM", "p1130:claim-refused", "g", "c2", "600000", "1-1"]);
    assert_eq!(r, "*0\r\n", "XCLAIM with an unmet min-idle claimed {r:?}");
    let r = c.send(&["XAUTOCLAIM", "p1130:autoclaim", "g", "c3", "1000", "0"]);
    assert!(r.contains("1-1") && r.contains("1-2"), "XAUTOCLAIM {r:?}");
    // History read and an empty `>` read: they change nothing.
    let r = c.send(&[
        "XREADGROUP",
        "GROUP",
        "g",
        "c1",
        "STREAMS",
        "p1130:history",
        "0",
    ]);
    assert!(r.contains("1-1"), "history read {r:?}");
    assert_eq!(
        c.send(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c1",
            "STREAMS",
            "p1130:history",
            ">"
        ]),
        "*-1\r\n"
    );
    Scenario { keys }
}

/// Probe every key: the clock-free state, and the PEL with idle times.
fn snapshot(c: &mut Conn, s: &Scenario) -> Vec<(String, Vec<Pending>)> {
    s.keys
        .iter()
        .map(|k| (group_state(c, k), pending(c, k)))
        .collect()
}

/// Compare the post-replay state with the pre-kill one: identical group
/// state, and every pending entry at least as idle as it was (a replayed
/// read or claim restarts it at zero).
fn diff(
    s: &Scenario,
    was: &[(String, Vec<Pending>)],
    now: &[(String, Vec<Pending>)],
) -> Vec<String> {
    let mut out = Vec::new();
    for ((k, (ws, wp)), (ns, np)) in s.keys.iter().zip(was).zip(now) {
        if ws != ns {
            out.push(format!("{k}: state\n      before={ws}\n      after ={ns}"));
        }
        for ((id, who, idle_before, _), (_, _, idle_after, _)) in wp.iter().zip(np) {
            if idle_after < idle_before {
                out.push(format!(
                    "{k}: {id} ({who}) idle {idle_before}ms before, {idle_after}ms after — \
                     its delivery time was re-stamped by the replay"
                ));
            }
        }
    }
    out
}

fn expect_setup(before: &[(String, Vec<Pending>)], s: &Scenario) {
    let by_key = |k: &str| &before[s.keys.iter().position(|x| x == k).expect("key")];
    // Guard the scenario itself, so a row that set up nothing proves nothing.
    let claim = &by_key("p1130:claim").1;
    assert!(
        claim
            .iter()
            .any(|(id, who, _, _)| id == "1-1" && who == "c2"),
        "setup: 1-1 not owned by c2: {claim:?}"
    );
    let auto = &by_key("p1130:autoclaim").1;
    assert_eq!(auto.len(), 1, "setup: XAUTOCLAIM left {auto:?}");
    assert_eq!(
        (auto[0].1.as_str(), auto[0].3),
        ("c3", 2),
        "setup: {auto:?}"
    );
    let read = &by_key("p1130:read").1;
    assert!(
        read.len() == 2 && read.iter().all(|p| p.2 >= 1000),
        "setup: plain read PEL {read:?}"
    );
}

fn run(shards: usize) {
    let dir = unique_test_dir(&format!("stream_group_plain_1130_s{shards}"));
    let (mut guard, port) = spawn_listening_guarded(|p| spawn(p, &dir, shards, true));
    let mut c = Conn::open(port);
    let s = build(&mut c);
    ok(&c.send(&["SET", "p1130:control", "1"]), "control");
    let before = snapshot(&mut c, &s);
    expect_setup(&before, &s);
    drop(c);
    guard.kill_now();
    common::wait_for_port_down(port);

    let (_g2, port2) = spawn_listening_guarded(|p| spawn(p, &dir, shards, true));
    let mut c = Conn::open(port2);
    assert_eq!(
        c.send(&["GET", "p1130:control"]),
        "$1\r\n1\r\n",
        "the control write did not survive the restart — the AOF itself is broken"
    );
    let after = snapshot(&mut c, &s);
    let mut problems = diff(&s, &before, &after);
    // The user-visible consequence: nothing is handed out a second time.
    for k in &s.keys {
        let again = c.send(&["XREADGROUP", "GROUP", "g", "c9", "STREAMS", k, ">"]);
        let expect_more = k == "{p1130m}:a" || k == "{p1130m}:b";
        if !expect_more && again != "*-1\r\n" {
            problems.push(format!(
                "{k}: a second `>` read after the restart got {again:?}"
            ));
        }
        if expect_more && (again.contains("1-1") || !again.contains("1-2")) {
            problems.push(format!("{k}: the cursor was not restored, got {again:?}"));
        }
    }
    assert!(
        problems.is_empty(),
        "shards={shards}: group state changed across kill -9 + restart:\n  {}",
        problems.join("\n  ")
    );
}

#[test]
fn plain_group_reads_and_claims_survive_kill9_one_shard() {
    run(1);
}

#[test]
fn plain_group_reads_and_claims_survive_kill9_four_shards() {
    run(4);
}

/// The same scenario against a live replica: the records applied there
/// must build the master's group state. A replica applying the VERBATIM
/// commands within ms of the master re-decides them at almost the same
/// instant, so this row passed before the fix too — it guards the rewritten
/// form, not the original bug (a replica catching up later from the backlog
/// diverges exactly as a restart does). `#[ignore]`d like every
/// replication suite (master-side replication is monoio-only):
///
/// ```text
/// MOON_BIN=... cargo test --test stream_group_plain_log_1130 -- --include-ignored
/// ```
#[test]
#[ignore = "replication: monoio master, run with --include-ignored"]
fn a_replica_holds_the_masters_group_state() {
    let mdir = unique_test_dir("stream_group_plain_1130_master");
    let rdir = unique_test_dir("stream_group_plain_1130_replica");
    let (_m, mport) = spawn_listening_guarded(|p| spawn(p, &mdir, 1, false));
    let (_r, rport) = spawn_listening_guarded(|p| spawn(p, &rdir, 1, false));
    let mut r = Conn::open(rport);
    assert!(
        r.send(&["REPLICAOF", "127.0.0.1", &mport.to_string()])
            .starts_with("+OK")
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    while !r
        .send(&["INFO", "replication"])
        .contains("master_link_status:up")
    {
        assert!(Instant::now() < deadline, "replica link never came up");
        std::thread::sleep(Duration::from_millis(100));
    }
    let mut m = Conn::open(mport);
    let s = build(&mut m);
    let on_master = snapshot(&mut m, &s);
    expect_setup(&on_master, &s);
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut problems = diff(&s, &on_master, &snapshot(&mut r, &s));
    while !problems.is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
        problems = diff(&s, &on_master, &snapshot(&mut r, &s));
    }
    assert!(
        problems.is_empty(),
        "the replica does not hold the master's group state:\n  {}",
        problems.join("\n  ")
    );
}
