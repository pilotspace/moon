//! moon#942 — `INCR`/`INCRBY`/`DECR`/`DECRBY` mutate the stored integer in
//! place. Client-visible behaviour must not move by one bit.
//!
//! The unit suite in `src/storage/db/incr.rs` pins the storage-level side
//! effects against a verbatim copy of the pre-#942 `get` + `set` pair. This
//! suite pins the things that copy cannot see, because they are properties of
//! a live server:
//!
//!   * `rdb_changes_since_last_save` — a PROCESS-global counter, unassertable
//!     from a unit test whose binary runs other writes concurrently;
//!   * the `incrby` keyspace notification, which needs a real subscriber;
//!   * `WATCH`/`EXEC`, which needs two real connections;
//!   * TTL survival, read back through `TTL`;
//!   * every encoding-width transition read back through `GET`, including the
//!     SSO seam at 12/13 digits where the in-place write swaps an inline
//!     payload for a `Box<[u8]>` and back;
//!   * `INCR` on a key that has been spilled to the cold tier.
//!
//! Run with (monoio default — the runtime that ships):
//!   cargo build --release
//!   MOON_BIN=$PWD/target/release/moon cargo test --release \
//!     --test incr_in_place_942

#![allow(clippy::unwrap_used)]

mod common;

use std::process::Command;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, find_moon_binary, spawn_listening_guarded};

// ---------------------------------------------------------------------------
// Servers
// ---------------------------------------------------------------------------

fn tmpdir(prefix: &str) -> tempfile::TempDir {
    let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/incr-942-tmp");
    std::fs::create_dir_all(&base).expect("create incr-942-tmp base dir");
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(&base)
        .expect("tempdir_in target/incr-942-tmp")
}

/// A plain single-shard server, no persistence. `--shards 1` because every
/// probe here is about one key's own bookkeeping, and a cross-shard hop adds
/// nothing but a race.
fn plain_server(dir: &std::path::Path, extra: &[&str]) -> (ServerGuard, u16) {
    spawn_listening_guarded(|port| {
        let mut args = vec![
            "--port".to_string(),
            port.to_string(),
            "--dir".to_string(),
            dir.to_string_lossy().into_owned(),
            "--shards".to_string(),
            "1".to_string(),
            "--appendonly".to_string(),
            "no".to_string(),
            // The shared /Volumes checkout hovers near the 5% diskfull guard;
            // a tripped guard turns every write into MOONERR and would fail
            // this suite for an unrelated reason.
            "--disk-free-min-pct".to_string(),
            "0".to_string(),
        ];
        args.extend(extra.iter().map(|s| (*s).to_string()));
        Command::new(find_moon_binary())
            .args(&args)
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
            .spawn()
            .expect("spawn moon")
    })
}

// ---------------------------------------------------------------------------
// Reply helpers — raw RESP on purpose. The abort signal IS the type byte.
// ---------------------------------------------------------------------------

fn int(reply: &str) -> i64 {
    let body = reply
        .strip_prefix(':')
        .unwrap_or_else(|| panic!("expected an integer reply, got {reply:?}"));
    body.trim_end_matches("\r\n")
        .parse()
        .unwrap_or_else(|_| panic!("unparsable integer reply {reply:?}"))
}

fn bulk(reply: &str) -> Option<String> {
    if reply.starts_with("$-1") || reply.starts_with("_\r\n") {
        return None;
    }
    let body = reply
        .strip_prefix('$')
        .unwrap_or_else(|| panic!("expected a bulk reply, got {reply:?}"));
    let mut parts = body.splitn(2, "\r\n");
    let _len = parts.next();
    Some(
        parts
            .next()
            .unwrap_or_else(|| panic!("truncated bulk reply {reply:?}"))
            .trim_end_matches("\r\n")
            .to_string(),
    )
}

fn is_err(reply: &str) -> bool {
    reply.starts_with('-')
}

/// One `key:value` line out of an `INFO` body.
fn info_field(body: &str, field: &str) -> Option<i64> {
    body.lines().find_map(|l| {
        l.strip_prefix(field)?
            .strip_prefix(':')?
            .trim()
            .parse()
            .ok()
    })
}

// ---------------------------------------------------------------------------
// 1 · the dirty counter (the side effect a unit test cannot own)
// ---------------------------------------------------------------------------

/// `Database::set` charges one `record_keyspace_change` per write. The
/// in-place path has to charge exactly the same, or `BGSAVE`'s
/// `save <sec> <changes>` triggers stop firing for counter workloads — a
/// durability regression with no error message anywhere.
#[test]
fn rdb1_a_successful_incr_is_one_dirty_change() {
    let dir = tmpdir("rdb1-");
    let (_g, port) = plain_server(dir.path(), &[]);
    let mut c = Conn::open(port);

    c.send(&["SET", "ctr", "1"]);
    let before = dirty(&mut c);
    for _ in 0..50 {
        c.send(&["INCR", "ctr"]);
    }
    let after = dirty(&mut c);
    assert_eq!(
        after - before,
        50,
        "50 INCRs must move rdb_changes_since_last_save by exactly 50"
    );
}

/// The mirror: an INCR that errors wrote nothing, so it is not a change.
#[test]
fn rdb2_a_failing_incr_is_no_dirty_change() {
    let dir = tmpdir("rdb2-");
    let (_g, port) = plain_server(dir.path(), &[]);
    let mut c = Conn::open(port);

    c.send(&["SET", "word", "abc"]);
    c.send(&["RPUSH", "lst", "a"]);
    c.send(&["SET", "big", "9223372036854775807"]);
    let before = dirty(&mut c);
    for _ in 0..10 {
        assert!(is_err(&c.send(&["INCR", "word"])));
        assert!(is_err(&c.send(&["INCR", "lst"])));
        assert!(is_err(&c.send(&["INCR", "big"])));
    }
    assert_eq!(
        dirty(&mut c) - before,
        0,
        "an error reply wrote nothing and must not count as a keyspace change"
    );
}

fn dirty(c: &mut Conn) -> i64 {
    let body = c.send(&["INFO", "persistence"]);
    info_field(&body, "rdb_changes_since_last_save")
        .unwrap_or_else(|| panic!("INFO persistence has no rdb_changes_since_last_save:\n{body}"))
}

// ---------------------------------------------------------------------------
// 2 · the keyspace notification
// ---------------------------------------------------------------------------

/// INCR is the only one of moon's five hot write families that notifies at
/// all (the others are #411). Losing it to a fast path would be a silent
/// regression for every client-side cache keyed on `__keyspace@0__`.
#[test]
fn ks1_incr_still_emits_the_incrby_event() {
    let dir = tmpdir("ks1-");
    let (_g, port) = plain_server(dir.path(), &[]);
    let mut writer = Conn::open(port);
    let mut sub = Conn::open(port);

    // Seed BEFORE enabling notifications: fanout is asynchronous, so a seed
    // written with notifications already on can land on the subscriber after
    // it registers and be mistaken for the event under test.
    writer.send(&["SET", "ctr", "1"]);
    // There is no CLI flag for this; the knob is CONFIG-only.
    assert!(
        writer
            .send(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
            .starts_with("+OK")
    );
    sub.send(&["SUBSCRIBE", "__keyspace@0__:ctr"]);

    for (label, cmd) in [
        ("INCR", vec!["INCR", "ctr"]),
        ("INCRBY", vec!["INCRBY", "ctr", "7"]),
        ("DECR", vec!["DECR", "ctr"]),
        ("DECRBY", vec!["DECRBY", "ctr", "3"]),
    ] {
        writer.send(&cmd);
        let msg = sub.read_replies(1);
        assert!(
            msg.contains("incrby"),
            "{label}: expected an `incrby` keyspace event, got {msg:?}"
        );
    }
}

/// And the mirror: a failed INCR wrote nothing, so it must say nothing.
#[test]
fn ks2_a_failing_incr_emits_nothing() {
    let dir = tmpdir("ks2-");
    let (_g, port) = plain_server(dir.path(), &[]);
    let mut writer = Conn::open(port);
    let mut sub = Conn::open(port);

    writer.send(&["SET", "word", "abc"]);
    assert!(
        writer
            .send(&["CONFIG", "SET", "notify-keyspace-events", "KEA"])
            .starts_with("+OK")
    );
    sub.send(&["SUBSCRIBE", "__keyspace@0__:word"]);
    assert!(is_err(&writer.send(&["INCR", "word"])));

    // Now provoke a real event on the same channel. If the failed INCR had
    // emitted, THIS read would return the stale `incrby` instead of `set`.
    writer.send(&["SET", "word", "1"]);
    let msg = sub.read_replies(1);
    assert!(
        msg.contains("set") && !msg.contains("incrby"),
        "the next event on the channel must be the SET, not an `incrby` from \
         the INCR that errored: {msg:?}"
    );
}

// ---------------------------------------------------------------------------
// 3 · WATCH — moon#926 in both directions
// ---------------------------------------------------------------------------

/// A successful INCR by another client must abort a watching transaction.
#[test]
fn w1_incr_aborts_a_live_watch() {
    let dir = tmpdir("w1-");
    let (_g, port) = plain_server(dir.path(), &[]);

    for cmd in [
        vec!["INCR", "ctr"],
        vec!["INCRBY", "ctr", "5"],
        vec!["DECR", "ctr"],
        vec!["DECRBY", "ctr", "5"],
    ] {
        let (mut a, mut b) = (Conn::open(port), Conn::open(port));
        a.send(&["SET", "ctr", "10"]);
        assert!(a.send(&["WATCH", "ctr"]).starts_with("+OK"));
        assert!(!is_err(&b.send(&cmd)), "{cmd:?} must succeed");
        a.send(&["MULTI"]);
        a.send(&["GET", "ctr"]);
        let exec = a.send(&["EXEC"]);
        assert!(
            exec.starts_with("*-1") || exec.starts_with("_\r\n"),
            "{cmd:?}: EXEC must abort — a watched counter was modified: {exec:?}"
        );
    }
}

/// moon#940's direction: an INCR that ERRORS wrote nothing, so a watching
/// transaction must still commit. A bump placed on the mutable-handle
/// acquisition rather than on the successful write fails exactly here.
#[test]
fn w2_a_failing_incr_does_not_abort_a_watch() {
    let dir = tmpdir("w2-");
    let (_g, port) = plain_server(dir.path(), &[]);

    for (label, seed, cmd) in [
        (
            "non-integer",
            vec!["SET", "ctr", "abc"],
            vec!["INCR", "ctr"],
        ),
        (
            "overflow",
            vec!["SET", "ctr", "9223372036854775807"],
            vec!["INCR", "ctr"],
        ),
        (
            "underflow",
            vec!["SET", "ctr", "-9223372036854775808"],
            vec!["DECR", "ctr"],
        ),
        ("wrongtype", vec!["RPUSH", "ctr", "x"], vec!["INCR", "ctr"]),
    ] {
        let (mut a, mut b) = (Conn::open(port), Conn::open(port));
        a.send(&["DEL", "ctr"]);
        a.send(&seed);
        assert!(a.send(&["WATCH", "ctr"]).starts_with("+OK"));
        assert!(
            is_err(&b.send(&cmd)),
            "{label}: the probe command must error"
        );
        a.send(&["MULTI"]);
        a.send(&["PING"]);
        let exec = a.send(&["EXEC"]);
        assert!(
            exec.starts_with('*') && !exec.starts_with("*-1"),
            "{label}: EXEC must COMMIT — the INCR errored and wrote nothing, \
             so nothing invalidated the watch (moon#940): {exec:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// 4 · TTL survival
// ---------------------------------------------------------------------------

/// Redis's INCR does not touch the key's TTL. Neither may an in-place write:
/// the entry it mutates carries the deadline, and a rebuild that forgot to
/// copy it forward would make counters immortal.
#[test]
fn ttl1_incr_preserves_an_existing_expiry() {
    let dir = tmpdir("ttl1-");
    let (_g, port) = plain_server(dir.path(), &[]);
    let mut c = Conn::open(port);

    c.send(&["SET", "ctr", "1", "EX", "1000"]);
    let before = int(&c.send(&["TTL", "ctr"]));
    assert!(before > 900, "seed TTL did not stick: {before}");

    for _ in 0..20 {
        c.send(&["INCR", "ctr"]);
    }
    let after = int(&c.send(&["TTL", "ctr"]));
    assert!(
        after > 900 && after <= before,
        "INCR must leave the TTL running, got {after} after {before}"
    );
    assert_eq!(bulk(&c.send(&["GET", "ctr"])).as_deref(), Some("21"));

    // And the other direction: a key with NO TTL must not acquire one.
    c.send(&["SET", "plain", "1"]);
    c.send(&["INCR", "plain"]);
    assert_eq!(
        int(&c.send(&["TTL", "plain"])),
        -1,
        "INCR must not invent a TTL"
    );
}

// ---------------------------------------------------------------------------
// 5 · encoding-width transitions, end to end
// ---------------------------------------------------------------------------

/// Every carry boundary, and both crossings of the 12-byte SSO seam where the
/// stored payload swaps between inline bytes and a `Box<[u8]>`. An in-place
/// write that got the seam wrong would leak, double-free, or serve the old
/// digits — all three are visible here as a wrong `GET`.
#[test]
fn e1_every_width_transition_reads_back_correctly() {
    let dir = tmpdir("e1-");
    let (_g, port) = plain_server(dir.path(), &[]);
    let mut c = Conn::open(port);

    for (start, delta, want) in [
        ("9", 1i64, "10"),
        ("10", -1, "9"),
        ("99", 1, "100"),
        ("100", -1, "99"),
        ("0", -1, "-1"),
        ("-1", 1, "0"),
        ("-9", -1, "-10"),
        ("2147483647", 1, "2147483648"),
        ("-2147483648", -1, "-2147483649"),
        // 12 digits inline -> 13 digits on the heap, and back.
        ("999999999999", 1, "1000000000000"),
        ("1000000000000", -1, "999999999999"),
        ("1000000000000", 1, "1000000000001"),
        ("-99999999999", -1, "-100000000000"),
        ("9223372036854775806", 1, "9223372036854775807"),
        ("-9223372036854775807", -1, "-9223372036854775808"),
    ] {
        c.send(&["SET", "ctr", start]);
        let r = c.send(&["INCRBY", "ctr", &delta.to_string()]);
        assert_eq!(
            int(&r).to_string(),
            want,
            "INCRBY {start} {delta} replied wrong"
        );
        assert_eq!(
            bulk(&c.send(&["GET", "ctr"])).as_deref(),
            Some(want),
            "INCRBY {start} {delta}: the STORED value is wrong"
        );
        assert_eq!(
            bulk(&c.send(&["OBJECT", "ENCODING", "ctr"])).as_deref(),
            Some("int"),
            "INCRBY {start} {delta}: the encoding must still read as `int`"
        );
    }
}

/// Overflow in both directions must be an error AND must leave the counter
/// exactly where it was. Redis does not clamp; neither does moon.
#[test]
fn e2_overflow_errors_and_leaves_the_value_untouched() {
    let dir = tmpdir("e2-");
    let (_g, port) = plain_server(dir.path(), &[]);
    let mut c = Conn::open(port);

    for (seed, cmd) in [
        ("9223372036854775807", vec!["INCR", "ctr"]),
        ("9223372036854775807", vec!["INCRBY", "ctr", "1"]),
        (
            "9223372036854775000",
            vec!["INCRBY", "ctr", "9223372036854775807"],
        ),
        ("-9223372036854775808", vec!["DECR", "ctr"]),
        ("-9223372036854775808", vec!["DECRBY", "ctr", "1"]),
    ] {
        c.send(&["SET", "ctr", seed]);
        assert!(
            is_err(&c.send(&cmd)),
            "{cmd:?} on {seed} must be an overflow error"
        );
        assert_eq!(
            bulk(&c.send(&["GET", "ctr"])).as_deref(),
            Some(seed),
            "{cmd:?} on {seed}: an overflow must not change the stored value"
        );
    }

    // A non-integer string, same contract.
    c.send(&["SET", "ctr", "hello"]);
    assert!(is_err(&c.send(&["INCR", "ctr"])));
    assert_eq!(bulk(&c.send(&["GET", "ctr"])).as_deref(), Some("hello"));

    // WRONGTYPE, same contract.
    c.send(&["DEL", "ctr"]);
    c.send(&["RPUSH", "ctr", "a", "b"]);
    let r = c.send(&["INCR", "ctr"]);
    assert!(
        r.starts_with("-WRONGTYPE"),
        "INCR on a list must be WRONGTYPE, got {r:?}"
    );
    assert_eq!(int(&c.send(&["LLEN", "ctr"])), 2, "the list must be intact");
}

/// Creation still works: INCR on an absent key is 1, and the key comes into
/// existence with no TTL.
#[test]
fn e3_incr_on_an_absent_key_creates_it() {
    let dir = tmpdir("e3-");
    let (_g, port) = plain_server(dir.path(), &[]);
    let mut c = Conn::open(port);

    assert_eq!(int(&c.send(&["INCR", "fresh"])), 1);
    assert_eq!(int(&c.send(&["TTL", "fresh"])), -1);
    assert_eq!(int(&c.send(&["INCRBY", "fresh2", "-7"])), -7);
    assert_eq!(bulk(&c.send(&["GET", "fresh2"])).as_deref(), Some("-7"));
    assert_eq!(int(&c.send(&["DECRBY", "fresh3", "5"])), -5);
}

/// An INCR on a key whose TTL has already passed starts from zero — the
/// expired value is gone, and its stale deadline must not come with it.
#[test]
fn e4_incr_on_an_expired_key_starts_from_zero() {
    let dir = tmpdir("e4-");
    let (_g, port) = plain_server(dir.path(), &[]);
    let mut c = Conn::open(port);

    c.send(&["SET", "ctr", "41", "PX", "60"]);
    std::thread::sleep(Duration::from_millis(250));
    assert_eq!(
        int(&c.send(&["INCR", "ctr"])),
        1,
        "the old value expired; INCR starts a new counter at 1"
    );
    assert_eq!(
        int(&c.send(&["TTL", "ctr"])),
        -1,
        "the expired key's deadline must not be inherited"
    );
}

// ---------------------------------------------------------------------------
// 6 · the cold tier
// ---------------------------------------------------------------------------

const COLD_COUNTERS: usize = 200;
const COLD_FILLERS: usize = 400;
const COLD_FILLER_BYTES: usize = 4096;
const COLD_MAXMEMORY: u64 = 512 * 1024;

/// A counter that eviction has spilled to disk must still INCREMENT, not
/// restart from zero. The fast path has to decline for it — `data.get_mut`
/// misses, and only the general path knows how to promote.
///
/// Every counter is checked, and the run asserts the tier actually engaged
/// (`cold_keys` past a floor) so it cannot pass vacuously on a server where
/// nothing was ever spilled.
#[test]
fn cold1_incr_promotes_a_spilled_counter_instead_of_resetting_it() {
    let dir = tmpdir("cold1-");
    let (_g, port) = spawn_listening_guarded(|port| {
        Command::new(find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.path().to_string_lossy(),
                "--shards",
                "1",
                // The async-spill path is inert without a ShardManifest,
                // which needs the AOF.
                "--appendonly",
                "yes",
                "--disk-offload",
                "enable",
                "--maxmemory",
                &COLD_MAXMEMORY.to_string(),
                // Spill e2e needs an EVICTING policy; `noeviction` OOMs
                // instead of spilling.
                "--maxmemory-policy",
                "allkeys-lru",
                "--maxmemory-samples",
                "200",
                // `cold_keys` is published by the orphan sweep, not the 100ms
                // tick — at the 60s default the field is simply ABSENT for the
                // life of this test and the vacuity guard below can never see
                // the tier engage.
                "--cold-orphan-sweep-interval-secs",
                "1",
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::fs::File::create(dir.path().join("moon.stdout.log")).expect("stdout"))
            .stderr(std::fs::File::create(dir.path().join("moon.stderr.log")).expect("stderr"))
            .spawn()
            .expect("spawn moon")
    });

    let mut c = Conn::open(port);
    // The counters go in FIRST, so allkeys-lru sees them as the coldest.
    for i in 0..COLD_COUNTERS {
        c.send(&["SET", &format!("ctr:{i}"), "41"]);
    }
    let filler = "x".repeat(COLD_FILLER_BYTES);
    for i in 0..COLD_FILLERS {
        c.send(&["SET", &format!("fill:{i}"), &filler]);
    }

    // Wait for the spill queue + manifest ticks to drain.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut cold;
    loop {
        let body = c.send(&["INFO", "moonstore"]);
        cold = info_field(&body, "cold_keys").unwrap_or(0);
        if cold >= COLD_COUNTERS as i64 || Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(
        cold >= COLD_COUNTERS as i64,
        "the cold tier never engaged (cold_keys={cold}) — this test would \
         pass vacuously against a purely hot keyspace"
    );

    // Every counter, cold or not, must read 42 after one INCR.
    for i in 0..COLD_COUNTERS {
        let key = format!("ctr:{i}");
        let got = int(&c.send(&["INCR", &key]));
        assert_eq!(
            got, 42,
            "{key}: a spilled counter must be PROMOTED and incremented, not \
             fabricated at 1 (cold_keys was {cold})"
        );
    }
}
