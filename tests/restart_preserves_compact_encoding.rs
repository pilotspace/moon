//! A restart must not flatten compact encodings.
//!
//! The wire format collapses every encoding of a type into ONE `ValueType`
//! (`Set | SetListpack | SetIntset -> ValueType::Set`), so decode had no tag to
//! tell it which form to rebuild and always produced the full one. Measured on
//! the unmodified binary: after a single restart a hash went
//! `listpack -> hashtable`, a list `listpack -> linkedlist`, and an all-integer
//! set `intset -> hashtable`, while redis 8.6.1 preserves all three across
//! `DEBUG RELOAD`.
//!
//! That made every container memory figure in BENCHMARK.md a fresh-server best
//! case: the SADD-listpack win (978.2 -> 404.5 B/key on Linux) reverts entirely
//! after one restart. This test is the regression gate.
//!
//! It is NOT `#[ignore]`d. Every `--ignored` invocation in `.github/workflows/`
//! names a specific `--test` target, so an ignored test here would never run
//! anywhere -- and a gate that cannot fire is worse than no gate. It spawns
//! one server twice on a reserved port in a unique `--dir`, like the ~60 other
//! un-ignored suites under `tests/` that do the same, so it runs in every leg
//! that runs `cargo nextest run` / `cargo test`: the hosted tokio Check leg,
//! the self-hosted monoio leg, and both VM suites of `scripts/ci-local.sh`.
//!
//! Probe choice (moon#832): `OBJECT ENCODING` reads `entry.value` through
//! `Database::get` / `get_if_alive_any_plane` on both dispatch paths -- neither
//! goes through `get_promoted`, so asking about the encoding cannot itself
//! flatten it. A probe routed through a `get_promoted` accessor (any mutable
//! container read) would upgrade the key on the first call and this test
//! would be measuring #832, not the restart.
//!
//! Run alone:
//!   cargo test --release --test restart_preserves_compact_encoding

mod common;

use std::process::{Child, Command};
use std::time::Duration;

fn start_moon(dir: &std::path::Path) -> (Child, u16) {
    common::spawn_listening(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--dir",
                dir.to_str().expect("utf8 dir"),
                "--appendonly",
                "yes",
                // The diskfull guard trips on a shared volume with little free
                // space and would abort startup before the probe ever runs.
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon")
    })
}

/// `OBJECT ENCODING <key>`, via a fresh connection each time.
fn encoding(port: u16, key: &str) -> String {
    let mut c = common::Conn::open(port);
    let raw = c.send(&["OBJECT", "ENCODING", key]);
    raw.trim_start_matches('$')
        .lines()
        .nth(1)
        .unwrap_or("")
        .trim()
        .to_string()
}

/// A layout-independent fingerprint of the AOF base that a completed rewrite
/// advances.
///
/// The two runtimes write DIFFERENT layouts, and the test runs on both:
/// the monoio writers use the `appendonlydir` manifest layout, while the tokio
/// TopLevel writer appends to one flat `<dir>/appendonly.aof`
/// (`src/persistence/aof/auto_rewrite.rs:59-62` — "they never coexist for one
/// server"). Watching only the manifest made this test hang for its full 30 s
/// timeout on the hosted tokio Check leg, where `appendonlydir` never exists
/// and the seq read is `None` forever.
#[derive(Debug, PartialEq)]
enum Base {
    /// monoio: the manifest `seq`. A completed rewrite publishes a new
    /// `moon.aof.<seq>.base.rdb` and bumps this; nothing else does.
    Manifest(u64),
    /// tokio: (len, mtime) of the flat file. A rewrite replaces it wholesale.
    Flat(u64, Option<std::time::SystemTime>),
    /// Neither layout present yet.
    Absent,
}

fn read_base(dir: &std::path::Path) -> Base {
    let manifest = dir.join("appendonlydir").join("moon.aof.manifest");
    if let Ok(text) = std::fs::read_to_string(&manifest)
        && let Some(seq) = text
            .lines()
            .find_map(|l| l.strip_prefix("seq ")?.trim().parse().ok())
    {
        return Base::Manifest(seq);
    }
    if let Ok(md) = std::fs::metadata(dir.join("appendonly.aof")) {
        return Base::Flat(md.len(), md.modified().ok());
    }
    Base::Absent
}

/// True once `after` shows a rewrite has landed relative to `before`.
fn base_advanced(before: &Base, after: &Base) -> bool {
    match (before, after) {
        (Base::Manifest(b), Base::Manifest(a)) => a > b,
        // The flat file is replaced wholesale by a rewrite: any change to its
        // size or mtime is the completion signal. Compared as a whole so a
        // same-size rewrite is still caught by mtime.
        (Base::Flat(..), Base::Flat(..)) => before != after,
        // Layout appeared during the wait (first rewrite on a fresh dir).
        (Base::Absent, Base::Manifest(_) | Base::Flat(..)) => true,
        _ => false,
    }
}

/// Wait until the rewrite kicked off by `BGREWRITEAOF` has published its new
/// base. `BGREWRITEAOF` acks at enqueue, and `INFO persistence` alone is not a
/// completion signal: `aof_rewrite_in_progress:0` can be observed BEFORE the
/// rewrite starts, and `aof_base_size` does not move when it finishes
/// (measured: 66 before and after a rewrite that wrote a 3 KB base). The
/// manifest `seq` advancing is what the loader itself trusts, so it is what
/// this waits on. Panics with the last observation on timeout.
fn wait_for_rewrite(port: u16, dir: &std::path::Path, before: &Base) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut last = (Base::Absent, String::new());
    while std::time::Instant::now() < deadline {
        let mut c = common::Conn::open(port);
        let info = c.send(&["INFO", "persistence"]);
        let now = read_base(dir);
        // On the manifest layout the published base file must also exist --
        // the seq line lands before the file is fsynced into place.
        let base_ready = match &now {
            Base::Manifest(s) => dir
                .join("appendonlydir")
                .join(format!("moon.aof.{s}.base.rdb"))
                .exists(),
            Base::Flat(..) => true,
            Base::Absent => false,
        };
        if info.contains("aof_rewrite_in_progress:0") && base_advanced(before, &now) && base_ready {
            return;
        }
        last = (now, info);
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "BGREWRITEAOF did not publish a new base within 30s (base before {before:?}, \
         last seen {:?}); last INFO persistence:\n{}",
        last.0, last.1
    );
}

#[test]
fn compact_encodings_survive_a_restart() {
    let dir = common::unique_test_dir("restart-encoding");
    std::fs::create_dir_all(&dir).expect("create dir");

    let (child, port) = start_moon(&dir);
    let mut guard = common::ServerGuard::new(child);

    {
        let mut c = common::Conn::open(port);
        // Every one of these is far below LISTPACK_MAX_ENTRIES (128) and
        // LISTPACK_MAX_ELEMENT_SIZE (64), so all four must be compact.
        c.send(&["HSET", "h", "f1", "v1", "f2", "v2", "f3", "v3"]);
        c.send(&["RPUSH", "l", "a", "b", "c", "d", "e"]);
        c.send(&["SADD", "s", "alpha", "beta", "gamma"]);
        c.send(&["SADD", "si", "1", "2", "3"]);
        // moon#787: ZADD now builds a listpack too, and the restart must keep
        // it -- this key is what lifted the zset exclusion from the fix.
        c.send(&["ZADD", "z", "1", "a", "2.5", "b", "3", "c"]);
        // A hash big enough to stay a hashtable — the negative control. Without
        // it a bug that compacts EVERYTHING would pass this test.
        let mut big: Vec<String> = vec!["HSET".into(), "hbig".into()];
        for i in 0..200 {
            big.push(format!("f{i:04}"));
            big.push("v".into());
        }
        let refs: Vec<&str> = big.iter().map(|s| s.as_str()).collect();
        c.send(&refs);
    }

    // Record the pre-restart encodings. Note `s` (a small *string* set) is a
    // hashtable on main and a listpack on `perf/set-listpack-encoding`; this
    // test must pass on both, so its precondition accepts either. What the
    // restart guarantee actually claims is asserted *after* the restart, in
    // absolute terms -- see `want` below.
    let before = [
        ("h", encoding(port, "h")),
        ("l", encoding(port, "l")),
        ("s", encoding(port, "s")),
        ("si", encoding(port, "si")),
        ("z", encoding(port, "z")),
        ("hbig", encoding(port, "hbig")),
    ];
    assert_eq!(
        before[0].1, "listpack",
        "precondition: small hash is a listpack"
    );
    assert_eq!(
        before[1].1, "listpack",
        "precondition: small list is a listpack"
    );
    assert!(
        before[2].1 == "listpack" || before[2].1 == "hashtable",
        "precondition: small string set is listpack or hashtable, got {}",
        before[2].1
    );
    assert_eq!(
        before[3].1, "intset",
        "precondition: all-integer set is an intset"
    );
    assert_eq!(
        before[4].1, "listpack",
        "precondition: small zset is a listpack"
    );
    assert_eq!(
        before[5].1, "hashtable",
        "precondition: a 200-field hash is a hashtable"
    );

    // Rewrite the AOF so the restart loads an RDB preamble -- the decode path
    // under test -- rather than replaying the command log (which would rebuild
    // every key live and prove nothing about reload). Then take the server
    // down and bring it back on the same --dir: the exact sequence that used
    // to flatten everything.
    let base_before = read_base(&dir);
    {
        let mut c = common::Conn::open(port);
        let reply = c.send(&["BGREWRITEAOF"]);
        assert!(
            reply.contains("rewriting started") || reply.contains("scheduled"),
            "BGREWRITEAOF was refused: {reply:?}"
        );
    }
    wait_for_rewrite(port, &dir, &base_before);
    guard.kill_now();
    common::wait_for_port_down(port);

    let (child2, port2) = start_moon(&dir);
    let mut guard2 = common::ServerGuard::new(child2);

    // The guarantee, stated absolutely: a restart must leave every small
    // container in its compact encoding, and must NOT compact the big one.
    // Asserting the target encoding (rather than before == after) is what makes
    // this meaningful on main, where `s` legitimately *gains* a listpack here.
    let want = [
        ("h", "listpack"),
        ("l", "listpack"),
        ("s", "listpack"),
        ("si", "intset"),
        ("z", "listpack"),
        ("hbig", "hashtable"),
    ];
    let mut failures = Vec::new();
    for ((key, expected), (_, was)) in want.iter().zip(before.iter()) {
        let now = encoding(port2, key);
        if &now != expected {
            failures.push(format!(
                "{key}: was {was}, after restart {now}, want {expected}"
            ));
        }
    }
    // The VALUE behind the compact zset, checked LAST: `ZSCORE` may take the
    // promoting mutable path (moon#832) and must not flatten `z` before its
    // encoding was recorded above. `2.5` is the non-integral score, so a
    // re-derivation that truncated or mis-parsed it would show here.
    let score = {
        let mut c = common::Conn::open(port2);
        c.send(&["ZSCORE", "z", "b"])
    };
    guard2.kill_now();
    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        score.contains("2.5"),
        "ZSCORE z b after restart must be 2.5, got {score:?}"
    );

    assert!(
        failures.is_empty(),
        "restart did not leave these in their compact encoding (redis preserves \
         all of them across DEBUG RELOAD): {}",
        failures.join(", ")
    );
    let _ = port2;
}
