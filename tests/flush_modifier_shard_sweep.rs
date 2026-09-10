//! `FLUSHALL ASYNC` must empty the whole keyspace, not one shard. (moon#925)
//!
//! `extract_primary_key` decides routing. Its keyless fast-path table had no
//! `f` arm, so `FLUSHALL` and `FLUSHDB` fell through to "the routing key is
//! `args[0]`". The BARE forms were correct only by accident of arity — an
//! earlier `args.is_empty()` guard returned `None` before the tail ran. Give
//! either command its optional modifier and `args[0]` is the literal `ASYNC`
//! or `SYNC`, which was hashed as if it were a key: `is_local` went false and
//! `coordinate_flush_broadcast` — which sits INSIDE the `is_local` block —
//! never ran. The client got `+OK` for a keyspace that was still mostly full,
//! and the survivors were live values, not tombstones.
//!
//! Measured against the unfixed build, 60 keys, one pinned connection:
//!
//! ```text
//! shards  FLUSHALL  FLUSHALL ASYNC  FLUSHALL SYNC   (same for FLUSHDB)
//!      1     0/60            0/60           0/60
//!      2     0/60           30/60           0/60
//!      3     0/60           34/60          39/60
//!      4     0/60            0/60          43/60
//!      5     0/60           44/60           0/60
//!      8     0/60            0/60          53/60
//! ```
//!
//! Two things that table forces on this suite:
//!
//! * **A fixed shard count is not discriminating.** `ASYNC` passes at 4 and 8
//!   and `SYNC` passes at 2 and 5, because `key_to_shard(<modifier>)` happens
//!   to land on the pinned connection's own shard. The issue reports a first
//!   probe reading 12/12 green at `--shards 4`. Only the sweep discriminates.
//! * **The bare form is the in-run control.** It is asserted in the same run,
//!   on the same server, through the same connection. A harness that wrote
//!   nothing — or one whose `DBSIZE` reads the wrong thing — fails on the
//!   control instead of reporting a green sweep over an empty keyspace.
//!
//! `SYNC` is covered because it passes today for the same accidental reason
//! and is exactly as unsafe.

mod common;

use common::{Conn, find_moon_binary, spawn_listening_guarded, unique_test_dir};

use std::process::{Child, Command, Stdio};

/// Every shard count that separates a real fix from a lucky hash. 1 proves the
/// defect is not merely a routing artefact (it must stay green); 2/4 and 3/5/8
/// split the two modifiers' accidental passes between them.
const SHARD_SWEEP: &[usize] = &[1, 2, 3, 4, 5, 8];

/// Spread over three prefixes so no shard count can co-locate the whole set on
/// the connection's own shard and pass vacuously.
const KEY_PREFIXES: &[&str] = &["a", "m", "z"];
const KEYS_PER_PREFIX: usize = 20;
const TOTAL_KEYS: usize = KEY_PREFIXES.len() * KEYS_PER_PREFIX;

/// `(argv, label)`. The bare forms lead so a broken harness fails on the
/// control before it can report anything about the modifiers.
const CASES: &[&[&str]] = &[
    &["FLUSHALL"],
    &["FLUSHALL", "ASYNC"],
    &["FLUSHALL", "SYNC"],
    &["FLUSHDB"],
    &["FLUSHDB", "ASYNC"],
    &["FLUSHDB", "SYNC"],
];

fn spawn(port: u16, dir: &std::path::Path, shards: usize) -> Child {
    Command::new(find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--dir",
            &dir.to_string_lossy(),
            "--shards",
            &shards.to_string(),
            // No WAL/AOF: this is a routing test, and a durability stall would
            // only add flake.
            "--appendonly",
            "no",
            // Below the ~5%-free threshold every write answers `MOONERR
            // diskfull` and the assertions never run. A guard that stops
            // running on a full disk is not a guard.
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn moon")
}

/// `:<n>\r\n` -> n. Panics on anything else — a `DBSIZE` that answered an
/// error or a null is the one reading that must never be silently counted as
/// "zero keys left".
fn dbsize(c: &mut Conn) -> usize {
    let reply = c.send(&["DBSIZE"]);
    let n = reply
        .strip_prefix(':')
        .and_then(|r| r.strip_suffix("\r\n"))
        .and_then(|r| r.parse::<usize>().ok());
    match n {
        Some(n) => n,
        None => panic!("DBSIZE answered {reply:?}, not an integer"),
    }
}

fn populate(c: &mut Conn) {
    for p in KEY_PREFIXES {
        for i in 0..KEYS_PER_PREFIX {
            c.send(&["SET", &format!("{p}:{i}"), "v"]);
        }
    }
}

/// The first key that outlived the flush, with its value — the issue's point
/// that survivors are readable, not tombstones.
fn first_survivor(c: &mut Conn) -> Option<String> {
    for p in KEY_PREFIXES {
        for i in 0..KEYS_PER_PREFIX {
            let key = format!("{p}:{i}");
            let reply = c.send(&["GET", &key]);
            if reply != "$-1\r\n" && reply != "_\r\n" {
                return Some(format!("{key} -> {reply:?}"));
            }
        }
    }
    None
}

#[test]
fn flush_with_a_modifier_empties_every_shard_at_every_shard_count() {
    let mut failures: Vec<String> = Vec::new();

    for &shards in SHARD_SWEEP {
        let dir = unique_test_dir(&format!("flush925_s{shards}"));
        let (_guard, port) = spawn_listening_guarded(|p| spawn(p, &dir, shards));
        let mut c = Conn::open(port);

        for case in CASES {
            let label = format!("shards={shards} `{}`", case.join(" "));

            populate(&mut c);
            let before = dbsize(&mut c);
            assert_eq!(
                before, TOTAL_KEYS,
                "{label}: setup wrote {before}/{TOTAL_KEYS} keys — a sweep over \
                 a keyspace that was never filled proves nothing"
            );

            let ack = c.send(case);
            assert_eq!(
                ack, "+OK\r\n",
                "{label}: the flush itself was refused ({ack:?})"
            );

            let after = dbsize(&mut c);
            if after != 0 {
                let survivor = first_survivor(&mut c)
                    .unwrap_or_else(|| "<DBSIZE > 0 but no key readable>".into());
                failures.push(format!(
                    "{label}: {after}/{TOTAL_KEYS} keys survived a flush that \
                     answered +OK; e.g. {survivor}"
                ));
                // Clear by hand so the next case starts from a known state
                // rather than inheriting this one's survivors.
                for p in KEY_PREFIXES {
                    for i in 0..KEYS_PER_PREFIX {
                        c.send(&["DEL", &format!("{p}:{i}")]);
                    }
                }
                let cleaned = dbsize(&mut c);
                assert_eq!(
                    cleaned, 0,
                    "{label}: could not clean up between cases ({cleaned} left)"
                );
            }
        }
    }

    assert!(
        failures.is_empty(),
        "flush left keys alive on {} of {} (shard count x case) rows:\n  {}",
        failures.len(),
        SHARD_SWEEP.len() * CASES.len(),
        failures.join("\n  ")
    );
}
