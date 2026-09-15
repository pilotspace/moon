//! The cross-shard aggregation gate must claim exactly the same commands at
//! `--shards 4` as it did before it was gated on `num_shards > 1`, and the
//! `--shards 1` leg must be indistinguishable from it in every observable way.
//!
//! Context: `try_handle_cross_shard_commands` is an `async fn` that was awaited
//! on EVERY command before discovering `num_shards <= 1` inside itself. Hoisting
//! that test to the call site is meant to be a pure no-op at one shard and no
//! change at all above one — but the gate sits directly on the seam that
//! produced moon#507, moon#513, moon#592 and moon#937, and the obvious wrong
//! predicate (`skip_name_gates`) would silently drop `MGET`/`MSET`/`UNLINK`/
//! `EXISTS`, all four of which carry `NO_INTERCEPT` *and* are claimed by the
//! multi-key arm.
//!
//! Every assertion therefore runs at BOTH shard counts with the same expected
//! values. moon#595 and moon#937 were both invisible because behaviour depended
//! on which shard owned the key; a one-shard-only suite cannot see this class.
//!
//! ## Why these assertions can fail
//!
//! The keys are chosen with moon's own `key_to_shard` so that a 4-shard run
//! provably spans shards. If the multi-key arm stopped claiming `MSET`, the
//! command would fall through to ordinary routing, which hashes its FIRST key
//! and executes the WHOLE command against that one shard's slice — the other
//! keys would then be unreadable from their owners. If the keyless arm stopped
//! claiming `DBSIZE`/`KEYS`, each would answer for one shard out of four.
//! Both are checked directly, and both were confirmed red by mutating the gate
//! to `ctx.num_shards > 8` (see the commit message).
//!
//! ## Which rows actually exercise the gate — measured, not assumed
//!
//! Mutating the gate to `> 8` and re-running showed that `MGET` and `EXISTS`
//! stay GREEN. They are spanning READS, so the moon#513 A2a fanout planner —
//! which sits ABOVE this gate in the loop and is untouched by it — splits them
//! per owner before the gate is reached. The rows that go red are `MSET`,
//! `UNLINK`/`DEL` (writes; `multikey_placement` deliberately does not split
//! those, so they reach `coordinate_multi_key`) and the keyless aggregators
//! `DBSIZE`/`KEYS`/`SCAN`/`RANDOMKEY`. The read rows are kept anyway: they are
//! the regression net for the planner above the gate, and a change that moved
//! the gate ABOVE the planner would be caught by them and by nothing else.

mod common;

use std::process::{Child, Command, Stdio};

use common::Conn;
use moon::shard::dispatch::key_to_shard;

/// Enough shards that the spanning fixture below is genuinely spanning, and
/// the same count `pipeline_cross_shard_ordering.rs` uses.
const SHARDS: u32 = 4;

struct Moon {
    child: Child,
    port: u16,
    dir: std::path::PathBuf,
}

impl Drop for Moon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn spawn_moon(shards: u32) -> Moon {
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        // A FRESH per-port directory. An empty `--dir` means CWD, which reloads
        // whatever a previous run left behind.
        let dir = std::env::temp_dir().join(format!("moon-xshardgate-{port}"));
        let _ = std::fs::create_dir_all(&dir);
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &shards.to_string(),
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                // The disk-free guard aborts startup on a nearly-full volume,
                // which is how this class of suite silently stops running.
                "--disk-free-min-pct",
                "0",
                "--dir",
                dir.to_str().expect("utf-8 temp dir"),
            ])
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(dir.join("moon.stderr")).expect("stderr log"))
            .spawn()
            .expect("spawn moon")
    });
    Moon {
        child,
        port,
        dir: std::env::temp_dir().join(format!("moon-xshardgate-{port}")),
    }
}

/// `n` keys that land on `n` DISTINCT shards at `SHARDS`, so a 4-shard run
/// cannot pass by accidentally co-locating them. At one shard they all land on
/// shard 0 by definition — which is the point: the SAME key set is used for
/// both legs so the two are comparable.
fn spanning_keys(n: usize) -> Vec<String> {
    let mut chosen: Vec<String> = Vec::with_capacity(n);
    let mut seen: Vec<usize> = Vec::with_capacity(n);
    for i in 0..100_000u32 {
        let k = format!("xsg:{i}");
        let s = key_to_shard(k.as_bytes(), SHARDS as usize);
        if seen.contains(&s) {
            continue;
        }
        seen.push(s);
        chosen.push(k);
        if chosen.len() == n {
            return chosen;
        }
    }
    panic!("could not find {n} keys on distinct shards out of {SHARDS}");
}

fn count_occurrences(haystack: &str, needle: &str) -> usize {
    haystack.matches(needle).count()
}

/// Drive every command the cross-shard gate claims, at one shard count.
///
/// Returns nothing: each assertion carries `shards` in its message, so a
/// failure names the leg. A helper shared by both legs is deliberate — a
/// divergence between the legs is exactly what a copy-pasted pair hides.
fn exercise(shards: u32) {
    let moon = spawn_moon(shards);
    let mut c = Conn::open(moon.port);
    // ONE connection for the whole leg. A fresh connection per probe would
    // spread the work over several shard threads and hide a shard-local
    // divergence, which is the inverse of the trap this suite exists for.

    let keys = spanning_keys(SHARDS as usize);
    let k: Vec<&str> = keys.iter().map(String::as_str).collect();
    assert_eq!(k.len(), 4, "fixture must be 4 keys");

    // Clean slate for every db this test observes.
    assert!(
        c.send(&["FLUSHALL"]).contains("+OK"),
        "FLUSHALL at s{shards}"
    );

    // --- MSET: the multi-key WRITE arm ----------------------------------
    let reply = c.send(&["MSET", k[0], "v0", k[1], "v1", k[2], "v2", k[3], "v3"]);
    assert!(reply.contains("+OK"), "MSET at s{shards}: {reply:?}");

    // Every key must be readable by its OWN single-key route. If MSET stopped
    // reaching the multi-key arm it would route by k[0] and write all four
    // values into that shard's slice; the other three GETs then return nil.
    for (i, key) in k.iter().enumerate() {
        let want = format!("v{i}");
        let got = c.send(&["GET", key]);
        assert!(
            got.contains(&want),
            "GET {key} at s{shards} must see the value MSET wrote \
             (want {want:?}, got {got:?}) — MSET was routed by its first key \
             instead of reaching the multi-key arm"
        );
    }

    // --- MGET: the multi-key READ arm -----------------------------------
    let got = c.send(&["MGET", k[0], k[1], k[2], k[3]]);
    for i in 0..4 {
        let want = format!("v{i}");
        assert!(
            got.contains(&want),
            "MGET at s{shards} lost {want:?}: {got:?}"
        );
    }

    // --- EXISTS: counts across shards -----------------------------------
    let got = c.send(&["EXISTS", k[0], k[1], k[2], k[3]]);
    assert!(
        got.starts_with(":4\r\n"),
        "EXISTS over four live keys at s{shards} must be 4, got {got:?}"
    );

    // --- keyless aggregation: DBSIZE / KEYS / SCAN / RANDOMKEY ----------
    let got = c.send(&["DBSIZE"]);
    assert!(
        got.starts_with(":4\r\n"),
        "DBSIZE at s{shards} must sum every shard, got {got:?}"
    );

    let got = c.send(&["KEYS", "xsg:*"]);
    assert!(
        got.starts_with("*4\r\n"),
        "KEYS at s{shards} must aggregate every shard, got {got:?}"
    );
    for key in &k {
        assert!(
            count_occurrences(&got, key) >= 1,
            "KEYS at s{shards} lost {key}"
        );
    }

    // SCAN's cursor is the cross-shard cursor (moon#368); a full sweep from 0
    // must still reach all four keys.
    let mut cursor = String::from("0");
    let mut seen = 0usize;
    for _ in 0..64 {
        let got = c.send(&["SCAN", &cursor, "COUNT", "100"]);
        for key in &k {
            seen += count_occurrences(&got, key);
        }
        // Reply shape: *2\r\n$<n>\r\n<cursor>\r\n*<m>\r\n...
        let after = got.splitn(3, "\r\n").nth(2).unwrap_or("");
        let next: String = after.chars().take_while(|ch| ch.is_ascii_digit()).collect();
        if next.is_empty() || next == "0" {
            break;
        }
        cursor = next;
    }
    assert_eq!(
        seen, 4,
        "a full SCAN sweep at s{shards} must reach all four keys, saw {seen}"
    );

    let got = c.send(&["RANDOMKEY"]);
    assert!(
        k.iter().any(|key| got.contains(key)),
        "RANDOMKEY at s{shards} must return one of the live keys, got {got:?}"
    );

    // --- UNLINK: the multi-key delete arm -------------------------------
    let got = c.send(&["UNLINK", k[0], k[1]]);
    assert!(
        got.starts_with(":2\r\n"),
        "UNLINK of two live keys at s{shards} must be 2, got {got:?}"
    );
    let got = c.send(&["EXISTS", k[0], k[1], k[2], k[3]]);
    assert!(
        got.starts_with(":2\r\n"),
        "after UNLINK, EXISTS at s{shards} must be 2, got {got:?}"
    );

    // --- DEL: the same arm, the other verb ------------------------------
    let got = c.send(&["DEL", k[2], k[3]]);
    assert!(
        got.starts_with(":2\r\n"),
        "DEL of two live keys at s{shards} must be 2, got {got:?}"
    );
    let got = c.send(&["DBSIZE"]);
    assert!(
        got.starts_with(":0\r\n"),
        "DBSIZE after deleting every key at s{shards} must be 0, got {got:?}"
    );
}

/// The single-shard leg. The gate's new `num_shards > 1` test makes this the
/// leg where the coordinator is never entered at all — so it is the leg that
/// would notice if anything the coordinator used to do at one shard actually
/// mattered.
#[test]
fn cross_shard_gate_one_shard() {
    exercise(1);
}

/// The multi-shard leg. Identical assertions, spanning keys, and the leg that
/// goes red if the gate ever stops claiming the multi-key or keyless arms.
#[test]
fn cross_shard_gate_four_shards() {
    exercise(SHARDS);
}

/// The fixture must actually span shards, or the four-shard leg passes
/// vacuously and proves nothing about routing. Asserted separately so a
/// degenerate `key_to_shard` fails HERE, with a clear message, rather than
/// turning the real test green.
#[test]
fn spanning_fixture_really_spans() {
    let keys = spanning_keys(SHARDS as usize);
    let mut shards: Vec<usize> = keys
        .iter()
        .map(|k| key_to_shard(k.as_bytes(), SHARDS as usize))
        .collect();
    shards.sort_unstable();
    shards.dedup();
    assert_eq!(
        shards.len(),
        SHARDS as usize,
        "the four fixture keys must live on four distinct shards, got {shards:?}"
    );
}
