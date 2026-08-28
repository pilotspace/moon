//! moon#500: a multi-key write inside a TXN must roll back **every** key it
//! wrote, not just the first one.
//!
//! The undo capture in both connection handlers records one key per command:
//!
//! ```ignore
//! } else if let Some(key) = shared::extract_primary_key(cmd, cmd_args) {
//!     match db.get(key.as_ref()).cloned() {
//!         None => txn.kv_undo.record_insert(key.clone()),
//!         Some(entry) => txn.kv_undo.record_update(key.clone(), entry),
//!     }
//! }
//! ```
//!
//! `extract_primary_key` returns exactly one `Bytes`. `MSET k1 v1 k2 v2 k3 v3`
//! mutates three keys and logs an undo record for one, so `TXN ABORT` restores
//! `k1` and leaves `k2`/`k3` at their new values — a keyspace that is neither
//! the pre-TXN nor the post-TXN image. Measured on the shipped binary before
//! the fix:
//!
//! Measured across the full matrix before the fix — keys restored by
//! `TXN ABORT`, out of 4:
//!
//! ```text
//!                     MSET      multi-key DEL
//!   monoio shards=1    1/4          4/4
//!   monoio shards=4    0/4          0/4
//!   tokio  shards=1    0/4          0/4      <- even DEL, which has its own arm
//!   tokio  shards=4    0/4          0/4
//! ```
//!
//! Three distinct causes hide in that table. The capture records one key
//! (`extract_primary_key`), which is the 1/4. At `--shards > 1` the command is
//! intercepted by `coordinate_multi_key` before the capture runs, which is the
//! 0/4. And `handler_sharded`'s multi-key branch — unlike its monoio twin — has
//! no `num_shards <= 1` early return, so on tokio the interception happens at
//! ONE shard too, which is why even `DEL` is lost there.
//!
//! This is silent corruption delivered by the one operation whose entire
//! purpose is to prevent partial state, and it needs no crash, no disk
//! pressure and no concurrency to fire.
//!
//! ## Why the DEL control matters
//!
//! moon#500's title also claims multi-key `DEL` bypasses undo. It does not —
//! `DEL`/`UNLINK` have their own arm that already iterates every argument, and
//! a 4-key `DEL` inside an aborted TXN restores all four today. The control
//! below pins that, so a fix aimed at `MSET` cannot regress the path that was
//! already correct.
//!
//! ## Why the single-key control matters
//!
//! "Roll back every written key" must not become "roll back every key the
//! command mentions". `GET` writes nothing and `SETRANGE k` writes one key;
//! widening the capture to all *named* keys would inflate `kv_write_intents`,
//! which is the cross-shard conflict surface, and turn working transactions
//! into spurious conflicts.

// Deliberately NOT gated to one runtime. This suite spawns the real `moon`
// binary, so it exercises whichever runtime that binary was built with, and the
// two handlers carry SEPARATE copies of both the undo capture and the multi-key
// intercept. Gating to monoio would have hidden the worst cell of the matrix
// above: `handler_sharded`'s multi-key branch has no `num_shards <= 1` early
// return, so on tokio at one shard even multi-key `DEL` — which the undo capture
// handles correctly — never reached the capture at all.
//
// Pin `MOON_BIN` when running this against a specific build; `find_moon_binary`
// otherwise falls back to `target/release/moon`, whose provenance is unknown.

mod common;

use std::process::{Command, Stdio};

fn spawn_moon(dir: &std::path::Path, shards: usize) -> (common::ServerGuard, u16) {
    let bin = common::find_moon_binary();
    common::spawn_listening_guarded(|port| {
        Command::new(&bin)
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
                // Dev volumes routinely sit under the 5% default; without this
                // every write answers `MOONERR diskfull` and the suite would
                // "pass" without ever performing a write to roll back.
                "--disk-free-min-pct",
                "0",
                "--dir",
                &dir.to_string_lossy(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon (run `cargo build --release` first)")
    })
}

// NOTE: `TXN` is connection-scoped — `BEGIN`, the body and `ABORT` must all
// travel the SAME connection, which is why every test below reuses one
// `common::Conn`. Splitting them across connections silently exercises three
// unrelated transactions; that exact mistake produced 7 of the 26 false
// failures in moon#683.

/// Read a `MGET`-shaped reply into the values, so the assertion can name which
/// key failed to roll back rather than diffing an opaque RESP blob.
fn mget_values(conn: &mut common::Conn, keys: &[&str]) -> Vec<Option<String>> {
    let mut argv = vec!["MGET"];
    argv.extend_from_slice(keys);
    let raw = conn.send(&argv);

    let mut out = Vec::new();
    let mut rest = raw.as_str();
    // Skip the array header.
    if let Some(pos) = rest.find("\r\n") {
        rest = &rest[pos + 2..];
    }
    while !rest.is_empty() {
        if let Some(tail) = rest.strip_prefix("$-1\r\n") {
            out.push(None);
            rest = tail;
            continue;
        }
        let Some(pos) = rest.find("\r\n") else { break };
        let Ok(len) = rest[1..pos].parse::<usize>() else {
            break;
        };
        let start = pos + 2;
        out.push(Some(rest[start..start + len].to_string()));
        rest = &rest[start + len + 2..];
    }
    out
}

fn assert_multikey_rollback(shards: usize) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (_guard, port) = spawn_moon(tmp.path(), shards);
    let mut conn = common::Conn::open(port);
    assert_eq!(conn.send(&["PING"]), "+PONG\r\n");

    let keys = ["m500:k1", "m500:k2", "m500:k3", "m500:k4"];

    // Pre-image.
    assert_eq!(
        conn.send(&[
            "MSET", keys[0], "B1", keys[1], "B2", keys[2], "B3", keys[3], "B4",
        ]),
        "+OK\r\n"
    );

    assert_eq!(conn.send(&["TXN", "BEGIN"]), "+OK\r\n");
    let wrote = conn.send(&[
        "MSET", keys[0], "A1", keys[1], "A2", keys[2], "A3", keys[3], "A4",
    ]);
    let accepted = wrote == "+OK\r\n";
    if !accepted {
        // The only acceptable refusal is the documented cross-shard TXN one:
        // there is no cross-shard undo log, so a key set spanning shards has no
        // correct destination. Any OTHER error would mean this test stopped
        // exercising rollback and started passing vacuously.
        assert!(
            wrote.contains("cross-shard"),
            "shards={shards}: the MSET was refused, but not by the cross-shard TXN \
             guard. Either the guard changed or this test is no longer exercising \
             a write that needs rolling back (reply: {wrote:?})"
        );
    }
    assert_eq!(conn.send(&["TXN", "ABORT"]), "+OK\r\n");

    // THE INVARIANT, whichever branch was taken: the keyspace must equal the
    // pre-TXN image. An accepted write must roll back completely; a refused
    // write must not have landed at all. What is forbidden is the third
    // outcome — a partial keyspace that is neither image.
    let got = mget_values(&mut conn, &keys);
    let want = ["B1", "B2", "B3", "B4"];
    let restored = got
        .iter()
        .zip(want.iter())
        .filter(|(g, w)| g.as_deref() == Some(**w))
        .count();

    assert_eq!(
        restored,
        4,
        "shards={shards}: after TXN ABORT only {restored}/4 keys hold their pre-TXN \
         value (MSET was {}). The keyspace is neither the pre-TXN image ({want:?}) \
         nor the post-TXN image — an acked ABORT left torn state. got={got:?}",
        if accepted { "accepted" } else { "refused" }
    );
}

#[test]
fn multikey_mset_rolls_back_every_key_single_shard() {
    assert_multikey_rollback(1);
}

#[test]
fn multikey_mset_rolls_back_every_key_multi_shard() {
    assert_multikey_rollback(4);
}

/// `DEL`/`UNLINK` have their own capture arm that already iterates every
/// argument, so at one shard on monoio they were the one cell of the matrix that
/// always worked. They are still covered at BOTH shard counts because the
/// multi-key interception that broke `MSET` does not care which command it is —
/// on tokio it swallowed `DEL` too.
fn assert_del_rollback(shards: usize) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (_guard, port) = spawn_moon(tmp.path(), shards);
    let mut conn = common::Conn::open(port);
    assert_eq!(conn.send(&["PING"]), "+PONG\r\n");

    let keys = ["m500d:e1", "m500d:e2", "m500d:e3", "m500d:e4"];
    assert_eq!(
        conn.send(&[
            "MSET", keys[0], "x", keys[1], "x", keys[2], "x", keys[3], "x"
        ]),
        "+OK\r\n"
    );

    assert_eq!(conn.send(&["TXN", "BEGIN"]), "+OK\r\n");
    conn.send(&["DEL", keys[0], keys[1], keys[2], keys[3]]);
    assert_eq!(conn.send(&["TXN", "ABORT"]), "+OK\r\n");

    let got = mget_values(&mut conn, &keys);
    assert!(
        got.iter().all(|v| v.as_deref() == Some("x")),
        "shards={shards}: a multi-key DEL inside an aborted TXN deleted keys that \
         TXN ABORT was supposed to restore. got={got:?}"
    );
}

#[test]
fn multikey_del_still_rolls_back_every_key() {
    assert_del_rollback(1);
}

#[test]
fn multikey_del_rolls_back_every_key_multi_shard() {
    assert_del_rollback(4);
}

/// Control: a command that writes ONE key must still record exactly that key.
/// This is the guard against "capture every key the command names" — `GET`
/// writes nothing, and inflating the captured set inflates `kv_write_intents`,
/// the cross-shard conflict surface.
#[test]
fn single_key_write_still_rolls_back_and_reads_capture_nothing() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (_guard, port) = spawn_moon(tmp.path(), 1);
    let mut conn = common::Conn::open(port);
    assert_eq!(conn.send(&["PING"]), "+PONG\r\n");

    assert_eq!(conn.send(&["SET", "m500s:a", "before"]), "+OK\r\n");
    assert_eq!(conn.send(&["SET", "m500s:b", "untouched"]), "+OK\r\n");

    assert_eq!(conn.send(&["TXN", "BEGIN"]), "+OK\r\n");
    conn.send(&["SET", "m500s:a", "after"]);
    // A pure read inside the TXN must not enter the undo log at all.
    conn.send(&["GET", "m500s:b"]);
    assert_eq!(conn.send(&["TXN", "ABORT"]), "+OK\r\n");

    let got = mget_values(&mut conn, &["m500s:a", "m500s:b"]);
    assert_eq!(
        got,
        vec![Some("before".to_string()), Some("untouched".to_string())],
        "single-key rollback regressed, or a read was captured as a write"
    );
}
