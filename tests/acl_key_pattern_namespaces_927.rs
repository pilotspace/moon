//! moon#927 — ACL `~pattern` must apply to `MQ`, and must not be silently
//! skipped for the namespaces it cannot scope.
//!
//! # The bypasses, as measured on the unfixed tree (moon 0.8.9, `--shards 1`)
//!
//! ```text
//! ACL SETUSER tenant on >pw ~cache:* +@all
//!
//! HGET secret:doc1 body   -> -NOPERM ... key 'secret:doc1'      (pattern IS enforced)
//! HSET cache:ok f v       -> :1                                  (permitted key works)
//!
//! MQ CREATE secretq       -> +OK          <-- writes OUTSIDE ~cache:*
//! MQ PUSH   secretq p v   -> "1789065040233-0"
//! MQ POP    secretq       -> [[id, [payload, stolen]]]
//!   admin TYPE secretq    -> stream        (a real keyspace key)
//!   admin XLEN secretq    -> 1
//!
//! FT.SEARCH tidx alpha    -> [1, "secret:doc1", ["__bm25_score", "0.287682"]]
//! CDC.READ  <wal_dir> 0   -> every command and value in the WHOLE keyspace
//! ```
//!
//! Mechanism: ACL key enforcement derives key positions from `COMMAND_META`,
//! and `first_key: 0` is indistinguishable between "this command has no keys"
//! and "nobody filled the key positions in". `MQ` declared the second while
//! meaning the first (`src/command/mq.rs` reads its queue key at `args[1]`).
//!
//! # What each test asserts
//!
//! Every case asserts BOTH halves. A test that only checks the denial passes
//! on a server that denies everything, and a test that only checks the
//! permitted key passes on a server that enforces nothing.
//!
//! # Runtime coverage
//!
//! moon has three connection handlers, each with its own ACL gate:
//! `handler_monoio` (default build), `handler_single` (tokio, `--shards 1`)
//! and `handler_sharded` (tokio, `--shards > 1`). Every test here runs at both
//! `--shards 1` and `--shards 4`, so one `cargo test` per runtime leg covers
//! all three. The inline fast path (`try_inline_dispatch`) is covered by
//! `tests/acl_inline_read_enforcement.rs`; it can only ever see `*2`/`*3`
//! frames and is gated on `acl_skip_allowed()`, so no `MQ`/`FT.` argv reaches
//! it.

mod common;

use std::process::{Command, Stdio};

use common::Conn;

const SHARD_COUNTS: [&str; 2] = ["1", "4"];

struct Moon {
    _guard: common::ServerGuard,
    port: u16,
    dir: std::path::PathBuf,
}

fn spawn_moon(shards: &str) -> Moon {
    // CARGO_BIN_EXE_moon is the binary cargo built for THIS invocation - fresh
    // and feature-matched. `find_moon_binary()` falls back to
    // `target/release/moon`, whose provenance is unknown.
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let dir_cell = std::cell::RefCell::new(std::path::PathBuf::new());
    let (guard, port) = common::spawn_listening_guarded(|port| {
        let dir = common::unique_test_dir(&format!("acl927-{shards}-{port}"));
        let _ = std::fs::create_dir_all(&dir);
        *dir_cell.borrow_mut() = dir.clone();
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                shards,
                "--admin-port",
                "0",
                // This host hovers near the 5% diskfull line; without this the
                // guard turns writes into MOONERR and the suite flakes.
                "--disk-free-min-pct",
                "0",
                "--dir",
                dir.to_str().expect("utf8 dir"),
            ])
            .stdout(Stdio::null())
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon")
    });
    let dir = dir_cell.into_inner();
    // `spawn_listening_guarded` only proves the port is bound. Prove the
    // process answering it is OURS: this host runs ~18 stray `redis-server`
    // processes, and a foreign listener would PONG just as happily.
    let mut probe = Conn::open(port);
    let info = probe.send(&["INFO", "server"]);
    assert!(
        info.contains("moon_version"),
        "port {port} is held by something that is not moon: {info:?}"
    );
    Moon {
        _guard: guard,
        port,
        dir,
    }
}

/// `ACL SETUSER tenant on >pw ~cache:* +@all` plus one protected and one
/// permitted hash. Returns an admin connection.
fn seed(port: u16) -> Conn {
    let mut admin = Conn::open(port);
    let r = admin.send(&["ACL", "SETUSER", "tenant", "on", ">pw", "~cache:*", "+@all"]);
    assert!(r.starts_with("+OK"), "ACL SETUSER tenant: {r:?}");
    // The `~*` control user: unrestricted keys, so every assertion about a
    // DENIAL below must NOT fire for this one.
    let r = admin.send(&["ACL", "SETUSER", "wide", "on", ">pw", "~*", "+@all"]);
    assert!(r.starts_with("+OK"), "ACL SETUSER wide: {r:?}");
    // Index FIRST, document SECOND: moon indexes on write, so a hash written
    // before `FT.CREATE` is invisible to `FT.SEARCH`. (Getting this backwards
    // is how the first draft of the reproduction "disproved" the bypass.)
    //
    // Not asserted: the `text-index` feature is in the DEFAULT set but is
    // dropped on the tokio CI leg, where `FT.CREATE` answers an error. The
    // denial under test is decided before dispatch and holds either way; only
    // this positive control is feature-dependent.
    let created = admin.send(&[
        "FT.CREATE",
        "tidx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "secret:",
        "SCHEMA",
        "body",
        "TEXT",
    ]);
    let r = admin.send(&["HSET", "secret:doc1", "body", "alpha beta gamma"]);
    assert!(r.starts_with(":1"), "seed secret:doc1: {r:?}");
    if created.starts_with("+OK") {
        let hit = admin.send(&["FT.SEARCH", "tidx", "alpha"]);
        assert!(
            hit.contains("secret:doc1"),
            "seed sanity: the index must really project the protected hash, got {hit:?}"
        );
    }
    admin
}

fn auth(port: u16, user: &str) -> Conn {
    let mut c = Conn::open(port);
    let r = c.send(&["AUTH", user, "pw"]);
    assert!(r.starts_with("+OK"), "AUTH {user}: {r:?}");
    c
}

fn assert_noperm(reply: &str, what: &str) {
    assert!(
        reply.starts_with("-NOPERM"),
        "{what} must be refused for a `~cache:*` user, got {reply:?}"
    );
}

fn assert_not_noperm(reply: &str, what: &str) {
    assert!(
        !reply.starts_with("-NOPERM"),
        "{what} must NOT be refused, got {reply:?}"
    );
}

/// The controls. If these fail, every other assertion in this file is
/// meaningless — a server that denies everything, or enforces nothing, would
/// otherwise pass the denial half or the permitted half by accident.
fn assert_controls(port: u16) {
    let mut tenant = auth(port, "tenant");
    assert_noperm(
        &tenant.send(&["HGET", "secret:doc1", "body"]),
        "HGET secret:doc1 (control: the pattern IS enforced)",
    );
    let ok = tenant.send(&["HSET", "cache:ok", "f", "v"]);
    assert!(
        ok.starts_with(":1") || ok.starts_with(":0"),
        "control: a PERMITTED key must still work, got {ok:?}"
    );
}

// ---------------------------------------------------------------------------
// MQ — the write bypass
// ---------------------------------------------------------------------------

/// Every `MQ` subcommand moon serves, with a queue name OUTSIDE `~cache:*`.
/// A subcommand missing from this list is a bypass that survives the fix, so
/// it is enumerated exhaustively against `src/command/mq.rs`.
const MQ_OUTSIDE: &[&[&str]] = &[
    &["MQ", "CREATE", "secretq"],
    &["MQ", "PUSH", "secretq", "payload", "stolen"],
    &["MQ", "POP", "secretq"],
    &["MQ", "ACK", "secretq", "1-1"],
    &["MQ", "DLQLEN", "secretq"],
    &["MQ", "TRIGGER", "secretq", "PING"],
    &["MQ", "PUBLISH", "secretq", "payload", "stolen"],
];

#[test]
fn mq_outside_the_pattern_is_denied_and_writes_nothing() {
    for shards in SHARD_COUNTS {
        let m = spawn_moon(shards);
        let mut admin = seed(m.port);
        assert_controls(m.port);

        let mut tenant = auth(m.port, "tenant");
        for argv in MQ_OUTSIDE {
            let reply = tenant.send(argv);
            assert_noperm(&reply, &format!("--shards {shards}: {argv:?}"));
        }
        // The denial must be a denial, not a late error: no keyspace key may
        // exist afterwards. `MQ CREATE secretq` answered `+OK` and left a
        // `stream` behind on the unfixed tree.
        let ty = admin.send(&["TYPE", "secretq"]);
        assert!(
            ty.starts_with("+none"),
            "--shards {shards}: MQ must not have created a key outside ~cache:*, TYPE gave {ty:?}"
        );
    }
}

#[test]
fn mq_inside_the_pattern_still_works() {
    for shards in SHARD_COUNTS {
        let m = spawn_moon(shards);
        let mut admin = seed(m.port);
        assert_controls(m.port);

        let mut tenant = auth(m.port, "tenant");
        for argv in [
            &["MQ", "CREATE", "cache:q"][..],
            &["MQ", "PUSH", "cache:q", "payload", "mine"][..],
            &["MQ", "POP", "cache:q"][..],
            &["MQ", "DLQLEN", "cache:q"][..],
            &["MQ", "TRIGGER", "cache:q", "PING"][..],
        ] {
            let reply = tenant.send(argv);
            assert_not_noperm(&reply, &format!("--shards {shards}: {argv:?}"));
        }
        let ty = admin.send(&["TYPE", "cache:q"]);
        assert!(
            ty.starts_with("+stream"),
            "--shards {shards}: a permitted MQ queue must really be created, TYPE gave {ty:?}"
        );
    }
}

#[test]
fn mq_unknown_subcommand_fails_closed() {
    for shards in SHARD_COUNTS {
        let m = spawn_moon(shards);
        let _admin = seed(m.port);
        let mut tenant = auth(m.port, "tenant");
        // A subcommand this walker does not know may name a key anywhere. It
        // must not be waved through on the strength of not being recognised.
        assert_noperm(
            &tenant.send(&["MQ", "NOSUCHSUB", "secretq"]),
            &format!("--shards {shards}: MQ NOSUCHSUB"),
        );
    }
}

// ---------------------------------------------------------------------------
// FT.* / CDC.READ — the namespaces `~pattern` cannot scope
// ---------------------------------------------------------------------------

#[test]
fn ft_search_does_not_project_protected_hashes_to_a_restricted_user() {
    for shards in SHARD_COUNTS {
        let m = spawn_moon(shards);
        let mut admin = seed(m.port);
        assert_controls(m.port);

        let mut tenant = auth(m.port, "tenant");
        assert_noperm(
            &tenant.send(&["FT.SEARCH", "tidx", "alpha"]),
            &format!("--shards {shards}: FT.SEARCH"),
        );
        assert_noperm(
            &tenant.send(&["FT.AGGREGATE", "tidx", "alpha"]),
            &format!("--shards {shards}: FT.AGGREGATE"),
        );
        assert_noperm(
            &tenant.send(&["FT._LIST"]),
            &format!("--shards {shards}: FT._LIST"),
        );

        // The other half: a `~*` user is UNAFFECTED and still gets an answer
        // (an `ERR`/empty result is fine — anything but NOPERM).
        let mut wide = auth(m.port, "wide");
        assert_not_noperm(
            &wide.send(&["FT._LIST"]),
            &format!("--shards {shards}: FT._LIST for a ~* user"),
        );
        assert_not_noperm(
            &admin.send(&["FT._LIST"]),
            &format!("--shards {shards}: FT._LIST for the unrestricted default user"),
        );
    }
}

/// `CDC.READ <wal_dir> <lsn>` returns the raw WAL — every command and value in
/// the whole keyspace, in Debezium JSON with the RESP bytes base64'd. Measured
/// on the unfixed tree, a `~cache:*` user read `secret:*` values straight out
/// of it. Its first argument is a DIRECTORY, not a key, so no key spec can
/// scope it.
#[test]
fn cdc_read_is_denied_for_a_key_restricted_user() {
    for shards in SHARD_COUNTS {
        let m = spawn_moon(shards);
        let mut admin = seed(m.port);
        assert_controls(m.port);
        let wal = m.dir.join("shard-0").join("wal-v3");
        let wal = wal.to_str().expect("utf8 wal dir");

        let mut tenant = auth(m.port, "tenant");
        assert_noperm(
            &tenant.send(&["CDC.READ", wal, "0"]),
            &format!("--shards {shards}: CDC.READ"),
        );

        let mut wide = auth(m.port, "wide");
        assert_not_noperm(
            &wide.send(&["CDC.READ", wal, "0"]),
            &format!("--shards {shards}: CDC.READ for a ~* user"),
        );
        assert_not_noperm(
            &admin.send(&["CDC.READ", wal, "0"]),
            &format!("--shards {shards}: CDC.READ for the unrestricted default user"),
        );
    }
}

// ---------------------------------------------------------------------------
// WS / GRAPH.* — decided to stay OUTSIDE the key-pattern model
// ---------------------------------------------------------------------------

/// `WS`'s arguments are workspace names and UUIDs, never keyspace keys
/// (`src/command/workspace.rs`), and `GRAPH.*` keeps its own store with no
/// keyspace reach at all (measured: `GRAPH.CREATE g` + `GRAPH.ADDNODE` leave
/// `DBSIZE` unchanged and `TYPE g` = none). Neither is scoped by `~pattern`;
/// both are gated by command/category permissions instead. This test pins that
/// decision so a future "tighten everything" sweep has to argue with it.
#[test]
fn ws_and_graph_stay_outside_the_key_pattern_model() {
    for shards in SHARD_COUNTS {
        let m = spawn_moon(shards);
        let _admin = seed(m.port);
        assert_controls(m.port);

        let mut tenant = auth(m.port, "tenant");
        for argv in [
            &["WS", "LIST"][..],
            &["WS", "CREATE", "ws927"][..],
            &["GRAPH.CREATE", "g927"][..],
            &["GRAPH.LIST"][..],
        ] {
            assert_not_noperm(
                &tenant.send(argv),
                &format!("--shards {shards}: {argv:?} is not gated by ~pattern"),
            );
        }
        // ... and the category IS the gate that works on them.
        let mut admin2 = Conn::open(m.port);
        let r = admin2.send(&["ACL", "SETUSER", "nows", "on", ">pw", "~*", "+@all", "-ws"]);
        assert!(r.starts_with("+OK"), "ACL SETUSER nows: {r:?}");
        let mut nows = auth(m.port, "nows");
        let reply = nows.send(&["WS", "LIST"]);
        assert!(
            reply.starts_with("-NOPERM"),
            "--shards {shards}: `-ws` must still deny WS, got {reply:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// No regression in the permissive direction
// ---------------------------------------------------------------------------

/// A user who legitimately has `~*` must be able to do everything above. This
/// is the constraint the fix is most likely to violate, so it is asserted for
/// the whole 927 surface in one place rather than as an afterthought in each
/// test.
#[test]
fn allkeys_user_is_unaffected_by_the_whole_927_surface() {
    for shards in SHARD_COUNTS {
        let m = spawn_moon(shards);
        let mut admin = seed(m.port);
        let wal = m.dir.join("shard-0").join("wal-v3");
        let wal = wal.to_str().expect("utf8 wal dir");

        for user in ["wide"] {
            let mut c = auth(m.port, user);
            for argv in [
                &["MQ", "CREATE", "anyq"][..],
                &["MQ", "PUSH", "anyq", "f", "v"][..],
                &["MQ", "POP", "anyq"][..],
                &["MQ", "DLQLEN", "anyq"][..],
                &["FT._LIST"][..],
                &["FT.SEARCH", "tidx", "alpha"][..],
                &["CDC.READ", wal, "0"][..],
                &["WS", "LIST"][..],
                &["GRAPH.LIST"][..],
                &["GET", "secret:doc1"][..],
            ] {
                assert_not_noperm(
                    &c.send(argv),
                    &format!("--shards {shards}: ~* user `{user}` running {argv:?}"),
                );
            }
        }
        // The unrestricted default user takes an even earlier short-circuit
        // (`user.unrestricted`), so it is proven separately.
        let ty = admin.send(&["TYPE", "anyq"]);
        assert!(
            ty.starts_with("+stream"),
            "--shards {shards}: the ~* user's queue must really exist, TYPE gave {ty:?}"
        );
    }
}
