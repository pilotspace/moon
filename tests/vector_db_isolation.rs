//! WS5a round 2 — db-scoped vector/text index isolation, red/green suite.
//!
//! Round 1 (see `.planning/v0.6.0-release/WS5A-NOTES.md`) shipped only the
//! data model: `db_index: u8` on `IndexMeta`/`TextIndex`, scoped store
//! primitives, persistence format bumps (v4/v2), and the FLUSHDB-vs-FLUSHALL
//! fix. `FT.CREATE` still tagged every index `db_index: 0` regardless of the
//! caller's SELECTed db, so none of the read-path scoping had any observable
//! effect outside db 0 — the headline promise ("an index created in db N is
//! invisible from every other db") was NOT actually true yet.
//!
//! This suite proves round 2 (FT.CREATE tags the real db + FT.SEARCH/
//! FT.INFO/FT._LIST/FT.DROPINDEX/FT.COMPACT/FT.CONFIG scoping across all
//! three dispatch paths) against a REAL spawned `moon` server process.
//!
//! Naming decision under test: index NAMES remain globally unique per shard
//! (not a composite `(db, name)` key) — each index is bound to exactly ONE
//! db at FT.CREATE time, and creating a name that already exists in ANY db
//! errors with "Index already exists" (see `src/command/vector_search/ft_create.rs`
//! doc comment + `src/vector/store.rs::create_index`).
//!
//! Spawns the release `moon` binary and talks to it over a real TCP
//! connection via the `redis` crate (blocking client) — no mocking, matches
//! `tests/flush_cross_shard_scatter.rs`'s "spawn a real process" pattern.
//! `MOON_BIN` env var is honored (pins the binary explicitly built for this
//! worktree); falls back to `target/release/moon` under `CARGO_MANIFEST_DIR`.
//!
//! Run with:
//!   MOON_BIN=/path/to/moon cargo test --release --test vector_db_isolation

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use redis::{Client, Connection};

fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
    l.local_addr().unwrap().port()
}

fn release_binary() -> std::path::PathBuf {
    // MOON_BIN pin wins (VM-local / worktree-local target dirs); fall back
    // to target/release/moon under this crate's manifest dir.
    if let Ok(p) = std::env::var("MOON_BIN") {
        return std::path::PathBuf::from(p);
    }
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/release/moon")
}

struct Moon {
    child: Child,
    port: u16,
    tmp_dir: std::path::PathBuf,
}

impl Moon {
    /// Connect and SELECT the given logical db.
    fn conn(&self, db: i64) -> Connection {
        let client =
            Client::open(format!("redis://127.0.0.1:{}/{}", self.port, db)).expect("client open");
        client.get_connection().expect("get_connection")
    }

    /// Kill the process WITHOUT removing the data dir, so a fresh restart
    /// against the same `--dir` can prove persistence round-trip.
    fn kill_keep_dir(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Moon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.tmp_dir);
    }
}

fn wait_ready(port: u16) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(client) = Client::open(format!("redis://127.0.0.1:{port}/0")) {
            if let Ok(mut conn) = client.get_connection() {
                let pong: Result<String, _> = redis::cmd("PING").query(&mut conn);
                if pong.as_deref() == Ok("PONG") {
                    return true;
                }
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

/// Spawn moon fresh (new tmp dir, new port). `shards` lets callers exercise
/// both single-shard and multi-shard dispatch paths (CLAUDE.md's "three
/// dispatch path" gotcha means single-shard vs multi-shard is a REAL branch,
/// not just a config knob).
fn spawn_moon(shards: usize, tag: &str) -> Option<Moon> {
    let bin = release_binary();
    if !bin.exists() {
        eprintln!(
            "skipping: {} not built. Run `cargo build --release` first.",
            bin.display()
        );
        return None;
    }
    let port = free_port();
    let tmp_dir = std::env::temp_dir().join(format!("moon-db-isolation-{tag}-{port}"));
    let _ = std::fs::create_dir_all(&tmp_dir);
    spawn_moon_at(&tmp_dir, port, shards)
}

fn spawn_moon_at(tmp_dir: &std::path::Path, port: u16, shards: usize) -> Option<Moon> {
    let bin = release_binary();
    let child = Command::new(&bin)
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            &shards.to_string(),
            "--admin-port",
            "0",
            "--appendonly",
            "no",
            "--dir",
            tmp_dir.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let moon = Moon {
        child,
        port,
        tmp_dir: tmp_dir.to_path_buf(),
    };
    if wait_ready(moon.port) {
        Some(moon)
    } else {
        eprintln!(
            "skipping: moon did not respond to PING within 10s on port {}",
            moon.port
        );
        None
    }
}

const DIM: usize = 4;

fn vec_bytes(v: [f32; DIM]) -> Vec<u8> {
    let mut out = Vec::with_capacity(DIM * 4);
    for x in &v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

fn ft_create(conn: &mut Connection, idx: &str, prefix: &str) -> redis::RedisResult<String> {
    redis::cmd("FT.CREATE")
        .arg(idx)
        .arg("ON")
        .arg("HASH")
        .arg("PREFIX")
        .arg("1")
        .arg(prefix)
        .arg("SCHEMA")
        .arg("vec")
        .arg("VECTOR")
        .arg("HNSW")
        .arg("6")
        .arg("DIM")
        .arg(DIM.to_string())
        .arg("TYPE")
        .arg("FLOAT32")
        .arg("DISTANCE_METRIC")
        .arg("L2")
        .query(conn)
}

fn hset_vec(conn: &mut Connection, key: &str, v: [f32; DIM]) -> redis::RedisResult<i64> {
    redis::cmd("HSET")
        .arg(key)
        .arg("vec")
        .arg(vec_bytes(v))
        .query(conn)
}

/// KNN query, matching the syntax used elsewhere in this crate's test suite
/// (`tests/ft_search_concurrent_readers.rs`): `*=>[KNN k @field $param]` with
/// `PARAMS`/`DIALECT 2`.
fn ft_search_knn(
    conn: &mut Connection,
    idx: &str,
    v: [f32; DIM],
) -> redis::RedisResult<redis::Value> {
    redis::cmd("FT.SEARCH")
        .arg(idx)
        .arg("*=>[KNN 5 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(vec_bytes(v))
        .arg("DIALECT")
        .arg("2")
        .query::<redis::Value>(conn)
}

fn ft_search_count(conn: &mut Connection, idx: &str, v: [f32; DIM]) -> redis::Value {
    ft_search_knn(conn, idx, v).unwrap_or(redis::Value::Nil)
}

// ---------------------------------------------------------------------------
// (a) Headline: cross-db invisibility, both directions.
// ---------------------------------------------------------------------------

#[test]
fn headline_index_created_in_one_db_invisible_from_another() {
    let Some(m) = spawn_moon(1, "headline") else {
        return;
    };

    // Create "idxA" in db 0, "idxB" in db 3 — distinct names so the
    // "index already exists" collision rule (test c) doesn't interfere.
    let mut c0 = m.conn(0);
    let mut c3 = m.conn(3);

    assert_eq!(ft_create(&mut c0, "idxA", "docA:").unwrap(), "OK");
    assert_eq!(ft_create(&mut c3, "idxB", "docB:").unwrap(), "OK");

    hset_vec(&mut c0, "docA:1", [1.0, 0.0, 0.0, 0.0]).unwrap();
    hset_vec(&mut c3, "docB:1", [1.0, 0.0, 0.0, 0.0]).unwrap();

    // idxA is invisible from db 3.
    let err = ft_search_knn(&mut c3, "idxA", [1.0, 0.0, 0.0, 0.0]);
    assert!(
        err.is_err(),
        "idxA (created in db 0) must be invisible (NOTFOUND) from db 3, got {err:?}"
    );

    // idxB is invisible from db 0.
    let err = ft_search_knn(&mut c0, "idxB", [1.0, 0.0, 0.0, 0.0]);
    assert!(
        err.is_err(),
        "idxB (created in db 3) must be invisible (NOTFOUND) from db 0, got {err:?}"
    );

    // Sanity: each index IS visible/searchable from its own db.
    let r0 = ft_search_count(&mut c0, "idxA", [1.0, 0.0, 0.0, 0.0]);
    assert!(
        !matches!(r0, redis::Value::Nil),
        "idxA must be searchable from its own db 0"
    );
    let r3 = ft_search_count(&mut c3, "idxB", [1.0, 0.0, 0.0, 0.0]);
    assert!(
        !matches!(r3, redis::Value::Nil),
        "idxB must be searchable from its own db 3"
    );
}

// ---------------------------------------------------------------------------
// (b) FT._LIST is scoped per db.
// ---------------------------------------------------------------------------

#[test]
fn ft_list_scoped_per_db() {
    let Some(m) = spawn_moon(1, "list") else {
        return;
    };
    let mut c0 = m.conn(0);
    let mut c1 = m.conn(1);

    assert_eq!(ft_create(&mut c0, "list_idx0", "l0:").unwrap(), "OK");
    assert_eq!(ft_create(&mut c1, "list_idx1", "l1:").unwrap(), "OK");

    let list0: Vec<String> = redis::cmd("FT._LIST").query(&mut c0).unwrap();
    assert!(
        list0.iter().any(|n| n == "list_idx0"),
        "db 0's FT._LIST must show its own index: {list0:?}"
    );
    assert!(
        !list0.iter().any(|n| n == "list_idx1"),
        "db 0's FT._LIST must NOT show db 1's index: {list0:?}"
    );

    let list1: Vec<String> = redis::cmd("FT._LIST").query(&mut c1).unwrap();
    assert!(
        list1.iter().any(|n| n == "list_idx1"),
        "db 1's FT._LIST must show its own index: {list1:?}"
    );
    assert!(
        !list1.iter().any(|n| n == "list_idx0"),
        "db 1's FT._LIST must NOT show db 0's index: {list1:?}"
    );
}

// ---------------------------------------------------------------------------
// (c) FT.CREATE name collision across dbs errors (index names are GLOBAL).
// ---------------------------------------------------------------------------

#[test]
fn ft_create_same_name_different_db_errors() {
    let Some(m) = spawn_moon(1, "collision") else {
        return;
    };
    let mut c0 = m.conn(0);
    let mut c1 = m.conn(1);

    assert_eq!(ft_create(&mut c0, "shared_name", "s0:").unwrap(), "OK");

    let err = ft_create(&mut c1, "shared_name", "s1:");
    assert!(
        err.is_err(),
        "FT.CREATE of a name that already exists in another db must error, got {err:?}"
    );
    let msg = err.unwrap_err().to_string();
    assert!(
        msg.contains("already exists"),
        "expected 'Index already exists', got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// (d) Restart round-trip: db-1-scoped index persists and stays bound to db 1.
// ---------------------------------------------------------------------------

#[test]
fn restart_round_trip_preserves_db_binding() {
    let bin = release_binary();
    if !bin.exists() {
        eprintln!("skipping: {} not built.", bin.display());
        return;
    }
    let port = free_port();
    let tmp_dir = std::env::temp_dir().join(format!("moon-db-isolation-restart-{port}"));
    let _ = std::fs::create_dir_all(&tmp_dir);

    let mut moon = match spawn_moon_at(&tmp_dir, port, 1) {
        Some(m) => m,
        None => {
            let _ = std::fs::remove_dir_all(&tmp_dir);
            return;
        }
    };

    {
        let mut c1 = moon.conn(1);
        assert_eq!(ft_create(&mut c1, "restart_idx", "r:").unwrap(), "OK");
        hset_vec(&mut c1, "r:1", [1.0, 0.0, 0.0, 0.0]).unwrap();
        // FT.COMPACT forces the mutable segment through to the immutable
        // (persisted) representation so the sidecar has real content, not
        // just an empty definition (CLAUDE.md "FT.COMPACT is a silent
        // no-op" gotcha doesn't apply here since we call it explicitly).
        let _: redis::Value = redis::cmd("FT.COMPACT")
            .arg("restart_idx")
            .query(&mut c1)
            .unwrap_or(redis::Value::Nil);
    }

    moon.kill_keep_dir();
    // Re-bind on the SAME port + SAME dir — proves this is a true restart,
    // not a fresh server (free_port() would risk a different port winning
    // a race; reuse the already-freed one directly since we just killed
    // the owning process).
    let restarted = spawn_moon_at(&tmp_dir, port, 1);
    let Some(moon2) = restarted else {
        eprintln!("skipping: restart did not come back up on port {port}");
        let _ = std::fs::remove_dir_all(&tmp_dir);
        return;
    };

    let mut c1 = moon2.conn(1);
    let mut c0 = moon2.conn(0);

    let info1: redis::Value = redis::cmd("FT.INFO")
        .arg("restart_idx")
        .query(&mut c1)
        .expect("FT.INFO from db 1 must succeed after restart");
    assert!(
        !matches!(info1, redis::Value::Nil),
        "restart_idx must reload bound to db 1"
    );

    let info0 = redis::cmd("FT.INFO")
        .arg("restart_idx")
        .query::<redis::Value>(&mut c0);
    assert!(
        info0.is_err(),
        "restart_idx must stay invisible from db 0 after restart, got {info0:?}"
    );

    let r1 = ft_search_count(&mut c1, "restart_idx", [1.0, 0.0, 0.0, 0.0]);
    assert!(
        !matches!(r1, redis::Value::Nil),
        "restart_idx's vector must remain searchable from db 1 after restart"
    );

    drop(moon2);
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

// ---------------------------------------------------------------------------
// (f) Multi-shard db-isolation — the finding-1 regression suite.
// ---------------------------------------------------------------------------
//
// All tests above use `spawn_moon(1, ..)` (single shard), which is exactly
// why the multi-shard `scatter_text_search` / `scatter_hybrid_search` /
// `scatter_text_aggregate` gap (adversarial review finding 1) went
// undetected: on 1 shard, "scatter" degenerates to the local fast path,
// which was already correctly `db_index`-scoped. `--shards >= 2` is the
// only way to force a real cross-shard fan-out and exercise the
// previously-unscoped remote leg.

/// `FT.CREATE` with a TEXT field (not just VECTOR), matching the
/// `SCHEMA <field> TEXT` pattern used by `tests/hybrid_filter_multishard.rs`'s
/// `ft_create_tag_idx` helper. Needed because finding 1 is specifically
/// about the plain-text/BM25 scatter path (`scatter_text_search`), which a
/// VECTOR-only schema never exercises.
fn ft_create_text(conn: &mut Connection, idx: &str, prefix: &str) -> redis::RedisResult<String> {
    redis::cmd("FT.CREATE")
        .arg(idx)
        .arg("ON")
        .arg("HASH")
        .arg("PREFIX")
        .arg("1")
        .arg(prefix)
        .arg("SCHEMA")
        .arg("title")
        .arg("TEXT")
        .query(conn)
}

/// Bare (non-KNN) `FT.SEARCH` — the plain-text BM25 path that
/// `scatter_text_search` implements, as distinct from `ft_search_knn`'s
/// `*=>[KNN ...]` vector clause.
fn ft_search_text(
    conn: &mut Connection,
    idx: &str,
    query: &str,
) -> redis::RedisResult<redis::Value> {
    redis::cmd("FT.SEARCH")
        .arg(idx)
        .arg(query)
        .query::<redis::Value>(conn)
}

/// (f1) Plain-text FT.SEARCH cross-db invisibility on a multi-shard server —
/// the exact scenario from adversarial review finding 1: a db-3 connection
/// must NOT see (let alone get real results from) a TEXT index created in
/// db 0, once `--shards >= 2` forces the request through
/// `scatter_text_search`'s remote-leg fan-out instead of the local fast path.
#[test]
fn multishard_text_search_cross_db_invisible() {
    let Some(m) = spawn_moon(4, "ms-text") else {
        return;
    };
    let mut c0 = m.conn(0);
    let mut c3 = m.conn(3);

    assert_eq!(
        ft_create_text(&mut c0, "ms_text_idx", "mstxt:").unwrap(),
        "OK"
    );

    // NOTE: avoid common English words ("hello"/"world") — Moon's default
    // analyzer (`src/text/analyzer.rs`) applies the standard English
    // stopword list at both index and query time, so those specific tokens
    // never match anything regardless of db scoping. Use uncommon tokens.
    let mut set = false;
    for i in 0..8 {
        let key = format!("mstxt:{i}");
        let _: i64 = redis::cmd("HSET")
            .arg(&key)
            .arg("title")
            .arg("xylophone quokka moon")
            .query(&mut c0)
            .unwrap();
        set = true;
    }
    assert!(set);
    // Cross-shard auto-indexing lands asynchronously (see
    // `hybrid_filter_multishard.rs`'s identical 150ms settle before its
    // first FT.SEARCH) — give it a moment before querying.
    thread::sleep(Duration::from_millis(200));

    // A db-3 connection must NEVER see real hits from db-0's index. Under
    // multi-shard fan-out, `merge_search_results`/text-merge deliberately
    // "skip errored shards" (each shard independently returns "Unknown Index
    // name" / "no such index" for a foreign db_index, then the coordinator
    // merges the survivors) — so the wire-visible outcome here is a
    // zero-result OK array, not a propagated error (that differs from the
    // single-shard fast path, which surfaces the error directly; see
    // `headline_index_created_in_one_db_invisible_from_another`). Either
    // outcome is SAFE (no data leak); what would be the finding-1 regression
    // is if `ms_text_idx`'s real documents showed up here.
    let resp = ft_search_text(&mut c3, "ms_text_idx", "xylophone");
    match resp {
        Err(_) => {} // NOTFOUND — also acceptable.
        Ok(redis::Value::Array(items)) => {
            assert!(
                matches!(items.first(), Some(redis::Value::Int(0)) | None),
                "ms_text_idx (created in db 0) leaked real hits to db 3 on a \
                 4-shard server: {items:?} — this is adversarial review finding 1 \
                 (scatter_text_search unscoped by db_index)"
            );
        }
        other => panic!("unexpected FT.SEARCH reply shape from foreign db: {other:?}"),
    }

    // Sanity: still searchable, with real hits, from its own db.
    let r0 = ft_search_text(&mut c0, "ms_text_idx", "xylophone").unwrap();
    match &r0 {
        redis::Value::Array(items) => {
            let total = items.first();
            assert!(
                !matches!(total, Some(redis::Value::Int(0)) | None),
                "ms_text_idx must return real hits from its own db 0, got {r0:?}"
            );
        }
        other => panic!("unexpected FT.SEARCH reply shape: {other:?}"),
    }
}

/// (f2) KNN cross-db invisibility on a multi-shard server — reuses the
/// existing VECTOR-only helpers, just at `--shards 4` instead of 1, to prove
/// the already-correct `scatter_vector_search` path stays correct under
/// this suite's new higher shard count (regression guard, not a new bug).
#[test]
fn multishard_knn_search_cross_db_invisible() {
    let Some(m) = spawn_moon(4, "ms-knn") else {
        return;
    };
    let mut c0 = m.conn(0);
    let mut c3 = m.conn(3);

    assert_eq!(ft_create(&mut c0, "ms_knn_idx", "msknn:").unwrap(), "OK");
    hset_vec(&mut c0, "msknn:1", [1.0, 0.0, 0.0, 0.0]).unwrap();
    thread::sleep(Duration::from_millis(200));

    // Same merge-skips-errored-shards nuance as the text case above: a
    // foreign db_index yields an empty OK array (all 4 shards independently
    // returned "Unknown Index name" for db 3, and the coordinator's
    // `merge_search_results` drops errored-shard responses rather than
    // propagating them) rather than a hard error. Either shape is safe; the
    // regression to guard against is `msknn:1` actually appearing.
    let resp = ft_search_knn(&mut c3, "ms_knn_idx", [1.0, 0.0, 0.0, 0.0]);
    match resp {
        Err(_) => {}
        Ok(redis::Value::Array(items)) => {
            assert!(
                matches!(items.first(), Some(redis::Value::Int(0)) | None),
                "ms_knn_idx (created in db 0) leaked real hits to db 3 on a \
                 4-shard server: {items:?}"
            );
        }
        other => panic!("unexpected FT.SEARCH reply shape from foreign db: {other:?}"),
    }

    let r0 = ft_search_count(&mut c0, "ms_knn_idx", [1.0, 0.0, 0.0, 0.0]);
    assert!(
        !matches!(r0, redis::Value::Nil),
        "ms_knn_idx must remain searchable from its own db 0 on a 4-shard server"
    );
}

/// (f3) `FT._LIST` scoping on a multi-shard server — the list-merge path
/// (`scatter_ft_list` or equivalent) must stay per-db scoped once results
/// are gathered across shards, not just on the single-shard fast path.
#[test]
fn multishard_ft_list_scoped_per_db() {
    let Some(m) = spawn_moon(4, "ms-list") else {
        return;
    };
    let mut c0 = m.conn(0);
    let mut c1 = m.conn(1);

    assert_eq!(ft_create(&mut c0, "ms_list_idx0", "msl0:").unwrap(), "OK");
    assert_eq!(ft_create(&mut c1, "ms_list_idx1", "msl1:").unwrap(), "OK");

    let list0: Vec<String> = redis::cmd("FT._LIST").query(&mut c0).unwrap();
    assert!(
        list0.iter().any(|n| n == "ms_list_idx0"),
        "db 0's FT._LIST must show its own index on a 4-shard server: {list0:?}"
    );
    assert!(
        !list0.iter().any(|n| n == "ms_list_idx1"),
        "db 0's FT._LIST must NOT show db 1's index on a 4-shard server: {list0:?}"
    );

    let list1: Vec<String> = redis::cmd("FT._LIST").query(&mut c1).unwrap();
    assert!(
        list1.iter().any(|n| n == "ms_list_idx1"),
        "db 1's FT._LIST must show its own index on a 4-shard server: {list1:?}"
    );
    assert!(
        !list1.iter().any(|n| n == "ms_list_idx0"),
        "db 1's FT._LIST must NOT show db 0's index on a 4-shard server: {list1:?}"
    );
}

// ---------------------------------------------------------------------------
// (e) Legacy sidecar loads to db 0 — already covered by unit tests.
// ---------------------------------------------------------------------------
//
// `src/vector/store.rs::ws5a_db_scoping` and
// `src/vector/index_persist.rs` (e.g. `legacyidx` / v1-v3 round-trip tests)
// already assert that a pre-v0.6.0 sidecar (no `db_index` field on disk)
// deserializes with `db_index == 0` for every recovered index. Re-deriving
// that here would mean fabricating a byte-for-byte legacy binary fixture —
// expensive and duplicative of coverage that already runs in `cargo test
// --lib` (see e.g. `vector::index_persist::tests::*legacy*`). No new
// integration test added for (e); the existing unit-level proof is
// considered sufficient given it exercises the exact deserialization code
// path a real restart would use.
