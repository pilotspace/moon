//! Lunaris V1 verification — Cypher edge-property temporal filter in nested MATCH.
//!
//! Phase 165 Plan 04 — Task 1. VERIFICATION ONLY. No `src/` files are modified
//! by this test (enforced by plan `test -z "$(git diff --name-only HEAD -- src/)"`).
//!
//! # Lunaris dependency
//!
//! Blueprint §5.2 / `lunaris_integration_guideline.md §C.6` maps
//! `GraphFirstRetriever` onto a Cypher query shape like:
//!
//! ```cypher
//! UNWIND $names AS n
//! MATCH (e:Entity {workspace:$ws}) WHERE e.name = n
//! MATCH (e)-[r*1..2]-(neighbor)
//! WHERE coalesce(r.valid_to, $asof) >= $asof
//! RETURN e, r, neighbor LIMIT $k
//! ```
//!
//! Three Moon features combine here:
//!   1. Variable-length path `-[r*1..2]-` (VERIFIED supported: parser mod.rs:620)
//!   2. Edge-variable `r` binding in variable-length expansion (UNVERIFIED)
//!   3. `coalesce()` function (VERIFIED ABSENT: grep `src/graph/cypher/executor/eval.rs`
//!      enumerates `id`, `labels`, `type`, `size`, `tointeger`, `tofloat`, `tostring`,
//!      `count`, `collect`; the `_ => Value::Null` fallthrough swallows any unknown
//!      function to Null, which silently NULL-propagates through `>=`.)
//!
//! # Expected default (per 165-04 plan priors): BRANCH_B_COALESCE_ABSENT
//!
//! The primary form with `coalesce()` is EXPECTED to return zero rows because
//! `coalesce(r.valid_to, 9999999999)` evaluates to `Value::Null` and
//! `Null >= 1000` is Null (non-bool) → the Filter op's `matches!(v, Value::Bool(true))`
//! rejects the row. This is a SILENT failure, not a parse error.
//!
//! # Observed at run time: BRANCH_B_SILENT_NULL_SWALLOW + FALLBACK_VACUOUSLY_TRUE
//!
//! Three sub-probes were run with a seeded graph of three nodes (alice, bob,
//! carol) and two edges (alice-KNOWS->bob with valid_to=1500, alice-KNOWS->carol
//! with no valid_to):
//!
//! | Sub-probe                                              | Observed rows |
//! |--------------------------------------------------------|---------------|
//! | A. baseline `MATCH (e) MATCH (e)-[r*1..2]-(n) RETURN`  |            18 |
//! | B. primary `WHERE coalesce(r.valid_to, 9999) >= 1000`  |             0 |
//! | C. fallback `WHERE r.valid_to IS NULL OR r.v_to >= 1`  |            18 |
//!
//! Interpretation:
//!   - Sub-probe B returns 0 because `coalesce()` is unknown → evaluates to
//!     Null → `Null >= 1000` is Null → Filter op drops every row.
//!   - Sub-probe C returns the same count as baseline (18) because `r.valid_to`
//!     is Null (the edge variable is unbound in variable-length expansion), so
//!     `r.valid_to IS NULL` is `Bool(true)` and the OR short-circuits to true
//!     for every row. The predicate is vacuously satisfied — no temporal
//!     filtering actually occurs, even though the query "succeeds".
//!
//! Both observed modes are dangerous for Lunaris: client code sees success and
//! plausible-shape results, but NO temporal semantics are applied. This is why
//! the gap doc REQUIRES client-side BFS + edge-property inspection.
//!
//! # Fallback form (WHERE r.valid_to IS NULL OR r.valid_to >= 1000)
//!
//! Evaluator behavior confirmed at `src/graph/cypher/executor/eval.rs:90-94`:
//! `Expr::IsNull` returns `Bool(true)` when expr evaluates to `Value::Null`.
//! Since `r.valid_to` IS `Null` (edge var unbound), the predicate is always
//! true. No real filtering.
//!
//! # Why this test PASSES despite the underlying feature gap
//!
//! The test's contract is "Moon's behaviour for edge-property temporal
//! filters in variable-length MATCH is DETERMINISTIC and DOCUMENTED." We lock
//! the exact observed row counts (0 for coalesce, == baseline for fallback) so
//! a future Moon change will trip the assertion and force the gap doc to be
//! re-evaluated. The gap doc tells Lunaris: do NOT use either form; fall
//! back to client-side BFS + edge-property inspection.
//!
//! Run:
//!   cargo test --test lunaris_cypher_temporal --no-default-features \
//!        --features runtime-tokio,jemalloc -- --test-threads=1 --nocapture
#![cfg(all(feature = "runtime-tokio", feature = "graph"))]

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::server::listener;
use moon::shard::Shard;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Test server harness — duplicated (not extracted) per Plan 165-03 convention.
// ---------------------------------------------------------------------------

fn build_config(port: u16, num_shards: usize) -> ServerConfig {
    ServerConfig {
        bind: "127.0.0.1".to_string(),
        port,
        databases: 16,
        requirepass: None,
        appendonly: "no".to_string(),
        appendfsync: "everysec".to_string(),
        save: None,
        dir: ".".to_string(),
        dbfilename: "dump.rdb".to_string(),
        appendfilename: "appendonly.aof".to_string(),
        maxmemory: 0,
        maxmemory_policy: "noeviction".to_string(),
        maxmemory_samples: 5,
        shards: num_shards,
        cluster_enabled: false,
        cluster_node_timeout: 15000,
        aclfile: None,
        protected_mode: "no".to_string(),
        acllog_max_len: 128,
        tls_port: 0,
        tls_cert_file: None,
        tls_key_file: None,
        tls_ca_cert_file: None,
        tls_ciphersuites: None,
        disk_offload: "disable".to_string(),
        disk_offload_dir: None,
        disk_offload_threshold: 0.85,
        segment_warm_after: 3600,
        pagecache_size: None,
        checkpoint_timeout: 300,
        checkpoint_completion: 0.9,
        max_wal_size: "256mb".to_string(),
        wal_fpi: "enable".to_string(),
        wal_compression: "lz4".to_string(),
        wal_segment_size: "16mb".to_string(),
        vec_codes_mlock: "enable".to_string(),
        segment_cold_after: 86400,
        segment_cold_min_qps: 0.1,
        vec_diskann_beam_width: 8,
        vec_diskann_cache_levels: 3,
        uring_sqpoll_ms: None,
        admin_port: 0,
        slowlog_log_slower_than: 10000,
        slowlog_max_len: 128,
        check_config: false,
        maxclients: 10000,
        timeout: 0,
        tcp_keepalive: 300,
        console_auth_required: false,
        console_auth_secret: String::new(),
        console_cors_origin: vec![],
        console_rate_limit: 1000.0,
        console_rate_burst: 2000.0,
    }
}

async fn start_moon_sharded(num_shards: usize) -> (u16, CancellationToken) {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let token = CancellationToken::new();
    let config = build_config(port, num_shards);
    let cancel = token.clone();

    std::thread::spawn(move || {
        let mut mesh = ChannelMesh::new(num_shards, CHANNEL_BUFFER_SIZE);
        let conn_txs: Vec<channel::MpscSender<(tokio::net::TcpStream, bool)>> =
            (0..num_shards).map(|i| mesh.conn_tx(i)).collect();
        let all_notifiers = mesh.all_notifiers();
        let all_pubsub_registries: Vec<
            std::sync::Arc<parking_lot::RwLock<moon::pubsub::PubSubRegistry>>,
        > = (0..num_shards)
            .map(|_| {
                std::sync::Arc::new(parking_lot::RwLock::new(moon::pubsub::PubSubRegistry::new()))
            })
            .collect();
        let all_remote_sub_maps: Vec<
            std::sync::Arc<
                parking_lot::RwLock<moon::shard::remote_subscriber_map::RemoteSubscriberMap>,
            >,
        > = (0..num_shards)
            .map(|_| {
                std::sync::Arc::new(parking_lot::RwLock::new(
                    moon::shard::remote_subscriber_map::RemoteSubscriberMap::new(),
                ))
            })
            .collect();

        let affinity_tracker = std::sync::Arc::new(parking_lot::RwLock::new(
            moon::shard::affinity::AffinityTracker::new(),
        ));

        let mut shards: Vec<Shard> = (0..num_shards)
            .map(|id| Shard::new(id, num_shards, config.databases, config.to_runtime_config()))
            .collect();
        let all_dbs: Vec<Vec<moon::storage::Database>> = shards
            .iter_mut()
            .map(|s| std::mem::take(&mut s.databases))
            .collect();
        let shard_databases = moon::shard::shared_databases::ShardDatabases::new(all_dbs);

        let mut shard_handles = Vec::with_capacity(num_shards);
        for (id, mut shard) in shards.into_iter().enumerate() {
            let producers = mesh.take_producers(id);
            let consumers = mesh.take_consumers(id);
            let conn_rx = mesh.take_conn_rx(id);
            let shard_config = config.clone();
            let shard_cancel = cancel.clone();
            let shard_spsc_notify = mesh.take_notify(id);
            let shard_all_notifiers = all_notifiers.clone();
            let shard_dbs = shard_databases.clone();
            let shard_pubsub_regs = all_pubsub_registries.clone();
            let shard_remote_sub_maps = all_remote_sub_maps.clone();
            let shard_affinity = affinity_tracker.clone();

            let handle = std::thread::Builder::new()
                .name(format!("lunaris-v1-shard-{}", id))
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to build shard runtime");

                    let local = tokio::task::LocalSet::new();

                    let (_snap_tx, snap_rx) = channel::watch(0u64);
                    let snap_tx = _snap_tx;
                    let acl_t = std::sync::Arc::new(std::sync::RwLock::new(
                        moon::acl::AclTable::load_or_default(&shard_config),
                    ));
                    let rt_cfg = std::sync::Arc::new(parking_lot::RwLock::new(
                        shard_config.to_runtime_config(),
                    ));
                    rt.block_on(local.run_until(shard.run(
                        conn_rx,
                        None,
                        consumers,
                        producers,
                        shard_cancel,
                        None,
                        None,
                        None,
                        snap_rx,
                        snap_tx,
                        None,
                        None,
                        0,
                        acl_t,
                        rt_cfg,
                        std::sync::Arc::new(shard_config),
                        shard_spsc_notify,
                        shard_all_notifiers,
                        shard_dbs,
                        shard_pubsub_regs,
                        shard_remote_sub_maps,
                        shard_affinity,
                    )));
                })
                .expect("failed to spawn shard thread");
            shard_handles.push(handle);
        }

        let listener_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build listener runtime");

        let listener_cancel = cancel.clone();
        listener_rt.block_on(async {
            if let Err(e) =
                listener::run_sharded(config, conn_txs, listener_cancel, false, affinity_tracker)
                    .await
            {
                eprintln!("Listener error: {}", e);
            }
        });

        cancel.cancel();
        for handle in shard_handles {
            let _ = handle.join();
        }
    });

    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    (port, token)
}

async fn connect(port: u16) -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}

// ---------------------------------------------------------------------------
// Helpers — seed graph + extract row count from GRAPH.QUERY response.
// ---------------------------------------------------------------------------

/// Seed the graph:
///   alice -[KNOWS{valid_from:500, valid_to:1500}]-> bob
///   alice -[KNOWS{valid_from:500}              ]-> carol  (no valid_to)
///
/// Returns (alice_id, bob_id, carol_id).
async fn seed_graph(conn: &mut redis::aio::MultiplexedConnection) -> (i64, i64, i64) {
    // Create the graph.
    let ok: redis::Value = redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async(conn)
        .await
        .expect("GRAPH.CREATE g should succeed");
    // Allow either +OK simple string or bulk "OK" — redis-rs normalises to a variant.
    match ok {
        redis::Value::SimpleString(ref s) if s == "OK" => {}
        redis::Value::BulkString(ref b) if b == b"OK" => {}
        redis::Value::Okay => {}
        other => panic!("GRAPH.CREATE returned unexpected value: {other:?}"),
    }

    // GRAPH.ADDNODE <graph> <label> [<prop> <val>]... — returns Integer(node_id).
    let alice_id: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g")
        .arg("Entity")
        .arg("name")
        .arg("alice")
        .arg("workspace")
        .arg("ws1")
        .query_async(conn)
        .await
        .expect("GRAPH.ADDNODE alice");
    let bob_id: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g")
        .arg("Entity")
        .arg("name")
        .arg("bob")
        .arg("workspace")
        .arg("ws1")
        .query_async(conn)
        .await
        .expect("GRAPH.ADDNODE bob");
    let carol_id: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g")
        .arg("Entity")
        .arg("name")
        .arg("carol")
        .arg("workspace")
        .arg("ws1")
        .query_async(conn)
        .await
        .expect("GRAPH.ADDNODE carol");

    // GRAPH.ADDEDGE <graph> <src> <dst> <type> [<prop> <val>]...
    let _: redis::Value = redis::cmd("GRAPH.ADDEDGE")
        .arg("g")
        .arg(alice_id)
        .arg(bob_id)
        .arg("KNOWS")
        .arg("valid_from")
        .arg(500_i64)
        .arg("valid_to")
        .arg(1500_i64)
        .query_async(conn)
        .await
        .expect("GRAPH.ADDEDGE alice->bob");
    let _: redis::Value = redis::cmd("GRAPH.ADDEDGE")
        .arg("g")
        .arg(alice_id)
        .arg(carol_id)
        .arg("KNOWS")
        .arg("valid_from")
        .arg(500_i64)
        .query_async(conn)
        .await
        .expect("GRAPH.ADDEDGE alice->carol");

    (alice_id, bob_id, carol_id)
}

/// A GRAPH.QUERY response is `Array[ headers_array, rows_array, stats_bulk ]`.
/// Return the row count (len of `rows_array`).
fn rows_count(v: &redis::Value) -> usize {
    match v {
        redis::Value::Array(items) if items.len() >= 2 => match &items[1] {
            redis::Value::Array(rows) => rows.len(),
            other => panic!("expected rows Array in position [1], got {other:?}"),
        },
        other => panic!("expected outer Array with >=2 elements from GRAPH.QUERY, got {other:?}"),
    }
}

/// Collect the stringified values from each row (flattened). Used for
/// diagnostic `{neighbor.name}` extraction.
fn rows_flat_strings(v: &redis::Value) -> Vec<String> {
    let mut out = Vec::new();
    if let redis::Value::Array(items) = v
        && let Some(redis::Value::Array(rows)) = items.get(1)
    {
        for row in rows {
            if let redis::Value::Array(cells) = row {
                for cell in cells {
                    match cell {
                        redis::Value::BulkString(b) => {
                            if let Ok(s) = std::str::from_utf8(b) {
                                    out.push(s.to_string());
                                }
                            }
                            redis::Value::SimpleString(s) => out.push(s.clone()),
                            _ => {}
                        }
                    }
                }
            }
        }
    out
}

// ---------------------------------------------------------------------------
// Primary V1 assertion — cypher_edge_property_temporal_filter_in_nested_match.
//
// Three sub-probes, all with deterministic outcomes:
//   A. Baseline MATCH without any WHERE clause — locks the traversal shape
//      (alice has exactly two 1..2-hop neighbours → 2 rows).
//   B. Primary form — `WHERE coalesce(r.valid_to, 9999999999) >= 1000` —
//      EXPECTED silent swallow to 0 rows (Branch B — coalesce absent,
//      NULL-propagation).
//   C. Fallback form — `WHERE r.valid_to IS NULL OR r.valid_to >= 1000` —
//      ALSO expected to return 0 rows because the edge variable `r` is not
//      bound in variable-length expansion (Expand op inserts only the target
//      node into the row, see `src/graph/cypher/executor/read.rs:151`).
//
// The test PASSES when all three assertions hold — this proves Moon's
// observed behaviour is deterministic. The gap document records the
// behaviour and the Lunaris client-side fallback.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cypher_edge_property_temporal_filter_in_nested_match() {
    let (port, shutdown) = start_moon_sharded(1).await;
    let mut conn = connect(port).await;

    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();

    let (_alice, _bob, _carol) = seed_graph(&mut conn).await;

    // ── Sub-probe A: baseline traversal (no WHERE) locks the shape ──────────
    let baseline: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg(
            "MATCH (e:Entity {name:'alice'}) \
             MATCH (e)-[r*1..2]-(neighbor) \
             RETURN e.name, neighbor.name",
        )
        .query_async(&mut conn)
        .await
        .expect("baseline GRAPH.QUERY must succeed");
    let baseline_rows = rows_count(&baseline);
    let baseline_flat = rows_flat_strings(&baseline);
    println!(
        "[V1 baseline] rows={} values={:?}",
        baseline_rows, baseline_flat
    );
    assert!(
        baseline_rows >= 2,
        "baseline MATCH (e)-[r*1..2]-(neighbor) must return at least 2 rows \
         (alice -> bob, alice -> carol); got {baseline_rows} rows, values={baseline_flat:?}"
    );

    // ── Sub-probe B: primary form with `coalesce` — POST-v0.1.9 CYP-03 ─────────
    //
    // v0.1.9 CYP-03 added `coalesce()` to the eval registry
    // (`src/graph/cypher/executor/eval.rs:209`). For variable-length
    // expansion `r*1..2` the edge variable is still unbound (CYP-06
    // variable-length case deferred to v0.2), so `r.valid_to` resolves
    // to Null for every row. `coalesce(Null, 9999999999)` now correctly
    // returns `9999999999`, and the predicate `9999999999 >= 1000` is
    // `true`, so every row passes — matching the baseline count.
    //
    // When v0.2 lands multi-hop edge-var binding as `Value::Path`,
    // this assertion flips again (rows become the temporally-filtered
    // subset).
    let primary: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg(
            "MATCH (e:Entity {name:'alice'}) \
             MATCH (e)-[r*1..2]-(neighbor) \
             WHERE coalesce(r.valid_to, 9999999999) >= 1000 \
             RETURN e.name, neighbor.name",
        )
        .query_async(&mut conn)
        .await
        .expect(
            "Moon must accept the coalesce() Cypher as a well-formed query (parses as FunctionCall)",
        );
    let primary_rows = rows_count(&primary);
    println!("[V1 coalesce post-v0.1.9] rows={primary_rows}");
    assert_eq!(
        primary_rows, baseline_rows,
        "v0.1.9 CYP-03 lock-in: coalesce() is now present, so coalesce(Null, 9999999999) \
         = 9999999999 >= 1000 = true for every row (variable-length edge-var still \
         unbound per CYP-06 v0.2 scope). Any other count means v0.2 multi-hop \
         edge-var binding landed or coalesce changed — update this test and the gap doc."
    );

    // ── Sub-probe C: fallback form (`IS NULL OR`) — VACUOUSLY TRUE, no filtering ─
    //
    // Edge variable `r` is not bound by variable-length Expand (see
    // `src/graph/cypher/executor/read.rs:126-168`; only the target node is
    // inserted into the row). `r.valid_to` therefore evaluates to Null for
    // every row. The evaluator at `src/graph/cypher/executor/eval.rs:90-94`
    // returns `Bool(true)` for `Null IS NULL`, so the OR short-circuits to
    // true for EVERY row. Net result: the fallback form passes the same
    // number of rows as the baseline (no actual temporal filtering occurs).
    //
    // This is the second dangerous failure mode documented in the gap doc:
    // the fallback "succeeds" with plausible-shape results, but temporal
    // semantics are completely absent.
    //
    // To lock the shape in an apples-to-apples way we reuse the same
    // two-MATCH pattern as the baseline (single-MATCH collapses paths
    // differently inside the planner).
    let fallback: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg(
            "MATCH (e:Entity {name:'alice'}) \
             MATCH (e)-[r*1..2]-(neighbor) \
             WHERE r.valid_to IS NULL OR r.valid_to >= 1000 \
             RETURN e.name, neighbor.name",
        )
        .query_async(&mut conn)
        .await
        .expect("fallback GRAPH.QUERY (no coalesce) must parse and execute");
    let fallback_rows = rows_count(&fallback);
    println!("[V1 fallback] rows={fallback_rows}");
    assert_eq!(
        fallback_rows, baseline_rows,
        "Branch B lock-in (fallback is vacuously true): the fallback predicate \
         MUST pass every row the baseline produces, because `r.valid_to` is always \
         Null (edge var unbound in variable-length expansion) so `r.valid_to IS NULL` \
         is Bool(true) and the OR short-circuits to true. Any other count means \
         Moon now binds edge vars in Expand OR changed IS NULL semantics — update \
         this test AND the gap doc."
    );

    shutdown.cancel();
}
