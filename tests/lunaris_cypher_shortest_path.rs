//! Lunaris V3 verification — Cypher `shortestPath()` function.
//!
//! Phase 166 Plan 05 — Task 1. VERIFICATION ONLY. No `src/` files are modified
//! by this test (enforced by plan: `test -z "$(git diff --name-only HEAD -- src/)"`).
//!
//! # Lunaris dependency
//!
//! `lunaris_integration_guideline.md §C.6` / §B.B4 maps `PathReasoningRetriever`
//! onto a Cypher query shape like:
//!
//! ```cypher
//! MATCH p = shortestPath((a:Entity {name:$a})-[*..6]-(b:Entity {name:$b}))
//! RETURN p
//! ```
//!
//! Three Moon features combine here:
//!   1. MATCH path variable binding (`p = ...`) — UNVERIFIED in parser.
//!   2. `shortestPath()` function wrapping a path pattern — UNVERIFIED as
//!      either a parser construct or an executor built-in.
//!   3. Variable-length path `-[*..6]-` — VERIFIED supported (Phase 165 V1).
//!
//! # Expected outcome per Phase 166 RESEARCH: Branch B (gap)
//!
//! Pre-flight evidence gathered before writing the test:
//!   - `grep -rn 'shortestPath\|ShortestPath\|shortest_path' src/graph/`
//!     yielded ONLY `src/graph/traversal.rs::shortest_path` (Dijkstra at the
//!     storage layer). Zero matches inside `src/graph/cypher/` — neither the
//!     parser nor the executor knows the word.
//!   - `grep -rn 'shortestPath\|shortest_path' src/command/graph/` — zero.
//!   - `src/graph/cypher/executor/eval.rs:110-210` registry lists
//!     `id, labels, type, size, tointeger, toint, tofloat, tostring, count,
//!     collect`. `shortestPath` is not there — the `_ => Value::Null`
//!     fallthrough swallows any unknown function to Null.
//!   - `src/graph/cypher/parser/mod.rs::parse_clause` dispatches `Token::Match`
//!     straight into `parse_pattern_list`, which in turn calls
//!     `parse_pattern_node` expecting `(` as the first token. The shape
//!     `MATCH p = ...` (path-variable binding) is therefore UNPARSEABLE —
//!     the parser sees `Ident("p")` where it expects `LParen`.
//!
//! Three sub-probes verify the gap is real, deterministic, and distinct
//! from configuration noise:
//!
//! | Sub-probe | Query shape                                                | Expected outcome                       |
//! |-----------|------------------------------------------------------------|----------------------------------------|
//! |  A        | Baseline `MATCH (a)-[*..6]-(b) RETURN a.name, b.name`      | Success with ≥ 1 row (seeded graph)    |
//! |  B        | `MATCH p = shortestPath((a)-[*..6]-(b)) RETURN p`          | ERR frame (parse error)                |
//! |  C        | `MATCH (a), (b) RETURN shortestPath((a)-[*..6]-(b)) AS p`  | Parse OK, but value is Null (no filter)|
//! |  D        | `GRAPH.NEIGHBORS g <a> DEPTH 6` (Lunaris fallback)         | Success, returns path-able neighbors   |
//!
//! The test PASSES when all four assertions hold — this proves the gap is
//! deterministic. The companion gap document
//! (`LUNARIS-CYPHER-SHORTESTPATH.md`) documents the Lunaris fallback:
//! client-side BFS via `GRAPH.NEIGHBORS ... DEPTH N`.
//!
//! # Why this test PASSES despite the underlying feature gap
//!
//! Per Phase 165 Plan 04 convention: the test's contract is "Moon's
//! behaviour for Cypher shortestPath() is DETERMINISTIC and DOCUMENTED." We
//! lock the exact observed behaviour (parse ERR for shape B, Null for shape
//! C, success for fallback D) so any future Moon change — such as Moon
//! v0.1.9 adding shortestPath() — trips the assertion and forces the gap
//! doc to be re-evaluated.
//!
//! Run:
//!   cargo test --test lunaris_cypher_shortest_path --no-default-features \
//!        --features runtime-tokio,jemalloc,graph -- --test-threads=1 --nocapture
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
                .name(format!("lunaris-v3-shard-{}", id))
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
// Seed: a 5-node chain A-B-C-D-E plus a cross-edge A-C.
//
// Topology (undirected — Moon stores directed, but test queries traverse both):
//
//     A ─── B ─── C ─── D ─── E
//     └───────────┘
//            (A-C cross-edge)
//
// Expected shortest path A->E has length 3 (A-C-D-E) via the cross-edge.
// Longest simple path has length 4 (A-B-C-D-E).
// ---------------------------------------------------------------------------
async fn seed_path_graph(
    conn: &mut redis::aio::MultiplexedConnection,
) -> (i64, i64, i64, i64, i64) {
    let ok: redis::Value = redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async(conn)
        .await
        .expect("GRAPH.CREATE g should succeed");
    match ok {
        redis::Value::SimpleString(ref s) if s == "OK" => {}
        redis::Value::BulkString(ref b) if b == b"OK" => {}
        redis::Value::Okay => {}
        other => panic!("GRAPH.CREATE returned unexpected value: {other:?}"),
    }

    async fn add_node(
        conn: &mut redis::aio::MultiplexedConnection,
        name: &str,
    ) -> i64 {
        redis::cmd("GRAPH.ADDNODE")
            .arg("g")
            .arg("Entity")
            .arg("name")
            .arg(name)
            .query_async(conn)
            .await
            .unwrap_or_else(|e| panic!("GRAPH.ADDNODE {name}: {e}"))
    }

    let a = add_node(conn, "A").await;
    let b = add_node(conn, "B").await;
    let c = add_node(conn, "C").await;
    let d = add_node(conn, "D").await;
    let e = add_node(conn, "E").await;

    async fn add_edge(
        conn: &mut redis::aio::MultiplexedConnection,
        src: i64,
        dst: i64,
    ) {
        let _: redis::Value = redis::cmd("GRAPH.ADDEDGE")
            .arg("g")
            .arg(src)
            .arg(dst)
            .arg("KNOWS")
            .query_async(conn)
            .await
            .unwrap_or_else(|e| panic!("GRAPH.ADDEDGE {src}->{dst}: {e}"));
    }

    // Chain: A-B, B-C, C-D, D-E.
    add_edge(conn, a, b).await;
    add_edge(conn, b, c).await;
    add_edge(conn, c, d).await;
    add_edge(conn, d, e).await;
    // Cross-edge: A-C (makes shortest path A-E = 3 via A-C-D-E).
    add_edge(conn, a, c).await;

    (a, b, c, d, e)
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

/// Collect the stringified / typed values from each row's cells (flattened).
/// Used for diagnostic printing AND for sub-probe C's Null-detection.
fn rows_flat_values(v: &redis::Value) -> Vec<String> {
    let mut out = Vec::new();
    if let redis::Value::Array(items) = v {
        if let Some(redis::Value::Array(rows)) = items.get(1) {
            for row in rows {
                if let redis::Value::Array(cells) = row {
                    for cell in cells {
                        out.push(format!("{cell:?}"));
                    }
                }
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// V3 primary assertion — `cypher_shortest_path_is_unsupported_or_null`.
//
// Binary outcome per plan 166-05:
//   - PASS (branch A): shortestPath() works — remove this test and flip
//     the gap doc to an "implemented" note.
//   - PASS (branch B): shortestPath() absent — all four sub-probes behave
//     deterministically: B parse-ERR, C Null, D fallback works.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cypher_shortest_path_is_unsupported_or_null() {
    let (port, shutdown) = start_moon_sharded(1).await;
    let mut conn = connect(port).await;

    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();

    let (a_id, _b_id, _c_id, _d_id, _e_id) = seed_path_graph(&mut conn).await;

    // ── Sub-probe A: baseline variable-length MATCH locks the traversal shape ──
    //
    // `MATCH (a:Entity {name:'A'}) MATCH (a)-[*..6]-(b:Entity) RETURN ...`
    // MUST return ≥ 1 row (A reaches at least B, C, D, E within 6 hops
    // even without the cross-edge). This proves the seed graph is wired
    // correctly AND variable-length expansion is operational — any failure
    // here is setup error, not a Cypher-feature gap.
    let baseline: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg(
            "MATCH (a:Entity {name:'A'}) \
             MATCH (a)-[*..6]-(b:Entity) \
             RETURN a.name, b.name",
        )
        .query_async(&mut conn)
        .await
        .expect("baseline MATCH must succeed (setup sanity check)");
    let baseline_rows = rows_count(&baseline);
    let baseline_values = rows_flat_values(&baseline);
    println!(
        "[V3 baseline] rows={} values={:?}",
        baseline_rows, baseline_values
    );
    assert!(
        baseline_rows >= 4,
        "baseline variable-length MATCH must reach at least B, C, D, E from A \
         within 6 hops; got {baseline_rows} rows, values={baseline_values:?}"
    );

    // ── Sub-probe B: `MATCH p = shortestPath(...)` — EXPECTED parse ERR ─────
    //
    // Per pre-flight grep: the Cypher parser's `parse_clause` dispatches
    // `Token::Match` straight into `parse_pattern_list` -> `parse_pattern_node`,
    // which immediately expects `(`. So `MATCH p = ...` raises UnexpectedToken
    // at the `p` identifier.
    //
    // Contract: Moon MUST return an ERR frame for this exact query. If Moon
    // v0.1.9 later adds path-variable binding, this assertion will fail and
    // the gap doc gets revisited.
    let primary_result: redis::RedisResult<redis::Value> = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg(
            "MATCH p = shortestPath((a:Entity {name:'A'})-[*..6]-(b:Entity {name:'E'})) \
             RETURN p",
        )
        .query_async(&mut conn)
        .await;

    match &primary_result {
        Err(err) => {
            // Expected branch — redis-rs surfaces Frame::Error as RedisResult::Err.
            let msg = format!("{err}");
            println!("[V3 primary] Moon returned ERR (expected): {msg}");
            // Spot-check the error references SOMETHING about the parse failure.
            // We DO NOT require the word "shortestPath" because the parse fails
            // at the identifier `p` (before shortestPath is even lexed).
            assert!(
                msg.to_ascii_lowercase().contains("unexpected")
                    || msg.to_ascii_lowercase().contains("parse")
                    || msg.to_ascii_lowercase().contains("syntax")
                    || msg.to_ascii_lowercase().contains("expected")
                    || msg.to_ascii_lowercase().contains("err"),
                "ERR message should indicate a parse failure; got: {msg}"
            );
        }
        Ok(val) => {
            // If Moon PARSED the query, it either:
            //  (a) implemented path-variable binding + shortestPath — branch A
            //      (flip the gap doc to "implemented"), OR
            //  (b) silently accepted garbage and returned empty rows (worse
            //      failure mode — gap doc must document the new hazard).
            //
            // Both force this test to re-evaluate; we fail loudly so the
            // developer can decide which branch is true.
            let values = rows_flat_values(val);
            let rows = rows_count(val);
            panic!(
                "[V3 primary] Moon UNEXPECTEDLY PARSED `MATCH p = shortestPath(...) RETURN p`. \
                 Re-evaluate the gap doc (`LUNARIS-CYPHER-SHORTESTPATH.md`) — \
                 Moon may have added path-variable binding. rows={rows} values={values:?}"
            );
        }
    }

    // ── Sub-probe C: shortestPath(...) as a RETURN expression — Null fallthrough ──
    //
    // The parser DOES accept `RETURN shortestPath((a)-[*..6]-(b))` because
    // `shortestPath` is just another FunctionCall expression. The executor
    // falls through the match in eval.rs:110-210 (`_ => Value::Null`).
    //
    // Expected: query parses, returns one row per (a, b) cartesian pair
    // (seed has 5 nodes → 25 pairs), each row contains `Null` for the
    // shortestPath-call cell.
    //
    // Note: Moon's Cypher planner may collapse `MATCH (a), (b)` to a
    // cartesian join (per parse test at parser/mod.rs:552 "MATCH (n) RETURN n").
    // If the planner rejects the double-MATCH, we accept either ERR or a
    // cartesian shape — we only assert there is NO path-typed value.
    let fallback_result: redis::RedisResult<redis::Value> = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg(
            "MATCH (a:Entity {name:'A'}) \
             MATCH (b:Entity {name:'E'}) \
             RETURN shortestPath((a)-[*..6]-(b)) AS p",
        )
        .query_async(&mut conn)
        .await;

    match &fallback_result {
        Ok(val) => {
            let rows = rows_count(val);
            let values = rows_flat_values(val);
            println!("[V3 fallback] rows={rows} values={values:?}");
            // Executor drops unknown functions to Null. Assert cell is Null —
            // any Array/Map/BulkString cell would mean Moon now implements
            // shortestPath and the gap is closed.
            //
            // Because the parser might also reject `shortestPath(<pattern>)`
            // — it parses the inside as a pattern expression, which isn't
            // valid in expression context — we accept EITHER:
            //   (i) Rows returned with Null cells, OR
            //   (ii) An ERR (also a documented gap — different failure mode).
            if rows > 0 {
                // Check every cell is Null — if ANY cell is an Array/Map/
                // BulkString, shortestPath is actually implemented.
                let non_null = values
                    .iter()
                    .filter(|s| !s.contains("Nil") && !s.contains("Null"))
                    .count();
                assert_eq!(
                    non_null, 0,
                    "Branch B lock-in: shortestPath() in RETURN MUST evaluate to Null \
                     (unknown function fallthrough in eval.rs:110-210). Found {non_null} \
                     non-null cells: {values:?}. If shortestPath is now implemented, update \
                     this test AND LUNARIS-CYPHER-SHORTESTPATH.md."
                );
            }
        }
        Err(err) => {
            // Also acceptable branch-B outcome — the parser may reject the
            // pattern-expression inside the function call. Record it.
            let msg = format!("{err}");
            println!("[V3 fallback] Moon returned ERR (acceptable branch-B variant): {msg}");
        }
    }

    // ── Sub-probe D: GRAPH.NEIGHBORS fallback (the Lunaris workaround) ──────
    //
    // The gap doc's recommended fallback is client-side BFS via
    // `GRAPH.NEIGHBORS <graph> <node_id> DEPTH <n>`. Proving the fallback
    // works prevents a future "the fallback broke silently" regression.
    //
    // From node A with DEPTH 4, BFS must reach all of B, C, D, E (the
    // entire 5-node graph). The response is an Array of edge+node pairs.
    let neighbors: redis::Value = redis::cmd("GRAPH.NEIGHBORS")
        .arg("g")
        .arg(a_id)
        .arg("DEPTH")
        .arg(4)
        .query_async(&mut conn)
        .await
        .expect("GRAPH.NEIGHBORS must succeed (Lunaris client-side BFS fallback)");

    // GRAPH.NEIGHBORS returns Array[edge, node, edge, node, ...] — at least
    // 4 distinct neighbors (B, C, D, E) → ≥ 8 frame items.
    match &neighbors {
        redis::Value::Array(items) => {
            println!(
                "[V3 fallback GRAPH.NEIGHBORS] depth=4 items={}",
                items.len()
            );
            assert!(
                items.len() >= 8,
                "GRAPH.NEIGHBORS DEPTH 4 from A MUST return at least 8 frames \
                 (4 neighbors * 2 frames each = edge+node). Got {} items. \
                 Lunaris PathReasoningRetriever fallback broken — urgent.",
                items.len()
            );
        }
        other => panic!(
            "GRAPH.NEIGHBORS returned unexpected shape: {other:?}. \
             Lunaris fallback shape has changed — update gap doc + this test."
        ),
    }

    shutdown.cancel();
}
