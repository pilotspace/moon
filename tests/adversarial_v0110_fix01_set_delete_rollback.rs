//! Phase 174 FIX-01 — Adversarial RED→GREEN coverage for TXN.ABORT rollback of
//! Cypher SET / DELETE / MERGE ON MATCH SET mutations.
//!
//! Source: merged_bug_015 (ultrareview of v0.1.9 / commit 2e397c7). Before this
//! fix shipped, `MutationRecord` only covered `CreateNode` / `CreateEdge`, so
//! `PhysicalOp::SetProperties` and `PhysicalOp::DeleteEntities` mutated
//! `write_buf` without emitting any record — `TXN.ABORT` silently skipped them
//! and the mutations "stuck". This test suite asserts the pre-mutation
//! state is fully restored after `TXN.ABORT` for each of the three uncovered
//! physical ops plus the MERGE-on-match-SET path.
//!
//! Tests (each asserts exact pre-state value, not just "command didn't error"):
//!   1. `test_txn_abort_rolls_back_cypher_set` — SET n.age = 99 → ABORT → age
//!      is 35 (pre-state), not 99.
//!   2. `test_txn_abort_rolls_back_cypher_delete_node` — DETACH DELETE n →
//!      ABORT → node still present with its labels + properties.
//!   3. `test_txn_abort_rolls_back_cypher_delete_edge` — DELETE r → ABORT →
//!      edge still present with src/dst/type.
//!   4. `test_txn_abort_rolls_back_merge_on_match_set` — MERGE ... ON MATCH
//!      SET n.age = 88 → ABORT → age is 35 (pre-state).
//!
//! Runtime: `cargo test --no-default-features --features runtime-tokio,jemalloc
//! --test adversarial_v0110_fix01_set_delete_rollback -- --test-threads=1`

#![cfg(all(feature = "runtime-tokio", feature = "graph"))]

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::server::listener;
use moon::shard::Shard;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Test server infrastructure — mirrors `tests/txn_cypher_write_rollback.rs`.
// Kept file-local because no shared harness module exists.
// ---------------------------------------------------------------------------

async fn start_txn_server(num_shards: usize) -> (u16, CancellationToken) {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let token = CancellationToken::new();

    let config = ServerConfig {
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
    };

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
                .name(format!("fix01-shard-{}", id))
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
// Value extraction helpers — pull the first scalar cell out of a GRAPH.QUERY
// response shaped as Array[headers, rows, stats]. Tests assert_eq! on the
// concrete value rather than fuzzy "contains" checks.
// ---------------------------------------------------------------------------

/// Extract the first row's first column as i64. Returns None if absent / wrong
/// shape / not an integer.
fn first_i64(v: &redis::Value) -> Option<i64> {
    let redis::Value::Array(items) = v else { return None };
    if items.len() < 2 {
        return None;
    }
    let redis::Value::Array(rows) = &items[1] else { return None };
    let first_row = rows.first()?;
    let redis::Value::Array(cells) = first_row else { return None };
    let first_cell = cells.first()?;
    match first_cell {
        redis::Value::Int(n) => Some(*n),
        redis::Value::BulkString(b) => std::str::from_utf8(b).ok()?.parse().ok(),
        redis::Value::SimpleString(s) => s.parse().ok(),
        _ => None,
    }
}

/// Number of rows in a GRAPH.QUERY response (Array[headers, rows, stats]).
fn row_count(v: &redis::Value) -> usize {
    let redis::Value::Array(items) = v else { return 0 };
    if items.len() < 2 {
        return 0;
    }
    match &items[1] {
        redis::Value::Array(rows) => rows.len(),
        _ => 0,
    }
}

// ===========================================================================
// TEST 1: TXN.ABORT rolls back Cypher SET on a node property.
// Pre-state: Alice with {age: 35}. Txn: SET n.age = 99. Abort must restore 35.
// ===========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_txn_abort_rolls_back_cypher_set() {
    let (port, shutdown) = start_txn_server(1).await;
    let mut conn = connect(port).await;

    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.CREATE");

    // Seed Alice outside any txn.
    let _: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("CREATE (:Person {name:'Alice', age:35})")
        .query_async(&mut conn)
        .await
        .expect("seed Alice");

    // Pre-state check: age == 35.
    let pre: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (n:Person {name:'Alice'}) RETURN n.age")
        .query_async(&mut conn)
        .await
        .expect("pre-check");
    assert_eq!(
        first_i64(&pre),
        Some(35),
        "pre-state must be age=35. Resp={pre:?}"
    );

    // Open txn, flip age to 99 via SET.
    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN");

    let _: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (n:Person {name:'Alice'}) SET n.age = 99 RETURN n.age")
        .query_async(&mut conn)
        .await
        .expect("SET age=99 in txn");

    let _: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN.ABORT");

    // Post-abort: age MUST be 35 (pre-state restored), not 99.
    let post: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (n:Person {name:'Alice'}) RETURN n.age")
        .query_async(&mut conn)
        .await
        .expect("post-abort check");
    assert_eq!(
        first_i64(&post),
        Some(35),
        "TXN.ABORT must restore age=35 after SET n.age=99. Found age={post:?}. \
         FIX-01: MutationRecord::SetProperty missing — SET leaks past rollback."
    );

    shutdown.cancel();
}

// ===========================================================================
// TEST 2: TXN.ABORT rolls back Cypher DETACH DELETE on a node.
// Pre-state: node :Person {name:'Bob', age:40}. Txn deletes the node. Abort
// must restore the node with its labels and properties.
// ===========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_txn_abort_rolls_back_cypher_delete_node() {
    let (port, shutdown) = start_txn_server(1).await;
    let mut conn = connect(port).await;

    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.CREATE");

    // Seed Bob.
    let _: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("CREATE (:Person {name:'Bob', age:40})")
        .query_async(&mut conn)
        .await
        .expect("seed Bob");

    // Pre-state: Bob present with age 40.
    let pre: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (n:Person {name:'Bob'}) RETURN n.age")
        .query_async(&mut conn)
        .await
        .expect("pre-check");
    assert_eq!(row_count(&pre), 1, "Bob must exist pre-txn. Resp={pre:?}");
    assert_eq!(first_i64(&pre), Some(40));

    // Txn: DETACH DELETE Bob.
    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN");

    let _: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (n:Person {name:'Bob'}) DETACH DELETE n")
        .query_async(&mut conn)
        .await
        .expect("DELETE Bob in txn");

    let _: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN.ABORT");

    // Post-abort: Bob must still be present, with age 40.
    let post: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (n:Person {name:'Bob'}) RETURN n.age")
        .query_async(&mut conn)
        .await
        .expect("post-abort check");
    assert_eq!(
        row_count(&post),
        1,
        "TXN.ABORT must restore Bob. Found 0 rows — FIX-01: MutationRecord::DeleteNode missing."
    );
    assert_eq!(
        first_i64(&post),
        Some(40),
        "TXN.ABORT must restore Bob with age=40. Resp={post:?}"
    );

    shutdown.cancel();
}

// ===========================================================================
// TEST 3: TXN.ABORT rolls back Cypher DELETE on an edge.
// Pre-state: Alice-[:KNOWS]->Bob. Txn deletes r. Abort must restore the edge.
// ===========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_txn_abort_rolls_back_cypher_delete_edge() {
    let (port, shutdown) = start_txn_server(1).await;
    let mut conn = connect(port).await;

    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.CREATE");

    // Seed Alice-[:KNOWS]->Bob.
    let _: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("CREATE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'})")
        .query_async(&mut conn)
        .await
        .expect("seed edge");

    // Pre-state: the KNOWS edge exists.
    let pre: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg(
            "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) RETURN a.name",
        )
        .query_async(&mut conn)
        .await
        .expect("pre-check edge");
    assert_eq!(
        row_count(&pre),
        1,
        "edge must exist pre-txn. Resp={pre:?}"
    );

    // Txn: delete the edge.
    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN");

    let _: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) DELETE r")
        .query_async(&mut conn)
        .await
        .expect("DELETE edge in txn");

    // Intermediate assertion: inside the txn, the edge must be gone. This
    // separates two latent defects:
    //   (i) write-path Expand ignoring `edge_variable` → DELETE r is a
    //       silent no-op. If this fires, the "rollback" is vacuous and the
    //       test below would pass for the wrong reason. Fixed in GREEN as
    //       part of making DELETE r observable.
    //   (ii) MutationRecord::DeleteEdge absent → rollback skips the edge.
    //        Checked post-ABORT below.
    let mid: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg(
            "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) RETURN a.name",
        )
        .query_async(&mut conn)
        .await
        .expect("mid-txn edge check");
    assert_eq!(
        row_count(&mid),
        0,
        "DELETE r inside TXN must soft-delete the edge. Found {} row(s) — write-path Expand ignores edge_variable, so DELETE r is a silent no-op today. Resp={mid:?}",
        row_count(&mid),
    );

    let _: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN.ABORT");

    // Post-abort: the KNOWS edge must be restored.
    let post: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg(
            "MATCH (a:Person {name:'Alice'})-[r:KNOWS]->(b:Person {name:'Bob'}) RETURN a.name",
        )
        .query_async(&mut conn)
        .await
        .expect("post-abort edge check");
    assert_eq!(
        row_count(&post),
        1,
        "TXN.ABORT must restore the :KNOWS edge. Found 0 rows — FIX-01: MutationRecord::DeleteEdge missing."
    );

    shutdown.cancel();
}

// ===========================================================================
// TEST 4: TXN.ABORT rolls back MERGE ON MATCH SET.
// Pre-state: Alice age=35. Txn: MERGE (n {name:'Alice'}) ON MATCH SET n.age = 88.
// Abort must restore age=35 (the match branch ran SET — roll it back).
// ===========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_txn_abort_rolls_back_merge_on_match_set() {
    let (port, shutdown) = start_txn_server(1).await;
    let mut conn = connect(port).await;

    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.CREATE");

    let _: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("CREATE (:Person {name:'Alice', age:35})")
        .query_async(&mut conn)
        .await
        .expect("seed Alice");

    // Pre-state.
    let pre: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (n:Person {name:'Alice'}) RETURN n.age")
        .query_async(&mut conn)
        .await
        .expect("pre-check");
    assert_eq!(first_i64(&pre), Some(35));

    // Txn: MERGE on-match flips age to 88.
    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN");

    let _: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MERGE (n:Person {name:'Alice'}) ON MATCH SET n.age = 88 RETURN n.age")
        .query_async(&mut conn)
        .await
        .expect("MERGE ON MATCH SET in txn");

    let _: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN.ABORT");

    // Post-abort: age must be 35 again.
    let post: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (n:Person {name:'Alice'}) RETURN n.age")
        .query_async(&mut conn)
        .await
        .expect("post-abort check");
    assert_eq!(
        first_i64(&post),
        Some(35),
        "TXN.ABORT must restore age=35 after MERGE ON MATCH SET n.age=88. Resp={post:?}. \
         FIX-01: MutationRecord::SetProperty missing in the MERGE-on-match path."
    );

    shutdown.cancel();
}
