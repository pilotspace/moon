//! Phase 167 (CYP-01/02) red-green TDD: GRAPH.QUERY CREATE/MERGE writes
//! must be captured on `CrossStoreTxn` so `TXN.ABORT` rolls them back.
//!
//! Closes the v0.1.8 TODO(ACID-08 follow-up) at handler_sharded.rs:2817 and
//! handler_monoio.rs:3298 — previously only explicit GRAPH.ADDNODE /
//! GRAPH.ADDEDGE produced abort intents; Cypher CREATE / MERGE inside
//! GRAPH.QUERY leaked past rollback.
//!
//! Three scenarios:
//!   1. `test_txn_abort_reverts_cypher_create` — CREATE clauses inside a txn
//!      leave 0 nodes of that label after TXN.ABORT. Nodes created OUTSIDE
//!      the txn must survive.
//!   2. `test_txn_abort_reverts_cypher_merge_create_branch` — MERGE that
//!      had to create (no existing match) rolls back completely.
//!   3. `test_txn_abort_cypher_merge_match_branch_noop` — MERGE that hit
//!      the match-branch emits no write intents and therefore leaves the
//!      matched node intact after TXN.ABORT.
//!
//! Mirrors the `start_txn_server` harness from `tests/txn_graph_wiring.rs`
//! (duplicated per-file rather than via a shared module since no such
//! module exists yet).
//!
//! Run:
//!   cargo test --test txn_cypher_write_rollback \
//!        --no-default-features --features runtime-tokio,jemalloc,graph,text-index \
//!        -- --test-threads=1

#![cfg(all(feature = "runtime-tokio", feature = "graph"))]

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::server::listener;
use moon::shard::Shard;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Test server infrastructure (duplicated from tests/txn_graph_wiring.rs;
// the helper is file-local there too).
// ---------------------------------------------------------------------------

async fn start_txn_server(num_shards: usize, persistence_dir: &str) -> (u16, CancellationToken) {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let token = CancellationToken::new();

    let (appendonly, dir) = if persistence_dir.is_empty() {
        ("no".to_string(), ".".to_string())
    } else {
        ("yes".to_string(), persistence_dir.to_string())
    };

    let config = ServerConfig {
        bind: "127.0.0.1".to_string(),
        port,
        databases: 16,
        requirepass: None,
        appendonly,
        appendfsync: "everysec".to_string(),
        save: None,
        dir,
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
                .name(format!("txn-cypher-shard-{}", id))
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

/// Parse GRAPH.INFO and return the node_count field. Returns 0 if absent.
fn parse_graph_node_count(v: &redis::Value) -> i64 {
    let mut nodes: i64 = 0;
    let lookup = |key: &[u8], val: &redis::Value, nodes: &mut i64| match val {
        redis::Value::Int(n) => {
            if key == b"node_count" {
                *nodes = *n;
            }
        }
        redis::Value::BulkString(b) => {
            if let Ok(s) = std::str::from_utf8(b)
                && let Ok(n) = s.parse::<i64>()
                && key == b"node_count"
            {
                *nodes = n;
            }
        }
        _ => {}
    };
    match v {
        redis::Value::Map(pairs) => {
            for (k, val) in pairs {
                let key_bytes: Option<&[u8]> = match k {
                    redis::Value::BulkString(b) => Some(b.as_slice()),
                    redis::Value::SimpleString(s) => Some(s.as_bytes()),
                    _ => None,
                };
                if let Some(kb) = key_bytes {
                    lookup(kb, val, &mut nodes);
                }
            }
        }
        redis::Value::Array(items) => {
            let mut i = 0;
            while i + 1 < items.len() {
                let key_bytes: Option<&[u8]> = match &items[i] {
                    redis::Value::BulkString(b) => Some(b.as_slice()),
                    redis::Value::SimpleString(s) => Some(s.as_bytes()),
                    _ => None,
                };
                if let Some(kb) = key_bytes {
                    lookup(kb, &items[i + 1], &mut nodes);
                }
                i += 2;
            }
        }
        _ => {}
    }
    nodes
}

/// Extract the row count from a GRAPH.QUERY response:
/// `Array[headers_array, rows_array, stats_bulk]`.
fn graph_query_row_count(v: &redis::Value) -> usize {
    match v {
        redis::Value::Array(items) => {
            if items.len() < 2 {
                return 0;
            }
            match &items[1] {
                redis::Value::Array(rows) => rows.len(),
                _ => 0,
            }
        }
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// Test 1: TXN.ABORT reverts GRAPH.QUERY CREATE. The "seed" entity created
// outside the txn must survive; Persons created inside the txn must be gone.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn test_txn_abort_reverts_cypher_create() {
    let (port, shutdown) = start_txn_server(1, "").await;
    let mut conn = connect(port).await;

    // Pre-seed: create the graph and one :Initial node outside any txn.
    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.CREATE");

    let _: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("CREATE (:Initial {name:'seed'})")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.QUERY CREATE seed");

    // Confirm the seed landed.
    let info_before: redis::Value = redis::cmd("GRAPH.INFO")
        .arg("g")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.INFO before txn");
    assert_eq!(parse_graph_node_count(&info_before), 1);

    // Open txn, create two :Person nodes via Cypher.
    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN");

    let _: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("CREATE (:Person {name:'alice'}) CREATE (:Person {name:'bob'})")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.QUERY CREATE alice+bob inside TXN");

    // Abort — must unwind both Person creates.
    let _: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN.ABORT");

    // MATCH all Persons — must return 0 rows.
    let persons: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (p:Person) RETURN p.name")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.QUERY MATCH persons");
    assert_eq!(
        graph_query_row_count(&persons),
        0,
        "TXN.ABORT must unwind Cypher CREATE (Persons). Found rows — intent capture missing. Resp={persons:?}"
    );

    // Seed must survive.
    let initial: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (n:Initial) RETURN n.name")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.QUERY MATCH initial");
    assert_eq!(
        graph_query_row_count(&initial),
        1,
        "Pre-txn :Initial seed must survive TXN.ABORT. Resp={initial:?}"
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 2: MERGE create-branch (no existing match) must roll back the
// freshly-created node on TXN.ABORT.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn test_txn_abort_reverts_cypher_merge_create_branch() {
    let (port, shutdown) = start_txn_server(1, "").await;
    let mut conn = connect(port).await;

    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.CREATE");

    // Start txn. MERGE a node that does NOT yet exist — create-branch.
    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN");

    let merge_resp: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MERGE (n:Person {name:'carol'}) RETURN n")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.QUERY MERGE create-branch inside TXN");
    assert_eq!(
        graph_query_row_count(&merge_resp),
        1,
        "MERGE inside TXN should return 1 row"
    );

    let _: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN.ABORT");

    let persons: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (p:Person) RETURN p.name")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.QUERY MATCH persons post-abort");
    assert_eq!(
        graph_query_row_count(&persons),
        0,
        "TXN.ABORT must unwind MERGE create-branch. Resp={persons:?}"
    );

    let info: redis::Value = redis::cmd("GRAPH.INFO")
        .arg("g")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.INFO");
    assert_eq!(
        parse_graph_node_count(&info),
        0,
        "graph must have 0 nodes after TXN.ABORT over MERGE create-branch"
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 3: MERGE match-branch (existing node found) emits no write intents.
// The matched node was created OUTSIDE the transaction; TXN.ABORT must
// leave it untouched (no spurious rollback).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn test_txn_abort_cypher_merge_match_branch_noop() {
    let (port, shutdown) = start_txn_server(1, "").await;
    let mut conn = connect(port).await;

    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.CREATE");

    // Pre-txn: create dave OUTSIDE any txn. This node must never be touched.
    let _: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("CREATE (:Person {name:'dave'})")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.QUERY CREATE dave outside TXN");
    assert_eq!(
        parse_graph_node_count(
            &redis::cmd("GRAPH.INFO")
                .arg("g")
                .query_async::<redis::Value>(&mut conn)
                .await
                .expect("GRAPH.INFO")
        ),
        1
    );

    // Start txn. MERGE dave — should hit the match-branch, not create.
    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN");

    let merge_resp: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MERGE (n:Person {name:'dave'}) RETURN n")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.QUERY MERGE match-branch inside TXN");
    assert_eq!(
        graph_query_row_count(&merge_resp),
        1,
        "MERGE match-branch should return 1 row (existing dave)"
    );

    // ABORT — no intents should have been recorded from the match-branch,
    // so dave must survive untouched.
    let _: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN.ABORT");

    let info: redis::Value = redis::cmd("GRAPH.INFO")
        .arg("g")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.INFO post-abort");
    assert_eq!(
        parse_graph_node_count(&info),
        1,
        "MERGE match-branch must not produce rollback intents; pre-existing dave must survive TXN.ABORT"
    );

    let persons: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (p:Person) RETURN p.name")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.QUERY MATCH dave post-abort");
    assert_eq!(
        graph_query_row_count(&persons),
        1,
        "dave must remain in MATCH results after TXN.ABORT. Resp={persons:?}"
    );

    shutdown.cancel();
}
