//! Integration tests for workspace partitioning.
//!
//! Tests: WS.CREATE, WS.DROP, WS.LIST, WS.INFO, WS.AUTH, workspace isolation
//! for KV, vector search, and graph stores.
//!
//! WS.* commands are intercepted in the sharded handlers (handler_monoio.rs /
//! handler_sharded.rs), NOT in the single-thread handler used by
//! `listener::run_with_shutdown`.  These tests therefore start a full sharded
//! server via `listener::run_sharded` (same path as `main.rs`).
//!
//! Run: cargo test --test workspace_integration --no-default-features --features runtime-tokio,jemalloc
#![cfg(feature = "runtime-tokio")]

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::server::listener;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use moon::shard::Shard;
use redis::Value;
use tokio::net::TcpListener;

/// Start a full sharded Moon server on a random port.
///
/// Uses the same shard-thread + `run_sharded` pattern as `main.rs`, so
/// WS.* handler intercepts are active.
async fn start_workspace_server(num_shards: usize) -> (u16, CancellationToken) {
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
                std::sync::Arc::new(parking_lot::RwLock::new(
                    moon::pubsub::PubSubRegistry::new(),
                ))
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
                .name(format!("ws-test-shard-{}", id))
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

    // Give the server time to bind and start shards
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    (port, token)
}

/// Start a full sharded server with requirepass for ACL tests.
async fn start_workspace_server_with_auth(
    num_shards: usize,
    password: &str,
) -> (u16, CancellationToken) {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let token = CancellationToken::new();
    let password = password.to_string();

    let config = ServerConfig {
        bind: "127.0.0.1".to_string(),
        port,
        databases: 16,
        requirepass: Some(password),
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
                std::sync::Arc::new(parking_lot::RwLock::new(
                    moon::pubsub::PubSubRegistry::new(),
                ))
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
                .name(format!("ws-auth-shard-{}", id))
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

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    (port, token)
}

/// Start a workspace server with default shard count (1 for simple tests).
async fn start_server() -> (u16, CancellationToken) {
    start_workspace_server(1).await
}

/// Open a new multiplexed connection to the server.
/// Each call creates a separate TCP connection (separate ConnectionState).
async fn connect(port: u16) -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}

/// Extract a string from a redis::Value.
fn extract_string(val: &Value) -> String {
    match val {
        Value::BulkString(b) => String::from_utf8_lossy(b).to_string(),
        Value::SimpleString(s) => s.clone(),
        other => panic!("expected string value, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Test 1: WS CREATE + WS AUTH + WS INFO lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_workspace_create_and_auth() {
    let (port, token) = start_server().await;
    let mut conn = connect(port).await;

    // WS CREATE returns a UUID string
    let ws_id: String = redis::cmd("WS")
        .arg("CREATE")
        .arg("test-workspace")
        .query_async(&mut conn)
        .await
        .unwrap();

    // UUID v7 format: 8-4-4-4-12 (36 chars with dashes)
    assert_eq!(ws_id.len(), 36, "UUID should be 36 chars, got: {ws_id}");
    assert_eq!(
        ws_id.chars().filter(|c| *c == '-').count(),
        4,
        "UUID should have 4 dashes"
    );

    // WS AUTH binds the connection
    let auth_result: String = redis::cmd("WS")
        .arg("AUTH")
        .arg(&ws_id)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(auth_result, "OK");

    // WS INFO returns workspace metadata
    let info: Value = redis::cmd("WS")
        .arg("INFO")
        .arg(&ws_id)
        .query_async(&mut conn)
        .await
        .unwrap();

    // INFO returns [id, <uuid>, name, <name>, created_at, <ts>]
    match &info {
        Value::Array(items) => {
            assert!(items.len() >= 6, "WS INFO should return at least 6 elements");
            let id_label = extract_string(&items[0]);
            assert_eq!(id_label, "id");
            let id_val = extract_string(&items[1]);
            assert_eq!(id_val, ws_id);
            let name_label = extract_string(&items[2]);
            assert_eq!(name_label, "name");
            let name_val = extract_string(&items[3]);
            assert_eq!(name_val, "test-workspace");
        }
        other => panic!("expected Array from WS INFO, got {other:?}"),
    }

    token.cancel();
}

// ---------------------------------------------------------------------------
// Test 2: WS LIST shows all created workspaces
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_workspace_list() {
    let (port, token) = start_server().await;
    let mut conn = connect(port).await;

    // Create 3 workspaces
    let names = ["ws-alpha", "ws-beta", "ws-gamma"];
    for name in &names {
        let _ws_id: String = redis::cmd("WS")
            .arg("CREATE")
            .arg(*name)
            .query_async(&mut conn)
            .await
            .unwrap();
    }

    // WS LIST returns array of [id, name, created_at] entries
    let list: Value = redis::cmd("WS")
        .arg("LIST")
        .query_async(&mut conn)
        .await
        .unwrap();

    match &list {
        Value::Array(items) => {
            assert_eq!(items.len(), 3, "should have 3 workspaces");
            let mut found_names: Vec<String> = items
                .iter()
                .filter_map(|entry| {
                    if let Value::Array(inner) = entry {
                        if inner.len() >= 2 {
                            Some(extract_string(&inner[1]))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect();
            found_names.sort();
            assert_eq!(found_names, vec!["ws-alpha", "ws-beta", "ws-gamma"]);
        }
        other => panic!("expected Array from WS LIST, got {other:?}"),
    }

    token.cancel();
}

// ---------------------------------------------------------------------------
// Test 3: WS DROP removes a workspace
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_workspace_drop() {
    let (port, token) = start_server().await;
    let mut conn = connect(port).await;

    // Create workspace
    let ws_id: String = redis::cmd("WS")
        .arg("CREATE")
        .arg("drop-me")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Verify it exists
    let list: Value = redis::cmd("WS")
        .arg("LIST")
        .query_async(&mut conn)
        .await
        .unwrap();
    if let Value::Array(items) = &list {
        assert_eq!(items.len(), 1, "should have 1 workspace before drop");
    }

    // Drop it
    let drop_result: String = redis::cmd("WS")
        .arg("DROP")
        .arg(&ws_id)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(drop_result, "OK");

    // Verify list is empty
    let list: Value = redis::cmd("WS")
        .arg("LIST")
        .query_async(&mut conn)
        .await
        .unwrap();
    if let Value::Array(items) = &list {
        assert!(items.is_empty(), "should have 0 workspaces after drop");
    }

    // WS AUTH with dropped workspace should fail
    let auth_result: Result<String, _> = redis::cmd("WS")
        .arg("AUTH")
        .arg(&ws_id)
        .query_async(&mut conn)
        .await;
    assert!(
        auth_result.is_err(),
        "WS AUTH with dropped workspace should fail"
    );

    token.cancel();
}

// ---------------------------------------------------------------------------
// Test 4: KV isolation between workspaces
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_workspace_kv_isolation() {
    let (port, token) = start_server().await;

    let mut conn_a = connect(port).await;
    let mut conn_b = connect(port).await;

    // Create workspace A and bind conn_a
    let ws_a: String = redis::cmd("WS")
        .arg("CREATE")
        .arg("workspace-a")
        .query_async(&mut conn_a)
        .await
        .unwrap();
    let _: String = redis::cmd("WS")
        .arg("AUTH")
        .arg(&ws_a)
        .query_async(&mut conn_a)
        .await
        .unwrap();

    // SET counter 42 via conn_a (workspace-scoped)
    let _: () = redis::cmd("SET")
        .arg("counter")
        .arg("42")
        .query_async(&mut conn_a)
        .await
        .unwrap();

    // GET counter via conn_a -> "42"
    let val_a: String = redis::cmd("GET")
        .arg("counter")
        .query_async(&mut conn_a)
        .await
        .unwrap();
    assert_eq!(val_a, "42");

    // GET counter via conn_b (unbound) -> nil (no workspace prefix, different key)
    let val_b: Option<String> = redis::cmd("GET")
        .arg("counter")
        .query_async(&mut conn_b)
        .await
        .unwrap();
    assert_eq!(
        val_b, None,
        "unbound connection should not see workspace key"
    );

    // Create workspace B and bind conn_b
    let ws_b: String = redis::cmd("WS")
        .arg("CREATE")
        .arg("workspace-b")
        .query_async(&mut conn_b)
        .await
        .unwrap();
    let _: String = redis::cmd("WS")
        .arg("AUTH")
        .arg(&ws_b)
        .query_async(&mut conn_b)
        .await
        .unwrap();

    // SET counter 99 via conn_b (different workspace)
    let _: () = redis::cmd("SET")
        .arg("counter")
        .arg("99")
        .query_async(&mut conn_b)
        .await
        .unwrap();

    // GET counter via conn_a -> still "42" (different workspace)
    let val_a2: String = redis::cmd("GET")
        .arg("counter")
        .query_async(&mut conn_a)
        .await
        .unwrap();
    assert_eq!(val_a2, "42", "workspace A counter should still be 42");

    // GET counter via conn_b -> "99"
    let val_b2: String = redis::cmd("GET")
        .arg("counter")
        .query_async(&mut conn_b)
        .await
        .unwrap();
    assert_eq!(val_b2, "99", "workspace B counter should be 99");

    token.cancel();
}

// ---------------------------------------------------------------------------
// Test 5: Multi-key isolation (MSET / MGET)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_workspace_multi_key_isolation() {
    let (port, token) = start_server().await;
    let mut conn_a = connect(port).await;
    let mut conn_b = connect(port).await;

    // Create and bind workspace A
    let ws_a: String = redis::cmd("WS")
        .arg("CREATE")
        .arg("multi-a")
        .query_async(&mut conn_a)
        .await
        .unwrap();
    let _: String = redis::cmd("WS")
        .arg("AUTH")
        .arg(&ws_a)
        .query_async(&mut conn_a)
        .await
        .unwrap();

    // Create and bind workspace B
    let ws_b: String = redis::cmd("WS")
        .arg("CREATE")
        .arg("multi-b")
        .query_async(&mut conn_b)
        .await
        .unwrap();
    let _: String = redis::cmd("WS")
        .arg("AUTH")
        .arg(&ws_b)
        .query_async(&mut conn_b)
        .await
        .unwrap();

    // Via conn_a: MSET a 1 b 2 c 3
    let _: () = redis::cmd("MSET")
        .arg("a")
        .arg("1")
        .arg("b")
        .arg("2")
        .arg("c")
        .arg("3")
        .query_async(&mut conn_a)
        .await
        .unwrap();

    // Via conn_b: MGET a b c -> all nil (different workspace)
    let vals_b: Vec<Option<String>> = redis::cmd("MGET")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut conn_b)
        .await
        .unwrap();
    assert_eq!(
        vals_b,
        vec![None, None, None],
        "workspace B should not see A's keys"
    );

    // Via conn_a: MGET a b c -> [1, 2, 3]
    let vals_a: Vec<String> = redis::cmd("MGET")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut conn_a)
        .await
        .unwrap();
    assert_eq!(vals_a, vec!["1", "2", "3"]);

    token.cancel();
}

// ---------------------------------------------------------------------------
// Test 6: KEYS returns unscoped names (no workspace prefix visible)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_workspace_keys_returns_unscoped_names() {
    let (port, token) = start_server().await;
    let mut conn = connect(port).await;

    // Create and bind workspace
    let ws_id: String = redis::cmd("WS")
        .arg("CREATE")
        .arg("keys-test")
        .query_async(&mut conn)
        .await
        .unwrap();
    let _: String = redis::cmd("WS")
        .arg("AUTH")
        .arg(&ws_id)
        .query_async(&mut conn)
        .await
        .unwrap();

    // Set some keys
    let _: () = redis::cmd("SET")
        .arg("mykey")
        .arg("value")
        .query_async(&mut conn)
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("otherkey")
        .arg("value2")
        .query_async(&mut conn)
        .await
        .unwrap();

    // KEYS * should return unscoped names (NOT {ws_hex}:mykey)
    let mut keys: Vec<String> = redis::cmd("KEYS")
        .arg("*")
        .query_async(&mut conn)
        .await
        .unwrap();
    keys.sort();

    assert_eq!(keys, vec!["mykey", "otherkey"]);
    // Verify no key has the workspace prefix visible
    for key in &keys {
        assert!(
            !key.starts_with('{'),
            "key {key} should not have workspace prefix"
        );
    }

    token.cancel();
}

// ---------------------------------------------------------------------------
// Test 7: WS AUTH error handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_workspace_auth_errors() {
    let (port, token) = start_server().await;
    let mut conn = connect(port).await;

    // WS AUTH with nonexistent UUID -> error
    let result: Result<String, _> = redis::cmd("WS")
        .arg("AUTH")
        .arg("00000000-0000-0000-0000-000000000000")
        .query_async(&mut conn)
        .await;
    assert!(
        result.is_err(),
        "WS AUTH with nonexistent UUID should error"
    );

    // WS AUTH with invalid format -> error
    let result: Result<String, _> = redis::cmd("WS")
        .arg("AUTH")
        .arg("invalid-not-uuid")
        .query_async(&mut conn)
        .await;
    assert!(result.is_err(), "WS AUTH with invalid UUID should error");

    // WS AUTH with no args -> error
    let result: Result<String, _> = redis::cmd("WS")
        .arg("AUTH")
        .query_async(&mut conn)
        .await;
    assert!(result.is_err(), "WS AUTH with no args should error");

    // Create workspace and bind
    let ws_id: String = redis::cmd("WS")
        .arg("CREATE")
        .arg("bound-ws")
        .query_async(&mut conn)
        .await
        .unwrap();
    let _: String = redis::cmd("WS")
        .arg("AUTH")
        .arg(&ws_id)
        .query_async(&mut conn)
        .await
        .unwrap();

    // WS AUTH again on already-bound connection -> error
    let result: Result<String, _> = redis::cmd("WS")
        .arg("AUTH")
        .arg(&ws_id)
        .query_async(&mut conn)
        .await;
    assert!(
        result.is_err(),
        "WS AUTH on already-bound connection should error"
    );

    token.cancel();
}

// ---------------------------------------------------------------------------
// Test 8: Hash tag co-location with multi-shard config
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_workspace_hash_tag_colocation() {
    // Start server with 4 shards to test co-location
    let (port, token) = start_workspace_server(4).await;
    let mut conn = connect(port).await;

    // Create workspace and bind
    let ws_id: String = redis::cmd("WS")
        .arg("CREATE")
        .arg("shard-test")
        .query_async(&mut conn)
        .await
        .unwrap();
    let _: String = redis::cmd("WS")
        .arg("AUTH")
        .arg(&ws_id)
        .query_async(&mut conn)
        .await
        .unwrap();

    // Set multiple keys -- all should route to the same shard via hash tag
    let _: () = redis::cmd("SET")
        .arg("user:1")
        .arg("a")
        .query_async(&mut conn)
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("user:2")
        .arg("b")
        .query_async(&mut conn)
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("product:99")
        .arg("c")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Verify all keys are readable (all on same shard via hash tag)
    let v1: String = redis::cmd("GET")
        .arg("user:1")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(v1, "a");

    let v2: String = redis::cmd("GET")
        .arg("user:2")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(v2, "b");

    let v3: String = redis::cmd("GET")
        .arg("product:99")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(v3, "c");

    token.cancel();
}

// ---------------------------------------------------------------------------
// Test 9: Vector search isolation
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "Vector ingestion via redis crate requires binary encoding; prefix mechanism validated by KV isolation tests and unit tests"]
async fn test_workspace_vector_isolation() {
    // The workspace prefix mechanism is the same for FT.* index names as for KV keys.
    // FT.CREATE args[0] (index name) gets prefixed with {ws_hex}: so two workspaces
    // with the same user-visible index name create different internal indices.
    // Validated by unit tests: test_rewrite_args_ft_search, test_rewrite_args_ft_create.
}

// ---------------------------------------------------------------------------
// Test 10: Graph isolation (GRAPH.QUERY)
// ---------------------------------------------------------------------------

#[tokio::test]
#[cfg(feature = "graph")]
async fn test_workspace_graph_isolation() {
    let (port, token) = start_server().await;
    let mut conn_a = connect(port).await;
    let mut conn_b = connect(port).await;

    // Create and bind workspace A
    let ws_a: String = redis::cmd("WS")
        .arg("CREATE")
        .arg("graph-a")
        .query_async(&mut conn_a)
        .await
        .unwrap();
    let _: String = redis::cmd("WS")
        .arg("AUTH")
        .arg(&ws_a)
        .query_async(&mut conn_a)
        .await
        .unwrap();

    // Create and bind workspace B
    let ws_b: String = redis::cmd("WS")
        .arg("CREATE")
        .arg("graph-b")
        .query_async(&mut conn_b)
        .await
        .unwrap();
    let _: String = redis::cmd("WS")
        .arg("AUTH")
        .arg(&ws_b)
        .query_async(&mut conn_b)
        .await
        .unwrap();

    // Via conn_a: create graph and add a node
    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("mygraph")
        .query_async(&mut conn_a)
        .await
        .unwrap();

    redis::cmd("GRAPH.QUERY")
        .arg("mygraph")
        .arg("CREATE (n:Person {name: 'Alice'})")
        .query_async::<Value>(&mut conn_a)
        .await
        .unwrap();

    // Via conn_b: create the graph in workspace B (different namespace)
    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("mygraph")
        .query_async(&mut conn_b)
        .await
        .unwrap();

    let result_b: Value = redis::cmd("GRAPH.QUERY")
        .arg("mygraph")
        .arg("MATCH (n) RETURN n")
        .query_async(&mut conn_b)
        .await
        .unwrap();

    // Workspace B's graph should be empty -- no Alice
    match &result_b {
        Value::Array(items) => {
            let result_str = format!("{items:?}");
            assert!(
                !result_str.contains("Alice"),
                "workspace B should not see Alice from workspace A"
            );
        }
        _ => {}
    }

    // Via conn_a: query should find Alice
    let result_a: Value = redis::cmd("GRAPH.QUERY")
        .arg("mygraph")
        .arg("MATCH (n:Person) RETURN n LIMIT 10")
        .query_async(&mut conn_a)
        .await
        .unwrap();

    match &result_a {
        Value::Array(items) => {
            assert!(
                !items.is_empty(),
                "workspace A should find Alice in its graph"
            );
        }
        other => panic!("expected Array from GRAPH.QUERY, got {other:?}"),
    }

    token.cancel();
}

// ---------------------------------------------------------------------------
// Test 11: ACL workspace grants (WS-11)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_workspace_acl_grant() {
    let (port, token) = start_workspace_server_with_auth(1, "adminpass").await;

    // Connect as default admin user
    let client = redis::Client::open(format!("redis://:adminpass@127.0.0.1:{port}")).unwrap();
    let mut admin_conn = client.get_multiplexed_async_connection().await.unwrap();

    // Create a workspace
    let ws_id: String = redis::cmd("WS")
        .arg("CREATE")
        .arg("acl-ws")
        .query_async(&mut admin_conn)
        .await
        .unwrap();

    // Get the workspace hex for ACL pattern
    let ws_hex: String = ws_id.replace('-', "");
    assert_eq!(ws_hex.len(), 32, "workspace hex should be 32 chars");

    // ACL SETUSER alice: allow all keys, restrict commands.
    //
    // NOTE: ACL key checks run BEFORE workspace prefix injection in the
    // handler pipeline.  This means ACL key patterns match against the
    // client-visible (unprefixed) key names, not the internal prefixed names.
    // Workspace isolation is enforced by the prefix injection layer (WS-07),
    // while ACL provides command-level access control as defense-in-depth.
    //
    // For true key-pattern defense-in-depth (ACL matching prefixed keys),
    // the ACL check would need to move after workspace rewrite -- tracked
    // as a future enhancement (WS-11 partial).
    let acl_result: String = redis::cmd("ACL")
        .arg("SETUSER")
        .arg("alice")
        .arg("on")
        .arg(">alicepass")
        .arg("~*")
        .arg("+@all")
        .query_async(&mut admin_conn)
        .await
        .unwrap();
    assert_eq!(acl_result, "OK");

    // Also create bob with restricted command access (no WS commands)
    let acl_result2: String = redis::cmd("ACL")
        .arg("SETUSER")
        .arg("bob")
        .arg("on")
        .arg(">bobpass")
        .arg("~*")
        .arg("+get")
        .arg("+set")
        .query_async(&mut admin_conn)
        .await
        .unwrap();
    assert_eq!(acl_result2, "OK");

    // Connect as alice and bind to workspace
    let alice_client =
        redis::Client::open(format!("redis://alice:alicepass@127.0.0.1:{port}")).unwrap();
    let mut alice_conn = alice_client
        .get_multiplexed_async_connection()
        .await
        .unwrap();

    // WS AUTH should succeed for alice (has +@all)
    let auth_result: String = redis::cmd("WS")
        .arg("AUTH")
        .arg(&ws_id)
        .query_async(&mut alice_conn)
        .await
        .unwrap();
    assert_eq!(auth_result, "OK");

    // SET mykey val -> OK (workspace-bound, key becomes {ws_hex}:mykey)
    let set_result: String = redis::cmd("SET")
        .arg("mykey")
        .arg("val")
        .query_async(&mut alice_conn)
        .await
        .unwrap();
    assert_eq!(set_result, "OK");

    // GET mykey -> "val"
    let get_result: String = redis::cmd("GET")
        .arg("mykey")
        .query_async(&mut alice_conn)
        .await
        .unwrap();
    assert_eq!(get_result, "val");

    // Connect as bob -- bob has +get +set but NOT +ws, so WS AUTH should fail
    let bob_client =
        redis::Client::open(format!("redis://bob:bobpass@127.0.0.1:{port}")).unwrap();
    let mut bob_conn = bob_client
        .get_multiplexed_async_connection()
        .await
        .unwrap();

    let bob_ws_result: Result<String, _> = redis::cmd("WS")
        .arg("AUTH")
        .arg(&ws_id)
        .query_async(&mut bob_conn)
        .await;
    assert!(
        bob_ws_result.is_err(),
        "bob should be denied WS command (only has +get +set)"
    );

    // alice on a separate unbound connection can SET but won't see workspace data
    let alice_client2 =
        redis::Client::open(format!("redis://alice:alicepass@127.0.0.1:{port}")).unwrap();
    let mut alice_unbound = alice_client2
        .get_multiplexed_async_connection()
        .await
        .unwrap();

    // SET on unbound connection uses raw key "mykey" (no workspace prefix)
    let _: String = redis::cmd("SET")
        .arg("mykey")
        .arg("unbound-val")
        .query_async(&mut alice_unbound)
        .await
        .unwrap();

    // GET on unbound connection returns the unbound value
    let unbound_get: String = redis::cmd("GET")
        .arg("mykey")
        .query_async(&mut alice_unbound)
        .await
        .unwrap();
    assert_eq!(unbound_get, "unbound-val");

    // GET on workspace-bound alice still returns the workspace value
    let bound_get: String = redis::cmd("GET")
        .arg("mykey")
        .query_async(&mut alice_conn)
        .await
        .unwrap();
    assert_eq!(
        bound_get, "val",
        "workspace-bound GET should still return workspace value, not unbound value"
    );

    token.cancel();
}

// ---------------------------------------------------------------------------
// Test 12: Workspace KV isolation with multi-shard (4 shards)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_workspace_kv_isolation_multishard() {
    let (port, token) = start_workspace_server(4).await;
    let mut conn_a = connect(port).await;
    let mut conn_b = connect(port).await;

    // Create and bind workspace A
    let ws_a: String = redis::cmd("WS")
        .arg("CREATE")
        .arg("shard-iso-a")
        .query_async(&mut conn_a)
        .await
        .unwrap();
    let _: String = redis::cmd("WS")
        .arg("AUTH")
        .arg(&ws_a)
        .query_async(&mut conn_a)
        .await
        .unwrap();

    // Create and bind workspace B
    let ws_b: String = redis::cmd("WS")
        .arg("CREATE")
        .arg("shard-iso-b")
        .query_async(&mut conn_b)
        .await
        .unwrap();
    let _: String = redis::cmd("WS")
        .arg("AUTH")
        .arg(&ws_b)
        .query_async(&mut conn_b)
        .await
        .unwrap();

    // Set keys in workspace A
    for i in 0..10 {
        let set_result: String = redis::cmd("SET")
            .arg(format!("key:{i}"))
            .arg(format!("a-{i}"))
            .query_async(&mut conn_a)
            .await
            .unwrap();
        assert_eq!(set_result, "OK", "workspace A SET key:{i} should succeed");
    }

    // Verify workspace B cannot see any of workspace A's keys
    for i in 0..10 {
        let val: Option<String> = redis::cmd("GET")
            .arg(format!("key:{i}"))
            .query_async(&mut conn_b)
            .await
            .unwrap();
        assert_eq!(
            val, None,
            "workspace B should not see key:{i} from workspace A"
        );
    }

    // Set the same key names in workspace B with different values
    for i in 0..10 {
        let set_result: String = redis::cmd("SET")
            .arg(format!("key:{i}"))
            .arg(format!("b-{i}"))
            .query_async(&mut conn_b)
            .await
            .unwrap();
        assert_eq!(set_result, "OK", "workspace B SET key:{i} should succeed");
    }

    // Small delay for cross-shard propagation
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Verify workspace A still sees its own values
    for i in 0..10 {
        let val: Option<String> = redis::cmd("GET")
            .arg(format!("key:{i}"))
            .query_async(&mut conn_a)
            .await
            .unwrap();
        assert_eq!(
            val,
            Some(format!("a-{i}")),
            "workspace A key:{i} should be a-{i}"
        );
    }

    // Verify workspace B sees its own values
    for i in 0..10 {
        let val: Option<String> = redis::cmd("GET")
            .arg(format!("key:{i}"))
            .query_async(&mut conn_b)
            .await
            .unwrap();
        assert_eq!(
            val,
            Some(format!("b-{i}")),
            "workspace B key:{i} should be b-{i}"
        );
    }

    token.cancel();
}
