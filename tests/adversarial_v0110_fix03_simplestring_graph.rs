// FIX-03 RED test -- raw RESP SimpleString graph name MUST participate in intent capture.
//!
//! Phase 174 FIX-03 -- Adversarial RED/GREEN coverage for SimpleString graph name
//! in Phase 167 intent capture.
//!
//! Source: merged_bug_004 + bug_021 (ultrareview of v0.1.9 / commit 2e397c7).
//! The Phase 167 intent capture code in both `handler_sharded.rs` and
//! `handler_monoio.rs` uses `if let Some(Frame::BulkString(gname)) = cmd_args.first()`
//! which silently drops intents when a RESP SimpleString graph name is sent.
//! Redis clients may legitimately send single-token arguments as SimpleString
//! (`+mygraph\r\n`). The parser and execution path already accept both, but the
//! intent capture pattern match only handles BulkString.
//!
//! Test: `test_simplestring_graph_name_captures_intent`
//!   1. Open raw TCP to moon server (bypass redis-rs which always sends BulkString).
//!   2. Issue GRAPH.CREATE via raw RESP (normal BulkStrings).
//!   3. Issue TXN BEGIN via raw RESP.
//!   4. Issue GRAPH.QUERY with graph name as SimpleString (+g\r\n) and a CREATE
//!      query as BulkString. This exercises the intent capture branch.
//!   5. Issue TXN ABORT via raw RESP.
//!   6. Issue GRAPH.QUERY MATCH to verify node was rolled back (count == 0).
//!
//! Current behaviour (RED): count == 1 (intent dropped because SimpleString
//! branch fell through without capturing, so TXN.ABORT had nothing to undo).
//!
//! Runtime: `cargo test --no-default-features --features runtime-tokio,jemalloc,graph
//! --test adversarial_v0110_fix03_simplestring_graph -- --test-threads=1`

#![cfg(all(feature = "runtime-tokio", feature = "graph"))]

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::server::listener;
use moon::shard::Shard;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use std::io::{Read, Write};
use std::net::TcpStream;
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Test server infrastructure — mirrors adversarial_v0110_fix01_set_delete_rollback.rs
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
        initial_keyspace_hint: 0,
        memory_arenas_cap: 8,
        maxclients: 10000,
        timeout: 0,
        tcp_keepalive: 300,
        console_auth_required: false,
        console_auth_secret: String::new(),
        console_cors_origin: vec![],
        console_rate_limit: 1000.0,
        console_rate_burst: 2000.0,
        recovery_target_lsn: None,
        recovery_target_time: None,
        manifest_tombstone_retain_epochs: 2,
        manifest_tombstone_retain_secs: 300,
        disk_free_min_pct: 5,
        mvcc_committed_prune_margin: 1000,
        max_unflushed_immutable_segments: 20,
        mvcc_old_snapshot_threshold_secs: 600,
        autovacuum: "enable".to_string(),
        autovacuum_budget_ms_min: 5,
        autovacuum_budget_ms_max: 200,
        autovacuum_target_p95_ms: 10,
        autovacuum_interval_secs: 30,
        graph_merge_max_segments: 8,
        graph_dead_edge_trigger: 0.20,
        autovacuum_starvation_cap_secs: 300,
        vec_warm_mmap_budget: "2gb".to_string(),
        cold_orphan_sweep_interval_secs: 300,
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
                .name(format!("fix03-shard-{}", id))
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

// ---------------------------------------------------------------------------
// Raw RESP helpers — we bypass redis-rs to send specific Frame types the
// library never produces (SimpleString as a command argument).
// ---------------------------------------------------------------------------

/// Send a RESP array where each element is a BulkString.
fn send_bulk_array(stream: &mut TcpStream, args: &[&[u8]]) {
    let mut buf = format!("*{}\r\n", args.len());
    for arg in args {
        buf.push_str(&format!("${}\r\n", arg.len()));
        // We'll write the binary arg and CRLF separately
        stream.write_all(buf.as_bytes()).unwrap();
        buf.clear();
        stream.write_all(arg).unwrap();
        stream.write_all(b"\r\n").unwrap();
    }
    if !buf.is_empty() {
        stream.write_all(buf.as_bytes()).unwrap();
    }
    stream.flush().unwrap();
}

/// Send a RESP array where the graph name (second element) is a SimpleString
/// and all other elements are BulkStrings.
/// Format: *3\r\n$10\r\nGRAPH.QUERY\r\n+g\r\n$N\r\n<cypher>\r\n
fn send_graph_query_with_simplestring_name(
    stream: &mut TcpStream,
    graph_name: &[u8],
    cypher: &[u8],
) {
    let header = format!("*3\r\n$11\r\nGRAPH.QUERY\r\n");
    stream.write_all(header.as_bytes()).unwrap();
    // Graph name as SimpleString (+ prefix)
    stream.write_all(b"+").unwrap();
    stream.write_all(graph_name).unwrap();
    stream.write_all(b"\r\n").unwrap();
    // Cypher query as BulkString
    let cypher_hdr = format!("${}\r\n", cypher.len());
    stream.write_all(cypher_hdr.as_bytes()).unwrap();
    stream.write_all(cypher).unwrap();
    stream.write_all(b"\r\n").unwrap();
    stream.flush().unwrap();
}

/// Read a complete RESP response (blocking, with timeout). Returns raw bytes.
fn read_response(stream: &mut TcpStream) -> Vec<u8> {
    let mut buf = vec![0u8; 4096];
    // Use a short read timeout — server should reply quickly.
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    let n = stream.read(&mut buf).unwrap_or(0);
    buf.truncate(n);
    buf
}

/// Check if a RESP response starts with an error prefix.
fn is_error_response(resp: &[u8]) -> bool {
    resp.starts_with(b"-")
}

/// Check if response indicates OK (SimpleString +OK or similar).
fn is_ok_response(resp: &[u8]) -> bool {
    resp.starts_with(b"+OK") || resp.starts_with(b"+ok")
}

// ===========================================================================
// TEST: SimpleString graph name drives full TXN.ABORT rollback.
//
// On buggy code: the intent capture uses `if let Some(Frame::BulkString(gname))`
// which does NOT match SimpleString frames. The intent is silently dropped, so
// TXN.ABORT has nothing to roll back. Node Eve persists (count == 1).
//
// After FIX-03: `extract_bytes` accepts both Frame variants, intent is captured,
// ABORT rolls back the CREATE, Eve is gone (count == 0).
// ===========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_simplestring_graph_name_captures_intent() {
    let (port, shutdown) = start_txn_server(1).await;

    // Use a single raw TCP connection for the entire flow (TXN is per-connection).
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).expect("connect to moon");
    stream.set_nodelay(true).expect("set nodelay");

    // 1. GRAPH.CREATE g (normal BulkString args)
    send_bulk_array(&mut stream, &[b"GRAPH.CREATE", b"g"]);
    let resp = read_response(&mut stream);
    assert!(
        is_ok_response(&resp) || resp.starts_with(b"*"),
        "GRAPH.CREATE must succeed. Got: {:?}",
        String::from_utf8_lossy(&resp)
    );

    // 2. TXN BEGIN
    send_bulk_array(&mut stream, &[b"TXN", b"BEGIN"]);
    let resp = read_response(&mut stream);
    assert!(
        is_ok_response(&resp),
        "TXN BEGIN must succeed. Got: {:?}",
        String::from_utf8_lossy(&resp)
    );

    // 3. GRAPH.QUERY with SimpleString graph name: +g\r\n
    // This is the critical frame — intent capture must handle it.
    send_graph_query_with_simplestring_name(&mut stream, b"g", b"CREATE (:Person {name:'Eve'})");
    let resp = read_response(&mut stream);
    // The query itself should succeed (CREATE returns stats array).
    assert!(
        !is_error_response(&resp),
        "GRAPH.QUERY CREATE with SimpleString graph name must succeed. Got: {:?}",
        String::from_utf8_lossy(&resp)
    );

    // 4. TXN ABORT
    send_bulk_array(&mut stream, &[b"TXN", b"ABORT"]);
    let resp = read_response(&mut stream);
    assert!(
        is_ok_response(&resp),
        "TXN ABORT must succeed. Got: {:?}",
        String::from_utf8_lossy(&resp)
    );

    // 5. Verify rollback: MATCH Eve — must return 0 rows.
    // Use normal BulkString framing for the verification query.
    send_bulk_array(
        &mut stream,
        &[
            b"GRAPH.QUERY",
            b"g",
            b"MATCH (n:Person {name:'Eve'}) RETURN n.name",
        ],
    );
    let resp = read_response(&mut stream);
    let resp_str = String::from_utf8_lossy(&resp);

    // Parse row count from the RESP response. The response is an Array:
    //   *3\r\n  (3 elements: headers, rows, stats)
    //   *1\r\n...(headers)...
    //   *N\r\n...(rows — N rows, each is an array of cells)...
    //   ...(stats bulk string)...
    //
    // When Eve is rolled back, the rows array is *0\r\n (empty).
    // When Eve persists (buggy), rows array is *1\r\n (one row with Eve's name).
    //
    // We parse the second top-level array element's size to get the row count.
    // A robust check: the rows section after headers should be "*0\r\n" for zero rows.

    // Find the rows array. The response structure is:
    // *3\r\n (top-level)
    //   *1\r\n ... (headers - 1 column: n.name)
    //   *N\r\n ... (rows)
    //   $M\r\n ... (stats)
    // We look for the second array marker after the headers section.
    // Simple approach: split on lines and find row data indicators.

    // If Eve was NOT rolled back, the rows array will contain her name "Eve".
    // If rolled back, rows array is empty (*0\r\n).
    let eve_present = resp_str.contains("Eve");

    assert!(
        !eve_present,
        "TXN.ABORT must roll back CREATE of Eve when graph name was sent as SimpleString. \
         Expected 0 rows (Eve removed) but found Eve in response. \
         FIX-03: intent capture uses `if let Some(Frame::BulkString(gname))` which \
         does NOT match SimpleString — intent dropped, ABORT has nothing to undo. \
         Raw response: {resp_str}"
    );

    drop(stream);
    shutdown.cancel();
}
