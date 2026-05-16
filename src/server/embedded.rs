//! `run_embedded` — minimal sharded boot for in-process Moon embedding.
//!
//! This is the production-equivalent path used by `helios moon-daemon`
//! (and any other embedder that needs the full sharded handler — most
//! importantly the TXN.BEGIN / TXN.COMMIT cross-store transaction wiring
//! implemented only in `handler_sharded.rs`).
//!
//! Reference shape: `tests/txn_kv_wiring.rs::start_txn_server` for the
//! minimal sharded wiring with public APIs, and `src/main.rs` for the
//! AOF / WAL recovery blocks we layer on top.
//!
//! # Why a dedicated helper instead of calling `run_with_shutdown`?
//!
//! `run_with_shutdown` drives `handler_single`, which deliberately does
//! NOT implement TXN (see comments in `conn/handler_single.rs`). Embedders
//! that need transactional KV must take the sharded path. This helper
//! exposes that path with a small surface that hides cluster/console/TLS
//! complexity an embedder does not need.
//!
//! # What is intentionally skipped (vs main.rs)
//!
//! - TLS / `tls_port` (embedders use loopback only)
//! - Console gateway, admin auth, CORS, rate limiting (`console` feature)
//! - Admin port / Prometheus HTTP server
//! - Cluster bus + gossip ticker
//! - SIGHUP TLS reload thread
//! - `--check-config` short-circuit (caller already validated)
//!
//! What IS included: AOF replay (single-shard manifest path), graph/temporal/
//! workspace/MQ WAL replay, auto-save timer, per-shard SO_REUSEPORT on Linux,
//! NUMA pinning, and graceful cancel-driven shutdown.

#![cfg(feature = "runtime-tokio")]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use anyhow::Context;
use parking_lot::RwLock;
use tracing::info;

use crate::config::ServerConfig;
use crate::persistence::aof::{self, AofMessage, FsyncPolicy};
use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use crate::runtime::{RuntimeFactoryImpl, traits::RuntimeFactory};
use crate::server;
use crate::shard::Shard;
use crate::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use crate::shard::shared_databases::ShardDatabases;

/// Run an embedded sharded Moon server until `cancel` is fired.
///
/// Behaves like the production `main.rs` startup path but with cluster,
/// TLS, console, and admin-port concerns elided. Suitable for in-process
/// embedding (e.g. `helios moon-daemon`).
///
/// # Arguments
/// * `config` — fully-resolved `ServerConfig` (the caller is responsible
///   for setting `shards >= 1`; if `0`, this fn auto-detects core count).
/// * `cancel` — when fired, the listener exits, AOF flushes via
///   `AofMessage::Shutdown`, and shard threads are joined.
///
/// # Returns
/// `Ok(())` on clean shutdown. Returns `Err` if the persistence directory
/// is unusable, AOF manifest is corrupt, or a shard thread fails to spawn.
pub async fn run_embedded(
    mut config: ServerConfig,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    // Validate / create persistence directory up front.
    std::fs::create_dir_all(&config.dir).with_context(|| {
        format!(
            "embedded moon: failed to create persistence directory {:?}",
            config.dir
        )
    })?;

    // Resolve shard count (`0` => auto-detect core count, matches main.rs).
    if config.shards == 0 {
        config.shards = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
    }
    let num_shards = config.shards;

    info!(
        "embedded moon: starting with {} shard(s) on {}:{}",
        num_shards, config.bind, config.port
    );

    // One-time global init that the production binary normally performs.
    crate::admin::metrics_setup::init_global_slowlog(
        config.slowlog_max_len,
        config.slowlog_log_slower_than,
    );
    crate::vector::distance::init();

    // Channel mesh for inter-shard messaging.
    let mut mesh = ChannelMesh::new(num_shards, CHANNEL_BUFFER_SIZE);
    let conn_txs: Vec<channel::MpscSender<(tokio::net::TcpStream, bool)>> =
        (0..num_shards).map(|i| mesh.conn_tx(i)).collect();
    let all_notifiers = mesh.all_notifiers();

    // AOF writer: dedicated std::thread (matches main.rs lifetime model).
    let aof_tx: Option<channel::MpscSender<AofMessage>> = if config.appendonly == "yes" {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(10_000);
        let aof_token = cancel.child_token();
        let fsync = FsyncPolicy::from_str(&config.appendfsync);
        let aof_file_path = PathBuf::from(&config.dir).join(&config.appendfilename);
        std::thread::Builder::new()
            .name("embedded-moon-aof".to_string())
            .spawn(move || {
                RuntimeFactoryImpl::block_on_local(
                    "embedded-moon-aof".to_string(),
                    aof::aof_writer_task(rx, aof_file_path, fsync, aof_token),
                );
            })
            .context("embedded moon: failed to spawn AOF writer thread")?;
        info!("embedded moon: AOF enabled (fsync: {:?})", fsync);
        Some(tx)
    } else {
        None
    };

    let bind_addr = format!("{}:{}", config.bind, config.port);

    // Snapshot trigger watch channel.
    let (snap_tx, snap_rx) = channel::watch(0u64);

    // Persistence dir is set whenever any persistence is on.
    let persistence_dir: Option<String> = if config.appendonly == "yes" || config.save.is_some() {
        Some(config.dir.clone())
    } else {
        None
    };

    // Replication state — embedded mode is single-node, but the shard
    // event loop still expects a populated state object.
    let (repl_id, repl_id2) =
        crate::replication::state::load_replication_state(std::path::Path::new(&config.dir));
    let repl_state = Arc::new(std::sync::RwLock::new(
        crate::replication::state::ReplicationState::new(num_shards, repl_id, repl_id2),
    ));
    crate::admin::metrics_setup::set_global_repl_state(repl_state.clone());

    // ACL table (loads aclfile if configured; default no-op otherwise).
    let acl_table: Arc<std::sync::RwLock<crate::acl::AclTable>> = Arc::new(
        std::sync::RwLock::new(crate::acl::AclTable::load_or_default(&config)),
    );

    // Shared runtime + server configs.
    let runtime_config_shared: Arc<RwLock<crate::config::RuntimeConfig>> =
        Arc::new(RwLock::new(config.to_runtime_config()));
    let server_config_shared: Arc<ServerConfig> = Arc::new(config.clone());

    // Per-shard pubsub + remote-subscriber registries.
    let all_pubsub_registries: Vec<Arc<RwLock<crate::pubsub::PubSubRegistry>>> = (0..num_shards)
        .map(|_| Arc::new(RwLock::new(crate::pubsub::PubSubRegistry::new())))
        .collect();
    let all_remote_sub_maps: Vec<
        Arc<RwLock<crate::shard::remote_subscriber_map::RemoteSubscriberMap>>,
    > = (0..num_shards)
        .map(|_| {
            Arc::new(RwLock::new(
                crate::shard::remote_subscriber_map::RemoteSubscriberMap::new(),
            ))
        })
        .collect();

    let affinity_tracker = Arc::new(RwLock::new(crate::shard::affinity::AffinityTracker::new()));

    // Build shards + run pre-loop persistence recovery (RDB / per-shard WAL
    // baseline). Disk offload is opt-in.
    let disk_offload_base = if config.disk_offload_enabled() {
        Some(config.effective_disk_offload_dir())
    } else {
        None
    };
    let mut shards: Vec<Shard> = (0..num_shards)
        .map(|id| {
            let mut shard = Shard::with_initial_keyspace_hint(
                id,
                num_shards,
                config.databases,
                config.initial_keyspace_hint,
                config.to_runtime_config(),
            );
            if let Some(ref dir) = persistence_dir {
                shard.restore_from_persistence(dir, disk_offload_base.as_deref());
            }
            if let Some(ref offload_base) = disk_offload_base {
                let shard_dir = offload_base.join(format!("shard-{}", id));
                for db in &mut shard.databases {
                    db.cold_shard_dir = Some(shard_dir.clone());
                    if db.cold_index.is_none() {
                        db.cold_index = Some(crate::storage::tiered::cold_index::ColdIndex::new());
                    }
                }
            }
            shard
        })
        .collect();

    // Multi-part AOF replay (single-shard only; matches main.rs constraint).
    if config.appendonly == "yes" && let Some(ref dir) = persistence_dir {
        use crate::persistence::aof_manifest::AofManifest;
        use crate::persistence::replay::DispatchReplayEngine;
        let base_dir = std::path::PathBuf::from(dir);
        let manifest_opt = AofManifest::load(&base_dir).with_context(|| {
            format!(
                "embedded moon: AOF manifest at {}/appendonlydir/ is corrupt; refusing to start to avoid data loss",
                base_dir.display()
            )
        })?;
        if let Some(ref manifest) = manifest_opt {
            if num_shards == 1 {
                for db in shards[0].databases.iter_mut() {
                    db.clear();
                }
                let loaded = crate::persistence::aof_manifest::replay_multi_part(
                    &mut shards[0].databases,
                    manifest,
                    &DispatchReplayEngine::new(),
                )
                .context("embedded moon: multi-part AOF replay failed")?;
                info!(
                    "embedded moon: AOF multi-part loaded (seq {}): {} entries",
                    manifest.seq, loaded
                );
                let legacy = base_dir.join("appendonly.aof");
                if legacy.exists() {
                    let retired = base_dir.join("appendonly.aof.legacy");
                    if let Err(e) = std::fs::rename(&legacy, &retired) {
                        tracing::warn!(
                            "embedded moon: failed to retire legacy AOF {}: {}",
                            legacy.display(),
                            e
                        );
                    }
                }
            } else {
                tracing::warn!(
                    "embedded moon: multi-part AOF skipped in multi-shard mode (not yet supported)"
                );
            }
        } else {
            // No manifest. If restore_from_persistence already loaded state,
            // snapshot it as the seq-1 base RDB to avoid losing it on next boot.
            let has_state = num_shards == 1 && shards[0].databases.iter().any(|db| db.len() > 0);
            if has_state {
                let rdb_bytes = crate::persistence::rdb::save_to_bytes(&shards[0].databases)
                    .context("embedded moon: failed to serialize legacy state for AOF base")?;
                AofManifest::initialize_with_base(&base_dir, &rdb_bytes)
                    .context("embedded moon: failed to initialize AOF manifest with base")?;
                info!(
                    "embedded moon: first-upgrade captured legacy state as AOF base seq 1 ({} bytes)",
                    rdb_bytes.len()
                );
                let legacy = base_dir.join("appendonly.aof");
                if legacy.exists() {
                    let retired = base_dir.join("appendonly.aof.legacy");
                    if let Err(e) = std::fs::rename(&legacy, &retired) {
                        tracing::warn!(
                            "embedded moon: failed to retire legacy AOF {}: {}",
                            legacy.display(),
                            e
                        );
                    }
                }
            } else {
                AofManifest::initialize(&base_dir)
                    .context("embedded moon: failed to initialize AOF manifest")?;
            }
        }
    }

    // Pull databases out into the shared registry for cross-shard reads.
    let all_dbs: Vec<Vec<crate::storage::Database>> = shards
        .iter_mut()
        .map(|s| std::mem::take(&mut s.databases))
        .collect();
    let shard_databases = ShardDatabases::new(all_dbs);

    // Auxiliary WAL replay paths (no-op when files do not exist).
    #[cfg(feature = "graph")]
    if let Some(ref dir) = persistence_dir {
        let dir_path = std::path::Path::new(dir);
        shard_databases.recover_graph_stores(dir_path);
        shard_databases.replay_graph_wal(dir_path);
    }
    if let Some(ref dir) = persistence_dir {
        let dir_path = std::path::Path::new(dir);
        shard_databases.replay_temporal_wal(dir_path);
        shard_databases.replay_workspace_wal(dir_path);
        shard_databases.replay_mq_wal(dir_path);
    }

    // Readiness flag — `/readyz` is gated on this; harmless without admin port.
    crate::admin::metrics_setup::set_server_ready();

    // Spawn shard threads.
    let mut shard_handles = Vec::with_capacity(num_shards);
    let config_port = config.port;
    for (id, mut shard) in shards.into_iter().enumerate() {
        let producers = mesh.take_producers(id);
        let consumers = mesh.take_consumers(id);
        let conn_rx = mesh.take_conn_rx(id);
        let shard_cancel = cancel.clone();
        let shard_aof_tx = aof_tx.clone();
        let shard_bind_addr = bind_addr.clone();
        let shard_persistence_dir = persistence_dir.clone();
        let shard_snap_rx = snap_rx.clone();
        let shard_snap_tx = snap_tx.clone();
        let shard_repl_state = repl_state.clone();
        let shard_acl_table = acl_table.clone();
        let shard_runtime_config = runtime_config_shared.clone();
        let shard_server_config = server_config_shared.clone();
        let shard_spsc_notify = mesh.take_notify(id);
        let shard_all_notifiers = all_notifiers.clone();
        let shard_dbs = shard_databases.clone();
        let shard_pubsub_regs = all_pubsub_registries.clone();
        let shard_remote_sub_maps = all_remote_sub_maps.clone();
        let shard_affinity = affinity_tracker.clone();

        let handle = std::thread::Builder::new()
            .name(format!("embedded-moon-shard-{}", id))
            .spawn(move || {
                crate::shard::numa::pin_to_core(id);
                RuntimeFactoryImpl::block_on_local(
                    format!("embedded-moon-shard-{}", id),
                    async move {
                        shard
                            .run(
                                conn_rx,
                                None, // tls
                                consumers,
                                producers,
                                shard_cancel,
                                shard_aof_tx,
                                Some(shard_bind_addr),
                                shard_persistence_dir,
                                shard_snap_rx,
                                shard_snap_tx,
                                Some(shard_repl_state),
                                None, // cluster_state
                                config_port,
                                shard_acl_table,
                                shard_runtime_config,
                                shard_server_config,
                                shard_spsc_notify,
                                shard_all_notifiers,
                                shard_dbs,
                                shard_pubsub_regs,
                                shard_remote_sub_maps,
                                shard_affinity,
                            )
                            .await;
                    },
                );
            })
            .context("embedded moon: failed to spawn shard thread")?;
        shard_handles.push(handle);
    }

    // Auto-save timer (no-op when save rules unset).
    let change_counter = Arc::new(AtomicU64::new(0));
    if config.save.is_some() {
        let rules = crate::persistence::auto_save::parse_save_rules(&config.save);
        if !rules.is_empty() {
            let auto_save_token = cancel.child_token();
            let auto_save_counter = change_counter.clone();
            let auto_save_snap_tx = snap_tx.clone();
            tokio::spawn(crate::persistence::auto_save::run_auto_save_sharded(
                rules,
                auto_save_counter,
                auto_save_token,
                auto_save_snap_tx,
            ));
            info!("embedded moon: auto-save timer started");
        }
    }

    // Run the sharded listener until cancelled.
    let per_shard_accept = cfg!(target_os = "linux");
    let listener_result = server::listener::run_sharded(
        config,
        conn_txs,
        cancel.clone(),
        per_shard_accept,
        affinity_tracker,
    )
    .await;

    if let Err(e) = &listener_result {
        tracing::error!("embedded moon: listener error: {}", e);
    }

    // Listener exited (cancel fired or fatal error). Flush AOF and cancel
    // any remaining shard work.
    if let Some(ref tx) = aof_tx {
        let _ = tx.send(AofMessage::Shutdown);
    }
    cancel.cancel();

    // Join shard threads on a blocking task — these are std::thread handles
    // owning current-thread runtimes and cannot be joined from async.
    let join_result = tokio::task::spawn_blocking(move || {
        for handle in shard_handles {
            let _ = handle.join();
        }
    })
    .await;
    if let Err(e) = join_result {
        tracing::warn!("embedded moon: shard-join task failed: {}", e);
    }

    info!("embedded moon: shut down");
    listener_result
}
