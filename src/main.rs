#[cfg(not(feature = "jemalloc"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "jemalloc")]
#[unsafe(export_name = "malloc_conf")]
pub static JEMALLOC_CONF: &[u8] = b"percpu_arena:percpu,background_thread:true,metadata_thp:auto,dirty_decay_ms:5000,muzzy_decay_ms:30000,abort_conf:true\0";

use std::path::PathBuf;

use clap::Parser;
use moon::config::ServerConfig;
use moon::persistence::aof::{self, AofMessage, FsyncPolicy};
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::runtime::{RuntimeFactoryImpl, traits::RuntimeFactory};
use moon::server;
use moon::shard::Shard;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use moon::shard::shared_databases::ShardDatabases;
use tracing::info;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "moon=info".into()),
        )
        .init();

    let config = ServerConfig::parse();

    // Protected mode startup warning
    if config.protected_mode == "yes" && config.requirepass.is_none() && config.aclfile.is_none() {
        tracing::warn!(
            "WARNING: no password set. Protected mode is enabled. \
             Only loopback connections are accepted. \
             Use --requirepass or --protected-mode no to change this."
        );
    }

    // Build TLS configuration if tls_port is set
    let tls_config: Option<std::sync::Arc<rustls::ServerConfig>> = if config.tls_port > 0 {
        let cert = config
            .tls_cert_file
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("--tls-cert-file required when --tls-port is set"))?;
        let key = config
            .tls_key_file
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("--tls-key-file required when --tls-port is set"))?;
        let tls_cfg = moon::tls::build_tls_config(
            cert,
            key,
            config.tls_ca_cert_file.as_deref(),
            config.tls_ciphersuites.as_deref(),
        )
        .map_err(|e| anyhow::anyhow!("Failed to build TLS config: {}", e))?;
        info!(
            "TLS enabled on port {} (TLS 1.3, rustls + aws-lc-rs)",
            config.tls_port
        );
        Some(tls_cfg)
    } else {
        None
    };

    // Determine number of shards
    let num_shards = if config.shards == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    } else {
        config.shards
    };

    info!("Starting with {} shards", num_shards);

    // Create channel mesh for inter-shard communication
    let mut mesh = ChannelMesh::new(num_shards, CHANNEL_BUFFER_SIZE);

    // Shared cancellation token for graceful shutdown
    let cancel_token = CancellationToken::new();

    // Collect connection senders for the listener before spawning shard threads
    let conn_txs: Vec<_> = (0..num_shards).map(|i| mesh.conn_tx(i)).collect();

    // Set up AOF channel: single writer, all shards send to it via mpsc::Sender clones.
    // The AOF writer task will be spawned on the listener runtime.
    let aof_tx: Option<channel::MpscSender<AofMessage>> = if config.appendonly == "yes" {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(10_000);
        let aof_token = cancel_token.child_token();
        let fsync = FsyncPolicy::from_str(&config.appendfsync);
        let aof_file_path = PathBuf::from(&config.dir).join(&config.appendfilename);
        // AOF writer task will be spawned on the listener runtime (see below)
        // We store rx to spawn later since listener_rt hasn't been created yet.
        // Instead, spawn on a dedicated thread so it's available before listener starts.
        std::thread::Builder::new()
            .name("aof-writer".to_string())
            .spawn(move || {
                RuntimeFactoryImpl::block_on_local(
                    "aof-writer".to_string(),
                    aof::aof_writer_task(rx, aof_file_path, fsync, aof_token),
                );
            })
            .expect("failed to spawn AOF writer thread");
        info!(
            "AOF enabled with fsync policy: {:?}",
            FsyncPolicy::from_str(&config.appendfsync)
        );
        Some(tx)
    } else {
        None
    };

    // Compute bind address for SO_REUSEPORT per-shard listeners (Linux io_uring path).
    let bind_addr = format!("{}:{}", config.bind, config.port);

    // Create watch channel for snapshot triggers (auto-save and BGSAVE)
    let (snapshot_trigger_tx, snapshot_trigger_rx) = moon::runtime::channel::watch(0u64);

    // Persistence directory for per-shard WAL and snapshots.
    // Only set when persistence is actually enabled (appendonly=yes or save rules exist)
    // to avoid creating WAL writers that fsync on every tick for no benefit.
    let persistence_dir = if config.appendonly == "yes" || config.save.is_some() {
        Some(config.dir.clone())
    } else {
        None
    };

    // Create replication state -- load persisted repl_id or generate new one.
    let (repl_id, repl_id2) =
        moon::replication::state::load_replication_state(std::path::Path::new(&config.dir));
    let repl_state = std::sync::Arc::new(std::sync::RwLock::new(
        moon::replication::state::ReplicationState::new(num_shards, repl_id, repl_id2),
    ));

    // Cluster mode initialization
    let cluster_state: Option<std::sync::Arc<std::sync::RwLock<moon::cluster::ClusterState>>> =
        if config.cluster_enabled {
            moon::cluster::CLUSTER_ENABLED.store(true, std::sync::atomic::Ordering::Relaxed);
            let self_addr: std::net::SocketAddr = format!("{}:{}", config.bind, config.port)
                .parse()
                .expect("invalid bind address");
            let node_id = moon::replication::state::generate_repl_id();
            let state = moon::cluster::ClusterState::new(node_id, self_addr);
            let cs = std::sync::Arc::new(std::sync::RwLock::new(state));
            info!(
                "Cluster mode enabled, node ID: {}",
                cs.read().unwrap().node_id
            );
            Some(cs)
        } else {
            None
        };

    // Build ACL table from config (load aclfile if configured, else bootstrap from requirepass)
    let acl_table: std::sync::Arc<std::sync::RwLock<moon::acl::AclTable>> = {
        let table = moon::acl::AclTable::load_or_default(&config);
        std::sync::Arc::new(std::sync::RwLock::new(table))
    };

    // Build shared runtime config for sharded handlers
    let runtime_config_shared: std::sync::Arc<std::sync::RwLock<moon::config::RuntimeConfig>> =
        { std::sync::Arc::new(std::sync::RwLock::new(config.to_runtime_config())) };
    let server_config_shared: std::sync::Arc<moon::config::ServerConfig> =
        { std::sync::Arc::new(config.clone()) };

    // Collect all notifiers before spawning shard threads
    let all_notifiers = mesh.all_notifiers();

    // Pre-create shared pubsub registries for cross-shard introspection reads.
    let all_pubsub_registries: Vec<
        std::sync::Arc<parking_lot::RwLock<moon::pubsub::PubSubRegistry>>,
    > = (0..num_shards)
        .map(|_| std::sync::Arc::new(parking_lot::RwLock::new(moon::pubsub::PubSubRegistry::new())))
        .collect();

    // Pre-create shared remote subscriber maps for zero-SPSC subscription propagation.
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

    // Create shared affinity tracker for pub/sub connection routing
    let affinity_tracker = std::sync::Arc::new(parking_lot::RwLock::new(
        moon::shard::affinity::AffinityTracker::new(),
    ));

    // Create and restore all shards on main thread, then extract databases
    // into centralized ShardDatabases for cross-shard direct read access.
    let mut shards: Vec<Shard> = (0..num_shards)
        .map(|id| {
            let mut shard =
                Shard::new(id, num_shards, config.databases, config.to_runtime_config());
            if let Some(ref dir) = persistence_dir {
                shard.restore_from_persistence(dir);
            }
            shard
        })
        .collect();

    // Extract databases from all shards and wrap in ShardDatabases
    let all_dbs: Vec<Vec<moon::storage::Database>> = shards
        .iter_mut()
        .map(|s| std::mem::take(&mut s.databases))
        .collect();
    let shard_databases = ShardDatabases::new(all_dbs);

    // Spawn shard threads
    let mut shard_handles = Vec::with_capacity(num_shards);
    let config_port = config.port;
    for (id, mut shard) in shards.into_iter().enumerate() {
        let producers = mesh.take_producers(id);
        let consumers = mesh.take_consumers(id);
        let conn_rx = mesh.take_conn_rx(id);
        let shard_cancel = cancel_token.clone();
        let shard_aof_tx = aof_tx.clone();
        let shard_bind_addr = bind_addr.clone();
        let shard_persistence_dir = persistence_dir.clone();
        let shard_snap_rx = snapshot_trigger_rx.clone();
        let shard_snap_tx = snapshot_trigger_tx.clone();
        let shard_repl_state = repl_state.clone();
        let shard_cluster_state = cluster_state.clone();
        let shard_acl_table = acl_table.clone();
        let shard_runtime_config = runtime_config_shared.clone();
        let shard_server_config = server_config_shared.clone();
        let shard_spsc_notify = mesh.take_notify(id);
        let shard_all_notifiers = all_notifiers.clone();
        let shard_tls_config = tls_config.clone();
        let shard_dbs = shard_databases.clone();
        let shard_pubsub_registries = all_pubsub_registries.clone();
        let shard_remote_sub_maps = all_remote_sub_maps.clone();
        let shard_affinity = affinity_tracker.clone();

        let handle = std::thread::Builder::new()
            .name(format!("shard-{}", id))
            .spawn(move || {
                // Pin shard thread to core BEFORE any allocations (NUMA locality)
                moon::shard::numa::pin_to_core(id);

                RuntimeFactoryImpl::block_on_local(format!("shard-{}", id), async move {
                    shard
                        .run(
                            conn_rx,
                            shard_tls_config,
                            consumers,
                            producers,
                            shard_cancel,
                            shard_aof_tx,
                            Some(shard_bind_addr),
                            shard_persistence_dir,
                            shard_snap_rx,
                            shard_snap_tx,
                            Some(shard_repl_state),
                            shard_cluster_state,
                            config_port,
                            shard_acl_table,
                            shard_runtime_config,
                            shard_server_config,
                            shard_spsc_notify,
                            shard_all_notifiers,
                            shard_dbs,
                            shard_pubsub_registries,
                            shard_remote_sub_maps,
                            shard_affinity,
                        )
                        .await;
                });
            })
            .expect("failed to spawn shard thread");

        shard_handles.push(handle);
    }

    // Set up change counter for auto-save
    let change_counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

    let listener_cancel = cancel_token.clone();

    // Run the sharded listener on the main thread.
    // Under tokio: uses current_thread runtime with tokio::spawn for background tasks.
    // Under monoio: uses monoio RuntimeFactory with simplified startup (cluster/gossip
    //   not yet supported under monoio).
    #[cfg(feature = "runtime-tokio")]
    {
        let listener_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build listener runtime");

        listener_rt.block_on(async {
            // Set up auto-save timer if save rules are configured (sharded mode)
            if config.save.is_some() {
                let rules = moon::persistence::auto_save::parse_save_rules(&config.save);
                if !rules.is_empty() {
                    let auto_save_token = cancel_token.child_token();
                    let auto_save_counter = change_counter.clone();
                    tokio::spawn(moon::persistence::auto_save::run_auto_save_sharded(
                        rules,
                        auto_save_counter,
                        auto_save_token,
                        snapshot_trigger_tx,
                    ));
                    info!("Auto-save timer started (sharded mode)");
                }
            }

            // Start cluster bus and gossip ticker when cluster mode is enabled
            if let Some(ref cs) = cluster_state {
                let cluster_port = (config.port as u32 + 10000) as u16;
                let cs_clone = cs.clone();
                let bus_cancel = cancel_token.child_token();
                let bind2 = config.bind.clone();
                let self_addr: std::net::SocketAddr =
                    format!("{}:{}", config.bind, config.port).parse().unwrap();

                // Shared vote channel: gossip ticker sets sender when election starts,
                // bus handler forwards FailoverAuthAck votes through it.
                let failover_vote_tx: moon::cluster::bus::SharedVoteTx =
                    std::sync::Arc::new(parking_lot::Mutex::new(None));

                let bus_vote_tx = failover_vote_tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = moon::cluster::bus::run_cluster_bus(
                        &bind2,
                        cluster_port,
                        self_addr,
                        cs_clone,
                        bus_cancel,
                        bus_vote_tx,
                    )
                    .await
                    {
                        tracing::error!("Cluster bus error: {}", e);
                    }
                });

                let cs_gossip = cs.clone();
                let gossip_cancel = cancel_token.child_token();
                let node_timeout = config.cluster_node_timeout;
                let self_addr2: std::net::SocketAddr =
                    format!("{}:{}", config.bind, config.port).parse().unwrap();
                let gossip_vote_tx = failover_vote_tx.clone();
                let gossip_repl_state = repl_state.clone();
                tokio::spawn(async move {
                    moon::cluster::gossip::run_gossip_ticker(
                        self_addr2,
                        cs_gossip,
                        node_timeout,
                        gossip_cancel,
                        gossip_vote_tx,
                        gossip_repl_state,
                    )
                    .await;
                });
                info!("Cluster bus and gossip ticker started");
            }

            let per_shard_accept = cfg!(target_os = "linux");
            if let Err(e) = server::listener::run_sharded(
                config,
                conn_txs,
                listener_cancel,
                per_shard_accept,
                affinity_tracker,
            )
            .await
            {
                tracing::error!("Listener error: {}", e);
            }
        });
    }

    #[cfg(feature = "runtime-monoio")]
    {
        // Monoio listener: simplified startup. Cluster bus and gossip not yet
        // supported under monoio.

        // Auto-save runs on a dedicated thread (same pattern as AOF writer).
        if config.save.is_some() {
            let rules = moon::persistence::auto_save::parse_save_rules(&config.save);
            if !rules.is_empty() {
                let auto_save_token = cancel_token.child_token();
                let auto_save_counter = change_counter.clone();
                let snap_tx = snapshot_trigger_tx;
                std::thread::Builder::new()
                    .name("auto-save".to_string())
                    .spawn(move || {
                        RuntimeFactoryImpl::block_on_local(
                            "auto-save".to_string(),
                            moon::persistence::auto_save::run_auto_save_sharded(
                                rules,
                                auto_save_counter,
                                auto_save_token,
                                snap_tx,
                            ),
                        );
                    })
                    .expect("failed to spawn auto-save thread");
                info!("Auto-save timer started (sharded mode, monoio)");
            }
        }

        let per_shard_accept = cfg!(target_os = "linux");
        RuntimeFactoryImpl::block_on_local("listener".to_string(), async move {
            if let Err(e) = server::listener::run_sharded(
                config,
                conn_txs,
                listener_cancel,
                per_shard_accept,
                affinity_tracker,
            )
            .await
            {
                tracing::error!("Listener error: {}", e);
            }
        });
    }

    // After listener exits, send AOF shutdown and cancel all shards
    if let Some(ref tx) = aof_tx {
        let _ = tx.send(AofMessage::Shutdown);
    }
    cancel_token.cancel();
    for handle in shard_handles {
        let _ = handle.join();
    }

    info!("Server shut down");
    Ok(())
}
