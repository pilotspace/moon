//! Shard event loop: the `run()` method with the tokio/monoio select! loop.
//!
//! Extracted from shard/mod.rs. The select! arms call into sub-handler modules
//! (spsc_handler, persistence_tick, conn_accept, timers, uring_handler).

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use ringbuf::HeapCons;
use ringbuf::HeapProd;
use tracing::info;

use crate::blocking::BlockingRegistry;
use crate::config::RuntimeConfig;
use crate::persistence::snapshot::SnapshotState;
use crate::persistence::wal::WalWriter;
use crate::pubsub::PubSubRegistry;
use crate::replication::backlog::ReplicationBacklog;
use crate::replication::state::ReplicationState;
use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use crate::runtime::{
    TimerImpl,
    traits::{RuntimeInterval, RuntimeTimer},
};
use crate::storage::entry::CachedClock;
use crate::tracking::TrackingTable;

#[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
use crate::io::{UringConfig, UringDriver};

use super::dispatch::ShardMessage;
use super::remote_subscriber_map::RemoteSubscriberMap;
#[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
use super::uring_handler;
use super::{conn_accept, persistence_tick, spsc_handler, timers};

impl super::Shard {
    /// Run the shard event loop on its dedicated current_thread runtime.
    ///
    /// Wraps shard databases and SPSC producers in `Rc<RefCell<...>>`
    /// (safe because the runtime is single-threaded -- cooperative scheduling prevents
    /// concurrent borrows). PubSubRegistry uses `Arc<RwLock<>>` for cross-shard
    /// introspection reads.
    ///
    /// Receives new connections from the listener and spawns them as local tasks.
    /// Drains SPSC consumers for cross-shard dispatch requests and PubSubFanOut.
    /// Runs cooperative active expiry. Shuts down gracefully on cancellation.
    pub async fn run(
        &mut self,
        conn_rx: channel::MpscReceiver<(crate::runtime::TcpStream, bool)>,
        tls_config: Option<std::sync::Arc<rustls::ServerConfig>>,
        mut consumers: Vec<HeapCons<ShardMessage>>,
        producers: Vec<HeapProd<ShardMessage>>,
        shutdown: CancellationToken,
        aof_tx: Option<channel::MpscSender<crate::persistence::aof::AofMessage>>,
        bind_addr: Option<String>,
        persistence_dir: Option<String>,
        snapshot_trigger_rx: channel::WatchReceiver<u64>,
        snapshot_trigger_tx: channel::WatchSender<u64>,
        repl_state_ext: Option<Arc<RwLock<ReplicationState>>>,
        cluster_state: Option<std::sync::Arc<std::sync::RwLock<crate::cluster::ClusterState>>>,
        config_port: u16,
        acl_table: Arc<RwLock<crate::acl::AclTable>>,
        runtime_config: Arc<RwLock<RuntimeConfig>>,
        server_config: Arc<crate::config::ServerConfig>,
        spsc_notify: Arc<channel::Notify>,
        all_notifiers: Vec<Arc<channel::Notify>>,
        all_pubsub_registries: Vec<Arc<RwLock<PubSubRegistry>>>,
        all_remote_sub_maps: Vec<Arc<RwLock<RemoteSubscriberMap>>>,
    ) {
        // On Linux with tokio runtime, attempt to initialize io_uring for high-performance I/O.
        #[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
        let mut uring_state: Option<UringDriver> = {
            if std::env::var("MOON_NO_URING").is_ok() {
                info!("Shard {} io_uring disabled via MOON_NO_URING", self.id);
                None
            } else {
                match UringDriver::new(UringConfig::default()) {
                    Ok(mut d) => match d.init() {
                        Ok(()) => {
                            info!("Shard {} started (io_uring mode)", self.id);
                            Some(d)
                        }
                        Err(e) => {
                            info!("Shard {} io_uring init failed: {}, using Tokio", self.id, e);
                            None
                        }
                    },
                    Err(e) => {
                        info!("Shard {} io_uring unavailable: {}, using Tokio", self.id, e);
                        None
                    }
                }
            }
        };

        // Wire multishot accept: create per-shard SO_REUSEPORT listener socket
        #[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
        let mut uring_listener_fd: Option<std::os::fd::RawFd> = None;
        #[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
        if let Some(ref mut d) = uring_state {
            if let Some(ref addr) = bind_addr {
                match uring_handler::create_reuseport_listener(addr) {
                    Ok(listener_fd) => {
                        if let Err(e) = d.submit_multishot_accept(listener_fd) {
                            tracing::warn!(
                                "Shard {}: multishot accept failed: {}, using conn_rx",
                                self.id,
                                e
                            );
                        } else {
                            info!(
                                "Shard {}: multishot accept armed on fd {}",
                                self.id, listener_fd
                            );
                            uring_listener_fd = Some(listener_fd);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Shard {}: SO_REUSEPORT bind failed: {}, using conn_rx",
                            self.id,
                            e
                        );
                    }
                }
            }
        }

        // Track per-connection parse state for io_uring path (Linux + tokio only).
        #[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
        let mut uring_parse_bufs: std::collections::HashMap<u32, bytes::BytesMut> =
            std::collections::HashMap::new();

        // Track in-flight send buffers for proper RAII cleanup (Linux + tokio only).
        #[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
        let mut inflight_sends: std::collections::HashMap<
            u32,
            Vec<uring_handler::InFlightSend>,
        > = std::collections::HashMap::new();

        // Per-shard SO_REUSEPORT listener (Linux + tokio, non-uring path).
        // When io_uring is active, multishot accept handles this already.
        // When io_uring is NOT active (MOON_NO_URING or init failure), create a tokio TcpListener.
        #[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
        let per_shard_listener: Option<tokio::net::TcpListener> = {
            if uring_state.is_none() {
                if let Some(ref addr) = bind_addr {
                    match conn_accept::create_reuseport_socket(addr) {
                        Ok(std_listener) => match tokio::net::TcpListener::from_std(std_listener) {
                            Ok(tl) => {
                                info!(
                                    "Shard {}: per-shard SO_REUSEPORT listener on {}",
                                    self.id, addr
                                );
                                Some(tl)
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Shard {}: tokio listener from_std failed: {}, using conn_rx",
                                    self.id,
                                    e
                                );
                                None
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                "Shard {}: SO_REUSEPORT bind failed: {}, using conn_rx",
                                self.id,
                                e
                            );
                            None
                        }
                    }
                } else {
                    None
                }
            } else {
                None // io_uring handles accept via multishot
            }
        };

        // Per-shard SO_REUSEPORT listener (Linux + monoio).
        // Each shard creates its own listener; the kernel distributes connections via SO_REUSEPORT.
        #[cfg(all(target_os = "linux", feature = "runtime-monoio"))]
        let per_shard_monoio_listener: Option<monoio::net::TcpListener> = {
            if let Some(ref addr) = bind_addr {
                match conn_accept::create_reuseport_socket(addr) {
                    Ok(std_listener) => match monoio::net::TcpListener::from_std(std_listener) {
                        Ok(ml) => {
                            info!(
                                "Shard {}: per-shard SO_REUSEPORT listener on {} (monoio)",
                                self.id, addr
                            );
                            Some(ml)
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Shard {}: monoio listener from_std failed: {}, using conn_rx",
                                self.id,
                                e
                            );
                            None
                        }
                    },
                    Err(e) => {
                        tracing::warn!(
                            "Shard {}: SO_REUSEPORT bind failed: {}, using conn_rx",
                            self.id,
                            e
                        );
                        None
                    }
                }
            } else {
                None
            }
        };

        #[cfg(not(any(
            all(target_os = "linux", feature = "runtime-tokio"),
            all(target_os = "linux", feature = "runtime-monoio"),
        )))]
        {
            let _ = &bind_addr; // Suppress unused warning when per-shard accept inactive
            info!("Shard {} started", self.id);
        }

        #[cfg(all(target_os = "linux", feature = "runtime-monoio"))]
        {
            if per_shard_monoio_listener.is_none() {
                info!("Shard {} started (monoio, conn_rx fallback)", self.id);
            }
        }

        let dispatch_tx = Rc::new(RefCell::new(producers));
        // Use pre-shared Arc<RwLock<PubSubRegistry>> for this shard.
        // Initialize with shard's restored registry data (from persistence/snapshot).
        let pubsub_arc = all_pubsub_registries[self.id].clone();
        {
            let mut reg = pubsub_arc.write().unwrap();
            *reg = std::mem::take(&mut self.pubsub_registry);
        }
        let tracking_rc = Rc::new(RefCell::new(TrackingTable::new()));
        let shard_id = self.id;
        let blocking_rc = Rc::new(RefCell::new(BlockingRegistry::new(shard_id)));
        let remote_sub_map_arc = all_remote_sub_maps[self.id].clone();
        let num_shards = self.num_shards;

        // Lazy per-shard Lua VM: deferred until first EVAL/EVALSHA to save ~1.5MB/shard.
        let lua_rc: Rc<RefCell<Option<Rc<mlua::Lua>>>> = Rc::new(RefCell::new(None));
        let script_cache_rc = Rc::new(RefCell::new(crate::scripting::ScriptCache::new()));

        // Per-shard snapshot state (None when no snapshot is active)
        let mut snapshot_state: Option<SnapshotState> = None;
        let mut snapshot_reply_tx: Option<channel::OneshotSender<Result<(), String>>> = None;

        // Per-shard WAL writer (created only when persistence is actually enabled).
        let appendonly_enabled = runtime_config
            .read()
            .map(|cfg| cfg.appendonly != "no")
            .unwrap_or(false);
        let mut wal_writer: Option<WalWriter> = if let Some(ref dir) = persistence_dir {
            if appendonly_enabled {
                match WalWriter::new(shard_id, std::path::Path::new(dir)) {
                    Ok(w) => {
                        info!("Shard {}: WAL writer initialized", shard_id);
                        Some(w)
                    }
                    Err(e) => {
                        tracing::warn!("Shard {}: WAL init failed: {}", shard_id, e);
                        None
                    }
                }
            } else {
                info!(
                    "Shard {}: WAL skipped (appendonly disabled, snapshot-only persistence)",
                    shard_id
                );
                None
            }
        } else {
            None
        };

        // Per-shard replication backlog (lazy: allocated on first RegisterReplica).
        let mut repl_backlog: Option<ReplicationBacklog> = None;
        let mut replica_txs: Vec<(u64, channel::MpscSender<bytes::Bytes>)> = Vec::new();
        let repl_state: Option<Arc<RwLock<ReplicationState>>> = repl_state_ext;

        // Track last seen snapshot epoch to detect watch channel triggers
        let mut last_snapshot_epoch = snapshot_trigger_rx.borrow();

        let mut expiry_interval = TimerImpl::interval(Duration::from_millis(100));
        let mut eviction_interval = TimerImpl::interval(Duration::from_millis(100));
        let mut periodic_interval = TimerImpl::interval(Duration::from_millis(1));
        let mut block_timeout_interval = TimerImpl::interval(Duration::from_millis(10));
        let mut wal_sync_interval = TimerImpl::interval(Duration::from_secs(1));
        let spsc_notify_local = spsc_notify;

        // Per-shard cached clock: updated once per 1ms tick.
        let cached_clock = CachedClock::new();

        // Pending FD migrations collected from SPSC drain (spawn wired in Plan 50-02).
        let mut pending_migrations: Vec<(
            std::os::unix::io::RawFd,
            crate::server::conn::affinity::MigratedConnectionState,
        )> = Vec::new();

        // Pending wakers for monoio cross-shard write dispatch.
        // monoio's !Send single-threaded executor doesn't see cross-thread Waker::wake()
        // from flume oneshot channels. Connection tasks register their waker here; the
        // event loop drains and wakes them after every SPSC processing cycle (~1ms).
        #[cfg(feature = "runtime-monoio")]
        let pending_wakers: Rc<RefCell<Vec<std::task::Waker>>> = Rc::new(RefCell::new(Vec::new()));

        loop {
            #[cfg(feature = "runtime-tokio")]
            tokio::select! {
                // Per-shard SO_REUSEPORT accept (Linux only, non-uring tokio path)
                result = async {
                    #[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
                    if let Some(ref listener) = per_shard_listener {
                        return listener.accept().await;
                    }
                    // Never resolves on non-Linux or when per_shard_listener is None
                    std::future::pending::<std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)>>().await
                } => {
                    match result {
                        Ok((tcp_stream, _addr)) => {
                            conn_accept::spawn_tokio_connection(
                                tcp_stream, false, &tls_config,
                                &shard_databases, &dispatch_tx, &pubsub_arc, &blocking_rc,
                                &shutdown, &aof_tx, &tracking_rc, &lua_rc, &script_cache_rc,
                                &acl_table, &runtime_config, &server_config, &all_notifiers,
                                &snapshot_trigger_tx, &repl_state, &cluster_state,
                                &cached_clock, shard_id, num_shards, config_port,
                            );
                        }
                        Err(e) => {
                            tracing::error!("Shard {}: per-shard accept error: {}", shard_id, e);
                        }
                    }
                }
                // Accept new connections from listener (MPSC fallback, always active on non-Linux)
                stream = conn_rx.recv_async() => {
                    match stream {
                        Ok((tcp_stream, is_tls)) => {
                            // On Linux with io_uring: extract raw fd, register with UringDriver.
                            #[cfg(target_os = "linux")]
                            {
                                if !is_tls {
                                    if let Some(ref mut driver) = uring_state {
                                        match tcp_stream.into_std() {
                                            Ok(std_stream) => {
                                                use std::os::unix::io::IntoRawFd;
                                                let raw_fd = std_stream.into_raw_fd();
                                                match driver.register_connection(raw_fd) {
                                                    Ok(Some(_conn_id)) => {}
                                                    Ok(None) => {}
                                                    Err(e) => {
                                                        tracing::warn!("Shard {}: register_connection error: {}", shard_id, e);
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                tracing::warn!("Shard {}: into_std failed: {}", shard_id, e);
                                            }
                                        }
                                        continue;
                                    }
                                }
                            }

                            conn_accept::spawn_tokio_connection(
                                tcp_stream, is_tls, &tls_config,
                                &shard_databases, &dispatch_tx, &pubsub_arc, &blocking_rc,
                                &shutdown, &aof_tx, &tracking_rc, &lua_rc, &script_cache_rc,
                                &acl_table, &runtime_config, &server_config, &all_notifiers,
                                &snapshot_trigger_tx, &repl_state, &cluster_state,
                                &cached_clock, &remote_sub_map_arc, &all_pubsub_registries,
                                &all_remote_sub_maps,
                                shard_id, num_shards, config_port,
                            );
                        }
                        Err(_) => {
                            info!("Shard {} connection channel closed", self.id);
                            break;
                        }
                    }
                }
                // SPSC notify -- event-driven cross-shard message drain
                _ = spsc_notify_local.notified() => {
                    let mut pending_snapshot = None;
                    spsc_handler::drain_spsc_shared(
                        &shard_databases, &mut consumers, &mut *pubsub_arc.write().unwrap(),
                        &blocking_rc, &mut pending_snapshot, &mut snapshot_state,
                        &mut wal_writer, &mut repl_backlog, &mut replica_txs,
                        &repl_state, shard_id, &script_cache_rc, &cached_clock,
                        &mut pending_migrations,
                    );
                    persistence_tick::handle_pending_snapshot(
                        pending_snapshot, &mut snapshot_state, &mut snapshot_reply_tx,
                        &shard_databases, shard_id,
                    );
                    for (fd, state) in pending_migrations.drain(..) {
                        tracing::info!(
                            "Shard {}: accepting migrated connection (fd={}, client_id={}, from={})",
                            shard_id, fd, state.client_id, state.peer_addr
                        );
                        #[cfg(feature = "runtime-tokio")]
                        {
                            conn_accept::spawn_migrated_tokio_connection(
                                fd, state,
                                &shard_databases, &dispatch_tx, &pubsub_arc, &blocking_rc,
                                &shutdown, &aof_tx, &tracking_rc, &lua_rc, &script_cache_rc,
                                &acl_table, &runtime_config, &server_config, &all_notifiers,
                                &snapshot_trigger_tx, &repl_state, &cluster_state,
                                &cached_clock, shard_id, num_shards, config_port,
                            );
                        }
                        #[cfg(feature = "runtime-monoio")]
                        {
                            conn_accept::spawn_migrated_monoio_connection(
                                fd, state,
                                &shard_databases, &dispatch_tx, &pubsub_arc, &blocking_rc,
                                &shutdown, &aof_tx, &tracking_rc, &lua_rc, &script_cache_rc,
                                &acl_table, &runtime_config, &server_config, &all_notifiers,
                                &snapshot_trigger_tx, &repl_state, &cluster_state,
                                &cached_clock, shard_id, num_shards, config_port,
                                &pending_wakers,
                            );
                        }
                    }
                }
                // Periodic 1ms timer for WAL flush, snapshot advance, io_uring poll
                _ = periodic_interval.tick() => {
                    cached_clock.update();

                    let mut pending_snapshot = None;
                    spsc_handler::drain_spsc_shared(
                        &shard_databases, &mut consumers, &mut *pubsub_arc.write().unwrap(),
                        &blocking_rc, &mut pending_snapshot, &mut snapshot_state,
                        &mut wal_writer, &mut repl_backlog, &mut replica_txs,
                        &repl_state, shard_id, &script_cache_rc, &cached_clock,
                        &mut pending_migrations,
                    );
                    persistence_tick::handle_pending_snapshot(
                        pending_snapshot, &mut snapshot_state, &mut snapshot_reply_tx,
                        &shard_databases, shard_id,
                    );
                    for (fd, state) in pending_migrations.drain(..) {
                        tracing::info!(
                            "Shard {}: accepting migrated connection (fd={}, client_id={}, from={})",
                            shard_id, fd, state.client_id, state.peer_addr
                        );
                        #[cfg(feature = "runtime-tokio")]
                        {
                            conn_accept::spawn_migrated_tokio_connection(
                                fd, state,
                                &shard_databases, &dispatch_tx, &pubsub_arc, &blocking_rc,
                                &shutdown, &aof_tx, &tracking_rc, &lua_rc, &script_cache_rc,
                                &acl_table, &runtime_config, &server_config, &all_notifiers,
                                &snapshot_trigger_tx, &repl_state, &cluster_state,
                                &cached_clock, shard_id, num_shards, config_port,
                            );
                        }
                        #[cfg(feature = "runtime-monoio")]
                        {
                            conn_accept::spawn_migrated_monoio_connection(
                                fd, state,
                                &shard_databases, &dispatch_tx, &pubsub_arc, &blocking_rc,
                                &shutdown, &aof_tx, &tracking_rc, &lua_rc, &script_cache_rc,
                                &acl_table, &runtime_config, &server_config, &all_notifiers,
                                &snapshot_trigger_tx, &repl_state, &cluster_state,
                                &cached_clock, shard_id, num_shards, config_port,
                                &pending_wakers,
                            );
                        }
                    }

                    persistence_tick::check_auto_save_trigger(
                        &snapshot_trigger_rx, &mut last_snapshot_epoch,
                        &mut snapshot_state, &shard_databases, &persistence_dir, shard_id,
                    );

                    // Advance snapshot one segment per tick (cooperative)
                    if persistence_tick::advance_snapshot_segment(
                        &mut snapshot_state,
                        &shard_databases,
                        shard_id,
                    ) {
                        if let Some(snap) = snapshot_state.as_mut() {
                            if let Err(e) = snap.finalize_async().await {
                                persistence_tick::finalize_snapshot_error(
                                    &mut snapshot_state, &mut snapshot_reply_tx, shard_id,
                                    &e.to_string(),
                                );
                            } else {
                                persistence_tick::finalize_snapshot_success(
                                    &mut snapshot_state, &mut snapshot_reply_tx,
                                    &mut wal_writer, shard_id,
                                );
                            }
                        }
                    }

                    persistence_tick::flush_wal_if_needed(&mut wal_writer);

                    // On Linux: poll io_uring for completions (non-blocking)
                    #[cfg(target_os = "linux")]
                    if let Some(ref mut driver) = uring_state {
                        let _ = driver.submit_and_wait_nonblocking();
                        let events = driver.drain_completions();
                        for event in events {
                            uring_handler::handle_uring_event(
                                event, driver, &shard_databases, shard_id, &mut uring_parse_bufs,
                                &mut inflight_sends, uring_listener_fd, &cached_clock,
                            );
                        }
                    }
                }
                // WAL fsync on 1-second interval
                _ = wal_sync_interval.tick() => {
                    timers::sync_wal(&mut wal_writer);
                }
                // Expire timed-out blocked clients every 10ms
                _ = block_timeout_interval.tick() => {
                    timers::expire_blocked_clients(&blocking_rc);
                }
                // Cooperative active expiry
                _ = expiry_interval.tick() => {
                    timers::run_active_expiry(&shard_databases, shard_id);
                }
                // Background eviction timer
                _ = eviction_interval.tick() => {
                    timers::run_eviction(&shard_databases, shard_id, &runtime_config);
                }
                _ = shutdown.cancelled() => {
                    info!("Shard {} shutting down", self.id);
                    if let Some(ref mut wal) = wal_writer {
                        let _ = wal.shutdown();
                    }
                    break;
                }
            }

            // Monoio runtime: full event loop mirroring the tokio path.
            #[cfg(feature = "runtime-monoio")]
            monoio::select! {
                // Per-shard SO_REUSEPORT accept (Linux only, monoio path)
                result = async {
                    #[cfg(all(target_os = "linux", feature = "runtime-monoio"))]
                    if let Some(ref listener) = per_shard_monoio_listener {
                        return listener.accept().await;
                    }
                    // Never resolves on non-Linux or when per_shard_monoio_listener is None
                    std::future::pending::<std::io::Result<(monoio::net::TcpStream, std::net::SocketAddr)>>().await
                } => {
                    match result {
                        Ok((stream, _addr)) => {
                            // Convert monoio TcpStream -> std::net::TcpStream (same pattern as listener.rs)
                            let std_stream = {
                                use std::os::unix::io::{IntoRawFd, FromRawFd};
                                let fd = stream.into_raw_fd();
                                unsafe { std::net::TcpStream::from_raw_fd(fd) }
                            };
                            conn_accept::spawn_monoio_connection(
                                std_stream, false, &tls_config,
                                &shard_databases, &dispatch_tx, &pubsub_arc, &blocking_rc,
                                &shutdown, &aof_tx, &tracking_rc, &lua_rc, &script_cache_rc,
                                &acl_table, &runtime_config, &server_config, &all_notifiers,
                                &snapshot_trigger_tx, &repl_state, &cluster_state,
                                &cached_clock, shard_id, num_shards, config_port,
                                &pending_wakers,
                            );
                        }
                        Err(e) => {
                            tracing::error!("Shard {}: per-shard accept error (monoio): {}", shard_id, e);
                        }
                    }
                }
                // Accept new connections from listener (MPSC fallback, always active on non-Linux)
                stream = conn_rx.recv_async() => {
                    match stream {
                        Ok((std_tcp_stream, is_tls)) => {
                            conn_accept::spawn_monoio_connection(
                                std_tcp_stream, is_tls, &tls_config,
                                &shard_databases, &dispatch_tx, &pubsub_arc, &blocking_rc,
                                &shutdown, &aof_tx, &tracking_rc, &lua_rc, &script_cache_rc,
                                &acl_table, &runtime_config, &server_config, &all_notifiers,
                                &snapshot_trigger_tx, &repl_state, &cluster_state,
                                &cached_clock, &remote_sub_map_arc, &all_pubsub_registries,
                                &all_remote_sub_maps,
                                shard_id, num_shards, config_port,
                                &pending_wakers,
                            );
                        }
                        Err(_) => {
                            info!("Shard {} connection channel closed", self.id);
                            break;
                        }
                    }
                }
                // SPSC notify -- event-driven cross-shard message drain
                _ = spsc_notify_local.notified() => {
                    tracing::trace!("Shard {}: SPSC notify fired", shard_id);
                    let mut pending_snapshot = None;
                    spsc_handler::drain_spsc_shared(
                        &shard_databases, &mut consumers, &mut *pubsub_arc.write().unwrap(),
                        &blocking_rc, &mut pending_snapshot, &mut snapshot_state,
                        &mut wal_writer, &mut repl_backlog, &mut replica_txs,
                        &repl_state, shard_id, &script_cache_rc, &cached_clock,
                        &mut pending_migrations,
                    );
                    // Wake connection tasks waiting for cross-shard write responses.
                    // They'll try_recv() — if the response arrived, proceed; otherwise re-register.
                    for waker in pending_wakers.borrow_mut().drain(..) {
                        waker.wake();
                    }
                    persistence_tick::handle_pending_snapshot(
                        pending_snapshot, &mut snapshot_state, &mut snapshot_reply_tx,
                        &shard_databases, shard_id,
                    );
                    for (fd, state) in pending_migrations.drain(..) {
                        tracing::info!(
                            "Shard {}: accepting migrated connection (fd={}, client_id={}, from={})",
                            shard_id, fd, state.client_id, state.peer_addr
                        );
                        #[cfg(feature = "runtime-tokio")]
                        {
                            conn_accept::spawn_migrated_tokio_connection(
                                fd, state,
                                &shard_databases, &dispatch_tx, &pubsub_arc, &blocking_rc,
                                &shutdown, &aof_tx, &tracking_rc, &lua_rc, &script_cache_rc,
                                &acl_table, &runtime_config, &server_config, &all_notifiers,
                                &snapshot_trigger_tx, &repl_state, &cluster_state,
                                &cached_clock, shard_id, num_shards, config_port,
                            );
                        }
                        #[cfg(feature = "runtime-monoio")]
                        {
                            conn_accept::spawn_migrated_monoio_connection(
                                fd, state,
                                &shard_databases, &dispatch_tx, &pubsub_arc, &blocking_rc,
                                &shutdown, &aof_tx, &tracking_rc, &lua_rc, &script_cache_rc,
                                &acl_table, &runtime_config, &server_config, &all_notifiers,
                                &snapshot_trigger_tx, &repl_state, &cluster_state,
                                &cached_clock, shard_id, num_shards, config_port,
                                &pending_wakers,
                            );
                        }
                    }
                }
                // Periodic 1ms timer for WAL flush, snapshot advance, SPSC safety net
                _ = periodic_interval.tick() => {
                    tracing::trace!("Shard {}: periodic tick", shard_id);
                    cached_clock.update();

                    let mut pending_snapshot = None;
                    spsc_handler::drain_spsc_shared(
                        &shard_databases, &mut consumers, &mut *pubsub_arc.write().unwrap(),
                        &blocking_rc, &mut pending_snapshot, &mut snapshot_state,
                        &mut wal_writer, &mut repl_backlog, &mut replica_txs,
                        &repl_state, shard_id, &script_cache_rc, &cached_clock,
                        &mut pending_migrations,
                    );
                    // Wake connection tasks waiting for cross-shard write responses.
                    for waker in pending_wakers.borrow_mut().drain(..) {
                        waker.wake();
                    }
                    persistence_tick::handle_pending_snapshot(
                        pending_snapshot, &mut snapshot_state, &mut snapshot_reply_tx,
                        &shard_databases, shard_id,
                    );
                    for (fd, state) in pending_migrations.drain(..) {
                        tracing::info!(
                            "Shard {}: accepting migrated connection (fd={}, client_id={}, from={})",
                            shard_id, fd, state.client_id, state.peer_addr
                        );
                        #[cfg(feature = "runtime-tokio")]
                        {
                            conn_accept::spawn_migrated_tokio_connection(
                                fd, state,
                                &shard_databases, &dispatch_tx, &pubsub_arc, &blocking_rc,
                                &shutdown, &aof_tx, &tracking_rc, &lua_rc, &script_cache_rc,
                                &acl_table, &runtime_config, &server_config, &all_notifiers,
                                &snapshot_trigger_tx, &repl_state, &cluster_state,
                                &cached_clock, shard_id, num_shards, config_port,
                            );
                        }
                        #[cfg(feature = "runtime-monoio")]
                        {
                            conn_accept::spawn_migrated_monoio_connection(
                                fd, state,
                                &shard_databases, &dispatch_tx, &pubsub_arc, &blocking_rc,
                                &shutdown, &aof_tx, &tracking_rc, &lua_rc, &script_cache_rc,
                                &acl_table, &runtime_config, &server_config, &all_notifiers,
                                &snapshot_trigger_tx, &repl_state, &cluster_state,
                                &cached_clock, shard_id, num_shards, config_port,
                                &pending_wakers,
                            );
                        }
                    }

                    persistence_tick::check_auto_save_trigger(
                        &snapshot_trigger_rx, &mut last_snapshot_epoch,
                        &mut snapshot_state, &shard_databases, &persistence_dir, shard_id,
                    );

                    // Advance snapshot one segment per tick (cooperative)
                    if persistence_tick::advance_snapshot_segment(
                        &mut snapshot_state,
                        &shard_databases,
                        shard_id,
                    ) {
                        if let Some(snap) = snapshot_state.as_mut() {
                            if let Err(e) = snap.finalize_async().await {
                                persistence_tick::finalize_snapshot_error(
                                    &mut snapshot_state, &mut snapshot_reply_tx, shard_id,
                                    &e.to_string(),
                                );
                                crate::command::persistence::bgsave_shard_done(false);
                            } else {
                                persistence_tick::finalize_snapshot_success(
                                    &mut snapshot_state, &mut snapshot_reply_tx,
                                    &mut wal_writer, shard_id,
                                );
                                crate::command::persistence::bgsave_shard_done(true);
                            }
                        }
                    }

                    persistence_tick::flush_wal_if_needed(&mut wal_writer);
                }
                // WAL fsync on 1-second interval
                _ = wal_sync_interval.tick() => {
                    timers::sync_wal(&mut wal_writer);
                }
                // Expire timed-out blocked clients every 10ms
                _ = block_timeout_interval.tick() => {
                    timers::expire_blocked_clients(&blocking_rc);
                }
                // Cooperative active expiry every 100ms
                _ = expiry_interval.tick() => {
                    timers::run_active_expiry(&shard_databases, shard_id);
                }
                // Background eviction timer
                _ = eviction_interval.tick() => {
                    timers::run_eviction(&shard_databases, shard_id, &runtime_config);
                }
                // Shutdown
                _ = shutdown.cancelled() => {
                    info!("Shard {} shutting down (monoio)", self.id);
                    if let Some(ref mut wal) = wal_writer {
                        let _ = wal.shutdown();
                    }
                    break;
                }
            }
        }

        // Close per-shard SO_REUSEPORT listener fd if created (Linux + tokio only).
        #[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
        if let Some(lfd) = uring_listener_fd {
            unsafe {
                libc::close(lfd);
            }
        }

        // Databases now live in Arc<ShardDatabases>, no reclaim needed.
        self.databases.clear();
        self.pubsub_registry = std::mem::take(&mut *pubsub_arc.write().unwrap());
    }
}
