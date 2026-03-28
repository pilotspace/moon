//! Shard event loop: the `run()` method with the tokio/monoio select! loop.
//!
//! Extracted from shard/mod.rs. The select! arms call into sub-handler modules
//! (spsc_handler, persistence_tick, conn_accept, timers, uring_handler).

use std::cell::RefCell;
use std::os::unix::io::FromRawFd;
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

use super::shared_databases::ShardDatabases;

#[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
use crate::io::{UringConfig, UringDriver};

use super::dispatch::ShardMessage;
#[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
use super::uring_handler;
use super::{conn_accept, persistence_tick, spsc_handler, timers};

impl super::Shard {
    /// Run the shard event loop on its dedicated current_thread runtime.
    ///
    /// Wraps shard databases, pubsub registry, and SPSC producers in `Rc<RefCell<...>>`
    /// (safe because the runtime is single-threaded -- cooperative scheduling prevents
    /// concurrent borrows).
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
        shard_databases: Arc<ShardDatabases>,
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

        #[cfg(not(all(target_os = "linux", feature = "runtime-tokio")))]
        {
            let _ = &bind_addr; // Suppress unused warning when io_uring path inactive
            info!("Shard {} started", self.id);
        }

        let dispatch_tx = Rc::new(RefCell::new(producers));
        let pubsub_rc = Rc::new(RefCell::new(std::mem::take(&mut self.pubsub_registry)));
        let tracking_rc = Rc::new(RefCell::new(TrackingTable::new()));
        let shard_id = self.id;
        let blocking_rc = Rc::new(RefCell::new(BlockingRegistry::new(shard_id)));
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

        loop {
            #[cfg(feature = "runtime-tokio")]
            tokio::select! {
                // Accept new connections from listener
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
                                &shard_databases, &dispatch_tx, &pubsub_rc, &blocking_rc,
                                &shutdown, &aof_tx, &tracking_rc, &lua_rc, &script_cache_rc,
                                &acl_table, &runtime_config, &server_config, &all_notifiers,
                                &snapshot_trigger_tx, &repl_state, &cluster_state,
                                &cached_clock, shard_id, num_shards, config_port,
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
                        &shard_databases, &mut consumers, &mut *pubsub_rc.borrow_mut(),
                        &blocking_rc, &mut pending_snapshot, &mut snapshot_state,
                        &mut wal_writer, &mut repl_backlog, &mut replica_txs,
                        &repl_state, shard_id, &script_cache_rc, &cached_clock,
                        &mut pending_migrations,
                    );
                    persistence_tick::handle_pending_snapshot(
                        pending_snapshot, &mut snapshot_state, &mut snapshot_reply_tx,
                        &shard_databases, shard_id,
                    );
                    for (fd, _state) in pending_migrations.drain(..) {
                        tracing::warn!("Shard {}: MigrateConnection received but spawn not yet wired (fd={})", shard_id, fd);
                        // TODO: Plan 50-02 will wire spawn_migrated_connection here
                        // Close the FD via OwnedFd drop to avoid leaking.
                        let _ = unsafe { std::os::unix::io::OwnedFd::from_raw_fd(fd) };
                    }
                }
                // Periodic 1ms timer for WAL flush, snapshot advance, io_uring poll
                _ = periodic_interval.tick() => {
                    cached_clock.update();

                    let mut pending_snapshot = None;
                    spsc_handler::drain_spsc_shared(
                        &shard_databases, &mut consumers, &mut *pubsub_rc.borrow_mut(),
                        &blocking_rc, &mut pending_snapshot, &mut snapshot_state,
                        &mut wal_writer, &mut repl_backlog, &mut replica_txs,
                        &repl_state, shard_id, &script_cache_rc, &cached_clock,
                        &mut pending_migrations,
                    );
                    persistence_tick::handle_pending_snapshot(
                        pending_snapshot, &mut snapshot_state, &mut snapshot_reply_tx,
                        &shard_databases, shard_id,
                    );
                    for (fd, _state) in pending_migrations.drain(..) {
                        tracing::warn!("Shard {}: MigrateConnection received but spawn not yet wired (fd={})", shard_id, fd);
                        // TODO: Plan 50-02 will wire spawn_migrated_connection here
                        // Close the FD via OwnedFd drop to avoid leaking.
                        let _ = unsafe { std::os::unix::io::OwnedFd::from_raw_fd(fd) };
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
                // Accept new connections from listener
                stream = conn_rx.recv_async() => {
                    match stream {
                        Ok((std_tcp_stream, is_tls)) => {
                            conn_accept::spawn_monoio_connection(
                                std_tcp_stream, is_tls, &tls_config,
                                &shard_databases, &dispatch_tx, &pubsub_rc, &blocking_rc,
                                &shutdown, &aof_tx, &tracking_rc, &lua_rc, &script_cache_rc,
                                &acl_table, &runtime_config, &server_config, &all_notifiers,
                                &snapshot_trigger_tx, &repl_state, &cluster_state,
                                &cached_clock, shard_id, num_shards, config_port,
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
                        &shard_databases, &mut consumers, &mut *pubsub_rc.borrow_mut(),
                        &blocking_rc, &mut pending_snapshot, &mut snapshot_state,
                        &mut wal_writer, &mut repl_backlog, &mut replica_txs,
                        &repl_state, shard_id, &script_cache_rc, &cached_clock,
                        &mut pending_migrations,
                    );
                    persistence_tick::handle_pending_snapshot(
                        pending_snapshot, &mut snapshot_state, &mut snapshot_reply_tx,
                        &shard_databases, shard_id,
                    );
                    for (fd, _state) in pending_migrations.drain(..) {
                        tracing::warn!("Shard {}: MigrateConnection received but spawn not yet wired (fd={})", shard_id, fd);
                        // TODO: Plan 50-02 will wire spawn_migrated_connection here
                        // Close the FD via OwnedFd drop to avoid leaking.
                        let _ = unsafe { std::os::unix::io::OwnedFd::from_raw_fd(fd) };
                    }
                }
                // Periodic 1ms timer for WAL flush, snapshot advance, SPSC safety net
                _ = periodic_interval.tick() => {
                    cached_clock.update();

                    let mut pending_snapshot = None;
                    spsc_handler::drain_spsc_shared(
                        &shard_databases, &mut consumers, &mut *pubsub_rc.borrow_mut(),
                        &blocking_rc, &mut pending_snapshot, &mut snapshot_state,
                        &mut wal_writer, &mut repl_backlog, &mut replica_txs,
                        &repl_state, shard_id, &script_cache_rc, &cached_clock,
                        &mut pending_migrations,
                    );
                    persistence_tick::handle_pending_snapshot(
                        pending_snapshot, &mut snapshot_state, &mut snapshot_reply_tx,
                        &shard_databases, shard_id,
                    );
                    for (fd, _state) in pending_migrations.drain(..) {
                        tracing::warn!("Shard {}: MigrateConnection received but spawn not yet wired (fd={})", shard_id, fd);
                        // TODO: Plan 50-02 will wire spawn_migrated_connection here
                        // Close the FD via OwnedFd drop to avoid leaking.
                        let _ = unsafe { std::os::unix::io::OwnedFd::from_raw_fd(fd) };
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
        self.pubsub_registry = match Rc::try_unwrap(pubsub_rc) {
            Ok(refcell) => refcell.into_inner(),
            Err(_) => {
                info!(
                    "Shard {}: could not reclaim pubsub_registry (outstanding Rc references)",
                    self.id
                );
                PubSubRegistry::new()
            }
        };
    }
}
