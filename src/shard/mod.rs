pub mod conn_accept;
pub mod coordinator;
pub mod dispatch;
pub mod mesh;
pub mod numa;
pub mod persistence_tick;
pub mod spsc_handler;
pub mod timers;
pub mod uring_handler;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use ringbuf::HeapCons;
use ringbuf::HeapProd;
use tracing::info;

use crate::blocking::BlockingRegistry;
use crate::config::RuntimeConfig;
use crate::persistence::replay::DispatchReplayEngine;
use crate::persistence::snapshot::SnapshotState;
use crate::persistence::wal::WalWriter;
use crate::pubsub::PubSubRegistry;
use crate::replication::backlog::ReplicationBacklog;
use crate::replication::state::ReplicationState;
use crate::runtime::{
    TimerImpl,
    traits::{RuntimeInterval, RuntimeTimer},
};
use crate::storage::Database;
use crate::storage::entry::CachedClock;
use crate::tracking::TrackingTable;

use std::sync::{Arc, RwLock};

#[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
use crate::io::{UringConfig, UringDriver};

use self::dispatch::ShardMessage;

/// A shard owns all per-core state. No Arc, no Mutex -- fully owned by its thread.
///
/// Each shard contains its own set of databases, runtime configuration, and will
/// eventually own its connection set and event loop. This is the fundamental unit
/// of the shared-nothing architecture.
pub struct Shard {
    /// Shard index (0..num_shards).
    pub id: usize,
    /// 16 databases per shard (SELECT 0-15), directly owned.
    pub databases: Vec<Database>,
    /// Total number of shards in the system.
    pub num_shards: usize,
    /// Runtime config (cloned per-shard, not shared).
    pub runtime_config: RuntimeConfig,
    /// Per-shard Pub/Sub registry -- no global Mutex, fully owned by shard thread.
    pub pubsub_registry: PubSubRegistry,
}

impl Shard {
    /// Create a new shard with `num_databases` empty databases.
    pub fn new(id: usize, num_shards: usize, num_databases: usize, config: RuntimeConfig) -> Self {
        let databases = (0..num_databases).map(|_| Database::new()).collect();
        Shard {
            id,
            databases,
            num_shards,
            runtime_config: config,
            pubsub_registry: PubSubRegistry::new(),
        }
    }

    /// Restore shard state from per-shard snapshot and WAL files at startup.
    ///
    /// Loads the per-shard RRDSHARD snapshot file first (if it exists), then replays
    /// the per-shard WAL for any commands written after the last snapshot.
    /// Returns total keys loaded (snapshot + WAL replay).
    pub fn restore_from_persistence(&mut self, persistence_dir: &str) -> usize {
        use crate::persistence::snapshot::shard_snapshot_load;
        use crate::persistence::wal;

        let dir = std::path::Path::new(persistence_dir);
        let mut total_keys = 0;

        // Load per-shard snapshot
        let snap_path = dir.join(format!("shard-{}.rrdshard", self.id));
        if snap_path.exists() {
            match shard_snapshot_load(&mut self.databases, &snap_path) {
                Ok(n) => {
                    info!("Shard {}: loaded {} keys from snapshot", self.id, n);
                    total_keys += n;
                }
                Err(e) => {
                    tracing::error!("Shard {}: snapshot load failed: {}", self.id, e);
                }
            }
        }

        // Replay per-shard WAL
        let wal_file = wal::wal_path(dir, self.id);
        if wal_file.exists() {
            match wal::replay_wal(&mut self.databases, &wal_file, &DispatchReplayEngine) {
                Ok(n) => {
                    info!("Shard {}: replayed {} WAL commands", self.id, n);
                    total_keys += n;
                }
                Err(e) => {
                    tracing::error!("Shard {}: WAL replay failed: {}", self.id, e);
                }
            }
        }

        total_keys
    }

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
        let mut inflight_sends: std::collections::HashMap<u32, Vec<uring_handler::InFlightSend>> =
            std::collections::HashMap::new();

        #[cfg(not(all(target_os = "linux", feature = "runtime-tokio")))]
        {
            let _ = &bind_addr; // Suppress unused warning when io_uring path inactive
            info!("Shard {} started", self.id);
        }

        // Wrap databases and pubsub_registry in Rc<RefCell> for sharing with spawned
        // connection tasks. Safe: single-threaded runtime guarantees no concurrent access.
        let databases = Rc::new(RefCell::new(std::mem::take(&mut self.databases)));
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
                                &databases, &dispatch_tx, &pubsub_rc, &blocking_rc,
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
                        &databases, &mut consumers, &mut *pubsub_rc.borrow_mut(),
                        &blocking_rc, &mut pending_snapshot, &mut snapshot_state,
                        &mut wal_writer, &mut repl_backlog, &mut replica_txs,
                        &repl_state, shard_id, &script_cache_rc, &cached_clock,
                    );
                    persistence_tick::handle_pending_snapshot(
                        pending_snapshot, &mut snapshot_state, &mut snapshot_reply_tx,
                        &databases, shard_id,
                    );
                }
                // Periodic 1ms timer for WAL flush, snapshot advance, io_uring poll
                _ = periodic_interval.tick() => {
                    cached_clock.update();

                    let mut pending_snapshot = None;
                    spsc_handler::drain_spsc_shared(
                        &databases, &mut consumers, &mut *pubsub_rc.borrow_mut(),
                        &blocking_rc, &mut pending_snapshot, &mut snapshot_state,
                        &mut wal_writer, &mut repl_backlog, &mut replica_txs,
                        &repl_state, shard_id, &script_cache_rc, &cached_clock,
                    );
                    persistence_tick::handle_pending_snapshot(
                        pending_snapshot, &mut snapshot_state, &mut snapshot_reply_tx,
                        &databases, shard_id,
                    );

                    persistence_tick::check_auto_save_trigger(
                        &snapshot_trigger_rx, &mut last_snapshot_epoch,
                        &mut snapshot_state, &databases, &persistence_dir, shard_id,
                    );

                    // Advance snapshot one segment per tick (cooperative)
                    if persistence_tick::advance_snapshot_segment(&mut snapshot_state, &databases) {
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
                                event, driver, &databases, &mut uring_parse_bufs,
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
                    timers::run_active_expiry(&databases);
                }
                // Background eviction timer
                _ = eviction_interval.tick() => {
                    timers::run_eviction(&databases, &runtime_config);
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
                                &databases, &dispatch_tx, &pubsub_rc, &blocking_rc,
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
                        &databases, &mut consumers, &mut *pubsub_rc.borrow_mut(),
                        &blocking_rc, &mut pending_snapshot, &mut snapshot_state,
                        &mut wal_writer, &mut repl_backlog, &mut replica_txs,
                        &repl_state, shard_id, &script_cache_rc, &cached_clock,
                    );
                    persistence_tick::handle_pending_snapshot(
                        pending_snapshot, &mut snapshot_state, &mut snapshot_reply_tx,
                        &databases, shard_id,
                    );
                }
                // Periodic 1ms timer for WAL flush, snapshot advance, SPSC safety net
                _ = periodic_interval.tick() => {
                    cached_clock.update();

                    let mut pending_snapshot = None;
                    spsc_handler::drain_spsc_shared(
                        &databases, &mut consumers, &mut *pubsub_rc.borrow_mut(),
                        &blocking_rc, &mut pending_snapshot, &mut snapshot_state,
                        &mut wal_writer, &mut repl_backlog, &mut replica_txs,
                        &repl_state, shard_id, &script_cache_rc, &cached_clock,
                    );
                    persistence_tick::handle_pending_snapshot(
                        pending_snapshot, &mut snapshot_state, &mut snapshot_reply_tx,
                        &databases, shard_id,
                    );

                    persistence_tick::check_auto_save_trigger(
                        &snapshot_trigger_rx, &mut last_snapshot_epoch,
                        &mut snapshot_state, &databases, &persistence_dir, shard_id,
                    );

                    // Advance snapshot one segment per tick (cooperative)
                    if persistence_tick::advance_snapshot_segment(&mut snapshot_state, &databases) {
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
                    timers::run_active_expiry(&databases);
                }
                // Background eviction timer
                _ = eviction_interval.tick() => {
                    timers::run_eviction(&databases, &runtime_config);
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

        // Restore databases and pubsub_registry back to self for cleanup.
        self.databases = match Rc::try_unwrap(databases) {
            Ok(refcell) => refcell.into_inner(),
            Err(_) => {
                info!(
                    "Shard {}: could not reclaim databases (outstanding Rc references)",
                    self.id
                );
                Vec::new()
            }
        };
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framevec;
    use crate::protocol::Frame;
    use crate::pubsub::subscriber::Subscriber;
    use crate::runtime::channel as rt_channel;
    use bytes::Bytes;
    use ringbuf::HeapRb;
    use ringbuf::traits::{Producer, Split};

    #[test]
    fn test_shard_new() {
        let config = RuntimeConfig::default();
        let shard = Shard::new(0, 4, 16, config);
        assert_eq!(shard.id, 0);
        assert_eq!(shard.num_shards, 4);
        assert_eq!(shard.databases.len(), 16);
    }

    #[test]
    fn test_shard_has_pubsub_registry() {
        let config = RuntimeConfig::default();
        let shard = Shard::new(0, 4, 16, config);
        assert_eq!(shard.pubsub_registry.channel_subscription_count(1), 0);
        assert_eq!(shard.pubsub_registry.pattern_subscription_count(1), 0);
    }

    #[test]
    fn test_shard_databases_independent() {
        let config = RuntimeConfig::default();
        let shard = Shard::new(1, 8, 4, config);
        assert_eq!(shard.databases.len(), 4);
        assert_eq!(shard.id, 1);
        assert_eq!(shard.num_shards, 8);
    }

    #[test]
    fn test_pubsub_fanout_via_spsc() {
        let mut pubsub = PubSubRegistry::new();
        let databases = Rc::new(RefCell::new(vec![Database::new()]));

        let (tx, rx) = rt_channel::mpsc_bounded::<Frame>(16);
        let sub = Subscriber::new(tx, 42);
        pubsub.subscribe(Bytes::from_static(b"news"), sub);

        let rb = HeapRb::new(64);
        let (mut prod, cons) = rb.split();
        prod.try_push(ShardMessage::PubSubFanOut {
            channel: Bytes::from_static(b"news"),
            message: Bytes::from_static(b"hello from shard 1"),
        })
        .ok()
        .expect("push should succeed");

        let mut pending_snap = None;
        let mut snap_state = None;
        let mut wal_w: Option<WalWriter> = None;
        let blocking = Rc::new(RefCell::new(BlockingRegistry::new(0)));
        let script_cache = Rc::new(RefCell::new(crate::scripting::ScriptCache::new()));
        let clock = CachedClock::new();
        spsc_handler::drain_spsc_shared(
            &databases,
            &mut [cons],
            &mut pubsub,
            &blocking,
            &mut pending_snap,
            &mut snap_state,
            &mut wal_w,
            &mut None,
            &mut Vec::new(),
            &None,
            0,
            &script_cache,
            &clock,
        );

        let msg = rx.try_recv().expect("subscriber should receive message");
        match msg {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Bytes::from_static(b"message")));
                assert_eq!(parts[1], Frame::BulkString(Bytes::from_static(b"news")));
                assert_eq!(
                    parts[2],
                    Frame::BulkString(Bytes::from_static(b"hello from shard 1"))
                );
            }
            _ => panic!("expected Array frame"),
        }
    }

    #[test]
    fn test_drain_spsc_respects_limit() {
        let mut pubsub = PubSubRegistry::new();
        let databases = Rc::new(RefCell::new(vec![Database::new()]));

        let rb = HeapRb::new(512);
        let (mut prod, cons) = rb.split();

        for _ in 0..300 {
            prod.try_push(ShardMessage::PubSubFanOut {
                channel: Bytes::from_static(b"ch"),
                message: Bytes::from_static(b"msg"),
            })
            .ok()
            .unwrap();
        }

        let mut pending_snap = None;
        let mut snap_state = None;
        let mut wal_w: Option<WalWriter> = None;
        let blocking = Rc::new(RefCell::new(BlockingRegistry::new(0)));
        let script_cache = Rc::new(RefCell::new(crate::scripting::ScriptCache::new()));
        let clock = CachedClock::new();
        spsc_handler::drain_spsc_shared(
            &databases,
            &mut [cons],
            &mut pubsub,
            &blocking,
            &mut pending_snap,
            &mut snap_state,
            &mut wal_w,
            &mut None,
            &mut Vec::new(),
            &None,
            0,
            &script_cache,
            &clock,
        );
    }

    #[test]
    fn test_extract_command_static_ping() {
        let frame = Frame::Array(framevec![Frame::BulkString(Bytes::from_static(b"PING")),]);
        let (cmd, args) = spsc_handler::extract_command_static(&frame).unwrap();
        assert_eq!(cmd, b"PING");
        assert!(args.is_empty());
    }

    #[test]
    fn test_extract_command_static_with_args() {
        let frame = Frame::Array(framevec![
            Frame::BulkString(Bytes::from_static(b"SET")),
            Frame::BulkString(Bytes::from_static(b"key")),
            Frame::BulkString(Bytes::from_static(b"value")),
        ]);
        let (cmd, args) = spsc_handler::extract_command_static(&frame).unwrap();
        assert_eq!(cmd, b"SET");
        assert_eq!(args.len(), 2);
    }

    #[test]
    fn test_extract_command_static_invalid() {
        // Non-array frame
        let frame = Frame::SimpleString(Bytes::from_static(b"PING"));
        assert!(spsc_handler::extract_command_static(&frame).is_none());

        // Empty array
        let frame = Frame::Array(framevec![]);
        assert!(spsc_handler::extract_command_static(&frame).is_none());

        // Array with non-string first element
        let frame = Frame::Array(framevec![Frame::Integer(42)]);
        assert!(spsc_handler::extract_command_static(&frame).is_none());
    }

    /// Linux-only: verify handle_uring_event processes Disconnect correctly.
    #[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
    #[test]
    fn test_handle_uring_event_disconnect() {
        use crate::io::{IoEvent, UringConfig, UringDriver};

        let config = RuntimeConfig::default();
        let mut shard = Shard::new(0, 1, 1, config);
        let databases = Rc::new(RefCell::new(std::mem::take(&mut shard.databases)));
        let mut parse_bufs = std::collections::HashMap::new();
        parse_bufs.insert(42u32, bytes::BytesMut::from(&b"partial"[..]));
        let mut inflight_sends = std::collections::HashMap::new();

        let mut driver = UringDriver::new(UringConfig::default()).unwrap();
        driver.init().unwrap();

        let clock = CachedClock::new();

        uring_handler::handle_uring_event(
            IoEvent::Disconnect { conn_id: 42 },
            &mut driver,
            &databases,
            &mut parse_bufs,
            &mut inflight_sends,
            None,
            &clock,
        );

        assert!(
            !parse_bufs.contains_key(&42),
            "parse buffer should be removed on disconnect"
        );
    }

    /// Linux-only: verify handle_uring_event processes SendComplete as no-op.
    #[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
    #[test]
    fn test_handle_uring_event_send_complete() {
        use crate::io::{IoEvent, UringConfig, UringDriver};

        let config = RuntimeConfig::default();
        let mut shard = Shard::new(0, 1, 1, config);
        let databases = Rc::new(RefCell::new(std::mem::take(&mut shard.databases)));
        let mut parse_bufs = std::collections::HashMap::new();
        let mut inflight_sends = std::collections::HashMap::new();

        let mut driver = UringDriver::new(UringConfig::default()).unwrap();
        driver.init().unwrap();

        let clock = CachedClock::new();

        uring_handler::handle_uring_event(
            IoEvent::SendComplete { conn_id: 1 },
            &mut driver,
            &databases,
            &mut parse_bufs,
            &mut inflight_sends,
            None,
            &clock,
        );
    }
}
