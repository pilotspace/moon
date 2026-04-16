//! Shard event loop: the `run()` method with the tokio/monoio select! loop.
//!
//! Extracted from shard/mod.rs. The select! arms call into sub-handler modules
//! (spsc_handler, persistence_tick, conn_accept, timers, uring_handler).

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use ringbuf::HeapCons;
use ringbuf::HeapProd;
use tracing::info;

use crate::blocking::BlockingRegistry;
use crate::config::RuntimeConfig;
use crate::persistence::control::ShardControlFile;
use crate::persistence::page_cache::PageCache;
use crate::persistence::snapshot::SnapshotState;
use crate::persistence::wal::WalWriter;
use crate::persistence::wal_v3::segment::WalWriterV3;
use crate::pubsub::PubSubRegistry;
use crate::replication::backlog::ReplicationBacklog;
use crate::replication::state::ReplicationState;
use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use crate::runtime::{TimerImpl, traits::RuntimeTimer};
use crate::storage::entry::CachedClock;
use crate::tracking::TrackingTable;

#[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
use crate::io::{UringConfig, UringDriver};

use super::affinity::AffinityTracker;
use super::dispatch::ShardMessage;
use super::remote_subscriber_map::RemoteSubscriberMap;
use super::shared_databases::ShardDatabases;
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
    /// Drains SPSC consumers for cross-shard dispatch requests and PubSubPublish.
    /// Runs cooperative active expiry. Shuts down gracefully on cancellation.
    pub async fn run(
        &mut self,
        conn_rx: channel::MpscReceiver<(crate::runtime::TcpStream, bool)>,
        tls_config: Option<crate::tls::SharedTlsConfig>,
        mut consumers: Vec<HeapCons<ShardMessage>>,
        producers: Vec<HeapProd<ShardMessage>>,
        shutdown: CancellationToken,
        aof_tx: Option<channel::MpscSender<crate::persistence::aof::AofMessage>>,
        bind_addr: Option<String>,
        persistence_dir: Option<String>,
        snapshot_trigger_rx: channel::WatchReceiver<u64>,
        snapshot_trigger_tx: channel::WatchSender<u64>,
        repl_state_ext: Option<Arc<std::sync::RwLock<ReplicationState>>>,
        cluster_state: Option<std::sync::Arc<std::sync::RwLock<crate::cluster::ClusterState>>>,
        config_port: u16,
        acl_table: Arc<std::sync::RwLock<crate::acl::AclTable>>,
        runtime_config: Arc<parking_lot::RwLock<RuntimeConfig>>,
        server_config: Arc<crate::config::ServerConfig>,
        spsc_notify: Arc<channel::Notify>,
        all_notifiers: Vec<Arc<channel::Notify>>,
        shard_databases: Arc<ShardDatabases>,
        all_pubsub_registries: Vec<Arc<parking_lot::RwLock<PubSubRegistry>>>,
        all_remote_sub_maps: Vec<Arc<parking_lot::RwLock<RemoteSubscriberMap>>>,
        affinity_tracker: Arc<parking_lot::RwLock<AffinityTracker>>,
    ) {
        let _shard_id = self.id;

        // Publish disk-offload status for INFO moonstore (set once per shard, idempotent).
        crate::vector::metrics::MOONSTORE_DISK_OFFLOAD_ENABLED.store(
            server_config.disk_offload_enabled(),
            std::sync::atomic::Ordering::Relaxed,
        );

        // On Linux with tokio runtime, attempt to initialize io_uring for high-performance I/O.
        #[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
        let mut uring_state: Option<UringDriver> = {
            if std::env::var("MOON_NO_URING").is_ok() {
                info!("Shard {} io_uring disabled via MOON_NO_URING", self.id);
                None
            } else {
                match UringDriver::new(UringConfig {
                    sqpoll_idle_ms: server_config.uring_sqpoll_ms,
                    ..UringConfig::default()
                }) {
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
                            // Flush the accept SQE to the kernel immediately.
                            let _ = d.submit_and_wait_nonblocking();
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

        // Wrap io_uring's CQE eventfd in tokio AsyncFd for select! integration.
        // When io_uring has completions, the kernel signals this eventfd, which
        // wakes tokio's epoll and fires the select! branch — instant CQE processing
        // with zero polling overhead.
        //
        // We dup() the eventfd so AsyncFd can take ownership without conflicting
        // with io_uring's registered eventfd (which must stay open).
        #[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
        let uring_cqe_fd: Option<tokio::io::unix::AsyncFd<std::os::fd::OwnedFd>> = {
            if let Some(ref d) = uring_state {
                use std::os::fd::{FromRawFd, OwnedFd};
                // SAFETY: dup() creates a new fd referencing the same eventfd.
                // OwnedFd takes ownership and will close the dup'd fd on drop.
                let dup_fd = unsafe { libc::dup(d.cqe_eventfd()) };
                if dup_fd >= 0 {
                    // SAFETY: dup_fd is a valid, fresh fd from dup() above (>= 0 check).
                    // OwnedFd takes sole ownership and will close it on drop.
                    let owned = unsafe { OwnedFd::from_raw_fd(dup_fd) };
                    match tokio::io::unix::AsyncFd::with_interest(
                        owned,
                        tokio::io::Interest::READABLE,
                    ) {
                        Ok(afd) => {
                            tracing::info!(
                                "Shard {}: io_uring eventfd registered with tokio (fd={})",
                                self.id,
                                dup_fd
                            );
                            Some(afd)
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Shard {}: AsyncFd for io_uring eventfd failed: {}",
                                self.id,
                                e
                            );
                            None
                        }
                    }
                } else {
                    tracing::warn!(
                        "Shard {}: dup(eventfd) failed: {}",
                        self.id,
                        std::io::Error::last_os_error()
                    );
                    None
                }
            } else {
                None
            }
        };

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

        // Per-shard SO_REUSEPORT listener (unix + tokio, non-uring path).
        // On Linux: only created when io_uring is NOT active (multishot accept handles it).
        // On macOS: always created (no io_uring).
        #[cfg(all(unix, feature = "runtime-tokio"))]
        let per_shard_listener: Option<tokio::net::TcpListener> = {
            #[cfg(target_os = "linux")]
            let uring_active = uring_state.is_some();
            #[cfg(not(target_os = "linux"))]
            let uring_active = false;

            if !uring_active {
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

        // Per-shard SO_REUSEPORT listener (unix + monoio).
        // Each shard creates its own listener; the kernel distributes connections via SO_REUSEPORT.
        #[cfg(all(unix, feature = "runtime-monoio"))]
        let mut per_shard_monoio_listener: Option<monoio::net::TcpListener> = {
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

        // Dedicated monoio::spawn accept task: avoids io_uring cancel/resubmit race
        // that occurs when accept() is a branch in monoio::select!.
        #[cfg(all(unix, feature = "runtime-monoio"))]
        let local_accept_rx: Option<flume::Receiver<std::net::TcpStream>> = {
            if let Some(listener) = per_shard_monoio_listener.take() {
                let (tx, rx) = flume::bounded(256);
                let shard_id_copy = self.id;
                monoio::spawn(async move {
                    loop {
                        match listener.accept().await {
                            Ok((stream, _addr)) => {
                                let std_stream = {
                                    use std::os::unix::io::{FromRawFd, IntoRawFd};
                                    let fd = stream.into_raw_fd();
                                    // SAFETY: fd is a valid socket from monoio TcpStream::into_raw_fd(),
                                    // which relinquished ownership. We take sole ownership here.
                                    unsafe { std::net::TcpStream::from_raw_fd(fd) }
                                };
                                if tx.send(std_stream).is_err() {
                                    break; // receiver dropped, shard shutting down
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Shard {}: per-shard accept error: {}",
                                    shard_id_copy,
                                    e
                                );
                            }
                        }
                    }
                });
                Some(rx)
            } else {
                None
            }
        };

        #[cfg(all(feature = "runtime-monoio", not(unix)))]
        let local_accept_rx: Option<flume::Receiver<std::net::TcpStream>> = None;

        #[cfg(not(any(
            all(unix, feature = "runtime-tokio"),
            all(unix, feature = "runtime-monoio"),
        )))]
        {
            let _ = &bind_addr; // Suppress unused warning when per-shard accept inactive
            info!("Shard {} started", self.id);
        }

        #[cfg(all(unix, feature = "runtime-monoio"))]
        if per_shard_monoio_listener.is_none() {
            info!("Shard {} started (monoio, conn_rx fallback)", self.id);
        }

        let dispatch_tx = Rc::new(RefCell::new(producers));
        // Use pre-shared Arc<RwLock<PubSubRegistry>> seeded from snapshot.
        let pubsub_arc = all_pubsub_registries[self.id].clone();
        {
            let mut reg = pubsub_arc.write();
            *reg = std::mem::take(&mut self.pubsub_registry);
        }
        let tracking_rc = Rc::new(RefCell::new(TrackingTable::new()));
        let shard_id = self.id;
        let blocking_rc = Rc::new(RefCell::new(BlockingRegistry::new(shard_id)));
        let remote_sub_map_arc = all_remote_sub_maps[self.id].clone();
        let num_shards = self.num_shards;

        // Lazy per-shard Lua VM: deferred until first EVAL/EVALSHA.
        let lua_rc: Rc<RefCell<Option<Rc<mlua::Lua>>>> = Rc::new(RefCell::new(None));
        let script_cache_rc = Rc::new(RefCell::new(crate::scripting::ScriptCache::new()));

        // Per-shard snapshot state (None when no snapshot is active)
        let mut snapshot_state: Option<SnapshotState> = None;
        let mut snapshot_reply_tx: Option<channel::OneshotSender<Result<(), String>>> = None;

        // Per-shard WAL writer (created only when persistence is actually enabled).
        let appendonly_enabled = runtime_config.read().appendonly != "no";
        let mut wal_writer: Option<WalWriter> = match (&persistence_dir, appendonly_enabled) {
            (Some(dir), true) => match WalWriter::new(shard_id, std::path::Path::new(dir)) {
                Ok(w) => {
                    info!("Shard {}: WAL writer initialized", shard_id);
                    Some(w)
                }
                Err(e) => {
                    tracing::warn!("Shard {}: WAL init failed: {}", shard_id, e);
                    None
                }
            },
            (Some(_), false) => {
                info!("Shard {}: WAL skipped (appendonly=no)", shard_id);
                None
            }
            (None, _) => None,
        };

        // Disk-offload base directory (None when disk-offload is disabled).
        let disk_offload_base: Option<std::path::PathBuf> = server_config
            .disk_offload_enabled()
            .then(|| server_config.effective_disk_offload_dir());

        // Per-shard WAL v3 writer (created only when disk-offload is enabled).
        // Provides per-record LSN tracking and FPI support for checkpoint-based recovery.
        // WAL v2 remains active for non-disk-offload mode; both writers can coexist.
        let mut wal_v3_writer: Option<WalWriterV3> = if server_config.disk_offload_enabled() {
            let shard_dir = server_config
                .effective_disk_offload_dir()
                .join(format!("shard-{}", shard_id));
            let wal_dir = shard_dir.join("wal-v3");
            match WalWriterV3::new(shard_id, &wal_dir, server_config.wal_segment_size_bytes()) {
                Ok(w) => {
                    info!(
                        "Shard {}: WAL v3 writer initialized (segment_size={})",
                        shard_id,
                        server_config.wal_segment_size_bytes()
                    );
                    Some(w)
                }
                Err(e) => {
                    tracing::warn!("Shard {}: WAL v3 init failed: {}", shard_id, e);
                    None
                }
            }
        } else {
            None
        };

        // Per-shard WAL append channel for local writes.
        // Connection handlers send serialized write commands here; we drain on the 1ms tick.
        let (wal_append_tx, wal_append_rx) = channel::mpsc_bounded::<bytes::Bytes>(4096);
        if appendonly_enabled || server_config.disk_offload_enabled() {
            shard_databases.set_wal_append_tx(shard_id, wal_append_tx);
        }

        // Per-shard PageCache (None when disk-offload is disabled).
        // Manages 4KB + 64KB page frames with clock-sweep eviction.
        let page_cache: Option<PageCache> = if server_config.disk_offload_enabled() {
            // Default: pagecache_size_bytes returns configured size or maxmemory/4.
            // Split: 75% for 4KB frames, 25% for 64KB frames.
            let budget = server_config.pagecache_size_bytes(server_config.maxmemory as u64);
            let num_4k = ((budget * 3 / 4) / 4096) as usize;
            let num_64k = ((budget / 4) / 65536) as usize;
            let num_4k = num_4k.max(64); // minimum 64 frames
            let num_64k = num_64k.max(8); // minimum 8 frames
            info!(
                "Shard {}: PageCache initialized ({} x 4KB + {} x 64KB frames, budget={})",
                shard_id, num_4k, num_64k, budget
            );
            Some(PageCache::new(num_4k, num_64k))
        } else {
            None
        };

        // Per-shard control file (disk-offload path).
        let mut control_file: Option<ShardControlFile> = if server_config.disk_offload_enabled() {
            let shard_dir = server_config
                .effective_disk_offload_dir()
                .join(format!("shard-{}", shard_id));
            let ctrl_path = ShardControlFile::control_path(&shard_dir, shard_id);
            if ctrl_path.exists() {
                match ShardControlFile::read(&ctrl_path) {
                    Ok(cf) => Some(cf),
                    Err(e) => {
                        tracing::warn!(
                            "Shard {}: control file read failed: {}, creating new",
                            shard_id,
                            e
                        );
                        Some(ShardControlFile::new([0u8; 16]))
                    }
                }
            } else {
                Some(ShardControlFile::new([0u8; 16]))
            }
        } else {
            None
        };
        let control_file_path: Option<std::path::PathBuf> = if server_config.disk_offload_enabled()
        {
            let shard_dir = server_config
                .effective_disk_offload_dir()
                .join(format!("shard-{}", shard_id));
            Some(ShardControlFile::control_path(&shard_dir, shard_id))
        } else {
            None
        };

        // Track WAL bytes since last checkpoint for trigger logic.
        let mut wal_bytes_since_checkpoint: u64 = 0;

        // Flag: BGSAVE snapshot completed, request a forced checkpoint on next tick.
        let mut bgsave_checkpoint_requested = false;

        // Per-shard checkpoint manager (None when disk-offload is disabled).
        // When enabled, drives the fuzzy checkpoint protocol: begin(redo_lsn) ->
        // advance_tick(flush pages) -> finalize(WAL record + manifest + control).
        // Wired to PageCache, WalWriterV3, ShardManifest, and ShardControlFile below.
        let mut checkpoint_manager: Option<crate::persistence::checkpoint::CheckpointManager> =
            if server_config.disk_offload_enabled() {
                let trigger = crate::persistence::checkpoint::CheckpointTrigger::new(
                    server_config.checkpoint_timeout,
                    server_config.max_wal_size_bytes(),
                    server_config.checkpoint_completion,
                );
                info!(
                    "Shard {}: checkpoint manager initialized (timeout={}s, max_wal={})",
                    shard_id,
                    server_config.checkpoint_timeout,
                    server_config.max_wal_size_bytes()
                );
                Some(crate::persistence::checkpoint::CheckpointManager::new(
                    trigger,
                ))
            } else {
                None
            };

        // Per-shard manifest for tracking segment files and checkpoint state.
        // Used by both checkpoint protocol (handle_checkpoint_tick) and warm
        // tier transitions (check_warm_transitions).
        let mut shard_manifest: Option<crate::persistence::manifest::ShardManifest> =
            if server_config.disk_offload_enabled() {
                let shard_dir = server_config
                    .effective_disk_offload_dir()
                    .join(format!("shard-{}", shard_id));
                std::fs::create_dir_all(&shard_dir).ok();
                let manifest_path = shard_dir.join(format!("shard-{}.manifest", shard_id));
                if manifest_path.exists() {
                    match crate::persistence::manifest::ShardManifest::open(&manifest_path) {
                        Ok(m) => Some(m),
                        Err(e) => {
                            tracing::warn!("Shard {}: shard manifest open failed: {}", shard_id, e);
                            None
                        }
                    }
                } else {
                    match crate::persistence::manifest::ShardManifest::create(&manifest_path) {
                        Ok(m) => Some(m),
                        Err(e) => {
                            tracing::warn!(
                                "Shard {}: shard manifest create failed: {}",
                                shard_id,
                                e
                            );
                            None
                        }
                    }
                }
            } else {
                None
            };
        // Per-shard background spill thread for async eviction pwrite.
        // When disk-offload is enabled, evicted KV entries are written to disk
        // on a background std::thread instead of blocking the event loop.
        let mut spill_thread: Option<crate::storage::tiered::spill_thread::SpillThread> =
            if server_config.disk_offload_enabled() {
                let st = crate::storage::tiered::spill_thread::SpillThread::new(shard_id);
                info!("Shard {}: spill background thread initialized", shard_id);
                Some(st)
            } else {
                None
            };

        // Shared spill file ID counter for connection handlers + event loop.
        // Rc<Cell<u64>> is safe: monoio is single-threaded per shard.
        let spill_sender: Option<
            flume::Sender<crate::storage::tiered::spill_thread::SpillRequest>,
        > = spill_thread.as_ref().map(|st| st.sender());
        let spill_file_id: std::rc::Rc<std::cell::Cell<u64>> =
            std::rc::Rc::new(std::cell::Cell::new(1));
        let mut next_file_id: u64 = 1;
        let disk_offload_dir: Option<std::path::PathBuf> = disk_offload_base.clone();
        // Tokio path doesn't take these into the spawn signatures; suppress warnings.
        let (_, _, _) = (&spill_sender, &spill_file_id, &disk_offload_dir);

        // Per-shard replication backlog (lazy: allocated on first RegisterReplica).
        let mut repl_backlog: Option<ReplicationBacklog> = None;
        let mut replica_txs: Vec<(u64, channel::MpscSender<bytes::Bytes>)> = Vec::new();
        let repl_state: Option<Arc<std::sync::RwLock<ReplicationState>>> = repl_state_ext;

        // Track last seen snapshot epoch to detect watch channel triggers
        let mut last_snapshot_epoch = snapshot_trigger_rx.borrow();

        // Sub-timer intervals: tokio uses separate select! branches for each.
        // monoio uses counter-based dispatch from a single periodic tick to avoid
        // monoio::select! memory leak (~100 bytes/re-entry at 1000Hz = ~100 KB/s/shard).
        #[cfg(feature = "runtime-tokio")]
        let mut expiry_interval = TimerImpl::interval(Duration::from_millis(100));
        #[cfg(feature = "runtime-tokio")]
        let mut eviction_interval = TimerImpl::interval(Duration::from_millis(100));
        let mut periodic_interval = TimerImpl::interval(Duration::from_millis(1));
        #[cfg(feature = "runtime-tokio")]
        let mut block_timeout_interval = TimerImpl::interval(Duration::from_millis(10));
        #[cfg(feature = "runtime-tokio")]
        let mut wal_sync_interval = TimerImpl::interval(Duration::from_secs(1));
        // Warm check interval adapts to segment_warm_after for fast testing:
        // default 10s, but if warm_after < 10s, poll at warm_after frequency.
        let warm_poll_ms =
            (server_config.segment_warm_after * 1000).clamp(1000, timers::WARM_CHECK_INTERVAL_MS);
        #[cfg(feature = "runtime-tokio")]
        let mut warm_check_interval = TimerImpl::interval(Duration::from_millis(warm_poll_ms));
        // Cold tier transition check: poll at min(60s, segment_cold_after) so the
        // timer fires within one cold-age window (default 60s; short for testing).
        let cold_poll_secs = if server_config.segment_cold_after > 0 {
            server_config.segment_cold_after.min(60)
        } else {
            60
        };
        #[cfg(feature = "runtime-tokio")]
        let mut cold_check_interval = TimerImpl::interval(Duration::from_secs(cold_poll_secs));

        // monoio: counter-based sub-timer dispatch from 1ms periodic tick.
        // Each sub-timer fires at its native interval via modular arithmetic.
        #[cfg(feature = "runtime-monoio")]
        let mut monoio_tick_counter: u64 = 0;
        // Used by tokio select! for event-driven SPSC drain; monoio drains in periodic tick.
        let spsc_notify_local = spsc_notify;
        #[cfg(feature = "runtime-monoio")]
        let _ = &spsc_notify_local;

        // Per-shard cached clock: updated once per 1ms tick.
        let cached_clock = CachedClock::new();

        // Pending FD migrations collected from SPSC drain (spawn wired in Plan 50-02).
        let mut pending_migrations: Vec<(
            std::os::unix::io::RawFd,
            crate::server::conn::affinity::MigratedConnectionState,
        )> = Vec::new();

        // Per-shard VectorStore: use the SHARED instance from ShardDatabases.
        // This ensures handler_sharded FT.* commands and SPSC auto-indexing
        // (triggered by HSET) operate on the SAME VectorStore.
        //
        // The shard-owned vector_store (from Shard struct) is discarded.
        // All vector operations go through shard_databases.vector_store(shard_id).
        let _discarded_vector_store = std::mem::replace(
            &mut self.vector_store,
            crate::vector::store::VectorStore::new(),
        );

        // Restore vector index metadata from sidecar file.
        // Set persist_dir so FT.CREATE/FT.DROPINDEX saves metadata for future recovery.
        // Try disk-offload dir first (higher priority), then main persistence dir.
        {
            let vector_persist_dir = if server_config.disk_offload_enabled() {
                Some(
                    server_config
                        .effective_disk_offload_dir()
                        .join(format!("shard-{}", shard_id)),
                )
            } else {
                persistence_dir.as_ref().map(|d| {
                    std::path::PathBuf::from(d).join(format!("shard-{}-vectors", shard_id))
                })
            };

            if let Some(ref vdir) = vector_persist_dir {
                let _ = std::fs::create_dir_all(vdir);
                let mut vs = shard_databases.vector_store(shard_id);
                vs.set_persist_dir(vdir.clone());
                drop(vs);

                // Text indexes share the same shard directory (different filename).
                let mut ts = shard_databases.text_store(shard_id);
                ts.set_persist_dir(vdir.clone());
                drop(ts);
            }

            // Try loading saved index metadata from the vector persist dir.
            let metas = vector_persist_dir.as_ref().and_then(|vdir| {
                match crate::vector::index_persist::load_index_metadata(vdir) {
                    Ok(m) if !m.is_empty() => Some(m),
                    _ => None,
                }
            });

            // Try loading saved text index metadata from the same persist dir.
            let text_metas = vector_persist_dir.as_ref().and_then(|vdir| {
                match crate::text::index_persist::load_text_index_metadata(vdir) {
                    Ok(m) if !m.is_empty() => Some(m),
                    _ => None,
                }
            });

            if let Some(ref metas) = metas {
                let mut vs = shard_databases.vector_store(shard_id);
                info!(
                    "Shard {}: restoring {} vector index(es) from sidecar",
                    shard_id,
                    metas.len()
                );
                for meta in metas {
                    if let Err(e) = vs.create_index(meta.clone()) {
                        tracing::warn!(
                            "Shard {}: failed to restore index '{}': {}",
                            shard_id,
                            String::from_utf8_lossy(&meta.name),
                            e
                        );
                    }
                }
                drop(vs); // release VectorStore lock before scanning databases
            }

            // Restore text indexes from sidecar metadata.
            #[cfg(feature = "text-index")]
            if let Some(ref text_metas) = text_metas {
                let mut ts = shard_databases.text_store(shard_id);
                info!(
                    "Shard {}: restoring {} text index(es) from sidecar",
                    shard_id,
                    text_metas.len()
                );
                for meta in text_metas {
                    let text_index = crate::text::store::TextIndex::new(
                        meta.name.clone(),
                        meta.key_prefixes.clone(),
                        meta.text_fields.clone(),
                        meta.bm25_config,
                    );
                    if let Err(e) = ts.create_index(meta.name.clone(), text_index) {
                        tracing::warn!(
                            "Shard {}: failed to restore text index '{}': {}",
                            shard_id,
                            String::from_utf8_lossy(&meta.name),
                            e
                        );
                    }
                }
                drop(ts);
            }

            // Auto-reindex existing HASH keys that match vector or text index prefixes.
            let has_indexes = metas.is_some() || text_metas.is_some();
            if has_indexes {
                let db_count = shard_databases.db_count();
                let mut reindexed = 0usize;
                for db_idx in 0..db_count {
                    let guard = shard_databases.read_db(shard_id, db_idx);
                    let mut matching: Vec<(Vec<u8>, Vec<crate::protocol::Frame>)> = Vec::new();
                    for (key, entry) in guard.data().iter() {
                        let key_bytes = key.as_bytes();
                        let matches_vector = metas.as_ref().is_some_and(|ms| {
                            ms.iter()
                                .any(|m| m.key_prefixes.iter().any(|p| key_bytes.starts_with(p)))
                        });
                        let matches_text = text_metas.as_ref().is_some_and(|ms| {
                            ms.iter()
                                .any(|m| m.key_prefixes.iter().any(|p| key_bytes.starts_with(p)))
                        });
                        if !matches_vector && !matches_text {
                            continue;
                        }
                        let mut args = Vec::new();
                        args.push(crate::protocol::Frame::BulkString(
                            bytes::Bytes::copy_from_slice(key_bytes),
                        ));
                        match entry.as_redis_value() {
                            crate::storage::compact_value::RedisValueRef::Hash(map) => {
                                for (field, value) in map.iter() {
                                    args.push(crate::protocol::Frame::BulkString(
                                        bytes::Bytes::copy_from_slice(field),
                                    ));
                                    args.push(crate::protocol::Frame::BulkString(
                                        bytes::Bytes::copy_from_slice(value),
                                    ));
                                }
                            }
                            crate::storage::compact_value::RedisValueRef::HashListpack(lp) => {
                                let entries: Vec<_> = lp.iter().collect();
                                let mut j = 0;
                                while j + 1 < entries.len() {
                                    args.push(crate::protocol::Frame::BulkString(
                                        bytes::Bytes::from(entries[j].as_bytes()),
                                    ));
                                    args.push(crate::protocol::Frame::BulkString(
                                        bytes::Bytes::from(entries[j + 1].as_bytes()),
                                    ));
                                    j += 2;
                                }
                            }
                            _ => continue,
                        }
                        if args.len() > 1 {
                            matching.push((key_bytes.to_vec(), args));
                        }
                    }
                    drop(guard);

                    if !matching.is_empty() {
                        let mut vs = shard_databases.vector_store(shard_id);
                        let mut ts = shard_databases.text_store(shard_id);
                        for (key, args) in &matching {
                            crate::shard::spsc_handler::auto_index_hset_public(&mut vs, &mut *ts, key, args);
                            reindexed += 1;
                        }
                    }
                }
                if reindexed > 0 {
                    info!(
                        "Shard {}: auto-reindexed {} HASH key(s) into restored vector/text indexes",
                        shard_id, reindexed
                    );
                }
            }
        }

        // Pending wakers for monoio cross-shard write dispatch.
        // monoio's !Send single-threaded executor doesn't see cross-thread Waker::wake()
        // from flume oneshot channels. Connection tasks register their waker here; the
        // event loop drains and wakes them after every SPSC processing cycle (~1ms).
        #[cfg(feature = "runtime-monoio")]
        let pending_wakers: Rc<RefCell<Vec<std::task::Waker>>> = Rc::new(RefCell::new(Vec::new()));

        loop {
            #[cfg(feature = "runtime-tokio")]
            tokio::select! {
                // io_uring CQE notification: eventfd becomes readable when completions arrive.
                // This wakes tokio's epoll instantly — no polling, no timer latency.
                // Processes ALL pending completions in a drain loop (accept → recv → send chain).
                _ = async {
                    #[cfg(target_os = "linux")]
                    if let Some(ref afd) = uring_cqe_fd {
                        if let Ok(mut guard) = afd.readable().await {
                            guard.clear_ready();
                            return;
                        }
                    }
                    std::future::pending::<()>().await
                } => {
                    #[cfg(target_os = "linux")]
                    if let Some(ref mut driver) = uring_state {
                        driver.drain_eventfd();
                        loop {
                            let _ = driver.submit_and_wait_nonblocking();
                            let events = driver.drain_completions();
                            if events.is_empty() {
                                break;
                            }
                            for event in events {
                                uring_handler::handle_uring_event(
                                    event, driver, &shard_databases, shard_id, &mut uring_parse_bufs,
                                    &mut inflight_sends, uring_listener_fd, &cached_clock,
                                );
                            }
                        }
                    }
                }
                // Per-shard SO_REUSEPORT accept (unix, non-uring tokio path)
                result = async {
                    #[cfg(all(unix, feature = "runtime-tokio"))]
                    if let Some(ref listener) = per_shard_listener {
                        return listener.accept().await;
                    }
                    // Never resolves on non-unix or when per_shard_listener is None
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
                                &cached_clock, &remote_sub_map_arc, &all_pubsub_registries,
                                &all_remote_sub_maps, &affinity_tracker,
                                shard_id, num_shards, config_port,
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
                                                    Ok(Some(_conn_id)) => {
                                                        // Immediately submit the recv SQE so the
                                                        // client doesn't wait for the next timer tick.
                                                        let _ = driver.submit_and_wait_nonblocking();
                                                    }
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
                                &affinity_tracker,
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
                        &shard_databases, &mut consumers, &mut *pubsub_arc.write(),
                        &blocking_rc, &mut pending_snapshot, &mut snapshot_state,
                        &mut wal_writer, &mut wal_v3_writer, &mut repl_backlog, &mut replica_txs,
                        &repl_state, shard_id, &script_cache_rc, &cached_clock,
                        &mut pending_migrations, &mut *shard_databases.vector_store(shard_id),
                    );
                    persistence_tick::handle_pending_snapshot(
                        pending_snapshot, &mut snapshot_state, &mut snapshot_reply_tx,
                        &shard_databases, disk_offload_base.as_deref(), shard_id,
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
                                &cached_clock, &remote_sub_map_arc, &all_pubsub_registries,
                                &all_remote_sub_maps, &affinity_tracker,
                                shard_id, num_shards, config_port,
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
                                &cached_clock, &remote_sub_map_arc, &all_pubsub_registries,
                                &all_remote_sub_maps, &affinity_tracker,
                                shard_id, num_shards, config_port,
                                &pending_wakers,
                                &spill_sender, &spill_file_id, &disk_offload_dir,
                            );
                        }
                    }
                }
                // Periodic 1ms timer for WAL flush, snapshot advance, io_uring poll
                _ = periodic_interval.0.tick() => {
                    cached_clock.update();
                    // Sync file ID from shared Cell (handlers may have incremented it)
                    next_file_id = next_file_id.max(spill_file_id.get());

                    let mut pending_snapshot = None;
                    spsc_handler::drain_spsc_shared(
                        &shard_databases, &mut consumers, &mut *pubsub_arc.write(),
                        &blocking_rc, &mut pending_snapshot, &mut snapshot_state,
                        &mut wal_writer, &mut wal_v3_writer, &mut repl_backlog, &mut replica_txs,
                        &repl_state, shard_id, &script_cache_rc, &cached_clock,
                        &mut pending_migrations, &mut *shard_databases.vector_store(shard_id),
                    );
                    persistence_tick::handle_pending_snapshot(
                        pending_snapshot, &mut snapshot_state, &mut snapshot_reply_tx,
                        &shard_databases, disk_offload_base.as_deref(), shard_id,
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
                                &cached_clock, &remote_sub_map_arc, &all_pubsub_registries,
                                &all_remote_sub_maps, &affinity_tracker,
                                shard_id, num_shards, config_port,
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
                                &cached_clock, &remote_sub_map_arc, &all_pubsub_registries,
                                &all_remote_sub_maps, &affinity_tracker,
                                shard_id, num_shards, config_port,
                                &pending_wakers,
                                &spill_sender, &spill_file_id, &disk_offload_dir,
                            );
                        }
                    }

                    persistence_tick::check_auto_save_trigger(
                        &snapshot_trigger_rx, &mut last_snapshot_epoch,
                        &mut snapshot_state, &shard_databases, &persistence_dir,
                        disk_offload_base.as_deref(), shard_id,
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
                                bgsave_checkpoint_requested = true;
                            }
                        }
                    }

                    // Drain local-write WAL channel (connection handler inline writes)
                    while let Ok(data) = wal_append_rx.try_recv() {
                        if let Some(ref mut wal) = wal_writer {
                            wal.append(&data);
                        }
                        if let Some(ref mut wal) = wal_v3_writer {
                            wal.append(
                                crate::persistence::wal_v3::record::WalRecordType::Command,
                                &data,
                            );
                        }
                    }

                    persistence_tick::flush_wal_if_needed(&mut wal_writer);
                    persistence_tick::flush_wal_v3_if_needed(&mut wal_v3_writer);

                    // appendfsync=always: fsync WAL v3 after every SPSC drain batch
                    if server_config.appendfsync == "always" {
                        if let Some(ref mut wal) = wal_v3_writer {
                            if let Err(e) = wal.flush_sync() {
                                tracing::error!("WAL v3 appendfsync=always failed: {}", e);
                            }
                        }
                    }

                    // Checkpoint protocol tick (disk-offload only)
                    if let (Some(ckpt_mgr), Some(page_cache_inst), Some(wal_v3), Some(manifest), Some(ctrl), Some(ctrl_path)) =
                        (&mut checkpoint_manager, &page_cache, &mut wal_v3_writer, &mut shard_manifest, &mut control_file, &control_file_path)
                    {
                        // BGSAVE-triggered forced checkpoint (bypasses trigger conditions)
                        if bgsave_checkpoint_requested && !ckpt_mgr.is_active() {
                            let lsn = wal_v3.current_lsn();
                            let dirty = page_cache_inst.dirty_page_count();
                            ckpt_mgr.force_begin(lsn, dirty);
                            bgsave_checkpoint_requested = false;
                        }
                        persistence_tick::maybe_begin_checkpoint(ckpt_mgr, wal_v3, page_cache_inst, wal_bytes_since_checkpoint);
                        if persistence_tick::handle_checkpoint_tick(ckpt_mgr, page_cache_inst, wal_v3, manifest, ctrl, ctrl_path) {
                            wal_bytes_since_checkpoint = 0;
                        }
                    }

                    // Also poll io_uring in the timer tick as a fallback.
                    // The eventfd select! branch should handle most CQEs instantly,
                    // but this catches any that slip through.
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
                _ = wal_sync_interval.0.tick() => {
                    timers::sync_wal(&mut wal_writer);
                    timers::sync_wal_v3(&mut wal_v3_writer);
                }
                // Warm tier transition check (10s interval, disk-offload only)
                _ = warm_check_interval.0.tick() => {
                    if server_config.disk_offload_enabled() {
                        if let Some(ref mut manifest) = shard_manifest {
                            let shard_dir = server_config.effective_disk_offload_dir()
                                .join(format!("shard-{}", shard_id));
                            persistence_tick::check_warm_transitions(
                                &*shard_databases.vector_store(shard_id),
                                &shard_dir,
                                manifest,
                                server_config.segment_warm_after,
                                &mut next_file_id,
                                shard_id,
                                &mut wal_v3_writer,
                            );
                        }
                    }
                }
                // Cold tier transition check (60s, disk-offload only)
                _ = cold_check_interval.0.tick() => {
                    if server_config.disk_offload_enabled() && server_config.segment_cold_after > 0 {
                        if let Some(ref mut manifest) = shard_manifest {
                            let shard_dir = server_config.effective_disk_offload_dir()
                                .join(format!("shard-{}", shard_id));
                            persistence_tick::check_cold_transitions(
                                &*shard_databases.vector_store(shard_id),
                                &shard_dir,
                                manifest,
                                server_config.segment_cold_after,
                                &mut next_file_id,
                                shard_id,
                            );
                        }
                    }
                }
                // Expire timed-out blocked clients every 10ms
                _ = block_timeout_interval.0.tick() => {
                    timers::expire_blocked_clients(&blocking_rc);
                }
                // Cooperative active expiry
                _ = expiry_interval.0.tick() => {
                    timers::run_active_expiry(&shard_databases, shard_id);
                }
                // Background eviction timer + memory pressure cascade
                _ = eviction_interval.0.tick() => {
                    persistence_tick::run_eviction_tick(
                        spill_thread.as_ref(),
                        &mut shard_manifest,
                        &shard_databases,
                        shard_id,
                        &server_config,
                        &runtime_config,
                        &page_cache,
                        &mut next_file_id,
                        &mut wal_v3_writer,
                        &spill_file_id,
                    );

                    // Reap idle io_uring connections (tokio+io_uring path).
                    // Cleans up CLOSE_WAIT connections where the multishot recv
                    // ended without producing a 0-byte CQE (client FIN + MORE=0).
                    #[cfg(target_os = "linux")]
                    if let Some(ref mut driver) = uring_state {
                        let _reaped = driver.reap_idle_connections(5000);
                    }
                }
                _ = shutdown.cancelled() => {
                    info!("Shard {} shutting down", self.id);
                    persistence_tick::drain_and_shutdown_spill(
                        &mut spill_thread,
                        &mut shard_manifest,
                        &shard_databases,
                        shard_id,
                    );
                    // Trigger final checkpoint before shutdown (design S9)
                    if let (Some(ckpt_mgr), Some(page_cache_inst), Some(wal_v3), Some(manifest), Some(ctrl), Some(ctrl_path)) =
                        (&mut checkpoint_manager, &page_cache, &mut wal_v3_writer, &mut shard_manifest, &mut control_file, &control_file_path)
                    {
                        persistence_tick::force_checkpoint(ckpt_mgr, page_cache_inst, wal_v3, manifest, ctrl, ctrl_path, shard_id);
                    }
                    // Persist graph store to disk on shutdown.
                    #[cfg(feature = "graph")]
                    if let Some(ref dir) = persistence_dir {
                        let gs = shard_databases.graph_store_read(shard_id);
                        if gs.graph_count() > 0 {
                            if let Err(e) = crate::graph::recovery::save_graph_store(&gs, std::path::Path::new(dir), shard_id) {
                                tracing::warn!("Shard {shard_id}: failed to save graph store on shutdown: {e}");
                            } else {
                                info!("Shard {shard_id}: graph store saved to {dir}");
                            }
                        }
                    }
                    if let Some(ref mut wal) = wal_writer {
                        let _ = wal.shutdown();
                    }
                    if let Some(ref mut wal_v3) = wal_v3_writer {
                        let _ = wal_v3.flush_sync();
                    }
                    break;
                }
            }

            // Drain per-shard accept channel (dedicated monoio::spawn task, no cancel race)
            #[cfg(feature = "runtime-monoio")]
            if let Some(ref rx) = local_accept_rx {
                while let Ok(std_tcp_stream) = rx.try_recv() {
                    conn_accept::spawn_monoio_connection(
                        std_tcp_stream,
                        false,
                        &tls_config,
                        &shard_databases,
                        &dispatch_tx,
                        &pubsub_arc,
                        &blocking_rc,
                        &shutdown,
                        &aof_tx,
                        &tracking_rc,
                        &lua_rc,
                        &script_cache_rc,
                        &acl_table,
                        &runtime_config,
                        &server_config,
                        &all_notifiers,
                        &snapshot_trigger_tx,
                        &repl_state,
                        &cluster_state,
                        &cached_clock,
                        &remote_sub_map_arc,
                        &all_pubsub_registries,
                        &all_remote_sub_maps,
                        &affinity_tracker,
                        shard_id,
                        num_shards,
                        config_port,
                        &pending_wakers,
                        &spill_sender,
                        &spill_file_id,
                        &disk_offload_dir,
                    );
                }
            }

            // Non-blocking drain: process all pending connections before entering select!.
            // monoio::select! drops and recreates conn_rx.recv_async() every iteration
            // (when timer tick fires), leaving queued connections unprocessed for ~1ms.
            // try_recv() is zero-cost when empty (atomic load + early return).
            #[cfg(feature = "runtime-monoio")]
            while let Ok((std_tcp_stream, is_tls)) = conn_rx.try_recv() {
                conn_accept::spawn_monoio_connection(
                    std_tcp_stream,
                    is_tls,
                    &tls_config,
                    &shard_databases,
                    &dispatch_tx,
                    &pubsub_arc,
                    &blocking_rc,
                    &shutdown,
                    &aof_tx,
                    &tracking_rc,
                    &lua_rc,
                    &script_cache_rc,
                    &acl_table,
                    &runtime_config,
                    &server_config,
                    &all_notifiers,
                    &snapshot_trigger_tx,
                    &repl_state,
                    &cluster_state,
                    &cached_clock,
                    &remote_sub_map_arc,
                    &all_pubsub_registries,
                    &all_remote_sub_maps,
                    &affinity_tracker,
                    shard_id,
                    num_shards,
                    config_port,
                    &pending_wakers,
                    &spill_sender,
                    &spill_file_id,
                    &disk_offload_dir,
                );
            }
            // Wake cross-shard response tasks that registered during the previous iteration.
            #[cfg(feature = "runtime-monoio")]
            for waker in pending_wakers.borrow_mut().drain(..) {
                waker.wake();
            }

            // Monoio runtime: direct-await on 1ms periodic tick.
            // AVOID monoio::select! — it leaks ~100 bytes per re-entry (internal future
            // state re-allocation). At 1000 Hz this causes ~100 KB/s/shard RSS growth.
            // Instead: await the single timer, drain connections + SPSC non-blocking,
            // and dispatch sub-timers via counter-based modular arithmetic.
            #[cfg(feature = "runtime-monoio")]
            {
                // Check shutdown before awaiting (non-blocking)
                if shutdown.is_cancelled() {
                    info!("Shard {} shutting down (monoio)", self.id);
                    persistence_tick::drain_and_shutdown_spill(
                        &mut spill_thread,
                        &mut shard_manifest,
                        &shard_databases,
                        shard_id,
                    );
                    if let (
                        Some(ckpt_mgr),
                        Some(page_cache_inst),
                        Some(wal_v3),
                        Some(manifest),
                        Some(ctrl),
                        Some(ctrl_path),
                    ) = (
                        &mut checkpoint_manager,
                        &page_cache,
                        &mut wal_v3_writer,
                        &mut shard_manifest,
                        &mut control_file,
                        &control_file_path,
                    ) {
                        persistence_tick::force_checkpoint(
                            ckpt_mgr,
                            page_cache_inst,
                            wal_v3,
                            manifest,
                            ctrl,
                            ctrl_path,
                            shard_id,
                        );
                    }
                    if let Some(ref mut wal) = wal_writer {
                        let _ = wal.shutdown();
                    }
                    if let Some(ref mut wal_v3) = wal_v3_writer {
                        let _ = wal_v3.flush_sync();
                    }
                    break;
                }

                // Single await point — no select!, no per-iteration allocation.
                // .0.tick() bypasses the RuntimeInterval trait's Box::pin() wrapper.
                periodic_interval.0.tick().await;
                monoio_tick_counter = monoio_tick_counter.wrapping_add(1);

                // --- Periodic tick body (same as tokio periodic_interval branch) ---
                cached_clock.update();
                next_file_id = next_file_id.max(spill_file_id.get());

                let mut pending_snapshot = None;
                spsc_handler::drain_spsc_shared(
                    &shard_databases,
                    &mut consumers,
                    &mut *pubsub_arc.write(),
                    &blocking_rc,
                    &mut pending_snapshot,
                    &mut snapshot_state,
                    &mut wal_writer,
                    &mut wal_v3_writer,
                    &mut repl_backlog,
                    &mut replica_txs,
                    &repl_state,
                    shard_id,
                    &script_cache_rc,
                    &cached_clock,
                    &mut pending_migrations,
                    &mut *shard_databases.vector_store(shard_id),
                );
                for waker in pending_wakers.borrow_mut().drain(..) {
                    waker.wake();
                }
                persistence_tick::handle_pending_snapshot(
                    pending_snapshot,
                    &mut snapshot_state,
                    &mut snapshot_reply_tx,
                    &shard_databases,
                    disk_offload_base.as_deref(),
                    shard_id,
                );
                for (fd, state) in pending_migrations.drain(..) {
                    tracing::info!(
                        "Shard {}: accepting migrated connection (fd={}, client_id={}, from={})",
                        shard_id,
                        fd,
                        state.client_id,
                        state.peer_addr
                    );
                    conn_accept::spawn_migrated_monoio_connection(
                        fd,
                        state,
                        &shard_databases,
                        &dispatch_tx,
                        &pubsub_arc,
                        &blocking_rc,
                        &shutdown,
                        &aof_tx,
                        &tracking_rc,
                        &lua_rc,
                        &script_cache_rc,
                        &acl_table,
                        &runtime_config,
                        &server_config,
                        &all_notifiers,
                        &snapshot_trigger_tx,
                        &repl_state,
                        &cluster_state,
                        &cached_clock,
                        &remote_sub_map_arc,
                        &all_pubsub_registries,
                        &all_remote_sub_maps,
                        &affinity_tracker,
                        shard_id,
                        num_shards,
                        config_port,
                        &pending_wakers,
                        &spill_sender,
                        &spill_file_id,
                        &disk_offload_dir,
                    );
                }

                persistence_tick::check_auto_save_trigger(
                    &snapshot_trigger_rx,
                    &mut last_snapshot_epoch,
                    &mut snapshot_state,
                    &shard_databases,
                    &persistence_dir,
                    disk_offload_base.as_deref(),
                    shard_id,
                );

                if persistence_tick::advance_snapshot_segment(
                    &mut snapshot_state,
                    &shard_databases,
                    shard_id,
                ) {
                    if let Some(snap) = snapshot_state.as_mut() {
                        if let Err(e) = snap.finalize_async().await {
                            persistence_tick::finalize_snapshot_error(
                                &mut snapshot_state,
                                &mut snapshot_reply_tx,
                                shard_id,
                                &e.to_string(),
                            );
                            crate::command::persistence::bgsave_shard_done(false);
                        } else {
                            persistence_tick::finalize_snapshot_success(
                                &mut snapshot_state,
                                &mut snapshot_reply_tx,
                                &mut wal_writer,
                                shard_id,
                            );
                            crate::command::persistence::bgsave_shard_done(true);
                            bgsave_checkpoint_requested = true;
                        }
                    }
                }

                // Drain local-write WAL channel
                while let Ok(data) = wal_append_rx.try_recv() {
                    if let Some(ref mut wal) = wal_writer {
                        wal.append(&data);
                    }
                    if let Some(ref mut wal) = wal_v3_writer {
                        wal.append(
                            crate::persistence::wal_v3::record::WalRecordType::Command,
                            &data,
                        );
                    }
                }

                persistence_tick::flush_wal_if_needed(&mut wal_writer);
                persistence_tick::flush_wal_v3_if_needed(&mut wal_v3_writer);

                if server_config.appendfsync == "always" {
                    if let Some(ref mut wal) = wal_v3_writer {
                        if let Err(e) = wal.flush_sync() {
                            tracing::error!("WAL v3 appendfsync=always failed: {}", e);
                        }
                    }
                }

                // Checkpoint protocol tick (disk-offload only)
                if let (
                    Some(ckpt_mgr),
                    Some(page_cache_inst),
                    Some(wal_v3),
                    Some(manifest),
                    Some(ctrl),
                    Some(ctrl_path),
                ) = (
                    &mut checkpoint_manager,
                    &page_cache,
                    &mut wal_v3_writer,
                    &mut shard_manifest,
                    &mut control_file,
                    &control_file_path,
                ) {
                    if bgsave_checkpoint_requested && !ckpt_mgr.is_active() {
                        let lsn = wal_v3.current_lsn();
                        let dirty = page_cache_inst.dirty_page_count();
                        ckpt_mgr.force_begin(lsn, dirty);
                        bgsave_checkpoint_requested = false;
                    }
                    persistence_tick::maybe_begin_checkpoint(
                        ckpt_mgr,
                        wal_v3,
                        page_cache_inst,
                        wal_bytes_since_checkpoint,
                    );
                    if persistence_tick::handle_checkpoint_tick(
                        ckpt_mgr,
                        page_cache_inst,
                        wal_v3,
                        manifest,
                        ctrl,
                        ctrl_path,
                    ) {
                        wal_bytes_since_checkpoint = 0;
                    }
                }

                // --- Counter-based sub-timer dispatch ---
                // block_timeout: every 10ms (10 ticks)
                if monoio_tick_counter % 10 == 0 {
                    timers::expire_blocked_clients(&blocking_rc);
                }
                // expiry + eviction: every 100ms (100 ticks)
                if monoio_tick_counter % 100 == 0 {
                    timers::run_active_expiry(&shard_databases, shard_id);
                    persistence_tick::run_eviction_tick(
                        spill_thread.as_ref(),
                        &mut shard_manifest,
                        &shard_databases,
                        shard_id,
                        &server_config,
                        &runtime_config,
                        &page_cache,
                        &mut next_file_id,
                        &mut wal_v3_writer,
                        &spill_file_id,
                    );
                }
                // WAL fsync: every 1s (1000 ticks)
                if monoio_tick_counter % 1000 == 0 {
                    timers::sync_wal(&mut wal_writer);
                    timers::sync_wal_v3(&mut wal_v3_writer);
                }
                // Warm tier check: every warm_poll_ms ticks
                if monoio_tick_counter % (warm_poll_ms as u64) == 0 {
                    if server_config.disk_offload_enabled() {
                        if let Some(ref mut manifest) = shard_manifest {
                            let shard_dir = server_config
                                .effective_disk_offload_dir()
                                .join(format!("shard-{}", shard_id));
                            persistence_tick::check_warm_transitions(
                                &*shard_databases.vector_store(shard_id),
                                &shard_dir,
                                manifest,
                                server_config.segment_warm_after,
                                &mut next_file_id,
                                shard_id,
                                &mut wal_v3_writer,
                            );
                        }
                    }
                }
                // Cold tier check: every cold_poll_secs * 1000 ticks
                if monoio_tick_counter % (cold_poll_secs as u64 * 1000) == 0 {
                    if server_config.disk_offload_enabled() && server_config.segment_cold_after > 0
                    {
                        if let Some(ref mut manifest) = shard_manifest {
                            let shard_dir = server_config
                                .effective_disk_offload_dir()
                                .join(format!("shard-{}", shard_id));
                            persistence_tick::check_cold_transitions(
                                &*shard_databases.vector_store(shard_id),
                                &shard_dir,
                                manifest,
                                server_config.segment_cold_after,
                                &mut next_file_id,
                                shard_id,
                            );
                        }
                    }
                }
            }
        }

        // Close per-shard SO_REUSEPORT listener fd if created (Linux + tokio only).
        #[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
        if let Some(lfd) = uring_listener_fd {
            // SAFETY: lfd is a valid SO_REUSEPORT listener fd created by this shard.
            // The event loop is shutting down, so no io_uring SQEs reference this fd.
            unsafe {
                libc::close(lfd);
            }
        }

        // Databases now live in Arc<ShardDatabases>, no reclaim needed.
        self.databases.clear();
        self.pubsub_registry = std::mem::take(&mut *pubsub_arc.write());
    }
}
