pub mod coordinator;
pub mod dispatch;
pub mod mesh;
pub mod numa;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use ringbuf::traits::Consumer;
use ringbuf::HeapCons;
use ringbuf::HeapProd;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::command::{dispatch as cmd_dispatch, DispatchResult};
use crate::config::RuntimeConfig;
use crate::persistence::aof;
use crate::persistence::snapshot::SnapshotState;
use crate::persistence::wal::WalWriter;
use crate::command::connection as conn_cmd;
use crate::blocking::BlockingRegistry;
use crate::pubsub::PubSubRegistry;
use crate::replication::backlog::ReplicationBacklog;
use crate::replication::state::ReplicationState;
use crate::server::connection::handle_connection_sharded;
use crate::storage::Database;
use crate::tracking::TrackingTable;

use std::sync::{Arc, RwLock};

#[cfg(target_os = "linux")]
use crate::io::{IoEvent, UringConfig, UringDriver, WritevGuard};

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

/// In-flight send buffer variants for proper RAII lifetime management (Linux only).
///
/// Keeps buffers alive until the corresponding io_uring SendComplete CQE arrives,
/// replacing the previous std::mem::forget memory leak.
#[cfg(target_os = "linux")]
enum InFlightSend {
    /// Serialized response buffer for non-BulkString frames.
    Buf(bytes::BytesMut),
    /// Scatter-gather writev guard for BulkString (zero-copy GET) responses.
    Writev(WritevGuard),
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
            match wal::replay_wal(&mut self.databases, &wal_file) {
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
        mut conn_rx: mpsc::Receiver<tokio::net::TcpStream>,
        mut consumers: Vec<HeapCons<ShardMessage>>,
        producers: Vec<HeapProd<ShardMessage>>,
        shutdown: CancellationToken,
        aof_tx: Option<mpsc::Sender<crate::persistence::aof::AofMessage>>,
        bind_addr: Option<String>,
        persistence_dir: Option<String>,
        mut snapshot_trigger_rx: tokio::sync::watch::Receiver<u64>,
        repl_state_ext: Option<Arc<RwLock<ReplicationState>>>,
        cluster_state: Option<std::sync::Arc<std::sync::RwLock<crate::cluster::ClusterState>>>,
        config_port: u16,
    ) {
        // On Linux, attempt to initialize io_uring for high-performance I/O.
        // If initialization fails, fall back to the Tokio path (same as macOS).
        #[cfg(target_os = "linux")]
        let mut uring_state: Option<UringDriver> = {
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
        };

        // Wire multishot accept: create per-shard SO_REUSEPORT listener socket
        #[cfg(target_os = "linux")]
        let mut uring_listener_fd: Option<std::os::fd::RawFd> = None;
        #[cfg(target_os = "linux")]
        if let Some(ref mut d) = uring_state {
            if let Some(ref addr) = bind_addr {
                match Self::create_reuseport_listener(addr) {
                    Ok(listener_fd) => {
                        if let Err(e) = d.submit_multishot_accept(listener_fd) {
                            tracing::warn!("Shard {}: multishot accept failed: {}, using conn_rx", self.id, e);
                        } else {
                            info!("Shard {}: multishot accept armed on fd {}", self.id, listener_fd);
                            uring_listener_fd = Some(listener_fd);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Shard {}: SO_REUSEPORT bind failed: {}, using conn_rx", self.id, e);
                    }
                }
            }
        }

        // Track per-connection parse state for io_uring path (Linux only).
        #[cfg(target_os = "linux")]
        let mut uring_parse_bufs: std::collections::HashMap<u32, bytes::BytesMut> =
            std::collections::HashMap::new();

        // Track in-flight send buffers for proper RAII cleanup (Linux only).
        // Replaces the previous std::mem::forget leak.
        #[cfg(target_os = "linux")]
        let mut inflight_sends: std::collections::HashMap<u32, Vec<InFlightSend>> =
            std::collections::HashMap::new();

        #[cfg(not(target_os = "linux"))]
        {
            let _ = &bind_addr; // Suppress unused warning on non-Linux
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

        // Per-shard snapshot state (None when no snapshot is active)
        let mut snapshot_state: Option<SnapshotState> = None;
        let mut snapshot_reply_tx: Option<tokio::sync::oneshot::Sender<Result<(), String>>> = None;

        // Per-shard WAL writer (created if persistence dir is configured)
        let mut wal_writer: Option<WalWriter> = if let Some(ref dir) = persistence_dir {
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
            None
        };

        // Per-shard replication backlog (in-memory, 1MB capacity, for partial resync).
        // IMPORTANT: monotonic offsets here are independent of WAL file size.
        let backlog_capacity = 1024 * 1024; // 1MB per shard
        let mut repl_backlog: Option<ReplicationBacklog> =
            Some(ReplicationBacklog::new(backlog_capacity));

        // Per-shard replica sender channels: (replica_id, Sender<Bytes>).
        // Populated by ShardMessage::RegisterReplica, cleared by UnregisterReplica.
        let mut replica_txs: Vec<(u64, tokio::sync::mpsc::Sender<bytes::Bytes>)> = Vec::new();

        // Shared ReplicationState injected from server startup.
        // When Some, enables WAL fan-out offset tracking and replica connection handling.
        let repl_state: Option<Arc<RwLock<ReplicationState>>> = repl_state_ext;

        // Track last seen snapshot epoch to detect watch channel triggers
        let mut last_snapshot_epoch = *snapshot_trigger_rx.borrow();

        let mut expiry_interval = tokio::time::interval(Duration::from_millis(100));
        // SPSC drain interval -- check for cross-shard messages every 1ms.
        // On Linux with io_uring, this also polls for io_uring completions.
        let mut spsc_interval = tokio::time::interval(Duration::from_millis(1));
        // Blocking command timeout scanner -- expire timed-out blocked clients every 10ms.
        let mut block_timeout_interval = tokio::time::interval(Duration::from_millis(10));

        loop {
            tokio::select! {
                // Accept new connections from listener
                stream = conn_rx.recv() => {
                    match stream {
                        Some(tcp_stream) => {
                            // On Linux with io_uring: extract raw fd, register with
                            // UringDriver for io_uring-based recv/send. Skip Tokio task.
                            #[cfg(target_os = "linux")]
                            {
                                if let Some(ref mut driver) = uring_state {
                                    match tcp_stream.into_std() {
                                        Ok(std_stream) => {
                                            use std::os::unix::io::IntoRawFd;
                                            let raw_fd = std_stream.into_raw_fd();
                                            match driver.register_connection(raw_fd) {
                                                Ok(Some(_conn_id)) => {
                                                    // Connection registered, io_uring handles I/O
                                                }
                                                Ok(None) => {
                                                    // FD table full, connection rejected (close handled inside)
                                                }
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
                                // Fallthrough: uring_state is None, use Tokio path below
                            }

                            let dbs = databases.clone();
                            let dtx = dispatch_tx.clone();
                            let psr = pubsub_rc.clone();
                            let blk = blocking_rc.clone();
                            let sd = shutdown.clone();
                            let aof = aof_tx.clone();
                            let trk = tracking_rc.clone();
                            let cid = conn_cmd::next_client_id();
                            let rs = repl_state.clone();
                            let cs = cluster_state.clone();
                            let cp = config_port;
                            tokio::task::spawn_local(async move {
                                handle_connection_sharded(
                                    tcp_stream,
                                    dbs,
                                    shard_id,
                                    num_shards,
                                    dtx,
                                    psr,
                                    blk,
                                    sd,
                                    None, // requirepass: TODO wire from shard config
                                    aof,
                                    trk,
                                    cid,
                                    rs,
                                    cs,
                                    cp,
                                ).await;
                            });
                        }
                        None => {
                            info!("Shard {} connection channel closed", self.id);
                            break;
                        }
                    }
                }
                // Drain SPSC consumers and poll io_uring completions (Linux)
                _ = spsc_interval.tick() => {
                    // Drain SPSC, collecting SnapshotBegin messages for deferred handling
                    let mut pending_snapshot: Option<(u64, std::path::PathBuf, tokio::sync::oneshot::Sender<Result<(), String>>)> = None;
                    Self::drain_spsc_shared(
                        &databases,
                        &mut consumers,
                        &mut *pubsub_rc.borrow_mut(),
                        &blocking_rc,
                        &mut pending_snapshot,
                        &mut snapshot_state,
                        &mut wal_writer,
                        &mut repl_backlog,
                        &mut replica_txs,
                        &repl_state,
                        shard_id,
                    );

                    // Handle pending SnapshotBegin from SPSC
                    if let Some((epoch, snap_dir, reply_tx)) = pending_snapshot {
                        if snapshot_state.is_some() {
                            let _ = reply_tx.send(Err("Snapshot already in progress".to_string()));
                        } else {
                            let dbs = databases.borrow();
                            let snap_path = snap_dir.join(format!("shard-{}.rrdshard", shard_id));
                            snapshot_state = Some(SnapshotState::new(shard_id as u16, epoch, &dbs, snap_path));
                            snapshot_reply_tx = Some(reply_tx);
                            drop(dbs);
                        }
                    }

                    // Check watch channel for auto-save snapshot triggers
                    if snapshot_trigger_rx.has_changed().unwrap_or(false) {
                        let new_epoch = *snapshot_trigger_rx.borrow_and_update();
                        if new_epoch > last_snapshot_epoch && snapshot_state.is_none() {
                            last_snapshot_epoch = new_epoch;
                            if let Some(ref dir) = persistence_dir {
                                let snap_path = std::path::PathBuf::from(dir)
                                    .join(format!("shard-{}.rrdshard", shard_id));
                                let dbs = databases.borrow();
                                snapshot_state = Some(SnapshotState::new(
                                    shard_id as u16, new_epoch, &dbs, snap_path,
                                ));
                                drop(dbs);
                                // No reply_tx for auto-save triggered snapshots
                            }
                        }
                    }

                    // Advance snapshot one segment per tick (cooperative)
                    if let Some(ref mut snap) = snapshot_state {
                        let dbs = databases.borrow();
                        let done = snap.advance_one_segment(&dbs);
                        drop(dbs);
                        if done {
                            let epoch = snap.epoch;
                            if let Err(e) = snap.finalize() {
                                tracing::error!("Shard {}: snapshot finalize failed: {}", shard_id, e);
                                if let Some(tx) = snapshot_reply_tx.take() {
                                    let _ = tx.send(Err(format!("finalize failed: {}", e)));
                                }
                            } else {
                                info!("Shard {}: snapshot epoch {} complete", shard_id, epoch);
                                // Truncate WAL after successful snapshot
                                if let Some(ref mut wal) = wal_writer {
                                    let _ = wal.truncate_after_snapshot(epoch);
                                }
                                if let Some(tx) = snapshot_reply_tx.take() {
                                    let _ = tx.send(Ok(()));
                                }
                            }
                            snapshot_state = None;
                        }
                    }

                    // Flush WAL on 1ms tick
                    if let Some(ref mut wal) = wal_writer {
                        let _ = wal.flush_if_needed();
                    }

                    // On Linux: poll io_uring for completions (non-blocking)
                    #[cfg(target_os = "linux")]
                    if let Some(ref mut driver) = uring_state {
                        // Non-blocking submit (flush pending SQEs without waiting)
                        let _ = driver.submit_and_wait_nonblocking();
                        let events = driver.drain_completions();
                        for event in events {
                            Self::handle_uring_event(
                                event,
                                driver,
                                &databases,
                                &mut uring_parse_bufs,
                                &mut inflight_sends,
                                uring_listener_fd,
                            );
                        }
                    }
                }
                // Expire timed-out blocked clients every 10ms
                _ = block_timeout_interval.tick() => {
                    let now = tokio::time::Instant::now();
                    blocking_rc.borrow_mut().expire_timed_out(now);
                }
                // Cooperative active expiry
                _ = expiry_interval.tick() => {
                    let mut dbs = databases.borrow_mut();
                    for db in dbs.iter_mut() {
                        crate::server::expiration::expire_cycle_direct(db);
                    }
                }
                _ = shutdown.cancelled() => {
                    info!("Shard {} shutting down", self.id);
                    // Flush and shutdown WAL writer
                    if let Some(ref mut wal) = wal_writer {
                        let _ = wal.shutdown();
                    }
                    break;
                }
            }
        }

        // Close per-shard SO_REUSEPORT listener fd if created (Linux only).
        #[cfg(target_os = "linux")]
        if let Some(lfd) = uring_listener_fd {
            unsafe { libc::close(lfd); }
        }

        // Restore databases and pubsub_registry back to self for cleanup.
        self.databases = match Rc::try_unwrap(databases) {
            Ok(refcell) => refcell.into_inner(),
            Err(_) => {
                info!("Shard {}: could not reclaim databases (outstanding Rc references)", self.id);
                Vec::new()
            }
        };
        self.pubsub_registry = match Rc::try_unwrap(pubsub_rc) {
            Ok(refcell) => refcell.into_inner(),
            Err(_) => {
                info!("Shard {}: could not reclaim pubsub_registry (outstanding Rc references)", self.id);
                PubSubRegistry::new()
            }
        };
    }

    /// Drain all SPSC consumer channels, processing cross-shard messages.
    ///
    /// SnapshotBegin messages are collected into `pending_snapshot` for deferred handling
    /// (the caller has mutable access to snapshot_state). COW intercepts and WAL appends
    /// happen inline for Execute/MultiExecute write commands.
    fn drain_spsc_shared(
        databases: &Rc<RefCell<Vec<Database>>>,
        consumers: &mut [HeapCons<ShardMessage>],
        pubsub_registry: &mut PubSubRegistry,
        blocking_registry: &Rc<RefCell<BlockingRegistry>>,
        pending_snapshot: &mut Option<(u64, std::path::PathBuf, tokio::sync::oneshot::Sender<Result<(), String>>)>,
        snapshot_state: &mut Option<SnapshotState>,
        wal_writer: &mut Option<WalWriter>,
        repl_backlog: &mut Option<ReplicationBacklog>,
        replica_txs: &mut Vec<(u64, tokio::sync::mpsc::Sender<bytes::Bytes>)>,
        repl_state: &Option<Arc<RwLock<ReplicationState>>>,
        shard_id: usize,
    ) {
        const MAX_DRAIN_PER_CYCLE: usize = 256;
        let mut drained = 0;

        for consumer in consumers.iter_mut() {
            while drained < MAX_DRAIN_PER_CYCLE {
                match consumer.try_pop() {
                    Some(msg) => {
                        drained += 1;
                        Self::handle_shard_message_shared(
                            databases,
                            pubsub_registry,
                            blocking_registry,
                            msg,
                            pending_snapshot,
                            snapshot_state,
                            wal_writer,
                            repl_backlog,
                            replica_txs,
                            repl_state,
                            shard_id,
                        );
                    }
                    None => break,
                }
            }
            if drained >= MAX_DRAIN_PER_CYCLE {
                break;
            }
        }
    }

    /// Process a single cross-shard message using shared database access.
    ///
    /// Performs COW intercept for write commands when a snapshot is active,
    /// and appends write commands to the per-shard WAL writer.
    fn handle_shard_message_shared(
        databases: &Rc<RefCell<Vec<Database>>>,
        pubsub_registry: &mut PubSubRegistry,
        blocking_registry: &Rc<RefCell<BlockingRegistry>>,
        msg: ShardMessage,
        pending_snapshot: &mut Option<(u64, std::path::PathBuf, tokio::sync::oneshot::Sender<Result<(), String>>)>,
        snapshot_state: &mut Option<SnapshotState>,
        wal_writer: &mut Option<WalWriter>,
        repl_backlog: &mut Option<ReplicationBacklog>,
        replica_txs: &mut Vec<(u64, tokio::sync::mpsc::Sender<bytes::Bytes>)>,
        repl_state: &Option<Arc<RwLock<ReplicationState>>>,
        shard_id: usize,
    ) {
        match msg {
            ShardMessage::Execute {
                db_index,
                command,
                reply_tx,
            } => {
                let response = {
                    let mut dbs = databases.borrow_mut();
                    let db_count = dbs.len();
                    let db_idx = db_index.min(db_count.saturating_sub(1));
                    dbs[db_idx].refresh_now();
                    let (cmd, args) = match Self::extract_command_static(&command) {
                        Some(pair) => pair,
                        None => {
                            let _ = reply_tx.send(crate::protocol::Frame::Error(
                                bytes::Bytes::from_static(b"ERR invalid command format"),
                            ));
                            return;
                        }
                    };

                    // COW intercept: capture old value before write if snapshot is active
                    if aof::is_write_command(cmd) {
                        Self::cow_intercept(snapshot_state, &dbs, db_idx, &command);
                    }

                    let mut selected = db_idx;
                    let result = cmd_dispatch(&mut dbs[db_idx], cmd, args, &mut selected, db_count);
                    let frame = match result {
                        DispatchResult::Response(f) => f,
                        DispatchResult::Quit(f) => f,
                    };

                    // WAL append + replication fan-out for successful write commands
                    if aof::is_write_command(cmd) && !matches!(frame, crate::protocol::Frame::Error(_)) {
                        let serialized = aof::serialize_command(&command);
                        Self::wal_append_and_fanout(
                            &serialized, wal_writer, repl_backlog, replica_txs, repl_state, shard_id,
                        );
                    }

                    // Post-dispatch wakeup hooks for producer commands (cross-shard blocking)
                    if !matches!(frame, crate::protocol::Frame::Error(_)) {
                        if cmd.eq_ignore_ascii_case(b"LPUSH")
                            || cmd.eq_ignore_ascii_case(b"RPUSH")
                            || cmd.eq_ignore_ascii_case(b"LMOVE")
                        {
                            if let Some(key) = args.first().and_then(|f| crate::server::connection::extract_bytes(f)) {
                                let mut reg = blocking_registry.borrow_mut();
                                crate::blocking::wakeup::try_wake_list_waiter(
                                    &mut reg, &mut dbs[db_idx], db_idx, &key,
                                );
                            }
                        }
                        if cmd.eq_ignore_ascii_case(b"ZADD") {
                            if let Some(key) = args.first().and_then(|f| crate::server::connection::extract_bytes(f)) {
                                let mut reg = blocking_registry.borrow_mut();
                                crate::blocking::wakeup::try_wake_zset_waiter(
                                    &mut reg, &mut dbs[db_idx], db_idx, &key,
                                );
                            }
                        }
                        if cmd.eq_ignore_ascii_case(b"XADD") {
                            if let Some(key) = args.first().and_then(|f| crate::server::connection::extract_bytes(f)) {
                                let mut reg = blocking_registry.borrow_mut();
                                crate::blocking::wakeup::try_wake_stream_waiter(
                                    &mut reg, &mut dbs[db_idx], db_idx, &key,
                                );
                            }
                        }
                    }

                    frame
                };
                let _ = reply_tx.send(response);
            }
            ShardMessage::MultiExecute {
                db_index,
                commands,
                reply_tx,
            } => {
                let mut results = Vec::with_capacity(commands.len());
                let mut dbs = databases.borrow_mut();
                let db_count = dbs.len();
                let db_idx = db_index.min(db_count.saturating_sub(1));
                dbs[db_idx].refresh_now();
                for (_key, cmd_frame) in &commands {
                    let (cmd, args) = match Self::extract_command_static(cmd_frame) {
                        Some(pair) => pair,
                        None => {
                            results.push(crate::protocol::Frame::Error(
                                bytes::Bytes::from_static(b"ERR invalid command format"),
                            ));
                            continue;
                        }
                    };

                    // COW intercept for each write command in the batch
                    if aof::is_write_command(cmd) {
                        Self::cow_intercept(snapshot_state, &dbs, db_idx, cmd_frame);
                    }

                    let mut selected = db_idx;
                    let result =
                        cmd_dispatch(&mut dbs[db_idx], cmd, args, &mut selected, db_count);
                    let frame = match result {
                        DispatchResult::Response(f) => f,
                        DispatchResult::Quit(f) => f,
                    };

                    // WAL append + replication fan-out for successful write commands
                    if aof::is_write_command(cmd) && !matches!(frame, crate::protocol::Frame::Error(_)) {
                        let serialized = aof::serialize_command(cmd_frame);
                        Self::wal_append_and_fanout(
                            &serialized, wal_writer, repl_backlog, replica_txs, repl_state, shard_id,
                        );
                    }

                    results.push(frame);
                }
                let _ = reply_tx.send(results);
            }
            ShardMessage::PubSubFanOut { channel, message } => {
                pubsub_registry.publish(&channel, &message);
            }
            ShardMessage::SnapshotBegin { epoch, snapshot_dir, reply_tx } => {
                // Defer to main event loop where we have mutable access to snapshot_state
                *pending_snapshot = Some((epoch, snapshot_dir, reply_tx));
            }
            ShardMessage::BlockRegister { db_index, key, wait_id, cmd, reply_tx } => {
                let entry = crate::blocking::WaitEntry {
                    wait_id,
                    cmd,
                    reply_tx,
                    deadline: None, // Remote registrations don't manage timeout locally
                };
                let mut reg = blocking_registry.borrow_mut();
                reg.register(db_index, key.clone(), entry);
                // Check if data is already available (race: data arrived before registration).
                // Only attempt wakeup if the key exists -- try_wake_list_waiter
                // destroys the waiter even when no data is available (sends None = timeout).
                let mut dbs = databases.borrow_mut();
                if dbs[db_index].exists(&key) {
                    let db = &mut dbs[db_index];
                    crate::blocking::wakeup::try_wake_list_waiter(&mut reg, db, db_index, &key);
                    crate::blocking::wakeup::try_wake_zset_waiter(&mut reg, db, db_index, &key);
                    crate::blocking::wakeup::try_wake_stream_waiter(&mut reg, db, db_index, &key);
                }
            }
            ShardMessage::BlockCancel { wait_id } => {
                blocking_registry.borrow_mut().remove_wait(wait_id);
            }
            ShardMessage::Shutdown => {
                info!("Received shutdown via SPSC");
            }
            ShardMessage::RegisterReplica { replica_id, tx } => {
                replica_txs.push((replica_id, tx));
            }
            ShardMessage::UnregisterReplica { replica_id } => {
                replica_txs.retain(|(id, _)| *id != replica_id);
            }
            ShardMessage::NewConnection(_) => {
                // NewConnection is handled via conn_rx, not SPSC
            }
        }
    }

    /// COW intercept: capture old value for a key being written if its segment is pending.
    ///
    /// Called before cmd_dispatch to preserve snapshot consistency. Only clones the old entry
    /// if the key's segment is actually pending serialization (fast bool check in hot path).
    fn cow_intercept(
        snapshot: &mut Option<SnapshotState>,
        dbs: &[Database],
        db_index: usize,
        command: &crate::protocol::Frame,
    ) {
        let Some(snap) = snapshot else { return };
        // Extract the primary key from the command (args[1] for Array commands)
        let key = match command {
            crate::protocol::Frame::Array(args) if args.len() >= 2 => {
                match &args[1] {
                    crate::protocol::Frame::BulkString(k) => k,
                    _ => return,
                }
            }
            _ => return,
        };
        let hash = crate::storage::dashtable::hash_key(key);
        let seg_idx = dbs[db_index].data().segment_index_for_hash(hash);
        if snap.is_segment_pending(db_index, seg_idx) {
            if let Some(old_entry) = dbs[db_index].data().get(key) {
                snap.capture_cow(db_index, seg_idx, key.clone(), old_entry.clone());
            }
        }
    }

    /// Process a single io_uring completion event (Linux only).
    ///
    /// Handles recv (parse RESP frames + execute commands + send responses),
    /// disconnect, recv rearm, accept, and send completion events.
    /// Command dispatch reuses the same `extract_command_static` + `cmd_dispatch`
    /// path as the Tokio connection handler.
    #[cfg(target_os = "linux")]
    fn handle_uring_event(
        event: IoEvent,
        driver: &mut UringDriver,
        databases: &Rc<RefCell<Vec<Database>>>,
        parse_bufs: &mut std::collections::HashMap<u32, bytes::BytesMut>,
        inflight_sends: &mut std::collections::HashMap<u32, Vec<InFlightSend>>,
        uring_listener_fd: Option<std::os::fd::RawFd>,
    ) {
        match event {
            IoEvent::Recv { conn_id, data } => {
                let parse_buf = parse_bufs
                    .entry(conn_id)
                    .or_insert_with(|| bytes::BytesMut::with_capacity(4096));
                parse_buf.extend_from_slice(&data);

                let parse_config = crate::protocol::ParseConfig::default();
                loop {
                    match crate::protocol::parse(parse_buf, &parse_config) {
                        Ok(Some(frame)) => {
                            // Execute command against shard databases
                            let response = {
                                let mut dbs = databases.borrow_mut();
                                let db_count = dbs.len();
                                let (cmd, args) = match Self::extract_command_static(&frame) {
                                    Some(pair) => pair,
                                    None => {
                                        crate::protocol::Frame::Error(
                                            bytes::Bytes::from_static(b"ERR invalid command"),
                                        )
                                    }
                                };
                                dbs[0].refresh_now();
                                let mut selected = 0usize;
                                let result = cmd_dispatch(
                                    &mut dbs[0], cmd, args, &mut selected, db_count,
                                );
                                match result {
                                    DispatchResult::Response(f) => f,
                                    DispatchResult::Quit(f) => f,
                                }
                            };

                            // Send response via io_uring with proper buffer lifetime management.
                            // BulkString responses use writev scatter-gather (zero-copy from DashTable).
                            // All other responses use submit_send with tracked InFlightSend::Buf.
                            match response {
                                crate::protocol::Frame::BulkString(ref value) if !value.is_empty() => {
                                    // Zero-copy path: scatter-gather via writev
                                    match driver.submit_writev_bulkstring(conn_id, value.clone()) {
                                        Ok(guard) => {
                                            inflight_sends
                                                .entry(conn_id)
                                                .or_default()
                                                .push(InFlightSend::Writev(guard));
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                "writev failed for conn {}: {}, falling back to send",
                                                conn_id, e
                                            );
                                            // Fallback to regular send
                                            let mut resp_buf = bytes::BytesMut::new();
                                            crate::protocol::serialize(&response, &mut resp_buf);
                                            let len = resp_buf.len() as u32;
                                            let ptr = resp_buf.as_ptr();
                                            let _ = driver.submit_send(conn_id, ptr, len);
                                            inflight_sends
                                                .entry(conn_id)
                                                .or_default()
                                                .push(InFlightSend::Buf(resp_buf));
                                        }
                                    }
                                }
                                _ => {
                                    // Standard path: serialize + send (buffer tracked, no leak)
                                    let mut resp_buf = bytes::BytesMut::new();
                                    crate::protocol::serialize(&response, &mut resp_buf);
                                    let len = resp_buf.len() as u32;
                                    let ptr = resp_buf.as_ptr();
                                    let _ = driver.submit_send(conn_id, ptr, len);
                                    inflight_sends
                                        .entry(conn_id)
                                        .or_default()
                                        .push(InFlightSend::Buf(resp_buf));
                                }
                            }
                        }
                        Ok(None) => break, // Need more data
                        Err(crate::protocol::ParseError::Incomplete) => break,
                        Err(_) => {
                            // Protocol error: close connection
                            let _ = driver.close_connection(conn_id);
                            parse_bufs.remove(&conn_id);
                            inflight_sends.remove(&conn_id);
                            break;
                        }
                    }
                }
            }
            IoEvent::Disconnect { conn_id } => {
                let _ = driver.close_connection(conn_id);
                parse_bufs.remove(&conn_id);
                inflight_sends.remove(&conn_id);
            }
            IoEvent::RecvNeedsRearm { conn_id } => {
                let _ = driver.rearm_recv(conn_id);
            }
            IoEvent::Accept { raw_fd } => {
                // Direct accept from multishot (if listener fd is registered)
                let _ = driver.register_connection(raw_fd);
            }
            IoEvent::AcceptError { .. } => {
                // Multishot accept cancelled on error -- re-submit
                if let Some(lfd) = uring_listener_fd {
                    let _ = driver.submit_multishot_accept(lfd);
                }
            }
            IoEvent::SendComplete { conn_id } => {
                // Drop the oldest in-flight send buffer (FIFO order matches CQE order)
                if let Some(sends) = inflight_sends.get_mut(&conn_id) {
                    if !sends.is_empty() {
                        sends.remove(0);
                    }
                    if sends.is_empty() {
                        inflight_sends.remove(&conn_id);
                    }
                }
            }
            IoEvent::SendError { conn_id, .. } => {
                let _ = driver.close_connection(conn_id);
                parse_bufs.remove(&conn_id);
                inflight_sends.remove(&conn_id);
            }
            IoEvent::Timeout | IoEvent::Wakeup => {
                // Handled by the spsc_interval tick (already draining SPSC)
            }
        }
    }

    /// Create an SO_REUSEPORT TCP listener socket for per-shard multishot accept.
    ///
    /// Each shard binds to the same address with SO_REUSEPORT, allowing the kernel
    /// to distribute incoming connections across shard threads without a shared listener.
    #[cfg(target_os = "linux")]
    fn create_reuseport_listener(addr: &str) -> std::io::Result<std::os::fd::RawFd> {
        use std::net::SocketAddr;
        let sock_addr: SocketAddr = addr.parse().map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, e)
        })?;

        // Create TCP socket
        let fd = unsafe {
            libc::socket(
                libc::AF_INET,
                libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                0,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }

        // Set SO_REUSEPORT + SO_REUSEADDR
        let optval: libc::c_int = 1;
        unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_REUSEPORT,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_REUSEADDR,
                &optval as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }

        // Bind
        match sock_addr {
            SocketAddr::V4(v4) => {
                let sa = libc::sockaddr_in {
                    sin_family: libc::AF_INET as libc::sa_family_t,
                    sin_port: v4.port().to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: u32::from_ne_bytes(v4.ip().octets()),
                    },
                    sin_zero: [0; 8],
                };
                let ret = unsafe {
                    libc::bind(
                        fd,
                        &sa as *const libc::sockaddr_in as *const libc::sockaddr,
                        std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
                    )
                };
                if ret < 0 {
                    unsafe { libc::close(fd); }
                    return Err(std::io::Error::last_os_error());
                }
            }
            SocketAddr::V6(_) => {
                unsafe { libc::close(fd); }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "IPv6 not yet supported for SO_REUSEPORT listener",
                ));
            }
        }

        // Listen with backlog 1024
        let ret = unsafe { libc::listen(fd, 1024) };
        if ret < 0 {
            unsafe { libc::close(fd); }
            return Err(std::io::Error::last_os_error());
        }

        Ok(fd)
    }

    /// Append WAL bytes, update the replication backlog, advance the monotonic shard offset,
    /// and fan-out to all connected replica sender channels (non-blocking try_send).
    ///
    /// CRITICAL: shard_offset in ReplicationState is SEPARATE from WalWriter::bytes_written.
    /// WalWriter::bytes_written resets on snapshot truncation; shard_offset NEVER resets.
    fn wal_append_and_fanout(
        data: &[u8],
        wal_writer: &mut Option<WalWriter>,
        repl_backlog: &mut Option<ReplicationBacklog>,
        replica_txs: &[(u64, tokio::sync::mpsc::Sender<bytes::Bytes>)],
        repl_state: &Option<Arc<RwLock<ReplicationState>>>,
        shard_id: usize,
    ) {
        // 1. WAL append (disk durability, unchanged behavior)
        if let Some(w) = wal_writer {
            w.append(data);
        }
        // 2. Replication backlog (in-memory circular buffer for partial resync)
        if let Some(backlog) = repl_backlog {
            backlog.append(data);
        }
        // 3. Advance monotonic replication offset (NEVER resets on WAL truncation)
        if let Some(rs) = repl_state {
            if let Ok(rs) = rs.try_read() {
                rs.increment_shard_offset(shard_id, data.len() as u64);
            }
        }
        // 4. Fan-out to replica sender tasks (non-blocking: lagging replicas are skipped)
        if !replica_txs.is_empty() {
            let bytes = bytes::Bytes::copy_from_slice(data);
            for (_id, tx) in replica_txs {
                let _ = tx.try_send(bytes.clone());
            }
        }
    }

    /// Extract command name and args from a Frame (static helper for SPSC dispatch).
    fn extract_command_static(frame: &crate::protocol::Frame) -> Option<(&[u8], &[crate::protocol::Frame])> {
        match frame {
            crate::protocol::Frame::Array(args) if !args.is_empty() => {
                let name = match &args[0] {
                    crate::protocol::Frame::BulkString(s) => s.as_ref(),
                    crate::protocol::Frame::SimpleString(s) => s.as_ref(),
                    _ => return None,
                };
                Some((name, &args[1..]))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ringbuf::HeapRb;
    use ringbuf::traits::{Producer, Split};
    use tokio::sync::mpsc as tokio_mpsc;

    use crate::protocol::Frame;
    use crate::pubsub::subscriber::Subscriber;

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

        let (tx, mut rx) = tokio_mpsc::channel::<Frame>(16);
        let sub = Subscriber::new(tx, 42);
        pubsub.subscribe(Bytes::from_static(b"news"), sub);

        let rb = HeapRb::new(64);
        let (mut prod, mut cons) = rb.split();
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
        Shard::drain_spsc_shared(&databases, &mut [cons], &mut pubsub, &blocking, &mut pending_snap, &mut snap_state, &mut wal_w, &mut None, &mut Vec::new(), &None, 0);

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
        let (mut prod, mut cons) = rb.split();

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
        Shard::drain_spsc_shared(&databases, &mut [cons], &mut pubsub, &blocking, &mut pending_snap, &mut snap_state, &mut wal_w, &mut None, &mut Vec::new(), &None, 0);
    }

    #[test]
    fn test_extract_command_static_ping() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Bytes::from_static(b"PING")),
        ]);
        let (cmd, args) = Shard::extract_command_static(&frame).unwrap();
        assert_eq!(cmd, b"PING");
        assert!(args.is_empty());
    }

    #[test]
    fn test_extract_command_static_with_args() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Bytes::from_static(b"SET")),
            Frame::BulkString(Bytes::from_static(b"key")),
            Frame::BulkString(Bytes::from_static(b"value")),
        ]);
        let (cmd, args) = Shard::extract_command_static(&frame).unwrap();
        assert_eq!(cmd, b"SET");
        assert_eq!(args.len(), 2);
    }

    #[test]
    fn test_extract_command_static_invalid() {
        // Non-array frame
        let frame = Frame::SimpleString(Bytes::from_static(b"PING"));
        assert!(Shard::extract_command_static(&frame).is_none());

        // Empty array
        let frame = Frame::Array(vec![]);
        assert!(Shard::extract_command_static(&frame).is_none());

        // Array with non-string first element
        let frame = Frame::Array(vec![Frame::Integer(42)]);
        assert!(Shard::extract_command_static(&frame).is_none());
    }

    /// Linux-only: verify handle_uring_event processes Disconnect correctly.
    /// This test only compiles and runs on Linux CI.
    #[cfg(target_os = "linux")]
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

        // Process disconnect event -- should clean up parse buffer and inflight sends
        Shard::handle_uring_event(
            IoEvent::Disconnect { conn_id: 42 },
            &mut driver,
            &databases,
            &mut parse_bufs,
            &mut inflight_sends,
            None,
        );

        assert!(!parse_bufs.contains_key(&42), "parse buffer should be removed on disconnect");
    }

    /// Linux-only: verify handle_uring_event processes SendComplete as no-op.
    #[cfg(target_os = "linux")]
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

        // SendComplete should clean up oldest in-flight send (no panic if empty)
        Shard::handle_uring_event(
            IoEvent::SendComplete { conn_id: 1 },
            &mut driver,
            &databases,
            &mut parse_bufs,
            &mut inflight_sends,
            None,
        );
    }
}
