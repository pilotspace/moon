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
use crate::persistence::wal_v3::segment::WalWriterV3;
use crate::pubsub::PubSubRegistry;
use crate::replication::state::ReplicationState;
use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use crate::runtime::{TimerImpl, traits::RuntimeTimer};
use crate::storage::entry::CachedClock;

#[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
use crate::io::{UringConfig, UringDriver};

use super::affinity::AffinityTracker;
use super::dispatch::ShardMessage;
use super::remote_subscriber_map::RemoteSubscriberMap;
use super::shared_databases::ShardDatabases;
#[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
use super::uring_handler;
use super::{conn_accept, persistence_tick, spsc_handler, timers};

/// F1 (#438): ceiling on the graceful-shutdown connection drain. Normal
/// drains finish in single-digit milliseconds (parked reads wake instantly,
/// in-flight batches are already bounded by the C1 write timeout); the
/// ceiling exists so a wedged peer — zero-window reader with
/// `client_write_timeout_ms 0`, or a hung cross-shard leg — cannot hold up
/// process shutdown. On expiry the remaining tasks are dropped, which is
/// exactly the pre-F1 behaviour for every task.
const SHUTDOWN_DRAIN_MAX: Duration = Duration::from_secs(5);

/// c10k hardening B4: may the experimental io_uring bridge be armed?
///
/// The bridge binds a SECOND `SO_REUSEPORT` listener on the server's own port
/// and dispatches commands from its own accept path — one with no auth gate,
/// no ACL check and no client registry. On a server that has configured
/// authentication the kernel would then load-balance new connections between
/// a listener that enforces auth and one that does not. Answer `false` there
/// and stay on the pure-tokio path.
///
/// Free function (not `cfg`-gated like its only call site) so the rule is
/// unit-testable on every platform and under both runtimes.
#[allow(dead_code)] // Called only under cfg(linux + runtime-tokio); tests use it everywhere.
pub(crate) fn uring_bridge_allowed(config: &crate::config::ServerConfig) -> bool {
    config.requirepass.is_none() && config.aclfile.is_none()
}

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
        consumers: Vec<HeapCons<ShardMessage>>,
        producers: Vec<HeapProd<ShardMessage>>,
        shutdown: CancellationToken,
        aof_pool: Option<Arc<crate::persistence::aof::AofWriterPool>>,
        bind_addr: Option<String>,
        persistence_dir: Option<String>,
        snapshot_trigger_rx: channel::WatchReceiver<u64>,
        snapshot_trigger_tx: channel::WatchSender<u64>,
        repl_state_ext: Option<Arc<parking_lot::RwLock<ReplicationState>>>,
        cluster_state: Option<std::sync::Arc<parking_lot::RwLock<crate::cluster::ClusterState>>>,
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
        slice_init: crate::shard::slice::ShardSliceInit,
    ) {
        let _shard_id = self.id;

        // C1: Initialize thread-local ShardSlice before any command handling.
        // MUST be called before the accept/drain loop — assert_initialized panics
        // on the first accept if this is skipped.
        crate::shard::slice::init_shard(crate::shard::slice::ShardSlice::new(slice_init));
        crate::shard::slice::assert_initialized(self.id);

        // Publish disk-offload status for INFO moonstore (set once per shard, idempotent).
        crate::vector::metrics::MOONSTORE_DISK_OFFLOAD_ENABLED.store(
            server_config.disk_offload_enabled(),
            std::sync::atomic::Ordering::Relaxed,
        );

        // io_uring under the tokio runtime is an EXPERIMENTAL bridge (io_uring CQEs
        // relayed into tokio via an eventfd). It is broken under sustained load: the
        // driver floods `Unknown io_uring event type: 0` and then drops connections
        // (BrokenPipe), taking multishard + disk-offload down with it. tokio is the
        // PORTABILITY runtime — production io_uring lives in monoio (a separate path,
        // unaffected by this gate). So default tokio to pure-tokio (epoll) I/O, which
        // is stable under load. Opt back into the bridge with `MOON_URING=1` (for
        // benchmarking / fixing it). `MOON_NO_URING` still force-disables and remains
        // the CI default. Guarded by tests/multishard_serve_smoke + crash_recovery_*.
        #[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
        let mut uring_state: Option<UringDriver> = {
            if std::env::var("MOON_NO_URING").is_ok() || std::env::var("MOON_URING").is_err() {
                info!(
                    "Shard {} io_uring disabled (tokio default; set MOON_URING=1 to opt in)",
                    self.id
                );
                None
            } else if !uring_bridge_allowed(&server_config) {
                // c10k hardening B4. The bridge's own accept path
                // (`uring_handler`) dispatches commands directly — it has no
                // auth gate, no ACL check and no client registry. On a server
                // that HAS configured authentication it therefore binds a
                // second SO_REUSEPORT socket on the very same port that serves
                // every command unauthenticated: the kernel load-balances new
                // connections between the two listeners, so roughly half of
                // them skip auth entirely. The documented limitation named
                // maxclients and CLIENT LIST/KILL, not this.
                //
                // Refusing to arm is the fail-closed answer and leaves the
                // shard on the stable pure-tokio path (which does enforce
                // auth), rather than killing a server over an experimental
                // opt-in.
                tracing::error!(
                    "Shard {} REFUSING io_uring bridge: MOON_URING=1 with requirepass/aclfile \
                     configured would serve unauthenticated commands on the same port. \
                     Falling back to tokio I/O; unset MOON_URING or remove auth to use it.",
                    self.id
                );
                None
            } else {
                match UringDriver::new(UringConfig {
                    sqpoll_idle_ms: server_config.uring_sqpoll_ms,
                    ..UringConfig::default()
                }) {
                    Ok(mut d) => match d.init() {
                        Ok(()) => {
                            info!("Shard {} started (io_uring mode)", self.id);
                            // c10k T3: known limitation, stated loudly — bridge
                            // connections bypass maxclients AND the client
                            // registry (CLIENT LIST/KILL blind); capacity is
                            // bounded only by the driver's FdTable.
                            tracing::warn!(
                                "Shard {} io_uring bridge: connections accepted here bypass \
                                 maxclients and CLIENT LIST/KILL (experimental path; see \
                                 .planning/rfcs/c1m-connection-plane.md)",
                                self.id
                            );
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
            std::collections::VecDeque<uring_handler::InFlightSend>,
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
                    let mut accept_backoff = crate::server::accept_backoff::AcceptBackoff::new();
                    loop {
                        match listener.accept().await {
                            Ok((stream, _addr)) => {
                                accept_backoff.reset();
                                let std_stream = {
                                    use std::os::unix::io::{FromRawFd, IntoRawFd};
                                    let fd = stream.into_raw_fd();
                                    // SAFETY: fd is a valid socket from monoio TcpStream::into_raw_fd(),
                                    // which relinquished ownership. We take sole ownership here.
                                    unsafe { std::net::TcpStream::from_raw_fd(fd) }
                                };
                                // send_async (not the blocking send): the accept
                                // task shares this shard's single monoio thread
                                // with the event-loop consumer, so a blocking
                                // send on a full channel would stall the loop
                                // that drains it -> shard deadlock. Awaiting
                                // yields cooperatively and keeps backpressure
                                // (no dropped connections).
                                if tx.send_async(std_stream).await.is_err() {
                                    break; // receiver dropped, shard shutting down
                                }
                            }
                            Err(e) => {
                                // R-4: capped backoff + rate-limited log so an
                                // fd-exhaustion storm can't hot-spin this shard.
                                let ctx = format!("Shard {shard_id_copy}: per-shard accept error");
                                accept_backoff.record_error(&ctx, &e).await;
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
        let tracking_rc = crate::tracking::global_table();
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

        // Per-shard WAL v3 writer — the only WAL (pre-1.0 format freeze dropped
        // WAL v2). Created whenever persistence is enabled (`--appendonly yes`),
        // rooted at:
        //   - `<disk_offload_dir>/shard-N/wal-v3` when disk-offload is enabled
        //     (matches the page-cache/manifest/control-file layout), or
        //   - `<persistence_dir>/shard-N/wal-v3` otherwise — the old WAL v2
        //     niche, now served by the v3 segment format so WAL durability/CDC
        //     parity is preserved when disk-offload is off.
        let appendonly_enabled = runtime_config.read().appendonly != "no";
        // `--wal-kv-log`: whether SPSC-executed KV writes are also logged to
        // the per-shard WAL. Resolved per drain cycle (Auto is dynamic on the
        // CDC registry); see wal_append_and_fanout for the rationale.
        let wal_kv_log_mode = server_config.wal_kv_log_mode();
        if wal_kv_log_mode == crate::config::WalKvLogMode::Off && !appendonly_enabled {
            tracing::warn!(
                shard_id,
                "--wal-kv-log off with --appendonly no: KV writes have NO durability log"
            );
        }

        // Disk-offload base directory (None when disk-offload is disabled).
        let disk_offload_base: Option<std::path::PathBuf> = server_config
            .disk_offload_enabled()
            .then(|| server_config.effective_disk_offload_dir());

        let wal_shard_dir: Option<std::path::PathBuf> = if !appendonly_enabled {
            info!("Shard {}: WAL skipped (appendonly=no)", shard_id);
            None
        } else if server_config.disk_offload_enabled() {
            Some(
                server_config
                    .effective_disk_offload_dir()
                    .join(format!("shard-{}", shard_id)),
            )
        } else {
            persistence_dir
                .as_ref()
                .map(|dir| std::path::Path::new(dir).join(format!("shard-{}", shard_id)))
        };
        let mut wal_writer: Option<WalWriterV3> = wal_shard_dir.and_then(|shard_dir| {
            let wal_dir = shard_dir.join("wal-v3");
            match WalWriterV3::new(shard_id, &wal_dir, server_config.wal_segment_size_bytes()) {
                Ok(w) => {
                    info!(
                        "Shard {}: WAL writer initialized (segment_size={})",
                        shard_id,
                        server_config.wal_segment_size_bytes()
                    );
                    Some(w)
                }
                Err(e) => {
                    tracing::warn!("Shard {}: WAL init failed: {}", shard_id, e);
                    None
                }
            }
        });

        // Per-shard WAL append channel for local writes.
        // Connection handlers send serialized write commands here; we drain on the 1ms tick.
        let (wal_append_tx, wal_append_rx) = channel::mpsc_bounded::<(
            crate::persistence::wal_v3::record::WalRecordType,
            bytes::Bytes,
        )>(4096);
        // INVARIANT: gate the sender wiring on `wal_writer.is_some()` — NOT on
        // `appendonly_enabled || disk_offload_enabled()` — because that OR is
        // broader than the condition that actually produced a writer above
        // (`wal_shard_dir` requires `appendonly_enabled`; disk-offload alone,
        // with `--appendonly no`, yields `wal_shard_dir == None` and hence
        // `wal_writer == None`). Wiring a live sender while `wal_writer` is
        // `None` would let `wal_append_rx.try_recv()` (the 1ms-tick drain,
        // see `if let Some(ref mut wal) = wal_writer` below) silently discard
        // every record instead of writing it — the channel accepts sends but
        // has no writer to drain into. Leaving both fields `None` here
        // instead degrades correctly: `ShardDatabases::wal_append` /
        // `try_wal_append_required` and `mq_exec::wal_append_on_slice` all
        // already treat a `None` sender as "persistence disabled, no
        // durability requirement" (see their doc comments), matching reality.
        if wal_writer.is_some() {
            // `ShardSlice::wal_append_tx` (used by the owner-shard MQ.* command
            // path in `mq_exec.rs::wal_append_on_slice`) is a SEPARATE field
            // from `ShardDatabases::wal_append_txs` (used by
            // `ShardDatabases::wal_append`, e.g. TEMPORAL.INVALIDATE) — both
            // must carry a live sender for the SAME channel, or MQ.CREATE /
            // MQ.ACK silently never reach the WAL at all (task #42: this was
            // the field's only assignment site missing; `ShardSliceInit`
            // always constructs it as `None` and nothing else ever set it).
            crate::shard::slice::with_shard(|s| {
                s.wal_append_tx = Some(wal_append_tx.clone());
            });
            shard_databases.set_wal_append_tx(shard_id, wal_append_tx);
        }

        // Per-shard PageCache (None when disk-offload is disabled).
        // Manages 4KB + 64KB page frames with clock-sweep eviction.
        let page_cache: Option<PageCache> = if server_config.disk_offload_enabled() {
            use crate::persistence::page_cache::{
                pagecache_frame_counts, per_shard_pagecache_budget,
            };
            // `pagecache_size_bytes` (explicit --pagecache-size, else 25% of
            // maxmemory) is a WHOLE-INSTANCE intent, but each shard builds its
            // own PageCache. Sizing every shard to the whole budget over-committed
            // by num_shards× — the multishard "zombie eating RAM". Divide across
            // shards so total pre-allocation is bounded by the budget. Buffers are
            // also lazy now (grown on first use), so this is a ceiling, not RSS.
            let whole_budget =
                server_config.pagecache_size_bytes(server_config.maxmemory.unwrap_or(0) as u64);
            let budget = per_shard_pagecache_budget(whole_budget, num_shards);
            let (num_4k, num_64k) = pagecache_frame_counts(budget);
            // Design-for-failure: an oversized explicit --pagecache-size lets the
            // page cache crowd out the keyspace under pressure. Warn once.
            if shard_id == 0
                && let Some(maxmem) = server_config.maxmemory
                && maxmem > 0
                && whole_budget.saturating_mul(2) > maxmem as u64
            {
                tracing::warn!(
                    "PageCache budget {} B is >50% of maxmemory {} B — under cache \
                     pressure it competes with the keyspace; lower --pagecache-size",
                    whole_budget,
                    maxmem
                );
            }
            info!(
                "Shard {}: PageCache initialized ({} x 4KB + {} x 64KB frames, \
                 per-shard budget={} B of whole {} B across {} shard(s), lazy buffers)",
                shard_id, num_4k, num_64k, budget, whole_budget, num_shards
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

        // P6: Track time of last completed checkpoint for the ceiling-trigger
        // lag guard. Initialised to process start so the first tick is not
        // suppressed by a stale timestamp.
        let mut last_checkpoint_completed_at = std::time::Instant::now();

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
        // Task #59: manifest commit fsyncs measured up to 1.0s each on this
        // event-loop thread under spill flood (every connection on the shard
        // stalls behind them). Move the file-I/O half onto the per-shard
        // manifest-sync thread: durable commits keep their blocking ack,
        // spill-completion commits become deferred sends.
        if let Some(ref mut m) = shard_manifest {
            m.enable_deferred_sync(shard_id);
        }
        // Task #55: background reclaim of crash-orphaned heap files that
        // recovery only CLASSIFIED (see `Shard::restore_from_persistence` /
        // `persistence::recovery::recover_shard_v3_pitr`). Classification is
        // cheap and already ran synchronously before this event loop existed
        // (metadata-only, observed the exact recovered manifest state); the
        // slow part — `remove_file` I/O, ~40s/shard at ~59K files in the G2
        // production bench — runs here, off a plain `std::thread` fully
        // decoupled from the event loop, so it never delays this shard's
        // first accepted connection and never contends with the shard's own
        // spill/cold-index state (crash orphans are by definition NOT
        // referenced by the manifest or any in-memory index, so deleting
        // them needs no `with_shard`/manifest-commit synchronization).
        //
        // No epoch-fence race is possible here: the file-id namespace is
        // monotonic and the snapshot of "which on-disk files are orphaned"
        // was taken before this shard served a single command, so nothing
        // this shard writes afterward can retroactively register one of
        // these exact paths — see `classify_orphan_heap_files`'s doc.
        {
            let pending_heap_orphans = std::mem::take(&mut self.pending_heap_orphans);
            if !pending_heap_orphans.is_empty() {
                let n = pending_heap_orphans.len();
                info!(
                    "Shard {}: starting background reclaim of {} crash-orphaned heap file(s)",
                    shard_id, n
                );
                std::thread::Builder::new()
                    .name(format!("moon-heap-orphan-sweep-{shard_id}"))
                    .spawn(move || {
                        // O5: spawned from the pinned shard thread — escape
                        // the inherited single-core mask (this sweep can run
                        // ~40s of I/O; on the shard's own core it would
                        // contend with command processing the whole time).
                        crate::shard::numa::pin_current_aux_thread(&format!(
                            "moon-heap-orphan-sweep-{shard_id}"
                        ));
                        for path in &pending_heap_orphans {
                            crate::storage::tiered::kv_spill::remove_orphan_heap_file(path);
                        }
                        info!(
                            "Shard {}: background reclaim of {} crash-orphaned heap file(s) complete",
                            shard_id, n
                        );
                    })
                    .ok();
            }
        }

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
        // Per-shard spill directory for the write-path eviction (handler_monoio).
        // MUST match the reader's `cold_shard_dir` (main.rs / shard::mod) and the
        // persistence-tick cascade, which both use `<offload>/shard-{id}`. Using the
        // bare base here wrote cold files to `<offload>/data` while reads looked in
        // `<offload>/shard-{id}/data`, so spilled values were never read back.
        let disk_offload_dir: Option<std::path::PathBuf> = disk_offload_base
            .clone()
            .map(|base| base.join(format!("shard-{}", shard_id)));

        // B-2: resume the spill file_id counter ABOVE every recovered
        // `heap-*.mpf`. Without this the counter restarts at 1 each boot and
        // post-restart re-eviction overwrites cold files the rebuilt cold_index
        // still points at, silently corrupting post-crash cold read-through.
        // Fresh server / disk-offload off → seed 1 (unchanged from before).
        let spill_seed =
            crate::storage::eviction::next_spill_file_id_seed(disk_offload_dir.as_deref());
        spill_file_id.set(spill_seed);
        let mut next_file_id: u64 = spill_seed;
        if spill_seed > 1 {
            info!(
                "Shard {}: spill file_id counter seeded at {} from recovered cold files",
                shard_id, spill_seed
            );
        }

        // Per-shard warm-segment mmap budget enforcer.
        // Owned exclusively by this event-loop task; no locking needed.
        // A5: the flag is an instance-total cap; each shard enforces its share.
        let mut warm_mmap_budget = crate::vector::persistence::mmap_budget::MmapBudget::new(
            server_config.vec_warm_mmap_budget_bytes_per_shard(),
        );
        // Tokio path doesn't take these into the spawn signatures; suppress warnings.
        let (_, _, _) = (&spill_sender, &spill_file_id, &disk_offload_dir);

        // Per-shard replication backlog (lazy: allocated on first REPLCONF or
        // RegisterReplica). Shared with PSYNC handlers via Arc<Mutex<Option<...>>>
        // on ReplicationState. We hold a clone of this shard's slot so the write
        // path doesn't need to traverse ReplicationState's outer RwLock per write.
        let repl_backlog: crate::replication::backlog::SharedBacklog = match repl_state_ext.as_ref()
        {
            Some(rs) => rs
                .read()
                .per_shard_backlogs
                .get(self.id)
                .cloned()
                .unwrap_or_else(|| std::sync::Arc::new(parking_lot::Mutex::new(None))),
            None => std::sync::Arc::new(parking_lot::Mutex::new(None)),
        };
        let mut replica_txs: Vec<crate::shard::dispatch::ReplicaFanout> = Vec::new();
        let repl_state: Option<Arc<parking_lot::RwLock<ReplicationState>>> = repl_state_ext;
        // QW3 (2026-06 review): lock-free offset handle cloned ONCE at shard
        // startup. The SPSC drain's per-write offset advance goes through this
        // handle, so the surrounding RwLock is never read-locked per write.
        let repl_offsets: Option<crate::replication::state::OffsetHandle> =
            repl_state.as_ref().map(|rs| rs.read().offset_handle());
        // #71b: lock-free replica-role mirror cloned ONCE at shard startup.
        // A replica must NOT run its own active-expiry deletion sweep — it
        // waits for the master's authoritative DEL (streamed via
        // `record_reason_del`) so both sides remove a key at the same logical
        // point in the stream. `run_active_expiry` reads this to skip its
        // sweeps while attached to a master (logical expiry on reads still
        // applies). Kept in sync by `ReplicationState::set_role`.
        let is_replica_mirror: Option<std::sync::Arc<std::sync::atomic::AtomicBool>> = repl_state
            .as_ref()
            .map(|rs| rs.read().is_replica_mirror.clone());

        // moon#508: a script whose keys all live on THIS shard is now routed
        // here over the SPSC mesh, so the shard must be able to run one even
        // when no connection ever landed on it. Shares `lua_rc` — the same slot
        // `conn_accept` lazily fills — so a shard still builds exactly one VM
        // whichever path reaches it first.
        let shard_lua_rt = crate::scripting::ShardLuaRuntime::new(
            lua_rc.clone(),
            crate::scripting::bridge::LuaEvictionCtx::new(
                shard_databases.clone(),
                runtime_config.clone(),
                shard_id,
                spill_sender.clone(),
                spill_file_id.clone(),
                disk_offload_dir.clone(),
                num_shards,
                repl_state.clone(),
                aof_pool.as_ref().map(Arc::clone),
            ),
            num_shards,
        );

        // Track last seen snapshot epoch to detect watch channel triggers
        // Test-only fault injection: delay every non-zero shard's loop start so
        // integration tests can deterministically exercise the startup window
        // where the listener already answers (fastest shard up) while other
        // shards are still here. Used by tests/bgsave_startup_race.rs; never
        // set in production.
        if shard_id != 0
            && let Ok(ms) = std::env::var("MOON_TEST_SLOW_SHARD_START_MS")
            && let Ok(ms) = ms.parse::<u64>()
        {
            std::thread::sleep(std::time::Duration::from_millis(ms));
        }
        // Start the cursor at 0, NOT at the watch channel's current value: the
        // listener accepts clients as soon as the fastest shard is up, so a
        // BGSAVE (or auto-save) can broadcast epoch N while a slower shard is
        // still in this setup code. Seeding the cursor with borrow() would
        // swallow that pending trigger — this shard never snapshots, the
        // BGSAVE_SHARDS_REMAINING counter never reaches zero, and every later
        // BGSAVE reports "already in progress" forever (observed as the
        // 20s-stuck `rdb_bgsave_in_progress:1` crash-matrix flake; the race
        // reproduces deterministically with a delayed shard start). Epochs are
        // per-process and start at 0, so 0 is always a safe "nothing seen yet".
        let mut last_snapshot_epoch = 0u64;

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
        // Warm check interval adapts to segment_warm_after / engine_offload_idle_secs
        // for fast testing: default 10s, but if either threshold is < 10s (WS3: an
        // operator or test wants near-immediate idle-unload), poll at that
        // frequency instead. `0` (idle criterion disabled) does not shrink the
        // interval.
        let warm_trigger_secs = if server_config.engine_offload_idle_secs > 0 {
            server_config
                .segment_warm_after
                .min(server_config.engine_offload_idle_secs)
        } else {
            server_config.segment_warm_after
        };
        let warm_poll_ms = (warm_trigger_secs * 1000).clamp(1000, timers::WARM_CHECK_INTERVAL_MS);
        #[cfg(feature = "runtime-tokio")]
        let mut warm_check_interval = TimerImpl::interval(Duration::from_millis(warm_poll_ms));
        // Cold-tier orphan sweeper: 5-minute default (P9). Disabled when interval is 0.
        // Shared by both tokio (select! branch) and monoio (counter-based dispatch).
        let orphan_sweep_interval_secs = server_config.cold_orphan_sweep_interval_secs;
        #[cfg(feature = "runtime-tokio")]
        let mut orphan_sweep_interval = if orphan_sweep_interval_secs > 0 {
            Some(TimerImpl::interval(Duration::from_secs(
                orphan_sweep_interval_secs,
            )))
        } else {
            None
        };
        // monoio uses the raw seconds value for counter arithmetic below;
        // the `#[cfg(runtime-tokio)]` interval above is unused on monoio so
        // we suppress the variable warning via the shared binding above.
        #[cfg(not(feature = "runtime-tokio"))]
        let _ = orphan_sweep_interval_secs; // consumed in monoio counter block below

        // MA12: Disk free-space poll interval (5 seconds, shard 0 only).
        #[cfg(feature = "runtime-tokio")]
        let mut disk_monitor_interval = TimerImpl::interval(Duration::from_secs(5));

        // P4: Autovacuum daemon — per-shard background reclamation with AIMD throttle.
        let autovacuum_cfg = crate::shard::autovacuum::config_from_server(&server_config);
        let autovacuum_interval_secs = autovacuum_cfg.interval_secs.max(1);
        let mut autovacuum_daemon = crate::shard::autovacuum::AutovacuumDaemon::new(autovacuum_cfg);
        // MA5: Load maintenance schedule from persistence_dir on startup (if configured).
        if let Some(ref dir) = persistence_dir {
            let path = std::path::Path::new(dir)
                .join(format!("shard-{shard_id}-reclamation-schedule.toml"));
            match crate::shard::maintenance_schedule::MaintenanceSchedule::load_from_file(&path) {
                Ok(loaded) if !loaded.list().is_empty() => {
                    autovacuum_daemon.maintenance_schedule = loaded;
                    tracing::info!(
                        shard_id,
                        "MA5: loaded maintenance schedule from {}",
                        path.display()
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(shard_id, error = %e, "MA5: failed to load maintenance schedule");
                }
            }
        }
        #[cfg(feature = "runtime-tokio")]
        let mut autovacuum_interval =
            TimerImpl::interval(Duration::from_secs(autovacuum_interval_secs));

        // monoio: counter-based sub-timer dispatch from 1ms periodic tick.
        // Each sub-timer fires at its native interval via modular arithmetic.
        #[cfg(feature = "runtime-monoio")]
        let mut monoio_tick_counter: u64 = 0;
        // #373 phase 2: adaptive idle park. After a proven-quiet streak the
        // periodic park stretches 1ms -> IDLE_PARK_MS (10ms), stepping the
        // counter by 10 from an aligned boundary so every counter-based
        // cadence below (all multiples of 10) still fires exactly on time.
        #[cfg(feature = "runtime-monoio")]
        let mut idle_park = crate::shard::idle_park::IdleParkState::new();
        // O3: adaptive busy-poll contention governor. Only constructed when a
        // spin budget is actually configured — otherwise there is nothing to
        // gate and the 1s /proc read would be pure waste.
        #[cfg(feature = "runtime-monoio")]
        let mut spin_governor: Option<crate::shard::spin_governor::SpinGovernor> =
            (crate::runtime::epoll_spin_configured(server_config.io_busy_poll_us)
                && crate::shard::spin_governor::adaptive_enabled())
            .then(crate::shard::spin_governor::SpinGovernor::new);
        // Used by tokio select! for event-driven SPSC drain; monoio drains in periodic tick.
        let spsc_notify_local = spsc_notify;
        #[cfg(feature = "runtime-monoio")]
        let _ = &spsc_notify_local;
        // Same-shard self-message queue (shard::self_msg): register this
        // shard's drain Notify so a push from a sibling task on this thread
        // (inline PSYNC registration, replication fan-out) wakes the drain
        // arm immediately instead of waiting for the next periodic tick.
        crate::shard::self_msg::register_drain_notify(spsc_notify_local.clone());

        // tokio drains through the select! arms below and mutates the Vec
        // directly; monoio re-wraps it in Rc<RefCell<>> for the spin probe.
        #[cfg(feature = "runtime-tokio")]
        let mut consumers = consumers;

        // Busy-poll skip-notify handshake (monoio only; tokio spawns Send
        // tasks so its consumers stay a plain Vec). The driver's spin probe
        // needs shared read access to this shard's SPSC consumers, so wrap
        // them in Rc<RefCell<>>; the probe runs only while the event-loop
        // task is parked in race2, so it never observes an active borrow
        // (drain_spsc_shared's borrow_mut is not held across an await).
        #[cfg(feature = "runtime-monoio")]
        let consumers = Rc::new(RefCell::new(consumers));
        #[cfg(feature = "runtime-monoio")]
        if num_shards > 1 && crate::runtime::epoll_spin_configured(server_config.io_busy_poll_us) {
            // While this shard's driver spin-polls, remote producers elide
            // their cross-thread wake (flume send + foreign-waker relay +
            // eventfd syscall) — the probe below discovers their ringbuf
            // pushes instead and re-arms the race2 Notify from the local
            // thread. set_skip_wake carries the SeqCst Dekker fences.
            crate::runtime::channel::enable_notify_skip_wake();
            let adv_notify = spsc_notify_local.clone();
            let probe_notify = spsc_notify_local.clone();
            let probe_consumers = Rc::clone(&consumers);
            monoio::set_legacy_spin_hooks(
                Box::new(move |spinning| adv_notify.set_skip_wake(spinning)),
                Box::new(move || {
                    use ringbuf::traits::Observer as _;
                    let has_pending = probe_consumers.borrow().iter().any(|c| !c.is_empty());
                    if has_pending {
                        probe_notify.notify_local();
                    }
                    has_pending
                }),
            );
            info!(
                "Shard {}: busy-poll skip-notify hooks registered ({} shards)",
                shard_id, num_shards
            );
        }

        // Per-shard cached clock: updated once per 1ms tick.
        let cached_clock = CachedClock::new();

        // Pending FD migrations collected from SPSC drain (spawn wired in Plan 50-02).
        // F4 (#438): the fd is an OwnedFd — entries still queued when the event
        // loop exits (shutdown) drop here and CLOSE their sockets, giving the
        // client a FIN instead of a permanently stranded silent connection.
        let mut pending_migrations: Vec<(
            crate::shard::dispatch::MigrateFd,
            crate::server::conn::affinity::MigratedConnectionState,
        )> = Vec::new();

        // C3b-2 — Per-shard CDC fan-out. The registry holds zero subscribers
        // (and consumes no CPU on `fanout_tick`) until the first
        // `ShardMessage::CdcSubscribe` lands.
        let mut cdc_registry = crate::cdc::CdcSubscriberRegistry::new(shard_id as u16);
        let mut pending_cdc_subscribes: Vec<crate::shard::dispatch::CdcSubscribePayload> =
            Vec::new();

        // Per-shard VectorStore: use the SHARED instance from ShardDatabases.
        // This ensures handler_sharded FT.* commands and SPSC auto-indexing
        // (triggered by HSET) operate on the SAME VectorStore.
        //
        // The shard-owned vector_store (populated by `Shard::restore_from_persistence`,
        // BEFORE the event loop starts) is discarded here in favor of the
        // one on `ShardSlice`. Real recovery happens below, against THIS
        // (live) store: index *definitions* come from the
        // `vector-indexes.meta` sidecar; index *contents* come from the B3
        // manifest/segment/keymap durability layout when present (see
        // `crate::vector::persistence::recover_v2`), reconciled against the
        // live keyspace by a dedup rescan — never from the discarded store.
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
                crate::shard::slice::with_shard(|s| {
                    s.vector_store.set_persist_dir(vdir.clone());
                    s.text_store.set_persist_dir(vdir.clone());
                });
            }

            // Try loading saved index metadata (with compaction weights) from the vector persist dir.
            // W3-deep: load_index_metadata_with_weights returns (IndexMeta, f32) pairs so that
            // persisted COMPACTION_WEIGHT values are restored into VectorIndex on startup.
            let metas = vector_persist_dir.as_ref().and_then(|vdir| {
                match crate::vector::index_persist::load_index_metadata_with_weights(vdir) {
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

            // B3 (vector-index durability): threaded through the index
            // creation loop below, the rescan loop further down, and the
            // finalize call at the end of this block. See
            // `crate::vector::persistence::recover_v2` module docs for the
            // full recovery contract (manifest/segment/keymap load + dedup
            // rescan + deletion probe + orphan sweep).
            let mut recovery_state = crate::vector::persistence::recover_v2::RecoveryState::new();

            if let (Some(metas), Some(vdir)) = (&metas, &vector_persist_dir) {
                crate::shard::slice::with_shard(|s| {
                    info!(
                        "Shard {}: restoring {} vector index(es) from sidecar",
                        shard_id,
                        metas.len()
                    );
                    for (meta, weight) in metas {
                        recovery_state.create_index(&mut s.vector_store, vdir, meta);
                        if *weight != 1.0 {
                            if let Some(idx) = s.vector_store.get_index_mut(&meta.name) {
                                idx.set_compaction_weight(*weight);
                            }
                        }
                    }
                });
            }

            // Reattach WARM-tier segments Stack A's v3 recovery discovered
            // from the manifest (`Shard::restore_from_persistence`, staged on
            // `self.recovered_warm_segments` because that pass populates a
            // throwaway store discarded above). Without this, WARM's RSS win
            // evaporated on every restart: the segment was still tracked by
            // Stack B's manifest (a `disk_segment_id` never GC'd -- see the
            // `persist_hook_after_install` call added to
            // `try_warm_transitions_idle`), so `RecoveryState::finish` below
            // reloaded it as a fully-materialized HOT/immutable segment
            // instead of a WARM one.
            //
            // Ordering (PR review round 2, commit 4): this MUST run right
            // here — after all `create_index` calls above (ownership
            // decisions need every sidecar index to already exist), but
            // BEFORE the keyspace dedup rescan below. Running it after
            // `RecoveryState::finish()` (an earlier revision did) let the
            // rescan see every WARM key as unknown — `key_hash_to_key`/
            // `key_hash_to_global_id` are the only things `reconcile_key`
            // consults, and `load_segments_and_keymap` never populates them
            // for WARM keys (see `recover_v2`'s `segment_resident` gate) —
            // forcing a full re-encode into the mutable segment for every
            // WARM doc on every normal restart; the duplication check in
            // `register_warm_segments` then saw those just-re-indexed keys
            // as "already covered" and retired (deleted) the WARM segment.
            // See `VectorStore::register_warm_segments`'s own docs for the
            // full ordering rationale and the keymap-population fix.
            let recovered_warm_segments = std::mem::take(&mut self.recovered_warm_segments);
            if !recovered_warm_segments.is_empty() {
                let n = recovered_warm_segments.len();
                crate::shard::slice::with_shard(|s| {
                    s.vector_store
                        .register_warm_segments(recovered_warm_segments);
                });
                info!(
                    "Shard {}: reattached {} WARM vector segment(s) after restart",
                    shard_id, n
                );
            }

            // Phase 1.5: snapshot the deletion-probe baseline now that both
            // Stack B's HOT/immutable load (`create_index`, above) AND WARM
            // reattachment (just above) have settled — see
            // `RecoveryState::snapshot_recovered_baseline`'s docs for why
            // this can't happen any earlier without silently excluding
            // every WARM key from `finish()`'s deletion probe. Must run
            // before the rescan loop below (phase 2).
            crate::shard::slice::with_shard(|s| {
                recovery_state.snapshot_recovered_baseline(&s.vector_store);
            });

            // Restore text indexes from sidecar metadata.
            #[cfg(feature = "text-index")]
            if let Some(ref text_metas) = text_metas {
                crate::shard::slice::with_shard(|s| {
                    info!(
                        "Shard {}: restoring {} text index(es) from sidecar",
                        shard_id,
                        text_metas.len()
                    );
                    for meta in text_metas {
                        let mut text_index = crate::text::store::TextIndex::new(
                            meta.name.clone(),
                            meta.key_prefixes.clone(),
                            meta.text_fields.clone(),
                            meta.bm25_config,
                        );
                        // WS5a: carry the persisted db_index forward so a
                        // restart doesn't silently re-home a restored text
                        // index to db 0.
                        text_index.db_index = meta.db_index;
                        if let Err(e) = s.text_store.create_index(meta.name.clone(), text_index) {
                            tracing::warn!(
                                "Shard {}: failed to restore text index '{}': {}",
                                shard_id,
                                String::from_utf8_lossy(&meta.name),
                                e
                            );
                        }
                    }
                });

                // Kernel M4 (task #50): seed each restored text index's term
                // dictionaries (and, where the sidecar validates cleanly,
                // FST maps) from the `.tfst` combined sidecar BEFORE the
                // keyspace rescan below runs any `index_document` calls.
                // This MUST happen in this order -- see
                // `TextStore::load_term_fst_sidecars`'s doc comment for why
                // seeding after the rescan (or not at all) is exactly the
                // stale-id-space corruption this closes.
                crate::shard::slice::with_shard(|s| {
                    s.text_store.load_term_fst_sidecars();
                });
            }

            // Auto-reindex existing HASH keys that match vector or text index prefixes.
            let has_indexes = metas.is_some() || text_metas.is_some();
            if has_indexes {
                let db_count = shard_databases.db_count();
                let mut reindexed = 0usize;
                for db_idx in 0..db_count {
                    let collect_matching = |db: &crate::storage::Database| -> Vec<(Vec<u8>, Vec<crate::protocol::Frame>)> {
                        let mut matching: Vec<(Vec<u8>, Vec<crate::protocol::Frame>)> = Vec::new();
                        for (key, entry) in db.data().iter() {
                            let key_bytes = key.as_bytes();
                            let matches_vector = metas.as_ref().is_some_and(|ms| {
                                ms.iter().any(|(m, _w)| {
                                    m.key_prefixes.iter().any(|p| key_bytes.starts_with(p))
                                })
                            });
                            let matches_text = text_metas.as_ref().is_some_and(|ms| {
                                ms.iter().any(|m| {
                                    m.key_prefixes.iter().any(|p| key_bytes.starts_with(p))
                                })
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
                        matching
                    };
                    let matching =
                        { crate::shard::slice::with_shard_db(db_idx, |db| collect_matching(db)) };

                    if !matching.is_empty() {
                        crate::shard::slice::with_shard(|s| {
                            for (key, args) in &matching {
                                // B3 dedup rescan: verifies each matching
                                // key against any recovered durable state
                                // (manifest/segment/keymap) before deciding
                                // whether to fully re-encode. Indexes with
                                // no durable state (fresh/no manifest) fall
                                // through to the same full-rescan behavior
                                // this replaced. See `recover_v2` docs.
                                recovery_state.reconcile_key(
                                    &mut s.vector_store,
                                    &mut s.text_store,
                                    key,
                                    args,
                                    db_idx as u8,
                                );
                                reindexed += 1;
                            }
                        });
                    }
                }
                if reindexed > 0 {
                    info!(
                        "Shard {}: auto-reindexed {} HASH key(s) into restored vector/text indexes",
                        shard_id, reindexed
                    );
                }
            }

            // B3 finalize: deletion probe (keymap keys no longer present
            // anywhere in the keyspace) + orphan sweep (segment/staging/
            // keymap files not referenced by the loaded manifest, and
            // unknown `idx-*` dirs with no matching sidecar index) +
            // per-index acceptance-signal log line. No-op (does nothing,
            // logs nothing) when no index had durable state to recover.
            if let Some(ref vdir) = vector_persist_dir {
                crate::shard::slice::with_shard(|s| {
                    recovery_state.finish(&mut s.vector_store, vdir);
                });
            }
        }

        // NOTE: the old `pending_wakers` relay (register-waker, event-loop
        // sweeps after SPSC drain) was deleted in the c10k wave — it had zero
        // registrants since M2 made the cross-shard reply path await its
        // oneshot directly (cross-thread wake via monoio's `sync` feature,
        // proven by tests/spsc_wake_floor_red.rs::swf0). For cross-thread
        // signalling prefer flume oneshots, not a waker relay.

        // R-4: backoff for the tokio per-shard SO_REUSEPORT accept branch so an
        // fd-exhaustion storm can't hot-spin this shard's select loop.
        #[cfg(feature = "runtime-tokio")]
        let mut per_shard_accept_backoff = crate::server::accept_backoff::AcceptBackoff::new();

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
                        // Borrow the driver's reusable event buffer: zero
                        // allocations per CQE batch in this drain loop.
                        let mut events = driver.take_event_scratch();
                        loop {
                            let _ = driver.submit_and_wait_nonblocking();
                            events.clear();
                            driver.drain_completions_into(&mut events);
                            if events.is_empty() {
                                break;
                            }
                            for event in events.drain(..) {
                                uring_handler::handle_uring_event(
                                    event, driver, &shard_databases, shard_id, &mut uring_parse_bufs,
                                    &mut inflight_sends, uring_listener_fd, &cached_clock,
                                );
                            }
                        }
                        driver.return_event_scratch(events);
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
                            per_shard_accept_backoff.reset();
                            conn_accept::spawn_tokio_connection(
                                tcp_stream, false, &tls_config,
                                &shard_databases, &dispatch_tx, &pubsub_arc, &blocking_rc,
                                &shutdown, &aof_pool, &tracking_rc, &lua_rc, &script_cache_rc,
                                &acl_table, &runtime_config, &server_config, &all_notifiers,
                                &snapshot_trigger_tx, &repl_state, &cluster_state,
                                &cached_clock, &remote_sub_map_arc, &all_pubsub_registries,
                                &all_remote_sub_maps, &affinity_tracker,
                                shard_id, num_shards, config_port,
                                &spill_sender, &spill_file_id, &disk_offload_dir,
                            );
                        }
                        Err(e) => {
                            let ctx = format!("Shard {shard_id}: per-shard accept error");
                            per_shard_accept_backoff.record_error(&ctx, &e).await;
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
                                                // QW1: nodelay before handing the fd to io_uring.
                                                let _ = crate::server::socket_opts::apply_client_socket_opts(&std_stream);
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
                                &shutdown, &aof_pool, &tracking_rc, &lua_rc, &script_cache_rc,
                                &acl_table, &runtime_config, &server_config, &all_notifiers,
                                &snapshot_trigger_tx, &repl_state, &cluster_state,
                                &cached_clock, &remote_sub_map_arc, &all_pubsub_registries,
                                &all_remote_sub_maps,
                                &affinity_tracker,
                                shard_id, num_shards, config_port,
                                &spill_sender, &spill_file_id, &disk_offload_dir,
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
                    crate::admin::metrics_setup::bump_spsc_notify_wake();
                    let mut pending_snapshot = None;
                    // No outer with_shard wrapper — each arm in drain_spsc_shared
                    // takes its own flat borrow, eliminating the re-entrancy BorrowMutError
                    // that occurred when arms called with_shard inside an enclosing borrow.
                    let hit_cap = spsc_handler::drain_spsc_shared(
                        &shard_databases, &mut consumers, &pubsub_arc,
                        &blocking_rc, &mut pending_snapshot, &mut snapshot_state,
                        &mut wal_writer, &repl_backlog, &mut replica_txs,
                        &repl_offsets, shard_id, &script_cache_rc, Some(&shard_lua_rt), &cached_clock,
                        &mut pending_migrations,
                        &mut pending_cdc_subscribes,
                        &mut shard_manifest,
                        server_config.mvcc_committed_prune_margin,
                        server_config.graph_merge_max_segments,
                        server_config.graph_dead_edge_trigger,
                        &mut autovacuum_daemon,
                        aof_pool.as_ref(),  // FIX-W1-2
                        match wal_kv_log_mode {
                            crate::config::WalKvLogMode::On => true,
                            crate::config::WalKvLogMode::Off => false,
                            crate::config::WalKvLogMode::Auto => {
                                !appendonly_enabled || !cdc_registry.is_empty()
                            }
                        },
                        &runtime_config,
                        spill_sender.as_ref(),
                        &spill_file_id,
                        disk_offload_dir.as_deref(),
                    );
                    if hit_cap {
                        // M3: capped drain may have left a tail — re-arm immediately
                        // instead of stranding it until the next periodic tick.
                        spsc_notify_local.notify_one();
                        crate::admin::metrics_setup::bump_spsc_drain_renotify();
                    }
                    // Deliver keyspace events produced ON THIS SHARD THREAD:
                    // a write routed here from another shard's connection
                    // executes here, and TTL expiry / eviction have no
                    // connection at all. The connection-side drain sees
                    // neither. Mirrored in the monoio arm below — this block
                    // is `#[cfg(runtime-tokio)]`, and putting it in only one
                    // of the two arms is invisible to the other runtime's CI.
                    crate::notify_fanout::flush_from_shard(
                        shard_id,
                        &pubsub_arc,
                        &remote_sub_map_arc,
                        &dispatch_tx,
                        &all_notifiers,
                    );
                    // MA5: persist maintenance schedule when modified by RECLAMATION SCHEDULE.
                    if autovacuum_daemon.maintenance_schedule.is_dirty() {
                        if let Some(ref dir) = persistence_dir {
                            let path = std::path::Path::new(dir)
                                .join(format!("shard-{shard_id}-reclamation-schedule.toml"));
                            if let Err(e) = autovacuum_daemon.maintenance_schedule.save_to_file(&path) {
                                tracing::warn!(shard_id, error = %e, "MA5: failed to persist maintenance schedule");
                            } else {
                                autovacuum_daemon.maintenance_schedule.mark_clean();
                            }
                        }
                    }
                    if !pending_cdc_subscribes.is_empty() {
                        let wal_dir = wal_writer.as_ref().map(|w| w.wal_dir());
                        cdc_registry.register_pending(
                            pending_cdc_subscribes.drain(..), wal_dir,
                        );
                    }
                    persistence_tick::handle_pending_snapshot(
                        pending_snapshot, &mut snapshot_state, &mut snapshot_reply_tx,
                        &shard_databases, disk_offload_base.as_deref(), shard_id,
                        wal_writer.as_ref().map(|w| w.current_lsn().saturating_sub(1)).unwrap_or(0),
                    );
                    for (fd, state) in pending_migrations.drain(..) {
                        #[cfg(unix)]
                        {
                            tracing::info!(
                                "Shard {}: accepting migrated connection (fd={}, client_id={}, from={})",
                                shard_id, std::os::fd::AsRawFd::as_raw_fd(&fd), state.client_id, state.peer_addr
                            );
                            #[cfg(feature = "runtime-tokio")]
                            conn_accept::spawn_migrated_tokio_connection(
                                fd, state,
                                &shard_databases, &dispatch_tx, &pubsub_arc, &blocking_rc,
                                &shutdown, &aof_pool, &tracking_rc, &lua_rc, &script_cache_rc,
                                &acl_table, &runtime_config, &server_config, &all_notifiers,
                                &snapshot_trigger_tx, &repl_state, &cluster_state,
                                &cached_clock, &remote_sub_map_arc, &all_pubsub_registries,
                                &all_remote_sub_maps, &affinity_tracker,
                                shard_id, num_shards, config_port,
                                &spill_sender, &spill_file_id, &disk_offload_dir,
                            );
                            #[cfg(feature = "runtime-monoio")]
                            conn_accept::spawn_migrated_monoio_connection(
                                fd, state,
                                &shard_databases, &dispatch_tx, &pubsub_arc, &blocking_rc,
                                &shutdown, &aof_pool, &tracking_rc, &lua_rc, &script_cache_rc,
                                &acl_table, &runtime_config, &server_config, &all_notifiers,
                                &snapshot_trigger_tx, &repl_state, &cluster_state,
                                &cached_clock, &remote_sub_map_arc, &all_pubsub_registries,
                                &all_remote_sub_maps, &affinity_tracker,
                                shard_id, num_shards, config_port,
                                &spill_sender, &spill_file_id, &disk_offload_dir,
                            );
                        }
                        #[cfg(not(unix))]
                        {
                            let _ = (fd, state);
                            tracing::debug!(
                                "Shard {}: connection migration not supported on this platform; \
                                 connection closed",
                                shard_id
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
                    // No outer with_shard — each arm takes its own flat borrow.
                    let hit_cap = spsc_handler::drain_spsc_shared(
                        &shard_databases, &mut consumers, &pubsub_arc,
                        &blocking_rc, &mut pending_snapshot, &mut snapshot_state,
                        &mut wal_writer, &repl_backlog, &mut replica_txs,
                        &repl_offsets, shard_id, &script_cache_rc, Some(&shard_lua_rt), &cached_clock,
                        &mut pending_migrations,
                        &mut pending_cdc_subscribes,
                        &mut shard_manifest,
                        server_config.mvcc_committed_prune_margin,
                        server_config.graph_merge_max_segments,
                        server_config.graph_dead_edge_trigger,
                        &mut autovacuum_daemon,
                        aof_pool.as_ref(),  // FIX-W1-2
                        match wal_kv_log_mode {
                            crate::config::WalKvLogMode::On => true,
                            crate::config::WalKvLogMode::Off => false,
                            crate::config::WalKvLogMode::Auto => {
                                !appendonly_enabled || !cdc_registry.is_empty()
                            }
                        },
                        &runtime_config,
                        spill_sender.as_ref(),
                        &spill_file_id,
                        disk_offload_dir.as_deref(),
                    );
                    if hit_cap {
                        // M3: capped drain may have left a tail — re-arm immediately
                        // instead of stranding it until the next periodic tick.
                        spsc_notify_local.notify_one();
                        crate::admin::metrics_setup::bump_spsc_drain_renotify();
                    }
                    // Deliver keyspace events produced ON THIS SHARD THREAD:
                    // a write routed here from another shard's connection
                    // executes here, and TTL expiry / eviction have no
                    // connection at all. The connection-side drain sees
                    // neither. Mirrored in the monoio arm below — this block
                    // is `#[cfg(runtime-tokio)]`, and putting it in only one
                    // of the two arms is invisible to the other runtime's CI.
                    crate::notify_fanout::flush_from_shard(
                        shard_id,
                        &pubsub_arc,
                        &remote_sub_map_arc,
                        &dispatch_tx,
                        &all_notifiers,
                    );
                    // MA5: persist maintenance schedule when modified by RECLAMATION SCHEDULE.
                    if autovacuum_daemon.maintenance_schedule.is_dirty() {
                        if let Some(ref dir) = persistence_dir {
                            let path = std::path::Path::new(dir)
                                .join(format!("shard-{shard_id}-reclamation-schedule.toml"));
                            if let Err(e) = autovacuum_daemon.maintenance_schedule.save_to_file(&path) {
                                tracing::warn!(shard_id, error = %e, "MA5: failed to persist maintenance schedule");
                            } else {
                                autovacuum_daemon.maintenance_schedule.mark_clean();
                            }
                        }
                    }
                    if !pending_cdc_subscribes.is_empty() {
                        let wal_dir = wal_writer.as_ref().map(|w| w.wal_dir());
                        cdc_registry.register_pending(
                            pending_cdc_subscribes.drain(..), wal_dir,
                        );
                    }
                    persistence_tick::handle_pending_snapshot(
                        pending_snapshot, &mut snapshot_state, &mut snapshot_reply_tx,
                        &shard_databases, disk_offload_base.as_deref(), shard_id,
                        wal_writer.as_ref().map(|w| w.current_lsn().saturating_sub(1)).unwrap_or(0),
                    );
                    for (fd, state) in pending_migrations.drain(..) {
                        #[cfg(unix)]
                        {
                            tracing::info!(
                                "Shard {}: accepting migrated connection (fd={}, client_id={}, from={})",
                                shard_id, std::os::fd::AsRawFd::as_raw_fd(&fd), state.client_id, state.peer_addr
                            );
                            #[cfg(feature = "runtime-tokio")]
                            conn_accept::spawn_migrated_tokio_connection(
                                fd, state,
                                &shard_databases, &dispatch_tx, &pubsub_arc, &blocking_rc,
                                &shutdown, &aof_pool, &tracking_rc, &lua_rc, &script_cache_rc,
                                &acl_table, &runtime_config, &server_config, &all_notifiers,
                                &snapshot_trigger_tx, &repl_state, &cluster_state,
                                &cached_clock, &remote_sub_map_arc, &all_pubsub_registries,
                                &all_remote_sub_maps, &affinity_tracker,
                                shard_id, num_shards, config_port,
                                &spill_sender, &spill_file_id, &disk_offload_dir,
                            );
                            #[cfg(feature = "runtime-monoio")]
                            conn_accept::spawn_migrated_monoio_connection(
                                fd, state,
                                &shard_databases, &dispatch_tx, &pubsub_arc, &blocking_rc,
                                &shutdown, &aof_pool, &tracking_rc, &lua_rc, &script_cache_rc,
                                &acl_table, &runtime_config, &server_config, &all_notifiers,
                                &snapshot_trigger_tx, &repl_state, &cluster_state,
                                &cached_clock, &remote_sub_map_arc, &all_pubsub_registries,
                                &all_remote_sub_maps, &affinity_tracker,
                                shard_id, num_shards, config_port,
                                &spill_sender, &spill_file_id, &disk_offload_dir,
                            );
                        }
                        #[cfg(not(unix))]
                        {
                            let _ = (fd, state);
                            tracing::debug!(
                                "Shard {}: connection migration not supported on this platform; \
                                 connection closed",
                                shard_id
                            );
                        }
                    }

                    persistence_tick::check_auto_save_trigger(
                        &snapshot_trigger_rx, &mut last_snapshot_epoch,
                        &mut snapshot_state, &shard_databases, &persistence_dir,
                        disk_offload_base.as_deref(), shard_id,
                        wal_writer.as_ref().map(|w| w.current_lsn().saturating_sub(1)).unwrap_or(0),
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
                                // Decrement the BGSAVE fan-in counter (same as
                                // the monoio arm below — this was missing here,
                                // so tokio BGSAVE left rdb_bgsave_in_progress
                                // stuck at 1 forever). Safe for auto-save
                                // snapshots too: the counter ignores calls at 0.
                                crate::command::persistence::bgsave_shard_done(false);
                            } else {
                                persistence_tick::finalize_snapshot_success(
                                    &mut snapshot_state, &mut snapshot_reply_tx, shard_id,
                                );
                                crate::command::persistence::bgsave_shard_done(true);
                                bgsave_checkpoint_requested = true;
                            }
                        }
                    }

                    // Drain local-write WAL channel (connection handler inline writes).
                    // K1a: the channel carries the producer's REAL record type —
                    // append it as-is instead of re-wrapping everything as `Command`.
                    while let Ok((record_type, data)) = wal_append_rx.try_recv() {
                        if let Some(ref mut wal) = wal_writer {
                            wal.append(record_type, &data);
                        }
                    }

                    persistence_tick::flush_wal_v3_if_needed(&mut wal_writer);

                    // C3b-2 — Drive CDC fan-out AFTER flush_if_needed so the
                    // tail reader sees the bytes just handed to the page
                    // cache. Zero CPU when no subscribers are attached.
                    if !cdc_registry.is_empty() {
                        cdc_registry.fanout_tick(cached_clock.ms() as i64);
                    }

                    // appendfsync=always: initiate WAL durability after every
                    // SPSC drain batch. The fsync runs on the off-loop sync
                    // agent — replies are not gated on WAL v3 durability (KV
                    // acks ride the AOF), so the drain loop no longer eats a
                    // full fsync per batch (−20% RPS / ~2× tail measured,
                    // tmp/WALV3-OFFLOOP-FSYNC.md §8).
                    if server_config.appendfsync == "always" {
                        if let Some(ref mut wal) = wal_writer {
                            if let Err(e) = wal.request_sync() {
                                tracing::error!("WAL appendfsync=always failed: {}", e);
                            }
                        }
                    }

                    // Checkpoint protocol tick (disk-offload only)
                    if let (Some(ckpt_mgr), Some(page_cache_inst), Some(wal_v3), Some(manifest), Some(ctrl), Some(ctrl_path)) =
                        (&mut checkpoint_manager, &page_cache, &mut wal_writer, &mut shard_manifest, &mut control_file, &control_file_path)
                    {
                        // BGSAVE-triggered forced checkpoint (bypasses trigger conditions)
                        if bgsave_checkpoint_requested && !ckpt_mgr.is_active() {
                            let lsn = wal_v3.current_lsn();
                            let dirty = page_cache_inst.dirty_page_count();
                            ckpt_mgr.force_begin(lsn, dirty);
                            bgsave_checkpoint_requested = false;
                        }
                        persistence_tick::maybe_begin_checkpoint(ckpt_mgr, wal_v3, page_cache_inst, wal_bytes_since_checkpoint);
                        if persistence_tick::handle_checkpoint_tick(ckpt_mgr, page_cache_inst, wal_v3, manifest, ctrl, ctrl_path, server_config.manifest_tombstone_retain_epochs, server_config.manifest_tombstone_retain_secs, &mut persistence_tick::graph_checkpoint_hook(persistence_dir.as_deref(), shard_id)) {
                            wal_bytes_since_checkpoint = 0;
                            last_checkpoint_completed_at = std::time::Instant::now();
                        }
                    }

                    // Also poll io_uring in the timer tick as a fallback.
                    // The eventfd select! branch should handle most CQEs instantly,
                    // but this catches any that slip through.
                    #[cfg(target_os = "linux")]
                    if let Some(ref mut driver) = uring_state {
                        let _ = driver.submit_and_wait_nonblocking();
                        // Reuse the driver's event buffer (1ms tick path).
                        let mut events = driver.take_event_scratch();
                        driver.drain_completions_into(&mut events);
                        for event in events.drain(..) {
                            uring_handler::handle_uring_event(
                                event, driver, &shard_databases, shard_id, &mut uring_parse_bufs,
                                &mut inflight_sends, uring_listener_fd, &cached_clock,
                            );
                        }
                        driver.return_event_scratch(events);
                    }
                }
                // WAL fsync + MVCC sweep on 1-second interval
                _ = wal_sync_interval.0.tick() => {
                    timers::sync_wal_v3(&mut wal_writer);
                    // D1: idle-timeout enforcement, same policy and cadence as
                    // the monoio chore (the per-connection timeout wrapper it
                    // replaces read its config once at connection setup and
                    // never exempted replication links).
                    let _ = crate::client_registry::kill_idle_clients(
                        shard_id,
                        num_shards,
                        runtime_config.read().timeout,
                        crate::storage::entry::current_time_ms(),
                    );
                    // P3+MA1+MA2: prune committed + sweep zombies + kill old snapshots
                    //             + update RECL_MVCC_* + segment-stall.
                    crate::shard::slice::with_shard(|s| {
                        timers::run_mvcc_sweep(
                            &mut s.vector_store,
                            #[cfg(feature = "graph")]
                            &mut s.graph_store,
                            server_config.mvcc_committed_prune_margin,
                            server_config.max_unflushed_immutable_segments,
                            server_config.mvcc_old_snapshot_threshold_secs,
                        );
                    });
                    // P6: ceiling-trigger — runs at 1s cadence to avoid the
                    // read_dir syscall overhead of wal.stats() on every 1ms tick.
                    if let (Some(ckpt_mgr), Some(page_cache_inst), Some(wal_v3), Some(manifest), Some(ctrl), Some(ctrl_path)) =
                        (&mut checkpoint_manager, &page_cache, &mut wal_writer, &mut shard_manifest, &mut control_file, &control_file_path)
                    {
                        if persistence_tick::maybe_force_checkpoint_on_wal_overflow(
                            ckpt_mgr,
                            wal_v3,
                            page_cache_inst,
                            manifest,
                            ctrl,
                            ctrl_path,
                            shard_id,
                            last_checkpoint_completed_at,
                            server_config.wal_max_checkpoint_lag_ms,
                            &mut persistence_tick::graph_checkpoint_hook(
                                persistence_dir.as_deref(),
                                shard_id,
                            ),
                        ) {
                            wal_bytes_since_checkpoint = 0;
                            last_checkpoint_completed_at = std::time::Instant::now();
                        }
                    }
                }
                // Warm tier transition check (10s interval, disk-offload only)
                _ = warm_check_interval.0.tick() => {
                    // task/issue #45: `disk_offload_dir` (Some iff offload
                    // enabled) is precomputed at shard init — no per-tick
                    // format!/join.
                    if let Some(shard_dir) = disk_offload_dir.as_deref() {
                        if let Some(ref mut manifest) = shard_manifest {
                            crate::shard::slice::with_shard(|s| {
                                persistence_tick::check_warm_transitions(
                                    &s.vector_store,
                                    shard_dir,
                                    manifest,
                                    server_config.segment_warm_after,
                                    server_config.engine_offload_idle_secs,
                                    &mut next_file_id,
                                    shard_id,
                                    &mut wal_writer,
                                );
                            });
                        }
                    }
                    // Budget enforcement runs on every warm-check tick regardless of
                    // disk-offload state: warm segments can accumulate from in-memory
                    // compaction even without disk-offload enabled.
                    crate::shard::slice::with_shard(|s| {
                        persistence_tick::enforce_warm_mmap_budget(
                            &s.vector_store,
                            &mut warm_mmap_budget,
                            shard_id,
                        );
                    });
                }
                // Cold-tier orphan sweeper (P9): runs every cold_orphan_sweep_interval_secs.
                // Identifies cold index entries whose key is now in the hot DashTable
                // (hot write shadowed the spilled copy) and deletes the stale DataFile.
                _ = async {
                    if let Some(ref mut sw) = orphan_sweep_interval {
                        sw.0.tick().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                }, if orphan_sweep_interval.is_some() && server_config.disk_offload_enabled() => {
                    // task/issue #45: precomputed shard dir; the select guard
                    // already requires disk-offload, so this is always Some.
                    if let Some(shard_dir) = disk_offload_dir.as_deref() {
                        timers::run_cold_orphan_sweep(
                            &shard_databases,
                            shard_id,
                            shard_dir,
                            shard_manifest.as_mut(),
                            cached_clock.ms(),
                        );
                    }
                }
                // Expire timed-out blocked clients every 10ms
                _ = block_timeout_interval.0.tick() => {
                    timers::expire_blocked_clients(&blocking_rc);
                }
                // Cooperative active expiry + MQ triggers
                _ = expiry_interval.0.tick() => {
                    timers::run_active_expiry(
                        &shard_databases, shard_id,
                        &mut wal_writer, &repl_backlog, &mut replica_txs, &repl_offsets,
                        aof_pool.as_ref(),
                        match wal_kv_log_mode {
                            crate::config::WalKvLogMode::On => true,
                            crate::config::WalKvLogMode::Off => false,
                            crate::config::WalKvLogMode::Auto => {
                                !appendonly_enabled || !cdc_registry.is_empty()
                            }
                        },
                        is_replica_mirror
                            .as_ref()
                            .is_some_and(|m| m.load(std::sync::atomic::Ordering::Acquire)),
                    );
                    // MQ trigger check: fire debounced triggers
                    timers::fire_pending_mq_triggers(
                        &shard_databases,
                        shard_id,
                        cached_clock.ms(),
                        &pubsub_arc,
                    );
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
                        &mut wal_writer,
                        &script_cache_rc,
                        &lua_rc,
                        &spill_file_id,
                        disk_offload_dir.as_deref(),
                        &repl_backlog, &mut replica_txs, &repl_offsets,
                        aof_pool.as_ref(),
                        match wal_kv_log_mode {
                            crate::config::WalKvLogMode::On => true,
                            crate::config::WalKvLogMode::Off => false,
                            crate::config::WalKvLogMode::Auto => {
                                !appendonly_enabled || !cdc_registry.is_empty()
                            }
                        },
                    );

                    // Reap idle io_uring connections (tokio+io_uring path).
                    // Cleans up CLOSE_WAIT connections where the multishot recv
                    // ended without producing a 0-byte CQE (client FIN + MORE=0).
                    #[cfg(target_os = "linux")]
                    if let Some(ref mut driver) = uring_state {
                        let _reaped = driver.reap_idle_connections(5000);
                    }
                }
                // MA12: Disk free-space poll (5s interval, shard 0 only).
                _ = disk_monitor_interval.0.tick() => {
                    if shard_id == 0 {
                        crate::shard::disk_monitor::poll_global();
                        // Wave 3: RSS memory watchdog poll (same 5s tick).
                        crate::shard::mem_monitor::poll_global();
                    }
                }
                // P4: Autovacuum daemon tick (default 30s interval).
                _ = autovacuum_interval.0.tick() => {
                    crate::shard::slice::with_shard(|s| {
                        autovacuum_daemon.run_tick(
                            &mut s.vector_store,
                            #[cfg(feature = "graph")]
                            &mut s.graph_store,
                            shard_manifest.as_mut(),
                            wal_writer.as_mut(),
                            control_file.as_ref(),
                            server_config.max_wal_size_bytes(),
                            server_config.disk_offload_enabled(),
                            server_config.manifest_tombstone_retain_epochs,
                            server_config.manifest_tombstone_retain_secs,
                            server_config.max_unflushed_immutable_segments as usize,
                            server_config.graph_merge_max_segments,
                            server_config.graph_dead_edge_trigger,
                            false,
                        );
                    });
                }
                _ = shutdown.cancelled() => {
                    info!("Shard {} shutting down", self.id);
                    // F1 (#438): bounded connection drain BEFORE persistence
                    // teardown — the `break` below returns from `run`, and
                    // dropping the LocalSet/runtime kills every connection
                    // task still pending, truncating in-flight replies and
                    // skipping the blocking/subscriber shutdown arms. The
                    // token (already cancelled) wakes the tokio selects'
                    // shutdown arms; this loop just keeps the thread polling
                    // until they have all exited through the flush+FIN
                    // epilogue, or the deadline expires (a wedged peer must
                    // not hold up shutdown — its task is then dropped, the
                    // pre-F1 behaviour).
                    {
                        let drain_deadline = std::time::Instant::now() + SHUTDOWN_DRAIN_MAX;
                        loop {
                            let live = conn_accept::live_conn_tasks();
                            if live == 0 {
                                break;
                            }
                            if std::time::Instant::now() >= drain_deadline {
                                tracing::warn!(
                                    "Shard {shard_id}: shutdown drain timed out with {live} connection task(s) still live; dropping them"
                                );
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                        }
                    }
                    persistence_tick::drain_and_shutdown_spill(
                        &mut spill_thread,
                        &mut shard_manifest,
                        &shard_databases,
                        shard_id,
                    );
                    // Trigger final checkpoint before shutdown (design S9)
                    if let (Some(ckpt_mgr), Some(page_cache_inst), Some(wal_v3), Some(manifest), Some(ctrl), Some(ctrl_path)) =
                        (&mut checkpoint_manager, &page_cache, &mut wal_writer, &mut shard_manifest, &mut control_file, &control_file_path)
                    {
                        persistence_tick::force_checkpoint(ckpt_mgr, page_cache_inst, wal_v3, manifest, ctrl, ctrl_path, shard_id, server_config.manifest_tombstone_retain_epochs, server_config.manifest_tombstone_retain_secs, &mut persistence_tick::graph_checkpoint_hook(persistence_dir.as_deref(), shard_id));
                    }
                    // Persist graph store to disk on shutdown.
                    #[cfg(feature = "graph")]
                    if let Some(ref dir) = persistence_dir {
                        crate::shard::slice::with_shard(|s| {
                            if s.graph_store.graph_count() > 0 {
                                if let Err(e) = crate::graph::recovery::save_graph_store(
                                    &s.graph_store,
                                    std::path::Path::new(dir),
                                    shard_id,
                                ) {
                                    tracing::warn!(
                                        "Shard {shard_id}: failed to save graph store on shutdown: {e}"
                                    );
                                } else {
                                    info!("Shard {shard_id}: graph store saved to {dir}");
                                }
                            }
                        });
                    }
                    if let Some(ref mut wal) = wal_writer {
                        let _ = wal.flush_sync();
                    }
                    // Task #59: flush pending deferred manifest commits and
                    // stop the manifest-sync thread before the loop exits.
                    if let Some(ref mut m) = shard_manifest {
                        m.shutdown_deferred();
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
                        &aof_pool,
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
                    &aof_pool,
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
                    &spill_sender,
                    &spill_file_id,
                    &disk_offload_dir,
                );
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
                    // F1 (#438): bounded connection drain BEFORE persistence
                    // teardown — the `break` below returns from `run`, and
                    // dropping the monoio runtime kills every connection task
                    // still pending, truncating in-flight replies and
                    // skipping the blocking/subscriber shutdown arms (found
                    // live: 19/50 BLPOP clients lost their shutdown reply on
                    // Linux io_uring). Stage-1/2 idle-park reads are plain
                    // awaits the token cannot wake, so fire their cancellers;
                    // the woken handlers see the cancelled token and exit
                    // through the flush+FIN epilogue. Re-fired every tick to
                    // close the mid-batch re-park race. Deadline-bounded: a
                    // wedged peer must not hold up shutdown — its task is
                    // then dropped, the pre-F1 behaviour.
                    {
                        let drain_deadline = std::time::Instant::now() + SHUTDOWN_DRAIN_MAX;
                        let mut ticks = 0u32;
                        loop {
                            crate::server::conn::handler_monoio::idle_park::cancel_all_parked();
                            let live = conn_accept::live_conn_tasks();
                            if live == 0 {
                                break;
                            }
                            if std::time::Instant::now() >= drain_deadline {
                                tracing::warn!(
                                    "Shard {shard_id}: shutdown drain timed out with {live} connection task(s) still live; dropping them"
                                );
                                break;
                            }
                            // Fast ticks catch the one legal re-park race (a
                            // task that passed its post-batch shutdown check
                            // before the token cancelled, then parked after
                            // the first canceller sweep); after that no task
                            // can park again, so back off — the O(registry)
                            // canceller scan every 2 ms for the full 5 s
                            // ceiling would be a shutdown-only CPU spike at
                            // high connection counts.
                            let tick = if ticks < 5 {
                                std::time::Duration::from_millis(2)
                            } else {
                                std::time::Duration::from_millis(50)
                            };
                            ticks += 1;
                            monoio::time::sleep(tick).await;
                        }
                    }
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
                        &mut wal_writer,
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
                            server_config.manifest_tombstone_retain_epochs,
                            server_config.manifest_tombstone_retain_secs,
                            &mut persistence_tick::graph_checkpoint_hook(
                                persistence_dir.as_deref(),
                                shard_id,
                            ),
                        );
                    }
                    if let Some(ref mut wal) = wal_writer {
                        let _ = wal.flush_sync();
                    }
                    // Task #59: flush pending deferred manifest commits and
                    // stop the manifest-sync thread before the loop exits.
                    if let Some(ref mut m) = shard_manifest {
                        m.shutdown_deferred();
                    }
                    break;
                }

                // Single race await — still no monoio::select! (it leaks ~100 B per
                // re-entry; hand-rolled race2 instead, M6) and no per-iteration
                // allocation. .0.tick() bypasses the RuntimeInterval trait's
                // Box::pin() wrapper. The timer arm drives the periodic body at its
                // ~1ms cadence; the Notify arm (cross-shard producers + drain-cap
                // self-re-notify) wakes the loop the moment a message arrives.
                // monoio 0.2.4's `sync` feature carries the producer's cross-thread
                // wake (per-thread waker channel + eventfd/kqueue unpark) — proven
                // at runtime by tests/spsc_wake_floor_red.rs::swf0 on both drivers.
                // A losing notified() arm re-queues an undelivered token on drop
                // (swf_a3), so there is no lost-wake window.
                let timer_fired = if idle_park.is_idle() {
                    // #373 phase 2: stretched park. A one-shot sleep replaces
                    // the Interval while idle — the Interval is deliberately
                    // NOT polled here and is reset on every idle exit, so its
                    // accumulated missed ticks can never burst-fire chores.
                    let tick = std::pin::pin!(monoio::time::sleep(Duration::from_millis(
                        crate::shard::idle_park::IDLE_PARK_MS
                    )));
                    let notified = std::pin::pin!(spsc_notify_local.notified());
                    matches!(
                        crate::runtime::race::race2(tick, notified).await,
                        crate::runtime::race::Arm::First(_)
                    )
                } else {
                    let tick = std::pin::pin!(periodic_interval.0.tick());
                    let notified = std::pin::pin!(spsc_notify_local.notified());
                    matches!(
                        crate::runtime::race::race2(tick, notified).await,
                        crate::runtime::race::Arm::First(_)
                    )
                };
                if !timer_fired {
                    if idle_park.note_notify_wake() {
                        // Woken out of an idle park by cross-shard work:
                        // refresh the cached clock before draining (the
                        // message must never see stretched staleness) and
                        // reset the interval for the fast cadence.
                        cached_clock.update();
                        periodic_interval = TimerImpl::interval(Duration::from_millis(1));
                    }
                    crate::admin::metrics_setup::bump_spsc_notify_wake();
                }

                // Adversarial-review fix (#378): cross-shard SPSC commands do
                // not bump the per-thread command counter (the ORIGIN shard's
                // handler records them; counting again here would double-count
                // INFO stats), so the idle gate observes queue occupancy
                // directly. Checked BEFORE the drain: any pending message —
                // read or write — makes this tick non-quiet.
                let spsc_had_work = {
                    use ringbuf::traits::Observer as _;
                    consumers.borrow().iter().any(|c| !c.is_empty())
                };
                // A timer-win race tie can reach the drain with pending
                // cross-shard messages while still idle-parked (the notify
                // token re-queues, but this iteration drains first): refresh
                // the clock before draining so cross-shard commands never
                // observe stretched staleness on EITHER race outcome.
                if timer_fired && spsc_had_work && idle_park.is_idle() {
                    cached_clock.update();
                }

                // --- Every-wake body (mirrors the tokio notify arm): drain SPSC,
                //     handle drain outputs ---
                let mut pending_snapshot = None;
                // No outer with_shard — each arm takes its own flat borrow.
                let hit_cap = spsc_handler::drain_spsc_shared(
                    &shard_databases,
                    &mut consumers.borrow_mut(),
                    &pubsub_arc,
                    &blocking_rc,
                    &mut pending_snapshot,
                    &mut snapshot_state,
                    &mut wal_writer,
                    &repl_backlog,
                    &mut replica_txs,
                    &repl_offsets,
                    shard_id,
                    &script_cache_rc,
                    Some(&shard_lua_rt),
                    &cached_clock,
                    &mut pending_migrations,
                    &mut pending_cdc_subscribes,
                    &mut shard_manifest,
                    server_config.mvcc_committed_prune_margin,
                    server_config.graph_merge_max_segments,
                    server_config.graph_dead_edge_trigger,
                    &mut autovacuum_daemon,
                    aof_pool.as_ref(), // FIX-W1-2
                    match wal_kv_log_mode {
                        crate::config::WalKvLogMode::On => true,
                        crate::config::WalKvLogMode::Off => false,
                        crate::config::WalKvLogMode::Auto => {
                            !appendonly_enabled || !cdc_registry.is_empty()
                        }
                    },
                    &runtime_config,
                    spill_sender.as_ref(),
                    &spill_file_id,
                    disk_offload_dir.as_deref(),
                );
                if hit_cap {
                    // M3: the drain stopped at its per-cycle cap (or a snapshot
                    // barrier) — re-arm immediately so the tail drains on the next
                    // iteration instead of stranding until the next timer tick.
                    spsc_notify_local.notify_one();
                    crate::admin::metrics_setup::bump_spsc_drain_renotify();
                }
                if !pending_cdc_subscribes.is_empty() {
                    let wal_dir = wal_writer.as_ref().map(|w| w.wal_dir());
                    cdc_registry.register_pending(pending_cdc_subscribes.drain(..), wal_dir);
                }
                // Deliver keyspace events produced ON THIS SHARD THREAD: a
                // write routed here from another shard's connection executes
                // here, and TTL expiry / eviction have no connection at all.
                // The connection-side drain cannot see either.
                crate::notify_fanout::flush_from_shard(
                    shard_id,
                    &pubsub_arc,
                    &remote_sub_map_arc,
                    &dispatch_tx,
                    &all_notifiers,
                );
                persistence_tick::handle_pending_snapshot(
                    pending_snapshot,
                    &mut snapshot_state,
                    &mut snapshot_reply_tx,
                    &shard_databases,
                    disk_offload_base.as_deref(),
                    shard_id,
                    wal_writer
                        .as_ref()
                        .map(|w| w.current_lsn().saturating_sub(1))
                        .unwrap_or(0),
                );
                for (fd, state) in pending_migrations.drain(..) {
                    #[cfg(unix)]
                    {
                        tracing::info!(
                            "Shard {}: accepting migrated connection (fd={}, client_id={}, from={})",
                            shard_id,
                            std::os::fd::AsRawFd::as_raw_fd(&fd),
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
                            &aof_pool,
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
                            &spill_sender,
                            &spill_file_id,
                            &disk_offload_dir,
                        );
                    }
                    #[cfg(not(unix))]
                    {
                        let _ = (fd, state);
                        tracing::debug!(
                            "Shard {}: connection migration not supported on this platform; \
                             connection closed",
                            shard_id
                        );
                    }
                }

                // --- Periodic tick body (timer arm ONLY — M4: cadence work like
                //     WAL flush, sub-timers, and the cached clock must never run
                //     off-schedule on a notify wake) ---
                if !timer_fired {
                    continue;
                }
                // Step by the period of the park that just elapsed (1 fast,
                // 10 idle) so the counter keeps counting nominal milliseconds.
                monoio_tick_counter = monoio_tick_counter.wrapping_add(idle_park.counter_step());
                cached_clock.update();
                next_file_id = next_file_id.max(spill_file_id.get());

                persistence_tick::check_auto_save_trigger(
                    &snapshot_trigger_rx,
                    &mut last_snapshot_epoch,
                    &mut snapshot_state,
                    &shard_databases,
                    &persistence_dir,
                    disk_offload_base.as_deref(),
                    shard_id,
                    wal_writer
                        .as_ref()
                        .map(|w| w.current_lsn().saturating_sub(1))
                        .unwrap_or(0),
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
                                shard_id,
                            );
                            crate::command::persistence::bgsave_shard_done(true);
                            bgsave_checkpoint_requested = true;
                        }
                    }
                }

                // Drain local-write WAL channel. K1a: append with the producer's
                // REAL record type instead of forcing `Command` for everything.
                while let Ok((record_type, data)) = wal_append_rx.try_recv() {
                    if let Some(ref mut wal) = wal_writer {
                        wal.append(record_type, &data);
                    }
                }

                persistence_tick::flush_wal_v3_if_needed(&mut wal_writer);

                // C3b-2 — Drive CDC fan-out on the same monoio tick body
                // as the tokio branch above. Zero CPU when no subscribers.
                if !cdc_registry.is_empty() {
                    cdc_registry.fanout_tick(cached_clock.ms() as i64);
                }

                // Off-loop fsync — same rationale as the monoio branch above.
                if server_config.appendfsync == "always" {
                    if let Some(ref mut wal) = wal_writer {
                        if let Err(e) = wal.request_sync() {
                            tracing::error!("WAL appendfsync=always failed: {}", e);
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
                    &mut wal_writer,
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
                        server_config.manifest_tombstone_retain_epochs,
                        server_config.manifest_tombstone_retain_secs,
                        &mut persistence_tick::graph_checkpoint_hook(
                            persistence_dir.as_deref(),
                            shard_id,
                        ),
                    ) {
                        wal_bytes_since_checkpoint = 0;
                        last_checkpoint_completed_at = std::time::Instant::now();
                    }
                }

                // --- Counter-based sub-timer dispatch ---
                // block_timeout: every 10ms (10 ticks)
                if monoio_tick_counter % 10 == 0 {
                    timers::expire_blocked_clients(&blocking_rc);
                }
                // expiry + eviction + MQ triggers: every 100ms (100 ticks)
                if monoio_tick_counter % 100 == 0 {
                    timers::run_active_expiry(
                        &shard_databases,
                        shard_id,
                        &mut wal_writer,
                        &repl_backlog,
                        &mut replica_txs,
                        &repl_offsets,
                        aof_pool.as_ref(),
                        match wal_kv_log_mode {
                            crate::config::WalKvLogMode::On => true,
                            crate::config::WalKvLogMode::Off => false,
                            crate::config::WalKvLogMode::Auto => {
                                !appendonly_enabled || !cdc_registry.is_empty()
                            }
                        },
                        is_replica_mirror
                            .as_ref()
                            .is_some_and(|m| m.load(std::sync::atomic::Ordering::Acquire)),
                    );
                    persistence_tick::run_eviction_tick(
                        spill_thread.as_ref(),
                        &mut shard_manifest,
                        &shard_databases,
                        shard_id,
                        &server_config,
                        &runtime_config,
                        &page_cache,
                        &mut next_file_id,
                        &mut wal_writer,
                        &script_cache_rc,
                        &lua_rc,
                        &spill_file_id,
                        disk_offload_dir.as_deref(),
                        &repl_backlog,
                        &mut replica_txs,
                        &repl_offsets,
                        aof_pool.as_ref(),
                        match wal_kv_log_mode {
                            crate::config::WalKvLogMode::On => true,
                            crate::config::WalKvLogMode::Off => false,
                            crate::config::WalKvLogMode::Auto => {
                                !appendonly_enabled || !cdc_registry.is_empty()
                            }
                        },
                    );
                    // MQ trigger check: fire debounced triggers
                    timers::fire_pending_mq_triggers(
                        &shard_databases,
                        shard_id,
                        cached_clock.ms(),
                        &pubsub_arc,
                    );
                }
                // WAL fsync + P6 ceiling-trigger + MVCC sweep: every 1s (1000 ticks).
                // P6 is gated here (not per-1ms tick) to avoid the read_dir
                // syscall overhead of wal.stats() on the hot path.
                if monoio_tick_counter % 1000 == 0 {
                    // O3: sample this shard thread's involuntary-preemption
                    // rate and gate the driver spin while the core is shared.
                    // The gate is thread-local in the vendored driver, so the
                    // flip below affects exactly this shard's parks.
                    if let Some(gov) = spin_governor.as_mut() {
                        if let Some(contended) = gov.tick() {
                            monoio::set_legacy_spin_contended(contended);
                            tracing::info!(
                                "Shard {}: busy-poll spin {} (involuntary-preemption governor)",
                                shard_id,
                                if contended {
                                    "GATED — core contended"
                                } else {
                                    "re-enabled — core quiet"
                                }
                            );
                        }
                    }
                    timers::sync_wal_v3(&mut wal_writer);
                    // c10k W11: cancel reads parked ≥1s so idle connections
                    // downshift to the probe-sized working set. Same thread
                    // as the connection tasks (thread-per-core), no locks.
                    let _ =
                        crate::server::conn::handler_monoio::idle_park::sweep(cached_clock.ms());
                    // D1: enforce `timeout N` here rather than from a
                    // per-connection select! arm — that arm shadowed every
                    // park stage above. Config is re-read each sweep, so
                    // CONFIG SET timeout applies to live connections.
                    let _ = crate::client_registry::kill_idle_clients(
                        shard_id,
                        num_shards,
                        runtime_config.read().timeout,
                        crate::storage::entry::current_time_ms(),
                    );
                    // P3+MA1+MA2: MVCC committed prune + zombie sweep + kill old snapshots
                    //             + RECL_* + segment-stall.
                    crate::shard::slice::with_shard(|s| {
                        timers::run_mvcc_sweep(
                            &mut s.vector_store,
                            #[cfg(feature = "graph")]
                            &mut s.graph_store,
                            server_config.mvcc_committed_prune_margin,
                            server_config.max_unflushed_immutable_segments,
                            server_config.mvcc_old_snapshot_threshold_secs,
                        );
                    });
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
                        &mut wal_writer,
                        &mut shard_manifest,
                        &mut control_file,
                        &control_file_path,
                    ) {
                        if persistence_tick::maybe_force_checkpoint_on_wal_overflow(
                            ckpt_mgr,
                            wal_v3,
                            page_cache_inst,
                            manifest,
                            ctrl,
                            ctrl_path,
                            shard_id,
                            last_checkpoint_completed_at,
                            server_config.wal_max_checkpoint_lag_ms,
                            &mut persistence_tick::graph_checkpoint_hook(
                                persistence_dir.as_deref(),
                                shard_id,
                            ),
                        ) {
                            wal_bytes_since_checkpoint = 0;
                            last_checkpoint_completed_at = std::time::Instant::now();
                        }
                    }
                }
                // Warm tier check: every warm_poll_ms ticks
                if monoio_tick_counter % (warm_poll_ms as u64) == 0 {
                    // task/issue #45: `disk_offload_dir` (Some iff offload
                    // enabled) is precomputed at shard init — no per-tick
                    // format!/join.
                    if let Some(shard_dir) = disk_offload_dir.as_deref() {
                        if let Some(ref mut manifest) = shard_manifest {
                            crate::shard::slice::with_shard(|s| {
                                persistence_tick::check_warm_transitions(
                                    &s.vector_store,
                                    shard_dir,
                                    manifest,
                                    server_config.segment_warm_after,
                                    server_config.engine_offload_idle_secs,
                                    &mut next_file_id,
                                    shard_id,
                                    &mut wal_writer,
                                );
                            });
                        }
                    }
                    // Budget enforcement: runs on every warm-check tick.
                    crate::shard::slice::with_shard(|s| {
                        persistence_tick::enforce_warm_mmap_budget(
                            &s.vector_store,
                            &mut warm_mmap_budget,
                            shard_id,
                        );
                    });
                }
                // MA12: Disk free-space poll (every 5000 ticks = 5s, shard 0 only).
                if shard_id == 0 && monoio_tick_counter % 5000 == 0 {
                    crate::shard::disk_monitor::poll_global();
                    // Wave 3: RSS memory watchdog poll (same 5s tick).
                    crate::shard::mem_monitor::poll_global();
                }
                // P4: Autovacuum daemon tick (every autovacuum_interval_secs * 1000 ticks).
                if monoio_tick_counter % (autovacuum_interval_secs * 1000) == 0
                    && monoio_tick_counter > 0
                {
                    crate::shard::slice::with_shard(|s| {
                        autovacuum_daemon.run_tick(
                            &mut s.vector_store,
                            #[cfg(feature = "graph")]
                            &mut s.graph_store,
                            shard_manifest.as_mut(),
                            wal_writer.as_mut(),
                            control_file.as_ref(),
                            server_config.max_wal_size_bytes(),
                            server_config.disk_offload_enabled(),
                            server_config.manifest_tombstone_retain_epochs,
                            server_config.manifest_tombstone_retain_secs,
                            server_config.max_unflushed_immutable_segments as usize,
                            server_config.graph_merge_max_segments,
                            server_config.graph_dead_edge_trigger,
                            false,
                        );
                    });
                }
                // Cold-tier orphan sweep (P9): every orphan_sweep_interval_secs * 1000 ticks.
                // Matches the tokio select! branch above. Disabled when interval is 0.
                if orphan_sweep_interval_secs > 0
                    && monoio_tick_counter % (orphan_sweep_interval_secs * 1000) == 0
                    && let Some(shard_dir) = disk_offload_dir.as_deref()
                {
                    timers::run_cold_orphan_sweep(
                        &shard_databases,
                        shard_id,
                        shard_dir,
                        shard_manifest.as_mut(),
                        cached_clock.ms(),
                    );
                }

                // #373 phase 2: decide the next park. Every counter-based
                // cadence above is a multiple of IDLE_PARK_MS — 10 / 100 /
                // 1000 / 5000 / warm_poll_ms (secs*1000, clamped ≥1000) /
                // autovacuum & orphan (secs*1000) — so the 10ms-stepped
                // counter hits each boundary exactly; entry is additionally
                // gated on an aligned counter. Any new `% N` dispatch added
                // here MUST keep N a multiple of IDLE_PARK_MS.
                let quiet = wal_writer
                    .as_ref()
                    .is_none_or(|w| w.buffered_bytes() == 0 && !w.flush_backing_off())
                    && wal_append_rx.is_empty()
                    && snapshot_state.is_none()
                    && !bgsave_checkpoint_requested
                    && checkpoint_manager.as_ref().is_none_or(|m| !m.is_active())
                    && server_config.appendfsync != "always"
                    && cdc_registry.is_empty()
                    && !hit_cap
                    && !spsc_had_work;
                let was_idle = idle_park.is_idle();
                let now_idle = idle_park.on_timer_tick(
                    crate::admin::metrics_setup::this_thread_commands(),
                    quiet,
                    monoio_tick_counter % crate::shard::idle_park::IDLE_PARK_MS == 0,
                );
                if was_idle && !now_idle {
                    // Timer-path idle exit (a condition turned non-quiet):
                    // reset the interval so its missed ticks can't burst.
                    periodic_interval = TimerImpl::interval(Duration::from_millis(1));
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

#[cfg(test)]
mod tests {
    use super::uring_bridge_allowed;
    use crate::config::ServerConfig;
    use clap::Parser;

    /// c10k B4. The bridge is only allowed when the server has no
    /// authentication to bypass in the first place.
    #[test]
    fn uring_bridge_is_refused_when_auth_is_configured() {
        let no_auth = ServerConfig::parse_from(["moon"]);
        assert!(
            uring_bridge_allowed(&no_auth),
            "no auth configured: the bridge has nothing to bypass"
        );

        let with_pass = ServerConfig::parse_from(["moon", "--requirepass", "hunter2"]);
        assert!(
            !uring_bridge_allowed(&with_pass),
            "requirepass set: the bridge would serve unauthenticated commands on the same port"
        );

        let with_aclfile = ServerConfig::parse_from(["moon", "--aclfile", "/tmp/users.acl"]);
        assert!(
            !uring_bridge_allowed(&with_aclfile),
            "aclfile set: same bypass, via ACL users instead of requirepass"
        );
    }
}
