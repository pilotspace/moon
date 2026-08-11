//! Connection acceptance handlers for the shard event loop.
//!
//! Extracted from shard/mod.rs. Contains the tokio and monoio spawn logic
//! for new TCP connections (both plain and TLS).

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use bytes::BytesMut;
use ringbuf::HeapProd;

use crate::blocking::BlockingRegistry;
use crate::command::connection as conn_cmd;
use crate::config::RuntimeConfig;
use crate::pubsub::PubSubRegistry;
use crate::replication::state::ReplicationState;
use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
#[cfg(unix)]
use crate::server::conn::affinity::MigratedConnectionState;
use crate::storage::entry::CachedClock;
use crate::tracking::TrackingTable;

use super::affinity::AffinityTracker;
use super::dispatch::ShardMessage;
use super::remote_subscriber_map::RemoteSubscriberMap;
use super::shared_databases::ShardDatabases;

/// Type alias to distinguish pre-existing std::sync::RwLock (for ACL, runtime config, etc.)
/// from parking_lot::RwLock (used for pubsub types).
type StdRwLock<T> = std::sync::RwLock<T>;

/// Maximum time a client may take to complete the TLS handshake after the
/// TCP connection is accepted (prod-hardening #17). Without a bound, a peer
/// that completes the TCP 3-way handshake on the TLS port and then sends no
/// (or a partial) ClientHello parks the task, its fd, AND its already-consumed
/// `maxclients` slot indefinitely — a zero-CPU slow-loris that can exhaust the
/// connection limit for legitimate TLS clients. On expiry we drop the stream
/// and release the slot via `record_connection_closed()`.
const TLS_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

thread_local! {
    /// F1 (#438): connection-scoped tasks alive on THIS shard thread —
    /// handler tasks, park watchers, resumed and migrated handlers. Both
    /// runtimes pin connection tasks to their shard thread (`monoio::spawn`
    /// / `tokio::task::spawn_local`), so a thread-local suffices and each
    /// shard's shutdown drain waits only on its own connections. Excludes
    /// deliberately short-lived tasks (maxclients-reject writer) and PSYNC
    /// hijack tasks (replication links have their own shutdown handling and
    /// must not be able to pin a shard's drain past its deadline… they can
    /// still eat the bounded deadline — see `SHUTDOWN_DRAIN` in event_loop).
    static LIVE_CONN_TASKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Number of live connection tasks on this shard thread (shutdown drain).
pub(crate) fn live_conn_tasks() -> usize {
    LIVE_CONN_TASKS.with(|c| c.get())
}

/// RAII increment of [`LIVE_CONN_TASKS`]. Created OUTSIDE the spawned
/// future and moved into it, so the count is visible the moment the spawn
/// call returns — a drain that runs between spawn and first poll must see
/// the task, not conclude the shard is idle.
pub(crate) struct ConnTaskGuard(());

impl ConnTaskGuard {
    pub(crate) fn enter() -> Self {
        LIVE_CONN_TASKS.with(|c| c.set(c.get() + 1));
        ConnTaskGuard(())
    }
}

impl Drop for ConnTaskGuard {
    fn drop(&mut self) {
        LIVE_CONN_TASKS.with(|c| c.set(c.get().saturating_sub(1)));
    }
}

/// Process-wide TCP listen backlog (S-5, `--tcp-backlog`). Set once at
/// startup before any listener binds; 1024 is the historical hardcoded
/// value. A process-wide static (rather than threading a param through the
/// four bind sites) because backlog is a bind-time server property.
static TCP_BACKLOG: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(1024);

/// Set the TCP listen backlog applied to every subsequently-bound listener.
/// Called once from startup after config parsing.
pub fn set_tcp_backlog(backlog: i32) {
    TCP_BACKLOG.store(backlog.max(1), std::sync::atomic::Ordering::Relaxed);
}

/// Create a SO_REUSEPORT TCP listener socket using socket2.
///
/// Returns a `std::net::TcpListener` that can be converted to
/// `tokio::net::TcpListener::from_std()` for per-shard accept, or consumed
/// via `into_raw_fd()` for io_uring multishot accept.
///
/// Each shard calls this with the same address; the kernel distributes
/// incoming connections across all sockets bound with SO_REUSEPORT.
#[cfg(unix)]
pub(crate) fn create_reuseport_socket(addr: &str) -> std::io::Result<std::net::TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};

    let sock_addr: std::net::SocketAddr = addr
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let domain = if sock_addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_port(true)?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&sock_addr.into())?;
    socket.listen(TCP_BACKLOG.load(std::sync::atomic::Ordering::Relaxed))?;
    Ok(socket.into())
}

/// Extract the read buffer remainder from migration state.
///
/// State restoration (selected_db, client_name, protocol_version, current_user)
/// is handled directly by the handler via the `migrated_state` parameter —
/// no synthetic RESP commands are injected into the read buffer.
///
/// unix-only: connection migration is fd-passing, so all callers are
/// `#[cfg(unix)]`-gated — this helper would be dead code on Windows.
#[cfg(unix)]
fn take_migration_read_buf(state: &mut MigratedConnectionState) -> BytesMut {
    std::mem::take(&mut state.read_buf_remainder)
}

/// Apply per-connection socket options on a raw file descriptor.
///
/// TCP_NODELAY always (QW1 — Redis defaults `tcp-nodelay yes`; Nagle holds
/// pipelined responses for a delayed-ACK window otherwise), plus SO_KEEPALIVE
/// with TCP_KEEPIDLE (Linux) / TCP_KEEPALIVE (macOS) to detect dead
/// connections when `keepalive_secs > 0`. Called once per accepted socket.
#[cfg(unix)]
fn apply_accepted_socket_opts(fd: std::os::unix::io::RawFd, keepalive_secs: u64) {
    use std::os::unix::io::BorrowedFd;
    // SAFETY: fd is a valid open socket owned by the caller. We borrow it
    // for the duration of this function — SockRef does not close on drop.
    let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
    let _ = crate::server::socket_opts::apply_client_socket_opts(&borrowed);
    if keepalive_secs == 0 {
        return;
    }
    let sock = socket2::SockRef::from(&borrowed);
    let interval = std::cmp::max(keepalive_secs / 3, 1);
    let ka = socket2::TcpKeepalive::new()
        .with_time(std::time::Duration::from_secs(keepalive_secs))
        .with_interval(std::time::Duration::from_secs(interval));
    let _ = sock.set_tcp_keepalive(&ka);
}

/// Spawn a new tokio connection handler task (plain TCP or TLS).
///
/// Clones all required Rc/Arc shared state and spawns via `tokio::task::spawn_local`.
/// Handles both plain TCP and TLS connections via the `is_tls` flag.
#[cfg(feature = "runtime-tokio")]
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_tokio_connection(
    tcp_stream: tokio::net::TcpStream,
    is_tls: bool,
    tls_config: &Option<crate::tls::SharedTlsConfig>,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    pubsub_arc: &Arc<parking_lot::RwLock<PubSubRegistry>>,
    blocking_rc: &Rc<RefCell<BlockingRegistry>>,
    shutdown: &CancellationToken,
    aof_pool: &Option<Arc<crate::persistence::aof::AofWriterPool>>,
    tracking_rc: &std::sync::Arc<parking_lot::Mutex<TrackingTable>>,
    lua_rc: &Rc<RefCell<Option<Rc<mlua::Lua>>>>,
    script_cache_rc: &Rc<RefCell<crate::scripting::ScriptCache>>,
    acl_table: &Arc<StdRwLock<crate::acl::AclTable>>,
    runtime_config: &Arc<parking_lot::RwLock<RuntimeConfig>>,
    server_config: &Arc<crate::config::ServerConfig>,
    all_notifiers: &[Arc<channel::Notify>],
    snapshot_trigger_tx: &channel::WatchSender<u64>,
    repl_state: &Option<Arc<parking_lot::RwLock<ReplicationState>>>,
    cluster_state: &Option<Arc<parking_lot::RwLock<crate::cluster::ClusterState>>>,
    cached_clock: &CachedClock,
    remote_subscriber_map: &Arc<parking_lot::RwLock<RemoteSubscriberMap>>,
    all_pubsub_registries: &[Arc<parking_lot::RwLock<PubSubRegistry>>],
    all_remote_sub_maps: &[Arc<parking_lot::RwLock<RemoteSubscriberMap>>],
    affinity_tracker: &Arc<parking_lot::RwLock<AffinityTracker>>,
    shard_id: usize,
    num_shards: usize,
    config_port: u16,
    // Disk-offload spill context (mirrors spawn_monoio_connection). The
    // per-shard event loop owns the SpillThread; threading its sender + the
    // shared file-id counter + the <offload>/shard-{id} dir into the ConnCtx
    // lets handler_sharded spill evicted KVs to the cold tier instead of
    // deleting them. All None/empty when disk-offload is disabled.
    spill_sender: &Option<flume::Sender<crate::storage::tiered::spill_thread::SpillRequest>>,
    spill_file_id: &Rc<std::cell::Cell<u64>>,
    disk_offload_dir: &Option<std::path::PathBuf>,
) {
    use crate::server::connection::handle_connection_sharded;
    use crate::server::connection::handle_connection_sharded_inner;

    // F1 (#438): re-register the stream with THIS shard's runtime io driver.
    // A connection accepted by the central listener arrives bound to the MAIN
    // runtime's driver (tokio io resources bind at creation); its io then
    // dies the moment main's runtime drops — during shutdown every drained
    // reply write on such a connection failed with "A Tokio 1.x context was
    // found, but it is being shutdown" while the shard's drain was still
    // running, and no shard-side ordering can save a foreign-driver stream.
    // (SO_REUSEPORT splits accepts roughly evenly between the central and
    // per-shard listeners on Linux, so about half of all connections were
    // affected.) The monoio path is immune: it forwards STD streams and
    // registers them on the shard's ring right here. `from_std` in this
    // function's (shard-thread) runtime context is the tokio equivalent.
    let tcp_stream = match tcp_stream
        .into_std()
        .and_then(tokio::net::TcpStream::from_std)
    {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                "Shard {shard_id}: shard-driver re-registration failed: {e}; dropping connection"
            );
            return;
        }
    };
    let aff = affinity_tracker.clone();
    let rsm = remote_subscriber_map.clone();
    let sdbs = shard_databases.clone();
    let dtx = dispatch_tx.clone();
    let psr = pubsub_arc.clone();
    let blk = blocking_rc.clone();
    let sd = shutdown.clone();
    let trk = tracking_rc.clone();
    let cid = conn_cmd::next_client_id();
    let rs = repl_state.clone();
    let cs = cluster_state.clone();
    let cp = config_port;
    #[allow(clippy::expect_used, clippy::unwrap_used)]
    // Startup: Lua VM init failure is fatal; as_ref() after is_none() guard
    let lua = {
        let mut lua_opt = lua_rc.borrow_mut();
        if lua_opt.is_none() {
            // M3 fix: bake this shard's OOM eviction context into the
            // redis.call/pcall closures at VM-setup time (once per shard,
            // shared by every connection landing here — shard_id/spill
            // handles/runtime_config are identical across connections).
            let eviction_ctx = crate::scripting::bridge::LuaEvictionCtx::new(
                sdbs.clone(),
                runtime_config.clone(),
                shard_id,
                spill_sender.clone(),
                spill_file_id.clone(),
                disk_offload_dir.clone(),
                num_shards,
                rs.clone(),
                aof_pool.as_ref().map(Arc::clone),
            );
            *lua_opt = Some(
                crate::scripting::setup_lua_vm(eviction_ctx).expect("Lua VM initialization failed"),
            );
        }
        lua_opt.as_ref().unwrap().clone()
    };
    let sc = script_cache_rc.clone();
    let acl = acl_table.clone();
    let rtcfg = runtime_config.clone();
    let scfg = server_config.clone();
    let notifiers = all_notifiers.to_vec();
    let snap_tx = snapshot_trigger_tx.clone();
    let all_regs = all_pubsub_registries.to_vec();
    let all_rsm = all_remote_sub_maps.to_vec();
    let reqpass = rtcfg.read().requirepass.clone();
    let maxclients_tokio = rtcfg.read().maxclients;
    let clk = cached_clock.clone();

    // Set TCP keepalive on accepted socket (unix-only: set_tcp_keepalive uses
    // raw-fd setsockopt; Windows analog deferred with the v0.3.0 service work)
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let tcp_keepalive_secs = rtcfg.read().tcp_keepalive;
        apply_accepted_socket_opts(tcp_stream.as_raw_fd(), tcp_keepalive_secs);
    }

    // Construct ConnectionContext from cloned shared state. The pool is
    // already built by the spawn site (main.rs / listener.rs / embedded.rs)
    // and threaded through `Shard::run` — we just clone the Arc once here.
    let pool_for_ctx = aof_pool.as_ref().map(Arc::clone);
    let conn_ctx = crate::server::conn::ConnectionContext::new(
        sdbs,
        shard_id,
        num_shards,
        psr,
        blk,
        reqpass,
        pool_for_ctx,
        trk,
        rs,
        cs,
        lua,
        sc,
        cp,
        acl,
        rtcfg,
        scfg,
        dtx,
        notifiers,
        snap_tx,
        clk,
        rsm,
        all_regs,
        all_rsm,
        aff,
        spill_sender.clone(),
        spill_file_id.clone(),
        disk_offload_dir.clone(),
    );

    if let (true, Some(tls_swap)) = (is_tls, tls_config.as_ref()) {
        // Load current TLS config from ArcSwap — new connections see reloaded certs
        let tls_cfg = tls_swap.load_full();
        let peer_addr = tcp_stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        let maxclients = maxclients_tokio;
        let conn_task = ConnTaskGuard::enter();
        tokio::task::spawn_local(async move {
            let _conn_task = conn_task; // F1: counted until task exit
            // maxclients check for TLS connections (plain TCP checks in handle_connection_sharded)
            if !crate::admin::metrics_setup::try_accept_connection(maxclients) {
                // c10k C3: rate-limited (see the plaintext site below).
                if let Some(suppressed) = crate::server::conn::util::maxclients_warn_due(
                    crate::storage::entry::current_time_ms(),
                ) {
                    tracing::warn!(
                        "Shard {}: TLS connection rejected: maxclients reached                          ({} further rejections suppressed since the last line)",
                        shard_id,
                        suppressed
                    );
                }
                // c10k W3 Redis parity: loud rejection. The plaintext write
                // surfaces client-side as a handshake failure (best effort).
                // c10k C3: bounded — this one awaits INLINE on the accept
                // path, so an unbounded write parks every subsequent accept
                // on this shard behind one zero-window peer.
                use tokio::io::AsyncWriteExt;
                let mut rejected = tcp_stream;
                let _ = tokio::time::timeout(
                    crate::server::conn::util::MAXCLIENTS_REJECT_WRITE_TIMEOUT,
                    rejected.write_all(b"-ERR max number of clients reached\r\n"),
                )
                .await;
                return;
            }
            // R-3: capture the underlying TCP fd before the handshake moves the
            // stream — shutdown on it force-closes the TLS session too.
            #[cfg(unix)]
            let kill_fd = {
                use std::os::unix::io::AsRawFd;
                tcp_stream.as_raw_fd()
            };
            #[cfg(not(unix))]
            let kill_fd = -1;
            let acceptor = tokio_rustls::TlsAcceptor::from(tls_cfg);
            // #17: bound the handshake so a stalled ClientHello cannot pin the
            // fd + maxclients slot forever.
            match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(tcp_stream)).await {
                Ok(Ok(tls_stream)) => {
                    let _ = handle_connection_sharded_inner(
                        tls_stream,
                        peer_addr,
                        &conn_ctx,
                        sd,
                        cid,
                        false, // can_migrate: TLS connections cannot transfer session state
                        BytesMut::new(),
                        None, // fresh connection
                        kill_fd,
                    )
                    .await;
                }
                Ok(Err(e)) => {
                    tracing::warn!("Shard {}: TLS handshake failed: {}", shard_id, e);
                }
                Err(_) => {
                    tracing::warn!(
                        "Shard {}: TLS handshake timed out after {}s",
                        shard_id,
                        TLS_HANDSHAKE_TIMEOUT.as_secs()
                    );
                }
            }
            crate::admin::metrics_setup::record_connection_closed();
        });
    } else {
        // Plain TCP connection
        let conn_task = ConnTaskGuard::enter();
        tokio::task::spawn_local(async move {
            let _conn_task = conn_task; // F1: counted until task exit
            handle_connection_sharded(tcp_stream, &conn_ctx, sd, cid).await;
        });
    }
}

/// Spawn a migrated connection handler on the target shard (Tokio runtime).
///
/// Reconstructs a `TcpStream` from the `OwnedFd` transferred via
/// `ShardMessage::MigrateConnection` (#438 F4: ownership is in the type — the
/// conversion chain is entirely safe, and an undelivered fd closes on drop),
/// prepends synthetic RESP commands for state restoration (SELECT, CLIENT
/// SETNAME), and spawns the handler.
///
/// # Limitations
///
/// TLS connections cannot be migrated because TLS session state lives in userspace and
/// cannot be reconstructed from a raw FD. Only plain TCP connections should be migrated.
// `unix`-gated alongside `runtime-tokio`: the signature takes an `OwnedFd`
// (`MigrateFd` aliases it on Unix only). Moon targets Linux + macOS (both
// Unix), so on every supported build `unix` is always true; the extra
// predicate just makes the platform coupling explicit (CodeRabbit PR #144).
#[cfg(all(feature = "runtime-tokio", unix))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_migrated_tokio_connection(
    fd: crate::shard::dispatch::MigrateFd,
    mut state: MigratedConnectionState,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    pubsub_arc: &Arc<parking_lot::RwLock<PubSubRegistry>>,
    blocking_rc: &Rc<RefCell<BlockingRegistry>>,
    shutdown: &CancellationToken,
    aof_pool: &Option<Arc<crate::persistence::aof::AofWriterPool>>,
    tracking_rc: &std::sync::Arc<parking_lot::Mutex<TrackingTable>>,
    lua_rc: &Rc<RefCell<Option<Rc<mlua::Lua>>>>,
    script_cache_rc: &Rc<RefCell<crate::scripting::ScriptCache>>,
    acl_table: &Arc<StdRwLock<crate::acl::AclTable>>,
    runtime_config: &Arc<parking_lot::RwLock<RuntimeConfig>>,
    server_config: &Arc<crate::config::ServerConfig>,
    all_notifiers: &[Arc<channel::Notify>],
    snapshot_trigger_tx: &channel::WatchSender<u64>,
    repl_state: &Option<Arc<parking_lot::RwLock<ReplicationState>>>,
    cluster_state: &Option<Arc<parking_lot::RwLock<crate::cluster::ClusterState>>>,
    cached_clock: &CachedClock,
    remote_subscriber_map: &Arc<parking_lot::RwLock<RemoteSubscriberMap>>,
    all_pubsub_registries: &[Arc<parking_lot::RwLock<PubSubRegistry>>],
    all_remote_sub_maps: &[Arc<parking_lot::RwLock<RemoteSubscriberMap>>],
    affinity_tracker: &Arc<parking_lot::RwLock<AffinityTracker>>,
    shard_id: usize,
    num_shards: usize,
    config_port: u16,
    spill_sender: &Option<flume::Sender<crate::storage::tiered::spill_thread::SpillRequest>>,
    spill_file_id: &Rc<std::cell::Cell<u64>>,
    disk_offload_dir: &Option<std::path::PathBuf>,
) {
    use crate::server::connection::handle_connection_sharded_inner;

    // F4 (#438): ownership rode the channel as an OwnedFd, so the conversion
    // to TcpStream is the safe From impl — no raw-fd reasoning left here.
    let std_stream = std::net::TcpStream::from(fd);
    if let Err(e) = std_stream.set_nonblocking(true) {
        tracing::warn!(
            "Shard {}: migrated fd set_nonblocking failed: {}",
            shard_id,
            e
        );
        return; // std_stream Drop closes FD
    }
    match tokio::net::TcpStream::from_std(std_stream) {
        Ok(tcp_stream) => {
            // Clone shared state (same pattern as spawn_tokio_connection)
            let rsm = remote_subscriber_map.clone();
            let all_regs = all_pubsub_registries.to_vec();
            let all_rsm = all_remote_sub_maps.to_vec();
            let aff = affinity_tracker.clone();
            let sdbs = shard_databases.clone();
            let dtx = dispatch_tx.clone();
            let psr = pubsub_arc.clone();
            let blk = blocking_rc.clone();
            let sd = shutdown.clone();
            let trk = tracking_rc.clone();
            let cid = state.client_id;
            let rs = repl_state.clone();
            let cs = cluster_state.clone();
            let cp = config_port;
            #[allow(clippy::expect_used, clippy::unwrap_used)]
            // Startup: Lua VM init failure is fatal; as_ref() after is_none() guard
            let lua = {
                let mut lua_opt = lua_rc.borrow_mut();
                if lua_opt.is_none() {
                    // M3 fix: see spawn_tokio_connection for rationale.
                    let eviction_ctx = crate::scripting::bridge::LuaEvictionCtx::new(
                        sdbs.clone(),
                        runtime_config.clone(),
                        shard_id,
                        spill_sender.clone(),
                        spill_file_id.clone(),
                        disk_offload_dir.clone(),
                        num_shards,
                        rs.clone(),
                        aof_pool.as_ref().map(Arc::clone),
                    );
                    *lua_opt = Some(
                        crate::scripting::setup_lua_vm(eviction_ctx)
                            .expect("Lua VM initialization failed"),
                    );
                }
                lua_opt.as_ref().unwrap().clone()
            };
            let sc = script_cache_rc.clone();
            let acl = acl_table.clone();
            let rtcfg = runtime_config.clone();
            // F5 (#438): read before rtcfg moves into ConnectionContext::new.
            let reqpass = rtcfg.read().requirepass.clone();
            let scfg = server_config.clone();
            let notifiers = all_notifiers.to_vec();
            let snap_tx = snapshot_trigger_tx.clone();
            let clk = cached_clock.clone();
            let peer_addr = state.peer_addr.clone();

            let migration_buf = take_migration_read_buf(&mut state);

            // Pool is built by the spawn site and threaded through here.
            let pool_for_ctx = aof_pool.as_ref().map(Arc::clone);
            // Preserve the disk-offload spill context across migration — without
            // this a migrated connection silently falls back to the non-spilling
            // eviction path even when disk-offload is enabled, so cold-tier
            // durability would depend on whether the connection had migrated.
            let spill_tx = spill_sender.clone();
            let spill_fid = spill_file_id.clone();
            let do_dir = disk_offload_dir.clone();
            let conn_ctx = crate::server::conn::ConnectionContext::new(
                sdbs,
                shard_id,
                num_shards,
                psr,
                blk,
                // F5 (#438, sec L3): carry the REAL requirepass. The conn's
                // auth state comes from MigratedConnectionState (a migrated
                // conn is already authenticated), so this is inert for the
                // session itself — but a later AUTH on the migrated conn now
                // validates against the actual password instead of erroring
                // with "no password is set", and any future code that
                // re-derives auth from ctx.requirepass sees the truth.
                reqpass,
                pool_for_ctx,
                trk,
                rs,
                cs,
                lua,
                sc,
                cp,
                acl,
                rtcfg,
                scfg,
                dtx,
                notifiers,
                snap_tx,
                clk,
                rsm,
                all_regs,
                all_rsm,
                aff,
                spill_tx,  // spill_sender (preserved across migration)
                spill_fid, // spill_file_id
                do_dir,    // disk_offload_dir
            );

            // State restoration happens directly via migrated_state parameter —
            // no synthetic RESP commands, no leaked responses.
            // R-3: capture the migrated socket fd for CLIENT KILL force-close.
            #[cfg(unix)]
            let kill_fd = {
                use std::os::unix::io::AsRawFd;
                tcp_stream.as_raw_fd()
            };
            #[cfg(not(unix))]
            let kill_fd = -1;
            let conn_task = ConnTaskGuard::enter();
            tokio::task::spawn_local(async move {
                let _conn_task = conn_task; // F1: counted until task exit
                let _ = handle_connection_sharded_inner(
                    tcp_stream,
                    peer_addr,
                    &conn_ctx,
                    sd,
                    cid,
                    false, // can_migrate: already-migrated connections skip re-migration sampling
                    migration_buf,
                    Some(&state),
                    kill_fd,
                )
                .await;
            });
        }
        Err(e) => {
            tracing::warn!("Shard {}: migration from_std failed: {}", shard_id, e);
            // FD already consumed by from_raw_fd; std_stream moved into from_std
        }
    }
}

/// Spawn a new monoio connection handler task (plain TCP or TLS).
///
/// Converts std TcpStream to monoio TcpStream and spawns via `monoio::spawn`.
#[cfg(feature = "runtime-monoio")]
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_monoio_connection(
    std_tcp_stream: crate::runtime::TcpStream,
    is_tls: bool,
    tls_config: &Option<crate::tls::SharedTlsConfig>,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    pubsub_arc: &Arc<parking_lot::RwLock<PubSubRegistry>>,
    blocking_rc: &Rc<RefCell<BlockingRegistry>>,
    shutdown: &CancellationToken,
    aof_pool: &Option<Arc<crate::persistence::aof::AofWriterPool>>,
    tracking_rc: &std::sync::Arc<parking_lot::Mutex<TrackingTable>>,
    lua_rc: &Rc<RefCell<Option<Rc<mlua::Lua>>>>,
    script_cache_rc: &Rc<RefCell<crate::scripting::ScriptCache>>,
    acl_table: &Arc<StdRwLock<crate::acl::AclTable>>,
    runtime_config: &Arc<parking_lot::RwLock<RuntimeConfig>>,
    server_config: &Arc<crate::config::ServerConfig>,
    all_notifiers: &[Arc<channel::Notify>],
    snapshot_trigger_tx: &channel::WatchSender<u64>,
    repl_state: &Option<Arc<parking_lot::RwLock<ReplicationState>>>,
    cluster_state: &Option<Arc<parking_lot::RwLock<crate::cluster::ClusterState>>>,
    cached_clock: &CachedClock,
    remote_subscriber_map: &Arc<parking_lot::RwLock<RemoteSubscriberMap>>,
    all_pubsub_registries: &[Arc<parking_lot::RwLock<PubSubRegistry>>],
    all_remote_sub_maps: &[Arc<parking_lot::RwLock<RemoteSubscriberMap>>],
    affinity_tracker: &Arc<parking_lot::RwLock<AffinityTracker>>,
    shard_id: usize,
    num_shards: usize,
    config_port: u16,
    spill_sender: &Option<flume::Sender<crate::storage::tiered::spill_thread::SpillRequest>>,
    spill_file_id: &Rc<std::cell::Cell<u64>>,
    disk_offload_dir: &Option<std::path::PathBuf>,
) {
    use crate::server::connection::handle_connection_sharded_monoio;

    // Set TCP keepalive on accepted socket before converting to monoio
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let keepalive_secs = runtime_config.read().tcp_keepalive;
        apply_accepted_socket_opts(std_tcp_stream.as_raw_fd(), keepalive_secs);
    }

    match monoio::net::TcpStream::from_std(std_tcp_stream) {
        Ok(tcp_stream) => {
            // R-3: capture the socket fd for CLIENT KILL force-close before the
            // stream is moved into a spawned handler (generic `S`, no AsRawFd).
            #[cfg(unix)]
            let kill_fd = {
                use std::os::unix::io::AsRawFd;
                tcp_stream.as_raw_fd()
            };
            #[cfg(not(unix))]
            let kill_fd = -1;

            // c10k W3: maxclients gate BEFORE the per-connection clone-fest +
            // ConnectionContext construction. A rejected connection previously
            // paid 3 O(num_shards) Vec clones, ~25 Arc bumps and a spawned
            // handler task before the CAS ran — and was then closed SILENTLY.
            // Redis parity: write `-ERR max number of clients reached` (best
            // effort; for TLS clients the plaintext write surfaces as a loud
            // handshake failure, matching Redis's raw-fd behavior), then
            // close. The slot was never taken, so no record_connection_closed.
            let maxclients = runtime_config.read().maxclients;
            if !crate::admin::metrics_setup::try_accept_connection(maxclients) {
                // c10k C3: one line per second, carrying the count it stands
                // for. Being at maxclients is exactly when connections arrive
                // fastest, so a line per rejection floods the log with the
                // symptom and competes for the I/O needed to diagnose it.
                if let Some(suppressed) = crate::server::conn::util::maxclients_warn_due(
                    crate::storage::entry::current_time_ms(),
                ) {
                    tracing::warn!(
                        "Shard {}: connection rejected: maxclients reached                          ({} further rejections suppressed since the last line)",
                        shard_id,
                        suppressed
                    );
                }
                monoio::spawn(async move {
                    use monoio::io::AsyncWriteRentExt;
                    let mut rejected = tcp_stream;
                    // c10k C3: bounded. This 36-byte write was untimed, so a
                    // zero-window peer parked the task and held the rejected
                    // fd open indefinitely — live fds climb past maxclients
                    // without limit, defeating the gate and the RLIMIT_NOFILE
                    // reconciliation behind it.
                    let _ = monoio::time::timeout(
                        crate::server::conn::util::MAXCLIENTS_REJECT_WRITE_TIMEOUT,
                        async {
                            let (_res, _buf) = rejected
                                .write_all(bytes::Bytes::from_static(
                                    b"-ERR max number of clients reached\r\n",
                                ))
                                .await;
                        },
                    )
                    .await;
                    // drop closes the fd
                });
                return;
            }

            let aff = affinity_tracker.clone();
            let rsm = remote_subscriber_map.clone();
            let sdbs = shard_databases.clone();
            let dtx = dispatch_tx.clone();
            let psr = pubsub_arc.clone();
            let blk = blocking_rc.clone();
            let sd = shutdown.clone();
            let trk = tracking_rc.clone();
            let cid = conn_cmd::next_client_id();
            let rs = repl_state.clone();
            let spill_tx = spill_sender.clone();
            let spill_fid = spill_file_id.clone();
            let do_dir = disk_offload_dir.clone();
            let cs = cluster_state.clone();
            let cp = config_port;
            #[allow(clippy::expect_used, clippy::unwrap_used)]
            // Startup: Lua VM init failure is fatal; as_ref() after is_none() guard
            let lua = {
                let mut lua_opt = lua_rc.borrow_mut();
                if lua_opt.is_none() {
                    // M3 fix: see spawn_tokio_connection for rationale.
                    let eviction_ctx = crate::scripting::bridge::LuaEvictionCtx::new(
                        sdbs.clone(),
                        runtime_config.clone(),
                        shard_id,
                        spill_tx.clone(),
                        spill_fid.clone(),
                        do_dir.clone(),
                        num_shards,
                        rs.clone(),
                        aof_pool.as_ref().map(Arc::clone),
                    );
                    *lua_opt = Some(
                        crate::scripting::setup_lua_vm(eviction_ctx)
                            .expect("Lua VM initialization failed"),
                    );
                }
                lua_opt.as_ref().unwrap().clone()
            };
            let sc = script_cache_rc.clone();
            let acl = acl_table.clone();
            let rtcfg = runtime_config.clone();
            let scfg = server_config.clone();
            let notifiers = all_notifiers.to_vec();
            let snap_tx = snapshot_trigger_tx.clone();
            let clk = cached_clock.clone();
            let all_regs = all_pubsub_registries.to_vec();
            let all_rsm = all_remote_sub_maps.to_vec();

            let peer_addr = tcp_stream
                .peer_addr()
                .map(|a| a.to_string())
                .unwrap_or_else(|_| "unknown".to_string());

            // Construct ConnectionContext from cloned shared state. Pool is
            // built by the spawn site and threaded through here.
            let reqpass = rtcfg.read().requirepass.clone();
            let pool_for_ctx = aof_pool.as_ref().map(Arc::clone);
            let conn_ctx = crate::server::conn::ConnectionContext::new(
                sdbs,
                shard_id,
                num_shards,
                psr,
                blk,
                reqpass,
                pool_for_ctx,
                trk,
                rs,
                cs,
                lua,
                sc,
                cp,
                acl,
                rtcfg,
                scfg,
                dtx,
                notifiers,
                snap_tx,
                clk,
                rsm,
                all_regs,
                all_rsm,
                aff,
                spill_tx,
                spill_fid,
                do_dir,
            );

            if let (true, Some(tls_swap)) = (is_tls, tls_config.as_ref()) {
                // Load current TLS config from ArcSwap — new connections see reloaded certs
                let tls_cfg = tls_swap.load_full();
                let conn_task = ConnTaskGuard::enter();
                monoio::spawn(async move {
                    let _conn_task = conn_task; // F1: counted until task exit
                    // maxclients slot already taken by the pre-spawn gate above
                    // (still BEFORE the handshake, preserving #17's stalled-
                    // ClientHello bound); released by record_connection_closed
                    // at the end of this task.
                    let acceptor = monoio_rustls::TlsAcceptor::from(tls_cfg);
                    // #17: bound the handshake so a stalled ClientHello cannot
                    // pin the fd + maxclients slot forever.
                    match monoio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(tcp_stream))
                        .await
                    {
                        Ok(Ok(tls_stream)) => {
                            // c1M P1-TLS: kept for the ParkIdle routing below
                            // (`sd` moves into the handler call).
                            let sd_park = sd.clone();
                            let result = handle_connection_sharded_monoio(
                                tls_stream,
                                peer_addr,
                                &conn_ctx,
                                sd,
                                cid,
                                false, // can_migrate: TLS connections cannot transfer session state
                                BytesMut::new(),
                                None, // fresh connection
                                kill_fd,
                                crate::server::conn::handler_monoio::ParkArgs::PARK, // routes ParkIdle below
                            )
                            .await;
                            if let (
                                crate::server::conn::handler_monoio::MonoioHandlerResult::ParkIdle {
                                    state,
                                    registry_guard,
                                },
                                Some(stream),
                            ) = result
                            {
                                // Watcher chain owns the close accounting now
                                // (the `return` skips this task's decrement).
                                spawn_parked_idle_watcher(
                                    stream,
                                    state,
                                    registry_guard,
                                    Box::new(conn_ctx),
                                    sd_park,
                                    cid,
                                    kill_fd,
                                );
                                return;
                            }
                        }
                        Ok(Err(e)) => {
                            tracing::warn!(
                                "Shard {}: Monoio TLS handshake failed: {}",
                                shard_id,
                                e
                            );
                        }
                        Err(_) => {
                            tracing::warn!(
                                "Shard {}: Monoio TLS handshake timed out after {}s",
                                shard_id,
                                TLS_HANDSHAKE_TIMEOUT.as_secs()
                            );
                        }
                    }
                    crate::admin::metrics_setup::record_connection_closed();
                });
            } else {
                // Plain TCP connection
                #[cfg(target_os = "linux")]
                let dtx2 = dispatch_tx.clone();
                #[cfg(target_os = "linux")]
                let notifiers2 = all_notifiers.to_vec();
                let conn_task = ConnTaskGuard::enter();
                monoio::spawn(async move {
                    let _conn_task = conn_task; // F1: counted until task exit
                    // maxclients slot already taken by the pre-spawn gate above;
                    // released by the migration-aware decrement at task end.
                    // R-6 fail-open: keep what we need to re-serve the client
                    // locally if a migration hand-off cannot be delivered.
                    #[cfg(target_os = "linux")]
                    let (sd2, peer2) = (sd.clone(), peer_addr.clone());
                    // c1M P1: kept for the ParkIdle routing below (`sd` moves
                    // into the handler call).
                    let sd_park = sd.clone();
                    let _result = handle_connection_sharded_monoio(
                        tcp_stream,
                        peer_addr,
                        &conn_ctx,
                        sd,
                        cid,
                        cfg!(target_os = "linux"), // can_migrate: FD dup requires libc (Linux only)
                        BytesMut::new(),
                        None, // fresh connection
                        kill_fd,
                        crate::server::conn::handler_monoio::ParkArgs::PARK, // this site routes ParkIdle below
                    )
                    .await;

                    // PSYNC hijack: master takes over the stream and drives the replica.
                    // We split the result into outcome + stream so we can route on the
                    // outcome variant before falling through to the migration path.
                    let _result_outcome = _result.0;
                    let _result_stream = _result.1;
                    let mut _hijacked_psync = false;
                    if let crate::server::conn::handler_monoio::MonoioHandlerResult::HijackForPsync {
                        client_repl_id,
                        client_offset,
                        peer_addr: hp_peer,
                    } = &_result_outcome {
                        if let Some(stream) = _result_stream {
                            _hijacked_psync = true;
                            let repl_state_clone = conn_ctx.repl_state.clone();
                            let shard_databases_clone = conn_ctx.shard_databases.clone();
                            // R2 (task #20): the multi-shard handler fans
                            // PrepareReplicaSync over the SPSC mesh.
                            let psync_num_shards = conn_ctx.num_shards;
                            let psync_dispatch_tx = conn_ctx.dispatch_tx.clone();
                            let psync_notifiers = conn_ctx.spsc_notifiers.clone();
                            let psync_shard_id = conn_ctx.shard_id;
                            let parsed_addr: std::net::SocketAddr = hp_peer
                                .parse()
                                .unwrap_or_else(|_| std::net::SocketAddr::from(([0, 0, 0, 0], 0)));
                            let client_repl_id_owned = client_repl_id.clone();
                            let client_offset_v = *client_offset;
                            monoio::spawn(async move {
                                if let Some(rs) = repl_state_clone {
                                    let res = if psync_num_shards > 1 {
                                        crate::replication::master::handle_psync_inline_multi_shard(
                                            stream,
                                            rs,
                                            parsed_addr,
                                            psync_dispatch_tx,
                                            psync_notifiers,
                                            psync_shard_id,
                                            psync_num_shards,
                                        ).await
                                    } else {
                                        crate::replication::master::handle_psync_inline_single_shard(
                                            &client_repl_id_owned,
                                            client_offset_v,
                                            stream,
                                            rs,
                                            shard_databases_clone,
                                            parsed_addr,
                                        ).await
                                    };
                                    if let Err(e) = res {
                                        tracing::warn!("PSYNC handler exited: {}", e);
                                    }
                                } else {
                                    tracing::warn!("PSYNC hijack but repl_state is missing");
                                }
                                crate::admin::metrics_setup::record_connection_closed();
                            });
                            // Skip the connection-closed metric below — the spawned task owns it.
                            return;
                        }
                    }
                    let _ = _hijacked_psync;
                    let _result = (_result_outcome, _result_stream);
                    // c1M P1: task-exit parking. Hand the stream + ~100 B of
                    // session state to a tiny readiness watcher and end this
                    // (fat) task WITHOUT recording a close — the connection
                    // is still live; the watcher chain owns the close metric
                    // from here.
                    let _result = match _result {
                        (
                            crate::server::conn::handler_monoio::MonoioHandlerResult::ParkIdle {
                                state,
                                registry_guard,
                            },
                            Some(stream),
                        ) => {
                            spawn_parked_idle_watcher(
                                stream,
                                state,
                                registry_guard,
                                Box::new(conn_ctx),
                                sd_park,
                                cid,
                                kill_fd,
                            );
                            return;
                        }
                        other => other,
                    };
                    // Handle migration result: extract FD via dup() and send via SPSC.
                    // libc::dup is only available on Linux (target-specific dependency).
                    #[cfg(target_os = "linux")]
                    let mut _migrated = false;
                    #[cfg(target_os = "linux")]
                    if let (crate::server::conn::handler_monoio::MonoioHandlerResult::MigrateConnection { state, target_shard }, Some(stream)) = _result {
                        use std::os::unix::io::{AsRawFd, FromRawFd};
                        use ringbuf::traits::Producer;
                        use crate::shard::mesh::ChannelMesh;

                        let raw_fd = stream.as_raw_fd();
                        // The ORIGINAL stream stays alive until the hand-off is
                        // confirmed (R-6 fail-open): if the push cannot be delivered
                        // we keep serving the client here instead of dropping it.
                        // SAFETY: raw_fd is a valid open socket fd from the monoio
                        // TcpStream; dup() creates a new, independent fd that we
                        // take ownership of.
                        let dup_fd = unsafe { libc::dup(raw_fd) };
                        if dup_fd < 0 {
                            tracing::warn!("Shard {}: migration dup() failed: {} — keeping connection local", shard_id, std::io::Error::last_os_error());
                            // Fail-open: resume serving on this shard, migration
                            // disabled so the affinity tracker cannot loop.
                            let _ = handle_connection_sharded_monoio(
                                stream, peer2, &conn_ctx, sd2, cid,
                                false, // can_migrate
                                BytesMut::new(),
                                Some(&state), kill_fd,
                                crate::server::conn::handler_monoio::ParkArgs::NO_PARK, // result is discarded here
                            )
                            .await;
                        } else {
                            // F4 (#438): wrap the dup immediately — from here on
                            // the fd is closed by Drop on every path (undelivered
                            // message, ring torn down at shutdown), never leaked.
                            // SAFETY: dup_fd is the fresh, uniquely-owned fd
                            // libc::dup returned above; nothing else closes it.
                            let owned_fd = unsafe { std::os::unix::io::OwnedFd::from_raw_fd(dup_fd) };
                            let mut pending_msg = Some(ShardMessage::MigrateConnection(Box::new(
                                crate::shard::dispatch::MigrateConnectionPayload {
                                    fd: owned_fd,
                                    state,
                                },
                            )));
                            let target_idx = ChannelMesh::target_index(shard_id, target_shard);
                            // Bounded retry: migration is elective — a briefly-full
                            // ring is worth a few 100µs waits, but a target that
                            // stays full gets the fail-open path, never a lost conn.
                            for attempt in 0..8u32 {
                                if attempt > 0 {
                                    monoio::time::sleep(std::time::Duration::from_micros(100)).await;
                                }
                                #[allow(clippy::unwrap_used)]
                                // pending_msg is always re-filled on Err below
                                let msg = pending_msg.take().unwrap();
                                let push_result = {
                                    let mut producers = dtx2.borrow_mut();
                                    producers[target_idx].try_push(msg)
                                };
                                match push_result {
                                    Ok(()) => {
                                        _migrated = true;
                                        notifiers2[target_shard].notify_one();
                                        tracing::info!(
                                            "Shard {}: migrated connection {} to shard {} (monoio)",
                                            shard_id, cid, target_shard
                                        );
                                        break;
                                    }
                                    Err(returned_msg) => pending_msg = Some(returned_msg),
                                }
                            }
                            if _migrated {
                                drop(stream); // hand-off confirmed: target owns dup_fd
                            } else if let Some(ShardMessage::MigrateConnection(payload)) = pending_msg {
                                // F4 (#438): dropping the payload closes the
                                // dup'd fd (OwnedFd) — no manual reclaim needed.
                                tracing::warn!(
                                    "Shard {}: migration SPSC full, keeping connection {} local (monoio)",
                                    shard_id, cid
                                );
                                // Fail-open (R-6): resume serving on this shard with
                                // the state recovered from the undelivered message.
                                let _ = handle_connection_sharded_monoio(
                                    stream, peer2, &conn_ctx, sd2, cid,
                                    false, // can_migrate: pin locally, no retry loop
                                    BytesMut::new(),
                                    Some(&payload.state), kill_fd,
                                    crate::server::conn::handler_monoio::ParkArgs::NO_PARK, // result is discarded here
                                )
                                .await;
                            }
                        }
                    }

                    // Decrement connected_clients unless connection was migrated (stays alive on target shard)
                    #[cfg(target_os = "linux")]
                    if !_migrated {
                        crate::admin::metrics_setup::record_connection_closed();
                    }
                    #[cfg(not(target_os = "linux"))]
                    crate::admin::metrics_setup::record_connection_closed();
                });
            }
        }
        Err(e) => {
            tracing::warn!("Shard {}: from_std failed: {}", shard_id, e);
        }
    }
}

/// c1M P1: raw-fd readiness await for the parked-idle watcher. TLS parks on
/// its UNDERLYING socket's readability (the vendored `io_ref()`
/// passthrough) — sound because the handler's `task_park_safe()` veto
/// guarantees nothing was buffered inside the TLS stack at park time, so
/// the next wire byte is the only possible wake source.
#[cfg(all(feature = "runtime-monoio", unix))]
pub(crate) trait ParkWatchable {
    fn park_readable(&self) -> impl std::future::Future<Output = std::io::Result<()>>;
}

#[cfg(all(feature = "runtime-monoio", unix))]
impl ParkWatchable for monoio::net::TcpStream {
    fn park_readable(&self) -> impl std::future::Future<Output = std::io::Result<()>> {
        self.readable(false)
    }
}

#[cfg(all(feature = "runtime-monoio", unix))]
impl ParkWatchable for monoio_rustls::ServerTlsStream<monoio::net::TcpStream> {
    fn park_readable(&self) -> impl std::future::Future<Output = std::io::Result<()>> {
        self.io_ref().readable(false)
    }
}

/// c1M P1: park an idle connection out of the task model. The fat handler
/// task has exited; this tiny watcher owns the stream, the ~100 B session
/// state, and the client-registry guard (CLIENT LIST/KILL entry + maxclients
/// slot stay live). It wakes on read-readiness (`park_readable()` is
/// race-free on both drivers: uring PollAdd / legacy readiness+poll) or
/// server shutdown. CLIENT KILL's `shutdown(2)` makes the fd read-ready, so
/// a parked kill flows through the wake path and the resumed handler's first
/// read sees EOF.
///
/// The future is boxed as `dyn` for two reasons: the parked footprint IS
/// this future, so it must stay small and measurable; and it breaks the
/// park→resume→park type cycle (the resumed handler task constructs this
/// watcher again on re-park).
/// RAII counter for connections currently held only by a park watcher.
#[cfg(all(feature = "runtime-monoio", unix))]
struct ParkedCount;

#[cfg(all(feature = "runtime-monoio", unix))]
impl ParkedCount {
    fn enter() -> Self {
        crate::client_registry::parked_delta(true);
        ParkedCount
    }
}

#[cfg(all(feature = "runtime-monoio", unix))]
impl Drop for ParkedCount {
    fn drop(&mut self) {
        crate::client_registry::parked_delta(false);
    }
}

/// F2 (#438, conn#9 / sec L1): a parked connection's registry guard and
/// stream, co-owned by one watcher future. `kill_clients`' per-stripe
/// fd-liveness invariant requires the guard (whose drop deregisters under
/// the stripe WRITE lock) to drop STRICTLY BEFORE the stream (whose drop
/// closes the fd): while a kill scan holds a stripe READ lock and sees an
/// entry, that entry's fd must still be an open socket, or the scan's
/// `shutdown(2)` lands on a reused fd of an unrelated connection. Inside a
/// handler task the ordering falls out of locals (guard) dropping before
/// parameters (stream); in this watcher both were co-captured upvars, whose
/// drop order is capture order — never violated in practice, but one
/// refactor away from it, and the F1 shutdown drain makes dropping this
/// future a routine path rather than a teardown-only one. The hand-written
/// Drop makes the order explicit and refactor-proof.
/// Generic over the guard type so the drop-order contract is unit-testable
/// under BOTH runtimes (monoio-only unit tests are invisible to CI — every
/// CI test job runs the tokio feature set). Production use instantiates
/// `G = RegistryGuard`.
///
/// FIELD ORDER IS THE CONTRACT: struct fields drop in declaration order
/// (RFC 1857), so `guard` (deregister, serializing against any in-flight
/// kill scan on this entry's stripe) drops strictly before `stream` (fd
/// close). Do not reorder these fields — the
/// `parked_session_drops_guard_before_stream` unit test pins the order,
/// and it lets the wake path destructure without `Option`s or a manual
/// `Drop` (which would forbid moving the parts back out).
pub(crate) struct ParkedSession<G, S> {
    guard: G,
    stream: S,
}

#[cfg_attr(not(all(feature = "runtime-monoio", unix)), allow(dead_code))]
impl<G, S> ParkedSession<G, S> {
    pub(crate) fn new(guard: G, stream: S) -> Self {
        ParkedSession { guard, stream }
    }

    /// Wake path: hand both back to the resumed handler, where the
    /// local-before-parameter drop ordering holds again.
    pub(crate) fn into_parts(self) -> (G, S) {
        (self.guard, self.stream)
    }
}

#[cfg(all(feature = "runtime-monoio", unix))]
impl<G, S: ParkWatchable> ParkedSession<G, S> {
    fn park_readable(&self) -> impl std::future::Future<Output = std::io::Result<()>> {
        self.stream.park_readable()
    }
}

#[cfg(all(feature = "runtime-monoio", unix))]
fn spawn_parked_idle_watcher<S>(
    stream: S,
    state: Box<MigratedConnectionState>,
    registry_guard: crate::server::conn::handler_monoio::RegistryGuard,
    conn_ctx: Box<crate::server::conn::ConnectionContext>,
    shutdown: CancellationToken,
    client_id: u64,
    kill_fd: i32,
) where
    S: monoio::io::AsyncReadRent
        + monoio::io::AsyncWriteRent
        + crate::server::conn::handler_monoio::idle_park::IdleParkRead
        + ParkWatchable
        + 'static,
{
    // F1: counted so the shutdown drain waits for this watcher to run its
    // shutdown arm (clean close + accounting) instead of dropping it cold.
    let conn_task = ConnTaskGuard::enter();
    // F2: guard-first drop order, whichever way this future ends.
    let session = ParkedSession::new(registry_guard, stream);
    let fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()>>> = Box::pin(async move {
        let _conn_task = conn_task;
        // Observability for `INFO clients.parked_clients`. RAII so the gauge
        // stays accurate even when this future is dropped outright (runtime
        // teardown), where neither select arm runs.
        let _parked = ParkedCount::enter();
        let woke = monoio::select! {
            res = session.park_readable() => {
                // Err (fd error) also resumes: the handler's first read
                // surfaces the real error and tears down cleanly.
                let _ = res;
                true
            }
            _ = shutdown.cancelled() => false,
        };
        if !woke {
            // Server shutdown: this watcher owns the close accounting.
            drop(session);
            crate::admin::metrics_setup::record_connection_closed();
            return;
        }
        // Woken by an out-of-band kill rather than client traffic: the idle
        // -timeout sweep and CLIENT KILL both `shutdown(2)` the fd purely to
        // break the park. Rehydrating a full handler task just so its first
        // read can observe `kill_flag` and exit is pure overhead, and a
        // fleet-wide `timeout` expiry wakes them in a burst. Close here
        // instead — this watcher already owns the close accounting.
        if crate::client_registry::is_killed(client_id) {
            drop(session);
            crate::admin::metrics_setup::record_connection_closed();
            return;
        }
        // Wake: hand the registration to the resumed handler (registration
        // handoff) — the entry is never dropped, so there is no window where
        // a racing CLIENT LIST misses the conn or CLIENT KILL ID returns 0,
        // and name/connected_at/kill state persist across the wake.
        let (registry_guard, stream) = session.into_parts();
        spawn_resumed_parked_conn(
            stream,
            state,
            registry_guard,
            conn_ctx,
            shutdown,
            client_id,
            kill_fd,
        );
    });
    monoio::spawn(fut);
}

/// c1M P1: rehydrate a parked connection into a full handler task, reusing
/// the migration-rehydration shape (`migrated_state` + initial bytes).
/// Resumed connections run with `can_migrate: false` (affinity re-sampling
/// would need the primary spawn site's full migration routing) and route
/// their own re-parks back to [`spawn_parked_idle_watcher`].
#[cfg(all(feature = "runtime-monoio", unix))]
fn spawn_resumed_parked_conn<S>(
    stream: S,
    mut state: Box<MigratedConnectionState>,
    registry_guard: crate::server::conn::handler_monoio::RegistryGuard,
    conn_ctx: Box<crate::server::conn::ConnectionContext>,
    shutdown: CancellationToken,
    client_id: u64,
    kill_fd: i32,
) where
    S: monoio::io::AsyncReadRent
        + monoio::io::AsyncWriteRent
        + crate::server::conn::handler_monoio::idle_park::IdleParkRead
        + ParkWatchable
        + 'static,
{
    use crate::server::connection::handle_connection_sharded_monoio;
    let conn_task = ConnTaskGuard::enter();
    monoio::spawn(async move {
        let _conn_task = conn_task; // F1: counted until task exit
        let initial = std::mem::take(&mut state.read_buf_remainder);
        let peer = state.peer_addr.clone();
        let (outcome, stream_back) = handle_connection_sharded_monoio(
            stream,
            peer,
            &conn_ctx,
            shutdown.clone(),
            client_id,
            // can_migrate=false is DELIBERATE (#438 F4 review): this wrapper
            // has no MigrateConnection arm — enabling migration here would
            // need the full dup/SPSC/fail-open plumbing of the primary spawn
            // site. The affinity value is also marginal: a conn only reaches
            // this path by idling past --conn-park-secs, and migration
            // sampling exists for HOT connections. A resumed conn that turns
            // hot simply stays on the shard the affinity router first chose.
            false, // can_migrate
            initial,
            Some(&state),
            kill_fd,
            // Routes ParkIdle below; carries the parked registration through
            // the wake so the registry entry is never dropped.
            crate::server::conn::handler_monoio::ParkArgs {
                can_park: true,
                kept_registration: Some(registry_guard),
            },
        )
        .await;
        match (outcome, stream_back) {
            (
                crate::server::conn::handler_monoio::MonoioHandlerResult::ParkIdle {
                    state,
                    registry_guard,
                },
                Some(stream),
            ) => {
                spawn_parked_idle_watcher(
                    stream,
                    state,
                    registry_guard,
                    conn_ctx,
                    shutdown,
                    client_id,
                    kill_fd,
                );
            }
            (
                crate::server::conn::handler_monoio::MonoioHandlerResult::HijackForPsync { .. },
                _,
            ) => {
                // A replica PSYNCs immediately after connecting — it never
                // idles past --conn-park-secs first — so the full inline-PSYNC
                // routing is not wired here. Loud so a real occurrence shows.
                tracing::warn!(
                    "connection {}: PSYNC on a resumed parked connection is unsupported; closing",
                    client_id
                );
                crate::admin::metrics_setup::record_connection_closed();
            }
            _ => {
                crate::admin::metrics_setup::record_connection_closed();
            }
        }
    });
}

/// Spawn a migrated connection handler on the target shard (monoio runtime).
///
/// Reconstructs a `monoio::net::TcpStream` from the `OwnedFd` transferred via
/// `ShardMessage::MigrateConnection` (#438 F4: safe ownership transfer, drop
/// closes), prepends synthetic RESP commands for state restoration, and
/// spawns the handler.
///
/// # Limitations
///
/// TLS connections cannot be migrated (TLS session state is in userspace).
// `unix`-gated alongside `runtime-monoio` for the same reason as the tokio twin
// above: `MigrateFd` is `OwnedFd` on Unix only. Always true on supported
// targets (Linux + macOS).
#[cfg(all(feature = "runtime-monoio", unix))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_migrated_monoio_connection(
    fd: crate::shard::dispatch::MigrateFd,
    mut state: MigratedConnectionState,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    pubsub_arc: &Arc<parking_lot::RwLock<PubSubRegistry>>,
    blocking_rc: &Rc<RefCell<BlockingRegistry>>,
    shutdown: &CancellationToken,
    aof_pool: &Option<Arc<crate::persistence::aof::AofWriterPool>>,
    tracking_rc: &std::sync::Arc<parking_lot::Mutex<TrackingTable>>,
    lua_rc: &Rc<RefCell<Option<Rc<mlua::Lua>>>>,
    script_cache_rc: &Rc<RefCell<crate::scripting::ScriptCache>>,
    acl_table: &Arc<StdRwLock<crate::acl::AclTable>>,
    runtime_config: &Arc<parking_lot::RwLock<RuntimeConfig>>,
    server_config: &Arc<crate::config::ServerConfig>,
    all_notifiers: &[Arc<channel::Notify>],
    snapshot_trigger_tx: &channel::WatchSender<u64>,
    repl_state: &Option<Arc<parking_lot::RwLock<ReplicationState>>>,
    cluster_state: &Option<Arc<parking_lot::RwLock<crate::cluster::ClusterState>>>,
    cached_clock: &CachedClock,
    remote_subscriber_map: &Arc<parking_lot::RwLock<RemoteSubscriberMap>>,
    all_pubsub_registries: &[Arc<parking_lot::RwLock<PubSubRegistry>>],
    all_remote_sub_maps: &[Arc<parking_lot::RwLock<RemoteSubscriberMap>>],
    pubsub_affinity: &Arc<parking_lot::RwLock<AffinityTracker>>,
    shard_id: usize,
    num_shards: usize,
    config_port: u16,
    spill_sender: &Option<flume::Sender<crate::storage::tiered::spill_thread::SpillRequest>>,
    spill_file_id: &Rc<std::cell::Cell<u64>>,
    disk_offload_dir: &Option<std::path::PathBuf>,
) {
    use crate::server::connection::handle_connection_sharded_monoio;

    // F4 (#438): same as the tokio twin — OwnedFd in, safe From conversion,
    // TcpStream is sole close-owner from here.
    let std_stream = std::net::TcpStream::from(fd);
    if let Err(e) = std_stream.set_nonblocking(true) {
        tracing::warn!(
            "Shard {}: migrated fd set_nonblocking failed: {}",
            shard_id,
            e
        );
        // Source shard's `try_accept_connection` already counted this
        // client; its wrapper skipped the decrement because the migration
        // succeeded from the source's perspective.  Since we cannot hand
        // ownership to the handler, we own the balancing decrement here.
        crate::admin::metrics_setup::record_connection_closed();
        return; // std_stream Drop closes FD
    }
    match monoio::net::TcpStream::from_std(std_stream) {
        Ok(tcp_stream) => {
            let sdbs = shard_databases.clone();
            let dtx = dispatch_tx.clone();
            let psr = pubsub_arc.clone();
            let blk = blocking_rc.clone();
            let sd = shutdown.clone();
            let trk = tracking_rc.clone();
            let cid = state.client_id;
            let rs = repl_state.clone();
            let cs = cluster_state.clone();
            let cp = config_port;
            #[allow(clippy::expect_used, clippy::unwrap_used)]
            // Startup: Lua VM init failure is fatal; as_ref() after is_none() guard
            let lua = {
                let mut lua_opt = lua_rc.borrow_mut();
                if lua_opt.is_none() {
                    // M3 fix: see spawn_tokio_connection for rationale.
                    let eviction_ctx = crate::scripting::bridge::LuaEvictionCtx::new(
                        sdbs.clone(),
                        runtime_config.clone(),
                        shard_id,
                        spill_sender.clone(),
                        spill_file_id.clone(),
                        disk_offload_dir.clone(),
                        num_shards,
                        rs.clone(),
                        aof_pool.as_ref().map(Arc::clone),
                    );
                    *lua_opt = Some(
                        crate::scripting::setup_lua_vm(eviction_ctx)
                            .expect("Lua VM initialization failed"),
                    );
                }
                lua_opt.as_ref().unwrap().clone()
            };
            let sc = script_cache_rc.clone();
            let acl = acl_table.clone();
            let rtcfg = runtime_config.clone();
            // F5 (#438): read before rtcfg moves into ConnectionContext::new.
            let reqpass = rtcfg.read().requirepass.clone();
            let scfg = server_config.clone();
            let notifiers = all_notifiers.to_vec();
            let snap_tx = snapshot_trigger_tx.clone();
            let clk = cached_clock.clone();
            let rsm = remote_subscriber_map.clone();
            let all_regs = all_pubsub_registries.to_vec();
            let all_rsm = all_remote_sub_maps.to_vec();
            let aff = pubsub_affinity.clone();
            let spill_tx = spill_sender.clone();
            let spill_fid = spill_file_id.clone();
            let do_dir = disk_offload_dir.clone();
            let peer_addr = state.peer_addr.clone();

            let migration_buf = take_migration_read_buf(&mut state);

            // Pool is built by the spawn site and threaded through here.
            let pool_for_ctx = aof_pool.as_ref().map(Arc::clone);
            let conn_ctx = crate::server::conn::ConnectionContext::new(
                sdbs,
                shard_id,
                num_shards,
                psr,
                blk,
                // F5 (#438, sec L3): carry the REAL requirepass. The conn's
                // auth state comes from MigratedConnectionState (a migrated
                // conn is already authenticated), so this is inert for the
                // session itself — but a later AUTH on the migrated conn now
                // validates against the actual password instead of erroring
                // with "no password is set", and any future code that
                // re-derives auth from ctx.requirepass sees the truth.
                reqpass,
                pool_for_ctx,
                trk,
                rs,
                cs,
                lua,
                sc,
                cp,
                acl,
                rtcfg,
                scfg,
                dtx,
                notifiers,
                snap_tx,
                clk,
                rsm,
                all_regs,
                all_rsm,
                aff,
                spill_tx,
                spill_fid,
                do_dir,
            );

            // R-3: capture the migrated socket fd for CLIENT KILL force-close.
            #[cfg(unix)]
            let kill_fd = {
                use std::os::unix::io::AsRawFd;
                tcp_stream.as_raw_fd()
            };
            #[cfg(not(unix))]
            let kill_fd = -1;
            let conn_task = ConnTaskGuard::enter();
            monoio::spawn(async move {
                let _conn_task = conn_task; // F1: counted until task exit
                // c1M P1: kept for the ParkIdle routing below (`sd` moves
                // into the handler call).
                let sd_park = sd.clone();
                let result = handle_connection_sharded_monoio(
                    tcp_stream,
                    peer_addr,
                    &conn_ctx,
                    sd,
                    cid,
                    false, // can_migrate: already-migrated connections skip re-migration sampling
                    migration_buf,
                    Some(&state),
                    kill_fd,
                    crate::server::conn::handler_monoio::ParkArgs::PARK, // routes ParkIdle below
                )
                .await;
                if let (
                    crate::server::conn::handler_monoio::MonoioHandlerResult::ParkIdle {
                        state,
                        registry_guard,
                    },
                    Some(stream),
                ) = result
                {
                    // Idle migrated connection parks like a plain one; the
                    // watcher chain now owns the balancing decrement below.
                    spawn_parked_idle_watcher(
                        stream,
                        state,
                        registry_guard,
                        Box::new(conn_ctx),
                        sd_park,
                        cid,
                        kill_fd,
                    );
                    return;
                }
                // Migrated connection: the source shard's wrapper skipped the
                // decrement (because `_migrated == true`), so this target-shard
                // spawn site owns the balancing decrement.  Without this the
                // AtomicU64 `CONNECTED_CLIENTS` counter would leak upward on
                // every migration and eventually trip `maxclients`.
                crate::admin::metrics_setup::record_connection_closed();
            });
        }
        Err(e) => {
            tracing::warn!(
                "Shard {}: migration from_std (monoio) failed: {}",
                shard_id,
                e
            );
            // Same rationale as the set_nonblocking failure above:
            // the source-side increment stands but no handler will run,
            // so we own the balancing decrement.
            crate::admin::metrics_setup::record_connection_closed();
        }
    }
}

#[cfg(test)]
mod conn_accept_tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    /// F2 (#438): the whole point of ParkedSession — guard drops strictly
    /// before stream, however the session ends.
    struct DropRecorder(&'static str, Rc<RefCell<Vec<&'static str>>>);
    impl Drop for DropRecorder {
        fn drop(&mut self) {
            self.1.borrow_mut().push(self.0);
        }
    }

    #[test]
    fn parked_session_drops_guard_before_stream() {
        let order = Rc::new(RefCell::new(Vec::new()));
        let session = ParkedSession::new(
            DropRecorder("guard", order.clone()),
            DropRecorder("stream", order.clone()),
        );
        drop(session);
        assert_eq!(
            &*order.borrow(),
            &["guard", "stream"],
            "kill_clients fd-liveness invariant: deregister before fd close"
        );
    }

    #[test]
    fn parked_session_into_parts_transfers_without_dropping() {
        let order = Rc::new(RefCell::new(Vec::new()));
        let session = ParkedSession::new(
            DropRecorder("guard", order.clone()),
            DropRecorder("stream", order.clone()),
        );
        let (guard, stream) = session.into_parts();
        assert!(
            order.borrow().is_empty(),
            "into_parts must not drop either part (registration handoff)"
        );
        // The caller re-establishes local-before-parameter ordering; here we
        // just verify both came back live.
        drop(guard);
        drop(stream);
        assert_eq!(&*order.borrow(), &["guard", "stream"]);
    }

    /// F1 (#438): the drain's stop condition — live count tracks guard
    /// lifetimes, visible immediately at enter (pre-spawn).
    #[test]
    fn conn_task_guard_counts_immediately_and_saturates() {
        let base = live_conn_tasks();
        let g1 = ConnTaskGuard::enter();
        let g2 = ConnTaskGuard::enter();
        assert_eq!(live_conn_tasks(), base + 2);
        drop(g1);
        assert_eq!(live_conn_tasks(), base + 1);
        drop(g2);
        assert_eq!(live_conn_tasks(), base);
    }
}
