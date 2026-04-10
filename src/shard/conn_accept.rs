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

/// Create a SO_REUSEPORT TCP listener socket using socket2.
///
/// Returns a `std::net::TcpListener` that can be converted to
/// `tokio::net::TcpListener::from_std()` for per-shard accept, or consumed
/// via `into_raw_fd()` for io_uring multishot accept.
///
/// Each shard calls this with the same address; the kernel distributes
/// incoming connections across all sockets bound with SO_REUSEPORT.
#[cfg(target_os = "linux")]
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
    socket.listen(1024)?;
    Ok(socket.into())
}

/// Extract the read buffer remainder from migration state.
///
/// State restoration (selected_db, client_name, protocol_version, current_user)
/// is handled directly by the handler via the `migrated_state` parameter —
/// no synthetic RESP commands are injected into the read buffer.
fn take_migration_read_buf(state: &mut MigratedConnectionState) -> BytesMut {
    std::mem::take(&mut state.read_buf_remainder)
}

/// Set TCP keepalive on a raw file descriptor.
///
/// Sets SO_KEEPALIVE and TCP_KEEPIDLE (Linux) / TCP_KEEPALIVE (macOS) to detect
/// dead connections. Called once per accepted socket.
#[cfg(unix)]
fn set_tcp_keepalive(fd: std::os::unix::io::RawFd, keepalive_secs: u64) {
    if keepalive_secs == 0 {
        return;
    }
    use std::os::unix::io::{FromRawFd, IntoRawFd};
    // SAFETY: we borrow the fd temporarily — socket2::Socket does NOT take ownership
    // because we call into_raw_fd() before this scope ends, preventing double-close.
    let sock = unsafe { socket2::Socket::from_raw_fd(fd) };
    let ka = socket2::TcpKeepalive::new()
        .with_time(std::time::Duration::from_secs(keepalive_secs));
    // Ignore errors — keepalive is best-effort
    let _ = sock.set_tcp_keepalive(&ka);
    // Release ownership back — we don't own this fd
    let _ = sock.into_raw_fd();
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
    aof_tx: &Option<channel::MpscSender<crate::persistence::aof::AofMessage>>,
    tracking_rc: &Rc<RefCell<TrackingTable>>,
    lua_rc: &Rc<RefCell<Option<Rc<mlua::Lua>>>>,
    script_cache_rc: &Rc<RefCell<crate::scripting::ScriptCache>>,
    acl_table: &Arc<StdRwLock<crate::acl::AclTable>>,
    runtime_config: &Arc<parking_lot::RwLock<RuntimeConfig>>,
    server_config: &Arc<crate::config::ServerConfig>,
    all_notifiers: &[Arc<channel::Notify>],
    snapshot_trigger_tx: &channel::WatchSender<u64>,
    repl_state: &Option<Arc<StdRwLock<ReplicationState>>>,
    cluster_state: &Option<Arc<StdRwLock<crate::cluster::ClusterState>>>,
    cached_clock: &CachedClock,
    remote_subscriber_map: &Arc<parking_lot::RwLock<RemoteSubscriberMap>>,
    all_pubsub_registries: &[Arc<parking_lot::RwLock<PubSubRegistry>>],
    all_remote_sub_maps: &[Arc<parking_lot::RwLock<RemoteSubscriberMap>>],
    affinity_tracker: &Arc<parking_lot::RwLock<AffinityTracker>>,
    shard_id: usize,
    num_shards: usize,
    config_port: u16,
) {
    use crate::server::connection::handle_connection_sharded;
    use crate::server::connection::handle_connection_sharded_inner;

    let aff = affinity_tracker.clone();
    let rsm = remote_subscriber_map.clone();
    let sdbs = shard_databases.clone();
    let dtx = dispatch_tx.clone();
    let psr = pubsub_arc.clone();
    let blk = blocking_rc.clone();
    let sd = shutdown.clone();
    let aof = aof_tx.clone();
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
            *lua_opt =
                Some(crate::scripting::setup_lua_vm().expect("Lua VM initialization failed"));
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
    let tcp_keepalive_secs = rtcfg.read().tcp_keepalive;
    let clk = cached_clock.clone();

    // Set TCP keepalive on accepted socket
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        set_tcp_keepalive(tcp_stream.as_raw_fd(), tcp_keepalive_secs);
    }

    // Construct ConnectionContext from cloned shared state
    let conn_ctx = crate::server::conn::ConnectionContext::new(
        sdbs,
        shard_id,
        num_shards,
        psr,
        blk,
        reqpass,
        aof,
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
        None, // spill_sender (tokio handler doesn't use tiered storage)
        Rc::new(std::cell::Cell::new(0)), // spill_file_id placeholder
        None, // disk_offload_dir
    );

    if let (true, Some(tls_swap)) = (is_tls, tls_config.as_ref()) {
        // Load current TLS config from ArcSwap — new connections see reloaded certs
        let tls_cfg = tls_swap.load_full();
        let peer_addr = tcp_stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        let maxclients = maxclients_tokio;
        tokio::task::spawn_local(async move {
            // maxclients check for TLS connections (plain TCP checks in handle_connection_sharded)
            if !crate::admin::metrics_setup::try_accept_connection(maxclients) {
                tracing::warn!("Shard {}: TLS connection rejected: maxclients reached", shard_id);
                return;
            }
            let acceptor = tokio_rustls::TlsAcceptor::from(tls_cfg);
            match acceptor.accept(tcp_stream).await {
                Ok(tls_stream) => {
                    let _ = handle_connection_sharded_inner(
                        tls_stream,
                        peer_addr,
                        &conn_ctx,
                        sd,
                        cid,
                        false, // can_migrate: TLS connections cannot transfer session state
                        BytesMut::new(),
                        None, // fresh connection
                    )
                    .await;
                }
                Err(e) => {
                    tracing::warn!("Shard {}: TLS handshake failed: {}", shard_id, e);
                }
            }
            crate::admin::metrics_setup::record_connection_closed();
        });
    } else {
        // Plain TCP connection
        tokio::task::spawn_local(async move {
            handle_connection_sharded(tcp_stream, &conn_ctx, sd, cid).await;
        });
    }
}

/// Spawn a migrated connection handler on the target shard (Tokio runtime).
///
/// Reconstructs a `TcpStream` from a raw FD transferred via `ShardMessage::MigrateConnection`,
/// prepends synthetic RESP commands for state restoration (SELECT, CLIENT SETNAME), and
/// spawns a handler with `requirepass = None` (pre-authenticated).
///
/// # Safety
///
/// The caller must ensure `fd` is a valid, open file descriptor representing a connected
/// TCP socket. Ownership is transferred: this function consumes the FD (via `from_raw_fd`).
///
/// # Limitations
///
/// TLS connections cannot be migrated because TLS session state lives in userspace and
/// cannot be reconstructed from a raw FD. Only plain TCP connections should be migrated.
#[cfg(feature = "runtime-tokio")]
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_migrated_tokio_connection(
    fd: std::os::unix::io::RawFd,
    mut state: MigratedConnectionState,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    pubsub_arc: &Arc<parking_lot::RwLock<PubSubRegistry>>,
    blocking_rc: &Rc<RefCell<BlockingRegistry>>,
    shutdown: &CancellationToken,
    aof_tx: &Option<channel::MpscSender<crate::persistence::aof::AofMessage>>,
    tracking_rc: &Rc<RefCell<TrackingTable>>,
    lua_rc: &Rc<RefCell<Option<Rc<mlua::Lua>>>>,
    script_cache_rc: &Rc<RefCell<crate::scripting::ScriptCache>>,
    acl_table: &Arc<StdRwLock<crate::acl::AclTable>>,
    runtime_config: &Arc<parking_lot::RwLock<RuntimeConfig>>,
    server_config: &Arc<crate::config::ServerConfig>,
    all_notifiers: &[Arc<channel::Notify>],
    snapshot_trigger_tx: &channel::WatchSender<u64>,
    repl_state: &Option<Arc<StdRwLock<ReplicationState>>>,
    cluster_state: &Option<Arc<StdRwLock<crate::cluster::ClusterState>>>,
    cached_clock: &CachedClock,
    remote_subscriber_map: &Arc<parking_lot::RwLock<RemoteSubscriberMap>>,
    all_pubsub_registries: &[Arc<parking_lot::RwLock<PubSubRegistry>>],
    all_remote_sub_maps: &[Arc<parking_lot::RwLock<RemoteSubscriberMap>>],
    affinity_tracker: &Arc<parking_lot::RwLock<AffinityTracker>>,
    shard_id: usize,
    num_shards: usize,
    config_port: u16,
) {
    use std::os::unix::io::FromRawFd;

    use crate::server::connection::handle_connection_sharded_inner;

    // `fd` was produced by `libc::dup()` on the source shard before being pushed
    // through the SPSC channel. That dup is a fresh, owned kernel fd, distinct from
    // any other open fd. Ownership is transferred exactly once through the channel —
    // the source shard drops the original stream immediately after `dup`, and on
    // SPSC push failure the producer reconstructs an `OwnedFd` to close the dup.
    // Here on the consumer side we take ownership by wrapping it in `TcpStream`,
    // whose `Drop` closes the fd exactly once. No aliasing, no double-close.
    // SAFETY: fd is a valid, uniquely-owned dup'd socket transferred via SPSC.
    let std_stream = unsafe { std::net::TcpStream::from_raw_fd(fd) };
    if let Err(e) = std_stream.set_nonblocking(true) {
        tracing::warn!(
            "Shard {}: migrated fd {} set_nonblocking failed: {}",
            shard_id,
            fd,
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
            let aof = aof_tx.clone();
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
                    *lua_opt = Some(
                        crate::scripting::setup_lua_vm().expect("Lua VM initialization failed"),
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
            let peer_addr = state.peer_addr.clone();

            let migration_buf = take_migration_read_buf(&mut state);

            let conn_ctx = crate::server::conn::ConnectionContext::new(
                sdbs,
                shard_id,
                num_shards,
                psr,
                blk,
                None, // requirepass: None = pre-authenticated
                aof,
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
                None,                             // spill_sender
                Rc::new(std::cell::Cell::new(0)), // spill_file_id
                None,                             // disk_offload_dir
            );

            // State restoration happens directly via migrated_state parameter —
            // no synthetic RESP commands, no leaked responses.
            tokio::task::spawn_local(async move {
                let _ = handle_connection_sharded_inner(
                    tcp_stream,
                    peer_addr,
                    &conn_ctx,
                    sd,
                    cid,
                    false, // can_migrate: already-migrated connections skip re-migration sampling
                    migration_buf,
                    Some(&state),
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
    aof_tx: &Option<channel::MpscSender<crate::persistence::aof::AofMessage>>,
    tracking_rc: &Rc<RefCell<TrackingTable>>,
    lua_rc: &Rc<RefCell<Option<Rc<mlua::Lua>>>>,
    script_cache_rc: &Rc<RefCell<crate::scripting::ScriptCache>>,
    acl_table: &Arc<StdRwLock<crate::acl::AclTable>>,
    runtime_config: &Arc<parking_lot::RwLock<RuntimeConfig>>,
    server_config: &Arc<crate::config::ServerConfig>,
    all_notifiers: &[Arc<channel::Notify>],
    snapshot_trigger_tx: &channel::WatchSender<u64>,
    repl_state: &Option<Arc<StdRwLock<ReplicationState>>>,
    cluster_state: &Option<Arc<StdRwLock<crate::cluster::ClusterState>>>,
    cached_clock: &CachedClock,
    remote_subscriber_map: &Arc<parking_lot::RwLock<RemoteSubscriberMap>>,
    all_pubsub_registries: &[Arc<parking_lot::RwLock<PubSubRegistry>>],
    all_remote_sub_maps: &[Arc<parking_lot::RwLock<RemoteSubscriberMap>>],
    affinity_tracker: &Arc<parking_lot::RwLock<AffinityTracker>>,
    shard_id: usize,
    num_shards: usize,
    config_port: u16,
    pending_wakers: &Rc<RefCell<Vec<std::task::Waker>>>,
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
        set_tcp_keepalive(std_tcp_stream.as_raw_fd(), keepalive_secs);
    }

    match monoio::net::TcpStream::from_std(std_tcp_stream) {
        Ok(tcp_stream) => {
            let aff = affinity_tracker.clone();
            let rsm = remote_subscriber_map.clone();
            let sdbs = shard_databases.clone();
            let dtx = dispatch_tx.clone();
            let psr = pubsub_arc.clone();
            let blk = blocking_rc.clone();
            let sd = shutdown.clone();
            let aof = aof_tx.clone();
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
                    *lua_opt = Some(
                        crate::scripting::setup_lua_vm().expect("Lua VM initialization failed"),
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
            let pw = pending_wakers.clone();
            let all_regs = all_pubsub_registries.to_vec();
            let all_rsm = all_remote_sub_maps.to_vec();

            let peer_addr = tcp_stream
                .peer_addr()
                .map(|a| a.to_string())
                .unwrap_or_else(|_| "unknown".to_string());

            // Construct ConnectionContext from cloned shared state
            let reqpass = rtcfg.read().requirepass.clone();
            let conn_ctx = crate::server::conn::ConnectionContext::new(
                sdbs, shard_id, num_shards, psr, blk, reqpass, aof, trk, rs, cs, lua, sc, cp, acl,
                rtcfg, scfg, dtx, notifiers, snap_tx, clk, rsm, all_regs, all_rsm, aff, spill_tx,
                spill_fid, do_dir,
            );

            let maxclients = conn_ctx.runtime_config.read().maxclients;
            if let (true, Some(tls_swap)) = (is_tls, tls_config.as_ref()) {
                // Load current TLS config from ArcSwap — new connections see reloaded certs
                let tls_cfg = tls_swap.load_full();
                monoio::spawn(async move {
                    if !crate::admin::metrics_setup::try_accept_connection(maxclients) {
                        tracing::warn!("Shard {}: TLS connection rejected: maxclients reached", shard_id);
                        return;
                    }
                    let acceptor = monoio_rustls::TlsAcceptor::from(tls_cfg);
                    match acceptor.accept(tcp_stream).await {
                        Ok(tls_stream) => {
                            let _ = handle_connection_sharded_monoio(
                                tls_stream,
                                peer_addr,
                                &conn_ctx,
                                sd,
                                cid,
                                false, // can_migrate: TLS connections cannot transfer session state
                                BytesMut::new(),
                                pw,
                                None, // fresh connection
                            )
                            .await;
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Shard {}: Monoio TLS handshake failed: {}",
                                shard_id,
                                e
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
                monoio::spawn(async move {
                    if !crate::admin::metrics_setup::try_accept_connection(maxclients) {
                        tracing::warn!("Shard {}: connection rejected: maxclients reached", shard_id);
                        return;
                    }
                    let _result = handle_connection_sharded_monoio(
                        tcp_stream,
                        peer_addr,
                        &conn_ctx,
                        sd,
                        cid,
                        cfg!(target_os = "linux"), // can_migrate: FD dup requires libc (Linux only)
                        BytesMut::new(),
                        pw,
                        None, // fresh connection
                    )
                    .await;

                    // Handle migration result: extract FD via dup() and send via SPSC.
                    // libc::dup is only available on Linux (target-specific dependency).
                    #[cfg(target_os = "linux")]
                    let mut _migrated = false;
                    #[cfg(target_os = "linux")]
                    if let (crate::server::conn::handler_monoio::MonoioHandlerResult::MigrateConnection { state, target_shard }, Some(stream)) = (_result.0, _result.1) {
                        use std::os::unix::io::{AsRawFd, FromRawFd};
                        use ringbuf::traits::Producer;
                        use crate::shard::mesh::ChannelMesh;

                        _migrated = true;
                        let raw_fd = stream.as_raw_fd();
                        // SAFETY: raw_fd is a valid open socket fd from the monoio TcpStream.
                        // dup() creates a new, independent fd that we take ownership of.
                        let dup_fd = unsafe { libc::dup(raw_fd) };
                        drop(stream); // closes original fd
                        if dup_fd < 0 {
                            tracing::warn!("Shard {}: migration dup() failed: {}", shard_id, std::io::Error::last_os_error());
                        } else {
                            let msg = ShardMessage::MigrateConnection { fd: dup_fd, state };
                            let target_idx = ChannelMesh::target_index(shard_id, target_shard);
                            let push_result = {
                                let mut producers = dtx2.borrow_mut();
                                producers[target_idx].try_push(msg)
                            };
                            match push_result {
                                Ok(()) => {
                                    notifiers2[target_shard].notify_one();
                                    tracing::info!(
                                        "Shard {}: migrated connection {} to shard {} (monoio)",
                                        shard_id, cid, target_shard
                                    );
                                }
                                Err(returned_msg) => {
                                    if let ShardMessage::MigrateConnection { fd, .. } = returned_msg {
                                        // SAFETY: fd is a valid dup'd socket from libc::dup above.
                                        // SPSC push failed, so ownership returns to us for cleanup.
                                        drop(unsafe { std::os::unix::io::OwnedFd::from_raw_fd(fd) });
                                    }
                                    tracing::warn!(
                                        "Shard {}: migration SPSC full, connection {} lost (monoio)",
                                        shard_id, cid
                                    );
                                }
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

/// Spawn a migrated connection handler on the target shard (monoio runtime).
///
/// Reconstructs a `monoio::net::TcpStream` from a raw FD transferred via
/// `ShardMessage::MigrateConnection`, prepends synthetic RESP commands for state
/// restoration, and spawns a handler with `requirepass = None` (pre-authenticated).
///
/// # Safety
///
/// Same safety requirements as `spawn_migrated_tokio_connection`: the caller must
/// ensure `fd` is a valid, open file descriptor for a connected TCP socket.
///
/// # Limitations
///
/// TLS connections cannot be migrated (TLS session state is in userspace).
#[cfg(feature = "runtime-monoio")]
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_migrated_monoio_connection(
    fd: std::os::unix::io::RawFd,
    mut state: MigratedConnectionState,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    pubsub_arc: &Arc<parking_lot::RwLock<PubSubRegistry>>,
    blocking_rc: &Rc<RefCell<BlockingRegistry>>,
    shutdown: &CancellationToken,
    aof_tx: &Option<channel::MpscSender<crate::persistence::aof::AofMessage>>,
    tracking_rc: &Rc<RefCell<TrackingTable>>,
    lua_rc: &Rc<RefCell<Option<Rc<mlua::Lua>>>>,
    script_cache_rc: &Rc<RefCell<crate::scripting::ScriptCache>>,
    acl_table: &Arc<StdRwLock<crate::acl::AclTable>>,
    runtime_config: &Arc<parking_lot::RwLock<RuntimeConfig>>,
    server_config: &Arc<crate::config::ServerConfig>,
    all_notifiers: &[Arc<channel::Notify>],
    snapshot_trigger_tx: &channel::WatchSender<u64>,
    repl_state: &Option<Arc<StdRwLock<ReplicationState>>>,
    cluster_state: &Option<Arc<StdRwLock<crate::cluster::ClusterState>>>,
    cached_clock: &CachedClock,
    remote_subscriber_map: &Arc<parking_lot::RwLock<RemoteSubscriberMap>>,
    all_pubsub_registries: &[Arc<parking_lot::RwLock<PubSubRegistry>>],
    all_remote_sub_maps: &[Arc<parking_lot::RwLock<RemoteSubscriberMap>>],
    pubsub_affinity: &Arc<parking_lot::RwLock<AffinityTracker>>,
    shard_id: usize,
    num_shards: usize,
    config_port: u16,
    pending_wakers: &Rc<RefCell<Vec<std::task::Waker>>>,
    spill_sender: &Option<flume::Sender<crate::storage::tiered::spill_thread::SpillRequest>>,
    spill_file_id: &Rc<std::cell::Cell<u64>>,
    disk_offload_dir: &Option<std::path::PathBuf>,
) {
    use std::os::unix::io::FromRawFd;

    use crate::server::connection::handle_connection_sharded_monoio;

    // Same ownership chain as `spawn_migrated_tokio_connection`: `fd` is a dup'd
    // socket transferred exactly once through SPSC, source already dropped its handle.
    // SAFETY: fd is a valid, uniquely-owned dup'd socket; TcpStream is sole close-owner.
    let std_stream = unsafe { std::net::TcpStream::from_raw_fd(fd) };
    if let Err(e) = std_stream.set_nonblocking(true) {
        tracing::warn!(
            "Shard {}: migrated fd {} set_nonblocking failed: {}",
            shard_id,
            fd,
            e
        );
        return; // std_stream Drop closes FD
    }
    match monoio::net::TcpStream::from_std(std_stream) {
        Ok(tcp_stream) => {
            let sdbs = shard_databases.clone();
            let dtx = dispatch_tx.clone();
            let psr = pubsub_arc.clone();
            let blk = blocking_rc.clone();
            let sd = shutdown.clone();
            let aof = aof_tx.clone();
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
                    *lua_opt = Some(
                        crate::scripting::setup_lua_vm().expect("Lua VM initialization failed"),
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
            let rsm = remote_subscriber_map.clone();
            let all_regs = all_pubsub_registries.to_vec();
            let all_rsm = all_remote_sub_maps.to_vec();
            let aff = pubsub_affinity.clone();
            let pw = pending_wakers.clone();
            let spill_tx = spill_sender.clone();
            let spill_fid = spill_file_id.clone();
            let do_dir = disk_offload_dir.clone();
            let peer_addr = state.peer_addr.clone();

            let migration_buf = take_migration_read_buf(&mut state);

            let conn_ctx = crate::server::conn::ConnectionContext::new(
                sdbs, shard_id, num_shards, psr, blk,
                None, // requirepass: None = pre-authenticated
                aof, trk, rs, cs, lua, sc, cp, acl, rtcfg, scfg, dtx, notifiers, snap_tx, clk, rsm,
                all_regs, all_rsm, aff, spill_tx, spill_fid, do_dir,
            );

            monoio::spawn(async move {
                let _ = handle_connection_sharded_monoio(
                    tcp_stream,
                    peer_addr,
                    &conn_ctx,
                    sd,
                    cid,
                    false, // can_migrate: already-migrated connections skip re-migration sampling
                    migration_buf,
                    pw,
                    Some(&state),
                )
                .await;
            });
        }
        Err(e) => {
            tracing::warn!(
                "Shard {}: migration from_std (monoio) failed: {}",
                shard_id,
                e
            );
        }
    }
}
