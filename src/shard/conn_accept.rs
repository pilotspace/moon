//! Connection acceptance handlers for the shard event loop.
//!
//! Extracted from shard/mod.rs. Contains the tokio and monoio spawn logic
//! for new TCP connections (both plain and TLS).

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

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

use super::dispatch::ShardMessage;
use super::remote_subscriber_map::RemoteSubscriberMap;
use super::shared_databases::ShardDatabases;

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

/// Spawn a new tokio connection handler task (plain TCP or TLS).
///
/// Clones all required Rc/Arc shared state and spawns via `tokio::task::spawn_local`.
/// Handles both plain TCP and TLS connections via the `is_tls` flag.
#[cfg(feature = "runtime-tokio")]
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_tokio_connection(
    tcp_stream: tokio::net::TcpStream,
    is_tls: bool,
    tls_config: &Option<Arc<rustls::ServerConfig>>,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    pubsub_arc: &Arc<RwLock<PubSubRegistry>>,
    blocking_rc: &Rc<RefCell<BlockingRegistry>>,
    shutdown: &CancellationToken,
    aof_tx: &Option<channel::MpscSender<crate::persistence::aof::AofMessage>>,
    tracking_rc: &Rc<RefCell<TrackingTable>>,
    lua_rc: &Rc<RefCell<Option<Rc<mlua::Lua>>>>,
    script_cache_rc: &Rc<RefCell<crate::scripting::ScriptCache>>,
    acl_table: &Arc<RwLock<crate::acl::AclTable>>,
    runtime_config: &Arc<RwLock<RuntimeConfig>>,
    server_config: &Arc<crate::config::ServerConfig>,
    all_notifiers: &[Arc<channel::Notify>],
    snapshot_trigger_tx: &channel::WatchSender<u64>,
    repl_state: &Option<Arc<RwLock<ReplicationState>>>,
    cluster_state: &Option<Arc<RwLock<crate::cluster::ClusterState>>>,
    cached_clock: &CachedClock,
    remote_subscriber_map: &Rc<RefCell<RemoteSubscriberMap>>,
    all_pubsub_registries: &[Arc<RwLock<PubSubRegistry>>],
    shard_id: usize,
    num_shards: usize,
    config_port: u16,
) {
    use crate::server::connection::handle_connection_sharded;
    use crate::server::connection::handle_connection_sharded_inner;

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
    // Fail closed: if the config lock is poisoned, treat as requiring auth
    // (deny by default) rather than silently disabling authentication.
    let reqpass = match rtcfg.read() {
        Ok(cfg) => cfg.requirepass.clone(),
        Err(poisoned) => {
            tracing::error!(
                "Shard {}: RuntimeConfig lock poisoned, using last known config for auth",
                shard_id
            );
            poisoned.into_inner().requirepass.clone()
        }
    };
    let clk = cached_clock.clone();

    if let (true, Some(tls_cfg_ref)) = (is_tls, tls_config.as_ref()) {
        // TLS handshake before handler spawn (wrap-before-spawn pattern)
        let tls_cfg = tls_cfg_ref.clone();
        let peer_addr = tcp_stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        tokio::task::spawn_local(async move {
            let acceptor = tokio_rustls::TlsAcceptor::from(tls_cfg);
            match acceptor.accept(tcp_stream).await {
                Ok(tls_stream) => {
                    let _ = handle_connection_sharded_inner(
                        tls_stream,
                        peer_addr,
                        sdbs,
                        shard_id,
                        num_shards,
                        dtx,
                        psr,
                        blk,
                        sd,
                        reqpass,
                        aof,
                        trk,
                        cid,
                        rs,
                        cs,
                        lua,
                        sc,
                        cp,
                        acl,
                        rtcfg,
                        scfg,
                        notifiers,
                        snap_tx,
                        clk,
                        rsm,
                        all_regs,
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
        });
    } else {
        // Plain TCP connection
        tokio::task::spawn_local(async move {
            handle_connection_sharded(
                tcp_stream, sdbs, shard_id, num_shards, dtx, psr, blk, sd, reqpass, aof, trk, cid,
                rs, cs, lua, sc, cp, acl, rtcfg, scfg, notifiers, snap_tx, clk, rsm, all_regs,
            )
            .await;
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
    pubsub_arc: &Arc<RwLock<PubSubRegistry>>,
    blocking_rc: &Rc<RefCell<BlockingRegistry>>,
    shutdown: &CancellationToken,
    aof_tx: &Option<channel::MpscSender<crate::persistence::aof::AofMessage>>,
    tracking_rc: &Rc<RefCell<TrackingTable>>,
    lua_rc: &Rc<RefCell<Option<Rc<mlua::Lua>>>>,
    script_cache_rc: &Rc<RefCell<crate::scripting::ScriptCache>>,
    acl_table: &Arc<RwLock<crate::acl::AclTable>>,
    runtime_config: &Arc<RwLock<RuntimeConfig>>,
    server_config: &Arc<crate::config::ServerConfig>,
    all_notifiers: &[Arc<channel::Notify>],
    snapshot_trigger_tx: &channel::WatchSender<u64>,
    repl_state: &Option<Arc<RwLock<ReplicationState>>>,
    cluster_state: &Option<Arc<RwLock<crate::cluster::ClusterState>>>,
    cached_clock: &CachedClock,
    remote_subscriber_map: &Rc<RefCell<RemoteSubscriberMap>>,
    all_pubsub_registries: &[Arc<RwLock<PubSubRegistry>>],
    shard_id: usize,
    num_shards: usize,
    config_port: u16,
) {
    use std::os::unix::io::FromRawFd;

    use crate::server::connection::handle_connection_sharded_inner;

    // SAFETY: caller guarantees fd is a valid connected TCP socket.
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

            // State restoration happens directly via migrated_state parameter —
            // no synthetic RESP commands, no leaked responses.
            tokio::task::spawn_local(async move {
                let _ = handle_connection_sharded_inner(
                    tcp_stream,
                    peer_addr,
                    sdbs,
                    shard_id,
                    num_shards,
                    dtx,
                    psr,
                    blk,
                    sd,
                    None, // requirepass: None = pre-authenticated
                    aof,
                    trk,
                    cid,
                    rs,
                    cs,
                    lua,
                    sc,
                    cp,
                    acl,
                    rtcfg,
                    scfg,
                    notifiers,
                    snap_tx,
                    clk,
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
    tls_config: &Option<Arc<rustls::ServerConfig>>,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    pubsub_arc: &Arc<RwLock<PubSubRegistry>>,
    blocking_rc: &Rc<RefCell<BlockingRegistry>>,
    shutdown: &CancellationToken,
    aof_tx: &Option<channel::MpscSender<crate::persistence::aof::AofMessage>>,
    tracking_rc: &Rc<RefCell<TrackingTable>>,
    lua_rc: &Rc<RefCell<Option<Rc<mlua::Lua>>>>,
    script_cache_rc: &Rc<RefCell<crate::scripting::ScriptCache>>,
    acl_table: &Arc<RwLock<crate::acl::AclTable>>,
    runtime_config: &Arc<RwLock<RuntimeConfig>>,
    server_config: &Arc<crate::config::ServerConfig>,
    all_notifiers: &[Arc<channel::Notify>],
    snapshot_trigger_tx: &channel::WatchSender<u64>,
    repl_state: &Option<Arc<RwLock<ReplicationState>>>,
    cluster_state: &Option<Arc<RwLock<crate::cluster::ClusterState>>>,
    cached_clock: &CachedClock,
    remote_subscriber_map: &Rc<RefCell<RemoteSubscriberMap>>,
    all_pubsub_registries: &[Arc<RwLock<PubSubRegistry>>],
    shard_id: usize,
    num_shards: usize,
    config_port: u16,
    pending_wakers: &Rc<RefCell<Vec<std::task::Waker>>>,
) {
    use crate::server::connection::handle_connection_sharded_monoio;

    match monoio::net::TcpStream::from_std(std_tcp_stream) {
        Ok(tcp_stream) => {
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

            let peer_addr = tcp_stream
                .peer_addr()
                .map(|a| a.to_string())
                .unwrap_or_else(|_| "unknown".to_string());

            if let (true, Some(tls_cfg_ref)) = (is_tls, tls_config.as_ref()) {
                // Monoio TLS handshake before handler spawn
                let tls_cfg = tls_cfg_ref.clone();
                monoio::spawn(async move {
                    let acceptor = monoio_rustls::TlsAcceptor::from(tls_cfg);
                    match acceptor.accept(tcp_stream).await {
                        Ok(tls_stream) => {
                            let reqpass = match rtcfg.read() {
                                Ok(cfg) => cfg.requirepass.clone(),
                                Err(poisoned) => poisoned.into_inner().requirepass.clone(),
                            };
                            let _ = handle_connection_sharded_monoio(
                                tls_stream,
                                peer_addr,
                                sdbs,
                                shard_id,
                                num_shards,
                                dtx,
                                psr,
                                blk,
                                sd,
                                reqpass,
                                aof,
                                trk,
                                cid,
                                rs,
                                cs,
                                lua,
                                sc,
                                cp,
                                acl,
                                rtcfg,
                                scfg,
                                notifiers,
                                snap_tx,
                                clk,
                                rsm,
                                all_regs,
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
                });
            } else {
                // Plain TCP connection
                #[cfg(target_os = "linux")]
                let dtx2 = dispatch_tx.clone();
                #[cfg(target_os = "linux")]
                let notifiers2 = all_notifiers.to_vec();
                monoio::spawn(async move {
                    let reqpass = match rtcfg.read() {
                        Ok(cfg) => cfg.requirepass.clone(),
                        Err(poisoned) => poisoned.into_inner().requirepass.clone(),
                    };
                    let _result = handle_connection_sharded_monoio(
                        tcp_stream,
                        peer_addr,
                        sdbs,
                        shard_id,
                        num_shards,
                        dtx,
                        psr,
                        blk,
                        sd,
                        reqpass,
                        aof,
                        trk,
                        cid,
                        rs,
                        cs,
                        lua,
                        sc,
                        cp,
                        acl,
                        rtcfg,
                        scfg,
                        notifiers,
                        snap_tx,
                        clk,
                        rsm,
                        all_regs,
                        cfg!(target_os = "linux"), // can_migrate: FD dup requires libc (Linux only)
                        BytesMut::new(),
                        pw,
                        None, // fresh connection
                    )
                    .await;

                    // Handle migration result: extract FD via dup() and send via SPSC.
                    // libc::dup is only available on Linux (target-specific dependency).
                    #[cfg(target_os = "linux")]
                    if let (crate::server::conn::handler_monoio::MonoioHandlerResult::MigrateConnection { state, target_shard }, Some(stream)) = (_result.0, _result.1) {
                        use std::os::unix::io::{AsRawFd, FromRawFd};
                        use ringbuf::traits::Producer;
                        use crate::shard::mesh::ChannelMesh;

                        let raw_fd = stream.as_raw_fd();
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
    pubsub_arc: &Arc<RwLock<PubSubRegistry>>,
    blocking_rc: &Rc<RefCell<BlockingRegistry>>,
    shutdown: &CancellationToken,
    aof_tx: &Option<channel::MpscSender<crate::persistence::aof::AofMessage>>,
    tracking_rc: &Rc<RefCell<TrackingTable>>,
    lua_rc: &Rc<RefCell<Option<Rc<mlua::Lua>>>>,
    script_cache_rc: &Rc<RefCell<crate::scripting::ScriptCache>>,
    acl_table: &Arc<RwLock<crate::acl::AclTable>>,
    runtime_config: &Arc<RwLock<RuntimeConfig>>,
    server_config: &Arc<crate::config::ServerConfig>,
    all_notifiers: &[Arc<channel::Notify>],
    snapshot_trigger_tx: &channel::WatchSender<u64>,
    repl_state: &Option<Arc<RwLock<ReplicationState>>>,
    cluster_state: &Option<Arc<RwLock<crate::cluster::ClusterState>>>,
    cached_clock: &CachedClock,
    shard_id: usize,
    num_shards: usize,
    config_port: u16,
    pending_wakers: &Rc<RefCell<Vec<std::task::Waker>>>,
) {
    use std::os::unix::io::FromRawFd;

    use crate::server::connection::handle_connection_sharded_monoio;

    // SAFETY: caller guarantees fd is a valid connected TCP socket.
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
            let peer_addr = state.peer_addr.clone();

            let migration_buf = take_migration_read_buf(&mut state);

            monoio::spawn(async move {
                let _ = handle_connection_sharded_monoio(
                    tcp_stream,
                    peer_addr,
                    sdbs,
                    shard_id,
                    num_shards,
                    dtx,
                    psr,
                    blk,
                    sd,
                    None, // requirepass: None = pre-authenticated
                    aof,
                    trk,
                    cid,
                    rs,
                    cs,
                    lua,
                    sc,
                    cp,
                    acl,
                    rtcfg,
                    scfg,
                    notifiers,
                    snap_tx,
                    clk,
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
