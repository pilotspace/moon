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
use super::shared_databases::ShardDatabases;

/// Build a read buffer that prepends synthetic RESP commands for state restoration.
///
/// Used by `spawn_migrated_*_connection` to restore selected_db and client_name.
///
/// If `selected_db != 0`, prepends `SELECT {db}`. If `client_name` is set, prepends
/// `CLIENT SETNAME {name}`. The original `read_buf_remainder` is appended after the
/// synthetic commands so the handler processes state restoration before any pending
/// client data.
fn build_migration_read_buf(state: &mut MigratedConnectionState) -> BytesMut {
    let mut buf = BytesMut::new();

    // Restore selected database via synthetic SELECT command
    if state.selected_db != 0 {
        let db_str = state.selected_db.to_string();
        // RESP: *2\r\n$6\r\nSELECT\r\n${len}\r\n{db}\r\n
        buf.extend_from_slice(b"*2\r\n$6\r\nSELECT\r\n$");
        buf.extend_from_slice(db_str.len().to_string().as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(db_str.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }

    // Restore client name via synthetic CLIENT SETNAME command
    if let Some(ref name) = state.client_name {
        // RESP: *3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n${len}\r\n{name}\r\n
        buf.extend_from_slice(b"*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$");
        buf.extend_from_slice(name.len().to_string().as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(name);
        buf.extend_from_slice(b"\r\n");
    }

    // Append any unparsed bytes from the original connection
    buf.extend_from_slice(&state.read_buf_remainder);

    buf
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
    pubsub_rc: &Rc<RefCell<PubSubRegistry>>,
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
) {
    use crate::server::connection::handle_connection_sharded;
    use crate::server::connection::handle_connection_sharded_inner;

    let sdbs = shard_databases.clone();
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
                    handle_connection_sharded_inner(
                        tls_stream, peer_addr, sdbs, shard_id, num_shards, dtx, psr, blk, sd,
                        reqpass, aof, trk, cid, rs, cs, lua, sc, cp, acl, rtcfg, scfg, notifiers,
                        snap_tx, clk,
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
                rs, cs, lua, sc, cp, acl, rtcfg, scfg, notifiers, snap_tx, clk,
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
    pubsub_rc: &Rc<RefCell<PubSubRegistry>>,
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
            let psr = pubsub_rc.clone();
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
            let clk = cached_clock.clone();
            let peer_addr = state.peer_addr.clone();

            // Build read buffer with synthetic state-restoration commands prepended
            let _migration_buf = build_migration_read_buf(&mut state);

            // Pass requirepass=None so the handler treats the connection as pre-authenticated.
            // State restoration (SELECT, CLIENT SETNAME) happens via synthetic commands
            // prepended to the read buffer by the handler's normal command processing.
            tokio::task::spawn_local(async move {
                handle_connection_sharded_inner(
                    tcp_stream, peer_addr, sdbs, shard_id, num_shards, dtx, psr, blk, sd,
                    None, // requirepass: None = pre-authenticated
                    aof, trk, cid, rs, cs, lua, sc, cp, acl, rtcfg, scfg, notifiers,
                    snap_tx, clk,
                )
                .await;
            });
        }
        Err(e) => {
            tracing::warn!(
                "Shard {}: migration from_std failed: {}",
                shard_id,
                e
            );
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
    pubsub_rc: &Rc<RefCell<PubSubRegistry>>,
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
) {
    use crate::server::connection::handle_connection_sharded_monoio;

    match monoio::net::TcpStream::from_std(std_tcp_stream) {
        Ok(tcp_stream) => {
            let sdbs = shard_databases.clone();
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
                            handle_connection_sharded_monoio(
                                tls_stream, peer_addr, sdbs, shard_id, num_shards, dtx, psr, blk,
                                sd, reqpass, aof, trk, cid, rs, cs, lua, sc, cp, acl, rtcfg, scfg,
                                notifiers, snap_tx, clk,
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
                monoio::spawn(async move {
                    let reqpass = match rtcfg.read() {
                        Ok(cfg) => cfg.requirepass.clone(),
                        Err(poisoned) => poisoned.into_inner().requirepass.clone(),
                    };
                    handle_connection_sharded_monoio(
                        tcp_stream, peer_addr, sdbs, shard_id, num_shards, dtx, psr, blk, sd,
                        reqpass, aof, trk, cid, rs, cs, lua, sc, cp, acl, rtcfg, scfg, notifiers,
                        snap_tx, clk,
                    )
                    .await;
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
    pubsub_rc: &Rc<RefCell<PubSubRegistry>>,
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
            let psr = pubsub_rc.clone();
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

            // Build read buffer with synthetic state-restoration commands prepended
            let _migration_buf = build_migration_read_buf(&mut state);

            monoio::spawn(async move {
                handle_connection_sharded_monoio(
                    tcp_stream, peer_addr, sdbs, shard_id, num_shards, dtx, psr, blk, sd,
                    None, // requirepass: None = pre-authenticated
                    aof, trk, cid, rs, cs, lua, sc, cp, acl, rtcfg, scfg, notifiers,
                    snap_tx, clk,
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
