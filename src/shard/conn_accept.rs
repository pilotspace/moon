//! Connection acceptance handlers for the shard event loop.
//!
//! Extracted from shard/mod.rs. Contains the tokio and monoio spawn logic
//! for new TCP connections (both plain and TLS).

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

use ringbuf::HeapProd;

use crate::blocking::BlockingRegistry;
use crate::command::connection as conn_cmd;
use crate::config::RuntimeConfig;
use crate::pubsub::PubSubRegistry;
use crate::replication::state::ReplicationState;
use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use crate::storage::Database;
use crate::storage::entry::CachedClock;
use crate::tracking::TrackingTable;

use super::dispatch::ShardMessage;

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
    databases: &Rc<RefCell<Vec<Database>>>,
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
    let reqpass = rtcfg
        .read()
        .map(|cfg| cfg.requirepass.clone())
        .ok()
        .flatten();
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
                        tls_stream, peer_addr, dbs, shard_id, num_shards, dtx, psr, blk, sd,
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
                tcp_stream, dbs, shard_id, num_shards, dtx, psr, blk, sd, reqpass, aof, trk, cid,
                rs, cs, lua, sc, cp, acl, rtcfg, scfg, notifiers, snap_tx, clk,
            )
            .await;
        });
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
    databases: &Rc<RefCell<Vec<Database>>>,
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
                            let reqpass = rtcfg
                                .read()
                                .map(|cfg| cfg.requirepass.clone())
                                .ok()
                                .flatten();
                            handle_connection_sharded_monoio(
                                tls_stream, peer_addr, dbs, shard_id, num_shards, dtx, psr, blk,
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
                    let reqpass = rtcfg
                        .read()
                        .map(|cfg| cfg.requirepass.clone())
                        .ok()
                        .flatten();
                    handle_connection_sharded_monoio(
                        tcp_stream, peer_addr, dbs, shard_id, num_shards, dtx, psr, blk, sd,
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
