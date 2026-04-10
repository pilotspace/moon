use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use ringbuf::HeapProd;

use crate::blocking::BlockingRegistry;
use crate::cluster::ClusterState;
use crate::config::{RuntimeConfig, ServerConfig};
use crate::persistence::aof::AofMessage;
use crate::protocol::Frame;
use crate::pubsub::PubSubRegistry;
use crate::replication::state::ReplicationState;
use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use crate::scripting::ScriptCache;
use crate::shard::dispatch::ShardMessage;
use crate::shard::shared_databases::ShardDatabases;
use crate::storage::entry::CachedClock;
use crate::tracking::{TrackingState, TrackingTable};

/// Shared immutable state provided to each connection handler by the shard.
///
/// Bundles the ~20 parameters currently passed individually to
/// `handle_connection_sharded_inner`. All fields use shared ownership
/// (`Rc`, `Arc`) so cloning is cheap.
///
/// **Phase 44:** Defined only. Adoption in connection handlers deferred to Phase 48.
#[allow(dead_code)]
pub struct ConnectionContext {
    pub shard_databases: Arc<ShardDatabases>,
    pub shard_id: usize,
    pub num_shards: usize,
    pub dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    pub pubsub_registry: Rc<RefCell<PubSubRegistry>>,
    pub blocking_registry: Rc<RefCell<BlockingRegistry>>,
    pub shutdown: CancellationToken,
    pub requirepass: Option<String>,
    pub aof_tx: Option<channel::MpscSender<AofMessage>>,
    pub tracking_table: Rc<RefCell<TrackingTable>>,
    pub repl_state: Option<Arc<RwLock<ReplicationState>>>,
    pub cluster_state: Option<Arc<RwLock<ClusterState>>>,
    pub lua: Rc<mlua::Lua>,
    pub script_cache: Rc<RefCell<ScriptCache>>,
    pub config_port: u16,
    pub acl_table: Arc<RwLock<crate::acl::AclTable>>,
    pub runtime_config: Arc<parking_lot::RwLock<RuntimeConfig>>,
    pub config: Arc<ServerConfig>,
    pub spsc_notifiers: Vec<Arc<channel::Notify>>,
    pub snapshot_trigger_tx: channel::WatchSender<u64>,
    pub cached_clock: CachedClock,
}

/// Per-connection mutable state capturing auth, transaction, pub/sub, and
/// protocol modes as a flat struct with explicit fields.
///
/// Created by `ConnectionState::new()` at connection accept time.
/// Mirrors the local variables currently declared at the top of each handler body.
///
/// **Phase 44:** Defined only. Adoption in connection handlers deferred to Phase 48.
pub struct ConnectionState {
    /// RESP protocol version (2 or 3, set by HELLO command)
    pub protocol_version: u8,
    /// Currently selected database index
    pub selected_db: usize,
    /// Whether the client has authenticated (true if no password required)
    pub authenticated: bool,
    /// ACL username for the current session
    pub current_user: String,
    /// Whether the client is in a MULTI transaction block
    pub in_multi: bool,
    /// Queued commands for MULTI/EXEC
    pub command_queue: Vec<Frame>,
    /// Client-side tracking state
    pub tracking_state: TrackingState,
    /// Receiver for tracking invalidation messages
    pub tracking_rx: Option<channel::MpscReceiver<Frame>>,
    /// Client name set by CLIENT SETNAME
    pub client_name: Option<Bytes>,
    /// Cluster ASKING flag (set by ASKING, cleared before routing check)
    pub asking: bool,
    /// Pub/sub subscription count (>0 means client is in subscriber mode)
    pub subscription_count: usize,
    /// Subscriber ID for pub/sub (0 means not subscribed)
    pub subscriber_id: u64,
    /// Pub/sub message sender
    pub pubsub_tx: Option<channel::MpscSender<Frame>>,
    /// Pub/sub message receiver
    pub pubsub_rx: Option<channel::MpscReceiver<Frame>>,
    /// Unique client ID assigned at connection accept
    pub client_id: u64,
    /// Peer address string for logging
    pub peer_addr: String,
}

impl ConnectionState {
    /// Create a new ConnectionState for a freshly accepted connection.
    ///
    /// `authenticated` is set to `true` if no requirepass is configured.
    pub fn new(client_id: u64, peer_addr: String, requirepass_set: bool) -> Self {
        Self {
            protocol_version: 2,
            selected_db: 0,
            authenticated: !requirepass_set,
            current_user: "default".to_string(),
            in_multi: false,
            command_queue: Vec::new(),
            tracking_state: TrackingState::default(),
            tracking_rx: None,
            client_name: None,
            asking: false,
            subscription_count: 0,
            subscriber_id: 0,
            pubsub_tx: None,
            pubsub_rx: None,
            client_id,
            peer_addr,
        }
    }

    /// Returns true if the client is in pub/sub subscriber mode.
    pub fn is_subscriber(&self) -> bool {
        self.subscription_count > 0
    }

    /// Returns true if the client is in a MULTI transaction block.
    pub fn is_in_transaction(&self) -> bool {
        self.in_multi
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_connection_state_defaults() {
        let state = ConnectionState::new(42, "127.0.0.1:9999".to_string(), false);
        assert_eq!(state.protocol_version, 2);
        assert_eq!(state.selected_db, 0);
        assert!(state.authenticated); // no requirepass
        assert_eq!(state.current_user, "default");
        assert!(!state.in_multi);
        assert!(state.command_queue.is_empty());
        assert!(!state.asking);
        assert_eq!(state.subscription_count, 0);
        assert_eq!(state.client_id, 42);
        assert_eq!(state.peer_addr, "127.0.0.1:9999");
    }

    #[test]
    fn new_connection_state_with_requirepass() {
        let state = ConnectionState::new(1, "10.0.0.1:1234".to_string(), true);
        assert!(!state.authenticated); // requirepass is set
    }

    #[test]
    fn is_subscriber_false_by_default() {
        let state = ConnectionState::new(1, "x".to_string(), false);
        assert!(!state.is_subscriber());
    }

    #[test]
    fn is_subscriber_true_when_subscribed() {
        let mut state = ConnectionState::new(1, "x".to_string(), false);
        state.subscription_count = 3;
        assert!(state.is_subscriber());
    }

    #[test]
    fn is_in_transaction_reflects_multi() {
        let mut state = ConnectionState::new(1, "x".to_string(), false);
        assert!(!state.is_in_transaction());
        state.in_multi = true;
        assert!(state.is_in_transaction());
    }
}
