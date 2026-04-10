//! Connection state machine types shared across all handlers.
//!
//! `ConnectionContext` bundles the immutable per-shard references that every connection
//! handler needs (databases, pubsub, ACL, config, etc.). Created once per shard and
//! passed by reference to each connection handler.
//!
//! `ConnectionState` bundles per-connection mutable state (selected_db, auth, pubsub,
//! tracking, transactions, etc.). Initialized fresh per connection or restored from
//! `MigratedConnectionState`.
//!
//! Phase 4 (future): Extract shared command routing into ConnectionCore::dispatch().

use bytes::Bytes;
use ringbuf::HeapProd;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use crate::acl::{AclLog, AclTable};
use crate::blocking::BlockingRegistry;
use crate::config::{RuntimeConfig, ServerConfig};
use crate::persistence::aof::AofMessage;
use crate::protocol::Frame;
use crate::pubsub::PubSubRegistry;
use crate::runtime::channel;
use crate::shard::dispatch::ShardMessage;
use crate::shard::shared_databases::ShardDatabases;
use crate::storage::entry::CachedClock;
use crate::tracking::{TrackingState, TrackingTable};

use super::affinity::{AffinityTracker, MigratedConnectionState};

/// Type alias for std::sync::RwLock to distinguish from parking_lot::RwLock.
pub(crate) type StdRwLock<T> = std::sync::RwLock<T>;

/// Immutable context shared across all connections on a shard.
///
/// Created once per shard and passed by reference to each connection handler.
/// Nothing in this struct is mutated during connection lifetime.
pub(crate) struct ConnectionContext {
    pub shard_databases: Arc<ShardDatabases>,
    pub shard_id: usize,
    pub num_shards: usize,
    pub pubsub_registry: Arc<parking_lot::RwLock<PubSubRegistry>>,
    pub blocking_registry: Rc<RefCell<BlockingRegistry>>,
    pub requirepass: Option<String>,
    pub aof_tx: Option<channel::MpscSender<AofMessage>>,
    pub tracking_table: Rc<RefCell<TrackingTable>>,
    pub repl_state: Option<Arc<StdRwLock<crate::replication::state::ReplicationState>>>,
    pub cluster_state: Option<Arc<StdRwLock<crate::cluster::ClusterState>>>,
    pub lua: Rc<mlua::Lua>,
    pub script_cache: Rc<RefCell<crate::scripting::ScriptCache>>,
    pub config_port: u16,
    pub acl_table: Arc<StdRwLock<AclTable>>,
    pub runtime_config: Arc<parking_lot::RwLock<RuntimeConfig>>,
    pub config: Arc<ServerConfig>,
    pub dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    pub spsc_notifiers: Vec<Arc<channel::Notify>>,
    pub snapshot_trigger_tx: channel::WatchSender<u64>,
    pub cached_clock: CachedClock,
    pub remote_subscriber_map:
        Arc<parking_lot::RwLock<crate::shard::remote_subscriber_map::RemoteSubscriberMap>>,
    pub all_pubsub_registries: Vec<Arc<parking_lot::RwLock<PubSubRegistry>>>,
    pub all_remote_sub_maps:
        Vec<Arc<parking_lot::RwLock<crate::shard::remote_subscriber_map::RemoteSubscriberMap>>>,
    pub pubsub_affinity: Arc<parking_lot::RwLock<crate::shard::affinity::AffinityTracker>>,
    #[allow(dead_code)] // Only used by monoio handler (tiered storage)
    pub spill_sender: Option<flume::Sender<crate::storage::tiered::spill_thread::SpillRequest>>,
    #[allow(dead_code)] // Only used by monoio handler (tiered storage)
    pub spill_file_id: Rc<std::cell::Cell<u64>>,
    #[allow(dead_code)] // Only used by monoio handler (tiered storage)
    pub disk_offload_dir: Option<std::path::PathBuf>,
}

impl ConnectionContext {
    /// Construct a new ConnectionContext from all required fields.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shard_databases: Arc<ShardDatabases>,
        shard_id: usize,
        num_shards: usize,
        pubsub_registry: Arc<parking_lot::RwLock<PubSubRegistry>>,
        blocking_registry: Rc<RefCell<BlockingRegistry>>,
        requirepass: Option<String>,
        aof_tx: Option<channel::MpscSender<AofMessage>>,
        tracking_table: Rc<RefCell<TrackingTable>>,
        repl_state: Option<Arc<StdRwLock<crate::replication::state::ReplicationState>>>,
        cluster_state: Option<Arc<StdRwLock<crate::cluster::ClusterState>>>,
        lua: Rc<mlua::Lua>,
        script_cache: Rc<RefCell<crate::scripting::ScriptCache>>,
        config_port: u16,
        acl_table: Arc<StdRwLock<AclTable>>,
        runtime_config: Arc<parking_lot::RwLock<RuntimeConfig>>,
        config: Arc<ServerConfig>,
        dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
        spsc_notifiers: Vec<Arc<channel::Notify>>,
        snapshot_trigger_tx: channel::WatchSender<u64>,
        cached_clock: CachedClock,
        remote_subscriber_map: Arc<
            parking_lot::RwLock<crate::shard::remote_subscriber_map::RemoteSubscriberMap>,
        >,
        all_pubsub_registries: Vec<Arc<parking_lot::RwLock<PubSubRegistry>>>,
        all_remote_sub_maps: Vec<
            Arc<parking_lot::RwLock<crate::shard::remote_subscriber_map::RemoteSubscriberMap>>,
        >,
        pubsub_affinity: Arc<parking_lot::RwLock<crate::shard::affinity::AffinityTracker>>,
        spill_sender: Option<flume::Sender<crate::storage::tiered::spill_thread::SpillRequest>>,
        spill_file_id: Rc<std::cell::Cell<u64>>,
        disk_offload_dir: Option<std::path::PathBuf>,
    ) -> Self {
        Self {
            shard_databases,
            shard_id,
            num_shards,
            pubsub_registry,
            blocking_registry,
            requirepass,
            aof_tx,
            tracking_table,
            repl_state,
            cluster_state,
            lua,
            script_cache,
            config_port,
            acl_table,
            runtime_config,
            config,
            dispatch_tx,
            spsc_notifiers,
            snapshot_trigger_tx,
            cached_clock,
            remote_subscriber_map,
            all_pubsub_registries,
            all_remote_sub_maps,
            pubsub_affinity,
            spill_sender,
            spill_file_id,
            disk_offload_dir,
        }
    }
}

/// Per-connection mutable state.
///
/// Initialized fresh per connection (or restored from `MigratedConnectionState`).
/// Bundling these fields eliminates the 15+ local variables at the top of each handler.
pub(crate) struct ConnectionState {
    #[allow(dead_code)] // Used in Phase 4 (shared dispatch extraction)
    pub client_id: u64,
    #[allow(dead_code)] // Used in Phase 4 (shared dispatch extraction)
    pub peer_addr: String,
    pub protocol_version: u8,
    pub selected_db: usize,
    pub authenticated: bool,
    pub current_user: String,
    pub client_name: Option<Bytes>,
    pub asking: bool,
    pub acl_log: AclLog,

    // Pub/Sub
    pub subscription_count: usize,
    pub subscriber_id: u64,
    pub pubsub_tx: Option<channel::MpscSender<bytes::Bytes>>,
    pub pubsub_rx: Option<channel::MpscReceiver<bytes::Bytes>>,

    // Transaction (MULTI/EXEC)
    pub in_multi: bool,
    pub command_queue: Vec<Frame>,

    // Tracking
    pub tracking_state: TrackingState,
    pub tracking_rx: Option<channel::MpscReceiver<Frame>>,

    // WATCH/EXEC optimistic locking (handler_single only)
    #[allow(dead_code)] // Only used by handler_single (tokio feature)
    pub watched_keys: HashMap<Bytes, u32>,

    // Connection affinity (migration)
    pub affinity_tracker: Option<AffinityTracker>,
    pub migration_target: Option<usize>,
}

impl ConnectionState {
    /// Create fresh connection state for a new client.
    pub fn new(
        client_id: u64,
        peer_addr: String,
        requirepass: &Option<String>,
        shard_id: usize,
        num_shards: usize,
        can_migrate: bool,
        acl_max_len: usize,
        migrated: Option<&MigratedConnectionState>,
    ) -> Self {
        let (protocol_version, selected_db, authenticated, current_user, client_name) =
            super::restore_migrated_state(migrated, requirepass);

        Self {
            client_id,
            peer_addr,
            protocol_version,
            selected_db,
            authenticated,
            current_user,
            client_name,
            asking: false,
            acl_log: AclLog::new(acl_max_len),
            subscription_count: 0,
            subscriber_id: 0,
            pubsub_tx: None,
            pubsub_rx: None,
            in_multi: false,
            command_queue: Vec::new(),
            tracking_state: TrackingState::default(),
            tracking_rx: None,
            watched_keys: HashMap::new(),
            affinity_tracker: if num_shards > 1 && can_migrate {
                Some(AffinityTracker::new(shard_id, num_shards))
            } else {
                None
            },
            migration_target: None,
        }
    }
}

/// Action returned by ConnectionCore's command processing.
///
/// Handlers translate these into runtime-specific I/O operations.
#[allow(dead_code)] // Reserved for Phase 4 (shared dispatch extraction)
pub(crate) enum CoreAction {
    /// Write response frame(s) to the client.
    Respond(Vec<Frame>),
    /// Close the connection (QUIT or fatal error).
    Close,
    /// Migrate connection to a different shard.
    Migrate { target_shard: usize },
}
