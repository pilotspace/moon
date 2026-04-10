//! Connection state machine types for the ConnectionCore extraction (HYGIENE-02).
//!
//! This module defines the shared types that will replace the 40+ parameter lists
//! in handler_monoio, handler_sharded, and handler_single. The extraction proceeds
//! in phases:
//!
//! Phase 1 (this file): Define types. No handler changes yet.
//! Phase 2: Migrate handler_single to use ConnectionContext + ConnectionState.
//! Phase 3: Migrate handler_sharded and handler_monoio.
//! Phase 4: Extract shared command routing into ConnectionCore::dispatch().

use bytes::Bytes;
use ringbuf::HeapProd;
use std::cell::RefCell;
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
#[allow(dead_code)]
type StdRwLock<T> = std::sync::RwLock<T>;

/// Immutable context shared across all connections on a shard.
///
/// Created once per shard and passed by reference to each connection handler.
/// Nothing in this struct is mutated during connection lifetime.
#[allow(dead_code)]
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
    pub spill_sender: Option<flume::Sender<crate::storage::tiered::spill_thread::SpillRequest>>,
    pub spill_file_id: Rc<std::cell::Cell<u64>>,
    pub disk_offload_dir: Option<std::path::PathBuf>,
}

/// Per-connection mutable state.
///
/// Initialized fresh per connection (or restored from `MigratedConnectionState`).
/// Bundling these fields eliminates the 15+ local variables at the top of each handler.
#[allow(dead_code)]
pub(crate) struct ConnectionState {
    pub client_id: u64,
    pub peer_addr: String,
    pub protocol_version: u8,
    pub selected_db: usize,
    pub authenticated: bool,
    pub current_user: Bytes,
    pub client_name: Option<Bytes>,
    pub asking: bool,
    pub acl_log: AclLog,

    // Pub/Sub
    pub subscription_count: usize,
    pub subscriber_id: u64,
    pub pubsub_tx: Option<channel::MpscSender<bytes::Bytes>>,
    pub pubsub_rx: Option<channel::MpscReceiver<bytes::Bytes>>,

    // Functions API
    pub func_registry: Rc<RefCell<crate::scripting::FunctionRegistry>>,

    // Transaction (MULTI/EXEC)
    pub in_multi: bool,
    pub command_queue: Vec<Frame>,

    // Tracking
    pub tracking_state: TrackingState,
    pub tracking_rx: Option<channel::MpscReceiver<Frame>>,

    // Connection affinity (migration)
    pub affinity_tracker: Option<AffinityTracker>,
    pub migration_target: Option<usize>,
}

impl ConnectionState {
    /// Create fresh connection state for a new client.
    #[allow(dead_code)]
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
            current_user: current_user.into(),
            client_name,
            asking: false,
            acl_log: AclLog::new(acl_max_len),
            subscription_count: 0,
            subscriber_id: 0,
            pubsub_tx: None,
            pubsub_rx: None,
            func_registry: Rc::new(RefCell::new(crate::scripting::FunctionRegistry::new())),
            in_multi: false,
            command_queue: Vec::new(),
            tracking_state: TrackingState::default(),
            tracking_rx: None,
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
#[allow(dead_code)]
pub(crate) enum CoreAction {
    /// Write response frame(s) to the client.
    Respond(Vec<Frame>),
    /// Close the connection (QUIT or fatal error).
    Close,
    /// Migrate connection to a different shard.
    Migrate { target_shard: usize },
}
