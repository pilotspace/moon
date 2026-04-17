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
use crate::transaction::CrossStoreTxn;
use crate::workspace::WorkspaceId;

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

    /// Cached per-connection: true when the current user has no ACL
    /// restrictions at all (default `on nopass ~* &* +@all`).  Checked on
    /// the command hot-path to skip the RwLock + HashMap probe on
    /// `AclTable` for unrestricted users.
    ///
    /// The cache is valid only when `cached_acl_version` matches the
    /// current `AclTable::version()`.  Runtime ACL mutations (ACL SETUSER /
    /// DELUSER / LOAD) bump the shared atomic, invalidating this flag on
    /// the next command.  Without that staleness check the cache would let
    /// an in-flight connection keep bypassing permission checks after its
    /// user's privileges were revoked.
    pub cached_acl_unrestricted: bool,

    /// Snapshot of `AclTable::version()` at the time the unrestricted flag
    /// above was computed.  Compared against
    /// `acl_version_handle.load(Acquire)` in the hot path to detect
    /// runtime ACL mutations that invalidate the cache.
    pub cached_acl_version: u64,

    /// Shared handle to `AclTable`'s atomic version counter.  Cloned from
    /// `AclTable::version_handle()` during `refresh_acl_cache`; the
    /// pointer stays stable across ACL LOAD because the table uses
    /// `replace_with` to preserve the counter's identity.
    pub acl_version_handle: Arc<std::sync::atomic::AtomicU64>,

    // Pub/Sub
    pub subscription_count: usize,
    pub subscriber_id: u64,
    pub pubsub_tx: Option<channel::MpscSender<bytes::Bytes>>,
    pub pubsub_rx: Option<channel::MpscReceiver<bytes::Bytes>>,

    // Transaction (MULTI/EXEC)
    pub in_multi: bool,
    /// Active cross-store transaction (None if not in transaction).
    /// Mutually exclusive with in_multi (MULTI/EXEC is KV-only).
    pub active_cross_txn: Option<CrossStoreTxn>,
    /// Active workspace binding for this connection (None = no workspace context).
    /// Set by WS.AUTH, cleared on connection drop.
    pub workspace_id: Option<WorkspaceId>,
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
            active_cross_txn: None,
            workspace_id: migrated.and_then(|s| s.workspace_id),
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
            cached_acl_unrestricted: false,
            cached_acl_version: 0,
            // Placeholder handle — `refresh_acl_cache` replaces this with
            // the authoritative `Arc<AtomicU64>` on first call (which is
            // invoked unconditionally at connection accept time).  The
            // initial counter is 0 so a missed refresh would compare equal
            // to the placeholder and bypass the lock-free staleness check;
            // the first `refresh_acl_cache()` call eliminates that window.
            acl_version_handle: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Resolve and cache the unrestricted flag from the AclTable.
    /// Called once on connection init and after AUTH / HELLO.
    ///
    /// The lock-free staleness-check path in the handlers relies on
    /// `acl_version_handle` pointing at the table's real counter, so this
    /// function always refreshes the handle (cheap Arc clone).  Reading
    /// the handle and the user data in the same critical section ensures
    /// the snapshot stays consistent: any mutator bumps the version only
    /// after releasing the write lock via Drop, so we cannot observe a
    /// post-mutation version with pre-mutation user data.
    #[inline]
    pub fn refresh_acl_cache(&mut self, acl_table: &StdRwLock<crate::acl::AclTable>) {
        // std RwLock: poison = prior panic = unrecoverable. Same convention
        // used throughout the server for the acl_table lock.
        #[allow(clippy::unwrap_used)]
        let guard = acl_table.read().unwrap();
        self.acl_version_handle = guard.version_handle();
        self.cached_acl_unrestricted = guard.is_user_unrestricted(&self.current_user);
        self.cached_acl_version = guard.version();
    }

    /// Lock-free check: is the cached unrestricted flag still valid?
    ///
    /// Returns true iff the ACL table has NOT mutated since the last
    /// `refresh_acl_cache`.  Readers combine this with
    /// `cached_acl_unrestricted` via [`Self::acl_skip_allowed`] to decide
    /// whether they may skip the normal ACL permission check.
    #[inline]
    pub fn acl_cache_fresh(&self) -> bool {
        self.acl_version_handle
            .load(std::sync::atomic::Ordering::Acquire)
            == self.cached_acl_version
    }

    /// Hot-path gate: returns `true` when this connection's current user
    /// is provably unrestricted AND no ACL mutation has occurred since the
    /// cache was populated.  Callers may skip the command/key permission
    /// check when this returns `true`.
    ///
    /// Both conditions are required — a stale cache saying "unrestricted"
    /// would be a privilege-escalation bug if ACL SETUSER has since
    /// revoked the user's permissions.
    #[inline]
    pub fn acl_skip_allowed(&self) -> bool {
        self.cached_acl_unrestricted && self.acl_cache_fresh()
    }

    /// Check if connection is bound to a workspace.
    #[inline]
    #[allow(dead_code)] // Used once WS.* handler intercepts are wired (Plan 02/03)
    pub fn in_workspace(&self) -> bool {
        self.workspace_id.is_some()
    }

    /// Check if connection is in a cross-store transaction.
    #[inline]
    pub fn in_cross_txn(&self) -> bool {
        self.active_cross_txn.is_some()
    }

    /// Get the active transaction's ID, if any.
    #[inline]
    pub fn cross_txn_id(&self) -> Option<u64> {
        self.active_cross_txn.as_ref().map(|t| t.txn_id)
    }

    /// Get the active transaction's snapshot LSN, if any.
    #[inline]
    pub fn cross_txn_snapshot(&self) -> Option<u64> {
        self.active_cross_txn.as_ref().map(|t| t.snapshot_lsn)
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
