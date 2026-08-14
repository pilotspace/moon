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
use crate::persistence::aof::AofWriterPool;
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
/// `ReplicationState` no longer uses this (task #70 — migrated to
/// `parking_lot::RwLock`, see `repl_state` below); it remains in use for
/// `acl_table` and `cluster_state`, which are out of scope for that migration.
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
    /// AOF writer pool — the **sole AOF interface** after the 2d/2e migration
    /// sequence. Built by spawn sites in `shard/conn_accept.rs` from the
    /// on-disk manifest layout: TopLevel wraps a single shared writer,
    /// PerShard owns one sender per shard. `try_send_append(shard_id, bytes)`
    /// routes to the owning shard; `try_send_rewrite(msg)` rejects under
    /// PerShard until per-shard rewrite ships (step 6 of the RFC).
    pub aof_pool: Option<Arc<AofWriterPool>>,
    pub tracking_table: std::sync::Arc<parking_lot::Mutex<TrackingTable>>,
    pub repl_state: Option<Arc<parking_lot::RwLock<crate::replication::state::ReplicationState>>>,
    /// Lock-free mirror of `repl_state.role == Replica { .. }`.
    /// Populated once in `new()` (from `repl_state.read()`), kept in sync
    /// thereafter by `ReplicationState::set_role()`. Read on every command
    /// dispatch by `try_enforce_readonly` to avoid the per-command RwLock CAS.
    /// `None` when replication is disabled entirely.
    pub is_replica_mirror: Option<Arc<std::sync::atomic::AtomicBool>>,
    pub cluster_state: Option<Arc<parking_lot::RwLock<crate::cluster::ClusterState>>>,
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
    /// Listener-side IP hint table. Stores BOTH key-access and pub/sub hints
    /// per client IP; the listener prefers the key-access hint (storage
    /// locality) and falls back to pub/sub (fan-out locality). Populated by
    /// pub/sub `SUBSCRIBE` and by per-connection `AffinityTracker` migration
    /// decisions when a connection converges ≥10/16 ops on a remote shard.
    pub pubsub_affinity: Arc<parking_lot::RwLock<crate::shard::affinity::AffinityTracker>>,
    // Used by the monoio handler's tiered-storage eviction path AND by both
    // handlers' `FunctionRegistry::new` construction site (Gap B) to build
    // the real `LuaEvictionCtx` for FCALL-internal writes.
    pub spill_sender: Option<flume::Sender<crate::storage::tiered::spill_thread::SpillRequest>>,
    pub spill_file_id: Rc<std::cell::Cell<u64>>,
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
        aof_pool: Option<Arc<AofWriterPool>>,
        tracking_table: std::sync::Arc<parking_lot::Mutex<TrackingTable>>,
        repl_state: Option<Arc<parking_lot::RwLock<crate::replication::state::ReplicationState>>>,
        cluster_state: Option<Arc<parking_lot::RwLock<crate::cluster::ClusterState>>>,
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
        // Snapshot the lock-free is_replica mirror from ReplicationState so
        // try_enforce_readonly can avoid taking the RwLock per command.
        // The Arc is cloned out under the read-lock once at connection setup;
        // ReplicationState::set_role() updates the same AtomicBool thereafter.
        let is_replica_mirror = repl_state
            .as_ref()
            .map(|rs| rs.read().is_replica_mirror.clone());
        Self {
            shard_databases,
            shard_id,
            num_shards,
            pubsub_registry,
            blocking_registry,
            requirepass,
            aof_pool,
            tracking_table,
            repl_state,
            is_replica_mirror,
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

    /// Build the eviction context for FCALL-internal `redis.call` writes
    /// (Gap B — same OOM gate as EVAL/EVALSHA). Used by the lazy
    /// `FunctionRegistry` init in both connection handlers: constructing this
    /// eagerly per connection cost 6 Arc/Rc clones for the >99% of
    /// connections that never call FUNCTION/FCALL.
    pub fn build_lua_eviction_ctx(&self) -> crate::scripting::bridge::LuaEvictionCtx {
        crate::scripting::bridge::LuaEvictionCtx::new(
            self.shard_databases.clone(),
            self.runtime_config.clone(),
            self.shard_id,
            self.spill_sender.clone(),
            self.spill_file_id.clone(),
            self.disk_offload_dir.clone(),
            self.num_shards,
            self.repl_state.clone(),
            self.aof_pool.clone(),
        )
    }
}

/// Get-or-init helper for the per-connection lazy `FunctionRegistry` slot.
/// Returns after guaranteeing the slot is `Some`.
pub(crate) fn ensure_function_registry(
    slot: &RefCell<Option<crate::scripting::FunctionRegistry>>,
    ctx: &ConnectionContext,
) {
    if slot.borrow().is_none() {
        *slot.borrow_mut() = Some(crate::scripting::FunctionRegistry::new(
            ctx.build_lua_eviction_ctx(),
        ));
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
    /// Per-connection READONLY flag (`READONLY` sets, `READWRITE` clears).
    ///
    /// Lets a replica serve READS for slots its master owns instead of
    /// redirecting. Writes are unaffected and still answer MOVED — the
    /// asymmetry is the whole point of the verb, and a "just return +OK"
    /// implementation is what gets it wrong.
    pub readonly: bool,
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

    // MONITOR. Separate from the pub/sub channel because a connection can be a
    // monitor without being a subscriber, and Redis's rules for the two modes
    // differ (a monitor may not touch the keyspace; a subscriber may).
    pub monitor_attached: bool,
    pub monitor_rx: Option<channel::MpscReceiver<bytes::Bytes>>,

    // Transaction (MULTI/EXEC)
    pub in_multi: bool,
    /// Active cross-store transaction (None if not in transaction).
    /// Mutually exclusive with in_multi (MULTI/EXEC is KV-only).
    /// Boxed (c10k W2): CrossStoreTxn is ~2.2 KB of inline SmallVecs which
    /// otherwise sits in EVERY connection's task future, transacting or not
    /// (tmp/C10K-REVIEW.md §2). TXN.BEGIN (cold path) pays one heap alloc.
    pub active_cross_txn: Option<Box<CrossStoreTxn>>,
    /// Active workspace binding for this connection (None = no workspace context).
    /// Set by WS.AUTH, cleared on connection drop.
    pub workspace_id: Option<WorkspaceId>,
    pub command_queue: Vec<Frame>,
    /// The open transaction hit a QUEUE-TIME fault (unknown command, wrong
    /// arity, an unqueueable verb) and must be refused wholesale at EXEC.
    ///
    /// Redis's `CLIENT_DIRTY_EXEC`. Without it Moon ran the half of a
    /// transaction that happened to parse: `MULTI / NOSUCHCMD / SET k v /
    /// EXEC` left `k` set, where Redis discards everything.
    ///
    /// Connection-local by construction, so shard count cannot affect it.
    /// MUST be cleared on every exit from MULTI state — EXEC, DISCARD, RESET.
    /// A leaked flag silently aborts an innocent later transaction, which is
    /// a worse bug than the one it fixes.
    pub multi_dirty: bool,

    /// c1M P1: this connection has issued REPLCONF — it is (almost
    /// certainly) a replica mid-handshake that will send PSYNC next, and
    /// the resumed-parked path does not support the PSYNC hijack. Such
    /// connections never task-park (sticky for the connection's lifetime;
    /// health-checker probes that send a bare REPLCONF are short-lived, so
    /// keeping them unparked costs nothing).
    #[allow(dead_code)] // Read only by the monoio handler's park predicate
    pub saw_replconf: bool,

    // Tracking
    pub tracking_state: TrackingState,
    pub tracking_rx: Option<channel::MpscReceiver<Frame>>,

    // WATCH/EXEC optimistic locking. Read by all three dispatch paths — the
    // `handler_single only` note and its dead_code allow were accurate right up
    // until they described the bug: the two production handlers parsed WATCH,
    // answered +OK, and never looked at this map again.
    pub watched_keys: HashMap<Bytes, crate::server::conn::shared::WatchToken>,

    // Connection affinity (migration)
    pub affinity_tracker: Option<AffinityTracker>,
    pub migration_target: Option<usize>,

    /// Per-connection command counter used for 1-in-N latency sampling on the
    /// hot dispatch path. Wraps on overflow. Sampling avoids the ~30–40 ns
    /// `Instant::now()` tax per command while still producing statistically
    /// accurate latency histograms. Slowlog coverage degrades to 1/16 but
    /// only matters when threshold <~ expected per-op latency; default 10 ms
    /// threshold effectively never fires on pipelined workloads regardless.
    pub cmd_counter: u32,

    /// Cached Prometheus metric handles for the most recently executed
    /// command on this connection. A cache hit skips the recorder backend's
    /// DashMap lookup (~6% of shard CPU on SET p=64 per flamegraph).
    pub cached_metrics: crate::admin::metrics_setup::CachedMetricsHandles,
}

impl ConnectionContext {
    /// The server-side address clients on this listener connected to, for
    /// `CLIENT INFO`/`CLIENT LIST`'s `laddr` field.
    ///
    /// Derived from the configured bind + port rather than the socket's real
    /// `local_addr()`: the monoio handler's stream is a generic `S` with no
    /// such method, and reporting a DIFFERENT laddr per runtime would recreate
    /// the per-path divergence this task exists to remove. A wildcard bind is
    /// rendered as loopback, matching the existing convention at
    /// `handler_monoio/dispatch.rs`. Known limit: a TLS connection is reported
    /// against the plain port.
    pub fn local_addr_string(&self) -> String {
        let host = match self.config.bind.as_str() {
            "0.0.0.0" | "::" | "*" | "" => "127.0.0.1",
            other => other,
        };
        format!("{host}:{}", self.config_port)
    }
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
            readonly: false,
            acl_log: AclLog::new(acl_max_len),
            subscription_count: 0,
            subscriber_id: 0,
            pubsub_tx: None,
            pubsub_rx: None,
            monitor_attached: false,
            monitor_rx: None,
            in_multi: false,
            active_cross_txn: None,
            workspace_id: migrated.and_then(|s| s.workspace_id),
            command_queue: Vec::new(),
            multi_dirty: false,
            // Not carried through MigratedConnectionState: a conn that sent
            // REPLCONF never parks (so never rehydrates through this path),
            // and migration of a mid-handshake replica is not a supported
            // flow (PSYNC hijacks before migration sampling matters).
            saw_replconf: false,
            tracking_state: TrackingState::default(),
            tracking_rx: None,
            watched_keys: HashMap::new(),
            affinity_tracker: if num_shards > 1 && can_migrate {
                Some(AffinityTracker::new(shard_id, num_shards))
            } else {
                None
            },
            migration_target: None,
            cmd_counter: 0,
            cached_metrics: crate::admin::metrics_setup::CachedMetricsHandles::new(),
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

    /// Adopt a new authenticated identity — the ONLY way to change
    /// `current_user`.
    ///
    /// c10k hardening B3. Beyond the ACL cache refresh, this publishes the
    /// new username to the client registry. `register` captures `user` once,
    /// at accept time, when it is always `default`; every AUTH/HELLO success
    /// used to update only the connection-local copy. The registry's copy is
    /// what `CLIENT LIST` reports and what `CLIENT KILL USER <name>` matches
    /// on, so both lied about every authenticated session: `CLIENT LIST`
    /// showed `user=default` for everyone and `CLIENT KILL USER alice`
    /// returned 0 — the primary incident-response lever for a compromised
    /// credential, inert. It is also what makes revocation reachable, since
    /// dropping a user from the table cannot by itself close their live
    /// sessions.
    ///
    /// The registry write takes a stripe lock, which is fine here: AUTH and
    /// HELLO are per-session events, never the steady-state batch loop.
    pub fn adopt_user(&mut self, username: String, acl_table: &StdRwLock<crate::acl::AclTable>) {
        self.current_user = username;
        self.refresh_acl_cache(acl_table);
        let user = self.current_user.clone();
        crate::client_registry::update(self.client_id, move |e| e.user = user);
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

    /// D4 (#438): whether this connection may migrate to another shard
    /// RIGHT NOW. `MigratedConnectionState` carries none of the state
    /// checked here (queued MULTI txn, cross-store txn, subscriptions,
    /// CLIENT TRACKING registration, replica handshake), so migrating
    /// while any of it is live silently discards it. Evaluated at BOTH
    /// the affinity-sampler latch point and the batch-end execution
    /// point — commands later in the same batch can flip any of these
    /// after the latch, so the execution-point check is authoritative.
    /// An ineligible batch end keeps `migration_target` latched; the
    /// migration runs at the first batch end where the connection is
    /// clean again (e.g. after EXEC/UNSUBSCRIBE).
    #[inline]
    pub fn migration_eligible(&self) -> bool {
        !self.in_multi
            && self.active_cross_txn.is_none()
            && self.subscription_count == 0
            && !self.tracking_state.enabled
            && !self.saw_replconf
            // A monitor's registration is process-global and keyed by client
            // id. Migration returns from the handler through its OWN path,
            // before the disconnect detach block runs, so a migrated monitor
            // would leave a dead sink registered forever — which also pins
            // `any_attached()` true and holds the inline fast path down for the
            // life of the process. Excluding monitors keeps every teardown on
            // the paths that actually detach; a monitor connection is a
            // diagnostic session, not something worth migrating.
            && !self.monitor_attached
    }

    /// Get the active transaction's ID, if any.
    #[inline]
    #[allow(dead_code)] // API reserved for future handler-level TXN integration
    pub fn cross_txn_id(&self) -> Option<u64> {
        self.active_cross_txn.as_ref().map(|t| t.txn_id)
    }

    /// Get the active transaction's snapshot LSN, if any.
    #[inline]
    #[allow(dead_code)] // API reserved for future handler-level TXN integration
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
