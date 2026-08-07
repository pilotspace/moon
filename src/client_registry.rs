//! Global client connection registry for CLIENT LIST/INFO/KILL.
//!
//! Every connection registers on accept and deregisters on close.
//! The registry is a 16-way striped `parking_lot::RwLock<HashMap>` (c10k W5;
//! stripe = `id % 16`) touched only on connect/disconnect and CLIENT
//! commands — accepts/closes contend per-stripe, and CLIENT LIST/KILL walk
//! one stripe at a time instead of freezing the whole registry. Per-batch
//! state (db, idle time, flags, kill checks) flows through the lock-free
//! [`ClientLiveState`] handle that `register` returns (QW8, 2026-06 review
//! finding 1.3 — previously the steady-state loop took the global write lock
//! after every pipeline batch and the global read lock for every kill check).

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Instant;

/// Stripe count for the global registry (c10k W5). Power of two, sized so
/// that at 10k conns a stripe holds ~625 entries: register/deregister
/// contention drops 16×, and CLIENT LIST/KILL walks lock ONE stripe at a
/// time instead of stalling every accept/close on all shards behind a
/// single write-preferring RwLock (tmp/C10K-REVIEW.md defect #2).
const STRIPES: usize = 16;

/// Global client registry, striped by `id % STRIPES`.
static REGISTRY: LazyLock<[RwLock<HashMap<u64, ClientEntry>>; STRIPES]> =
    LazyLock::new(|| std::array::from_fn(|_| RwLock::new(HashMap::new())));

/// Live client count across all stripes — pre-sizes CLIENT LIST output
/// without locking anything.
static TOTAL_CLIENTS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// The stripe owning client `id`.
#[inline]
fn stripe(id: u64) -> &'static RwLock<HashMap<u64, ClientEntry>> {
    &REGISTRY[(id as usize) % STRIPES]
}

/// Per-shard live connection counts (c10k W8). Readable — unlike the
/// metrics gauge — so the listener's IP-affinity funnel can refuse to pile
/// further connections onto an already-overloaded shard. Initialized once
/// at startup; when unset (unit tests, embedded), the load gate is inert.
static SHARD_CONNS: std::sync::OnceLock<Box<[AtomicUsize]>> = std::sync::OnceLock::new();

/// Initialize the per-shard connection counters. Called once from startup
/// with the resolved shard count; later calls are no-ops.
pub fn init_shard_conn_counts(num_shards: usize) {
    let _ = SHARD_CONNS.set(
        (0..num_shards)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    );
}

#[inline]
fn shard_conns_delta(shard: usize, add: bool) {
    if let Some(counts) = SHARD_CONNS.get()
        && let Some(c) = counts.get(shard)
    {
        if add {
            c.fetch_add(1, Ordering::Relaxed);
        } else {
            c.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// Is `shard` carrying disproportionate connection load? Used by the
/// listener to gate the IP-affinity hint: one migrated/subscribed client
/// used to pin ALL subsequent central-accepted connections from its IP onto
/// one shard with no load feedback (~2× worst-case skew,
/// tmp/C10K-REVIEW.md defect #5). Threshold: 2× the mean + a small
/// absolute floor so tiny fleets never flap.
pub fn shard_overloaded(shard: usize, num_shards: usize) -> bool {
    let Some(counts) = SHARD_CONNS.get() else {
        return false;
    };
    let Some(c) = counts.get(shard) else {
        return false;
    };
    overload_threshold_exceeded(
        c.load(Ordering::Relaxed),
        TOTAL_CLIENTS.load(Ordering::Relaxed),
        num_shards,
    )
}

/// Pure overload predicate (unit-test surface for the W8 gate).
#[inline]
pub(crate) fn overload_threshold_exceeded(count: usize, total: usize, num_shards: usize) -> bool {
    count > 2 * (total / num_shards.max(1)) + 16
}

/// Lock-free per-connection state, shared between the connection task
/// (writer, once per batch) and CLIENT LIST/INFO/KILL (occasional readers).
pub struct ClientLiveState {
    pub connected_at: Instant,
    /// Epoch ms at registration — baseline for `touch`'s caller-supplied clock.
    pub connected_at_epoch_ms: u64,
    pub db: AtomicUsize,
    /// Milliseconds since `connected_at` of the last completed batch.
    pub last_cmd_ms: AtomicU64,
    /// Bit-packed [`ClientFlags`] (see `ClientFlags::to_bits`).
    pub flags: AtomicU8,
    /// Set by CLIENT KILL — the handler checks this and closes the connection.
    pub kill_flag: AtomicBool,
    /// Raw socket fd for out-of-band force-close (R-1/R-3). `CLIENT KILL` sets
    /// `kill_flag` (cooperative) AND `shutdown(2)`s this fd, so a connection
    /// parked in `read().await` (idle, `timeout 0`) is torn down immediately
    /// instead of waiting until it next sends bytes. `-1` = no fd (tests /
    /// non-unix). Set once at registration; only read by `kill_clients` while
    /// holding the registry lock (see the drop-ordering safety note there).
    pub kill_fd: i32,
    /// Bytes of reply currently held in an in-flight write (c10k C1).
    ///
    /// This is the memory a client that stops reading is pinning: the handler
    /// serializes a whole batch into one buffer before its single write
    /// syscall, so a deep pipeline against a closed receive window parks
    /// hundreds of MB here. `CLIENT LIST` reported `obl=0 oll=0 omem=0`
    /// unconditionally, which made the attack invisible while it happened.
    /// Reported as `obl`/`omem` — moon has one contiguous output buffer, not
    /// Redis's static-buffer + reply-list pair, so `oll` stays 0.
    pub pending_out_bytes: AtomicU64,
    /// Cumulative reply bytes successfully written — `tot-net-out`.
    pub tot_net_out: AtomicU64,
}

impl ClientLiveState {
    /// Record batch-completion state. Three relaxed stores — no lock, and no
    /// clock read: `now_epoch_ms` comes from the caller (the shard-cached
    /// clock on hot paths), keeping `Instant::now()` off the per-batch path
    /// per the timestamp-caching invariant. Up to 1ms of cached-clock
    /// staleness is fine for an idle-time stat; `saturating_sub` guards the
    /// stale-clock-before-connect edge.
    #[inline]
    pub fn touch(&self, db: usize, flags: ClientFlags, now_epoch_ms: u64) {
        self.db.store(db, Ordering::Relaxed);
        self.last_cmd_ms.store(
            now_epoch_ms.saturating_sub(self.connected_at_epoch_ms),
            Ordering::Relaxed,
        );
        self.flags.store(flags.to_bits(), Ordering::Relaxed);
    }

    /// Mark a reply write as in flight. One relaxed store, on a path that is
    /// about to make a syscall — the cost is not measurable there.
    #[inline]
    pub fn begin_write(&self, bytes: usize) {
        self.pending_out_bytes
            .store(bytes as u64, Ordering::Relaxed);
    }

    /// Clear the in-flight marker. `completed` distinguishes a delivered reply
    /// from one abandoned by `--client-write-timeout-ms` or a socket error, so
    /// `tot-net-out` counts only bytes that actually reached the peer.
    #[inline]
    pub fn end_write(&self, bytes: usize, completed: bool) {
        self.pending_out_bytes.store(0, Ordering::Relaxed);
        if completed {
            self.tot_net_out.fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }

    /// Lock-free CLIENT KILL check for the connection's own loop.
    #[inline]
    pub fn is_killed(&self) -> bool {
        self.kill_flag.load(Ordering::Relaxed)
    }

    /// Set or clear the blocked bit, leaving the other flags and the idle
    /// clock alone.
    ///
    /// Only the connection's own task writes `flags`, so the read-modify-write
    /// is not a race — same relaxed, single-writer discipline as [`touch`].
    ///
    /// Needed because every `touch` call site hardcodes `blocked: false`: the
    /// bit existed but was never set, so `CLIENT LIST`'s `b` flag was dead
    /// code, and — the reason this matters now — [`kill_idle_clients`] would
    /// see a client parked in `BLPOP key 0` as idle and close it. Redis
    /// exempts blocked clients from `timeout`, and so did moon implicitly
    /// (a blocked client never reached the old per-connection timeout arm).
    #[inline]
    pub fn set_blocked(&self, blocked: bool) {
        const BLOCKED_BIT: u8 = 1 << 2;
        let cur = self.flags.load(Ordering::Relaxed);
        let next = if blocked {
            cur | BLOCKED_BIT
        } else {
            cur & !BLOCKED_BIT
        };
        self.flags.store(next, Ordering::Relaxed);
    }

    /// Mark the client blocked for the lifetime of the returned guard.
    ///
    /// Prefer this over a manual `set_blocked(true)` / `set_blocked(false)`
    /// pair around an await: the pair leaks the exemption if the await panics
    /// or its future is dropped before completing (connection migration,
    /// task cancellation), and a client stuck with `blocked` set is exempt
    /// from `timeout` FOREVER — the same immortal-connection shape the
    /// timeout sweep exists to prevent.
    #[inline]
    pub fn blocked_guard(self: &Arc<Self>) -> BlockedGuard {
        self.set_blocked(true);
        BlockedGuard {
            live: Arc::clone(self),
        }
    }
}

/// RAII `blocked` flag, cleared on drop however the scope is left.
pub struct BlockedGuard {
    live: Arc<ClientLiveState>,
}

impl Drop for BlockedGuard {
    fn drop(&mut self) {
        self.live.set_blocked(false);
    }
}

/// Information about a connected client.
pub struct ClientEntry {
    pub id: u64,
    pub addr: String,
    pub name: Option<String>,
    pub user: String,
    pub shard: usize,
    pub live: Arc<ClientLiveState>,
}

/// Client connection flags (matches Redis CLIENT LIST flag characters).
#[derive(Clone, Copy, Default)]
pub struct ClientFlags {
    pub subscriber: bool,
    pub in_multi: bool,
    pub blocked: bool,
    /// Replication link (this connection sent REPLCONF/PSYNC).
    ///
    /// Exemption bit only — deliberately NOT surfaced by [`to_flag_str`], so
    /// `CLIENT LIST` output is byte-identical to before. Redis's
    /// `clientsCronHandleTimeout` skips `CLIENT_SLAVE`/`CLIENT_MASTER`, and
    /// [`kill_idle_clients`] needs the same exemption: a replica link is
    /// idle between writes by design and must never be idle-closed.
    pub replica: bool,
}

impl ClientFlags {
    /// Format as Redis-compatible flag string (e.g., "N", "S", "x").
    pub fn to_flag_str(self) -> &'static str {
        if self.subscriber {
            "S"
        } else if self.in_multi {
            "x"
        } else if self.blocked {
            "b"
        } else {
            "N"
        }
    }

    /// Pack into one byte for `ClientLiveState::flags`.
    #[inline]
    pub fn to_bits(self) -> u8 {
        (self.subscriber as u8)
            | ((self.in_multi as u8) << 1)
            | ((self.blocked as u8) << 2)
            | ((self.replica as u8) << 3)
    }

    /// Unpack from `ClientLiveState::flags`.
    #[inline]
    pub fn from_bits(bits: u8) -> Self {
        ClientFlags {
            subscriber: bits & 1 != 0,
            in_multi: bits & 2 != 0,
            blocked: bits & 4 != 0,
            replica: bits & 8 != 0,
        }
    }
}

/// Register a new client connection.
///
/// Returns the connection's lock-free live-state handle; the connection task
/// keeps it for per-batch `touch()` and `is_killed()` without the registry lock.
pub fn register(
    id: u64,
    addr: String,
    user: String,
    shard: usize,
    kill_fd: i32,
) -> Arc<ClientLiveState> {
    let live = Arc::new(ClientLiveState {
        connected_at: Instant::now(),
        connected_at_epoch_ms: crate::storage::entry::current_time_ms(),
        db: AtomicUsize::new(0),
        last_cmd_ms: AtomicU64::new(0),
        pending_out_bytes: AtomicU64::new(0),
        tot_net_out: AtomicU64::new(0),
        flags: AtomicU8::new(ClientFlags::default().to_bits()),
        kill_flag: AtomicBool::new(false),
        kill_fd,
    });
    let entry = ClientEntry {
        id,
        addr,
        name: None,
        user,
        shard,
        live: Arc::clone(&live),
    };
    // #438 conn-secondary: a re-register over a still-present id (the
    // `kept_registration` miss-arm fail-safe firing while the entry somehow
    // survived, or any future double-register bug) must not double-count —
    // the single eventual deregister would then leave TOTAL_CLIENTS and the
    // shard gauges permanently high, skewing `shard_overloaded` routing.
    // Balance against whatever the insert replaced.
    let replaced = stripe(id).write().insert(id, entry);
    if let Some(old) = replaced {
        tracing::warn!(
            "client registry: id {} re-registered while present (shard {} -> {})",
            id,
            old.shard,
            shard
        );
        shard_conns_delta(old.shard, false);
        crate::admin::metrics_setup::record_shard_connection_delta(old.shard, -1.0);
    } else {
        TOTAL_CLIENTS.fetch_add(1, Ordering::Relaxed);
    }
    shard_conns_delta(shard, true);
    crate::admin::metrics_setup::record_shard_connection_delta(shard, 1.0);
    live
}

/// Fetch the live-state handle for an already-registered client. Used by
/// resumed parked connections (c1M P1) to reuse their registration across a
/// park/wake cycle instead of deregister + re-register — keeping the entry
/// (name, connected_at, counters) continuously intact.
pub fn live_handle(id: u64) -> Option<Arc<ClientLiveState>> {
    stripe(id).read().get(&id).map(|e| Arc::clone(&e.live))
}

/// Deregister a client connection.
pub fn deregister(id: u64) {
    if let Some(entry) = stripe(id).write().remove(&id) {
        TOTAL_CLIENTS.fetch_sub(1, Ordering::Relaxed);
        shard_conns_delta(entry.shard, false);
        crate::admin::metrics_setup::record_shard_connection_delta(entry.shard, -1.0);
    }
}

/// Update mutable fields for a client (CLIENT SETNAME and similar — rare,
/// never the steady-state batch loop; batch state goes through the
/// [`ClientLiveState`] handle instead).
pub fn update<F: FnOnce(&mut ClientEntry)>(id: u64, f: F) {
    if let Some(entry) = stripe(id).write().get_mut(&id) {
        f(entry);
    }
}

/// Check if a client has been marked for killing.
///
/// Registry-lookup variant for code without the live handle; connection
/// loops use `ClientLiveState::is_killed` (lock-free) instead.
pub fn is_killed(id: u64) -> bool {
    stripe(id)
        .read()
        .get(&id)
        .is_some_and(|e| e.live.is_killed())
}

/// Format all clients as a CLIENT LIST string.
///
/// Each line: `id=N addr=... fd=0 name=... db=N ...`
/// Returns the full response string.
pub fn client_list() -> String {
    let now = Instant::now();
    // Pre-size from the lock-free total; walk one stripe at a time so a big
    // listing never blocks register/deregister on the other 15 stripes
    // (c10k W5 — the old single lock held every accept/close hostage for the
    // whole O(N) format). Ordering across stripes is not insertion order;
    // Redis makes no CLIENT LIST ordering guarantee.
    let mut result =
        String::with_capacity(TOTAL_CLIENTS.load(Ordering::Relaxed).saturating_mul(128));
    for lock in REGISTRY.iter() {
        let stripe = lock.read();
        for entry in stripe.values() {
            format_client_line(&mut result, entry, now);
        }
    }
    // Remove trailing newline if present
    if result.ends_with('\n') {
        result.pop();
    }
    result
}

/// Format a single client's info (for CLIENT INFO).
pub fn client_info(id: u64) -> Option<String> {
    let now = Instant::now();
    stripe(id).read().get(&id).map(|entry| {
        let mut result = String::with_capacity(128);
        format_client_line(&mut result, entry, now);
        if result.ends_with('\n') {
            result.pop();
        }
        result
    })
}

/// Kill clients matching the given filter. Returns count of killed clients.
///
/// Sets the cooperative `kill_flag` (checked once per batch by the handler)
/// AND force-closes the socket via `shutdown(2)`, so a connection currently
/// parked in `read().await` — idle with `timeout 0` — is torn down at once
/// rather than lingering until it next sends bytes. The parked read returns
/// `Ok(0)`/`Err`, which the handler already treats as disconnect.
///
/// `self_id` is the id of the connection *executing* CLIENT KILL, if any.
/// A matching self entry gets the cooperative flag only — never the fd
/// shutdown — so the `+count` reply is still delivered before the handler's
/// per-batch kill check closes the connection (Redis replies first on
/// self-kill, then closes).
pub fn kill_clients(filter: &KillFilter, self_id: Option<u64>) -> u64 {
    // Lock ordering / fd-liveness invariant, per stripe (c10k W5): the raw-fd
    // `shutdown` is race-free because a connection's `RegistryGuard` drops —
    // calling `deregister`, which needs its OWN stripe's WRITE lock —
    // strictly before its `stream` drops and closes the fd. So while we hold
    // a stripe's READ lock and an entry is present in it, `deregister` for
    // that entry is blocked, its `stream` has not dropped, and `kill_fd` is
    // still an open socket. The invariant only ever involves one entry and
    // its own stripe, so striping preserves it; entries in other stripes are
    // simply not visited while their lock is free.
    //
    // WHO upholds guard-before-stream (F2, #438): in a handler task it falls
    // out of drop order — the guard is a local, the stream a parameter, and
    // locals drop first. A task-parked connection has NO handler task; its
    // watcher co-owns both as future upvars (drop order = capture order, no
    // language guarantee that helps), so they are wrapped in
    // `conn_accept::ParkedSession`, whose hand-written Drop deregisters
    // before closing. Any future owner of a {guard, stream} pair must
    // preserve this order or wrap in ParkedSession.
    let mut count = 0u64;
    let kill_entry = |entry: &ClientEntry, count: &mut u64| {
        entry.live.kill_flag.store(true, Ordering::Relaxed);
        // Self-kill stays cooperative: shutting down our own socket here
        // would happen mid-command, before the reply is flushed.
        if self_id != Some(entry.id) {
            force_close_fd(entry.live.kill_fd);
        }
        *count += 1;
    };
    match filter {
        // Id filter: O(1) — direct stripe lookup instead of the old full
        // registry scan (the map was always id-keyed).
        KillFilter::Id(target_id) => {
            let guard = stripe(*target_id).read();
            if let Some(entry) = guard.get(target_id) {
                kill_entry(entry, &mut count);
            }
        }
        // Addr/User filters: walk stripes ONE at a time — a broad kill no
        // longer stalls every accept/close globally. A connection that
        // registers into an already-visited stripe mid-walk is missed, the
        // same inherent race the single-lock version had at command
        // granularity.
        KillFilter::Addr(_) | KillFilter::User(_) => {
            for lock in REGISTRY.iter() {
                let guard = lock.read();
                for entry in guard.values() {
                    let matches = match filter {
                        KillFilter::Id(_) => false, // handled by the arm above
                        KillFilter::Addr(addr) => entry.addr == *addr,
                        KillFilter::User(user) => entry.user == *user,
                    };
                    if matches {
                        kill_entry(entry, &mut count);
                    }
                }
            }
        }
    }
    count
}

/// Connections whose handler task has exited and which are held only by a
/// readiness watcher (c1M task-exit parking).
///
/// Exposed as `parked_clients` in `INFO clients`. Operators get to see how
/// much of the connection fleet is actually parked; tests get a direct,
/// non-flaky assertion that parking engaged, instead of inferring it from
/// process RSS.
static PARKED_CLIENTS: AtomicU64 = AtomicU64::new(0);

/// Record a connection entering (`+1`) or leaving (`-1`) task-exit parking.
pub fn parked_delta(entering: bool) {
    if entering {
        PARKED_CLIENTS.fetch_add(1, Ordering::Relaxed);
    } else {
        PARKED_CLIENTS.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Number of connections currently task-parked.
pub fn parked_clients() -> u64 {
    PARKED_CLIENTS.load(Ordering::Relaxed)
}

/// Close connections idle for at least `timeout_secs` — Redis's `timeout`
/// config. Returns the number closed.
///
/// Called once per second from each shard's chore, which visits only the
/// entries it owns (`entry.shard == shard`), so the work is distributed and
/// no shard walks another's connections.
///
/// **Why this is not a per-connection timer (c10k hardening D1).** `timeout`
/// used to be enforced by a `select!` arm racing the connection's idle read
/// against `sleep(timeout)`. That arm sat FIRST in the read loop's if/else
/// chain, which made the entire c1M connection park structurally unreachable
/// whenever `timeout` was set: every connection reverted from the parked
/// ~3.3 KB to its full ~46 KB working set, silently, in exactly the
/// deployments that use the only slowloris knob we ship. Enforcing from the
/// chore instead costs no per-connection timer or allocation, and reaches
/// connections the handler cannot: ones parked in `read()`, and ones whose
/// handler TASK has exited entirely (task-exit parking) and are held only by
/// a readiness watcher. Redis enforces `timeout` from `serverCron` the same
/// way, so enforcement being up to one sweep interval late matches upstream
/// rather than diverging from it.
///
/// Exemptions mirror Redis's `clientsCronHandleTimeout`: subscribers,
/// blocked clients, and replication links are never idle-closed.
pub fn kill_idle_clients(
    shard: usize,
    num_shards: usize,
    timeout_secs: u64,
    now_epoch_ms: u64,
) -> u64 {
    sweep_idle_clients(shard, num_shards, timeout_secs, now_epoch_ms, None)
}

/// Core of [`kill_idle_clients`], with a test-only shard filter.
///
/// The registry is striped by `id % STRIPES`, NOT by shard, so a stripe holds
/// clients belonging to every shard. Each shard therefore sweeps a DISJOINT
/// SUBSET OF STRIPES (`i % num_shards == shard`) rather than the whole
/// registry filtered by `entry.shard`: the latter is what the first version
/// did, and because the chore runs once per shard per second it made the
/// total cost `O(num_shards × total_clients)` per second — 12× the necessary
/// work at `--shards 12`, all of it holding stripe read locks that contend
/// with the write locks `register`/`deregister` need on accept and close.
/// Partitioning keeps the cost at one full pass per second no matter how many
/// shards there are, and genuinely distributes it. Shards ≥ `STRIPES` sweep
/// nothing, which is correct: stripes 0..STRIPES are still fully covered.
///
/// Killing is deliberately shard-agnostic — `kill_flag` is an atomic and
/// `force_close_fd` is a `shutdown(2)`, both safe from any thread. This is
/// exactly what `CLIENT KILL` already does cross-shard.
///
/// `only_shard` is `Some` only from unit tests: the registry is process-global
/// and cargo runs tests in parallel, so each test scopes its assertions to its
/// own synthetic shard id.
fn sweep_idle_clients(
    shard: usize,
    num_shards: usize,
    timeout_secs: u64,
    now_epoch_ms: u64,
    only_shard: Option<usize>,
) -> u64 {
    if timeout_secs == 0 || num_shards == 0 {
        return 0;
    }
    let timeout_ms = timeout_secs.saturating_mul(1000);
    let mut count = 0u64;
    // Same per-stripe fd-liveness invariant as `kill_clients`: while this
    // stripe's READ lock is held and an entry is present, that connection's
    // `deregister` (which needs the stripe's WRITE lock) cannot have run, so
    // its stream has not dropped and `kill_fd` is still an open socket.
    for (idx, lock) in REGISTRY.iter().enumerate() {
        if idx % num_shards != shard {
            continue;
        }
        let guard = lock.read();
        for entry in guard.values() {
            if entry.live.is_killed() {
                continue;
            }
            if only_shard.is_some_and(|s| entry.shard != s) {
                continue;
            }
            let flags = ClientFlags::from_bits(entry.live.flags.load(Ordering::Relaxed));
            if flags.subscriber || flags.blocked || flags.replica {
                continue;
            }
            // `last_cmd_ms` is ms-since-connect of the last completed batch,
            // so a connection that has never completed one reads 0 and is
            // idle since it connected — which is what we want for a client
            // that opens a socket and never speaks.
            let idle_ms = now_epoch_ms
                .saturating_sub(entry.live.connected_at_epoch_ms)
                .saturating_sub(entry.live.last_cmd_ms.load(Ordering::Relaxed));
            if idle_ms >= timeout_ms {
                entry.live.kill_flag.store(true, Ordering::Relaxed);
                force_close_fd(entry.live.kill_fd);
                count += 1;
            }
        }
    }
    count
}

/// `shutdown(SHUT_RDWR)` the given socket fd to wake a parked `read`. No-op for
/// `-1` (no fd) and on non-unix targets (CLIENT KILL stays cooperative there).
#[inline]
fn force_close_fd(fd: i32) {
    #[cfg(unix)]
    if fd >= 0 {
        // Per the lock-ordering note in `kill_clients`, `fd` is a live socket
        // for the duration of this call. We intentionally ignore the return
        // value: an already-closed/half-closed socket is a benign no-op.
        // SAFETY: `libc::shutdown` is an FFI call that cannot cause memory
        // unsafety for any integer argument — an invalid fd merely returns
        // `EBADF`.
        unsafe {
            libc::shutdown(fd, libc::SHUT_RDWR);
        }
    }
    #[cfg(not(unix))]
    let _ = fd;
}

/// Filter for CLIENT KILL.
pub enum KillFilter {
    Id(u64),
    Addr(String),
    User(String),
}

/// Parse CLIENT KILL arguments into a KillFilter.
///
/// Supports both the legacy form (`CLIENT KILL addr:port`) and the modern
/// filter form (`CLIENT KILL ID id`, `CLIENT KILL ADDR addr`, `CLIENT KILL USER user`).
pub fn parse_kill_args(args: &[&[u8]]) -> Option<KillFilter> {
    if args.is_empty() {
        return None;
    }
    // Legacy single-arg form: CLIENT KILL addr:port
    if args.len() == 1 {
        let addr = std::str::from_utf8(args[0]).ok()?;
        return Some(KillFilter::Addr(addr.to_string()));
    }
    // Modern filter form: CLIENT KILL ID|ADDR|USER value
    let mut i = 0;
    while i + 1 < args.len() {
        let key = args[i];
        let val = args[i + 1];
        if key.eq_ignore_ascii_case(b"ID") {
            let id_str = std::str::from_utf8(val).ok()?;
            let id = id_str.parse::<u64>().ok()?;
            return Some(KillFilter::Id(id));
        } else if key.eq_ignore_ascii_case(b"ADDR") {
            let addr = std::str::from_utf8(val).ok()?;
            return Some(KillFilter::Addr(addr.to_string()));
        } else if key.eq_ignore_ascii_case(b"USER") {
            let user = std::str::from_utf8(val).ok()?;
            return Some(KillFilter::User(user.to_string()));
        }
        i += 2;
    }
    None
}

/// Format one CLIENT LIST/INFO line.
///
/// Field set matches real Redis (`clientCommand`/`catClientInfoString`) so
/// tooling that parses CLIENT LIST by key (redis-cli, client libraries,
/// `redis_exporter`) doesn't choke on missing keys. Fields Moon doesn't
/// track yet (`laddr`, `rbs`/`rbp`/`obl`/`oll`/`omem`, `tot-net-in/out`,
/// `cmd`, `events`) are emitted with honest placeholder values (0 / "NULL" /
/// unknown) rather than omitted — see docs/redis-compat.md for the list of
/// fields that are structurally present but not yet semantically populated.
///
/// `redir` and the tracking flag char (`t`) are intentionally left at their
/// "off" defaults (`redir=-1`, never appended to `flags`): PR #234 is adding
/// real CLIENT TRACKING state, and that work owns wiring these two fields to
/// live data. Do not add tracking introspection here — it would conflict.
fn format_client_line(buf: &mut String, entry: &ClientEntry, now: Instant) {
    use std::fmt::Write;
    let live = &*entry.live;
    let age = now.duration_since(live.connected_at).as_secs();
    let last_cmd_secs = live.last_cmd_ms.load(Ordering::Relaxed) / 1000;
    let idle = age.saturating_sub(last_cmd_secs);
    let name = entry.name.as_deref().unwrap_or("");
    let flags = ClientFlags::from_bits(live.flags.load(Ordering::Relaxed)).to_flag_str();
    let db = live.db.load(Ordering::Relaxed);
    // c10k C1: `obl`/`omem` report the reply bytes this connection is holding
    // in an in-flight write. A client that stops reading pins that memory for
    // as long as `--client-write-timeout-ms` allows, and with these hardcoded
    // to 0 there was no way to see it happening. `oll` stays 0: moon has one
    // contiguous output buffer, not Redis's static-buffer + reply-list pair,
    // so there is no list whose length it could report.
    let omem = live.pending_out_bytes.load(Ordering::Relaxed);
    let tot_net_out = live.tot_net_out.load(Ordering::Relaxed);
    let _ = writeln!(
        buf,
        "id={} addr={} laddr=127.0.0.1:0 fd=0 name={} age={} idle={} flags={} db={} \
         sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 \
         tot-net-in=0 tot-net-out={} rbs=1024 rbp=0 obl={} oll=0 omem={} tot-mem={} events=r \
         cmd=NULL user={} redir=-1 resp=2 lib-name= lib-ver=",
        entry.id, entry.addr, name, age, idle, flags, db, tot_net_out, omem, omem, omem, entry.user,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_list() {
        let id = 999_000;
        register(id, "127.0.0.1:12345".into(), "default".into(), 0, -1);
        let list = client_list();
        assert!(list.contains("id=999000"));
        assert!(list.contains("addr=127.0.0.1:12345"));
        assert!(list.contains("user=default"));
        deregister(id);
        let list = client_list();
        assert!(!list.contains("id=999000"));
    }

    #[test]
    fn test_client_info() {
        let id = 999_001;
        register(id, "10.0.0.1:5000".into(), "alice".into(), 1, -1);
        let info = client_info(id);
        assert!(info.is_some());
        assert!(info.as_ref().is_some_and(|s| s.contains("user=alice")));
        deregister(id);
        assert!(client_info(id).is_none());
    }

    /// CLIENT LIST/INFO field-set completeness: every key real Redis emits
    /// (`catClientInfoString`) must be present so key=value parsers (redis-cli,
    /// client libraries, exporters) never choke on a missing field.
    #[test]
    fn test_client_info_field_set_matches_redis() {
        let id = 999_030;
        register(id, "10.0.0.3:7000".into(), "carol".into(), 0, -1);
        let info = client_info(id).expect("registered client has info");
        for field in [
            "id=",
            "addr=",
            "laddr=",
            "fd=",
            "name=",
            "age=",
            "idle=",
            "flags=",
            "db=",
            "sub=",
            "psub=",
            "ssub=",
            "multi=",
            "watch=",
            "qbuf=",
            "qbuf-free=",
            "argv-mem=",
            "multi-mem=",
            "tot-net-in=",
            "tot-net-out=",
            "rbs=",
            "rbp=",
            "obl=",
            "oll=",
            "omem=",
            "tot-mem=",
            "events=",
            "cmd=",
            "user=",
            "redir=",
            "resp=",
            "lib-name=",
            "lib-ver=",
        ] {
            assert!(
                info.contains(field),
                "CLIENT INFO missing field {field:?} in {info:?}"
            );
        }
        deregister(id);
    }

    #[test]
    fn test_kill_by_id() {
        let id = 999_002;
        let live = register(id, "10.0.0.2:6000".into(), "bob".into(), 0, -1);
        assert!(!is_killed(id));
        assert!(!live.is_killed());
        let count = kill_clients(&KillFilter::Id(id), None);
        assert_eq!(count, 1);
        assert!(is_killed(id));
        assert!(live.is_killed(), "live handle observes the kill lock-free");
        deregister(id);
    }

    // R-3: CLIENT KILL must force-close the socket so a connection blocked in
    // read() (idle, `timeout 0`) is torn down immediately, not only on its next
    // byte. We register one end of a socketpair and assert the peer observes EOF
    // after the kill — proving the fd was actually shut down.
    #[cfg(unix)]
    #[test]
    fn test_kill_force_closes_socket_fd() {
        use std::io::Read;
        use std::os::unix::io::AsRawFd;
        use std::os::unix::net::UnixStream;

        let (killed_end, mut peer) = UnixStream::pair().expect("socketpair");
        let id = 999_020;
        let live = register(
            id,
            "unix:socketpair".into(),
            "default".into(),
            0,
            killed_end.as_raw_fd(),
        );

        let count = kill_clients(&KillFilter::Id(id), None);
        assert_eq!(count, 1);
        assert!(live.is_killed());

        // After shutdown(SHUT_RDWR) on killed_end, the peer's read returns 0 (EOF).
        let mut buf = [0u8; 8];
        let n = peer.read(&mut buf).expect("peer read after kill");
        assert_eq!(n, 0, "peer must see EOF once the killed fd is shut down");

        deregister(id);
        drop(killed_end);
    }

    // Self-kill (CLIENT KILL matching the executing connection) must stay
    // cooperative: the fd is NOT shut down, so the `+count` reply can still be
    // flushed; only the kill_flag is set (handler closes after the batch).
    #[cfg(unix)]
    #[test]
    fn test_self_kill_does_not_force_close_socket_fd() {
        use std::io::{Read, Write};
        use std::os::unix::io::AsRawFd;
        use std::os::unix::net::UnixStream;

        let (self_end, mut peer) = UnixStream::pair().expect("socketpair");
        let id = 999_021;
        let live = register(
            id,
            "unix:socketpair-self".into(),
            "default".into(),
            0,
            self_end.as_raw_fd(),
        );

        let count = kill_clients(&KillFilter::Id(id), Some(id));
        assert_eq!(count, 1);
        assert!(live.is_killed(), "cooperative flag must still be set");

        // The socket must remain writable in both directions — the reply to
        // the killer is flushed over exactly this fd after kill_clients runs.
        (&self_end)
            .write_all(b"+1\r\n")
            .expect("self fd must still accept the reply write");
        let mut buf = [0u8; 8];
        let n = peer.read(&mut buf).expect("peer read");
        assert_eq!(n, 4, "peer must receive the reply, not EOF");
        assert_eq!(&buf[..4], b"+1\r\n");

        deregister(id);
        drop(self_end);
    }

    #[test]
    fn test_kill_by_user() {
        let id1 = 999_010;
        let id2 = 999_011;
        register(id1, "10.0.0.3:7000".into(), "eve".into(), 0, -1);
        register(id2, "10.0.0.4:7001".into(), "eve".into(), 1, -1);
        let count = kill_clients(&KillFilter::User("eve".into()), None);
        assert_eq!(count, 2);
        assert!(is_killed(id1));
        assert!(is_killed(id2));
        deregister(id1);
        deregister(id2);
    }

    #[test]
    fn test_update_and_touch() {
        let id = 999_003;
        let live = register(id, "10.0.0.5:8000".into(), "default".into(), 0, -1);
        update(id, |e| {
            e.name = Some("myconn".into());
        });
        live.touch(3, ClientFlags::default(), live.connected_at_epoch_ms);
        let info = client_info(id).unwrap();
        assert!(info.contains("name=myconn"));
        assert!(info.contains("db=3"));
        deregister(id);
    }

    #[test]
    fn test_touch_uses_caller_clock_not_instant_now() {
        let id = 999_004;
        let live = register(id, "10.0.0.6:9000".into(), "default".into(), 0, -1);
        // touch takes the caller's (shard-cached) epoch clock — no per-op
        // Instant::now(). last_cmd_ms stays "ms since connect".
        live.touch(2, ClientFlags::default(), live.connected_at_epoch_ms + 5000);
        assert_eq!(live.last_cmd_ms.load(Ordering::Relaxed), 5000);
        // A cached clock up to 1ms stale can lag the registration timestamp;
        // must saturate to 0, never wrap.
        live.touch(
            2,
            ClientFlags::default(),
            live.connected_at_epoch_ms.saturating_sub(10),
        );
        assert_eq!(live.last_cmd_ms.load(Ordering::Relaxed), 0);
        deregister(id);
    }

    #[test]
    fn test_flags_bits_roundtrip() {
        for bits in 0..8u8 {
            assert_eq!(ClientFlags::from_bits(bits).to_bits(), bits);
        }
    }

    #[test]
    fn test_parse_kill_args() {
        let args: Vec<&[u8]> = vec![b"ID", b"42"];
        let filter = parse_kill_args(&args).unwrap();
        assert!(matches!(filter, KillFilter::Id(42)));

        let args: Vec<&[u8]> = vec![b"ADDR", b"127.0.0.1:6379"];
        let filter = parse_kill_args(&args).unwrap();
        assert!(matches!(filter, KillFilter::Addr(a) if a == "127.0.0.1:6379"));

        let args: Vec<&[u8]> = vec![b"USER", b"alice"];
        let filter = parse_kill_args(&args).unwrap();
        assert!(matches!(filter, KillFilter::User(u) if u == "alice"));
    }
}

#[cfg(test)]
mod striping_tests {
    use super::*;

    /// Ids chosen to span every stripe; registry state is process-global, so
    /// use a distinct id range from the legacy tests (which use small ids).
    const BASE: u64 = 91_000;

    fn register_n(n: u64, user: &str) -> Vec<u64> {
        (0..n)
            .map(|i| {
                let id = BASE + i;
                let _ = register(id, format!("10.0.0.{i}:1234"), user.to_string(), 0, -1);
                id
            })
            .collect()
    }

    #[test]
    fn cross_stripe_list_and_targeted_kill() {
        let ids = register_n(40, "stripe-user");

        // CLIENT LIST must see every entry regardless of stripe.
        let list = client_list();
        for id in &ids {
            assert!(
                list.contains(&format!("id={id} ")),
                "id {id} missing from list"
            );
        }

        // Kill-by-id is a single-stripe O(1) lookup — exactly one victim.
        assert_eq!(kill_clients(&KillFilter::Id(BASE + 17), None), 1);
        assert!(is_killed(BASE + 17));
        assert!(!is_killed(BASE + 18));

        // Kill-by-user walks all stripes and reaches every entry.
        assert_eq!(
            kill_clients(&KillFilter::User("stripe-user".to_string()), None),
            40
        );
        for id in &ids {
            assert!(is_killed(*id));
            deregister(*id);
        }
        assert!(client_info(BASE).is_none(), "deregistered id must be gone");
    }
}

#[cfg(test)]
mod overload_gate_tests {
    use super::overload_threshold_exceeded;

    #[test]
    fn balanced_load_is_not_overloaded() {
        // 4 shards, 1000 total, this shard at the mean.
        assert!(!overload_threshold_exceeded(250, 1000, 4));
        // Right at 2x mean + 16 is still allowed (strictly-greater gate).
        assert!(!overload_threshold_exceeded(516, 1000, 4));
    }

    #[test]
    fn funneled_shard_trips_the_gate() {
        assert!(overload_threshold_exceeded(517, 1000, 4));
        assert!(overload_threshold_exceeded(900, 1000, 4));
    }

    #[test]
    fn small_fleets_never_flap() {
        // Absolute floor: with 10 total conns, even a 10/0/0/0 split stays
        // under mean*2+16 — affinity wins for tiny fleets.
        assert!(!overload_threshold_exceeded(10, 10, 4));
    }

    #[test]
    fn zero_shards_is_safe() {
        assert!(!overload_threshold_exceeded(0, 0, 0));
    }
}

#[cfg(test)]
mod idle_timeout_tests {
    use super::*;

    /// Sweep EVERY stripe but act only on `shard`'s entries.
    ///
    /// Production partitions stripes across shards and does not filter by
    /// `entry.shard` at all (see [`sweep_idle_clients`]), so tests cannot use
    /// `kill_idle_clients` directly: a test's clients land in stripes by
    /// `id % STRIPES` and would only be visited by whichever shard owns that
    /// stripe. This walks all stripes — the pre-partition behaviour — and
    /// keeps the shard filter purely as test isolation.
    fn sweep_for_test_shard(shard: usize, timeout_secs: u64, now_epoch_ms: u64) -> u64 {
        sweep_idle_clients(0, 1, timeout_secs, now_epoch_ms, Some(shard))
    }

    /// The registry is process-global and `cargo test` runs these in
    /// parallel, so every test gets its own synthetic shard id, applied via
    /// `sweep_for_test_shard` above.
    /// (`SHARD_CONNS` is unset in unit tests, so the per-shard counters are
    /// inert for these ids.)
    fn client(id: u64, shard: usize) -> Arc<ClientLiveState> {
        register(id, format!("127.0.0.1:{id}"), "default".into(), shard, -1)
    }

    /// `kill_fd` is -1 here, so `force_close_fd` is a no-op and the observable
    /// effect is the cooperative kill flag — which is what the handler loop
    /// checks anyway.
    fn killed(id: u64) -> bool {
        is_killed(id)
    }

    /// The stripe partition is the whole point of the sweep's shape: each
    /// shard must visit a DISJOINT subset of stripes, and the union across
    /// shards must cover every stripe exactly once. Without this the chore
    /// re-scans the entire registry once per shard per second.
    #[test]
    fn stripe_partition_is_disjoint_and_total() {
        for num_shards in [1usize, 2, 3, 4, 12, 16, 24] {
            let mut seen = vec![0usize; STRIPES];
            for shard in 0..num_shards {
                for stripe in 0..STRIPES {
                    if stripe % num_shards == shard {
                        seen[stripe] += 1;
                    }
                }
            }
            assert!(
                seen.iter().all(|&n| n == 1),
                "num_shards={num_shards}: every stripe must be swept exactly \
                 once per round, got {seen:?}"
            );
        }
    }

    /// A shard only sweeps its own stripes, so an idle client is reaped by
    /// exactly ONE shard — never by all of them, and never by none.
    #[test]
    fn idle_client_is_reaped_by_exactly_one_shard() {
        const SHARD: usize = 908;
        const NUM_SHARDS: usize = 4;
        let live = client(9_080, SHARD);
        let now = live.connected_at_epoch_ms + 30_000;

        // Which stripe holds it, and therefore which shard owns the sweep.
        let owner = (9_080usize % STRIPES) % NUM_SHARDS;
        for shard in 0..NUM_SHARDS {
            if shard == owner {
                continue;
            }
            assert_eq!(
                sweep_idle_clients(shard, NUM_SHARDS, 5, now, Some(SHARD)),
                0,
                "shard {shard} must not sweep another shard's stripes"
            );
        }
        assert!(!killed(9_080), "no non-owner shard may have killed it");
        assert_eq!(
            sweep_idle_clients(owner, NUM_SHARDS, 5, now, Some(SHARD)),
            1,
            "the owning shard must reap it"
        );
        assert!(killed(9_080));
        deregister(9_080);
    }

    /// The `blocked` exemption must survive an early return: a leaked bit
    /// would exempt the client from `timeout` forever.
    #[test]
    fn blocked_guard_clears_on_drop() {
        const SHARD: usize = 909;
        let live = client(9_090, SHARD);
        let now = live.connected_at_epoch_ms + 30_000;
        {
            let _g = live.blocked_guard();
            assert_eq!(
                sweep_for_test_shard(SHARD, 5, now),
                0,
                "a blocked client is exempt while the guard is alive"
            );
        }
        assert_eq!(
            sweep_for_test_shard(SHARD, 5, now),
            1,
            "once the guard drops the exemption must be gone"
        );
        deregister(9_090);
    }

    #[test]
    fn idle_past_timeout_is_killed_and_active_is_not() {
        const SHARD: usize = 900;
        let live_idle = client(9_001, SHARD);
        let live_active = client(9_002, SHARD);
        let now = live_idle.connected_at_epoch_ms + 30_000;

        // The active client completed a batch 1s ago; the idle one never has.
        live_active.touch(0, ClientFlags::default(), now - 1_000);

        assert_eq!(
            sweep_for_test_shard(SHARD, 10, now),
            1,
            "only the idle client"
        );
        assert!(killed(9_001));
        assert!(!killed(9_002));

        deregister(9_001);
        deregister(9_002);
    }

    #[test]
    fn timeout_zero_never_kills() {
        const SHARD: usize = 901;
        let live = client(9_003, SHARD);
        let now = live.connected_at_epoch_ms + 3_600_000;
        assert_eq!(sweep_for_test_shard(SHARD, 0, now), 0);
        assert!(!killed(9_003));
        deregister(9_003);
    }

    /// Redis's `clientsCronHandleTimeout` exempts blocked clients,
    /// subscribers and replication links. Before D1 these were exempt only by
    /// accident of control flow (each took a different arm of the read loop);
    /// a sweep has to exempt them explicitly.
    #[test]
    fn blocked_subscriber_and_replica_are_exempt() {
        const SHARD: usize = 902;
        let blocked = client(9_010, SHARD);
        let subscriber = client(9_011, SHARD);
        let replica = client(9_012, SHARD);
        let now = blocked.connected_at_epoch_ms + 60_000;

        blocked.set_blocked(true);
        subscriber.touch(
            0,
            ClientFlags {
                subscriber: true,
                ..Default::default()
            },
            blocked.connected_at_epoch_ms,
        );
        replica.touch(
            0,
            ClientFlags {
                replica: true,
                ..Default::default()
            },
            blocked.connected_at_epoch_ms,
        );

        assert_eq!(
            sweep_for_test_shard(SHARD, 5, now),
            0,
            "all three are exempt"
        );
        assert!(!killed(9_010));
        assert!(!killed(9_011));
        assert!(!killed(9_012));

        deregister(9_010);
        deregister(9_011);
        deregister(9_012);
    }

    /// A blocked client becomes eligible again once it unblocks — the bit is
    /// an exemption, not permanent immunity.
    #[test]
    fn unblocking_restores_eligibility() {
        const SHARD: usize = 903;
        let live = client(9_020, SHARD);
        let now = live.connected_at_epoch_ms + 60_000;

        live.set_blocked(true);
        assert_eq!(sweep_for_test_shard(SHARD, 5, now), 0);

        live.set_blocked(false);
        assert_eq!(sweep_for_test_shard(SHARD, 5, now), 1);
        assert!(killed(9_020));
        deregister(9_020);
    }

    /// `set_blocked` must not disturb the other flag bits.
    #[test]
    fn set_blocked_preserves_other_flags() {
        const SHARD: usize = 904;
        let live = client(9_030, SHARD);
        live.touch(
            3,
            ClientFlags {
                subscriber: true,
                in_multi: true,
                blocked: false,
                replica: true,
            },
            live.connected_at_epoch_ms,
        );

        live.set_blocked(true);
        let f = ClientFlags::from_bits(live.flags.load(Ordering::Relaxed));
        assert!(f.subscriber && f.in_multi && f.replica && f.blocked);

        live.set_blocked(false);
        let f = ClientFlags::from_bits(live.flags.load(Ordering::Relaxed));
        assert!(f.subscriber && f.in_multi && f.replica && !f.blocked);
        deregister(9_030);
    }

    /// Each shard sweeps only the connections it owns, so N shards do not do
    /// N times the work — and never close another shard's clients.
    #[test]
    fn sweep_only_touches_its_own_shard() {
        const MINE: usize = 905;
        const THEIRS: usize = 906;
        let mine = client(9_040, MINE);
        let _theirs = client(9_041, THEIRS);
        let now = mine.connected_at_epoch_ms + 60_000;

        assert_eq!(sweep_for_test_shard(MINE, 5, now), 1);
        assert!(killed(9_040));
        assert!(
            !killed(9_041),
            "shard 906's client is not shard 905's business"
        );

        deregister(9_040);
        deregister(9_041);
    }

    /// An already-killed client is not counted twice by a later sweep.
    #[test]
    fn already_killed_is_not_recounted() {
        const SHARD: usize = 907;
        let live = client(9_050, SHARD);
        let now = live.connected_at_epoch_ms + 60_000;

        assert_eq!(sweep_for_test_shard(SHARD, 5, now), 1);
        assert_eq!(sweep_for_test_shard(SHARD, 5, now), 0, "idempotent");
        deregister(9_050);
    }

    #[test]
    fn parked_gauge_tracks_deltas() {
        let before = parked_clients();
        parked_delta(true);
        parked_delta(true);
        assert_eq!(parked_clients(), before + 2);
        parked_delta(false);
        parked_delta(false);
        assert_eq!(parked_clients(), before);
    }

    /// #438 conn-secondary: a re-register of a still-present id (the
    /// kept_registration miss-arm fail-safe racing an entry back into
    /// existence) must NOT increment TOTAL_CLIENTS again — the single
    /// eventual deregister would then leave the count permanently +1,
    /// skewing `shard_overloaded` routing. (TOTAL_CLIENTS is process-global
    /// and other tests register concurrently, so the assert brackets only
    /// the one replacing insert — the narrowest possible window.)
    #[test]
    fn replacing_register_does_not_double_count() {
        use std::sync::atomic::Ordering;
        const ID: u64 = 9_060;
        let _a = register(ID, "t:1".into(), "default".into(), 908, -1);
        let t1 = TOTAL_CLIENTS.load(Ordering::Relaxed);
        let _b = register(ID, "t:1".into(), "default".into(), 909, -1);
        let t2 = TOTAL_CLIENTS.load(Ordering::Relaxed);
        // Pre-fix, the unconditional fetch_add makes this read t1+1 even
        // with zero concurrency. (An unrelated test registering inside this
        // two-instruction window could false-fail; the window is nanoseconds
        // and the suite's other global-counter tests accept the same odds.)
        assert_eq!(t2, t1, "replacing insert must not double-count");
        // The entry itself was replaced, not duplicated, and one deregister
        // fully clears it.
        deregister(ID);
        assert!(live_handle(ID).is_none(), "single deregister must clear");
    }
}
