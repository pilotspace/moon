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

    /// Lock-free CLIENT KILL check for the connection's own loop.
    #[inline]
    pub fn is_killed(&self) -> bool {
        self.kill_flag.load(Ordering::Relaxed)
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
        (self.subscriber as u8) | ((self.in_multi as u8) << 1) | ((self.blocked as u8) << 2)
    }

    /// Unpack from `ClientLiveState::flags`.
    #[inline]
    pub fn from_bits(bits: u8) -> Self {
        ClientFlags {
            subscriber: bits & 1 != 0,
            in_multi: bits & 2 != 0,
            blocked: bits & 4 != 0,
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
    stripe(id).write().insert(id, entry);
    TOTAL_CLIENTS.fetch_add(1, Ordering::Relaxed);
    shard_conns_delta(shard, true);
    crate::admin::metrics_setup::record_shard_connection_delta(shard, 1.0);
    live
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
    // `shutdown` is race-free because a connection's `RegistryGuard` (a local)
    // drops — calling `deregister`, which needs its OWN stripe's WRITE lock —
    // strictly before its `stream` (a parameter) drops and closes the fd. So
    // while we hold a stripe's READ lock and an entry is present in it,
    // `deregister` for that entry is blocked, its `stream` has not dropped,
    // and `kill_fd` is still an open socket. The invariant only ever involves
    // one entry and its own stripe, so striping preserves it; entries in
    // other stripes are simply not visited while their lock is free.
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
    let _ = writeln!(
        buf,
        "id={} addr={} laddr=127.0.0.1:0 fd=0 name={} age={} idle={} flags={} db={} \
         sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 \
         tot-net-in=0 tot-net-out=0 rbs=1024 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r \
         cmd=NULL user={} redir=-1 resp=2 lib-name= lib-ver=",
        entry.id, entry.addr, name, age, idle, flags, db, entry.user,
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
