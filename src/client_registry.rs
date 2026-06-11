//! Global client connection registry for CLIENT LIST/INFO/KILL.
//!
//! Every connection registers on accept and deregisters on close.
//! The registry is a global `parking_lot::RwLock<HashMap>` touched only on
//! connect/disconnect and CLIENT commands. Per-batch state (db, idle time,
//! flags, kill checks) flows through the lock-free [`ClientLiveState`] handle
//! that `register` returns (QW8, 2026-06 review finding 1.3 — previously the
//! steady-state loop took the global write lock after every pipeline batch
//! and the global read lock for every kill check).

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Instant;

/// Global client registry.
static REGISTRY: LazyLock<RwLock<HashMap<u64, ClientEntry>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Lock-free per-connection state, shared between the connection task
/// (writer, once per batch) and CLIENT LIST/INFO/KILL (occasional readers).
pub struct ClientLiveState {
    pub connected_at: Instant,
    pub db: AtomicUsize,
    /// Milliseconds since `connected_at` of the last completed batch.
    pub last_cmd_ms: AtomicU64,
    /// Bit-packed [`ClientFlags`] (see `ClientFlags::to_bits`).
    pub flags: AtomicU8,
    /// Set by CLIENT KILL — the handler checks this and closes the connection.
    pub kill_flag: AtomicBool,
}

impl ClientLiveState {
    /// Record batch-completion state. Three relaxed stores — no lock.
    #[inline]
    pub fn touch(&self, db: usize, flags: ClientFlags) {
        self.db.store(db, Ordering::Relaxed);
        self.last_cmd_ms.store(
            self.connected_at.elapsed().as_millis() as u64,
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
pub fn register(id: u64, addr: String, user: String, shard: usize) -> Arc<ClientLiveState> {
    let live = Arc::new(ClientLiveState {
        connected_at: Instant::now(),
        db: AtomicUsize::new(0),
        last_cmd_ms: AtomicU64::new(0),
        flags: AtomicU8::new(ClientFlags::default().to_bits()),
        kill_flag: AtomicBool::new(false),
    });
    let entry = ClientEntry {
        id,
        addr,
        name: None,
        user,
        shard,
        live: Arc::clone(&live),
    };
    REGISTRY.write().insert(id, entry);
    live
}

/// Deregister a client connection.
pub fn deregister(id: u64) {
    REGISTRY.write().remove(&id);
}

/// Update mutable fields for a client (CLIENT SETNAME and similar — rare,
/// never the steady-state batch loop; batch state goes through the
/// [`ClientLiveState`] handle instead).
pub fn update<F: FnOnce(&mut ClientEntry)>(id: u64, f: F) {
    if let Some(entry) = REGISTRY.write().get_mut(&id) {
        f(entry);
    }
}

/// Check if a client has been marked for killing.
///
/// Registry-lookup variant for code without the live handle; connection
/// loops use `ClientLiveState::is_killed` (lock-free) instead.
pub fn is_killed(id: u64) -> bool {
    REGISTRY.read().get(&id).is_some_and(|e| e.live.is_killed())
}

/// Format all clients as a CLIENT LIST string.
///
/// Each line: `id=N addr=... fd=0 name=... db=N ...`
/// Returns the full response string.
pub fn client_list() -> String {
    let registry = REGISTRY.read();
    let now = Instant::now();
    let mut result = String::with_capacity(registry.len() * 128);
    for entry in registry.values() {
        format_client_line(&mut result, entry, now);
    }
    // Remove trailing newline if present
    if result.ends_with('\n') {
        result.pop();
    }
    result
}

/// Format a single client's info (for CLIENT INFO).
pub fn client_info(id: u64) -> Option<String> {
    let registry = REGISTRY.read();
    let now = Instant::now();
    registry.get(&id).map(|entry| {
        let mut result = String::with_capacity(128);
        format_client_line(&mut result, entry, now);
        if result.ends_with('\n') {
            result.pop();
        }
        result
    })
}

/// Kill clients matching the given filter. Returns count of killed clients.
pub fn kill_clients(filter: &KillFilter) -> u64 {
    let registry = REGISTRY.read();
    let mut count = 0u64;
    for entry in registry.values() {
        let matches = match filter {
            KillFilter::Id(target_id) => entry.id == *target_id,
            KillFilter::Addr(addr) => entry.addr == *addr,
            KillFilter::User(user) => entry.user == *user,
        };
        if matches {
            entry.live.kill_flag.store(true, Ordering::Relaxed);
            count += 1;
        }
    }
    count
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
        "id={} addr={} fd=0 name={} db={} sub=0 psub=0 ssub=0 multi=-1 \
         watch=0 qbuf=0 qbuf-free=0 argv-mem=0 tot-mem=0 net-i=0 net-o=0 \
         age={} idle={} flags={} user={}",
        entry.id, entry.addr, name, db, age, idle, flags, entry.user,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_list() {
        let id = 999_000;
        register(id, "127.0.0.1:12345".into(), "default".into(), 0);
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
        register(id, "10.0.0.1:5000".into(), "alice".into(), 1);
        let info = client_info(id);
        assert!(info.is_some());
        assert!(info.as_ref().is_some_and(|s| s.contains("user=alice")));
        deregister(id);
        assert!(client_info(id).is_none());
    }

    #[test]
    fn test_kill_by_id() {
        let id = 999_002;
        let live = register(id, "10.0.0.2:6000".into(), "bob".into(), 0);
        assert!(!is_killed(id));
        assert!(!live.is_killed());
        let count = kill_clients(&KillFilter::Id(id));
        assert_eq!(count, 1);
        assert!(is_killed(id));
        assert!(live.is_killed(), "live handle observes the kill lock-free");
        deregister(id);
    }

    #[test]
    fn test_kill_by_user() {
        let id1 = 999_010;
        let id2 = 999_011;
        register(id1, "10.0.0.3:7000".into(), "eve".into(), 0);
        register(id2, "10.0.0.4:7001".into(), "eve".into(), 1);
        let count = kill_clients(&KillFilter::User("eve".into()));
        assert_eq!(count, 2);
        assert!(is_killed(id1));
        assert!(is_killed(id2));
        deregister(id1);
        deregister(id2);
    }

    #[test]
    fn test_update_and_touch() {
        let id = 999_003;
        let live = register(id, "10.0.0.5:8000".into(), "default".into(), 0);
        update(id, |e| {
            e.name = Some("myconn".into());
        });
        live.touch(3, ClientFlags::default());
        let info = client_info(id).unwrap();
        assert!(info.contains("name=myconn"));
        assert!(info.contains("db=3"));
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
