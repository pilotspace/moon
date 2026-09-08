//! Whether this shard is still rebuilding its indexes (moon#476).
//!
//! Index recovery runs on the shard thread after the keyspace itself has been
//! restored (`restore_from_persistence`, `main.rs`). Until it finishes, the
//! vector/text indexes are partially built, so a command that reads them would
//! see a store that is neither the old one nor the new one.
//!
//! Before this flag existed the shard simply did not serve anyone during that
//! window: the listener was bound and its accept task spawned, but recovery was
//! synchronous, so nothing was ever scheduled to answer. The kernel completed
//! handshakes from the backlog by itself, so a client connected successfully
//! and then waited out the whole recovery in silence — indistinguishable from
//! a wedged server, and unbounded in the store's size (measured: 1,228 ms on
//! 83,828 keys; the reported production case was ~94 minutes).
//!
//! The flag is per-shard and lives in thread-local storage because every reader
//! is on the shard thread already: the command path pays one thread-local read,
//! not an atomic. The process-wide counter exists only so `INFO` can answer
//! "is anything still loading?" from any thread.

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

thread_local! {
    /// Set only by this shard's own recovery task.
    static SHARD_LOADING: Cell<bool> = const { Cell::new(false) };
}

/// Shards currently loading, for cross-thread reporting (`INFO persistence`).
static LOADING_SHARDS: AtomicUsize = AtomicUsize::new(0);

/// Mark this shard as loading (or done). Idempotent: calling it twice with the
/// same value must not double-count the process-wide total, or `INFO` would
/// report loading forever after a spurious repeat.
pub fn set_loading(on: bool) {
    SHARD_LOADING.with(|c| {
        if c.get() == on {
            return;
        }
        c.set(on);
        if on {
            LOADING_SHARDS.fetch_add(1, Ordering::Relaxed);
        } else {
            LOADING_SHARDS.fetch_sub(1, Ordering::Relaxed);
        }
    });
}

/// Is THIS shard still loading? One thread-local read on the command path.
#[inline]
pub fn is_loading() -> bool {
    SHARD_LOADING.with(Cell::get)
}

/// Is any shard still loading? For `INFO`, which may be answered anywhere.
pub fn any_shard_loading() -> bool {
    LOADING_SHARDS.load(Ordering::Relaxed) > 0
}

/// Holds a shard in the loading state for as long as it lives.
///
/// The flag must be cleared on EVERY exit, not just the happy one: a panic
/// inside the recovery task, or the task being dropped un-polled at shutdown,
/// would otherwise leave this shard answering `-LOADING` to every command for
/// the life of the process — a far worse failure than the hang being fixed.
/// `Drop` is the only exit path that covers all three cases.
///
/// Acquire it on the thread that will do the loading, BEFORE spawning the
/// recovery task, and move it into that task: acquiring inside the task would
/// leave a window between spawn and first poll where the flag reads false and
/// commands would be served against indexes that are not yet rebuilt.
#[must_use = "dropping the guard immediately clears the loading state"]
pub struct LoadingGuard(());

impl LoadingGuard {
    pub fn acquire() -> Self {
        set_loading(true);
        Self(())
    }
}

impl Drop for LoadingGuard {
    fn drop(&mut self) {
        set_loading(false);
    }
}

/// Commands that still answer normally while the dataset loads.
///
/// Mirrors Redis's `CMD_LOADING` flag: everything an operator or a client
/// library needs to diagnose, authenticate, or disconnect, and nothing that
/// reads or writes the keyspace. The list is deliberately conservative —
/// admitting a command that touches data would expose a half-built index,
/// which is the whole reason for refusing.
///
/// `SUBSCRIBE` and friends are admitted because pub/sub carries no persisted
/// state; a subscriber that connects during a restart has nothing to read back.
#[must_use]
pub fn allowed_while_loading(cmd_upper: &[u8]) -> bool {
    matches!(
        cmd_upper,
        b"PING"
            | b"ECHO"
            | b"AUTH"
            | b"HELLO"
            | b"QUIT"
            | b"RESET"
            | b"INFO"
            | b"CLIENT"
            | b"CONFIG"
            | b"COMMAND"
            | b"SHUTDOWN"
            | b"SELECT"
            | b"SUBSCRIBE"
            | b"UNSUBSCRIBE"
            | b"PSUBSCRIBE"
            | b"PUNSUBSCRIBE"
            | b"SSUBSCRIBE"
            | b"SUNSUBSCRIBE"
            | b"LATENCY"
            | b"SLOWLOG"
            | b"MEMORY"
            | b"REPLCONF"
    )
}

/// The refusal every other command gets while this shard loads.
///
/// Wire format matches Redis so existing clients' retry/failover logic fires
/// unchanged — the error name is what they switch on.
#[must_use]
pub fn loading_error() -> crate::protocol::Frame {
    crate::protocol::Frame::Error(bytes::Bytes::from_static(
        b"LOADING moon is loading the dataset in memory",
    ))
}

// ---------------------------------------------------------------------------
// Operator escape hatch: skip the boot-time keyspace reindex (moon#882)
// ---------------------------------------------------------------------------

/// Should this boot SKIP the keyspace walk that repopulates vector/text
/// indexes from the restored hashes?
///
/// Index definitions are restored from the sidecars either way — this only
/// governs phase 2, the walk over every key matching an index prefix that
/// re-derives its postings/vectors. On a large corpus that walk is the whole
/// of a long startup: a production instance measured ~200 ms of CPU per key
/// over 293,439 keys, a 12-hour boot during which every command answers
/// `-LOADING` and the server is, from a client's point of view, down.
///
/// With the hatch on, the shard accepts traffic in seconds and the KV plane is
/// complete and correct. The indexes are **empty**: `FT.SEARCH` answers zero
/// results rather than an error, which is a silent wrong answer to anything
/// that searches. That is the trade, and it is why this is opt-in, off by
/// default, and logged at WARN on every affected shard for the life of the
/// process — never inferred, never automatic.
///
/// The deletion probe is skipped with it. The probe tombstones every key_hash
/// the manifest loaded that the walk did not observe; with no walk it observes
/// nothing, so running it would erase the durable index state this hatch
/// exists to preserve for a later rebuild.
pub fn skip_index_recovery() -> bool {
    static SKIP: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *SKIP.get_or_init(|| skip_index_recovery_from(std::env::var("MOON_SKIP_INDEX_RECOVERY").ok()))
}

/// The parse behind [`skip_index_recovery`], split out so it is testable
/// without mutating process environment (which is racy across a test binary's
/// threads and, since Rust 2024, `unsafe`).
///
/// Fails CLOSED: anything that is not an affirmative spelling leaves recovery
/// running. A typo in a deploy must not silently ship a server with empty
/// indexes.
pub(crate) fn skip_index_recovery_from(var: Option<String>) -> bool {
    match var {
        Some(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The flag is thread-local but the counter is process-wide, and the test
    /// harness runs these on separate threads in parallel. Tests that assert on
    /// the COUNTER must serialise; tests that only assert on the flag need not.
    /// `into_inner` on poison because the panic test deliberately unwinds while
    /// holding this.
    static COUNTER_TESTS: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// The flag must not leak across the set/clear pair, or every command after
    /// a restart would be refused forever.
    #[test]
    fn the_flag_clears_and_the_counter_balances() {
        let _serial = COUNTER_TESTS.lock().unwrap_or_else(|e| e.into_inner());
        assert!(!is_loading());
        let base = LOADING_SHARDS.load(Ordering::Relaxed);
        set_loading(true);
        assert!(is_loading());
        assert_eq!(LOADING_SHARDS.load(Ordering::Relaxed), base + 1);
        set_loading(false);
        assert!(!is_loading());
        assert_eq!(LOADING_SHARDS.load(Ordering::Relaxed), base);
    }

    /// A repeated set must not double-count: an unbalanced counter would leave
    /// `INFO` reporting `loading:1` for the life of the process.
    #[test]
    fn repeated_sets_do_not_unbalance_the_counter() {
        let _serial = COUNTER_TESTS.lock().unwrap_or_else(|e| e.into_inner());
        let base = LOADING_SHARDS.load(Ordering::Relaxed);
        set_loading(true);
        set_loading(true);
        set_loading(true);
        assert_eq!(LOADING_SHARDS.load(Ordering::Relaxed), base + 1);
        set_loading(false);
        set_loading(false);
        assert_eq!(LOADING_SHARDS.load(Ordering::Relaxed), base);
    }

    /// The guard is the only thing standing between a panicking recovery task
    /// and a shard that refuses every command forever.
    #[test]
    fn the_guard_clears_the_flag_when_dropped() {
        let _serial = COUNTER_TESTS.lock().unwrap_or_else(|e| e.into_inner());
        assert!(!is_loading());
        {
            let _g = LoadingGuard::acquire();
            assert!(is_loading());
        }
        assert!(!is_loading(), "guard must clear the flag on drop");
    }

    #[test]
    fn the_guard_clears_the_flag_when_its_holder_panics() {
        let _serial = COUNTER_TESTS.lock().unwrap_or_else(|e| e.into_inner());
        assert!(!is_loading());
        let caught = std::panic::catch_unwind(|| {
            let _g = LoadingGuard::acquire();
            assert!(is_loading());
            panic!("recovery blew up");
        });
        assert!(caught.is_err(), "the panic must actually have happened");
        assert!(
            !is_loading(),
            "a panicking recovery task must not leave the shard refusing commands"
        );
    }
    /// The two halves of the contract: diagnostics get through, data does not.
    #[test]
    fn data_commands_are_refused_and_diagnostics_are_not() {
        for cmd in [
            &b"PING"[..],
            b"INFO",
            b"AUTH",
            b"HELLO",
            b"CLIENT",
            b"CONFIG",
            b"SHUTDOWN",
            b"SUBSCRIBE",
        ] {
            assert!(
                allowed_while_loading(cmd),
                "{} must still answer while loading",
                String::from_utf8_lossy(cmd)
            );
        }
        for cmd in [
            &b"GET"[..],
            b"SET",
            b"HGET",
            b"HSET",
            b"MGET",
            b"DEL",
            b"FT.SEARCH",
            b"EVAL",
            b"MULTI",
            b"EXEC",
            b"SCAN",
            b"KEYS",
            b"DBSIZE",
        ] {
            assert!(
                !allowed_while_loading(cmd),
                "{} reads or writes the keyspace and must be refused while loading",
                String::from_utf8_lossy(cmd)
            );
        }
    }

    /// Clients switch on the error NAME. If it drifts from Redis's, their
    /// retry/failover paths stop recognising it and the fix silently regresses
    /// into a plain error.
    #[test]
    fn the_error_is_wire_compatible_with_redis() {
        let crate::protocol::Frame::Error(e) = loading_error() else {
            panic!("loading_error must be an error frame");
        };
        assert!(
            e.starts_with(b"LOADING "),
            "error name must be LOADING, got {:?}",
            String::from_utf8_lossy(&e)
        );
    }

    /// The hatch must be OFF unless someone spelled it affirmatively. An unset
    /// var, an empty one, `0`, and a typo all leave index recovery running:
    /// shipping empty indexes is the dangerous direction, so it fails closed.
    #[test]
    fn the_skip_hatch_is_off_unless_affirmatively_set() {
        for off in [
            None,
            Some(""),
            Some("0"),
            Some("false"),
            Some("no"),
            Some("of"),
            Some("ON1"),
        ] {
            assert!(
                !skip_index_recovery_from(off.map(str::to_string)),
                "{off:?} must NOT skip index recovery"
            );
        }
    }

    /// The spellings an operator actually types, including whitespace an
    /// environment file or a plist string tends to carry.
    #[test]
    fn the_skip_hatch_accepts_the_usual_affirmative_spellings() {
        for on in [
            "1", "true", "TRUE", "True", "yes", "YES", "on", "ON", " 1 ", "\ttrue\n",
        ] {
            assert!(
                skip_index_recovery_from(Some(on.to_string())),
                "{on:?} must skip index recovery"
            );
        }
    }
}
