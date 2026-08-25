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
}
