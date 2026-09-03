//! CLIENT PAUSE / UNPAUSE global state.
//!
//! When paused, command processing is delayed for all clients until the pause
//! expires or CLIENT UNPAUSE is called. Supports two modes:
//! - ALL: pause all commands
//! - WRITE: pause only write commands (reads still served)

use parking_lot::RwLock;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PauseMode {
    All,
    Write,
}

struct PauseState {
    active: bool,
    mode: PauseMode,
    until: Instant,
}

static PAUSE: LazyLock<RwLock<PauseState>> = LazyLock::new(|| {
    RwLock::new(PauseState {
        active: false,
        mode: PauseMode::All,
        until: Instant::now(),
    })
});

/// Lock-free "a pause may be active" hint, for hot paths that must not take
/// the `PAUSE` lock on every command (moon#660).
///
/// Set alongside `PAUSE.active` and cleared with it, and — critically — under
/// the SAME `PAUSE` write lock as every other writer, so the three of them
/// serialize. It is deliberately CONSERVATIVE in one direction only:
/// `check_pause` treats a pause whose deadline has passed as inactive without
/// clearing `active`, so this flag can read `true` for an already-expired
/// pause until `expire_if_needed` runs. A stale `true` costs a fast path its
/// fast leg for one command; a stale `false` would let a paused write through.
///
/// The first version of this published the hint BEFORE taking the lock, and a
/// concurrent clear could land in between:
///
/// 1. `pause()` stores `PAUSE_ANY = true`, then blocks on `PAUSE.write()`.
/// 2. `unpause()` / `expire_if_needed()` takes the lock, clears `active`, and
///    stores `PAUSE_ANY = false`.
/// 3. `pause()` finally acquires the lock and sets `active = true`.
///
/// leaving `active == true` with the hint reading `false` for the whole pause
/// window — every inline `SET` escaping a pause an operator believes is in
/// force. Publishing inside the lock is what makes "stale `false` cannot
/// happen" true rather than merely asserted. Guarded by
/// `test_pause_hint_survives_a_clear_racing_an_activation`.
static PAUSE_ANY: AtomicBool = AtomicBool::new(false);

/// Lock-free: is a pause POSSIBLY active?
///
/// `false` is authoritative — no pause is active, and the caller may take its
/// fast path. `true` means "ask [`check_pause`]", which owns the real decision
/// (mode, expiry, remaining duration). Callers must not re-derive that here.
#[inline]
#[must_use]
pub fn pause_possibly_active() -> bool {
    PAUSE_ANY.load(Ordering::Relaxed)
}

/// Activate pause for the given duration and mode.
pub fn pause(duration_ms: u64, mode: PauseMode) {
    let mut state = PAUSE.write();
    // Published under the lock that clears it, never before acquiring it —
    // see the note on `PAUSE_ANY`.
    PAUSE_ANY.store(true, Ordering::Relaxed);
    state.active = true;
    state.mode = mode;
    state.until = Instant::now() + std::time::Duration::from_millis(duration_ms);
}

/// Deactivate pause immediately.
pub fn unpause() {
    let mut state = PAUSE.write();
    state.active = false;
    PAUSE_ANY.store(false, Ordering::Relaxed);
}

/// Check if the server is currently paused. Returns the remaining duration
/// if paused, or None if not paused. Auto-expires when the deadline passes.
pub fn check_pause(is_write: bool) -> Option<std::time::Duration> {
    let state = PAUSE.read();
    if !state.active {
        return None;
    }
    let now = Instant::now();
    if now >= state.until {
        // Expired — will be cleaned up on next write access
        return None;
    }
    // In WRITE mode, only pause write commands
    if state.mode == PauseMode::Write && !is_write {
        return None;
    }
    Some(state.until - now)
}

/// Clean up expired pause state (called periodically from handlers).
/// Takes a write lock to avoid TOCTOU between expiry check and clear.
pub fn expire_if_needed() {
    let mut state = PAUSE.write();
    if state.active && Instant::now() >= state.until {
        state.active = false;
        PAUSE_ANY.store(false, Ordering::Relaxed);
    }
}

/// The ONE mutex serialising every test that touches process-global pause
/// state — this module's own, and `server::conn::tests`, whose
/// `try_inline_dispatch` calls consult `pause_possibly_active()`.
///
/// It lives here, next to the state, because two disjoint mutexes are not
/// serialisation: `client_pause`'s tests and the inline-dispatch tests each
/// had their own, so `test_pause_and_check`'s `pause(5000, All)` could run
/// concurrently with an inline SET and fail that test's CONTROL assertion for
/// a reason nothing in its body mentions.
#[cfg(test)]
pub(crate) fn pause_test_lock() -> parking_lot::MutexGuard<'static, ()> {
    static LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());
    LOCK.lock()
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::pause_test_lock as TEST_LOCK_FN;

    /// The clear-racing-an-activation interleaving, made deterministic.
    ///
    /// The test thread holds the `PAUSE` write lock, so the `pause()` below is
    /// forced to block at exactly the point the race lives. A clear then lands
    /// FIRST, while `pause()` is still blocked, and `pause()` completes after.
    ///
    /// With the hint published before the lock, `pause()`'s `true` is already
    /// in place when the clear overwrites it with `false`, and nothing
    /// re-publishes — the pause goes active with the hint reading `false` and
    /// every inline write escapes it. Publishing inside the lock puts the
    /// store after the clear, where it belongs.
    ///
    /// Reddening mutation: move `PAUSE_ANY.store(true, ..)` in `pause()` back
    /// above `let mut state = PAUSE.write();`.
    #[test]
    fn test_pause_hint_survives_a_clear_racing_an_activation() {
        let _lock = TEST_LOCK_FN();
        unpause();

        let guard = PAUSE.write();
        let activating = std::thread::spawn(|| pause(5_000, PauseMode::All));
        // Long enough for the spawned thread to reach the lock (and, under the
        // old ordering, to have already published its hint).
        std::thread::sleep(std::time::Duration::from_millis(100));
        // The concurrent clear, landing while `pause()` is still blocked. This
        // is what `unpause()` / `expire_if_needed()` do under this same lock.
        PAUSE_ANY.store(false, Ordering::Relaxed);
        drop(guard);
        activating.join().expect("pause thread");

        // Precondition: the activation did take effect.
        assert!(
            check_pause(true).is_some(),
            "precondition: the pause must be active for this to prove anything"
        );
        assert!(
            pause_possibly_active(),
            "CLIENT PAUSE is active but the lock-free hint reads false, so \
             every inline write will skip its pause pre-gate for the whole \
             pause window"
        );
        unpause();
    }

    #[test]
    fn test_pause_and_check() {
        let _lock = TEST_LOCK_FN();
        unpause();
        assert!(check_pause(true).is_none());

        pause(5000, PauseMode::All);
        assert!(check_pause(true).is_some());
        assert!(check_pause(false).is_some());

        unpause();
        assert!(check_pause(true).is_none());
    }

    #[test]
    fn test_write_mode_allows_reads() {
        let _lock = TEST_LOCK_FN();
        unpause();
        pause(5000, PauseMode::Write);
        assert!(check_pause(true).is_some()); // write blocked
        assert!(check_pause(false).is_none()); // read allowed
        unpause();
    }
}
