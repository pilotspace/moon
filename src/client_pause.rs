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
/// Set alongside `PAUSE.active` and cleared with it. It is deliberately
/// CONSERVATIVE in one direction only: `check_pause` treats a pause whose
/// deadline has passed as inactive without clearing `active`, so this flag can
/// read `true` for an already-expired pause until `expire_if_needed` runs. A
/// stale `true` costs a fast path its fast leg for one command; a stale `false`
/// would let a paused write through, and cannot happen — every site that sets
/// `active = true` sets this first.
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
    PAUSE_ANY.store(true, Ordering::Relaxed);
    let mut state = PAUSE.write();
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

#[cfg(test)]
mod tests {
    use super::*;

    // Tests share global state — run sequentially via a mutex
    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_pause_and_check() {
        let _lock = TEST_LOCK.lock();
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
        let _lock = TEST_LOCK.lock();
        unpause();
        pause(5000, PauseMode::Write);
        assert!(check_pause(true).is_some()); // write blocked
        assert!(check_pause(false).is_none()); // read allowed
        unpause();
    }
}
