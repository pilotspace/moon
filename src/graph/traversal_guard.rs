//! Bounded epoch hold for graph traversals.
//!
//! Prevents long-running traversals from holding old graph versions indefinitely.
//! A `TraversalGuard` captures a snapshot-LSN at traversal start and enforces a
//! configurable timeout (default 30s). Multi-hop traversals check the guard at
//! each hop to ensure the timeout has not been exceeded.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Default traversal timeout: 30 seconds.
pub const DEFAULT_TRAVERSAL_TIMEOUT: Duration = Duration::from_secs(30);

/// Process-wide default traversal timeout in milliseconds (0 = unlimited).
///
/// Set once at startup from `--graph-timeout-ms`; read by
/// `with_default_timeout`. Per-query `TIMEOUT <ms>` overrides it for one
/// query via `TraversalGuard::new`.
static DEFAULT_TIMEOUT_MS: AtomicU64 = AtomicU64::new(DEFAULT_TRAVERSAL_TIMEOUT.as_millis() as u64);

/// Set the process-wide default traversal timeout (`--graph-timeout-ms`).
/// `0` disables the timeout entirely.
pub fn set_default_traversal_timeout_ms(ms: u64) {
    DEFAULT_TIMEOUT_MS.store(ms, Ordering::Relaxed);
}

/// The configured default traversal timeout (`Duration::MAX` = unlimited).
pub fn default_traversal_timeout() -> Duration {
    match DEFAULT_TIMEOUT_MS.load(Ordering::Relaxed) {
        0 => Duration::MAX,
        ms => Duration::from_millis(ms),
    }
}

/// Error returned when a traversal exceeds its time budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraversalTimeout {
    /// Elapsed time since traversal start.
    pub elapsed: Duration,
    /// Configured timeout.
    pub timeout: Duration,
    /// The snapshot LSN that was held.
    pub snapshot_lsn: u64,
}

impl core::fmt::Display for TraversalTimeout {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "traversal timeout: elapsed {:?} exceeds limit {:?} (snapshot_lsn={})",
            self.elapsed, self.timeout, self.snapshot_lsn
        )
    }
}

/// Guard that tracks a traversal's snapshot and enforces bounded epoch hold.
///
/// Created once at traversal start. The snapshot_lsn is captured once and used
/// for all hops, ensuring consistent graph visibility across the entire traversal.
/// Each hop should call `check_timeout()` to verify the traversal has not exceeded
/// its time budget.
#[derive(Debug, Clone, Copy)]
pub struct TraversalGuard {
    snapshot_lsn: u64,
    start: Instant,
    timeout: Duration,
}

impl TraversalGuard {
    /// Create a new guard with the given snapshot LSN and timeout.
    pub fn new(snapshot_lsn: u64, timeout: Duration) -> Self {
        Self {
            snapshot_lsn,
            start: Instant::now(),
            timeout,
        }
    }

    /// Create a guard with the configured process-wide default timeout
    /// (`--graph-timeout-ms`; 30s unless overridden, 0 = unlimited).
    pub fn with_default_timeout(snapshot_lsn: u64) -> Self {
        Self::new(snapshot_lsn, default_traversal_timeout())
    }

    /// The snapshot LSN captured at traversal start.
    #[inline]
    pub fn snapshot_lsn(&self) -> u64 {
        self.snapshot_lsn
    }

    /// Check if the traversal has exceeded its timeout.
    ///
    /// Returns `Ok(())` if within budget, `Err(TraversalTimeout)` if exceeded.
    /// Called once per hop during multi-hop traversal.
    #[inline]
    pub fn check_timeout(&self) -> Result<(), TraversalTimeout> {
        let elapsed = self.start.elapsed();
        if elapsed > self.timeout {
            Err(TraversalTimeout {
                elapsed,
                timeout: self.timeout,
                snapshot_lsn: self.snapshot_lsn,
            })
        } else {
            Ok(())
        }
    }

    /// Elapsed time since traversal start.
    #[inline]
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_guard_captures_snapshot_lsn() {
        let guard = TraversalGuard::new(42, Duration::from_secs(10));
        assert_eq!(guard.snapshot_lsn(), 42);
    }

    #[test]
    fn test_guard_default_timeout_is_configurable() {
        // Single test owns the global (parallel tests share the process);
        // every transient value here is still "generous" for concurrent
        // with_default_timeout users.
        let guard = TraversalGuard::with_default_timeout(100);
        assert_eq!(guard.snapshot_lsn(), 100);
        assert_eq!(guard.timeout, DEFAULT_TRAVERSAL_TIMEOUT);

        set_default_traversal_timeout_ms(12_345);
        let guard = TraversalGuard::with_default_timeout(7);
        assert_eq!(guard.timeout, Duration::from_millis(12_345));

        // 0 = unlimited.
        set_default_traversal_timeout_ms(0);
        let guard = TraversalGuard::with_default_timeout(7);
        assert_eq!(guard.timeout, Duration::MAX);
        assert!(guard.check_timeout().is_ok());

        set_default_traversal_timeout_ms(DEFAULT_TRAVERSAL_TIMEOUT.as_millis() as u64);
        let guard = TraversalGuard::with_default_timeout(7);
        assert_eq!(guard.timeout, DEFAULT_TRAVERSAL_TIMEOUT);
    }

    #[test]
    fn test_check_timeout_within_budget() {
        let guard = TraversalGuard::new(10, Duration::from_secs(60));
        assert!(guard.check_timeout().is_ok());
    }

    #[test]
    fn test_check_timeout_exceeded() {
        // Use a zero-duration timeout so it immediately expires.
        let guard = TraversalGuard::new(10, Duration::from_nanos(0));
        // A tiny sleep is not needed -- even Instant::now() granularity
        // means elapsed() > 0ns after construction.
        std::thread::sleep(Duration::from_millis(1));
        let err = guard.check_timeout().unwrap_err();
        assert_eq!(err.timeout, Duration::from_nanos(0));
        assert_eq!(err.snapshot_lsn, 10);
        assert!(err.elapsed > Duration::from_nanos(0));
    }

    #[test]
    fn test_traversal_timeout_display() {
        let err = TraversalTimeout {
            elapsed: Duration::from_secs(35),
            timeout: Duration::from_secs(30),
            snapshot_lsn: 42,
        };
        let msg = format!("{err}");
        assert!(msg.contains("traversal timeout"));
        assert!(msg.contains("snapshot_lsn=42"));
    }

    #[test]
    fn test_elapsed_advances() {
        let guard = TraversalGuard::new(1, Duration::from_secs(60));
        let e1 = guard.elapsed();
        std::thread::sleep(Duration::from_millis(1));
        let e2 = guard.elapsed();
        assert!(e2 > e1);
    }
}
