//! Bounded backoff for connection-accept loops (R-4, design-for-failure).
//!
//! Every accept loop previously retried immediately on error:
//! `Err(e) => { error!("Accept error: {e}"); }` falling straight back into the
//! loop. On `EMFILE`/`ENFILE` (per-process / system-wide fd exhaustion),
//! `accept()` returns an error *synchronously* rather than pending on
//! readiness, so the loop becomes a 100%-of-one-core spin plus one log line per
//! iteration — worsening the very fd/memory pressure that caused it. This is
//! exactly the connection-storm regime (`ulimit -n` exhaustion) documented in
//! the project's own gotchas.
//!
//! [`AcceptBackoff`] rate-limits the error log and, for resource-exhaustion
//! errors only, sleeps a capped exponential backoff (1ms → 100ms). The cap is
//! deliberately modest: several accept loops sleep *inside* a `select!` arm,
//! where the shutdown branch cannot fire mid-sleep — 100ms bounds shutdown
//! latency during a storm while still cutting the spin to ≤10 wakeups/s. Benign
//! per-connection errors (a client that reset mid-handshake, `ECONNABORTED`)
//! are logged but not slept on, so normal accept throughput is unaffected.
//! Call [`AcceptBackoff::reset`] after every successful `accept()`.

use std::time::Duration;

/// Ceiling for the exponential backoff (100ms). Bounded well below the
/// shutdown-responsiveness budget because the sleep runs inside `select!`
/// arms where cancellation cannot preempt it.
const MAX_BACKOFF_MS: u64 = 100;

/// Per-accept-loop backoff + log rate-limiter. One per accept loop; reset on
/// every successful accept so a transient error never permanently slows the
/// loop.
pub(crate) struct AcceptBackoff {
    consecutive: u32,
}

impl AcceptBackoff {
    pub(crate) const fn new() -> Self {
        Self { consecutive: 0 }
    }

    /// Call after a successful `accept()` to clear the error streak.
    #[inline]
    pub(crate) fn reset(&mut self) {
        self.consecutive = 0;
    }

    /// Record an accept error: log it (rate-limited) and, when it indicates
    /// resource exhaustion, sleep a capped exponential backoff so the loop
    /// cannot hot-spin. `context` labels the loop (e.g. `"Accept error"`).
    pub(crate) async fn record_error(&mut self, context: &str, err: &std::io::Error) {
        self.consecutive = self.consecutive.saturating_add(1);
        let exhausted = is_resource_exhaustion(err);

        // Rate-limit: always log the first few, then only at power-of-two
        // counts, so a sustained storm cannot flood the log.
        if self.consecutive <= 3 || self.consecutive.is_power_of_two() {
            if exhausted {
                tracing::error!(
                    "{context}: {err} (resource exhaustion; consecutive={}, backing off)",
                    self.consecutive
                );
            } else {
                tracing::error!("{context}: {err} (consecutive={})", self.consecutive);
            }
        }

        if exhausted {
            let delay = backoff_for(self.consecutive);
            // Use each runtime's native timer directly rather than the boxed
            // `TimerImpl::sleep` (which is `!Send`): the tokio TLS listener runs
            // under `tokio::spawn`, which requires a `Send` future.
            #[cfg(feature = "runtime-tokio")]
            tokio::time::sleep(delay).await;
            #[cfg(feature = "runtime-monoio")]
            monoio::time::sleep(delay).await;
        }
    }
}

/// Capped exponential backoff: 1ms, 2ms, 4ms, … saturating at [`MAX_BACKOFF_MS`].
/// `consecutive` is 1-based (the count returned by the first error is 1).
fn backoff_for(consecutive: u32) -> Duration {
    let shift = consecutive.saturating_sub(1).min(20);
    let ms = (1u64 << shift).min(MAX_BACKOFF_MS);
    Duration::from_millis(ms.max(1))
}

/// Errors that indicate the process/system is out of a resource needed to
/// accept — retrying immediately would just spin. Everything else is treated
/// as a transient per-connection error (logged, loop continues, no sleep).
fn is_resource_exhaustion(err: &std::io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::EMFILE) | Some(libc::ENFILE) | Some(libc::ENOBUFS) | Some(libc::ENOMEM)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_exponentially_then_caps() {
        assert_eq!(backoff_for(1), Duration::from_millis(1));
        assert_eq!(backoff_for(2), Duration::from_millis(2));
        assert_eq!(backoff_for(3), Duration::from_millis(4));
        assert_eq!(backoff_for(4), Duration::from_millis(8));
        // Caps at 100ms (shutdown-responsiveness bound) regardless of streak.
        assert_eq!(backoff_for(8), Duration::from_millis(100));
        assert_eq!(backoff_for(11), Duration::from_millis(100));
        assert_eq!(backoff_for(1000), Duration::from_millis(100));
    }

    #[test]
    fn backoff_never_zero() {
        // Even a bogus 0 count yields a non-zero sleep (never a busy 0-sleep).
        assert!(backoff_for(0) >= Duration::from_millis(1));
    }

    #[test]
    fn reset_clears_streak() {
        let mut b = AcceptBackoff::new();
        b.consecutive = 7;
        b.reset();
        assert_eq!(b.consecutive, 0);
    }

    #[test]
    fn resource_exhaustion_classification() {
        let emfile = std::io::Error::from_raw_os_error(libc::EMFILE);
        let enfile = std::io::Error::from_raw_os_error(libc::ENFILE);
        assert!(is_resource_exhaustion(&emfile));
        assert!(is_resource_exhaustion(&enfile));

        // A reset-by-peer during accept is transient, not exhaustion.
        let aborted = std::io::Error::from_raw_os_error(libc::ECONNABORTED);
        assert!(!is_resource_exhaustion(&aborted));
        // A non-OS error (e.g. synthesized) is not classified as exhaustion.
        let other = std::io::Error::new(std::io::ErrorKind::Other, "synthetic");
        assert!(!is_resource_exhaustion(&other));
    }
}
