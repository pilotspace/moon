//! Token-bucket per-IP rate limiter (HARD-03, Phase 137).
//!
//! Protects the admin/console HTTP port from abuse. Defaults are sized for
//! a single-host admin UI:
//! - 1000 req/s sustained refill, burst capacity 2000 (≈ 86 M req/day).
//! - Per-remote-IP bucket kept in a `parking_lot::Mutex<HashMap<IpAddr, Bucket>>`.
//! - Background cleanup tick evicts IPs idle for > 5 minutes (runs every 60s
//!   on the admin tokio runtime; weak reference so the task exits with the
//!   limiter).
//!
//! In-memory only — no Redis roundtrip on the hot path. Acceptable for a
//! single-process admin port; distributed limits are deferred to v0.2.
//!
//! This module is feature-gated behind `console`.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// Per-IP bucket state.
///
/// `last_refill` tracks the monotonic time at which `tokens` was last
/// recomputed. `last_access` is the most recent `check()` time, used for
/// idle eviction.
#[derive(Debug, Clone)]
struct Bucket {
    tokens: f64,
    last_refill: Instant,
    last_access: Instant,
}

/// Token-bucket per-IP rate limiter.
pub struct RateLimiter {
    rate_per_sec: f64,
    burst: f64,
    enabled: bool,
    buckets: Arc<Mutex<HashMap<IpAddr, Bucket>>>,
}

impl RateLimiter {
    /// Build a new limiter and spawn the 60s cleanup task on the current
    /// tokio runtime.
    ///
    /// When `rate_per_sec <= 0.0` the limiter is disabled: [`check`] always
    /// returns `Ok(())`. The cleanup task is not spawned in that case.
    pub fn new(rate_per_sec: f64, burst: f64) -> Arc<Self> {
        let enabled = rate_per_sec > 0.0 && burst > 0.0;
        let limiter = Arc::new(Self {
            rate_per_sec,
            burst,
            enabled,
            buckets: Arc::new(Mutex::new(HashMap::new())),
        });
        if enabled {
            // Spawn cleanup on the current tokio runtime. The weak reference
            // ensures the task exits automatically when the last Arc is dropped.
            let weak = Arc::downgrade(&limiter);
            // `tokio::spawn` requires a tokio runtime context. Callers in
            // tests (`#[tokio::test]`) and in the admin server runtime both
            // satisfy that. If invoked outside a runtime we silently skip.
            if tokio::runtime::Handle::try_current().is_ok() {
                tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(Duration::from_secs(60));
                    loop {
                        ticker.tick().await;
                        let Some(l) = weak.upgrade() else {
                            return;
                        };
                        l.cleanup(Duration::from_secs(300));
                    }
                });
            }
        }
        limiter
    }

    /// Returns `Ok(())` if the request is allowed; `Err(retry_after_secs)`
    /// if the bucket is drained. The `Retry-After` value is at least 1.
    pub fn check(&self, ip: IpAddr) -> Result<(), u32> {
        if !self.enabled {
            return Ok(());
        }
        let now = Instant::now();
        let mut map = self.buckets.lock();
        let bucket = map.entry(ip).or_insert_with(|| Bucket {
            tokens: self.burst,
            last_refill: now,
            last_access: now,
        });
        // Refill proportional to elapsed wall time, clamped at burst capacity.
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.rate_per_sec).min(self.burst);
        bucket.last_refill = now;
        bucket.last_access = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(())
        } else {
            // Seconds until a single token regenerates.
            let deficit = 1.0 - bucket.tokens;
            let retry = (deficit / self.rate_per_sec).ceil() as u32;
            Err(retry.max(1))
        }
    }

    /// Evict bucket entries that have not been accessed within `idle`.
    /// Called by the spawned cleanup task every 60s (idle = 5 min).
    fn cleanup(&self, idle: Duration) {
        let cutoff = Instant::now() - idle;
        self.buckets.lock().retain(|_, b| b.last_access >= cutoff);
    }

    /// Number of tracked IPs (diagnostic/test).
    #[cfg(test)]
    fn bucket_count(&self) -> usize {
        self.buckets.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[tokio::test]
    async fn allows_under_limit() {
        let l = RateLimiter::new(1000.0, 10.0);
        let ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        for _ in 0..10 {
            assert!(l.check(ip).is_ok());
        }
    }

    #[tokio::test]
    async fn blocks_when_burst_drained() {
        // 1 rps refill, burst 2 — third consecutive call is denied because
        // refill between immediate calls is < 1 token.
        let l = RateLimiter::new(1.0, 2.0);
        let ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        assert!(l.check(ip).is_ok());
        assert!(l.check(ip).is_ok());
        let Err(retry) = l.check(ip) else {
            panic!("expected 429")
        };
        assert!(retry >= 1);
    }

    #[tokio::test]
    async fn distinct_ips_have_separate_buckets() {
        let l = RateLimiter::new(1.0, 1.0);
        let a = IpAddr::V4(Ipv4Addr::new(1, 0, 0, 1));
        let b = IpAddr::V4(Ipv4Addr::new(1, 0, 0, 2));
        // Each IP gets its own burst: both first calls succeed.
        assert!(l.check(a).is_ok());
        assert!(l.check(b).is_ok());
        // Each is now drained; second call on either one fails.
        assert!(l.check(a).is_err());
        assert!(l.check(b).is_err());
    }

    #[tokio::test]
    async fn cleanup_evicts_idle() {
        let l = RateLimiter::new(1000.0, 10.0);
        let ip = IpAddr::V4(Ipv4Addr::new(9, 9, 9, 9));
        let _ = l.check(ip);
        assert_eq!(l.bucket_count(), 1);
        // Idle = 0 evicts everything.
        l.cleanup(Duration::ZERO);
        assert_eq!(l.bucket_count(), 0);
    }

    #[tokio::test]
    async fn disabled_rate_zero_always_ok() {
        let l = RateLimiter::new(0.0, 0.0);
        let ip = IpAddr::V4(Ipv4Addr::new(5, 5, 5, 5));
        for _ in 0..10_000 {
            assert!(l.check(ip).is_ok());
        }
        // Disabled limiter does not spawn buckets.
        assert_eq!(l.bucket_count(), 0);
    }

    #[tokio::test]
    async fn retry_after_scales_with_refill_rate() {
        // Very slow refill — retry-after should be >= 1 second.
        let l = RateLimiter::new(0.5, 1.0);
        let ip = IpAddr::V4(Ipv4Addr::new(7, 7, 7, 7));
        assert!(l.check(ip).is_ok());
        let Err(retry) = l.check(ip) else {
            panic!("expected 429")
        };
        assert!(retry >= 1, "retry-after should be at least 1 second");
    }
}
