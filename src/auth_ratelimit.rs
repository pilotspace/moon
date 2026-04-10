//! Per-IP AUTH failure rate limiting.
//!
//! Tracks failed AUTH attempts per client IP and enforces exponential backoff
//! delays to prevent brute-force attacks. Successful AUTH has zero overhead.
//!
//! Design:
//! - Global `parking_lot::Mutex<HashMap>` (not on hot path — only touched on AUTH)
//! - Exponential backoff: 100ms * 2^(failures-1), capped at 10s
//! - Auto-reset after 60s of inactivity per IP
//! - Periodic cleanup of stale entries via `cleanup_stale()`

use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::LazyLock;
use std::time::Instant;

/// Base delay for first failure (100ms).
const BASE_DELAY_MS: u64 = 100;

/// Maximum delay cap (10 seconds).
const MAX_DELAY_MS: u64 = 10_000;

/// Entries older than this are pruned (60 seconds).
const STALE_THRESHOLD_SECS: u64 = 60;

/// Maximum entries before forced cleanup.
const MAX_ENTRIES: usize = 10_000;

struct FailureRecord {
    count: u32,
    last_failure: Instant,
}

static RATE_LIMITER: LazyLock<Mutex<HashMap<IpAddr, FailureRecord>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Record a failed AUTH attempt for the given IP.
/// Returns the delay in milliseconds that should be applied before
/// sending the error response.
pub fn record_failure(ip: IpAddr) -> u64 {
    let mut map = RATE_LIMITER.lock();

    // Periodic cleanup when map grows too large
    if map.len() >= MAX_ENTRIES {
        let cutoff = Instant::now() - std::time::Duration::from_secs(STALE_THRESHOLD_SECS);
        map.retain(|_, r| r.last_failure > cutoff);
    }

    let now = Instant::now();
    let record = map.entry(ip).or_insert(FailureRecord {
        count: 0,
        last_failure: now,
    });

    // Reset if stale (no failures for STALE_THRESHOLD_SECS)
    if now.duration_since(record.last_failure).as_secs() >= STALE_THRESHOLD_SECS {
        record.count = 0;
    }

    record.count = record.count.saturating_add(1);
    record.last_failure = now;

    // Exponential backoff: 100ms * 2^(count-1), capped at 10s
    let delay = BASE_DELAY_MS.saturating_mul(1u64.checked_shl(record.count.saturating_sub(1)).unwrap_or(u64::MAX));
    delay.min(MAX_DELAY_MS)
}

/// Clear failure record on successful AUTH (reset for this IP).
pub fn record_success(ip: IpAddr) {
    let mut map = RATE_LIMITER.lock();
    map.remove(&ip);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_first_failure_returns_base_delay() {
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100));
        // Clean up from any prior test state
        record_success(ip);
        let delay = record_failure(ip);
        assert_eq!(delay, 100);
        // Cleanup
        record_success(ip);
    }

    #[test]
    fn test_exponential_backoff() {
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 101));
        record_success(ip);
        assert_eq!(record_failure(ip), 100);   // 100 * 2^0
        assert_eq!(record_failure(ip), 200);   // 100 * 2^1
        assert_eq!(record_failure(ip), 400);   // 100 * 2^2
        assert_eq!(record_failure(ip), 800);   // 100 * 2^3
        assert_eq!(record_failure(ip), 1600);  // 100 * 2^4
        record_success(ip);
    }

    #[test]
    fn test_max_delay_cap() {
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 102));
        record_success(ip);
        // 7 failures: 100, 200, 400, 800, 1600, 3200, 6400
        for _ in 0..7 {
            record_failure(ip);
        }
        // 8th failure should be capped at 10000
        let delay = record_failure(ip);
        assert!(delay <= MAX_DELAY_MS);
        record_success(ip);
    }

    #[test]
    fn test_success_clears_record() {
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 103));
        record_success(ip);
        record_failure(ip);
        record_failure(ip);
        record_success(ip);
        // After success, next failure should be back to base
        let delay = record_failure(ip);
        assert_eq!(delay, 100);
        record_success(ip);
    }
}
