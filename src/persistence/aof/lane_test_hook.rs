//! `MOON_TEST_AOF_FIRST_OFFER_DELAY_MS=<ms>` (test-only, R2b round 2 F3): the
//! AOF writers do not hand their append position to the shard threads until
//! `<ms>` after the first offer any of them attempts, and `main` skips its
//! boot wait for that hand-over. So the first writes of a fresh server
//! provably meet HELD lanes (moon#1266 1A, W2B-1): without the hold they
//! would be acknowledged before their records were written. Read once per
//! process; unset, the check is one cached `Option` load per offer.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// The delay, if the hook is set.
pub fn first_offer_delay() -> Option<Duration> {
    static DELAY: OnceLock<Option<Duration>> = OnceLock::new();
    *DELAY.get_or_init(|| {
        let ms = std::env::var("MOON_TEST_AOF_FIRST_OFFER_DELAY_MS")
            .ok()?
            .trim()
            .parse::<u64>()
            .ok()?;
        tracing::warn!(
            "MOON_TEST_AOF_FIRST_OFFER_DELAY_MS={ms}: AOF writers keep their append position \
             (lanes held) for {ms} ms after their first offer (test hook)"
        );
        Some(Duration::from_millis(ms))
    })
}

/// Whether a writer may offer its append position now.
#[inline]
pub(crate) fn offer_allowed() -> bool {
    let Some(delay) = first_offer_delay() else {
        return true;
    };
    static FIRST: OnceLock<Instant> = OnceLock::new();
    FIRST.get_or_init(Instant::now).elapsed() >= delay
}
