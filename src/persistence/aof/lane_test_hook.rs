//! `MOON_TEST_AOF_WRITER_START_DELAY_MS=<ms>` (test-only, R2b round 2 F3):
//! every AOF writer sleeps `<ms>` before its first receive — it neither
//! reads its channel nor hands its append position over — while moon#1266
//! 1A stays on, and `main` skips its boot wait for the hand-over. So a fresh
//! server's first writes provably meet HELD lanes (W2B-1): their replies wait
//! for the writer's barrier ack. Without the hold they would be acknowledged
//! from the channel of a writer that has not started, and a kill -9 on the
//! ack loses them. Read once per process; unset, one cached `Option` load
//! per writer wake.

use std::cell::Cell;
use std::sync::OnceLock;
use std::time::Duration;

/// The delay, if the hook is set.
pub fn writer_start_delay() -> Option<Duration> {
    static DELAY: OnceLock<Option<Duration>> = OnceLock::new();
    *DELAY.get_or_init(|| {
        let ms = std::env::var("MOON_TEST_AOF_WRITER_START_DELAY_MS")
            .ok()?
            .trim()
            .parse::<u64>()
            .ok()?;
        tracing::warn!(
            "MOON_TEST_AOF_WRITER_START_DELAY_MS={ms}: AOF writers keep their append position \
             and start reading their channel {ms} ms late (test hook)"
        );
        Some(Duration::from_millis(ms))
    })
}

thread_local! {
    static STARTED: Cell<bool> = const { Cell::new(false) };
}

/// The writer's first wake (its own thread): sleep the delay, once.
#[inline]
pub(crate) fn delay_writer_start_once() {
    let Some(delay) = writer_start_delay() else {
        return;
    };
    if !STARTED.with(|s| s.replace(true)) {
        std::thread::sleep(delay);
    }
}
