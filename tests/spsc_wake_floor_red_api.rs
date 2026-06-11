//! ADD task `spsc-wake-floor` — failing-first suite (compile-red, new API).
//!
//! These tests reference API that does not exist yet and therefore FAIL TO
//! COMPILE (the red state for new surface area). They go green when the build
//! lands:
//!   - `moon::runtime::race::{race2, Arm}` — the allocation-free two-arm race
//!     replacing the monoio loop's timer-only await (M1/M6). NOT monoio::select!.
//!   - `moon::admin::metrics_setup::{bump_spsc_notify_wake, spsc_notify_wakes,
//!     bump_spsc_drain_renotify, spsc_drain_renotify}` — the M5 counters.
//!
//! Run this file alone with: cargo test --test spsc_wake_floor_red_api

use std::time::Duration;

// ---------------------------------------------------------------------------
// race2 — semantics of the two-arm race primitive (M1/M6)
// ---------------------------------------------------------------------------

mod race2_semantics {
    use super::*;
    use moon::runtime::race::{Arm, race2};
    use std::pin::pin;

    /// First arm already Ready -> Arm::First, second arm never completes.
    #[test]
    fn first_arm_wins() {
        let outcome = futures::executor::block_on(async {
            let a = pin!(std::future::ready(7u32));
            let b = pin!(std::future::pending::<u32>());
            race2(a, b).await
        });
        assert!(matches!(outcome, Arm::First(7)));
    }

    /// Second arm Ready while first is Pending -> Arm::Second.
    #[test]
    fn second_arm_wins() {
        let outcome = futures::executor::block_on(async {
            let a = pin!(std::future::pending::<u32>());
            let b = pin!(std::future::ready(9u32));
            race2(a, b).await
        });
        assert!(matches!(outcome, Arm::Second(9)));
    }

    /// The losing arm is merely dropped, never polled after the race resolves —
    /// a notify token delivered to the losing `notified()` arm must remain
    /// consumable on the next iteration (the A3 lost-wake guard, end to end).
    #[test]
    fn losing_notified_arm_keeps_token() {
        let notify = moon::runtime::channel::Notify::new();

        // Round 1: timer arm wins; the losing notified() arm is dropped.
        futures::executor::block_on(async {
            let a = pin!(std::future::ready(1u8)); // timer arm "fires"
            let b = pin!(notify.notified());
            let outcome = race2(a, b).await;
            assert!(matches!(outcome, Arm::First(1)));
        });

        // Token arrives after the race resolved.
        notify.notify_one();

        // Round 2: the token must still be consumable (not lost with the hook).
        let woke = futures::executor::block_on(async {
            let a = pin!(notify.notified());
            let b = pin!(async {
                // generous watchdog so a lost token fails, not hangs
                std::thread::sleep(Duration::from_millis(250));
            });
            matches!(race2(a, b).await, Arm::First(()))
        });
        assert!(woke, "notify token lost across a resolved race");
    }
}

// ---------------------------------------------------------------------------
// M5 counters — runtime-agnostic atomics surfaced through INFO
// ---------------------------------------------------------------------------

mod counters {
    use moon::admin::metrics_setup::{
        bump_spsc_drain_renotify, bump_spsc_notify_wake, spsc_drain_renotify, spsc_notify_wakes,
    };

    #[test]
    fn notify_wake_counter_is_monotonic() {
        let before = spsc_notify_wakes();
        bump_spsc_notify_wake();
        bump_spsc_notify_wake();
        assert!(
            spsc_notify_wakes() >= before + 2,
            "spsc_notify_wakes must count every notify-arm wake"
        );
    }

    #[test]
    fn drain_renotify_counter_is_monotonic() {
        let before = spsc_drain_renotify();
        bump_spsc_drain_renotify();
        assert!(
            spsc_drain_renotify() >= before + 1,
            "spsc_drain_renotify must count every capped-drain self-notify"
        );
    }
}
