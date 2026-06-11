//! Allocation-free two-arm race future (spsc-wake-floor, M1/M6).
//!
//! The monoio shard loop needs to wake on EITHER its periodic timer OR a
//! cross-shard `Notify` — but `monoio::select!` is banned in this codebase
//! (known memory leak), and the tokio `select!` macro is runtime-specific.
//! `race2` is the minimal hand-rolled alternative: poll the first arm, then
//! the second, resolve on the first `Ready`. No allocation, no waker
//! wrapping — both arms see the caller's `Context` directly, so whichever
//! wakes last still wakes the race.
//!
//! The losing arm is NOT polled after the race resolves; the caller drops it.
//! Dropping a flume `RecvFut` deregisters its hook and re-queues an
//! undelivered token (pinned by `swf_a3_notify_token_survives_poll_drop` and
//! `losing_notified_arm_keeps_token`), so a race loop over
//! `Notify::notified()` has no lost-wake window.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Which arm of a [`race2`] completed first.
#[derive(Debug)]
pub enum Arm<A, B> {
    /// The first arm resolved (it has priority on simultaneous readiness).
    First(A),
    /// The second arm resolved while the first was still pending.
    Second(B),
}

/// Future returned by [`race2`].
pub struct Race2<'a, A: Future, B: Future> {
    a: Pin<&'a mut A>,
    b: Pin<&'a mut B>,
}

/// Race two pinned futures; resolves with the first `Ready` arm. The first
/// arm is polled first on every wake, so it wins ties deterministically.
pub fn race2<'a, A: Future, B: Future>(a: Pin<&'a mut A>, b: Pin<&'a mut B>) -> Race2<'a, A, B> {
    Race2 { a, b }
}

impl<A: Future, B: Future> Future for Race2<'_, A, B> {
    type Output = Arm<A::Output, B::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Race2 holds `Pin<&mut _>` fields, which are always Unpin, so the
        // struct itself is Unpin and `get_mut` is safe-by-construction.
        let this = self.get_mut();
        if let Poll::Ready(out) = this.a.as_mut().poll(cx) {
            return Poll::Ready(Arm::First(out));
        }
        if let Poll::Ready(out) = this.b.as_mut().poll(cx) {
            return Poll::Ready(Arm::Second(out));
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::pin;

    #[test]
    fn first_arm_priority_on_double_ready() {
        let outcome = futures::executor::block_on(async {
            let a = pin!(std::future::ready(1u8));
            let b = pin!(std::future::ready(2u8));
            race2(a, b).await
        });
        assert!(matches!(outcome, Arm::First(1)));
    }

    #[test]
    fn second_arm_resolves_when_first_pending() {
        let outcome = futures::executor::block_on(async {
            let a = pin!(std::future::pending::<u8>());
            let b = pin!(std::future::ready(2u8));
            race2(a, b).await
        });
        assert!(matches!(outcome, Arm::Second(2)));
    }

    #[test]
    fn pending_then_woken_arm_resolves() {
        // A future that is Pending once, then Ready — the race must pass the
        // caller's waker through so the wake reaches the executor.
        struct ReadyOnSecondPoll(bool);
        impl Future for ReadyOnSecondPoll {
            type Output = u8;
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u8> {
                if self.0 {
                    Poll::Ready(7)
                } else {
                    self.0 = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }
        let outcome = futures::executor::block_on(async {
            let a = pin!(std::future::pending::<u8>());
            let b = pin!(ReadyOnSecondPoll(false));
            race2(a, b).await
        });
        assert!(matches!(outcome, Arm::Second(7)));
    }
}
