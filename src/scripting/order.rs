//! One order for script-cache and function-registry mutations across shards
//! (moon#1235).
//!
//! Each shard thread owns its own [`crate::scripting::ScriptCache`] and
//! [`crate::scripting::FunctionRegistry`]; a mutation is applied on the origin
//! shard and replayed on the others over the SPSC mesh. Two mutations issued at
//! the same moment from connections on DIFFERENT shards used to reach the other
//! shards in different orders — each origin replays on its own rings — so a
//! `SCRIPT LOAD` racing a `SCRIPT FLUSH` left some shards holding the script
//! and others not, permanently, with both clients told `+OK`. redis has one
//! cache and one registry, so its state never depends on which shard answers.
//!
//! The two stores need different tools, because they are different kinds of
//! state:
//!
//! * **The script cache** only ever grows by a body keyed by its own digest,
//!   and shrinks only by a whole-cache flush. That makes an order-FREE rule
//!   possible, which is also the only one that covers `EVAL`'s implicit cache
//!   insert: every flush takes a fresh number from [`next_script_flush_epoch`],
//!   every insert is tagged with the number current when it was issued
//!   ([`script_flush_epoch`]), and a shard keeps an insert only while its tag is
//!   at least the newest flush it has applied (`ScriptCache::flush_at` drops
//!   the older entries; `store_at` refuses an older insert that arrives
//!   late). Whatever order the messages arrive in, every shard ends holding
//!   exactly the bodies inserted at or after the newest flush — the same set.
//!   Each insert is linearized at its tag read and each flush at its bump, both
//!   inside their commands, so the result is also one redis could produce.
//!
//! * **The function registry** is a general state machine: `LOAD` without
//!   `REPLACE` fails when the library exists, function names collide ACROSS
//!   libraries, `DELETE` removes one library. No per-message rule makes that
//!   converge, so its mutations are SERIALIZED instead: a mutation holds the
//!   process-wide [`FunctionOrderGuard`] from before its local apply until
//!   every other shard has acknowledged it. The next mutation, from whichever
//!   shard, starts only after the previous one is installed everywhere, so all
//!   shards apply the same sequence, and the origin's local validation sees
//!   the same registry every shard has — `LOAD` refuses an existing library
//!   server-wide, exactly as redis does.
//!
//! The guard is a single-token `flume` channel, not a lock: it is held across
//! the fan-out's awaits, which a `parking_lot` lock must never be, and flume's
//! cross-thread wake reaches a `!Send` monoio task (the reply path's oneshots
//! rely on the same property). The waiting is bounded by the caller.
//!
//! None of this is on a data path: `SCRIPT`/`FUNCTION` mutations are
//! administrative, and `EVAL` reads the epoch only when a body is new to the
//! shard (the one time it fans the body out).

use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// The newest `SCRIPT FLUSH` number handed out, process-wide.
///
/// SeqCst on a cold path: the argument above needs a load that follows a
/// completed flush to observe its bump, and there is no reason to reason about
/// anything weaker here.
static SCRIPT_FLUSH_EPOCH: AtomicU64 = AtomicU64::new(0);

/// The flush epoch a script-cache insert issued NOW is tagged with.
#[inline]
pub fn script_flush_epoch() -> u64 {
    SCRIPT_FLUSH_EPOCH.load(Ordering::SeqCst)
}

/// Take a fresh epoch for a `SCRIPT FLUSH` issued now. Strictly greater than
/// every epoch handed out before it.
#[inline]
pub fn next_script_flush_epoch() -> u64 {
    SCRIPT_FLUSH_EPOCH.fetch_add(1, Ordering::SeqCst) + 1
}

/// The single token that orders function-registry mutations.
struct Token {
    tx: flume::Sender<()>,
    rx: flume::Receiver<()>,
}

static FUNCTION_ORDER: LazyLock<Token> = LazyLock::new(|| {
    let (tx, rx) = flume::bounded(1);
    // The channel is empty and bounded(1): this cannot fail.
    let _ = tx.try_send(());
    Token { tx, rx }
});

/// Proof that this task holds the function-mutation token. Dropping it hands
/// the token to the next waiter — on every exit path, including an early
/// return or an unwinding panic.
#[must_use = "the token is released as soon as the guard is dropped"]
pub struct FunctionOrderGuard {
    _private: (),
}

impl Drop for FunctionOrderGuard {
    fn drop(&mut self) {
        // The token is out of the channel while a guard exists, so the
        // bounded(1) channel has room: this cannot fail.
        let _ = FUNCTION_ORDER.tx.try_send(());
    }
}

/// Take the function-mutation token, waiting at most `timeout`.
///
/// `None` when it did not come free in time — some other mutation is still
/// fanning out, which only a wedged shard makes last that long. The caller
/// reports that to its client instead of applying the mutation out of order.
pub async fn acquire_function_order(timeout: Duration) -> Option<FunctionOrderGuard> {
    use crate::runtime::race::{Arm, race2};
    if FUNCTION_ORDER.rx.try_recv().is_ok() {
        return Some(FunctionOrderGuard { _private: () });
    }
    let recv = std::pin::pin!(FUNCTION_ORDER.rx.recv_async());
    #[cfg(feature = "runtime-tokio")]
    let sleep = std::pin::pin!(tokio::time::sleep(timeout));
    #[cfg(feature = "runtime-monoio")]
    let sleep = std::pin::pin!(monoio::time::sleep(timeout));
    // `race2` polls the token arm first, so a token that is ready wins the
    // tie. A losing `RecvFut` deregisters on drop and re-queues a token it
    // was handed but never returned (see `runtime::race`), so a timeout can
    // never swallow the token.
    match race2(recv, sleep).await {
        Arm::First(Ok(())) => Some(FunctionOrderGuard { _private: () }),
        // The sender lives in a static and is never dropped.
        Arm::First(Err(_)) | Arm::Second(()) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flush_epochs_are_strictly_increasing_and_visible() {
        let before = script_flush_epoch();
        let a = next_script_flush_epoch();
        let b = next_script_flush_epoch();
        assert!(a > before, "a flush epoch must exceed every earlier tag");
        assert!(b > a, "flush epochs are unique and increasing");
        assert!(
            script_flush_epoch() >= b,
            "an insert issued after a flush is tagged at or past it"
        );
    }

    /// Test-only: run `f` on a fresh current-thread runtime of the configured
    /// flavour.
    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        #[cfg(feature = "runtime-monoio")]
        {
            monoio::RuntimeBuilder::<monoio::LegacyDriver>::new()
                .enable_timer()
                .build()
                .expect("monoio runtime")
                .block_on(f)
        }
        #[cfg(all(feature = "runtime-tokio", not(feature = "runtime-monoio")))]
        {
            tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("tokio runtime")
                .block_on(f)
        }
    }

    /// The token is exclusive, a waiter times out while it is held, and
    /// dropping the guard hands it on — including across threads, which is
    /// how two shards' connections contend for it.
    #[test]
    fn the_function_token_is_exclusive_bounded_and_released_on_drop() {
        block_on(async {
            let held = acquire_function_order(Duration::from_secs(5))
                .await
                .expect("the token starts free");
            assert!(
                acquire_function_order(Duration::from_millis(20))
                    .await
                    .is_none(),
                "a second holder must wait while the token is out"
            );
            drop(held);
            let again = acquire_function_order(Duration::from_millis(200)).await;
            assert!(again.is_some(), "dropping the guard releases the token");
            drop(again);
        });

        // Cross-thread: a holder on another thread releases to a waiter here.
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        let holder = std::thread::spawn(move || {
            block_on(async {
                let g = acquire_function_order(Duration::from_secs(5))
                    .await
                    .expect("free token");
                held_tx.send(()).expect("signal held");
                go_rx.recv().expect("wait for go");
                drop(g);
            });
        });
        held_rx.recv().expect("holder took the token");
        block_on(async {
            assert!(
                acquire_function_order(Duration::from_millis(20))
                    .await
                    .is_none(),
                "held on another thread"
            );
            go_tx.send(()).expect("release");
            assert!(
                acquire_function_order(Duration::from_secs(5))
                    .await
                    .is_some(),
                "a release on another thread wakes the waiter here"
            );
        });
        holder.join().expect("holder thread");
    }
}
