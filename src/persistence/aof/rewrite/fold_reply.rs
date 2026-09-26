//! Asking a shard for its cooperative rewrite snapshot (`AofFold`), and
//! waiting for the answer — unless the shard is gone (moon#1274 round 3, A1).
//!
//! The writer pushes `AofFold { reply_tx }` into the shard's SPSC ring and
//! polls `reply_rx`. A shard that has stopped never answers, and never drops
//! `reply_tx` either: the ring outlives the shard, because the pool holds its
//! producer. A rewrite dispatched just before a graceful shutdown then
//! wedged its writer here, and every other writer at the rewrite barrier, and
//! the exit waited out `aof::writer_stop`'s whole bound (60 s).
//!
//! A shard's exit drops its ring CONSUMER, which releases the ring's read
//! hold: `read_is_held()` on the producer turns false. So the push refuses a
//! ring nobody reads, and the wait gives up once the reader is gone (after
//! one last look for an answer sent just before it left). The fold then
//! fails; the rewrite aborts through the coordinator, the old generation
//! stays authoritative, and every writer drains its channel and spill buffer
//! into the OLD incr and exits.

use ringbuf::traits::{Observer, Producer};
use tracing::warn;

use crate::persistence::aof::AofError;
use crate::runtime::channel::Notify;
use crate::shard::dispatch::{AofFoldSnapshot, ShardMessage};

/// The fold ring's producer, as the writers hold it.
pub(crate) type FoldProducer = parking_lot::Mutex<ringbuf::HeapProd<ShardMessage>>;

/// Push `AofFold` into the shard's ring and wait for its snapshot. `who`
/// names the shard in errors and logs.
pub(crate) fn request_fold_snapshot(
    fold_producer: &FoldProducer,
    fold_notifier: &Notify,
    who: &str,
) -> Result<AofFoldSnapshot, AofError> {
    let failed = |why: &str| AofError::RewriteFailed {
        detail: format!("{who}: {why} — fold aborted, the old generation stays authoritative"),
    };
    let (reply_tx, reply_rx) = crate::runtime::channel::oneshot::<AofFoldSnapshot>();
    {
        let mut prod = fold_producer.lock();
        if !prod.read_is_held() {
            return Err(failed(
                "the shard has stopped (nobody reads its AofFold ring)",
            ));
        }
        prod.try_push(ShardMessage::AofFold { reply_tx })
            .map_err(|_| failed("AofFold SPSC ring full"))?;
    }
    fold_notifier.notify_one();

    // Poll with try_recv + sleep (this runs on the writer's own OS thread),
    // warning once if the reply is slow (a stalled or starved shard).
    let wait_start = std::time::Instant::now();
    let mut warned = false;
    loop {
        match reply_rx.try_recv() {
            Ok(snapshot) => return Ok(snapshot),
            Err(flume::TryRecvError::Disconnected) => {
                return Err(failed("AofFold reply channel dropped (shard shut down?)"));
            }
            Err(flume::TryRecvError::Empty) => {}
        }
        if !fold_producer.lock().read_is_held() {
            // The shard left. An answer it sent on the way out still counts.
            return reply_rx
                .try_recv()
                .map_err(|_| failed("the shard stopped before serving AofFold"));
        }
        if !warned && wait_start.elapsed() >= std::time::Duration::from_millis(500) {
            warned = true;
            warn!(
                "{who}: waiting for AofFold snapshot ({:.1}s elapsed) — shard event loop may \
                 be stalled or fold consumer not draining",
                wait_start.elapsed().as_secs_f64()
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

#[cfg(test)]
mod tests {
    use ringbuf::traits::Split;

    use super::*;

    fn ring() -> (FoldProducer, ringbuf::HeapCons<ShardMessage>) {
        let (prod, cons) = ringbuf::HeapRb::<ShardMessage>::new(4).split();
        (parking_lot::Mutex::new(prod), cons)
    }

    /// A1: the shard stopped before the push — the fold fails at once.
    #[test]
    fn a_ring_nobody_reads_refuses_the_fold() {
        let (prod, cons) = ring();
        drop(cons);
        let notify = Notify::new();
        let t = std::time::Instant::now();
        let r = request_fold_snapshot(&prod, &notify, "shard 1");
        assert!(r.is_err());
        assert!(t.elapsed() < std::time::Duration::from_secs(1));
    }

    /// A1: pushed, then the shard stops without serving it — the wait ends.
    /// (Before: `reply_tx` sat in the orphaned ring forever.)
    #[test]
    fn a_shard_stopping_with_the_fold_unserved_ends_the_wait() {
        let (prod, cons) = ring();
        let notify = Notify::new();
        let stopper = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            drop(cons);
        });
        let t = std::time::Instant::now();
        let r = request_fold_snapshot(&prod, &notify, "shard 1");
        stopper.join().expect("stopper");
        assert!(
            r.is_err(),
            "the fold was answered by a shard that never served it"
        );
        assert!(t.elapsed() < std::time::Duration::from_secs(5));
    }
}
