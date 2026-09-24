//! Off-shard-thread execution for `CDC.READ` (moon#1181).
//!
//! `CDC.READ` reads WAL segment files and encodes Debezium envelopes; it
//! touches no shard state, yet it ran inline on the shard event loop, so a
//! poll stalled every connection on that shard for the duration of its disk
//! reads. The connection task now hands the read to this small dedicated
//! pool and awaits the reply (a `flume` channel — its cross-thread wake
//! reaches both runtimes' tasks), so its siblings on the shard keep running.
//!
//! Same shape as `storage::tiered::cold_read_pool`: lazily spawned on first
//! use, fixed worker count, unbounded queue (a backlogged disk is the
//! bottleneck, not the queue). If no worker could be spawned the caller runs
//! the read inline — slower, never lost.

use std::sync::OnceLock;

/// Worker threads. Two: enough that one slow poll does not serialize every
/// consumer behind it, few enough to stay out of the shards' way.
const CDC_READ_POOL_THREADS: usize = 2;

type Job = Box<dyn FnOnce() + Send + 'static>;

static JOBS: OnceLock<Option<flume::Sender<Job>>> = OnceLock::new();

fn job_sender() -> Option<&'static flume::Sender<Job>> {
    JOBS.get_or_init(|| {
        let (tx, rx) = flume::unbounded::<Job>();
        let mut spawned = 0usize;
        for i in 0..CDC_READ_POOL_THREADS {
            let rx = rx.clone();
            let name = format!("moon-cdc-read-{i}");
            let started = std::thread::Builder::new()
                .name(name.clone())
                .spawn(move || {
                    // Spawned lazily from a pinned shard thread: re-pin to the
                    // non-shard cores before serving anything.
                    crate::shard::numa::pin_current_aux_thread(&name);
                    for job in rx.iter() {
                        job();
                    }
                });
            if started.is_ok() {
                spawned += 1;
            }
        }
        (spawned > 0).then_some(tx)
    })
    .as_ref()
}

/// Run `job` on the pool. `Err(job)` hands it back when the pool has no
/// worker; the caller then runs it inline.
pub(crate) fn submit<F>(job: F) -> Result<(), F>
where
    F: FnOnce() + Send + 'static,
{
    let Some(tx) = job_sender() else {
        return Err(job);
    };
    // A send can only fail if every worker exited (they never do); the job
    // is lost with the boxed closure, so report it as not accepted by
    // running nothing and letting the caller's reply channel disconnect.
    let _ = tx.send(Box::new(job));
    Ok(())
}
