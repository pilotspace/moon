//! Stopping the AOF writers at the end of a graceful shutdown (moon#1274).
//!
//! redis's `prepareForShutdown` flushes the AOF buffer and fsyncs before the
//! process exits. moon's writers are threads fed by bounded channels, and the
//! old shutdown lost what they still held: it sent them `Shutdown` with a
//! `try_send` BEFORE the shards stopped, cancelled their token together with
//! the server's (the tokio writers left at once), and joined only the shard
//! threads. The process then exited with acknowledged `appendfsync everysec`
//! appends still queued — a SIGTERM right after 100 SETs lost all 100 at
//! `--shards 1` (monoio), and some at `--shards 4` on both runtimes.
//!
//! Now `main.rs` joins the shards first, so no append can arrive after that,
//! and only then calls [`stop_writers`]: each writer gets `Shutdown` BEHIND
//! everything queued (a blocking send, not a `try_send` that a full channel
//! drops), writes it in FIFO order, fsyncs, and exits, and the process
//! waits for that. A rewrite fold in flight drains its spill buffer
//! (`RewriteOverflow`) into the incr before the writer reads on — that drain
//! can swallow the `Shutdown`, so it is re-sent until the writer is gone.
//! The writers have their own token (not the server's), cancelled only if a
//! writer is still running after [`STOP_BOUND`] (one wedged on a fold whose
//! shards are gone): then the error is logged and the process exits
//! without it.

use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tracing::{error, info};

use super::AofWriterPool;
use crate::runtime::cancel::CancellationToken;

/// How long the exit waits for the writers to drain and fsync.
pub const STOP_BOUND: Duration = Duration::from_secs(60);

/// How often `Shutdown` is re-sent to a writer still running.
const RESEND_EVERY: Duration = Duration::from_millis(500);

/// Drain, fsync and join every AOF writer. Call once every producer (every
/// shard thread) has stopped; `token` is the writers' own.
pub fn stop_writers(pool: &AofWriterPool, writers: Vec<JoinHandle<()>>, token: &CancellationToken) {
    let started = Instant::now();
    let deadline = started + STOP_BOUND;
    pool.broadcast_shutdown(deadline);
    let mut sent = Instant::now();
    loop {
        let running = writers.iter().filter(|w| !w.is_finished()).count();
        if running == 0 {
            break;
        }
        let now = Instant::now();
        if now >= deadline {
            error!(
                "{running} AOF writer(s) still running {STOP_BOUND:?} into shutdown; exiting \
                 without them — records they had not written yet are lost"
            );
            token.cancel();
            return;
        }
        if now - sent >= RESEND_EVERY {
            pool.broadcast_shutdown(now);
            sent = now;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    for writer in writers {
        if writer.join().is_err() {
            error!("an AOF writer thread panicked during shutdown");
        }
    }
    info!("AOF writers drained and synced in {:?}", started.elapsed());
}
