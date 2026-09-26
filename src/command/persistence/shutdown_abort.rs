//! `SHUTDOWN ABORT` (moon#1264): cancel a shutdown that is still saving.
//!
//! A `SHUTDOWN` with a save (save points configured, or `SHUTDOWN SAVE`), and
//! a SIGTERM / SIGINT with save points (moon#1263), wait for a save already
//! running and then run their own before the server exits — up to one 20 s
//! deadline ([`super::SHUTDOWN_SAVE_DEADLINE_MS`]). That is the window redis
//! 7 calls "a shutdown in progress": `SHUTDOWN ABORT` from another client
//! cancels it, answers `+OK`, and the waiting `SHUTDOWN` gets
//! `-ERR Errors trying to SHUTDOWN. Check logs.`; the server stays up. With no
//! shutdown in progress it answers `-ERR No shutdown in progress.`. (redis
//! 7.0.15, measured: a master waiting for a stopped replica.)
//!
//! Two counters, no lock: [`Pending`] counts the shutdowns in that window,
//! and every abort that found one bumps the abort generation, which each
//! pending shutdown compares with the generation it started under. A save a
//! cancelled shutdown already started keeps running to completion in the
//! background — a cooperative snapshot cannot be stopped mid-shard — which
//! is harmless: it is an ordinary BGSAVE.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;
use tracing::warn;

use crate::protocol::Frame;

/// What a `SHUTDOWN` that `SHUTDOWN ABORT` cancelled answers (redis 7.0.15).
pub const SHUTDOWN_ABORTED_ERR: &[u8] = b"ERR Errors trying to SHUTDOWN. Check logs.";

/// What `SHUTDOWN ABORT` answers with no shutdown in progress (redis 7.0.15,
/// period included).
const NO_SHUTDOWN_IN_PROGRESS_ERR: &[u8] = b"ERR No shutdown in progress.";

/// Shutdowns now waiting for a save or running their own.
static PENDING: AtomicUsize = AtomicUsize::new(0);

/// Aborts that found a shutdown pending.
static ABORTS: AtomicU64 = AtomicU64::new(0);

/// One shutdown in the window `SHUTDOWN ABORT` can cancel; leaves it on drop.
pub(super) struct Pending {
    aborts_at_start: u64,
}

impl Pending {
    /// Enter the window. The generation is read BEFORE the count goes up: an
    /// abort that sees this shutdown pending then bumps past it, and one that
    /// ran earlier saw no shutdown and answered so.
    pub(super) fn enter() -> Self {
        let aborts_at_start = ABORTS.load(Ordering::SeqCst);
        PENDING.fetch_add(1, Ordering::SeqCst);
        Self { aborts_at_start }
    }

    /// Has a `SHUTDOWN ABORT` cancelled this shutdown?
    pub(super) fn aborted(&self) -> bool {
        ABORTS.load(Ordering::SeqCst) != self.aborts_at_start
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        PENDING.fetch_sub(1, Ordering::SeqCst);
    }
}

/// `SHUTDOWN ABORT`: cancel every pending shutdown, or say there is none.
pub(super) fn abort() -> Frame {
    if PENDING.load(Ordering::SeqCst) == 0 {
        return Frame::Error(Bytes::from_static(NO_SHUTDOWN_IN_PROGRESS_ERR));
    }
    ABORTS.fetch_add(1, Ordering::SeqCst);
    warn!("Shutdown manually aborted.");
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// The reply of a shutdown that was aborted.
pub(super) fn aborted_reply() -> Frame {
    Frame::Error(Bytes::from_static(SHUTDOWN_ABORTED_ERR))
}

/// Shutdowns pending right now (tests).
#[cfg(test)]
pub(super) fn pending_for_test() -> usize {
    PENDING.load(Ordering::SeqCst)
}
