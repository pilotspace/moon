//! `SHUTDOWN ABORT` (moon#1264): cancel a shutdown that is still saving.
//!
//! A `SHUTDOWN` with a save (save points configured, or `SHUTDOWN SAVE`), and
//! a SIGTERM / SIGINT with save points (moon#1263), wait for a save already
//! running and then run their own before the server exits (`save_wait`:
//! while the save progresses, and a signal's for good). That is the window redis
//! 7 calls "a shutdown in progress": `SHUTDOWN ABORT` from another client
//! cancels it, answers `+OK`, and the waiting `SHUTDOWN` gets
//! `-ERR Errors trying to SHUTDOWN. Check logs.`; the server stays up. With no
//! shutdown in progress it answers `-ERR No shutdown in progress.`. (redis
//! 7.0.15, measured: a master waiting for a stopped replica.)
//!
//! One decision per shutdown (review F2): a pending shutdown ends either
//! COMMITTED (its save is on disk and it exits) or ABORTED, and the two are
//! decided under one lock, so only the winner replies. An abort takes every
//! shutdown still pending at that instant; a shutdown that committed first
//! is no longer pending, and the abort answers that none is in progress.
//! (The two used to be separate atomics: an abort landing in the poll where
//! the save completed answered `+OK` for a shutdown that then exited.)
//!
//! A save a cancelled shutdown already started keeps running to completion
//! in the background — a cooperative snapshot cannot be stopped mid-shard —
//! which is harmless: it is an ordinary BGSAVE.

use bytes::Bytes;
use parking_lot::Mutex;
use tracing::warn;

use crate::protocol::Frame;

/// What a `SHUTDOWN` that `SHUTDOWN ABORT` cancelled answers (redis 7.0.15).
pub const SHUTDOWN_ABORTED_ERR: &[u8] = b"ERR Errors trying to SHUTDOWN. Check logs.";

/// What `SHUTDOWN ABORT` answers with no shutdown in progress (redis 7.0.15,
/// period included).
const NO_SHUTDOWN_IN_PROGRESS_ERR: &[u8] = b"ERR No shutdown in progress.";

/// The shutdowns in the window, and the aborts that found one there.
struct Window {
    /// Shutdowns entered and not yet decided.
    pending: usize,
    /// Aborts that found a shutdown pending (a generation).
    aborts: u64,
}

static WINDOW: Mutex<Window> = Mutex::new(Window {
    pending: 0,
    aborts: 0,
});

/// One shutdown in the window `SHUTDOWN ABORT` can cancel. It leaves the
/// window when [`Pending::commit`] decides it, or undecided on drop (its
/// save failed or was aborted: the server stays up either way).
pub(super) struct Pending {
    aborts_at_start: u64,
    decided: bool,
}

impl Pending {
    /// Enter the window.
    pub(super) fn enter() -> Self {
        let mut window = WINDOW.lock();
        window.pending += 1;
        Self {
            aborts_at_start: window.aborts,
            decided: false,
        }
    }

    /// Has a `SHUTDOWN ABORT` cancelled this shutdown? (A poll hint; the
    /// decision is [`Self::commit`].)
    pub(super) fn aborted(&self) -> bool {
        WINDOW.lock().aborts != self.aborts_at_start
    }

    /// The decision, once the shutdown's save is on disk: exit (`Ok`) unless
    /// an abort already took this shutdown, in which case the caller answers
    /// the aborted reply and stays up. From `Ok` on no abort can claim it.
    pub(super) fn commit(mut self) -> Result<(), Frame> {
        let mut window = WINDOW.lock();
        window.pending -= 1;
        self.decided = true;
        if window.aborts == self.aborts_at_start {
            Ok(())
        } else {
            Err(aborted_reply())
        }
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if !self.decided {
            WINDOW.lock().pending -= 1;
        }
    }
}

/// `SHUTDOWN ABORT`: cancel every pending shutdown, or say there is none.
pub(super) fn abort() -> Frame {
    {
        let mut window = WINDOW.lock();
        if window.pending == 0 {
            return Frame::Error(Bytes::from_static(NO_SHUTDOWN_IN_PROGRESS_ERR));
        }
        window.aborts += 1;
    }
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
    WINDOW.lock().pending
}
