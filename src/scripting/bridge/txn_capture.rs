//! moon#894: a queued script's effect records, captured into the MULTI/EXEC
//! body's own record list.

use std::cell::RefCell;

use bytes::Bytes;

use crate::protocol::Frame;

/// One captured durability record: the db it executed in, and its serialized
/// command bytes (the same shape the MULTI/EXEC executor collects).
pub(crate) type CapturedEffect = (usize, Bytes);

thread_local! {
    /// moon#894: while a MULTI/EXEC body runs a queued script, that script's
    /// effect records land here instead of going straight to the AOF writer
    /// and the replication stream.
    ///
    /// Outside a transaction a script emits each effect the moment its
    /// `redis.call` succeeds, which is the right order because nothing else
    /// runs on the shard meanwhile. Inside `EXEC` the body's OTHER writes are
    /// collected and appended only after the whole body has run. A script
    /// emitting directly would therefore put its effects AHEAD of the writes
    /// queued before it: `SET k 1; EVAL "SET k 2"` would be logged `SET k 2;
    /// SET k 1`, and a restart or a replica would read `1` where the master
    /// answered `2`. Capturing puts the script's records into the body's list
    /// at the script's own position.
    ///
    /// `None` outside a capture. Only the synchronous executor arms it, and
    /// `capture_txn_effects` resets it on every exit path.
    static TXN_EFFECT_CAPTURE: RefCell<Option<Vec<CapturedEffect>>> = const { RefCell::new(None) };
}

/// Run `run` with this thread's script-effect emission diverted into a
/// buffer, and return that buffer in emission order (moon#894).
///
/// For the MULTI/EXEC executor only: `run` must be synchronous (no `.await`
/// can happen inside a closure), so no other connection's script can
/// interleave on this shard thread while the capture is armed.
pub(crate) fn capture_txn_effects<R>(run: impl FnOnce() -> R) -> (R, Vec<CapturedEffect>) {
    /// Disarms the capture even if `run` unwinds, so a panic cannot leave
    /// every later script on this thread writing into a dead buffer.
    struct Disarm;
    impl Drop for Disarm {
        fn drop(&mut self) {
            TXN_EFFECT_CAPTURE.with(|c| c.borrow_mut().take());
        }
    }
    TXN_EFFECT_CAPTURE.with(|c| *c.borrow_mut() = Some(Vec::new()));
    let disarm = Disarm;
    let out = run();
    let captured = TXN_EFFECT_CAPTURE
        .with(|c| c.borrow_mut().take())
        .unwrap_or_default();
    drop(disarm);
    (out, captured)
}

/// Record a script write effect into the armed capture. Returns `false`
/// (nothing done) when no capture is armed.
pub(super) fn capture_txn_effect(db_index: usize, cmd_and_args: &[Frame], reply: &Frame) -> bool {
    TXN_EFFECT_CAPTURE.with(|c| {
        let mut slot = c.borrow_mut();
        let Some(buf) = slot.as_mut() else {
            return false;
        };
        let frame = Frame::Array(crate::protocol::FrameVec::from_vec(cmd_and_args.to_vec()));
        // moon#825: frame AND reply, exactly as `record_effect_write` derives
        // it. No record means the reply proves nothing was written.
        for bytes in crate::persistence::aof::serialize_effect_for_log(&frame, reply) {
            buf.push((db_index, bytes));
        }
        true
    })
}

/// Record an eviction plain-drop `DEL` into the armed capture. Returns
/// `false` when no capture is armed.
#[cfg(feature = "runtime-monoio")]
pub(super) fn capture_txn_del(db_index: usize, key: &[u8]) -> bool {
    TXN_EFFECT_CAPTURE.with(|c| {
        let mut slot = c.borrow_mut();
        let Some(buf) = slot.as_mut() else {
            return false;
        };
        buf.push((db_index, crate::replication::reason_del::serialize_del(key)));
        true
    })
}
