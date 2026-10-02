//! The AOF writer's side of the moon#1266 1A lane (`aof::lane`): the four
//! writer loops call these at fixed points, so the append position moves
//! between the writer and the producers the same way in every loop.
//!
//! - [`top_of_wake`] at the top of every wake: under `always` the position is
//!   the writer's (its group commit acks after the fsync) and the lane is
//!   held, so the producers take the acked path (W2B-1);
//! - [`reclaim`] before the writer handles any message, and before every
//!   stop path: the position (and its `RecordCtx`) is the writer's again,
//!   with whatever the producers buffered written first — so rewrites,
//!   generation switches, overflow drains, `always` batches and the
//!   clean-close marker all run exactly as in Option 3;
//! - [`on_wake`] on every wake, before the everysec deadline check: direct
//!   writes owe the deadline (learned BEFORE the deadline's `claim`, so the
//!   R1 heal rule counts only writes made after a failure was learned);
//! - [`offer`] at the top of every wake, before the receive (so the boot and
//!   a policy that just left `always` hand the position over at once, not
//!   after the next record — W2B-1), and again at the end of a wake: hand the
//!   position to the producers when nothing is in flight, which clears the
//!   lane's hold;
//! - [`park`]: the monoio writer parks in its receive while the shard threads
//!   write their own records (DIRECT) or wait for its acks (held); it
//!   warm-polls, as in Option 3, only while records reach it with no ack to
//!   wait for (a fold, a latched write error).

use super::*;
use crate::persistence::aof::lane::AofLane;

/// The append position is the writer's again (see the module doc). A failed
/// direct write latches the writer's `write_error`: the stream may be torn.
pub(super) fn reclaim(lane: &AofLane, ctx: &mut RecordCtx, write_error: &mut bool) {
    if !lane.is_on() {
        return;
    }
    adopt(lane.take_back(), ctx, write_error);
}

/// What a take-back hands the writer: its context, and a failed direct
/// write's latch.
fn adopt(
    back: crate::persistence::aof::lane_protocol::TakenBack<
        crate::persistence::aof::lane::DirectCtx,
    >,
    ctx: &mut RecordCtx,
    write_error: &mut bool,
) {
    if let Some(direct) = back.ctx {
        *ctx = direct.rec;
    }
    if back.write_failed && !*write_error {
        error!("AOF direct write failed earlier: write-error latched. Persistence degraded.");
        *write_error = true;
    }
}

/// Top of every wake, before the receive: `always` (also after a runtime
/// `CONFIG SET`) keeps the position on the writer and holds the lane; any
/// other policy offers it to the producers at once — at the boot (the lane is
/// held from attach until this first hand-over) and on the first wake after
/// the policy left `always` — instead of only after the next record.
pub(super) fn top_of_wake(
    lane: &AofLane,
    rx: &channel::MpscReceiver<AofMessage>,
    fsync: FsyncPolicy,
    write_error: &mut bool,
    ctx: &mut RecordCtx,
    floor: FoldEpoch,
    file: &impl DupFile,
) {
    on_policy(lane, fsync, ctx, write_error);
    offer(lane, rx, fsync, *write_error, ctx, floor, file);
}

/// `always` keeps the position on the writer and holds the lane.
fn on_policy(lane: &AofLane, fsync: FsyncPolicy, ctx: &mut RecordCtx, write_error: &mut bool) {
    if fsync == FsyncPolicy::Always && lane.is_on() {
        // Take the position back and hold the lane under one lock: the
        // producers' replies wait for this writer's acks from now until the
        // next hand-over (also once the policy leaves `always` again).
        adopt(lane.take_back_held(), ctx, write_error);
    }
}

/// Every wake, before the everysec deadline check (see the module doc).
pub(super) fn on_wake(
    lane: &AofLane,
    fsync: FsyncPolicy,
    everysec: &mut EverysecSync,
    idle_wait: &mut IdleWait,
) {
    if lane.take_written() && fsync == FsyncPolicy::EverySec {
        everysec.note_written();
        idle_wait.mark_pending();
    }
}

/// Whether the monoio writer parks in its receive (no warm poll): under
/// `always`, and under 1A unless records reach it with no ack to wait for.
#[cfg(feature = "runtime-monoio")]
pub(super) fn park(lane: &AofLane, fsync: FsyncPolicy) -> bool {
    fsync == FsyncPolicy::Always || (lane.is_on() && !lane.channel_unacked())
}

/// A writer's file the producers can get a handle on.
pub(super) trait DupFile {
    fn dup(&self) -> std::io::Result<std::fs::File>;
}

impl DupFile for std::fs::File {
    fn dup(&self) -> std::io::Result<std::fs::File> {
        self.try_clone()
    }
}

/// The tokio writers: the dup is taken synchronously from the raw handle
/// (no lock is held across an await). Their `BufWriter` is empty at the end
/// of a wake: every batch ends with a flush to the kernel (moon#1266).
#[cfg(feature = "runtime-tokio")]
impl DupFile for tokio::io::BufWriter<tokio::fs::File> {
    fn dup(&self) -> std::io::Result<std::fs::File> {
        dup_tokio_file(self.get_ref())
    }
}

/// Top and end of a wake: hand the position to the producers when nothing
/// is in flight (`AofLane::release` re-checks under the lane lock), which
/// clears the lane's hold. A latched write error drops the hold instead: the
/// writer appends nothing more, and the acked path would only turn every
/// write into an error (Option 3's behaviour, moon#1314).
pub(super) fn offer(
    lane: &AofLane,
    rx: &channel::MpscReceiver<AofMessage>,
    fsync: FsyncPolicy,
    write_error: bool,
    ctx: &mut RecordCtx,
    floor: FoldEpoch,
    file: &impl DupFile,
) {
    if write_error {
        lane.unhold();
        return;
    }
    if fsync == FsyncPolicy::Always
        || !lane.may_release(rx)
        || !crate::persistence::aof::lane_test_hook::offer_allowed()
    {
        return;
    }
    match file.dup() {
        Ok(dup) => {
            lane.release(rx, ctx, floor, dup);
        }
        Err(e) => warn!("AOF writer: could not dup its file for the shard-thread write ({e})"),
    }
}

#[cfg(all(feature = "runtime-tokio", unix))]
fn dup_tokio_file(file: &tokio::fs::File) -> std::io::Result<std::fs::File> {
    use std::os::fd::AsFd;
    Ok(std::fs::File::from(file.as_fd().try_clone_to_owned()?))
}

#[cfg(all(feature = "runtime-tokio", windows))]
fn dup_tokio_file(file: &tokio::fs::File) -> std::io::Result<std::fs::File> {
    use std::os::windows::io::AsHandle;
    Ok(std::fs::File::from(file.as_handle().try_clone_to_owned()?))
}

#[cfg(all(feature = "runtime-tokio", not(any(unix, windows))))]
fn dup_tokio_file(_file: &tokio::fs::File) -> std::io::Result<std::fs::File> {
    Err(std::io::Error::other("no fd duplication on this platform"))
}
