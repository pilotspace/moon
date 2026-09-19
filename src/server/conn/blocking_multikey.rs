//! How a multi-key blocking waiter registers on its keys, and how any
//! blocking wait settles when it ends without a reply (moon#989, moon#1019,
//! moon#1023).
//!
//! Shared by both runtimes' `handle_blocking_command*` so the two cannot
//! drift: the registration protocol IS the exactly-once argument, and a
//! second copy of it is a second place for that argument to break.
//!
//! ## Registration: runs, in argument order
//!
//! The keys are cut into RUNS — maximal stretches of consecutive keys owned by
//! the same shard.
//!
//! * Every key local: one thread serves the waiter, `remove_wait` unregisters
//!   its siblings before the next wake can run, and nothing more is needed —
//!   the keys are registered directly, with no claim token, exactly as before.
//! * Any key remote: the waiter carries a [`ClaimToken`] shared by all of its
//!   registrations, and every answer — an element or an error — is given only
//!   by the shard that wins it (moon#1019). The leading LOCAL run, if any, was
//!   already scanned by `immediate_scan` and is registered directly. Every run
//!   after it is registered ONE AT A TIME, in argument order: remote runs as a
//!   `BlockRegisterGroup` to their owner, local runs through the same
//!   `register_group` on this thread. Each run but the last is acknowledged
//!   before the next is sent.
//!
//! The acknowledgement is what makes the immediate answer redis's: a later
//! run is only ever decided after every earlier key was found empty and of
//! the right type BY ITS OWN OWNER. If an earlier key receives data after its
//! owner looked, that owner serves the waiter itself (it is registered there)
//! and, because it pushes and wakes in one synchronous stretch, it claims the
//! waiter at the instant of the push — a later owner's claim then fails and
//! puts its element back. The price is one round trip per remote run that is
//! not last; co-located keys (one run) and single remote keys pay nothing.
//!
//! ## Settling: when the wait ends without a reply
//!
//! A timeout, a shutdown, a vanished peer or a failed registration ends the
//! wait while an owner may be serving it on another thread. Dropping the
//! receivers is not enough (moon#1023): a serve already sent into a live
//! receiver is dropped with it, and the owner's undo — which runs only when
//! its send FAILS — never runs. [`settle`] closes the claim token first:
//!
//! * `Dead` — no shard served and none ever can; the receivers hold nothing of
//!   value and are dropped. This is why no drain is needed on this branch.
//! * `Claimed` — a serve is committed and its reply is in flight on exactly one
//!   receiver; it is taken, and then delivered (timeout, shutdown) or, for a
//!   vanished peer, left standing and logged like a delivered reply
//!   ([`finish_unserved`] says why it is never put back).
//!
//! A waiter with no token (every key local) settles by draining after its
//! synchronous `remove_wait`: nothing can send after that on a single thread,
//! so the drain is exact.

use std::cell::RefCell;
use std::rc::Rc;

use bytes::Bytes;
use futures::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};
use ringbuf::HeapProd;

use crate::blocking::{BlockedCommand, BlockingRegistry, ClaimToken, Settled, WaitEntry};
use crate::protocol::Frame;
use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use crate::shard::dispatch::{
    BlockRegisterGroupPayload, BlockRegisterMember, ShardMessage, key_to_shard,
};

/// One run of consecutive keys owned by one shard, staged for registration.
pub(super) struct PendingRun {
    pub owner: usize,
    pub members: Vec<BlockRegisterMember>,
}

/// Everything a multi-key waiter holds once its registrations are staged.
pub(super) struct StagedWait {
    pub wait_id: u64,
    /// One receiver per key, local and remote. The first `Some(frame)` wins.
    pub receivers: FuturesUnordered<channel::OneshotReceiver<Option<Frame>>>,
    /// `Some` iff any key is remote. See the module docs.
    pub claim: Option<ClaimToken>,
    /// Runs still to register, in argument order: everything from the first
    /// remote run on. Registered by the caller after it releases its
    /// registry borrow (A4/A5), one at a time.
    pub pending_runs: Vec<PendingRun>,
}

/// Register the keys the waiter can register right now, and stage the rest.
///
/// `local_cmd` builds the `BlockedCommand` for a LOCAL key (it binds a stream
/// `$` against this shard's view); `remote_cmd` builds one for a key whose
/// owner — possibly this shard, later — binds it on registration.
pub(super) fn stage_multikey_wait(
    registry: &mut BlockingRegistry,
    keys: &[Bytes],
    local_cmd: &dyn Fn(&Bytes) -> BlockedCommand,
    remote_cmd: &dyn Fn() -> BlockedCommand,
    selected_db: usize,
    shard_id: usize,
    num_shards: usize,
    deadline: Option<std::time::Instant>,
) -> StagedWait {
    let wait_id = registry.next_wait_id();
    let receivers = FuturesUnordered::new();
    let owner_of = |k: &Bytes| {
        if num_shards > 1 {
            key_to_shard(k, num_shards)
        } else {
            shard_id
        }
    };
    let first_remote = keys.iter().position(|k| owner_of(k) != shard_id);
    let claim = first_remote.map(|_| ClaimToken::new());
    let leading = first_remote.unwrap_or(keys.len());

    // The leading local run: `immediate_scan` already found it empty and of
    // the right type in this same synchronous stretch, so it can be
    // registered directly without losing a wake.
    for key in &keys[..leading] {
        let (tx, rx) = channel::oneshot::<Option<Frame>>();
        receivers.push(rx);
        registry.register(
            selected_db,
            key.clone(),
            WaitEntry {
                wait_id,
                cmd: local_cmd(key),
                reply_tx: tx,
                deadline,
                claim: claim.clone(),
            },
        );
    }

    let mut pending_runs: Vec<PendingRun> = Vec::new();
    for key in &keys[leading..] {
        let owner = owner_of(key);
        let (tx, rx) = channel::oneshot::<Option<Frame>>();
        receivers.push(rx);
        let member = BlockRegisterMember {
            key: key.clone(),
            cmd: remote_cmd(),
            reply_tx: tx,
        };
        match pending_runs.last_mut() {
            Some(run) if run.owner == owner => run.members.push(member),
            _ => pending_runs.push(PendingRun {
                owner,
                members: vec![member],
            }),
        }
    }

    StagedWait {
        wait_id,
        receivers,
        claim,
        pending_runs,
    }
}

/// The payload that registers `run` for waiter `wait_id`.
pub(super) fn run_payload(
    selected_db: usize,
    wait_id: u64,
    run: PendingRun,
    claim: &ClaimToken,
    ack: Option<channel::OneshotSender<()>>,
) -> BlockRegisterGroupPayload {
    BlockRegisterGroupPayload {
        db_index: selected_db,
        wait_id,
        members: run.members,
        claim: claim.clone(),
        ack,
    }
}

/// Why a wait ended without a reply from a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WaitEnd {
    Timeout,
    Shutdown,
    PeerGone,
    RegisterFailed,
}

/// Settle a wait that ended without a reply: close the claim token, and take
/// the committed serve if one exists. Returns that serve's reply.
///
/// `receivers` must be the waiter's receivers, still alive. With a token, a
/// `Claimed` settle awaits the winner's reply, bounded by
/// [`XSHARD_REPLY_TIMEOUT`](crate::shard::dispatch::XSHARD_REPLY_TIMEOUT):
/// the winner sends in the same synchronous stretch it claimed in, so the
/// bound is only ever reached by a shard that stopped running.
///
/// Without a token the caller must already have run its synchronous
/// `remove_wait` — then every receiver is resolved and the drain is exact.
pub(super) async fn settle<S>(claim: Option<&ClaimToken>, receivers: &mut S) -> Option<Frame>
where
    S: futures::Stream<Item = Result<Option<Frame>, channel::RecvError>> + Unpin,
{
    match claim.map(ClaimToken::settle) {
        Some(Settled::Dead) => None,
        Some(Settled::Claimed) => await_committed(receivers).await,
        None => drain_resolved(receivers),
    }
}

/// Local only: every sender was dropped by `remove_wait` or has already
/// sent, so every receiver is resolved and one pass finds any buffered reply
/// — for any number of keys.
///
/// A `Pending` from a resolved set can only be `FuturesUnordered`'s
/// cooperative yield (it hands back control after polling `len` futures and
/// wakes itself); the next poll resumes where it stopped. So a `Pending` ends
/// the drain only when the poll before it made no progress either — two in a
/// row with nothing consumed means the set is genuinely empty of ready items.
fn drain_resolved<S>(receivers: &mut S) -> Option<Frame>
where
    S: futures::Stream<Item = Result<Option<Frame>, channel::RecvError>> + Unpin,
{
    let mut idle = false;
    loop {
        match receivers.next().now_or_never() {
            Some(Some(Ok(Some(frame)))) => return Some(frame),
            Some(Some(_)) => idle = false,
            Some(None) => return None,
            None if idle => return None,
            None => idle = true,
        }
    }
}

/// The one committed reply among `receivers` (moon#1019 guarantees there is
/// at most one: nobody sends `Some` without winning the claim).
async fn await_committed<S>(receivers: &mut S) -> Option<Frame>
where
    S: futures::Stream<Item = Result<Option<Frame>, channel::RecvError>> + Unpin,
{
    use crate::runtime::race::{Arm, race2};
    use crate::runtime::{TimerImpl, traits::RuntimeTimer};
    // Usually the winner's reply is already buffered — it was sent in the
    // stretch that won the claim, before this thread even settled. Take it
    // without arming a timer.
    while let Some(item) = receivers.next().now_or_never() {
        match item {
            Some(Ok(Some(frame))) => return Some(frame),
            Some(_) => continue,
            None => return None,
        }
    }
    let next_frame = async {
        while let Some(r) = receivers.next().await {
            if let Ok(Some(frame)) = r {
                return Some(frame);
            }
        }
        None
    };
    let next_frame = std::pin::pin!(next_frame);
    let bound = std::pin::pin!(TimerImpl::sleep(
        crate::shard::dispatch::XSHARD_REPLY_TIMEOUT
    ));
    match race2(next_frame, bound).await {
        Arm::First(frame) => frame,
        Arm::Second(()) => {
            tracing::warn!(
                "blocking wait: a shard claimed this waiter but its reply never arrived; \
                 the owner shard stopped running mid-serve"
            );
            None
        }
    }
}

/// Wait for a run's acknowledgement — but never past the client's own
/// deadline, and never through a shutdown.
///
/// The registration phase runs BEFORE the wait loop arms the client's timer,
/// so this is the only thing standing between a slow owner and the client's
/// timeout: `BLPOP q1 q2 q3 1` against an owner busy for 5 s used to answer at
/// ~5 s, and past 30 s it answered `MOONERR` instead of nil. Now:
///
/// * the ack arrives → `Ok(())`, register the next run;
/// * the deadline passes first → [`WaitEnd::Timeout`], settled and cleaned up
///   exactly like a timeout in the wait loop (an earlier run may have served
///   the waiter meanwhile — the settle finds that);
/// * shutdown → [`WaitEnd::Shutdown`], the ordinary shutdown reply;
/// * the owner dropped the ack unsent (it is shutting down and discarded the
///   message) → [`WaitEnd::RegisterFailed`].
///
/// With no deadline (`timeout 0`) there is no bound: the client asked to wait
/// forever, and an owner that never answers is a wedged shard — an incident a
/// fabricated error would only hide.
pub(super) async fn await_run_ack(
    ack: channel::OneshotReceiver<()>,
    shutdown: &CancellationToken,
    deadline: Option<std::time::Instant>,
) -> Result<(), WaitEnd> {
    use crate::runtime::race::{Arm, race2};
    use crate::runtime::{TimerImpl, traits::RuntimeTimer};
    let ack = std::pin::pin!(ack);
    let stop = std::pin::pin!(async {
        let cancelled = std::pin::pin!(shutdown.cancelled());
        match deadline {
            Some(dl) => {
                let sleep = std::pin::pin!(TimerImpl::sleep(
                    dl.saturating_duration_since(std::time::Instant::now())
                ));
                match race2(cancelled, sleep).await {
                    Arm::First(()) => WaitEnd::Shutdown,
                    Arm::Second(()) => WaitEnd::Timeout,
                }
            }
            None => {
                cancelled.await;
                WaitEnd::Shutdown
            }
        }
    });
    match race2(ack, stop).await {
        Arm::First(Ok(())) => Ok(()),
        Arm::First(Err(_)) => Err(WaitEnd::RegisterFailed),
        Arm::Second(end) => Err(end),
    }
}

/// Test-only fault injection (moon#1023): hold a blocking wait that ended
/// WITHOUT a reply (timeout, shutdown, vanished peer) for this many
/// milliseconds before it settles, so an integration test can land a serve
/// inside the window deterministically. Read once; unset or unparsable is a
/// no-op. Production cost: one `OnceLock` load per wait that ends unserved.
pub(super) async fn settle_delay_for_test() {
    use crate::runtime::traits::RuntimeTimer;
    static DELAY: std::sync::OnceLock<std::time::Duration> = std::sync::OnceLock::new();
    let delay = *DELAY.get_or_init(|| {
        std::env::var("MOON_TEST_BLOCK_SETTLE_DELAY_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(std::time::Duration::from_millis)
            .unwrap_or(std::time::Duration::ZERO)
    });
    if !delay.is_zero() {
        crate::runtime::TimerImpl::sleep(delay).await;
    }
}

/// What every blocking wait needs to reach its own shard's registry and the
/// other shards' rings. `notifiers` is `Some` only on the monoio runtime,
/// whose shard loops park and need an explicit kick.
pub(super) struct BlockCtx<'a> {
    pub(super) selected_db: usize,
    pub(super) blocking_registry: &'a Rc<RefCell<crate::blocking::BlockingRegistry>>,
    pub(super) shard_id: usize,
    pub(super) dispatch_tx: &'a Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    pub(super) shutdown: &'a CancellationToken,
    pub(super) notifiers: Option<&'a [std::sync::Arc<channel::Notify>]>,
}

impl BlockCtx<'_> {
    fn notifier(&self, shard: usize) -> Option<&channel::Notify> {
        self.notifiers.and_then(|n| n.get(shard)).map(|n| &**n)
    }
}

/// Register a spanning waiter's staged runs one at a time, in argument order
/// (moon#1019; protocol in `blocking_multikey`).
///
/// Stops early once the claim is taken — an earlier run already answered the
/// waiter (a pop or a `-WRONGTYPE`), so nothing after it may be consulted.
/// Every owner a registration was delivered to is appended to `registered`,
/// for the `BlockCancel` fan-out.
///
/// `Err` ends the wait before the wait loop starts, with the reason to settle
/// it by: [`WaitEnd::RegisterFailed`] when a run could not be delivered, or
/// whatever ended the wait for an acknowledgement ([`await_run_ack`]).
pub(super) async fn register_runs(
    ctx: &BlockCtx<'_>,
    wait_id: u64,
    claim: &crate::blocking::ClaimToken,
    runs: Vec<PendingRun>,
    registered: &mut Vec<usize>,
    deadline: Option<std::time::Instant>,
) -> Result<(), WaitEnd> {
    let total = runs.len();
    for (i, run) in runs.into_iter().enumerate() {
        if !claim.is_open() {
            break;
        }
        let owner = run.owner;
        if owner == ctx.shard_id {
            // A later LOCAL run: decided right here, by the same code an owner
            // runs — the ladder, the registration and any serve are one
            // synchronous stretch of this thread.
            let payload = run_payload(ctx.selected_db, wait_id, run, claim, None);
            crate::shard::slice::with_shard_db(ctx.selected_db, |db| {
                crate::blocking::group::register_group(
                    &mut ctx.blocking_registry.borrow_mut(),
                    db,
                    payload,
                );
            });
            continue;
        }
        let (ack_tx, ack_rx) = if i + 1 < total {
            let (tx, rx) = channel::oneshot::<()>();
            (Some(tx), Some(rx))
        } else {
            // The last run has nothing after it to hold back.
            (None, None)
        };
        let msg = ShardMessage::BlockRegisterGroup(Box::new(run_payload(
            ctx.selected_db,
            wait_id,
            run,
            claim,
            ack_tx,
        )));
        if !super::blocking::push_block_msg(
            ctx.shutdown,
            ctx.dispatch_tx,
            ctx.shard_id,
            owner,
            msg,
            ctx.notifier(owner),
        )
        .await
        {
            return Err(WaitEnd::RegisterFailed);
        }
        if !registered.contains(&owner) {
            registered.push(owner);
        }
        if let Some(ack) = ack_rx {
            await_run_ack(ack, ctx.shutdown, deadline).await?;
        }
    }
    Ok(())
}

/// The answer a wait owes once it ended without a reply from a key.
///
/// `served` is the committed serve [`settle`] found, if any (moon#1023). A
/// client that is still connected receives it — on a timeout redis would have
/// answered it too (the serve happened before the timeout was observed), and
/// on shutdown the reply is the only place the element still exists.
///
/// A client that is GONE cannot receive it, and the serve STANDS: the caller
/// logs its effect through the same path as a delivered reply and closes
/// ([`ServedPeerGone`](super::blocking::BlockingOutcome::ServedPeerGone)).
/// That is what redis does — it pops and propagates when it serves, and a
/// client that disconnects with the reply in its output buffer loses it.
///
/// Putting the element back instead (the first cut of moon#1023) is not
/// sound here. The owner's pop is not logged when it happens; its record is
/// written by the connection, from the reply. A restore therefore lands AFTER
/// whatever other clients logged meanwhile, on top of a pop the log never
/// saw. `RPUSH k a` → served, client gone → `RPUSH k b; LPOP k` (logged) →
/// restore `a`: the master holds `[a]` while its AOF replays `[b]`, and a
/// `DEL k` in the window is undone. Logging the restore does not help either
/// — the unlogged pop is still missing from the history it would be appended
/// to.
///
/// "Through the same path as a delivered reply" is only as good as that
/// path, and for a gone client it inherits its gaps rather than closing them:
/// the record goes to the CONNECTION's shard AOF, so for a key another shard
/// owns replay drops it (moon#1056); and the tokio handler appends it to the
/// AOF only, never to the replication stream.
pub(super) fn finish_unserved(
    end: WaitEnd,
    served: Option<Frame>,
) -> super::blocking::BlockingOutcome {
    use super::blocking::BlockingOutcome;
    match (end, served) {
        (WaitEnd::PeerGone, Some(frame)) => BlockingOutcome::ServedPeerGone(frame),
        (_, Some(frame)) => BlockingOutcome::Reply(frame),
        (WaitEnd::Timeout, None) => BlockingOutcome::Reply(Frame::NullArray),
        (WaitEnd::Shutdown, None) => BlockingOutcome::Reply(Frame::Error(Bytes::from_static(
            b"ERR server shutting down",
        ))),
        (WaitEnd::PeerGone, None) => BlockingOutcome::PeerGone,
        (WaitEnd::RegisterFailed, None) => BlockingOutcome::Reply(Frame::Error(
            Bytes::from_static(super::blocking::BLOCK_REGISTER_FAILED),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd() -> BlockedCommand {
        BlockedCommand::BLPop
    }

    fn keys_of(names: &[&'static str]) -> Vec<Bytes> {
        names
            .iter()
            .map(|k| Bytes::from_static(k.as_bytes()))
            .collect()
    }

    fn pick(owner: usize, tag: &str, n: usize) -> Bytes {
        (0..10_000)
            .map(|i| Bytes::from(format!("{tag}{i}")))
            .find(|k| key_to_shard(k, n) == owner)
            .expect("a key for every shard")
    }

    /// Co-located keys owned by another shard: ONE run, every key in argument
    /// order, nothing registered locally — and a claim token, because the
    /// owner serves on another thread (moon#1023).
    #[test]
    fn colocated_remote_keys_are_one_run() {
        const N: usize = 4;
        let keys = keys_of(&["{t}a", "{t}b", "{t}c", "{t}b"]);
        let owner = key_to_shard(&keys[0], N);
        let me = (owner + 1) % N;
        let mut reg = BlockingRegistry::new(me);
        let staged = stage_multikey_wait(&mut reg, &keys, &|_| cmd(), &cmd, 0, me, N, None);
        assert!(staged.claim.is_some());
        assert_eq!(staged.pending_runs.len(), 1);
        assert_eq!(staged.pending_runs[0].owner, owner);
        let names: Vec<&[u8]> = staged.pending_runs[0]
            .members
            .iter()
            .map(|m| m.key.as_ref())
            .collect();
        assert_eq!(names, [&b"{t}a"[..], b"{t}b", b"{t}c", b"{t}b"]);
        assert_eq!(staged.receivers.len(), 4);
        assert!(
            !reg.is_waiting(staged.wait_id),
            "nothing registered locally"
        );
    }

    /// Keys this shard owns never leave it, and need no token: the fast path
    /// is unchanged.
    #[test]
    fn local_keys_register_locally_without_a_token() {
        const N: usize = 4;
        let keys = keys_of(&["{t}a", "{t}b"]);
        let me = key_to_shard(&keys[0], N);
        let mut reg = BlockingRegistry::new(me);
        let staged = stage_multikey_wait(&mut reg, &keys, &|_| cmd(), &cmd, 0, me, N, None);
        assert!(staged.pending_runs.is_empty());
        assert!(staged.claim.is_none());
        assert!(reg.is_waiting(staged.wait_id));
    }

    /// Spanning keys: the leading local run registers now; everything after
    /// the first remote key is staged as runs in ARGUMENT order — a key that
    /// returns to an earlier owner opens a new run rather than joining the
    /// old one, and a later local run is staged too.
    #[test]
    fn spanning_keys_stage_runs_in_argument_order() {
        const N: usize = 4;
        let me = 0usize;
        let keys = vec![
            pick(me, "w", N),
            pick(1, "x", N),
            pick(1, "y", N),
            pick(2, "z", N),
            pick(me, "v", N),
            pick(1, "u", N),
        ];
        let mut reg = BlockingRegistry::new(me);
        let staged = stage_multikey_wait(&mut reg, &keys, &|_| cmd(), &cmd, 0, me, N, None);
        assert!(staged.claim.is_some());
        assert!(reg.is_waiting(staged.wait_id), "the leading local key");
        let runs: Vec<(usize, usize)> = staged
            .pending_runs
            .iter()
            .map(|r| (r.owner, r.members.len()))
            .collect();
        assert_eq!(runs, vec![(1, 2), (2, 1), (me, 1), (1, 1)]);
        assert_eq!(staged.receivers.len(), keys.len());
    }

    /// `--shards 1`: everything is local whatever the key names hash to.
    #[test]
    fn single_shard_is_always_local() {
        let keys = keys_of(&["a", "b", "c"]);
        let mut reg = BlockingRegistry::new(0);
        let staged = stage_multikey_wait(&mut reg, &keys, &|_| cmd(), &cmd, 0, 0, 1, None);
        assert!(staged.claim.is_none());
        assert!(staged.pending_runs.is_empty());
    }

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        futures::executor::block_on(f)
    }

    /// moon#1023: a token nobody claimed settles DEAD and yields nothing —
    /// and from then on no shard can claim it.
    #[test]
    fn settle_dead_yields_nothing_and_closes_the_token() {
        let claim = ClaimToken::new();
        let mut rxs: FuturesUnordered<channel::OneshotReceiver<Option<Frame>>> =
            FuturesUnordered::new();
        let (_tx, rx) = channel::oneshot();
        rxs.push(rx);
        assert!(block_on(settle(Some(&claim), &mut rxs)).is_none());
        assert!(!claim.clone().try_claim());
    }

    /// moon#1023: a claim won before the waiter settles hands the waiter the
    /// winner's reply, even though its other receivers are silent or closed.
    #[test]
    fn settle_claimed_takes_the_committed_reply() {
        let claim = ClaimToken::new();
        let mut rxs: FuturesUnordered<channel::OneshotReceiver<Option<Frame>>> =
            FuturesUnordered::new();
        let (dropped_tx, rx1) = channel::oneshot();
        let (_silent_tx, rx2) = channel::oneshot();
        let (winner_tx, rx3) = channel::oneshot();
        rxs.push(rx1);
        rxs.push(rx2);
        rxs.push(rx3);
        drop(dropped_tx);
        assert!(claim.clone().try_claim());
        let frame = Frame::BulkString(Bytes::from_static(b"v"));
        assert!(winner_tx.send(Some(frame.clone())).is_ok());
        assert_eq!(block_on(settle(Some(&claim), &mut rxs)), Some(frame));
    }

    /// No token (every key local): the drain finds a reply already buffered —
    /// the `select!` picked the timer over a ready receiver.
    #[test]
    fn settle_local_drains_a_buffered_reply() {
        let mut rxs: FuturesUnordered<channel::OneshotReceiver<Option<Frame>>> =
            FuturesUnordered::new();
        let (tx1, rx1) = channel::oneshot();
        let (tx2, rx2) = channel::oneshot();
        rxs.push(rx1);
        rxs.push(rx2);
        drop(tx1);
        let frame = Frame::BulkString(Bytes::from_static(b"v"));
        assert!(tx2.send(Some(frame.clone())).is_ok());
        assert_eq!(block_on(settle(None, &mut rxs)), Some(frame));
    }

    /// The local drain is correct for ANY key count. It used to
    /// give up after 1024 items, so a reply behind 1024+ closed receivers
    /// (a `BLPOP` over that many keys) was dropped with its element.
    #[test]
    fn settle_local_drain_finds_a_reply_behind_any_number_of_keys() {
        for closed in [0usize, 1, 1_023, 1_024, 5_000] {
            let mut rxs: FuturesUnordered<channel::OneshotReceiver<Option<Frame>>> =
                FuturesUnordered::new();
            for _ in 0..closed {
                let (tx, rx) = channel::oneshot::<Option<Frame>>();
                rxs.push(rx);
                drop(tx);
            }
            let (tx, rx) = channel::oneshot();
            rxs.push(rx);
            let frame = Frame::BulkString(Bytes::from_static(b"v"));
            assert!(tx.send(Some(frame.clone())).is_ok());
            assert_eq!(
                block_on(settle(None, &mut rxs)),
                Some(frame),
                "{closed} closed receivers ahead of the reply"
            );
        }
    }

    /// The local drain also terminates when there is nothing to find.
    #[test]
    fn settle_local_drain_of_closed_receivers_yields_nothing() {
        let mut rxs: FuturesUnordered<channel::OneshotReceiver<Option<Frame>>> =
            FuturesUnordered::new();
        for _ in 0..3_000 {
            let (tx, rx) = channel::oneshot::<Option<Frame>>();
            rxs.push(rx);
            drop(tx);
        }
        assert!(block_on(settle(None, &mut rxs)).is_none());
    }
}
