use std::cell::RefCell;
use std::rc::Rc;
#[cfg(feature = "runtime-monoio")]
use std::sync::Arc;

use bytes::Bytes;
use bytes::BytesMut;
use futures::StreamExt;
use ringbuf::HeapProd;
use ringbuf::traits::Producer;

use crate::framevec;
use crate::protocol::Frame;
use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use crate::shard::dispatch::{ShardMessage, key_to_shard};
use crate::shard::mesh::ChannelMesh;
use crate::shard::shared_databases::ShardDatabases;
use crate::storage::Database;

use super::util::extract_bytes;

/// Returned to the client when a blocking command cannot be registered on the
/// shard that owns its key. Fails the command instead of the two silent
/// alternatives the old code had: replying nil at t=0 (protocol violation) or
/// blocking forever on a key nobody is watching.
const BLOCK_REGISTER_FAILED: &[u8] =
    b"MOONERR blocking registration failed: owning shard not draining";

/// What a blocking command produced.
///
/// c10k hardening A1: the wait used to have only two exits — a reply or a
/// timeout — so a client that vanished mid-`BLPOP key 0` was unreachable
/// forever. [`PeerGone`](BlockingOutcome::PeerGone) is the third, and it is
/// deliberately NOT a `Frame`: there is nobody left to send one to, and the
/// caller must close the connection rather than write into a dead socket.
pub(crate) enum BlockingOutcome {
    /// Normal completion: wake-up value, timeout nil, or an error frame.
    Reply(Frame),
    /// The peer went away while blocked (EOF, RST, or a `CLIENT KILL`
    /// `shutdown(2)`). Every registration this waiter held has already been
    /// torn down; the caller must close the connection without replying.
    PeerGone,
}

/// Scratch size for one drain of a blocked client's socket. Only ever used by
/// a client that pipelines behind a blocking command, which is legal but rare
/// — the buffer is allocated once per such connection and reused across the
/// wait loop, never on any dispatch path.
#[cfg(feature = "runtime-monoio")]
const PEER_DRAIN_BUF: usize = 512;

/// Ceiling on bytes a blocked client may pipeline behind its blocking command
/// before the connection is dropped.
///
/// The carry buffer is the one place A1 accumulates client input outside the
/// handler's own read loop, so it gets its own bound rather than inheriting
/// the read loop's (which has none — that is finding C2, a separate fix). The
/// value is far above any legitimate pipeline and far below a memory problem
/// at c10k.
const PEER_CARRY_LIMIT: usize = 64 * 1024 * 1024;

/// c10k A1 (tokio): classify one peer read taken while a client is blocked.
///
/// `true` means the wait is over because the peer is gone: EOF, a socket
/// error, or a client that pipelined past [`PEER_CARRY_LIMIT`]. `false` means
/// the bytes were carried and the client stays blocked.
#[cfg(feature = "runtime-tokio")]
fn peer_wake_ends_wait(read: std::io::Result<usize>, carry: &BytesMut) -> bool {
    match read {
        Ok(0) => true,
        Ok(_) => carry.len() > PEER_CARRY_LIMIT,
        // `Interrupted` is a retry signal, not a dead peer (tokio never
        // surfaces `WouldBlock` here — it returns `Pending` instead).
        Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => false,
        Err(_) => true,
    }
}

/// c10k A1: result of one peer-readiness wake while a client is blocked.
#[cfg(feature = "runtime-monoio")]
enum PeerWake {
    /// Bytes arrived and were carried forward; the client is alive and stays
    /// blocked (Redis executes pipelined commands only after the block ends).
    Alive,
    /// EOF or a fatal socket error — the connection is finished.
    Gone,
}

/// c10k A1 (monoio): consume one readiness worth of bytes from a blocked
/// client's socket into `carry`.
///
/// MUST be called only after [`IdleParkRead::peer_readable`] has fired, and
/// MUST NOT be called from inside a `select!`. Both rules exist for the same
/// reason: level-triggered readiness guarantees this read completes at once
/// (data, EOF, or error), so it can never be cancelled mid-flight and lose
/// client bytes — the failure mode that rules out a naive read arm.
///
/// Racing the reply is harmless: `reply_tx` is a `flume` bounded(1), so a
/// wake-up delivered while this read is in flight simply sits in the channel
/// and the next loop turn picks it up.
#[cfg(feature = "runtime-monoio")]
async fn drain_peer_bytes<S: monoio::io::AsyncReadRent>(
    stream: &mut S,
    scratch: &mut Vec<u8>,
    carry: &mut BytesMut,
) -> PeerWake {
    if scratch.len() != PEER_DRAIN_BUF {
        scratch.resize(PEER_DRAIN_BUF, 0);
    }
    let buf = std::mem::take(scratch);
    let (res, buf) = stream.read(buf).await;
    let wake = match res {
        Ok(0) => PeerWake::Gone,
        Ok(n) => {
            carry.extend_from_slice(&buf[..n]);
            PeerWake::Alive
        }
        // NOT death. `readable(false)` does a real `poll(2)`, but the legacy
        // (epoll/kqueue) driver can still hand back a readiness that yields
        // nothing, and a cancel is a control signal, not a peer event.
        // Misreading either as EOF would drop a perfectly healthy blocked
        // client — the exact inverse of the bug being fixed. Re-arming is
        // safe: the next poll only resolves on a real event.
        Err(ref e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
            ) || super::handler_monoio::idle_park::is_sweep_cancel(e) =>
        {
            PeerWake::Alive
        }
        Err(_) => PeerWake::Gone,
    };
    *scratch = buf;
    wake
}

/// c10k A1 (monoio): handle one resolved peer-readiness poll.
///
/// `true` means the wait is over because the peer is gone. A readiness error
/// counts as gone; readiness success means bytes or EOF are waiting, so the
/// (uncancellable, immediately-completing) drain decides which.
#[cfg(feature = "runtime-monoio")]
async fn peer_wake_ends_wait_monoio<S: monoio::io::AsyncReadRent>(
    ready: std::io::Result<()>,
    stream: &mut S,
    scratch: &mut Vec<u8>,
    carry: &mut BytesMut,
) -> bool {
    if let Err(e) = ready {
        // Same rule as the drain below: only a real error is death.
        return !(matches!(
            e.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
        ) || super::handler_monoio::idle_park::is_sweep_cancel(&e));
    }
    match drain_peer_bytes(stream, scratch, carry).await {
        PeerWake::Gone => true,
        PeerWake::Alive => carry.len() > PEER_CARRY_LIMIT,
    }
}

/// Push a blocking-registry control message (`BlockRegister` / `BlockCancel`)
/// to the shard that owns the key.
///
/// c10k hardening A4/A5/A6. Every call site was previously either a bare
/// `try_push` — silently dropping the message when the ring was full — or an
/// unbounded `loop { try_push; sleep(10µs) }` with no shutdown arm. Each
/// failure mode is a real defect:
///   * a dropped `BlockRegister` drops `reply_tx` with it, so the waiter's
///     receiver disconnects and `BLPOP key 0` returns nil immediately;
///   * a dropped `BlockCancel` leaks the owner-side `WaitEntry` permanently
///     (remote entries carry `deadline: None`, so the W6 sweep never reaps
///     them) — the leak that then feeds the A2 data-loss path;
///   * the unbounded retry turned a saturated owner into a zombie connection
///     that also prevented graceful shutdown from completing.
///
/// Uses the same budget as every other cross-shard push
/// ([`CROSS_SHARD_PUSH_MAX_RETRIES`] × [`CROSS_SHARD_PUSH_BACKOFF`] ≈ 0.5 s):
/// wedged-shard detection scale, generous enough not to false-reject a merely
/// saturated shard. Deliberately NOT a tighter blocking-specific budget — a
/// registration that gives up early is a client-visible error, and the old
/// monoio path retried forever, so the only safe direction to move is toward
/// the plane-wide bound.
///
/// Returns `false` if the message could not be delivered.
async fn push_block_msg(
    shutdown: &CancellationToken,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    my_shard: usize,
    target_shard: usize,
    msg: ShardMessage,
    notify: Option<&channel::Notify>,
) -> bool {
    debug_assert_ne!(
        my_shard, target_shard,
        "blocking control message must target a remote shard"
    );
    let target_idx = ChannelMesh::target_index(my_shard, target_shard);
    let mut slot = Some(msg);
    let outcome = crate::shard::dispatch::push_with_backpressure(
        shutdown,
        crate::shard::dispatch::CROSS_SHARD_PUSH_MAX_RETRIES,
        crate::shard::dispatch::CROSS_SHARD_PUSH_BACKOFF,
        || {
            let Some(m) = slot.take() else { return true };
            // Borrow is scoped to this closure body, never held across the
            // caller's `.await` between retries.
            let mut producers = dispatch_tx.borrow_mut();
            match producers[target_idx].try_push(m) {
                Ok(()) => true,
                Err(back) => {
                    slot = Some(back);
                    false
                }
            }
        },
    )
    .await;
    if outcome != crate::shard::dispatch::PushOutcome::Pushed {
        return false;
    }
    // A5: the owner's event loop may be parked; without this kick the
    // registration sits in the ring until the next periodic tick.
    //
    // `notify` is `Some` only on the monoio runtime, whose shard loops park in
    // the driver and need the explicit eventfd kick. The tokio shard loop has
    // no equivalent task-local `Notify` receive path — it discovers ring items
    // through its own periodic drain — so tokio call sites pass `None` and
    // accept up to one tick of latency rather than a lost registration. Wiring
    // a `Notify` into the tokio loop is a separate change; this comment states
    // what the code actually does today.
    if let Some(n) = notify {
        n.notify_one();
    }
    true
}

/// Tear down every registration a multi-key waiter holds: the local ones
/// directly, the remote ones with a `BlockCancel` per owning shard.
///
/// A6: the cancels were bare `try_push`es, so a full ring leaked the owner's
/// `WaitEntry` — which is exactly the ghost waiter the A2 wake path then had
/// to defend against. `notifiers` is `Some` only on the monoio runtime, whose
/// shard loops park and need an explicit kick.
async fn cancel_multikey_registrations(
    shutdown: &CancellationToken,
    blocking_registry: &Rc<RefCell<crate::blocking::BlockingRegistry>>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    shard_id: usize,
    wait_id: u64,
    registered_remote_shards: &[usize],
    notifiers: Option<&[std::sync::Arc<channel::Notify>]>,
) {
    blocking_registry.borrow_mut().remove_wait(wait_id);
    for &remote_shard in registered_remote_shards {
        let delivered = push_block_msg(
            shutdown,
            dispatch_tx,
            shard_id,
            remote_shard,
            ShardMessage::BlockCancel { wait_id },
            notifiers.map(|n| &*n[remote_shard]),
        )
        .await;
        if !delivered {
            // The push budget is bounded by design (A4 — the old unbounded
            // retry was itself the bug), so a shard wedged for the whole
            // ~0.5 s budget, or a shutdown mid-cancel, can still drop this.
            // The owner then keeps a `WaitEntry` that its deadline sweep will
            // not reap (remote entries carry `deadline: None`).
            //
            // This is a memory leak, NOT data loss: since A2 every wake path
            // checks `is_disconnected()` before touching the datastore and
            // restores anything it popped, so a stale entry is skipped and
            // pruned rather than swallowing an element. Log it so the leak is
            // observable instead of silent — a shard wedged this long is
            // already an incident.
            tracing::warn!(
                wait_id,
                from_shard = shard_id,
                owner_shard = remote_shard,
                "BlockCancel undelivered: owner shard not draining; its waiter \
                 entry leaks until a later push prunes it"
            );
        }
    }
}

/// The full set of client-blocking commands (the ones whose handler may
/// early-flush accumulated responses and await outside the batch loop).
/// Keep in sync with the dispatch guards in `try_handle_blocking`
/// (handler_monoio) and the sharded handler's blocking arm.
///
/// Every command in this list answers a timeout with the RESP2 **Null Array**
/// (`*-1`), which is why the wait paths below resolve to `Frame::NullArray`
/// rather than `Frame::Null`. That includes `BRPOPLPUSH` and `BLMOVE`, whose
/// SUCCESSFUL reply is a bulk string — measured against redis-server 8.6.1,
/// not inferred from the success shape (moon#482).
pub(crate) fn is_blocking_command(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"BLPOP")
        || cmd.eq_ignore_ascii_case(b"BRPOP")
        || cmd.eq_ignore_ascii_case(b"BLMOVE")
        || cmd.eq_ignore_ascii_case(b"BZPOPMIN")
        || cmd.eq_ignore_ascii_case(b"BZPOPMAX")
        || cmd.eq_ignore_ascii_case(b"BLMPOP")
        || cmd.eq_ignore_ascii_case(b"BRPOPLPUSH")
        || cmd.eq_ignore_ascii_case(b"BZMPOP")
}

/// The frame a blocking command is QUEUED as inside `MULTI`.
///
/// Two outcomes, and which one applies is the whole of moon#524:
///
/// * `BLMOVE`/`BLMPOP`/`BZMPOP`/`BRPOPLPUSH` are rewritten to their
///   non-blocking sibling (`LMOVE`/`LMPOP`/`ZMPOP`/`LMOVE`). Those siblings
///   take the same keys and answer the same shape, so the transaction executor
///   can run them through the ordinary dispatch table.
/// * `BLPOP`/`BRPOP`/`BZPOPMIN`/`BZPOPMAX` are queued UNCHANGED — the frame
///   comes back out of this function as it went in. Their siblings answer a
///   DIFFERENT shape (`LPOP` returns a bare element where `BLPOP` returns
///   `[key, element]`), and `LPOP key1 key2` is not even the same command:
///   LPOP's second argument is a COUNT. They are executed in immediate-only
///   mode at EXEC instead — see [`super::blocking_txn`], which owns the
///   matching predicate.
///
/// Not a hot path: this runs once per command QUEUED inside a transaction.
pub(crate) fn queued_blocking_frame(cmd: &[u8], args: &[Frame]) -> Frame {
    if super::blocking_txn::queues_unrewritten(cmd) {
        // Reconstruct the original frame. Callers hand this straight to
        // `command_queue`, so returning the input verbatim keeps all three
        // queue sites (both sharded handlers + the monoio blocking arm)
        // identical instead of each growing its own branch.
        let mut original = Vec::with_capacity(args.len() + 1);
        original.push(Frame::BulkString(Bytes::copy_from_slice(cmd)));
        original.extend(args.iter().cloned());
        return Frame::Array(original.into());
    }
    let mut new_args = Vec::new();
    if cmd.eq_ignore_ascii_case(b"BLMOVE") {
        new_args.push(Frame::BulkString(Bytes::from_static(b"LMOVE")));
        // src dst LEFT|RIGHT LEFT|RIGHT (skip timeout which is last arg)
        for arg in args.iter().take(4) {
            new_args.push(arg.clone());
        }
    } else if cmd.eq_ignore_ascii_case(b"BLMPOP") {
        // BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT n]
        // -> LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT n] (skip timeout = args[0])
        new_args.push(Frame::BulkString(Bytes::from_static(b"LMPOP")));
        for arg in args.iter().skip(1) {
            new_args.push(arg.clone());
        }
    } else if cmd.eq_ignore_ascii_case(b"BRPOPLPUSH") {
        // BRPOPLPUSH src dst timeout -> RPOPLPUSH src dst (skip timeout = args[2])
        // Actually, convert to LMOVE src dst RIGHT LEFT
        new_args.push(Frame::BulkString(Bytes::from_static(b"LMOVE")));
        // src, dst
        for arg in args.iter().take(2) {
            new_args.push(arg.clone());
        }
        new_args.push(Frame::BulkString(Bytes::from_static(b"RIGHT")));
        new_args.push(Frame::BulkString(Bytes::from_static(b"LEFT")));
    } else if cmd.eq_ignore_ascii_case(b"BZMPOP") {
        // BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT n]
        // -> ZMPOP numkeys key [key ...] MIN|MAX [COUNT n] (skip timeout = args[0])
        new_args.push(Frame::BulkString(Bytes::from_static(b"ZMPOP")));
        for arg in args.iter().skip(1) {
            new_args.push(arg.clone());
        }
    }
    Frame::Array(new_args.into())
}

/// Handle a blocking command (BLPOP/BRPOP/BLMOVE/BZPOPMIN/BZPOPMAX).
///
/// 1. Non-blocking fast path: check if data is immediately available.
/// 2. Single-key fast path: one oneshot, one local registration (zero overhead).
/// 3. Multi-key coordinator: FuturesUnordered across local + remote shards,
///    first-wakeup-wins, BlockCancel cleanup on completion/timeout/shutdown.
///
/// CRITICAL: RefCell borrows MUST be dropped before any .await point.
///
/// c10k A1: `stream` is watched for EOF throughout the wait. `tokio`'s
/// `AsyncReadExt::read_buf` is documented cancel-safe (a losing `select!`
/// branch reads nothing), so here the watch is a plain read arm and any bytes
/// it does pick up are carried into `carry` for the handler's read loop.
#[cfg(feature = "runtime-tokio")]
pub(crate) async fn handle_blocking_command<S>(
    cmd: &[u8],
    args: &[Frame],
    selected_db: usize,
    _shard_databases: &std::sync::Arc<ShardDatabases>,
    blocking_registry: &Rc<RefCell<crate::blocking::BlockingRegistry>>,
    shard_id: usize,
    num_shards: usize,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    shutdown: &CancellationToken,
    stream: &mut S,
    carry: &mut BytesMut,
) -> BlockingOutcome
where
    S: tokio::io::AsyncRead + Unpin,
{
    use futures::stream::FuturesUnordered;
    use tokio::io::AsyncReadExt;

    // Parse timeout (last argument for all blocking commands)
    let timeout_secs = match parse_blocking_timeout(cmd, args) {
        Ok(t) => t,
        Err(e) => return BlockingOutcome::Reply(e),
    };

    // Parse keys and command-specific args
    let (keys, blocked_cmd_factory) = match parse_blocking_args(cmd, args) {
        Ok(v) => v,
        Err(e) => return BlockingOutcome::Reply(e),
    };

    // --- Non-blocking fast path: try to get data immediately ---
    {
        // Check immediate availability via the thread-local slice.
        let maybe_frame = crate::shard::slice::with_shard_db(selected_db, |db| {
            immediate_scan(cmd, args, &keys, db, shard_id, num_shards)
        });
        if let Some(frame) = maybe_frame {
            return BlockingOutcome::Reply(frame);
        }
        // Borrow released at with_shard_db boundary — safe before await.
    }

    let deadline = if timeout_secs > 0.0 {
        Some(std::time::Instant::now() + std::time::Duration::from_secs_f64(timeout_secs))
    } else {
        None // 0 = block forever
    };

    // --- Single-key fast path: one registration, direct await (zero overhead) ---
    if keys.len() == 1 {
        let target = key_to_shard(&keys[0], num_shards);
        let is_remote = target != shard_id;
        let (reply_tx, mut reply_rx) = channel::oneshot::<Option<Frame>>();
        let wait_id = blocking_registry.borrow_mut().next_wait_id();
        if is_remote {
            // Remote registration via SPSC — bounded and shutdown-aware
            // (A4/A5). The borrow above is released before this await.
            let msg = ShardMessage::BlockRegister(Box::new(
                crate::shard::dispatch::BlockRegisterPayload {
                    db_index: selected_db,
                    key: keys[0].clone(),
                    wait_id,
                    cmd: blocked_cmd_factory(),
                    reply_tx,
                    // The single-key fast path: this key IS the command, so
                    // the owner may answer a type error for it (moon#556).
                    sole_key: true,
                },
            ));
            if !push_block_msg(shutdown, dispatch_tx, shard_id, target, msg, None).await {
                return BlockingOutcome::Reply(Frame::Error(Bytes::from_static(
                    BLOCK_REGISTER_FAILED,
                )));
            }
        } else {
            // Local registration
            let entry = crate::blocking::WaitEntry {
                wait_id,
                cmd: blocked_cmd_factory(),
                reply_tx,
                deadline,
            };
            blocking_registry
                .borrow_mut()
                .register(selected_db, keys[0].clone(), entry);
        }

        // A1: every arm below is a `loop` because the peer-watch arm can fire
        // repeatedly without ending the wait — a client is allowed to pipeline
        // behind a blocking command, and Redis runs those commands only after
        // the block resolves. `&mut reply_rx` is re-selected across turns
        // safely: `OneshotReceiver` caches its inner `flume` future, so the
        // waker registration survives and no wake-up is lost.
        let result = if let Some(dl) = deadline {
            let sleep = tokio::time::sleep(dl.saturating_duration_since(std::time::Instant::now()));
            tokio::pin!(sleep);
            loop {
                tokio::select! {
                    res = &mut reply_rx => {
                        break match res {
                            Ok(Some(frame)) => Some(frame),
                            Ok(None) | Err(_) => Some(Frame::NullArray),
                        };
                    }
                    _ = &mut sleep => {
                        blocking_registry.borrow_mut().remove_wait(wait_id);
                        break Some(Frame::NullArray);
                    }
                    _ = shutdown.cancelled() => {
                        blocking_registry.borrow_mut().remove_wait(wait_id);
                        break Some(Frame::Error(Bytes::from_static(b"ERR server shutting down")));
                    }
                    read = stream.read_buf(carry) => {
                        if peer_wake_ends_wait(read, carry) {
                            blocking_registry.borrow_mut().remove_wait(wait_id);
                            break None;
                        }
                    }
                }
            }
        } else {
            loop {
                tokio::select! {
                    res = &mut reply_rx => {
                        break match res {
                            Ok(Some(frame)) => Some(frame),
                            Ok(None) | Err(_) => Some(Frame::NullArray),
                        };
                    }
                    _ = shutdown.cancelled() => {
                        blocking_registry.borrow_mut().remove_wait(wait_id);
                        break Some(Frame::Error(Bytes::from_static(b"ERR server shutting down")));
                    }
                    read = stream.read_buf(carry) => {
                        if peer_wake_ends_wait(read, carry) {
                            blocking_registry.borrow_mut().remove_wait(wait_id);
                            break None;
                        }
                    }
                }
            }
        };
        // Cleanup remote registration on timeout/shutdown.
        //
        // A3: this used to read
        //   `is_remote && !matches!(result, Frame::Null) || matches!(result, Frame::Error(_))`
        // which Rust parses as `(is_remote && !Null) || Error`. Two bugs fell
        // out of that precedence:
        //   * on TIMEOUT (`result == Frame::Null`) the cancel was never sent,
        //     so the owner shard kept a `WaitEntry` that nothing can ever
        //     reap (remote entries carry `deadline: None`, so the W6 deadline
        //     sweep skips them) — a permanent leak that also hands the next
        //     pusher a ghost waiter;
        //   * on a LOCAL wait ending in shutdown (`Frame::Error`) it took the
        //     remote branch anyway and computed `target_index(shard, shard)`,
        //     which underflows to a panic on shard 0 and misroutes the cancel
        //     to an unrelated shard otherwise.
        // The correct condition is simply "did we register remotely" — the
        // monoio twin below always had it right. A cancel that races a
        // successful wake is a harmless no-op (`remove_wait` on an unknown id).
        if is_remote {
            let _ = push_block_msg(
                shutdown,
                dispatch_tx,
                shard_id,
                target,
                ShardMessage::BlockCancel { wait_id },
                None,
            )
            .await;
        }
        // A1: the cancel above runs for a vanished peer too — that is the
        // whole point. Leaving the owner-side `WaitEntry` behind is what made
        // a disconnected `BLPOP key 0` unreapable.
        return match result {
            Some(frame) => BlockingOutcome::Reply(frame),
            None => BlockingOutcome::PeerGone,
        };
    }

    // --- Multi-key coordinator: register on ALL keys across local + remote shards ---
    // Uses FuturesUnordered for first-wakeup-wins semantics.
    let wait_id;
    let mut receivers: FuturesUnordered<channel::OneshotReceiver<Option<Frame>>> =
        FuturesUnordered::new();
    let mut registered_remote_shards: Vec<usize> = Vec::new();

    // A4/A5: build every registration under one borrow, but STAGE the remote
    // ones and push them only after the borrow is released — the old code
    // pushed while holding both borrows, which forced the bare `try_push`
    // (no await possible, so no backpressure retry and no shutdown arm).
    let mut pending_remote: Vec<(usize, ShardMessage)> = Vec::new();
    {
        let mut reg = blocking_registry.borrow_mut();
        wait_id = reg.next_wait_id();

        for key in &keys {
            let target = key_to_shard(key, num_shards);
            let (tx, rx) = channel::oneshot::<Option<Frame>>();
            receivers.push(rx);

            if target == shard_id {
                // Local registration
                let entry = crate::blocking::WaitEntry {
                    wait_id,
                    cmd: blocked_cmd_factory(),
                    reply_tx: tx,
                    deadline,
                };
                reg.register(selected_db, key.clone(), entry);
            } else {
                // Remote registration via SPSC
                let msg = ShardMessage::BlockRegister(Box::new(
                    crate::shard::dispatch::BlockRegisterPayload {
                        db_index: selected_db,
                        key: key.clone(),
                        wait_id,
                        cmd: blocked_cmd_factory(),
                        reply_tx: tx,
                        // One of several keys — see `sole_key`'s docs for why
                        // the owner must NOT decide the whole command here.
                        sole_key: false,
                    },
                ));
                pending_remote.push((target, msg));
                if !registered_remote_shards.contains(&target) {
                    registered_remote_shards.push(target);
                }
            }
        }
    } // borrows dropped -- CRITICAL before await

    // A5: a silently dropped registration leaves this waiter blocked on a key
    // nobody is watching — it would sleep through data that IS available.
    // Fail the whole command instead, after unwinding what did register.
    let mut registration_failed = false;
    for (target, msg) in pending_remote {
        if !push_block_msg(shutdown, dispatch_tx, shard_id, target, msg, None).await {
            registration_failed = true;
            break;
        }
    }
    if registration_failed {
        cancel_multikey_registrations(
            shutdown,
            blocking_registry,
            dispatch_tx,
            shard_id,
            wait_id,
            &registered_remote_shards,
            None,
        )
        .await;
        drop(receivers);
        return BlockingOutcome::Reply(Frame::Error(Bytes::from_static(BLOCK_REGISTER_FAILED)));
    }

    // Await first successful result from any key/shard.
    // FuturesUnordered may return Err (sender dropped by remove_wait cleanup) before
    // returning the successful Ok. We must skip Err/None results and keep polling.
    //
    // A1: `peer_gone` breaks the same loop as every other terminal condition,
    // so the shared cleanup below runs identically — a vanished multi-key
    // waiter unwinds ALL of its registrations, local and remote.
    let mut peer_gone = false;
    let frame = if let Some(dl) = deadline {
        let sleep = tokio::time::sleep(dl.saturating_duration_since(std::time::Instant::now()));
        tokio::pin!(sleep);
        let mut result_frame = Frame::NullArray;
        loop {
            tokio::select! {
                result = receivers.next() => {
                    match result {
                        Some(Ok(Some(frame))) => { result_frame = frame; break; }
                        Some(Ok(None)) | Some(Err(_)) => continue, // cancelled/dropped, try next
                        None => break, // all receivers exhausted
                    }
                }
                _ = &mut sleep => break,
                _ = shutdown.cancelled() => {
                    result_frame = Frame::Error(Bytes::from_static(b"ERR server shutting down"));
                    break;
                }
                read = stream.read_buf(carry) => {
                    if peer_wake_ends_wait(read, carry) { peer_gone = true; break; }
                }
            }
        }
        result_frame
    } else {
        let mut result_frame = Frame::NullArray;
        loop {
            tokio::select! {
                result = receivers.next() => {
                    match result {
                        Some(Ok(Some(frame))) => { result_frame = frame; break; }
                        Some(Ok(None)) | Some(Err(_)) => continue, // cancelled/dropped, try next
                        None => break,
                    }
                }
                _ = shutdown.cancelled() => {
                    result_frame = Frame::Error(Bytes::from_static(b"ERR server shutting down"));
                    break;
                }
                read = stream.read_buf(carry) => {
                    if peer_wake_ends_wait(read, carry) { peer_gone = true; break; }
                }
            }
        }
        result_frame
    };

    // Cleanup: cancel all remaining registrations (local + remote)
    cancel_multikey_registrations(
        shutdown,
        blocking_registry,
        dispatch_tx,
        shard_id,
        wait_id,
        &registered_remote_shards,
        None,
    )
    .await;
    // Drop remaining receivers; remote senders get Err on send -- harmless
    drop(receivers);

    if peer_gone {
        BlockingOutcome::PeerGone
    } else {
        BlockingOutcome::Reply(frame)
    }
}

/// Monoio version of handle_blocking_command.
///
/// Identical logic to the tokio version but uses:
/// - `monoio::select!` instead of `tokio::select!`
/// - `monoio::time::sleep` instead of `tokio::time::sleep`
/// - `std::pin::pin!` instead of `tokio::pin!`
/// - `monoio::time::sleep(Duration::from_micros(10))` for SPSC backpressure
///
/// CRITICAL: RefCell borrows MUST be dropped before any .await point.
///
/// c10k A1: `stream` is watched for EOF throughout the wait, but unlike the
/// tokio twin the watch is a two-step — [`IdleParkRead::peer_readable`] inside
/// the `select!`, [`drain_peer_bytes`] outside it. A monoio read owns its
/// buffer, so a read arm losing the race could take client bytes down with the
/// cancelled op; a readiness poll owns nothing and is safe to drop.
///
/// Streams whose `peer_readable` is the never-resolving default (TLS — see the
/// trait docs) keep the pre-A1 behaviour: their blocked clients are still not
/// reapable by disconnect. TLS at least requires a completed handshake, so it
/// is not the unauthenticated one-command DoS this fixes.
#[cfg(feature = "runtime-monoio")]
#[allow(clippy::await_holding_refcell_ref)]
pub(crate) async fn handle_blocking_command_monoio<S>(
    cmd: &[u8],
    args: &[Frame],
    selected_db: usize,
    _shard_databases: &std::sync::Arc<ShardDatabases>,
    blocking_registry: &Rc<RefCell<crate::blocking::BlockingRegistry>>,
    shard_id: usize,
    num_shards: usize,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    shutdown: &CancellationToken,
    spsc_notifiers: &[Arc<channel::Notify>],
    stream: &mut S,
    carry: &mut BytesMut,
) -> BlockingOutcome
where
    S: super::handler_monoio::idle_park::IdleParkRead,
{
    use futures::stream::FuturesUnordered;

    // Parse timeout (last argument for all blocking commands)
    let timeout_secs = match parse_blocking_timeout(cmd, args) {
        Ok(t) => t,
        Err(e) => return BlockingOutcome::Reply(e),
    };

    // Parse keys and command-specific args
    let (keys, blocked_cmd_factory) = match parse_blocking_args(cmd, args) {
        Ok(v) => v,
        Err(e) => return BlockingOutcome::Reply(e),
    };

    // Reused across every drain in this wait — one allocation for a client
    // that actually pipelines, none for the overwhelming majority that do not.
    let mut peer_scratch: Vec<u8> = Vec::new();

    // --- Non-blocking fast path: try to get data immediately ---
    // Use with_shard_db (thread-local ShardSlice) — no RwLock guard needed.
    {
        let immediate_result = crate::shard::slice::with_shard_db(selected_db, |db| {
            immediate_scan(cmd, args, &keys, db, shard_id, num_shards)
        });
        if let Some(frame) = immediate_result {
            return BlockingOutcome::Reply(frame);
        }
    }

    let deadline = if timeout_secs > 0.0 {
        Some(std::time::Instant::now() + std::time::Duration::from_secs_f64(timeout_secs))
    } else {
        None // 0 = block forever
    };

    // --- Single-key fast path: one registration, direct await (zero overhead) ---
    if keys.len() == 1 {
        let target = key_to_shard(&keys[0], num_shards);
        let is_remote = target != shard_id;
        let (reply_tx, mut reply_rx) = channel::oneshot::<Option<Frame>>();
        let wait_id = blocking_registry.borrow_mut().next_wait_id();
        if is_remote {
            // A4: the old `loop { try_push; sleep(10µs) }` had no retry cap
            // and no shutdown arm — a saturated owner turned this into a
            // zombie connection that also blocked graceful shutdown.
            let msg = ShardMessage::BlockRegister(Box::new(
                crate::shard::dispatch::BlockRegisterPayload {
                    db_index: selected_db,
                    key: keys[0].clone(),
                    wait_id,
                    cmd: blocked_cmd_factory(),
                    reply_tx,
                    // The single-key fast path: this key IS the command, so
                    // the owner may answer a type error for it (moon#556).
                    sole_key: true,
                },
            ));
            if !push_block_msg(
                shutdown,
                dispatch_tx,
                shard_id,
                target,
                msg,
                Some(&spsc_notifiers[target]),
            )
            .await
            {
                return BlockingOutcome::Reply(Frame::Error(Bytes::from_static(
                    BLOCK_REGISTER_FAILED,
                )));
            }
        } else {
            let entry = crate::blocking::WaitEntry {
                wait_id,
                cmd: blocked_cmd_factory(),
                reply_tx,
                deadline,
            };
            blocking_registry
                .borrow_mut()
                .register(selected_db, keys[0].clone(), entry);
        }

        // A1: see the tokio twin for why these are loops. The peer arm only
        // awaits READINESS; the read that follows happens after the select!
        // has already resolved, where no cancellation can reach it.
        let result = if let Some(dl) = deadline {
            let mut sleep = std::pin::pin!(monoio::time::sleep(
                dl.saturating_duration_since(std::time::Instant::now())
            ));
            loop {
                let ready = monoio::select! {
                    res = &mut reply_rx => {
                        break match res {
                            Ok(Some(frame)) => Some(frame),
                            Ok(None) | Err(_) => Some(Frame::NullArray),
                        };
                    }
                    _ = &mut sleep => {
                        blocking_registry.borrow_mut().remove_wait(wait_id);
                        break Some(Frame::NullArray);
                    }
                    _ = shutdown.cancelled() => {
                        blocking_registry.borrow_mut().remove_wait(wait_id);
                        break Some(Frame::Error(Bytes::from_static(b"ERR server shutting down")));
                    }
                    r = stream.peer_readable() => r,
                };
                if peer_wake_ends_wait_monoio(ready, stream, &mut peer_scratch, carry).await {
                    blocking_registry.borrow_mut().remove_wait(wait_id);
                    break None;
                }
            }
        } else {
            loop {
                let ready = monoio::select! {
                    res = &mut reply_rx => {
                        break match res {
                            Ok(Some(frame)) => Some(frame),
                            Ok(None) | Err(_) => Some(Frame::NullArray),
                        };
                    }
                    _ = shutdown.cancelled() => {
                        blocking_registry.borrow_mut().remove_wait(wait_id);
                        break Some(Frame::Error(Bytes::from_static(b"ERR server shutting down")));
                    }
                    r = stream.peer_readable() => r,
                };
                if peer_wake_ends_wait_monoio(ready, stream, &mut peer_scratch, carry).await {
                    blocking_registry.borrow_mut().remove_wait(wait_id);
                    break None;
                }
            }
        };
        if is_remote {
            // A6: bounded push + notify instead of a bare `try_push` that
            // leaked the owner's WaitEntry whenever the ring was full.
            let _ = push_block_msg(
                shutdown,
                dispatch_tx,
                shard_id,
                target,
                ShardMessage::BlockCancel { wait_id },
                Some(&spsc_notifiers[target]),
            )
            .await;
        }
        return match result {
            Some(frame) => BlockingOutcome::Reply(frame),
            None => BlockingOutcome::PeerGone,
        };
    }

    // --- Multi-key coordinator: register on ALL keys across local + remote shards ---
    // Uses FuturesUnordered for first-wakeup-wins semantics.
    let wait_id;
    let mut receivers: FuturesUnordered<channel::OneshotReceiver<Option<Frame>>> =
        FuturesUnordered::new();
    let mut registered_remote_shards: Vec<usize> = Vec::new();

    // A4/A5: stage remote registrations, push them after the borrow is
    // released. See the tokio twin for the full rationale.
    let mut pending_remote: Vec<(usize, ShardMessage)> = Vec::new();
    {
        let mut reg = blocking_registry.borrow_mut();
        wait_id = reg.next_wait_id();

        for key in &keys {
            let target = key_to_shard(key, num_shards);
            let (tx, rx) = channel::oneshot::<Option<Frame>>();
            receivers.push(rx);

            if target == shard_id {
                // Local registration
                let entry = crate::blocking::WaitEntry {
                    wait_id,
                    cmd: blocked_cmd_factory(),
                    reply_tx: tx,
                    deadline,
                };
                reg.register(selected_db, key.clone(), entry);
            } else {
                // Remote registration via SPSC
                let msg = ShardMessage::BlockRegister(Box::new(
                    crate::shard::dispatch::BlockRegisterPayload {
                        db_index: selected_db,
                        key: key.clone(),
                        wait_id,
                        cmd: blocked_cmd_factory(),
                        reply_tx: tx,
                        // One of several keys — see `sole_key`'s docs for why
                        // the owner must NOT decide the whole command here.
                        sole_key: false,
                    },
                ));
                pending_remote.push((target, msg));
                if !registered_remote_shards.contains(&target) {
                    registered_remote_shards.push(target);
                }
            }
        }
    } // borrows dropped -- CRITICAL before await

    let mut registration_failed = false;
    for (target, msg) in pending_remote {
        if !push_block_msg(
            shutdown,
            dispatch_tx,
            shard_id,
            target,
            msg,
            Some(&spsc_notifiers[target]),
        )
        .await
        {
            registration_failed = true;
            break;
        }
    }
    if registration_failed {
        cancel_multikey_registrations(
            shutdown,
            blocking_registry,
            dispatch_tx,
            shard_id,
            wait_id,
            &registered_remote_shards,
            Some(spsc_notifiers),
        )
        .await;
        drop(receivers);
        return BlockingOutcome::Reply(Frame::Error(Bytes::from_static(BLOCK_REGISTER_FAILED)));
    }

    // Await first successful result from any key/shard.
    // FuturesUnordered may return Err (sender dropped by remove_wait cleanup) before
    // returning the successful Ok. We must skip Err/None results and keep polling.
    let mut peer_gone = false;
    let frame = if let Some(dl) = deadline {
        let mut sleep = std::pin::pin!(monoio::time::sleep(
            dl.saturating_duration_since(std::time::Instant::now())
        ));
        let mut result_frame = Frame::NullArray;
        loop {
            let ready = monoio::select! {
                result = receivers.next() => {
                    match result {
                        Some(Ok(Some(frame))) => { result_frame = frame; break; }
                        Some(Ok(None)) | Some(Err(_)) => continue, // cancelled/dropped, try next
                        None => break, // all receivers exhausted
                    }
                }
                _ = &mut sleep => break,
                _ = shutdown.cancelled() => {
                    result_frame = Frame::Error(Bytes::from_static(b"ERR server shutting down"));
                    break;
                }
                r = stream.peer_readable() => r,
            };
            if peer_wake_ends_wait_monoio(ready, stream, &mut peer_scratch, carry).await {
                peer_gone = true;
                break;
            }
        }
        result_frame
    } else {
        let mut result_frame = Frame::NullArray;
        loop {
            let ready = monoio::select! {
                result = receivers.next() => {
                    match result {
                        Some(Ok(Some(frame))) => { result_frame = frame; break; }
                        Some(Ok(None)) | Some(Err(_)) => continue, // cancelled/dropped, try next
                        None => break,
                    }
                }
                _ = shutdown.cancelled() => {
                    result_frame = Frame::Error(Bytes::from_static(b"ERR server shutting down"));
                    break;
                }
                r = stream.peer_readable() => r,
            };
            if peer_wake_ends_wait_monoio(ready, stream, &mut peer_scratch, carry).await {
                peer_gone = true;
                break;
            }
        }
        result_frame
    };

    // Cleanup: cancel all remaining registrations (local + remote)
    cancel_multikey_registrations(
        shutdown,
        blocking_registry,
        dispatch_tx,
        shard_id,
        wait_id,
        &registered_remote_shards,
        Some(spsc_notifiers),
    )
    .await;
    // Drop remaining receivers; remote senders get Err on send -- harmless
    drop(receivers);

    if peer_gone {
        BlockingOutcome::PeerGone
    } else {
        BlockingOutcome::Reply(frame)
    }
}

/// Parse timeout from a blocking command.
/// For most commands, timeout is the last argument.
/// For BLMPOP, timeout is the first argument.
/// Returns seconds as f64. 0 = block forever.
pub(crate) fn parse_blocking_timeout(cmd: &[u8], args: &[Frame]) -> Result<f64, Frame> {
    if args.is_empty() {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for blocking command",
        )));
    }
    // BLMPOP/BZMPOP: timeout is the FIRST argument; others: last argument
    let timeout_frame =
        if cmd.eq_ignore_ascii_case(b"BLMPOP") || cmd.eq_ignore_ascii_case(b"BZMPOP") {
            &args[0]
        } else {
            // args confirmed non-empty above
            #[allow(clippy::unwrap_used)] // args.is_empty() checked at entry
            args.last().unwrap()
        };
    let timeout_bytes = match timeout_frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => b,
        _ => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR timeout is not a float or out of range",
            )));
        }
    };
    let timeout_str = std::str::from_utf8(timeout_bytes).map_err(|_| {
        Frame::Error(Bytes::from_static(
            b"ERR timeout is not a float or out of range",
        ))
    })?;
    let timeout: f64 = timeout_str.parse().map_err(|_| {
        Frame::Error(Bytes::from_static(
            b"ERR timeout is not a float or out of range",
        ))
    })?;
    if !timeout.is_finite() || timeout < 0.0 {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR timeout is not a float or out of range",
        )));
    }
    Ok(timeout)
}

/// Parse keys and build a BlockedCommand factory from blocking command args.
pub(crate) fn parse_blocking_args(
    cmd: &[u8],
    args: &[Frame],
) -> Result<(Vec<Bytes>, Box<dyn Fn() -> crate::blocking::BlockedCommand>), Frame> {
    if cmd.eq_ignore_ascii_case(b"BLPOP") {
        // BLPOP key [key ...] timeout
        if args.len() < 2 {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'blpop' command",
            )));
        }
        let keys: Vec<Bytes> = args[..args.len() - 1]
            .iter()
            .filter_map(|f| extract_bytes(f))
            .collect();
        if keys.is_empty() {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'blpop' command",
            )));
        }
        Ok((keys, Box::new(|| crate::blocking::BlockedCommand::BLPop)))
    } else if cmd.eq_ignore_ascii_case(b"BRPOP") {
        if args.len() < 2 {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'brpop' command",
            )));
        }
        let keys: Vec<Bytes> = args[..args.len() - 1]
            .iter()
            .filter_map(|f| extract_bytes(f))
            .collect();
        if keys.is_empty() {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'brpop' command",
            )));
        }
        Ok((keys, Box::new(|| crate::blocking::BlockedCommand::BRPop)))
    } else if cmd.eq_ignore_ascii_case(b"BLMOVE") {
        // BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
        if args.len() != 5 {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'blmove' command",
            )));
        }
        let source = extract_bytes(&args[0])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid source key")))?;
        let destination = extract_bytes(&args[1])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid destination key")))?;
        let wherefrom_bytes = extract_bytes(&args[2])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR syntax error")))?;
        let whereto_bytes = extract_bytes(&args[3])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR syntax error")))?;
        let wherefrom = if wherefrom_bytes.eq_ignore_ascii_case(b"LEFT") {
            crate::blocking::Direction::Left
        } else if wherefrom_bytes.eq_ignore_ascii_case(b"RIGHT") {
            crate::blocking::Direction::Right
        } else {
            return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
        };
        let whereto = if whereto_bytes.eq_ignore_ascii_case(b"LEFT") {
            crate::blocking::Direction::Left
        } else if whereto_bytes.eq_ignore_ascii_case(b"RIGHT") {
            crate::blocking::Direction::Right
        } else {
            return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
        };
        Ok((
            vec![source],
            Box::new(move || crate::blocking::BlockedCommand::BLMove {
                destination: destination.clone(),
                wherefrom,
                whereto,
            }),
        ))
    } else if cmd.eq_ignore_ascii_case(b"BZPOPMIN") {
        if args.len() < 2 {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'bzpopmin' command",
            )));
        }
        let keys: Vec<Bytes> = args[..args.len() - 1]
            .iter()
            .filter_map(|f| extract_bytes(f))
            .collect();
        if keys.is_empty() {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'bzpopmin' command",
            )));
        }
        Ok((keys, Box::new(|| crate::blocking::BlockedCommand::BZPopMin)))
    } else if cmd.eq_ignore_ascii_case(b"BZPOPMAX") {
        if args.len() < 2 {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'bzpopmax' command",
            )));
        }
        let keys: Vec<Bytes> = args[..args.len() - 1]
            .iter()
            .filter_map(|f| extract_bytes(f))
            .collect();
        if keys.is_empty() {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'bzpopmax' command",
            )));
        }
        Ok((keys, Box::new(|| crate::blocking::BlockedCommand::BZPopMax)))
    } else if cmd.eq_ignore_ascii_case(b"BLMPOP") {
        // BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT n]
        // args[0] = timeout (already parsed), args[1] = numkeys, args[2..2+numkeys] = keys,
        // args[2+numkeys] = direction, optionally args[2+numkeys+1..] = COUNT n
        if args.len() < 4 {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'blmpop' command",
            )));
        }
        let numkeys_bytes = extract_bytes(&args[1])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR syntax error")))?;
        let numkeys: usize = std::str::from_utf8(&numkeys_bytes)
            .map_err(|_| Frame::Error(Bytes::from_static(b"ERR numkeys is not an integer")))?
            .parse()
            .map_err(|_| {
                Frame::Error(Bytes::from_static(
                    b"ERR numkeys is not an integer or is out of range",
                ))
            })?;
        if numkeys == 0 || args.len() < 2 + numkeys + 1 {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR numkeys is not an integer or is out of range",
            )));
        }
        let keys: Vec<Bytes> = args[2..2 + numkeys]
            .iter()
            .filter_map(|f| extract_bytes(f))
            .collect();
        if keys.len() != numkeys {
            return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
        }
        let dir_bytes = extract_bytes(&args[2 + numkeys])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR syntax error")))?;
        let dir = if dir_bytes.eq_ignore_ascii_case(b"LEFT") {
            crate::blocking::Direction::Left
        } else if dir_bytes.eq_ignore_ascii_case(b"RIGHT") {
            crate::blocking::Direction::Right
        } else {
            return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
        };
        // Parse optional COUNT n
        let mut count: u32 = 1;
        let remaining = &args[3 + numkeys..];
        if remaining.len() >= 2 {
            let kw = extract_bytes(&remaining[0]);
            if let Some(kw) = kw {
                if kw.eq_ignore_ascii_case(b"COUNT") {
                    let count_bytes = extract_bytes(&remaining[1])
                        .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR syntax error")))?;
                    count = std::str::from_utf8(&count_bytes)
                        .map_err(|_| {
                            Frame::Error(Bytes::from_static(b"ERR count is not an integer"))
                        })?
                        .parse()
                        .map_err(|_| {
                            Frame::Error(Bytes::from_static(
                                b"ERR count is not an integer or is out of range",
                            ))
                        })?;
                    if count == 0 {
                        return Err(Frame::Error(Bytes::from_static(
                            b"ERR count is not an integer or is out of range",
                        )));
                    }
                }
            }
        }
        Ok((
            keys,
            Box::new(move || crate::blocking::BlockedCommand::BLMPop { dir, count }),
        ))
    } else if cmd.eq_ignore_ascii_case(b"BRPOPLPUSH") {
        // BRPOPLPUSH source destination timeout
        if args.len() != 3 {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'brpoplpush' command",
            )));
        }
        let source = extract_bytes(&args[0])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid source key")))?;
        let destination = extract_bytes(&args[1])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid destination key")))?;
        Ok((
            vec![source],
            Box::new(move || crate::blocking::BlockedCommand::BLMove {
                destination: destination.clone(),
                wherefrom: crate::blocking::Direction::Right,
                whereto: crate::blocking::Direction::Left,
            }),
        ))
    } else if cmd.eq_ignore_ascii_case(b"BZMPOP") {
        // BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT n]
        // args[0] = timeout (already parsed), args[1] = numkeys, args[2..2+numkeys] = keys,
        // args[2+numkeys] = MIN|MAX, optionally args[2+numkeys+1..] = COUNT n
        if args.len() < 4 {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'bzmpop' command",
            )));
        }
        let numkeys_bytes = extract_bytes(&args[1])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR syntax error")))?;
        let numkeys: usize = std::str::from_utf8(&numkeys_bytes)
            .map_err(|_| Frame::Error(Bytes::from_static(b"ERR numkeys is not an integer")))?
            .parse()
            .map_err(|_| {
                Frame::Error(Bytes::from_static(
                    b"ERR numkeys is not an integer or is out of range",
                ))
            })?;
        if numkeys == 0 || args.len() < 2 + numkeys + 1 {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR numkeys is not an integer or is out of range",
            )));
        }
        let keys: Vec<Bytes> = args[2..2 + numkeys]
            .iter()
            .filter_map(|f| extract_bytes(f))
            .collect();
        if keys.len() != numkeys {
            return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
        }
        let side_bytes = extract_bytes(&args[2 + numkeys])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR syntax error")))?;
        let is_min = if side_bytes.eq_ignore_ascii_case(b"MIN") {
            true
        } else if side_bytes.eq_ignore_ascii_case(b"MAX") {
            false
        } else {
            return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
        };
        // Parse optional COUNT n
        let mut count: u32 = 1;
        let remaining = &args[3 + numkeys..];
        if remaining.len() >= 2 {
            let kw = extract_bytes(&remaining[0]);
            if let Some(kw) = kw {
                if kw.eq_ignore_ascii_case(b"COUNT") {
                    let count_bytes = extract_bytes(&remaining[1])
                        .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR syntax error")))?;
                    count = std::str::from_utf8(&count_bytes)
                        .map_err(|_| {
                            Frame::Error(Bytes::from_static(b"ERR count is not an integer"))
                        })?
                        .parse()
                        .map_err(|_| {
                            Frame::Error(Bytes::from_static(
                                b"ERR count is not an integer or is out of range",
                            ))
                        })?;
                    if count == 0 {
                        return Err(Frame::Error(Bytes::from_static(
                            b"ERR count is not an integer or is out of range",
                        )));
                    }
                }
            }
        }
        Ok((
            keys,
            Box::new(move || crate::blocking::BlockedCommand::BZMPop { min: is_min, count }),
        ))
    } else {
        Err(Frame::Error(Bytes::from_static(
            b"ERR unknown blocking command",
        )))
    }
}

/// Which collection a blocking command pops from, i.e. which type its key
/// must hold. `None` for anything that is not a blocking pop.
///
/// Deliberately expressed as the registry's [`WaitFamily`](crate::blocking::WaitFamily)
/// so the type gate below and the waker that will eventually serve the same
/// waiter agree on what "the right type" means by construction.
pub(crate) fn blocking_pop_family(cmd: &[u8]) -> Option<crate::blocking::WaitFamily> {
    use crate::blocking::WaitFamily;
    if cmd.eq_ignore_ascii_case(b"BLPOP")
        || cmd.eq_ignore_ascii_case(b"BRPOP")
        || cmd.eq_ignore_ascii_case(b"BLMOVE")
        || cmd.eq_ignore_ascii_case(b"BRPOPLPUSH")
        || cmd.eq_ignore_ascii_case(b"BLMPOP")
    {
        Some(WaitFamily::List)
    } else if cmd.eq_ignore_ascii_case(b"BZPOPMIN")
        || cmd.eq_ignore_ascii_case(b"BZPOPMAX")
        || cmd.eq_ignore_ascii_case(b"BZMPOP")
    {
        Some(WaitFamily::ZSet)
    } else {
        None
    }
}

/// moon#556: the `-WRONGTYPE` a blocking pop owes its client when `key`
/// already exists holding a type this command cannot pop from.
///
/// Redis answers this at once and never blocks (`blockingPopGenericCommand`
/// runs `checkType` on every key before it considers waiting). Moon used to
/// block instead, because the pop helpers reach the store through
/// `get_mut_if_present(..).ok()??` — an `Err(WRONGTYPE)` that becomes `None`,
/// indistinguishable from "empty", so the caller registered and waited on a
/// key that can never serve it.
///
/// Read-only: a missing key, an empty key and a right-typed key are all
/// `None`, and nothing here mutates the keyspace. The in-MULTI path
/// ([`super::blocking_txn`]) has had this gate since moon#524; this is the
/// live path catching up to it.
pub(crate) fn blocking_wrongtype_error(
    cmd: &[u8],
    db: &mut Database,
    key: &Bytes,
) -> Option<Frame> {
    match blocking_pop_family(cmd)? {
        crate::blocking::WaitFamily::List => db.get_list(key).err(),
        crate::blocking::WaitFamily::ZSet => db.get_sorted_set(key).err(),
        // Stream blocking (XREAD) does not come through this path.
        crate::blocking::WaitFamily::Stream => None,
    }
}

/// The scan a blocking pop runs over its keys BEFORE it decides to block.
///
/// Answers `Some(frame)` when the command can be completed right now — a
/// served pop, or an error the client is owed — and `None` when it must
/// register and wait.
///
/// Keys are visited left to right and the FIRST one that either errors or
/// serves decides the reply, which is what Redis does: an existing key of the
/// wrong type is an error even when a later key could have served the pop.
///
/// One shared implementation for both runtimes' `handle_blocking_command*`,
/// so the two cannot drift.
pub(crate) fn immediate_scan(
    cmd: &[u8],
    args: &[Frame],
    keys: &[Bytes],
    db: &mut Database,
    shard_id: usize,
    num_shards: usize,
) -> Option<Frame> {
    for key in keys {
        // moon#557: `db` is the CLIENT'S OWN shard slice, so this scan may
        // only speak for the keys this shard owns. A key that hashes elsewhere
        // lives in another shard's `Database` and is answered where it lives —
        // the registration the caller is about to send carries it to its owner,
        // whose `BlockRegister` handler serves it on the spot if data is
        // already there (`spsc_handler.rs`). Consulting the local slice for it
        // can only produce a wrong answer from a stale look-alike, and since
        // moon#556 that includes inventing a `-WRONGTYPE` for a key whose real
        // owner holds the right type.
        if num_shards > 1 && key_to_shard(key, num_shards) != shard_id {
            continue;
        }
        // moon#556: the type gate runs BEFORE the pop attempt and before any
        // registration, so a wrong-typed key is an error rather than a wait.
        if let Some(err) = blocking_wrongtype_error(cmd, db, key) {
            return Some(err);
        }
        if let Some(frame) = try_immediate_pop(cmd, db, key, args) {
            return Some(frame);
        }
    }
    None
}

/// moon#556: the `-WRONGTYPE` a `BLMOVE`/`BRPOPLPUSH` owes its client because
/// the DESTINATION holds something that is not a list.
///
/// Two rules, both taken from `lmoveGenericCommand` (which moon's own
/// non-blocking `LMOVE` already mirrors, `command/list/list_write.rs`):
///
/// * the destination's type is consulted only when the move is actually about
///   to happen. An absent or empty source blocks, and Redis never looks at the
///   destination on that path — so neither does this;
/// * the error arrives INSTEAD of the move. Pre-fix the element was popped
///   from the source and then silently swallowed by `list_push_*`'s
///   `if let Ok(list)`: the client got the value in its reply while the value
///   left the keyspace entirely.
///
/// `source == destination` is the rotate form: same key, therefore same type,
/// nothing to check.
fn move_destination_error(db: &mut Database, source: &Bytes, dest: &Bytes) -> Option<Frame> {
    if source == dest {
        return None;
    }
    match db.get_list(source) {
        Ok(Some(list)) if !list.is_empty() => {}
        _ => return None,
    }
    db.get_list(dest).err()
}

/// Try to pop data immediately (non-blocking fast path).
pub(crate) fn try_immediate_pop(
    cmd: &[u8],
    db: &mut Database,
    key: &Bytes,
    args: &[Frame],
) -> Option<Frame> {
    if cmd.eq_ignore_ascii_case(b"BLPOP") {
        db.list_pop_front(key).map(|v| {
            Frame::Array(framevec![
                Frame::BulkString(key.clone()),
                Frame::BulkString(v),
            ])
        })
    } else if cmd.eq_ignore_ascii_case(b"BRPOP") {
        db.list_pop_back(key).map(|v| {
            Frame::Array(framevec![
                Frame::BulkString(key.clone()),
                Frame::BulkString(v),
            ])
        })
    } else if cmd.eq_ignore_ascii_case(b"BZPOPMIN") {
        db.zset_pop_min(key).map(|(member, score)| {
            Frame::Array(framevec![
                Frame::BulkString(key.clone()),
                Frame::BulkString(member),
                Frame::BulkString(Bytes::from(format_blocking_score(score))),
            ])
        })
    } else if cmd.eq_ignore_ascii_case(b"BZPOPMAX") {
        db.zset_pop_max(key).map(|(member, score)| {
            Frame::Array(framevec![
                Frame::BulkString(key.clone()),
                Frame::BulkString(member),
                Frame::BulkString(Bytes::from(format_blocking_score(score))),
            ])
        })
    } else if cmd.eq_ignore_ascii_case(b"BLMOVE") {
        // BLMOVE: try immediate LMOVE
        // args: [source, destination, wherefrom, whereto, timeout]
        use crate::blocking::Direction;
        let dest = extract_bytes(&args[1])?;
        let wherefrom = if extract_bytes(&args[2])?.eq_ignore_ascii_case(b"LEFT") {
            Direction::Left
        } else {
            Direction::Right
        };
        let whereto = if extract_bytes(&args[3])?.eq_ignore_ascii_case(b"LEFT") {
            Direction::Left
        } else {
            Direction::Right
        };
        if let Some(err) = move_destination_error(db, key, &dest) {
            return Some(err);
        }
        let val = match wherefrom {
            Direction::Left => db.list_pop_front(key),
            Direction::Right => db.list_pop_back(key),
        }?;
        match whereto {
            Direction::Left => db.list_push_front(&dest, val.clone()),
            Direction::Right => db.list_push_back(&dest, val.clone()),
        }
        Some(Frame::BulkString(val))
    } else if cmd.eq_ignore_ascii_case(b"BLMPOP") {
        // BLMPOP: immediate pop up to COUNT elements
        // args: [timeout, numkeys, key [key ...], LEFT|RIGHT, [COUNT n]]
        let numkeys_bytes = extract_bytes(&args[1])?;
        let numkeys: usize = std::str::from_utf8(&numkeys_bytes).ok()?.parse().ok()?;
        if numkeys == 0 || args.len() < 2 + numkeys + 1 {
            return None;
        }
        let dir_bytes = extract_bytes(&args[2 + numkeys])?;
        let dir = if dir_bytes.eq_ignore_ascii_case(b"LEFT") {
            crate::blocking::Direction::Left
        } else {
            crate::blocking::Direction::Right
        };
        // Parse COUNT
        let mut count: u32 = 1;
        let remaining = &args[3 + numkeys..];
        if remaining.len() >= 2 {
            if let Some(kw) = extract_bytes(&remaining[0]) {
                if kw.eq_ignore_ascii_case(b"COUNT") {
                    if let Some(cb) = extract_bytes(&remaining[1]) {
                        if let Some(c) = std::str::from_utf8(&cb)
                            .ok()
                            .and_then(|s| s.parse::<u32>().ok())
                        {
                            if c > 0 {
                                count = c;
                            }
                        }
                    }
                }
            }
        }
        // Check if list has elements
        let list_len = match db.get_list(key) {
            Ok(Some(l)) => l.len(),
            _ => 0,
        };
        if list_len == 0 {
            return None;
        }
        let n = std::cmp::min(count as usize, list_len);
        let mut elems = smallvec::SmallVec::<[Frame; 16]>::new();
        for _ in 0..n {
            let val = match dir {
                crate::blocking::Direction::Left => db.list_pop_front(key),
                crate::blocking::Direction::Right => db.list_pop_back(key),
            };
            match val {
                Some(v) => elems.push(Frame::BulkString(v)),
                None => break,
            }
        }
        if elems.is_empty() {
            None
        } else {
            let elem_vec: Vec<Frame> = elems.into_vec();
            Some(Frame::Array(framevec![
                Frame::BulkString(key.clone()),
                Frame::Array(elem_vec.into()),
            ]))
        }
    } else if cmd.eq_ignore_ascii_case(b"BRPOPLPUSH") {
        // BRPOPLPUSH: immediate RPOP from source, LPUSH to destination
        // args: [source, destination, timeout]
        let dest = extract_bytes(&args[1])?;
        if let Some(err) = move_destination_error(db, key, &dest) {
            return Some(err);
        }
        let val = db.list_pop_back(key)?;
        db.list_push_front(&dest, val.clone());
        Some(Frame::BulkString(val))
    } else if cmd.eq_ignore_ascii_case(b"BZMPOP") {
        // BZMPOP: immediate pop from sorted set
        // args: [timeout, numkeys, key [key ...], MIN|MAX, [COUNT n]]
        let numkeys_bytes = extract_bytes(&args[1])?;
        let numkeys: usize = std::str::from_utf8(&numkeys_bytes).ok()?.parse().ok()?;
        if numkeys == 0 || args.len() < 2 + numkeys + 1 {
            return None;
        }
        let side_bytes = extract_bytes(&args[2 + numkeys])?;
        let is_min = side_bytes.eq_ignore_ascii_case(b"MIN");
        // Parse COUNT
        let mut count: u32 = 1;
        let remaining = &args[3 + numkeys..];
        if remaining.len() >= 2 {
            if let Some(kw) = extract_bytes(&remaining[0]) {
                if kw.eq_ignore_ascii_case(b"COUNT") {
                    if let Some(cb) = extract_bytes(&remaining[1]) {
                        if let Some(c) = std::str::from_utf8(&cb)
                            .ok()
                            .and_then(|s| s.parse::<u32>().ok())
                        {
                            if c > 0 {
                                count = c;
                            }
                        }
                    }
                }
            }
        }
        // Check if sorted set has elements (use zset_pop to check)
        let n = count as usize;
        let mut elems = smallvec::SmallVec::<[Frame; 16]>::new();
        for _ in 0..n {
            let popped = if is_min {
                db.zset_pop_min(key)
            } else {
                db.zset_pop_max(key)
            };
            match popped {
                Some((member, score)) => {
                    elems.push(Frame::Array(framevec![
                        Frame::BulkString(member),
                        Frame::BulkString(Bytes::from(format_blocking_score(score))),
                    ]));
                }
                None => break,
            }
        }
        if elems.is_empty() {
            None
        } else {
            let elem_vec: Vec<Frame> = elems.into_vec();
            Some(Frame::Array(framevec![
                Frame::BulkString(key.clone()),
                Frame::Array(elem_vec.into()),
            ]))
        }
    } else {
        None
    }
}

/// Format a float score the same way Redis does (integer if whole, otherwise full precision).
pub(crate) fn format_blocking_score(score: f64) -> String {
    if score == score.floor() && score.abs() < i64::MAX as f64 {
        format!("{}", score as i64)
    } else {
        format!("{}", score)
    }
}

/// Inline dispatch: attempt to process a single GET or plain SET command directly
/// from raw RESP bytes in `read_buf`, bypassing Frame construction and the dispatch
/// table entirely.
///
/// **GET:** read-only, no side-effects.  Handles cold storage fallback.
///
/// **SET:** plain `SET key value` only (exactly `*3` args, no NX/XX/EX/PX options).
///   Side-effects handled by this path:
///   - maxmemory eviction (`evict_to_budget`)
///   - AOF append (raw RESP bytes, zero re-serialization)
///
///   Side-effects intentionally skipped (caller gates via `can_inline_writes`):
///   - ACL permission check (caller sets `can_inline_writes = false` unless
///     `cached_acl_unrestricted`)
///   - CLIENT TRACKING invalidation (guarded by `!tracking_state.enabled`)
///   - MULTI transaction queue (guarded by `!in_multi`)
///   - Metrics / slowlog recording (matches existing inline GET behaviour)
///
///   Side-effects not applicable to plain SET:
///   - Blocking-waiter wakeup (only for LPUSH/RPUSH/ZADD, not SET)
///   - Vector auto-index (only for HSET, not SET)
///
/// Returns the number of commands inlined (0 if none, 1 on success).
/// On success the serialized response is appended to `write_buf`.
#[cfg(feature = "runtime-monoio")]
pub(crate) fn try_inline_dispatch(
    read_buf: &mut BytesMut,
    write_buf: &mut BytesMut,
    shard_databases: &std::sync::Arc<ShardDatabases>,
    shard_id: usize,
    selected_db: usize,
    aof_pool: &Option<std::sync::Arc<crate::persistence::aof::AofWriterPool>>,
    repl_state: &Option<
        std::sync::Arc<parking_lot::RwLock<crate::replication::state::ReplicationState>>,
    >,
    now_ms: u64,
    num_shards: usize,
    can_inline_reads: bool,
    can_inline_writes: bool,
    // moon#522: the connection's negotiated protocol. This path frames reply
    // bytes itself, so it must be TOLD the protocol version — every other
    // reply on the connection gets it from the serializer. Without it a RESP3
    // client received the RESP2 null bulk for any key its own shard owned.
    resp3: bool,
    runtime_config: &parking_lot::RwLock<crate::config::RuntimeConfig>,
) -> usize {
    let buf = &read_buf[..];
    let len = buf.len();

    // Minimum valid command: *2\r\n$3\r\nGET\r\n$1\r\nX\r\n = 20 bytes
    if len < 20 || buf[0] != b'*' {
        return 0;
    }

    // H1: under `appendfsync=always` we MUST fsync before +OK. The inline
    // SET path is synchronous and cannot await the writer's ack, so
    // refuse to inline writes when Always is in effect. GETs and other
    // read-only commands are fine to inline. The non-inline dispatch
    // path (handler_monoio/handler_sharded) uses
    // `AofWriterPool::try_send_append_durable` and awaits the ack.
    if let Some(pool) = aof_pool {
        if pool.fsync_policy() == crate::persistence::aof::FsyncPolicy::Always && buf[1] == b'3'
        // SET shape (*3 ...); GETs (*2) are still safe to inline.
        {
            return 0;
        }
    }

    // Parse array count: only *2 (GET) and *3 (SET plain) are inlined.
    let argc = buf[1];
    if buf[2] != b'\r' || buf[3] != b'\n' {
        return 0;
    }
    // `can_inline_reads` is NOT optional bookkeeping: this path answers GET
    // without entering generic dispatch, so it runs neither the ACL
    // command/key check nor `tracking::invalidation::track_read_keys`.
    // Ungated it was (a) an ACL bypass — a `-@all` user read any key by name
    // — and (b) a silent client-side-caching failure: a tracking client's own
    // GETs were never registered, so nothing ever invalidated them.
    // See `tests/acl_inline_read_enforcement.rs` and the inline-GET cases in
    // `tests/client_tracking_invalidation.rs`.
    let is_get = argc == b'2' && can_inline_reads;
    let is_set = argc == b'3' && can_inline_writes;
    if !is_get && !is_set {
        return 0;
    }

    // Expect $3\r\n for 3-letter command name (GET or SET)
    if buf[4] != b'$' || buf[5] != b'3' || buf[6] != b'\r' || buf[7] != b'\n' {
        return 0;
    }

    // Command name at positions 8,9,10
    let cmd_upper = [
        buf[8].to_ascii_uppercase(),
        buf[9].to_ascii_uppercase(),
        buf[10].to_ascii_uppercase(),
    ];
    if buf[11] != b'\r' || buf[12] != b'\n' {
        return 0;
    }

    // Validate command matches argc
    match (&cmd_upper, is_get) {
        ([b'G', b'E', b'T'], true) => {}
        ([b'S', b'E', b'T'], false) => {}
        _ => return 0,
    }

    // --- Parse first bulk-string argument (the key) ---
    if len <= 13 || buf[13] != b'$' {
        return 0;
    }
    let mut pos = 14usize;
    let mut key_len: usize = 0;
    while pos < len && buf[pos] != b'\r' {
        let d = buf[pos];
        if d < b'0' || d > b'9' {
            return 0;
        }
        // Saturating arithmetic defends against a malicious client sending
        // a huge digit run. On overflow `key_len` clamps to `usize::MAX`,
        // which trips the subsequent bounds check (`key_end + 2 > len`).
        key_len = key_len
            .saturating_mul(10)
            .saturating_add((d - b'0') as usize);
        pos += 1;
    }
    if pos + 1 >= len || buf[pos] != b'\r' || buf[pos + 1] != b'\n' {
        return 0;
    }
    pos += 2;
    let key_start = pos;
    // `checked_add` catches the `key_len = usize::MAX` saturation case above:
    // plain `key_start + key_len` would wrap to a small value and falsely
    // satisfy the subsequent `key_end + 2 > len` bounds check.  Same for
    // `key_end + 2` — on overflow it could wrap below `len` and slip past
    // the guard, then panic on the out-of-bounds `buf[key_end]` index.
    let Some(key_end) = key_start.checked_add(key_len) else {
        return 0;
    };
    let Some(key_end_crlf) = key_end.checked_add(2) else {
        return 0;
    };
    if key_end_crlf > len || buf[key_end] != b'\r' || buf[key_end + 1] != b'\n' {
        return 0;
    }

    // Multi-shard: bail if key routes to a remote shard
    if num_shards > 1 && key_to_shard(&buf[key_start..key_end], num_shards) != shard_id {
        return 0;
    }

    if is_get {
        // ---- GET path (read-only) ----
        // `key_end_crlf` above already validated `key_end + 2 <= len` via
        // checked_add, so reusing it avoids re-deriving a value that was
        // proven safe just above.
        let consumed = key_end_crlf;
        let key_bytes = &buf[key_start..key_end];
        // Hot-key sampling + lookup via thread-local slice — no lock needed.
        // Frame the reply straight from the borrowed `&[u8]` INSIDE the closure so
        // the value is copied exactly once (borrow -> write_buf), never via an
        // intermediate `Vec` (`val.to_vec()`). The closure writes response bytes +
        // itoa ONLY; it must not re-enter `with_shard*` (thread-local RefCell
        // reentrancy) and takes no `.await`.
        enum GetOutcome {
            // Hit or wrong-type: the response is already framed into `write_buf`.
            Handled,
            // Absent locally: consult the cold tier below (`cold_loc` carries where).
            Miss,
        }
        let (outcome, cold_loc) = crate::shard::slice::with_shard_db(selected_db, |db| {
            if db.hot_keys().tick() {
                db.hot_keys().observe(key_bytes);
            }
            // #459: a key mid-spill is in neither hot nor cold, so the miss
            // arm below would frame `$-1` inline for a key that exists and
            // that EXISTS reports as present. Pull it back first — RAM only,
            // no disk read — so the hot lookup answers it. Costs one
            // `is_empty()` load per inline GET on a server that is not
            // spilling, which is every server without --disk-offload.
            db.promote_inflight_if_present(key_bytes, now_ms);
            match db.get_if_alive(key_bytes, now_ms) {
                Some(entry) => match entry.value.as_bytes() {
                    Some(val) => {
                        write_buf.extend_from_slice(b"$");
                        let mut itoa_buf = itoa::Buffer::new();
                        write_buf.extend_from_slice(itoa_buf.format(val.len()).as_bytes());
                        write_buf.extend_from_slice(b"\r\n");
                        write_buf.extend_from_slice(val);
                        write_buf.extend_from_slice(b"\r\n");
                        // This is the route a plain `GET key` actually takes
                        // under monoio — it frames the reply here and returns,
                        // reaching NEITHER `get` nor `get_readonly`. Without
                        // this call `keyspace_hits` stayed at zero on the
                        // shipped runtime while reading correct under tokio,
                        // which is why it survived CI (#477).
                        crate::admin::metrics_setup::record_keyspace_hit();
                        (GetOutcome::Handled, None)
                    }
                    None => {
                        write_buf.extend_from_slice(
                            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                        );
                        (GetOutcome::Handled, None)
                    }
                },
                None => {
                    let loc = db.cold_lookup_location(key_bytes);
                    (GetOutcome::Miss, loc)
                }
            }
        });
        match outcome {
            GetOutcome::Handled => {
                let _ = read_buf.split_to(consumed);
                return 1;
            }
            GetOutcome::Miss => {
                match cold_loc {
                    // task #59 (coverage-gap fix): a REAL cold entry exists
                    // for this key. `try_inline_dispatch` is synchronous --
                    // it cannot `.await` the off-shard-thread pool read
                    // (`storage::tiered::cold_read_pool::read_cold_entry_async`)
                    // the async connection-handler layer uses for exactly
                    // this case. Doing the blocking `pread` HERE, inline on
                    // the shard event-loop thread, is precisely the ~1.9s
                    // stall this task targets -- and plain `GET key` (what
                    // redis-benchmark and the task's repro hit) is served
                    // by THIS inline fast path, not the handler's general
                    // dispatch_read branch, so skipping the fix here left
                    // the benchmarked path completely unchanged.
                    //
                    // Fix: decline to inline this command. Do NOT consume
                    // the bytes (`read_buf` is left untouched, exactly as
                    // every other "not eligible for inlining" early-return
                    // above already does) and do NOT write a response --
                    // return 0 so `try_inline_dispatch_loop` stops and the
                    // caller falls through to normal Frame parsing +
                    // `handler_monoio`'s async GET pre-warm hook, which
                    // `.await`s the REAL result off-thread without blocking
                    // the shard.
                    //
                    // The genuine-miss case (`cold_loc.is_none()` -- key is
                    // absent from BOTH tiers) is unaffected: still answered
                    // inline with a fast `$-1`, no disk I/O either way.
                    Some(_) => return 0,
                    None => {
                        // moon#522: `_` on RESP3, `$-1` on RESP2. The two
                        // other byte strings this path writes — `+OK` and the
                        // `-WRONGTYPE` error — are spelled identically in both
                        // protocols, so this is the only protocol-dependent
                        // reply the inline path frames.
                        write_buf.extend_from_slice(crate::io::static_responses::null_bulk(resp3));
                        crate::admin::metrics_setup::record_keyspace_miss();
                        // Queued, delivered by this shard's own drain. 'm' is
                        // not in the 'A' class, so this is inert unless an
                        // operator asked for keymiss explicitly.
                        crate::notify::notify_keyspace_event(
                            crate::notify::NotifyFlags::KEY_MISS,
                            "keymiss",
                            key_bytes,
                            selected_db,
                        );
                    }
                }
            }
        }
        let _ = read_buf.split_to(consumed);
        return 1;
    }

    // ---- SET path (write, plain *3 only) ----
    // Parse second bulk-string argument (the value)
    pos = key_end + 2;
    if pos >= len || buf[pos] != b'$' {
        return 0;
    }
    pos += 1;
    let mut val_len: usize = 0;
    while pos < len && buf[pos] != b'\r' {
        let d = buf[pos];
        if d < b'0' || d > b'9' {
            return 0;
        }
        // See saturating_mul rationale on the matching key_len parse above.
        val_len = val_len
            .saturating_mul(10)
            .saturating_add((d - b'0') as usize);
        pos += 1;
    }
    if pos + 1 >= len || buf[pos] != b'\r' || buf[pos + 1] != b'\n' {
        return 0;
    }
    pos += 2;
    let val_start = pos;
    // See key_end checked_add above — defends against saturated val_len.
    let Some(val_end) = val_start.checked_add(val_len) else {
        return 0;
    };
    let Some(consumed) = val_end.checked_add(2) else {
        return 0;
    };
    if consumed > len || buf[val_end] != b'\r' || buf[val_end + 1] != b'\n' {
        return 0;
    }

    // Freeze the consumed prefix of `read_buf` into an Arc-backed `Bytes`.
    // This replaces the BytesMut prefix with a refcounted view over the SAME
    // allocation, so `key`, `value`, and the AOF record can all be extracted
    // with `slice()` (Arc refcount bump, no malloc + memcpy).
    //
    // NOTE: this releases the earlier `&read_buf[..]` borrow (held as `buf`).
    // We must not index into `buf` after this point — use `frozen` instead.
    let frozen = read_buf.split_to(consumed).freeze();

    // Eviction check + write via the thread-local slice. The lock-free
    // pre-gate proves the common case (no memory pressure / no limit) without
    // the per-SET `runtime_config.read()` lock pair; only under pressure —
    // or before the hints are published — does the full locked path run.
    {
        let budget = shard_databases.elastic_budget(shard_id);
        let est = crate::shard::slice::with_shard_db(selected_db, |db| db.estimated_memory());
        if !crate::storage::eviction::inline_write_can_skip_eviction(est, budget) {
            let rt = runtime_config.read();
            let oom = crate::shard::slice::with_shard_db(selected_db, |db| {
                crate::storage::eviction::evict_to_budget(
                    db,
                    &rt,
                    crate::storage::eviction::EvictionRun::plain()
                        .budget(budget)
                        .report(
                            // task #34 (Wave A): the inline SET fast path is the
                            // ONLY write-eviction gate that can plain-drop a key
                            // with zero AOF/replication visibility today — the
                            // key vanishes from RAM but the client's `+OK` and any
                            // attached replica/AOF replay never learn it happened.
                            // `can_inline_writes` (this path's caller) already
                            // forces the generic dispatch path once a replica has
                            // ever attached (`fanout_hint_active`), so this sink's
                            // replication leg is a cheap no-op in that case; the
                            // AOF leg still fires unconditionally.
                            &mut |key| {
                                crate::replication::reason_del::record_reason_del_conn(
                                    repl_state,
                                    shard_id,
                                    num_shards,
                                    aof_pool.as_ref(),
                                    selected_db,
                                    key,
                                );
                            },
                        ),
                )
                .is_err()
            });
            drop(rt);
            if oom {
                write_buf.extend_from_slice(
                    b"-OOM command not allowed when used memory > 'maxmemory'\r\n",
                );
                return 1;
            }
        }
        // WS5b: per-db quota, additive and finer-grained than the
        // whole-instance maxmemory gate above. Lock-free pre-gate mirrors
        // `inline_write_can_skip_eviction` above: the overwhelmingly common
        // case (no `--db-maxmemory` configured anywhere) costs one Relaxed
        // atomic load, no `RuntimeConfig` lock.
        if crate::storage::db_quota::db_maxmemory_any_set() {
            let rt = runtime_config.read();
            let quota_err = crate::shard::slice::with_shard_db(selected_db, |db| {
                crate::storage::db_quota::check_db_maxmemory(db, selected_db, &rt).err()
            });
            drop(rt);
            if let Some(crate::protocol::Frame::Error(msg)) = quota_err {
                write_buf.extend_from_slice(b"-");
                write_buf.extend_from_slice(&msg);
                write_buf.extend_from_slice(b"\r\n");
                return 1;
            }
        }

        let key = frozen.slice(key_start..key_end);
        let value = frozen.slice(val_start..val_end);
        // `move` closure: `key` and `value` are consumed here (last use), so the
        // entry/set take ownership — no Bytes refcount bump+drop pair per SET.
        crate::shard::slice::with_shard_db(selected_db, move |db| {
            if db.hot_keys().tick() {
                db.hot_keys().observe(&key);
            }
            // moon#558: this path frames a plain SET straight from the read
            // buffer — it never builds a `Frame`, never enters
            // `command::dispatch`, and never reaches
            // `spsc_handler::cow_intercept`. It must still stash the key's
            // epoch-start value while a BGSAVE is in flight, or the snapshot
            // serializes the overwritten value for a segment it has not
            // written yet. One thread-local `bool` load when no snapshot is
            // armed, which is every SET on a server that is not saving.
            crate::persistence::snapshot_cow::capture_key_pre_image(db, selected_db, &key);
            let mut entry = crate::storage::entry::Entry::new_string(value);
            entry.set_last_access(db.now());
            entry.set_access_counter(5);
            // Same reason as the inline GET above: a plain `SET k v` is served
            // HERE and never reaches `string::set`, so the notification has to
            // be queued on this path too or it exists only for SETs complex
            // enough to fall out of the fast path.
            crate::notify::notify_keyspace_event(
                crate::notify::NotifyFlags::STRING,
                "set",
                &key,
                selected_db,
            );
            db.set(key, entry);
        });
    }

    // AOF: reuse the frozen RESP bytes directly (Arc clone, zero-copy).
    // This path is monoio inline GET/SET — the writer for the local shard
    // (shard_id) owns the AOF record; under PerShard layout that routes
    // to shard_id's writer. LSN must be sourced from `repl_state` so the
    // inline path's writes share an LSN namespace with the non-inline path
    // — otherwise per-shard replay merge in RFC step 5 would see two
    // disjoint LSN streams per shard. Cost: one extra read-lock acquire
    // (uncontended) + one atomic fetch_add per inline SET.
    if let Some(pool) = aof_pool {
        let lsn = crate::persistence::aof::AofWriterPool::issue_append_lsn(
            repl_state,
            shard_id,
            frozen.len(),
        );
        // Bounded backpressure (not fire-and-forget): this path writes `+OK`
        // below, so a silent drop here is a client-acked write that never
        // reached the durability machinery. Fast path is the same try_send;
        // the block is reachable only when the writer channel is completely
        // full. On loss, fail-loud: the SET is applied in memory, but the
        // client must see an error, not `+OK` (review finding, PR #211).
        let mut aof_budget = crate::persistence::aof::AOF_SPSC_BACKPRESSURE_BOUND;
        if !pool.send_append_bounded_blocking(shard_id, lsn, selected_db, frozen, &mut aof_budget) {
            write_buf.extend_from_slice(
                b"-MOONERR AOF backpressure: write applied in memory but not queued \
                  for persistence\r\n",
            );
            return 1;
        }
    }

    write_buf.extend_from_slice(b"+OK\r\n");
    1
}

/// Loop wrapper: call try_inline_dispatch repeatedly until it returns 0.
/// Returns total number of commands inlined.
///
/// `cluster_enabled` must carry `crate::cluster::cluster_enabled()` (a param
/// for unit-testability): in cluster mode NOTHING may inline — reads and
/// writes alike must reach the generic dispatch loop's
/// `try_handle_cluster_routing` so mis-routed keys get MOVED/ASK instead of
/// being silently served/misplaced against the local DashTable (deep-review
/// R6 — the classic "three dispatch paths" gap).
#[cfg(feature = "runtime-monoio")]
#[allow(clippy::too_many_arguments)]
pub(crate) fn try_inline_dispatch_loop(
    read_buf: &mut BytesMut,
    write_buf: &mut BytesMut,
    shard_databases: &std::sync::Arc<ShardDatabases>,
    shard_id: usize,
    selected_db: usize,
    aof_pool: &Option<std::sync::Arc<crate::persistence::aof::AofWriterPool>>,
    repl_state: &Option<
        std::sync::Arc<parking_lot::RwLock<crate::replication::state::ReplicationState>>,
    >,
    now_ms: u64,
    num_shards: usize,
    can_inline_reads: bool,
    can_inline_writes: bool,
    cluster_enabled: bool,
    // moon#522: the connection's negotiated protocol version, forwarded to
    // `try_inline_dispatch` so its self-framed GET miss picks the right null.
    resp3: bool,
    runtime_config: &parking_lot::RwLock<crate::config::RuntimeConfig>,
) -> usize {
    if cluster_enabled {
        return 0;
    }
    let mut total = 0;
    loop {
        let n = try_inline_dispatch(
            read_buf,
            write_buf,
            shard_databases,
            shard_id,
            selected_db,
            aof_pool,
            repl_state,
            now_ms,
            num_shards,
            can_inline_reads,
            can_inline_writes,
            resp3,
            runtime_config,
        );
        if n == 0 {
            break;
        }
        total += n;
    }
    total
}
