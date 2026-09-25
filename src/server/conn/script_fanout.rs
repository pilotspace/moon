//! SCRIPT and FUNCTION mutations across shards: the bounded, acked fan-out
//! that replays a script-cache or function-registry change on every other
//! shard, and the one body each dispatch path runs for `SCRIPT` and
//! `FUNCTION` (moon#514, moon#515, moon#1229, moon#1235, moon#567).
//!
//! Moved out of `shared.rs` verbatim (moon#1226: that file is ~7K lines, far
//! past the 1,500-line rule); `shared` re-exports what the handlers call.

use bytes::Bytes;

use crate::protocol::Frame;

/// Fan one script-cache insert out to every other shard with bounded
/// backpressure (E3), tagged with the flush epoch it was issued under
/// (moon#1235) so every shard places it on the same side of every flush.
///
/// A full ring used to drop the load SILENTLY, leaving that shard's script
/// cache divergent: EVALSHA there answered NOSCRIPT for a sha this server had
/// just returned. On give-up the drop is loud (warn + counter), and the caller
/// decides what the client hears: `SCRIPT LOAD` reports it
/// ([`script_command_fanout`]), `EVAL` keeps the duty owed so its next call
/// republishes ([`eval_script_fanout`]).
pub(crate) async fn script_fanout_bounded(
    ctx: &super::core::ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    script: &Bytes,
    epoch: u64,
) -> FanoutOutcome {
    fanout_to_other_shards(ctx, shutdown, "script_load", |ack| {
        crate::shard::dispatch::ShardMessage::ScriptLoad {
            script: script.clone(),
            epoch,
            ack: Some(ack),
        }
    })
    .await
}

/// Replay `SCRIPT FLUSH` on every other shard and wait until each has flushed
/// (moon#1229).
///
/// The script cache is per shard. A flush that cleared only the connection's
/// shard left every `EVALSHA` whose keys routed elsewhere running the flushed
/// body — measured at `--shards 2` as 18 of 40 calls still executing — and
/// `SCRIPT EXISTS` answering 1 or 0 depending on which shard the connection
/// landed on. redis answers `NOSCRIPT` everywhere once the flush returns.
///
/// Returns the reply the client gets INSTEAD of `+OK` when some shard did not
/// confirm the flush, on the same contract as [`function_registry_fanout`]: a
/// flush is idempotent, so telling the client and letting it re-issue beats
/// an `+OK` over caches that still run the script. `SYNC` and `ASYNC` take the
/// same path: moon's flush is a map clear either way, and redis's `ASYNC`
/// only defers freeing memory — the scripts are gone when it replies.
///
/// `epoch` is the flush's own epoch (moon#1235): each shard drops only what
/// was inserted before it.
#[must_use]
pub(crate) async fn script_flush_fanout(
    ctx: &super::core::ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    epoch: u64,
) -> Option<Frame> {
    if ctx.num_shards <= 1 {
        return None;
    }
    let outcome = fanout_to_other_shards(ctx, shutdown, "script_flush", |ack| {
        crate::shard::dispatch::ShardMessage::ScriptFlush {
            epoch,
            ack: Some(ack),
        }
    })
    .await;
    partial_fanout_reply("SCRIPT FLUSH", outcome)
}

/// The reply that replaces a script mutation's own when its fan-out did not
/// reach every shard. The mutation is idempotent, so re-issuing converges.
fn partial_fanout_reply(what: &str, outcome: FanoutOutcome) -> Option<Frame> {
    match outcome {
        FanoutOutcome::Complete => None,
        FanoutOutcome::Partial { reached, targets } => Some(Frame::Error(Bytes::from(format!(
            "MOONERR partialfanout {what} applied on {} of {} shards; re-issue it to converge",
            reached + 1,
            targets + 1,
        )))),
    }
}

/// Run one `SCRIPT …` command for a connection and return the client's reply:
/// the subcommand on this shard's cache, then the replay the other shards are
/// owed. The one body behind both runtimes' live and MULTI paths.
pub(crate) async fn run_script_command(
    ctx: &super::core::ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    cmd_args: &[Frame],
) -> Frame {
    let (response, fanout) =
        crate::scripting::handle_script_subcommand(&ctx.script_cache, cmd_args);
    script_command_fanout(ctx, shutdown, response, fanout).await
}

/// Replay an accepted `SCRIPT LOAD` / `SCRIPT FLUSH` on every other shard and
/// return the reply the client gets (moon#1235, moon#1229, moon#567).
///
/// `fanout` is what [`crate::scripting::handle_script_subcommand`] says the
/// other shards are owed, epoch included, so the replay files the op in the
/// same order on every shard as on this one. The local reply stands when every
/// shard confirmed; otherwise the client is told the op is only partly applied
/// — for `SCRIPT LOAD` too, which used to answer the sha over a shard that
/// never got the body (moon#567: a redis-py `Lock.release()` then met
/// `NOSCRIPT` for a sha the server had just returned). A client may re-issue
/// either op; both are idempotent.
async fn script_command_fanout(
    ctx: &super::core::ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    response: Frame,
    fanout: Option<crate::scripting::ScriptFanout>,
) -> Frame {
    if ctx.num_shards <= 1 {
        return response;
    }
    let partial = match fanout {
        None => None,
        Some(crate::scripting::ScriptFanout::Load { script, epoch }) => partial_fanout_reply(
            "SCRIPT LOAD",
            script_fanout_bounded(ctx, shutdown, &script, epoch).await,
        ),
        Some(crate::scripting::ScriptFanout::Flush { epoch }) => {
            script_flush_fanout(ctx, shutdown, epoch).await
        }
    };
    partial.unwrap_or(response)
}

/// Shards whose inbound fan-out pushes are forced to fail, from
/// `MOON_TEST_DROP_FANOUT_TO_SHARD` (comma-separated ids). Read ONCE, into a
/// bitmask; never set in production.
///
/// The partial-fan-out path needs a wedged shard to reach, so without an
/// injection point the code that reports a partial `FUNCTION` mutation, and
/// the `ScriptCache` republish-on-retry, would ship covered only by reasoning.
/// The riskiest branch in a fix is the one no test can enter. Same pattern as
/// `MOON_TEST_SLOW_SHARD_START_MS`.
///
/// A SET rather than a single id because a test cannot choose which shard its
/// connection lands on: `SO_REUSEPORT` placement is the kernel's business, and
/// a connection is never a target of its own fan-out, so naming one shard
/// leaves the outcome dependent on where the client happened to land — which
/// is exactly how the first draft of this gate produced a test that passed by
/// accident. Listing every shard makes the fan-out fail from wherever the
/// client lands, deterministically.
fn fanout_drop_mask() -> u64 {
    static GATE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *GATE.get_or_init(|| {
        std::env::var("MOON_TEST_DROP_FANOUT_TO_SHARD")
            .ok()
            .map(|v| {
                v.split(',')
                    .filter_map(|p| p.trim().parse::<u32>().ok())
                    .filter(|id| *id < 64)
                    .fold(0u64, |m, id| m | (1u64 << id))
            })
            .unwrap_or(0)
    })
}

/// Push one message to one shard, retrying a full ring with bounded
/// backpressure and honouring shutdown.
///
/// The single primitive under every fan-out push here, so a second copy of the
/// retry loop cannot drift from the first.
async fn push_bounded(
    ctx: &super::core::ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    target: usize,
    msg: crate::shard::dispatch::ShardMessage,
) -> crate::shard::dispatch::PushOutcome {
    use crate::shard::mesh::ChannelMesh;
    use ringbuf::traits::Producer;

    if target < 64 && fanout_drop_mask() & (1u64 << target) != 0 {
        // Reported as backpressure because that is what a wedged shard looks
        // like from here: never executed, target divergent.
        return crate::shard::dispatch::PushOutcome::Backpressure;
    }
    let idx = ChannelMesh::target_index(ctx.shard_id, target);
    let mut pending = Some(msg);
    let outcome = crate::shard::dispatch::push_with_backpressure(
        shutdown,
        crate::shard::dispatch::CROSS_SHARD_PUSH_MAX_RETRIES,
        crate::shard::dispatch::CROSS_SHARD_PUSH_BACKOFF,
        || match pending.take() {
            None => true,
            Some(m) => {
                // Borrow taken+released inside each attempt, never held
                // across the backoff await.
                let mut producers = ctx.dispatch_tx.borrow_mut();
                match producers[idx].try_push(m) {
                    Ok(()) => true,
                    Err(back) => {
                        pending = Some(back);
                        false
                    }
                }
            }
        },
    )
    .await;
    if matches!(outcome, crate::shard::dispatch::PushOutcome::Pushed) {
        ctx.spsc_notifiers[target].notify_one();
    }
    outcome
}

/// How long a whole scripting fan-out may take — pushes AND acks together.
///
/// Deliberately far shorter than [`crate::shard::dispatch::XSHARD_REPLY_TIMEOUT`]
/// (30s), and deliberately ONE budget for the whole operation rather than a
/// fresh one per shard. Both halves compound per shard if left alone, and the
/// fan-out is on the `EVAL` path, not just on cold admin verbs:
///
/// * Acks: `XSHARD_REPLY_TIMEOUT` covers a target that is EXECUTING a command
///   and may legitimately be slow. An ack covers a queue drain and a map
///   insert — a shard that has not answered in seconds is wedged, not busy.
///   Per-receiver budgets would mean 450s at `--shards 16` with a wedged mesh.
/// * Pushes: `push_bounded` retries a full ring for
///   `CROSS_SHARD_PUSH_MAX_RETRIES × CROSS_SHARD_PUSH_BACKOFF` (~0.5s) per
///   target, sequentially, BEFORE any ack is awaited. Unbounded in aggregate
///   for the same reason.
///
/// On a healthy mesh a push costs microseconds and an ack one round trip, so
/// this ceiling is never approached; it exists so a wedged shard degrades to a
/// reported divergence instead of a stalled client.
///
/// With AOF on, a target's ring can legitimately hold a routed write parked
/// for AOF room ahead of the fan-out message, for up to
/// `AofWriterPool::routed_admission_wait` (moon#769). The budget then grows by
/// that wait (see [`fanout_budget`]) so a slow disk is not reported as a
/// divergent shard.
const FANOUT_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);

/// [`FANOUT_BUDGET`] plus the longest a routed write can sit parked at the
/// head of a target's ring, ahead of the fan-out message, waiting for AOF
/// room. Past that wait the parked write is refused and every later write
/// that finds no room is refused at once, so the ring moves again.
fn fanout_budget(ctx: &super::core::ConnectionContext) -> std::time::Duration {
    FANOUT_BUDGET
        + ctx
            .aof_pool
            .as_ref()
            .map_or(std::time::Duration::ZERO, |p| p.routed_admission_wait())
}

/// What a fan-out managed to do, from the point of view of the client waiting
/// on the command that triggered it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FanoutOutcome {
    /// Every other shard applied the op.
    Complete,
    /// At least one shard did not: the push gave up, the ack never arrived, or
    /// the shard answered that it could not apply it. `reached` counts the
    /// ones that DID apply, out of `targets`.
    Partial { reached: usize, targets: usize },
}

/// Replay one registry mutation on every OTHER shard, WAIT for each to apply
/// it, and report whether they all did.
///
/// # Why it waits
///
/// Pushing to the ring only enqueues; the target applies the message in its
/// own drain loop. Returning `+OK` at push time makes `FUNCTION LOAD` /
/// `SCRIPT LOAD` a lie for as long as that queue takes to drain: a client that
/// loads and then immediately calls — the normal thing to do — can have its
/// `FCALL` reach a shard that has not installed the library yet and get
/// `ERR Function not found` for something the server just accepted. This is
/// not theoretical; it is exactly what `sff9` caught at `--shards 4`, on the
/// FIRST key of the loop.
///
/// `FUNCTION`/`SCRIPT` verbs are cold, administrative and once-per-body, so
/// the round trip is the right trade. Waiting also makes the client-visible
/// contract match Redis's: when the command returns, the state is installed
/// server-wide.
///
/// # Why the verdict is returned rather than logged
///
/// An earlier draft swallowed a partial fan-out, told the client `+OK`, and
/// leaned on a REPAIR LEG — a later routed call that hit `NOSCRIPT` /
/// `ERR Function not found` on the target re-pushed the state from a shard
/// that had it. That is unsound, and adversarial review found two ways it
/// corrupts rather than heals:
///
/// * The error a repair triggers on is also what a shard says when it has
///   ALREADY APPLIED a `FUNCTION DELETE` that has not reached the origin yet.
///   The repair cannot tell "you never got it" from "you already dropped it",
///   so it RESURRECTS deleted libraries — and since a diverged shard keeps
///   re-seeding whichever shard owns the next key, the divergence spreads.
/// * The same mechanism un-does `SCRIPT FLUSH`, which is local-only, putting a
///   flushed body back into the cache it was just evicted from.
///
/// Fixing that with epochs means a total order over registry mutations, which
/// this design does not have. The cheap, honest alternative — now that the
/// acks exist — is to tell the client the truth and let it retry: a retried
/// `FUNCTION DELETE`/`FLUSH` is idempotent, and a retried `FUNCTION LOAD
/// REPLACE` converges. A caller that cannot act on the verdict may ignore it,
/// but nothing in this module may claim success it did not observe.
///
/// # Failure modes, all reported as [`FanoutOutcome::Partial`]
///
/// * The push gives up — the target ring stayed full past the retry budget,
///   shutdown was signalled, or [`FANOUT_BUDGET`] had already expired on an
///   earlier target. That shard never gets the message.
/// * The push lands but no ack arrives inside what is left of
///   [`FANOUT_BUDGET`]. Wedged; the op may still apply later, which is exactly
///   why the caller must not report success.
/// * The ack arrives saying `false` — the shard received the op and could not
///   apply it (no Lua runtime, a colliding function name, a digest mismatch).
///
/// All three are also loud: `warn!` plus `moon_xshard_fanout_drop_total{kind}`.
async fn fanout_to_other_shards(
    ctx: &super::core::ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    kind: &'static str,
    mut make_msg: impl FnMut(
        crate::runtime::channel::OneshotSender<bool>,
    ) -> crate::shard::dispatch::ShardMessage,
) -> FanoutOutcome {
    let targets = ctx.num_shards.saturating_sub(1);
    let mut acks: Vec<(usize, crate::runtime::channel::OneshotReceiver<bool>)> =
        Vec::with_capacity(targets);
    let mut reached = 0usize;
    // ONE deadline for pushes and acks together. A wedged mesh must cost the
    // client a bounded wait, not (retry budget + ack budget) x N shards.
    let budget = fanout_budget(ctx);
    let deadline = std::time::Instant::now() + budget;
    for target in 0..ctx.num_shards {
        if target == ctx.shard_id {
            continue;
        }
        let (ack_tx, ack_rx) = crate::runtime::channel::oneshot();
        let outcome = if std::time::Instant::now() >= deadline {
            // Earlier targets already spent the budget. Reported, not
            // attempted: pushing here would extend the stall by another full
            // retry budget for a mesh that has already proven itself wedged.
            crate::shard::dispatch::PushOutcome::Backpressure
        } else {
            push_bounded(ctx, shutdown, target, make_msg(ack_tx)).await
        };
        if matches!(outcome, crate::shard::dispatch::PushOutcome::Pushed) {
            acks.push((target, ack_rx));
        } else {
            tracing::warn!(
                "shard {}: {kind} fan-out to shard {target} dropped ({outcome:?}); that shard is \
                 divergent until the op is re-issued",
                ctx.shard_id
            );
            crate::admin::metrics_setup::record_xshard_fanout_drop(kind);
        }
    }
    // The targets apply concurrently, so on the happy path this is a single
    // round trip, not N.
    for (target, ack_rx) in acks {
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        match crate::shard::coordinator::recv_reply_within(ack_rx, left).await {
            Ok(true) => reached += 1,
            Ok(false) => {
                // The shard already logged WHY; this side logs that the client
                // is about to be told, which is what an operator correlates.
                tracing::warn!(
                    "shard {}: {kind} fan-out was rejected by shard {target}; it is divergent \
                     until the op is re-issued",
                    ctx.shard_id
                );
                crate::admin::metrics_setup::record_xshard_fanout_drop(kind);
            }
            Err(_) => {
                tracing::warn!(
                    "shard {}: {kind} fan-out to shard {target} was pushed but not acked \
                     within the {budget:?} fan-out budget; that shard may be wedged and \
                     is divergent until it drains or the op is re-issued",
                    ctx.shard_id
                );
                crate::admin::metrics_setup::record_xshard_fanout_drop(kind);
            }
        }
    }
    if reached == targets {
        FanoutOutcome::Complete
    } else {
        FanoutOutcome::Partial { reached, targets }
    }
}

/// Cache an `EVAL`'s body on this shard and, the FIRST time this shard sees
/// it, fan it out to the others (moon#515).
///
/// Redis caches an `EVAL`'d script server-wide, so `EVAL` once then `EVALSHA`
/// by sha is a supported — and extremely common — idiom. moon cached it only
/// in the executing shard's `ScriptCache`, so at `--shards 4` a bare `EVAL`
/// followed by `EVALSHA` on twelve keys answered `NOSCRIPT` for ten of them:
/// the ones whose key routed to a shard that had never seen the body.
/// `SCRIPT LOAD` already fanned out, which is why `register_script`-based
/// clients (`redis.lock.Lock`) never hit this.
///
/// Published once per distinct body rather than per call: N-1 SPSC pushes and
/// a cross-shard round trip on every `EVAL` would be a real cost on the
/// scripting path. The duty is claimed by `ScriptCache::claim_fanout_duty` and
/// cleared only on a COMPLETE publish, so a fan-out that fails is retried by
/// the next `EVAL` of the same body instead of being written off.
///
/// A partial publish does NOT fail the `EVAL`. The script itself runs fine —
/// only a later `EVALSHA` routed to the shard that missed the body is
/// affected, and `NOSCRIPT` is precisely the error every client library
/// already handles by re-sending the full `EVAL` (which republishes). Failing
/// the `EVAL` would trade a self-healing degradation for an outage.
///
/// Called BEFORE routing so the ordering is well defined: the fan-out and the
/// routed execution travel the same SPSC ring in that order, and the fan-out
/// is acked before the routed call is even sent.
pub(crate) async fn eval_script_fanout(
    ctx: &super::core::ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    cmd_args: &[Frame],
) {
    if ctx.num_shards <= 1 {
        return;
    }
    // Nothing is published for a malformed EVAL: `parse_eval_args` is the same
    // gate the local handler applies, so a body that is about to be rejected
    // for a missing `numkeys` never reaches another shard's cache.
    if crate::scripting::parse_eval_args(cmd_args).is_err() {
        return;
    }
    let Some(Frame::BulkString(script)) = cmd_args.first() else {
        return;
    };
    // Borrow scoped tight: never held across the fan-out's awaits. The claim
    // is this shard's copy of the insert; the fan-out carries the same flush
    // epoch, so all copies are ONE insert in the agreed order (moon#1235).
    let (sha1, owed, epoch) = {
        let mut cache = ctx.script_cache.borrow_mut();
        cache.claim_fanout_duty(script.clone())
    };
    if !owed {
        return;
    }
    if script_fanout_bounded(ctx, shutdown, script, epoch).await == FanoutOutcome::Complete {
        ctx.script_cache.borrow_mut().mark_fanned_out(&sha1, epoch);
    }
}

/// Replay a `FUNCTION LOAD`/`DELETE`/`FLUSH` on every other shard (moon#514).
///
/// `FUNCTION` is a server-wide verb in Redis; in moon each shard thread owns
/// its own registry, so without this a library loaded through one connection
/// was invisible to `FCALL` on every other shard — `ERR Function not found`
/// for whichever keys did not happen to land on the loading shard.
///
/// `DELETE`/`FLUSH` fan out for the same reason in reverse: a delete that only
/// reached one shard leaves the library callable elsewhere, which is a worse
/// lie than never having deleted it.
///
/// Returns the reply the client should get INSTEAD of the local one when the
/// replay did not reach every shard. `FUNCTION` is administrative and its ops
/// are idempotent under retry (`DELETE`/`FLUSH` outright, `LOAD` when the
/// client re-sends it with `REPLACE`), so reporting the partial state is
/// strictly better than `+OK` over a registry that answers differently
/// depending on which shard a key lands on.
///
/// # Order
///
/// Each origin shard replays on its own set of rings, so two registry
/// mutations issued CONCURRENTLY from connections on different shards used to
/// apply in different orders on different shards (moon#1235: a `FUNCTION
/// LOAD` racing a `FUNCTION FLUSH` left the library on some shards only, both
/// clients told `+OK`). Callers now hold the process-wide function-order token
/// ([`crate::scripting::order::acquire_function_order`]) from before their
/// local apply until this returns — see [`run_function_command`] — so the
/// mutations form one sequence and every shard applies it in that order.
#[must_use]
pub(crate) async fn function_registry_fanout(
    ctx: &super::core::ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    op: crate::scripting::FunctionRegistryOp,
) -> Option<Frame> {
    if ctx.num_shards <= 1 {
        return None;
    }
    let verb = match op {
        crate::scripting::FunctionRegistryOp::Load { .. } => "LOAD",
        crate::scripting::FunctionRegistryOp::Delete { .. } => "DELETE",
        crate::scripting::FunctionRegistryOp::Flush => "FLUSH",
    };
    match fanout_to_other_shards(ctx, shutdown, "function_op", |ack| {
        crate::shard::dispatch::ShardMessage::FunctionRegistry {
            op: op.clone(),
            ack: Some(ack),
        }
    })
    .await
    {
        FanoutOutcome::Complete => None,
        FanoutOutcome::Partial { reached, targets } => Some(Frame::Error(Bytes::from(format!(
            "MOONERR partialfanout FUNCTION {verb} applied on {} of {} shards; re-issue it \
             (LOAD needs REPLACE) to converge",
            reached + 1,
            targets + 1,
        )))),
    }
}

/// Whether `FUNCTION <sub> …` would mutate the registry, decided from the
/// subcommand NAME before anything runs — the order token must be taken
/// before the local apply, not after it (moon#1235).
fn is_function_mutation(cmd_args: &[Frame]) -> bool {
    matches!(
        cmd_args.first(),
        Some(Frame::BulkString(sub))
            if sub.eq_ignore_ascii_case(b"LOAD")
                || sub.eq_ignore_ascii_case(b"DELETE")
                || sub.eq_ignore_ascii_case(b"FLUSH")
    )
}

/// Run one `FUNCTION …` command for a connection: apply it to this shard's
/// registry and, for an accepted `LOAD`/`DELETE`/`FLUSH`, replay it on every
/// other shard. Returns the client's reply.
///
/// The one body behind the live paths of both runtimes and the MULTI path, so
/// the ordering rule below cannot be present on one and missing on another.
///
/// At `--shards > 1` a mutation holds the process-wide function-order token
/// from BEFORE the local apply until every other shard has acknowledged the
/// replay (moon#1235). Mutations therefore form one sequence that every shard
/// applies in the same order, and the local validation (`LOAD` of an existing
/// library, a function name another library owns) sees the registry every
/// shard has — redis's answer. The wait for the token is bounded by the same
/// budget as a fan-out; a mutation that cannot get it in time is NOT applied
/// anywhere, and the client is told so rather than having it applied out of
/// order.
pub(crate) async fn run_function_command(
    ctx: &super::core::ConnectionContext,
    shutdown: &crate::runtime::cancel::CancellationToken,
    func_registry: &std::rc::Rc<std::cell::RefCell<Option<crate::scripting::FunctionRegistry>>>,
    cmd_args: &[Frame],
) -> Frame {
    crate::server::conn::core::ensure_function_registry(func_registry, ctx);
    let _order = if ctx.num_shards > 1 && is_function_mutation(cmd_args) {
        match crate::scripting::order::acquire_function_order(fanout_budget(ctx)).await {
            Some(guard) => Some(guard),
            None => {
                tracing::warn!(
                    "shard {}: FUNCTION mutation refused: another FUNCTION mutation held the \
                     order token past the fan-out budget (a shard may be wedged)",
                    ctx.shard_id
                );
                crate::admin::metrics_setup::record_xshard_fanout_drop("function_op");
                return Frame::Error(Bytes::from_static(
                    b"MOONERR partialfanout FUNCTION not applied: another FUNCTION mutation is \
                      still reaching the shards; re-issue it",
                ));
            }
        }
    } else {
        None
    };
    // Borrow scoped to this block and released before the fan-out await: the
    // registry `RefCell` is shared with this shard thread's SPSC drain loop,
    // which applies INBOUND fan-outs — holding it across a yield would make an
    // arriving library land on a borrowed cell and be dropped. A trailing
    // `drop(guard)` is not enough: it satisfies the borrow checker but still
    // trips `await_holding_refcell_ref`.
    let mut response = {
        let mut guard = func_registry.borrow_mut();
        match guard.as_mut() {
            Some(reg) => crate::command::functions::handle_function(reg, cmd_args),
            // `ensure_function_registry` just filled the slot.
            None => Frame::Error(Bytes::from_static(b"ERR function registry unavailable")),
        }
    };
    // A replay that did not reach every shard REPLACES the local reply. The
    // client asked for a server-wide mutation; `+OK` over a registry that
    // answers differently per shard is the defect, not a shape to preserve.
    if let Some(op) = function_fanout_op(cmd_args, &response)
        && let Some(partial) = function_registry_fanout(ctx, shutdown, op).await
    {
        response = partial;
    }
    response
    // `_order` drops here: the next mutation, from any shard, may start.
}

/// The fan-out replay a `FUNCTION` invocation owes the other shards, if any.
///
/// Returns `None` for read-only subcommands (`LIST`, `DUMP`, `STATS`) and for
/// any invocation the local registry rejected — replaying a failed mutation
/// would diverge the very shards it is meant to keep in step.
pub(crate) fn function_fanout_op(
    cmd_args: &[Frame],
    response: &Frame,
) -> Option<crate::scripting::FunctionRegistryOp> {
    if matches!(response, Frame::Error(_)) {
        return None;
    }
    let Some(Frame::BulkString(sub)) = cmd_args.first() else {
        return None;
    };
    if sub.eq_ignore_ascii_case(b"LOAD") {
        // FUNCTION LOAD [REPLACE] <body> — the body is the LAST argument in
        // both arities, and `handle_function_load` has already rejected every
        // other shape by the time we see a non-error response.
        let Some(Frame::BulkString(body)) = cmd_args.last() else {
            return None;
        };
        Some(crate::scripting::FunctionRegistryOp::Load {
            source: body.clone(),
        })
    } else if sub.eq_ignore_ascii_case(b"DELETE") {
        let Some(Frame::BulkString(lib)) = cmd_args.get(1) else {
            return None;
        };
        Some(crate::scripting::FunctionRegistryOp::Delete {
            library: lib.clone(),
        })
    } else if sub.eq_ignore_ascii_case(b"FLUSH") {
        Some(crate::scripting::FunctionRegistryOp::Flush)
    } else {
        None
    }
}
