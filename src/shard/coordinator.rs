//! VLL multi-key coordination for MGET, MSET, DEL, UNLINK, EXISTS.
//!
//! Groups keys by target shard in a BTreeMap (ascending shard-ID order for
//! deadlock prevention -- VLL pattern), dispatches to each shard, collects
//! results, and assembles the final response. Local-shard keys are executed
//! directly without SPSC overhead.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use bytes::Bytes;
use ringbuf::HeapProd;
use ringbuf::traits::Producer;

use crate::command::{DispatchResult, dispatch as cmd_dispatch};
use crate::framevec;
use crate::protocol::Frame;
use crate::runtime::channel;
// Coordinator uses oneshot channels (not ResponseSlotPool) for cross-thread safety.
// ResponseSlotPool's AtomicWaker doesn't work with monoio's !Send executor.
// The oneshot overhead (~80ns) is negligible on the multi-key coordination path.
use crate::shard::dispatch::{ShardMessage, key_to_shard};
use crate::shard::mesh::ChannelMesh;
use crate::storage::entry::CachedClock;

use super::shared_databases::ShardDatabases;
/// Coordinate a multi-key command across shards.
///
/// Routes MGET, MSET, DEL (multi), UNLINK (multi), and EXISTS (multi)
/// to the appropriate per-command coordinator.
pub async fn coordinate_multi_key(
    cmd: &[u8],
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    if cmd.eq_ignore_ascii_case(b"MGET") {
        coordinate_mget(
            args,
            my_shard,
            num_shards,
            db_index,
            shard_databases,
            dispatch_tx,
            spsc_notifiers,
            cached_clock,
            _response_pool,
        )
        .await
    } else if cmd.eq_ignore_ascii_case(b"MSET") {
        coordinate_mset(
            args,
            my_shard,
            num_shards,
            db_index,
            shard_databases,
            dispatch_tx,
            spsc_notifiers,
            cached_clock,
            _response_pool,
        )
        .await
    } else if cmd.eq_ignore_ascii_case(b"BITOP") {
        coordinate_bitop(
            args,
            my_shard,
            num_shards,
            db_index,
            shard_databases,
            dispatch_tx,
            spsc_notifiers,
            cached_clock,
            _response_pool,
        )
        .await
    } else if cmd.eq_ignore_ascii_case(b"COPY") {
        coordinate_copy(
            args,
            my_shard,
            num_shards,
            db_index,
            shard_databases,
            dispatch_tx,
            spsc_notifiers,
            cached_clock,
            _response_pool,
        )
        .await
    } else {
        // DEL, UNLINK, EXISTS with multiple keys
        coordinate_multi_del_or_exists(
            cmd,
            args,
            my_shard,
            num_shards,
            db_index,
            shard_databases,
            dispatch_tx,
            spsc_notifiers,
            cached_clock,
            _response_pool,
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// Shared legs for the BITOP / COPY coordinators
// ---------------------------------------------------------------------------

/// Run one full command on the LOCAL shard through the real dispatcher
/// (identical semantics to a remote MultiExecute leg, minus the hop).
fn run_local(
    shard_databases: &Arc<ShardDatabases>,
    db_index: usize,
    cached_clock: &CachedClock,
    cmd: &[u8],
    args: &[Frame],
) -> Frame {
    let db_count = shard_databases.db_count();
    let mut selected = db_index;
    let run = |db: &mut crate::storage::Database| {
        db.refresh_now_from_cache(cached_clock);
        match cmd_dispatch(db, cmd, args, &mut selected, db_count) {
            DispatchResult::Response(f) | DispatchResult::Quit(f) => f,
        }
    };
    crate::shard::slice::with_shard_db(db_index, run)
}

/// Send one full command to a REMOTE shard and await its reply.
async fn run_remote(
    target_shard: usize,
    routing_key: &Bytes,
    command: Frame,
    my_shard: usize,
    db_index: usize,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
) -> Frame {
    // ChannelMesh has no self-send slot — local legs must go via run_local.
    debug_assert_ne!(target_shard, my_shard, "run_remote called for own shard");
    let (reply_tx, reply_rx) = channel::oneshot();
    let msg = ShardMessage::MultiExecute {
        db_index,
        commands: vec![(routing_key.clone(), command)],
        reply_tx,
    };
    spsc_send(dispatch_tx, my_shard, target_shard, msg, spsc_notifiers).await;
    match reply_rx.recv().await {
        Ok(mut frames) if !frames.is_empty() => frames.swap_remove(0),
        _ => Frame::Error(Bytes::from_static(b"ERR cross-shard reply channel closed")),
    }
}

/// Run one full command on whichever shard owns `routing_key`.
#[allow(clippy::too_many_arguments)]
async fn run_on_owner(
    routing_key: &Bytes,
    command_parts: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
) -> Frame {
    let owner = key_to_shard(routing_key, num_shards);
    if owner == my_shard {
        let (cmd, args) = match command_parts.split_first() {
            Some((Frame::BulkString(c), rest)) => (c.clone(), rest),
            _ => return Frame::Error(Bytes::from_static(b"ERR invalid command format")),
        };
        run_local(shard_databases, db_index, cached_clock, &cmd, args)
    } else {
        let command = Frame::Array(command_parts.to_vec().into());
        run_remote(
            owner,
            routing_key,
            command,
            my_shard,
            db_index,
            dispatch_tx,
            spsc_notifiers,
        )
        .await
    }
}

fn bulk(b: &Bytes) -> Frame {
    Frame::BulkString(b.clone())
}

fn bulk_static(s: &'static [u8]) -> Frame {
    Frame::BulkString(Bytes::from_static(s))
}

/// Coordinate BITOP across shards.
///
/// `BITOP <op> dest src [src ...]` — sources are gathered (local read or
/// remote GET), combined via the same `bitop_compute` the local path uses,
/// and the result is written on DEST's owning shard (SET, or DEL when the
/// combine is empty). Full Redis semantics: BITOP is string-only by spec,
/// so value transfer is exact; WRONGTYPE from any source propagates.
#[allow(clippy::too_many_arguments)]
async fn coordinate_bitop(
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    _response_pool: &(),
) -> Frame {
    // Single-shard server: straight to local dispatch — zero coordinator
    // overhead (no key vec, no owner hashing) on the 1-shard hot path.
    if num_shards == 1 {
        return run_local(shard_databases, db_index, cached_clock, b"BITOP", args);
    }
    if args.len() < 3 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'BITOP' command",
        ));
    }
    let (Some(op), Some(dest)) = (extract_key(&args[0]), extract_key(&args[1])) else {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'BITOP' command",
        ));
    };
    // Redis validation order: NOT arity errors before any key is touched.
    if op.eq_ignore_ascii_case(b"NOT") && args.len() != 3 {
        return Frame::Error(Bytes::from_static(
            b"ERR BITOP NOT requires one and only one key",
        ));
    }
    let mut src_keys: Vec<Bytes> = Vec::with_capacity(args.len() - 2);
    for arg in &args[2..] {
        match extract_key(arg) {
            Some(k) => src_keys.push(k),
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR wrong number of arguments for 'BITOP' command",
                ));
            }
        }
    }

    // Single-owner fast path: every key (dest included) on one shard —
    // forward the whole command for byte-identical local semantics.
    let dest_shard = key_to_shard(&dest, num_shards);
    if src_keys
        .iter()
        .all(|k| key_to_shard(k, num_shards) == dest_shard)
    {
        let mut parts: Vec<Frame> = Vec::with_capacity(args.len() + 1);
        parts.push(bulk_static(b"BITOP"));
        parts.extend_from_slice(args);
        return run_on_owner(
            &dest,
            &parts,
            my_shard,
            num_shards,
            db_index,
            shard_databases,
            dispatch_tx,
            spsc_notifiers,
            cached_clock,
        )
        .await;
    }

    // Gather sources: group by shard ascending (VLL), local legs direct,
    // remote legs as GET batches.
    let mut groups: BTreeMap<usize, Vec<(usize, Bytes)>> = BTreeMap::new();
    for (i, k) in src_keys.iter().enumerate() {
        groups
            .entry(key_to_shard(k, num_shards))
            .or_default()
            .push((i, k.clone()));
    }
    let mut sources: Vec<Option<Vec<u8>>> = vec![None; src_keys.len()];
    let mut pending: Vec<(Vec<usize>, channel::OneshotReceiver<Vec<Frame>>)> = Vec::new();
    for (shard_id, indexed) in &groups {
        if *shard_id == my_shard {
            for (idx, key) in indexed {
                let reply = run_local(
                    shard_databases,
                    db_index,
                    cached_clock,
                    b"GET",
                    &[bulk(key)],
                );
                match reply {
                    Frame::BulkString(v) => sources[*idx] = Some(v.to_vec()),
                    Frame::Error(e) => return Frame::Error(e),
                    _ => sources[*idx] = Some(Vec::new()),
                }
            }
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let commands: Vec<(Bytes, Frame)> = indexed
                .iter()
                .map(|(_, k)| {
                    (
                        k.clone(),
                        Frame::Array(framevec![bulk_static(b"GET"), bulk(k)]),
                    )
                })
                .collect();
            let indices: Vec<usize> = indexed.iter().map(|(i, _)| *i).collect();
            let msg = ShardMessage::MultiExecute {
                db_index,
                commands,
                reply_tx,
            };
            spsc_send(dispatch_tx, my_shard, *shard_id, msg, spsc_notifiers).await;
            pending.push((indices, reply_rx));
        }
    }
    for (indices, reply_rx) in pending {
        match reply_rx.recv().await {
            Ok(frames) => {
                for (idx, frame) in indices.into_iter().zip(frames) {
                    match frame {
                        Frame::BulkString(v) => sources[idx] = Some(v.to_vec()),
                        Frame::Error(e) => return Frame::Error(e),
                        _ => sources[idx] = Some(Vec::new()),
                    }
                }
            }
            Err(_) => {
                return Frame::Error(Bytes::from_static(b"ERR cross-shard reply channel closed"));
            }
        }
    }
    let gathered: Vec<Vec<u8>> = sources.into_iter().map(Option::unwrap_or_default).collect();

    match crate::command::string::bitop_compute(&op, &gathered) {
        Err(e) => e,
        Ok(None) => {
            // All sources empty/missing — dest is deleted, reply 0.
            let reply = run_on_owner(
                &dest,
                &[bulk_static(b"DEL"), bulk(&dest)],
                my_shard,
                num_shards,
                db_index,
                shard_databases,
                dispatch_tx,
                spsc_notifiers,
                cached_clock,
            )
            .await;
            if let Frame::Error(e) = reply {
                return Frame::Error(e);
            }
            Frame::Integer(0)
        }
        Ok(Some(result)) => {
            let len = result.len() as i64;
            let reply = run_on_owner(
                &dest,
                &[
                    bulk_static(b"SET"),
                    bulk(&dest),
                    Frame::BulkString(Bytes::from(result)),
                ],
                my_shard,
                num_shards,
                db_index,
                shard_databases,
                dispatch_tx,
                spsc_notifiers,
                cached_clock,
            )
            .await;
            if let Frame::Error(e) = reply {
                return Frame::Error(e);
            }
            Frame::Integer(len)
        }
    }
}

/// Coordinate COPY across shards.
///
/// `COPY src dst [REPLACE]` — same-shard pairs (hash tags) forward to the
/// owning shard and keep full any-type fidelity via the local copy path.
/// Cross-shard pairs transfer STRING values exactly (value + TTL, NX unless
/// REPLACE); cross-shard non-string values return an explicit error instead
/// of silently corrupting (full-fidelity transfer is DUMP/RESTORE territory,
/// tracked in the task backlog). `COPY ... DB n` never reaches this path
/// (excluded in `is_multi_key_command`; the handlers' two-db interception
/// keeps owning it).
#[allow(clippy::too_many_arguments)]
async fn coordinate_copy(
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    _response_pool: &(),
) -> Frame {
    // Single-shard server: straight to local dispatch — zero coordinator
    // overhead on the 1-shard hot path.
    if num_shards == 1 {
        return run_local(shard_databases, db_index, cached_clock, b"COPY", args);
    }
    if args.len() < 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'COPY' command",
        ));
    }
    let (Some(src), Some(dst)) = (extract_key(&args[0]), extract_key(&args[1])) else {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'COPY' command",
        ));
    };
    let mut replace = false;
    for opt in &args[2..] {
        match extract_key(opt) {
            Some(o) if o.eq_ignore_ascii_case(b"REPLACE") => replace = true,
            _ => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
        }
    }

    let src_shard = key_to_shard(&src, num_shards);
    let dst_shard = key_to_shard(&dst, num_shards);

    // Same owner: forward the whole COPY — full any-type fidelity.
    if src_shard == dst_shard {
        let mut parts: Vec<Frame> = Vec::with_capacity(args.len() + 1);
        parts.push(bulk_static(b"COPY"));
        parts.extend_from_slice(args);
        return run_on_owner(
            &src,
            &parts,
            my_shard,
            num_shards,
            db_index,
            shard_databases,
            dispatch_tx,
            spsc_notifiers,
            cached_clock,
        )
        .await;
    }

    // Cross-shard: read value + TTL from src's shard.
    let value = match run_on_owner(
        &src,
        &[bulk_static(b"GET"), bulk(&src)],
        my_shard,
        num_shards,
        db_index,
        shard_databases,
        dispatch_tx,
        spsc_notifiers,
        cached_clock,
    )
    .await
    {
        Frame::BulkString(v) => v,
        Frame::Error(e) if e.starts_with(b"WRONGTYPE") => {
            return Frame::Error(Bytes::from_static(
                b"ERR COPY across shards supports only string values; co-locate the keys with {hash} tags for other types",
            ));
        }
        Frame::Error(e) => return Frame::Error(e),
        _ => return Frame::Integer(0), // src missing
    };
    let ttl_ms = match run_on_owner(
        &src,
        &[bulk_static(b"PTTL"), bulk(&src)],
        my_shard,
        num_shards,
        db_index,
        shard_databases,
        dispatch_tx,
        spsc_notifiers,
        cached_clock,
    )
    .await
    {
        Frame::Integer(t) if t > 0 => Some(t),
        Frame::Integer(-2) => return Frame::Integer(0), // expired between reads
        _ => None,
    };

    // Write to dst's shard: NX unless REPLACE, then restore TTL.
    let set_parts: Vec<Frame> = if replace {
        vec![bulk_static(b"SET"), bulk(&dst), Frame::BulkString(value)]
    } else {
        vec![
            bulk_static(b"SET"),
            bulk(&dst),
            Frame::BulkString(value),
            bulk_static(b"NX"),
        ]
    };
    let set_reply = run_on_owner(
        &dst,
        &set_parts,
        my_shard,
        num_shards,
        db_index,
        shard_databases,
        dispatch_tx,
        spsc_notifiers,
        cached_clock,
    )
    .await;
    match set_reply {
        Frame::SimpleString(_) => {}
        Frame::Error(e) => return Frame::Error(e),
        // Null reply = NX refused (dst exists); anything else is unexpected.
        _ => return Frame::Integer(0),
    }
    if let Some(t) = ttl_ms {
        let mut ttl_buf = itoa::Buffer::new();
        let reply = run_on_owner(
            &dst,
            &[
                bulk_static(b"PEXPIRE"),
                bulk(&dst),
                Frame::BulkString(Bytes::copy_from_slice(ttl_buf.format(t).as_bytes())),
            ],
            my_shard,
            num_shards,
            db_index,
            shard_databases,
            dispatch_tx,
            spsc_notifiers,
            cached_clock,
        )
        .await;
        if let Frame::Error(e) = reply {
            return Frame::Error(e);
        }
    }
    Frame::Integer(1)
}

/// Extract Bytes from a Frame argument.
fn extract_key(frame: &Frame) -> Option<Bytes> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.clone()),
        _ => None,
    }
}

/// Send a ShardMessage via SPSC with spin-retry on full buffer.
///
/// Calls `notify_one()` on the target shard's notifier after successful push
/// for immediate wake (avoids relying on the 1ms periodic timer safety net).
///
/// Exposed at `pub(crate)` so sibling files (e.g. `scatter_aggregate`)
/// can dispatch via the same contention-safe path.
pub(crate) async fn spsc_send(
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    my_shard: usize,
    target_shard: usize,
    msg: ShardMessage,
    spsc_notifiers: &[Arc<channel::Notify>],
) {
    let target_idx = ChannelMesh::target_index(my_shard, target_shard);
    let mut pending = msg;
    loop {
        let push_result = {
            let mut producers = dispatch_tx.borrow_mut();
            producers[target_idx].try_push(pending)
        }; // borrow dropped before yield
        match push_result {
            Ok(()) => {
                spsc_notifiers[target_shard].notify_one();
                return;
            }
            Err(val) => {
                pending = val;
                // Yield to other tasks to avoid busy-spinning on full SPSC buffer.
                #[cfg(feature = "runtime-tokio")]
                tokio::task::yield_now().await;
                #[cfg(feature = "runtime-monoio")]
                monoio::time::sleep(std::time::Duration::from_micros(10)).await;
            }
        }
    }
}

/// Coordinate MGET across shards using VLL pattern.
///
/// Groups keys by shard in a BTreeMap (ascending shard-ID order), executes
/// local keys directly, dispatches remote keys via MultiExecute batches,
/// reassembles results in original order.
async fn coordinate_mget(
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    _shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'mget' command",
        ));
    }

    // Group keys by shard in ascending order (BTreeMap = VLL)
    let mut groups: BTreeMap<usize, Vec<(usize, Bytes)>> = BTreeMap::new();
    for (i, arg) in args.iter().enumerate() {
        if let Some(key) = extract_key(arg) {
            let shard = key_to_shard(&key, num_shards);
            groups.entry(shard).or_default().push((i, key));
        }
    }

    // Fast path: all keys on local shard -- use mget directly
    if groups.len() == 1 && groups.contains_key(&my_shard) {
        return crate::shard::slice::with_shard_db(db_index, |db| {
            db.refresh_now_from_cache(cached_clock);
            crate::command::string::mget(db, args)
        });
    }

    let total = args.len();
    let mut results: Vec<Option<Frame>> = vec![None; total];
    let mut pending_shards: Vec<(Vec<usize>, channel::OneshotReceiver<Vec<Frame>>)> = Vec::new();

    // Iterate in ascending shard-ID order (BTreeMap guarantees this)
    for (shard_id, indexed_keys) in &groups {
        let original_indices: Vec<usize> = indexed_keys.iter().map(|(i, _)| *i).collect();

        if *shard_id == my_shard {
            // Local execution: GET each key directly
            crate::shard::slice::with_shard_db(db_index, |db| {
                db.refresh_now_from_cache(cached_clock);
                for (orig_idx, key) in indexed_keys {
                    let entry = db.get(key);
                    let frame = match entry {
                        Some(e) => match e.value.as_bytes() {
                            Some(v) => Frame::BulkString(Bytes::copy_from_slice(v)),
                            None => Frame::Null,
                        },
                        None => Frame::Null,
                    };
                    results[*orig_idx] = Some(frame);
                }
            });
        } else {
            // Remote dispatch: batch of GET commands via MultiExecuteSlotted
            let (reply_tx, reply_rx) = channel::oneshot();
            let commands: Vec<(Bytes, Frame)> = indexed_keys
                .iter()
                .map(|(_, k)| {
                    let cmd = Frame::Array(framevec![
                        Frame::BulkString(Bytes::from_static(b"GET")),
                        Frame::BulkString(k.clone()),
                    ]);
                    (k.clone(), cmd)
                })
                .collect();
            let msg = ShardMessage::MultiExecute {
                db_index,
                commands,
                reply_tx,
            };
            spsc_send(dispatch_tx, my_shard, *shard_id, msg, spsc_notifiers).await;
            pending_shards.push((original_indices, reply_rx));
        }
    }

    // Await all remote results
    for (indices, reply_rx) in pending_shards {
        match reply_rx.recv().await {
            Ok(frames) => {
                for (idx, frame) in indices.into_iter().zip(frames) {
                    results[idx] = Some(frame);
                }
            }
            Err(_) => {
                // Channel closed — target shard dropped without responding.
                for idx in indices {
                    results[idx] = Some(Frame::Error(Bytes::from_static(
                        b"ERR cross-shard reply channel closed",
                    )));
                }
            }
        }
    }

    // Assemble in original order
    Frame::Array(
        results
            .into_iter()
            .map(|opt| opt.unwrap_or(Frame::Null))
            .collect(),
    )
}

/// Coordinate MSET across shards using VLL pattern.
///
/// Groups key-value pairs by shard in ascending order, dispatches SET
/// sub-commands per shard. Returns OK when all complete.
async fn coordinate_mset(
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    _shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'mset' command",
        ));
    }

    // Group key-value pairs by shard in ascending order (BTreeMap = VLL)
    let mut groups: BTreeMap<usize, Vec<(Bytes, Bytes)>> = BTreeMap::new();
    for pair in args.chunks(2) {
        let key = match extract_key(&pair[0]) {
            Some(k) => k,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR wrong number of arguments for 'mset' command",
                ));
            }
        };
        let value = match extract_key(&pair[1]) {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR wrong number of arguments for 'mset' command",
                ));
            }
        };
        let shard = key_to_shard(&key, num_shards);
        groups.entry(shard).or_default().push((key, value));
    }

    // Fast path: all keys on local shard
    if groups.len() == 1 && groups.contains_key(&my_shard) {
        return crate::shard::slice::with_shard_db(db_index, |db| {
            db.refresh_now_from_cache(cached_clock);
            crate::command::string::mset(db, args)
        });
    }

    let mut pending_shards: Vec<channel::OneshotReceiver<Vec<Frame>>> = Vec::new();

    for (shard_id, kv_pairs) in &groups {
        if *shard_id == my_shard {
            crate::shard::slice::with_shard_db(db_index, |db| {
                db.refresh_now_from_cache(cached_clock);
                for (key, value) in kv_pairs {
                    db.set_string(key.clone(), value.clone());
                }
            });
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let commands: Vec<(Bytes, Frame)> = kv_pairs
                .iter()
                .map(|(k, v)| {
                    let cmd = Frame::Array(framevec![
                        Frame::BulkString(Bytes::from_static(b"SET")),
                        Frame::BulkString(k.clone()),
                        Frame::BulkString(v.clone()),
                    ]);
                    (k.clone(), cmd)
                })
                .collect();
            let msg = ShardMessage::MultiExecute {
                db_index,
                commands,
                reply_tx,
            };
            spsc_send(dispatch_tx, my_shard, *shard_id, msg, spsc_notifiers).await;
            pending_shards.push(reply_rx);
        }
    }

    for reply_rx in pending_shards {
        let _ = reply_rx.recv().await;
    }

    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// Coordinate DEL/UNLINK/EXISTS with multiple keys across shards using VLL pattern.
///
/// Groups keys by shard in ascending order (BTreeMap), dispatches sub-commands
/// per shard via MultiExecute, sums integer results.
async fn coordinate_multi_del_or_exists(
    cmd: &[u8],
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    let cmd_upper = cmd.to_ascii_uppercase();

    // Group keys by shard in ascending order (BTreeMap = VLL)
    let mut groups: BTreeMap<usize, Vec<Frame>> = BTreeMap::new();
    for arg in args {
        if let Some(key) = extract_key(arg) {
            let shard = key_to_shard(&key, num_shards);
            groups.entry(shard).or_default().push(arg.clone());
        }
    }

    // db_count() lives on ShardDatabases — read once, share across both branches.
    let db_count = shard_databases.db_count();

    // Fast path: all keys on local shard
    if groups.len() == 1 && groups.contains_key(&my_shard) {
        let mut selected = db_index;
        let result = crate::shard::slice::with_shard_db(db_index, |db| {
            db.refresh_now_from_cache(cached_clock);
            cmd_dispatch(db, cmd, args, &mut selected, db_count)
        });
        return match result {
            DispatchResult::Response(f) => f,
            DispatchResult::Quit(f) => f,
        };
    }

    let mut total_count: i64 = 0;
    let mut pending_shards: Vec<channel::OneshotReceiver<Vec<Frame>>> = Vec::new();

    for (shard_id, key_args) in &groups {
        if *shard_id == my_shard {
            let mut selected = db_index;
            let result = crate::shard::slice::with_shard_db(db_index, |db| {
                db.refresh_now_from_cache(cached_clock);
                cmd_dispatch(db, cmd, key_args, &mut selected, db_count)
            });
            if let DispatchResult::Response(Frame::Integer(n)) = result {
                total_count += n;
            }
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let commands: Vec<(Bytes, Frame)> = key_args
                .iter()
                .map(|arg| {
                    let key = extract_key(arg).unwrap_or_default();
                    let cmd_frame = Frame::Array(framevec![
                        Frame::BulkString(Bytes::from(cmd_upper.clone())),
                        arg.clone(),
                    ]);
                    (key, cmd_frame)
                })
                .collect();
            let msg = ShardMessage::MultiExecute {
                db_index,
                commands,
                reply_tx,
            };
            spsc_send(dispatch_tx, my_shard, *shard_id, msg, spsc_notifiers).await;
            pending_shards.push(reply_rx);
        }
    }

    for reply_rx in pending_shards {
        match reply_rx.recv().await {
            Ok(frames) => {
                for frame in frames {
                    match frame {
                        Frame::Integer(n) => total_count += n,
                        Frame::Error(_) => return frame,
                        _ => {}
                    }
                }
            }
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR cross-shard reply channel closed during DEL/UNLINK",
                ));
            }
        }
    }

    Frame::Integer(total_count)
}

/// Coordinate KEYS across all shards.
///
/// Dispatches KEYS command to every shard, collects and merges results.
pub async fn coordinate_keys(
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'keys' command",
        ));
    }

    let mut all_keys: Vec<Frame> = Vec::new();
    let mut pending_shards: Vec<channel::OneshotReceiver<Frame>> = Vec::new();

    // Execute locally on this shard
    {
        let db_count = shard_databases.db_count();
        let mut selected = db_index;
        let result = crate::shard::slice::with_shard_db(db_index, |db| {
            db.refresh_now_from_cache(cached_clock);
            cmd_dispatch(db, b"KEYS", args, &mut selected, db_count)
        });
        if let DispatchResult::Response(Frame::Array(keys)) = result {
            all_keys.extend(keys);
        }
    }

    // Dispatch to all remote shards
    for target in 0..num_shards {
        if target == my_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let cmd_frame = {
            let mut parts = vec![Frame::BulkString(Bytes::from_static(b"KEYS"))];
            for a in args {
                parts.push(a.clone());
            }
            Frame::Array(parts.into())
        };
        let msg = ShardMessage::Execute {
            db_index,
            command: std::sync::Arc::new(cmd_frame),
            reply_tx,
        };
        spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await;
        pending_shards.push(reply_rx);
    }

    // Collect remote results
    for reply_rx in pending_shards {
        match reply_rx.recv().await {
            Ok(Frame::Array(keys)) => all_keys.extend(keys),
            Ok(_) => {} // Non-array response (e.g., error) — skip
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR cross-shard reply channel closed during KEYS",
                ));
            }
        }
    }

    Frame::Array(all_keys.into())
}

/// Coordinate SCAN across all shards.
///
/// Cursor encoding: upper 16 bits = shard index, lower 48 bits = per-shard cursor.
/// This allows SCAN to iterate through all shards sequentially.
pub async fn coordinate_scan(
    args: &[Frame],
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    cached_clock: &CachedClock,
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'scan' command",
        ));
    }

    // Parse the composite cursor from the first arg
    let cursor_val: i64 = match &args[0] {
        Frame::BulkString(b) | Frame::SimpleString(b) => std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0),
        Frame::Integer(n) => *n,
        _ => 0,
    };
    let cursor_u64 = cursor_val as u64;

    // Decode: upper 16 bits = shard index, lower 48 bits = per-shard cursor
    let current_shard = ((cursor_u64 >> 48) & 0xFFFF) as usize;
    let shard_cursor = (cursor_u64 & 0x0000_FFFF_FFFF_FFFF) as i64;

    // Determine the target shard (may differ from my_shard)
    let target_shard_id = current_shard.min(num_shards - 1);

    // Build the SCAN command with the per-shard cursor
    let mut scan_args = vec![Frame::BulkString(Bytes::from(shard_cursor.to_string()))];
    // Forward remaining args (COUNT, MATCH, etc.)
    for a in &args[1..] {
        scan_args.push(a.clone());
    }

    // Execute SCAN on the target shard
    let scan_result = if target_shard_id == my_shard {
        let db_count = shard_databases.db_count();
        let mut selected = db_index;
        let result = crate::shard::slice::with_shard_db(db_index, |db| {
            db.refresh_now_from_cache(cached_clock);
            cmd_dispatch(db, b"SCAN", &scan_args, &mut selected, db_count)
        });
        match result {
            DispatchResult::Response(f) => f,
            DispatchResult::Quit(f) => f,
        }
    } else {
        // Remote dispatch
        let (reply_tx, reply_rx) = channel::oneshot();
        let mut parts = vec![Frame::BulkString(Bytes::from_static(b"SCAN"))];
        parts.extend(scan_args);
        let cmd_frame = Frame::Array(parts.into());
        let msg = ShardMessage::Execute {
            db_index,
            command: std::sync::Arc::new(cmd_frame),
            reply_tx,
        };
        spsc_send(dispatch_tx, my_shard, target_shard_id, msg, spsc_notifiers).await;
        match reply_rx.recv().await {
            Ok(frame) => frame,
            Err(_) => Frame::Error(Bytes::from_static(
                b"ERR cross-shard reply channel closed during SCAN",
            )),
        }
    };

    // Parse the SCAN response: [cursor, [keys...]]
    match scan_result {
        Frame::Array(parts) if parts.len() == 2 => {
            let next_shard_cursor: i64 = match &parts[0] {
                Frame::BulkString(b) => std::str::from_utf8(b)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0),
                Frame::Integer(n) => *n,
                _ => 0,
            };

            let keys = parts[1].clone();

            // Compute next composite cursor
            let next_composite = if next_shard_cursor == 0 {
                // This shard is done, move to the next shard
                let next_shard = target_shard_id + 1;
                if next_shard >= num_shards {
                    // All shards done
                    0u64
                } else {
                    // Start of next shard (cursor 0)
                    (next_shard as u64) << 48
                }
            } else {
                // Continue on current shard
                ((target_shard_id as u64) << 48)
                    | (next_shard_cursor as u64 & 0x0000_FFFF_FFFF_FFFF)
            };

            Frame::Array(framevec![
                Frame::BulkString(Bytes::from(next_composite.to_string())),
                keys,
            ])
        }
        other => other,
    }
}

/// Coordinate DBSIZE across all shards.
///
/// Returns the sum of keys across all shards.
pub async fn coordinate_dbsize(
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    _shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    let mut total: i64 = 0;
    let mut pending_shards: Vec<channel::OneshotReceiver<Frame>> = Vec::new();

    // Local shard
    {
        let local_len = crate::shard::slice::with_shard_db(db_index, |db| db.len()) as i64;
        total += local_len;
    }

    // Remote shards
    for target in 0..num_shards {
        if target == my_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let cmd_frame = Frame::Array(framevec![Frame::BulkString(Bytes::from_static(b"DBSIZE"))]);
        let msg = ShardMessage::Execute {
            db_index,
            command: std::sync::Arc::new(cmd_frame),
            reply_tx,
        };
        spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await;
        pending_shards.push(reply_rx);
    }

    for reply_rx in pending_shards {
        match reply_rx.recv().await {
            Ok(Frame::Integer(n)) => total += n,
            Ok(_) => {} // Non-integer response — skip
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR cross-shard reply channel closed during DBSIZE",
                ));
            }
        }
    }

    Frame::Integer(total)
}

/// Coordinate HOTKEYS across all shards: merge per-shard top-K sketches.
///
/// Each key lives on exactly one shard, so the merge never has to sum
/// duplicate keys — it sorts the union by sampled count and truncates.
pub async fn coordinate_hotkeys(
    count: usize,
    my_shard: usize,
    num_shards: usize,
    db_index: usize,
    _shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    _response_pool: &(), // placeholder — coordinator uses oneshot internally
) -> Frame {
    let mut merged: Vec<(Bytes, i64)> = Vec::new();

    // Local shard: read the sketch directly.
    {
        let local = crate::shard::slice::with_shard_db(db_index, |db| db.hot_keys().top(count));
        merged.extend(local.into_iter().map(|(k, c)| (k, c as i64)));
    }

    // Remote shards: synthetic HOTKEYS COUNT <n> executes via normal dispatch.
    let mut count_buf = itoa::Buffer::new();
    let count_bytes = Bytes::copy_from_slice(count_buf.format(count).as_bytes());
    let mut pending_shards: Vec<channel::OneshotReceiver<Frame>> = Vec::new();
    for target in 0..num_shards {
        if target == my_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let cmd_frame = Frame::Array(framevec![
            Frame::BulkString(Bytes::from_static(b"HOTKEYS")),
            Frame::BulkString(Bytes::from_static(b"COUNT")),
            Frame::BulkString(count_bytes.clone()),
        ]);
        let msg = ShardMessage::Execute {
            db_index,
            command: std::sync::Arc::new(cmd_frame),
            reply_tx,
        };
        spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await;
        pending_shards.push(reply_rx);
    }

    for reply_rx in pending_shards {
        match reply_rx.recv().await {
            Ok(Frame::Array(entries)) => {
                for entry in entries.iter() {
                    if let Frame::Array(pair) = entry
                        && let (Some(Frame::BulkString(k)), Some(Frame::Integer(c))) =
                            (pair.first(), pair.get(1))
                    {
                        merged.push((k.clone(), *c));
                    }
                }
            }
            Ok(_) => {} // Error/unexpected reply from one shard — skip its entries
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR cross-shard reply channel closed during HOTKEYS",
                ));
            }
        }
    }

    merged.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    merged.truncate(count);
    let mut out: Vec<Frame> = Vec::with_capacity(merged.len());
    for (key, sampled) in merged {
        out.push(Frame::Array(framevec![
            Frame::BulkString(key),
            Frame::Integer(sampled),
        ]));
    }
    Frame::Array(out.into())
}

/// Scatter a vector search query to all shards, collect per-shard results,
/// and merge into a global top-K response.
///
/// Used when the connection handler receives FT.SEARCH and num_shards > 1.
/// Each shard runs a local search and returns its local top-K. The coordinator
/// merges all per-shard results and returns the globally correct top-K.
///
/// For single-shard deployments, FT.SEARCH executes directly without scatter.
pub async fn scatter_vector_search(
    index_name: Bytes,
    query_blob: Bytes,
    k: usize,
    as_of_lsn: u64,
    my_shard: usize,
    num_shards: usize,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    vector_store: &mut crate::vector::store::VectorStore,
) -> Frame {
    let mut receivers = Vec::with_capacity(num_shards);
    let mut local_result: Option<Frame> = None;

    for shard_id in 0..num_shards {
        if shard_id == my_shard {
            // Execute locally -- avoid SPSC overhead for local shard.
            // Phase 171 SCAT-01: thread as_of_lsn through the local branch so
            // the coordinator honors temporal filtering on its own shard too.
            local_result = Some(crate::command::vector_search::search_local_filtered(
                vector_store,
                &index_name,
                &query_blob,
                k,
                None,
                0,
                usize::MAX,
                None,
                as_of_lsn,
            ));
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let msg =
                ShardMessage::VectorSearch(Box::new(crate::shard::dispatch::VectorSearchPayload {
                    index_name: index_name.clone(),
                    query_blob: query_blob.clone(),
                    k,
                    as_of_lsn,
                    reply_tx,
                }));
            spsc_send(dispatch_tx, my_shard, shard_id, msg, spsc_notifiers).await;
            receivers.push(reply_rx);
        }
    }

    let mut shard_responses = Vec::with_capacity(num_shards);
    if let Some(local) = local_result {
        shard_responses.push(local);
    }
    for rx in receivers {
        match rx.recv().await {
            Ok(frame) => shard_responses.push(frame),
            Err(_) => {
                return Frame::Error(bytes::Bytes::from_static(
                    b"ERR shard reply channel closed during vector search scatter-gather",
                ));
            }
        }
    }

    crate::command::vector_search::merge_search_results(&shard_responses, k, 0, usize::MAX)
}

/// Scatter FT.SEARCH to all shards via SPSC (no local vector_store needed).
///
/// Used by connection handlers that don't have direct vector_store access.
/// Sends VectorSearch to every shard (including local) via SPSC, collects
/// results, and merges into a global top-K response.
/// Scatter FT.SEARCH to all shards (local + remote), merge top-K results.
///
/// Local shard: direct VectorStore access via shard_databases (no SPSC self-send).
/// Remote shards: SPSC dispatch with VectorSearch message.
/// Single-shard (num_shards == 1): local-only, no SPSC needed.
pub async fn scatter_vector_search_remote(
    index_name: Bytes,
    query_blob: Bytes,
    k: usize,
    as_of_lsn: u64,
    my_shard: usize,
    num_shards: usize,
    shard_databases: &Arc<crate::shard::shared_databases::ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
) -> Frame {
    let _ = shard_databases; // E2 removes
    // LOCAL: direct vector store access (avoids SPSC self-send).
    // Phase 171 SCAT-01: honor AS_OF on the coordinator's own shard by
    // routing through `search_local_filtered` with the resolved LSN rather
    // than the AS_OF-unaware `search_local` helper.
    let local_result = crate::shard::slice::with_shard(|s| {
        crate::command::vector_search::search_local_filtered(
            &mut s.vector_store,
            &index_name,
            &query_blob,
            k,
            None,
            0,
            usize::MAX,
            None,
            as_of_lsn,
        )
    });

    // REMOTE: SPSC to all other shards
    let mut receivers = Vec::with_capacity(num_shards.saturating_sub(1));
    for shard_id in 0..num_shards {
        if shard_id == my_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let msg =
            ShardMessage::VectorSearch(Box::new(crate::shard::dispatch::VectorSearchPayload {
                index_name: index_name.clone(),
                query_blob: query_blob.clone(),
                k,
                as_of_lsn,
                reply_tx,
            }));
        spsc_send(dispatch_tx, my_shard, shard_id, msg, spsc_notifiers).await;
        receivers.push(reply_rx);
    }

    let mut shard_responses = Vec::with_capacity(num_shards);
    shard_responses.push(local_result);
    for rx in receivers {
        match rx.recv().await {
            Ok(frame) => shard_responses.push(frame),
            Err(_) => {
                return Frame::Error(bytes::Bytes::from_static(
                    b"ERR shard reply channel closed during vector search scatter-gather",
                ));
            }
        }
    }

    crate::command::vector_search::merge_search_results(&shard_responses, k, 0, usize::MAX)
}

/// Broadcast an FT.* command (FT.CREATE, FT.DROPINDEX) to ALL shards.
///
/// Each shard creates its own copy of the index so HSET auto-indexing works
/// regardless of which shard the key routes to.
///
/// Local shard: direct VectorStore access via shard_databases.
/// Remote shards: SPSC dispatch with VectorCommand message.
/// Single-shard (num_shards == 1): local-only, no SPSC needed.
pub async fn broadcast_vector_command(
    command: std::sync::Arc<Frame>,
    my_shard: usize,
    num_shards: usize,
    shard_databases: &Arc<crate::shard::shared_databases::ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
) -> Frame {
    let _ = shard_databases; // E2 removes
    // REMOTE FIRST: send to all other shards via SPSC before local mutation.
    // This ensures we detect remote failures before committing locally,
    // avoiding partial index metadata across the cluster.
    let mut receivers = Vec::with_capacity(num_shards.saturating_sub(1));
    for target in 0..num_shards {
        if target == my_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let msg = ShardMessage::VectorCommand {
            command: command.clone(),
            reply_tx,
        };
        spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await;
        receivers.push(reply_rx);
    }

    // Collect remote results — fail if any shard errors or disconnects
    for rx in receivers {
        match rx.recv().await {
            Ok(Frame::Error(e)) => return Frame::Error(e),
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR vector command failed: cross-shard reply channel closed",
                ));
            }
            _ => {}
        }
    }

    // LOCAL: execute only after all remote shards succeeded
    // FT.DROPINDEX with DD flag needs Database to delete indexed docs.
    let is_dropindex = match command.as_ref() {
        Frame::Array(arr) if !arr.is_empty() => {
            matches!(&arr[0], Frame::BulkString(b) if b.eq_ignore_ascii_case(b"FT.DROPINDEX"))
        }
        _ => false,
    };

    // Split borrows so rustc sees `&mut s.vector_store`,
    // `&mut s.text_store`, `&s.graph_store`, and optional
    // `&mut s.databases[0]` as four disjoint fields.
    let local_result = crate::shard::slice::with_shard(|s| {
        let (db_slice, vs, ts);
        #[cfg(feature = "graph")]
        let graph_ref;
        {
            vs = &mut s.vector_store;
            ts = &mut s.text_store;
            #[cfg(feature = "graph")]
            {
                graph_ref = &s.graph_store;
            }
            db_slice = &mut s.databases;
        }
        let db_opt = if is_dropindex {
            db_slice.get_mut(0)
        } else {
            None
        };
        crate::shard::spsc_handler::dispatch_vector_command(
            vs,
            ts,
            #[cfg(feature = "graph")]
            Some(graph_ref),
            &command,
            db_opt,
        )
    });
    local_result
}

/// Scatter `FT.INVALIDATE_RANGE` to all shards and return the summed deleted-document count.
///
/// Unlike `broadcast_vector_command` (which returns the first non-error response),
/// this helper sends the command to every shard, collects each shard's `Frame::Integer`
/// count, and returns `Frame::Integer(sum)`.  If any shard returns an error the error
/// is propagated immediately (same early-exit semantics as `broadcast_vector_command`).
///
/// # Lock safety
/// All per-shard local execution is synchronous (no `.await` inside the local block),
/// so no `MutexGuard` is held across an `.await` point.
#[cfg(feature = "text-index")]
pub async fn scatter_invalidate_range(
    command: std::sync::Arc<Frame>,
    my_shard: usize,
    num_shards: usize,
    shard_databases: &Arc<crate::shard::shared_databases::ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
) -> Frame {
    let _ = shard_databases; // E2 removes
    // PARTIAL-STATE: sends fire to all remotes in parallel, then we collect
    // sequentially. If any remote returns an error after others already
    // applied their deletes, this call returns Frame::Error but the
    // successful remotes have already advanced their text_version_token
    // and removed matching docs — there is no cross-shard rollback.
    // Lunaris must treat error replies as "retry the whole invalidation"
    // rather than "no work was done."
    let mut receivers = Vec::with_capacity(num_shards.saturating_sub(1));
    for target in 0..num_shards {
        if target == my_shard {
            continue;
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let msg = ShardMessage::VectorCommand {
            command: command.clone(),
            reply_tx,
        };
        spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await;
        receivers.push(reply_rx);
    }

    // Collect remote counts — fail on any error (see PARTIAL-STATE above).
    let mut total: i64 = 0;
    for rx in receivers {
        match rx.recv().await {
            Ok(Frame::Integer(n)) => total = total.saturating_add(n),
            Ok(Frame::Error(e)) => return Frame::Error(e),
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR FT.INVALIDATE_RANGE: cross-shard reply channel closed",
                ));
            }
            Ok(other) => {
                // Unexpected response type — surface for debugging.
                let _ = other;
                return Frame::Error(Bytes::from_static(
                    b"ERR FT.INVALIDATE_RANGE: unexpected response from remote shard",
                ));
            }
        }
    }

    // Execute locally and add to total.
    let local = crate::shard::slice::with_shard(|s| {
        crate::shard::spsc_handler::dispatch_vector_command(
            &mut s.vector_store,
            &mut s.text_store,
            #[cfg(feature = "graph")]
            Some(&s.graph_store),
            &command,
            None,
        )
    });

    match local {
        Frame::Integer(n) => Frame::Integer(total.saturating_add(n)),
        Frame::Error(e) => Frame::Error(e),
        other => {
            let _ = other;
            Frame::Error(Bytes::from_static(
                b"ERR FT.INVALIDATE_RANGE: unexpected local response",
            ))
        }
    }
}

/// Two-phase DFS scatter-gather for globally accurate BM25 text search (per D-04).
///
/// **Phase 1** — DocFreq scatter: collect (term, df) + total N from every shard,
/// aggregate into global document frequency statistics.
///
/// **Phase 2** — TextSearch scatter: execute BM25 search on every shard using the
/// injected global IDF weights, then merge per-shard top-K results.
///
/// **Single-shard fast path** (per D-06): when `num_shards == 1`, the local shard's
/// FieldStats are globally accurate, so the DFS pre-pass is skipped entirely.
///
/// # Lock safety
/// Every access to `shard_databases.text_store(shard_id)` returns a `MutexGuard`.
/// All local data extraction is wrapped in a block scope so the guard is dropped
/// **before** any `.await` point — required by RESEARCH Pitfall 2.
pub async fn scatter_text_search(
    index_name: Bytes,
    query: Bytes,
    top_k: usize,
    offset: usize,
    count: usize,
    my_shard: usize,
    num_shards: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
    highlight_opts: Option<crate::command::vector_search::HighlightOpts>,
    summarize_opts: Option<crate::command::vector_search::SummarizeOpts>,
) -> Frame {
    let _ = shard_databases; // E2 removes

    // ── Parse once for Phase-1 df terms + highlight terms (fts-query-eval-dispatch 2b) ──
    // Parse inside a with_shard block so we have the index schema, then move owned
    // values out. with_shard releases the borrow before any .await.
    #[cfg(feature = "text-index")]
    let (field_queries, term_strings) = {
        use crate::text::query::{QuerySchema, collect_df_field_terms, collect_highlight_terms};
        let parse_result: Result<
            (Vec<(Option<usize>, Vec<String>)>, Vec<String>),
            crate::protocol::Frame,
        > = crate::shard::slice::with_shard(|s| {
            match s.text_store.get_index(&index_name) {
                None => Err(Frame::Error(Bytes::from_static(b"ERR no such index"))),
                Some(text_index) => {
                    let schema = QuerySchema::from_index(text_index);
                    match crate::text::query::parse_query(&query, &schema) {
                        Err(e) => Err(Frame::Error(Bytes::copy_from_slice(e.code().as_bytes()))),
                        Ok(node) => {
                            let fq = collect_df_field_terms(&node, text_index);
                            let ts = collect_highlight_terms(&node, text_index);
                            Ok((fq, ts))
                        }
                    }
                }
            }
        });
        match parse_result {
            Err(err_frame) => return err_frame,
            Ok(pair) => pair,
        }
    };
    #[cfg(not(feature = "text-index"))]
    let (field_queries, _term_strings): (Vec<(Option<usize>, Vec<String>)>, Vec<String>) =
        (Vec::new(), Vec::new());

    // ── Single-shard fast path (per D-06) ────────────────────────────────────
    if num_shards == 1 {
        // Local IDF is globally accurate with one shard — skip DFS pre-pass.
        // Use run_text_query_on_index with no global IDF (single-shard path).
        //
        // text_store + databases[0] accessed simultaneously via a single
        // `with_shard` call to avoid a reentrant `with_shard*` panic.
        let result = crate::shard::slice::with_shard(|s| {
            let ts = &s.text_store;
            let text_index = match ts.get_index(&index_name) {
                Some(idx) => idx,
                None => return Frame::Error(Bytes::from_static(b"ERR no such index")),
            };
            #[cfg(feature = "text-index")]
            {
                let mut r = crate::command::vector_search::ft_text_search::run_text_query_on_index(
                    text_index,
                    &query,
                    None,
                    None,
                    top_k,
                    offset,
                    count,
                );
                if highlight_opts.is_some() || summarize_opts.is_some() {
                    // databases[0] borrowed disjointly from text_store — both live on `s`.
                    if let Some(db) = s.databases.get_mut(0) {
                        crate::command::vector_search::ft_text_search::apply_post_processing(
                            &mut r,
                            &term_strings,
                            text_index,
                            db,
                            highlight_opts.as_ref(),
                            summarize_opts.as_ref(),
                        );
                    }
                }
                r
            }
            #[cfg(not(feature = "text-index"))]
            {
                let _ = text_index;
                Frame::Error(Bytes::from_static(b"ERR text-index feature not enabled"))
            }
        });
        return result;
    }

    // ── Phase 1: scatter DocFreq to all shards ────────────────────────────────
    // Collect (term, df, N) from each shard to build global IDF weights.
    // field_queries comes from collect_df_field_terms (above); shape unchanged.
    let mut doc_freq_receivers: Vec<crate::runtime::channel::OneshotReceiver<Frame>> =
        Vec::with_capacity(num_shards.saturating_sub(1));
    let mut local_doc_freq: Option<Frame> = None;

    for shard_id in 0..num_shards {
        if shard_id == my_shard {
            // Local: extract df/N directly — no SPSC overhead.
            // Shard slice released before any .await.
            let response =
                crate::shard::slice::with_shard(|s| match s.text_store.get_index(&index_name) {
                    Some(text_index) => {
                        let mut items: Vec<Frame> = Vec::new();
                        for (field_idx_opt, terms) in &field_queries {
                            let fidx = field_idx_opt.unwrap_or(0);
                            let (term_dfs, n) = text_index.doc_freq_for_terms(fidx, terms);
                            for (term, df) in term_dfs {
                                items.push(Frame::BulkString(Bytes::from(term)));
                                items.push(Frame::Integer(i64::from(df)));
                            }
                            items.push(Frame::BulkString(Bytes::from_static(b"N")));
                            items.push(Frame::Integer(i64::from(n)));
                        }
                        Frame::Array(items.into())
                    }
                    None => Frame::Error(Bytes::from_static(b"ERR unknown index")),
                });
            local_doc_freq = Some(response);
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let msg = ShardMessage::DocFreq {
                index_name: index_name.clone(),
                field_queries: field_queries.clone(),
                reply_tx,
            };
            spsc_send(dispatch_tx, my_shard, shard_id, msg, spsc_notifiers).await;
            doc_freq_receivers.push(reply_rx);
        }
    }

    // Collect Phase 1 responses and aggregate.
    let mut doc_freq_responses = Vec::with_capacity(num_shards);
    if let Some(local) = local_doc_freq {
        doc_freq_responses.push(local);
    }
    for rx in doc_freq_receivers {
        match rx.recv().await {
            Ok(frame) => doc_freq_responses.push(frame),
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR DFS phase 1 channel closed unexpectedly",
                ));
            }
        }
    }

    let (global_df, global_n) = aggregate_doc_freq(&doc_freq_responses);

    // ── Phase 2: scatter TextSearch with global IDF to all shards ─────────────
    let mut search_receivers: Vec<crate::runtime::channel::OneshotReceiver<Frame>> =
        Vec::with_capacity(num_shards.saturating_sub(1));
    let mut local_search: Option<Frame> = None;

    for shard_id in 0..num_shards {
        if shard_id == my_shard {
            // Local: execute with global IDF via run_text_query_on_index.
            // text_store + databases[0] folded into a single `with_shard` to
            // avoid reentrant `with_shard*` panic. Slice released before .await.
            let response = crate::shard::slice::with_shard(|s| {
                match s.text_store.get_index(&index_name) {
                    Some(text_index) => {
                        #[cfg(feature = "text-index")]
                        {
                            let mut r = crate::command::vector_search::ft_text_search::run_text_query_on_index(
                                text_index,
                                &query,
                                Some(&global_df),
                                Some(global_n),
                                top_k,
                                0,      // each shard returns top_k; coordinator applies final offset
                                top_k,
                            );
                            if highlight_opts.is_some() || summarize_opts.is_some() {
                                if let Some(db) = s.databases.get_mut(0) {
                                    crate::command::vector_search::ft_text_search::apply_post_processing(
                                        &mut r,
                                        &term_strings,
                                        text_index,
                                        db,
                                        highlight_opts.as_ref(),
                                        summarize_opts.as_ref(),
                                    );
                                }
                            }
                            r
                        }
                        #[cfg(not(feature = "text-index"))]
                        {
                            let _ = text_index;
                            Frame::Error(Bytes::from_static(b"ERR text-index feature not enabled"))
                        }
                    }
                    None => Frame::Error(Bytes::from_static(b"ERR unknown index")),
                }
            });
            local_search = Some(response);
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let msg =
                ShardMessage::TextSearch(Box::new(crate::shard::dispatch::TextSearchPayload {
                    index_name: index_name.clone(),
                    // Send raw query bytes; each remote shard re-parses with the full AST.
                    query: query.clone(),
                    global_df: global_df.clone(),
                    global_n,
                    top_k,
                    offset: 0, // each shard returns top_k; coordinator applies final offset+count
                    count: top_k,
                    // Pass opts to each remote shard — each applies post-processing locally.
                    highlight_opts: highlight_opts.clone(),
                    summarize_opts: summarize_opts.clone(),
                    reply_tx,
                }));
            spsc_send(dispatch_tx, my_shard, shard_id, msg, spsc_notifiers).await;
            search_receivers.push(reply_rx);
        }
    }

    // Collect Phase 2 responses.
    let mut search_responses = Vec::with_capacity(num_shards);
    if let Some(local) = local_search {
        search_responses.push(local);
    }
    for rx in search_receivers {
        match rx.recv().await {
            Ok(frame) => search_responses.push(frame),
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR DFS phase 2 channel closed unexpectedly",
                ));
            }
        }
    }

    // Merge and apply final pagination.
    crate::command::vector_search::merge_text_results(&search_responses, top_k, offset, count)
}

/// Scatter a FieldFilter (TAG — Plan 07 adds NumericRange) across all shards.
///
/// Plan 152-06 (B-02): mirrors `scatter_text_search` for FieldFilter clauses
/// but:
/// - Skips the DFS pre-pass (FieldFilter has no per-term IDF).
/// - Dispatches `ShardMessage::InvertedSearch` instead of `TextSearch`.
/// - Merges response frames via `merge_text_results` — results arrive with
///   `score=0.0`, `merge_text_results` preserves insertion order within its
///   bucket so the per-shard doc_id ascending order becomes a consistent
///   cross-shard ordering after the sort-by-score tie-break (score is
///   uniform, so the secondary key — key-bytes — resolves deterministically).
///
/// # Lock safety
/// Single-shard fast path scopes the `MutexGuard` inside a block so it
/// drops before any `.await` (RESEARCH Pitfall 2).
#[cfg(feature = "text-index")]
#[allow(clippy::too_many_arguments)]
pub async fn scatter_text_search_filter(
    index_name: Bytes,
    filter: crate::command::vector_search::ft_text_search::FieldFilter,
    top_k: usize,
    offset: usize,
    count: usize,
    my_shard: usize,
    num_shards: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
) -> Frame {
    let _ = shard_databases; // E2 removes
    // ── Single-shard fast path ────────────────────────────────────────────────
    if num_shards == 1 {
        let response =
            crate::shard::slice::with_shard(|s| match s.text_store.get_index(&index_name) {
                None => Frame::Error(Bytes::from_static(b"ERR no such index")),
                Some(text_index) => {
                    let clause = crate::command::vector_search::ft_text_search::TextQueryClause {
                        field_name: None,
                        terms: Vec::new(),
                        filter: Some(filter),
                    };
                    let results =
                        crate::command::vector_search::ft_text_search::execute_query_on_index(
                            text_index, &clause, None, None, top_k,
                        );
                    crate::command::vector_search::ft_text_search::build_text_response(
                        &results, offset, count,
                    )
                }
            });
        return response;
    }

    // ── Multi-shard fan-out ──────────────────────────────────────────────────
    let mut receivers: Vec<crate::runtime::channel::OneshotReceiver<Frame>> =
        Vec::with_capacity(num_shards.saturating_sub(1));
    let mut local_response: Option<Frame> = None;

    for shard_id in 0..num_shards {
        if shard_id == my_shard {
            let response =
                crate::shard::slice::with_shard(|s| match s.text_store.get_index(&index_name) {
                    None => Frame::Error(Bytes::from_static(b"ERR no such index")),
                    Some(text_index) => {
                        let clause =
                            crate::command::vector_search::ft_text_search::TextQueryClause {
                                field_name: None,
                                terms: Vec::new(),
                                filter: Some(filter.clone()),
                            };
                        let results =
                            crate::command::vector_search::ft_text_search::execute_query_on_index(
                                text_index, &clause, None, None, top_k,
                            );
                        // Each shard returns top_k; coordinator applies final offset+count
                        // after merging.
                        crate::command::vector_search::ft_text_search::build_text_response(
                            &results, 0, top_k,
                        )
                    }
                });
            local_response = Some(response);
        } else {
            let (reply_tx, reply_rx) = channel::oneshot();
            let msg = ShardMessage::InvertedSearch(Box::new(
                crate::shard::dispatch::InvertedSearchPayload {
                    index_name: index_name.clone(),
                    filter: filter.clone(),
                    top_k,
                    offset: 0,
                    count: top_k,
                    reply_tx,
                },
            ));
            spsc_send(dispatch_tx, my_shard, shard_id, msg, spsc_notifiers).await;
            receivers.push(reply_rx);
        }
    }

    let mut responses = Vec::with_capacity(num_shards);
    if let Some(local) = local_response {
        responses.push(local);
    }
    for rx in receivers {
        match rx.recv().await {
            Ok(frame) => responses.push(frame),
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR InvertedSearch channel closed unexpectedly",
                ));
            }
        }
    }

    // Uniform score=0.0 across shards; merge_text_results stabilizes order.
    crate::command::vector_search::merge_text_results(&responses, top_k, offset, count)
}

/// Aggregate document frequencies from multiple shard `DocFreq` responses.
///
/// Each response is a `Frame::Array` with interleaved `[term, df, ..., "N", n]` entries.
/// This function sums `df` per term across shards and sums `N` (total docs) across shards.
///
/// Returns `(global_df: HashMap<String, u32>, global_n: u32)`.
pub(crate) fn aggregate_doc_freq(
    responses: &[Frame],
) -> (std::collections::HashMap<String, u32>, u32) {
    let mut global_df: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    let mut global_n: u32 = 0;

    for resp in responses {
        let items = match resp {
            Frame::Array(items) => items,
            _ => continue, // Skip error frames from shards that don't have the index
        };

        let mut i = 0;
        while i + 1 < items.len() {
            match &items[i] {
                Frame::BulkString(key) => {
                    if key.as_ref() == b"N" {
                        // "N" sentinel: next item is the total doc count for this shard
                        if let Frame::Integer(n) = &items[i + 1] {
                            global_n = global_n.saturating_add(*n as u32);
                        }
                        i += 2;
                    } else {
                        // term -> df pair
                        let term = match std::str::from_utf8(key) {
                            Ok(s) => s.to_owned(),
                            Err(_) => {
                                i += 2;
                                continue;
                            }
                        };
                        if let Frame::Integer(df) = &items[i + 1] {
                            *global_df.entry(term).or_insert(0) = global_df
                                .get(&term)
                                .copied()
                                .unwrap_or(0)
                                .saturating_add(*df as u32);
                        }
                        i += 2;
                    }
                }
                _ => {
                    i += 1;
                }
            }
        }
    }

    (global_df, global_n)
}

/// Broadcast SWAPDB to all shards and await acknowledgement from each.
///
/// # Flow
///
/// - Local shard: inline swap under ascending-index write locks (no SPSC round-trip).
/// - Remote shards: send `ShardMessage::SwapDb` and collect oneshot replies.
///
/// All-shard acks are awaited before returning `+OK`.  Between the first and
/// last ack a brief window exists where a cross-shard GET may observe the
/// pre-swap state on one shard and post-swap on another.  This matches Redis
/// cluster relaxed semantics and is documented as the "brief-skew" acceptance.
///
/// # Consistency note
///
/// WAL is emitted by each shard's SPSC handler *before* performing the swap,
/// so crash-recovery replay applies them in the correct order.
pub async fn coordinate_swapdb(
    a: usize,
    b: usize,
    my_shard: usize,
    num_shards: usize,
    shard_databases: &Arc<ShardDatabases>,
    dispatch_tx: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    spsc_notifiers: &[Arc<channel::Notify>],
) -> Frame {
    // ChannelMesh has no self-send slot (target_index panics when my_id == target_id).
    // Skip self in the SPSC loop; handle the local shard inline below.
    let remote_count = num_shards.saturating_sub(1);
    let mut receivers: Vec<channel::OneshotReceiver<()>> = Vec::with_capacity(remote_count);

    for target in 0..num_shards {
        if target == my_shard {
            continue; // handled inline below
        }
        let (reply_tx, reply_rx) = channel::oneshot();
        let msg = ShardMessage::SwapDb { a, b, reply_tx };
        spsc_send(dispatch_tx, my_shard, target, msg, spsc_notifiers).await;
        receivers.push(reply_rx);
    }

    // Local shard: emit WAL via the per-shard append channel, then swap databases.
    // This mirrors the spsc_handler SwapDb arm — WAL record BEFORE the swap.
    // SWAPDB has no command-level rollback; if persistence is configured and
    // the WAL channel rejects the enqueue (full / closed), we MUST NOT perform
    // the local swap, otherwise the cluster state diverges from the on-disk log.
    {
        let mut a_buf = itoa::Buffer::new();
        let mut b_buf = itoa::Buffer::new();
        let wal_frame = Frame::Array(framevec![
            Frame::BulkString(Bytes::from_static(b"SWAPDB")),
            Frame::BulkString(Bytes::copy_from_slice(a_buf.format(a).as_bytes())),
            Frame::BulkString(Bytes::copy_from_slice(b_buf.format(b).as_bytes())),
        ]);
        let serialized = crate::persistence::aof::serialize_command(&wal_frame);
        if !shard_databases.try_wal_append_required(my_shard, serialized) {
            return Frame::Error(bytes::Bytes::from_static(
                b"ERR SWAPDB aborted: WAL enqueue failed (persistence backpressure)",
            ));
        }
        crate::shard::slice::with_shard(|s| {
            if a != b {
                s.databases.swap(a, b);
            }
        });
    }

    // Await all-remote-shard acks before returning +OK.
    for rx in receivers {
        match rx.recv().await {
            Ok(()) => {}
            Err(_) => {
                return Frame::Error(bytes::Bytes::from_static(
                    b"ERR cross-shard reply channel closed during SWAPDB",
                ));
            }
        }
    }

    Frame::SimpleString(bytes::Bytes::from_static(b"OK"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btreemap_ascending_order() {
        // BTreeMap guarantees ascending shard order -- VLL deadlock prevention
        let keys = vec![
            Bytes::from_static(b"key1"),
            Bytes::from_static(b"key2"),
            Bytes::from_static(b"key3"),
            Bytes::from_static(b"key4"),
        ];
        let mut groups: BTreeMap<usize, Vec<Bytes>> = BTreeMap::new();
        for key in &keys {
            let shard = key_to_shard(key, 4);
            groups.entry(shard).or_default().push(key.clone());
        }
        let shard_ids: Vec<usize> = groups.keys().copied().collect();
        for i in 1..shard_ids.len() {
            assert!(
                shard_ids[i] > shard_ids[i - 1],
                "BTreeMap should yield ascending shard IDs"
            );
        }
    }

    #[test]
    fn test_hash_tag_co_location() {
        let keys = vec![
            Bytes::from_static(b"{user}.name"),
            Bytes::from_static(b"{user}.email"),
            Bytes::from_static(b"{user}.age"),
        ];
        let mut shards: std::collections::HashSet<usize> = std::collections::HashSet::new();
        for key in &keys {
            shards.insert(key_to_shard(key, 8));
        }
        assert_eq!(
            shards.len(),
            1,
            "all keys with same hash tag should map to one shard"
        );
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn test_coordinate_mget_all_local() {
        use crate::storage::Database;
        let mut dbs = vec![Database::new()];
        dbs[0].set_string(Bytes::from_static(b"a"), Bytes::from_static(b"1"));
        dbs[0].set_string(Bytes::from_static(b"b"), Bytes::from_static(b"2"));

        let (shard_databases, mut inits) = ShardDatabases::new(vec![dbs]);
        // coordinate_mget uses with_shard_db for local keys; ShardSlice must be initialized.
        crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(
            inits.remove(0),
        ));
        let dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>> =
            Rc::new(RefCell::new(Vec::new()));

        let args = vec![
            Frame::BulkString(Bytes::from_static(b"a")),
            Frame::BulkString(Bytes::from_static(b"b")),
        ];

        // With num_shards=1, all keys are local
        let notifiers: Vec<Arc<channel::Notify>> = Vec::new();
        let cached_clock = CachedClock::new();
        let response_pool = ();
        let result = coordinate_mget(
            &args,
            0,
            1,
            0,
            &shard_databases,
            &dispatch_tx,
            &notifiers,
            &cached_clock,
            &response_pool,
        )
        .await;
        match result {
            Frame::Array(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], Frame::BulkString(Bytes::from_static(b"1")));
                assert_eq!(items[1], Frame::BulkString(Bytes::from_static(b"2")));
            }
            _ => panic!("expected Array response"),
        }
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn test_coordinate_mset_all_local() {
        use crate::storage::Database;
        let dbs = vec![Database::new()];
        let (shard_databases, mut inits) = ShardDatabases::new(vec![dbs]);
        crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(
            inits.remove(0),
        ));
        let dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>> =
            Rc::new(RefCell::new(Vec::new()));

        let args = vec![
            Frame::BulkString(Bytes::from_static(b"x")),
            Frame::BulkString(Bytes::from_static(b"10")),
            Frame::BulkString(Bytes::from_static(b"y")),
            Frame::BulkString(Bytes::from_static(b"20")),
        ];

        let notifiers: Vec<Arc<channel::Notify>> = Vec::new();
        let cached_clock = CachedClock::new();
        let response_pool = ();
        let result = coordinate_mset(
            &args,
            0,
            1,
            0,
            &shard_databases,
            &dispatch_tx,
            &notifiers,
            &cached_clock,
            &response_pool,
        )
        .await;
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));

        // Verify keys were set via ShardSlice path.
        crate::shard::slice::with_shard_db(0, |db| {
            let entry = db.get(b"x");
            assert!(entry.is_some());
        });
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn test_coordinate_del_all_local() {
        use crate::storage::Database;
        let mut dbs = vec![Database::new()];
        dbs[0].set_string(Bytes::from_static(b"a"), Bytes::from_static(b"1"));
        dbs[0].set_string(Bytes::from_static(b"b"), Bytes::from_static(b"2"));
        dbs[0].set_string(Bytes::from_static(b"c"), Bytes::from_static(b"3"));

        let (shard_databases, mut inits) = ShardDatabases::new(vec![dbs]);
        // coordinate_multi_del_or_exists uses with_shard_db for local keys; ShardSlice must be initialized.
        crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(
            inits.remove(0),
        ));
        let dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>> =
            Rc::new(RefCell::new(Vec::new()));

        let args = vec![
            Frame::BulkString(Bytes::from_static(b"a")),
            Frame::BulkString(Bytes::from_static(b"b")),
            Frame::BulkString(Bytes::from_static(b"nonexistent")),
        ];

        let notifiers: Vec<Arc<channel::Notify>> = Vec::new();
        let cached_clock = CachedClock::new();
        let response_pool = ();
        let result = coordinate_multi_del_or_exists(
            b"DEL",
            &args,
            0,
            1,
            0,
            &shard_databases,
            &dispatch_tx,
            &notifiers,
            &cached_clock,
            &response_pool,
        )
        .await;
        assert_eq!(result, Frame::Integer(2)); // a and b deleted, nonexistent = 0
    }

    // ── aggregate_doc_freq tests ───────────────────────────────────────────────

    /// Helper: build a DocFreq response frame from a list of (term, df) pairs + N.
    fn make_doc_freq_frame(term_dfs: &[(&str, u32)], n: u32) -> Frame {
        let mut items: Vec<Frame> = Vec::new();
        for (term, df) in term_dfs {
            items.push(Frame::BulkString(Bytes::copy_from_slice(term.as_bytes())));
            items.push(Frame::Integer(i64::from(*df)));
        }
        items.push(Frame::BulkString(Bytes::from_static(b"N")));
        items.push(Frame::Integer(i64::from(n)));
        Frame::Array(items.into())
    }

    #[test]
    fn test_aggregate_doc_freq_two_shards() {
        // Shard A: "machine" df=3, N=10
        // Shard B: "machine" df=5, N=15
        // Global: "machine" df=8, N=25
        let shard_a = make_doc_freq_frame(&[("machine", 3)], 10);
        let shard_b = make_doc_freq_frame(&[("machine", 5)], 15);

        let (global_df, global_n) = aggregate_doc_freq(&[shard_a, shard_b]);

        assert_eq!(global_n, 25, "global N should be 10+15=25");
        assert_eq!(
            global_df.get("machine").copied(),
            Some(8),
            "global df for 'machine' should be 3+5=8"
        );
    }

    #[test]
    fn test_aggregate_doc_freq_missing_term_on_one_shard() {
        // Shard A: "rare" df=1, N=10
        // Shard B: no "rare" entry, N=8
        // Global: "rare" df=1, N=18
        let shard_a = make_doc_freq_frame(&[("rare", 1)], 10);
        let shard_b = make_doc_freq_frame(&[], 8); // empty term list, just N=8

        let (global_df, global_n) = aggregate_doc_freq(&[shard_a, shard_b]);

        assert_eq!(global_n, 18, "global N should be 10+8=18");
        assert_eq!(
            global_df.get("rare").copied(),
            Some(1),
            "global df for 'rare' should be 1 (only present on shard A)"
        );
    }

    #[test]
    fn test_aggregate_doc_freq_multiple_terms() {
        // Two shards each have two terms
        let shard_a = make_doc_freq_frame(&[("rust", 4), ("async", 2)], 20);
        let shard_b = make_doc_freq_frame(&[("rust", 6), ("async", 3)], 30);

        let (global_df, global_n) = aggregate_doc_freq(&[shard_a, shard_b]);

        assert_eq!(global_n, 50, "global N should be 20+30=50");
        assert_eq!(global_df.get("rust").copied(), Some(10), "rust df=4+6=10");
        assert_eq!(global_df.get("async").copied(), Some(5), "async df=2+3=5");
    }

    #[test]
    fn test_aggregate_doc_freq_error_frame_skipped() {
        // If one shard returns an error (e.g. index not found), it should be skipped.
        let shard_a = make_doc_freq_frame(&[("term", 3)], 10);
        let shard_err = Frame::Error(Bytes::from_static(b"ERR unknown index"));

        let (global_df, global_n) = aggregate_doc_freq(&[shard_a, shard_err]);

        // Error frame should be skipped; only shard_a contributes
        assert_eq!(global_n, 10);
        assert_eq!(global_df.get("term").copied(), Some(3));
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn test_scatter_text_search_single_shard_skips_dfs() {
        // Single-shard (num_shards==1): scatter_text_search must return immediately
        // from execute_text_search_local without sending any DocFreq or TextSearch
        // ShardMessages via SPSC. We verify this by:
        //   1. Passing an empty dispatch_tx (no SPSC channels — would panic if used)
        //   2. Verifying the result is an Array (success format, not a channel error)
        //
        // We use an empty TextStore (no indexes), so the result is "ERR no such index".
        // That's still the correct single-shard path — no channels were touched.
        use crate::storage::Database;

        let dbs = vec![Database::new()];
        let (shard_databases_inner, mut inits) = ShardDatabases::new(vec![dbs]);
        // scatter_text_search calls execute_text_search_local which uses with_shard;
        // ShardSlice must be initialized on this thread.
        crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(
            inits.remove(0),
        ));
        let shard_databases = Arc::new(shard_databases_inner);

        // Empty dispatch_tx — any SPSC send would panic (no channels configured).
        let dispatch_tx: Rc<RefCell<Vec<HeapProd<ShardMessage>>>> =
            Rc::new(RefCell::new(Vec::new()));
        let notifiers: Vec<Arc<channel::Notify>> = Vec::new();

        let result = scatter_text_search(
            Bytes::from_static(b"nonexistent_index"),
            Bytes::from_static(b"machine"), // raw query bytes (fts-query-eval-dispatch 2b)
            10,
            0,
            10,
            0, // my_shard
            1, // num_shards = 1 -> single-shard fast path
            &shard_databases,
            &dispatch_tx,
            &notifiers,
            None, // highlight_opts
            None, // summarize_opts
        )
        .await;

        // Should be "ERR no such index" (single-shard run_text_query_on_index path),
        // NOT a channel error. This proves the DFS pre-pass was skipped entirely.
        match &result {
            Frame::Error(e) => {
                let msg = std::str::from_utf8(e).unwrap_or("");
                assert!(
                    msg.contains("no such index"),
                    "expected 'no such index' error for missing index, got: {}",
                    msg
                );
            }
            other => panic!("expected Error frame, got: {:?}", other),
        }
    }
}
