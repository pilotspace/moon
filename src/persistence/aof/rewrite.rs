//! AOF rewrite / compaction: snapshot generation, drain, and per-shard /
//! single / sharded rewrite paths.
#![allow(unused_imports, unused_variables, unreachable_code, clippy::empty_loop)]

use super::*;

/// Generate synthetic RESP commands from the current database state for AOF rewriting.
///
/// Produces commands for all 5 data types plus PEXPIRE for keys with TTL.
#[allow(dead_code)] // Retained for RESP-only AOF rewrite fallback and testing
pub fn generate_rewrite_commands(databases: &[Database]) -> BytesMut {
    let mut buf = BytesMut::new();
    let now_ms = current_time_ms();

    for (db_idx, db) in databases.iter().enumerate() {
        let base_ts = db.base_timestamp();
        let data = db.data();
        if data.is_empty() {
            continue;
        }

        // Generate SELECT if not db 0
        if db_idx > 0 {
            let select_frame = Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"SELECT")),
                Frame::BulkString(Bytes::from(db_idx.to_string())),
            ]);
            serialize::serialize(&select_frame, &mut buf);
        }

        for (key, entry) in data {
            // Skip expired entries
            if entry.is_expired_at(base_ts, now_ms) {
                continue;
            }

            match entry.value.as_redis_value() {
                RedisValueRef::String(val) => {
                    let frame = Frame::Array(framevec![
                        Frame::BulkString(Bytes::from_static(b"SET")),
                        Frame::BulkString(key.to_bytes()),
                        Frame::BulkString(Bytes::copy_from_slice(val)),
                    ]);
                    serialize::serialize(&frame, &mut buf);
                }
                RedisValueRef::Hash(map) => {
                    if map.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"HSET")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for (field, val) in map.iter() {
                        args.push(Frame::BulkString(field.clone()));
                        args.push(Frame::BulkString(val.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                // Phase 200: for HashWithTtl we emit two RESP frames per key.
                //   1. `HSET key f1 v1 f2 v2 ...` rebuilds the hash body.
                //   2. `HPEXPIREAT key abs_ms FIELDS 1 field` for every entry
                //      in the TTL sidecar — one per TTL'd field for clarity
                //      (BGREWRITEAOF is rare; per-field framing keeps the
                //      replay shim simple, see `persistence::replay`).
                RedisValueRef::HashWithTtl { fields, ttls, .. } => {
                    if fields.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"HSET")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for (field, val) in fields.iter() {
                        args.push(Frame::BulkString(field.clone()));
                        args.push(Frame::BulkString(val.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);

                    for (field, ttl_ms) in ttls.iter() {
                        let mut ttl_args = vec![
                            Frame::BulkString(Bytes::from_static(b"HPEXPIREAT")),
                            Frame::BulkString(key.to_bytes()),
                            Frame::BulkString(Bytes::copy_from_slice(
                                ttl_ms.to_string().as_bytes(),
                            )),
                            Frame::BulkString(Bytes::from_static(b"FIELDS")),
                            Frame::BulkString(Bytes::from_static(b"1")),
                            Frame::BulkString(field.clone()),
                        ];
                        ttl_args.shrink_to_fit();
                        serialize::serialize(&Frame::Array(ttl_args.into()), &mut buf);
                    }
                }
                RedisValueRef::HashListpack(lp) => {
                    let map = lp.to_hash_map();
                    if map.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"HSET")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for (field, val) in &map {
                        args.push(Frame::BulkString(field.clone()));
                        args.push(Frame::BulkString(val.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::List(list) => {
                    if list.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"RPUSH")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for elem in list.iter() {
                        args.push(Frame::BulkString(elem.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::ListListpack(lp) => {
                    let list = lp.to_vec_deque();
                    if list.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"RPUSH")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for elem in &list {
                        args.push(Frame::BulkString(elem.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::Set(set) => {
                    if set.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"SADD")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for member in set.iter() {
                        args.push(Frame::BulkString(member.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::SetListpack(lp) => {
                    let set = lp.to_hash_set();
                    if set.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"SADD")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for member in &set {
                        args.push(Frame::BulkString(member.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::SetIntset(is) => {
                    let set = is.to_hash_set();
                    if set.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"SADD")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for member in &set {
                        args.push(Frame::BulkString(member.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::SortedSet { members, .. }
                | RedisValueRef::SortedSetBPTree { members, .. } => {
                    if members.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"ZADD")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for (member, score) in members.iter() {
                        args.push(Frame::BulkString(Bytes::from(score.to_string())));
                        args.push(Frame::BulkString(member.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::SortedSetListpack(lp) => {
                    let pairs: Vec<_> = lp.iter_pairs().collect();
                    if pairs.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"ZADD")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for (member_entry, score_entry) in &pairs {
                        let score_bytes = score_entry.as_bytes();
                        args.push(Frame::BulkString(Bytes::from(score_bytes)));
                        args.push(Frame::BulkString(Bytes::from(member_entry.as_bytes())));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::Stream(stream) => {
                    for (id, fields) in &stream.entries {
                        let mut args = vec![
                            Frame::BulkString(Bytes::from_static(b"XADD")),
                            Frame::BulkString(key.to_bytes()),
                            Frame::BulkString(id.to_bytes()),
                        ];
                        for (field, value) in fields {
                            args.push(Frame::BulkString(field.clone()));
                            args.push(Frame::BulkString(value.clone()));
                        }
                        serialize::serialize(&Frame::Array(args.into()), &mut buf);
                    }
                }
            }

            // Generate PEXPIRE for keys with TTL
            if entry.has_expiry() {
                let exp_ms = entry.expires_at_ms(base_ts);
                if exp_ms > now_ms {
                    let remaining_ms = exp_ms - now_ms;
                    let pexpire_frame = Frame::Array(framevec![
                        Frame::BulkString(Bytes::from_static(b"PEXPIRE")),
                        Frame::BulkString(key.to_bytes()),
                        Frame::BulkString(Bytes::from(remaining_ms.to_string())),
                    ]);
                    serialize::serialize(&pexpire_frame, &mut buf);
                }
            }
        }
    }

    buf
}

/// Snapshot databases and generate compacted AOF commands.
///
/// Shared by both the async (tokio) and sync (monoio) rewrite paths.
#[allow(dead_code)]
fn snapshot_and_generate(db: &SharedDatabases) -> BytesMut {
    let snapshot: Vec<(Vec<(CompactKey, Entry)>, u32)> = db
        .iter()
        .map(|lock| {
            let guard = lock.read();
            let base_ts = guard.base_timestamp();
            let entries = guard
                .data()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            (entries, base_ts)
        })
        .collect();

    let mut temp_dbs: Vec<Database> = Vec::with_capacity(snapshot.len());
    for (entries, _base_ts) in &snapshot {
        let mut db = Database::new();
        for (key, entry) in entries {
            db.set(key.to_bytes(), entry.clone());
        }
        temp_dbs.push(db);
    }

    generate_rewrite_commands(&temp_dbs)
}

/// Drain any queued `AofMessage::Append` messages to the current incr file.
///
/// Called during rewrite to catch in-flight appends that handlers sent before
/// the writer thread could enter the rewrite routine. Messages of other variants
/// are dropped silently (duplicate rewrites while a rewrite is in progress) or
/// returned via the flag for Shutdown (caller is responsible for honoring it
/// after the rewrite completes).
#[cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#[derive(Default)]
pub(crate) struct DrainOutcome {
    pub(crate) drained: usize,
    shutdown_requested: bool,
    /// AppendSync ack senders for entries drained during a rewrite. Under
    /// `appendfsync=always` the client must NOT be told `Synced` until the
    /// post-drain boundary `sync_data()` makes those bytes durable, so the acks
    /// are parked here and resolved by [`fulfill_acks`](Self::fulfill_acks) AFTER
    /// the caller's fsync — `Synced` on success, `FsyncFailed` on failure. Acking
    /// inside the drain (the old behaviour) reports a write durable before the
    /// boundary fsync, so a crash in that window loses an entry the client was
    /// told was safe. (Issue #140.)
    pub(crate) pending_acks: Vec<crate::runtime::channel::OneshotSender<AofAck>>,
}

#[cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
impl DrainOutcome {
    /// Resolve every parked AppendSync ack after the rewrite-boundary fsync.
    /// `synced=true` → `Synced`; `false` → `FsyncFailed`. A fresh `AofAck` is
    /// built per sender so `AofAck` needs no `Copy`/`Clone`. Drains the vec so a
    /// second call is a no-op.
    pub(crate) fn fulfill_acks(&mut self, synced: bool) {
        for tx in std::mem::take(&mut self.pending_acks) {
            let _ = tx.send(if synced {
                AofAck::Synced
            } else {
                AofAck::FsyncFailed
            });
        }
    }
}

/// Fsync `file` at a rewrite drain boundary, then resolve the drained batch's
/// parked AppendSync acks against the result: `Synced` on success, or
/// `FsyncFailed` + propagate the IO error on failure. Centralizes the issue-#140
/// durability ordering (ack strictly AFTER the bytes are durable) for every
/// `do_rewrite_*` drain site.
#[cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
pub(crate) fn sync_and_fulfill_drain(
    outcome: &mut DrainOutcome,
    file: &mut std::fs::File,
    incr_path: PathBuf,
) -> Result<(), MoonError> {
    match file.sync_data() {
        Ok(()) => {
            outcome.fulfill_acks(true);
            Ok(())
        }
        Err(e) => {
            // Boundary fsync failed: the drained writes are NOT durable. Tell the
            // waiting clients FsyncFailed (never Synced) and propagate the error
            // so the rewrite aborts.
            outcome.fulfill_acks(false);
            Err(AofError::Io {
                path: incr_path,
                source: e,
            }
            .into())
        }
    }
}

/// Bounded variant of [`drain_pending_appends`]: drain at most `max_drain`
/// messages (RAW RESP, no framing).  Pass [`usize::MAX`] for unbounded.
///
/// The TopLevel fold's phase-3 mid-drain passes `pending_aof_count` as the
/// bound (C4-DRAIN-BOUND): draining more would consume post-snapshot appends
/// that belong in the NEW incr, and those bytes would then be lost when
/// `manifest.advance()` deletes the old incr at the end of the fold.
#[cfg(feature = "runtime-monoio")]
pub(crate) fn drain_pending_appends_bounded(
    rx: &channel::MpscReceiver<AofMessage>,
    file: &mut std::fs::File,
    max_drain: usize,
) -> Result<DrainOutcome, MoonError> {
    use std::io::Write;
    let mut outcome = DrainOutcome::default();
    while outcome.drained < max_drain {
        match rx.try_recv() {
            Ok(msg) => match msg {
                AofMessage::Append {
                    bytes: data,
                    lsn: _,
                } => {
                    file.write_all(&data).map_err(|e| AofError::Io {
                        path: PathBuf::from("<aof toplevel incr drain>"),
                        source: e,
                    })?;
                    outcome.drained += 1;
                }
                AofMessage::AppendSync {
                    bytes: data,
                    lsn: _,
                    ack,
                } => {
                    file.write_all(&data).map_err(|e| AofError::Io {
                        path: PathBuf::from("<aof toplevel incr drain>"),
                        source: e,
                    })?;
                    outcome.drained += 1;
                    outcome.pending_acks.push(ack);
                }
                AofMessage::Shutdown => {
                    outcome.shutdown_requested = true;
                }
                AofMessage::Rewrite(_)
                | AofMessage::RewriteSharded(_)
                | AofMessage::RewritePerShard { .. } => {
                    // Already rewriting — drop redundant request.
                }
            },
            Err(flume::TryRecvError::Empty) => break,
            Err(flume::TryRecvError::Disconnected) => break,
        }
    }
    Ok(outcome)
}

#[cfg(feature = "runtime-monoio")]
pub(crate) fn drain_pending_appends(
    rx: &channel::MpscReceiver<AofMessage>,
    file: &mut std::fs::File,
) -> Result<DrainOutcome, MoonError> {
    drain_pending_appends_bounded(rx, file, usize::MAX)
}

/// [F6] Drain at most `max_drain` pending [`AofMessage::Append`] /
/// [`AofMessage::AppendSync`] messages from `rx` into `file` using the framed
/// `[u64 lsn LE][u32 len LE][RESP bytes]` on-disk format that per-shard
/// recovery expects.
///
/// Pass [`usize::MAX`] for an unbounded drain (captures all currently-queued
/// messages). The fold phase-3 mid-drain passes
/// [`AofFoldSnapshot::pending_aof_count`] as the bound to avoid an infinite
/// loop under sustained high write load: all pre-snapshot appends are
/// guaranteed to be in the channel already (shard event loop is single-
/// threaded), so draining exactly that many captures every pre-snapshot append
/// without consuming post-snapshot ones.
///
/// This is the per-shard twin of [`drain_pending_appends`] (which writes the
/// legacy TopLevel raw-RESP format). Correctness depends on the framing
/// matching `replay_per_shard`'s reader.
#[cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
pub(crate) fn drain_pending_appends_framed(
    rx: &channel::MpscReceiver<AofMessage>,
    file: &mut std::fs::File,
    max_drain: usize,
) -> Result<DrainOutcome, MoonError> {
    use std::io::Write;
    let mut outcome = DrainOutcome::default();
    let write_framed = |file: &mut std::fs::File, lsn: u64, data: &[u8]| -> std::io::Result<()> {
        let mut header = [0u8; 12];
        header[..8].copy_from_slice(&lsn.to_le_bytes());
        header[8..].copy_from_slice(&(data.len() as u32).to_le_bytes());
        file.write_all(&header)?;
        file.write_all(data)
    };
    while outcome.drained < max_drain {
        match rx.try_recv() {
            Ok(msg) => match msg {
                AofMessage::Append { lsn, bytes: data } => {
                    write_framed(file, lsn, &data).map_err(|e| AofError::Io {
                        path: PathBuf::from("<aof per-shard incr drain>"),
                        source: e,
                    })?;
                    outcome.drained += 1;
                }
                AofMessage::AppendSync {
                    lsn,
                    bytes: data,
                    ack,
                } => {
                    // H1-BARRIER: a zero-length AppendSync is an fsync barrier
                    // (pool::fsync_barrier) — it must produce NO on-disk record
                    // (a len=0 framed header reads as corruption on replay) but
                    // still counts toward `drained` (sender.len() counted it)
                    // and its ack still parks for the boundary fsync.
                    if !data.is_empty() {
                        write_framed(file, lsn, &data).map_err(|e| AofError::Io {
                            path: PathBuf::from("<aof per-shard incr drain>"),
                            source: e,
                        })?;
                    }
                    outcome.drained += 1;
                    // Park the ack until the caller's post-drain boundary fsync
                    // (issue #140); resolved Synced/FsyncFailed by
                    // `sync_and_fulfill_drain`. Mirrors `drain_pending_appends`.
                    outcome.pending_acks.push(ack);
                }
                AofMessage::Shutdown => {
                    outcome.shutdown_requested = true;
                }
                AofMessage::Rewrite(_)
                | AofMessage::RewriteSharded(_)
                | AofMessage::RewritePerShard { .. } => {
                    // Already rewriting this shard — drop redundant request.
                }
            },
            Err(flume::TryRecvError::Empty) => break,
            Err(flume::TryRecvError::Disconnected) => break,
        }
    }
    Ok(outcome)
}

/// [F6] Per-shard rewrite fold (monoio). Run by a single per-shard writer for
/// ITS shard only; the manifest commit is coordinated across all shards by the
/// shared [`PerShardRewriteCoord`].
///
/// Correctness ordering (prevents double-apply of non-idempotent commands like
/// INCR after the rewrite) — identical discipline to [`do_rewrite_sharded`],
/// scoped to one shard:
///
/// 1. Drain queued appends into the OLD incr (framed) and fsync.
/// 2. Acquire write locks on this shard's databases.
/// 3. Re-drain appends that arrived between phase 1 and the lock, into OLD
///    incr, and fsync.
/// 4. Snapshot this shard's databases under the locks.
/// 5. Release the locks before the expensive base-RDB write.
/// 6. Write the new base + new (empty) incr at `coord.new_seq` via
///    `advance_shard` (which does NOT bump `manifest.seq`), then reopen
///    `file` to the new incr. Subsequent appends land in the new generation.
/// 7. Signal completion to the coordinator; the last shard commits the
///    manifest (single seq flip) and prunes the old generation.
///
/// Until step 7's commit, the on-disk manifest still resolves to the old seq,
/// so a crash anywhere in steps 1-6 recovers the intact old generation.
///
/// # Cross-thread exactly-once invariant (load-bearing)
///
/// This fold runs on the per-shard *writer* thread, which is distinct from the
/// shard event-loop thread that applies commands. Exactly-once across the
/// rewrite boundary depends on a single ordering fact: the live write path
/// enqueues each command's AOF append **inside** the same `RwLock<Database>`
/// write guard under which it mutated the db (see `spsc_handler.rs`:
/// `wal_append_and_fanout` is called before `drop(guard)`). Phase 2 here
/// acquires those *same* locks (`all_shard_dbs()[sidx]` is
/// `ShardDatabases::shards[sidx]`, the exact `RwLock`s `write_db` locks), so
/// RwLock mutual exclusion forces the order
/// `enqueue → guard-release → fold-acquire → mid-drain(phase 3)`. Hence every
/// INCR whose mutation lands in the phase-4 snapshot had its append drained
/// into the OLD incr (then pruned at commit) — never replayed on top of the
/// new base. Were the append enqueued *after* the guard drop, a snapshot would
/// capture the mutation while its append still raced toward the NEW incr →
/// double-apply. The in-guard append is therefore the invariant; do not move it.
///
/// This also assumes the `RwLock`-backed `ShardDatabases` is the *live* store.
/// It is, because the thread-local `ShardSlice` fast path is dead code until
/// Phase 4 wires `init_shard` (`is_initialized()` is always false today). A
/// future Phase 4 that makes ShardSlice live MUST revisit this fold: the writer
/// thread cannot lock another thread's `!Send` `Rc<RefCell<Shard>>`, so the
/// per-shard rewrite would need a different snapshot-coordination mechanism.
///
/// # Known limitation — channel saturation during the fold
///
/// Exactly-once holds *absent append-channel saturation during the fold*. While
/// this function runs (phases 2-6, including the base-RDB serialize + write +
/// fsync of phase 6, which is hundreds of ms on a large shard) the writer is
/// NOT in its recv loop, so it is not draining the bounded
/// `mpsc_bounded::<AofMessage>(10_000)` append channel. Post-snapshot appends
/// queue there for the new incr; the event loop enqueues them with
/// `try_send_append` (drop-on-full, return ignored — `spsc_handler.rs`). Under
/// *sustained concurrent* writes on a large dataset, > 10_000 appends can pile
/// up during the window and the overflow is silently dropped — lost even on a
/// clean restart (worse than the everysec contract, which only loses on crash).
/// The single-client crash matrix cannot surface this (serialized `redis-cli`
/// never pressures the channel). This window is *pre-existing*: the shipped
/// `do_rewrite_sharded` has the identical non-draining gap. Tracked as a
/// known limitation (F6 is behind `--experimental-per-shard-rewrite`); the fix
/// (keep draining during phase 6, or block-on-full for the rewrite's duration)
/// is a separate scoped task. See `tmp/F6-known-limitations.md`.
#[cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
pub(crate) fn do_rewrite_per_shard(
    shard_id: u16,
    _shard_dbs: &crate::shard::shared_databases::ShardDatabases,
    file: &mut std::fs::File,
    rx: &channel::MpscReceiver<AofMessage>,
    coord: &PerShardRewriteCoord,
    fold_producer: &parking_lot::Mutex<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>,
    fold_notifier: &std::sync::Arc<crate::runtime::channel::Notify>,
) -> Result<(), MoonError> {
    use ringbuf::traits::Producer;
    // Panic/early-error safety: guarantees `shard_done` runs on EVERY exit
    // (success via `complete()`, `?`-error or panic-unwind via `Drop`). The
    // phase-8 `await_outcome` barrier makes that a liveness requirement, so
    // callers MUST NOT call `shard_done` after invoking this function — the
    // guard owns the single decrement for all exits. See `ShardDoneGuard`.
    let guard = ShardDoneGuard::new(coord);
    let _fold_t0 = std::time::Instant::now();

    // Phase 1: drain pre-rewrite queued appends into old incr (framed), fsync,
    // then resolve their parked AppendSync acks (issue #140).
    //
    // C4-DRAIN-BOUND (phase 1): snapshot `rx.len()` NOW — all messages currently
    // in the channel are pre-fold appends. New appends that arrive AFTER this
    // snapshot were enqueued concurrently with the fold and belong in the NEW incr;
    // draining them here would pull post-fold appends into the OLD incr, causing
    // them to be replayed on top of a base that already contains them (double-apply).
    // Without this bound, under sustained high write load the drain loops forever
    // because the INCR producer keeps the channel perpetually non-empty.
    let pre_drain_bound = rx.len();
    let mut pre_drain = drain_pending_appends_framed(rx, file, pre_drain_bound)?;
    sync_and_fulfill_drain(&mut pre_drain, file, PathBuf::from("<aof per-shard incr>"))?;
    info!(
        "F6 shard {} phase1 done: drained {} appends ({:.1}ms)",
        shard_id,
        pre_drain.drained,
        _fold_t0.elapsed().as_secs_f64() * 1000.0
    );

    // Phases 2-5 (C4 cooperative snapshot — ShardSlice is the live store):
    //
    // Instead of acquiring RwLock write guards on the shard's databases (which
    // the AOF writer thread cannot do because ShardSlice is thread-local/!Send),
    // we send an AofFold message to the shard's SPSC ring and block on the
    // oneshot reply.  The shard event loop processes AofFold atomically between
    // commands (single-threaded cooperative runtime), providing the same mutual
    // exclusion that RwLock write guards gave.  See ShardMessage::AofFold and
    // its handler in spsc_handler.rs.
    //
    // Ordering discipline (exactly-once guarantee preserved):
    //   1. Phase 1 above drained pre-fold appends into OLD incr.
    //   2. AofFold push notifies the shard; the shard event loop will drain its
    //      SPSC ring before processing AofFold, so any command that was
    //      in-flight when Phase 1 finished but not yet queued to the AOF channel
    //      will be processed (and its append enqueued) before the snapshot.
    //   3. Phase 3 (mid-drain below) captures appends enqueued after Phase 1
    //      but before the shard built the snapshot — same as the old mid-drain.
    //   4. Snapshot is replied after all prior mutations are applied; no
    //      command can mutate the shard while it is building the snapshot.
    let (reply_tx, reply_rx) =
        crate::runtime::channel::oneshot::<crate::shard::dispatch::AofFoldSnapshot>();
    {
        let mut prod = fold_producer.lock();
        prod.try_push(crate::shard::dispatch::ShardMessage::AofFold { reply_tx })
            .map_err(|_| AofError::RewriteFailed {
                detail: format!(
                    "do_rewrite_per_shard: shard {} AofFold SPSC ring full — fold aborted",
                    shard_id
                ),
            })?;
    }
    fold_notifier.notify_one();

    // Phase 4: collect the cooperative snapshot (blocks until the shard replies).
    // Use recv_blocking since do_rewrite_per_shard runs on a dedicated std::thread
    // (the per-shard AOF writer), not inside an async executor.
    //
    // C4 ordering invariant (exactly-once guarantee — frozen contract §3 C4):
    //   Every command the shard processed BEFORE building its snapshot had already
    //   enqueued its AOF append (via `try_send_append`) before the shard sent the
    //   reply — happens-before, by the single-threaded event-loop order. Therefore
    //   the mid-drain (phase 3) executed AFTER this recv_blocking captures ALL
    //   pre-snapshot appends into the OLD incr; post-snapshot appends land in the
    //   NEW incr. Running mid-drain BEFORE recv_blocking (the prior buggy order)
    //   misses appends that arrive while the shard is building its snapshot —
    //   they would land in the NEW incr and be replayed on top of a base that
    //   already contains them → double-apply on recovery (observed: 2016 of
    //   272988 INCRs survived restart in test_ssm4a_fold_4shard_experimental).
    // Wait for the shard event loop to build and reply with the snapshot.
    // Poll with try_recv + sleep so we can log a warning if the reply is slow
    // (helps diagnose starvation of the fold consumer under high write load).
    let fold_snapshot = {
        let wait_start = std::time::Instant::now();
        let mut warned = false;
        loop {
            match reply_rx.try_recv() {
                Ok(snap) => break snap,
                Err(flume::TryRecvError::Empty) => {
                    if !warned && wait_start.elapsed() >= std::time::Duration::from_millis(500) {
                        warned = true;
                        warn!(
                            "do_rewrite_per_shard: shard {} waiting for AofFold snapshot ({:.1}s elapsed) — \
                             shard event loop may be stalled or fold consumer not draining",
                            shard_id,
                            wait_start.elapsed().as_secs_f64()
                        );
                    }
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(flume::TryRecvError::Disconnected) => {
                    return Err(AofError::RewriteFailed {
                        detail: format!(
                            "do_rewrite_per_shard: shard {} AofFold reply channel dropped (shard shut down?)",
                            shard_id
                        ),
                    }.into());
                }
            }
        }
    };
    let pending_aof_count = fold_snapshot.pending_aof_count;
    let snapshot = fold_snapshot.dbs;
    info!(
        "F6 shard {} snapshot received: {} dbs, {} pre-snapshot pending ({:.1}ms total)",
        shard_id,
        snapshot.len(),
        pending_aof_count,
        _fold_t0.elapsed().as_secs_f64() * 1000.0
    );

    // Phase 3: drain appends that arrived while the shard was building the snapshot,
    // fsync, then resolve their parked AppendSync acks (issue #140).
    //
    // MUST run AFTER receiving the snapshot (see C4 ordering invariant above): the
    // shard's event loop enqueues every pre-snapshot append before sending the reply,
    // so a post-reply drain captures them all into the OLD incr.
    //
    // C4-DRAIN-BOUND: drain at most `pending_aof_count` messages — the exact number
    // of pre-snapshot appends the shard reported before building its snapshot. This
    // prevents an infinite drain loop under sustained high write load where new
    // (post-snapshot) appends arrive faster than we can drain them.
    let mut mid_drain = drain_pending_appends_framed(rx, file, pending_aof_count)?;
    sync_and_fulfill_drain(&mut mid_drain, file, PathBuf::from("<aof per-shard incr>"))?;
    info!(
        "F6 shard {} phase3 done: drained {} mid-appends ({:.1}ms total)",
        shard_id,
        mid_drain.drained,
        _fold_t0.elapsed().as_secs_f64() * 1000.0
    );

    // Phase 6: write new base, advance THIS shard's manifest entry (no seq
    // commit), reopen to the new incr. The manifest lock is held only for the
    // brief, await-free advance_shard call.
    let rdb_bytes = crate::persistence::rdb::save_snapshot_to_bytes(&snapshot)?;
    info!(
        "F6 shard {} rdb serialized: {} bytes ({:.1}ms total)",
        shard_id,
        rdb_bytes.len(),
        _fold_t0.elapsed().as_secs_f64() * 1000.0
    );
    let new_incr = {
        let mut m = coord.manifest.lock();
        m.advance_shard(shard_id, coord.new_seq, &rdb_bytes)?
    };
    *file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&new_incr)
        .map_err(|e| AofError::Io {
            path: new_incr,
            source: e,
        })?;

    info!(
        "F6 per-shard rewrite: shard {} folded (drained {}+{} appends), new seq {}",
        shard_id, pre_drain.drained, mid_drain.drained, coord.new_seq
    );
    if pre_drain.shutdown_requested || mid_drain.shutdown_requested {
        warn!(
            "F6 per-shard rewrite: shard {} saw shutdown during rewrite (honored after commit)",
            shard_id
        );
    }

    // Phase 7: signal completion; the last writer commits + prunes. `complete()`
    // performs the single clean `shard_done` and disarms the guard's Drop.
    guard.complete();

    // Phase 8 (barrier-before-resume): block until the terminal writer publishes
    // the committed generation, then make sure THIS writer's append file points
    // at it. On the happy path committed == new_seq and *file already points at
    // new_incr (phase 6) — nothing to do. On an abort/commit-failure the manifest
    // kept old_seq and pruned our new_seq incr, so reopen *file onto old_seq's
    // incr; otherwise we keep appending into a discarded generation that recovery
    // ignores — silent data loss. Replaces the old "RESTART recommended" hazard.
    let committed_seq = coord.await_outcome();
    if committed_seq != coord.new_seq {
        let committed_incr = coord
            .manifest
            .lock()
            .shard_incr_path_seq(shard_id, committed_seq);
        *file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&committed_incr)
            .map_err(|e| AofError::Io {
                path: committed_incr,
                source: e,
            })?;
        warn!(
            "F6 per-shard rewrite ABORTED: shard {} rolled its append file back to \
             committed seq {} (no restart needed)",
            shard_id, committed_seq
        );
    }
    Ok(())
}

/// Multi-part rewrite: snapshot single-shard databases → RDB base → advance manifest.
///
/// Correctness ordering (prevents double-apply of non-idempotent commands like
/// INCR/LPUSH/SADD after rewrite):
///
/// 1. Drain any queued appends into the OLD incr file and fsync.
/// 2. Acquire write locks on all databases in the shard. This blocks handlers
///    from applying new writes or queueing new appends for the locked dbs.
/// 3. Drain the channel once more — catches appends for writes that the
///    handler completed between step 1 and step 2.
/// 4. Snapshot every database under the write locks. Because no handler can
///    mutate the dbs while we hold the locks, the snapshot is atomic with
///    respect to the post-drain channel state.
/// 5. Release the write locks. New handler writes from here on queue in the
///    channel and will be processed into the NEW incr file after rotation.
/// 6. Write the new base RDB, advance the manifest, reopen the file handle.
///
/// Invariant: any write captured in the new base is NOT in the new incr file
/// (handlers were blocked between drain and snapshot), and any write NOT in
/// the new base IS in the new incr file (queued after lock release).
#[cfg(feature = "runtime-monoio")]
pub(crate) fn do_rewrite_single(
    db: &SharedDatabases,
    manifest: &mut crate::persistence::aof_manifest::AofManifest,
    file: &mut std::fs::File,
    rx: &channel::MpscReceiver<AofMessage>,
) -> Result<(), MoonError> {
    // Phase 1: drain pre-rewrite queued appends into old incr, fsync, then
    // resolve their parked AppendSync acks (issue #140).
    let mut pre_drain = drain_pending_appends(rx, file)?;
    sync_and_fulfill_drain(&mut pre_drain, file, manifest.incr_path())?;

    // Phase 2: acquire write locks on every database in the shard.
    // Order is consistent (index-ascending) so concurrent callers would
    // serialize without deadlock — but in practice only this thread
    // acquires multi-db locks.
    let guards: Vec<_> = db.iter().map(|lock| lock.write()).collect();

    // Phase 3: drain any appends the handlers sent between phase 1 and phase 2,
    // fsync, then resolve their parked AppendSync acks (issue #140).
    let mut mid_drain = drain_pending_appends(rx, file)?;
    sync_and_fulfill_drain(&mut mid_drain, file, manifest.incr_path())?;

    // Phase 4: snapshot under the write locks. No mutation is possible.
    let now_ms = current_time_ms();
    let snapshot: Vec<(
        Vec<(
            crate::storage::compact_key::CompactKey,
            crate::storage::entry::Entry,
        )>,
        u32,
    )> = guards
        .iter()
        .map(|guard| {
            let base_ts = guard.base_timestamp();
            let entries: Vec<_> = guard
                .data()
                .iter()
                .filter(|(_, v)| !v.is_expired_at(base_ts, now_ms))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            (entries, base_ts)
        })
        .collect();

    // Phase 5: release locks. Handlers resume; new appends queue in the channel
    // and will be processed into the new incr after step 6.
    drop(guards);

    // Phase 6: write new base, advance manifest, reopen.
    let rdb_bytes = crate::persistence::rdb::save_snapshot_to_bytes(&snapshot)?;
    let new_incr = manifest.advance(&rdb_bytes)?;

    *file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&new_incr)
        .map_err(|e| AofError::Io {
            path: new_incr,
            source: e,
        })?;

    info!(
        "AOF rewrite complete (single): drained {}+{} pre-snapshot appends, seq={}",
        pre_drain.drained, mid_drain.drained, manifest.seq
    );
    if pre_drain.shutdown_requested || mid_drain.shutdown_requested {
        // Caller doesn't currently observe this; logging is the escape hatch.
        warn!("AOF writer: shutdown requested during rewrite (will honor on next recv)");
    }
    Ok(())
}

/// TopLevel cooperative fold: snapshot shard 0 → merged RDB base → advance manifest.
///
/// Implements the C4 cooperative-snapshot protocol for the TopLevel (--shards 1)
/// AOF layout.  Mirrors [`do_rewrite_per_shard`] but operates on the single
/// TopLevel manifest instead of per-shard manifest entries.
///
/// Correctness ordering (exactly-once for non-idempotent commands like INCR):
///
/// 1. Drain queued appends (RAW RESP, not framed) into the OLD incr and fsync.
/// 2. Send `ShardMessage::AofFold` to shard 0's SPSC ring and block on the
///    cooperative snapshot reply.  The shard event loop processes AofFold
///    atomically between commands, providing the same mutual exclusion the old
///    RwLock write guards gave.
/// 3. Mid-drain: drain exactly `pending_aof_count` pre-snapshot appends into
///    OLD incr and fsync.
/// 4. Write new base RDB → advance manifest → reopen `file` to new incr.
///
/// If `fold_channels` is `None` (main.rs failed to wire them at startup),
/// abort cleanly with an error so the old generation stays authoritative.
///
/// **RAW RESP format**: TopLevel incr files use plain RESP bytes (no [lsn][len]
/// framing).  [`drain_pending_appends`] writes the correct format; never use
/// [`drain_pending_appends_framed`] here.
#[cfg(feature = "runtime-monoio")]
pub(crate) fn do_rewrite_sharded(
    _shard_dbs: &crate::shard::shared_databases::ShardDatabases,
    manifest: &mut crate::persistence::aof_manifest::AofManifest,
    file: &mut std::fs::File,
    rx: &channel::MpscReceiver<AofMessage>,
    fold_channels: Option<&(
        Arc<parking_lot::Mutex<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>>,
        Arc<crate::runtime::channel::Notify>,
    )>,
) -> Result<(), MoonError> {
    use ringbuf::traits::Producer;

    let _fold_t0 = std::time::Instant::now();

    // C4 deadlock guard: fold channels MUST be wired.  If absent, abort
    // cleanly — the old generation stays authoritative.
    let (fold_producer, fold_notifier) = match fold_channels {
        Some(pair) => pair,
        None => {
            return Err(AofError::RewriteFailed {
                detail: "do_rewrite_sharded (TopLevel): fold channels not wired at startup; \
                         rewrite aborted — old generation stays authoritative. \
                         Check main.rs fold-channel wiring."
                    .to_string(),
            }
            .into());
        }
    };

    // Phase 1: drain pre-rewrite queued appends into old incr (RAW RESP), fsync.
    let mut pre_drain = drain_pending_appends(rx, file)?;
    sync_and_fulfill_drain(&mut pre_drain, file, manifest.incr_path())?;
    info!(
        "TopLevel fold phase1 done: drained {} appends ({:.1}ms)",
        pre_drain.drained,
        _fold_t0.elapsed().as_secs_f64() * 1000.0
    );

    // Phases 2-4 (C4 cooperative snapshot):
    //
    // Send AofFold to shard 0 and block on the cooperative snapshot reply.
    // The shard event loop processes AofFold atomically between commands —
    // same mutual exclusion the old RwLock write guards provided.
    let (reply_tx, reply_rx) =
        crate::runtime::channel::oneshot::<crate::shard::dispatch::AofFoldSnapshot>();
    {
        let mut prod = fold_producer.lock();
        prod.try_push(crate::shard::dispatch::ShardMessage::AofFold { reply_tx })
            .map_err(|_| AofError::RewriteFailed {
                detail: "do_rewrite_sharded (TopLevel): AofFold SPSC ring full — fold aborted"
                    .to_string(),
            })?;
    }
    fold_notifier.notify_one();

    // Poll for the cooperative snapshot (blocks until shard 0 replies).
    // Runs on the dedicated AOF writer thread (not inside an async executor),
    // so recv_blocking / sleep are safe.
    let fold_snapshot = {
        let wait_start = std::time::Instant::now();
        let mut warned = false;
        loop {
            match reply_rx.try_recv() {
                Ok(snap) => break snap,
                Err(flume::TryRecvError::Empty) => {
                    if !warned && wait_start.elapsed() >= std::time::Duration::from_millis(500) {
                        warned = true;
                        warn!(
                            "do_rewrite_sharded (TopLevel): waiting for AofFold snapshot \
                             ({:.1}s elapsed) — shard 0 event loop may be stalled",
                            wait_start.elapsed().as_secs_f64()
                        );
                    }
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(flume::TryRecvError::Disconnected) => {
                    return Err(AofError::RewriteFailed {
                        detail: "do_rewrite_sharded (TopLevel): AofFold reply channel dropped \
                                 (shard 0 shut down?)"
                            .to_string(),
                    }
                    .into());
                }
            }
        }
    };
    let pending_aof_count = fold_snapshot.pending_aof_count;
    let snapshot = fold_snapshot.dbs;
    info!(
        "TopLevel fold snapshot received: {} dbs, {} pre-snapshot pending ({:.1}ms)",
        snapshot.len(),
        pending_aof_count,
        _fold_t0.elapsed().as_secs_f64() * 1000.0
    );

    // Phase 3: mid-drain — drain exactly `pending_aof_count` pre-snapshot appends
    // into OLD incr (RAW RESP format), then fsync + fulfill parked AppendSync acks.
    //
    // MUST run AFTER receiving the snapshot (C4 ordering invariant — identical
    // to do_rewrite_per_shard's phase 3 rationale).
    //
    // C4-DRAIN-BOUND: drain at most `pending_aof_count` messages — the exact count
    // the shard reported before building its snapshot.  Draining beyond this count
    // would consume post-snapshot appends that belong in the NEW incr; those bytes
    // would be silently lost when `manifest.advance()` deletes the old incr file
    // at the end of phase 4.  This mirrors the identical bound used by
    // `drain_pending_appends_framed` in `do_rewrite_per_shard`.
    let mut mid_drain = drain_pending_appends_bounded(rx, file, pending_aof_count)?;
    sync_and_fulfill_drain(&mut mid_drain, file, manifest.incr_path())?;
    info!(
        "TopLevel fold phase3 done: drained {} mid-appends ({:.1}ms)",
        mid_drain.drained,
        _fold_t0.elapsed().as_secs_f64() * 1000.0
    );

    // Phase 4 (= Phase 6 in do_rewrite_per_shard numbering):
    // Write new base RDB, advance the manifest (bumps seq, writes manifest,
    // deletes old files), reopen `file` to the new incr.
    let rdb_bytes = crate::persistence::rdb::save_snapshot_to_bytes(&snapshot)?;
    info!(
        "TopLevel fold rdb serialized: {} bytes ({:.1}ms)",
        rdb_bytes.len(),
        _fold_t0.elapsed().as_secs_f64() * 1000.0
    );
    let new_incr = manifest.advance(&rdb_bytes)?;
    *file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&new_incr)
        .map_err(|e| AofError::Io {
            path: new_incr,
            source: e,
        })?;

    info!(
        "TopLevel AOF rewrite complete: drained {}+{} appends, seq={}",
        pre_drain.drained, mid_drain.drained, manifest.seq
    );
    if pre_drain.shutdown_requested || mid_drain.shutdown_requested {
        warn!("TopLevel AOF writer: shutdown requested during rewrite (honored on next recv)");
    }
    Ok(())
}

/// Rewrite the AOF file with RDB preamble (binary base + empty RESP incremental).
///
/// Uses the same strategy as Redis 7+ `aof-use-rdb-preamble yes`:
/// the rewritten AOF starts with a full RDB snapshot (compact binary),
/// and new writes are appended as RESP after it. On startup, the loader
/// detects the RDB magic and reads the binary preamble, then switches
/// to RESP parsing for any incremental commands appended after.
#[allow(dead_code)] // Retained for legacy single-file and tokio path
fn rewrite_aof_sync(db: &SharedDatabases, aof_path: &Path) -> Result<(), MoonError> {
    // Snapshot under read locks, build temp Database objects for RDB serialization
    let snapshot: Vec<Database> = db
        .iter()
        .map(|lock| {
            let guard = lock.read();
            let mut temp = Database::new();
            let now_ms = current_time_ms();
            for (k, v) in guard.data().iter() {
                if !v.is_expired_at(guard.base_timestamp(), now_ms) {
                    temp.set(k.to_bytes(), v.clone());
                }
            }
            temp
        })
        .collect();

    let rdb_bytes = crate::persistence::rdb::save_to_bytes(&snapshot)?;

    let tmp_path = aof_path.with_extension("aof.tmp");
    std::fs::write(&tmp_path, &rdb_bytes).map_err(|e| AofError::Io {
        path: tmp_path.clone(),
        source: e,
    })?;
    std::fs::rename(&tmp_path, aof_path).map_err(|e| AofError::RewriteFailed {
        detail: format!(
            "rename {} -> {}: {}",
            tmp_path.display(),
            aof_path.display(),
            e
        ),
    })?;

    info!(
        "AOF rewrite complete (RDB preamble): {} bytes",
        rdb_bytes.len()
    );
    Ok(())
}

/// TopLevel cooperative fold for the tokio runtime.
///
/// Implements the C4 cooperative-snapshot protocol for the TopLevel (--shards 1)
/// AOF layout under `runtime-tokio`.  Called by the tokio `aof_writer_task` when
/// it receives `AofMessage::RewriteSharded`.
///
/// Unlike the monoio path (which has a manifest and a proper incr file), the
/// tokio TopLevel writer uses the legacy single-file `aof_path`.  The rewrite:
///
/// 1. Drains pre-snapshot appends from `rx` into `old_file` (RAW RESP).
/// 2. Sends `ShardMessage::AofFold` to shard 0 and blocks for the snapshot.
/// 3. Mid-drains into `old_file`.
/// 4. Writes the RDB snapshot to `aof_path.tmp` then renames atomically to
///    `aof_path` — the same post-rewrite file the caller will reopen for appending.
///
/// Returns `Err` (abort, old file unchanged) if fold channels are absent or the
/// shard reply channel is dropped.  The caller clears `AOF_REWRITE_IN_PROGRESS`
/// regardless.
///
/// **Detection note:** the tokio path writes to `aof_path` (e.g. `appendonly.aof`),
/// NOT to a manifest-stamped `moon.aof.<seq>.base.rdb`.  Test helpers that poll
/// for completion should check for `appendonly.aof` containing the RDB preamble
/// (`MOON` magic at offset 0) rather than a manifest-based file.
#[allow(dead_code)]
pub(crate) fn rewrite_aof_sharded_sync(
    _shard_dbs: &crate::shard::shared_databases::ShardDatabases,
    aof_path: &Path,
    rx: &channel::MpscReceiver<AofMessage>,
    old_file: &mut std::fs::File,
    fold_channels: Option<&(
        Arc<parking_lot::Mutex<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>>,
        Arc<crate::runtime::channel::Notify>,
    )>,
) -> Result<(), MoonError> {
    use ringbuf::traits::Producer;
    use std::io::Write as _;

    let _fold_t0 = std::time::Instant::now();

    // C4 deadlock guard: fold channels MUST be wired.
    let (fold_producer, fold_notifier) = match fold_channels {
        Some(pair) => pair,
        None => {
            return Err(AofError::RewriteFailed {
                detail: "rewrite_aof_sharded_sync (tokio TopLevel): fold channels not wired; \
                         rewrite aborted — old file unchanged."
                    .to_string(),
            }
            .into());
        }
    };

    // Phase 1: pre-drain (RAW RESP) into old file, then fsync + resolve parked
    // AppendSync acks (D2 fix: park acks here; fulfill after boundary fsync so
    // clients are never told Synced before the bytes are durable).
    // drain_pending_appends is #[cfg(runtime-monoio)] only; under tokio we do
    // the minimal equivalent inline (read until channel is empty).
    let mut pre_shutdown_requested = false;
    {
        let mut pre_acks: Vec<crate::runtime::channel::OneshotSender<AofAck>> = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            match msg {
                AofMessage::Append { bytes: data, .. } => {
                    old_file.write_all(&data).map_err(|e| AofError::Io {
                        path: aof_path.to_path_buf(),
                        source: e,
                    })?;
                }
                AofMessage::AppendSync {
                    bytes: data, ack, ..
                } => {
                    old_file.write_all(&data).map_err(|e| AofError::Io {
                        path: aof_path.to_path_buf(),
                        source: e,
                    })?;
                    pre_acks.push(ack);
                }
                AofMessage::Shutdown => {
                    pre_shutdown_requested = true;
                }
                AofMessage::Rewrite(_)
                | AofMessage::RewriteSharded(_)
                | AofMessage::RewritePerShard { .. } => {}
            }
        }
        let _ = old_file.flush();
        // Fulfill pre-drain acks after the boundary fsync (issue #140).
        match old_file.sync_data() {
            Ok(()) => {
                for ack in pre_acks {
                    let _ = ack.send(AofAck::Synced);
                }
            }
            Err(_) => {
                for ack in pre_acks {
                    let _ = ack.send(AofAck::FsyncFailed);
                }
            }
        }
    }
    info!(
        "rewrite_aof_sharded_sync (tokio) phase1 done ({:.1}ms)",
        _fold_t0.elapsed().as_secs_f64() * 1000.0
    );

    // Phases 2-3: AofFold cooperative snapshot.
    let (reply_tx, reply_rx) =
        crate::runtime::channel::oneshot::<crate::shard::dispatch::AofFoldSnapshot>();
    {
        let mut prod = fold_producer.lock();
        prod.try_push(crate::shard::dispatch::ShardMessage::AofFold { reply_tx })
            .map_err(|_| AofError::RewriteFailed {
                detail: "rewrite_aof_sharded_sync (tokio): AofFold SPSC ring full".to_string(),
            })?;
    }
    fold_notifier.notify_one();

    let fold_snapshot = {
        let wait_start = std::time::Instant::now();
        let mut warned = false;
        loop {
            match reply_rx.try_recv() {
                Ok(snap) => break snap,
                Err(flume::TryRecvError::Empty) => {
                    if !warned && wait_start.elapsed() >= std::time::Duration::from_millis(500) {
                        warned = true;
                        warn!(
                            "rewrite_aof_sharded_sync (tokio): waiting for AofFold snapshot \
                             ({:.1}s) — shard 0 may be stalled",
                            wait_start.elapsed().as_secs_f64()
                        );
                    }
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(flume::TryRecvError::Disconnected) => {
                    return Err(AofError::RewriteFailed {
                        detail: "rewrite_aof_sharded_sync (tokio): AofFold reply channel dropped"
                            .to_string(),
                    }
                    .into());
                }
            }
        }
    };
    // D1 fix: capture pending_aof_count to bound the mid-drain (C4-DRAIN-BOUND).
    // The shard reports exactly how many AOF messages were enqueued before it
    // built the snapshot. Draining more would consume post-snapshot appends that
    // must land in the new file (aof_path) AFTER the rename in phase 4.
    let pending_aof_count = fold_snapshot.pending_aof_count;
    let snapshot = fold_snapshot.dbs;
    info!(
        "rewrite_aof_sharded_sync (tokio) snapshot: {} dbs, {} pre-snapshot pending ({:.1}ms)",
        snapshot.len(),
        pending_aof_count,
        _fold_t0.elapsed().as_secs_f64() * 1000.0
    );

    // Phase 3 mid-drain: drain exactly `pending_aof_count` pre-snapshot appends
    // into OLD file (RAW RESP), then fsync + resolve parked AppendSync acks.
    //
    // C4-DRAIN-BOUND: must NOT drain beyond pending_aof_count. Post-snapshot
    // appends MUST remain in the channel so the writer loop appends them to the
    // reopened new file after this function returns. Draining them here would
    // cause them to be written to old_file (old inode), which is superseded by
    // the rename in phase 4 → those bytes are lost on restart (D1 bug).
    let mut mid_shutdown_requested = false;
    {
        let mut mid_acks: Vec<crate::runtime::channel::OneshotSender<AofAck>> = Vec::new();
        let mut drained = 0usize;
        while drained < pending_aof_count {
            match rx.try_recv() {
                Ok(msg) => match msg {
                    AofMessage::Append { bytes: data, .. } => {
                        old_file.write_all(&data).map_err(|e| AofError::Io {
                            path: aof_path.to_path_buf(),
                            source: e,
                        })?;
                        drained += 1;
                    }
                    AofMessage::AppendSync {
                        bytes: data, ack, ..
                    } => {
                        old_file.write_all(&data).map_err(|e| AofError::Io {
                            path: aof_path.to_path_buf(),
                            source: e,
                        })?;
                        drained += 1;
                        mid_acks.push(ack);
                    }
                    AofMessage::Shutdown => {
                        mid_shutdown_requested = true;
                    }
                    AofMessage::Rewrite(_)
                    | AofMessage::RewriteSharded(_)
                    | AofMessage::RewritePerShard { .. } => {}
                },
                Err(_) => break,
            }
        }
        let _ = old_file.flush();
        // Fulfill mid-drain acks after the boundary fsync (issue #140).
        match old_file.sync_data() {
            Ok(()) => {
                for ack in mid_acks {
                    let _ = ack.send(AofAck::Synced);
                }
            }
            Err(_) => {
                for ack in mid_acks {
                    let _ = ack.send(AofAck::FsyncFailed);
                }
            }
        }
        info!(
            "rewrite_aof_sharded_sync (tokio) phase3 mid-drain done: {} messages ({:.1}ms)",
            drained,
            _fold_t0.elapsed().as_secs_f64() * 1000.0
        );
    }

    // Phase 4: write RDB snapshot to aof_path (atomic tmp → rename).
    let rdb_bytes = crate::persistence::rdb::save_snapshot_to_bytes(&snapshot)?;
    let tmp_path = aof_path.with_extension("aof.tmp");
    {
        let mut f = std::fs::File::create(&tmp_path).map_err(|e| AofError::Io {
            path: tmp_path.clone(),
            source: e,
        })?;
        f.write_all(&rdb_bytes).map_err(|e| AofError::Io {
            path: tmp_path.clone(),
            source: e,
        })?;
        f.sync_data().map_err(|e| AofError::Io {
            path: tmp_path.clone(),
            source: e,
        })?;
    }
    std::fs::rename(&tmp_path, aof_path).map_err(|e| AofError::RewriteFailed {
        detail: format!(
            "rename {} -> {}: {}",
            tmp_path.display(),
            aof_path.display(),
            e
        ),
    })?;

    info!(
        "rewrite_aof_sharded_sync (tokio) complete: {} bytes ({:.1}ms)",
        rdb_bytes.len(),
        _fold_t0.elapsed().as_secs_f64() * 1000.0
    );
    if pre_shutdown_requested || mid_shutdown_requested {
        warn!(
            "rewrite_aof_sharded_sync (tokio): shutdown requested during rewrite \
             (will honor on next recv)"
        );
    }
    Ok(())
}

/// Reopen AOF file in append mode after atomic rewrite replaced it.
#[allow(dead_code)]
fn reopen_aof_sync(aof_path: &Path) -> Result<std::fs::File, std::io::Error> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(aof_path)
}

/// Rewrite the AOF file (tokio async wrapper).
///
/// Delegates to `rewrite_aof_sync` — the actual I/O is synchronous (temp write + rename).
#[cfg(feature = "runtime-tokio")]
#[tracing::instrument(skip_all, level = "info")]
pub async fn rewrite_aof(db: SharedDatabases, aof_path: &Path) -> Result<(), MoonError> {
    rewrite_aof_sync(&db, aof_path)
}
