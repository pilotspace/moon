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
fn sync_and_fulfill_drain(
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

#[cfg(feature = "runtime-monoio")]
pub(crate) fn drain_pending_appends(
    rx: &channel::MpscReceiver<AofMessage>,
    file: &mut std::fs::File,
) -> Result<DrainOutcome, MoonError> {
    use std::io::Write;
    let mut outcome = DrainOutcome::default();
    while let Ok(msg) = rx.try_recv() {
        match msg {
            // BGREWRITEAOF drain runs on the TopLevel writer (monoio) only;
            // PerShard rewrite is RFC step 6. Legacy v1 disk format → ignore lsn.
            AofMessage::Append {
                bytes: data,
                lsn: _,
            } => {
                file.write_all(&data).map_err(|e| AofError::Io {
                    path: PathBuf::from("<aof incr drain>"),
                    source: e,
                })?;
                outcome.drained += 1;
            }
            // AppendSync during a rewrite drain: bytes are written and counted,
            // but the ack is PARKED until the caller's post-drain boundary fsync
            // (issue #140) — acking `Synced` here would report durability before
            // the bytes are fsynced. If the write itself fails the `?` propagates
            // the error and the parked ack is dropped with the outcome — the
            // caller observes `RecvError`, which it treats as failure.
            AofMessage::AppendSync {
                bytes: data,
                lsn: _,
                ack,
            } => {
                file.write_all(&data).map_err(|e| AofError::Io {
                    path: PathBuf::from("<aof incr drain>"),
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
        }
    }
    Ok(outcome)
}

/// [F6] Drain a per-shard writer's queued appends into its OLD incr file using
/// the framed `[u64 lsn LE][u32 len LE][RESP bytes]` on-disk format that
/// per-shard recovery expects.
///
/// This is the per-shard twin of [`drain_pending_appends`] (which writes the
/// legacy TopLevel raw-RESP format). Correctness depends on the framing
/// matching `replay_per_shard`'s reader — an unframed write here would make the
/// drained appends unparseable on restart.
#[cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
pub(crate) fn drain_pending_appends_framed(
    rx: &channel::MpscReceiver<AofMessage>,
    file: &mut std::fs::File,
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
    while let Ok(msg) = rx.try_recv() {
        match msg {
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
                write_framed(file, lsn, &data).map_err(|e| AofError::Io {
                    path: PathBuf::from("<aof per-shard incr drain>"),
                    source: e,
                })?;
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
    shard_dbs: &crate::shard::shared_databases::ShardDatabases,
    file: &mut std::fs::File,
    rx: &channel::MpscReceiver<AofMessage>,
    coord: &PerShardRewriteCoord,
) -> Result<(), MoonError> {
    // Panic/early-error safety: guarantees `shard_done` runs on EVERY exit
    // (success via `complete()`, `?`-error or panic-unwind via `Drop`). The
    // phase-8 `await_outcome` barrier makes that a liveness requirement, so
    // callers MUST NOT call `shard_done` after invoking this function — the
    // guard owns the single decrement for all exits. See `ShardDoneGuard`.
    let guard = ShardDoneGuard::new(coord);

    let sidx = shard_id as usize;
    let all_shards = shard_dbs.all_shard_dbs();
    if sidx >= all_shards.len() {
        return Err(AofError::RewriteFailed {
            detail: format!(
                "do_rewrite_per_shard: shard {} out of range ({} shards)",
                sidx,
                all_shards.len()
            ),
        }
        .into());
    }

    // Phase 1: drain pre-rewrite queued appends into old incr (framed), fsync,
    // then resolve their parked AppendSync acks (issue #140).
    let mut pre_drain = drain_pending_appends_framed(rx, file)?;
    sync_and_fulfill_drain(&mut pre_drain, file, PathBuf::from("<aof per-shard incr>"))?;

    // Phase 2: acquire write locks on this shard's db(s) (db_idx ascending).
    let shard_locks = &all_shards[sidx];
    let guards: Vec<_> = shard_locks.iter().map(|lock| lock.write()).collect();

    // Phase 3: drain appends that completed between phase 1 and phase 2, fsync,
    // then resolve their parked AppendSync acks (issue #140).
    let mut mid_drain = drain_pending_appends_framed(rx, file)?;
    sync_and_fulfill_drain(&mut mid_drain, file, PathBuf::from("<aof per-shard incr>"))?;

    // Phase 4: snapshot this shard's databases under the locks.
    let now_ms = current_time_ms();
    let mut snapshot: Vec<(
        Vec<(
            crate::storage::compact_key::CompactKey,
            crate::storage::entry::Entry,
        )>,
        u32,
    )> = Vec::with_capacity(guards.len());
    for guard in &guards {
        let base_ts = guard.base_timestamp();
        let mut entries = Vec::new();
        for (key, entry) in guard.data().iter() {
            if !entry.is_expired_at(base_ts, now_ms) {
                entries.push((key.clone(), entry.clone()));
            }
        }
        snapshot.push((entries, base_ts));
    }

    // Phase 5: release locks before the expensive disk write.
    drop(guards);

    // Phase 6: write new base, advance THIS shard's manifest entry (no seq
    // commit), reopen to the new incr. The manifest lock is held only for the
    // brief, await-free advance_shard call.
    let rdb_bytes = crate::persistence::rdb::save_snapshot_to_bytes(&snapshot)?;
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

/// Multi-part rewrite: snapshot all shards → merged RDB base → advance manifest.
///
/// See [`do_rewrite_single`] for the ordering rationale. The multi-shard variant
/// holds write locks on every (shard, db) pair simultaneously for the duration
/// of the snapshot. This creates a brief global write pause, but it is the only
/// way to guarantee a torn-free snapshot without per-message sequence numbers.
#[cfg(feature = "runtime-monoio")]
pub(crate) fn do_rewrite_sharded(
    shard_dbs: &crate::shard::shared_databases::ShardDatabases,
    manifest: &mut crate::persistence::aof_manifest::AofManifest,
    file: &mut std::fs::File,
    rx: &channel::MpscReceiver<AofMessage>,
) -> Result<(), MoonError> {
    // Phase 1: drain pre-rewrite queued appends into old incr, fsync, then
    // resolve their parked AppendSync acks (issue #140).
    let mut pre_drain = drain_pending_appends(rx, file)?;
    sync_and_fulfill_drain(&mut pre_drain, file, manifest.incr_path())?;

    // Phase 2: acquire write locks on ALL (shard, db) pairs simultaneously.
    // Lock order is (shard_idx, db_idx) ascending — must match anywhere else
    // that acquires multiple locks to prevent deadlock (currently no other
    // call site does, but the ordering discipline is documented for future
    // maintainers).
    let all_shards = shard_dbs.all_shard_dbs();
    let mut guards: Vec<Vec<_>> = Vec::with_capacity(all_shards.len());
    for shard_locks in all_shards {
        let mut shard_guards = Vec::with_capacity(shard_locks.len());
        for lock in shard_locks {
            shard_guards.push(lock.write());
        }
        guards.push(shard_guards);
    }

    // Phase 3: drain appends completed between phase 1 and phase 2, fsync, then
    // resolve their parked AppendSync acks (issue #140).
    let mut mid_drain = drain_pending_appends(rx, file)?;
    sync_and_fulfill_drain(&mut mid_drain, file, manifest.incr_path())?;

    // Phase 4: snapshot under locks.
    let db_count = shard_dbs.db_count();
    let mut merged: Vec<(
        Vec<(
            crate::storage::compact_key::CompactKey,
            crate::storage::entry::Entry,
        )>,
        u32,
    )> = (0..db_count).map(|_| (Vec::new(), 0u32)).collect();
    let now_ms = current_time_ms();
    for shard_guards in &guards {
        for (db_idx, guard) in shard_guards.iter().enumerate() {
            let base_ts = guard.base_timestamp();
            if merged[db_idx].0.is_empty() {
                merged[db_idx].1 = base_ts;
            }
            for (key, entry) in guard.data().iter() {
                if !entry.is_expired_at(base_ts, now_ms) {
                    merged[db_idx].0.push((key.clone(), entry.clone()));
                }
            }
        }
    }

    // Phase 5: release locks before the expensive disk write.
    drop(guards);

    // Phase 6: write new base, advance manifest, reopen.
    let rdb_bytes = crate::persistence::rdb::save_snapshot_to_bytes(&merged)?;
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
        "AOF rewrite complete (sharded): drained {}+{} pre-snapshot appends, seq={}",
        pre_drain.drained, mid_drain.drained, manifest.seq
    );
    if pre_drain.shutdown_requested || mid_drain.shutdown_requested {
        warn!("AOF writer: shutdown requested during rewrite (will honor on next recv)");
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

/// Rewrite the AOF in sharded mode with RDB preamble.
///
/// Merges all shards' databases into a single RDB snapshot, writes it as
/// the AOF base file. New incremental writes are appended as RESP after.
#[allow(dead_code)]
pub(crate) fn rewrite_aof_sharded_sync(
    shard_dbs: &crate::shard::shared_databases::ShardDatabases,
    aof_path: &Path,
) -> Result<(), MoonError> {
    let db_count = shard_dbs.db_count();
    let now_ms = current_time_ms();
    let mut merged_dbs: Vec<Database> = (0..db_count).map(|_| Database::new()).collect();

    for shard_locks in shard_dbs.all_shard_dbs() {
        for (db_idx, lock) in shard_locks.iter().enumerate() {
            let guard = lock.read();
            for (key, entry) in guard.data().iter() {
                if !entry.is_expired_at(guard.base_timestamp(), now_ms) {
                    merged_dbs[db_idx].set(key.to_bytes(), entry.clone());
                }
            }
        }
    }

    let rdb_bytes = crate::persistence::rdb::save_to_bytes(&merged_dbs)?;

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
        "AOF rewrite (sharded, RDB preamble) complete: {} bytes",
        rdb_bytes.len()
    );
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
