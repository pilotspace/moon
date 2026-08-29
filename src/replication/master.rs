//! Master-side PSYNC2 handler and WAIT command support.
//!
//! Provides the inline single-/multi-shard PSYNC handlers for incoming PSYNC
//! connections (monoio-only — tokio rejects master-side PSYNC upstream via
//! `try_handle_psync_unsupported`) and `wait_for_replicas` for the WAIT
//! command.
//!
//! The original cross-shard-coordinated `handle_psync_on_master` +
//! `register_replica_with_shards` pair (both tokio and monoio variants) was
//! dead code — never called from any handler — and was deleted (task #72).
//! Its WAIT-ack wiring was also latent-broken: it initialized `ack_offsets`
//! but never spawned `ack_read_loop` to drain replica ACKs into them, so
//! `wait_for_replicas` against those registrations would never observe a
//! non-zero ack. The live path (`handle_psync_inline_single_shard` /
//! `handle_psync_inline_multi_shard`, called from
//! `shard/conn_accept.rs`) does this correctly.

use std::sync::Arc;

use parking_lot::RwLock;

#[cfg(feature = "runtime-monoio")]
use std::cell::RefCell;
#[cfg(feature = "runtime-monoio")]
use std::rc::Rc;
#[cfg(feature = "runtime-monoio")]
use tracing::info;

#[cfg(feature = "runtime-monoio")]
use crate::replication::backlog::SharedBacklog;
#[cfg(feature = "runtime-monoio")]
use crate::replication::handshake::PsyncDecision;
#[cfg(feature = "runtime-monoio")]
use crate::replication::state::ReplicaInfo;
use crate::replication::state::ReplicationState;

/// Read backlog bytes from one shard, returning None if the offset is evicted
/// or the backlog is unallocated.
#[cfg(feature = "runtime-monoio")]
fn backlog_bytes_from(shared: &SharedBacklog, from_offset: u64) -> Option<Vec<u8>> {
    let g = shared.lock();
    g.as_ref().and_then(|b| b.bytes_from(from_offset))
}

/// Inline single-shard PSYNC handler: snapshots the local shard's databases
/// directly (no SnapshotBegin SPSC self-send), sends `+FULLRESYNC` followed by
/// the RDB, then registers the replica for live streaming.
///
/// This bypasses the cross-shard SnapshotBegin coordination because for
/// `--shards 1` the connection runs on the same task as the shard event loop;
/// there is no second event loop to coordinate with.
///
/// Multi-shard PSYNC is rejected upstream in `try_handle_psync` until the
/// cross-shard coordination is wired (DispatchOutcome::Hijacked + per-shard
/// PrepareReplicaSync messages).
#[cfg(feature = "runtime-monoio")]
#[allow(clippy::too_many_arguments)]
pub async fn handle_psync_inline_single_shard(
    client_repl_id: &str,
    client_offset: i64,
    mut stream: monoio::net::TcpStream,
    repl_state: Arc<RwLock<ReplicationState>>,
    shard_databases: Arc<crate::shard::shared_databases::ShardDatabases>,
    replica_addr: std::net::SocketAddr,
) -> anyhow::Result<()> {
    use monoio::io::AsyncWriteRentExt;

    // The snapshot offset is NOT read here: FullResync re-reads it inside the
    // same synchronous stretch as the RDB capture (see below) so no write can
    // slip between the two.
    let (repl_id, repl_id2, backlog_slot) = {
        let rs = repl_state.read();
        let slot = rs
            .per_shard_backlogs
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("backlog slot missing"))?;
        (rs.repl_id.clone(), rs.repl_id2.clone(), slot)
    };

    // Decide full vs partial resync against the single-shard backlog.
    //
    // EC9: the three INFO `sync_*` counters are recorded here, at the one
    // point where the distinction is still visible. `PSYNC ? -1` is a replica
    // ASKING for a full resync, not a partial resync that failed — counting it
    // as `sync_partial_err` would make a healthy first-time replica look like
    // a backlog problem. Only a replica that offered a replid+offset and was
    // refused counts as a partial-resync failure.
    let decision = if client_offset < 0 {
        crate::admin::metrics_setup::record_sync_full();
        PsyncDecision::FullResync
    } else if client_repl_id != repl_id && client_repl_id != repl_id2 {
        crate::admin::metrics_setup::record_sync_partial_err();
        crate::admin::metrics_setup::record_sync_full();
        PsyncDecision::FullResync
    } else {
        let off = client_offset as u64;
        let g = backlog_slot.lock();
        if g.as_ref().is_some_and(|b| b.contains_offset(off)) {
            crate::admin::metrics_setup::record_sync_partial_ok();
            PsyncDecision::PartialResync { from_offset: off }
        } else {
            crate::admin::metrics_setup::record_sync_partial_err();
            crate::admin::metrics_setup::record_sync_full();
            PsyncDecision::FullResync
        }
    };

    match decision {
        PsyncDecision::FullResync => {
            // Snapshot-offset read and RDB capture share ONE synchronous
            // stretch (no `.await` between them): tasks on this thread are
            // cooperatively scheduled, so nothing can advance the offset or
            // mutate the keyspace in between. Reading the offset at fn entry
            // (before the FULLRESYNC line was written) let a write land both
            // inside the RDB AND above snapshot_offset — re-delivered via
            // catch-up, double-applying non-idempotent commands (INCR).
            //
            // This atomicity argument additionally requires that every local
            // write advances the offset IN its own synchronous stretch —
            // `record_local_write` appends the backlog bytes and moves the
            // counter at write time (only the live replica try_send is
            // deferred to the event-loop drain). If the advance were deferred
            // too (the pre-review design queued backlog+offset+fanout as one
            // message), a mutation already visible to this RDB capture could
            // still be BELOW `total_offset()` here, land in the catch-up
            // range, and double-apply — adversarial-review P0-2.
            //
            // The RDB is generated inline by reading all databases on shard 0.
            // Hold read guards across the synchronous write to avoid any
            // Clone requirement on Database (the type intentionally is not
            // Clone — its internal DashTable + FT/graph indices are large).
            let mut rdb_buf: Vec<u8> = Vec::new();
            let snapshot_offset = {
                let g = repl_state.read();
                // HIGH-2 (task #22): reset the stream's db context in
                // the SAME synchronous stretch as the snapshot capture
                // — every byte at offset ≥ snapshot_offset then starts
                // from "db unknown", so the first post-snapshot write
                // re-emits `SELECT <db>` and this replica's drain
                // (which starts at db 0 after loading the RDB) can
                // never bind a write to the wrong db. Redis's
                // `slaveseldb = -1` idiom. Redundant re-SELECTs for
                // already-attached replicas are idempotent.
                if let Some(slot) = g.stream_db.first() {
                    slot.store(-1, std::sync::atomic::Ordering::Relaxed);
                }
                let off = g.total_offset();
                drop(g);
                // Shard 0 is this thread's shard — use the thread-local slice.
                crate::shard::slice::with_shard(|s| {
                    // v0.7 R0.5: carry vector/text index DEFINITIONS inside the
                    // snapshot as moon-private RDB aux fields (reusing the
                    // sidecar codecs), so a fresh replica can recreate the
                    // indexes and backfill matching hashes after loading the
                    // keyspace. Contents then stay in sync via the live stream.
                    let vec_defs = {
                        let pairs = s.vector_store.collect_index_metas_with_weights();
                        if pairs.is_empty() {
                            None
                        } else {
                            Some(crate::vector::index_persist::serialize_index_metas_v5(
                                &pairs,
                            ))
                        }
                    };
                    let text_defs = {
                        let metas = s.text_store.collect_index_metas();
                        if metas.is_empty() {
                            None
                        } else {
                            Some(crate::text::index_persist::serialize_text_index_metas(
                                &metas,
                            ))
                        }
                    };
                    // v0.7 graph replication: whole-graph-store snapshot
                    // (frozen CSR segments + id cursors). ALWAYS written when
                    // the graph feature is on — an empty blob (0 graphs) tells
                    // the replica the master authoritatively has none.
                    #[cfg(feature = "graph")]
                    let graph_blob =
                        crate::replication::graph_sync::export_graph_store(&mut s.graph_store);
                    // Wave B ws-plane: the workspace registry snapshot. Shard
                    // 0 IS this thread's shard (single-shard path), so this
                    // capture is trivially in the same synchronous stretch as
                    // the offset read above — same convention as the graph
                    // blob: always written, an empty blob (0 entries) tells
                    // the replica the master authoritatively has none.
                    let ws_registry_blob = crate::replication::ws_sync::export_workspace_registry(
                        shard_databases.workspace_registry().as_deref(),
                    );
                    // Wave B stage 2b: this shard's MQ durable-queue +
                    // trigger registry snapshot. ALWAYS written (empty blob
                    // when both registries are unset) so the replica can
                    // distinguish "master shard has no MQ state" from
                    // "pre-MQ-replication master" (aux absent entirely).
                    let mq_blob = crate::replication::mq_sync::export_mq_registry(
                        s.durable_queue_registry.as_deref(),
                        s.trigger_registry.as_deref(),
                    );
                    let mut moon_aux: Vec<(&[u8], &[u8])> = Vec::new();
                    if let Some(ref v) = vec_defs {
                        moon_aux
                            .push((crate::persistence::redis_rdb::MOON_AUX_VECTOR_DEFS, &v[..]));
                    }
                    if let Some(ref t) = text_defs {
                        moon_aux.push((crate::persistence::redis_rdb::MOON_AUX_TEXT_DEFS, &t[..]));
                    }
                    #[cfg(feature = "graph")]
                    moon_aux.push((
                        crate::persistence::redis_rdb::MOON_AUX_GRAPH_STORE,
                        &graph_blob[..],
                    ));
                    moon_aux.push((
                        crate::persistence::redis_rdb::MOON_AUX_WORKSPACE_REGISTRY,
                        &ws_registry_blob[..],
                    ));
                    moon_aux.push((
                        crate::persistence::redis_rdb::MOON_AUX_MQ_REGISTRY,
                        &mq_blob[..],
                    ));
                    // Every database read-guarded for the whole RDB write, so
                    // the capture is a single cross-db consistent point — the
                    // same atomicity the single-threaded slice gave it, and the
                    // property `snapshot_offset` is paired with.
                    s.databases.with_all_read(|refs| {
                        crate::persistence::redis_rdb::write_rdb_refs_with_moon_aux(
                            refs,
                            &moon_aux,
                            &mut rdb_buf,
                        );
                    });
                });
                off
            };
            let response = format!("+FULLRESYNC {} {}\r\n", repl_id, snapshot_offset);
            let (wr, _) = stream.write_all(response.into_bytes()).await;
            wr.map_err(|e| anyhow::anyhow!(e))?;
            let header = format!("${}\r\n", rdb_buf.len());
            let (wr, _) = stream.write_all(header.into_bytes()).await;
            wr.map_err(|e| anyhow::anyhow!(e))?;
            let (wr, _) = stream.write_all(rdb_buf).await;
            wr.map_err(|e| anyhow::anyhow!(e))?;
            // Note: standard Redis replication does NOT terminate the bulk
            // string with \r\n during diskless full resync; the next bytes are
            // backlog/replication stream. Match that wire format.

            // Register FIRST, then catch up to exactly the registration
            // offset. The event loop replies with the offset at which live
            // fan-out to this replica begins; every byte below it comes from
            // the backlog read, every byte at or above it arrives on the
            // replica channel. Reading the backlog BEFORE registering (the
            // old order) left a window where a write drained in between
            // reached neither leg — a silent, unlogged replica gap.
            let reg = push_register_replica_inline(&repl_state)?;
            let reg_offset = reg
                .reg_rx
                .recv_async()
                .await
                .map_err(|_| anyhow::anyhow!("event loop dropped registration reply"))?;
            send_backlog_range(&mut stream, &backlog_slot, snapshot_offset, reg_offset).await?;

            drain_replica_inline_single_shard(reg, replica_addr, stream, repl_state).await?;
        }
        PsyncDecision::PartialResync { from_offset } => {
            let response = format!("+CONTINUE {}\r\n", repl_id);
            let (wr, _) = stream.write_all(response.into_bytes()).await;
            wr.map_err(|e| anyhow::anyhow!(e))?;

            // Same register-then-catch-up ordering as the FullResync arm.
            let reg = push_register_replica_inline(&repl_state)?;
            let reg_offset = reg
                .reg_rx
                .recv_async()
                .await
                .map_err(|_| anyhow::anyhow!("event loop dropped registration reply"))?;
            send_backlog_range(&mut stream, &backlog_slot, from_offset, reg_offset).await?;

            drain_replica_inline_single_shard(reg, replica_addr, stream, repl_state).await?;
        }
    }
    Ok(())
}

/// R2 (task #20): multi-shard master full resync — RFC 1B.
///
/// Every multi-shard PSYNC is answered with a FULL resync: the replica's
/// single scalar offset cannot be mapped back onto N per-shard backlogs, so
/// `+CONTINUE` is never offered (the client's requested replid/offset are
/// accepted but ignored). Flow:
///
///   1. Fan a [`ShardMessage::PrepareReplicaSync`] to every shard — its own
///      via the self queue (the SPSC mesh has no self-loop), the rest over
///      `dispatch_tx` + notifier. Each shard's arm snapshots its keyspace
///      slice to an RDB *body*, captures its shard offset, and registers the
///      replica's live channel — all in ONE synchronous stretch, so per shard
///      nothing can land between "in the snapshot" and "streamed live".
///   2. Stitch the bodies into ONE Redis-format RDB (`write_rdb_merged`) —
///      index definitions once, one graph blob PER shard — and send
///      `+FULLRESYNC <replid> <Σ shard offsets>` + the `$<len>` bulk. A
///      single-shard replica loads it through the unchanged R0 path.
///   3. Drain the merged live channel onto the socket (same drain + ACK
///      reader + overflow-kick loop as the single-shard path).
///
/// The summed offset is consistent even though shards capture at different
/// times: each shard's live records begin exactly at its own captured offset,
/// so bytes-on-wire past the FULLRESYNC base always equal
/// `total_offset() - base` — which keeps WAIT/ACK math exact.
#[cfg(feature = "runtime-monoio")]
#[allow(clippy::too_many_arguments)]
pub async fn handle_psync_inline_multi_shard(
    mut stream: monoio::net::TcpStream,
    repl_state: Arc<RwLock<ReplicationState>>,
    replica_addr: std::net::SocketAddr,
    dispatch_tx: Rc<RefCell<Vec<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>>>,
    spsc_notifiers: Vec<std::sync::Arc<crate::runtime::channel::Notify>>,
    self_shard_id: usize,
    num_shards: usize,
) -> anyhow::Result<()> {
    use monoio::io::AsyncWriteRentExt;
    use ringbuf::traits::Producer;

    let (repl_id, backlog_capacity) = {
        let rs = repl_state.read();
        (rs.repl_id.clone(), rs.backlog_capacity)
    };

    let replica_id = next_replica_id();
    // One merged live channel: every shard's fan-out entry holds a clone of
    // `tx`; the drain loop below pumps `rx` onto the socket. Capacity choice
    // matches the single-shard path (task #35) — shared across all shards.
    let (tx, rx) = crate::runtime::channel::mpsc_bounded::<bytes::Bytes>(16384);
    let kicked = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    // ── One uniform leg per shard: PrepareReplicaSync — the self shard via
    // the thread-local self queue (the SPSC mesh has no self-loop), remote
    // shards over the mesh + notifier. Each arm captures its RDB body, reads
    // its shard offset, and registers the replica's fan-out entry with
    // `cut = <captured offset>` in ONE synchronous stretch on its own thread.
    //
    // Exactly-once no longer depends on WHERE the registration lands in the
    // drain FIFO (two adversarial-review rounds found opposite failure modes
    // for FIFO-placement schemes): every live record is delivered through
    // `ReplicaLiveFanout` messages carrying the record's per-shard
    // `end_offset`, and delivery is filtered per replica by `end_offset >
    // cut`. A write applied before the arm's capture is inside the body and
    // at/below the cut (its queued fan-out message no-ops); a write applied
    // after it carries a higher end_offset and is delivered live exactly
    // once. Wire order per shard equals the self-queue FIFO order equals
    // offset order, so same-key writes replay in the master's order.
    let mut vector_defs: Option<Vec<u8>> = None;
    let mut text_defs: Option<Vec<u8>> = None;
    let mut reply_rxs = Vec::with_capacity(num_shards);
    for shard in 0..num_shards {
        let (reply_tx, reply_rx) =
            crate::runtime::channel::mpsc_bounded::<crate::shard::dispatch::PreparedShardSync>(1);
        let mut msg = crate::shard::dispatch::ShardMessage::PrepareReplicaSync(Box::new(
            crate::shard::dispatch::PrepareReplicaSyncPayload {
                replica_id,
                tx: tx.clone(),
                kicked: kicked.clone(),
                backlog_capacity,
                reply_tx,
            },
        ));
        if shard == self_shard_id {
            // Self queue push is infallible; the event loop drains it on its
            // next cycle while this task awaits the reply below.
            crate::shard::self_msg::push(msg);
            reply_rxs.push((shard, reply_rx));
            continue;
        }
        let idx = crate::shard::mesh::ChannelMesh::target_index(self_shard_id, shard);
        // The SPSC ring can be transiently full under load — bounded retry,
        // then abort loudly (the replica reconnects and retries the sync).
        let mut attempts = 0u32;
        loop {
            let res = { dispatch_tx.borrow_mut()[idx].try_push(msg) };
            match res {
                Ok(()) => {
                    spsc_notifiers[shard].notify_one();
                    break;
                }
                Err(back) => {
                    msg = back;
                    attempts += 1;
                    if attempts > 5_000 {
                        unregister_replica_all_shards(
                            replica_id,
                            &dispatch_tx,
                            &spsc_notifiers,
                            self_shard_id,
                            num_shards,
                        );
                        anyhow::bail!(
                            "shard {} SPSC full for >5s during PSYNC fan-out; aborting sync",
                            shard
                        );
                    }
                    monoio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            }
        }
        reply_rxs.push((shard, reply_rx));
    }

    // Collect every leg. A dropped reply means that shard could not prepare
    // (or we raced shutdown) — abort and explicitly unregister everywhere
    // (review P2: passive Disconnected pruning only fires on a shard's NEXT
    // write, which may never come).
    let mut bodies: Vec<Vec<u8>> = Vec::with_capacity(num_shards);
    let mut snapshot_offset: u64 = 0;
    #[cfg(feature = "graph")]
    let mut graph_blobs: Vec<Vec<u8>> = Vec::with_capacity(num_shards);
    // Wave B ws-plane: the registry is process-global, so only shard 0's leg
    // populates this (`Some`) — every other shard replies `None` (see
    // `PreparedShardSync::ws_registry_blob`). "Keep the first Some" matches
    // the `vector_defs`/`text_defs` convention below.
    let mut ws_registry_blob: Option<Vec<u8>> = None;
    let mut mq_blobs: Vec<Vec<u8>> = Vec::with_capacity(num_shards);
    for (shard, reply_rx) in reply_rxs {
        // Bounded wait (review): a wedged shard must not park this task —
        // and its registrations — forever. 30s is far past any observed
        // body-serialization time; on expiry the replica reconnects and
        // retries the sync.
        let prepared =
            match monoio::time::timeout(std::time::Duration::from_secs(30), reply_rx.recv_async())
                .await
            {
                Ok(Ok(p)) => p,
                timeout_or_dropped => {
                    unregister_replica_all_shards(
                        replica_id,
                        &dispatch_tx,
                        &spsc_notifiers,
                        self_shard_id,
                        num_shards,
                    );
                    anyhow::bail!(
                        "shard {} PrepareReplicaSync reply {} — aborting sync",
                        shard,
                        if timeout_or_dropped.is_err() {
                            "timed out after 30s"
                        } else {
                            "dropped"
                        }
                    );
                }
            };
        snapshot_offset += prepared.shard_offset;
        // Index definitions are keyspace-global and identical on every shard —
        // keep the first non-empty copy.
        if vector_defs.is_none() {
            vector_defs = prepared.vector_defs;
        }
        if text_defs.is_none() {
            text_defs = prepared.text_defs;
        }
        if ws_registry_blob.is_none() {
            ws_registry_blob = prepared.ws_registry_blob;
        }
        #[cfg(feature = "graph")]
        graph_blobs.push(prepared.graph_blob);
        mq_blobs.push(prepared.mq_blob);
        bodies.push(prepared.rdb_body);
    }

    // Stitch ONE valid Redis-format RDB. Graph content is sharded: one aux
    // entry per shard, imported in order by the replica (`read_moon_aux_all`).
    let mut moon_aux: Vec<(&[u8], &[u8])> = Vec::new();
    if let Some(v) = &vector_defs {
        moon_aux.push((crate::persistence::redis_rdb::MOON_AUX_VECTOR_DEFS, &v[..]));
    }
    if let Some(t) = &text_defs {
        moon_aux.push((crate::persistence::redis_rdb::MOON_AUX_TEXT_DEFS, &t[..]));
    }
    #[cfg(feature = "graph")]
    for blob in &graph_blobs {
        moon_aux.push((
            crate::persistence::redis_rdb::MOON_AUX_GRAPH_STORE,
            &blob[..],
        ));
    }
    if let Some(w) = &ws_registry_blob {
        moon_aux.push((
            crate::persistence::redis_rdb::MOON_AUX_WORKSPACE_REGISTRY,
            &w[..],
        ));
    }
    // MQ registry state is per-shard (owner-hashed by queue/trigger key,
    // same sharding model as graph names): one aux entry per shard, merged
    // additively into every replica shard by `mq_sync::install_mq_registry_many`.
    for blob in &mq_blobs {
        moon_aux.push((
            crate::persistence::redis_rdb::MOON_AUX_MQ_REGISTRY,
            &blob[..],
        ));
    }
    let mut rdb_buf: Vec<u8> = Vec::new();
    crate::persistence::redis_rdb::write_rdb_merged(&moon_aux, &bodies, &mut rdb_buf);
    info!(
        replica_id,
        num_shards,
        snapshot_offset,
        rdb_bytes = rdb_buf.len(),
        "multi-shard full resync prepared"
    );

    // Socket-write failures (replica died mid-transfer) must ALSO unregister
    // everywhere — otherwise the fan-out entries linger until each shard's
    // next write passively prunes them (review).
    let sent: anyhow::Result<()> = async {
        let response = format!("+FULLRESYNC {} {}\r\n", repl_id, snapshot_offset);
        let (wr, _) = stream.write_all(response.into_bytes()).await;
        wr.map_err(|e| anyhow::anyhow!(e))?;
        let header = format!("${}\r\n", rdb_buf.len());
        let (wr, _) = stream.write_all(header.into_bytes()).await;
        wr.map_err(|e| anyhow::anyhow!(e))?;
        let (wr, _) = stream.write_all(rdb_buf).await;
        wr.map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }
    .await;
    if let Err(e) = sent {
        unregister_replica_all_shards(
            replica_id,
            &dispatch_tx,
            &spsc_notifiers,
            self_shard_id,
            num_shards,
        );
        return Err(e);
    }
    // No backlog catch-up leg: each shard's registration IS its snapshot
    // point (same synchronous stretch), so live fan-out already covers every
    // byte past `snapshot_offset`.

    let reg = InlineReplicaRegistration {
        replica_id,
        tx,
        rx,
        // The multi-shard path has no registration-offset reply channel —
        // offsets arrived in the PrepareReplicaSync replies.
        reg_rx: crate::runtime::channel::mpsc_bounded::<u64>(1).1,
        kicked,
    };
    let drain_result =
        drain_replica_inline_single_shard(reg, replica_addr, stream, repl_state).await;
    // Best-effort explicit unregister on the REMOTE shards (the drain already
    // self-queued UnregisterReplica for this shard). A full ring is fine —
    // dropping `rx` above already flipped every sender to Disconnected, which
    // the next fan-out send prunes.
    unregister_replica_all_shards(
        replica_id,
        &dispatch_tx,
        &spsc_notifiers,
        self_shard_id,
        num_shards,
    );
    drain_result
}

/// Best-effort `UnregisterReplica` to every shard: the self shard via the
/// self queue, remote shards via the mesh (a full ring is tolerated — the
/// passive Disconnected prune covers it on that shard's next write). Used on
/// multi-shard PSYNC abort paths and after the drain loop exits, so a shard
/// that never sees another write doesn't hold a dead fan-out entry forever
/// (review P2).
#[cfg(feature = "runtime-monoio")]
fn unregister_replica_all_shards(
    replica_id: u64,
    dispatch_tx: &Rc<RefCell<Vec<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>>>,
    spsc_notifiers: &[std::sync::Arc<crate::runtime::channel::Notify>],
    self_shard_id: usize,
    num_shards: usize,
) {
    use ringbuf::traits::Producer;

    crate::shard::self_msg::push(crate::shard::dispatch::ShardMessage::UnregisterReplica {
        replica_id,
    });
    for shard in 0..num_shards {
        if shard == self_shard_id {
            continue;
        }
        let idx = crate::shard::mesh::ChannelMesh::target_index(self_shard_id, shard);
        let pushed = dispatch_tx.borrow_mut()[idx]
            .try_push(crate::shard::dispatch::ShardMessage::UnregisterReplica { replica_id });
        if pushed.is_ok() {
            spsc_notifiers[shard].notify_one();
        }
    }
}

/// Send backlog bytes `[from, to)` to the replica, or fail LOUDLY if the
/// backlog can no longer serve that range (evicted mid-sync). Aborting drops
/// the connection so the replica retries with a fresh full resync — strictly
/// better than the silent gap the old `if let Some(...)` skip produced.
#[cfg(feature = "runtime-monoio")]
async fn send_backlog_range(
    stream: &mut monoio::net::TcpStream,
    backlog_slot: &SharedBacklog,
    from: u64,
    to: u64,
) -> anyhow::Result<()> {
    use monoio::io::AsyncWriteRentExt;

    if to <= from {
        return Ok(());
    }
    let need = (to - from) as usize;
    let bytes = backlog_bytes_from(backlog_slot, from).ok_or_else(|| {
        anyhow::anyhow!(
            "replication backlog evicted during catch-up ({}..{}); aborting sync so the \
             replica retries a fresh full resync",
            from,
            to
        )
    })?;
    if bytes.len() < need {
        // The event loop appended [from, to) before replying with `to`, so a
        // shorter read means the head of the range was evicted.
        anyhow::bail!(
            "replication backlog short read during catch-up (have {} bytes, need {}); \
             aborting sync so the replica retries a fresh full resync",
            bytes.len(),
            need
        );
    }
    // Bytes past `to` are already queued on the replica channel by live
    // fan-out — truncate to avoid delivering them twice.
    let (wr, _) = stream.write_all(bytes[..need].to_vec()).await;
    wr.map_err(|e| anyhow::anyhow!(e))?;
    Ok(())
}

/// Everything the PSYNC task holds between pushing `RegisterReplica` and
/// draining the replica channel: the id, the receive half of the live
/// fan-out channel, its keep-alive tx (for WAIT/INFO bookkeeping), and the
/// registration-offset reply receiver.
#[cfg(feature = "runtime-monoio")]
struct InlineReplicaRegistration {
    replica_id: u64,
    tx: crate::runtime::channel::MpscSender<bytes::Bytes>,
    rx: crate::runtime::channel::MpscReceiver<bytes::Bytes>,
    reg_rx: crate::runtime::channel::MpscReceiver<u64>,
    /// Overflow disconnect signal shared with the shard fan-out — set when
    /// this replica's channel filled and a record could not be queued
    /// (task #35). The drain loop polls it and closes the socket so the
    /// replica resyncs instead of silently diverging.
    kicked: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

/// Push `RegisterReplica` onto shard 0's SPSC so the event loop captures the
/// tx into its local `replica_txs` Vec — the sole authority used by
/// `wal_append_and_fanout` for live write streaming. The message carries a
/// reply channel; the event loop answers with the shard offset at which live
/// fan-out begins, which the caller uses to bound its backlog catch-up read
/// (see `handle_psync_inline_single_shard`).
#[cfg(feature = "runtime-monoio")]
fn next_replica_id() -> u64 {
    use std::sync::atomic::Ordering;
    static NEXT_REPLICA_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT_REPLICA_ID.fetch_add(1, Ordering::Relaxed)
}

#[cfg(feature = "runtime-monoio")]
fn push_register_replica_inline(
    repl_state: &Arc<RwLock<ReplicationState>>,
) -> anyhow::Result<InlineReplicaRegistration> {
    let replica_id = next_replica_id();

    // 16384 records (task #35): 1024 overflowed within one pipelined burst on
    // the same host — every overflow now KICKS the replica into a resync, so
    // headroom directly reduces resync churn. Records are Bytes handles;
    // 16k × ~50 B typical ≈ under 1 MB queued worst-case per replica.
    let (tx, rx) = crate::runtime::channel::mpsc_bounded::<bytes::Bytes>(16384);
    let (reg_tx, reg_rx) = crate::runtime::channel::mpsc_bounded::<u64>(1);
    let kicked = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    // `--repl-backlog-size`, carried in RegisterReplica for the lazy fallback-init.
    let backlog_capacity = repl_state.read().backlog_capacity;
    // The inline PSYNC task runs ON the owning shard's thread; the SPSC mesh
    // has no self-loop (N·(N−1) skip-self — at shards=1 the producer Vec is
    // EMPTY), so registration goes through the thread-local self queue the
    // event loop drains alongside its SPSC consumers.
    //
    // The live-fanout start offset is captured HERE, at push time — NOT at
    // drain time. Local writes advance the shard offset synchronously at
    // write time (`record_local_write`), so a write that lands between this
    // push and the drain has already moved the counter; a drain-time read
    // would put it below `reg_offset` (delivered via backlog catch-up) while
    // its `ReplicaLiveFanout` message — queued BEHIND this registration —
    // also delivers it live: double-applied on the replica. The push-time
    // offset keeps catch-up and live delivery disjoint for every interleave
    // (see `RegisterReplica::push_offset`).
    let (push_offset, push_shard_offset) = {
        let g = repl_state.read();
        // Master-axis offset for the catch-up reply protocol, PER-SHARD-axis
        // offset for the fan-out cut. This path only runs at shards=1
        // (multi-shard PSYNC routes through `handle_psync_inline_multi_shard`),
        // so shard 0 is THE shard — and `seed_master_offset` (AOF recovery,
        // task #67) seeds shard 0 to the same value as the master axis, so
        // the two stay equal here even across a restart with prior write
        // history. Still read as two separate values (not asserted equal):
        // this function is generic over shard count and the invariant is
        // shard-0-specific.
        (g.total_offset(), g.shard_offset(0))
    };
    crate::shard::self_msg::push(crate::shard::dispatch::ShardMessage::RegisterReplica(
        Box::new(crate::shard::dispatch::RegisterReplicaPayload {
            replica_id,
            tx: tx.clone(),
            kicked: kicked.clone(),
            backlog_capacity,
            registered: Some(reg_tx),
            push_offset: Some(push_offset),
            cut: Some(push_shard_offset),
        }),
    ));
    Ok(InlineReplicaRegistration {
        replica_id,
        tx,
        rx,
        reg_rx,
        kicked,
    })
}

/// Single-shard inline replica drain: record the replica in
/// `ReplicationState.replicas` for WAIT / INFO bookkeeping, then pump the
/// live fan-out channel onto the replica's socket until the peer disconnects.
#[cfg(feature = "runtime-monoio")]
#[allow(clippy::await_holding_refcell_ref)]
async fn drain_replica_inline_single_shard(
    reg: InlineReplicaRegistration,
    addr: std::net::SocketAddr,
    stream: monoio::net::TcpStream,
    repl_state: Arc<RwLock<ReplicationState>>,
) -> anyhow::Result<()> {
    use monoio::io::AsyncWriteRentExt;

    let InlineReplicaRegistration {
        replica_id,
        tx,
        rx,
        reg_rx: _,
        kicked,
    } = reg;

    // Bookkeeping for WAIT/INFO.
    let replica_info = ReplicaInfo {
        id: replica_id,
        addr,
        ack_offsets: vec![std::sync::atomic::AtomicU64::new(0)],
        shard_txs: vec![tx],
        last_ack_time: std::sync::atomic::AtomicU64::new(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        ),
    };
    repl_state.write().replicas.push(replica_info);

    // R1 (task #19): the hijacked PSYNC socket is full-duplex — the replica
    // sends `REPLCONF ACK <offset>` back on it (1s cadence). Split the stream
    // so a local reader task records ACKs into this replica's
    // `ack_offsets`/`last_ack_time` (the data WAIT and INFO lag read) while
    // the write loop below streams live fan-out bytes. Same-thread
    // `monoio::spawn` — the task is !Send, which is fine here.
    use monoio::io::Splitable as _;
    let (rd, mut wr_half) = stream.into_split();
    let ack_reader = monoio::spawn({
        let repl_state = repl_state.clone();
        async move { ack_read_loop(rd, replica_id, repl_state).await }
    });

    // Drain the channel and write to the stream until the replica
    // disconnects — or until the shard fan-out KICKS this replica (task #35:
    // its channel overflowed, so at least one record is already missing from
    // the stream; continuing would deliver a silently-corrupt sequence). The
    // kick cannot arrive as an in-band message (the trigger IS a full
    // channel), so the recv races a coarse poll timer. `ReplicaInfo.shard_txs`
    // and this task both hold sender clones, which is why channel closure
    // can't signal this either.
    loop {
        monoio::select! {
            recv = rx.recv_async() => {
                let Ok(data) = recv else { break };
                let buf = data.to_vec();
                let (wr, _) = wr_half.write_all(buf).await;
                if wr.is_err() {
                    info!("Replica {} disconnected", replica_id);
                    break;
                }
            }
            _ = monoio::time::sleep(std::time::Duration::from_millis(250)) => {
                if kicked.load(std::sync::atomic::Ordering::Acquire) {
                    tracing::warn!(
                        replica_id,
                        "closing kicked replica connection (fan-out overflow) — \
                         replica will reconnect and resync"
                    );
                    break;
                }
            }
        }
    }
    // A kicked replica may still have queued records; they are stale (the
    // stream already has a gap) — drop them with the channel.
    // Dropping the write half closes our outbound side; the reader task ends
    // on EOF/error when the peer closes (its socket dies with the write half
    // on a disconnect-driven exit, so it does not linger).
    drop(wr_half);
    drop(ack_reader);
    // Remove from ReplicationState; the event loop will drop its replica_txs
    // entry on the next failed send via its own UnregisterReplica path.
    repl_state.write().replicas.retain(|r| r.id != replica_id);
    // Same-thread → self queue (no self-SPSC exists; see push_register_replica_inline).
    crate::shard::self_msg::push(crate::shard::dispatch::ShardMessage::UnregisterReplica {
        replica_id,
    });
    Ok(())
}

/// Read `REPLCONF ACK <offset>` frames off the replica's half of the hijacked
/// PSYNC socket and record them (R1, task #19). Runs as a same-thread task
/// beside the write-drain loop in `drain_replica_inline_single_shard`; exits
/// on EOF/read error. Anything other than a well-formed ACK is logged and
/// skipped — a replica cannot corrupt master state through this path.
#[cfg(feature = "runtime-monoio")]
async fn ack_read_loop(
    mut rd: monoio::net::tcp::TcpOwnedReadHalf,
    replica_id: u64,
    repl_state: Arc<RwLock<ReplicationState>>,
) {
    use monoio::io::AsyncReadRent;
    use std::sync::atomic::Ordering;

    let mut buf = bytes::BytesMut::with_capacity(4096);
    loop {
        let tmp = vec![0u8; 4096];
        let (res, tmp) = rd.read(tmp).await;
        let n = match res {
            Ok(0) | Err(_) => return, // replica closed its send half
            Ok(n) => n,
        };
        buf.extend_from_slice(&tmp[..n]);
        // Parse complete RESP frames directly — the shared replication
        // drainer (`drain_replicated_commands`) deliberately DROPS REPLCONF
        // as chatter, which is exactly the frame this loop exists to read.
        let acks = match drain_ack_offsets(&mut buf) {
            Ok(acks) => acks,
            Err(()) => {
                tracing::warn!(
                    replica_id,
                    "unparseable bytes on replica ACK channel — closing"
                );
                return;
            }
        };
        for offset in acks {
            let rs = repl_state.read();
            if let Some(info) = rs.replicas.iter().find(|r| r.id == replica_id) {
                // fetch_max: ACKs can only move forward — a reordered or
                // duplicate ACK never regresses the recorded offset.
                if let Some(slot) = info.ack_offsets.first() {
                    slot.fetch_max(offset, Ordering::Relaxed);
                }
                info.last_ack_time.store(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    Ordering::Relaxed,
                );
            }
        }
    }
}

/// Drain every complete RESP frame from `buf` and return the offsets of all
/// well-formed `REPLCONF ACK <offset>` frames (R1). Non-ACK frames are
/// skipped at debug level; a parse error returns `Err(())` — the unframed
/// stream cannot be resynced, so the caller must drop the connection.
#[cfg(feature = "runtime-monoio")]
fn drain_ack_offsets(buf: &mut bytes::BytesMut) -> Result<Vec<u64>, ()> {
    use crate::protocol::{Frame, ParseConfig, parse};

    let config = ParseConfig::default();
    let mut acks = Vec::new();
    loop {
        if buf.is_empty() {
            return Ok(acks);
        }
        let frame = match parse::parse(buf, &config) {
            Ok(Some(frame)) => frame,
            Ok(None) => return Ok(acks), // partial trailing frame — wait for more
            Err(_) => return Err(()),
        };
        let Frame::Array(items) = &frame else {
            continue; // inline keepalive etc. — ignore
        };
        let bulk = |f: &Frame| -> Option<bytes::Bytes> {
            match f {
                Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.clone()),
                _ => None,
            }
        };
        let is_ack = items.len() >= 3
            && bulk(&items[0]).is_some_and(|c| c.eq_ignore_ascii_case(b"REPLCONF"))
            && bulk(&items[1]).is_some_and(|s| s.eq_ignore_ascii_case(b"ACK"));
        if !is_ack {
            tracing::debug!("ignoring non-ACK frame on replica channel");
            continue;
        }
        if let Some(offset) = bulk(&items[2])
            .and_then(|b| std::str::from_utf8(&b).ok().map(|s| s.to_owned()))
            .and_then(|s| s.trim().parse::<u64>().ok())
        {
            acks.push(offset);
        }
    }
}

/// WAIT command: block until N replicas acknowledge >= target_offset, or timeout expires.
///
/// Returns the count of replicas that have acknowledged the offset.
pub async fn wait_for_replicas(
    num_required: usize,
    timeout_ms: u64,
    repl_state: &Arc<RwLock<ReplicationState>>,
) -> usize {
    let target_offset = {
        let rs = repl_state.read();
        rs.total_offset()
    };

    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms.max(1));

    loop {
        let acked_count = {
            let rs = repl_state.read();
            rs.replicas
                .iter()
                .filter(|r| {
                    let ack: u64 = r
                        .ack_offsets
                        .iter()
                        .map(|a| a.load(std::sync::atomic::Ordering::Relaxed))
                        .sum();
                    ack >= target_offset
                })
                .count()
        };

        if acked_count >= num_required {
            return acked_count;
        }
        if std::time::Instant::now() >= deadline {
            return acked_count;
        }
        #[cfg(feature = "runtime-tokio")]
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        #[cfg(feature = "runtime-monoio")]
        monoio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "runtime-tokio")]
    use super::*;

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn test_wait_for_replicas_no_replicas() {
        let state = Arc::new(RwLock::new(ReplicationState::new(
            1,
            "a".repeat(40),
            "b".repeat(40),
        )));
        let count = wait_for_replicas(1, 50, &state).await;
        assert_eq!(count, 0, "No replicas connected, should return 0");
    }

    #[cfg(feature = "runtime-tokio")]
    #[tokio::test]
    async fn test_wait_for_replicas_zero_required() {
        let state = Arc::new(RwLock::new(ReplicationState::new(
            1,
            "a".repeat(40),
            "b".repeat(40),
        )));
        let count = wait_for_replicas(0, 50, &state).await;
        assert_eq!(count, 0, "0 required with 0 replicas returns 0 immediately");
    }
}
