//! Master-side PSYNC2 handler and WAIT command support.
//!
//! Provides `handle_psync_on_master` for incoming PSYNC connections
//! and `wait_for_replicas` for the WAIT command.
#![allow(unused_imports)]

use std::sync::{Arc, RwLock};

#[cfg(feature = "runtime-monoio")]
use std::cell::RefCell;
#[cfg(feature = "runtime-monoio")]
use std::rc::Rc;
#[cfg(feature = "runtime-tokio")]
use tokio::io::AsyncWriteExt;
#[cfg(feature = "runtime-tokio")]
use tokio::net::tcp::OwnedWriteHalf;
use tracing::info;

use crate::replication::backlog::SharedBacklog;
use crate::replication::handshake::PsyncDecision;
use crate::replication::state::{ReplicaInfo, ReplicationState};

/// Evaluate PSYNC against shared backlogs by briefly taking each shard's mutex
/// to call `evaluate_psync` against the backlog snapshot.
fn evaluate_psync_shared(
    client_repl_id: &str,
    client_offset: i64,
    server_repl_id: &str,
    server_repl_id2: &str,
    shared: &[SharedBacklog],
) -> PsyncDecision {
    if client_offset < 0 {
        return PsyncDecision::FullResync;
    }
    let id_matches = client_repl_id == server_repl_id || client_repl_id == server_repl_id2;
    if !id_matches {
        return PsyncDecision::FullResync;
    }
    let offset = client_offset as u64;
    let all_cover = shared.iter().all(|s| {
        let g = s.lock();
        g.as_ref().is_some_and(|b| b.contains_offset(offset))
    });
    if all_cover {
        PsyncDecision::PartialResync {
            from_offset: offset,
        }
    } else {
        PsyncDecision::FullResync
    }
}

/// Read backlog bytes from one shard, returning None if the offset is evicted
/// or the backlog is unallocated.
fn backlog_bytes_from(shared: &SharedBacklog, from_offset: u64) -> Option<Vec<u8>> {
    let g = shared.lock();
    g.as_ref().and_then(|b| b.bytes_from(from_offset))
}

/// Master-side PSYNC handler: evaluate the request, respond, and wire up replication.
///
/// Called from handle_connection_sharded when PSYNC arrives on a connection.
/// Returns Ok(()) after handing the connection off to per-shard replica sender tasks.
///
/// Full resync flow:
///   1. Record snapshot_start_offset from current master_repl_offset
///   2. Send SnapshotBegin to ALL shards simultaneously
///   3. Await all N snapshot completions
///   4. Send per-shard RDB files as $<len>\r\n<bytes>\r\n bulk strings
///   5. Send backlog bytes from snapshot_start_offset to current offset
///   6. Register replica (RegisterReplica) with all shards for live streaming
///
/// Partial resync flow:
///   1. Send +CONTINUE <repl_id>
///   2. Send backlog bytes from client_offset to current offset for each shard
///   3. Register replica with all shards for live streaming
#[cfg(feature = "runtime-tokio")]
#[tracing::instrument(skip_all, level = "debug", fields(repl_id = %client_repl_id, offset = client_offset))]
pub async fn handle_psync_on_master(
    client_repl_id: &str,
    client_offset: i64,
    mut write_half: OwnedWriteHalf,
    repl_state: Arc<RwLock<ReplicationState>>,
    per_shard_backlogs: &[SharedBacklog],
    shard_producers: &mut Vec<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>,
    persistence_dir: &str,
    replica_addr: std::net::SocketAddr,
) -> anyhow::Result<()> {
    let (repl_id, repl_id2, current_offset) = {
        let rs = repl_state
            .read()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
        (rs.repl_id.clone(), rs.repl_id2.clone(), rs.total_offset())
    };

    let decision = evaluate_psync_shared(
        client_repl_id,
        client_offset,
        &repl_id,
        &repl_id2,
        per_shard_backlogs,
    );

    match decision {
        PsyncDecision::FullResync => {
            // Respond: +FULLRESYNC <repl_id> <offset>
            let response = format!("+FULLRESYNC {} {}\r\n", repl_id, current_offset);
            write_half.write_all(response.as_bytes()).await?;

            let snapshot_start_offset = current_offset;

            // Trigger per-shard snapshots in parallel
            let snap_dir = std::path::PathBuf::from(persistence_dir);
            let epoch = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            let num_shards = shard_producers.len();
            let mut snap_rxs: Vec<crate::runtime::channel::OneshotReceiver<Result<(), String>>> =
                Vec::new();

            for (shard_id, prod) in shard_producers.iter_mut().enumerate() {
                use ringbuf::traits::Producer;
                let (tx, rx) = crate::runtime::channel::oneshot();
                let msg = crate::shard::dispatch::ShardMessage::SnapshotBegin {
                    epoch,
                    snapshot_dir: snap_dir.clone(),
                    reply_tx: tx,
                };
                if prod.try_push(msg).is_err() {
                    anyhow::bail!("Failed to send SnapshotBegin to shard {}", shard_id);
                }
                snap_rxs.push(rx);
            }

            // Await all shard snapshots
            for (shard_id, rx) in snap_rxs.into_iter().enumerate() {
                match rx.await {
                    Ok(Ok(())) => info!("Master: shard {} snapshot complete", shard_id),
                    Ok(Err(e)) => anyhow::bail!("Shard {} snapshot failed: {}", shard_id, e),
                    Err(_) => anyhow::bail!("Shard {} snapshot channel dropped", shard_id),
                }
            }

            // Transfer per-shard RDB files using async I/O to avoid blocking the event loop.
            //
            // TODO: For standard Redis replicas, convert RRDSHARD data to Redis RDB format
            // using crate::persistence::redis_rdb::write_rdb() before sending. Currently we
            // send RRDSHARD format which our own replicas understand natively. The redis_rdb
            // module (from Plan 43-01) provides the conversion primitives when needed.
            for shard_id in 0..num_shards {
                let snap_path = snap_dir.join(format!("shard-{}.rrdshard", shard_id));
                let data = tokio::fs::read(&snap_path).await.map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to read shard {} snapshot at {:?}: {}",
                        shard_id,
                        snap_path,
                        e
                    )
                })?;
                let header = format!("${}\r\n", data.len());
                write_half.write_all(header.as_bytes()).await?;
                write_half.write_all(&data).await?;
                write_half.write_all(b"\r\n").await?;
                info!("Master: sent shard {} RDB ({} bytes)", shard_id, data.len());
            }

            // Stream backlog bytes accumulated since snapshot_start_offset
            for (shard_id, backlog) in per_shard_backlogs.iter().enumerate() {
                if let Some(bytes) = backlog_bytes_from(backlog, snapshot_start_offset) {
                    if !bytes.is_empty() {
                        write_half.write_all(&bytes).await?;
                        info!(
                            "Master: sent shard {} backlog ({} bytes)",
                            shard_id,
                            bytes.len()
                        );
                    }
                }
            }

            // Register this replica with all shards for live WAL streaming
            register_replica_with_shards(
                replica_addr,
                write_half,
                repl_state,
                shard_producers,
                num_shards,
            )
            .await?;
        }

        PsyncDecision::PartialResync { from_offset } => {
            // Respond: +CONTINUE <repl_id>
            let response = format!("+CONTINUE {}\r\n", repl_id);
            write_half.write_all(response.as_bytes()).await?;

            let num_shards = shard_producers.len();

            // Stream backlog bytes from from_offset to current for each shard
            for (shard_id, backlog) in per_shard_backlogs.iter().enumerate() {
                if let Some(bytes) = backlog_bytes_from(backlog, from_offset) {
                    if !bytes.is_empty() {
                        write_half.write_all(&bytes).await?;
                        info!(
                            "Master: partial resync shard {} ({} bytes)",
                            shard_id,
                            bytes.len()
                        );
                    }
                }
            }

            // Register for live streaming
            register_replica_with_shards(
                replica_addr,
                write_half,
                repl_state,
                shard_producers,
                num_shards,
            )
            .await?;
        }
    }

    Ok(())
}

/// Master-side PSYNC handler for monoio runtime.
///
/// Same logic as the tokio variant but uses monoio ownership I/O for all TCP writes.
/// Takes a mutable reference to `monoio::net::TcpStream` instead of `OwnedWriteHalf`.
#[cfg(feature = "runtime-monoio")]
#[tracing::instrument(skip_all, level = "debug", fields(repl_id = %client_repl_id, offset = client_offset))]
pub async fn handle_psync_on_master(
    client_repl_id: &str,
    client_offset: i64,
    mut stream: monoio::net::TcpStream,
    repl_state: Arc<RwLock<ReplicationState>>,
    per_shard_backlogs: &[SharedBacklog],
    shard_producers: &mut Vec<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>,
    persistence_dir: &str,
    replica_addr: std::net::SocketAddr,
) -> anyhow::Result<()> {
    use monoio::io::AsyncWriteRentExt;

    let (repl_id, repl_id2, current_offset) = {
        let rs = repl_state
            .read()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
        (rs.repl_id.clone(), rs.repl_id2.clone(), rs.total_offset())
    };

    let decision = evaluate_psync_shared(
        client_repl_id,
        client_offset,
        &repl_id,
        &repl_id2,
        per_shard_backlogs,
    );

    match decision {
        PsyncDecision::FullResync => {
            // Respond: +FULLRESYNC <repl_id> <offset>
            let response = format!("+FULLRESYNC {} {}\r\n", repl_id, current_offset);
            let data = response.into_bytes();
            let (wr, _) = stream.write_all(data).await;
            wr.map_err(|e| anyhow::anyhow!(e))?;

            let snapshot_start_offset = current_offset;

            // Trigger per-shard snapshots in parallel
            let snap_dir = std::path::PathBuf::from(persistence_dir);
            let epoch = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            let num_shards = shard_producers.len();
            let mut snap_rxs: Vec<crate::runtime::channel::OneshotReceiver<Result<(), String>>> =
                Vec::new();

            for (shard_id, prod) in shard_producers.iter_mut().enumerate() {
                use ringbuf::traits::Producer;
                let (tx, rx) = crate::runtime::channel::oneshot();
                let msg = crate::shard::dispatch::ShardMessage::SnapshotBegin {
                    epoch,
                    snapshot_dir: snap_dir.clone(),
                    reply_tx: tx,
                };
                if prod.try_push(msg).is_err() {
                    anyhow::bail!("Failed to send SnapshotBegin to shard {}", shard_id);
                }
                snap_rxs.push(rx);
            }

            // Await all shard snapshots
            for (shard_id, rx) in snap_rxs.into_iter().enumerate() {
                match rx.await {
                    Ok(Ok(())) => info!("Master: shard {} snapshot complete", shard_id),
                    Ok(Err(e)) => anyhow::bail!("Shard {} snapshot failed: {}", shard_id, e),
                    Err(_) => anyhow::bail!("Shard {} snapshot channel dropped", shard_id),
                }
            }

            // Transfer per-shard RDB files.
            // Monoio: synchronous file read. Thread-per-core model means this
            // blocks only this core's event loop. For large files, consider
            // monoio::fs::File with read_at() in the future.
            //
            // TODO: For standard Redis replicas, convert RRDSHARD data to Redis RDB format
            // using crate::persistence::redis_rdb::write_rdb() before sending. Currently we
            // send RRDSHARD format which our own replicas understand natively.
            for shard_id in 0..num_shards {
                let snap_path = snap_dir.join(format!("shard-{}.rrdshard", shard_id));
                let file_data = std::fs::read(&snap_path).map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to read shard {} snapshot at {:?}: {}",
                        shard_id,
                        snap_path,
                        e
                    )
                })?;
                let header = format!("${}\r\n", file_data.len());
                let (wr, _) = stream.write_all(header.into_bytes()).await;
                wr.map_err(|e| anyhow::anyhow!(e))?;
                let (wr, _) = stream.write_all(file_data).await;
                wr.map_err(|e| anyhow::anyhow!(e))?;
                let (wr, _) = stream.write_all(b"\r\n".to_vec()).await;
                wr.map_err(|e| anyhow::anyhow!(e))?;
                info!(
                    "Master: sent shard {} RDB ({} bytes)",
                    shard_id,
                    std::fs::metadata(&snap_path).map(|m| m.len()).unwrap_or(0)
                );
            }

            // Stream backlog bytes accumulated since snapshot_start_offset
            for (shard_id, backlog) in per_shard_backlogs.iter().enumerate() {
                if let Some(bytes) = backlog_bytes_from(backlog, snapshot_start_offset) {
                    if !bytes.is_empty() {
                        let (wr, _) = stream.write_all(bytes.to_vec()).await;
                        wr.map_err(|e| anyhow::anyhow!(e))?;
                        info!(
                            "Master: sent shard {} backlog ({} bytes)",
                            shard_id,
                            bytes.len()
                        );
                    }
                }
            }

            // Register this replica with all shards for live WAL streaming
            register_replica_with_shards(
                replica_addr,
                stream,
                repl_state,
                shard_producers,
                num_shards,
            )
            .await?;
        }

        PsyncDecision::PartialResync { from_offset } => {
            // Respond: +CONTINUE <repl_id>
            let response = format!("+CONTINUE {}\r\n", repl_id);
            let (wr, _) = stream.write_all(response.into_bytes()).await;
            wr.map_err(|e| anyhow::anyhow!(e))?;

            let num_shards = shard_producers.len();

            // Stream backlog bytes from from_offset to current for each shard
            for (shard_id, backlog) in per_shard_backlogs.iter().enumerate() {
                if let Some(bytes) = backlog_bytes_from(backlog, from_offset) {
                    if !bytes.is_empty() {
                        let (wr, _) = stream.write_all(bytes.to_vec()).await;
                        wr.map_err(|e| anyhow::anyhow!(e))?;
                        info!(
                            "Master: partial resync shard {} ({} bytes)",
                            shard_id,
                            bytes.len()
                        );
                    }
                }
            }

            // Register for live streaming
            register_replica_with_shards(
                replica_addr,
                stream,
                repl_state,
                shard_producers,
                num_shards,
            )
            .await?;
        }
    }

    Ok(())
}

/// Assign a unique replica ID and register the replica's write half with all shards.
///
/// For each shard, creates a bounded mpsc channel, spawns a replica_sender_task
/// that drains the channel to the socket, and sends RegisterReplica to each shard.
#[cfg(feature = "runtime-tokio")]
async fn register_replica_with_shards(
    addr: std::net::SocketAddr,
    write_half: OwnedWriteHalf,
    repl_state: Arc<RwLock<ReplicationState>>,
    shard_producers: &mut Vec<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>,
    num_shards: usize,
) -> anyhow::Result<()> {
    use ringbuf::traits::Producer;
    use std::sync::atomic::Ordering;

    static NEXT_REPLICA_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let replica_id = NEXT_REPLICA_ID.fetch_add(1, Ordering::Relaxed);

    // Share the write_half across per-shard sender tasks
    let write_half = Arc::new(tokio::sync::Mutex::new(write_half));

    // `--repl-backlog-size`, carried in RegisterReplica for the lazy fallback-init.
    let backlog_capacity = repl_state
        .read()
        .map(|g| g.backlog_capacity)
        .unwrap_or(crate::replication::state::DEFAULT_REPL_BACKLOG_SIZE);

    let channel_capacity = 1024;
    let mut shard_txs = Vec::with_capacity(num_shards);
    let mut ack_offsets = Vec::with_capacity(num_shards);

    for shard_id in 0..num_shards {
        let (tx, rx) = crate::runtime::channel::mpsc_bounded::<bytes::Bytes>(channel_capacity);
        shard_txs.push(tx.clone());
        ack_offsets.push(std::sync::atomic::AtomicU64::new(0));

        // Send RegisterReplica to the shard's SPSC
        if let Some(prod) = shard_producers.get_mut(shard_id) {
            let msg = crate::shard::dispatch::ShardMessage::RegisterReplica(Box::new(
                crate::shard::dispatch::RegisterReplicaPayload {
                    replica_id,
                    tx,
                    // Legacy multi-shard drain loops do not poll the kick flag
                    // (superseded by the R2 redesign); overflow still stops
                    // queueing via the fan-out's retain.
                    kicked: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                    backlog_capacity,
                    // Fire-and-forget: the multi-shard register paths are superseded
                    // by the R2 PrepareReplicaSync redesign; the offset-reply catch-up
                    // protocol is wired on the single-shard inline path only.
                    registered: None,
                    // Cross-shard registration: the target shard's offset is
                    // owned by its own thread — the arm reads it at drain.
                    push_offset: None,
                    // No snapshot body was captured on this shard's thread —
                    // the arm's drain-time offset is the correct cut.
                    cut: None,
                },
            ));
            let _ = prod.try_push(msg);
        }

        // Spawn sender task: drains channel -> writes to TCP socket
        let wh = Arc::clone(&write_half);
        tokio::spawn(async move {
            while let Ok(data) = rx.recv_async().await {
                let mut guard = wh.lock().await;
                if guard.write_all(&data).await.is_err() {
                    info!("Replica sender shard {}: socket closed", shard_id);
                    break;
                }
            }
        });
    }

    // Register replica in ReplicationState
    let replica_info = ReplicaInfo {
        id: replica_id,
        addr,
        ack_offsets,
        shard_txs,
        last_ack_time: std::sync::atomic::AtomicU64::new(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        ),
    };
    if let Ok(mut rs) = repl_state.write() {
        rs.replicas.push(replica_info);
    }

    info!(
        "Master: replica {} registered across {} shards",
        replica_id, num_shards
    );
    Ok(())
}

/// Monoio variant of replica registration.
///
/// Uses `Rc<RefCell<Option<monoio::net::TcpStream>>>` with take/put-back pattern
/// for ownership I/O writes. Single-threaded cooperative scheduling ensures only
/// one sender task runs at a time.
#[cfg(feature = "runtime-monoio")]
async fn register_replica_with_shards(
    addr: std::net::SocketAddr,
    stream: monoio::net::TcpStream,
    repl_state: Arc<RwLock<ReplicationState>>,
    shard_producers: &mut Vec<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>,
    num_shards: usize,
) -> anyhow::Result<()> {
    use monoio::io::AsyncWriteRentExt;
    use ringbuf::traits::Producer;
    use std::sync::atomic::Ordering;

    static NEXT_REPLICA_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let replica_id = NEXT_REPLICA_ID.fetch_add(1, Ordering::Relaxed);

    // Share the stream across per-shard sender tasks.
    // monoio's write_all takes &mut self + owned buffer, so RefCell<TcpStream> suffices.
    // Single-threaded cooperative scheduling ensures no concurrent borrows.
    let shared_stream: Rc<RefCell<monoio::net::TcpStream>> = Rc::new(RefCell::new(stream));

    // `--repl-backlog-size`, carried in RegisterReplica for the lazy fallback-init.
    let backlog_capacity = repl_state
        .read()
        .map(|g| g.backlog_capacity)
        .unwrap_or(crate::replication::state::DEFAULT_REPL_BACKLOG_SIZE);

    let channel_capacity = 1024;
    let mut shard_txs = Vec::with_capacity(num_shards);
    let mut ack_offsets = Vec::with_capacity(num_shards);

    for shard_id in 0..num_shards {
        let (tx, rx) = crate::runtime::channel::mpsc_bounded::<bytes::Bytes>(channel_capacity);
        shard_txs.push(tx.clone());
        ack_offsets.push(std::sync::atomic::AtomicU64::new(0));

        // Send RegisterReplica to the shard's SPSC
        if let Some(prod) = shard_producers.get_mut(shard_id) {
            let msg = crate::shard::dispatch::ShardMessage::RegisterReplica(Box::new(
                crate::shard::dispatch::RegisterReplicaPayload {
                    replica_id,
                    tx,
                    // Legacy multi-shard drain loops do not poll the kick flag
                    // (superseded by the R2 redesign); overflow still stops
                    // queueing via the fan-out's retain.
                    kicked: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                    backlog_capacity,
                    // Fire-and-forget: the multi-shard register paths are superseded
                    // by the R2 PrepareReplicaSync redesign; the offset-reply catch-up
                    // protocol is wired on the single-shard inline path only.
                    registered: None,
                    // Cross-shard registration: the target shard's offset is
                    // owned by its own thread — the arm reads it at drain.
                    push_offset: None,
                    // No snapshot body was captured on this shard's thread —
                    // the arm's drain-time offset is the correct cut.
                    cut: None,
                },
            ));
            let _ = prod.try_push(msg);
        }

        // Spawn sender task: drains channel -> writes to TCP socket
        // monoio write_all takes &mut self, so we borrow_mut() across the await.
        // This is safe because monoio is single-threaded and cooperative —
        // only one sender task runs at a time, so no concurrent borrows occur.
        let wh = Rc::clone(&shared_stream);
        #[allow(clippy::await_holding_refcell_ref)]
        monoio::spawn(async move {
            while let Ok(data) = rx.recv_async().await {
                let data_vec = data.to_vec();
                let (wr, _) = wh.borrow_mut().write_all(data_vec).await;
                if wr.is_err() {
                    info!("Replica sender shard {}: socket closed", shard_id);
                    break;
                }
            }
        });
    }

    // Register replica in ReplicationState
    let replica_info = ReplicaInfo {
        id: replica_id,
        addr,
        ack_offsets,
        shard_txs,
        last_ack_time: std::sync::atomic::AtomicU64::new(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        ),
    };
    if let Ok(mut rs) = repl_state.write() {
        rs.replicas.push(replica_info);
    }

    info!(
        "Master: replica {} registered across {} shards",
        replica_id, num_shards
    );
    Ok(())
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
        let rs = repl_state
            .read()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
        let slot = rs
            .per_shard_backlogs
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("backlog slot missing"))?;
        (rs.repl_id.clone(), rs.repl_id2.clone(), slot)
    };

    // Decide full vs partial resync against the single-shard backlog.
    let decision = if client_offset < 0 {
        PsyncDecision::FullResync
    } else if client_repl_id != repl_id && client_repl_id != repl_id2 {
        PsyncDecision::FullResync
    } else {
        let off = client_offset as u64;
        let g = backlog_slot.lock();
        if g.as_ref().is_some_and(|b| b.contains_offset(off)) {
            PsyncDecision::PartialResync { from_offset: off }
        } else {
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
                let off = repl_state
                    .read()
                    .map(|g| {
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
                        g.total_offset()
                    })
                    .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
                // Shard 0 is this thread's shard — use the thread-local slice.
                crate::shard::slice::with_shard(|s| {
                    let refs: Vec<&crate::storage::Database> = s.databases.iter().collect();
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
                    crate::persistence::redis_rdb::write_rdb_refs_with_moon_aux(
                        &refs,
                        &moon_aux,
                        &mut rdb_buf,
                    );
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
        let rs = repl_state
            .read()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
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
    let backlog_capacity = repl_state
        .read()
        .map(|g| g.backlog_capacity)
        .unwrap_or(crate::replication::state::DEFAULT_REPL_BACKLOG_SIZE);
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
        let g = repl_state
            .read()
            .map_err(|_| anyhow::anyhow!("replication state lock poisoned"))?;
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
    if let Ok(mut rs) = repl_state.write() {
        rs.replicas.push(replica_info);
    }

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
    if let Ok(mut rs) = repl_state.write() {
        rs.replicas.retain(|r| r.id != replica_id);
    }
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
            if let Ok(rs) = repl_state.read() {
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
        let rs = repl_state.read().unwrap();
        rs.total_offset()
    };

    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms.max(1));

    loop {
        let acked_count = {
            let rs = repl_state.read().unwrap();
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
