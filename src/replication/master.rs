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

    let channel_capacity = 1024;
    let mut shard_txs = Vec::with_capacity(num_shards);
    let mut ack_offsets = Vec::with_capacity(num_shards);

    for shard_id in 0..num_shards {
        let (tx, rx) = crate::runtime::channel::mpsc_bounded::<bytes::Bytes>(channel_capacity);
        shard_txs.push(tx.clone());
        ack_offsets.push(std::sync::atomic::AtomicU64::new(0));

        // Send RegisterReplica to the shard's SPSC
        if let Some(prod) = shard_producers.get_mut(shard_id) {
            let msg = crate::shard::dispatch::ShardMessage::RegisterReplica { replica_id, tx };
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

    let channel_capacity = 1024;
    let mut shard_txs = Vec::with_capacity(num_shards);
    let mut ack_offsets = Vec::with_capacity(num_shards);

    for shard_id in 0..num_shards {
        let (tx, rx) = crate::runtime::channel::mpsc_bounded::<bytes::Bytes>(channel_capacity);
        shard_txs.push(tx.clone());
        ack_offsets.push(std::sync::atomic::AtomicU64::new(0));

        // Send RegisterReplica to the shard's SPSC
        if let Some(prod) = shard_producers.get_mut(shard_id) {
            let msg = crate::shard::dispatch::ShardMessage::RegisterReplica { replica_id, tx };
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
    dispatch_tx: Rc<
        RefCell<Vec<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>>,
    >,
    replica_addr: std::net::SocketAddr,
) -> anyhow::Result<()> {
    use monoio::io::AsyncWriteRentExt;

    let (repl_id, repl_id2, current_offset, backlog_slot) = {
        let rs = repl_state
            .read()
            .map_err(|_| anyhow::anyhow!("lock poisoned"))?;
        let slot = rs
            .per_shard_backlogs
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("backlog slot missing"))?;
        (
            rs.repl_id.clone(),
            rs.repl_id2.clone(),
            rs.total_offset(),
            slot,
        )
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
            let snapshot_offset = current_offset;
            let response = format!("+FULLRESYNC {} {}\r\n", repl_id, snapshot_offset);
            let (wr, _) = stream.write_all(response.into_bytes()).await;
            wr.map_err(|e| anyhow::anyhow!(e))?;

            // Generate RDB inline by reading all databases on shard 0.
            // Hold read guards across the synchronous write to avoid any
            // Clone requirement on Database (the type intentionally is not
            // Clone — its internal DashTable + FT/graph indices are large).
            let mut rdb_buf: Vec<u8> = Vec::new();
            {
                let db_count = shard_databases.db_count();
                let mut guards = Vec::with_capacity(db_count);
                for db_idx in 0..db_count {
                    guards.push(shard_databases.read_db(0, db_idx));
                }
                let refs: Vec<&crate::storage::Database> =
                    guards.iter().map(|g| &**g).collect();
                crate::persistence::redis_rdb::write_rdb_refs(&refs, &mut rdb_buf);
            }
            let header = format!("${}\r\n", rdb_buf.len());
            let (wr, _) = stream.write_all(header.into_bytes()).await;
            wr.map_err(|e| anyhow::anyhow!(e))?;
            let (wr, _) = stream.write_all(rdb_buf).await;
            wr.map_err(|e| anyhow::anyhow!(e))?;
            // Note: standard Redis replication does NOT terminate the bulk
            // string with \r\n during diskless full resync; the next bytes are
            // backlog/replication stream. Match that wire format.

            // Stream any backlog bytes appended between snapshot capture and now.
            if let Some(bytes) = backlog_bytes_from(&backlog_slot, snapshot_offset) {
                if !bytes.is_empty() {
                    let (wr, _) = stream.write_all(bytes).await;
                    wr.map_err(|e| anyhow::anyhow!(e))?;
                }
            }

            register_replica_inline_single_shard(
                replica_addr,
                stream,
                repl_state,
                dispatch_tx,
            )
            .await?;
        }
        PsyncDecision::PartialResync { from_offset } => {
            let response = format!("+CONTINUE {}\r\n", repl_id);
            let (wr, _) = stream.write_all(response.into_bytes()).await;
            wr.map_err(|e| anyhow::anyhow!(e))?;

            if let Some(bytes) = backlog_bytes_from(&backlog_slot, from_offset) {
                if !bytes.is_empty() {
                    let (wr, _) = stream.write_all(bytes).await;
                    wr.map_err(|e| anyhow::anyhow!(e))?;
                }
            }
            register_replica_inline_single_shard(
                replica_addr,
                stream,
                repl_state,
                dispatch_tx,
            )
            .await?;
        }
    }
    Ok(())
}

/// Single-shard inline replica registration.
///
/// Creates an MPSC channel, pushes `RegisterReplica` onto shard 0's SPSC
/// producer so the event loop picks up the tx into its local `replica_txs`
/// (the authority for live write fan-out), and also records the replica in
/// `ReplicationState.replicas` for WAIT / INFO bookkeeping. Then drains the
/// channel onto the replica's socket until the peer disconnects.
#[cfg(feature = "runtime-monoio")]
#[allow(clippy::await_holding_refcell_ref)]
async fn register_replica_inline_single_shard(
    addr: std::net::SocketAddr,
    stream: monoio::net::TcpStream,
    repl_state: Arc<RwLock<ReplicationState>>,
    dispatch_tx: Rc<
        RefCell<Vec<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>>,
    >,
) -> anyhow::Result<()> {
    use monoio::io::AsyncWriteRentExt;
    use ringbuf::traits::Producer;
    use std::sync::atomic::Ordering;

    static NEXT_REPLICA_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let replica_id = NEXT_REPLICA_ID.fetch_add(1, Ordering::Relaxed);

    let (tx, rx) = crate::runtime::channel::mpsc_bounded::<bytes::Bytes>(1024);

    // Push RegisterReplica onto shard 0's SPSC so the event loop captures the
    // tx into its local replica_txs Vec — the sole authority used by
    // wal_append_and_fanout for live write streaming.
    {
        let mut prods = dispatch_tx.borrow_mut();
        if let Some(prod) = prods.get_mut(0) {
            let msg = crate::shard::dispatch::ShardMessage::RegisterReplica {
                replica_id,
                tx: tx.clone(),
            };
            if prod.try_push(msg).is_err() {
                anyhow::bail!("failed to push RegisterReplica onto shard 0 SPSC");
            }
        } else {
            anyhow::bail!("shard 0 producer missing");
        }
    }

    // Also bookkeeping for WAIT/INFO.
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

    // Drain the channel and write to the stream until the replica disconnects.
    let stream = std::cell::RefCell::new(stream);
    #[allow(clippy::await_holding_refcell_ref)]
    while let Ok(data) = rx.recv_async().await {
        let buf = data.to_vec();
        let (wr, _) = stream.borrow_mut().write_all(buf).await;
        if wr.is_err() {
            info!("Replica {} disconnected", replica_id);
            break;
        }
    }
    // Remove from ReplicationState; the event loop will drop its replica_txs
    // entry on the next failed send via its own UnregisterReplica path.
    if let Ok(mut rs) = repl_state.write() {
        rs.replicas.retain(|r| r.id != replica_id);
    }
    {
        let mut prods = dispatch_tx.borrow_mut();
        if let Some(prod) = prods.get_mut(0) {
            let _ = prod.try_push(
                crate::shard::dispatch::ShardMessage::UnregisterReplica { replica_id },
            );
        }
    }
    Ok(())
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
