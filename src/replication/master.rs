//! Master-side PSYNC2 handler and WAIT command support.
//!
//! Provides `handle_psync_on_master` for incoming PSYNC connections
//! and `wait_for_replicas` for the WAIT command.

use std::sync::{Arc, RwLock};

#[cfg(feature = "runtime-tokio")]
use tokio::io::AsyncWriteExt;
#[cfg(feature = "runtime-tokio")]
use tokio::net::tcp::OwnedWriteHalf;
use tracing::info;

use crate::replication::backlog::ReplicationBacklog;
use crate::replication::handshake::{evaluate_psync, PsyncDecision};
use crate::replication::state::{ReplicaInfo, ReplicationState};

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
pub async fn handle_psync_on_master(
    client_repl_id: &str,
    client_offset: i64,
    mut write_half: OwnedWriteHalf,
    repl_state: Arc<RwLock<ReplicationState>>,
    per_shard_backlogs: &[ReplicationBacklog],
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

    let decision = evaluate_psync(
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
            let mut snap_rxs: Vec<crate::runtime::channel::OneshotReceiver<Result<(), String>>> = Vec::new();

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

            // Transfer per-shard RDB files
            for shard_id in 0..num_shards {
                let snap_path = snap_dir.join(format!("shard-{}.rrdshard", shard_id));
                let data = std::fs::read(&snap_path).unwrap_or_default();
                let header = format!("${}\r\n", data.len());
                write_half.write_all(header.as_bytes()).await?;
                write_half.write_all(&data).await?;
                write_half.write_all(b"\r\n").await?;
                info!(
                    "Master: sent shard {} RDB ({} bytes)",
                    shard_id,
                    data.len()
                );
            }

            // Stream backlog bytes accumulated since snapshot_start_offset
            for (shard_id, backlog) in per_shard_backlogs.iter().enumerate() {
                if let Some(bytes) = backlog.bytes_from(snapshot_start_offset) {
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
                if let Some(bytes) = backlog.bytes_from(from_offset) {
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

    static NEXT_REPLICA_ID: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(1);
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
            let msg = crate::shard::dispatch::ShardMessage::RegisterReplica {
                replica_id,
                tx,
            };
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

    let deadline =
        std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms.max(1));

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
