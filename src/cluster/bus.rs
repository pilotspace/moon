//! Cluster bus TCP listener on port + 10000.
//!
//! Each peer connection runs a request-response gossip loop:
//! peer sends PING (len-prefixed), we respond with PONG.
//! All I/O is on the listener runtime, never on shard threads.
#![allow(unused_imports)]

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

#[cfg(feature = "runtime-tokio")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(feature = "runtime-tokio")]
use tokio::net::{TcpListener, TcpStream};
use crate::runtime::cancel::CancellationToken;
use tracing::{debug, info, warn};

use crate::cluster::gossip::{
    build_message, deserialize_gossip, merge_gossip_into_state, serialize_gossip, GossipMsgType,
};
use crate::cluster::ClusterState;

/// Shared vote sender: set by gossip ticker when election starts, cleared when election ends.
/// Bus handler forwards FailoverAuthAck votes through this channel.
pub type SharedVoteTx =
    Arc<parking_lot::Mutex<Option<crate::runtime::channel::MpscSender<String>>>>;

/// Run the cluster bus listener loop.
///
/// Spawns a new task for each incoming peer connection.
/// Should be spawned on the listener runtime as a separate task.
#[cfg(feature = "runtime-tokio")]
pub async fn run_cluster_bus(
    bind: &str,
    cluster_port: u16,
    self_addr: SocketAddr,
    cluster_state: Arc<RwLock<ClusterState>>,
    shutdown: CancellationToken,
    vote_tx: SharedVoteTx,
) -> anyhow::Result<()> {
    let addr = format!("{}:{}", bind, cluster_port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Cluster bus listening on {}", addr);

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, peer_addr)) => {
                        let cs = cluster_state.clone();
                        let tok = shutdown.child_token();
                        let sa = self_addr;
                        let vtx = vote_tx.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_cluster_peer(stream, peer_addr, sa, cs, tok, vtx).await {
                                debug!("Cluster peer {} error: {}", peer_addr, e);
                            }
                        });
                    }
                    Err(e) => {
                        warn!("Cluster bus accept error: {}", e);
                    }
                }
            }
            _ = shutdown.cancelled() => {
                info!("Cluster bus shutting down");
                break;
            }
        }
    }
    Ok(())
}

/// Handle a single cluster peer connection.
///
/// Reads length-prefixed gossip messages in a loop.
/// For PING/MEET: responds with PONG.
/// For PONG: merges into our state.
#[cfg(feature = "runtime-tokio")]
async fn handle_cluster_peer(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    self_addr: SocketAddr,
    cluster_state: Arc<RwLock<ClusterState>>,
    shutdown: CancellationToken,
    vote_tx: SharedVoteTx,
) -> anyhow::Result<()> {
    loop {
        // Read 4-byte length prefix
        let mut len_buf = [0u8; 4];
        tokio::select! {
            result = stream.read_exact(&mut len_buf) => {
                result?;
            }
            _ = shutdown.cancelled() => return Ok(()),
        }
        let msg_len = u32::from_be_bytes(len_buf) as usize;
        if msg_len > 64 * 1024 {
            anyhow::bail!("gossip message too large: {} bytes", msg_len);
        }

        // Read message body
        let mut buf = vec![0u8; msg_len];
        stream.read_exact(&mut buf).await?;

        // Deserialize
        let msg = match deserialize_gossip(&buf) {
            Ok(m) => m,
            Err(e) => {
                warn!("Bad gossip from {}: {}", peer_addr, e);
                continue;
            }
        };

        match msg.msg_type {
            GossipMsgType::Ping | GossipMsgType::Meet => {
                // Merge their state into ours
                {
                    let mut cs = cluster_state.write().unwrap();
                    merge_gossip_into_state(&mut cs, &msg);
                }
                // Respond with PONG
                let pong = {
                    let cs = cluster_state.read().unwrap();
                    build_message(&cs, self_addr, GossipMsgType::Pong)
                };
                let pong_bytes = serialize_gossip(&pong);
                let len = (pong_bytes.len() as u32).to_be_bytes();
                stream.write_all(&len).await?;
                stream.write_all(&pong_bytes).await?;
            }
            GossipMsgType::Pong => {
                let mut cs = cluster_state.write().unwrap();
                merge_gossip_into_state(&mut cs, &msg);
            }
            GossipMsgType::FailoverAuthRequest => {
                let sender_id = std::str::from_utf8(&msg.sender_node_id)
                    .unwrap_or("")
                    .trim_end_matches('\0')
                    .to_string();
                let request_epoch = msg.config_epoch;
                let voted = {
                    let mut cs = cluster_state.write().unwrap();
                    crate::cluster::failover::handle_failover_auth_request(
                        &mut cs,
                        &sender_id,
                        request_epoch,
                    )
                };
                if voted {
                    // Send FailoverAuthAck back to the requesting replica
                    let ack = {
                        let cs = cluster_state.read().unwrap();
                        build_message(&cs, self_addr, GossipMsgType::FailoverAuthAck)
                    };
                    let ack_bytes = serialize_gossip(&ack);
                    let len = (ack_bytes.len() as u32).to_be_bytes();
                    let _ = stream.write_all(&len).await;
                    let _ = stream.write_all(&ack_bytes).await;
                }
            }
            GossipMsgType::FailoverAuthAck => {
                let sender_id = std::str::from_utf8(&msg.sender_node_id)
                    .unwrap_or("")
                    .trim_end_matches('\0')
                    .to_string();
                debug!("Received failover ACK from {}", sender_id);
                if let Some(tx) = vote_tx.lock().as_ref() {
                    let _ = tx.send(sender_id);
                }
            }
        }
    }
}
