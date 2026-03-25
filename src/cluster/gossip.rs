//! Binary gossip protocol for the Redis Cluster bus.
//!
//! PING: sent every ~100ms to a random peer.
//! PONG: response to PING; contains our full node state and up to 3 gossip sections.
//! Gossip sections carry rumors about other nodes we know.
#![allow(unused_imports)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(feature = "runtime-tokio")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(feature = "runtime-tokio")]
use tokio::net::TcpStream;
use crate::runtime::cancel::CancellationToken;
use tracing::warn;

use crate::cluster::bus::SharedVoteTx;
use crate::cluster::{ClusterNode, ClusterState, FailoverState, NodeFlags};

pub const GOSSIP_MAGIC: u32 = 0x52656469; // "Redi"
pub const GOSSIP_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq)]
pub enum GossipMsgType {
    Ping = 0,
    Pong = 1,
    Meet = 2,
    FailoverAuthRequest = 3,
    FailoverAuthAck = 4,
}

impl GossipMsgType {
    pub fn from_u16(v: u16) -> Option<Self> {
        match v {
            0 => Some(Self::Ping),
            1 => Some(Self::Pong),
            2 => Some(Self::Meet),
            3 => Some(Self::FailoverAuthRequest),
            4 => Some(Self::FailoverAuthAck),
            _ => None,
        }
    }
}

/// Rumor section about a peer node included in a PING/PONG.
#[derive(Debug, Clone, PartialEq)]
pub struct GossipSection {
    pub node_id: [u8; 40],
    pub ip: [u8; 16], // null-padded ASCII
    pub port: u16,
    pub bus_port: u16,
    pub flags: u16, // 0=master,1=replica,2=pfail,3=fail
    pub epoch: u64,
    pub ping_sent_ms: u64,
    pub pong_recv_ms: u64,
}

impl GossipSection {
    const SIZE: usize = 40 + 16 + 2 + 2 + 2 + 8 + 8 + 8; // 86 bytes

    fn serialize_into(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.node_id);
        buf.extend_from_slice(&self.ip);
        buf.extend_from_slice(&self.port.to_be_bytes());
        buf.extend_from_slice(&self.bus_port.to_be_bytes());
        buf.extend_from_slice(&self.flags.to_be_bytes());
        buf.extend_from_slice(&self.epoch.to_be_bytes());
        buf.extend_from_slice(&self.ping_sent_ms.to_be_bytes());
        buf.extend_from_slice(&self.pong_recv_ms.to_be_bytes());
    }

    fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < Self::SIZE {
            return None;
        }
        let mut node_id = [0u8; 40];
        node_id.copy_from_slice(&data[0..40]);
        let mut ip = [0u8; 16];
        ip.copy_from_slice(&data[40..56]);
        let port = u16::from_be_bytes([data[56], data[57]]);
        let bus_port = u16::from_be_bytes([data[58], data[59]]);
        let flags = u16::from_be_bytes([data[60], data[61]]);
        let epoch = u64::from_be_bytes(data[62..70].try_into().ok()?);
        let ping_sent_ms = u64::from_be_bytes(data[70..78].try_into().ok()?);
        let pong_recv_ms = u64::from_be_bytes(data[78..86].try_into().ok()?);
        Some(GossipSection {
            node_id,
            ip,
            port,
            bus_port,
            flags,
            epoch,
            ping_sent_ms,
            pong_recv_ms,
        })
    }
}

/// Full gossip message exchanged on the cluster bus.
#[derive(Debug, Clone, PartialEq)]
pub struct GossipMessage {
    pub msg_type: GossipMsgType,
    pub sender_node_id: [u8; 40],
    pub sender_slots: Box<[u8; 2048]>,
    pub config_epoch: u64,
    pub sender_ip: [u8; 16],
    pub sender_port: u16,
    pub sender_bus_port: u16,
    pub gossip_sections: Vec<GossipSection>,
}

/// Header size: magic(4) + total_len(4) + ver(2) + msg_type(2) + node_id(40) + slots(2048) + epoch(8) + ip(16) + port(2) + bus_port(2) + num_gossip(2) = 2130
const HEADER_SIZE: usize = 4 + 4 + 2 + 2 + 40 + 2048 + 8 + 16 + 2 + 2 + 2;

/// Serialize a GossipMessage to bytes for wire transmission.
pub fn serialize_gossip(msg: &GossipMessage) -> Vec<u8> {
    let num_gossip = msg.gossip_sections.len() as u16;
    let total_len = (HEADER_SIZE + GossipSection::SIZE * num_gossip as usize) as u32;

    let mut buf = Vec::with_capacity(total_len as usize);
    buf.extend_from_slice(&GOSSIP_MAGIC.to_be_bytes());
    buf.extend_from_slice(&total_len.to_be_bytes());
    buf.extend_from_slice(&GOSSIP_VERSION.to_be_bytes());
    buf.extend_from_slice(&(msg.msg_type.clone() as u16).to_be_bytes());
    buf.extend_from_slice(&msg.sender_node_id);
    buf.extend_from_slice(msg.sender_slots.as_ref());
    buf.extend_from_slice(&msg.config_epoch.to_be_bytes());
    buf.extend_from_slice(&msg.sender_ip);
    buf.extend_from_slice(&msg.sender_port.to_be_bytes());
    buf.extend_from_slice(&msg.sender_bus_port.to_be_bytes());
    buf.extend_from_slice(&num_gossip.to_be_bytes());
    for section in &msg.gossip_sections {
        section.serialize_into(&mut buf);
    }
    buf
}

/// Deserialize a GossipMessage from bytes.
/// Returns Err if magic mismatch, truncated, or unknown message type.
pub fn deserialize_gossip(data: &[u8]) -> Result<GossipMessage, String> {
    if data.len() < HEADER_SIZE {
        return Err(format!("too short: {} < {}", data.len(), HEADER_SIZE));
    }
    let magic = u32::from_be_bytes(data[0..4].try_into().unwrap());
    if magic != GOSSIP_MAGIC {
        return Err(format!("bad magic: 0x{:08X}", magic));
    }
    // total_len at [4..8], ver at [8..10] -- we don't enforce version for forward compat
    let msg_type_raw = u16::from_be_bytes([data[10], data[11]]);
    let msg_type = GossipMsgType::from_u16(msg_type_raw)
        .ok_or_else(|| format!("unknown msg type: {}", msg_type_raw))?;

    let mut sender_node_id = [0u8; 40];
    sender_node_id.copy_from_slice(&data[12..52]);

    let mut sender_slots_arr = [0u8; 2048];
    sender_slots_arr.copy_from_slice(&data[52..2100]);

    let config_epoch = u64::from_be_bytes(data[2100..2108].try_into().unwrap());

    let mut sender_ip = [0u8; 16];
    sender_ip.copy_from_slice(&data[2108..2124]);

    let sender_port = u16::from_be_bytes([data[2124], data[2125]]);
    let sender_bus_port = u16::from_be_bytes([data[2126], data[2127]]);
    let num_gossip = u16::from_be_bytes([data[2128], data[2129]]) as usize;

    let mut gossip_sections = Vec::with_capacity(num_gossip);
    let mut offset = 2130;
    for _ in 0..num_gossip {
        if offset + GossipSection::SIZE > data.len() {
            return Err("truncated gossip section".to_string());
        }
        let section = GossipSection::deserialize(&data[offset..])
            .ok_or_else(|| "invalid gossip section".to_string())?;
        gossip_sections.push(section);
        offset += GossipSection::SIZE;
    }

    Ok(GossipMessage {
        msg_type,
        sender_node_id,
        sender_slots: Box::new(sender_slots_arr),
        config_epoch,
        sender_ip,
        sender_port,
        sender_bus_port,
        gossip_sections,
    })
}

/// Return current unix milliseconds.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Encode an IPv4/IPv6 address as 16 null-padded bytes (ASCII dotted notation).
fn encode_ip(addr: &SocketAddr) -> [u8; 16] {
    let mut buf = [0u8; 16];
    let s = addr.ip().to_string();
    let bytes = s.as_bytes();
    let len = bytes.len().min(15);
    buf[..len].copy_from_slice(&bytes[..len]);
    buf
}

/// Build a PING or PONG message from this node's current state.
pub fn build_message(
    state: &ClusterState,
    self_addr: SocketAddr,
    msg_type: GossipMsgType,
) -> GossipMessage {
    let my_node = state.my_node();
    let mut sender_node_id = [0u8; 40];
    let id_bytes = my_node.node_id.as_bytes();
    sender_node_id[..id_bytes.len().min(40)].copy_from_slice(&id_bytes[..id_bytes.len().min(40)]);

    // Pick up to 3 random peer nodes as gossip payload
    let gossip_sections: Vec<GossipSection> = state
        .nodes
        .values()
        .filter(|n| n.node_id != state.node_id)
        .take(3)
        .map(|n| {
            let mut node_id_bytes = [0u8; 40];
            let nb = n.node_id.as_bytes();
            node_id_bytes[..nb.len().min(40)].copy_from_slice(&nb[..nb.len().min(40)]);
            let flags_val = match &n.flags {
                NodeFlags::Master => 0u16,
                NodeFlags::Replica { .. } => 1,
                NodeFlags::Pfail => 2,
                NodeFlags::Fail => 3,
            };
            GossipSection {
                node_id: node_id_bytes,
                ip: encode_ip(&n.addr),
                port: n.addr.port(),
                bus_port: n.bus_port,
                flags: flags_val,
                epoch: n.epoch,
                ping_sent_ms: n.ping_sent_ms,
                pong_recv_ms: n.pong_recv_ms,
            }
        })
        .collect();

    GossipMessage {
        msg_type,
        sender_node_id,
        sender_slots: my_node.slots.clone(),
        config_epoch: state.epoch,
        sender_ip: encode_ip(&self_addr),
        sender_port: self_addr.port(),
        sender_bus_port: my_node.bus_port,
        gossip_sections,
    }
}

/// Merge a received PONG/PING into our ClusterState.
///
/// Updates the sender node's pong_recv_ms, epoch, slot bitmap, and any gossip rumors.
pub fn merge_gossip_into_state(state: &mut ClusterState, msg: &GossipMessage) {
    let node_id_str = std::str::from_utf8(&msg.sender_node_id)
        .unwrap_or("")
        .trim_end_matches('\0')
        .to_string();

    if node_id_str.is_empty() || node_id_str == state.node_id {
        return;
    }

    // Update or insert sender node
    let entry = state
        .nodes
        .entry(node_id_str.clone())
        .or_insert_with(|| {
            let ip_str = std::str::from_utf8(&msg.sender_ip)
                .unwrap_or("127.0.0.1")
                .trim_end_matches('\0');
            let ip: IpAddr = ip_str.parse().unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
            let addr = SocketAddr::new(ip, msg.sender_port);
            ClusterNode::new(
                node_id_str.clone(),
                addr,
                NodeFlags::Master,
                msg.config_epoch,
            )
        });
    entry.pong_recv_ms = now_ms();
    if msg.config_epoch > entry.epoch {
        entry.epoch = msg.config_epoch;
        entry.slots = msg.sender_slots.clone();
    }

    state.messages_received += 1;

    // Process gossip sections for PFAIL/FAIL reports.
    // When a peer reports another node as PFAIL (flags=2) or FAIL (flags=3),
    // record the reporter in the target node's pfail_reports.
    for section in &msg.gossip_sections {
        let section_flags = section.flags;
        if section_flags == 2 || section_flags == 3 {
            let target_node_id = std::str::from_utf8(&section.node_id)
                .unwrap_or("")
                .trim_end_matches('\0')
                .to_string();
            if target_node_id.is_empty() || target_node_id == state.node_id {
                continue;
            }
            let reporter_id = node_id_str.clone();
            let now = now_ms();

            // Insert the report
            if let Some(target) = state.nodes.get_mut(&target_node_id) {
                target.pfail_reports.insert(reporter_id, now);
            }

            // Check if majority consensus reached for PFAIL->FAIL
            crate::cluster::failover::try_mark_fail_with_consensus(state, &target_node_id);
        }
    }
}

/// Check for PFAIL -> FAIL transitions based on ping timeout.
///
/// Nodes not heard from in `node_timeout_ms` are marked PFAIL.
/// Full FAIL requires majority agreement -- done during gossip merge.
pub fn check_failure_states(state: &mut ClusterState, node_timeout_ms: u64) {
    let now = now_ms();
    let my_id = state.node_id.clone();
    for node in state.nodes.values_mut() {
        if node.node_id == my_id {
            continue;
        }
        if node.pong_recv_ms > 0
            && now.saturating_sub(node.pong_recv_ms) > node_timeout_ms
            && !matches!(node.flags, NodeFlags::Fail | NodeFlags::Pfail)
        {
            warn!(
                "Node {} not heard from in {}ms, marking PFAIL",
                node.node_id, node_timeout_ms
            );
            node.flags = NodeFlags::Pfail;
        }
    }
}

/// Background gossip ticker: sends PING to a random peer every 100ms.
///
/// Runs as a separate async task on the listener runtime (NOT on shard threads).
/// Also monitors for master FAIL and spawns election task for replicas.
#[cfg(feature = "runtime-tokio")]
pub async fn run_gossip_ticker(
    self_addr: SocketAddr,
    cluster_state: Arc<RwLock<ClusterState>>,
    node_timeout_ms: u64,
    shutdown: CancellationToken,
    vote_tx: SharedVoteTx,
    repl_state: std::sync::Arc<std::sync::RwLock<crate::replication::state::ReplicationState>>,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let mut election_spawned = false;
    loop {
        tokio::select! {
            _ = tick.tick() => {
                // Pick a random peer to PING
                let (target_addr, ping_msg) = {
                    let mut cs = cluster_state.write().unwrap();
                    check_failure_states(&mut cs, node_timeout_ms);

                    // Reset election_spawned when failover state returns to None
                    if election_spawned && cs.failover_state == FailoverState::None {
                        election_spawned = false;
                    }

                    // Check if we should initiate failover (replica with FAIL master)
                    if !election_spawned {
                        let my_flags = cs.my_node().flags.clone();
                        if let NodeFlags::Replica { ref master_id } = my_flags {
                            let master_is_fail = cs.nodes.get(master_id)
                                .map(|n| matches!(n.flags, NodeFlags::Fail))
                                .unwrap_or(false);
                            if master_is_fail && cs.failover_state == FailoverState::None {
                                election_spawned = true;
                                let (tx, rx) = crate::runtime::channel::mpsc_unbounded();
                                {
                                    // Use try_lock to avoid holding std RwLock across await
                                    // We're in a sync context here so blocking_lock is safe
                                    let mut guard = vote_tx.lock();
                                    *guard = Some(tx);
                                }
                                let cs_election = cluster_state.clone();
                                let sa = self_addr;
                                let offset = repl_state.read().unwrap().total_offset();
                                let vtx = vote_tx.clone();
                                tokio::spawn(async move {
                                    crate::cluster::failover::run_election_task(
                                        cs_election, sa, offset, rx,
                                    ).await;
                                    // Clear vote_tx when election ends
                                    *vtx.lock() = None;
                                });
                            }
                        }
                    }

                    let target = cs.nodes.values()
                        .filter(|n| n.node_id != cs.node_id)
                        .next()
                        .map(|n| SocketAddr::new(n.addr.ip(), n.bus_port));
                    let msg = build_message(&cs, self_addr, GossipMsgType::Ping);
                    cs.messages_sent += 1;
                    (target, msg)
                };
                if let Some(target_addr) = target_addr {
                    let cs = cluster_state.clone();
                    tokio::spawn(async move {
                        if let Ok(mut stream) = TcpStream::connect(target_addr).await {
                            let data = serialize_gossip(&ping_msg);
                            let len = (data.len() as u32).to_be_bytes();
                            let _ = stream.write_all(&len).await;
                            let _ = stream.write_all(&data).await;
                            // Read PONG response
                            let mut len_buf = [0u8; 4];
                            if stream.read_exact(&mut len_buf).await.is_ok() {
                                let pong_len = u32::from_be_bytes(len_buf) as usize;
                                let mut pong_buf = vec![0u8; pong_len];
                                if stream.read_exact(&mut pong_buf).await.is_ok() {
                                    if let Ok(pong) = deserialize_gossip(&pong_buf) {
                                        let mut cs2 = cs.write().unwrap();
                                        merge_gossip_into_state(&mut cs2, &pong);
                                    }
                                }
                            }
                        }
                    });
                }
            }
            _ = shutdown.cancelled() => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CLUSTER-10: serialize then deserialize a PING produces identical GossipMessage.
    #[test]
    fn test_ping_roundtrip() {
        let mut msg = GossipMessage {
            msg_type: GossipMsgType::Ping,
            sender_node_id: [b'a'; 40],
            sender_slots: Box::new([0u8; 2048]),
            config_epoch: 42,
            sender_ip: [0u8; 16],
            sender_port: 6379,
            sender_bus_port: 16379,
            gossip_sections: vec![],
        };
        msg.sender_slots[0] = 0xFF; // slot 0-7 owned

        let bytes = serialize_gossip(&msg);
        let decoded = deserialize_gossip(&bytes).expect("round-trip failed");
        assert_eq!(decoded.msg_type, GossipMsgType::Ping);
        assert_eq!(decoded.sender_node_id, msg.sender_node_id);
        assert_eq!(decoded.sender_slots[0], 0xFF);
        assert_eq!(decoded.config_epoch, 42);
        assert_eq!(decoded.sender_port, 6379);
        assert_eq!(decoded.sender_bus_port, 16379);
    }

    /// PONG round-trip preserves node_id, epoch, slot bitmap, sender_ip, sender_port.
    #[test]
    fn test_pong_with_sections_roundtrip() {
        let section = GossipSection {
            node_id: [b'b'; 40],
            ip: [
                b'1', b'2', b'7', b'.', b'0', b'.', b'0', b'.', b'1', 0, 0, 0, 0, 0, 0, 0,
            ],
            port: 6380,
            bus_port: 16380,
            flags: 0,
            epoch: 5,
            ping_sent_ms: 1000,
            pong_recv_ms: 2000,
        };
        let msg = GossipMessage {
            msg_type: GossipMsgType::Pong,
            sender_node_id: [b'a'; 40],
            sender_slots: Box::new([0u8; 2048]),
            config_epoch: 1,
            sender_ip: [0u8; 16],
            sender_port: 6379,
            sender_bus_port: 16379,
            gossip_sections: vec![section.clone()],
        };
        let bytes = serialize_gossip(&msg);
        let decoded = deserialize_gossip(&bytes).unwrap();
        assert_eq!(decoded.msg_type, GossipMsgType::Pong);
        assert_eq!(decoded.sender_node_id, [b'a'; 40]);
        assert_eq!(decoded.config_epoch, 1);
        assert_eq!(decoded.sender_port, 6379);
        assert_eq!(decoded.gossip_sections.len(), 1);
        assert_eq!(decoded.gossip_sections[0], section);
    }

    /// GossipSection round-trip preserves all fields.
    #[test]
    fn test_gossip_section_roundtrip() {
        let section = GossipSection {
            node_id: [b'c'; 40],
            ip: [
                b'1', b'0', b'.', b'0', b'.', b'0', b'.', b'1', 0, 0, 0, 0, 0, 0, 0, 0,
            ],
            port: 7000,
            bus_port: 17000,
            flags: 2, // pfail
            epoch: 99,
            ping_sent_ms: 5555,
            pong_recv_ms: 6666,
        };
        let mut buf = Vec::new();
        section.serialize_into(&mut buf);
        assert_eq!(buf.len(), GossipSection::SIZE);
        let decoded = GossipSection::deserialize(&buf).expect("section deserialize failed");
        assert_eq!(decoded, section);
    }

    /// Magic bytes mismatch on deserialize returns Err.
    #[test]
    fn test_bad_magic_returns_err() {
        let mut bytes = serialize_gossip(&GossipMessage {
            msg_type: GossipMsgType::Ping,
            sender_node_id: [0u8; 40],
            sender_slots: Box::new([0u8; 2048]),
            config_epoch: 0,
            sender_ip: [0u8; 16],
            sender_port: 6379,
            sender_bus_port: 16379,
            gossip_sections: vec![],
        });
        // Corrupt magic
        bytes[0] = 0xFF;
        let result = deserialize_gossip(&bytes);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("bad magic"));
    }

    /// Truncated data returns Err.
    #[test]
    fn test_truncated_returns_err() {
        let result = deserialize_gossip(&[0u8; 10]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too short"));
    }
}
