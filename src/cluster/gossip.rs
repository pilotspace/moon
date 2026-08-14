//! Binary gossip protocol for the Redis Cluster bus.
//!
//! PING: sent every ~100ms to a random peer.
//! PONG: response to PING; contains our full node state and up to 3 gossip sections.
//! Gossip sections carry rumors about other nodes we know.
#![allow(unused_imports)]

use parking_lot::RwLock;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::runtime::cancel::CancellationToken;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::warn;

use crate::cluster::bus::SharedVoteTx;
use crate::cluster::{ClusterNode, ClusterState, FailoverState, NodeHealth, NodeRole};

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

/// Pack role + health into the gossip `flags` word.
///
/// bit 0   : 0 = master, 1 = replica
/// bits 1-2: 0 = online, 1 = pfail, 2 = fail
///
/// Both axes travel together; the previous scheme could express only one, so a
/// PFAIL rumor about a replica erased the fact that it was a replica.
pub fn gossip_flags(role: &crate::cluster::NodeRole, health: crate::cluster::NodeHealth) -> u16 {
    let role_bit = match role {
        crate::cluster::NodeRole::Master => 0u16,
        crate::cluster::NodeRole::Replica { .. } => 1,
    };
    let health_bits = match health {
        crate::cluster::NodeHealth::Online => 0u16,
        crate::cluster::NodeHealth::Pfail => 1,
        crate::cluster::NodeHealth::Fail => 2,
    };
    role_bit | (health_bits << 1)
}

/// Unpack the gossip `flags` word. Unknown health bits read as Online — a
/// forward-compatible peer must never be treated as failed by accident.
pub fn parse_gossip_flags(flags: u16) -> (bool, crate::cluster::NodeHealth) {
    let is_replica = flags & 1 != 0;
    let health = match (flags >> 1) & 0b11 {
        1 => crate::cluster::NodeHealth::Pfail,
        2 => crate::cluster::NodeHealth::Fail,
        _ => crate::cluster::NodeHealth::Online,
    };
    (is_replica, health)
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
pub(crate) fn now_ms() -> u64 {
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
            // Two orthogonal axes packed into the existing u16, so the wire
            // header keeps its fixed 2130-byte size: bit 0 is the role, bits
            // 1-2 the health. The old encoding (0=master 1=replica 2=pfail
            // 3=fail) could only carry ONE of them, so gossiping a failed
            // master told the receiver nothing about its role.
            let flags_val = gossip_flags(&n.role, n.health);
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

    let sender_addr = {
        let ip_str = std::str::from_utf8(&msg.sender_ip)
            .unwrap_or("127.0.0.1")
            .trim_end_matches('\0');
        let ip: IpAddr = ip_str.parse().unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
        SocketAddr::new(ip, msg.sender_port)
    };

    // Update or insert sender node
    let entry = state.nodes.entry(node_id_str.clone()).or_insert_with(|| {
        ClusterNode::new(
            node_id_str.clone(),
            sender_addr,
            NodeRole::Master,
            msg.config_epoch,
        )
    });
    entry.pong_recv_ms = now_ms();

    // We just heard from this node DIRECTLY, so any suspicion about it is
    // stale. Nothing else clears health: `check_failure_states` only ever sets
    // PFAIL, so without this a node that recovered stayed suspected forever and
    // its slots never counted as covered again.
    entry.health = crate::cluster::NodeHealth::Online;
    entry.pfail_reports.clear();

    // A node is AUTHORITATIVE for its own slot bitmap at EQUAL epoch, not only
    // at a strictly higher one.
    //
    // The `>` here was the root cause of #485: `CLUSTER ADDSLOTS` never bumps
    // the config epoch, so a hand-built cluster sits at epoch 0 forever,
    // `0 > 0` is false, and peer ownership NEVER merged. Every node then
    // believed it owned everything it had been given and nothing else existed,
    // so `route_slot` found no owner for a peer's slot and served the key
    // locally — a write accepted by the wrong node, invisible to the right one.
    //
    // A strictly LOWER epoch is still ignored, which preserves Redis's
    // "highest epoch wins" tie-break for genuinely conflicting claims.
    if msg.config_epoch >= entry.epoch {
        entry.epoch = msg.config_epoch;
        entry.slots = msg.sender_slots.clone();
    }

    // C-1 formation fix: CLUSTER MEET registers the peer under a RANDOM
    // placeholder id ("replaced by the handshake") — but the handshake lands
    // HERE under the peer's real id, so the placeholder must die now or every
    // met peer is double-counted forever (observed as known_nodes 5 in a
    // 3-node cluster). The same rule retires a stale entry for a node that
    // restarted at the same address under a fresh id.
    let self_id = state.node_id.clone();
    state
        .nodes
        .retain(|id, n| *id == node_id_str || *id == self_id || n.addr != sender_addr);

    state.messages_received += 1;

    // Process gossip sections. Failure rumors (flags=2 PFAIL / flags=3 FAIL)
    // feed the pfail consensus; healthy rumors teach us nodes we have never
    // talked to directly — without that, two nodes MEET-ed into a third never
    // learn about EACH OTHER and the mesh cannot complete.
    let my_addr = state.nodes.get(&self_id).map(|n| n.addr);
    for section in &msg.gossip_sections {
        let (_rumor_is_replica, rumor_health) = parse_gossip_flags(section.flags);
        let target_node_id = std::str::from_utf8(&section.node_id)
            .unwrap_or("")
            .trim_end_matches('\0')
            .to_string();
        if target_node_id.is_empty() || target_node_id == state.node_id {
            continue;
        }
        // Any non-Online rumor feeds the PFAIL consensus. Read through
        // `parse_gossip_flags` rather than comparing the raw word: the flags
        // now pack role AND health, so the old `== 2 || == 3` test would read a
        // suspected REPLICA (0b011) as a confirmed failure.
        if rumor_health != crate::cluster::NodeHealth::Online {
            let reporter_id = node_id_str.clone();
            let now = now_ms();

            // Insert the report
            if let Some(target) = state.nodes.get_mut(&target_node_id) {
                target.pfail_reports.insert(reporter_id, now);
            }

            // Check if majority consensus reached for PFAIL->FAIL
            crate::cluster::failover::try_mark_fail_with_consensus(state, &target_node_id);
        } else if !state.nodes.contains_key(&target_node_id) {
            // C-1 formation fix: adopt healthy unknown nodes from rumors.
            // Skip rumors without a usable address, rumors pointing at our
            // own address (a peer's not-yet-resolved MEET placeholder for
            // us), and addresses we already know under another id (the real
            // entry wins; placeholder rumors die out on direct contact).
            let ip_str = std::str::from_utf8(&section.ip)
                .unwrap_or("")
                .trim_end_matches('\0');
            let Ok(ip) = ip_str.parse::<IpAddr>() else {
                continue;
            };
            let addr = SocketAddr::new(ip, section.port);
            if section.port == 0
                || Some(addr) == my_addr
                || state.nodes.values().any(|n| n.addr == addr)
            {
                continue;
            }
            // Flags from rumors are advisory; the node starts as Master and
            // direct contact refreshes liveness (replica linkage propagates
            // via the failover path, not rumors).
            let mut node = ClusterNode::new(
                target_node_id.clone(),
                addr,
                NodeRole::Master,
                section.epoch,
            );
            node.bus_port = section.bus_port;
            // Freshness baseline: `check_failure_states` skips entries with
            // pong_recv_ms == 0, so a rumored node that dies before our
            // first direct contact would otherwise NEVER go PFAIL on this
            // node (observed as one survivor permanently not flagging a
            // killed peer). Adoption time starts the staleness clock.
            node.pong_recv_ms = now_ms();
            state.nodes.insert(target_node_id, node);
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
            && !matches!(node.health, NodeHealth::Fail | NodeHealth::Pfail)
        {
            warn!(
                "Node {} not heard from in {}ms, marking PFAIL",
                node.node_id, node_timeout_ms
            );
            node.health = NodeHealth::Pfail;
        }
    }
}

/// Background gossip ticker: sends PING to a random peer every 100ms.
///
/// Runs as a separate async task on the listener runtime (NOT on shard threads).
/// Also monitors for master FAIL and spawns election task for replicas.
pub async fn run_gossip_ticker(
    self_addr: SocketAddr,
    cluster_state: Arc<RwLock<ClusterState>>,
    node_timeout_ms: u64,
    shutdown: CancellationToken,
    vote_tx: SharedVoteTx,
    repl_state: std::sync::Arc<parking_lot::RwLock<crate::replication::state::ReplicationState>>,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let mut election_spawned = false;
    // Bound outstanding PING probes: a dead/partitioned peer must not leak a
    // new connect+read task every 100ms. `ping_in_flight` allows at most one
    // outstanding probe at a time; `ping_rotation` spreads probes across peers
    // (the "random peer" the doc promised — the old code always picked the
    // first non-self node).
    let ping_in_flight = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut ping_rotation: usize = 0;
    loop {
        tokio::select! {
            _ = tick.tick() => {
                // Pick a random peer to PING
                let (target_addr, ping_msg) = {
                    let mut cs = cluster_state.write();
                    check_failure_states(&mut cs, node_timeout_ms);

                    // Reset election_spawned when failover state returns to None
                    if election_spawned && cs.failover_state == FailoverState::None {
                        election_spawned = false;
                    }

                    // Check if we should initiate failover (replica with FAIL master)
                    if !election_spawned {
                        let my_flags = cs.my_node().role.clone();
                        if let NodeRole::Replica { ref master_id } = my_flags {
                            let master_is_fail = cs.nodes.get(master_id)
                                .map(|n| matches!(n.health, NodeHealth::Fail))
                                .unwrap_or(false);
                            if master_is_fail && cs.failover_state == FailoverState::None {
                                election_spawned = true;
                                let (tx, rx) = crate::runtime::channel::mpsc_unbounded();
                                {
                                    // Use try_lock to avoid holding std RwLock across await
                                    // We're in a sync context here so blocking_lock is safe
                                    let mut guard = vote_tx.lock();
                                    *guard = Some(tx.clone());
                                }
                                let cs_election = cluster_state.clone();
                                let sa = self_addr;
                                let offset = repl_state.read().total_offset();
                                let vtx = vote_tx.clone();
                                let tx_direct = tx.clone();
                                tokio::spawn(async move {
                                    crate::cluster::failover::run_election_task(
                                        cs_election, sa, offset, rx, tx_direct,
                                    ).await;
                                    // Clear vote_tx when election ends
                                    *vtx.lock() = None;
                                });
                            }
                        }
                    }

                    // Rotate across peers instead of always pinging the first.
                    let peer_count = cs
                        .nodes
                        .values()
                        .filter(|n| n.node_id != cs.node_id)
                        .count();
                    let target = if peer_count == 0 {
                        None
                    } else {
                        let idx = ping_rotation % peer_count;
                        ping_rotation = ping_rotation.wrapping_add(1);
                        cs.nodes
                            .values()
                            .filter(|n| n.node_id != cs.node_id)
                            .nth(idx)
                            .map(|n| SocketAddr::new(n.addr.ip(), n.bus_port))
                    };
                    let msg = build_message(&cs, self_addr, GossipMsgType::Ping);
                    cs.messages_sent += 1;
                    (target, msg)
                };
                if let Some(target_addr) = target_addr {
                    // Skip if the previous probe hasn't resolved yet — bounds
                    // outstanding tasks to one even against a dead peer.
                    if !ping_in_flight.swap(true, std::sync::atomic::Ordering::AcqRel) {
                        let cs = cluster_state.clone();
                        let inflight = ping_in_flight.clone();
                        let probe_timeout =
                            Duration::from_millis((node_timeout_ms / 2).max(100));
                        tokio::spawn(async move {
                            let _ = tokio::time::timeout(probe_timeout, async move {
                                if let Ok(mut stream) = TcpStream::connect(target_addr).await {
                                    let data = serialize_gossip(&ping_msg);
                                    let len = (data.len() as u32).to_be_bytes();
                                    let _ = stream.write_all(&len).await;
                                    let _ = stream.write_all(&data).await;
                                    // Read PONG response
                                    let mut len_buf = [0u8; 4];
                                    if stream.read_exact(&mut len_buf).await.is_ok() {
                                        let pong_len = u32::from_be_bytes(len_buf) as usize;
                                        // Alloc guard: the length prefix is
                                        // peer-controlled — cap it like the bus
                                        // listener before sizing the buffer.
                                        if pong_len > crate::cluster::bus::MAX_GOSSIP_FRAME_LEN {
                                            return;
                                        }
                                        let mut pong_buf = vec![0u8; pong_len];
                                        if stream.read_exact(&mut pong_buf).await.is_ok() {
                                            if let Ok(pong) = deserialize_gossip(&pong_buf) {
                                                let mut cs2 = cs.write();
                                                merge_gossip_into_state(&mut cs2, &pong);
                                            }
                                        }
                                    }
                                }
                            })
                            .await;
                            inflight.store(false, std::sync::atomic::Ordering::Release);
                        });
                    }
                }
            }
            _ = shutdown.cancelled() => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id40(c: u8) -> String {
        String::from_utf8(vec![c; 40]).unwrap()
    }

    fn test_msg_from(node_id: &str, port: u16, sections: Vec<GossipSection>) -> GossipMessage {
        let mut sender_node_id = [0u8; 40];
        sender_node_id.copy_from_slice(node_id.as_bytes());
        let mut sender_ip = [0u8; 16];
        sender_ip[..9].copy_from_slice(b"127.0.0.1");
        GossipMessage {
            msg_type: GossipMsgType::Pong,
            sender_node_id,
            sender_slots: Box::new([0u8; 2048]),
            config_epoch: 0,
            sender_ip,
            sender_port: port,
            sender_bus_port: port + 10000,
            gossip_sections: sections,
        }
    }

    fn test_section_for(node_id: &str, port: u16, flags: u16) -> GossipSection {
        let mut id = [0u8; 40];
        id.copy_from_slice(node_id.as_bytes());
        let mut ip = [0u8; 16];
        ip[..9].copy_from_slice(b"127.0.0.1");
        GossipSection {
            node_id: id,
            ip,
            port,
            bus_port: port + 10000,
            flags,
            epoch: 0,
            ping_sent_ms: 0,
            pong_recv_ms: 0,
        }
    }

    /// C-1 formation fix: the random-id placeholder CLUSTER MEET creates must
    /// be retired when the peer's handshake arrives under its real id from
    /// the same address — otherwise every met peer is double-counted forever.
    #[test]
    fn handshake_retires_meet_placeholder() {
        let self_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let mut state = ClusterState::new(id40(b'a'), self_addr);
        let peer_addr: SocketAddr = "127.0.0.1:7001".parse().unwrap();
        state.nodes.insert(
            id40(b'p'),
            ClusterNode::new(id40(b'p'), peer_addr, NodeRole::Master, 0),
        );
        assert_eq!(state.nodes.len(), 2);

        let real = id40(b'b');
        merge_gossip_into_state(&mut state, &test_msg_from(&real, 7001, vec![]));

        assert!(state.nodes.contains_key(&real), "real peer entry missing");
        assert!(
            !state.nodes.contains_key(&id40(b'p')),
            "MEET placeholder at the peer's addr must be retired by the handshake"
        );
        assert_eq!(state.nodes.len(), 2, "self + real peer, nothing else");
    }

    /// C-1 formation fix: a healthy gossip section about a node we have never
    /// talked to must be adopted — this is the only way two nodes MEET-ed
    /// into a third learn about each other.
    #[test]
    fn healthy_rumor_adds_unknown_node() {
        let self_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let mut state = ClusterState::new(id40(b'a'), self_addr);

        let peer = id40(b'b');
        let third = id40(b'c');
        let msg = test_msg_from(&peer, 7001, vec![test_section_for(&third, 7002, 0)]);
        merge_gossip_into_state(&mut state, &msg);

        let adopted = state.nodes.get(&third).expect("rumored node not adopted");
        assert_eq!(
            adopted.addr,
            "127.0.0.1:7002".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(adopted.bus_port, 17002);
        assert!(
            adopted.pong_recv_ms > 0,
            "adopted nodes need a freshness baseline or they can never go PFAIL"
        );
        assert_eq!(state.nodes.len(), 3, "self + sender + rumored third");
    }

    /// A rumor pointing at OUR OWN address (a peer's not-yet-resolved MEET
    /// placeholder for us) must not create a phantom self entry, and a rumor
    /// about an address we already know under another id must not duplicate it.
    #[test]
    fn rumor_about_known_addr_is_ignored() {
        let self_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let mut state = ClusterState::new(id40(b'a'), self_addr);

        let peer = id40(b'b');
        let msg = test_msg_from(
            &peer,
            7001,
            vec![
                test_section_for(&id40(b'q'), 7000, 0), // our own addr
                test_section_for(&id40(b'r'), 7001, 0), // sender's addr, other id
            ],
        );
        merge_gossip_into_state(&mut state, &msg);

        assert!(!state.nodes.contains_key(&id40(b'q')));
        assert!(!state.nodes.contains_key(&id40(b'r')));
        assert_eq!(state.nodes.len(), 2, "self + sender only");
    }

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
