//! CLUSTER command family handlers.
//!
//! All subcommands operate on a shared Arc<RwLock<ClusterState>>.
//! Called from handle_connection_sharded (intercepted before dispatch, like AUTH/CONFIG).

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use bytes::Bytes;

use crate::cluster::failover::DEFAULT_NODE_TIMEOUT_MS;
use crate::cluster::slots::slot_for_key;
use crate::cluster::{ClusterNode, ClusterState, ClusterStatus, NodeFlags};
use crate::framevec;
use crate::protocol::Frame;
/// Entry point: dispatch CLUSTER <subcommand> [args...] to the correct handler.
///
/// Returns a Frame response, or Frame::Error if the subcommand is unknown or args invalid.
pub fn handle_cluster_command(
    args: &[Frame],
    cluster_state: &Arc<RwLock<ClusterState>>,
    self_addr: SocketAddr,
) -> Frame {
    let subcmd = match args.first() {
        Some(Frame::BulkString(b)) | Some(Frame::SimpleString(b)) => b.clone(),
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for CLUSTER",
            ));
        }
    };
    let subcmd_upper = subcmd.to_ascii_uppercase();

    match subcmd_upper.as_slice() {
        b"INFO" => handle_cluster_info(cluster_state, self_addr),
        b"MYID" => handle_cluster_myid(cluster_state),
        b"NODES" => handle_cluster_nodes(cluster_state, self_addr),
        b"SLOTS" => handle_cluster_slots(cluster_state),
        b"MEET" => handle_cluster_meet(&args[1..], cluster_state),
        b"ADDSLOTS" => handle_cluster_addslots(&args[1..], cluster_state),
        b"DELSLOTS" => handle_cluster_delslots(&args[1..], cluster_state),
        b"SETSLOT" => handle_cluster_setslot(&args[1..], cluster_state),
        b"KEYSLOT" => handle_cluster_keyslot(&args[1..]),
        b"COUNTKEYSINSLOT" => handle_cluster_countkeysinslot(&args[1..]),
        b"RESET" => handle_cluster_reset(&args[1..], cluster_state, self_addr),
        b"REPLICATE" => handle_cluster_replicate(&args[1..], cluster_state),
        b"FAILOVER" => handle_cluster_failover(&args[1..], cluster_state),
        b"REPLICAS" | b"SLAVES" => handle_cluster_replicas(&args[1..], cluster_state),
        b"COUNT-FAILURE-REPORTS" => handle_cluster_count_failure_reports(&args[1..], cluster_state),
        _ => Frame::Error(Bytes::from(format!(
            "ERR unknown subcommand '{}' for CLUSTER",
            String::from_utf8_lossy(&subcmd_upper)
        ))),
    }
}

/// CLUSTER INFO -- return a bulk string with cluster statistics in Redis format.
pub fn handle_cluster_info(cs: &Arc<RwLock<ClusterState>>, _self_addr: SocketAddr) -> Frame {
    let state = cs.read().unwrap();
    let cluster_state_str = if state.status == ClusterStatus::Ok {
        "ok"
    } else {
        "fail"
    };
    let assigned = state.assigned_slot_count();
    let known_nodes = state.nodes.len();
    let size = state.master_count();
    let epoch = state.epoch;
    let sent = state.messages_sent;
    let recv = state.messages_received;

    let info = format!(
        "cluster_enabled:1\r\n\
         cluster_state:{}\r\n\
         cluster_slots_assigned:{}\r\n\
         cluster_slots_ok:{}\r\n\
         cluster_slots_pfail:0\r\n\
         cluster_slots_fail:0\r\n\
         cluster_known_nodes:{}\r\n\
         cluster_size:{}\r\n\
         cluster_current_epoch:{}\r\n\
         cluster_my_epoch:{}\r\n\
         cluster_stats_messages_sent:{}\r\n\
         cluster_stats_messages_received:{}\r\n",
        cluster_state_str, assigned, assigned, known_nodes, size, epoch, epoch, sent, recv
    );
    Frame::BulkString(Bytes::from(info))
}

/// CLUSTER MYID -- return this node's 40-char hex node ID.
pub fn handle_cluster_myid(cs: &Arc<RwLock<ClusterState>>) -> Frame {
    let state = cs.read().unwrap();
    Frame::BulkString(Bytes::from(state.node_id.clone()))
}

/// Format a single node description line in Redis CLUSTER NODES wire format.
///
/// Format: `<node-id> <ip>:<port>@<bus-port> <flags> <master-id> <ping-sent> <pong-recv> <epoch> <link-state> <slot-ranges>`
///
/// No trailing newline — callers add `\n` for CLUSTER NODES (bulk-string concat) or
/// wrap in `Frame::BulkString` directly for CLUSTER REPLICAS (array of strings).
fn format_node_line(node: &ClusterNode, self_node_id: &str) -> String {
    // CLUSTER NODES flags field is a comma-separated keyword list — never
    // includes the master-id (that goes in its own column below). Including
    // master_id here previously produced a malformed line with master_id
    // appearing twice (once inside flags, once in the dedicated column),
    // which broke whitespace-tokenized parsers (e.g. redis-cli's own line
    // splitter). Reference: Redis CLUSTER NODES format spec.
    let flags_str = if node.node_id == self_node_id {
        match &node.flags {
            NodeFlags::Master => "myself,master",
            NodeFlags::Replica { .. } => "myself,slave",
            NodeFlags::Pfail => "myself,pfail",
            NodeFlags::Fail => "myself,fail",
        }
    } else {
        match &node.flags {
            NodeFlags::Master => "master",
            NodeFlags::Replica { .. } => "slave",
            NodeFlags::Pfail => "pfail",
            NodeFlags::Fail => "fail",
        }
    };

    let master_id_field = match &node.flags {
        NodeFlags::Replica { master_id } => master_id.clone(),
        _ => "-".to_string(),
    };

    let slot_ranges = bitmap_to_ranges(&node.slots);
    let link_state = if matches!(node.flags, NodeFlags::Fail) {
        "disconnected"
    } else {
        "connected"
    };

    format!(
        "{} {}:{}@{} {} {} {} {} {} {} {}",
        node.node_id,
        node.addr.ip(),
        node.addr.port(),
        node.bus_port,
        flags_str,
        master_id_field,
        node.ping_sent_ms,
        node.pong_recv_ms,
        node.epoch,
        link_state,
        slot_ranges
    )
}

/// CLUSTER NODES -- one line per known node in nodes.conf format:
/// `<node-id> <ip>:<port>@<bus-port> <flags> <master-id> <ping-sent> <pong-recv> <epoch> <link-state> <slot-ranges>`
pub fn handle_cluster_nodes(cs: &Arc<RwLock<ClusterState>>, _self_addr: SocketAddr) -> Frame {
    let state = cs.read().unwrap();
    let mut output = String::new();
    for node in state.nodes.values() {
        output.push_str(&format_node_line(node, &state.node_id));
        output.push('\n');
    }
    Frame::BulkString(Bytes::from(output))
}

/// CLUSTER REPLICAS <node-id> / CLUSTER SLAVES <node-id>
///
/// Returns an array of CLUSTER NODES-format lines, one per replica of the given master.
/// Empty array if the master has no replicas.
/// `ERR Unknown node <id>` if the node-id is not known to this cluster.
///
/// SLAVES is the deprecated alias; both subcommands dispatch here.
pub fn handle_cluster_replicas(args: &[Frame], cs: &Arc<RwLock<ClusterState>>) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for CLUSTER REPLICAS",
        ));
    }
    let target_id = extract_string(&args[0]);

    let state = cs.read().unwrap();

    // ERR if the requested node-id is not in the cluster.
    if !state.nodes.contains_key(&target_id) {
        return Frame::Error(Bytes::from(format!("ERR Unknown node {}", target_id)));
    }

    // Collect all nodes whose flags mark them as replicas of target_id.
    let lines: Vec<Frame> = state
        .nodes
        .values()
        .filter(|n| matches!(&n.flags, NodeFlags::Replica { master_id } if master_id == &target_id))
        .map(|n| Frame::BulkString(Bytes::from(format_node_line(n, &state.node_id))))
        .collect();

    Frame::Array(lines.into())
}

/// CLUSTER SLOTS -- return nested array: [start, end, [master-ip, master-port, master-id], [replica...]]
pub fn handle_cluster_slots(cs: &Arc<RwLock<ClusterState>>) -> Frame {
    let state = cs.read().unwrap();
    let mut result = Vec::new();
    for node in state.nodes.values() {
        if !matches!(node.flags, NodeFlags::Master) {
            continue;
        }
        // Find contiguous ranges
        let mut start: Option<u16> = None;
        let mut prev: Option<u16> = None;
        for slot in 0u16..=16383 {
            if node.owns_slot(slot) {
                if start.is_none() {
                    start = Some(slot);
                }
                prev = Some(slot);
            } else if let (Some(s), Some(p)) = (start.take(), prev.take()) {
                let node_entry = Frame::Array(framevec![
                    Frame::BulkString(Bytes::from(node.addr.ip().to_string())),
                    Frame::Integer(node.addr.port() as i64),
                    Frame::BulkString(Bytes::from(node.node_id.clone())),
                ]);
                result.push(Frame::Array(framevec![
                    Frame::Integer(s as i64),
                    Frame::Integer(p as i64),
                    node_entry,
                ]));
            }
        }
        if let (Some(s), Some(p)) = (start, prev) {
            let node_entry = Frame::Array(framevec![
                Frame::BulkString(Bytes::from(node.addr.ip().to_string())),
                Frame::Integer(node.addr.port() as i64),
                Frame::BulkString(Bytes::from(node.node_id.clone())),
            ]);
            result.push(Frame::Array(framevec![
                Frame::Integer(s as i64),
                Frame::Integer(p as i64),
                node_entry,
            ]));
        }
    }
    Frame::Array(result.into())
}

/// CLUSTER MEET <ip> <port> -- add a node to our cluster state.
///
/// In full implementation this connects to the node and exchanges PING/PONG.
/// Here we add the node entry with a placeholder ID (will be filled by gossip).
pub fn handle_cluster_meet(args: &[Frame], cs: &Arc<RwLock<ClusterState>>) -> Frame {
    if args.len() < 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for CLUSTER MEET",
        ));
    }
    let ip = match &args[0] {
        Frame::BulkString(b) | Frame::SimpleString(b) => String::from_utf8_lossy(b).to_string(),
        _ => return Frame::Error(Bytes::from_static(b"ERR invalid IP")),
    };
    let port: u16 = match &args[1] {
        Frame::BulkString(b) | Frame::SimpleString(b) => {
            match std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()) {
                Some(p) => p,
                None => return Frame::Error(Bytes::from_static(b"ERR invalid port")),
            }
        }
        _ => return Frame::Error(Bytes::from_static(b"ERR invalid port")),
    };

    let addr: std::net::SocketAddr = format!("{}:{}", ip, port)
        .parse()
        .unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap());

    // Generate a placeholder node ID (will be replaced by gossip handshake)
    use crate::replication::state::generate_repl_id;
    let peer_id = generate_repl_id();

    let mut state = cs.write().unwrap();
    if !state.nodes.contains_key(&peer_id) {
        let node = ClusterNode::new(peer_id.clone(), addr, NodeFlags::Master, 0);
        state.nodes.insert(peer_id, node);
    }
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// CLUSTER ADDSLOTS <slot> [slot ...] -- assign slots to this node.
pub fn handle_cluster_addslots(args: &[Frame], cs: &Arc<RwLock<ClusterState>>) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for CLUSTER ADDSLOTS",
        ));
    }
    let slots = match parse_slots(args) {
        Ok(s) => s,
        Err(e) => return Frame::Error(Bytes::from(e)),
    };
    let mut state = cs.write().unwrap();
    for slot in &slots {
        state.my_node_mut().set_slot(*slot);
    }
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// CLUSTER DELSLOTS <slot> [slot ...] -- remove slot ownership from this node.
pub fn handle_cluster_delslots(args: &[Frame], cs: &Arc<RwLock<ClusterState>>) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for CLUSTER DELSLOTS",
        ));
    }
    let slots = match parse_slots(args) {
        Ok(s) => s,
        Err(e) => return Frame::Error(Bytes::from(e)),
    };
    let mut state = cs.write().unwrap();
    for slot in &slots {
        state.my_node_mut().clear_slot(*slot);
    }
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// CLUSTER SETSLOT <slot> MIGRATING|IMPORTING|NODE|STABLE [node-id]
pub fn handle_cluster_setslot(args: &[Frame], cs: &Arc<RwLock<ClusterState>>) -> Frame {
    if args.len() < 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for CLUSTER SETSLOT",
        ));
    }
    let slot: u16 = match extract_u16(&args[0]) {
        Some(s) if s < 16384 => s,
        _ => return Frame::Error(Bytes::from_static(b"ERR invalid slot")),
    };
    let subop = match &args[1] {
        Frame::BulkString(b) | Frame::SimpleString(b) => b.to_ascii_uppercase(),
        _ => return Frame::Error(Bytes::from_static(b"ERR invalid subcommand")),
    };

    let mut state = cs.write().unwrap();
    match subop.as_slice() {
        b"MIGRATING" => {
            if args.len() < 3 {
                return Frame::Error(Bytes::from_static(b"ERR node id required for MIGRATING"));
            }
            let node_id = extract_string(&args[2]);
            state.migrating.insert(slot, node_id);
        }
        b"IMPORTING" => {
            if args.len() < 3 {
                return Frame::Error(Bytes::from_static(b"ERR node id required for IMPORTING"));
            }
            let node_id = extract_string(&args[2]);
            state.importing.insert(slot, node_id);
        }
        b"NODE" => {
            if args.len() < 3 {
                return Frame::Error(Bytes::from_static(b"ERR node id required for NODE"));
            }
            let node_id = extract_string(&args[2]);
            // Clear migration state for this slot
            state.migrating.remove(&slot);
            state.importing.remove(&slot);
            // Transfer ownership: clear from all nodes, set on target
            for node in state.nodes.values_mut() {
                node.clear_slot(slot);
            }
            if node_id == state.node_id {
                state.my_node_mut().set_slot(slot);
            } else if let Some(node) = state.nodes.get_mut(&node_id) {
                node.set_slot(slot);
            }
        }
        b"STABLE" => {
            state.migrating.remove(&slot);
            state.importing.remove(&slot);
        }
        _ => return Frame::Error(Bytes::from_static(b"ERR unknown SETSLOT subcommand")),
    }
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// CLUSTER KEYSLOT <key> -- return the hash slot for a key.
pub fn handle_cluster_keyslot(args: &[Frame]) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for CLUSTER KEYSLOT",
        ));
    }
    let key = match &args[0] {
        Frame::BulkString(b) | Frame::SimpleString(b) => b.clone(),
        _ => return Frame::Error(Bytes::from_static(b"ERR invalid key")),
    };
    Frame::Integer(slot_for_key(&key) as i64)
}

/// CLUSTER COUNTKEYSINSLOT <slot> -- returns 0 (full impl in 20-04 with shard queries).
pub fn handle_cluster_countkeysinslot(args: &[Frame]) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(b"ERR wrong number of arguments"));
    }
    Frame::Integer(0)
}

/// CLUSTER RESET [HARD|SOFT] -- reset cluster state.
/// HARD: remove all nodes, clear slots. SOFT: clear slots only.
pub fn handle_cluster_reset(
    args: &[Frame],
    cs: &Arc<RwLock<ClusterState>>,
    _self_addr: SocketAddr,
) -> Frame {
    let hard = args.first().map(|a| matches!(a, Frame::BulkString(b) | Frame::SimpleString(b) if b.eq_ignore_ascii_case(b"hard"))).unwrap_or(false);
    let mut state = cs.write().unwrap();
    let my_id = state.node_id.clone();
    // Clear slots on my node
    *state.my_node_mut().slots = [0u8; 2048];
    state.importing.clear();
    state.migrating.clear();
    state.epoch = 0;
    if hard {
        // Remove all non-self nodes
        state.nodes.retain(|id, _| id == &my_id);
    }
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// CLUSTER REPLICATE <node-id> -- make this node a replica of the given master.
pub fn handle_cluster_replicate(args: &[Frame], cs: &Arc<RwLock<ClusterState>>) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(b"ERR wrong number of arguments"));
    }
    let master_id = extract_string(&args[0]);
    let mut state = cs.write().unwrap();
    let my_id = state.node_id.clone();
    if let Some(my_node) = state.nodes.get_mut(&my_id) {
        my_node.flags = NodeFlags::Replica { master_id };
    }
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

// --- Failover command ------------------------------------------------------------

/// CLUSTER FAILOVER mode: Normal (election via gossip), Force (skip vote), Takeover (skip vote + bump epoch).
enum FailoverMode {
    Normal,
    Force,
    Takeover,
}

/// Return current unix milliseconds.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// CLUSTER FAILOVER [FORCE|TAKEOVER] -- manually trigger failover on a replica.
///
/// - No args: sets FailoverState::WaitingDelay for gossip ticker to start election.
/// - FORCE: skips vote collection, promotes immediately (master may be unreachable).
/// - TAKEOVER: bumps epoch and promotes immediately (no delay, no voting).
fn handle_cluster_failover(args: &[Frame], cs: &Arc<RwLock<ClusterState>>) -> Frame {
    // Parse optional subcommand: FORCE or TAKEOVER
    let mode = if let Some(arg) = args.first() {
        let s = extract_string(arg).to_ascii_uppercase();
        match s.as_str() {
            "FORCE" => FailoverMode::Force,
            "TAKEOVER" => FailoverMode::Takeover,
            _ => {
                return Frame::Error(Bytes::from_static(
                    b"ERR CLUSTER FAILOVER only accepts FORCE or TAKEOVER",
                ));
            }
        }
    } else {
        FailoverMode::Normal
    };

    let mut state = cs.write().unwrap();

    // Must be a replica to failover
    let _master_id = match &state.my_node().flags {
        NodeFlags::Replica { master_id } => master_id.clone(),
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR CLUSTER FAILOVER can only be called on a replica node",
            ));
        }
    };

    match mode {
        FailoverMode::Normal => {
            // Set failover_state to trigger election on next gossip tick
            state.failover_state = crate::cluster::FailoverState::WaitingDelay {
                start_ms: now_ms(),
                delay_ms: 0, // gossip ticker will compute actual delay
            };
            Frame::SimpleString(Bytes::from_static(b"OK"))
        }
        FailoverMode::Force => {
            // Skip voting, promote immediately (master may be unreachable)
            crate::cluster::failover::check_and_initiate_failover(&mut state, 0);
            Frame::SimpleString(Bytes::from_static(b"OK"))
        }
        FailoverMode::Takeover => {
            // Skip everything: bump epoch, promote, take slots
            state.epoch += 1;
            crate::cluster::failover::check_and_initiate_failover(&mut state, 0);
            Frame::SimpleString(Bytes::from_static(b"OK"))
        }
    }
}

/// CLUSTER COUNT-FAILURE-REPORTS <node-id>
///
/// Returns the number of active (non-stale) PFAIL reports for the given node-id.
/// A report is stale when `(now_ms - reported_at) >= DEFAULT_NODE_TIMEOUT_MS * 2`.
///
/// Returns `:0` for an unknown node-id (matches real Redis behaviour).
pub fn handle_cluster_count_failure_reports(
    args: &[Frame],
    cs: &Arc<RwLock<ClusterState>>,
) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for CLUSTER COUNT-FAILURE-REPORTS",
        ));
    }
    let target_id = extract_string(&args[0]);
    let now = now_ms();
    // A report is active when its age is strictly less than 2 * timeout.
    let stale_cutoff = now.saturating_sub(2 * DEFAULT_NODE_TIMEOUT_MS);

    let state = cs.read().unwrap();
    let count = match state.nodes.get(&target_id) {
        None => 0i64,
        Some(node) => node
            .pfail_reports
            .values()
            .filter(|&&ts| ts > stale_cutoff)
            .count() as i64,
    };
    Frame::Integer(count)
}

// --- Helpers ---------------------------------------------------------------------

fn parse_slots(args: &[Frame]) -> Result<Vec<u16>, String> {
    args.iter()
        .map(|a| {
            extract_u16(a)
                .filter(|&s| s < 16384)
                .ok_or_else(|| "ERR Invalid or out of range slot".to_string())
        })
        .collect()
}

fn extract_u16(frame: &Frame) -> Option<u16> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => std::str::from_utf8(b).ok()?.parse().ok(),
        Frame::Integer(n) if *n >= 0 && *n <= 16383 => Some(*n as u16),
        _ => None,
    }
}

fn extract_string(frame: &Frame) -> String {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => String::from_utf8_lossy(b).to_string(),
        _ => String::new(),
    }
}

/// Convert a 2048-byte slot bitmap to a space-separated string of slot ranges.
/// E.g., bits 0-8192 set -> "0-8192"
fn bitmap_to_ranges(bitmap: &[u8; 2048]) -> String {
    let mut ranges = Vec::new();
    let mut start: Option<u16> = None;
    let mut prev: Option<u16> = None;

    for slot in 0u16..=16383 {
        let owned = bitmap[slot as usize / 8] & (1 << (slot as usize % 8)) != 0;
        if owned {
            if start.is_none() {
                start = Some(slot);
            }
            prev = Some(slot);
        } else if let (Some(s), Some(p)) = (start.take(), prev.take()) {
            if s == p {
                ranges.push(format!("{}", s));
            } else {
                ranges.push(format!("{}-{}", s, p));
            }
        }
    }
    if let (Some(s), Some(p)) = (start, prev) {
        if s == p {
            ranges.push(format!("{}", s));
        } else {
            ranges.push(format!("{}-{}", s, p));
        }
    }

    if ranges.is_empty() {
        "-".to_string() // no slots assigned
    } else {
        ranges.join(" ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn make_cs() -> Arc<RwLock<ClusterState>> {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6379);
        Arc::new(RwLock::new(ClusterState::new("a".repeat(40), addr)))
    }

    /// CLUSTER-06
    #[test]
    fn test_cluster_info_contains_enabled() {
        let cs = make_cs();
        let frame = handle_cluster_info(&cs, "127.0.0.1:6379".parse().unwrap());
        let s = match frame {
            Frame::BulkString(b) => String::from_utf8(b.to_vec()).unwrap(),
            _ => panic!("expected BulkString"),
        };
        assert!(s.contains("cluster_enabled:1"));
        assert!(s.contains("cluster_state:ok"));
    }

    /// CLUSTER-07
    #[test]
    fn test_cluster_myid_length() {
        let cs = make_cs();
        let frame = handle_cluster_myid(&cs);
        let id = match frame {
            Frame::BulkString(b) => b,
            _ => panic!("expected BulkString"),
        };
        assert_eq!(id.len(), 40);
    }

    /// CLUSTER-13: NODES output has 9+ fields per line.
    #[test]
    fn test_cluster_nodes_format() {
        let cs = make_cs();
        let frame = handle_cluster_nodes(&cs, "127.0.0.1:6379".parse().unwrap());
        let s = match frame {
            Frame::BulkString(b) => String::from_utf8(b.to_vec()).unwrap(),
            _ => panic!("expected BulkString"),
        };
        for line in s.trim().lines() {
            let fields: Vec<&str> = line.split_whitespace().collect();
            assert!(
                fields.len() >= 9,
                "expected >= 9 fields, got {}: {}",
                fields.len(),
                line
            );
        }
    }

    /// CLUSTER-08: ADDSLOTS increases assigned_slot_count.
    #[test]
    fn test_addslots_updates_bitmap() {
        let cs = make_cs();
        {
            let state = cs.read().unwrap();
            assert_eq!(state.assigned_slot_count(), 0);
        }
        let args = vec![
            Frame::BulkString(bytes::Bytes::from_static(b"0")),
            Frame::BulkString(bytes::Bytes::from_static(b"1")),
        ];
        let result = handle_cluster_addslots(&args, &cs);
        assert!(matches!(result, Frame::SimpleString(_)));
        let state = cs.read().unwrap();
        assert_eq!(state.assigned_slot_count(), 2);
        assert!(state.my_node().owns_slot(0));
        assert!(state.my_node().owns_slot(1));
    }

    #[test]
    fn test_setslot_migrating_importing() {
        let cs = make_cs();
        let node_id = "b".repeat(40);
        // SETSLOT 100 MIGRATING <node-id>
        let args = vec![
            Frame::BulkString(bytes::Bytes::from_static(b"100")),
            Frame::BulkString(bytes::Bytes::from_static(b"MIGRATING")),
            Frame::BulkString(bytes::Bytes::from(node_id.clone())),
        ];
        let r = handle_cluster_setslot(&args, &cs);
        assert!(matches!(r, Frame::SimpleString(_)));
        assert_eq!(cs.read().unwrap().migrating.get(&100), Some(&node_id));
    }

    /// KEYSLOT: slot_for_key("foo") == 12182 per CRC16-XMODEM.
    #[test]
    fn test_keyslot_foo() {
        let args = vec![Frame::BulkString(bytes::Bytes::from_static(b"foo"))];
        let result = handle_cluster_keyslot(&args);
        assert_eq!(result, Frame::Integer(12182));
    }

    #[test]
    fn test_delslots() {
        let cs = make_cs();
        // Add slot 5 first
        handle_cluster_addslots(&[Frame::BulkString(bytes::Bytes::from_static(b"5"))], &cs);
        assert!(cs.read().unwrap().my_node().owns_slot(5));
        // Delete it
        handle_cluster_delslots(&[Frame::BulkString(bytes::Bytes::from_static(b"5"))], &cs);
        assert!(!cs.read().unwrap().my_node().owns_slot(5));
    }

    /// SETSLOT NODE clears migrating/importing and transfers ownership.
    #[test]
    fn test_setslot_node_clears_migration() {
        let cs = make_cs();
        let my_id = "a".repeat(40);
        // First add slot to self and mark it as migrating
        handle_cluster_addslots(&[Frame::BulkString(bytes::Bytes::from_static(b"42"))], &cs);
        let args = vec![
            Frame::BulkString(bytes::Bytes::from_static(b"42")),
            Frame::BulkString(bytes::Bytes::from_static(b"MIGRATING")),
            Frame::BulkString(bytes::Bytes::from("b".repeat(40))),
        ];
        handle_cluster_setslot(&args, &cs);
        assert!(cs.read().unwrap().migrating.contains_key(&42));
        // Now SETSLOT 42 NODE <my_id>
        let args2 = vec![
            Frame::BulkString(bytes::Bytes::from_static(b"42")),
            Frame::BulkString(bytes::Bytes::from_static(b"NODE")),
            Frame::BulkString(bytes::Bytes::from(my_id.clone())),
        ];
        handle_cluster_setslot(&args2, &cs);
        let state = cs.read().unwrap();
        assert!(!state.migrating.contains_key(&42));
        assert!(state.my_node().owns_slot(42));
    }

    /// CLUSTER MEET adds a new node.
    #[test]
    fn test_cluster_meet_adds_node() {
        let cs = make_cs();
        assert_eq!(cs.read().unwrap().nodes.len(), 1);
        let args = vec![
            Frame::BulkString(bytes::Bytes::from_static(b"MEET")),
            Frame::BulkString(bytes::Bytes::from_static(b"192.168.1.2")),
            Frame::BulkString(bytes::Bytes::from_static(b"6380")),
        ];
        let result = handle_cluster_command(&args, &cs, "127.0.0.1:6379".parse().unwrap());
        assert!(matches!(result, Frame::SimpleString(_)));
        assert_eq!(cs.read().unwrap().nodes.len(), 2);
    }

    /// Helper: create a ClusterState where this node is a replica of a FAIL master.
    fn make_replica_with_fail_master() -> Arc<RwLock<ClusterState>> {
        let my_id = "a".repeat(40);
        let master_id = "b".repeat(40);
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6379);
        let cs = Arc::new(RwLock::new(ClusterState::new(my_id.clone(), addr)));
        {
            let mut state = cs.write().unwrap();
            // Make self a replica
            state.my_node_mut().flags = NodeFlags::Replica {
                master_id: master_id.clone(),
            };
            // Add master node as FAIL with some slots
            let mut master = ClusterNode::new(
                master_id.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6380),
                NodeFlags::Fail,
                1,
            );
            for slot in 0u16..=100 {
                master.set_slot(slot);
            }
            state.nodes.insert(master_id, master);
        }
        cs
    }

    /// CLUSTER FAILOVER on a master returns ERR.
    #[test]
    fn test_failover_rejects_on_master() {
        let cs = make_cs(); // default is master
        let result = handle_cluster_failover(&[], &cs);
        match result {
            Frame::Error(msg) => {
                let s = String::from_utf8_lossy(&msg);
                assert!(
                    s.contains("can only be called on a replica"),
                    "unexpected error: {}",
                    s
                );
            }
            other => panic!("expected Error, got {:?}", other),
        }
    }

    /// CLUSTER FAILOVER FORCE on a replica with FAIL master promotes to master.
    #[test]
    fn test_failover_force_promotes_replica() {
        let cs = make_replica_with_fail_master();
        let args = vec![Frame::BulkString(bytes::Bytes::from_static(b"FORCE"))];
        let result = handle_cluster_failover(&args, &cs);
        assert!(matches!(result, Frame::SimpleString(_)));
        let state = cs.read().unwrap();
        assert!(
            matches!(state.my_node().flags, NodeFlags::Master),
            "expected Master after FORCE failover"
        );
        // Should have inherited master's slots
        assert!(state.my_node().owns_slot(50));
    }

    /// CLUSTER FAILOVER TAKEOVER promotes and increments epoch.
    #[test]
    fn test_failover_takeover_promotes_replica() {
        let cs = make_replica_with_fail_master();
        let epoch_before = cs.read().unwrap().epoch;
        let args = vec![Frame::BulkString(bytes::Bytes::from_static(b"TAKEOVER"))];
        let result = handle_cluster_failover(&args, &cs);
        assert!(matches!(result, Frame::SimpleString(_)));
        let state = cs.read().unwrap();
        assert!(
            matches!(state.my_node().flags, NodeFlags::Master),
            "expected Master after TAKEOVER failover"
        );
        // Epoch should have been bumped (TAKEOVER +1, then check_and_initiate_failover +1)
        assert!(
            state.epoch > epoch_before,
            "epoch {} should be > {}",
            state.epoch,
            epoch_before
        );
        assert!(state.my_node().owns_slot(50));
    }

    /// CLUSTER FAILOVER with invalid subcommand returns ERR.
    #[test]
    fn test_failover_invalid_subcommand() {
        let cs = make_replica_with_fail_master();
        let args = vec![Frame::BulkString(bytes::Bytes::from_static(b"INVALID"))];
        let result = handle_cluster_failover(&args, &cs);
        match result {
            Frame::Error(msg) => {
                let s = String::from_utf8_lossy(&msg);
                assert!(
                    s.contains("only accepts FORCE or TAKEOVER"),
                    "unexpected error: {}",
                    s
                );
            }
            other => panic!("expected Error, got {:?}", other),
        }
    }

    /// CLUSTER FAILOVER (no args) on a replica sets WaitingDelay state.
    #[test]
    fn test_failover_normal_sets_waiting_delay() {
        let cs = make_replica_with_fail_master();
        let result = handle_cluster_failover(&[], &cs);
        assert!(matches!(result, Frame::SimpleString(_)));
        let state = cs.read().unwrap();
        assert!(
            matches!(
                state.failover_state,
                crate::cluster::FailoverState::WaitingDelay { .. }
            ),
            "expected WaitingDelay state, got {:?}",
            state.failover_state
        );
    }

    // -------------------------------------------------------------------------
    // T2.4: CLUSTER REPLICAS / SLAVES
    // -------------------------------------------------------------------------

    /// Build a ClusterState with one master (master_id) and two replicas of it.
    /// The self-node ("a"×40) is master; "c"×40 and "d"×40 are its replicas.
    fn make_cs_with_replicas() -> (Arc<RwLock<ClusterState>>, String, String, String) {
        let master_id = "a".repeat(40);
        let replica1_id = "c".repeat(40);
        let replica2_id = "d".repeat(40);
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6379);
        let cs = Arc::new(RwLock::new(ClusterState::new(master_id.clone(), addr)));
        {
            let mut state = cs.write().unwrap();
            let r1 = ClusterNode::new(
                replica1_id.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6380),
                NodeFlags::Replica {
                    master_id: master_id.clone(),
                },
                0,
            );
            let r2 = ClusterNode::new(
                replica2_id.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6381),
                NodeFlags::Replica {
                    master_id: master_id.clone(),
                },
                0,
            );
            state.nodes.insert(replica1_id.clone(), r1);
            state.nodes.insert(replica2_id.clone(), r2);
        }
        (cs, master_id, replica1_id, replica2_id)
    }

    /// T2.4-a: Empty array when master has no replicas.
    #[test]
    fn cluster_replicas_returns_empty_for_master_with_no_replicas() {
        let cs = make_cs(); // single-node, no replicas
        let my_id = cs.read().unwrap().node_id.clone();
        let args = vec![Frame::BulkString(bytes::Bytes::from(my_id))];
        let result = handle_cluster_replicas(&args, &cs);
        match result {
            Frame::Array(items) => assert!(
                items.is_empty(),
                "expected empty array, got {} items",
                items.len()
            ),
            other => panic!("expected Array, got {:?}", other),
        }
    }

    /// T2.4-b: Array with one BulkString per replica, each containing exactly
    /// 9 fields and the correct replica-row layout.
    ///
    /// Pinned columns (whitespace-tokenized):
    ///   [0] node-id            (40 hex chars)
    ///   [1] ip:port@bus
    ///   [2] flags              (== "slave" for non-self replica)
    ///   [3] master-id          (40 hex chars, the master being replicated)
    ///   [4] ping-sent
    ///   [5] pong-recv
    ///   [6] epoch
    ///   [7] link-state
    ///   [8] slot-ranges        ("-" for replicas, never empty)
    ///
    /// This guards against the previous bug where `format_node_line` embedded
    /// the master-id inside the flags field, producing "slave <master_id>" and
    /// shifting every subsequent column right by one.
    #[test]
    fn cluster_replicas_lists_replicas() {
        let (cs, master_id, replica1_id, replica2_id) = make_cs_with_replicas();
        let args = vec![Frame::BulkString(bytes::Bytes::from(master_id.clone()))];
        let result = handle_cluster_replicas(&args, &cs);
        match result {
            Frame::Array(items) => {
                assert_eq!(items.len(), 2, "expected 2 replicas");
                let mut seen_ids = std::collections::HashSet::new();
                for item in &*items {
                    let line = match item {
                        Frame::BulkString(b) => String::from_utf8(b.to_vec()).unwrap(),
                        other => panic!("expected BulkString element, got {:?}", other),
                    };
                    assert!(
                        !line.ends_with('\n'),
                        "line must not end with newline: {:?}",
                        line
                    );
                    let fields: Vec<&str> = line.split_whitespace().collect();
                    // Exactly the pinned columns above; slot-ranges renders
                    // as "-" for replicas (no slot ownership), so we expect
                    // exactly 9 whitespace-separated tokens. If
                    // `format_node_line` ever re-embeds master_id inside the
                    // flags column, this would become 10 — which is the
                    // signal we want to lock down.
                    assert_eq!(
                        fields.len(),
                        9,
                        "wrong column count (would silently regress if flags \
                         re-embed master_id): {:?}",
                        line
                    );
                    // flags column must be exactly "slave" — NOT "slave <hex40>".
                    assert_eq!(
                        fields[2], "slave",
                        "flags column must be 'slave', got '{}': {:?}",
                        fields[2], line
                    );
                    // master-id column must be the 40-char master id of the
                    // node being replicated, NOT empty and NOT a copy of any
                    // other field.
                    assert_eq!(
                        fields[3].len(),
                        40,
                        "master-id column must be 40 hex chars: {:?}",
                        line
                    );
                    assert_eq!(
                        fields[3], master_id,
                        "master-id column must match the master being queried: {:?}",
                        line
                    );
                    // master-id must appear exactly once on the line (the bug
                    // duplicated it: once in flags, once in its own column).
                    let occurrences = line.matches(master_id.as_str()).count();
                    assert_eq!(
                        occurrences, 1,
                        "master-id must appear exactly once on the line, found \
                         {} occurrences: {:?}",
                        occurrences, line
                    );
                    seen_ids.insert(fields[0].to_string());
                }
                assert!(seen_ids.contains(&replica1_id), "replica1 not in result");
                assert!(seen_ids.contains(&replica2_id), "replica2 not in result");
            }
            other => panic!("expected Array, got {:?}", other),
        }
    }

    /// T2.4-c: ERR Unknown node for non-existent master-id.
    #[test]
    fn cluster_replicas_rejects_unknown_node_id() {
        let cs = make_cs();
        let unknown = "f".repeat(40);
        let args = vec![Frame::BulkString(bytes::Bytes::from(unknown.clone()))];
        let result = handle_cluster_replicas(&args, &cs);
        match result {
            Frame::Error(msg) => {
                let s = String::from_utf8_lossy(&msg);
                assert!(
                    s.contains("Unknown node"),
                    "expected 'Unknown node' in error, got: {}",
                    s
                );
                assert!(s.contains(&unknown), "error should include the node id");
            }
            other => panic!("expected Error, got {:?}", other),
        }
    }

    /// T2.4-d: SLAVES is an alias for REPLICAS.
    #[test]
    fn cluster_slaves_is_alias_for_replicas() {
        let (cs, master_id, _, _) = make_cs_with_replicas();
        let args_replicas = vec![Frame::BulkString(bytes::Bytes::from(master_id.clone()))];
        let args_slaves = vec![Frame::BulkString(bytes::Bytes::from(master_id.clone()))];

        let replicas_result = handle_cluster_command(
            &[
                Frame::BulkString(bytes::Bytes::from_static(b"REPLICAS")),
                Frame::BulkString(bytes::Bytes::from(master_id.clone())),
            ],
            &cs,
            "127.0.0.1:6379".parse().unwrap(),
        );
        let slaves_result = handle_cluster_command(
            &[
                Frame::BulkString(bytes::Bytes::from_static(b"SLAVES")),
                Frame::BulkString(bytes::Bytes::from(master_id.clone())),
            ],
            &cs,
            "127.0.0.1:6379".parse().unwrap(),
        );

        // Both must return arrays of the same length
        let replicas_len = match &replicas_result {
            Frame::Array(v) => v.len(),
            other => panic!("REPLICAS: expected Array, got {:?}", other),
        };
        let slaves_len = match &slaves_result {
            Frame::Array(v) => v.len(),
            other => panic!("SLAVES: expected Array, got {:?}", other),
        };
        assert_eq!(
            replicas_len, slaves_len,
            "REPLICAS and SLAVES must return same number of elements"
        );
        drop(args_replicas);
        drop(args_slaves);
    }

    /// T2.4-e: Self appears with "myself," prefix when this node is one of the replicas.
    #[test]
    fn cluster_replicas_includes_myself_marker_when_self_is_replica() {
        let master_id = "b".repeat(40);
        let my_id = "a".repeat(40);
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6379);
        let cs = Arc::new(RwLock::new(ClusterState::new(my_id.clone(), addr)));
        {
            let mut state = cs.write().unwrap();
            // Make self a replica of master_id
            state.my_node_mut().flags = NodeFlags::Replica {
                master_id: master_id.clone(),
            };
            // Add the master node
            let master = ClusterNode::new(
                master_id.clone(),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6380),
                NodeFlags::Master,
                1,
            );
            state.nodes.insert(master_id.clone(), master);
        }
        let args = vec![Frame::BulkString(bytes::Bytes::from(master_id))];
        let result = handle_cluster_replicas(&args, &cs);
        match result {
            Frame::Array(items) => {
                assert_eq!(items.len(), 1, "expected exactly 1 replica (self)");
                let line = match &items[0] {
                    Frame::BulkString(b) => String::from_utf8(b.to_vec()).unwrap(),
                    other => panic!("expected BulkString, got {:?}", other),
                };
                assert!(
                    line.contains("myself,"),
                    "expected 'myself,' prefix in line: {}",
                    line
                );
            }
            other => panic!("expected Array, got {:?}", other),
        }
    }

    // -------------------------------------------------------------------------
    // T2.5: CLUSTER COUNT-FAILURE-REPORTS
    // -------------------------------------------------------------------------

    /// T2.5-a: Returns :0 for an unknown node-id (matches Redis behaviour).
    #[test]
    fn cluster_count_failure_reports_returns_zero_for_unknown_node() {
        let cs = make_cs();
        let unknown = "f".repeat(40);
        let args = vec![Frame::BulkString(bytes::Bytes::from(unknown))];
        let result = handle_cluster_count_failure_reports(&args, &cs);
        assert_eq!(result, Frame::Integer(0));
    }

    /// T2.5-b: Returns :0 for a healthy node with an empty pfail_reports map.
    #[test]
    fn cluster_count_failure_reports_returns_zero_for_healthy_node() {
        let cs = make_cs();
        let my_id = cs.read().unwrap().node_id.clone();
        let args = vec![Frame::BulkString(bytes::Bytes::from(my_id))];
        let result = handle_cluster_count_failure_reports(&args, &cs);
        assert_eq!(result, Frame::Integer(0));
    }

    /// T2.5-c: Counts active (non-stale) pfail_reports.
    #[test]
    fn cluster_count_failure_reports_counts_active_reports() {
        let cs = make_cs();
        let my_id = cs.read().unwrap().node_id.clone();
        let now = now_ms();
        {
            let mut state = cs.write().unwrap();
            let node = state.nodes.get_mut(&my_id).unwrap();
            // Two very recent reports
            node.pfail_reports.insert("reporter1".to_string(), now);
            node.pfail_reports.insert("reporter2".to_string(), now);
        }
        let args = vec![Frame::BulkString(bytes::Bytes::from(my_id))];
        let result = handle_cluster_count_failure_reports(&args, &cs);
        assert_eq!(result, Frame::Integer(2));
    }

    /// T2.5-d: Stale reports (age >= 2 * DEFAULT_NODE_TIMEOUT_MS) are excluded.
    #[test]
    fn cluster_count_failure_reports_excludes_stale_reports() {
        let cs = make_cs();
        let my_id = cs.read().unwrap().node_id.clone();
        // Use absolute timestamps that are unambiguously on each side of any
        // reasonable stale_cutoff, so the test is not sensitive to clock skew
        // between when we insert and when the handler calls now_ms().
        //
        // stale_ts = 0 (Unix epoch): age is enormous → always excluded.
        // active_ts = u64::MAX / 2: far-future ms → stale_cutoff (now - 60_000)
        //   is orders of magnitude smaller → always counted.
        let stale_ts: u64 = 0;
        let active_ts: u64 = u64::MAX / 2;
        {
            let mut state = cs.write().unwrap();
            let node = state.nodes.get_mut(&my_id).unwrap();
            node.pfail_reports
                .insert("stale_reporter".to_string(), stale_ts);
            node.pfail_reports
                .insert("active_reporter".to_string(), active_ts);
        }
        let args = vec![Frame::BulkString(bytes::Bytes::from(my_id))];
        let result = handle_cluster_count_failure_reports(&args, &cs);
        // Only the active_reporter (ts = u64::MAX/2) should be counted.
        assert_eq!(result, Frame::Integer(1));
    }
}
