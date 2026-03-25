//! Slot migration: nodes.conf persistence and key enumeration for slot transfer.
//!
//! Migration flow:
//! 1. Source: CLUSTER SETSLOT <slot> MIGRATING <dst-id>
//! 2. Target: CLUSTER SETSLOT <slot> IMPORTING <src-id>
//! 3. Source iterates: CLUSTER GETKEYSINSLOT <slot> <count> -> send MIGRATE for each
//! 4. After all keys: CLUSTER SETSLOT <slot> NODE <dst-id> on all nodes

use std::io::{BufRead, Write};
use std::net::SocketAddr;
use std::path::Path;

use bytes::Bytes;

use crate::cluster::{ClusterNode, ClusterState, NodeFlags};
use crate::cluster::slots::slot_for_key;

// --- nodes.conf persistence ---

/// Format:
/// `<node-id> <ip>:<port>@<bus-port> <flags> <master-id-or-> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot-ranges>`
/// Final line: `vars currentEpoch <N> lastVoteEpoch <M>`

pub fn save_nodes_conf(state: &ClusterState, dir: &Path) -> std::io::Result<()> {
    let path = dir.join("nodes.conf");
    let tmp_path = dir.join("nodes.conf.tmp");

    let mut f = std::fs::File::create(&tmp_path)?;

    for node in state.nodes.values() {
        let flags_str = if node.node_id == state.node_id {
            match &node.flags {
                NodeFlags::Master => "myself,master".to_string(),
                NodeFlags::Replica { .. } => "myself,slave".to_string(),
                NodeFlags::Pfail => "myself,pfail".to_string(),
                NodeFlags::Fail => "myself,fail".to_string(),
            }
        } else {
            match &node.flags {
                NodeFlags::Master => "master".to_string(),
                NodeFlags::Replica { .. } => "slave".to_string(),
                NodeFlags::Pfail => "pfail".to_string(),
                NodeFlags::Fail => "fail".to_string(),
            }
        };
        let master_id_field = match &node.flags {
            NodeFlags::Replica { master_id } => master_id.clone(),
            _ => "-".to_string(),
        };
        let link_state = if matches!(node.flags, NodeFlags::Fail) {
            "disconnected"
        } else {
            "connected"
        };
        let slot_ranges = bitmap_to_ranges_migration(&node.slots);

        writeln!(
            f,
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
            slot_ranges,
        )?;
    }
    writeln!(
        f,
        "vars currentEpoch {} lastVoteEpoch {}",
        state.epoch, state.last_vote_epoch
    )?;

    drop(f);
    std::fs::rename(&tmp_path, &path)?;
    Ok(())
}

/// Load nodes.conf and populate ClusterState.nodes.
/// Returns Ok(count) of nodes loaded, or Err on I/O or parse failure.
pub fn load_nodes_conf(state: &mut ClusterState, dir: &Path) -> std::io::Result<usize> {
    let path = dir.join("nodes.conf");
    if !path.exists() {
        return Ok(0);
    }
    let file = std::fs::File::open(&path)?;
    let reader = std::io::BufReader::new(file);
    let mut count = 0;

    for line in reader.lines() {
        let line = line?;
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        if line.starts_with("vars ") {
            // vars currentEpoch <N> lastVoteEpoch <M>
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 4 {
                if let Ok(epoch) = parts[2].parse::<u64>() {
                    state.epoch = state.epoch.max(epoch);
                }
                if parts.len() >= 5 {
                    if let Ok(ve) = parts[4].parse::<u64>() {
                        state.last_vote_epoch = ve;
                    }
                }
            }
            continue;
        }

        // <node-id> <ip>:<port>@<bus-port> <flags> <master-id> <ping-sent> <pong-recv> <epoch> <link-state> <slot-ranges...>
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 9 {
            continue;
        }
        let node_id = parts[0].to_string();
        // Parse "ip:port@bus_port"
        let addr_str = parts[1];
        let (data_addr_str, bus_port) = if let Some(at) = addr_str.find('@') {
            let bus_p: u16 = addr_str[at + 1..].parse().unwrap_or(0);
            (&addr_str[..at], bus_p)
        } else {
            (addr_str, 0u16)
        };
        let addr: SocketAddr = data_addr_str
            .parse()
            .unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap());
        let flags_str = parts[2];
        let flags = if flags_str.contains("slave") {
            NodeFlags::Replica {
                master_id: parts[3].to_string(),
            }
        } else if flags_str.contains("pfail") {
            NodeFlags::Pfail
        } else if flags_str.contains("fail") {
            NodeFlags::Fail
        } else {
            NodeFlags::Master
        };
        let ping_sent_ms: u64 = parts[4].parse().unwrap_or(0);
        let pong_recv_ms: u64 = parts[5].parse().unwrap_or(0);
        let epoch: u64 = parts[6].parse().unwrap_or(0);

        let mut node = ClusterNode::new(node_id.clone(), addr, flags, epoch);
        node.bus_port = bus_port;
        node.ping_sent_ms = ping_sent_ms;
        node.pong_recv_ms = pong_recv_ms;

        // Parse slot ranges (parts[8..])
        for range_str in &parts[8..] {
            if *range_str == "-" {
                continue;
            }
            if let Some(dash) = range_str.find('-') {
                let start: u16 = range_str[..dash].parse().unwrap_or(0);
                let end: u16 = range_str[dash + 1..].parse().unwrap_or(0);
                for s in start..=end {
                    node.set_slot(s);
                }
            } else if let Ok(s) = range_str.parse::<u16>() {
                node.set_slot(s);
            }
        }

        state.nodes.insert(node_id, node);
        count += 1;
    }
    Ok(count)
}

/// Handle GetKeysInSlot ShardMessage in the shard event loop.
///
/// Iterates keys in `db_index` and returns up to `count` keys whose CRC16 slot matches.
/// Called from the shard SPSC drain handler when ShardMessage::GetKeysInSlot arrives.
pub fn handle_get_keys_in_slot(
    databases: &[crate::storage::Database],
    db_index: usize,
    slot: u16,
    count: usize,
) -> Vec<Bytes> {
    let db = match databases.get(db_index) {
        Some(d) => d,
        None => return Vec::new(),
    };
    let mut result = Vec::new();
    for key in db.keys() {
        if result.len() >= count {
            break;
        }
        if slot_for_key(key.as_bytes()) == slot {
            result.push(Bytes::copy_from_slice(key.as_bytes()));
        }
    }
    result
}

fn bitmap_to_ranges_migration(bitmap: &[u8; 2048]) -> String {
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
            ranges.push(if s == p {
                format!("{}", s)
            } else {
                format!("{}-{}", s, p)
            });
        }
    }
    if let (Some(s), Some(p)) = (start, prev) {
        ranges.push(if s == p {
            format!("{}", s)
        } else {
            format!("{}-{}", s, p)
        });
    }
    if ranges.is_empty() {
        "-".to_string()
    } else {
        ranges.join(" ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use tempfile::TempDir;

    fn test_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
    }

    /// CLUSTER-11: nodes.conf round-trip.
    #[test]
    fn test_nodes_conf_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let my_id = "a".repeat(40);
        let mut state = ClusterState::new(my_id.clone(), test_addr(6379));
        state.my_node_mut().set_slot(0);
        state.my_node_mut().set_slot(100);
        state.epoch = 5;

        save_nodes_conf(&state, tmp.path()).unwrap();

        // Load into a fresh state
        let new_id = "b".repeat(40);
        let mut new_state = ClusterState::new(new_id, test_addr(6380));
        let count = load_nodes_conf(&mut new_state, tmp.path()).unwrap();
        assert!(count >= 1, "expected at least 1 node loaded");
        assert_eq!(new_state.epoch, 5);

        // Find the loaded node with our original ID
        let loaded = new_state.nodes.get(&my_id).expect("original node not found");
        assert!(loaded.owns_slot(0));
        assert!(loaded.owns_slot(100));
        assert!(!loaded.owns_slot(50));
    }

    /// handle_get_keys_in_slot returns matching keys only.
    #[test]
    fn test_get_keys_in_slot_filters_correctly() {
        use crate::storage::Database;
        use crate::storage::Entry;
        let mut db = Database::new();
        db.set(
            bytes::Bytes::from_static(b"foo"),
            Entry::new_string(bytes::Bytes::from_static(b"v1")),
        );
        db.set(
            bytes::Bytes::from_static(b"bar"),
            Entry::new_string(bytes::Bytes::from_static(b"v2")),
        );

        let databases = vec![db];
        let foo_slot = crate::cluster::slots::slot_for_key(b"foo");
        let keys = handle_get_keys_in_slot(&databases, 0, foo_slot, 10);
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].as_ref(), b"foo");

        let bar_slot = crate::cluster::slots::slot_for_key(b"bar");
        let keys2 = handle_get_keys_in_slot(&databases, 0, bar_slot, 10);
        assert_eq!(keys2.len(), 1);
        assert_eq!(keys2[0].as_ref(), b"bar");
    }

    /// CLUSTER-12: route_slot returns Ask when slot is MIGRATING.
    #[test]
    fn test_migrating_slot_returns_ask_route() {
        let my_id = "a".repeat(40);
        let peer_id = "b".repeat(40);
        let mut state = ClusterState::new(my_id.clone(), test_addr(6379));
        // foo hashes to slot 12182
        let foo_slot = crate::cluster::slots::slot_for_key(b"foo");
        state.my_node_mut().set_slot(foo_slot);
        let peer = crate::cluster::ClusterNode::new(
            peer_id.clone(),
            test_addr(6380),
            NodeFlags::Master,
            0,
        );
        state.nodes.insert(peer_id.clone(), peer);
        // Mark slot as migrating to peer
        state.migrating.insert(foo_slot, peer_id.clone());

        let route = state.route_slot(foo_slot, false);
        // When we own the slot but it's MIGRATING -> ASK
        assert!(
            matches!(route, crate::cluster::SlotRoute::Ask { .. }),
            "expected Ask, got {:?}",
            route
        );
    }
}
