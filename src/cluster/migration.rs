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

use crate::cluster::slots::slot_for_key;
use crate::cluster::{ClusterNode, ClusterState, NodeHealth, NodeRole};

// --- nodes.conf persistence ---

/// Format:
/// `<node-id> <ip>:<port>@<bus-port> <flags> <master-id-or-> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot-ranges>`
/// Final line: `vars currentEpoch <N> lastVoteEpoch <M>`

pub fn save_nodes_conf(state: &ClusterState, dir: &Path) -> std::io::Result<()> {
    let path = dir.join("nodes.conf");

    let mut buf: Vec<u8> = Vec::new();

    for node in state.nodes.values() {
        // Comma-separated, exactly as CLUSTER NODES renders it: role and health
        // are independent, so `master,fail` is a normal line. The previous
        // single-enum form could only write one of them, which meant a failed
        // node reloaded from disk came back with its ROLE erased.
        let mut flags_parts: Vec<&str> = Vec::with_capacity(3);
        if node.node_id == state.node_id {
            flags_parts.push("myself");
        }
        flags_parts.push(if node.is_master() { "master" } else { "slave" });
        match node.health {
            NodeHealth::Online => {}
            NodeHealth::Pfail => flags_parts.push("fail?"),
            NodeHealth::Fail => flags_parts.push("fail"),
        }
        let flags_str = flags_parts.join(",");
        let master_id_field = node
            .master_id()
            .map(|m| m.to_string())
            .unwrap_or_else(|| "-".to_string());
        let link_state = if matches!(node.health, NodeHealth::Fail) {
            "disconnected"
        } else {
            "connected"
        };
        let slot_ranges = bitmap_to_ranges_migration(&node.slots);

        writeln!(
            buf,
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
        buf,
        "vars currentEpoch {} lastVoteEpoch {}",
        state.epoch, state.last_vote_epoch
    )?;

    // Atomic via `atomic_write_durable` (task #49): temp + fsync + rename
    // + dir-fsync. The prior code wrote+renamed with no fsync at all, so a
    // kill-9 right after `rename()` returned could still revert
    // nodes.conf on ext4/xfs -- a corrupted/stale cluster topology file
    // read back at the next boot.
    crate::persistence::atomic::atomic_write_durable(&path, &buf)?;
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
        // Two independent axes read from one comma-separated column. Order
        // matters: `fail?` must be tested BEFORE `fail`, because `"fail?"`
        // contains `"fail"`.
        let role = if flags_str.contains("slave") {
            NodeRole::Replica {
                master_id: parts[3].to_string(),
            }
        } else {
            NodeRole::Master
        };
        let health = if flags_str.contains("fail?") {
            NodeHealth::Pfail
        } else if flags_str.contains("fail") {
            NodeHealth::Fail
        } else {
            NodeHealth::Online
        };
        let ping_sent_ms: u64 = parts[4].parse().unwrap_or(0);
        let pong_recv_ms: u64 = parts[5].parse().unwrap_or(0);
        let epoch: u64 = parts[6].parse().unwrap_or(0);

        let mut node = ClusterNode::new(node_id.clone(), addr, role, epoch);
        node.health = health;
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
        let loaded = new_state
            .nodes
            .get(&my_id)
            .expect("original node not found");
        assert!(loaded.owns_slot(0));
        assert!(loaded.owns_slot(100));
        assert!(!loaded.owns_slot(50));
    }

    /// Task #49: `save_nodes_conf` must go through `atomic_write_durable`,
    /// not a hand-rolled `File::create(tmp)` + `rename`. Regression pin: no
    /// leftover `nodes.conf.tmp` after a successful save.
    #[test]
    fn test_save_nodes_conf_leaves_no_leftover_temp_file() {
        let tmp = TempDir::new().unwrap();
        let my_id = "c".repeat(40);
        let state = ClusterState::new(my_id, test_addr(6379));

        save_nodes_conf(&state, tmp.path()).unwrap();

        let entries: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(entries, vec![std::ffi::OsString::from("nodes.conf")]);
    }

    /// nodes.conf must round-trip BOTH axes for every combination, not just a
    /// healthy master.
    ///
    /// The flags column is one comma-separated string parsed by substring, and
    /// `"fail?"` CONTAINS `"fail"` — so testing them in the wrong order silently
    /// reloads every PFAIL node as confirmed FAIL. The existing round-trip test
    /// covers a healthy master only, which is exactly the shape that lets that
    /// bug through: the ordering is correct today and nothing pinned it.
    #[test]
    fn test_nodes_conf_roundtrips_role_and_health_independently() {
        let my_id = "a".repeat(40);
        let master_id = "m".repeat(40);

        // Every role x health pairing, including the ones the old single enum
        // could not represent at all (a failed node that is still a master).
        let cases: Vec<(&str, NodeRole, NodeHealth)> = vec![
            ("master-online", NodeRole::Master, NodeHealth::Online),
            ("master-pfail", NodeRole::Master, NodeHealth::Pfail),
            ("master-fail", NodeRole::Master, NodeHealth::Fail),
            (
                "replica-online",
                NodeRole::Replica {
                    master_id: master_id.clone(),
                },
                NodeHealth::Online,
            ),
            (
                "replica-pfail",
                NodeRole::Replica {
                    master_id: master_id.clone(),
                },
                NodeHealth::Pfail,
            ),
            (
                "replica-fail",
                NodeRole::Replica {
                    master_id: master_id.clone(),
                },
                NodeHealth::Fail,
            ),
        ];

        for (label, role, health) in cases {
            let tmp = TempDir::new().unwrap();
            let mut state = ClusterState::new(my_id.clone(), test_addr(6379));
            let peer_id = "p".repeat(40);
            let mut peer =
                crate::cluster::ClusterNode::new(peer_id.clone(), test_addr(6380), role.clone(), 3);
            peer.health = health;
            peer.set_slot(42);
            state.nodes.insert(peer_id.clone(), peer);

            save_nodes_conf(&state, tmp.path()).unwrap();

            let mut fresh = ClusterState::new("z".repeat(40), test_addr(6381));
            load_nodes_conf(&mut fresh, tmp.path()).unwrap();
            let loaded = fresh
                .nodes
                .get(&peer_id)
                .unwrap_or_else(|| panic!("{label}: peer not reloaded"));

            assert_eq!(loaded.health, health, "{label}: health did not survive");
            assert_eq!(
                loaded.is_master(),
                matches!(role, NodeRole::Master),
                "{label}: role did not survive"
            );
            if let NodeRole::Replica { .. } = role {
                assert_eq!(
                    loaded.master_id(),
                    Some(master_id.as_str()),
                    "{label}: a replica that forgets its master cannot be grouped into a shard"
                );
            }
            assert!(loaded.owns_slot(42), "{label}: slots did not survive");
        }
    }

    /// handle_get_keys_in_slot returns matching keys only.
    #[test]
    fn test_get_keys_in_slot_filters_correctly() {
        use crate::storage::Database;
        use crate::storage::Entry;
        let mut db = Database::new();
        db.set(
            &bytes::Bytes::from_static(b"foo"),
            Entry::new_string(bytes::Bytes::from_static(b"v1")),
        );
        db.set(
            &bytes::Bytes::from_static(b"bar"),
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
        let peer =
            crate::cluster::ClusterNode::new(peer_id.clone(), test_addr(6380), NodeRole::Master, 0);
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
