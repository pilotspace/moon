//! TEMPORAL.* command validation and helpers.
//!
//! Commands:
//! - TEMPORAL.SNAPSHOT_AT: Record wall-clock -> LSN binding
//! - TEMPORAL.INVALIDATE: Set valid_to on a graph entity
//!
//! Handler integration: TEMPORAL.* commands are intercepted BEFORE dispatch
//! (same pattern as TXN.*) in handler_monoio.rs.

use bytes::Bytes;

use crate::protocol::Frame;

/// Error: wrong number of arguments for TEMPORAL.SNAPSHOT_AT.
pub const ERR_SNAPSHOT_AT_ARGS: &[u8] =
    b"ERR wrong number of arguments for 'TEMPORAL.SNAPSHOT_AT' command";

/// Error: wrong number of arguments for TEMPORAL.INVALIDATE.
pub const ERR_INVALIDATE_ARGS: &[u8] =
    b"ERR wrong number of arguments for 'TEMPORAL.INVALIDATE' command";

/// Error: invalid entity kind (must be NODE or EDGE).
pub const ERR_INVALID_ENTITY_KIND: &[u8] = b"ERR entity kind must be NODE or EDGE";

/// Error: invalid entity_id (must be a valid u64).
pub const ERR_INVALID_ENTITY_ID: &[u8] = b"ERR invalid entity_id (must be u64)";

/// Error: no temporal snapshot registered at or before requested time.
pub const ERR_NO_SNAPSHOT_AT_TIME: &[u8] =
    b"ERR no temporal snapshot registered at or before requested time";

/// Error: invalid timestamp (must be a valid i64).
pub const ERR_INVALID_TIMESTAMP: &[u8] = b"ERR invalid timestamp (must be i64)";

/// Error: graph not found.
pub const ERR_GRAPH_NOT_FOUND: &[u8] = b"ERR graph not found";

/// Error: entity not found in graph.
pub const ERR_ENTITY_NOT_FOUND: &[u8] = b"ERR entity not found in graph";

/// Check if a command is TEMPORAL.SNAPSHOT_AT.
pub fn is_temporal_snapshot_at(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"TEMPORAL.SNAPSHOT_AT")
}

/// Check if a command is TEMPORAL.INVALIDATE.
pub fn is_temporal_invalidate(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"TEMPORAL.INVALIDATE")
}

/// Validate TEMPORAL.SNAPSHOT_AT arguments.
/// TEMPORAL.SNAPSHOT_AT takes no arguments. Wall-clock and LSN are captured at handler level.
pub fn validate_snapshot_at(args: &[Frame]) -> Result<(), Frame> {
    if !args.is_empty() {
        return Err(Frame::Error(Bytes::from_static(ERR_SNAPSHOT_AT_ARGS)));
    }
    Ok(())
}

/// Validate TEMPORAL.INVALIDATE arguments and parse them.
/// TEMPORAL.INVALIDATE <entity_id> <NODE|EDGE> <graph_name>
/// Returns (entity_id, is_node, graph_name) on success.
pub fn validate_invalidate(args: &[Frame]) -> Result<(u64, bool, Bytes), Frame> {
    if args.len() != 3 {
        return Err(Frame::Error(Bytes::from_static(ERR_INVALIDATE_ARGS)));
    }
    let entity_id = match &args[0] {
        Frame::BulkString(bs) => std::str::from_utf8(bs)
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .ok_or_else(|| Frame::Error(Bytes::from_static(ERR_INVALID_ENTITY_ID)))?,
        _ => return Err(Frame::Error(Bytes::from_static(ERR_INVALID_ENTITY_ID))),
    };
    let is_node = match &args[1] {
        Frame::BulkString(bs) if bs.eq_ignore_ascii_case(b"NODE") => true,
        Frame::BulkString(bs) if bs.eq_ignore_ascii_case(b"EDGE") => false,
        _ => return Err(Frame::Error(Bytes::from_static(ERR_INVALID_ENTITY_KIND))),
    };
    let graph_name = match &args[2] {
        Frame::BulkString(bs) => bs.clone(),
        _ => return Err(Frame::Error(Bytes::from_static(ERR_INVALIDATE_ARGS))),
    };
    Ok((entity_id, is_node, graph_name))
}

/// Serialize the deterministic, wall-clock-pinned replication form of
/// TEMPORAL.INVALIDATE (v0.7 graph replication, adversarial round-2 finding
/// B): `TEMPORAL.INVALIDATE-AT <graph> <N|E> <entity_id> <wall_ms>`.
///
/// The user command captures `wall_ms` at execution time, so streaming it
/// verbatim would let master and replica disagree on `valid_to`; and the
/// drained `GraphTemporal` WAL record is a binary wal_v3 payload the RESP
/// replication link cannot carry. This internal RESP form pins the master's
/// wall clock; the replica applies it via `apply_invalidate` with the SAME
/// `wall_ms` (see `replication::apply`).
#[cfg(feature = "graph")]
pub fn serialize_invalidate_at(
    graph_name: &[u8],
    is_node: bool,
    entity_id: u64,
    wall_ms: i64,
) -> Vec<u8> {
    fn write_bulk(buf: &mut Vec<u8>, data: &[u8]) {
        let mut n = itoa::Buffer::new();
        buf.push(b'$');
        buf.extend_from_slice(n.format(data.len()).as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(data);
        buf.extend_from_slice(b"\r\n");
    }
    let mut id_buf = itoa::Buffer::new();
    let mut ms_buf = itoa::Buffer::new();
    let mut buf = Vec::with_capacity(96 + graph_name.len());
    buf.extend_from_slice(b"*5\r\n");
    write_bulk(&mut buf, b"TEMPORAL.INVALIDATE-AT");
    write_bulk(&mut buf, graph_name);
    write_bulk(&mut buf, if is_node { b"N" } else { b"E" });
    write_bulk(&mut buf, id_buf.format(entity_id).as_bytes());
    write_bulk(&mut buf, ms_buf.format(wall_ms).as_bytes());
    buf
}

/// Parse the argument list of a replicated `TEMPORAL.INVALIDATE-AT` record
/// (inverse of [`serialize_invalidate_at`], minus the command name).
/// Returns `(graph_name, is_node, entity_id, wall_ms)` or `None` on any
/// malformed field — the replica warns and skips rather than diverging
/// silently on garbage.
#[cfg(feature = "graph")]
pub fn parse_invalidate_at(args: &[Frame]) -> Option<(Bytes, bool, u64, i64)> {
    if args.len() != 4 {
        return None;
    }
    let bulk = |f: &Frame| -> Option<Bytes> {
        match f {
            Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.clone()),
            _ => None,
        }
    };
    let graph_name = bulk(&args[0])?;
    let is_node = match bulk(&args[1])?.as_ref() {
        b"N" => true,
        b"E" => false,
        _ => return None,
    };
    let entity_id: u64 = std::str::from_utf8(&bulk(&args[2])?).ok()?.parse().ok()?;
    let wall_ms: i64 = std::str::from_utf8(&bulk(&args[3])?).ok()?.parse().ok()?;
    Some((graph_name, is_node, entity_id, wall_ms))
}

/// Apply a TEMPORAL.INVALIDATE mutation to a graph store.
///
/// Sets `valid_to = wall_ms` on the entity and pushes the WAL payload into
/// `gs.wal_pending`. The CALLER drains the WAL and appends on its own shard —
/// this keeps the function usable from both the connection-local path and the
/// shard-side `ShardMessage::GraphCommand` handler (multi-shard routing sends
/// the command to the shard that owns the graph name).
#[cfg(feature = "graph")]
pub fn apply_invalidate(
    gs: &mut crate::graph::store::GraphStore,
    entity_id: u64,
    is_node: bool,
    graph_name: &Bytes,
    wall_ms: i64,
) -> Result<(), &'static [u8]> {
    let Some(named_graph) = gs.get_graph_mut(graph_name) else {
        return Err(ERR_GRAPH_NOT_FOUND);
    };
    let mutated = if is_node {
        let node_key: crate::graph::types::NodeKey = slotmap::KeyData::from_ffi(entity_id).into();
        if let Some(node) = named_graph.write_buf.get_node_mut(node_key) {
            node.valid_to = wall_ms;
            true
        } else {
            false
        }
    } else {
        let edge_key: crate::graph::types::EdgeKey = slotmap::KeyData::from_ffi(entity_id).into();
        if let Some(edge) = named_graph.write_buf.get_edge_mut(edge_key) {
            edge.valid_to = wall_ms;
            true
        } else {
            false
        }
    };
    if !mutated {
        return Err(ERR_ENTITY_NOT_FOUND);
    }
    let payload = crate::persistence::wal_v3::record::encode_graph_temporal(
        entity_id, is_node, wall_ms, wall_ms,
    );
    gs.wal_pending.push(payload);
    // Task #32: TEMPORAL.INVALIDATE mutates valid_to on write_buf directly,
    // changing what a subsequent read sees -- invalidate the graph's cached
    // query results. Re-fetch rather than reuse `named_graph` above: the
    // borrow was released at the `let Some(named_graph) = ...` match end.
    if let Some(named_graph) = gs.get_graph_mut(graph_name) {
        named_graph.touch();
    }
    Ok(())
}

/// Capture the current wall-clock time as i64 Unix milliseconds.
///
/// CRITICAL: This function MUST be called at the handler level BEFORE
/// passing the value to TemporalRegistry::record() or WAL payload
/// construction. The registry and WAL encode functions NEVER call NOW()
/// themselves. This enforces the P6 MEDIUM constraint from STATE.md.
#[inline]
pub fn capture_wall_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_snapshot_at_no_args() {
        assert!(validate_snapshot_at(&[]).is_ok());
    }

    #[test]
    fn test_validate_snapshot_at_rejects_args() {
        let args = [Frame::BulkString(Bytes::from_static(b"extra"))];
        assert!(validate_snapshot_at(&args).is_err());
    }

    #[test]
    fn test_validate_invalidate_valid() {
        let args = [
            Frame::BulkString(Bytes::from_static(b"42")),
            Frame::BulkString(Bytes::from_static(b"NODE")),
            Frame::BulkString(Bytes::from_static(b"mygraph")),
        ];
        let (eid, is_node, gname) = validate_invalidate(&args).unwrap();
        assert_eq!(eid, 42);
        assert!(is_node);
        assert_eq!(gname, Bytes::from_static(b"mygraph"));
    }

    #[test]
    fn test_validate_invalidate_edge() {
        let args = [
            Frame::BulkString(Bytes::from_static(b"99")),
            Frame::BulkString(Bytes::from_static(b"EDGE")),
            Frame::BulkString(Bytes::from_static(b"social")),
        ];
        let (eid, is_node, _) = validate_invalidate(&args).unwrap();
        assert_eq!(eid, 99);
        assert!(!is_node);
    }

    #[test]
    fn test_validate_invalidate_wrong_arg_count() {
        assert!(validate_invalidate(&[]).is_err());
        let args = [
            Frame::BulkString(Bytes::from_static(b"42")),
            Frame::BulkString(Bytes::from_static(b"NODE")),
        ];
        assert!(validate_invalidate(&args).is_err());
    }

    #[test]
    fn test_validate_invalidate_invalid_entity_id() {
        let args = [
            Frame::BulkString(Bytes::from_static(b"notanumber")),
            Frame::BulkString(Bytes::from_static(b"NODE")),
            Frame::BulkString(Bytes::from_static(b"mygraph")),
        ];
        assert!(validate_invalidate(&args).is_err());
    }

    #[test]
    fn test_validate_invalidate_invalid_kind() {
        let args = [
            Frame::BulkString(Bytes::from_static(b"42")),
            Frame::BulkString(Bytes::from_static(b"VERTEX")),
            Frame::BulkString(Bytes::from_static(b"mygraph")),
        ];
        assert!(validate_invalidate(&args).is_err());
    }

    #[test]
    fn test_is_temporal_snapshot_at() {
        assert!(is_temporal_snapshot_at(b"TEMPORAL.SNAPSHOT_AT"));
        assert!(is_temporal_snapshot_at(b"temporal.snapshot_at"));
        assert!(!is_temporal_snapshot_at(b"TEMPORAL.INVALIDATE"));
        assert!(!is_temporal_snapshot_at(b"SET"));
    }

    #[test]
    fn test_is_temporal_invalidate() {
        assert!(is_temporal_invalidate(b"TEMPORAL.INVALIDATE"));
        assert!(is_temporal_invalidate(b"temporal.invalidate"));
        assert!(!is_temporal_invalidate(b"TEMPORAL.SNAPSHOT_AT"));
    }

    #[test]
    fn test_capture_wall_ms_positive() {
        let ms = capture_wall_ms();
        // Should be a reasonable timestamp (after 2020-01-01 = 1577836800000)
        assert!(ms > 1_577_836_800_000);
    }
}
