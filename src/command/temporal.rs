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
