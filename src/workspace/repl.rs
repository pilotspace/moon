//! Replication wire format for WS.CREATE / WS.DROP (Wave B ws-plane).
//!
//! The workspace registry is process-global (not per-key), so its mutations
//! cannot ride the generic KV replication stream (which replays user-syntax
//! commands through `dispatch()`). Two internal-only pseudo-commands carry
//! the WAL payload bytes verbatim over the SAME RESP wire the KV/graph
//! streams use (`record_local_write` / `drain_replicated_commands`):
//!
//! ```text
//! WS.CREATE.APPLY <payload>   payload = wal::encode_workspace_create(..)
//! WS.DROP.APPLY   <payload>   payload = wal::encode_workspace_drop(..)
//! ```
//!
//! Reusing the exact WAL byte layout (rather than inventing a second encoding)
//! means the replica applies the SAME `ws_id` + `name` + `created_at_ms` the
//! master's WAL persisted — no separate serializer to keep in sync, no risk of
//! the wire form drifting from the on-disk form. `apply::apply_local`
//! recognizes both names before generic dispatch (mirroring
//! `GraphReplayCollector::is_graph_command`); neither name is a real client
//! command (`is_ws_command` / the `phf` dispatch table never see them).
//!
//! Never emitted with a re-minted UUID: `WS.CREATE.APPLY`'s payload always
//! carries the MASTER's `ws_id` (UUIDv7 is nondeterministic — a replica must
//! never allocate its own).

use bytes::Bytes;

/// Internal-only command name for a replicated WorkspaceCreate record. Never
/// dispatched from a client connection.
pub const WS_CREATE_APPLY_CMD: &[u8] = b"WS.CREATE.APPLY";
/// Internal-only command name for a replicated WorkspaceDrop record.
pub const WS_DROP_APPLY_CMD: &[u8] = b"WS.DROP.APPLY";

/// Write a RESP bulk string element: `$<len>\r\n<data>\r\n`
fn write_bulk(buf: &mut Vec<u8>, data: &[u8]) {
    buf.push(b'$');
    buf.extend_from_slice(itoa::Buffer::new().format(data.len()).as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(data);
    buf.extend_from_slice(b"\r\n");
}

/// Write a RESP array header: `*<count>\r\n`
fn write_array_header(buf: &mut Vec<u8>, count: usize) {
    buf.push(b'*');
    buf.extend_from_slice(itoa::Buffer::new().format(count).as_bytes());
    buf.extend_from_slice(b"\r\n");
}

/// Serialize `WS.CREATE.APPLY <payload>` as a RESP array. `payload` is the
/// exact byte output of [`super::wal::encode_workspace_create`].
pub fn serialize_ws_create_apply(payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(32 + payload.len());
    write_array_header(&mut buf, 2);
    write_bulk(&mut buf, WS_CREATE_APPLY_CMD);
    write_bulk(&mut buf, payload);
    buf
}

/// Serialize `WS.DROP.APPLY <payload>` as a RESP array. `payload` is the
/// exact byte output of [`super::wal::encode_workspace_drop`].
pub fn serialize_ws_drop_apply(payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(32 + payload.len());
    write_array_header(&mut buf, 2);
    write_bulk(&mut buf, WS_DROP_APPLY_CMD);
    write_bulk(&mut buf, payload);
    buf
}

/// Extract the single bulk-string payload argument from a parsed
/// `WS.CREATE.APPLY` / `WS.DROP.APPLY` frame's args (i.e. everything after the
/// command name). Returns `None` if the arg is missing or not a bulk/simple
/// string — a malformed or truncated replication record.
pub fn extract_payload(args: &[crate::protocol::Frame]) -> Option<Bytes> {
    use crate::protocol::Frame;
    match args.first() {
        Some(Frame::BulkString(b) | Frame::SimpleString(b)) => Some(b.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ParseConfig, parse};
    use bytes::BytesMut;

    /// Parse RESP bytes back into a `Frame` the same way the replica's
    /// `drain_replicated_commands` does, and return (cmd, args).
    fn parse_command(bytes: &[u8]) -> (Vec<u8>, Vec<crate::protocol::Frame>) {
        use crate::protocol::Frame;
        let mut buf = BytesMut::from(bytes);
        let config = ParseConfig::default();
        let frame = parse::parse(&mut buf, &config)
            .expect("parse ok")
            .expect("complete frame");
        match frame {
            Frame::Array(arr) => {
                let mut it = arr.into_iter();
                let cmd = match it.next() {
                    Some(Frame::BulkString(b) | Frame::SimpleString(b)) => b.to_vec(),
                    _ => panic!("expected command name"),
                };
                (cmd, it.collect())
            }
            other => panic!("expected array frame, got {other:?}"),
        }
    }

    #[test]
    fn ws_create_apply_round_trips_through_resp_parser() {
        let ws_id = [7u8; 16];
        let payload =
            crate::workspace::wal::encode_workspace_create(&ws_id, b"acme", 1_752_000_000_000);
        let wire = serialize_ws_create_apply(&payload);

        let (cmd, args) = parse_command(&wire);
        assert_eq!(cmd, WS_CREATE_APPLY_CMD);
        let decoded_payload = extract_payload(&args).expect("payload arg present");
        let (decoded_id, decoded_name, decoded_created_at) =
            crate::workspace::wal::decode_workspace_create(&decoded_payload)
                .expect("valid payload");
        assert_eq!(decoded_id, ws_id);
        assert_eq!(decoded_name, b"acme");
        assert_eq!(decoded_created_at, 1_752_000_000_000);
    }

    #[test]
    fn ws_drop_apply_round_trips_through_resp_parser() {
        let ws_id = [9u8; 16];
        let payload = crate::workspace::wal::encode_workspace_drop(&ws_id);
        let wire = serialize_ws_drop_apply(&payload);

        let (cmd, args) = parse_command(&wire);
        assert_eq!(cmd, WS_DROP_APPLY_CMD);
        let decoded_payload = extract_payload(&args).expect("payload arg present");
        let decoded_id =
            crate::workspace::wal::decode_workspace_drop(&decoded_payload).expect("valid payload");
        assert_eq!(decoded_id, ws_id);
    }

    #[test]
    fn extract_payload_missing_arg_returns_none() {
        assert!(extract_payload(&[]).is_none());
    }
}
