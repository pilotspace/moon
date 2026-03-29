use bytes::Bytes;

use super::affinity::MigratedConnectionState;
use crate::protocol::Frame;

/// Extract command name (as raw byte slice reference) and args from a Frame::Array.
/// Returns the name without allocation -- callers use `eq_ignore_ascii_case` for matching.
pub(crate) fn extract_command(frame: &Frame) -> Option<(&[u8], &[Frame])> {
    match frame {
        Frame::Array(args) if !args.is_empty() => {
            let name = match &args[0] {
                Frame::BulkString(s) => s.as_ref(),
                Frame::SimpleString(s) => s.as_ref(),
                _ => return None,
            };
            Some((name, &args[1..]))
        }
        _ => None,
    }
}

/// Extract a Bytes value from a Frame argument.
pub(crate) fn extract_bytes(frame: &Frame) -> Option<Bytes> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.clone()),
        _ => None,
    }
}

/// Restore connection state from migration, or use defaults for fresh connections.
///
/// Returns (protocol_version, selected_db, authenticated, current_user, client_name).
pub(crate) fn restore_migrated_state(
    migrated: Option<&MigratedConnectionState>,
    requirepass: &Option<String>,
) -> (u8, usize, bool, String, Option<Bytes>) {
    (
        migrated.map_or(2, |s| s.protocol_version),
        migrated.map_or(0, |s| s.selected_db),
        migrated.map_or(requirepass.is_none(), |s| s.authenticated),
        migrated.map_or_else(|| "default".to_string(), |s| s.current_user.clone()),
        migrated.and_then(|s| s.client_name.clone()),
    )
}

/// Apply RESP3 response type conversion based on command name and protocol version.
/// Uppercases the command name into a stack buffer for O(1) lookup.
#[inline]
pub(crate) fn apply_resp3_conversion(cmd: &[u8], response: Frame, proto: u8) -> Frame {
    if proto < 3 {
        return response;
    }
    let mut cmd_upper_buf = [0u8; 32];
    let cmd_upper_len = cmd.len().min(32);
    cmd_upper_buf[..cmd_upper_len].copy_from_slice(&cmd[..cmd_upper_len]);
    cmd_upper_buf[..cmd_upper_len].make_ascii_uppercase();
    crate::protocol::resp3::maybe_convert_resp3(&cmd_upper_buf[..cmd_upper_len], response, proto)
}

/// Propagate a subscription (or pattern subscription) to all remote shards' subscriber maps.
///
/// Called after subscribing locally to ensure remote shards know to forward published
/// messages to this shard. Acquires each shard's RemoteSubscriberMap write lock
/// individually (no nested locks).
pub(crate) fn propagate_subscription(
    all_remote_sub_maps: &[std::sync::Arc<
        parking_lot::RwLock<crate::shard::remote_subscriber_map::RemoteSubscriberMap>,
    >],
    channel: &Bytes,
    shard_id: usize,
    num_shards: usize,
    is_pattern: bool,
) {
    for target in 0..num_shards {
        if target == shard_id {
            continue;
        }
        all_remote_sub_maps[target]
            .write()
            .add(channel.clone(), shard_id, is_pattern);
    }
}

/// Remove a subscription (or pattern subscription) from all remote shards' subscriber maps.
///
/// Called after unsubscribing locally. Acquires each shard's RemoteSubscriberMap write lock
/// individually (no nested locks).
pub(crate) fn unpropagate_subscription(
    all_remote_sub_maps: &[std::sync::Arc<
        parking_lot::RwLock<crate::shard::remote_subscriber_map::RemoteSubscriberMap>,
    >],
    channel: &Bytes,
    shard_id: usize,
    num_shards: usize,
    is_pattern: bool,
) {
    for target in 0..num_shards {
        if target == shard_id {
            continue;
        }
        all_remote_sub_maps[target]
            .write()
            .remove(channel, shard_id, is_pattern);
    }
}
