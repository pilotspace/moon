use bytes::Bytes;

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
