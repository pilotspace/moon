use bytes::Bytes;

use crate::protocol::Frame;

/// Return ERR wrong number of arguments for a given command.
pub fn err_wrong_args(cmd: &str) -> Frame {
    Frame::Error(Bytes::from(format!(
        "ERR wrong number of arguments for '{}' command",
        cmd
    )))
}

/// Extract &Bytes from a BulkString or SimpleString frame.
pub fn extract_bytes(frame: &Frame) -> Option<&Bytes> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b),
        _ => None,
    }
}

/// OK response.
pub fn ok() -> Frame {
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// Generic error response.
pub fn err(msg: &str) -> Frame {
    Frame::Error(Bytes::from(msg.to_string()))
}
