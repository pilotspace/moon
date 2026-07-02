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

/// Whether an absolute expiry (unix millis) is representable without a
/// client-visible wrap.
///
/// Expiry is stored as `u64` millis, but `PTTL`/`PEXPIRETIME` cast it back to
/// `i64` — an expiry past `i64::MAX` surfaces as a NEGATIVE TTL on a key that is
/// very much alive. Redis rejects such out-of-range expiries outright
/// (`when > LLONG_MAX / 1000` → "invalid expire time"), so every expiry-setting
/// command must too. Returns `true` when `expires_at_ms` is safe to store.
#[inline]
pub fn expiry_ms_in_range(expires_at_ms: u64) -> bool {
    expires_at_ms <= i64::MAX as u64
}
