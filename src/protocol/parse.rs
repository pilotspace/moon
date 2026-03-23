use bytes::BytesMut;

use super::frame::{Frame, ParseConfig, ParseError};

/// Attempt to parse one RESP2 frame from the buffer.
///
/// On success, advances the buffer past the consumed bytes and returns `Ok(Some(frame))`.
/// Returns `Ok(None)` if the buffer doesn't contain a complete frame (need more data).
/// Returns `Err` if the data violates the RESP2 protocol specification.
pub fn parse(_buf: &mut BytesMut, _config: &ParseConfig) -> Result<Option<Frame>, ParseError> {
    todo!()
}
