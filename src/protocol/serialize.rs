use bytes::BytesMut;

use super::frame::Frame;

/// Serialize a Frame into RESP2 wire format, appending to the buffer.
pub fn serialize(_frame: &Frame, _buf: &mut BytesMut) {
    todo!()
}
