#![no_main]
use libfuzzer_sys::fuzz_target;

use bytes::BytesMut;
use moon::protocol::inline;

/// Fuzz the inline (telnet-style) command parser.
///
/// Inline parsing is simpler than RESP but still operates on raw bytes:
/// splits on whitespace, handles quoted strings, looks for CRLF termination.
fuzz_target!(|data: &[u8]| {
    let mut buf = BytesMut::from(data);
    let _ = inline::parse_inline(&mut buf);
});
