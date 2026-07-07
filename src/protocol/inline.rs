use memchr::{memchr, memchr2};

use bytes::{Buf, Bytes, BytesMut};

use super::frame::{Frame, FrameVec, ParseError};

/// Parse an inline command from the buffer.
///
/// Inline commands are plain text lines terminated by `\r\n`, where arguments
/// are separated by whitespace (spaces or tabs). This is what telnet users
/// and redis-cli direct input send.
///
/// Returns `Ok(Some(Frame::Array(...)))` with each argument as a `BulkString`,
/// `Ok(None)` if the buffer doesn't contain a complete line (no `\r\n` found),
/// or `Ok(None)` for empty/whitespace-only lines (after advancing past the CRLF).
///
/// `max_inline_size` bounds the length of a single inline line. If the buffer
/// grows past it without a terminating `\r\n`, the line is rejected with a
/// protocol error instead of returning `Ok(None)` forever — this is what stops
/// a client that never sends `\r\n` from growing the read buffer without limit
/// (mirrors Redis's `PROTO_INLINE_MAX_SIZE`).
pub fn parse_inline(
    buf: &mut BytesMut,
    max_inline_size: usize,
) -> Result<Option<Frame>, ParseError> {
    // Find the CRLF terminator
    let crlf_pos = match find_crlf_position(&buf[..]) {
        Some(pos) => pos,
        None => {
            // No complete line yet. Reject before the buffer can grow unbounded:
            // a non-RESP stream with no `\r\n` would otherwise "need more data"
            // forever. Redis caps this at PROTO_INLINE_MAX_SIZE.
            if buf.len() > max_inline_size {
                return Err(ParseError::Invalid {
                    message: "Protocol error: too big inline request".into(),
                    offset: 0,
                });
            }
            return Ok(None); // Incomplete -- need more data
        }
    };

    // Reject an oversize completed line too (parity with Redis, which rejects
    // any inline request past the cap regardless of termination).
    if crlf_pos > max_inline_size {
        return Err(ParseError::Invalid {
            message: "Protocol error: too big inline request".into(),
            offset: 0,
        });
    }

    // Extract line content before CRLF
    let line = &buf[..crlf_pos];

    // Split by whitespace (spaces and tabs) using SIMD, filtering empty slices
    let mut args = FrameVec::new();
    let mut start = 0;
    while start < line.len() {
        // Skip whitespace
        while start < line.len() && (line[start] == b' ' || line[start] == b'\t') {
            start += 1;
        }
        if start >= line.len() {
            break;
        }
        // Find next whitespace using SIMD
        match memchr2(b' ', b'\t', &line[start..]) {
            Some(pos) => {
                args.push(Frame::BulkString(Bytes::copy_from_slice(
                    &line[start..start + pos],
                )));
                start += pos + 1;
            }
            None => {
                args.push(Frame::BulkString(Bytes::copy_from_slice(&line[start..])));
                break;
            }
        }
    }

    // Advance buffer past line + CRLF
    buf.advance(crlf_pos + 2);

    // Empty/whitespace-only lines produce no frame
    if args.is_empty() {
        return Ok(None);
    }

    Ok(Some(Frame::Array(args)))
}

/// SIMD-accelerated CRLF position finder. Returns position of \r.
#[inline]
fn find_crlf_position(buf: &[u8]) -> Option<usize> {
    if buf.len() < 2 {
        return None;
    }
    let mut search_from = 0;
    loop {
        match memchr(b'\r', &buf[search_from..]) {
            Some(rel_pos) => {
                let abs_pos = search_from + rel_pos;
                if abs_pos + 1 < buf.len() && buf[abs_pos + 1] == b'\n' {
                    return Some(abs_pos);
                }
                search_from = abs_pos + 1;
                if search_from >= buf.len() {
                    return None;
                }
            }
            None => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framevec;

    // Generous cap for the behavioural tests below; the cap itself is exercised
    // by the dedicated tests at the end of this module.
    const TEST_MAX_INLINE: usize = 64 * 1024;

    fn parse_inline_bytes(input: &[u8]) -> Result<Option<Frame>, ParseError> {
        let mut buf = BytesMut::from(input);
        parse_inline(&mut buf, TEST_MAX_INLINE)
    }

    #[test]
    fn test_parse_inline_ping() {
        let result = parse_inline_bytes(b"PING\r\n").unwrap().unwrap();
        assert_eq!(
            result,
            Frame::Array(framevec![Frame::BulkString(Bytes::from_static(b"PING"))])
        );
    }

    #[test]
    fn test_parse_inline_set_key_value() {
        let result = parse_inline_bytes(b"SET key value\r\n").unwrap().unwrap();
        assert_eq!(
            result,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"SET")),
                Frame::BulkString(Bytes::from_static(b"key")),
                Frame::BulkString(Bytes::from_static(b"value")),
            ])
        );
    }

    #[test]
    fn test_parse_inline_double_spaces() {
        let result = parse_inline_bytes(b"SET  key  value\r\n").unwrap().unwrap();
        assert_eq!(
            result,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"SET")),
                Frame::BulkString(Bytes::from_static(b"key")),
                Frame::BulkString(Bytes::from_static(b"value")),
            ])
        );
    }

    #[test]
    fn test_parse_inline_empty_line() {
        let result = parse_inline_bytes(b"\r\n").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_inline_whitespace_only() {
        let result = parse_inline_bytes(b"  \r\n").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_inline_incomplete_no_crlf() {
        let result = parse_inline_bytes(b"PING").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_inline_sequential() {
        let mut buf = BytesMut::from(&b"GET key\r\nPING\r\n"[..]);
        let frame1 = parse_inline(&mut buf, TEST_MAX_INLINE).unwrap().unwrap();
        assert_eq!(
            frame1,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"GET")),
                Frame::BulkString(Bytes::from_static(b"key")),
            ])
        );
        let frame2 = parse_inline(&mut buf, TEST_MAX_INLINE).unwrap().unwrap();
        assert_eq!(
            frame2,
            Frame::Array(framevec![Frame::BulkString(Bytes::from_static(b"PING"))])
        );
    }

    #[test]
    fn test_parse_inline_leading_whitespace() {
        let result = parse_inline_bytes(b"  PING\r\n").unwrap().unwrap();
        assert_eq!(
            result,
            Frame::Array(framevec![Frame::BulkString(Bytes::from_static(b"PING"))])
        );
    }

    #[test]
    fn test_parse_inline_tab_separated() {
        let result = parse_inline_bytes(b"SET\tkey\tvalue\r\n").unwrap().unwrap();
        assert_eq!(
            result,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"SET")),
                Frame::BulkString(Bytes::from_static(b"key")),
                Frame::BulkString(Bytes::from_static(b"value")),
            ])
        );
    }

    #[test]
    fn test_parse_inline_buffer_consumed() {
        let mut buf = BytesMut::from(&b"PING\r\nremaining"[..]);
        let _ = parse_inline(&mut buf, TEST_MAX_INLINE).unwrap().unwrap();
        assert_eq!(&buf[..], b"remaining");
    }

    #[test]
    fn test_parse_inline_empty_line_buffer_consumed() {
        let mut buf = BytesMut::from(&b"\r\nPING\r\n"[..]);
        let result = parse_inline(&mut buf, TEST_MAX_INLINE).unwrap();
        assert!(result.is_none());
        assert_eq!(&buf[..], b"PING\r\n");
    }

    #[test]
    fn test_parse_inline_incomplete_under_cap_needs_more_data() {
        // A partial line shorter than the cap is still "need more data", not an error.
        let mut buf = BytesMut::from(&b"PARTIAL WITHOUT TERMINATOR"[..]);
        let result = parse_inline(&mut buf, TEST_MAX_INLINE).unwrap();
        assert!(result.is_none());
        // Buffer is untouched so the caller can append more bytes.
        assert_eq!(&buf[..], b"PARTIAL WITHOUT TERMINATOR");
    }

    #[test]
    fn test_parse_inline_incomplete_over_cap_rejected() {
        // No CRLF and length exceeds the cap -> reject instead of Ok(None) forever.
        // This is the memory-exhaustion guard: without it the read buffer grows
        // without bound for a client that never sends `\r\n`.
        let cap = 16;
        let mut buf = BytesMut::from(&b"AAAAAAAAAAAAAAAAAAAAAAAAAAAA"[..]); // 28 bytes, no CRLF
        let err = parse_inline(&mut buf, cap).unwrap_err();
        match err {
            ParseError::Invalid { message, .. } => {
                assert!(message.contains("too big inline request"), "got: {message}");
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_inline_complete_over_cap_rejected() {
        // A terminated line that is itself larger than the cap is also rejected.
        let cap = 8;
        let mut buf = BytesMut::from(&b"THIS LINE IS WAY TOO LONG\r\n"[..]);
        let err = parse_inline(&mut buf, cap).unwrap_err();
        assert!(matches!(err, ParseError::Invalid { .. }));
    }

    #[test]
    fn test_parse_inline_exactly_at_cap_ok() {
        // A complete line of exactly the cap length is accepted (boundary is inclusive).
        let cap = 4;
        let mut buf = BytesMut::from(&b"PING\r\n"[..]); // line content "PING" == 4 bytes
        let frame = parse_inline(&mut buf, cap).unwrap().unwrap();
        assert_eq!(
            frame,
            Frame::Array(framevec![Frame::BulkString(Bytes::from_static(b"PING"))])
        );
    }
}
