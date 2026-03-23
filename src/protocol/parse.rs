use std::io::Cursor;

use bytes::{Buf, Bytes, BytesMut};

use super::frame::{Frame, ParseConfig, ParseError};
use super::inline;

/// Attempt to parse one RESP2 frame from the buffer.
///
/// Uses a two-pass check-then-parse pattern:
/// 1. Check pass: verify a complete frame exists using a read-only Cursor
/// 2. Parse pass: extract the frame and advance the buffer
///
/// On success, advances the buffer past the consumed bytes and returns `Ok(Some(frame))`.
/// Returns `Ok(None)` if the buffer doesn't contain a complete frame (need more data).
/// Returns `Err` if the data violates the RESP2 protocol specification.
pub fn parse(buf: &mut BytesMut, config: &ParseConfig) -> Result<Option<Frame>, ParseError> {
    if buf.is_empty() {
        return Ok(None);
    }

    // Dispatch: RESP-prefixed bytes go to RESP parser, everything else is inline
    match buf[0] {
        b'+' | b'-' | b':' | b'$' | b'*' => { /* fall through to RESP parsing below */ }
        _ => return inline::parse_inline(buf),
    }

    // Pass 1: Check if a complete frame exists (peek only, no buffer modification)
    let mut cursor = Cursor::new(&buf[..]);
    match check(&mut cursor, config, 0) {
        Ok(()) => {
            // Frame is complete. Record how many bytes it consumed.
            let len = cursor.position() as usize;

            // Pass 2: Parse the frame (re-read from start)
            let mut cursor = Cursor::new(&buf[..]);
            let frame = parse_frame(&mut cursor)?;

            // Advance the buffer past the consumed bytes
            buf.advance(len);

            Ok(Some(frame))
        }
        Err(ParseError::Incomplete) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Check if a complete RESP2 frame exists in the buffer.
/// Does NOT modify the buffer -- uses Cursor for read-only traversal.
fn check(
    cursor: &mut Cursor<&[u8]>,
    config: &ParseConfig,
    depth: usize,
) -> Result<(), ParseError> {
    if depth > config.max_array_depth {
        return Err(ParseError::Invalid {
            message: format!(
                "array nesting depth {} exceeds maximum {}",
                depth, config.max_array_depth
            ),
            offset: cursor.position() as usize,
        });
    }

    if !cursor.has_remaining() {
        return Err(ParseError::Incomplete);
    }

    match get_u8(cursor)? {
        b'+' | b'-' => {
            // Simple string or error: find \r\n
            find_crlf(cursor)?;
            Ok(())
        }
        b':' => {
            // Integer: find \r\n (validates line is complete)
            find_crlf(cursor)?;
            Ok(())
        }
        b'$' => {
            // Bulk string: read length, then skip that many bytes + \r\n
            let len = read_decimal(cursor)?;
            if len == -1 {
                // Null bulk string
                return Ok(());
            }
            if len < 0 {
                return Err(ParseError::Invalid {
                    message: format!("invalid bulk string length: {}", len),
                    offset: cursor.position() as usize,
                });
            }
            let len = len as usize;
            if len > config.max_bulk_string_size {
                return Err(ParseError::Invalid {
                    message: format!(
                        "bulk string size {} exceeds maximum {}",
                        len, config.max_bulk_string_size
                    ),
                    offset: cursor.position() as usize,
                });
            }
            // Skip data + trailing \r\n
            skip(cursor, len + 2)?;
            Ok(())
        }
        b'*' => {
            // Array: read count, then check each element
            let count = read_decimal(cursor)?;
            if count == -1 {
                // Null array
                return Ok(());
            }
            if count < 0 {
                return Err(ParseError::Invalid {
                    message: format!("invalid array length: {}", count),
                    offset: cursor.position() as usize,
                });
            }
            let count = count as usize;
            if count > config.max_array_length {
                return Err(ParseError::Invalid {
                    message: format!(
                        "array length {} exceeds maximum {}",
                        count, config.max_array_length
                    ),
                    offset: cursor.position() as usize,
                });
            }
            for _ in 0..count {
                check(cursor, config, depth + 1)?;
            }
            Ok(())
        }
        byte => Err(ParseError::Invalid {
            message: format!("unknown type byte: 0x{:02x}", byte),
            offset: cursor.position() as usize - 1,
        }),
    }
}

/// Parse a complete RESP2 frame from the cursor.
/// Assumes `check()` has already validated that a complete frame exists.
fn parse_frame(cursor: &mut Cursor<&[u8]>) -> Result<Frame, ParseError> {
    match get_u8(cursor)? {
        b'+' => {
            let line = find_crlf(cursor)?;
            Ok(Frame::SimpleString(Bytes::copy_from_slice(line)))
        }
        b'-' => {
            let line = find_crlf(cursor)?;
            Ok(Frame::Error(Bytes::copy_from_slice(line)))
        }
        b':' => {
            let line = find_crlf(cursor)?;
            let s = std::str::from_utf8(line).map_err(|_| ParseError::Invalid {
                message: "invalid UTF-8 in integer".into(),
                offset: cursor.position() as usize,
            })?;
            let n: i64 = s.parse().map_err(|_| ParseError::Invalid {
                message: format!("invalid integer: {:?}", s),
                offset: cursor.position() as usize,
            })?;
            Ok(Frame::Integer(n))
        }
        b'$' => {
            let len = read_decimal(cursor)?;
            if len == -1 {
                return Ok(Frame::Null);
            }
            let len = len as usize;
            let pos = cursor.position() as usize;
            let data = &cursor.get_ref()[pos..pos + len];
            let frame = Frame::BulkString(Bytes::copy_from_slice(data));
            // Skip past data + \r\n
            skip(cursor, len + 2)?;
            Ok(frame)
        }
        b'*' => {
            let count = read_decimal(cursor)?;
            if count == -1 {
                return Ok(Frame::Null);
            }
            let count = count as usize;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(parse_frame(cursor)?);
            }
            Ok(Frame::Array(items))
        }
        byte => Err(ParseError::Invalid {
            message: format!("unknown type byte: 0x{:02x}", byte),
            offset: cursor.position() as usize - 1,
        }),
    }
}

/// Read a single byte from the cursor, or return Incomplete.
fn get_u8(cursor: &mut Cursor<&[u8]>) -> Result<u8, ParseError> {
    if !cursor.has_remaining() {
        return Err(ParseError::Incomplete);
    }
    Ok(cursor.get_u8())
}

/// Find the next CRLF sequence in the cursor.
/// Returns the bytes before the CRLF (the line content) and advances the cursor past the CRLF.
/// Returns Incomplete if no CRLF is found.
fn find_crlf<'a>(cursor: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], ParseError> {
    let start = cursor.position() as usize;
    let buf = cursor.get_ref();
    let end = buf.len();

    // Search for \r\n starting from current position
    if start >= end {
        return Err(ParseError::Incomplete);
    }

    for i in start..end.saturating_sub(1) {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            // Found CRLF at position i
            let line = &buf[start..i];
            cursor.set_position((i + 2) as u64);
            return Ok(line);
        }
    }

    Err(ParseError::Incomplete)
}

/// Read a CRLF-terminated decimal integer from the cursor.
fn read_decimal(cursor: &mut Cursor<&[u8]>) -> Result<i64, ParseError> {
    let line = find_crlf(cursor)?;
    let s = std::str::from_utf8(line).map_err(|_| ParseError::Invalid {
        message: "invalid UTF-8 in decimal".into(),
        offset: cursor.position() as usize,
    })?;
    s.parse::<i64>().map_err(|_| ParseError::Invalid {
        message: format!("invalid decimal: {:?}", s),
        offset: cursor.position() as usize,
    })
}

/// Advance the cursor by `n` bytes, or return Incomplete if not enough data.
fn skip(cursor: &mut Cursor<&[u8]>, n: usize) -> Result<(), ParseError> {
    let pos = cursor.position() as usize;
    let end = cursor.get_ref().len();
    if pos + n > end {
        return Err(ParseError::Incomplete);
    }
    cursor.set_position((pos + n) as u64);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_bytes(input: &[u8]) -> Result<Option<Frame>, ParseError> {
        let mut buf = BytesMut::from(input);
        parse(&mut buf, &ParseConfig::default())
    }

    fn parse_bytes_with_buf(input: &[u8]) -> (Result<Option<Frame>, ParseError>, BytesMut) {
        let mut buf = BytesMut::from(input);
        let result = parse(&mut buf, &ParseConfig::default());
        (result, buf)
    }

    // === Simple String tests ===

    #[test]
    fn test_parse_simple_string() {
        let result = parse_bytes(b"+OK\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
    }

    #[test]
    fn test_parse_simple_string_long() {
        let result = parse_bytes(b"+hello world\r\n").unwrap().unwrap();
        assert_eq!(
            result,
            Frame::SimpleString(Bytes::from_static(b"hello world"))
        );
    }

    // === Error tests ===

    #[test]
    fn test_parse_error() {
        let result = parse_bytes(b"-ERR unknown command\r\n").unwrap().unwrap();
        assert_eq!(
            result,
            Frame::Error(Bytes::from_static(b"ERR unknown command"))
        );
    }

    // === Integer tests ===

    #[test]
    fn test_parse_integer_positive() {
        let result = parse_bytes(b":1000\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::Integer(1000));
    }

    #[test]
    fn test_parse_integer_negative() {
        let result = parse_bytes(b":-42\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::Integer(-42));
    }

    #[test]
    fn test_parse_integer_zero() {
        let result = parse_bytes(b":0\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::Integer(0));
    }

    // === Bulk String tests ===

    #[test]
    fn test_parse_bulk_string() {
        let result = parse_bytes(b"$5\r\nhello\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"hello")));
    }

    #[test]
    fn test_parse_empty_bulk_string() {
        let result = parse_bytes(b"$0\r\n\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::BulkString(Bytes::new()));
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let result = parse_bytes(b"$-1\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_parse_binary_data_in_bulk_string() {
        // $4\r\n\r\n\r\n\r\n -- data is \r\n\r\n (4 bytes)
        let result = parse_bytes(b"$4\r\n\r\n\r\n\r\n").unwrap().unwrap();
        assert_eq!(
            result,
            Frame::BulkString(Bytes::from_static(b"\r\n\r\n"))
        );
    }

    // === Null Array tests ===

    #[test]
    fn test_parse_null_array() {
        let result = parse_bytes(b"*-1\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::Null);
    }

    // === Empty Array tests ===

    #[test]
    fn test_parse_empty_array() {
        let result = parse_bytes(b"*0\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::Array(vec![]));
    }

    // === Array tests ===

    #[test]
    fn test_parse_array_of_bulk_strings() {
        let result = parse_bytes(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(
            result,
            Frame::Array(vec![
                Frame::BulkString(Bytes::from_static(b"foo")),
                Frame::BulkString(Bytes::from_static(b"bar")),
            ])
        );
    }

    #[test]
    fn test_parse_nested_array() {
        let result = parse_bytes(b"*1\r\n*1\r\n:1\r\n").unwrap().unwrap();
        assert_eq!(
            result,
            Frame::Array(vec![Frame::Array(vec![Frame::Integer(1)])])
        );
    }

    #[test]
    fn test_parse_array_with_null_element() {
        let result = parse_bytes(b"*3\r\n$3\r\nhey\r\n$-1\r\n$3\r\nfoo\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(
            result,
            Frame::Array(vec![
                Frame::BulkString(Bytes::from_static(b"hey")),
                Frame::Null,
                Frame::BulkString(Bytes::from_static(b"foo")),
            ])
        );
    }

    // === Incomplete data tests ===

    #[test]
    fn test_parse_incomplete_simple_string() {
        let (result, buf) = parse_bytes_with_buf(b"+OK");
        assert!(result.unwrap().is_none());
        assert_eq!(&buf[..], b"+OK"); // buffer unchanged
    }

    #[test]
    fn test_parse_incomplete_bulk_string() {
        let (result, buf) = parse_bytes_with_buf(b"$5\r\nhel");
        assert!(result.unwrap().is_none());
        assert_eq!(&buf[..], b"$5\r\nhel"); // buffer unchanged
    }

    #[test]
    fn test_parse_incomplete_array() {
        let (result, buf) = parse_bytes_with_buf(b"*2\r\n$3\r\nfoo\r\n");
        assert!(result.unwrap().is_none());
        assert_eq!(&buf[..], b"*2\r\n$3\r\nfoo\r\n"); // buffer unchanged
    }

    #[test]
    fn test_parse_empty_buffer() {
        let result = parse_bytes(b"");
        assert!(result.unwrap().is_none());
    }

    // === Invalid data tests ===

    #[test]
    fn test_parse_non_resp_prefix_routes_to_inline() {
        // Non-RESP prefix bytes (like '!') are now routed to the inline parser
        let result = parse_bytes(b"!foo\r\n").unwrap().unwrap();
        assert_eq!(
            result,
            Frame::Array(vec![Frame::BulkString(Bytes::from_static(b"!foo"))])
        );
    }

    #[test]
    fn test_parse_bulk_string_exceeding_max_size() {
        let mut buf = BytesMut::from(&b"$999999999\r\n"[..]);
        let config = ParseConfig {
            max_bulk_string_size: 100,
            ..ParseConfig::default()
        };
        let result = parse(&mut buf, &config);
        assert!(matches!(result, Err(ParseError::Invalid { .. })));
    }

    #[test]
    fn test_parse_array_depth_exceeding_max() {
        // Create deeply nested array: *1\r\n*1\r\n*1\r\n ... :1\r\n
        let mut input = Vec::new();
        for _ in 0..10 {
            input.extend_from_slice(b"*1\r\n");
        }
        input.extend_from_slice(b":1\r\n");

        let mut buf = BytesMut::from(&input[..]);
        let config = ParseConfig {
            max_array_depth: 8,
            ..ParseConfig::default()
        };
        let result = parse(&mut buf, &config);
        assert!(matches!(result, Err(ParseError::Invalid { .. })));
    }

    // === Buffer consumption tests ===

    #[test]
    fn test_buffer_consumed_after_parse() {
        let mut buf = BytesMut::from(&b"+OK\r\nremaining"[..]);
        let result = parse(&mut buf, &ParseConfig::default()).unwrap().unwrap();
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(&buf[..], b"remaining");
    }

    #[test]
    fn test_parse_two_frames_sequentially() {
        let mut buf = BytesMut::from(&b"+OK\r\n:42\r\n"[..]);
        let config = ParseConfig::default();

        let frame1 = parse(&mut buf, &config).unwrap().unwrap();
        assert_eq!(frame1, Frame::SimpleString(Bytes::from_static(b"OK")));

        let frame2 = parse(&mut buf, &config).unwrap().unwrap();
        assert_eq!(frame2, Frame::Integer(42));

        assert!(buf.is_empty());
    }

    // === Inline dispatch integration tests ===

    #[test]
    fn test_parse_inline_ping_via_dispatch() {
        let result = parse_bytes(b"PING\r\n").unwrap().unwrap();
        assert_eq!(
            result,
            Frame::Array(vec![Frame::BulkString(Bytes::from_static(b"PING"))])
        );
    }

    #[test]
    fn test_parse_resp_simple_string_not_inline() {
        let result = parse_bytes(b"+OK\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
    }

    #[test]
    fn test_parse_resp_array_not_inline() {
        let result = parse_bytes(b"*1\r\n$4\r\nPING\r\n").unwrap().unwrap();
        assert_eq!(
            result,
            Frame::Array(vec![Frame::BulkString(Bytes::from_static(b"PING"))])
        );
    }
}
