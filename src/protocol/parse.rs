use memchr::memchr;

use bytes::{Buf, Bytes, BytesMut};

use super::frame::{Frame, ParseConfig, ParseError};
use super::inline;

/// Attempt to parse one RESP2 frame from the buffer.
///
/// Uses a single-pass approach: validates completeness and extracts frame data
/// in one traversal of the buffer. On success, advances the buffer past the
/// consumed bytes and returns `Ok(Some(frame))`.
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

    let mut pos = 0;
    match parse_single_frame(&buf[..], &mut pos, config, 0) {
        Ok(frame) => {
            buf.advance(pos);
            Ok(Some(frame))
        }
        Err(ParseError::Incomplete) => Ok(None),
        Err(e) => Err(e),
    }
}

/// SIMD-accelerated CRLF finder. Returns absolute position of \r in buf.
/// Returns None if no complete \r\n found starting from `start`.
#[inline]
fn find_crlf(buf: &[u8], start: usize) -> Option<usize> {
    if start >= buf.len() {
        return None;
    }
    let mut search_from = start;
    loop {
        match memchr(b'\r', &buf[search_from..]) {
            Some(rel_pos) => {
                let abs_pos = search_from + rel_pos;
                if abs_pos + 1 < buf.len() && buf[abs_pos + 1] == b'\n' {
                    return Some(abs_pos);
                }
                // Bare \r without \n -- skip past it and continue
                search_from = abs_pos + 1;
                if search_from >= buf.len() {
                    return None;
                }
            }
            None => return None,
        }
    }
}

/// Read a CRLF-terminated decimal integer from buf at position pos.
/// Advances pos past the CRLF.
#[inline]
fn read_decimal(buf: &[u8], pos: &mut usize) -> Result<i64, ParseError> {
    let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
    let line = &buf[*pos..crlf];
    let n = atoi::atoi::<i64>(line).ok_or_else(|| ParseError::Invalid {
        message: format!("invalid decimal: {:?}", String::from_utf8_lossy(line)),
        offset: *pos,
    })?;
    *pos = crlf + 2;
    Ok(n)
}

/// Parse a single RESP2 frame from buf using direct index tracking.
///
/// Validates completeness and extracts frame data simultaneously.
/// Returns `Err(ParseError::Incomplete)` if not enough data is available.
fn parse_single_frame(
    buf: &[u8],
    pos: &mut usize,
    config: &ParseConfig,
    depth: usize,
) -> Result<Frame, ParseError> {
    if depth > config.max_array_depth {
        return Err(ParseError::Invalid {
            message: format!(
                "array nesting depth {} exceeds maximum {}",
                depth, config.max_array_depth
            ),
            offset: *pos,
        });
    }
    if *pos >= buf.len() {
        return Err(ParseError::Incomplete);
    }
    let type_byte = buf[*pos];
    *pos += 1;

    match type_byte {
        b'+' => {
            let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
            let line = &buf[*pos..crlf];
            *pos = crlf + 2;
            Ok(Frame::SimpleString(Bytes::copy_from_slice(line)))
        }
        b'-' => {
            let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
            let line = &buf[*pos..crlf];
            *pos = crlf + 2;
            Ok(Frame::Error(Bytes::copy_from_slice(line)))
        }
        b':' => {
            let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
            let line = &buf[*pos..crlf];
            let n = atoi::atoi::<i64>(line).ok_or_else(|| ParseError::Invalid {
                message: format!("invalid integer: {:?}", String::from_utf8_lossy(line)),
                offset: *pos,
            })?;
            *pos = crlf + 2;
            Ok(Frame::Integer(n))
        }
        b'$' => {
            let len = read_decimal(buf, pos)?;
            if len == -1 {
                return Ok(Frame::Null);
            }
            if len < 0 {
                return Err(ParseError::Invalid {
                    message: format!("invalid bulk string length: {}", len),
                    offset: *pos,
                });
            }
            let len = len as usize;
            if len > config.max_bulk_string_size {
                return Err(ParseError::Invalid {
                    message: format!(
                        "bulk string size {} exceeds maximum {}",
                        len, config.max_bulk_string_size
                    ),
                    offset: *pos,
                });
            }
            // CRITICAL: Do NOT scan for CRLF inside bulk string data (Pitfall 6).
            // The length tells us exactly where the terminator is.
            let remaining = buf.len() - *pos;
            if remaining < len + 2 {
                return Err(ParseError::Incomplete);
            }
            let data = &buf[*pos..*pos + len];
            let frame = Frame::BulkString(Bytes::copy_from_slice(data));
            *pos += len + 2; // skip data + \r\n
            Ok(frame)
        }
        b'*' => {
            let count = read_decimal(buf, pos)?;
            if count == -1 {
                return Ok(Frame::Null);
            }
            if count < 0 {
                return Err(ParseError::Invalid {
                    message: format!("invalid array length: {}", count),
                    offset: *pos,
                });
            }
            let count = count as usize;
            if count > config.max_array_length {
                return Err(ParseError::Invalid {
                    message: format!(
                        "array length {} exceeds maximum {}",
                        count, config.max_array_length
                    ),
                    offset: *pos,
                });
            }
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(parse_single_frame(buf, pos, config, depth + 1)?);
            }
            Ok(Frame::Array(items))
        }
        byte => Err(ParseError::Invalid {
            message: format!("unknown type byte: 0x{:02x}", byte),
            offset: *pos - 1,
        }),
    }
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
