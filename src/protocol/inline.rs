use bytes::{Buf, Bytes, BytesMut};

use super::frame::{Frame, ParseError};

/// Parse an inline command from the buffer.
///
/// Inline commands are plain text lines terminated by `\r\n`, where arguments
/// are separated by whitespace (spaces or tabs). This is what telnet users
/// and redis-cli direct input send.
///
/// Returns `Ok(Some(Frame::Array(...)))` with each argument as a `BulkString`,
/// `Ok(None)` if the buffer doesn't contain a complete line (no `\r\n` found),
/// or `Ok(None)` for empty/whitespace-only lines (after advancing past the CRLF).
pub fn parse_inline(buf: &mut BytesMut) -> Result<Option<Frame>, ParseError> {
    // Find the CRLF terminator
    let crlf_pos = match find_crlf_position(&buf[..]) {
        Some(pos) => pos,
        None => return Ok(None), // Incomplete -- need more data
    };

    // Extract line content before CRLF
    let line = &buf[..crlf_pos];

    // Split by whitespace (spaces and tabs), filtering empty slices
    let args: Vec<Frame> = line
        .split(|&b| b == b' ' || b == b'\t')
        .filter(|s| !s.is_empty())
        .map(|token| Frame::BulkString(Bytes::copy_from_slice(token)))
        .collect();

    // Advance buffer past line + CRLF
    buf.advance(crlf_pos + 2);

    // Empty/whitespace-only lines produce no frame
    if args.is_empty() {
        return Ok(None);
    }

    Ok(Some(Frame::Array(args)))
}

/// Search for a `\r\n` pair in the buffer, returning the position of `\r`.
fn find_crlf_position(buf: &[u8]) -> Option<usize> {
    if buf.len() < 2 {
        return None;
    }
    for i in 0..buf.len() - 1 {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_inline_bytes(input: &[u8]) -> Result<Option<Frame>, ParseError> {
        let mut buf = BytesMut::from(input);
        parse_inline(&mut buf)
    }

    #[test]
    fn test_parse_inline_ping() {
        let result = parse_inline_bytes(b"PING\r\n").unwrap().unwrap();
        assert_eq!(
            result,
            Frame::Array(vec![Frame::BulkString(Bytes::from_static(b"PING"))])
        );
    }

    #[test]
    fn test_parse_inline_set_key_value() {
        let result = parse_inline_bytes(b"SET key value\r\n").unwrap().unwrap();
        assert_eq!(
            result,
            Frame::Array(vec![
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
            Frame::Array(vec![
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
        let frame1 = parse_inline(&mut buf).unwrap().unwrap();
        assert_eq!(
            frame1,
            Frame::Array(vec![
                Frame::BulkString(Bytes::from_static(b"GET")),
                Frame::BulkString(Bytes::from_static(b"key")),
            ])
        );
        let frame2 = parse_inline(&mut buf).unwrap().unwrap();
        assert_eq!(
            frame2,
            Frame::Array(vec![Frame::BulkString(Bytes::from_static(b"PING"))])
        );
    }

    #[test]
    fn test_parse_inline_leading_whitespace() {
        let result = parse_inline_bytes(b"  PING\r\n").unwrap().unwrap();
        assert_eq!(
            result,
            Frame::Array(vec![Frame::BulkString(Bytes::from_static(b"PING"))])
        );
    }

    #[test]
    fn test_parse_inline_tab_separated() {
        let result = parse_inline_bytes(b"SET\tkey\tvalue\r\n").unwrap().unwrap();
        assert_eq!(
            result,
            Frame::Array(vec![
                Frame::BulkString(Bytes::from_static(b"SET")),
                Frame::BulkString(Bytes::from_static(b"key")),
                Frame::BulkString(Bytes::from_static(b"value")),
            ])
        );
    }

    #[test]
    fn test_parse_inline_buffer_consumed() {
        let mut buf = BytesMut::from(&b"PING\r\nremaining"[..]);
        let _ = parse_inline(&mut buf).unwrap().unwrap();
        assert_eq!(&buf[..], b"remaining");
    }

    #[test]
    fn test_parse_inline_empty_line_buffer_consumed() {
        let mut buf = BytesMut::from(&b"\r\nPING\r\n"[..]);
        let result = parse_inline(&mut buf).unwrap();
        assert!(result.is_none());
        assert_eq!(&buf[..], b"PING\r\n");
    }
}
