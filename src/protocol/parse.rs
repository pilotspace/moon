#![allow(unused_imports, dead_code)]
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

    // Dispatch: RESP2/RESP3 prefixed bytes go to RESP parser, everything else is inline
    match buf[0] {
        b'+' | b'-' | b':' | b'$' | b'*' // RESP2
        | b'%' | b'~' | b',' | b'#' | b'_' | b'=' | b'(' | b'>' // RESP3
        => { /* fall through to RESP parsing below */ }
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

/// Single-pass parser that produces zero-copy frames using Bytes::slice().
/// Works on a frozen `Bytes` buffer for Arc-backed sub-slicing.
fn parse_single_frame_zc(
    buf: &Bytes,
    pos: &mut usize,
    config: &ParseConfig,
    depth: usize,
) -> Result<Frame, ParseError> {
    if depth > config.max_array_depth {
        return Err(ParseError::Invalid {
            message: format!("array nesting depth {} exceeds maximum {}", depth, config.max_array_depth),
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
            let line = buf.slice(*pos..crlf);
            *pos = crlf + 2;
            Ok(Frame::SimpleString(line))
        }
        b'-' => {
            let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
            let line = buf.slice(*pos..crlf);
            *pos = crlf + 2;
            Ok(Frame::Error(line))
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
            let len = read_decimal_zc(buf, pos)?;
            if len == -1 { return Ok(Frame::Null); }
            if len < 0 {
                return Err(ParseError::Invalid {
                    message: format!("invalid bulk string length: {}", len),
                    offset: *pos,
                });
            }
            let len = len as usize;
            if len > config.max_bulk_string_size {
                return Err(ParseError::Invalid {
                    message: format!("bulk string size {} exceeds maximum {}", len, config.max_bulk_string_size),
                    offset: *pos,
                });
            }
            let remaining = buf.len() - *pos;
            if remaining < len + 2 {
                return Err(ParseError::Incomplete);
            }
            // ZERO-COPY: Bytes::slice() does Arc refcount bump, no memcpy
            let data = buf.slice(*pos..*pos + len);
            *pos += len + 2;
            Ok(Frame::BulkString(data))
        }
        b'*' => {
            let count = read_decimal_zc(buf, pos)?;
            if count == -1 { return Ok(Frame::Null); }
            if count < 0 {
                return Err(ParseError::Invalid {
                    message: format!("invalid array count: {}", count),
                    offset: *pos,
                });
            }
            let count = count as usize;
            if count > config.max_array_length {
                return Err(ParseError::Invalid {
                    message: format!("array length {} exceeds maximum {}", count, config.max_array_length),
                    offset: *pos,
                });
            }
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(parse_single_frame_zc(buf, pos, config, depth + 1)?);
            }
            Ok(Frame::Array(items))
        }
        b'%' => {
            let count = read_decimal_zc(buf, pos)?;
            if count < 0 { return Err(ParseError::Invalid { message: "invalid map count".into(), offset: *pos }); }
            let count = count as usize;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let key = parse_single_frame_zc(buf, pos, config, depth + 1)?;
                let val = parse_single_frame_zc(buf, pos, config, depth + 1)?;
                entries.push((key, val));
            }
            Ok(Frame::Map(entries))
        }
        b'~' => {
            let count = read_decimal_zc(buf, pos)?;
            let count = count as usize;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(parse_single_frame_zc(buf, pos, config, depth + 1)?);
            }
            Ok(Frame::Set(items))
        }
        b',' => {
            let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
            let line = &buf[*pos..crlf];
            let s = std::str::from_utf8(line).map_err(|_| ParseError::Invalid {
                message: "invalid UTF-8 in double".into(), offset: *pos,
            })?;
            let f = if s == "inf" { f64::INFINITY }
            else if s == "-inf" { f64::NEG_INFINITY }
            else { s.parse::<f64>().map_err(|_| ParseError::Invalid {
                message: format!("invalid double: {}", s), offset: *pos,
            })? };
            *pos = crlf + 2;
            Ok(Frame::Double(f))
        }
        b'#' => {
            if *pos + 2 >= buf.len() { return Err(ParseError::Incomplete); }
            let val = buf[*pos];
            // Boolean format: #t\r\n or #f\r\n — exactly 1 char then CRLF
            if (val != b't' && val != b'f') || buf[*pos + 1] != b'\r' || buf[*pos + 2] != b'\n' {
                return Err(ParseError::Invalid {
                    message: format!("invalid boolean format at offset {}", *pos),
                    offset: *pos,
                });
            }
            *pos += 3;
            Ok(Frame::Boolean(val == b't'))
        }
        b'_' => {
            *pos += 2;
            Ok(Frame::Null)
        }
        b'=' => {
            let len = read_decimal_zc(buf, pos)? as usize;
            let remaining = buf.len() - *pos;
            if remaining < len + 2 { return Err(ParseError::Incomplete); }
            let payload = &buf[*pos..*pos + len];
            let encoding = Bytes::copy_from_slice(&payload[..3]);
            let data = buf.slice(*pos + 4..*pos + len);
            *pos += len + 2;
            Ok(Frame::VerbatimString { encoding, data })
        }
        b'(' => {
            let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
            let line = buf.slice(*pos..crlf);
            *pos = crlf + 2;
            Ok(Frame::BigNumber(line))
        }
        b'>' => {
            let count = read_decimal_zc(buf, pos)? as usize;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(parse_single_frame_zc(buf, pos, config, depth + 1)?);
            }
            Ok(Frame::Push(items))
        }
        other => Err(ParseError::Invalid {
            message: format!("unknown type byte: 0x{:02x}", other),
            offset: *pos - 1,
        }),
    }
}

/// Read a decimal integer from the frozen buffer (for zero-copy parser).
fn read_decimal_zc(buf: &Bytes, pos: &mut usize) -> Result<i64, ParseError> {
    let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
    let line = &buf[*pos..crlf];
    let n = atoi::atoi::<i64>(line).ok_or_else(|| ParseError::Invalid {
        message: format!("invalid decimal: {:?}", String::from_utf8_lossy(line)),
        offset: *pos,
    })?;
    *pos = crlf + 2;
    Ok(n)
}

/// Zero-copy frame extraction from a frozen `Bytes` buffer.
/// Called AFTER validation succeeds, so all bounds are guaranteed safe.
/// Uses `bytes.slice(start..end)` for zero-copy sub-slicing (Arc refcount bump only).
fn parse_frame_zerocopy(
    buf: &Bytes,
    pos: &mut usize,
    config: &ParseConfig,
    depth: usize,
) -> Frame {
    let type_byte = buf[*pos];
    *pos += 1;

    match type_byte {
        b'+' => {
            let crlf = find_crlf(buf, *pos).unwrap();
            let line = buf.slice(*pos..crlf);
            *pos = crlf + 2;
            Frame::SimpleString(line)
        }
        b'-' => {
            let crlf = find_crlf(buf, *pos).unwrap();
            let line = buf.slice(*pos..crlf);
            *pos = crlf + 2;
            Frame::Error(line)
        }
        b':' => {
            let crlf = find_crlf(buf, *pos).unwrap();
            let line = &buf[*pos..crlf];
            let n = atoi::atoi::<i64>(line).unwrap();
            *pos = crlf + 2;
            Frame::Integer(n)
        }
        b'$' => {
            // Read length
            let crlf = find_crlf(buf, *pos).unwrap();
            let line = &buf[*pos..crlf];
            let len_val = atoi::atoi::<i64>(line).unwrap();
            *pos = crlf + 2;
            if len_val == -1 {
                return Frame::Null;
            }
            let len = len_val as usize;
            // Zero-copy: slice the Bytes (Arc refcount bump, no memcpy)
            let data = buf.slice(*pos..*pos + len);
            *pos += len + 2;
            Frame::BulkString(data)
        }
        b'*' => {
            let crlf = find_crlf(buf, *pos).unwrap();
            let line = &buf[*pos..crlf];
            let count = atoi::atoi::<i64>(line).unwrap();
            *pos = crlf + 2;
            if count == -1 {
                return Frame::Null;
            }
            let count = count as usize;
            let mut items = Vec::with_capacity(count.min(config.max_array_length));
            for _ in 0..count {
                items.push(parse_frame_zerocopy(buf, pos, config, depth + 1));
            }
            Frame::Array(items)
        }
        b'%' => {
            // Map
            let crlf = find_crlf(buf, *pos).unwrap();
            let line = &buf[*pos..crlf];
            let count = atoi::atoi::<i64>(line).unwrap() as usize;
            *pos = crlf + 2;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let key = parse_frame_zerocopy(buf, pos, config, depth + 1);
                let val = parse_frame_zerocopy(buf, pos, config, depth + 1);
                entries.push((key, val));
            }
            Frame::Map(entries)
        }
        b'~' => {
            // Set
            let crlf = find_crlf(buf, *pos).unwrap();
            let line = &buf[*pos..crlf];
            let count = atoi::atoi::<i64>(line).unwrap() as usize;
            *pos = crlf + 2;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(parse_frame_zerocopy(buf, pos, config, depth + 1));
            }
            Frame::Set(items)
        }
        b',' => {
            // Double
            let crlf = find_crlf(buf, *pos).unwrap();
            let line = &buf[*pos..crlf];
            let s = std::str::from_utf8(line).unwrap();
            let f = if s == "inf" { f64::INFINITY }
            else if s == "-inf" { f64::NEG_INFINITY }
            else { s.parse::<f64>().unwrap() };
            *pos = crlf + 2;
            Frame::Double(f)
        }
        b'#' => {
            // Boolean
            let val = buf[*pos];
            *pos += 3; // t/f + \r\n
            Frame::Boolean(val == b't')
        }
        b'_' => {
            *pos += 2; // \r\n
            Frame::Null
        }
        b'=' => {
            // Verbatim string
            let crlf = find_crlf(buf, *pos).unwrap();
            let line = &buf[*pos..crlf];
            let len = atoi::atoi::<i64>(line).unwrap() as usize;
            *pos = crlf + 2;
            let payload = &buf[*pos..*pos + len];
            let encoding = Bytes::copy_from_slice(&payload[..3]);
            let data = buf.slice(*pos + 4..*pos + len);
            *pos += len + 2;
            Frame::VerbatimString { encoding, data }
        }
        b'(' => {
            // Big number
            let crlf = find_crlf(buf, *pos).unwrap();
            let line = buf.slice(*pos..crlf);
            *pos = crlf + 2;
            Frame::BigNumber(line)
        }
        b'>' => {
            // Push
            let crlf = find_crlf(buf, *pos).unwrap();
            let line = &buf[*pos..crlf];
            let count = atoi::atoi::<i64>(line).unwrap() as usize;
            *pos = crlf + 2;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(parse_frame_zerocopy(buf, pos, config, depth + 1));
            }
            Frame::Push(items)
        }
        _ => Frame::Null, // unreachable after validation
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
        // === RESP3 types ===
        b'_' => {
            // RESP3 Null: `_\r\n`
            let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
            *pos = crlf + 2;
            Ok(Frame::Null)
        }
        b'#' => {
            // RESP3 Boolean: `#t\r\n` or `#f\r\n`
            let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
            let line = &buf[*pos..crlf];
            let val = match line {
                b"t" => true,
                b"f" => false,
                _ => {
                    return Err(ParseError::Invalid {
                        message: format!(
                            "invalid boolean value: {:?}",
                            String::from_utf8_lossy(line)
                        ),
                        offset: *pos,
                    });
                }
            };
            *pos = crlf + 2;
            Ok(Frame::Boolean(val))
        }
        b',' => {
            // RESP3 Double: `,<double>\r\n`
            let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
            let line = &buf[*pos..crlf];
            let s = std::str::from_utf8(line).map_err(|_| ParseError::Invalid {
                message: "invalid UTF-8 in double".into(),
                offset: *pos,
            })?;
            let val = if s.eq_ignore_ascii_case("inf") {
                f64::INFINITY
            } else if s.eq_ignore_ascii_case("-inf") {
                f64::NEG_INFINITY
            } else if s.eq_ignore_ascii_case("nan") {
                f64::NAN
            } else {
                s.parse::<f64>().map_err(|_| ParseError::Invalid {
                    message: format!("invalid double: {:?}", s),
                    offset: *pos,
                })?
            };
            *pos = crlf + 2;
            Ok(Frame::Double(val))
        }
        b'(' => {
            // RESP3 BigNumber: `(<number>\r\n`
            let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
            let line = &buf[*pos..crlf];
            *pos = crlf + 2;
            Ok(Frame::BigNumber(Bytes::copy_from_slice(line)))
        }
        b'=' => {
            // RESP3 VerbatimString: `=<len>\r\n<enc>:<data>\r\n`
            let len = read_decimal(buf, pos)?;
            if len < 4 {
                return Err(ParseError::Invalid {
                    message: format!(
                        "verbatim string length {} too short (minimum 4 for encoding + colon)",
                        len
                    ),
                    offset: *pos,
                });
            }
            let len = len as usize;
            if len > config.max_bulk_string_size {
                return Err(ParseError::Invalid {
                    message: format!(
                        "verbatim string size {} exceeds maximum {}",
                        len, config.max_bulk_string_size
                    ),
                    offset: *pos,
                });
            }
            let remaining = buf.len() - *pos;
            if remaining < len + 2 {
                return Err(ParseError::Incomplete);
            }
            let payload = &buf[*pos..*pos + len];
            if payload[3] != b':' {
                return Err(ParseError::Invalid {
                    message: "verbatim string missing ':' after 3-byte encoding".into(),
                    offset: *pos + 3,
                });
            }
            let encoding = Bytes::copy_from_slice(&payload[..3]);
            let data = Bytes::copy_from_slice(&payload[4..]);
            *pos += len + 2;
            Ok(Frame::VerbatimString { encoding, data })
        }
        b'%' => {
            // RESP3 Map: `%<count>\r\n<key><value>...`
            let count = read_decimal(buf, pos)?;
            if count < 0 {
                return Err(ParseError::Invalid {
                    message: format!("invalid map length: {}", count),
                    offset: *pos,
                });
            }
            let count = count as usize;
            if count > config.max_array_length {
                return Err(ParseError::Invalid {
                    message: format!(
                        "map length {} exceeds maximum {}",
                        count, config.max_array_length
                    ),
                    offset: *pos,
                });
            }
            let mut pairs = Vec::with_capacity(count);
            for _ in 0..count {
                let key = parse_single_frame(buf, pos, config, depth + 1)?;
                let value = parse_single_frame(buf, pos, config, depth + 1)?;
                pairs.push((key, value));
            }
            Ok(Frame::Map(pairs))
        }
        b'~' => {
            // RESP3 Set: `~<count>\r\n<elements...>`
            let count = read_decimal(buf, pos)?;
            if count < 0 {
                return Err(ParseError::Invalid {
                    message: format!("invalid set length: {}", count),
                    offset: *pos,
                });
            }
            let count = count as usize;
            if count > config.max_array_length {
                return Err(ParseError::Invalid {
                    message: format!(
                        "set length {} exceeds maximum {}",
                        count, config.max_array_length
                    ),
                    offset: *pos,
                });
            }
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(parse_single_frame(buf, pos, config, depth + 1)?);
            }
            Ok(Frame::Set(items))
        }
        b'>' => {
            // RESP3 Push: `><count>\r\n<elements...>`
            let count = read_decimal(buf, pos)?;
            if count < 0 {
                return Err(ParseError::Invalid {
                    message: format!("invalid push length: {}", count),
                    offset: *pos,
                });
            }
            let count = count as usize;
            if count > config.max_array_length {
                return Err(ParseError::Invalid {
                    message: format!(
                        "push length {} exceeds maximum {}",
                        count, config.max_array_length
                    ),
                    offset: *pos,
                });
            }
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(parse_single_frame(buf, pos, config, depth + 1)?);
            }
            Ok(Frame::Push(items))
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

    // === RESP3 parse tests ===

    #[test]
    fn test_parse_resp3_null() {
        let result = parse_bytes(b"_\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_parse_resp3_boolean_true() {
        let result = parse_bytes(b"#t\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::Boolean(true));
    }

    #[test]
    fn test_parse_resp3_boolean_false() {
        let result = parse_bytes(b"#f\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::Boolean(false));
    }

    #[test]
    fn test_parse_resp3_double() {
        let result = parse_bytes(b",1.23\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::Double(1.23));
    }

    #[test]
    fn test_parse_resp3_double_inf() {
        let result = parse_bytes(b",inf\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::Double(f64::INFINITY));
    }

    #[test]
    fn test_parse_resp3_double_neg_inf() {
        let result = parse_bytes(b",-inf\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::Double(f64::NEG_INFINITY));
    }

    #[test]
    fn test_parse_resp3_big_number() {
        let result = parse_bytes(b"(3492890328409238509324850943850943825024385\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(
            result,
            Frame::BigNumber(Bytes::from_static(
                b"3492890328409238509324850943850943825024385"
            ))
        );
    }

    #[test]
    fn test_parse_resp3_verbatim_string() {
        let result = parse_bytes(b"=15\r\ntxt:Some string\r\n").unwrap().unwrap();
        assert_eq!(
            result,
            Frame::VerbatimString {
                encoding: Bytes::from_static(b"txt"),
                data: Bytes::from_static(b"Some string"),
            }
        );
    }

    #[test]
    fn test_parse_resp3_map() {
        let result = parse_bytes(b"%2\r\n+key1\r\n:1\r\n+key2\r\n:2\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(
            result,
            Frame::Map(vec![
                (
                    Frame::SimpleString(Bytes::from_static(b"key1")),
                    Frame::Integer(1)
                ),
                (
                    Frame::SimpleString(Bytes::from_static(b"key2")),
                    Frame::Integer(2)
                ),
            ])
        );
    }

    #[test]
    fn test_parse_resp3_set() {
        let result = parse_bytes(b"~3\r\n+a\r\n+b\r\n+c\r\n").unwrap().unwrap();
        assert_eq!(
            result,
            Frame::Set(vec![
                Frame::SimpleString(Bytes::from_static(b"a")),
                Frame::SimpleString(Bytes::from_static(b"b")),
                Frame::SimpleString(Bytes::from_static(b"c")),
            ])
        );
    }

    #[test]
    fn test_parse_resp3_push() {
        let result = parse_bytes(b">2\r\n$10\r\ninvalidate\r\n*1\r\n$3\r\nfoo\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(
            result,
            Frame::Push(vec![
                Frame::BulkString(Bytes::from_static(b"invalidate")),
                Frame::Array(vec![Frame::BulkString(Bytes::from_static(b"foo"))]),
            ])
        );
    }

    #[test]
    fn test_parse_resp3_incomplete_boolean() {
        let (result, _) = parse_bytes_with_buf(b"#t");
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_parse_resp3_incomplete_map() {
        let (result, _) = parse_bytes_with_buf(b"%2\r\n+key1\r\n");
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_parse_resp3_incomplete_double() {
        let (result, _) = parse_bytes_with_buf(b",1.23");
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_parse_resp3_boolean_invalid() {
        // #foo is not a valid boolean, should error (not route to inline)
        let result = parse_bytes(b"#foo\r\n");
        assert!(result.is_err());
    }
}
