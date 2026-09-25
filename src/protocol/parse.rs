use atoi::FromRadix10SignedChecked;
use memchr::memchr;

use bytes::{Buf, Bytes, BytesMut};

use super::flat::{FlatScan, build_flat, scan_flat};
use super::frame::{Frame, FrameVec, ParseConfig, ParseError, ProtoFault};
use super::inline;

pub use super::flat::ParseState;

/// Attempt to parse one RESP2/RESP3 frame from the buffer.
///
/// Stateless form of [`parse_resumable`]: every call starts from byte 0 of
/// `buf`. Use it for buffers that hold whole frames (AOF replay, tests, reply
/// parsing). A read loop that feeds one buffer across socket reads must use
/// [`parse_resumable`] with a per-connection [`ParseState`], or a large frame
/// arriving in many reads is re-scanned from the start on every one of them.
///
/// On success, advances the buffer past the consumed bytes and returns `Ok(Some(frame))`.
/// Returns `Ok(None)` if the buffer doesn't contain a complete frame (need more data).
/// Returns `Err` if the data violates the RESP2 protocol specification.
#[inline]
pub fn parse(buf: &mut BytesMut, config: &ParseConfig) -> Result<Option<Frame>, ParseError> {
    parse_resumable(buf, config, &mut ParseState::new())
}

/// Parse one RESP2/RESP3 frame, resuming the scan of an incomplete frame at the
/// front of `buf` where the previous call on the same `state` stopped.
///
/// A flat top-level `*N` of `$`-bulks -- the shape of essentially every client
/// command -- is scanned in **one** pass by `flat::scan_flat`, which records
/// each argument's span while it validates and answers one of three things:
///
/// - **Complete**: the `Frame` is built straight from the spans.
/// - **Incomplete**: `Ok(None)`, and nothing else runs. `validate_frame` would
///   report `Incomplete` for exactly the same prefixes; asking it too is what
///   made every read of a large upload cost two full walks (moon#1164).
/// - **Decline**: anything that is not exactly that shape -- malformed lengths,
///   negative or out-of-range counts, null bulks, nested or non-bulk elements,
///   RESP3 containers, inline commands -- takes the original two-pass path:
///   1. Validate structure and compute byte length (`validate_frame`)
///   2. Freeze validated bytes and extract frame data via `Bytes::slice`
///
/// Declining is always safe; answering differently never is, which is what
/// `single_pass_multibulk_agrees_with_two_pass`, the every-prefix resumable
/// tests and the `resp_parse_fused` / `resp_parse_resumable` fuzz targets
/// assert over arbitrary input.
///
/// `state` carries the cursor between calls. See [`ParseState`] for the one
/// rule callers must follow: anything else that consumes the front of `buf`
/// resets it.
pub fn parse_resumable(
    buf: &mut BytesMut,
    config: &ParseConfig,
    state: &mut ParseState,
) -> Result<Option<Frame>, ParseError> {
    // Resume the frame at the front where the last call stopped. Only an
    // "incomplete" answer is taken from the resumed scan; a frame it calls
    // complete is re-verified from byte 0 below before it is built, so a stale
    // cursor can cost a scan but never produce a frame.
    if let Some(cursor) = state.take_resumable(&buf[..]) {
        if let FlatScan::Incomplete {
            cursor: Some(next),
            need,
        } = scan_flat(&buf[..], config, Some(cursor))
        {
            #[cfg(test)]
            if state.verify_resume() {
                let fresh = scan_flat(&buf[..], config, None);
                assert!(
                    matches!(fresh, FlatScan::Incomplete { cursor: Some(f), .. } if f == next),
                    "resumed scan disagrees with a from-zero scan: stale ParseState"
                );
            }
            state.note_incomplete(Some(next), buf.len(), need);
            return Ok(None);
        }
    }

    match dispatch_prefix(buf, config)? {
        PrefixOutcome::Done(frame) => {
            state.note_other();
            return Ok(frame);
        }
        PrefixOutcome::Resp => {}
    }
    match scan_flat(&buf[..], config, None) {
        FlatScan::Complete {
            total_len,
            count,
            first_elem,
            spans,
        } => {
            state.note_other();
            Ok(Some(build_flat(buf, total_len, count, first_elem, &spans)))
        }
        FlatScan::Incomplete { cursor, need } => {
            state.note_incomplete(cursor, buf.len(), need);
            Ok(None)
        }
        FlatScan::Decline => {
            state.note_other();
            parse_resp_two_pass(buf, config)
        }
    }
}

/// `parse` with the single-pass fast path removed -- the two-pass pipeline exactly
/// as it stood before it was added.
///
/// Exists so the differential test and the `resp_parse_fused` fuzz target can
/// assert byte-identical behaviour on arbitrary input. Not compiled into a
/// release build.
#[cfg(any(test, feature = "fuzzing"))]
pub fn parse_reference_two_pass(
    buf: &mut BytesMut,
    config: &ParseConfig,
) -> Result<Option<Frame>, ParseError> {
    match dispatch_prefix(buf, config)? {
        PrefixOutcome::Done(frame) => return Ok(frame),
        PrefixOutcome::Resp => {}
    }
    parse_resp_two_pass(buf, config)
}

/// What [`dispatch_prefix`] decided about the head of the buffer.
enum PrefixOutcome {
    /// Answered outright (inline command, blank line, or empty buffer).
    Done(Option<Frame>),
    /// The buffer starts with a RESP type byte; the RESP parsers own it.
    Resp,
}

/// Route the head of the buffer to the RESP parser or the inline splitter.
fn dispatch_prefix(buf: &mut BytesMut, config: &ParseConfig) -> Result<PrefixOutcome, ParseError> {
    // Dispatch: RESP2/RESP3 prefixed bytes go to RESP parser, everything else
    // is inline.
    //
    // The loop exists for blank inline lines (#578). `parse_inline` answers
    // `Ok(None)` both for "that line was empty, I consumed it" and for "I need
    // more bytes" — and every read loop reads the second meaning and parks. So
    // `\r\n\r\nECHO hi\r\n` arriving in ONE read left the `ECHO` unparsed until
    // unrelated later traffic happened to wake the loop; redis-server answers
    // it immediately. Resolving it here, at the single funnel the codec and all
    // three connection handlers share, is what keeps the fix from being
    // CI-invisible in whichever handler got missed.
    //
    // Re-dispatching from the top (rather than retrying the inline splitter) is
    // deliberate: what follows a blank line is very often a RESP array, and
    // feeding `*1` to the inline path would turn a real command into the
    // literal token `*1`.
    //
    // Termination: each iteration either returns, or consumes at least the one
    // byte of a line terminator, so the buffer strictly shrinks.
    loop {
        if buf.is_empty() {
            return Ok(PrefixOutcome::Done(None));
        }
        match buf[0] {
            b'+' | b'-' | b':' | b'$' | b'*' // RESP2
            | b'%' | b'~' | b',' | b'#' | b'_' | b'=' | b'(' | b'>' // RESP3
            => return Ok(PrefixOutcome::Resp), // the RESP parsers own it
            _ => {
                let before = buf.len();
                match inline::parse_inline(buf, config.max_inline_size)? {
                    Some(frame) => return Ok(PrefixOutcome::Done(Some(frame))),
                    // Nothing consumed => genuinely incomplete, so waiting for
                    // more bytes is correct and looping would spin.
                    None if buf.len() == before => return Ok(PrefixOutcome::Done(None)),
                    // Bytes consumed but no frame: an empty or whitespace-only
                    // line. Ask again — there may be a real command behind it.
                    None => continue,
                }
            }
        }
    }
}

/// The original two-pass RESP pipeline: validate, freeze, extract.
///
/// The caller has already established that `buf` is non-empty and starts with a
/// RESP type byte.
fn parse_resp_two_pass(
    buf: &mut BytesMut,
    config: &ParseConfig,
) -> Result<Option<Frame>, ParseError> {
    // Pass 1: Validate structure and compute total byte length (zero allocations)
    let mut pos = 0;
    match validate_frame(&buf[..], &mut pos, config, 0) {
        Ok(()) => {
            // A top-level multibulk with a count BELOW -1 (`*-9`) is
            // well-formed but carries no command. Redis consumes it and says
            // nothing at all. Consume the bytes — so the caller does not spin
            // on them forever — and report "no frame here", which is exactly
            // what `Ok(None)` means to the read loops.
            //
            // `*-1` is EXCLUDED: it is the canonical null array and must keep
            // yielding `Frame::Null`, because `parse()` also parses replies
            // (replication), not just requests. Folding it in here broke
            // `test_parse_null_array`.
            let is_null_multibulk = buf[0] == b'*'
                && buf.len() > 3
                && buf[1] == b'-'
                && !(buf[2] == b'1' && buf[3] == b'\r')
                && matches!(find_crlf(&buf[..], 1), Some(c) if c == pos - 2);
            if is_null_multibulk {
                buf.advance(pos);
                return Ok(None);
            }
            // Pass 2: Zero-copy extraction from frozen Bytes
            // split_to moves bytes out of buf; freeze() enables Arc-backed slicing
            let frozen = buf.split_to(pos).freeze();
            let mut zc_pos = 0;
            let frame = parse_frame_zerocopy(&frozen, &mut zc_pos, config, 0);
            Ok(Some(frame))
        }
        Err(ParseError::Incomplete) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Zero-copy frame extraction from a frozen `Bytes` buffer.
/// Called AFTER validation succeeds, so all CRLF/atoi lookups should succeed.
/// Uses `bytes.slice(start..end)` for zero-copy sub-slicing (Arc refcount bump only).
///
/// Defensive: returns `Frame::Null` on any parse failure rather than panicking,
/// because validation/zerocopy position divergence bugs exist (found by fuzzing).
fn parse_frame_zerocopy(buf: &Bytes, pos: &mut usize, config: &ParseConfig, depth: usize) -> Frame {
    if *pos >= buf.len() {
        return Frame::Null;
    }
    let type_byte = buf[*pos];
    *pos += 1;

    // Helper: find CRLF or bail to Frame::Null
    macro_rules! crlf_or_null {
        ($buf:expr, $pos:expr) => {
            match find_crlf($buf, *$pos) {
                Some(p) => p,
                None => return Frame::Null,
            }
        };
    }

    // Helper: strict integer parse or bail to Frame::Null
    macro_rules! atoi_or_null {
        ($line:expr) => {
            match strict_atoi($line) {
                Some(n) => n,
                None => return Frame::Null,
            }
        };
    }

    // Helper: parse count for collection types (array/set/push/map)
    macro_rules! parse_count {
        ($buf:expr, $pos:expr) => {{
            let crlf = crlf_or_null!($buf, $pos);
            let line = &$buf[*$pos..crlf];
            let count = atoi_or_null!(line);
            *$pos = crlf + 2;
            if count == -1 {
                return Frame::Null;
            }
            if count < 0 {
                return Frame::Null;
            }
            (count as usize).min(config.max_array_length)
        }};
    }

    match type_byte {
        b'+' => {
            let crlf = crlf_or_null!(buf, pos);
            let line = buf.slice(*pos..crlf);
            *pos = crlf + 2;
            Frame::SimpleString(line)
        }
        b'-' => {
            let crlf = crlf_or_null!(buf, pos);
            let line = buf.slice(*pos..crlf);
            *pos = crlf + 2;
            Frame::Error(line)
        }
        b':' => {
            let crlf = crlf_or_null!(buf, pos);
            let line = &buf[*pos..crlf];
            let n = atoi_or_null!(line);
            *pos = crlf + 2;
            Frame::Integer(n)
        }
        b'$' => {
            let crlf = crlf_or_null!(buf, pos);
            let line = &buf[*pos..crlf];
            let len_val = atoi_or_null!(line);
            *pos = crlf + 2;
            if len_val == -1 {
                return Frame::Null;
            }
            if len_val < 0 {
                return Frame::Null;
            }
            let len = len_val as usize;
            if *pos + len + 2 > buf.len() {
                return Frame::Null;
            }
            let data = buf.slice(*pos..*pos + len);
            *pos += len + 2;
            Frame::BulkString(data)
        }
        b'*' => {
            // `*-1` is the RESP2 Null Array — a well-formed frame, not a
            // failure. It is handled HERE rather than in `parse_count!`
            // because that macro is shared with `%`, `~` and `>`, where a
            // `-1` count has no such meaning and must stay a parse failure
            // (moon#482).
            //
            // Any other negative count, and any malformed length, still falls
            // through to `parse_count!` and yields `Frame::Null` — the
            // failure sentinel is unchanged.
            let crlf = crlf_or_null!(buf, pos);
            if &buf[*pos..crlf] == b"-1" {
                *pos = crlf + 2;
                return Frame::NullArray;
            }
            let count = parse_count!(buf, pos);
            let mut items = FrameVec::with_capacity(count);
            for _ in 0..count {
                items.push(parse_frame_zerocopy(buf, pos, config, depth + 1));
            }
            Frame::Array(items)
        }
        b'%' => {
            let count = parse_count!(buf, pos);
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let key = parse_frame_zerocopy(buf, pos, config, depth + 1);
                let val = parse_frame_zerocopy(buf, pos, config, depth + 1);
                entries.push((key, val));
            }
            Frame::Map(entries)
        }
        b'~' => {
            let count = parse_count!(buf, pos);
            let mut items = FrameVec::with_capacity(count);
            for _ in 0..count {
                items.push(parse_frame_zerocopy(buf, pos, config, depth + 1));
            }
            Frame::Set(items)
        }
        b',' => {
            let crlf = crlf_or_null!(buf, pos);
            let line = &buf[*pos..crlf];
            let f = match std::str::from_utf8(line) {
                Ok("inf") => f64::INFINITY,
                Ok("-inf") => f64::NEG_INFINITY,
                Ok(s) => s.parse::<f64>().unwrap_or(0.0),
                Err(_) => 0.0,
            };
            *pos = crlf + 2;
            Frame::Double(f)
        }
        b'#' => {
            let crlf = crlf_or_null!(buf, pos);
            // Defensive: exactly one byte (t or f) before CRLF
            if crlf != *pos + 1 {
                return Frame::Null;
            }
            let val = buf[*pos];
            *pos = crlf + 2;
            Frame::Boolean(val == b't')
        }
        b'_' => {
            let crlf = crlf_or_null!(buf, pos);
            // Defensive: CRLF must be immediately at *pos (no junk)
            if crlf != *pos {
                return Frame::Null;
            }
            *pos = crlf + 2;
            Frame::Null
        }
        b'=' => {
            let crlf = crlf_or_null!(buf, pos);
            let line = &buf[*pos..crlf];
            let len = match strict_atoi(line) {
                Some(n) if n >= 4 => n as usize,
                _ => return Frame::Null,
            };
            *pos = crlf + 2;
            if *pos + len + 2 > buf.len() || buf[*pos + 3] != b':' {
                return Frame::Null;
            }
            let payload = &buf[*pos..*pos + len];
            let encoding = [payload[0], payload[1], payload[2]];
            let data = buf.slice(*pos + 4..*pos + len);
            *pos += len + 2;
            Frame::VerbatimString { encoding, data }
        }
        b'(' => {
            let crlf = crlf_or_null!(buf, pos);
            let line = buf.slice(*pos..crlf);
            *pos = crlf + 2;
            Frame::BigNumber(line)
        }
        b'>' => {
            let count = parse_count!(buf, pos);
            let mut items = FrameVec::with_capacity(count);
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
pub(super) fn find_crlf(buf: &[u8], start: usize) -> Option<usize> {
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

/// Strict decimal parse: all bytes in the slice must be consumed by the integer.
/// Rejects inputs like `b"5\n"` where `atoi::atoi` would silently ignore trailing bytes.
#[inline]
pub(super) fn strict_atoi(line: &[u8]) -> Option<i64> {
    let (val, used) = i64::from_radix_10_signed_checked(line);
    match val {
        Some(n) if used == line.len() => Some(n),
        _ => None,
    }
}

/// Read a CRLF-terminated decimal integer from buf at position pos.
/// Advances pos past the CRLF.
///
/// `kind` comes from the caller because this helper serves BOTH bulk headers
/// (`$`, `=`) and collection counts (`*`/`%`/`~`/`>`), and Redis names those
/// two faults differently. Hardcoding one here made `$abc` report
/// "invalid multibulk length" — right machinery, wrong noun.
#[inline]
fn read_decimal(buf: &[u8], pos: &mut usize, kind: ProtoFault) -> Result<i64, ParseError> {
    let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
    let line = &buf[*pos..crlf];
    let n = strict_atoi(line).ok_or_else(|| ParseError::Invalid {
        kind,
        message: format!("invalid decimal: {:?}", String::from_utf8_lossy(line)),
        offset: *pos,
    })?;
    *pos = crlf + 2;
    Ok(n)
}

/// Lightweight validation pass: walks the buffer to compute total frame byte length
/// without allocating any Frame objects. Returns Ok(()) on success with `pos` advanced
/// past the complete frame, or Err on incomplete/invalid data.
fn validate_frame(
    buf: &[u8],
    pos: &mut usize,
    config: &ParseConfig,
    depth: usize,
) -> Result<(), ParseError> {
    if depth > config.max_array_depth {
        return Err(ParseError::Invalid {
            kind: ProtoFault::MultibulkLen,
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
        b'+' | b'-' | b'(' => {
            // SimpleString, Error, BigNumber: skip to CRLF
            let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
            *pos = crlf + 2;
            Ok(())
        }
        b':' => {
            // Integer: validate parseable (strict — all bytes must be digits)
            let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
            let line = &buf[*pos..crlf];
            strict_atoi(line).ok_or_else(|| ParseError::Invalid {
                kind: ProtoFault::ExpectedDollar(type_byte),
                message: format!("invalid integer: {:?}", String::from_utf8_lossy(line)),
                offset: *pos,
            })?;
            *pos = crlf + 2;
            Ok(())
        }
        b',' => {
            // Double: validate parseable
            let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
            let line = &buf[*pos..crlf];
            let s = std::str::from_utf8(line).map_err(|_| ParseError::Invalid {
                kind: ProtoFault::ExpectedDollar(type_byte),
                message: "invalid UTF-8 in double".into(),
                offset: *pos,
            })?;
            if !matches!(s, "inf" | "-inf" | "nan") {
                s.parse::<f64>().map_err(|_| ParseError::Invalid {
                    kind: ProtoFault::ExpectedDollar(type_byte),
                    message: format!("invalid double: {:?}", s),
                    offset: *pos,
                })?;
            }
            *pos = crlf + 2;
            Ok(())
        }
        b'#' => {
            // Boolean: must be exactly t or f followed by CRLF
            let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
            let line = &buf[*pos..crlf];
            match line {
                b"t" | b"f" => {}
                _ => {
                    return Err(ParseError::Invalid {
                        kind: ProtoFault::ExpectedDollar(type_byte),
                        message: format!(
                            "invalid boolean value: {:?}",
                            String::from_utf8_lossy(line)
                        ),
                        offset: *pos,
                    });
                }
            }
            *pos = crlf + 2;
            Ok(())
        }
        b'_' => {
            // Null: CRLF must be immediately at *pos (no intervening bytes)
            let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
            if crlf != *pos {
                return Err(ParseError::Invalid {
                    kind: ProtoFault::ExpectedDollar(type_byte),
                    message: format!(
                        "RESP3 null has trailing data before CRLF at offset {}",
                        *pos
                    ),
                    offset: *pos,
                });
            }
            *pos = crlf + 2;
            Ok(())
        }
        b'$' => {
            let len = read_decimal(buf, pos, ProtoFault::BulkLen)?;
            if len == -1 {
                return Ok(());
            } // Null bulk string
            if len < 0 {
                return Err(ParseError::Invalid {
                    kind: ProtoFault::BulkLen,
                    message: format!("invalid bulk string length: {}", len),
                    offset: *pos,
                });
            }
            let len = len as usize;
            if len > config.max_bulk_string_size {
                return Err(ParseError::Invalid {
                    kind: ProtoFault::BulkLen,
                    message: format!(
                        "bulk string size {} exceeds maximum {}",
                        len, config.max_bulk_string_size
                    ),
                    offset: *pos,
                });
            }
            let remaining = buf.len() - *pos;
            if remaining < len + 2 {
                return Err(ParseError::Incomplete);
            }
            *pos += len + 2; // skip data + \r\n
            Ok(())
        }
        b'=' => {
            // VerbatimString: length-prefixed like bulk string
            let len = read_decimal(buf, pos, ProtoFault::BulkLen)?;
            if len < 4 {
                return Err(ParseError::Invalid {
                    kind: ProtoFault::ExpectedDollar(type_byte),
                    message: format!("verbatim string length {} too short", len),
                    offset: *pos,
                });
            }
            let len = len as usize;
            if len > config.max_bulk_string_size {
                return Err(ParseError::Invalid {
                    kind: ProtoFault::ExpectedDollar(type_byte),
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
            if buf[*pos + 3] != b':' {
                return Err(ParseError::Invalid {
                    kind: ProtoFault::ExpectedDollar(type_byte),
                    message: "verbatim string missing ':' after 3-byte encoding".into(),
                    offset: *pos + 3,
                });
            }
            *pos += len + 2;
            Ok(())
        }
        b'*' | b'~' | b'>' => {
            // Array, Set, Push: count + elements
            let count = read_decimal(buf, pos, ProtoFault::MultibulkLen)?;
            if count == -1 {
                return Ok(());
            } // Null array
            if count < 0 {
                // Below -1 is lenient for `*` ONLY. Measured against
                // redis-server 8.6.1: `*-9\r\n` is consumed silently and the
                // connection keeps serving, where Moon used to kill it.
                //
                // Scoped to `*` deliberately: RESP3 Set (`~`) and Push (`>`)
                // have no such Redis behaviour to match, and blanket leniency
                // silently stopped rejecting `~-2` — caught by
                // `test_resp3_negative_set_count`, which is why that test
                // exists.
                if type_byte == b'*' {
                    return Ok(());
                }
                return Err(ParseError::Invalid {
                    kind: ProtoFault::MultibulkLen,
                    message: format!("invalid array/set/push length: {}", count),
                    offset: *pos,
                });
            }
            let count = count as usize;
            if count > config.max_array_length {
                return Err(ParseError::Invalid {
                    kind: ProtoFault::MultibulkLen,
                    message: format!(
                        "length {} exceeds maximum {}",
                        count, config.max_array_length
                    ),
                    offset: *pos,
                });
            }
            for _ in 0..count {
                validate_frame(buf, pos, config, depth + 1)?;
            }
            Ok(())
        }
        b'%' => {
            // Map: count pairs
            let count = read_decimal(buf, pos, ProtoFault::MultibulkLen)?;
            if count == -1 {
                return Ok(()); // Null map
            }
            if count < 0 {
                return Err(ParseError::Invalid {
                    kind: ProtoFault::MultibulkLen,
                    message: format!("invalid map length: {}", count),
                    offset: *pos,
                });
            }
            let count = count as usize;
            if count > config.max_array_length {
                return Err(ParseError::Invalid {
                    kind: ProtoFault::MultibulkLen,
                    message: format!(
                        "map length {} exceeds maximum {}",
                        count, config.max_array_length
                    ),
                    offset: *pos,
                });
            }
            for _ in 0..count {
                validate_frame(buf, pos, config, depth + 1)?;
                validate_frame(buf, pos, config, depth + 1)?;
            }
            Ok(())
        }
        byte => Err(ParseError::Invalid {
            kind: ProtoFault::UnknownType(byte),
            message: format!("unknown type byte: 0x{:02x}", byte),
            offset: *pos - 1,
        }),
    }
}

// Tests live in the `parse/tests.rs` submodule so parse.rs stays under the 1500-line cap
// (moon#1226).
#[cfg(test)]
mod tests;
