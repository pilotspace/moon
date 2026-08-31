#![allow(unused_imports, dead_code)]
use atoi::FromRadix10SignedChecked;
use memchr::memchr;

use bytes::{Buf, Bytes, BytesMut};
use smallvec::SmallVec;

use super::frame::{Frame, FrameVec, ParseConfig, ParseError, ProtoFault};
use super::inline;

/// Attempt to parse one RESP2/RESP3 frame from the buffer.
///
/// A flat top-level `*N` of `$`-bulks -- the shape of essentially every client
/// command -- is scanned in **one** pass: [`scan_flat_multibulk`] records each
/// argument's span while it validates, and the `Frame` is built straight from
/// those spans.
///
/// Everything else keeps the original two-pass approach:
/// 1. Validate structure and compute byte length (`validate_frame`, offsets discarded)
/// 2. Freeze validated bytes and extract frame data via `Bytes::slice`
///    (Arc refcount bump, no memcpy)
///
/// The fast path declines -- falling through to the two-pass path -- on *anything*
/// it does not handle exactly: incomplete input, malformed lengths, negative or
/// out-of-range counts, null bulks, nested or non-bulk elements, RESP3
/// containers, inline commands. Declining is always safe; answering differently
/// never is, which is what `single_pass_multibulk_agrees_with_two_pass` and the
/// `resp_parse_fused` fuzz target assert over arbitrary input.
///
/// On success, advances the buffer past the consumed bytes and returns `Ok(Some(frame))`.
/// Returns `Ok(None)` if the buffer doesn't contain a complete frame (need more data).
/// Returns `Err` if the data violates the RESP2 protocol specification.
pub fn parse(buf: &mut BytesMut, config: &ParseConfig) -> Result<Option<Frame>, ParseError> {
    match dispatch_prefix(buf, config)? {
        PrefixOutcome::Done(frame) => return Ok(frame),
        PrefixOutcome::Resp => {}
    }
    if let Some(frame) = parse_flat_multibulk(buf, config) {
        return Ok(Some(frame));
    }
    parse_resp_two_pass(buf, config)
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

/// A completed single-pass scan of a flat top-level multibulk.
struct FlatScan {
    /// Total byte length of the frame, i.e. what `validate_frame` would have
    /// left in `pos`.
    total_len: usize,
    /// `(offset, len)` of each argument's payload, relative to the frame start.
    ///
    /// Sixteen inline covers every command moon has; past that the spill costs
    /// one allocation, which is still one fewer walk than the two-pass path.
    spans: SmallVec<[(u32, u32); 16]>,
}

/// How many argument spans to reserve for a claimed element count.
///
/// The count comes off the wire, so reserving `count` outright hands a client a
/// memory amplifier: `*1048576\r\n` is ten bytes and `max_array_length`
/// defaults to 1Mi, which would reserve 8 MiB before the scan discovered the
/// frame was incomplete. The two-pass path never had this problem -- it reaches
/// `FrameVec::with_capacity` only after `validate_frame` proved the whole frame
/// is present, so the buffer bounds the count for free.
///
/// The shortest an element can be is six bytes (`$0\r\n` plus the two trailing
/// bytes every bulk is charged), so `buf.len() / 6` is a hard ceiling on how
/// many can possibly be there. Capping at it never under-allocates for a scan
/// that goes on to succeed, and `SmallVec` grows anyway if it somehow did.
#[inline]
fn span_capacity(count: usize, buf_len: usize) -> usize {
    count.min(buf_len / 6)
}

/// Scan a flat top-level `*N` of `$`-bulks in ONE pass, recording each
/// argument's span.
///
/// Returns `None` for **anything** that is not exactly that shape, complete and
/// within `config`'s limits — incomplete input, a malformed count or length, a
/// negative count (`*-1`, `*-9`), a null bulk (`$-1`), a nested or non-bulk
/// element, an over-limit count or payload. The caller then runs the two-pass
/// path, which owns every one of those cases and their exact error kinds.
///
/// Declining more often than strictly necessary is always safe; answering where
/// the two-pass path would answer differently never is. Every rule below is a
/// deliberate mirror of `validate_frame`'s `b'*'` and `b'$'` arms, including one
/// piece of leniency that looks like a bug and is not: **the two bytes after a
/// bulk payload are never checked**. `validate_frame` does `*pos += len + 2`
/// without verifying they are CRLF, and `parse_frame_zerocopy` skips them the
/// same way, so `*1\r\n$1\r\naXY` parses. Verifying them here would make the
/// fast path stricter than the path it replaces.
fn scan_flat_multibulk(buf: &[u8], config: &ParseConfig) -> Option<FlatScan> {
    // Spans are recorded as u32. A buffer that large is not a client command.
    if buf.len() > u32::MAX as usize {
        return None;
    }
    if buf.first() != Some(&b'*') {
        return None;
    }

    // Decline on the BYTE, not on the parsed count. `parse_resp_two_pass`'s
    // `is_null_multibulk` gate keys on `buf[1] == b'-'`, and `strict_atoi` reads
    // a lone `-` (and `-0`) as ZERO -- so `*-\r\n` has a non-negative count and
    // is still silently consumed there while reporting no frame at all. Testing
    // `count < 0` alone let the fast path answer `*0`-shaped where the two-pass
    // path answers nothing. Found by the `resp_parse_fused` differential fuzz
    // target within 90 seconds of its first run.
    if buf.get(1) == Some(&b'-') {
        return None;
    }

    let mut pos = 1usize;
    let crlf = find_crlf(buf, pos)?;
    let count = strict_atoi(&buf[pos..crlf])?;
    pos = crlf + 2;

    // `*-1` is the null array and anything below it is Redis's silently-consumed
    // case; both live in `parse_resp_two_pass`, which spells them differently.
    // Unreachable after the byte test above, and kept as the belt to its braces.
    if count < 0 {
        return None;
    }
    let count = count as usize;
    if count > config.max_array_length {
        return None;
    }
    // `validate_frame` validates the ELEMENTS at depth 1, so a non-empty array
    // is a depth error when the limit is 0. Let the two-pass path raise it.
    if count > 0 && config.max_array_depth < 1 {
        return None;
    }

    let mut spans: SmallVec<[(u32, u32); 16]> =
        SmallVec::with_capacity(span_capacity(count, buf.len()));
    for _ in 0..count {
        if buf.get(pos) != Some(&b'$') {
            return None;
        }
        pos += 1;
        let crlf = find_crlf(buf, pos)?;
        let len = strict_atoi(&buf[pos..crlf])?;
        pos = crlf + 2;
        // `$-1` (null bulk) yields a `Frame::Null` element on the two-pass path.
        if len < 0 {
            return None;
        }
        let len = len as usize;
        if len > config.max_bulk_string_size {
            return None;
        }
        if buf.len() - pos < len + 2 {
            return None; // incomplete
        }
        spans.push((pos as u32, len as u32));
        pos += len + 2;
    }

    Some(FlatScan {
        total_len: pos,
        spans,
    })
}

/// Consume and build a flat multibulk from a single scan, or leave `buf`
/// untouched and return `None` so the caller falls through to the two-pass path.
fn parse_flat_multibulk(buf: &mut BytesMut, config: &ParseConfig) -> Option<Frame> {
    let scan = scan_flat_multibulk(&buf[..], config)?;
    // Same freeze the two-pass path performs: one `Shared` promotion for the
    // whole frame, amortised across the batch.
    let frozen = buf.split_to(scan.total_len).freeze();
    let mut items = FrameVec::with_capacity(scan.spans.len());
    for (start, len) in scan.spans {
        let start = start as usize;
        items.push(Frame::BulkString(frozen.slice(start..start + len as usize)));
    }
    Some(Frame::Array(items))
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
            let encoding = Bytes::copy_from_slice(&payload[..3]);
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

/// Strict decimal parse: all bytes in the slice must be consumed by the integer.
/// Rejects inputs like `b"5\n"` where `atoi::atoi` would silently ignore trailing bytes.
#[inline]
fn strict_atoi(line: &[u8]) -> Option<i64> {
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
            let n = strict_atoi(line).ok_or_else(|| ParseError::Invalid {
                kind: ProtoFault::ExpectedDollar(type_byte),
                message: format!("invalid integer: {:?}", String::from_utf8_lossy(line)),
                offset: *pos,
            })?;
            *pos = crlf + 2;
            Ok(Frame::Integer(n))
        }
        b'$' => {
            let len = read_decimal(buf, pos, ProtoFault::BulkLen)?;
            if len == -1 {
                return Ok(Frame::Null);
            }
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
            let count = read_decimal(buf, pos, ProtoFault::MultibulkLen)?;
            if count == -1 {
                return Ok(Frame::Null);
            }
            if count < 0 {
                return Err(ParseError::Invalid {
                    kind: ProtoFault::MultibulkLen,
                    message: format!("invalid array length: {}", count),
                    offset: *pos,
                });
            }
            let count = count as usize;
            if count > config.max_array_length {
                return Err(ParseError::Invalid {
                    kind: ProtoFault::MultibulkLen,
                    message: format!(
                        "array length {} exceeds maximum {}",
                        count, config.max_array_length
                    ),
                    offset: *pos,
                });
            }
            let mut items = FrameVec::with_capacity(count);
            for _ in 0..count {
                items.push(parse_single_frame(buf, pos, config, depth + 1)?);
            }
            Ok(Frame::Array(items))
        }
        // === RESP3 types ===
        b'_' => {
            // RESP3 Null: `_\r\n` — CRLF must be immediately at *pos
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
                        kind: ProtoFault::ExpectedDollar(type_byte),
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
                kind: ProtoFault::ExpectedDollar(type_byte),
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
                    kind: ProtoFault::ExpectedDollar(type_byte),
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
            let len = read_decimal(buf, pos, ProtoFault::BulkLen)?;
            if len < 4 {
                return Err(ParseError::Invalid {
                    kind: ProtoFault::ExpectedDollar(type_byte),
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
            let payload = &buf[*pos..*pos + len];
            if payload[3] != b':' {
                return Err(ParseError::Invalid {
                    kind: ProtoFault::ExpectedDollar(type_byte),
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
            let count = read_decimal(buf, pos, ProtoFault::MultibulkLen)?;
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
            let count = read_decimal(buf, pos, ProtoFault::MultibulkLen)?;
            if count < 0 {
                return Err(ParseError::Invalid {
                    kind: ProtoFault::MultibulkLen,
                    message: format!("invalid set length: {}", count),
                    offset: *pos,
                });
            }
            let count = count as usize;
            if count > config.max_array_length {
                return Err(ParseError::Invalid {
                    kind: ProtoFault::MultibulkLen,
                    message: format!(
                        "set length {} exceeds maximum {}",
                        count, config.max_array_length
                    ),
                    offset: *pos,
                });
            }
            let mut items = FrameVec::with_capacity(count);
            for _ in 0..count {
                items.push(parse_single_frame(buf, pos, config, depth + 1)?);
            }
            Ok(Frame::Set(items))
        }
        b'>' => {
            // RESP3 Push: `><count>\r\n<elements...>`
            let count = read_decimal(buf, pos, ProtoFault::MultibulkLen)?;
            if count < 0 {
                return Err(ParseError::Invalid {
                    kind: ProtoFault::MultibulkLen,
                    message: format!("invalid push length: {}", count),
                    offset: *pos,
                });
            }
            let count = count as usize;
            if count > config.max_array_length {
                return Err(ParseError::Invalid {
                    kind: ProtoFault::MultibulkLen,
                    message: format!(
                        "push length {} exceeds maximum {}",
                        count, config.max_array_length
                    ),
                    offset: *pos,
                });
            }
            let mut items = FrameVec::with_capacity(count);
            for _ in 0..count {
                items.push(parse_single_frame(buf, pos, config, depth + 1)?);
            }
            Ok(Frame::Push(items))
        }
        byte => Err(ParseError::Invalid {
            kind: ProtoFault::UnknownType(byte),
            message: format!("unknown type byte: 0x{:02x}", byte),
            offset: *pos - 1,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framevec;

    fn parse_bytes(input: &[u8]) -> Result<Option<Frame>, ParseError> {
        let mut buf = BytesMut::from(input);
        parse(&mut buf, &ParseConfig::default())
    }

    fn parse_bytes_with_buf(input: &[u8]) -> (Result<Option<Frame>, ParseError>, BytesMut) {
        let mut buf = BytesMut::from(input);
        let result = parse(&mut buf, &ParseConfig::default());
        (result, buf)
    }

    // === Single-pass multibulk fast path: differential against the two-pass path ===

    /// Every input, byte-identical outcome.
    ///
    /// `parse` now scans a flat top-level `*N` of `$`-bulks ONCE, recording the
    /// argument spans as it validates, instead of walking the bytes twice
    /// (`validate_frame` computing every offset and throwing them away, then
    /// `parse_frame_zerocopy` re-deriving them). Everything else — RESP3
    /// containers, nested arrays, null bulks, inline commands, reply parsing —
    /// still takes the two-pass path unchanged.
    ///
    /// The contract is total: for ANY input the fast path must produce exactly
    /// what the two-pass path produces — same `Ok`/`Err`, same frame, same
    /// number of bytes consumed, same bytes left in the buffer.
    fn assert_parse_agrees(input: &[u8], config: &ParseConfig) {
        let mut fast_buf = BytesMut::from(input);
        let mut ref_buf = BytesMut::from(input);
        let shown = String::from_utf8_lossy(input);

        // Drain the whole pipeline, not just the first frame: a divergence that
        // only shows on the second frame is exactly what a single-frame
        // comparison misses.
        for step in 0..16 {
            if fast_buf.is_empty() && ref_buf.is_empty() {
                break;
            }
            let fast = parse(&mut fast_buf, config);
            let slow = parse_reference_two_pass(&mut ref_buf, config);
            match (&fast, &slow) {
                (Ok(a), Ok(b)) => assert_eq!(a, b, "frame diverged on {shown:?} at step {step}"),
                // `Display` carries the wire fault name, the internal message and
                // the byte offset -- the strongest comparison available without
                // `PartialEq` on `ParseError`.
                (Err(a), Err(b)) => assert_eq!(
                    a.to_string(),
                    b.to_string(),
                    "error diverged on {shown:?} at step {step}"
                ),
                _ => panic!(
                    "ok/err diverged on {shown:?} at step {step}: fast={:?} slow={:?}",
                    fast.is_ok(),
                    slow.is_ok()
                ),
            }
            assert_eq!(
                fast_buf.as_ref(),
                ref_buf.as_ref(),
                "buffer advancement diverged on {shown:?} at step {step}"
            );
            if !matches!(fast, Ok(Some(_))) {
                break;
            }
        }
    }

    /// The corpus deliberately covers what the fast path must DECLINE as well as
    /// what it must accept: a fast path that silently answered for `$-1` or a
    /// nested array would be wrong, and a fast path that declined everything
    /// would be useless — `flat_multibulk_fast_path_actually_fires` pins that.
    fn differential_corpus() -> Vec<Vec<u8>> {
        let mut v: Vec<Vec<u8>> = vec![
            // Ordinary commands, argc 0..8 — the shape the fast path exists for.
            b"*0\r\n".to_vec(),
            b"*1\r\n$4\r\nPING\r\n".to_vec(),
            b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n".to_vec(),
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n".to_vec(),
            b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$3\r\n100\r\n".to_vec(),
            b"*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\nf\r\n$1\r\nv\r\n".to_vec(),
            // Empty bulk, and a bulk holding CRLF and NULs.
            b"*2\r\n$3\r\nGET\r\n$0\r\n\r\n".to_vec(),
            b"*1\r\n$4\r\na\r\nb\r\n".to_vec(),
            b"*1\r\n$3\r\na\0b\r\n".to_vec(),
            // Null array, null bulk element, negative-but-not--1 count.
            b"*-1\r\n".to_vec(),
            b"*-9\r\n".to_vec(),
            b"*-1x\r\n".to_vec(),
            // Found by the `resp_parse_fused` fuzz target. `strict_atoi` reads a
            // LONE minus as 0, so `*-\r\n` scans as an empty array -- but
            // `parse_resp_two_pass`'s `is_null_multibulk` gate keys on
            // `buf[1] == b'-'`, not on the parsed count, and silently CONSUMES
            // it while reporting no frame. The fast path must decline on the
            // byte, not on the number.
            b"*-\r\n".to_vec(),
            b"*-\r\n*0\r\n".to_vec(),
            b"*-0\r\n".to_vec(),
            b"*-0\r\n*1\r\n$1\r\na\r\n".to_vec(),
            b"*2\r\n$-1\r\n$1\r\nk\r\n".to_vec(),
            b"*1\r\n$-1\r\n".to_vec(),
            b"*1\r\n$-2\r\n".to_vec(),
            // Nested and non-bulk elements: must fall through, not be answered.
            b"*2\r\n*1\r\n$1\r\na\r\n$1\r\nb\r\n".to_vec(),
            b"*2\r\n+OK\r\n$1\r\nb\r\n".to_vec(),
            b"*2\r\n:1\r\n$1\r\nb\r\n".to_vec(),
            b"*1\r\n_\r\n".to_vec(),
            b"*1\r\n#t\r\n".to_vec(),
            // Malformed headers and lengths.
            b"*\r\n".to_vec(),
            b"*abc\r\n".to_vec(),
            b"* 2\r\n$1\r\na\r\n".to_vec(),
            b"*+2\r\n$1\r\na\r\n$1\r\nb\r\n".to_vec(),
            b"*2\r\n$abc\r\n".to_vec(),
            b"*2\r\n$ 1\r\na\r\n".to_vec(),
            b"*1\r\n$99999999999999999999\r\n".to_vec(),
            b"*99999999999999999999\r\n".to_vec(),
            // Bare \r and \n, and a missing terminator after the payload —
            // `validate_frame` does NOT verify the two bytes after a bulk body,
            // so the fast path must not verify them either.
            b"*1\r\n$1\r\naXY".to_vec(),
            b"*1\r\n$1\r\na\r\n".to_vec(),
            b"*1\r$1\r\na\r\n".to_vec(),
            b"*1\n$1\n a\n".to_vec(),
            b"*1\r\n$5\ra\r\nbc\r\n".to_vec(),
            // Trailing pipeline bytes must survive untouched.
            b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n".to_vec(),
            b"*1\r\n$4\r\nPING\r\nGARBAGE".to_vec(),
            // Non-array RESP and RESP3 containers keep the two-pass path.
            b"+OK\r\n".to_vec(),
            b"-ERR bad\r\n".to_vec(),
            b":42\r\n".to_vec(),
            b"$3\r\nabc\r\n".to_vec(),
            b"$-1\r\n".to_vec(),
            b"%1\r\n$1\r\na\r\n$1\r\nb\r\n".to_vec(),
            b"~2\r\n$1\r\na\r\n$1\r\nb\r\n".to_vec(),
            b">2\r\n$1\r\na\r\n$1\r\nb\r\n".to_vec(),
            b",1.5\r\n".to_vec(),
            b"(12345\r\n".to_vec(),
            b"=8\r\ntxt:abcd\r\n".to_vec(),
            // Inline commands and blank lines share the pre-RESP funnel.
            b"PING\r\n".to_vec(),
            b"\r\n\r\nECHO hi\r\n".to_vec(),
            b"".to_vec(),
            b"\r\n".to_vec(),
            // Unknown type byte.
            b"@1\r\n".to_vec(),
        ];

        // Every truncation of every case above: the fast path must agree on
        // "need more bytes" at every prefix length, not just on whole frames.
        let whole = v.clone();
        for case in &whole {
            for cut in 0..case.len() {
                v.push(case[..cut].to_vec());
            }
        }
        v
    }

    #[test]
    fn single_pass_multibulk_agrees_with_two_pass() {
        let config = ParseConfig::default();
        for case in differential_corpus() {
            assert_parse_agrees(&case, &config);
        }
    }

    /// The limits are part of the contract: a fast path that applied its own
    /// bounds, or none, would diverge exactly where a hostile client aims.
    #[test]
    fn single_pass_multibulk_agrees_under_tight_limits() {
        let configs = [
            ParseConfig {
                max_bulk_string_size: 2,
                max_array_depth: 4,
                max_array_length: 256,
                max_inline_size: 64,
            },
            ParseConfig {
                max_bulk_string_size: 1024,
                max_array_depth: 4,
                max_array_length: 2,
                max_inline_size: 64,
            },
            // Depth 0 forbids the ELEMENTS of a top-level array, so a `*1` must
            // be rejected while `*0` is still fine.
            ParseConfig {
                max_bulk_string_size: 1024,
                max_array_depth: 0,
                max_array_length: 256,
                max_inline_size: 64,
            },
            ParseConfig {
                max_bulk_string_size: 0,
                max_array_depth: 0,
                max_array_length: 0,
                max_inline_size: 0,
            },
        ];
        for config in &configs {
            for case in differential_corpus() {
                assert_parse_agrees(&case, config);
            }
        }
    }

    /// A structured sweep over argc and payload length, including the `argc > 4`
    /// boundary where `FrameVec`'s inline capacity spills and the 16-span
    /// boundary where the scanner's own `SmallVec` spills.
    #[test]
    fn single_pass_multibulk_agrees_across_argc_and_length() {
        let config = ParseConfig::default();
        for argc in 0..=20usize {
            for arglen in [0usize, 1, 3, 12, 13, 64, 300] {
                let mut frame = format!("*{argc}\r\n").into_bytes();
                for i in 0..argc {
                    let payload = vec![b'a' + (i % 26) as u8; arglen];
                    frame.extend_from_slice(format!("${arglen}\r\n").as_bytes());
                    frame.extend_from_slice(&payload);
                    frame.extend_from_slice(b"\r\n");
                }
                assert_parse_agrees(&frame, &config);
                // and one byte short of complete
                if !frame.is_empty() {
                    assert_parse_agrees(&frame[..frame.len() - 1], &config);
                }
            }
        }
    }

    /// A differential that both sides decline is worth nothing. This pins that
    /// the fast path really answers the common command shapes, so the test above
    /// is testing the new code and not the old code twice.
    #[test]
    fn flat_multibulk_fast_path_actually_fires() {
        let config = ParseConfig::default();
        let fires = [
            &b"*0\r\n"[..],
            &b"*1\r\n$4\r\nPING\r\n"[..],
            &b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"[..],
            &b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"[..],
            &b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$3\r\n100\r\n"[..],
            &b"*2\r\n$3\r\nGET\r\n$0\r\n\r\n"[..],
        ];
        for case in fires {
            let mut buf = BytesMut::from(case);
            assert!(
                scan_flat_multibulk(&buf[..], &config).is_some(),
                "fast path declined {:?} — the differential would be vacuous",
                String::from_utf8_lossy(case)
            );
            // and it produces the frame, not just a scan
            assert!(matches!(
                parse_flat_multibulk(&mut buf, &config),
                Some(Frame::Array(_))
            ));
        }

        let declines = [
            &b"*-1\r\n"[..],
            &b"*2\r\n$-1\r\n$1\r\nk\r\n"[..],
            &b"*2\r\n*1\r\n$1\r\na\r\n$1\r\nb\r\n"[..],
            &b"*2\r\n+OK\r\n$1\r\nb\r\n"[..],
            &b"*2\r\n$3\r\nGET\r\n"[..], // incomplete
            &b"*abc\r\n"[..],
        ];
        for case in declines {
            assert!(
                scan_flat_multibulk(case, &config).is_none(),
                "fast path accepted {:?}, which the two-pass path treats specially",
                String::from_utf8_lossy(case)
            );
        }
    }

    /// The scanned spans must be the spans the zero-copy pass would have
    /// derived — pointing INTO the frozen buffer, not copies.
    #[test]
    fn fast_path_slices_alias_the_frozen_buffer() {
        let config = ParseConfig::default();
        let mut buf = BytesMut::from(&b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n"[..]);
        let frame = parse(&mut buf, &config).unwrap().unwrap();
        assert!(buf.is_empty(), "the whole frame must be consumed");
        match frame {
            Frame::Array(items) => {
                let got: Vec<&[u8]> = items
                    .iter()
                    .map(|f| match f {
                        Frame::BulkString(b) => b.as_ref(),
                        other => panic!("expected BulkString, got {other:?}"),
                    })
                    .collect();
                assert_eq!(got, vec![&b"SET"[..], &b"hello"[..], &b"world"[..]]);
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    /// `*1048576\r\n` is ten bytes and `max_array_length` defaults to 1Mi, so a
    /// naive `SmallVec::with_capacity(count)` would allocate 8 MiB before the scanner
    /// discovered the frame was incomplete. The two-pass path never had that
    /// amplification: `parse_frame_zerocopy` only reaches `FrameVec::with_capacity`
    /// AFTER `validate_frame` proved the whole frame is present, so the buffer itself
    /// bounds the count. The scanner allocates first, so it must bound the count itself.
    ///
    /// Six bytes is the shortest an element can be (`$0\r\n` + the two trailing bytes),
    /// so a buffer can never hold more than `len / 6` of them and capping there can
    /// never under-allocate for a scan that goes on to succeed.
    #[test]
    fn span_capacity_is_bounded_by_the_buffer_not_the_claimed_count() {
        // The attack: a huge claimed count in a tiny buffer.
        assert_eq!(span_capacity(1024 * 1024, b"*1048576\r\n".len()), 1);
        assert_eq!(span_capacity(usize::MAX, 0), 0);
        assert_eq!(span_capacity(1_000_000, 60), 10);

        // Honest commands are unaffected: capacity still covers every argument.
        for case in [
            &b"*1\r\n$4\r\nPING\r\n"[..],
            &b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"[..],
            &b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"[..],
            &b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$3\r\n100\r\n"[..],
        ] {
            let scan = scan_flat_multibulk(case, &ParseConfig::default())
                .expect("honest command must still take the fast path");
            assert!(
                span_capacity(scan.spans.len(), case.len()) >= scan.spans.len(),
                "capped capacity under-allocated for {:?}",
                String::from_utf8_lossy(case)
            );
        }

        // And end to end: the pathological header must be declined, not answered.
        assert!(scan_flat_multibulk(b"*1048576\r\n", &ParseConfig::default()).is_none());
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
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"\r\n\r\n")));
    }

    // === Null Array tests ===

    #[test]
    fn test_parse_null_array() {
        // CHANGED by moon#482. This test previously asserted `Frame::Null`,
        // i.e. it pinned the defect: `*-1` collapsed into the null-BULK
        // variant, so a reply parsed from a peer re-serialised as `$-1`. The
        // frozen contract requires the two nulls to stay distinct, so the
        // expectation moves with it.
        let result = parse_bytes(b"*-1\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::NullArray);
        assert_ne!(result, Frame::Null);
    }

    #[test]
    fn test_null_array_round_trips_through_parse_and_serialize() {
        // Moon parses REPLIES too (replication, peers). A `*-1` that came in
        // must go back out as `*-1`, not as `$-1`.
        let mut buf = BytesMut::new();
        let f = parse_bytes(b"*-1\r\n").unwrap().unwrap();
        crate::protocol::serialize(&f, &mut buf);
        assert_eq!(&buf[..], b"*-1\r\n");

        // The RESP3 null keeps its own identity in the other direction.
        let n = parse_bytes(b"_\r\n").unwrap().unwrap();
        assert_eq!(n, Frame::Null);
        buf.clear();
        crate::protocol::serialize(&n, &mut buf);
        assert_eq!(&buf[..], b"$-1\r\n");
    }

    #[test]
    fn test_malformed_aggregate_is_null_not_null_array() {
        // The parse-FAILURE sentinel stays `Frame::Null`. Only a well-formed
        // `*-1` yields `NullArray` — otherwise the new variant would become a
        // second failure sentinel and callers could not tell a hostile frame
        // from a legitimate empty reply (moon#482, CLAUDE.md parser
        // defensiveness).
        // The outcome that matters is "never NullArray". Which NON-NullArray
        // outcome a given input takes differs by API: `parse()` reports a
        // truncated frame as `Ok(None)` (needs more bytes), while the
        // zero-copy inner parser collapses a malformed one to `Frame::Null`.
        // Both are acceptable here; a `NullArray` never is.
        for bad in [
            &b"*-7\r\n"[..],  // negative, but not -1
            &b"*-1x\r\n"[..], // trailing junk after the -1
            &b"*abc\r\n"[..], // not a number at all
            &b"%-1\r\n"[..],  // -1 has no null meaning for a map
            &b"~-1\r\n"[..],  // nor for a set
        ] {
            let shown = String::from_utf8_lossy(bad);
            match parse_bytes(bad) {
                Ok(Some(Frame::NullArray)) => panic!(
                    "malformed input {shown:?} produced a NullArray — the new \
                     variant must never become a parse-failure sentinel"
                ),
                Ok(_) | Err(_) => {}
            }
        }

        // And the zero-copy parser specifically: its documented sentinel is
        // `Frame::Null`, and a bad aggregate length must still hit it.
        let mut pos = 0usize;
        let got = parse_frame_zerocopy(
            &Bytes::from_static(b"*-7\r\n"),
            &mut pos,
            &ParseConfig::default(),
            0,
        );
        assert_eq!(
            got,
            Frame::Null,
            "a negative-but-not--1 aggregate length must stay the Null sentinel"
        );
    }

    // === Empty Array tests ===

    #[test]
    fn test_parse_empty_array() {
        let result = parse_bytes(b"*0\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::Array(framevec![]));
    }

    // === Array tests ===

    #[test]
    fn test_parse_array_of_bulk_strings() {
        let result = parse_bytes(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(
            result,
            Frame::Array(framevec![
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
            Frame::Array(framevec![Frame::Array(framevec![Frame::Integer(1)])])
        );
    }

    #[test]
    fn test_parse_array_with_null_element() {
        let result = parse_bytes(b"*3\r\n$3\r\nhey\r\n$-1\r\n$3\r\nfoo\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(
            result,
            Frame::Array(framevec![
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
            Frame::Array(framevec![Frame::BulkString(Bytes::from_static(b"!foo"))])
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
            Frame::Array(framevec![Frame::BulkString(Bytes::from_static(b"PING"))])
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
            Frame::Array(framevec![Frame::BulkString(Bytes::from_static(b"PING"))])
        );
    }

    // === #578: a blank inline line must not stall the buffered command ===
    //
    // `parse_inline` is right to answer `Ok(None)` for a blank line — it has no
    // frame to give — but `Ok(None)` also means "need more bytes", and the read
    // loops act on that second meaning: they break and wait for another
    // `read()`. So a command sitting right behind a blank line in the SAME
    // buffer went unparsed until unrelated later traffic kicked the loop.
    //
    // Measured: `\r\n\r\nECHO hi\r\n` in one send() -> `$2 hi` on redis 8.0.5,
    // no reply at all on moon. Note this needs no bare LF: it is a pure-CRLF
    // bug and predates #381.
    //
    // The fix belongs in `parse()`, the single funnel every read loop and the
    // codec share — not in the loops, where the three-dispatch-path trap would
    // let one of them silently keep the old behaviour.

    #[test]
    fn test_parse_blank_crlf_line_then_command_same_buffer() {
        let result = parse_bytes(b"\r\n\r\nECHO hi\r\n").unwrap();
        assert_eq!(
            result,
            Some(Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"ECHO")),
                Frame::BulkString(Bytes::from_static(b"hi")),
            ])),
            "a command behind blank lines must parse without another read()"
        );
    }

    #[test]
    fn test_parse_blank_lf_line_then_command_same_buffer() {
        let result = parse_bytes(b"\n\nECHO hi\n").unwrap();
        assert_eq!(
            result,
            Some(Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"ECHO")),
                Frame::BulkString(Bytes::from_static(b"hi")),
            ]))
        );
    }

    #[test]
    fn test_parse_whitespace_only_line_then_command() {
        // Whitespace-only lines take the same "consumed, but no frame" path.
        let result = parse_bytes(b"   \nECHO hi\n").unwrap();
        assert_eq!(
            result,
            Some(Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"ECHO")),
                Frame::BulkString(Bytes::from_static(b"hi")),
            ]))
        );
    }

    #[test]
    fn test_parse_blank_line_then_resp_frame_redispatches() {
        // The re-dispatch must go back through the RESP/inline decision, not
        // just retry the inline splitter: what follows a blank line is very
        // often a real RESP array, and feeding `*1` to the inline path would
        // turn a valid command into the literal token "*1".
        let result = parse_bytes(b"\r\n*1\r\n$4\r\nPING\r\n").unwrap();
        assert_eq!(
            result,
            Some(Frame::Array(framevec![Frame::BulkString(
                Bytes::from_static(b"PING")
            )]))
        );
    }

    #[test]
    fn test_parse_only_blank_lines_is_still_need_more_data() {
        // Nothing to answer with, and every byte consumed: the loop must end
        // rather than spin on an empty buffer.
        let mut buf = BytesMut::from(&b"\r\n\r\n"[..]);
        let config = ParseConfig::default();
        assert_eq!(parse(&mut buf, &config).unwrap(), None);
        assert!(buf.is_empty(), "blank lines must be consumed");
    }

    #[test]
    fn test_parse_blank_lines_then_partial_command_needs_more_data() {
        // The blank lines are consumed, the partial line is kept intact so the
        // caller can append to it.
        let mut buf = BytesMut::from(&b"\r\n\r\nECHO hi"[..]);
        let config = ParseConfig::default();
        assert_eq!(parse(&mut buf, &config).unwrap(), None);
        assert_eq!(&buf[..], b"ECHO hi");
    }

    // === RESP3 parse tests ===

    #[test]
    fn test_parse_resp3_null() {
        let result = parse_bytes(b"_\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_parse_resp3_null_rejects_junk() {
        // `_junk\r\n` must be rejected, not parsed as Null
        let result = parse_bytes(b"_junk\r\n");
        assert!(
            result.is_err(),
            "expected error for _junk\\r\\n but got {:?}",
            result
        );
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
            Frame::Set(framevec![
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
            Frame::Push(framevec![
                Frame::BulkString(Bytes::from_static(b"invalidate")),
                Frame::Array(framevec![Frame::BulkString(Bytes::from_static(b"foo"))]),
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

    #[test]
    fn test_fuzz_crash_resp3_set_negative_count() {
        // Regression test from cargo-fuzz crash artifact.
        // ~-1 followed by garbage bytes — must not panic or crash.
        let data: &[u8] = &[
            126, 45, 49, 255, 58, 10, 49, 1, 0, 141, 13, 10, 36, 45, 49, 255, 58, 10, 48, 13, 49,
            48, 141, 13, 10, 36, 45, 49, 255, 58, 48, 13, 13, 10,
        ];
        let config = ParseConfig {
            max_bulk_string_size: 64 * 1024,
            max_array_depth: 4,
            max_array_length: 256,
            max_inline_size: 64 * 1024,
        };
        let mut buf = BytesMut::from(data);
        // Must not panic — any combination of Ok/Err is acceptable
        for _ in 0..16 {
            if buf.is_empty() {
                break;
            }
            match parse(&mut buf, &config) {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(_) => break,
            }
        }
    }

    #[test]
    fn test_resp3_null_set() {
        // ~-1\r\n is a null RESP3 set
        let result = parse_bytes(b"~-1\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_resp3_null_push() {
        // >-1\r\n is a null RESP3 push
        let result = parse_bytes(b">-1\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_resp3_null_map() {
        // %-1\r\n is a null RESP3 map
        let result = parse_bytes(b"%-1\r\n").unwrap().unwrap();
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_resp3_negative_set_count() {
        // ~-2\r\n is invalid (not null, not valid count)
        let result = parse_bytes(b"~-2\r\n");
        assert!(result.is_err());
    }

    #[test]
    fn test_resp3_negative_map_count() {
        // %-2\r\n is invalid
        let result = parse_bytes(b"%-2\r\n");
        assert!(result.is_err());
    }

    #[test]
    fn test_crash_artifact_bare_lf_in_frame_count() {
        // Crash artifact: bare \n (0x0a) in array count causes validate/zerocopy divergence
        let data: &[u8] = &[
            0x2a, 0x33, 0x0d, 0x0a, 0x2a, 0x35, 0x0a, 0x0d, 0x0a, 0x5f, 0xfe, 0xff, 0xff, 0x0d,
            0x0a, 0x5f, 0x5f, 0x5f, 0x0a, 0x3a, 0x2a, 0x30, 0x0a, 0x0d, 0x0a, 0x5f, 0xfe, 0xff,
            0xe9, 0x0d, 0x0a, 0x5f, 0x5f, 0x5f, 0x0d, 0x0a, 0x5f, 0xfe, 0xff, 0xff, 0x0d, 0x0a,
            0x5f, 0x5f, 0x5f, 0x0a, 0x2a, 0x31, 0x0a, 0x0d, 0x0a, 0x5f, 0xfe, 0xff, 0xff, 0x0d,
            0x0a, 0x5f, 0x5f, 0x0a, 0x0d, 0x0a,
        ];
        // Must not panic — should return Ok or Err, never crash
        let mut buf = BytesMut::from(data);
        let config = ParseConfig {
            max_bulk_string_size: 64 * 1024,
            max_array_depth: 4,
            max_array_length: 256,
            max_inline_size: 64 * 1024,
        };
        for _ in 0..16 {
            if buf.is_empty() {
                break;
            }
            match parse(&mut buf, &config) {
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => break,
            }
        }
    }
}
