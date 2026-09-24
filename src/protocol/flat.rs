//! Single-pass, resumable scanner for the flat top-level multibulk — a `*N` of
//! `$`-bulks, the shape of essentially every client command (moon#1164).
//!
//! Two properties make a large upload cost O(n) instead of O(n²):
//!
//! 1. **Tri-state answer.** [`scan_flat`] says `Complete`, `Incomplete` or
//!    `Decline`. The old scanner answered `None` for both "incomplete" and "not
//!    my shape", so every incomplete frame was walked a SECOND time by
//!    `validate_frame` just to learn it was incomplete. For every prefix this
//!    scanner calls `Incomplete`, `validate_frame` also reports `Incomplete`:
//!    both walk the same `$`-bulks with the same rules and stop at the same
//!    missing byte. The differential tests and the `resp_parse_fused` /
//!    `resp_parse_resumable` fuzz targets pin that equivalence.
//! 2. **Resumable cursor.** A [`ParseState`] keeps how far into the frame at the
//!    FRONT of the buffer the scan got, so the next attempt starts there
//!    instead of at byte 0. It is only a hint: a completed frame is always
//!    re-verified from byte 0 before it is built, so a stale cursor can never
//!    produce a wrong frame.
//!
//! Nothing here allocates while a frame is incomplete: argument spans are
//! recorded in 16 inline slots and never spill; elements past the sixteenth are
//! re-walked once, after the whole frame is known to be present.

use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;

use super::frame::{Frame, FrameVec, ParseConfig};
use super::parse::{find_crlf, strict_atoi};

/// Argument spans recorded inline by a from-zero scan. Covers every command
/// shape moon serves at argc ≤ 16; longer frames re-walk the tail once when
/// the frame is built instead of spilling to the heap mid-scan.
pub(super) const INLINE_SPANS: usize = 16;

/// A cursor is kept across calls only once this many bytes of the front frame
/// have been validated. Re-scanning a shorter prefix costs less than a read
/// syscall, so below this the hint buys nothing and is not worth holding.
pub(super) const RESUME_MIN_BYTES: usize = 16 * 1024;

/// A cursor is kept only for frames claiming at least this many elements.
///
/// This is what makes the cursor safe against the one consumer of the monoio
/// read buffer that is not the parser: the inline GET/SET fast path
/// (`server::conn::blocking::try_inline_dispatch`) only ever consumes `*2` and
/// `*3` frames, so it can never consume a frame a cursor describes. The
/// handlers still reset the state explicitly after it consumes anything —
/// this is the construction-level guarantee behind that reset.
pub(super) const RESUME_MIN_COUNT: usize = 4;

/// Position inside a flat multibulk whose first `done` elements are valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FlatCursor {
    /// Claimed element count from the `*N` header.
    pub(super) count: usize,
    /// Offset of the first element, i.e. the length of `*N\r\n`.
    pub(super) first_elem: usize,
    /// Elements validated so far.
    pub(super) done: usize,
    /// Offset at which element `done` starts.
    pub(super) pos: usize,
    /// Offset at which element `done - 1` starts (`first_elem` when `done == 0`).
    pub(super) last_elem: usize,
}

/// Outcome of [`scan_flat`].
pub(super) enum FlatScan {
    /// The whole frame is present and valid.
    Complete {
        /// Total byte length of the frame — what `validate_frame` leaves in `pos`.
        total_len: usize,
        count: usize,
        first_elem: usize,
        /// `(offset, len)` of the first `min(count, 16)` payloads — recorded
        /// only by a scan that started at byte 0.
        spans: SmallVec<[(u32, u32); INLINE_SPANS]>,
    },
    /// Valid so far; more bytes are needed. `validate_frame` agrees.
    Incomplete {
        /// Where the scan stopped, once the header has been read.
        cursor: Option<FlatCursor>,
        /// Total buffer length at which the element the scan stopped in is
        /// complete, when its length is known (0 = unknown).
        need: usize,
    },
    /// Not exactly this shape, or over a limit: the two-pass path owns it and
    /// its exact error kinds.
    Decline,
}

/// One `<decimal>\r\n` header.
enum Header {
    /// The value and the offset just past its CRLF.
    Value(i64, usize),
    /// No CRLF yet.
    Incomplete,
    /// A CRLF, but not a strict decimal before it.
    Malformed,
}

/// Parse the decimal header starting at `start`, exactly as `validate_frame`'s
/// `read_decimal` does: first CRLF, then a strict signed decimal.
#[inline]
fn header(buf: &[u8], start: usize) -> Header {
    let Some(crlf) = find_crlf(buf, start) else {
        return Header::Incomplete;
    };
    match strict_atoi(&buf[start..crlf]) {
        Some(n) => Header::Value(n, crlf + 2),
        None => Header::Malformed,
    }
}

/// The `(payload_start, len)` of an ALREADY VALIDATED bulk at `pos`, or `None`
/// if the bytes there are not one (only possible on a stale cursor).
#[inline]
pub(super) fn walk_bulk(buf: &[u8], pos: usize) -> Option<(usize, usize)> {
    if buf.get(pos) != Some(&b'$') {
        return None;
    }
    match header(buf, pos + 1) {
        Header::Value(n, payload) if n >= 0 => {
            let len = n as usize;
            if buf.len() - payload < len + 2 {
                return None;
            }
            Some((payload, len))
        }
        _ => None,
    }
}

/// Scan a flat top-level `*N` of `$`-bulks, from byte 0 (`from == None`) or
/// from a cursor a previous call returned for the same bytes.
///
/// Every rule mirrors `validate_frame`'s `*` and `$` arms, including the
/// leniency that looks like a bug and is not: **the two bytes after a bulk
/// payload are never checked** (`validate_frame` does `*pos += len + 2`), so
/// `*1\r\n$1\r\naXY` parses. Verifying them here would make the fast path
/// stricter than the path it replaces.
pub(super) fn scan_flat(buf: &[u8], config: &ParseConfig, from: Option<FlatCursor>) -> FlatScan {
    // Spans are recorded as u32. A buffer that large is not a client command.
    if buf.len() > u32::MAX as usize {
        return FlatScan::Decline;
    }
    let record = from.is_none();
    let mut cur = match from {
        Some(c) => c,
        None => {
            if buf.first() != Some(&b'*') {
                return FlatScan::Decline;
            }
            // Decline on the BYTE, not on the parsed count: `strict_atoi` reads
            // a lone `-` (and `-0`) as ZERO, while `parse_resp_two_pass`'s
            // null-multibulk gate keys on `buf[1] == b'-'` and silently
            // consumes `*-\r\n`. Found by the `resp_parse_fused` fuzz target.
            if buf.get(1) == Some(&b'-') {
                return FlatScan::Decline;
            }
            let (count, first_elem) = match header(buf, 1) {
                Header::Value(n, next) => (n, next),
                Header::Incomplete => {
                    return FlatScan::Incomplete {
                        cursor: None,
                        need: 0,
                    };
                }
                Header::Malformed => return FlatScan::Decline,
            };
            // `*-1` and below live in `parse_resp_two_pass`. Unreachable after
            // the byte test above; kept as the belt to its braces.
            if count < 0 {
                return FlatScan::Decline;
            }
            let count = count as usize;
            if count > config.max_array_length {
                return FlatScan::Decline;
            }
            // `validate_frame` validates the ELEMENTS at depth 1, so a
            // non-empty array is a depth error when the limit is 0.
            if count > 0 && config.max_array_depth < 1 {
                return FlatScan::Decline;
            }
            FlatCursor {
                count,
                first_elem,
                done: 0,
                pos: first_elem,
                last_elem: first_elem,
            }
        }
    };

    let mut spans: SmallVec<[(u32, u32); INLINE_SPANS]> = SmallVec::new();
    while cur.done < cur.count {
        let pos = cur.pos;
        #[cfg(test)]
        test_counters::note_element_visit();
        let Some(&type_byte) = buf.get(pos) else {
            return FlatScan::Incomplete {
                cursor: Some(cur),
                need: 0,
            };
        };
        if type_byte != b'$' {
            return FlatScan::Decline;
        }
        let (len, payload) = match header(buf, pos + 1) {
            Header::Value(n, next) => (n, next),
            Header::Incomplete => {
                return FlatScan::Incomplete {
                    cursor: Some(cur),
                    need: 0,
                };
            }
            Header::Malformed => return FlatScan::Decline,
        };
        // `$-1` is a null element on the two-pass path; any other negative
        // length is its error to raise.
        if len < 0 {
            return FlatScan::Decline;
        }
        let len = len as usize;
        if len > config.max_bulk_string_size {
            return FlatScan::Decline;
        }
        if buf.len() - payload < len + 2 {
            return FlatScan::Incomplete {
                cursor: Some(cur),
                need: payload + len + 2,
            };
        }
        if record && spans.len() < INLINE_SPANS {
            spans.push((payload as u32, len as u32));
        }
        cur.last_elem = pos;
        cur.pos = payload + len + 2;
        cur.done += 1;
    }

    FlatScan::Complete {
        total_len: cur.pos,
        count: cur.count,
        first_elem: cur.first_elem,
        spans,
    }
}

/// Consume a frame a from-zero [`scan_flat`] reported `Complete`, and build it.
///
/// One freeze for the whole frame (the same `Shared` promotion the two-pass
/// path performs); every argument is a zero-copy slice of it.
pub(super) fn build_flat(
    buf: &mut BytesMut,
    total_len: usize,
    count: usize,
    first_elem: usize,
    spans: &[(u32, u32)],
) -> Frame {
    let frozen: Bytes = buf.split_to(total_len).freeze();
    // `count` is bounded by the bytes actually present: the scan walked every
    // element, and each costs at least six bytes.
    let mut items = FrameVec::with_capacity(count);
    for &(start, len) in spans {
        let start = start as usize;
        items.push(Frame::BulkString(frozen.slice(start..start + len as usize)));
    }
    let mut pos = match spans.last() {
        Some(&(start, len)) => start as usize + len as usize + 2,
        None => first_elem,
    };
    while items.len() < count {
        // Validated by the scan that reported `total_len` in this same call,
        // so this cannot miss. Defensive anyway, as `parse_frame_zerocopy` is:
        // malformed input must never crash the server.
        let Some((start, len)) = walk_bulk(&frozen, pos) else {
            debug_assert!(false, "build_flat walked an unvalidated element");
            items.push(Frame::Null);
            break;
        };
        items.push(Frame::BulkString(frozen.slice(start..start + len)));
        pos = start + len + 2;
    }
    Frame::Array(items)
}

/// Resumable parse progress for ONE connection's read buffer (moon#1164).
///
/// Holds the cursor of the incomplete flat multibulk at the front of the buffer
/// between calls to [`crate::protocol::parse_resumable`], plus a read-size hint.
///
/// # Contract
///
/// A state belongs to one buffer and one [`ParseConfig`]. Between calls the
/// buffer may only be APPENDED to (or moved with its bytes intact). Anything
/// else that consumes, prepends to, or replaces the bytes at the front must
/// call [`ParseState::reset`]. That is deliberately not keyed on the buffer's
/// pointer — `BytesMut` hands the same address back after a consume + reserve,
/// which would make a pointer check pass for different bytes.
///
/// The contract only protects latency, never correctness of a frame: a frame
/// the cursor says is complete is re-verified from byte 0 before it is built,
/// and a cursor that no longer matches the header and last validated element
/// is dropped. What a missed reset could still do is report "incomplete" for
/// bytes that are complete, so the resets are not optional.
#[derive(Debug, Clone)]
pub struct ParseState {
    front: Option<SavedCursor>,
    /// Minimum total buffer length the incomplete front frame needs; 0 when
    /// the last call did not stop on an incomplete RESP multibulk.
    pending: usize,
    resume_min_bytes: usize,
    resume_min_count: usize,
    /// Re-check every resumed "incomplete" against a from-zero scan. Unit
    /// tests only; it is O(n) per call by design.
    #[cfg(test)]
    verify_resume: bool,
}

#[derive(Debug, Clone, Copy)]
struct SavedCursor {
    cur: FlatCursor,
    /// Buffer length when saved. Append-only use can only grow it.
    buf_len: usize,
}

impl Default for ParseState {
    fn default() -> Self {
        Self::new()
    }
}

impl ParseState {
    /// A state with no saved progress.
    pub const fn new() -> Self {
        Self {
            front: None,
            pending: 0,
            resume_min_bytes: RESUME_MIN_BYTES,
            resume_min_count: RESUME_MIN_COUNT,
            #[cfg(test)]
            verify_resume: true,
        }
    }

    /// A state that keeps its cursor from the first validated element on —
    /// for tests and fuzz targets, which need the resume path on small inputs.
    /// The production thresholds exist for the inline fast path's sake (see
    /// `RESUME_MIN_COUNT`), which a pure-parser harness does not have.
    #[cfg(any(test, feature = "fuzzing"))]
    pub fn eager() -> Self {
        Self {
            resume_min_bytes: 0,
            resume_min_count: 0,
            ..Self::new()
        }
    }

    /// Forget the saved cursor. Call after anything other than
    /// `parse_resumable` consumes, prepends to, or replaces the front of the
    /// buffer this state tracks.
    #[inline]
    pub fn reset(&mut self) {
        self.front = None;
        self.pending = 0;
    }

    /// Whether a cursor into the front frame is currently held.
    #[inline]
    pub fn is_resuming(&self) -> bool {
        self.front.is_some()
    }

    /// Minimum total buffer length the incomplete front frame needs before
    /// another parse can finish it: the end of the bulk the scan stopped in
    /// when its length is known, otherwise one byte more than was buffered.
    /// 0 when the last call did not stop on an incomplete multibulk.
    #[inline]
    pub fn pending_len(&self) -> usize {
        self.pending
    }

    /// Take the saved cursor if it still plausibly describes `buf`.
    pub(super) fn take_resumable(&mut self, buf: &[u8]) -> Option<FlatCursor> {
        let saved = self.front.take()?;
        let c = saved.cur;
        // Append-only use can only grow the buffer.
        if buf.len() < saved.buf_len {
            return None;
        }
        // Same header at the front...
        if buf.first() != Some(&b'*') {
            return None;
        }
        match header(buf, 1) {
            Header::Value(n, next) if n >= 0 && n as usize == c.count && next == c.first_elem => {}
            _ => return None,
        }
        // ...and the last validated element still ends exactly at the cursor.
        if c.done > 0 {
            match walk_bulk(buf, c.last_elem) {
                Some((start, len)) if start + len + 2 == c.pos => {}
                _ => return None,
            }
        }
        Some(c)
    }

    /// Record the outcome of a scan that stopped on an incomplete frame.
    pub(super) fn note_incomplete(
        &mut self,
        cursor: Option<FlatCursor>,
        buf_len: usize,
        need: usize,
    ) {
        self.pending = if need > buf_len { need } else { buf_len + 1 };
        self.front = cursor
            .filter(|c| c.count >= self.resume_min_count && c.pos >= self.resume_min_bytes)
            .map(|cur| SavedCursor { cur, buf_len });
    }

    /// A parse that did not stop on an incomplete multibulk.
    #[inline]
    pub(super) fn note_other(&mut self) {
        self.front = None;
        self.pending = 0;
    }

    #[cfg(test)]
    pub(super) fn verify_resume(&self) -> bool {
        self.verify_resume
    }

    /// Turn off the from-zero re-check of resumed scans (unit tests that
    /// measure the resumed path's work).
    #[cfg(test)]
    pub(crate) fn without_resume_verification(mut self) -> Self {
        self.verify_resume = false;
        self
    }
}

/// Per-thread element-visit counter, so a unit test can prove the work a
/// chunked upload costs is linear without timing anything.
#[cfg(test)]
pub(crate) mod test_counters {
    use std::cell::Cell;

    thread_local! {
        static VISITS: Cell<u64> = const { Cell::new(0) };
    }

    #[inline]
    pub(crate) fn note_element_visit() {
        VISITS.with(|v| v.set(v.get() + 1));
    }

    /// Visits so far on this thread; reset to 0.
    pub(crate) fn take_element_visits() -> u64 {
        VISITS.with(|v| v.replace(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::ParseError;
    use crate::protocol::parse::{parse, parse_reference_two_pass, parse_resumable};

    /// Deterministic xorshift64* — no dependency, reproducible failures.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n.max(1) as u64) as usize
        }
    }

    fn enc(args: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    /// A random flat multibulk, sometimes mutated into a shape the fast path
    /// must decline (null bulk, nested array, bad length, non-bulk element) or
    /// that breaks the rules the scanner mirrors (missing trailing CRLF).
    fn random_frame(rng: &mut Rng) -> Vec<u8> {
        let argc = match rng.below(10) {
            0 => 0,
            1..=6 => 1 + rng.below(6),
            _ => 4 + rng.below(40),
        };
        let mut args: Vec<Vec<u8>> = Vec::with_capacity(argc);
        for _ in 0..argc {
            let len = match rng.below(8) {
                0 => 0,
                1..=5 => 1 + rng.below(12),
                _ => 13 + rng.below(300),
            };
            // Binary payloads, CRLF and '$'/'*' bytes included on purpose.
            let arg: Vec<u8> = (0..len)
                .map(|_| match rng.below(8) {
                    0 => b'\r',
                    1 => b'\n',
                    2 => b'$',
                    3 => b'*',
                    _ => b'a' + rng.below(26) as u8,
                })
                .collect();
            args.push(arg);
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        let mut frame = enc(&refs);
        if rng.below(4) == 0 && !frame.is_empty() {
            let mutant: &[u8] = match rng.below(7) {
                0 => b"$-1\r\n",
                1 => b"*1\r\n$1\r\nz\r\n",
                2 => b"$abc\r\n",
                3 => b":7\r\n",
                4 => b"$-2\r\n",
                5 => b"$99999999999999999999\r\n",
                _ => b"XY",
            };
            let at = rng.below(frame.len());
            frame.splice(at..at, mutant.iter().copied());
        }
        frame
    }

    fn inputs() -> Vec<Vec<u8>> {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut v: Vec<Vec<u8>> = vec![
            enc(&[b"PING"]),
            enc(&[b"SET", b"k", b"v", b"EX", b"100"]),
            enc(&[b"GET", b""]),
            b"*-1\r\n".to_vec(),
            b"*-\r\n*0\r\n".to_vec(),
            b"*1\r\n$1\r\naXY".to_vec(),
            b"\r\n\r\nECHO hi\r\n*1\r\n$4\r\nPING\r\n".to_vec(),
            b"%1\r\n$1\r\na\r\n$1\r\nb\r\n".to_vec(),
            b"*2\r\n$3\r\nGET\r\n:1\r\n".to_vec(),
        ];
        for _ in 0..160 {
            // Pipelines of 1..4 frames: a divergence on the second frame is
            // exactly what a single-frame check misses.
            let mut pipe = Vec::new();
            for _ in 0..1 + rng.below(4) {
                pipe.extend(random_frame(&mut rng));
            }
            v.push(pipe);
        }
        v
    }

    fn same(
        fast: &Result<Option<Frame>, ParseError>,
        slow: &Result<Option<Frame>, ParseError>,
        ctx: &str,
    ) {
        match (fast, slow) {
            (Ok(a), Ok(b)) => assert_eq!(a, b, "frame diverged {ctx}"),
            (Err(a), Err(b)) => assert_eq!(a.to_string(), b.to_string(), "error diverged {ctx}"),
            _ => panic!("ok/err diverged {ctx}: fast={fast:?} slow={slow:?}"),
        }
    }

    /// Feed `input` in the given chunk sizes to ONE resumable state and to the
    /// stateless two-pass reference, draining both after every chunk exactly
    /// as a read loop does; every answer and every byte left must agree.
    fn feed_and_compare(input: &[u8], chunks: &[usize], config: &ParseConfig, state: ParseState) {
        let mut state = state;
        let mut fast_buf = BytesMut::new();
        let mut slow_buf = BytesMut::new();
        let mut at = 0usize;
        let mut ci = 0usize;
        let shown = String::from_utf8_lossy(input);
        while at < input.len() {
            let n = chunks[ci % chunks.len()].max(1).min(input.len() - at);
            ci += 1;
            fast_buf.extend_from_slice(&input[at..at + n]);
            slow_buf.extend_from_slice(&input[at..at + n]);
            at += n;
            for step in 0..64 {
                let fast = parse_resumable(&mut fast_buf, config, &mut state);
                let slow = parse_reference_two_pass(&mut slow_buf, config);
                let ctx = format!("on {shown:?} after {at} bytes, step {step}");
                same(&fast, &slow, &ctx);
                assert_eq!(
                    fast_buf.as_ref(),
                    slow_buf.as_ref(),
                    "consumption diverged {ctx}"
                );
                match fast {
                    Ok(Some(_)) => continue,
                    Ok(None) => break,
                    // A protocol fault closes the connection: nothing more is fed.
                    Err(_) => return,
                }
            }
        }
    }

    /// moon#1164 property: for EVERY prefix of every input — fed one byte at a
    /// time, so every prefix is a parse attempt — the resumable parser answers
    /// exactly what the two-pass reference answers, with the resume cursor kept
    /// at every opportunity (`ParseState::eager`).
    #[test]
    fn every_prefix_resumable_agrees_with_two_pass() {
        let configs = [
            ParseConfig::default(),
            ParseConfig {
                max_bulk_string_size: 8,
                max_array_depth: 4,
                max_array_length: 16,
                max_inline_size: 64,
            },
            ParseConfig {
                max_bulk_string_size: 1024,
                max_array_depth: 0,
                max_array_length: 256,
                max_inline_size: 64,
            },
        ];
        for config in &configs {
            for input in inputs() {
                feed_and_compare(&input, &[1], config, ParseState::eager());
            }
        }
    }

    /// Same property over irregular chunking, and with the production
    /// thresholds as well as the eager ones.
    #[test]
    fn chunked_resumable_agrees_with_two_pass() {
        let config = ParseConfig::default();
        let mut rng = Rng(42);
        for input in inputs() {
            let chunks: Vec<usize> = (0..8).map(|_| 1 + rng.below(97)).collect();
            feed_and_compare(&input, &chunks, &config, ParseState::eager());
            feed_and_compare(&input, &chunks, &config, ParseState::new());
        }
        // A frame big enough to cross the PRODUCTION thresholds, in 8 KiB
        // reads, followed by a pipelined command in the same read.
        let elems: Vec<Vec<u8>> = (0..9000).map(|i| format!("e{i}").into_bytes()).collect();
        let mut args: Vec<&[u8]> = vec![b"RPUSH", b"k"];
        args.extend(elems.iter().map(|e| e.as_slice()));
        let mut big = enc(&args);
        big.extend(enc(&[b"PING"]));
        feed_and_compare(&big, &[8192], &config, ParseState::new());
        feed_and_compare(&big, &[8192, 1, 777], &config, ParseState::new());
    }

    fn rpush(n: usize) -> Vec<u8> {
        let mut out = format!("*{}\r\n$5\r\nRPUSH\r\n$4\r\nbigl\r\n", n + 2).into_bytes();
        for _ in 0..n {
            out.extend_from_slice(b"$1\r\nx\r\n");
        }
        out
    }

    /// Element visits to parse `input` fed in `chunk`-byte reads.
    fn visits(input: &[u8], chunk: usize, mut state: Option<ParseState>) -> (u64, Frame) {
        let config = ParseConfig::default();
        let mut buf = BytesMut::new();
        test_counters::take_element_visits();
        for piece in input.chunks(chunk) {
            buf.extend_from_slice(piece);
            let got = match state.as_mut() {
                Some(s) => parse_resumable(&mut buf, &config, s),
                None => parse(&mut buf, &config),
            };
            if let Ok(Some(frame)) = got {
                assert!(buf.is_empty());
                return (test_counters::take_element_visits(), frame);
            }
        }
        panic!("frame never completed");
    }

    /// moon#1164 red test: an n-element command arriving in 8 KiB reads must
    /// cost O(n) element visits. Before the fix every read re-walked the whole
    /// frame (twice), i.e. ~n²/2k visits for k elements per read.
    #[test]
    fn chunked_upload_work_is_linear() {
        for n in [20_000usize, 80_000] {
            let input = rpush(n);
            let (resumed, frame) = visits(
                &input,
                8192,
                Some(ParseState::new().without_resume_verification()),
            );
            match frame {
                Frame::Array(items) => assert_eq!(items.len(), n + 2),
                other => panic!("expected Array, got {other:?}"),
            }
            // Resumed scans visit each element about once, plus ONE from-zero
            // verification walk when the frame completes, plus the prefix
            // re-scanned before the cursor is kept (< RESUME_MIN_BYTES).
            let budget = 2 * (n as u64 + 2) + 2 * RESUME_MIN_BYTES as u64;
            assert!(
                resumed <= budget,
                "n={n}: {resumed} element visits for a chunked upload (budget {budget}) — not linear"
            );

            // The harness can tell: the stateless parser re-walks from byte 0
            // on every read, which is quadratic (~16x the resumed work here).
            if n == 80_000 {
                let (stateless, _) = visits(&input, 8192, None);
                assert!(
                    stateless > 8 * resumed,
                    "n={n}: stateless parse did {stateless} visits vs {resumed} resumed — \
                     the linearity check above is vacuous"
                );
            }
        }
    }

    /// A cursor left over from different bytes (a missed `reset`) must never
    /// yield a wrong frame: a "complete" answer is re-verified from byte 0.
    #[test]
    fn stale_cursor_never_builds_a_wrong_frame() {
        let config = ParseConfig::default();
        let a = enc(&[b"RPUSH", b"k", b"aaaa", b"bbbb", b"cccc"]);
        // Same header and same element layout, different bytes.
        let b = enc(&[b"RPUSH", b"k", b"wxyz", b"wxyz", b"wxyz"]);
        let mut state = ParseState::eager();
        let mut buf = BytesMut::from(&a[..a.len() - 3]);
        assert!(
            parse_resumable(&mut buf, &config, &mut state)
                .unwrap()
                .is_none()
        );
        assert!(state.is_resuming());
        // Simulate a consumer that swapped the bytes WITHOUT resetting.
        buf.clear();
        buf.extend_from_slice(&b);
        let mut state = state.without_resume_verification();
        let got = parse_resumable(&mut buf, &config, &mut state)
            .unwrap()
            .unwrap();
        let mut rbuf = BytesMut::from(&b[..]);
        assert_eq!(
            Some(got),
            parse_reference_two_pass(&mut rbuf, &config).unwrap()
        );
        assert!(buf.is_empty());

        // A stale cursor whose header no longer matches is dropped outright.
        let mut state = ParseState::eager();
        let mut buf = BytesMut::from(&a[..a.len() - 3]);
        assert!(
            parse_resumable(&mut buf, &config, &mut state)
                .unwrap()
                .is_none()
        );
        buf.clear();
        buf.extend_from_slice(&enc(&[b"GET", b"k"]));
        let got = parse_resumable(&mut buf, &config, &mut state)
            .unwrap()
            .unwrap();
        assert_eq!(
            got,
            Frame::Array(crate::framevec![
                Frame::BulkString(Bytes::from_static(b"GET")),
                Frame::BulkString(Bytes::from_static(b"k")),
            ])
        );
        assert!(!state.is_resuming());
    }

    /// `reset` returns the state to "fresh", and the production thresholds keep
    /// small frames and `*2`/`*3` frames (the inline fast path's shapes) from
    /// ever holding a cursor.
    #[test]
    fn production_thresholds_and_reset() {
        let config = ParseConfig::default();
        // A big *3 SET: long prefix, but count 3 — never resumable.
        let key = vec![b'k'; 40_000];
        let set = enc(&[b"SET", &key, b"value"]);
        let mut state = ParseState::new();
        let mut buf = BytesMut::from(&set[..set.len() - 2]);
        assert!(
            parse_resumable(&mut buf, &config, &mut state)
                .unwrap()
                .is_none()
        );
        assert!(!state.is_resuming(), "a *3 frame must never hold a cursor");
        // A small many-element frame: under RESUME_MIN_BYTES — no cursor.
        let small = enc(&[b"DEL", b"a", b"b", b"c", b"d"]);
        let mut buf = BytesMut::from(&small[..small.len() - 1]);
        assert!(
            parse_resumable(&mut buf, &config, &mut state)
                .unwrap()
                .is_none()
        );
        assert!(!state.is_resuming());
        // A large many-element frame keeps one, and `reset` drops it.
        let big = rpush(10_000);
        let mut buf = BytesMut::from(&big[..big.len() - 1]);
        assert!(
            parse_resumable(&mut buf, &config, &mut state)
                .unwrap()
                .is_none()
        );
        assert!(state.is_resuming());
        assert_eq!(state.pending_len(), big.len());
        state.reset();
        assert!(!state.is_resuming());
        assert_eq!(state.pending_len(), 0);
    }

    /// The read-size hint: the end of the bulk the scan stopped in when its
    /// length is known, one byte past the buffer otherwise, 0 once parsed.
    #[test]
    fn pending_len_hint() {
        let config = ParseConfig::default();
        let mut state = ParseState::new();
        let head = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1000000\r\nabc";
        let mut buf = BytesMut::from(&head[..]);
        assert!(
            parse_resumable(&mut buf, &config, &mut state)
                .unwrap()
                .is_none()
        );
        let payload_start = head.len() - 3;
        assert_eq!(state.pending_len(), payload_start + 1_000_000 + 2);

        let mut buf = BytesMut::from(&b"*3\r\n$3\r\nSET\r\n$1"[..]);
        assert!(
            parse_resumable(&mut buf, &config, &mut state)
                .unwrap()
                .is_none()
        );
        assert_eq!(state.pending_len(), buf.len() + 1);

        let mut buf = BytesMut::from(&b"*1\r\n$4\r\nPING\r\n"[..]);
        assert!(
            parse_resumable(&mut buf, &config, &mut state)
                .unwrap()
                .is_some()
        );
        assert_eq!(state.pending_len(), 0);
    }
}
