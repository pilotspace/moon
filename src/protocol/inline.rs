use memchr::{memchr, memchr2};

use bytes::{Buf, Bytes, BytesMut};

use super::frame::{Frame, FrameVec, ParseError, ProtoFault};

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
                    kind: ProtoFault::InlineTooBig,
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
            kind: ProtoFault::InlineTooBig,
            message: "Protocol error: too big inline request".into(),
            offset: 0,
        });
    }

    // Extract line content before CRLF
    let line = &buf[..crlf_pos];

    // Quoted inline arguments take the careful path. Redis parses inline
    // commands with `sdssplitargs`, which understands quoting and escapes;
    // Moon used to split on whitespace unconditionally, so `SET k "a b"`
    // became three arguments with literal quote bytes in them, and an
    // unterminated quote was silently accepted as part of a key.
    //
    // memchr2 over the line is one SIMD pass and is only paid once per inline
    // command — the unquoted case (every benchmark, every redis-cli one-liner
    // without spaces in values) keeps the original loop untouched below.
    if memchr2(b'"', b'\'', line).is_some() {
        let args = split_args_quoted(line)?;
        buf.advance(crlf_pos + 2);
        if args.is_empty() {
            return Ok(None);
        }
        return Ok(Some(Frame::Array(args)));
    }

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

/// The inline whitespace set, defined once so the argument splitter's skip loop
/// and its token-terminator loop cannot disagree.
///
/// When they disagreed, a byte that terminated a token but was not skipped left
/// the splitter unable to advance — see #487. These are exactly the bytes C's
/// `isspace()` accepts, which is what Redis's `sdssplitargs` uses.
#[inline]
fn is_inline_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

/// Split an inline command line that contains at least one quote character.
///
/// A port of Redis's `sdssplitargs` (`sds.c`), which is what defines inline
/// argument syntax for every client that speaks it — telnet users, `redis-cli`
/// pasting a quoted value, and the health-check scripts that send `PING\r\n`
/// down a bare socket.
///
/// Rules, all of them Redis's:
///   * double quotes honour `\xHH` hex and the `\n \r \t \b \a` escapes;
///     any other `\<c>` is that literal character.
///   * single quotes honour only `\'`; everything else is literal.
///   * a closing quote must be followed by whitespace or end-of-line —
///     `"foo"bar` is an error, not two tokens.
///   * an unterminated quote is an error for the whole request.
///
/// Returns `ParseError::Invalid` with [`ProtoFault::UnbalancedQuotes`] on any
/// of the error cases, which the caller turns into Redis's
/// `-ERR Protocol error: unbalanced quotes in request` and then closes.
fn split_args_quoted(line: &[u8]) -> Result<FrameVec, ParseError> {
    #[inline]
    fn hex_val(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    }
    let unbalanced = || ParseError::Invalid {
        kind: ProtoFault::UnbalancedQuotes,
        message: "Protocol error: unbalanced quotes in request".into(),
        offset: 0,
    };

    let mut args = FrameVec::new();
    let mut i = 0;
    loop {
        // The skip set MUST match the token loop's terminator set below.
        //
        // It used to be ` ` and `\t` only, while the unquoted token loop broke
        // — without advancing `i` — on ` \t \n \r \x0b \x0c`. A byte in the
        // difference at a token boundary therefore made no progress at all:
        // the token loop returned immediately, an empty arg was pushed, and the
        // outer loop restarted at the same `i`. Forever, with `args` growing
        // until the process died. Remotely reachable and pre-auth, since
        // parsing happens before dispatch (#487).
        //
        // `is_inline_space` is now the single definition both loops read, so
        // they cannot drift apart again. Matching Redis, whose `sdssplitargs`
        // skips with `isspace()` — the same six bytes.
        while i < line.len() && is_inline_space(line[i]) {
            i += 1;
        }
        if i >= line.len() {
            return Ok(args);
        }

        // One token. `current` is only allocated for tokens that actually
        // need unescaping; a plain token is copied once, same as the fast path.
        let mut current: Vec<u8> = Vec::new();
        let quote = match line[i] {
            q @ (b'"' | b'\'') => {
                i += 1;
                Some(q)
            }
            _ => None,
        };

        match quote {
            Some(b'"') => loop {
                if i >= line.len() {
                    return Err(unbalanced());
                }
                match line[i] {
                    b'\\' if i + 3 < line.len() && line[i + 1] == b'x' => {
                        match (hex_val(line[i + 2]), hex_val(line[i + 3])) {
                            (Some(hi), Some(lo)) => {
                                current.push(hi * 16 + lo);
                                i += 4;
                            }
                            // Not a valid hex escape: `\x` is literal, exactly
                            // as Redis falls through here.
                            _ => {
                                current.push(b'x');
                                i += 2;
                            }
                        }
                    }
                    b'\\' if i + 1 < line.len() => {
                        current.push(match line[i + 1] {
                            b'n' => b'\n',
                            b'r' => b'\r',
                            b't' => b'\t',
                            b'b' => 0x08,
                            b'a' => 0x07,
                            other => other,
                        });
                        i += 2;
                    }
                    b'"' => {
                        // A closing quote must end the token.
                        if i + 1 < line.len() && line[i + 1] != b' ' && line[i + 1] != b'\t' {
                            return Err(unbalanced());
                        }
                        i += 1;
                        break;
                    }
                    c => {
                        current.push(c);
                        i += 1;
                    }
                }
            },
            Some(_) => loop {
                // Single quotes: only \' is an escape.
                if i >= line.len() {
                    return Err(unbalanced());
                }
                match line[i] {
                    b'\\' if i + 1 < line.len() && line[i + 1] == b'\'' => {
                        current.push(b'\'');
                        i += 2;
                    }
                    b'\'' => {
                        if i + 1 < line.len() && line[i + 1] != b' ' && line[i + 1] != b'\t' {
                            return Err(unbalanced());
                        }
                        i += 1;
                        break;
                    }
                    c => {
                        current.push(c);
                        i += 1;
                    }
                }
            },
            None => {
                while i < line.len() {
                    // Terminator set — read through the same helper the outer
                    // skip loop uses. Breaking here does NOT advance `i`; the
                    // outer loop is what must step past this byte, which is
                    // only true while both sides agree on the set (#487).
                    if is_inline_space(line[i]) {
                        break;
                    }
                    current.push(line[i]);
                    i += 1;
                }
            }
        }
        args.push(Frame::BulkString(Bytes::from(current)));
    }
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

    // === #487: whitespace-set mismatch in the quoted path ===
    //
    // `split_args_quoted`'s outer skip loop advanced past only ` ` and `\t`,
    // while its unquoted-token loop BROKE (without advancing `i`) on the wider
    // set ` \t \n \r \x0b \x0c`. Any byte in the difference — `\n`, `\r`, VT,
    // FF — at a token boundary therefore made no progress: the token loop
    // pushed an empty arg, the outer loop restarted at the same `i`, forever,
    // growing `args` until the process died.
    //
    // Reachable remotely and pre-auth: parsing happens before dispatch, so a
    // client that opens a socket and writes `"\r\r\n` pins a shard thread at
    // 100% CPU and allocates until OOM. A lone `\r` survives into the line
    // because `find_crlf_position` only terminates on the `\r\n` PAIR.
    //
    // The quoted path is entered by any line containing `"` or `'`.
    //
    // These tests hang rather than fail before the fix — an infinite loop has
    // no assertion to trip — so a timeout IS the red signal.

    #[test]
    fn test_inline_quoted_line_with_bare_cr_terminates() {
        // `\r"` then CRLF. The line is `\r"`, so:
        //   * it contains a quote -> the quoted path handles it, and
        //   * the FIRST token byte is `\r` -> the UNQUOTED branch handles that
        //     token, and that is the branch which breaks without advancing.
        // A leading quote instead would enter the quoted sub-loop, which does
        // advance — that variant is not the bug and passes either way.
        let mut buf = BytesMut::from(&b"\r\"\r\n"[..]);
        let result = parse_inline(&mut buf, TEST_MAX_INLINE);
        match result {
            Ok(Some(Frame::Array(args))) => assert!(
                args.len() < 16,
                "bare CR produced {} args — the empty-arg loop is back",
                args.len()
            ),
            Ok(None) | Err(_) => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn test_inline_quoted_line_with_interior_whitespace_bytes_terminates() {
        // Every byte the token loop treats as a terminator must also be
        // skippable by the outer loop, or the two disagree and progress stops.
        // `\n`, VT and FF are in the token loop's set but were absent from the
        // skip loop's ` `/`\t` pair.
        for b in [b'\n', b'\r', 0x0b, 0x0c] {
            let line = [b, b'"', b'\r', b'\n'];
            let mut buf = BytesMut::from(&line[..]);
            let result = parse_inline(&mut buf, TEST_MAX_INLINE);
            if let Ok(Some(Frame::Array(args))) = &result {
                assert!(
                    args.len() < 16,
                    "byte {b:#04x} produced {} args",
                    args.len()
                );
            }
        }
    }

    #[test]
    fn test_inline_bare_cr_before_token_still_parses_the_token() {
        // Progress must not come at the cost of eating the command: a stray
        // CR ahead of a real token leaves the token intact.
        let mut buf = BytesMut::from(&b"\"a\" \rPING\r\n"[..]);
        let frame = parse_inline(&mut buf, TEST_MAX_INLINE).unwrap().unwrap();
        let Frame::Array(args) = frame else {
            panic!("expected array")
        };
        assert_eq!(args.len(), 2, "got {args:?}");
        assert_eq!(args[0], Frame::BulkString(Bytes::from_static(b"a")));
        assert_eq!(args[1], Frame::BulkString(Bytes::from_static(b"PING")));
    }

    /// The exact libFuzzer `oom-` artifact from the #487 run, through the same
    /// pipelined loop the fuzz target uses.
    #[test]
    fn test_fuzz_artifact_487_terminates() {
        use crate::protocol::{ParseConfig, parse};
        let data: &[u8] = &[
            32, 0, 1, 32, 13, 10, 13, 0, 34, 10, 13, 0, 10, 45, 48, 13, 10, 45, 56, 13, 10, 0, 124,
            13,
        ];
        let config = ParseConfig {
            max_bulk_string_size: 64 * 1024,
            max_array_depth: 4,
            max_array_length: 256,
            max_inline_size: 64 * 1024,
        };
        let mut buf = BytesMut::from(data);
        for _ in 0..16 {
            if buf.is_empty() {
                break;
            }
            match parse::parse(&mut buf, &config) {
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => break,
            }
        }
    }
}

#[cfg(test)]
mod inline_termination {
    use super::*;

    /// Every short line built from the bytes that steer the splitter's control
    /// flow must RETURN — 271k cases in ~0.03s.
    ///
    /// This is a structural guard, not four hand-picked inputs: #487 was found
    /// by a fuzzer precisely because the hand-written tests all happened to
    /// start their lines with a quote, which enters the quoted sub-loop and
    /// advances. The bug needed a quote SOMEWHERE plus `\n`/`\r`/VT/FF at a
    /// token start, a combination nobody thought to write down.
    ///
    /// Failure mode is not an assertion: without the fix this test allocates
    /// until the OS kills the process (verified — SIGKILL, exit 101), which is
    /// the same OOM libFuzzer reported. A test run that dies or hangs here IS
    /// the signal.
    #[test]
    fn every_short_line_terminates() {
        const ALPHA: &[u8] = &[
            b' ', b'\t', b'\n', b'\r', 0x0b, 0x0c, b'"', b'\'', b'\\', b'a', b'x', b'0',
        ];
        // The alphabet is every byte class the splitter branches on: each
        // whitespace byte, both quote characters, the escape byte, a hex
        // digit, an `x` (the \xHH lead-in) and an ordinary letter.
        let mut checked: u64 = 0;
        for len in 0..=5usize {
            let total = ALPHA.len().pow(len as u32);
            for n in 0..total {
                let mut line = Vec::with_capacity(len + 2);
                let mut m = n;
                for _ in 0..len {
                    line.push(ALPHA[m % ALPHA.len()]);
                    m /= ALPHA.len();
                }
                line.extend_from_slice(b"\r\n");
                let mut buf = BytesMut::from(&line[..]);
                let r = parse_inline(&mut buf, 64 * 1024);
                if let Ok(Some(Frame::Array(args))) = &r {
                    assert!(
                        args.len() <= len + 1,
                        "line {line:?} produced {} args for {len} input bytes",
                        args.len()
                    );
                }
                checked += 1;
            }
        }
        eprintln!("exhaustively checked {checked} lines");
    }
}
