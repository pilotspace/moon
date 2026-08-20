use memchr::{memchr, memchr2, memchr3};

use bytes::{Buf, Bytes, BytesMut};

use super::frame::{Frame, FrameVec, ParseError, ProtoFault};

/// Parse an inline command from the buffer.
///
/// Inline commands are plain text lines terminated by `\n`, optionally preceded
/// by `\r`, where arguments are separated by whitespace. This is what telnet
/// users, redis-cli direct input, and shell/awk-generated command streams send —
/// the last of which commonly emit bare LF (#381).
///
/// Returns `Ok(Some(Frame::Array(...)))` with each argument as a `BulkString`,
/// `Ok(None)` if the buffer doesn't contain a complete line (no `\n` found),
/// or `Ok(None)` for empty/whitespace-only lines (after advancing past the
/// terminator).
///
/// Note those two `Ok(None)`s differ in whether the buffer moved, and callers
/// must not conflate them: an empty line consumed bytes and the caller should
/// ask again, whereas an incomplete line did not and the caller must read more.
/// [`crate::protocol::parse`] is where that distinction is handled for every
/// read loop (#578); do not re-derive it per call site.
///
/// `max_inline_size` bounds the length of a single inline line. If the buffer
/// grows past it without a terminator, the line is rejected with a protocol
/// error instead of returning `Ok(None)` forever — this is what stops a client
/// that never sends a line break from growing the read buffer without limit
/// (mirrors Redis's `PROTO_INLINE_MAX_SIZE`).
pub fn parse_inline(
    buf: &mut BytesMut,
    max_inline_size: usize,
) -> Result<Option<Frame>, ParseError> {
    // Find the line terminator: `\n`, with an optional `\r` before it.
    let (line_len, consumed) = match find_line_terminator(&buf[..]) {
        Some(t) => t,
        None => {
            // No complete line yet. Reject before the buffer can grow unbounded:
            // a non-RESP stream with no line break would otherwise "need more
            // data" forever. Redis caps this at PROTO_INLINE_MAX_SIZE.
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
    if line_len > max_inline_size {
        return Err(ParseError::Invalid {
            kind: ProtoFault::InlineTooBig,
            message: "Protocol error: too big inline request".into(),
            offset: 0,
        });
    }

    // Extract line content before the terminator
    let line = &buf[..line_len];

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
        buf.advance(consumed);
        if args.is_empty() {
            return Ok(None);
        }
        return Ok(Some(Frame::Array(args)));
    }

    // Split on the separator set using SIMD, filtering empty slices.
    //
    // `\r` belongs here and used to be missing: Redis breaks unquoted tokens on
    // {space, \n, \r, \t}, so `RPUSH k a\rb` is two elements there and was one
    // in Moon (measured). `\n` cannot appear — it is the terminator — so the
    // three bytes below are the whole set this path can ever see.
    let mut args = FrameVec::new();
    let mut start = 0;
    while start < line.len() {
        // Skip separators
        while start < line.len() && is_inline_separator(line[start]) {
            start += 1;
        }
        if start >= line.len() {
            break;
        }
        // Find next separator using SIMD
        match memchr3(b' ', b'\t', b'\r', &line[start..]) {
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

    // Advance buffer past line + terminator
    buf.advance(consumed);

    // Empty/whitespace-only lines produce no frame
    if args.is_empty() {
        return Ok(None);
    }

    Ok(Some(Frame::Array(args)))
}

/// The bytes the splitter may SKIP between tokens, and the bytes a closing
/// quote may be followed by. Exactly C's `isspace()`.
///
/// Redis reads two different sets here, and conflating them is a bug in either
/// direction:
///   * `sdssplitargs` skips inter-token bytes with `isspace()`, and validates
///     the byte after a closing quote with `isspace()` — this set.
///   * it ends an unquoted token on an explicit 5-byte list — see
///     [`is_inline_separator`], which is strictly narrower.
///
/// A previous comment here claimed this set was what `sdssplitargs` uses for
/// *both* jobs. It is not: `\x0b` and `\x0c` are `isspace()` but are NOT token
/// separators, so `RPUSH k a\x0bb` is ONE element on redis-server 8.0.5
/// (measured). Using this wider set as the terminator split it into two.
///
/// The skip set must remain a SUPERSET of the terminator set. When it was not,
/// a byte that ended a token but was not skipped left the splitter unable to
/// advance: the token loop returned without moving `i`, an empty arg was
/// pushed, and the outer loop restarted at the same byte — forever, growing
/// `args` until the process died, remotely and pre-auth (#487).
#[inline]
fn is_inline_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

/// The bytes that END an unquoted inline token.
///
/// Redis's `sdssplitargs` token loop tests an explicit list — `' '`, `'\n'`,
/// `'\r'`, `'\t'`, `'\0'` — not `isspace()`. `\0` is omitted here because the
/// oracle is ambiguous (redis-server returns nothing at all for an inline line
/// containing a NUL), so Moon keeps its existing behaviour of treating it as an
/// ordinary token byte rather than guessing.
///
/// A strict subset of [`is_inline_space`], which is what keeps the #487
/// progress invariant intact: every byte that terminates a token is still
/// skippable by the outer loop.
#[inline]
fn is_inline_separator(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r')
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
                        // A closing quote must end the token — it may be
                        // followed by ANY inline whitespace, or nothing.
                        // Reading the narrow ` `/`\t` pair here while the skip
                        // loop reads the wide set is the same disagreement that
                        // caused #487, in its milder form: `"a"\rb` was
                        // rejected as unbalanced where Redis splits it into two
                        // arguments (measured — `sdssplitargs` tests
                        // `isspace(p[1])`).
                        if i + 1 < line.len() && !is_inline_space(line[i + 1]) {
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
                        // Same rule as the double-quote branch above; measured
                        // identical on redis-server for both quote types.
                        if i + 1 < line.len() && !is_inline_space(line[i + 1]) {
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
                    // Terminator set — NARROWER than the outer skip set, which
                    // is what Redis does (`\x0b`/`\x0c` are `isspace()` but do
                    // not end a token). Breaking here does NOT advance `i`; the
                    // outer loop must step past this byte, which stays true as
                    // long as the terminator set remains a subset of the skip
                    // set (#487).
                    if is_inline_separator(line[i]) {
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

/// SIMD-accelerated inline line-terminator finder.
///
/// Returns `(line_len, consumed)`: the length of the line content excluding the
/// terminator, and how far the caller must advance to step past line and
/// terminator together.
///
/// This mirrors Redis's `processInlineBuffer`, which searches for the first
/// `\n` and then drops ONE optional preceding `\r`. It does **not** require
/// `\r\n` (#381).
///
/// The old version searched for `\r\n` and, on finding a lone `\r`, kept
/// scanning. Two consequences, both measured against redis-server 8.0.5:
///   * a bare-LF stream never terminated, so the command was never dispatched
///     and the client simply hung; and
///   * worse, `SET k v1\nSET k v2\r\n` skipped the interior `\n` to reach the
///     trailing `\r\n` and produced ONE command with the second command's
///     arguments appended — a silent write loss, not just a rejection.
#[inline]
fn find_line_terminator(buf: &[u8]) -> Option<(usize, usize)> {
    let nl = memchr(b'\n', buf)?;
    // Only the `\r` immediately before the `\n` is part of the terminator; any
    // earlier one is line content, and `is_inline_separator` treats it as a
    // token break exactly as Redis does.
    if nl > 0 && buf[nl - 1] == b'\r' {
        Some((nl - 1, nl + 1))
    } else {
        Some((nl, nl + 1))
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
    fn test_closing_quote_may_be_followed_by_any_inline_space() {
        // Measured against redis-server 8.6.1 over a raw socket, one fresh
        // connection per probe:
        //   ECHO "hi"\rx    -> -ERR wrong number of arguments for 'echo'
        //   ECHO 'hi'\rx    -> -ERR wrong number of arguments for 'echo'
        //   ECHO "hi"\x0bx  -> -ERR wrong number of arguments for 'echo'
        //   ECHO "hi"\x0cx  -> -ERR wrong number of arguments for 'echo'
        //   ECHO "hi"\tx    -> -ERR wrong number of arguments for 'echo'
        //   ECHO "hi"y      -> -ERR Protocol error: unbalanced quotes in request
        // i.e. the arg-count error proves Redis SPLIT the line into three
        // arguments; only a non-whitespace byte is unbalanced. That is
        // `isspace(p[1])` in `sdssplitargs`, so the check must read the same
        // set the rest of the splitter uses.
        for q in [b'"', b'\''] {
            for ws in [b' ', b'\t', b'\r', 0x0b, 0x0c] {
                let mut line = vec![q, b'a', q];
                line.push(ws);
                line.extend_from_slice(b"b\r\n");
                let mut buf = BytesMut::from(&line[..]);
                let frame = parse_inline(&mut buf, TEST_MAX_INLINE)
                    .unwrap_or_else(|e| {
                        panic!(
                            "quote {:?} + ws {ws:#04x} must not be unbalanced: {e:?}",
                            q as char
                        )
                    })
                    .expect("a frame");
                let Frame::Array(args) = frame else {
                    panic!("expected array")
                };
                assert_eq!(
                    args.len(),
                    2,
                    "quote {:?} + ws {ws:#04x} should split into 2 args, got {args:?}",
                    q as char
                );
                assert_eq!(args[0], Frame::BulkString(Bytes::from_static(b"a")));
                assert_eq!(args[1], Frame::BulkString(Bytes::from_static(b"b")));
            }
        }
    }

    #[test]
    fn test_closing_quote_followed_by_non_space_is_still_unbalanced() {
        // The other half of the measured oracle — widening the accepted set
        // must not turn `"foo"bar` into two tokens.
        for q in [b'"', b'\''] {
            let line = [q, b'a', q, b'b', b'\r', b'\n'];
            let mut buf = BytesMut::from(&line[..]);
            let err = parse_inline(&mut buf, TEST_MAX_INLINE)
                .expect_err("a closing quote glued to a token is an error");
            match err {
                ParseError::Invalid { kind, .. } => {
                    assert_eq!(kind, ProtoFault::UnbalancedQuotes)
                }
                other => panic!("unexpected error: {other:?}"),
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

    // === #381: line termination and the separator set ===
    //
    // Every expectation below was measured against `redis-server 8.0.5` over a
    // raw socket, one fresh connection per case, on the moon-dev VM. Where a
    // token count decides the answer the probe used `RPUSH`, whose integer
    // reply IS the element count — an `ECHO` probe cannot tell 3 args from 4,
    // because both are just "wrong number of arguments".
    //
    // Redis's `processInlineBuffer` looks for `\n` and then drops ONE optional
    // preceding `\r`; it never requires `\r\n`. Moon required `\r\n` and
    // scanned PAST an embedded `\n`, which is why bare-LF hung and why
    // `ECHO a\nECHO b\r\n` silently merged two commands into one.

    /// Split an inline line and return its arguments, or panic with the frame.
    fn args_of(input: &[u8]) -> Vec<Bytes> {
        let mut buf = BytesMut::from(input);
        match parse_inline(&mut buf, TEST_MAX_INLINE) {
            Ok(Some(Frame::Array(args))) => args
                .iter()
                .map(|a| match a {
                    Frame::BulkString(b) => b.clone(),
                    other => panic!("non-bulk arg: {other:?}"),
                })
                .collect(),
            other => panic!("expected an argument array, got {other:?}"),
        }
    }

    #[test]
    fn test_inline_bare_lf_terminates_a_line() {
        // redis: `PING\n` -> +PONG. moon before #381: no reply at all, ever —
        // the line never terminated so the command was never dispatched.
        let mut buf = BytesMut::from(&b"PING\n"[..]);
        let frame = parse_inline(&mut buf, TEST_MAX_INLINE).unwrap().unwrap();
        assert_eq!(
            frame,
            Frame::Array(framevec![Frame::BulkString(Bytes::from_static(b"PING"))])
        );
        assert!(buf.is_empty(), "LF must be consumed, left: {:?}", &buf[..]);
    }

    #[test]
    fn test_inline_bare_lf_two_commands_in_one_buffer() {
        // redis: `SET il 1\nGET il\n` -> +OK then $1 1.
        let mut buf = BytesMut::from(&b"SET il 1\nGET il\n"[..]);
        let first = parse_inline(&mut buf, TEST_MAX_INLINE).unwrap().unwrap();
        assert_eq!(
            first,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"SET")),
                Frame::BulkString(Bytes::from_static(b"il")),
                Frame::BulkString(Bytes::from_static(b"1")),
            ])
        );
        let second = parse_inline(&mut buf, TEST_MAX_INLINE).unwrap().unwrap();
        assert_eq!(
            second,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"GET")),
                Frame::BulkString(Bytes::from_static(b"il")),
            ])
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn test_inline_lf_must_not_swallow_the_following_command() {
        // The correctness half of #381, and the reason it is not merely a
        // rejection bug: scanning past the interior `\n` to reach the trailing
        // `\r\n` made ONE command out of two, silently appending the second
        // command's arguments to the first.
        //
        // `SET k v1\nSET k v2\r\n` must be two writes, never one malformed
        // command. redis: two replies.
        let mut buf = BytesMut::from(&b"SET k v1\nSET k v2\r\n"[..]);
        let first = parse_inline(&mut buf, TEST_MAX_INLINE).unwrap().unwrap();
        assert_eq!(
            first,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"SET")),
                Frame::BulkString(Bytes::from_static(b"k")),
                Frame::BulkString(Bytes::from_static(b"v1")),
            ]),
            "the interior LF must end the first command"
        );
        let second = parse_inline(&mut buf, TEST_MAX_INLINE).unwrap().unwrap();
        assert_eq!(
            second,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"SET")),
                Frame::BulkString(Bytes::from_static(b"k")),
                Frame::BulkString(Bytes::from_static(b"v2")),
            ])
        );
    }

    #[test]
    fn test_inline_crlf_still_terminates_after_the_lf_change() {
        // Guard the common path against a regression in the terminator rewrite.
        let mut buf = BytesMut::from(&b"GET key\r\nPING\r\n"[..]);
        assert_eq!(
            parse_inline(&mut buf, TEST_MAX_INLINE).unwrap().unwrap(),
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"GET")),
                Frame::BulkString(Bytes::from_static(b"key")),
            ])
        );
        assert_eq!(
            parse_inline(&mut buf, TEST_MAX_INLINE).unwrap().unwrap(),
            Frame::Array(framevec![Frame::BulkString(Bytes::from_static(b"PING"))])
        );
    }

    #[test]
    fn test_inline_only_one_cr_is_stripped_before_the_lf() {
        // redis `PING\r\r\n` -> +PONG: it strips ONE `\r`, leaving `PING\r`,
        // and then `\r` is a token separator so the trailing one vanishes.
        // moon kept it and answered `unknown command 'PING\r'`.
        assert_eq!(args_of(b"PING\r\r\n"), vec![Bytes::from_static(b"PING")]);
    }

    #[test]
    fn test_inline_cr_separates_unquoted_tokens() {
        // Measured with RPUSH: redis pushes 2 elements for `a\rb`, moon pushed
        // 1. Redis's `sdssplitargs` breaks unquoted tokens on {space, \n, \r,
        // \t, \0}; moon's fast path broke on {space, \t} only.
        assert_eq!(
            args_of(b"RPUSH k a\rb\r\n"),
            vec![
                Bytes::from_static(b"RPUSH"),
                Bytes::from_static(b"k"),
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
            ]
        );
    }

    #[test]
    fn test_inline_vt_and_ff_do_not_separate_tokens_in_either_path() {
        // The mirror-image divergence. Redis's token loop uses an explicit
        // 5-byte list, NOT `isspace()` — so VT and FF stay INSIDE a token.
        // Measured: `RPUSH u a\x0bb` -> 1 element on redis.
        //
        // Moon's fast unquoted path already agreed; its quoted path did not,
        // because `is_inline_space` (an isspace() set) served as the token
        // terminator too. A line merely CONTAINING a quote therefore split
        // `a\x0bb` while the same line without one did not.
        for sep in [0x0bu8, 0x0c] {
            let mut line = b"RPUSH u a".to_vec();
            line.push(sep);
            line.extend_from_slice(b"b\r\n");
            assert_eq!(
                args_of(&line),
                vec![
                    Bytes::from_static(b"RPUSH"),
                    Bytes::from_static(b"u"),
                    Bytes::copy_from_slice(&[b'a', sep, b'b']),
                ],
                "unquoted line, separator {sep:#04x}"
            );

            // Same line, but a quoted token earlier forces the quoted path.
            let mut qline = b"RPUSH \"q\" a".to_vec();
            qline.push(sep);
            qline.extend_from_slice(b"b\r\n");
            assert_eq!(
                args_of(&qline),
                vec![
                    Bytes::from_static(b"RPUSH"),
                    Bytes::from_static(b"q"),
                    Bytes::copy_from_slice(&[b'a', sep, b'b']),
                ],
                "quoted line, separator {sep:#04x}"
            );
        }
    }

    #[test]
    fn test_inline_cr_separates_tokens_in_the_quoted_path_too() {
        // The quoted path already got this right; pin it so the narrowing of
        // the terminator set does not drop `\r` along with VT and FF.
        assert_eq!(
            args_of(b"RPUSH \"q\" a\rb\r\n"),
            vec![
                Bytes::from_static(b"RPUSH"),
                Bytes::from_static(b"q"),
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
            ]
        );
    }

    #[test]
    fn test_inline_closing_quote_then_vt_skips_to_the_next_token() {
        // Redis uses TWO different sets, and this is where they part company:
        // the byte after a closing quote is validated with `isspace()` (so VT
        // is legal there and is skipped), while the token loop's narrower list
        // decides where a token ENDS. Narrowing the terminator set must not
        // also narrow the skip set, or `"a"\x0bb` would yield `\x0bb`.
        assert_eq!(
            args_of(b"ECHO \"a\"\x0bb\r\n"),
            vec![
                Bytes::from_static(b"ECHO"),
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
            ]
        );
    }

    #[test]
    fn test_inline_lf_terminated_line_can_be_unbalanced() {
        // redis: `ECHO "hi\n` -> -ERR Protocol error: unbalanced quotes.
        // moon returned Ok(None) and waited forever for a `\r\n` that the
        // client had no reason to send.
        let mut buf = BytesMut::from(&b"ECHO \"hi\n"[..]);
        let err = parse_inline(&mut buf, TEST_MAX_INLINE)
            .expect_err("an LF-terminated unbalanced quote is still an error");
        match err {
            ParseError::Invalid { kind, .. } => assert_eq!(kind, ProtoFault::UnbalancedQuotes),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_inline_lf_inside_quotes_still_terminates_the_line() {
        // Termination is decided BEFORE quoting: redis finds the `\n` first,
        // so `ECHO "a\nb"\r\n` is the line `ECHO "a` — unbalanced. moon parsed
        // the whole thing and handed back a value containing a newline.
        let mut buf = BytesMut::from(&b"ECHO \"a\nb\"\r\n"[..]);
        let err = parse_inline(&mut buf, TEST_MAX_INLINE)
            .expect_err("a quote cannot span an inline line break");
        match err {
            ParseError::Invalid { kind, .. } => assert_eq!(kind, ProtoFault::UnbalancedQuotes),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_inline_single_quoted_arg_with_lf_terminator() {
        // redis: `ECHO 'a b'\n` -> $3 "a b". The quoted path must see the same
        // terminator rules as the fast path.
        assert_eq!(
            args_of(b"ECHO 'a b'\n"),
            vec![Bytes::from_static(b"ECHO"), Bytes::from_static(b"a b")]
        );
    }

    #[test]
    fn test_inline_size_cap_applies_to_lf_terminated_lines() {
        // The memory guard must not be bypassable by using LF instead of CRLF.
        let cap = 8;
        let mut buf = BytesMut::from(&b"THIS LINE IS WAY TOO LONG\n"[..]);
        let err = parse_inline(&mut buf, cap).unwrap_err();
        assert!(matches!(err, ParseError::Invalid { .. }));
    }

    #[test]
    fn test_inline_lf_line_exactly_at_cap_is_accepted() {
        // Boundary stays inclusive, and the terminator is not counted.
        let mut buf = BytesMut::from(&b"PING\n"[..]);
        let frame = parse_inline(&mut buf, 4).unwrap().unwrap();
        assert_eq!(
            frame,
            Frame::Array(framevec![Frame::BulkString(Bytes::from_static(b"PING"))])
        );
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
