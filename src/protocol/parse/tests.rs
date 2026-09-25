//! Unit tests of `protocol::parse` (moved verbatim out of parse.rs to keep it under the
//! 1500-line cap, moon#1226).

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
/// is testing the new code and not the old code twice — and that the three
/// answers are the ones intended: an incomplete frame is `Incomplete` (never
/// handed to `validate_frame`), a special shape is `Decline`.
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
        assert!(
            matches!(scan_flat(case, &config, None), FlatScan::Complete { .. }),
            "fast path declined {:?} — the differential would be vacuous",
            String::from_utf8_lossy(case)
        );
        // and it produces the frame, not just a scan
        let mut buf = BytesMut::from(case);
        assert!(matches!(
            parse(&mut buf, &config),
            Ok(Some(Frame::Array(_)))
        ));
        assert!(buf.is_empty());
    }

    let declines = [
        &b"*-1\r\n"[..],
        &b"*2\r\n$-1\r\n$1\r\nk\r\n"[..],
        &b"*2\r\n*1\r\n$1\r\na\r\n$1\r\nb\r\n"[..],
        &b"*2\r\n+OK\r\n$1\r\nb\r\n"[..],
        &b"*abc\r\n"[..],
    ];
    for case in declines {
        assert!(
            matches!(scan_flat(case, &config, None), FlatScan::Decline),
            "fast path accepted {:?}, which the two-pass path treats specially",
            String::from_utf8_lossy(case)
        );
    }

    let incompletes = [
        &b"*"[..],
        &b"*2"[..],
        &b"*2\r\n"[..],
        &b"*2\r\n$3\r\nGET\r\n"[..],
        &b"*2\r\n$3\r\nGET\r\n$1"[..],
        &b"*2\r\n$3\r\nGET\r\n$1\r\nk"[..],
    ];
    for case in incompletes {
        assert!(
            matches!(scan_flat(case, &config, None), FlatScan::Incomplete { .. }),
            "fast path did not call {:?} incomplete",
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

/// `*1048576\r\n` is ten bytes and `max_array_length` defaults to 1Mi. The
/// scanner must not size anything off that claim before the elements are
/// actually present: it records spans in 16 INLINE slots only and never
/// spills, so an incomplete scan allocates nothing at all, and the only
/// count-sized allocation (`FrameVec::with_capacity` in `build_flat`)
/// happens after the whole frame was walked — the buffer bounds it, as it
/// always did on the two-pass path.
#[test]
fn scan_never_sizes_off_the_claimed_count() {
    let config = ParseConfig::default();
    // The attack: a huge claimed count in a tiny buffer.
    match scan_flat(b"*1048576\r\n", &config, None) {
        FlatScan::Incomplete {
            cursor: Some(c), ..
        } => {
            assert_eq!(c.count, 1 << 20);
            assert_eq!(c.done, 0);
        }
        _ => panic!("a bare huge header must be an incomplete scan"),
    }
    // A long honest frame: spans stay inline however many elements.
    let mut frame = b"*40\r\n".to_vec();
    for i in 0..40 {
        frame.extend_from_slice(format!("$2\r\n{:02}\r\n", i).as_bytes());
    }
    match scan_flat(&frame, &config, None) {
        FlatScan::Complete { spans, count, .. } => {
            assert_eq!(count, 40);
            assert_eq!(spans.len(), crate::protocol::flat::INLINE_SPANS);
            assert!(!spans.spilled(), "span recording must never allocate");
        }
        _ => panic!("honest frame must complete"),
    }
    // and the elements past the inline spans are still built correctly
    let mut buf = BytesMut::from(&frame[..]);
    match parse(&mut buf, &config).unwrap().unwrap() {
        Frame::Array(items) => {
            assert_eq!(items.len(), 40);
            for (i, f) in items.iter().enumerate() {
                assert_eq!(f, &Frame::BulkString(Bytes::from(format!("{:02}", i))));
            }
        }
        other => panic!("expected Array, got {other:?}"),
    }
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
            encoding: *b"txt",
            data: Bytes::from_static(b"Some string"),
        }
    );
}

/// moon#1179 item 2: the inline `[u8; 3]` tag round-trips byte-for-byte
/// on the RESP3 wire for any encoding, and RESP2 still downgrades to the
/// bare payload.
#[test]
fn verbatim_encoding_tag_round_trips_byte_identical() {
    for wire in [
        &b"=15\r\ntxt:Some string\r\n"[..],
        &b"=11\r\nmkd:# hello\r\n"[..],
    ] {
        let frame = parse_bytes(wire).unwrap().unwrap();
        let mut out = BytesMut::new();
        crate::protocol::serialize_resp3(&frame, &mut out);
        assert_eq!(&out[..], wire);
    }
    let frame = parse_bytes(b"=11\r\nmkd:# hello\r\n").unwrap().unwrap();
    assert!(matches!(&frame, Frame::VerbatimString { encoding, .. } if encoding == b"mkd"));
    let mut out = BytesMut::new();
    crate::protocol::serialize(&frame, &mut out);
    assert_eq!(&out[..], b"$7\r\n# hello\r\n");
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
        126, 45, 49, 255, 58, 10, 49, 1, 0, 141, 13, 10, 36, 45, 49, 255, 58, 10, 48, 13, 49, 48,
        141, 13, 10, 36, 45, 49, 255, 58, 48, 13, 13, 10,
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
        0x2a, 0x33, 0x0d, 0x0a, 0x2a, 0x35, 0x0a, 0x0d, 0x0a, 0x5f, 0xfe, 0xff, 0xff, 0x0d, 0x0a,
        0x5f, 0x5f, 0x5f, 0x0a, 0x3a, 0x2a, 0x30, 0x0a, 0x0d, 0x0a, 0x5f, 0xfe, 0xff, 0xe9, 0x0d,
        0x0a, 0x5f, 0x5f, 0x5f, 0x0d, 0x0a, 0x5f, 0xfe, 0xff, 0xff, 0x0d, 0x0a, 0x5f, 0x5f, 0x5f,
        0x0a, 0x2a, 0x31, 0x0a, 0x0d, 0x0a, 0x5f, 0xfe, 0xff, 0xff, 0x0d, 0x0a, 0x5f, 0x5f, 0x0a,
        0x0d, 0x0a,
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
