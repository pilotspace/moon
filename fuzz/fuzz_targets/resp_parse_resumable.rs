#![no_main]
use libfuzzer_sys::fuzz_target;

use bytes::BytesMut;
use moon::protocol::{Frame, ParseConfig, ParseError, ParseState, parse};

/// Differential for the RESUMABLE parser (moon#1164): the same bytes, delivered
/// in fuzzer-chosen chunks to one `ParseState`, must produce exactly what the
/// two-pass reference produces on an identical buffer — at EVERY read, frame,
/// error kind and bytes consumed.
///
/// The first input byte seeds the chunking, so libFuzzer explores split points
/// as well as contents. `ParseState::eager()` keeps the cursor from the first
/// validated element on, so every resume path is reachable with small inputs
/// (the production thresholds would hide it below 16 KiB).
///
/// What this guards: the cursor may never change an answer. A resumed scan
/// that disagreed with a from-zero scan would show up as a divergence here —
/// most dangerously as `Ok(None)` ("need more") for a buffer the reference
/// parses, which on a live connection is a hang.
fn configs() -> [ParseConfig; 2] {
    [
        ParseConfig {
            max_bulk_string_size: 64 * 1024,
            max_array_depth: 4,
            max_array_length: 256,
            max_inline_size: 64 * 1024,
        },
        ParseConfig {
            max_bulk_string_size: 8,
            max_array_depth: 1,
            max_array_length: 3,
            max_inline_size: 32,
        },
    ]
}

fn compare(
    fast: &Result<Option<Frame>, ParseError>,
    slow: &Result<Option<Frame>, ParseError>,
    fast_buf: &BytesMut,
    slow_buf: &BytesMut,
) {
    match (fast, slow) {
        (Ok(a), Ok(b)) => assert_eq!(a, b, "resumable parse produced a different frame"),
        (Err(a), Err(b)) => assert_eq!(
            a.to_string(),
            b.to_string(),
            "resumable parse produced a different error"
        ),
        _ => panic!(
            "resumable parse diverged on ok/err: fast_ok={} slow_ok={}",
            fast.is_ok(),
            slow.is_ok()
        ),
    }
    assert_eq!(
        fast_buf.as_ref(),
        slow_buf.as_ref(),
        "resumable parse consumed a different number of bytes"
    );
}

fuzz_target!(|data: &[u8]| {
    let Some((&seed, input)) = data.split_first() else {
        return;
    };
    for config in configs() {
        let mut state = ParseState::eager();
        let mut fast_buf = BytesMut::new();
        let mut slow_buf = BytesMut::new();
        let mut at = 0usize;
        let mut rng = seed as u32 | 1;
        'feed: while at < input.len() {
            // xorshift32 chunk sizes 1..=64
            rng ^= rng << 13;
            rng ^= rng >> 17;
            rng ^= rng << 5;
            let n = (1 + (rng % 64) as usize).min(input.len() - at);
            fast_buf.extend_from_slice(&input[at..at + n]);
            slow_buf.extend_from_slice(&input[at..at + n]);
            at += n;
            for _ in 0..32 {
                let fast = moon::protocol::parse_resumable(&mut fast_buf, &config, &mut state);
                let slow = parse::parse_reference_two_pass(&mut slow_buf, &config);
                compare(&fast, &slow, &fast_buf, &slow_buf);
                match fast {
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(_) => break 'feed,
                }
            }
        }
    }
});
