#![no_main]
use libfuzzer_sys::fuzz_target;

use bytes::BytesMut;
use moon::protocol::{Frame, ParseConfig, ParseError, parse};

/// True differential: the fused single-pass multibulk path vs the two-pass path
/// it replaces.
///
/// `parse` scans a flat top-level `*N` of `$`-bulks in ONE pass, recording each
/// argument's span while validating, instead of walking the bytes twice
/// (`validate_frame` computing every offset and discarding them, then
/// `parse_frame_zerocopy` re-deriving them). `parse_reference_two_pass` is the
/// pipeline exactly as it stood before, exported under the `fuzzing` feature.
///
/// The contract is total: for ANY input — valid, truncated, malformed, hostile —
/// the two must agree on the frame, on `Ok`/`Err`, on the error KIND, and on how
/// many bytes were consumed. A fast path that merely declines more often is
/// fine; one that answers where the reference answers differently is a bug.
///
/// Distinct from `resp_parse_differential`, which runs `parse` against ITSELF
/// and can only find non-determinism.
fn configs() -> [ParseConfig; 3] {
    [
        ParseConfig {
            max_bulk_string_size: 64 * 1024,
            max_array_depth: 4,
            max_array_length: 256,
            max_inline_size: 64 * 1024,
        },
        // Tight bounds: the limits are where a fast path most easily diverges.
        ParseConfig {
            max_bulk_string_size: 8,
            max_array_depth: 1,
            max_array_length: 3,
            max_inline_size: 32,
        },
        // Depth 0 forbids the ELEMENTS of a top-level array.
        ParseConfig {
            max_bulk_string_size: 1024,
            max_array_depth: 0,
            max_array_length: 16,
            max_inline_size: 1024,
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
        (Ok(a), Ok(b)) => assert_eq!(a, b, "fused parse produced a different frame"),
        // `Display` carries the wire fault name, the internal message and the
        // byte offset — the strongest comparison available without PartialEq.
        (Err(a), Err(b)) => assert_eq!(
            a.to_string(),
            b.to_string(),
            "fused parse produced a different error"
        ),
        _ => panic!(
            "fused parse diverged on ok/err: fast_ok={} slow_ok={}",
            fast.is_ok(),
            slow.is_ok()
        ),
    }
    assert_eq!(
        fast_buf.as_ref(),
        slow_buf.as_ref(),
        "fused parse consumed a different number of bytes"
    );
}

fuzz_target!(|data: &[u8]| {
    for config in configs() {
        let mut fast_buf = BytesMut::from(data);
        let mut slow_buf = BytesMut::from(data);

        // Drain up to 16 pipelined frames, comparing every step. A divergence
        // that only appears on the second frame of a pipeline is exactly the
        // kind a single-frame target would miss.
        for _ in 0..16 {
            if fast_buf.is_empty() && slow_buf.is_empty() {
                break;
            }
            let fast = parse::parse(&mut fast_buf, &config);
            let slow = parse::parse_reference_two_pass(&mut slow_buf, &config);
            compare(&fast, &slow, &fast_buf, &slow_buf);
            match fast {
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => break,
            }
        }
    }
});
