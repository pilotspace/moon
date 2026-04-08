#![no_main]
use libfuzzer_sys::fuzz_target;

use bytes::BytesMut;
use moon::protocol::{ParseConfig, parse};

/// Differential fuzz: parse the same input twice from identical buffers.
///
/// Invariant: both parses must produce the same result (Ok/Err) and
/// consume the same number of bytes. Any divergence indicates a
/// non-determinism or state-dependent bug in the two-pass pipeline.
fuzz_target!(|data: &[u8]| {
    let config = ParseConfig {
        max_bulk_string_size: 64 * 1024,
        max_array_depth: 4,
        max_array_length: 256,
    };

    let mut buf1 = BytesMut::from(data);
    let mut buf2 = BytesMut::from(data);

    let result1 = parse::parse(&mut buf1, &config);
    let result2 = parse::parse(&mut buf2, &config);

    // Both must agree on success/failure/incomplete
    match (&result1, &result2) {
        (Ok(None), Ok(None)) => {}
        (Err(_), Err(_)) => {}
        (Ok(Some(_)), Ok(Some(_))) => {
            // Both parsed successfully — must consume same byte count
            assert_eq!(
                buf1.len(),
                buf2.len(),
                "differential: same input produced different buffer advancement"
            );
        }
        _ => {
            panic!(
                "differential: divergent results from identical input\n  r1: {:?}\n  r2: {:?}",
                result1.is_ok(),
                result2.is_ok()
            );
        }
    }
});
