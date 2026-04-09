#![no_main]
use libfuzzer_sys::fuzz_target;

use bytes::BytesMut;
use moon::protocol::{ParseConfig, parse};

/// Fuzz the RESP2/RESP3 parser with bounded config to prevent OOM.
///
/// This target exercises the two-pass zero-copy parsing pipeline:
/// 1. validate_frame (structure check)
/// 2. parse_frame_zerocopy (Bytes::slice extraction)
///
/// Any panic, OOB access, or buffer overrun is a bug.
fuzz_target!(|data: &[u8]| {
    let config = ParseConfig {
        max_bulk_string_size: 64 * 1024, // 64 KB — enough to exercise bulk paths
        max_array_depth: 4,              // Prevent stack overflow from nesting
        max_array_length: 256,           // Bound allocation from large arrays
    };

    let mut buf = BytesMut::from(data);
    // Parse up to 16 pipelined frames from the same buffer
    for _ in 0..16 {
        if buf.is_empty() {
            break;
        }
        match parse::parse(&mut buf, &config) {
            Ok(Some(_frame)) => {
                // Parsed a valid frame — buffer advanced, try next
            }
            Ok(None) => break,    // Incomplete — need more data
            Err(_) => break,      // Protocol error — stop
        }
    }
});
