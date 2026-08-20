#![no_main]
use libfuzzer_sys::fuzz_target;

use bytes::BytesMut;
use moon::protocol::{ParseConfig, inline, parse};

fn config() -> ParseConfig {
    ParseConfig {
        max_bulk_string_size: 64 * 1024,
        max_array_depth: 4,
        max_array_length: 256,
        max_inline_size: 64 * 1024,
    }
}

/// Drain `parse` until it stops yielding frames, asserting forward progress.
///
/// Every frame must consume at least one byte. That invariant is what catches
/// the livelock class this parser has produced twice: #487 (a token terminator
/// the skip loop would not skip, so `args` grew without the cursor moving) and
/// #578's fix (a blank-line loop in `parse`, which would spin if a zero-consume
/// `Ok(None)` ever fell through to `continue`).
fn drain(data: &[u8]) {
    let cfg = config();
    let mut buf = BytesMut::from(data);
    let mut frames = 0usize;
    loop {
        let before = buf.len();
        match parse(&mut buf, &cfg) {
            Ok(Some(_)) => {
                assert!(
                    buf.len() < before,
                    "parse yielded a frame without consuming bytes: len still {before}"
                );
                frames += 1;
                // Each frame eats >= 1 byte, so more frames than input bytes is
                // impossible without a cursor bug.
                assert!(
                    frames <= data.len(),
                    "{frames} frames from {} bytes",
                    data.len()
                );
            }
            Ok(None) | Err(_) => break,
        }
    }
}

// Fuzz the inline (telnet-style) command parser.
//
// Inline parsing is simpler than RESP but still operates on raw bytes: splits
// on a separator set, handles quoted strings and escapes, and finds line
// termination.
fuzz_target!(|data: &[u8]| {
    // 1. Single-shot, exactly as before — keeps the existing corpus meaningful.
    let mut buf = BytesMut::from(data);
    let _ = inline::parse_inline(&mut buf, 64 * 1024);

    // 2. The pipelined drain, where cursor bugs actually show up.
    drain(data);

    // 3. The same input with CRLF rewritten to bare LF (#381). Redis terminates
    //    an inline line on `\n` and strips at most one preceding `\r`, so every
    //    corpus entry doubles as an LF-only case for free — including the
    //    quoting and escape paths, which is where LF-vs-CRLF changes what the
    //    line even contains.
    if data.contains(&b'\r') {
        let lf_only: Vec<u8> = {
            let mut out = Vec::with_capacity(data.len());
            let mut i = 0;
            while i < data.len() {
                if data[i] == b'\r' && i + 1 < data.len() && data[i + 1] == b'\n' {
                    out.push(b'\n');
                    i += 2;
                } else {
                    out.push(data[i]);
                    i += 1;
                }
            }
            out
        };
        let mut buf = BytesMut::from(&lf_only[..]);
        let _ = inline::parse_inline(&mut buf, 64 * 1024);
        drain(&lf_only);
    }
});
