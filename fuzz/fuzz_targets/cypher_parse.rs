#![no_main]
//! Fuzz target for the Cypher parser.
//!
//! Feeds random bytes to `parse_cypher()` — must never panic.
//! Errors are expected (most random input is invalid Cypher); the goal
//! is to verify no panics, no stack overflows, no OOB reads.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // The parser must handle any input gracefully — errors are fine, panics are not.
    let _ = moon::graph::cypher::parse_cypher(data);

    // Also test with a small nesting depth limit to exercise the depth guard.
    let mut parser = moon::graph::cypher::Parser::new(data, 4);
    let _ = parser.parse();
});
