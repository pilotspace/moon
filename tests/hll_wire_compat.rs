//! HLL wire compatibility tests — byte-for-byte DUMP equality vs redis-server 7.x.
//!
//! These tests spawn a moon server and a real redis-server, perform identical
//! PFADD sequences on both, then compare DUMP output byte-for-byte.
//!
//! Gated on Linux + redis-server availability (not available in all CI envs).

// TODO(task3): uncomment test bodies after command dispatch lands
// Tests require both moon server and redis-server on PATH.

#[test]
#[ignore] // Enable after Task 3 wires PFADD/PFCOUNT/PFMERGE dispatch
fn hll_wire_compat_small_sequence() {
    // PFADD key a b c on both moon and redis-server
    // DUMP key on both
    // assert_eq!(moon_dump, redis_dump)
}

#[test]
#[ignore] // Enable after Task 3 wires PFADD/PFCOUNT/PFMERGE dispatch
fn hll_wire_compat_medium_sequence() {
    // PFADD key x1..x100 on both
    // DUMP key on both
    // assert_eq!(moon_dump, redis_dump)
}

#[test]
#[ignore] // Enable after Task 3 wires PFADD/PFCOUNT/PFMERGE dispatch
fn hll_wire_compat_large_sequence() {
    // PFADD key x1..x10000 on both
    // DUMP key on both
    // assert_eq!(moon_dump, redis_dump)
}
