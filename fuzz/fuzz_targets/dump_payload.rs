#![no_main]
use libfuzzer_sys::fuzz_target;

use moon::persistence::dump_payload;

// Fuzz the `DUMP`/`RESTORE` payload decoder (moon#636).
//
// This decoder is reachable from an UNAUTHENTICATED-adjacent surface: any
// client that may run `RESTORE` hands it arbitrary bytes, and the payload is
// attacker-chosen in full — type byte, length prefixes, element counts and
// the CRC trailer alike. A panic here is a remote DoS.
//
// The interesting shapes are the ones a checksum does not protect against.
// A payload can carry a perfectly valid CRC over a body that claims a
// four-billion-element list, so the length-driven allocation guards inside
// `read_rdb_entry` — not the trailer — are what has to hold. Also exercised:
// payloads shorter than the trailer, a length prefix that runs off the end,
// version bytes above this server's own, and type tags moon has no decoder
// for (redis 8's listpack forms), which must be refused rather than
// mis-parsed.
//
// Any outcome except a panic is acceptable: `Ok` for a payload that happens
// to be well-formed, `Err` for everything else.
fuzz_target!(|data: &[u8]| {
    let _ = dump_payload::decode(data);
});
