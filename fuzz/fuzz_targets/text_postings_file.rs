#![no_main]
use libfuzzer_sys::fuzz_target;

use moon::text::postings_persist::{decode, trailer_checksum};

/// Fuzz the `.tpost` text-postings decoder (`docs/internal/text-postings-persistence.md`).
///
/// Exercises header/version/length framing, the trailer checksum, every
/// per-section count (docs, terms, postings, positions, TAG/NUMERIC
/// entries) and the structural invariants the store relies on. Any panic,
/// OOB access or unbounded allocation is a bug: a torn, bit-rotted or
/// hostile file must always fail closed with `Err`, never panic, and
/// never hand back a partially populated index.
///
/// Most random inputs die at the checksum, so the target also re-stamps
/// the trailer over the input to reach the structural checks — that is the
/// half a corrupt-but-checksummed file (a buggy writer) would hit.
fuzz_target!(|data: &[u8]| {
    let _ = decode(data);
    if data.len() >= 32 {
        let mut restamped = data.to_vec();
        let n = restamped.len();
        // Make the header claim the real payload length so the length
        // check passes and the checksum is recomputed over the body.
        let payload_len = (n - 24 - 8) as u64;
        restamped[16..24].copy_from_slice(&payload_len.to_le_bytes());
        let sum = trailer_checksum(&restamped[..n - 8]);
        restamped[n - 8..].copy_from_slice(&sum.to_le_bytes());
        let _ = decode(&restamped);
    }
});
