#![no_main]
use libfuzzer_sys::fuzz_target;

use moon::storage::numeric::{canonical_i64, is_canonical_i64};

/// Differential fuzz: the routing recognizer vs the parsing oracle.
///
/// `canonical_i64` decides "is this the exact decimal rendering of an i64?"
/// by DOING the work — UTF-8 validation, an `i64` parse, an `itoa` render, a
/// `memcmp`. `is_canonical_i64` decides the same question from the bytes
/// alone, for the callers that only need to route (SADD's `all_integers`
/// pre-pass is the first). They are two hand-written recognizers of one
/// grammar, and the only thing keeping them honest is that they agree.
///
/// A divergence is not cosmetic. moon#795 was a real data-corruption bug:
/// a non-canonical spelling that reaches an integer encoding destroys the
/// caller's bytes (`SADD s 000000012345` came back as `12345`) and makes a
/// re-rendered integer answer for a member nobody added. If the cheap
/// recognizer ever says `true` where the oracle says `None`, exactly that
/// re-opens — silently, on the hot path, for whichever spelling diverged.
///
/// Invariant, needing no external oracle: for EVERY byte string,
/// `is_canonical_i64(d) == canonical_i64(d).is_some()`. When both accept, the
/// value must additionally render back to the input, which is the property
/// moon#795 is really about.
fuzz_target!(|data: &[u8]| {
    let cheap = is_canonical_i64(data);
    let full = canonical_i64(data);

    assert_eq!(
        cheap,
        full.is_some(),
        "routing verdict diverged from the oracle for {data:?}"
    );

    // Whenever the verdict is "yes", the accepted value MUST render back to
    // the caller's exact bytes. This is the moon#795 property itself, so the
    // target guards the consequence and not only the agreement.
    if let Some(v) = full {
        assert_eq!(
            v.to_string().as_bytes(),
            data,
            "accepted {data:?} but it does not round-trip"
        );
    }

    // The recognizer must never be affected by what follows the slice, so a
    // suffix-extended copy is re-checked: a length-confused implementation
    // (one reading past its bound, or keying off a NUL) would part company
    // with the oracle here even when it agrees on the bare input.
    if data.len() < 64 {
        let mut extended = data.to_vec();
        extended.push(b'0');
        assert_eq!(
            is_canonical_i64(&extended),
            canonical_i64(&extended).is_some(),
            "routing verdict diverged from the oracle for {extended:?}"
        );
    }
});
