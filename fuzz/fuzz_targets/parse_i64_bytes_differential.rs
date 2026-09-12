#![no_main]
use libfuzzer_sys::fuzz_target;

use moon::storage::numeric::parse_i64_bytes;

/// Differential fuzz: `INCR`'s byte-level integer parser vs the std
/// composition it replaced.
///
/// `parse_i64_bytes` reads the `[+-]?[0-9]+` grammar straight off the bytes so
/// that `INCR`/`INCRBY`/`DECR`/`DECRBY` stop walking their stored counter
/// twice — once to prove it is UTF-8, once to reject everything that is not an
/// ASCII digit (moon#942). The redundancy argument is sound on paper: every
/// byte string the grammar admits is pure ASCII, so `from_utf8` never casts
/// the deciding vote. This target is what keeps it sound in practice.
///
/// The invariant needs no external oracle, because the oracle IS std:
///
/// ```text
/// parse_i64_bytes(d) == from_utf8(d).ok().and_then(|s| s.parse::<i64>().ok())
/// ```
///
/// A divergence is client-visible in both directions. Accepting something std
/// rejects makes `INCR` succeed on a counter Redis calls "not an integer" —
/// and then *stores* the parsed value, destroying the caller's bytes. Rejecting
/// something std accepts turns a working counter into a permanent error reply
/// for whatever spelling diverged (`"+5"`, `"007"`, `"-0"` and
/// `"-9223372036854775808"` are the shapes that have historically been got
/// wrong by hand-rolled versions of this grammar).
fuzz_target!(|data: &[u8]| {
    let oracle: Option<i64> = std::str::from_utf8(data)
        .ok()
        .and_then(|s| s.parse::<i64>().ok());

    assert_eq!(
        parse_i64_bytes(data),
        oracle,
        "parse verdict diverged for {data:?}"
    );

    // A length-confused implementation — one reading past its bound, or
    // keying off a NUL — can agree on the bare input and part company the
    // moment the slice is extended. Re-check with one more digit appended,
    // which is also how a 19-digit accept becomes a 20-digit overflow.
    if data.len() < 64 {
        let mut extended = data.to_vec();
        extended.push(b'7');
        let oracle_ext: Option<i64> = std::str::from_utf8(&extended)
            .ok()
            .and_then(|s| s.parse::<i64>().ok());
        assert_eq!(
            parse_i64_bytes(&extended),
            oracle_ext,
            "parse verdict diverged for {extended:?}"
        );
    }
});
