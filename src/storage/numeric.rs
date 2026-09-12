//! Canonical integer recognition for value encodings.
//!
//! Redis stores a string in an integer encoding only when the decimal
//! rendering of the parsed value reproduces the original bytes exactly.
//! `"12345"` qualifies; `"000000012345"`, `"+5"`, `"-0"` and `" 7"` do not —
//! they stay strings, and `OBJECT ENCODING` reports `embstr`, not `int`.
//!
//! A bare `parse::<i64>()` accepts all of those and yields a value whose
//! rendering differs from the input. Anything that then *stores* the parsed
//! integer has silently destroyed the caller's bytes: `SADD s 000000012345`
//! followed by `SMEMBERS` returns `12345`. Anything that uses it to *look up*
//! a member reports a false positive: `SISMEMBER s 000000012345` matches a
//! stored `12345`.
//!
//! Every site that maps bytes to an integer encoding — or reports one — must
//! go through [`canonical_i64`].

/// Parse `value` as an i64 **only if** rendering that i64 reproduces `value`
/// byte for byte.
///
/// Returns `None` for any non-canonical form, so the caller keeps the original
/// bytes as a string.
///
/// ```
/// use moon::storage::numeric::canonical_i64;
/// assert_eq!(canonical_i64(b"12345"), Some(12345));
/// assert_eq!(canonical_i64(b"-42"), Some(-42));
/// assert_eq!(canonical_i64(b"0"), Some(0));
/// // Non-canonical: the bytes must be preserved as a string.
/// assert_eq!(canonical_i64(b"000000012345"), None);
/// assert_eq!(canonical_i64(b"+5"), None);
/// assert_eq!(canonical_i64(b"-0"), None);
/// assert_eq!(canonical_i64(b" 7"), None);
/// assert_eq!(canonical_i64(b""), None);
/// ```
#[inline]
pub fn canonical_i64(value: &[u8]) -> Option<i64> {
    // i64::MIN is 20 bytes ("-9223372036854775808"); nothing longer can be
    // canonical, and the length check keeps the UTF-8 validation bounded.
    if value.is_empty() || value.len() > 20 {
        return None;
    }
    let s = std::str::from_utf8(value).ok()?;
    let v: i64 = s.parse().ok()?;
    // The round trip is the whole point: it rejects leading zeros, a leading
    // '+', "-0", and any surrounding whitespace that `parse` would tolerate.
    let mut buf = itoa::Buffer::new();
    if buf.format(v).as_bytes() == value {
        Some(v)
    } else {
        None
    }
}

/// `true` exactly when [`canonical_i64`] would return `Some` — the same
/// verdict, without producing the value.
///
/// Callers that only need to *route* (does this batch belong in an intset?
/// does this value belong in an integer encoding?) pay `canonical_i64`'s full
/// round trip — a UTF-8 validation, an `i64` parse, an `itoa` render and a
/// `memcmp` — for a number they then throw away. This decides the same
/// question from the bytes alone.
///
/// The equivalence is the whole contract, and it is pinned by differential
/// tests against `canonical_i64` rather than by hand-written expectations:
/// exhaustively over every 1-byte input and over a discriminating alphabet at
/// widths 2 and 3, across both `i64` boundaries digit by digit, and over
/// 200,000 randomised digit-heavy inputs. **Any change here must keep that
/// equivalence** — a verdict that drifts silently re-opens moon#795, where a
/// non-canonical spelling entering an integer encoding destroyed the caller's
/// bytes (`SADD s 000000012345` came back as `12345`).
///
/// ```
/// use moon::storage::numeric::{canonical_i64, is_canonical_i64};
/// for input in [&b"12345"[..], b"-42", b"0", b"007", b"+7", b"-0", b" 7", b""] {
///     assert_eq!(is_canonical_i64(input), canonical_i64(input).is_some());
/// }
/// ```
#[inline]
#[must_use]
pub fn is_canonical_i64(value: &[u8]) -> bool {
    // A leading '-' is the only sign a canonical rendering carries; a leading
    // '+' survives to the digit test below and fails it, exactly as
    // `canonical_i64`'s round trip rejects it.
    let (negative, digits) = match value.first() {
        Some(b'-') => (true, &value[1..]),
        _ => (false, value),
    };
    // At least one digit, and never more than the 19 magnitude digits of
    // `i64::MIN`. This subsumes `canonical_i64`'s `len() > 20` guard.
    if digits.is_empty() || digits.len() > 19 {
        return false;
    }
    // `itoa` emits a leading '0' for one value only: zero itself, spelled
    // `"0"`. So any other rendering starting with '0' is padded and
    // non-canonical, and `"-0"` is non-canonical because zero renders unsigned.
    // Checked before the digit scan because it is the single most common
    // reject in practice (zero-padded IDs) and settles the answer in two
    // loads.
    if digits[0] == b'0' {
        return digits.len() == 1 && !negative;
    }
    // ASCII-digit-only also settles UTF-8 validity: every accepted byte is
    // single-byte UTF-8, so `canonical_i64`'s `from_utf8` can never reject an
    // input this admits.
    if !digits.iter().all(u8::is_ascii_digit) {
        return false;
    }
    // At 19 digits with no leading zero, acceptance stops being a syntax
    // question and becomes a range one. Equal length and no leading zero make
    // the lexicographic byte order the numeric order, so one slice compare
    // settles it. Below 19 digits every value fits an i64.
    if digits.len() == 19 {
        let limit: &[u8] = if negative {
            b"9223372036854775808" // |i64::MIN|
        } else {
            b"9223372036854775807" // i64::MAX
        };
        return digits <= limit;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::canonical_i64;

    #[test]
    fn accepts_canonical_forms() {
        for (input, want) in [
            (&b"0"[..], 0i64),
            (b"7", 7),
            (b"12345", 12345),
            (b"-1", -1),
            (b"-9223372036854775808", i64::MIN),
            (b"9223372036854775807", i64::MAX),
        ] {
            assert_eq!(canonical_i64(input), Some(want), "input {input:?}");
        }
    }

    /// Every one of these is accepted by a bare `parse::<i64>()` and renders
    /// back differently — the exact inputs that corrupted stored values.
    #[test]
    fn rejects_non_canonical_forms() {
        for input in [
            &b"000000012345"[..], // leading zeros (zero-padded IDs)
            b"007",
            b"00",
            b"+5", // leading plus
            b"-0", // negative zero renders as "0"
            b" 7", // parse tolerates neither, but be explicit
            b"7 ",
            b"",
            b"9223372036854775808",  // i64::MAX + 1, overflows
            b"-9223372036854775809", // i64::MIN - 1
            b"1e5",
            b"0x10",
            b"1_000",
        ] {
            assert_eq!(
                canonical_i64(input),
                None,
                "input {input:?} must stay a string"
            );
        }
    }

    /// The property that matters: whenever we DO encode as an integer, the
    /// rendering must be byte-identical to what the caller gave us.
    #[test]
    fn accepted_inputs_always_round_trip() {
        for i in -1000i64..1000 {
            let s = i.to_string();
            let got = canonical_i64(s.as_bytes());
            assert_eq!(got, Some(i));
            assert_eq!(got.unwrap().to_string(), s);
        }
    }

    /// A canonical value with any number of leading zeros prepended must be
    /// rejected, for every width.
    #[test]
    fn rejects_every_zero_padding_width() {
        for pad in 1..=8usize {
            let s = format!("{}{}", "0".repeat(pad), 12345);
            assert_eq!(canonical_i64(s.as_bytes()), None, "padding {pad}: {s}");
        }
    }

    // ── `is_canonical_i64` — the routing twin ───────────────────────────
    //
    // It exists to answer the ROUTING question ("would this member fit an
    // intset?") without paying for the value nobody is asking for. Its whole
    // contract is that it is indistinguishable from `canonical_i64(..).is_some()`
    // — so every test here is DIFFERENTIAL against that oracle, never against
    // a hand-written expectation. A hand-written expectation could encode the
    // same mistake twice; the oracle cannot.

    /// The named moon#795 inputs: leading zeros, a leading `+`, `-0`,
    /// surrounding whitespace, the empty string, an over-long number, and the
    /// i64 boundaries on both sides. Each was a real data-corruption vector.
    #[test]
    fn routing_twin_agrees_on_every_byte_transparency_case() {
        for input in [
            &b"007"[..],
            b"+7",
            b"-0",
            b" 7",
            b"7 ",
            b"",
            b"12345678901234567890", // 20 digits, overflows i64
            b"9223372036854775807",  // i64::MAX
            b"9223372036854775808",  // i64::MAX + 1
            b"-9223372036854775808", // i64::MIN
            b"-9223372036854775809", // i64::MIN - 1
            b"000000012345",         // redis-benchmark's __rand_int__ shape
            b"0",
            b"-1",
            b"-",
            b"+",
            b"--1",
            b"1e5",
            b"0x10",
            b"1_000",
            b"\xff",     // not UTF-8 at all
            b"\xd9\xa1", // Arabic-Indic ONE: valid UTF-8, not a digit
        ] {
            assert_eq!(
                super::is_canonical_i64(input),
                canonical_i64(input).is_some(),
                "routing verdict diverged on {input:?}"
            );
        }
    }

    /// Exhaustive over every 1-byte input, and over every 2- and 3-byte input
    /// drawn from the alphabet that actually discriminates. 8^3 + 8^2 + 256
    /// cases, each checked against the oracle.
    #[test]
    fn routing_twin_agrees_exhaustively_on_short_inputs() {
        for b in 0u8..=255 {
            let input = [b];
            assert_eq!(
                super::is_canonical_i64(&input),
                canonical_i64(&input).is_some(),
                "routing verdict diverged on {input:?}"
            );
        }
        const ALPHABET: [u8; 8] = [b'0', b'1', b'9', b'-', b'+', b' ', b'a', 0x00];
        for a in ALPHABET {
            for b in ALPHABET {
                let two = [a, b];
                assert_eq!(
                    super::is_canonical_i64(&two),
                    canonical_i64(&two).is_some(),
                    "routing verdict diverged on {two:?}"
                );
                for c in ALPHABET {
                    let three = [a, b, c];
                    assert_eq!(
                        super::is_canonical_i64(&three),
                        canonical_i64(&three).is_some(),
                        "routing verdict diverged on {three:?}"
                    );
                }
            }
        }
    }

    /// The width where the two implementations could most plausibly part
    /// company: 19 magnitude digits, where acceptance stops being a syntax
    /// question and becomes a range question. Walk the last digit of both
    /// boundaries, and every zero-padded and sign-prefixed variant of a value
    /// that IS canonical.
    #[test]
    fn routing_twin_agrees_at_the_i64_boundaries_and_around_them() {
        let mut cases: Vec<Vec<u8>> = Vec::new();
        for v in [i64::MIN, i64::MIN + 1, i64::MAX - 1, i64::MAX, 0, -1, 1] {
            let s = v.to_string();
            cases.push(s.clone().into_bytes());
            cases.push(format!("+{s}").into_bytes());
            cases.push(format!("0{s}").into_bytes());
            cases.push(format!("00{s}").into_bytes());
            cases.push(format!(" {s}").into_bytes());
            cases.push(format!("{s} ").into_bytes());
            cases.push(format!("-{s}").into_bytes());
        }
        // Walk the final digit across both 19-digit magnitude boundaries, so
        // the in-range/out-of-range flip is crossed in both directions.
        for last in b'0'..=b'9' {
            for stem in ["922337203685477580", "-922337203685477580"] {
                let mut s = stem.as_bytes().to_vec();
                s.push(last);
                cases.push(s);
            }
        }
        // 20 magnitude digits is unconditionally out of range.
        cases.push(b"9999999999999999999".to_vec());
        cases.push(b"10000000000000000000".to_vec());
        cases.push(b"-10000000000000000000".to_vec());
        for input in &cases {
            assert_eq!(
                super::is_canonical_i64(input),
                canonical_i64(input).is_some(),
                "routing verdict diverged on {:?}",
                String::from_utf8_lossy(input)
            );
        }
    }

    /// Randomised differential sweep over digit-heavy noise, at every length
    /// that spans the accept/reject boundary. Deterministic seed so a failure
    /// is reproducible.
    #[test]
    fn routing_twin_agrees_on_randomised_digit_noise() {
        // xorshift64*, inlined so the test owns its own determinism.
        let mut state: u64 = 0x2545_F491_4F6C_DD1D;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        const ALPHABET: [u8; 16] = [
            b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9', b'-', b'+', b' ', b'a',
            b'.', 0x00,
        ];
        let mut input: Vec<u8> = Vec::with_capacity(22);
        for _ in 0..200_000 {
            let len = (next() % 23) as usize;
            input.clear();
            for _ in 0..len {
                input.push(ALPHABET[(next() % ALPHABET.len() as u64) as usize]);
            }
            assert_eq!(
                super::is_canonical_i64(&input),
                canonical_i64(&input).is_some(),
                "routing verdict diverged on {:?}",
                String::from_utf8_lossy(&input)
            );
        }
    }
}
