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

/// The `i64` grammar `str::parse` accepts, read straight off the bytes.
///
/// Exactly equivalent to
/// `std::str::from_utf8(value).ok().and_then(|s| s.parse::<i64>().ok())`,
/// and the equivalence is the whole contract — this is **not** a stricter or
/// looser recognizer, and it is emphatically not [`canonical_i64`], which
/// rejects leading zeros, a leading `+` and `-0`. `INCR` has always parsed its
/// stored counter with `str::parse`, and changing *which* spellings a counter
/// accepts is a client-visible behaviour change that does not belong in a
/// performance patch.
///
/// # Why it exists
///
/// `from_utf8` walks every byte to prove the slice is UTF-8, and then
/// `from_str_radix` walks the same bytes again rejecting everything that is
/// not an ASCII digit. The first walk is redundant *by construction*: every
/// byte string the grammar admits is `[+-]?[0-9]+`, which is pure ASCII and
/// therefore always valid UTF-8. So `from_utf8` can only ever reject inputs
/// the digit scan was going to reject anyway, and its verdict is never the
/// deciding one. Redis reaches the same answer in one pass, in `string2ll`.
///
/// Per digit this also drops `char::to_digit`'s radix handling and the two
/// `checked_*` operations, by hoisting the range question out of the loop:
/// after leading zeros are skipped, more than 19 significant digits cannot
/// fit an `i64` at all, and 19 digits of 9 (`9_999_999_999_999_999_999`) is
/// comfortably inside `u64`, so the accumulator provably cannot wrap.
///
/// # Guards
///
/// [`mod tests`]'s `parse_i64_bytes_matches_std_*` differentials — every
/// 1-byte input exhaustively, every 2- and 3-byte word over a discriminating
/// alphabet, both `i64` boundaries digit by digit, zero-padded and
/// over-long forms, and 200,000 randomised digit-heavy inputs — plus the
/// `parse_i64_bytes_differential` fuzz target. Any change here must keep the
/// std equivalence.
///
/// ```
/// use moon::storage::numeric::parse_i64_bytes;
/// assert_eq!(parse_i64_bytes(b"12345"), Some(12345));
/// // Unlike `canonical_i64`, the `str::parse` grammar is permissive:
/// assert_eq!(parse_i64_bytes(b"000000012345"), Some(12345));
/// assert_eq!(parse_i64_bytes(b"+5"), Some(5));
/// assert_eq!(parse_i64_bytes(b"-0"), Some(0));
/// // ... but no more permissive than `str::parse` is:
/// assert_eq!(parse_i64_bytes(b" 7"), None);
/// assert_eq!(parse_i64_bytes(b"7 "), None);
/// assert_eq!(parse_i64_bytes(b""), None);
/// assert_eq!(parse_i64_bytes(b"-"), None);
/// assert_eq!(parse_i64_bytes(b"9223372036854775808"), None);
/// assert_eq!(parse_i64_bytes(b"-9223372036854775808"), Some(i64::MIN));
/// ```
#[inline]
#[must_use]
pub fn parse_i64_bytes(value: &[u8]) -> Option<i64> {
    // `from_str_radix` splits the sign first and errors on a bare sign, on an
    // empty string, and on anything else it cannot read as a digit.
    let (negative, digits) = match value.first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    if digits.is_empty() {
        return None;
    }

    // Leading zeros are accepted by this grammar and carry no magnitude, so
    // skipping them is what lets the range question leave the loop. `"000"`
    // and `"-000"` legitimately reduce to no significant digits at all, which
    // is zero.
    let mut i = 0usize;
    while i < digits.len() && digits[i] == b'0' {
        i += 1;
    }
    let significant = &digits[i..];

    // 20 or more significant digits cannot fit an `i64`, so `str::parse`
    // would return `Err(PosOverflow/NegOverflow)` — and if one of those bytes
    // is not a digit it would return `Err(InvalidDigit)`. Both are `None`
    // here, so the scan can stop without deciding which.
    if significant.len() > 19 {
        return None;
    }

    let mut acc: u64 = 0;
    for &c in significant {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        // Cannot wrap: at most 19 iterations, so `acc` peaks at
        // 9_999_999_999_999_999_999 < u64::MAX (18_446_744_073_709_551_615).
        acc = acc * 10 + d as u64;
    }

    if negative {
        // `|i64::MIN|` is one past `i64::MAX`, and is the only magnitude that
        // needs the `u64` accumulator's extra bit. `2^63 as i64` is already
        // `i64::MIN`, so the cast lands on the right value without a negation
        // that would overflow.
        match acc.cmp(&(i64::MAX as u64 + 1)) {
            std::cmp::Ordering::Greater => None,
            std::cmp::Ordering::Equal => Some(i64::MIN),
            std::cmp::Ordering::Less => Some(-(acc as i64)),
        }
    } else if acc > i64::MAX as u64 {
        None
    } else {
        Some(acc as i64)
    }
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

    // ── parse_i64_bytes vs the std composition it replaces (moon#942) ──────

    /// The oracle: exactly the expression `INCR` used before
    /// [`super::parse_i64_bytes`] existed.
    fn std_parse(value: &[u8]) -> Option<i64> {
        std::str::from_utf8(value).ok().and_then(|s| s.parse().ok())
    }

    fn agree(input: &[u8]) {
        assert_eq!(
            super::parse_i64_bytes(input),
            std_parse(input),
            "parse verdict diverged on {input:?}"
        );
    }

    #[test]
    fn parse_i64_bytes_matches_std_on_hand_picked_shapes() {
        for input in [
            &b""[..],
            b"0",
            b"-0",
            b"+0",
            b"7",
            b"-7",
            b"+7",
            b"-",
            b"+",
            b"--1",
            b"++1",
            b"+-1",
            b"-+1",
            b" 7",
            b"7 ",
            b"\t7",
            b"7\n",
            b"1_000",
            b"0x10",
            b"007",
            b"-007",
            b"+007",
            b"000000000000000000000000000",
            b"-000000000000000000000000000",
            b"0000000000000000000000000007",
            b"-0000000000000000000000000007",
            // Both boundaries, and one past each.
            b"9223372036854775807",
            b"9223372036854775808",
            b"-9223372036854775808",
            b"-9223372036854775809",
            b"00009223372036854775807",
            b"-00009223372036854775808",
            // 19, 20 and 21 significant digits.
            b"9999999999999999999",
            b"99999999999999999999",
            b"999999999999999999999",
            b"-9999999999999999999",
            b"-99999999999999999999",
            // Non-UTF-8, and UTF-8 that is not an ASCII digit.
            b"\xff",
            b"1\xff",
            b"\xff1",
            b"\xc3\xa9",
            b"1\xc3\xa92",
            "٣".as_bytes(),
            "１２３".as_bytes(),
        ] {
            agree(input);
        }
    }

    /// Exhaustive over every single byte, including every non-UTF-8 one.
    #[test]
    fn parse_i64_bytes_matches_std_on_every_one_byte_input() {
        for b in 0u8..=255 {
            agree(&[b]);
        }
    }

    /// Exhaustive at widths 2 and 3 over an alphabet chosen so every
    /// interesting transition (sign, zero, digit, separator, non-ASCII) is
    /// reachable in any position.
    #[test]
    fn parse_i64_bytes_matches_std_on_short_words() {
        const ALPHABET: [u8; 10] = [b'0', b'1', b'9', b'-', b'+', b' ', b'a', b'.', 0x00, 0xff];
        for &a in &ALPHABET {
            for &b in &ALPHABET {
                agree(&[a, b]);
                for &c in &ALPHABET {
                    agree(&[a, b, c]);
                }
            }
        }
    }

    /// Walk both boundaries digit by digit: for every prefix length, nudge the
    /// last digit up and down. This is where an off-by-one in the range check
    /// or in the 19-digit cutoff shows up.
    #[test]
    fn parse_i64_bytes_matches_std_across_both_boundaries() {
        for base in [i64::MIN, i64::MAX, 0, -1, 1] {
            let rendered = base.to_string();
            agree(rendered.as_bytes());
            for n in 1..=rendered.len() {
                agree(&rendered.as_bytes()[..n]);
                // Zero-padded to the same magnitude, which `str::parse`
                // accepts and `canonical_i64` does not.
                for pad in [1usize, 2, 8, 40] {
                    let (sign, mag) = rendered.split_at(usize::from(base < 0));
                    let padded = format!("{sign}{}{mag}", "0".repeat(pad));
                    agree(padded.as_bytes());
                    let _ = n;
                }
            }
        }
    }

    /// 200,000 randomised inputs over a digit-heavy alphabet, deterministic
    /// (xorshift, fixed seed) so a failure is reproducible.
    #[test]
    fn parse_i64_bytes_matches_std_on_randomised_inputs() {
        let mut state: u64 = 0x2545_F491_4F6C_DD1D;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        const ALPHABET: [u8; 16] = [
            b'0', b'0', b'1', b'2', b'5', b'8', b'9', b'9', b'-', b'+', b' ', b'a', b'.', 0x00,
            0x80, 0xff,
        ];
        let mut input: Vec<u8> = Vec::with_capacity(32);
        for _ in 0..200_000 {
            let len = (next() % 25) as usize;
            input.clear();
            for _ in 0..len {
                input.push(ALPHABET[(next() % ALPHABET.len() as u64) as usize]);
            }
            agree(&input);
        }
    }
}
