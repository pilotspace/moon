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
}
