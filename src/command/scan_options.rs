//! One option parser for the whole SCAN family.
//!
//! `SCAN`, `HSCAN`, `SSCAN` and `ZSCAN` share an option grammar, and Moon had
//! **eight** hand-copied parsers for it — a `_readonly` twin of each command on
//! top of the four. They had already drifted apart: `ZSCAN` refused a
//! non-numeric `COUNT` while the other three silently kept the default, and
//! `ZSCAN` refused a dangling `MATCH` while the others ignored it. None of them
//! refused a token they did not recognise.
//!
//! That last one is not a cosmetic gap. `HSCAN ... NOVALUES` was accepted and
//! DROPPED, so a client that asked for field names got the field/value
//! interleave and read every value as a field name — a well-formed reply full
//! of names that do not exist (moon#630). The option was never "unimplemented":
//! it was silently agreed to. Any option added later inherits that, which is
//! why the fix is one exhaustive parser and not four more `else if` arms.
//!
//! Every error string below is transcribed from a live redis-server 8.6.1
//! sweep on 2026-08-22, including the ones that are surprising:
//!
//! ```text
//!   MATCH with no argument          ERR syntax error
//!   COUNT with no argument          ERR syntax error
//!   COUNT abc                       ERR value is not an integer or out of range
//!   COUNT 0  /  COUNT -1            ERR syntax error   (not the integer error)
//!   TYPE on HSCAN/SSCAN/ZSCAN       ERR syntax error
//!   NOVALUES on SCAN/SSCAN/ZSCAN    ERR NOVALUES option can only be used in HSCAN
//!   any unknown token               ERR syntax error
//!   TYPE nosuchtype                 accepted — a filter that matches nothing
//! ```

use crate::protocol::Frame;
use bytes::Bytes;

/// Which command is parsing, and therefore which options are legal.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ScanKind {
    /// `SCAN cursor [MATCH p] [COUNT n] [TYPE t]` — the only one with `TYPE`.
    Keyspace,
    /// `HSCAN key cursor [MATCH p] [COUNT n] [NOVALUES]` — the only one with
    /// `NOVALUES`.
    Hash,
    /// `SSCAN key cursor [MATCH p] [COUNT n]`.
    Set,
    /// `ZSCAN key cursor [MATCH p] [COUNT n]`.
    SortedSet,
}

/// The parsed options. Borrows from `args`, so nothing is allocated.
#[derive(Debug)]
pub struct ScanOptions<'a> {
    pub pattern: Option<&'a [u8]>,
    /// Redis's default when `COUNT` is absent.
    pub count: usize,
    /// `SCAN ... TYPE t` only; `None` for every other kind.
    pub type_filter: Option<&'a [u8]>,
    /// `HSCAN ... NOVALUES` only; always `false` for every other kind.
    pub novalues: bool,
}

fn syntax_error() -> Frame {
    Frame::Error(Bytes::from_static(b"ERR syntax error"))
}

fn not_an_integer() -> Frame {
    Frame::Error(Bytes::from_static(
        b"ERR value is not an integer or out of range",
    ))
}

/// Parse the options that follow the cursor.
///
/// `rest` is the argument slice AFTER the key (if any) and the cursor — the
/// caller has already consumed those, because their arity and their error
/// messages differ per command.
///
/// Returns the offending `Frame::Error` rather than a typed error: every
/// command in this family answers `Frame` and none of them has anything to add
/// to the message.
pub fn parse_scan_options<'a>(kind: ScanKind, rest: &'a [Frame]) -> Result<ScanOptions<'a>, Frame> {
    let mut out = ScanOptions {
        pattern: None,
        count: 10,
        type_filter: None,
        novalues: false,
    };

    let mut i = 0;
    while i < rest.len() {
        // A non-string argument cannot match any option name, so it is exactly
        // as unknown as a misspelled one. The old parsers SKIPPED it, which
        // meant `HSCAN h 0 <int> MATCH p` silently applied the MATCH.
        let Some(opt) = crate::command::helpers::extract_bytes(&rest[i]) else {
            return Err(syntax_error());
        };
        let opt = opt.as_ref();

        if opt.eq_ignore_ascii_case(b"MATCH") {
            let Some(v) = rest
                .get(i + 1)
                .and_then(crate::command::helpers::extract_bytes)
            else {
                return Err(syntax_error());
            };
            out.pattern = Some(v.as_ref());
            i += 2;
        } else if opt.eq_ignore_ascii_case(b"COUNT") {
            let Some(v) = rest
                .get(i + 1)
                .and_then(crate::command::helpers::extract_bytes)
            else {
                return Err(syntax_error());
            };
            // Redis parses the integer FIRST and judges the range second, and
            // the two failures have different messages — `COUNT abc` is the
            // integer error while `COUNT 0` is a plain syntax error. Order is
            // observable, so it is preserved.
            let Ok(n) = std::str::from_utf8(v.as_ref())
                .map_err(|_| ())
                .and_then(|s| s.parse::<i64>().map_err(|_| ()))
            else {
                return Err(not_an_integer());
            };
            if n < 1 {
                return Err(syntax_error());
            }
            out.count = n as usize;
            i += 2;
        } else if opt.eq_ignore_ascii_case(b"TYPE") {
            if kind != ScanKind::Keyspace {
                return Err(syntax_error());
            }
            let Some(v) = rest
                .get(i + 1)
                .and_then(crate::command::helpers::extract_bytes)
            else {
                return Err(syntax_error());
            };
            // An unknown type NAME is legal — it is a filter that matches
            // nothing, and Redis answers an empty result rather than an error.
            out.type_filter = Some(v.as_ref());
            i += 2;
        } else if opt.eq_ignore_ascii_case(b"NOVALUES") {
            if kind != ScanKind::Hash {
                // Redis names the command rather than saying "syntax error",
                // which is the difference between a client author finding the
                // problem in a minute and in an hour.
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR NOVALUES option can only be used in HSCAN",
                )));
            }
            out.novalues = true;
            i += 1;
        } else {
            return Err(syntax_error());
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(items: &[&str]) -> Vec<Frame> {
        items
            .iter()
            .map(|s| Frame::BulkString(Bytes::copy_from_slice(s.as_bytes())))
            .collect()
    }

    fn err_text(f: &Frame) -> String {
        match f {
            Frame::Error(b) => String::from_utf8_lossy(b).into_owned(),
            other => panic!("expected an error, got {other:?}"),
        }
    }

    #[test]
    fn defaults_when_no_options_are_given() {
        let o = parse_scan_options(ScanKind::Hash, &[]).expect("empty is valid");
        assert_eq!(o.count, 10, "Redis's default COUNT");
        assert!(o.pattern.is_none());
        assert!(!o.novalues);
        assert!(o.type_filter.is_none());
    }

    #[test]
    fn match_count_and_novalues_compose_in_any_order() {
        let a = args(&["NOVALUES", "COUNT", "50", "MATCH", "f*"]);
        let o = parse_scan_options(ScanKind::Hash, &a).expect("valid");
        assert!(o.novalues);
        assert_eq!(o.count, 50);
        assert_eq!(o.pattern, Some(&b"f*"[..]));
    }

    #[test]
    fn options_are_matched_case_insensitively() {
        let a = args(&["novalues", "count", "7", "match", "x"]);
        let o = parse_scan_options(ScanKind::Hash, &a).expect("valid");
        assert!(o.novalues);
        assert_eq!(o.count, 7);
    }

    #[test]
    fn an_unknown_token_is_a_syntax_error() {
        for kind in [
            ScanKind::Keyspace,
            ScanKind::Hash,
            ScanKind::Set,
            ScanKind::SortedSet,
        ] {
            let a = args(&["BOGUSTOKEN"]);
            let e = parse_scan_options(kind, &a).expect_err("must refuse");
            assert_eq!(err_text(&e), "ERR syntax error", "{kind:?}");
        }
    }

    #[test]
    fn a_dangling_option_is_a_syntax_error() {
        for a in [args(&["MATCH"]), args(&["COUNT"])] {
            let e = parse_scan_options(ScanKind::Hash, &a).expect_err("must refuse");
            assert_eq!(err_text(&e), "ERR syntax error");
        }
        let e = parse_scan_options(ScanKind::Keyspace, &args(&["TYPE"])).expect_err("must refuse");
        assert_eq!(err_text(&e), "ERR syntax error");
    }

    #[test]
    fn count_distinguishes_not_a_number_from_out_of_range() {
        // The two messages differ in Redis and the ORDER of the checks is what
        // makes them differ; collapsing them loses a real signal.
        let e = parse_scan_options(ScanKind::Hash, &args(&["COUNT", "abc"])).expect_err("refuse");
        assert_eq!(err_text(&e), "ERR value is not an integer or out of range");
        for bad in ["0", "-1"] {
            let e = parse_scan_options(ScanKind::Hash, &args(&["COUNT", bad])).expect_err("refuse");
            assert_eq!(err_text(&e), "ERR syntax error", "COUNT {bad}");
        }
    }

    #[test]
    fn type_belongs_to_scan_alone() {
        let a = args(&["TYPE", "hash"]);
        let o = parse_scan_options(ScanKind::Keyspace, &a).expect("valid");
        assert_eq!(o.type_filter, Some(&b"hash"[..]));
        // An unknown type name is a filter that matches nothing, not an error.
        assert!(parse_scan_options(ScanKind::Keyspace, &args(&["TYPE", "nosuchtype"])).is_ok());
        for kind in [ScanKind::Hash, ScanKind::Set, ScanKind::SortedSet] {
            let e = parse_scan_options(kind, &args(&["TYPE", "hash"])).expect_err("must refuse");
            assert_eq!(err_text(&e), "ERR syntax error", "{kind:?}");
        }
    }

    #[test]
    fn novalues_belongs_to_hscan_alone_and_says_so() {
        assert!(parse_scan_options(ScanKind::Hash, &args(&["NOVALUES"])).is_ok());
        for kind in [ScanKind::Keyspace, ScanKind::Set, ScanKind::SortedSet] {
            let e = parse_scan_options(kind, &args(&["NOVALUES"])).expect_err("must refuse");
            assert_eq!(
                err_text(&e),
                "ERR NOVALUES option can only be used in HSCAN",
                "{kind:?}"
            );
        }
    }

    #[test]
    fn a_trailing_argument_after_novalues_is_refused() {
        let e =
            parse_scan_options(ScanKind::Hash, &args(&["NOVALUES", "EXTRA"])).expect_err("refuse");
        assert_eq!(err_text(&e), "ERR syntax error");
    }

    #[test]
    fn a_non_string_argument_cannot_be_an_option() {
        // The old parsers SKIPPED anything that was not a string, so an Integer
        // frame in the option position was ignored and parsing continued —
        // `HSCAN h 0 <int> MATCH p` quietly applied the MATCH.
        let a = vec![
            Frame::Integer(3),
            Frame::BulkString(Bytes::from_static(b"MATCH")),
            Frame::BulkString(Bytes::from_static(b"p")),
        ];
        let e = parse_scan_options(ScanKind::Hash, &a).expect_err("must refuse");
        assert_eq!(err_text(&e), "ERR syntax error");
    }
}
