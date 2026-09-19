use crate::protocol::Frame;
use bytes::Bytes;

/// Parsed CLIENT TRACKING configuration.
pub struct TrackingConfig {
    pub enable: bool,
    pub bcast: bool,
    pub optin: bool,
    pub optout: bool,
    pub noloop: bool,
    pub redirect: Option<u64>,
    pub prefixes: Vec<Bytes>,
}

/// Parse CLIENT TRACKING ON|OFF options.
/// `args` starts from the subcommand after "CLIENT", i.e. args[0] = "TRACKING", args[1] = ON|OFF.
///
/// Follows redis `clientCommand` step for step, because the ORDER decides
/// which error a malformed request gets (all measured on redis-server 8.6.1):
/// the options are parsed first — an unknown one is a plain syntax error, and
/// `REDIRECT <id>` is checked against `redirect_exists` right where it is
/// parsed, so `REDIRECT 99999 PREFIX x` names the missing client, not the
/// missing BCAST — then `ON|OFF`, then (for ON) the PREFIX-needs-BCAST rule.
/// The rules that depend on the connection's CURRENT mode are checked by the
/// caller, which has that state.
pub fn parse_tracking_args(
    args: &[Frame],
    redirect_exists: impl Fn(u64) -> bool,
) -> Result<TrackingConfig, Frame> {
    const SYNTAX: &[u8] = b"ERR syntax error";
    if args.len() < 2 {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'client|tracking' command",
        )));
    }

    let mut bcast = false;
    let mut optin = false;
    let mut optout = false;
    let mut noloop = false;
    let mut redirect: Option<u64> = None;
    let mut prefixes: Vec<Bytes> = Vec::new();

    let mut i = 2;
    while i < args.len() {
        let opt = match &args[i] {
            Frame::BulkString(s) | Frame::SimpleString(s) => s.clone(),
            _ => return Err(Frame::Error(Bytes::from_static(b"ERR syntax error"))),
        };
        if opt.eq_ignore_ascii_case(b"BCAST") {
            bcast = true;
        } else if opt.eq_ignore_ascii_case(b"OPTIN") {
            optin = true;
        } else if opt.eq_ignore_ascii_case(b"OPTOUT") {
            optout = true;
        } else if opt.eq_ignore_ascii_case(b"NOLOOP") {
            noloop = true;
        } else if opt.eq_ignore_ascii_case(b"REDIRECT") && i + 1 < args.len() {
            i += 1;
            if redirect.is_some() {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR A client can only redirect to a single other client",
                )));
            }
            let not_int = || {
                Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ))
            };
            let id = match &args[i] {
                Frame::BulkString(s) | Frame::SimpleString(s) => std::str::from_utf8(s)
                    .ok()
                    .and_then(|t| t.parse::<i64>().ok())
                    .ok_or_else(not_int)?,
                _ => return Err(not_int()),
            };
            // Client ids start at 1, so 0 and negatives name nobody — redis
            // answers them exactly like an id that has disconnected.
            match u64::try_from(id) {
                Ok(id) if id > 0 && redirect_exists(id) => redirect = Some(id),
                _ => {
                    return Err(Frame::Error(Bytes::from_static(
                        b"ERR The client ID you want redirect to does not exist",
                    )));
                }
            }
        } else if opt.eq_ignore_ascii_case(b"PREFIX") && i + 1 < args.len() {
            i += 1;
            let prefix = match &args[i] {
                Frame::BulkString(s) | Frame::SimpleString(s) => s.clone(),
                _ => return Err(Frame::Error(Bytes::from_static(SYNTAX))),
            };
            prefixes.push(prefix);
        } else {
            return Err(Frame::Error(Bytes::from_static(SYNTAX)));
        }
        i += 1;
    }

    let enable = match &args[1] {
        Frame::BulkString(s) | Frame::SimpleString(s) if s.eq_ignore_ascii_case(b"ON") => true,
        Frame::BulkString(s) | Frame::SimpleString(s) if s.eq_ignore_ascii_case(b"OFF") => false,
        _ => return Err(Frame::Error(Bytes::from_static(SYNTAX))),
    };

    // PREFIX requires BCAST
    if enable && !prefixes.is_empty() && !bcast {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR PREFIX option requires BCAST mode to be enabled",
        )));
    }

    // `BCAST` with no `PREFIX` ("invalidate me for EVERY key") is normalised
    // to the empty prefix by `tracking::client_cmd`, AFTER the prefix
    // collision check — redis skips that check when no PREFIX was given.

    Ok(TrackingConfig {
        enable,
        bcast,
        optin,
        optout,
        noloop,
        redirect,
        prefixes,
    })
}

/// `CLIENT NO-EVICT ON|OFF` and `CLIENT NO-TOUCH ON|OFF` (moon#580).
///
/// Redis 7+ registers both subcommands in its command table with arity **3**
/// (exact), so the `ON|OFF` argument is mandatory and a fourth argument is an
/// arity error too — only a present-but-unrecognised value reaches the
/// subcommand body and becomes a syntax error. Measured against redis-server
/// 8.6.1:
///
/// ```text
/// CLIENT NO-EVICT           -> -ERR wrong number of arguments for 'client|no-evict' command
/// CLIENT NO-EVICT ON EXTRA  -> -ERR wrong number of arguments for 'client|no-evict' command
/// CLIENT NO-EVICT MAYBE     -> -ERR syntax error
/// CLIENT NO-EVICT ON|OFF    -> +OK
/// ```
///
/// Moon used to answer `+OK` to every one of those, telling a client the
/// setting had been applied when nothing was ever parsed.
///
/// `sub` is the raw subcommand token (any case); `args` starts AT that token,
/// so `args[0]` is the subcommand and `args[1]` is its `ON|OFF` argument —
/// the same slice the three dispatch paths already hold. Allocation-free: both
/// error texts are static, so this is safe on the dispatch hot path.
///
/// Moon does not yet act on either flag (it has no per-client eviction bucket
/// and no LRU-touch suppression); this makes the PARSE faithful, so a client
/// is no longer told a setting took effect when the request was malformed.
#[must_use]
pub fn no_evict_or_no_touch(sub: &[u8], args: &[Frame]) -> Frame {
    let arity_err: &'static [u8] = if sub.eq_ignore_ascii_case(b"NO-TOUCH") {
        b"ERR wrong number of arguments for 'client|no-touch' command"
    } else {
        b"ERR wrong number of arguments for 'client|no-evict' command"
    };
    if args.len() != 2 {
        return Frame::Error(Bytes::from_static(arity_err));
    }
    match &args[1] {
        Frame::BulkString(v) | Frame::SimpleString(v)
            if v.eq_ignore_ascii_case(b"ON") || v.eq_ignore_ascii_case(b"OFF") =>
        {
            Frame::SimpleString(Bytes::from_static(b"OK"))
        }
        _ => Frame::Error(Bytes::from_static(b"ERR syntax error")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::from(s.to_vec()))
    }

    // ---- CLIENT NO-EVICT / NO-TOUCH arity (moon#580) ----------------------

    fn err_text(f: &Frame) -> String {
        match f {
            Frame::Error(e) => String::from_utf8_lossy(e).into_owned(),
            Frame::SimpleString(s) => format!("+{}", String::from_utf8_lossy(s)),
            other => format!("{other:?}"),
        }
    }

    #[test]
    fn no_evict_without_its_argument_is_an_arity_error() {
        assert_eq!(
            err_text(&no_evict_or_no_touch(b"NO-EVICT", &[bs(b"NO-EVICT")])),
            "ERR wrong number of arguments for 'client|no-evict' command"
        );
        assert_eq!(
            err_text(&no_evict_or_no_touch(b"NO-TOUCH", &[bs(b"NO-TOUCH")])),
            "ERR wrong number of arguments for 'client|no-touch' command"
        );
    }

    #[test]
    fn no_evict_with_an_extra_argument_is_also_an_arity_error() {
        // Redis's arity 3 is EXACT, not a minimum.
        assert_eq!(
            err_text(&no_evict_or_no_touch(
                b"NO-EVICT",
                &[bs(b"NO-EVICT"), bs(b"ON"), bs(b"EXTRA")]
            )),
            "ERR wrong number of arguments for 'client|no-evict' command"
        );
    }

    #[test]
    fn no_evict_with_a_bad_value_is_a_syntax_error_not_an_arity_error() {
        // The two error classes are distinct in Redis and must stay distinct
        // here: arity is checked by the command table, the value by the body.
        assert_eq!(
            err_text(&no_evict_or_no_touch(
                b"NO-EVICT",
                &[bs(b"NO-EVICT"), bs(b"MAYBE")]
            )),
            "ERR syntax error"
        );
    }

    #[test]
    fn no_evict_on_and_off_are_accepted_in_any_case() {
        for v in [&b"ON"[..], b"off", b"On", b"OFF"] {
            assert_eq!(
                no_evict_or_no_touch(b"no-evict", &[bs(b"no-evict"), bs(v)]),
                Frame::SimpleString(Bytes::from_static(b"OK")),
                "CLIENT NO-EVICT {} must be accepted",
                String::from_utf8_lossy(v)
            );
        }
    }

    #[test]
    fn no_evict_with_a_non_string_argument_is_a_syntax_error() {
        // A client can put any RESP type in the slot; the parser must answer,
        // never panic.
        assert_eq!(
            err_text(&no_evict_or_no_touch(
                b"NO-EVICT",
                &[bs(b"NO-EVICT"), Frame::Integer(1)]
            )),
            "ERR syntax error"
        );
    }

    #[test]
    fn test_parse_tracking_on() {
        let args = vec![bs(b"TRACKING"), bs(b"ON")];
        let config = parse_tracking_args(&args, |_| true).unwrap();
        assert!(config.enable);
        assert!(!config.bcast);
        assert!(!config.noloop);
    }

    #[test]
    fn test_parse_tracking_off() {
        let args = vec![bs(b"TRACKING"), bs(b"OFF")];
        let config = parse_tracking_args(&args, |_| true).unwrap();
        assert!(!config.enable);
    }

    #[test]
    fn test_parse_tracking_on_bcast() {
        let args = vec![bs(b"TRACKING"), bs(b"ON"), bs(b"BCAST")];
        let config = parse_tracking_args(&args, |_| true).unwrap();
        assert!(config.enable);
        assert!(config.bcast);
    }

    #[test]
    fn test_parse_tracking_on_bcast_prefix() {
        let args = vec![
            bs(b"TRACKING"),
            bs(b"ON"),
            bs(b"BCAST"),
            bs(b"PREFIX"),
            bs(b"user:"),
        ];
        let config = parse_tracking_args(&args, |_| true).unwrap();
        assert!(config.enable);
        assert!(config.bcast);
        assert_eq!(config.prefixes.len(), 1);
        assert_eq!(config.prefixes[0].as_ref(), b"user:");
    }

    #[test]
    fn test_parse_tracking_prefix_without_bcast_fails() {
        let args = vec![bs(b"TRACKING"), bs(b"ON"), bs(b"PREFIX"), bs(b"user:")];
        let result = parse_tracking_args(&args, |_| true);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_tracking_on_noloop() {
        let args = vec![bs(b"TRACKING"), bs(b"ON"), bs(b"NOLOOP")];
        let config = parse_tracking_args(&args, |_| true).unwrap();
        assert!(config.enable);
        assert!(config.noloop);
    }

    #[test]
    fn test_parse_tracking_on_redirect() {
        let args = vec![bs(b"TRACKING"), bs(b"ON"), bs(b"REDIRECT"), bs(b"42")];
        let config = parse_tracking_args(&args, |_| true).unwrap();
        assert!(config.enable);
        assert_eq!(config.redirect, Some(42));
    }

    #[test]
    fn test_parse_tracking_redirect_invalid_int() {
        let args = vec![bs(b"TRACKING"), bs(b"ON"), bs(b"REDIRECT"), bs(b"abc")];
        let result = parse_tracking_args(&args, |_| true);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_tracking_too_few_args() {
        let args = vec![bs(b"TRACKING")];
        let result = parse_tracking_args(&args, |_| true);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_tracking_on_bcast_noloop_multiple_prefixes() {
        let args = vec![
            bs(b"TRACKING"),
            bs(b"ON"),
            bs(b"BCAST"),
            bs(b"NOLOOP"),
            bs(b"PREFIX"),
            bs(b"user:"),
            bs(b"PREFIX"),
            bs(b"session:"),
        ];
        let config = parse_tracking_args(&args, |_| true).unwrap();
        assert!(config.enable);
        assert!(config.bcast);
        assert!(config.noloop);
        assert_eq!(config.prefixes.len(), 2);
    }

    fn parse_err(parts: &[&[u8]], exists: fn(u64) -> bool) -> String {
        let args: Vec<Frame> = parts.iter().map(|p| bs(p)).collect();
        match parse_tracking_args(&args, exists) {
            Ok(_) => panic!("expected an error for {parts:?}"),
            Err(f) => err_text(&f),
        }
    }

    /// Error precedence measured on redis-server 8.6.1 (moon#1048): options
    /// are parsed before ON|OFF, REDIRECT's target is checked where it is
    /// parsed, and PREFIX-needs-BCAST comes last.
    #[test]
    fn tracking_parse_errors_follow_redis_order() {
        let missing = "ERR The client ID you want redirect to does not exist";
        let exists_42: fn(u64) -> bool = |id| id == 42;
        assert_eq!(
            parse_err(&[b"TRACKING", b"ON", b"REDIRECT", b"7"], exists_42),
            missing
        );
        assert_eq!(
            parse_err(&[b"TRACKING", b"ON", b"REDIRECT", b"0"], |_| true),
            missing
        );
        assert_eq!(
            parse_err(&[b"TRACKING", b"ON", b"REDIRECT", b"-1"], |_| true),
            missing
        );
        assert_eq!(
            parse_err(
                &[b"TRACKING", b"ON", b"REDIRECT", b"7", b"PREFIX", b"x"],
                exists_42
            ),
            missing,
            "the missing client is reported before the missing BCAST"
        );
        assert_eq!(
            parse_err(&[b"TRACKING", b"MAYBE", b"REDIRECT", b"7"], exists_42),
            missing,
            "options are parsed before ON|OFF"
        );
        assert_eq!(
            parse_err(
                &[b"TRACKING", b"ON", b"REDIRECT", b"42", b"REDIRECT", b"42"],
                exists_42
            ),
            "ERR A client can only redirect to a single other client"
        );
        assert_eq!(
            parse_err(&[b"TRACKING", b"ON", b"FOO"], |_| true),
            "ERR syntax error"
        );
        assert_eq!(
            parse_err(&[b"TRACKING", b"ON", b"REDIRECT"], |_| true),
            "ERR syntax error"
        );
        assert_eq!(
            parse_err(&[b"TRACKING", b"ON", b"PREFIX"], |_| true),
            "ERR syntax error"
        );
        // OFF with a PREFIX but no BCAST is accepted: redis checks that rule
        // for ON only.
        let off: Vec<Frame> = [b"TRACKING".as_ref(), b"OFF", b"PREFIX", b"x"]
            .iter()
            .map(|p| bs(p))
            .collect();
        assert!(parse_tracking_args(&off, |_| true).is_ok());
    }
}
