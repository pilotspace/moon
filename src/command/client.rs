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
pub fn parse_tracking_args(args: &[Frame]) -> Result<TrackingConfig, Frame> {
    if args.len() < 2 {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'client|tracking' command",
        )));
    }
    let on_off = match &args[1] {
        Frame::BulkString(s) | Frame::SimpleString(s) => s,
        _ => return Err(Frame::Error(Bytes::from_static(b"ERR syntax error"))),
    };

    let enable = if on_off.eq_ignore_ascii_case(b"ON") {
        true
    } else if on_off.eq_ignore_ascii_case(b"OFF") {
        false
    } else {
        return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
    };

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
        } else if opt.eq_ignore_ascii_case(b"REDIRECT") {
            i += 1;
            if i >= args.len() {
                return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
            }
            let id_bytes = match &args[i] {
                Frame::BulkString(s) | Frame::SimpleString(s) => s,
                _ => return Err(Frame::Error(Bytes::from_static(b"ERR syntax error"))),
            };
            let id_str = std::str::from_utf8(id_bytes).map_err(|_| {
                Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ))
            })?;
            redirect = Some(id_str.parse::<u64>().map_err(|_| {
                Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ))
            })?);
        } else if opt.eq_ignore_ascii_case(b"PREFIX") {
            i += 1;
            if i >= args.len() {
                return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
            }
            let prefix = match &args[i] {
                Frame::BulkString(s) | Frame::SimpleString(s) => s.clone(),
                _ => return Err(Frame::Error(Bytes::from_static(b"ERR syntax error"))),
            };
            prefixes.push(prefix);
        } else {
            return Err(Frame::Error(Bytes::from(format!(
                "ERR Unrecognized option: {:?}",
                String::from_utf8_lossy(&opt)
            ))));
        }
        i += 1;
    }

    // PREFIX requires BCAST
    if !prefixes.is_empty() && !bcast {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR PREFIX option requires BCAST mode to be enabled",
        )));
    }

    // `BCAST` with no `PREFIX` means "invalidate me for EVERY key" in Redis.
    // The handlers register broadcast interest with
    // `TrackingTable::register_prefix` inside `for prefix in &prefixes`, so a
    // prefix-less BCAST client used to register nothing and then never
    // received an invalidation — at any shard count, and regardless of what it
    // read (BCAST does not depend on reads at all). `TrackingTable` matches
    // with `key.starts_with(prefix)`, for which the empty prefix is exactly
    // "all keys". Normalising here fixes all three handlers at once
    // (monoio/dispatch.rs, handler_single.rs, handler_sharded/dispatch.rs)
    // and keeps the semantics in one place.
    if bcast && prefixes.is_empty() {
        prefixes.push(Bytes::new());
    }

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
        let config = parse_tracking_args(&args).unwrap();
        assert!(config.enable);
        assert!(!config.bcast);
        assert!(!config.noloop);
    }

    #[test]
    fn test_parse_tracking_off() {
        let args = vec![bs(b"TRACKING"), bs(b"OFF")];
        let config = parse_tracking_args(&args).unwrap();
        assert!(!config.enable);
    }

    #[test]
    fn test_parse_tracking_on_bcast() {
        let args = vec![bs(b"TRACKING"), bs(b"ON"), bs(b"BCAST")];
        let config = parse_tracking_args(&args).unwrap();
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
        let config = parse_tracking_args(&args).unwrap();
        assert!(config.enable);
        assert!(config.bcast);
        assert_eq!(config.prefixes.len(), 1);
        assert_eq!(config.prefixes[0].as_ref(), b"user:");
    }

    #[test]
    fn test_parse_tracking_prefix_without_bcast_fails() {
        let args = vec![bs(b"TRACKING"), bs(b"ON"), bs(b"PREFIX"), bs(b"user:")];
        let result = parse_tracking_args(&args);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_tracking_on_noloop() {
        let args = vec![bs(b"TRACKING"), bs(b"ON"), bs(b"NOLOOP")];
        let config = parse_tracking_args(&args).unwrap();
        assert!(config.enable);
        assert!(config.noloop);
    }

    #[test]
    fn test_parse_tracking_on_redirect() {
        let args = vec![bs(b"TRACKING"), bs(b"ON"), bs(b"REDIRECT"), bs(b"42")];
        let config = parse_tracking_args(&args).unwrap();
        assert!(config.enable);
        assert_eq!(config.redirect, Some(42));
    }

    #[test]
    fn test_parse_tracking_redirect_invalid_int() {
        let args = vec![bs(b"TRACKING"), bs(b"ON"), bs(b"REDIRECT"), bs(b"abc")];
        let result = parse_tracking_args(&args);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_tracking_too_few_args() {
        let args = vec![bs(b"TRACKING")];
        let result = parse_tracking_args(&args);
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
        let config = parse_tracking_args(&args).unwrap();
        assert!(config.enable);
        assert!(config.bcast);
        assert!(config.noloop);
        assert_eq!(config.prefixes.len(), 2);
    }
}
