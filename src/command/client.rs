use bytes::Bytes;
use crate::protocol::Frame;

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
            b"ERR wrong number of arguments for 'CLIENT TRACKING' command",
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

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::from(s.to_vec()))
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
