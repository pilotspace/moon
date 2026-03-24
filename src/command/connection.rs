use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Database;

/// Global monotonic client ID counter.
static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

/// Allocate a unique client connection ID.
pub fn next_client_id() -> u64 {
    NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed)
}

/// CLIENT ID command: return the connection's unique ID.
pub fn client_id(id: u64) -> Frame {
    Frame::Integer(id as i64)
}

/// PING command handler.
///
/// No args: return PONG as SimpleString.
/// One arg: return the arg as BulkString.
/// More args: return arity error.
pub fn ping(args: &[Frame]) -> Frame {
    match args.len() {
        0 => Frame::SimpleString(Bytes::from_static(b"PONG")),
        1 => match &args[0] {
            Frame::BulkString(s) => Frame::BulkString(s.clone()),
            Frame::SimpleString(s) => Frame::BulkString(s.clone()),
            _ => Frame::BulkString(Bytes::from(format!("{:?}", args[0]))),
        },
        _ => Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'PING' command",
        )),
    }
}

/// ECHO command handler.
///
/// Exactly one arg: return it as BulkString.
/// Wrong arity: return error.
pub fn echo(args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'ECHO' command",
        ));
    }
    match &args[0] {
        Frame::BulkString(s) => Frame::BulkString(s.clone()),
        Frame::SimpleString(s) => Frame::BulkString(s.clone()),
        _ => Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'ECHO' command",
        )),
    }
}

/// SELECT command handler.
///
/// Parse arg as integer, validate range 0..db_count, set selected_db.
pub fn select(args: &[Frame], selected_db: &mut usize, db_count: usize) -> Frame {
    if args.len() != 1 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'SELECT' command",
        ));
    }
    let index_str = match &args[0] {
        Frame::BulkString(s) => s,
        Frame::SimpleString(s) => s,
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };
    let index_str = match std::str::from_utf8(index_str) {
        Ok(s) => s,
        Err(_) => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };
    let index: usize = match index_str.parse() {
        Ok(n) => n,
        Err(_) => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };
    if index >= db_count {
        return Frame::Error(Bytes::from_static(b"ERR DB index is out of range"));
    }
    *selected_db = index;
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// COMMAND command handler.
///
/// COMMAND (bare): return integer 0.
/// COMMAND DOCS: return empty array.
/// Any other subcommand: return empty array.
pub fn command(args: &[Frame]) -> Frame {
    if args.is_empty() {
        return Frame::Integer(0);
    }
    // Check for DOCS subcommand (case-insensitive, zero-alloc)
    if let Some(Frame::BulkString(sub)) | Some(Frame::SimpleString(sub)) = args.first() {
        if sub.eq_ignore_ascii_case(b"DOCS") || sub.eq_ignore_ascii_case(b"COUNT") {
            return Frame::Array(vec![]);
        }
    }
    Frame::Array(vec![])
}

/// INFO command handler.
///
/// Returns a BulkString with minimal INFO sections.
pub fn info(db: &Database, _args: &[Frame]) -> Frame {
    let mut sections = String::new();

    sections.push_str("# Server\r\n");
    sections.push_str("redis_version:0.1.0\r\n");
    sections.push_str("rust_redis:true\r\n");
    sections.push_str("\r\n");

    sections.push_str("# Clients\r\n");
    sections.push_str("\r\n");

    sections.push_str("# Memory\r\n");
    sections.push_str("\r\n");

    sections.push_str("# Keyspace\r\n");
    let key_count = db.len();
    let expires_count = db.expires_count();
    if key_count > 0 {
        sections.push_str(&format!(
            "db0:keys={},expires={},avg_ttl=0\r\n",
            key_count, expires_count
        ));
    }

    Frame::BulkString(Bytes::from(sections))
}

/// INFO command handler (read-only variant for RwLock read path).
///
/// Identical to info() -- Database methods used (len, expires_count) are already &self.
pub fn info_readonly(db: &Database, args: &[Frame]) -> Frame {
    info(db, args)
}

/// AUTH command handler.
///
/// Authenticates the client with the configured password.
/// Returns OK on success, WRONGPASS on mismatch, or ERR if no password is configured.
pub fn auth(args: &[Frame], requirepass: &Option<String>) -> Frame {
    if args.len() != 1 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'AUTH' command",
        ));
    }

    let password = match requirepass {
        Some(p) => p,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR Client sent AUTH, but no password is set",
            ));
        }
    };

    let provided = match &args[0] {
        Frame::BulkString(s) => s,
        Frame::SimpleString(s) => s,
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR invalid password type",
            ));
        }
    };

    if provided.as_ref() == password.as_bytes() {
        Frame::SimpleString(Bytes::from_static(b"OK"))
    } else {
        Frame::Error(Bytes::from_static(
            b"WRONGPASS invalid username-password pair or user is disabled.",
        ))
    }
}

/// Extract a byte slice reference from a Frame argument (zero-alloc).
fn extract_bytes_ref(frame: &Frame) -> Option<&[u8]> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.as_ref()),
        _ => None,
    }
}

/// Extract an owned Bytes from a Frame argument.
fn extract_bytes_owned(frame: &Frame) -> Option<Bytes> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.clone()),
        _ => None,
    }
}

/// HELLO command handler.
///
/// HELLO [protover [AUTH username password] [SETNAME clientname]]
/// Returns server info Map. Sets protocol_version if protover given.
/// Returns (response_frame, new_protocol_version, new_client_name)
pub fn hello(
    args: &[Frame],
    current_proto: u8,
    client_id: u64,
    requirepass: &Option<String>,
    authenticated: &mut bool,
) -> (Frame, u8, Option<Bytes>) {
    let mut proto = current_proto;
    let mut client_name: Option<Bytes> = None;
    let mut i = 0;

    // Parse optional protover
    if i < args.len() {
        if let Some(ver_bytes) = extract_bytes_ref(&args[i]) {
            if let Ok(ver_str) = std::str::from_utf8(ver_bytes) {
                if let Ok(ver) = ver_str.parse::<u8>() {
                    if ver != 2 && ver != 3 {
                        return (
                            Frame::Error(Bytes::from_static(
                                b"NOPROTO unsupported protocol version",
                            )),
                            current_proto,
                            None,
                        );
                    }
                    proto = ver;
                    i += 1;
                }
            }
        }
    }

    // Parse optional AUTH and SETNAME (can appear in any order after protover)
    while i < args.len() {
        if let Some(keyword) = extract_bytes_ref(&args[i]) {
            if keyword.eq_ignore_ascii_case(b"AUTH") {
                // Need username and password (2 more args)
                if i + 2 >= args.len() {
                    return (
                        Frame::Error(Bytes::from_static(
                            b"ERR Syntax error in HELLO option 'auth'",
                        )),
                        current_proto,
                        None,
                    );
                }
                // username is args[i+1] (we ignore it -- single-user mode)
                // password is args[i+2]
                let auth_result = auth(&[args[i + 2].clone()], requirepass);
                if matches!(&auth_result, Frame::Error(_)) {
                    return (auth_result, current_proto, None); // Auth failed, don't change proto
                }
                *authenticated = true;
                i += 3;
            } else if keyword.eq_ignore_ascii_case(b"SETNAME") {
                if i + 1 >= args.len() {
                    return (
                        Frame::Error(Bytes::from_static(
                            b"ERR Syntax error in HELLO option 'setname'",
                        )),
                        current_proto,
                        None,
                    );
                }
                client_name = extract_bytes_owned(&args[i + 1]);
                i += 2;
            } else {
                return (
                    Frame::Error(Bytes::from(format!(
                        "ERR Unrecognized HELLO option: {:?}",
                        String::from_utf8_lossy(keyword)
                    ))),
                    current_proto,
                    None,
                );
            }
        } else {
            break;
        }
    }

    // Build response Map
    let response = Frame::Map(vec![
        (
            Frame::BulkString(Bytes::from_static(b"server")),
            Frame::BulkString(Bytes::from_static(b"rustredis")),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"version")),
            Frame::BulkString(Bytes::from_static(b"0.1.0")),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"proto")),
            Frame::Integer(proto as i64),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"id")),
            Frame::Integer(client_id as i64),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"mode")),
            Frame::BulkString(Bytes::from_static(b"standalone")),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"role")),
            Frame::BulkString(Bytes::from_static(b"master")),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"modules")),
            Frame::Array(vec![]),
        ),
    ]);

    (response, proto, client_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping_no_args() {
        let result = ping(&[]);
        assert_eq!(
            result,
            Frame::SimpleString(Bytes::from_static(b"PONG"))
        );
    }

    #[test]
    fn test_ping_with_arg() {
        let result = ping(&[Frame::BulkString(Bytes::from_static(b"hello"))]);
        assert_eq!(
            result,
            Frame::BulkString(Bytes::from_static(b"hello"))
        );
    }

    #[test]
    fn test_ping_too_many_args() {
        let result = ping(&[
            Frame::BulkString(Bytes::from_static(b"a")),
            Frame::BulkString(Bytes::from_static(b"b")),
        ]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_echo() {
        let result = echo(&[Frame::BulkString(Bytes::from_static(b"hello"))]);
        assert_eq!(
            result,
            Frame::BulkString(Bytes::from_static(b"hello"))
        );
    }

    #[test]
    fn test_echo_wrong_arity() {
        let result = echo(&[]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_select_valid() {
        let mut selected = 0usize;
        let result = select(
            &[Frame::BulkString(Bytes::from_static(b"5"))],
            &mut selected,
            16,
        );
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(selected, 5);
    }

    #[test]
    fn test_select_out_of_range() {
        let mut selected = 0usize;
        let result = select(
            &[Frame::BulkString(Bytes::from_static(b"16"))],
            &mut selected,
            16,
        );
        assert!(matches!(result, Frame::Error(ref s) if s.as_ref() == b"ERR DB index is out of range"));
        assert_eq!(selected, 0); // unchanged
    }

    #[test]
    fn test_select_non_integer() {
        let mut selected = 0usize;
        let result = select(
            &[Frame::BulkString(Bytes::from_static(b"abc"))],
            &mut selected,
            16,
        );
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_command_bare() {
        let result = command(&[]);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_command_docs() {
        let result = command(&[Frame::BulkString(Bytes::from_static(b"DOCS"))]);
        assert_eq!(result, Frame::Array(vec![]));
    }

    #[test]
    fn test_command_docs_lowercase() {
        let result = command(&[Frame::BulkString(Bytes::from_static(b"docs"))]);
        assert_eq!(result, Frame::Array(vec![]));
    }

    #[test]
    fn test_info_basic() {
        let db = Database::new();
        let result = info(&db, &[]);
        match result {
            Frame::BulkString(s) => {
                let text = std::str::from_utf8(&s).unwrap();
                assert!(text.contains("# Server"));
                assert!(text.contains("redis_version:0.1.0"));
                assert!(text.contains("# Keyspace"));
            }
            _ => panic!("Expected BulkString"),
        }
    }

    #[test]
    fn test_info_with_keys() {
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"key1"),
            crate::storage::Entry::new_string(Bytes::from_static(b"val")),
        );
        let result = info(&db, &[]);
        match result {
            Frame::BulkString(s) => {
                let text = std::str::from_utf8(&s).unwrap();
                assert!(text.contains("db0:keys=1,expires=0,avg_ttl=0"));
            }
            _ => panic!("Expected BulkString"),
        }
    }

    #[test]
    fn test_auth_correct_password() {
        let pass = Some("secret123".to_string());
        let result = auth(
            &[Frame::BulkString(Bytes::from_static(b"secret123"))],
            &pass,
        );
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
    }

    #[test]
    fn test_auth_wrong_password() {
        let pass = Some("secret123".to_string());
        let result = auth(
            &[Frame::BulkString(Bytes::from_static(b"wrong"))],
            &pass,
        );
        assert!(matches!(result, Frame::Error(ref s) if s.starts_with(b"WRONGPASS")));
    }

    #[test]
    fn test_auth_no_password_configured() {
        let pass: Option<String> = None;
        let result = auth(
            &[Frame::BulkString(Bytes::from_static(b"anything"))],
            &pass,
        );
        assert!(matches!(result, Frame::Error(ref s) if s.starts_with(b"ERR Client sent AUTH")));
    }

    #[test]
    fn test_auth_wrong_arity() {
        let pass = Some("secret".to_string());
        let result = auth(&[], &pass);
        assert!(matches!(result, Frame::Error(ref s) if s.starts_with(b"ERR wrong number")));
    }

    // === HELLO command tests ===

    fn get_proto_from_hello_response(frame: &Frame) -> Option<i64> {
        if let Frame::Map(entries) = frame {
            for (k, v) in entries {
                if let Frame::BulkString(key) = k {
                    if key.as_ref() == b"proto" {
                        if let Frame::Integer(n) = v {
                            return Some(*n);
                        }
                    }
                }
            }
        }
        None
    }

    #[test]
    fn test_hello_no_args_returns_current_proto() {
        let mut auth = true;
        let (resp, proto, name) = hello(&[], 2, 1, &None, &mut auth);
        assert!(matches!(resp, Frame::Map(_)));
        assert_eq!(get_proto_from_hello_response(&resp), Some(2));
        assert_eq!(proto, 2);
        assert!(name.is_none());
    }

    #[test]
    fn test_hello_upgrade_to_resp3() {
        let mut auth = true;
        let (resp, proto, _) = hello(
            &[Frame::BulkString(Bytes::from_static(b"3"))],
            2,
            1,
            &None,
            &mut auth,
        );
        assert_eq!(proto, 3);
        assert_eq!(get_proto_from_hello_response(&resp), Some(3));
    }

    #[test]
    fn test_hello_downgrade_to_resp2() {
        let mut auth = true;
        let (resp, proto, _) = hello(
            &[Frame::BulkString(Bytes::from_static(b"2"))],
            3,
            1,
            &None,
            &mut auth,
        );
        assert_eq!(proto, 2);
        assert_eq!(get_proto_from_hello_response(&resp), Some(2));
    }

    #[test]
    fn test_hello_with_auth_success() {
        let pass = Some("secret".to_string());
        let mut auth = false;
        let (resp, proto, _) = hello(
            &[
                Frame::BulkString(Bytes::from_static(b"3")),
                Frame::BulkString(Bytes::from_static(b"AUTH")),
                Frame::BulkString(Bytes::from_static(b"default")),
                Frame::BulkString(Bytes::from_static(b"secret")),
            ],
            2,
            1,
            &pass,
            &mut auth,
        );
        assert_eq!(proto, 3);
        assert!(matches!(resp, Frame::Map(_)));
        assert!(auth); // authenticated
    }

    #[test]
    fn test_hello_with_auth_failure() {
        let pass = Some("secret".to_string());
        let mut auth = false;
        let (resp, proto, _) = hello(
            &[
                Frame::BulkString(Bytes::from_static(b"3")),
                Frame::BulkString(Bytes::from_static(b"AUTH")),
                Frame::BulkString(Bytes::from_static(b"default")),
                Frame::BulkString(Bytes::from_static(b"wrong")),
            ],
            2,
            1,
            &pass,
            &mut auth,
        );
        // Auth failed: proto stays at current, response is error
        assert_eq!(proto, 2);
        assert!(matches!(resp, Frame::Error(ref s) if s.starts_with(b"WRONGPASS")));
        assert!(!auth); // not authenticated
    }

    #[test]
    fn test_hello_with_setname() {
        let mut auth = true;
        let (_, _, name) = hello(
            &[
                Frame::BulkString(Bytes::from_static(b"3")),
                Frame::BulkString(Bytes::from_static(b"SETNAME")),
                Frame::BulkString(Bytes::from_static(b"myclient")),
            ],
            2,
            1,
            &None,
            &mut auth,
        );
        assert_eq!(name, Some(Bytes::from_static(b"myclient")));
    }

    #[test]
    fn test_hello_noproto() {
        let mut auth = true;
        let (resp, proto, _) = hello(
            &[Frame::BulkString(Bytes::from_static(b"4"))],
            2,
            1,
            &None,
            &mut auth,
        );
        assert_eq!(proto, 2); // unchanged
        assert!(matches!(resp, Frame::Error(ref s) if s.starts_with(b"NOPROTO")));
    }

    #[test]
    fn test_client_id_returns_integer() {
        let result = client_id(42);
        assert_eq!(result, Frame::Integer(42));
    }
}
