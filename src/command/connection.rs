use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Database;

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
    // Check for DOCS subcommand (case-insensitive)
    if let Some(Frame::BulkString(sub)) | Some(Frame::SimpleString(sub)) = args.first() {
        let upper = sub.to_ascii_uppercase();
        if upper.as_slice() == b"DOCS" || upper.as_slice() == b"COUNT" {
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
}
