//! MQ.* command validation and helpers.
//!
//! Commands:
//! - MQ CREATE <key> [MAXDELIVERY <n>] [DEBOUNCE <ms>]: Create a durable queue
//! - MQ PUSH <key> <f1> <v1> [f2 v2 ...]: Enqueue a message
//! - MQ POP <key> [COUNT <n>]: Claim messages
//! - MQ ACK <key> <id1> [id2 ...]: Acknowledge messages
//! - MQ DLQLEN <key>: Return dead-letter queue depth
//! - MQ TRIGGER <key> <callback_cmd> [DEBOUNCE <ms>]: Register debounced trigger
//! - MQ PUBLISH <key> <f1> <v1> [f2 v2 ...]: Transactional enqueue (TXN only)
//!
//! Handler integration: MQ.* commands are intercepted BEFORE dispatch
//! (same pattern as TXN.*, TEMPORAL.*, WS.*) in handler_monoio.rs.

use bytes::Bytes;

use crate::protocol::Frame;

/// Error: wrong number of arguments for 'MQ' command.
pub const ERR_MQ_ARGS: &[u8] = b"ERR wrong number of arguments for 'MQ' command";

/// Error: wrong number of arguments for 'MQ CREATE' command.
pub const ERR_MQ_CREATE_ARGS: &[u8] = b"ERR wrong number of arguments for 'MQ CREATE' command";

/// Error: wrong number of arguments for 'MQ PUSH' command.
pub const ERR_MQ_PUSH_ARGS: &[u8] = b"ERR wrong number of arguments for 'MQ PUSH' command";

/// Error: wrong number of arguments for 'MQ POP' command.
pub const ERR_MQ_POP_ARGS: &[u8] = b"ERR wrong number of arguments for 'MQ POP' command";

/// Error: wrong number of arguments for 'MQ ACK' command.
pub const ERR_MQ_ACK_ARGS: &[u8] = b"ERR wrong number of arguments for 'MQ ACK' command";

/// Error: wrong number of arguments for 'MQ DLQLEN' command.
pub const ERR_MQ_DLQLEN_ARGS: &[u8] = b"ERR wrong number of arguments for 'MQ DLQLEN' command";

/// Error: wrong number of arguments for 'MQ TRIGGER' command.
pub const ERR_MQ_TRIGGER_ARGS: &[u8] = b"ERR wrong number of arguments for 'MQ TRIGGER' command";

/// Error: wrong number of arguments for 'MQ PUBLISH' command.
pub const ERR_MQ_PUBLISH_ARGS: &[u8] = b"ERR wrong number of arguments for 'MQ PUBLISH' command";

/// Error: unknown MQ subcommand.
pub const ERR_MQ_UNKNOWN_SUB: &[u8] = b"ERR unknown MQ subcommand";

/// Error: stream is not a durable queue.
pub const ERR_MQ_NOT_DURABLE: &[u8] = b"ERR stream is not a durable queue";

/// Error: invalid message ID format.
pub const ERR_MQ_INVALID_ID: &[u8] = b"ERR invalid message ID format";

/// Error: invalid COUNT value.
pub const ERR_MQ_INVALID_COUNT: &[u8] = b"ERR invalid COUNT value";

/// Error: invalid MAXDELIVERY value.
pub const ERR_MQ_INVALID_MAXDELIVERY: &[u8] = b"ERR invalid MAXDELIVERY value";

/// Error: invalid DEBOUNCE value.
pub const ERR_MQ_INVALID_DEBOUNCE: &[u8] = b"ERR invalid DEBOUNCE value";

/// Parse a stream ID from bytes in "ms-seq" format.
///
/// Returns `None` on any malformed input.
fn parse_stream_id(data: &[u8]) -> Option<(u64, u64)> {
    let s = std::str::from_utf8(data).ok()?;
    let dash_pos = s.find('-')?;
    let ms = s[..dash_pos].parse::<u64>().ok()?;
    let seq = s[dash_pos + 1..].parse::<u64>().ok()?;
    Some((ms, seq))
}

/// Extract `BulkString` data from a `Frame`, returning an error if wrong type.
#[inline]
fn extract_bulk<'a>(frame: &'a Frame, err: &'static [u8]) -> Result<&'a Bytes, Frame> {
    match frame {
        Frame::BulkString(data) => Ok(data),
        _ => Err(Frame::Error(Bytes::from_static(err))),
    }
}

/// Parse the MQ subcommand from args.
///
/// Returns the subcommand bytes (e.g., `b"CREATE"`) or an error Frame.
pub fn parse_mq_subcommand(args: &[Frame]) -> Result<&[u8], Frame> {
    if args.is_empty() {
        return Err(Frame::Error(Bytes::from_static(ERR_MQ_ARGS)));
    }
    match &args[0] {
        Frame::BulkString(data) => Ok(data),
        _ => Err(Frame::Error(Bytes::from_static(ERR_MQ_ARGS))),
    }
}

/// Validate MQ CREATE arguments.
///
/// Expected: `MQ CREATE <key> [MAXDELIVERY <n>] [DEBOUNCE <ms>]`
/// args = [CREATE, key, ...optional keyword-value pairs].
///
/// Returns `(queue_key, max_delivery_count, debounce_ms)`.
/// Defaults: max_delivery_count=3, debounce_ms=0 (disabled).
pub fn validate_mq_create(args: &[Frame]) -> Result<(Bytes, u32, u64), Frame> {
    // Minimum: [CREATE, key] = 2 args
    if args.len() < 2 {
        return Err(Frame::Error(Bytes::from_static(ERR_MQ_CREATE_ARGS)));
    }
    let key = extract_bulk(&args[1], ERR_MQ_CREATE_ARGS)?.clone();

    let mut max_delivery_count: u32 = 3;
    let mut debounce_ms: u64 = 0;

    // Parse optional keyword-value pairs
    let mut i = 2;
    while i + 1 < args.len() {
        let keyword = extract_bulk(&args[i], ERR_MQ_CREATE_ARGS)?;
        let value = extract_bulk(&args[i + 1], ERR_MQ_CREATE_ARGS)?;

        if keyword.eq_ignore_ascii_case(b"MAXDELIVERY") {
            let s = std::str::from_utf8(value)
                .map_err(|_| Frame::Error(Bytes::from_static(ERR_MQ_INVALID_MAXDELIVERY)))?;
            max_delivery_count = s
                .parse::<u32>()
                .map_err(|_| Frame::Error(Bytes::from_static(ERR_MQ_INVALID_MAXDELIVERY)))?;
        } else if keyword.eq_ignore_ascii_case(b"DEBOUNCE") {
            let s = std::str::from_utf8(value)
                .map_err(|_| Frame::Error(Bytes::from_static(ERR_MQ_INVALID_DEBOUNCE)))?;
            debounce_ms = s
                .parse::<u64>()
                .map_err(|_| Frame::Error(Bytes::from_static(ERR_MQ_INVALID_DEBOUNCE)))?;
        } else {
            return Err(Frame::Error(Bytes::from_static(ERR_MQ_CREATE_ARGS)));
        }
        i += 2;
    }

    // Odd trailing arg (keyword without value)
    if i < args.len() {
        return Err(Frame::Error(Bytes::from_static(ERR_MQ_CREATE_ARGS)));
    }

    Ok((key, max_delivery_count, debounce_ms))
}

/// Validate MQ PUSH arguments.
///
/// Expected: `MQ PUSH <key> <f1> <v1> [f2 v2 ...]`
/// args = [PUSH, key, f1, v1, ...]. Must have >= 4 args and even field/value pairs after key.
///
/// Returns `(queue_key, fields)`.
pub fn validate_mq_push(args: &[Frame]) -> Result<(Bytes, Vec<(Bytes, Bytes)>), Frame> {
    // Minimum: [PUSH, key, f1, v1] = 4 args
    if args.len() < 4 {
        return Err(Frame::Error(Bytes::from_static(ERR_MQ_PUSH_ARGS)));
    }
    let key = extract_bulk(&args[1], ERR_MQ_PUSH_ARGS)?.clone();

    // field/value pairs start at args[2]
    let pairs_slice = &args[2..];
    if pairs_slice.len() % 2 != 0 {
        return Err(Frame::Error(Bytes::from_static(ERR_MQ_PUSH_ARGS)));
    }

    let mut fields = Vec::with_capacity(pairs_slice.len() / 2);
    for chunk in pairs_slice.chunks(2) {
        let field = extract_bulk(&chunk[0], ERR_MQ_PUSH_ARGS)?.clone();
        let value = extract_bulk(&chunk[1], ERR_MQ_PUSH_ARGS)?.clone();
        fields.push((field, value));
    }

    Ok((key, fields))
}

/// Validate MQ POP arguments.
///
/// Expected: `MQ POP <key> [COUNT <n>]`
/// args = [POP, key, ...optional]. Defaults: count=1.
///
/// Returns `(queue_key, count)`.
pub fn validate_mq_pop(args: &[Frame]) -> Result<(Bytes, usize), Frame> {
    // Minimum: [POP, key] = 2 args
    if args.len() < 2 {
        return Err(Frame::Error(Bytes::from_static(ERR_MQ_POP_ARGS)));
    }
    let key = extract_bulk(&args[1], ERR_MQ_POP_ARGS)?.clone();

    let mut count: usize = 1;

    // Parse optional COUNT keyword-value pair
    if args.len() >= 4 {
        let keyword = extract_bulk(&args[2], ERR_MQ_POP_ARGS)?;
        if keyword.eq_ignore_ascii_case(b"COUNT") {
            let value = extract_bulk(&args[3], ERR_MQ_POP_ARGS)?;
            let s = std::str::from_utf8(value)
                .map_err(|_| Frame::Error(Bytes::from_static(ERR_MQ_INVALID_COUNT)))?;
            count = s
                .parse::<usize>()
                .map_err(|_| Frame::Error(Bytes::from_static(ERR_MQ_INVALID_COUNT)))?;
            if count == 0 {
                return Err(Frame::Error(Bytes::from_static(ERR_MQ_INVALID_COUNT)));
            }
        } else {
            return Err(Frame::Error(Bytes::from_static(ERR_MQ_POP_ARGS)));
        }
    }

    // Reject unexpected extra args
    if args.len() > 4 {
        return Err(Frame::Error(Bytes::from_static(ERR_MQ_POP_ARGS)));
    }
    // Reject incomplete keyword (key only, no value)
    if args.len() == 3 {
        return Err(Frame::Error(Bytes::from_static(ERR_MQ_POP_ARGS)));
    }

    Ok((key, count))
}

/// Validate MQ ACK arguments.
///
/// Expected: `MQ ACK <key> <id1> [id2 ...]`
/// args = [ACK, key, id1, ...]. Must have >= 3 args.
///
/// Returns `(queue_key, vec of (ms, seq) tuples)`.
pub fn validate_mq_ack(args: &[Frame]) -> Result<(Bytes, Vec<(u64, u64)>), Frame> {
    // Minimum: [ACK, key, id1] = 3 args
    if args.len() < 3 {
        return Err(Frame::Error(Bytes::from_static(ERR_MQ_ACK_ARGS)));
    }
    let key = extract_bulk(&args[1], ERR_MQ_ACK_ARGS)?.clone();

    let mut ids = Vec::with_capacity(args.len() - 2);
    for arg in &args[2..] {
        let id_data = extract_bulk(arg, ERR_MQ_INVALID_ID)?;
        let (ms, seq) = parse_stream_id(id_data)
            .ok_or_else(|| Frame::Error(Bytes::from_static(ERR_MQ_INVALID_ID)))?;
        ids.push((ms, seq));
    }

    Ok((key, ids))
}

/// Validate MQ DLQLEN arguments.
///
/// Expected: `MQ DLQLEN <key>`
/// args = [DLQLEN, key]. Returns queue_key.
pub fn validate_mq_dlqlen(args: &[Frame]) -> Result<Bytes, Frame> {
    if args.len() != 2 {
        return Err(Frame::Error(Bytes::from_static(ERR_MQ_DLQLEN_ARGS)));
    }
    let key = extract_bulk(&args[1], ERR_MQ_DLQLEN_ARGS)?.clone();
    Ok(key)
}

/// Validate MQ TRIGGER arguments.
///
/// Expected: `MQ TRIGGER <key> <callback_cmd> [DEBOUNCE <ms>]`
/// args = [TRIGGER, key, callback_cmd, ...optional]. Default debounce_ms=1000.
///
/// Returns `(queue_key, callback_cmd, debounce_ms)`.
pub fn validate_mq_trigger(args: &[Frame]) -> Result<(Bytes, Bytes, u64), Frame> {
    // Minimum: [TRIGGER, key, callback] = 3 args
    if args.len() < 3 {
        return Err(Frame::Error(Bytes::from_static(ERR_MQ_TRIGGER_ARGS)));
    }
    let key = extract_bulk(&args[1], ERR_MQ_TRIGGER_ARGS)?.clone();
    let callback_cmd = extract_bulk(&args[2], ERR_MQ_TRIGGER_ARGS)?.clone();

    let mut debounce_ms: u64 = 1000;

    // Parse optional DEBOUNCE keyword-value pair
    if args.len() >= 5 {
        let keyword = extract_bulk(&args[3], ERR_MQ_TRIGGER_ARGS)?;
        if keyword.eq_ignore_ascii_case(b"DEBOUNCE") {
            let value = extract_bulk(&args[4], ERR_MQ_TRIGGER_ARGS)?;
            let s = std::str::from_utf8(value)
                .map_err(|_| Frame::Error(Bytes::from_static(ERR_MQ_INVALID_DEBOUNCE)))?;
            debounce_ms = s
                .parse::<u64>()
                .map_err(|_| Frame::Error(Bytes::from_static(ERR_MQ_INVALID_DEBOUNCE)))?;
        } else {
            return Err(Frame::Error(Bytes::from_static(ERR_MQ_TRIGGER_ARGS)));
        }
    }

    // Reject unexpected extra args or incomplete keyword
    if args.len() == 4 || args.len() > 5 {
        return Err(Frame::Error(Bytes::from_static(ERR_MQ_TRIGGER_ARGS)));
    }

    Ok((key, callback_cmd, debounce_ms))
}

/// Validate MQ PUBLISH arguments.
///
/// Expected: `MQ PUBLISH <key> <f1> <v1> [f2 v2 ...]`
/// args = [PUBLISH, key, f1, v1, ...]. Same structure as PUSH.
///
/// Returns `(queue_key, fields)`.
pub fn validate_mq_publish(args: &[Frame]) -> Result<(Bytes, Vec<(Bytes, Bytes)>), Frame> {
    // Minimum: [PUBLISH, key, f1, v1] = 4 args
    if args.len() < 4 {
        return Err(Frame::Error(Bytes::from_static(ERR_MQ_PUBLISH_ARGS)));
    }
    let key = extract_bulk(&args[1], ERR_MQ_PUBLISH_ARGS)?.clone();

    // field/value pairs start at args[2]
    let pairs_slice = &args[2..];
    if pairs_slice.len() % 2 != 0 {
        return Err(Frame::Error(Bytes::from_static(ERR_MQ_PUBLISH_ARGS)));
    }

    let mut fields = Vec::with_capacity(pairs_slice.len() / 2);
    for chunk in pairs_slice.chunks(2) {
        let field = extract_bulk(&chunk[0], ERR_MQ_PUBLISH_ARGS)?.clone();
        let value = extract_bulk(&chunk[1], ERR_MQ_PUBLISH_ARGS)?.clone();
        fields.push((field, value));
    }

    Ok((key, fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Helper ---

    fn bs(data: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(data))
    }

    // --- parse_stream_id ---

    #[test]
    fn test_parse_stream_id_valid() {
        assert_eq!(parse_stream_id(b"1234-5"), Some((1234, 5)));
        assert_eq!(parse_stream_id(b"0-0"), Some((0, 0)));
        assert_eq!(
            parse_stream_id(b"1713394800000-42"),
            Some((1713394800000, 42))
        );
    }

    #[test]
    fn test_parse_stream_id_invalid() {
        assert!(parse_stream_id(b"").is_none());
        assert!(parse_stream_id(b"1234").is_none());
        assert!(parse_stream_id(b"-1234").is_none());
        assert!(parse_stream_id(b"abc-def").is_none());
        assert!(parse_stream_id(b"1234-").is_none());
        assert!(parse_stream_id(b"-").is_none());
    }

    // --- parse_mq_subcommand ---

    #[test]
    fn test_parse_mq_subcommand_empty() {
        assert!(parse_mq_subcommand(&[]).is_err());
    }

    #[test]
    fn test_parse_mq_subcommand_valid() {
        let args = [bs(b"CREATE")];
        let sub = parse_mq_subcommand(&args).unwrap();
        assert_eq!(sub, b"CREATE");
    }

    #[test]
    fn test_parse_mq_subcommand_wrong_type() {
        let args = [Frame::Integer(42)];
        assert!(parse_mq_subcommand(&args).is_err());
    }

    // --- validate_mq_create ---

    #[test]
    fn test_validate_mq_create_minimal() {
        let args = [bs(b"CREATE"), bs(b"orders")];
        let (key, mdc, debounce) = validate_mq_create(&args).unwrap();
        assert_eq!(key.as_ref(), b"orders");
        assert_eq!(mdc, 3); // default
        assert_eq!(debounce, 0); // default
    }

    #[test]
    fn test_validate_mq_create_with_maxdelivery() {
        let args = [bs(b"CREATE"), bs(b"q"), bs(b"MAXDELIVERY"), bs(b"10")];
        let (_, mdc, _) = validate_mq_create(&args).unwrap();
        assert_eq!(mdc, 10);
    }

    #[test]
    fn test_validate_mq_create_with_debounce() {
        let args = [bs(b"CREATE"), bs(b"q"), bs(b"DEBOUNCE"), bs(b"5000")];
        let (_, _, debounce) = validate_mq_create(&args).unwrap();
        assert_eq!(debounce, 5000);
    }

    #[test]
    fn test_validate_mq_create_with_both_options() {
        let args = [
            bs(b"CREATE"),
            bs(b"q"),
            bs(b"MAXDELIVERY"),
            bs(b"7"),
            bs(b"DEBOUNCE"),
            bs(b"2000"),
        ];
        let (key, mdc, debounce) = validate_mq_create(&args).unwrap();
        assert_eq!(key.as_ref(), b"q");
        assert_eq!(mdc, 7);
        assert_eq!(debounce, 2000);
    }

    #[test]
    fn test_validate_mq_create_case_insensitive() {
        let args = [bs(b"CREATE"), bs(b"q"), bs(b"maxdelivery"), bs(b"5")];
        let (_, mdc, _) = validate_mq_create(&args).unwrap();
        assert_eq!(mdc, 5);
    }

    #[test]
    fn test_validate_mq_create_missing_key() {
        let args = [bs(b"CREATE")];
        assert!(validate_mq_create(&args).is_err());
    }

    #[test]
    fn test_validate_mq_create_invalid_maxdelivery() {
        let args = [bs(b"CREATE"), bs(b"q"), bs(b"MAXDELIVERY"), bs(b"abc")];
        let err = validate_mq_create(&args).unwrap_err();
        match err {
            Frame::Error(msg) => assert_eq!(msg.as_ref(), ERR_MQ_INVALID_MAXDELIVERY),
            _ => panic!("expected Error frame"),
        }
    }

    #[test]
    fn test_validate_mq_create_invalid_debounce() {
        let args = [bs(b"CREATE"), bs(b"q"), bs(b"DEBOUNCE"), bs(b"-1")];
        assert!(validate_mq_create(&args).is_err());
    }

    #[test]
    fn test_validate_mq_create_unknown_keyword() {
        let args = [bs(b"CREATE"), bs(b"q"), bs(b"UNKNOWN"), bs(b"5")];
        assert!(validate_mq_create(&args).is_err());
    }

    #[test]
    fn test_validate_mq_create_odd_trailing() {
        let args = [bs(b"CREATE"), bs(b"q"), bs(b"MAXDELIVERY")];
        assert!(validate_mq_create(&args).is_err());
    }

    // --- validate_mq_push ---

    #[test]
    fn test_validate_mq_push_valid() {
        let args = [bs(b"PUSH"), bs(b"q"), bs(b"name"), bs(b"alice")];
        let (key, fields) = validate_mq_push(&args).unwrap();
        assert_eq!(key.as_ref(), b"q");
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].0.as_ref(), b"name");
        assert_eq!(fields[0].1.as_ref(), b"alice");
    }

    #[test]
    fn test_validate_mq_push_multiple_fields() {
        let args = [
            bs(b"PUSH"),
            bs(b"q"),
            bs(b"f1"),
            bs(b"v1"),
            bs(b"f2"),
            bs(b"v2"),
        ];
        let (_, fields) = validate_mq_push(&args).unwrap();
        assert_eq!(fields.len(), 2);
    }

    #[test]
    fn test_validate_mq_push_missing_args() {
        // Only [PUSH, key] -- no field/value
        let args = [bs(b"PUSH"), bs(b"q")];
        assert!(validate_mq_push(&args).is_err());
    }

    #[test]
    fn test_validate_mq_push_odd_field_count() {
        // [PUSH, key, f1, v1, f2] -- odd field/value count
        let args = [bs(b"PUSH"), bs(b"q"), bs(b"f1"), bs(b"v1"), bs(b"f2")];
        assert!(validate_mq_push(&args).is_err());
    }

    #[test]
    fn test_validate_mq_push_wrong_type() {
        let args = [bs(b"PUSH"), Frame::Integer(42), bs(b"f"), bs(b"v")];
        assert!(validate_mq_push(&args).is_err());
    }

    // --- validate_mq_pop ---

    #[test]
    fn test_validate_mq_pop_default_count() {
        let args = [bs(b"POP"), bs(b"q")];
        let (key, count) = validate_mq_pop(&args).unwrap();
        assert_eq!(key.as_ref(), b"q");
        assert_eq!(count, 1);
    }

    #[test]
    fn test_validate_mq_pop_with_count() {
        let args = [bs(b"POP"), bs(b"q"), bs(b"COUNT"), bs(b"10")];
        let (_, count) = validate_mq_pop(&args).unwrap();
        assert_eq!(count, 10);
    }

    #[test]
    fn test_validate_mq_pop_count_zero() {
        let args = [bs(b"POP"), bs(b"q"), bs(b"COUNT"), bs(b"0")];
        assert!(validate_mq_pop(&args).is_err());
    }

    #[test]
    fn test_validate_mq_pop_invalid_count() {
        let args = [bs(b"POP"), bs(b"q"), bs(b"COUNT"), bs(b"abc")];
        let err = validate_mq_pop(&args).unwrap_err();
        match err {
            Frame::Error(msg) => assert_eq!(msg.as_ref(), ERR_MQ_INVALID_COUNT),
            _ => panic!("expected Error frame"),
        }
    }

    #[test]
    fn test_validate_mq_pop_missing_key() {
        let args = [bs(b"POP")];
        assert!(validate_mq_pop(&args).is_err());
    }

    #[test]
    fn test_validate_mq_pop_extra_args() {
        let args = [bs(b"POP"), bs(b"q"), bs(b"COUNT"), bs(b"5"), bs(b"extra")];
        assert!(validate_mq_pop(&args).is_err());
    }

    #[test]
    fn test_validate_mq_pop_incomplete_keyword() {
        let args = [bs(b"POP"), bs(b"q"), bs(b"COUNT")];
        assert!(validate_mq_pop(&args).is_err());
    }

    #[test]
    fn test_validate_mq_pop_unknown_keyword() {
        let args = [bs(b"POP"), bs(b"q"), bs(b"TIMEOUT"), bs(b"5000")];
        assert!(validate_mq_pop(&args).is_err());
    }

    // --- validate_mq_ack ---

    #[test]
    fn test_validate_mq_ack_single_id() {
        let args = [bs(b"ACK"), bs(b"q"), bs(b"1234-0")];
        let (key, ids) = validate_mq_ack(&args).unwrap();
        assert_eq!(key.as_ref(), b"q");
        assert_eq!(ids, vec![(1234, 0)]);
    }

    #[test]
    fn test_validate_mq_ack_multiple_ids() {
        let args = [
            bs(b"ACK"),
            bs(b"q"),
            bs(b"100-0"),
            bs(b"200-1"),
            bs(b"300-2"),
        ];
        let (_, ids) = validate_mq_ack(&args).unwrap();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[0], (100, 0));
        assert_eq!(ids[1], (200, 1));
        assert_eq!(ids[2], (300, 2));
    }

    #[test]
    fn test_validate_mq_ack_missing_id() {
        let args = [bs(b"ACK"), bs(b"q")];
        assert!(validate_mq_ack(&args).is_err());
    }

    #[test]
    fn test_validate_mq_ack_invalid_id_format() {
        let args = [bs(b"ACK"), bs(b"q"), bs(b"not-a-number")];
        let err = validate_mq_ack(&args).unwrap_err();
        match err {
            Frame::Error(msg) => assert_eq!(msg.as_ref(), ERR_MQ_INVALID_ID),
            _ => panic!("expected Error frame"),
        }
    }

    #[test]
    fn test_validate_mq_ack_id_without_dash() {
        let args = [bs(b"ACK"), bs(b"q"), bs(b"12345")];
        assert!(validate_mq_ack(&args).is_err());
    }

    #[test]
    fn test_validate_mq_ack_wrong_type() {
        let args = [bs(b"ACK"), bs(b"q"), Frame::Integer(42)];
        assert!(validate_mq_ack(&args).is_err());
    }

    // --- validate_mq_dlqlen ---

    #[test]
    fn test_validate_mq_dlqlen_valid() {
        let args = [bs(b"DLQLEN"), bs(b"orders")];
        let key = validate_mq_dlqlen(&args).unwrap();
        assert_eq!(key.as_ref(), b"orders");
    }

    #[test]
    fn test_validate_mq_dlqlen_missing_key() {
        let args = [bs(b"DLQLEN")];
        assert!(validate_mq_dlqlen(&args).is_err());
    }

    #[test]
    fn test_validate_mq_dlqlen_extra_args() {
        let args = [bs(b"DLQLEN"), bs(b"q"), bs(b"extra")];
        assert!(validate_mq_dlqlen(&args).is_err());
    }

    #[test]
    fn test_validate_mq_dlqlen_wrong_type() {
        let args = [bs(b"DLQLEN"), Frame::Integer(42)];
        assert!(validate_mq_dlqlen(&args).is_err());
    }

    // --- validate_mq_trigger ---

    #[test]
    fn test_validate_mq_trigger_minimal() {
        let args = [bs(b"TRIGGER"), bs(b"q"), bs(b"PUBLISH mq:events new")];
        let (key, callback, debounce) = validate_mq_trigger(&args).unwrap();
        assert_eq!(key.as_ref(), b"q");
        assert_eq!(callback.as_ref(), b"PUBLISH mq:events new");
        assert_eq!(debounce, 1000); // default
    }

    #[test]
    fn test_validate_mq_trigger_with_debounce() {
        let args = [
            bs(b"TRIGGER"),
            bs(b"q"),
            bs(b"CMD"),
            bs(b"DEBOUNCE"),
            bs(b"5000"),
        ];
        let (_, _, debounce) = validate_mq_trigger(&args).unwrap();
        assert_eq!(debounce, 5000);
    }

    #[test]
    fn test_validate_mq_trigger_missing_callback() {
        let args = [bs(b"TRIGGER"), bs(b"q")];
        assert!(validate_mq_trigger(&args).is_err());
    }

    #[test]
    fn test_validate_mq_trigger_incomplete_debounce() {
        // [TRIGGER, key, callback, DEBOUNCE] -- missing value
        let args = [bs(b"TRIGGER"), bs(b"q"), bs(b"CMD"), bs(b"DEBOUNCE")];
        assert!(validate_mq_trigger(&args).is_err());
    }

    #[test]
    fn test_validate_mq_trigger_extra_args() {
        let args = [
            bs(b"TRIGGER"),
            bs(b"q"),
            bs(b"CMD"),
            bs(b"DEBOUNCE"),
            bs(b"1000"),
            bs(b"extra"),
        ];
        assert!(validate_mq_trigger(&args).is_err());
    }

    #[test]
    fn test_validate_mq_trigger_invalid_debounce() {
        let args = [
            bs(b"TRIGGER"),
            bs(b"q"),
            bs(b"CMD"),
            bs(b"DEBOUNCE"),
            bs(b"not_a_number"),
        ];
        assert!(validate_mq_trigger(&args).is_err());
    }

    // --- validate_mq_publish ---

    #[test]
    fn test_validate_mq_publish_valid() {
        let args = [bs(b"PUBLISH"), bs(b"q"), bs(b"field"), bs(b"value")];
        let (key, fields) = validate_mq_publish(&args).unwrap();
        assert_eq!(key.as_ref(), b"q");
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].0.as_ref(), b"field");
        assert_eq!(fields[0].1.as_ref(), b"value");
    }

    #[test]
    fn test_validate_mq_publish_multiple_fields() {
        let args = [
            bs(b"PUBLISH"),
            bs(b"q"),
            bs(b"a"),
            bs(b"1"),
            bs(b"b"),
            bs(b"2"),
            bs(b"c"),
            bs(b"3"),
        ];
        let (_, fields) = validate_mq_publish(&args).unwrap();
        assert_eq!(fields.len(), 3);
    }

    #[test]
    fn test_validate_mq_publish_missing_args() {
        let args = [bs(b"PUBLISH"), bs(b"q")];
        assert!(validate_mq_publish(&args).is_err());
    }

    #[test]
    fn test_validate_mq_publish_odd_fields() {
        let args = [bs(b"PUBLISH"), bs(b"q"), bs(b"f1"), bs(b"v1"), bs(b"f2")];
        assert!(validate_mq_publish(&args).is_err());
    }

    #[test]
    fn test_validate_mq_publish_wrong_type() {
        let args = [bs(b"PUBLISH"), Frame::Integer(1), bs(b"f"), bs(b"v")];
        assert!(validate_mq_publish(&args).is_err());
    }
}
