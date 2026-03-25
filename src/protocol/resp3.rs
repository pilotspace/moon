
use super::Frame;

/// Convert a dispatch response to RESP3 types based on command name.
/// Only applies when protocol_version >= 3. Commands not in the table pass through unchanged.
/// The command name bytes must be uppercase for matching.
pub fn maybe_convert_resp3(cmd: &[u8], response: Frame, proto: u8) -> Frame {
    if proto < 3 {
        return response;
    }
    // Error responses always pass through unchanged
    if matches!(&response, Frame::Error(_)) {
        return response;
    }
    // Null responses pass through unchanged
    if matches!(&response, Frame::Null) {
        return response;
    }

    match cmd {
        // Map responses (flat array -> map)
        b"HGETALL" | b"CONFIG" | b"HRANDFIELD" | b"ZRANDMEMBER" => array_to_map(response),
        // Set responses (array -> set)
        b"SMEMBERS" | b"SINTER" | b"SUNION" | b"SDIFF" => array_to_set(response),
        // Boolean responses (integer 0/1 -> boolean)
        // Note: EXISTS is intentionally excluded -- multi-key EXISTS returns a count > 1
        // which should remain Integer. Single-key EXISTS would need arg-count awareness.
        b"SISMEMBER" | b"HEXISTS" | b"EXPIRE" | b"PEXPIRE" | b"PERSIST" | b"SETNX"
        | b"MSETNX" => int_to_bool(response),
        // Double responses (bulk string -> double)
        b"ZSCORE" | b"ZINCRBY" | b"INCRBYFLOAT" | b"HINCRBYFLOAT" => bulk_to_double(response),
        _ => response,
    }
}

pub fn array_to_map(frame: Frame) -> Frame {
    match frame {
        Frame::Array(items) if items.len() % 2 == 0 && !items.is_empty() => {
            let mut pairs = Vec::with_capacity(items.len() / 2);
            let mut iter = items.into_iter();
            while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
                pairs.push((k, v));
            }
            Frame::Map(pairs)
        }
        other => other,
    }
}

pub fn array_to_set(frame: Frame) -> Frame {
    match frame {
        Frame::Array(items) => Frame::Set(items),
        other => other,
    }
}

pub fn int_to_bool(frame: Frame) -> Frame {
    match frame {
        Frame::Integer(n) => Frame::Boolean(n != 0),
        other => other,
    }
}

pub fn bulk_to_double(frame: Frame) -> Frame {
    match frame {
        Frame::BulkString(ref s) => {
            if let Ok(text) = std::str::from_utf8(s) {
                if let Ok(f) = text.parse::<f64>() {
                    return Frame::Double(f);
                }
            }
            frame
        }
        Frame::Null => Frame::Null, // ZSCORE of nonexistent member
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_array_to_map() {
        let arr = Frame::Array(vec![
            Frame::BulkString(Bytes::from_static(b"k1")),
            Frame::BulkString(Bytes::from_static(b"v1")),
            Frame::BulkString(Bytes::from_static(b"k2")),
            Frame::BulkString(Bytes::from_static(b"v2")),
        ]);
        let result = array_to_map(arr);
        assert_eq!(
            result,
            Frame::Map(vec![
                (
                    Frame::BulkString(Bytes::from_static(b"k1")),
                    Frame::BulkString(Bytes::from_static(b"v1")),
                ),
                (
                    Frame::BulkString(Bytes::from_static(b"k2")),
                    Frame::BulkString(Bytes::from_static(b"v2")),
                ),
            ])
        );
    }

    #[test]
    fn test_array_to_map_empty_passthrough() {
        let arr = Frame::Array(vec![]);
        let result = array_to_map(arr.clone());
        assert_eq!(result, Frame::Array(vec![]));
    }

    #[test]
    fn test_array_to_set() {
        let arr = Frame::Array(vec![
            Frame::BulkString(Bytes::from_static(b"a")),
            Frame::BulkString(Bytes::from_static(b"b")),
            Frame::BulkString(Bytes::from_static(b"c")),
        ]);
        let result = array_to_set(arr);
        assert_eq!(
            result,
            Frame::Set(vec![
                Frame::BulkString(Bytes::from_static(b"a")),
                Frame::BulkString(Bytes::from_static(b"b")),
                Frame::BulkString(Bytes::from_static(b"c")),
            ])
        );
    }

    #[test]
    fn test_int_to_bool() {
        assert_eq!(int_to_bool(Frame::Integer(1)), Frame::Boolean(true));
        assert_eq!(int_to_bool(Frame::Integer(0)), Frame::Boolean(false));
        assert_eq!(int_to_bool(Frame::Integer(5)), Frame::Boolean(true));
    }

    #[test]
    fn test_bulk_to_double() {
        assert_eq!(
            bulk_to_double(Frame::BulkString(Bytes::from_static(b"1.5"))),
            Frame::Double(1.5)
        );
        assert_eq!(
            bulk_to_double(Frame::BulkString(Bytes::from_static(b"42"))),
            Frame::Double(42.0)
        );
        // Non-numeric passthrough
        let non_num = Frame::BulkString(Bytes::from_static(b"notanumber"));
        assert_eq!(
            bulk_to_double(non_num.clone()),
            non_num
        );
    }

    #[test]
    fn test_bulk_to_double_null() {
        assert_eq!(bulk_to_double(Frame::Null), Frame::Null);
    }

    #[test]
    fn test_maybe_convert_hgetall_resp3() {
        let arr = Frame::Array(vec![
            Frame::BulkString(Bytes::from_static(b"k")),
            Frame::BulkString(Bytes::from_static(b"v")),
        ]);
        let result = maybe_convert_resp3(b"HGETALL", arr, 3);
        assert_eq!(
            result,
            Frame::Map(vec![(
                Frame::BulkString(Bytes::from_static(b"k")),
                Frame::BulkString(Bytes::from_static(b"v")),
            )])
        );
    }

    #[test]
    fn test_maybe_convert_hgetall_resp2_unchanged() {
        let arr = Frame::Array(vec![
            Frame::BulkString(Bytes::from_static(b"k")),
            Frame::BulkString(Bytes::from_static(b"v")),
        ]);
        let result = maybe_convert_resp3(b"HGETALL", arr.clone(), 2);
        assert_eq!(result, arr);
    }

    #[test]
    fn test_maybe_convert_smembers_resp3() {
        let arr = Frame::Array(vec![
            Frame::BulkString(Bytes::from_static(b"a")),
            Frame::BulkString(Bytes::from_static(b"b")),
        ]);
        let result = maybe_convert_resp3(b"SMEMBERS", arr, 3);
        assert_eq!(
            result,
            Frame::Set(vec![
                Frame::BulkString(Bytes::from_static(b"a")),
                Frame::BulkString(Bytes::from_static(b"b")),
            ])
        );
    }

    #[test]
    fn test_maybe_convert_sismember_resp3() {
        assert_eq!(
            maybe_convert_resp3(b"SISMEMBER", Frame::Integer(1), 3),
            Frame::Boolean(true)
        );
        assert_eq!(
            maybe_convert_resp3(b"SISMEMBER", Frame::Integer(0), 3),
            Frame::Boolean(false)
        );
    }

    #[test]
    fn test_maybe_convert_zscore_resp3() {
        assert_eq!(
            maybe_convert_resp3(b"ZSCORE", Frame::BulkString(Bytes::from_static(b"1.5")), 3),
            Frame::Double(1.5)
        );
    }

    #[test]
    fn test_maybe_convert_get_unchanged() {
        let val = Frame::BulkString(Bytes::from_static(b"val"));
        assert_eq!(
            maybe_convert_resp3(b"GET", val.clone(), 3),
            val
        );
    }

    #[test]
    fn test_maybe_convert_error_passthrough() {
        let err = Frame::Error(Bytes::from_static(b"ERR something"));
        assert_eq!(
            maybe_convert_resp3(b"HGETALL", err.clone(), 3),
            err
        );
    }

    #[test]
    fn test_maybe_convert_null_passthrough() {
        assert_eq!(
            maybe_convert_resp3(b"ZSCORE", Frame::Null, 3),
            Frame::Null
        );
    }
}
