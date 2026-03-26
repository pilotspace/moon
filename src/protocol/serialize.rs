use bytes::{BufMut, BytesMut};

use super::frame::Frame;

/// Serialize a Frame into RESP2 wire format, appending to the buffer.
pub fn serialize(frame: &Frame, buf: &mut BytesMut) {
    match frame {
        Frame::SimpleString(s) => {
            buf.put_u8(b'+');
            buf.put_slice(s);
            buf.put_slice(b"\r\n");
        }
        Frame::Error(s) => {
            buf.put_u8(b'-');
            buf.put_slice(s);
            buf.put_slice(b"\r\n");
        }
        Frame::Integer(n) => {
            buf.put_u8(b':');
            let mut itoa_buf = itoa::Buffer::new();
            buf.put_slice(itoa_buf.format(*n).as_bytes());
            buf.put_slice(b"\r\n");
        }
        Frame::BulkString(data) => {
            buf.put_u8(b'$');
            let mut itoa_buf = itoa::Buffer::new();
            buf.put_slice(itoa_buf.format(data.len()).as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(data);
            buf.put_slice(b"\r\n");
        }
        Frame::Array(items) => {
            buf.put_u8(b'*');
            let mut itoa_buf = itoa::Buffer::new();
            buf.put_slice(itoa_buf.format(items.len()).as_bytes());
            buf.put_slice(b"\r\n");
            for item in items {
                serialize(item, buf);
            }
        }
        Frame::Null => {
            buf.put_slice(b"$-1\r\n");
        }
        // RESP3 types downgraded to RESP2 format
        Frame::Map(entries) => {
            // Downgrade: flat array [k1, v1, k2, v2, ...]
            buf.put_u8(b'*');
            let mut itoa_buf = itoa::Buffer::new();
            buf.put_slice(itoa_buf.format(entries.len() * 2).as_bytes());
            buf.put_slice(b"\r\n");
            for (key, value) in entries {
                serialize(key, buf);
                serialize(value, buf);
            }
        }
        Frame::Set(items) | Frame::Push(items) => {
            // Downgrade: serialize as Array
            buf.put_u8(b'*');
            let mut itoa_buf = itoa::Buffer::new();
            buf.put_slice(itoa_buf.format(items.len()).as_bytes());
            buf.put_slice(b"\r\n");
            for item in items {
                serialize(item, buf);
            }
        }
        Frame::Double(f) => {
            // Downgrade: BulkString of formatted float
            let s = if f.is_infinite() {
                if f.is_sign_positive() {
                    "inf".to_string()
                } else {
                    "-inf".to_string()
                }
            } else if f.is_nan() {
                "nan".to_string()
            } else {
                format!("{}", f)
            };
            buf.put_u8(b'$');
            let mut itoa_buf = itoa::Buffer::new();
            buf.put_slice(itoa_buf.format(s.len()).as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
        }
        Frame::Boolean(b) => {
            // Downgrade: Integer 1 or 0
            buf.put_u8(b':');
            buf.put_slice(if *b { b"1" } else { b"0" });
            buf.put_slice(b"\r\n");
        }
        Frame::VerbatimString { data, .. } => {
            // Downgrade: BulkString of data (drop encoding hint)
            buf.put_u8(b'$');
            let mut itoa_buf = itoa::Buffer::new();
            buf.put_slice(itoa_buf.format(data.len()).as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(data);
            buf.put_slice(b"\r\n");
        }
        Frame::BigNumber(n) => {
            // Downgrade: BulkString of number bytes
            buf.put_u8(b'$');
            let mut itoa_buf = itoa::Buffer::new();
            buf.put_slice(itoa_buf.format(n.len()).as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(n);
            buf.put_slice(b"\r\n");
        }
        Frame::PreSerialized(data) => {
            // Already contains complete RESP wire format -- write directly
            buf.put_slice(data);
        }
    }
}

/// Serialize a Frame into RESP3 wire format, appending to the buffer.
///
/// RESP2 types serialize identically in RESP3 (except Null which uses `_\r\n`).
/// New RESP3 types use their native wire format.
pub fn serialize_resp3(frame: &Frame, buf: &mut BytesMut) {
    match frame {
        Frame::SimpleString(s) => {
            buf.put_u8(b'+');
            buf.put_slice(s);
            buf.put_slice(b"\r\n");
        }
        Frame::Error(s) => {
            buf.put_u8(b'-');
            buf.put_slice(s);
            buf.put_slice(b"\r\n");
        }
        Frame::Integer(n) => {
            buf.put_u8(b':');
            let mut itoa_buf = itoa::Buffer::new();
            buf.put_slice(itoa_buf.format(*n).as_bytes());
            buf.put_slice(b"\r\n");
        }
        Frame::BulkString(data) => {
            buf.put_u8(b'$');
            let mut itoa_buf = itoa::Buffer::new();
            buf.put_slice(itoa_buf.format(data.len()).as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(data);
            buf.put_slice(b"\r\n");
        }
        Frame::Array(items) => {
            buf.put_u8(b'*');
            let mut itoa_buf = itoa::Buffer::new();
            buf.put_slice(itoa_buf.format(items.len()).as_bytes());
            buf.put_slice(b"\r\n");
            for item in items {
                serialize_resp3(item, buf);
            }
        }
        // RESP3 Null uses `_\r\n` instead of `$-1\r\n`
        Frame::Null => {
            buf.put_slice(b"_\r\n");
        }
        Frame::Boolean(b) => {
            buf.put_u8(b'#');
            buf.put_u8(if *b { b't' } else { b'f' });
            buf.put_slice(b"\r\n");
        }
        Frame::Double(f) => {
            buf.put_u8(b',');
            if f.is_infinite() {
                if f.is_sign_positive() {
                    buf.put_slice(b"inf");
                } else {
                    buf.put_slice(b"-inf");
                }
            } else if f.is_nan() {
                buf.put_slice(b"nan");
            } else {
                let s = format!("{}", f);
                buf.put_slice(s.as_bytes());
            }
            buf.put_slice(b"\r\n");
        }
        Frame::BigNumber(n) => {
            buf.put_u8(b'(');
            buf.put_slice(n);
            buf.put_slice(b"\r\n");
        }
        Frame::VerbatimString { encoding, data } => {
            buf.put_u8(b'=');
            let total_len = encoding.len() + 1 + data.len();
            let mut itoa_buf = itoa::Buffer::new();
            buf.put_slice(itoa_buf.format(total_len).as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(encoding);
            buf.put_u8(b':');
            buf.put_slice(data);
            buf.put_slice(b"\r\n");
        }
        Frame::Map(entries) => {
            buf.put_u8(b'%');
            let mut itoa_buf = itoa::Buffer::new();
            buf.put_slice(itoa_buf.format(entries.len()).as_bytes());
            buf.put_slice(b"\r\n");
            for (key, value) in entries {
                serialize_resp3(key, buf);
                serialize_resp3(value, buf);
            }
        }
        Frame::Set(items) => {
            buf.put_u8(b'~');
            let mut itoa_buf = itoa::Buffer::new();
            buf.put_slice(itoa_buf.format(items.len()).as_bytes());
            buf.put_slice(b"\r\n");
            for item in items {
                serialize_resp3(item, buf);
            }
        }
        Frame::Push(items) => {
            buf.put_u8(b'>');
            let mut itoa_buf = itoa::Buffer::new();
            buf.put_slice(itoa_buf.format(items.len()).as_bytes());
            buf.put_slice(b"\r\n");
            for item in items {
                serialize_resp3(item, buf);
            }
        }
        Frame::PreSerialized(data) => {
            // Already contains complete RESP wire format -- write directly
            buf.put_slice(data);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::framevec;

    use super::super::frame::ParseConfig;
    use super::super::parse;

    fn serialize_frame(frame: &Frame) -> BytesMut {
        let mut buf = BytesMut::new();
        serialize(frame, &mut buf);
        buf
    }

    // === Direct serialization tests ===

    #[test]
    fn test_serialize_simple_string() {
        let buf = serialize_frame(&Frame::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(&buf[..], b"+OK\r\n");
    }

    #[test]
    fn test_serialize_error() {
        let buf = serialize_frame(&Frame::Error(Bytes::from_static(b"ERR bad")));
        assert_eq!(&buf[..], b"-ERR bad\r\n");
    }

    #[test]
    fn test_serialize_integer_positive() {
        let buf = serialize_frame(&Frame::Integer(42));
        assert_eq!(&buf[..], b":42\r\n");
    }

    #[test]
    fn test_serialize_integer_negative() {
        let buf = serialize_frame(&Frame::Integer(-1));
        assert_eq!(&buf[..], b":-1\r\n");
    }

    #[test]
    fn test_serialize_integer_zero() {
        let buf = serialize_frame(&Frame::Integer(0));
        assert_eq!(&buf[..], b":0\r\n");
    }

    #[test]
    fn test_serialize_bulk_string() {
        let buf = serialize_frame(&Frame::BulkString(Bytes::from_static(b"hello")));
        assert_eq!(&buf[..], b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_serialize_empty_bulk_string() {
        let buf = serialize_frame(&Frame::BulkString(Bytes::new()));
        assert_eq!(&buf[..], b"$0\r\n\r\n");
    }

    #[test]
    fn test_serialize_null() {
        let buf = serialize_frame(&Frame::Null);
        assert_eq!(&buf[..], b"$-1\r\n");
    }

    #[test]
    fn test_serialize_empty_array() {
        let buf = serialize_frame(&Frame::Array(framevec![]));
        assert_eq!(&buf[..], b"*0\r\n");
    }

    #[test]
    fn test_serialize_array_of_bulk_strings() {
        let frame = Frame::Array(framevec![
            Frame::BulkString(Bytes::from_static(b"foo")),
            Frame::BulkString(Bytes::from_static(b"bar")),
        ]);
        let buf = serialize_frame(&frame);
        assert_eq!(&buf[..], b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    }

    #[test]
    fn test_serialize_nested_array() {
        let frame = Frame::Array(framevec![Frame::Array(framevec![Frame::Integer(1)])]);
        let buf = serialize_frame(&frame);
        assert_eq!(&buf[..], b"*1\r\n*1\r\n:1\r\n");
    }

    // === Round-trip tests ===

    fn round_trip(frame: &Frame) {
        let mut buf = BytesMut::new();
        serialize(frame, &mut buf);
        let parsed = parse::parse(&mut buf, &ParseConfig::default())
            .unwrap()
            .unwrap();
        assert_eq!(&parsed, frame);
    }

    #[test]
    fn test_round_trip_simple_string() {
        round_trip(&Frame::SimpleString(Bytes::from_static(b"OK")));
    }

    #[test]
    fn test_round_trip_error() {
        round_trip(&Frame::Error(Bytes::from_static(b"ERR something went wrong")));
    }

    #[test]
    fn test_round_trip_integer() {
        round_trip(&Frame::Integer(42));
        round_trip(&Frame::Integer(-100));
        round_trip(&Frame::Integer(0));
        round_trip(&Frame::Integer(i64::MAX));
        round_trip(&Frame::Integer(i64::MIN));
    }

    #[test]
    fn test_round_trip_bulk_string() {
        round_trip(&Frame::BulkString(Bytes::from_static(b"hello world")));
        round_trip(&Frame::BulkString(Bytes::new())); // empty
        round_trip(&Frame::BulkString(Bytes::from_static(b"\r\n\r\n"))); // binary data
    }

    #[test]
    fn test_round_trip_null() {
        round_trip(&Frame::Null);
    }

    #[test]
    fn test_round_trip_array() {
        round_trip(&Frame::Array(framevec![])); // empty
        round_trip(&Frame::Array(framevec![
            Frame::BulkString(Bytes::from_static(b"SET")),
            Frame::BulkString(Bytes::from_static(b"key")),
            Frame::BulkString(Bytes::from_static(b"value")),
        ]));
    }

    #[test]
    fn test_round_trip_nested_array() {
        round_trip(&Frame::Array(framevec![
            Frame::Array(framevec![Frame::Integer(1), Frame::Integer(2)]),
            Frame::Array(framevec![Frame::Integer(3), Frame::Integer(4)]),
        ]));
    }

    #[test]
    fn test_round_trip_mixed_array_with_null() {
        round_trip(&Frame::Array(framevec![
            Frame::BulkString(Bytes::from_static(b"hello")),
            Frame::Null,
            Frame::Integer(42),
            Frame::SimpleString(Bytes::from_static(b"OK")),
            Frame::Error(Bytes::from_static(b"ERR")),
            Frame::Array(framevec![Frame::Null, Frame::Integer(-1)]),
        ]));
    }

    // === RESP3 serialize_resp3 direct tests ===

    fn serialize_resp3_frame(frame: &Frame) -> BytesMut {
        let mut buf = BytesMut::new();
        super::serialize_resp3(frame, &mut buf);
        buf
    }

    #[test]
    fn test_serialize_resp3_null() {
        let buf = serialize_resp3_frame(&Frame::Null);
        assert_eq!(&buf[..], b"_\r\n");
    }

    #[test]
    fn test_serialize_resp3_boolean_true() {
        let buf = serialize_resp3_frame(&Frame::Boolean(true));
        assert_eq!(&buf[..], b"#t\r\n");
    }

    #[test]
    fn test_serialize_resp3_boolean_false() {
        let buf = serialize_resp3_frame(&Frame::Boolean(false));
        assert_eq!(&buf[..], b"#f\r\n");
    }

    #[test]
    fn test_serialize_resp3_double() {
        let buf = serialize_resp3_frame(&Frame::Double(1.23));
        assert_eq!(&buf[..], b",1.23\r\n");
    }

    #[test]
    fn test_serialize_resp3_double_inf() {
        let buf = serialize_resp3_frame(&Frame::Double(f64::INFINITY));
        assert_eq!(&buf[..], b",inf\r\n");
    }

    #[test]
    fn test_serialize_resp3_double_neg_inf() {
        let buf = serialize_resp3_frame(&Frame::Double(f64::NEG_INFINITY));
        assert_eq!(&buf[..], b",-inf\r\n");
    }

    #[test]
    fn test_serialize_resp3_big_number() {
        let buf = serialize_resp3_frame(&Frame::BigNumber(Bytes::from_static(b"12345")));
        assert_eq!(&buf[..], b"(12345\r\n");
    }

    #[test]
    fn test_serialize_resp3_verbatim_string() {
        let buf = serialize_resp3_frame(&Frame::VerbatimString {
            encoding: Bytes::from_static(b"txt"),
            data: Bytes::from_static(b"hello"),
        });
        assert_eq!(&buf[..], b"=9\r\ntxt:hello\r\n");
    }

    #[test]
    fn test_serialize_resp3_map() {
        let buf = serialize_resp3_frame(&Frame::Map(vec![(
            Frame::SimpleString(Bytes::from_static(b"key")),
            Frame::Integer(1),
        )]));
        assert_eq!(&buf[..], b"%1\r\n+key\r\n:1\r\n");
    }

    #[test]
    fn test_serialize_resp3_set() {
        let buf = serialize_resp3_frame(&Frame::Set(framevec![
            Frame::SimpleString(Bytes::from_static(b"a")),
            Frame::SimpleString(Bytes::from_static(b"b")),
        ]));
        assert_eq!(&buf[..], b"~2\r\n+a\r\n+b\r\n");
    }

    #[test]
    fn test_serialize_resp3_push() {
        let buf = serialize_resp3_frame(&Frame::Push(framevec![
            Frame::SimpleString(Bytes::from_static(b"a")),
            Frame::SimpleString(Bytes::from_static(b"b")),
        ]));
        assert_eq!(&buf[..], b">2\r\n+a\r\n+b\r\n");
    }

    // === RESP3 round-trip tests ===

    fn round_trip_resp3(frame: &Frame) {
        let mut buf = BytesMut::new();
        super::serialize_resp3(frame, &mut buf);
        let parsed = parse::parse(&mut buf, &ParseConfig::default())
            .unwrap()
            .unwrap();
        assert_eq!(&parsed, frame);
    }

    #[test]
    fn test_round_trip_resp3_null() {
        round_trip_resp3(&Frame::Null);
    }

    #[test]
    fn test_round_trip_resp3_boolean() {
        round_trip_resp3(&Frame::Boolean(true));
        round_trip_resp3(&Frame::Boolean(false));
    }

    #[test]
    fn test_round_trip_resp3_double() {
        round_trip_resp3(&Frame::Double(1.23));
        round_trip_resp3(&Frame::Double(f64::INFINITY));
        round_trip_resp3(&Frame::Double(f64::NEG_INFINITY));
        round_trip_resp3(&Frame::Double(0.0));
        round_trip_resp3(&Frame::Double(-42.5));
    }

    #[test]
    fn test_round_trip_resp3_big_number() {
        round_trip_resp3(&Frame::BigNumber(Bytes::from_static(
            b"3492890328409238509324850943850943825024385",
        )));
    }

    #[test]
    fn test_round_trip_resp3_verbatim_string() {
        round_trip_resp3(&Frame::VerbatimString {
            encoding: Bytes::from_static(b"txt"),
            data: Bytes::from_static(b"Some string"),
        });
    }

    #[test]
    fn test_round_trip_resp3_map() {
        round_trip_resp3(&Frame::Map(vec![
            (
                Frame::SimpleString(Bytes::from_static(b"key1")),
                Frame::Integer(1),
            ),
            (
                Frame::SimpleString(Bytes::from_static(b"key2")),
                Frame::Integer(2),
            ),
        ]));
    }

    #[test]
    fn test_round_trip_resp3_set() {
        round_trip_resp3(&Frame::Set(framevec![
            Frame::SimpleString(Bytes::from_static(b"a")),
            Frame::SimpleString(Bytes::from_static(b"b")),
            Frame::SimpleString(Bytes::from_static(b"c")),
        ]));
    }

    #[test]
    fn test_round_trip_resp3_push() {
        round_trip_resp3(&Frame::Push(framevec![
            Frame::BulkString(Bytes::from_static(b"invalidate")),
            Frame::Array(framevec![Frame::BulkString(Bytes::from_static(b"foo"))]),
        ]));
    }

    // === RESP2 downgrade tests ===

    #[test]
    fn test_resp2_downgrade_map_to_flat_array() {
        let frame = Frame::Map(vec![
            (
                Frame::SimpleString(Bytes::from_static(b"k1")),
                Frame::Integer(1),
            ),
            (
                Frame::SimpleString(Bytes::from_static(b"k2")),
                Frame::Integer(2),
            ),
        ]);
        let mut buf = BytesMut::new();
        serialize(&frame, &mut buf);
        let parsed = parse::parse(&mut buf, &ParseConfig::default())
            .unwrap()
            .unwrap();
        assert_eq!(
            parsed,
            Frame::Array(framevec![
                Frame::SimpleString(Bytes::from_static(b"k1")),
                Frame::Integer(1),
                Frame::SimpleString(Bytes::from_static(b"k2")),
                Frame::Integer(2),
            ])
        );
    }

    #[test]
    fn test_resp2_downgrade_set_to_array() {
        let frame = Frame::Set(framevec![
            Frame::SimpleString(Bytes::from_static(b"a")),
            Frame::SimpleString(Bytes::from_static(b"b")),
        ]);
        let mut buf = BytesMut::new();
        serialize(&frame, &mut buf);
        let parsed = parse::parse(&mut buf, &ParseConfig::default())
            .unwrap()
            .unwrap();
        assert_eq!(
            parsed,
            Frame::Array(framevec![
                Frame::SimpleString(Bytes::from_static(b"a")),
                Frame::SimpleString(Bytes::from_static(b"b")),
            ])
        );
    }

    #[test]
    fn test_resp2_downgrade_boolean_to_integer() {
        let buf = serialize_frame(&Frame::Boolean(true));
        assert_eq!(&buf[..], b":1\r\n");
        let buf = serialize_frame(&Frame::Boolean(false));
        assert_eq!(&buf[..], b":0\r\n");
    }

    #[test]
    fn test_resp2_downgrade_double_to_bulk_string() {
        let buf = serialize_frame(&Frame::Double(1.5));
        assert_eq!(&buf[..], b"$3\r\n1.5\r\n");
    }

    #[test]
    fn test_resp2_null_still_dollar_minus_one() {
        let buf = serialize_frame(&Frame::Null);
        assert_eq!(&buf[..], b"$-1\r\n");
    }
}
