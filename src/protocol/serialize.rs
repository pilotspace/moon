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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

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
        let buf = serialize_frame(&Frame::Array(vec![]));
        assert_eq!(&buf[..], b"*0\r\n");
    }

    #[test]
    fn test_serialize_array_of_bulk_strings() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Bytes::from_static(b"foo")),
            Frame::BulkString(Bytes::from_static(b"bar")),
        ]);
        let buf = serialize_frame(&frame);
        assert_eq!(&buf[..], b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    }

    #[test]
    fn test_serialize_nested_array() {
        let frame = Frame::Array(vec![Frame::Array(vec![Frame::Integer(1)])]);
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
        round_trip(&Frame::Array(vec![])); // empty
        round_trip(&Frame::Array(vec![
            Frame::BulkString(Bytes::from_static(b"SET")),
            Frame::BulkString(Bytes::from_static(b"key")),
            Frame::BulkString(Bytes::from_static(b"value")),
        ]));
    }

    #[test]
    fn test_round_trip_nested_array() {
        round_trip(&Frame::Array(vec![
            Frame::Array(vec![Frame::Integer(1), Frame::Integer(2)]),
            Frame::Array(vec![Frame::Integer(3), Frame::Integer(4)]),
        ]));
    }

    #[test]
    fn test_round_trip_mixed_array_with_null() {
        round_trip(&Frame::Array(vec![
            Frame::BulkString(Bytes::from_static(b"hello")),
            Frame::Null,
            Frame::Integer(42),
            Frame::SimpleString(Bytes::from_static(b"OK")),
            Frame::Error(Bytes::from_static(b"ERR")),
            Frame::Array(vec![Frame::Null, Frame::Integer(-1)]),
        ]));
    }
}
