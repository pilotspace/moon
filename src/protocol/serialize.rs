use bytes::{BufMut, BytesMut};

use super::frame::Frame;

/// Stack buffer holding the `Display` output of an `f64`.
///
/// `{}` formatting of `f64` never uses scientific notation, so the longest
/// output is a subnormal like `5e-324`: `-0.` + 323 zeros + up to 17
/// significant digits ≈ 343 bytes. 400 leaves comfortable margin.
/// Produces byte-identical output to `format!("{}", f)` without the heap
/// allocation (response serialization is a hot path).
struct F64Display {
    buf: [u8; 400],
    len: usize,
}

impl F64Display {
    #[inline]
    fn format(f: f64) -> Self {
        use std::fmt::Write;
        let mut this = F64Display {
            buf: [0u8; 400],
            len: 0,
        };
        // Cannot fail: buffer is sized for the worst-case f64 Display output
        // (see struct doc). On the impossible overflow, output is truncated
        // rather than panicking.
        let _ = write!(this, "{f}");
        this
    }

    #[inline]
    fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

impl std::fmt::Write for F64Display {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let bytes = s.as_bytes();
        let end = self.len + bytes.len();
        let Some(dst) = self.buf.get_mut(self.len..end) else {
            return Err(std::fmt::Error);
        };
        dst.copy_from_slice(bytes);
        self.len = end;
        Ok(())
    }
}

/// Write one line-framed reply -- `+status`, `-error`, `(bignumber` -- as
/// `<prefix><payload>\r\n`, with every CR and LF in `payload` written as a
/// space.
///
/// A line-framed reply ends at the first `\r\n`, so an unescaped CR/LF lets
/// whoever controls the payload end the reply early and append replies the
/// server never sent: error texts quote client input (an unknown command
/// name, an `ACL SETUSER` rule), and a client that pipelines would then read
/// the injected tail as the answer to its NEXT command. Redis does the same
/// mapping for error text (`addReplyErrorFormat`: `sdsmapchars(s, "\r\n",
/// "  ", 2)`); doing it here, where every line-framed type is written, covers
/// every producer instead of each call site that happens to quote input.
///
/// Allocation-free: a payload with no CR/LF (every reply in practice) is one
/// `memchr2` scan and one copy, as before; otherwise the runs between
/// offending bytes are copied straight into `buf`.
#[inline]
pub fn put_line(buf: &mut BytesMut, prefix: u8, payload: &[u8]) {
    buf.reserve(payload.len() + 3);
    buf.put_u8(prefix);
    let mut start = 0;
    for i in memchr::memchr2_iter(b'\r', b'\n', payload) {
        buf.put_slice(&payload[start..i]);
        buf.put_u8(b' ');
        start = i + 1;
    }
    buf.put_slice(&payload[start..]);
    buf.put_slice(b"\r\n");
}

/// Serialize a Frame into RESP2 wire format, appending to the buffer.
pub fn serialize(frame: &Frame, buf: &mut BytesMut) {
    match frame {
        Frame::SimpleString(s) => put_line(buf, b'+', s),
        Frame::Error(s) => put_line(buf, b'-', s),
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
        // RESP2 has TWO nulls. `$-1` above is the missing STRING; `*-1` here is
        // the missing ARRAY (BLPOP timeout, aborted EXEC, GEOPOS of an absent
        // member). A typed client decodes them differently — moon#482.
        Frame::NullArray => {
            buf.put_slice(b"*-1\r\n");
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
            // Downgrade: BulkString of formatted float (zero-alloc: stack
            // buffer for finite values, static slices for inf/nan)
            let formatted;
            let s: &[u8] = if f.is_infinite() {
                if f.is_sign_positive() {
                    b"inf"
                } else {
                    b"-inf"
                }
            } else if f.is_nan() {
                b"nan"
            } else {
                formatted = F64Display::format(*f);
                formatted.as_bytes()
            };
            buf.put_u8(b'$');
            let mut itoa_buf = itoa::Buffer::new();
            buf.put_slice(itoa_buf.format(s.len()).as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(s);
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
        Frame::SimpleString(s) => put_line(buf, b'+', s),
        Frame::Error(s) => put_line(buf, b'-', s),
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
        // RESP3 has ONE null for both shapes — verified against redis-server
        // 8.6.1, where `BLPOP` timeout and `GET` miss both answer `_\r\n`.
        // That is why the divergence moon#482 fixes is RESP2-only.
        Frame::NullArray => {
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
                // Zero-alloc: stack buffer, byte-identical to format!("{}", f)
                buf.put_slice(F64Display::format(*f).as_bytes());
            }
            buf.put_slice(b"\r\n");
        }
        Frame::BigNumber(n) => put_line(buf, b'(', n),
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
    use crate::framevec;
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
    fn test_serialize_null_array_resp2_is_star_minus_one() {
        // RESP2's two nulls. `$-1` (Frame::Null) says the missing value is a
        // STRING; `*-1` says it is an ARRAY. moon#482.
        assert_eq!(&serialize_frame(&Frame::NullArray)[..], b"*-1\r\n");
        assert_eq!(&serialize_frame(&Frame::Null)[..], b"$-1\r\n");
    }

    #[test]
    fn test_serialize_null_array_resp3_collapses_to_underscore() {
        // RESP3 has ONE null for both shapes — verified against redis-server
        // 8.6.1, which answers `_\r\n` to both a BLPOP timeout and a GET miss.
        assert_eq!(&serialize_resp3_frame(&Frame::NullArray)[..], b"_\r\n");
        assert_eq!(&serialize_resp3_frame(&Frame::Null)[..], b"_\r\n");
    }

    #[test]
    fn test_serialize_null_array_nests_inside_an_array() {
        // GEOPOS of an absent member is `*1\r\n*-1\r\n` — the variant has to
        // be composable, not a top-level-reply special case.
        let f = Frame::Array(framevec![Frame::NullArray]);
        assert_eq!(&serialize_frame(&f)[..], b"*1\r\n*-1\r\n");

        // And the contrast in the same shape: GEOHASH's miss stays a bulk.
        let g = Frame::Array(framevec![Frame::Null]);
        assert_eq!(&serialize_frame(&g)[..], b"*1\r\n$-1\r\n");
    }

    #[test]
    fn test_null_array_is_not_an_empty_array() {
        // `*-1` (no array) and `*0` (an array with nothing in it) are
        // different replies; conflating them is the other half of moon#482.
        assert_eq!(&serialize_frame(&Frame::NullArray)[..], b"*-1\r\n");
        assert_eq!(&serialize_frame(&Frame::Array(framevec![]))[..], b"*0\r\n");
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
        round_trip(&Frame::Error(Bytes::from_static(
            b"ERR something went wrong",
        )));
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

    // === F64Display stack formatter: must match format!("{}", f) exactly ===

    #[test]
    fn test_f64_display_matches_std_format() {
        // Includes the worst-case Display lengths: f64::MAX (~309 digits)
        // and the smallest subnormal (~326 chars), which guard the stack
        // buffer capacity in F64Display.
        // Test data literal picked for its 16 significant digits, not for its
        // mathematical identity — not a candidate for f64::consts::PI.
        #[allow(clippy::approx_constant)]
        let cases = [
            0.0,
            -0.0,
            1.5,
            -42.5,
            1.23,
            3.141592653589793,
            f64::MAX,
            f64::MIN,
            f64::MIN_POSITIVE,
            5e-324, // smallest subnormal — longest Display output
            1e308,
            -1e-308,
        ];
        for f in cases {
            let expected = format!("{}", f);
            let got = F64Display::format(f);
            assert_eq!(
                got.as_bytes(),
                expected.as_bytes(),
                "F64Display mismatch for {f:?}"
            );
        }
    }

    #[test]
    fn test_serialize_double_extreme_values_resp2_and_resp3() {
        for f in [f64::MAX, 5e-324, -1e-308] {
            let s = format!("{}", f);
            let buf = serialize_frame(&Frame::Double(f));
            let expected = format!("${}\r\n{}\r\n", s.len(), s);
            assert_eq!(&buf[..], expected.as_bytes());

            let buf3 = serialize_resp3_frame(&Frame::Double(f));
            let expected3 = format!(",{}\r\n", s);
            assert_eq!(&buf3[..], expected3.as_bytes());
        }
    }

    #[test]
    fn test_resp2_null_still_dollar_minus_one() {
        let buf = serialize_frame(&Frame::Null);
        assert_eq!(&buf[..], b"$-1\r\n");
    }

    /// A line-framed reply (`-err`, `+status`, `(bignum`) cannot carry CR or
    /// LF: the first `\r\n` ends the reply and the rest parses as a second
    /// reply the server never sent. Redis maps both bytes to spaces in error
    /// text; the expected bytes are redis-server 8.6.1's for the same input.
    #[test]
    fn test_line_replies_never_carry_cr_or_lf() {
        let cases: &[(&[u8], &[u8])] = &[
            (
                b"ERR Error in ACL SETUSER modifier '-@bogus\r\n+INJECTED': Unknown command or category name in ACL",
                b"ERR Error in ACL SETUSER modifier '-@bogus  +INJECTED': Unknown command or category name in ACL",
            ),
            (
                b"ERR unknown command 'foo\r\n+INJ2'",
                b"ERR unknown command 'foo  +INJ2'",
            ),
            (
                b"ERR unknown command '\r\nfoo\r\n'",
                b"ERR unknown command '  foo  '",
            ),
            (
                b"ERR unknown subcommand 'x\ry\nz'. Try CONFIG HELP.",
                b"ERR unknown subcommand 'x y z'. Try CONFIG HELP.",
            ),
            (b"\r\n", b"  "),
            (b"\n\n\n", b"   "),
            (b"", b""),
            (b"ERR plain", b"ERR plain"),
        ];
        for (raw, clean) in cases {
            let raw = Bytes::copy_from_slice(raw);
            for (prefix, frame) in [
                (b'-', Frame::Error(raw.clone())),
                (b'+', Frame::SimpleString(raw.clone())),
            ] {
                let mut expected = vec![prefix];
                expected.extend_from_slice(clean);
                expected.extend_from_slice(b"\r\n");
                assert_eq!(&serialize_frame(&frame)[..], &expected[..], "RESP2 {raw:?}");
                assert_eq!(
                    &serialize_resp3_frame(&frame)[..],
                    &expected[..],
                    "RESP3 {raw:?}"
                );
            }
            let mut expected = b"(".to_vec();
            expected.extend_from_slice(clean);
            expected.extend_from_slice(b"\r\n");
            let big = Frame::BigNumber(raw.clone());
            assert_eq!(
                &serialize_resp3_frame(&big)[..],
                &expected[..],
                "BigNumber {raw:?}"
            );
        }
    }

    /// Whatever bytes an error carries, its serialized reply parses back as
    /// exactly ONE frame that consumes the whole buffer -- the property the
    /// injection broke. Every single-byte payload, in both protocols, and
    /// nested inside an array.
    #[test]
    fn test_any_error_payload_is_exactly_one_frame() {
        let cfg = ParseConfig::default();
        let mut payloads: Vec<Vec<u8>> = (0u8..=255).map(|b| vec![b'E', b, b'x']).collect();
        payloads.push(b"a\r\n-b\r\n+c\r\n:1\r\n".to_vec());
        payloads.push(b"\r\r\n\n\r".to_vec());
        for p in payloads {
            let frame = Frame::Error(Bytes::from(p.clone()));
            for wire in [serialize_frame(&frame), serialize_resp3_frame(&frame)] {
                let mut buf = wire.clone();
                let parsed = parse::parse(&mut buf, &cfg)
                    .unwrap_or_else(|e| panic!("{p:?}: parse error {e:?}"))
                    .unwrap_or_else(|| panic!("{p:?}: incomplete"));
                assert!(matches!(parsed, Frame::Error(_)), "{p:?}");
                assert!(buf.is_empty(), "{p:?}: {} trailing bytes", buf.len());
            }
            let arr = Frame::Array(framevec![frame.clone(), Frame::Integer(7)]);
            let mut buf = serialize_frame(&arr);
            let parsed = parse::parse(&mut buf, &cfg).ok().flatten();
            assert!(
                matches!(parsed, Some(Frame::Array(ref v)) if v.len() == 2),
                "{p:?}"
            );
            assert!(buf.is_empty(), "{p:?}");
        }
    }
}
