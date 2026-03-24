use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use crate::protocol::{self, Frame, ParseConfig, ParseError};

/// RESP2/RESP3 codec wrapping the parser and dual serializers.
///
/// Implements tokio-util's `Decoder` and `Encoder` traits so it can be used
/// with `Framed<TcpStream, RespCodec>` for stream-oriented frame I/O.
///
/// The `protocol_version` field controls serialization format:
/// - 2 (default): RESP2 wire format via `serialize()`
/// - 3: RESP3 wire format via `serialize_resp3()`
///
/// Decoding is version-agnostic: the parser handles both RESP2 and RESP3 frames
/// since clients always send commands as RESP2 arrays regardless of protocol.
pub struct RespCodec {
    config: ParseConfig,
    protocol_version: u8,
}

impl RespCodec {
    pub fn new(config: ParseConfig) -> Self {
        Self {
            config,
            protocol_version: 2,
        }
    }

    pub fn set_protocol_version(&mut self, version: u8) {
        self.protocol_version = version;
    }

    pub fn protocol_version(&self) -> u8 {
        self.protocol_version
    }
}

impl Default for RespCodec {
    fn default() -> Self {
        Self::new(ParseConfig::default())
    }
}

impl Decoder for RespCodec {
    type Item = Frame;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, Self::Error> {
        match protocol::parse(src, &self.config) {
            Ok(frame) => Ok(frame),
            Err(ParseError::Incomplete) => Ok(None),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        }
    }
}

impl Encoder<Frame> for RespCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if self.protocol_version >= 3 {
            protocol::serialize_resp3(&item, dst);
        } else {
            protocol::serialize(&item, dst);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_decode_simple_string() {
        let mut codec = RespCodec::default();
        let mut buf = BytesMut::from(&b"+OK\r\n"[..]);
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_incomplete() {
        let mut codec = RespCodec::default();
        let mut buf = BytesMut::from(&b"+OK"[..]);
        let result = codec.decode(&mut buf).unwrap();
        assert!(result.is_none());
        // Buffer should be unchanged
        assert_eq!(&buf[..], b"+OK");
    }

    #[test]
    fn test_encode_simple_string() {
        let mut codec = RespCodec::default();
        let mut buf = BytesMut::new();
        codec
            .encode(Frame::SimpleString(Bytes::from_static(b"OK")), &mut buf)
            .unwrap();
        assert_eq!(&buf[..], b"+OK\r\n");
    }

    #[test]
    fn test_roundtrip_bulk_string() {
        let mut codec = RespCodec::default();
        let original = Frame::BulkString(Bytes::from_static(b"hello world"));

        // Encode
        let mut buf = BytesMut::new();
        codec.encode(original.clone(), &mut buf).unwrap();

        // Decode
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, original);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_array_command() {
        let mut codec = RespCodec::default();
        let mut buf = BytesMut::from(&b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n"[..]);
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::BulkString(Bytes::from_static(b"PING")),
                Frame::BulkString(Bytes::from_static(b"hello")),
            ])
        );
    }

    #[test]
    fn test_decode_invalid_data() {
        let config = ParseConfig {
            max_bulk_string_size: 100,
            ..ParseConfig::default()
        };
        let mut codec = RespCodec::new(config);
        let mut buf = BytesMut::from(&b"$999999999\r\n"[..]);
        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_protocol_version_default() {
        let codec = RespCodec::default();
        assert_eq!(codec.protocol_version(), 2);
    }

    #[test]
    fn test_set_protocol_version() {
        let mut codec = RespCodec::default();
        codec.set_protocol_version(3);
        assert_eq!(codec.protocol_version(), 3);
        codec.set_protocol_version(2);
        assert_eq!(codec.protocol_version(), 2);
    }

    #[test]
    fn test_encode_map_resp2_flat_array() {
        let mut codec = RespCodec::default(); // protocol_version=2
        let map = Frame::Map(vec![(
            Frame::BulkString(Bytes::from_static(b"key")),
            Frame::BulkString(Bytes::from_static(b"val")),
        )]);
        let mut buf = BytesMut::new();
        codec.encode(map, &mut buf).unwrap();
        // RESP2 downgrades Map to flat Array
        assert_eq!(&buf[..], b"*2\r\n$3\r\nkey\r\n$3\r\nval\r\n");
    }

    #[test]
    fn test_encode_map_resp3_native() {
        let mut codec = RespCodec::default();
        codec.set_protocol_version(3);
        let map = Frame::Map(vec![(
            Frame::BulkString(Bytes::from_static(b"key")),
            Frame::BulkString(Bytes::from_static(b"val")),
        )]);
        let mut buf = BytesMut::new();
        codec.encode(map, &mut buf).unwrap();
        // RESP3 uses native Map format with % prefix
        assert_eq!(&buf[..], b"%1\r\n$3\r\nkey\r\n$3\r\nval\r\n");
    }

    #[test]
    fn test_encode_null_resp2() {
        let mut codec = RespCodec::default(); // protocol_version=2
        let mut buf = BytesMut::new();
        codec.encode(Frame::Null, &mut buf).unwrap();
        assert_eq!(&buf[..], b"$-1\r\n");
    }

    #[test]
    fn test_encode_null_resp3() {
        let mut codec = RespCodec::default();
        codec.set_protocol_version(3);
        let mut buf = BytesMut::new();
        codec.encode(Frame::Null, &mut buf).unwrap();
        assert_eq!(&buf[..], b"_\r\n");
    }
}
