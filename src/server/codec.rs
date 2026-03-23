use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use crate::protocol::{self, Frame, ParseConfig, ParseError};

/// RESP2 codec wrapping the Phase 1 parser and serializer.
///
/// Implements tokio-util's `Decoder` and `Encoder` traits so it can be used
/// with `Framed<TcpStream, RespCodec>` for stream-oriented frame I/O.
pub struct RespCodec {
    config: ParseConfig,
}

impl RespCodec {
    pub fn new(config: ParseConfig) -> Self {
        Self { config }
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
        protocol::serialize(&item, dst);
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
}
