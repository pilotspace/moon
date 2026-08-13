use bytes::BytesMut;
#[cfg(feature = "runtime-tokio")]
use tokio_util::codec::{Decoder, Encoder};

use crate::protocol::{self, Frame, ParseConfig, ParseError};

/// RESP2/RESP3 codec wrapping the parser and dual serializers.
///
/// When `runtime-tokio` is active, implements tokio-util's `Decoder` and `Encoder`
/// traits for use with `Framed<TcpStream, RespCodec>`.
///
/// Standalone `decode_frame` and `encode_frame` methods are always available
/// for runtime-agnostic usage (e.g., monoio manual buffer management).
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
    /// c10k C2: per-connection query-buffer ceiling in bytes (0 = unlimited).
    ///
    /// Enforced here rather than in the handler because `Framed::next()` does
    /// not yield between reads while a frame is still incomplete — it loops
    /// read -> decode -> read internally, so a handler-side check would never
    /// run during the growth that matters. `decode` is the one place that
    /// sees the buffer on every pass.
    max_query_buf: usize,
    /// Fault from the last failed decode — see `take_last_fault`. The
    /// `Decoder` trait's `io::Error` cannot carry it, and a bare close leaves
    /// the client unable to tell a bad encoder from a dropped network.
    last_fault: Option<crate::protocol::ProtoFault>,
}

impl RespCodec {
    pub fn new(config: ParseConfig) -> Self {
        Self {
            config,
            protocol_version: 2,
            max_query_buf: 0,
            last_fault: None,
        }
    }

    /// Set the query-buffer ceiling (0 = unlimited). Callers raise this from
    /// the pre-auth value to the full one when a connection authenticates.
    pub fn set_max_query_buf(&mut self, max: usize) {
        self.max_query_buf = max;
    }

    pub fn set_protocol_version(&mut self, version: u8) {
        self.protocol_version = version;
    }

    pub fn protocol_version(&self) -> u8 {
        self.protocol_version
    }

    /// Decode a frame from the buffer (runtime-agnostic).
    ///
    /// Returns `Ok(Some(frame))` on success, `Ok(None)` if incomplete,
    /// or `Err` on parse error.
    pub fn decode_frame(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, std::io::Error> {
        if self.max_query_buf != 0 && src.len() > self.max_query_buf {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "query buffer limit reached",
            ));
        }
        match protocol::parse(src, &self.config) {
            Ok(frame) => Ok(frame),
            Err(ParseError::Incomplete) => Ok(None),
            Err(e) => {
                // `Decoder` pins the error type to `io::Error`, which cannot
                // carry the fault kind — and the kind is exactly what the
                // client needs to be told. Park it here for the handler to
                // pick up; it is read immediately after this returns Err, so
                // there is no window in which it can go stale.
                if let ParseError::Invalid { kind, .. } = &e {
                    self.last_fault = Some(*kind);
                }
                Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            }
        }
    }

    /// The fault from the most recent failed [`Self::decode_frame`], taken.
    ///
    /// `None` means the read failed for a non-protocol reason (a dead socket,
    /// the query-buffer ceiling) — in which case there is nothing useful to
    /// tell the client and the connection just closes.
    pub fn take_last_fault(&mut self) -> Option<crate::protocol::ProtoFault> {
        self.last_fault.take()
    }

    /// Encode a frame into the buffer (runtime-agnostic).
    ///
    /// Serializes using RESP2 or RESP3 format based on `protocol_version`.
    pub fn encode_frame(&mut self, item: &Frame, dst: &mut BytesMut) {
        if self.protocol_version >= 3 {
            protocol::serialize_resp3(item, dst);
        } else {
            protocol::serialize(item, dst);
        }
    }
}

impl Default for RespCodec {
    fn default() -> Self {
        Self::new(ParseConfig::default())
    }
}

#[cfg(feature = "runtime-tokio")]
impl Decoder for RespCodec {
    type Item = Frame;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, Self::Error> {
        self.decode_frame(src)
    }
}

#[cfg(feature = "runtime-tokio")]
impl Encoder<Frame> for RespCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.encode_frame(&item, dst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framevec;
    use bytes::Bytes;

    #[test]
    fn test_decode_frame_simple_string() {
        let mut codec = RespCodec::default();
        let mut buf = BytesMut::from(&b"+OK\r\n"[..]);
        let frame = codec.decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(frame, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_frame_incomplete() {
        let mut codec = RespCodec::default();
        let mut buf = BytesMut::from(&b"+OK"[..]);
        let result = codec.decode_frame(&mut buf).unwrap();
        assert!(result.is_none());
        assert_eq!(&buf[..], b"+OK");
    }

    #[test]
    fn test_encode_frame_simple_string() {
        let mut codec = RespCodec::default();
        let mut buf = BytesMut::new();
        codec.encode_frame(&Frame::SimpleString(Bytes::from_static(b"OK")), &mut buf);
        assert_eq!(&buf[..], b"+OK\r\n");
    }

    #[test]
    fn test_roundtrip_bulk_string() {
        let mut codec = RespCodec::default();
        let original = Frame::BulkString(Bytes::from_static(b"hello world"));

        // Encode
        let mut buf = BytesMut::new();
        codec.encode_frame(&original, &mut buf);

        // Decode
        let decoded = codec.decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, original);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_frame_array_command() {
        let mut codec = RespCodec::default();
        let mut buf = BytesMut::from(&b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n"[..]);
        let frame = codec.decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(
            frame,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"PING")),
                Frame::BulkString(Bytes::from_static(b"hello")),
            ])
        );
    }

    #[test]
    fn test_decode_frame_invalid_data() {
        let config = ParseConfig {
            max_bulk_string_size: 100,
            ..ParseConfig::default()
        };
        let mut codec = RespCodec::new(config);
        let mut buf = BytesMut::from(&b"$999999999\r\n"[..]);
        let result = codec.decode_frame(&mut buf);
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
    fn test_encode_frame_map_resp2_flat_array() {
        let mut codec = RespCodec::default(); // protocol_version=2
        let map = Frame::Map(vec![(
            Frame::BulkString(Bytes::from_static(b"key")),
            Frame::BulkString(Bytes::from_static(b"val")),
        )]);
        let mut buf = BytesMut::new();
        codec.encode_frame(&map, &mut buf);
        // RESP2 downgrades Map to flat Array
        assert_eq!(&buf[..], b"*2\r\n$3\r\nkey\r\n$3\r\nval\r\n");
    }

    #[test]
    fn test_encode_frame_map_resp3_native() {
        let mut codec = RespCodec::default();
        codec.set_protocol_version(3);
        let map = Frame::Map(vec![(
            Frame::BulkString(Bytes::from_static(b"key")),
            Frame::BulkString(Bytes::from_static(b"val")),
        )]);
        let mut buf = BytesMut::new();
        codec.encode_frame(&map, &mut buf);
        // RESP3 uses native Map format with % prefix
        assert_eq!(&buf[..], b"%1\r\n$3\r\nkey\r\n$3\r\nval\r\n");
    }

    #[test]
    fn test_encode_frame_null_resp2() {
        let mut codec = RespCodec::default(); // protocol_version=2
        let mut buf = BytesMut::new();
        codec.encode_frame(&Frame::Null, &mut buf);
        assert_eq!(&buf[..], b"$-1\r\n");
    }

    #[test]
    fn test_encode_frame_null_resp3() {
        let mut codec = RespCodec::default();
        codec.set_protocol_version(3);
        let mut buf = BytesMut::new();
        codec.encode_frame(&Frame::Null, &mut buf);
        assert_eq!(&buf[..], b"_\r\n");
    }

    // Tokio-specific Decoder/Encoder trait tests
    #[cfg(feature = "runtime-tokio")]
    mod tokio_tests {
        use super::*;

        #[test]
        fn test_decoder_simple_string() {
            let mut codec = RespCodec::default();
            let mut buf = BytesMut::from(&b"+OK\r\n"[..]);
            let frame = codec.decode(&mut buf).unwrap().unwrap();
            assert_eq!(frame, Frame::SimpleString(Bytes::from_static(b"OK")));
        }

        #[test]
        fn test_encoder_simple_string() {
            let mut codec = RespCodec::default();
            let mut buf = BytesMut::new();
            codec
                .encode(Frame::SimpleString(Bytes::from_static(b"OK")), &mut buf)
                .unwrap();
            assert_eq!(&buf[..], b"+OK\r\n");
        }
    }
}
