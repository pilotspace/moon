use bytes::Bytes;
use ordered_float::OrderedFloat;
use thiserror::Error;

/// Default maximum size for bulk strings (512 MB).
pub const DEFAULT_MAX_BULK_STRING_SIZE: usize = 512 * 1024 * 1024;

/// Default maximum nesting depth for arrays.
pub const DEFAULT_MAX_ARRAY_DEPTH: usize = 8;

/// Default maximum number of elements in an array.
pub const DEFAULT_MAX_ARRAY_LENGTH: usize = 1024 * 1024;

/// A RESP2/RESP3 protocol frame.
///
/// All string payloads use `Bytes` for zero-copy semantics.
/// No lifetime parameters -- Bytes is reference-counted.
#[derive(Debug, Clone)]
pub enum Frame {
    // === RESP2 variants ===
    /// `+<string>\r\n` -- Non-binary status reply
    SimpleString(Bytes),
    /// `-<error>\r\n` -- Error reply
    Error(Bytes),
    /// `:<integer>\r\n` -- Signed 64-bit integer
    Integer(i64),
    /// `$<len>\r\n<data>\r\n` -- Binary-safe string
    BulkString(Bytes),
    /// `*<count>\r\n<elements...>` -- Ordered collection of frames
    Array(Vec<Frame>),
    /// `$-1\r\n` (RESP2) or `_\r\n` (RESP3) -- Null value
    Null,

    // === RESP3 variants ===
    /// `%<count>\r\n<key><value>...` -- Key-value map
    Map(Vec<(Frame, Frame)>),
    /// `~<count>\r\n<elements...>` -- Unordered set of frames
    Set(Vec<Frame>),
    /// `,<double>\r\n` -- IEEE 754 double-precision float
    Double(f64),
    /// `#t\r\n` or `#f\r\n` -- Boolean value
    Boolean(bool),
    /// `=<len>\r\n<enc>:<data>\r\n` -- Verbatim string with encoding hint
    VerbatimString {
        /// 3-byte encoding hint (e.g. "txt", "mkd")
        encoding: Bytes,
        /// The string data
        data: Bytes,
    },
    /// `(<number>\r\n` -- Arbitrary precision integer as bytes
    BigNumber(Bytes),
    /// `><count>\r\n<elements...>` -- Push data (server-initiated)
    Push(Vec<Frame>),
    /// Already-serialized RESP data -- written directly to output, no re-serialization.
    /// Used by hot-path commands (GET) to skip Frame construction + serialize overhead.
    PreSerialized(Bytes),
}

/// Check if a pre-serialized RESP bulk string wire format equals a BulkString payload.
/// Expected wire format: `$<len>\r\n<data>\r\n`
fn preserialized_eq_bulk_string(wire: &Bytes, data: &Bytes) -> bool {
    // Minimum wire: "$0\r\n\r\n" = 6 bytes
    if wire.len() < 6 || wire[0] != b'$' {
        return false;
    }
    // Find the first \r\n to get the length prefix
    let Some(crlf_pos) = wire[1..].windows(2).position(|w| w == b"\r\n") else {
        return false;
    };
    let header_end = 1 + crlf_pos + 2; // past the \r\n
    // The data portion is wire[header_end .. wire.len()-2] (strip trailing \r\n)
    if wire.len() < header_end + 2 {
        return false;
    }
    &wire[header_end..wire.len() - 2] == data.as_ref()
}

impl PartialEq for Frame {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::SimpleString(a), Self::SimpleString(b)) => a == b,
            (Self::Error(a), Self::Error(b)) => a == b,
            (Self::Integer(a), Self::Integer(b)) => a == b,
            (Self::BulkString(a), Self::BulkString(b)) => a == b,
            (Self::Array(a), Self::Array(b)) => a == b,
            (Self::Null, Self::Null) => true,
            (Self::Map(a), Self::Map(b)) => a == b,
            (Self::Set(a), Self::Set(b)) => a == b,
            (Self::Double(a), Self::Double(b)) => OrderedFloat(*a) == OrderedFloat(*b),
            (Self::Boolean(a), Self::Boolean(b)) => a == b,
            (
                Self::VerbatimString {
                    encoding: ae,
                    data: ad,
                },
                Self::VerbatimString {
                    encoding: be,
                    data: bd,
                },
            ) => ae == be && ad == bd,
            (Self::BigNumber(a), Self::BigNumber(b)) => a == b,
            (Self::Push(a), Self::Push(b)) => a == b,
            (Self::PreSerialized(a), Self::PreSerialized(b)) => a == b,
            // Cross-variant: PreSerialized bulk string == BulkString
            (Self::PreSerialized(wire), Self::BulkString(data))
            | (Self::BulkString(data), Self::PreSerialized(wire)) => {
                preserialized_eq_bulk_string(wire, data)
            }
            _ => false,
        }
    }
}

/// Errors that can occur when parsing RESP2 frames.
#[derive(Debug, Error)]
pub enum ParseError {
    /// Not enough data in the buffer to parse a complete frame.
    /// This is NOT a protocol error -- the caller should read more data.
    #[error("incomplete frame: need more data")]
    Incomplete,

    /// The data violates the RESP2 protocol specification.
    #[error("invalid frame at byte {offset}: {message}")]
    Invalid { message: String, offset: usize },

    /// An I/O error occurred while reading from the buffer.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Configuration for the RESP2 parser.
///
/// Controls limits on frame sizes to prevent denial-of-service attacks.
#[derive(Debug, Clone)]
pub struct ParseConfig {
    /// Maximum size in bytes for a bulk string payload.
    pub max_bulk_string_size: usize,
    /// Maximum nesting depth for arrays.
    pub max_array_depth: usize,
    /// Maximum number of elements in a single array.
    pub max_array_length: usize,
}

impl Default for ParseConfig {
    fn default() -> Self {
        Self {
            max_bulk_string_size: DEFAULT_MAX_BULK_STRING_SIZE,
            max_array_depth: DEFAULT_MAX_ARRAY_DEPTH,
            max_array_length: DEFAULT_MAX_ARRAY_LENGTH,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_simple_string_debug_clone_partialeq() {
        let frame = Frame::SimpleString(Bytes::from_static(b"OK"));
        let cloned = frame.clone();
        assert_eq!(frame, cloned);
        // Verify Debug is implemented
        let debug_str = format!("{:?}", frame);
        assert!(debug_str.contains("SimpleString"));
    }

    #[test]
    fn test_frame_null_not_equal_to_empty_bulk_string() {
        assert_ne!(Frame::Null, Frame::BulkString(Bytes::new()));
    }

    #[test]
    fn test_frame_empty_array_is_valid() {
        let frame = Frame::Array(vec![]);
        assert_eq!(frame, Frame::Array(vec![]));
    }

    #[test]
    fn test_parse_error_incomplete_display() {
        let err = ParseError::Incomplete;
        assert_eq!(format!("{}", err), "incomplete frame: need more data");
    }

    #[test]
    fn test_parse_error_invalid_display() {
        let err = ParseError::Invalid {
            message: "bad".into(),
            offset: 5,
        };
        assert_eq!(format!("{}", err), "invalid frame at byte 5: bad");
    }

    #[test]
    fn test_parse_config_default_max_bulk_string_size() {
        let config = ParseConfig::default();
        assert_eq!(config.max_bulk_string_size, 512 * 1024 * 1024);
    }

    #[test]
    fn test_parse_config_default_max_array_depth() {
        let config = ParseConfig::default();
        assert_eq!(config.max_array_depth, 8);
    }

    #[test]
    fn test_parse_config_default_max_array_length() {
        let config = ParseConfig::default();
        assert_eq!(config.max_array_length, 1024 * 1024);
    }
}
