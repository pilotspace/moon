use bytes::Bytes;
use ordered_float::OrderedFloat;
use thiserror::Error;

/// The element collection of `Frame::Array` / `Set` / `Push`.
///
/// A plain `Vec<Frame>` behind a newtype (moon#1179 item 1). It used to be
/// `Box<SmallVec<[Frame; 4]>>`: the box was the indirection a recursive type
/// needs, and the inline four were meant to save the element allocation. In
/// practice it cost MORE: every array paid the ~300 B box, a request with more
/// than four arguments (`SET k v EX 100`, `HSET k f1 v1 f2 v2`) spilled and
/// paid the box AND a heap array, and every `Vec -> FrameVec` reply site
/// either copied into the inline slots or adopted the `Vec` and boxed it
/// anyway. A `Vec` is one allocation for any length, adopted for free from
/// the reply sites that build one, and 24 bytes inline in `Frame`.
///
/// No `SmallVec`-specific method was ever used through the `Deref`, so the API
/// is unchanged: it derefs to `Vec<Frame>` (and from there to `[Frame]`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FrameVec(Vec<Frame>);

impl FrameVec {
    #[inline]
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        Self(Vec::with_capacity(cap))
    }

    /// Adopt a `Vec` without copying or allocating.
    #[inline]
    pub fn from_vec(v: Vec<Frame>) -> Self {
        Self(v)
    }

    #[inline]
    pub fn from_elem(elem: Frame) -> Self {
        Self(vec![elem])
    }

    /// The underlying `Vec`, without copying.
    #[inline]
    pub fn into_vec(self) -> Vec<Frame> {
        self.0
    }
}

impl std::ops::Deref for FrameVec {
    type Target = Vec<Frame>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for FrameVec {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl IntoIterator for FrameVec {
    type Item = Frame;
    type IntoIter = std::vec::IntoIter<Frame>;
    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a FrameVec {
    type Item = &'a Frame;
    type IntoIter = std::slice::Iter<'a, Frame>;
    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl FromIterator<Frame> for FrameVec {
    #[inline]
    fn from_iter<I: IntoIterator<Item = Frame>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl From<Vec<Frame>> for FrameVec {
    #[inline]
    fn from(v: Vec<Frame>) -> Self {
        Self(v)
    }
}

impl From<FrameVec> for Vec<Frame> {
    #[inline]
    fn from(v: FrameVec) -> Self {
        v.0
    }
}

/// Create a `FrameVec` from a list of `Frame` values, analogous to `vec![]`.
#[macro_export]
macro_rules! framevec {
    () => {
        $crate::protocol::FrameVec::new()
    };
    ($($x:expr),+ $(,)?) => {
        $crate::protocol::FrameVec::from_vec(vec![$($x),+])
    };
}

/// Default maximum size for bulk strings (512 MB).
pub const DEFAULT_MAX_BULK_STRING_SIZE: usize = 512 * 1024 * 1024;

/// Default maximum nesting depth for arrays.
pub const DEFAULT_MAX_ARRAY_DEPTH: usize = 8;

/// Default maximum number of elements in an array.
pub const DEFAULT_MAX_ARRAY_LENGTH: usize = 1024 * 1024;

/// Default maximum size for a single inline command line (64 KB).
///
/// Mirrors Redis's `PROTO_INLINE_MAX_SIZE`. Bounds the RESP-less inline path,
/// which the framed-parser limits above do not cover: without this, a client
/// that never sends a `\r\n` (raw non-RESP bytes) grows the read buffer without
/// limit — a per-connection memory-exhaustion vector.
pub const DEFAULT_MAX_INLINE_SIZE: usize = 64 * 1024;

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
    Array(FrameVec),
    /// `$-1\r\n` (RESP2) or `_\r\n` (RESP3) -- Null value.
    ///
    /// This is the null whose missing value is a STRING. For a reply whose
    /// missing value is an array, use [`Frame::NullArray`] -- RESP2 spells the
    /// two differently and a statically-typed client decodes them differently.
    Null,
    /// `*-1\r\n` (RESP2) or `_\r\n` (RESP3) -- Null ARRAY.
    ///
    /// RESP2 has two nulls and they are not interchangeable: `$-1` says "the
    /// string you asked for is missing", `*-1` says "the array you asked for is
    /// missing". A client that decodes `BLPOP` as an array errors on `$-1`.
    /// RESP3 collapses both to `_` (verified against redis-server 8.6.1), which
    /// is why the divergence is RESP2-only.
    ///
    /// Composable: as an element of a [`Frame::Array`] it emits `*-1` inline,
    /// which is how `GEOPOS` reports an absent member (`*1\r\n*-1\r\n`).
    ///
    /// Never produced by a parse FAILURE -- that sentinel stays [`Frame::Null`]
    /// (see `parse_frame_zerocopy`). Only a well-formed `*-1` yields this.
    NullArray,

    // === RESP3 variants ===
    /// `%<count>\r\n<key><value>...` -- Key-value map
    Map(Vec<(Frame, Frame)>),
    /// `~<count>\r\n<elements...>` -- Unordered set of frames
    Set(FrameVec),
    /// `,<double>\r\n` -- IEEE 754 double-precision float
    Double(f64),
    /// `#t\r\n` or `#f\r\n` -- Boolean value
    Boolean(bool),
    /// `=<len>\r\n<enc>:<data>\r\n` -- Verbatim string with encoding hint
    VerbatimString {
        /// 3-byte encoding hint (e.g. "txt", "mkd").
        ///
        /// Inline bytes, not a `Bytes`: the RESP3 wire fixes it at exactly
        /// three, and a second `Bytes` here was the ONLY thing making `Frame`
        /// 72 bytes instead of 40 — every `Frame` move (`push`,
        /// `mem::replace`, `DispatchResult`, `ResponseSlot`) paid for it, and
        /// the parser mallocs a 3-byte copy per verbatim reply (moon#1179).
        encoding: [u8; 3],
        /// The string data
        data: Bytes,
    },
    /// `(<number>\r\n` -- Arbitrary precision integer as bytes
    BigNumber(Bytes),
    /// `><count>\r\n<elements...>` -- Push data (server-initiated)
    Push(FrameVec),
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
            // Explicit, NOT left to the `_ => false` arm below: without this a
            // NullArray would not even equal itself. The two nulls stay
            // distinct from each other on purpose — that distinction is the
            // whole point of the variant (moon#482).
            (Self::NullArray, Self::NullArray) => true,
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

/// Which protocol fault occurred, in the vocabulary Redis puts on the wire.
///
/// `ParseError::Invalid` already carries a detailed `message` — it is what a
/// fuzz triage or a log reader wants ("invalid bulk string length: -5"
/// localises a bug; "invalid bulk length" does not). But that detail is
/// Moon's own wording, and a driver author reading it cannot match it against
/// the Redis error they wrote their reconnect logic around.
///
/// So the fault carries BOTH: this enum for the wire, the message for us.
/// Redis's set is small and fixed (`networking.c`), which is why this is a
/// closed enum of `&'static str` rather than a formatted string — nothing
/// here allocates, and it is only ever reached on a connection that is
/// already terminating.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtoFault {
    /// A bulk header's length was absent, non-numeric, negative (other than
    /// the `-1` null), or past the configured maximum.
    BulkLen,
    /// An array/set/map/push header's element count was malformed or too deep.
    MultibulkLen,
    /// An array element did not begin with `$`. Carries the offending byte.
    ExpectedDollar(u8),
    /// An inline request exceeded the inline cap.
    InlineTooBig,
    /// An inline request ended with a quote still open.
    UnbalancedQuotes,
    /// The multibulk count string itself was implausibly long.
    MbulkCountTooBig,
    /// A type byte Moon parses but a Redis client should never send inbound.
    /// Redis conflates this with `ExpectedDollar`, and so do we.
    UnknownType(u8),
}

impl ProtoFault {
    /// The reason text, verbatim from redis-server 8.6.1. The caller prefixes
    /// `-ERR `. Returns `None` for the two byte-carrying variants, whose text
    /// must be formatted — see [`ProtoFault::wire_text_owned`].
    pub fn wire_text(&self) -> &'static str {
        match self {
            ProtoFault::BulkLen => "Protocol error: invalid bulk length",
            ProtoFault::MultibulkLen => "Protocol error: invalid multibulk length",
            ProtoFault::InlineTooBig => "Protocol error: too big inline request",
            ProtoFault::UnbalancedQuotes => "Protocol error: unbalanced quotes in request",
            ProtoFault::MbulkCountTooBig => "Protocol error: too big mbulk count string",
            // Both byte-carrying variants render through `wire_text_owned`;
            // this arm exists so a caller that only wants a static string
            // still gets the right *shape* rather than a panic.
            ProtoFault::ExpectedDollar(_) | ProtoFault::UnknownType(_) => {
                "Protocol error: expected '$', got '?'"
            }
        }
    }

    /// The reason text with any offending byte substituted in.
    ///
    /// Allocates, deliberately: this runs once per doomed connection, never on
    /// a serving path, and the alternative (a stack buffer threaded through
    /// three handlers) buys nothing measurable.
    pub fn wire_text_owned(&self) -> String {
        match self {
            ProtoFault::ExpectedDollar(b) | ProtoFault::UnknownType(b) => {
                // Redis prints the raw byte. A non-printable one renders as
                // whatever the terminal makes of it, which is Redis's
                // behavior too — matching it matters more than prettiness.
                // Except CR and LF: this text is written to the socket as a
                // raw `-ERR` line, not through `serialize::put_line`, so a raw
                // CR/LF would end the reply early. Redis prints a space
                // (`addReplyErrorFormat` maps both), and so do we.
                let shown = if matches!(*b, b'\r' | b'\n') {
                    ' '
                } else {
                    *b as char
                };
                format!("Protocol error: expected '$', got '{shown}'")
            }
            other => other.wire_text().to_string(),
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
    ///
    /// `kind` is what goes on the wire; `message` is the detailed internal
    /// reason kept for logs and fuzz triage. They are deliberately different.
    #[error("invalid frame at byte {offset}: {message}")]
    Invalid {
        kind: ProtoFault,
        message: String,
        offset: usize,
    },

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
    /// Maximum size in bytes of a single inline (non-RESP) command line.
    pub max_inline_size: usize,
}

impl Default for ParseConfig {
    fn default() -> Self {
        Self {
            max_bulk_string_size: DEFAULT_MAX_BULK_STRING_SIZE,
            max_array_depth: DEFAULT_MAX_ARRAY_DEPTH,
            max_array_length: DEFAULT_MAX_ARRAY_LENGTH,
            max_inline_size: DEFAULT_MAX_INLINE_SIZE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The protocol-fault text echoes one client byte and is written raw, so
    /// a CR or LF there must become a space (redis-server 8.6.1:
    /// `*1\r\n\r\n` -> `-ERR Protocol error: expected '$', got ' '`).
    #[test]
    fn proto_fault_text_never_echoes_cr_or_lf() {
        for b in [b'\r', b'\n'] {
            for fault in [ProtoFault::ExpectedDollar(b), ProtoFault::UnknownType(b)] {
                assert_eq!(
                    fault.wire_text_owned(),
                    "Protocol error: expected '$', got ' '"
                );
            }
        }
        assert_eq!(
            ProtoFault::ExpectedDollar(b'x').wire_text_owned(),
            "Protocol error: expected '$', got 'x'"
        );
    }

    #[test]
    fn frame_size_measurement() {
        // moon#1179 item 2: every variant fits in 32 payload bytes once the
        // verbatim encoding tag is inline `[u8; 3]` (it was a second `Bytes`,
        // making every `Frame` 72 bytes). A regression here is a 44% bigger
        // copy on every `push`, `mem::replace` and cross-shard reply.
        assert_eq!(std::mem::size_of::<Frame>(), 40);
        assert_eq!(std::mem::size_of::<FrameVec>(), 24);
    }

    /// moon#1179 item 1: `FrameVec` is a `Vec<Frame>` newtype — no box, so an
    /// array costs one allocation at any length (the old
    /// `Box<SmallVec<[Frame; 4]>>` paid the box always and a spill past four
    /// arguments on top), and a `Vec` built by a reply site is ADOPTED, never
    /// copied (the old `from_vec` copied short vecs into the inline slots).
    #[test]
    fn framevec_is_an_unboxed_vec_and_adopts_without_copying() {
        assert_eq!(
            std::mem::size_of::<FrameVec>(),
            std::mem::size_of::<Vec<Frame>>(),
            "FrameVec must be a bare Vec (no Box indirection)"
        );
        for n in [1usize, 3, 4, 5, 17] {
            let v: Vec<Frame> = (0..n as i64).map(Frame::Integer).collect();
            let ptr = v.as_ptr();
            let fv = FrameVec::from_vec(v);
            assert_eq!(fv.as_ptr(), ptr, "from_vec copied a {n}-element Vec");
            let fv2: FrameVec = fv.into_vec().into();
            assert_eq!(fv2.as_ptr(), ptr, "From<Vec> copied a {n}-element Vec");
            let back: Vec<Frame> = fv2.into();
            assert_eq!(back.as_ptr(), ptr);
        }
        // A parsed 5-argument request holds its arguments in ONE exact-size
        // allocation — the shape that used to take a box plus a spill.
        let mut buf = bytes::BytesMut::from(
            &b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$3\r\n100\r\n"[..],
        );
        match crate::protocol::parse(&mut buf, &ParseConfig::default()) {
            Ok(Some(Frame::Array(items))) => {
                assert_eq!(items.len(), 5);
                assert_eq!(items.capacity(), 5);
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

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
    fn test_null_array_is_distinct_from_null_and_equal_to_itself() {
        // Equality is hand-written with a `_ => false` catch-all, so without
        // an explicit arm a NullArray would not even equal itself (moon#482).
        assert_eq!(Frame::NullArray, Frame::NullArray);
        assert_ne!(Frame::NullArray, Frame::Null);
        assert_ne!(Frame::Null, Frame::NullArray);
        assert_ne!(Frame::NullArray, Frame::Array(framevec![]));
    }

    #[test]
    fn test_frame_empty_array_is_valid() {
        let frame = Frame::Array(framevec![]);
        assert_eq!(frame, Frame::Array(framevec![]));
    }

    #[test]
    fn test_parse_error_incomplete_display() {
        let err = ParseError::Incomplete;
        assert_eq!(format!("{}", err), "incomplete frame: need more data");
    }

    #[test]
    fn test_parse_error_invalid_display() {
        let err = ParseError::Invalid {
            kind: ProtoFault::BulkLen,
            message: "bad".into(),
            offset: 5,
        };
        // Display still renders the DETAILED internal message, not the wire
        // text — the two are deliberately different, and this assertion is
        // what pins that. `kind` travels alongside for the client's benefit.
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
