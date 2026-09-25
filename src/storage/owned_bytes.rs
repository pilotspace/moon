//! Exact-size owned copies of request bytes that are about to be STORED
//! (moon#1160).
//!
//! The RESP parser freezes a whole request out of the connection's read
//! buffer and hands every argument out as a `Bytes` SLICE of that one
//! allocation (`protocol::parse`); AOF replay slices its replay buffer the
//! same way. `.clone()` of such a slice is a refcount bump on the WHOLE
//! buffer, so a collection element stored that way keeps the entire read
//! buffer alive: the connection cannot reuse it and allocates a fresh one,
//! while the ledger bills only the element's own size. Measured on `935c555`:
//! 100K `SADD s member:<i>` interleaved with 4 KiB `SET scratch` writes drove
//! RSS from 18.8 MB to 578 MB with `used_memory` at 8.2 MB.
//!
//! Strings never had the problem (`CompactValue::heap_string_owned` goes
//! through `Vec<u8>`, which copies a shared `Bytes`), nor do the compact
//! encodings (listpack, intset copy into their own buffer). The FULL
//! collection encodings stored the frame's `Bytes` as-is. Every such write
//! site now stores [`detach`]'s copy instead: one exact-size allocation per
//! stored element, which is what the listpack path, the string path and
//! redis (`sdsnewlen`) already pay — and it drops an atomic increment now and
//! a decrement later on the shared buffer's refcount.
//!
//! This lives in `storage/` so `src/command/` keeps its no-allocation rule:
//! the allocation belongs to the store, not to the command.

use bytes::Bytes;

/// An exact-size, uniquely owned copy of `bytes`, for an element about to be
/// stored in a long-lived container.
///
/// Never shares an allocation with its input, whatever the input's backing:
/// that is the whole contract, and what `owned_bytes::tests` pins with a
/// pointer-range check against the buffer the parser sliced.
#[inline]
#[must_use]
pub fn detach(bytes: &[u8]) -> Bytes {
    Bytes::copy_from_slice(bytes)
}

#[cfg(test)]
mod tests;

/// Shared by the pointer-range tests of the OTHER long-lived stores that keep
/// request bytes (the script cache, the pub/sub registries): arguments exactly
/// as the RESP parser hands them out, and the address range of the buffer
/// they are slices of.
#[cfg(test)]
pub(crate) mod test_support {
    use std::ops::Range;

    use bytes::{Bytes, BytesMut};

    use crate::protocol::{Frame, ParseConfig, parse};

    /// One parsed request. The argument slices keep the buffer alive, so no
    /// other allocation can land inside `range` while this lives — an address
    /// inside it can only be a slice of the request.
    pub(crate) struct WireArgs {
        pub(crate) args: Vec<Bytes>,
        range: Range<usize>,
    }

    impl WireArgs {
        /// Encode `parts` as one RESP array into its own 4 KiB buffer (the
        /// shape of a connection read buffer) and parse it with the real
        /// parser.
        pub(crate) fn parse(parts: &[&[u8]]) -> Self {
            let mut buf = BytesMut::with_capacity(4096);
            buf.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
            for p in parts {
                buf.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
                buf.extend_from_slice(p);
                buf.extend_from_slice(b"\r\n");
            }
            let base = buf.as_ptr() as usize;
            let range = base..base + buf.capacity();
            let frame = parse::parse(&mut buf, &ParseConfig::default())
                .expect("well-formed RESP")
                .expect("one complete frame");
            let Frame::Array(items) = frame else {
                panic!("a command parses to an array");
            };
            let args: Vec<Bytes> = items
                .iter()
                .map(|f| match f {
                    Frame::BulkString(b) => b.clone(),
                    other => panic!("every argument is a bulk string, got {other:?}"),
                })
                .collect();
            for a in &args {
                assert!(
                    range.contains(&(a.as_ptr() as usize)),
                    "precondition: the parser hands arguments out as slices of the read buffer"
                );
            }
            Self { args, range }
        }

        /// Assert `stored` does not point into the request buffer.
        pub(crate) fn assert_detached(&self, what: &str, stored: &Bytes) {
            assert!(
                !self.range.contains(&(stored.as_ptr() as usize)),
                "{what}: the stored bytes are a slice of the request buffer (moon#1160)"
            );
        }
    }
}
