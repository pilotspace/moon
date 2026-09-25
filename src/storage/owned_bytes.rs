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
