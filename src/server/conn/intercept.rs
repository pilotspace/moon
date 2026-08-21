//! The one exit a connection-level intercept's reply leaves through.
//!
//! # Why this type exists
//!
//! Most replies are produced by `dispatch()` and leave through a single exit
//! that applies the RESP2→RESP3 shape policy ([`crate::protocol::resp3`]).
//! **Intercepts short-circuit that exit.** A `try_handle_*` function answers
//! the command itself and pushes straight onto the response vector, so the
//! policy never runs for it — unless the intercept remembers to call it, which
//! is a thing a person has to remember and a reviewer has to notice.
//!
//! Nobody did. `CONFIG GET` reached the wire as a flat Array where Redis sends
//! a Map, from the day RESP3 landed until moon#462 — and it could not be fixed
//! by editing the policy table, because the table is applied at a choke point
//! `CONFIG` never reached. `CLIENT INFO` had the same hole and was patched by
//! hand. `INFO` and `CLIENT LIST` still had it: both answer a VerbatimString on
//! redis-8.6.1 and a plain BulkString on Moon.
//!
//! [`InterceptReplies`] closes the class by taking the choice away. An
//! intercept receives this instead of a `&mut Vec<Frame>`, and its `push`
//! applies the policy unconditionally. An intercept cannot push an unshaped
//! reply at all: the opt-out is refusing the sink in the signature, which is
//! visible in the function's own parameter list and is spelled out at each of
//! the three places that do it (below).
//!
//! # Cost
//!
//! Nothing on RESP2: [`crate::server::conn::util::apply_resp3_conversion`]
//! returns before classifying when `proto < 3`. On RESP3 it is one match on
//! the uppercased command name, no allocation — the same work the dispatch
//! exit already does for every non-intercepted reply.
//!
//! # What is NOT routed through here
//!
//! Three paths keep a plain `&mut Vec<Frame>`, each with the reason written at
//! its signature:
//!
//! * **the blocking pops** (`try_handle_blocking`) encode their reply straight
//!   into the write buffer and then `clear()` the response vector, because the
//!   reply arrives after the batch has already been flushed;
//! * **the AUTH gate** (`check_auth_gate`) runs before the command name has
//!   been extracted, so there is nothing to classify from — and it answers
//!   only `AUTH` and `HELLO`, neither of which carries a shape;
//! * **the cross-shard batch**, in the handlers themselves — the reply comes
//!   back long after the command, carrying only the command NAME, so the shape
//!   is classified at enqueue time and rides along in `RemoteMeta` as a
//!   one-byte tag.

use crate::protocol::Frame;

/// A response vector that shapes every reply pushed through it.
///
/// Borrows the command name and arguments so the shape can be classified at
/// push time; both outlive the intercept call that receives this.
pub(crate) struct InterceptReplies<'a> {
    out: &'a mut Vec<Frame>,
    cmd: &'a [u8],
    args: &'a [Frame],
    proto: u8,
}

impl<'a> InterceptReplies<'a> {
    /// `args` EXCLUDES the command name, matching what every intercept is
    /// handed and what [`crate::protocol::resp3::resp3_shape_of`] expects.
    #[inline]
    pub(crate) fn new(
        out: &'a mut Vec<Frame>,
        cmd: &'a [u8],
        args: &'a [Frame],
        proto: u8,
    ) -> Self {
        Self {
            out,
            cmd,
            args,
            proto,
        }
    }

    /// Push a reply, applying the RESP3 shape policy for this command.
    ///
    /// This is the default and should stay the only method intercepts call.
    #[inline]
    pub(crate) fn push(&mut self, frame: Frame) {
        self.out
            .push(crate::server::conn::util::apply_resp3_conversion(
                self.cmd, self.args, frame, self.proto,
            ));
    }

    /// The index the next pushed reply will occupy.
    ///
    /// Only the monoio handler needs this; the tokio handler's intercepts
    /// never have to remember where a reply landed, hence the cfg'd allow.
    ///
    /// Used by the callers that must remember where a reply landed — the
    /// protocol-switch record (`HELLO 3` must be encoded in the protocol the
    /// batch STARTED in) and the group-commit barrier (a local-leg write whose
    /// reply is overwritten if the fsync fails).
    #[inline]
    #[cfg_attr(not(feature = "runtime-monoio"), allow(dead_code))]
    pub(crate) fn len(&self) -> usize {
        self.out.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn args(items: &[&str]) -> Vec<Frame> {
        items
            .iter()
            .map(|s| Frame::BulkString(Bytes::copy_from_slice(s.as_bytes())))
            .collect()
    }

    #[test]
    fn push_shapes_the_reply_without_the_caller_asking() {
        // The whole point: the intercept pushes what it built, and the map
        // conversion happens anyway. `CONFIG GET` is the command that was
        // wrong for the entire life of RESP3 support.
        let mut out = Vec::new();
        let a = args(&["GET", "maxmemory"]);
        let mut sink = InterceptReplies::new(&mut out, b"CONFIG", &a, 3);
        sink.push(Frame::Array(
            vec![
                Frame::BulkString(Bytes::from_static(b"maxmemory")),
                Frame::BulkString(Bytes::from_static(b"0")),
            ]
            .into(),
        ));
        assert!(
            matches!(out[0], Frame::Map(_)),
            "CONFIG GET must leave an intercept as a Map on RESP3, got {:?}",
            out[0]
        );
    }

    #[test]
    fn resp2_is_untouched() {
        let mut out = Vec::new();
        let a = args(&["GET", "maxmemory"]);
        let mut sink = InterceptReplies::new(&mut out, b"CONFIG", &a, 2);
        sink.push(Frame::Array(
            vec![
                Frame::BulkString(Bytes::from_static(b"maxmemory")),
                Frame::BulkString(Bytes::from_static(b"0")),
            ]
            .into(),
        ));
        assert!(
            matches!(out[0], Frame::Array(_)),
            "a RESP2 client must see no conversion at all"
        );
    }

    #[test]
    fn a_command_with_no_policy_entry_passes_through() {
        // Shaping every intercept reply is only safe because an unlisted
        // command classifies as `None`. If that ever stopped being true this
        // test would catch it before a live reply changed type.
        let mut out = Vec::new();
        let a = args(&["KILL", "ID", "7"]);
        let mut sink = InterceptReplies::new(&mut out, b"CLIENT", &a, 3);
        sink.push(Frame::Integer(1));
        assert!(matches!(out[0], Frame::Integer(1)));
    }

    #[test]
    fn errors_pass_through_whatever_the_shape_says() {
        // Every `try_enforce_*` intercept pushes an error for an ARBITRARY
        // command, including commands that carry a shape. An error must never
        // be run through a converter.
        let mut out = Vec::new();
        let a = args(&["GET", "maxmemory"]);
        let mut sink = InterceptReplies::new(&mut out, b"CONFIG", &a, 3);
        sink.push(Frame::Error(Bytes::from_static(b"READONLY nope")));
        assert!(matches!(out[0], Frame::Error(_)));
    }

    #[test]
    fn len_tracks_the_underlying_vector() {
        let mut out = vec![Frame::Integer(0)];
        let a = args(&["INFO"]);
        let mut sink = InterceptReplies::new(&mut out, b"CLIENT", &a, 2);
        assert_eq!(sink.len(), 1, "len is the index the next reply will take");
        sink.push(Frame::Null);
        assert_eq!(sink.len(), 2);
    }
}
