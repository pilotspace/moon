//! A `PUBLISH`/`SPUBLISH` queued inside `MULTI`, deferred to after `EXEC`'s
//! body (C2, moon#1043).
//!
//! The transaction executors cannot publish in place: they hold the keyspace,
//! not the connection's pub/sub registry or the SPSC mesh, and a remote fan-out
//! awaits. So each executor leaves a placeholder in the EXEC reply and records
//! one of these; the connection handler fans the message out after the body
//! and patches the placeholder with the receiver count (or `NOPERM`).
//!
//! Lives in its own module because it crosses three layers: the executors in
//! `server::conn::shared`, the owner-routed EXEC reply
//! ([`super::dispatch::TxnExecReply`]), and the three connection handlers.

use bytes::Bytes;

/// The pub/sub namespace a publish targets.
///
/// An enum and not a `bool` for the reason [`super::dispatch::ShardMessage`]
/// keeps `SPublishBatch` separate from `PubSubPublishBatch`: the two
/// namespaces are different destinations that may share a channel NAME
/// without sharing subscribers, so a flag would be one `if` away from
/// cross-delivering.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishKind {
    /// `PUBLISH`: the global channel namespace (`SUBSCRIBE`/`PSUBSCRIBE`).
    Global,
    /// `SPUBLISH`: the shard-channel namespace (`SSUBSCRIBE`).
    Shard,
}

impl PublishKind {
    /// The queued command's kind, or `None` when it is not a publish.
    #[inline]
    pub fn of(cmd: &[u8]) -> Option<Self> {
        if cmd.eq_ignore_ascii_case(b"PUBLISH") {
            Some(Self::Global)
        } else if cmd.eq_ignore_ascii_case(b"SPUBLISH") {
            Some(Self::Shard)
        } else {
            None
        }
    }
}

/// One publish queued in a transaction, awaiting its post-body fan-out.
#[derive(Clone, Debug)]
pub struct ExecPublish {
    /// Index of this command's placeholder in the EXEC reply array.
    pub slot: usize,
    pub channel: Bytes,
    pub message: Bytes,
    pub kind: PublishKind,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_of_recognises_both_publish_verbs_case_insensitively() {
        assert_eq!(PublishKind::of(b"PUBLISH"), Some(PublishKind::Global));
        assert_eq!(PublishKind::of(b"publish"), Some(PublishKind::Global));
        assert_eq!(PublishKind::of(b"SPUBLISH"), Some(PublishKind::Shard));
        assert_eq!(PublishKind::of(b"sPublish"), Some(PublishKind::Shard));
        assert_eq!(PublishKind::of(b"SUBSCRIBE"), None);
        assert_eq!(PublishKind::of(b"SPUBLISHX"), None);
    }
}
