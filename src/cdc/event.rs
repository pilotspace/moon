//! Typed change-data-capture events derived from WAL v3 records.

use bytes::Bytes;

/// Debezium-style operation hint.
///
/// Maps to the `op` field of the JSON envelope. The decoder makes a best
/// effort to classify; ambiguous cases (e.g. SET that may be either an
/// insert or update — Moon's KV layer doesn't distinguish) use `Upsert`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcOp {
    /// Create-or-update (Moon doesn't track INSERT vs UPDATE separately).
    Upsert,
    /// Delete.
    Delete,
    /// Catch-all for records that don't fit the create/update/delete model
    /// (checkpoints, graph temporal markers, unknown record types).
    Other,
}

impl CdcOp {
    /// Debezium single-character code: `c|u|d|r`. Moon emits `u` for upsert,
    /// `d` for delete, and `r` for the catch-all (read-style / metadata).
    #[inline]
    pub fn debezium_code(self) -> &'static str {
        match self {
            CdcOp::Upsert => "u",
            CdcOp::Delete => "d",
            CdcOp::Other => "r",
        }
    }
}

/// One CDC event decoded from a WAL record.
///
/// The variants are deliberately narrow: this layer surfaces the *change
/// itself*, not the bytes-on-the-wire. Consumers who want the original
/// RESP payload can subscribe with the future `FORMAT raw` and skip this
/// decode entirely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CdcEvent {
    /// A key-value write extracted from a Command record (`SET`, `HSET`,
    /// `ZADD`, etc.) or unrecognized command. `command` and `key` are
    /// best-effort; `raw_resp` is always set so consumers have a fallback
    /// if our extraction missed something.
    KvCommand {
        op: CdcOp,
        /// Uppercase command verb if we could extract it (e.g. b"SET").
        command: Option<Bytes>,
        /// First argument if it's a simple key (None for variadic / multi-key).
        key: Option<Bytes>,
        /// Full RESP payload — always present, lets consumers re-parse.
        raw_resp: Bytes,
        lsn: u64,
        shard: u16,
    },

    /// `TEMPORAL.UPSERT` decoded into structured fields. Stamped with
    /// `system_from` from the original record so consumers can do
    /// time-travel queries without re-deriving it from RESP.
    TemporalUpsert {
        key: Bytes,
        value: Bytes,
        valid_from: i64,
        system_from: i64,
        lsn: u64,
        shard: u16,
    },

    /// Graph entity validity-window mutation.
    GraphTemporal {
        entity_id: u64,
        is_node: bool,
        valid_to: i64,
        system_from: i64,
        lsn: u64,
        shard: u16,
    },

    /// Checkpoint marker. Useful to consumers that want to know when WAL
    /// recycling occurred. Payload carries the new `redo_lsn`.
    Checkpoint { redo_lsn: u64, lsn: u64, shard: u16 },

    /// Catch-all for record types CDC doesn't expand yet (vector ops,
    /// file lifecycle events, workspace ops, etc.). Consumers can still
    /// observe the LSN and record_type discriminant.
    Other {
        record_type: u8,
        payload: Bytes,
        lsn: u64,
        shard: u16,
    },
}

impl CdcEvent {
    /// LSN of the underlying WAL record.
    #[inline]
    pub fn lsn(&self) -> u64 {
        match self {
            CdcEvent::KvCommand { lsn, .. }
            | CdcEvent::TemporalUpsert { lsn, .. }
            | CdcEvent::GraphTemporal { lsn, .. }
            | CdcEvent::Checkpoint { lsn, .. }
            | CdcEvent::Other { lsn, .. } => *lsn,
        }
    }

    /// Shard that produced the record.
    #[inline]
    pub fn shard(&self) -> u16 {
        match self {
            CdcEvent::KvCommand { shard, .. }
            | CdcEvent::TemporalUpsert { shard, .. }
            | CdcEvent::GraphTemporal { shard, .. }
            | CdcEvent::Checkpoint { shard, .. }
            | CdcEvent::Other { shard, .. } => *shard,
        }
    }

    /// Debezium op code (c/u/d/r).
    #[inline]
    pub fn op(&self) -> CdcOp {
        match self {
            CdcEvent::KvCommand { op, .. } => *op,
            CdcEvent::TemporalUpsert { .. } => CdcOp::Upsert,
            CdcEvent::GraphTemporal { .. } => CdcOp::Other,
            CdcEvent::Checkpoint { .. } => CdcOp::Other,
            CdcEvent::Other { .. } => CdcOp::Other,
        }
    }
}
