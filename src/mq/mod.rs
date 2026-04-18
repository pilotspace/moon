//! Durable message queue types and functions.
//!
//! Provides `DurableStreamConfig` for queue metadata, `DurableQueueRegistry`
//! for per-shard state, `TriggerRegistry` for debounced callbacks, and WAL
//! encode/decode helpers for `MqCreate` and `MqAck` records.
//!
//! MQ.* commands are intercepted BEFORE dispatch in handler_monoio.rs
//! (same pattern as TXN.*, TEMPORAL.*, WS.*).

pub mod registry;
pub mod trigger;
pub mod wal;

use bytes::Bytes;

pub use registry::DurableQueueRegistry;
pub use trigger::{TriggerEntry, TriggerRegistry};

/// Configuration for a durable message queue built on top of Redis Streams.
///
/// Each durable queue wraps a standard Stream with at-least-once delivery
/// guarantees, automatic dead-letter routing, and WAL-backed crash recovery.
#[derive(Debug, Clone)]
pub struct DurableStreamConfig {
    /// The stream key in the database.
    pub queue_key: Bytes,
    /// Maximum delivery attempts before dead-letter routing (default 3).
    pub max_delivery_count: u32,
    /// Dead-letter queue key (derived: `{queue_key}::mq:dlq`).
    pub dlq_key: Bytes,
    /// Reserved consumer group name for MQ.POP.
    pub consumer_group: Bytes,
}

impl DurableStreamConfig {
    /// Create a new durable stream configuration.
    ///
    /// Derives `dlq_key` as `{queue_key}::mq:dlq` (double-colon convention
    /// to avoid user key collision). Sets consumer group to `__mq_consumers`.
    pub fn new(queue_key: Bytes, max_delivery_count: u32) -> Self {
        let dlq_key = {
            let mut buf = Vec::with_capacity(queue_key.len() + 8);
            buf.extend_from_slice(&queue_key);
            buf.extend_from_slice(b"::mq:dlq");
            Bytes::from(buf)
        };
        Self {
            queue_key,
            max_delivery_count,
            dlq_key,
            consumer_group: Bytes::from_static(b"__mq_consumers"),
        }
    }
}

/// Check if a command name is the MQ command group.
#[inline]
pub fn is_mq_command(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"MQ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_mq_command() {
        assert!(is_mq_command(b"MQ"));
        assert!(is_mq_command(b"mq"));
        assert!(is_mq_command(b"Mq"));
        assert!(is_mq_command(b"mQ"));
        assert!(!is_mq_command(b"MQX"));
        assert!(!is_mq_command(b"M"));
        assert!(!is_mq_command(b""));
        assert!(!is_mq_command(b"WS"));
    }

    #[test]
    fn test_durable_stream_config_new() {
        let config = DurableStreamConfig::new(Bytes::from_static(b"myqueue"), 5);
        assert_eq!(config.queue_key.as_ref(), b"myqueue");
        assert_eq!(config.max_delivery_count, 5);
        assert_eq!(config.consumer_group.as_ref(), b"__mq_consumers");
    }

    #[test]
    fn test_dlq_key_derivation() {
        let config = DurableStreamConfig::new(Bytes::from_static(b"orders"), 3);
        assert_eq!(config.dlq_key.as_ref(), b"orders::mq:dlq");

        // Verify double-colon convention avoids collision with user keys
        let config2 = DurableStreamConfig::new(Bytes::from_static(b"tasks:pending"), 3);
        assert_eq!(config2.dlq_key.as_ref(), b"tasks:pending::mq:dlq");
    }

    #[test]
    fn test_durable_stream_config_default_max_delivery() {
        let config = DurableStreamConfig::new(Bytes::from_static(b"q"), 3);
        assert_eq!(config.max_delivery_count, 3);
    }

    #[test]
    fn test_durable_stream_config_empty_key() {
        let config = DurableStreamConfig::new(Bytes::new(), 1);
        assert_eq!(config.dlq_key.as_ref(), b"::mq:dlq");
    }
}
