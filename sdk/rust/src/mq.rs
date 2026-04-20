use crate::error::Result;
use crate::types::MqMessage;
use crate::util::value_to_string;

/// Message queue sub-client (`MQ.*` commands).
///
/// Moon message queues are built on Redis Streams with at-least-once delivery
/// and automatic dead-letter queuing after `max_delivery` failures.
///
/// Obtain via [`MoonClient::mq`](crate::MoonClient::mq).
pub struct MqClient {
    pub(crate) conn: redis::aio::MultiplexedConnection,
}

impl MqClient {
    /// Create a message queue at `key`.
    ///
    /// - `max_delivery` — redelivery limit before moving to DLQ (default 3)
    pub async fn create(
        &mut self,
        key: &str,
        max_delivery: Option<usize>,
    ) -> Result<()> {
        let mut cmd = redis::cmd("MQ");
        cmd.arg("CREATE").arg(key);
        if let Some(n) = max_delivery {
            cmd.arg("MAXDELIVERY").arg(n);
        }
        cmd.query_async::<()>(&mut self.conn).await?;
        Ok(())
    }

    /// Push a message onto the queue. Returns the stream entry ID.
    pub async fn push(&mut self, key: &str, data: &[u8]) -> Result<String> {
        Ok(redis::cmd("MQ").arg("PUSH").arg(key).arg("body").arg(data)
            .query_async(&mut self.conn).await?)
    }

    /// Push a message onto a partitioned topic (`MQ.PUSH <topic> <partition> <payload>`).
    ///
    /// Lunaris-shaped helper: Lunaris models queues as `(topic, partition)` pairs
    /// (matching Kafka semantics) and expects a numeric offset back rather than
    /// a Redis-stream entry ID. Returns the broker-assigned monotonic offset
    /// within `(topic, partition)`.
    pub async fn push_partitioned(
        &mut self,
        topic: &str,
        partition: u16,
        payload: &[u8],
    ) -> Result<u64> {
        Ok(redis::cmd("MQ.PUSH")
            .arg(topic)
            .arg(partition)
            .arg(payload)
            .query_async(&mut self.conn)
            .await?)
    }

    /// Long-poll pop one message from a partitioned topic, blocking up to
    /// `block_ms` milliseconds (`MQ.POP <group> <topic> <partition> COUNT 1
    /// BLOCK <ms>`).
    ///
    /// Lunaris-shaped helper. Returns the raw `redis::Value` so callers can
    /// distinguish `Value::Nil` (no message in the poll window) from
    /// `Value::Array` (at least one message ready) without paying for a typed
    /// parse pass that would lose the Nil signal.
    pub async fn pop_partitioned(
        &mut self,
        group: &str,
        topic: &str,
        partition: u16,
        block_ms: u64,
    ) -> Result<redis::Value> {
        Ok(redis::cmd("MQ.POP")
            .arg(group)
            .arg(topic)
            .arg(partition)
            .arg("COUNT")
            .arg(1)
            .arg("BLOCK")
            .arg(block_ms)
            .query_async(&mut self.conn)
            .await?)
    }

    /// Pop up to `count` messages from the queue.
    ///
    /// Returns consumed messages. Callers must [`ack`](MqClient::ack) each message
    /// after successful processing to prevent redelivery.
    pub async fn pop(&mut self, key: &str, count: usize) -> Result<Vec<MqMessage>> {
        let raw: redis::Value = redis::cmd("MQ")
            .arg("POP").arg(key).arg("COUNT").arg(count)
            .query_async(&mut self.conn).await?;
        Ok(parse_mq_messages(raw))
    }

    /// Acknowledge a message by its stream entry ID, removing it from the pending list.
    pub async fn ack(&mut self, key: &str, id: &str) -> Result<()> {
        redis::cmd("MQ").arg("ACK").arg(key).arg(id)
            .query_async::<()>(&mut self.conn).await?;
        Ok(())
    }

    /// Get the number of messages in the dead-letter queue for `key`.
    pub async fn dlq_len(&mut self, key: &str) -> Result<i64> {
        Ok(redis::cmd("MQ").arg("DLQLEN").arg(key)
            .query_async(&mut self.conn).await?)
    }

    /// Register a debounced trigger. When a message arrives at `key`, Moon
    /// publishes a notification to `callback_key` after the debounce window.
    pub async fn trigger(
        &mut self,
        key: &str,
        callback_key: &str,
        debounce_ms: Option<u64>,
    ) -> Result<()> {
        let mut cmd = redis::cmd("MQ");
        cmd.arg("TRIGGER").arg(key).arg(callback_key);
        if let Some(ms) = debounce_ms {
            cmd.arg("DEBOUNCE").arg(ms);
        }
        cmd.query_async::<()>(&mut self.conn).await?;
        Ok(())
    }

    /// Transactional publish — enqueues a message inside an open `TXN` block.
    /// Must be called between [`MoonClient::txn_begin`] and [`MoonClient::txn_commit`].
    pub async fn publish_txn(&mut self, key: &str, data: &[u8]) -> Result<()> {
        redis::cmd("MQ").arg("PUBLISH").arg(key).arg("body").arg(data)
            .query_async::<()>(&mut self.conn).await?;
        Ok(())
    }
}

fn parse_mq_messages(raw: redis::Value) -> Vec<MqMessage> {
    let arr = match raw {
        redis::Value::Array(a) => a,
        _ => return vec![],
    };
    arr.into_iter()
        .filter_map(|item| {
            // Server returns: [id, Array[field, val, field, val, ...]]
            if let redis::Value::Array(entry) = item {
                let id = entry.first().map(value_to_string).unwrap_or_default();
                let data = match entry.get(1) {
                    Some(redis::Value::Array(kv)) => {
                        // Extract "body" field value from the flat key-value pairs
                        let mut body = bytes::Bytes::new();
                        let mut i = 0;
                        while i + 1 < kv.len() {
                            if value_to_string(&kv[i]).eq_ignore_ascii_case("body") {
                                if let redis::Value::BulkString(b) = &kv[i + 1] {
                                    body = bytes::Bytes::from(b.clone());
                                }
                                break;
                            }
                            i += 2;
                        }
                        body
                    }
                    Some(redis::Value::BulkString(b)) => bytes::Bytes::from(b.clone()),
                    _ => bytes::Bytes::new(),
                };
                Some(MqMessage { id, data, delivery_count: 1 })
            } else {
                None
            }
        })
        .collect()
}
