use crate::error::Result;

/// Temporal sub-client (`TEMPORAL.*` commands).
///
/// Moon tracks bi-temporal MVCC state for both graph entities and vector records,
/// allowing point-in-time queries via wall-clock timestamps.
///
/// Obtain via [`MoonClient::temporal`](crate::MoonClient::temporal).
pub struct TemporalClient {
    pub(crate) conn: redis::aio::MultiplexedConnection,
}

impl TemporalClient {
    /// Take a temporal snapshot at the current wall-clock time.
    ///
    /// After this command, reads on this connection see the state of the world
    /// at the moment this command was executed. The server captures the timestamp
    /// internally — no argument is accepted.
    pub async fn snapshot_at(&mut self) -> Result<()> {
        redis::cmd("TEMPORAL.SNAPSHOT_AT")
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    /// Invalidate (logically delete) a graph entity at the current wall-clock time.
    ///
    /// The entity remains queryable via historical snapshots but is excluded from
    /// current reads.
    ///
    /// - `entity_id` — the node or edge ID
    /// - `entity_type` — `"NODE"` or `"EDGE"`
    /// - `graph_name` — the graph the entity belongs to
    pub async fn invalidate(
        &mut self,
        entity_id: &str,
        entity_type: EntityType,
        graph_name: &str,
    ) -> Result<()> {
        redis::cmd("TEMPORAL.INVALIDATE")
            .arg(entity_id)
            .arg(entity_type.as_str())
            .arg(graph_name)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }
}

/// Entity type for temporal invalidation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityType {
    Node,
    Edge,
}

impl EntityType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Node => "NODE",
            Self::Edge => "EDGE",
        }
    }
}
