use crate::error::Result;

/// Temporal sub-client (`TEMPORAL.*` commands).
///
/// Moon tracks bi-temporal MVCC state for both graph entities and vector records,
/// allowing point-in-time queries via wall-clock timestamps.
///
/// Obtain via [`MoonClient::temporal`](crate::MoonClient::temporal).
pub struct TemporalClient {
    pub(crate) conn: redis::aio::ConnectionManager,
}

impl TemporalClient {
    /// Publish a temporal checkpoint at the current wall-clock time.
    ///
    /// The server captures the timestamp itself — no argument is accepted — and
    /// records a `wall_ms -> LSN` binding in the shard's temporal registry.
    ///
    /// This does NOT pin the connection to a snapshot view; nothing about
    /// subsequent reads on this connection changes. The binding exists so a
    /// LATER query can name that instant: `FT.SEARCH … AS_OF <wall_ms>`
    /// resolves through this registry (see
    /// [`VectorClient::search_opts`](crate::VectorClient::search_opts)'s
    /// `as_of`). Take a checkpoint first, keep the timestamp, query against it
    /// afterwards.
    pub async fn snapshot_at(&mut self) -> Result<()> {
        redis::cmd("TEMPORAL.SNAPSHOT_AT")
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    // `snapshot_at_packed` was removed in 0.3.0.
    //
    // It sent `TEMPORAL.SNAPSHOT_AT <packed_hlc>`, and the server's
    // `validate_snapshot_at` rejects ANY argument — the command captures the
    // timestamp itself. Every call answered `ERR wrong number of arguments`.
    // The doc claimed "Moon parses the stringified value via BIGNUM"; nothing
    // in the server ever did.
    //
    // This one is why the wire-form guard sends real arguments as well as bare
    // names: the command name was right, so a name-only sweep saw nothing
    // wrong. Pinning AS_OF to a historical timestamp has no server support at
    // all — use [`snapshot_at`](Self::snapshot_at), which pins to now.

    // `release_snapshot` was removed in 0.3.0.
    //
    // It sent a bare `TEMPORAL.INVALIDATE`, but that command is the 3-arg
    // entity form below (`validate_invalidate` requires exactly
    // `<entity_id> <NODE|EDGE> <graph>`), so every call answered `ERR wrong
    // number of arguments`. Found by the round-trip guard, not by review —
    // this one had survived a name-level audit because `TEMPORAL.INVALIDATE`
    // is a command Moon really does have.
    //
    // There is no replacement because there is nothing to release: the doc's
    // premise was wrong. `TEMPORAL.SNAPSHOT_AT` never pinned the connection to
    // a snapshot view — it records a shard-global `wall_ms -> LSN` binding
    // that `AS_OF` resolves later. No pin is taken, so no pin can be dropped,
    // and a connection is never in "snapshot mode" to return from.
    //
    // Callers that were relying on this to restore live reads can simply
    // delete the call; their reads were already live.

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
