//! The monoio connection's ONE exit epilogue (moon#1299 R1 F1).
//!
//! A cross-store `TXN` holds every key it wrote until it commits or aborts,
//! so a connection that leaves with a transaction open MUST roll it back —
//! otherwise its holds, write intents and uncommitted writes outlive it for
//! the life of the process (every other writer answered `-TXNCONFLICT`,
//! `FLUSHALL` refused, expiry and eviction skipping the keys).
//!
//! That rollback used to sit at the tail of the connection body, and some
//! twenty exits `return`ed before reaching it (a protocol fault, a blocked
//! `BLPOP` whose peer went away, the subscriber loop's `QUIT` and faults, a
//! `PSYNC` hijack, …). It now lives HERE, in the function every caller
//! enters: the body borrows the [`ConnectionState`] this wrapper owns, so
//! however the body leaves — a `return` anywhere, the loop's `break`, a
//! hand-off result — control comes back to this function with the state
//! still in hand, and the epilogue runs before the result reaches the
//! caller. No exit, present or future, can skip it without editing this
//! file.
//!
//! Hand-offs:
//! - `HijackForPsync`: the socket stops being a client connection for good
//!   (it becomes a replication stream), so its transaction can never commit.
//!   It is rolled back here, BEFORE the stream is returned to the caller
//!   that starts the replica sync — the same outcome as a disconnect.
//! - `MigrateConnection` / `ParkIdle`: never carry a transaction —
//!   `ConnectionState::migration_eligible` and the park gate both require
//!   `active_cross_txn.is_none()` — so the epilogue is a no-op there. Should
//!   that gate ever regress, the transaction is rolled back rather than
//!   leaked: the migrated/parked state has no field that could carry it.

use bytes::BytesMut;

use super::{MonoioHandlerResult, ParkArgs, handle_connection_body, idle_park};
use crate::runtime::cancel::CancellationToken;
use crate::server::conn::affinity::MigratedConnectionState;
use crate::server::conn::core::{ConnectionContext, ConnectionState};
use crate::server::conn::txn_abort::{AbortCause, end_open_txn};

/// Monoio connection handler: builds the connection's state, serves it
/// (`handle_connection_body`), then runs the exit epilogue whatever the
/// body returned — see the module doc.
#[tracing::instrument(skip_all, level = "debug")]
pub(crate) async fn handle_connection_sharded_monoio<
    S: monoio::io::AsyncReadRent + monoio::io::AsyncWriteRent + idle_park::IdleParkRead,
>(
    stream: S,
    peer_addr: String,
    ctx: &ConnectionContext,
    shutdown: CancellationToken,
    client_id: u64,
    can_migrate: bool,
    initial_read_buf: BytesMut,
    migrated_state: Option<&MigratedConnectionState>,
    // Raw socket fd for CLIENT KILL force-close (R-3), or -1 if unavailable.
    kill_fd: i32,
    // c1M P1 park plumbing (see [`ParkArgs`]).
    park: ParkArgs,
) -> (MonoioHandlerResult, Option<S>) {
    let mut conn = ConnectionState::new(
        client_id,
        peer_addr.clone(),
        &ctx.requirepass,
        ctx.shard_id,
        ctx.num_shards,
        can_migrate,
        ctx.runtime_config.read().acllog_max_len,
        migrated_state,
    );
    conn.refresh_acl_cache(&ctx.acl_table);

    let result = handle_connection_body(
        stream,
        peer_addr,
        ctx,
        shutdown,
        client_id,
        initial_read_buf,
        migrated_state,
        kill_fd,
        park,
        &mut conn,
    )
    .await;

    // --- the exit epilogue: every exit of the body arrives here -----------
    // A refusal of the rollback's log records is counted by the pool and
    // logged by `abort_logged` (moon#1285 review MINOR 5); there is no
    // client left to tell.
    debug_assert!(
        !matches!(
            result.0,
            MonoioHandlerResult::MigrateConnection { .. } | MonoioHandlerResult::ParkIdle { .. }
        ) || conn.active_cross_txn.is_none(),
        "a migrated or parked connection must not carry an open TXN"
    );
    let _ = end_open_txn(
        ctx,
        &mut conn,
        super::ft::abort_replicator(ctx),
        AbortCause::Disconnect,
    )
    .await;
    result
}
