//! The tokio connection's ONE exit epilogue (moon#1299 R1 F1).
//!
//! A cross-store `TXN` holds every key it wrote until it commits or aborts,
//! so a connection that leaves with a transaction open MUST roll it back —
//! otherwise its holds, write intents and uncommitted writes outlive it for
//! the life of the process (every other writer answered `-TXNCONFLICT`,
//! `FLUSHALL` refused, expiry and eviction skipping the keys).
//!
//! That rollback used to sit at the tail of the connection body, and the
//! body's early `return`s skipped it (a blocked `BLPOP` whose peer went away,
//! a failed or over-limit reply write, a fatal cross-shard reply, the
//! subscriber loop's protocol fault, …). It now lives HERE, in the function
//! every caller enters: the body borrows the [`ConnectionState`] this
//! wrapper owns, so however the body leaves — a `return` anywhere, the
//! loop's `break`, a hand-off result — control comes back to this function
//! with the state still in hand, and the epilogue runs before the result
//! reaches the caller. No exit, present or future, can skip it without
//! editing this file.
//!
//! Hand-off: `MigrateConnection` never carries a transaction —
//! `ConnectionState::migration_eligible` requires `active_cross_txn.is_none()`
//! — so the epilogue is a no-op there. Should that gate ever regress, the
//! transaction is rolled back rather than leaked: the migrated state has no
//! field that could carry it. This runtime has no master-side `PSYNC`
//! hijack.

use bytes::BytesMut;

use super::{HandlerResult, handle_connection_body};
use crate::runtime::cancel::CancellationToken;
use crate::server::conn::affinity::MigratedConnectionState;
use crate::server::conn::core::{ConnectionContext, ConnectionState};
use crate::server::conn::txn_abort::{AbortCause, end_open_txn};

/// Generic inner handler for sharded connections (Tokio runtime): builds the
/// connection's state, serves it (`handle_connection_body`), then runs the
/// exit epilogue whatever the body returned — see the module doc.
///
/// Works with any stream implementing `AsyncRead + AsyncWrite + Unpin`,
/// enabling both plain TCP (`TcpStream`) and TLS
/// (`tokio_rustls::server::TlsStream<TcpStream>`). Returns
/// `(HandlerResult, Option<S>)`: the stream is returned when migration is
/// triggered so the concrete caller can extract the raw FD. `can_migrate`
/// controls whether the AffinityTracker is active (set to `false` for TLS
/// connections).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_connection_sharded_inner<
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
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
) -> (HandlerResult, Option<S>) {
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
        kill_fd,
        &mut conn,
    )
    .await;

    // --- the exit epilogue: every exit of the body arrives here -----------
    // A refusal of the rollback's log records is counted by the pool and
    // logged by `abort_logged` (moon#1285 review MINOR 5); there is no
    // client left to tell. This runtime serves no replicas (`None`).
    debug_assert!(
        !matches!(result.0, HandlerResult::MigrateConnection { .. })
            || conn.active_cross_txn.is_none(),
        "a migrated connection must not carry an open TXN"
    );
    let _ = end_open_txn(ctx, &mut conn, None, AbortCause::Disconnect).await;
    result
}
