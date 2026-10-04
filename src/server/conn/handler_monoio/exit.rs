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
//!
//! Close order (moon#1299 R2 N1): the socket is closed HERE, after the
//! epilogue — never by the body. The rollback awaits the AOF's fsync barrier
//! under `appendfsync always` and releases the holds only at its end; a body
//! that closed first let a client read its `QUIT` reply (or its
//! protocol-error reply) and EOF, then find its keys still held from
//! another connection. The body hands the stream back in a [`BodyExit`]
//! saying how to close it.

use bytes::BytesMut;

use super::{MonoioHandlerResult, ParkArgs, handle_connection_body, idle_park};
use crate::runtime::cancel::CancellationToken;
use crate::server::conn::affinity::MigratedConnectionState;
use crate::server::conn::core::{ConnectionContext, ConnectionState};
use crate::server::conn::txn_abort::{AbortCause, end_open_txn};

/// How the connection body hands its socket back to this wrapper
/// (moon#1299 R2 N1): what to do with it AFTER the exit epilogue.
pub(super) enum BodyExit<S> {
    /// A hand-off (`MigrateConnection`, `ParkIdle`, `HijackForPsync`): the
    /// stream goes back to the caller, open.
    HandOff(S),
    /// An early exit — a protocol fault, subscriber `QUIT`/fault, a failed,
    /// timed-out or over-limit reply write, a vanished blocked peer: closed
    /// by dropping it, as before. No graceful `shutdown()` here: on a peer
    /// that stopped reading, a TLS `close_notify` would park the task.
    Close(S),
    /// The loop's normal exit (EOF, `QUIT`, shutdown): a graceful
    /// `shutdown()` — FIN, and `close_notify` on TLS — so the socket does
    /// not linger in CLOSE_WAIT, then drop. monoio's own `shutdown()`
    /// manages the fd through the runtime (a raw `libc::shutdown` corrupts
    /// monoio's state).
    Shutdown(S),
}

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

    let (result, exit) = handle_connection_body(
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
            result,
            MonoioHandlerResult::MigrateConnection { .. } | MonoioHandlerResult::ParkIdle { .. }
        ) || conn.active_cross_txn.is_none(),
        "a migrated or parked connection must not carry an open TXN"
    );
    debug_assert!(
        matches!(exit, BodyExit::HandOff(_)) != matches!(result, MonoioHandlerResult::Done),
        "the stream is handed back open exactly when the result is a hand-off"
    );
    let _ = end_open_txn(
        ctx,
        &mut conn,
        super::ft::abort_replicator(ctx),
        AbortCause::Disconnect,
    )
    .await;
    // Only now may the client see the close (moon#1299 R2 N1).
    match exit {
        BodyExit::HandOff(stream) => (result, Some(stream)),
        BodyExit::Close(stream) => {
            drop(stream);
            (result, None)
        }
        BodyExit::Shutdown(mut stream) => {
            let _ = stream.shutdown().await;
            drop(stream);
            (result, None)
        }
    }
}
