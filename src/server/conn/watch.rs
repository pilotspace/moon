//! `WATCH` / `UNWATCH` — the optimistic-locking half of Redis transactions.
//!
//! Lives in its own module rather than inside each handler because this task
//! (`watch-cas-transactions`) exists precisely because the three dispatch paths
//! drifted: `handler_single` re-checked the recorded versions at `EXEC` while
//! `handler_monoio` and `handler_sharded` parsed `WATCH`, answered `+OK`, and
//! then never consulted the watch set again. Two byte-identical copies of the
//! command arm would have re-created the same failure mode one edit later, so
//! both production handlers call this one function.
//!
//! `handler_single` keeps its own inline arm: it holds the database lock
//! directly and has no shard mesh to hop, so it shares no code with this path.

use bytes::Bytes;

use crate::protocol::Frame;
use crate::server::conn::core::{ConnectionContext, ConnectionState};
use crate::server::conn::shared::WatchToken;

/// Handle `WATCH` / `UNWATCH`, returning `true` when the command was consumed.
///
/// MUST be called BEFORE the `MULTI` queueing step, so `WATCH` inside a
/// transaction is refused rather than queued as an ordinary command.
pub(crate) async fn try_handle_watch_unwatch(
    cmd: &[u8],
    args: &[Frame],
    conn: &mut ConnectionState,
    ctx: &ConnectionContext,
    responses: &mut Vec<Frame>,
) -> bool {
    if cmd.eq_ignore_ascii_case(b"WATCH") {
        if args.is_empty() {
            responses.push(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'watch' command",
            )));
        } else if conn.in_multi {
            responses.push(Frame::Error(Bytes::from_static(
                b"ERR WATCH inside MULTI is not allowed",
            )));
        } else {
            let keys: Vec<Bytes> = args
                .iter()
                .filter_map(|f| match f {
                    Frame::BulkString(b) => Some(b.clone()),
                    Frame::SimpleString(b) => Some(b.clone()),
                    _ => None,
                })
                .collect();
            // Snapshot each key's version WHERE IT LIVES. Reading the local
            // slice for a remote key would read a different database entirely.
            let versions = crate::shard::coordinator::snapshot_versions(
                &keys,
                ctx.shard_id,
                ctx.num_shards,
                conn.selected_db,
                &ctx.dispatch_tx,
                &ctx.spsc_notifiers,
            )
            .await;
            for (k, v) in keys.into_iter().zip(versions) {
                conn.watched_keys.insert(k, WatchToken { version: v });
            }
            responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
        }
        return true;
    }

    if cmd.eq_ignore_ascii_case(b"UNWATCH") {
        if !args.is_empty() {
            responses.push(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'unwatch' command",
            )));
        } else {
            conn.watched_keys.clear();
            responses.push(Frame::SimpleString(Bytes::from_static(b"OK")));
        }
        return true;
    }

    false
}
