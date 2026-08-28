pub mod affinity;
pub mod blocking;
/// moon#556/#557: runtime-agnostic tests for the blocking pre-registration
/// scan. Separate from `tests` below, which only compiles under monoio.
#[cfg(test)]
mod blocking_tests;
pub mod blocking_txn;
pub mod core;
pub(crate) mod fanout;
#[cfg(feature = "runtime-monoio")]
pub mod handler_monoio;
#[cfg(feature = "runtime-tokio")]
pub mod handler_sharded;
#[cfg(feature = "runtime-tokio")]
pub mod handler_single;
pub mod intercept;
pub mod monitor_mode;
pub mod park_policy;
pub mod shared;
pub mod subscriber_mode;
#[cfg(all(test, feature = "runtime-monoio"))]
mod tests;
pub mod util;
pub mod watch;

// Re-export for internal use
#[allow(unused_imports)]
pub(crate) use self::core::{ConnectionContext, ConnectionState};
#[allow(unused_imports)]
pub(crate) use affinity::{AffinityTracker, MigratedConnectionState};
#[cfg(feature = "runtime-tokio")]
pub(crate) use blocking::handle_blocking_command;
#[cfg(feature = "runtime-monoio")]
pub(crate) use blocking::handle_blocking_command_monoio;
pub(crate) use blocking::queued_blocking_frame;
#[cfg(feature = "runtime-monoio")]
#[allow(unused_imports)]
pub(crate) use blocking::try_inline_dispatch;
#[cfg(feature = "runtime-monoio")]
pub(crate) use blocking::try_inline_dispatch_loop;
#[cfg(feature = "runtime-tokio")]
pub(crate) use shared::{SharedDatabases, execute_transaction};
pub(crate) use shared::{
    execute_transaction_sharded, extract_primary_key, handle_config, is_multi_key_command,
};
#[allow(unused_imports)]
pub(crate) use util::{
    apply_resp3_conversion, extract_bytes, extract_command, propagate_shard_subscription,
    propagate_subscription, resp3_shape_for, restore_migrated_state,
    unpropagate_shard_subscription, unpropagate_subscription,
};

// Re-export handler functions at the module level so external callers
// can use crate::server::conn::{handle_connection_sharded, ...}
#[cfg(feature = "runtime-monoio")]
#[allow(unused_imports)]
pub(crate) use handler_monoio::handle_connection_sharded_monoio;
#[cfg(feature = "runtime-tokio")]
#[allow(unused_imports)]
pub(crate) use handler_sharded::{handle_connection_sharded, handle_connection_sharded_inner};
#[cfg(feature = "runtime-tokio")]
pub use handler_single::handle_connection;

use crate::storage::Database;
use crate::transaction::CrossStoreTxn;

/// Record a KV write operation in the active cross-store transaction.
///
/// Called by handlers BEFORE executing SET/HSET/DEL/etc. when `in_cross_txn()`.
/// Records the before-image for rollback on TXN.ABORT.
///
/// # Arguments
/// - `txn`: Mutable reference to the active transaction
/// - `db`: Mutable reference to the database (for reading current value via get())
/// - `key`: The key being modified
/// - `is_delete`: True if this is a delete operation (DEL, HDEL, etc.)
#[inline]
#[allow(dead_code)] // API reserved for future handler-level KV transaction tracking
pub(crate) fn record_txn_kv_write(
    txn: &mut CrossStoreTxn,
    db: &mut Database,
    key: &bytes::Bytes,
    is_delete: bool,
) {
    if is_delete {
        // Capture before-image so TXN.ABORT can restore the deleted key.
        if let Some(old_entry) = db.get(key) {
            txn.record_kv_delete(key.clone(), old_entry.clone());
        }
        // If the key doesn't exist, a delete is a no-op — nothing to undo.
    } else {
        // Check if key exists for insert vs update
        match db.get(key) {
            Some(old_entry) => txn.record_kv_update(key.clone(), old_entry.clone()),
            None => txn.record_kv_insert(key.clone()),
        }
    }
}
