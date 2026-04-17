pub mod affinity;
pub mod blocking;
pub mod core;
#[cfg(feature = "runtime-monoio")]
pub mod handler_monoio;
#[cfg(feature = "runtime-tokio")]
pub mod handler_sharded;
#[cfg(feature = "runtime-tokio")]
pub mod handler_single;
pub mod shared;
#[cfg(all(test, feature = "runtime-monoio"))]
mod tests;
pub mod util;

// Re-export for internal use
#[allow(unused_imports)]
pub(crate) use self::core::{ConnectionContext, ConnectionState};
#[allow(unused_imports)]
pub(crate) use affinity::{AffinityTracker, MigratedConnectionState};
pub(crate) use blocking::convert_blocking_to_nonblocking;
#[cfg(feature = "runtime-tokio")]
pub(crate) use blocking::handle_blocking_command;
#[cfg(feature = "runtime-monoio")]
pub(crate) use blocking::handle_blocking_command_monoio;
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
pub(crate) use util::{
    apply_resp3_conversion, extract_bytes, extract_command, propagate_subscription,
    restore_migrated_state, unpropagate_subscription,
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
pub(crate) fn record_txn_kv_write(
    txn: &mut CrossStoreTxn,
    db: &mut Database,
    key: &bytes::Bytes,
    is_delete: bool,
) {
    if is_delete {
        // Record delete (no rollback action needed - key is being removed)
        txn.record_kv_delete(key.clone());
    } else {
        // Check if key exists for insert vs update
        match db.get(key) {
            Some(old_entry) => txn.record_kv_update(key.clone(), old_entry.clone()),
            None => txn.record_kv_insert(key.clone()),
        }
    }
}
