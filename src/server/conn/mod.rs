pub mod affinity;
pub mod blocking;
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
pub(crate) use affinity::{AffinityTracker, MigratedConnectionState};
pub(crate) use blocking::convert_blocking_to_nonblocking;
#[cfg(feature = "runtime-tokio")]
pub(crate) use blocking::handle_blocking_command;
#[cfg(feature = "runtime-monoio")]
pub(crate) use blocking::handle_blocking_command_monoio;
#[cfg(feature = "runtime-monoio")]
pub(crate) use blocking::try_inline_dispatch_loop;
#[cfg(feature = "runtime-tokio")]
pub(crate) use shared::{SharedDatabases, execute_transaction};
pub(crate) use shared::{
    execute_transaction_sharded, extract_primary_key, handle_config, is_multi_key_command,
};
pub(crate) use util::{
    apply_resp3_conversion, extract_bytes, extract_command, restore_migrated_state,
};

// Re-export handler functions at the module level so external callers
// can use crate::server::conn::{handle_connection_sharded, ...}
#[cfg(feature = "runtime-monoio")]
pub use handler_monoio::handle_connection_sharded_monoio;
#[cfg(feature = "runtime-tokio")]
pub use handler_sharded::{handle_connection_sharded, handle_connection_sharded_inner};
#[cfg(feature = "runtime-tokio")]
pub use handler_single::handle_connection;
