pub mod codec;
pub mod conn;
pub mod conn_state;
pub mod expiration;
pub mod listener;
pub mod shutdown;

// Backward-compatible re-export: callers using crate::server::connection::* still work
pub mod connection {
    #[cfg(feature = "runtime-tokio")]
    pub use super::conn::handler_single::handle_connection;
    #[cfg(feature = "runtime-tokio")]
    pub use super::conn::handler_sharded::{
        handle_connection_sharded, handle_connection_sharded_inner,
    };
    #[cfg(feature = "runtime-monoio")]
    pub use super::conn::handler_monoio::handle_connection_sharded_monoio;
    pub(crate) use super::conn::util::extract_bytes;
}
