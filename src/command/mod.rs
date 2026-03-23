use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Database;

/// Result of command dispatch.
pub enum DispatchResult {
    /// Normal response to send back to the client.
    Response(Frame),
    /// Response to send, then close the connection (QUIT).
    Quit(Frame),
}

/// Dispatch a command frame to the appropriate handler.
///
/// This is a stub that returns ERR for all commands. Task 3 replaces this
/// with the real dispatch implementation.
pub fn dispatch(
    _db: &mut Database,
    _frame: Frame,
    _selected_db: &mut usize,
    _db_count: usize,
) -> DispatchResult {
    DispatchResult::Response(Frame::Error(Bytes::from_static(b"ERR not implemented")))
}
