use crate::protocol::Frame;
use crate::storage::Database;

/// Storage abstraction for cold-path consumers (persistence replay, Lua scripting).
///
/// This trait provides a command-level interface to the storage layer,
/// decoupling persistence and scripting from the concrete `Database` type.
/// Hot-path command handlers continue to use `Database` directly.
pub trait StorageEngine {
    /// Execute a command against the storage, returning the response frame.
    ///
    /// `selected_db` may be mutated by SELECT commands.
    fn execute_command(
        &mut self,
        cmd: &[u8],
        args: &[Frame],
        selected_db: &mut usize,
        db_count: usize,
    ) -> Frame;
}

impl StorageEngine for Database {
    fn execute_command(
        &mut self,
        cmd: &[u8],
        args: &[Frame],
        selected_db: &mut usize,
        db_count: usize,
    ) -> Frame {
        match crate::command::dispatch(self, cmd, args, selected_db, db_count) {
            crate::command::DispatchResult::Response(f)
            | crate::command::DispatchResult::Quit(f) => f,
        }
    }
}
