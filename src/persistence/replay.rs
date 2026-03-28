use crate::protocol::Frame;
use crate::storage::Database;

/// Trait that abstracts command dispatch for AOF/WAL replay.
///
/// This decouples persistence replay from `command::dispatch`, allowing
/// replay logic to work through a trait object on the cold startup path.
pub trait CommandReplayEngine {
    /// Replay a single parsed command against the database slice.
    ///
    /// `selected_db` may be mutated by SELECT commands during replay.
    /// The response is intentionally discarded -- replay cares only about
    /// side effects on the databases.
    fn replay_command(
        &self,
        databases: &mut [Database],
        cmd: &[u8],
        args: &[Frame],
        selected_db: &mut usize,
    );
}

/// Concrete implementation that delegates to `command::dispatch`.
///
/// This is the **only** place that imports `command::dispatch` for replay
/// purposes, centralizing the dependency.
pub struct DispatchReplayEngine;

impl CommandReplayEngine for DispatchReplayEngine {
    fn replay_command(
        &self,
        databases: &mut [Database],
        cmd: &[u8],
        args: &[Frame],
        selected_db: &mut usize,
    ) {
        let db_count = databases.len();
        let _ = crate::command::dispatch(
            &mut databases[*selected_db],
            cmd,
            args,
            selected_db,
            db_count,
        );
    }
}
