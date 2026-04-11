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
/// purposes, centralizing the dependency. When the `graph` feature is
/// enabled, graph WAL commands (GRAPH.CREATE, GRAPH.ADDNODE, etc.) are
/// collected into a `GraphReplayCollector` instead of being dispatched
/// to the KV command path. After replay, call `replay_graph_commands()`
/// to apply them to a `GraphStore`.
pub struct DispatchReplayEngine {
    #[cfg(feature = "graph")]
    graph_collector: std::cell::RefCell<crate::graph::replay::GraphReplayCollector>,
}

impl DispatchReplayEngine {
    /// Create a new replay engine.
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "graph")]
            graph_collector: std::cell::RefCell::new(
                crate::graph::replay::GraphReplayCollector::new(),
            ),
        }
    }

    /// Replay collected graph commands into a `GraphStore`.
    ///
    /// Must be called after WAL/AOF replay completes. Returns the number
    /// of graph commands successfully replayed.
    #[cfg(feature = "graph")]
    pub fn replay_graph_commands(
        &self,
        store: &mut crate::graph::store::GraphStore,
    ) -> usize {
        self.graph_collector.borrow().replay_into(store)
    }

    /// Number of graph commands collected during replay.
    #[cfg(feature = "graph")]
    pub fn graph_command_count(&self) -> usize {
        self.graph_collector.borrow().command_count()
    }
}

impl CommandReplayEngine for DispatchReplayEngine {
    fn replay_command(
        &self,
        databases: &mut [Database],
        cmd: &[u8],
        args: &[Frame],
        selected_db: &mut usize,
    ) {
        // Intercept graph commands and route to the collector instead of KV dispatch.
        // Graph WAL records are collected during the first pass, then replayed in
        // correct order (creates -> nodes -> edges -> removes -> drops) via
        // replay_graph_commands() after all KV replay completes.
        #[cfg(feature = "graph")]
        {
            if crate::graph::replay::GraphReplayCollector::is_graph_command(cmd) {
                // Check if any args are Integer frames (node/edge IDs may be
                // encoded as RESP integers). If so, convert to string bytes
                // for GraphReplayCollector which expects all-text args.
                let has_integers = args.iter().any(|f| matches!(f, Frame::Integer(_)));
                if has_integers {
                    let owned: smallvec::SmallVec<[Vec<u8>; 8]> = args
                        .iter()
                        .filter_map(|f| match f {
                            Frame::BulkString(b) => Some(b.to_vec()),
                            Frame::Integer(i) => Some(i.to_string().into_bytes()),
                            _ => None,
                        })
                        .collect();
                    let refs: smallvec::SmallVec<[&[u8]; 8]> =
                        owned.iter().map(|v| v.as_slice()).collect();
                    self.graph_collector
                        .borrow_mut()
                        .collect_command(cmd, &refs);
                } else {
                    let bulk_args: smallvec::SmallVec<[&[u8]; 8]> = args
                        .iter()
                        .filter_map(|f| match f {
                            Frame::BulkString(b) => Some(b.as_ref()),
                            _ => None,
                        })
                        .collect();
                    self.graph_collector
                        .borrow_mut()
                        .collect_command(cmd, &bulk_args);
                }
                return;
            }
        }

        let db_count = databases.len();
        if *selected_db >= db_count {
            tracing::warn!(
                "WAL replay: selected_db {} out of range (have {} databases), resetting to 0",
                *selected_db,
                db_count
            );
            *selected_db = 0;
        }
        let _ = crate::command::dispatch(
            &mut databases[*selected_db],
            cmd,
            args,
            selected_db,
            db_count,
        );
    }
}
