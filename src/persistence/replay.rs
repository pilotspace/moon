use crate::protocol::Frame;
use crate::storage::Database;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::DispatchResult;
    use crate::framevec;

    fn make_db_with_key(key: &[u8], value: &[u8]) -> Database {
        let mut db = Database::new();
        let mut selected = 0usize;
        let args = framevec![
            Frame::BulkString(bytes::Bytes::copy_from_slice(key)),
            Frame::BulkString(bytes::Bytes::copy_from_slice(value)),
        ];
        let _ = crate::command::dispatch(&mut db, b"SET", &args, &mut selected, 16);
        db
    }

    fn get_key(db: &mut Database, key: &[u8]) -> Option<bytes::Bytes> {
        let mut selected = 0usize;
        let args = framevec![Frame::BulkString(bytes::Bytes::copy_from_slice(key))];
        match crate::command::dispatch(db, b"GET", &args, &mut selected, 16) {
            DispatchResult::Response(Frame::BulkString(v)) => Some(v),
            _ => None,
        }
    }

    // ── SWAPDB replay intercept ───────────────────────────────────────────────

    /// SWAPDB during WAL replay must exchange the two databases.
    #[test]
    fn replay_swapdb_exchanges_databases() {
        let engine = DispatchReplayEngine::new();
        let mut databases = vec![
            make_db_with_key(b"key_a", b"val_a"), // db-0
            make_db_with_key(b"key_b", b"val_b"), // db-1
        ];
        let mut selected = 0usize;
        let args = framevec![
            Frame::BulkString(bytes::Bytes::from_static(b"0")),
            Frame::BulkString(bytes::Bytes::from_static(b"1")),
        ];
        engine.replay_command(&mut databases, b"SWAPDB", &args, &mut selected);

        // After swap: db-0 has key_b, db-1 has key_a.
        assert_eq!(
            get_key(&mut databases[0], b"key_b").as_deref(),
            Some(b"val_b".as_ref()),
            "db-0 should have key_b after SWAPDB replay"
        );
        assert_eq!(
            get_key(&mut databases[1], b"key_a").as_deref(),
            Some(b"val_a".as_ref()),
            "db-1 should have key_a after SWAPDB replay"
        );
    }

    /// Same-index SWAPDB is a no-op during replay.
    #[test]
    fn replay_swapdb_same_index_noop() {
        let engine = DispatchReplayEngine::new();
        let mut databases = vec![make_db_with_key(b"mykey", b"myval"), Database::new()];
        let mut selected = 0usize;
        let args = framevec![
            Frame::BulkString(bytes::Bytes::from_static(b"0")),
            Frame::BulkString(bytes::Bytes::from_static(b"0")),
        ];
        engine.replay_command(&mut databases, b"SWAPDB", &args, &mut selected);
        assert_eq!(
            get_key(&mut databases[0], b"mykey").as_deref(),
            Some(b"myval".as_ref()),
            "db-0 should be unchanged after same-index SWAPDB replay"
        );
    }

    /// Out-of-range SWAPDB silently skips during replay (no panic).
    #[test]
    fn replay_swapdb_out_of_range_skips() {
        let engine = DispatchReplayEngine::new();
        let mut databases = vec![make_db_with_key(b"mykey", b"myval"), Database::new()];
        let mut selected = 0usize;
        let args = framevec![
            Frame::BulkString(bytes::Bytes::from_static(b"0")),
            Frame::BulkString(bytes::Bytes::from_static(b"99")),
        ];
        // Must not panic.
        engine.replay_command(&mut databases, b"SWAPDB", &args, &mut selected);
        assert_eq!(
            get_key(&mut databases[0], b"mykey").as_deref(),
            Some(b"myval".as_ref()),
            "db-0 should be unchanged after out-of-range SWAPDB replay"
        );
    }
}

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
    pub fn replay_graph_commands(&self, store: &mut crate::graph::store::GraphStore) -> usize {
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
                let collected = if has_integers {
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
                        .collect_command(cmd, &refs)
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
                        .collect_command(cmd, &bulk_args)
                };
                if !collected {
                    tracing::warn!(
                        "WAL replay: malformed graph command {:?} with {} args — skipping",
                        std::str::from_utf8(cmd).unwrap_or("<non-utf8>"),
                        args.len()
                    );
                }
                return;
            }
        }

        // SWAPDB requires access to the full database slice — intercept before
        // calling `command::dispatch` which only sees a single `&mut Database`.
        if cmd.eq_ignore_ascii_case(b"SWAPDB") {
            let a = args.first().and_then(|f| match f {
                Frame::BulkString(b) => std::str::from_utf8(b).ok()?.parse::<usize>().ok(),
                Frame::Integer(n) => usize::try_from(*n).ok(),
                _ => None,
            });
            let b_idx = args.get(1).and_then(|f| match f {
                Frame::BulkString(b) => std::str::from_utf8(b).ok()?.parse::<usize>().ok(),
                Frame::Integer(n) => usize::try_from(*n).ok(),
                _ => None,
            });
            match (a, b_idx) {
                (Some(a), Some(b)) if a != b && a < databases.len() && b < databases.len() => {
                    let (lo, hi) = if a < b { (a, b) } else { (b, a) };
                    // Split the slice to get two non-overlapping mutable references.
                    let (left, right) = databases.split_at_mut(lo + 1);
                    std::mem::swap(&mut left[lo], &mut right[hi - lo - 1]);
                }
                _ => {
                    // Out-of-range or same-index — silently skip (same as Redis).
                }
            }
            return;
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
