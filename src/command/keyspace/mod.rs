//! Keyspace-scoped commands that require access to multiple databases on the same shard.
//!
//! Commands here (MOVE, COPY DB n, ...) cannot go through the central `dispatch()`
//! function which only receives a single `&mut Database`. Each command provides:
//! - A pure `*_core()` function for two-database data-plane logic (testable without handlers)
//! - Helper functions (`with_two_dbs_locked`, `with_two_slice_dbs`) for safe two-db access
//! - Handler-layer intercept wiring handled in each of the four handler paths

pub mod move_cmd;
