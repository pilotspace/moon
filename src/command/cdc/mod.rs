//! `CDC.*` command handlers.
//!
//! C3 v1 ships `CDC.READ` -- a polling-based read-from-disk endpoint that
//! lets external consumers drain Debezium-formatted change events out of a
//! Moon shard's WAL directory. Push-based streaming over RESP3 Push frames
//! is C3b (separate phase) -- it requires hooking the shard event loop
//! after `wal_append_and_fanout` and introducing a new `ShardMessage`
//! variant. The Rust API in `crate::cdc::*` plus this disk-polling command
//! covers every existing CDC adoption path today (Kafka Connect with a
//! polling source, Flink CDC source).

pub mod read;

pub use read::cdc_read;
