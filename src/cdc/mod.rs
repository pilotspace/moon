//! Change Data Capture (CDC) for Moon.
//!
//! Translates raw WAL v3 records into typed `CdcEvent`s and ships them to
//! consumers as Debezium-compatible JSON envelopes. The transport layer
//! (RESP3 Push frames over `CDC.SUBSCRIBE`) lives in `src/command/cdc/`.
//!
//! ## Pipeline
//!
//! ```text
//!  WAL v3 record  --decode-->  CdcEvent  --debezium-->  JSON bytes
//!                                ^                          |
//!                                |                          v
//!                          (typed enum)           consumer (Kafka Connect,
//!                                                  Flink CDC, etc.)
//! ```
//!
//! ## Why Debezium JSON?
//!
//! Lets Moon plug into the existing ETL ecosystem on day one — Kafka
//! Connect sinks, Flink CDC, Materialize, Decodable, every Debezium-aware
//! tool already speaks this envelope. The cost is ~3× the bytes of a
//! Moon-native binary format, which we accept for v0.2 in exchange for
//! ecosystem reach. A `FORMAT raw` mode for the latency-sensitive case
//! can land in a follow-up.

pub mod debezium;
pub mod decode;
pub mod event;

pub use debezium::{DebeziumEnvelope, encode_debezium};
pub use decode::decode_wal_record;
pub use event::{CdcEvent, CdcOp};
