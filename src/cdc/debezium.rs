//! Debezium-compatible JSON envelope for CDC events.
//!
//! The envelope mirrors the schema documented at
//! <https://debezium.io/documentation/reference/stable/connectors/postgresql.html>.
//! We emit a minimal subset:
//!
//! ```json
//! {
//!   "op": "c|u|d|r",
//!   "ts_ms": 1735689600000,
//!   "source": {
//!     "version": "moon-0.2",
//!     "shard": 0,
//!     "lsn": 12345,
//!     "record_type": "kv|temporal_upsert|graph_temporal|checkpoint|other"
//!   },
//!   "before": null,
//!   "after": { ... }
//! }
//! ```
//!
//! We do not emit the optional `schema` block — consumers using Schema
//! Registry can derive schemas from this typed payload, and most modern
//! sinks (Kafka Connect Single-Message Transforms, Flink CDC, Materialize)
//! work without it.

use bytes::Bytes;
use std::fmt::Write as _;

use super::event::CdcEvent;
#[cfg(test)]
use super::event::CdcOp;

/// Compile-time identifier for the Moon producer version. Bumped whenever
/// the envelope schema changes in a way that consumers should detect.
pub const MOON_CDC_VERSION: &str = "moon-0.2";

/// Owned JSON-encoded envelope. Type alias kept thin so callers can hold
/// it as `Bytes` (zero-copy fan-out via existing pubsub backlog plumbing).
pub type DebeziumEnvelope = Bytes;

/// Encode a `CdcEvent` into a Debezium JSON envelope.
///
/// The encoder writes directly to a pre-sized `String` buffer to avoid
/// pulling in `serde_json` for a hot path. Output is canonical (stable key
/// order) so golden-file regression tests are stable.
pub fn encode_debezium(event: &CdcEvent, ts_ms: i64) -> DebeziumEnvelope {
    // 256 bytes covers the envelope skeleton + small keys. Larger payloads
    // grow the String naturally.
    let mut out = String::with_capacity(256);
    out.push('{');
    write_kv_str(&mut out, "op", event.op().debezium_code(), true);
    write_kv_i64(&mut out, "ts_ms", ts_ms, false);
    out.push_str(",\"source\":{");
    write_kv_str(&mut out, "version", MOON_CDC_VERSION, true);
    write_kv_u16(&mut out, "shard", event.shard(), false);
    write_kv_u64(&mut out, "lsn", event.lsn(), false);
    write_kv_str(&mut out, "record_type", record_type_tag(event), false);
    out.push('}');
    // Per-event "after" payload. "before" is always null in v0.2.
    out.push_str(",\"before\":null,\"after\":");
    write_after(&mut out, event);
    out.push('}');
    Bytes::from(out.into_bytes())
}

fn record_type_tag(event: &CdcEvent) -> &'static str {
    match event {
        CdcEvent::KvCommand { .. } => "kv",
        CdcEvent::TemporalUpsert { .. } => "temporal_upsert",
        CdcEvent::GraphTemporal { .. } => "graph_temporal",
        CdcEvent::Checkpoint { .. } => "checkpoint",
        CdcEvent::Other { .. } => "other",
    }
}

fn write_after(out: &mut String, event: &CdcEvent) {
    match event {
        CdcEvent::KvCommand {
            command,
            key,
            raw_resp,
            ..
        } => {
            out.push('{');
            write_kv_opt_bytes(out, "command", command.as_deref(), true);
            write_kv_opt_bytes(out, "key", key.as_deref(), false);
            write_kv_bytes_b64(out, "raw_resp", raw_resp, false);
            out.push('}');
        }
        CdcEvent::TemporalUpsert {
            key,
            value,
            valid_from,
            system_from,
            ..
        } => {
            out.push('{');
            write_kv_bytes(out, "key", key, true);
            write_kv_bytes(out, "value", value, false);
            write_kv_i64(out, "valid_from", *valid_from, false);
            write_kv_i64(out, "system_from", *system_from, false);
            out.push('}');
        }
        CdcEvent::GraphTemporal {
            entity_id,
            is_node,
            valid_to,
            system_from,
            ..
        } => {
            out.push('{');
            write_kv_u64(out, "entity_id", *entity_id, true);
            let _ = write!(out, ",\"is_node\":{}", is_node);
            write_kv_i64(out, "valid_to", *valid_to, false);
            write_kv_i64(out, "system_from", *system_from, false);
            out.push('}');
        }
        CdcEvent::Checkpoint { redo_lsn, .. } => {
            out.push('{');
            write_kv_u64(out, "redo_lsn", *redo_lsn, true);
            out.push('}');
        }
        CdcEvent::Other {
            record_type,
            payload,
            ..
        } => {
            out.push('{');
            write_kv_u16(out, "record_type", *record_type as u16, true);
            write_kv_bytes_b64(out, "payload", payload, false);
            out.push('}');
        }
    }
}

// ── JSON micro-encoder helpers ───────────────────────────────────────────
// Why hand-rolled? CDC is on the write fan-out hot path. Pulling in
// serde_json adds a non-trivial allocation profile and locks us into its
// number-format quirks. Strings here are all short, key sets are known
// at compile time — a one-pass `write!`-style encoder beats it on both
// allocations and code size.

fn write_kv_str(out: &mut String, key: &str, value: &str, first: bool) {
    if !first {
        out.push(',');
    }
    out.push('"');
    out.push_str(key);
    out.push_str("\":");
    write_json_string(out, value.as_bytes());
}

fn write_kv_bytes(out: &mut String, key: &str, value: &[u8], first: bool) {
    if !first {
        out.push(',');
    }
    out.push('"');
    out.push_str(key);
    out.push_str("\":");
    write_json_string(out, value);
}

fn write_kv_opt_bytes(out: &mut String, key: &str, value: Option<&[u8]>, first: bool) {
    if !first {
        out.push(',');
    }
    out.push('"');
    out.push_str(key);
    out.push_str("\":");
    match value {
        Some(v) => write_json_string(out, v),
        None => out.push_str("null"),
    }
}

fn write_kv_bytes_b64(out: &mut String, key: &str, value: &[u8], first: bool) {
    if !first {
        out.push(',');
    }
    out.push('"');
    out.push_str(key);
    out.push_str("\":\"");
    base64_encode(out, value);
    out.push('"');
}

fn write_kv_u64(out: &mut String, key: &str, value: u64, first: bool) {
    if !first {
        out.push(',');
    }
    let _ = write!(out, "\"{}\":{}", key, value);
}

fn write_kv_u16(out: &mut String, key: &str, value: u16, first: bool) {
    if !first {
        out.push(',');
    }
    let _ = write!(out, "\"{}\":{}", key, value);
}

fn write_kv_i64(out: &mut String, key: &str, value: i64, first: bool) {
    if !first {
        out.push(',');
    }
    let _ = write!(out, "\"{}\":{}", key, value);
}

/// Write a JSON string literal. If `value` is valid UTF-8 we emit it with
/// minimal escaping; otherwise we fall back to a JSON object signalling a
/// base64-encoded payload so binary keys/values remain representable.
fn write_json_string(out: &mut String, value: &[u8]) {
    match std::str::from_utf8(value) {
        Ok(s) => {
            out.push('"');
            for c in s.chars() {
                match c {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\t' => out.push_str("\\t"),
                    c if (c as u32) < 0x20 => {
                        let _ = write!(out, "\\u{:04x}", c as u32);
                    }
                    c => out.push(c),
                }
            }
            out.push('"');
        }
        Err(_) => {
            // Binary fallback — opaque base64 payload + sentinel marker.
            out.push_str("{\"_b64\":\"");
            base64_encode(out, value);
            out.push_str("\"}");
        }
    }
}

/// Minimal base64 encoder (RFC 4648, no padding compression). Pulled in
/// inline rather than via a dependency — we encode short payloads and
/// don't need SIMD.
fn base64_encode(out: &mut String, input: &[u8]) {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut i = 0;
    while i + 3 <= input.len() {
        let b = ((input[i] as u32) << 16) | ((input[i + 1] as u32) << 8) | (input[i + 2] as u32);
        out.push(ALPHABET[((b >> 18) & 0x3F) as usize] as char);
        out.push(ALPHABET[((b >> 12) & 0x3F) as usize] as char);
        out.push(ALPHABET[((b >> 6) & 0x3F) as usize] as char);
        out.push(ALPHABET[(b & 0x3F) as usize] as char);
        i += 3;
    }
    let rem = input.len() - i;
    if rem == 1 {
        let b = (input[i] as u32) << 16;
        out.push(ALPHABET[((b >> 18) & 0x3F) as usize] as char);
        out.push(ALPHABET[((b >> 12) & 0x3F) as usize] as char);
        out.push('=');
        out.push('=');
    } else if rem == 2 {
        let b = ((input[i] as u32) << 16) | ((input[i + 1] as u32) << 8);
        out.push(ALPHABET[((b >> 18) & 0x3F) as usize] as char);
        out.push(ALPHABET[((b >> 12) & 0x3F) as usize] as char);
        out.push(ALPHABET[((b >> 6) & 0x3F) as usize] as char);
        out.push('=');
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    /// C2 — KvCommand SET envelope matches the documented Debezium schema.
    /// Golden-file style assertion: the envelope must be byte-identical
    /// across runs so consumer code can rely on the field order.
    #[test]
    fn test_debezium_envelope_kv_set_golden() {
        let event = CdcEvent::KvCommand {
            op: CdcOp::Upsert,
            command: Some(Bytes::from_static(b"SET")),
            key: Some(Bytes::from_static(b"hello")),
            raw_resp: Bytes::from_static(b"*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n"),
            lsn: 42,
            shard: 0,
        };
        let json = encode_debezium(&event, 1_700_000_000_000);
        let s = std::str::from_utf8(&json).unwrap();
        let expected = concat!(
            "{",
            "\"op\":\"u\",",
            "\"ts_ms\":1700000000000,",
            "\"source\":{",
            "\"version\":\"moon-0.2\",",
            "\"shard\":0,",
            "\"lsn\":42,",
            "\"record_type\":\"kv\"",
            "},",
            "\"before\":null,",
            "\"after\":{",
            "\"command\":\"SET\",",
            "\"key\":\"hello\",",
            "\"raw_resp\":\"KjMNCiQzDQpTRVQNCiQ1DQpoZWxsbw0KJDUNCndvcmxkDQo=\"",
            "}",
            "}",
        );
        assert_eq!(s, expected);
    }

    /// C2 — Delete envelope uses `op=d`.
    #[test]
    fn test_debezium_envelope_delete_op() {
        let event = CdcEvent::KvCommand {
            op: CdcOp::Delete,
            command: Some(Bytes::from_static(b"DEL")),
            key: Some(Bytes::from_static(b"k")),
            raw_resp: Bytes::from_static(b"*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n"),
            lsn: 1,
            shard: 0,
        };
        let json = encode_debezium(&event, 0);
        let s = std::str::from_utf8(&json).unwrap();
        assert!(s.starts_with("{\"op\":\"d\","));
        assert!(s.contains("\"record_type\":\"kv\""));
        assert!(s.contains("\"key\":\"k\""));
    }

    /// C2 — TemporalUpsert envelope carries valid_from and system_from.
    #[test]
    fn test_debezium_envelope_temporal_upsert() {
        let event = CdcEvent::TemporalUpsert {
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v"),
            valid_from: 100,
            system_from: 200,
            lsn: 7,
            shard: 3,
        };
        let json = encode_debezium(&event, 1000);
        let s = std::str::from_utf8(&json).unwrap();
        assert!(s.contains("\"op\":\"u\""));
        assert!(s.contains("\"record_type\":\"temporal_upsert\""));
        assert!(s.contains("\"valid_from\":100"));
        assert!(s.contains("\"system_from\":200"));
        assert!(s.contains("\"shard\":3"));
        assert!(s.contains("\"lsn\":7"));
    }

    /// C2 — Checkpoint envelope exposes redo_lsn.
    #[test]
    fn test_debezium_envelope_checkpoint() {
        let event = CdcEvent::Checkpoint {
            redo_lsn: 555,
            lsn: 600,
            shard: 1,
        };
        let json = encode_debezium(&event, 1);
        let s = std::str::from_utf8(&json).unwrap();
        assert!(s.contains("\"op\":\"r\""));
        assert!(s.contains("\"record_type\":\"checkpoint\""));
        assert!(s.contains("\"redo_lsn\":555"));
    }

    /// C2 — Binary key (non-UTF8) falls back to base64-tagged JSON.
    #[test]
    fn test_debezium_envelope_binary_key_fallback() {
        let event = CdcEvent::TemporalUpsert {
            key: Bytes::from_static(&[0xFFu8, 0xFE, 0x00, 0x80]),
            value: Bytes::from_static(b"v"),
            valid_from: 0,
            system_from: 0,
            lsn: 1,
            shard: 0,
        };
        let json = encode_debezium(&event, 0);
        let s = std::str::from_utf8(&json).unwrap();
        // The binary key must be wrapped in the {"_b64": "..."} sentinel so
        // consumers know it's not a literal UTF-8 string.
        assert!(s.contains("\"key\":{\"_b64\":\""));
    }
}
