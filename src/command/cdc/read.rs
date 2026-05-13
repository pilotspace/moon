//! `CDC.READ <wal_dir> <from_lsn> [LIMIT N]`
//!
//! Drains a batch of WAL records starting at `from_lsn` and returns them
//! encoded as Debezium JSON envelopes. The response is an RESP array whose
//! first element is the LSN to use for the next poll, followed by N bulk
//! strings (one envelope each):
//!
//! ```text
//! *3
//!   :42                        // next_lsn -- pass back as <from_lsn> next poll
//!   $...{"op":"u",...,"lsn":40,...}
//!   $...{"op":"u",...,"lsn":41,...}
//! ```
//!
//! Notes / limitations (v1):
//! - **Polling**, not streaming. Push-based CDC ships in C3b.
//! - The handler opens a `WalTailReader` on every call. Consumers should
//!   request `LIMIT >= 100` to amortize the per-poll filesystem walk.
//! - When the WAL has no records `>= from_lsn`, returns `[from_lsn]` with
//!   no envelopes -- a stable "no new data" signal the consumer can detect
//!   without parsing the timestamp.

use bytes::Bytes;

use crate::cdc::{decode_wal_record, encode_debezium};
use crate::persistence::wal_v3::{TailCursor, WalTailReader};
use crate::protocol::{Frame, FrameVec};

/// Default events per poll if LIMIT is omitted. Sized to fit comfortably in
/// a 1 MB RESP response while still amortizing the per-poll cost.
const DEFAULT_CDC_LIMIT: usize = 256;

/// Hard ceiling -- prevents a runaway consumer from pinning a poll-thread.
const MAX_CDC_LIMIT: usize = 10_000;

/// Handle `CDC.READ <wal_dir> <from_lsn> [LIMIT N]`.
pub fn cdc_read(args: &[Frame]) -> Frame {
    // ── argument parsing ────────────────────────────────────────────
    let (wal_dir, from_lsn, limit) = match parse_args(args) {
        Ok(t) => t,
        Err(e) => return Frame::Error(Bytes::from(format!("ERR CDC.READ: {}", e))),
    };

    // ── drain ──────────────────────────────────────────────────────
    let mut tail = WalTailReader::new(&wal_dir, TailCursor::start());
    let mut envelopes: FrameVec = FrameVec::new();
    let mut next_lsn = from_lsn;
    let ts_ms = current_time_ms();

    while envelopes.len() < limit {
        match tail.read_next() {
            Ok(Some(rec)) => {
                if rec.lsn < from_lsn {
                    continue;
                }
                let event = decode_wal_record(&rec, 0);
                let envelope = encode_debezium(&event, ts_ms);
                envelopes.push(Frame::BulkString(envelope));
                next_lsn = rec.lsn + 1;
            }
            Ok(None) => break,
            Err(e) => {
                return Frame::Error(Bytes::from(format!("ERR CDC.READ tail error: {}", e)));
            }
        }
    }

    // ── response ───────────────────────────────────────────────────
    // First element is the cursor (next_lsn). Subsequent elements are
    // Debezium JSON envelopes in LSN order.
    let mut out = FrameVec::with_capacity(envelopes.len() + 1);
    out.push(Frame::Integer(next_lsn as i64));
    for env in envelopes {
        out.push(env);
    }
    Frame::Array(out)
}

fn parse_args(args: &[Frame]) -> Result<(String, u64, usize), String> {
    if args.len() < 2 || args.len() > 4 {
        return Err(format!(
            "wrong number of arguments ({}); expected `CDC.READ <wal_dir> <from_lsn> [LIMIT N]`",
            args.len()
        ));
    }
    let wal_dir = bulk_to_string(&args[0]).ok_or("wal_dir must be a string")?;
    let from_lsn = bulk_to_u64(&args[1]).ok_or("from_lsn must be a non-negative integer")?;

    let limit = if args.len() >= 4 {
        // Expect "LIMIT N"
        let kw = bulk_to_string(&args[2]).ok_or("expected LIMIT keyword")?;
        if !kw.eq_ignore_ascii_case("LIMIT") {
            return Err(format!("unknown option '{}'; expected LIMIT", kw));
        }
        let n = bulk_to_u64(&args[3]).ok_or("LIMIT N must be a positive integer")?;
        (n as usize).clamp(1, MAX_CDC_LIMIT)
    } else if args.len() == 3 {
        return Err("LIMIT requires a value: `CDC.READ ... LIMIT N`".into());
    } else {
        DEFAULT_CDC_LIMIT
    };

    Ok((wal_dir, from_lsn, limit))
}

fn bulk_to_string(f: &Frame) -> Option<String> {
    match f {
        Frame::BulkString(b) | Frame::SimpleString(b) => {
            std::str::from_utf8(b).ok().map(|s| s.to_owned())
        }
        _ => None,
    }
}

fn bulk_to_u64(f: &Frame) -> Option<u64> {
    let s = bulk_to_string(f)?;
    s.parse::<u64>().ok()
}

fn current_time_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::wal_v3::record::WalRecordType;
    use crate::persistence::wal_v3::segment::{DEFAULT_SEGMENT_SIZE, WalWriterV3};

    fn bulk(s: &str) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    fn write_test_wal(wal_dir: &std::path::Path, n: u64) {
        let mut writer = WalWriterV3::new(0, wal_dir, DEFAULT_SEGMENT_SIZE).unwrap();
        for _ in 0..n {
            writer.append(
                WalRecordType::Command,
                b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
            );
        }
        writer.flush_sync().unwrap();
    }

    /// C3 — happy path: drain 10 SET records as Debezium envelopes.
    /// Response is `[next_lsn, env1, env2, ...]` with envelopes in LSN order.
    #[test]
    fn test_cdc_read_drains_in_lsn_order() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        write_test_wal(&wal_dir, 10);

        let dir_arg = bulk(wal_dir.to_str().unwrap());
        let from_arg = bulk("1");
        let resp = cdc_read(&[dir_arg, from_arg]);

        match resp {
            Frame::Array(arr) => {
                // First frame is next_lsn (11 = max_lsn 10 + 1).
                match arr[0] {
                    Frame::Integer(n) => assert_eq!(n, 11),
                    _ => panic!("first frame must be Integer(next_lsn)"),
                }
                // The rest are envelopes — one per record.
                assert_eq!(arr.len() - 1, 10);
                for env in arr.iter().skip(1) {
                    let s = match env {
                        Frame::BulkString(b) => std::str::from_utf8(b).unwrap(),
                        _ => panic!("envelope must be BulkString"),
                    };
                    assert!(s.starts_with("{\"op\":\"u\""));
                    assert!(s.contains("\"record_type\":\"kv\""));
                }
            }
            other => panic!("expected Array, got {:?}", other),
        }
    }

    /// C3 — `from_lsn` filters records strictly below the target.
    #[test]
    fn test_cdc_read_respects_from_lsn() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        write_test_wal(&wal_dir, 10);

        let resp = cdc_read(&[bulk(wal_dir.to_str().unwrap()), bulk("6")]);
        match resp {
            Frame::Array(arr) => {
                // next_lsn = 11, plus envelopes for LSN 6..=10 (five of them).
                assert!(matches!(arr[0], Frame::Integer(11)));
                assert_eq!(arr.len() - 1, 5);
            }
            _ => panic!("expected Array"),
        }
    }

    /// C3 — empty WAL: returns `[from_lsn]` with no envelopes (stable
    /// no-new-data signal — consumer detects via len == 1).
    #[test]
    fn test_cdc_read_no_new_records_returns_cursor_only() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let resp = cdc_read(&[bulk(wal_dir.to_str().unwrap()), bulk("42")]);
        match resp {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 1);
                assert!(matches!(arr[0], Frame::Integer(42)));
            }
            _ => panic!("expected Array"),
        }
    }

    /// C3 — LIMIT caps the batch.
    #[test]
    fn test_cdc_read_limit_clamps_batch() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_dir = tmp.path().join("wal");
        write_test_wal(&wal_dir, 50);

        let resp = cdc_read(&[
            bulk(wal_dir.to_str().unwrap()),
            bulk("1"),
            bulk("LIMIT"),
            bulk("5"),
        ]);
        match resp {
            Frame::Array(arr) => {
                // next_lsn should be 6 (we read LSN 1..=5, so next is 6).
                assert!(matches!(arr[0], Frame::Integer(6)));
                assert_eq!(arr.len() - 1, 5);
            }
            _ => panic!("expected Array"),
        }
    }

    /// C3 — arity validation surfaces a friendly RESP error.
    #[test]
    fn test_cdc_read_argument_errors() {
        let too_few = cdc_read(&[bulk("only-one-arg")]);
        match too_few {
            Frame::Error(b) => assert!(std::str::from_utf8(&b).unwrap().contains("CDC.READ")),
            _ => panic!("expected Error"),
        }

        let bad_limit_kw = cdc_read(&[bulk("/tmp"), bulk("1"), bulk("BOGUS"), bulk("5")]);
        match bad_limit_kw {
            Frame::Error(b) => {
                let s = std::str::from_utf8(&b).unwrap();
                assert!(s.contains("LIMIT") || s.contains("unknown option"));
            }
            _ => panic!("expected Error"),
        }
    }
}
