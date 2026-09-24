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
//! - When the WAL has no records `>= from_lsn`, returns `[from_lsn]` with
//!   no envelopes -- a stable "no new data" signal the consumer can detect
//!   without parsing the timestamp.
//!
//! Cost (moon#1181). A poll used to rebuild a tail reader at segment 1 and
//! walk every retained record with ~9 syscalls each, on the shard thread —
//! proportional to the retained WAL, not to `LIMIT` or to how far behind the
//! consumer is. Now:
//! - the connection task hands the read to a small off-shard pool
//!   ([`cdc_read_async`], `cdc::read_pool`) and awaits it;
//! - the start position comes from the position hint the previous poll left
//!   for exactly this `(wal_dir, from_lsn)` (a sequential consumer), else
//!   from the segment headers' first-LSN field (one 64-byte read per
//!   segment), so at most one segment's records are skipped;
//! - skipped records are validated without copying their payload, and a
//!   call examines at most [`CDC_SKIP_BUDGET`] of them (the rest resumes on
//!   the next poll from a hint);
//! - the reader keeps each segment open and reads it in large `pread`
//!   chunks.
//!
//! Envelopes and cursors are identical to the full scan for a well-formed
//! WAL: records are yielded in the same order with the same `lsn >=
//! from_lsn` filter. One difference: a corrupt record in a segment wholly
//! BELOW `from_lsn` used to stall every later poll at that record; it is now
//! skipped with its segment (a corrupt record at or after the cursor still
//! stops the read exactly as before).

use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::cdc::{decode_wal_record, encode_debezium};
use crate::persistence::wal_v3::segment::{WAL_V3_HEADER_SIZE, WAL_V3_MAGIC, WAL_V3_VERSION};
use crate::persistence::wal_v3::tail::SkipOutcome;
use crate::persistence::wal_v3::{TailCursor, WalSegment, WalTailReader};
use crate::protocol::{Frame, FrameVec};

/// Default events per poll if LIMIT is omitted. Sized to fit comfortably in
/// a 1 MB RESP response while still amortizing the per-poll cost.
const DEFAULT_CDC_LIMIT: usize = 256;

/// Hard ceiling -- prevents a runaway consumer from pinning a poll-thread.
const MAX_CDC_LIMIT: usize = 10_000;

/// Records one poll may skip on the way to `from_lsn` (moon#1181). A
/// default 16 MiB segment holds fewer, so with the header seek this only
/// binds for oversized segments; when it does, the poll answers "no new
/// data" and leaves a hint the next poll resumes from.
pub const CDC_SKIP_BUDGET: usize = 1 << 20;

/// A parsed `CDC.READ` request.
#[derive(Debug, Clone)]
pub struct CdcReadRequest {
    wal_dir: PathBuf,
    from_lsn: u64,
    limit: usize,
}

/// Handle `CDC.READ <wal_dir> <from_lsn> [LIMIT N]` synchronously, on the
/// caller's thread (tests; the server uses [`cdc_read_async`]).
pub fn cdc_read(args: &[Frame]) -> Frame {
    match parse_request(args) {
        Ok(req) => execute(&req, current_time_ms()),
        Err(e) => e,
    }
}

/// Handle `CDC.READ` OFF the shard thread (moon#1181): parse here, read the
/// WAL on the CDC read pool, await the reply. Falls back to an inline read
/// if the pool has no worker.
pub async fn cdc_read_async(args: &[Frame]) -> Frame {
    let req = match parse_request(args) {
        Ok(req) => req,
        Err(e) => return e,
    };
    let ts_ms = current_time_ms();
    let (tx, rx) = flume::bounded::<Frame>(1);
    let job = move || {
        let _ = tx.send(execute(&req, ts_ms));
    };
    if let Err(job) = crate::cdc::read_pool::submit(job) {
        job();
    }
    rx.recv_async().await.unwrap_or_else(|_| {
        Frame::Error(Bytes::from_static(
            b"ERR CDC.READ reader exited without a reply",
        ))
    })
}

/// Parse the arguments; `Err` is the error reply.
pub fn parse_request(args: &[Frame]) -> Result<CdcReadRequest, Frame> {
    match parse_args(args) {
        Ok((wal_dir, from_lsn, limit)) => Ok(CdcReadRequest {
            wal_dir: PathBuf::from(wal_dir),
            from_lsn,
            limit,
        }),
        Err(e) => Err(Frame::Error(Bytes::from(format!("ERR CDC.READ: {}", e)))),
    }
}

/// Drain a batch of envelopes for `req` (the blocking part).
pub fn execute(req: &CdcReadRequest, ts_ms: i64) -> Frame {
    let wal_dir = req.wal_dir.as_path();
    let from_lsn = req.from_lsn;
    let limit = req.limit;

    // ── position ───────────────────────────────────────────────────
    let start = locate_start(wal_dir, from_lsn);
    let mut tail = WalTailReader::new(wal_dir, start);
    match tail.skip_below(from_lsn, CDC_SKIP_BUDGET) {
        Ok(SkipOutcome::Reached) => {}
        Ok(SkipOutcome::BudgetExhausted) => {
            // Every record before the cursor is below `from_lsn`: the next
            // poll resumes here instead of skipping them again.
            remember(wal_dir, from_lsn, tail.cursor());
            return Frame::Array(FrameVec::from_elem(Frame::Integer(from_lsn as i64)));
        }
        Err(e) => {
            return Frame::Error(Bytes::from(format!("ERR CDC.READ tail error: {}", e)));
        }
    }

    // ── drain ──────────────────────────────────────────────────────
    let mut envelopes: FrameVec = FrameVec::new();
    let mut next_lsn = from_lsn;
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
    // The consumer's next poll asks for `next_lsn`: leave it the position.
    remember(wal_dir, next_lsn, tail.cursor());

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

// ── start position ─────────────────────────────────────────────────────

/// A position a previous poll left for `(wal_dir, lsn)`: every record before
/// it in the chain has `lsn < lsn`. `base_lsn` is the segment header's
/// first-LSN at the time, which revalidates the hint against a recreated
/// directory.
#[derive(Debug, Clone)]
struct PositionHint {
    wal_dir: PathBuf,
    lsn: u64,
    seq: u64,
    offset: u64,
    base_lsn: u64,
}

/// Hints for the most recent consumers (a handful at most in practice).
const CDC_HINT_SLOTS: usize = 64;

static HINTS: parking_lot::Mutex<Vec<PositionHint>> = parking_lot::const_mutex(Vec::new());

/// Header of segment `seq`: its first-LSN when the file is a WAL v3 segment.
fn segment_base_lsn(wal_dir: &Path, seq: u64) -> Option<u64> {
    let file = std::fs::File::open(WalSegment::segment_path(wal_dir, seq)).ok()?;
    let mut hdr = [0u8; WAL_V3_HEADER_SIZE];
    let n = crate::persistence::wal_v3::tail::read_at(&file, 0, &mut hdr).ok()?;
    if n < WAL_V3_HEADER_SIZE || &hdr[..6] != WAL_V3_MAGIC || hdr[6] != WAL_V3_VERSION {
        return None;
    }
    Some(u64::from_le_bytes(hdr[28..36].try_into().ok()?))
}

fn remember(wal_dir: &Path, lsn: u64, cursor: TailCursor) {
    let Some(base_lsn) = segment_base_lsn(wal_dir, cursor.segment_seq) else {
        return;
    };
    let hint = PositionHint {
        wal_dir: wal_dir.to_path_buf(),
        lsn,
        seq: cursor.segment_seq,
        offset: cursor.byte_offset,
        base_lsn,
    };
    let mut hints = HINTS.lock();
    hints.retain(|h| !(h.lsn == lsn && h.wal_dir == hint.wal_dir));
    if hints.len() >= CDC_HINT_SLOTS {
        hints.remove(0);
    }
    hints.push(hint);
}

/// A remembered position for `(wal_dir, from_lsn)` that still describes the
/// same file: header first-LSN unchanged, offset inside the file, and the
/// record there (if any) valid with an LSN not past `from_lsn`.
fn hinted_start(wal_dir: &Path, from_lsn: u64) -> Option<TailCursor> {
    let hint = HINTS
        .lock()
        .iter()
        .find(|h| h.lsn == from_lsn && h.wal_dir == wal_dir)
        .cloned()?;
    if segment_base_lsn(wal_dir, hint.seq)? != hint.base_lsn {
        return None;
    }
    let file = std::fs::File::open(WalSegment::segment_path(wal_dir, hint.seq)).ok()?;
    let len = file.metadata().ok()?.len();
    if hint.offset < WAL_V3_HEADER_SIZE as u64 || hint.offset > len {
        return None;
    }
    if hint.offset < len {
        let mut head = [0u8; 4];
        let n = crate::persistence::wal_v3::tail::read_at(&file, hint.offset, &mut head).ok()?;
        if n == 4 {
            let rlen = u32::from_le_bytes(head) as u64;
            if rlen > 0 && hint.offset + rlen <= len {
                let mut rec = vec![0u8; rlen as usize];
                let n =
                    crate::persistence::wal_v3::tail::read_at(&file, hint.offset, &mut rec).ok()?;
                if n as u64 == rlen {
                    let lsn = crate::persistence::wal_v3::record::peek_wal_v3_record_lsn(&rec)?;
                    if lsn > from_lsn {
                        return None;
                    }
                }
            }
        }
    }
    Some(TailCursor {
        segment_seq: hint.seq,
        byte_offset: hint.offset,
        last_lsn: 0,
    })
}

/// Where a scan for `from_lsn` starts: a validated hint, else the last
/// segment of the chain whose header first-LSN is `<= from_lsn`, else (no
/// readable, monotonic headers) the origin — the full scan.
///
/// The chain is the one the full scan walks: the first segment at or after
/// sequence 1, then each next sequence while it exists.
fn locate_start(wal_dir: &Path, from_lsn: u64) -> TailCursor {
    if let Some(cursor) = hinted_start(wal_dir, from_lsn) {
        return cursor;
    }
    let Ok(entries) = std::fs::read_dir(wal_dir) else {
        return TailCursor::start();
    };
    let mut seqs: Vec<u64> = entries
        .flatten()
        .filter_map(|e| {
            e.file_name()
                .to_str()?
                .strip_suffix(".wal")?
                .parse::<u64>()
                .ok()
        })
        .filter(|&seq| seq >= 1)
        .collect();
    seqs.sort_unstable();
    let Some(&first) = seqs.first() else {
        return TailCursor::start();
    };
    let mut best = first;
    let mut prev_base = 0u64;
    let mut expected = first;
    for &seq in &seqs {
        if seq != expected {
            break; // the full scan stops at a gap
        }
        expected += 1;
        let Some(base) = segment_base_lsn(wal_dir, seq) else {
            return TailCursor::start();
        };
        if base < prev_base {
            // Non-monotonic first-LSNs (a pre-P0 WAL): only the full scan
            // reproduces the record order.
            return TailCursor::start();
        }
        prev_base = base;
        if base <= from_lsn {
            best = seq;
        }
    }
    TailCursor::at_segment(best)
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
    use crate::persistence::wal_v3::segment::{DEFAULT_SEGMENT_SIZE, WalBounds, WalWriterV3};

    fn bulk(s: &str) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    fn write_test_wal(wal_dir: &std::path::Path, n: u64) {
        let mut writer =
            WalWriterV3::new(0, wal_dir, DEFAULT_SEGMENT_SIZE, WalBounds::DEFAULT).unwrap();
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

#[cfg(test)]
mod seek_tests;
