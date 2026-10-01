//! The forward scan across a foreign segment (R2 review of moon#1283:
//! [`super::clock`]'s positional rule).
//!
//! A replay reads a file forwards and, at a clean-close marker
//! (`MOON.TS <ms> CLOSE`), cannot know yet which stamp ends the segment
//! another binary may have appended after it. [`scan_after_close`] reads
//! ahead from the marker's end — a second, independent reader over the same
//! file, parsing records exactly as the replay's reader does — and stops at
//! the first well-formed stamp (`MOON.TS <ms>` or another `CLOSE`). It reads
//! only the segment: nothing when this binary reopened the file (its session
//! stamp is the very next record), the older binary's records otherwise. It
//! is a parse, not a byte search, so bytes inside a value never pass for a
//! stamp.
//!
//! A torn or corrupt record ends the scan as the end of the file does: the
//! replay stops at the same place (a truncated tail, or a refused file).

use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use super::clock::LogFormat;
use super::pseudo::{self, Pseudo};
use crate::protocol::Frame;

/// What follows a clean-close marker, up to the next stamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentScan {
    /// The value of the stamp that ends the segment (`None`: the segment runs
    /// to the end of the file, or the file could not be read on).
    pub next_stamp_ms: Option<u64>,
    /// How many records lie between the marker and that stamp (malformed
    /// stamps, which a replay skips, excluded).
    pub records: u64,
}

/// The stamp `frame` is, as the replay classifies it: `Some(Some(ms))` for a
/// well-formed `MOON.TS` or `CLOSE`, `Some(None)` for a malformed one (skipped
/// by the replay, so neither a stamp nor a record), `None` for any other
/// record.
fn stamp_of(frame: &Frame) -> Option<Option<u64>> {
    let Frame::Array(arr) = frame else {
        return None;
    };
    let (name, args) = arr.split_first()?;
    let name = match name {
        Frame::BulkString(s) | Frame::SimpleString(s) => s.as_ref(),
        _ => return None,
    };
    match pseudo::classify(name, args)? {
        Pseudo::Ts(ms) | Pseudo::Close(ms) => Some(Some(ms)),
        Pseudo::MalformedTs => Some(None),
        Pseudo::ColdPlane | Pseudo::Txn(_) | Pseudo::MalformedTxn => None,
    }
}

/// Scan `path` from byte `offset` (just past a clean-close marker) to the
/// next stamp; see the module doc. Never fails: an unreadable file reads as
/// a segment of unknown length that runs to its end (`records: 1`), the
/// conservative answer.
#[must_use]
pub fn scan_after_close(path: &Path, offset: u64, format: LogFormat) -> SegmentScan {
    let unknown = SegmentScan {
        next_stamp_ms: None,
        records: 1,
    };
    let Ok(mut file) = std::fs::File::open(path) else {
        return unknown;
    };
    let Ok(len) = file.metadata().map(|m| m.len()) else {
        return unknown;
    };
    if file.seek(SeekFrom::Start(offset)).is_err() {
        return unknown;
    }
    match format {
        LogFormat::Resp => scan_resp(file, offset),
        LogFormat::Framed => scan_framed(std::io::BufReader::new(file), len.saturating_sub(offset)),
    }
}

fn scan_resp(src: impl Read, offset: u64) -> SegmentScan {
    use super::chunks::{ReplayChunks, ReplayNext};
    let mut chunks = ReplayChunks::new(src, offset);
    let mut records = 0u64;
    loop {
        match chunks.next_frame() {
            Ok(ReplayNext::Frame(frame)) => match stamp_of(&frame) {
                Some(Some(ms)) => {
                    return SegmentScan {
                        next_stamp_ms: Some(ms),
                        records,
                    };
                }
                Some(None) => {}
                None => records += 1,
            },
            Ok(ReplayNext::End | ReplayNext::Truncated { .. } | ReplayNext::Corrupt { .. })
            | Err(_) => {
                return SegmentScan {
                    next_stamp_ms: None,
                    records,
                };
            }
        }
    }
}

/// The per-shard framed layout (`[u64 lsn LE][u32 len LE][RESP]`), as
/// `aof_manifest::shard_replay::replay_incr_framed` reads it. `remaining` is
/// how many bytes the file holds past the start: a declared length beyond it
/// is a torn tail, never an allocation of that size.
fn scan_framed(mut src: impl Read, mut remaining: u64) -> SegmentScan {
    use crate::protocol::{ParseConfig, parse};
    const HEADER_LEN: u64 = 12;
    let config = ParseConfig::default();
    let mut records = 0u64;
    let mut payload = bytes::BytesMut::new();
    let end = |records| SegmentScan {
        next_stamp_ms: None,
        records,
    };
    loop {
        let mut header = [0u8; HEADER_LEN as usize];
        if remaining < HEADER_LEN || src.read_exact(&mut header).is_err() {
            return end(records);
        }
        remaining -= HEADER_LEN;
        let (lsn, len) = header.split_at(8);
        let mut lsn8 = [0u8; 8];
        lsn8.copy_from_slice(lsn);
        let mut len4 = [0u8; 4];
        len4.copy_from_slice(len);
        let (lsn, len) = (
            u64::from_le_bytes(lsn8),
            u64::from(u32::from_le_bytes(len4)),
        );
        if len > remaining {
            return end(records);
        }
        remaining -= len;
        let Ok(len) = usize::try_from(len) else {
            return end(records);
        };
        payload.clear();
        payload.resize(len, 0);
        if src.read_exact(&mut payload).is_err() {
            return end(records);
        }
        if len == 0 {
            continue; // a barrier: the replay skips it
        }
        if lsn & crate::persistence::aof::ORDERED_LSN_FLAG != 0 {
            records += 1; // merge-replayed data, never a stamp
            continue;
        }
        match parse::parse(&mut payload, &config) {
            Ok(Some(frame)) => match stamp_of(&frame) {
                Some(Some(ms)) => {
                    return SegmentScan {
                        next_stamp_ms: Some(ms),
                        records,
                    };
                }
                Some(None) => {}
                None => records += 1,
            },
            // The replay refuses the file here.
            _ => return end(records),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::replay::pseudo::TsRecord;

    fn resp(parts: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", parts.len()).into_bytes();
        for p in parts {
            out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            out.extend_from_slice(p);
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    fn framed(records: &[Vec<u8>]) -> Vec<u8> {
        let mut out = Vec::new();
        for r in records {
            out.extend_from_slice(&0u64.to_le_bytes());
            out.extend_from_slice(&(r.len() as u32).to_le_bytes());
            out.extend_from_slice(r);
        }
        out
    }

    fn file_with(bytes: &[u8]) -> tempfile::NamedTempFile {
        let f = tempfile::NamedTempFile::new().expect("tempfile");
        std::fs::write(f.path(), bytes).expect("write");
        f
    }

    /// `records` after a `CLOSE`, in both layouts: the scan from the
    /// marker's end.
    fn scan(records: &[Vec<u8>]) -> [SegmentScan; 2] {
        let close = TsRecord::close(100).as_bytes().to_vec();
        let mut all = vec![resp(&[b"SET", b"a", b"1"]), close];
        all.extend(records.iter().cloned());
        let flat: Vec<u8> = all.concat();
        let from = (all[0].len() + all[1].len()) as u64;
        let f = file_with(&flat);
        let resp_scan = scan_after_close(f.path(), from, LogFormat::Resp);
        let fr = framed(&all);
        let from = (all[0].len() + all[1].len() + 24) as u64;
        let f = file_with(&fr);
        let framed_scan = scan_after_close(f.path(), from, LogFormat::Framed);
        [resp_scan, framed_scan]
    }

    #[test]
    fn a_foreign_segment_ends_at_the_next_stamp() {
        let got = scan(&[
            resp(&[b"SET", b"k", b"10", b"PX", b"600"]),
            resp(&[b"INCR", b"k"]),
            TsRecord::new(5_000).as_bytes().to_vec(),
            resp(&[b"SET", b"later", b"v"]),
            TsRecord::new(6_000).as_bytes().to_vec(),
        ]);
        for g in got {
            assert_eq!(
                g,
                SegmentScan {
                    next_stamp_ms: Some(5_000),
                    records: 2
                }
            );
        }
    }

    #[test]
    fn a_stamp_right_after_the_close_is_an_empty_segment() {
        for next in [TsRecord::new(7), TsRecord::close(8)] {
            let got = scan(&[next.as_bytes().to_vec(), resp(&[b"SET", b"k", b"v"])]);
            for g in got {
                assert_eq!(g.records, 0);
                assert!(g.next_stamp_ms.is_some());
            }
        }
    }

    #[test]
    fn a_segment_at_the_end_of_the_file_has_no_next_stamp() {
        for g in scan(&[resp(&[b"SET", b"k", b"v"]), resp(&[b"DEL", b"k"])]) {
            assert_eq!(
                g,
                SegmentScan {
                    next_stamp_ms: None,
                    records: 2
                }
            );
        }
        for g in scan(&[]) {
            assert_eq!(
                g,
                SegmentScan {
                    next_stamp_ms: None,
                    records: 0
                }
            );
        }
    }

    /// A malformed stamp is skipped (as the replay skips it), a value that
    /// SPELLS a stamp is data, and a torn tail ends the scan.
    #[test]
    fn only_a_parsed_well_formed_stamp_ends_the_segment() {
        let spelled = TsRecord::new(9).as_bytes().to_vec();
        let got = scan(&[
            resp(&[b"MOON.TS", b"0"]),
            resp(&[b"MOON.TS", b"1", b"OPEN"]),
            resp(&[b"SET", b"k", &spelled]),
            resp(&[b"MOON.COLDCUT", b"3"]),
            b"*2\r\n$7\r\nMOON.TS\r\n$4\r\n12".to_vec(),
        ]);
        for g in got {
            assert_eq!(
                g,
                SegmentScan {
                    next_stamp_ms: None,
                    records: 2
                }
            );
        }
    }

    #[test]
    fn an_absurd_framed_length_is_a_torn_tail_not_an_allocation() {
        let mut bytes = TsRecord::close(1).as_bytes().to_vec();
        let mut log = framed(&[bytes.clone()]);
        let from = log.len() as u64;
        log.extend_from_slice(&0u64.to_le_bytes());
        log.extend_from_slice(&u32::MAX.to_le_bytes());
        bytes.truncate(3);
        log.extend_from_slice(&bytes);
        let f = file_with(&log);
        assert_eq!(
            scan_after_close(f.path(), from, LogFormat::Framed),
            SegmentScan {
                next_stamp_ms: None,
                records: 0
            }
        );
    }

    #[test]
    fn an_unreadable_file_is_a_segment_to_its_end() {
        assert_eq!(
            scan_after_close(Path::new("/nonexistent/moon/aof"), 0, LogFormat::Resp),
            SegmentScan {
                next_stamp_ms: None,
                records: 1
            }
        );
    }
}
