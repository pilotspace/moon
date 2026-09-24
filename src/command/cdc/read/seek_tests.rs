//! moon#1181 — `CDC.READ` returns exactly what the full scan returned, at a
//! cost bounded by one segment (or `LIMIT` for a sequential consumer), not by
//! the retained WAL.

use super::*;
use crate::persistence::wal_v3::record::{WalRecord, WalRecordType, read_wal_v3_record};
use crate::persistence::wal_v3::segment::{WalBounds, WalWriterV3};

/// HEAD `935c555`'s CDC.READ, verbatim: a fresh reader at segment 1 that
/// stats the segment and opens/seeks/reads it per record. The oracle.
mod head {
    use super::*;
    use std::fs;

    fn read_at(path: &Path, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = fs::File::open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut total = 0;
        while total < buf.len() {
            match file.read(&mut buf[total..])? {
                0 => break,
                n => total += n,
            }
        }
        Ok(total)
    }

    fn find_segment_after(dir: &Path, after: u64) -> Option<u64> {
        let mut best: Option<u64> = None;
        for e in fs::read_dir(dir).ok()?.flatten() {
            let name = e.file_name();
            if let Some(seq) = name
                .to_string_lossy()
                .strip_suffix(".wal")
                .and_then(|s| s.parse::<u64>().ok())
                && seq > after
                && best.is_none_or(|b| seq < b)
            {
                best = Some(seq);
            }
        }
        best
    }

    struct Reader {
        dir: PathBuf,
        seq: u64,
        off: u64,
    }

    impl Reader {
        fn read_next(&mut self) -> std::io::Result<Option<WalRecord>> {
            loop {
                let path = WalSegment::segment_path(&self.dir, self.seq);
                let len = match fs::metadata(&path) {
                    Ok(m) => m.len(),
                    Err(_) => {
                        if let Some(n) = find_segment_after(&self.dir, self.seq) {
                            self.seq = n;
                            self.off = WAL_V3_HEADER_SIZE as u64;
                            continue;
                        }
                        return Ok(None);
                    }
                };
                if self.off + 4 > len {
                    if WalSegment::segment_path(&self.dir, self.seq + 1).exists() {
                        self.seq += 1;
                        self.off = WAL_V3_HEADER_SIZE as u64;
                        continue;
                    }
                    return Ok(None);
                }
                let mut lb = [0u8; 4];
                if read_at(&path, self.off, &mut lb)? < 4 {
                    return Ok(None);
                }
                let rlen = u32::from_le_bytes(lb) as u64;
                if rlen == 0 || self.off + rlen > len {
                    return Ok(None);
                }
                let mut buf = vec![0u8; rlen as usize];
                if (read_at(&path, self.off, &mut buf)? as u64) < rlen {
                    return Ok(None);
                }
                let Some(rec) = read_wal_v3_record(&buf) else {
                    return Ok(None);
                };
                self.off += rlen;
                return Ok(Some(rec));
            }
        }
    }

    pub(super) fn cdc_read(dir: &Path, from_lsn: u64, limit: usize, ts_ms: i64) -> Frame {
        let mut tail = Reader {
            dir: dir.to_path_buf(),
            seq: 1,
            off: WAL_V3_HEADER_SIZE as u64,
        };
        let mut envelopes = FrameVec::new();
        let mut next_lsn = from_lsn;
        while envelopes.len() < limit {
            match tail.read_next() {
                Ok(Some(rec)) => {
                    if rec.lsn < from_lsn {
                        continue;
                    }
                    let event = decode_wal_record(&rec, 0);
                    envelopes.push(Frame::BulkString(encode_debezium(&event, ts_ms)));
                    next_lsn = rec.lsn + 1;
                }
                Ok(None) => break,
                Err(e) => {
                    return Frame::Error(Bytes::from(format!("ERR CDC.READ tail error: {}", e)));
                }
            }
        }
        let mut out = FrameVec::with_capacity(envelopes.len() + 1);
        out.push(Frame::Integer(next_lsn as i64));
        for env in envelopes {
            out.push(env);
        }
        Frame::Array(out)
    }
}

const TS: i64 = 1_700_000_000_000;

fn req(dir: &Path, from_lsn: u64, limit: usize) -> CdcReadRequest {
    CdcReadRequest {
        wal_dir: dir.to_path_buf(),
        from_lsn,
        limit,
    }
}

/// A multi-segment WAL of distinct SET / DEL / HSET records.
fn write_wal(dir: &Path, segment_size: u64, n: u64) -> u64 {
    let mut w = WalWriterV3::new(0, dir, segment_size, WalBounds::DEFAULT).unwrap();
    let mut last = 0;
    for i in 0..n {
        let payload = match i % 3 {
            0 => format!(
                "*3\r\n$3\r\nSET\r\n$6\r\nk{:05}\r\n$2\r\nv{}\r\n",
                i,
                i % 10
            ),
            1 => format!("*2\r\n$3\r\nDEL\r\n$6\r\nk{:05}\r\n", i),
            _ => format!(
                "*4\r\n$4\r\nHSET\r\n$6\r\nh{:05}\r\n$1\r\nf\r\n$3\r\nx{:02}\r\n",
                i,
                i % 100
            ),
        };
        last = w.append(WalRecordType::Command, payload.as_bytes());
        if i % 7 == 0 {
            w.flush_sync().unwrap();
        }
    }
    w.flush_sync().unwrap();
    last
}

/// Every `(from_lsn, LIMIT)` returns exactly the full scan's response —
/// cursors and envelopes, byte for byte — including segment boundaries,
/// the first and last record, and cursors past the end.
#[test]
fn responses_are_identical_to_the_full_scan() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("wal");
    let last = write_wal(&dir, 2048, 900);
    assert!(
        std::fs::read_dir(&dir).unwrap().count() > 10,
        "fixture must span many segments"
    );
    for from in [
        0,
        1,
        2,
        17,
        63,
        64,
        65,
        300,
        449,
        450,
        451,
        last - 1,
        last,
        last + 1,
        last + 50,
    ] {
        for limit in [1usize, 5, 256, 10_000] {
            let got = execute(&req(&dir, from, limit), TS);
            let want = head::cdc_read(&dir, from, limit, TS);
            assert_eq!(got, want, "from={from} limit={limit}");
        }
    }
}

/// A consumer polling with each reply's cursor sees the full scan's stream,
/// also while the WAL keeps growing and rotating under it.
#[test]
fn sequential_polls_match_the_full_scan_on_a_live_wal() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("wal");
    let mut w = WalWriterV3::new(0, &dir, 1024, WalBounds::DEFAULT).unwrap();
    let mut cursor = 1u64;
    for round in 0..40u64 {
        for i in 0..(round % 7) * 5 {
            let p = format!(
                "*2\r\n$4\r\nINCR\r\n$4\r\nc{:03}\r\n",
                (round * 31 + i) % 1000
            );
            w.append(WalRecordType::Command, p.as_bytes());
        }
        w.flush_sync().unwrap();
        for _ in 0..3 {
            let got = execute(&req(&dir, cursor, 9), TS);
            let want = head::cdc_read(&dir, cursor, 9, TS);
            assert_eq!(got, want, "round {round} cursor {cursor}");
            if let Frame::Array(a) = &got
                && let Frame::Integer(n) = a[0]
            {
                cursor = n as u64;
            }
        }
    }
    assert!(cursor > 300, "consumer must have advanced, at {cursor}");
}

/// moon#1221 review R1: a WAL written by the unreleased moon#1188 build —
/// whose segment headers overstate `base_lsn` (the LSN after the records
/// appended while a rotation's fsync was in flight) — still reads exactly
/// as the full scan does: the header seek only ever starts early.
#[test]
fn an_overstated_segment_header_still_reads_like_the_full_scan() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("wal");
    let last = write_wal(&dir, 2048, 900);
    let header_base = |seq: u64| -> Option<u64> {
        let data = std::fs::read(WalSegment::segment_path(&dir, seq)).ok()?;
        Some(u64::from_le_bytes(data[28..36].try_into().ok()?))
    };
    let firsts: Vec<u64> = (1..).map_while(header_base).collect();
    assert!(firsts.len() > 10, "fixture must span many segments");
    // The PR head's header for segment N lay in [first_N, first_{N+1}].
    for (i, pair) in firsts.windows(2).enumerate().skip(1) {
        let path = WalSegment::segment_path(&dir, i as u64 + 1);
        let mut data = std::fs::read(&path).unwrap();
        data[28..36].copy_from_slice(&((pair[0] + pair[1]) / 2).to_le_bytes());
        std::fs::write(&path, &data).unwrap();
    }
    let froms = firsts
        .iter()
        .flat_map(|&f| [f.saturating_sub(1), f, f + 1, f + 3])
        .chain([last, last + 1]);
    for from in froms {
        for limit in [1usize, 7, 10_000] {
            let got = execute(&req(&dir, from, limit), TS);
            let want = head::cdc_read(&dir, from, limit, TS);
            assert_eq!(got, want, "from={from} limit={limit}");
        }
    }
}

/// A stale hint (the directory was wiped and rebuilt with other contents)
/// is rejected, never trusted.
#[test]
fn a_hint_for_a_recreated_directory_is_not_trusted() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("wal");
    let last = write_wal(&dir, 4096, 300);
    let _ = execute(&req(&dir, last + 1, 10), TS); // leaves a hint at the end
    std::fs::remove_dir_all(&dir).unwrap();
    let last2 = write_wal(&dir, 4096, 600);
    assert!(last2 > last);
    let got = execute(&req(&dir, last + 1, 10_000), TS);
    let want = head::cdc_read(&dir, last + 1, 10_000, TS);
    assert_eq!(got, want);
}

/// A corrupt record at or after the cursor still stops the read where the
/// full scan stopped.
#[test]
fn a_corrupt_record_after_the_cursor_stops_the_read_as_before() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("wal");
    let _ = write_wal(&dir, 1 << 20, 50);
    let path = WalSegment::segment_path(&dir, 1);
    let mut bytes = std::fs::read(&path).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    std::fs::write(&path, &bytes).unwrap();
    for from in [1, 10, 20] {
        let got = execute(&req(&dir, from, 100), TS);
        assert_eq!(got, head::cdc_read(&dir, from, 100, TS), "from={from}");
    }
}

/// Wall time of one poll.
fn poll_wall(dir: &Path, from: u64, limit: usize) -> std::time::Duration {
    let t = std::time::Instant::now();
    let _ = std::hint::black_box(execute(&req(dir, from, limit), TS));
    t.elapsed()
}

/// The cost of a poll does not grow with the retained WAL: a caught-up
/// consumer and a fresh consumer at the tail pay ~the same on a 10x longer
/// history. At HEAD both re-read every retained record (~9 syscalls each).
#[test]
fn poll_cost_is_independent_of_retained_history() {
    let tmp = tempfile::tempdir().unwrap();
    let short = tmp.path().join("short");
    let long = tmp.path().join("long");
    let seg = 64 * 1024;
    let last_short = write_wal(&short, seg, 6_000);
    let last_long = write_wal(&long, seg, 60_000);

    // Fresh consumer at the tail (no hint): header seek + at most one segment.
    // A poll leaves its hint at the NEXT cursor (`last + 1`), so repeating
    // the poll at `last` stays fresh. The minimum of several interleaved
    // runs, like the caught-up check below: one sample of each is a coin
    // flip on a shared, loaded box (moon#1221 review R9).
    let (mut fresh_short, mut fresh_long) = (std::time::Duration::MAX, std::time::Duration::MAX);
    for _ in 0..7 {
        fresh_short = fresh_short.min(poll_wall(&short, last_short, 100));
        fresh_long = fresh_long.min(poll_wall(&long, last_long, 100));
    }
    // Caught-up consumer (hint left by the previous poll).
    let _ = execute(&req(&long, last_long + 1, 100), TS);
    let caught_up_long = (0..20)
        .map(|_| poll_wall(&long, last_long + 1, 100))
        .min()
        .unwrap();

    // Loose absolute bounds (debug build, shared box): the full scan of
    // 60k records costs ~0.5M syscalls — hundreds of ms.
    assert!(
        fresh_long < fresh_short * 4 + std::time::Duration::from_millis(40),
        "fresh poll grew with history: {fresh_short:?} (6k) vs {fresh_long:?} (60k)"
    );
    assert!(
        caught_up_long < std::time::Duration::from_millis(20),
        "caught-up poll on 60k records took {caught_up_long:?}"
    );
    let head_long = {
        let t = std::time::Instant::now();
        let _ = std::hint::black_box(head::cdc_read(&long, last_long + 1, 100, TS));
        t.elapsed()
    };
    eprintln!(
        "moon#1181 poll cost: fresh 6k={fresh_short:?} fresh 60k={fresh_long:?} \
         caught-up 60k={caught_up_long:?} | HEAD full scan 60k={head_long:?}"
    );
}

/// The skip budget bounds a poll's work and the next poll resumes where it
/// stopped: nothing is lost, the stream is the full scan's.
#[test]
fn skip_budget_resumes_across_polls() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("wal");
    // One huge segment: the header seek cannot help, only the budget.
    let _ = write_wal(&dir, 64 << 20, 3000);
    let from = 2500u64;
    let mut tail = WalTailReader::new(&dir, TailCursor::start());
    // Emulate a small budget directly on the reader.
    let mut polls = 0;
    loop {
        polls += 1;
        match tail.skip_below(from, 700).unwrap() {
            SkipOutcome::BudgetExhausted => continue,
            SkipOutcome::Reached => break,
        }
    }
    assert_eq!(polls, 4, "2499 skipped records at 700 per poll");
    let rec = tail.read_next().unwrap().unwrap();
    assert_eq!(rec.lsn, from);
}
