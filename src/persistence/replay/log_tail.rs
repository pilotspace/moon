//! The last `MOON.TS` of a log file, found by reading it backwards from the
//! end (R1 review of moon#1283, finding 1: [`super::clock`]'s foreign-tail
//! rule).
//!
//! A replay reads a file forwards and cannot know, at a stamp, whether a
//! later stamp follows. The foreign-tail rule needs exactly that: the value of
//! the file's LAST well-formed stamp. This scan finds it from the end, so it
//! reads only what follows that stamp — the last clock tick of this binary's
//! own writes (a few KiB), or the whole tail an older binary appended after
//! it. It runs at most once per replayed file, and only when the file has a
//! stamp at all and its mtime is later than a stamp it holds.
//!
//! It is a byte search, not a parse: bytes inside a value that spell a
//! complete `MOON.TS` record would be taken for one. That can only move the
//! rule's cut point to a record that is not a stamp, where no replayed stamp
//! matches it, so the rule stays off and the replay judges as it did before
//! the rule existed (by the last stamp read).

use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use super::pseudo::{MAX_TS_MS, TS_RECORD_MAX_LEN};

/// Every `MOON.TS` record starts with this: a 2-element array whose first
/// element is the 7-byte name. The writer emits the name upper-case only.
const NEEDLE: &[u8] = b"*2\r\n$7\r\nMOON.TS\r\n$";

/// Bytes read per backward step.
const CHUNK: usize = 64 * 1024;

/// The value of the last well-formed `MOON.TS` record in `path`, or `None`
/// when it holds none (or cannot be read). A malformed or torn candidate is
/// skipped the way a replay skips it, so the answer is the last stamp a replay
/// actually applies.
pub fn last_ts_in_file(path: &Path) -> Option<u64> {
    let mut file = std::fs::File::open(path).ok()?;
    let len = file.metadata().ok()?.len();
    let finder = memchr::memmem::FinderRev::new(NEEDLE);
    // `window` = the chunk at [start, start + chunk_len) followed by up to
    // TS_RECORD_MAX_LEN - 1 bytes of the chunk after it, so a record that
    // straddles the boundary is still whole in the window that holds its
    // first byte.
    let mut window: Vec<u8> = Vec::with_capacity(CHUNK + TS_RECORD_MAX_LEN);
    let mut carry: Vec<u8> = Vec::with_capacity(TS_RECORD_MAX_LEN);
    let mut end = len;
    while end > 0 {
        let start = end.saturating_sub(CHUNK as u64);
        let chunk_len = usize::try_from(end - start).ok()?;
        window.clear();
        window.resize(chunk_len, 0);
        file.seek(SeekFrom::Start(start)).ok()?;
        file.read_exact(&mut window).ok()?;
        window.extend_from_slice(&carry);
        // Candidates in this chunk, last first; one that starts in the carry
        // was already tried with the chunk after this one.
        for at in finder.rfind_iter(&window) {
            if at >= chunk_len {
                continue;
            }
            if let Some(ms) = parse_ts_value(&window[at + NEEDLE.len()..]) {
                return Some(ms);
            }
        }
        carry.clear();
        carry.extend_from_slice(&window[..chunk_len.min(TS_RECORD_MAX_LEN - 1)]);
        end = start;
    }
    None
}

/// `<n>\r\n<n digits>\r\n` at the start of `rest` (what follows [`NEEDLE`]),
/// as the replay's `classify` accepts it: non-zero, at most [`MAX_TS_MS`].
fn parse_ts_value(rest: &[u8]) -> Option<u64> {
    let crlf = rest.iter().take(3).position(|&b| b == b'\r')?;
    let n: usize = std::str::from_utf8(&rest[..crlf]).ok()?.parse().ok()?;
    let digits = rest.get(crlf + 2..crlf + 2 + n)?;
    if rest.get(crlf + 1) != Some(&b'\n')
        || rest.get(crlf + 2 + n..crlf + 4 + n) != Some(&b"\r\n"[..])
    {
        return None;
    }
    let ms: u64 = std::str::from_utf8(digits).ok()?.parse().ok()?;
    (ms != 0 && ms <= MAX_TS_MS).then_some(ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::replay::pseudo::TsRecord;

    fn file_with(bytes: &[u8]) -> tempfile::NamedTempFile {
        let f = tempfile::NamedTempFile::new().expect("tempfile");
        std::fs::write(f.path(), bytes).expect("write");
        f
    }

    fn set(k: &str) -> Vec<u8> {
        format!("*3\r\n$3\r\nSET\r\n${}\r\n{k}\r\n$1\r\nv\r\n", k.len()).into_bytes()
    }

    #[test]
    fn finds_the_last_stamp_behind_an_unstamped_tail() {
        let mut log = Vec::new();
        log.extend_from_slice(TsRecord::new(1_000).as_bytes());
        log.extend(set("a"));
        log.extend_from_slice(TsRecord::new(2_000).as_bytes());
        log.extend(set("b"));
        // An older binary's tail: well over one chunk of unstamped records.
        for i in 0..20_000 {
            log.extend(set(&format!("old{i}")));
        }
        assert!(log.len() > 3 * CHUNK);
        assert_eq!(last_ts_in_file(file_with(&log).path()), Some(2_000));
    }

    #[test]
    fn a_stamp_straddling_a_chunk_boundary_is_found() {
        let stamp = TsRecord::new(1_700_000_000_123);
        for split in 1..stamp.as_bytes().len() {
            // Place the stamp so that `split` of its bytes fall in the chunk
            // before the last one.
            let tail = CHUNK;
            let mut log = vec![b'x'; 100];
            log.extend_from_slice(stamp.as_bytes());
            let pad = tail - (stamp.as_bytes().len() - split);
            log.extend(std::iter::repeat_n(b'y', pad));
            assert_eq!(
                last_ts_in_file(file_with(&log).path()),
                Some(1_700_000_000_123),
                "split {split}"
            );
        }
    }

    #[test]
    fn malformed_and_torn_stamps_are_skipped_like_a_replay_skips_them() {
        let mut log = Vec::new();
        log.extend_from_slice(TsRecord::new(5).as_bytes());
        log.extend_from_slice(TsRecord::new(0).as_bytes()); // zero: skipped
        log.extend_from_slice(TsRecord::new(MAX_TS_MS + 1).as_bytes()); // past 9999
        log.extend_from_slice(b"*2\r\n$7\r\nMOON.TS\r\n$3\r\n1x3\r\n"); // not a number
        log.extend_from_slice(b"*2\r\n$7\r\nMOON.TS\r\n$4\r\n12"); // torn at EOF
        assert_eq!(last_ts_in_file(file_with(&log).path()), Some(5));
    }

    #[test]
    fn no_stamp_and_no_file_are_none() {
        assert_eq!(last_ts_in_file(file_with(&set("a")).path()), None);
        assert_eq!(last_ts_in_file(file_with(b"").path()), None);
        assert_eq!(last_ts_in_file(Path::new("/nonexistent/moon/aof")), None);
    }

    /// The framed per-shard incr: `[u64 lsn][u32 len]` headers do not hide
    /// the RESP bytes of a stamp.
    #[test]
    fn finds_a_stamp_in_a_framed_log() {
        let mut log = Vec::new();
        for (lsn, rec) in [(0u64, TsRecord::new(42).as_bytes().to_vec()), (7, set("a"))] {
            log.extend_from_slice(&lsn.to_le_bytes());
            log.extend_from_slice(&(rec.len() as u32).to_le_bytes());
            log.extend_from_slice(&rec);
        }
        assert_eq!(last_ts_in_file(file_with(&log).path()), Some(42));
    }
}
