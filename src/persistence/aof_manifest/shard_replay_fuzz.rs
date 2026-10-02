//! Fuzzing entry points into the incr-file replay readers (moon#1283):
//! `fuzz/fuzz_targets/aof_incr_replay.rs` drives arbitrary bytes through
//! them into real databases, with the production [`DispatchReplayEngine`]
//! and its pseudo-command intercept (`MOON.TS`, `MOON.COLDCUT`,
//! `MOON.SPILLED`, `MOON.TXN`).
//!
//! The flat `appendonly.aof` reader (`aof::replay_aof`) is public already;
//! the target reaches it through a temp file.
//!
//! [`DispatchReplayEngine`]: crate::persistence::replay::DispatchReplayEngine

use crate::persistence::replay::CommandReplayEngine;
use crate::storage::Database;

/// Replay `data` as a framed per-shard incr (`[u64 lsn][u32 len][RESP]…`),
/// then merge-replay the `OrderedAcrossShards` entries it buffered, as
/// `replay_per_shard` + `replay_ordered_merge` do. Returns the inline record
/// count, or `None` when the reader refused the file (a corrupt entry: the
/// boot would fail loudly, which is the contract — never a panic).
pub fn replay_framed(
    databases: &mut [Database],
    data: &[u8],
    engine: &dyn CommandReplayEngine,
) -> Option<usize> {
    let mut ordered = Vec::new();
    let (count, _max_lsn, _torn) =
        super::replay_incr_framed(0, databases, data, engine, &mut ordered).ok()?;
    let mut per_shard: [&mut [Database]; 1] = [databases];
    let _ = super::replay_ordered_merge(&mut per_shard, ordered, engine);
    Some(count)
}

/// Replay `data` as a multi-part incr (plain RESP, streamed through the
/// bounded chunk reader). `None` when the reader refused it.
pub fn replay_resp(
    databases: &mut [Database],
    data: &[u8],
    engine: &dyn CommandReplayEngine,
) -> Option<usize> {
    super::replay_incr_resp(databases, data, engine)
        .ok()
        .map(|(n, _)| n)
}

/// Replay the framed per-shard incr at `path` as `replay_per_shard` does:
/// pinned to its mtime, with the positional foreign-segment rule scanning the
/// file itself (R2 review of moon#1283). `None` when the reader refused it.
pub fn replay_framed_file(
    databases: &mut [Database],
    path: &std::path::Path,
    engine: &dyn CommandReplayEngine,
) -> Option<usize> {
    use crate::persistence::replay::clock::{LogFormat, pin_replay_clock_to_log};
    let data = std::fs::read(path).ok()?;
    let _clock = pin_replay_clock_to_log(path, LogFormat::Framed);
    replay_framed(databases, &data, engine)
}

/// Replay the multi-part RESP incr at `path` as `replay_multi_part` does
/// (see [`replay_framed_file`]).
pub fn replay_resp_file(
    databases: &mut [Database],
    path: &std::path::Path,
    engine: &dyn CommandReplayEngine,
) -> Option<usize> {
    use crate::persistence::replay::clock::{LogFormat, pin_replay_clock_to_log};
    let file = std::fs::File::open(path).ok()?;
    let _clock = pin_replay_clock_to_log(path, LogFormat::Resp);
    super::replay_incr_resp(databases, file, engine)
        .ok()
        .map(|(n, _)| n)
}

/// A stable-toolchain smoke run of the fuzz target's contract (the libFuzzer
/// target itself needs nightly): a well-formed stamped log, then thousands of
/// seeded mutations of it, through both readers. Nothing may panic and the
/// replay clock may never leak out of its pin scope.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::replay::DispatchReplayEngine;
    use crate::persistence::replay::clock::{pin_replay_clock_ms, pinned_replay_clock_ms};

    fn resp(parts: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", parts.len()).into_bytes();
        for p in parts {
            out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            out.extend_from_slice(p);
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    fn log(framed: bool) -> Vec<u8> {
        let records: Vec<Vec<u8>> = vec![
            resp(&[b"MOON.COLDCUT", b"1"]),
            resp(&[b"MOON.TS", b"1790000000000"]),
            resp(&[b"SET", b"k", b"5", b"PXAT", b"1790000000100"]),
            resp(&[b"MOON.TS", b"1790000000200"]),
            resp(&[b"INCR", b"k"]),
            resp(&[b"SELECT", b"1"]),
            resp(&[b"MOON.TS", b"0"]),
            resp(&[b"MOON.TS", b"99999999999999999999"]),
            resp(&[b"HSET", b"h", b"f", b"v"]),
            resp(&[b"MOON.SPILLED", b"3", b"k"]),
            resp(&[b"MOON.TS"]),
            resp(&[b"APPEND", b"s", b"x"]),
            // R2 review: a clean close, a foreign segment, the next session.
            resp(&[b"MOON.TS", b"1790000000300", b"CLOSE"]),
            resp(&[b"INCR", b"k"]),
            resp(&[b"MOON.TS", b"1790000000400"]),
            resp(&[b"MOON.TS", b"1790000000500", b"CLOSE"]),
            resp(&[b"MOON.TS", b"1", b"OPEN"]),
            resp(&[b"DEL", b"k"]),
            // moon#1300: transaction blocks — ended, paused, reset, cut by
            // the end of the file, malformed.
            resp(&[b"MOON.TXN", b"BEGIN", b"7"]),
            resp(&[b"SET", b"k", b"txn"]),
            resp(&[b"MOON.TXN", b"PAUSE", b"7"]),
            resp(&[b"SET", b"o", b"other"]),
            resp(&[b"MOON.TXN", b"BEGIN", b"7"]),
            resp(&[b"HSET", b"h", b"f", b"txn"]),
            resp(&[b"MOON.TXN", b"END", b"7"]),
            resp(&[b"MOON.TXN", b"END", b"9"]),
            resp(&[b"MOON.TXN", b"BEGIN", b"8"]),
            resp(&[b"INCR", b"c"]),
            resp(&[b"MOON.TXN", b"RESET"]),
            resp(&[b"MOON.TXN", b"BEGIN", b"0"]),
            resp(&[b"MOON.TXN", b"BEGIN", b"3"]),
            resp(&[b"APPEND", b"s", b"y"]),
        ];
        let mut out = Vec::new();
        for (i, r) in records.iter().enumerate() {
            if framed {
                // Every third record is an ordered (merge-replayed) entry.
                let lsn = if i % 3 == 2 {
                    crate::persistence::aof::ORDERED_LSN_FLAG | i as u64
                } else {
                    i as u64
                };
                out.extend_from_slice(&lsn.to_le_bytes());
                out.extend_from_slice(&(r.len() as u32).to_le_bytes());
            }
            out.extend_from_slice(r);
        }
        out
    }

    #[test]
    fn mutated_stamped_logs_never_panic_nor_leak_the_clock() {
        let engine = DispatchReplayEngine::new();
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("incr.aof");
        for framed in [true, false] {
            let good = log(framed);
            let mut dbs: Vec<Database> = (0..4).map(|_| Database::new()).collect();
            {
                let _pin = pin_replay_clock_ms(1);
                let n = if framed {
                    replay_framed(&mut dbs, &good, &engine)
                } else {
                    replay_resp(&mut dbs, &good, &engine)
                };
                assert!(n.is_some(), "the well-formed log replays (framed {framed})");
            }
            assert_eq!(pinned_replay_clock_ms(), None);
            for _ in 0..3_000 {
                let mut bytes = good.clone();
                for _ in 0..1 + next() % 4 {
                    let at = (next() as usize) % bytes.len().max(1);
                    match next() % 3 {
                        0 if !bytes.is_empty() => bytes[at] = next() as u8,
                        1 => bytes.truncate(at),
                        _ => bytes.insert(at.min(bytes.len()), next() as u8),
                    }
                }
                let mut dbs: Vec<Database> = (0..4).map(|_| Database::new()).collect();
                {
                    let _pin = pin_replay_clock_ms(next() % (1 << 45));
                    if framed {
                        let _ = replay_framed(&mut dbs, &bytes, &engine);
                    } else {
                        let _ = replay_resp(&mut dbs, &bytes, &engine);
                    }
                }
                assert_eq!(pinned_replay_clock_ms(), None, "the replay clock leaked");
                // The file readers: a clean-close marker scans the file.
                std::fs::write(&path, &bytes).expect("write");
                let mut dbs: Vec<Database> = (0..4).map(|_| Database::new()).collect();
                if framed {
                    let _ = replay_framed_file(&mut dbs, &path, &engine);
                } else {
                    let _ = replay_resp_file(&mut dbs, &path, &engine);
                }
                assert_eq!(pinned_replay_clock_ms(), None, "the replay clock leaked");
                // moon#1300: every reader closes the blocks of its file.
                assert_eq!(engine.finish_log(&mut dbs), 0, "a block outlived its file");
                let _ = crate::persistence::replay::clock::take_open_foreign_segment(&path);
                let _ = crate::persistence::replay::txn::take_reset_owed(&path);
            }
        }
    }
}
