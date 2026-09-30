#![no_main]
use libfuzzer_sys::fuzz_target;

use moon::persistence::aof_manifest::shard_replay::fuzz::{
    replay_framed, replay_framed_file, replay_resp, replay_resp_file,
};
use moon::persistence::replay::DispatchReplayEngine;
use moon::persistence::replay::clock::{
    pin_replay_clock_ms, pinned_replay_clock_ms, take_open_foreign_segment,
};
use moon::storage::Database;

// Fuzz the AOF replay engine end to end (moon#1283): arbitrary log bytes
// through the three production readers — the framed per-shard incr
// (`[u64 lsn][u32 len][RESP]`, plus the ordered-entry merge), the multi-part
// RESP incr, and the flat `appendonly.aof` (RDB preamble detection included)
// — into real databases via `DispatchReplayEngine` and its pseudo-command
// intercept (`MOON.TS`, `MOON.COLDCUT`, `MOON.SPILLED`; `MOON.TXN` once
// moon#1300 lands).
//
// R2 review of moon#1283: the clean-close marker `MOON.TS <ms> CLOSE` and the
// forward scan across the foreign segment after it (`replay::log_segment`),
// which re-reads the FILE — reached through the file-backed readers (mode 2,
// and mode 3: the framed or RESP incr from a file, pinned as production pins
// it).
//
// Invariants: no reader panics on any input (a corrupt file is refused with
// an error, a torn tail is dropped); the expiry-judgment clock a replay sets
// from `MOON.TS` never outlives the replay's pin scope (a leak would make the
// next replay on this thread — or a live read — judge by a stale log clock).
//
// Input: byte 0 picks the reader (low 2 bits), which well-formed pseudo
// records to put in front of the fuzzed bytes (bits 2..5: COLDCUT, TS, a
// malformed TS, a CLOSE marker), so the intercept arms are reached without
// the fuzzer having to discover `MOON.TS`, and, in mode 3, the layout (bit
// 6: framed); bytes 1..9 are the stamp / watermark value; the rest is the
// log.
fn resp(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn framed(resp: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + resp.len());
    out.extend_from_slice(&0u64.to_le_bytes());
    out.extend_from_slice(&(resp.len() as u32).to_le_bytes());
    out.extend_from_slice(resp);
    out
}

fuzz_target!(|data: &[u8]| {
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    let (value, log) = if rest.len() >= 8 {
        let (v, l) = rest.split_at(8);
        (u64::from_le_bytes(v.try_into().unwrap()), l)
    } else {
        (0, rest)
    };
    let mode = sel & 0b11;
    let mut prefix: Vec<Vec<u8>> = Vec::new();
    let v = value.to_string();
    if sel & 0b0100 != 0 {
        prefix.push(resp(&[b"MOON.COLDCUT", v.as_bytes()]));
    }
    if sel & 0b1000 != 0 {
        prefix.push(resp(&[b"MOON.TS", v.as_bytes()]));
    }
    if sel & 0b1_0000 != 0 {
        prefix.push(resp(&[b"MOON.TS"]));
    }
    if sel & 0b10_0000 != 0 {
        prefix.push(resp(&[b"MOON.TS", v.as_bytes(), b"CLOSE"]));
    }
    let framed_file = mode == 3 && sel & 0b100_0000 != 0;
    let mut bytes = Vec::new();
    for p in &prefix {
        if mode == 0 || framed_file {
            bytes.extend_from_slice(&framed(p));
        } else {
            bytes.extend_from_slice(p);
        }
    }
    bytes.extend_from_slice(log);

    // A production pin is a file mtime capped at the wall clock: keep the
    // fuzzed one in that range (2^45 ms is the year 3084).
    let pin = value % (1u64 << 45);
    let engine = DispatchReplayEngine::new();
    let mut dbs: Vec<Database> = (0..4).map(|_| Database::new()).collect();
    match mode {
        0 => {
            let _pin = pin_replay_clock_ms(pin);
            let _ = replay_framed(&mut dbs, &bytes, &engine);
        }
        1 => {
            let _pin = pin_replay_clock_ms(pin);
            let _ = replay_resp(&mut dbs, &bytes, &engine);
        }
        _ => {
            // The file readers pin their own clock to the file's mtime and
            // scan it after a clean-close marker.
            let Ok(dir) = tempfile::tempdir() else {
                return;
            };
            let path = dir.path().join("appendonly.aof");
            if std::fs::write(&path, &bytes).is_ok() {
                if mode == 2 {
                    let _ = moon::persistence::aof::replay_aof(&mut dbs, &path, &engine);
                } else if framed_file {
                    let _ = replay_framed_file(&mut dbs, &path, &engine);
                } else {
                    let _ = replay_resp_file(&mut dbs, &path, &engine);
                }
            }
            // A segment open at the end of the file was reported for the
            // writer; nobody takes it here, so drop it (bounded registry).
            let _ = take_open_foreign_segment(&path);
        }
    }
    assert_eq!(
        pinned_replay_clock_ms(),
        None,
        "a replay's judgment clock leaked out of its pin scope"
    );
});
