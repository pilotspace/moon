//! Boot refusals for an AOF on disk the configured server cannot replay
//! correctly (R2b round 3 F-C, F-H). Checked in `main.rs` before any AOF
//! writer starts and before recovery, beside the TopLevel-manifest-with-
//! `--shards N` refusal; the boot exits 2 with the message.
//!
//! - **F-C (moon#1321):** a flat `appendonly.aof` holding a byte and no
//!   manifest, booted with `--shards N >= 2`. Every shard's recovery replayed
//!   the WHOLE file (the flat layout is single-shard), the fresh PerShard
//!   manifest then stored N copies of the dataset as its bases and the flat
//!   file was retired — permanently: 20 keys came back as `DBSIZE` 80, and a
//!   key's stale copies on other shards resurfaced on replicas.
//! - **F-H:** a manifest (written by the monoio build at `--shards 1`) booted
//!   by the tokio build at `--shards 1`, which reads only the flat file. It
//!   booted EMPTY with a WARN, wrote a new flat file, and the next monoio
//!   boot replayed the stale manifest and retired that newer file: every
//!   tokio-era write was lost.
//!
//!   A flat file that holds no data — a fresh tokio generation's head alone
//!   (`MOON.COLDCUT`, its dead-slot DELs, clock stamps) — is not refused: the
//!   multi-shard boot retires it (R2b round 2 F2).
//! - **R2b round 4 F6:** a dir with BOTH a manifest and a flat file holding
//!   data (left by the pre-fix F-H bug: a stale monoio manifest and a newer
//!   tokio flat file) was refused with "boot it with the monoio build" —
//!   whose boot retires the flat file and its writes. Neither layout may be
//!   picked silently: the refusal says both hold data and how to recover.
//!   The F-H refusal names only a single-shard (TopLevel) manifest (F8: a
//!   per-shard one gets the shard-count refusal instead).
//! - **moon#1321 (other half):** `--appendfilename` with the tokio flat
//!   layout: the writer appended to the named file, recovery replayed only
//!   `appendonly.aof` — every write was lost at the next boot (DBSIZE 0).

use std::path::Path;

use super::flat_file::{FLAT_AOF_NAME, flat_aof_path};
use crate::persistence::aof_manifest::AofLayout;

/// Why this boot must not start, or `None`. `manifest`: the layout of the
/// AOF manifest in `dir`, if any. `reads_single_shard_manifest`: this build
/// replays a `--shards 1` manifest (monoio). `appendfilename`:
/// `--appendfilename`.
#[must_use]
pub fn refusal(
    dir: &Path,
    num_shards: usize,
    manifest: Option<AofLayout>,
    reads_single_shard_manifest: bool,
    appendfilename: &str,
) -> Option<String> {
    let flat = flat_aof_path(dir);
    let flat_len = std::fs::metadata(&flat).map(|m| m.len()).unwrap_or(0);
    let flat_data = flat_len > 0 && holds_data(&flat);
    let has_manifest = manifest.is_some();
    if has_manifest && flat_data {
        let aofdir = dir.join("appendonlydir");
        return Some(format!(
            "{dir} holds BOTH an AOF manifest ({aofdir}) and a single-file {FLAT_AOF_NAME} \
             with data ({flat_len} bytes). A manifest boot would retire the file and the \
             writes only it holds; a single-file boot would ignore the manifest. Neither is \
             retired automatically. Decide which holds the data you need: to keep the \
             manifest's, move {flat} aside and boot; to keep the single file's, move \
             {aofdir} aside and boot with --shards 1 (the build that wrote it); to merge \
             them, boot each on its own copy of this directory (one copy without the file, \
             one without {aofdir}) and copy the keys you need across (DUMP / RESTORE)",
            dir = dir.display(),
            aofdir = aofdir.display(),
            flat = flat.display(),
        ));
    }
    if !has_manifest && num_shards >= 2 && flat_data {
        return Some(format!(
            "{} ({flat_len} bytes) is a single-shard AOF and this boot has --shards \
             {num_shards}: replaying it would load the whole dataset into every shard \
             (moon#1321). Boot this dir with --shards 1, or migrate it to the per-shard \
             layout first: `moon --migrate-aof-from {} --migrate-aof-to <new dir> \
             --migrate-aof-shards {num_shards}`, then start with --dir <new dir> --shards \
             {num_shards} --appendonly yes",
            flat.display(),
            dir.display()
        ));
    }
    if manifest == Some(AofLayout::TopLevel) && num_shards == 1 && !reads_single_shard_manifest {
        return Some(format!(
            "{} holds a single-shard AOF manifest (written by the monoio build), which this \
             build (tokio) does not replay at --shards 1: booting would serve an EMPTY \
             dataset and start a new {FLAT_AOF_NAME} that a later monoio boot discards. \
             Boot this dir with the monoio build; or, to move it to the tokio build, BGSAVE \
             on the monoio build, stop it, move {} aside, and start this build (the \
             snapshot then loads and a new {FLAT_AOF_NAME} is opened over it)",
            dir.join("appendonlydir").display(),
            dir.join("appendonlydir").display()
        ));
    }
    if num_shards == 1 && !reads_single_shard_manifest && appendfilename != FLAT_AOF_NAME {
        return Some(format!(
            "--appendfilename {appendfilename}: this build (tokio --shards 1) appends to that \
             file but its recovery replays only {FLAT_AOF_NAME} (moon#1321), so every write \
             would be lost at the next boot. Drop the option (rename an existing \
             {appendfilename} to {FLAT_AOF_NAME} first)"
        ));
    }
    None
}

/// Whether the flat AOF at `path` holds a record that may create data: an
/// RDB preamble, or any record other than the pseudo-records (`MOON.*`),
/// `SELECT` and the deletions a fresh generation's head carries. Streams the
/// file and stops at the first such record; an unreadable file counts as
/// data (refusing is the safe side).
fn holds_data(path: &Path) -> bool {
    use crate::persistence::replay::chunks::{ReplayChunks, ReplayNext};
    use crate::protocol::Frame;
    use std::io::Read;
    let Ok(mut file) = std::fs::File::open(path) else {
        return true;
    };
    let mut magic = [0u8; 4];
    if file.read_exact(&mut magic).is_ok() && &magic == b"MOON" {
        return true;
    }
    let Ok(file) = std::fs::File::open(path) else {
        return true;
    };
    let mut chunks = ReplayChunks::new(file, 0);
    loop {
        match chunks.next_frame() {
            Ok(ReplayNext::Frame(Frame::Array(arr))) => {
                let Some(Frame::BulkString(cmd)) = arr.first() else {
                    return true;
                };
                let harmless = cmd.len() > 5 && cmd[..5].eq_ignore_ascii_case(b"MOON.")
                    || [&b"SELECT"[..], b"DEL", b"UNLINK"]
                        .iter()
                        .any(|c| cmd.eq_ignore_ascii_case(c));
                if !harmless {
                    return true;
                }
            }
            Ok(ReplayNext::End) | Ok(ReplayNext::Truncated { .. }) => return false,
            Ok(_) | Err(_) => return true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_flat_aof_with_data_refuses_a_multi_shard_boot_without_a_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path();
        assert!(
            refusal(d, 4, None, true, FLAT_AOF_NAME).is_none(),
            "no file"
        );
        std::fs::write(flat_aof_path(d), b"").unwrap();
        assert!(
            refusal(d, 4, None, true, FLAT_AOF_NAME).is_none(),
            "an empty file"
        );
        // A fresh generation's head alone: no data, the boot retires it.
        let head = b"*2\r\n$12\r\nMOON.COLDCUT\r\n$1\r\n7\r\n*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n";
        std::fs::write(flat_aof_path(d), head).unwrap();
        assert!(
            refusal(d, 4, None, false, FLAT_AOF_NAME).is_none(),
            "a head alone"
        );
        let mut data = head.to_vec();
        data.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
        std::fs::write(flat_aof_path(d), &data).unwrap();
        let m = refusal(d, 4, None, false, FLAT_AOF_NAME).expect("refused");
        assert!(m.contains("--migrate-aof-shards 4"), "{m}");
        assert!(m.contains("moon#1321"), "{m}");
        assert!(
            refusal(d, 1, None, false, FLAT_AOF_NAME).is_none(),
            "--shards 1 reads it"
        );
        // An RDB preamble is data, and so is damage the scan cannot read.
        std::fs::write(flat_aof_path(d), b"MOON\x0bpreamble").unwrap();
        assert!(
            refusal(d, 4, None, false, FLAT_AOF_NAME).is_some(),
            "preamble"
        );
        std::fs::write(flat_aof_path(d), b"?garbage\r\n").unwrap();
        assert!(
            refusal(d, 4, None, false, FLAT_AOF_NAME).is_some(),
            "unreadable"
        );
        // F6: a manifest AND a flat file with data: both refused, whatever
        // the runtime or shard count; a head-only flat file is retired.
        let m = refusal(d, 4, Some(AofLayout::PerShard), true, FLAT_AOF_NAME).expect("both");
        assert!(m.contains("BOTH") && m.contains("DUMP / RESTORE"), "{m}");
        assert!(refusal(d, 1, Some(AofLayout::TopLevel), true, FLAT_AOF_NAME).is_some());
        std::fs::write(flat_aof_path(d), head).unwrap();
        assert!(
            refusal(d, 4, Some(AofLayout::PerShard), true, FLAT_AOF_NAME).is_none(),
            "a manifest owns the dir; the head-only file is retired"
        );
    }

    /// F8: the F-H refusal is for a single-shard (TopLevel) manifest only; a
    /// per-shard one under --shards 1 gets main.rs's shard-count refusal.
    #[test]
    fn only_a_top_level_manifest_gets_the_tokio_single_shard_refusal() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path();
        assert!(refusal(d, 1, Some(AofLayout::PerShard), false, FLAT_AOF_NAME).is_none());
        assert!(refusal(d, 1, Some(AofLayout::TopLevel), false, FLAT_AOF_NAME).is_some());
    }

    #[test]
    fn a_single_shard_manifest_refuses_a_build_that_does_not_read_it() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path();
        let m = refusal(d, 1, Some(AofLayout::TopLevel), false, FLAT_AOF_NAME).expect("refused");
        assert!(m.contains("monoio build"), "{m}");
        assert!(m.contains("BGSAVE"), "{m}");
        assert!(
            refusal(d, 1, Some(AofLayout::TopLevel), true, FLAT_AOF_NAME).is_none(),
            "monoio reads it"
        );
        assert!(
            refusal(d, 1, None, false, FLAT_AOF_NAME).is_none(),
            "no manifest"
        );
    }

    /// moon#1321's other half: the tokio flat layout's writer honours
    /// `--appendfilename` but its recovery does not.
    #[test]
    fn a_non_default_appendfilename_refuses_the_flat_layout() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path();
        let m = refusal(d, 1, None, false, "foo.aof").expect("refused");
        assert!(m.contains("--appendfilename foo.aof"), "{m}");
        assert!(
            refusal(d, 1, None, true, "foo.aof").is_none(),
            "monoio ignores it"
        );
        assert!(
            refusal(d, 4, None, false, "foo.aof").is_none(),
            "per-shard ignores it"
        );
    }
}
