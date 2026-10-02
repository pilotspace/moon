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

use std::path::Path;

use super::flat_file::{FLAT_AOF_NAME, flat_aof_path};

/// Why this boot must not start, or `None`. `has_manifest`: an AOF manifest
/// exists in `dir`. `reads_single_shard_manifest`: this build replays a
/// `--shards 1` manifest (monoio).
#[must_use]
pub fn refusal(
    dir: &Path,
    num_shards: usize,
    has_manifest: bool,
    reads_single_shard_manifest: bool,
) -> Option<String> {
    let flat = flat_aof_path(dir);
    let flat_len = std::fs::metadata(&flat).map(|m| m.len()).unwrap_or(0);
    if !has_manifest && num_shards >= 2 && flat_len > 0 {
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
    if has_manifest && num_shards == 1 && !reads_single_shard_manifest {
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
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_flat_aof_with_data_refuses_a_multi_shard_boot_without_a_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path();
        assert!(refusal(d, 4, false, true).is_none(), "no file");
        std::fs::write(flat_aof_path(d), b"").unwrap();
        assert!(refusal(d, 4, false, true).is_none(), "an empty file");
        std::fs::write(flat_aof_path(d), b"*1\r\n$4\r\nPING\r\n").unwrap();
        let m = refusal(d, 4, false, false).expect("refused");
        assert!(m.contains("--migrate-aof-shards 4"), "{m}");
        assert!(m.contains("moon#1321"), "{m}");
        assert!(refusal(d, 1, false, false).is_none(), "--shards 1 reads it");
        assert!(
            refusal(d, 4, true, true).is_none(),
            "a manifest owns the dir"
        );
    }

    #[test]
    fn a_single_shard_manifest_refuses_a_build_that_does_not_read_it() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path();
        let m = refusal(d, 1, true, false).expect("refused");
        assert!(m.contains("monoio build"), "{m}");
        assert!(m.contains("BGSAVE"), "{m}");
        assert!(refusal(d, 1, true, true).is_none(), "monoio reads it");
        assert!(refusal(d, 1, false, false).is_none(), "no manifest");
    }
}
