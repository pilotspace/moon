//! A record torn by a crash at the end of an AOF file — cut at boot, before
//! anything is appended behind it (R2b round 3 F-B, redis
//! `aof-load-truncated`).
//!
//! Every replay reader keeps the complete records before a torn tail and
//! stops there. The file used to be left as it was, and the writer then
//! APPENDED behind the torn bytes: the torn record's declared length (a RESP
//! bulk `$<n>`, a framed `[lsn][len]` header) swallowed every record written
//! after it, so each write acknowledged after that boot was lost — silently,
//! at the NEXT boot (a `kill -9` during the append of a 400 MB `SET`: boot 3
//! lost every write of boot 2). redis truncates the file at the last
//! complete record before it serves (`aof-load-truncated yes`, the default);
//! so does moon now, at every boot-time replay of a file it then appends to:
//! the flat `appendonly.aof` ([`super::replay_aof_at_boot`]), the
//! single-shard multi-part incr and each per-shard incr
//! (`aof_manifest::shard_replay`).
//!
//! The cut bytes are not discarded: they are copied first to a sidecar next
//! to the file, `<name>.torn-<offset>` (never overwriting one), so a tail
//! that was not a crash's — a damaged length field that made a later
//! stretch of the file look torn — can still be inspected and recovered.
//! A boot that cannot save the sidecar or cut the file refuses to start and
//! leaves the file as it was: appending behind the torn bytes is the one
//! outcome that loses data.
//!
//! The cut runs before any writer appends: the tokio flat writer opens the
//! file only after recovery (`open_gate`); the monoio writers have their
//! incr open in `O_APPEND` mode but write nothing before the listener
//! starts, and an `O_APPEND` write lands at the end of the file as it is
//! then. The file's mtime is kept (the replay clock judges expiry by it,
//! moon#1277).

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Where a replayed log stopped short: the record starting at byte `offset`
/// runs to the end of the file (`len` bytes) and is incomplete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TornTail {
    pub offset: u64,
    pub len: u64,
}

/// What [`cut`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cut {
    /// The file's length now (the torn record's offset).
    pub kept: u64,
    /// Bytes removed from the file.
    pub removed: u64,
    /// Where the removed bytes were saved.
    pub sidecar: PathBuf,
}

/// The first free `<path>.torn-<offset>[.<n>]`.
fn sidecar_path(path: &Path, offset: u64) -> PathBuf {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let mut candidate = dir.join(format!("{name}.torn-{offset}"));
    let mut n = 1u32;
    while candidate.exists() {
        candidate = dir.join(format!("{name}.torn-{offset}.{n}"));
        n += 1;
    }
    candidate
}

/// Cut `path` at `tail.offset`: copy the bytes from there to the end into a
/// fresh sidecar (fsynced), truncate the file (fsynced), fsync the
/// directory, restore the file's mtime. `Err`: nothing was cut (a sidecar
/// may remain; it is a copy).
pub fn cut(path: &Path, tail: TornTail) -> std::io::Result<Cut> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    let meta = file.metadata()?;
    let len = meta.len();
    if tail.offset >= len {
        // Nothing past the offset (already cut by an earlier boot that
        // crashed before logging): a no-op.
        return Ok(Cut {
            kept: len,
            removed: 0,
            sidecar: PathBuf::new(),
        });
    }
    let mtime = meta.modified().ok();
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let sidecar = sidecar_path(path, tail.offset);
    {
        let mut out = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&sidecar)?;
        file.seek(SeekFrom::Start(tail.offset))?;
        let copied = std::io::copy(&mut (&mut file).take(len - tail.offset), &mut out)?;
        if copied != len - tail.offset {
            return Err(std::io::Error::other(format!(
                "copied {copied} of {} torn bytes into {}",
                len - tail.offset,
                sidecar.display()
            )));
        }
        out.flush()?;
        out.sync_all()?;
    }
    crate::persistence::fsync::fsync_directory(dir)?;
    file.set_len(tail.offset)?;
    file.sync_all()?;
    if let Some(t) = mtime {
        let _ = file.set_modified(t);
    }
    crate::persistence::fsync::fsync_directory(dir)?;
    Ok(Cut {
        kept: tail.offset,
        removed: len - tail.offset,
        sidecar,
    })
}

/// [`cut`] at a boot, logged at WARN (what was cut and where it was saved).
/// `Err` is the refusal text: the boot must not start (the file is intact).
pub fn cut_at_boot(path: &Path, tail: TornTail) -> Result<Cut, String> {
    match cut(path, tail) {
        Ok(c) => {
            if c.removed > 0 {
                tracing::warn!(
                    "AOF {} ended in a record torn by a crash: cut {} byte(s) at offset {} \
                     (the {} bytes before it are complete records and were replayed). The \
                     cut bytes are saved in {}; delete it once you are satisfied nothing \
                     there is needed (redis aof-load-truncated)",
                    path.display(),
                    c.removed,
                    tail.offset,
                    c.kept,
                    c.sidecar.display()
                );
            }
            Ok(c)
        }
        Err(e) => Err(format!(
            "{} ends in a record torn by a crash (offset {}, {} bytes), and cutting it \
             failed: {e}. The file is unchanged. Appending behind the torn bytes would lose \
             every later write at the next boot, so moon will not start: free space or fix \
             permissions in {}, or truncate a copy by hand (truncate -s {} <file>) after \
             saving the tail",
            path.display(),
            tail.offset,
            tail.len,
            path.parent().unwrap_or_else(|| Path::new(".")).display(),
            tail.offset
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cut_keeps_the_prefix_saves_the_tail_and_the_mtime() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("appendonly.aof");
        std::fs::write(&path, b"*1\r\n$4\r\nPING\r\n*3\r\n$3\r\nSET\r\n$4\r\ntorn").unwrap();
        let old = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(old)
            .unwrap();
        let tail = TornTail {
            offset: 14,
            len: 23,
        };
        let c = cut_at_boot(&path, tail).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"*1\r\n$4\r\nPING\r\n");
        assert_eq!((c.kept, c.removed), (14, 21));
        assert_eq!(
            std::fs::read(&c.sidecar).unwrap(),
            b"*3\r\n$3\r\nSET\r\n$4\r\ntorn"
        );
        assert!(c.sidecar.ends_with("appendonly.aof.torn-14"));
        assert_eq!(std::fs::metadata(&path).unwrap().modified().unwrap(), old);
        // A second cut at the same offset is a no-op; a new tail there gets
        // its own sidecar, never overwriting the first.
        assert_eq!(cut(&path, tail).unwrap().removed, 0);
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"*2\r\n")
            .unwrap();
        let c2 = cut(&path, TornTail { offset: 14, len: 4 }).unwrap();
        assert_ne!(c2.sidecar, c.sidecar);
        assert_eq!(std::fs::read(&c.sidecar).unwrap().len(), 21);
    }

    #[test]
    fn a_failed_cut_leaves_the_file_and_explains() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("missing.aof");
        let err = cut_at_boot(&path, TornTail { offset: 3, len: 9 }).unwrap_err();
        assert!(err.contains("will not start"), "{err}");
        assert!(err.contains("truncate -s 3"), "{err}");
    }
}
