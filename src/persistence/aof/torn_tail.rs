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
//! A boot that cannot save the sidecar (other than for a full disk) or cut
//! the file refuses to start and leaves the file as it was, with no partial
//! sidecar: appending behind the torn bytes is the one outcome that loses
//! data. On a full disk the file is cut without a sidecar and the cut is
//! logged with a hex prefix of the bytes (R2b round 4 F4).
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
    /// Where the removed bytes were saved (empty: not saved — a full disk,
    /// or nothing was cut).
    pub sidecar: PathBuf,
    /// The first bytes cut (at most 64), for the log.
    pub prefix: Vec<u8>,
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
/// directory, restore the file's mtime. `Err`: nothing was cut, and no
/// sidecar is left behind.
///
/// On a full disk (ENOSPC) the sidecar cannot be written (R2b round 4 F4):
/// its partial copy is removed and the file is truncated in place without
/// one, as redis does, the cut recorded in the log instead ([`Cut::sidecar`]
/// empty, [`Cut::prefix`] the first bytes cut). Refusing would keep the
/// server down until space is freed — every retry used to leave another
/// empty `.torn-<offset>.<n>` — and the bytes cut are a record the crash
/// left incomplete.
pub fn cut(path: &Path, tail: TornTail) -> std::io::Result<Cut> {
    cut_with(path, tail, save_sidecar)
}

/// The sidecar writer: copy `len` bytes of `file` from its position into
/// `sidecar` and fsync it.
fn save_sidecar(file: &mut std::fs::File, sidecar: &Path, len: u64) -> std::io::Result<()> {
    let mut out = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(sidecar)?;
    let copied = std::io::copy(&mut file.take(len), &mut out)?;
    if copied != len {
        return Err(std::io::Error::other(format!(
            "copied {copied} of {len} torn bytes into {}",
            sidecar.display()
        )));
    }
    out.flush()?;
    out.sync_all()
}

/// `e` is a full disk (or quota).
fn no_space(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::StorageFull | std::io::ErrorKind::QuotaExceeded
    ) || e.raw_os_error() == Some(28)
}

/// [`cut`] with the sidecar writer injected (tests simulate a full disk).
fn cut_with(
    path: &Path,
    tail: TornTail,
    save: impl FnOnce(&mut std::fs::File, &Path, u64) -> std::io::Result<()>,
) -> std::io::Result<Cut> {
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
            prefix: Vec::new(),
        });
    }
    let removed = len - tail.offset;
    let mtime = meta.modified().ok();
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let mut prefix = vec![0u8; removed.min(PREFIX_LEN as u64) as usize];
    file.seek(SeekFrom::Start(tail.offset))?;
    file.read_exact(&mut prefix)?;
    file.seek(SeekFrom::Start(tail.offset))?;
    let mut sidecar = sidecar_path(path, tail.offset);
    match save(&mut file, &sidecar, removed) {
        Ok(()) => crate::persistence::fsync::fsync_directory(dir)?,
        Err(e) => {
            // Never leave a partial copy (it would also make the next
            // retry's name `.torn-<offset>.1`).
            let _ = std::fs::remove_file(&sidecar);
            if !no_space(&e) {
                return Err(e);
            }
            sidecar = PathBuf::new();
        }
    }
    file.set_len(tail.offset)?;
    file.sync_all()?;
    if let Some(t) = mtime {
        let _ = file.set_modified(t);
    }
    crate::persistence::fsync::fsync_directory(dir)?;
    Ok(Cut {
        kept: tail.offset,
        removed,
        sidecar,
        prefix,
    })
}

/// How many of the cut bytes a cut without a sidecar logs.
const PREFIX_LEN: usize = 64;

/// `bytes` as hex.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// [`cut`] at a boot, logged at WARN (what was cut and where it was saved).
/// `Err` is the refusal text: the boot must not start (the file is intact).
pub fn cut_at_boot(path: &Path, tail: TornTail) -> Result<Cut, String> {
    match cut(path, tail) {
        Ok(c) => {
            if c.removed > 0 && c.sidecar.as_os_str().is_empty() {
                tracing::warn!(
                    "AOF {} ended in a record torn by a crash: cut {} byte(s) at offset {} \
                     (the {} bytes before it are complete records and were replayed). The \
                     disk is full, so the cut bytes were NOT saved; they began with (hex) {}",
                    path.display(),
                    c.removed,
                    tail.offset,
                    c.kept,
                    hex(&c.prefix)
                );
            } else if c.removed > 0 {
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

    fn torn_file(dir: &Path) -> (PathBuf, TornTail) {
        let path = dir.join("appendonly.aof");
        std::fs::write(&path, b"*1\r\n$4\r\nPING\r\n*3\r\n$3\r\nSET\r\n$4\r\ntorn").unwrap();
        (
            path,
            TornTail {
                offset: 14,
                len: 21,
            },
        )
    }

    /// R2b round 4 F4: a full disk while saving the sidecar removes the
    /// partial copy and cuts in place, keeping the cut's first bytes for the
    /// log; any other failure leaves the file whole and no sidecar.
    #[test]
    fn a_full_disk_cuts_without_a_sidecar_and_other_errors_leave_none() {
        let tmp = tempfile::tempdir().unwrap();
        let (path, tail) = torn_file(tmp.path());
        let enospc = |f: &mut std::fs::File, side: &Path, _len: u64| {
            let mut out = std::fs::File::create(side)?;
            let mut part = [0u8; 4];
            f.read_exact(&mut part)?;
            out.write_all(&part)?;
            Err(std::io::Error::from_raw_os_error(28))
        };
        let c = cut_with(&path, tail, enospc).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"*1\r\n$4\r\nPING\r\n");
        assert!(c.sidecar.as_os_str().is_empty());
        assert_eq!(c.prefix, b"*3\r\n$3\r\nSET\r\n$4\r\ntorn");
        assert_eq!(hex(&c.prefix[..2]), "2a33");
        let sidecars = std::fs::read_dir(tmp.path())
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().contains(".torn-"))
            .count();
        assert_eq!(sidecars, 0, "no partial sidecar left");

        let tmp = tempfile::tempdir().unwrap();
        let (path, tail) = torn_file(tmp.path());
        let before = std::fs::read(&path).unwrap();
        let eio = |_f: &mut std::fs::File, side: &Path, _len: u64| {
            std::fs::write(side, b"part")?;
            Err(std::io::Error::other("injected I/O error"))
        };
        assert!(cut_with(&path, tail, eio).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before, "file left whole");
        let sidecars = std::fs::read_dir(tmp.path())
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().contains(".torn-"))
            .count();
        assert_eq!(sidecars, 0, "no partial sidecar left");
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
