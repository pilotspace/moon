//! The clean-close marker every AOF writer appends when it stops in order
//! (R2 review of moon#1283).
//!
//! On SHUTDOWN, SIGTERM or a closed channel, each writer loop appends
//! [`RecordCtx::close_records`] — the session stamp it still owes, if it
//! wrote nothing, then `MOON.TS <ms> CLOSE` — to its live incr before its
//! final sync, which makes the marker durable. The replay's positional rule
//! (`replay::clock`) reads the records after a marker and before the next
//! stamp as another binary's. A writer whose stream is torn (its write-error
//! latch) appends nothing, the marker included: bytes after a tear cannot be
//! parsed.

use super::*;

/// The bytes of [`RecordCtx::close_records`], each framed `[lsn 0][len]` for
/// a per-shard incr (`framed`), bare RESP otherwise.
pub(super) fn close_bytes(ctx: &mut RecordCtx, framed: bool) -> Vec<u8> {
    let mut out =
        Vec::with_capacity(2 * (crate::persistence::replay::pseudo::CLOSE_RECORD_MAX_LEN + 12));
    for rec in ctx.close_records() {
        if framed {
            out.extend_from_slice(&0u64.to_le_bytes());
            out.extend_from_slice(&(rec.len() as u32).to_le_bytes());
        }
        out.extend_from_slice(&rec);
    }
    out
}

/// R2b round 4 F5: a writer stopping before the boot completed appends no
/// close records ([`crate::persistence::aof::writer_stop::boot_pending`]).
fn skip_for_pending_boot() -> bool {
    let pending = crate::persistence::aof::writer_stop::boot_pending();
    if pending {
        info!(
            "AOF writer stopping before the boot completed: no clean-close marker appended \
             (the file stays as the boot found it)"
        );
    }
    pending
}

fn warn_unwritten(e: &std::io::Error) {
    error!(
        "AOF clean-close marker not written ({e}): the next boot reads this stop as a crash, \
         and records a downgraded binary appends before it are judged by the last stamp"
    );
}

/// Append the close records through a std file (the monoio writers). The
/// caller's final sync makes them durable.
#[cfg(feature = "runtime-monoio")]
pub(super) fn append_sync(file: &mut impl std::io::Write, ctx: &mut RecordCtx, framed: bool) {
    if skip_for_pending_boot() {
        return;
    }
    match file.write_all(&close_bytes(ctx, framed)) {
        Ok(()) => crate::persistence::aof::writer_stop::note_close_marker(),
        Err(e) => warn_unwritten(&e),
    }
}

/// Append the close records through the tokio writers' `BufWriter`. The
/// caller's final flush and sync make them durable.
#[cfg(feature = "runtime-tokio")]
pub(super) async fn append_async<W>(writer: &mut W, ctx: &mut RecordCtx, framed: bool)
where
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;
    if skip_for_pending_boot() {
        return;
    }
    match writer.write_all(&close_bytes(ctx, framed)).await {
        Ok(()) => crate::persistence::aof::writer_stop::note_close_marker(),
        Err(e) => warn_unwritten(&e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::replay::pseudo::{is_close_record, is_ts_record};

    #[test]
    fn a_close_is_one_marker_framed_with_lsn_0_or_bare() {
        let mut ctx = RecordCtx::new();
        let bare = close_bytes(&mut ctx, false);
        assert!(
            is_close_record(&bare),
            "{:?}",
            String::from_utf8_lossy(&bare)
        );
        let framed = close_bytes(&mut ctx, true);
        assert_eq!(&framed[..8], &0u64.to_le_bytes());
        let len = u32::from_le_bytes(framed[8..12].try_into().expect("4")) as usize;
        assert_eq!(framed.len(), 12 + len);
        assert!(is_close_record(&framed[12..]));
    }

    /// A reopened file whose boot replay left a foreign segment open at its
    /// end: an orderly stop with nothing written still owes that segment its
    /// stamp, BEFORE the marker, so a later boot judges it the same way.
    #[test]
    fn a_close_with_nothing_written_pays_the_owed_segment_stamp_first() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("moon.aof.1.incr.aof");
        std::fs::write(&path, b"").expect("create");
        crate::persistence::replay::clock::tests_support::report_open_segment(&path, 4_242);
        let mut ctx = RecordCtx::appending(&path);
        let out = close_bytes(&mut ctx, false);
        let ts = crate::persistence::replay::pseudo::TsRecord::new(4_242);
        assert!(out.starts_with(ts.as_bytes()));
        assert!(is_ts_record(&out));
        assert!(is_close_record(&out[ts.as_bytes().len()..]));
    }
}
