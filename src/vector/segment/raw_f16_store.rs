//! Storage backend for the exact-rerank f16 sidecar (HQ-1).
//!
//! An [`ImmutableSegment`](super::immutable::ImmutableSegment) built fresh by
//! compaction/merge already owns its `raw_f16` buffer as a `Vec<u16>` — no
//! extra work needed there. A segment **reloaded from disk** used to
//! re-materialize that same buffer with a fresh `fs::read` + decode, which
//! doubles resident memory for the sidecar on every warm start / segment
//! promotion. [`RawF16Store::Mapped`] instead memory-maps `raw_f16.bin` and
//! hands out a zero-copy `&[u16]` view backed by the kernel page cache — RSS
//! only grows for pages the rerank path actually touches.
//!
//! # The seal contract this relies on
//!
//! `raw_f16.bin` lives inside a `segment-{id}/` directory written by
//! [`crate::vector::persistence::segment_io::write_segment_files`] and made
//! visible only via the staged-directory -> final-directory atomic rename in
//! [`crate::vector::persistence::segment_io::write_immutable_segment_staged`].
//! After that rename:
//! - No moon code ever opens `raw_f16.bin` for writing again — the file is
//!   part of an immutable segment.
//! - The only later operation on the containing directory is a whole-directory
//!   removal during GC (`run_snapshot_job` / `sweep_orphans_from_disk`) once
//!   the segment is superseded by a merge. `unlink`/`remove_dir_all` on POSIX
//!   does not invalidate an already-established `mmap` of a file that still
//!   has an open mapping — the kernel keeps the inode's pages alive until the
//!   last reference (including mmaps) drops. So a concurrent GC racing an
//!   in-flight reader is safe, not just the common case.
//!
//! This mirrors the existing warm-segment seal contract documented in
//! [`crate::vector::persistence::sealed_mmap`] and the CSR mmap loader in
//! [`crate::graph::csr::mmap`] — same invariant, applied to a new file.

use std::fs::File;
use std::io;
use std::path::Path;

use memmap2::Mmap;

/// Backing storage for an immutable segment's exact-rerank f16 sidecar.
pub enum RawF16Store {
    /// Freshly-built segment (compaction/merge): the sidecar buffer it
    /// already owns in memory.
    Owned(Vec<u16>),
    /// Segment reloaded from disk: zero-copy view into `raw_f16.bin`'s page
    /// cache. `len` is the number of `u16` halves (== `mmap.len() / 2`).
    Mapped { mmap: Mmap, len: usize },
}

impl RawF16Store {
    /// Memory-map `path` (a sealed `raw_f16.bin`, see module docs) read-only.
    ///
    /// Returns `Ok(None)` when the file's byte length does not match
    /// `expected_halves * 2` — the caller treats this exactly like a missing
    /// file (size mismatch means a corrupt/truncated sidecar; search falls
    /// back to quantized ADC distances rather than trusting bad data).
    ///
    /// # Errors
    ///
    /// Propagates `File::open`/`metadata`/`mmap` I/O errors (including
    /// "not found" for pre-sidecar segments) — callers map those to "no
    /// sidecar" too.
    pub fn map_file(path: &Path, expected_halves: usize) -> io::Result<Option<Self>> {
        let file = File::open(path)?;
        let len_bytes = file.metadata()?.len() as usize;
        if len_bytes != expected_halves * 2 {
            return Ok(None);
        }
        if len_bytes == 0 {
            // memmap2 refuses to map a zero-length file; an empty sidecar
            // (e.g. a segment with zero live vectors) needs no mapping.
            return Ok(Some(Self::Owned(Vec::new())));
        }
        // `path` is `raw_f16.bin` inside a `segment-{id}` directory. It is
        // written exactly once by `write_segment_files` and only made
        // visible via the staged-dir -> final-dir atomic rename in
        // `write_immutable_segment_staged` (see module docs for the full
        // seal contract, including why a racing GC removal is also safe).
        // SAFETY: read-only map of a sealed, write-once file (see above).
        let mmap = unsafe { Mmap::map(&file) }?;
        Ok(Some(Self::Mapped {
            mmap,
            len: expected_halves,
        }))
    }

    /// Number of `u16` halves stored.
    pub fn len(&self) -> usize {
        match self {
            Self::Owned(v) => v.len(),
            Self::Mapped { len, .. } => *len,
        }
    }

    /// `true` when this store holds no halves.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `true` when this store is backed by a memory map rather than a heap
    /// buffer. Exposed for tests/diagnostics — not on any hot path.
    pub fn is_mapped(&self) -> bool {
        matches!(self, Self::Mapped { .. })
    }

    /// Heap bytes this store pins: the full buffer when `Owned`, `0` when
    /// `Mapped` — mapped pages are kernel page cache, reclaimable under
    /// memory pressure, so counting them as resident would make the elastic
    /// memory budget / eviction pipeline behave as if the mmap RSS win never
    /// happened for reloaded segments.
    pub fn resident_bytes(&self) -> usize {
        match self {
            Self::Owned(v) => v.len() * std::mem::size_of::<u16>(),
            Self::Mapped { .. } => 0,
        }
    }

    /// Encode a slice of f16 halves to little-endian bytes, ready to hand to
    /// a page/sidecar writer (`raw_f16.bin`, or the warm tier's
    /// `vectors.mpf` — WS3). Off the query hot path (compaction /
    /// HOT->WARM transition tick only), so the allocation here is fine.
    pub fn le_bytes(halves: &[u16]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(halves.len() * 2);
        for &h in halves {
            buf.extend_from_slice(&h.to_le_bytes());
        }
        buf
    }

    /// Zero-copy view of the sidecar as `&[u16]`.
    pub fn as_slice(&self) -> &[u16] {
        match self {
            Self::Owned(v) => v,
            Self::Mapped { mmap, len } => {
                // `mmap` maps exactly `len * 2` bytes of a file written as
                // `len` little-endian `u16` halves (see
                // `write_segment_files`'s `h.to_le_bytes()` loop in
                // segment_io.rs). Moon's only target architectures
                // (x86_64, aarch64 — see CLAUDE.md "Target Platform") are
                // little-endian, so a native `u16` read reproduces exactly
                // the value the writer encoded; there is no target where
                // this would silently byte-swap. This mirrors the identical
                // `from_raw_parts` reinterpret pattern already used for
                // mmap'd CSR arrays in `crate::graph::csr::mmap`.
                // SAFETY: kernel mappings are page-aligned (satisfies u16's
                // 2-byte alignment); length verified at map time (above).
                unsafe { std::slice::from_raw_parts(mmap.as_ptr().cast::<u16>(), *len) }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_halves(path: &Path, halves: &[u16]) {
        let mut f = File::create(path).unwrap();
        for h in halves {
            f.write_all(&h.to_le_bytes()).unwrap();
        }
        f.sync_all().unwrap();
    }

    #[test]
    fn map_file_roundtrips_halves() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raw_f16.bin");
        let halves: Vec<u16> = (0..256u16).collect();
        write_halves(&path, &halves);

        let store = RawF16Store::map_file(&path, halves.len()).unwrap().unwrap();
        assert!(store.is_mapped());
        assert_eq!(store.len(), halves.len());
        assert_eq!(store.as_slice(), halves.as_slice());
    }

    #[test]
    fn map_file_rejects_size_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raw_f16.bin");
        write_halves(&path, &[1, 2, 3]);

        // Claiming 10 halves (20 bytes) against an actual 6-byte file.
        let store = RawF16Store::map_file(&path, 10).unwrap();
        assert!(store.is_none());
    }

    #[test]
    fn map_file_missing_file_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does-not-exist.bin");
        assert!(RawF16Store::map_file(&path, 4).is_err());
    }

    #[test]
    fn map_file_empty_expected_is_owned_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raw_f16.bin");
        write_halves(&path, &[]);

        let store = RawF16Store::map_file(&path, 0).unwrap().unwrap();
        assert!(!store.is_mapped());
        assert!(store.is_empty());
        assert_eq!(store.as_slice(), &[] as &[u16]);
    }

    #[test]
    fn owned_store_is_not_mapped() {
        let store = RawF16Store::Owned(vec![7, 8, 9]);
        assert!(!store.is_mapped());
        assert_eq!(store.as_slice(), &[7u16, 8, 9]);
    }

    /// Resident accounting: an Owned store pins its full buffer on the heap;
    /// a Mapped store pins nothing (kernel page cache, reclaimable) — the
    /// elastic memory budget / eviction pipeline must see the mmap RSS win,
    /// not pretend the bytes are still resident.
    #[test]
    fn resident_bytes_owned_full_mapped_zero() {
        let owned = RawF16Store::Owned(vec![0u16; 100]);
        assert_eq!(owned.resident_bytes(), 200);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raw_f16.bin");
        let halves: Vec<u16> = (0..100u16).collect();
        write_halves(&path, &halves);
        let mapped = RawF16Store::map_file(&path, halves.len()).unwrap().unwrap();
        assert!(mapped.is_mapped());
        assert_eq!(
            mapped.resident_bytes(),
            0,
            "mapped sidecar pages are page cache, not pinned heap"
        );
    }
}
