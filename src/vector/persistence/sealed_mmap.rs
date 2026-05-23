//! Centralized helper for mmapping sealed warm-segment files.
//!
//! # The seal contract
//!
//! Warm-segment files (`codes.mpf`, `graph.mpf`, `mvcc.mpf`, `vectors.mpf`) are
//! produced by the mutable → warm transition in
//! [`crate::vector::persistence::warm_segment`] and
//! [`crate::storage::tiered::warm_tier`]:
//!
//! 1. A writer builds the file under a temp path.
//! 2. The writer calls `fsync` on the file and its parent directory.
//! 3. The writer atomically renames the temp path to the final name.
//! 4. After the rename completes, **no process or thread in moon ever writes
//!    to, truncates, or unlinks that file while any mmap of it may be live**.
//!    Deletion only happens via segment retirement, which waits on the segment
//!    handle refcount to drop to zero (so all mmaps are already dropped).
//!
//! As long as that contract holds, `memmap2::Mmap` of the file is sound: the
//! backing bytes will not mutate underneath us, so the `&[u8]` view the mmap
//! hands out is effectively immutable for its entire lifetime.
//!
//! **Do not call the raw `memmap2::MmapOptions::new().map(&file)` elsewhere in
//! the warm/sealed paths.** Use [`map_sealed_file`] so the invariant lives in
//! exactly one place and any future audit only has to verify this module.
//!
//! # Breaking the contract
//!
//! If you add code that writes to a sealed file after rename, you must:
//!   - migrate it to write-to-temp + rename, or
//!   - use a mutable segment, not a warm segment, or
//!   - redesign this helper to hand out an explicitly-mutable mapping.
//!
//! There is no safe middle ground: concurrent writes to an mmapped file are
//! undefined behavior in Rust's memory model regardless of the OS semantics.

use std::fs::File;
use std::io;
use std::path::Path;

use memmap2::Mmap;

// ── MADV_DONTNEED helper ──────────────────────────────────────────────────────

/// Advise the kernel that pages in the sealed mmap region are no longer needed.
///
/// This is the low-level primitive for the warm-tier budget enforcer.
/// Calling this releases physical pages backing the mapped region back to the
/// OS page cache while keeping the virtual mapping alive. The kernel will
/// page-fault the data back from the file on the next read, so no data is
/// lost and the `&[u8]` slice remains valid.
///
/// # Platform semantics
///
/// - **Linux**: `MADV_DONTNEED` frees the pages immediately and zeroes them on
///   re-fault (anonymous mappings) or re-reads from the file (file-backed
///   mappings). File-backed sealed mmaps always re-read from disk — correct.
/// - **macOS**: `MADV_FREE_REUSABLE` / `MADV_DONTNEED` are advisory only; the
///   kernel may ignore them. Pages may not be freed. This is acceptable — the
///   call is a best-effort hint and silent no-ops are safe.
///
/// # Safety
///
/// `MADV_DONTNEED` on a sealed read-only file-backed mapping is sound:
/// - The file contents do not change after the rename (seal contract).
/// - The kernel re-reads unchanged file bytes on the next page-fault.
/// - No writes go through this mapping, so no dirty data is discarded.
/// - `mmap` remains valid for the lifetime of the `Mmap` guard; the virtual
///   range is still allocated and future reads succeed.
///
/// This function is `#[cfg(unix)]`; on non-Unix targets it is a no-op stub.
#[cfg(unix)]
pub fn advise_dontneed(mmap: &Mmap) {
    // mmap is a valid, live read-only file-backed mapping produced by
    // map_sealed / map_sealed_file. MADV_DONTNEED on a read-only file-backed
    // region is sound: the kernel pages data back from the unchanged sealed
    // file on the next access.
    // SAFETY: read-only file-backed mapping; no writes lost on DONTNEED.
    unsafe {
        libc::madvise(
            mmap.as_ptr() as *mut libc::c_void,
            mmap.len(),
            libc::MADV_DONTNEED,
        );
    }
}

/// No-op stub on non-Unix platforms (Windows, WASM).
#[cfg(not(unix))]
#[inline]
pub fn advise_dontneed(_mmap: &Mmap) {}

// Make libc available in this module for the unix cfg above.
#[cfg(unix)]
use libc;

/// Open `path` read-only and return a read-only mmap of the full file.
///
/// The returned [`Mmap`] is sound to read for as long as the file adheres to
/// the seal contract documented in the module header. Callers are responsible
/// for ensuring the file belongs to a sealed warm segment — this helper does
/// not (and cannot) verify that at runtime.
///
/// # Errors
///
/// Returns any error from [`File::open`] or [`memmap2::MmapOptions::map`].
#[inline]
pub fn map_sealed_file(path: &Path) -> io::Result<Mmap> {
    let file = File::open(path)?;
    map_sealed(&file)
}

/// Map an already-opened sealed file.
///
/// Prefer [`map_sealed_file`] when you have a path; this variant exists for
/// call sites that already hold a `File` handle (e.g. after `File::open` in a
/// `match` arm that handles `NotFound` specially).
///
/// # Safety contract (caller-enforced)
///
/// `file` must refer to a warm-segment file that satisfies the seal contract.
/// Violating this is undefined behavior.
#[inline]
pub fn map_sealed(file: &File) -> io::Result<Mmap> {
    // The file is a sealed warm-segment: after its producing rename completed,
    // no moon code writes to or truncates it while any mmap may be live.
    // SAFETY: File is sealed (read-only); no concurrent mutation within our process.
    unsafe { memmap2::MmapOptions::new().map(file) }
}
