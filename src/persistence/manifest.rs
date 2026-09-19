//! ShardManifest — dual-root atomic metadata store for shard file tracking.
//!
//! Uses LMDB-style alternating 4KB root pages at offsets 0 and 4096.
//! A single `sync_data()` call is the atomic commit point.
//! CRC32C checksum via MoonPageHeader ensures crash-safe recovery.

use std::collections::HashMap;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::persistence::page::{MOONPAGE_HEADER_SIZE, MoonPageHeader, PAGE_4K, PageType};

/// [`ShardManifest::add_file`] refused an entry because the manifest already
/// lists one with the same `(file_id, file_type)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "manifest already lists file_id {file_id} (type {file_type}); refusing a second \
     entry for it"
)]
pub struct DuplicateFileEntry {
    /// The id that was already listed.
    pub file_id: u64,
    /// The `PageType` discriminant of both entries.
    pub file_type: u8,
}

impl From<DuplicateFileEntry> for std::io::Error {
    fn from(e: DuplicateFileEntry) -> Self {
        std::io::Error::new(std::io::ErrorKind::AlreadyExists, e)
    }
}

/// File lifecycle status within the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FileStatus {
    /// File is active and serving reads.
    Active = 1,
    /// File is being built (not yet readable).
    Building = 2,
    /// File is sealed (immutable, compaction candidate).
    Sealed = 3,
    /// File is undergoing compaction.
    Compacting = 4,
    /// File is logically deleted (physical removal pending).
    Tombstone = 5,
    /// File has been moved to archive storage.
    Archived = 6,
}

impl FileStatus {
    /// Deserialize from a raw byte.
    #[inline]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Active),
            2 => Some(Self::Building),
            3 => Some(Self::Sealed),
            4 => Some(Self::Compacting),
            5 => Some(Self::Tombstone),
            6 => Some(Self::Archived),
            _ => None,
        }
    }
}

/// Storage tier for tiered storage placement.
///
/// Discriminant values match MOONSTORE-V2-COMPREHENSIVE-DESIGN.md §4.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StorageTier {
    /// Data in RAM (file is WAL/snapshot only).
    Hot = 0x01,
    /// File is mmap'd, OS page cache manages residency.
    Warm = 0x02,
    /// File on SSD, accessed via io_uring / direct I/O.
    Cold = 0x03,
    /// Object storage (S3), accessed via HTTP range reads.
    Archive = 0x04,
}

impl StorageTier {
    /// Deserialize from a raw byte.
    #[inline]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::Hot),
            0x02 => Some(Self::Warm),
            0x03 => Some(Self::Cold),
            0x04 => Some(Self::Archive),
            _ => None,
        }
    }
}

/// Fixed-size 56-byte file entry in the shard manifest (format_version=2).
///
/// **v0.2 — PITR/CDC support.** Adds `last_modified_lsn` so recovery can
/// pick the right files for a `--recovery-target-lsn`. Legacy 48-byte (v1)
/// entries are decoded with `last_modified_lsn = created_lsn` (lossless
/// fallback — the file's only known LSN reference point is when it was
/// created).
///
/// Byte layout (all little-endian):
/// ```text
/// Offset  Size  Field
/// 0..8    8     file_id (u64 LE)
/// 8       1     file_type (PageType discriminant)
/// 9       1     status (FileStatus as u8)
/// 10      1     tier (StorageTier as u8)
/// 11      1     page_size_log2 (e.g. 12 for 4KB, 16 for 64KB)
/// 12..16  4     page_count (u32 LE)
/// 16..24  8     byte_size (u64 LE)
/// 24..32  8     created_lsn (u64 LE)
/// 32..40  8     db_index (u64 LE; formerly min_key_hash, always written 0 — see field doc)
/// 40..48  8     max_key_hash (u64 LE)
/// 48..56  8     last_modified_lsn (u64 LE)  -- v2 only
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    pub file_id: u64,
    pub file_type: u8,
    pub status: FileStatus,
    pub tier: StorageTier,
    pub page_size_log2: u8,
    pub page_count: u32,
    pub byte_size: u64,
    pub created_lsn: u64,
    /// Logical database index every entry in this file belongs to (#139:
    /// cold recovery must re-attach spilled keys to the database they were
    /// evicted from, not unconditionally db 0).
    ///
    /// Byte-compatible repurpose of the former `min_key_hash` field, which
    /// every writer in the codebase's history serialized as 0 — so v1/v2
    /// manifests written before this field existed read back as db 0,
    /// which is exactly correct for them (the spill buffer was db-blind
    /// and recovery attached everything to db 0). The spill path
    /// guarantees single-db files by cutting flush chunks at db
    /// boundaries (`spill_thread::flush_buffer`).
    pub db_index: u64,
    pub max_key_hash: u64,
    /// LSN of the last mutation to this file (v2). For files imported from
    /// a v1 manifest this is set equal to `created_lsn`. Used by PITR to
    /// select the file set valid at `--recovery-target-lsn`.
    pub last_modified_lsn: u64,
}

impl FileEntry {
    /// On-disk size of a v2 FileEntry (with `last_modified_lsn`).
    pub const SIZE: usize = 56;

    /// On-disk size of a legacy v1 FileEntry (without `last_modified_lsn`).
    pub const SIZE_V1: usize = 48;

    /// Serialize this entry as v2 (56 bytes) into `buf` (must be >= 56 bytes).
    ///
    /// # Panics
    ///
    /// Panics if `buf.len() < 56`.
    pub fn write_to(&self, buf: &mut [u8]) {
        assert!(
            buf.len() >= Self::SIZE,
            "buffer too small for FileEntry: {} < {}",
            buf.len(),
            Self::SIZE,
        );

        buf[0..8].copy_from_slice(&self.file_id.to_le_bytes());
        buf[8] = self.file_type;
        buf[9] = self.status as u8;
        buf[10] = self.tier as u8;
        buf[11] = self.page_size_log2;
        buf[12..16].copy_from_slice(&self.page_count.to_le_bytes());
        buf[16..24].copy_from_slice(&self.byte_size.to_le_bytes());
        buf[24..32].copy_from_slice(&self.created_lsn.to_le_bytes());
        buf[32..40].copy_from_slice(&self.db_index.to_le_bytes());
        buf[40..48].copy_from_slice(&self.max_key_hash.to_le_bytes());
        buf[48..56].copy_from_slice(&self.last_modified_lsn.to_le_bytes());
    }

    /// Deserialize a v2 FileEntry (56 bytes) from `buf`.
    ///
    /// Returns `None` if `buf.len() < 56`.
    pub fn read_from(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }

        let mut entry = Self::read_v1(buf)?;
        entry.last_modified_lsn = u64::from_le_bytes([
            buf[48], buf[49], buf[50], buf[51], buf[52], buf[53], buf[54], buf[55],
        ]);
        Some(entry)
    }

    /// Deserialize a legacy v1 FileEntry (48 bytes) from `buf`.
    ///
    /// `last_modified_lsn` is synthesized as `created_lsn` — the only known
    /// LSN reference point for files written under the v1 schema.
    pub fn read_v1(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE_V1 {
            return None;
        }

        let file_id = u64::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        let file_type = buf[8];
        let status = FileStatus::from_u8(buf[9])?;
        let tier = StorageTier::from_u8(buf[10])?;
        let page_size_log2 = buf[11];
        let page_count = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
        let byte_size = u64::from_le_bytes([
            buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
        ]);
        let created_lsn = u64::from_le_bytes([
            buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31],
        ]);
        let db_index = u64::from_le_bytes([
            buf[32], buf[33], buf[34], buf[35], buf[36], buf[37], buf[38], buf[39],
        ]);
        let max_key_hash = u64::from_le_bytes([
            buf[40], buf[41], buf[42], buf[43], buf[44], buf[45], buf[46], buf[47],
        ]);

        Some(Self {
            file_id,
            file_type,
            status,
            tier,
            page_size_log2,
            page_count,
            byte_size,
            created_lsn,
            db_index,
            max_key_hash,
            // v1 fallback — synthesize from created_lsn so existing manifests
            // remain readable and PITR target_lsn comparisons stay sane.
            last_modified_lsn: created_lsn,
        })
    }
}

/// Offset of Root A page within the manifest file.
const ROOT_A_OFFSET: u64 = 0;

/// Offset of Root B page within the manifest file.
const ROOT_B_OFFSET: u64 = PAGE_4K as u64;

/// Payload starts after 64-byte MoonPageHeader.
/// Layout per §4.2: epoch(8) + redo_lsn(8) + wal_flush_lsn(8) + file_count(4) +
/// entry_page_count(4) + snapshot_lsn(8) + created_at(8) + shard_uuid(16) = 64 bytes,
/// then file_count * FileEntry::SIZE bytes of FileEntry records.
const ROOT_META_SIZE: usize = 64;

/// Manifest format version embedded in `MoonPageHeader.format_version`.
/// - 1 = legacy 48-byte FileEntry layout (read-only fallback).
/// - 2 = current 56-byte layout with `last_modified_lsn` for PITR.
pub const MANIFEST_FORMAT_V1: u8 = 1;
pub const MANIFEST_FORMAT_V2: u8 = 2;

/// Maximum inline FileEntry records per root page (v2 layout).
/// (4096 - 64 header - 64 meta) / 56 = 70.
pub const MAX_INLINE_ENTRIES: usize =
    (PAGE_4K - MOONPAGE_HEADER_SIZE - ROOT_META_SIZE) / FileEntry::SIZE;

/// FileEntry records per overflow (`ManifestEntry`) page: a full 4 KB page
/// minus the 64-byte MoonPageHeader, no per-page meta. (4096 - 64) / 56 = 72.
/// Entries beyond `MAX_INLINE_ENTRIES` spill into append-only overflow pages.
pub const ENTRIES_PER_OVERFLOW_PAGE: usize = (PAGE_4K - MOONPAGE_HEADER_SIZE) / FileEntry::SIZE;

/// In-memory representation of one manifest root page.
///
/// Fields match MOONSTORE-V2-COMPREHENSIVE-DESIGN.md §4.2.
#[derive(Debug, Clone)]
pub struct ManifestRoot {
    /// Monotonically increasing epoch (commit counter).
    pub epoch: u64,
    /// WAL REDO point from last checkpoint.
    pub redo_lsn: u64,
    /// Highest durable WAL LSN.
    pub wal_flush_lsn: u64,
    /// Number of file entries.
    pub file_count: u32,
    /// Number of overflow ManifestEntry pages.
    pub entry_page_count: u32,
    /// LSN of latest completed snapshot.
    pub snapshot_lsn: u64,
    /// Unix timestamp (seconds).
    pub created_at: u64,
    /// Unique shard identifier (must match control file).
    pub shard_uuid: [u8; 16],
    /// File entries tracked by this root.
    pub entries: Vec<FileEntry>,
}

/// Dual-root atomic manifest for tracking shard files.
///
/// Uses LMDB-style alternating root pages: writes go to the inactive
/// slot, and a single `sync_data()` is the atomic commit point.
///
/// ## Tombstone GC
///
/// `FileEntry` has no on-disk field for when a tombstone was created (the v2
/// layout is frozen; P6 owns format v3). Tombstone metadata is therefore kept
/// in an in-memory side table (`tombstone_registry`) keyed by
/// `(file_id, file_type)`.
///
/// On `open()`, all tombstoned entries are seeded with `(current_epoch,
/// Instant::now())` — a conservative re-clocking that is safe because a
/// process restart implies no in-flight readers holding old snapshot views.
/// After the configured retention period both tombstone entries are
/// physically removed from `active_root.entries` by `gc_tombstones`.
/// Test-only fault-injection and observation knobs for [`ManifestIo::persist`],
/// scoped to **one** [`ShardManifest`].
///
/// These were three process-global statics. Rust runs a crate's unit tests
/// concurrently in a single process, so a global that `persist()` reads is a
/// channel between unrelated tests: while one test held the injected-error flag
/// true, any other test's `commit()` failed with `"injected persist failure
/// (test)"` — moon#750, observed once in `ci-local` as a failure of
/// `test_overflow_compaction_bounds_growth`, a test that has nothing to do with
/// fault injection. `TEST_AGENT_REGISTRY_LOCK` did not help: it serialises the
/// tests that *set* the knobs against each other, while every test that merely
/// commits a manifest is an unguarded *reader*.
///
/// Owning the knobs per-manifest removes the channel by construction, so a test
/// added later cannot re-open the race by forgetting to take a lock.
///
/// The fields are `Arc<Atomic…>` rather than plain values because
/// [`ShardManifest::enable_deferred_sync`] moves the [`ManifestIo`] onto the
/// per-shard sync thread: the test still has to arm the knobs and read the
/// count while the io lives over there. `ShardManifest` keeps a clone, so the
/// handle stays valid across backend swaps.
#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub(crate) struct TestKnobs {
    /// Injected delay inside `persist`, mirroring the
    /// `cold_read::TEST_INJECT_DELAY_MS` pattern: simulates a slow/contended
    /// device so tests can prove which thread pays for the sync.
    pub(crate) sync_delay_ms: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// Count of `persist` executions on this manifest (for coalescing
    /// assertions). Per-manifest, so it needs no before/after delta.
    pub(crate) persist_count: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// When true, `persist` fails with an injected I/O error (simulating
    /// ENOSPC/EIO on the manifest device) so the sync agent's failure latch
    /// can be exercised.
    pub(crate) inject_persist_error: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

/// Serializes tests that touch the **process-global** manifest-sync agent
/// registry: `manifest_sync::AGENT_REGISTRY` and the `flush_all_agents()`
/// barrier that walks it. A concurrently-registered agent from another test
/// changes the registry length and can fail (or heal) the barrier, so any test
/// asserting on either must hold this.
///
/// It deliberately does NOT cover fault injection any more — that is per
/// manifest now (see [`TestKnobs`]). A lock only helps when every participant
/// takes it, and the readers here (`commit()`) never did.
#[cfg(test)]
pub(crate) static TEST_AGENT_REGISTRY_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// The file-I/O half of the manifest: the handle, the dual-slot alternation
/// state, and the reopen bookkeeping that `persist`/`compact` maintain.
///
/// Owned inline by [`ShardManifest`] (synchronous commits on the caller
/// thread — the default everywhere outside the shard event loop), or moved
/// onto the per-shard manifest-sync thread by
/// [`ShardManifest::enable_deferred_sync`] so commit fsyncs never run on the
/// shard event loop (task #59).
#[derive(Debug)]
pub(crate) struct ManifestIo {
    /// File handle opened for read/write.
    file: std::fs::File,
    /// Path to the manifest file on disk.
    path: PathBuf,
    /// Which slot is currently active: 0 = Root A (offset 0), 1 = Root B (offset 4096).
    active_slot: u8,
    /// Set when `compact()` rewrote+renamed the manifest durably but then failed
    /// to reopen `self.file` against the new inode. The old handle now refers to
    /// the pre-rename (orphaned) inode, so writing through it would silently
    /// discard every subsequent commit. While set, `persist()` reattaches to
    /// `self.path` BEFORE writing (or fails loudly if that reopen also fails).
    needs_reopen: bool,
    /// Test-only fault injection: when true, `compact()` simulates the
    /// "rename succeeded, reopen failed" race by pointing `self.file` at a
    /// throwaway inode and returning an error, exercising the `needs_reopen`
    /// recovery path without needing real fd/permission failures.
    #[cfg(test)]
    fail_compact_reopen: bool,
    /// Test-only fault-injection/observation knobs, shared with the owning
    /// [`ShardManifest`] so they survive this io moving to the sync thread.
    #[cfg(test)]
    knobs: TestKnobs,
}

/// Where a [`ShardManifest`]'s file I/O runs.
#[derive(Debug)]
enum IoBackend {
    /// Commits persist synchronously on the calling thread (the historical
    /// behavior; used by recovery, tests, and every non-shard-loop caller).
    Inline(ManifestIo),
    /// Commits are shipped to the per-shard manifest-sync thread; deferred
    /// commits return immediately, durable commits block on an ack.
    Deferred(crate::persistence::manifest_sync::ManifestSyncAgent),
    /// Transient placeholder during backend swaps, and the terminal state if
    /// the sync thread died holding the file handle. Commits fail loudly.
    Poisoned,
}

#[derive(Debug)]
pub struct ShardManifest {
    /// File-I/O backend (see [`IoBackend`]).
    io: IoBackend,
    /// Path to the manifest file on disk.
    path: PathBuf,
    /// Currently active root (the last successfully committed state).
    active_root: ManifestRoot,
    /// In-memory registry of tombstoned files: (file_id, file_type) →
    /// (tombstone_epoch, tombstoned_at). Keyed by type as well as id for the
    /// same reason `remove_file` matches on both (moon#893): two artifacts of
    /// different kinds can share an id, and each tombstone ages on its own.
    ///
    /// `tombstone_epoch` is the `active_root.epoch` at the moment `remove_file` was called
    /// (i.e. the epoch of the root that will be committed next, which equals current + 1
    /// after the commit flip — we record the pre-commit value so age = current - tombstone_epoch).
    /// `tombstoned_at` is a monotonic `Instant` for wall-clock retention.
    tombstone_registry: HashMap<(u64, u8), (u64, Instant)>,
    /// Test-only knob handles, shared with this manifest's [`ManifestIo`]. Held
    /// here as well as there so a test can arm injection and read the persist
    /// count after `enable_deferred_sync` has moved the io to the sync thread.
    #[cfg(test)]
    knobs: TestKnobs,
}

/// Write a brand-new manifest image so a crash can never leave a prefix of it
/// at `path`: temp file, fsync, rename, fsync the directory.
///
/// `create` used to `std::fs::write` straight to `path`. A SIGKILL or ENOSPC
/// part-way left a file shorter than the two root pages, which `open` rejects
/// on every later boot. The temp name is fixed per manifest, so a leftover
/// from a crash is simply truncated by the next create.
fn write_new_manifest_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let Some(name) = path.file_name() else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("manifest path has no file name: {}", path.display()),
        ));
    };
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let tmp = parent.join(format!(".{}.creating", name.to_string_lossy()));
    {
        let mut f = std::fs::File::create(&tmp)?;
        #[cfg(test)]
        if let Some(n) = torn_create_knob::take() {
            // Simulated crash after `n` bytes reached the file.
            f.write_all(&bytes[..n.min(bytes.len())])?;
            return Err(std::io::Error::other("injected crash mid-create"));
        }
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    crate::persistence::fsync::fsync_directory(parent)
}

/// Test-only: make the next `create` on THIS thread die after `n` bytes.
#[cfg(test)]
pub(crate) mod torn_create_knob {
    use std::cell::Cell;

    thread_local! {
        static AFTER: Cell<Option<usize>> = const { Cell::new(None) };
    }

    pub(crate) fn arm(after_bytes: usize) {
        AFTER.with(|c| c.set(Some(after_bytes)));
    }

    pub(super) fn take() -> Option<usize> {
        AFTER.with(Cell::take)
    }
}

impl ShardManifest {
    /// Create a new manifest file with an empty Root A at epoch 1.
    ///
    /// The file will be exactly 8192 bytes (two 4KB root pages).
    pub fn create(path: &Path) -> std::io::Result<Self> {
        let mut buf = vec![0u8; 2 * PAGE_4K];

        // Build Root A at offset 0 with epoch=1, file_count=0
        let root = ManifestRoot {
            epoch: 1,
            redo_lsn: 0,
            wal_flush_lsn: 0,
            file_count: 0,
            entry_page_count: 0,
            snapshot_lsn: 0,
            created_at: 0,
            shard_uuid: [0u8; 16],
            entries: Vec::new(),
        };
        Self::serialize_root(&root, 0, &mut buf[..PAGE_4K]);

        // Temp + fsync + rename + dir fsync: `path` either does not exist or
        // holds both root pages, never a prefix of them (see
        // `write_new_manifest_file`).
        write_new_manifest_file(path, &buf)?;

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;

        #[cfg(test)]
        let knobs = TestKnobs::default();
        Ok(Self {
            io: IoBackend::Inline(ManifestIo {
                file,
                path: path.to_path_buf(),
                active_slot: 0,
                needs_reopen: false,
                #[cfg(test)]
                fail_compact_reopen: false,
                #[cfg(test)]
                knobs: knobs.clone(),
            }),
            path: path.to_path_buf(),
            active_root: root,
            tombstone_registry: HashMap::new(),
            #[cfg(test)]
            knobs,
        })
    }

    /// Whether the manifest file at `path` is a torn [`Self::create`]: shorter
    /// than the two root pages every manifest starts with.
    ///
    /// Nothing but an interrupted create can produce one — commits rewrite a
    /// root page in place inside those first `2 * PAGE_4K` bytes and
    /// compaction replaces the file by rename — so such a file holds no
    /// committed entry and references no cold file. Builds from before create
    /// became atomic wrote it in place, so data dirs in the field can hold one.
    pub fn is_torn_create(path: &Path) -> std::io::Result<bool> {
        Ok(std::fs::metadata(path)?.len() < (2 * PAGE_4K) as u64)
    }

    /// [`Self::open`], except that a torn create ([`Self::is_torn_create`]) is
    /// replaced by a fresh empty manifest, with a WARN naming the file, instead
    /// of failing on every boot. A manifest of normal length whose roots are
    /// both corrupt still fails: that one DID hold entries.
    pub fn open_repairing_torn_create(path: &Path) -> std::io::Result<Self> {
        if Self::is_torn_create(path)? {
            tracing::warn!(
                path = %path.display(),
                "shard manifest is shorter than its two root pages: an interrupted \
                 create that committed no entry and references no cold file - \
                 re-creating it empty (the torn file is safe to remove)"
            );
            return Self::create(path);
        }
        Self::open(path)
    }

    /// Open an existing manifest file and recover the latest valid root.
    ///
    /// Reads both root pages, validates CRC32C, and picks the one with
    /// the higher epoch. If both are corrupted, returns an error.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let buf = std::fs::read(path)?;
        if buf.len() < 2 * PAGE_4K {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "manifest file too small: {} bytes, expected at least {}",
                    buf.len(),
                    2 * PAGE_4K,
                ),
            ));
        }

        // Load each slot fully (inline root + its append-only overflow run). A
        // slot whose overflow is torn/short/corrupt returns None so selection
        // falls back to the other (last-good) slot — never surfacing garbage.
        let root_a = Self::load_root(&buf, 0);
        let root_b = Self::load_root(&buf, PAGE_4K);

        let (active_root, active_slot) = match (root_a, root_b) {
            (Some(a), Some(b)) => {
                if b.epoch >= a.epoch {
                    (b, 1u8)
                } else {
                    (a, 0u8)
                }
            }
            (Some(a), None) => (a, 0),
            (None, Some(b)) => (b, 1),
            (None, None) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "both manifest root pages are corrupted",
                ));
            }
        };

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;

        // Seed tombstone_registry for all tombstoned entries found on disk.
        // Conservative re-clocking: use current epoch and Instant::now() so
        // retention timers start from process restart, not original tombstone time.
        // Safe because a restart implies no in-flight readers hold old snapshot views.
        let now = Instant::now();
        let current_epoch = active_root.epoch;
        let mut tombstone_registry = HashMap::new();
        for entry in &active_root.entries {
            if entry.status == FileStatus::Tombstone {
                tombstone_registry.insert((entry.file_id, entry.file_type), (current_epoch, now));
            }
        }

        #[cfg(test)]
        let knobs = TestKnobs::default();
        Ok(Self {
            io: IoBackend::Inline(ManifestIo {
                file,
                path: path.to_path_buf(),
                active_slot,
                needs_reopen: false,
                #[cfg(test)]
                fail_compact_reopen: false,
                #[cfg(test)]
                knobs: knobs.clone(),
            }),
            path: path.to_path_buf(),
            active_root,
            tombstone_registry,
            #[cfg(test)]
            knobs,
        })
    }

    /// Commit the current state to the inactive root page.
    ///
    /// 1. Increment epoch
    /// 2. Serialize to the inactive slot
    /// 3. `sync_data()` — this is the atomic commit point
    /// 4. Flip active_slot
    pub fn commit(&mut self) -> std::io::Result<()> {
        let snapshot = self.snapshot_for_commit();
        match &mut self.io {
            IoBackend::Inline(io) => io.persist(snapshot),
            IoBackend::Deferred(agent) => agent.commit_durable(snapshot),
            IoBackend::Poisoned => Err(std::io::Error::other(
                "manifest io backend lost (sync thread died holding the handle)",
            )),
        }
    }

    /// Commit without waiting for durability: the snapshot is handed to the
    /// manifest-sync thread and this returns immediately (task #59). Falls
    /// back to a synchronous inline persist when deferred sync is not
    /// enabled, so semantics degrade to `commit()` — never to a silent no-op.
    ///
    /// ONLY correct for callers whose durability backstop is elsewhere (the
    /// async-spill completion path runs exclusively under `--appendonly yes`,
    /// where AOF replay + the orphan sweep reconstruct anything a lost
    /// manifest commit would have recorded). Paths where the manifest IS the
    /// durability record must call `commit()`.
    pub fn commit_deferred(&mut self) -> std::io::Result<()> {
        let snapshot = self.snapshot_for_commit();
        match &mut self.io {
            IoBackend::Inline(io) => io.persist(snapshot),
            IoBackend::Deferred(agent) => agent.commit_deferred(snapshot),
            IoBackend::Poisoned => Err(std::io::Error::other(
                "manifest io backend lost (sync thread died holding the handle)",
            )),
        }
    }

    /// Advance the epoch and clone the root for a commit. Each snapshot is a
    /// COMPLETE manifest state: the sync agent may coalesce a run of queued
    /// snapshots down to the newest one with no loss.
    fn snapshot_for_commit(&mut self) -> ManifestRoot {
        self.active_root.epoch += 1;
        self.active_root.file_count = self.active_root.entries.len() as u32;
        self.active_root.clone()
    }

    /// Move the file-I/O half onto a dedicated `manifest-sync-{shard_id}`
    /// thread. From here on, `commit()` blocks on an ack from that thread
    /// (unchanged durability) while `commit_deferred()` returns immediately.
    /// Idempotent.
    pub fn enable_deferred_sync(&mut self, shard_id: usize) {
        let cur = std::mem::replace(&mut self.io, IoBackend::Poisoned);
        self.io = match cur {
            IoBackend::Inline(io) => {
                match crate::persistence::manifest_sync::ManifestSyncAgent::spawn(io, shard_id) {
                    Ok(agent) => IoBackend::Deferred(agent),
                    // Degrade, don't destroy: a failed thread spawn keeps the
                    // working inline backend (commits fsync on the shard
                    // thread again — slow, but durable and loud about it).
                    Err((io, e)) => {
                        tracing::error!(
                            shard_id, error = %e,
                            "manifest-sync: thread spawn failed — staying inline \
                             (manifest commits fsync on the shard thread)"
                        );
                        IoBackend::Inline(io)
                    }
                }
            }
            other => other,
        };
    }

    /// Flush every pending deferred commit, stop the sync thread, and take
    /// the file handle back inline so later commits (e.g. during shutdown
    /// finalization) persist synchronously again. Idempotent.
    pub fn shutdown_deferred(&mut self) {
        let cur = std::mem::replace(&mut self.io, IoBackend::Poisoned);
        self.io = match cur {
            IoBackend::Deferred(agent) => match agent.shutdown() {
                Some(io) => IoBackend::Inline(io),
                None => {
                    tracing::error!(
                        "manifest-sync thread lost the manifest file handle; \
                         further commits on this shard will fail"
                    );
                    IoBackend::Poisoned
                }
            },
            other => other,
        };
    }

    /// Add a file entry to the manifest (in-memory only until commit).
    ///
    /// # Errors
    ///
    /// Refuses, leaving the manifest untouched, when an entry with the same
    /// `(file_id, file_type)` is already listed in ANY status (moon#893). Two
    /// entries for one `(id, type)` name one path, so the second can only mean
    /// the id was re-issued: the new artifact did not land where the manifest
    /// would say it did. Appending anyway is how one warm segment id collected
    /// several Active entries, and recovery then walked the directory twice and
    /// deleted it on the second pass. A tombstoned entry counts too — its file
    /// may still be on disk inside the tombstone retention window.
    ///
    /// Rejecting rather than replacing is deliberate: the existing entry is the
    /// one that describes what is on disk, and replacing it would, for a spill
    /// file, re-attribute its keys to the new entry's `db_index`.
    pub fn add_file(&mut self, entry: FileEntry) -> Result<(), DuplicateFileEntry> {
        if self.has_entry(entry.file_id, entry.file_type) {
            tracing::error!(
                file_id = entry.file_id,
                file_type = entry.file_type,
                "manifest: refusing a second entry for a file id already listed \
                 (the id was re-issued; the manifest keeps the entry it has)"
            );
            return Err(DuplicateFileEntry {
                file_id: entry.file_id,
                file_type: entry.file_type,
            });
        }
        self.active_root.entries.push(entry);
        Ok(())
    }

    /// Whether an entry with this `(file_id, file_type)` is listed, in any
    /// status — exactly the condition under which [`Self::add_file`] refuses.
    pub fn has_entry(&self, file_id: u64, file_type: u8) -> bool {
        self.active_root
            .entries
            .iter()
            .any(|e| e.file_id == file_id && e.file_type == file_type)
    }

    /// Collapse duplicate Active entries — several entries with one
    /// `(file_id, file_type)` — down to the LAST of them, and return how many
    /// were dropped (in-memory only until commit).
    ///
    /// Heals manifests written by builds before moon#893, whose `add_file`
    /// appended without a check while the id counter re-issued live ids. The
    /// last entry is the one kept because it describes what is on disk: an
    /// older build renamed its newer spill batch over the file, and a warm
    /// segment's directory is found by id alone. Tombstoned entries are left
    /// as they are; they never reach a reader.
    pub fn dedupe_active_entries(&mut self) -> usize {
        let entries = &mut self.active_root.entries;
        let mut last: HashMap<(u64, u8), usize> = HashMap::new();
        for (i, e) in entries.iter().enumerate() {
            if e.status == FileStatus::Active {
                last.insert((e.file_id, e.file_type), i);
            }
        }
        let active = entries
            .iter()
            .filter(|e| e.status == FileStatus::Active)
            .count();
        if last.len() == active {
            return 0;
        }
        let before = entries.len();
        let mut i = 0usize;
        entries.retain(|e| {
            let keep =
                e.status != FileStatus::Active || last.get(&(e.file_id, e.file_type)) == Some(&i);
            i += 1;
            keep
        });
        before - entries.len()
    }

    /// Append `entry` with NO duplicate check — the append semantics of the
    /// builds before moon#893, kept only so tests can reproduce a manifest an
    /// older build already wrote.
    #[cfg(test)]
    pub(crate) fn push_entry_unchecked(&mut self, entry: FileEntry) {
        self.active_root.entries.push(entry);
    }

    /// Mark the `file_type` file with `file_id` as Tombstone (in-memory only
    /// until commit).
    ///
    /// Matches on `(file_id, file_type)`, never on the id alone (moon#893).
    /// A KV spill file (`KvLeaf`, `data/heap-{id}.mpf`) and a warm vector
    /// segment (`VecCodes`, `vectors/segment-{id}/`) are different artifacts,
    /// and a manifest written before the file_id seed covered every artifact
    /// kind can hold one of each under the same id. Retiring a vanished
    /// segment's entry by id alone tombstoned the live spill file beside it,
    /// and every key in it read as absent after the next restart.
    ///
    /// Records the tombstone in the in-memory registry with the current epoch
    /// and wall-clock instant so `gc_tombstones` can enforce two-axis retention.
    /// The epoch recorded is the pre-commit epoch; after `commit()` the active
    /// epoch is incremented by 1, so tombstone age in epochs = current_epoch - tombstone_epoch.
    pub fn remove_file(&mut self, file_id: u64, file_type: PageType) {
        let file_type = file_type as u8;
        for entry in &mut self.active_root.entries {
            if entry.file_id == file_id && entry.file_type == file_type {
                entry.status = FileStatus::Tombstone;
                // Register tombstone with current epoch and monotonic clock.
                // Use entry() to avoid overwriting an existing registry entry
                // if remove_file is called twice for the same file_id.
                self.tombstone_registry
                    .entry((file_id, file_type))
                    .or_insert_with(|| (self.active_root.epoch, Instant::now()));
            }
        }
    }

    /// Physically remove tombstoned entries that satisfy BOTH retention axes.
    ///
    /// ## Two-axis retention
    ///
    /// An entry is eligible for physical removal only when **both** conditions hold:
    ///
    /// - **Epoch axis**: `current_epoch - tombstone_epoch >= retain_epochs`
    ///   Guards against pruning files that a snapshot reader opened before the
    ///   tombstone was recorded; each committed epoch is a new snapshot generation.
    ///
    /// - **Time axis**: elapsed wall-clock time since tombstone >= `retain_secs`
    ///   Guards against pruning files that a long-running reader holds open;
    ///   `retain_secs` must be ≥ the longest expected reader snapshot age.
    ///
    /// ## Crash safety
    ///
    /// This method is in-memory only — it does **not** commit. The caller must
    /// call `commit()` after GC to persist the pruned state through the dual-root
    /// atomic swap. A crash before commit leaves the on-disk root untouched; GC
    /// will re-evaluate the same tombstones on the next invocation (idempotent).
    ///
    /// ## Parameters
    ///
    /// - `retain_epochs`: minimum epoch age before a tombstone is eligible.
    ///   Default recommendation: `2` (two manifest commit generations).
    /// - `retain_secs`: minimum wall-clock age in seconds.
    ///   Default recommendation: `300` (5 minutes).
    /// - `now`: monotonic clock instant for the time axis check. Pass
    ///   `Instant::now()` in production; inject a future instant in tests.
    ///
    /// ## Returns
    ///
    /// Count of entries physically removed from `active_root.entries`.
    pub fn gc_tombstones(&mut self, retain_epochs: u64, retain_secs: u64, now: Instant) -> usize {
        let current_epoch = self.active_root.epoch;
        let mut pruned = 0usize;

        self.active_root.entries.retain(|entry| {
            if entry.status != FileStatus::Tombstone {
                return true; // keep all non-tombstone entries
            }
            let Some(&(tombstone_epoch, tombstoned_at)) = self
                .tombstone_registry
                .get(&(entry.file_id, entry.file_type))
            else {
                // No registry entry — conservatively retain (should not happen in
                // normal operation; seeded on open() and written in remove_file()).
                return true;
            };

            let epoch_age = current_epoch.saturating_sub(tombstone_epoch);
            let time_age_secs = now.saturating_duration_since(tombstoned_at).as_secs();

            let epoch_ok = epoch_age >= retain_epochs;
            let time_ok = time_age_secs >= retain_secs;

            if epoch_ok && time_ok {
                pruned += 1;
                false // remove this entry
            } else {
                true // retain
            }
        });

        // Clean up registry entries for pruned tombstones.
        if pruned > 0 {
            let live: std::collections::HashSet<(u64, u8)> = self
                .active_root
                .entries
                .iter()
                .map(|e| (e.file_id, e.file_type))
                .collect();
            self.tombstone_registry.retain(|key, _| live.contains(key));
        }

        pruned
    }

    /// Return the count of non-tombstone entries in the active root.
    ///
    /// Used by P10 to populate `reclamation_manifest_active` in INFO output.
    pub fn active_entry_count(&self) -> usize {
        self.active_root
            .entries
            .iter()
            .filter(|e| e.status != FileStatus::Tombstone)
            .count()
    }

    /// Return the count of tombstone entries in the active root.
    ///
    /// Used by P10 to populate `reclamation_manifest_tombstones` in INFO output.
    pub fn tombstone_count(&self) -> usize {
        self.active_root
            .entries
            .iter()
            .filter(|e| e.status == FileStatus::Tombstone)
            .count()
    }

    /// Update a file entry in-place (in-memory only until commit).
    pub fn update_file(&mut self, file_id: u64, f: impl FnOnce(&mut FileEntry)) {
        for entry in &mut self.active_root.entries {
            if entry.file_id == file_id {
                f(entry);
                return;
            }
        }
    }

    /// Return a reference to the active file entries.
    pub fn files(&self) -> &[FileEntry] {
        &self.active_root.entries
    }

    /// Return the current epoch.
    pub fn epoch(&self) -> u64 {
        self.active_root.epoch
    }

    /// Return the currently active slot (0 = Root A, 1 = Root B).
    ///
    /// Only meaningful while the io backend is inline (tests, recovery); the
    /// slot lives on the sync thread once deferred sync is enabled.
    pub fn active_slot(&self) -> u8 {
        match &self.io {
            IoBackend::Inline(io) => io.active_slot,
            IoBackend::Deferred(_) | IoBackend::Poisoned => 0,
        }
    }

    /// Test-only: arm/disarm the compact-reopen fault injection on the inline io.
    #[cfg(test)]
    pub(crate) fn set_fail_compact_reopen(&mut self, armed: bool) {
        if let IoBackend::Inline(io) = &mut self.io {
            io.fail_compact_reopen = armed;
        }
    }

    /// Test-only: arm/disarm this manifest's persist fault injection.
    ///
    /// Scoped to `self` (moon#750) — arming it here cannot fail another
    /// manifest's commit, including one running concurrently in another test.
    /// Works in both io backends: the knob handle is shared with the
    /// [`ManifestIo`], wherever that currently lives.
    #[cfg(test)]
    pub(crate) fn set_inject_persist_error(&mut self, armed: bool) {
        self.knobs
            .inject_persist_error
            .store(armed, std::sync::atomic::Ordering::SeqCst);
    }

    /// Test-only: set this manifest's injected per-persist delay, in ms.
    #[cfg(test)]
    pub(crate) fn set_inject_sync_delay_ms(&mut self, ms: u64) {
        self.knobs
            .sync_delay_ms
            .store(ms, std::sync::atomic::Ordering::SeqCst);
    }

    /// Test-only: how many times this manifest's io has run `persist`.
    #[cfg(test)]
    pub(crate) fn persist_count(&self) -> u64 {
        self.knobs
            .persist_count
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Test-only: whether the inline io is flagged for reattach.
    #[cfg(test)]
    pub(crate) fn needs_reopen(&self) -> bool {
        matches!(&self.io, IoBackend::Inline(io) if io.needs_reopen)
    }

    /// Test-only: run a compaction of the current state through the inline io.
    #[cfg(test)]
    pub(crate) fn compact_for_test(&mut self) -> std::io::Result<()> {
        let mut root = self.active_root.clone();
        match &mut self.io {
            IoBackend::Inline(io) => io.compact(&mut root),
            _ => Err(std::io::Error::other("compact_for_test requires inline io")),
        }
    }

    /// Return the path to the manifest file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Serialize a ManifestRoot's INLINE portion into a 4KB root page (v2).
    ///
    /// Only the first `min(entries.len(), MAX_INLINE_ENTRIES)` records live in
    /// the root page; any surplus lives in append-only overflow pages written
    /// by `commit`. `overflow_start_page` is the file-relative page index where
    /// this root's overflow run begins (0 when none) and is stamped into the
    /// header `next_page` field; `entry_page_count` (root meta) records the run
    /// length. `file_count` in the meta is the TOTAL across inline + overflow.
    ///
    /// Layout: epoch(8) + redo_lsn(8) + wal_flush_lsn(8) + file_count(4) +
    /// entry_page_count(4) + snapshot_lsn(8) + created_at(8) + shard_uuid(16) =
    /// 64 bytes, then inline_count * 56-byte FileEntry records.
    fn serialize_root(root: &ManifestRoot, overflow_start_page: u32, page: &mut [u8]) {
        assert!(page.len() >= PAGE_4K);

        // Zero the page
        page[..PAGE_4K].fill(0);

        let total = root.entries.len();
        let inline_count = total.min(MAX_INLINE_ENTRIES);

        // Payload framing covers ONLY the inline entries — the region this
        // page's CRC32C authenticates. Overflow pages carry their own CRC.
        let payload_bytes = ROOT_META_SIZE + inline_count * FileEntry::SIZE;

        // Header — stamp v2 format so readers know to expect 56-byte entries.
        let mut hdr = MoonPageHeader::new(PageType::ManifestRoot, 0, 0);
        hdr.format_version = MANIFEST_FORMAT_V2;
        hdr.payload_bytes = payload_bytes as u32;
        hdr.entry_count = inline_count as u32;
        hdr.next_page = overflow_start_page; // 0 when no overflow run
        hdr.write_to(page);

        // Manifest-specific metadata after header (64 bytes)
        let p = MOONPAGE_HEADER_SIZE;
        page[p..p + 8].copy_from_slice(&root.epoch.to_le_bytes());
        page[p + 8..p + 16].copy_from_slice(&root.redo_lsn.to_le_bytes());
        page[p + 16..p + 24].copy_from_slice(&root.wal_flush_lsn.to_le_bytes());
        // file_count = TOTAL entries (inline + overflow), the source of truth
        // used on read to compute how many overflow entries to expect.
        page[p + 24..p + 28].copy_from_slice(&(total as u32).to_le_bytes());
        page[p + 28..p + 32].copy_from_slice(&root.entry_page_count.to_le_bytes());
        page[p + 32..p + 40].copy_from_slice(&root.snapshot_lsn.to_le_bytes());
        page[p + 40..p + 48].copy_from_slice(&root.created_at.to_le_bytes());
        page[p + 48..p + 64].copy_from_slice(&root.shard_uuid);

        // Inline FileEntry records (first `inline_count`)
        let entries_start = p + ROOT_META_SIZE;
        for (i, entry) in root.entries.iter().take(inline_count).enumerate() {
            let offset = entries_start + i * FileEntry::SIZE;
            entry.write_to(&mut page[offset..offset + FileEntry::SIZE]);
        }

        // Compute CRC32C over payload region
        MoonPageHeader::compute_checksum(page);
    }

    /// Serialize up to `ENTRIES_PER_OVERFLOW_PAGE` FileEntry records into a 4KB
    /// `ManifestEntry` overflow page (header + entries + CRC32C, no per-page
    /// meta). `entries.len()` must be ≤ `ENTRIES_PER_OVERFLOW_PAGE`.
    fn serialize_overflow_page(entries: &[FileEntry], page: &mut [u8]) {
        assert!(page.len() >= PAGE_4K);
        assert!(entries.len() <= ENTRIES_PER_OVERFLOW_PAGE);
        page[..PAGE_4K].fill(0);

        let mut hdr = MoonPageHeader::new(PageType::ManifestEntry, 0, 0);
        hdr.format_version = MANIFEST_FORMAT_V2;
        hdr.payload_bytes = (entries.len() * FileEntry::SIZE) as u32;
        hdr.entry_count = entries.len() as u32;
        hdr.write_to(page);

        let start = MOONPAGE_HEADER_SIZE;
        for (i, e) in entries.iter().enumerate() {
            let off = start + i * FileEntry::SIZE;
            e.write_to(&mut page[off..off + FileEntry::SIZE]);
        }
        MoonPageHeader::compute_checksum(page);
    }

    /// Parse a `ManifestEntry` overflow page. Returns `None` on ANY header,
    /// type, CRC, or framing failure so a torn/corrupt overflow page makes the
    /// whole root fall back to the last-good slot (never surfaces garbage).
    fn parse_overflow_page(page: &[u8]) -> Option<Vec<FileEntry>> {
        if page.len() < PAGE_4K {
            return None;
        }
        let hdr = MoonPageHeader::read_from(page)?;
        if hdr.page_type != PageType::ManifestEntry {
            return None;
        }
        if !MoonPageHeader::verify_checksum(page) {
            return None;
        }
        let n = hdr.entry_count as usize;
        if n > ENTRIES_PER_OVERFLOW_PAGE {
            return None;
        }
        if hdr.payload_bytes as usize != n * FileEntry::SIZE {
            return None;
        }
        let start = MOONPAGE_HEADER_SIZE;
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let off = start + i * FileEntry::SIZE;
            out.push(FileEntry::read_from(&page[off..])?);
        }
        Some(out)
    }

    /// Load one slot fully: parse its inline root, then read its append-only
    /// overflow run. Returns `None` (→ dual-root fallback) if the root is
    /// invalid OR its overflow is torn/short/corrupt/inconsistent — so a crash
    /// mid-overflow-write can never surface a torn mix or garbage entries.
    fn load_root(buf: &[u8], slot_offset: usize) -> Option<ManifestRoot> {
        let end = slot_offset.checked_add(PAGE_4K)?;
        if end > buf.len() {
            return None;
        }
        let slice = &buf[slot_offset..end];
        let mut root = Self::try_parse_root(slice)?;

        if root.entry_page_count > 0 {
            // Overflow run start lives in the root header's `next_page`.
            let hdr = MoonPageHeader::read_from(slice)?;
            let start = hdr.next_page as usize;
            let npages = root.entry_page_count as usize;
            for pi in 0..npages {
                let off = start.checked_add(pi)?.checked_mul(PAGE_4K)?;
                let pend = off.checked_add(PAGE_4K)?;
                if pend > buf.len() {
                    return None; // torn/short overflow → fall back to last-good
                }
                let entries = Self::parse_overflow_page(&buf[off..pend])?;
                root.entries.extend(entries);
            }
        }

        // The reconstructed total must reconcile with the root's file_count,
        // else treat the slot as corrupt and fall back.
        if root.entries.len() != root.file_count as usize {
            return None;
        }
        Some(root)
    }
}

impl ManifestIo {
    /// Persist a complete root snapshot: append+sync its overflow run, write
    /// the root to the inactive slot, sync (the atomic commit point), flip
    /// slots, then opportunistically compact. This is the historical body of
    /// `ShardManifest::commit()`, made independent of the in-RAM manifest
    /// state so it can run on the manifest-sync thread (task #59).
    pub(crate) fn persist(&mut self, mut root: ManifestRoot) -> std::io::Result<()> {
        #[cfg(test)]
        {
            self.knobs
                .persist_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let ms = self
                .knobs
                .sync_delay_ms
                .load(std::sync::atomic::Ordering::SeqCst);
            if ms > 0 {
                std::thread::sleep(std::time::Duration::from_millis(ms));
            }
            if self
                .knobs
                .inject_persist_error
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(std::io::Error::other("injected persist failure (test)"));
            }
        }
        // A prior compaction renamed a fresh manifest into place durably but then
        // failed to reopen our handle, so `self.file` still points at the
        // pre-rename (orphaned) inode. Reattach to the live file BEFORE any write;
        // writing through the stale handle would silently discard this and every
        // later commit. If the reopen still fails, surface the error (this commit
        // genuinely cannot be persisted) rather than losing data silently.
        if self.needs_reopen {
            self.file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.path)?;
            self.active_slot = 0;
            self.needs_reopen = false;
        }
        let total = root.entries.len();

        // Entries beyond the inline cap go into append-only overflow pages.
        // ORDER IS THE CRASH-SAFETY INVARIANT: overflow pages are appended at
        // EOF and `sync_data`'d BEFORE the root that references them. Because
        // the run is appended (never overwriting the currently-active slot's
        // older overflow), a crash before the root's atomic commit leaves the
        // previously-committed state — its root AND its overflow run — fully
        // intact, so `open()` falls back to it. Partial loss, never corruption.
        let inline_count = total.min(MAX_INLINE_ENTRIES);
        let overflow = &root.entries[inline_count..];
        let npages = overflow.len().div_ceil(ENTRIES_PER_OVERFLOW_PAGE);

        let overflow_start_page: u32 = if npages > 0 {
            let eof = self.file.seek(SeekFrom::End(0))?;
            // The manifest is always a whole number of 4 KB pages.
            debug_assert_eq!(eof % PAGE_4K as u64, 0);
            let start_page = (eof / PAGE_4K as u64) as u32;
            let mut buf = vec![0u8; npages * PAGE_4K];
            for (pi, chunk) in overflow.chunks(ENTRIES_PER_OVERFLOW_PAGE).enumerate() {
                ShardManifest::serialize_overflow_page(
                    chunk,
                    &mut buf[pi * PAGE_4K..(pi + 1) * PAGE_4K],
                );
            }
            self.file.seek(SeekFrom::Start(eof))?;
            self.file.write_all(&buf)?;
            self.file.sync_data()?; // overflow durable BEFORE the root points at it
            start_page
        } else {
            0
        };

        root.entry_page_count = npages as u32;

        let mut page = [0u8; PAGE_4K];
        ShardManifest::serialize_root(&root, overflow_start_page, &mut page);

        // Write to the inactive slot
        let write_offset = if self.active_slot == 0 {
            ROOT_B_OFFSET
        } else {
            ROOT_A_OFFSET
        };

        self.file.seek(SeekFrom::Start(write_offset))?;
        self.file.write_all(&page)?;
        self.file.sync_data()?; // ATOMIC COMMIT POINT

        // Flip active slot
        self.active_slot = if self.active_slot == 0 { 1 } else { 0 };

        // Bound append-only growth: when the file is mostly dead overflow from
        // superseded commits, rewrite it compactly. Best-effort — the commit is
        // already durable, so a compaction failure must not fail the commit.
        if npages > 0 {
            if let Ok(file_len) = self.file.seek(SeekFrom::End(0)) {
                let live_pages = 2 + npages as u64;
                let live_bytes = live_pages * PAGE_4K as u64;
                if file_len > live_bytes.saturating_mul(4) && file_len > 16 * PAGE_4K as u64 {
                    if let Err(e) = self.compact(&mut root) {
                        tracing::warn!(error = %e, "manifest compaction failed (commit already durable)");
                    }
                }
            }
        }

        Ok(())
    }

    /// Rewrite the manifest compactly, reclaiming dead overflow pages from
    /// superseded commits. Layout becomes `[Root A][Root B][fresh overflow]`
    /// with the active root written to BOTH slots (same epoch) so either is a
    /// valid recovery target. Crash-safe via temp-file + atomic rename: the
    /// live manifest stays valid until the rename completes.
    fn compact(&mut self, root: &mut ManifestRoot) -> std::io::Result<()> {
        let total = root.entries.len();
        let inline_count = total.min(MAX_INLINE_ENTRIES);
        let overflow = &root.entries[inline_count..];
        let npages = overflow.len().div_ceil(ENTRIES_PER_OVERFLOW_PAGE);

        // Overflow starts immediately after the two root pages.
        let overflow_start_page: u32 = if npages > 0 { 2 } else { 0 };
        root.entry_page_count = npages as u32;

        let mut buf = vec![0u8; (2 + npages) * PAGE_4K];
        for (pi, chunk) in overflow.chunks(ENTRIES_PER_OVERFLOW_PAGE).enumerate() {
            let o = (2 + pi) * PAGE_4K;
            ShardManifest::serialize_overflow_page(chunk, &mut buf[o..o + PAGE_4K]);
        }
        ShardManifest::serialize_root(root, overflow_start_page, &mut buf[0..PAGE_4K]);
        ShardManifest::serialize_root(root, overflow_start_page, &mut buf[PAGE_4K..2 * PAGE_4K]);

        let tmp = self.path.with_extension("manifest.compact.tmp");
        std::fs::write(&tmp, &buf)?;
        {
            let tf = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&tmp)?;
            tf.sync_data()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        if let Some(parent) = self.path.parent() {
            crate::persistence::fsync::fsync_directory(parent)?;
        }
        // Repoint the file handle at the freshly-rewritten manifest. Both slots
        // carry the active root at the same epoch; treat slot 0 as active so the
        // next commit writes the incremented epoch to slot 1 (the newest).
        //
        // The rename above is the durability point — the compacted manifest is
        // already safe on disk. If the reopen below fails, our `self.file` still
        // refers to the pre-rename (orphaned) inode; do NOT keep using it (that
        // silently discards every later commit). Mark `needs_reopen` so the next
        // `commit()` reattaches to `self.path` first, and propagate the error.
        #[cfg(test)]
        if self.fail_compact_reopen {
            // Simulate the lost-handle race: point `self.file` at a throwaway
            // inode (the real "orphaned pre-rename fd"), mark for reopen, return.
            let orphan = self.path.with_extension("compact.orphan.test");
            self.file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&orphan)?;
            self.needs_reopen = true;
            return Err(std::io::Error::other("simulated compact reopen failure"));
        }
        match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
        {
            Ok(f) => {
                self.file = f;
                self.active_slot = 0;
                self.needs_reopen = false;
                Ok(())
            }
            Err(e) => {
                self.needs_reopen = true;
                Err(e)
            }
        }
    }
}

impl ShardManifest {
    /// Try to parse a root page from a 4KB buffer.
    ///
    /// Returns `None` if magic/type mismatch or CRC32C fails. Recognizes both
    /// v1 (48-byte entries, format_version=1) and v2 (56-byte entries,
    /// format_version=2). v1 entries are upgraded in-memory with
    /// `last_modified_lsn = created_lsn` so the rest of the system sees a
    /// uniform v2 view.
    fn try_parse_root(page: &[u8]) -> Option<ManifestRoot> {
        if page.len() < PAGE_4K {
            return None;
        }

        // Verify header
        let hdr = MoonPageHeader::read_from(page)?;
        if hdr.page_type != PageType::ManifestRoot {
            return None;
        }

        // Verify CRC32C
        if !MoonPageHeader::verify_checksum(page) {
            return None;
        }

        // Pick the entry size based on the on-disk format_version.
        // Unknown versions are rejected (defensive — better to fail loudly
        // than misinterpret a future format).
        let entry_size = match hdr.format_version {
            MANIFEST_FORMAT_V1 => FileEntry::SIZE_V1,
            MANIFEST_FORMAT_V2 => FileEntry::SIZE,
            _ => return None,
        };

        // Parse metadata (64 bytes)
        let p = MOONPAGE_HEADER_SIZE;
        let epoch = u64::from_le_bytes(page[p..p + 8].try_into().ok()?);
        let redo_lsn = u64::from_le_bytes(page[p + 8..p + 16].try_into().ok()?);
        let wal_flush_lsn = u64::from_le_bytes(page[p + 16..p + 24].try_into().ok()?);
        // `file_count` is the TOTAL across inline + overflow. Only the first
        // `inline_count` records live in this root page; the surplus lives in
        // overflow pages loaded later by `load_root`. v1 never overflows.
        let file_count = u32::from_le_bytes(page[p + 24..p + 28].try_into().ok()?);
        let entry_page_count = u32::from_le_bytes(page[p + 28..p + 32].try_into().ok()?);
        let inline_count = if entry_size == FileEntry::SIZE_V1 {
            file_count as usize
        } else {
            (file_count as usize).min(MAX_INLINE_ENTRIES)
        };

        // Validate payload framing against the INLINE count — the region this
        // page's CRC32C authenticates. Overflow pages carry their own CRC and
        // are validated when loaded. This prevents reading unchecked trailing
        // bytes on a corrupted root page.
        let expected_payload = ROOT_META_SIZE.checked_add(inline_count.checked_mul(entry_size)?)?;
        if hdr.payload_bytes as usize != expected_payload {
            return None;
        }
        if hdr.entry_count as usize != inline_count {
            return None;
        }
        let snapshot_lsn = u64::from_le_bytes(page[p + 32..p + 40].try_into().ok()?);
        let created_at = u64::from_le_bytes(page[p + 40..p + 48].try_into().ok()?);
        let mut shard_uuid = [0u8; 16];
        shard_uuid.copy_from_slice(&page[p + 48..p + 64]);

        // Parse the inline entries. Use the size dictated by format_version so
        // v1 manifests remain readable; FileEntry::read_v1 synthesizes
        // `last_modified_lsn = created_lsn` for the upgraded in-memory view.
        let entries_start = p + ROOT_META_SIZE;
        let mut entries = Vec::with_capacity(file_count as usize);
        for i in 0..inline_count {
            let offset = entries_start + i * entry_size;
            let entry = if entry_size == FileEntry::SIZE_V1 {
                FileEntry::read_v1(&page[offset..])?
            } else {
                FileEntry::read_from(&page[offset..])?
            };
            entries.push(entry);
        }

        Some(ManifestRoot {
            epoch,
            redo_lsn,
            wal_flush_lsn,
            file_count,
            entry_page_count,
            snapshot_lsn,
            created_at,
            shard_uuid,
            entries,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_entry_roundtrip_all_fields() {
        let entry = FileEntry {
            file_id: 0x0102_0304_0506_0708,
            file_type: PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: 1000,
            byte_size: 4_096_000,
            created_lsn: 42,
            db_index: 0x1111_2222_3333_4444,
            max_key_hash: 0xAAAA_BBBB_CCCC_DDDD,
            last_modified_lsn: 4242,
        };

        let mut buf = [0u8; FileEntry::SIZE];
        entry.write_to(&mut buf);

        let parsed = FileEntry::read_from(&buf).expect("should parse");
        assert_eq!(parsed, entry);
    }

    #[test]
    fn file_entry_exactly_56_bytes() {
        let entry = FileEntry {
            file_id: 1,
            file_type: PageType::VecCodes as u8,
            status: FileStatus::Sealed,
            tier: StorageTier::Warm,
            page_size_log2: 16,
            page_count: 500,
            byte_size: 32_768_000,
            created_lsn: 100,
            db_index: 0,
            max_key_hash: u64::MAX,
            last_modified_lsn: 200,
        };

        let mut buf = [0xFFu8; 64];
        entry.write_to(&mut buf);

        // First 56 bytes get written (v2 layout); bytes 56..64 must stay 0xFF.
        assert_eq!(FileEntry::SIZE, 56);
        assert_eq!(buf[56..64], [0xFF; 8]);
    }

    /// P1 — last_modified_lsn must roundtrip independently of created_lsn.
    #[test]
    fn file_entry_last_modified_lsn_independent_of_created() {
        let entry = FileEntry {
            file_id: 7,
            file_type: PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Warm,
            page_size_log2: 12,
            page_count: 10,
            byte_size: 40960,
            created_lsn: 1,
            db_index: 0,
            max_key_hash: 0,
            last_modified_lsn: 99_999,
        };

        let mut buf = [0u8; FileEntry::SIZE];
        entry.write_to(&mut buf);
        let parsed = FileEntry::read_from(&buf).expect("should parse v2");
        assert_eq!(parsed.created_lsn, 1);
        assert_eq!(parsed.last_modified_lsn, 99_999);
    }

    /// P1 — legacy v1 (48-byte) entries must decode with
    /// `last_modified_lsn = created_lsn` as a lossless fallback. This is the
    /// contract that lets existing on-disk manifests survive the upgrade.
    #[test]
    fn file_entry_v1_decodes_with_synthesized_last_modified() {
        // Build a 48-byte v1 entry by hand (no last_modified_lsn trailer).
        let mut buf = [0u8; FileEntry::SIZE_V1];
        buf[0..8].copy_from_slice(&123u64.to_le_bytes()); // file_id
        buf[8] = PageType::KvLeaf as u8;
        buf[9] = FileStatus::Active as u8;
        buf[10] = StorageTier::Hot as u8;
        buf[11] = 12;
        buf[12..16].copy_from_slice(&50u32.to_le_bytes()); // page_count
        buf[16..24].copy_from_slice(&(50u64 * 4096).to_le_bytes()); // byte_size
        buf[24..32].copy_from_slice(&777u64.to_le_bytes()); // created_lsn
        buf[32..40].copy_from_slice(&0u64.to_le_bytes());
        buf[40..48].copy_from_slice(&u64::MAX.to_le_bytes());

        let parsed = FileEntry::read_v1(&buf).expect("v1 decode");
        assert_eq!(parsed.file_id, 123);
        assert_eq!(parsed.created_lsn, 777);
        assert_eq!(
            parsed.last_modified_lsn, 777,
            "v1 fallback must synthesize last_modified_lsn = created_lsn",
        );
    }

    #[test]
    fn file_status_all_variants() {
        assert_eq!(FileStatus::from_u8(1), Some(FileStatus::Active));
        assert_eq!(FileStatus::from_u8(2), Some(FileStatus::Building));
        assert_eq!(FileStatus::from_u8(3), Some(FileStatus::Sealed));
        assert_eq!(FileStatus::from_u8(4), Some(FileStatus::Compacting));
        assert_eq!(FileStatus::from_u8(5), Some(FileStatus::Tombstone));
        assert_eq!(FileStatus::from_u8(6), Some(FileStatus::Archived));
        assert_eq!(FileStatus::from_u8(0), None);
        assert_eq!(FileStatus::from_u8(7), None);
        assert_eq!(FileStatus::from_u8(255), None);
    }

    #[test]
    fn file_storage_tier_all_variants() {
        assert_eq!(StorageTier::from_u8(0x01), Some(StorageTier::Hot));
        assert_eq!(StorageTier::from_u8(0x02), Some(StorageTier::Warm));
        assert_eq!(StorageTier::from_u8(0x03), Some(StorageTier::Cold));
        assert_eq!(StorageTier::from_u8(0x04), Some(StorageTier::Archive));
        assert_eq!(StorageTier::from_u8(0), None);
        assert_eq!(StorageTier::from_u8(5), None);
        assert_eq!(StorageTier::from_u8(255), None);
    }

    #[test]
    fn file_entry_page_size_variants() {
        // 4KB pages
        let entry_4k = FileEntry {
            file_id: 10,
            file_type: PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: 100,
            byte_size: 409_600,
            created_lsn: 1,
            db_index: 0,
            max_key_hash: 0,
            last_modified_lsn: 1,
        };
        let mut buf = [0u8; FileEntry::SIZE];
        entry_4k.write_to(&mut buf);
        let parsed = FileEntry::read_from(&buf).unwrap();
        assert_eq!(parsed.page_size_log2, 12);

        // 64KB pages
        let entry_64k = FileEntry {
            page_size_log2: 16,
            file_type: PageType::VecCodes as u8,
            ..entry_4k
        };
        entry_64k.write_to(&mut buf);
        let parsed = FileEntry::read_from(&buf).unwrap();
        assert_eq!(parsed.page_size_log2, 16);
    }

    #[test]
    fn file_entry_read_from_short_buffer() {
        // v2 read needs >= 56 bytes
        let buf = [0u8; 55];
        assert!(FileEntry::read_from(&buf).is_none());
        // v1 read needs >= 48 bytes
        let buf = [0u8; 47];
        assert!(FileEntry::read_v1(&buf).is_none());
    }

    // --- ShardManifest tests ---

    fn make_entry(id: u64) -> FileEntry {
        FileEntry {
            file_id: id,
            file_type: PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: 100,
            byte_size: 409_600,
            created_lsn: id,
            db_index: 0,
            max_key_hash: u64::MAX,
            last_modified_lsn: id,
        }
    }

    #[test]
    fn test_manifest_create_and_open() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let m = ShardManifest::create(&path).unwrap();
        assert_eq!(m.epoch(), 1);
        assert_eq!(m.active_slot(), 0);
        assert!(m.files().is_empty());

        // File should be exactly 8192 bytes
        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), 8192);

        // Re-open should recover same state
        let m2 = ShardManifest::open(&path).unwrap();
        assert_eq!(m2.epoch(), 1);
        assert!(m2.files().is_empty());
    }

    #[test]
    fn test_manifest_alternating_commit() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let mut m = ShardManifest::create(&path).unwrap();
        assert_eq!(m.active_slot(), 0); // Root A is active after create

        // First commit: writes to Root B (inactive), then flips active to 1
        m.add_file(make_entry(1)).unwrap();
        m.commit().unwrap();
        assert_eq!(m.epoch(), 2);
        assert_eq!(m.active_slot(), 1); // Now Root B is active

        // Second commit: writes to Root A (inactive), then flips active to 0
        m.add_file(make_entry(2)).unwrap();
        m.commit().unwrap();
        assert_eq!(m.epoch(), 3);
        assert_eq!(m.active_slot(), 0); // Back to Root A

        // Verify recovery picks epoch 3
        let m2 = ShardManifest::open(&path).unwrap();
        assert_eq!(m2.epoch(), 3);
        assert_eq!(m2.files().len(), 2);
    }

    // Regression (PR #136 review, BUG #1): compact() renames a fresh manifest
    // into place durably, then reopens self.file against the new inode. If that
    // reopen fails, the old handle refers to the pre-rename (orphaned) inode —
    // continuing to write through it silently discards every later commit, and
    // recovery sees only the compaction snapshot. The fix flags `needs_reopen`
    // and makes the next commit reattach to self.path before writing.
    #[test]
    fn compact_reopen_failure_does_not_silently_lose_later_commits() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");
        let mut m = ShardManifest::create(&path).unwrap();

        // Seed committed state large enough to exercise inline + overflow.
        for id in 1..=80 {
            m.add_file(make_entry(id)).unwrap();
        }
        m.commit().unwrap();

        // Simulate the race: compaction rewrote + renamed durably (data safe on
        // disk) but then failed to reopen the handle.
        m.set_fail_compact_reopen(true);
        let err = m.compact_for_test().unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        assert!(
            m.needs_reopen(),
            "compact() must flag needs_reopen when it loses the file handle"
        );
        m.set_fail_compact_reopen(false);

        // The next commit MUST reattach to the real manifest first. Without the
        // guard this write lands in the orphaned inode and is lost on recovery.
        m.add_file(make_entry(999)).unwrap();
        m.commit().unwrap();
        assert!(
            !m.needs_reopen(),
            "commit() must clear needs_reopen after reattaching to self.path"
        );

        // Recover from disk: the post-failure entry MUST be durable.
        let recovered = ShardManifest::open(&path).unwrap();
        let ids: Vec<u64> = recovered.files().iter().map(|e| e.file_id).collect();
        assert!(
            ids.contains(&999),
            "an entry committed after a compact-reopen failure must survive \
             recovery (stale-handle silent data-loss regression)"
        );
        assert_eq!(recovered.files().len(), 81);
    }

    #[test]
    fn test_manifest_recovery_picks_higher_epoch() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let mut m = ShardManifest::create(&path).unwrap();
        // epoch 1 on Root A

        m.add_file(make_entry(1)).unwrap();
        m.commit().unwrap(); // epoch 2 on Root B

        m.add_file(make_entry(2)).unwrap();
        m.commit().unwrap(); // epoch 3 on Root A

        m.add_file(make_entry(3)).unwrap();
        m.commit().unwrap(); // epoch 4 on Root B

        m.add_file(make_entry(4)).unwrap();
        m.commit().unwrap(); // epoch 5 on Root A

        m.add_file(make_entry(5)).unwrap();
        m.commit().unwrap(); // epoch 6 on Root B

        // Root A has epoch 5 (entries 1-4), Root B has epoch 6 (entries 1-5)
        // Recovery should pick Root B (higher epoch)
        let m2 = ShardManifest::open(&path).unwrap();
        assert_eq!(m2.epoch(), 6);
        assert_eq!(m2.active_slot(), 1);
        assert_eq!(m2.files().len(), 5);
    }

    #[test]
    fn test_manifest_recovery_corrupt_root_fallback() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let mut m = ShardManifest::create(&path).unwrap();

        m.add_file(make_entry(1)).unwrap();
        m.commit().unwrap(); // epoch 2 on Root B

        m.add_file(make_entry(2)).unwrap();
        m.commit().unwrap(); // epoch 3 on Root A

        // Corrupt Root A (offset 0) payload
        let mut buf = std::fs::read(&path).unwrap();
        buf[MOONPAGE_HEADER_SIZE + 5] ^= 0xFF;
        std::fs::write(&path, &buf).unwrap();

        // Should fallback to Root B (epoch 2)
        let m2 = ShardManifest::open(&path).unwrap();
        assert_eq!(m2.epoch(), 2);
        assert_eq!(m2.active_slot(), 1);
        assert_eq!(m2.files().len(), 1);
    }

    #[test]
    fn test_manifest_both_corrupt_returns_error() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let m = ShardManifest::create(&path).unwrap();
        drop(m);

        // Corrupt both roots
        let mut buf = std::fs::read(&path).unwrap();
        // Corrupt Root A payload
        buf[MOONPAGE_HEADER_SIZE + 3] ^= 0xFF;
        // Corrupt Root B payload
        buf[PAGE_4K + MOONPAGE_HEADER_SIZE + 3] ^= 0xFF;
        std::fs::write(&path, &buf).unwrap();

        let result = ShardManifest::open(&path);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("corrupted"),
            "error should mention corruption: {}",
            err,
        );
    }

    #[test]
    fn test_manifest_max_inline_entries() {
        // v2: (4096 - 64 header - 64 meta) / 56 = 70.
        // Capacity drops from 82 (v1) → 70 (v2) as the price of adding
        // last_modified_lsn for PITR. Overflow pages (entry_page_count) are
        // the long-term answer beyond this ceiling.
        assert_eq!(MAX_INLINE_ENTRIES, 70);

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let mut m = ShardManifest::create(&path).unwrap();

        // Add exactly 70 entries
        for i in 0..70u64 {
            m.add_file(make_entry(i + 1)).unwrap();
        }
        m.commit().unwrap();

        // Verify recovery
        let m2 = ShardManifest::open(&path).unwrap();
        assert_eq!(m2.files().len(), 70);

        // Beyond the inline cap, entries now persist via overflow pages
        // (was: commit rejected the 71st entry).
        drop(m2);
        let mut m3 = ShardManifest::open(&path).unwrap();
        m3.add_file(make_entry(71)).unwrap();
        m3.commit()
            .expect("71st entry must persist via an overflow page");
        drop(m3);
        assert_eq!(
            ShardManifest::open(&path).unwrap().files().len(),
            71,
            "overflow entry must survive reopen",
        );
    }

    /// #15 RED — durability contract: a manifest holding FAR more than the
    /// 70-entry inline cap must persist every entry and recover them all.
    /// 70 entries ≈ 9 MB cold/shard, so every real disk-offload deployment
    /// blows past the cap immediately (Phase C hit "495 > 70"). RED today:
    /// `commit()` returns Err at the 71st entry.
    #[test]
    fn test_overflow_persists_beyond_inline_cap() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let n = 200u64; // ~3 overflow pages worth
        let mut m = ShardManifest::create(&path).unwrap();
        for i in 0..n {
            m.add_file(make_entry(i + 1)).unwrap();
        }
        m.commit()
            .expect("manifest must persist >70 entries via overflow pages");
        drop(m);

        // Reopen from disk only — proves on-disk durability, not in-memory state.
        let m2 = ShardManifest::open(&path).unwrap();
        assert_eq!(
            m2.files().len(),
            n as usize,
            "all {n} entries must recover from overflow pages",
        );
        let ids: std::collections::HashSet<u64> = m2.files().iter().map(|e| e.file_id).collect();
        for i in 0..n {
            assert!(ids.contains(&(i + 1)), "file_id {} lost on recovery", i + 1);
        }
    }

    /// #15 RED — crash-atomicity: a torn overflow write during a later commit
    /// must NEVER corrupt the previously-committed state. The advisor's key
    /// risk: a naïve overflow chain converts today's PARTIAL loss (last-good
    /// inline root intact) into TOTAL loss (root points at a half-written
    /// overflow page → garbage → lose everything).
    ///
    /// Model: commit state1 (N1=150 > cap), then commit state2 (N2=300), then
    /// truncate the file by one 4 KB page to simulate a crash mid-overflow-write
    /// of state2. Reopen MUST yield a consistent state — the last-good state1
    /// (150) via dual-root fallback — never a panic, never a torn mix.
    #[test]
    fn test_overflow_commit_crash_atomicity() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let n1 = 150u64;
        let n2 = 300u64;

        let mut m = ShardManifest::create(&path).unwrap();
        for i in 0..n1 {
            m.add_file(make_entry(i + 1)).unwrap();
        }
        m.commit().expect("state1 (>cap) must commit via overflow");
        // state2: extend to n2 and commit again (flips active slot; state1's
        // root remains in the now-inactive slot as the last-good fallback).
        for i in n1..n2 {
            m.add_file(make_entry(i + 1)).unwrap();
        }
        m.commit().expect("state2 (>cap) must commit via overflow");
        drop(m);

        // Simulate a crash that tore the tail of state2's overflow write:
        // lop off the final 4 KB page. state2's root now references an
        // incomplete overflow region; state1's root + overflow are untouched.
        let full = std::fs::metadata(&path).unwrap().len();
        assert!(full > (2 * PAGE_4K) as u64, "overflow must extend the file");
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(full - PAGE_4K as u64).unwrap();
        drop(f);

        // Recovery must be CONSISTENT, never corrupt. Newest root (state2) has a
        // torn overflow → open() must fall back to the last-good state1 root.
        let recovered = ShardManifest::open(&path)
            .expect("torn overflow tail must not make the manifest unopenable");
        let len = recovered.files().len();
        assert_eq!(
            len, n1 as usize,
            "torn state2 overflow must fall back to last-good state1 ({n1}), got {len}",
        );
        // And every recovered entry must be a real state1 entry (no garbage ids).
        for e in recovered.files() {
            assert!(
                e.file_id >= 1 && e.file_id <= n1,
                "garbage file_id {} after torn-overflow recovery",
                e.file_id,
            );
        }
    }

    /// #16 — append-only overflow must not grow the manifest without bound:
    /// repeatedly committing a >cap set (each commit appends a fresh overflow
    /// run) must trigger compaction so the file stays a small multiple of the
    /// live set, and the data must still fully recover.
    #[test]
    fn test_overflow_compaction_bounds_growth() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let mut m = ShardManifest::create(&path).unwrap();
        for i in 0..100u64 {
            m.add_file(make_entry(i + 1)).unwrap();
        }
        // Each commit re-appends the overflow run; without compaction the file
        // would grow ~1 page per commit (60+ dead runs). Compaction bounds it.
        for _ in 0..60 {
            m.commit().unwrap();
        }
        drop(m);

        let len = std::fs::metadata(&path).unwrap().len();
        // Live set = 2 roots + ceil(30/72)=1 overflow page = 3 pages (12 KB).
        // Compaction keeps the file within a small multiple of that.
        assert!(
            len <= 20 * PAGE_4K as u64,
            "manifest grew unbounded despite compaction: {len} bytes",
        );

        let m2 = ShardManifest::open(&path).unwrap();
        assert_eq!(m2.files().len(), 100, "data lost after compaction");
        let ids: std::collections::HashSet<u64> = m2.files().iter().map(|e| e.file_id).collect();
        for i in 0..100u64 {
            assert!(ids.contains(&(i + 1)), "file_id {} lost", i + 1);
        }
    }

    /// #16 — tombstoning + GC of an entry that lives in the OVERFLOW region
    /// (id > inline cap) must remove exactly that entry and survive reopen.
    #[test]
    fn test_overflow_tombstone_and_gc() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let mut m = ShardManifest::create(&path).unwrap();
        for i in 0..200u64 {
            m.add_file(make_entry(i + 1)).unwrap();
        }
        m.commit().unwrap();

        // id 150 lives in the overflow region (inline cap is 70).
        m.remove_file(150, PageType::KvLeaf);
        let pruned = m.gc_tombstones(0, 0, std::time::Instant::now());
        assert_eq!(pruned, 1, "overflow-region tombstone must be prunable");
        m.commit().unwrap();
        drop(m);

        let m2 = ShardManifest::open(&path).unwrap();
        assert_eq!(m2.files().len(), 199);
        assert!(
            m2.files().iter().all(|e| e.file_id != 150),
            "tombstoned overflow entry survived recovery",
        );
    }

    /// P1 — manifest written today must always stamp format_version = 2.
    /// This is the contract that lets future readers know to expect 56-byte
    /// FileEntry records.
    #[test]
    fn test_manifest_writes_v2_format_version() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let mut m = ShardManifest::create(&path).unwrap();
        m.add_file(make_entry(1)).unwrap();
        m.commit().unwrap();
        drop(m);

        let buf = std::fs::read(&path).unwrap();
        // After the create + one commit, the active root has the latest data.
        // Either Root A or Root B should carry format_version = 2 — just
        // assert that at least one slot is stamped v2 (the active one).
        let v2_at_a = buf[4] == MANIFEST_FORMAT_V2;
        let v2_at_b = buf[PAGE_4K + 4] == MANIFEST_FORMAT_V2;
        assert!(
            v2_at_a || v2_at_b,
            "expected v2 format_version on at least one root; got A={} B={}",
            buf[4],
            buf[PAGE_4K + 4],
        );
    }

    /// P1 — a hand-crafted v1 manifest page (48-byte entries, format_version=1)
    /// must be readable by the current code, with `last_modified_lsn`
    /// synthesized from `created_lsn`. This guards the upgrade path for any
    /// pre-existing on-disk manifests.
    #[test]
    fn test_manifest_v1_format_compat() {
        use crate::persistence::page::{MoonPageHeader, PageType};

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        // Build an 8KB buffer with a v1 Root A and an empty Root B.
        let mut buf = vec![0u8; 2 * PAGE_4K];

        // --- Root A (v1) ---
        // 3 v1 entries of 48 bytes each.
        let entry_size_v1 = FileEntry::SIZE_V1;
        let n_entries = 3usize;
        let payload_bytes = ROOT_META_SIZE + n_entries * entry_size_v1;
        let mut hdr = MoonPageHeader::new(PageType::ManifestRoot, 0, 0);
        hdr.format_version = MANIFEST_FORMAT_V1;
        hdr.payload_bytes = payload_bytes as u32;
        hdr.entry_count = n_entries as u32;
        hdr.write_to(&mut buf[..PAGE_4K]);

        // Manifest meta: epoch=1, everything else zero.
        let p = MOONPAGE_HEADER_SIZE;
        buf[p..p + 8].copy_from_slice(&1u64.to_le_bytes()); // epoch
        buf[p + 24..p + 28].copy_from_slice(&(n_entries as u32).to_le_bytes()); // file_count

        // Three v1 entries with distinct created_lsn values.
        let entries_start = p + ROOT_META_SIZE;
        for i in 0..n_entries {
            let off = entries_start + i * entry_size_v1;
            let created_lsn = 100u64 + i as u64;
            buf[off..off + 8].copy_from_slice(&((i as u64) + 1).to_le_bytes());
            buf[off + 8] = PageType::KvLeaf as u8;
            buf[off + 9] = FileStatus::Active as u8;
            buf[off + 10] = StorageTier::Hot as u8;
            buf[off + 11] = 12;
            buf[off + 12..off + 16].copy_from_slice(&10u32.to_le_bytes());
            buf[off + 16..off + 24].copy_from_slice(&40960u64.to_le_bytes());
            buf[off + 24..off + 32].copy_from_slice(&created_lsn.to_le_bytes());
            buf[off + 32..off + 40].copy_from_slice(&0u64.to_le_bytes());
            buf[off + 40..off + 48].copy_from_slice(&u64::MAX.to_le_bytes());
        }
        MoonPageHeader::compute_checksum(&mut buf[..PAGE_4K]);
        // Root B stays zeroed → invalid on parse, will be ignored.

        std::fs::write(&path, &buf).unwrap();

        // Now open with the current (v2) code and verify v1 compat.
        let m = ShardManifest::open(&path).unwrap();
        assert_eq!(m.epoch(), 1);
        assert_eq!(m.files().len(), n_entries);
        for (i, entry) in m.files().iter().enumerate() {
            assert_eq!(entry.created_lsn, 100 + i as u64);
            assert_eq!(
                entry.last_modified_lsn, entry.created_lsn,
                "v1 entry must synthesize last_modified_lsn = created_lsn",
            );
        }
    }

    #[test]
    fn test_manifest_add_remove_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let mut m = ShardManifest::create(&path).unwrap();

        m.add_file(make_entry(1)).unwrap();
        m.add_file(make_entry(2)).unwrap();
        m.add_file(make_entry(3)).unwrap();
        m.commit().unwrap();

        // Remove file 2
        m.remove_file(2, PageType::KvLeaf);
        m.commit().unwrap();

        let m2 = ShardManifest::open(&path).unwrap();
        assert_eq!(m2.files().len(), 3); // Still 3 entries, one is tombstoned
        assert_eq!(m2.files()[1].status, FileStatus::Tombstone);
        assert_eq!(m2.files()[0].status, FileStatus::Active);
        assert_eq!(m2.files()[2].status, FileStatus::Active);
    }

    /// Two artifacts sharing an id age as two tombstones: GC prunes the older
    /// one and keeps the one tombstoned later (the registry used to be keyed
    /// by id alone, so the later tombstone inherited the earlier one's age).
    #[test]
    fn test_tombstones_sharing_an_id_age_independently() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");
        let mut m = ShardManifest::create(&path).unwrap();
        let mut warm = make_entry(5);
        warm.file_type = PageType::VecCodes as u8;
        m.add_file(warm).unwrap();
        m.add_file(make_entry(5)).unwrap(); // KvLeaf, same id
        m.commit().unwrap();

        m.remove_file(5, PageType::KvLeaf);
        m.commit().unwrap();
        m.commit().unwrap();
        m.remove_file(5, PageType::VecCodes);

        let pruned = m.gc_tombstones(2, 0, Instant::now());
        assert_eq!(pruned, 1, "only the tombstone two epochs old is due");
        let left: Vec<u8> = m.files().iter().map(|e| e.file_type).collect();
        assert_eq!(left, vec![PageType::VecCodes as u8]);
    }

    /// A create that dies part-way must leave NO file at the manifest path —
    /// never a prefix shorter than the two root pages, which every later
    /// `open` rejects (moon#997 review).
    #[test]
    fn test_create_that_dies_midway_leaves_no_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        torn_create_knob::arm(100);
        assert!(ShardManifest::create(&path).is_err(), "the injected crash");
        assert!(
            !path.exists(),
            "a crashed create left {} bytes at the manifest path",
            std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0)
        );

        // The next create succeeds over the leftover temp file.
        let m = ShardManifest::create(&path).unwrap();
        assert_eq!(m.files().len(), 0);
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            (2 * PAGE_4K) as u64
        );
    }

    /// A torn create already on disk (written by an older build) is replaced
    /// by an empty manifest; a full-length corrupt one still fails.
    #[test]
    fn test_open_repairing_torn_create() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");
        std::fs::write(&path, vec![0u8; 100]).unwrap();
        assert!(ShardManifest::open(&path).is_err(), "precondition: torn");
        assert!(ShardManifest::is_torn_create(&path).unwrap());

        let m = ShardManifest::open_repairing_torn_create(&path).unwrap();
        assert_eq!(m.files().len(), 0);
        drop(m);
        assert!(ShardManifest::open(&path).is_ok(), "repaired on disk");

        std::fs::write(&path, vec![0xA5u8; 2 * PAGE_4K]).unwrap();
        assert!(!ShardManifest::is_torn_create(&path).unwrap());
        assert!(
            ShardManifest::open_repairing_torn_create(&path).is_err(),
            "a full-length manifest with both roots corrupt held entries: never re-create it"
        );
    }

    /// moon#893: one id, two artifact kinds — `remove_file` retires only the
    /// kind it is asked for.
    #[test]
    fn test_remove_file_matches_file_type_not_id_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let mut m = ShardManifest::create(&path).unwrap();
        let mut warm = make_entry(5);
        warm.file_type = PageType::VecCodes as u8;
        warm.tier = StorageTier::Warm;
        m.add_file(warm).unwrap();
        m.add_file(make_entry(5)).unwrap(); // KvLeaf, same id
        m.commit().unwrap();

        m.remove_file(5, PageType::VecCodes);
        m.commit().unwrap();

        let m2 = ShardManifest::open(&path).unwrap();
        let status = |t: PageType| {
            m2.files()
                .iter()
                .find(|e| e.file_type == t as u8)
                .map(|e| e.status)
        };
        assert_eq!(status(PageType::VecCodes), Some(FileStatus::Tombstone));
        assert_eq!(status(PageType::KvLeaf), Some(FileStatus::Active));
    }

    /// moon#893: `add_file` appended without a check, so a re-issued id
    /// collected a second Active entry for one path. It must refuse — in any
    /// status of the existing entry — and leave the manifest as it was, on
    /// disk too.
    #[test]
    fn test_add_file_refuses_a_second_entry_for_one_id_and_type() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");
        let mut m = ShardManifest::create(&path).unwrap();

        m.add_file(make_entry(4)).unwrap();
        let mut again = make_entry(4);
        again.db_index = 7;
        assert_eq!(
            m.add_file(again.clone()),
            Err(DuplicateFileEntry {
                file_id: 4,
                file_type: PageType::KvLeaf as u8,
            })
        );
        m.commit().unwrap();
        let reopened = ShardManifest::open(&path).unwrap();
        assert_eq!(
            reopened.files(),
            &[make_entry(4)][..],
            "the first entry is kept"
        );

        // A tombstoned entry still owns its id: its file may still be on disk.
        m.remove_file(4, PageType::KvLeaf);
        assert!(m.add_file(again).is_err());
        assert!(m.has_entry(4, PageType::KvLeaf as u8));
        assert!(!m.has_entry(4, PageType::VecCodes as u8));
    }

    /// Manifests an older build wrote with duplicates must heal, keeping the
    /// LAST Active entry per `(id, type)` and leaving tombstones and distinct
    /// entries alone.
    #[test]
    fn test_dedupe_active_entries_keeps_the_last_of_each() {
        let tmp = tempfile::tempdir().unwrap();
        let mut m = ShardManifest::create(&tmp.path().join("shard-0.manifest")).unwrap();
        let with_db = |id: u64, db: u64| FileEntry {
            db_index: db,
            ..make_entry(id)
        };
        let mut tomb = make_entry(1);
        tomb.status = FileStatus::Tombstone;
        let mut warm1 = make_entry(1);
        warm1.file_type = PageType::VecCodes as u8;
        m.push_entry_unchecked(tomb.clone());
        m.push_entry_unchecked(with_db(1, 0));
        m.push_entry_unchecked(warm1.clone());
        m.push_entry_unchecked(with_db(2, 0));
        m.push_entry_unchecked(with_db(1, 1));
        m.push_entry_unchecked(with_db(1, 2));

        assert_eq!(m.dedupe_active_entries(), 2);
        assert_eq!(
            m.files(),
            &[tomb, warm1, with_db(2, 0), with_db(1, 2)][..],
            "order preserved, last Active kept"
        );
        assert_eq!(m.dedupe_active_entries(), 0, "idempotent");
    }

    #[test]
    fn test_manifest_update_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("shard-0.manifest");

        let mut m = ShardManifest::create(&path).unwrap();
        m.add_file(make_entry(1)).unwrap();
        m.commit().unwrap();

        m.update_file(1, |e| {
            e.status = FileStatus::Sealed;
            e.tier = StorageTier::Warm;
        });
        m.commit().unwrap();

        let m2 = ShardManifest::open(&path).unwrap();
        assert_eq!(m2.files()[0].status, FileStatus::Sealed);
        assert_eq!(m2.files()[0].tier, StorageTier::Warm);
    }

    /// moon#750: fault injection must not escape the manifest that armed it.
    ///
    /// `TEST_INJECT_PERSIST_ERROR` was a process-global static read inside
    /// `ManifestIo::persist`, and `cargo test` runs unit tests concurrently in
    /// one process. `TEST_SYNC_KNOB_LOCK` serialised the tests that *set* it
    /// against each other, but every test that merely *commits* a manifest is a
    /// reader that never takes the lock — so for the window in which the
    /// injecting test held the flag true, an unrelated `commit()` failed with
    /// "injected persist failure (test)". That is exactly the panic #750
    /// recorded, in `test_overflow_compaction_bounds_growth`.
    ///
    /// This test makes that leak deterministic instead of load-dependent: the
    /// flag is held true across the victim's commit, so pre-fix it fails every
    /// run rather than 1-in-many.
    #[test]
    fn injected_persist_failure_is_scoped_to_the_manifest_that_armed_it() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut victim =
            ShardManifest::create(&tmp.path().join("victim.manifest")).expect("create victim");
        let mut injector =
            ShardManifest::create(&tmp.path().join("injector.manifest")).expect("create injector");

        injector.set_inject_persist_error(true);

        // The victim shares nothing with the injector but the process.
        victim.add_file(make_entry(1)).unwrap();
        victim
            .commit()
            .expect("another manifest's injected failure must not fail this commit");

        // Control: the knob is actually armed. Without this, deleting the
        // injection entirely would satisfy the assertion above.
        injector.add_file(make_entry(1)).unwrap();
        assert!(
            injector.commit().is_err(),
            "the manifest that armed injection must still fail its own persist"
        );

        // Disarming is scoped too, and heals only the injector.
        injector.set_inject_persist_error(false);
        injector.commit().expect("disarmed injector must persist");
    }
}
