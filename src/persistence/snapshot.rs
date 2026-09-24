//! Forkless compartmentalized snapshot engine.
//!
//! Produces per-shard **RDB v2** snapshots (`RRDSHARD` magic, version byte
//! `2`) — part of the **storage format v1** umbrella commitment, see
//! [`docs/STORAGE-FORMAT-V1.md`](../../../docs/STORAGE-FORMAT-V1.md).
//!
//! Serializes DashTable segments one at a time as a cooperative task within the
//! shard event loop. Keys modified in not-yet-serialized segments have their old
//! values captured in a per-snapshot overflow buffer (segment-level COW).
//!
//! ## Progress is a position in HASH space, not a segment index (moon#1216)
//!
//! The DashTable keeps splitting while an epoch serializes: a split moves
//! about half of a segment's keys into a NEW segment appended at the end of
//! the segment store. An epoch that walked store indices `0..count` captured
//! at its start therefore never visited those halves — the moon#1216 repro
//! lost 1,744 of 2,000 epoch-start keys. Walking by index up to the CURRENT
//! count instead writes a split-off half of an already-written segment a
//! second time.
//!
//! Extendible hashing gives a structure-independent order instead: a segment
//! of local depth `d` owns exactly the aligned block of hash space sharing
//! its top `d` hash bits, and a split only divides a block in two. So the
//! epoch keeps a per-database `cursor` in hash space and each advance
//! serializes the segment covering `cursor`, then moves `cursor` to the end
//! of that segment's block. Every segment lies wholly below the cursor
//! (written) or wholly at/above it (pending) at every instant — a split
//! cannot straddle it, because the cursor only ever lands on a block
//! boundary and splits only add boundaries. "Is this key still pending?" is
//! `hash(key) >= cursor` in the current database, which no split, directory
//! doubling or segment-store growth can change. No DashTable hook is needed,
//! so a table with no snapshot armed pays nothing at all.
//!
//! ## Pre-images are point-in-time, including ABSENCE
//!
//! A pre-image is the key's state at epoch start: its entry, or `None` when
//! it did not exist then (a TOMBSTONE). Without tombstones a key CREATED in
//! a pending range during the epoch was serialized, and the WAL/AOF record
//! that created it replayed on top (`INCR new` -> 1 in the file, 2 after
//! replay). Pre-images are kept per database ordered by `(hash, key)`, so
//! the ones for the range being written are taken in `O(log n)` and a split
//! moving a key never separates it from its pre-image.
//!
//! ## Unwrap Classification
//!
//! | Context | Classification | Rationale |
//! |---------|---------------|-----------|
//! | `finalize`, `begin_finalize`/`poll_finalize` | **should-recover** (`Result<_, MoonError>`) | Snapshot save failure should not crash server |
//! | `shard_snapshot_save` | **should-recover** (`Result<_, MoonError>`) | Calls finalize; same recovery semantics |
//! | `shard_snapshot_load` | **should-recover** (`Result<_, MoonError>`) | Startup load; failure = log + continue empty |
//! | All `unwrap()` calls (30) | **test-only** | Only appear in `#[cfg(test)]` module |

use std::collections::{BTreeMap, HashSet};
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use crc32fast::Hasher;

use crate::error::{MoonError, SnapshotError};
use crate::persistence::rdb;
use crate::storage::db::Database;
use crate::storage::entry::{Entry, current_time_ms};

// Per-shard snapshot format constants
const SHARD_RDB_MAGIC: &[u8] = b"RRDSHARD";

/// Legacy format: magic(8) + version(1) + shard_id(2) + epoch(8) = 19 bytes preamble
/// (plus eof(1) + global_crc(4) = 24 byte minimum file).
const SHARD_RDB_VERSION_V1: u8 = 1;

/// v0.2 PITR format: adds last_lsn(8) + created_at_unix_ms(8) after epoch.
/// Preamble grows to 35 bytes (minimum file = 40 bytes with eof + crc).
const SHARD_RDB_VERSION_V2: u8 = 2;

/// v0.2 hash-field TTL: same preamble as V2, but every hash entry's body is
/// followed by a `[ttl_count u32][field, ttl_ms u64]*` trailer (count is 0
/// for non-TTL hashes). V1/V2 readers skip the trailer because they don't
/// know to read it; V3 readers handle all three versions.
const SHARD_RDB_VERSION_V3: u8 = 3;

/// Current write version. Bumping this changes on-disk format; the loader
/// branches on the byte to remain backward-compatible with v1/v2 snapshots.
const SHARD_RDB_VERSION: u8 = SHARD_RDB_VERSION_V3;

const EOF_MARKER: u8 = 0xFF;
const SEGMENT_BLOCK_MARKER: u8 = 0xFD;
const DB_SELECTOR: u8 = 0xFE;

/// Snapshot header metadata, peekable without fully loading the file.
///
/// Used by P3 recovery to pick the snapshot whose `last_lsn <= target_lsn`
/// without paying the cost of a full snapshot load.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotMeta {
    pub version: u8,
    pub shard_id: u16,
    pub epoch: u64,
    /// LSN at which the snapshot was taken. 0 for legacy v1 snapshots
    /// (forces full WAL replay — safe fallback).
    pub last_lsn: u64,
    /// Wall-clock when the snapshot finished, milliseconds since epoch.
    /// 0 for legacy v1 snapshots.
    pub created_at_unix_ms: u64,
}

/// State machine for cooperative segment-by-segment snapshot.
///
/// Created when a snapshot epoch begins. Advanced one segment per tick.
/// Completed when all segments across all databases have been serialized.
pub struct SnapshotState {
    /// Snapshot epoch number.
    pub epoch: u64,
    /// Current database index being serialized (0..num_databases).
    current_db: usize,
    /// Position in hash space, within `current_db`, of the next key range to
    /// serialize: every key of `current_db` hashing below it is written
    /// (moon#1216 — see the module docs).
    cursor: u64,
    /// Total number of databases to snapshot.
    num_databases: usize,
    /// Segment counts per database, captured at epoch start. Informational
    /// only since moon#1216: progress is tracked in hash space.
    segment_counts: Vec<usize>,
    /// Segment blocks written so far, across every database.
    segments_written: usize,
    /// Output buffer accumulating serialized bytes. With a
    /// [`SnapshotStream`](crate::persistence::snapshot_stream::SnapshotStream)
    /// attached (every event-loop snapshot, moon#1186) it holds only the
    /// block not yet handed to the writer thread; without one (synchronous
    /// `finalize` callers) it holds the whole file.
    output_buf: Vec<u8>,
    /// Off-shard-thread writer (moon#1186): receives filled blocks, writes
    /// the temp file with a streaming CRC, and publishes it on finalize.
    stream: Option<crate::persistence::snapshot_stream::SnapshotStream>,
    /// COW overflow, one map per database, keyed by `(hash, key)`: the
    /// epoch-start state of every key written before its range was
    /// serialized — its entry, or `None` if it did not exist then. Only
    /// PENDING keys are ever inserted, so a map holds nothing below its
    /// database's cursor; an advance splits the written range off the front
    /// (moved, never cloned). First capture of a key wins (moon#517): it is
    /// the only one taken before ANY write of this epoch touched the key.
    overflow: Vec<BTreeMap<(u64, Bytes), PreImage>>,
    /// Shard ID for the snapshot file header.
    shard_id: u16,
    /// Output file path.
    file_path: PathBuf,
    /// Whether the header has been written.
    header_written: bool,
    /// Whether we need to write a DB selector for the current database.
    db_selector_written: Vec<bool>,
    /// WAL LSN at which this snapshot was taken (v0.2 PITR field).
    /// 0 means "unknown — recovery should replay all WAL from origin".
    /// Set via `set_last_lsn` before the first segment is serialized.
    last_lsn: u64,
    /// Wall-clock at snapshot construction, milliseconds since unix epoch (v0.2).
    created_at_unix_ms: u64,
    /// Set when a structural change the epoch cannot follow (a SWAPDB)
    /// happened while it was in flight: the file would not be point-in-time,
    /// so the snapshot fails loudly instead of publishing it.
    aborted: Option<&'static str>,
}

/// A key's state at the start of a snapshot epoch: its entry, or `None` when
/// the key did not exist then (moon#1216 — see the module docs).
pub type PreImage = Option<Entry>;

/// The aligned block of hash space owned by a DashTable segment of local
/// depth `depth` that contains `hash`: `(start, end)`, `end` exclusive and
/// `None` when the block runs to the top of the hash space.
#[inline]
fn segment_block(hash: u64, depth: u32) -> (u64, Option<u64>) {
    if depth == 0 {
        return (0, None);
    }
    // 1..=64 prefix bits -> 0..=63 bits of span below the prefix.
    let span_bits = 64 - depth.min(64);
    let start = (hash >> span_bits) << span_bits;
    (start, start.checked_add(1u64 << span_bits))
}

impl SnapshotState {
    /// Create a new snapshot state machine.
    ///
    /// Captures segment counts from each database at epoch start.
    pub fn new(shard_id: u16, epoch: u64, databases: &[Database], file_path: PathBuf) -> Self {
        let num_databases = databases.len();
        let segment_counts: Vec<usize> = databases
            .iter()
            .map(|db| db.data().segment_count())
            .collect();
        Self::new_from_metadata(shard_id, epoch, num_databases, segment_counts, file_path)
    }

    /// Create from pre-collected metadata (for Arc<ShardDatabases> path).
    pub fn new_from_metadata(
        shard_id: u16,
        epoch: u64,
        num_databases: usize,
        segment_counts: Vec<usize>,
        file_path: PathBuf,
    ) -> Self {
        SnapshotState {
            epoch,
            current_db: 0,
            cursor: 0,
            num_databases,
            segment_counts,
            segments_written: 0,
            output_buf: Vec::with_capacity(4096),
            stream: None,
            overflow: (0..num_databases).map(|_| BTreeMap::new()).collect(),
            shard_id,
            file_path,
            header_written: false,
            db_selector_written: vec![false; num_databases],
            last_lsn: 0,
            created_at_unix_ms: current_time_ms() as u64,
            aborted: None,
        }
    }

    /// Stamp the WAL LSN at which this snapshot was taken.
    ///
    /// Called by the shard event loop before serialization starts, using
    /// `ShardControlFile.wal_flush_lsn` as the source. Must be called before
    /// `write_header_if_needed` (i.e. before any `advance_one_segment` call).
    /// If not set, the header records `last_lsn = 0`, which downstream
    /// recovery interprets as "replay all WAL from origin" — the safe fallback.
    pub fn set_last_lsn(&mut self, lsn: u64) {
        debug_assert!(
            !self.header_written,
            "set_last_lsn must be called before the snapshot header is written",
        );
        self.last_lsn = lsn;
    }

    /// Return the stamped LSN (0 if never set).
    #[inline]
    pub fn last_lsn(&self) -> u64 {
        self.last_lsn
    }

    /// Return the snapshot's wall-clock creation time (unix ms).
    #[inline]
    pub fn created_at_unix_ms(&self) -> u64 {
        self.created_at_unix_ms
    }

    /// Get the current database index being serialized.
    #[inline]
    pub fn current_db_index(&self) -> usize {
        self.current_db
    }

    /// Hash-space position (within [`Self::current_db_index`]) of the next
    /// key range to serialize (moon#1216): every key of the current database
    /// hashing below it is already in the file.
    #[inline]
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Segment blocks written so far, across every database.
    #[inline]
    pub fn segments_written(&self) -> usize {
        self.segments_written
    }

    /// Segment counts per database captured at epoch start.
    #[inline]
    pub fn segment_counts(&self) -> &[usize] {
        &self.segment_counts
    }

    /// Stream serialized blocks to an off-shard-thread writer (moon#1186)
    /// instead of buffering the whole file. Call right after construction;
    /// on `Err` (no thread) the state keeps the in-memory path.
    pub fn start_streaming(&mut self) -> Result<(), MoonError> {
        if self.stream.is_some() {
            return Ok(());
        }
        let stream = crate::persistence::snapshot_stream::SnapshotStream::spawn(
            self.shard_id,
            self.file_path.clone(),
        )
        .map_err(|e| SnapshotError::Io {
            path: self.file_path.clone(),
            source: e,
        })?;
        self.stream = Some(stream);
        self.ship_if_ready();
        Ok(())
    }

    /// Hand the output block to the writer thread once it is big enough.
    fn ship_if_ready(&mut self) {
        use crate::persistence::snapshot_stream::SNAPSHOT_STREAM_CHUNK;
        if let Some(stream) = &self.stream
            && self.output_buf.len() >= SNAPSHOT_STREAM_CHUNK
        {
            let block = std::mem::replace(
                &mut self.output_buf,
                Vec::with_capacity(SNAPSHOT_STREAM_CHUNK + SNAPSHOT_STREAM_CHUNK / 4),
            );
            stream.send(block);
        }
    }

    /// The writer thread is more than
    /// [`SNAPSHOT_STREAM_MAX_IN_FLIGHT`](crate::persistence::snapshot_stream::SNAPSHOT_STREAM_MAX_IN_FLIGHT)
    /// bytes behind: the tick should not serialize another segment yet.
    #[inline]
    pub fn stream_backlogged(&self) -> bool {
        self.stream.as_ref().is_some_and(|s| s.backlogged())
    }

    /// A write on the writer thread already failed: the snapshot cannot
    /// succeed, finalize now to report it.
    #[inline]
    pub fn stream_failed(&self) -> bool {
        self.aborted.is_some() || self.stream.as_ref().is_some_and(|s| s.failed())
    }

    /// The error a finalize of an aborted snapshot reports.
    fn aborted_error(&self, why: &str) -> MoonError {
        SnapshotError::Io {
            path: self.file_path.clone(),
            source: std::io::Error::other(format!("snapshot aborted: {why}")),
        }
        .into()
    }

    /// Start publishing the snapshot OFF the shard thread (moon#1186): the
    /// last block and the EOF marker go to the writer thread, which appends
    /// the CRC footer, fsyncs, renames and fsyncs the directory. Poll the
    /// outcome with [`Self::poll_finalize`]. Idempotent.
    pub fn begin_finalize(&mut self) -> Result<(), MoonError> {
        if let Some(why) = self.aborted {
            // Never publish: dropping the state abandons the stream, whose
            // writer removes the temp file.
            return Err(self.aborted_error(why));
        }
        if self.stream.as_ref().is_some_and(|s| s.finishing()) {
            return Ok(());
        }
        if self.stream.is_none() {
            // In-memory state: the whole buffer goes to a writer thread now.
            self.start_streaming()?;
        }
        self.output_buf.push(EOF_MARKER);
        let block = std::mem::take(&mut self.output_buf);
        if let Some(stream) = self.stream.as_mut() {
            stream.send(block);
            stream.finish();
        }
        Ok(())
    }

    /// True once [`Self::begin_finalize`] ran.
    #[inline]
    pub fn finalize_started(&self) -> bool {
        self.stream.as_ref().is_some_and(|s| s.finishing())
    }

    /// Non-blocking: `Some(outcome)` once the writer thread has published
    /// (or failed to publish) the file; `None` while it is still working.
    pub fn poll_finalize(&self) -> Option<Result<(), String>> {
        match &self.stream {
            Some(s) if s.finishing() => s.poll(),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn stream_in_flight(&self) -> usize {
        self.stream.as_ref().map_or(0, |s| s.in_flight())
    }

    /// Is the epoch-start state of a key hashing to `hash` in `db_index`
    /// still to be written? A write to such a key must capture its
    /// pre-image first; a write to any other key needs nothing.
    ///
    /// A pure function of `(db_index, hash)` and the cursor — splits,
    /// directory doublings and segment-store growth cannot change the
    /// answer (moon#1216).
    #[inline]
    pub fn is_hash_pending(&self, db_index: usize, hash: u64) -> bool {
        if db_index >= self.num_databases || self.aborted.is_some() {
            return false;
        }
        db_index > self.current_db || (db_index == self.current_db && hash >= self.cursor)
    }

    /// [`Self::is_hash_pending`] for a key.
    #[inline]
    pub fn is_key_pending(&self, db_index: usize, key: &[u8]) -> bool {
        self.is_hash_pending(db_index, crate::storage::dashtable::hash_key(key))
    }

    /// Record a key's epoch-start state before a write changes it: its old
    /// entry, or `None` when the key does not exist yet.
    ///
    /// First capture of a key wins (moon#517): it is the only one taken
    /// before ANY write of this epoch touched the key, so it is the only one
    /// that is the epoch-start state. Repeat captures are dropped, and so is
    /// a capture for a key whose range is already written (the file holds
    /// its epoch-start bytes; keeping the copy would also break the
    /// "nothing below the cursor" invariant the range take relies on).
    pub fn capture_cow(&mut self, db_index: usize, key: Bytes, pre_image: PreImage) {
        let hash = crate::storage::dashtable::hash_key(&key);
        if !self.is_hash_pending(db_index, hash) {
            return;
        }
        self.overflow[db_index]
            .entry((hash, key))
            .or_insert(pre_image);
    }

    /// Pre-images captured and not yet written, across every database.
    #[inline]
    pub fn pending_pre_images(&self) -> usize {
        self.overflow.iter().map(BTreeMap::len).sum()
    }

    /// Fail this snapshot instead of publishing a file that is not
    /// point-in-time (a SWAPDB swapped a database the epoch had not finished
    /// with). The shard's next tick reports the failure; the previous
    /// snapshot file stays in place.
    pub fn abort(&mut self, why: &'static str) {
        if self.aborted.is_none() {
            tracing::error!(
                "Shard {}: snapshot epoch {} aborted: {}",
                self.shard_id,
                self.epoch,
                why
            );
            self.aborted = Some(why);
        }
        self.overflow.iter_mut().for_each(BTreeMap::clear);
    }

    /// Why this snapshot was aborted, if it was.
    #[inline]
    pub fn aborted(&self) -> Option<&'static str> {
        self.aborted
    }

    /// Advance the snapshot by one segment. Returns true when all segments are done.
    ///
    /// Serializes the segment covering the cursor (overflow pre-images
    /// first, then the live entries they do not shadow), writes its CRC32,
    /// and moves the cursor to the end of that segment's hash block.
    /// Advance using a single database reference (for Arc<ShardDatabases> path).
    pub fn advance_one_segment_db(&mut self, db: &Database) -> bool {
        self.write_header_if_needed();
        if self.current_db >= self.num_databases || self.aborted.is_some() {
            return true;
        }
        self.advance_segment_inner(db)
    }

    pub fn advance_one_segment(&mut self, databases: &[Database]) -> bool {
        self.write_header_if_needed();
        if self.current_db >= self.num_databases || self.aborted.is_some() {
            return true;
        }
        let db = &databases[self.current_db];
        self.advance_segment_inner(db)
    }

    fn write_header_if_needed(&mut self) {
        if !self.header_written {
            // Layout (v2):
            //   magic(8) + version(1=v2) + shard_id(2) + epoch(8)
            //   + last_lsn(8) + created_at_unix_ms(8) = 35 bytes preamble
            self.output_buf.extend_from_slice(SHARD_RDB_MAGIC);
            self.output_buf.push(SHARD_RDB_VERSION);
            self.output_buf
                .extend_from_slice(&self.shard_id.to_le_bytes());
            self.output_buf.extend_from_slice(&self.epoch.to_le_bytes());
            self.output_buf
                .extend_from_slice(&self.last_lsn.to_le_bytes());
            self.output_buf
                .extend_from_slice(&self.created_at_unix_ms.to_le_bytes());
            self.header_written = true;
        }
    }

    /// Remove and return the current database's pre-images hashing below
    /// `end` (`None` = to the top of the hash space). The map holds nothing
    /// below the cursor, so this is exactly the range being written.
    fn take_pre_images_below(&mut self, end: Option<u64>) -> BTreeMap<(u64, Bytes), PreImage> {
        let map = &mut self.overflow[self.current_db];
        match end {
            None => std::mem::take(map),
            Some(end) => {
                let rest = map.split_off(&(end, Bytes::new()));
                std::mem::replace(map, rest)
            }
        }
    }

    fn advance_segment_inner(&mut self, db: &Database) -> bool {
        let now_ms = current_time_ms();

        // Write DB selector if this is the first segment of a new database
        if !self.db_selector_written[self.current_db] {
            self.output_buf.push(DB_SELECTOR);
            self.output_buf.push(self.current_db as u8);
            self.db_selector_written[self.current_db] = true;
        }

        // moon#1216: the segment covering the cursor, and the hash block it
        // owns. Splits never straddle the cursor (it only lands on block
        // boundaries), so the block normally STARTS at the cursor. It starts
        // below only if the table was replaced mid-epoch (FLUSHDB/FLUSHALL
        // install a fresh one): its keys below the cursor were written into
        // this db after its range was already in the file — skip them.
        let table = db.data();
        let start = self.cursor;
        let seg_idx = table.segment_index_for_hash(start);
        let segment = table.segment(seg_idx);
        let (block_start, block_end) = segment_block(start, segment.depth());
        let skip_below_cursor = block_start < start;

        // This range's pre-images, MOVED out of the overflow map (moon#1186:
        // no rescan of every captured pre-image, no deep clone).
        let pre_images = self.take_pre_images_below(block_end);
        let shadowed: HashSet<&[u8]> = pre_images.keys().map(|(_, k)| k.as_ref()).collect();

        // Segment block: marker + segment_idx + entry_count + data + CRC32,
        // written straight into the output buffer (moon#1186: no per-segment
        // staging `Vec` copied a second time). `entry_count` is patched in
        // once the entries are known. The index is informational (the
        // loader only logs it).
        self.output_buf.push(SEGMENT_BLOCK_MARKER);
        self.output_buf
            .extend_from_slice(&(seg_idx as u32).to_le_bytes());
        let count_pos = self.output_buf.len();
        self.output_buf.extend_from_slice(&0u32.to_le_bytes());
        let data_start = self.output_buf.len();
        let mut entry_count: u32 = 0;

        // Epoch-start values of keys written since: these win over the live
        // entries. A tombstone (the key did not exist at epoch start) writes
        // nothing and hides the live entry below.
        for ((_, key), pre_image) in &pre_images {
            let Some(entry) = pre_image else { continue };
            if entry.has_expiry() && entry.is_expired_at(now_ms) {
                continue;
            }
            let mark = self.output_buf.len();
            if let Err(e) = rdb::write_entry(&mut self.output_buf, key, entry) {
                // Drop the partial entry: the block must stay parseable.
                self.output_buf.truncate(mark);
                tracing::warn!("Snapshot: skipping entry serialization error: {}", e);
                continue;
            }
            entry_count += 1;
        }

        // Live entries: skip those shadowed by a pre-image and expired ones.
        for (key, entry) in segment.iter_occupied() {
            if !shadowed.is_empty() && shadowed.contains(key.as_bytes()) {
                continue;
            }
            if skip_below_cursor && crate::storage::dashtable::hash_key(key.as_bytes()) < start {
                continue;
            }
            if entry.has_expiry() && entry.is_expired_at(now_ms) {
                continue;
            }
            let mark = self.output_buf.len();
            if let Err(e) = rdb::write_entry(&mut self.output_buf, key.as_bytes(), entry) {
                self.output_buf.truncate(mark);
                tracing::warn!("Snapshot: skipping entry serialization error: {}", e);
                continue;
            }
            entry_count += 1;
        }

        // Per-segment CRC32 covers the entry data
        let data_end = self.output_buf.len();
        self.output_buf[count_pos..count_pos + 4].copy_from_slice(&entry_count.to_le_bytes());
        let mut hasher = Hasher::new();
        hasher.update(&self.output_buf[data_start..data_end]);
        let crc = hasher.finalize();
        self.output_buf.extend_from_slice(&crc.to_le_bytes());
        self.segments_written += 1;

        // Move past this block; the last block of a database ends at the top
        // of the hash space.
        match block_end {
            Some(end) => self.cursor = end,
            None => {
                self.current_db += 1;
                self.cursor = 0;
            }
        }

        // moon#1186: hand full blocks to the writer thread as they fill.
        self.ship_if_ready();

        self.current_db >= self.num_databases
    }

    /// Finalize the snapshot: write EOF marker, global CRC32, and atomically write to disk.
    ///
    /// Synchronous — for callers off the shard event loop (`SAVE`-style
    /// helpers, tests). With a stream attached it hands off to the writer
    /// thread and waits for it; the event loop uses
    /// [`Self::begin_finalize`] + [`Self::poll_finalize`] instead.
    pub fn finalize(&mut self) -> Result<(), MoonError> {
        if let Some(why) = self.aborted {
            return Err(self.aborted_error(why));
        }
        if self.stream.is_some() {
            self.begin_finalize()?;
            let outcome = self.stream.as_ref().map_or(Ok(()), |s| s.wait());
            return outcome.map_err(|detail| {
                SnapshotError::Io {
                    path: self.file_path.clone(),
                    source: std::io::Error::other(detail),
                }
                .into()
            });
        }
        // Write EOF marker
        self.output_buf.push(EOF_MARKER);

        // Global CRC32 of entire output_buf
        let mut hasher = Hasher::new();
        hasher.update(&self.output_buf);
        let global_crc = hasher.finalize();
        self.output_buf.extend_from_slice(&global_crc.to_le_bytes());

        // Atomic write: write to .tmp, fsync file, rename, fsync directory
        let tmp_path = self.file_path.with_extension("rrdshard.tmp");
        std::fs::write(&tmp_path, &self.output_buf).map_err(|e| SnapshotError::Io {
            path: tmp_path.clone(),
            source: e,
        })?;
        crate::persistence::fsync::fsync_file(&tmp_path).map_err(|e| SnapshotError::Io {
            path: tmp_path.clone(),
            source: e,
        })?;
        std::fs::rename(&tmp_path, &self.file_path).map_err(|e| SnapshotError::Io {
            path: self.file_path.clone(),
            source: e,
        })?;
        if let Some(parent) = self.file_path.parent() {
            crate::persistence::fsync::fsync_directory(parent).map_err(|e| SnapshotError::Io {
                path: parent.to_path_buf(),
                source: e,
            })?;
        }

        Ok(())
    }

    /// Check if all databases have been fully serialized.
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.current_db >= self.num_databases
    }
}

/// Synchronous all-at-once snapshot save for testing and simple use cases.
///
/// Creates a SnapshotState, runs advance_one_segment in a loop until complete,
/// then finalizes and writes to disk.
pub fn shard_snapshot_save(
    shard_id: u16,
    epoch: u64,
    databases: &[Database],
    path: &Path,
) -> Result<(), MoonError> {
    let mut state = SnapshotState::new(shard_id, epoch, databases, path.to_path_buf());
    while !state.advance_one_segment(databases) {}
    state.finalize()
}

/// Synchronous snapshot save that stamps a WAL LSN into the header.
///
/// This is the PITR-aware variant: pass `wal_flush_lsn` from the shard's
/// control file so recovery can pick this snapshot for any target_lsn
/// >= last_lsn. Equivalent to `shard_snapshot_save` for last_lsn = 0.
pub fn shard_snapshot_save_with_lsn(
    shard_id: u16,
    epoch: u64,
    last_lsn: u64,
    databases: &[Database],
    path: &Path,
) -> Result<(), MoonError> {
    let mut state = SnapshotState::new(shard_id, epoch, databases, path.to_path_buf());
    state.set_last_lsn(last_lsn);
    while !state.advance_one_segment(databases) {}
    state.finalize()
}

/// Peek at a snapshot file's header without fully loading it.
///
/// Returns the version, shard_id, epoch, last_lsn, and created_at_unix_ms.
/// Used by P3 recovery to enumerate available snapshots and pick the one
/// with the highest `last_lsn` that is still `<= target_lsn`.
///
/// Does NOT verify the global CRC32 — that's only meaningful when the full
/// payload is being loaded. Header bytes are integrity-checked by the magic
/// + version validation; corrupt headers return `Corrupted` errors.
pub fn read_snapshot_metadata(path: &Path) -> Result<SnapshotMeta, MoonError> {
    use std::io::Read;
    let mut file = std::fs::File::open(path).map_err(|e| SnapshotError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    // v2 preamble is 35 bytes — read up to that.
    let mut buf = [0u8; 35];
    let n = file.read(&mut buf).map_err(|e| SnapshotError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    if n < 19 {
        return Err(SnapshotError::Corrupted {
            detail: format!("snapshot header truncated: {} bytes", n),
        }
        .into());
    }
    if &buf[0..8] != SHARD_RDB_MAGIC {
        return Err(SnapshotError::Corrupted {
            detail: "invalid RRDSHARD magic header".into(),
        }
        .into());
    }
    let version = buf[8];
    if version != SHARD_RDB_VERSION_V1
        && version != SHARD_RDB_VERSION_V2
        && version != SHARD_RDB_VERSION_V3
    {
        return Err(SnapshotError::VersionMismatch {
            expected: SHARD_RDB_VERSION as u32,
            actual: version as u32,
        }
        .into());
    }
    let shard_id = u16::from_le_bytes([buf[9], buf[10]]);
    let epoch = u64::from_le_bytes([
        buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18],
    ]);
    // V2 and V3 share the same preamble layout (LSN + timestamp after epoch).
    let (last_lsn, created_at_unix_ms) =
        if version == SHARD_RDB_VERSION_V2 || version == SHARD_RDB_VERSION_V3 {
            if n < 35 {
                return Err(SnapshotError::Corrupted {
                    detail: format!("v2 snapshot header truncated: {} bytes", n),
                }
                .into());
            }
            let lsn = u64::from_le_bytes([
                buf[19], buf[20], buf[21], buf[22], buf[23], buf[24], buf[25], buf[26],
            ]);
            let ts = u64::from_le_bytes([
                buf[27], buf[28], buf[29], buf[30], buf[31], buf[32], buf[33], buf[34],
            ]);
            (lsn, ts)
        } else {
            (0u64, 0u64)
        };
    Ok(SnapshotMeta {
        version,
        shard_id,
        epoch,
        last_lsn,
        created_at_unix_ms,
    })
}

/// Load a per-shard snapshot file and populate databases. Returns total keys loaded.
///
/// Reads RRDSHARD format with per-segment CRC32 verification.
///
/// Global checksum mismatch is a hard error (whole file suspect).
/// Per-segment checksum mismatch and unknown tags use log+skip recovery:
/// the corrupted segment is skipped and loading continues with the next segment.
pub fn shard_snapshot_load<D: std::borrow::BorrowMut<Database>>(
    databases: &mut [D],
    path: &Path,
) -> Result<usize, MoonError> {
    let data = std::fs::read(path).map_err(|e| SnapshotError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;

    // Minimum size depends on version, but every valid file is at least 24 bytes:
    //   v1 preamble(19) + eof(1) + global_crc(4) = 24
    //   v2 preamble(35) + eof(1) + global_crc(4) = 40
    // We re-check the precise bound after reading the version byte below.
    if data.len() < 24 {
        return Err(SnapshotError::Corrupted {
            detail: "snapshot file too small".into(),
        }
        .into());
    }

    // Verify global CRC32: all bytes except last 4 vs last 4 bytes — hard error
    let (payload, checksum_bytes) = data.split_at(data.len() - 4);
    let stored_checksum = u32::from_le_bytes([
        checksum_bytes[0],
        checksum_bytes[1],
        checksum_bytes[2],
        checksum_bytes[3],
    ]);
    let mut hasher = Hasher::new();
    hasher.update(payload);
    let computed_checksum = hasher.finalize();
    if stored_checksum != computed_checksum {
        return Err(SnapshotError::Corrupted {
            detail: format!(
                "global checksum mismatch: stored={:#010x}, computed={:#010x}",
                stored_checksum, computed_checksum
            ),
        }
        .into());
    }

    let mut cursor = Cursor::new(payload);

    // Verify magic
    let mut magic = [0u8; 8];
    cursor
        .read_exact(&mut magic)
        .map_err(|e| SnapshotError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
    if &magic != SHARD_RDB_MAGIC {
        return Err(SnapshotError::Corrupted {
            detail: "invalid RRDSHARD magic header".into(),
        }
        .into());
    }

    // Verify version (accept both v1 and v2 — branch later).
    let mut version = [0u8; 1];
    cursor
        .read_exact(&mut version)
        .map_err(|e| SnapshotError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
    let on_disk_version = version[0];
    if on_disk_version != SHARD_RDB_VERSION_V1
        && on_disk_version != SHARD_RDB_VERSION_V2
        && on_disk_version != SHARD_RDB_VERSION_V3
    {
        return Err(SnapshotError::VersionMismatch {
            expected: SHARD_RDB_VERSION as u32,
            actual: on_disk_version as u32,
        }
        .into());
    }
    let has_hash_ttl_trailer = on_disk_version >= SHARD_RDB_VERSION_V3;
    // Enforce the per-version minimum file size now that we know the version.
    let min_file_size = match on_disk_version {
        SHARD_RDB_VERSION_V1 => 24, // 19 preamble + 1 eof + 4 crc
        SHARD_RDB_VERSION_V2 | SHARD_RDB_VERSION_V3 => 40, // 35 preamble + 1 eof + 4 crc
        _ => 24,
    };
    if data.len() < min_file_size {
        return Err(SnapshotError::Corrupted {
            detail: format!(
                "snapshot file too small for v{}: {} bytes < {}",
                on_disk_version,
                data.len(),
                min_file_size
            ),
        }
        .into());
    }

    // Read shard_id and epoch (for logging/verification)
    let mut shard_id_buf = [0u8; 2];
    cursor
        .read_exact(&mut shard_id_buf)
        .map_err(|e| SnapshotError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
    let _shard_id = u16::from_le_bytes(shard_id_buf);

    let mut epoch_buf = [0u8; 8];
    cursor
        .read_exact(&mut epoch_buf)
        .map_err(|e| SnapshotError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
    let _epoch = u64::from_le_bytes(epoch_buf);

    // v2 extra fields: last_lsn + created_at_unix_ms. v1 snapshots leave these
    // implicit zeros — recovery treats `last_lsn = 0` as "replay all WAL from
    // origin", which is the lossless fallback for legacy files.
    let (_last_lsn, _created_at_unix_ms) =
        if on_disk_version == SHARD_RDB_VERSION_V2 || on_disk_version == SHARD_RDB_VERSION_V3 {
            let mut lsn_buf = [0u8; 8];
            cursor
                .read_exact(&mut lsn_buf)
                .map_err(|e| SnapshotError::Io {
                    path: path.to_path_buf(),
                    source: e,
                })?;
            let mut ts_buf = [0u8; 8];
            cursor
                .read_exact(&mut ts_buf)
                .map_err(|e| SnapshotError::Io {
                    path: path.to_path_buf(),
                    source: e,
                })?;
            (u64::from_le_bytes(lsn_buf), u64::from_le_bytes(ts_buf))
        } else {
            (0u64, 0u64)
        };

    let now_ms = current_time_ms();
    let mut total_keys = 0usize;
    let mut skipped_segments = 0usize;
    let mut current_db: usize = 0;

    loop {
        let mut tag = [0u8; 1];
        if cursor.read_exact(&mut tag).is_err() {
            // Truncated tail: treat as implicit EOF
            tracing::warn!(
                "Snapshot load: truncated tail after {} keys, treating as end of file",
                total_keys
            );
            break;
        }

        match tag[0] {
            EOF_MARKER => break,
            DB_SELECTOR => {
                let mut db_idx = [0u8; 1];
                cursor
                    .read_exact(&mut db_idx)
                    .map_err(|e| SnapshotError::Io {
                        path: path.to_path_buf(),
                        source: e,
                    })?;
                current_db = db_idx[0] as usize;
                if current_db >= databases.len() {
                    return Err(SnapshotError::Corrupted {
                        detail: format!(
                            "snapshot references database {} but only {} configured",
                            current_db,
                            databases.len()
                        ),
                    }
                    .into());
                }
            }
            SEGMENT_BLOCK_MARKER => {
                // Read segment index and entry count
                let seg_idx = rdb::read_u32(&mut cursor)? as usize;
                let entry_count = rdb::read_u32(&mut cursor)?;

                // Read all entry bytes for CRC verification
                let data_start = cursor.position() as usize;

                // Bound the untrusted entry_count against remaining input before
                // allocating — a crafted/corrupt file can otherwise claim
                // u32::MAX entries and drive a multi-GB Vec::with_capacity
                // before a single byte of entry data is validated (DoS: OOM /
                // allocator abort under `panic = "abort"`). Minimum bytes a
                // single entry can consume: type_tag(1) + key_len(4) +
                // ttl(8) + value_len_or_count(4) = 17.
                rdb::validate_count(&cursor, entry_count as usize, 17, "segment_entries")?;

                // First pass: read entries, tracking bytes consumed
                let mut entries: Vec<(Bytes, Entry)> = Vec::with_capacity(entry_count as usize);
                let mut segment_parse_failed = false;
                for _ in 0..entry_count {
                    let mut type_tag = [0u8; 1];
                    if cursor.read_exact(&mut type_tag).is_err() {
                        segment_parse_failed = true;
                        break;
                    }
                    match rdb::read_entry(&mut cursor, type_tag[0], has_hash_ttl_trailer) {
                        Ok((key, entry)) => entries.push((key, entry)),
                        Err(e) => {
                            tracing::warn!(
                                "Snapshot load: entry parse error in segment {}: {}",
                                seg_idx,
                                e
                            );
                            segment_parse_failed = true;
                            break;
                        }
                    }
                }

                let data_end = cursor.position() as usize;

                // Read and verify per-segment CRC32
                let mut crc_buf = [0u8; 4];
                if cursor.read_exact(&mut crc_buf).is_err() {
                    tracing::warn!(
                        "Snapshot load: missing CRC for segment {}, skipping",
                        seg_idx
                    );
                    skipped_segments += 1;
                    continue;
                }
                let stored_crc = u32::from_le_bytes(crc_buf);

                if segment_parse_failed {
                    tracing::warn!(
                        "Snapshot load: skipping segment {} due to parse failure",
                        seg_idx
                    );
                    skipped_segments += 1;
                    continue;
                }

                let segment_data = &payload[data_start..data_end];
                let mut seg_hasher = Hasher::new();
                seg_hasher.update(segment_data);
                let computed_crc = seg_hasher.finalize();
                if stored_crc != computed_crc {
                    // Per-segment CRC mismatch: log+skip this segment, continue
                    tracing::warn!(
                        "Snapshot load: segment {} CRC mismatch (stored={:#010x}, computed={:#010x}), skipping",
                        seg_idx,
                        stored_crc,
                        computed_crc
                    );
                    skipped_segments += 1;
                    continue;
                }

                // Insert non-expired entries into the database
                for (key, entry) in entries {
                    if entry.has_expiry() && entry.is_expired_at(now_ms) {
                        continue;
                    }
                    if current_db < databases.len() {
                        databases[current_db].borrow_mut().set(&key, entry);
                        total_keys += 1;
                    }
                }
            }
            other => {
                // Unknown tag: log+skip, try to continue
                tracing::warn!(
                    "Snapshot load: unknown tag {:#04x} at offset {}, skipping",
                    other,
                    cursor.position() - 1
                );
                // Cannot reliably skip unknown-length data, break
                break;
            }
        }
    }

    if skipped_segments > 0 {
        tracing::warn!(
            "Snapshot load completed with {} segments skipped, {} keys loaded",
            skipped_segments,
            total_keys
        );
    }

    Ok(total_keys)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod stream_tests;

#[cfg(test)]
mod epoch_harness;

#[cfg(test)]
mod split_epoch_tests;

#[cfg(test)]
mod table_swap_tests;
