//! The disk half of the cold-tier reclaim, run on the shard's spill thread
//! (moon#1240): read and decode a mostly-dead spill file, and write a
//! compaction's survivors durably. Nothing here touches the cold index, the
//! ledger or the manifest — those stay on the shard thread
//! (`super::cold_reclaim`), which decides what to read, which slots survive,
//! and what a written file means.
//!
//! A compaction takes two round trips through the spill thread's reclaim
//! channel: [`ReclaimJob::Read`] -> [`ReclaimDone::Read`] (the shard filters
//! the survivors and mints the output ids) -> [`ReclaimJob::Write`] ->
//! [`ReclaimDone::Written`] (the shard records the compaction for adoption).

use std::path::{Path, PathBuf};

use bytes::Bytes;

use super::cold_index::{ColdLocation, PageVerdict, classify_page};
use crate::persistence::kv_page::{KvLeafPage, entry_flags, read_overflow_chain};
use crate::persistence::page::PAGE_4K;
use crate::storage::tiered::spill_thread::{SpillCompletion, SpillRequest, flush_buffer};

/// One slot of a spill file as read from disk.
#[derive(Debug)]
pub(crate) struct DecodedSlot {
    pub(crate) key: Bytes,
    pub(crate) page_idx: u32,
    pub(crate) slot_idx: u16,
    /// `None` when the slot's overflow chain is broken: fatal only if the
    /// slot turns out to be a survivor.
    pub(crate) value: Option<Bytes>,
}

/// Every slot of one spill file.
#[derive(Debug)]
pub(crate) struct FileSlots {
    pub(crate) slots: Vec<DecodedSlot>,
    pub(crate) bytes_read: u64,
}

/// Work for the spill thread.
pub(crate) enum ReclaimJob {
    /// Read and decode every slot of `file_id`.
    Read {
        db_index: usize,
        file_id: u64,
        shard_dir: PathBuf,
    },
    /// Write `requests` (the survivors of `old_file`, each with its fresh id)
    /// durably. `moved` pairs each request, in order, with the key and the
    /// slot it was read from.
    Write {
        db_index: usize,
        old_file: u64,
        epoch: u64,
        moved: Vec<(Bytes, ColdLocation)>,
        requests: Vec<SpillRequest>,
        shard_dir: PathBuf,
    },
}

/// What the spill thread hands back.
pub(crate) enum ReclaimDone {
    Read {
        db_index: usize,
        file_id: u64,
        result: Result<FileSlots, String>,
    },
    Written {
        db_index: usize,
        old_file: u64,
        epoch: u64,
        moved: Vec<(Bytes, ColdLocation)>,
        /// On failure every file the write produced has been removed.
        result: Result<Vec<SpillCompletion>, String>,
    },
}

pub(crate) fn heap_path(shard_dir: &Path, file_id: u64) -> PathBuf {
    shard_dir
        .join("data")
        .join(format!("heap-{file_id:06}.mpf"))
}

/// Remove an unlisted compaction output (a write that will never be
/// adopted). Best effort: a leftover is an unlisted file the startup orphan
/// sweep removes.
pub(crate) fn discard_output(shard_dir: &Path, file_id: u64) {
    if let Err(e) = std::fs::remove_file(heap_path(shard_dir, file_id))
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(
            file_id,
            err = %e,
            "cold reclaim: could not remove an unadopted compaction file; the startup \
             orphan sweep removes it"
        );
    }
}

/// Read `file_id` and decode every `KvLeaf` slot in it.
pub(crate) fn read_file_slots(shard_dir: &Path, file_id: u64) -> Result<FileSlots, String> {
    #[cfg(test)]
    note_io_thread();
    let raw = std::fs::read(heap_path(shard_dir, file_id)).map_err(|e| e.to_string())?;
    let mut slots = Vec::new();
    for (page_idx, chunk) in raw.chunks_exact(PAGE_4K).enumerate() {
        if classify_page(chunk) != PageVerdict::Leaf {
            continue;
        }
        let mut buf = [0u8; PAGE_4K];
        buf.copy_from_slice(chunk);
        let Some(page) = KvLeafPage::from_bytes(buf) else {
            continue;
        };
        for slot_idx in 0..page.slot_count() {
            let Some(kv) = page.get(slot_idx) else {
                continue;
            };
            let value = if kv.flags & entry_flags::OVERFLOW != 0 {
                kv.value
                    .get(..4)
                    .and_then(|b| <[u8; 4]>::try_from(b).ok())
                    .map(u32::from_le_bytes)
                    .and_then(|start| read_overflow_chain(&raw, start as usize))
                    .map(Bytes::from)
            } else {
                Some(Bytes::from(kv.value))
            };
            slots.push(DecodedSlot {
                key: Bytes::from(kv.key),
                page_idx: page_idx as u32,
                slot_idx,
                value,
            });
        }
    }
    Ok(FileSlots {
        slots,
        bytes_read: raw.len() as u64,
    })
}

/// Write a compaction's survivors durably through the spill writer (temp
/// file, fsync, rename, directory fsync — `flush_buffer`). On any failure the
/// files that were written are removed and nothing is returned.
pub(crate) fn write_outputs(
    mut requests: Vec<SpillRequest>,
    shard_dir: &Path,
) -> Result<Vec<SpillCompletion>, String> {
    #[cfg(test)]
    note_io_thread();
    let completions = flush_buffer(&mut requests);
    if completions.iter().any(|c| !c.success) {
        for c in completions.iter().filter(|c| c.success) {
            discard_output(shard_dir, c.file_entry.file_id);
        }
        return Err("writing the compacted file failed".to_string());
    }
    Ok(completions)
}

/// Run one job (on the spill thread).
pub(crate) fn run_job(job: ReclaimJob) -> ReclaimDone {
    match job {
        ReclaimJob::Read {
            db_index,
            file_id,
            shard_dir,
        } => ReclaimDone::Read {
            db_index,
            file_id,
            result: read_file_slots(&shard_dir, file_id),
        },
        ReclaimJob::Write {
            db_index,
            old_file,
            epoch,
            moved,
            requests,
            shard_dir,
        } => ReclaimDone::Written {
            db_index,
            old_file,
            epoch,
            moved,
            result: write_outputs(requests, &shard_dir),
        },
    }
}

#[cfg(test)]
static IO_THREADS: parking_lot::Mutex<Vec<std::thread::ThreadId>> =
    parking_lot::Mutex::new(Vec::new());

#[cfg(test)]
fn note_io_thread() {
    IO_THREADS.lock().push(std::thread::current().id());
}

/// Test-only: whether `thread` ran a reclaim read or write.
#[cfg(test)]
pub(crate) fn ran_reclaim_io(thread: std::thread::ThreadId) -> bool {
    IO_THREADS.lock().contains(&thread)
}
