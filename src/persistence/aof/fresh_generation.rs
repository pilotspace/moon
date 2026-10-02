//! A fresh legacy single-file AOF generation carries the keyspace it was
//! opened over (R2b review P1).
//!
//! `appendonly.aof` holding a record is the ONLY KV source of a boot
//! (`recovery::KvSources::AofOnly`, redis's `appendonly yes` rule): a
//! snapshot saved while the file was being appended holds a prefix of its
//! records, and loading both applied that prefix twice. That rule needs the
//! file to be the complete history, so a generation opened at a boot whose
//! keyspace is not empty — loaded from a snapshot because no AOF held a record
//! (the `--appendonly no` -> `yes` switch, a removed AOF, a runtime switch
//! from a manifest layout) — must start with that keyspace. It does so in the
//! shape a rewrite publishes: an RDB preamble, then the generation head
//! (`MOON.COLDCUT` + its `DEL`s) when the caller writes one.
//!
//! The file is published by rename, so a crash never leaves a torn preamble
//! (one would make every later boot fail to load the AOF). The writer opens
//! the same path with `O_APPEND` when it starts, which is BEFORE recovery
//! runs, so a rename after that would leave it appending to the unlinked old
//! inode: [`hold_writer_open`] keeps the tokio TopLevel writer from opening
//! the file until the guard is dropped.

use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::persistence::cold_records::ColdDeletes;

/// A count of boots that may still publish a fresh generation by rename.
pub struct OpenGate(AtomicUsize);

impl OpenGate {
    pub const fn new() -> Self {
        Self(AtomicUsize::new(0))
    }

    /// Whether a writer may open its file now.
    pub fn is_open(&self) -> bool {
        self.0.load(Ordering::Acquire) == 0
    }

    /// Close the gate until the returned guard is dropped.
    pub fn hold(&'static self) -> OpenGateGuard {
        self.0.fetch_add(1, Ordering::AcqRel);
        OpenGateGuard(self)
    }
}

impl Default for OpenGate {
    fn default() -> Self {
        Self::new()
    }
}

/// Re-opens the gate when dropped — on every exit path of the boot.
pub struct OpenGateGuard(&'static OpenGate);

impl OpenGateGuard {
    /// Never re-open the gate: the boot refused to start (R2b round 2 F1),
    /// and the writer must not open — nor, at its stop, append to — the file
    /// the boot could not read. It exits at cancellation without opening.
    pub fn keep_closed(self) {
        std::mem::forget(self);
    }
}

impl Drop for OpenGateGuard {
    fn drop(&mut self) {
        self.0.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The gate the tokio TopLevel AOF writer waits on before it opens its file.
static WRITER_OPEN_GATE: OpenGate = OpenGate::new();

/// Keep the tokio TopLevel AOF writer from opening its file until the guard
/// is dropped (after [`open_fresh_flat_generation`]).
pub fn hold_writer_open() -> OpenGateGuard {
    WRITER_OPEN_GATE.hold()
}

/// Wait until no boot holds the gate. `false` when `cancel` fired first (the
/// writer then exits without opening anything, as it would have stopped).
#[cfg(feature = "runtime-tokio")]
pub async fn wait_writer_open(cancel: &crate::runtime::cancel::CancellationToken) -> bool {
    while !WRITER_OPEN_GATE.is_open() {
        if cancel.is_cancelled() {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    true
}

/// What [`open_fresh_flat_generation`] did.
#[derive(Debug, PartialEq, Eq)]
pub enum FreshGeneration {
    /// The file already holds a record: nothing written.
    Existing,
    /// Empty keyspace: only the head (when one was asked for) was written.
    HeadOnly,
    /// The keyspace was published as the generation's RDB preamble
    /// (`bytes` of it), followed by the head when one was asked for.
    WithBase { bytes: usize },
}

/// Open the generation at `path` when it holds no record yet (absent or
/// empty). `base` serializes the boot's keyspace — `None` when it is empty —
/// and is only called for a fresh file, so a boot that replayed the AOF pays
/// nothing. `head` is the `MOON.COLDCUT` watermark and `DEL`s to write after
/// the preamble (tokio `--shards 1`); the embedded server writes none.
///
/// With a base the generation is written to a temporary file, fsynced and
/// renamed over `path` (directory fsynced), so it exists whole or not at
/// all; the caller must hold [`hold_writer_open`] when a writer appends to
/// `path`. Without one the head is appended in place, as before.
pub fn open_fresh_flat_generation(
    path: &Path,
    base: impl FnOnce() -> std::io::Result<Option<Vec<u8>>>,
    head: Option<(u64, ColdDeletes)>,
) -> std::io::Result<FreshGeneration> {
    match std::fs::metadata(path) {
        Ok(meta) if meta.len() > 0 => return Ok(FreshGeneration::Existing),
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let Some(rdb) = base()? else {
        if let Some((watermark, deletes)) = head {
            crate::persistence::cold_records::seed_generation_head_if_fresh(
                path, watermark, deletes,
            )?;
        }
        return Ok(FreshGeneration::HeadOnly);
    };
    let mut tmp_name = path.as_os_str().to_owned();
    tmp_name.push(".fresh.tmp");
    let tmp = std::path::PathBuf::from(tmp_name);
    let written = (|| {
        let mut file = std::fs::File::create(&tmp)?;
        {
            let mut out = std::io::BufWriter::new(&mut file);
            out.write_all(&rdb)?;
            if let Some((watermark, deletes)) = head {
                crate::persistence::cold_records::write_generation_head_to(
                    &mut out, watermark, deletes, false,
                )?;
            }
            out.flush()?;
        }
        file.sync_all()?;
        std::fs::rename(&tmp, path)
    })();
    if let Err(e) = written {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        crate::persistence::fsync::fsync_directory(parent)?;
    }
    Ok(FreshGeneration::WithBase { bytes: rdb.len() })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::replay::DispatchReplayEngine;
    use crate::storage::{Database, Entry};
    use bytes::Bytes;

    fn keyspace() -> Vec<Database> {
        let mut dbs = vec![Database::new()];
        dbs[0].set(b"k", Entry::new_string(Bytes::from_static(b"v")));
        dbs
    }

    #[test]
    fn a_fresh_file_gets_the_keyspace_as_its_preamble_then_the_head() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("appendonly.aof");
        let dbs = keyspace();
        let out = open_fresh_flat_generation(
            &path,
            || {
                crate::persistence::rdb::save_to_bytes(&dbs)
                    .map(Some)
                    .map_err(std::io::Error::other)
            },
            Some((7, ColdDeletes::default())),
        )
        .unwrap();
        assert!(matches!(out, FreshGeneration::WithBase { .. }), "{out:?}");
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(&bytes[..4], b"MOON", "the preamble opens the file");
        assert!(
            bytes.windows(12).any(|w| w == b"MOON.COLDCUT"),
            "the head follows the preamble"
        );
        assert!(!tmp.path().join("appendonly.aof.fresh.tmp").exists());

        // The generation alone rebuilds the keyspace it was opened over.
        let mut back = vec![Database::new()];
        crate::persistence::aof::replay_aof(&mut back, &path, &DispatchReplayEngine::new())
            .unwrap();
        assert!(back[0].get(b"k").is_some());
    }

    #[test]
    fn a_file_with_a_record_is_left_alone_and_base_is_not_serialized() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("appendonly.aof");
        std::fs::write(&path, b"*1\r\n$4\r\nPING\r\n").unwrap();
        let out = open_fresh_flat_generation(
            &path,
            || panic!("a boot that replayed the AOF must not serialize its keyspace"),
            Some((7, ColdDeletes::default())),
        )
        .unwrap();
        assert_eq!(out, FreshGeneration::Existing);
        assert_eq!(std::fs::read(&path).unwrap(), b"*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn an_empty_keyspace_writes_the_head_only() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("appendonly.aof");
        std::fs::write(&path, b"").unwrap();
        let out = open_fresh_flat_generation(&path, || Ok(None), Some((3, ColdDeletes::default())))
            .unwrap();
        assert_eq!(out, FreshGeneration::HeadOnly);
        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.starts_with(b"*2\r\n$12\r\nMOON.COLDCUT"));
        // No head asked for (embedded): nothing written.
        let other = tmp.path().join("other.aof");
        let out = open_fresh_flat_generation(&other, || Ok(None), None).unwrap();
        assert_eq!(out, FreshGeneration::HeadOnly);
        assert!(!other.exists());
    }

    #[test]
    fn the_gate_closes_while_any_guard_lives() {
        static GATE: OpenGate = OpenGate::new();
        assert!(GATE.is_open());
        let a = GATE.hold();
        let b = GATE.hold();
        assert!(!GATE.is_open());
        drop(a);
        assert!(!GATE.is_open());
        drop(b);
        assert!(GATE.is_open());
    }
}
