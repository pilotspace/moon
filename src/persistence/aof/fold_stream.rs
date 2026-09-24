//! Streaming base image for the AOF rewrite fold (moon#1185).
//!
//! The fold's cooperative snapshot (`ShardMessage::AofFold`) used to deep-copy
//! every live entry of the shard — `(key.clone(), entry.clone())`, a malloc
//! and memcpy per heap key or value and a full clone of every collection — on
//! the shard thread, hand the copy to the AOF writer, and let the writer
//! serialize it into ONE growing `Vec` (`rdb::save_snapshot_to_bytes`). Peak
//! memory was the live data plus the clone plus the doubling image: ~2.5-3x
//! the shard's data, none of it visible to `used_memory`.
//!
//! Now the shard serializes each live entry straight from the keyspace into
//! the RDB byte stream ([`crate::persistence::rdb::RdbStreamWriter`], CRC
//! computed as the bytes pass) and ships it to the writer in chunks of
//! [`FOLD_CHUNK_BYTES`] as they fill. The writer appends the chunks to the
//! new base file as they arrive ([`write_fold_image`]). No per-entry copy and
//! no doubling `Vec`, but the image in flight is NOT bounded (moon#1221 review
//! R4): the channel is unbounded, the shard serializes the whole image inside
//! the `AofFold` arm with no backpressure, and every writer path starts
//! reading only after its phase-3 drain and fsync (and, per shard, the
//! manifest's staging lock). The chunk SIZE is bounded; how many chunks queue
//! is whatever the writer has not yet written — on a slow disk up to one
//! whole serialized image per shard, held in memory `used_memory` does not
//! count. See [`fold_image_channel`] for why the channel is not bounded.
//!
//! The exactly-once contract (#455, C4) is untouched: the image is serialized
//! inside the same `AofFold` arm, from the same keyspace instant, as the
//! `pending_aof_count` / `fold_epoch` / cold-watermark cuts the reply carries —
//! no command runs on the shard in between. Only where the bytes are produced
//! moved; which state they describe did not.

use std::io::Write;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tracing::warn;

use crate::error::{AofError, MoonError};
use crate::persistence::cold_records::ColdDeletes;
use crate::persistence::rdb::RdbStreamWriter;
use crate::storage::db::Database;

/// Size of one chunk of the base image in flight between the shard and its
/// AOF writer.
pub const FOLD_CHUNK_BYTES: usize = 1 << 20;

/// One message of a fold's base-image stream.
#[derive(Debug)]
pub enum FoldChunk {
    /// The next bytes of the base RDB image, in order.
    Data(Bytes),
    /// Sent once, after the last `Data` and before `End`, when the fold found
    /// dead cold slots (moon#1215): the keys the new generation's head must
    /// delete. Absent means none.
    ColdDeletes(ColdDeletes),
    /// The image is complete: the preceding `Data` chunks end with the EOF
    /// marker and CRC32 footer.
    End,
    /// Serialization failed on the shard; the fold must abort (the old
    /// generation stays authoritative).
    Failed(String),
}

/// Writer-side handle on a fold's base image, carried by
/// `AofFoldSnapshot::image`.
#[derive(Debug)]
pub struct FoldImage {
    rx: flume::Receiver<FoldChunk>,
}

/// Shard-side producer of a fold's base image: an `io::Write` that ships
/// [`FOLD_CHUNK_BYTES`] chunks as they fill.
pub struct FoldImageSink {
    tx: flume::Sender<FoldChunk>,
    buf: Vec<u8>,
    closed: bool,
}

/// A connected (sink, image) pair.
///
/// The channel is unbounded on purpose — and so is the memory in flight, up
/// to one serialized image (moon#1221 review R4). A bounded channel would
/// make [`FoldImageSink::write`] block the shard's event loop inside the
/// `AofFold` arm until the writer drains it, and the writer drains only
/// after its phase-3 drain + fsync (and, per shard, the coordinator's
/// manifest lock): the shard stall would grow from the serialization's CPU
/// time to the writer's disk time, and the arm cannot yield instead — the
/// image must describe one instant, with no command run in between (#455).
///
/// TODO(moon#1185 follow-up: incremental COW fold): bound the in-flight
/// image by serializing segment by segment across ticks, with the COW
/// pre-image capture BGSAVE uses; that needs moon#1216 / moon#1217 first.
pub fn fold_image_channel() -> (FoldImageSink, FoldImage) {
    let (tx, rx) = flume::unbounded();
    (
        FoldImageSink {
            tx,
            buf: Vec::new(),
            closed: false,
        },
        FoldImage { rx },
    )
}

impl FoldImageSink {
    fn ship(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let chunk = std::mem::take(&mut self.buf);
        if self.tx.send(FoldChunk::Data(Bytes::from(chunk))).is_err() {
            // The writer dropped the image (fold aborted): stop producing.
            self.closed = true;
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "fold image receiver dropped",
            ));
        }
        Ok(())
    }

    /// Ship the tail, then the fold's cold deletes (if any), and mark the
    /// image complete.
    fn end(mut self, cold_deletes: ColdDeletes) {
        if self.ship().is_err() {
            return;
        }
        if !cold_deletes.is_empty() && self.tx.send(FoldChunk::ColdDeletes(cold_deletes)).is_err() {
            return;
        }
        let _ = self.tx.send(FoldChunk::End);
    }

    /// Mark the image failed; the writer aborts the fold.
    fn fail(self, why: String) {
        let _ = self.tx.send(FoldChunk::Failed(why));
    }
}

impl Write for FoldImageSink {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if self.closed {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "fold image receiver dropped",
            ));
        }
        if self.buf.capacity() == 0 {
            self.buf.reserve(FOLD_CHUNK_BYTES.max(data.len()));
        }
        self.buf.extend_from_slice(data);
        if self.buf.len() >= FOLD_CHUNK_BYTES {
            self.ship()?;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.ship()
    }
}

/// Serialize the fold's base image straight from the live keyspace into
/// `sink` — on the shard thread, inside the `AofFold` arm (moon#1185).
///
/// `dbs` is every database of the shard, in index order; entries expired at
/// `now_ms` (the fold instant) are skipped, as the old cloning capture did.
/// Always terminates the stream: `End` on success, `Failed` on an encode
/// error (the writer aborts the fold). A writer that already dropped the
/// image stops the serialization at the next chunk boundary.
///
/// The image is the keyspace at the fold instant, and that includes every
/// key whose spill is IN FLIGHT (moon#1223): eviction has removed its hot
/// copy and its value lives only in the in-flight plane until the spill
/// completion publishes it. Streaming `db.data()` alone left such a key out
/// of the base, and every way its spill can end without publishing after the
/// fold — the `MOON.SPILLED` marker refused under AOF backpressure (the
/// moon#1202 withdraw), a failed pwrite, a re-issued file id (moon#893) —
/// puts it back in RAM with no log record: after the fold committed it was
/// in no durable artifact at all. See [`write_in_flight_entries`].
///
/// It also decides which keys the new generation must DELETE (moon#1215,
/// [`cold_deletes_of`]): shipped as [`FoldChunk::ColdDeletes`] and written
/// by the writer right after the new incr's `MOON.COLDCUT`.
pub fn stream_fold_image(dbs: &[&Database], now_ms: u64, mut sink: FoldImageSink) {
    let mut cold_deletes = ColdDeletes::default();
    let result = (|| -> Result<(), MoonError> {
        let mut w = RdbStreamWriter::new(&mut sink)?;
        for (db_idx, db) in dbs.iter().enumerate() {
            let cold = db.cold_index.as_ref().filter(|ci| ci.len() > 0);
            // Keys the base drops as expired whose stale cold shadow would
            // otherwise come back as their value (moon#1215).
            let mut expired_shadows: Vec<Bytes> = Vec::new();
            for (key, entry) in db.data().iter() {
                if entry.is_expired_at(now_ms) {
                    if cold.is_some_and(|ci| ci.lookup(key.as_bytes()).is_some()) {
                        expired_shadows.push(Bytes::copy_from_slice(key.as_bytes()));
                    }
                    continue;
                }
                w.write_entry(db_idx, key.as_bytes(), entry)?;
            }
            // Same db, right after its hot entries: the writer needs one
            // database's entries contiguous.
            write_in_flight_entries(&mut w, db_idx, db, now_ms)?;
            let dead = cold_deletes_of(db, now_ms, expired_shadows);
            if !dead.is_empty() {
                cold_deletes.per_db.push((db_idx, dead));
            }
        }
        w.finish()?;
        Ok(())
    })();
    match result {
        Ok(()) => sink.end(cold_deletes),
        Err(e) => sink.fail(e.to_string()),
    }
}

/// The keys of `db` a new AOF generation cut at `now_ms` must delete right
/// after its `MOON.COLDCUT` (moon#1215).
///
/// A key qualifies when it has a slot on disk that recovery would index —
/// every key in the cold index's dead-slot ledger, plus `expired_shadows`
/// (hot keys the base drops as expired whose cold entry is a stale shadow) —
/// and it is NOT alive at the fold instant by the base's own rules:
/// - hot and not expired: in the base, which wins over any cold slot;
/// - in flight and not expired: in the base ([`write_in_flight_entries`]);
///   one whose payload does not rehydrate is also left alone, since its
///   published spill file is then its only copy;
/// - in the cold index (not hot): its entry is its newest slot, which the
///   rebuild picks, and that slot's own TTL governs it.
///
/// Everything else is dead at the fold instant, and without a `DEL` in the
/// new generation the rebuild would re-index one of its old slots and the
/// cut would authorize it: a deleted, flushed or expired key back from the
/// dead. The `DEL` removes whatever the rebuild indexed for the key (older
/// copies included), and anything the key becomes after the fold is in the
/// generation's own records.
pub(crate) fn cold_deletes_of(
    db: &Database,
    now_ms: u64,
    expired_shadows: Vec<Bytes>,
) -> Vec<Bytes> {
    let Some(ci) = db.cold_index.as_ref() else {
        return Vec::new();
    };
    if ci.dead_slots().is_empty() && expired_shadows.is_empty() {
        return Vec::new();
    }
    let alive = |key: &[u8]| {
        if let Some(entry) = db.data().get(key) {
            return !entry.is_expired_at(now_ms);
        }
        db.spill_inflight_alive(key, now_ms) || ci.lookup(key).is_some()
    };
    // No dedupe here: this runs on the shard thread inside the `AofFold` arm,
    // where every nanosecond per ledger key is stall. A key dead in several
    // files (or also an expired shadow) is listed more than once, and
    // `generation_head` dedupes on the writer thread.
    let mut dead = expired_shadows;
    dead.reserve(ci.dead_slots().len());
    for key in ci.dead_slots().keys() {
        if !alive(key) {
            dead.push(key.clone());
        }
    }
    dead
}

/// In-flight spill payloads a fold base image could not carry because they
/// did not rehydrate (moon#1223). Each is a key whose only copy is its spill
/// file, IF that file publishes; each is logged when counted.
pub static FOLD_IN_FLIGHT_UNENCODABLE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Append db `db_idx`'s in-flight spill payloads to the base image, each as
/// the key's value at the fold instant (moon#1223).
///
/// Every outcome of the spill after the fold is then value-correct: a
/// completion that publishes WITH its marker logs the marker in the new
/// generation, whose replay drops this base copy and keeps the key cold (the
/// restart-as-cold shape, unchanged); one that is withdrawn, fails its
/// pwrite or is refused its file id puts the key back in RAM with exactly
/// this value; a DEL or overwrite after the fold is in the new generation's
/// records and replays on top of it. The spill path itself is untouched.
///
/// Skipped: a key that is also hot (the hot copy is newer — `set` retires the
/// in-flight record, so this is defensive), and a payload expired at
/// `now_ms` (the base's own filter). A payload that does not rehydrate (it
/// cannot be produced by `build_spill_payload`; memory corruption) cannot be
/// written; it is counted in [`FOLD_IN_FLIGHT_UNENCODABLE`] and logged,
/// never silently dropped.
fn write_in_flight_entries<W: Write>(
    w: &mut RdbStreamWriter<W>,
    db_idx: usize,
    db: &Database,
    now_ms: u64,
) -> Result<(), MoonError> {
    for_each_in_flight_base_entry(db, db_idx, now_ms, |key, entry| {
        w.write_entry(db_idx, key, &entry)
    })
}

/// Visit every in-flight spill payload of `db` that belongs in a fold base
/// taken at `now_ms`, rehydrated — the selection [`write_in_flight_entries`]
/// documents, shared with the legacy cloning fold (`do_rewrite_single`).
pub(crate) fn for_each_in_flight_base_entry(
    db: &Database,
    db_idx: usize,
    now_ms: u64,
    mut visit: impl FnMut(&Bytes, crate::storage::entry::Entry) -> Result<(), MoonError>,
) -> Result<(), MoonError> {
    if db.spill_inflight_is_empty() {
        return Ok(());
    }
    for key in db.spill_inflight_keys() {
        if db.data().get(key.as_ref()).is_some() {
            continue;
        }
        match db.spill_inflight_entry(key, now_ms) {
            Some(entry) => visit(key, entry)?,
            None if db.spill_inflight_alive(key, now_ms) => {
                FOLD_IN_FLIGHT_UNENCODABLE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::error!(
                    db = db_idx,
                    key_len = key.len(),
                    "AOF rewrite fold: an in-flight spill payload does not rehydrate, so the new \
                     base cannot carry the key; its spill file is its only copy if it publishes \
                     (moon#1223)"
                );
            }
            // Expired at the fold instant: absent from the base, like a hot
            // key expired at the same instant.
            None => {}
        }
    }
    Ok(())
}

/// [`cold_deletes_of`] for every database, with its own pass for the
/// expired shadows — for a fold that does not stream its base through
/// [`stream_fold_image`] (the legacy cloning `do_rewrite_single`).
// Only the monoio-only `do_rewrite_single` folds without streaming.
#[cfg_attr(not(feature = "runtime-monoio"), allow(dead_code))]
pub(crate) fn fold_cold_deletes(dbs: &[&Database], now_ms: u64) -> ColdDeletes {
    let mut out = ColdDeletes::default();
    for (db_idx, db) in dbs.iter().enumerate() {
        let Some(ci) = db
            .cold_index
            .as_ref()
            .filter(|ci| ci.len() > 0 || !ci.dead_slots().is_empty())
        else {
            continue;
        };
        let expired_shadows: Vec<Bytes> = db
            .data()
            .iter()
            .filter(|(key, entry)| {
                entry.is_expired_at(now_ms) && ci.lookup(key.as_bytes()).is_some()
            })
            .map(|(key, _)| Bytes::copy_from_slice(key.as_bytes()))
            .collect();
        let dead = cold_deletes_of(db, now_ms, expired_shadows);
        if !dead.is_empty() {
            out.per_db.push((db_idx, dead));
        }
    }
    out
}

/// How often a writer waiting on a slow image logs that it is still waiting.
const FOLD_IMAGE_WAIT_WARN: Duration = Duration::from_secs(5);

/// Append a fold's base image to `out` as its chunks arrive; returns the
/// image length and the keys the new generation's head must delete
/// (moon#1215 — empty when the fold found none). Blocks until the shard ends
/// the stream (a slow shard is logged, never abandoned — as the
/// snapshot-reply wait was). `Err` when the shard reports a serialization
/// failure or drops the stream unfinished, or on a write error: the caller
/// aborts the fold and the old generation stays committed.
pub fn write_fold_image(
    image: FoldImage,
    out: &mut impl Write,
    what: &str,
) -> Result<(u64, ColdDeletes), MoonError> {
    let started = Instant::now();
    let mut written = 0u64;
    let mut cold_deletes = ColdDeletes::default();
    loop {
        match image.rx.recv_timeout(FOLD_IMAGE_WAIT_WARN) {
            Ok(FoldChunk::Data(chunk)) => {
                out.write_all(&chunk)?;
                written += chunk.len() as u64;
            }
            Ok(FoldChunk::ColdDeletes(d)) => cold_deletes = d,
            Ok(FoldChunk::End) => return Ok((written, cold_deletes)),
            Ok(FoldChunk::Failed(why)) => {
                return Err(AofError::RewriteFailed {
                    detail: format!("{what}: base image serialization failed: {why}"),
                }
                .into());
            }
            Err(flume::RecvTimeoutError::Timeout) => {
                warn!(
                    "{what}: still waiting for the fold base image ({} bytes after {:.1}s)",
                    written,
                    started.elapsed().as_secs_f64()
                );
            }
            Err(flume::RecvTimeoutError::Disconnected) => {
                return Err(AofError::RewriteFailed {
                    detail: format!("{what}: fold base image stream ended unfinished"),
                }
                .into());
            }
        }
    }
}

/// Create `path`, append the fold's base image as it streams in, and fsync
/// it; returns what [`write_fold_image`] returns. On any failure the partial
/// file is removed and the error returned (the caller aborts the fold).
pub(crate) fn write_fold_image_file(
    path: &std::path::Path,
    image: FoldImage,
    what: &str,
) -> Result<(u64, ColdDeletes), MoonError> {
    let result = (|| -> Result<(u64, ColdDeletes), MoonError> {
        let mut f = std::fs::File::create(path).map_err(|e| AofError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        let n = write_fold_image(image, &mut f, what)?;
        f.sync_data().map_err(|e| AofError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        Ok(n)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(path);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::compact_key::CompactKey;
    use crate::storage::entry::Entry;

    fn fixture() -> Vec<Database> {
        let mut dbs: Vec<Database> = (0..4).map(|_| Database::new()).collect();
        for i in 0..3000u32 {
            let key = format!("key:{i:06}");
            // Values on both sides of the inline/heap boundary, and one
            // bigger than a chunk to force a chunk-straddling entry.
            let val = if i == 1500 {
                vec![b'Q'; FOLD_CHUNK_BYTES + 17]
            } else {
                format!("value-{}", "x".repeat((i % 40) as usize)).into_bytes()
            };
            dbs[0].set_string(key.as_bytes(), Bytes::from(val));
        }
        // db 1 empty; db 2 mixed types; db 3 a single key.
        dbs[2].set_string(b"s", Bytes::from_static(b"v"));
        let mut map = std::collections::HashMap::new();
        map.insert(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"));
        map.insert(Bytes::from_static(b"f2"), Bytes::from_static(b"v2"));
        let mut h = Entry::new_hash();
        if let Some(rv) = h.redis_value_mut() {
            *rv = crate::storage::entry::RedisValue::Hash(Box::new(map));
        }
        dbs[2].set(b"h", h);
        dbs[3].set_string(b"only", Bytes::from_static(b"one"));
        dbs
    }

    fn cloned(dbs: &[Database], now_ms: u64) -> Vec<Vec<(CompactKey, Entry)>> {
        dbs.iter()
            .map(|db| {
                db.data()
                    .iter()
                    .filter(|(_, e)| !e.is_expired_at(now_ms))
                    .map(|(k, e)| (k.clone(), e.clone()))
                    .collect()
            })
            .collect()
    }

    /// The streamed image is byte-identical to the image HEAD built from the
    /// deep-cloned snapshot with `save_snapshot_to_bytes` (moon#1185).
    #[test]
    fn streamed_image_is_byte_identical_to_the_cloned_snapshot_image() {
        let dbs = fixture();
        let now = crate::storage::entry::current_time_ms();
        let expected = crate::persistence::rdb::save_snapshot_to_bytes(&cloned(&dbs, now))
            .expect("head image");

        let (sink, image) = fold_image_channel();
        let refs: Vec<&Database> = dbs.iter().collect();
        stream_fold_image(&refs, now, sink);
        let mut out = Vec::new();
        let (n, deletes) = write_fold_image(image, &mut out, "test").expect("image");
        assert_eq!(n as usize, out.len());
        assert!(deletes.is_empty(), "no cold tier, nothing to delete");
        assert_eq!(out, expected);

        // And it loads back to the same dataset.
        let mut loaded: Vec<Database> = (0..4).map(|_| Database::new()).collect();
        crate::persistence::rdb::load_from_bytes(&mut loaded, &out).expect("load");
        for (a, b) in dbs.iter().zip(loaded.iter()) {
            assert_eq!(a.len(), b.len());
        }
    }

    /// Each chunk is bounded in size: every `Data` chunk but the last is at
    /// least a chunk and at most a chunk plus one entry. (How MANY chunks can
    /// be in flight is not bounded — see `fold_image_channel`.)
    #[test]
    fn image_ships_in_bounded_chunks() {
        let dbs = fixture();
        let (sink, image) = fold_image_channel();
        let refs: Vec<&Database> = dbs.iter().collect();
        stream_fold_image(&refs, 0, sink);
        let mut sizes = Vec::new();
        loop {
            match image.rx.recv().expect("stream") {
                FoldChunk::Data(c) => sizes.push(c.len()),
                FoldChunk::ColdDeletes(d) => panic!("no cold tier, yet deletes: {d:?}"),
                FoldChunk::End => break,
                FoldChunk::Failed(w) => panic!("failed: {w}"),
            }
        }
        assert!(sizes.len() >= 2, "fixture must span several chunks");
        for s in &sizes[..sizes.len() - 1] {
            assert!(
                *s >= FOLD_CHUNK_BYTES && *s <= 2 * FOLD_CHUNK_BYTES + 64,
                "{s}"
            );
        }
    }

    /// A writer that drops the image stops the shard's serialization
    /// instead of letting it buffer the rest of the keyspace.
    #[test]
    fn dropped_image_stops_serialization() {
        let dbs = fixture();
        let (sink, image) = fold_image_channel();
        drop(image);
        let refs: Vec<&Database> = dbs.iter().collect();
        // Must return promptly and not panic.
        stream_fold_image(&refs, 0, sink);
    }

    /// A stream that ends without `End` (shard gone) aborts the fold.
    #[test]
    fn unfinished_stream_is_an_error() {
        let (mut sink, image) = fold_image_channel();
        sink.write_all(b"MOON").unwrap();
        sink.flush().unwrap();
        drop(sink);
        let mut out = Vec::new();
        assert!(write_fold_image(image, &mut out, "test").is_err());
    }

    fn in_flight(db: &mut Database, key: &'static [u8], value: &'static [u8], ttl: Option<u64>) {
        db.spill_inflight_mark(
            Bytes::from_static(key),
            crate::storage::db::PendingSpill {
                req_id: 7,
                value_type: crate::persistence::kv_page::ValueType::String,
                value_bytes: Bytes::from_static(value),
                ttl_ms: ttl,
            },
        );
    }

    fn image_of(dbs: &[Database], now_ms: u64) -> Vec<Database> {
        let (sink, image) = fold_image_channel();
        let refs: Vec<&Database> = dbs.iter().collect();
        stream_fold_image(&refs, now_ms, sink);
        let mut out = Vec::new();
        write_fold_image(image, &mut out, "test").expect("image");
        let mut loaded: Vec<Database> = (0..dbs.len()).map(|_| Database::new()).collect();
        crate::persistence::rdb::load_from_bytes(&mut loaded, &out).expect("load");
        loaded
    }

    fn string_at(db: &Database, key: &[u8]) -> Option<Vec<u8>> {
        db.data()
            .get(key)
            .and_then(|e| e.value.as_bytes().map(|b| b.to_vec()))
    }

    /// moon#1223: a key whose spill is in flight at the fold instant is part
    /// of the keyspace; the base carries it (in its own db, TTL kept), next
    /// to the hot keys.
    #[test]
    fn in_flight_spills_are_part_of_the_base() {
        let mut dbs: Vec<Database> = (0..3).map(|_| Database::new()).collect();
        dbs[0].set_string(b"hot", Bytes::from_static(b"h"));
        in_flight(&mut dbs[0], b"flying", b"f0", None);
        in_flight(&mut dbs[2], b"flying2", b"f2", Some(u64::MAX / 2));
        let loaded = image_of(&dbs, 1_000);
        assert_eq!(string_at(&loaded[0], b"hot").as_deref(), Some(&b"h"[..]));
        assert_eq!(
            string_at(&loaded[0], b"flying").as_deref(),
            Some(&b"f0"[..])
        );
        assert_eq!(
            string_at(&loaded[2], b"flying2").as_deref(),
            Some(&b"f2"[..])
        );
        assert_eq!(
            loaded[2]
                .data()
                .get(b"flying2".as_slice())
                .map(|e| e.expires_at_ms()),
            Some(u64::MAX / 2),
            "the payload's TTL rides along"
        );
        assert!(loaded[1].data().is_empty());
    }

    /// An in-flight payload expired at the fold instant is left out, like a
    /// hot key expired at the same instant.
    #[test]
    fn expired_in_flight_spills_are_left_out() {
        // Real-clock deadlines: loading the image also drops entries that
        // are expired NOW, so the kept one must outlive the test.
        let now = crate::storage::entry::current_time_ms();
        let mut db = Database::new();
        in_flight(&mut db, b"gone", b"x", Some(now - 1));
        in_flight(&mut db, b"kept", b"y", Some(now + 3_600_000));
        let (sink, image) = fold_image_channel();
        stream_fold_image(&[&db], now, sink);
        let mut out = Vec::new();
        write_fold_image(image, &mut out, "test").expect("image");
        assert!(
            !out.windows(4).any(|w| w == b"gone"),
            "an in-flight payload expired at the fold instant must not be written"
        );
        let mut loaded = vec![Database::new()];
        crate::persistence::rdb::load_from_bytes(&mut loaded, &out).expect("load");
        assert_eq!(string_at(&loaded[0], b"kept").as_deref(), Some(&b"y"[..]));
    }

    /// A payload that does not rehydrate cannot be encoded: counted and
    /// logged, and the rest of the image is still produced.
    #[test]
    fn an_undecodable_in_flight_payload_is_counted_not_fatal() {
        let mut dbs = vec![Database::new()];
        dbs[0].spill_inflight_mark(
            Bytes::from_static(b"broken"),
            crate::storage::db::PendingSpill {
                req_id: 1,
                value_type: crate::persistence::kv_page::ValueType::Hash,
                value_bytes: Bytes::from_static(b"\xff\xff not a hash body"),
                ttl_ms: None,
            },
        );
        in_flight(&mut dbs[0], b"fine", b"ok", None);
        let before = FOLD_IN_FLIGHT_UNENCODABLE.load(std::sync::atomic::Ordering::Relaxed);
        let loaded = image_of(&dbs, 0);
        assert!(
            FOLD_IN_FLIGHT_UNENCODABLE.load(std::sync::atomic::Ordering::Relaxed) > before,
            "the unencodable payload must be counted"
        );
        assert_eq!(string_at(&loaded[0], b"broken"), None);
        assert_eq!(string_at(&loaded[0], b"fine").as_deref(), Some(&b"ok"[..]));
    }

    fn cold_loc(file_id: u64) -> crate::storage::tiered::cold_index::ColdLocation {
        crate::storage::tiered::cold_index::ColdLocation {
            file_id,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: None,
            value_type: crate::persistence::kv_page::ValueType::String,
        }
    }

    fn deletes_of(dbs: &[Database], now_ms: u64) -> ColdDeletes {
        let (sink, image) = fold_image_channel();
        let refs: Vec<&Database> = dbs.iter().collect();
        stream_fold_image(&refs, now_ms, sink);
        let mut out = Vec::new();
        write_fold_image(image, &mut out, "test").expect("image").1
    }

    fn sorted(keys: &[Bytes]) -> Vec<&[u8]> {
        let mut v: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
        v.sort_unstable();
        v
    }

    /// moon#1215: which keys with a dead cold slot the fold deletes — only
    /// those not alive at the fold instant by the base's own rules.
    #[test]
    fn the_fold_deletes_exactly_the_keys_dead_at_its_instant() {
        let now = 1_000_000u64;
        let mut db = Database::new();
        let mut ci = crate::storage::tiered::cold_index::ColdIndex::new();
        for k in [
            &b"deleted"[..],
            b"rehot",
            b"rehot_expired",
            b"flying",
            b"flying_expired",
            b"respilled",
            b"twice",
        ] {
            ci.insert(Bytes::copy_from_slice(k), cold_loc(1));
        }
        for k in [
            &b"deleted"[..],
            b"rehot",
            b"rehot_expired",
            b"flying",
            b"flying_expired",
            b"twice",
        ] {
            ci.remove(k);
        }
        // `respilled` moved to file 2 (alive there); `twice` was also dead in
        // file 3 — one DEL.
        ci.insert(Bytes::from_static(b"respilled"), cold_loc(2));
        ci.note_dead_slot(3, Bytes::from_static(b"twice"));
        // A live cold key with no dead slot, and a stale shadow behind a hot
        // key whose value expired.
        ci.insert(Bytes::from_static(b"cold"), cold_loc(4));
        ci.insert(Bytes::from_static(b"shadowed_expired"), cold_loc(4));
        db.cold_index = Some(ci);
        db.set_string(b"rehot", Bytes::from_static(b"v"));
        db.set_string_with_expiry(b"rehot_expired", Bytes::from_static(b"v"), now - 1);
        db.set_string_with_expiry(b"shadowed_expired", Bytes::from_static(b"v"), now - 1);
        in_flight(&mut db, b"flying", b"v", None);
        in_flight(&mut db, b"flying_expired", b"v", Some(now - 1));

        let deletes = deletes_of(&[Database::new(), db], now);
        assert_eq!(deletes.per_db.len(), 1);
        let (db_idx, keys) = &deletes.per_db[0];
        assert_eq!(*db_idx, 1, "the deletes are tagged with their database");
        let mut unique = sorted(keys);
        unique.dedup();
        assert_eq!(
            keys.len(),
            unique.len() + 1,
            "`twice` is listed once per dead slot; the head dedupes"
        );
        assert_eq!(
            unique,
            vec![
                &b"deleted"[..],
                b"flying_expired",
                b"rehot_expired",
                b"shadowed_expired",
                b"twice"
            ]
        );
    }

    /// No cold tier, or nothing dead: no `ColdDeletes` chunk at all.
    #[test]
    fn nothing_dead_ships_no_deletes() {
        let mut db = Database::new();
        let mut ci = crate::storage::tiered::cold_index::ColdIndex::new();
        ci.insert(Bytes::from_static(b"cold"), cold_loc(1));
        db.cold_index = Some(ci);
        db.set_string(b"hot", Bytes::from_static(b"v"));
        let (sink, image) = fold_image_channel();
        stream_fold_image(&[&db], 0, sink);
        loop {
            match image.rx.recv().expect("stream") {
                FoldChunk::Data(_) => {}
                FoldChunk::ColdDeletes(d) => panic!("nothing is dead, yet {d:?}"),
                FoldChunk::End => break,
                FoldChunk::Failed(w) => panic!("failed: {w}"),
            }
        }
    }

    /// The legacy cloning fold (`do_rewrite_single`) selects the same keys.
    #[test]
    fn the_legacy_fold_helper_agrees_with_the_streaming_fold() {
        let now = 1_000_000u64;
        let mut db = Database::new();
        let mut ci = crate::storage::tiered::cold_index::ColdIndex::new();
        ci.insert(Bytes::from_static(b"dead"), cold_loc(1));
        ci.insert(Bytes::from_static(b"shadow"), cold_loc(1));
        ci.remove(b"dead");
        db.cold_index = Some(ci);
        db.set_string_with_expiry(b"shadow", Bytes::from_static(b"v"), now - 1);
        let streamed = deletes_of(std::slice::from_ref(&db), now);
        let legacy = fold_cold_deletes(&[&db], now);
        assert_eq!(streamed, legacy);
        assert_eq!(legacy.len(), 2);
    }

    /// A reported serialization failure aborts the fold.
    #[test]
    fn failed_stream_is_an_error() {
        let (sink, image) = fold_image_channel();
        sink.fail("boom".into());
        let mut out = Vec::new();
        let err = write_fold_image(image, &mut out, "test").unwrap_err();
        assert!(err.to_string().contains("boom"), "{err}");
    }
}
