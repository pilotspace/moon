//! The writer's running record context (db + clock) and the stamp a producer
//! hands a record (moon#1283).
//!
//! Every AOF writer tracks what a replay of its file will believe at the
//! current end of the stream:
//! - the database a replay has SELECTed (task #35: a record whose execution db
//!   differs gets a `SELECT <db>` record first);
//! - the expiry-judgment clock a replay will use (moon#1283: a record whose
//!   producer's clock differs gets a `MOON.TS <ms>` record first — see
//!   [`crate::persistence::replay::pseudo`]).
//!
//! [`RecordCtx`] is that state; [`RecordCtx::prefix`] is the ONE rule every
//! write site applies — the four batch loops through
//! [`super::inject_record_prefixes`], the rewrite drains and the overflow
//! drains per record. A new generation (a fresh incr, a rewritten flat file)
//! starts from [`RecordCtx::reset`]: db 0, and no clock, so its first stamped
//! record carries its own `MOON.TS`. A writer that starts appending to a file
//! it did not just create starts from [`RecordCtx::appending`] instead: the
//! file may end in any `SELECT`, so the db is unknown until the first record
//! states it (redis: `aof_selected_db = -1`).
//!
//! ## The session stamp and the clean-close marker (R2 review of moon#1283)
//!
//! The replay's positional rule (`replay::clock`) relies on two things this
//! context guarantees for a reopened file:
//! - the writer's FIRST append carries a `MOON.TS` — even a record with an
//!   unknown clock gets one (the writer's own clock) — so no record of this
//!   binary ever follows a `CLOSE` unstamped. When the boot's replay met a
//!   foreign segment at the end of the file, its judgment is written first
//!   ([`crate::persistence::replay::clock::take_open_foreign_segment`]), so
//!   later boots judge that segment the way this boot did;
//! - [`RecordCtx::close_records`] is what an orderly stop appends:
//!   `MOON.TS <ms> CLOSE` (after the session's owed stamp, if nothing was
//!   written yet).
//!
//! A new generation owes no session stamp: its head carries one, and it
//! holds no other binary's records.
//!
//! [`AppendStamp`] is what a producer reads in the same synchronous section as
//! the mutation its record logs: the fold epoch (#455) and the shard's cached
//! clock — the value `Database::now_ms` judged the command with. Both ride the
//! [`super::AofMessage`] in memory only. A `clock_ms` of 0 means "unknown": no
//! `MOON.TS` is emitted for the record, which replays under the previous one
//! (barriers, tests, and records with no producer clock).

use std::path::{Path, PathBuf};

use bytes::{Bytes, BytesMut};

use super::FoldEpoch;
use crate::persistence::replay::pseudo::{TS_RECORD_MAX_LEN, TsRecord};

/// What a producer stamps a record with, read in the same synchronous section
/// as the mutation (see the module doc).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AppendStamp {
    /// The writer's fold epoch at the mutation (#455).
    pub epoch: FoldEpoch,
    /// The shard's cached clock at the mutation, unix ms (0 = unknown).
    pub clock_ms: u64,
}

impl AppendStamp {
    /// No fold epoch, no clock: a barrier, or a producer with no AOF.
    pub const INITIAL: AppendStamp = AppendStamp {
        epoch: FoldEpoch::INITIAL,
        clock_ms: 0,
    };

    /// `epoch` with this thread's cached clock (`current_time_ms`: the shard's
    /// `CachedClock` on a shard thread, a clock read elsewhere).
    #[inline]
    #[must_use]
    pub fn now(epoch: FoldEpoch) -> Self {
        Self {
            epoch,
            clock_ms: crate::storage::entry::current_time_ms(),
        }
    }
}

/// A bare epoch carries no clock (0 = unknown).
impl From<FoldEpoch> for AppendStamp {
    #[inline]
    fn from(epoch: FoldEpoch) -> Self {
        Self { epoch, clock_ms: 0 }
    }
}

/// `MOON.TS` records are carved out of one arena buffer this big, so emitting
/// one allocates only when the arena runs out (about one allocation per 90
/// stamps, none while earlier stamps have been written and dropped).
const TS_ARENA_BYTES: usize = 4096;

/// [`RecordCtx::db`] of a stream whose selected db is not known: no real db
/// index equals it, so the next record always gets a `SELECT`.
pub const UNKNOWN_DB: usize = usize::MAX;

/// The writer's running record context. See the module doc.
#[derive(Debug, Default)]
pub struct RecordCtx {
    /// The db a replay has selected at the current end of the stream
    /// ([`UNKNOWN_DB`] when the writer cannot know it).
    db: usize,
    /// The last `MOON.TS` in the stream (0 = none since the generation began).
    ts_ms: u64,
    /// Backing store of the emitted `MOON.TS` records.
    arena: BytesMut,
    /// The reopened file whose first append still owes a stamp (see the
    /// module doc); `None` once one is written, or for a new generation.
    session_owed: Option<PathBuf>,
}

impl RecordCtx {
    /// A context at the start of a generation.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A context for a writer that (re)opens its append target at boot: an
    /// incr or flat `appendonly.aof` a previous run may have left in ANY db
    /// (its last `SELECT`). The db is unknown, so the first non-empty record
    /// always carries a `SELECT <db>` — without it, a restarted writer's
    /// first db-0 record replayed into whatever db the previous run ended in
    /// (R1 review, finding 2). The clock is unknown too (no `MOON.TS` yet),
    /// as in [`Self::new`]. redis starts every AOF with `aof_selected_db =
    /// -1` for the same reason.
    ///
    /// `path` is the file: its first append owes a stamp (the module doc).
    #[must_use]
    pub fn appending(path: &Path) -> Self {
        Self {
            db: UNKNOWN_DB,
            session_owed: Some(path.to_path_buf()),
            ..Self::default()
        }
    }

    /// The writer moved to a NEW generation file: a replay starts it at db 0
    /// with no clock (the next stamped record emits its `MOON.TS`), and it
    /// owes no session stamp (its head carries one).
    #[inline]
    pub fn reset(&mut self) {
        self.db = 0;
        self.ts_ms = 0;
        self.session_owed = None;
    }

    /// The db a replay of the stream so far has selected ([`UNKNOWN_DB`]
    /// before the first record of an [`Self::appending`] context).
    #[inline]
    #[must_use]
    pub fn db(&self) -> usize {
        self.db
    }

    /// The last `MOON.TS` value in the stream (0 = none).
    #[inline]
    #[must_use]
    pub fn ts_ms(&self) -> u64 {
        self.ts_ms
    }

    /// Whether a record (`db`, `clock_ms`, empty or not) needs a prefix.
    /// A zero-length payload (the `fsync_barrier`) writes nothing, so it
    /// never needs, nor moves, either part.
    #[inline]
    #[must_use]
    pub fn needs_prefix(&self, db: usize, clock_ms: u64, payload_is_empty: bool) -> bool {
        !payload_is_empty
            && (db != self.db
                || (clock_ms != 0 && clock_ms != self.ts_ms)
                || self.session_owed.is_some())
    }

    /// The records to write BEFORE a record, in order — the session stamps
    /// of a reopened file's first append (the module doc), `MOON.TS
    /// <clock_ms>` when the clock changed, then `SELECT <db>` when the db
    /// changed — and the context advanced past them. Empty for a barrier and
    /// for a record that changes nothing.
    #[inline]
    pub fn prefix(&mut self, db: usize, clock_ms: u64, payload_is_empty: bool) -> RecordPrefix {
        let mut out = RecordPrefix::default();
        if payload_is_empty {
            return out;
        }
        let mut clock_ms = clock_ms;
        if let Some(path) = self.session_owed.take() {
            out.session = self.segment_stamp(&path);
            // The first record carries a stamp even with no producer clock.
            if clock_ms == 0 {
                clock_ms = crate::storage::entry::current_time_ms();
            }
        }
        if clock_ms != 0 && clock_ms != self.ts_ms {
            self.ts_ms = clock_ms;
            out.ts = Some(self.ts_bytes(clock_ms));
        }
        if db != self.db {
            self.db = db;
            out.select = Some(super::serialize_select_record(db));
        }
        out
    }

    /// The judgment the boot's replay gave a foreign segment at the end of
    /// `path`, as a stamp to write before anything else (see the module doc).
    fn segment_stamp(&mut self, path: &Path) -> Option<Bytes> {
        let ms = crate::persistence::replay::clock::take_open_foreign_segment(path)?;
        self.ts_ms = ms;
        Some(self.ts_bytes(ms))
    }

    /// What an orderly stop appends to the live file (R2 review of
    /// moon#1283): the session's owed segment stamp when nothing was written
    /// yet, then the clean-close marker `MOON.TS <ms> CLOSE` at the writer's
    /// clock (never behind the last stamp). The caller makes them durable
    /// with its final sync, and appends nothing after them.
    pub fn close_records(&mut self) -> RecordPrefix {
        let mut out = RecordPrefix::default();
        if let Some(path) = self.session_owed.take() {
            out.session = self.segment_stamp(&path);
        }
        let ms = crate::storage::entry::current_time_ms().max(self.ts_ms);
        self.ts_ms = ms;
        out.ts = Some(Bytes::copy_from_slice(TsRecord::close(ms).as_bytes()));
        out
    }

    /// `MOON.TS <ms>` as `Bytes` carved from the arena.
    fn ts_bytes(&mut self, ms: u64) -> Bytes {
        if self.arena.capacity() - self.arena.len() < TS_RECORD_MAX_LEN {
            self.arena.reserve(TS_ARENA_BYTES);
        }
        self.arena.extend_from_slice(TsRecord::new(ms).as_bytes());
        self.arena.split().freeze()
    }
}

/// The records [`RecordCtx::prefix`] asks for, iterated in write order.
#[derive(Debug, Default)]
pub struct RecordPrefix {
    session: Option<Bytes>,
    ts: Option<Bytes>,
    select: Option<Bytes>,
}

impl RecordPrefix {
    /// Whether nothing is to be written.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.session.is_none() && self.ts.is_none() && self.select.is_none()
    }
}

impl Iterator for RecordPrefix {
    type Item = Bytes;

    #[inline]
    fn next(&mut self) -> Option<Bytes> {
        self.session
            .take()
            .or_else(|| self.ts.take())
            .or_else(|| self.select.take())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(p: RecordPrefix) -> Vec<Vec<u8>> {
        p.map(|b| b.to_vec()).collect()
    }

    #[test]
    fn a_clock_change_emits_one_ts_before_the_select() {
        let mut ctx = RecordCtx::new();
        let got = collect(ctx.prefix(2, 1_700_000_000_123, false));
        assert_eq!(
            got,
            vec![
                TsRecord::new(1_700_000_000_123).as_bytes().to_vec(),
                super::super::serialize_select_record(2).to_vec(),
            ]
        );
        assert_eq!((ctx.db(), ctx.ts_ms()), (2, 1_700_000_000_123));
        // Same db, same clock: nothing.
        assert!(ctx.prefix(2, 1_700_000_000_123, false).is_empty());
        assert!(!ctx.needs_prefix(2, 1_700_000_000_123, false));
        // The clock moves BACK (a parked producer): the older stamp is written.
        assert!(ctx.needs_prefix(2, 1_700_000_000_100, false));
        let got = collect(ctx.prefix(2, 1_700_000_000_100, false));
        assert_eq!(
            got,
            vec![TsRecord::new(1_700_000_000_100).as_bytes().to_vec()]
        );
    }

    #[test]
    fn an_unknown_clock_and_a_barrier_emit_no_ts() {
        let mut ctx = RecordCtx::new();
        assert!(ctx.prefix(0, 0, false).is_empty());
        assert!(!ctx.needs_prefix(0, 0, false));
        // A barrier never needs nor moves anything.
        assert!(ctx.prefix(3, 42, true).is_empty());
        assert_eq!((ctx.db(), ctx.ts_ms()), (0, 0));
    }

    #[test]
    fn a_new_generation_restamps_its_first_record() {
        let mut ctx = RecordCtx::new();
        let _ = collect(ctx.prefix(1, 5, false));
        ctx.reset();
        assert_eq!((ctx.db(), ctx.ts_ms()), (0, 0));
        let got = collect(ctx.prefix(0, 5, false));
        assert_eq!(got, vec![TsRecord::new(5).as_bytes().to_vec()]);
    }

    /// R1 review finding 2: a writer that reopens an existing file does not
    /// know the stream's db, so even a db-0 record gets its `SELECT 0` —
    /// once — and a barrier still writes nothing.
    #[test]
    fn a_reopened_stream_selects_its_first_records_db_even_db_0() {
        let path = std::path::Path::new("/nonexistent/moon/reopened.aof");
        let mut ctx = RecordCtx::appending(path);
        assert_eq!(ctx.db(), UNKNOWN_DB);
        assert!(ctx.prefix(0, 0, true).is_empty(), "a barrier never selects");
        assert!(ctx.needs_prefix(0, 7, false));
        let got = collect(ctx.prefix(0, 7, false));
        assert_eq!(
            got,
            vec![
                TsRecord::new(7).as_bytes().to_vec(),
                super::super::serialize_select_record(0).to_vec(),
            ]
        );
        assert!(ctx.prefix(0, 7, false).is_empty(), "db 0 is now known");
        // A new generation after it starts at a known db 0 again.
        ctx.reset();
        assert_eq!(ctx.prefix(0, 7, false).count(), 1, "only the stamp");
    }

    /// R2 review of moon#1283: the first append to a reopened file always
    /// carries a stamp — a record with no producer clock gets the writer's —
    /// so no record of this binary follows a clean-close marker unstamped.
    /// After it, an unknown clock adds nothing, as before.
    #[test]
    fn a_reopened_files_first_append_is_stamped_even_without_a_clock() {
        let path = std::path::Path::new("/nonexistent/moon/reopened.aof");
        let before = crate::storage::entry::current_time_ms();
        let mut ctx = RecordCtx::appending(path);
        assert!(ctx.needs_prefix(UNKNOWN_DB, 0, false));
        let got = collect(ctx.prefix(0, 0, false));
        assert_eq!(got.len(), 2, "the session stamp, then SELECT 0");
        assert!(crate::persistence::replay::pseudo::is_ts_record(&got[0]));
        assert!(ctx.ts_ms() >= before, "the writer's clock");
        assert_eq!(got[1], super::super::serialize_select_record(0).to_vec());
        assert!(ctx.prefix(0, 0, false).is_empty());
        // A new generation owes nothing: its head carries the stamp.
        let mut ctx = RecordCtx::appending(path);
        ctx.reset();
        assert!(ctx.prefix(0, 0, false).is_empty());
    }

    /// The boot's replay left a foreign segment open at the END of the file,
    /// judged at 4_000: the first append writes that judgment first, then
    /// its own stamp — so on later boots the segment ends at a stamp of the
    /// value this boot judged it by.
    #[test]
    fn the_first_append_writes_an_open_segments_judgment_first() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("moon.aof.3.incr.aof");
        std::fs::write(&path, b"").expect("create");
        crate::persistence::replay::clock::tests_support::report_open_segment(&path, 4_000);
        let mut ctx = RecordCtx::appending(&path);
        let got = collect(ctx.prefix(2, 9_000, false));
        assert_eq!(
            got,
            vec![
                TsRecord::new(4_000).as_bytes().to_vec(),
                TsRecord::new(9_000).as_bytes().to_vec(),
                super::super::serialize_select_record(2).to_vec(),
            ]
        );
        // Taken once: a second writer session (or a second file) owes nothing.
        assert_eq!(
            crate::persistence::replay::clock::take_open_foreign_segment(&path),
            None
        );
        // Same value as the record's clock: one stamp serves both.
        crate::persistence::replay::clock::tests_support::report_open_segment(&path, 9_000);
        let mut ctx = RecordCtx::appending(&path);
        assert_eq!(
            collect(ctx.prefix(0, 9_000, false)),
            vec![
                TsRecord::new(9_000).as_bytes().to_vec(),
                super::super::serialize_select_record(0).to_vec(),
            ]
        );
    }

    /// An orderly stop: the marker at the writer's clock, never behind the
    /// last stamp written.
    #[test]
    fn a_close_marker_is_never_behind_the_last_stamp() {
        let mut ctx = RecordCtx::new();
        let future = crate::storage::entry::current_time_ms() + 3_600_000;
        let _ = collect(ctx.prefix(0, future, false));
        let got = collect(ctx.close_records());
        assert_eq!(got, vec![TsRecord::close(future).as_bytes().to_vec()]);
    }

    #[test]
    fn stamps_share_the_arena() {
        let mut ctx = RecordCtx::new();
        let mut kept = Vec::new();
        for ms in 1..=500u64 {
            let p: Vec<Bytes> = ctx.prefix(0, ms, false).collect();
            assert_eq!(p.len(), 1);
            assert_eq!(p[0].as_ref(), TsRecord::new(ms).as_bytes());
            kept.extend(p);
        }
        // Every stamp still reads back intact after later ones were carved.
        for (i, b) in kept.iter().enumerate() {
            assert_eq!(b.as_ref(), TsRecord::new(i as u64 + 1).as_bytes());
        }
    }

    #[test]
    fn stamps_convert_from_a_bare_epoch_with_no_clock() {
        let s: AppendStamp = FoldEpoch(3).into();
        assert_eq!(
            s,
            AppendStamp {
                epoch: FoldEpoch(3),
                clock_ms: 0
            }
        );
        assert_eq!(AppendStamp::INITIAL.clock_ms, 0);
        assert_ne!(AppendStamp::now(FoldEpoch(1)).clock_ms, 0);
    }

    fn payloads(batch: &[super::super::AofMessage]) -> Vec<Vec<u8>> {
        use super::super::AofMessage;
        batch
            .iter()
            .map(|m| match m {
                AofMessage::Append { bytes, .. } | AofMessage::AppendSync { bytes, .. } => {
                    bytes.to_vec()
                }
                _ => b"<control>".to_vec(),
            })
            .collect()
    }

    /// moon#1283: a record whose clock differs from the last stamp in the
    /// stream gets a `MOON.TS` first (before its `SELECT`); records of the
    /// same tick share it; an unknown clock (0) and a barrier add none; the
    /// clock may go back (a parked producer).
    #[test]
    fn a_clock_change_injects_one_stamp_before_the_record() {
        use super::super::AofMessage;
        let at = |payload: &'static [u8], db: usize, clock_ms: u64| AofMessage::Append {
            lsn: 9,
            db,
            bytes: Bytes::from_static(payload),
            epoch: FoldEpoch::INITIAL,
            clock_ms,
        };
        let ts = |ms: u64| {
            crate::persistence::replay::pseudo::TsRecord::new(ms)
                .as_bytes()
                .to_vec()
        };
        let mut ctx = RecordCtx::new();
        let out = super::super::inject_record_prefixes(
            vec![
                at(b"a", 0, 100),
                at(b"b", 0, 100),
                at(b"", 4, 250), // barrier
                at(b"c", 0, 0),  // unknown clock
                at(b"d", 3, 101),
                at(b"e", 3, 99), // parked producer: older stamp
            ],
            FoldEpoch::INITIAL,
            &mut ctx,
        );
        assert_eq!(
            payloads(&out),
            vec![
                ts(100),
                b"a".to_vec(),
                b"b".to_vec(),
                b"".to_vec(),
                b"c".to_vec(),
                ts(101),
                super::super::serialize_select_record(3).to_vec(),
                b"d".to_vec(),
                ts(99),
                b"e".to_vec(),
            ]
        );
        // Injected records carry lsn 0, so a framed replay's max_lsn ignores them.
        let lsns: Vec<u64> = out
            .iter()
            .map(|m| match m {
                AofMessage::Append { lsn, .. } | AofMessage::AppendSync { lsn, .. } => *lsn,
                _ => u64::MAX,
            })
            .collect();
        assert_eq!(lsns, vec![0, 9, 9, 9, 9, 0, 0, 9, 0, 9]);
        assert_eq!((ctx.db(), ctx.ts_ms()), (3, 99));
        // The same tick again: the fast path, nothing injected.
        let batch = vec![at(b"f", 3, 99)];
        let ptr = batch.as_ptr();
        let out = super::super::inject_record_prefixes(batch, FoldEpoch::INITIAL, &mut ctx);
        assert_eq!(out.as_ptr(), ptr);
    }

    /// moon#1283 carries `clock_ms` on `Append` / `AppendSync`: at most one
    /// more word per queued message. The mirror is the enum as it was before
    /// (same variants and field types, no `clock_ms`); `--nocapture` prints
    /// both sizes.
    #[test]
    fn the_clock_costs_at_most_one_word_per_message() {
        use super::super::{AofAck, AofMessage, PerShardRewriteCoord, SharedDatabases, rewrite};
        use std::sync::Arc;
        #[allow(dead_code)]
        enum Before {
            Append {
                lsn: u64,
                db: usize,
                bytes: Bytes,
                epoch: FoldEpoch,
            },
            AppendSync {
                lsn: u64,
                db: usize,
                bytes: Bytes,
                ack: crate::runtime::channel::OneshotSender<AofAck>,
                epoch: FoldEpoch,
            },
            Rewrite(SharedDatabases, Arc<rewrite::RewriteOverflow>),
            RewriteSharded(
                Arc<crate::shard::shared_databases::ShardDatabases>,
                Arc<rewrite::RewriteOverflow>,
            ),
            RewritePerShard {
                shard_dbs: Arc<crate::shard::shared_databases::ShardDatabases>,
                coord: Arc<PerShardRewriteCoord>,
                fold_producer: Arc<
                    parking_lot::Mutex<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>,
                >,
                fold_notifier: Arc<crate::runtime::channel::Notify>,
                overflow: Arc<rewrite::RewriteOverflow>,
            },
            Shutdown,
        }
        let (before, after) = (
            std::mem::size_of::<Before>(),
            std::mem::size_of::<AofMessage>(),
        );
        println!("size_of::<AofMessage>(): {before} bytes before moon#1283, {after} after");
        assert!(after <= before + 8, "{before} -> {after}");
    }
}
