//! The stream-reader waker: serve the `XREAD` / `XREADGROUP` clients parked
//! on a key, and the errors a blocking stream read is owed before it parks.
//!
//! Split out of `wakeup.rs`, which serves the destructive list and zset
//! wakes; re-exported from there, so every caller keeps its path.

use bytes::Bytes;

use crate::blocking::{BlockedCommand, BlockingRegistry};
use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;

/// Called after `XADD` adds an entry to a stream key, and again right after a
/// remote `BlockRegister` lands, to serve whatever stream readers that key now
/// has parked on it.
///
/// Returns true if at least one blocked client was answered.
///
/// # Why this is not shaped like the list and zset wakers
///
/// Those wakers CONSUME: a pushed element belongs to exactly one waiter, so
/// `pop_front_of_family` + answer + `return true` is the whole operation, and
/// a waiter they cannot serve means the key really is empty.
///
/// Stream reads are non-destructive, and both halves of that matter
/// (both measured against redis-server 8.6.1):
///
/// * **one `XADD` wakes EVERY parked `XREAD`.** Two clients on
///   `XREAD BLOCK 5000 STREAMS k $` each receive the entry from a single
///   `XADD`. Stopping at the first served waiter would have left the second
///   parked until its deadline.
/// * **a waiter this `XADD` cannot serve must stay parked.** The pre-#595
///   code ran `remove_wait` + `reply_tx.send(None)` for every waiter it
///   popped, servable or not — so an `XADD` at an id BELOW a `$`-bound
///   reader's cursor, or an `XREADGROUP` whose entries a sibling consumer
///   just took, unblocked that reader with a premature null. That same
///   `send(None)` is what would have fired on the re-check the
///   `BlockRegister` handler runs immediately after registering, making a
///   remote `XREAD BLOCK` answer null the instant it was registered.
///
/// So this walks the key's stream-family waiters in FIFO order, decides each
/// one against the store while it is still queued, and only removes the ones
/// it can actually answer ([`BlockingRegistry::take_waits`]). Nothing is ever
/// answered `None` here; a waiter that is not served stays registered and is
/// released by its own deadline, its client's disconnect, or a later `XADD`.
///
/// # A group read is claimed before it reads, and logged as it reads
///
/// `XREADGROUP` is not read-only: the read moves what it delivers into the
/// consumer's PEL and advances the group's cursor. So a group reader is
/// claimed FIRST and read only on a won claim (moon#1047) — a remote reader
/// whose timeout settles its claim at that moment answers nil with the group
/// untouched, instead of with entries stranded in its PEL — and the read is
/// logged by this shard in the same synchronous stretch, before the reply
/// leaves (moon#1104, [`crate::blocking::stream_log`]).
pub fn try_wake_stream_waiter(
    registry: &mut BlockingRegistry,
    db: &mut Database,
    db_index: usize,
    key: &Bytes,
) -> bool {
    try_wake_stream_waiter_budgeted(
        registry,
        db,
        db_index,
        key,
        &mut crate::blocking::pop_log::wake_budget(),
    )
}

/// [`try_wake_stream_waiter`] drawing the logging of every group read it
/// serves from the wake pass's shared AOF backpressure `budget`
/// ([`crate::blocking::pop_log::wake_budget`]).
pub(crate) fn try_wake_stream_waiter_budgeted(
    registry: &mut BlockingRegistry,
    db: &mut Database,
    db_index: usize,
    key: &Bytes,
    budget: &mut std::time::Duration,
) -> bool {
    // Decide (and, for a group read, claim and read) in ONE pass over the
    // queue with every waiter still in place, so a decision of "cannot serve"
    // costs nothing and leaves FIFO order untouched; take and answer the
    // served ones after.
    //
    // Queue order is NOT `wait_id` order (moon#620). An id is
    // `(shard_id << 48) | counter`, minted by the registry of the shard the
    // waiter's CONNECTION lives on, while the queue belongs to the shard that
    // owns the KEY — so a reader on shard 3 that parks before a reader on
    // shard 1 puts the larger id first. Everything downstream of here treats
    // the two orders as independent.
    let mut decisions: smallvec::SmallVec<[(u64, Option<Frame>); 4]> = smallvec::SmallVec::new();
    // moon#1056: once a group read's record could not reach the AOF within
    // the pass's budget, every further group serve here would be refused
    // too; the group readers behind it stay parked beside their data instead.
    let mut aof_lost = false;
    {
        let Some(queue) = registry.waiters_on(db_index, key) else {
            return false;
        };
        for entry in queue
            .iter()
            .filter(|e| e.cmd.family() == crate::blocking::WaitFamily::Stream)
        {
            // c10k A2 / moon#1023: a client that already went away, or a
            // remote reader whose wait already ended (its claim token is
            // settled), must not consume the wake a live sibling needs. It is
            // removed without touching the store.
            if entry.is_settled() {
                decisions.push((entry.wait_id, None));
                continue;
            }
            match &entry.cmd {
                // `XREAD` mutates nothing: build the reply, then claim.
                BlockedCommand::XRead { streams, count } => {
                    let Some(frame) = xread_reply(db, key, streams, *count) else {
                        continue;
                    };
                    let won = claim_for_serve(entry);
                    decisions.push((entry.wait_id, won.then_some(frame)));
                }
                // `XREADGROUP` WRITES: the read moves entries into a PEL and
                // advances the group's cursor. moon#1047: so the waiter is
                // claimed FIRST, and the read runs only on a won claim. The
                // readiness check before it is read-only, and nothing between
                // it and the read can suspend, so a won claim always finds the
                // entries it was decided on. A reader whose timeout settled
                // its claim a moment earlier answers nil with the group
                // untouched — the entries are still there for the next `>`.
                BlockedCommand::XReadGroup {
                    group,
                    consumer,
                    count,
                    noack,
                    ..
                } => {
                    match group_reader_ready(
                        db, db_index, key, group, consumer, *noack, *count, budget,
                    ) {
                        GroupReadiness::NotYet => continue,
                        // moon#1086: the stream or the group is gone, or the
                        // key holds another type. Redis unblocks the reader
                        // with the error its read now answers; claimed like
                        // any other answer, and nothing to log.
                        GroupReadiness::Gone(err) => {
                            let won = claim_for_serve(entry);
                            decisions.push((entry.wait_id, won.then_some(err)));
                            continue;
                        }
                        // moon#1111: retried once the writer has room.
                        GroupReadiness::Ready if aof_lost => {
                            crate::blocking::wakeup::defer_wake(db_index, key);
                            continue;
                        }
                        GroupReadiness::Ready => {}
                    }
                    if !claim_for_serve(entry) {
                        decisions.push((entry.wait_id, None));
                        continue;
                    }
                    let served = serve_group_read(
                        db, db_index, key, group, consumer, *count, *noack, budget,
                    );
                    if let Some((_, true)) = served {
                        aof_lost = true;
                    }
                    decisions.push((entry.wait_id, served.map(|(frame, _)| frame)));
                }
                // Unreachable: the filter above admits only the two stream
                // readers.
                _ => {}
            }
            // Anything not decided stays REGISTERED and is released by its own
            // deadline, its client's disconnect, or a later write. It is never
            // answered `None` here.
        }
    }
    if decisions.is_empty() {
        return false;
    }

    // `take_waits` looks each id up with a binary search, so it must be handed
    // a SORTED slice — queue order will not do (see above). An unsorted slice
    // makes the search miss ids that are present, which silently leaves those
    // waiters parked until their own deadline: the lost wakeup moon#620 was
    // filed for.
    let mut ids: smallvec::SmallVec<[u64; 4]> = decisions.iter().map(|(id, _)| *id).collect();
    ids.sort_unstable();

    let mut woke = false;
    // Pair each returned entry with its decision BY `wait_id`, never by
    // position: `take_waits` hands entries back in queue order while `ids` is
    // sorted, and a positional pairing would hand one reader the entries
    // computed for another's cursor.
    for entry in registry.take_waits(db_index, key, &ids) {
        let Some(slot) = decisions.iter_mut().find(|(id, _)| *id == entry.wait_id) else {
            debug_assert!(false, "take_waits returned an entry we did not ask for");
            continue;
        };
        // `None`: a settled waiter or a lost claim — removed, nothing to send.
        let Some(frame) = slot.1.take() else {
            continue;
        };
        // Every frame here was claimed in the pass above. A failed send is
        // the residual A2 race — a receiver dropped without settling its
        // claim (its task torn down). An `XREAD` changed nothing; a group
        // read stands, logged, exactly like a client that disconnects right
        // after its reply was written (see `wakeup::WakeUndo`).
        if entry.reply_tx.send(Some(frame)).is_ok() {
            woke = true;
        }
    }
    woke
}

/// Become the one serve of `entry`: a local reader (no token) is this
/// thread's alone, and was just checked to be alive; a remote one is won with
/// its claim token (moon#1019, moon#1023).
fn claim_for_serve(entry: &crate::blocking::WaitEntry) -> bool {
    #[cfg(test)]
    race::settle_before_claim(entry.claim.as_ref());
    entry
        .claim
        .as_ref()
        .is_none_or(crate::blocking::ClaimToken::try_claim)
}

/// The error a blocking stream read owes its client IMMEDIATELY, decided on
/// the shard that owns `key` (moon#595).
///
/// Registering is the wrong answer to a question the keyspace has already
/// settled. `-WRONGTYPE` and `XREADGROUP`'s two errors are permanent for as
/// long as the key is what it is: a group that does not exist cannot start
/// existing because someone `XADD`s to the stream, so a waiter parked on that
/// hope would burn its whole budget and then answer the null array.
///
/// It runs HERE, in the `BlockRegister` handler, and not only in the client's
/// own pre-registration scan, because that scan can see only the keys its
/// shard owns. Without this, `XREADGROUP GROUP nope c BLOCK 800 STREAMS k >`
/// answered `-NOGROUP` in 0.000 s when `k` hashed to the client's own shard
/// and parked for the full 800 ms when it did not — the same command, two
/// answers, decided by a hash.
///
/// `None` means "nothing settled; park".
pub fn stream_register_error(
    db: &mut Database,
    key: &Bytes,
    cmd: &BlockedCommand,
) -> Option<Frame> {
    // Wrong type is wrong type for both stream readers.
    if let Some(err) = db.get_stream(key).err() {
        return Some(err);
    }
    let BlockedCommand::XReadGroup { group, .. } = cmd else {
        // A plain XREAD on a missing key is not an error — that is exactly the
        // `$`-on-a-future-stream case, and it must park.
        return None;
    };
    // Redis answers both with one text naming the key and the group.
    let Ok(Some(stream)) = db.get_stream(key) else {
        return Some(xreadgroup_nogroup(key, group));
    };
    if !stream.groups.contains_key(group.as_ref()) {
        return Some(xreadgroup_nogroup(key, group));
    }
    None
}

/// The reply a parked `XREAD` is owed by the current state of `key`, or
/// `None` if this key cannot serve it yet. Read-only.
fn xread_reply(
    db: &mut Database,
    key: &Bytes,
    streams: &[(Bytes, crate::blocking::StreamSince)],
    count: Option<usize>,
) -> Option<Frame> {
    use crate::command::stream::format_entry;
    use crate::storage::stream::StreamId;

    // `find` rather than an index: a multi-key XREAD registers the same
    // command on several keys and only this key's cursor applies.
    //
    // `StreamSince::Latest` here means `$` was never bound to a number. That
    // is a binding bug, not a client state — and it resolves to "serve
    // nothing" deliberately: treating it as `0-0` would replay the stream's
    // whole history to a client that asked only for what arrives next.
    let since = streams
        .iter()
        .find(|(k, _)| k == key)
        .and_then(|(_, since)| since.id())?;
    let start = if since.seq == u64::MAX {
        StreamId {
            ms: since.ms.saturating_add(1),
            seq: 0,
        }
    } else {
        StreamId {
            ms: since.ms,
            seq: since.seq.saturating_add(1),
        }
    };
    let stream = db.get_stream(key).ok()??;
    let entries = stream.range(start, StreamId::MAX, count);
    if entries.is_empty() {
        return None;
    }
    let frames: Vec<Frame> = entries
        .into_iter()
        .map(|(id, fields)| format_entry(id, fields))
        .collect();
    Some(served_reply(key, frames))
}

/// `[[key, entries]]`: only the stream that actually had entries appears,
/// which is both what redis answers a woken reader and what moon#594 made the
/// non-blocking `XREAD` do.
fn served_reply(key: &Bytes, entries: Vec<Frame>) -> Frame {
    Frame::Array(framevec![Frame::Array(framevec![
        Frame::BulkString(key.clone()),
        Frame::Array(entries.into()),
    ])])
}

/// What a parked group reader is owed right now.
enum GroupReadiness {
    /// A `>` read would deliver.
    Ready,
    /// Nothing new yet: stay parked.
    NotYet,
    /// The read can never be served as registered — its stream is gone,
    /// holds another type, or its group was destroyed. Carries the error the
    /// read answers now (moon#1086).
    Gone(Frame),
}

/// Can a `>` read of `group` by `consumer` on `key` deliver something now?
///
/// Read-only as far as the group's delivery state goes (moon#1047: nothing
/// may move into a PEL before the waiter is claimed). The one thing it may
/// write is the CONSUMER: redis creates it when the read is issued, before
/// the client blocks, and propagates that only under `NOACK`
/// (`XGROUP CREATECONSUMER`, moon#1104). A remote reader's first pass through
/// here — the one its `BlockRegister` runs — is that moment on this shard; a
/// local reader already created it in its immediate read.
///
/// A reader whose stream or group no longer exists, or whose key now holds
/// another type, is [`GroupReadiness::Gone`], with the exact error
/// redis-server 8.6.1 unblocks it with (moon#1086).
#[allow(clippy::too_many_arguments)]
fn group_reader_ready(
    db: &mut Database,
    db_index: usize,
    key: &Bytes,
    group: &Bytes,
    consumer: &Bytes,
    noack: bool,
    count: Option<usize>,
    budget: &mut std::time::Duration,
) -> GroupReadiness {
    // Deciding is not a write: a read-only look first, because
    // `get_stream_mut` stamps the key's `WATCH` version and a wake that serves
    // nothing would abort every transaction watching the stream.
    let (consumer_exists, has_new) = match db.get_stream(key) {
        Ok(Some(stream)) => match stream.groups.get(group.as_ref()) {
            Some(g) => (
                g.consumers.contains_key(consumer),
                stream.group_has_new(group) == Some(true),
            ),
            None => return GroupReadiness::Gone(xreadgroup_nogroup(key, group)),
        },
        Ok(None) => return GroupReadiness::Gone(xreadgroup_nogroup(key, group)),
        Err(wrongtype) => return GroupReadiness::Gone(wrongtype),
    };
    // `COUNT 0` reads nothing in moon's `read_group_new`, so it can never be
    // served here; deciding otherwise would claim a waiter with nothing to
    // hand it.
    let readiness = if count != Some(0) && has_new {
        GroupReadiness::Ready
    } else {
        GroupReadiness::NotYet
    };
    if consumer_exists {
        return readiness;
    }
    // Redis creates a blocked reader's consumer without signalling the key's
    // watchers (`keyModified(..., signal=0)`), so neither does this.
    let Ok(Some(stream)) = db.get_stream_mut_unsignalled(key) else {
        return GroupReadiness::Gone(xreadgroup_nogroup(key, group));
    };
    let Ok(created) = stream.create_consumer(group, consumer.clone()) else {
        return GroupReadiness::Gone(xreadgroup_nogroup(key, group));
    };
    if created && noack && crate::blocking::pop_log::has_work() {
        // Nothing to tell the waiter if this record is refused: it has not
        // been served, and stays parked either way.
        let _ = crate::blocking::pop_log::log_records(
            db_index,
            &[Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"XGROUP")),
                Frame::BulkString(Bytes::from_static(b"CREATECONSUMER")),
                Frame::BulkString(key.clone()),
                Frame::BulkString(group.clone()),
                Frame::BulkString(consumer.clone()),
            ])],
            budget,
        );
    }
    readiness
}

/// `-NOGROUP No such key '<key>' or consumer group '<group>' in XREADGROUP
/// with GROUP option` — redis's answer to an `XREADGROUP` whose stream or
/// group does not exist, both when it is issued and when a parked one loses
/// them (moon#1086).
pub(crate) fn xreadgroup_nogroup(key: &[u8], group: &[u8]) -> Frame {
    crate::command::stream::nogroup_key_or_group(key, group, b" in XREADGROUP with GROUP option")
}

/// Run a claimed group reader's `>` read and log it (moon#1104) in this same
/// synchronous stretch, before the reply leaves. Returns the reply and
/// whether its record was refused by the AOF writer — in which case the reply
/// is the fail-loud error, as for a blocking pop (the read stands: its
/// entries are in the PEL and on every replica). `None` only if the read
/// found nothing, which [`group_reader_ready`] rules out.
#[allow(clippy::too_many_arguments)]
fn serve_group_read(
    db: &mut Database,
    db_index: usize,
    key: &Bytes,
    group: &Bytes,
    consumer: &Bytes,
    count: Option<usize>,
    noack: bool,
    budget: &mut std::time::Duration,
) -> Option<(Frame, bool)> {
    use crate::blocking::stream_log;
    use crate::command::stream::format_entry;

    let before = stream_log::group_read_before(db, key, group, consumer)?;
    let stream = db.get_stream_mut(key).ok()??;
    let entries = stream.read_group_new(group, consumer, count, noack).ok()?;
    if entries.is_empty() {
        return None;
    }
    let ids: smallvec::SmallVec<[crate::storage::stream::StreamId; 8]> =
        entries.iter().map(|(id, _)| *id).collect();
    let frame = served_reply(
        key,
        entries
            .iter()
            .map(|(id, fields)| format_entry(*id, fields))
            .collect(),
    );
    if !crate::blocking::pop_log::has_work() {
        return Some((frame, false));
    }
    let records = stream_log::group_read_records(db, key, group, consumer, noack, &before, &ids);
    match crate::blocking::pop_log::log_records(db_index, &records, budget) {
        crate::blocking::pop_log::PopLog::AofLost => Some((
            Frame::Error(Bytes::from_static(
                crate::shard::spsc_handler::AOF_APPEND_LOST_ERR,
            )),
            true,
        )),
        _ => Some((frame, false)),
    }
}

/// Test-only instrumentation for the timeout race of moon#1047: the waiter's
/// own deadline firing on its thread while this shard is between deciding to
/// serve it and claiming it.
#[cfg(test)]
pub(crate) mod race {
    use std::cell::Cell;

    thread_local! {
        static SETTLE_BEFORE_CLAIM: Cell<bool> = const { Cell::new(false) };
    }

    /// Arm (or disarm) the race for this thread's wakes.
    pub(crate) fn arm(on: bool) {
        SETTLE_BEFORE_CLAIM.with(|c| c.set(on));
    }

    /// Where the waker is about to claim a waiter: when armed, settle the
    /// token first, exactly as a waiter whose timeout fired now would.
    pub(crate) fn settle_before_claim(claim: Option<&crate::blocking::ClaimToken>) {
        if SETTLE_BEFORE_CLAIM.with(Cell::get)
            && let Some(c) = claim
        {
            let _ = c.settle();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blocking::{ClaimToken, Settled, StreamSince, WaitEntry};
    use crate::runtime::channel::{self, OneshotReceiver};
    use crate::storage::stream::StreamId;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn cmd(db: &mut Database, parts: &[&str]) -> Frame {
        let (name, rest) = parts.split_first().expect("a command");
        let args: Vec<Frame> = rest.iter().map(|p| Frame::BulkString(b(p))).collect();
        match *name {
            "XGROUP" => crate::command::stream::xgroup(db, &args),
            "XADD" => crate::command::stream::xadd(db, &args),
            "XREADGROUP" => crate::command::stream::xreadgroup(db, &args),
            other => panic!("no {other} in this harness"),
        }
    }

    fn park_group_reader(
        reg: &mut BlockingRegistry,
        key: &str,
        consumer: &str,
        claim: Option<ClaimToken>,
    ) -> OneshotReceiver<Option<Frame>> {
        let (tx, rx) = channel::oneshot();
        let wait_id = reg.next_wait_id();
        reg.register(
            0,
            b(key),
            WaitEntry {
                wait_id,
                cmd: BlockedCommand::XReadGroup {
                    group: b("g"),
                    consumer: b(consumer),
                    streams: vec![(b(key), StreamSince::Id(StreamId::ZERO))],
                    count: None,
                    noack: false,
                },
                reply_tx: tx,
                deadline: None,
                claim,
            },
        );
        rx
    }

    fn pending(db: &mut Database, key: &str) -> usize {
        let stream = db.get_stream(key.as_bytes()).unwrap().unwrap();
        stream.groups[b"g".as_ref()].pel.len()
    }

    /// moon#1047: a remote `XREADGROUP ... BLOCK` whose timeout settles its
    /// claim in the instant between this shard deciding to serve it and
    /// claiming it. The waiter answers nil — so the read must not have
    /// happened: nothing in its PEL, the cursor where it was, and the entry
    /// still there for the next `>` reader.
    #[test]
    fn a_group_reader_that_times_out_as_it_is_served_leaves_the_group_untouched() {
        let mut db = Database::new();
        let mut reg = BlockingRegistry::new(0);
        cmd(&mut db, &["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"]);
        let claim = ClaimToken::new();
        let rx = park_group_reader(&mut reg, "s", "c", Some(claim.clone()));
        cmd(&mut db, &["XADD", "s", "1-1", "f", "v"]);

        race::arm(true);
        let woke = try_wake_stream_waiter(&mut reg, &mut db, 0, &b("s"));
        race::arm(false);

        assert!(!woke, "nobody was answered");
        assert_eq!(claim.settle(), Settled::Dead, "the waiter gave up first");
        assert!(!matches!(rx.try_recv(), Ok(Some(_))), "and got no entries");
        assert_eq!(
            pending(&mut db, "s"),
            0,
            "an entry sits in the PEL of a consumer that was never handed it"
        );
        let next = cmd(
            &mut db,
            &["XREADGROUP", "GROUP", "g", "other", "STREAMS", "s", ">"],
        );
        assert!(
            matches!(next, Frame::Array(_)),
            "the entry must still be delivered to the next reader, got {next:?}"
        );
    }

    /// Looking at a parked group reader is not a write to its stream. A wake
    /// that serves nothing — this reader's group has nothing new — must leave
    /// the key's `WATCH` version alone, or a transaction watching the stream
    /// aborts although nothing in it changed. Redis creates the consumer of a
    /// blocked `XREADGROUP` with `keyModified(..., signal=0)`, which touches
    /// no watcher either, so a consumer this wake has to create is not a
    /// `WATCH` event.
    #[test]
    fn a_wake_that_serves_nothing_leaves_the_watch_version_alone() {
        let mut db = Database::new();
        let mut reg = BlockingRegistry::new(0);
        cmd(&mut db, &["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"]);
        cmd(&mut db, &["XADD", "s", "1-1", "f", "v"]);
        // `g` already delivered 1-1 elsewhere: nothing new for the reader.
        cmd(&mut db, &["XGROUP", "SETID", "s", "g", "1-1"]);
        let rx = park_group_reader(&mut reg, "s", "fresh-consumer", None);
        let before = db.get_version(b"s");

        assert!(!try_wake_stream_waiter(&mut reg, &mut db, 0, &b("s")));

        assert_eq!(
            db.get_version(b"s"),
            before,
            "a wake that served nothing stamped the stream's WATCH version"
        );
        assert!(rx.try_recv().is_err(), "the reader stays parked");
        let consumers = db.get_stream(b"s").unwrap().unwrap().groups[b"g".as_ref()]
            .consumers
            .len();
        assert_eq!(consumers, 1, "the consumer is created, as in redis");
    }

    /// moon#1104: the shard that serves a parked group read logs it — the
    /// records are in its writer channel before the wake returns, in the
    /// waiter's db, and they are what redis propagates for the read.
    #[test]
    fn a_wake_served_group_read_is_enqueued_before_the_wake_returns() {
        use crate::persistence::aof::{AofMessage, AofWriterPool};
        let (tx, rx) = flume::bounded::<AofMessage>(16);
        crate::blocking::pop_log::install(0, Some(AofWriterPool::top_level(tx)), None);
        let mut db = Database::new();
        let mut reg = BlockingRegistry::new(0);
        cmd(&mut db, &["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"]);
        let (reply_tx, reply_rx) = channel::oneshot();
        let wait_id = reg.next_wait_id();
        reg.register(
            3,
            b("s"),
            WaitEntry {
                wait_id,
                cmd: BlockedCommand::XReadGroup {
                    group: b("g"),
                    consumer: b("c"),
                    streams: vec![(b("s"), StreamSince::Id(StreamId::ZERO))],
                    count: None,
                    noack: false,
                },
                reply_tx,
                deadline: None,
                claim: Some(ClaimToken::new()),
            },
        );
        cmd(&mut db, &["XADD", "s", "5-1", "f", "v"]);

        assert!(try_wake_stream_waiter(&mut reg, &mut db, 3, &b("s")));

        let mut got = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            if let AofMessage::Append { db, bytes, .. } = msg {
                got.push((db, String::from_utf8_lossy(&bytes).replace("\r\n", " ")));
            }
        }
        assert_eq!(got.len(), 2, "one XCLAIM and one SETID: {got:?}");
        assert!(
            got.iter().all(|(d, _)| *d == 3),
            "logged in the waiter's db"
        );
        assert!(
            got[0]
                .1
                .contains("XCLAIM $1 s $1 g $1 c $1 0 $3 5-1 $4 TIME")
                && got[0]
                    .1
                    .contains("RETRYCOUNT $1 1 $5 FORCE $6 JUSTID $6 LASTID $3 0-0"),
            "{:?}",
            got[0].1
        );
        assert!(
            got[1].1.contains("XGROUP $5 SETID $1 s $1 g $3 5-1"),
            "{:?}",
            got[1].1
        );
        assert!(matches!(reply_rx.try_recv(), Ok(Some(Frame::Array(_)))));
        crate::blocking::pop_log::uninstall();
    }

    /// moon#1086: a group reader whose stream is deleted, retyped, or loses
    /// its group is answered at once with redis's error; a plain `XREAD`
    /// parked on the same key stays parked.
    #[test]
    fn a_group_reader_whose_stream_goes_is_answered_its_error() {
        type Change = fn(&mut Database);
        let nogroup =
            "NOGROUP No such key 's' or consumer group 'g' in XREADGROUP with GROUP option";
        let cases: [(&str, Change, &str); 3] = [
            (
                "deleted",
                |db| {
                    db.remove(b"s");
                },
                nogroup,
            ),
            (
                "retyped",
                |db| {
                    db.remove(b"s");
                    db.list_push_back(&Bytes::from_static(b"s"), Bytes::from_static(b"x"));
                },
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            ),
            (
                "group destroyed",
                |db| {
                    cmd(db, &["XGROUP", "DESTROY", "s", "g"]);
                },
                nogroup,
            ),
        ];
        for (label, change, want) in cases {
            let mut db = Database::new();
            let mut reg = BlockingRegistry::new(0);
            cmd(&mut db, &["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"]);
            let claim = ClaimToken::new();
            let group_rx = park_group_reader(&mut reg, "s", "c", Some(claim.clone()));
            assert!(reg.has_group_readers(), "{label}");
            let (xtx, xrx) = channel::oneshot();
            let wait_id = reg.next_wait_id();
            reg.register(
                0,
                b("s"),
                WaitEntry {
                    wait_id,
                    cmd: BlockedCommand::XRead {
                        streams: vec![(b("s"), StreamSince::Id(StreamId::ZERO))],
                        count: None,
                    },
                    reply_tx: xtx,
                    deadline: None,
                    claim: None,
                },
            );
            change(&mut db);

            assert!(
                try_wake_stream_waiter(&mut reg, &mut db, 0, &b("s")),
                "{label}"
            );
            match group_rx.try_recv() {
                Ok(Some(Frame::Error(e))) => assert_eq!(&e[..], want.as_bytes(), "{label}"),
                other => panic!("{label}: expected the error, got {other:?}"),
            }
            assert_eq!(
                claim.settle(),
                Settled::Claimed,
                "{label}: answered on a won claim"
            );
            assert!(xrx.try_recv().is_err(), "{label}: the XREAD stays parked");
            assert!(reg.has_waiters(0, b"s"), "{label}");
            assert!(
                !reg.has_group_readers(),
                "{label}: no group reader left parked"
            );
        }
    }

    /// moon#1086 through the write hook itself: a `SET` (`@string`, which the
    /// cheap gate skips) over a stream reaches its parked group reader while
    /// one is parked on this thread — and only then.
    #[test]
    fn a_set_over_the_stream_reaches_the_parked_group_reader() {
        let mut db = Database::new();
        cmd(&mut db, &["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"]);
        let set_args = [Frame::BulkString(b("s")), Frame::BulkString(b("x"))];
        let reg = std::cell::RefCell::new(BlockingRegistry::new(0));
        assert!(
            !crate::blocking::wakeup::may_wake(b"SET"),
            "no group reader parked: SET keeps the cheap gate"
        );
        let rx = park_group_reader(&mut reg.borrow_mut(), "s", "c", None);
        assert!(crate::blocking::wakeup::may_wake(b"SET"));
        crate::command::string::set(&mut db, &set_args);
        assert!(crate::blocking::wakeup::wake_written_keys(
            &reg, &mut db, 0, b"SET", &set_args
        ));
        assert!(matches!(rx.try_recv(), Ok(Some(Frame::Error(e))) if e.starts_with(b"WRONGTYPE")));
        assert!(
            !crate::blocking::wakeup::may_wake(b"SET"),
            "and the gate narrows again"
        );
    }
}
