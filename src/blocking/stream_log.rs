//! The durability records of a consumer-group read served through the
//! blocking machinery (moon#1104).
//!
//! `XREADGROUP ... >` is a write: it moves every entry it delivers into the
//! group's pending list (unless `NOACK`), advances the group's last-delivered
//! id, and may create the consumer. A blocking read reaches the keyspace
//! through the blocking intercept, not the ordinary write exit, so — exactly
//! like the blocking pops before moon#1056 — it logged nothing: after `kill -9`
//! the PEL was empty and the entries were delivered a second time.
//!
//! The record is not the command itself (a replayed `BLOCK` would park the
//! loader). It is what redis-server 8.6.1 propagates for an `XREADGROUP`
//! (`streamReplyWithRange` / `xreadCommand`, confirmed against its AOF):
//!
//! ```text
//! XGROUP CREATECONSUMER key group consumer        -- NOACK, consumer new
//! XCLAIM key group consumer 0 <id> TIME <delivery-ms> RETRYCOUNT <count>
//!        FORCE JUSTID LASTID <group's last id BEFORE the read>
//!                                                 -- one per delivered entry,
//!                                                    unless NOACK
//! XGROUP SETID key group <last delivered id>      -- when the read advanced it
//! ```
//!
//! `FORCE` recreates the pending entry on the replaying side, `TIME` and
//! `RETRYCOUNT` restore its delivery metadata exactly (a replayed read would
//! stamp the replay time), and `SETID` moves the cursor. Two deliberate
//! differences from redis's bytes, neither of which changes what a moon
//! replay or replica ends up holding:
//!
//! * redis appends `ENTRIESREAD <n>` to the `SETID`. Moon keeps no
//!   entries-read counter (`XINFO GROUPS` reports it as nil), so any number
//!   written here would be invented.
//! * redis wraps a multi-record serve in `MULTI` / `EXEC`. Moon's log never
//!   carries `MULTI` (an `EXEC` body is logged unwrapped too); the records are
//!   enqueued back to back in one synchronous stretch of the owning shard, so
//!   nothing can fall between them, and [`crate::blocking::pop_log::log_records`]
//!   keeps the AOF a prefix of them if the writer refuses one.

use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::stream::StreamId;

/// Records of one group read, in log order.
pub(crate) type GroupReadRecords = smallvec::SmallVec<[Frame; 4]>;

/// What the records need from the group as it was BEFORE the read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GroupReadBefore {
    /// The group's last-delivered id — the `LASTID` of every `XCLAIM`, and
    /// the baseline that decides whether a `SETID` is owed.
    pub last_delivered: StreamId,
    /// Whether the consumer already existed; a `NOACK` read that creates it
    /// owes an `XGROUP CREATECONSUMER`.
    pub consumer_existed: bool,
}

/// Snapshot [`GroupReadBefore`] for `group` on the stream at `key`, or `None`
/// when there is no such stream or group (a read against it answers an error
/// and changes nothing).
pub(crate) fn group_read_before(
    db: &mut Database,
    key: &[u8],
    group: &[u8],
    consumer: &[u8],
) -> Option<GroupReadBefore> {
    let stream = db.get_stream(key).ok()??;
    let g = stream.groups.get(group)?;
    Some(GroupReadBefore {
        last_delivered: g.last_delivered_id,
        consumer_existed: g.consumers.contains_key(consumer),
    })
}

fn bulk(s: &'static [u8]) -> Frame {
    Frame::BulkString(Bytes::from_static(s))
}

fn bulk_u64(n: u64) -> Frame {
    let mut b = itoa::Buffer::new();
    Frame::BulkString(Bytes::copy_from_slice(b.format(n).as_bytes()))
}

fn bulk_id(id: StreamId) -> Frame {
    // `ms-seq` is at most 41 bytes; built without `format!`.
    let mut out = Vec::with_capacity(41);
    let mut b = itoa::Buffer::new();
    out.extend_from_slice(b.format(id.ms).as_bytes());
    out.push(b'-');
    out.extend_from_slice(b.format(id.seq).as_bytes());
    Frame::BulkString(Bytes::from(out))
}

/// The records a `>` read of `group` by `consumer` on `key` owes, given the
/// state it left behind in `db`, the ids it `delivered` (in order) and the
/// group as it was [`before`](group_read_before) it ran. Empty when the read
/// changed nothing that propagates (nothing delivered, and no `NOACK`
/// consumer created).
pub(crate) fn group_read_records(
    db: &mut Database,
    key: &Bytes,
    group: &Bytes,
    consumer: &Bytes,
    noack: bool,
    before: &GroupReadBefore,
    delivered: &[StreamId],
) -> GroupReadRecords {
    let mut out = GroupReadRecords::new();
    let Ok(Some(stream)) = db.get_stream(key) else {
        return out;
    };
    let Some(g) = stream.groups.get(group.as_ref()) else {
        return out;
    };
    if noack && !before.consumer_existed && g.consumers.contains_key(consumer.as_ref()) {
        out.push(Frame::Array(framevec![
            bulk(b"XGROUP"),
            bulk(b"CREATECONSUMER"),
            Frame::BulkString(key.clone()),
            Frame::BulkString(group.clone()),
            Frame::BulkString(consumer.clone()),
        ]));
    }
    if !noack {
        for &id in delivered {
            // Every delivered id is pending right after a non-NOACK read; one
            // that is not was never moved, and claiming it would invent a
            // delivery.
            let Some(pe) = g.pel.get(&id) else {
                continue;
            };
            out.push(Frame::Array(framevec![
                bulk(b"XCLAIM"),
                Frame::BulkString(key.clone()),
                Frame::BulkString(group.clone()),
                Frame::BulkString(pe.consumer.clone()),
                bulk(b"0"),
                bulk_id(id),
                bulk(b"TIME"),
                bulk_u64(pe.delivery_time),
                bulk(b"RETRYCOUNT"),
                bulk_u64(pe.delivery_count),
                bulk(b"FORCE"),
                bulk(b"JUSTID"),
                bulk(b"LASTID"),
                bulk_id(before.last_delivered),
            ]));
        }
    }
    if g.last_delivered_id != before.last_delivered {
        out.push(Frame::Array(framevec![
            bulk(b"XGROUP"),
            bulk(b"SETID"),
            Frame::BulkString(key.clone()),
            Frame::BulkString(group.clone()),
            bulk_id(g.last_delivered_id),
        ]));
    }
    out
}

/// The entry ids an `XREADGROUP` reply delivered, in order, read from its
/// RESP2 shape `[[key, [[id, [field, value, ...]], ...]], ...]`. Empty for a
/// null, an error, or any other shape.
pub(crate) fn delivered_ids(reply: &Frame) -> smallvec::SmallVec<[StreamId; 8]> {
    let mut ids = smallvec::SmallVec::new();
    let Frame::Array(streams) = reply else {
        return ids;
    };
    for stream in streams.iter() {
        let Frame::Array(pair) = stream else { continue };
        let Some(Frame::Array(entries)) = pair.get(1) else {
            continue;
        };
        for entry in entries.iter() {
            if let Frame::Array(e) = entry
                && let Some(Frame::BulkString(id)) = e.first()
                && let Ok(id) = StreamId::parse(id, 0)
            {
                ids.push(id);
            }
        }
    }
    ids
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn flat(f: &Frame) -> String {
        let Frame::Array(items) = f else {
            return "<not-an-array>".into();
        };
        items
            .iter()
            .map(|i| match i {
                Frame::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                other => format!("{other:?}"),
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn stream_with_group(db: &mut Database, key: &str, ids: &[(u64, u64)]) {
        let args: Vec<Frame> = ["CREATE", key, "g", "$", "MKSTREAM"]
            .iter()
            .map(|s| Frame::BulkString(b(s)))
            .collect();
        crate::command::stream::xgroup(db, &args);
        for &(ms, seq) in ids {
            let id = format!("{ms}-{seq}");
            let args: Vec<Frame> = [key, id.as_str(), "f", "v"]
                .iter()
                .map(|s| Frame::BulkString(b(s)))
                .collect();
            crate::command::stream::xadd(db, &args);
        }
    }

    fn read(db: &mut Database, key: &str, consumer: &str, noack: bool) -> Frame {
        let mut parts = vec!["GROUP", "g", consumer];
        if noack {
            parts.push("NOACK");
        }
        parts.extend(["STREAMS", key, ">"]);
        let args: Vec<Frame> = parts.iter().map(|s| Frame::BulkString(b(s))).collect();
        crate::command::stream::xreadgroup(db, &args)
    }

    /// The shape redis-server 8.6.1 writes to its AOF for the same read: one
    /// XCLAIM per entry (LASTID = the cursor BEFORE the read), then SETID.
    #[test]
    fn a_read_is_one_xclaim_per_entry_then_setid() {
        let mut db = Database::new();
        stream_with_group(&mut db, "s", &[(1, 1), (1, 2)]);
        let before = group_read_before(&mut db, b"s", b"g", b"c").unwrap();
        let reply = read(&mut db, "s", "c", false);
        let ids = delivered_ids(&reply);
        assert_eq!(ids.len(), 2);
        let recs = group_read_records(&mut db, &b("s"), &b("g"), &b("c"), false, &before, &ids);
        let got: Vec<String> = recs.iter().map(flat).collect();
        let stream = db.get_stream(b"s").unwrap().unwrap();
        let t = stream.groups[b"g".as_ref()].pel[&StreamId { ms: 1, seq: 1 }].delivery_time;
        assert_eq!(
            got,
            vec![
                format!("XCLAIM s g c 0 1-1 TIME {t} RETRYCOUNT 1 FORCE JUSTID LASTID 0-0"),
                format!("XCLAIM s g c 0 1-2 TIME {t} RETRYCOUNT 1 FORCE JUSTID LASTID 0-0"),
                "XGROUP SETID s g 1-2".to_string(),
            ]
        );
    }

    /// NOACK: no PEL, so no XCLAIM — the consumer's creation and the cursor.
    #[test]
    fn a_noack_read_creates_the_consumer_and_moves_the_cursor() {
        let mut db = Database::new();
        stream_with_group(&mut db, "n", &[(1, 1)]);
        let before = group_read_before(&mut db, b"n", b"g", b"c2").unwrap();
        let reply = read(&mut db, "n", "c2", true);
        let recs = group_read_records(
            &mut db,
            &b("n"),
            &b("g"),
            &b("c2"),
            true,
            &before,
            &delivered_ids(&reply),
        );
        let got: Vec<String> = recs.iter().map(flat).collect();
        assert_eq!(
            got,
            vec!["XGROUP CREATECONSUMER n g c2", "XGROUP SETID n g 1-1"]
        );
    }

    /// A read that delivered nothing and created no NOACK consumer logs
    /// nothing — including the non-NOACK consumer it did create, which redis
    /// does not propagate either.
    #[test]
    fn a_read_that_found_nothing_logs_nothing() {
        let mut db = Database::new();
        stream_with_group(&mut db, "e", &[]);
        let before = group_read_before(&mut db, b"e", b"g", b"c").unwrap();
        let reply = read(&mut db, "e", "c", false);
        assert!(matches!(reply, Frame::NullArray));
        assert!(
            group_read_records(&mut db, &b("e"), &b("g"), &b("c"), false, &before, &[]).is_empty()
        );
    }

    #[test]
    fn delivered_ids_ignores_what_is_not_a_read_reply() {
        assert!(delivered_ids(&Frame::NullArray).is_empty());
        assert!(delivered_ids(&Frame::Error(b("NOGROUP x"))).is_empty());
    }
}
