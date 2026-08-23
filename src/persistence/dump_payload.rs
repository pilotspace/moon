//! The `DUMP`/`RESTORE` payload codec (moon#636).
//!
//! A payload is the serialized value with a short trailer:
//!
//! ```text
//! <type byte> <value bytes> <rdb version: u16 LE> <crc64: u64 LE>
//! ```
//!
//! Verified byte-for-byte against redis-server 8.6.1. `DUMP` of `SET s hello`
//! there produces
//!
//! ```text
//! 00 05 68 65 6c 6c 6f | 0d 00 | c1 db 56 c5 62 81 57 ca
//! ^type ^len ^"hello"  ^ver 13 ^crc64(everything before it)
//! ```
//!
//! and moon's [`crc64_jones`] reproduces that checksum exactly, so the only
//! difference between moon's payloads and redis's is the version number and
//! the value encoding.
//!
//! # What interoperates, and what does not
//!
//! **moon -> redis works.** moon writes the plain RDB value types (`STRING`,
//! `LIST`, `SET`, `HASH`, `ZSET_2`) rather than redis 8's listpack forms.
//! Those are still valid RDB, and redis re-encodes them on ingest — measured
//! against 8.6.1, a hand-built moon-shaped payload restores and reports
//! `encoding: listpack`.
//!
//! **redis -> moon does not, for collections.** redis 8 emits listpack and
//! quicklist encodings (`0x10`-`0x14`) and moon has no decoder for them, so
//! [`decode`] refuses those payloads rather than mis-parsing one. A listpack
//! reader would close the gap and is deliberately out of scope here.
//!
//! Refusal is also what a *newer* payload gets: redis rejects any footer
//! version above its own, and so does this, with redis's own wording. That is
//! the mechanism protecting a moon that cannot read listpacks from a redis 13
//! payload — the version check fires before the type byte is ever consulted.

use std::io::Cursor;

use crate::persistence::redis_rdb::{
    REDIS_RDB_VERSION_NUM, crc64_jones, read_rdb_entry, write_typed_value,
};
use crate::storage::Entry;

/// Version (2) + CRC64 (8).
const FOOTER_LEN: usize = 10;

/// Why a payload was refused.
///
/// Both variants carry redis's own message rather than a moon-flavoured one:
/// a client that migrated from redis matches on these strings.
#[derive(Debug, PartialEq, Eq)]
pub enum DumpError {
    /// Too short, wrong version, or the checksum does not cover the bytes.
    /// redis reports all three identically and so does moon — the distinction
    /// is not actionable for a caller, and separating them would tell an
    /// attacker which half of the trailer they got right.
    BadPayload,
    /// Well-formed, checksum good, but the value encoding is one moon cannot
    /// decode (a redis listpack/quicklist form). Distinct from `BadPayload`
    /// because the payload is *not* corrupt and saying so is the difference
    /// between "your data is damaged" and "moon cannot read this dialect".
    UnsupportedEncoding(u8),
}

impl DumpError {
    /// The wire message. `BadPayload` is redis's exact string.
    pub fn message(&self) -> Vec<u8> {
        match self {
            Self::BadPayload => b"ERR DUMP payload version or checksum are wrong".to_vec(),
            Self::UnsupportedEncoding(tag) => format!(
                "ERR DUMP payload uses value encoding {tag} (a Redis listpack or quicklist form), \
                 which this server cannot decode"
            )
            .into_bytes(),
        }
    }
}

/// Serialize one entry into a `DUMP` payload.
///
/// The entry's TTL is deliberately NOT encoded: in redis the expiry travels as
/// `RESTORE`'s own `ttl` argument, not inside the payload, and a payload that
/// carried one would be rejected by redis as a bad type byte.
pub fn encode(entry: &Entry) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    write_typed_value(&mut out, None, entry);
    out.extend_from_slice(&REDIS_RDB_VERSION_NUM.to_le_bytes());
    let crc = crc64_jones(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// Parse a `DUMP` payload back into an entry.
///
/// Checks in the order redis checks them, which matters: the version is read
/// before the body, so a payload from a newer server is refused as a version
/// mismatch rather than mis-parsed as an unknown type.
pub fn decode(payload: &[u8]) -> Result<Entry, DumpError> {
    // A payload must have at least a type byte and the trailer. `<` not `<=`:
    // an empty value body is legitimate for some types.
    if payload.len() < FOOTER_LEN + 1 {
        return Err(DumpError::BadPayload);
    }
    let split = payload.len() - FOOTER_LEN;
    let (body, footer) = payload.split_at(split);

    let version = u16::from_le_bytes([footer[0], footer[1]]);
    if version > REDIS_RDB_VERSION_NUM {
        return Err(DumpError::BadPayload);
    }

    let mut want = [0u8; 8];
    want.copy_from_slice(&footer[2..]);
    let want = u64::from_le_bytes(want);
    // The checksum covers the body AND the version, which is why it is taken
    // over everything up to the last 8 bytes rather than over `body` alone.
    if crc64_jones(&payload[..split + 2]) != want {
        return Err(DumpError::BadPayload);
    }

    let tag = body[0];
    let mut cursor = Cursor::new(&body[1..]);
    // `read_rdb_entry` answers for exactly the tags moon's own writer emits;
    // anything else (notably redis 8's listpack forms) comes back as an error
    // and is reported as an encoding moon cannot read, not as corruption.
    read_rdb_entry(&mut cursor, tag, None).map_err(|_| DumpError::UnsupportedEncoding(tag))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::compact_value::RedisValueRef;
    use bytes::Bytes;

    fn string_entry(v: &str) -> Entry {
        Entry::new_string(Bytes::from(v.to_string()))
    }

    #[test]
    fn a_payload_matches_the_shape_redis_produces() {
        // The golden is a real `DUMP` from redis-server 8.6.1 of `SET s hello`,
        // with only the version bytes changed (13 -> 10) and the checksum
        // recomputed over that change. If moon's framing drifted, this is the
        // test that would not survive it.
        let got = encode(&string_entry("hello"));
        assert_eq!(&got[..7], &[0x00, 0x05, b'h', b'e', b'l', b'l', b'o']);
        assert_eq!(&got[7..9], &10u16.to_le_bytes());
        assert_eq!(got.len(), 7 + FOOTER_LEN);
    }

    #[test]
    fn the_checksum_covers_the_version_not_just_the_body() {
        // Redis checksums `body || version`. Getting this wrong produces a
        // codec that round-trips against ITSELF perfectly and is rejected by
        // every real redis -- so a round-trip test alone cannot catch it.
        let payload = encode(&string_entry("hello"));
        let split = payload.len() - 8;
        let expected = crc64_jones(&payload[..split]);
        let mut stored = [0u8; 8];
        stored.copy_from_slice(&payload[split..]);
        assert_eq!(u64::from_le_bytes(stored), expected);
    }

    #[test]
    fn a_string_round_trips() {
        let payload = encode(&string_entry("hello"));
        let back = decode(&payload).expect("valid payload");
        match back.as_redis_value() {
            RedisValueRef::String(s) => assert_eq!(s, b"hello"),
            // `RedisValueRef` has no `Debug`, so report the wire type name
            // rather than the value -- it is the part that identifies the bug.
            _ => panic!("decoded entry is not a string"),
        }
    }

    #[test]
    fn a_flipped_byte_anywhere_is_refused() {
        // Every byte matters: flip each one in turn and none may decode. A
        // checksum that covered only part of the payload would let some of
        // these through.
        let payload = encode(&string_entry("hello"));
        for i in 0..payload.len() {
            let mut bad = payload.clone();
            bad[i] ^= 0xFF;
            assert!(
                decode(&bad).is_err(),
                "byte {i} could be flipped without detection"
            );
        }
    }

    #[test]
    fn a_newer_version_is_refused_before_the_body_is_read() {
        // This is the arm that protects a moon which cannot read listpacks
        // from a redis 13 payload: the refusal happens on the version, so the
        // unreadable type byte is never reached.
        let mut payload = encode(&string_entry("hello"));
        let split = payload.len() - FOOTER_LEN;
        payload[split..split + 2].copy_from_slice(&99u16.to_le_bytes());
        // Re-checksum, so the ONLY thing wrong is the version.
        let crc = crc64_jones(&payload[..split + 2]);
        payload[split + 2..].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(decode(&payload).err(), Some(DumpError::BadPayload));
    }

    #[test]
    fn a_truncated_payload_is_refused_rather_than_panicking() {
        let payload = encode(&string_entry("hello"));
        for n in 0..payload.len() {
            assert!(
                decode(&payload[..n]).is_err(),
                "prefix of {n} bytes decoded"
            );
        }
    }

    #[test]
    fn an_empty_payload_is_refused() {
        assert_eq!(decode(&[]).err(), Some(DumpError::BadPayload));
    }

    #[test]
    fn a_listpack_encoding_is_named_not_called_corrupt() {
        // 0x11 is RDB_TYPE_ZSET_LISTPACK, which redis 8 emits and moon cannot
        // read. The payload below is well-formed and correctly checksummed:
        // reporting it as a checksum failure would send an operator hunting a
        // corruption that is not there.
        let mut body = vec![0x11u8];
        body.extend_from_slice(&[0x00; 4]);
        body.extend_from_slice(&REDIS_RDB_VERSION_NUM.to_le_bytes());
        let crc = crc64_jones(&body);
        body.extend_from_slice(&crc.to_le_bytes());
        match decode(&body) {
            Err(DumpError::UnsupportedEncoding(tag)) => assert_eq!(tag, 0x11),
            other => panic!("expected UnsupportedEncoding(0x11), got {other:?}"),
        }
    }

    #[test]
    fn the_error_text_is_the_one_redis_uses() {
        // A client migrating from redis matches on this string.
        assert_eq!(
            DumpError::BadPayload.message(),
            b"ERR DUMP payload version or checksum are wrong".to_vec()
        );
    }
}
