//! The cold-graves trailer of a shard snapshot (moon#1281).
//!
//! Without an AOF the durable state after a crash is the shard's last
//! snapshot (the HOT keyspace) plus every spill file the manifest lists. A
//! spill file is unlinked only when its last live key leaves it, so a file
//! with live neighbours keeps the slots of keys that have since been deleted,
//! overwritten, flushed or read back into RAM, and the boot rebuild indexes
//! every slot of every listed file. Nothing durable said such a slot was
//! dead: a cold key deleted before a successful `BGSAVE` came back after a
//! kill -9 (36-46 of 100 probes in the issue's repro).
//!
//! The snapshot now says it. At the snapshot's start the shard encodes the
//! dead slots its cold indexes hold (`storage::tiered::slot_graves`) —
//! every slot that was already dead at that instant — and the snapshot
//! carries them as a trailer. The boot of a no-AOF process drops exactly
//! those slots before it resolves each key's newest copy
//! (`ColdIndex::rebuild_from_manifest_per_db_with_graves`). A slot that dies
//! AFTER the start is not in this snapshot: the key was alive at the
//! snapshot's instant, so coming back is what the snapshot's point in time
//! says. The contract is the issue's: a cold deletion is durable no later
//! than the next successful snapshot.
//!
//! # Where it sits, and why no version bump
//!
//! The trailer goes AFTER the `EOF` marker and before the global CRC32:
//!
//! ```text
//! ... segment blocks ... EOF(0xFF) | trailer | global_crc32
//! trailer = "MCGV" | ver u8 (=1) | file_count u32
//!           | { file_id u64 | slot_count u32 | { page_idx u32 | slot_idx u16 } * slot_count } * file_count
//!           | crc32 u32 (over "MCGV" .. last slot)
//! ```
//!
//! Every reader of shard snapshots v1-v3 stops at `EOF` and verifies the
//! global CRC over the whole payload, so it loads the file and ignores the
//! trailer: a binary older than this one boots from a new snapshot exactly
//! as it booted from an old one (the deleted slots come back, as before).
//! The format version stays 3; old snapshots have no trailer and mean "no
//! graves", which is also the old behaviour. A trailer that fails its own
//! checks is logged and ignored — never a reason to refuse the key data the
//! global CRC already proved.
//!
//! Slots are identified by location, not by key: `(file_id, page, slot)` is
//! exact (a key may have a dead slot in one file and its live one in
//! another), needs no key bytes (6 B per slot on disk, 8 B in RAM), and file
//! ids are never reissued (moon#1067), so a stale grave can never hit a new
//! file's slot.

use std::collections::{HashMap, HashSet};

/// Trailer magic: "Moon Cold GraVes".
const MAGIC: &[u8; 4] = b"MCGV";
/// Trailer layout version.
const VERSION: u8 = 1;
/// magic + version + file_count.
const HEADER_LEN: usize = 4 + 1 + 4;
/// file_id + slot_count.
const FILE_HEADER_LEN: usize = 8 + 4;
/// page_idx + slot_idx.
const SLOT_LEN: usize = 4 + 2;
/// The trailer's own CRC32.
const CRC_LEN: usize = 4;

/// Pack a slot's page and index into one `u64` (the in-RAM form).
#[inline]
#[must_use]
pub fn pack_slot(page_idx: u32, slot_idx: u16) -> u64 {
    (u64::from(page_idx) << 16) | u64::from(slot_idx)
}

/// Inverse of [`pack_slot`].
#[inline]
#[must_use]
pub fn unpack_slot(packed: u64) -> (u32, u16) {
    ((packed >> 16) as u32, (packed & 0xFFFF) as u16)
}

/// The dead slots a snapshot carries: file id -> packed slots.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ColdGraves {
    by_file: HashMap<u64, HashSet<u64>>,
}

impl ColdGraves {
    /// Whether `(file_id, page_idx, slot_idx)` is a grave.
    #[inline]
    #[must_use]
    pub fn contains(&self, file_id: u64, page_idx: u32, slot_idx: u16) -> bool {
        self.by_file
            .get(&file_id)
            .is_some_and(|s| s.contains(&pack_slot(page_idx, slot_idx)))
    }

    /// Whether `file_id` has any grave.
    #[inline]
    #[must_use]
    pub fn has_file(&self, file_id: u64) -> bool {
        self.by_file.contains_key(&file_id)
    }

    /// Number of graves.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_file.values().map(HashSet::len).sum()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_file.is_empty()
    }

    /// Every grave as `(file_id, packed slots)`, both sorted — the input
    /// [`encode`] takes (round-trip tests and the fuzz target).
    #[must_use]
    pub fn to_files(&self) -> Vec<(u64, Vec<u64>)> {
        let mut files: Vec<(u64, Vec<u64>)> = self
            .by_file
            .iter()
            .map(|(&f, s)| {
                let mut v: Vec<u64> = s.iter().copied().collect();
                v.sort_unstable();
                (f, v)
            })
            .collect();
        files.sort_unstable_by_key(|(f, _)| *f);
        files
    }

    /// Add one grave (tests, and the decoder).
    pub fn insert(&mut self, file_id: u64, page_idx: u32, slot_idx: u16) {
        self.by_file
            .entry(file_id)
            .or_default()
            .insert(pack_slot(page_idx, slot_idx));
    }
}

/// Why a trailer was not accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GravesError {
    /// Fewer bytes than the layout needs.
    Truncated,
    /// Not a cold-graves trailer.
    BadMagic,
    /// A trailer layout this binary does not know.
    UnknownVersion(u8),
    /// The trailer's own CRC32 does not match.
    BadChecksum,
    /// Bytes left over after the declared content.
    TrailingBytes,
}

impl std::fmt::Display for GravesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => f.write_str("truncated"),
            Self::BadMagic => f.write_str("bad magic"),
            Self::UnknownVersion(v) => write!(f, "unknown version {v}"),
            Self::BadChecksum => f.write_str("checksum mismatch"),
            Self::TrailingBytes => f.write_str("trailing bytes"),
        }
    }
}

/// Encode `files` (file id, packed slots) as a trailer. Empty input encodes
/// to nothing: a snapshot with no graves is byte-identical to the old format.
#[must_use]
pub fn encode(files: &[(u64, Vec<u64>)]) -> Vec<u8> {
    let files: Vec<&(u64, Vec<u64>)> = files.iter().filter(|(_, s)| !s.is_empty()).collect();
    if files.is_empty() {
        return Vec::new();
    }
    let slots: usize = files.iter().map(|(_, s)| s.len()).sum();
    let mut out =
        Vec::with_capacity(HEADER_LEN + files.len() * FILE_HEADER_LEN + slots * SLOT_LEN + CRC_LEN);
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    out.extend_from_slice(&(files.len() as u32).to_le_bytes());
    for (file_id, packed) in files {
        out.extend_from_slice(&file_id.to_le_bytes());
        out.extend_from_slice(&(packed.len() as u32).to_le_bytes());
        for &p in packed {
            let (page, slot) = unpack_slot(p);
            out.extend_from_slice(&page.to_le_bytes());
            out.extend_from_slice(&slot.to_le_bytes());
        }
    }
    let crc = crc32fast::hash(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

#[inline]
fn take<'a>(buf: &mut &'a [u8], n: usize) -> Result<&'a [u8], GravesError> {
    if buf.len() < n {
        return Err(GravesError::Truncated);
    }
    let (head, rest) = buf.split_at(n);
    *buf = rest;
    Ok(head)
}

#[inline]
fn le_u32(b: &[u8]) -> u32 {
    let mut a = [0u8; 4];
    a.copy_from_slice(&b[..4]);
    u32::from_le_bytes(a)
}

/// Decode a trailer (the bytes between `EOF` and the global CRC). Never
/// panics and never allocates past what the input can hold: every count is
/// checked against the bytes left before anything is reserved.
pub fn decode(bytes: &[u8]) -> Result<ColdGraves, GravesError> {
    if bytes.len() < HEADER_LEN + CRC_LEN {
        return Err(GravesError::Truncated);
    }
    if &bytes[..4] != MAGIC {
        return Err(GravesError::BadMagic);
    }
    if bytes[4] != VERSION {
        return Err(GravesError::UnknownVersion(bytes[4]));
    }
    let (body, crc) = bytes.split_at(bytes.len() - CRC_LEN);
    if crc32fast::hash(body) != le_u32(crc) {
        return Err(GravesError::BadChecksum);
    }
    let mut rest = &body[HEADER_LEN..];
    let file_count = le_u32(&body[5..9]) as usize;
    if file_count > rest.len() / FILE_HEADER_LEN {
        return Err(GravesError::Truncated);
    }
    let mut graves = ColdGraves::default();
    for _ in 0..file_count {
        let head = take(&mut rest, FILE_HEADER_LEN)?;
        let mut id = [0u8; 8];
        id.copy_from_slice(&head[..8]);
        let file_id = u64::from_le_bytes(id);
        let slot_count = le_u32(&head[8..12]) as usize;
        if slot_count > rest.len() / SLOT_LEN {
            return Err(GravesError::Truncated);
        }
        let set = graves.by_file.entry(file_id).or_default();
        set.reserve(slot_count);
        for _ in 0..slot_count {
            let s = take(&mut rest, SLOT_LEN)?;
            let page = le_u32(&s[..4]);
            let slot = u16::from_le_bytes([s[4], s[5]]);
            set.insert(pack_slot(page, slot));
        }
    }
    if !rest.is_empty() {
        return Err(GravesError::TrailingBytes);
    }
    graves.by_file.retain(|_, s| !s.is_empty());
    Ok(graves)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_encodes_to_nothing() {
        assert!(encode(&[]).is_empty());
        assert!(encode(&[(7, Vec::new())]).is_empty());
    }

    #[test]
    fn round_trip_keeps_every_slot_exactly() {
        let files = vec![
            (
                3u64,
                vec![pack_slot(0, 0), pack_slot(0, 255), pack_slot(9, 1)],
            ),
            (u64::MAX, vec![pack_slot(u32::MAX, u16::MAX)]),
        ];
        let g = decode(&encode(&files)).expect("decode");
        assert_eq!(g.len(), 4);
        assert!(g.contains(3, 0, 0));
        assert!(g.contains(3, 0, 255));
        assert!(g.contains(3, 9, 1));
        assert!(!g.contains(3, 9, 2), "a neighbouring slot is not a grave");
        assert!(!g.contains(4, 0, 0), "another file's slot is not a grave");
        assert!(g.contains(u64::MAX, u32::MAX, u16::MAX));
        assert!(g.has_file(3) && !g.has_file(4));
    }

    #[test]
    fn every_corruption_is_refused_never_a_panic() {
        let good = encode(&[(1, vec![pack_slot(2, 3)])]);
        for cut in 0..good.len() {
            assert!(decode(&good[..cut]).is_err(), "prefix of {cut} bytes");
        }
        for i in 0..good.len() {
            let mut bad = good.clone();
            bad[i] ^= 0x40;
            assert!(decode(&bad).is_err(), "flipped byte {i}");
        }
        let mut long = good.clone();
        long.insert(good.len() - CRC_LEN, 0);
        assert!(decode(&long).is_err());
    }

    /// A huge declared count with a valid CRC must not reserve memory for it.
    #[test]
    fn a_count_larger_than_the_input_is_truncated_not_an_allocation() {
        let mut body = Vec::new();
        body.extend_from_slice(MAGIC);
        body.push(VERSION);
        body.extend_from_slice(&u32::MAX.to_le_bytes());
        let crc = crc32fast::hash(&body);
        body.extend_from_slice(&crc.to_le_bytes());
        assert_eq!(decode(&body), Err(GravesError::Truncated));

        let mut body = Vec::new();
        body.extend_from_slice(MAGIC);
        body.push(VERSION);
        body.extend_from_slice(&1u32.to_le_bytes());
        body.extend_from_slice(&5u64.to_le_bytes());
        body.extend_from_slice(&u32::MAX.to_le_bytes());
        let crc = crc32fast::hash(&body);
        body.extend_from_slice(&crc.to_le_bytes());
        assert_eq!(decode(&body), Err(GravesError::Truncated));
    }

    #[test]
    fn pack_is_lossless() {
        for (p, s) in [(0u32, 0u16), (1, 1), (u32::MAX, u16::MAX), (123_456, 255)] {
            assert_eq!(unpack_slot(pack_slot(p, s)), (p, s));
        }
    }
}
