//! PFADD, PFCOUNT, PFMERGE command handlers.
//!
//! HLL values are stored as `RedisValue::String(Bytes)` — the raw HYLL wire
//! bytes. Redis `TYPE` reports "string" for HLL keys.

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::entry::Entry;
use crate::storage::hll::Hll;

use super::helpers::{err_wrong_args, extract_bytes, ok};

/// Redis-exact WRONGTYPE error for non-HLL string values.
const WRONGTYPE_HLL: &[u8] = b"WRONGTYPE Key is not a valid HyperLogLog string value.";

/// Load an existing HLL from the database (mutable access).
fn load_hll(db: &mut Database, key: &[u8]) -> Result<Option<Hll>, Frame> {
    match db.get(key) {
        Some(entry) => {
            let raw = match entry.value.as_bytes() {
                Some(b) => b,
                None => return Err(Frame::Error(Bytes::from_static(WRONGTYPE_HLL))),
            };
            if !Hll::is_hll(raw) {
                return Err(Frame::Error(Bytes::from_static(WRONGTYPE_HLL)));
            }
            let owned = match entry.value.as_bytes_owned() {
                Some(b) => b,
                None => return Err(Frame::Error(Bytes::from_static(WRONGTYPE_HLL))),
            };
            match Hll::from_bytes(owned) {
                Ok(hll) => Ok(Some(hll)),
                Err(_) => Err(Frame::Error(Bytes::from_static(WRONGTYPE_HLL))),
            }
        }
        None => Ok(None),
    }
}

/// Load an existing HLL from the database (read-only access).
fn load_hll_readonly(db: &Database, key: &[u8], now_ms: u64) -> Result<Option<Hll>, Frame> {
    match db.get_if_alive(key, now_ms) {
        Some(entry) => {
            let raw = match entry.value.as_bytes() {
                Some(b) => b,
                None => return Err(Frame::Error(Bytes::from_static(WRONGTYPE_HLL))),
            };
            if !Hll::is_hll(raw) {
                return Err(Frame::Error(Bytes::from_static(WRONGTYPE_HLL)));
            }
            let owned = match entry.value.as_bytes_owned() {
                Some(b) => b,
                None => return Err(Frame::Error(Bytes::from_static(WRONGTYPE_HLL))),
            };
            match Hll::from_bytes(owned) {
                Ok(hll) => Ok(Some(hll)),
                Err(_) => Err(Frame::Error(Bytes::from_static(WRONGTYPE_HLL))),
            }
        }
        None => Ok(None),
    }
}

/// Store HLL back into the database as RedisValue::String.
fn store_hll(db: &mut Database, key: Bytes, hll: Hll) {
    let mut entry = Entry::new_string(hll.into_bytes());
    entry.set_last_access(db.now());
    entry.set_access_counter(5);
    db.set(key, entry);
}

/// PFADD key [element [element ...]]
///
/// Adds elements to the HLL. Returns 1 if any register changed (or key
/// was created), 0 otherwise.
pub fn pfadd(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("PFADD");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("PFADD"),
    };
    let key_owned = key.clone();

    let existing = match load_hll(db, key) {
        Ok(v) => v,
        Err(e) => return e,
    };

    let mut created = false;
    let mut hll = match existing {
        Some(h) => h,
        None => {
            created = true;
            Hll::new_sparse()
        }
    };

    let mut changed = created;
    for arg in &args[1..] {
        if let Some(elem) = extract_bytes(arg) {
            if hll.add(elem) {
                changed = true;
            }
        }
    }

    if changed {
        store_hll(db, key_owned, hll);
        Frame::Integer(1)
    } else {
        Frame::Integer(0)
    }
}

/// PFCOUNT key [key ...] — read-only dispatch path.
///
/// Single key: return cardinality estimate.
/// Multiple keys: merge into temp HLL and return merged cardinality.
/// Does NOT mutate any source key.
pub fn pfcount_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.is_empty() {
        return err_wrong_args("PFCOUNT");
    }

    if args.len() == 1 {
        let key = match extract_bytes(&args[0]) {
            Some(k) => k,
            None => return err_wrong_args("PFCOUNT"),
        };
        match load_hll_readonly(db, key, now_ms) {
            Ok(Some(hll)) => {
                let count = hll.count();
                Frame::Integer(count as i64)
            }
            Ok(None) => Frame::Integer(0),
            Err(e) => e,
        }
    } else {
        let mut merged = Hll::new_sparse();
        for arg in args {
            let key = match extract_bytes(arg) {
                Some(k) => k,
                None => return err_wrong_args("PFCOUNT"),
            };
            match load_hll_readonly(db, key, now_ms) {
                Ok(Some(hll)) => {
                    merged.merge_from(&hll);
                }
                Ok(None) => {}
                Err(e) => return e,
            }
        }
        let count = merged.count();
        Frame::Integer(count as i64)
    }
}

/// PFCOUNT key [key ...] — write dispatch path (for mutable access).
pub fn pfcount(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("PFCOUNT");
    }

    if args.len() == 1 {
        let key = match extract_bytes(&args[0]) {
            Some(k) => k,
            None => return err_wrong_args("PFCOUNT"),
        };
        match load_hll(db, key) {
            Ok(Some(hll)) => {
                let count = hll.count();
                Frame::Integer(count as i64)
            }
            Ok(None) => Frame::Integer(0),
            Err(e) => e,
        }
    } else {
        let mut merged = Hll::new_sparse();
        for arg in args {
            let key = match extract_bytes(arg) {
                Some(k) => k,
                None => return err_wrong_args("PFCOUNT"),
            };
            match load_hll(db, key) {
                Ok(Some(hll)) => {
                    merged.merge_from(&hll);
                }
                Ok(None) => {}
                Err(e) => return e,
            }
        }
        let count = merged.count();
        Frame::Integer(count as i64)
    }
}

/// PFMERGE destkey sourcekey [sourcekey ...]
pub fn pfmerge(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("PFMERGE");
    }

    let dest_key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("PFMERGE"),
    };
    let dest_key_owned = dest_key.clone();

    let mut dest_hll = match load_hll(db, dest_key) {
        Ok(Some(h)) => h,
        Ok(None) => Hll::new_sparse(),
        Err(e) => return e,
    };

    for arg in &args[1..] {
        let src_key = match extract_bytes(arg) {
            Some(k) => k,
            None => return err_wrong_args("PFMERGE"),
        };
        match load_hll(db, src_key) {
            Ok(Some(hll)) => {
                dest_hll.merge_from(&hll);
            }
            Ok(None) => {}
            Err(e) => return e,
        }
    }

    store_hll(db, dest_key_owned, dest_hll);
    ok()
}
