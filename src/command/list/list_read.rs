use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;

use crate::command::helpers::{err_wrong_args, extract_bytes};
use super::{parse_i64, resolve_index};

// ---------------------------------------------------------------------------
// LLEN key
// ---------------------------------------------------------------------------

/// LLEN key
pub fn llen(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("LLEN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LLEN"),
    };
    match db.get_list(key) {
        Ok(Some(list)) => Frame::Integer(list.len() as i64),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

// ---------------------------------------------------------------------------
// LINDEX key index
// ---------------------------------------------------------------------------

/// LINDEX key index
pub fn lindex(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("LINDEX");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LINDEX"),
    };
    let index = match parse_i64(&args[1]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };

    let list = match db.get_list(key) {
        Ok(Some(l)) => l,
        Ok(None) => return Frame::Null,
        Err(e) => return e,
    };

    match resolve_index(index, list.len()) {
        Some(i) => Frame::BulkString(list[i].clone()),
        None => Frame::Null,
    }
}

// ---------------------------------------------------------------------------
// LRANGE key start stop
// ---------------------------------------------------------------------------

/// LRANGE key start stop
pub fn lrange(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("LRANGE");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LRANGE"),
    };
    let start = match parse_i64(&args[1]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    let stop = match parse_i64(&args[2]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };

    let list = match db.get_list(key) {
        Ok(Some(l)) => l,
        Ok(None) => return Frame::Array(framevec![]),
        Err(e) => return e,
    };

    let len = list.len() as i64;

    // Resolve negative indices
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };

    // Clamp
    if s < 0 {
        s = 0;
    }
    if e >= len {
        e = len - 1;
    }
    if s > e || s >= len {
        return Frame::Array(framevec![]);
    }

    let items: Vec<Frame> = (s as usize..=e as usize)
        .map(|i| Frame::BulkString(list[i].clone()))
        .collect();
    Frame::Array(items.into())
}

// ---------------------------------------------------------------------------
// LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
// ---------------------------------------------------------------------------

/// LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
pub fn lpos(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("LPOS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LPOS"),
    };
    let element = match extract_bytes(&args[1]) {
        Some(v) => v.clone(),
        None => return err_wrong_args("LPOS"),
    };

    // Parse optional arguments
    let mut rank: i64 = 1;
    let mut count: Option<usize> = None;
    let mut maxlen: usize = 0; // 0 means no limit

    let mut i = 2;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) {
            Some(o) => o.as_ref(),
            None => return err_wrong_args("LPOS"),
        };
        i += 1;
        if i >= args.len() {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
        let val = match parse_i64(&args[i]) {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ));
            }
        };
        i += 1;
        if opt.eq_ignore_ascii_case(b"RANK") {
            if val == 0 {
                return Frame::Error(Bytes::from_static(
                    b"ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative values meaning from the end of the list",
                ));
            }
            rank = val;
        } else if opt.eq_ignore_ascii_case(b"COUNT") {
            if val < 0 {
                return Frame::Error(Bytes::from_static(b"ERR COUNT can't be negative"));
            }
            count = Some(val as usize);
        } else if opt.eq_ignore_ascii_case(b"MAXLEN") {
            if val < 0 {
                return Frame::Error(Bytes::from_static(b"ERR MAXLEN can't be negative"));
            }
            maxlen = val as usize;
        } else {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
    }

    let list = match db.get_list(key) {
        Ok(Some(l)) => l,
        Ok(None) => {
            return if count.is_some() {
                Frame::Array(framevec![])
            } else {
                Frame::Null
            };
        }
        Err(e) => return e,
    };

    let len = list.len();
    let max_count = match count {
        Some(0) => usize::MAX, // COUNT 0 means all
        Some(c) => c,
        None => 1, // Without COUNT, return first match only
    };

    let mut matches: Vec<i64> = Vec::new();
    let mut match_count = 0usize;

    if rank > 0 {
        // Forward scan
        let mut skip = rank as usize - 1;
        let scan_limit = if maxlen > 0 { maxlen.min(len) } else { len };
        for idx in 0..scan_limit {
            if list[idx] == element {
                if skip > 0 {
                    skip -= 1;
                } else {
                    matches.push(idx as i64);
                    match_count += 1;
                    if match_count >= max_count {
                        break;
                    }
                }
            }
        }
    } else {
        // Reverse scan
        let mut skip = (-rank) as usize - 1;
        let scan_limit = if maxlen > 0 { maxlen.min(len) } else { len };
        let mut scanned = 0;
        for idx in (0..len).rev() {
            if scanned >= scan_limit {
                break;
            }
            scanned += 1;
            if list[idx] == element {
                if skip > 0 {
                    skip -= 1;
                } else {
                    matches.push(idx as i64);
                    match_count += 1;
                    if match_count >= max_count {
                        break;
                    }
                }
            }
        }
    }

    if count.is_some() {
        // Return array of positions
        Frame::Array(matches.into_iter().map(Frame::Integer).collect())
    } else {
        // Return single position or Null
        matches
            .first()
            .map(|&pos| Frame::Integer(pos))
            .unwrap_or(Frame::Null)
    }
}

// ---------------------------------------------------------------------------
// Read-only variants for RwLock read path
// ---------------------------------------------------------------------------

/// LLEN (read-only).
pub fn llen_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("LLEN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LLEN"),
    };
    match db.get_list_ref_if_alive(key, now_ms) {
        Ok(Some(lref)) => Frame::Integer(lref.len() as i64),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// LRANGE (read-only).
pub fn lrange_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("LRANGE");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LRANGE"),
    };
    let start = match parse_i64(&args[1]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    let stop = match parse_i64(&args[2]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    let lref = match db.get_list_ref_if_alive(key, now_ms) {
        Ok(Some(l)) => l,
        Ok(None) => return Frame::Array(framevec![]),
        Err(e) => return e,
    };
    let len = lref.len() as i64;
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };
    if s < 0 {
        s = 0;
    }
    if e >= len {
        e = len - 1;
    }
    if s > e || s >= len {
        return Frame::Array(framevec![]);
    }
    let items: Vec<Frame> = lref
        .range(s as usize, e as usize)
        .into_iter()
        .map(Frame::BulkString)
        .collect();
    Frame::Array(items.into())
}

/// LINDEX (read-only).
pub fn lindex_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("LINDEX");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LINDEX"),
    };
    let index = match parse_i64(&args[1]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    let lref = match db.get_list_ref_if_alive(key, now_ms) {
        Ok(Some(l)) => l,
        Ok(None) => return Frame::Null,
        Err(e) => return e,
    };
    match resolve_index(index, lref.len()) {
        Some(i) => match lref.get(i) {
            Some(v) => Frame::BulkString(v),
            None => Frame::Null,
        },
        None => Frame::Null,
    }
}

/// LPOS (read-only).
pub fn lpos_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("LPOS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LPOS"),
    };
    let element = match extract_bytes(&args[1]) {
        Some(v) => v.clone(),
        None => return err_wrong_args("LPOS"),
    };
    let mut rank: i64 = 1;
    let mut count: Option<usize> = None;
    let mut maxlen: usize = 0;
    let mut i = 2;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) {
            Some(o) => o.as_ref(),
            None => return err_wrong_args("LPOS"),
        };
        i += 1;
        if i >= args.len() {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
        let val = match parse_i64(&args[i]) {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ));
            }
        };
        i += 1;
        if opt.eq_ignore_ascii_case(b"RANK") {
            if val == 0 {
                return Frame::Error(Bytes::from_static(
                    b"ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative values meaning from the end of the list",
                ));
            }
            rank = val;
        } else if opt.eq_ignore_ascii_case(b"COUNT") {
            if val < 0 {
                return Frame::Error(Bytes::from_static(b"ERR COUNT can't be negative"));
            }
            count = Some(val as usize);
        } else if opt.eq_ignore_ascii_case(b"MAXLEN") {
            if val < 0 {
                return Frame::Error(Bytes::from_static(b"ERR MAXLEN can't be negative"));
            }
            maxlen = val as usize;
        } else {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
    }
    let lref = match db.get_list_ref_if_alive(key, now_ms) {
        Ok(Some(l)) => l,
        Ok(None) => {
            return if count.is_some() {
                Frame::Array(framevec![])
            } else {
                Frame::Null
            };
        }
        Err(e) => return e,
    };
    let all_elements = lref.iter_bytes();
    let len = all_elements.len();
    let max_count = match count {
        Some(0) => usize::MAX,
        Some(c) => c,
        None => 1,
    };
    let mut matches: Vec<i64> = Vec::new();
    let mut match_count = 0usize;
    if rank > 0 {
        let mut skip = rank as usize - 1;
        let scan_limit = if maxlen > 0 { maxlen.min(len) } else { len };
        for idx in 0..scan_limit {
            if all_elements[idx] == element {
                if skip > 0 {
                    skip -= 1;
                } else {
                    matches.push(idx as i64);
                    match_count += 1;
                    if match_count >= max_count {
                        break;
                    }
                }
            }
        }
    } else {
        let mut skip = (-rank) as usize - 1;
        let scan_limit = if maxlen > 0 { maxlen.min(len) } else { len };
        let mut scanned = 0;
        for idx in (0..len).rev() {
            if scanned >= scan_limit {
                break;
            }
            scanned += 1;
            if all_elements[idx] == element {
                if skip > 0 {
                    skip -= 1;
                } else {
                    matches.push(idx as i64);
                    match_count += 1;
                    if match_count >= max_count {
                        break;
                    }
                }
            }
        }
    }
    if count.is_some() {
        Frame::Array(matches.into_iter().map(Frame::Integer).collect())
    } else {
        matches
            .first()
            .copied()
            .map(Frame::Integer)
            .unwrap_or(Frame::Null)
    }
}
