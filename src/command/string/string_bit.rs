use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::entry::Entry;

use super::parse_i64;
use crate::command::helpers::{err_wrong_args, extract_bytes};

/// GETBIT key offset
///
/// Returns the bit value at offset in the string value stored at key.
pub fn getbit(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("GETBIT");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("GETBIT"),
    };
    let offset = match parse_i64(&args[1]) {
        Some(v) if v >= 0 => v as usize,
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR bit offset is not an integer or out of range",
            ));
        }
    };

    let byte_idx = offset / 8;
    let bit_idx = 7 - (offset % 8); // Redis uses big-endian bit ordering

    match db.get(key) {
        Some(entry) => match entry.value.as_bytes() {
            Some(data) => {
                if byte_idx >= data.len() {
                    Frame::Integer(0)
                } else {
                    Frame::Integer(((data[byte_idx] >> bit_idx) & 1) as i64)
                }
            }
            None => Frame::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
        },
        None => Frame::Integer(0),
    }
}

/// GETBIT readonly variant for dispatch_read.
pub fn getbit_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("GETBIT");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("GETBIT"),
    };
    let offset = match parse_i64(&args[1]) {
        Some(v) if v >= 0 => v as usize,
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR bit offset is not an integer or out of range",
            ));
        }
    };

    let byte_idx = offset / 8;
    let bit_idx = 7 - (offset % 8);

    match db.get_if_alive(key, now_ms) {
        Some(entry) => match entry.value.as_bytes() {
            Some(data) => {
                if byte_idx >= data.len() {
                    Frame::Integer(0)
                } else {
                    Frame::Integer(((data[byte_idx] >> bit_idx) & 1) as i64)
                }
            }
            None => Frame::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
        },
        None => Frame::Integer(0),
    }
}

/// SETBIT key offset value
///
/// Sets or clears the bit at offset in the string value stored at key.
/// Returns the original bit value at the offset.
pub fn setbit(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("SETBIT");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("SETBIT"),
    };
    let offset = match parse_i64(&args[1]) {
        Some(v) if v >= 0 && v < (512 * 1024 * 1024 * 8) => v as usize,
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR bit offset is not an integer or out of range",
            ));
        }
    };
    let bit_val = match parse_i64(&args[2]) {
        Some(0) => 0u8,
        Some(1) => 1u8,
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR bit is not an integer or out of range",
            ));
        }
    };

    let byte_idx = offset / 8;
    let bit_idx = 7 - (offset % 8);

    let base_ts = db.base_timestamp();
    let (existing_data, existing_expiry_ms) = match db.get(&key) {
        Some(entry) => {
            let expiry = entry.expires_at_ms(base_ts);
            match entry.value.as_bytes() {
                Some(v) => (Some(v.to_vec()), expiry),
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }
            }
        }
        None => (None, 0),
    };

    let mut buf = existing_data.unwrap_or_default();
    // Extend with zero bytes if needed
    if byte_idx >= buf.len() {
        buf.resize(byte_idx + 1, 0);
    }

    // Get original bit
    let original = (buf[byte_idx] >> bit_idx) & 1;

    // Set or clear bit
    if bit_val == 1 {
        buf[byte_idx] |= 1 << bit_idx;
    } else {
        buf[byte_idx] &= !(1 << bit_idx);
    }

    let new_val = Bytes::from(buf);
    let mut entry = if existing_expiry_ms > 0 {
        Entry::new_string_with_expiry(new_val, existing_expiry_ms, base_ts)
    } else {
        Entry::new_string(new_val)
    };
    entry.set_last_access(db.now());
    entry.set_access_counter(5);
    db.set(key, entry);

    Frame::Integer(original as i64)
}

/// BITCOUNT key [start end [BYTE|BIT]]
///
/// Count the number of set bits in a string.
pub fn bitcount(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("BITCOUNT");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("BITCOUNT"),
    };

    let data = match db.get(key) {
        Some(entry) => match entry.value.as_bytes() {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }
        },
        None => return Frame::Integer(0),
    };

    if data.is_empty() {
        return Frame::Integer(0);
    }

    // Parse optional range
    let (start, end, use_bit) = if args.len() >= 3 {
        let s = match parse_i64(&args[1]) {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ));
            }
        };
        let e = match parse_i64(&args[2]) {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ));
            }
        };
        let use_bit = if args.len() >= 4 {
            let mode = match extract_bytes(&args[3]) {
                Some(m) => m,
                None => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
            };
            if mode.eq_ignore_ascii_case(b"BIT") {
                true
            } else if mode.eq_ignore_ascii_case(b"BYTE") {
                false
            } else {
                return Frame::Error(Bytes::from_static(b"ERR syntax error"));
            }
        } else {
            false
        };
        (s, e, use_bit)
    } else {
        (0i64, -1i64, false)
    };

    if use_bit {
        // BIT mode: count bits in the bit range
        let total_bits = (data.len() * 8) as i64;
        let s = normalize_start(start, total_bits);
        let e = normalize_end(end, total_bits);
        if s > e {
            return Frame::Integer(0);
        }
        let count = count_bits_in_range(data, s as usize, e as usize);
        Frame::Integer(count as i64)
    } else {
        // BYTE mode (default): count bits in the byte range
        let len = data.len() as i64;
        let s = normalize_start(start, len);
        let e = normalize_end(end, len);
        if s > e {
            return Frame::Integer(0);
        }
        let slice = &data[s as usize..=e as usize];
        let count: u32 = slice.iter().map(|b| b.count_ones()).sum();
        Frame::Integer(count as i64)
    }
}

/// BITCOUNT readonly variant.
pub fn bitcount_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.is_empty() {
        return err_wrong_args("BITCOUNT");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("BITCOUNT"),
    };

    let data = match db.get_if_alive(key, now_ms) {
        Some(entry) => match entry.value.as_bytes() {
            Some(v) => v.to_vec(),
            None => {
                return Frame::Error(Bytes::from_static(
                    b"WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }
        },
        None => return Frame::Integer(0),
    };

    if data.is_empty() {
        return Frame::Integer(0);
    }

    let (start, end, use_bit) = if args.len() >= 3 {
        let s = match parse_i64(&args[1]) {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ));
            }
        };
        let e = match parse_i64(&args[2]) {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ));
            }
        };
        let use_bit = if args.len() >= 4 {
            let mode = match extract_bytes(&args[3]) {
                Some(m) => m,
                None => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
            };
            if mode.eq_ignore_ascii_case(b"BIT") {
                true
            } else if mode.eq_ignore_ascii_case(b"BYTE") {
                false
            } else {
                return Frame::Error(Bytes::from_static(b"ERR syntax error"));
            }
        } else {
            false
        };
        (s, e, use_bit)
    } else {
        (0i64, -1i64, false)
    };

    if use_bit {
        let total_bits = (data.len() * 8) as i64;
        let s = normalize_start(start, total_bits);
        let e = normalize_end(end, total_bits);
        if s > e {
            return Frame::Integer(0);
        }
        let count = count_bits_in_range(&data, s as usize, e as usize);
        Frame::Integer(count as i64)
    } else {
        let len = data.len() as i64;
        let s = normalize_start(start, len);
        let e = normalize_end(end, len);
        if s > e {
            return Frame::Integer(0);
        }
        let slice = &data[s as usize..=e as usize];
        let count: u32 = slice.iter().map(|b| b.count_ones()).sum();
        Frame::Integer(count as i64)
    }
}

/// BITOP operation destkey key [key ...]
///
/// Perform bitwise operations between strings.
pub fn bitop(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 3 {
        return err_wrong_args("BITOP");
    }
    let op = match extract_bytes(&args[0]) {
        Some(o) => o,
        None => return err_wrong_args("BITOP"),
    };
    let destkey = match extract_bytes(&args[1]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("BITOP"),
    };

    // Determine operation
    let is_not = op.eq_ignore_ascii_case(b"NOT");
    if is_not && args.len() != 3 {
        return Frame::Error(Bytes::from_static(
            b"ERR BITOP NOT requires one and only one key",
        ));
    }

    // Gather source values
    let mut sources: Vec<Vec<u8>> = Vec::with_capacity(args.len() - 2);
    let mut max_len = 0usize;
    for arg in &args[2..] {
        let key = match extract_bytes(arg) {
            Some(k) => k,
            None => return err_wrong_args("BITOP"),
        };
        let data = match db.get(key) {
            Some(entry) => match entry.value.as_bytes() {
                Some(v) => v.to_vec(),
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }
            },
            None => Vec::new(),
        };
        if data.len() > max_len {
            max_len = data.len();
        }
        sources.push(data);
    }

    if max_len == 0 {
        // All keys empty/missing — delete dest, return 0
        db.remove(&destkey);
        return Frame::Integer(0);
    }

    let mut result = vec![0u8; max_len];

    if is_not {
        let src = &sources[0];
        for (i, byte) in result.iter_mut().enumerate() {
            *byte = if i < src.len() { !src[i] } else { 0xFF };
        }
    } else if op.eq_ignore_ascii_case(b"AND") {
        // Start with all 1s
        result.iter_mut().for_each(|b| *b = 0xFF);
        for src in &sources {
            for (i, byte) in result.iter_mut().enumerate() {
                let v = if i < src.len() { src[i] } else { 0 };
                *byte &= v;
            }
        }
    } else if op.eq_ignore_ascii_case(b"OR") {
        for src in &sources {
            for (i, byte) in result.iter_mut().enumerate() {
                if i < src.len() {
                    *byte |= src[i];
                }
            }
        }
    } else if op.eq_ignore_ascii_case(b"XOR") {
        for src in &sources {
            for (i, byte) in result.iter_mut().enumerate() {
                if i < src.len() {
                    *byte ^= src[i];
                }
            }
        }
    } else {
        return Frame::Error(Bytes::from_static(
            b"ERR BITOP requires AND, OR, XOR, or NOT",
        ));
    }

    let result_len = result.len() as i64;
    let entry = Entry::new_string(Bytes::from(result));
    db.set(destkey, entry);

    Frame::Integer(result_len)
}

/// BITPOS key bit [start [end [BYTE|BIT]]]
///
/// Return the position of the first bit set to 0 or 1 in a string.
pub fn bitpos(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("BITPOS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("BITPOS"),
    };
    let target_bit = match parse_i64(&args[1]) {
        Some(0) => 0u8,
        Some(1) => 1u8,
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR bit is not an integer or out of range",
            ));
        }
    };

    let data = match db.get(key) {
        Some(entry) => match entry.value.as_bytes() {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }
        },
        None => {
            // Missing key: looking for 0 returns 0, looking for 1 returns -1
            return if target_bit == 0 {
                Frame::Integer(0)
            } else {
                Frame::Integer(-1)
            };
        }
    };

    if data.is_empty() {
        return if target_bit == 0 {
            Frame::Integer(0)
        } else {
            Frame::Integer(-1)
        };
    }

    // Parse optional range
    let has_start = args.len() >= 3;
    let has_end = args.len() >= 4;

    let use_bit = if args.len() >= 5 {
        let mode = match extract_bytes(&args[4]) {
            Some(m) => m,
            None => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
        };
        if mode.eq_ignore_ascii_case(b"BIT") {
            true
        } else if mode.eq_ignore_ascii_case(b"BYTE") {
            false
        } else {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
    } else {
        false
    };

    let start = if has_start {
        match parse_i64(&args[2]) {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ));
            }
        }
    } else {
        0
    };

    let end = if has_end {
        match parse_i64(&args[3]) {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ));
            }
        }
    } else {
        -1
    };

    if use_bit {
        let total_bits = (data.len() * 8) as i64;
        let s = normalize_start(start, total_bits) as usize;
        let e = normalize_end(end, total_bits) as usize;
        if s > e {
            return Frame::Integer(-1);
        }
        for bit_pos in s..=e {
            let byte_idx = bit_pos / 8;
            let bit_idx = 7 - (bit_pos % 8);
            if byte_idx < data.len() {
                let bit = (data[byte_idx] >> bit_idx) & 1;
                if bit == target_bit {
                    return Frame::Integer(bit_pos as i64);
                }
            }
        }
        Frame::Integer(-1)
    } else {
        let len = data.len() as i64;
        let s = normalize_start(start, len) as usize;
        let e = normalize_end(end, len) as usize;
        if s > e {
            return Frame::Integer(-1);
        }
        let slice = &data[s..=e];
        for (byte_offset, &byte) in slice.iter().enumerate() {
            for bit in 0..8u8 {
                let bit_idx = 7 - bit;
                let val = (byte >> bit_idx) & 1;
                if val == target_bit {
                    return Frame::Integer(((s + byte_offset) * 8 + bit as usize) as i64);
                }
            }
        }
        // If searching for 0 without explicit end, Redis treats the string as
        // having an implicit 0x00 byte beyond the last byte
        if target_bit == 0 && !has_end {
            return Frame::Integer(((e + 1) * 8) as i64);
        }
        Frame::Integer(-1)
    }
}

/// BITPOS readonly variant.
pub fn bitpos_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("BITPOS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("BITPOS"),
    };
    let target_bit = match parse_i64(&args[1]) {
        Some(0) => 0u8,
        Some(1) => 1u8,
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR bit is not an integer or out of range",
            ));
        }
    };

    let data_owned;
    let data: &[u8] = match db.get_if_alive(key, now_ms) {
        Some(entry) => match entry.value.as_bytes() {
            Some(v) => {
                data_owned = v.to_vec();
                &data_owned
            }
            None => {
                return Frame::Error(Bytes::from_static(
                    b"WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }
        },
        None => {
            return if target_bit == 0 {
                Frame::Integer(0)
            } else {
                Frame::Integer(-1)
            };
        }
    };

    if data.is_empty() {
        return if target_bit == 0 {
            Frame::Integer(0)
        } else {
            Frame::Integer(-1)
        };
    }

    let has_start = args.len() >= 3;
    let has_end = args.len() >= 4;

    let use_bit = if args.len() >= 5 {
        let mode = match extract_bytes(&args[4]) {
            Some(m) => m,
            None => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
        };
        if mode.eq_ignore_ascii_case(b"BIT") {
            true
        } else if mode.eq_ignore_ascii_case(b"BYTE") {
            false
        } else {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
    } else {
        false
    };

    let start = if has_start {
        match parse_i64(&args[2]) {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ));
            }
        }
    } else {
        0
    };

    let end = if has_end {
        match parse_i64(&args[3]) {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ));
            }
        }
    } else {
        -1
    };

    if use_bit {
        let total_bits = (data.len() * 8) as i64;
        let s = normalize_start(start, total_bits) as usize;
        let e = normalize_end(end, total_bits) as usize;
        if s > e {
            return Frame::Integer(-1);
        }
        for bit_pos in s..=e {
            let byte_idx = bit_pos / 8;
            let bit_idx = 7 - (bit_pos % 8);
            if byte_idx < data.len() {
                let bit = (data[byte_idx] >> bit_idx) & 1;
                if bit == target_bit {
                    return Frame::Integer(bit_pos as i64);
                }
            }
        }
        Frame::Integer(-1)
    } else {
        let len = data.len() as i64;
        let s = normalize_start(start, len) as usize;
        let e = normalize_end(end, len) as usize;
        if s > e {
            return Frame::Integer(-1);
        }
        let slice = &data[s..=e];
        for (byte_offset, &byte) in slice.iter().enumerate() {
            for bit in 0..8u8 {
                let bit_idx = 7 - bit;
                let val = (byte >> bit_idx) & 1;
                if val == target_bit {
                    return Frame::Integer(((s + byte_offset) * 8 + bit as usize) as i64);
                }
            }
        }
        if target_bit == 0 && !has_end {
            return Frame::Integer(((e + 1) * 8) as i64);
        }
        Frame::Integer(-1)
    }
}

/// Normalize a start index: negative wraps from end, positive stays unclamped
/// so callers detect empty ranges via `start > end`.
fn normalize_start(idx: i64, len: i64) -> i64 {
    if len == 0 {
        return 0;
    }
    let normalized = if idx < 0 { len + idx } else { idx };
    normalized.max(0)
}

/// Normalize an end index: negative wraps from end, clamped to `len - 1`
/// to prevent out-of-bounds slicing.
fn normalize_end(idx: i64, len: i64) -> i64 {
    if len == 0 {
        return 0;
    }
    let normalized = if idx < 0 { len + idx } else { idx };
    normalized.clamp(0, len - 1)
}

/// Count set bits in a specific bit range within a byte slice.
fn count_bits_in_range(data: &[u8], start_bit: usize, end_bit: usize) -> u32 {
    let mut count = 0u32;
    for bit_pos in start_bit..=end_bit {
        let byte_idx = bit_pos / 8;
        let bit_idx = 7 - (bit_pos % 8);
        if byte_idx < data.len() && (data[byte_idx] >> bit_idx) & 1 == 1 {
            count += 1;
        }
    }
    count
}

/// BITFIELD key [GET encoding offset] [SET encoding offset value]
///   [INCRBY encoding offset increment] [OVERFLOW WRAP|SAT|FAIL]
///
/// Treat a string as an array of packed integers of configurable width.
pub fn bitfield(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("BITFIELD");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("BITFIELD"),
    };

    let base_ts = db.base_timestamp();
    let (existing_data, existing_expiry_ms) = match db.get(&key) {
        Some(entry) => {
            let expiry = entry.expires_at_ms(base_ts);
            match entry.value.as_bytes() {
                Some(v) => (v.to_vec(), expiry),
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }
            }
        }
        None => (Vec::new(), 0),
    };

    let mut buf = existing_data;
    let mut results = Vec::new();
    let mut overflow = Overflow::Wrap;
    let mut modified = false;
    let mut i = 1;

    while i < args.len() {
        let subcmd = match extract_bytes(&args[i]) {
            Some(s) => s,
            None => {
                i += 1;
                continue;
            }
        };

        if subcmd.eq_ignore_ascii_case(b"OVERFLOW") {
            i += 1;
            let mode = match args.get(i).and_then(|f| extract_bytes(f)) {
                Some(m) => m,
                None => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
            };
            if mode.eq_ignore_ascii_case(b"WRAP") {
                overflow = Overflow::Wrap;
            } else if mode.eq_ignore_ascii_case(b"SAT") {
                overflow = Overflow::Sat;
            } else if mode.eq_ignore_ascii_case(b"FAIL") {
                overflow = Overflow::Fail;
            } else {
                return Frame::Error(Bytes::from_static(b"ERR syntax error"));
            }
            i += 1;
            continue;
        }

        // Parse encoding and offset
        let enc = match args.get(i + 1).and_then(|f| extract_bytes(f)) {
            Some(e) => e,
            None => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
        };
        let offset_arg = match args.get(i + 2).and_then(|f| extract_bytes(f)) {
            Some(o) => o,
            None => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
        };

        let (signed, bits) = match parse_encoding(enc) {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR Invalid bitfield type. Use something like i8 u8 i16 u16 ...",
                ));
            }
        };
        let bit_offset = match parse_bit_offset(offset_arg, bits) {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR bit offset is not an integer or out of range",
                ));
            }
        };

        if subcmd.eq_ignore_ascii_case(b"GET") {
            let val = bf_get(&buf, bit_offset, bits, signed);
            results.push(Frame::Integer(val));
            i += 3;
        } else if subcmd.eq_ignore_ascii_case(b"SET") {
            let value = match args.get(i + 3).and_then(|f| parse_i64(f)) {
                Some(v) => v,
                None => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
            };
            let old = bf_get(&buf, bit_offset, bits, signed);
            bf_set(&mut buf, bit_offset, bits, value);
            results.push(Frame::Integer(old));
            modified = true;
            i += 4;
        } else if subcmd.eq_ignore_ascii_case(b"INCRBY") {
            let increment = match args.get(i + 3).and_then(|f| parse_i64(f)) {
                Some(v) => v,
                None => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
            };
            let old = bf_get(&buf, bit_offset, bits, signed);
            let (new_val, overflowed) = bf_incr(old, increment, bits, signed);
            if overflowed && matches!(overflow, Overflow::Fail) {
                results.push(Frame::Null);
            } else {
                let clamped = if overflowed && matches!(overflow, Overflow::Sat) {
                    bf_saturate(old, increment, bits, signed)
                } else {
                    new_val
                };
                bf_set(&mut buf, bit_offset, bits, clamped);
                results.push(Frame::Integer(clamped));
                modified = true;
            }
            i += 4;
        } else {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
    }

    if modified {
        let new_val = Bytes::from(buf);
        let mut entry = if existing_expiry_ms > 0 {
            Entry::new_string_with_expiry(new_val, existing_expiry_ms, base_ts)
        } else {
            Entry::new_string(new_val)
        };
        entry.set_last_access(db.now());
        entry.set_access_counter(5);
        db.set(key, entry);
    }

    Frame::Array(results.into())
}

#[derive(Clone, Copy)]
enum Overflow {
    Wrap,
    Sat,
    Fail,
}

/// Parse encoding like "u8", "i16", "u32" etc. Returns (signed, bits).
fn parse_encoding(enc: &[u8]) -> Option<(bool, u32)> {
    if enc.is_empty() {
        return None;
    }
    let signed = match enc[0] | 0x20 {
        b'i' => true,
        b'u' => false,
        _ => return None,
    };
    let bits: u32 = std::str::from_utf8(&enc[1..]).ok()?.parse().ok()?;
    if bits == 0 || bits > 64 || (!signed && bits > 63) {
        return None;
    }
    Some((signed, bits))
}

/// Parse bit offset: plain number or `#N` (type-multiplied).
fn parse_bit_offset(arg: &[u8], type_bits: u32) -> Option<usize> {
    if arg.first() == Some(&b'#') {
        let n: usize = std::str::from_utf8(&arg[1..]).ok()?.parse().ok()?;
        Some(n * type_bits as usize)
    } else {
        let n: usize = std::str::from_utf8(arg).ok()?.parse().ok()?;
        Some(n)
    }
}

/// Read `bits` bits starting at `bit_offset` from `data`, interpreting as signed/unsigned.
fn bf_get(data: &[u8], bit_offset: usize, bits: u32, signed: bool) -> i64 {
    let mut val: u64 = 0;
    for b in 0..bits as usize {
        let pos = bit_offset + b;
        let byte_idx = pos / 8;
        let bit_idx = 7 - (pos % 8);
        if byte_idx < data.len() && (data[byte_idx] >> bit_idx) & 1 == 1 {
            val |= 1 << (bits as usize - 1 - b);
        }
    }
    if signed && bits < 64 && val & (1 << (bits - 1)) != 0 {
        // Sign extend
        val |= !0u64 << bits;
    }
    val as i64
}

/// Write `bits` bits starting at `bit_offset` into `data`.
fn bf_set(data: &mut Vec<u8>, bit_offset: usize, bits: u32, value: i64) {
    let val = value as u64;
    let needed_bytes = (bit_offset + bits as usize + 7) / 8;
    if data.len() < needed_bytes {
        data.resize(needed_bytes, 0);
    }
    for b in 0..bits as usize {
        let pos = bit_offset + b;
        let byte_idx = pos / 8;
        let bit_idx = 7 - (pos % 8);
        if val & (1 << (bits as usize - 1 - b)) != 0 {
            data[byte_idx] |= 1 << bit_idx;
        } else {
            data[byte_idx] &= !(1 << bit_idx);
        }
    }
}

/// Increment with overflow detection. Returns (result, overflowed).
fn bf_incr(old: i64, incr: i64, bits: u32, signed: bool) -> (i64, bool) {
    if signed {
        let min = -(1i64 << (bits - 1));
        let max = (1i64 << (bits - 1)) - 1;
        match old.checked_add(incr) {
            Some(v) if v >= min && v <= max => (v, false),
            Some(v) => {
                // Wrap
                let range = 1i64 << bits;
                let wrapped = ((v - min) % range + range) % range + min;
                (wrapped, true)
            }
            None => {
                // Overflow in i64 itself — definitely overflowed
                let wrapped = old.wrapping_add(incr);
                (wrapped, true)
            }
        }
    } else {
        let max = if bits >= 64 {
            u64::MAX
        } else {
            (1u64 << bits) - 1
        };
        let old_u = old as u64 & max;
        let incr_u = incr as u64;
        let sum = old_u.wrapping_add(incr_u);
        let masked = sum & max;
        (masked as i64, masked != sum || (incr < 0 && sum > old_u))
    }
}

/// Saturate to min/max of the type.
fn bf_saturate(_old: i64, incr: i64, bits: u32, signed: bool) -> i64 {
    if signed {
        let min = -(1i64 << (bits - 1));
        let max = (1i64 << (bits - 1)) - 1;
        if incr > 0 { max } else { min }
    } else {
        let max = if bits >= 64 {
            i64::MAX
        } else {
            ((1u64 << bits) - 1) as i64
        };
        if incr > 0 { max } else { 0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Database;

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn make_db() -> Database {
        Database::new()
    }

    // --- GETBIT tests ---

    #[test]
    fn test_getbit_missing_key() {
        let mut db = make_db();
        let result = getbit(&mut db, &[bs(b"key"), bs(b"0")]);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_getbit_out_of_range() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"\xff"));
        let result = getbit(&mut db, &[bs(b"key"), bs(b"100")]);
        assert_eq!(result, Frame::Integer(0));
    }

    // --- SETBIT tests ---

    #[test]
    fn test_setbit_new_key() {
        let mut db = make_db();
        let result = setbit(&mut db, &[bs(b"key"), bs(b"7"), bs(b"1")]);
        assert_eq!(result, Frame::Integer(0)); // original was 0
        let result = getbit(&mut db, &[bs(b"key"), bs(b"7")]);
        assert_eq!(result, Frame::Integer(1));
    }

    #[test]
    fn test_setbit_clear() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"\xff"));
        let result = setbit(&mut db, &[bs(b"key"), bs(b"0"), bs(b"0")]);
        assert_eq!(result, Frame::Integer(1)); // original was 1
        let result = getbit(&mut db, &[bs(b"key"), bs(b"0")]);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_setbit_extends_string() {
        let mut db = make_db();
        setbit(&mut db, &[bs(b"key"), bs(b"23"), bs(b"1")]);
        // 23 / 8 = byte 2, so 3 bytes total
        let entry = db.get(b"key").unwrap();
        assert_eq!(entry.value.as_bytes().unwrap().len(), 3);
    }

    // --- BITCOUNT tests ---

    #[test]
    fn test_bitcount_full_string() {
        let mut db = make_db();
        // "foobar" in bytes
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"foobar"));
        let result = bitcount(&mut db, &[bs(b"key")]);
        assert_eq!(result, Frame::Integer(26)); // known count for "foobar"
    }

    #[test]
    fn test_bitcount_range() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"foobar"));
        let result = bitcount(&mut db, &[bs(b"key"), bs(b"0"), bs(b"0")]);
        // 'f' = 0x66 = 01100110 → 4 bits
        assert_eq!(result, Frame::Integer(4));
    }

    #[test]
    fn test_bitcount_negative_range() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"foobar"));
        let result = bitcount(&mut db, &[bs(b"key"), bs(b"-1"), bs(b"-1")]);
        // 'r' = 0x72 = 01110010 → 4 bits
        assert_eq!(result, Frame::Integer(4));
    }

    #[test]
    fn test_bitcount_missing_key() {
        let mut db = make_db();
        let result = bitcount(&mut db, &[bs(b"key")]);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_bitcount_bit_mode() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"\xff\x00"));
        // BIT mode: bits 0-7 are all 1 = 8 bits
        let result = bitcount(&mut db, &[bs(b"key"), bs(b"0"), bs(b"7"), bs(b"BIT")]);
        assert_eq!(result, Frame::Integer(8));
    }

    // --- BITOP tests ---

    #[test]
    fn test_bitop_and() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"a"), Bytes::from_static(b"\xff\x0f"));
        db.set_string(Bytes::from_static(b"b"), Bytes::from_static(b"\x0f\xff"));
        let result = bitop(&mut db, &[bs(b"AND"), bs(b"dest"), bs(b"a"), bs(b"b")]);
        assert_eq!(result, Frame::Integer(2));
        let data = db.get(b"dest").unwrap().value.as_bytes().unwrap().to_vec();
        assert_eq!(data, vec![0x0f, 0x0f]);
    }

    #[test]
    fn test_bitop_or() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"a"), Bytes::from_static(b"\xf0"));
        db.set_string(Bytes::from_static(b"b"), Bytes::from_static(b"\x0f"));
        let result = bitop(&mut db, &[bs(b"OR"), bs(b"dest"), bs(b"a"), bs(b"b")]);
        assert_eq!(result, Frame::Integer(1));
        let data = db.get(b"dest").unwrap().value.as_bytes().unwrap().to_vec();
        assert_eq!(data, vec![0xff]);
    }

    #[test]
    fn test_bitop_xor() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"a"), Bytes::from_static(b"\xff"));
        db.set_string(Bytes::from_static(b"b"), Bytes::from_static(b"\x0f"));
        let result = bitop(&mut db, &[bs(b"XOR"), bs(b"dest"), bs(b"a"), bs(b"b")]);
        assert_eq!(result, Frame::Integer(1));
        let data = db.get(b"dest").unwrap().value.as_bytes().unwrap().to_vec();
        assert_eq!(data, vec![0xf0]);
    }

    #[test]
    fn test_bitop_not() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"a"), Bytes::from_static(b"\x0f"));
        let result = bitop(&mut db, &[bs(b"NOT"), bs(b"dest"), bs(b"a")]);
        assert_eq!(result, Frame::Integer(1));
        let data = db.get(b"dest").unwrap().value.as_bytes().unwrap().to_vec();
        assert_eq!(data, vec![0xf0]);
    }

    #[test]
    fn test_bitop_not_requires_one_key() {
        let mut db = make_db();
        let result = bitop(&mut db, &[bs(b"NOT"), bs(b"dest"), bs(b"a"), bs(b"b")]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_bitop_unequal_lengths() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"a"), Bytes::from_static(b"\xff\xff"));
        db.set_string(Bytes::from_static(b"b"), Bytes::from_static(b"\x0f"));
        let result = bitop(&mut db, &[bs(b"AND"), bs(b"dest"), bs(b"a"), bs(b"b")]);
        assert_eq!(result, Frame::Integer(2));
        let data = db.get(b"dest").unwrap().value.as_bytes().unwrap().to_vec();
        // b is zero-padded → \x0f\x00, AND with \xff\xff → \x0f\x00
        assert_eq!(data, vec![0x0f, 0x00]);
    }

    // --- BITPOS tests ---

    #[test]
    fn test_bitpos_first_one() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"\x00\xff"));
        let result = bitpos(&mut db, &[bs(b"key"), bs(b"1")]);
        assert_eq!(result, Frame::Integer(8)); // first 1 bit at position 8
    }

    #[test]
    fn test_bitpos_first_zero() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"\xff\x00"));
        let result = bitpos(&mut db, &[bs(b"key"), bs(b"0")]);
        assert_eq!(result, Frame::Integer(8)); // first 0 bit at position 8
    }

    #[test]
    fn test_bitpos_no_one_found() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"\x00\x00"));
        let result = bitpos(&mut db, &[bs(b"key"), bs(b"1")]);
        assert_eq!(result, Frame::Integer(-1));
    }

    #[test]
    fn test_bitpos_missing_key_zero() {
        let mut db = make_db();
        let result = bitpos(&mut db, &[bs(b"key"), bs(b"0")]);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_bitpos_missing_key_one() {
        let mut db = make_db();
        let result = bitpos(&mut db, &[bs(b"key"), bs(b"1")]);
        assert_eq!(result, Frame::Integer(-1));
    }
}
