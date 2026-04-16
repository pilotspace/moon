//! FT.CONFIG SET/GET command handler — per-index configuration.

use bytes::Bytes;

use crate::protocol::Frame;
use crate::vector::store::VectorStore;

use super::extract_bulk;

/// FT.CONFIG SET index_name AUTOCOMPACT ON|OFF
/// FT.CONFIG GET index_name AUTOCOMPACT
///
/// Per-index configuration. Currently supports AUTOCOMPACT only.
/// args[0] = SET|GET, args[1] = index_name, args[2] = param_name, args[3] = value (SET only)
pub fn ft_config(
    store: &mut VectorStore,
    text_store: &mut crate::text::store::TextStore,
    args: &[Frame],
) -> Frame {
    if args.len() < 3 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.CONFIG' command",
        ));
    }
    let subcommand = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid subcommand")),
    };
    let index_name = match extract_bulk(&args[1]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid index name")),
    };
    let param_name = match extract_bulk(&args[2]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid parameter name")),
    };

    if subcommand.eq_ignore_ascii_case(b"SET") {
        if args.len() < 4 {
            return Frame::Error(Bytes::from_static(b"ERR SET requires a value"));
        }
        let value = match extract_bulk(&args[3]) {
            Some(b) => b,
            None => return Frame::Error(Bytes::from_static(b"ERR invalid value")),
        };
        ft_config_set(store, text_store, &index_name, &param_name, &value)
    } else if subcommand.eq_ignore_ascii_case(b"GET") {
        ft_config_get(store, text_store, &index_name, &param_name)
    } else {
        Frame::Error(Bytes::from_static(
            b"ERR FT.CONFIG subcommand must be SET or GET",
        ))
    }
}

fn ft_config_set(
    store: &mut VectorStore,
    text_store: &mut crate::text::store::TextStore,
    index_name: &[u8],
    param: &[u8],
    value: &[u8],
) -> Frame {
    // BM25 parameters route to TextStore
    if param.eq_ignore_ascii_case(b"BM25_K1") || param.eq_ignore_ascii_case(b"BM25_B") {
        let text_idx = match text_store.get_index_mut(index_name) {
            Some(i) => i,
            None => return Frame::Error(Bytes::from_static(b"Unknown Index name")),
        };
        let parsed: f32 = match std::str::from_utf8(value)
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(v) => v,
            None => return Frame::Error(Bytes::from_static(b"ERR invalid numeric value")),
        };
        if param.eq_ignore_ascii_case(b"BM25_K1") {
            if parsed < 0.0 {
                return Frame::Error(Bytes::from_static(b"ERR BM25_K1 must be >= 0.0"));
            }
            text_idx.bm25_config.k1 = parsed;
        } else {
            if !(0.0..=1.0).contains(&parsed) {
                return Frame::Error(Bytes::from_static(
                    b"ERR BM25_B must be between 0.0 and 1.0",
                ));
            }
            text_idx.bm25_config.b = parsed;
        }
        return Frame::SimpleString(Bytes::from_static(b"OK"));
    }
    let idx = match store.get_index_mut(index_name) {
        Some(i) => i,
        None => return Frame::Error(Bytes::from_static(b"Unknown Index name")),
    };
    if param.eq_ignore_ascii_case(b"AUTOCOMPACT") {
        if value.eq_ignore_ascii_case(b"ON") || value == b"1" || value.eq_ignore_ascii_case(b"TRUE")
        {
            idx.autocompact_enabled = true;
            Frame::SimpleString(Bytes::from_static(b"OK"))
        } else if value.eq_ignore_ascii_case(b"OFF")
            || value == b"0"
            || value.eq_ignore_ascii_case(b"FALSE")
        {
            idx.autocompact_enabled = false;
            Frame::SimpleString(Bytes::from_static(b"OK"))
        } else {
            Frame::Error(Bytes::from_static(
                b"ERR AUTOCOMPACT value must be ON or OFF",
            ))
        }
    } else {
        Frame::Error(Bytes::from_static(b"ERR unknown config parameter"))
    }
}

fn ft_config_get(
    store: &mut VectorStore,
    text_store: &mut crate::text::store::TextStore,
    index_name: &[u8],
    param: &[u8],
) -> Frame {
    // BM25 parameters route to TextStore
    if param.eq_ignore_ascii_case(b"BM25_K1") || param.eq_ignore_ascii_case(b"BM25_B") {
        let text_idx = match text_store.get_index(index_name) {
            Some(i) => i,
            None => return Frame::Error(Bytes::from_static(b"Unknown Index name")),
        };
        let val = if param.eq_ignore_ascii_case(b"BM25_K1") {
            text_idx.bm25_config.k1
        } else {
            text_idx.bm25_config.b
        };
        let mut buf = String::with_capacity(8);
        use std::fmt::Write;
        let _ = write!(buf, "{val}");
        return Frame::BulkString(Bytes::from(buf));
    }
    let idx = match store.get_index_mut(index_name) {
        Some(i) => i,
        None => return Frame::Error(Bytes::from_static(b"Unknown Index name")),
    };
    if param.eq_ignore_ascii_case(b"AUTOCOMPACT") {
        let val = if idx.autocompact_enabled { "ON" } else { "OFF" };
        Frame::BulkString(Bytes::from(val))
    } else {
        Frame::Error(Bytes::from_static(b"ERR unknown config parameter"))
    }
}
