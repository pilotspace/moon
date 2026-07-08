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
    db_index: u8,
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
        ft_config_set(
            store,
            text_store,
            &index_name,
            &param_name,
            &value,
            db_index,
        )
    } else if subcommand.eq_ignore_ascii_case(b"GET") {
        ft_config_get(store, text_store, &index_name, &param_name, db_index)
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
    db_index: u8,
) -> Frame {
    // BM25 parameters route to TextStore (db-scoped: WS5a)
    if param.eq_ignore_ascii_case(b"BM25_K1") || param.eq_ignore_ascii_case(b"BM25_B") {
        let text_idx = match text_store.get_index_mut_for_db(index_name, db_index) {
            Some(i) => i,
            None => return Frame::Error(Bytes::from_static(b"Unknown Index name")),
        };
        let parsed: f32 = match std::str::from_utf8(value).ok().and_then(|s| s.parse().ok()) {
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
    let idx = match store.get_index_mut_for_db(index_name, db_index) {
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
    } else if param.eq_ignore_ascii_case(b"COMPACTION_WEIGHT") {
        // W3-deep: per-index autovacuum priority multiplier.
        // Parse + validate before taking the mutable borrow so we can flush the
        // sidecar after the borrow is released.
        let parsed: f32 = match std::str::from_utf8(value)
            .ok()
            .and_then(|s| s.parse::<f32>().ok())
        {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR COMPACTION_WEIGHT must be a number",
                ));
            }
        };
        // Scoped block: set weight, ending borrow of `store` via `idx` before
        // we call `store.save_index_meta_sidecar()`.
        let set_result = { idx.try_set_compaction_weight(parsed) };
        match set_result {
            Ok(()) => {
                // Persist the new weight so it survives a server restart.
                store.save_index_meta_sidecar();
                Frame::SimpleString(Bytes::from_static(b"OK"))
            }
            Err(e) => Frame::Error(Bytes::from(format!("ERR {e}").into_bytes())),
        }
    } else if param.eq_ignore_ascii_case(b"MERGE_RECALL_TOLERANCE") {
        // VEC-4: recall gate for UNATTENDED (background/vacuum) GraphUnion
        // merges. Default 0.70 catches only catastrophic collapse; operators
        // running recall-sensitive workloads can tighten toward the manual
        // FT.COMPACT gate (0.90). Raising it can make auto-merge abort
        // repeatedly on small indexes (segments stay > threshold) — informed
        // trade-off, hence a knob rather than a new default.
        let parsed: f32 = match std::str::from_utf8(value)
            .ok()
            .and_then(|s| s.parse::<f32>().ok())
        {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR MERGE_RECALL_TOLERANCE must be a number",
                ));
            }
        };
        if !(0.0..=1.0).contains(&parsed) {
            return Frame::Error(Bytes::from_static(
                b"ERR MERGE_RECALL_TOLERANCE must be between 0.0 and 1.0",
            ));
        }
        idx.merge_recall_tolerance = parsed;
        Frame::SimpleString(Bytes::from_static(b"OK"))
    } else if param.eq_ignore_ascii_case(b"EF_RUNTIME") {
        // Runtime-tunable search beam width, same range as FT.CREATE's
        // EF_RUNTIME (10-4096). 0 restores the auto heuristic (max(k*20, 200)
        // with dimension boost). Query path reads meta.hnsw_ef_runtime per
        // search, so the change applies to the next FT.SEARCH immediately.
        let parsed: u32 = match std::str::from_utf8(value)
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
        {
            Some(v) if v == 0 || (10..=4096).contains(&v) => v,
            Some(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR EF_RUNTIME must be 10-4096 (or 0 for auto)",
                ));
            }
            None => {
                return Frame::Error(Bytes::from_static(b"ERR invalid EF_RUNTIME value"));
            }
        };
        idx.meta.hnsw_ef_runtime = parsed;
        // Persist so the value survives a server restart.
        store.save_index_meta_sidecar();
        Frame::SimpleString(Bytes::from_static(b"OK"))
    } else if param.eq_ignore_ascii_case(b"RERANK_MULT") {
        // HQ-1 depth: re-score the top `mult · k` beam candidates against the
        // f16 exact sidecar. Raising it recovers true neighbors the quantized
        // ADC ranking pushed below the 4·k default (the main lever for the
        // last few recall points below EXACT_BEAM); cost ~mult·k·dim f16
        // decodes per segment. Applies to the next FT.SEARCH immediately.
        let parsed: u32 = match std::str::from_utf8(value)
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
        {
            Some(v) if (1..=64).contains(&v) => v,
            Some(_) => {
                return Frame::Error(Bytes::from_static(b"ERR RERANK_MULT must be 1-64"));
            }
            None => {
                return Frame::Error(Bytes::from_static(b"ERR invalid RERANK_MULT value"));
            }
        };
        idx.meta.rerank_mult = parsed;
        // Persist so the value survives a server restart.
        store.save_index_meta_sidecar();
        Frame::SimpleString(Bytes::from_static(b"OK"))
    } else if param.eq_ignore_ascii_case(b"EXACT_BEAM") {
        // Navigate the HNSW beam with exact f16 sidecar distances instead of
        // quantized ADC: recall becomes graph-limited (~1.0 at ef 256) at a
        // QPS cost that grows with dimension. Segments without a sidecar
        // (pre-HQ-1 reloads) silently keep the ADC beam.
        let parsed = if value.eq_ignore_ascii_case(b"ON")
            || value == b"1"
            || value.eq_ignore_ascii_case(b"TRUE")
        {
            true
        } else if value.eq_ignore_ascii_case(b"OFF")
            || value == b"0"
            || value.eq_ignore_ascii_case(b"FALSE")
        {
            false
        } else {
            return Frame::Error(Bytes::from_static(
                b"ERR EXACT_BEAM value must be ON or OFF",
            ));
        };
        idx.meta.exact_beam = parsed;
        // Persist so the value survives a server restart.
        store.save_index_meta_sidecar();
        Frame::SimpleString(Bytes::from_static(b"OK"))
    } else {
        Frame::Error(Bytes::from_static(b"ERR unknown config parameter"))
    }
}

fn ft_config_get(
    store: &mut VectorStore,
    text_store: &mut crate::text::store::TextStore,
    index_name: &[u8],
    param: &[u8],
    db_index: u8,
) -> Frame {
    // BM25 parameters route to TextStore (db-scoped: WS5a)
    if param.eq_ignore_ascii_case(b"BM25_K1") || param.eq_ignore_ascii_case(b"BM25_B") {
        let text_idx = match text_store.get_index_for_db(index_name, db_index) {
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
    let idx = match store.get_index_mut_for_db(index_name, db_index) {
        Some(i) => i,
        None => return Frame::Error(Bytes::from_static(b"Unknown Index name")),
    };
    if param.eq_ignore_ascii_case(b"AUTOCOMPACT") {
        let val = if idx.autocompact_enabled { "ON" } else { "OFF" };
        Frame::BulkString(Bytes::from(val))
    } else if param.eq_ignore_ascii_case(b"COMPACTION_WEIGHT") {
        // W3-deep: return current weight as a decimal string.
        let mut buf = String::with_capacity(8);
        use std::fmt::Write as _;
        let _ = write!(buf, "{}", idx.compaction_weight());
        Frame::BulkString(Bytes::from(buf))
    } else if param.eq_ignore_ascii_case(b"MERGE_RECALL_TOLERANCE") {
        let mut buf = String::with_capacity(8);
        use std::fmt::Write as _;
        let _ = write!(buf, "{}", idx.merge_recall_tolerance);
        Frame::BulkString(Bytes::from(buf))
    } else if param.eq_ignore_ascii_case(b"EF_RUNTIME") {
        let mut buf = String::with_capacity(8);
        use std::fmt::Write as _;
        let _ = write!(buf, "{}", idx.meta.hnsw_ef_runtime);
        Frame::BulkString(Bytes::from(buf))
    } else if param.eq_ignore_ascii_case(b"RERANK_MULT") {
        let mut buf = String::with_capacity(8);
        use std::fmt::Write as _;
        let _ = write!(buf, "{}", idx.meta.rerank_mult);
        Frame::BulkString(Bytes::from(buf))
    } else if param.eq_ignore_ascii_case(b"EXACT_BEAM") {
        let val = if idx.meta.exact_beam { "ON" } else { "OFF" };
        Frame::BulkString(Bytes::from(val))
    } else {
        Frame::Error(Bytes::from_static(b"ERR unknown config parameter"))
    }
}
