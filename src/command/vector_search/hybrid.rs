//! Hybrid FT.SEARCH (BM25 + dense + sparse, three-way RRF) — Phase 152.
//!
//! # Syntax (per CONTEXT.md D-09)
//!
//! ```text
//! FT.SEARCH idx <text_query>
//!     HYBRID VECTOR @<dense_field> $<param>
//!     [SPARSE @<sparse_field> $<param>]
//!     FUSION RRF
//!     [WEIGHTS <w_bm25> <w_dense> <w_sparse>]
//!     [K_PER_STREAM N]
//!     [LIMIT offset count]
//!     PARAMS <count> <name1> <blob1> ...
//! ```
//!
//! # Invariants
//! - If `HYBRID` keyword is absent, `parse_hybrid_modifier` returns `Ok(None)` and
//!   caller falls through to the existing FT.SEARCH path (CONTEXT D-18 — zero impact
//!   on Phase 149–151 code paths).
//! - `FUSION RRF` is mandatory in v1. Other fusion modes return `Frame::Error`.
//! - `WEIGHTS` requires exactly 3 finite non-negative floats (CONTEXT D-17);
//!   `0.0` disables a stream silently; negative / NaN → `Frame::Error`.
//! - `K_PER_STREAM` default = `max(60, 3 * top_k)` (CONTEXT D-15), computed by
//!   [`HybridQuery::effective_k_per_stream`].
//! - A `SPARSE` clause on an index lacking the named sparse field returns
//!   `Frame::Error("ERR sparse field not defined in index")` (CONTEXT D-16) — this
//!   is enforced in [`execute_hybrid_search_local`], not the parser.
//!
//! This file owns the local (single-shard) path. Multi-shard coordination with
//! DFS pre-pass for global BM25 IDF lives in Plan 05.

use bytes::Bytes;

use crate::protocol::Frame;

use super::ft_search::parse::{extract_param_blob, parse_usize};
use super::{extract_bulk, matches_keyword};

// ─── Types ────────────────────────────────────────────────────────────────────

/// Fully-assembled hybrid query ready for local execution.
///
/// Produced by the caller (FT.SEARCH dispatcher) by combining:
/// - [`HybridQueryPartial`] from [`parse_hybrid_modifier`] (HYBRID clause fields)
/// - the FT.SEARCH index name and text query (already parsed from args[0..2])
/// - top_k / offset / count resolved from the LIMIT clause (if present)
#[derive(Debug, Clone)]
pub struct HybridQuery {
    pub index_name: Bytes,
    pub text_query: Bytes,
    pub dense_field: Bytes,
    pub dense_blob: Bytes,
    /// `(field_name, blob)` — absent ⇒ two-way fusion (BM25 + dense only) per D-16.
    pub sparse: Option<(Bytes, Bytes)>,
    /// `[w_bm25, w_dense, w_sparse]` — caller-validated finite non-negative per D-17.
    pub weights: [f32; 3],
    /// Per-stream candidate cap. `None` ⇒ use default `max(60, 3 * top_k)` (D-15).
    pub k_per_stream: Option<usize>,
    /// Final top-K after fusion.
    pub top_k: usize,
    /// LIMIT offset (default 0).
    pub offset: usize,
    /// LIMIT count (default top_k).
    pub count: usize,
}

impl HybridQuery {
    /// Resolve the effective per-stream candidate cap per CONTEXT D-15.
    ///
    /// Default: `max(60, 3 * top_k)`. This gives each stream at least 60
    /// candidates (matching RRF's k-constant headroom) and scales with top_k
    /// so fusion has enough rescue headroom for docs appearing in multiple streams.
    #[inline]
    pub fn effective_k_per_stream(&self) -> usize {
        self.k_per_stream
            .unwrap_or_else(|| 60usize.max(3usize.saturating_mul(self.top_k.max(1))))
    }
}

/// Output of [`parse_hybrid_modifier`] — the pure HYBRID-clause fields plus the
/// index into `args` one past the last HYBRID token consumed, so downstream
/// parsers (LIMIT, SESSION) can continue scanning without re-reading the HYBRID
/// block.
#[derive(Debug, Clone)]
pub struct HybridQueryPartial {
    pub dense_field: Bytes,
    pub dense_blob: Bytes,
    pub sparse: Option<(Bytes, Bytes)>,
    pub weights: [f32; 3],
    pub k_per_stream: Option<usize>,
    /// Index into `args` one past the last HYBRID-modifier token consumed.
    /// LIMIT / SESSION / PARAMS parsers may start their scan from here.
    pub end_index: usize,
}

// ─── Parser ───────────────────────────────────────────────────────────────────

/// Parse the HYBRID modifier from FT.SEARCH args.
///
/// Returns:
/// - `Ok(None)` — HYBRID keyword absent; caller falls through to existing path (D-18)
/// - `Ok(Some(..))` — valid HYBRID clause
/// - `Err(Frame::Error)` — HYBRID present but malformed; caller returns the frame
///
/// Resolution of `$<param>` references happens inline via the existing
/// `extract_param_blob` helper (same shape as KNN's `$blob` resolution).
pub fn parse_hybrid_modifier(args: &[Frame]) -> Result<Option<HybridQueryPartial>, Frame> {
    // Scan for HYBRID keyword (may appear after text_query, after LIMIT — arbitrary order).
    let mut i = 0;
    while i < args.len() {
        if matches_keyword(&args[i], b"HYBRID") {
            break;
        }
        i += 1;
    }
    if i >= args.len() {
        return Ok(None);
    }

    // ── VECTOR @<field> $<param> (mandatory) ──────────────────────────────────
    i += 1;
    if i + 2 >= args.len() || !matches_keyword(&args[i], b"VECTOR") {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR HYBRID requires VECTOR @field $blob",
        )));
    }
    let dense_field = extract_field_token(&args[i + 1])
        .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid VECTOR field")))?;
    let dense_param = extract_param_token(&args[i + 2])
        .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid VECTOR blob reference")))?;
    let dense_blob = resolve_param_blob(args, &dense_param)?;
    i += 3;

    // ── Optional SPARSE @<field> $<param> ─────────────────────────────────────
    let sparse = if i < args.len() && matches_keyword(&args[i], b"SPARSE") {
        if i + 2 >= args.len() {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR SPARSE requires @field $blob",
            )));
        }
        let sf = extract_field_token(&args[i + 1])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid SPARSE field")))?;
        let sp = extract_param_token(&args[i + 2])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid SPARSE blob reference")))?;
        let sb = resolve_param_blob(args, &sp)?;
        i += 3;
        Some((sf, sb))
    } else {
        None
    };

    // ── FUSION RRF (mandatory in v1) ──────────────────────────────────────────
    if i + 1 >= args.len() || !matches_keyword(&args[i], b"FUSION") {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR HYBRID requires FUSION RRF",
        )));
    }
    let fusion_name = extract_bulk(&args[i + 1])
        .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid FUSION mode")))?;
    if !fusion_name.eq_ignore_ascii_case(b"RRF") {
        let mut msg = b"ERR unknown FUSION mode: ".to_vec();
        msg.extend_from_slice(&fusion_name);
        return Err(Frame::Error(Bytes::from(msg)));
    }
    i += 2;

    // ── Optional WEIGHTS w_bm25 w_dense w_sparse (D-17) ───────────────────────
    let mut weights = [1.0f32, 1.0, 1.0];
    if i < args.len() && matches_keyword(&args[i], b"WEIGHTS") {
        // Need 3 more args after the WEIGHTS keyword.
        if i + 3 >= args.len() {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR WEIGHTS requires exactly 3 floats",
            )));
        }
        for j in 0..3 {
            let w = parse_f32(&args[i + 1 + j]).ok_or_else(|| {
                Frame::Error(Bytes::from_static(b"ERR invalid weight value"))
            })?;
            if !w.is_finite() || w < 0.0 {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR hybrid weights must be finite and non-negative",
                )));
            }
            weights[j] = w;
        }
        i += 4;
    }

    // ── Optional K_PER_STREAM N (D-15) ────────────────────────────────────────
    let k_per_stream = if i < args.len() && matches_keyword(&args[i], b"K_PER_STREAM") {
        if i + 1 >= args.len() {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR K_PER_STREAM requires a count",
            )));
        }
        let k = parse_usize(&args[i + 1])
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR invalid K_PER_STREAM")))?;
        i += 2;
        Some(k)
    } else {
        None
    };

    Ok(Some(HybridQueryPartial {
        dense_field,
        dense_blob,
        sparse,
        weights,
        k_per_stream,
        end_index: i,
    }))
}

// ─── Parser helpers (private) ─────────────────────────────────────────────────

/// Extract an `@field` or bare field name as Bytes. Strips leading `@` if present.
fn extract_field_token(frame: &Frame) -> Option<Bytes> {
    let raw = extract_bulk(frame)?;
    if let Some(stripped) = raw.strip_prefix(b"@") {
        Some(Bytes::copy_from_slice(stripped))
    } else {
        Some(raw)
    }
}

/// Extract a `$param` reference. Keeps the leading `$` for `resolve_param_blob`
/// to distinguish PARAMS-references from literal blobs.
fn extract_param_token(frame: &Frame) -> Option<Bytes> {
    extract_bulk(frame)
}

/// Resolve `$<name>` to the bytes of the named PARAMS entry. Literal bytes
/// (without `$` prefix) pass through unchanged. Unknown parameter names error.
fn resolve_param_blob(args: &[Frame], param_ref: &Bytes) -> Result<Bytes, Frame> {
    if let Some(name) = param_ref.strip_prefix(b"$") {
        extract_param_blob(args, name).ok_or_else(|| {
            let mut msg = b"ERR unknown parameter: ".to_vec();
            msg.extend_from_slice(name);
            Frame::Error(Bytes::from(msg))
        })
    } else {
        Ok(param_ref.clone())
    }
}

/// Parse a Frame as `f32`. Accepts bulk-string UTF-8 ("1.5", "NaN") and integer frames.
fn parse_f32(frame: &Frame) -> Option<f32> {
    match frame {
        Frame::BulkString(b) => std::str::from_utf8(b).ok()?.parse::<f32>().ok(),
        Frame::Integer(n) => Some(*n as f32),
        _ => None,
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    /// Build an args vector like real FT.SEARCH:
    /// `[index_name, query, ...hybrid_tokens..., PARAMS n name1 blob1 ...]`
    fn args_with_params(
        hybrid_tokens: Vec<Frame>,
        params: Vec<(&[u8], &[u8])>,
    ) -> Vec<Frame> {
        let mut args = vec![bs(b"myidx"), bs(b"machine learning")];
        args.extend(hybrid_tokens);
        if !params.is_empty() {
            args.push(bs(b"PARAMS"));
            args.push(bs(format!("{}", params.len() * 2).as_bytes()));
            for (n, v) in params {
                args.push(bs(n));
                args.push(bs(v));
            }
        }
        args
    }

    #[test]
    fn test_parse_hybrid_missing_keyword_returns_none() {
        // Non-HYBRID args should pass through untouched.
        let args = vec![
            bs(b"myidx"),
            bs(b"machine learning"),
            bs(b"LIMIT"),
            bs(b"0"),
            bs(b"10"),
        ];
        let got = parse_hybrid_modifier(&args).expect("no error");
        assert!(got.is_none(), "HYBRID absent → Ok(None)");
    }

    #[test]
    fn test_parse_hybrid_minimal() {
        // HYBRID VECTOR @vec $qv FUSION RRF   PARAMS 2 qv <blob>
        let args = args_with_params(
            vec![
                bs(b"HYBRID"),
                bs(b"VECTOR"),
                bs(b"@vec"),
                bs(b"$qv"),
                bs(b"FUSION"),
                bs(b"RRF"),
            ],
            vec![(b"qv", b"DENSE_BLOB")],
        );
        let got = parse_hybrid_modifier(&args).expect("no error").expect("some");
        assert_eq!(got.dense_field, Bytes::from_static(b"vec"));
        assert_eq!(got.dense_blob, Bytes::from_static(b"DENSE_BLOB"));
        assert!(got.sparse.is_none());
        assert_eq!(got.weights, [1.0, 1.0, 1.0]);
        assert!(got.k_per_stream.is_none());
    }

    #[test]
    fn test_parse_hybrid_with_sparse() {
        let args = args_with_params(
            vec![
                bs(b"HYBRID"),
                bs(b"VECTOR"),
                bs(b"@vec"),
                bs(b"$qv"),
                bs(b"SPARSE"),
                bs(b"@sp"),
                bs(b"$qs"),
                bs(b"FUSION"),
                bs(b"RRF"),
            ],
            vec![(b"qv", b"DBLOB"), (b"qs", b"SBLOB")],
        );
        let got = parse_hybrid_modifier(&args).expect("no error").expect("some");
        assert_eq!(got.dense_field, Bytes::from_static(b"vec"));
        let (sf, sb) = got.sparse.expect("sparse");
        assert_eq!(sf, Bytes::from_static(b"sp"));
        assert_eq!(sb, Bytes::from_static(b"SBLOB"));
    }

    #[test]
    fn test_parse_hybrid_weights() {
        let args = args_with_params(
            vec![
                bs(b"HYBRID"),
                bs(b"VECTOR"),
                bs(b"@vec"),
                bs(b"$qv"),
                bs(b"FUSION"),
                bs(b"RRF"),
                bs(b"WEIGHTS"),
                bs(b"1.0"),
                bs(b"1.5"),
                bs(b"0.5"),
            ],
            vec![(b"qv", b"DBLOB")],
        );
        let got = parse_hybrid_modifier(&args).expect("no error").expect("some");
        assert_eq!(got.weights, [1.0, 1.5, 0.5]);
    }

    #[test]
    fn test_parse_hybrid_k_per_stream() {
        let args = args_with_params(
            vec![
                bs(b"HYBRID"),
                bs(b"VECTOR"),
                bs(b"@vec"),
                bs(b"$qv"),
                bs(b"FUSION"),
                bs(b"RRF"),
                bs(b"K_PER_STREAM"),
                bs(b"100"),
            ],
            vec![(b"qv", b"DBLOB")],
        );
        let got = parse_hybrid_modifier(&args).expect("no error").expect("some");
        assert_eq!(got.k_per_stream, Some(100));
    }

    #[test]
    fn test_parse_hybrid_rejects_non_rrf_fusion() {
        let args = args_with_params(
            vec![
                bs(b"HYBRID"),
                bs(b"VECTOR"),
                bs(b"@vec"),
                bs(b"$qv"),
                bs(b"FUSION"),
                bs(b"FOO"),
            ],
            vec![(b"qv", b"DBLOB")],
        );
        let err = parse_hybrid_modifier(&args).expect_err("must error");
        match err {
            Frame::Error(msg) => {
                let s = std::str::from_utf8(&msg).unwrap();
                assert!(
                    s.contains("unknown FUSION mode"),
                    "got: {s}"
                );
                assert!(s.contains("FOO"), "must echo offending mode: {s}");
            }
            _ => panic!("expected Frame::Error"),
        }
    }

    #[test]
    fn test_parse_hybrid_rejects_negative_weight() {
        let args = args_with_params(
            vec![
                bs(b"HYBRID"),
                bs(b"VECTOR"),
                bs(b"@vec"),
                bs(b"$qv"),
                bs(b"FUSION"),
                bs(b"RRF"),
                bs(b"WEIGHTS"),
                bs(b"1.0"),
                bs(b"-0.5"),
                bs(b"1.0"),
            ],
            vec![(b"qv", b"DBLOB")],
        );
        let err = parse_hybrid_modifier(&args).expect_err("must error");
        match err {
            Frame::Error(msg) => assert_eq!(
                &msg[..],
                b"ERR hybrid weights must be finite and non-negative"
            ),
            _ => panic!("expected Frame::Error"),
        }
    }

    #[test]
    fn test_parse_hybrid_rejects_nan_weight() {
        let args = args_with_params(
            vec![
                bs(b"HYBRID"),
                bs(b"VECTOR"),
                bs(b"@vec"),
                bs(b"$qv"),
                bs(b"FUSION"),
                bs(b"RRF"),
                bs(b"WEIGHTS"),
                bs(b"1.0"),
                bs(b"NaN"),
                bs(b"1.0"),
            ],
            vec![(b"qv", b"DBLOB")],
        );
        let err = parse_hybrid_modifier(&args).expect_err("must error");
        if let Frame::Error(msg) = err {
            assert_eq!(
                &msg[..],
                b"ERR hybrid weights must be finite and non-negative"
            );
        } else {
            panic!("expected Frame::Error");
        }
    }

    #[test]
    fn test_parse_hybrid_rejects_wrong_weight_count() {
        // WEIGHTS with only 2 floats followed by PARAMS — the PARAMS keyword
        // should trip parse_f32 (which cannot parse "PARAMS") OR the arg-count
        // guard if PARAMS clause is absent. We test the arg-count guard directly:
        let args = vec![
            bs(b"myidx"),
            bs(b"q"),
            bs(b"HYBRID"),
            bs(b"VECTOR"),
            bs(b"@vec"),
            bs(b"$qv"),
            bs(b"FUSION"),
            bs(b"RRF"),
            bs(b"WEIGHTS"),
            bs(b"1.0"),
            bs(b"1.0"),
            // Intentionally only 2 floats, no trailing tokens.
            bs(b"PARAMS"),
            bs(b"2"),
            bs(b"qv"),
            bs(b"DBLOB"),
        ];
        // With PARAMS following WEIGHTS, the 3rd "weight" slot parses as
        // "PARAMS" → parse_f32 returns None → ERR invalid weight value.
        let err = parse_hybrid_modifier(&args).expect_err("must error");
        if let Frame::Error(msg) = err {
            let s = std::str::from_utf8(&msg).unwrap();
            assert!(
                s.contains("WEIGHTS requires exactly 3 floats")
                    || s.contains("invalid weight value"),
                "got: {s}"
            );
        } else {
            panic!("expected Frame::Error");
        }
    }

    #[test]
    fn test_parse_hybrid_rejects_wrong_weight_count_exact() {
        // Arg-count guard path: WEIGHTS with only 2 trailing weight-slots (not 3)
        // AND no trailing non-weight tokens. PARAMS placed BEFORE HYBRID so the
        // $qv reference still resolves (PARAMS scan is global, not position-bound).
        // We test the arg-count guard specifically: 3 * w_i slots would walk off
        // the end of args, so the guard `i + 3 >= args.len()` trips before parse_f32.
        let args = vec![
            bs(b"myidx"),
            bs(b"q"),
            bs(b"PARAMS"),
            bs(b"2"),
            bs(b"qv"),
            bs(b"DBLOB"),
            bs(b"HYBRID"),
            bs(b"VECTOR"),
            bs(b"@vec"),
            bs(b"$qv"),
            bs(b"FUSION"),
            bs(b"RRF"),
            bs(b"WEIGHTS"),
            bs(b"1.0"),
            bs(b"1.0"),
            // Only 2 weight-slots available before end-of-args → guard trips.
        ];
        let err = parse_hybrid_modifier(&args).expect_err("must error");
        if let Frame::Error(msg) = err {
            assert_eq!(&msg[..], b"ERR WEIGHTS requires exactly 3 floats");
        } else {
            panic!("expected Frame::Error");
        }
    }

    #[test]
    fn test_parse_hybrid_accepts_zero_weight() {
        // Zero weights are silently allowed per D-17 (disables stream).
        let args = args_with_params(
            vec![
                bs(b"HYBRID"),
                bs(b"VECTOR"),
                bs(b"@vec"),
                bs(b"$qv"),
                bs(b"FUSION"),
                bs(b"RRF"),
                bs(b"WEIGHTS"),
                bs(b"0.0"),
                bs(b"1.0"),
                bs(b"1.0"),
            ],
            vec![(b"qv", b"DBLOB")],
        );
        let got = parse_hybrid_modifier(&args).expect("no error").expect("some");
        assert_eq!(got.weights, [0.0, 1.0, 1.0]);
    }

    #[test]
    fn test_parse_hybrid_extracts_dollar_blob() {
        // $mydense resolves via PARAMS.
        let args = args_with_params(
            vec![
                bs(b"HYBRID"),
                bs(b"VECTOR"),
                bs(b"@vec"),
                bs(b"$mydense"),
                bs(b"FUSION"),
                bs(b"RRF"),
            ],
            vec![(b"mydense", b"HELLO_WORLD_BLOB")],
        );
        let got = parse_hybrid_modifier(&args).expect("no error").expect("some");
        assert_eq!(got.dense_blob, Bytes::from_static(b"HELLO_WORLD_BLOB"));
    }

    #[test]
    fn test_parse_hybrid_unknown_param_errors() {
        let args = args_with_params(
            vec![
                bs(b"HYBRID"),
                bs(b"VECTOR"),
                bs(b"@vec"),
                bs(b"$missing"),
                bs(b"FUSION"),
                bs(b"RRF"),
            ],
            vec![(b"other", b"SOMETHING")],
        );
        let err = parse_hybrid_modifier(&args).expect_err("must error");
        if let Frame::Error(msg) = err {
            let s = std::str::from_utf8(&msg).unwrap();
            assert!(s.contains("unknown parameter"), "got: {s}");
            assert!(s.contains("missing"), "must echo name: {s}");
        } else {
            panic!("expected Frame::Error");
        }
    }

    #[test]
    fn test_effective_k_per_stream_default() {
        let hq = HybridQuery {
            index_name: Bytes::new(),
            text_query: Bytes::new(),
            dense_field: Bytes::new(),
            dense_blob: Bytes::new(),
            sparse: None,
            weights: [1.0, 1.0, 1.0],
            k_per_stream: None,
            top_k: 10,
            offset: 0,
            count: 10,
        };
        // max(60, 3*10) = 60
        assert_eq!(hq.effective_k_per_stream(), 60);

        let hq50 = HybridQuery { top_k: 50, ..hq.clone() };
        // max(60, 150) = 150
        assert_eq!(hq50.effective_k_per_stream(), 150);

        let hq_explicit = HybridQuery {
            k_per_stream: Some(200),
            ..hq
        };
        assert_eq!(hq_explicit.effective_k_per_stream(), 200);
    }
}
