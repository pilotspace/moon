//! Telemetry helpers for OpenTelemetry tracing spans.
//!
//! When the `otel` feature is enabled, this module provides span helpers
//! that wrap key operations with `tracing` spans. These spans are
//! automatically exported when an OpenTelemetry subscriber layer is
//! installed (future work: add `tracing-opentelemetry` + `opentelemetry-otlp`).
//!
//! When `otel` is disabled, all helpers are no-ops with zero overhead --
//! the compiler eliminates them entirely.
//!
//! # Usage
//!
//! ```rust,ignore
//! use crate::telemetry;
//!
//! // In FT.SEARCH handler:
//! let _span = telemetry::span_ft_search("my_index", 10, 384);
//! // ... do search work ...
//! // span is dropped (and ended) when _span goes out of scope.
//! ```

/// Guard type returned by span helpers. Dropping it ends the span.
///
/// When `otel` is disabled, this is a zero-sized type that the compiler
/// optimizes away completely.
#[cfg(feature = "otel")]
pub struct SpanGuard {
    _span: tracing::span::EnteredSpan,
}

#[cfg(not(feature = "otel"))]
pub struct SpanGuard;

// ── FT.SEARCH ──────────────────────────────────────────────────────────

/// Create a tracing span for FT.SEARCH operations.
///
/// Records index name, k (neighbor count), and vector dimension as span fields.
#[cfg(feature = "otel")]
#[inline]
pub fn span_ft_search(index: &str, k: usize, dim: usize) -> SpanGuard {
    let span = tracing::info_span!(
        "ft.search",
        otel.name = "ft.search",
        index = index,
        k = k,
        dim = dim,
    );
    SpanGuard {
        _span: span.entered(),
    }
}

#[cfg(not(feature = "otel"))]
#[inline(always)]
pub fn span_ft_search(_index: &str, _k: usize, _dim: usize) -> SpanGuard {
    SpanGuard
}

// ── FT.CACHESEARCH ─────────────────────────────────────────────────────

/// Create a tracing span for FT.CACHESEARCH operations.
///
/// Records index name, threshold, and whether the result was a cache hit.
/// The `cache_hit` field should be updated by the caller after determining
/// the cache status.
#[cfg(feature = "otel")]
#[inline]
pub fn span_ft_cachesearch(index: &str, threshold: f32) -> SpanGuard {
    let span = tracing::info_span!(
        "ft.cachesearch",
        otel.name = "ft.cachesearch",
        index = index,
        threshold = threshold,
        cache_hit = tracing::field::Empty,
    );
    SpanGuard {
        _span: span.entered(),
    }
}

#[cfg(not(feature = "otel"))]
#[inline(always)]
pub fn span_ft_cachesearch(_index: &str, _threshold: f32) -> SpanGuard {
    SpanGuard
}

// ── FT.RECOMMEND ───────────────────────────────────────────────────────

/// Create a tracing span for FT.RECOMMEND operations.
///
/// Records index name and the number of positive/negative example keys.
#[cfg(feature = "otel")]
#[inline]
pub fn span_ft_recommend(index: &str, positive_count: usize, negative_count: usize) -> SpanGuard {
    let span = tracing::info_span!(
        "ft.recommend",
        otel.name = "ft.recommend",
        index = index,
        positive_count = positive_count,
        negative_count = negative_count,
    );
    SpanGuard {
        _span: span.entered(),
    }
}

#[cfg(not(feature = "otel"))]
#[inline(always)]
pub fn span_ft_recommend(
    _index: &str,
    _positive_count: usize,
    _negative_count: usize,
) -> SpanGuard {
    SpanGuard
}

// ── FT.NAVIGATE ────────────────────────────────────────────────────────

/// Create a tracing span for FT.NAVIGATE operations.
///
/// Records index name, k, and the number of graph hops.
#[cfg(feature = "otel")]
#[inline]
pub fn span_ft_navigate(index: &str, k: usize, hops: u32) -> SpanGuard {
    let span = tracing::info_span!(
        "ft.navigate",
        otel.name = "ft.navigate",
        index = index,
        k = k,
        hops = hops,
    );
    SpanGuard {
        _span: span.entered(),
    }
}

#[cfg(not(feature = "otel"))]
#[inline(always)]
pub fn span_ft_navigate(_index: &str, _k: usize, _hops: u32) -> SpanGuard {
    SpanGuard
}

// ── Generic span helper ────────────────────────────────────────────────

/// Create a tracing span for any named operation.
///
/// Use this for ad-hoc instrumentation of operations that don't have
/// a dedicated helper above.
#[cfg(feature = "otel")]
#[inline]
pub fn span_operation(name: &'static str) -> SpanGuard {
    let span = tracing::info_span!("moon.operation", otel.name = name,);
    SpanGuard {
        _span: span.entered(),
    }
}

#[cfg(not(feature = "otel"))]
#[inline(always)]
pub fn span_operation(_name: &'static str) -> SpanGuard {
    SpanGuard
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn span_guards_are_created_and_dropped() {
        // Ensure no panics when creating and dropping span guards.
        let _s1 = span_ft_search("test_idx", 10, 384);
        let _s2 = span_ft_cachesearch("test_idx", 0.95);
        let _s3 = span_ft_recommend("test_idx", 3, 1);
        let _s4 = span_ft_navigate("test_idx", 10, 2);
        let _s5 = span_operation("custom_op");
    }

    #[cfg(not(feature = "otel"))]
    #[test]
    fn span_guard_is_zero_sized() {
        assert_eq!(std::mem::size_of::<SpanGuard>(), 0);
    }
}
