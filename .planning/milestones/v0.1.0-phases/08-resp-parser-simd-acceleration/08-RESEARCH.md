# Phase 8: RESP Parser SIMD Acceleration - Research

**Researched:** 2026-03-24
**Domain:** RESP2 parser optimization (memchr SIMD, fast integer parsing, arena allocation)
**Confidence:** HIGH

## Summary

This phase replaces the hand-written byte-by-byte RESP2 parser with SIMD-accelerated scanning and arena-based allocation. The current parser in `parse.rs` uses `std::io::Cursor` with a manual `for` loop in `find_crlf()` that scans one byte at a time -- the classic pattern that SIMD acceleration targets. The `memchr` crate (v2.8.0) provides drop-in replacement with automatic SSE2/AVX2/NEON detection at runtime, scanning ~32 bytes/cycle on AVX2 hardware versus 1 byte/cycle for the current loop. The `atoi` crate (v2.0.0) eliminates the current `str::from_utf8` + `str::parse` two-step integer parsing. The `bumpalo` crate (v3.20.2) provides O(1) bulk deallocation via arena reset after each pipeline batch.

The current codebase has clean separation: `parse()` is the only public entry point called by `RespCodec::decode()`, and `parse_inline()` handles non-RESP-prefixed input. Both share the same CRLF scanning bottleneck. The `Frame` enum and `ParseConfig` are not modified -- this is a pure internal optimization. The existing Criterion bench harness covers single-frame operations but lacks pipelined workload benchmarks needed to validate the 4x improvement target.

**Primary recommendation:** Replace `find_crlf()` with `memchr::memchr(b'\r', buf)`, replace `Cursor` with direct `&[u8]` index arithmetic, replace `read_decimal()`/integer parsing with `atoi::atoi::<i64>()`, and add `bumpalo::Bump` arena to `handle_connection()` for per-batch temporary allocation.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Use `memchr` crate (v2.8+) for all CRLF scanning -- it auto-detects SSE2/AVX2/NEON at runtime
- Replace the `Cursor`-based byte-by-byte scan in `parse_single_frame` with `memchr::memchr(b'\r', buf)` followed by `buf[pos+1] == b'\n'` verification
- Use `memchr::memchr2(b'\r', b'\n', buf)` where detecting either terminator is useful for validation
- Do NOT hand-roll AVX2 intrinsics -- `memchr` already implements the optimal SIMD path internally
- Use `atoi` crate for parsing RESP length prefixes and integer frame values
- Replace the hand-written `parse_integer` function in parse.rs with `atoi::atoi::<i64>(slice)`
- `atoi` handles negative numbers, leading zeros, and overflow detection
- Add `bumpalo` crate as dependency; each connection gets a `Bump` arena
- Arena used for: parsed command argument vectors, temporary string slices during inline parsing
- `bump.reset()` called after each pipeline batch completes (O(1) deallocation)
- Arena NOT used for `Frame` or `Bytes` -- those are owned types that outlive the parse phase
- Keep existing `BytesMut` + `Bytes::split_to().freeze()` zero-copy pattern -- it's already correct
- No changes to the Framed codec layer or connection handler structure -- only parse.rs and inline.rs internals change
- Extend existing Criterion bench (`benches/resp_parsing.rs`) with before/after comparison
- Add pipelined workload benchmark: 32-command pipeline parse throughput
- Add single-command parse latency benchmark for GET/SET patterns

### Claude's Discretion
- Exact memchr call patterns (memchr vs memchr2 vs memmem for multi-byte sequences)
- Whether to replace Cursor with direct index arithmetic for further speedup
- Arena initial capacity sizing (recommend 4KB default)
- Whether inline parser also benefits from memchr (likely yes for space detection)

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| memchr | 2.8.0 | SIMD-accelerated byte search (CRLF detection) | Industry standard; BurntSushi's crate; used by regex, ripgrep; auto-detects SSE2/AVX2/NEON at runtime |
| atoi | 2.0.0 | Parse integers directly from `&[u8]` without UTF-8 detour | ~3x faster than `str::from_utf8` + `str::parse`; handles signed via `FromRadix10SignedChecked` |
| bumpalo | 3.20.2 | Bump allocation arena for per-batch temporaries | ~2ns allocation; O(1) reset; widely used (wasm-bindgen, Dodrio) |

### Supporting (already in project)
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| bytes | 1.10 | Zero-copy buffer management (`BytesMut`/`Bytes`) | Already used -- no changes needed |
| criterion | 0.8 | Benchmarking harness | Already configured -- extend with pipeline benchmarks |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| memchr for CRLF | memmem::find for `\r\n` | memmem has higher setup cost for 2-byte needle; memchr + verify is faster for single-`\r` scan since `\r` without `\n` is extremely rare in RESP |
| atoi | Manual SIMD integer parsing | Massive complexity for negligible gain; integer lines are typically <10 bytes |
| bumpalo | typed-arena | bumpalo supports heterogeneous types and has `reset()`; typed-arena only supports one type per arena |

**Installation:**
```bash
cargo add memchr@2.8 atoi@2.0 bumpalo@3.20
```

## Architecture Patterns

### Recommended Changes to Project Structure
```
src/protocol/
  parse.rs       # PRIMARY TARGET: replace Cursor + find_crlf with memchr + index arithmetic
  inline.rs      # SECONDARY TARGET: replace find_crlf_position with memchr
  frame.rs       # NO CHANGES -- public API contract
src/server/
  codec.rs       # NO CHANGES -- calls parse() which is unchanged in signature
  connection.rs  # MINOR: add Bump arena ownership, pass to parse, reset after batch
benches/
  resp_parsing.rs # EXTEND: add pipeline benchmarks
```

### Pattern 1: memchr-accelerated CRLF scanning (replacing find_crlf)

**What:** Replace the byte-by-byte `for` loop with `memchr::memchr(b'\r', &buf[start..])` followed by `buf[pos+1] == b'\n'` verification.

**When to use:** Every CRLF search in both `parse.rs` and `inline.rs`.

**Example:**
```rust
// Source: memchr docs + project-specific adaptation
use memchr::memchr;

/// Find CRLF in buf starting from `start`. Returns position of \r.
/// Returns None if no complete CRLF found.
#[inline]
fn find_crlf(buf: &[u8], start: usize) -> Option<usize> {
    let search = &buf[start..];
    let mut offset = 0;
    loop {
        match memchr(b'\r', &search[offset..]) {
            Some(pos) => {
                let abs = start + offset + pos;
                if abs + 1 < buf.len() && buf[abs + 1] == b'\n' {
                    return Some(abs);
                }
                // Bare \r without \n -- skip and continue (extremely rare in RESP)
                offset += pos + 1;
            }
            None => return None,
        }
    }
}
```

### Pattern 2: Cursor elimination with direct index tracking

**What:** Replace `Cursor<&[u8]>` with a plain `usize` position tracking index. The Cursor adds method call overhead (`position()`, `set_position()`, `get_ref()`, `has_remaining()`, `get_u8()`) that compiles to the same thing as direct indexing but obscures the optimizer.

**When to use:** All of `parse_single_frame` and its callees.

**Example:**
```rust
// Replace Cursor-based parsing with direct index arithmetic
fn parse_single_frame(
    buf: &[u8],
    pos: &mut usize,
    config: &ParseConfig,
    depth: usize,
) -> Result<Frame, ParseError> {
    if depth > config.max_array_depth {
        return Err(ParseError::Invalid {
            message: format!("array nesting depth {} exceeds maximum {}", depth, config.max_array_depth),
            offset: *pos,
        });
    }
    if *pos >= buf.len() {
        return Err(ParseError::Incomplete);
    }
    let type_byte = buf[*pos];
    *pos += 1;

    match type_byte {
        b'+' => {
            let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
            let line = &buf[*pos..crlf];
            *pos = crlf + 2;
            Ok(Frame::SimpleString(Bytes::copy_from_slice(line)))
        }
        b':' => {
            let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
            let line = &buf[*pos..crlf];
            let n = atoi::atoi::<i64>(line).ok_or_else(|| ParseError::Invalid {
                message: format!("invalid integer: {:?}", std::str::from_utf8(line).unwrap_or("?")),
                offset: *pos,
            })?;
            *pos = crlf + 2;
            Ok(Frame::Integer(n))
        }
        // ... similar for $, *, -
        _ => Err(ParseError::Invalid {
            message: format!("unknown type byte: 0x{:02x}", type_byte),
            offset: *pos - 1,
        }),
    }
}
```

### Pattern 3: atoi for RESP decimal parsing

**What:** Replace `find_crlf` + `str::from_utf8` + `str::parse::<i64>` with `find_crlf` + `atoi::atoi::<i64>(line)`.

**Current code path (3 operations):**
```rust
let line = find_crlf(cursor)?;                    // 1. find CRLF
let s = std::str::from_utf8(line)?;               // 2. validate UTF-8
let n: i64 = s.parse()?;                          // 3. parse integer
```

**New code path (2 operations):**
```rust
let crlf = find_crlf(buf, *pos)?;                 // 1. find CRLF (SIMD)
let n = atoi::atoi::<i64>(&buf[*pos..crlf])?;     // 2. parse directly from bytes
*pos = crlf + 2;
```

**Key detail:** `atoi::atoi::<i64>()` returns `Option<i64>`. It implements `FromRadix10SignedChecked` which handles negative numbers (the `-1` in `$-1\r\n` for null), leading signs, and overflow detection. It returns `None` for empty slices or non-digit input.

### Pattern 4: Inline parser memchr optimization

**What:** Replace `find_crlf_position()` in `inline.rs` with `memchr::memchr(b'\r', buf)` + verify.

**Additionally:** Use `memchr::memchr2(b' ', b'\t', &line[start..])` to accelerate whitespace-delimited token splitting instead of the current `line.split(|&b| b == b' ' || b == b'\t')` closure-based approach. This matters for long inline commands.

**Example:**
```rust
use memchr::memchr2;

/// Split inline line into tokens using SIMD space/tab scanning
fn split_inline_tokens(line: &[u8]) -> Vec<&[u8]> {
    let mut tokens = Vec::new();
    let mut start = 0;
    while start < line.len() {
        // Skip whitespace
        while start < line.len() && (line[start] == b' ' || line[start] == b'\t') {
            start += 1;
        }
        if start >= line.len() {
            break;
        }
        // Find next whitespace using SIMD
        match memchr2(b' ', b'\t', &line[start..]) {
            Some(pos) => {
                tokens.push(&line[start..start + pos]);
                start += pos + 1;
            }
            None => {
                tokens.push(&line[start..]);
                break;
            }
        }
    }
    tokens
}
```

### Pattern 5: Bumpalo arena per-connection

**What:** Add a `bumpalo::Bump` arena to `handle_connection()`, pass it to parse functions for temporary allocations, and call `bump.reset()` after each pipeline batch.

**Integration point:** The arena is created once per connection and lives for the connection's duration. It is reset (not dropped) after each batch cycle.

**Where arena is used:**
- `Vec::with_capacity` for array frame items in `parse_single_frame` -- allocate from arena
- Inline token collection in `parse_inline`
- NOT used for `Frame` or `Bytes` values (they must outlive the parse phase and enter the dispatch pipeline)

**Key constraint:** Bumpalo's `collections` feature provides `bumpalo::collections::Vec` which allocates from the arena. However, since `Frame::Array(Vec<Frame>)` uses `std::vec::Vec`, we would need to either (a) change the Frame type (FORBIDDEN -- API contract) or (b) use the arena only for intermediate scratch buffers, then copy into std Vec. Option (b) is correct.

**Practical arena usage:** The primary win is for the `dispatchable` and `responses` vectors in `handle_connection()` pipeline batching, plus any temporary buffers during parse. The arena avoids individual allocator round-trips for these per-batch temporaries.

```rust
use bumpalo::Bump;

// In handle_connection():
let mut arena = Bump::with_capacity(4096); // 4KB initial

// In the batch processing loop:
loop {
    // ... collect batch ...
    // Process batch
    // ...
    // After batch responses are written:
    arena.reset(); // O(1) bulk deallocation
}
```

**Capacity recommendation:** 4KB default. A 32-command pipeline with 3 args each = ~96 Frame pointers (~768 bytes on 64-bit). 4KB provides comfortable headroom without waste. Bumpalo auto-grows if needed.

### Anti-Patterns to Avoid
- **Do NOT use `memmem::find(b"\r\n")` for CRLF:** The 2-byte needle has higher Finder construction cost. `memchr(b'\r')` + verify is faster because bare `\r` without `\n` is essentially nonexistent in valid RESP protocol data.
- **Do NOT change the `Frame` enum:** The public API (`Frame::Array(Vec<Frame>)`) must remain unchanged. Arena-allocated Vecs cannot escape into Frame.
- **Do NOT add arena lifetime parameters to Frame or ParseConfig:** This would infect the entire codebase with lifetime annotations.
- **Do NOT use `bumpalo` `allocator_api` feature:** It requires nightly Rust. Use the `collections` feature only if needed for scratch Vecs, or just use the arena for raw allocations.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| SIMD byte scanning | AVX2/SSE2 intrinsics for `\r` detection | `memchr::memchr` | Runtime CPU detection, fallback paths, tested on all platforms |
| Integer parsing from bytes | Custom digit-loop with overflow checking | `atoi::atoi::<i64>` | Handles signs, overflow, edge cases; ~3x faster than UTF-8 detour |
| Arena allocation | Custom bump allocator or slab | `bumpalo::Bump` | Battle-tested, correct alignment, auto-grow, O(1) reset |
| CRLF substring search | Manual two-byte pattern matching | `memchr` + verify pattern | Single-byte SIMD scan is faster than 2-byte memmem for this case |

**Key insight:** The performance win comes from replacing O(N) byte-by-byte scans with SIMD-accelerated O(N/32) scans, not from algorithmic changes. The crates handle all the platform-specific complexity.

## Common Pitfalls

### Pitfall 1: atoi returns None for empty slices
**What goes wrong:** If the line between the type byte and CRLF is empty (e.g., `$\r\n` with no length), `atoi::atoi::<i64>(b"")` returns `None`.
**Why it happens:** Edge case in malformed RESP input.
**How to avoid:** Map `None` to `ParseError::Invalid` with a descriptive message. The current code already handles this via `str::parse` failure -- just adapt the error mapping.
**Warning signs:** Tests for malformed input (empty integer, empty length) would catch this.

### Pitfall 2: atoi differs from str::parse for edge cases
**What goes wrong:** `atoi::atoi::<i64>` may accept leading whitespace or behave differently for inputs like `+42` (with explicit plus sign).
**Why it happens:** Different parsing semantics between Rust's `i64::from_str` and `atoi`'s `FromRadix10SignedChecked`.
**How to avoid:** Verify behavior for: empty input, `+0`, `-0`, leading zeros `007`, max/min i64, non-digit characters. Write targeted unit tests for these edge cases.
**Warning signs:** Existing tests passing does not guarantee edge case parity.

### Pitfall 3: memchr position is relative to the search slice
**What goes wrong:** `memchr(b'\r', &buf[start..])` returns position relative to `&buf[start..]`, not relative to `buf`. Forgetting to add `start` causes off-by-N errors.
**Why it happens:** Slice semantics -- memchr operates on the slice it receives.
**How to avoid:** Always compute absolute position: `let abs_pos = start + memchr(b'\r', &buf[start..]).ok_or(Incomplete)?;` Then verify `buf[abs_pos + 1] == b'\n'`.
**Warning signs:** Parsing second/third frames in a pipeline produces wrong data.

### Pitfall 4: Arena lifetime must not escape into Frame
**What goes wrong:** If arena-allocated data is placed into Frame values, the arena reset will invalidate the Frame contents, causing use-after-free.
**Why it happens:** Bumpalo allocations have the arena's lifetime; Frame values outlive the parse phase.
**How to avoid:** Only use the arena for truly temporary data (scratch Vecs used during parsing that are converted to owned data before returning). Frame payloads must continue using `Bytes::copy_from_slice` or `Bytes::split_to().freeze()`.
**Warning signs:** Sporadic corruption after arena.reset() in pipeline workloads.

### Pitfall 5: Benchmark methodology -- BytesMut re-creation overhead
**What goes wrong:** Benchmarks that create `BytesMut::from(&data[..])` inside the measured loop include allocation cost, not just parse cost.
**Why it happens:** The existing benchmarks already have this issue -- `BytesMut::from` allocates.
**How to avoid:** For parse-only throughput measurement, pre-allocate a large `BytesMut` with the pipelined data, clone it for each iteration. For pipeline benchmarks specifically, create a single large buffer with 32 concatenated commands.
**Warning signs:** Variance in benchmark results; allocation dominating parse time.

### Pitfall 6: Bare \r in bulk string data
**What goes wrong:** `memchr(b'\r', buf)` finds `\r` inside bulk string payload data, not the CRLF terminator.
**Why it happens:** Bulk strings are binary-safe and can contain `\r\n` within their payload.
**How to avoid:** For bulk string parsing (`$` type), after reading the length, skip directly to `pos + len` and verify `buf[pos+len] == b'\r' && buf[pos+len+1] == b'\n'`. Do NOT scan for CRLF within bulk string payloads -- the length tells you exactly where the terminator is.
**Warning signs:** The test `test_parse_binary_data_in_bulk_string` (parsing `$4\r\n\r\n\r\n\r\n`) would catch this.

## Code Examples

### Complete find_crlf replacement
```rust
// Source: Verified memchr 2.8.0 API
use memchr::memchr;

/// SIMD-accelerated CRLF finder. Returns absolute position of \r in buf.
#[inline]
fn find_crlf(buf: &[u8], start: usize) -> Option<usize> {
    if start >= buf.len() {
        return None;
    }
    let mut search_from = start;
    loop {
        match memchr(b'\r', &buf[search_from..]) {
            Some(rel_pos) => {
                let abs_pos = search_from + rel_pos;
                if abs_pos + 1 < buf.len() && buf[abs_pos + 1] == b'\n' {
                    return Some(abs_pos);
                }
                // Bare \r -- skip past it and continue
                search_from = abs_pos + 1;
                if search_from >= buf.len() {
                    return None;
                }
            }
            None => return None,
        }
    }
}
```

### Complete read_decimal replacement
```rust
// Source: Verified atoi 2.0.0 API (FromRadix10SignedChecked for i64)
use atoi::atoi as parse_int;

/// Read a CRLF-terminated decimal integer from buf at position pos.
/// Advances pos past the CRLF.
#[inline]
fn read_decimal(buf: &[u8], pos: &mut usize) -> Result<i64, ParseError> {
    let crlf = find_crlf(buf, *pos).ok_or(ParseError::Incomplete)?;
    let line = &buf[*pos..crlf];
    let n = parse_int::<i64>(line).ok_or_else(|| ParseError::Invalid {
        message: format!("invalid decimal: {:?}", String::from_utf8_lossy(line)),
        offset: *pos,
    })?;
    *pos = crlf + 2;
    Ok(n)
}
```

### Pipeline benchmark pattern
```rust
// Source: Criterion docs + project bench pattern
fn bench_parse_pipeline_32(c: &mut Criterion) {
    let config = ParseConfig::default();
    // Build a 32-command pipeline: *3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n x32
    let single_cmd = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n";
    let mut pipeline_data = Vec::with_capacity(single_cmd.len() * 32);
    for _ in 0..32 {
        pipeline_data.extend_from_slice(single_cmd);
    }

    c.bench_function("parse_pipeline_32cmd", |b| {
        b.iter(|| {
            let mut buf = BytesMut::from(&pipeline_data[..]);
            let mut count = 0;
            while let Ok(Some(frame)) = parse(black_box(&mut buf), &config) {
                black_box(&frame);
                count += 1;
            }
            assert_eq!(count, 32);
        })
    });
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| byte-by-byte loop for CRLF | `memchr::memchr` SIMD scan | memchr 2.0+ (2019) | ~6.5x faster byte scanning on x86_64 |
| `str::from_utf8` + `str::parse` | `atoi::atoi` direct from bytes | atoi 1.0+ (2020) | ~3x faster integer parsing, no UTF-8 validation step |
| Per-object malloc/free | Bump arena allocation + reset | bumpalo 2.0+ (2019) | ~2ns alloc vs ~25ns malloc; O(1) bulk free |
| `std::io::Cursor` position tracking | Direct `usize` index | Always available | Eliminates virtual dispatch overhead from Buf trait methods |

**Deprecated/outdated:**
- `Cursor<&[u8]>` for parser position tracking: adds unnecessary abstraction layer; direct indexing is both simpler and faster
- `str::from_utf8` before integer parsing: unnecessary for ASCII digit sequences in RESP protocol

## Open Questions

1. **Arena usage scope -- is it worth the threading complexity?**
   - What we know: The arena must be owned by `handle_connection()` (per-connection). The parse functions in `parse.rs` are currently stateless (they take `&mut BytesMut` and return `Result<Option<Frame>>`).
   - What's unclear: Whether passing `&Bump` through the parse API is worth it when `Frame` itself cannot use arena allocation (it must be `'static`-compatible for dispatch).
   - Recommendation: Start with arena for the connection-level batch vectors (`responses`, `dispatchable`, `aof_entries` in `handle_connection()`). Only extend to parse internals if profiling shows allocator overhead in Vec growth during array parsing. The 4x target may be achievable with memchr + atoi alone.

2. **Bulk string payload -- should we use `Bytes::split_to().freeze()` instead of `copy_from_slice`?**
   - What we know: The current parser uses `Bytes::copy_from_slice(data)` everywhere, which copies. The zero-copy alternative is `buf.split_to(len).freeze()` which shares the underlying BytesMut allocation.
   - What's unclear: Whether the copy vs share tradeoff matters for typical Redis payloads (most are <1KB). Zero-copy requires the BytesMut to not be compacted/reused.
   - Recommendation: This is a separate optimization from SIMD scanning. Keep `copy_from_slice` for now to minimize behavioral changes. Zero-copy can be a follow-up if benchmarks show copy overhead.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in `#[test]` + Criterion 0.8 |
| Config file | `Cargo.toml` `[[bench]]` section |
| Quick run command | `cargo test --lib -p rust-redis -- protocol` |
| Full suite command | `cargo test && cargo bench --bench resp_parsing` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| SIMD-01 | memchr replaces manual byte scanning in parse.rs | unit | `cargo test --lib -- protocol::parse::tests` | Existing tests cover all parse paths |
| SIMD-02 | atoi replaces hand-written integer parsing | unit | `cargo test --lib -- protocol::parse::tests::test_parse_integer` | Existing tests for positive/negative/zero |
| SIMD-03 | Inline parser uses memchr for CRLF and whitespace | unit | `cargo test --lib -- protocol::inline::tests` | Existing tests cover inline parsing |
| SIMD-04 | Criterion benchmarks show >= 4x pipeline improvement | bench | `cargo bench --bench resp_parsing -- parse_pipeline` | Needs new benchmark (Wave 0) |
| SIMD-05 | All existing tests pass unchanged | integration | `cargo test` | Existing full suite |
| ARENA-01 | Per-connection bumpalo arena reset after batch | unit | Manual inspection / connection test | Needs integration verification |

### Sampling Rate
- **Per task commit:** `cargo test --lib -- protocol`
- **Per wave merge:** `cargo test && cargo bench --bench resp_parsing`
- **Phase gate:** Full suite green + benchmark showing >= 4x pipeline improvement

### Wave 0 Gaps
- [ ] `benches/resp_parsing.rs` needs `bench_parse_pipeline_32` function -- covers SIMD-04
- [ ] `benches/resp_parsing.rs` needs `bench_parse_get_single` and `bench_parse_set_single` -- covers single-command latency baseline
- [ ] Unit tests for `atoi` edge cases (empty input, `+42`, `-0`, overflow) -- covers SIMD-02 edge cases

## Sources

### Primary (HIGH confidence)
- [memchr 2.8.0 docs](https://docs.rs/memchr/latest/memchr/) - API surface, memchr/memchr2/memmem functions
- [atoi 2.0.0 docs](https://docs.rs/atoi/latest/atoi/) - `atoi::<i64>()` signature, `FromRadix10SignedChecked` trait
- [bumpalo 3.20.2 docs](https://docs.rs/bumpalo/latest/bumpalo/) - `Bump::new()`, `reset()`, `with_capacity()`, `alloc_slice_copy()`
- [crates.io](https://crates.io) - Version verification: memchr 2.8.0, atoi 2.0.0, bumpalo 3.20.2

### Secondary (MEDIUM confidence)
- Project blueprint `.planning/architect-blue-print.md` - ~32 bytes/cycle throughput claim for AVX2 memchr, ~2ns bump allocation cost
- [memchr GitHub](https://github.com/BurntSushi/memchr) - SIMD implementation details, platform support

### Tertiary (LOW confidence)
- 4x improvement target for 32-command pipeline -- achievable based on SIMD theory but depends on how much time is actually spent in CRLF scanning vs other overhead. Must be validated empirically.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - All three crates verified on crates.io with current versions and API docs
- Architecture: HIGH - Current parse.rs code is straightforward to analyze; replacement patterns are well-established
- Pitfalls: HIGH - Bulk string binary data pitfall (Pitfall 6) is a known RESP parsing trap; atoi edge cases identified from docs
- 4x benchmark target: MEDIUM - Depends on ratio of scan time to total parse time; single small frames may not show 4x; pipeline batches should

**Research date:** 2026-03-24
**Valid until:** 2026-04-24 (stable domain, 30 days)
