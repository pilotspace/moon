# Moon Graph Subsystem — Deep Review (2026-06-16)

## Section A — Architecture Map

### Component Inventory

| Module / Type | Role | Key file:line |
|---|---|---|
| `types.rs` | Core types: `NodeKey`/`EdgeKey` (SlotMap generational), `PropertyMap` (SmallVec<4>), `PropertyValue`, `MutableNode`, `MutableEdge`, `GraphSegmentHeader` (`#[repr(C, align(64))]`, CRC32-validated), `EdgeMeta`, `NodeMeta` | `types.rs:1` |
| `memgraph.rs::MemGraph` | Mutable write buffer; SlotMap-backed adjacency lists; soft-delete via `deleted_lsn`; freeze to `FrozenMemGraph`; O(1) neighbor iteration with no allocation via `NeighborIter` | `memgraph.rs:31` |
| `store.rs::GraphStore` | Per-shard registry; `Option<HashMap<Bytes, NamedGraph>>` (lazy init); monotonic LSN counter; WAL pending buffer; `AtomicU64` version token | `store.rs:111` |
| `store.rs::NamedGraph` | Named graph: `GraphSegmentHolder` (ArcSwap), `MemGraph` write buf, `plan_cache: Mutex<PlanCache>`, `key_to_node: HashMap<Bytes, NodeKey>`, `GraphStats` | `store.rs:29` |
| `csr/mod.rs::CsrSegment` | Immutable CSR graph segment; `row_offsets: Vec<u32>`, `col_indices: Vec<u32>`, `edge_meta`, `node_meta`, `edge_created_ms` (v3+); `RoaringBitmap` validity; MPH + HashMap dual-lookup | `csr/mod.rs:40` |
| `csr/mmap.rs::MmapCsrSegment` | Zero-copy mmap variant; raw pointer slices into mapped region; runtime alignment checks; `unsafe impl Send+Sync` with full SAFETY documentation | `csr/mmap.rs:18` |
| `csr/storage.rs::CsrStorage` | Enum dispatch: `Heap(CsrSegment)` or `Mmap(MmapCsrSegment)`; prefers mmap on load | `csr/storage.rs` |
| `index.rs::MphNodeIndex` | boomphf minimal perfect hash; O(1) NodeKey→CSR-row; 3 bits/key; fuzz target guards duplicate-key panic | `index.rs` |
| `index.rs::LabelIndex` / `EdgeTypeIndex` | Roaring bitmap indexes per label and per edge-type; O(1) membership | `index.rs` |
| `cypher/lexer.rs::Lexer` | Logos-based zero-allocation lexer on `&[u8]`; 2-slot lookahead; comments skipped; errors → `continue` (skip, not crash) | `lexer.rs:180` |
| `cypher/parser/mod.rs::Parser` | Recursive descent; nesting depth tracked (`DEFAULT_MAX_NESTING_DEPTH = 32`); returns typed `CypherError`, never panics | `parser/mod.rs:91` |
| `cypher/ast.rs` | Clause/Expr/Pattern AST; parameterized queries via `Expr::Parameter` (injection-safe) | `cypher/ast.rs` |
| `cypher/planner.rs::compile` | AST→`PhysicalPlan` (NodeScan / Expand / Filter / Project / ShortestPath / etc.); cost-based strategy (`estimate_graph_first_cost`, `estimate_vector_first_cost`); `PlanCache` (xxhash64, 1024 entries, LRU-absent eviction) | `planner.rs:273` |
| `cypher/executor/read.rs::execute` | Operator pipeline over `Vec<Row>` (`HashMap<String,Value>`); BFS for multi-hop expand (MAX_HOPS_LIMIT=20, MAX_RESULT_ROWS=100K); `SegmentMergeReader` for cross-segment neighbor lookup | `executor/read.rs:6` |
| `cypher/executor/write.rs` | Mutable executor: CREATE, DELETE, SET, MERGE; produces `MutationRecord` for WAL and TXN rollback | `executor/write.rs` |
| `cypher/executor/shortest_path.rs` | Delegates to `DijkstraTraversal::shortest_path`; optional temporal-decay cost function | `executor/shortest_path.rs` |
| `traversal.rs::SegmentMergeReader` | Unified neighbor view across MemGraph + all CSR segments; `neighbors_into` for zero-alloc hot-path reuse; deduplication via `HashSet<NodeKey>` | `traversal.rs:94` |
| `traversal.rs::BoundedBfs` | BFS with frontier cap (100K) and depth limit; sequential | `traversal.rs:283` |
| `traversal.rs::ParallelBfs` | Morsel-parallel BFS (threshold 256, morsel 128); `DashSet` visited set in parallel phase; `std::thread::scope` | `traversal.rs:377` |
| `traversal.rs::DijkstraTraversal` | Weighted shortest path; `WeightedCostFn` for decay; depth cap via `depth_map` | `traversal.rs:~630` |
| `hybrid.rs` | Four hybrid graph+vector patterns (HYB-01 through HYB-04); BFS neighborhood collection + cosine scoring via `simd::cosine_similarity`; vector-guided walk; graph-constrained re-ranker | `hybrid.rs:1` |
| `simd.rs::cosine_similarity` | SIMD-dispatched cosine: NEON (aarch64 mandatory), AVX2+FMA (x86 runtime detected), scalar fallback; all paths tested | `simd.rs:251` |
| `segment.rs::GraphSegmentHolder` | `ArcSwap<SegmentSnapshot>`; lock-free reads; atomic publish of new immutable segments | `segment.rs` |
| `wal.rs` | RESP array serialization for graph WAL records | `wal.rs` |
| `compaction.rs` | Freeze MemGraph → CSR; scheduled on `should_compact()` threshold | `compaction.rs` |
| `recovery.rs` / `replay.rs` | Load CSR segments from disk; replay WAL into MemGraph | `recovery.rs`, `replay.rs` |
| `command/graph/graph_read.rs` | Handlers: GRAPH.QUERY, GRAPH.RO_QUERY, GRAPH.EXPLAIN, GRAPH.PROFILE, GRAPH.NEIGHBORS, GRAPH.VSEARCH, GRAPH.HYBRID | `graph_read.rs` |
| `command/graph/graph_write.rs` | Handlers: GRAPH.CREATE, GRAPH.ADDNODE, GRAPH.ADDEDGE, GRAPH.DELETE | `graph_write.rs` |
| `command/graph/mod.rs` | Dispatch: `is_graph_write_cmd`, `is_cypher_write_query` (double-parse), `dispatch_graph_read/write/command` | `mod.rs:24` |

### Data Flow (GRAPH.QUERY path)

```
Client RESP → shard event loop
  → is_cypher_write_query() [parses Cypher once to detect write clauses]
  → dispatch_graph_read (read) or dispatch_graph_write (write)
     → graph_query() / graph_query_or_write()
        → parse_cypher(bytes)          -- Logos lexer + recursive descent
        → planner::compile(ast)        -- PhysicalPlan (cached by xxhash64)
        → executor::execute(graph, plan, params, ctx)
           → PhysicalOp::NodeScan      -- MemGraph.iter_nodes()
           → PhysicalOp::Expand        -- SegmentMergeReader.neighbors[_into]
                                          (MemGraph + all CSR segments)
           → PhysicalOp::Filter        -- eval_expr on each row
           → PhysicalOp::Project       -- columnar output
        → Frame::Array([headers, rows, stats])
```

For write queries: `execute_mut` applies changes to `graph.write_buf` (MemGraph), appends `MutationRecord` for WAL serialization, checks `should_compact()` for CSR freeze.

### Lifecycle / State Machine

```
MemGraph (mutable, SlotMap)
  ↓  [edge_threshold reached → freeze()]
FrozenMemGraph (Vec snapshot)
  ↓  [CsrSegment::from_frozen()]
CsrSegment (heap) → write_to_file → MmapCsrSegment (zero-copy)
  ↓  [GraphSegmentHolder.add_immutable()]
ArcSwap<SegmentSnapshot>  [lock-free concurrent reads]

CSR file format versions:
  v1 → 32-byte NodeMeta (no bi-temporal)
  v2 → 48-byte NodeMeta (valid_from, valid_to)
  v3 → + edge_created_ms array (wall-clock stamps for decay scoring)
```

On recovery: metadata JSON → empty NamedGraph shells → load CSR from files → replay WAL into MemGraph.

### Design Decisions Observed

1. **SlotMap generational indices** (`NodeKey`, `EdgeKey`): ABA-proof on long-running servers. Stable keys after deletion are a hard correctness property. (`types.rs:10`)
2. **Soft-delete via `deleted_lsn = u64::MAX`**: MVCC without reallocation. The sentinel `u64::MAX` means "alive"; any LSN value means "deleted at LSN N". (`memgraph.rs:55`)
3. **Dual CSR lookup (MPH + HashMap)**: boomphf MPH is the fast path; the fallback HashMap prevents incorrect `None` from hash collisions. (`csr/mod.rs:261`)
4. **Label bitmap is 32-bit**: caps the system at 32 distinct labels per CSR-serialized node. Labels 32+ are silently dropped on freeze. (`csr/mod.rs:161`)
5. **CSR stores outgoing edges only**: incoming neighbors in CSR segments are skipped; `Direction::Incoming` silently contributes 0 neighbors from CSR, only MemGraph. (`traversal.rs:191`)
6. **Plan cache keyed by raw query bytes (xxhash64)**: parameter values do not affect the plan, which is correct since parameters are `Expr::Parameter` nodes resolved at execution time. Cache size is 1024 entries with arbitrary (HashMap key-order) eviction. (`planner.rs:110`)
7. **Double-parse on GRAPH.QUERY**: `is_cypher_write_query()` in dispatch and `graph_query_or_write()` both parse the Cypher string. The second parse is redundant on the hot path. (`mod.rs:36`, `mod.rs:111`)
8. **Parallel BFS copies `nb_buf` for each node** (`nb_buf.clone()` at `traversal.rs:453`): correct but heap-allocates a `Vec<MergedNeighbor>` per frontier node during the parallel phase collection step.

### Concurrency Model

- **GraphStore** is fully per-shard, no cross-shard sharing. All writes happen on the shard thread (mutable reference). No `Arc<Mutex<>>` on the write path.
- **Segment reads** are lock-free via `ArcSwap<SegmentSnapshot>` (`segment.rs`). Readers load a snapshot, pin it, iterate immutable CSR slices.
- **`plan_cache`** is `parking_lot::Mutex<PlanCache>` inside `NamedGraph`. This is held briefly for cache lookup/insert; never across an `.await` point (all graph execution is synchronous within the shard dispatch).
- **ParallelBfs** spawns OS threads via `std::thread::scope` but the `SegmentMergeReader` stays on the main thread (it borrows `&MemGraph` which is `!Send`). The scope ensures all threads complete before returning; no dangling references possible.
- **MmapCsrSegment** has `unsafe impl Send + Sync` with complete SAFETY justification: the mapped region is read-only, all mutation is via `&mut self` methods on the `validity` bitmap only. (`mmap.rs:47-56`)

---

## Section B — Code Audit (Ranked Findings)

### P0 — Correctness / Crash / Security / Data Loss

**[P0] CSR incoming-edge gap: `Direction::Both`/`Direction::Incoming` silently loses CSR edges — `traversal.rs:191`**

The SegmentMergeReader skips all CSR neighbor iteration when `self.direction == Direction::Incoming`. For `Direction::Both`, CSR contributes outgoing edges only, meaning the incoming half is invisible in immutable segments. Once a MemGraph is frozen to CSR, any query with an incoming or bidirectional traversal will miss those edges. There is no error, no log — results are silently incomplete. For `Direction::Both`, the MemGraph portion covers both directions, so the gap only manifests after compaction.

Fix: Build and maintain a reverse CSR (or a full adjacency CSR with both directions) alongside the forward CSR. Alternatively, annotate CSR edges bidirectionally at build time and serve `Direction::Incoming` from a separate offset block. Until fixed, `should_compact()` must not trigger if any active query requires `Incoming` traversal over compacted data.

---

**[P0] Label bitmap truncates silently at label ID ≥ 32 — `csr/mod.rs:161`**

During `CsrSegment::from_frozen`, node labels are packed into a `u32` bitmap. The guard `if label < 32 { label_bitmap |= 1 << label; }` silently drops any label with ID ≥ 32. Since `label_to_id` is a pure hash (`xxhash` truncated), labels assigned IDs ≥ 32 are invisible in immutable CSR segments after compaction. The MemGraph (pre-compaction) stores all labels correctly via `SmallVec`, so the discrepancy only surfaces after freeze. A graph with > 32 unique label types will exhibit phantom label matches (missing nodes) in CSR-backed scans.

Fix: Extend `label_bitmap` to u64 (doubles capacity to 64), or use a `RoaringBitmap` per node (heavier). At minimum, add a `debug_assert!(label < 32)` and return a hard error from `from_frozen` if any label exceeds the limit, rather than silently dropping it.

---

### P1 — Performance Hot-Path / Lock / Unsafe / Unwrap in Lib

**[P1] `unwrap()` in WAL test helper bleeds into shared source file — `wal.rs:228-243`**

`wal.rs` contains `parse_resp_array` — a test-only helper function — inside a `#[cfg(test)] mod tests` block. Six `unwrap()` calls appear there (`wal.rs:228, 230, 232, 239, 241, 243`). These are technically inside a test module, so they do not violate the library-code rule. However, the CLAUDE.md rubric requires explicit audit of all `unwrap()` in non-test paths. **Verdict:** All six are inside `mod tests` — compliant. No action required.

**[P1] `advance()` in Parser uses `unwrap_or` with a synthetic `Token::Null` — `parser/mod.rs:491`**

`Parser::advance()` returns `self.lexer.next_token().unwrap_or(SpannedToken { token: Token::Null, span: 0..0 })`. The comment says "Caller must have checked peek() first." However, this fallback produces a `Token::Null` token that may propagate silently through the `parse_primary` arm at `expr.rs:228` which matches `Token::Null` and returns `Expr::Null` — making malformed input silently produce a null expression rather than an error in certain edge cases. The pattern is not a direct panic, but it violates the "malformed input must error, never silently succeed" principle. The call sites in `parse_clause` always call `peek_token_ref()` first (which returns `Err` on EOF), so the fallback should be unreachable in practice — but it is not statically enforced.

Fix: Replace `unwrap_or(synthetic)` with `unreachable!("advance called without peeking")` or restructure callers to always pass through `expect_token_match`.

**[P1] Double-parse of Cypher string on every GRAPH.QUERY — `mod.rs:36-48` and `mod.rs:111-112`**

`dispatch_graph_command` calls `is_cypher_write_query()` which fully parses the Cypher (lexer + recursive descent) to detect write clauses, then immediately calls `graph_query_or_write()` which parses again. This doubles parser CPU on every query. For typical 100-byte queries the overhead is small, but for complex queries with deeply nested expressions this is measurable.

Fix: Pass the already-parsed `CypherQuery` (or a `bool is_write` flag) from `is_cypher_write_query` into `graph_query_or_write` to avoid re-parsing.

**[P1] `Instant::now()` called per query in `execute()` and `execute_mut()` — `executor/read.rs:12`, `executor/write.rs:15`**

CLAUDE.md requires timestamp caching (no `Instant::now()` per-key). The graph executor calls `std::time::Instant::now()` at the top of every query execution. This is one call per query, not per-key, so it is materially different from the vector-search anti-pattern. For high-QPS graphs (thousands of short queries/second) this adds one syscall per query. For GRAPH.PROFILE (`executor/read.rs:523, 541`) the comments acknowledge this is a debug path. The non-profile `execute()` call at line 12 lacks this justification.

Fix: Pass the shard-cached wall-clock timestamp (`current_time_ms()`) into the executor context and use it for `execution_time_us` measurement. This matches the existing `ClockPin` pattern used elsewhere.

**[P1] ParallelBfs clones `Vec<MergedNeighbor>` per frontier node — `traversal.rs:453`**

During parallel BFS, the collect phase does `nb_buf.clone()` for each frontier node to produce `neighbor_lists: Vec<Vec<MergedNeighbor>>`. For a frontier of 10K nodes this materializes 10K heap-allocated `Vec`s before spawning threads. The comment acknowledges the `!Send` reader constraint but the clone is avoidable: collect all neighbors into a single flat `Vec<MergedNeighbor>` with offsets (like a miniature CSR), avoiding per-node allocation.

Fix: `let mut flat: Vec<MergedNeighbor> = Vec::new(); let mut offsets: Vec<usize> = vec![0];` then iterate frontier, append to flat, push offset. Pass slices into the parallel morsels.

**[P1] `traversal.rs` exceeds 1500-line file-size limit — `traversal.rs:1586 lines`**

CLAUDE.md hard cap is 1500 lines per file. `traversal.rs` is 1586 lines. Similarly `hybrid.rs` is 1189 lines (under the 1500 cap but worth noting as approaching the command-group split threshold of 1000 for read/write split).

Fix: Split `traversal.rs` into `traversal/mod.rs` (shared types, `SegmentMergeReader`), `traversal/bfs.rs` (`BoundedBfs`, `ParallelBfs`), `traversal/dfs.rs` (`BoundedDfs`), `traversal/dijkstra.rs` (`DijkstraTraversal`), `traversal/merge_reader.rs` (`SegmentMergeReader`). Re-export via `pub use` in `mod.rs`.

### P2 — Maintainability / Style / File Size

**[P2] Plan cache eviction is arbitrary (HashMap key-order), not LRU — `planner.rs:131`**

The `PlanCache::insert` evicts "an arbitrary key" by calling `self.cache.keys().next()` — which returns entries in HashMap's random bucket order. For a 1024-entry cache this means frequently-used plans can be evicted while stale ones survive. The comment acknowledges this but marks it as "simple eviction".

Fix: Use an LRU structure (`lru` crate) or at minimum track access counts per plan to evict the least-recently-used entry. The current behavior is P2 because it degrades performance without causing correctness errors.

**[P2] `cypher/parser/mod.rs` is 1140 lines — approaching the 1000-line split threshold**

The parser `mod.rs` includes clause dispatch, all helper methods, and 60+ tests. Expression and pattern parsing are already split into `expr.rs` and `pattern.rs`. Remaining clause parsers (MERGE, CALL, WITH, UNWIND, ORDER BY) and test bodies could move to reduce the main file to under 800 lines, matching the project's read/write split convention.

**[P2] `SegmentMergeReader` CSR contribution assigns synthetic `EdgeKey` from `KeyData::from_ffi(0)` — `traversal.rs:222`**

CSR edges do not carry a real `EdgeKey` (they are not in a SlotMap). The merge reader emits `slotmap::KeyData::from_ffi(0).into()` as a placeholder. The executor stores this in `row.insert(evar.clone(), Value::Edge(merged.edge))` for single-hop edge variable binding. A downstream predicate like `WHERE r.valid_to >= $asof` will resolve the edge via `memgraph.get_edge(merged.edge)` which will return `None` (key 0 is invalid), silently evaluating as `Null`. No error is raised.

Fix: Either (a) prohibit edge variable binding when the matched edge may originate from a CSR segment, returning a clear error, or (b) materialize edge properties into `MergedNeighbor` at read time so CSR edges carry the data they need.

**[P2] `execute()` starts with a `roaring::RoaringBitmap::new()` per `PhysicalOp::NodeScan` and per `PhysicalOp::Expand` — `executor/read.rs:33, 95`**

These empty bitmaps are passed into `is_node_visible(node, ..., &committed, ...)` where `committed` represents "committed transactions" for MVCC visibility. Since the current MVCC is partial (txn_id 0 = no transaction), the bitmap is always empty and the allocation is pure waste on every query. This is not on the per-key inner loop but is per-operator, which means O(operators) allocations per query.

Fix: Move `committed` to a `OnceLock<RoaringBitmap>` or pass a `&'static EMPTY_BITMAP` constant.

### P3 — Nits

**[P3] `advance()` comment "Caller must have checked peek() first" is unenforced — `parser/mod.rs:489`**

The contract is informal. A `debug_assert!(self.lexer.peek().is_some(), "advance without peek")` would catch future callers violating this invariant in debug builds.

**[P3] `parse_primary` fallback arms return `Ok(Expr::Null)` for unreachable branches — `expr.rs:195, 206, 239`**

Inside `Token::Integer`, `Token::Float`, `Token::Parameter` arms, after matching and advancing, the else branch returns `Ok(Expr::Null)`. These branches are statically unreachable (the token was already matched on peek) but are not annotated as such. Replace with `unreachable!()` inside `#[cfg(debug_assertions)]` or use `let t = self.advance(); let Token::Integer(s) = t.token else { unreachable!() };`.

**[P3] `GraphStore::resident_bytes()` loads an ArcSwap snapshot per call — `store.rs:274`**

`graph.segments.load()` clones an `Arc` for each named graph. For large stores with many graphs this is O(graph_count) Arc clones per call. This is only used for reporting, so it is a P3.

---

## Unsafe Audit

All `unsafe` blocks in the graph subsystem are well-justified:

- `csr/mmap.rs:47-56` — `unsafe impl Send/Sync`: SAFETY comments explicitly state the mmap region is immutable and mutation only touches the validity bitmap behind `&mut self`.
- `csr/mmap.rs:154-232` — pointer arithmetic for mmap slices: SAFETY comments document bounds validation and alignment checks that precede each `unsafe` block. Runtime alignment gates return `CsrError::InvalidData` rather than proceeding with misaligned access.
- `simd.rs:52, 140, 229` — NEON/AVX2 intrinsics: SAFETY comments cite the mandatory/runtime-detected feature invariants. NEON is unconditional on aarch64; AVX2 is gated by `is_x86_feature_detected!` at the call site.
- `mmap.rs:397-418` — `madvise`: SAFETY comment references the valid mmap region.

**No `unsafe` block is missing a `// SAFETY:` comment.** This is a clean bill on the unsafe audit.

---

## Parser Defensiveness Assessment

The Cypher parser is robust against malformed client input:

1. **Stack overflow prevention**: `DEFAULT_MAX_NESTING_DEPTH = 32` (`parser/mod.rs:88`). Exceeded nesting returns `CypherError::NestingDepthExceeded`, not a stack overflow. Tested explicitly at `mod.rs:770-796`.
2. **Lexer errors skipped, not panicked**: `lexer.rs:239` — `Err(())` from Logos → `continue`, not panic. Unrecognized bytes are silently skipped; the parser handles the resulting unexpected-token condition via `CypherError::UnexpectedToken`.
3. **EOF safety**: All `peek_token_ref` and `expect_token_match` calls return `CypherError::UnexpectedEof` on empty input. No `unwrap()` on `next_token()` in the main parse path.
4. **Injection prevention**: `$param` tokens produce `Expr::Parameter(name)` — never interpolated into the query string. Tested at `mod.rs:739-763`.
5. **Fuzz target**: `fuzz/fuzz_targets/` includes a Cypher parser fuzz target. The CSR deserializer fuzz target found the duplicate-external-id panic (now fixed at `csr/mod.rs:640-648` and `mmap.rs:242-248`).
6. **One gap**: `advance()` at `parser/mod.rs:491` uses `unwrap_or(Token::Null)` instead of returning an error. In practice this path is not reachable from valid call sequences, but the fallback silently produces `Expr::Null` rather than `CypherError::UnexpectedEof`. Low severity (the path is guarded by callers), but violates the stated contract.

---

## Verdict

The Moon graph subsystem is architecturally sound and well-engineered for a property graph embedded in a Redis-compatible server. The core data model — SlotMap generational indices, SmallVec adjacency lists, ArcSwap segment snapshots, CSR with Roaring validity bitmaps — is correct and cache-friendly. The Cypher parser is defensively written (depth-limited, EOF-safe, injection-proof, fuzz-covered) with one minor advance()-fallback gap. Unsafe code is appropriately scoped and fully commented.

The two P0 findings require immediate attention before production use: the CSR incoming/bidirectional edge gap silently returns incomplete traversal results after compaction, and the 32-label bitmap limit silently loses label information. Both are data-correctness issues invisible to the caller. The P1 findings (double-parse, Instant::now per query, parallel BFS allocation) are performance issues, not correctness bugs. The 1586-line `traversal.rs` file exceeds the project's own limit and should be split.

---

## Top-3 Fix List

1. **[P0] Fix CSR incoming/bidirectional edge gap** (`traversal.rs:191`): Build a reverse edge index in CSR (or emit bidirectional edge rows at compaction time) so `Direction::Incoming` and `Direction::Both` produce correct results from immutable segments. This is a silent data-loss bug that surfaces only after the first compaction.

2. **[P0] Fix silent label truncation at label ID ≥ 32** (`csr/mod.rs:161`): Extend the label bitmap to `u64` or return a hard `CsrError` from `from_frozen` when any label exceeds the limit. The current silent drop makes graphs with > 32 label types unreliable after compaction.

3. **[P1] Split `traversal.rs`** (1586 lines, exceeds the project 1500-line cap): Extract `SegmentMergeReader` into `traversal/merge_reader.rs`, BFS variants into `traversal/bfs.rs`, DFS into `traversal/dfs.rs`, Dijkstra into `traversal/dijkstra.rs`. This also makes the parallel-BFS allocation fix (P1) easier to implement in isolation.
