# MILESTONE: Graph Correctness & Cypher Filtering

goal: Moon's graph queries are correct: Cypher MATCH narrows on inline node-property predicates instead of full-scanning the label, directional traversal covers incoming/Both edges post-compaction, and node labels >= 32 are no longer silently dropped.
rationale: new-major (split 2/3) — part of the v3 "secondary-engine correctness & parity" theme from the 2026-06-16 deep review + 4-feature benchmark. No closed milestone (v1 shared-nothing, v2/v2-1 throughput) covers graph-query correctness. This slice owns the three graph defects the review/benchmark surfaced; FTS (v3-1) and Vector/KV (v3-3) are sibling slices. Correctness only — Moon's native graph build + 1-hop already beat FalkorDB (bench §11.4).
stage: production · status: planned · created: 2026-06-16

> SDD living doc for this milestone. Keep it THIN: breadth, shared decisions, and
> exit criteria only — per-task detail lives in each `.add/tasks/<slug>/TASK.md`,
> written just-in-time. Update this doc whenever a task reveals a milestone gap.

## Scope
In:
- Cypher MATCH narrows on an inline node-property predicate (e.g. `MATCH (a {id:N})`) instead of
  full-scanning the label. (bench: `cypher_match_rows` = 14,991 ≈ |E| vs FalkorDB's filtered 4;
  Moon Cypher point-query 40 qps.)
- `Direction::Incoming` and `Direction::Both` traversals return incoming edges after compaction —
  CSR currently stores only outgoing. `src/graph/traversal.rs:188–191`.
- Node label storage supports labels with id >= 32 (the current 32-bit bitmap truncates).
  `src/graph/csr/mod.rs:161` (`if label < 32`).

Out:
- Full openCypher coverage / general predicate pushdown beyond inline node-property equality — only
  the point-filter the benchmark exercised is in scope.
- Graph throughput — native GRAPH.ADDNODE/ADDEDGE build + native 1-hop already lead FalkorDB
  (bench §11.4); this milestone is correctness, not speed.
- Cross-shard graph — single-keyspace per CLAUDE.md, unchanged here.

## Shared decisions & glossary deltas   (living — every task must honor these)
- TDD red/green: each defect lands a FAILING test first (wrong rows / missing incoming edge /
  dropped label), then the fix (CLAUDE.md Rule 3).
- Malformed Cypher must never panic — return an error frame (parser/eval defensiveness).
- New/changed graph commands keep their `scripts/test-consistency.sh` + `scripts/test-commands.sh`
  entries (CLAUDE.md New Commands).
- No new `unsafe` without explicit user approval + a `// SAFETY:` comment.

## Shared / risky contracts (freeze these first)
- Cypher inline-predicate evaluation semantics — how `{prop:val}` narrows the candidate set (index
  probe vs filtered scan) and what it returns. Wrong shape re-does every downstream Cypher query.
  -> owning task `graph-cypher-inline-filter`
- Incoming-edge representation — reverse adjacency vs on-demand scan; a layout choice the label and
  traversal work both read.                                   -> owning task `graph-incoming-edges`

## Tasks (breadth-first decomposition; detail lives in each TASK.md)
- [ ] graph-cypher-inline-filter   depends-on: none   — Cypher MATCH narrows on inline node-property
      predicate instead of full-scanning the label; point-query returns only the matching node's edges.
- [ ] graph-incoming-edges         depends-on: none   — CSR traversal returns incoming / Both-direction
      edges post-compaction (reverse adjacency or incoming index).
- [ ] graph-label-bitmap-overflow  depends-on: none   — node label storage supports id >= 32 without
      silent truncation.

## Exit criteria (observable; map each to the task that delivers it)
- [ ] A Cypher point-query `MATCH (a {id:N})-[]->(b)` returns only node N's edges (returned rows ==
      expected, not ≈|E|) — test asserts the narrowed row count.            (← graph-cypher-inline-filter)
- [ ] On a compacted graph, `Direction::Incoming` and `Both` return the incoming edges (non-empty
      where they should be) — test on a post-compaction graph.              (← graph-incoming-edges)
- [ ] A node assigned label id >= 32 (e.g. a 40-label graph) is matched by its label query — no
      silent drop; test with >= 33 distinct labels.                         (← graph-label-bitmap-overflow)
