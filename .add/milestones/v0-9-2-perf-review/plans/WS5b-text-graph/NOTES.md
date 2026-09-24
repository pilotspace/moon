# WS5b-text-graph — working notes (ADD discipline, not a shared artifact)

## Context loaded
- TEAM-RULES, PLAN, MILESTONE, personas (performance-engineer lead, routing-dispatch lens), CONVENTIONS
  FTS lessons (distinct asymmetric fixtures; assert non-zero ran-count per suite), text-postings-persistence.md.
- Frozen contracts I must not break:
  - fts-posting-rank-tf §3: `PostingList::tf(doc)` / `positions_for(doc)` rank-correct, `tf_absent = 0`,
    `doc_ids` stays a RoaringBitmap, M4 byte-identical BM25 on ascending corpora.
  - fts-upsert-incremental §3: `remove_doc` O(terms-in-doc), same return semantics, results-identical.
  - fts-query-eval-dispatch §3: `eval_set` pub (membership), `eval_query(..) -> Vec<TextSearchResult>`,
    wire reply unchanged, order score DESC / doc_id ASC, pure-filter docs score 0.0, DFS path preserved.
  - graph-cypher-inline-filter §3: plan shape (Filter after binding op); no new PhysicalOp variant.

## Decisions
- #1191: bit-identical scores (not just f32-tolerance): the single pass keeps HEAD's exact f32 op order
  (0.0 + leaf1 + leaf2…, per-field 0.0 + t1 + t2…, fuzzy max with `>`), IDF hoisted with the same
  expression (`bm25_idf`/`bm25_len_norm`/`bm25_from_parts` are a literal split of `bm25_score`).
  Wire score is `{:.6}` so bit-identity == byte-identical replies.
- Membership (`eval_set`) is bitmap algebra only; resolvability is one `&= live_docs` (new bitmap kept in
  lock-step with `doc_id_to_key`), so `total` = cardinality.
- Tie order (score DESC, doc_id ASC) via a bounded heap on a total key; NaN (unreachable with finite
  weights) sinks to the bottom instead of HEAD's algorithm-dependent placement.
- #1195: fresh-doc-id-on-upsert rejected — it changes tie order for updated docs (visible reply change vs
  HEAD), needs dead-doc masking in df/IDF/DFS/persist, and migrating TAG/NUMERIC entries to the new id.
  Chosen: chunked rank-aligned tf/positions (flat below a threshold, blocks above) — doc ids unchanged,
  zero semantic change, `.tpost` format unchanged.
