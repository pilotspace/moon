# moondb examples

Runnable demos that exercise the `moondb` SDK end-to-end. Each example is
self-contained, deterministic, and idempotent — run it twice against the same
server and you get the same results both times.

## Prerequisites

1. **A running Moon server.** Build and start locally:

   ```bash
   cargo build --release
   ./target/release/moon --port 6399 --shards 4
   ```

2. **The `moondb` SDK installed with the `examples` extra.** From `sdk/python/`:

   ```bash
   uv sync --extra examples
   # or, with pip:
   pip install -e '.[examples]'
   ```

   The `examples` extra pulls in `numpy>=1.24` for deterministic vector
   generation. `numpy` is **not** a default dependency of `moondb` — it is
   only required when running these scripts.

## `pilotspace_example.py` — PilotSpace-shaped demo

A full-fidelity demo that mirrors the PilotSpace data shape (issues with
`title`, `body`, `status`, `priority`, `vec`). Seeds an `issues` index with 20
deterministic sample issues, then runs three representative queries:

1. **BM25 text search** — `client.text.text_search("issues", "crash on
   startup", limit=5)` — shows BM25 ranking with field highlighting.
2. **Aggregate facet** — `client.text.aggregate(...)` with `GroupBy(["priority"],
   [Count(alias="n")]) + SortBy("n", "DESC")` — shows priority distribution.
3. **Hybrid search** — `client.text.hybrid_search("issues", "crash bug memory",
   vector=..., weights=(1.0, 1.5, 0.0))` — shows fused BM25 + dense RRF.

### Run against a live server

```bash
uv run python examples/pilotspace_example.py --host localhost --port 6399
```

### Run without a server (dry-run mode, CI-friendly)

```bash
uv run python examples/pilotspace_example.py --dry-run
```

Dry-run replaces `MoonClient` with a `unittest.mock.MagicMock` whose return
values are pre-seeded, exercises every dispatch path, and never touches the
network. Useful for smoke-testing SDK upgrades locally or in CI.

### CLI flags

| Flag            | Default       | Description                                            |
| --------------- | ------------- | ------------------------------------------------------ |
| `--host`        | `localhost`   | Moon server hostname                                   |
| `--port`        | `6399`        | Moon server port                                       |
| `--index`       | `issues`      | FT index name to create / query                        |
| `--seed`        | `0xC0DE`      | RNG seed for deterministic fixture + vectors           |
| `--num-issues`  | `20`          | Number of sample issues to seed                        |
| `--dim`         | `384`         | Dense vector dimensionality                            |
| `--dry-run`     | (off)         | Use a mocked client (no server needed)                 |

### Expected output (truncated)

```
== Text search: 'crash on startup' ==
  [3.812] issue:002  title='Crash on startup when config is missing'
  [2.946] issue:005  title='Null pointer at cold startup'
  ...

== Aggregate facet (by priority) ==
  priority=P0  n=7
  priority=P1  n=7
  priority=P2  n=6

== Hybrid search: 'crash bug memory' ==
  [0.832] issue:003  title='Auth flow returns 500 on expired JWT'
  [0.817] issue:001  title='Memory leak in session cache under load'
  ...

Done.
```

### Exit codes

| Code | Meaning                                              |
| ---- | ---------------------------------------------------- |
| `0`  | Demo completed successfully                          |
| `2`  | Could not reach Moon server (connection error)       |

### Deterministic fixtures

Every run with the same `--seed` produces the same sample issues, the same
384-dim vectors, and the same query vector. `build_sample_issues(seed=0xC0DE)`
called twice yields structurally identical lists — useful for regression tests.

## Public module API

The example is also importable as a module:

```python
from examples.pilotspace_example import (
    DEFAULT_SEED,
    SampleIssue,
    build_sample_issues,
    query_embedding,
    run_demo,
    main,
)

issues = build_sample_issues(n=50, dim=384, seed=0xCAFE)
# ... use issues.embedding / issues.title / etc.
```

## Related

- `moondb.TextCommands` — full-text command surface (`create_text_index`,
  `text_search`, `aggregate`, `hybrid_search`)
- `moondb.integrations.langchain.MoonVectorStore` — LangChain hybrid retriever
  (pass `search_type="hybrid"`)
- `moondb.integrations.llamaindex.MoonVectorStore` — LlamaIndex hybrid query
  mode (`VectorStoreQueryMode.HYBRID`)
