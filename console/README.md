# Moon Console

React + TypeScript admin UI for [Moon](../README.md). Built with Vite, Tailwind,
Zustand, Monaco, Recharts, and three.js. Bundled into the server binary when
built with `cargo build --features console` and served from Moon's admin port
at `/ui/`.

## Development

```bash
pnpm install
pnpm dev                # Vite dev server on http://localhost:5173
                        # proxies /api → http://localhost:9100
```

Start Moon separately so the dev server has an API to talk to:

```bash
# from repo root, via OrbStack (moon-dev VM)
cargo build --release
./target/release/moon --port 6399 --admin-port 9100 --shards 1
```

## Build

```bash
pnpm build              # tsc -b && vite build → dist/
```

The `dist/` directory is consumed by the Rust build when `--features console`
is enabled; it is embedded into the binary via `rust-embed`.

## Testing

Two layers of automated tests live in `console/`:

### Unit tests (Vitest)

Vitest with jsdom + React Testing Library covers Zustand stores, pure logic
(Monarch tokenizers, completion provider), and small components.

```bash
pnpm install
pnpm test               # run once (CI-shaped)
pnpm test:watch         # watch mode for TDD
pnpm test:coverage      # v8 coverage report → ./coverage/
```

Test files live in `tests/unit/` and follow `*.test.ts[x]`.

### End-to-end smoke tests (Playwright)

Playwright drives Chromium against a running Moon instance and asserts each
view route renders without placeholder text.

**Prerequisite:** Moon must be running on port 9100 with the `console` feature:

```bash
# Linux / OrbStack VM (moon-dev)
cargo build --release --features console
./target/release/moon --admin-port 9100 --shards 1
```

Then install browsers once and run the suite:

```bash
pnpm test:e2e:install   # downloads chromium (one-time)
pnpm test:e2e
```

Override the target URL via `MOON_CONSOLE_URL`:

```bash
MOON_CONSOLE_URL=http://my-host:9100/ui/ pnpm test:e2e
```

Reports land in `playwright-report/` (HTML) and `test-results/` (traces,
screenshots, videos on failure). Both are gitignored.

The suite contains 7 tests across 6 spec files — one per view (Dashboard,
Browser, Console, Vectors, Graph, Memory). Each spec asserts route mount +
absence of dev placeholder text. Specs do NOT depend on populated data; they
pass against a fresh Moon instance.

### CI (runs both suites)

```bash
pnpm ci:test            # runs `pnpm test && pnpm test:e2e`
```

CI is expected to spawn Moon before invoking `ci:test`; see phase 136 for the
full integration harness.
