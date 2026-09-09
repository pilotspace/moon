# Raw data — boot index reconcile cost model, 2026-09-09

Backs [`../../startup-reconcile-cost-model.md`](../../startup-reconcile-cost-model.md).

| | |
|---|---|
| host | GCE `t2a-standard-8` — Ubuntu 24.04, Linux 7.0.0 aarch64, 8 vCPU, 31 GB RAM |
| load | nothing else running; dedicated instance, started for this run and stopped after |
| moon | `0bfc730d`, `release` profile (fat LTO, codegen-units 1), `RUSTFLAGS="-C target-cpu=native"` |
| binary sha256 | `51de528cd7f1a203` (first 16) |
| harness | `run2.sh` + `load.py` + `reconcile_time.py`, index-per-repo corpus, fixed RNG seed 1234 |

Timing is the interval between the `recovery scanned db` and `auto-reindexed`
log lines of the second boot — the reconcile itself, not process startup.

**Not the reconcile progress lines.** Those tick every 10 s, so a reconcile
shorter than one tick emits none at all; the first version of the extractor read
them and silently reported nothing for every fast run.

## Files

| file | what | reps |
|---|---|---|
| `plane-isolation.tsv` | both planes vs `--no-vector` vs `--no-text`, 500 idx x 70 docs, 300 words | 3 |
| `doc-size.tsv` | 300 / 1200 / 4800 words per doc (key count scaled down as docs grow) | 3 |
| `tpost.tsv` | `.tpost` deleted between boots vs kept, 200 idx x 70 docs, 300 words | 3 |

Columns: label, wall seconds, ms/key, key count, corpus parameters.

## Provenance notes, so these are not read as more than they are

**The corpus is synthetic and its documents are small.** 300 words per document
against a production instance whose 293,439 keys took 1h50m — applying the cost
model backwards puts that corpus at roughly 45x the synthetic document size.
**The ratios in these files transfer; the absolute ms/key does not.**

**Identical numbers across repetitions are expected, not suspicious.**
`load.py` seeds `random.Random(1234)`, so each repetition builds a
byte-identical corpus and does deterministic work on a dedicated host. That is
why `w1200` reads `1.6026` twice. It also means the CVs here measure host
stability, not corpus variation, and are therefore *narrower* than a
production-like noise floor would be.

**`plane-isolation.tsv` came from a first sweep whose B and C sections were
lost.** Every run after the first long one failed: a leftover server held the
port (moon has hung on SIGTERM under REUSEPORT before), the harness's `set -e`
exited before writing `boot2.log`, and the wrapper deleted the directory — so
the failures printed nothing at all. The re-run (`gcp-sweep2.sh`) clears the
port hard between runs and keeps failed directories. Section A was unaffected
and is reported as measured; B and C here are from the re-run, at a smaller
corpus (200 idx instead of 500), which is why their key counts differ.

**A freshness guard gates these numbers.** The host had a binary from a previous
session's build sitting in `target/release/`, and a sweep against it would have
produced real-looking numbers for the wrong commit. The guard refuses unless the
build finished and nothing in `target/release/moon.d` — the build's own declared
inputs — is newer than the binary. Its first version compared the binary against
the build log and rejected every correct build, because cargo appends `Finished`
about 64 ms after linking.
