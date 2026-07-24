# Benchmark tooling

This directory owns the benchmark implementations, active schemas, machine-readable
terminology, configuration-spelling compatibility data, and source fixtures.

Primary entry points:

- `incremental_speed.py`: measure indexing lifecycles and emit canonical fact tables.
- `run_experiments.py`: build and execute immutable, resumable experiment matrices.
- `summarize_results.py`: render quality-gated Markdown from retained result JSON.
- `fact_comparisons.py`: derive parity, capability-delta, and lifecycle tables from
  canonical facts.
- `autotune.py`: run the isolated PageRank tuning experiment.

`schema/` contains schemas for records emitted by current tooling.
`terminology.json` defines every normative fact, step, join, and formula identifier.
The generated human view remains in `docs/BENCHMARK_TERMINOLOGY.md`, and the full
workflow is documented in `docs/BENCHMARK_EXPERIMENTS.md`.

Files under `scripts/` with historical benchmark names are compatibility frontends,
not independent implementations. `docs/schema/benchmark-facts-v1.schema.json` is the
only schema intentionally left outside this directory because retained v1 bundles
embed that frozen URI.
