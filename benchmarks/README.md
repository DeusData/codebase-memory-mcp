# Benchmark tooling

This directory owns the benchmark implementations, active schemas, machine-readable
terminology, configuration-spelling compatibility data, and source fixtures.

Primary entry points:

- `run_benchmark.py`: run one isolated benchmark and emit canonical fact tables.
- `run_experiments.py`: build and execute immutable, resumable benchmark matrices.
- `summarize_results.py`: render quality-gated Markdown from retained result JSON.
- `fact_comparisons.py`: derive parity, capability-delta, and lifecycle tables from
  canonical facts.
- `autotune.py`: run the isolated PageRank tuning experiment.

Start with the built-in help:

```sh
uv run python benchmarks/run_benchmark.py --help
uv run python benchmarks/run_experiments.py --help
```

`run_benchmark.py` is the single-run entry point. `run_experiments.py` is the
multi-candidate, repeated-run entry point; automatic modes store durable ignored
state under `.worktrees/benchmark-campaign/`. Explicit runs should use an ignored
`benchmark-results/` root or another durable path outside the checkout.

`schema/` contains schemas for records emitted by current tooling.
`terminology.json` defines every normative fact, step, join, and formula identifier.
The generated human view remains in `docs/BENCHMARK_TERMINOLOGY.md`, and the full
workflow is documented in `docs/BENCHMARK_EXPERIMENTS.md`.

The upstream-owned `scripts/benchmark-index.sh`, `scripts/benchmark-search-graph.sh`,
and `scripts/clone-bench-repos.sh` retain their established locations. Branch-created
Python benchmark implementations live here without executable compatibility copies.
`docs/schema/benchmark-facts-v1.schema.json` retains its frozen URI because v1 bundles
embed that identifier.
