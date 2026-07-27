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

The automatic `--quick` and `--full` matrices default to MCP transport. Use
`--transport cli` for a cross-build comparison when an account-wide CBM daemon is
already active: each cell then invokes its candidate directly, so a candidate with
a different build identity cannot conflict with the active daemon. MCP cells require
an isolated account/runtime or an active daemon compatible with every candidate;
the runner does not silently change transports. Neither entry point requires Docker.

For example, this runs the full repeated matrix against the exact local `main` ref:

```sh
uv run python benchmarks/run_experiments.py --full --transport cli \
  --candidate-ref upstream-main=main \
  --experiment-root /durable/path/full-head-vs-main
```

`upstream-main` is the stable candidate role used by the report schema. Overriding
its ref does not rename the role, so cite the resolved ref and commit recorded in the
expanded plan when describing results.

`schema/` contains schemas for records emitted by current tooling.
`terminology.json` defines every normative fact, step, join, and formula identifier.
The generated human view remains in `docs/BENCHMARK_TERMINOLOGY.md`, and the full
workflow is documented in `docs/BENCHMARK_EXPERIMENTS.md`.

The upstream-owned `scripts/benchmark-index.sh`, `scripts/benchmark-search-graph.sh`,
and `scripts/clone-bench-repos.sh` retain their established locations. Branch-created
Python benchmark implementations live here without executable compatibility copies.
`docs/schema/benchmark-facts-v1.schema.json` retains its frozen URI because v1 bundles
embed that identifier.
