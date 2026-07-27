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

The automatic `--quick` and `--full` matrices default to MCP transport. Cross-build
CLI matrices require a dedicated OS account/runtime or a quiescent account-wide CBM
daemon: current one-shot CLI commands enforce the same exact-build cohort as MCP and
correctly reject a candidate that differs from an active daemon. MCP matrices require
an isolated account/runtime or one active daemon compatible with every candidate.
The runner does not silently change transports. Neither entry point requires Docker.

For example, this runs the full repeated matrix against the exact local `main` ref:

```sh
uv run python benchmarks/run_experiments.py --full --transport cli \
  --candidate-ref upstream-main=main \
  --experiment-root /durable/path/full-head-vs-main
```

Run that command only after verifying that its OS account has no active CBM daemon,
or from a dedicated benchmark account. Merely changing `CBM_CACHE_DIR`, the Git
worktree, or the experiment root does not create a separate daemon cohort.

`upstream-main` is the stable candidate role used by the report schema. Overriding
its ref does not rename the role, so cite the resolved ref and commit recorded in the
expanded plan when describing results.

For ongoing development, a compact `--matrix-spec` may use arbitrary candidate
`{"label": "...", "ref": "branch-or-commit"}` entries. The runner resolves,
production-builds, hashes, and archives those candidates before expanding the
existing immutable plan schema. Profiles remain the reusable configuration axis:
use `config_overrides` for product config keys, `product_environment` for explicit
`CBM_*` process knobs such as `CBM_WORKERS`, and `benchmark_args` for additive
workload flags. Candidate, profile, and scenario scopes can add new branches,
capabilities, and controlled sweeps without editing the built-in dated presets.
Top-level `build_environment` is shared across candidates and accepts only `CC`,
`CXX`, `EXTRA_CFLAGS`, and `EXTRA_CXXFLAGS`; probes and builds retain those values
in candidate identity, and arbitrary environment keys are rejected.
See [the complete matrix example](../docs/BENCHMARK_EXPERIMENTS.md#reusable-ref-based-matrices).

For cross-build measurements while the host daemon remains active, use the native
container coordinator. It creates an exact Git bundle, runs the same experiment
runner with no host bind mounts, and exports immutable results back to the named
history:

```sh
uv run python benchmarks/run_container_experiment.py \
  --matrix-spec /absolute/path/development-comparison.json \
  --experiment-root /durable/ignored/path/development-comparison \
  --cpus 4 --memory 8g --workers 4
```

CPU, memory, and worker budgets are required rather than guessed. Candidate builds,
Git worktrees, caches, daemon state, and result generation remain on two labeled
Docker volumes; the coordinator prints their exact names and retains them for
auditable resume. Rerun the same source spec and experiment root to resume, or remove
the printed volumes after exported results are verified. The measured container is
always native `arm64` or `amd64`, resource bounded, and removed on success or failure.
Each invocation writes a content-addressed container-environment manifest, so changed
arguments create a new audit record instead of replacing history. Failed candidate
build logs are exported under `container-failures/<source-commit>/build-logs/`; if
that export itself fails, the error identifies the retained work volume and path.
Each measured source/spec/resource cohort is isolated under
`runsets/<content-id>/`; `--audit-only` resolves to the same cohort and cannot create
a second attempt, while a different commit, ref bundle, spec, or resource budget
cannot be misreported as an unplanned cell in the current runset.
Container numbers are controlled Linux relative comparisons, not absolute macOS
latency. See [Container isolation](../docs/BENCHMARK_EXPERIMENTS.md#container-isolation).

`schema/` contains schemas for records emitted by current tooling.
`terminology.json` defines every normative fact, step, join, and formula identifier.
The generated human view remains in `docs/BENCHMARK_TERMINOLOGY.md`, and the full
workflow is documented in `docs/BENCHMARK_EXPERIMENTS.md`.

The upstream-owned `scripts/benchmark-index.sh`, `scripts/benchmark-search-graph.sh`,
and `scripts/clone-bench-repos.sh` retain their established locations. Branch-created
Python benchmark implementations live here without executable compatibility copies.
`docs/schema/benchmark-facts-v1.schema.json` retains its frozen URI because v1 bundles
embed that identifier.
