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

Rank-quality campaign files (see "Rank-quality campaign" below):

- `corpora-v1.json`: pinned real corpora and what each one is for.
- `rank-queries-v1/manifest.json`: the query battery and its relevance judgments.
- `config-keys-v1.json`: config keys the product actually reads (generated).
- `generate_test_labels.py`: derives test/not-test labels from a corpus's own declaration.
  The harness calls it at run time against the measured checkout; the output is data
  derived from a corpus and is gitignored rather than stored.
- `generate_config_keys.py`: regenerates the config-key allowlist, checked by CI.
- `test_rank_quality.py`: tests for the campaign code. Runs in CI.

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
New candidate worktrees default to `<repo>/.worktrees/benchmark-candidates`.
Use one `--candidate-root` to select a different writable primary and repeat
`--candidate-search-root` to reuse exact, clean, registered worktrees from moved
locations. Search roots are checked in argument order and never receive new
worktrees or harness metadata; a selected existing worktree may have its ordinary
`build/` output refreshed to verify the requested toolchain identity.
See [the complete matrix example](../docs/BENCHMARK_EXPERIMENTS.md#reusable-ref-based-matrices).

For cross-build measurements while the host daemon remains active, select container
execution on the same experiment CLI. It creates an exact Git bundle, runs the same
experiment runner with no host bind mounts, and exports immutable results back to the
named history:

```sh
uv run python benchmarks/run_experiments.py \
  --matrix-spec /absolute/path/development-comparison.json \
  --experiment-root /durable/ignored/path/development-comparison \
  --container --cpus 4 --memory 8g --workers 4
```

CPU, memory, and worker budgets are required rather than guessed. Candidate builds
use the complete declared CPU budget by default (`--cpus 16` runs `make -j16`);
fractional CPU budgets round up to avoid leaving an available execution slot idle.
Use `--build-jobs N` only when build-memory pressure requires a smaller positive
override. The resolved value is part of the run identity and environment manifest.
The repository-relative `.worktrees/benchmark-candidates` default, build outputs, caches,
daemon state, and result generation remain on two labeled Docker volumes; the
coordinator prints their exact names and retains them for auditable resume. Rerun
the same source spec and experiment root to resume, or remove the printed volumes
after exported results are verified. The measured container is
always native `arm64` or `amd64`, resource bounded, and removed on success or failure.
Each invocation writes a content-addressed container-environment manifest, so changed
arguments create a new audit record instead of replacing history. Failed candidate
build logs are exported under `container-failures/<source-commit>/build-logs/`; if
that export itself fails, the error identifies the retained work volume and path.
Each measured source/spec/resource cohort is isolated under
`runsets/<content-id>/`; `--audit-only` resolves to the same cohort and cannot create
a second attempt. Canonical Git ref/commit content identifies the repository
snapshot even when equivalent bundle pack bytes differ; the exact bundle SHA-256
remains in the manifest. The run key also hashes the selected image's runtime
configuration and root-filesystem layers while ignoring BuildKit's nondeterministic
attestation index. A different snapshot, runtime image, spec, or resource budget cannot
be misreported as an unplanned cell in the current runset.
Container numbers are controlled Linux relative comparisons, not absolute macOS
latency. See [Container isolation](../docs/BENCHMARK_EXPERIMENTS.md#container-isolation).
Docker benchmarks default to Clang 18.1.3. The pinned image also provides GCC for
explicit portability or compiler-ablation cohorts; override both `CC` and `CXX`
together in `build_environment`. Run Clang and GCC as distinct named histories
with otherwise identical specs, and never infer cross-cohort comparability from a
shared OS image alone. Native execution retains its existing configurable compiler
selection.

To run the fixed nine-cell synthetic rank diagnostic through that same isolated
container path, generate its matrix from the canonical hypothesis registry and pass the
result to the coordinator:

```sh
uv run python benchmarks/campaign_specs.py \
  --quick-hypotheses \
  --out-dir /durable/ignored/path/rank-hypotheses-spec

uv run python benchmarks/run_experiments.py \
  --matrix-spec /durable/ignored/path/rank-hypotheses-spec/rank-hypotheses-quick-v2.json \
  --experiment-root /durable/ignored/path/rank-hypotheses \
  --container --cpus 4 --memory 6g --workers 4
```

The generated matrix contains exactly nine profiles, one MCP repetition, no real-corpus
dependency, and no suite-duration target. Its per-cell timeout is a subprocess safety
boundary recorded in the matrix, not an expected duration or suite cutoff.

`schema/` contains schemas for records emitted by current tooling.
`terminology.json` defines every normative fact, step, join, and formula identifier.
The generated human view remains in `docs/BENCHMARK_TERMINOLOGY.md`, and the full
workflow is documented in `docs/BENCHMARK_EXPERIMENTS.md`.

The upstream-owned `scripts/benchmark-index.sh`, `scripts/benchmark-search-graph.sh`,
and `scripts/clone-bench-repos.sh` retain their established locations. Branch-created
Python benchmark implementations live here without executable compatibility copies.
`docs/schema/benchmark-facts-v1.schema.json` retains its frozen URI because v1 bundles
embed that identifier.

---

# Rank-quality campaign

Measures **which persisted score should order search results**, on real corpora, against
the baseline the upstream maintainer proposed instead of PageRank.

## What question this answers

PageRank has been rejected upstream twice (PR #147, PR #151) with a specific, testable
objection: *"PageRank on a call graph would rank `log.Error()` and `fmt.Sprintf()` as the
most important functions in any codebase… just utility popularity. We already have
`min_degree`/`max_degree`… the same ranking signal without the conceptual mismatch or the
computation cost."*

So `ORDER BY degree DESC` is the **required baseline arm**, not "ranking off", and the
campaign is built to be able to conclude that the maintainer was right.

## Run it

Nothing below clones or writes outside the cache unless you ask it to.

```sh
# 1. One corpus, one cell. Resolves the pin, materializes it, runs the battery.
uv run python benchmarks/run_benchmark.py \
    --capability-quality rank \
    --corpus cosign \
    --rank-query-manifest benchmarks/rank-queries-v1/manifest.json \
    --clone-missing-real-repos \
    --out results/cosign.json

# 2. Already have a checkout? Point at it and skip the network entirely.
uv run python benchmarks/run_benchmark.py \
    --capability-quality rank --corpus cosign \
    --corpus-repo cosign=/path/to/cosign \
    --rank-query-manifest benchmarks/rank-queries-v1/manifest.json \
    --out results/cosign.json

# 3. Render the report, including the ranking score comparison table.
uv run python benchmarks/summarize_results.py \
    --input cosign=results/cosign.json --out results/report.md
```

A corpus is resolved in this order, and **nothing is ever cloned implicitly**:

1. `--corpus-repo <id>=<path>`
2. `CBM_BENCH_CORPUS_<ID>` environment variable
3. `~/.cache/codebase-memory-mcp/bench-repos/<id>`
4. clone the pinned commit — only with `--clone-missing-real-repos`

If none apply, the error names every path searched, so a typo cannot look like a network
problem.

**Defaults are inert.** Without `--corpus` and `--rank-query-manifest`, a rank run issues
the single built-in query exactly as before, so previously cached cells stay valid.

**A real corpus is indexed alone.** The synthetic `zz_order_core` fixture is *not* written
into a materialized corpus tree, because no real corpus battery queries those symbols — it
would dilute every corpus-scoped score without acting as a control. `--rank-fixture-overlay`
plants it deliberately as a positive control and appends its canary query so the control
actually runs. Either way `cases[0].fixture.corpus_overlay` records which happened.

## Run the whole campaign in Docker

The host daemon enforces an exact-build cohort, so a candidate that differs from an active
daemon is correctly rejected. The `--container` execution path sidesteps that without
weakening it: no host bind mounts, an exact Git bundle, and pinned corpora carried in.

```sh
# 1. Generate the four arms. --corpora runs a pilot first; drop it for the sweep.
uv run python benchmarks/campaign_specs.py \
    --out-dir /durable/ignored/path/campaign-specs --corpora flask

# 2. One arm per invocation. Corpora named in the spec are staged automatically.
uv run python benchmarks/run_experiments.py \
    --matrix-spec /durable/ignored/path/campaign-specs/rank-quality-v1.json \
    --experiment-root /durable/ignored/path/campaign \
    --container --cpus 6 --memory 6g --workers 4 \
    --clone-missing-real-repos

# 3. Roll every arm up into one verdict document.
uv run python benchmarks/rank_report.py \
    --experiment-root /durable/ignored/path/campaign \
    --out /durable/ignored/path/campaign/rollup.md \
    --json /durable/ignored/path/campaign/rollup.json
```

Each corpus is resolved and **pin-verified on the host** — where the failure is cheap and
visible — then copied into the retained work volume at a revision-keyed path and exposed
to the container as `CBM_BENCH_CORPUS_<ID>`. That is rung 2 of the ladder above, so the
container needs no new flag, no network, and no second resolver. The staged pins are part
of the container run key: re-pinning a corpus starts a new runset instead of resuming into
cells measured against a different tree.

`campaign_specs.py` emits four arms from one definition:

| Arm | `index_mode` | What it answers |
|---|---|---|
| `rank-quality-v1` | `full` | H1–H7. Full indexing keeps `FAST_SKIP_DIRS` out of the ranking comparison, so ordering is not confounded with coverage. Includes the cosign reply-detail frontier |
| `coverage-full-v1` | `full` | the reference arm for the silent-drop diff |
| `coverage-moderate-v1` | `moderate` | coverage lost outside full mode |
| `coverage-fast-v1` | `fast` | the mode 151 of upstream's 159 evaluation corpora use |

The three coverage arms are byte-identical apart from `index_mode` — a test asserts it,
because any other difference would be measured as coverage loss.

## Read the results

| Column / field | Means | Watch for |
|---|---|---|
| `Utility contamination` | fraction of the top-K that is a logging/allocation utility, matched on identifier tokens | **High for `degree` and low for `pagerank` contradicts the PR #151 objection.** The reverse confirms it |
| `Scaffolding` | fraction of the top-K that is test scaffolding, derived from the corpus's own test declaration | `null` means no independent labels were supplied — it is never inferred from this server's own predicates |
| `ρ vs degree`, `Jaccard` | agreement between a score and the degree baseline | **High values support the maintainer's "same ranking signal" claim.** Report them either way |
| `Rank views fresh` | whether `pagerank`/`linkrank`/`node_degree` were current | **`False` invalidates the ranking numbers in that row** — `search_graph` silently falls back to a degree sort when these are stale |
| `Status` = `N/A` | that score is absent from this database | Expected for `importance` on any binary without PR #879's pass |
| `zero_result_behavior` | zero-result rate across the battery | `search_code` was 8% of recovered real calls but ~70% of zero-result failures |
| `repetition_stability` | result counts across repeats of one query | `stable: false` means the same call against a fixed index returned different counts |

**Polarity warning.** On `jest` the product *is* test infrastructure, so a **low**
scaffolding score is a defect, not a win. The sign is reported per corpus.

## Adjust or extend it

**Add a corpus** — append to `corpora-v1.json` with a 40-char `revision` **and** `tree`
(both are verified), `stars >= 1000`, and which hypothesis it discriminates. Then add a
matching entry to `rank-queries-v1/manifest.json`: the two must agree, and
`manifest_corpus_disagreements()` fails the campaign if they drift.

**Add a query** — add it under a corpus in `rank-queries-v1/manifest.json`. Include
`judgments` to have it scored (`expected_substring`, optional `required_substrings`,
`relevance`), or omit them for a behavioral-only probe that measures result counts and
latency without claiming relevance. Set `repetitions` above 1 to measure count stability.
Author graded targets **grep-first** — find them by reading source, never through the
graph — per `docs/EVALUATION_PLAN.md` [CR-1]; record the evidence in `grep_first_evidence`.

**Add a scorer** — add SQL to `RANK_PROBE_SQL` in `run_benchmark.py` returning
`(qualified_name, file_path, score)`.

**Inspect labels** for a corpus — the harness derives these automatically, so this is
only for checking what a corpus declares:

```sh
uv run python benchmarks/generate_test_labels.py --corpus cosign
uv run python benchmarks/generate_config_keys.py          # after any config change
```

**Tune weights** — pass `--config edge_weight_tests=0.01`. Unknown keys are rejected at
parse time against `config-keys-v1.json`, because the product itself accepts any key and
would otherwise report a difference of exactly zero for a typo.

## Test it

```sh
uv run python benchmarks/test_rank_quality.py
```

25 tests, stdlib only, no binary or network. Runs in CI. Every test names the defect it
guards; most exist because that defect shipped and was found by audit rather than by a test.
