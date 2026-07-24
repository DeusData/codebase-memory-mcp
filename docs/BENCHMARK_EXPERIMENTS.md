# Reproducible benchmark experiments

`benchmarks/run_experiments.py` runs a JSON plan sequentially and keeps every
attempt under a content-addressed cell directory. It is intended for release-build
comparisons where correctness and query-result quality are gates, not optional
context around a speed claim. New automation uses this entry point and
`--experiment-root`. When retained plan or command records name a retired benchmark
script path, the loader resolves that path to the canonical entry point; no executable
compatibility wrapper is installed. Legacy flag aliases, persisted JSON keys, and the
`.worktrees/benchmark-campaign/` location remain readable for retained runs.
Retained plans may also name the removed `optional_graph_disabled` profile; it resolves
to the same capability manifest as `minimal_indexing`, which is the only one new
automatic plans emit.

Use a durable ignored experiment root. Automatic runs continue to use
`.worktrees/benchmark-campaign/` so existing retained runsets resume in place.
The runner rejects the operating-system temporary tree by default because a crash
or reboot can otherwise erase manifests, results, and logs. Do not track generated
results or the generated Markdown report in Git.

## Repository layout

Branch-created benchmark code, active schemas, terminology, configuration data, and
fixtures live under `benchmarks/`. The three benchmark shell scripts inherited from
upstream remain at their established `scripts/` paths. Human-facing guides remain
under `docs/`, tests under `tests/`, and the generated profiling header under `src/`
because those files belong to their respective integration surfaces.

New commands and source anchors use `benchmarks/`. The frozen
`docs/schema/benchmark-facts-v1.schema.json` remains at its original URI because
retained v1 bundles embed that exact identifier. The loaders accept the former v2
schema URI and recorded terminology hash, while new bundles emit only the canonical
`benchmarks/schema/facts-v2.schema.json` URI. The experiment runner resolves the
retired single-run script path in retained plans without shipping a duplicate
entry point.

## Cell identity

A cell ID is the first 24 hexadecimal characters of the SHA-256 of these canonical
JSON fields:

- full revision and binary SHA-256;
- build metadata, including the compiler and optimization flags;
- capability configuration;
- transport, scenario, repetition, and harness version;
- command, working directory, environment overrides, timeout, and accepted exit codes.

Changing any of those inputs creates a different cell. A completed cell is resumed
only when its completion marker, retained result, result SHA-256, binary SHA-256,
current plan identity, and every archived artifact path, size, and SHA-256 agree.

## Canonical fact tables

Every new benchmark result also writes a schema-valid `facts.json` bundle, normalized
`runs.json`, `steps.jsonl`, `results.json`, and `artifacts.json` tables, and a hashed
`manifest.json` under the experiment attempt's artifact directory. Standalone runs use
`--facts-dir DIR`; when only `--out result.json` is given, facts default to
`result.facts/`. New bundles use `benchmarks/schema/facts-v2.schema.json`;
the retained v1 schema remains available for earlier runsets. The
canonical benchmark vocabulary is `benchmarks/terminology.json`; its generated
human-readable view is [BENCHMARK_TERMINOLOGY.md](BENCHMARK_TERMINOLOGY.md).
`uv run python benchmarks/run_benchmark.py --describe-terms
json|markdown` prints either view without requiring a benchmark binary.

The run row records the experiment cell ID and label, exact candidate commit,
repetition, binary path/hash/size, build metadata, harness hash, host metadata,
capability arguments, workload scope, and per-layer cache knowledge. Step rows keep
each occurrence separate and distinguish elapsed work from CPU, queue, worker,
dependency, and monotonic-boundary fields. A field absent from the historical
measurement is an explicit `{"status":"unknown","reason":"..."}` value; it is
never reconstructed from a preset name or treated as suitable for a parity join.
Every fact bundle records the terminology version, canonical-content SHA-256, and
benchmark-generator SHA-256. A retained bundle therefore identifies the exact
definitions and normalizer that gave each field and step ID its meaning.
Experiment cells supply the candidate commit and build metadata automatically.
Standalone runs must pass `--candidate-revision FULL_COMMIT` and
`--build-metadata-json '{...}'` to make those fields authoritative; otherwise the
current checkout HEAD is retained separately as measurement context and the binary's
source revision/build flags remain `unknown`.

When every completed experiment cell has a canonical fact bundle, report generation
also writes `*.comparisons.json` and `*.fact-appendix.md`. The JSON conforms to
`benchmarks/schema/comparisons-v1.schema.json` and retains the source bundle,
run, and occurrence IDs behind every derived row. It classifies each cell pair as:

- `parity_comparison`: identical mode, complete effective capabilities, scope,
  cache state, host, benchmark contract, and correctness contract, with no unknown
  required value. Only this class may report a cross-implementation elapsed-time
  ratio.
- `capability_delta_comparison`: identical mode, scope, cache state, host, benchmark
  contract, and correctness contract, but explicitly different complete capability
  manifests. It reports the differences and no speed ratio.
- `not_eligible`: a required equality failed or required evidence is incomplete or
  unknown. The record states each rejection reason.

The emitted `join_id` and `formula_id` values have normative definitions in the
terminology registry. Lifecycle tables select recorded outer lifecycle occurrences;
they never construct wall time by summing child spans that may overlap or execute on
different threads or processes. Reports for retained runs that predate fact bundles
still render, but state that fact-derived comparisons are unavailable.

Retained reports from earlier harness versions remain usable:

```bash
uv run python benchmarks/run_benchmark.py \
  --import-report path/to/result.json \
  --facts-dir path/to/recovered-facts
```

The importer recovers binary identity, recorded configuration, elapsed phases,
peak RSS, outcomes, and retained-log hashes when present. It preserves missing
revision, build, cache, CPU, timestamp, worker, and concurrency evidence as
`unknown`, so generated comparisons can state the historical limitation rather
than silently inventing parity.

### Fact vocabulary and timing rules

These terms are normative in the runner, schema, JSON tables, and generated reports:

| Term | Meaning |
|---|---|
| Experiment | One declared comparison design: its candidates, capabilities, workloads, transports, repetitions, and execution order. |
| Runset | The immutable experiment specification identified by the first 12 hexadecimal characters of its canonical JSON SHA-256. Reusing identical semantic inputs resumes the same runset. |
| Cell | One fully resolved point in the experiment matrix, including one candidate revision, binary, build, capability map, workload, transport, and repetition. |
| Attempt | One process execution of a cell. Failed or interrupted attempts remain evidence; only a validated attempt creates `complete.json`. |
| Lifecycle | The user-observable sequence represented by one `run_id`, from process invocation through the benchmark gate. Component steps may overlap within it. |
| Run row | Identity and conditions shared by every observation in one lifecycle: implementation, harness, host, capability, scope, and cache facts. |
| Step row | One measured operation occurrence. `step_id` names the operation class; `occurrence_id` identifies this occurrence, so repeated operations are never merged. |
| Parent occurrence | A containment relation in the recorded operation hierarchy. It does not prove that parent and child executed serially. |
| Dependency occurrence | A measured predecessor that must finish before the step can proceed. An empty list means no dependency edge was recorded, not that the step was independent. |
| `elapsed_ms` | Wall-clock duration of that occurrence. Overlapping parent, child, or sibling durations must not be summed. |
| `cpu_ms` | CPU time consumed by the named `cpu_scope`. It remains `unknown` when the profiler recorded only wall time. |
| `queue_wait_ms` | Time after the operation became runnable but before its worker began executing. It remains `unknown` without scheduler instrumentation. |
| Critical path | The dependency-chain duration that determines lifecycle wall time. It remains `unknown` unless timestamped dependency events make the chain recoverable. |
| Result row | A correctness, quality, or instrumentation outcome. Product-contract failures and harness failures use different `kind` values. |
| Artifact row | A retained file identified by path, byte count when known, and SHA-256. |
| Unknown fact | `{"status":"unknown","reason":"..."}`: the source did not record the value. Unknown values prohibit capability-parity joins and arithmetic. |

`timing_components_ms` values parsed from existing worker profile markers become
separate step occurrences. The current markers provide elapsed wall time and
containment but not start/end timestamps, worker IDs, CPU time, queue wait, or a
dependency event graph; those fields therefore remain explicitly `unknown`.
This preserves parallel implementations without pretending their overlapping work
was serial. Future low-overhead instrumentation can populate the same fields without
changing the fact-table contract.

## Plan format

```json
{
  "schema_version": 1,
  "cells": [
    {
      "label": "final-defaults",
      "revision": "0123456789abcdef0123456789abcdef01234567",
      "binary_sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
      "build": {
        "target": "make -f Makefile.cbm cbm",
        "compiler": "Apple clang 17.0.0",
        "cflags": "-O2 -DCBM_BIND_TS_ALLOCATOR=1"
      },
      "capabilities": {},
      "transport": "mcp",
      "scenario": "self_dogfood",
      "repetition": 1,
      "harness_version": "run_benchmark.py:<sha256>",
      "cwd": "/absolute/path/to/codebase-memory-mcp",
      "command": [
        "uv", "run", "python", "benchmarks/run_benchmark.py",
        "--binary", "/absolute/path/to/release-binary",
        "--self-dogfood", "--repo-root", "/absolute/path/to/codebase-memory-mcp",
        "--transport", "mcp", "--out", "{result_path}"
      ],
      "accepted_exit_codes": [0, 1],
      "timeout_seconds": 3600
    }
  ]
}
```

Exit code `1` is explicit in this example because the benchmark harness uses it for
a valid measurement that fails a quality or performance gate. The experiment runner
still requires a parseable result whose `binary_metadata.sha256` matches the plan.
Crashes, timeouts, other exit codes, missing results, and mismatched binaries remain
failed attempts and never receive `complete.json`.

For compact `--matrix-spec` grids, each scenario requires `frontier_files` and
`exact_caps` arrays. Use a positive integer cap for an explicit cap sweep. Use
`null` to preserve each candidate's configured/default
`incremental_exact_max_affected_paths`; the generated cell is labelled
`capdefault` and does not inject a config override.

Set top-level `"accepted_exit_codes": [0, 1]` when the matrix benchmark uses exit
code 1 for a completed measurement that missed a correctness or quality gate. The
expanded cells retain that policy in their identities. Result parsing, binary-hash
validation, and the structured `error` check still prevent crashes or harness
errors from becoming completed evidence.

Legacy compact specs retain their original grouped cell order and plan hashes. New
performance experiments should set top-level
`"execution_order": "paired_interleaved"`. That opt-in order executes every
candidate/profile cell for repetition 1 before repetition 2, and records the
repetition block plus absolute execution position in each cell identity. This reduces
alignment between one configuration and slow host drift while keeping heavy cells
strictly sequential. It is deterministic rather than randomly shuffled, so the plan
is exactly reproducible; reports must still retain raw order and variation.

For isolated capability fixtures, set top-level `"capability_quality"` to `"rank"`,
`"dependencies"`, `"similarity"`, or `"semantic_edges"` and omit `scenarios`.
The runner expands candidate, profile, transport, and repetition axes without adding
incremental frontier arguments. Each command records the capability fixture and uses
`--include-logs`, while named config profiles provide matched enabled/disabled
ablations. Set top-level `"index_mode": "moderate"` or `"full"` for `similarity`
and `semantic_edges`; FAST mode intentionally does not generate either relationship.

The semantic pair task set is content-addressed from its version, source hashes,
relationship, score property, and explicit positive/negative pair judgments.
`SIMILAR_TO` structural clones and `SEMANTICALLY_RELATED` control-flow variants are
separate cases because the semantic pass intentionally excludes pairs already above
the structural MinHash threshold. Pair reports retain TP/FP/FN/TN and witnesses,
precision, recall, F1, false-positive rate, per-category counts, raw query rows,
latency, bytes, and estimated tokens. Natural-repository pairs outside the explicit
judgment set are retained as `unjudged`; incomplete natural ground truth never turns
an unknown result into a false positive.

For full-index, real-edit incremental, fresh-rebuild, query, response-size, and peak-RSS
measurements on one pinned repository, use `"workload": "self_dogfood"` with an exact
repository identity:

```json
{
  "workload": "self_dogfood",
  "repository_background": {
    "repo": "/absolute/path/to/source-checkout",
    "revision": "0123456789abcdef0123456789abcdef01234567",
    "tree": "89abcdef0123456789abcdef0123456789abcdef"
  },
  "scenarios": [{"name": "route_handler"}]
}
```

Each cell creates a detached worktree from the declared commit rather than mutable
`HEAD`. The plan identity retains the repository revision and tree, and result
validation rejects either mismatch. Use a scenario with an actual source edit when
making incremental-index claims; `noop` measures invocation overhead only.

Older candidates may not expose configuration flags added by a newer branch. A profile
can therefore declare `"candidate_labels": ["latest"]` to restrict an ablation to
candidates that accept it. Keep an unrestricted default profile for every candidate,
and record fixed-default or unsupported capabilities in `capability_support`; do not
pass an unknown flag to an old binary or pretend that its default is an ablation.
The harness likewise leaves `rank_refresh` untouched by default, records
`"rank_refresh": "candidate_default"` and
`"rank_refresh_override_applied": false`, and therefore measures each candidate's
real compiled/configured policy. Use `--rank-refresh at_publish`,
`defer_exact_delta_reindexes`, or `defer_all_incremental_reindexes` only for an
explicit policy experiment. The harness reads the versioned spelling map and
translates these values only when running a retained candidate that predates the
canonical names.

Each semantic pair case also supplies a content-addressed replacement source. A real
one-file mutation removes one judged positive and adds another, retaining pre/post
source hashes and changed paths. The harness records initial, incremental, and fresh
index measurements; pre/post confusion witnesses; freshness warnings; exact publish
kind; bounded pair equality; and whole canonical-graph equality. This prevents a
no-op reindex or a stale expected edge from being reported as successful changed-file
quality.

For a realistic background, add this top-level compact-spec object:

```json
{
  "quality_background": {
    "repo": "/absolute/path/to/source-checkout",
    "revision": "0123456789abcdef0123456789abcdef01234567",
    "tree": "89abcdef0123456789abcdef0123456789abcdef"
  }
}
```

This is supported by `similarity` and `semantic_edges` quality cases. The harness
streams tracked files from that exact commit through `git archive`, excluding the
source checkout's dirty and untracked state, then overlays the versioned canaries in
the isolated per-cell repository. It removes its transient tar archive after safe
extraction. The cell identity binds the resolved source path, commit, and tree;
result acceptance rejects a missing or mismatched retained commit/tree identity.
Neither the source checkout nor its worktree registry is modified.

Capability ablations should use the named `--config-profile` values so an important
cost center cannot be silently omitted. Repeated `--config KEY=VALUE` arguments
remain available and take priority over the selected profile. The benchmark default,
`automatic_dependency_source_indexing_disabled`, explicitly pins the current product
capability values and sets `auto_index_deps=false`. Use
`automatic_dependency_source_indexing_enabled` for the same capability set with
`auto_index_deps=true`. `candidate_native_configuration` applies no overrides and is
reserved for older candidates that do not implement the current configuration keys;
its unspecified effective values cannot participate in capability-parity joins. The
PageRank/LinkRank ablation is:

```text
--config-profile rank_disabled
```

`benchmarks/autotune.py` is a safe frontend for the corresponding PageRank parameter
sweep. It requires exact build metadata, generates a content-addressed rank-quality
experiment, interleaves candidate-default and ablation repetitions, and stores the
plan, results, logs, and report under a durable ignored result root. It does not
change the normal user configuration or cache. Use `--plan-only` to validate and
inspect the expanded cells before spending CPU time.

The independent `--mcp-surface-parity` mode records classic, streamlined before
reveal, and the same streamlined process after reveal. It compares names plus the
full `tools/list` client contract (description, input/output schemas, and MCP
annotations), reports user outcomes before tool counts, checks bounded pre-reveal
handler recognition, and requires server processes and reader threads to be reaped.
These probes establish discovery and dispatch parity; functional quality claims
must still come from the capability fixtures and repository workloads below.

The optional graph-pass ablation retains the benchmark default
`auto_index_deps=false` and is:

```text
--config-profile optional_graph_disabled
```

The immediate semantic/similarity freshness profile is:

```text
--config-profile incremental_derived_results_refresh_at_publish
```

It changes only `incremental_derived_results_refresh=at_publish`. The default
`defer_all_incremental_reindexes` policy may publish an exact or containment delta
after marking global `SIMILAR_TO`/`SEMANTICALLY_RELATED` views stale; graph queries
must then retain an explicit freshness warning until an at-publish or full rebuild.
Reports score this warning as policy conformance, not an unexplained execution
failure, but they keep immediate semantic task quality false. The at-publish profile
must produce the post-mutation judged pair set and edge scores identically to a fresh
rebuild without a stale warning. Compare both profiles when selecting a
latency/freshness Pareto point.

Cross-version candidate-default cells do not assume that older binaries share the
latest binary's derived-refresh default. With no explicit
`incremental_derived_results_refresh` override, the retained policy is
`candidate_default` and the harness classifies observed behavior as immediate pair
freshness, deferred with a structured warning, or unreported stale output. Explicit
at-publish/deferred profiles continue to validate against the requested policy. This
keeps an older immediate-refresh default from being judged against a newer deferred
default.

Large mutation reports keep Core graph and Full graph freshness separate. A
`PASS: DECLARED STALE VIEWS` decision requires structured `stale_with_warning`
metadata and a second canonical comparison that excludes only the declared
`SEMANTICALLY_RELATED` rows. Every remaining node, edge, property, and file hash
must still equal the matching fresh rebuild. An undeclared difference—or any
non-semantic difference—remains a core correctness failure. Full graph freshness
stays zero until the unfiltered graphs match, so the latency/freshness tradeoff is
visible rather than relabeled as full equality.

The lowest-cost indexing baseline also disables installed-package indexing and is:

```text
--config-profile minimal_indexing
```

`minimal_indexing` expands to `auto_index_deps=false`, `rank_enabled=false`,
`similarity_enabled=false`, `semantic_edges_enabled=false`,
`githistory_enabled=false`, and `httplinks_enabled=false`. Reports retain both the
profile name and the fully expanded requested/effective override maps for
auditability. Each benchmark case removes inherited `CBM_*` product variables,
uses an isolated cache, and records that worker selection follows the candidate's
native default with `CBM_WORKERS` unset. Candidate-native profiles record effective
configuration as unknown instead of inferring defaults that an older binary did not
report.

Only apply gates a candidate revision actually supports. Record unsupported
combinations as compatibility findings rather than silently treating them as the
same configuration.

## Run and resume

```sh
uv run python benchmarks/run_experiments.py \
  --plan .worktrees/benchmark-campaign/plan.json \
  --experiment-root .worktrees/benchmark-campaign/results
```

The retained `--campaign-root` spelling remains accepted. Archived plans that name
the former single-run script path resolve it to `benchmarks/run_benchmark.py` at
execution time without rewriting the archived plan or changing its cell identity.

Rerunning the same command resumes validated cells. The runner executes cells
sequentially by default so concurrent indexing does not distort latency or peak RSS.
Each cell retains immutable timestamped attempts with `command.json`, `stdout.log`,
`stderr.log`, `result.json`, and `attempt.json`. `complete.json` is written with an
atomic replace only after validation. Per-cell exclusive locks reject a live or
recent competing run; stale lock recovery is recorded instead of hidden.
Each benchmark command runs in an isolated process group. Timeout or user interrupt
signals the whole group, waits up to 30 seconds for the harness to remove its cache
and detached worktrees, then force-stops any remaining descendants. The immutable
attempt record is written before an interrupt is re-raised.

`command.json` records a pre-run resource snapshot and `attempt.json` records a
post-run snapshot: UTC time, hostname, CPU count, physical memory when the platform
exposes it, 1/5/15-minute load average when available, and experiment-root
filesystem total, used, and free bytes. The experiment-level environment snapshot
retains the same host data. These observations diagnose load or disk drift; they
are not substitutes for per-process peak RSS recorded by the benchmark itself.

Every invocation also writes:

- an immutable copy of the plan keyed by its SHA-256;
- a timestamped environment snapshot and manifest;
- counts for planned, complete, missing, corrupt, duplicate-attempt, and unplanned
  run directories;
- `reports/summary.md`, regenerated from validated completion records only.

The report lists exact bytes from each tool's default response encoding and a clearly
labeled deterministic `ceil(UTF-8 bytes / 4)` token estimate. Each quality oracle makes
a second request with `format=json`; its latency and canonical JSON size are recorded
separately as `quality_probe_elapsed_ms` and `quality_response_bytes`, so parsing the
oracle cannot silently replace or inflate the default user-facing measurement. Pareto
membership is restricted to candidates that pass every applicable quality/correctness
gate and have query latency, response tokens, incremental latency, and peak RSS
measurements. It maximizes quality while minimizing those cost axes. Exact bytes remain
visible so the token estimate is never presented as tokenizer ground truth.
Peak RSS and internal indexing time are extracted in one streaming pass from the
worker logfile named by the index response. The harness sets `CBM_PROFILE=1` for
every candidate because successful supervisors otherwise delete that logfile; this
also makes the profiling configuration consistent and visible across revisions.
Only the exact `mem.phase`,
`pipeline.done`, and `incremental.done` marker lines (at most 512) are retained in
the result, keeping memory bounded while preserving the evidence after transient
worker logs are cleaned.

Use `--audit-only` to scan and regenerate the report without running missing cells.
The audit re-inventories every completed attempt's artifact directory and rejects
changed, missing, or unlisted worker logs rather than trusting `attempt.json` alone.
Use `--minimum-free-gb` and `--stale-lock-hours` only when the recorded defaults are
inappropriate for the host.

`--quick` and `--full` build their candidate set from `DEFAULT_CANDIDATE_REFS`,
which pins one baseline (`upstream/main`) and two dated tags. The baseline falls
back from `upstream/main` to `origin/main` to `main` automatically if the pinned
ref does not resolve (for example, after the `upstream` remote is removed
post-merge). Use repeatable `--candidate-ref LABEL=REF` (for example
`--candidate-ref upstream-main=origin/main`) to override any default candidate's
ref explicitly; an explicit override is fail-closed like the rest of the runner —
an unresolvable override ref raises rather than silently substituting a different
comparison point.

## Cross-experiment composition

Use `benchmarks/summarize_results.py --composition-spec SPEC --out REPORT`
to combine incremental correctness and capability-quality evidence into one
configuration row. A composition input may name an exact matrix spec or the immutable
expanded plan already archived in its durable experiment root. The generator validates
the selected plan, requires every selected cell to have a hash-validated completion,
and consumes the derived report inputs without altering immutable raw results. Using
an archived plan permits historical report regeneration without requiring the old
candidate executable path to still exist.

The generated Markdown records the composition-spec SHA-256. A sibling
`REPORT.manifest.json` records every materialized input path and SHA-256, making the
uncommitted report reproducible and auditable without committing experiment logs or
results.

Reports show observation counts, medians, and min–max ranges for incremental, query,
and full-index latency. The ranges are descriptive, not confidence intervals: the
default sequential grouped order avoids concurrent contention but is not a paired or
randomized design suitable for an effect-size interval.
