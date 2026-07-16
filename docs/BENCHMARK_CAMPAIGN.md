# Reproducible benchmark campaigns

`scripts/run-benchmark-campaign.py` runs a JSON plan sequentially and keeps every
attempt under a content-addressed cell directory. It is intended for release-build
comparisons where correctness and query-result quality are gates, not optional
context around a speed claim.

Use a durable ignored campaign root such as `.worktrees/benchmark-campaign`.
The runner rejects the operating-system temporary tree by default because a crash
or reboot can otherwise erase manifests, results, and logs. Do not track generated
results or the generated Markdown report in Git.

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
and current plan identity all agree.

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
      "harness_version": "benchmark-incremental-speed.py:<sha256>",
      "cwd": "/absolute/path/to/codebase-memory-mcp",
      "command": [
        "uv", "run", "python", "scripts/benchmark-incremental-speed.py",
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
a valid measurement that fails a quality or performance gate. The campaign runner
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
performance campaigns should set top-level
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

Each semantic pair case also supplies a content-addressed replacement source. A real
one-file mutation removes one judged positive and adds another, retaining pre/post
source hashes and changed paths. The harness records initial, incremental, and fresh
index measurements; pre/post confusion witnesses; freshness warnings; exact publish
kind; bounded pair equality; and whole canonical-graph equality. This prevents a
no-op reindex or a stale expected edge from being reported as successful changed-file
quality.

Capability ablations should use the named `--config-profile` values so an important
cost center cannot be silently omitted. Repeated `--config KEY=VALUE` arguments
remain available and take priority over the selected profile. The default profile
uses no overrides. The PageRank/LinkRank ablation is:

```text
--config-profile rank_disabled
```

The optional graph-pass ablation keeps dependency indexing enabled and is:

```text
--config-profile optional_graph_disabled
```

The immediate semantic/similarity freshness profile is:

```text
--config-profile incremental_semantic_freshness_eager
```

It changes only `incremental_derived_refresh=eager`. The default
`stale_on_incremental` policy may publish an exact or containment delta after
marking global `SIMILAR_TO`/`SEMANTICALLY_RELATED` views stale; graph queries must
then retain an explicit freshness warning until an eager or full rebuild. Reports
score this warning as policy conformance, not an unexplained execution failure, but
they keep immediate semantic task quality false. The eager profile must produce the
post-mutation judged pair set and edge scores identically to a fresh rebuild without
a stale warning. Compare both profiles when selecting a latency/freshness Pareto
point.

The lowest-cost indexing baseline also disables installed-package indexing and is:

```text
--config-profile minimal_indexing
```

`minimal_indexing` expands to `auto_index_deps=false`, `rank_enabled=false`,
`similarity_enabled=false`, `semantic_edges_enabled=false`,
`githistory_enabled=false`, and `httplinks_enabled=false`. Reports retain both the
profile name and the fully expanded `config_overrides` map for auditability.

Only apply gates a candidate revision actually supports. Record unsupported
combinations as compatibility findings rather than silently treating them as the
same configuration.

## Run and resume

```sh
uv run python scripts/run-benchmark-campaign.py \
  --plan .worktrees/benchmark-campaign/plan.json \
  --campaign-root .worktrees/benchmark-campaign/results
```

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
exposes it, 1/5/15-minute load average when available, and campaign-filesystem total,
used, and free bytes. The campaign-level environment snapshot retains the same host
data. These observations diagnose load or disk drift; they are not substitutes for
per-process peak RSS recorded by the benchmark itself.

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
Use `--minimum-free-gb` and `--stale-lock-hours` only when the recorded defaults are
inappropriate for the host.

## Cross-campaign composition

Use `scripts/summarize-benchmark-results.py --composition-spec SPEC --out REPORT`
to combine incremental correctness and capability-quality evidence into one
configuration row. A composition input may name an exact matrix spec or the immutable
expanded plan already archived in its durable campaign root. The generator validates
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
