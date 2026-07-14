# Reproducible benchmark campaigns

`scripts/run-benchmark-campaign.py` runs a JSON plan sequentially and keeps every
attempt under a content-addressed cell directory. It is intended for release-build
comparisons where correctness and query-result quality are gates, not optional
context around a speed claim.

Use an external campaign root such as `/tmp/cbm-campaign`. Do not put generated
results or the generated Markdown report in the repository.

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

Capability ablations use repeated `--config KEY=VALUE` command arguments. The
default configuration uses no overrides. The PageRank/LinkRank ablation is:

```text
--config rank_enabled=false
```

The full optional-indexing ablation is:

```text
--config rank_enabled=false
--config similarity_enabled=false
--config semantic_edges_enabled=false
--config githistory_enabled=false
--config httplinks_enabled=false
```

Only apply gates a candidate revision actually supports. Record unsupported
combinations as compatibility findings rather than silently treating them as the
same configuration.

## Run and resume

```sh
uv run python scripts/run-benchmark-campaign.py \
  --plan /tmp/cbm-campaign-plan.json \
  --campaign-root /tmp/cbm-campaign
```

Rerunning the same command resumes validated cells. The runner executes cells
sequentially by default so concurrent indexing does not distort latency or peak RSS.
Each cell retains immutable timestamped attempts with `command.json`, `stdout.log`,
`stderr.log`, `result.json`, and `attempt.json`. `complete.json` is written with an
atomic replace only after validation. Per-cell exclusive locks reject a live or
recent competing run; stale lock recovery is recorded instead of hidden.

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

Use `--audit-only` to scan and regenerate the report without running missing cells.
Use `--minimum-free-gb` and `--stale-lock-hours` only when the recorded defaults are
inappropriate for the host.
