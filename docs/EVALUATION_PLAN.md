# Evaluation plan

This document defines the release gate for the agents-first preview. It is a
protocol, not a result report. Numbers may be published only from a versioned
artifact produced by this protocol.

## Pinned evaluation set

Use 36 real maintenance tasks: one task in each of the six categories below for
each core language family (Go, Python, TypeScript/JavaScript, Java, C/C++, Rust).

1. Explain an unfamiliar subsystem.
2. Locate the smallest safe change.
3. Find the root cause of a defect.
4. Trace change impact and affected tests.
5. Review a patch for regressions.
6. Gather security-relevant context.

Every task manifest must record:

- repository URL and immutable commit;
- dirty-worktree patch, if any;
- expected evidence and acceptable target symbols;
- exact prompts, tool configuration, model and client versions;
- hardware, operating system, compiler, parser and SQLite versions.

The manifest and raw task artifacts are immutable inputs to scoring.

## A/B conditions

Run every task under both conditions in randomized order:

- baseline: normal file/search tools with full-file reads;
- treatment: the default `codebase-memory-mcp` toolset and `get_context`.

Do not grant the treatment condition additional editing, execution, or oracle
tools. Preserve complete MCP requests, responses, progress events, client
transcripts and token accounting. A grader must be blind to the condition.

## Correctness gates

On annotated core-language fixtures:

- symbol search Hit@5 is at least 95%;
- `CALLS` precision is at least 90% and recall at least 85%;
- snippet boundaries are correct in at least 99% of cases;
- every MCP response validates against its declared `outputSchema`;
- the same query and generation produce the same ordered response.

Ten full builds with worker counts 1, 2, 4 and automatic must have the same
normalized structural digest. Derived heuristic edges are scored separately and
must identify their derived generation.

## Incremental gates

Use a pinned medium fixture and reference hardware. Run one warmup followed by
at least five measured repetitions. Report median, p95 and MAD for parse,
resolve, persist and derived phases, plus peak RSS and database size.

- unchanged dirty worktree for 30 minutes: no repeated indexing and idle CPU
  below 1%;
- one-file update: no more than 10% of full-rebuild wall time;
- one-file update peak RSS: no more than 25% of full-rebuild peak RSS;
- incremental structural result equals a clean full rebuild;
- cover add, delete, rename, 1/10/100 changed files, and same-size/same-mtime
  content changes.

Crash and fault injection at every publication stage must leave the previous
successful index readable and marked stale rather than corrupt.

## Agent-value gates

Across all 36 tasks:

- task success is no more than two percentage points below the baseline;
- input-token consumption is at least 40% lower;
- retrieval tool calls are at least 30% lower.

Report confidence intervals and per-task outcomes, not only aggregate averages.
Failed, timed-out and truncated runs remain in the artifact.

## Artifact layout

Each published run uses:

```text
benchmarks/runs/<run-id>/
  manifest.json
  environment.json
  fixtures.json
  raw/
  traces/
  metrics.json
  report.md
  checksums.txt
```

`manifest.json` identifies the codebase-memory commit and normalized graph
digest. `checksums.txt` covers every artifact. A release claim must link to the
exact run directory that supports it.
