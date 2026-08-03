# Codebase Memory MCP dogfood regressions — 2026-08-03T15:57:48Z

## Capture metadata

- Capture UTC: `2026-08-03T15:57:48Z`
- Repository: `/Users/athundt/.claude/codebase-memory-mcp/.worktrees/api-consolidation-merge`
- Branch: `api-consolidation-merge`
- HEAD before the experiment-harness commit: `af6a9e60f34034349c7b8fe99b440b8ca5fd0617`
- Indexed MCP project supplied to calls: `Users-athundt-.claude-codebase-memory-mcp`
- Host: macOS 26.5 build 25F71, arm64
- Tool-reported installed version: `codebase-memory-mcp dev`
- MCP process executable observed with `pgrep -fl codebase-memory-mcp` and `ps`: `/Users/athundt/.local/bin/codebase-memory-mcp`
- MCP-process binary SHA-256: `6e7e7dab434d7fcc215a82575450b32fbac117aa21a8270337cc210bf1f70edb`
- MCP-process binary filesystem birth/modify time: `2026-07-29T21:49:13-0400`; size `295160944` bytes; Mach-O arm64
- Shared daemon PID/start at capture: PID `1888`, started `2026-07-30T14:54:35-0400`, command `/Users/athundt/.local/bin/codebase-memory-mcp --cbm-daemon-internal`
- Session-process caveat: 17 additional long-lived `/Users/athundt/.local/bin/codebase-memory-mcp` processes were present. The MCP protocol did not expose which PID owned this conversation, so a more specific PID claim would be fabricated.
- Shell lookup mismatch: `which codebase-memory-mcp` resolved `/usr/local/bin/codebase-memory-mcp`, a different binary with SHA-256 `c554be7ca02834d2b1d1d942232585df832da445bf3bbd8034ff2e84ccfe581f`, birth/modify time `2026-07-26T18:09:51-0400`, and size `295125264` bytes. Reproduction commands must therefore name the MCP process executable explicitly when binary identity matters.
- Target-worktree build (not the MCP process binary): `build/c/codebase-memory-mcp`, SHA-256 `8ef6a23421cbb3b099dbd48839a1132584d443f5f3cefa2643a4575b91421d49`, birth `2026-08-02T04:45:05-0400`, modify `2026-08-02T04:46:21-0400`, size `295252000` bytes.
- Graph-tool operating tier: Tier 2 verification.
- Source fallback used after MCP failure: direct numbered reads and `rg` over the target worktree.

The filesystem birth time is the closest available local proxy for installation time. It does not prove when a package-manager transaction started or completed; no package-manager receipt was exposed by the MCP protocol.

## D-001 — `get_code` returns source outside the requested symbol

### Context

During the ranking-harness audit, `search_graph` found `score_ranked_relevance` in `benchmarks/run_benchmark.py`. A subsequent `get_code` call named that returned qualified symbol and reported the function's line metadata, but the source payload began in unrelated preceding SQL/code rather than at the requested function definition.

### Reproduction

1. Start the MCP server with `/Users/athundt/.local/bin/codebase-memory-mcp` at the binary identity above.
2. Call `search_graph` for `score_ranked_relevance` in project `Users-athundt-.claude-codebase-memory-mcp`.
3. Copy the exact returned `qualified_name` into `get_code`, with `mode="full"` and a sufficient `max_lines` value.
4. Compare the returned first source line and body with the definition found by a numbered direct read of `benchmarks/run_benchmark.py`.

### Expected

The source payload starts at the requested definition and the source body agrees with its reported path and line metadata.

### Observed

The metadata identified `score_ranked_relevance`, but the payload included unrelated preceding SQL/code. This is dangerous for an auditor because a plausible path/line wrapper can make unrelated source look authoritative.

### Workaround and likely investigation seam

- Verify every material `get_code` payload against a direct numbered source read until fixed.
- Check whether stored symbol spans are stale after watched-worktree changes, whether Python decorator/definition ranges are offset, and whether `get_code` slices from the previous symbol's start while retaining the selected symbol's metadata.
- Add a protocol-level regression test asserting that the first non-comment token in a returned full function body belongs to the requested definition and that the returned range encloses the graph symbol's declared start/end.

## D-005 — concurrent graph calls permanently close the session transport

### Context

Ten independent symbol lookups were issued concurrently to reduce audit latency. All ten returned `Transport closed`. Every later single graph call in the same conversation also returned `Transport closed`, including the fresh Tier-2 verification call at `2026-08-03T15:57:48Z`.

### Initial reproducer

Issue concurrent `search_graph` calls for these names against the same MCP session and project:

1. `rank_cases`
2. `validation_gate`
3. `build_rollup`
4. `build_rank_spec`
5. `build_all_specs`
6. `score_quality_oracles`
7. `main`
8. `profiles`
9. `build_matrix_spec`
10. `run_rank_score_probes`

### Persistence reproducer

After the concurrent failure, issue this single call in the same session:

```text
search_graph(
  project="Users-athundt-.claude-codebase-memory-mcp",
  name_pattern="^(build_matrix_spec|run_rank_score_probes|build_rollup|build_automatic_spec)$",
  limit=20,
  format="json"
)
```

Observed response at `2026-08-03T15:57:48Z`:

```text
tool call error: tool call failed for `codebase-memory-mcp/search_graph`

Caused by:
    Transport closed
```

Independent serial confirmation at `2026-08-03T16:07:08Z`: after completing direct
source edits and issuing no intervening MCP requests, one `search_graph` call requested
`build_quick_hypothesis_spec|run_rank_score_probes|expand_matrix_spec|build_receipt` in
`benchmarks/*.py`, with `include_connected=true`, `limit=30`, and JSON output. It failed
immediately with the same `Transport closed` error. This rules out an individual symbol
ambiguity and confirms that normal filesystem activity does not recover the connection.

### Expected

Concurrent requests are either serviced, queued with bounded backpressure, or rejected individually with a structured retryable error. A failed request must not permanently invalidate unrelated later requests.

### Observed impact

- Graph discovery, `get_code`, traces, and `check_index_coverage` became unavailable for the rest of the conversation.
- Tier-2 coverage verification could not be completed through MCP.
- The audit had to use direct source reads, increasing latency and removing graph freshness/coverage metadata from later claims.

### Workaround and root-cause guidance

- Until fixed, serialize graph calls per MCP session and restart the client/session after the first `Transport closed` response.
- Reproduce under ASan/UBSan and with MCP framing logs. Inspect request-lifetime ownership, writer serialization, cancellation propagation, and whether one worker closes shared stdin/stdout or the common transport after a per-request failure.
- Add a test that submits more simultaneous requests than the worker count, verifies every response has a matching request ID, then submits a final single request on the same connection.
- Return explicit overload/backpressure metadata rather than closing the transport.

## Verification limitation

`check_index_coverage` was required for the operated Python paths but could not be called after D-005. Direct source reads covered:

- `benchmarks/autotune.py`
- `benchmarks/campaign_specs.py`
- `benchmarks/rank_hypotheses.py`
- `benchmarks/rank_report.py`
- `benchmarks/run_benchmark.py`
- `benchmarks/run_evidence_suite.py`
- `benchmarks/run_experiments.py`
- `benchmarks/test_rank_quality.py`
- `tests/test_autotune.py`
- `tests/test_rank_hypotheses.py`

This fallback verifies the edited text and tests, but it does not establish that the graph index has no skipped, partial, stale, or excluded ranges.

## D-006 — local candidate benchmarks fan out identical cohort failures

### Capture and affected identities

- First observed UTC: `2026-08-03T16:10:28Z`; fail-fast verification UTC:
  `2026-08-03T16:14:46Z`.
- Active account build: `6e7e7dab434d7fcc215a82575450b32fbac117aa21a8270337cc210bf1f70edb`.
- Requested worktree build: `8ef6a23421cbb3b099dbd48839a1132584d443f5f3cefa2643a4575b91421d49`.
- Original preserved run root:
  `.worktrees/benchmark-experiments/autotune-quick-integration/`.
- Fail-fast preserved run root:
  `.worktrees/benchmark-experiments/autotune-quick-preflight/`.

### Reproduction

1. Keep any MCP-backed AI session open so the active account daemon/build cohort remains
   leased.
2. Build a different checkout with `make -f Makefile.cbm cbm`.
3. Run `uv run python benchmarks/autotune.py --quick` against that worktree binary,
   supplying the required `--build-target`, `--compiler`, and `--cflags` provenance.
4. Inspect each attempt's `result.json` and `stderr.log` beneath the run root.
5. From the same checkout, run `build/c/codebase-memory-mcp daemon status`.

The original harness started all nine fixed profiles. The baseline MCP process exited
during `initialize`; every override arm then failed its first `config set`. Their common
stderr was:

```text
CBM could not start because a conflicting CBM process is active (build; active version
dev, build 6e7e7dab...; requested version dev, build 8ef6a234...). Close all CBM sessions
and commands, then retry.
```

Despite that live cohort conflict, the requested-build `daemon status` command returned
`daemon: not running`. This status is technically local to what the requested build can
connect to, but it is misleading operational guidance because its next stateful command
is rejected by the active account cohort.

### Product policy versus harness defect

The single-cohort policy itself is explicit:
`src/daemon/service.c:cbm_daemon_rendezvous_key:167-184` uses one product-domain key;
`src/daemon/service.c:cbm_daemon_hello_compare:187-232` rejects a different build
fingerprint; and
`src/daemon/bootstrap.h:cbm_daemon_bootstrap_endpoint_new:44-47` documents one stable
per-account endpoint. Meanwhile, `benchmarks/run_benchmark.py:build_env:5784-5803`
correctly assigns each case an isolated `CBM_CACHE_DIR`. These requirements mean an
in-process local benchmark cannot test a different candidate while the user's MCP
sessions remain active; cache isolation does not create a separate daemon cohort.

The harness defect was allowing one environment incompatibility to fan out into nine
identical failed attempts and dropping candidate stderr from the baseline MCP error. The
local fix now:

- reuses the first scientific cell as a content-addressed fail-fast preflight;
- starts zero remaining cells after failure;
- retains the bounded MCP stderr tail and return code; and
- tells the user to close active CBM sessions and run from a standalone terminal, or use
  the campaign's isolated container environment.

### Remaining product-level fixes

- Make `daemon status` distinguish `no compatible daemon` from `incompatible active
  cohort`, including both build fingerprints and the same remediation as admission.
- Provide a documented, safe benchmark/container command that establishes OS-level
  account/runtime isolation; do not weaken the production single-cohort invariant merely
  to make benchmark subprocesses convenient.
- Add an integration test that keeps build A leased, asks build B for status, then runs a
  stateful B command and verifies that both surfaces report the same conflict class.
