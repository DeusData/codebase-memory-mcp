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

## D-005 recurrence — transport remains closed after the client turn resumes

- Recurrence observed UTC: `2026-08-03T18:01:18Z`.
- Recorded UTC: `2026-08-03T18:11:07Z`.
- Project argument:
  `/Users/athundt/.claude/codebase-memory-mcp/.worktrees/api-consolidation-merge`.
- Tool: `search_graph`.
- Arguments: `pattern="build_quick_hypothesis_spec|run_container_experiment|materialize_container_matrix_spec|build_measured_command"`,
  `file_pattern="benchmarks/*.py"`, `include_connected=true`, `limit=40`.

The resumed client advertised the MCP tools again and supplied fresh project context, but
the first serialized graph call still failed immediately:

```text
tool call error: tool call failed for `codebase-memory-mcp/search_graph`

Caused by:
    Transport closed
```

Reproduction: trigger D-005, continue the same conversation in a later agent turn, then
issue one serialized `search_graph` request. Expected: a newly advertised tool connection
is usable, or the client reports a structured reconnect action. Observed: the stale closed
transport survives the resumed turn. Direct reads were therefore used for
`benchmarks/campaign_specs.py`, `benchmarks/run_container_experiment.py`,
`benchmarks/run_evidence_suite.py`, `benchmarks/README.md`, and their tests; Tier-2
`check_index_coverage` remained impossible.

## D-005 recurrence 2 — newly advertised Tier-2 calls still fail after compaction

- Recurrence observed and recorded UTC: `2026-08-03T18:43:16Z`.
- Source revision: `a67a91d2b191f723dc11873e4c51fc9537cbac1f`.
- Source tree: `6de33913ca345b4e513cbf9a7cbc7c48ff47436b`.
- Branch/worktree: `api-consolidation-merge` at
  `/Users/athundt/.claude/codebase-memory-mcp/.worktrees/api-consolidation-merge`.
- Client context advertised the project as indexed, `auto_watch=true`, and Tier 2.
- Calls were serialized in one tool invocation; no new concurrent request burst occurred.

The first three post-compaction `search_graph` requests targeted distinct, existing Python
surfaces:

1. `pattern="*autotune*"`, `limit=20`, `include_dependencies=false`;
2. `file_pattern="benchmarks/run_experiments.py"`, `mode="summary"`, `limit=20`;
3. `file_pattern="benchmarks/rank_report.py"`, `pattern="*"`, `limit=20`.

Every call immediately returned the same response:

```text
tool call error: tool call failed for `codebase-memory-mcp/search_graph`

Caused by:
    Transport closed
```

This recurrence is stronger than a stale-symbol explanation: the calls included a file-scoped
summary that did not depend on resolving a particular qualified name, and they were issued after
the client supplied a fresh automatic session context. Expected behavior is either a working new
transport or a structured reconnect/retry instruction. Observed behavior is a re-advertised but
unusable tool surface. Exact source reads were used for `benchmarks/autotune.py`,
`benchmarks/campaign_specs.py`, `benchmarks/rank_hypotheses.py`,
`benchmarks/rank_report.py`, `benchmarks/run_benchmark.py`,
`benchmarks/run_container_experiment.py`, `benchmarks/run_evidence_suite.py`,
`benchmarks/run_experiments.py`, `benchmarks/README.md`,
`docs/BENCHMARK_EXPERIMENTS.md`, and their relevant tests. Tier-2
`check_index_coverage` could not be completed.

## D-005 recurrence 3 — explicit worktree project still uses the closed transport

- Recurrence observed UTC: `2026-08-03T18:51:55Z`.
- Recorded UTC: `2026-08-03T18:57:17Z`.
- Source revision: `a67a91d2b191f723dc11873e4c51fc9537cbac1f`.
- Source tree: `6de33913ca345b4e513cbf9a7cbc7c48ff47436b`.
- Client again advertised the graph project as indexed, `auto_watch=true`, and Tier 2.
- The call was serialized and was the first graph request after the new client context.

Reproduction call:

```text
search_graph(
  project="/Users/athundt/.claude/codebase-memory-mcp/.worktrees/api-consolidation-merge",
  file_pattern="benchmarks/run_evidence_suite.py",
  mode="summary"
)
```

Observed response:

```text
tool call error: tool call failed for `codebase-memory-mcp/search_graph`

Caused by:
    Transport closed
```

This attempt supplied the exact worktree path rather than the earlier derived project name,
so project-name resolution is not a sufficient explanation. The tool was newly advertised
and accepted the request schema, but the request reached the same closed transport. Expected:
the first serialized request after fresh session context either succeeds or returns a
structured reconnect action. Observed: the closed transport remains terminal across context
refreshes and explicit project addressing. Direct numbered source reads and `rg` were used for
the remaining benchmark, database-lifecycle, and report work; Tier-2
`check_index_coverage` was still unavailable.

## D-005 recovery boundary — a new app process restores the transport

- Recovery verified UTC: `2026-08-03T19:48:03Z`.
- Source revision: `e2d8fa48d34409eb3adf798c4ae23e4d09a168e4`.
- Indexed project:
  `Users-athundt-.claude-codebase-memory-mcp-.worktrees-api-consolidation-merge`.
- Calls were serialized.

After the desktop app crashed and restarted, the first `search_graph` call succeeded. The same
connection then completed `trace_path`, `check_index_coverage`, `get_code`, and `index_status`.
This narrows D-005: a new conversation turn, compaction, re-advertised tools, and explicit project
addressing did not recover the transport, but a new app process did. The available evidence is
consistent with a client/app-lifetime connection remaining closed after a concurrent request
failure; it does not prove whether the close originates in the client, MCP bridge, or server.

Reproduction:

1. Trigger D-005 with concurrent graph requests.
2. Verify that serialized requests remain closed across a later turn or compaction.
3. Restart the desktop app without restarting the shared daemon.
4. Issue one serialized `search_graph` request.

Expected: the original session recovers automatically or returns a structured reconnect action.
Observed: only the new app process restored graph calls. A transport regression test should cover
both reconnect-in-place and reconnect-after-client-restart behavior and identify which process
owns the failed connection.

## D-001 recurrence — stale span is returned despite coverage knowing the file changed

- Recurrence verified UTC: `2026-08-03T19:48:03Z`.
- Requested symbol:
  `Users-athundt-.claude-codebase-memory-mcp-.worktrees-api-consolidation-merge.src.pagerank.pagerank.edge_type_weight`.
- Requested mode: `full`, `max_lines=80`, `compact=false`.

The exact `qualified_name` came from a successful `search_graph` result. `get_code` reported
`start_line=49`, `end_line=65`, retained the correct function name/signature, but returned the
unrelated `CBM_ENABLE_TEST_SEAMS` block and the beginning of
`cbm_pagerank_test_fail_scan_after`. The live source defines `edge_type_weight` at
`src/pagerank/pagerank.c:99-107`.

The same session's `check_index_coverage` reported `freshness=metadata_changed` for
`src/pagerank/pagerank.c` and recommended `read_source_and_reindex`. The unsafe inconsistency is
therefore not only stale metadata: `get_code` used the known-stale span without warning, refusing,
or refreshing, while presenting current-looking symbol metadata.

Exact call:

```text
get_code(
  project="Users-athundt-.claude-codebase-memory-mcp-.worktrees-api-consolidation-merge",
  qualified_name="Users-athundt-.claude-codebase-memory-mcp-.worktrees-api-consolidation-merge.src.pagerank.pagerank.edge_type_weight",
  mode="full",
  max_lines=80,
  compact=false
)
```

Root-cause acceptance test: mutate a watched file so a symbol moves, then call `get_code` before
and after background refresh. The tool must either return source whose live range encloses the
requested definition or return a structured stale-source error carrying the coverage action. It
must never combine the selected symbol's name/signature with another symbol's body.

## D-007 — freshness surfaces can simultaneously read as current and stale

- Verified UTC: `2026-08-03T19:48:03Z`.
- `index_status(verbose=true)` reported `status=ready`, Git
  `head_sha=e2d8fa48d34409eb3adf798c4ae23e4d09a168e4`,
  `head_matches_worktree=true`, and `worktree_dirty=false`.
- `check_index_coverage` reported coverage generation `2026-07-30T01:53:35Z`,
  `freshness=metadata_changed` for the C sources and `freshness=not_tracked` for several
  benchmark Python files.
- `search_graph` reported PageRank, LinkRank, and node-degree derived views stale.
- `index_status` recorded `src/cli/cli.c:1-13308` as one parse-partial range.

These fields may describe different layers—live Git state, persisted coverage generation, and
derived-view freshness—but the response does not provide one top-level statement separating
them. A caller can reasonably read `status=ready` plus `head_matches_worktree=true` as evidence
that graph spans are current, even though `get_code` then returns a stale body.

Reproduction: run `index_status(verbose=true)`, `check_index_coverage` for a changed file, and
`get_code` for a moved symbol in sequence. Expected: one explicit source-freshness verdict and an
actionable distinction among repository state, symbol/span generation, coverage generation, and
derived-view generation. Observed: each surface is internally plausible but their composition is
unsafe without expert interpretation.

## D-001 recurrence — Python symbols carry another function's source body

- Recurrence verified UTC: `2026-08-04T01:26:46Z`.
- Requested project path:
  `/Users/athundt/.claude/codebase-memory-mcp/.worktrees/api-consolidation-merge`.
- Calls were serialized.

`search_graph(pattern="^(ensure_volume|export_results|main|parse_args)$",
file_pattern="benchmarks/run_container_experiment.py")` returned the expected current qualified
names and identified `main` as the caller of both helpers. The following exact `get_code` calls
then combined those names and signatures with unrelated live source:

```text
get_code(
  project="/Users/athundt/.claude/codebase-memory-mcp/.worktrees/api-consolidation-merge",
  qualified_name="Users-athundt-.claude-codebase-memory-mcp-.worktrees-api-consolidation-merge.benchmarks.run_container_experiment.ensure_volume",
  mode="full",
  max_lines=120,
  compact=false
)
```

Observed metadata reported `start_line=400`, `end_line=430`, and signature
`(docker: str, name: str, role: str)`, while the payload began with
`native_linux_platform` and `validate_resources`. The live numbered source defines
`ensure_volume` at `benchmarks/run_container_experiment.py:652-682` before the retention fix.

The equivalent call for `export_results` reported `start_line=468`, `end_line=498`, and its
correct signature, while returning the middle of `materialize_container_matrix_spec`. The live
numbered source defines `export_results` at
`benchmarks/run_container_experiment.py:720-750` before the retention fix. A `head_tail` request
for `main` likewise reported stale `start_line=601`, `end_line=902` and began with `run_command`;
the live definition begins at line 923.

Reproduction: run the `search_graph` request above, copy each returned exact qualified name into
`get_code`, and compare the returned payload with a numbered read of the reported file. Expected:
the body and line range enclose the selected definition, or the tool returns a structured stale
span error. Observed: correct symbol metadata is attached to another definition's body without a
staleness warning. This confirms D-001 is not limited to C parsing or one symbol kind.

### D-001 recurrence during immutable Docker-lock TDD

- Recurrence verified UTC: `2026-08-04T02:09:44Z`.
- Requested project path:
  `/Users/athundt/.claude/codebase-memory-mcp/.worktrees/api-consolidation-merge`.
- Calls were serialized; the live Python file had uncommitted benchmark-lifecycle edits.

`search_graph` with
`name_pattern="^(main|archive_source_bundle|acquire_history_lock|apply_work_volume_retention|benchmark_volume_labels)$"`,
`file_pattern="benchmarks/run_container_experiment.py"`, and `include_connected=true` returned only
`main`, warned that PageRank, LinkRank, and node-degree views were stale, and did not return the
four named live helpers. A subsequent exact call was:

```text
get_code(
  project="/Users/athundt/.claude/codebase-memory-mcp/.worktrees/api-consolidation-merge",
  qualified_name="Users-athundt-.claude-codebase-memory-mcp-.worktrees-api-consolidation-merge.benchmarks.run_container_experiment.main",
  mode="head_tail",
  max_lines=80,
  compact=false
)
```

Observed metadata again reported `start_line=601`, `end_line=902`, and the correct `main`
signature, while the source payload began inside `merge_exported_tree` and then showed
`archive_source_bundle` and `run_command`. The current numbered source defines `main` at
`benchmarks/run_container_experiment.py:1018`; it now extends beyond line 1410. Expected: current
`main` source, or a structured stale-span result that names the required refresh action. This
recurrence also shows that a positive graph match is not sufficient evidence that adjacent newly
added Python definitions are indexed.

The required post-discovery `check_index_coverage` call reported `metadata_changed` with
`read_source_and_reindex` for both benchmark modules and both focused test modules. It reported
`docs/` and `notes/` as excluded/not tracked. Direct numbered source and the executed tests are
therefore authoritative for this commit; this graph generation cannot verify the edited spans.
