# Measuring quality, latency, and agent savings

This guide describes how to measure codebase-memory-mcp (CBM) on your own
repository without mixing three different questions:

1. Does the agent give a better answer?
2. Is CBM fast and stable under the workload?
3. Does graph-assisted exploration use fewer model tokens or tool calls than
   file-by-file exploration?

Measure and report those classes separately. A fast graph query does not prove
that the final answer is correct, and CBM's query counters do not reveal the
agent's model-token or total tool-call consumption. The README's published
[performance figures](../README.md#performance), the
[language benchmark](BENCHMARK.md) and the more detailed
[evaluation plan](EVALUATION_PLAN.md) provide the grading concepts and the
larger comparative methodology behind this smaller recipe.

## Freeze the experiment

The shell examples in this guide require a POSIX-compatible shell on macOS or
Linux, or Git Bash on Windows. They are not native PowerShell syntax.
Run the command blocks in one dedicated shell: the setup and preflight blocks
enable fail-fast handling, and they capture Git command output before testing it
so a failed inner command cannot be mistaken for a clean worktree.

Before either condition runs, record:

- the repository URL or local identity and exact Git commit SHA;
- the question set and expected scope of each answer;
- the CBM version, index mode, operating system, and machine details;
- the agent model/version, system prompt, tool instructions, context limits,
  and per-question budget;
- which condition runs first, plus any warm-up policy;
- the client or evaluation harness used to record tokens and tool calls.

Use the same repository SHA, questions, model, prompt, budget, and clean-session
policy for both conditions. Do not let the second condition see the first
condition's answers or tool results. If runs are repeated, choose the repeat
count and aggregation method before looking at results and retain every run,
including failures and zero-result queries.

Do not run either condition in a merely clean-looking working copy. A frozen
experiment includes untracked files, so create separate clean detached
worktrees for the two conditions:

```bash
set -eu
SOURCE_REPO=/absolute/path/to/source-repository
COMMITISH=main
SHA=$(git -C "$SOURCE_REPO" rev-parse "$COMMITISH^{commit}") || exit 1
MODE=full
RUN_ROOT=$(mktemp -d) || exit 1
GRAPH_REPO="$RUN_ROOT/graph"
BASELINE_REPO="$RUN_ROOT/baseline"
ARTIFACT_ROOT="$RUN_ROOT/artifacts"

git -C "$SOURCE_REPO" worktree add --detach "$GRAPH_REPO" "$SHA" || exit 1
git -C "$SOURCE_REPO" worktree add --detach "$BASELINE_REPO" "$SHA" || exit 1
mkdir "$ARTIFACT_ROOT" || exit 1

GRAPH_SHA=$(git -C "$GRAPH_REPO" rev-parse HEAD) || exit 1
GRAPH_STATUS=$(git -C "$GRAPH_REPO" status --porcelain=v1 --untracked-files=all) || exit 1
BASELINE_SHA=$(git -C "$BASELINE_REPO" rev-parse HEAD) || exit 1
BASELINE_STATUS=$(git -C "$BASELINE_REPO" status --porcelain=v1 --untracked-files=all) || exit 1
test "$GRAPH_SHA" = "$SHA" || exit 1
test -z "$GRAPH_STATUS" || exit 1
test "$BASELINE_SHA" = "$SHA" || exit 1
test -z "$BASELINE_STATUS" || exit 1
```

Repeat the matching SHA and cleanliness assertions immediately before indexing
and immediately before each condition. If either assertion fails, stop and
create a new detached worktree; do not erase unknown files to make a reused
checkout appear clean. Write run artifacts under `ARTIFACT_ROOT`, outside both
condition worktrees.

## Preflight the graph

Immediately before every Graph-condition measurement, repeat its clean-worktree
assertions and require a successful fresh index of that exact worktree and mode:

```bash
set -eu
GRAPH_SHA=$(git -C "$GRAPH_REPO" rev-parse HEAD) || exit 1
GRAPH_STATUS=$(git -C "$GRAPH_REPO" status --porcelain=v1 --untracked-files=all) || exit 1
test "$GRAPH_SHA" = "$SHA" || exit 1
test -z "$GRAPH_STATUS" || exit 1
codebase-memory-mcp cli index_repository \
  --repo-path "$GRAPH_REPO" \
  --mode "$MODE" || exit 1
GRAPH_SHA_AFTER=$(git -C "$GRAPH_REPO" rev-parse HEAD) || exit 1
GRAPH_STATUS_AFTER=$(git -C "$GRAPH_REPO" status --porcelain=v1 --untracked-files=all) || exit 1
test "$GRAPH_SHA_AFTER" = "$SHA" || exit 1
test -z "$GRAPH_STATUS_AFTER" || exit 1
```

Take `PROJECT_NAME` from the successful indexing response. A verbose status call
is useful only as a root/current-HEAD cross-check:

```bash
codebase-memory-mcp cli list_projects
codebase-memory-mcp cli index_status --project PROJECT_NAME --verbose
```

Confirm that `root_path` is `GRAPH_REPO`, `git.head_sha` is `SHA`, and the Git
context is detached. `index_status` describes the project root and its current
filesystem Git context; it does **not** by itself prove that indexed records
were produced from that revision. The immediately preceding successful fresh
`index_repository` call is the freshness requirement.

Finally, run one or more representative queries whose expected symbols you
have verified directly at the recorded SHA:

```bash
codebase-memory-mcp cli search_graph \
  --project PROJECT_NAME \
  --name-pattern 'KNOWN_SYMBOL_PATTERN' \
  --limit 10
```

Do not start measurement until the fresh index succeeds, the status cross-check
names the intended detached checkout and current SHA, and the representative
queries return the expected symbols. Save the indexing response, status, and
query outputs with the run artifacts. Immediately before the file-by-file
condition, repeat the corresponding SHA and cleanliness assertions for
`BASELINE_REPO`.

## 1. Measure answer quality

Create a fixed set of real developer questions. Include a mix of definition
discovery, relationships or call paths, targeted source retrieval,
architecture, and cross-cutting questions where those dimensions apply. Record
the expected scope or an independently derived ground truth for each question.

Run two isolated conditions:

| Condition | Allowed exploration tools |
|-----------|---------------------------|
| Graph | CBM graph tools such as `search_graph`, `trace_path`, `query_graph`, `get_code_snippet`, `get_architecture`, and `search_code` |
| File-by-file baseline | File listing, text search, and targeted file reads only |

Grade the final answers against the source at the frozen SHA, not against how
plausible they sound. The compact rubric in [BENCHMARK.md](BENCHMARK.md) uses
PASS (1.0), PARTIAL (0.5), and FAIL (0.0), excluding truly inapplicable
questions from the denominator. For a finer comparison, follow
[EVALUATION_PLAN.md](EVALUATION_PLAN.md): score correctness, completeness, and
specificity separately, blind the grader to which condition produced each
answer, randomize A/B order, and require source evidence for the grade.

Keep quality scores beside, but separate from, efficiency metrics. A token
reduction is only useful when the answer still meets the chosen quality bar.

## 2. Measure latency and stability

Record indexing time separately from query latency, and classify the index run
as full-source, artifact-assisted, or incremental. When no local project
database exists, `index_repository` can import a compatible
`.codebase-memory/graph.db.zst` and then take the incremental-manifest route.
Before timing, record whether that artifact exists in `GRAPH_REPO`, retain the
index log, and require the later successful `artifact.import` record containing
`db` and `size_mb` before classifying the run as artifact-assisted. A bootstrap
attempt alone, or an `artifact.import` record containing `skip` or `err`, does
not prove that the artifact was used. For queries, use the same fixed workload
in the same order, identify warm-up calls in advance, and retain per-call
durations and exit status rather than only a single average. Report the machine,
OS, CBM version, repository SHA, index mode, question set, index-run class, and
whether query results are cold or warm.

### Built-in diagnostics

For a daemon-backed run, enable diagnostics before the first session starts.
The daemon captures its environment at startup; if it is already running,
close all daemon-backed sessions before changing the setting. See
[Configuration](CONFIGURATION.md#4-environment-variables) for the complete
environment contract.

```bash
export CBM_DIAGNOSTICS=1
```

CBM creates a fresh randomized, owner-private diagnostics directory below the
system temporary directory. Do not assume or construct an old predictable
`/tmp` filename. Discover the exact `snapshot` and `trajectory` paths from the
`diagnostics.start` JSON record in
`${CBM_CACHE_DIR}/logs/cbm-daemon.log` (the default cache directory is
`~/.cache/codebase-memory-mcp`). The record is emitted even when the configured
log level suppresses ordinary logging. The README's
[diagnostics section](../README.md#troubleshooting--diagnostics) explains the
files and retention behavior.

The live `snapshot.json` includes CBM-side `query_count`, `query_errors`,
`query_total_us`, `query_avg_us`, and `query_max_us`, along with process resource
counters. The retained `trajectory.ndjson` provides the resource and query-count
trend over time. These are useful for CBM latency and stability analysis, but
they are not a record of an agent's model usage or of non-CBM tools.
Because a daemon can be shared by multiple sessions, use an otherwise idle
daemon and record before/after values (or start a dedicated run) when attributing
its counters to one workload.

### Canonical soak workload

From a source checkout, the canonical endurance entry point is:

```bash
scripts/soak-legs.sh build/c/codebase-memory-mcp 10
```

It runs the quick mixed workload and the read-only query-leak workload, checks
for a valid completion summary, and writes per-call latency plus resource
results. The quick leg writes `soak-results/`; the query-leak leg writes
`soak-results-query-leak/`. Each directory contains `latency.csv`, `metrics.csv`,
and `summary.txt`. The two CSV files begin with these exact headers:

```text
timestamp,tool,duration_ms,exit_code
timestamp,uptime_s,rss_bytes,heap_committed,fd_count,query_count,query_max_us
```

Do not invoke `scripts/soak-test.sh` directly; `soak-legs.sh` owns the
release-gating sequence. Treat these artifacts as stability and CBM latency
evidence, not as answer-quality or model-token evidence.

## 3. Measure token and tool-call savings

Run the Graph and file-by-file conditions on the same frozen inputs described
above. The MCP client or evaluation harness must capture agent usage because
CBM cannot know the final model input/output token count or the agent's total
tool-call consumption.

Define each `run_id` as one paired experimental replicate. Each
`(run_id, condition)` is exactly one isolated client session that answers one
question. Capture one row for every usage window the client directly measures:

```text
run_id,condition,repo_sha,question_id,window,input_tokens,output_tokens,total_tokens,tool_calls,wall_time_ms,answer_artifact,quality_score
```

In each row, `tool_calls` is the count of every client tool invocation inside
that same window, including orchestration calls and the graph or file tools
permitted for that condition, plus retries, errors, and zero-result calls. Do
not count only CBM calls. Candidate windows are:

- **Answering tokens:** input plus output tokens between fixed markers around
  the question-answering phase.
- **Full-session tokens:** the entire isolated session, including orientation,
  initial probes, dead ends, and answer formatting.

The full-session value best represents an adopter's total cost; the answering
window helps explain where a difference arose. Do not substitute CBM's
`query_count` for either `tool_calls` value: it cannot see file searches, file
reads, or other client tools.

For each Graph/baseline pair, compare only a window that both clients report
directly, with at most one row per `(run_id, condition, window)`. If either
client cannot expose a window, omit that window or record its usage fields as
N/A and exclude it from the comparison. Never infer a missing window, partition
a session total across questions, or duplicate one session total into multiple
question rows.

Select one window `W` and calculate from the Graph and baseline rows for that
same window only:

```text
token reduction (%)     = 100 * (baseline tokens - Graph tokens) / baseline tokens
tool-call reduction (%) = 100 * (baseline calls  - Graph calls)  / baseline calls
token ratio              = baseline tokens / Graph tokens
tool-call ratio          = baseline calls  / Graph calls
```

Calculate and label every common measured window separately; never mix their
numerators or denominators. If baseline tokens or calls are zero, the
corresponding reduction percentage is N/A. If Graph tokens or calls are zero,
the corresponding ratio is N/A. Do not add pseudocounts. Publish the raw paired
counts alongside ratios and percentages. Also publish the quality result for
each pair, the number of runs, aggregation method, failures, and experimental
controls. Never turn a result from one repository, question set, model, or
machine into a universal savings claim.

## Reproducibility checklist

- Repository identity and exact SHA are recorded; each condition uses a clean,
  detached worktree with no tracked or untracked changes.
- CBM version, index mode, project name, and preflight outputs are retained.
- Every Graph run follows a successful fresh index of the frozen worktree and
  mode; verbose status is only a root/current-HEAD cross-check.
- Questions, ground truth, prompts, budgets, and condition order are frozen.
- Graph and file-by-file runs use isolated sessions and the same controls.
- Quality, CBM latency/stability, and agent savings are reported separately.
- Usage counts come directly from the client or evaluation harness; each
  condition run is one isolated question/session, common windows use separate
  rows, and unsupported windows are omitted or N/A rather than inferred.
- Diagnostics paths come from `diagnostics.start`, not a guessed temp path.
- Raw outputs, errors, zero results, and calculation inputs are retained.
- Claims identify their repository, SHA, question set, model, machine, and run
  count, with no invented or extrapolated benchmark numbers.
