# GDScript Real-Project Proof Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a reproducible local-only proof workflow that validates GDScript indexing against one or more real Godot projects and generates reviewable evidence for issue #186.

**Architecture:** Add one focused shell entrypoint for proof collection, one committed runbook, and one ignore rule for local artifacts. The script should create an isolated per-run workspace under `.artifacts/gdscript-proof/`, build/use the local `codebase-memory-mcp` binary, index one or more real repos, capture the fixed query suite as JSON, and synthesize per-repo plus aggregate Markdown summaries with explicit pass/fail coverage.

**Tech Stack:** POSIX shell/bash, `python3` stdlib `json`, existing `codebase-memory-mcp` CLI commands, git metadata commands, `.gitignore`, Markdown docs.

---

## Execution Contract (must be reflected in the implementation)

- Build the local binary with:

```bash
make -f Makefile.cbm cbm
```

- Use the resulting binary at:

```bash
build/codebase-memory-mcp
```

- Isolate proof state on every indexing/query command with:

```bash
export HOME="$RUN_ROOT/state/home"
export XDG_CONFIG_HOME="$RUN_ROOT/state/config"
export XDG_CACHE_HOME="$RUN_ROOT/state/cache"
```

- Treat the effective store root as:

```text
$XDG_CACHE_HOME/codebase-memory-mcp/
```

- Make these the actual runtime locations, not just reporting aliases:

```text
$RUN_ROOT/state/config
$RUN_ROOT/state/cache
$RUN_ROOT/state/store
```

- For compatibility with code paths that still fall back to `HOME/.cache`, create `"$HOME/.cache" -> "$XDG_CACHE_HOME"` and `"$RUN_ROOT/state/store" -> "$XDG_CACHE_HOME/codebase-memory-mcp"` before running CBM commands.

- Use raw CLI output for machine-readable capture:

```bash
build/codebase-memory-mcp cli --raw index_repository '{"repo_path":"/absolute/path/to/repo","mode":"full"}'
build/codebase-memory-mcp cli --raw query_graph '{"project":"<project_id>","query":"MATCH (f:File) RETURN count(f)","max_rows":5}'
```

- Distinguish these two identifiers everywhere:
  - `artifact_slug`: deterministic human-readable directory name for proof artifacts
  - `project_id`: the actual indexed project identifier used by `query_graph`

- Resolve `project_id` after indexing via `build/codebase-memory-mcp cli --raw list_projects`, matching the indexed repo path, then persist both identifiers in repo metadata.

- Every query artifact must preserve:
  - the query name
  - the literal query string
  - the `project_id`
  - the `artifact_slug`
  - the raw JSON response payload

- Use `python3` consistently for JSON wrapper-file creation and summary/count extraction; do not rely on optional local tools like `jq`.

---

## Source Spec

- Design spec: `docs/superpowers/specs/2026-04-04-gdscript-real-project-proof-design.md`

## File Structure (Planned Touchpoints)

### Proof workflow
- Create: `scripts/gdscript-proof.sh`
- Modify: `.gitignore`

### Documentation
- Create: `docs/superpowers/proofs/gdscript-real-project-validation.md`

---

## Chunk 1: Proof Workflow + Reproduction Docs

### Task 1: Add ignored local proof artifact root

**Files:**
- Modify: `.gitignore`

- [ ] **Step 1: Add a failing git-ignore check command**

Run from the `gdscript-support` worktree:

```bash
mkdir -p .artifacts/gdscript-proof/test-run && touch .artifacts/gdscript-proof/test-run/probe.txt
git status --short .artifacts/gdscript-proof/test-run/probe.txt
```

Expected: file shows as untracked before the ignore rule is added.

- [ ] **Step 2: Add the ignore rule**

Update `.gitignore` to include:

```gitignore
.artifacts/gdscript-proof/
```

- [ ] **Step 3: Re-run the git-ignore check**

Run:

```bash
git status --short .artifacts/gdscript-proof/test-run/probe.txt
```

Expected: no output.

- [ ] **Step 4: Clean up the probe directory**

Run:

```bash
rm -rf .artifacts/gdscript-proof/test-run
```

- [ ] **Step 5: Commit**

```bash
git add .gitignore
git commit -m "chore: ignore local gdscript proof artifacts"
```

---

### Task 2: Add the proof script skeleton and usage contract

**Files:**
- Create: `scripts/gdscript-proof.sh`

- [ ] **Step 1: Write the failing script invocation checks**

Run:

```bash
bash scripts/gdscript-proof.sh
```

Expected: FAIL because the script does not exist yet.

Then add a second red-state command for bad usage after the file exists:

```bash
bash scripts/gdscript-proof.sh >/tmp/gd-proof-usage.out 2>&1; echo $?
```

Expected after creating only a stub: non-zero exit with a clear usage message when no repo is provided.

- [ ] **Step 2: Create the minimal script with strict shell settings**

Create `scripts/gdscript-proof.sh` with at least:

```bash
#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/gdscript-proof.sh --repo /abs/path/to/repo [--repo /abs/path/to/repo2 ...] [--godot-version REPO=4.x] [--label REPO=name]
EOF
}

if [ "$#" -eq 0 ]; then
  usage >&2
  exit 2
fi
```

Keep the first commit focused on argument handling + usage only; do not build the full workflow yet.

- [ ] **Step 3: Re-run the usage checks**

Run:

```bash
bash scripts/gdscript-proof.sh >/tmp/gd-proof-usage.out 2>&1; rc=$?; printf '%s\n' "$rc"; cat /tmp/gd-proof-usage.out
```

Expected:
- exit code `2`
- output includes `Usage:` and `--repo`

- [ ] **Step 4: Add executable bit and shell syntax verification**

Run:

```bash
chmod +x scripts/gdscript-proof.sh
bash -n scripts/gdscript-proof.sh
```

Expected: no syntax errors.

- [ ] **Step 5: Commit**

```bash
git add scripts/gdscript-proof.sh
git commit -m "feat: add gdscript proof script usage contract"
```

---

### Task 3: Implement isolated artifact/state workspace creation

**Files:**
- Modify: `scripts/gdscript-proof.sh`

- [ ] **Step 1: Add a failing workspace-creation check**

Use a tiny local fixture repo or any disposable git repo path and run:

```bash
bash scripts/gdscript-proof.sh --repo /absolute/path/to/test-repo
```

Expected: FAIL because the script does not yet create the artifact root/state layout.

- [ ] **Step 2: Implement minimal artifact-root + state-root creation**

Add logic to create:

```text
.artifacts/gdscript-proof/<timestamp>/
  env.txt
  commands.log
  state/
    config/
    cache/
    store/
```

Implementation requirements:
- use UTC timestamp in the directory name
- log the current worktree path, branch, commit SHA, and binary path
- set `HOME="$RUN_ROOT/state/home"`, `XDG_CONFIG_HOME="$RUN_ROOT/state/config"`, and `XDG_CACHE_HOME="$RUN_ROOT/state/cache"` before any indexing/query command
- treat `$XDG_CACHE_HOME/codebase-memory-mcp/` as the effective cache/store root used by the proof workflow
- create compatibility symlinks so code paths using `HOME/.cache` still resolve inside the same isolated artifact root
- keep all runtime state under `state/`
- write helper functions instead of duplicating path creation logic

- [ ] **Step 3: Re-run the workspace-creation check**

Run:

```bash
bash scripts/gdscript-proof.sh --repo /absolute/path/to/test-repo || true
ls .artifacts/gdscript-proof
```

Expected: a new timestamped run directory exists with `env.txt`, `commands.log`, and `state/` subdirectories.

- [ ] **Step 4: Verify git still ignores the generated artifacts**

Run:

```bash
git status --short .artifacts/gdscript-proof
```

Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add scripts/gdscript-proof.sh
git commit -m "feat: isolate gdscript proof runtime state"
```

---

### Task 4: Record per-repo metadata and deterministic repo slugs

**Files:**
- Modify: `scripts/gdscript-proof.sh`

- [ ] **Step 1: Add a failing metadata check**

Run the script with one local git repo:

```bash
bash scripts/gdscript-proof.sh --repo /absolute/path/to/test-repo || true
```

Expected: FAIL because `<repo-slug>/repo-meta.json` is not yet produced.

- [ ] **Step 2: Implement repo argument parsing and metadata capture**

Add support for:
- repeated `--repo /abs/path`
- optional `--label /abs/path=name`
- optional `--godot-version /abs/path=4.x`

For each repo, write `repo-meta.json` containing at least:
- repo path
- repo name
- artifact slug
- `project_id` placeholder set to `null` or `"pending"` until indexing resolves the real value in Task 5
- proof-run UTC timestamp
- git commit/ref when available
- provided Godot version
- whether the repo qualifies for final Godot 4.x acceptance
- codebase-memory-mcp branch/commit/worktree under test

If a repo is not a git checkout, record commit/ref metadata as unavailable instead of failing this task.

Keep `artifact_slug` deterministic and collision-proof using:
- sanitized repo name
- short ref when available
- short stable hash of the canonical absolute repo path

Within a single run, collisions must be impossible.

Do **not** resolve the final `project_id` yet in this task; only prepare the metadata file shape. Resolve and backfill it after indexing in Task 5 by running:

```bash
build/codebase-memory-mcp cli --raw list_projects
```

Match the indexed repo path in that JSON response and persist the returned `project_id` separately from `artifact_slug`.

- [ ] **Step 3: Re-run the metadata check**

Run:

```bash
bash scripts/gdscript-proof.sh --repo /absolute/path/to/test-repo --godot-version /absolute/path/to/test-repo=4.2 || true
```

Expected: the latest run directory contains `<repo-slug>/repo-meta.json` with the required fields.

- [ ] **Step 4: Verify multi-repo parsing**

Run:

```bash
bash scripts/gdscript-proof.sh --repo /absolute/path/to/repo1 --repo /absolute/path/to/repo2 || true
```

Expected: two repo directories are created under the latest run root.

- [ ] **Step 5: Commit**

```bash
git add scripts/gdscript-proof.sh
git commit -m "feat: capture gdscript proof repo metadata"
```

---

### Task 5: Build/use the local binary and run indexing per repo

**Files:**
- Modify: `scripts/gdscript-proof.sh`

- [ ] **Step 1: Add a failing indexing check**

Run:

```bash
bash scripts/gdscript-proof.sh --repo /absolute/path/to/real-godot-repo
```

Expected: FAIL because indexing commands/logging are not yet implemented.

- [ ] **Step 2: Implement binary resolution and indexing**

Implementation requirements:
- always build or refresh the binary with `make -f Makefile.cbm cbm` at the start of the proof run
- always use `build/codebase-memory-mcp` for the remainder of the run; do not switch between multiple binary paths
- log every build/index command to `commands.log`
- write per-repo indexing output to `<artifact-slug>/index.log`
- ensure isolated proof state is used for every indexing/query command by exporting `HOME="$RUN_ROOT/state/home"`, `XDG_CONFIG_HOME="$RUN_ROOT/state/config"`, and `XDG_CACHE_HOME="$RUN_ROOT/state/cache"`
- pass `mode:"full"` in the `index_repository` payload for reproducibility
- after indexing each repo, resolve `project_id` from `build/codebase-memory-mcp cli --raw list_projects` by matching the repo path
- update the existing `<artifact-slug>/repo-meta.json` with the resolved `project_id` after indexing succeeds
- if one repo fails indexing, mark that repo failed/incomplete, preserve its artifacts, continue processing remaining repos, and decide aggregate pass/fail only after all repos have been attempted

Required command shape to preserve in the script/runbook:

```bash
build/codebase-memory-mcp cli --raw index_repository '{"repo_path":"/absolute/path/to/repo","mode":"full"}'
build/codebase-memory-mcp cli --raw list_projects
```

- [ ] **Step 3: Re-run the indexing check**

Run:

```bash
bash scripts/gdscript-proof.sh --repo /absolute/path/to/real-godot-repo
```

Expected: `index.log` is created for the repo and the script advances to the query stage.

- [ ] **Step 4: Verify failure behavior when indexing cannot succeed**

Run:

```bash
bash scripts/gdscript-proof.sh --repo /definitely/missing/path --repo /absolute/path/to/real-godot-repo; echo $?
```

Expected:
- the missing repo is recorded as failed/incomplete
- the valid repo is still indexed and gets artifacts
- `aggregate-summary.md` is still written after both repos are attempted
- final exit is decided only after all candidate repos are processed

- [ ] **Step 5: Commit**

```bash
git add scripts/gdscript-proof.sh
git commit -m "feat: index real repos in gdscript proof workflow"
```

---

### Task 6: Capture the fixed query suite as raw JSON outputs

**Files:**
- Modify: `scripts/gdscript-proof.sh`

- [ ] **Step 1: Add a failing query-output check**

Run the script on a real Godot repo that indexes successfully.

Expected: FAIL because `queries/*.json` files are not yet created.

- [ ] **Step 2: Implement query execution helpers**

Add one helper that runs a literal query string for the resolved `project_id` and writes a wrapper JSON document to a named file under:

```text
<artifact-slug>/queries/
```

Each JSON file must contain this shape:

```json
{
  "query_name": "gd-files",
  "project_id": "<project_id>",
  "artifact_slug": "<artifact-slug>",
  "query": "MATCH (f:File) WHERE f.file_path ENDS WITH \".gd\" RETURN count(f) AS gd_files",
  "result": {"...": "raw --raw CLI JSON response"}
}
```

Use `python3` to create these wrapper JSON files so the script does not depend on `jq`.

Also add one test-only failure-injection hook so query-failure handling is deterministic without hand-editing the script, for example:

```bash
GDSCRIPT_PROOF_INJECT_INVALID_QUERY=gd-files
```

When this env var names one required query, the script should replace only that query text with a known-invalid Cypher fragment and continue through the normal error-handling path.

The script must capture these exact queries:

```text
MATCH (f:File) WHERE f.file_path ENDS WITH ".gd" RETURN count(f) AS gd_files
MATCH (c:Class) WHERE c.file_path ENDS WITH ".gd" RETURN count(c) AS gd_classes
MATCH (m:Method) WHERE m.file_path ENDS WITH ".gd" RETURN count(m) AS gd_methods
MATCH (c:Class) WHERE c.file_path ENDS WITH ".gd" RETURN c.name, c.file_path ORDER BY c.file_path, c.name LIMIT 5
MATCH (m:Method) WHERE m.file_path ENDS WITH ".gd" RETURN m.name, m.file_path ORDER BY m.file_path, m.name LIMIT 5
MATCH (caller)-[c:CALLS]->(t:Function) WHERE t.qualified_name CONTAINS ".signal." RETURN count(c) AS signal_calls
MATCH (a)-[r:INHERITS]->(b) WHERE (a.file_path ENDS WITH ".gd") OR (b.file_path ENDS WITH ".gd") RETURN count(r) AS gd_inherits_edges
MATCH (m)-[r:IMPORTS]->(n) WHERE n.file_path ENDS WITH ".gd" RETURN count(r) AS gd_deps
```

Write them as:
- `gd-files.json`
- `gd-classes.json`
- `gd-methods.json`
- `gd-class-sample.json`
- `gd-method-sample.json`
- `signal-calls.json`
- `gd-inherits.json`
- `gd-imports.json`

Required command shape for each query:

```bash
build/codebase-memory-mcp cli --raw query_graph '{"project":"<project_id>","query":"<literal query>","max_rows":5}'
```

- [ ] **Step 3: Re-run the query-output check**

Run:

```bash
bash scripts/gdscript-proof.sh --repo /absolute/path/to/real-godot-repo || true
```

Expected: the repo output directory contains all eight JSON query files.

- [ ] **Step 4: Verify query failure handling**

Run the script with the test-only injection hook against a repo that otherwise indexes successfully:

```bash
GDSCRIPT_PROOF_INJECT_INVALID_QUERY=gd-files \
bash scripts/gdscript-proof.sh --repo /absolute/path/to/real-godot-repo; echo $?
```

Expected:
- non-zero exit
- latest run root still contains repo artifacts
- the repo summary is marked incomplete
- the aggregate summary does not count that repo toward final acceptance
- if additional repos are supplied in the same run, the script continues to them before exiting

- [ ] **Step 5: Commit**

```bash
git add scripts/gdscript-proof.sh
git commit -m "feat: capture gdscript proof query outputs"
```

---

### Task 7: Generate per-repo summaries and aggregate pass/fail coverage

**Files:**
- Modify: `scripts/gdscript-proof.sh`

- [ ] **Step 1: Add a failing summary check**

Run the script after raw query output exists.

Expected: FAIL because `summary.md` and `aggregate-summary.md` do not yet exist or do not contain pass/fail coverage.

- [ ] **Step 2: Implement summary generation**

For each repo, generate `summary.md` containing:
- repo path/name
- artifact slug
- resolved project ID
- commit/ref
- Godot version
- qualification status for final acceptance
- `.gd` file/class/method counts
- signal call count
- `.gd` inheritance count
- `.gd` import count
- short notes on what that repo proves
- incomplete/failed status if any required step failed

Define repo completeness explicitly in the script:
- `repo_complete=true` only when indexing succeeded and all eight required query wrapper JSON files were written successfully
- `repo_complete=false` if indexing failed, any required query command failed, or any required query wrapper file is missing/unparseable

Use `python3` for all JSON reads needed here: extracting counts from wrapper files, checking parseability, and generating the derived Markdown summary values.

Generate `aggregate-summary.md` containing:
- codebase-memory-mcp branch/commit/worktree under test
- list of repos processed
- which repos contributed to each acceptance category
- whether final acceptance passed
- explicit missing categories if final acceptance failed

Use this exact aggregate acceptance algorithm:

```text
indexing_coverage = any repo where repo_complete && godot_version_qualifies && gd_files > 0 && gd_classes > 0 && gd_methods > 0
signal_coverage = any repo where repo_complete && signal_calls > 0
imports_coverage = any repo where repo_complete && gd_deps > 0
inherits_coverage = any repo where repo_complete && gd_inherits_edges > 0
aggregate_pass = indexing_coverage && signal_coverage && imports_coverage && inherits_coverage
```

Additional rules:
- only a confirmed Godot 4.x repo may satisfy `indexing_coverage`
- a repo with unknown Godot version may still satisfy signal/imports/inherits coverage if `repo_complete=true`, but must be marked non-qualifying for the Godot-4.x indexing requirement
- a repo confirmed as Godot 3.x must never contribute to any acceptance category
- incomplete repos must never contribute to any coverage category

- [ ] **Step 3: Implement exit semantics**

Make the script:
- exit `0` only when aggregate acceptance passes
- exit non-zero when aggregate acceptance fails or all candidate repos fail indexing/query collection
- include the exact missing coverage categories in `aggregate-summary.md` before exiting non-zero
- attempt all candidate repos before deciding the final exit code; do not abort the overall run on the first per-repo indexing/query failure

- [ ] **Step 4: Re-run the summary checks**

Run on:
- one repo expected to pass enough categories
- one run expected to fail aggregate coverage

Expected:
- passing run exits `0`
- failing run exits non-zero
- both runs produce reviewable summaries under the local artifact root

- [ ] **Step 5: Commit**

```bash
git add scripts/gdscript-proof.sh
git commit -m "feat: summarize gdscript proof coverage"
```

---

### Task 8: Write the operator runbook

**Files:**
- Create: `docs/superpowers/proofs/gdscript-real-project-validation.md`

- [ ] **Step 1: Write the failing documentation gap down as a checklist**

Before writing the doc, confirm these missing items are not yet documented in one place:
- prerequisites
- required repo metadata
- exact script invocation examples
- output directory layout
- success/failure interpretation

Expected: there is no existing single doc covering the proof workflow.

- [ ] **Step 2: Write the runbook**

Document:
- purpose and link to issue #186
- prerequisites (local repos, expected binary/build state)
- accepted script arguments
- example single-repo and multi-repo commands
- what qualifies a repo as Godot 4.x evidence
- local artifact directory layout
- exact query suite names / output files
- how to read `summary.md` and `aggregate-summary.md`
- what exit code `0` vs non-zero means
- what must be included when reporting proof externally (repo, ref, Godot version, counts)

- [ ] **Step 3: Verify the doc matches the script**

Run through the examples in the doc and confirm the flags/path names match the implemented script exactly.

- [ ] **Step 4: Re-read for drift against the spec**

Compare the runbook against `docs/superpowers/specs/2026-04-04-gdscript-real-project-proof-design.md`.

Expected: no contradiction on artifact root, query suite, exit semantics, or Godot 4.x qualification.

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/proofs/gdscript-real-project-validation.md
git commit -m "docs: add gdscript real-project proof runbook"
```

---

### Task 9: Full verification against real repos

**Files:**
- Verify only: `scripts/gdscript-proof.sh`
- Verify only: `docs/superpowers/proofs/gdscript-real-project-validation.md`

- [ ] **Step 1: Run the proof workflow on at least one confirmed Godot 4.x repo**

Run the documented script invocation with one confirmed Godot 4.x repo.

Expected:
- per-repo summary exists
- aggregate summary exists
- repo is marked qualifying
- the qualifying repo has non-zero `.gd` file/class/method counts

- [ ] **Step 2: If needed, run additional repos to fill missing acceptance categories**

If the first repo does not show non-zero signal calls, `.gd` imports, and `.gd` inheritance together, run one or more additional repos.

Expected: final aggregate coverage reaches all required categories.

Before treating the run as successful, verify the aggregate summary explicitly shows:
- one confirmed Godot 4.x repo satisfying indexing coverage
- one or more complete repos satisfying signal coverage
- one or more complete repos satisfying import coverage
- one or more complete repos satisfying inheritance coverage

- [ ] **Step 3: Verify git remains clean except intended tracked changes**

Run:

```bash
git status --short
```

Expected: local proof artifacts under `.artifacts/gdscript-proof/` do not appear in status.

- [ ] **Step 4: Run repository test/build verification relevant to the touched files**

Run:

```bash
bash -n scripts/gdscript-proof.sh
make -f Makefile.cbm test
```

Expected:
- script syntax passes
- repository test suite passes

- [ ] **Step 5: Record the final verification evidence locations**

Write down for the eventual PR/issue update:
- proof run artifact root path(s)
- repo path(s) used
- repo commit/ref for each run
- Godot version metadata for each contributing repo
- final aggregate pass/fail result

Expected: verification is complete without creating an empty extra commit.

---

## Global Verification Checklist (must pass before PR)

- [ ] `scripts/gdscript-proof.sh` creates isolated local state only under `.artifacts/gdscript-proof/<timestamp>/`
- [ ] `.artifacts/gdscript-proof/` stays ignored by git
- [ ] raw JSON query outputs are written for every required proof query
- [ ] per-repo and aggregate summaries are generated
- [ ] aggregate exit semantics are correct (`0` only on full acceptance)
- [ ] at least one confirmed Godot 4.x repo contributes to the final proof
- [ ] if one repo is insufficient, additional repos fill the remaining coverage gaps
- [ ] `bash -n scripts/gdscript-proof.sh` passes
- [ ] `make -f Makefile.cbm test` passes

## Skills to apply during execution

- `@superpowers:test-driven-development` before each task implementation
- `@superpowers:systematic-debugging` if any command, test, or proof run behaves unexpectedly
- `@superpowers:verification-before-completion` before claiming the proof workflow is complete
- `@superpowers:requesting-code-review` before merge
