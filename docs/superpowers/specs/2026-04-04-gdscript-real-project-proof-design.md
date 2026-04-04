# Design Spec: Real-Project GDScript Validation Proof

Date: 2026-04-04  
Related issue: https://github.com/DeusData/codebase-memory-mcp/issues/186

## 1) Objective

Produce reproducible **real-project validation evidence** that the new GDScript indexing behavior works on actual Godot repositories, without committing machine-local MCP databases or proof output artifacts into git.

## 2) Scope and Constraints

### In scope
- A reproducible, in-repo validation workflow for real Godot projects
- A local-only proof workspace for generated evidence artifacts
- Automated collection of the required query evidence for issue #186
- Support for validating one or more external Godot repositories in a single run
- Human-readable local proof output summarizing what was validated

### Out of scope
- Committing generated proof output into the repository
- Checking external Godot repos into this repository
- Changing graph schema or GDScript extraction behavior as part of the proof workflow itself
- Treating raw SQLite / MCP memory files as the review artifact

### Hard constraints
- Proof artifacts must remain **local-only**
- Reproduction steps must be committed **in-repo**
- Validation may span **2+ real Godot repos** if one repo does not cover all required behaviors
- The workflow must work from the `gdscript-support` worktree without mutating unrelated repository state
- Final acceptance for issue #186 requires evidence from at least one validated **Godot 4.x** repo

## 3) Selected Approach

Chosen approach: **scripted local proof workspace + committed runbook**.

Rationale:
- Keeps the evidence reproducible without polluting git with machine-specific output
- Makes review practical by generating concise summaries from raw query results
- Separates durable instructions from disposable validation data
- Supports both “one repo covers everything” and “multiple repos cover the full acceptance set”

## 4) Architecture and Module Boundaries

### A. Committed reproduction surface
- Add a committed runbook describing prerequisites, inputs, commands, and acceptance criteria.
- Add a committed script that performs indexing and proof-query collection against one or more repo paths.

### B. Local proof workspace
- Generate all runtime artifacts under an ignored local directory such as `.artifacts/gdscript-proof/<timestamp>/`.
- Store logs, raw query output, environment details, and generated summaries there.

### C. Isolated MCP/store state
- Use a dedicated local cache/config/state location for the proof run so the workflow does not rely on or contaminate the operator’s normal codebase-memory-mcp state.
- Require **all** runtime state for the proof run to live under the ignored artifact root.
- Each run should be self-contained and attributable to a specific timestamp + repo set.
- Each repo in a run should get a deterministic slug/project ID derived from repo name + short commit/ref (or sanitized path fallback) to prevent cross-repo collisions.

### D. Evidence synthesis
- Emit both raw machine-readable outputs and short Markdown summaries.
- Summaries should explicitly state what each repo proves: `.gd` discovery, class/method indexing, signal-call evidence, `.gd` dependency/import evidence, and `.gd` inheritance evidence.

## 5) Planned File-Level Changes

### Runbook + durable docs
- `docs/superpowers/specs/2026-04-04-gdscript-real-project-proof-design.md`
  - This design spec.
- `docs/superpowers/proofs/gdscript-real-project-validation.md`
  - Operator-facing reproduction guide.
  - Documents prerequisites, required repo metadata, commands, expected output shape, and success criteria.

### Automation
- `scripts/gdscript-proof.sh`
  - Accept one or more repo paths.
  - Optionally accept repo labels / Godot version metadata.
  - Create isolated local artifact/cache directories.
  - Index each repo.
  - Run the fixed query suite.
  - Write per-repo and aggregate summaries.

### Ignore rules
- `.gitignore`
  - Ignore generated local proof output directory (for example `.artifacts/gdscript-proof/`).

## 6) Data and Control Flow

1. Operator chooses one or more local Godot repo paths.
2. Proof script creates a timestamped local artifact root.
3. Proof script configures an isolated local codebase-memory-mcp config/cache/store root under that artifact directory.
4. For each repo:
   - record repo path, repo name, commit/ref, and optional Godot version metadata
   - run `index_repository`
   - run the fixed proof query set
   - persist raw query output
   - generate a concise Markdown summary for that repo
5. After all repos finish, generate an aggregate summary stating whether the overall acceptance coverage is satisfied.

## 7) Execution Contract

The implementation must make the proof run reproducible and machine-checkable.

### Required command shape
- The workflow must use the locally built `codebase-memory-mcp` binary from the `gdscript-support` worktree under test.
- The script must log the exact build + validation commands it runs.
- Raw query results must be captured in a machine-readable format (`json` preferred).
- The script must explicitly set isolated config/cache/state paths under the artifact root before any indexing/query step.

### Required recorded execution metadata
- codebase-memory-mcp worktree path
- current branch name
- current commit SHA under test
- binary path used for the proof run
- timestamp (UTC)
- per-repo project ID / slug

### Exit semantics
- Exit `0` only if aggregate acceptance coverage is satisfied.
- Exit non-zero if:
  - indexing fails for every candidate repo
  - any required query fails for a repo that would otherwise contribute required coverage
  - aggregate acceptance coverage is not satisfied
- A repo with incomplete or failed evidence collection may still have artifacts written, but it must not count toward final acceptance.

## 8) Proof Artifact Model

### Committed artifacts
- Reproduction runbook
- Proof automation script
- Ignore rules for local proof output

### Local-only artifacts
- Isolated MCP/config/cache/store state
- Raw CLI command logs
- Raw query outputs (JSON/Markdown/text)
- Generated per-repo summaries
- Generated aggregate summary

### Suggested local directory shape

```text
.artifacts/gdscript-proof/<timestamp>/
  env.txt
  commands.log
  aggregate-summary.md
  state/
    config/
    cache/
    store/
  <repo-slug>/
    repo-meta.json
    index.log
    queries/
      gd-files.json
      gd-classes.json
      gd-methods.json
      gd-class-sample.json
      gd-method-sample.json
      signal-calls.json
      gd-inherits.json
      gd-imports.json
    summary.md
```

## 9) Required Query Suite

The proof workflow should capture, at minimum, the queries already called for by the Task 7 validation checklist:

- count `.gd` files
- count `.gd` classes
- count `.gd` methods
- sample `.gd` classes
- sample `.gd` methods
- count signal-targeted `CALLS`
- count `.gd`-related `INHERITS`
- count `.gd`-targeted `IMPORTS`

### Literal query contract
The implementation should keep the query texts aligned with the Task 7 validation checklist and record the literal query string for each captured result. At minimum, the query suite must cover:

- `MATCH (f:File) WHERE f.file_path ENDS WITH ".gd" RETURN count(f) AS gd_files`
- `MATCH (c:Class) WHERE c.file_path ENDS WITH ".gd" RETURN count(c) AS gd_classes`
- `MATCH (m:Method) WHERE m.file_path ENDS WITH ".gd" RETURN count(m) AS gd_methods`
- `MATCH (c:Class) WHERE c.file_path ENDS WITH ".gd" RETURN c.name, c.file_path ORDER BY c.file_path, c.name LIMIT 5`
- `MATCH (m:Method) WHERE m.file_path ENDS WITH ".gd" RETURN m.name, m.file_path ORDER BY m.file_path, m.name LIMIT 5`
- `MATCH (caller)-[c:CALLS]->(t:Function) WHERE t.qualified_name CONTAINS ".signal." RETURN count(c) AS signal_calls`
- `MATCH (a)-[r:INHERITS]->(b) WHERE (a.file_path ENDS WITH ".gd") OR (b.file_path ENDS WITH ".gd") RETURN count(r) AS gd_inherits_edges`
- `MATCH (m)-[r:IMPORTS]->(n) WHERE n.file_path ENDS WITH ".gd" RETURN count(r) AS gd_deps`

Raw outputs should be written one file per query in JSON form, and summary generation should consume those exact files.

### Required per-repo metadata
- repo path
- repo name
- current commit/ref
- Godot version if known/provided by the operator
- timestamp of the proof run
- whether the repo qualifies as a Godot 4.x validation target
- codebase-memory-mcp commit SHA / branch / worktree under test

### Godot version qualification
- A repo with confirmed Godot 4.x metadata may satisfy final issue #186 acceptance.
- A repo with unknown version may still be processed and reported, but it must be marked **non-qualifying** for final acceptance until a Godot 4.x version is confirmed.
- A repo confirmed as Godot 3.x must not count toward final issue #186 acceptance.

## 10) Acceptance Semantics

The aggregate proof passes when the collected evidence shows:

1. at least one validated **Godot 4.x** real repo with non-zero `.gd` file/class/method indexing
2. at least one validated real repo with non-zero signal-call evidence
3. at least one validated real repo with non-zero resolvable `.gd` dependency/import evidence
4. at least one validated real repo with non-zero `.gd` inheritance evidence

One repo may satisfy all four categories, but the workflow must also support the case where coverage is split across multiple repos.

If coverage is split across multiple repos, at least one of the contributing repos must be a confirmed Godot 4.x repo, and no repo with incomplete query collection may contribute to the aggregate pass decision.

## 11) Error Handling and Degradation

- Missing repo path: fail fast with a clear usage message.
- Non-git repo path: continue if indexing works, but record missing ref metadata clearly.
- Missing Godot version metadata: continue, but mark version as unknown and non-qualifying in the report.
- Query failure after successful indexing: mark that repo as incomplete and continue to the next repo where practical.
- Empty evidence for a category: treat as a validation gap, not as a script crash.

## 12) Risks and Mitigations

1. **Operator accidentally validates against mixed personal state**
   - Mitigate with isolated proof cache/config/store directories.
2. **Raw proof output is too noisy to review**
   - Mitigate by generating concise per-repo and aggregate summaries alongside raw outputs.
3. **No single repo demonstrates all required behaviors**
   - Mitigate by supporting multiple repos and aggregate coverage checks.
4. **Local proof outputs get committed accidentally**
   - Mitigate by adding a dedicated ignore rule for the proof artifact root.

## 13) Test and Verification Plan

### Script-level verification
- Verify the proof script accepts multiple repo paths.
- Verify it creates a timestamped artifact directory.
- Verify it records repo metadata and query outputs.
- Verify it produces an aggregate summary.

### Workflow verification
- Run the script against at least one real Godot repo during development.
- Confirm generated summaries match the raw query outputs.
- Confirm the ignored artifact directory remains untracked in git.
- Confirm the script exits non-zero when aggregate acceptance is not satisfied.

### Human-review verification
- Reviewer should be able to inspect the committed runbook and determine:
  - which commands are run
  - where outputs are written
  - what counts as successful proof
  - how to reproduce the result locally

## 14) Acceptance Criteria

This proof work is complete when:

- there is an in-repo runbook for reproducing real-project validation
- there is an in-repo script that generates local proof artifacts for one or more Godot repos
- generated proof artifacts stay local-only and are ignored by git
- the generated local report captures repo metadata, raw query outputs, and concise summaries
- the generated local report captures the codebase-memory-mcp commit/branch/worktree under test
- the workflow is capable of demonstrating the issue #186 real-project acceptance evidence across one or more repos
- aggregate acceptance requires at least one confirmed Godot 4.x repo

## 15) Follow-up (Explicitly Deferred)

- CI automation for proof runs against external repos
- automatic Godot version detection beyond simple metadata capture
- committing curated proof snapshots into the repository
