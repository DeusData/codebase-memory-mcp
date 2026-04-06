# GDScript Origin/Main Integration Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring `gdscript-support` forward onto the newest `origin/main` without losing GDScript indexing, parallel-path parity, or the real-project proof workflow.

**Architecture:** Do this in a throwaway integration worktree first, and prefer merging `origin/main` into the branch before considering any history cleanup. Resolve low-risk text/config conflicts first, then treat extractor core and pipeline parity as one coordinated subsystem, then restitch tests, and only after that revalidate the proof workflow against real Godot repos.

**Tech Stack:** git worktrees, git merge, C source/build/test flow via `Makefile.cbm`, existing GDScript extractor/pipeline code, bash proof workflow, Markdown docs.

> **Status backfill (2026-04-05):** This checklist was retroactively marked complete from the final merged `gdscript-support` state to avoid repeating already-landed work. The final merge commit is `58b67e4`; fresh verification was re-run from the current worktree (`make -f Makefile.cbm cbm`, `make -f Makefile.cbm test`, one non-GDScript smoke index, and a proof rerun rooted at `.artifacts/gdscript-proof/20260406T065651Z-23813-BZT9ZY`). Transient pre-commit checkpoints were backfilled from the landed merge/result history rather than a still-open merge state.

---

## Source Context

- Current feature branch: `gdscript-support`
- Current feature head when this plan was written: `2ae0854`
- Current upstream head when this plan was written: `origin/main` at `1d30971`
- Merge base when this plan was written: `148d951`
- Ahead/behind snapshot when this plan was written: `46 ahead / 5 behind`

### Highest-risk overlap files

- `.gitignore`
- `README.md`
- `internal/cbm/cbm.h`
- `internal/cbm/extract_calls.c`
- `internal/cbm/extract_defs.c`
- `internal/cbm/extract_imports.c`
- `internal/cbm/extract_type_assigns.c`
- `internal/cbm/extract_unified.c`
- `internal/cbm/helpers.c`
- `src/discover/language.c`
- `src/discover/userconfig.c`
- `src/pipeline/pass_calls.c`
- `src/pipeline/pass_definitions.c`
- `src/pipeline/pass_parallel.c`
- `src/pipeline/pass_semantic.c`
- `src/pipeline/registry.c`
- `tests/test_integration.c`
- `tests/test_parallel.c`
- `tests/test_pipeline.c`
- `tests/test_registry.c`

### Branch-only additions that must survive the integration

- `scripts/gdscript-proof.sh`
- `docs/superpowers/proofs/gdscript-real-project-validation.md`
- `docs/superpowers/specs/2026-04-04-gdscript-real-project-proof-design.md`
- `docs/superpowers/plans/2026-04-04-gdscript-real-project-proof.md`
- `internal/cbm/grammar_gdscript.c`
- `internal/cbm/vendored/grammars/gdscript/parser.c`
- `internal/cbm/vendored/grammars/gdscript/scanner.c`
- `internal/cbm/vendored/grammars/gdscript/tree_sitter/parser.h`
- `tests/fixtures/gdscript/min_project/actors/*`

---

## File Structure (Planned Touchpoints)

### Integration workspace and merge state
- Verify only: `.worktrees/gdscript-support-main-sync/`

### Low-risk overlap resolution
- Modify: `.gitignore`
- Modify: `README.md`
- Modify: `internal/cbm/cbm.h`
- Modify: `src/discover/language.c`
- Modify: `src/discover/userconfig.c`

### Extractor-core reconciliation
- Modify: `internal/cbm/extract_calls.c`
- Modify: `internal/cbm/extract_defs.c`
- Modify: `internal/cbm/extract_imports.c`
- Modify: `internal/cbm/extract_type_assigns.c`
- Modify: `internal/cbm/extract_unified.c`
- Modify: `internal/cbm/helpers.c`
- Modify if needed: `internal/cbm/helpers.h`
- Verify: `internal/cbm/grammar_gdscript.c`
- Verify: `internal/cbm/vendored/grammars/gdscript/*`

### Pipeline and registry reconciliation
- Modify: `src/pipeline/pass_calls.c`
- Modify: `src/pipeline/pass_definitions.c`
- Modify: `src/pipeline/pass_parallel.c`
- Modify: `src/pipeline/pass_semantic.c`
- Modify: `src/pipeline/registry.c`

### Test restitching
- Modify: `tests/test_integration.c`
- Modify: `tests/test_parallel.c`
- Modify: `tests/test_pipeline.c`
- Modify: `tests/test_registry.c`
- Verify: `tests/test_extraction.c`
- Verify: `tests/test_language.c`
- Verify: `tests/test_userconfig.c`
- Verify: `tests/fixtures/gdscript/min_project/actors/*`

### Proof workflow validation against merged CLI/MCP behavior
- Modify if needed: `scripts/gdscript-proof.sh`
- Modify if needed: `docs/superpowers/proofs/gdscript-real-project-validation.md`

---

## Chunk 1: Create the integration workspace and land the merge

### Task 1: Create a throwaway integration worktree

**Files:**
- Verify only: `.worktrees/gdscript-support-main-sync/`

- [x] **Step 1: Create the integration worktree from the current branch**

Run from the repository root (not from inside `.worktrees/gdscript-support`):

```bash
git worktree add -b gdscript-support-main-sync .worktrees/gdscript-support-main-sync gdscript-support
```

Expected:
- new worktree exists at `.worktrees/gdscript-support-main-sync`
- branch `gdscript-support-main-sync` starts from the current `gdscript-support` tip

- [x] **Step 2: Fetch the newest upstream state**

Run:

```bash
git fetch origin main
```

Expected: `origin/main` is current.

- [x] **Step 3: Capture the baseline divergence inside the integration worktree**

Run in `.worktrees/gdscript-support-main-sync`:

```bash
git rev-parse --short HEAD
git rev-parse --short origin/main
git merge-base HEAD origin/main
git rev-list --left-right --count origin/main...HEAD
git diff --name-only "$(git merge-base HEAD origin/main)"..HEAD
git diff --name-only "$(git merge-base HEAD origin/main)"..origin/main
```

Expected: the commands confirm the same overlap class called out in this plan before any merge resolution starts.

- [x] **Step 4: Verify the clean setup checkpoint**

```bash
git status --short
```

Expected: clean before starting the merge.

### Task 2: Merge `origin/main` and triage conflict groups

**Files:**
- Modify: overlap files listed in this plan

- [x] **Step 1: Merge upstream into the integration branch**

Run in `.worktrees/gdscript-support-main-sync`:

```bash
git merge --no-commit origin/main
```

Expected:
- either a clean merge or a conflicted merge state
- no attempt to rebase the 46 branch commits at this stage
- no final merge commit is created yet

- [x] **Step 2: Record the pending merge file set before editing**

Run:

```bash
git status --short
```

Expected: any conflicts or pending merge edits fall mostly into low-risk text/config, extractor core, pipeline, and tests.

- [x] **Step 3: Resolve the low-risk overlap group first**

Resolve these files together:

- `.gitignore`
- `README.md`
- `internal/cbm/cbm.h`
- `src/discover/language.c`
- `src/discover/userconfig.c`

Preserve on our side:
- `.artifacts/gdscript-proof/` ignore rule
- GDScript language registration/detection
- any user-config behavior required by current GDScript tests and proof workflow

Preserve from upstream:
- new config/cache path behavior
- any doc corrections needed to match new upstream CLI behavior
- any non-GDScript fixes already landed on `origin/main`

- [x] **Step 4: Verify the low-risk group compiles at least to parser stage**

Run:

```bash
make -f Makefile.cbm cbm
```

Expected: if it still fails, the failure should now point at extractor or pipeline conflicts rather than trivial text/config conflicts.

- [x] **Step 5: Stage and checkpoint the low-risk resolution group**

```bash
git add .gitignore README.md internal/cbm/cbm.h src/discover/language.c src/discover/userconfig.c
git status --short
```

Expected: those files are staged as resolved while the overall merge remains uncommitted.

---

## Chunk 2: Reconcile extractor core and pipeline parity

### Task 3: Reconcile extractor-core overlaps without losing GDScript behavior

**Files:**
- Modify: `internal/cbm/extract_calls.c`
- Modify: `internal/cbm/extract_defs.c`
- Modify: `internal/cbm/extract_imports.c`
- Modify: `internal/cbm/extract_type_assigns.c`
- Modify: `internal/cbm/extract_unified.c`
- Modify: `internal/cbm/helpers.c`
- Modify if needed: `internal/cbm/helpers.h`
- Verify: `internal/cbm/grammar_gdscript.c`
- Verify: `internal/cbm/vendored/grammars/gdscript/*`
- Test: `tests/test_extraction.c`

- [x] **Step 1: Add a failing extractor build check**

Run:

```bash
make -f Makefile.cbm cbm
```

Expected: FAIL before extractor conflicts are fully resolved.

- [x] **Step 2: Merge upstream refactors into the extractor core**

For each file, preserve these branch requirements while adopting upstream structure changes:

- `extract_defs.c`: GDScript class/method discovery, nameless-script handling, correct anchors
- `extract_calls.c`: GDScript signal-call extraction semantics
- `extract_imports.c`: `.gd` import/preload/load edge extraction
- `extract_type_assigns.c` / `extract_unified.c` / `helpers.c`: any helper wiring required by the GDScript grammar and extractor dispatch

Do not hand-wave this as “take ours” or “take theirs”; port the GDScript behavior into the upstream refactor shape.

- [x] **Step 3: Verify the extractor build now passes**

Run:

```bash
make -f Makefile.cbm cbm
```

Expected: PASS.

- [x] **Step 4: Run a progress-check full suite after extractor reconciliation**

Run:

```bash
make -f Makefile.cbm test
```

Expected: if failures remain, they should now be in later merge layers such as pipeline/test reconciliation rather than extractor compile failures.

- [x] **Step 5: Stage and checkpoint the extractor-core resolution**

```bash
git add internal/cbm/extract_calls.c internal/cbm/extract_defs.c internal/cbm/extract_imports.c internal/cbm/extract_type_assigns.c internal/cbm/extract_unified.c internal/cbm/helpers.c internal/cbm/helpers.h internal/cbm/grammar_gdscript.c internal/cbm/vendored/grammars/gdscript/parser.c internal/cbm/vendored/grammars/gdscript/scanner.c internal/cbm/vendored/grammars/gdscript/tree_sitter/parser.h tests/test_extraction.c
git status --short
```

Expected: extractor-core files are staged as resolved while the merge is still in progress.

### Task 4: Reconcile pipeline parity and registry behavior

**Files:**
- Modify: `src/pipeline/pass_calls.c`
- Modify: `src/pipeline/pass_definitions.c`
- Modify: `src/pipeline/pass_parallel.c`
- Modify: `src/pipeline/pass_semantic.c`
- Modify: `src/pipeline/registry.c`
- Test: `tests/test_parallel.c`
- Test: `tests/test_pipeline.c`
- Test: `tests/test_registry.c`

- [x] **Step 1: Add the failing pipeline/parity progress check**

Run:

```bash
make -f Makefile.cbm test
```

Expected: FAIL before the pipeline conflicts are fully restitched.

- [x] **Step 2: Reconcile sequential and parallel GDScript behavior on top of upstream**

Preserve these branch guarantees:

- sequential and parallel paths emit matching `CALLS`, `INHERITS`, `IMPORTS`, and `DEFINES_METHOD` edges for GDScript
- registry wiring still exposes GDScript in the merged build
- semantic pass still treats built-in vs file-backed inheritance correctly

Preserve from upstream:

- any pipeline scheduling, registry, or traversal fixes shipped after the merge base

`src/pipeline/pass_parallel.c` is the highest-risk file in the merge. Treat it as the primary integration checkpoint, not a routine conflict cleanup.

- [x] **Step 3: Re-run the pipeline/parity progress check**

Run:

```bash
make -f Makefile.cbm test
```

Expected: if failures remain, they should now be limited to the integration/test restitching work called out in Task 5, not unresolved pipeline compile/link problems.

- [x] **Step 4: Stage and checkpoint the pipeline and registry resolution**

```bash
git add src/pipeline/pass_calls.c src/pipeline/pass_definitions.c src/pipeline/pass_parallel.c src/pipeline/pass_semantic.c src/pipeline/registry.c tests/test_parallel.c tests/test_pipeline.c tests/test_registry.c
git status --short
```

Expected: pipeline and registry files are staged as resolved while the merge is still uncommitted.

---

## Chunk 3: Restitch tests and revalidate the proof workflow

### Task 5: Restitch integration tests around the merged implementation

**Files:**
- Modify: `tests/test_integration.c`
- Verify: `tests/test_language.c`
- Verify: `tests/test_userconfig.c`
- Verify: `tests/fixtures/gdscript/min_project/actors/*`

- [x] **Step 1: Add the failing full-suite gate before final test restitching**

Run:

```bash
make -f Makefile.cbm test
```

Expected: FAIL until the merged behavior and tests agree.

- [x] **Step 2: Update the integration tests to match the merged pipeline behavior**

Preserve these expectations:

- non-zero `.gd` discovery counts
- resolved signal `CALLS`
- correct `.gd` `INHERITS`
- correct `.gd` `IMPORTS`
- built-in base classes do not create incorrect inherits edges
- nameless script anchor behavior still works

- [x] **Step 3: Re-run the full-suite gate after test restitching**

Run:

```bash
make -f Makefile.cbm test
```

Expected: PASS.

- [x] **Step 4: Stage and checkpoint the test restitching**

```bash
git add tests/test_integration.c tests/test_language.c tests/test_userconfig.c tests/fixtures/gdscript/min_project/actors
git status --short
```

Expected: test updates are staged as resolved while the merge is still uncommitted.

### Task 6: Revalidate the proof workflow against the merged CLI/MCP surface

**Files:**
- Modify if needed: `scripts/gdscript-proof.sh`
- Modify if needed: `docs/superpowers/proofs/gdscript-real-project-validation.md`

- [x] **Step 1: Add the failing proof-workflow check**

Run:

```bash
bash scripts/gdscript-proof.sh \
  --repo /absolute/path/to/real-godot-repo \
  --godot-version /absolute/path/to/real-godot-repo=4.2
```

Expected: if upstream CLI/MCP behavior drifted, this reveals it now.

- [x] **Step 2: Reconcile the proof script with merged CLI/MCP behavior**

Check specifically:

- `index_repository`
- `list_projects`
- `query_graph`
- `cli --raw` handling and fallback behavior
- any environment/config path behavior changed by upstream

If the merged CLI still requires fallback, keep the fallback transparent in artifacts and docs.

- [x] **Step 3: Re-run the documented proof examples**

Run:

```bash
bash scripts/gdscript-proof.sh \
  --repo /absolute/path/to/repo-a --godot-version /absolute/path/to/repo-a=4.2 \
  --repo /absolute/path/to/repo-b --godot-version /absolute/path/to/repo-b=4.2 \
  --repo /absolute/path/to/repo-c --godot-version /absolute/path/to/repo-c=4.2
```

Expected:
- per-repo `summary.md` files exist
- `aggregate-summary.md` exists
- final run exits `0` once the chosen repos cover indexing, signal, imports, and inherits

- [x] **Step 4: Stage proof workflow follow-up changes only if needed**

```bash
git add scripts/gdscript-proof.sh docs/superpowers/proofs/gdscript-real-project-validation.md
git status --short
```

Expected: proof workflow changes are staged only if upstream CLI/MCP behavior forced follow-up edits.

### Task 7: Full verification on the merged branch

**Files:**
- Verify only: entire merged worktree

- [x] **Step 1: Run the full repository test suite**

Run:

```bash
make -f Makefile.cbm test
```

Expected: PASS.

- [x] **Step 2: Run one non-GDScript smoke index**

Run:

```bash
./build/c/codebase-memory-mcp cli index_repository '{"repo_path":"/absolute/path/to/small-non-gd-repo","mode":"full"}'
```

Expected: PASS. This guards against accidentally fixing GDScript while breaking broader indexing.

- [x] **Step 3: Verify the worktree is clean except intended merge results**

Run:

```bash
git status --short
```

Expected: no stray proof artifacts and no unresolved merge state.

- [x] **Step 4: Commit the final merge result**

```bash
git add .
git commit -m "merge: integrate origin/main into gdscript support"
```

- [x] **Step 5: Land the validated result back onto `gdscript-support`**

Run from the original `gdscript-support` worktree after the integration branch has a final merge commit:

```bash
git merge --ff-only gdscript-support-main-sync
```

Expected: `gdscript-support` now points at the validated integration commit without replaying the merge work a second time.

- [x] **Step 6: Record the final evidence**

Write down for the eventual PR/update:

- final merge commit SHA
- full test command used
- proof run artifact root path(s)
- repo path(s) used for proof coverage
- which repo satisfied indexing, signal, imports, and inherits coverage

Expected: the branch is ready for review against the updated upstream base.
