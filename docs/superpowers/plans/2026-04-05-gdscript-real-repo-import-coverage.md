# GDScript Real-Repo Import Coverage Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore real-repository GDScript `.gd` `IMPORTS` edges so the proof workflow reaches full acceptance, then finish the remaining merged-branch verification and landing steps.

**Architecture:** The current merged branch already passes the full C test suite and the proof harness itself works. The remaining blocker is narrow: real Godot repos index `.gd` files/classes/methods and even produce `.gd` `INHERITS`, but they still produce zero `.gd` `IMPORTS` edges. Fix this with TDD at the extractor level first, add one pipeline/parity regression so extracted imports cannot disappear between sequential and parallel indexing, then rerun the real-project proof workflow and finish the original integration plan.

**Tech Stack:** C11, tree-sitter AST walking, custom C test runner via `Makefile.cbm`, CLI graph queries via `build/codebase-memory-mcp` (the proof workflow standardizes on this path and creates the compatibility symlink if needed), Bash proof harness.

---

## Merge-state rule for this plan

This worktree is already in an active merge. Because of that, do **not** create interim commits for Tasks 1-4. Use `git add ...` plus `git status --short` as checkpoints while the merge remains open, then create exactly one final merge commit in Task 5.

## Current context to preserve

- Worktree: `.worktrees/gdscript-support-main-sync`
- Branch under test: `gdscript-support-main-sync`
- Current merged tree status before starting this plan:
  - `make -f Makefile.cbm cbm` passes
  - `make -f Makefile.cbm test` passes
  - no unresolved merge files remain
- Remaining blocker from the previous integration plan (`docs/superpowers/plans/2026-04-04-gdscript-origin-main-integration.md:398-515`): Task 6 real-project proof still fails `imports_coverage`
- Known proof evidence:
  - proof run with signal + inherits + imports candidates: `.artifacts/gdscript-proof/20260405T235011Z-46399-3VzjGb/aggregate-summary.md`
  - that run proves:
    - indexing coverage: pass
    - signal coverage: pass
    - inherits coverage: pass
    - imports coverage: fail
- Important diagnosis already established:
  - broad graph query on the `plugins` demo project returns **zero `IMPORTS` edges total**
  - `.gd` `Module` nodes do exist in that same graph
  - the most suspicious code path is nested GDScript `preload/load` handling in `internal/cbm/extract_imports.c`
  - secondary fallback suspicion is pipeline import-edge creation in:
    - `src/pipeline/pass_definitions.c:278-315`
    - `src/pipeline/pass_parallel.c:1346-1383`

## File structure and responsibilities

- `internal/cbm/extract_imports.c`
  - Primary fix target.
  - Responsible for extracting GDScript imports from AST shapes before pipeline edge creation.
  - Most relevant functions:
    - `gdscript_extract_load_path()` at `internal/cbm/extract_imports.c:137`
    - `gdscript_maybe_emit_import()` at `internal/cbm/extract_imports.c:151`
    - `gdscript_alias_from_statement_text()` at `internal/cbm/extract_imports.c:173`
    - `walk_gdscript_imports()` at `internal/cbm/extract_imports.c:1015`

- `tests/test_extraction.c`
  - Extractor-level regression coverage.
  - Existing relevant helpers/tests:
    - `has_import_exact()` at `tests/test_extraction.c:51`
    - `has_string_ref_value()` at `tests/test_extraction.c:63`
    - `gdscript_signal_calls_and_import_alias` at `tests/test_extraction.c:1974`
  - Extend this file rather than inventing new test helpers unless duplication becomes painful.

- `src/pipeline/pass_definitions.c`
  - Sequential `IMPORTS` edge creation.
  - Relevant functions:
    - `repo_file_exists()` at `src/pipeline/pass_definitions.c:100`
    - `create_import_edges_for_file()` at `src/pipeline/pass_definitions.c:278`

- `src/pipeline/pass_parallel.c`
  - Parallel `IMPORTS` edge creation parity.
  - Relevant functions:
    - `gdscript_repo_file_exists()` at `src/pipeline/pass_parallel.c:484`
    - parallel import edge creation block at `src/pipeline/pass_parallel.c:1359`

- `tests/test_pipeline.c`
  - Extend the existing `.gd` import coverage area rather than inventing a separate test shape if the current fixture can be reused.
  - Add one regression that proves a nested GDScript preload produces a sequential `IMPORTS` edge in the graph, not just an extractor entry.

- `tests/test_parallel.c`
  - Extend the existing GDScript parity coverage rather than duplicating setup.
  - Add one parity regression that the same nested GDScript preload produces matching sequential/parallel `IMPORTS` edges.

- `scripts/gdscript-proof.sh`
  - Do **not** change unless the proof harness itself is wrong. Right now it appears correct and is successfully exercising the CLI.

- `docs/superpowers/proofs/gdscript-real-project-validation.md`
  - Update only if the accepted proof repo set or proof instructions change.

## Repos to use for proof reruns

Use these exact local repos unless a better local candidate is discovered during implementation:

- signal coverage: `/Users/shaunmcmanus/Downloads/ShitsAndGiggles/codebase-memory-mcp/godot-demo-projects/3d/squash_the_creeps`
- inherits coverage: `/Users/shaunmcmanus/Downloads/ShitsAndGiggles/codebase-memory-mcp/godot-demo-projects/2d/finite_state_machine`
- imports candidate: `/Users/shaunmcmanus/Downloads/ShitsAndGiggles/codebase-memory-mcp/godot-demo-projects/plugins`

All three are confirmed Godot `4.6` projects from their `project.godot` files.

---

### Task 1: Lock the failing extractor regression for nested GDScript preload/load imports

**Files:**
- Modify: `internal/cbm/extract_imports.c:137-260,1015-1141`
- Modify: `tests/test_extraction.c:1974-2030`

- [ ] **Step 1: Add a failing extractor test for nested preload inside a call chain**

Add a new test near the existing GDScript extraction tests in `tests/test_extraction.c`:

```c
TEST(gdscript_nested_preload_call_chain_emits_import) {
    const char *src =
        "extends EditorPlugin\n"
        "func _enter_tree():\n"
        "    import_plugin = preload(\"import.gd\").new()\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "addons/simple_import_plugin/plugin.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_import_exact(r, NULL, "addons/simple_import_plugin/import.gd") ||
           has_import_exact(r, "import_plugin", "addons/simple_import_plugin/import.gd"));
    cbm_free_result(r);
    PASS();
}
```

- [ ] **Step 2: Add a failing extractor test for nested preload used as a function argument**

Add a second test right after it:

```c
TEST(gdscript_nested_preload_argument_emits_import) {
    const char *src =
        "extends EditorPlugin\n"
        "func handles(object):\n"
        "    return is_instance_of(object, preload(\"res://addons/main_screen/handled_by_main_screen.gd\"))\n";

    CBMFileResult *r = extract(src, CBM_LANG_GDSCRIPT, "t", "addons/main_screen/main_screen_plugin.gd");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT(has_import(r, "addons/main_screen/handled_by_main_screen.gd"));
    cbm_free_result(r);
    PASS();
}
```

- [ ] **Step 3: Run the test suite to verify the new regression fails first**

Run:

```bash
make -f Makefile.cbm test
```

Expected: FAIL in one or both new `gdscript_nested_preload_*` tests.

- [ ] **Step 4: Branch on what actually failed**

Decision rule:
- if the new extractor tests fail, fix the extractor first
- if the new extractor tests already pass, do **not** force extractor edits; move straight to Task 2 and treat the remaining bug as pipeline-side until proven otherwise

- [ ] **Step 5: Implement the minimal extractor fix only if Task 1 proved extraction is broken**

Target the nested-call path, not the proof script.

Required behavior:
- when the walker visits a nested `preload(...)` or `load(...)` node inside another call or chain, it must still recognize the inner global load call
- it must normalize relative and `res://` `.gd` paths correctly for the current `rel_path`
- it must push a `CBMImport` even when the load call is not the whole statement or assignment RHS

Keep the fix as small as possible. Prefer adjusting `walk_gdscript_imports()` and, only if necessary, `gdscript_extract_load_path()` or its callee-detection assumptions.

- [ ] **Step 6: Re-run the full test suite**

Run:

```bash
make -f Makefile.cbm test
```

Expected: PASS.

- [ ] **Step 7: Stage the extractor checkpoint without committing**

```bash
git add internal/cbm/extract_imports.c tests/test_extraction.c
git status --short
```

Expected: the extractor files are staged as part of the still-open merge; no merge commit is created yet.

---

### Task 2: Lock graph-edge regressions so extracted nested imports survive sequential indexing

**Files:**
- Modify: `src/pipeline/pass_definitions.c:278-315`
- Modify: `tests/test_pipeline.c`

- [ ] **Step 1: Extend the existing sequential `.gd` import regression instead of inventing a disconnected fixture**

Before writing new test code, inspect the existing `.gd` import coverage in `tests/test_pipeline.c` and extend that area if it already models the right pipeline harness.

- [ ] **Step 2: Add a failing sequential pipeline regression for nested preload/import edges**

Add a small two-file GDScript fixture inline in `tests/test_pipeline.c` that models a real nested preload case:

```c
// file A: addons/simple_import_plugin/plugin.gd
"extends EditorPlugin\n"
"func _enter_tree():\n"
"    import_plugin = preload(\"import.gd\").new()\n"

// file B: addons/simple_import_plugin/import.gd
"extends EditorImportPlugin\n"
```

Assert that the indexed graph contains an `IMPORTS` edge from the source file-side node (the current pipeline uses the source `__file__` node / file-side source) to the imported `.gd` module/file target, matching the existing `IMPORTS` edge shape already used elsewhere in pipeline tests.

- [ ] **Step 3: Run the full suite to prove the new regression fails first**

Run:

```bash
make -f Makefile.cbm test
```

Expected: FAIL in the new pipeline regression if extracted imports are still being dropped before edge creation.

- [ ] **Step 4: Fix sequential import edge creation only if the new regression proves it is needed**

Inspect `create_import_edges_for_file()` in `src/pipeline/pass_definitions.c`.

Allowed minimal fixes:
- adjust the `.gd` file existence gate if it rejects valid normalized repo-relative paths
- adjust source/target module lookup if nested preload imports are extracted but never resolved into nodes

Do **not** loosen path validation blindly. Keep the `.gd` guard if it still catches bogus edges.

- [ ] **Step 5: Re-run the full suite**

Run:

```bash
make -f Makefile.cbm test
```

Expected: PASS.

- [ ] **Step 6: Stage the sequential pipeline checkpoint without committing**

```bash
git add src/pipeline/pass_definitions.c tests/test_pipeline.c
git status --short
```

Expected: the sequential pipeline files are staged as part of the still-open merge; no merge commit is created yet.

---

### Task 3: Lock parallel parity for nested GDScript import edges

**Files:**
- Modify: `src/pipeline/pass_parallel.c:484,1359-1383`
- Modify: `tests/test_parallel.c`

- [ ] **Step 1: Extend the existing GDScript parallel parity coverage instead of duplicating setup**

Before adding code, inspect the existing GDScript parity block in `tests/test_parallel.c` and append the nested preload/import case there if possible.

- [ ] **Step 2: Add a failing sequential-vs-parallel parity regression**

In `tests/test_parallel.c`, add a test using the same two-file nested preload fixture as Task 2.

Assert:
- sequential indexing emits exactly one `.gd` `IMPORTS` edge
- parallel indexing emits exactly one matching `.gd` `IMPORTS` edge
- the source and target file paths match in both modes

- [ ] **Step 3: Run the full suite to confirm the parity regression fails first**

Run:

```bash
make -f Makefile.cbm test
```

Expected: FAIL in the new parallel regression if sequential and parallel import-edge handling still differ.

- [ ] **Step 4: Fix parallel import-edge handling minimally**

Only touch the parallel import path if the new regression demands it.

Keep the helper-based merged shape. Limit changes to the `.gd` import-edge creation block around:

- `gdscript_repo_file_exists()`
- the loop that converts `result->imports` into graph edges

- [ ] **Step 5: Re-run the full suite**

Run:

```bash
make -f Makefile.cbm test
```

Expected: PASS.

- [ ] **Step 6: Stage the parallel parity checkpoint without committing**

```bash
git add src/pipeline/pass_parallel.c tests/test_parallel.c
git status --short
```

Expected: the parallel pipeline files are staged as part of the still-open merge; no merge commit is created yet.

---

### Task 4: Re-run the proof workflow until `imports_coverage` passes

**Files:**
- Modify if needed: `scripts/gdscript-proof.sh:62-93,549-650,1110`
- Modify if needed: `docs/superpowers/proofs/gdscript-real-project-validation.md`

- [ ] **Step 1: Re-run the known failing proof combination**

Run:

```bash
bash scripts/gdscript-proof.sh \
  --repo /Users/shaunmcmanus/Downloads/ShitsAndGiggles/codebase-memory-mcp/godot-demo-projects/3d/squash_the_creeps \
  --godot-version /Users/shaunmcmanus/Downloads/ShitsAndGiggles/codebase-memory-mcp/godot-demo-projects/3d/squash_the_creeps=4.6 \
  --label /Users/shaunmcmanus/Downloads/ShitsAndGiggles/codebase-memory-mcp/godot-demo-projects/3d/squash_the_creeps=signals \
  --repo /Users/shaunmcmanus/Downloads/ShitsAndGiggles/codebase-memory-mcp/godot-demo-projects/2d/finite_state_machine \
  --godot-version /Users/shaunmcmanus/Downloads/ShitsAndGiggles/codebase-memory-mcp/godot-demo-projects/2d/finite_state_machine=4.6 \
  --label /Users/shaunmcmanus/Downloads/ShitsAndGiggles/codebase-memory-mcp/godot-demo-projects/2d/finite_state_machine=inherits \
  --repo /Users/shaunmcmanus/Downloads/ShitsAndGiggles/codebase-memory-mcp/godot-demo-projects/plugins \
  --godot-version /Users/shaunmcmanus/Downloads/ShitsAndGiggles/codebase-memory-mcp/godot-demo-projects/plugins=4.6 \
  --label /Users/shaunmcmanus/Downloads/ShitsAndGiggles/codebase-memory-mcp/godot-demo-projects/plugins=imports
```

Expected: final proof exits `0` and the new `aggregate-summary.md` reports all four coverage categories as pass.

- [ ] **Step 2: Read the aggregate summary immediately**

Read:

```text
.artifacts/gdscript-proof/<latest-run>/aggregate-summary.md
```

Expected:
- `Final acceptance: passed`
- `Missing coverage categories:` absent or empty
- `imports_coverage: pass`

- [ ] **Step 3: Only if proof still fails, debug with one `.gd`-filtered graph query before changing the proof harness**

Use the latest proof-run state from `env.txt` plus the `plugins` repo summary and run:

```bash
RUN_ROOT="$(python3 - <<'PY'
from pathlib import Path
roots = sorted(Path('.artifacts/gdscript-proof').glob('*'), key=lambda p: p.stat().st_mtime, reverse=True)
print(roots[0])
PY
)"
HOME="$(python3 - "$RUN_ROOT/env.txt" <<'PY'
import sys
for line in open(sys.argv[1]):
    if line.startswith('home='):
        print(line.split('=', 1)[1].strip())
        break
PY
)"
XDG_CONFIG_HOME="$(python3 - "$RUN_ROOT/env.txt" <<'PY'
import sys
for line in open(sys.argv[1]):
    if line.startswith('xdg_config_home='):
        print(line.split('=', 1)[1].strip())
        break
PY
)"
XDG_CACHE_HOME="$(python3 - "$RUN_ROOT/env.txt" <<'PY'
import sys
for line in open(sys.argv[1]):
    if line.startswith('xdg_cache_home='):
        print(line.split('=', 1)[1].strip())
        break
PY
)"
PLUGINS_SUMMARY="$(python3 - "$RUN_ROOT" <<'PY'
from pathlib import Path
import sys
run_root = Path(sys.argv[1])
for path in sorted(run_root.glob('*/summary.md')):
    text = path.read_text()
    if '/godot-demo-projects/plugins' in text:
        print(path)
        break
PY
)"
PROJECT_ID="$(python3 - "$PLUGINS_SUMMARY" <<'PY'
import sys
for line in open(sys.argv[1]):
    if line.startswith('- Resolved project ID: '):
        print(line.split('`')[1])
        break
PY
)"
HOME="$HOME" XDG_CONFIG_HOME="$XDG_CONFIG_HOME" XDG_CACHE_HOME="$XDG_CACHE_HOME" \
./build/codebase-memory-mcp cli query_graph '{"project":"'"$PROJECT_ID"'","query":"MATCH (m)-[r:IMPORTS]->(n) WHERE n.file_path ENDS WITH \".gd\" RETURN count(r) AS gd_deps","max_rows":5}'
```

Expected:
- if this returns `0`, the remaining bug is still in extraction/pipeline, not in the proof query
- if this returns `>0` while the saved `queries/gd-imports.json` still reports `0`, then and only then update `scripts/gdscript-proof.sh`

- [ ] **Step 4: Update proof script/docs only if the proof harness was actually wrong**

Allowed changes:
- widen the proof query only if the graph contains valid GDScript import edges that the current proof query misses
- document any continued `cli --raw` fallback if it is still required

Do **not** change proof docs just to match a broken graph.

- [ ] **Step 5: Re-run the proof workflow after any proof-harness follow-up**

Run the same command from Step 1 again.

Expected: PASS.

- [ ] **Step 6: Stage proof-harness/doc changes only if needed**

```bash
git add scripts/gdscript-proof.sh docs/superpowers/proofs/gdscript-real-project-validation.md
git status --short
```

If no harness/doc change was needed, skip this step.

---

### Task 5: Finish the remaining original integration-plan verification and landing steps

**Files:**
- Verify only: entire merged worktree
- Modify only if needed: final evidence notes / PR notes outside this plan

- [ ] **Step 1: Run the full repository suite one more time**

```bash
make -f Makefile.cbm test
```

Expected: PASS.

- [ ] **Step 2: Run the non-GDScript smoke index**

Use the path the user provided earlier:

```bash
./build/codebase-memory-mcp cli index_repository '{"repo_path":"./graph-ui","mode":"full"}'
```

Expected: PASS.

- [ ] **Step 3: Verify worktree status is clean except intended merge results**

```bash
git diff --name-only --diff-filter=U
git status --short
```

Expected:
- no unresolved merge files
- no stray proof artifacts staged

- [ ] **Step 4: Commit the final merge result**

```bash
git add .
git commit -m "merge: integrate origin/main into gdscript support"
```

- [ ] **Step 5: Fast-forward the original `gdscript-support` worktree to the validated merge commit**

Run from `.worktrees/gdscript-support`:

```bash
git merge --ff-only gdscript-support-main-sync
```

Expected: PASS.

- [ ] **Step 6: Record the final evidence for handoff/PR**

Write down:
- final merge commit SHA
- exact test command: `make -f Makefile.cbm test`
- final proof artifact root
- repos used for indexing/signal/imports/inherits coverage
- whether `scripts/gdscript-proof.sh` or proof docs changed

- [ ] **Step 7: Commit the evidence note only if the repo keeps a tracked note file for it**

If there is no tracked evidence file, do not invent one. Put the evidence in the PR description or handoff message instead.

---

## Stop conditions

Stop and ask for guidance instead of guessing if any of these happen:

- extractor-level tests pass but real proof still shows zero `IMPORTS` edges everywhere
- sequential pipeline fix works but parallel parity fix requires broader refactoring than the import-edge block
- proof rerun shows graph `IMPORTS` edges exist but `scripts/gdscript-proof.sh` still reports `0`
- finishing verification fails on the non-GDScript smoke index

## Expected commit sequence

Because this is an active merge, the expected commit sequence is just:

1. `merge: integrate origin/main into gdscript support`

Record the Task 1-4 checkpoints with staged-file evidence and green verification output instead of interim commits.
