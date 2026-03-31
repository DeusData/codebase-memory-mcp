# Design Spec: GDScript Support for codebase-memory-mcp

Date: 2026-03-30  
Issue: https://github.com/DeusData/codebase-memory-mcp/issues/186

## 1) Objective

Add first-class **Godot 4.x GDScript (`.gd`)** support to the indexing pipeline so real Godot projects produce useful graph output (definitions, calls, inheritance, resource dependencies, and signal-related behavior) without introducing new graph schema types.

## 2) Scope and Constraints

### In scope
- `.gd` language detection and naming
- Tree-sitter grammar vendoring and runtime wiring
- Extraction support for:
  - classes (`class_name`, class declarations)
  - functions (`func`, `static func`, common lifecycle methods)
  - signal declarations and signal usage patterns (`connect`, `emit`, `emit_signal`)
  - inheritance (`extends`)
  - resource dependencies (`preload`, `load`)
  - exported vars (`@export var`)
- Automated tests and real-project validation evidence

### Out of scope
- New edge types (no `EMITS`/`CONNECTS_TO` in this issue)
- Graph schema migrations
- Godot 3.x compatibility guarantees

### Hard constraints
- Target **Godot 4.x only**
- Reuse existing graph model and extraction architecture
- Include **real-project indexing proof** as acceptance evidence

## 3) Selected Approach

Chosen approach: **Parser wiring + targeted GDScript semantic hooks**.

Rationale:
- Delivers issue intent in one pass
- Avoids schema churn
- Preserves existing MCP tool compatibility
- Improves practical Godot traceability over parser-only onboarding

## 4) Architecture and Module Boundaries

### A. Language onboarding
- Add `CBM_LANG_GDSCRIPT` to language enum and detection tables.
- Ensure extension and language-name resolution works in both built-in and user override paths.

### B. Parser integration
- Vendor `tree-sitter-gdscript` in vendored grammar tree.
- Register `tree_sitter_gdscript()` in parser dispatch.
- Add `CBMLangSpec` entry for node-kind sets used by existing extractors.

### C. Semantics via existing extractors
- Keep extraction flow unchanged:
  1. definitions
  2. imports/dependencies
  3. unified pass (calls/usages/vars/branches/etc.)
- Add GDScript node-kind mappings and targeted handling where generic extraction is insufficient.

### D. Graph-model compatibility
- Encode signals/resources/inheritance using existing node/edge semantics.
- No new edge labels in this issue.

## 5) Planned File-Level Changes

## Discovery and language registration
- `internal/cbm/cbm.h`
  - Add `CBM_LANG_GDSCRIPT` to `CBMLanguage` enum.
- `src/discover/language.c`
  - Add `.gd` mapping in extension table.
  - Add `"GDScript"` to human-readable language names.
- `src/discover/userconfig.c`
  - Add `"gdscript"` language alias for user config overrides.

## Grammar + parser dispatch
- `internal/cbm/vendored/grammars/gdscript/*`
  - Add generated parser/scanner/runtime files required by tree-sitter-gdscript.
- `internal/cbm/grammar_gdscript.c`
  - Add grammar shim file consistent with existing `grammar_*.c` wrappers.
- `internal/cbm/lang_specs.c`
  - Add extern declaration for `tree_sitter_gdscript()`.
  - Add parser switch case in `cbm_ts_language()`.
  - Add `CBMLangSpec` row with node-kind arrays for defs/calls/imports/branches/vars/assigns.

## Extraction behavior alignment
- `internal/cbm/lang_specs.c`
  - Define GDScript node-kind arrays (functions, classes/modules, calls, resource-load/import-like nodes, branches, vars, assigns).
- `internal/cbm/helpers.c`
  - Add function-kind and module-level parent-kind mappings for GDScript.
  - Add shared script-anchor QN helper and ensure enclosing-function QN generation uses anchor-based naming for GDScript top-level methods.
- `internal/cbm/extract_defs.c`
  - Add GDScript-specific handling for top-level script members so `func`/`var`/`@export var`/`signal` are always attached to the per-file script anchor (with `class_name` as alias metadata when present).
  - Add `extends_statement` handling to populate base-class metadata for downstream `INHERITS` edges.
- `internal/cbm/extract_unified.c`
  - Update scope tracking / enclosing-func resolution to reuse the same GDScript script-anchor helper used by definitions, preventing QN drift between defs vs calls/usages.
- `internal/cbm/extract_calls.c`
  - Add robust member-call callee extraction for `.connect()`, `.emit()`, and `emit_signal(...)` patterns (including receiver/member normalization where feasible).
- `internal/cbm/extract_imports.c`
  - Add GDScript import/dependency extraction dispatch (`preload`/`load` patterns), using generic helpers when possible.

## Resolver and semantic-edge alignment
- `src/pipeline/registry.c`
  - Add resolution support for exact-QN signal targets (`<script_qn>.signal.<name>`) and path-normalized script inheritance targets.
  - Add same-script-anchor fallback resolution for ordinary in-file calls (`foo()` resolves to `<script_anchor_qn>.foo` when applicable).
- `src/pipeline/pass_calls.c`
  - Ensure resolved signal call targets survive call-edge persistence via existing resolved-callee pathways.
- `src/pipeline/pass_semantic.c`
  - Ensure `extends` targets normalized from `.gd` paths can resolve into class/script anchors for `INHERITS` edge creation.
- `src/pipeline/pass_parallel.c`
  - Mirror the same GDScript call/inheritance/dependency resolution behavior used by sequential passes so large-index production path has parity.

## Tests + docs
- `tests/test_language.c`
  - Add extension/language detection tests for `.gd`.
- `tests/test_userconfig.c`
  - Add `gdscript` override-name mapping tests.
- `tests/test_extraction.c`
  - Add GDScript extraction tests for class/function/signal/extends/preload-load/export patterns.
- Pipeline/integration test (existing test suite location)
  - Add tiny Godot fixture repo assertions for cross-pass graph persistence behavior.
- `README.md` (if language list is documented)
  - Add GDScript to supported languages.

## 6) Data and Control Flow

1. `cbm_discover` identifies `.gd` files and assigns `CBM_LANG_GDSCRIPT`.
2. Pipeline passes include `.gd` files naturally.
3. `cbm_extract_file(..., CBM_LANG_GDSCRIPT, ...)` resolves:
   - `cbm_lang_spec(CBM_LANG_GDSCRIPT)`
   - `cbm_ts_language(CBM_LANG_GDSCRIPT)`
4. Existing extraction phases execute:
   - definitions
   - imports/dependencies
   - unified walk for calls/usages/vars/branches
5. Graph writer persists results as existing node/edge types.
6. Existing MCP tools surface GDScript data without protocol/tool changes.

## 7) Semantic Mapping Rules (Godot-focused)

- `class_name` / class constructs → existing Class/Module definition channels
- **Script anchor model (v1, explicit):**
  - every `.gd` file gets a synthetic script Class anchor (one per file), even when `class_name` is absent.
  - QN contract:
    - base = `cbm_pipeline_fqn_module(project, <repo_rel_path_to_file.gd>)`
    - if `class_name Foo` exists, anchor QN = `<base>.Foo`.
    - otherwise anchor QN = `<base>.__script__` (deterministic fallback).
  - this makes class-name-based resolution (`extends Foo`) compatible with current registry last-segment indexing behavior.
- `class_name_statement` + script root:
  - mapped to the script anchor so top-level members are always associated with a class context.
- `extends_statement`:
  - populate class/base metadata used by existing `INHERITS` generation.
- top-level `func` / `static func` / lifecycle funcs:
  - emitted as callable definitions attached as `Method` with `parent_class` = script anchor.
- top-level `var` and `@export var`:
  - emitted via existing variable-definition channels.
- `signal <name>`:
  - represented as existing definition node type (Function label in v1) with deterministic naming (`<script_qn>.signal.<name>`) to make queries and call-usage joins stable.
- `.connect(...)`, `.emit(...)`, `emit_signal(...)`:
  - emitted as `CALLS` via existing call extraction; signal name retained in captured call args where string literal/identifier is available.
  - **resolution rule (required for persisted edges):** when signal name is statically known, resolve callsite to `<script_qn>.signal.<name>` (same-file by default) so call edges survive current unresolved-call filtering.
  - for static receiver targets resolvable to local script classes, resolve to that target script/class signal QN; otherwise keep as unresolved metadata only.
- `preload("...")` / `load("...")`:
  - treated as dependency/import-like references with conservative normalization rules (below).

### Inheritance target scope (v1)
- Repo-local/script-resolvable inheritance is in scope for graph `INHERITS` edges.
- Built-in Godot engine classes (`Node`, `Node2D`, etc.) are represented as unresolved external base names in metadata only in v1 (no synthetic external class nodes).

### Resource dependency normalization (v1)
- Normalize only resolvable script targets to graph dependencies:
  - `res://.../*.gd` and relative `*.gd` paths become import/dependency links when they resolve to repo files.
- Normalization output must be repo-relative module paths compatible with `cbm_pipeline_fqn_module()` / registry resolution.
- Non-code assets (`.tscn`, `.tres`, textures, etc.) are not turned into module edges in v1.
  - They are retained as string references (existing string-ref channels) and explicitly deferred for richer asset graph support.
- Unresolvable runtime-computed paths stay as string refs only (no speculative edges).

### preload/load alias extraction (required for resolver usefulness)
- Capture assignment aliases for resource loads:
  - `const Foo = preload("res://foo.gd")`
  - `var Foo = load("res://foo.gd")`
- Store alias (`Foo`) as import/dependency `local_name`, and normalized repo-relative `.gd` path as `module_path`.
- Feed these pairs into existing import-map-based call/inheritance resolution paths.
- For class-context resolution (`extends Foo`), lift alias-resolved `.gd` module targets to their script-anchor `Class` QN before `INHERITS` persistence.
  - Promotion point (single place): perform module→script-anchor class lift in semantic inheritance resolution path before `INHERITS` edge write.

### Attachment semantics (v1)
- Method attachment is structural via existing `Method.parent_class` + `DEFINES_METHOD` pipeline behavior.
- Signal attachment is query-level by deterministic signal QN prefix under the script anchor (`<script_anchor_qn>.signal.<name>`), not a new edge type.
- Export var attachment remains metadata-level in existing variable channels (no new relation type in v1).

## 8) Error Handling and Degradation

- Missing grammar wiring: file marked extraction error (`has_error`) and indexing continues.
- Parse failure on malformed `.gd`: file-local failure only; no pipeline abort.
- Partial node-kind mismatch due grammar evolution: retain generic extraction fallback where possible.

## 9) Risks and Mitigations

1. **Enum/table misalignment risk**
   - Mitigate with tests that validate extension→enum→parser dispatch consistency.
2. **Grammar drift risk**
   - Pin vendored grammar revision and cover core Godot constructs in fixtures.
3. **Signal over/under-linking risk**
   - Start conservative (obvious connect/emit patterns) to reduce false positives.

## 10) Test and Verification Plan

### Automated
- Language detection tests:
  - `.gd` extension resolves to `CBM_LANG_GDSCRIPT`
  - human-readable language name is stable
- Extraction tests:
  - class/function definitions
  - `extends`
  - signal declaration + emit/connect forms
  - preload/load dependency capture
  - export var capture
- End-to-end smoke fixture:
  - small Godot-like project indexed through full pipeline

### Real-project acceptance (required)
- Run indexing on a real Godot 4.x repository.
- Capture evidence:
  - non-zero `.gd` files indexed
  - expected function/class nodes present
  - signal-related traces visible via existing queries/tools
  - preload/load `.gd` dependencies represented in graph when resolvable

### Concrete verification queries/assertions
- `DEFINES_METHOD` assertions for script functions under a class/script context.
- `INHERITS` assertions for `extends` on fixture scripts.
- `CALLS` assertions for `connect`/`emit`/`emit_signal` occurrences where signal names are statically resolvable.
- Dependency assertions:
  - positive for resolvable `.gd` preload/load targets
  - negative (string-ref only) for non-code assets.

## 11) Acceptance Criteria

Issue #186 is considered complete when:
- `.gd` files are discovered and parsed in normal indexing flow.
- GDScript definitions/calls/inheritance/resource dependencies are queryable.
- Signal patterns are represented via existing graph constructs.
- Sequential and parallel pipeline modes produce equivalent GDScript resolution behavior for core fixtures.
- Automated tests pass with new GDScript coverage.
- Real-project Godot 4.x indexing proof is provided.

## 13) Deterministic Script Anchor Contract (v1)

- Exactly one synthetic script `Class` node is emitted per `.gd` file before member definitions are persisted.
- Anchor QN is deterministic from project + repo-relative script path, with naming contract:
  - uses `class_name` as terminal segment when present,
  - otherwise uses deterministic file-based fallback terminal segment.
- Top-level `func` definitions become `Method` with `parent_class = script_anchor_qn`.
- `class_name` does not create a second class node; it names the single anchor in v1.

## 12) Follow-up (Explicitly Deferred)

- Dedicated signal edge types if needed after usage feedback
- Godot 3.x compatibility layer
- Additional Godot semantic enrichments (scene tree conventions, autoload-specific inference heuristics)
