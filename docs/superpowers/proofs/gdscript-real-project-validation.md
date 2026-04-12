# GDScript Real-Project Validation Runbook

Related issue: https://github.com/DeusData/codebase-memory-mcp/issues/186

## Purpose

Use this runbook to generate reproducible, local-only proof for GDScript support from the `gdscript-support` worktree.

The workflow:
- builds and uses the local `build/c/codebase-memory-mcp` binary under test,
- writes all runtime state and proof artifacts under `.artifacts/gdscript-proof/`,
- supports one or more local Godot repos in a single run,
- can run in legacy coarse-coverage mode or manifest-driven good-tier mode,
- runs manifest-mode proof targets in both `CBM_FORCE_PIPELINE_MODE=sequential` and `CBM_FORCE_PIPELINE_MODE=parallel`,
- produces per-repo `summary.md` files plus one `aggregate-summary.md`, and
- writes additive run-level `run-index.json`, `semantic-parity.json`, and `semantic-parity.md` artifacts so operators can discover repo metadata, isolated state roots, wrapper paths, and sequential-vs-parallel semantic evidence without manual database inspection.

The committed good-tier manifest currently tracks 4 pinned proof targets across 2 remotes. When a single upstream repo hosts multiple proof targets, the manifest records a `project_subpath` to keep the target reproducible.

Do not commit generated proof artifacts.

## Prerequisites

- Work from the `gdscript-support` worktree.
- Have one or more local Godot repositories available by path. The recommended local checkout root is `.artifacts/gdscript-proof/repos/`.
- Have the local build prerequisites needed for `make -f Makefile.cbm cbm`.
- For manifest mode, have a committed manifest such as `docs/superpowers/proofs/gdscript-good-tier-manifest.json`.

Runtime state is isolated per repo under the run root with per-repo `HOME`, `XDG_CONFIG_HOME`, and `XDG_CACHE_HOME`.

## Accepted arguments

```bash
bash scripts/gdscript-proof.sh \
  [--manifest docs/superpowers/proofs/gdscript-good-tier-manifest.json] \
  --repo /abs/path/to/repo \
  [--repo /abs/path/to/repo2 ...] \
  [--godot-version REPO=4.x] \
  [--label REPO=name]
```

Rules:
- `--repo` is required at least once.
- `--repo` may be repeated.
- `--label` and `--godot-version` use `REPO=value` form.
- `--label` and `--godot-version` must reference a repo path already declared with `--repo`.
- Repo and manifest paths are canonicalized to absolute paths.
- In manifest mode, every repo should have a label and every label must exist in the manifest.
- In manifest mode, missing `--godot-version` metadata is filled from the manifest when available.

## Manifest mode contract

Manifest mode through `scripts/gdscript-proof.sh` is the only approval-bearing workflow for v1 good-tier proof. The manifest defines:
- the curated repo labels,
- the optional `project_subpath` for proof targets that share an upstream repo,
- pinned commits,
- canonical approval metadata,
- per-repo `required_for` categories,
- gating assertions,
- informational assertions,
- minimum repo count.

For approval-bearing manifest entries, canonical target identity is:

- `remote`
- `pinned_commit`
- `project_subpath` when one upstream repo hosts multiple proof targets
- recorded `godot_version`

Additional identity rules:

- `label` is readability metadata only; it is not canonical identity.
- Local checkout path is run evidence only; it changes per machine/run and is not part of target identity.
- Only entries with explicitly recorded Godot `4.x` metadata qualify for v1 approval.
- Ad hoc `--repo` runs remain allowed for debugging/investigation, but they are non-canonical and non-qualifying unless they are executed through the committed manifest lane.

The script compares actual results to the manifest and classifies:
- each assertion as `pass`, `fail`, or `incomplete`,
- each repo as `pass`, `fail`, or `incomplete`,
- the aggregate run as `pass`, `fail`, or `incomplete`.

Manifest-mode incompleteness includes cases such as:
- missing required repos,
- labels missing from the manifest,
- pinned commit mismatches,
- indexing or query capture failures,
- unreadable or missing metadata,
- unparsable query outputs.

Manifest-mode aggregate rules:
- `pass` only when all gating assertions pass and no comparability issue exists,
- `fail` when at least one gating assertion fails and nothing is incomplete,
- `incomplete` when the run is not comparable.

The script exits `0` only for aggregate `pass`.

## Example commands

### Single-repo manifest lane

Useful when checking one manifest repo in isolation while debugging:

```bash
bash scripts/gdscript-proof.sh \
  --manifest docs/superpowers/proofs/gdscript-good-tier-manifest.json \
  --repo "$PWD/.artifacts/gdscript-proof/repos/TopdownStarter" \
  --label "$PWD/.artifacts/gdscript-proof/repos/TopdownStarter=topdown-starter"
```

This normally yields aggregate `incomplete` because required manifest repos are missing from the run.

### Full good-tier manifest run

```bash
bash scripts/gdscript-proof.sh \
  --manifest docs/superpowers/proofs/gdscript-good-tier-manifest.json \
  --repo "$PWD/.artifacts/gdscript-proof/repos/godot-demo-projects/3d/squash_the_creeps" \
  --label "$PWD/.artifacts/gdscript-proof/repos/godot-demo-projects/3d/squash_the_creeps=squash-the-creeps" \
  --repo "$PWD/.artifacts/gdscript-proof/repos/godot-demo-projects/networking/webrtc_signaling" \
  --label "$PWD/.artifacts/gdscript-proof/repos/godot-demo-projects/networking/webrtc_signaling=webrtc-signaling" \
  --repo "$PWD/.artifacts/gdscript-proof/repos/godot-demo-projects/networking/webrtc_minimal" \
  --label "$PWD/.artifacts/gdscript-proof/repos/godot-demo-projects/networking/webrtc_minimal=webrtc-minimal" \
  --repo "$PWD/.artifacts/gdscript-proof/repos/TopdownStarter" \
  --label "$PWD/.artifacts/gdscript-proof/repos/TopdownStarter=topdown-starter"
```

### Legacy coarse-coverage run

```bash
bash scripts/gdscript-proof.sh \
  --repo /absolute/path/to/repo-a \
  --godot-version /absolute/path/to/repo-a=4.2 \
  --label /absolute/path/to/repo-a=signals \
  --repo /absolute/path/to/repo-b \
  --godot-version /absolute/path/to/repo-b=4.2 \
  --label /absolute/path/to/repo-b=imports
```

Without `--manifest`, the script preserves the old coarse category coverage behavior.

## Local artifact layout

Each run creates a unique local artifact root:

```text
.artifacts/gdscript-proof/<timestamp-pid-suffix>/
  env.txt
  commands.log
  build.log
  aggregate-summary.md
  run-index.json
  semantic-parity.json
  semantic-parity.md
  state/
    <artifact-slug>/
      home/
      config/
      cache/
      cache/codebase-memory-mcp/
  <comparison-label>-<mode>-<artifact-id>/
    repo-meta.json
    index.log
    index.json
    list-projects.json
    queries/
      gd-files.json
      gd-classes.json
      gd-methods.json
      gd-class-sample.json
      gd-method-sample.json
      signal-calls.json
      gd-inherits.json
      gd-imports.json
      signal-call-edges.json
      gd-inherits-edges.json
      gd-import-edges.json
      gd-same-script-calls.json
    summary.md
```

`env.txt` records the run root, worktree under test, binary path, manifest path if present, and the repo metadata passed to the script.

`run-index.json` is the additive machine-readable inventory for the run. It keeps the wrapper-first evidence contract intact by pointing to artifacts instead of replacing them. The file records:
- run-level metadata (`aggregate-summary.md`, build status, worktree metadata, manifest path, and `state/` root),
- one entry per repo/mode artifact with `repo-meta.json`, `summary.md`, `index.json`, `list-projects.json`, isolated `state/<slug>/{home,config,cache}` paths, and mode metadata under `requested_mode`, `actual_mode`, and `comparison_label`,
- a run-level `semantic_pairs` map keyed by manifest label so the sequential and parallel artifacts for one proof target are machine-addressable,
- the fixed query suite names, and
- per-query artifact metadata under `artifacts.queries.<query_name>` with `path` plus a `status` of `present`, `failed`, `not_run`, or `missing` so incomplete runs stay inspectable from disk evidence alone.

`semantic-parity.json` and `semantic-parity.md` are additive review artifacts. They are derived from the canonical `queries/*.json` wrapper files and do not replace them.

## Query suite and outputs

The script captures these wrapper files per complete repo, and `queries/*.json` remains the canonical raw MCP evidence set even after `run-index.json` was added:
- `gd-files.json`
- `gd-classes.json`
- `gd-methods.json`
- `gd-class-sample.json`
- `gd-method-sample.json`
- `signal-calls.json`
- `gd-inherits.json`
- `gd-imports.json`
- `signal-call-edges.json`
- `gd-inherits-edges.json`
- `gd-import-edges.json`
- `gd-same-script-calls.json`

Each wrapper file records:
- `query_name`
- `project_id`
- `artifact_slug`
- literal `query`
- raw `result`

Use `run-index.json` to locate wrapper files quickly; use the wrapper files themselves as the raw evidence source when reviewing query outputs.

## How to read the outputs

## Sequential vs parallel semantic parity

Manifest mode now produces one sequential artifact and one parallel artifact for every approved manifest label. Review them in this order:

1. Open `run-index.json` and find `semantic_pairs.<label>.requested_modes.sequential` and `.parallel` to locate the two repo artifact directories.
2. Confirm each paired `repo-meta.json` records `requested_mode`, `actual_mode`, and `comparison_label`.
3. Use the paired `queries/*.json` files as the canonical raw evidence source.
4. Open `semantic-parity.json` or `semantic-parity.md` to review additive parity outcomes for `SEM-01` through `SEM-06`.
5. For `SEM-01`, require non-zero class and method counts in both modes plus representative class and method samples from the paired wrappers.
6. For `SEM-02` through `SEM-05`, review semantic comparison entries from `semantic-parity.json` or `semantic-parity.md` and confirm the representative edge samples remain meaningfully the same in sequential and parallel mode.
7. Treat `SEM-06` as semantic comparison only: counts plus representative samples and edges must stay consistent across modes. Do not require byte-for-byte artifact equality, and do not accept aggregate manifest pass/fail alone as sufficient evidence.
8. If any parity result is `incomplete`, inspect `run-index.json`, the paired `repo-meta.json` files, and the wrapper `queries/*.json` evidence before deciding whether the run is reviewable.

### Per-repo `summary.md`

Legacy mode reports coarse repo completeness and category contributions.

Manifest mode reports:
- repo path, slug, manifest label, and resolved project ID,
- actual and pinned commit,
- pinned-commit match,
- Godot version,
- repo completeness,
- repo outcome,
- required-for categories,
- CLI capture mode and fallback note,
- a comparability-issues section when the repo is incomplete,
- a `Gating assertions` table,
- an `Informational assertions` table.

### `aggregate-summary.md`

Manifest mode reports:
- worktree / branch / commit under test,
- binary path and build status,
- manifest path,
- final aggregate outcome,
- aggregate pass boolean,
- aggregate note,
- gating and informational totals,
- one repo table with label, slug, outcome, required-for, pinned commit, actual commit, and notes.

Legacy mode continues to report the older indexing / signal / imports / inherits coverage categories.

### `run-index.json`

Use the run index when you need a machine-readable map of the entire proof run:
- `run.aggregate_summary` points to the human-readable top-level summary.
- `repos.<slug>.artifacts.repo_meta` and `.summary` point to the repo-level status files.
- `repos.<slug>.requested_mode`, `.actual_mode`, and `.comparison_label` record how that artifact participated in a sequential-vs-parallel pair.
- `repos.<slug>.runtime.state_root`, `.home`, `.xdg_config_home`, and `.xdg_cache_home` show the isolated state layout under `state/<slug>/...`.
- `repos.<slug>.artifacts.queries.<query_name>` records both wrapper `path` and `status`.
- `repos.<slug>.failure_context` records the incomplete-path message and failed query name when a query breaks mid-suite.
- `semantic_pairs.<label>` maps each manifest label to its sequential and parallel repo artifacts.

This means incomplete runs can be diagnosed from the artifact tree directly instead of re-running manual database queries.

## Exit codes

- Exit `0`: aggregate outcome is `pass`.
- Non-zero exit: aggregate outcome is `fail` or `incomplete`.

The script still attempts all candidate repos before deciding the final exit code.
It also prints `Proof run root: ...` on stdout so you can jump directly to the latest artifact directory.

## Operator checklist

- Confirm `.artifacts/gdscript-proof/` stays ignored by git.
- Confirm the latest run wrote `aggregate-summary.md`.
- Confirm the latest run wrote `run-index.json` beside `aggregate-summary.md`.
- Confirm the latest run wrote `semantic-parity.json` and `semantic-parity.md` beside `aggregate-summary.md`.
- Confirm each processed repo has `summary.md` and `repo-meta.json`.
- Confirm `run-index.json` exposes isolated `state/<slug>/home`, `config`, and `cache` paths for every processed repo.
- Confirm `run-index.json` points to `queries/*.json` wrapper files instead of replacing them; the wrapper JSON files remain the canonical raw evidence set.
- Confirm `run-index.json` exposes `requested_mode`, `actual_mode`, `comparison_label`, and `semantic_pairs` for every approval-bearing manifest label.
- Confirm `semantic-parity.*` reports sequential-vs-parallel counts and representative samples for classes, methods, same-script calls, inherits, imports, and signal calls.
- Confirm `scripts/gdscript-proof.sh` remains the only approval-bearing workflow; do not substitute ad hoc MCP commands or a second verifier.
- Confirm approval coverage includes all four manifest repos (`squash-the-creeps`, `webrtc-signaling`, `webrtc-minimal`, and `topdown-starter`) in both `sequential` and `parallel` mode.
- Confirm `SEM-01` review includes non-zero class and method counts plus representative class/method samples, not counts alone.
- Confirm `SEM-06` is judged by semantic consistency in `semantic-parity.json` / `semantic-parity.md`, not byte-for-byte artifact equality.
- Confirm approval-bearing evidence came from manifest mode rather than ad hoc `--repo` selection.
- Confirm every approval-bearing repo summary exposes the canonical identity tuple (`remote`, `pinned_commit`, `project_subpath` when needed, `godot_version`).
- Confirm labels are presented as readability metadata and local checkout paths as run evidence only.
- Confirm approved targets are explicitly labeled Godot 4.x qualifying, and non-manifest/debug runs are explicitly labeled non-canonical/non-qualifying.
- In manifest mode, confirm the aggregate summary clearly distinguishes `pass`, `fail`, and `incomplete`.
- If a repo or run is `incomplete`, inspect `run-index.json`, the paired `repo-meta.json` files, and any already-written `queries/*.json` wrappers for failure context and missing query status before considering manual database inspection.
- After every manifest run, immediately update:
  - `docs/superpowers/proofs/gdscript-good-tier-misses.md`
  - `docs/superpowers/proofs/gdscript-good-tier-checklist.md`
- If the aggregate run is not `pass`, do not promote README or benchmark claims.
