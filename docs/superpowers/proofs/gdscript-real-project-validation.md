# GDScript Real-Project Validation Runbook

Related issue: https://github.com/DeusData/codebase-memory-mcp/issues/186

## Purpose

Use this runbook to generate reproducible, local-only proof that the GDScript indexing changes work on real Godot repositories.

The proof workflow:
- runs from the `gdscript-support` worktree
- builds and uses the local `codebase-memory-mcp` binary under test
- writes all runtime state and proof artifacts under `.artifacts/gdscript-proof/`
- supports one or more local Godot repos in a single run
- produces per-repo `summary.md` files plus one `aggregate-summary.md`

Do not commit generated proof artifacts.

## Prerequisites

- Work from the `gdscript-support` worktree.
- Have one or more local Godot repositories available by path.
- Have the local build prerequisites needed for `make -f Makefile.cbm cbm`.
- Be ready to provide Godot version metadata for any repo you want to count toward the Godot 4.x indexing requirement.

The script builds the binary automatically at run start and uses:

- `build/c/codebase-memory-mcp`

Runtime state is separated per artifact slug under the run root (`HOME`, `XDG_CONFIG_HOME`, `XDG_CACHE_HOME`).

Artifact slugs are derived from a sanitized repo label/basename, short commit/ref (or `unavailable`), and a stable path-hash suffix, so separate repos cannot collide even when names/refs match.

## Accepted arguments

Usage:

```bash
bash scripts/gdscript-proof.sh \
  --repo /abs/path/to/repo \
  [--repo /abs/path/to/repo2 ...] \
  [--godot-version REPO=4.x] \
  [--label REPO=name]
```

Rules:

- `--repo` is required at least once.
- `--repo` may be repeated.
- `--godot-version` and `--label` use `REPO=value` form.
- `--godot-version` and `--label` must reference a repo path already declared with `--repo`.
- Repo paths are canonicalized to absolute paths.

Examples of accepted Godot version values:

- `4.2`
- `v4.2`
- `Godot 4.2`
- `3.5`

## Required repo metadata

For each repo, capture or provide:

- local repo path
- repo commit/ref under test
- Godot version if known
- optional human-friendly label

At least one contributing repo must be a confirmed Godot 4.x repo for final issue #186 acceptance.

## Example commands

### Single-repo run

```bash
bash scripts/gdscript-proof.sh \
  --repo /absolute/path/to/real-godot-repo \
  --godot-version /absolute/path/to/real-godot-repo=Godot\ 4.2 \
  --label /absolute/path/to/real-godot-repo=primary
```

### Multi-repo run

```bash
bash scripts/gdscript-proof.sh \
  --repo /absolute/path/to/repo-a \
  --godot-version /absolute/path/to/repo-a=4.2 \
  --label /absolute/path/to/repo-a=signals \
  --repo /absolute/path/to/repo-b \
  --godot-version /absolute/path/to/repo-b=4.2 \
  --label /absolute/path/to/repo-b=inherits \
  --repo /absolute/path/to/repo-c \
  --godot-version /absolute/path/to/repo-c=4.2 \
  --label /absolute/path/to/repo-c=imports
```

Use multiple repos when one repo does not provide all required categories.

## What qualifies a repo as Godot 4.x evidence

- A repo with confirmed Godot 4.x metadata may satisfy the indexing requirement.
- A repo with unknown version may still count for signal/imports/inherits coverage if the repo is complete, but it does not count for the Godot 4.x indexing requirement.
- A repo confirmed as Godot 3.x does not count toward any acceptance category.

## Local artifact layout

Each run creates a unique local artifact root:

```text
.artifacts/gdscript-proof/<timestamp-pid-suffix>/
  env.txt
  commands.log
  build.log
  aggregate-summary.md
  state/
    <artifact-slug>/
      home/
      config/
      cache/
      cache/codebase-memory-mcp/
  <artifact-slug>/
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
    summary.md
```

All runtime state for the proof run lives under this artifact root.

Per repo, command execution uses:

- `HOME=<run>/state/<artifact-slug>/home`
- `XDG_CONFIG_HOME=<run>/state/<artifact-slug>/config`
- `XDG_CACHE_HOME=<run>/state/<artifact-slug>/cache`

## Query suite and output files

The script always captures these eight query wrapper files per complete repo:

- `gd-files.json` (`gd-files`)
- `gd-classes.json` (`gd-classes`)
- `gd-methods.json` (`gd-methods`)
- `gd-class-sample.json` (`gd-class-sample`)
- `gd-method-sample.json` (`gd-method-sample`)
- `signal-calls.json` (`signal-calls`)
- `gd-inherits.json` (`gd-inherits`)
- `gd-imports.json` (`gd-imports`)

Each wrapper file records:

- `query_name`
- `project_id`
- `artifact_slug`
- literal `query`
- raw `result`

## How to read the outputs

### Per-repo `summary.md`

Each repo summary reports:

- repo path, name, slug, and resolved project ID
- git ref / commit / branch
- Godot version and qualification status
- indexing / project-resolution / overall status
- CLI capture mode and fallback note if applicable
- `.gd` file / class / method counts
- signal call / inherits / imports counts
- whether the repo contributes to indexing, signal, imports, and inherits coverage
- short proof notes
- incomplete or failed status details when applicable

`repo_complete=true` means:

- indexing succeeded
- project resolution succeeded
- overall repo status is `complete`
- `project_id` is present
- all eight query wrapper files were written and parsed successfully

### `aggregate-summary.md`

The aggregate summary reports:

- codebase-memory-mcp worktree / branch / commit under test
- binary path and build status
- all repos processed
- which repos contributed to each coverage category
- final acceptance result
- exact missing coverage categories when the run fails

Coverage rules are:

```text
indexing_coverage = any repo where repo_complete && godot_version_qualifies && gd_files > 0 && gd_classes > 0 && gd_methods > 0
signal_coverage = any repo where repo_complete && signal_calls > 0
imports_coverage = any repo where repo_complete && gd_deps > 0
inherits_coverage = any repo where repo_complete && gd_inherits_edges > 0
aggregate_pass = indexing_coverage && signal_coverage && imports_coverage && inherits_coverage
```

Additional rules:

- only a confirmed Godot 4.x repo may satisfy `indexing_coverage`
- unknown-version repos may satisfy signal/imports/inherits coverage if complete
- confirmed Godot 3.x repos never contribute to any category
- incomplete repos never contribute to any category

## Exit codes

- Exit `0`: aggregate acceptance passed.
- Non-zero exit: aggregate acceptance failed, all candidates failed, or at least one required category is still missing.

The script attempts all candidate repos before deciding the final exit code.
It also prints `Proof run root: ...` on stdout so you can jump directly to the latest artifact directory.

## External reporting checklist

When reporting proof in an issue, PR, or review comment, include:

- artifact root path used for the final run
- repo path for each contributing repo
- repo commit/ref for each contributing repo
- Godot version metadata for each contributing repo
- `.gd` file / class / method counts for the qualifying Godot 4.x repo
- signal call count from the repo that satisfied signal coverage
- `.gd` import count from the repo that satisfied imports coverage
- `.gd` inheritance count from the repo that satisfied inherits coverage
- final aggregate pass/fail result
- path to `aggregate-summary.md`

## Verification checklist for operators

- Confirm `.artifacts/gdscript-proof/` stays ignored by git.
- Confirm the latest run wrote `aggregate-summary.md`.
- Confirm each contributing repo has `summary.md` and `repo-meta.json`.
- Confirm the aggregate summary explicitly shows:
  - one confirmed Godot 4.x repo satisfying indexing coverage
  - one or more complete repos satisfying signal coverage
  - one or more complete repos satisfying imports coverage
  - one or more complete repos satisfying inherits coverage
- Confirm the final exit code matches the aggregate result.
