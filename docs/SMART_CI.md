# Smart CI: pull-request test selection on GitHub Actions

Smart CI runs, for each pull request, only the CI jobs and tests that the
change can reach. This page explains how this repository does it and how
another repository on GitHub Actions could do the same with the released
`codebase-memory-mcp` binary.

Status: experimental. Every answer carries `"experimental": true`. The
selection narrows real test runs only while the repository variable
`TEST_IMPACT_MODE` is `gate`; otherwise it runs in report-only (shadow) mode.

> **Read the scope section before you start.** The lane layer is a pattern
> any repository can copy. The test layer narrows only C/C++ code whose tests
> follow this repository's test-framework conventions (or declared
> equivalents), and only with a per-test coverage map that you must produce
> yourself. In any other repository the engine answers "run everything". That
> is safe, but it saves nothing.

## The guarantee

Smart CI never runs fewer tests unless it can show that the omitted tests
cannot be affected. When it is unsure, it runs everything:

- a path that no rule knows, an empty or truncated file list, or a change to
  the selection's own configuration, tooling or workflows;
- a shallow clone, a diff that cannot be read in full, or an invalid config;
- no coverage map, or a coverage map that is not admitted (wrong commit, more
  than 7 days old, made for another platform or harness);
- a change that reaches the test runner's `main`, or a test-runner role
  (below) that no test names;
- a role table entry the engine cannot read, or a macro naming a role's
  literal that it cannot follow;
- any failure of the selection job itself.

Each of these makes the run cost more CI minutes. None of them loses coverage.

## Two layers

```
changed files ──► Layer 1: lane selector ──► which CI jobs run
                  (scripts/ci/select-lanes.sh, your own rules)
                                   │
                                   ▼  (the Linux x86_64 test leg only, in this repo)
merge base + PR ─► Layer 2: test-impact engine ─► which suites / tests run
                  (codebase-memory-mcp test-impact select
                   + team-artifact bundle from main)
```

| | Layer 1: lanes | Layer 2: tests |
|---|---|---|
| Question | Which jobs (lint, test legs, smoke, sanitizers) does this PR need? | Inside a test job, which suites and tests? |
| Input | The PR's changed-file list | Git history, the code graph, a per-test coverage map |
| Implementation | `scripts/ci/select-lanes.sh` (repository-specific rules) | The binary's `test-impact select` subcommand |
| Unknown input | Every lane runs | Every test runs (`run_all` with reasons) |
| Portable? | As a pattern: you write your own rules | C/C++ with supported test conventions only (see below) |

## Can your repository use it?

| Piece | General? | What you provide |
|---|---|---|
| Lane selection (Layer 1) | Pattern only | Your own path → category → lanes rules. `select-lanes.sh` encodes this repository's paths. |
| Engine (`test-impact select`) | Ships in the binary | Nothing to build. |
| Source languages it can narrow | **C family only**: `.c .h .cc .cpp .hpp .m .mm .cxx .hh` | Every other changed file must match a config rule (`ignore`, `referencing_tests`, ...). Otherwise the answer is `run_all` (`UNMAPPED_FILE`). |
| Test model | **C family only** | The `c-cbm` preset (this repository's `TEST`/`SUITE`/`RUN_TEST`/`RUN_SELECTED_SUITE` macros, see `tests/test_framework.h`), or custom `tests.conventions` records for equivalent C macros. |
| Coverage map | **Required to narrow at all** | Format-1 files (`functions.tsv`, `tests.tsv`, `meta.json`). This repository produces them with an LLVM-instrumented runner and `scripts/test-impact/coverage-map.py`, which are both specific to its harness. |
| Runner filter | Pattern | A test runner that can run a `suite` / `suite:test` list and fails when a listed test does not exist. |

The other presets (`pytest`, `go`, `junit`, `gtest`, `rust`, `jest`) are
accepted switches, but the engine does not model them. An enabled `gtest`
blocks narrowing for C++ files. Custom `tests.conventions` are covered by the
model's unit tests. The engine's end-to-end tests use `c-cbm` only. Treat
custom conventions as unproven until you have shadow evidence.

Without admitted coverage the engine marks every suite `whole`, and the lane
reports `run_all`. Static reachability alone never omits a suite.

## Prerequisites

1. **The binary, pinned.** Use the same release in the producer and the PR
   jobs. Download that release's Linux archive and verify it against the
   release's `checksums.txt`, as this repository's shadow job does
   (`scripts/ci/test-impact-shadow.sh --fetch-release`):

   ```bash
   CBM_VERSION=vX.Y.Z   # a release that contains the features you use (see below)
   asset=codebase-memory-mcp-linux-amd64.tar.gz
   gh release download "$CBM_VERSION" --repo DeusData/codebase-memory-mcp \
     --pattern "$asset" --pattern checksums.txt --dir "$RUNNER_TEMP/cbm"
   (cd "$RUNNER_TEMP/cbm" && grep " $asset\$" checksums.txt | sha256sum -c - &&
     tar -xzf "$asset" codebase-memory-mcp)
   "$RUNNER_TEMP/cbm/codebase-memory-mcp" --version
   ```

   For a quick start, `install.sh` installs the latest release with the same
   checks (HTTPS only, SHA-256 against `checksums.txt`, the archive's member
   list, `--version`); `--skip-config` skips agent configuration, which a CI
   runner does not need. It has no version flag, so it is not a pin.

   **Version note.** `v0.12.0` and `v0.12.1` have `test-impact select`/`publish`.
   They do not have `key: "path"`, `unreferenced`, `--vetoed-out` or
   `run_all_rules`; those ship in a later release. With either version, a config
   that uses those keys is invalid, and every answer is `run_all`. Until that
   release, build the binary from source (`make -f Makefile.cbm cbm`), as this
   repository's producer and `select-tests` jobs do.

2. **Full git history.** Check out with `fetch-depth: 0` in every job that
   runs the engine. The engine verifies the history it diffs and refuses a
   shallow clone (`GIT_UNAVAILABLE`, everything runs).

3. **A runner that runs a named subset.** See "The test job" below.

4. **A coverage producer on the default branch** that writes the format-1 map
   for every test, child processes included. A test whose coverage is
   uncertain must be marked `incomplete`. An incomplete test is always
   selected.

5. **`gh`, `python3`, and `GH_TOKEN` with `actions: read`** in the PR job, to
   fetch the bundle. GitHub-hosted runners include `gh` and `python3`.

## Recipe

Copy these files from this repository and adapt the marked lines:

| File | Adapt |
|---|---|
| `scripts/ci/test-impact-bundle.sh` | `WORKFLOW=` (producer file name) and the hard-coded `main` branch filter. |
| `scripts/ci/test-impact-select.sh` | `--lane unit` (your lane name) and the `PLATFORM` default (must equal the producer's). |
| `scripts/test-impact/selection.py` | Nothing, if your runner accepts `suite` / `suite:test` lines. |

### (a) Producer: publish a bundle for every commit on main

The bundle holds the commit's frozen graph, its coverage map and a
`receipt.json` that binds both by SHA-256. It is uploaded as the Actions
artifact `test-impact-<sha>`. `test-impact-bundle.sh` accepts a bundle only
from a successful `push`, `schedule` or `workflow_dispatch` run of the
producer workflow, in the same repository, on `main`, whose head is exactly
the requested commit.

```yaml
# .github/workflows/test-impact-artifact.yml  (the name test-impact-bundle.sh looks for)
name: test-impact-artifact
on:
  push:
    branches: [main]
  schedule:
    - cron: "23 3 * * 0"        # weekly full refresh
  workflow_dispatch:
permissions:
  contents: read
  actions: read                 # fetch the previous bundle (incremental mode)
concurrency:
  group: test-impact-artifact   # bundles chain: never overlap, never cancel
  cancel-in-progress: false
jobs:
  publish:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@<pinned-sha>
        with:
          fetch-depth: 0
          persist-credentials: false
      - name: Install codebase-memory-mcp
        run: <the pinned install from Prerequisites>
      - name: Per-test coverage map            # YOURS: format 1, see coverage-map.py
        run: |
          echo "OBSERVED=$(date +%s)" >> "$GITHUB_ENV"   # when the coverage run starts
          ./ci/coverage-map.sh --out "$RUNNER_TEMP/coverage"
      - name: Publish the bundle
        run: |
          export CBM_CACHE_DIR="$RUNNER_TEMP/ti-cache"
          export CBM_RUNTIME_DIR="$(mktemp -d /tmp/cbm-rt.XXXXXX)"   # short: sun_path
          mkdir -p "$RUNNER_TEMP/ti-work" && chmod 700 "$RUNNER_TEMP/ti-work"
          "$RUNNER_TEMP/cbm/codebase-memory-mcp" test-impact publish \
            --repo "$GITHUB_WORKSPACE" --out "$RUNNER_TEMP/bundle" \
            --work "$RUNNER_TEMP/ti-work" --coverage "$RUNNER_TEMP/coverage" \
            --observed-at "$OBSERVED" --platform linux-x86_64-clang21
      - uses: actions/upload-artifact@<pinned-sha>
        with:
          name: test-impact-${{ github.sha }}
          path: ${{ runner.temp }}/bundle
          include-hidden-files: true    # the graph lives in .codebase-memory/
          retention-days: 30
          if-no-files-found: error
```

This repository's producer (`.github/workflows/test-impact-artifact.yml` with
`scripts/ci/test-impact-publish.sh`) adds an incremental mode. A push re-runs
coverage only for the suites that its own selection picks and merges them into
the previous map (`coverage-merge.py`). The graph is built incrementally from
the previous bundle (`--previous`). The weekly run rebuilds everything and
fails if the incremental graph topology differs from the full one. The
incremental mode is an optimisation. A full coverage run on every push is
simpler and just as correct.

The receipt keeps the time of the oldest coverage observation. An incremental
publish carries it forward unchanged. A map whose oldest observation is more
than 7 days old is refused (`TOO_OLD`), so a full refresh has to run more often
than that.

### (b) PR job, shadow mode (report only)

```yaml
  test-impact-shadow:
    runs-on: ubuntu-latest
    continue-on-error: true          # can never block or pass a merge
    permissions:
      contents: read
      actions: read                  # the merge base's bundle
    steps:
      - uses: actions/checkout@<pinned-sha>
        with:
          fetch-depth: 0
          persist-credentials: false
      - name: Install codebase-memory-mcp
        run: <the pinned install from Prerequisites>
      - name: Select with the team artifact (shadow)
        env:
          GH_TOKEN: ${{ github.token }}
        run: scripts/ci/test-impact-select.sh --binary "$RUNNER_TEMP/cbm/codebase-memory-mcp" --out "$RUNNER_TEMP/test-impact"
      - uses: actions/upload-artifact@<pinned-sha>
        if: ${{ always() }}
        with:
          name: test-impact-prediction
          path: |
            ${{ runner.temp }}/test-impact/selection-engine.json
            ${{ runner.temp }}/test-impact/summary-engine.md
```

Leave this job out of your aggregate required check. What
`test-impact-select.sh` does:

1. Takes the base side of the PR merge commit (`HEAD^1`) and fetches that
   commit's bundle.
2. If the producer skipped that commit (merge bursts: GitHub keeps one pending
   run per concurrency group), it uses the nearest first-parent ancestor that
   has a bundle, up to 20 commits back, and diffs against that ancestor. If the
   PR reverts a change that main made after the ancestor, the ancestor's map
   cannot speak for that file, so the script selects without coverage (the
   revert guard).
3. Passes `--artifact-verified` only for a bundle that it fetched and checked
   itself. Nothing inside a bundle can assert where the bundle came from.
4. Runs `test-impact select` and writes `selection-engine.json`. It also
   writes a Markdown summary to the job's step summary.

The engine reads `.codebase-memory.json` from the **selection base** (the merge
base, or the ancestor whose bundle was admitted), not from the PR. A PR cannot
loosen its own selection. A PR that changes the config runs
everything (`builtin:test-impact-config`).

### (c) Gate job and the test job

```yaml
  select-tests:
    if: ${{ vars.TEST_IMPACT_MODE == 'gate' }}
    runs-on: ubuntu-24.04
    permissions:
      contents: read
      actions: read
    outputs:
      selection: ${{ steps.select.outputs.selection }}
    steps:
      - uses: actions/checkout@<pinned-sha>
        with:
          fetch-depth: 0
          persist-credentials: false
      - name: Install codebase-memory-mcp
        run: <the pinned install from Prerequisites>
      - id: select
        env:
          GH_TOKEN: ${{ github.token }}
        run: scripts/ci/test-impact-select.sh --binary "$RUNNER_TEMP/cbm/codebase-memory-mcp" --out "$RUNNER_TEMP/test-impact" --gate "$RUNNER_TEMP/test-selection"
      - if: ${{ steps.select.outputs.selection == 'narrowed' }}
        uses: actions/upload-artifact@<pinned-sha>
        with:
          name: test-selection
          path: ${{ runner.temp }}/test-selection
          if-no-files-found: error

  test:
    needs: [select-tests]
    if: ${{ !cancelled() }}          # a skipped or failed select-tests must NOT skip the tests
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@<pinned-sha>
      - if: ${{ needs.select-tests.result == 'success' && needs.select-tests.outputs.selection == 'narrowed' }}
        uses: actions/download-artifact@<pinned-sha>
        with:
          name: test-selection
          path: ${{ runner.temp }}/test-selection
      - run: ./ci/run-tests.sh          # YOURS: narrows only if test-only.txt is present
```

With `--gate DIR`, the script runs `selection.py filter … --lane unit` and
writes `selection=narrowed` or `selection=all` to `$GITHUB_OUTPUT`. A narrowed
answer writes:

- `DIR/test-only.txt`: one token per line, either `suite` (run the whole
  suite) or `suite:test`;
- `DIR/optional-suites.txt`: selected suites that are registered only under a
  preprocessor condition. A build may lack them.

Everything else means `all`: a `run_all` answer, an unknown schema or filter
encoding, a filter that disagrees with the lane's suite list, a crash, and a
lane that selects nothing. The test job then gets no selection and runs every
test.

**The test job's runner** has to keep the same contract as this repository's
runner (`scripts/test.sh` → `scripts/run-tests-parallel.sh` →
`CBM_TEST_ONLY_FILE`, read by `tests/test_main.c`):

- launch only the suites that the tokens name, and only the named tests inside
  a `suite:test` suite;
- **fail** when the selection names a suite the build does not register,
  unless `optional-suites.txt` lists it;
- **fail** when a selected test cannot be found;
- **fail** when a selection was promised but the file is missing or empty.

A selection that is silently ignored, or partly ignored, would turn a narrowing
into missed coverage.

**A runner that starts itself again.** Tests often spawn the test binary as a
child (`<runner> __probe_x ...`) and its `main` hands over to a probe. A change
that reaches `main` reaches every test, so the engine runs everything
(`RUNNER_REACHED`). List the probes instead in a table in the runner's source,
one entry per line, as `tests/test_main.c` does:

```c
TF_ROLE(TF_PHASE_SEAMED, "__cbm_sockprobe", tf_maybe_run_socket_probe),
TF_ROLE(TF_PHASE_WORKER, "--index-worker", tf_maybe_run_index_worker),
TF_ROLE(TF_PHASE_SPAWNER, "CBM_INDEX_SUPERVISOR", tf_maybe_run_index_worker),
```

The engine reads the table: a change that only a probe's handler reaches
selects the tests whose source names one of that handler's literals (the argv
the child gets, or a switch that lets production code start it), also through
a `#define` that names the literal. The runner's own use of a literal names no
spawner. A reached handler that no test names, a table entry the engine cannot
read, and a change reaching `main` any other way still run everything; the
engine note names the call chain into `main`.

**Only the legs the coverage speaks for should narrow.** Here the coverage
comes from one Linux x86_64 clang-21 producer. Only the Linux x86_64 test leg
downloads the selection (`runner.os == 'Linux' && runner.arch == 'X64'` in
`_test.yml`). The arm64, macOS and Windows legs always run everything,
because code under architecture or platform `#ifdef`s has static evidence
only there. That leg builds with gcc, not clang: code under a compiler `#if`
has static evidence only too, and `--platform` names the producer's platform,
so the engine's platform check admits the map by construction.

### (d) Flip the variable

Shadow is the default: the variable is unset, `select-tests` is skipped, and
every test runs. When the evidence is in, set the repository variable:

```bash
gh variable set TEST_IMPACT_MODE --body gate      # gate
gh variable delete TEST_IMPACT_MODE               # back to shadow
```

Do not add `select-tests` to your required checks or to the aggregate gate.
When it breaks, PRs cost more minutes. They do not lose coverage.

### Optional: Layer 1 (lanes)

`scripts/ci/select-lanes.sh` is this repository's lane rule set. A `changes`
job lists the PR's files (the `pulls/{n}/files` API, with a git fallback),
runs the selector, and writes `tier`, `full`, `lanes` (a JSON list) and
`reasons` to `$GITHUB_OUTPUT`. Each lane job then runs
`if: contains(fromJSON(needs.changes.outputs.lanes), '<lane>')`. A single
aggregate job (`ci-ok`, `scripts/ci/require-all-green.sh`) is the only required
check. It fails when a *selected* lane was skipped, cancelled or missing.

For your own version, keep these properties:

- An unknown path, an empty list, workflow and harness changes, and a list
  truncated at the API's 3000-file cap all select every lane.
- The selector runs from a checkout without credentials, after the step that
  holds the token.
- Replay your PR history through the selector and check that every real
  failure had its catching lane selected. This repository's equivalent is
  `scripts/test-impact/replay-selector.sh`.

## Configuration: `.codebase-memory.json` → `test_impact`

| Key | Values | Default |
|---|---|---|
| `version` | `1` (required) | — |
| `tests.presets` | `{"c-cbm": true, ...}`. Unknown names are an error. | `c-cbm` off, the six others on |
| `tests.conventions` | C-family macro records: `case`, `suite`, `registration`, `suite_registration` | none |
| `rules` | ordered list, see below | none (built-ins only) |
| `lanes` | list of `{name, suites, exclude_suites?, default: "run"\|"skip", narrow?}` | one lane `unit`: all suites except `@perf`, `narrow: true` |
| `unmapped_files` | `"run_all"` (the only accepted value) | `run_all` |
| `coverage.compatibility_paths` | repo-relative files that the coverage map depends on (harness, runner, build flags) | none |

**Rules.** Each rule is `{id, paths, action, ...}`. `paths` are gitignore-style
globs with `{a,b}` alternatives. A pattern with a `/` is anchored at the
repository root. Without a `/`, it matches at any depth. `**` matches across
directories. Project rules come first, then the built-ins. For each changed
path, **the first matching rule wins**, so order matters.

| `action` | Effect |
|---|---|
| `run_all` | Any match runs everything. The summary names the rule (`run_all_rules`). |
| `ignore` | The path selects nothing. |
| `referencing_tests` | Selects the tests whose source contains the path as text. The engine tries the path itself, then its parent directories down to the `key`, each with and without its first segment. `key: "path_segment:N"` stops at segment N (zero-based). `key: "path"` tries the exact path only. If no test names the path, everything runs, unless `unreferenced: "ignore"` is set. |
| `suites` | Runs the named suites whole. |
| `lanes` | Activates the named lanes (for lanes with `default: "skip"`). |

Built-in rules, applied after yours:

| id | paths | action |
|---|---|---|
| `builtin:test-impact-config` | `.codebase-memory.json` | `run_all` |
| `builtin:build` | `Makefile*`, `CMakeLists.txt`, `*.cmake`, `meson.build`, `BUILD*`, `WORKSPACE`, `package*.json`, `go.mod`, `go.sum`, `Cargo.*`, `pom.xml`, `build.gradle*`, `pyproject.toml`, `setup.*`, `requirements*.txt` | `run_all` |
| `builtin:ci` | `.github/**`, `.gitlab-ci.yml`, `.circleci/**`, `Jenkinsfile*`, `.buildkite/**`, `azure-pipelines*.yml` | `run_all` |
| `builtin:vendor` | `vendor*/**`, `third_party/**` | `run_all` |
| `builtin:conftest` | `conftest.py` | `run_all` |
| `builtin:documentation` | `docs/**`, `**/*.md`, `LICENSE*` | `ignore` |

**Lanes.** A lane that you declare yourself does **not** narrow unless you set
`"narrow": true`. `signals` is accepted but has no effect yet.

**This repository's file, explained:**

| Rule | Why |
|---|---|
| `harness` → `run_all` | The test framework and the runner's `main` touch every suite. |
| `engine` → `run_all` | The selection's own code and config. A change to them could loosen what is selected. |
| `build` → `run_all` | Build and runner scripts: the file list cannot tell a flag change from a harmless one. `/.gitattributes` is anchored at the root. |
| `grammars` → `run_all` | Vendored parsers are not modelled per test. |
| `fixtures` → `referencing_tests`, `path_segment:2` | `tests/fixtures/<name>/...` selects the tests that mention the fixture, down to `tests/fixtures/<name>`. |
| `contracts` → `ignore` | Shell, Python and data contract tests run in every leg's contract step, which is never narrowed. |
| `notices` → `ignore` | Release notices. |
| `repo-data` → `referencing_tests`, `path`, `unreferenced: ignore` | Scripts, docs and packaging. A C test whose source names one of these files (its path, as text) is selected. A file that no test names selects nothing. |

`coverage.compatibility_paths` lists the harness and coverage tools. Changing
the bytes of any of them, or the `--platform` label, makes the existing coverage
map incompatible until the producer publishes a new one.

## Rollout and evidence

1. **Shadow.** Run (a) and (b). Leave `TEST_IMPACT_MODE` unset.
2. **Measure.** For each PR run, compare `selection-engine.json` with what
   failed in that run's full test legs.
   - A **miss** is a test that failed in the full run but was not selected.
     One unexplained miss blocks the flip.
   - **Savings** are the suites and tests that were not selected.

   Nothing in this repository computes misses for Layer 2 yet. Layer 1's
   history replay (`replay-selector.sh`) is the model to follow.
3. **Gate.** Set `TEST_IMPACT_MODE=gate`. Keep the shadow job running, so that
   the evidence keeps coming.

**Reading the step summary** (`### test-impact engine`):

| Line | Meaning |
|---|---|
| `decision: \`selected\`` / `\`run_all\`` / `\`nothing\``, then `run-all reasons [...]` | The top-level answer and why everything runs. `decided by rule(s) [...]` names the matched `run_all` rule ids. |
| `team artifact: fetched (ancestor …)` / `none`; `coverage \`admitted\`` / `\`rejected\` [reasons]` / `\`unavailable\`` | Whether a bundle was used, and whether its coverage was admitted. |
| `engine note: …` | The engine's first diagnostic line, such as the path that caused `UNMAPPED_FILE`, or the call chain into the runner's `main` for `RUNNER_REACHED`. |
| `lane \`unit\` (selected): N whole [REASON n, …], M narrowed (K tests)` | Per lane: suites that run whole (with a histogram of reasons), and suites narrowed to K tests. |
| `the runner-main veto (RUNNER_REACHED) discarded this selection:` | The change reached the runner's `main`, so everything runs. The lines below show what would have been selected otherwise. |
| `PR CI runs N selection tokens` / `PR CI runs every test (selection exit N)` | Gate mode only: what the test legs received. |

Common run-all reasons:

| Reason | Cause |
|---|---|
| `RULE_RUN_ALL` | A `run_all` rule matched (see `run_all_rules`). |
| `UNMAPPED_FILE` | A changed file matched no rule and is not C-family source. |
| `RULE_TARGET_UNKNOWN` | A `referencing_tests` path that no test names (without `unreferenced: "ignore"`). |
| `RUNNER_REACHED` | The change reaches the test runner's `main` (or a child role none of whose literals a test names). |
| `GIT_UNAVAILABLE` | No git, a shallow clone, or a diff that cannot be read in full. |
| `CONFIG_INVALID` / `POLICY_UNAVAILABLE` | The merge base's config or rules do not parse. |

Reasons per suite: `ARTIFACT_REJECTED` (no admitted coverage), `SETUP_HIT` /
`SETUP_UNKNOWN` (the suite's between-test setup ran changed code, or its
coverage is incomplete), `INVENTORY_UNKNOWN` (the suite cannot be read with
certainty), `CONDITIONAL` (`#if`-registered), `STATIC` / `CHANGED` / `COVERAGE`
/ `RULE` (the evidence that selected it).

## Failure modes

Every row below costs CI minutes, not coverage.

| What happens | Result |
|---|---|
| No bundle for the merge base | Nearest bundled ancestor, up to 20 first-parent commits back. Otherwise no coverage, and everything runs. |
| Bundle fetch fails (token, API) | Warning, no coverage, and everything runs. |
| The PR reverts a change that main made after the ancestor | Revert guard: no coverage, and everything runs. |
| Coverage older than 7 days, or a platform/compatibility mismatch | Map refused: every suite runs whole. |
| Shallow checkout | `GIT_UNAVAILABLE`: everything runs. |
| Engine deadline (default 1800 s) or crash | The job fails. The test job gets no selection, so everything runs. |
| `select-tests` skipped (shadow) or failed | Everything runs (`!cancelled()` on the test job). |
| The selection names a suite or test that the build lacks | The test run **fails**: a plumbing fault, reported, never skipped. |

## Known limits

- Narrowing covers C-family source and C test conventions only. Everything
  else is decided by rules or runs everything.
- The coverage map format and its producer come from this repository's LLVM
  toolchain and harness. You need your own equivalent.
- One producer platform. The selection narrows only the leg that matches the
  producer's platform; every other leg runs everything. Narrowing more legs
  needs a producer (and a bundle) per platform.
- The weekly full refresh and the 7-day age limit are the same length. Each
  week, when the refresh runs late, there can be a short window in which maps
  are refused and every test runs.
- In merge bursts the producer skips commits. The ancestor fallback covers up
  to 20 of them.
- The answer schema is `cbm.test_impact.v0` and is marked experimental. It may
  change between releases. Pin the binary.
