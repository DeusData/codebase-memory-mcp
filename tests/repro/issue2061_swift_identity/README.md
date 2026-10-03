# Original Swift issue #2061: standalone pipeline/store proof

## Candidate MCP presentation check (executed; FAILED)

`mcp_driver.c` is a separate, thin main using the existing MCP server API.
It is not registered in test suites or CI. Candidate
`0007c1d22858e1548ca392f382275595a0a2c691` was run once: build exit 0,
run exit 1 with a 1024-byte LeakSanitizer leak. Observations 1–4 passed;
observation 5's response/OBS and final SUMMARY were not fully saved and remain
unverified. The separate `4d99219e41c35a4dff2c1b0f211e423b913ac65f`
checkpoint was not the executed candidate. See the
[published failure record](https://github.com/DavidHLP/codebase-memory-mcp/blob/34a370ce2debd9ce5721ad99d8c7be67ef3ffa2b/docs/SWIFT_IDENTITY_VALIDATION.md#mcp-presentation-attempt--fail-2026-10-03).
The inherited generic MCP/Store leak is **OUT OF SCOPE**; original graph-layer
P RED/A GREEN is unchanged. Obtain fresh root authorization before any further
backup, build or execution; the five-observation attempt did not pass.

Inputs are the sealed, accepted A graph from candidate
`cf67f49dc2d718709846adefff5bab6cf9b671d4` and its original three-file fixture,
produced by harness `69e58c0df998e350d14fd9db8e8d205f7bf6036a`.
Their private locations are operator inputs, never committed artifacts.
The accepted project is `issue2061-swift-identity`. The graph DB SHA-256 is
`6e8389bdcacf62cf4956556329afc74f6312b6dbe69c3a12ffb1c0579587e4ce`;
fixture hashes are in `docs/SWIFT_IDENTITY_VALIDATION.md`.

Layout: a new ordinary evidence directory holds `source/`, `build/`, `tmp/`,
`cache/issue2061-swift-identity.db`, logs and a fixture copy. Nothing belongs
in Git. Never reuse, overwrite, delete or modify prior evidence. Record exact
source/driver SHAs, UTC, toolchain, commands, actual exits, stdout/stderr and
before/after hashes (including original DB/sidecars and fixtures). Log only
an explicit non-secret environment allowlist; other variables are redacted.

### Read-only consistent backup, after authorization

The accepted DB is closed and sealed; read-only lstat/hash preflight found no
`-wal`, `-shm` or `-journal`. Reconfirm those facts and absence of writers.
Use SQLite's backup API, not a lone DB-file copy. For this sealed no-sidecar
case, `mode=ro&immutable=1` prevents SQLite from writing the original or making
sidecars. Immutable mode must NOT be used if writers or a WAL are present:
stop and obtain a separately reviewed WAL-aware snapshot plan instead.

This template is not part of driver execution. `accepted_db`,
`accepted_fixture`, and `evidence` must be supplied privately; the destination
cache must be newly created and empty. Preserve/hash the original files and
copy the three fixture files separately without overwriting any destination.

```python
# Run only after root approval; no database access during syntax/preflight.
import os, sqlite3
from pathlib import Path
source = Path(os.environ["accepted_db"]).resolve(strict=True)
destination = Path(os.environ["evidence"]) / "cache/issue2061-swift-identity.db"
assert not destination.exists()
assert all(not os.path.lexists(str(source) + s)
           for s in ("-wal", "-shm", "-journal"))
# Operator also checks the accepted SHA-256 and that no writer is active.
fd = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
os.close(fd)  # Exclusive creation also refuses existing/broken symlinks.
uri = source.as_uri() + "?mode=ro&immutable=1"
with sqlite3.connect(uri, uri=True) as src:
    with sqlite3.connect(str(destination)) as dst:
        src.backup(dst)
```

The backup is logically identical; its physical hash may differ. Capture both
hashes. Do not rewrite project roots in it. Isolation comes from the independent
`CBM_CACHE_DIR` and DB copy: embedded MCP `search_graph`/`trace_path` do not
validate the stored root against `CBM_ALLOWED_ROOT`. The server session context
may retain the verified original fixture root, which remains read-only; it is
not a query-layer isolation check. The fixture copy preserves the source bytes.

### API and assertions

The driver calls `cbm_mcp_server_new(NULL)`, disables background tasks, selects
the analysis profile, sets explicit session/allowed roots, and submits
`tools/call` JSON-RPC through `cbm_mcp_server_handle`. Production resolve-store
opens the existing named DB in the independent `CBM_CACHE_DIR` query-only.
There is no indexing, direct store access, graph traversal or result filtering.

Five observations are collected:

1. `search_graph(^work$, json)`: exactly two identities at Service lines 5-7
   and 10-12, with labels/types in their QNs. The search matches the stored bare
   name; grouped presentation's `name` cell is the signature-bearing QN suffix.
2. `trace_path(target, inbound, depth=3, json)`: exactly flag overload at hop1,
   total1/relation eq; neither name overload nor false caller can be present.
3. The same target request with default tree output: exact singleton direct
   table, total1/relation eq, full flag QN and hop1, no additional rows/cursor.
   A presentation encoding change is a mismatch to inspect, not permission to
   silently relax the assertion.
4. `trace_path(onlyCallsOverloadB, outbound, depth=1, json)`: only name overload
   at hop1, total1/relation eq.
5. Name overload's exact QN, outbound depth1/json: zero callees, total0/eq.

Each request prints the original request and raw response. Envelope checks
require JSON-RPC 2.0, matching ID, no error, explicit isError=false and exactly
one text item. JSON requires structuredContent to equal the parsed text object;
tree requires structuredContent absent. All JSON rows are checked, along with
columns, totals, exact relation, and absence of truncation/continuation.
Expected summary: passed=5 failed=0 errors=0 exit=0. Exit1 means a response
mismatch; exit2 means setup/envelope failure. Neither establishes a P RED.

### Syntax/preflight now; build/run only after another review

Use an exact unmodified production archive, not the dirty checkout Makefile.
The existing `syntax.mk` supports this driver without modification:

```bash
make -f "$harness/syntax.mk" issue2061-syntax \
  CC=gcc CXX=g++ TEST_SEAMS=1 BUILD_DIR="$evidence/build" \
  ALL_TEST_SRCS="$harness/mcp_driver.c"
make -n -f Makefile.cbm "$evidence/build/test-runner" \
  CC=gcc CXX=g++ TEST_SEAMS=1 BUILD_DIR="$evidence/build" \
  ALL_TEST_SRCS="$harness/mcp_driver.c"
```

Preflight must show only the external driver as the test/main input, standard
production/grammar dependencies, no suite execution, and no flag changes.
Syntax/preflight success is not runtime acceptance. After root reviews the
pushed candidate, archive one approved full SHA into a fresh evidence directory;
retain the same GCC/G++ 16.2.1, Make 4.4.1 and standard test flags:

```bash
export TMPDIR="$evidence/tmp"
make -j8 -f Makefile.cbm "$evidence/build/test-runner" \
  CC=gcc CXX=g++ TEST_SEAMS=1 BUILD_DIR="$evidence/build" \
  ALL_TEST_SRCS="$harness/mcp_driver.c"
export CBM_CACHE_DIR="$evidence/cache"
export CBM_ALLOWED_ROOT="$accepted_fixture"
if ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
timeout 180s "$evidence/build/test-runner" issue2061-swift-identity \
  > "$evidence/run.stdout" 2> "$evidence/run.stderr"; then
  run_exit=0
else
  run_exit=$?
fi
printf '%s\n' "$run_exit" > "$evidence/run.exit"
# Preserve complete stderr; an info log is not a runner failure.
```

Stop on input/hash/SHA mismatch, unexpected sidecars/writers, nonempty existing
destination, expanded dry-run, compiler failure, timeout/signal, sanitizer
diagnostic, invalid/error response, missing rows or incomplete output. Normal
info stderr is not fatal. Preserve the checkpoint/evidence and report; do not
change production code, rerun P, or repair global baselines. This stage cannot
establish incremental multi-trailing-closure, full-suite, CI, DCO or maintainer
acceptance.

This runner embeds only the three original Swift files from
[issue #2061](https://github.com/DeusData/codebase-memory-mcp/issues/2061).
It uses the real production pipeline in FAST mode and queries its persisted
store. It neither implements a resolver nor copies a new resolver into P.
It is deliberately absent from `ALL_TEST_SRCS`, `TEST_REPRO_SRCS` and CI.

**Review this checkpoint on GitHub before building or running either side.**
Syntax checks do not establish RED/GREEN. This is a focused graph proof,
not a full gate or a test of the `trace_path` presentation layer.

The runner prints every observation and a final summary. It checks two
`work` nodes, their original file/line ranges, flag → target, name ↛ target,
caller → name, caller ↛ flag, and absence of the caller in target's inbound
CALLS traversal through depth three. Node lookup uses names, files and lines,
so it works with both P's unsuffixed QNs and A's suffixed QNs. Missing
endpoints produce unavailable edge observations (`-1`), never successful
absence checks. All semantic checks are collected rather than failing early.

Exit meanings: **0** = all observations pass; **1** = semantic mismatches
after successful indexing/store access; **2** = setup, pipeline or store API
failure. A compiler/linker failure, timeout, signal, or sanitizer diagnostic
is an infrastructure/runtime failure, not the target RED. Only an exit-1
run with the expected collapse/false-caller observations and no runtime
diagnostics establishes the baseline reproduction.

## Syntax only (before review)

From each exact source archive, using the same external checkpoint files:

```bash
make -f "$harness/syntax.mk" issue2061-syntax \
  CC=gcc CXX=g++ TEST_SEAMS=1 BUILD_DIR="$build_dir" \
  ALL_TEST_SRCS="$harness/main.c"
```

`syntax.mk` only includes the archive's unmodified `Makefile.cbm` and adds
one `-fsyntax-only` recipe using its standard `CFLAGS_TEST`. It builds no
objects, links no executable and runs no fixture. It does not change flags.

## Comparable P/A build and run (after root review)

Run in Bash from the repository root. Set `reviewed_checkpoint` to the full
SHA root actually reviewed. Use one checkpoint's harness for both sides.
All archives, binaries, logs and generated fixtures stay outside Git.
No worktree or tracked source modification is required.

```bash
set -euo pipefail
reviewed_checkpoint=REPLACE_WITH_ROOT_REVIEWED_FULL_SHA
evidence=$(mktemp -d /tmp/dav64-issue2061-XXXXXX)
mkdir "$evidence/harness"
git archive "$reviewed_checkpoint" tests/repro/issue2061_swift_identity \
  | tar -x -C "$evidence/harness"
harness="$evidence/harness/tests/repro/issue2061_swift_identity"
P=5538355530bb126c3041f7fcf8ef7a82b4bb3fec
A=cf67f49dc2d718709846adefff5bab6cf9b671d4
gcc --version > "$evidence/toolchain.txt"
g++ --version >> "$evidence/toolchain.txt"
uname -a > "$evidence/platform.txt"
sha256sum "$harness/main.c" "$harness/syntax.mk" > "$evidence/harness.sha256"

for side in P A; do
  sha=${!side}
  mkdir "$evidence/$side-source"
  git archive "$sha" > "$evidence/$side.tar"
  tar -xf "$evidence/$side.tar" -C "$evidence/$side-source"
  printf '%s\n' "$sha" > "$evidence/$side.commit"
  sha256sum "$evidence/$side.tar" > "$evidence/$side.archive.sha256"
  build_dir="$evidence/$side-build"
  (
    cd "$evidence/$side-source"
    make -n -f Makefile.cbm "$build_dir/test-runner" \
      CC=gcc CXX=g++ TEST_SEAMS=1 BUILD_DIR="$build_dir" \
      ALL_TEST_SRCS="$harness/main.c"
  ) > "$evidence/$side.build-plan.txt" 2>&1
done
```

Before compiling, inspect both plans: the only test source must be the
external `main.c`; compiler, effective flags and dependency source sets must
be comparable (normalize the P/A source/build paths). Stop and report any
unexplained difference. The standard target name `test-runner` is retained,
but `ALL_TEST_SRCS` replaces the entire test source list with this one main.
The archive's standard production/extraction/grammar dependencies and
sanitizer flags are retained. No all-tests runner or suite is compiled/run.

Then build **only the binary target**, never `test`, `test-focused` or
`test-repro`. For each side, record UTC start/end and the actual build exit:

```bash
for side in P A; do
  build_dir="$evidence/$side-build"
  date -u +%FT%TZ > "$evidence/$side.build-start"
  set +e
  (
    cd "$evidence/$side-source"
    make -j8 -f Makefile.cbm "$build_dir/test-runner" \
      CC=gcc CXX=g++ TEST_SEAMS=1 BUILD_DIR="$build_dir" \
      ALL_TEST_SRCS="$harness/main.c"
  ) > "$evidence/$side.build.stdout" 2> "$evidence/$side.build.stderr"
  rc=$?
  set -e
  printf '%s\n' "$rc" > "$evidence/$side.build.exit"
  date -u +%FT%TZ > "$evidence/$side.build-end"
  if [ "$rc" -ne 0 ]; then
    printf 'BUILD FAILURE on %s; not semantic RED\n' "$side"
    exit "$rc"
  fi
  sha256sum "$build_dir/test-runner" > "$evidence/$side.binary.sha256"
done
```

Use the same inherited environment for both runs. Record an allowlist of
relevant variables (compiler paths, sanitizer options, `CBM_*` resource/index
settings and locale), not a raw environment dump that might contain secrets.
Leave resolver controls enabled; do not disable LSP or alter graph behavior
to force a desired result. Use fresh per-side repositories/databases and
the same working directory. The run directory must not already exist.

```bash
for side in P A; do
  date -u +%FT%TZ > "$evidence/$side.run-start"
  set +e
  (
    cd "$evidence"
    ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
    UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
      timeout 180s "$evidence/$side-build/test-runner" "$evidence/$side-run"
  ) > "$evidence/$side.run.stdout" 2> "$evidence/$side.run.stderr"
  rc=$?
  set -e
  printf '%s\n' "$rc" > "$evidence/$side.run.exit"
  date -u +%FT%TZ > "$evidence/$side.run-end"
done
sha256sum "$harness/main.c" "$harness/syntax.mk" > "$evidence/harness-after.sha256"
cmp "$evidence/harness.sha256" "$evidence/harness-after.sha256"
for side in P A; do
  sha256sum "$evidence/$side-run/repo/Sources/"*.swift \
    > "$evidence/$side.fixture.sha256"
done
```

Compare fixture bytes between sides and with the issue, and verify archived
source manifests before/after execution. Retain raw stdout/stderr, actual
exits, timestamps, expanded build plans, toolchain/platform and fingerprints.
Inspect all observations and sanitizer output before attributing RED/GREEN.
The three-file graph has no dependency on a daemon or a new CLI architecture.
No P/A execution result is claimed by this checkpoint.
