# API consolidation merge assessment and release record

## Decision dashboard

| Field | Current evidence |
| --- | --- |
| Purpose | Release record, benchmark report, and remaining publication plan for PR #1245 |
| Status | Live upstream v0.10.0 is merged; exact-head correctness, safety, DCO, parent benchmark, publication, and installation gates pass; a TDD fix for the first hosted Windows harness failure is locally green and awaits replacement-head CI |
| Evidence baseline | macOS arm64, 2026-08-10; product tree `f0627b1c`; merge `caf81288`; published evidence head `9dd18e53` |
| Destination parent | `9953c328d1af27a836c533b399dc5b8ec08a22f5` |
| Exact final merge parents | First `8b6d321953ad7f40083c2f3c7f5cf65daff6c81d`; second/current upstream `61b3b1b2ed740cbdb0a7ca6bd209d37a80135add` |
| Current upstream main | `61b3b1b2ed740cbdb0a7ca6bd209d37a80135add` (`v0.10.0`), confirmed by `ls-remote` and an explicit remote-tracking ref fetch on 2026-08-10 |
| Fork `origin/main` | `988d9758134a555d95a3adf154013a69a9856d18`, an ancestor of current upstream; the PR base truth is `upstream/main` |
| Original consolidation merge base | `10cb0e03fbb03fc62435174df5a52cad3186c444` |
| Exact final-parent merge base | `aa6d740a8b7c7819c045d7bab67b69456994d70e` |
| Consolidation merge | `32bc95314e2e9d64bb62211a78c16c58331c0588` |
| Current-main refresh merges | `29c7a2f1` joins `1f8ccb76` with `ad010b16`; `8778050` joins `a2e019fc` with `aa6d740a`; `caf81288` joins release head `8b6d3219` with live upstream `61b3b1b2` |
| Recovery refs | Prior recovery pairs plus `refs/merge-input/api-consolidation-pre-61b3-8b6d321` and `refs/merge-input/upstream-main-20260810-61b3b1b2` |
| Publication baseline | Remote `api-consolidation` and `api-consolidation-merge` both resolve to `9dd18e53`; replacement publication must move both atomically to the signed Windows harness fix |
| Decision needed | None for code composition. Release still requires green replacement-head CI and separate PR-metadata authorization. |

This note supersedes its earlier test counts, parent SHAs, benchmark paths, and
installation hash. Historical commits remain available in Git.

## Before, now, and target

| Area | Before | Verified now | Release target | Consequence |
| --- | --- | --- | --- | --- |
| Branch topology | Destination and upstream had independent changes after `10cb0e03`; a fetch that populated only `FETCH_HEAD` initially left the remote-tracking ref stale | `caf81288` retains exact parents `8b6d3219` and live upstream `61b3b1b2`; its only first-parent delta is the py-LSP benchmark-test union | Move both release branch names atomically to the exact final tip | Reviewers see one auditable superset history based on the actual upstream ref |
| Release packaging | Upstream carried the newer embedded UI/runtime-set design; destination carried rollback and richer installation behavior | One embedded binary, one four-member archive set, exact asset lookup, rollback, activation, and ownership contracts coexist | Keep all package and install checks green on the published head | No sidecar-era or variant-binary regression |
| Indexing and API behavior | Destination was ahead in dependency indexing, PageRank, incremental indexing, extraction, and diagnostics | Those paths remain, together with upstream UI, Windows, archive, and release hardening | Preserve both parents' useful behavior | The merge does not trade branch functionality for upstream freshness |
| Python registry filtering | Repeated indexed root-child scans scaled poorly with registry size | One circular `TSTreeCursor` performs root membership checks in `O(R*N)` time and `O(1)` auxiliary space | Retain exact class filtering for arbitrary registry order | Scale latency and retired work fall without output loss |
| Hook migration | Upstream static re-embedding retained only the older released SessionStart identity | The finite allowlist includes both exact released scripts | Remove only known owned historical scripts | User hooks are migrated without fuzzy deletion |
| Trust boundaries | Invalid UI/port inputs and rollback failures could be hidden by permissive or secondary paths | Shared strict parsing, pre-admission rejection, and propagated persistence/rollback errors fail loudly | Preserve the same errors in package and hosted tests | Bad input and partial persistence cannot look successful |

No code-composition decision remains. The release gate is green final-head CI,
matching refs, clean worktrees, and current PR metadata.

## Parent assessment

| Parent | Value and quality | Risk if taken wholesale | Disposition |
| --- | --- | --- | --- |
| First merge parent `8b6d3219` | Retains destination `9953c328`'s dependency indexing, PageRank, exact-delta and overlay indexing, richer extraction, activation transactions, daemon admission, JSON hot paths, diagnostics, tests, benchmark coverage, storeless daemon sessions, and the shared empty-runtime-parent fix | Its py-LSP benchmark still used sanitizer-adjusted 150/1,500 ms absolute timing assertions that upstream found flaky on same-SHA Ubuntu arm64 runs | Use as the behavior, correctness, and hot-path preservation baseline; replace only the flaky absolute benchmark gate |
| Second merge parent `61b3b1b2` | Retains upstream's embedded UI/templates, package and Windows contracts, OS-truth Linux RSS, and v0.10.0 release history; `e628d9d2` changes the py-LSP absolute assertion into a 30,000 ms liveness backstop while preserving the resolution-ratio performance assertion | Wholesale replacement would remove 975 first-parent-only commits, including indexing, query, lifecycle, diagnostics, benchmark, and release fixes | Take the bounded test-only liveness correction and retain the first parent's counters and product behavior |

The original `10cb0e03` consolidation base to destination `9953c328` range
changes 350 files (179,660 insertions and 31,411 deletions); the same base to
current upstream changes 80 files (3,136 insertions and 11,672 deletions). The
exact final parents diverge at `aa6d740a`, with 975 first-parent-only commits and
2 second-parent-only commits. Upstream's entire `aa6d740a..61b3b1b2` delta is
one substantive test commit plus its merge: 9 insertions and 8 deletions in
`tests/test_py_lsp_bench.c`. Relative to `8b6d3219`, merge `caf81288` changes only
that test (11 insertions and 14 deletions); the `src`, `internal/cbm`, `vendored`,
and `benchmarks` trees are byte-identical. Investigation difficulty is
**5/5** because many lifecycle, package, protocol, and performance contracts cross
file boundaries without textual conflicts. Completion difficulty is **4/5**: the
shared fixes are small, but exact-parent builds, sanitizer lanes, allocator checks,
installation, Windows evidence, DCO, and guarded publication are all required.

## Merge composition and lateral repairs

1. `32bc9531` composes embedded UI/templates and current package contracts with
   destination dependency indexing, PageRank, incremental indexing, richer
   extraction, activation rollback, daemon admission, and diagnostics.
2. `39578807` restores `O(log A)` UI asset lookup for `A` generated assets,
   enforces generated sort order, and routes daemon-control port parsing through
   the strict shared parser.
3. `8df9f9d` keeps one tree cursor across Python module-class epoch checks.
   It also allocates finalized method indexes from the supplied scratch arena,
   skips empty import-spec walks, and skips LSP timing work for languages with no
   per-file LSP pass.
4. `5504dcf` restores the exact released streamlined SessionStart migration
   identity. It also corrects a one-element test `argv` that was passed with
   `argc=2`, which the canonical ASan run exposed as a stack-buffer-overflow.
5. Publication rollback and cancellation report persistence failures instead of
   returning success after failed cleanup.
6. Package, protocol, schema, daemon, UI, activation, extraction, and benchmark
   callers were checked laterally so the repairs remain shared and finite.
7. `29c7a2f1` takes current main's single-composition smoke workflow exactly.
   Unix, Windows, and portable artifact legs set `SMOKE_REQUIRE_UI=1`, so an
   archive that loses its embedded UI fails instead of passing a reduced smoke.
8. `a2e019fc` applies only the five layouts required by `clang-format-20` in
   `src/main.c`, `src/pipeline/pipeline.c`, and `src/ui/embedded_assets.h`.
9. `8778050` takes upstream's Linux diagnostics fix through the already shared
   `cbm_mem_rss()` and `cbm_mem_peak_rss()` helpers. The merge retains this
   branch's process CPU fields, private output paths, retained-trajectory event,
   and export-based soak cache isolation.
10. `1a64a1f5` makes the smoke contract require the exact fail-loud
    `error: unknown update option: --standard` rejection. `be24566f` replaces a
    one-second PowerShell/WMI child probe with native Toolhelp enumeration for
    the Windows test scheduler; enumeration errors still fail closed.
11. `f0627b1c` removes the temporary SQLite memory store from daemon session
    construction. Public `cbm_mcp_server_new(NULL)` behavior is unchanged for
    stdio MCP, hook, UI, and embedded callers; only `application_session_open()`
    uses the internal storeless constructor before required project context is
    established.
12. `a60199ac` exercises the streamlined released SessionStart identity in the
    Windows migration lifecycle test. It changes only `tests/test_cli.c`, so it
    does not invalidate the production benchmark binaries built from
    `f0627b1c`.
13. `add6e20` removes `main.c`'s duplicate read of
    `CBM_TEST_DAEMON_RUNTIME_PARENT`. The shared bootstrap helper now owns its
    existing empty-is-unset rule for every frontend. Nonempty invalid paths still
    fail at the IPC boundary, and the seam-free production binary remains
    byte-identical to `f0627b1c`.
14. `caf81288` takes upstream's exact 30,000 ms py-LSP liveness backstop and
    retains this branch's definition, call, resolution, usage, type-reference,
    and read/write counters plus the 50% resolution-ratio assertion. The focused
    fixture passes with 55 of 56 calls resolved (98%) in 31.55 ms.
15. Both exact parents carry the same extensionless executable validation in
    `tests/test_daemon_open_readiness.py`, but only the first parent's stronger
    canonical test flow invokes it. Both hosted Windows shards completed their
    unit suites and then failed loudly because MSYS execution resolves `.exe`
    while Python's `os.path.isfile()` does not. `resolve_binary_path()` now uses
    the `.exe` sibling only on Windows and leaves missing paths on the existing
    exit-2 `SETUP FAIL` path.

## Correctness, safety, and robustness evidence

| Gate | Final local result |
| --- | --- |
| Canonical ASan/UBSan on `caf81288` | 8,637 tests passed, 2 platform tests skipped; exit 0; no sanitizer or runtime-error report |
| Focused py-LSP ASan/UBSan on `caf81288` | 1 test passed; 55 of 56 calls resolved (98%); 176 usages, 25 type references, 20 read/write edges; 31.55 ms |
| Focused daemon ASan/UBSan on `f0627b1c` | Application 51, runtime 47, bootstrap 24, and IPC 47 tests passed |
| ThreadSanitizer on `f0627b1c` | 1,349 tests passed, 2 platform tests skipped; exit 0; no race report |
| Apple `leaks` on `f0627b1c` | 1,319 allocation-owning tests passed; 0 leaks for 0 total leaked bytes |
| MallocScribble and MallocPreScribble on `f0627b1c` | 8,637 tests passed, 2 platform tests skipped; no corruption crash |
| Guard Malloc on `f0627b1c` | 1,366 allocation-owning tests passed; no overrun or use-after-free crash |
| Clang static analyzer | Exit 0; no warning in an edited line range |
| Local lint | `lint-ci`, clang-format, cppcheck, NOLINT, and source-safety passed. LLVM 22 clang-tidy found no issue in either edited production source; its full-tree target reports 1,088 existing findings in unrelated extraction and CLI files. |
| Top-level Python unit suite | 272 tests passed, 1 platform test skipped; the Windows extensionless-path regression is included |
| Windows readiness harness fix | Red before implementation; focused unit passed; a nonexistent binary still returns exit 2 with `SETUP FAIL`; the real three-case isolated UI fixture passed |
| DCO through this evidence commit | All 954 owned non-merge commits after live upstream `61b3b1b2` contain valid sign-offs |
| Daemon production lifecycle | Full stability guard passed; 20 consecutive six-client cold waves accepted 120 clients and retired each ephemeral daemon |
| CLI after `a60199ac` | ASan/UBSan CLI and agent-client lane passed 355 tests |
| Hosted `f0627b1c` checks | DCO, lint, analyze, CodeQL, diagnostics, LSan, PR smoke, package wrappers, TSan, dedicated Windows daemon guard, and completed Unix legs passed |
| Hosted Windows unit shards on `a60199ac` | Shard 1 passed 4,522 tests with 23 platform skips; shard 2 passed 3,812 tests with 47 platform skips. Both then exposed the same post-unit worker-transport defect: an empty test runtime parent reached IPC as an explicit path and daemon startup timed out. `add6e20` delegates the empty-is-unset contract to the shared bootstrap helper. |
| Installed production binary | Build/install SHA-256 `14ab8e54db1f735a9e8e69067ce88c319f5b69113c694f3da3a73c5d5064dd90`; mode 755; strict code signature, `--version`, `--help`, and MCP initialize passed. The only post-`f0627b1c` product-source change is compiled under `CBM_TEST_SEAMS`; the seam-free release binary remains byte-identical. |
| Production npm dependency audit | 0 vulnerabilities with `npm audit --omit=dev --audit-level=high` |
| Source secret scan | No match |

The leak and memory reports are `build/c/leak-report.txt` and
`build/c/mem-report.txt`. The verification host retains the complete command
logs outside the repository because subsequent diagnostic targets overwrite
`build/c/mem-report.txt`.

## Production benchmark evidence

### Method and artifacts

Three production `-O2` arm64 runners use the same neutral comparison harness,
the production allocator, and strict ad-hoc code signing. Its measurement main
and scale source are the exact pre-`8df9f9da` versions at `39578807`: they retain
output and resolution checks while omitting the final-only zero-LSP-timer
assertion and use the older 100x scale liveness ceiling so the slower parent can
be measured. The committed
35x final-branch complexity guard remains active in the canonical suite and
rejects upstream's observed 68--72x ratio. Candidate order rotates for two
warmups and 41 measured repetitions of startup, shared JSON, fixed C#, fixed
Python, and Python scale cohorts. The driver subtracts each matching startup
sample from RSS, physical footprint, retired instructions, and cycles.

| Runner | Source | SHA-256 |
| --- | --- | --- |
| Final merge `caf81288` | `caf81288` | `1d94e3f86b2ccece56ddcf018c6017b71df64499cd52fdec23b1acb2e3c01c95` |
| First parent | `8b6d3219` | `1d94e3f86b2ccece56ddcf018c6017b71df64499cd52fdec23b1acb2e3c01c95` |
| Second parent/current upstream | `61b3b1b2` | `5832ce4e1a68a9a90c21d773b79f0e9707e2194edbbdaf957c156958fe5252ff` |

The final and first-parent runners are byte-identical under the common harness;
any timing or allocator-counter difference between them is measurement noise,
not a code difference.

Result JSON: `native-extraction-results.json`, retained outside the repository.

Result SHA-256:
`cd9aa079e1b6010e826efc004dde43293b547209379aeddc22fcbe7d6f96dece`

All five compared cohorts have exact output parity against both exact merge
parents. The table omits the startup resource baseline and gives medians in
final product / first parent / second parent order.

| Workload | Latency ms | Retired instructions | Incremental instructions | Incremental max RSS bytes | Incremental peak footprint bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Shared JSON, 1,024 extracts | 50.17 / 50.19 / 82.44 | 803,943,485 / 803,962,371 / 1,312,783,374 | 777,478,490 / 777,468,758 / 1,286,445,772 | 1,064,960 / 1,064,960 / 1,130,496 | 294,984 / 311,368 / 327,752 |
| C# fixed | 3.60 / 3.57 / 3.58 | 70,414,041 / 70,576,190 / 70,649,018 | 44,046,718 / 44,032,078 / 44,368,139 | 4,882,432 / 4,866,048 / 4,751,360 | 770,120 / 770,120 / 819,272 |
| Python fixed | 8.20 / 8.16 / 8.27 | 118,265,820 / 118,308,162 / 119,001,270 | 91,776,812 / 91,791,818 / 92,737,315 | 4,423,680 / 4,407,296 / 4,276,224 | 1,474,632 / 1,474,632 / 1,540,168 |
| Python scale, 8,000 calls | 525.5 / 527.2 / 1,208.4 | 9,688,051,282 / 9,690,561,447 / 19,862,333,958 | 9,661,676,166 / 9,664,059,805 / 19,835,983,754 | 42,926,080 / 42,909,696 / 42,876,928 | 40,157,304 / 40,173,664 / 40,288,376 |

### Performance decision

Against the first parent, source-tree and runner hashes prove exact equivalence.
The independently scheduled samples quantify the host-noise envelope: paired
Python-scale differences are -0.11% latency, -0.006% instructions, -0.16%
cycles, +0.04% incremental max RSS, and 0.00% incremental footprint. The same
binary's fixed C# samples differ by +1.12% latency, establishing that sub-percent
and low-single-percent fixed-fixture deltas are not code regressions.

Against the second parent, Python scale falls 56.38% in latency, 51.22% in
retired instructions, 51.29% in incremental instructions, 53.09% in cycles, and
0.28% in incremental physical footprint; incremental max RSS differs by +0.11%.
Shared JSON falls 38.97% in latency, 38.76% in instructions, 39.57% in
incremental instructions, 38.09% in cycles, 5.80% in incremental max RSS, and
9.99% in incremental physical footprint. Fixed Python latency falls 0.72%.
Fixed C# is 0.02 ms (+0.56%) slower while retired instructions fall 0.33%; that
latency delta is smaller than the 0.03 ms difference between the byte-identical
final and first-parent runners. All 615 measured samples retain exact outputs.

The result is practical performance equivalence to the first parent and a large
scale-sensitive improvement over the second. No claim is made that every noisy
allocator-page counter is numerically lower.

### Daemon cold-session benchmark

The change-specific workload runs six concurrent `cli list_projects` one-shots
from no daemon. Each candidate had one warmup and five measured samples; the
harness required every compared binary process to be absent before and after
each sample. One earlier mixed-binary probe is excluded because it switched
builds before the account-global daemon exited and produced explicit build/cache
conflicts.

| Production binary | Valid samples | Median six-client cold-session latency |
| --- | ---: | ---: |
| Final `caf81288` product | 5 of 5 | 982.366 ms |
| First parent `8b6d3219` | 5 of 5 | 982.366 ms |
| Second parent `61b3b1b2` | 5 of 5 | 12,194.046 ms |

The final and first-parent production trees and release binaries are identical,
so the retained final samples are the exact first-parent result. The second
parent's product tree is identical to `aa6d740a`; `aa6d740a..61b3b1b2` changes
only `tests/test_py_lsp_bench.c`, so its retained process-gated samples remain
exact. Final median latency is equal to the first parent and 91.94% lower than
the second parent. The result file
`final-f0627b1-parents-cold-session-5.json` is retained outside the repository
with SHA-256
`19b74ef128f2a9d7412b424d7817dd771768dcc5b11cede25c151bbe005f4913`.

## Asymptotic bounds

Let `N` be top-level Python syntax nodes, `R` registry types, `A` UI assets,
`L` input length, `P` project files, `K` concurrent callers, and `G` emitted
graph size. Let `S` be the SQLite pragma, schema, and user-index initialization
work for one temporary store.

| Path | Candidate bound | Parent comparison |
| --- | --- | --- |
| Python module-class epoch | `O(R*N)` time, `O(1)` auxiliary space | Better than the destination/current pre-repair indexed-child restart behavior; exact reversed-order test passes |
| UI asset lookup | `O(log A)` time, `O(1)` lookup space after generated storage | Equal to destination and better than upstream's linear scan |
| UI and daemon port parsing | `O(L)` time, `O(1)` space | Equal growth bound; strict shared parser rejects prefixes and overflow |
| Empty import specification | `O(1)` | Avoids an unnecessary root cursor walk |
| Unsupported-language LSP gate | `O(1)` dispatch with no timer/atomic write | Removes empty instrumentation work |
| Py-LSP benchmark liveness backstop | `O(1)` test-only comparison after extraction | Equal to both parents; the merge changes only the constant threshold and retains the ratio assertion |
| Windows readiness executable resolution | `O(1)` time and `O(1)` space with at most two regular-file probes | Equal growth bounds; test-only and leaves the production tree and benchmark runners unchanged |
| Registry finalization scratch index | `O(F)` temporary memory for `F` functions, reclaimed with the supplied scratch arena | Same time bound; lower retained memory ownership than allocating into the result arena |
| Optional diagnostics RSS sample | `O(1)` time and `O(1)` space every five seconds while diagnostics are enabled | Same bounds as both parents; OS RSS helpers replace a Linux mimalloc counter that could wrap to a false exabyte-scale value; disabled request paths are unchanged |
| General indexing | Existing destination bounds in `P` and `G` | Destination algorithms and exact-delta behavior retained |
| Daemon session construction | `O(K)` MCP-session initialization and `O(K)` session storage; the required named store opens lazily after project context exists | Both exact parents perform `O(K*S)` temporary SQLite initialization through `sqlite3_open_v2()`, `configure_pragmas()`, `init_schema()`, and `create_user_indexes()` before immediately closing each store. Final removes the `S` factor and its transient allocations. |
| Concurrent daemon admission | Existing `O(K)` connection work and storage | No serial-generation, debounce, or polling structure added |

No candidate path has a worse asymptotic runtime, retained-memory, auxiliary-space,
latency, or output-growth bound than either parent. The benchmark scale fixture
confirms the Python hot-path change reduces retired work as input grows.

## Publication and CI plan

- [x] Pin live parents `8b6d3219` and `61b3b1b2`, merge base `aa6d740a`,
  recovery refs, and isolated worktrees.
- [x] Inspect both parents' actual code, the complete two-commit upstream delta,
  the semantic conflict, and affected lateral behavior.
- [x] Create signed merge `caf81288` with exact parents and retain the branch's
  counters, output checks, product fixes, and 35x scale guard alongside
  upstream's 30-second py-LSP liveness backstop.
- [x] Pass the exact-head 8,637-test ASan/UBSan suite, focused py-LSP test,
  lint, source-safety, and DCO audit while retaining the product-tree TSan,
  leak, Scribble, Guard Malloc, analyzer, daemon, and Windows evidence.
- [x] Build all exact-parent runners under one neutral harness; complete the
  rotated 2-warmup/41-repetition matrix with 615 exact-output samples and verify
  byte identity with the first parent plus scale improvement over upstream.
- [x] Revalidate cold-session evidence against the exact parents by release
  binary identity with `8b6d3219` and product-tree identity between `61b3b1b2`
  and measured `aa6d740a`.
- [x] Commit this final evidence record with DCO.
- [x] Atomically publish both release branch names with fresh exact leases to
  signed evidence head `9dd18e53`.
- [x] Build and install with no active CBM process; verify build/install hash
  equality, mode 755, strict code signature, `--version`, `--help`, and MCP
  initialize.
- [x] Diagnose both hosted Windows shards from their exact logs, add the smallest
  red extensionless-`.exe` regression, preserve fail-loud missing-path behavior,
  and pass the focused and full Python unit suites plus the real UI fixture.
- [ ] Commit and atomically publish the signed Windows harness fix, then obtain
  green hosted checks on that exact head, including both Windows unit shards and
  the Windows daemon guard.
- [ ] Publish the final PR title/body after separate authorization and verify its
  rendered metadata.

## Evidence limits

The code-graph transport returned `Transport closed` during this merge. Direct
source, parent diffs, history, tests, disassembly where needed, and retained
benchmark artifacts are the evidence authority.

Local macOS verification cannot replace native Windows lifecycle checks. The
published head is release-ready only after those hosted checks pass. Installation
ran only after the account had no active codebase-memory-mcp process; no daemon or
client was stopped for it.
