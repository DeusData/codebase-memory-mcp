# API consolidation merge assessment and release record

## Decision dashboard

| Field | Current evidence |
| --- | --- |
| Purpose | Release record, benchmark report, and remaining publication plan for PR #1245 |
| Status | Product code, two-parent benchmarks, sanitizer lanes, and installation verified; final dual-ref publication, hosted CI, and PR metadata remain |
| Evidence baseline | macOS arm64, 2026-08-10; product tree `f0627b1c`; local candidate `add6e20`; published head `a60199ac` |
| Destination parent | `9953c328d1af27a836c533b399dc5b8ec08a22f5` |
| Exact final merge parents | First `a2e019fc9455632632bb15786a0dd6671ba3c054`; second/current upstream `aa6d740a8b7c7819c045d7bab67b69456994d70e` |
| Current upstream main | `aa6d740a8b7c7819c045d7bab67b69456994d70e`, confirmed by a fresh fetch on 2026-08-10 |
| Fork `origin/main` | `988d9758134a555d95a3adf154013a69a9856d18`, an ancestor of current upstream; the PR base truth is `upstream/main` |
| Original consolidation merge base | `10cb0e03fbb03fc62435174df5a52cad3186c444` |
| Exact final-parent merge base | `ad010b1656aa39c48a34b180e3501b9205a27ffd` |
| Consolidation merge | `32bc95314e2e9d64bb62211a78c16c58331c0588` |
| Current-main refresh merges | `29c7a2f1` joins `1f8ccb76` with `ad010b16`; `8778050` joins `a2e019fc` with current main `aa6d740a` |
| Recovery refs | Prior `refs/merge-recovery/` and `refs/merge-input/` pairs plus `pre-upstream-main-20260810-a2e019fc` and `upstream-main-20260810-aa6d740a` |
| Publication baseline | Remote `api-consolidation` and `api-consolidation-merge` both resolve to prior diagnostic head `a60199ac`; final publication must move both to the evidence commit containing this note |
| Decision needed | None for code composition. Release still requires green final-head CI and separate PR-metadata authorization. |

This note supersedes its earlier test counts, parent SHAs, benchmark paths, and
installation hash. Historical commits remain available in Git.

## Before, now, and target

| Area | Before | Verified now | Release target | Consequence |
| --- | --- | --- | --- | --- |
| Branch topology | Destination and refreshed upstream had independent changes after `10cb0e03` | `8778050` retains exact parents `a2e019fc` and `aa6d740a`; both release refs published signed diagnostic head `a60199ac` | Move both release branch names atomically to the exact final tip | Reviewers see one auditable superset history |
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
| First merge parent `a2e019fc` | Retains destination `9953c328`'s dependency indexing, PageRank, exact-delta and overlay indexing, richer extraction, activation transactions, daemon admission, JSON hot paths, diagnostics, tests, and benchmark coverage; also contains the earlier upstream refresh through `ad010b16` | It lacked the two newer upstream commits, including the Linux RSS correction | Use as the behavior, correctness, and hot-path preservation baseline |
| Second merge parent `aa6d740a` | Coherent embedded UI/templates, unsuffixed archive composition, wrapper/install corrections, Windows path handling, arm64 W^X behavior, release contracts, and OS-truth diagnostic RSS on Linux | Wholesale replacement would remove first-parent-only indexing, query, lifecycle, diagnostics, and benchmark work | Take the release, portability, and RSS-correctness delta, then compose it with first-parent behavior |

The original `10cb0e03` consolidation base to destination `9953c328` range
changes 350 files (179,660 insertions and 31,411 deletions); the same base to
current upstream changes 80 files (3,136 insertions and 11,672 deletions). The
exact final parents diverge at `ad010b16`, with 966 first-parent-only commits and
2 second-parent-only commits. Relative to `a2e019fc`, product merge `8778050`
changes only `src/foundation/diagnostics.c` (10 insertions and 3 deletions), while
retaining the first parent's broader code and tests. Investigation difficulty is
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

## Correctness, safety, and robustness evidence

| Gate | Final local result |
| --- | --- |
| Canonical ASan/UBSan on `add6e20` | 8,637 tests passed, 2 platform tests skipped; exit 0; no sanitizer or runtime-error report |
| Focused daemon ASan/UBSan on `f0627b1c` | Application 51, runtime 47, bootstrap 24, and IPC 47 tests passed |
| ThreadSanitizer on `f0627b1c` | 1,349 tests passed, 2 platform tests skipped; exit 0; no race report |
| Apple `leaks` on `f0627b1c` | 1,319 allocation-owning tests passed; 0 leaks for 0 total leaked bytes |
| MallocScribble and MallocPreScribble on `f0627b1c` | 8,637 tests passed, 2 platform tests skipped; no corruption crash |
| Guard Malloc on `f0627b1c` | 1,366 allocation-owning tests passed; no overrun or use-after-free crash |
| Clang static analyzer | Exit 0; no warning in an edited line range |
| Local lint | `lint-ci`, clang-format, cppcheck, NOLINT, and source-safety passed. LLVM 22 clang-tidy found no issue in either edited production source; its full-tree target reports 1,088 existing findings in unrelated extraction and CLI files. |
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

Three production `-O2` arm64 runners used the production allocator and strict
ad-hoc code signing. Candidate order rotated for two warmups and 41 measured
repetitions of startup, shared JSON, fixed C#, fixed Python, and Python scale
cohorts. The driver subtracts each matching startup sample from RSS, physical
footprint, retired instructions, and cycles.

| Runner | Source | SHA-256 |
| --- | --- | --- |
| Final product tree | `f0627b1c` | `adf1dab52f46b718889b618c5344ec7ea94fe825de6d4ad774b5e27f394f91ae` |
| First parent | `a2e019fc` | `dc1d8ecc6af78c7893e7df5247d0cf73f354149ab29c9b2e9cfb2ac032077d5a` |
| Second parent/current upstream | `aa6d740a` | `b5bd4b4bd9a3ffbac12f70a240a8295505374dfa889933998a22795062ba34cd` |

Result JSON: `native-extraction-results.json`, retained outside the repository.

Result SHA-256:
`acf52d4764cf737450d55ac567fcc916e1882528354d839511c91e245647ac16`

All five compared cohorts have exact output parity against both exact merge
parents. The table omits the startup resource baseline and gives medians in
final product / first parent / second parent order.

| Workload | Latency ms | Retired instructions | Incremental instructions | Incremental max RSS bytes | Incremental peak footprint bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Shared JSON, 1,024 extracts | 49.03 / 49.07 / 79.54 | 803,515,745 / 803,488,230 / 1,278,632,695 | 777,108,558 / 777,055,534 / 1,252,303,821 | 1,064,960 / 1,064,960 / 1,130,496 | 294,984 / 311,368 / 327,752 |
| C# fixed | 3.41 / 3.41 / 3.44 | 70,148,317 / 70,174,188 / 70,731,906 | 43,856,245 / 43,845,064 / 44,482,198 | 4,898,816 / 4,898,816 / 4,734,976 | 770,120 / 770,120 / 802,888 |
| Python fixed | 7.94 / 7.92 / 7.97 | 118,309,924 / 118,353,404 / 118,768,535 | 92,021,680 / 92,016,057 / 92,539,016 | 4,407,296 / 4,407,296 / 4,259,840 | 1,474,632 / 1,474,632 / 1,523,784 |
| Python scale, 8,000 calls | 519.8 / 518.7 / 1,190.1 | 9,698,224,935 / 9,698,721,813 / 19,478,797,591 | 9,671,743,245 / 9,671,974,558 / 19,452,575,158 | 42,893,312 / 42,909,696 / 42,844,160 | 40,140,920 / 40,157,280 / 40,255,608 |

### Performance decision

Against the first parent, the merged runner is practically equivalent. Python
scale paired differences are +0.30% latency, -0.004% retired instructions,
+0.40% cycles, -0.04% max RSS, and -0.04% physical footprint. Fixed C# latency
is equal; fixed Python and shared JSON latency are 0.13% and 0.10% lower. Shared
JSON's incremental footprint differs by one 16 KiB startup-subtraction page;
full footprint differs by 24 bytes and incremental max RSS medians are equal.
The benchmark workloads do not enable optional diagnostics, so these page-sized
differences do not indicate an algorithmic or retained-memory growth change.

Against the second parent, Python scale falls 56.33% in latency, 50.21% in
retired instructions, 50.28% in incremental instructions, 52.97% in cycles, and
0.24% in incremental physical footprint. Shared JSON falls 38.27% in latency,
37.16% in instructions, 5.80% in incremental max RSS, and 10.00% in incremental
physical footprint. Fixed C# and Python latency fall 1.17% and 0.63%. All
compared outputs are exact.

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
| Final `f0627b1c` | 5 of 5 | 982.366 ms |
| First parent `a2e019fc` | 5 of 5 | 14,422.502 ms |
| Second parent `aa6d740a` | 5 of 5 | 12,194.046 ms |

Final median latency is 93.19% lower than the first parent and 91.94% lower than
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
| Registry finalization scratch index | `O(F)` temporary memory for `F` functions, reclaimed with the supplied scratch arena | Same time bound; lower retained memory ownership than allocating into the result arena |
| Optional diagnostics RSS sample | `O(1)` time and `O(1)` space every five seconds while diagnostics are enabled | Same bounds as both parents; OS RSS helpers replace a Linux mimalloc counter that could wrap to a false exabyte-scale value; disabled request paths are unchanged |
| General indexing | Existing destination bounds in `P` and `G` | Destination algorithms and exact-delta behavior retained |
| Daemon session construction | `O(K)` MCP-session initialization and `O(K)` session storage; the required named store opens lazily after project context exists | Both exact parents perform `O(K*S)` temporary SQLite initialization through `sqlite3_open_v2()`, `configure_pragmas()`, `init_schema()`, and `create_user_indexes()` before immediately closing each store. Final removes the `S` factor and its transient allocations. |
| Concurrent daemon admission | Existing `O(K)` connection work and storage | No serial-generation, debounce, or polling structure added |

No candidate path has a worse asymptotic runtime, retained-memory, auxiliary-space,
latency, or output-growth bound than either parent. The benchmark scale fixture
confirms the Python hot-path change reduces retired work as input grows.

## Publication and CI plan

- [x] Pin both parents, merge base, recovery refs, and isolated parent worktrees.
- [x] Inspect both parents' actual code, first-parent changes, and lateral callers.
- [x] Create the signed two-parent merge and retain both parents as ancestors.
- [x] Preserve strict parsing, exact incremental indexing, released-hook
  migration, fail-loud rollback, and upstream packaging contracts with focused
  regressions.
- [x] Remove temporary per-session SQLite initialization at `f0627b1c` and pass
  canonical sanitizer, TSan, leak, Scribble, Guard Malloc, analyzer, lint,
  daemon, and cold-storm gates.
- [x] Build `f0627b1c` and both exact parent runners; complete the rotated
  41-repetition extraction matrix with exact output parity.
- [x] Complete five valid process-gated six-client cold-session samples for each
  production binary and record both parent comparisons.
- [x] Commit the Windows released-hook lifecycle correction as signed
  `a60199ac`; pass the local 355-test CLI/agent-client sanitizer lane.
- [x] Publish `a60199ac` atomically to both release branch names with exact
  force-with-lease guards; verify PR #1245 resolves to that head.
- [x] Commit `add6e20` after both Windows unit shards passed their unit tests and
  reproduced the same post-unit empty-runtime-parent timeout; pass the native and
  simulated-Windows worker transport guards and preserve the release binary hash.
- [ ] Obtain green hosted checks on the exact final head, including both Windows
  unit shards and the Windows daemon guard.
- [x] Commit the parent, benchmark, and sanitizer record as signed `96cf097`.
- [ ] Commit the final Windows/install closure with DCO, audit every owned
  non-merge commit after `aa6d740a`, then atomically republish both branch names
  with fresh exact leases.
- [x] Build and install with no active CBM process;
  verify build/install SHA-256 equality, mode 755, strict code signature,
  `--version`, `--help`, and MCP initialize.
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
