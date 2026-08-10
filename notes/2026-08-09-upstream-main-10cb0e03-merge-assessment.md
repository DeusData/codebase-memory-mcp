# API consolidation merge assessment and release record

## Decision dashboard

| Field | Current evidence |
| --- | --- |
| Purpose | Release record, benchmark report, and remaining publication plan for PR #1245 |
| Status | Exact current-main candidate, installation, sanitizer lanes, and two-parent benchmarks verified; guarded publication, PR metadata, and final-head hosted CI remain |
| Evidence baseline | macOS arm64, 2026-08-10; final product merge `8778050` |
| Destination parent | `9953c328d1af27a836c533b399dc5b8ec08a22f5` |
| Exact final merge parents | First `a2e019fc9455632632bb15786a0dd6671ba3c054`; second/current upstream `aa6d740a8b7c7819c045d7bab67b69456994d70e` |
| Current upstream main | `aa6d740a8b7c7819c045d7bab67b69456994d70e` |
| Merge base | `10cb0e03fbb03fc62435174df5a52cad3186c444` |
| Consolidation merge | `32bc95314e2e9d64bb62211a78c16c58331c0588` |
| Current-main refresh merges | `29c7a2f1` joins `1f8ccb76` with `ad010b16`; `8778050` joins `a2e019fc` with current main `aa6d740a` |
| Recovery refs | Prior `refs/merge-recovery/` and `refs/merge-input/` pairs plus `pre-upstream-main-20260810-a2e019fc` and `upstream-main-20260810-aa6d740a` |
| Release refs | `api-consolidation` and `api-consolidation-merge` must resolve to the same final commit |
| Decision needed | None for local content. Publication uses exact remote leases; PR metadata still requires its separate authorization. |

This note supersedes its earlier 10cb0e03-era test counts, parent SHAs, benchmark
paths, and installation hash. Historical commits remain available in Git.

## Before, now, and target

| Area | Before | Verified now | Release target | Consequence |
| --- | --- | --- | --- | --- |
| Branch topology | Destination and refreshed upstream had independent changes after `10cb0e03` | `32bc9531` joins `9953c328` with `4ed8d384`; `29c7a2f1` takes `ad010b16`; `8778050` takes current main `aa6d740a` | Publish both release branch names at one tip | Reviewers see one auditable superset history |
| Release packaging | Upstream carried the newer embedded UI/runtime-set design; destination carried rollback and richer installation behavior | One embedded binary, one four-member archive set, exact asset lookup, rollback, activation, and ownership contracts coexist | Keep all package and install checks green on the published head | No sidecar-era or variant-binary regression |
| Indexing and API behavior | Destination was ahead in dependency indexing, PageRank, incremental indexing, extraction, and diagnostics | Those paths remain, together with upstream UI, Windows, archive, and release hardening | Preserve both parents' useful behavior | The merge does not trade branch functionality for upstream freshness |
| Python registry filtering | Repeated indexed root-child scans scaled poorly with registry size | One circular `TSTreeCursor` performs root membership checks in `O(R*N)` time and `O(1)` auxiliary space | Retain exact class filtering for arbitrary registry order | Scale latency and retired work fall without output loss |
| Hook migration | Upstream static re-embedding retained only the older released SessionStart identity | The finite allowlist includes both exact released scripts | Remove only known owned historical scripts | User hooks are migrated without fuzzy deletion |
| Trust boundaries | Invalid UI/port inputs and rollback failures could be hidden by permissive or secondary paths | Shared strict parsing, pre-admission rejection, and propagated persistence/rollback errors fail loudly | Preserve the same errors in package and hosted tests | Bad input and partial persistence cannot look successful |

No content decision remains. The implementation gate is guarded dual-ref
publication and green final-head CI.

## Parent assessment

| Parent | Value and quality | Risk if taken wholesale | Disposition |
| --- | --- | --- | --- |
| Destination `9953c328` | More advanced dependency indexing, PageRank, exact-delta and overlay indexing, richer extraction, activation transactions, daemon admission, JSON hot paths, diagnostics, tests, and benchmark coverage | It lacked the refreshed upstream release composition and some later portability fixes | Use as the behavior, correctness, and hot-path baseline |
| Upstream `aa6d740a` | Coherent embedded UI/templates, unsuffixed archive composition, wrapper/install corrections, Windows path handling, arm64 W^X behavior, release contracts, and OS-truth diagnostic RSS on Linux | Wholesale replacement would remove destination-only indexing, query, lifecycle, diagnostics, and benchmark work | Take the release, portability, and RSS-correctness changes, then compose them with destination behavior |

The merge base to destination range changes 350 files (179,660 insertions and
31,411 deletions). The merge base to current upstream range changes 80 files
(3,136 insertions and 11,672 deletions). Investigation difficulty is **5/5** because
many lifecycle, package, protocol, and performance contracts cross file boundaries
without textual conflicts. Completion difficulty is **4/5**: the shared fixes are
small, but exact-parent builds, sanitizer lanes, allocator checks, installation,
Windows evidence, DCO, and guarded publication are all required.

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

## Correctness, safety, and robustness evidence

| Gate | Final local result |
| --- | --- |
| Canonical ASan/UBSan on `8778050` | 8,637 passed, 2 platform skips; exit 0; focused diagnostics 7/7 and memory/RSS 51/51 also passed; no sanitizer or runtime-error report |
| ThreadSanitizer concurrency matrix on `8778050` | 1,349 passed, 2 platform skips; exit 0; no race report |
| Apple `leaks` on the nosan runner | Full allocation-owning lane and exact merged-head diagnostics lane: 0 leaks for 0 total leaked bytes |
| MallocScribble and MallocPreScribble | 8,637 passed, 2 skipped; no corruption crash |
| Guard Malloc allocation-owning suites | 1,366 passed; no overrun or use-after-free crash |
| Clang static analyzer | exit 0; no warning in a modified production file |
| CLI suite after the malformed-`argv` repair | 325/325 passed under ASan/UBSan |
| Python registry, LSP, and scale focus | 151/151 passed under ASan/UBSan |
| Benchmark driver tests | 9/9 passed |
| Go, npm, and PyPI wrappers | 42, 11, and 28 tests passed |
| Embedded readiness, archive, vendored, schema, daemon, UI, activation, and protocol gates | passed |
| Installed embedded-UI binary from `8778050` | build/install SHA-256 `08c8125485cd46a240b958ddfeec436fd356fdf68f1ada3ba25105a48cb24443`; mode 755; strict code signature, `--version`, `--help`, and MCP initialize passed |
| Production npm dependency audit | 0 vulnerabilities with `npm audit --omit=dev --audit-level=high` |
| Source secret scan | no match |

The canonical logs are retained under
`/Users/athundt/Library/Application Support/rtk/tee/`. The leak and memory
reports are `build/c/leak-report.txt` and `build/c/mem-report.txt`; subsequent
targets overwrite the latter, so the RTK logs retain each complete run.

## Production benchmark evidence

### Method and artifacts

Three production `-O2` arm64 runners used the production allocator and strict
ad-hoc code signing. Candidate order rotated for two warmups and 41 measured
repetitions of startup, shared JSON, fixed C#, fixed Python, and Python scale
cohorts. The driver subtracts each matching startup sample from RSS, physical
footprint, retired instructions, and cycles.

| Runner | Source | SHA-256 |
| --- | --- | --- |
| Merged | `8778050` | `0c01baff77eb4f2abeaa3f8aba8d7e8fb2ab62d08045b5ccfa0559a5540d68d6` |
| First parent | `a2e019fc` | `dc1d8ecc6af78c7893e7df5247d0cf73f354149ab29c9b2e9cfb2ac032077d5a` |
| Second parent/current upstream | `aa6d740a` | `b5bd4b4bd9a3ffbac12f70a240a8295505374dfa889933998a22795062ba34cd` |

Result JSON:
`/Users/athundt/.cache/codebase-memory-mcp/api-consolidation-final-20260810/results/final-aa6d-parents-41/native-extraction-results.json`

Result SHA-256:
`aa4b0930907ec0d8e4dfcf79432053610b12aa68cb51fd89ad7b183996c700ae`

All four workloads have exact output parity against both exact merge parents.
Values below are medians in merged / first parent / second parent order.

| Workload | Latency ms | Retired instructions | Incremental instructions | Incremental max RSS bytes | Incremental peak footprint bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Shared JSON, 1,024 extracts | 49.03 / 49.03 / 79.40 | 803,503,444 / 803,460,727 / 1,278,571,646 | 777,142,702 / 777,124,750 / 1,252,339,746 | 1,064,960 / 1,064,960 / 1,130,496 | 311,368 / 294,984 / 327,752 |
| C# fixed | 3.40 / 3.40 / 3.44 | 70,184,869 / 70,194,052 / 70,706,640 | 43,868,794 / 43,918,495 / 44,519,872 | 4,915,200 / 4,898,816 / 4,734,976 | 786,504 / 770,120 / 802,888 |
| Python fixed | 7.94 / 7.93 / 8.00 | 118,314,141 / 118,567,672 / 118,744,994 | 92,038,490 / 92,099,899 / 92,544,047 | 4,423,680 / 4,407,296 / 4,259,840 | 1,491,016 / 1,474,632 / 1,523,784 |
| Python scale, 8,000 calls | 519.9 / 519.9 / 1,191.0 | 9,703,504,999 / 9,699,097,011 / 19,481,093,409 | 9,677,219,788 / 9,672,557,183 / 19,451,724,113 | 42,893,312 / 42,909,696 / 42,844,160 | 40,157,304 / 40,173,664 / 40,255,608 |

### Performance decision

Against the first parent, the merged runner is practically equivalent. Python
scale has equal 519.9 ms medians; paired differences are +0.10% latency, +0.03%
retired instructions, +0.04% incremental instructions, +0.14% cycles, -0.07%
incremental max RSS, and -0.08% incremental physical footprint. Shared JSON
differs by +0.04% latency, +0.002% instructions, and -0.03% cycles. Its +5.55%
incremental footprint difference is exactly one 16 KiB startup-subtraction page;
full footprint falls by 24 bytes and incremental max RSS medians are equal. The
benchmarked workloads do not enable optional diagnostics, so this is code-layout
and page-subtraction noise rather than algorithmic or retained-memory growth.

Against the second parent, Python scale falls 56.33% in latency, 50.19% in
retired instructions, 50.25% in incremental instructions, 53.11% in cycles, and
0.24% in incremental physical footprint. Shared JSON falls 38.08% in latency,
37.16% in instructions, and 4.99% in incremental physical footprint. Fixed C#
and Python latency fall 0.89% and 0.85%. All compared outputs are exact.

The result is practical performance equivalence to the first parent and a large
scale-sensitive improvement over the second. No claim is made that every noisy
allocator-page counter is numerically lower.

## Asymptotic bounds

Let `N` be top-level Python syntax nodes, `R` registry types, `A` UI assets,
`L` input length, `P` project files, `K` concurrent callers, and `G` emitted
graph size.

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
| Concurrent daemon admission | Existing `O(K)` connection work and storage | No serial-generation or polling structure added |

No candidate path has a worse asymptotic runtime, retained-memory, auxiliary-space,
latency, or output-growth bound than either parent. The benchmark scale fixture
confirms the Python hot-path change reduces retired work as input grows.

## Publication and CI plan

- [x] Pin both parents, merge base, recovery refs, and isolated parent worktrees.
- [x] Inspect both parents' actual code, first-parent changes, and lateral callers.
- [x] Create the signed two-parent merge and retain both parents as ancestors.
- [x] Add red tests for parser strictness, Python registry order/scale, scratch
  ownership, no-op LSP work, and released-hook migration.
- [x] Run canonical sanitizer, TSan, leak, scribble, Guard Malloc, analyzer,
  protocol, package, source-safety, and focused unit gates.
- [x] Build `8778050` and both exact parent runners; run the full rotated
  41-pair matrix with exact output parity.
- [x] Commit product changes `8df9f9d` and `5504dcf` with DCO signoffs.
- [x] Merge current upstream `ad010b16` and pass YAML, venue-parity,
  smoke-fixture, and Windows single-binary workflow contracts.
- [x] Merge current upstream `aa6d740a`; verify the diagnostics and memory/RSS
  suites, full TSan lane, diagnostics leak lane, and canonical lint.
- [x] Build and install `8778050`; verify matching build/install SHA-256, mode,
  strict code signature, `--version`, `--help`, and MCP initialize.
- [ ] Commit this final release record with DCO and validate every non-merge
  commit after `aa6d740a` with the repository DCO checker.
- [ ] Fetch both remote refs, record exact old object IDs, and publish both names
  atomically with `--force-with-lease=<ref>:<old>`.
- [ ] Verify both remote names and PR #1245's head resolve to the final commit.
- [ ] Replace PR #1245's title/body with the final evidence and monitor every
  required check, including native Windows daemon/UI lifecycle coverage.

## Evidence limits

The code-graph transport returned `Transport closed` during this merge. Direct
source, parent diffs, history, tests, disassembly where needed, and retained
benchmark artifacts are the evidence authority.

Local macOS verification cannot replace native Windows lifecycle checks. The
published head is release-ready only after those hosted checks pass. No
codebase-memory-mcp process was active during installation, so no daemon or client
was stopped.
