# API consolidation merge assessment and release record

## Decision dashboard

| Field | Current evidence |
| --- | --- |
| Purpose | Release record, benchmark report, and remaining publication plan for PR #1245 |
| Status | Local candidate and installation verified; guarded publication, PR metadata, and final-head hosted CI remain |
| Evidence baseline | macOS arm64, 2026-08-10; product source `5504dcf`; evidence commit `8208adda` |
| Destination parent | `9953c328d1af27a836c533b399dc5b8ec08a22f5` |
| Upstream parent | `4ed8d384f76b4945f1b50845d8b0e28c78ea304b` |
| Merge base | `10cb0e03fbb03fc62435174df5a52cad3186c444` |
| Two-parent merge | `32bc95314e2e9d64bb62211a78c16c58331c0588` |
| Recovery refs | `refs/merge-recovery/pre-upstream-main-20260810-9953c328`; `refs/merge-input/upstream-main-20260810-4ed8d384` |
| Release refs | `api-consolidation` and `api-consolidation-merge` must resolve to the same final commit |
| Decision needed | None for local content. Publication is gated on final install smoke tests and exact remote leases. |

This note supersedes its earlier 10cb0e03-era test counts, parent SHAs, benchmark
paths, and installation hash. Historical commits remain available in Git.

## Before, now, and target

| Area | Before | Verified now | Release target | Consequence |
| --- | --- | --- | --- | --- |
| Branch topology | Destination and refreshed upstream had independent changes after `10cb0e03` | `32bc9531` has exactly `9953c328` and `4ed8d384` as parents; both are ancestors of `5504dcf` | Publish both release branch names at one tip | Reviewers see one auditable superset history |
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
| Upstream `4ed8d384` | Coherent embedded UI/templates, unsuffixed archive composition, wrapper/install corrections, Windows path handling, arm64 W^X behavior, and current release contracts | Wholesale replacement would remove destination-only indexing, query, lifecycle, and benchmark work | Take the release and portability changes, then compose them with destination behavior |

The merge base to destination range changes 350 files (179,660 insertions and
31,411 deletions). The merge base to upstream range changes 78 files (3,113
insertions and 11,652 deletions). Investigation difficulty is **5/5** because
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

## Correctness, safety, and robustness evidence

| Gate | Final local result |
| --- | --- |
| Canonical ASan/UBSan | 8,637 passed, 2 skipped; exit 0; no sanitizer or runtime-error report |
| ThreadSanitizer concurrency matrix | 1,349 passed, 2 platform skips; exit 0; no race report |
| Apple `leaks` on the nosan runner | 0 leaks for 0 total leaked bytes |
| MallocScribble and MallocPreScribble | 8,637 passed, 2 skipped; no corruption crash |
| Guard Malloc allocation-owning suites | 1,366 passed; no overrun or use-after-free crash |
| Clang static analyzer | exit 0; no warning in a modified production file |
| CLI suite after the malformed-`argv` repair | 325/325 passed under ASan/UBSan |
| Python registry, LSP, and scale focus | 151/151 passed under ASan/UBSan |
| Benchmark driver tests | 9/9 passed |
| Go, npm, and PyPI wrappers | 42, 11, and 28 tests passed |
| Embedded readiness, archive, vendored, schema, daemon, UI, activation, and protocol gates | passed |
| Installed embedded-UI binary | build/install SHA-256 `30ed57eeee097a4aeff8f90a355b3da86c5159464586b0fb4794dfd17731d1f1`; mode 755; strict code signature, `--version`, `--help`, and MCP initialize passed |
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
| Candidate | `5504dcf` source | `dc1d8ecc6af78c7893e7df5247d0cf73f354149ab29c9b2e9cfb2ac032077d5a` |
| Destination | `9953c328` | `416eff2286cbcc5d7705e01097209467ce79dbb596f1d308f26ad6f554d936b7` |
| Upstream | `4ed8d384` | `0a898316c849917f952b330f6b82fdd9cab10b1ec74cd8acbf9def46aae01b5` |

Result JSON:
`/Users/athundt/.cache/codebase-memory-mcp/api-consolidation-final-20260810/results/final-stabilized-41/native-extraction-results.json`

Result SHA-256:
`ad21bfe84d0701d845d6b625f794c06692ddf7a6902efc88e04fa096dddc6b8f`

All four workloads have exact output parity against both parents. Values below
are medians in candidate / destination / upstream order.

| Workload | Latency ms | Retired instructions | Incremental instructions | Incremental max RSS bytes | Incremental peak footprint bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Shared JSON, 1,024 extracts | 49.05 / 49.01 / 79.27 | 803,404,990 / 801,234,593 / 1,278,503,305 | 777,097,059 / 774,914,800 / 1,252,293,393 | 1,064,960 / 1,015,808 / 1,146,880 | 294,984 / 311,368 / 344,136 |
| C# fixed | 3.39 / 3.42 / 3.44 | 70,178,887 / 70,381,243 / 70,708,239 | 43,897,296 / 44,058,863 / 44,497,127 | 4,898,816 / 4,898,816 / 4,751,360 | 770,120 / 786,504 / 802,864 |
| Python fixed | 7.88 / 7.91 / 7.99 | 118,322,330 / 118,465,081 / 118,740,070 | 92,052,545 / 92,169,953 / 92,533,743 | 4,407,296 / 4,456,448 / 4,276,224 | 1,474,632 / 1,491,016 / 1,540,168 |
| Python scale, 8,000 calls | 517.7 / 979.7 / 1,190.6 | 9,699,128,817 / 16,139,328,792 / 19,479,359,303 | 9,672,815,144 / 16,113,033,627 / 19,452,944,028 | 42,909,696 / 42,958,848 / 42,860,544 | 40,173,688 / 40,190,048 / 40,271,992 |

### Performance decision

Against destination, Python scale latency falls 47.21%, retired instructions
fall 39.91%, incremental instructions fall 39.97%, cycles fall 43.70%, and
incremental physical footprint falls 0.04%. Fixed C# and Python latency and
instructions also fall. Against upstream, Python scale latency falls 56.51%,
retired instructions fall 50.21%, cycles fall 53.22%, and incremental physical
footprint falls 0.24%.

Shared JSON is 38.23% faster than upstream with 37.16% fewer instructions and
14.28% lower incremental physical footprint. Relative to destination, shared
JSON differs by +0.23% latency, +0.27% instructions, +0.09% incremental cycles,
equal peak footprint, and -5.26% incremental peak footprint. These sub-1% work
counters are treated as practical equivalence under the rotated 41-pair run;
RSS moves by allocator pages and is not monotonic in the fixed cohorts.

The candidate therefore improves the scale-sensitive workload and total measured
work while retaining exact output. No claim is made that every noisy point
estimate is numerically lower.

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
- [x] Build the exact parent and candidate runners; run the full 41-pair matrix.
- [x] Commit product changes `8df9f9d` and `5504dcf` with DCO signoffs.
- [x] Commit the refreshed assessment with DCO; all 941 non-merge commits in
  `4ed8d384..8208adda` have author-matching `Signed-off-by` trailers.
- [x] Build and install the embedded-UI binary; verify mode, matching build/install
  hash, strict code signature, `--version`, `--help`, and MCP initialize.
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
