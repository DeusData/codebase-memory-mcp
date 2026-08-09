# Upstream main 10cb0e03 merge assessment, performance record, and release plan

## Decision record

| Field | Evidence |
| --- | --- |
| Working branch | `api-consolidation-merge` |
| Destination parent | `cd412fa8b84a085ad9777b0ec045616af1bf3e5b` |
| Incoming parent | `10cb0e03fbb03fc62435174df5a52cad3186c444` |
| Merge base | `2c50c7741ec89dbcf43c2c85e005c0b58a4dbbf3` |
| Two-parent merge | `7d3f0cfc9658c71f249466469838600d46e9d4f4` |
| Nosan linkage repair | `779717e28c5848ac44cecad23dbbf5b7793e7740` |
| JSON linear-walk repair | `7486ba1359681d33875845717d10c1ad3ecd8428` |
| Recovery refs | `refs/merge-recovery/pre-upstream-main-20260809-cd412fa8` and `refs/merge-input/upstream-main-20260809-10cb0e03` |
| Evidence host | macOS arm64, 2026-08-09 |
| Local decision | Candidate is ready to push to both PR branches with exact leases; CI and installation remain |

The candidate is a semantic superset of both parents. It retains the destination branch's
dependency indexing, PageRank, incremental indexing, richer extraction, activation rollback,
fragmented SHA-256 handling, and bounded smoke-process lifecycle. It also takes upstream's
content-addressed UI asset packs, authenticated UI readiness, runtime-set locks, UTF-8 path
handling, and exact release/VirusTotal contracts.

This was not a safe “take incoming” merge. The parents conflicted in 11 files, and several clean
sections still required cross-file checks because build linkage, install transactions, sidecar
ownership, release archives, smoke fixtures, and Windows launch behavior form one contract.

## Critical parent assessment

| Parent | Value and quality | Risks or limits | Disposition |
| --- | --- | --- | --- |
| Destination `cd412fa8` | More advanced indexing and query implementation, dependency graph and PageRank, incremental correctness fixes, activation rollback, richer benchmark output, broad native tests, and prior performance evidence. | Lacked upstream's final external UI-pack/runtime-set release design and the newest Windows/VirusTotal hardening. Its JSON specialized walk also called output-free YAML handlers, creating a latent nested-input time-bound defect. | Keep as the behavioral, correctness, and hot-path baseline; repair the JSON traversal defect rather than accepting it as branch cost. |
| Upstream `10cb0e03` | Sixteen focused commits externalize UI assets, bind runtime sets, authenticate loopback readiness, harden Windows packaging, and make archive/VirusTotal evidence exact and content-bound. The release/security design is coherent and heavily contract-tested. | Does not contain the destination's later dependency indexing, PageRank, rollback, extraction result surface, or benchmark infrastructure. Its scale and shared-extraction runners are materially slower because those destination optimizations are absent. | Take the release/security design and compose it with destination behavior; do not replace destination subsystems wholesale. |

Investigation difficulty was **5/5**. The incoming side spans 128 first-parent-diff files with
20,699 additions and 2,674 deletions, while the destination has a much larger independent history.
Text resolution alone could not establish lifecycle safety or performance equivalence. Completion
difficulty was **4/5**: the code resolutions were bounded, but equivalent production runners,
two-parent benchmarks, sanitizer rebuilds, daemon lifecycle tests, package contracts, and DCO
history verification were all needed before a credible push.

## Merge composition and lateral repairs

1. `Makefile.cbm` links dependency indexing and PageRank together with the real UI asset parser in
   sanitizer, TSan, and nosan runners. `779717e2` fixes the nosan-only unresolved asset symbols that
   the first merge build exposed.
2. `src/cli/cli.c` keeps one path/sidecar transaction with destination rollback and upstream
   runtime-set, UTF-8, asset staging, and install/uninstall ownership checks.
3. `scripts/smoke-test.sh` preserves the destination FIFO stdin owner while probing upstream's
   external UI pack. Release, package, Windows bundle, no-embedded-script, venue, archive, and
   VirusTotal contracts remain active.
4. `tests/test_cli.c` retains destination fragmented SHA-256 and historical ownership coverage
   alongside upstream runtime-set, HMAC, secure-zero, and UI-asset tests.
5. `internal/cbm/extract_unified.c:2205` no longer invokes constant, YAML-nesting, or YAML
   infrastructure handlers from the JSON cursor. Those handlers emitted no JSON output; the YAML
   scanner recursively walked each nested JSON `document` or `array` subtree.
6. `tests/test_extraction.c:3795` proves URL string references still survive the specialized JSON
   walk. The existing TypeScript URL check now uses the same `has_string_ref()` helper.

## Correctness, safety, and robustness evidence

| Gate | Result |
| --- | --- |
| Canonical ASan/UBSan matrix, `scripts/test.sh` | 8,649 passed, 0 failed, 2 skipped across 145 suites and 16 jobs |
| Changed extraction suite within the full matrix | 327 passed, 0 failed |
| Parent/worker watchdogs, worker error transport, verified daemon UI readiness, security strings | passed |
| Python tests | 429 passed, 1 skipped, 78 subtests passed |
| macOS leak probe on extraction | 327 passed; 0 leaks for 0 total leaked bytes |
| MallocScribble/PreScribble JSON probe | 7 passed, 352 filtered |
| Guard Malloc JSON probe | 7 passed, 352 filtered |
| ThreadSanitizer selected concurrency lane | 1,348 passed, 2 skipped; no race report |
| Clang static analyzer | exit 0; accepted test-framework macro warnings; no finding in either changed file |
| CI lint profile | cppcheck, clang-format, NOLINT policy, source safety, and protocol stdout passed |
| Package wrappers before the extraction-only commit | Go passed; npm 29/29; PyPI 36/36 |
| Focused release contracts | smoke, package runtime, archive extraction, UI pack, vendored integrity, VirusTotal, Windows bundle, no-embedded-script, and venue parity passed |

The final two-file extraction commit cannot affect package wrapper code, archive composition, or
platform launchers. Those gates therefore remain valid for the merged release surface, while all
native and Python tests were rerun after that commit.

## Production benchmark evidence

### Method and artifacts

Each candidate used a production `-O2` native runner, production allocator binding, no sanitizer,
and no test API. Candidate order rotated on every repetition. Two warmups preceded 41 measured
repetitions for startup, shared JSON, fixed C#, fixed Python, and Python scale cohorts. Startup
resource use was paired with each workload sample before incremental memory was calculated.

- Confirmation JSON: `/Users/athundt/.cache/codebase-memory-mcp/api-consolidation-merge-bench-20260809/inprocess/post-json-linear-confirmation-41-upstream-10cb0e03/native-extraction-results.json`
- Raw output: adjacent `native-extraction-raw.txt`
- Earlier 21-run calibration: `/Users/athundt/.cache/codebase-memory-mcp/api-consolidation-merge-bench-20260809/inprocess/post-json-linear-21-upstream-10cb0e03/`

The destination runner is pinned to `cd412fa8`; the upstream runner is pinned to `10cb0e03`; the
merge runner contains `7486ba13` production sources. All shared and destination comparisons have
exact output parity. The fixed upstream cohorts intentionally differ because the merge retains the
destination's richer result surface.

Values below are 41-run medians, shown as destination / merge / upstream.

| Workload | Latency ms | Retired instructions | Cycles | Incremental max RSS bytes | Incremental peak footprint bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Shared JSON, 1,024 extracts | 61.81 / 51.02 / 81.72 | 944,031,323 / 807,246,004 / 1,284,135,173 | 230,576,749 / 191,402,971 / 302,523,482 | 1,032,192 / 999,424 / 1,146,880 | 311,344 / 295,008 / 327,776 |
| C# fixed | 3.57 / 3.56 / 3.59 | 70,526,266 / 70,497,498 / 70,851,355 | 21,463,685 / 21,537,164 / 21,498,955 | 4,849,664 / 4,915,200 / 4,849,664 | 770,120 / 770,120 / 802,888 |
| Python fixed | 8.63 / 8.56 / 8.45 | 119,225,690 / 119,065,639 / 119,976,146 | 40,886,276 / 40,827,037 / 40,806,119 | 4,374,528 / 4,440,064 / 4,292,608 | 1,474,632 / 1,490,992 / 1,523,784 |
| Python scale | 1,018.8 / 1,016.2 / 1,250.0 | 16,198,342,406 / 16,134,261,930 / 20,221,240,670 | 4,181,450,170 / 4,168,946,688 / 5,046,955,196 | 44,711,936 / 44,449,792 / 44,630,016 | 41,959,544 / 41,680,992 / 42,041,464 |

### Performance decision

Against the destination parent, shared JSON latency falls 17.27%, instructions fall 14.49%, cycles
fall 16.97%, incremental max RSS falls 1.64%, and incremental physical footprint is effectively
unchanged at -0.007%. Total extraction time falls 26.76%. Against upstream, the same cohort falls
37.61% in latency, 37.13% in instructions, 36.72% in cycles, 13.70% in incremental max RSS, and
13.04% in incremental physical footprint.

The scale cohort is also better than both parents: versus destination, latency is 0.32% lower,
instructions 0.39% lower, cycles 0.27% lower, and all four absolute/incremental memory medians are
lower. Versus upstream, latency is 18.82% lower, instructions 20.21% lower, cycles 17.38% lower,
and all four memory medians are lower.

Fixed C#/Python measurements differ by one to four allocator pages. The 21- and 41-run trials flip
the direction of the C# footprint difference, demonstrating page quantization and host variation
rather than a growing allocation. Fixed latency and instructions are lower than destination in the
41-run confirmation. These tiny fixed-memory shifts do not change the space bound; shared and
scale memory, where growth would be visible, are lower.

## Asymptotic bounds

Let `N` be JSON syntax nodes, `D` maximum nesting depth, `R` emitted references, `P` project files,
and `A` UI asset bytes.

| Path | Candidate bound | Comparison with both parents |
| --- | --- | --- |
| Specialized JSON extraction | `O(N + R)` time; existing tree/output storage, no new allocation | Parents could invoke a recursive YAML subtree scan at each nested JSON array/document, producing `N + (N-1) + ... = O(N^2)` time on a nested chain. The candidate performs one cursor walk and preserves exact output. |
| JSON auxiliary traversal memory | No added scanner allocation; recursion from the YAML helper is removed | Equal or lower than both parents; no new depth-dependent auxiliary structure is introduced. |
| General extraction/indexing | Existing destination bounds in `P` and emitted graph size | Destination implementation retained; exact scale output and lower instructions/latency/memory than both measured parents. |
| UI asset verification | `O(A)` time and memory with the upstream size cap | Same asymptotic class as upstream; bounded independently of project size. Destination did not have this release check. |
| Install/activation persistence | `O(A)` sidecar copy plus existing transaction work | Same bounded asset cost as upstream while retaining destination rollback. |
| Runtime-set locking/readiness checks | Constant metadata/lock work per invocation | Same class as upstream; no indexing/query hot-path cost. |

No candidate path worsens runtime, latency, or memory growth relative to either parent. The one
identified asymptotic defect is removed, and the production measurements show lower work on the
shared and scale cohorts without dropping destination output.

## Push, PR, CI, and installation plan

- [x] Pin both parents, merge base, recovery refs, and isolated parent worktrees.
- [x] Audit the DCO rewrite for tree and patch identity.
- [x] Resolve the 11 conflicts as a semantic union and create a signed two-parent merge.
- [x] Add the nosan linkage repair and JSON linear-walk repair as signed checkpoints.
- [x] Run the full native, Python, lint, analyzer, leak, scribble, Guard Malloc, and TSan gates.
- [x] Run 21- and 41-repetition three-candidate production benchmark matrices.
- [x] Add this ignored note as an intentional tracked assessment artifact.
- [ ] Rerun DCO over `upstream/main..HEAD` after the note's signed commit.
- [ ] Push `api-consolidation-merge` with an exact lease against remote `cd412fa8`.
- [ ] Fast-forward local `api-consolidation`, then force-with-lease its stale pre-DCO remote head
  `7433fee6` to the final signed candidate.
- [ ] Rewrite PR #1245's title/body from this evidence and verify its head/base commit IDs.
- [ ] Monitor every required GitHub check; repair any branch-specific failure before installation.
- [ ] Install from the final verified commit and verify executable mode, version, code signature,
  sidecar/UI assets, hash binding, and a real MCP request.

## Evidence limits

The code-graph `search_graph` and `check_index_coverage` calls returned `Transport closed` for the
worktree. Direct source, disassembly, parent diffs, tests, and retained benchmark artifacts are the
authorities for this assessment. Local macOS tests cannot replace native Linux/Windows CI, so the
installation decision remains contingent on the required GitHub matrix after the branch push.
