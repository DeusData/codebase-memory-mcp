# Upstream main 2c50c774 merge assessment and installation plan

## Decision record

| Field | Evidence |
| --- | --- |
| Destination branch | `api-consolidation-merge` |
| Destination parent | `845578613391d1482d1e488ea53dd5d2052a0bc7` |
| Incoming parent | `2c50c7741ec89dbcf43c2c85e005c0b58a4dbbf3` |
| Merge base | `a8c6b3f579e0de491bf10b4769f651e6401d69c0` |
| Recovery refs | `refs/merge-recovery/pre-upstream-main-20260808-84557861` and `refs/merge-input/upstream-main-20260808-2c50c774` |
| Evidence host | macOS arm64, 2026-08-08 |
| Local decision | Commit the prepared candidate as one two-parent merge; do not push |
| Release boundary | macOS installation is ready; native Linux and Windows CI remain required before a release |

The candidate should be committed. It preserves the destination branch's newer extraction,
daemon, MCP, Cypher, installer, and benchmark behavior while adding upstream's integration-asset
hardening. The incoming range is narrow, but its release-layout changes cross CLI activation,
installers, archive composition, smoke tests, Windows handling, and antivirus policy.

The merge was text-clean. That did not make it mechanically safe. Three cleanly merged paths would
have lost destination behavior or shipped an incomplete install: `install.ps1` needed the sidecar
inside the destination transaction, `src/cli/cli.c` needed the destination JSON renderer without
the embedded script bodies, and the asset needed both historical Claude SessionStart bodies.

## Parent assessment

| Parent | Value and quality | Risks or limits | Disposition |
| --- | --- | --- | --- |
| Destination `84557861` | Carries the more advanced extraction and MCP surfaces, transactional activation, explicit client/profile behavior, two released Claude session hook bodies, and extensive benchmark evidence. Its same-commit baseline passed 8,625 C tests, Python contracts, leaks, the analyzer memory gate, security, and benchmarks. | It still compiled integration script bodies into the binary and did not ship the hash-verified JSON sidecar. | Keep it as the behavioral and performance baseline. |
| Incoming `2c50c774` | Ten commits move eight integration templates into a 9,536-byte JSON asset, generate and compile its SHA-256, reject missing or modified assets, package the asset on Unix and Windows, remove a policy-bypass command from binary strings, and tighten VirusTotal reporting. Its isolated ASan/UBSan run passed 7,433 tests with four skips. | Its Makefile has no explicit `install` target, so `make install` follows an implicit rule and does not install the sidecar. Its fixed benchmark output uses the older lines/calls/resolved schema. | Take the security and packaging design, then compose it with destination install and CLI ownership. |

The incoming work has high release-security value and good local tests. Its integration quality is
medium until composed with the destination because a correct release archive is not enough: every
supported install route must place the sidecar where activation can find and verify it.

Investigation difficulty was 4/5. The incoming range changes 23 files with 1,470 additions and 438
deletions, while the destination has changed 343 files since the merge base. Implementation
difficulty was 3/5 because the merge had no text conflicts, but ownership, activation history,
Windows archive layout, and user-daemon isolation required source and lifecycle checks.

## Candidate behavior and lateral repairs

1. `assets/cbm-integrations.json` is the only script-template body source. The generated
   `cbm_integrations_hash.h` pins its SHA-256, and `integration_assets.c` fails closed when a
   candidate is missing, oversized, malformed, or modified.
2. `Makefile.cbm`, `install.sh`, `install.ps1`, `scripts/package-release.sh`, local smoke staging,
   VM smoke staging, and binary-composition checks all carry the sidecar. The PowerShell path keeps
   destination rollback and cleanup semantics.
3. `src/cli/cli.c` retains the destination JSON renderer and activation transaction. It reads
   templates through `integration_assets.h`; no script body remains compiled into production C.
4. `claude_session` retains two released bodies. Uninstall and ownership recognition therefore
   continue to recognize the destination branch's older SessionStart installation.
5. `sb_append_len()` in `src/cli/client_adapter.c` now handles a zero-length append without
   allocating, rejects `size_t` overflow before addition, and allocates when the buffer is null.
   This is the shared root fix for the Clang analyzer warning `Null pointer passed as 1st argument
   to memory copy function`.
6. `cbm_daemon_bootstrap_endpoint_new()` honors `CBM_TEST_DAEMON_RUNTIME_PARENT` only under
   `CBM_ENABLE_TEST_SEAMS`. The three process-watchdog scripts use a private, owner-only runtime
   parent, so a test build cannot collide with the user's account-wide installed daemon. Release
   builds do not contain this seam.
7. `run_native_extraction_comparison.py` accepts both fixed-output schemas and reports a missing
   output field as a difference. The previous parser aborted before measuring the incoming parent;
   silently treating absent fields as parity would also have been wrong.

## Correctness, safety, and installation evidence

| Gate | Result |
| --- | --- |
| Final ASan/UBSan C matrix | 8,629 passed, 0 failed, 2 skipped across 145 suites and 16 jobs |
| Parent, worker, and worker-error watchdogs | passed with private test runtime parents |
| Python contracts | 396 passed, 1 skipped, 61 subtests passed |
| Benchmark parser contracts | 9 passed, including legacy schema and absent-field comparisons |
| macOS leak gate | 0 leaks for 0 bytes in `build/c/leak-report.txt` |
| CI lint profile | no-skips policy, cppcheck, clang-format, NOLINT policy, source safety, and protocol stdout passed |
| Clang analyzer | repository scan exited 0 with 411 existing warnings; the changed client builder has no focused warning |
| Hash-pinned memory analyzer | `memory gate clean` |
| Security target | all eight layers passed, including 23/23 fuzz cases, install isolation, network policy, and vendored integrity |
| Explicit temporary install | binary mode 0755, sidecar mode 0644, sidecar byte-identical, strict code signature valid, `--version` runs |
| Darwin arm64 release artifact | package, extract, index, MCP framing, all-client install, ownership uninstall, tamper rejection, updater handoff, checksum, and orphan checks passed |

The final temporary install root was `/private/tmp/cbm-install-final.3ozJSn`. The release artifact smoke
indexed 9,039 nodes and 22,578 edges, then completed every standard-artifact phase. These tests used
isolated homes and runtime directories; they did not replace or stop the installed user daemon.

This evidence makes the candidate install-ready on macOS arm64. It does not establish native Linux
or Windows behavior. Docker is installed but its daemon socket is absent, and no Windows VM config
exists at `~/.claude/cbm-vm/config`.

The required external release lanes are already encoded in the repository:

- `_test.yml`: Ubuntu amd64/arm64, macOS arm64/Intel, Windows amd64, broad Windows ARM64, and TSan.
- `_build.yml`: dynamic Linux and macOS artifacts, Windows amd64/arm64, and static Linux portable
  amd64/arm64 artifacts.
- `_smoke.yml`: standard and UI artifacts on every core platform, broad Windows and macOS, portable
  old-glibc checks, security audits, and platform antivirus scans.

## Production benchmark evidence

### Method

The neutral runner used production `-O2`, production allocator binding, no sanitizers, and no test
API. Each candidate ran two warmups and 21 measured repetitions per suite. Candidate order rotated
each repetition. Every workload sample was paired with that candidate's no-work startup process so
the report could record both absolute and startup-adjusted memory.

The incoming fixed-suite output lacks the destination's definitions, usages, type references, and
read/write counts. The parser records those fields as absent instead of inventing zeros.

Final evidence:

- JSON: `/Users/athundt/.cache/codebase-memory-mcp/api-consolidation-merge-bench-20260808/inprocess/final-upstream-2c50/native-extraction-results.json`
- Raw process output: the adjacent `native-extraction-raw.txt`

Values below are medians. Each cell is destination / merge / upstream.

| Workload | Latency ms | Incremental max RSS bytes | Incremental peak footprint bytes | Retired instructions |
| --- | ---: | ---: | ---: | ---: |
| Shared parse, 1,024 extracts | 58.99 / 58.94 / 78.33 | 1,048,576 / 999,424 / 1,097,728 | 294,984 / 294,984 / 327,752 | 944,260,614 / 943,944,549 / 1,280,424,462 |
| C# fixed | 3.31 / 3.25 / 3.24 | 4,931,584 / 4,833,280 / 4,800,512 | 753,712 / 770,120 / 802,888 | 70,437,769 / 70,683,663 / 70,867,410 |
| Python fixed | 7.61 / 7.63 / 7.66 | 4,374,528 / 4,358,144 / 4,276,224 | 1,474,632 / 1,474,632 / 1,523,784 | 118,332,649 / 118,690,038 / 119,508,638 |
| Python scale, 8,000 resolved calls | 971.1 / 972.3 / 1,202.6 | 42,811,392 / 42,893,312 / 42,827,776 | 40,108,152 / 40,157,304 / 40,239,200 | 16,178,818,156 / 16,187,308,571 / 19,612,491,667 |

### Performance decision

Against the destination, all output counts are exact. The merge changes no extraction source under
`internal/cbm` or `src/pipeline`; its measured shifts are consistent with equivalent binaries and
host noise. Paired median latency is -2.09% for C#, -0.26% for Python, +0.11% at scale, and -0.31%
for shared parse. The scale changes are +0.19% incremental max RSS, +0.08% incremental physical
footprint, and +0.05% instructions. No destination performance regression is demonstrated.

Against upstream, shared parse latency is 24.88% lower and the scale latency is 18.97% lower. Scale
instructions are 17.46% lower. The fixed C# case is 1.26% slower by paired median, a 0.04 ms absolute
difference, while instructions are 0.23% lower and incremental physical footprint is 4.17% lower.
The fixed Python case is 0.40% faster, instructions are 0.57% lower, and incremental physical
footprint is 3.23% lower. Incremental max RSS is 0.68% higher for C# and 1.92% higher for Python;
these are within page-level variation and are not accompanied by retained physical-memory growth.

The merge also reports richer fixed-suite results: C# has 46 definitions, 99 usages, two type
references, and three read/write facts; Python has 56 definitions, 176 usages, 25 type references,
and 20 read/write facts. Calls and resolved calls match upstream at 54/50 and 56/55. The fixed C#
latency is classified as equivalent, not faster, because it performs and reports the larger result
surface.

No-work startup max RSS is 2,146,304 bytes for the merge, 2,129,920 for destination, and 2,064,384
for upstream. The merge and destination benchmark executables have identical Mach-O segment sizes;
their 16 KiB RSS difference is one page. The 80 KiB difference from upstream prices the retained
destination capability set and remains constant with project size.

## Asymptotic and resource bounds

Let `N` be project extraction work, `A` the integration asset bytes, `T` selected templates, `R`
released historical bodies, and `E` generated adapter output bytes. `A` is capped at 1 MiB; the
shipped asset is 9,536 bytes and has eight templates.

| Path | Candidate bound | Comparison with both parents |
| --- | --- | --- |
| Indexing, extraction, query, daemon request | Existing destination bounds in `N`; no changed hot-path source | Exact destination implementation retained; scale output is exact and latency is equivalent to destination and lower than upstream |
| First integration asset use | `O(A + T + R)` time and memory for read, SHA-256, JSON parse, and cache | Same design as upstream; bounded `O(1)` with respect to project size because `A` has a fixed 1 MiB cap |
| Cached template lookup | `O(T)` time, no new allocation | Eight current entries; bounded independently of project size and unchanged from upstream |
| Adapter generation | Amortized `O(E)` time and `O(E)` memory | Same class as both parents; the overflow and null guards add constant work and zero-length appends do less work |
| Install or activation persistence | `O(A)` sidecar copy plus existing transaction work | Adds one bounded file to destination; preserves upstream's sidecar cost and destination rollback behavior |
| Watchdog isolation seam | `O(1)` path selection in test builds only | Compiled out of release builds |
| Three-candidate comparison | Offline `O(P^2*S*R*M)` report work | No production runtime or memory cost |

No changed path worsens runtime or memory growth as a function of project size. The only added
production work is bounded release-asset verification during integration activation or install;
normal indexing, querying, and daemon request paths do not load the asset.

## Commit and release plan

- [x] Pin both parent commits, the merge base, recovery refs, and isolated parent worktrees.
- [x] Run clean parent correctness, install, and benchmark baselines.
- [x] Compose the sidecar with destination CLI rendering, activation transactions, install paths,
  historical ownership bodies, packaging, and platform smoke staging.
- [x] Add TDD canaries for the historical SessionStart body, watchdog runtime isolation, legacy
  benchmark output, and absent output metrics; validate the client builder with its adapter suite
  and a focused Clang analyzer run.
- [x] Run the full C, Python, leak, analyzer, memory, lint, security, installer, and release-artifact
  gates on macOS arm64.
- [x] Run the final 21-sample three-candidate production benchmark matrix and retain raw evidence.
- [ ] Verify the staged diff, create one DCO-signed two-parent merge commit, and confirm both pinned
  commits are direct parents.
- [ ] Run the repository's required Linux and Windows test/build/smoke matrices before release.
- [ ] Push only after the user explicitly requests it.

The local commit should use `84557861` as parent one and `2c50c774` as parent two. No additional
merge from upstream is needed.

## Evidence limitations

The code-graph coverage call returned `Transport closed`. Exact source, parent diffs, focused tests,
the full sanitizer matrix, and retained benchmark artifacts are the authorities for this decision.
Docker and the Windows VM were unavailable, so the note makes no native Linux or Windows execution
claim.
