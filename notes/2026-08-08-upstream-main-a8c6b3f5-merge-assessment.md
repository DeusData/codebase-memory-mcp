# Upstream main merge assessment and execution plan

## Decision record

| Field | Evidence |
| --- | --- |
| Branch | `api-consolidation-merge` |
| Status | Local two-parent merge committed and topology verified; no push |
| Destination parent | `55dc5ecfcc05672fa66d08e8a73d83d2e28e0380` |
| Incoming parent | `a8c6b3f579e0de491bf10b4769f651e6401d69c0` |
| Merge base | `d6be58ef9d43c574a2d1b0827ecc1e3c4846f0fe` |
| Recovery ref | `refs/merge-recovery/pre-upstream-main-20260807-55dc5ecf` |
| Evidence date and host | 2026-08-08, macOS arm64 |
| In scope | Exact behavior, parent capability retention, runtime, latency, peak memory, asymptotic bounds, tests, benchmark tooling, and merge topology |
| Out of scope | Pushes, remote changes, ranking-default changes, and claims about native Linux or Windows behavior from a macOS run |

Decision: retain the candidate as one two-parent merge. It preserves useful behavior from both
parents, repairs defects exposed only after composition, and passes the local correctness, leak,
analyzer-memory, CI-lint, security, and production benchmark gates. Native Linux and Windows CI
remain release gates because this host cannot reproduce their allocator, process, and path semantics.

The active merge started with 57 conflicted files and 203 conflict markers. Those figures understate
the work because several files merged textually but still violated shared publication, lifecycle,
or extraction contracts. Replacing the candidate with either parent's file set would discard
tested behavior from the other parent.

## Value, quality, and integration difficulty

| Change group | Value | Quality | Difficulty | Disposition |
| --- | --- | --- | --- | --- |
| Exact and overlay incremental publication | Very high | Destination has stronger stale-state, parity, and failure canaries | Very high | Destination remains the publication authority; incoming closure and surface behavior feed that path |
| Per-file LSP surfaces and semantic references | Very high | Incoming adds exact resolved-call and usage behavior across several languages | Very high | Retained with destination import maps, pass ownership, schema, and publication contracts |
| Daemon, activation, and subprocess lifecycle | Very high | Destination passes the readiness case that fails on incoming; incoming adds rejection and attribution checks | High | Composed state transitions, cleanup, and exact-build identity checks |
| Cypher ordering, traversal, and typed errors | High | Both parents contain independent parser and runtime corrections | High | Unioned under one grammar/runtime authority with trail and precision canaries |
| Workspace trust and platform safety | High | Incoming adds root classification, grants, spawn, and path handling | Medium-high | Shared boundary retained; native Linux and Windows execution remains required |
| CLI, MCP, and client adapters | Medium-high | Incoming consolidates adapter behavior; destination carries later profile and activation behavior | Medium-high | One adapter and registry path retained across clients |
| Test and CI expansion | High maintenance value | Parent tests exposed real composition defects, including daemon readiness and SystemVerilog duplicate calls | Medium | Useful parent coverage retained; corrected contracts replace obsolete test spellings |
| Production benchmark support | High merge-safety value | Parent tooling did not provide an in-process, output-aware, three-candidate comparison | Medium | Added an `-O2` runner, rotated samples, startup-normalized memory, paired deltas, raw receipts, and parser tests |

Overall quality is strong with two explicit limits. First, the incoming parent has one reproduced
daemon-readiness failure, so the merged lifecycle behavior must follow the destination state
machine. Second, platform-specific CI is still necessary. Investigation difficulty was 4/5 and
implementation difficulty was 5/5 because both parents changed the same semantic authorities after
the merge base. Local integration is complete; native platform release gates remain.

## Semantic decisions and repairs

1. `src/pipeline/pipeline.c` and the destination exact/overlay model own publication.
   Incoming closure planning and LSP surfaces enter through that authority; `pipeline_delta.c` is
   not a second publication engine.
2. Full pipeline input reports `CBM_PIPELINE_PUBLISH_FULL`. A mechanically merged assertion briefly
   expected an incremental-exact result for full input and was corrected to match the API contract.
3. SystemVerilog standalone `subroutine_call` nodes remain supported. A nested
   `subroutine_call` under `function_subroutine_call` is suppressed so one source call emits one row.
4. `cbm_extract_file_with_options_ex()` balances exactly one crash-journal start with one completion.
   The shared release helper owns composite and cross-state cleanup.
5. Call metric attribution uses a source-ordered linear path for non-overlapping definitions, a heap
   for overlapping definitions, and an exact linear fallback. Nested-call tests require attribution
   to the innermost callable.
6. Complexity and MinHash traversals no longer stop at fixed 4,096-frame, 2,048-node, or 4,096-token
   prefixes. They use tree cursors and growable depth storage, preserving exact output on larger ASTs.
7. The JSON fast path no longer zeroes the full unified `WalkState`; string handlers receive only the
   enclosing function pointer they consume. This removes unused lexical-scope state from that path.
8. Daemon readiness uses the merged readiness bound. Cancellation retains its separate 3-second
   bound, so startup tolerance does not weaken shutdown behavior.
9. The two merged pipeline state structs are reordered by alignment class. Clang's padding analyzer
   no longer reports their former 34-byte layouts, removing 32 avoidable bytes per instance without
   changing field identity, initialization, ownership, or complexity.

## Correctness and safety evidence

| Gate | Result |
| --- | --- |
| Destination parent ASan/UBSan matrix | 8,014 passed, 2 skipped at `55dc5ecf` |
| Incoming parent ASan/UBSan matrix | 7,429 passed, 1 daemon-readiness failure, 4 skipped at `a8c6b3f5` |
| Initial mechanical merge | 8,602 passed, 17 failed, 2 skipped |
| Final ASan/UBSan parallel matrix | 8,625 passed, 0 failed, 2 skipped across 145 suites |
| Focused extraction matrix | 358 passed, including the NULL-context and deep/wide exactness canaries |
| Python contracts | 394 passed, 1 skipped, 61 subtests |
| Native comparison parser and normalization contracts | 7 passed |
| macOS leak gate | 1,317 passed; 0 leaks for 0 bytes in `build/c/leak-report.txt` |
| CI lint profile | passed no-skips, cppcheck, clang-format, NOLINT policy, source-safety, and protocol-stdout checks |
| Clang static analyzer | exited 0; the hash-pinned Clang memory gate reports `memory gate clean` |
| Security target | passed static, binary-string, UI, isolated-install, network-policy, 23/23 fuzz, and vendored-integrity checks |
| Full clang-tidy comparison | destination and incoming baselines fail with 6,277 and 5,563 errors; six actionable merge-composition findings were repaired and their targeted rerun is clear |
| `git diff --check` | passed on the final staged tree |

The inspected registered C test-name delta consists of corrected contracts or stronger replacement
tests rather than silently omitted behavior.

## Production benchmark evidence

### Method and authority

- Build: production `-O2`, production allocator binding, no sanitizers or test-only APIs.
- Candidates: the three pinned executables named in the decision record.
- Matrix: 21 measured repetitions per candidate and suite, 2 warmups, candidate order rotated each
  repetition.
- Correctness: output counts are compared before timing is interpreted.
- Memory: every repetition includes a no-work startup process. The report subtracts that candidate's
  matching startup RSS and physical footprint from each workload peak.
- Final JSON:
  `/Users/athundt/.cache/codebase-memory-mcp/api-consolidation-merge-bench-20260808/inprocess/final-post-gates-21/native-extraction-results.json`
- Raw receipt: the adjacent `native-extraction-raw.txt`.
- Earlier-source, higher-sample confirmation (supporting evidence, not the final authority):
  `/Users/athundt/.cache/codebase-memory-mcp/api-consolidation-merge-bench-20260808/inprocess/confirmation-41/native-extraction-results.json`

On macOS, peak footprint is the physical working-set signal. Maximum RSS also includes executable
mappings, so both absolute and startup-normalized values remain in the artifact.

### Merged versus incoming, equal output

All four workloads have exact output parity between incoming and merged.

| Workload | Merged median latency | Paired latency | Paired instructions | Paired incremental peak footprint | Paired incremental max RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| Shared JSON, 1,024 extracts | 61.63 ms | -24.128% | -26.317% | -9.522% | -5.882% |
| C# fixed | 3.45 ms | -0.852% | -0.340% | -5.999% | +4.152% |
| Python fixed | 8.00 ms | -0.743% | -0.415% | -4.255% | +1.136% |
| Python scale, 8,000 resolved calls | 972.0 ms | -17.035% | -18.642% | -0.326% | -0.038% |

Latency and retired instructions are lower on every parity workload, with the fixed fixtures inside
a 1% equivalence band. Startup-normalized peak physical footprint is lower on every workload.
Normalized max RSS is slightly higher on two cohorts, but max RSS includes mapped executable
pages on this host; the physical-footprint counter does not show retained-memory growth.

### Merged versus destination

Only the shared JSON workload has output parity with destination. On that cohort, merged is 11.280%
lower in paired latency and 13.674% lower in instructions. Startup-normalized peak footprint is equal
and max RSS is 1.538% lower. Absolute footprint is 1.940% higher because the superset executable
starts with 2.352% more mapped pages; the paired startup subtraction accounts for those pages.

The other destination comparisons are capability deltas, not performance claims:

- C# destination emits 40 usages; merged emits 99.
- Python fixed destination resolves 52 calls and emits 210 usages; merged resolves 55 and emits 176.
- Python scale destination resolves 6,000 of 8,000 calls; merged resolves all 8,000.

Raw latency and memory for those three workloads price different work and cannot establish a
regression or a speedup.

## Asymptotic bounds

Let `N` be visited AST nodes, `C` calls, `D` callable definitions, `H` tree depth, `L` semantic
leaves, `K` signature length, and `U` unique tokens.

| Path | Parent bound or defect | Candidate bound | Result |
| --- | --- | --- | --- |
| JSON specialized traversal | `O(N)` time; incoming carries the larger unified state | `O(N)` time, `O(1)` traversal scratch plus output | Same asymptotic bound, lower touched stack constant |
| Function complexity | Destination `O(N)` with wider scratch; incoming truncates after a fixed frame limit | `O(N)` time, `O(H)` explicit scratch | Exact beyond the incoming cap; time is no worse |
| MinHash tree signature | Incoming fixed node/token prefixes can truncate; destination is exact | `O(N + L*K)` time, `O(U + H)` scratch | Exact destination bound retained |
| Call metric attribution | Parent normal path can scan definitions per call | `O(C + D)` for ordered non-overlap; `O((C + D) log D)` and `O(D)` scratch for overlap; exact `O(C*D)` fallback | Normal case is lower; worst case is no worse than the parent scan |
| SystemVerilog wrapper suppression | Mechanical union can emit two rows | `O(1)` parent-kind check per candidate call | Exact one-call output with constant work |
| Benchmark comparison | Not present in either parent | `O(P^2*S*R*M)` offline for candidates, suites, repetitions, and metrics | No production extraction cost |

No changed production path has a worse asymptotic runtime or memory bound than both parents. Tests
cover more than 4,096 sibling branches, nested callable attribution, and 8,000 resolved scale calls.

## Execution plan and handoff

- [x] Pin destination, incoming, merge base, recovery ref, and isolated parent worktrees.
- [x] Resolve conflicts in dependency order: foundation/storage, publication, extraction/LSP,
  Cypher, daemon/CLI/MCP, then tests, CI, and docs.
- [x] Audit clean merges for schemas, registrations, defaults, generated clients, platform branches,
  and lifecycle cleanup.
- [x] Establish correctness and production benchmark baselines for both parents.
- [x] Repair composition defects with focused regression tests.
- [x] Remove changed-path traversal caps and verify time/memory bounds from source and adversarial tests.
- [x] Run the full three-candidate production matrix and a 41-sample confirmation.
- [x] Run ASan/UBSan, Python, leak, CI-lint, Clang analyzer, hash-pinned memory, security, and
  patch-integrity gates.
- [x] Rebuild the final production runner and repeat the 21-sample matrix after all source repairs.
- [x] Stage the complete merge and create one signed two-parent commit with concrete source and test
  evidence.
- [x] Verify both pinned commits are direct parents and ancestors, the worktree is clean, and no push
  occurred.
- [ ] Run native Linux and Windows CI before release.

## Limitations

The code-graph coverage call returned `Transport closed` during final verification. Exact source,
parent diffs, focused tests, the full sanitizer matrix, and retained benchmark artifacts are the
authority for the affected paths. Docker was installed but its runtime was not active, so no
container matrix was started. Neither limitation changes the local merge decision, but both are
recorded to prevent broader claims than the evidence supports.

No push is part of this plan.
