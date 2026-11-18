# Neo4j fork feature-parity validation

Validated on Linux x86-64 with GCC 14 on 2026-11-06. The core MCP feature comparison uses a trusted build of fork main `67518b6f` before executable renaming. The official upstream 0.12.1 release is additionally used for installed coexistence and embedded-UI compatibility.

## Current results

| Surface | Result |
| --- | --- |
| Complete native suite, ASan + UBSan with `halt_on_error=1` | **9,995 passed, 0 failed, 10 platform skips; all 192 suites in one complete run** |
| Canonical static contract gate | **43 passed**, including the new Docker entrypoint regression |
| Original versus renamed MCP | All **17 tools**, capabilities, prompt definitions, input schemas, and stable graph/source/ADR/trace results match after documented identity/path changes |
| Official upstream API | Every advertised upstream tool and input property is retained, with compatible argument types/enums and no additional required arguments |
| Real MCP workflows | All indexing modes, background indexing/status polling, graph search/query/comparison, cursor continuation, traversal, snippets, outlines, schema, architecture, coverage, change impact, ADR writes/section updates, trace ingestion, and project deletion passed |
| CLI and configuration | Graph search and configuration set/get/reset passed through both executables |
| Language/infra fixture | Python, C, Go, Java, TypeScript, Rust, C#, Ruby, Perl, Markdown/RST, Docker/Compose, and Terraform inputs; language symbols verified with label-neutral discovery |
| Restricted profiles | Analysis/scout catalogs match; attempted indexing is rejected and leaves no project behind |
| Persisted state | Distinct cache overrides and artifact directories; the fork preserves upstream artifact bytes |
| Embedded UI | Real embedded page, HTTP JSON-RPC project listing, preferences, and graph-health endpoints passed against official upstream and the UI-enabled fork |
| Installed coexistence | Reinstall/update, native updater handoff, simultaneous daemons, and fork uninstall preserve upstream executable, updater, registrations, skills, agents, hooks, and plugins across the five exercised clients |
| Live Neo4j | **29 passed** with strict UBSan against disposable Docker Neo4j 5.26 |
| Docker graph image and runtime | Full image build passed. With the corrected entrypoint layered on that source-built image, fresh-volume indexing, repeated indexing, and graph search passed; results explicitly report Neo4j as the source |
| Frontend | **4,001 unit tests passed**; TypeScript/Vite production build passed |
| Package wrappers | Python **45 passed**; npm **20 passed, 3 skipped**; Go tests passed |
| Changed first-party implementation C files | clang-format 20 and cppcheck 2.20.0 checks passed |
| Vendor integrity | Passed after recording the two intentional scanner source patches |

The strict full native run used the canonical compiler/sanitizer flags and vendored objects, compiled as separate objects for parallelism, then the repository's suite-enumerating parallel harness. Build configuration confirmed sanitizers and test seams enabled. No assertions or sanitizer checks were disabled to obtain the result. Test processes ran under a Linux child subreaper with permission to create Unix sockets. The previous plain-build-only GCC diagnostic downgrade was not needed for this strict run.

## Defects found and corrected

- The OpenHands fixture used the executable name as an MCP registration key, and an archive negative test targeted the wrong member. Both are corrected.
- A Helm chart-image path passed a null key to `strlen`; direct nullable-pointer guarding passed the previously crashing 184-test grammar/Helm suite.
- Strict UBSan exposed zero-length operations on null buffers/tables in callable signatures, Perl sorting, and ObjectScript scanner deserialization. Guards preserve nonempty behavior. A direct scanner-state regression failed with strict UBSan before the correction and passes afterward; positive grammar extraction remains covered by the full suite.
- RST linking called `bsearch` on an empty null configuration table. The empty-table guard passed the full 45-test documentation/grammar suite and the complete strict run.
- The Docker entrypoint inherited a permissive umask and created a world-writable HOME, which private settings correctly rejected. It now creates private state. The script also assumed a default CLI format and selected the last textual `status`, mistaking Neo4j's nested `serving` status for the index result. It now requests raw JSON and selects `structuredContent.status` with the runtime's installed JSON parser; failed indexes retain their diagnostics. The entrypoint regression covers all three conditions and is part of the canonical gate.
- The previously intermittent daemon EOF timeout check passed ten isolated stress runs and the complete strict run. Failure diagnostics were added; its historical intermittent root cause is not claimed to be resolved.

The vendored scanner patches guard only zero-length deserialization. Their two hashes in `scripts/vendored-checksums.txt` record these intentional source changes; downloaded artifact verification was not bypassed.

## Reproduce the feature comparison

Build the fork with embedded UI using `make -j4 -f Makefile.cbm cbm-with-ui`, then run:

```sh
python3 tests/test_fork_feature_parity.py \
  --baseline /path/to/trusted/pre-rename/codebase-memory-mcp \
  --ui-baseline /path/to/verified/upstream-release \
  --fork build/c/codebase-memory-mcp-neo4j \
  --report parity.json
python3 tests/test_fork_coexistence.py --upstream /path/to/verified/upstream-release
```

The parity report includes executable hashes, catalogs, tool responses, profile coverage, and the completed checks. All accounts, repositories, and databases in these tests are disposable fixtures. For the full merge workflow use `scripts/test.sh`; for lint use `scripts/lint.sh --ci CLANG_FORMAT=clang-format-20`. Linux containers must reap child processes and permit Unix sockets. Detailed logs for this audit are retained under `/workspace/cbm-validation` in the development workspace.

## Remaining boundaries

- Native Windows/PowerShell and macOS execution remain untested here; Linux packaging contracts and path fixtures do not replace those platform runs.
- Model-backed UI evaluation and four retained frontend release-proof checks require fresh candidate-bound evidence and the absent local GGUF models/backend evaluation setup. UI unit/build/HTTP checks passed; live local-model generation is not claimed. Historical evidence timestamps and hashes were not edited to manufacture a passing release report.
- The allocator-budget linter still fails on original main as well as this branch; its recorded baseline is behind existing Cypher/MCP/Neo4j/store allocations. Existing cppcheck constant-condition findings also remain in production-disabled allocation-fault seams. These limits prevent claiming a wholly green repository merge gate despite the clean native test run. Allocation budgets were not raised and checks were not suppressed.
- Existing Docker volumes with an insecure `/data/home` must have that dedicated directory's ownership and permissions repaired before use. New entrypoints create private state; runtime security checks were preserved.
- A future release must produce renamed archives and refresh package-manager versions/checksums. No release was published. See [FORK_COEXISTENCE.md](FORK_COEXISTENCE.md) for migration and explicitly shared-cache/port/Neo4j graph risks.
