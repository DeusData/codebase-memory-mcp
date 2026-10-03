# Swift identity validation

This records local evidence for issue #2061 and PR #2436: Swift
callable identity and conservative overload candidates, including candidate
counts and multiple trailing closure labels. The scope remains one language,
one claim.

## Tested source

The A-only checkpoint contains the seven Swift implementation and regression
test paths committed in `fix(swift): match multiple trailing closure labels`.
It was tested from a fresh `git archive` snapshot, without uncommitted
infrastructure changes or Git metadata.

Historical publication snapshot, recorded before the checkpoint was pushed:
public PR head S was `b0958c6cd36c68825fac55cc58934181a2251192` and did
not contain the local A checkpoint. A's parent was local merge
`b7c86690618ed0accf137a42a987887872475fc3`; the checkpoint and this document
were then unpublished. That publication status is superseded: public harness
checkpoint `69e58c0df998e350d14fd9db8e8d205f7bf6036a` includes both.
PR readiness remains unestablished.

| Identity | Value |
| --- | --- |
| Commit | `761ea8ca432ae0f58a6bb77c438ccedcb1ade3a1` |
| Tree | `b1718c8b0e14eb1bd7d2a2f6da1bda024e4a1912` |
| Archive SHA-256 | `d934685cc5e521060a8ddb29dcb65e5961c10d103763375e647a9e9da6a1617e` |
| Content manifest SHA-256 | `10011442026e4c5a4900c6b24ee19dda1ea3c1eda15e8bf88b0eec35b4ef77de` |

The archive path set matched all 2,254 tracked paths, including one symlink.
The content manifest contains sorted file SHA-256 records and symlink targets.
Its before/after comparison returned **0**, with identical hashes.

## Focused result

From the archived source root, with `VALIDATION_BUILD_DIR` set to a separate
temporary build directory:

```bash
scripts/test.sh --suites extraction,callable_sig,registry,pipeline \
  BUILD_DIR="$VALIDATION_BUILD_DIR"
```

The canonical iteration entry compiled the standard test runner and executed
exactly these four suites:

| Suite | Relevant coverage |
| --- | --- |
| `extraction` | Multiple trailing closures, bounded labels, compact/spill round trips |
| `callable_sig` | Swift signature identity, defaults, identity length bounds |
| `registry` | Conservative overload candidates, closure labels, defaults and registration order |
| `pipeline` | Serial/parallel candidates and incremental signature restoration |

Environment: Linux x86_64, GCC/G++ **16.2.1 20260810**, eight build cores.
The repository's default ASan+UBSan settings and the
`sanitized=1 test_seams=1` build-config assertion remained enabled.

- Started: **2026-10-03T01:45:48Z**.
- Finished: **2026-10-03T02:12:28Z**.
- Test command exit: **0**.
- Complete runner summary: **787 passed**.
- Archived source content remained identical after execution.

## Evidence limits and earlier failures

At the time of the 787-pass focused run, no attributable raw Swift negative
reproduction log had been found for parent baseline
`5538355530bb126c3041f7fcf8ef7a82b4bb3fec`. That focused run alone did not
establish RED/GREEN. The subsequent original-fixture comparison below fills
the graph-layer reproduction gap without changing the focused run's scope.

Earlier checks used combined working trees containing Swift and independent
infrastructure changes, based on
`b7c86690618ed0accf137a42a987887872475fc3`. Their recorded
`git diff --binary` SHA-256 scopes differ:

- GCC TSan and daemon smoke:
  `b1c2b8cb54ab868ab7f053f1936ecc48370f7cd3123cb5f5efd23858a114982c`.
- The 131-source analyzer run, before the parser patch:
  `c0ac0734e190a79031c5a430e4cd71f52e6280bdfbd47bf9631cfcb8d397c1be`.

These results apply to their respective snapshots and were not rerun on the
A-only archive:

| Earlier check | Recorded result and scope |
| --- | --- |
| GCC TSan | Canonical full TSan leg failed: exit 2; 1,220 passed, 2 failed, 8 skipped. Daemon frontend EOF fixtures hit the existing 90-second child alarm. |
| Daemon smoke | Production daemon/standalone CLI smoke failed: exit 2; standalone CLI created a daemon socket, violating the smoke test's one-shot expectation. |
| Memory analysis | Canonical 131-source analysis remained FAIL. Verified execution reported findings; earlier provisional green results with missing-tool/parser failure risks were invalid. Bounded LLVM 21/22 comparisons over seven translation units found the same 16 diagnostic lines across parent/local/combined snapshots; that diagnostic comparison does not replace the 131-source gate. |

The focused A-only result does not establish full acceptance or PR readiness.
The earlier failures remain open in their respective scopes. No broader CLI,
daemon, Cypher, MCP or Store repair is included in this Swift checkpoint.

## Original issue #2061 graph-layer RED/GREEN (2026-10-03)

The coordinator accepted the original three-file fixture comparison as
graph-layer P RED / A GREEN. This tests the production pipeline and persisted
CALLS graph, not the `trace_path` presentation layer or all PR gates. It is
evidence for the overall issue #2061 repair, not isolated proof of the multiple
trailing-closure patch.

| Role | Exact commit |
| --- | --- |
| P: parent baseline | `5538355530bb126c3041f7fcf8ef7a82b4bb3fec` |
| A: Swift candidate | `cf67f49dc2d718709846adefff5bab6cf9b671d4` |
| Shared external harness | `69e58c0df998e350d14fd9db8e8d205f7bf6036a` |

Both sides used fresh full Git archives, separate source/build/repository/DB
directories, the same external `tests/repro/issue2061_swift_identity/main.c`,
and the existing production pipeline/store APIs in FAST mode. No resolver,
P/A source or harness was modified. Neither side was rebuilt or rerun.

Linux x86_64; GCC/G++ **16.2.1 20260810**. The standard archived Makefile's
production/extraction/grammar dependencies and `CFLAGS_TEST` were retained:
C11, `-g -O1`, `-fsanitize=address,undefined`,
`-fno-omit-frame-pointer`, standard warning/feature/API defines, and
`TEST_SEAMS=1`. Path-normalized dry-run plans were identical; the only test
source was the external harness, with no full test suite compiled or run.
The build command, with evidence locations represented by variables, was:

```bash
make -j8 -f Makefile.cbm "$BUILD/test-runner" \
  CC=gcc CXX=g++ TEST_SEAMS=1 BUILD_DIR="$BUILD" \
  ALL_TEST_SRCS="$HARNESS/main.c"
```

P/A inherited the same execution environment and shared fresh `TMPDIR`.
The evidence directory and compilation temporaries used disk-backed storage
after a capacity-only preflight blocked the initial tmpfs location; that
initial attempt started no archive/build/run. Environment evidence records
only the reviewed non-secret allowlist, with other CBM variables redacted.
Each harness invocation used `timeout 180s`,
`ASAN_OPTIONS=detect_leaks=1:halt_on_error=1` and
`UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1`.

All timestamps below are UTC on **2026-10-03**:

| Side | Build start / end | Build exit | Run start / end | Run exit |
| --- | --- | --- | --- | --- |
| P | 05:03:18.197110 / 05:19:16.170967 | 0 | 05:27:49.563068 / 05:27:49.764641 | 1 |
| A | 05:19:16.745556 / 05:27:49.016321 | 0 | 05:46:05.496977 / 05:46:05.680162 | 0 |

P reported **4 passed, 6 failed, errors 0, exit 1**. The source's two `work`
overloads collapsed into one node at lines 10–12. The surviving name overload
had `name -> target`, the caller had `caller -> name`, and the depth-three
inbound traversal observed the false caller (`actual=1`, expected 0).
Those positive wrong-path observations establish the original defect;
the missing flag node and unavailable (`-1`) edge checks alone do not.

A reported **10 passed, 0 failed, errors 0, exit 0**: distinct work nodes at
lines 5–7 and 10–12; flag -> target; name has no target edge; caller -> name
and no flag edge; no false caller within target's three inbound CALLS levels.
Both complete stderr files contain normal `level=info` pipeline logs, with
no sanitizer/runtime diagnostics.

The external scheduler accepted P's expected exit 1, then incorrectly treated
any nonempty stderr as fatal, stopping on normal info logs before A ran.
An external continuation corrected only that control flow. It first checked
that no target process, A result files or A-run directory existed, verified
the already-built binaries and recorded environment, then ran A exactly once
using the original binary/options/TMPDIR. P was not rerun. The old stopping
record and original seal remain intact.

SHA-256 fingerprints (full values):

| Artifact | P | A |
| --- | --- | --- |
| Full Git archive | `cc83e94190a4abeff58d1add0f9e575ebc4fa7d639bb62873800e6177ba2acc6` | `c46f2286fb49e056c24cbf907ee098b798c6c5b9099343d57d80df1588e03df2` |
| Harness binary | `99d812bec8cb5ac2e7b642ff40566a363a3d9d183cce63b05c9c26af02bd0e34` | `f625323a623bc181fcaddedbe7a196d876984ec559985496ff043e37f4a16181` |
| Source manifest before/final | `ced6a6ee75e083666b3cacd1d3829ff9d2cc09225c952c826f941f33fa319fe3` | `c388f26045627f29bc7e4707331832d14c64d40d04edf15b61d06c17c9780b0a` |

| Shared artifact | SHA-256 |
| --- | --- |
| Harness `main.c` | `d2cdee7ba8dca1965a653803c5f0d4362aa5f9f5b65c321c9aa3268b23a100c0` |
| `Caller.swift` (both sides) | `325eb2e1034f00f9c2bc02d88123e3eb06089f39f38e8e1b0ded21a6d8314aa7` |
| `Service.swift` (both sides) | `5e18290f7bbbc29c341d5ddffb51d2c19ed00b97726e2fad0d50ebcfbcd1a529` |
| `Sink.swift` (both sides) | `a847552c25bf3a5114a565c64a9f80d8fa659f45c0dff9d1a208c700724af094` |
| Main checkout dirty/untracked content manifest before/final | `6f9882e6b094e355cf8cfdaa56748fa4826e8adafd36c9eb08ca5b8a652c1ab0` |
| Final evidence SHA-256 manifest file | `ef33eceb9ed9d3cdbb0f8910159d37e4b2c5fcf87b015fabb66ec65f9011b2de` |

All 24 continuation checks passed. Source manifests cover archived tracked
files, modes and symlink targets and match archive contents before/final.
Archive/binary hashes stayed unchanged. The fixtures match one another and
the original three embedded issue files; all harness file hashes remained
identical. Main checkout content, status, diff, index and HEAD were unchanged.
Every previously recorded evidence file was preserved byte-for-byte.
The final continuation seal is **2026-10-03T05:46:17.826586Z**.

Complete commands, raw stdout/stderr, actual exits, UTC records, environment
allowlist and manifests remain in the retained external evidence directory;
private absolute locations and secrets are intentionally omitted here.
This comparison does not turn the historical mixed failures into passes,
establish full/TSan/lint/memory gates on the current PR candidate, or establish
PR readiness. Remaining Swift acceptance work requires a separately approved
plan and evidence tied to one exact final candidate.

## MCP presentation attempt — FAIL, 2026-10-03

Tested candidate: `0007c1d22858e1548ca392f382275595a0a2c691`; one exact-archive build/run, no reindexing.
Build: **06:26:37.439977–06:35:25.810701 UTC**, exit **0**; run: **06:35:26.198188–06:35:26.380432 UTC**, exit **1**.
Standard GCC/G++ 16.2.1 ASan+UBSan/test-seams flags and leak detection remained enabled.

Requests 1–4 had OBS PASS: two work identities; target inbound depth3 JSON and default tree (only flag overload, hop1, total1/eq); caller outbound (only name overload, hop1, total1/eq).
All five MCP status logs were `ok`, but stdout ends at `RAW 5 {`; request 5's complete response, OBS and final SUMMARY were not flushed and its semantics remain unverified.
LeakSanitizer reported a **1024-byte direct leak**; the entire run failed, not five observations passed or a semantic RED.
Key stack: [find_nodes_generic / Store](https://github.com/DavidHLP/codebase-memory-mcp/blob/0007c1d22858e1548ca392f382275595a0a2c691/src/store/store.c#L2779) → `cbm_store_find_nodes_by_name` → [handle_trace_call_path / MCP](https://github.com/DavidHLP/codebase-memory-mcp/blob/0007c1d22858e1548ca392f382275595a0a2c691/src/mcp/mcp.c#L9291).
The complete MCP and Store source files are byte-identical to parent P `5538355530bb126c3041f7fcf8ef7a82b4bb3fec`, as confirmed by source comparison; this does not claim a new P runtime result.
This inherited generic leak is **OUT OF SCOPE**: it is not included in the Swift fix or an additional Swift acceptance gate. No repair, suppression or rerun was performed.
Original DB, backup copy, fixtures, archived source, binary and main checkout were **unchanged**; independent cache and DB copy provided isolation, not a query-layer stored-root check.
Original graph-layer P RED/A GREEN remains unchanged; the **787 focused passes belong only to core `761ea8ca432ae0f58a6bb77c438ccedcb1ade3a1`**.

Original seal: **2026-10-03T06:35:33.549096Z**; evidence-manifest SHA-256: `0ea8f6f2de4cc10cc9b93234f557ed2aca774abcdf859982aa972691cc4bfadb`.
Raw evidence and the verbatim 69-line record are retained externally; that record's SHA-256 is `cb6278aa60003baeb4e0ccbf47ca39f72af546d389d29fa708e428b2eb7402de`. Existing evidence and seal were not overwritten.

The [owner's issue decision](https://github.com/DeusData/codebase-memory-mcp/issues/2061#issuecomment-5836287644) requests Swift on parent PR2342, centered on helper/regression coverage: stable labels+types QNs with bare names, compatible label/default/trailing-closure selection, all compatible candidate edges with counts, and an index-format rebuild.
The [owner's PR reply](https://github.com/DeusData/codebase-memory-mcp/pull/2436#issuecomment-5936875903) queues deeper review; it is neither approval nor a routine-rebase request.
The [automated PR acknowledgement](https://github.com/DeusData/codebase-memory-mcp/pull/2436#issuecomment-5902448775) asks for CI green **or an explanation of believed pre-existing failures**; full/native macOS/strict-P matrices were contributor validation designs or commitments, not owner-specified Swift requirements.

## Existing index_format suite — 2026-10-03

One authorized invocation of the standard test entry point passed **3/3**,
exit **0**, **08:48:58.454697–08:48:59.120654 UTC**. `make -q` returned **0**
and the entry-point build reported the existing runner up to date: no rebuild.
The archived source/core was `761ea8ca432ae0f58a6bb77c438ccedcb1ade3a1`;
the then-public candidate was `34a370ce2debd9ce5721ad99d8c7be67ef3ffa2b`.
All core-to-candidate changes were validation docs or standalone repro files,
not runner inputs. Makefile, test entry point and test source matched candidate
HEAD. Working-tree WIP was excluded. GCC/G++ **16.2.1 20260810**, Make **4.4.1**,
default ASan+UBSan, and `sanitized=1 test_seams=1` build metadata were retained.

Exact invocation, with private path bindings recorded in external evidence:

```bash
cd "$EXACT_SOURCE"
TMPDIR="$EVIDENCE/tmp" scripts/test.sh --suites index_format \
  BUILD_DIR="$EXISTING_BUILD" CC=gcc CXX=g++ TEST_SEAMS=1
```

`index_format_siblings_distinct_and_searchable`,
`index_format_legacy_index_rebuilds_and_repairs`, and
`index_format_version_one_rebuilds` passed. The last test writes format **1**,
asserts full-rebuild routing and `format_migration:true`, verifies current
format **2** and preserved file count, then checks no second rebuild on an
unchanged run. This is narrow rebuild-boundary evidence, not a full-suite or
native-macOS acceptance result; the earlier 787 result keeps its original SHA.

Before/after archive, source manifest, runner, test source, entire old build
and main checkout were unchanged. Complete stdout/stderr and real exit/UTC
records are preserved in a new independent `/home` evidence directory; hardcoded
test fixtures still used `/tmp`, whose available space was checked separately.
No sanitizer diagnostic occurred and no other suite or lint was run.

| SHA-256 artifact | Value |
| --- | --- |
| Archive, before/after | `d934685cc5e521060a8ddb29dcb65e5961c10d103763375e647a9e9da6a1617e` |
| Source content manifest, before/after | `10011442026e4c5a4900c6b24ee19dda1ea3c1eda15e8bf88b0eec35b4ef77de` |
| Existing runner, before/after | `ad35773a1bc097cf980e67a4215baaee1b26337560696db85a908ce3ec2df11c` |
| tests/test_index_format.c, archive and checkout | `8599852fefeee42d879b38f22305cb4e3e309ead1b16bd7e6d8954a8358f43f4` |
| New suite evidence manifest | `2ebd57e301f1e5ea3049fc5e9ed5014c0991222d57662642d6f9d778c93135a7` |
| Later build-config metadata supplement manifest | `3ab556d8a8cbf66cf45c733624a6a460ed6b7de55c3ced539bcf4e6eebdf172c` |

Allocator scope decision: retain the existing paired
`cbm_alloc(CBM_MEM_CLASS_OTHER, ...)` / `cbm_free(CBM_MEM_CLASS_OTHER, ...)`
suffix-scan scratch hunk in `cbm_registry_find_ending_with`, introduced by
`bbd0957937ee01c483c04fe5d20f23f72bcd1fc5`. It is not Swift semantics, but
reverting those two calls increases `src/pipeline/registry.c` raw sites from
the checked-in ratchet **17** to **19**, failing lint-memory-core/lint-ci.
The allocator pair and baseline counts were not changed; this scope rationale
does not claim a fresh lint run or authorize other allocator work.
