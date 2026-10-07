# PR 2068: maintainer handoff from Bernhard

Prepared on 10 September 2026 (Europe/Berlin). All development stayed on `feat/codeatlas-web`, starting at `80f4f41f9bcb9daf60afc5f607bcbcba24a4b196`; the PR base remains `feat/atlas-r1`. No other branch was changed. This is a continuation of [PR #2068](https://github.com/DeusData/codebase-memory-mcp/pull/2068), not a replacement PR or a merge approval.

The first implementation pass did not meet the intended product flows. A critical [product audit](pr-2068-product-audit.md) reproduced broken search, source navigation and disconnected analysis entry points. The second pass fixed those failures and exercised the integrated application with Playwright against this repository's real daemon and index. Earlier failures remain separately recorded. The [full final report](pr-2068-final-delivery.md) distinguishes working software, executed checks and remaining limitations.

## Implemented and exercised

| Developer task | Delivered behavior | Main implementation |
| --- | --- | --- |
| Understand a repository and choose a starting point | README/header source quotes, source areas, entry candidates, bounded static call paths, progressive navigation and explicit evidence limits | `graph-ui/src/architecture/`, `src/ui/atlas_repository.c` |
| Inspect why a selection matters | Incoming/outgoing relationships, entry paths and separately observed agent activity, with direct source and analysis actions | `graph-ui/src/why/SelectionContext.tsx`, shared `SourceEvidenceDrawer` |
| Assess a file, symbol or change | One analysis workspace for selection, working tree and comparison ref; structural paths, test candidates and bounded Git co-change evidence; transparent uncertainty | `graph-ui/src/impact/`, `src/ui/atlas_impact.c` |
| Use agent activity and logs through one daemon | SQLite activity schema 4, WAL, retention, cursor ordering and persistent deduplication; same-port polling; explicit local hook setup | `src/ui/activity.c`, `src/ui/http_server.c`, `graph-ui/src/agents/` |
| Recognize failures and inspect incomplete indexing | Persisted project errors in System and Galaxy; filters before limits; existing coverage diagnostics and user-initiated local report review/download | `graph-ui/src/diagnostics/`, `src/daemon/application.c`, `src/pipeline/pipeline.c` |

No new npm dependency or rendering framework was added. Existing MCP transports remain. Agent history does not imply current activity or agent intent. Static reachability is not runtime execution or data flow; co-changes are not functional dependencies or calibrated defect probabilities. Diagnostics do not upload reports or create issues automatically.

The real-browser repair pass also corrected compact RPC reference decoding and pagination, complete-path search including immediate Enter and delayed responses, source lookup outside the loaded map, EOF pagination, unverified callsite coordinates, small-window analysis layout, and truncated large repository-map HTTP responses. The remaining C parser coordinate defect is disclosed below.

## Validation and evidence

- Repeated at handoff: **2473 frontend tests in 175 files**, **203 portable acceptance checks**, production build, style and promise gates, **5 operational-reference checks**, and **7 local hook tests**, all passing. The production asset remained `index-nfAv5dSK.js`. Logs are in [the archive manifest](../../graph-ui/verification/pr-2068/log-archives.json); `.log.gz` files decompress to the original bytes.
- A final style failure came from the hook evidence being renamed while the scanner still expected its previous name. The scanner now names the actual download and a regression check compares its bytes with the canonical hook. The original local diagnostic download is preserved verbatim in a JSON evidence wrapper; it was not rewritten to satisfy the style rule.
- Recorded final native repair suites: **83 HTTP/journal/transport tests passed, one existing Windows-only skip**, and **40 UI/map/impact tests passed**, under ASan/UBSan. These are the prior repair-round results; the handoff does not claim a new full native or cross-platform run.
- [Repository map flow](../../graph-ui/verification/pr-2068/repair-map-e2e.json): real entry, path, edge evidence, correctly highlighted source line, return to the retained map and shared change analysis.
- [Handoff browser recheck](../../graph-ui/verification/pr-2068/handoff-recheck/repair-map-e2e.json): after restarting the local verification daemon, the same complete map flow passed again with six fresh screenshots, no page exceptions and no external requests. The earlier browser records were preserved.
- [Explore and search](../../graph-ui/verification/pr-2068/explore-context-e2e.json): 144 functions and eight classes in `application.c`, source/context/impact navigation and a delayed genuine search-response race.
- [Change analysis](../../graph-ui/verification/pr-2068/change-analysis-e2e.json): real working-tree and ref changes, file/symbol scope, dependency paths, test source, Git evidence, keyboard focus and 1024x800 layout.
- [Live errors](../../graph-ui/verification/pr-2068/repair-errors-e2e.json): clearly labelled local indexing failures reached System and Galaxy without reload; displayed filters matched SQLite.
- [Hook setup](../../graph-ui/verification/pr-2068/agent-setup-integration.json) and [restart/reconnect](../../graph-ui/verification/pr-2068/final-agent-diagnosis-restart.json): real local TEST-labelled producer, retained event after an actual daemon restart, and browser offline/online recovery without duplication. No production agent or LLM session was launched.
- [Before](../../graph-ui/verification/pr-2068/final-before-architecture.png), [after](../../graph-ui/verification/pr-2068/final-after-architecture.png), and [comparison protocol](../../graph-ui/verification/pr-2068/final-comparison.json). Both frontends used the same current backend/index, so this is a frontend comparison. Port 9751 was only the temporary comparison server.

## Continue locally

Use the PR discussion for the published delivery reference. The handoff is based on the exact PR head above and preserves the existing implementation; do not reset a newer maintainer checkout to this base. Review and apply the commits on `feat/codeatlas-web` using an ordinary fast-forward or an explicitly reviewed integration if that branch has advanced.

From the repository root:

```sh
scripts/ci/test-ui.sh
node --test graph-ui/tests/scaffold/operational-client-reference.test.mjs
python3 graph-ui/agents/hooks/test_atlas_trace.py
make -f Makefile.cbm cbm-with-ui
```

Start the resulting binary with `--ui=true --port=9749` through the usual MCP/daemon setup, then index this checkout. The recorded product runners under `graph-ui/tools/pr-2068-*.mjs` use project name `cbm-pr2068`, port 9749 and this repository's actual symbols; update those inputs for another local project. The comparison runner additionally needs the original frontend on 9751. Recorded worktree counts describe their test moment and will change after commits are applied. No local cache, database, personal client settings or temporary session wrapper is required for the code handoff.

Review source commit `4372d9fada2b79e668da32193fb1c398436576dd` first. The subsequent report/evidence commit contains the larger screenshot and archived-log collection. One small hook-download fixture stays with the source because its byte-identity regression check uses it.

## Open work and acceptance limits

1. **Global C lint is not green.** The recorded full lint run lacks local `cppcheck` and contains extensive clang-tidy findings, including complexity/style findings in the new C files. Resolve and rerun the required native lint gates before treating this as merge-ready. Passing compilation and targeted sanitizer suites do not replace that gate.
2. **The C preprocessor/index coordinate cause remains unfixed.** The UI suppresses demonstrably impossible callsite links and preserves the reported number as unverified evidence. Repair the parser mapping with a reproducer before claiming accurate callsites generally.
3. **Architecture boundaries and entry roles remain heuristics.** Source quotes are author descriptions, not independently verified subsystem responsibilities. No first-time-developer usability study established the qualitative onboarding criterion.
4. **Coverage and history remain bounded.** The recorded index had 85 partial parse paths and 300 intentionally excluded path records, including directories. Those are not coverage percentages. Diff hunks are narrowed to symbols manually; missing history or index freshness is shown as uncertainty.
5. **Platform/release validation remains maintainer work.** No new Linux/Windows release ladder, production LLM session or automatic semantic diff mapping is claimed. The PR must stay stacked on its intended base until the maintainer integrates it.

The [research review](pr-2068-final-review.md) records verified project identities, actual browser interactions versus source inspection, licenses and decisions. Navigation hierarchy, selected paths, source reading without losing context and separate historical evidence influenced the implementation. No competitor frontend, additional database stack or noncommercial/GPL source was incorporated.
