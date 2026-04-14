# Phase 5: Web UI Launch Repair - Context

**Gathered:** 2026-04-12
**Status:** Ready for planning

<domain>
## Phase Boundary

Restore the supported `--ui=true` launch path so the embedded HTTP UI starts again when maintainers enable it. This phase repairs the existing persisted UI-enable contract and guards it against regression; it does not add localization, new UI variants, or a broader frontend redesign.

</domain>

<decisions>
## Implementation Decisions

### Launch contract
- **D-01:** The broken path to repair is `--ui=true`, not `--ui=en`.
- **D-02:** `--ui=true` and `--ui=false` remain the supported first-class CLI contract for enabling and disabling the embedded UI.
- **D-03:** Phase 5 should restore the existing embedded UI startup behavior instead of introducing a new language or variant-selection interface.

### Persistence behavior
- **D-04:** The current persisted-config behavior stays in place: `--ui=true` should continue to save UI enabled state for later launches.
- **D-05:** The fix should preserve the current help-text promise that UI enablement is persisted, not reinterpret the flag as a one-shot runtime override.

### Failure target
- **D-06:** The user-visible regression to eliminate is: with `--ui=true`, the UI never starts.

### OpenCode's Discretion
- Exact root-cause fix location across argument parsing, UI config loading, or HTTP server startup, as long as the supported `--ui=true` persisted launch path works again.
- Exact regression-test shape, as long as it protects the `--ui=true` startup path instead of only checking unrelated UI helpers.
- Any minimal documentation/help-text touch-ups needed to keep the repaired contract explicit and aligned with implementation.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase contract
- `.planning/ROADMAP.md` — Phase 5 goal, success criteria, and dependency chain for restoring the supported UI launch path.
- `.planning/REQUIREMENTS.md` — `UI-01` requirement definition and traceability for the repaired `--ui=true` contract.
- `.planning/STATE.md` — Current phase focus and carried-forward project decisions.

### Existing UI launch path
- `src/main.c` — CLI help text, `parse_ui_flags()`, config persistence, and conditional HTTP UI startup from `main()`.
- `src/ui/config.h` — Persisted UI config shape, which currently includes only `ui_enabled` and `ui_port`.
- `src/ui/config.c` — Load/save behavior for persisted UI config, including default enablement when embedded assets exist.
- `src/ui/http_server.h` — HTTP UI server lifecycle surface used by `src/main.c`.

### Existing verification surface
- `tests/test_ui.c` — Existing UI config persistence tests that establish current config expectations and are the natural regression home for launch-contract coverage.
- `.planning/codebase/ARCHITECTURE.md` — Architecture notes that define the HTTP UI server as the `--ui=true`-triggered embedded UI path.

### Prior phase context
- `.planning/phases/04-verdicts-and-acceptance-summaries/04-CONTEXT.md` — Most recent locked context and documentation/contract preservation expectations.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/main.c`: Already owns the `--ui=` CLI parsing, persisted config save, and conditional `cbm_http_server_new()` / background-thread startup path.
- `src/ui/config.c`: Already persists `ui_enabled` and `ui_port`; there is no existing stored language or UI variant field.
- `tests/test_ui.c`: Already verifies load/save/overwrite/default behavior for UI config and provides a lightweight regression style for this phase.

### Established Patterns
- The UI contract is currently boolean and persisted, not locale-based.
- Embedded UI startup is gated by both `ui_enabled` and `CBM_EMBEDDED_FILE_COUNT > 0`.
- The repo already treats the embedded UI as an optional localhost service started from the native entrypoint rather than a separate frontend product mode.

### Integration Points
- Phase 5 will likely touch `src/main.c` first, then either `src/ui/config.*` or `src/ui/http_server.*` depending on root cause.
- Regression coverage should land in `tests/test_ui.c` and possibly a launch-path test near the entrypoint if config-only tests are not enough.
- Any documentation update should stay aligned with `print_help()` in `src/main.c` and the architecture note in `.planning/codebase/ARCHITECTURE.md`.

</code_context>

<specifics>
## Specific Ideas

- The supported flag is `--ui=true`; `--ui=en` was a mistaken assumption during discussion and should not drive planning.
- This is a launch-repair phase, not a localization or multi-variant UI phase.

</specifics>

<deferred>
## Deferred Ideas

- Locale selection or translated UI variants would be a separate capability if needed later.
- Any broader UI redesign remains out of scope for this repair phase.

</deferred>

---

*Phase: 05-web-ui-launch-repair*
*Context gathered: 2026-04-12*
