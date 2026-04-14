# Phase 5: Web UI Launch Repair - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-12
**Phase:** 05-web-ui-launch-repair
**Areas discussed:** Flag contract

---

## Flag contract

| Option | Description | Selected |
|--------|-------------|----------|
| Explicit English mode | `en` is a first-class accepted value that launches the current built-in UI path; no broader localization promise yet. | |
| Legacy alias only | Treat `en` as a compatibility alias for UI enabled, but keep `true` as the real contract. | |
| Start locale contract | Use `en` as the first member of a broader language-selection interface that future values could extend. | |

**User's choice:** Correction: the supported contract is `--ui=true` / `--ui=false`, not `--ui=en`.
**Notes:** The discussion corrected the original phase wording. The real regression is that `--ui=true` no longer starts the UI.

---

## UI failure

| Option | Description | Selected |
|--------|-------------|----------|
| UI never starts | The server or browser path does not come up at all when UI is enabled. | ✓ |
| Wrong mode starts | The process runs, but it ignores the UI flag or behaves like standard mode. | |
| Starts then errors | The UI path begins, then fails with a runtime/server/assets problem. | |

**User's choice:** UI never starts.
**Notes:** This is the failure target to repair in Phase 5.

---

## Persistence

| Option | Description | Selected |
|--------|-------------|----------|
| Keep persisted behavior | `--ui=true` should still save UI enabled state and affect later launches, just like the current help text promises. | ✓ |
| Current launch only | Make `--ui=true` affect only the current run and stop persisting it. | |
| You decide | Keep the phase focused on restoring the launch path and let implementation choose the smallest safe config behavior. | |

**User's choice:** Keep persisted behavior.
**Notes:** The fix should preserve the current config contract rather than re-scope the flag.

---

## OpenCode's Discretion

- Exact implementation location for the launch repair.
- Exact regression-test mechanism, provided it covers the broken `--ui=true` startup path.

## Deferred Ideas

- Locale or language-specific UI selection remains out of scope for this phase.
