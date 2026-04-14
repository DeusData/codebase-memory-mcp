---
phase: 06
slug: parallel-native-suite-repair
status: approved
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-12
---

# Phase 06 - Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | C test runner via `Makefile.cbm` |
| **Config file** | none |
| **Quick run command** | `CBM_FORCE_PIPELINE_MODE=parallel rtk make -f Makefile.cbm test` |
| **Full suite command** | `rtk make -f Makefile.cbm test && CBM_FORCE_PIPELINE_MODE=parallel rtk make -f Makefile.cbm test` |
| **Estimated runtime** | ~180 seconds |

---

## Sampling Rate

- **After every task commit:** Run `CBM_FORCE_PIPELINE_MODE=parallel rtk make -f Makefile.cbm test`
- **After every plan wave:** Run `rtk make -f Makefile.cbm test && CBM_FORCE_PIPELINE_MODE=parallel rtk make -f Makefile.cbm test`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 180 seconds

---

## Per-task Verification Map

| task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 06-01-01 | 01 | 1 | None | T-06-01 | Forced-parallel native tests preserve the FastAPI `Depends()` CALLS edge instead of silently dropping it on the parallel path. | regression | `CBM_FORCE_PIPELINE_MODE=parallel rtk make -f Makefile.cbm test` | ✅ | ⬜ pending |
| 06-01-02 | 01 | 1 | None | T-06-01 / T-06-02 | Full and incremental parallel paths run the shared FastAPI Depends pass before cache teardown and keep normal suite behavior intact. | integration | `rtk make -f Makefile.cbm test && CBM_FORCE_PIPELINE_MODE=parallel rtk make -f Makefile.cbm test` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Existing infrastructure covers all phase requirements.

---

## Manual-Only Verifications

All phase behaviors have automated verification.

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 180s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved 2026-04-12
