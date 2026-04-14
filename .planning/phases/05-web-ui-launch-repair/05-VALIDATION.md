---
phase: 05
slug: web-ui-launch-repair
status: approved
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-12
---

# Phase 05 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | C test runner + Python subprocess regressions via `Makefile.cbm` |
| **Config file** | none |
| **Quick run command** | `python3 -B scripts/test_ui_launch_persisted_flag.py` |
| **Full suite command** | `make -f Makefile.cbm test` |
| **Estimated runtime** | ~90 seconds |

---

## Sampling Rate

- **After every task commit:** Run `python3 -B scripts/test_ui_launch_persisted_flag.py`
- **After every plan wave:** Run `make -f Makefile.cbm test`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 90 seconds

---

## Per-task Verification Map

| task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 05-01-01 | 01 | 1 | UI-01 | T-05-01 / T-05-02 | Persisted UI enable, relaunch, and disable behavior stays locked to the documented `--ui=true` / `--ui=false` contract and saved `ui_enabled` / `ui_port` keys only. | regression / e2e | `python3 -B scripts/test_ui_launch_persisted_flag.py` | ✅ | ✅ green |
| 05-01-02 | 01 | 1 | UI-01 | T-05-01 / T-05-03 | Runtime load, flag parse, optional save, and startup gating share one `ui_cfg` flow so the localhost UI only starts when enabled and assets exist. | integration | `python3 -B scripts/test_ui_launch_persisted_flag.py && make -f Makefile.cbm test` | ✅ | ✅ green |

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
- [x] Feedback latency < 90s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved 2026-04-12
