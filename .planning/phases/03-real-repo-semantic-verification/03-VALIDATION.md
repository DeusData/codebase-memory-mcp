---
phase: 03
slug: real-repo-semantic-verification
status: approved
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-13
---

# Phase 03 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Native-suite and proof-harness parity regressions grounded in committed sequential and parallel evidence |
| **Config file** | none |
| **Quick run command** | `python3 -B scripts/test_gdscript_proof_semantic_parity.py` |
| **Full suite command** | `python3 -B scripts/test_gdscript_proof_manifest_contract.py && python3 -B scripts/test_gdscript_proof_semantic_parity.py && rtk make -f Makefile.cbm test && CBM_FORCE_PIPELINE_MODE=parallel rtk make -f Makefile.cbm test` |
| **Estimated runtime** | ~180 seconds |

---

## Sampling Rate

- **After every task commit:** Run `python3 -B scripts/test_gdscript_proof_semantic_parity.py`
- **After every plan wave:** Run `python3 -B scripts/test_gdscript_proof_manifest_contract.py && python3 -B scripts/test_gdscript_proof_semantic_parity.py && rtk make -f Makefile.cbm test && CBM_FORCE_PIPELINE_MODE=parallel rtk make -f Makefile.cbm test`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 180 seconds

---

## Per-task Verification Map

| task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 03-01-01 | 01 | 1 | SEM-06 | T-07-01 / T-07-02 | Deterministic sequential versus parallel mode selection stays explicit and approval evidence can force each path without silent fallback. | native regression | `rtk make -f Makefile.cbm test && CBM_FORCE_PIPELINE_MODE=parallel rtk make -f Makefile.cbm test` | ✅ | ✅ green |
| 03-02-01 | 02 | 1 | SEM-01 | T-07-01 / T-07-02 | Non-zero class and method extraction remains reviewable through counts plus representative samples sourced from wrapper-backed parity artifacts. | proof regression | `python3 -B scripts/test_gdscript_proof_semantic_parity.py` | ✅ | ✅ green |
| 03-02-02 | 02 | 1 | SEM-02 / SEM-03 / SEM-04 / SEM-05 / SEM-06 | T-07-01 / T-07-02 | Same-script calls, inherits, imports, signal behavior, and sequential-versus-parallel parity stay reviewable from committed `semantic-parity.json` and wrapper evidence only. | proof regression | `python3 -B scripts/test_gdscript_proof_manifest_contract.py && python3 -B scripts/test_gdscript_proof_semantic_parity.py` | ✅ | ✅ green |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Existing infrastructure covers all phase requirements.

---

## Manual-Only Verifications

This approved backfill reconstructs the Phase 03 Nyquist contract from `03-VERIFICATION.md` and the committed summaries for SEM-01 through SEM-06. No fresh proof campaign is required for approval.

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 180s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved 2026-04-13
