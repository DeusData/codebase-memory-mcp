---
phase: 04
slug: verdicts-and-acceptance-summaries
status: approved
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-13
---

# Phase 04 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Proof-harness summary regressions and operator-doc contract checks backed by committed verdict evidence |
| **Config file** | none |
| **Quick run command** | `python3 -B scripts/test_gdscript_proof_verdict_summaries.py` |
| **Full suite command** | `python3 -B scripts/test_gdscript_proof_verdict_summaries.py && python3 -B scripts/test_gdscript_proof_manifest_contract.py && python3 -B scripts/test_gdscript_proof_incomplete_artifacts.py && python3 -B scripts/test_gdscript_proof_same_script_calls.py` |
| **Estimated runtime** | ~30 seconds |

---

## Sampling Rate

- **After every task commit:** Run `python3 -B scripts/test_gdscript_proof_verdict_summaries.py`
- **After every plan wave:** Run `python3 -B scripts/test_gdscript_proof_verdict_summaries.py && python3 -B scripts/test_gdscript_proof_manifest_contract.py && python3 -B scripts/test_gdscript_proof_incomplete_artifacts.py && python3 -B scripts/test_gdscript_proof_same_script_calls.py`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 30 seconds

---

## Per-task Verification Map

| task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 04-01-01 | 01 | 1 | PROOF-03 | T-07-01 / T-07-02 | Each proof target emits an explicit repo verdict so approval contribution is reviewable instead of inferred from raw output. | regression | `python3 -B scripts/test_gdscript_proof_verdict_summaries.py` | ✅ | ✅ green |
| 04-01-02 | 01 | 1 | EVID-03 | T-07-01 / T-07-02 | Aggregate proof output answers promotion first, preserves incomplete versus fail, and bounds any approval claim to `qualified-support-only`. | regression | `python3 -B scripts/test_gdscript_proof_verdict_summaries.py && python3 -B scripts/test_gdscript_proof_manifest_contract.py` | ✅ | ✅ green |
| 04-02-01 | 02 | 1 | PROOF-03 / EVID-03 | T-07-01 / T-07-02 | Verdict summaries remain additive to raw wrapper evidence, and incomplete-path review still falls back to manifest-contract, incomplete-artifact, and same-script regressions. | regression | `python3 -B scripts/test_gdscript_proof_incomplete_artifacts.py && python3 -B scripts/test_gdscript_proof_same_script_calls.py` | ✅ | ✅ green |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Existing infrastructure covers all phase requirements.

---

## Manual-Only Verifications

This approved backfill is reconstructed from `04-VERIFICATION.md`, the committed Phase 04 summary artifacts, and the existing verdict-summary command surface. No new rerun is required for approval.

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 30s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved 2026-04-13
