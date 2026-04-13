---
phase: 02
slug: isolated-proof-harness
status: approved
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-13
---

# Phase 02 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Proof-harness regression checks plus runbook-backed artifact-contract verification from committed evidence |
| **Config file** | none |
| **Quick run command** | `python3 scripts/test_gdscript_proof_manifest_contract.py` |
| **Full suite command** | `python3 scripts/test_gdscript_proof_manifest_contract.py && python3 scripts/test_gdscript_proof_incomplete_artifacts.py` |
| **Estimated runtime** | ~20 seconds |

---

## Sampling Rate

- **After every task commit:** Run `python3 scripts/test_gdscript_proof_manifest_contract.py`
- **After every plan wave:** Run `python3 scripts/test_gdscript_proof_manifest_contract.py && python3 scripts/test_gdscript_proof_incomplete_artifacts.py`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 20 seconds

---

## Per-task Verification Map

| task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 02-01-01 | 01 | 1 | PROOF-02 | T-07-01 / T-07-02 | The canonical proof entrypoint stays `scripts/gdscript-proof.sh`, manifest mode remains approval-bearing, and wrapper-first machine-readable evidence stays anchored to committed artifacts. | regression | `python3 scripts/test_gdscript_proof_manifest_contract.py` | ✅ | ✅ green |
| 02-01-02 | 01 | 1 | EVID-01 | T-07-01 / T-07-02 | Isolated runtime state remains under repo-owned artifacts and the run-level `run-index.json` keeps state and artifact paths inspectable. | regression | `python3 scripts/test_gdscript_proof_manifest_contract.py` | ✅ | ✅ green |
| 02-02-01 | 02 | 1 | EVID-02 | T-07-01 / T-07-02 | Raw wrapper artifacts remain canonical, `run-index.json` points to them, and incomplete-path evidence preserves missing-wrapper context instead of hiding partial results. | regression | `python3 scripts/test_gdscript_proof_incomplete_artifacts.py` | ✅ | ✅ green |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Existing infrastructure covers all phase requirements.

---

## Manual-Only Verifications

This approved backfill is reconstructed from the committed `02-VERIFICATION.md`, Phase 02 summaries, and the existing wrapper-first artifact contract. No new proof rerun is required for approval.

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 20s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved 2026-04-13
