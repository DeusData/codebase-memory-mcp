---
phase: 07
slug: nyquist-validation-backfill
status: approved
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-12
---

# Phase 07 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Python regression script for markdown/frontmatter contract verification |
| **Config file** | none |
| **Quick run command** | `python3 -B scripts/test_phase07_nyquist_validation.py --group all` |
| **Full suite command** | `python3 -B scripts/test_phase07_nyquist_validation.py --group all` |
| **Estimated runtime** | ~1 second |

---

## Sampling Rate

- **After every task commit:** Run `python3 -B scripts/test_phase07_nyquist_validation.py --group all`
- **After every plan wave:** Run `python3 -B scripts/test_phase07_nyquist_validation.py --group all`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 1 second

---

## Per-task Verification Map

| task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 07-01-01 | 01 | 1 | None | T-07-01 | Backfilled Phase 01-02 validation docs point only to existing approved evidence and keep approved Nyquist frontmatter. | integration | `python3 -B scripts/test_phase07_nyquist_validation.py --group phase-01-02` | ✅ | ✅ green |
| 07-01-02 | 01 | 1 | None | T-07-01 / T-07-02 | Backfilled Phase 03-04 validation docs preserve real command/task mappings and do not invent new approval criteria. | integration | `python3 -B scripts/test_phase07_nyquist_validation.py --group phase-03-04` | ✅ | ✅ green |
| 07-01-03 | 01 | 1 | None | T-07-03 | Milestone-audit evidence is refreshed so Nyquist discovery can find complete validation-doc coverage. | integration | `python3 -B scripts/test_phase07_nyquist_validation.py --group audit` | ✅ | ✅ green |

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
- [x] Feedback latency < 1s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved 2026-04-12

## Validation Audit 2026-04-13

| Metric | Count |
|--------|-------|
| Gaps found | 3 |
| Resolved | 3 |
| Escalated | 0 |
