---
phase: 01
slug: proof-contract-and-corpus
status: approved
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-13
---

# Phase 01 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Markdown/frontmatter contract verification rooted in committed proof-contract docs and Python regressions |
| **Config file** | none |
| **Quick run command** | `python3 -c "import json, pathlib; data=json.loads(pathlib.Path('docs/superpowers/proofs/gdscript-good-tier-manifest.json').read_text()); assert len(data['repos'])==4; assert data['approval_contract']['approval_mode']=='manifest-only'; assert all(repo['canonicality']=='canonical-approved-v1-target' for repo in data['repos']); assert all(repo['qualification']['qualifies_for_v1'] for repo in data['repos'])"` |
| **Full suite command** | `python3 -c "import json, pathlib; data=json.loads(pathlib.Path('docs/superpowers/proofs/gdscript-good-tier-manifest.json').read_text()); assert len(data['repos'])==4; assert data['approval_contract']['approval_mode']=='manifest-only'; assert all(repo['canonicality']=='canonical-approved-v1-target' for repo in data['repos']); assert all(repo['qualification']['qualifies_for_v1'] for repo in data['repos'])" && python3 scripts/test_gdscript_proof_same_script_calls.py && python3 scripts/test_gdscript_proof_manifest_contract.py` |
| **Estimated runtime** | ~10 seconds |

---

## Sampling Rate

- **After every task commit:** Run `python3 -c "import json, pathlib; data=json.loads(pathlib.Path('docs/superpowers/proofs/gdscript-good-tier-manifest.json').read_text()); assert len(data['repos'])==4; assert data['approval_contract']['approval_mode']=='manifest-only'; assert all(repo['canonicality']=='canonical-approved-v1-target' for repo in data['repos']); assert all(repo['qualification']['qualifies_for_v1'] for repo in data['repos'])"`
- **After every plan wave:** Run `python3 -c "import json, pathlib; data=json.loads(pathlib.Path('docs/superpowers/proofs/gdscript-good-tier-manifest.json').read_text()); assert len(data['repos'])==4; assert data['approval_contract']['approval_mode']=='manifest-only'; assert all(repo['canonicality']=='canonical-approved-v1-target' for repo in data['repos']); assert all(repo['qualification']['qualifies_for_v1'] for repo in data['repos'])" && python3 scripts/test_gdscript_proof_same_script_calls.py && python3 scripts/test_gdscript_proof_manifest_contract.py`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 10 seconds

---

## Per-task Verification Map

| task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 01-01-01 | 01 | 1 | PROOF-01 | T-07-01 / T-07-02 | Approved proof targets are manifest-defined, approval-bearing metadata is explicit, and Godot 4.x qualification stays reviewable from committed evidence. | contract | `python3 -c "import json, pathlib; data=json.loads(pathlib.Path('docs/superpowers/proofs/gdscript-good-tier-manifest.json').read_text()); assert len(data['repos'])==4; assert data['approval_contract']['approval_mode']=='manifest-only'; assert all(repo['canonicality']=='canonical-approved-v1-target' for repo in data['repos']); assert all(repo['qualification']['qualifies_for_v1'] for repo in data['repos'])"` | ✅ | ✅ green |
| 01-01-02 | 01 | 1 | PROOF-01 | T-07-01 / T-07-02 | Canonical versus ad hoc proof behavior remains executable and reviewable through the existing proof-harness regression surface described in the phase verification report. | regression | `python3 scripts/test_gdscript_proof_same_script_calls.py` | ✅ | ✅ green |
| 01-02-01 | 02 | 1 | PROOF-01 | T-07-01 / T-07-02 | Manifest-mode proof output preserves canonical identity fields, approval posture, and non-canonical handling using existing committed proof-harness assertions only. | regression | `python3 scripts/test_gdscript_proof_manifest_contract.py` | ✅ | ✅ green |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Existing infrastructure covers all phase requirements.

---

## Manual-Only Verifications

This approved backfill is reconstructed from existing committed evidence in `01-VERIFICATION.md` and the Phase 01 summaries. No additional manual rerun is required for approval.

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 10s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved 2026-04-13
