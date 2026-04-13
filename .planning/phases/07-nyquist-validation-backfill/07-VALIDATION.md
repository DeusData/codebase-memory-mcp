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
| **Framework** | Markdown/frontmatter contract verification via repo file reads and CLI checks |
| **Config file** | none |
| **Quick run command** | `python3 - <<'PY'\nfrom pathlib import Path\nfor phase in ['01','02','03','04']:\n    p = Path(f'.planning/phases/{phase}-' + {'01':'proof-contract-and-corpus','02':'isolated-proof-harness','03':'real-repo-semantic-verification','04':'verdicts-and-acceptance-summaries'}[phase] + f'/{phase}-VALIDATION.md')\n    text = p.read_text()\n    assert 'status: approved' in text\n    assert 'nyquist_compliant: true' in text\n    assert 'wave_0_complete: true' in text\nprint('PASS')\nPY` |
| **Full suite command** | `python3 - <<'PY'\nfrom pathlib import Path\npaths = [\n    Path('.planning/phases/01-proof-contract-and-corpus/01-VALIDATION.md'),\n    Path('.planning/phases/02-isolated-proof-harness/02-VALIDATION.md'),\n    Path('.planning/phases/03-real-repo-semantic-verification/03-VALIDATION.md'),\n    Path('.planning/phases/04-verdicts-and-acceptance-summaries/04-VALIDATION.md'),\n]\nfor p in paths:\n    text = p.read_text()\n    for needle in ['## Test Infrastructure','## Sampling Rate','## Per-task Verification Map','## Wave 0 Requirements','## Manual-Only Verifications','## Validation Sign-Off','Approval: approved']:\n        assert needle in text, (p, needle)\naudit = Path('.planning/v1.0-v1.0-MILESTONE-AUDIT.md').read_text()\nassert 'Nyquist discovery is enabled' in audit\nprint('PASS')\nPY` |
| **Estimated runtime** | ~5 seconds |

---

## Sampling Rate

- **After every task commit:** Run the quick command
- **After every plan wave:** Run the full suite command
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 5 seconds

---

## Per-task Verification Map

| task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 07-01-01 | 01 | 1 | None | T-07-01 | Backfilled Phase 01-02 validation docs point only to existing approved evidence and keep approved Nyquist frontmatter. | contract | `python3 - <<'PY'\nfrom pathlib import Path\nfor p in [Path('.planning/phases/01-proof-contract-and-corpus/01-VALIDATION.md'), Path('.planning/phases/02-isolated-proof-harness/02-VALIDATION.md')]:\n    text = p.read_text()\n    assert 'status: approved' in text\n    assert 'nyquist_compliant: true' in text\nprint('PASS')\nPY` | ✅ | ⬜ pending |
| 07-01-02 | 01 | 1 | None | T-07-01 / T-07-02 | Backfilled Phase 03-04 validation docs preserve real command/task mappings and do not invent new approval criteria. | contract | `python3 - <<'PY'\nfrom pathlib import Path\nfor p in [Path('.planning/phases/03-real-repo-semantic-verification/03-VALIDATION.md'), Path('.planning/phases/04-verdicts-and-acceptance-summaries/04-VALIDATION.md')]:\n    text = p.read_text()\n    assert '## Per-task Verification Map' in text\n    assert 'Approval: approved' in text\nprint('PASS')\nPY` | ✅ | ⬜ pending |
| 07-01-03 | 01 | 1 | None | T-07-03 | Milestone-audit evidence is refreshed so Nyquist discovery can find complete validation-doc coverage. | integration | `python3 - <<'PY'\nfrom pathlib import Path\npaths = [\n    Path('.planning/phases/01-proof-contract-and-corpus/01-VALIDATION.md'),\n    Path('.planning/phases/02-isolated-proof-harness/02-VALIDATION.md'),\n    Path('.planning/phases/03-real-repo-semantic-verification/03-VALIDATION.md'),\n    Path('.planning/phases/04-verdicts-and-acceptance-summaries/04-VALIDATION.md'),\n]\nassert all(p.exists() for p in paths)\nprint('PASS')\nPY` | ✅ | ⬜ pending |

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
- [x] Feedback latency < 5s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved 2026-04-12
