---
phase: 06
slug: parallel-native-suite-repair
status: approved
threats_open: 0
asvs_level: 1
created: 2026-04-12
---

# Phase 06 - Security

> Per-phase security contract: threat register, accepted risks, and audit trail.

---

## Trust Boundaries

| Boundary | Description | Data Crossing |
|----------|-------------|---------------|
| `CBM_FORCE_PIPELINE_MODE` / test fixture input -> pipeline mode routing | Untrusted local test input forces the code down the parallel indexing path whose semantics must stay equivalent for supported edge extraction. | Local environment-variable override and synthetic FastAPI source text |
| parallel cache lifetime -> FastAPI Depends edge generation | Parsed extraction results cross from parallel extract/resolve stages into a later semantic pass that must not read freed cache entries. | In-memory `CBMFileResult **result_cache` contents and derived CALLS edges |

---

## Threat Register

| Threat ID | Category | Component | Disposition | Mitigation | Status |
|-----------|----------|-----------|-------------|------------|--------|
| T-06-01 | D | forced-parallel FastAPI dependency edge path | mitigate | Keep a dedicated forced-parallel regression in `tests/test_pipeline.c` and require the full forced-parallel suite command in validation so silent edge loss is caught before release. | closed |
| T-06-02 | T | parallel cache lifetime around `cbm_pipeline_pass_fastapi_depends()` | mitigate | Reuse the existing pass while cache entries are still alive in both full and incremental parallel flows; do not duplicate the semantic logic in a second implementation. | closed |

*Status: open · closed*
*Disposition: mitigate (implementation required) · accept (documented risk) · transfer (third-party)*

---

## Accepted Risks Log

No accepted risks.

---

## Security Audit Trail

| Audit Date | Threats Total | Closed | Open | Run By |
|------------|---------------|--------|------|--------|
| 2026-04-12 | 2 | 2 | 0 | OpenCode |

---

## Sign-Off

- [x] All threats have a disposition (mitigate / accept / transfer)
- [x] Accepted risks documented in Accepted Risks Log
- [x] `threats_open: 0` confirmed
- [x] `status: approved` set in frontmatter

**Approval:** approved 2026-04-12
