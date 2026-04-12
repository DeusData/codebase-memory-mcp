# Phase 3: Real-Repo Semantic Verification - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-12
**Phase:** 03-real-repo-semantic-verification
**Areas discussed:** MCP usefulness bar, Repo coverage, Evidence granularity, Sequential versus parallel parity

---

## MCP usefulness bar

| Option | Description | Selected |
|--------|-------------|----------|
| Queries only | Use the existing fixed proof query suite as the approval bar for Phase 03, without adding separate direct MCP workflow checks. | ✓ |
| Queries plus MCP spot-checks | Keep the fixed proof query suite as primary evidence, but add a small number of direct MCP graph/query/tracing checks for usefulness. | |
| Broader MCP workflow set | Require a larger set of direct MCP workflow checks beyond the existing proof query suite. | |

**User's choice:** Queries only
**Notes:** The existing proof query suite should be sufficient for Phase 03 usefulness validation.

---

## Repo coverage

| Option | Description | Selected |
|--------|-------------|----------|
| All four repos | Run evidence across all four pinned manifest repos so Phase 03 covers the full approved corpus. | ✓ |
| Category owners only | Only require the repos tagged as owning each required behavior in manifest `required_for`. | |
| Anchor plus spot-checks | Require one anchor repo for broad coverage, with lighter spot-check coverage on the rest. | |

**User's choice:** All four repos
**Notes:** Phase 03 should keep the full manifest corpus as the approval surface.

---

## Evidence granularity

| Option | Description | Selected |
|--------|-------------|----------|
| Counts plus samples | Require non-zero counts and representative samples, but not explicit asserted edge checks everywhere. | ✓ |
| Edges plus samples | Require the strongest available evidence: explicit asserted edges where relevant, plus representative class/method samples. | |
| Counts only | Treat non-zero counts alone as sufficient evidence. | |

**User's choice:** Counts plus samples
**Notes:** Representative samples are required, but Phase 03 should not force explicit edge assertions as the universal bar for every behavior.

---

## Sequential versus parallel parity

| Option | Description | Selected |
|--------|-------------|----------|
| Same semantic outcomes | Require the same core semantic results and gating outcomes across both paths, while tolerating non-semantic artifact differences. | ✓ |
| Exact artifact equality | Require sequential and parallel runs to match exactly at the artifact level. | |
| Same pass/fail only | Only require both paths to produce the same high-level gating pass/fail result. | |

**User's choice:** Same semantic outcomes
**Notes:** Semantic consistency matters; exact artifact equality is unnecessary, and pass/fail parity alone is too weak.

---

## OpenCode's Discretion

- Exact comparison-report format for sequential-versus-parallel semantic review.
- Exact sample presentation and threshold details, provided they support counts-plus-samples evidence.

## Deferred Ideas

- Direct MCP workflow spot-checks beyond the fixed proof query suite.
- Exact artifact-equality comparison requirements.

---

*Phase: 03-real-repo-semantic-verification*
*Discussion log generated: 2026-04-12*
