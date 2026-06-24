# Security Disclosure & Handling Process

This document explains **how security reports are handled** for
codebase-memory-mcp — what happens after you report a vulnerability, what you
can expect from us, and how disclosure and credit work.

For **how to report** a vulnerability and **what is in scope**, see
[`SECURITY.md`](../SECURITY.md). This document covers the process *after* a
report arrives.

> **This is a solo, volunteer-maintained project.** Everything below is handled
> on a good-faith, best-effort basis. The timeframes are honest targets we aim
> to beat — not contractual guarantees. If something will take longer, we will
> tell you and keep you updated rather than go silent.

## Principles

We follow **coordinated disclosure**:

1. **Fix privately, disclose publicly.** Details of an unfixed vulnerability are
   never discussed in the open. We develop and validate the fix in private, ship
   a release, and only then disclose.
2. **Patch before publicity.** A fixed release is always available *before* the
   vulnerability is described publicly, so users can update immediately.
3. **Credit the researcher.** Public credit by default; anonymity on request.
4. **A bug fixed once should stay fixed.** Every fix ships with a regression
   test or guard so the same class of issue cannot silently return.

## What happens after you report

| Step | What we do | Target (best-effort) |
|------|------------|----------------------|
| 1. **Acknowledge** | Confirm we received your report and are looking at it. | within **7 days** (usually much sooner) |
| 2. **Triage & severity** | Reproduce the issue and assign a severity (CVSS). | within **14 days** |
| 3. **Fix privately** | Develop the fix in a private environment, with a regression guard, and validate it across all supported platforms (Linux, macOS, Windows) under full CI. | severity-dependent |
| 4. **You verify** | We invite you (read-only) to confirm the fix resolves the issue and that the guard prevents regression. Your sign-off is welcomed; an unresponsive reporter will not indefinitely block a release. | — |
| 5. **Release** | Merge the fix and cut a patched release promptly. | as fast as severity warrants |
| 6. **Disclose** | Publish a [GitHub Security Advisory](https://github.com/DeusData/codebase-memory-mcp/security/advisories), request a **CVE** (GitHub is a CNA), and credit you. | after a short upgrade window |

**Overall fix timeline:** we aim to resolve and release a fix within **90 days**
of triage, and much faster for high-severity issues. Critical, actively
exploitable issues are handled with the highest priority.

## Severity

We assess severity using **CVSS** and prioritise accordingly. Roughly:

- **Critical / High** — remote code execution, sandbox/scope escape, supply-chain
  compromise. Prioritised; expedited release.
- **Medium** — issues requiring local access, non-default configuration, or
  significant user interaction.
- **Low** — defense-in-depth gaps, hardening, information exposure with limited
  impact.

## Credit & CVE

- You are **credited by name/handle** in the published advisory unless you ask to
  remain anonymous.
- A **CVE identifier** is requested for each distinct vulnerability via the
  GitHub Security Advisory (one CVE per vulnerability, not per report — a single
  report may yield several).
- The advisory lists the **affected and patched version ranges** so downstream
  tooling (e.g. Dependabot) can alert users automatically.

## Safe harbor

We will not pursue or support legal action against researchers who act in
**good faith**, meaning you:

- only access, modify, or store data in **your own test environment**;
- avoid privacy violations, data destruction, and degradation of service for
  others;
- give us a **reasonable opportunity to fix** the issue before disclosing it
  publicly;
- do not exploit the issue beyond the minimum necessary to demonstrate it.

Good-faith research conducted under this policy is considered authorised, and we
will work with you, not against you.

## What we ask of you

- Report **privately** (see [`SECURITY.md`](../SECURITY.md)) — not as a public
  issue, PR, or social-media post.
- Give us **reasonable time** to fix before any public write-up.
- Provide enough detail to **reproduce** (affected version, steps, impact).

Thank you for helping keep a tool used by developers worldwide safe. 🙏
