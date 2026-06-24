# Security Policy

## Transparency & Disclaimer

codebase-memory-mcp interacts deeply with your filesystem. It reads source files across your entire codebase, writes to agent configuration files, and spawns background processes. This is inherent to what it does — not a bug.

**If you are uncomfortable with these access patterns**, please audit the source code before running. The full source is available in this repository. Every release binary is reproducibly built from this source and can be independently verified via SLSA provenance, Sigstore signatures, and SHA-256 checksums (see [Verification](#verification) below).

We are humans and can make mistakes. We take security seriously — it is Priority #1 for this project — but we cannot guarantee perfection. By using this software you accept responsibility for evaluating whether it meets your own security requirements.

## Help Us Stay Secure

**We actively invite security researchers to try to break this project.**

If you find a vulnerability — anything from a logic bug to a remote code execution — we want to know. You will receive a fast response, public credit (if you want it), and the knowledge that you helped make a tool used by developers worldwide more secure.

What we consider in scope:

- Arbitrary code execution via MCP tool inputs or CLI arguments
- File reads or writes outside the indexed project root
- Shell injection through any code path
- Binary tampering or supply chain attacks
- Privilege escalation or sandbox escapes

Please report **privately** rather than as a public issue so we can fix before public disclosure. See below for how.

## Reporting a Vulnerability

If you discover a security vulnerability, please report it **privately** so we
can fix it before public disclosure:

1. **Do NOT open a public issue, PR, or social-media post** for security
   vulnerabilities.
2. **Preferred:** use GitHub's [private vulnerability reporting](https://github.com/DeusData/codebase-memory-mcp/security/advisories/new)
   (the repository's **Security → Report a vulnerability** button). This keeps
   everything in one place and starts a private advisory automatically.
3. **Alternative:** email martin.vogel.tech@gmail.com.
4. Include: description, reproduction steps, affected version, and potential
   impact.
5. Include your **GitHub handle and a contact email**. We use these to credit
   you and to invite you (read-only) to privately verify the fix before its
   release — see step 4 of the
   [handling process](docs/SECURITY-DISCLOSURE.md#what-happens-after-you-report).
   Let us know if you would prefer to remain anonymous.

> **This is a solo, volunteer-maintained project, so security handling is
> best-effort.** As good-faith targets — not guarantees — we aim to:
>
> - **acknowledge** your report within **7 days** (usually much sooner);
> - give an **initial assessment and severity** within **14 days**;
> - **develop, validate, and release a fix** as quickly as the severity
>   warrants — typically within **90 days**, and expedited for high-severity
>   issues.
>
> If something will take longer, we will tell you and keep you updated.

We follow **coordinated disclosure**: fixes are developed privately, validated
across all supported platforms, released, and only then disclosed publicly via a
[GitHub Security Advisory](https://github.com/DeusData/codebase-memory-mcp/security/advisories)
with a **CVE** and credit to you. The full handling process — including how you
can verify the fix before release — is documented in
[`docs/SECURITY-DISCLOSURE.md`](docs/SECURITY-DISCLOSURE.md).

### Safe harbor

We will not pursue or support legal action against researchers who act in good
faith — accessing only their own test data, avoiding privacy violations and
service disruption, and giving us reasonable time to fix before public
disclosure. Research conducted under this policy is considered authorised.

## Security Measures

This project implements multiple layers of security verification. Every release binary must pass all checks before users can download it (draft → verify → publish flow).

### Build-Time (CI — every commit)

- **8-layer security audit suite** runs on every build:
  - Layer 1: Static allow-list for dangerous calls (`system`/`popen`/`fork`) + hardcoded URLs
  - Layer 2: Binary string audit (URLs, credentials, dangerous commands)
  - Layer 3: Network egress monitoring via strace (Linux)
  - Layer 4: Install output path + content validation
  - Layer 5: Smoke test hardening (clean shutdown, residual processes, version integrity)
  - Layer 6: Graph UI audit (external domains, CORS, server binding, eval/iframe)
  - Layer 7: MCP robustness (23 adversarial JSON-RPC payloads)
  - Layer 8: Vendored dependency integrity (SHA-256 checksums, dangerous call scan)
- **All dangerous function calls** require a reviewed entry in `scripts/security-allowlist.txt`
- **Time-bomb pattern detection** — scans for `time()`/`sleep()` near dangerous calls (could indicate delayed activation)
- **MCP tool handler file read audit** — tracks file read count in `mcp.c` against an expected maximum (detects added file reads that could exfiltrate data through tool responses)
- **CodeQL SAST** — static application security testing on every push (taint analysis, CWE detection, data flow tracking). Any open alert blocks the release.
- **Fuzz testing** — random/mutated inputs to MCP server and Cypher parser (60 seconds per build). Catches crashes, segfaults, and memory errors that structured tests miss.
- **Native antivirus scanning** on every platform (any detection fails the build):
  - **Windows**: Windows Defender with ML heuristics — the same engine end users run
  - **Linux**: ClamAV with daily signature updates
  - **macOS**: ClamAV with daily signature updates

### Release-Time (draft → verify → publish)

Releases are created as **drafts** (invisible to users) and only published after all verification passes:

1. **SLSA build provenance** — cryptographic attestation proving each binary was built by GitHub Actions from this repository
2. **Sigstore cosign signing** — keyless digital signatures verifiable by anyone
3. **SBOM** — Software Bill of Materials (CycloneDX) listing all vendored dependencies
4. **SHA-256 checksums** — published with every release
5. **VirusTotal scanning** — all binaries scanned by 70+ antivirus engines (zero-tolerance: any detection blocks the release)
6. **OpenSSF Scorecard** — repository security health score

If ANY antivirus engine flags ANY binary, the release stays as a draft and is not published until the issue is investigated and resolved.

### Code-Level Defenses

- **Shell injection prevention** — `cbm_validate_shell_arg()` rejects metacharacters before all `popen()`/`system()` calls
- **SQLite authorizer** — blocks `ATTACH`/`DETACH` at engine level (prevents file creation via SQL injection)
- **CORS locked to localhost** — graph UI only accessible from localhost origins
- **Path containment** — `realpath()` check prevents reading files outside project root
- **Process-kill restriction** — only server-spawned PIDs can be terminated
- **SHA-256 checksum verification** — update command verifies downloaded binary before installing

### Verification

Users can independently verify any release binary:

```bash
# SLSA provenance (proves binary came from this repo's CI)
gh attestation verify <downloaded-file> --repo DeusData/codebase-memory-mcp

# Sigstore cosign (keyless signature)
cosign verify-blob --bundle <file>.bundle <file>

# SHA-256 checksum
sha256sum -c checksums.txt

# VirusTotal (upload binary or check the report links in the release notes)
# https://www.virustotal.com/
```

## Supported Versions

| Version | Supported |
|---------|-----------|
| Latest `0.8.x` | Yes — security fixes land in the newest release |
| < 0.8   | No — please upgrade to the latest release |

Only the latest release is supported. Security fixes are shipped in a new
patched release rather than backported to older versions; upgrading to the
newest version is the supported path to receive them.
