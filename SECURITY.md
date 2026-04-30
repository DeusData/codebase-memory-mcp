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

If you discover a security vulnerability, please report it responsibly:

1. **Do NOT open a public issue** for security vulnerabilities
2. Email: martin.vogel.tech@gmail.com
3. Include: description, reproduction steps, affected version, potential impact

We will acknowledge your report within 48 hours and provide a fix timeline within 7 days.

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
| 0.5.x   | Yes       |
| < 0.5   | No (Go codebase, superseded by C rewrite) |
