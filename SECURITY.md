# Security Policy

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
