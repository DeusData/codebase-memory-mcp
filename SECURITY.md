# Security Policy

## Transparency & Disclaimer

codebase-memory-mcp interacts deeply with your filesystem. It reads source files across your entire codebase, writes to agent configuration files, and spawns background processes. This is inherent to what it does — not a bug.

**If you are uncomfortable with these access patterns**, please audit the source code before running. The full source is available in this repository. Release archives produced by the current release pipeline are verifiably built from this source and can be independently verified via SLSA Build Level 3 provenance, Sigstore signatures, and SHA-256 checksums (see [Verification](#verification) below). Each archive contains a native executable and its authenticated release-owned runtime assets.

We are humans and can make mistakes. We take security seriously — it is Priority #1 for this project — but we cannot guarantee perfection. By using this software you accept responsibility for evaluating whether it meets your own security requirements.

## Runtime Network Behavior

Indexing, graph queries, semantic search, and MCP tool handling run locally. The
MCP server does not upload source code, repository paths, graph indexes, query
contents, environment variables, usage metrics, or telemetry.

The MCP server has one best-effort external runtime check: after MCP
`initialize`, it starts a background update-check thread that requests release
metadata from
`https://api.github.com/repos/DeusData/codebase-memory-mcp/releases/latest`.
That request is used only to show an update notice when a newer release exists.
It sends no project data; only standard HTTPS metadata, such as the destination
host and the normal `curl` request headers, are visible to GitHub and the
network path.

The update check is non-blocking for MCP startup and tool calls. If the machine
is offline, DNS fails, GitHub is unreachable, or `curl` exits with an error, the
check is ignored. The request is also bounded with `curl --max-time 5`; a
process shutting down immediately while the check is still running may wait for
that bounded background thread to finish.

Explicit install, package-manager, and `codebase-memory-mcp update` flows are
separate user-initiated network operations that download release assets and
checksums from GitHub.

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

This project implements multiple layers of security verification. Every release archive is signed and checksummed, and every extracted runtime object must pass its applicable checks before users can download it (draft → verify → publish flow).

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

1. **SLSA Build Level 3 provenance for release archives** — cryptographic attestation generated inside the trusted GitHub Actions build workflow immediately after each release archive is produced
2. **Sigstore cosign signing** — keyless digital signatures verifiable by anyone
3. **SBOM** — Software Bill of Materials (SPDX) listing all vendored dependencies
4. **SHA-256 checksums** — published with every release
5. **VirusTotal scanning** — three behaviourally identical executable candidates — unstripped, debug-stripped and stripped — are derived from one linker output for every release product and scanned before smoke/soak. Zero malicious and zero suspicious is preferred; the only tolerated result is exactly one Microsoft malicious label ending in `!ml`, which is disclosed. The selected executable is packaged without changing its SHA-256. After packaging, every distinct object extracted from the 14 shipped containers is scanned under the same policy — `install.sh`, `install.ps1`, `LICENSE`, `THIRD_PARTY_NOTICES.md`, the MCPB `manifest.json` and the unpacked UI assets — so the covered surface is everything we publish, not only the executables. Release notes link the verdict for the exact bytes shipped; the per-candidate results and the selection table are published as release assets for independent audit.
6. **OpenSSF Scorecard** — repository security health score

Scope of the SLSA claim: this is a build provenance claim for release
artifacts. It proves the attested archive was produced by the repository's
trusted GitHub-hosted build workflow from repository source. It is not
third-party certification, it is not retroactive to older releases, and it does
not mean the source code is vulnerability-free or that maintainers cannot change
source. Consumers should verify the signer workflow, not only repository
ownership.

If a scanned executable candidate has any result outside the one narrowly reviewed
Microsoft `!ml` tolerance, the release stays as a draft and is not published.
There is no manual bypass around that release gate; its sole machine-enforced
tolerance and selection table are specified in [Our release policy](#our-release-policy).

### Code-Level Defenses

- **Shell injection prevention** — `cbm_validate_shell_arg()` rejects metacharacters before all `popen()`/`system()` calls
- **SQLite authorizer** — blocks `ATTACH`/`DETACH` at engine level (prevents file creation via SQL injection)
- **CORS locked to localhost** — graph UI only accessible from localhost origins
- **Path containment** — `realpath()` check prevents reading files outside project root
- **Process-kill restriction** — only server-spawned PIDs can be terminated
- **Release-set verification** — installers verify the downloaded archive's exact
  member set and SHA-256 before the candidate executable is activated

### Verification

Users can independently verify any release archive and the runtime set it contains:

```bash
# SLSA Build Level 3 provenance for release archives
gh attestation verify <downloaded-file> \
  --repo DeusData/codebase-memory-mcp \
  --signer-workflow DeusData/codebase-memory-mcp/.github/workflows/_build.yml

# Sigstore cosign (keyless signature)
cosign verify-blob --bundle <file>.bundle <file>

# SHA-256 checksum
sha256sum -c checksums.txt

# VirusTotal (follow the durable per-candidate result links in the release notes)
# https://www.virustotal.com/
```

## Antivirus False Positives

Some release binaries are reported by **one** engine — Microsoft — as
`Trojan:Script/Wacatac.B!ml`. We believe this is a false positive, we do not hide
it, and this section exists so you can check that judgement yourself rather than
take our word for it.

### What the detection is

The `!ml` suffix marks a **machine-learning / heuristic** classification, not a
signature match. The `Script` token is a generic bucket in that naming scheme and
says nothing about script content — GitHub's own `gh` CLI and Anthropic's Claude
installer have both carried `Trojan:Script/Wacatac.H!ml` on native binaries.

Typically 61 of ~62 engines on VirusTotal return clean for the same file.

### What we measured

We dissected a full release matrix built from one commit. The verdicts split
across every axis at once:

| Binary | Link | Verdict |
|---|---|---|
| linux-amd64 | dynamic | flagged |
| linux-amd64-portable | static | clean |
| linux-arm64 | dynamic | clean |
| linux-arm64-portable | static | flagged |
| darwin-amd64 | — | flagged |
| darwin-arm64 | — | clean |
| windows amd64 / arm64 | — | clean |

The static/dynamic axis **inverts** between architectures, so no build or link
property explains it. The two macOS binaries have identical segment structure and
still split. Sibling artifacts from one build landed in *different* variant
buckets (`.B` vs `.C`).

We also tested and rejected the obvious structural hypothesis: entropy is low
everywhere (embedded vectors 4.17, parse tables 3.46 bits/byte, against 7.5–8.0
for genuinely packed payloads), so the binary does not resemble a packed dropper.

### What we changed, and what we reverted

We removed every embedded shell script from the binary and moved the UI bundle
and the agent integration templates out into separate verified files. **The
detection count did not drop** — it simply moved between artifacts. We reverted
both changes rather than keep permanent complexity that bought nothing. We are
documenting that here because a negative result is still evidence.

### This is endemic, not specific to us

The same `!ml` family repeatedly hits large, unsigned, native open-source
binaries:

- [llama.cpp #15874](https://github.com/ggml-org/llama.cpp/issues/15874),
  [#24487](https://github.com/ggml-org/llama.cpp/issues/24487),
  [#24558](https://github.com/ggml-org/llama.cpp/issues/24558) — including one
  DLL of many in a single archive, with nothing found on reverse engineering
- [GitHub CLI #13306](https://github.com/cli/cli/issues/13306)
- [Microsoft's own Go toolchain #1255](https://github.com/microsoft/go/issues/1255)
- [Anthropic Claude Code #36796](https://github.com/anthropics/claude-code/issues/36796)
- [yt-dlp #7532](https://github.com/yt-dlp/yt-dlp/issues/7532),
  [Godot #110612](https://github.com/godotengine/godot/issues/110612),
  [PyInstaller #5854](https://github.com/pyinstaller/pyinstaller/issues/5854),
  [Tauri #2486](https://github.com/tauri-apps/tauri/issues/2486),
  [OpenAI Codex #2228](https://github.com/openai/codex/issues/2228),
  [rust-lang/rust #88297](https://github.com/rust-lang/rust/issues/88297)

A Microsoft engineer on the Go team [put it plainly](https://github.com/microsoft/go/issues/1255):
*"we're aware of Windows Security/Defender issues with Go apps… we can't exactly
go fix something and solve all Go false positives."*

### Why we do not obfuscate around it

Deliberately reshaping a binary to avoid a classifier is what malware does, and
it measurably backfires — the same Microsoft engineer reports that obfuscation
*"increases scrutiny rather than avoiding it."* We would rather be scannable and
explain a false positive than be unreadable and score well.

### Verify it yourself

Every release ships the material needed to check our artifacts independently.
Use the commands in [Verification](#verification) above: SLSA Build Level 3
provenance ties the archive to the workflow run that produced it, Sigstore cosign
verifies the signature, and `checksums.txt` pins the bytes. The release notes
carry a durable VirusTotal link for every executable candidate, including any
tolerated detection — we publish those results whether or not they are clean.

You can also rebuild from source and compare: `scripts/build.sh --with-ui`
produces the shipped composition.

### Our release policy

A release may ship with **at most one** detection, and only when the engine is
Microsoft and the label ends in `!ml`. Two or more engines, any signature-based
label, any other vendor, or any "suspicious" verdict blocks the release. That
rule is enforced in `scripts/ci/check-virustotal.sh` and pinned by
`tests/test_vt_gate_policy_contract.sh`.

Before smoke and soak testing, each of the eight supported platform/link-mode
products is built in two conventional forms from the same linked executable:
stripped and unstripped. Both candidates are scanned, and selection is confined
to that product tuple; for example, a portable static Linux build can never be
substituted for the ordinary dynamically linked Linux build.

The selection policy is deterministic:

| Stripped candidate | Unstripped candidate | Selected candidate |
|---|---|---|
| Clean | Clean | Stripped |
| Clean | One tolerated Microsoft `!ml` | Stripped |
| One tolerated Microsoft `!ml` | Clean | Unstripped |
| One tolerated Microsoft `!ml` | One tolerated Microsoft `!ml` | Stripped |

Any other verdict, an incomplete scan, a missing sibling, or a provenance/hash
mismatch blocks the complete release. The selected bytes are then packaged
without stripping, signing, relinking, or any other content mutation, and the
resulting archives—not rejected candidates—are what smoke and soak testing execute.
After packaging, every archive and MCPB executable is extracted and its SHA-256
must still equal the selected candidate. This identity check makes a second
VirusTotal submission of the same bytes unnecessary.

This is not obfuscation: stripping is a standard release transformation, both
forms and their SHA-256 provenance are recorded, and no binary is modified in
response to an engine result. If the unstripped form is selected, it is larger
and can expose compiler symbol names and build-time path metadata (but not source
file contents by itself). Stripping also changes intentionally observable
developer-facing surfaces such as symbolized backtraces, debugger visibility,
and executable-symbol lookup; the two forms are therefore not claimed to be
byte-for-byte or introspection-equivalent. They are conventional copy/strip
outputs of one linker result, and the actually selected form—not its sibling—is
subjected to the full smoke/soak gate. That trade-off is visible in the
published selection evidence.

Policy identifier: `cbm-vt-candidate-selection-v1`. It is reviewed at least
every 90 days; last review: 2026-08-13, next review due: 2026-11-11.

### If you find something real

We would genuinely rather be wrong in public than confidently wrong. If you find
anything that explains or contradicts the assessment above:

- Open an issue with the **`av-analysis`** label — include the artifact SHA-256,
  the engine and label, and what you found.
- If it looks like an actual compromise rather than a classifier artifact, use
  the private process in [Reporting a Vulnerability](#reporting-a-vulnerability)
  instead.

A concrete finding changes our position. Signing is on the roadmap and will help
on Windows, but note that no code-signing scheme exists that AV engines honour
for Linux ELF binaries, so it is not a complete answer either.

## Supported Versions

| Version | Supported |
|---------|-----------|
| Latest `0.10.x` | Yes — security fixes land in the newest release |
| < 0.10   | No — please upgrade to the latest release |

Only the latest release is supported. Security fixes are shipped in a new
patched release rather than backported to older versions; upgrading to the
newest version is the supported path to receive them.
