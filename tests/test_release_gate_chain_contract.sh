#!/usr/bin/env bash
# Contract: making a release phase optional must not silently make the phases
# AFTER it optional too, and a draft must require smoke to have actually run.
#
# This exists because it nearly shipped untested binaries. `skip_tests: true`
# skipped `test`, and GitHub propagates "skipped" TRANSITIVELY down the needs
# graph: `build` overrode the condition and ran, but `smoke` and `soak` had no
# override and were skipped. Nothing failed. Nothing said so. `release-draft`
# then ran anyway, because `!cancelled() && !failure()` is fail-OPEN — a skipped
# job is neither cancelled nor failed — so the pipeline was one gate away from
# publishing artifacts nobody had smoke-tested or soaked.
#
# Two properties are pinned:
#   1. every job downstream of an optional phase carries the
#      `!cancelled() && !failure()` override, so it runs when an ancestor was
#      deliberately skipped;
#   2. release-draft requires both smoke and soak to succeed explicitly, so a
#      skipped runtime gate BLOCKS the draft instead of sailing past it.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WF="$ROOT/.github/workflows/release.yml"
BUILD_WF="$ROOT/.github/workflows/_build.yml"
DRY_WF="$ROOT/.github/workflows/dry-run.yml"
SOAK_WF="$ROOT/.github/workflows/_soak.yml"
[ -f "$WF" ] || { echo "FAIL: $WF not found" >&2; exit 2; }
[ -f "$BUILD_WF" ] || { echo "FAIL: $BUILD_WF not found" >&2; exit 2; }
[ -f "$DRY_WF" ] || { echo "FAIL: $DRY_WF not found" >&2; exit 2; }
[ -f "$SOAK_WF" ] || { echo "FAIL: $SOAK_WF not found" >&2; exit 2; }

python3 - "$WF" "$BUILD_WF" "$DRY_WF" "$SOAK_WF" <<'PY'
import pathlib
import re
import sys

text = pathlib.Path(sys.argv[1]).read_text()
build_text = pathlib.Path(sys.argv[2]).read_text()
dry_text = pathlib.Path(sys.argv[3]).read_text()
soak_text = pathlib.Path(sys.argv[4]).read_text()

# Slice the file into top-level job blocks: two-space indented "name:".
blocks, current, name = {}, [], None
for line in text.splitlines():
    m = re.match(r"^  ([A-Za-z0-9_-]+):\s*$", line)
    if m:
        if name:
            blocks[name] = "\n".join(current)
        name, current = m.group(1), []
        continue
    if name is not None:
        current.append(line)
if name:
    blocks[name] = "\n".join(current)

failures = []

def cond(job):
    """The job's `if:` expression, block scalars folded onto one line."""
    body = blocks.get(job, "")
    m = re.search(r"^    if:\s*(>-|>|\|-|\|)?\s*(.*?)(?=^    [a-z_-]+:|\Z)",
                  body, re.S | re.M)
    if not m:
        return ""
    return " ".join(m.group(2).split())

# 1. Gate conditions: tolerate ONLY the sanctioned skip, fail closed otherwise.
#    `test` is the optional phase (if: !inputs.skip_tests). The old contract
#    required the bare `!cancelled() && !failure()` idiom — but failure() does
#    NOT cover a needed job that was CANCELLED (e.g. a lint timeout), so that
#    idiom let a cancelled gate cascade test into 'skipped' and publish with
#    the whole test matrix silently gone (v0.10.7 incident, 2026-08-18).
#    Each gate must name the results it accepts explicitly.
GATE_REQUIREMENTS = {
    "build": [
        "!cancelled()",
        "needs.lint.result == 'success'",
        "needs.test.result == 'success'",
        "inputs.skip_tests && needs.test.result == 'skipped'",
    ],
    "smoke": ["!cancelled()", "needs.build.result == 'success'"],
    "soak": ["!cancelled()", "needs.build.result == 'success'"],
    "release-draft": ["!cancelled()", "!failure()"],
}
for job, required in GATE_REQUIREMENTS.items():
    if job not in blocks:
        failures.append(f"{job}: job missing from release.yml — update this contract")
        continue
    c = cond(job)
    for fragment in required:
        if fragment not in c:
            failures.append(
                f"{job}: `if:` must contain `{fragment}` (got: {c or '<none>'}).\n"
                f"      Explicit results only: failure() misses CANCELLED needed\n"
                f"      jobs, and a bare tolerate-skip idiom is fail-open.")

# 1b. The preflight input guard must exist and gate the whole chain: the tag is
#     inputs.version verbatim, and a bare (non-v-prefixed) version publishes a
#     release installers can never resolve — unrecoverable under immutability.
if "preflight" not in blocks:
    failures.append("preflight: job missing — the version-input guard must exist")
lint_needs = re.search(r"^    needs:\s*(.*)$", blocks.get("lint", ""), re.M)
if not lint_needs or "preflight" not in lint_needs.group(1):
    failures.append(
        "lint: must `needs: [preflight]` so a malformed version stops the\n"
        "      chain before any gate runs.")

# 2. The draft must require both runtime gates to have genuinely succeeded.
draft = cond("release-draft")
if "needs.smoke.result == 'success'" not in draft:
    failures.append(
        "release-draft: `if:` must require needs.smoke.result == 'success'.\n"
        "      `!cancelled() && !failure()` alone is fail-open: a SKIPPED smoke\n"
        "      passes it, and the draft gets cut from unsmoked binaries.")

if "needs.soak.result == 'success'" not in draft:
    failures.append(
        "release-draft: `if:` must require needs.soak.result == 'success'.\n"
        "      Every production release must soak the exact selected bytes; a\n"
        "      skipped soak must block the draft.")

soak_input = re.search(
    r"^      soak_level:\s*\n(?P<body>(?:^        .*\n?)*)", text, re.M)
if not soak_input or "none" in soak_input.group("body"):
    failures.append(
        "release: production soak_level must not expose a 'none' choice.\n"
        "      The explicit bypass belongs only to dry-run; selected release\n"
        "      bytes must always complete soak before drafting.")

# The MCP registry validates every package URL it is handed by FETCHING it, and
# a DRAFT release's assets are not publicly readable. publish-final is the job
# that un-drafts. Both jobs used to need only publish-registries, so they raced
# and the registry was told its own .mcpb URL was a 404 (v0.10.3). The registry
# must therefore run AFTER the release is public — while still never gating it.
def needs(job):
    """The job's `needs:` list as a single line."""
    m = re.search(r"^    needs:\s*(.*)$", blocks.get(job, ""), re.M)
    return m.group(1).strip() if m else ""

if "publish-mcp-registry" not in blocks:
    failures.append("publish-mcp-registry: job missing from release.yml — update this contract")
elif "publish-final" not in needs("publish-mcp-registry"):
    failures.append(
        "publish-mcp-registry: must `needs:` publish-final. The registry fetches\n"
        "      the .mcpb URLs it validates, and a draft release's assets 404 —\n"
        "      running it in parallel with the un-draft is a race it loses.")

# ...and the reverse must NEVER hold: a registry outage must not block shipping.
if "publish-final" in blocks and "publish-mcp-registry" in needs("publish-final"):
    failures.append(
        "publish-final: must NOT depend on publish-mcp-registry. The binary\n"
        "      release is the product and the registry entry is metadata; a\n"
        "      registry-preview outage must never hold up a release.")

# 3. Candidate selection is part of the reusable build boundary. Native jobs
#    may upload only candidate pairs; the reusable workflow must not complete
#    until one candidate in each of the eight immutable target tuples has been
#    scanned, selected, and packaged under the historical binaries-* names.
def workflow_jobs(source):
    result, current, current_name = {}, [], None
    for line in source.splitlines():
        match = re.match(r"^  ([A-Za-z0-9_-]+):\s*$", line)
        if match:
            if current_name:
                result[current_name] = "\n".join(current)
            current_name, current = match.group(1), []
            continue
        if current_name is not None:
            current.append(line)
    if current_name:
        result[current_name] = "\n".join(current)
    return result

build_jobs = workflow_jobs(build_text)
select = build_jobs.get("select-package", "")
native_jobs = ["build-unix", "build-windows", "build-windows-arm64",
               "build-linux-portable"]

if not re.search(r"(?m)^      scan_candidates:\s*\n(?:        [^\n]*\n)*?        default:\s*true\s*$",
                 build_text):
    failures.append(
        "_build.yml: workflow_call must expose scan_candidates with default true;\n"
        "      an omitted input must fail toward scanning, never toward bypass.")

if not select:
    failures.append(
        "_build.yml: select-package job is missing. Candidate scanning, tuple-local\n"
        "      selection, and packaging must finish inside the reusable build.")
else:
    needs_match = re.search(r"^    needs:\s*\[([^]]+)\]", select, re.M)
    selected_needs = set()
    if needs_match:
        selected_needs = {part.strip() for part in needs_match.group(1).split(",")}
    missing = [job for job in native_jobs if job not in selected_needs]
    if missing:
        failures.append(
            "select-package: must need every native candidate producer; missing "
            + ", ".join(missing))

    timeout = re.search(r"^    timeout-minutes:\s*(\d+)\s*$", select, re.M)
    if not timeout or int(timeout.group(1)) < 300:
        failures.append(
            "select-package: timeout-minutes must be at least 300 so the bounded\n"
            "      four-hour VirusTotal poll can complete before job cleanup.")

    for token in ("--expect-targets 8", "--expect-candidates 24",
                  "VT_POLL_TIMEOUT_SECONDS: 14400",
                  "scripts/ci/select-release-candidates.py",
                  "scripts/ci/verify-release-selection.py",
                  'if ($i == "selected_sha256") sha_col = i',
                  '--expected-sha256 "$selected_sha"'):
        if token not in select:
            failures.append(f"select-package: missing required exact-set token: {token}")

    if "release-candidate-scan/objects/*" not in select:
        failures.append(
            "select-package: VirusTotal must scan only the staged 16 candidate\n"
            "      objects, not archives, sidecars, or unrelated artifacts.")
    if "--default-stripped" not in select or "unscanned-dry-run" not in select:
        failures.append(
            "select-package: the dry-run-only scan bypass must explicitly select\n"
            "      stripped candidates and record policy state unscanned-dry-run.")

    for artifact in (
        "binaries-linux-amd64",
        "binaries-linux-arm64",
        "binaries-linux-amd64-portable",
        "binaries-linux-arm64-portable",
        "binaries-darwin-amd64",
        "binaries-darwin-arm64",
        "binaries-windows-amd64",
        "binaries-windows-arm64",
        "release-selection-evidence",
    ):
        if f"name: {artifact}" not in select:
            failures.append(
                f"select-package: missing canonical output artifact {artifact}")

for job in native_jobs:
    body = build_jobs.get(job, "")
    if not body:
        failures.append(f"_build.yml: native producer job {job} is missing")
        continue
    if "release-candidates-" not in body:
        failures.append(f"{job}: must upload release-candidates-* pair artifact")
    if "name: binaries-" in body:
        failures.append(
            f"{job}: must not upload binaries-* before central VirusTotal selection")

# 4. Release cannot opt out of candidate scanning. Dry-run may do so only via
#    its explicit, visible skip_virustotal input; either way smoke and soak see
#    the reusable build only after selection/default-selection and packaging.
release_build = blocks.get("build", "")
if "scan_candidates: true" not in release_build or "secrets: inherit" not in release_build:
    failures.append(
        "release build: must pass scan_candidates: true and inherit the VT secret;\n"
        "      a release has no candidate-scan bypass.")

dry_jobs = workflow_jobs(dry_text)
dry_build = dry_jobs.get("build", "")
if "scan_candidates: ${{ !inputs.skip_virustotal }}" not in dry_build:
    failures.append(
        "dry-run build: scan_candidates must be exactly !inputs.skip_virustotal.")
if "secrets: inherit" not in dry_build:
    failures.append("dry-run build: must inherit the VirusTotal API secret")
if "virustotal" in dry_jobs:
    failures.append(
        "dry-run: obsolete post-smoke virustotal job must be removed; candidate\n"
        "      scanning/selection belongs before smoke inside the build job.")
for caller_name, caller_jobs in (("release", blocks), ("dry-run", dry_jobs)):
    for downstream in ("smoke", "soak"):
        body = caller_jobs.get(downstream, "")
        if not re.search(r"^    needs:\s*\[?build\]?\s*$", body, re.M):
            failures.append(
                f"{caller_name} {downstream}: must depend directly on completed build/select")
    if "use_release_artifacts: true" not in caller_jobs.get("soak", ""):
        failures.append(
            f"{caller_name} soak: release-bound soak must consume the selected binaries-* artifacts")

soak_jobs = workflow_jobs(soak_text)
for job in ("soak-quick", "soak-quick-windows", "soak-quick-windows-arm64"):
    body = soak_jobs.get(job, "")
    for token in ("inputs.use_release_artifacts", "actions/download-artifact@", "cbm-selected-artifact"):
        if token not in body:
            failures.append(f"_soak.yml {job}: selected-artifact mode is missing {token}")
portable_soak = soak_jobs.get("soak-quick-linux-portable", "")
for token in ("if: ${{ inputs.use_release_artifacts }}",
              "binaries-linux-${{ matrix.arch }}-portable",
              "cbm-selected-artifact"):
    if token not in portable_soak:
        failures.append(f"_soak.yml portable soak: missing selected tuple token {token}")

# The draft must not merge all workflow artifacts: that would accidentally
# publish rejected candidates. Download canonical release containers by their
# historical name, and preserve only the small selection evidence separately.
draft_body = blocks.get("release-draft", "")
if "pattern: binaries-*" not in draft_body:
    failures.append(
        "release-draft: canonical container download must be narrowed to binaries-*")
if "name: release-selection-evidence" not in draft_body:
    failures.append(
        "release-draft: selection/VT provenance must be downloaded separately")
if re.search(r"uses:\s*actions/download-artifact@[^\n]+\n\s+with:\s*\n\s+merge-multiple:\s*true",
             draft_body):
    failures.append(
        "release-draft: an unfiltered merge-multiple download could place rejected\n"
        "      candidate binaries in the public release staging directory.")

verify_body = blocks.get("verify", "")
if "needs.security.result == 'success'" not in verify_body:
    failures.append(
        "release verify: an explicitly skipped security dependency must block publication")
for token in ("--pattern 'release-selection.tsv'",
              "scripts/ci/verify-release-selection.py",
              '--selection "$RUNNER_TEMP/release-selection.tsv"',
              "--require-policy virustotal-v2",
              '--archive-dir "$ARCHIVE_DIR"'):
    if token not in verify_body:
        failures.append(
            f"release verify: final draft-byte selection binding is missing token: {token}")

# The post-package scan is REQUIRED, not forbidden. It is not a duplicate of the
# candidate scan: that one covers executables only, while this one covers every
# distinct object extracted from the 14 shipped containers - install.sh,
# install.ps1, LICENSE, THIRD_PARTY_NOTICES.md, the MCPB manifest.json and the
# unpacked UI assets. Those are the bytes users pipe into a shell, and dropping
# them would narrow the promise made in README.md and SECURITY.md.
#
# Re-submitting the selected executables alongside them is close to free:
# VirusTotal is content-addressed and returns the analysis it already holds for
# identical bytes.
for token in ("crazy-max/ghaction-virustotal", "scripts/ci/check-virustotal.sh",
              "scripts/ci/publish-vt-evidence.sh", "files: binaries/objects/*"):
    if token not in verify_body:
        failures.append(
            f"release verify: full-surface VirusTotal pass is missing: {token}")
for token in ("release-candidates.tsv", "virustotal-candidate-results.tsv",
              "release-selection.tsv", "scripts/ci/append-vt-notes.sh"):
    if token not in verify_body:
        failures.append(
            f"release verify: existing candidate evidence is not reused: {token}")

if failures:
    for f in failures:
        print("FAIL: " + f, file=sys.stderr)
    print(f"release gate-chain contract FAILED with {len(failures)} violation(s)",
          file=sys.stderr)
    sys.exit(1)
print("PASS: optional phases cannot silently disable the phases after them")
PY
