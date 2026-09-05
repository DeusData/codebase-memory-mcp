#!/usr/bin/env bash
# Venue-parity contract: "a venue may PROVISION a machine; only a CANONICAL leg
# script may EXERCISE the product."
#
# Why this exists: smoke ran on THREE harnesses until 2026-07-26 — local CI and
# PR CI shared the wrappers while _smoke.yml carried a hand-copied YAML
# reimplementation (no env sandbox, no user-PATH guard, fixed port); the soak
# sequence existed as six hand-synced YAML step pairs; the arm64 sanitizer
# flags lived inline in _test.yml where the local venue could not see them.
# Each drift was invisible until a release-only leg went red. This contract
# makes the drift itself the failure: every `run:` step in every VENUE workflow
# is walked line by line, and each line must classify as provisioning, artifact
# plumbing, or a call to a canonical leg entry — an unclassifiable line is a
# red build naming the file, line and remedy. It runs as Step 0j of
# scripts/test.sh, i.e. inside every venue it polices.
#
# Layers:
#   1. FORBIDDEN markers — hard failures in ANY workflow (inline fixture
#      serving, direct smoke-test.sh/soak-test.sh calls, inline ACL code).
#   2. Whitelist walk — every run-step line in the VENUE workflows classified.
#   3. REQUIRED markers — the unification itself must stay referenced.
#   4. Local venues — docker-compose services and the VM scripts must route
#      through the same canonical entries.
#   5. Interface probes — every entry must answer --help (exit 0, "Usage:")
#      and reject unknown flags with exit 2 + "Please consult --help." so an
#      agent can always discover exactly what a run will do.
#
# Usage: tests/test_venue_parity_contract.sh [repo-root]   (root override so
# the suite can prove the contract FAILS on a violation, not just passes;
# layer 5 runs only against the real repo root).

set -euo pipefail

ROOT="${1:-$(cd "$(dirname "$0")/.." && pwd)}"

python3 - "$ROOT" <<'PYEOF'
from __future__ import annotations

import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
workflows = root / ".github" / "workflows"

failures: list[str] = []

# ── The canonical leg entries (the ONLY product-exercising calls allowed) ──
CANONICAL = re.compile(
    r"scripts/(test|build|lint|clean|smoke-local|soak-legs|smoke-invariants|package-release)\.sh"
    r"|test-infrastructure/vm/vm-smoke\.sh"
    r"|scripts/ci/[a-z0-9-]+\.(sh|ps1|py)"
    r"|scripts/security-[a-z0-9-]+\.sh"
    r"|scripts/gen-third-party-notices\.sh"
    r"|scripts/check-no-test-skips\.sh"
    r"|scripts/check-lsp-originality\.sh"
    r"|scripts/test-windows\.ps1"
    r"|pkg/glama/verify\.sh"
)

# ── Layer 1: forbidden in ANY workflow (the known drift signatures) ──
FORBIDDEN = [
    (re.compile(r"http\.server"),
     "inline fixture server — smoke-local.sh / vm-smoke.sh own serving"),
    (re.compile(r"SMOKE_DOWNLOAD_URL|SMOKE_UPDATE_FIXTURE_DIR"),
     "fixture-server env is wrapper-owned; pass CBM_SMOKE_ARTIFACT_DIR instead"),
    (re.compile(r"scripts/smoke-test\.sh"),
     "smoke-test.sh must be reached through smoke-local.sh / vm-smoke.sh"),
    (re.compile(r"scripts/soak-test\.sh"),
     "soak-test.sh must be reached through scripts/soak-legs.sh"),
    (re.compile(r"scripts/run-tests-parallel\.sh"),
     "the parallel harness must be reached through scripts/test.sh"),
    (re.compile(r"AddAccessRule|SetAccessRuleProtection"),
     "inline ACL construction — call scripts/ci/new-protected-temp-root.ps1"),
]

# ── Layer 2: the whitelist walk over VENUE workflows ──
VENUE_WORKFLOWS = [
    "pr.yml", "dry-run.yml", "release.yml", "nightly-soak.yml",
    "_test.yml", "_smoke.yml", "_soak.yml", "_build.yml", "_lint.yml",
    "smoke.yml",
]

# Provisioning / plumbing commands a venue may run (first word of a line).
ALLOWED_CMDS = {
    # package + toolchain provisioning
    "sudo", "apt-get", "brew", "wget", "curl", "git", "cmake", "freshclam",
    "clamscan", "systemctl",
    # shell + env plumbing
    "echo", "printf", "export", "cd", "set", "source", "eval", "env",
    "if", "then", "else", "elif", "fi", "for", "while", "until", "do", "done",
    "case", "esac", "break", "continue", "return", "shift", "read", "local",
    "trap", "exit", "true", "ccache",
    # file/artifact plumbing
    "chmod", "mkdir", "cp", "mv", "rm", "tar", "unzip", "zip", "test",
    "[", "[[", "sha256sum", "shasum", "cygpath", "mktemp", "ls", "cat",
    "grep", "sed", "tee", "jq", "tr", "find", "file", "basename", "dirname",
    # CI plumbing
    "gh", "python3", "node", "codesign", "xcrun", "command", "awk",
}
# Per-file additions: _build.yml packages artifacts (make/strip are packaging
# steps, not test legs) and drives npm for the UI build; release.yml pushes finished artifacts to registries after
# every product-exercising gate already ran; the brew tap smoke installs the
# released formula.
ALLOWED_EXTRA = {
    "_build.yml": {"make", "npm", "strip", "install_name_tool", "otool", "ldd"},
    # The npm-wrapper failure-handling test is its own leg (package.json's test
    # script is its entry). RECORDED gap: it has no local-ladder equivalent.
    "_test.yml": {"npm"},
    "release.yml": {"npm", "pip", "pip3", "twine", "python", "make",
                    "./mcp-publisher"},
    "_smoke.yml": {"codebase-memory-mcp"},  # brew-installed CLI version check
}
# Steps that are scanners/data-generators with inherently inline bodies, keyed
# "<file>:<step name>". Additions here are DELIBERATE holes — keep tiny.
EXEMPT_STEPS = {
    "_smoke.yml:Windows Defender scan",   # AV cmdlet exit-code interpretation
    "_smoke.yml:Compute matrices",        # pure JSON matrix data + jq
    "_test.yml:Compute matrices",
    "_test.yml:Compute shard matrix",
}

def classify_line(line: str, allowed: set[str]) -> bool:
    s = line.strip()
    if not s or s.startswith("#"):
        return True
    # Continuation fragments, closers, pipes into plumbing, pure data.
    if s in {"|", "&&", "||", "}", "{", ")", "fi", "done", "esac", "then", "else"}:
        return True
    if s.startswith(("| ", "|| ", "&& ", ")")):
        return True
    first = s.split()[0]
    # Direct test-runner EXECUTION (first token) — building it as a make
    # target is packaging; running it bypasses scripts/test.sh.
    if re.match(r'^"?[\w./-]*test-runner(\.exe)?"?$', first):
        return False
    # VAR=... — a pure assignment, a command-substitution value (classify the
    # substituted command), or an env-prefixed command (classify the command).
    m = re.match(r"^[A-Za-z_][A-Za-z0-9_]*=(.*)$", s)
    if m:
        value = m.group(1).strip()
        if value.startswith("$("):
            s = value[2:].strip()
            if not s:
                return True
        else:
            tokens = value.split(None, 1)
            if len(tokens) == 2 and re.match(r"^[\w./-]+$", tokens[0]):
                s = tokens[1].strip()  # env-prefix form: VAR=x cmd ...
            else:
                return True            # plain assignment (any value shape)
        first = s.split()[0]
    if first in {"bash", "sh", "pwsh", "powershell", "powershell.exe", "&"}:
        # interpreter prefix: the thing being run must itself be canonical
        return bool(CANONICAL.search(s))
    if CANONICAL.search(s):
        return True
    # data fragments: quoted strings, JSON, heredoc bodies, YAML-ish keys
    if re.match(r"""^["'\[{$(-]""", s) or s.endswith(("\\", "'", '"')):
        return True
    return first in allowed

def walk_runs(path: pathlib.Path):
    """Yield (step_name, line_number, line) for every run:-block line.
    Indentation state machine over our own consistently-styled workflows —
    an unrecognized run: form is itself a contract failure. Heredoc bodies
    are opaque data (the OPENER line still gets classified)."""
    name = "?"
    lines = path.read_text().splitlines()
    i = 0
    while i < len(lines):
        raw = lines[i]
        m = re.match(r"^(\s*)- name:\s*(.+?)\s*$", raw)
        if m:
            name = m.group(2)
        m = re.match(r"^(\s*)run:\s*(.*)$", raw)
        if m:
            indent, rest = len(m.group(1)), m.group(2)
            if rest and rest not in {"|", "|-", ">-", ">"}:
                yield name, i + 1, rest          # single-line run
            elif rest in {">-", ">"}:
                failures.append(f"{path.name}:{i+1}: folded run: style — use `run: |`")
            else:                                 # block scalar
                i += 1
                heredoc = None
                while i < len(lines):
                    body = lines[i]
                    if heredoc is not None:
                        if body.strip() == heredoc:
                            heredoc = None
                        i += 1
                        continue
                    if body.strip() and (len(body) - len(body.lstrip())) <= indent:
                        i -= 1
                        break
                    hd = re.search(r"<<-?\s*['\"]?(\w+)['\"]?", body)
                    if hd:
                        heredoc = hd.group(1)
                    yield name, i + 1, body
                    i += 1
        i += 1

seen = 0
for path in sorted(workflows.glob("*.yml")):
    text = path.read_text()
    seen += 1
    for number, line in enumerate(text.splitlines(), 1):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        for pattern, why in FORBIDDEN:
            if pattern.search(stripped):
                failures.append(
                    f"{path.name}:{number}: forbidden `{pattern.pattern}` — {why}\n"
                    f"      {stripped}")

for wf in VENUE_WORKFLOWS:
    path = workflows / wf
    if not path.exists():
        failures.append(f"{wf}: venue workflow missing — update the contract "
                        f"if it was deliberately renamed/retired")
        continue
    allowed = ALLOWED_CMDS | ALLOWED_EXTRA.get(wf, set())
    continued = False
    for step, number, line in walk_runs(path):
        # A line following a backslash-continued line is that command's
        # argument tail, not a new command — plumbing by association.
        was_continued, continued = continued, line.rstrip().endswith("\\")
        if was_continued:
            continue
        if f"{wf}:{step}" in EXEMPT_STEPS:
            continue
        if not classify_line(line, allowed):
            failures.append(
                f"{wf}:{number}: unclassifiable in step '{step}' — a venue may "
                f"provision, plumb artifacts, or call a canonical leg script; "
                f"anything else belongs INSIDE a scripts/ entry\n"
                f"      {line.strip()}")

# ── Layer 3: the unification must stay referenced ──
REQUIRED = [
    ("_smoke.yml", r"vm-smoke\.sh", "the Windows smoke legs run the shared wrapper"),
    ("_smoke.yml", r"scripts/smoke-local\.sh", "the unix smoke legs run the shared wrapper"),
    ("_soak.yml", r"scripts/soak-legs\.sh", "the soak sequence lives in the canonical entry"),
    ("_soak.yml", r"scripts/ci/new-protected-temp-root\.ps1", "Windows soak uses the shared temp root"),
    ("_test.yml", r"scripts/ci/new-protected-temp-root\.ps1", "Windows tests use the shared temp root"),
    ("_test.yml", r"scripts/test\.sh", "the test legs run the canonical entry"),
    ("pr.yml", r"vm-smoke\.sh", "PR CI smokes through the shared wrapper"),
    ("_build.yml", r"scripts/package-release\.sh",
     "release archives are produced by the canonical packaging entry"),
]
for name, pattern, why in REQUIRED:
    path = workflows / name
    if path.exists() and not re.search(pattern, path.read_text()):
        failures.append(f"{name}: missing required `{pattern}` — {why}")

# Defender posture: hosted Windows runners have real-time protection
# POLICY-LOCKED OFF (verified 2026-07-27: WinDefend starts, Set-MpPreference
# accepts, RTP stays off), so runner jobs must NOT gate on enabling it; the
# LOCAL VM leg enforces Defender-ON via the same canonical script in its
# preflight (see LOCAL_REQUIRED below) — superset coverage where the platform
# allows it.
for name in ("_test.yml", "_soak.yml", "_smoke.yml", "pr.yml"):
    path = workflows / name
    if path.exists() and "ensure-defender.ps1" in path.read_text():
        failures.append(
            f"{name}: ensure-defender.ps1 must not gate hosted runners — RTP is "
            f"policy-locked off there; the VM preflight owns Defender-ON coverage")

# ── Layer 4: the local venues route through the same entries ──
compose = root / "test-infrastructure" / "docker-compose.yml"
if compose.exists():
    text = compose.read_text()
    for number, line in enumerate(text.splitlines(), 1):
        s = line.strip()
        if s.startswith("#"):
            continue
        if re.search(r"scripts/(smoke-test|soak-test)\.sh", s):
            failures.append(
                f"docker-compose.yml:{number}: internal harness called directly — "
                f"route through smoke-local.sh / soak-legs.sh\n      {s}")

for local in ["test-infrastructure/vm/win.sh",
              "test-infrastructure/vm/vm-run-tests.sh"]:
    path = root / local
    if not path.exists():
        continue
    text = path.read_text()
    for number, line in enumerate(text.splitlines(), 1):
        s = line.strip()
        if s.startswith("#"):
            continue
        if re.search(r"scripts/(smoke-test|soak-test)\.sh", s):
            failures.append(
                f"{local}:{number}: internal harness called directly — route "
                f"through the canonical entries\n      {s}")

# The parity lanes exist on the LOCAL venues too: artifact-flow smoke and the
# glibc-floor leg in compose/run.sh, Defender preflight in the VM driver.
LOCAL_REQUIRED = [
    ("test-infrastructure/docker-compose.yml", r"scripts/ci/smoke-artifact\.sh",
     "the local artifact-flow smoke lane exists"),
    ("test-infrastructure/docker-compose.yml", r"Dockerfile\.glibc22",
     "the glibc-floor venue exists"),
    ("test-infrastructure/run.sh", r"smoke-artifact",
     "run.sh exposes the artifact-flow smoke leg"),
    ("test-infrastructure/run.sh", r"glibc-floor",
     "run.sh exposes the glibc-floor leg"),
    # Slash-agnostic: the VM driver invokes it via a Windows path
    # (C:\cbm\scripts\ci\ensure-defender.ps1).
    ("test-infrastructure/vm/win.sh", r"ensure-defender\.ps1",
     "the VM preflight enforces Defender-ON parity"),
    ("test-infrastructure/vm/win.sh", r"scripts/ci/smoke-artifact\.sh",
     "the VM driver exposes the artifact-flow smoke lane"),
]
for local, pattern, why in LOCAL_REQUIRED:
    path = root / local
    if path.exists() and not re.search(pattern, path.read_text()):
        failures.append(f"{local}: missing required `{pattern}` — {why}")

# Native Windows process lookup searches system locations before PATH. A bare
# `bash` argv launched by embedded Python can therefore select the WSL alias
# instead of the MSYS2 Bash that is already running this contract. Every
# shell-hosted Python block must receive the current `$BASH` as an explicit
# argument and launch that resolved path. Enforce this textually on every
# maintained shell surface so the rule is deterministic on non-Windows hosts
# too, rather than relying on a runner to happen to have the WSL alias.
BARE_BASH_ARGV = re.compile(r"""[\[(]\s*["']bash["']\s*,""")
shell_surfaces = list(root.glob("*.sh"))
for directory in ("tests", "scripts", "test-infrastructure", "pkg"):
    base = root / directory
    if not base.exists():
        continue
    shell_surfaces.extend(base.rglob("*.sh"))
for path in sorted(shell_surfaces):
    text = path.read_text(encoding="utf-8")
    for match in BARE_BASH_ARGV.finditer(text):
        number = text.count("\n", 0, match.start()) + 1
        relative = path.relative_to(root).as_posix()
        failures.append(
            f"{relative}:{number}: embedded Python must launch the current "
            "$BASH by explicit argv path; bare `bash` is WSL-alias-sensitive "
            "under native Windows process lookup"
        )

# ── Release archives are extracted from an ISOLATED directory ──
# The release/dry-run verify jobs used to download archives into the tracked
# checkout, so the extractor scanned a directory that also held repo files.
# Both now stage into $RUNNER_TEMP. This assertion lived in the archive-extractor
# contract, which was deleted with the UI-pack architecture it tested; the fix
# itself is unrelated to packs, so its guard moves here rather than vanishing.
for name in ("release.yml", "dry-run.yml"):
    text = (workflows / name).read_text(encoding="utf-8")
    if "scripts/ci/extract-release-archives.sh" not in text:
        continue
    if "$RUNNER_TEMP/release-archives" not in text:
        failures.append(
            f"{name}: release archives must be downloaded into "
            "$RUNNER_TEMP/release-archives, never into the tracked checkout"
        )

if failures:
    print("VENUE PARITY CONTRACT VIOLATED — one harness, every venue:")
    for failure in failures:
        print(f"  {failure}")
    sys.exit(1)

print(f"venue-parity contract OK ({seen} workflows marker-checked, "
      f"{len(VENUE_WORKFLOWS)} venue workflows whitelist-walked, "
      f"{len(EXEMPT_STEPS)} deliberate step exemptions)")
PYEOF

# ── Layer 5: interface probes (real repo root only — the fake-root RED-proof
# trees carry no scripts). Every entry answers --help; the strict ones reject
# an unknown flag with exit 2 and the uniform "Please consult --help." line.
if [ -d "$ROOT/scripts" ] && [ -f "$ROOT/scripts/test.sh" ]; then
    probe_failures=0

    HELP_ENTRIES="
scripts/test.sh
scripts/build.sh
scripts/lint.sh
scripts/smoke-local.sh
scripts/soak-legs.sh
scripts/smoke-test.sh
scripts/soak-test.sh
scripts/smoke-invariants.sh
scripts/ci/preflight-docker.sh
scripts/ci/require-all-green.sh
scripts/ci/verify-shard-union.sh
scripts/ci/generate-sbom.py
scripts/package-release.sh
scripts/ci/smoke-artifact.sh
test-infrastructure/run.sh
test-infrastructure/vm/vm-smoke.sh
test-infrastructure/vm/vm-run-tests.sh
test-infrastructure/vm/win.sh
"
    for entry in $HELP_ENTRIES; do
        out=$(cd "$ROOT" && bash "$entry" --help 2>&1) && rc=0 || rc=$?
        case "$entry" in
        *.py) out=$(cd "$ROOT" && python3 "$entry" --help 2>&1) && rc=0 || rc=$? ;;
        esac
        # Substring test in-shell: piping into `grep -q` is unsafe under
        # `set -o pipefail` because grep exits at the first match and the
        # writer takes EPIPE on any payload larger than the pipe buffer,
        # turning a SATISFIED contract into a probe failure (macOS, 698-byte
        # --help vs a 512-byte PIPE_BUF).
        case "$out" in
        *"Usage:"*) has_usage=1 ;;
        *) has_usage=0 ;;
        esac
        if [ "$rc" -ne 0 ] || [ "$has_usage" -eq 0 ]; then
            echo "INTERFACE CONTRACT: $entry --help must exit 0 and print a Usage: block (rc=$rc)" >&2
            probe_failures=1
        fi
    done

    # Strict-flag probe: hermetic entries only (no external deps before their
    # argument validation). vm-run-tests takes free-form suite names and
    # win.sh/run.sh need infra before dispatch — their --help is probed above.
    STRICT_ENTRIES="
scripts/test.sh
scripts/build.sh
scripts/lint.sh
scripts/smoke-local.sh
scripts/soak-legs.sh
scripts/ci/preflight-docker.sh
test-infrastructure/vm/vm-smoke.sh
scripts/smoke-invariants.sh
"
    for entry in $STRICT_ENTRIES; do
        out=$(cd "$ROOT" && bash "$entry" --definitely-not-a-flag 2>&1) && rc=0 || rc=$?
        case "$out" in
        *"Please consult --help."*) has_hint=1 ;;
        *) has_hint=0 ;;
        esac
        if [ "$rc" -ne 2 ] || [ "$has_hint" -eq 0 ]; then
            echo "INTERFACE CONTRACT: $entry must reject unknown flags with exit 2 + 'Please consult --help.' (rc=$rc)" >&2
            probe_failures=1
        fi
    done

    if [ "$probe_failures" -ne 0 ]; then
        echo "VENUE PARITY CONTRACT VIOLATED — interface probes failed (layer 5)" >&2
        exit 1
    fi
    echo "interface probes OK ($(printf '%s' "$HELP_ENTRIES" | grep -c .) --help entries, $(printf '%s' "$STRICT_ENTRIES" | grep -c .) strict-flag entries)"
fi
