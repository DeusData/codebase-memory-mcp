#!/usr/bin/env bash
# INVERTED release-surface contract: Windows ships ONE binary.
#
# History this guards against. The Windows release used to be a PAIR — a small
# permanent launcher (codebase-memory-mcp.exe) plus the real product binary
# (codebase-memory-mcp.payload.exe). The launcher existed for exactly one
# reason: a running .exe cannot replace its own image on Windows, so an
# in-process self-update needs a second resident binary to do the swap.
#
# That stub is statically indistinguishable from a dropper — a small unsigned
# PE whose whole job is verify-and-execute another binary — and Defender's ML
# scored it Trojan:Win32/Wacatac.B!ml on x64 regardless of what we changed
# (bcrypt-free, stripped, VERSIONINFO'd and even resource-free builds were all
# flagged), while the product binary itself scans clean on every platform.
#
# The fix removed the stub and moved self-update OUT of the process into
# install.ps1, which runs while CBM is NOT running. So this file asserts the
# ABSENCE of the flagged design, plus the one step that replaces it:
# install.ps1 must RETIRE the running binary (rename it out of the way) before
# publishing the new one — losing that step silently breaks every Windows
# update.
#
# The non-launcher release/security assertions from the previous contract
# (VM-driver hardening, archive allowlists, HTTPS-only downloads, profile-rooted
# smoke fixtures, PR-smoke delegation) are preserved below.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

python3 - "$ROOT" <<'PY'
from __future__ import annotations

import pathlib
import re
import subprocess
import sys


root = pathlib.Path(sys.argv[1])
failures: list[str] = []


def read(relative: str) -> str:
    return (root / relative).read_text(encoding="utf-8")


def require(condition: bool, message: str) -> None:
    if not condition:
        failures.append(message)


def yaml_run_blocks(text: str) -> list[str]:
    """Return literal/folded YAML run blocks without requiring PyYAML."""
    lines = text.splitlines()
    blocks: list[str] = []
    index = 0
    while index < len(lines):
        match = re.match(r"^(\s*)run:\s*[|>]", lines[index])
        if match is None:
            index += 1
            continue
        base_indent = len(match.group(1))
        block: list[str] = []
        index += 1
        while index < len(lines):
            line = lines[index]
            if line.strip() and len(line) - len(line.lstrip()) <= base_indent:
                break
            block.append(line)
            index += 1
        blocks.append("\n".join(block))
    return blocks


binary = "codebase-memory-mcp.exe"
payload = "codebase-memory-mcp.payload.exe"
windows_archive_names = (binary, "LICENSE", "install.ps1", "THIRD_PARTY_NOTICES.md")

# ── 1. The archive is exactly four files, defined in ONE place ───────────────
# Every venue (release build, local artifact-flow smoke) produces archives
# through scripts/package-release.sh, so the layout is asserted where it is
# defined and cannot fork per venue.
package_release = read("scripts/package-release.sh")
zip_call = re.search(
    r'zip -q -X "\$ARCHIVE_TMP" \\\n\s+(?P<members>[^\n]+)', package_release
)
require(zip_call is not None, "package-release.sh must build the Windows zip in one zip call")
zip_members = zip_call.group("members").split() if zip_call else []
require(
    zip_members == list(windows_archive_names),
    "package-release.sh must archive EXACTLY "
    f"{' '.join(windows_archive_names)} (found: {' '.join(zip_members) or 'nothing'})",
)
require(
    "--selected-binary" in package_release
    and "--expected-sha256" in package_release
    and 'assert_expected_hash "$VERIFY_DIR/archive/$STAGED_BINARY_NAME"' in package_release,
    "package-release.sh must bind the selected input and archived executable to the same "
    "caller-supplied SHA-256",
)

# ── 2. No shipped surface may name the payload or build a launcher ───────────
# These are every file that decides what the Windows release contains or how a
# Windows install is assembled. A single reappearance of the payload name (or
# of a launcher build/copy) means the flagged two-binary design is back.
launcher_markers = (
    payload,
    "codebase-memory-mcp-launcher",
    "windows_launcher_state",
    "src/launcher/",
)
shipped_surfaces = (
    "scripts/package-release.sh",
    "install.ps1",
    "pkg/npm/install.js",
    "pkg/npm/bin.js",
    "pkg/pypi/src/codebase_memory_mcp/_cli.py",
    ".github/workflows/_build.yml",
)
for relative in shipped_surfaces:
    source = read(relative)
    for marker in launcher_markers:
        require(
            marker not in source,
            f"{relative} must not reference '{marker}': Windows ships ONE binary and the "
            "launcher stub is what Defender flagged as Trojan:Win32/Wacatac.B!ml",
        )

# The stub sources themselves must stay deleted, and the build must not carry a
# rule that could resurrect them.
for relative in (
    "src/launcher/windows_launcher.c",
    "src/launcher/windows_launcher.rc",
    "src/cli/windows_launcher_state.c",
    "src/cli/windows_launcher_state.h",
    "tests/test_windows_launcher_state.c",
):
    require(
        not (root / relative).exists(),
        f"{relative} must stay deleted — the Windows launcher stub is gone for good",
    )
makefile = read("Makefile.cbm")
require(
    "codebase-memory-mcp-launcher" not in makefile and "WINDOWS_LAUNCHER" not in makefile,
    "Makefile.cbm must not expose a Windows launcher target or variable",
)

# Every archive is produced through the ONE canonical packaging entry, so the
# four-file layout above governs all of them. Selection may choose stripped or
# unstripped only inside the immutable Windows architecture tuple; it must hand
# final bytes to the packager by hash, never revive a payload composition.
build_workflow = read(".github/workflows/_build.yml")
require(
    "scripts/package-release.sh" in build_workflow
    and "--selected-binary" in build_workflow
    and "--expected-sha256" in build_workflow,
    "_build.yml must hand hash-bound selected bytes to the canonical packager",
)
for target in ("windows-amd64", "windows-arm64"):
    require(
        target in build_workflow,
        f"_build.yml must retain the {target} release product",
    )

artifact_smoke = read("scripts/ci/smoke-artifact.sh")
require(
    "scripts/ci/prepare-release-candidates.sh" in artifact_smoke
    and '${GOOS}-${GOARCH}/stripped/$SELECTED_NAME' in artifact_smoke
    and "--selected-binary" in artifact_smoke
    and "--expected-sha256" in artifact_smoke,
    "local artifact smoke must derive both candidates, default-select stripped within its "
    "target tuple, and package the selected bytes by SHA-256",
)
require(
    'codesign --sign' not in artifact_smoke,
    "local artifact smoke must not sign after candidate derivation; packaging starts from "
    "already-final bytes",
)

# ── 3. install.ps1 retires the running binary before publishing ──────────────
# This is THE step that replaces the launcher. Windows keeps an image lock on a
# running .exe: it cannot be overwritten, but it CAN be renamed out of the way.
# install.ps1 runs while CBM is not running, renames the installed binary to
# "<dest>.retired-<timestamp>", then installs over the freed name. Without the
# retire step every Windows update fails on a locked destination.
installer = read("install.ps1")
# Windows PowerShell 5.1 decodes a BOM-less .ps1 as ANSI, so a UTF-8 em-dash
# arrives as three cp1252 characters ending in a double quote. Inside a string
# literal that quote closes it early and the whole script dies with a cascade of
# parse errors before its first statement runs. A BOM would fix the file on disk
# but corrupt the documented `irm ... | iex` path, which pipes the bytes
# straight into the parser -- so the shipped installer stays pure ASCII.
non_ascii = sorted({ch for ch in installer if ord(ch) > 0x7F})
require(
    not non_ascii,
    "install.ps1 must be pure ASCII (found: "
    + ", ".join(f"U+{ord(ch):04X}" for ch in non_ascii)
    + ")",
)
require(
    "$Dest = Join-Path $InstallDir $BinName" in installer,
    "install.ps1 must resolve the canonical install destination as $Dest",
)
retire_match = re.search(
    r"if \(Test-Path -LiteralPath \$Dest -PathType Leaf\) \{(?P<body>(?:.|\n)*?)\n\}\n",
    installer,
)
retire = retire_match.group("body") if retire_match else ""
require(
    retire_match is not None
    and re.search(r"\$retired\s*=\s*\"\$Dest\.retired-", retire) is not None
    and "Move-Item -LiteralPath $Dest -Destination $retired" in retire,
    "install.ps1 must RETIRE the running binary (Move-Item $Dest -> $retired) before "
    "publishing the new one — a running .exe cannot be overwritten in place",
)
require(
    retire_match is not None and "$renamed" in retire and "exit 1" in retire,
    "install.ps1 must fail loudly when the running binary cannot be retired",
)
publish_index = installer.find("& $DownloadedBinary @InstallArgs")
require(
    publish_index > (retire_match.start() if retire_match else -1) >= 0,
    "install.ps1 must retire BEFORE it publishes the downloaded binary",
)
require(
    "& $DownloadedBinary --version" in installer
    and "& $DownloadedBinary @InstallArgs" in installer,
    "install.ps1 must verify and install through the downloaded binary",
)

# PS 5.1 wraps redirected native stderr into ErrorRecords; under the global
# ErrorActionPreference=Stop a healthy binary that warns on stderr aborts the
# version probes (#1255). Pin the guard's two load-bearing parts so neither
# can be silently dropped: the probe-scoped relaxation with an exception-safe
# restore, and the $LASTEXITCODE pre-seed that keeps a start-failure from
# inheriting a stale 0 and reading as success.
require(
    installer.count('$ErrorActionPreference = "Continue"') == 2
    and installer.count("$global:LASTEXITCODE = 1") == 2
    and installer.count("$ErrorActionPreference = $ProbeEap") == 2,
    "install.ps1 version probes must relax EAP with restore and pre-seed LASTEXITCODE (#1255)",
)

# ── 4. Package-manager shims resolve the single Windows binary ───────────────
single_binary_contracts = {
    "pkg/npm/install.js": (
        r"const\s+WINDOWS_BINARY_NAME\s*=\s*['\"]codebase-memory-mcp\.exe['\"]",
        r"installWindowsBinaryAtomically\(",
        r"windowsBinaryReady\(",
    ),
    "pkg/npm/bin.js": (
        r"binName\s*=\s*isWindows\s*\?\s*['\"]codebase-memory-mcp\.exe['\"]",
        r"const\s+executionPath\s*=\s*binPath",
    ),
    "pkg/pypi/src/codebase_memory_mcp/_cli.py": (
        r"_WINDOWS_BINARY_NAME\s*=\s*['\"]codebase-memory-mcp\.exe['\"]",
        r"def\s+_runtime_set_ready\(",
    ),
}
for relative, patterns in single_binary_contracts.items():
    source = read(relative)
    require(
        all(re.search(pattern, source, re.DOTALL) for pattern in patterns),
        f"{relative} must resolve the single Windows binary",
    )
    require(
        ".cbm/generations" not in source and "current-v1" not in source,
        f"{relative} must remain portable and not own managed launcher state",
    )

# All package downloaders parse the Windows archive against the exact official
# four-root-file allowlist; they may not silently ignore an attacker-controlled
# fifth member.
exact_archive_guards = {
    "install.ps1": (
        "$seen.Count -ne $WindowsArchiveNames.Count",
        '"LICENSE"',
        '"install.ps1"',
        "THIRD_PARTY_NOTICES.md",
    ),
    "pkg/npm/install.js": (
        "$seen.Count -ne $requiredNames.Count",
        "WINDOWS_BINARY_NAME",
        "'LICENSE'",
        "'install.ps1'",
        "THIRD_PARTY_NOTICES.md",
    ),
    "pkg/pypi/src/codebase_memory_mcp/_cli.py": (
        "name not in required_set",
        "len(seen) != len(required)",
        "_WINDOWS_BINARY_NAME",
        '"LICENSE"',
        '"install.ps1"',
        "THIRD_PARTY_NOTICES.md",
    ),
}
for relative, needles in exact_archive_guards.items():
    source = read(relative)
    require(
        all(needle in source for needle in needles),
        f"{relative} must reject every Windows zip namespace except the official four-file "
        "allowlist",
    )

# A portable mutation refusal must point to the owning package manager.
guidance_contracts = {
    "pkg/npm/bin.js": (
        "npm install codebase-memory-mcp@latest",
        "npm uninstall codebase-memory-mcp",
        "codebase-memory-mcp install --yes",
    ),
    "pkg/pypi/src/codebase_memory_mcp/_cli.py": (
        "python -m pip install --upgrade codebase-memory-mcp",
        "python -m pip uninstall codebase-memory-mcp",
        "install --yes",
    ),
}
for relative, needles in guidance_contracts.items():
    source = read(relative)
    require(
        all(needle in source for needle in needles),
        f"{relative} must provide actionable package and managed-install guidance",
    )

# ── 5. Native Windows guard coverage of the replacement contract ─────────────
windows_test_driver = read("scripts/test-windows.ps1")
require(
    "tests\\windows\\test_windows_update_handoff.py" in windows_test_driver
    or "tests/windows/test_windows_update_handoff.py" in windows_test_driver,
    "scripts/test-windows.ps1 must run tests/windows/test_windows_update_handoff.py",
)
require(
    "test_windows_launcher.py" not in windows_test_driver
    and "test_cli_activation_helper.py" not in windows_test_driver,
    "scripts/test-windows.ps1 must not run the retired launcher guards",
)
require(
    'Copy-Item -LiteralPath $bin -Destination $guardBin' in windows_test_driver
    and "$guardPayload" not in windows_test_driver,
    "native Windows guards must stage exactly one executable",
)
require(
    windows_test_driver.count("| Out-Host") >= 1
    and windows_test_driver.count("$buildExit = $LASTEXITCODE") >= 1,
    "Windows build helpers must not leak compiler output into returned artifact paths",
)
require(
    '$code -eq 1 -or $t -eq "tests\\windows\\test_windows_update_handoff.py"'
    in windows_test_driver,
    "the update-handoff guard must fail instead of skip on driver/precondition errors",
)
require(
    all(
        needle in windows_test_driver
        for needle in (
            "[Environment+SpecialFolder]::UserProfile",
            '$guardRoot = Join-Path $userProfile '
            '("cbm-windows-guards-root-" + [guid]::NewGuid().ToString("N"))',
            '$env:TEMP = $guardRoot',
            '$env:TMP = $guardRoot',
            '$env:TMPDIR = $guardRoot',
            'Remove-Item -LiteralPath $guardRoot -Recurse -Force',
        )
    ),
    "Windows guards must keep staged and Python-created fixtures beneath the current "
    "account profile",
)
require(
    '$guardRoot = $null\ntry {\n    $userProfile = '
    '[Environment]::GetFolderPath([Environment+SpecialFolder]::UserProfile)'
    in windows_test_driver
    and 'if ($guardRoot) {\n        Remove-Item -LiteralPath $guardRoot -Recurse -Force'
    in windows_test_driver,
    "Windows guard setup must be covered by profile-fixture cleanup",
)

# The hosted runner profile can itself be trusted while newly-created children
# still inherit mutation-capable principals. Require the guard root to replace
# that inherited DACL with a protected, current-account-owned ACL before any
# staged executable or Python temporary descendant is created below it.
guard_root_creation = "New-Item -ItemType Directory -Path $guardRoot | Out-Null"
guard_bundle_creation = "$guardBundle = Join-Path $guardRoot "
acl_start = windows_test_driver.find(guard_root_creation)
acl_end = windows_test_driver.find(guard_bundle_creation, acl_start + 1)
guard_acl_setup = (
    windows_test_driver[acl_start:acl_end] if acl_start >= 0 and acl_end > acl_start else ""
)
require(
    all(
        needle in guard_acl_setup
        for needle in (
            "[System.Security.Principal.WindowsIdentity]::GetCurrent().User",
            "[System.Security.AccessControl.DirectorySecurity]::new()",
            "$guardAcl.SetOwner($currentSid)",
            "$guardAcl.SetAccessRuleProtection($true, $false)",
            "[System.Security.AccessControl.FileSystemRights]::FullControl",
            "[System.Security.AccessControl.InheritanceFlags]::ContainerInherit",
            "[System.Security.AccessControl.InheritanceFlags]::ObjectInherit",
            "[System.Security.AccessControl.PropagationFlags]::None",
            "[System.Security.AccessControl.AccessControlType]::Allow",
            "Set-Acl -LiteralPath $guardRoot -AclObject $guardAcl",
        )
    ),
    "Windows guards must protect the guard-root DACL and grant only the current account "
    "inheritable full control before creating descendants",
)

# The native guard must assert the REPLACEMENT contract, not merely exist.
update_guard = read("tests/windows/test_windows_update_handoff.py")
require(
    all(
        needle in update_guard
        for needle in (
            '"install.ps1" in lowered',
            "result.returncode == 0",
            "sha256_file(binary) == before",
            "codebase-memory-mcp.payload.exe",
        )
    ),
    "the native update guard must assert exit 0, the printed install.ps1 command, an "
    "unchanged own image, and the absence of a payload sibling",
)

# ── 6. Native path-tree trust boundary (unchanged, launcher-independent) ─────
require(
    all(
        needle in read("src/daemon/ipc.c")
        for needle in (
            "win_directory_component_secure",
            "win_file_security_secure(security, directory, false, mutation, true)",
            "win_private_mutation_rights()",
            "~((DWORD)FILE_ADD_SUBDIRECTORY)",
            "FILE_ADD_FILE",
            "FILE_DELETE_CHILD",
            "final runtime",
            "ACCESS_SYSTEM_SECURITY",
            "956008885U",
            "FILE_ATTRIBUTE_REPARSE_POINT",
        )
    ),
    "src/daemon/ipc.c must enforce the shared cross-account ancestor trust policy",
)

# ── 6b. AppContainer tolerance is ANCESTOR-ONLY ─────────────────────────────
# Package (S-1-15-2-*) and capability (S-1-15-3-*) SIDs are admitted on ancestor
# components so a machine running a sandboxed desktop app (Claude Desktop stamps
# its package SID on %LOCALAPPDATA%) is not locked out. The private runtime
# directory must keep demanding the exact current user: it is passed
# ancestor=false and that is the whole boundary.
require(
    all(
        needle in read("src/daemon/ipc.c")
        for needle in (
            "win_sid_is_app_container",
            "ancestor && win_sid_is_app_container",
            # both AppContainer forms, not capability alone
            "first == 2U || first == 3U",
            # APP_PACKAGE identifier authority
            "sid[7] != 15U",
            # the private runtime directory is validated with ancestor=false
            "win_private_mutation_rights(), false)",
        )
    ),
    "AppContainer SIDs must be tolerated on ancestors ONLY, covering both package "
    "and capability forms, with the private runtime directory still strict",
)

# On Windows subprocess supervision receives a non-NULL lpApplicationName, so a
# literal `git` would not use PATH. Resolve only git.exe beneath inherited
# absolute PATH entries and never permit the current-directory search implied by
# empty or relative entries. POSIX retains execvp via argv[0].
watcher_source = read("src/watcher/watcher.c")
require(
    all(
        needle in watcher_source
        for needle in (
            "watcher_resolve_git_executable",
            'GetEnvironmentVariableW(L"PATH"',
            'L"%ls\\\\git.exe"',
            "GetFullPathNameW",
            "watcher_windows_path_absolute",
            "FILE_FLAG_OPEN_REPARSE_POINT",
            ".bin = git_executable",
            ".bin = argv[0]",
            "empty/relative entries",
        )
    )
    and "popen(" not in watcher_source,
    "Windows watcher Git commands must resolve an explicit absolute git.exe without cwd "
    "search while POSIX retains literal argv supervision",
)

# ── 7. Real-Windows local-CI drivers ─────────────────────────────────────────
vm_host_scripts = (
    "test-infrastructure/vm/provision-windows.sh",
    "test-infrastructure/vm/vm-smoke.sh",
    "test-infrastructure/vm/win.sh",
)
for relative in vm_host_scripts:
    indexed = subprocess.run(
        ["git", "-C", str(root), "ls-files", "--stage", "--", relative],
        check=False,
        capture_output=True,
        text=True,
    )
    indexed_mode = (
        indexed.stdout.split(maxsplit=1)[0] if indexed.returncode == 0 and indexed.stdout else ""
    )
    require(
        indexed_mode == "100755"
        if indexed_mode
        else (root / relative).stat().st_mode & 0o111 != 0,
        f"{relative} must be executable as documented",
    )

vm_driver = read("test-infrastructure/vm/win.sh")
vm_provision = read("test-infrastructure/vm/provision-windows.sh")
vm_common = read("test-infrastructure/vm/ssh-common.sh")
require(
    "JOBS='$(nproc)'" in vm_driver,
    "win.sh must defer nproc expansion to the remote MSYS shell without over-escaping it",
)
for relative, source in (
    ("test-infrastructure/vm/win.sh", vm_driver),
    ("test-infrastructure/vm/provision-windows.sh", vm_provision),
):
    require(
        "StrictHostKeyChecking=no" not in source
        and "UserKnownHostsFile=/dev/null" not in source,
        f"{relative} must not disable SSH server identity verification",
    )
    require(
        "CBM_VM_HOST_KEY_SHA256" in source,
        f"{relative} must require the pinned VM SSH host-key fingerprint",
    )
require(
    "msys2-x86_64-latest" not in vm_provision
    and "msys2-base-x86_64-20260611.sfx.exe" in vm_provision
    and "c105946e64e08f099ac0e4647461ce762b95333ad211777666476a9a41451d65" in vm_provision,
    "provision-windows.sh must pin the official MSYS2 image and SHA-256 digest",
)
require(
    "pacman -Syu --noconfirm --noprogressbar\" || true" not in vm_provision,
    "provision-windows.sh must fail rather than hide an incomplete MSYS2 upgrade",
)
require(
    "feat/shared-coordination-daemon" not in vm_driver
    and "feat/shared-coordination-daemon" not in vm_provision,
    "Windows VM drivers must not default permanently to the feature branch",
)
require(
    "mac-vm)" not in read("test-infrastructure/run.sh")
    and "CBM_WIN_VM_SSH" not in read("test-infrastructure/run.sh"),
    "run.sh must not retain duplicate mutable VM drivers outside vm/win.sh",
)

vm_bootstrap = read("test-infrastructure/vm/windows-bootstrap.ps1")
require(
    re.search(r"ssh-(?:ed25519|rsa)\s+[A-Za-z0-9+/]{40,}={0,3}", vm_bootstrap) is None,
    "windows-bootstrap.ps1 must never embed an administrator-authorized SSH key",
)
require(
    "SshPublicKeyPath" in vm_bootstrap,
    "windows-bootstrap.ps1 must require an explicit caller-supplied SSH public key file",
)
smoke_case = re.search(r"^smoke-install\)\n(?P<body>.*?)^\s*;;", vm_driver, re.MULTILINE | re.DOTALL)
require(smoke_case is not None, "win.sh must expose the smoke-install command")
require(
    smoke_case is not None
    and "bash test-infrastructure/vm/vm-smoke.sh" in smoke_case.group("body"),
    "win.sh smoke-install must run the isolated CI-equivalent vm-smoke harness",
)
sync_case = re.search(r"^sync\)\n(?P<body>.*?)^\s*;;", vm_driver, re.MULTILINE | re.DOTALL)
require(sync_case is not None, "win.sh must expose an exact local-worktree sync command")
require(
    sync_case is not None
    and "cbm_vm_write_untracked_manifest" in sync_case.group("body")
    and 'git -C "$ROOT" diff --binary' in sync_case.group("body")
    and "git reset --hard" in sync_case.group("body")
    and "git clean -fdx" in sync_case.group("body")
    and 'exec "$0" build' in sync_case.group("body")
    and "ls-files" in vm_common
    and '"$link"/*' in vm_common
    and "-e build" not in sync_case.group("body"),
    "win.sh sync must apply the binary Git diff plus untracked files, invalidate stale build "
    "outputs, and rebuild automatically",
)
require(
    sync_case is not None
    and "COPYFILE_DISABLE=1" in sync_case.group("body")
    and "--no-xattrs" in sync_case.group("body")
    and "--no-mac-metadata" in sync_case.group("body"),
    "win.sh sync must suppress macOS metadata instead of creating Windows AppleDouble files",
)
require(
    sync_case is not None
    # Assert the PROPERTY, not one spelling of it: capture the remote HEAD into
    # a local variable and compare it here. The checkout path became a variable
    # (per-run isolation), so pinning the literal `/c/cbm` was asserting the
    # implementation rather than the contract it exists to protect.
    and re.search(
        r'remote_head="\$\(vm clangarm64 "cd \S+ && git rev-parse --verify HEAD"\)"',
        sync_case.group("body"),
    )
    and 'test \\"\\$(git rev-parse --verify HEAD)\\"' not in sync_case.group("body"),
    "win.sh sync must compare the remote HEAD locally instead of nesting shell quotes through "
    "cmd.exe",
)

# ── 8. Release smoke stays profile-rooted and single-binary ──────────────────
smoke_workflow = read(".github/workflows/_smoke.yml")
windows_match = re.search(
    r"(?ms)^  smoke-windows:\s*(.*?)(?=^  [A-Za-z0-9_-]+:\s*$|\Z)", smoke_workflow
)
windows_smoke = windows_match.group(1) if windows_match else ""
require(bool(windows_smoke), "_smoke.yml must contain the smoke-windows job")
vm_smoke = read("test-infrastructure/vm/vm-smoke.sh")
smoke_local = read("scripts/smoke-local.sh")
require(
    'scripts/smoke-test.sh "$SMOKE_DIR/codebase-memory-mcp.exe"' in vm_smoke,
    f"Windows smoke wrapper must execute the canonical {binary}",
)
require(
    "bash test-infrastructure/vm/vm-smoke.sh" in windows_smoke
    and 'CBM_SMOKE_ARTIFACT_DIR="$(cygpath -u "$RUNNER_TEMP")/cbm-artifact"' in windows_smoke,
    "Windows release smoke must call the canonical wrapper on the extracted artifact",
)
require(
    f'test ! -e "$ARTIFACT_DIR/{payload}"' in windows_smoke,
    "Windows release smoke must assert the published archive has no payload sibling",
)
require(
    all(
        needle in vm_smoke
        for needle in (
            'PROFILE_ROOT="$(cygpath -u "$USERPROFILE")"',
            'SMOKE_DIR="$(mktemp -d "$PROFILE_ROOT/cbm-vm-smoke.XXXXXX")"',
            'cp "$BINARY_SRC" "$SMOKE_DIR/codebase-memory-mcp.exe"',
            'CBM_CACHE_DIR="$(cygpath -m "$SMOKE_DIR/cache")"',
            'SMOKE_TEMP_ROOT="$SMOKE_DIR"',
        )
    ),
    "Windows smoke wrapper must keep every fixture beneath the current account profile",
)
smoke_blocks = yaml_run_blocks(windows_smoke)
windows_release_version_blocks = [
    re.sub(r"\s+", " ", re.sub(r"\\\s*\n\s*", " ", block)).strip()
    for block in smoke_blocks
    if 'LAUNCH_DIR="$(mktemp -d "$PROFILE_ROOT/cbm-release-version.XXXXXX")"' in block
]
require(
    len(windows_release_version_blocks) == 1
    and all(
        needle in windows_release_version_blocks[0]
        for needle in (
            'PROFILE_ROOT="$(cygpath -u "$USERPROFILE")"',
            'cp "$ARTIFACT_DIR/codebase-memory-mcp.exe" "$LAUNCH_DIR/"',
            '"$LAUNCH_DIR/codebase-memory-mcp.exe" --version',
        )
    ),
    "Windows release version checks must execute the single binary beneath the current "
    "account profile",
)
# RUNNER_TEMP is legitimate ONLY for artifact provisioning/scanning; every
# executable fixture and execution must stay beneath the account profile.
runner_temp_allowed = (
    re.compile(r'ARTIFACT_DIR="\$\(cygpath -u "\$RUNNER_TEMP"\)/cbm-artifact"'),
    re.compile(r'Join-Path \$env:RUNNER_TEMP "cbm-artifact"'),
)
runner_temp_lines = [line.strip() for line in windows_smoke.splitlines() if "RUNNER_TEMP" in line]
require(
    bool(runner_temp_lines)
    and all(
        any(pattern.search(line) for pattern in runner_temp_allowed) for line in runner_temp_lines
    )
    and 'mktemp -d "$RUNNER_TEMP' not in windows_smoke
    and re.search(r'"\$RUNNER_TEMP[^"\n]*\.exe"', windows_smoke) is None,
    "Windows release smoke may use RUNNER_TEMP only to provision/scan the artifact, never as "
    "an execution root",
)
windows_release_security_blocks = [
    re.sub(r"\s+", " ", re.sub(r"\\\s*\n\s*", " ", block)).strip()
    for block in smoke_blocks
    if 'scripts/security-install.sh "$SECURITY_DIR/codebase-memory-mcp.exe"' in block
]
require(
    len(windows_release_security_blocks) == 1
    and all(
        needle in windows_release_security_blocks[0]
        for needle in (
            'PROFILE_ROOT="$(cygpath -u "$USERPROFILE")"',
            'SECURITY_DIR="$(mktemp -d "$PROFILE_ROOT/cbm-release-security.XXXXXX")"',
            'cp "$ARTIFACT_DIR/codebase-memory-mcp.exe" "$SECURITY_DIR/"',
            'TMPDIR="$SECURITY_DIR" '
            'scripts/security-install.sh "$SECURITY_DIR/codebase-memory-mcp.exe"',
        )
    ),
    "Windows release install audit must execute the single binary beneath the current "
    "account profile",
)

# ── 9. Update transport stays HTTPS-only in production ───────────────────────
smoke_script = read("scripts/smoke-test.sh")
require(
    'copy_smoke_binary "$FAKE_HOME/.local/bin/codebase-memory-mcp.exe"' not in smoke_script
    and 'copy_smoke_binary "$UPDATE_HOME/.local/bin/codebase-memory-mcp.exe"' not in smoke_script,
    "Windows smoke must leave canonical targets absent for an authenticated install",
)
require(
    "smoke_mktemp_file" in smoke_script
    and "smoke_mktemp_dir" in smoke_script
    and re.search(r"\$\(\s*mktemp(?:\s+-d)?(?:\s|\))", smoke_script) is None,
    "smoke-test.sh must route every temporary fixture through its private-root helpers",
)
require(
    "SMOKE_UPDATE_FIXTURE_DIR" in smoke_script
    and 'UPDATE_DOWNLOAD_URL="file://$UPDATE_FIXTURE_DIR"' in smoke_script
    and 'UPDATE_DOWNLOAD_URL="file:///$UPDATE_FIXTURE_DIR"' in smoke_script
    and 'CBM_DOWNLOAD_URL="$UPDATE_DOWNLOAD_URL"' in smoke_script,
    "Phase 14 native update must use an explicit file:// fixture override",
)
# The handoff contract is no longer Windows-specific: no platform replaces its
# own image in process, so Phase 14 asserts one platform-neutral contract and
# selects the script name via UPDATE_SCRIPT. Windows still has the strictest
# reason for it -- regressing here means reintroducing the launcher stub.
require(
    "FAIL 14a: update replaced the binary in-process" in smoke_script
    and 'grep -q "$UPDATE_SCRIPT" "$UPDATE_LOG"' in smoke_script
    and 'UPDATE_SCRIPT="install.ps1"' in smoke_script,
    "Phase 14 must assert the update handoff instead of an in-process replacement",
)
require(
    'HOME="$WIN_HOME" TEMP="$WIN_HOME" TMP="$WIN_HOME"' in smoke_script
    and "MSYS2_ARG_CONV_EXCL='*'" in smoke_script
    and "powershell.exe -NoProfile -ExecutionPolicy Bypass -File" in smoke_script
    and '"$WIN_SCRIPT" "--dir=$WIN_DIR"' in smoke_script
    and "& $args[1]" not in smoke_script,
    "Windows install.ps1 smoke must pass native HOME/TEMP/TMP and execute the script directly",
)
require(
    'CBM_DOWNLOAD_URL="$SMOKE_DOWNLOAD_URL"' in smoke_script
    and '"$SMOKE_DOWNLOAD_URL/$DL_ARCHIVE"' in smoke_script,
    "installer and raw download smoke phases must retain loopback HTTP coverage",
)
require(
    'SMOKE_UPDATE_FIXTURE_DIR="$FIXTURE_DIR"' in vm_smoke
    and 'SMOKE_UPDATE_FIXTURE_DIR="$FIXTURE_DIR"' in smoke_local
    and "scripts/smoke-local.sh" in smoke_workflow
    and smoke_workflow.count("CBM_SMOKE_ARTIFACT_DIR") >= 2,
    "Unix and Windows release smoke must identify their local update fixture via the "
    "canonical wrappers",
)

cli_source = read("src/cli/cli.c")
probe_start = cli_source.find("cbm_json_mcp_probe_windows_command_path(")
probe_end = cli_source.find("#endif", probe_start)
safe_command_probe = (
    cli_source[probe_start:probe_end] if probe_start >= 0 and probe_end > probe_start else ""
)
require(
    "HANDLE *component_handles" in safe_command_probe
    and "component_handle_count" in safe_command_probe
    and "FILE_SHARE_WRITE" not in safe_command_probe
    and "FILE_SHARE_DELETE" not in safe_command_probe
    and "CloseHandle(component_handles[handle_index])" in safe_command_probe,
    "Windows stale-command probing must retain validated ancestor handles and deny "
    "write/delete sharing until the complete local path has been classified",
)
file_override_match = re.search(
    r"static bool cli_download_is_explicit_file_override\(.*?\n}\n", cli_source, re.DOTALL
)
protocol_match = re.search(
    r"static const char \*cli_download_protocol\(.*?\n}\n", cli_source, re.DOTALL
)
file_override = file_override_match.group(0) if file_override_match else ""
protocol = protocol_match.group(0) if protocol_match else ""
require(
    re.search(r'cbm_safe_getenv\s*\(\s*"CBM_DOWNLOAD_URL"', file_override) is not None
    and 'strncmp(override, "file://", 7)' in file_override
    and "strncmp(url, override, override_length)" in file_override,
    "file:// downloads must remain restricted to the explicit test override",
)
require(
    'strncmp(url, "https://", 8)' in protocol
    and 'return "=https"' in protocol
    and "cli_download_is_explicit_file_override(url)" in protocol
    and 'return "=file"' in protocol
    and '"http://"' not in protocol,
    "production native downloads must remain HTTPS-only",
)
download_helpers = cli_source[
    cli_source.find("static int cbm_download_to_file(") : cli_source.find("/* ── macOS ad-hoc signing")
]
require(
    download_helpers.count('"--proto"') >= 2 and download_helpers.count('"--proto-redir"') >= 2,
    "native curl invocations must pin both initial and redirected protocols",
)

# The Windows `update` command must hand off to install.ps1 and must NOT carry
# an in-process self-update path (which is what required the launcher stub).
update_start = cli_source.find("int cbm_cmd_update(int argc, char **argv) {")
update_end = cli_source.find("\n/* ── ", update_start)
update_windows_block = (
    cli_source[update_start : update_end if update_end > update_start else len(cli_source)]
)
require(
    update_start >= 0
    and "install.ps1" in update_windows_block
    and "powershell -File" in update_windows_block,
    "cbm_cmd_update must print the install.ps1 command on Windows",
)
# The printed command must NOT carry an execution-policy override. That is a
# canonical malicious-loader pattern, and emitting it as a string literal put
# the signature inside every Windows artifact we ship — to save the user one
# documented step. The hand-off above is the property this contract cares
# about; the bypass flag was only ever the literal form it happened to take.
# Unblock-File covers the common case and the README covers the rest.
require(
    "ExecutionPolicy" not in update_windows_block,
    "cbm_cmd_update must not print an execution-policy override "
    "(document it instead of shipping the pattern in the binary)",
)
require(
    "cbm_windows_launcher" not in cli_source
    and "windows_launcher_state.h" not in cli_source,
    "src/cli/cli.c must not retain any launcher-state API usage",
)

# ── 10. PR smoke delegates to the maintained native harness ──────────────────
pr_workflow = read(".github/workflows/pr.yml")
pr_smoke_match = re.search(r"(?ms)^  pr-smoke:\s*(.*?)(?=^  [A-Za-z0-9_-]+:\s*$|\Z)", pr_workflow)
pr_smoke = pr_smoke_match.group(1) if pr_smoke_match else ""
require(bool(pr_smoke), "pr.yml must contain the pr-smoke job")
pr_windows_blocks = [
    block for block in yaml_run_blocks(pr_smoke) if "scripts/build.sh CC=clang CXX=clang++" in block
]
require(
    len(pr_windows_blocks) == 1,
    "PR smoke must contain exactly one Windows production build run block",
)
if pr_windows_blocks:
    pr_windows_block = re.sub(r"\\\s*\n\s*", " ", pr_windows_blocks[0])
    pr_windows_block = re.sub(r"\s+", " ", pr_windows_block).strip()
    require(
        "SMOKE_ARCH=amd64 bash test-infrastructure/vm/vm-smoke.sh" in pr_windows_block,
        "Windows PR smoke must invoke the maintained native harness with the authoritative "
        "amd64 artifact architecture",
    )
    require(
        pr_windows_block.find("scripts/build.sh CC=clang CXX=clang++")
        < pr_windows_block.find("SMOKE_ARCH=amd64 bash test-infrastructure/vm/vm-smoke.sh"),
        "Windows PR smoke must build before invoking the maintained harness",
    )
    require(
        "scripts/smoke-test.sh" not in pr_windows_block and payload not in pr_windows_block,
        "Windows PR workflow must not duplicate vm-smoke staging or smoke-test logic",
    )
    require(
        "$RUNNER_TEMP" not in pr_windows_block,
        "Windows PR smoke must not treat GitHub's shared RUNNER_TEMP ancestry as private",
    )

if failures:
    print("Windows single-binary bundle contract FAILED:", file=sys.stderr)
    for failure in failures:
        print(f"  - {failure}", file=sys.stderr)
    raise SystemExit(1)

print("Windows single-binary bundle contract passed")
PY
