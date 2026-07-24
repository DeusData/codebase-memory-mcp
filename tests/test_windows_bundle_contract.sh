#!/usr/bin/env bash
# Static release-surface contract for the Windows permanent launcher.
#
# This intentionally inspects the checked-in packaging sources.  Native
# launcher behavior is covered separately on Windows; this guard makes it hard
# for a release/package edit to silently put the managed launcher where a
# portable wrapper expects the payload (or omit one half of the release pair).

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


# The real-Windows local-CI drivers are documented as direct host entrypoints.
# Keep their Git checkout modes executable, and ensure the daily smoke command
# uses the isolated CI-equivalent harness rather than mutating the VM user's
# actual managed installation.
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
    indexed_mode = indexed.stdout.split(maxsplit=1)[0] if indexed.returncode == 0 and indexed.stdout else ""
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
    and "c105946e64e08f099ac0e4647461ce762b95333ad211777666476a9a41451d65"
    in vm_provision,
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

local_ci_driver = read("test-infrastructure/run.sh")
require(
    "mac-vm)" not in local_ci_driver and "CBM_WIN_VM_SSH" not in local_ci_driver,
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
    and "git -C \"$ROOT\" diff --binary" in sync_case.group("body")
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
    and 'remote_head="$(vm clangarm64 "cd /c/cbm && git rev-parse --verify HEAD")"'
    in sync_case.group("body")
    and 'test \\"\\$(git rev-parse --verify HEAD)\\"' not in sync_case.group("body"),
    "win.sh sync must compare the remote HEAD locally instead of nesting shell quotes through cmd.exe",
)


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


launcher = "codebase-memory-mcp.exe"
payload = "codebase-memory-mcp.payload.exe"
windows_archive_names = (
    launcher,
    payload,
    "LICENSE",
    "install.ps1",
    "THIRD_PARTY_NOTICES.md",
)

# Every Windows archive (standard/UI, x64/ARM64) is a five-file bundle containing
# the executable pair plus the three release/legal files. Check the producing run
# block itself so an attestation or comment cannot accidentally satisfy it.
build_workflow = read(".github/workflows/_build.yml")
build_blocks = yaml_run_blocks(build_workflow)
archives = (
    "codebase-memory-mcp-windows-amd64.zip",
    "codebase-memory-mcp-ui-windows-amd64.zip",
    "codebase-memory-mcp-windows-arm64.zip",
    "codebase-memory-mcp-ui-windows-arm64.zip",
)
for archive in archives:
    archive_blocks = [block for block in build_blocks if archive in block]
    require(bool(archive_blocks), f"_build.yml has no run block producing {archive}")
    require(
        any(all(name in block for name in windows_archive_names) for block in archive_blocks),
        f"{archive} must archive the exact official five-file Windows bundle",
    )

# install.ps1 must preserve the complete adjacent pair and enter the native
# process through the downloaded launcher. The launcher owns containment and
# resolves the payload; invoking the payload directly bypasses that boundary.
installer = read("install.ps1")
require(
    "$DownloadedLauncher = Join-Path $TmpDir $BinName" in installer
    and "$DownloadedPayload = Join-Path $TmpDir $PayloadName" in installer
    and "Test-Path -LiteralPath $DownloadedLauncher -PathType Leaf" in installer
    and "Test-Path -LiteralPath $DownloadedPayload -PathType Leaf" in installer,
    "install.ps1 must retain an adjacent downloaded launcher/payload pair",
)
require(
    "& $DownloadedLauncher --version" in installer
    and "& $DownloadedLauncher @InstallArgs" in installer
    and "& $DownloadedPayload @InstallArgs" not in installer,
    "install.ps1 must verify and install through the downloaded launcher",
)

# Package-manager shims are intentionally one-shot portable instances on
# Windows. Their downloaders validate both release executables adjacently in a
# private temporary directory, cache the immutable pair for a later managed
# install transition, and execute the launcher. They must not create or
# interpret managed generation state themselves.
portable_cache_contracts = {
    "pkg/npm/install.js": (
        r"const\s+WINDOWS_PAYLOAD_NAME\s*=\s*['\"]codebase-memory-mcp\.payload\.exe['\"]",
        r"binName\s*=\s*platform\s*===\s*['\"]windows['\"]\s*\?\s*WINDOWS_PAYLOAD_NAME",
    ),
    "pkg/npm/bin.js": (
        r"binName\s*=\s*isWindows\s*\?\s*['\"]codebase-memory-mcp\.payload\.exe['\"]",
    ),
    "pkg/pypi/src/codebase_memory_mcp/_cli.py": (
        r"_WINDOWS_PAYLOAD_NAME\s*=\s*['\"]codebase-memory-mcp\.payload\.exe['\"]",
        r"def\s+_bin_path[\s\S]{0,500}_WINDOWS_PAYLOAD_NAME\s+if\s+sys\.platform\s*==\s*['\"]win32['\"]",
    ),
    "pkg/go/cmd/codebase-memory-mcp/main.go": (
        r"windowsPayloadName\s*=\s*['\"]codebase-memory-mcp\.payload\.exe['\"]",
        r"if\s+runtime\.GOOS\s*==\s*['\"]windows['\"][\s\S]{0,300}name\s*=\s*windowsPayloadName",
    ),
}
for relative, patterns in portable_cache_contracts.items():
    source = read(relative)
    require(
        payload in source
        and all(re.search(pattern, source, re.DOTALL) for pattern in patterns),
        f"{relative} must retain {payload} as the Windows cache readiness path",
    )
    require(
        ".cbm/generations" not in source and "current-v1" not in source,
        f"{relative} must remain portable and not own managed launcher state",
    )

# Portable package shims keep an immutable, exact-version launcher/payload pair.
# The launcher resolves its adjacent payload for every native command.
# Concurrent first-run contenders publish launcher first and payload last;
# payload is therefore the readiness signal. npm and Go serialize pair repair,
# preserve any complete runnable winner, and roll back only bytes proven to
# belong to their own transaction. PyPI uses identical-byte collision checks.
immutable_pair_cache_contracts = {
    "pkg/npm/install.js": (
        "const cacheNames = platform === 'windows'",
        "const lock = acquireWindowsPairLock(destDir)",
        "publishedDigests.set(name, stagedDigests.get(name))",
        "if (digest && pathMatchesDigest(target, digest))",
    ),
    "pkg/pypi/src/codebase_memory_mcp/_cli.py": (
        "cache_names = extraction_names",
        "publish_names = (",
        "_files_equal_sha256",
    ),
    "pkg/go/cmd/codebase-memory-mcp/main.go": (
        "installWindowsPairAtomically(tmp, filepath.Dir(dest))",
        "lock, err := acquireWindowsPairLock(dstDir)",
        "publishedDigests[name] = stagedDigests[name]",
        "if ok && pathMatchesSHA256(target, digest)",
    ),
}
for relative, needles in immutable_pair_cache_contracts.items():
    source = read(relative)
    require(
        all(needle in source for needle in needles),
        f"{relative} must publish an immutable exact-version Windows pair with {payload} last",
    )
    require(
        "publishedPaths" not in source
        and "published_paths" not in source
        and "const published = []" not in source,
        f"{relative} must not roll back published destinations by pathname",
    )

npm_installer = read("pkg/npm/install.js")
require(
    "for (const name of [WINDOWS_LAUNCHER_NAME, WINDOWS_PAYLOAD_NAME])" in npm_installer
    and "windowsPairReady(destDir, verifier)" in npm_installer
    and "publishedDigests.set(name, stagedDigests.get(name))" in npm_installer
    and "if (digest && pathMatchesDigest(target, digest))" in npm_installer,
    "npm must publish launcher then payload, preserve a coherent winner, and "
    "roll back only transaction-owned bytes",
)
python_wrapper = read("pkg/pypi/src/codebase_memory_mcp/_cli.py")
require(
    re.search(
        r"publish_names = \(\s*\(_WINDOWS_LAUNCHER_NAME, "
        r"_WINDOWS_PAYLOAD_NAME\)",
        python_wrapper,
    ) is not None
    and "_files_equal_sha256(staged_paths[name], target)" in python_wrapper,
    "PyPI must publish launcher then payload and only accept an identical collision",
)
go_wrapper = read("pkg/go/cmd/codebase-memory-mcp/main.go")
require(
    "names := []string{windowsLauncherName, windowsPayloadName}" in go_wrapper
    and "windowsPairReady(dstDir, verifier)" in go_wrapper
    and "publishedDigests[name] = stagedDigests[name]" in go_wrapper
    and "if ok && pathMatchesSHA256(target, digest)" in go_wrapper,
    "Go must publish launcher then payload, preserve a coherent winner, and "
    "roll back only transaction-owned bytes",
)

npm_shim = read("pkg/npm/bin.js")
require(
    "launcherPath" in npm_shim
    and "windowsLauncherName" in npm_shim
    and "fs.existsSync(launcherPath)" in npm_shim,
    "pkg/npm/bin.js readiness must require the adjacent launcher/payload pair",
)
require(
    "const executionPath = isWindows ? launcherPath : binPath" in npm_shim
    and "spawnSync(executionPath, process.argv.slice(2)" in npm_shim
    and "spawnSync(binPath, process.argv.slice(2)" not in npm_shim,
    "npm must execute Windows commands through the adjacent launcher",
)
require(
    "verifyCandidate(extractedPaths.get(WINDOWS_LAUNCHER_NAME))" in npm_installer
    and "if (platform === 'windows' && windowsPairReady(BIN_DIR)) return;" in npm_installer
    and "verifier(launcher)" in npm_installer,
    "npm must validate extracted and cached Windows pairs through the launcher",
)
require(
    "_windows_pair_ready" in python_wrapper,
    "PyPI readiness must require and validate the adjacent launcher/payload pair",
)
require(
    "return payload.with_name(_WINDOWS_LAUNCHER_NAME)" in python_wrapper
    and "execution_path = _execution_path(bin_path, sys.platform)" in python_wrapper
    and "args = [str(execution_path)] + sys.argv[1:]" in python_wrapper
    and "result = subprocess.run(args)" in python_wrapper
    and "args = [str(bin_path)] + sys.argv[1:]" not in python_wrapper,
    "PyPI must execute Windows commands through the adjacent launcher",
)
require(
    "windowsLauncherPath" in go_wrapper
    and "installWindowsPairAtomically" in go_wrapper,
    "Go readiness must require and validate the adjacent launcher/payload pair",
)
require(
    "return filepath.Join(filepath.Dir(payload), windowsLauncherName)" in go_wrapper
    and "return executionPathForOS(payload, runtime.GOOS), nil" in go_wrapper
    and "execBinary(executable, os.Args[1:])" in go_wrapper,
    "Go must execute Windows commands through the adjacent launcher",
)

# All package downloaders parse the Windows archive against the exact official
# five-root-file allowlist. Portable wrappers privately validate the executable
# pair and cache that pair immutably; they may not silently ignore an
# attacker-controlled sixth member.
exact_archive_guards = {
    "install.ps1": (
        "$seen.Count -ne $WindowsArchiveNames.Count",
        '"LICENSE"',
        '"install.ps1"',
        "THIRD_PARTY_NOTICES.md",
    ),
    "pkg/npm/install.js": (
        "$seen.Count -ne $requiredNames.Count",
        "WINDOWS_LAUNCHER_NAME",
        "WINDOWS_PAYLOAD_NAME",
        "'LICENSE'",
        "'install.ps1'",
        "THIRD_PARTY_NOTICES.md",
    ),
    "pkg/pypi/src/codebase_memory_mcp/_cli.py": (
        "name not in required_set",
        "len(seen) != len(required)",
        "_WINDOWS_LAUNCHER_NAME",
        "_WINDOWS_PAYLOAD_NAME",
        '"LICENSE"',
        '"install.ps1"',
        "THIRD_PARTY_NOTICES.md",
    ),
    "pkg/go/cmd/codebase-memory-mcp/main.go": (
        "len(seen) != len(archiveNames)",
        "windowsLauncherName",
        "windowsPayloadName",
        '"LICENSE"',
        '"install.ps1"',
        "THIRD_PARTY_NOTICES.md",
    ),
}
for relative, needles in exact_archive_guards.items():
    source = read(relative)
    require(
        all(needle in source for needle in needles),
        f"{relative} must reject every Windows zip namespace except the official five-file allowlist",
    )

# A portable mutation refusal must point to the owning package manager and to
# the explicit one-time transition into managed mode.
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
    "pkg/go/cmd/codebase-memory-mcp/main.go": (
        "go install github.com/DeusData/codebase-memory-mcp/pkg/go/cmd/codebase-memory-mcp@latest",
        "Remove-Item (Get-Command codebase-memory-mcp).Source",
        "codebase-memory-mcp install --yes",
    ),
}
for relative, needles in guidance_contracts.items():
    source = read(relative)
    require(
        all(needle in source for needle in needles),
        f"{relative} must provide actionable package and managed-install guidance",
    )

# Native Windows regression coverage moves from the transient activation helper
# to the permanent launcher suite.
windows_test_driver = read("scripts/test-windows.ps1")
require(
    "tests\\windows\\test_windows_launcher.py" in windows_test_driver
    or "tests/windows/test_windows_launcher.py" in windows_test_driver,
    "scripts/test-windows.ps1 must run tests/windows/test_windows_launcher.py",
)
require(
    "test_cli_activation_helper.py" not in windows_test_driver,
    "scripts/test-windows.ps1 must not run the legacy transient activation-helper test",
)
require(
    all(
        needle in windows_test_driver
        for needle in (
            'Copy-Item -LiteralPath $launcherBin -Destination $guardBin',
            'Copy-Item -LiteralPath $bin -Destination $guardPayload',
            '& $py $t $guardBin',
            '& $py $t $guardBin $guardPayload $abiMismatchLauncher',
        )
    ),
    "ordinary native Windows guards must execute a staged launcher/payload release pair",
)
require(
    windows_test_driver.count("| Out-Host") >= 3
    and windows_test_driver.count("$buildExit = $LASTEXITCODE") >= 3,
    "Windows build helpers must not leak compiler output into returned artifact paths",
)
require(
    '$code -eq 1 -or $t -eq "tests\\windows\\test_windows_launcher.py"'
    in windows_test_driver,
    "the permanent-launcher guard must fail instead of skip on driver/precondition errors",
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
    "Windows launcher guards must keep staged and Python-created fixtures "
    "beneath the current account profile",
)
require(
    '$guardRoot = $null\ntry {\n    $userProfile = '
    '[Environment]::GetFolderPath([Environment+SpecialFolder]::UserProfile)'
    in windows_test_driver
    and 'if ($guardRoot) {\n        Remove-Item -LiteralPath $guardRoot -Recurse -Force'
    in windows_test_driver,
    "Windows launcher guard setup must be covered by profile-fixture cleanup",
)

# The hosted runner profile can itself be trusted while newly-created children
# still inherit mutation-capable principals. Require the guard root to replace
# that inherited DACL with a protected, current-account-owned ACL before any
# launcher bundle or Python temporary descendants are created below it.
guard_root_creation = (
    'New-Item -ItemType Directory -Path $guardRoot | Out-Null'
)
guard_bundle_creation = '$guardBundle = Join-Path $guardRoot '
acl_start = windows_test_driver.find(guard_root_creation)
acl_end = windows_test_driver.find(guard_bundle_creation, acl_start + 1)
guard_acl_setup = (
    windows_test_driver[acl_start:acl_end]
    if acl_start >= 0 and acl_end > acl_start
    else ""
)
require(
    all(
        needle in guard_acl_setup
        for needle in (
            '[System.Security.Principal.WindowsIdentity]::GetCurrent().User',
            '[System.Security.AccessControl.DirectorySecurity]::new()',
            '$guardAcl.SetOwner($currentSid)',
            '$guardAcl.SetAccessRuleProtection($true, $false)',
            '[System.Security.AccessControl.FileSystemRights]::FullControl',
            '[System.Security.AccessControl.InheritanceFlags]::ContainerInherit',
            '[System.Security.AccessControl.InheritanceFlags]::ObjectInherit',
            '[System.Security.AccessControl.PropagationFlags]::None',
            '[System.Security.AccessControl.AccessControlType]::Allow',
            'Set-Acl -LiteralPath $guardRoot -AclObject $guardAcl',
        )
    ),
    "Windows launcher guards must protect the guard-root DACL and grant only "
    "the current account inheritable full control before creating descendants",
)

# Launcher supervision has two distinct failure directions: killing the
# launcher must kill its payload job, and killing only the launcher's immediate
# parent must terminate the launcher plus every descendant. Require the native
# test to invoke both probes, not merely define helpers that never run.
windows_launcher_test = read("tests/windows/test_windows_launcher.py")
unsafe_acl_test = windows_launcher_test[
    windows_launcher_test.find("def assert_untrusted_ancestor_acl_rejected(") :
    windows_launcher_test.find("\ndef process_entries()")
]
require(
    unsafe_acl_test.count('(extended_launcher, "extended DOS")') >= 2,
    "native Windows coverage must reject unsafe ancestor ACLs through extended DOS paths",
)
add_only_start = windows_launcher_test.find(
    "def assert_add_only_ancestor_acl_allowed("
)
add_only_grant = windows_launcher_test.find(
    'grant = run(["icacls", ancestor, "/grant", "*S-1-1-0:(AD)"]',
    add_only_start,
)
add_only_before_ace = windows_launcher_test[add_only_start:add_only_grant]
require(
    add_only_start >= 0
    and add_only_grant > add_only_start
    and 'before_ace = run([launcher, "--version"], env)' in add_only_before_ace
    and "launcher context failed before the add-only ACE was installed"
    in add_only_before_ace,
    "native add-only coverage must prove launcher-context authentication before mutating the ACL",
)
require(
    windows_launcher_test.count("assert_launcher_death_contains_payload(") >= 2
    and "server.proc.kill()" in windows_launcher_test
    and "wait_for_process_exit(child_pid" in windows_launcher_test,
    "native Windows coverage must kill the launcher and reject an orphaned payload",
)
require(
    windows_launcher_test.count(
        "assert_immediate_parent_death_contains_launcher_tree("
    ) >= 2
    and '"--launcher-parent-probe"' in windows_launcher_test
    and "wrapper.kill()" in windows_launcher_test
    and "wait_for_process_exit(launcher_pid" in windows_launcher_test,
    "native Windows coverage must kill the immediate parent and reject a surviving launcher tree",
)

launcher_source = read("src/launcher/windows_launcher.c")
require(
    "JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE" in launcher_source
    and "PROC_THREAD_ATTRIBUTE_JOB_LIST" in launcher_source
    and "launcher_parent_liveness_open" in launcher_source
    and "WaitForMultipleObjects" in launcher_source
    and "TerminateJobObject" in launcher_source,
    "the permanent launcher must contain payloads and supervise immediate-parent death",
)

# Launcher-context authentication accepts the launcher's bounded privileged
# owner set without widening the exact-current-owner policy used by mutable
# state transactions.
launcher_state_source = read("src/cli/windows_launcher_state.c")
generic_opener_start = launcher_state_source.find(
    "static HANDLE windows_open_regular_no_reparse_links("
)
context_opener_start = launcher_state_source.find(
    "static HANDLE windows_open_launcher_context_file("
)
generic_opener = launcher_state_source[generic_opener_start:context_opener_start]
context_opener_end = launcher_state_source.find(
    "static HANDLE windows_open_directory_secure_access(", context_opener_start
)
context_opener = launcher_state_source[context_opener_start:context_opener_end]
context_consume_start = launcher_state_source.rfind(
    "bool cbm_windows_launcher_context_consume("
)
context_consume_end = launcher_state_source.find(
    "bool cbm_windows_launcher_context_complete(", context_consume_start
)
context_consume = launcher_state_source[context_consume_start:context_consume_end]
require(
    generic_opener_start >= 0
    and context_opener_start > generic_opener_start
    and "windows_owner_is_current(file)" in generic_opener
    and "windows_owner_secure(file, false)" not in generic_opener,
    "generic Windows transaction-file opens must continue requiring the exact current owner",
)
require(
    context_opener_end > context_opener_start
    and all(
        needle in context_opener
        for needle in (
            "windows_path_tree_plain(path)",
            "windows_file_identity_links(file, &information, expected_links)",
            "windows_owner_secure(file, false)",
            "windows_acl_secure(file)",
            "FILE_FLAG_OPEN_REPARSE_POINT",
        )
    ),
    "launcher-context file opens must use the launcher trusted-owner set without weakening path, "
    "link-count, reparse-point, or ACL checks",
)
require(
    context_consume_start >= 0
    and context_consume_end > context_consume_start
    and all(
        needle in context_consume
        for needle in (
            "GetNamedPipeServerProcessId(pipe, &actual_pid)",
            "actual_pid == claimed_pid",
            "windows_filetime_equal(&actual_creation, &claimed_creation)",
            "QueryFullProcessImageNameW(server, 0U, actual_path, &actual_path_length)",
            "windows_open_launcher_context_file(actual_path, expected_launcher_links)",
            "windows_open_launcher_context_file(claimed_path, expected_launcher_links)",
            "windows_file_identity_links(actual_file, &actual_info, expected_launcher_links)",
            "windows_file_identity_links(claimed_file, &claimed_info, expected_launcher_links)",
            "windows_same_identity(&actual_info, &claimed_info)",
            "Windows launcher context actual image failed secure open",
            "Windows launcher context claimed path failed secure open",
            "Windows launcher context actual/claimed file identity mismatch",
        )
    ),
    "launcher-context consumption must bind PID/creation/file identity and diagnose each file-auth "
    "stage",
)

# Managed generations are exact two-file directories. Publication creates the
# canonical name as the generation launcher's second hard link; update and
# uninstall use mapped-image-safe namespace transactions rather than attempting
# to delete the mapped executable directly.
require(
    all(
        needle in launcher_state_source
        for needle in (
            "windows_generation_layout_exact",
            "CreateHardLinkW(stage, backing_path, NULL)",
            "windows_posix_rename_handle(stage_file, target_path)",
            "cbm_windows_managed_launcher_backing",
            "canonical launcher has no unique exact generation backing",
            "windows_open_regular_no_reparse_links(launcher_path, GENERIC_READ, 1U)",
        )
    ),
    "Windows launcher publication must enforce an exact two-file generation and exact-two "
    "canonical/backing hard-link identity",
)
capability_probe_start = launcher_state_source.rfind(
    "static bool windows_launcher_capability_probe_once("
)
capability_probe = launcher_state_source[capability_probe_start:]
require(
    capability_probe_start >= 0
    and "bool cbm_windows_launcher_capability_probe(" in capability_probe,
    "the capability probe implementation must keep its cold-scan retry wrapper",
)
require(
    all(
        needle in capability_probe
        for needle in (
            "old_generation",
            "new_generation",
            "windows_generation_layout_exact(old_generation, 2U, NULL)",
            "cbm_windows_launcher_replace_atomic(canonical, new_backing",
            "cbm_windows_launcher_remove_posix(canonical",
            "windows_posix_rename_handle_no_replace(state, retired_directory)",
            "windows_posix_rename_handle_no_replace(state, state_directory)",
            "WaitForSingleObject(child_b.hProcess, 0U) == WAIT_TIMEOUT",
        )
    ),
    "the capability probe must exercise the real exact-generation hard-link transaction and "
    "mapped state-directory retire/restore before session drain",
)
uninstall_commit_start = launcher_state_source.rfind(
    "bool cbm_windows_launcher_uninstall_commit("
)
uninstall_commit_end = launcher_state_source.find(
    "static bool windows_generation_directory_path(", uninstall_commit_start
)
uninstall_commit = launcher_state_source[uninstall_commit_start:uninstall_commit_end]
require(
    all(
        needle in uninstall_commit
        for needle in (
            "cbm_windows_retired_state_path",
            "windows_posix_rename_handle_no_replace(state, retired_directory)",
            "cbm_windows_launcher_remove_posix",
            "original_canonical_survived",
            "windows_same_identity(&canonical_information, &original_backing_information)",
            "windows_posix_rename_handle_no_replace(state, state_directory)",
        )
    ),
    "uninstall must retire state before its final canonical unlink and restore only when the "
    "original exact launcher identity survived",
)

cleanup_start = launcher_source.find("static bool launcher_schedule_managed_cleanup(")
cleanup_end = launcher_source.find("static bool launcher_read_all(", cleanup_start)
cleanup_source = launcher_source[cleanup_start:cleanup_end]
require(
    cleanup_start >= 0
    and all(
        needle in cleanup_source
        for needle in (
            "cbm_windows_retired_state_path",
            "cbm_windows_managed_launcher_backing",
            "GetSystemDirectoryW",
            'L"%ls\\\\cmd.exe"',
            'L"%ls\\\\ping.exe"',
            "DETACHED_PROCESS",
            "CREATE_NEW_PROCESS_GROUP",
            "CreateProcessW(command_path, mutable_command, NULL, NULL, FALSE",
            "retired_basename",
        )
    )
    and "rd /s /q .cbm" not in cleanup_source
    and "exist .cbm" not in cleanup_source,
    "launcher cleanup must delete only the authenticated retired basename via validated absolute "
    "System32 utilities and must preserve a concurrent reinstall's new .cbm",
)

cli_uninstall_source = read("src/cli/cli.c")
managed_uninstall_match = re.search(
    r"static int cli_windows_managed_uninstall_activate\(.*?\n}\n",
    cli_uninstall_source,
    re.DOTALL,
)
managed_uninstall = managed_uninstall_match.group(0) if managed_uninstall_match else ""
require(
    "cbm_windows_launcher_uninstall_commit" in managed_uninstall
    and "activation->state.payload_sha256" in managed_uninstall
    and "cbm_windows_launcher_remove_posix" not in managed_uninstall,
    "managed uninstall must use the state-retirement/final-unlink commit API rather than unlinking "
    "the mapped launcher directly",
)

require(
    all(
        needle in windows_launcher_test
        for needle in (
            "interrupted launcher-first initial install",
            "os.link(interrupted_backing, interrupted_launcher)",
            "assert_failed_uninstall_restores_state",
            "assert_uninstall_immediate_reinstall_safe",
            "retired_state_siblings",
            "old detached cleanup deleted or replaced",
            "canonical.samefile(backing)",
            "canonical.stat().st_nlink == 2",
        )
    ),
    "native Windows coverage must exercise interrupted hard-link publication, unlink rollback, "
    "retired cleanup, and immediate reinstall isolation",
)

# On Windows subprocess supervision receives a non-NULL lpApplicationName, so
# a literal `git` would not use PATH. Resolve only git.exe beneath inherited
# absolute PATH entries and never permit the current-directory search implied
# by empty or relative entries. POSIX retains execvp via argv[0].
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
    "Windows watcher Git commands must resolve an explicit absolute git.exe without cwd search "
    "while POSIX retains literal argv supervision",
)

# The build system exposes a distinct launcher artifact; release jobs later copy
# that small binary to the canonical public name.
makefile = read("Makefile.cbm")
launcher_target = re.search(
    r"(?m)^\s*[^#\n]*codebase-memory-mcp-launcher(?:\.exe|\$\([A-Za-z0-9_]+\))?\s*:",
    makefile,
)
require(
    launcher_target is not None,
    "Makefile.cbm must expose a codebase-memory-mcp-launcher executable target",
)

# Every native path-tree trust boundary must validate opened ancestor handles,
# reject reparse points, inspect mutation-capable allow ACEs, and recognize the
# bounded privileged-principal set (current user, SYSTEM, Administrators, and
# the fixed TrustedInstaller service SID). The normal C:\\Users ancestor grants
# cross-account add-subdirectory, so each implementation permits only that
# right on intermediate components while keeping its final private directory
# strict. The native tests inject real ACLs; these static markers preserve the
# narrow exception and its surrounding defenses on non-Windows presubmit hosts.
ancestor_security_contracts = {
    "src/daemon/ipc.c": (
        "win_directory_component_secure",
        "win_file_security_secure(security, directory, false, mutation)",
        "win_private_mutation_rights()",
        "~((DWORD)FILE_ADD_SUBDIRECTORY)",
        "FILE_ADD_FILE",
        "FILE_DELETE_CHILD",
        "final runtime",
        "ACCESS_SYSTEM_SECURITY",
        "956008885U",
        "FILE_ATTRIBUTE_REPARSE_POINT",
    ),
    "src/cli/windows_launcher_state.c": (
        "windows_owner_secure(component, false)",
        "windows_acl_secure_for_mutation(component, mutation)",
        "windows_private_mutation_rights()",
        "if (index < directory_length)",
        "windows_private_mutation_rights() & ~((DWORD)FILE_ADD_SUBDIRECTORY)",
        "mutation &= ~((DWORD)FILE_ADD_SUBDIRECTORY)",
        "FILE_ADD_FILE",
        "FILE_DELETE_CHILD",
        "immediate parent remains fully private",
        "FILE_READ_ATTRIBUTES | READ_CONTROL",
        "ACCESS_SYSTEM_SECURITY",
        "956008885U",
        "FILE_FLAG_OPEN_REPARSE_POINT",
    ),
    "src/launcher/windows_launcher.c": (
        "launcher_security_is_safe(component, false, mutation)",
        "launcher_private_mutation_rights()",
        "if (index < directory_length)",
        "mutation &= ~((DWORD)FILE_ADD_SUBDIRECTORY)",
        "FILE_ADD_FILE",
        "FILE_DELETE_CHILD",
        "DLL or .exe.local",
        "ACCESS_SYSTEM_SECURITY",
        "956008885U",
        "FILE_FLAG_OPEN_REPARSE_POINT",
        "PIPE_REJECT_REMOTE_CLIENTS",
    ),
}
for relative, needles in ancestor_security_contracts.items():
    source = read(relative)
    require(
        all(needle in source for needle in needles),
        f"{relative} must enforce the shared cross-account ancestor trust policy",
    )

# Release smoke must run the canonical launcher while preserving the payload it
# resolves.  Requiring both names in the Windows smoke job prevents a test from
# accidentally continuing to exercise a renamed monolithic payload.
smoke_workflow = read(".github/workflows/_smoke.yml")
windows_match = re.search(
    r"(?ms)^  smoke-windows:\s*(.*?)(?=^  [A-Za-z0-9_-]+:\s*$|\Z)",
    smoke_workflow,
)
windows_smoke = windows_match.group(1) if windows_match else ""
require(bool(windows_smoke), "_smoke.yml must contain the smoke-windows job")
require(
    'scripts/smoke-test.sh "$SMOKE_DIR/codebase-memory-mcp.exe"' in windows_smoke,
    f"Windows release smoke must execute the canonical {launcher}",
)
require(
    launcher in windows_smoke and payload in windows_smoke,
    f"Windows release smoke must retain both {launcher} and {payload}",
)
smoke_blocks = yaml_run_blocks(windows_smoke)
require(
    any("zip" in block and launcher in block and payload in block for block in smoke_blocks),
    "Windows artifact-server smoke archive must contain launcher and payload",
)
windows_release_smoke_blocks = [
    re.sub(r"\s+", " ", re.sub(r"\\\s*\n\s*", " ", block)).strip()
    for block in smoke_blocks
    if 'scripts/smoke-test.sh "$SMOKE_DIR/codebase-memory-mcp.exe"' in block
]
require(
    len(windows_release_smoke_blocks) == 1
    and all(
        needle in windows_release_smoke_blocks[0]
        for needle in (
            'PROFILE_ROOT="$(cygpath -u "$USERPROFILE")"',
            'SMOKE_DIR="$(mktemp -d "$PROFILE_ROOT/cbm-release-smoke.XXXXXX")"',
            'cp codebase-memory-mcp.exe codebase-memory-mcp.payload.exe "$SMOKE_DIR/"',
            'CBM_CACHE_DIR="$(cygpath -m "$SMOKE_DIR/cache")" '
            'SMOKE_TEMP_ROOT="$SMOKE_DIR" '
            'scripts/smoke-test.sh "$SMOKE_DIR/codebase-memory-mcp.exe"',
        )
    ),
    "Windows release smoke must keep every launcher fixture beneath the current account profile",
)
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
            'cp codebase-memory-mcp.exe codebase-memory-mcp.payload.exe "$LAUNCH_DIR/"',
            '"$LAUNCH_DIR/codebase-memory-mcp.payload.exe" --version',
            '"$LAUNCH_DIR/codebase-memory-mcp.exe" --version',
        )
    ),
    "Windows release version checks must execute the pair beneath the current account profile",
)
require(
    "$RUNNER_TEMP" not in windows_smoke,
    "Windows release smoke must not treat GitHub's shared RUNNER_TEMP ancestry as private",
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
            'cp codebase-memory-mcp.exe codebase-memory-mcp.payload.exe "$SECURITY_DIR/"',
            'TMPDIR="$SECURITY_DIR" '
            'scripts/security-install.sh "$SECURITY_DIR/codebase-memory-mcp.exe"',
        )
    ),
    "Windows release install audit must execute the pair beneath the current account profile",
)

# Native update transport remains HTTPS-only in production. Release smoke may
# use an explicit file:// CBM_DOWNLOAD_URL only for its local fixture, while the
# installer and raw-curl phases continue to exercise the loopback HTTP server.
smoke_script = read("scripts/smoke-test.sh")
require(
    'copy_smoke_binary "$FAKE_HOME/.local/bin/codebase-memory-mcp.exe"' not in smoke_script
    and 'copy_smoke_binary "$UPDATE_HOME/.local/bin/codebase-memory-mcp.exe"'
    not in smoke_script,
    "Windows smoke must leave managed canonical targets absent for authenticated install",
)
require(
    "smoke_mktemp_file" in smoke_script
    and "smoke_mktemp_dir" in smoke_script
    and re.search(r"\$\(\s*mktemp(?:\s+-d)?(?:\s|\))", smoke_script) is None,
    "smoke-test.sh must route every temporary fixture through its private-root helpers",
)
require(
    'SMOKE_UPDATE_FIXTURE_DIR' in smoke_script
    and 'UPDATE_DOWNLOAD_URL="file://$UPDATE_FIXTURE_DIR"' in smoke_script
    and 'UPDATE_DOWNLOAD_URL="file:///$UPDATE_FIXTURE_DIR"' in smoke_script
    and 'CBM_DOWNLOAD_URL="$UPDATE_DOWNLOAD_URL"' in smoke_script,
    "Phase 14 native update must use an explicit file:// fixture override",
)
require(
    "'$env:TEMP=$args[0]; $env:TMP=$args[0]; & $args[1] $args[2]'" in smoke_script
    and '"$WIN_HOME" "$WIN_SCRIPT" "--dir=$WIN_DIR"' in smoke_script,
    "Windows install.ps1 smoke must set native TEMP/TMP inside PowerShell",
)
require(
    'CBM_DOWNLOAD_URL="$SMOKE_DOWNLOAD_URL"' in smoke_script
    and '"$SMOKE_DOWNLOAD_URL/$DL_ARCHIVE"' in smoke_script,
    "installer and raw download smoke phases must retain loopback HTTP coverage",
)
require(
    smoke_workflow.count("SMOKE_UPDATE_FIXTURE_DIR: /tmp/smoke-server") == 2,
    "Unix and Windows release smoke must identify their local update fixture",
)

cli_source = read("src/cli/cli.c")
file_override_match = re.search(
    r"static bool cli_download_is_explicit_file_override\(.*?\n}\n",
    cli_source,
    re.DOTALL,
)
protocol_match = re.search(
    r"static const char \*cli_download_protocol\(.*?\n}\n",
    cli_source,
    re.DOTALL,
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
    cli_source.find("static int cbm_download_to_file("):
    cli_source.find("/* ── macOS ad-hoc signing")
]
require(
    download_helpers.count('"--proto"') >= 2
    and download_helpers.count('"--proto-redir"') >= 2,
    "native curl invocations must pin both initial and redirected protocols",
)

# PR smoke builds artifacts in-place rather than downloading a release archive.
# It must therefore stage the build payload and permanent launcher under their
# release names in a disposable directory, then exercise the canonical launcher.
pr_workflow = read(".github/workflows/pr.yml")
pr_smoke_match = re.search(
    r"(?ms)^  pr-smoke:\s*(.*?)(?=^  [A-Za-z0-9_-]+:\s*$|\Z)",
    pr_workflow,
)
pr_smoke = pr_smoke_match.group(1) if pr_smoke_match else ""
require(bool(pr_smoke), "pr.yml must contain the pr-smoke job")
pr_windows_blocks = [
    block
    for block in yaml_run_blocks(pr_smoke)
    if "scripts/build.sh CC=clang CXX=clang++" in block
]
require(
    len(pr_windows_blocks) == 1,
    "PR smoke must contain exactly one Windows production build run block",
)
if pr_windows_blocks:
    pr_windows_block = re.sub(r"\\\s*\n\s*", " ", pr_windows_blocks[0])
    pr_windows_block = re.sub(r"\s+", " ", pr_windows_block).strip()
    staging_steps = (
        'PROFILE_ROOT="$(cygpath -u "$USERPROFILE")"',
        'SMOKE_DIR="$(mktemp -d "$PROFILE_ROOT/cbm-pr-smoke.XXXXXX")"',
        'trap \'rm -rf "$SMOKE_DIR"\' EXIT',
        'cp build/c/codebase-memory-mcp-launcher.exe '
        '"$SMOKE_DIR/codebase-memory-mcp.exe"',
        'cp build/c/codebase-memory-mcp.exe '
        '"$SMOKE_DIR/codebase-memory-mcp.payload.exe"',
        'CBM_CACHE_DIR="$(cygpath -m "$SMOKE_DIR/cache")" '
        'SMOKE_TEMP_ROOT="$SMOKE_DIR" '
        'scripts/smoke-test.sh "$SMOKE_DIR/codebase-memory-mcp.exe"',
    )
    positions = [pr_windows_block.find(step) for step in staging_steps]
    require(
        all(position >= 0 for position in positions),
        "Windows PR smoke must stage launcher and payload under release names "
        "beneath the current account profile and invoke the canonical launcher",
    )
    require(
        all(left < right for left, right in zip(positions, positions[1:])),
        "Windows PR smoke must create the temporary bundle before invoking it",
    )
    require(
        pr_windows_block.count("scripts/smoke-test.sh") == 1,
        "Windows PR smoke must invoke smoke-test exactly once through the launcher",
    )
    require(
        "$RUNNER_TEMP" not in pr_windows_block,
        "Windows PR smoke must not treat GitHub's shared RUNNER_TEMP ancestry as private",
    )

if failures:
    print("Windows launcher bundle contract FAILED:", file=sys.stderr)
    for failure in failures:
        print(f"  - {failure}", file=sys.stderr)
    raise SystemExit(1)

print("Windows launcher bundle contract passed")
PY
