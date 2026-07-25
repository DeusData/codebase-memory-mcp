"""GREEN native-Windows guard for the permanent launcher contract.

The release contains two executables:

* ``codebase-memory-mcp.exe`` is the small permanent launcher.
* ``codebase-memory-mcp.payload.exe`` is the portable CBM payload.

Only ``install`` turns that pair into a managed installation.  A managed
launcher resolves its payload from a strict, fixed-size current-v1 record
relative to the launcher's own directory.  Portable package-manager payloads
remain useful for ordinary MCP/CLI commands, but must refuse self-update and
self-uninstall before they disturb an active daemon generation.

This guard deliberately crosses real CreateProcess/stdio/filesystem boundaries
on native Windows.  It also kills the launcher abruptly and inspects the
process tree, because a waiting launcher must own its payload child strongly
enough that killing the launcher cannot leave an orphaned MCP client.

Exit code: 0 == contract honored, 1 == regression, 2 == precondition failure.

Usage:
    python test_windows_launcher.py \
        <launcher.exe> <payload.exe> <abi-mismatch-launcher.exe>
"""

import ctypes
from ctypes import wintypes
import hashlib
import os
import pathlib
import platform
import re
import shutil
import struct
import subprocess
import sys
import tempfile
import time
import zipfile

if os.name == "nt":
    import winreg

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mcp_stdio import McpError, McpServer  # noqa: E402


CURRENT_MAGIC = b"CBMCUR1\0"
CURRENT_RECORD_SIZE = 128
CURRENT_FORMAT_OFFSET = 8
CURRENT_SIZE_OFFSET = 12
CURRENT_ABI_MIN_OFFSET = 16
CURRENT_ABI_MAX_OFFSET = 20
CURRENT_PAYLOAD_SIZE_OFFSET = 24
CURRENT_SHA256_OFFSET = 32
CURRENT_SHA256_SIZE = 64
CURRENT_RESERVED_OFFSET = 96

DESCRIPTOR_MAGIC = b"CBMWRD1\0"
DESCRIPTOR_RECORD_SIZE = 128
DESCRIPTOR_FORMAT_OFFSET = 8
DESCRIPTOR_SIZE_OFFSET = 12
DESCRIPTOR_LAUNCHER_ABI_OFFSET = 16
DESCRIPTOR_PAYLOAD_ABI_MIN_OFFSET = 20
DESCRIPTOR_PAYLOAD_ABI_MAX_OFFSET = 24
DESCRIPTOR_FLAGS_OFFSET = 28
DESCRIPTOR_PAYLOAD_SIZE_OFFSET = 32
DESCRIPTOR_SHA256_OFFSET = 40
DESCRIPTOR_RESERVED_OFFSET = 104

TH32CS_SNAPPROCESS = 0x00000002
INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
SYNCHRONIZE = 0x00100000
WAIT_OBJECT_0 = 0x00000000
WAIT_TIMEOUT = 0x00000102


class GuardFailure(Exception):
    pass


def sha256_file(path):
    """SHA-256 hex digest of a file's contents (payload/launcher digest checks)."""
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


class PROCESSENTRY32W(ctypes.Structure):
    _fields_ = [
        ("dwSize", wintypes.DWORD),
        ("cntUsage", wintypes.DWORD),
        ("th32ProcessID", wintypes.DWORD),
        ("th32DefaultHeapID", ctypes.c_size_t),
        ("th32ModuleID", wintypes.DWORD),
        ("cntThreads", wintypes.DWORD),
        ("th32ParentProcessID", wintypes.DWORD),
        ("pcPriClassBase", wintypes.LONG),
        ("dwFlags", wintypes.DWORD),
        ("szExeFile", wintypes.WCHAR * 260),
    ]


def require(condition, message):
    if not condition:
        raise GuardFailure(message)


def output_text(result):
    return ((result.stdout or b"") + b"\n" + (result.stderr or b"")).decode(
        "utf-8", "replace"
    )


def run(command, env, timeout=30):
    try:
        return subprocess.run(
            [str(part) for part in command],
            input=b"",
            capture_output=True,
            env=env,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise GuardFailure(
            "command exceeded %ss: %s" % (timeout, " ".join(map(str, command)))
        ) from exc


def isolated_environment(work):
    home = work / "home"
    cache = work / "cache"
    home.mkdir(parents=True)
    cache.mkdir(parents=True)
    env = dict(os.environ)
    env.update(
        {
            "HOME": str(home),
            "USERPROFILE": str(home),
            "APPDATA": str(home / "AppData" / "Roaming"),
            "LOCALAPPDATA": str(home / "AppData" / "Local"),
            "CBM_CACHE_DIR": str(cache),
            "PYTHONUTF8": "1",
        }
    )
    return env, cache


def copy_portable_pair(source_launcher, source_payload, directory):
    directory.mkdir(parents=True, exist_ok=True)
    launcher = directory / "codebase-memory-mcp.exe"
    payload = directory / "codebase-memory-mcp.payload.exe"
    shutil.copy2(source_launcher, launcher)
    shutil.copy2(source_payload, payload)
    return launcher, payload


def copy_portable_payload(source_payload, directory):
    directory.mkdir(parents=True, exist_ok=True)
    payload = directory / "codebase-memory-mcp.payload.exe"
    shutil.copy2(source_payload, payload)
    return payload


def path_registry_snapshot():
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as key:
            value, kind = winreg.QueryValueEx(key, "Path")
            return True, value, kind
    except FileNotFoundError:
        return False, None, None


def path_registry_restore(snapshot):
    existed, value, kind = snapshot
    with winreg.CreateKey(winreg.HKEY_CURRENT_USER, "Environment") as key:
        if existed:
            winreg.SetValueEx(key, "Path", 0, kind, value)
        else:
            try:
                winreg.DeleteValue(key, "Path")
            except FileNotFoundError:
                pass


def find_and_validate_current(managed_dir):
    matches = []
    for candidate in (managed_dir / ".cbm").rglob("*"):
        if not candidate.is_file():
            continue
        try:
            data = candidate.read_bytes()
        except OSError:
            continue
        if len(data) == CURRENT_RECORD_SIZE and data.startswith(CURRENT_MAGIC):
            matches.append((candidate, data))
    require(
        len(matches) == 1,
        "managed layout must contain exactly one 128-byte CBMCUR1 current record; got %d"
        % len(matches),
    )
    current, data = matches[0]
    version = struct.unpack_from("<I", data, CURRENT_FORMAT_OFFSET)[0]
    size = struct.unpack_from("<I", data, CURRENT_SIZE_OFFSET)[0]
    abi_min = struct.unpack_from("<I", data, CURRENT_ABI_MIN_OFFSET)[0]
    abi_max = struct.unpack_from("<I", data, CURRENT_ABI_MAX_OFFSET)[0]
    payload_size = struct.unpack_from("<Q", data, CURRENT_PAYLOAD_SIZE_OFFSET)[0]
    try:
        digest = data[
            CURRENT_SHA256_OFFSET : CURRENT_SHA256_OFFSET + CURRENT_SHA256_SIZE
        ].decode("ascii")
    except UnicodeDecodeError as exc:
        raise GuardFailure("current-v1 SHA-256 is not ASCII") from exc

    require(version == 1, "current record is not current-v1")
    require(size == CURRENT_RECORD_SIZE, "current-v1 record_size is not 128")
    require(abi_min > 0 and abi_min <= abi_max, "current-v1 ABI range is invalid")
    require(re.fullmatch(r"[0-9a-f]{64}", digest), "current-v1 digest is not lowercase SHA-256")
    require(
        data[CURRENT_RESERVED_OFFSET:] == bytes(CURRENT_RECORD_SIZE - CURRENT_RESERVED_OFFSET),
        "current-v1 reserved bytes must be zero",
    )

    payload = (
        managed_dir
        / ".cbm"
        / "generations"
        / digest
        / "codebase-memory-mcp.payload.exe"
    )
    backing = payload.parent / "codebase-memory-mcp.exe"
    canonical = managed_dir / "codebase-memory-mcp.exe"
    require(payload.is_file(), "current-v1 generation payload is missing: %s" % payload)
    require(backing.is_file(), "current-v1 generation launcher backing is missing: %s" % backing)
    require(canonical.is_file(), "managed canonical launcher is missing: %s" % canonical)
    require(
        {path.name for path in payload.parent.iterdir()}
        == {"codebase-memory-mcp.payload.exe", "codebase-memory-mcp.exe"},
        "managed generation must contain exactly payload plus launcher backing",
    )
    require(payload.stat().st_size == payload_size, "current-v1 payload size does not match")
    observed = hashlib.sha256(payload.read_bytes()).hexdigest()
    require(observed == digest, "installed generation name/digest does not match payload bytes")
    require(canonical.samefile(backing), "canonical launcher is not its generation hardlink")
    require(
        canonical.stat().st_nlink == 2 and backing.stat().st_nlink == 2,
        "canonical launcher/backing must report exactly two links",
    )
    require(payload.stat().st_nlink == 1, "generation payload must remain an exact one-link file")
    return current, data, payload


def current_v1_record(payload, abi_min=1, abi_max=1):
    digest = sha256_file(payload)
    record = bytearray(CURRENT_RECORD_SIZE)
    record[: len(CURRENT_MAGIC)] = CURRENT_MAGIC
    struct.pack_into("<I", record, CURRENT_FORMAT_OFFSET, 1)
    struct.pack_into("<I", record, CURRENT_SIZE_OFFSET, CURRENT_RECORD_SIZE)
    struct.pack_into("<I", record, CURRENT_ABI_MIN_OFFSET, abi_min)
    struct.pack_into("<I", record, CURRENT_ABI_MAX_OFFSET, abi_max)
    struct.pack_into("<Q", record, CURRENT_PAYLOAD_SIZE_OFFSET, payload.stat().st_size)
    record[
        CURRENT_SHA256_OFFSET : CURRENT_SHA256_OFFSET + CURRENT_SHA256_SIZE
    ] = digest.encode("ascii")
    return bytes(record)


def generation_paths(managed_dir, payload):
    digest = sha256_file(payload)
    generation = managed_dir / ".cbm" / "generations" / digest
    return (
        generation,
        generation / "codebase-memory-mcp.payload.exe",
        generation / "codebase-memory-mcp.exe",
    )


def wait_for_path_absent(path, timeout=30):
    deadline = time.monotonic() + timeout
    while path.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    return not path.exists()


# The retired-state directory embeds the first 16 hex chars (64 bits) of the
# payload digest — enough to identify the generation while keeping the retired
# path within the managed uninstall's directory-rename length limit.
RETIRED_TAG_HEX = 16


def retired_state_siblings(managed_dir, digest=None):
    tag = digest[:RETIRED_TAG_HEX] if digest else "*"
    pattern = ".cbm-retired-v1-%s-*" % tag
    return sorted(path for path in managed_dir.glob(pattern) if path.is_dir())


def run_uninstall_observing_retirement(launcher, managed_dir, env, timeout=180):
    _, current_bytes, _ = find_and_validate_current(managed_dir)
    digest = current_bytes[
        CURRENT_SHA256_OFFSET : CURRENT_SHA256_OFFSET + CURRENT_SHA256_SIZE
    ].decode("ascii")
    process = subprocess.Popen(
        [str(launcher), "uninstall", "--yes"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    if process.stdin:
        process.stdin.close()
        process.stdin = None
    observed = set()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        observed.update(retired_state_siblings(managed_dir, digest))
        if process.poll() is not None:
            break
        time.sleep(0.001)
    if process.poll() is None:
        process.kill()
        process.wait(timeout=5)
        raise GuardFailure("managed uninstall exceeded %ss" % timeout)
    stdout, stderr = process.communicate(timeout=5)
    observed.update(retired_state_siblings(managed_dir, digest))
    result = subprocess.CompletedProcess(
        [str(launcher), "uninstall", "--yes"], process.returncode, stdout, stderr
    )
    return result, observed, digest


def parse_release_descriptor(data):
    require(len(data) == DESCRIPTOR_RECORD_SIZE, "release descriptor size is not 128")
    require(data[:8] == DESCRIPTOR_MAGIC, "release descriptor magic is invalid")
    require(
        struct.unpack_from("<I", data, DESCRIPTOR_FORMAT_OFFSET)[0] == 1,
        "release descriptor format is not v1",
    )
    require(
        struct.unpack_from("<I", data, DESCRIPTOR_SIZE_OFFSET)[0]
        == DESCRIPTOR_RECORD_SIZE,
        "release descriptor record_size is invalid",
    )
    launcher_abi = struct.unpack_from("<I", data, DESCRIPTOR_LAUNCHER_ABI_OFFSET)[0]
    payload_min = struct.unpack_from("<I", data, DESCRIPTOR_PAYLOAD_ABI_MIN_OFFSET)[0]
    payload_max = struct.unpack_from("<I", data, DESCRIPTOR_PAYLOAD_ABI_MAX_OFFSET)[0]
    flags = struct.unpack_from("<I", data, DESCRIPTOR_FLAGS_OFFSET)[0]
    payload_size = struct.unpack_from("<Q", data, DESCRIPTOR_PAYLOAD_SIZE_OFFSET)[0]
    digest = data[DESCRIPTOR_SHA256_OFFSET:DESCRIPTOR_RESERVED_OFFSET].decode("ascii")
    require(flags == 0, "release descriptor flags are nonzero")
    require(
        payload_min > 0 and payload_min <= launcher_abi <= payload_max,
        "release descriptor launcher/payload ABI range is incompatible",
    )
    require(payload_size > 0, "release descriptor payload size is zero")
    require(re.fullmatch(r"[0-9a-f]{64}", digest), "release descriptor digest is invalid")
    require(
        data[DESCRIPTOR_RESERVED_OFFSET:] == bytes(
            DESCRIPTOR_RECORD_SIZE - DESCRIPTOR_RESERVED_OFFSET
        ),
        "release descriptor reserved bytes are nonzero",
    )
    return launcher_abi, payload_min, payload_max, payload_size, digest


def assert_release_descriptor(source_launcher, source_payload, env, cache):
    cache_before = sorted(str(path.relative_to(cache)) for path in cache.rglob("*"))
    result = run(
        [source_launcher, "__cbm_windows_release_descriptor_v1"], env, timeout=30
    )
    require(
        result.returncode == 0,
        "release descriptor probe failed: %s" % output_text(result)[-800:],
    )
    _, _, _, payload_size, digest = parse_release_descriptor(result.stdout)
    require(payload_size == source_payload.stat().st_size, "descriptor payload size mismatch")
    require(digest == sha256_file(source_payload), "descriptor payload digest mismatch")
    cache_after = sorted(str(path.relative_to(cache)) for path in cache.rglob("*"))
    require(cache_after == cache_before, "release descriptor probe touched cache/daemon state")
    print("PASS: release pair self-described ABI and payload identity without side effects")


def assert_portable_mutations_refuse(source_payload, env, cache, work):
    session_payload = copy_portable_payload(source_payload, work / "portable-session")
    with McpServer(str(session_payload), cache_dir=str(cache), extra_env=env) as server:
        server.initialize(timeout=30)
        require(server.tools_list(timeout=30), "portable MCP control session has no tools")

        for action, options in (
            ("update", ["--yes", "--standard"]),
            ("uninstall", ["--yes"]),
        ):
            command_payload = copy_portable_payload(
                source_payload, work / ("portable-" + action)
            )
            command_env = dict(env)
            # If the update guard regresses, keep its unintended network path
            # deterministic and fast.  A correct implementation refuses before
            # it consults this URL or asks the daemon to drain.
            command_env["CBM_DOWNLOAD_URL"] = "https://127.0.0.1:1"
            # Warm the freshly-copied cold binary first: first-touch antivirus
            # scanning of the just-written image inflates process load time, and
            # the refusal itself is a fast STATELESS local check (no daemon IPC,
            # no download) whose timing is what this guard actually measures. The
            # warm-up refuses identically and is discarded.
            run([command_payload, action] + options, command_env, timeout=10)
            started = time.monotonic()
            result = run([command_payload, action] + options, command_env, timeout=10)
            elapsed = time.monotonic() - started
            diagnostic = output_text(result).lower()
            require(result.returncode != 0, "portable %s unexpectedly succeeded" % action)
            require(elapsed < 8.0, "portable %s did not fail early" % action)
            require(
                "install" in diagnostic
                and any(
                    word in diagnostic
                    for word in ("package manager", "managed", "launcher")
                ),
                "portable %s did not explain package-manager/managed-install recovery"
                % action,
            )
            # The same already-open stdio session must still own the same live
            # daemon connection.  A stop-and-transparent-restart is not enough:
            # the existing pipe itself has to remain usable.
            require(
                server.tools_list(timeout=10),
                "portable %s drained the active MCP/daemon session before refusing"
                % action,
            )
            print("PASS: portable %s refused early without draining the session" % action)


def assert_untrusted_ancestor_acl_rejected(
    source_launcher, source_payload, env, work
):
    unsafe_ancestor = work / "unsafe cross-account ACL ancestor"
    launcher, _ = copy_portable_pair(
        source_launcher, source_payload, unsafe_ancestor / "bundle"
    )
    require(
        run([launcher, "--version"], env).returncode == 0,
        "portable launcher ACL control failed before the unsafe ACE was added",
    )
    extended_launcher = "\\\\?\\" + str(launcher.resolve())
    extended_result = run([extended_launcher, "--version"], env)
    require(
        extended_result.returncode == 0,
        "launcher rejected its documented extended DOS module path: %s"
        % output_text(extended_result)[-600:],
    )

    grant = run(
        ["icacls", unsafe_ancestor, "/grant", "*S-1-1-0:(M)"], env
    )
    require(
        grant.returncode == 0,
        "could not install native Everyone-modify ancestor fixture: %s"
        % output_text(grant)[-600:],
    )
    try:
        for candidate, spelling in (
            (launcher, "normal"),
            (extended_launcher, "extended DOS"),
        ):
            result = run([candidate, "--version"], env)
            diagnostic = output_text(result).lower()
            require(
                result.returncode != 0,
                "%s launcher path accepted an ancestor granting cross-account modify access"
                % spelling,
            )
            require(
                any(
                    word in diagnostic
                    for word in (
                        "unsafe",
                        "security",
                        "ownership",
                        "access",
                        "resolve",
                    )
                ),
                "%s launcher path unsafe-ancestor refusal was not explicit"
                % spelling,
            )
    finally:
        remove = run(
            ["icacls", unsafe_ancestor, "/remove:g", "*S-1-1-0"], env
        )
        require(
            remove.returncode == 0,
            "could not remove native Everyone-modify ancestor fixture",
        )
    for candidate, spelling in (
        (launcher, "normal"),
        (extended_launcher, "extended DOS"),
    ):
        require(
            run([candidate, "--version"], env).returncode == 0,
            "%s launcher path did not recover after unsafe ancestor ACE removal"
            % spelling,
        )
    print("PASS: launcher rejected an untrusted mutation ACE on an ancestor")


def assert_add_only_ancestor_acl_allowed(source_launcher, source_payload, env, work):
    ancestor = work / "cross-account add-only ancestor"
    launcher, portable_payload = copy_portable_pair(
        source_launcher, source_payload, ancestor / "bundle"
    )
    # The VM's hosted-runner policy can make this fresh copy
    # BUILTIN\Administrators-owned. Prove the trusted launcher context works
    # before the add-only ACE changes the ancestor's security descriptor.
    before_ace = run([launcher, "--version"], env)
    require(
        before_ace.returncode == 0,
        "launcher context failed before the add-only ACE was installed: %s"
        % output_text(before_ace)[-600:],
    )
    grant = run(["icacls", ancestor, "/grant", "*S-1-1-0:(AD)"], env)
    require(
        grant.returncode == 0,
        "could not install native Everyone-add-subdirectory ancestor fixture: %s"
        % output_text(grant)[-600:],
    )
    try:
        for candidate, spelling in (
            (launcher, "normal"),
            ("\\\\?\\" + str(launcher.resolve()), "extended DOS"),
        ):
            result = run([candidate, "--version"], env)
            require(
                result.returncode == 0,
                "%s launcher path treated sibling creation as replacement access: %s"
                % (spelling, output_text(result)[-600:]),
            )

        managed_dir = ancestor / "managed install under add-only ancestor"
        install = run(
            [
                portable_payload,
                "install",
                "--yes",
                "--force",
                "--skip-config",
                "--dir",
                managed_dir,
            ],
            env,
            timeout=60,
        )
        require(
            install.returncode == 0,
            "managed install rejected a standard add-subdirectory-only ancestor: %s"
            % output_text(install)[-800:],
        )
        managed_launcher = managed_dir / "codebase-memory-mcp.exe"
        require(managed_launcher.is_file(), "add-only managed install produced no launcher")
        uninstall = run([managed_launcher, "uninstall", "--yes"], env, timeout=180)
        require(
            uninstall.returncode == 0,
            "add-only managed install could not be uninstalled: %s"
            % output_text(uninstall)[-800:],
        )
    finally:
        remove = run(["icacls", ancestor, "/remove:g", "*S-1-1-0"], env)
        require(
            remove.returncode == 0,
            "could not remove native Everyone-add-subdirectory ancestor fixture",
        )
    print(
        "PASS: launcher and managed install allowed add-only sibling creation "
        "without weakening path integrity"
    )


def assert_targeted_ancestor_acl_rejected(path, launcher, right, description, env):
    grant = run(["icacls", path, "/grant", "*S-1-1-0:(%s)" % right], env)
    require(
        grant.returncode == 0,
        "could not install native Everyone-%s fixture: %s"
        % (description, output_text(grant)[-600:]),
    )
    try:
        for candidate, spelling in (
            (launcher, "normal"),
            ("\\\\?\\" + str(launcher.resolve()), "extended DOS"),
        ):
            result = run([candidate, "--version"], env)
            require(
                result.returncode != 0,
                "%s launcher accepted cross-account %s"
                % (spelling, description),
            )
    finally:
        remove = run(["icacls", path, "/remove:g", "*S-1-1-0"], env)
        require(
            remove.returncode == 0,
            "could not remove native Everyone-%s fixture" % description,
        )
    require(
        run([launcher, "--version"], env).returncode == 0,
        "launcher did not recover after the %s ACE was removed" % description,
    )


def assert_file_add_and_executable_parent_acl_rejected(
    source_launcher, source_payload, env, work
):
    file_add_ancestor = work / "cross-account file-add ancestor"
    launcher, _ = copy_portable_pair(
        source_launcher, source_payload, file_add_ancestor / "bundle"
    )
    assert_targeted_ancestor_acl_rejected(
        file_add_ancestor, launcher, "WD", "file creation on an intermediate ancestor", env
    )

    executable_parent = work / "cross-account executable parent"
    launcher, _ = copy_portable_pair(source_launcher, source_payload, executable_parent)
    assert_targeted_ancestor_acl_rejected(
        executable_parent,
        launcher,
        "AD",
        "subdirectory creation beside the executable",
        env,
    )
    print("PASS: launcher kept file-add and executable-parent ACL boundaries strict")


def process_entries():
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateToolhelp32Snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
    kernel32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    kernel32.Process32FirstW.argtypes = [wintypes.HANDLE, ctypes.POINTER(PROCESSENTRY32W)]
    kernel32.Process32FirstW.restype = wintypes.BOOL
    kernel32.Process32NextW.argtypes = [wintypes.HANDLE, ctypes.POINTER(PROCESSENTRY32W)]
    kernel32.Process32NextW.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL

    snapshot = kernel32.CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0)
    if snapshot == INVALID_HANDLE_VALUE:
        raise GuardFailure("CreateToolhelp32Snapshot failed")
    entries = []
    try:
        entry = PROCESSENTRY32W()
        entry.dwSize = ctypes.sizeof(PROCESSENTRY32W)
        ok = kernel32.Process32FirstW(snapshot, ctypes.byref(entry))
        while ok:
            entries.append(
                (int(entry.th32ProcessID), int(entry.th32ParentProcessID), entry.szExeFile)
            )
            entry.dwSize = ctypes.sizeof(PROCESSENTRY32W)
            ok = kernel32.Process32NextW(snapshot, ctypes.byref(entry))
    finally:
        kernel32.CloseHandle(snapshot)
    return entries


def process_is_alive(pid):
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel32.WaitForSingleObject.restype = wintypes.DWORD
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    handle = kernel32.OpenProcess(
        SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION, False, pid
    )
    if not handle:
        return False
    try:
        status = kernel32.WaitForSingleObject(handle, 0)
        return status == WAIT_TIMEOUT
    finally:
        kernel32.CloseHandle(handle)


def wait_for_payload_child(launcher_pid, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        children = [
            (pid, name)
            for pid, parent, name in process_entries()
            if parent == launcher_pid
            and name.lower().endswith("codebase-memory-mcp.payload.exe")
        ]
        if len(children) == 1:
            return children[0][0]
        if len(children) > 1:
            raise GuardFailure("launcher spawned more than one direct payload child")
        time.sleep(0.05)
    raise GuardFailure("launcher did not create its direct payload child")


def wait_for_process_exit(pid, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not process_is_alive(pid):
            return True
        time.sleep(0.05)
    return not process_is_alive(pid)


def descendant_pids(root_pid):
    entries = process_entries()
    descendants = set()
    frontier = {root_pid}
    while frontier:
        next_frontier = {
            pid
            for pid, parent, _ in entries
            if parent in frontier and pid not in descendants
        }
        descendants.update(next_frontier)
        frontier = next_frontier
    return descendants


def assert_launcher_death_contains_payload(launcher, env, cache):
    server = McpServer(str(launcher), cache_dir=str(cache), extra_env=env)
    try:
        server.start()
        server.initialize(timeout=30)
        child_pid = wait_for_payload_child(server.proc.pid)
        require(process_is_alive(child_pid), "payload child exited before crash probe")
        server.proc.kill()
        server.proc.wait(timeout=10)
        require(
            wait_for_process_exit(child_pid, timeout=10),
            "killing the permanent launcher orphaned payload pid %d" % child_pid,
        )
        print("PASS: abrupt launcher death contained its payload child")
    finally:
        server.close()


def assert_immediate_parent_death_contains_launcher_tree(launcher, env, work):
    pid_file = work / "launcher-parent-probe.pid"
    wrapper = subprocess.Popen(
        [
            sys.executable,
            str(pathlib.Path(__file__).resolve()),
            "--launcher-parent-probe",
            str(launcher),
            str(pid_file),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
    )
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not pid_file.exists():
            if wrapper.poll() is not None:
                raise GuardFailure("parent-probe wrapper exited before publishing launcher PID")
            time.sleep(0.05)
        require(pid_file.exists(), "parent-probe wrapper did not publish launcher PID")
        launcher_pid = int(pid_file.read_text(encoding="ascii").strip())
        payload_pid = wait_for_payload_child(launcher_pid, timeout=10)
        tracked = {launcher_pid, payload_pid} | descendant_pids(launcher_pid)
        require(process_is_alive(launcher_pid), "launcher exited before parent-death probe")
        require(process_is_alive(payload_pid), "payload exited before parent-death probe")

        # Kill only the wrapper. The test process intentionally keeps the
        # wrapper stdin pipe open, so MCP EOF cannot explain child teardown.
        wrapper.kill()
        wrapper.wait(timeout=10)
        require(
            wait_for_process_exit(launcher_pid, timeout=10),
            "killing only the wrapper left launcher pid %d alive" % launcher_pid,
        )
        survivors = [pid for pid in tracked if process_is_alive(pid)]
        descendant_deadline = time.monotonic() + 10
        while survivors and time.monotonic() < descendant_deadline:
            time.sleep(0.05)
            survivors = [pid for pid in survivors if process_is_alive(pid)]
        require(not survivors, "wrapper death left launcher descendants alive: %s" % survivors)
        print("PASS: immediate parent death terminated launcher and payload session tree")
    finally:
        if wrapper.poll() is None:
            wrapper.kill()
            wrapper.wait(timeout=10)
        if wrapper.stdin:
            wrapper.stdin.close()


def assert_launcher_relay(launcher, payload, env, cache):
    direct_version = run([payload, "--version"], env)
    launched_version = run([launcher, "--version"], env)
    require(direct_version.returncode == 0, "managed payload --version failed")
    require(
        launched_version.returncode == direct_version.returncode,
        "launcher did not propagate the payload's success exit status exactly",
    )
    require(
        launched_version.stdout == direct_version.stdout
        and launched_version.stderr == direct_version.stderr,
        "launcher did not relay --version stdout/stderr byte-for-byte",
    )

    # A missing CLI tool has a stable non-zero product exit.  Compare it with
    # the same generation payload rather than baking the numeric code into the
    # launcher contract.
    direct_error = run([payload, "cli"], env)
    launched_error = run([launcher, "cli"], env)
    require(direct_error.returncode != 0, "CLI error control unexpectedly succeeded")
    require(
        launched_error.returncode == direct_error.returncode,
        "launcher changed the payload's non-zero exit status",
    )
    require(
        launched_error.stdout == direct_error.stdout
        and launched_error.stderr == direct_error.stderr,
        "launcher did not relay failing CLI stdout/stderr byte-for-byte",
    )

    with McpServer(str(launcher), cache_dir=str(cache), extra_env=env) as server:
        init = server.initialize(timeout=30)
        require("result" in init, "launcher did not relay MCP initialize response")
        require(server.tools_list(timeout=30), "launcher did not relay MCP tools/list")
    print("PASS: launcher relayed MCP stdio and exact child exit/status streams")


def assert_current_fail_closed(launcher, current, original, env):
    # cbm writes current-v1 with FILE_ATTRIBUTE_HIDDEN; Python's write_bytes
    # (CREATE_ALWAYS) cannot overwrite a hidden file (WinError 5 -> Errno 13),
    # so clear the attribute before corrupting it. The launcher reads current-v1
    # regardless of the attribute, so this does not affect the behavior tested.
    ctypes.windll.kernel32.SetFileAttributesW(ctypes.c_wchar_p(str(current)), 0x80)
    incompatible = bytearray(original)
    struct.pack_into("<II", incompatible, CURRENT_ABI_MIN_OFFSET, 0xFFFFFFFF, 0xFFFFFFFF)
    current.write_bytes(incompatible)
    result = run([launcher, "--version"], env)
    diagnostic = output_text(result).lower()
    require(result.returncode != 0, "launcher accepted an incompatible current-v1 ABI")
    require("abi" in diagnostic or "incompatible" in diagnostic, "ABI refusal was not explicit")

    corrupt = bytearray(original)
    corrupt[: len(CURRENT_MAGIC)] = b"\0" * len(CURRENT_MAGIC)
    current.write_bytes(corrupt)
    result = run([launcher, "--version"], env)
    diagnostic = output_text(result).lower()
    require(result.returncode != 0, "launcher accepted a corrupted current record")
    require(
        any(word in diagnostic for word in ("current", "corrupt", "invalid", "state")),
        "corrupted-current failure was not explicit",
    )
    current.write_bytes(original)
    require(run([launcher, "--version"], env).returncode == 0, "restored current-v1 stopped working")
    print("PASS: current-v1 compatibility succeeded and corrupt/incompatible state failed hard")


def assert_managed_update(
    source_launcher, source_payload, launcher, managed_dir, env, work
):
    release = work / "release"
    release.mkdir()
    updated_payload = release / "updated-payload.exe"
    updated_payload.write_bytes(
        source_payload.read_bytes() + b"\0CBM runnable update generation fixture\n"
    )
    require(
        run([updated_payload, "--version"], env).returncode == 0,
        "PE-overlay update payload fixture is not runnable",
    )
    machine = platform.machine().lower()
    arch = "arm64" if machine in ("arm64", "aarch64") else "amd64"
    asset_name = "codebase-memory-mcp-windows-%s.zip" % arch
    asset = release / asset_name
    with zipfile.ZipFile(asset, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.write(source_launcher, "codebase-memory-mcp.exe")
        archive.write(updated_payload, "codebase-memory-mcp.payload.exe")
        archive.writestr("LICENSE", "test license\n")
        archive.writestr("install.ps1", "# test installer\n")
        archive.writestr("THIRD_PARTY_NOTICES.md", "test notices\n")
    digest = hashlib.sha256(asset.read_bytes()).hexdigest()
    (release / "checksums.txt").write_text(
        "%s  %s\n" % (digest, asset_name), encoding="ascii"
    )

    update_env = dict(env)
    update_env["CBM_DOWNLOAD_URL"] = release.resolve().as_uri()
    _, _, old_payload = find_and_validate_current(managed_dir)
    old_generation = old_payload.parent
    orphan_bytes = source_payload.read_bytes() + b"\0CBM generation prune fixture\n"
    orphan_digest = hashlib.sha256(orphan_bytes).hexdigest()
    orphan_directory = managed_dir / ".cbm" / "generations" / orphan_digest
    orphan_directory.mkdir()
    (orphan_directory / "codebase-memory-mcp.payload.exe").write_bytes(orphan_bytes)
    shutil.copy2(source_launcher, orphan_directory / "codebase-memory-mcp.exe")
    # A genuine orphan generation is cbm-created and current-user-owned; this
    # hand-built one defaults to the Administrators group on runner images, so
    # the update's owner-strict generation pruning (correctly) refuses it. Stamp
    # it so the prune reaches the same not-referenced verdict it would in reality.
    stamp_fixture_owner_current(orphan_directory, env)
    result = run(
        [launcher, "update", "--force", "--standard", "--yes"],
        update_env,
        timeout=90,
    )
    require(
        result.returncode == 0,
        "managed update failed: %s" % output_text(result)[-1200:],
    )
    require(launcher.is_file(), "managed update removed the canonical launcher")
    _, _, generation_payload = find_and_validate_current(managed_dir)
    require(
        run([launcher, "--version"], env).returncode == 0,
        "managed update returned with a non-runnable launcher/current pair",
    )
    require(
        run([generation_payload, "--version"], env).returncode == 0,
        "managed update returned with a non-runnable generation payload",
    )
    require(not orphan_directory.exists(), "successful update did not prune orphan generation")
    require(
        old_generation.exists(),
        "update deleted the old generation while its supervising launcher was still mapped",
    )
    require(
        {path.name for path in old_generation.iterdir()}
        == {"codebase-memory-mcp.payload.exe", "codebase-memory-mcp.exe"}
        and (old_generation / "codebase-memory-mcp.exe").stat().st_nlink == 1,
        "deferred mapped old generation was not retained as an exact safe pair",
    )

    # The first updater has now exited, so a same-release activation can
    # prune the no-longer-mapped old backing without publishing another
    # generation.
    second = run(
        [launcher, "update", "--force", "--standard", "--yes"],
        update_env,
        timeout=90,
    )
    require(
        second.returncode == 0,
        "second managed update failed to prune deferred generation: %s"
        % output_text(second)[-1200:],
    )
    _, _, generation_payload = find_and_validate_current(managed_dir)
    require(not old_generation.exists(), "later activation did not prune unmapped old generation")
    generation_directories = [
        path
        for path in (managed_dir / ".cbm" / "generations").iterdir()
        if path.is_dir()
    ]
    require(
        len(generation_directories) == 1
        and generation_directories[0].name == generation_payload.parent.name,
        "successful update did not leave exactly the current generation",
    )
    print(
        "PASS: managed update retained mapped old backing, then pruned it on the next activation"
    )


def assert_managed_update_rejects_unrunnable_launcher_before_drain(
    source_payload, launcher, managed_dir, env, cache, work
):
    release = work / "release-unrunnable-launcher"
    release.mkdir()
    machine = platform.machine().lower()
    arch = "arm64" if machine in ("arm64", "aarch64") else "amd64"
    asset_name = "codebase-memory-mcp-windows-%s.zip" % arch
    asset = release / asset_name
    with zipfile.ZipFile(asset, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("codebase-memory-mcp.exe", b"MZ-not-a-runnable-launcher")
        archive.write(source_payload, "codebase-memory-mcp.payload.exe")
        archive.writestr("LICENSE", "test license\n")
        archive.writestr("install.ps1", "# test installer\n")
        archive.writestr("THIRD_PARTY_NOTICES.md", "test notices\n")
    digest = hashlib.sha256(asset.read_bytes()).hexdigest()
    (release / "checksums.txt").write_text(
        "%s  %s\n" % (digest, asset_name), encoding="ascii"
    )
    update_env = dict(env)
    update_env["CBM_DOWNLOAD_URL"] = release.resolve().as_uri()
    current, current_before, _ = find_and_validate_current(managed_dir)
    launcher_before = sha256_file(launcher)

    with McpServer(str(launcher), cache_dir=str(cache), extra_env=env) as server:
        server.initialize(timeout=30)
        result = run(
            [launcher, "update", "--force", "--standard", "--yes"],
            update_env,
            timeout=60,
        )
        diagnostic = output_text(result).lower()
        require(result.returncode != 0, "unrunnable launcher update succeeded")
        require(
            any(word in diagnostic for word in ("launcher", "candidate", "runnable", "capability")),
            "unrunnable launcher rejection was not explicit",
        )
        require(
            server.tools_list(timeout=10),
            "unrunnable launcher candidate drained the active managed session",
        )

    require(sha256_file(launcher) == launcher_before, "failed update replaced the launcher")
    require(current.read_bytes() == current_before, "failed update changed current-v1")
    print("PASS: unrunnable update launcher failed before session drain")


def assert_managed_update_rejects_cross_abi_pair_before_drain(
    abi_mismatch_launcher, source_payload, launcher, managed_dir, env, cache, work
):
    release = work / "release-abi-mismatch"
    release.mkdir()
    machine = platform.machine().lower()
    arch = "arm64" if machine in ("arm64", "aarch64") else "amd64"
    asset_name = "codebase-memory-mcp-windows-%s.zip" % arch
    asset = release / asset_name
    with zipfile.ZipFile(asset, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.write(abi_mismatch_launcher, "codebase-memory-mcp.exe")
        archive.write(source_payload, "codebase-memory-mcp.payload.exe")
        archive.writestr("LICENSE", "test license\n")
        archive.writestr("install.ps1", "# test installer\n")
        archive.writestr("THIRD_PARTY_NOTICES.md", "test notices\n")
    digest = hashlib.sha256(asset.read_bytes()).hexdigest()
    (release / "checksums.txt").write_text(
        "%s  %s\n" % (digest, asset_name), encoding="ascii"
    )
    update_env = dict(env)
    update_env["CBM_DOWNLOAD_URL"] = release.resolve().as_uri()
    current, current_before, _ = find_and_validate_current(managed_dir)
    launcher_before = sha256_file(launcher)
    generations_before = sorted(
        str(path.relative_to(managed_dir))
        for path in (managed_dir / ".cbm" / "generations").rglob("*")
    )

    with McpServer(str(launcher), cache_dir=str(cache), extra_env=env) as server:
        server.initialize(timeout=30)
        result = run(
            [launcher, "update", "--force", "--standard", "--yes"],
            update_env,
            timeout=60,
        )
        diagnostic = output_text(result).lower()
        require(result.returncode != 0, "cross-ABI launcher/payload update succeeded")
        require(
            any(word in diagnostic for word in ("abi", "descriptor", "bridge", "incompatible")),
            "cross-ABI update refusal did not explain compatibility failure",
        )
        require(
            server.tools_list(timeout=10),
            "cross-ABI candidate drained the active managed session",
        )

    generations_after = sorted(
        str(path.relative_to(managed_dir))
        for path in (managed_dir / ".cbm" / "generations").rglob("*")
    )
    require(sha256_file(launcher) == launcher_before, "cross-ABI update replaced launcher")
    require(current.read_bytes() == current_before, "cross-ABI update changed current-v1")
    require(generations_after == generations_before, "cross-ABI update published a generation")
    print("PASS: incompatible future launcher ABI failed hard before session drain")


def assert_failed_update_rolls_back_new_generation(
    source_launcher, source_payload, launcher, managed_dir, env, work
):
    release = work / "release-generation-rollback"
    release.mkdir()
    mutated_payload = release / "mutated.payload.exe"
    mutated_payload.write_bytes(
        source_payload.read_bytes() + b"\0CBM generation rollback fixture\n"
    )
    require(
        run([mutated_payload, "--version"], env).returncode == 0,
        "PE overlay payload fixture is not runnable",
    )
    mutated_digest = sha256_file(mutated_payload)
    machine = platform.machine().lower()
    arch = "arm64" if machine in ("arm64", "aarch64") else "amd64"
    asset_name = "codebase-memory-mcp-windows-%s.zip" % arch
    asset = release / asset_name
    with zipfile.ZipFile(asset, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.write(source_launcher, "codebase-memory-mcp.exe")
        archive.write(mutated_payload, "codebase-memory-mcp.payload.exe")
        archive.writestr("LICENSE", "test license\n")
        archive.writestr("install.ps1", "# test installer\n")
        archive.writestr("THIRD_PARTY_NOTICES.md", "test notices\n")
    digest = hashlib.sha256(asset.read_bytes()).hexdigest()
    (release / "checksums.txt").write_text(
        "%s  %s\n" % (digest, asset_name), encoding="ascii"
    )
    update_env = dict(env)
    update_env["CBM_DOWNLOAD_URL"] = release.resolve().as_uri()
    current, current_before, _ = find_and_validate_current(managed_dir)
    launcher_before = sha256_file(launcher)

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    kernel32.CreateFileW.restype = wintypes.HANDLE
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    held_launcher = kernel32.CreateFileW(
        str(launcher), 0x80000000, 0x00000001, None, 3, 0x80, None
    )
    require(held_launcher != INVALID_HANDLE_VALUE, "could not lock canonical launcher fixture")
    try:
        result = run(
            [launcher, "update", "--force", "--standard", "--yes"],
            update_env,
            timeout=90,
        )
    finally:
        kernel32.CloseHandle(held_launcher)
    require(result.returncode != 0, "forced post-publication launcher failure succeeded")
    require(sha256_file(launcher) == launcher_before, "failed update changed launcher")
    require(current.read_bytes() == current_before, "failed update changed current-v1")
    require(
        not (managed_dir / ".cbm" / "generations" / mutated_digest).exists(),
        "failed update orphaned its newly published generation",
    )
    print("PASS: post-publication update failure rolled back its new generation")


def assert_managed_update_dry_run_skips_capability_probe(
    launcher, managed_dir, env
):
    dry_env = dict(env)
    dry_env["CBM_TEST_WINDOWS_LAUNCHER_CAPABILITY_PROBE"] = "fail"
    current, current_before, _ = find_and_validate_current(managed_dir)
    launcher_before = sha256_file(launcher)
    result = run(
        [launcher, "update", "--dry-run", "--force", "--standard", "--yes"],
        dry_env,
        timeout=30,
    )
    require(
        result.returncode == 0,
        "managed update dry-run invoked the forced-failing capability probe: %s"
        % output_text(result)[-800:],
    )
    require(sha256_file(launcher) == launcher_before, "update dry-run changed launcher")
    require(current.read_bytes() == current_before, "update dry-run changed current-v1")
    leftovers = [
        path
        for path in managed_dir.iterdir()
        if path.name.startswith(".cbm-launcher-probe-")
        or path.name == ".cbm-probe-path-check"
    ]
    require(not leftovers, "update dry-run left capability-probe artifacts: %s" % leftovers)
    print("PASS: managed update dry-run skipped mapped-image capability mutation")


def assert_capability_probe_fail_hard(
    source_launcher, source_payload, launcher, env, cache, work
):
    _, probe_payload = copy_portable_pair(
        source_launcher, source_payload, work / "portable-probe"
    )
    rejected_dir = work / "probe-rejected_日本語"
    rejected_env = dict(env)
    # Deterministic native fault injection for the exact-volume launcher
    # capability probe.  Production must treat this solely as a probe failure;
    # it must not bypass or weaken the real NTFS checks.
    rejected_env["CBM_TEST_WINDOWS_LAUNCHER_CAPABILITY_PROBE"] = "fail"

    with McpServer(str(launcher), cache_dir=str(cache), extra_env=env) as server:
        server.initialize(timeout=30)
        result = run(
            [
                probe_payload,
                "install",
                "--yes",
                "--force",
                "--skip-config",
                "--dir",
                rejected_dir,
            ],
            rejected_env,
            timeout=30,
        )
        diagnostic = output_text(result).lower()
        require(result.returncode != 0, "forced launcher capability-probe failure succeeded")
        require(
            any(word in diagnostic for word in ("capability", "ntfs", "atomic", "replace")),
            "launcher capability-probe failure was not explicit",
        )
        leftover_files = [p for p in rejected_dir.rglob("*") if p.is_file()]
        require(not leftover_files, "failed capability probe left managed-install files behind")
        require(
            server.tools_list(timeout=10),
            "capability-probe failure drained the already-active managed session",
        )
    print("PASS: unsupported launcher capability failed hard before session drain")


def install_managed(portable_payload, managed_dir, env, timeout=180):
    return run(
        [
            portable_payload,
            "install",
            "--yes",
            "--force",
            "--skip-config",
            "--dir",
            managed_dir,
        ],
        env,
        timeout=timeout,
    )


def stamp_fixture_owner_current(managed_dir, env):
    """Give a hand-built managed fixture the current-user ownership a genuine
    cbm install would stamp (windows_stamp_current_owner_private).  Runner images
    default newly created files to the Administrators group, but the launcher's
    exact-owner validators — the recovery-install path and the capability
    probe's secure open of the target directory — (correctly) require the
    current user, so an Administrators-owned fixture is an unfaithful simulation
    of a real interrupted/partial install and must be re-owned."""
    result = run(
        ["icacls", str(managed_dir), "/setowner", os.environ.get("USERNAME", ""),
         "/T", "/C", "/Q"],
        env,
    )
    require(
        result.returncode == 0,
        "could not stamp current-user ownership on the managed fixture: %s"
        % output_text(result)[-400:],
    )


def assert_partial_generation_recovery(
    source_launcher, source_payload, portable_payload, env, work
):
    # A current-v1 record is the commit point. Once it references a generation,
    # either missing half is corruption and install must fail without rewriting
    # or deleting the valid half.
    referenced = work / "referenced partial generation"
    _, referenced_payload, referenced_backing = generation_paths(
        referenced, source_payload
    )
    referenced_payload.parent.mkdir(parents=True)
    shutil.copy2(source_payload, referenced_payload)
    current = referenced / ".cbm" / "current-v1"
    current_bytes = current_v1_record(source_payload)
    current.write_bytes(current_bytes)
    payload_bytes = referenced_payload.read_bytes()
    rejected = install_managed(portable_payload, referenced, env, timeout=30)
    require(rejected.returncode != 0, "install repaired a current-referenced partial generation")
    require(current.read_bytes() == current_bytes, "failed partial repair changed current-v1")
    require(
        referenced_payload.read_bytes() == payload_bytes and not referenced_backing.exists(),
        "failed current-referenced repair modified or deleted the valid half",
    )

    # A failed unreferenced repair owns only the leaf it successfully creates.
    # A conflicting non-file at the missing leaf forces failure and the valid
    # payload half must remain byte-identical.
    blocked = work / "blocked partial generation repair"
    _, blocked_payload, blocked_backing = generation_paths(blocked, source_payload)
    blocked_payload.parent.mkdir(parents=True)
    shutil.copy2(source_payload, blocked_payload)
    blocked_bytes = blocked_payload.read_bytes()
    blocked_backing.mkdir()
    failed = install_managed(portable_payload, blocked, env, timeout=30)
    require(failed.returncode != 0, "conflicting partial generation repair succeeded")
    require(
        blocked_payload.read_bytes() == blocked_bytes and blocked_backing.is_dir(),
        "failed repair removed a pre-existing valid generation half",
    )

    # A launcher-first crash can leave the authenticated complete generation
    # published at canonical before current-v1 is committed.  The exact-two
    # hardlink is intentionally not runnable as a portable launcher; install
    # must recognize and finish this one candidate without replacing either
    # immutable half.
    interrupted = work / "interrupted launcher-first initial install"
    _, interrupted_payload, interrupted_backing = generation_paths(
        interrupted, source_payload
    )
    interrupted_backing.parent.mkdir(parents=True)
    shutil.copy2(source_payload, interrupted_payload)
    shutil.copy2(source_launcher, interrupted_backing)
    interrupted_launcher = interrupted / "codebase-memory-mcp.exe"
    os.link(interrupted_backing, interrupted_launcher)
    payload_bytes = interrupted_payload.read_bytes()
    backing_bytes = interrupted_backing.read_bytes()
    require(
        interrupted_launcher.stat().st_nlink == 2
        and not (interrupted / ".cbm" / "current-v1").exists(),
        "interrupted launcher-first fixture is not exact pre-current state",
    )
    stamp_fixture_owner_current(interrupted, env)
    recovered = install_managed(portable_payload, interrupted, env)
    require(
        recovered.returncode == 0,
        "interrupted initial install recovery failed: %s"
        % output_text(recovered)[-800:],
    )
    require(
        interrupted_payload.read_bytes() == payload_bytes
        and interrupted_backing.read_bytes() == backing_bytes,
        "interrupted install recovery replaced an authenticated generation half",
    )
    find_and_validate_current(interrupted)
    assert_managed_uninstall(interrupted_launcher, interrupted, env)

    # The inverse safe partial (correct launcher backing, missing payload) is
    # repaired in place, then canonical is published as its second hardlink.
    launcher_only = work / "launcher-only partial generation"
    _, launcher_only_payload, launcher_only_backing = generation_paths(
        launcher_only, source_payload
    )
    launcher_only_backing.parent.mkdir(parents=True)
    shutil.copy2(source_launcher, launcher_only_backing)
    backing_bytes = launcher_only_backing.read_bytes()
    stamp_fixture_owner_current(launcher_only, env)
    repaired = install_managed(portable_payload, launcher_only, env)
    require(
        repaired.returncode == 0,
        "launcher-only generation repair failed: %s" % output_text(repaired)[-800:],
    )
    require(
        launcher_only_backing.read_bytes() == backing_bytes
        and launcher_only_payload.is_file(),
        "launcher-only repair replaced its valid half instead of adding the missing payload",
    )
    find_and_validate_current(launcher_only)
    assert_managed_uninstall(
        launcher_only / "codebase-memory-mcp.exe", launcher_only, env
    )
    print(
        "PASS: generation repair is additive, launcher-first recovery completes, "
        "and current-v1 corruption fails closed"
    )


def assert_managed_uninstall(launcher, managed_dir, env):
    result, observed, digest = run_uninstall_observing_retirement(
        launcher, managed_dir, env
    )
    require(result.returncode == 0, "managed uninstall failed: %s" % output_text(result)[-800:])
    require(not launcher.exists(), "managed uninstall returned before launcher removal")
    require(
        observed,
        "uninstall never exposed the authenticated SHA/PID retired-state commit",
    )
    deadline = time.monotonic() + 30
    while retired_state_siblings(managed_dir, digest) and time.monotonic() < deadline:
        time.sleep(0.05)
    require(
        not retired_state_siblings(managed_dir, digest),
        "launcher-owned detached cleanup did not remove the retired state after launcher exit",
    )
    require(
        not (managed_dir / ".cbm").exists(),
        "uninstall unexpectedly recreated the active .cbm state",
    )
    print("PASS: uninstall committed canonical unlink and cleaned only its retired SHA/PID state")


def assert_failed_uninstall_restores_state(launcher, managed_dir, env):
    current, current_before, _ = find_and_validate_current(managed_dir)
    retired_before = set(retired_state_siblings(managed_dir))
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    kernel32.CreateFileW.restype = wintypes.HANDLE
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    held_launcher = kernel32.CreateFileW(
        str(launcher), 0x80000000, 0x00000001, None, 3, 0x80, None
    )
    require(held_launcher != INVALID_HANDLE_VALUE, "could not lock uninstall launcher fixture")
    try:
        result = run([launcher, "uninstall", "--yes"], env, timeout=60)
    finally:
        kernel32.CloseHandle(held_launcher)
    require(result.returncode != 0, "share-blocked uninstall unexpectedly succeeded")
    require(current.read_bytes() == current_before, "failed uninstall changed current-v1")
    find_and_validate_current(managed_dir)
    require(
        set(retired_state_siblings(managed_dir)) == retired_before,
        "failed canonical unlink did not restore its retired state directory",
    )
    require(
        run([launcher, "--version"], env).returncode == 0,
        "failed uninstall did not leave a runnable launcher/current pair",
    )
    print("PASS: failed final unlink restored active state only for the original launcher identity")


def assert_uninstall_immediate_reinstall_safe(
    launcher, managed_dir, portable_payload, env
):
    result, observed, digest = run_uninstall_observing_retirement(
        launcher, managed_dir, env
    )
    require(result.returncode == 0, "pre-reinstall uninstall failed: %s" % output_text(result)[-800:])
    require(observed, "immediate-reinstall test never observed retired state")
    require(not launcher.exists(), "uninstall returned before canonical unlink")

    reinstall = install_managed(portable_payload, managed_dir, env)
    require(
        reinstall.returncode == 0,
        "immediate managed reinstall failed: %s" % output_text(reinstall)[-800:],
    )
    new_current, new_current_bytes, _ = find_and_validate_current(managed_dir)
    deadline = time.monotonic() + 30
    while retired_state_siblings(managed_dir, digest) and time.monotonic() < deadline:
        time.sleep(0.05)
    require(
        not retired_state_siblings(managed_dir, digest),
        "detached cleanup did not remove its old retired state",
    )
    current_after, current_after_bytes, _ = find_and_validate_current(managed_dir)
    require(
        current_after == new_current and current_after_bytes == new_current_bytes,
        "old detached cleanup deleted or replaced the immediate reinstall's new .cbm",
    )
    require(
        run([launcher, "--version"], env).returncode == 0,
        "old detached cleanup broke the immediately reinstalled launcher",
    )
    print("PASS: retired cleanup preserved an immediate concurrent-generation reinstall")
    assert_managed_uninstall(launcher, managed_dir, env)


def launcher_parent_probe_role():
    launcher = pathlib.Path(sys.argv[2]).resolve()
    pid_file = pathlib.Path(sys.argv[3]).resolve()
    child = subprocess.Popen(
        [str(launcher)],
        stdin=None,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=os.environ.copy(),
    )
    pid_file.write_text(str(child.pid), encoding="ascii")
    return child.wait()


def main():
    if os.name != "nt":
        print("PRECONDITION: native Windows is required")
        return 2
    if len(sys.argv) == 4 and sys.argv[1] == "--launcher-parent-probe":
        return launcher_parent_probe_role()
    if len(sys.argv) != 4:
        print(
            "usage: python test_windows_launcher.py "
            "<launcher.exe> <payload.exe> <abi-mismatch-launcher.exe>"
        )
        return 2

    source_launcher = pathlib.Path(sys.argv[1]).resolve()
    source_payload = pathlib.Path(sys.argv[2]).resolve()
    abi_mismatch_launcher = pathlib.Path(sys.argv[3]).resolve()
    if not source_launcher.is_file():
        print("PRECONDITION: launcher not found: %s" % source_launcher)
        return 2
    if not source_payload.is_file():
        print("PRECONDITION: payload not found: %s" % source_payload)
        return 2
    if not abi_mismatch_launcher.is_file():
        print("PRECONDITION: ABI mismatch launcher not found: %s" % abi_mismatch_launcher)
        return 2
    if source_launcher.samefile(source_payload):
        print("PRECONDITION: launcher and payload must be distinct executables")
        return 2

    work = pathlib.Path(tempfile.mkdtemp(prefix="cbm_win_launcher_"))
    path_snapshot = path_registry_snapshot()
    try:
        env, cache = isolated_environment(work)
        assert_release_descriptor(source_launcher, source_payload, env, cache)
        assert_portable_mutations_refuse(source_payload, env, cache, work)
        assert_add_only_ancestor_acl_allowed(
            source_launcher, source_payload, env, work
        )
        assert_file_add_and_executable_parent_acl_rejected(
            source_launcher, source_payload, env, work
        )
        assert_untrusted_ancestor_acl_rejected(
            source_launcher, source_payload, env, work
        )

        _, portable_payload = copy_portable_pair(
            source_launcher, source_payload, work / "portable-install"
        )
        assert_partial_generation_recovery(
            source_launcher, source_payload, portable_payload, env, work
        )
        managed_dir = work / "managed café_日本語 with spaces"
        # The other safe partial (correct payload, missing launcher backing) is
        # the main managed fixture. Install must add only the missing half and
        # publish canonical as its second link.
        _, payload_half, backing_half = generation_paths(managed_dir, source_payload)
        payload_half.parent.mkdir(parents=True)
        shutil.copy2(source_payload, payload_half)
        payload_half_bytes = payload_half.read_bytes()
        stamp_fixture_owner_current(managed_dir, env)
        install = install_managed(portable_payload, managed_dir, env)
        require(install.returncode == 0, "managed install failed: %s" % output_text(install)[-800:])
        require(
            payload_half.read_bytes() == payload_half_bytes and backing_half.is_file(),
            "payload-only repair replaced its valid half instead of adding the launcher",
        )

        launcher = managed_dir / "codebase-memory-mcp.exe"
        require(launcher.is_file(), "managed custom path has no canonical launcher")
        current, current_bytes, generation_payload = find_and_validate_current(managed_dir)
        print(
            "PASS: payload-only generation recovered into strict hardlink/current-v1 layout"
        )

        assert_launcher_relay(launcher, generation_payload, env, cache)
        assert_current_fail_closed(launcher, current, current_bytes, env)
        assert_managed_update_dry_run_skips_capability_probe(
            launcher, managed_dir, env
        )
        assert_managed_update_rejects_unrunnable_launcher_before_drain(
            source_payload, launcher, managed_dir, env, cache, work
        )
        assert_managed_update_rejects_cross_abi_pair_before_drain(
            abi_mismatch_launcher,
            source_payload,
            launcher,
            managed_dir,
            env,
            cache,
            work,
        )
        assert_failed_update_rolls_back_new_generation(
            source_launcher,
            source_payload,
            launcher,
            managed_dir,
            env,
            work,
        )
        assert_managed_update(
            source_launcher,
            source_payload,
            launcher,
            managed_dir,
            env,
            work,
        )
        assert_launcher_death_contains_payload(launcher, env, cache)
        assert_immediate_parent_death_contains_launcher_tree(
            launcher, env, work
        )
        assert_capability_probe_fail_hard(
            source_launcher, source_payload, launcher, env, cache, work
        )
        assert_failed_uninstall_restores_state(launcher, managed_dir, env)
        assert_uninstall_immediate_reinstall_safe(
            launcher, managed_dir, portable_payload, env
        )
        print("\nGREEN: permanent Windows launcher contract honored.")
        return 0
    except (GuardFailure, McpError, OSError, subprocess.SubprocessError) as exc:
        print("\nRED: %s" % exc)
        return 1
    finally:
        try:
            path_registry_restore(path_snapshot)
        except OSError as exc:
            print("CLEANUP WARNING: could not restore HKCU Environment\\Path: %s" % exc)
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
