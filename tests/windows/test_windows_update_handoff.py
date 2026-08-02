"""GREEN native-Windows guard for the `update` -> install.ps1 handoff.

Windows ships ONE binary, exactly like Linux and macOS: ``codebase-memory-mcp.exe``.

There used to be a second, permanently resident launcher stub whose only job
was to swap the product binary out from under itself, because a running .exe
cannot replace its own image on Windows.  Defender's ML scored that stub
Trojan:Win32/Wacatac.B!ml on x64 no matter what the build changed (bcrypt-free,
stripped, versioned and resource-free variants were all flagged), while the
product binary itself scans clean everywhere.  So the swap moved OUT of the
process into install.ps1, which runs while CBM is NOT running.

This guard asserts the replacement contract on real native Windows:

* ``update`` exits 0 and prints the exact install.ps1 command.
* ``update`` NEVER replaces the running image in-process — the executable's
  bytes are unchanged, and no launcher/payload sibling appears next to it.
* ``update`` refuses to reach the network first: it hands off before it
  consults CBM_DOWNLOAD_URL, so it stays fast even when that URL is a black
  hole.
* ``update`` does not disturb an already-open MCP/daemon session.

A regression here means the in-process self-update — and therefore the
AV-flagged launcher stub — came back.

Exit code: 0 == contract honored, 1 == regression, 2 == precondition failure.

Usage:
    python test_windows_update_handoff.py <codebase-memory-mcp.exe>
"""

import hashlib
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mcp_stdio import McpError, McpServer  # noqa: E402


class GuardFailure(Exception):
    pass


def require(condition, message):
    if not condition:
        raise GuardFailure(message)


def sha256_file(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


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


def copy_binary(source, directory):
    directory.mkdir(parents=True, exist_ok=True)
    binary = directory / "codebase-memory-mcp.exe"
    shutil.copy2(source, binary)
    return binary


def assert_update_hands_off_to_install_script(source, env, work):
    binary = copy_binary(source, work / "update-handoff")
    before = sha256_file(binary)
    command_env = dict(env)
    # If the handoff regresses into a real in-process update, keep its
    # unintended network path deterministic and fast: a correct implementation
    # never consults this URL.
    command_env["CBM_DOWNLOAD_URL"] = "https://127.0.0.1:1"

    # Warm the freshly-copied cold binary first: first-touch antivirus scanning
    # of the just-written image inflates process load time, and the handoff
    # itself is a fast STATELESS local print (no daemon IPC, no download) whose
    # timing is what this guard measures. The warm-up behaves identically.
    run([binary, "update", "--yes", "--standard"], command_env, timeout=20)
    started = time.monotonic()
    result = run([binary, "update", "--yes", "--standard"], command_env, timeout=20)
    elapsed = time.monotonic() - started
    diagnostic = output_text(result)
    lowered = diagnostic.lower()

    require(
        result.returncode == 0,
        "update exited %s; the Windows handoff must succeed: %s"
        % (result.returncode, diagnostic[-800:]),
    )
    require(
        "install.ps1" in lowered,
        "update did not print the install.ps1 command: %s" % diagnostic[-800:],
    )
    require(
        "powershell" in lowered,
        "update did not print a runnable PowerShell command: %s" % diagnostic[-800:],
    )
    require(
        elapsed < 8.0,
        "update took %.1fs — it must hand off before any network I/O" % elapsed,
    )
    require(
        sha256_file(binary) == before,
        "update replaced the running image in-process (a running .exe cannot "
        "replace itself; that is exactly what the removed launcher stub was for)",
    )
    require(
        not (binary.parent / "codebase-memory-mcp.payload.exe").exists(),
        "update recreated a launcher/payload pair beside the binary",
    )
    require(
        not list(binary.parent.glob("*launcher*")),
        "update produced a launcher artifact beside the binary",
    )
    print("PASS: update handed off to install.ps1 without touching its own image")


def assert_update_does_not_drain_active_session(source, env, cache, work):
    binary = copy_binary(source, work / "update-session")
    with McpServer(str(binary), cache_dir=str(cache), extra_env=env) as server:
        server.initialize(timeout=30)
        require(server.tools_list(timeout=30), "MCP control session has no tools")

        command = copy_binary(source, work / "update-session-command")
        command_env = dict(env)
        command_env["CBM_DOWNLOAD_URL"] = "https://127.0.0.1:1"
        result = run([command, "update", "--yes", "--standard"], command_env, timeout=20)
        require(
            result.returncode == 0,
            "update exited %s beside a live session: %s"
            % (result.returncode, output_text(result)[-800:]),
        )
        # The same already-open stdio session must still own the same live
        # daemon connection. A stop-and-transparent-restart is not enough: the
        # existing pipe itself has to remain usable.
        require(
            server.tools_list(timeout=10),
            "update drained the active MCP/daemon session",
        )
    print("PASS: update left the active MCP/daemon session untouched")


def main():
    if os.name != "nt":
        print("PRECONDITION: native Windows is required")
        return 2
    if len(sys.argv) != 2:
        print("usage: python test_windows_update_handoff.py <codebase-memory-mcp.exe>")
        return 2

    source = pathlib.Path(sys.argv[1]).resolve()
    if not source.is_file():
        print("PRECONDITION: binary not found: %s" % source)
        return 2

    work = pathlib.Path(tempfile.mkdtemp(prefix="cbm_win_update_"))
    try:
        env, cache = isolated_environment(work)
        assert_update_hands_off_to_install_script(source, env, work)
        assert_update_does_not_drain_active_session(source, env, cache, work)
        print("\nGREEN: Windows update handoff contract honored.")
        return 0
    except (GuardFailure, McpError, OSError, subprocess.SubprocessError) as exc:
        print("\nRED: %s" % exc)
        return 1
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
