r"""GREEN guard — an initialized, idle MCP frontend must not spin on Windows.

Regression test for #1764. The frontend used to run two independent
maintenance observers every 10 ms. Each observer reopened and locked the
version-cohort marker, causing substantial CPU usage while a stdio client was
idle on Windows.

Exit code: 0 == green, 1 == regression, 2 == setup error.

Usage:
    python test_idle_stdio_cpu.py <path-to-codebase-memory-mcp.exe>
"""
import ctypes
from ctypes import wintypes
import os
import re
import socket
import subprocess
import sys
import tempfile
import time

from mcp_stdio import McpError, McpServer


SAMPLE_SECONDS = 5.0
SETTLE_SECONDS = 2.0
MAX_PERCENT_OF_ONE_CORE = 20.0
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
SYNCHRONIZE = 0x00100000
WAIT_OBJECT_0 = 0


class FileTime(ctypes.Structure):
    _fields_ = [("low", wintypes.DWORD), ("high", wintypes.DWORD)]


def process_cpu_seconds(pid):
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.GetProcessTimes.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(FileTime),
        ctypes.POINTER(FileTime),
        ctypes.POINTER(FileTime),
        ctypes.POINTER(FileTime),
    ]
    kernel32.GetProcessTimes.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL

    handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        raise OSError(ctypes.get_last_error(), "OpenProcess failed for pid %d" % pid)
    try:
        created = FileTime()
        exited = FileTime()
        kernel = FileTime()
        user = FileTime()
        if not kernel32.GetProcessTimes(
                handle, ctypes.byref(created), ctypes.byref(exited),
                ctypes.byref(kernel), ctypes.byref(user)):
            raise OSError(ctypes.get_last_error(), "GetProcessTimes failed for pid %d" % pid)
        kernel_ticks = (kernel.high << 32) | kernel.low
        user_ticks = (user.high << 32) | user.low
        return (kernel_ticks + user_ticks) / 10_000_000.0
    finally:
        kernel32.CloseHandle(handle)


def wait_process_exit(pid, timeout_seconds):
    if not pid:
        return True
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel32.WaitForSingleObject.restype = wintypes.DWORD
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.OpenProcess(SYNCHRONIZE, False, pid)
    if not handle:
        return True
    try:
        milliseconds = min(int(timeout_seconds * 1000), 0xFFFFFFFE)
        return kernel32.WaitForSingleObject(handle, milliseconds) == WAIT_OBJECT_0
    finally:
        kernel32.CloseHandle(handle)


def free_port():
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]
    finally:
        listener.close()


def run_cli(binary, env, args, timeout=60):
    return subprocess.run([binary] + args, capture_output=True, env=env, timeout=timeout)


def output_text(result):
    return ((result.stdout or b"") + (result.stderr or b"")).decode("utf-8", "replace")


def daemon_pid_from(text):
    match = re.search(r"pid[: ]+(\d+)", text)
    return int(match.group(1)) if match else 0


def stop_daemon(binary, env, known_pid):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        try:
            stopped = run_cli(binary, env, ["daemon", "stop"], timeout=30)
            status = run_cli(binary, env, ["daemon", "status"], timeout=30)
            stop_accepted = stopped.returncode == 0
            status_clear = status.returncode != 0 and "not running" in output_text(status)
            if (stop_accepted or status_clear) and wait_process_exit(known_pid, 2):
                time.sleep(0.25)  # let the daemon logger release its final file handle
                return
        except subprocess.SubprocessError:
            pass
        time.sleep(0.5)
    if known_pid:
        subprocess.run(["taskkill", "/F", "/PID", str(known_pid)],
                       capture_output=True, timeout=30)
        wait_process_exit(known_pid, 10)
        time.sleep(0.25)


def main():
    if os.name != "nt":
        print("PRECONDITION: this CPU regression guard requires native Windows")
        return 2
    if len(sys.argv) != 2 or not os.path.isfile(sys.argv[1]):
        print("SETUP FAIL: pass the product executable as the only argument")
        return 2

    binary = os.path.abspath(sys.argv[1])
    daemon_pid = 0
    session = None
    with tempfile.TemporaryDirectory(prefix="cbm-idle-cpu-") as work:
        cache = os.path.join(work, "cache")
        runtime = os.path.join(work, "runtime")
        os.makedirs(cache)
        os.makedirs(runtime)
        env = dict(os.environ)
        env["CBM_CACHE_DIR"] = cache
        env["CBM_RUNTIME_DIR"] = runtime

        try:
            started = run_cli(binary, env, ["daemon", "start", "--port=%d" % free_port()])
            daemon_pid = daemon_pid_from(output_text(started))
            if started.returncode != 0 or not daemon_pid:
                print("SETUP FAIL: isolated daemon did not start:\n%s" % output_text(started)[:800])
                return 2

            session = McpServer(binary, cache_dir=cache,
                                extra_env={"CBM_RUNTIME_DIR": runtime}, cwd=work)
            session.start()
            session.initialize(timeout=60)
            time.sleep(SETTLE_SECONDS)
            if session.proc.poll() is not None:
                print("SETUP FAIL: initialized MCP frontend exited before the CPU sample")
                return 2

            before_cpu = process_cpu_seconds(session.proc.pid)
            before_wall = time.perf_counter()
            time.sleep(SAMPLE_SECONDS)
            elapsed = time.perf_counter() - before_wall
            cpu_seconds = process_cpu_seconds(session.proc.pid) - before_cpu
            percent = 100.0 * cpu_seconds / elapsed
            print("Idle MCP frontend CPU: %.2f%% of one logical core over %.2fs"
                  % (percent, elapsed))
            if percent > MAX_PERCENT_OF_ONE_CORE:
                print("RED: idle frontend CPU %.2f%% exceeds %.2f%%; "
                      "maintenance polling is spinning" %
                      (percent, MAX_PERCENT_OF_ONE_CORE))
                return 1
            print("PASS: initialized idle MCP frontend stays below the Windows CPU guard")
            return 0
        except (McpError, OSError, subprocess.SubprocessError) as exc:
            print("SETUP FAIL: %s" % exc)
            return 2
        finally:
            if session is not None:
                session.close()
            stop_daemon(binary, env, daemon_pid)


if __name__ == "__main__":
    raise SystemExit(main())
