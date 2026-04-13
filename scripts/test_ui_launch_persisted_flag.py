#!/usr/bin/env python3
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.request import urlopen


REPO_ROOT = Path(__file__).resolve().parent.parent
BUILD_BIN = REPO_ROOT / "build" / "c" / "codebase-memory-mcp"
CONFIG_REL = Path(".cache") / "codebase-memory-mcp" / "config.json"


def fail(message: str) -> None:
    raise AssertionError(message)


def choose_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_http(port: int, timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            with urlopen(f"http://127.0.0.1:{port}/", timeout=0.5) as response:
                if response.status < 500:
                    return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        time.sleep(0.1)
    fail(f"HTTP UI did not become reachable on 127.0.0.1:{port}: {last_error}")


def wait_for_closed_port(port: int, timeout: float = 3.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                time.sleep(0.1)
                continue
        except OSError:
            return
    fail(f"127.0.0.1:{port} stayed reachable when it should be closed")


def read_config(home_dir: Path) -> dict:
    config_path = home_dir / CONFIG_REL
    if not config_path.exists():
        fail(f"Expected persisted UI config at {config_path}")
    with config_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if set(data.keys()) != {"ui_enabled", "ui_port"}:
        fail(f"Unexpected config keys: {sorted(data.keys())}")
    return data


def wait_for_config(
    home_dir: Path, expected: dict | None = None, timeout: float = 5.0
) -> dict:
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            cfg = read_config(home_dir)
            if expected is None or cfg == expected:
                return cfg
            last_error = AssertionError(f"Expected config {expected}, got {cfg}")
        except AssertionError as exc:
            last_error = exc
        time.sleep(0.1)
    fail(str(last_error) if last_error else "UI config was not persisted")


def launch(home_dir: Path, *args: str) -> subprocess.Popen:
    env = os.environ.copy()
    env["HOME"] = str(home_dir)
    env.pop("CBM_CACHE_DIR", None)
    return subprocess.Popen(
        [str(BUILD_BIN), *args],
        cwd=REPO_ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
    )


def stop_process(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def run_help() -> str:
    result = subprocess.run(
        [str(BUILD_BIN), "--help"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def main() -> int:
    subprocess.run(
        ["make", "-f", "Makefile.cbm", "cbm-with-ui"], cwd=REPO_ROOT, check=True
    )

    temp_root = Path(tempfile.mkdtemp(prefix="cbm-ui-launch-"))
    home_dir = temp_root / "home"
    home_dir.mkdir(parents=True, exist_ok=True)

    try:
        port = choose_free_port()

        proc = launch(home_dir, f"--ui=false", f"--port={port}")
        try:
            cfg = wait_for_config(home_dir, {"ui_enabled": False, "ui_port": port})
            wait_for_closed_port(port)
            if cfg != {"ui_enabled": False, "ui_port": port}:
                fail(f"Unexpected initial disabled config: {cfg}")
        finally:
            stop_process(proc)

        wait_for_closed_port(port)

        proc = launch(home_dir, f"--port={port}")
        try:
            time.sleep(0.5)
            if proc.poll() is None:
                wait_for_closed_port(port)
        finally:
            stop_process(proc)

        proc = launch(home_dir, f"--ui=true", f"--port={port}")
        try:
            wait_for_http(port)
            cfg = wait_for_config(home_dir, {"ui_enabled": True, "ui_port": port})
            if cfg != {"ui_enabled": True, "ui_port": port}:
                fail(f"Unexpected enabled config: {cfg}")
        finally:
            stop_process(proc)

        wait_for_closed_port(port)

        proc = launch(home_dir, f"--port={port}")
        try:
            wait_for_http(port)
        finally:
            stop_process(proc)

        wait_for_closed_port(port)

        proc = launch(home_dir, f"--ui=false", f"--port={port}")
        try:
            cfg = wait_for_config(home_dir, {"ui_enabled": False, "ui_port": port})
            wait_for_closed_port(port)
            if cfg != {"ui_enabled": False, "ui_port": port}:
                fail(f"Unexpected disabled config: {cfg}")
        finally:
            stop_process(proc)

        wait_for_closed_port(port)

        proc = launch(home_dir, f"--port={port}")
        try:
            time.sleep(0.5)
            if proc.poll() is None:
                wait_for_closed_port(port)
        finally:
            stop_process(proc)

        help_output = run_help()
        if "Enable HTTP graph visualization (persisted)" not in help_output:
            fail("Missing persisted help text for --ui=true")
        if "Disable HTTP graph visualization (persisted)" not in help_output:
            fail("Missing persisted help text for --ui=false")
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)

    print("PASS: persisted UI launch contract works")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        raise SystemExit(1)
