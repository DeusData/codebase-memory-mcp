#!/usr/bin/env bash
# Static + light functional contract for the local/PR release-fixture smoke.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

python3 - "$ROOT" <<'PY'
from __future__ import annotations

import pathlib
import re
import subprocess
import sys
import tempfile
import time
import urllib.request


root = pathlib.Path(sys.argv[1])
failures: list[str] = []


def read(relative: str) -> str:
    path = root / relative
    if not path.is_file():
        failures.append(f"{relative} must exist")
        return ""
    return path.read_text(encoding="utf-8")


def require(condition: bool, message: str) -> None:
    if not condition:
        failures.append(message)


helper_relative = "scripts/smoke-fixture-server.py"
helper = root / helper_relative
smoke_local = read("scripts/smoke-local.sh")
vm_smoke = read("test-infrastructure/vm/vm-smoke.sh")
windows_path_guard = read("test-infrastructure/vm/windows-user-path-guard.ps1")
compose = read("test-infrastructure/docker-compose.yml")
pr_workflow = read(".github/workflows/pr.yml")
test_driver = read("scripts/test.sh")
cli_source = read("src/cli/cli.c")
helper_source = read(helper_relative)

# The server owns the ephemeral bind. A parent-side socket probe followed by
# python -m http.server would reintroduce the close/rebind race this guards.
require(
    "ThreadingHTTPServer((args.bind, 0)" in helper_source,
    "fixture server must bind port 0 itself and retain the listening socket",
)
require(
    "--port-file" in helper_source and "os.replace" in helper_source,
    "fixture server must publish its assigned port atomically through a file",
)
require(
    "socket.socket" not in smoke_local and "python3 -m http.server" not in smoke_local,
    "smoke-local.sh must not reserve/release a port or launch a separate http.server",
)

for relative, source in (
    ("scripts/smoke-local.sh", smoke_local),
    ("test-infrastructure/vm/vm-smoke.sh", vm_smoke),
):
    require(
        helper_relative in source and "--port-file" in source,
        f"{relative} must use the shared race-free fixture server",
    )
    require(
        "EXPECTED_ARTIFACT" in source and "SERVER_LOG" in source,
        f"{relative} must poll its specific expected artifact and retain a server log",
    )
    require(
        'cat "$SERVER_LOG"' in source,
        f"{relative} must print the server log on readiness failure",
    )
    require(
        'wait "$SERVER_PID"' in source,
        f"{relative} cleanup must reap the fixture-server process",
    )
    for variable in (
        "CLAUDE_CONFIG_DIR",
        "CODEX_HOME",
        "KIRO_HOME",
        "HERMES_HOME",
        "QWEN_HOME",
        "CLINE_DATA_DIR",
        "OPENCLAW_HOME",
        "OPENCLAW_STATE_DIR",
        "OPENCLAW_PROFILE",
        "OPENCLAW_CONFIG_PATH",
        "OPENCLAW_WORKSPACE_DIR",
        "OPENCODE_CONFIG",
        "OPENCODE_CONFIG_DIR",
        "COPILOT_HOME",
        "CRUSH_GLOBAL_CONFIG",
        "VIBE_HOME",
        "GLAB_CONFIG_DIR",
        "KIMI_CODE_HOME",
        "CBM_CONTINUE_CONFIG_PATH",
        "CBM_TRAE_CONFIG_PATH",
        "CBM_ROO_CONFIG_PATH",
        "CBM_CODY_CONFIG_PATH",
        "CBM_TEST_WINDOWS_USER_PATH_RUN_ID",
    ):
        require(
            f"-u {variable}" in source,
            f"{relative} must neutralize ambient {variable}",
        )
    for variable in (
        "HOME",
        "USERPROFILE",
        "XDG_CONFIG_HOME",
        "APPDATA",
        "LOCALAPPDATA",
        "TMPDIR",
        "TEMP",
        "TMP",
        "SHELL",
    ):
        require(
            f"{variable}=" in source,
            f"{relative} must pin isolated {variable}",
        )

# Unix fixtures mirror the release archive surface and Linux update aliases.
for name in ("LICENSE", "install.sh", "THIRD_PARTY_NOTICES.md"):
    require(name in smoke_local, f"smoke-local.sh archive must include {name}")
require(
    "codebase-memory-mcp${SUFFIX}-${OS}-${ARCH}.tar.gz" in smoke_local
    and "codebase-memory-mcp-${OS}-${ARCH}.tar.gz" in smoke_local,
    "smoke-local.sh must create the selected variant and standard alias",
)
require(
    "codebase-memory-mcp${SUFFIX}-${OS}-${ARCH}-portable.tar.gz" in smoke_local
    and "codebase-memory-mcp-${OS}-${ARCH}-portable.tar.gz" in smoke_local,
    "smoke-local.sh must create Linux portable selected-variant and standard aliases",
)
require(
    'CBM_CACHE_DIR="$WORK_DIR/cache"' in smoke_local
    and 'SMOKE_TEMP_ROOT="$SMOKE_TEMP_DIR"' in smoke_local,
    "smoke-local.sh must isolate daemon/cache and temporary state from live user sessions",
)
require(
    "machdep.cpu.brand_string" in smoke_local,
    "smoke-local.sh must select arm64 artifacts when running under Rosetta",
)
require(
    "sha256sum ./*.tar.gz" not in smoke_local
    and "shasum -a 256 ./*.tar.gz" not in smoke_local
    and "sha256sum ./*.zip" not in vm_smoke,
    "fixture checksums must name exact artifact basenames, never ./-prefixed paths",
)

# Native Windows packages and serves the exact five-file release bundle, then
# runs the full smoke from a protected profile-rooted directory/cache.
for name in (
    "codebase-memory-mcp.exe",
    "codebase-memory-mcp.payload.exe",
    "LICENSE",
    "install.ps1",
    "THIRD_PARTY_NOTICES.md",
):
    require(name in vm_smoke, f"vm-smoke.sh archive must include {name}")
require("checksums.txt" in vm_smoke, "vm-smoke.sh must generate checksums.txt")
require(
    "SMOKE_DOWNLOAD_URL=" in vm_smoke
    and "SMOKE_UPDATE_FIXTURE_DIR=" in vm_smoke
    and "SMOKE_ARCH=" in vm_smoke,
    "vm-smoke.sh must enable Phase 12-14 fixture semantics with an explicit arch",
)
require(
    "PROFILE_ROOT=" in vm_smoke
    and 'SMOKE_TEMP_ROOT="$SMOKE_DIR"' in vm_smoke
    and 'CBM_CACHE_DIR="$(cygpath -m "$SMOKE_DIR/cache")"' in vm_smoke,
    "vm-smoke.sh must isolate smoke temp/cache below the protected user profile",
)
require(
    "--agent-config-only" not in vm_smoke,
    "vm-smoke.sh must run the full smoke, not a reduced mode",
)
require(
    "windows-user-path-guard.ps1" in vm_smoke
    and "-Mode prepare" in vm_smoke
    and "-Mode verify" in vm_smoke
    and "-Mode cleanup" in vm_smoke,
    "vm-smoke.sh must prepare, verify, and clean up an isolated Windows PATH key",
)
require(
    'CBM_TEST_WINDOWS_USER_PATH_RUN_ID="$PATH_RUN_ID"' in vm_smoke
    and 'SMOKE_DOWNLOAD_URL="http://127.0.0.1:$PORT"' in vm_smoke
    and vm_smoke.find("-Mode verify") > vm_smoke.find("scripts/smoke-test.sh"),
    "vm-smoke.sh must pass the run ID and loopback gate through the full smoke, then verify it",
)
require(
    "DoNotExpandEnvironmentNames" in windows_path_guard
    and "Assert-StateEqual" in windows_path_guard
    and 'Read-PathValue "Environment"' in windows_path_guard,
    "Windows PATH guard must compare the live raw value and registry kind without expanding it",
)
require(
    'Software\\CodebaseMemoryMCP\\Smoke\\$RunId' in windows_path_guard
    and "Assert-SmokePathValue" in windows_path_guard
    and "DeleteSubKeyTree" in windows_path_guard
    and "restore" not in windows_path_guard.lower(),
    "Windows PATH smoke must mutate and delete only its GUID-scoped scratch registry leaf",
)
require(
    "CBM_TEST_WINDOWS_USER_PATH_RUN_ID" in cli_source
    and 'L"SMOKE_DOWNLOAD_URL"' in cli_source
    and 'L"Software\\\\CodebaseMemoryMCP\\\\Smoke\\\\%ls"' in cli_source
    and "cli_windows_smoke_download_url_valid" in cli_source
    and "CbmSmokeRunId" in cli_source,
    "the Windows PATH test seam must be run-ID-only, sentinel-bound, and loopback-gated",
)

# Every maintained local/PR native leg enters through the fixture wrapper.
for service in ("smoke:", "smoke-amd64:"):
    match = re.search(
        rf"^  {re.escape(service)}\n(?P<body>.*?)(?=^  [A-Za-z0-9_-]+:|\Z)",
        compose,
        re.MULTILINE | re.DOTALL,
    )
    section = match.group("body") if match else ""
    require(
        "scripts/smoke-local.sh" in section,
        f"docker-compose {service[:-1]} service must run smoke-local.sh",
    )
require(
    pr_workflow.count("scripts/smoke-local.sh") >= 2,
    "PR Ubuntu and macOS smoke steps must run smoke-local.sh",
)
require(
    "SMOKE_ARCH=amd64" in pr_workflow
    and "test-infrastructure/vm/vm-smoke.sh" in pr_workflow,
    "PR Windows smoke must call vm-smoke.sh with SMOKE_ARCH=amd64",
)
smoke_test = read("scripts/smoke-test.sh")
require(
    "MSYS2_ARG_CONV_EXCL='*'" in smoke_test
    and 'powershell.exe -NoProfile -ExecutionPolicy Bypass -File' in smoke_test
    and "& $args[1]" not in smoke_test,
    "Windows Phase 13 must execute install.ps1 directly with native paths",
)
require(
    smoke_test.count("--noproxy '*'") >= 4,
    "all loopback release-fixture curl requests must bypass ambient proxies",
)
require(
    "/tmp/cbm-curl12a.err" not in smoke_test
    and 'CURL12_ERR="$DL_DIR/curl12a.err"' in smoke_test,
    "curl diagnostics must stay inside the per-smoke download directory",
)
require(
    "CBM_TEST_WINDOWS_USER_PATH_RUN_ID=invalid" in smoke_test
    and "invalid Windows PATH smoke seam fell back" in smoke_test,
    "Windows release smoke must prove malformed PATH-test gating fails closed",
)
require(
    'if [ "$UPD_CMD" != "$EXPECTED_UPD_CMD" ]' in smoke_test,
    "Phase 14 must require the refreshed MCP command to equal the updated binary",
)
require(
    'UPDATE_DRIVER="$RETIRED_DIR/codebase-memory-mcp"' in smoke_test
    and 'STALE_CMD="$UPDATE_DRIVER"' in smoke_test,
    "POSIX Phase 14 must refresh from positive running-image identity without probing config paths",
)
require(
    'STALE_CMD="$UPDATE_HOME/retired-install/codebase-memory-mcp.exe"' in smoke_test,
    "Windows Phase 14 must test a literal missing executable, not an ambiguous extensionless command",
)
for changed_path in (
    "install\\.(sh|ps1)",
    "scripts/smoke-local",
    "scripts/smoke-fixture-server",
    "scripts/gen-third-party-notices",
    "test-infrastructure/vm/vm-smoke",
    "windows-user-path-guard",
):
    require(
        changed_path in pr_workflow,
        f"PR product-change detector must include {changed_path}",
    )
require(
    "tests/test_smoke_fixture_contract.sh" in test_driver,
    "scripts/test.sh must run the smoke fixture contract",
)

# Functional check: the helper must publish a live kernel-assigned port and
# serve the exact expected artifact. This is intentionally build-free.
if helper.is_file():
    with tempfile.TemporaryDirectory(prefix="cbm-fixture-contract-") as temp:
        temp_path = pathlib.Path(temp)
        fixture = temp_path / "fixture"
        fixture.mkdir()
        expected = fixture / "expected-artifact.txt"
        expected.write_bytes(b"fixture-ok\n")
        port_file = temp_path / "port"
        log_file = temp_path / "server.log"
        with log_file.open("wb") as log:
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(helper),
                    "--directory",
                    str(fixture),
                    "--port-file",
                    str(port_file),
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            try:
                port = 0
                # This is a hang detector, not a Python-startup latency
                # assertion. Under a concurrent three-platform local gate the
                # interpreter can take more than two seconds to reach main;
                # keep the same bounded readiness semantics without turning
                # ordinary host scheduling into a false red.
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    if port_file.is_file():
                        text = port_file.read_text(encoding="ascii").strip()
                        if text:
                            port = int(text)
                            break
                    if process.poll() is not None:
                        break
                    time.sleep(0.02)
                require(port > 0, "fixture server must publish a nonzero assigned port")
                if port == 0 and log_file.is_file():
                    details = log_file.read_text(encoding="utf-8", errors="replace").strip()
                    if details:
                        failures.append(f"fixture server startup log: {details}")
                if port > 0:
                    opener = urllib.request.build_opener(
                        urllib.request.ProxyHandler({})
                    )
                    body = opener.open(
                        f"http://127.0.0.1:{port}/{expected.name}", timeout=2
                    ).read()
                    require(body == b"fixture-ok\n", "fixture server returned wrong artifact")
            finally:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)
                    failures.append("fixture server did not terminate promptly")

if failures:
    print("smoke fixture contract: FAIL", file=sys.stderr)
    for failure in failures:
        print(f"  - {failure}", file=sys.stderr)
    raise SystemExit(1)

print("smoke fixture contract: OK")
PY
