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
local_ci = read("test-infrastructure/run.sh")
windows_vm = read("test-infrastructure/vm/win.sh")
windows_vm_runner = read("test-infrastructure/vm/vm-run-tests.sh")
pr_workflow = read(".github/workflows/pr.yml")
test_driver = read("scripts/test.sh")
cli_source = read("src/cli/cli.c")
helper_source = read(helper_relative)

# The server owns the ephemeral bind. A parent-side socket probe followed by
# python -m http.server would reintroduce the close/rebind race this guards.
require(
    "HTTPServer((args.bind, 0)" in helper_source
    and "ThreadingHTTPServer)" in helper_source,
    "fixture server must bind port 0 itself on a threading server and retain "
    "the listening socket",
)
require(
    "--port-file" in helper_source and "os.replace" in helper_source,
    "fixture server must publish its assigned port atomically through a file",
)
require(
    "os.fsync(" not in helper_source,
    "fixture readiness publication must not wait for crash-durability sync",
)
require(
    "socket.socket" not in smoke_local and "python3 -m http.server" not in smoke_local,
    "smoke-local.sh must not reserve/release a port or launch a separate http.server",
)

# Every environment variable the CLI honours ahead of $HOME. A fixture that
# redirects only HOME still resolves these to the developer's real config, so
# both the shell fixtures and the C runner must neutralize the whole set.
CLIENT_HOME_OVERRIDES = (
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
    "OMP_PROFILE",
    "PI_CODING_AGENT_DIR",
)

# The C suite exercises the same install/uninstall paths as the shell fixtures,
# so it needs the same neutralization — otherwise a green run on a developer
# machine only proves the ambient config happened to be writable.
test_main = read("tests/test_main.c")
for variable in CLIENT_HOME_OVERRIDES:
    require(
        f'"{variable}"' in test_main,
        f"tests/test_main.c must neutralize ambient {variable}",
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
    for variable in CLIENT_HOME_OVERRIDES + ("CBM_TEST_WINDOWS_USER_PATH_RUN_ID",):
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
install_script = read("install.sh")
require(
    'tar --no-same-owner -xzf "$DLDIR/$ARCHIVE" -C "$DLDIR"' in install_script,
    "install.sh must not preserve release-builder ownership when extracting tar archives",
)
require(
    all(
        needle in install_script
        for needle in (
            "ARCHIVE_MEMBER_COUNT",
            "release archive contains unexpected member",
            "release archive does not match the exact member set",
            'for extracted_member in "$ARCHIVE_BINARY" LICENSE "$ARCHIVE_INSTALLER"',
        )
    )
    and "cbm-integrations.json" not in install_script,
    "install.sh must validate and accept the exact four-member release archive layout",
)
# One composition ships, so there is one archive name and no variant alias.
require(
    "codebase-memory-mcp-${OS}-${ARCH}.tar.gz" in smoke_local
    and "${SUFFIX}" not in smoke_local,
    "smoke-local.sh must create the single canonical archive with no variant alias",
)
require(
    "codebase-memory-mcp-${OS}-${ARCH}-portable.tar.gz" in smoke_local,
    "smoke-local.sh must create the Linux portable update alias",
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

# Native Windows packages and serves the exact four-file release bundle (ONE
# binary, like every other platform), then runs the full smoke from a protected
# profile-rooted directory/cache.
for name in (
    "codebase-memory-mcp.exe",
    "LICENSE",
    "install.ps1",
    "THIRD_PARTY_NOTICES.md",
):
    require(name in vm_smoke, f"vm-smoke.sh archive must include {name}")
require(
    "codebase-memory-mcp.payload.exe" not in vm_smoke,
    "vm-smoke.sh must not stage a Windows launcher/payload pair",
)
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
for service in ("smoke-windows:",):
    match = re.search(
        rf"^  {re.escape(service)}\n(?P<body>.*?)(?=^  [A-Za-z0-9_-]+:|\Z)",
        compose,
        re.MULTILINE | re.DOTALL,
    )
    section = match.group("body") if match else ""
    require(
        "codebase-memory-mcp-launcher" not in section
        and "codebase-memory-mcp.payload.exe" not in section.replace(
            "test ! -e build/win-cross/codebase-memory-mcp.payload.exe", ""
        ),
        f"docker-compose {service[:-1]} must build ONE Windows binary, not a launcher/payload pair",
    )
    require(
        "test ! -e build/win-cross/codebase-memory-mcp.payload.exe" in section,
        f"docker-compose {service[:-1]} must assert no payload sibling is produced",
    )
require(
    "wine64 ./build/win-cross/codebase-memory-mcp.exe --version" in compose
    and "wine64 cmd /c build/win-cross/codebase-memory-mcp.exe --version" in compose,
    "docker-compose Windows cross-smoke must execute the single binary through Wine and through a "
    "Wine Windows parent",
)
require(
    "soak-windows:" not in compose,
    "docker-compose must not advertise a Wine daemon soak that cannot enforce Windows security "
    "and locking semantics",
)
require(
    'if [ "${1:-full}" = "soak-windows" ]' in local_ci
    and 'exec "$ROOT/test-infrastructure/vm/win.sh" soak 10' in local_ci,
    "the maintained Windows soak entry point must route to the real Windows VM",
)
require(
    "soak)" in windows_vm
    and "vm-run-tests.sh --soak '$duration'" in windows_vm,
    "the Windows VM driver must route the native daemon soak through its protected-temp harness",
)
# The soak sequence and its completion guards moved into the ONE canonical
# entry scripts/soak-legs.sh (venue parity: _soak.yml, compose and the VM all
# run the same file); the VM wrapper supplies the protected temp root and
# routes into it.
soak_legs = read("scripts/soak-legs.sh")
require(
    'if [ "$1" = "--soak" ]' in windows_vm_runner
    and 'scripts/soak-legs.sh "$binary" "$duration"' in windows_vm_runner,
    "the Windows VM harness must route the native daemon soak through the canonical soak "
    "entry under its protected temp root",
)
require(
    "grep -Fq '=== soak-test: PASSED ==='" in soak_legs,
    "the canonical soak entry must completion-guard every leg on the soak summary",
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
# A count cannot tell "we still have four curls" from "every curl is safe" — it
# passed while a loopback UI probe carried no --noproxy at all, which is the
# exact failure this rule exists to prevent (an ambient http_proxy makes a
# 127.0.0.1 request leave the machine). Assert the property on every invocation
# instead, so adding or removing a curl cannot silently satisfy the rule.
unproxied_curls = [
    line.strip()
    for line in smoke_test.splitlines()
    # An invocation starts a command: line start, or after ; & | ( or `if`/`then`
    # etc. Mentions inside echo/comment strings are not invocations.
    if re.search(r"(?:^|[;&|(]|\b(?:if|then|else|do|not)\s)\s*curl\s", line)
    and not line.lstrip().startswith("#")
    and not re.search(r"echo\s", line.split("curl")[0])
    and "--noproxy" not in line
]
require(
    not unproxied_curls,
    "every curl in the smoke fixture must bypass ambient proxies (--noproxy '*'): "
    + "; ".join(unproxied_curls[:3]),
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
# There is no in-process update left to refresh the MCP command, so Phase 14
# cannot assert a refresh. The refresh itself still happens -- the install
# script re-runs `install` -- and is covered by Phase 8 (agent config install
# E2E) and Phase 13 (install script E2E). Phase 14 must say so rather than
# quietly dropping the step.
require(
    "config refresh covered by install" in smoke_test,
    "Phase 14 must name where the config-refresh coverage moved to",
)
# The retired-image driver existed to exercise an in-process replacement that no
# platform performs any more: `update` prints the shipped install script's
# command and touches nothing. Phase 14 now drives from the installed binary
# everywhere, and 14a asserts the binary is byte-identical afterwards.
require(
    'UPDATE_DRIVER="$UPDATE_HOME/.local/bin/codebase-memory-mcp"' in smoke_test
    and 'STALE_CMD="$UPDATE_DRIVER"' in smoke_test,
    "Phase 14 must drive update from the installed binary on every platform",
)
require(
    "update replaced the binary in-process" in smoke_test,
    "Phase 14 must assert update leaves the binary byte-identical",
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
                        try:
                            text = port_file.read_text(encoding="ascii").strip()
                        except OSError:
                            # Windows: the atomic replace publishing the port
                            # (or a first-touch AV scan of the fresh file)
                            # briefly denies the read — not ready yet.
                            text = ""
                        if text:
                            port = int(text)
                            break
                    if process.poll() is not None:
                        break
                    time.sleep(0.02)
                require(port > 0, "fixture server must publish a nonzero assigned port")
                if port == 0:
                    # A bare "no port" verdict is unactionable on a runner we
                    # cannot reproduce locally: this failed macOS-Intel-only
                    # and the conditional startup-log line named nothing
                    # because the log was empty. Report the observable state
                    # unconditionally so the next remote run identifies the
                    # cause instead of costing another blind round.
                    waited = 30 - max(0.0, deadline - time.monotonic())
                    exit_status = process.poll()
                    if log_file.is_file():
                        details = log_file.read_text(
                            encoding="utf-8", errors="replace"
                        ).strip()
                    else:
                        details = "(no server.log created)"
                    if port_file.is_file():
                        raw = port_file.read_text(encoding="ascii", errors="replace")
                        port_state = f"exists content={raw!r}"
                    else:
                        port_state = "absent"
                    leftovers = sorted(
                        entry.name
                        for entry in temp_path.iterdir()
                        if entry.name.startswith(f".{port_file.name}.")
                    )
                    failures.append(
                        "fixture server diagnostics: "
                        f"waited={waited:.1f}s "
                        f"exit_status={'alive' if exit_status is None else exit_status} "
                        f"port_file={port_state} "
                        f"staged_temp_files={leftovers or 'none'} "
                        f"interpreter={sys.executable} "
                        f"startup_log={details or '(empty)'}"
                    )
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

# Regression guard, by construction: binding must not depend on reverse DNS.
# http.server's default server_bind() resolves socket.getfqdn(), which blocks
# for the resolver timeout on hosts that do not answer for the bind address --
# the server stays alive and never publishes its port (macOS Intel). Force the
# resolver to hang and require the port anyway, so the dependency cannot
# return without turning this gate red on every platform.
if helper.is_file():
    with tempfile.TemporaryDirectory(prefix="cbm-fixture-dns-") as dns_temp:
        dns_root = pathlib.Path(dns_temp)
        dns_fixture = dns_root / "fixture"
        dns_fixture.mkdir()
        (dns_fixture / "expected-artifact.txt").write_bytes(b"fixture-ok\n")
        dns_port_file = dns_root / "port"
        dns_wrapper = dns_root / "hang_reverse_dns.py"
        dns_wrapper.write_text(
            "import runpy, socket, sys, time\n"
            "socket.getfqdn = lambda *a, **k: time.sleep(300)\n"
            "sys.argv = ['smoke-fixture-server.py', '--directory', "
            f"{str(dns_fixture)!r}, '--port-file', {str(dns_port_file)!r}]\n"
            f"runpy.run_path({str(helper)!r}, run_name='__main__')\n",
            encoding="utf-8",
        )
        dns_log = dns_root / "server.log"
        with dns_log.open("wb") as dns_handle:
            dns_process = subprocess.Popen(
                [sys.executable, str(dns_wrapper)],
                stdout=dns_handle,
                stderr=subprocess.STDOUT,
            )
        try:
            dns_port = 0
            dns_deadline = time.monotonic() + 20
            while time.monotonic() < dns_deadline:
                if dns_port_file.is_file():
                    try:
                        dns_text = dns_port_file.read_text(encoding="ascii").strip()
                    except OSError:
                        # Windows: replace-in-flight / first-touch AV scan —
                        # not ready yet (see the port poll above).
                        dns_text = ""
                    if dns_text:
                        dns_port = int(dns_text)
                        break
                if dns_process.poll() is not None:
                    break
                time.sleep(0.02)
            require(
                dns_port > 0,
                "fixture server must bind without a reverse-DNS lookup "
                "(hanging socket.getfqdn must not block port publication)",
            )
        finally:
            dns_process.terminate()
            try:
                dns_process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                dns_process.kill()
                dns_process.wait(timeout=3)

if failures:
    print("smoke fixture contract: FAIL", file=sys.stderr)
    for failure in failures:
        print(f"  - {failure}", file=sys.stderr)
    raise SystemExit(1)

print("smoke fixture contract: OK")
PY
