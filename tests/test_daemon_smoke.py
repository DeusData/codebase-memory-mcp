#!/usr/bin/env python3
"""Real-binary smoke test for the mandatory per-account CBM daemon.

This is intentionally a separate POSIX smoke test rather than part of the C
unit suite. It owns the account-wide rendezvous point while it runs. Direct
invocations may skip an occupied endpoint; the Make target requires a clean
rendezvous and fails instead of reporting a false-green skip.
Linux and macOS are covered; Windows named-pipe/mutex behavior remains covered
by the deterministic C IPC/runtime tests.
"""

import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import socket
import stat
import struct
import subprocess
import sys
import tarfile
import tempfile
import threading
import time

try:
    import fcntl
except ImportError:  # Windows reaches the explicit platform skip in main().
    fcntl = None


RENDEZVOUS_KEY = "c888acc3ae367a1e"
RENDEZVOUS_REQUEST_SIZE = 133
RENDEZVOUS_RESPONSE_SIZE = 798
RENDEZVOUS_VERSION_CAP = 64
RENDEZVOUS_BUILD_CAP = 65
RENDEZVOUS_MESSAGE_CAP = 512
START_TIMEOUT = 30.0
OPERATION_TIMEOUT = 30.0
SHUTDOWN_TIMEOUT = 35.0
RESTART_GUIDANCE = (
    "Please restart your coding-agent sessions to properly take this into "
    "account."
)


class SmokeFailure(RuntimeError):
    pass


def check(condition, message):
    if not condition:
        raise SmokeFailure(message)


def wait_until(predicate, timeout, description):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise SmokeFailure("timed out waiting for " + description)


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def lock_status(path, record_lock):
    """Return free, held, or error without creating a missing lock file."""
    if not path.exists():
        return "free"
    flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(str(path), flags)
    except OSError:
        return "error"
    try:
        status = os.fstat(fd)
        if (
            not stat.S_ISREG(status.st_mode)
            or status.st_uid != os.geteuid()
            or status.st_nlink != 1
            or stat.S_IMODE(status.st_mode) != 0o600
        ):
            return "error"
        try:
            if record_lock:
                fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.lockf(fd, fcntl.LOCK_UN)
            else:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(fd, fcntl.LOCK_UN)
            return "free"
        except BlockingIOError:
            return "held"
        except OSError:
            return "error"
    finally:
        os.close(fd)


def process_gone_or_zombie(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "stat="],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    return result.returncode != 0 or result.stdout.strip().startswith("Z")


def process_identity(pid):
    """Return (ppid, pgid, state) for a live non-zombie process."""
    result = subprocess.run(
        [
            "ps",
            "-p",
            str(pid),
            "-o",
            "ppid=",
            "-o",
            "pgid=",
            "-o",
            "stat=",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    try:
        ppid, pgid, state = result.stdout.split(None, 2)
        if state.startswith("Z"):
            return None
        return int(ppid), int(pgid), state
    except (TypeError, ValueError):
        return None


def worker_tree_identity_matches(
    supervisor_pid, worker_pid, worker_pgid, descendant_pid
):
    """Revalidate the exact test-owned worker tree before signaling it."""
    if (
        not worker_pid
        or not worker_pgid
        or worker_pid <= 1
        or worker_pgid != worker_pid
        or not descendant_pid
        or descendant_pid <= 1
    ):
        return False
    worker = process_identity(worker_pid)
    descendant = process_identity(descendant_pid)
    if not worker or not descendant:
        return False
    worker_ppid, current_worker_pgid, _ = worker
    descendant_ppid, current_descendant_pgid, _ = descendant
    return (
        current_worker_pgid == worker_pgid
        and descendant_ppid == worker_pid
        and current_descendant_pgid == worker_pgid
        and (supervisor_pid is None or worker_ppid == supervisor_pid)
    )


def worker_group_contains_descendant(worker_pgid, descendant_pid):
    """Validate the preserved group through its test-created descendant."""
    if not worker_pgid or worker_pgid <= 1 or not descendant_pid:
        return False
    descendant = process_identity(descendant_pid)
    return bool(descendant and descendant[1] == worker_pgid)


def worker_is_stopped(worker_pid, worker_pgid):
    identity = process_identity(worker_pid)
    return bool(
        identity
        and identity[1] == worker_pgid
        and identity[2].startswith("T")
    )


def read_private_pid_fields(path, field_count):
    """Read a small owner-only marker, returning None while it is incomplete."""
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        fd = os.open(str(path), flags)
    except OSError:
        return None
    try:
        status = os.fstat(fd)
        if (
            not stat.S_ISREG(status.st_mode)
            or status.st_uid != os.geteuid()
            or status.st_nlink != 1
            or stat.S_IMODE(status.st_mode) != 0o600
            or status.st_size > 128
        ):
            return None
        payload = os.read(fd, 129)
    except OSError:
        return None
    finally:
        os.close(fd)
    if len(payload) > 128:
        return None
    try:
        fields = payload.decode("ascii").split()
        values = tuple(int(field) for field in fields)
    except (UnicodeDecodeError, ValueError):
        return None
    if len(values) != field_count or any(value <= 1 for value in values):
        return None
    return values


def json_events(path, event):
    return [record for record in json_records(path) if record.get("event") == event]


def json_records(path):
    if not path.exists():
        return []
    found = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict):
            found.append(record)
    return found


def daemon_lifecycle_sequence(path):
    return [
        record.get("event")
        for record in json_records(path)
        if record.get("event") in ("daemon.start", "daemon.stop")
    ]


def fixed_wire_text(value, capacity):
    encoded = value.encode("ascii")
    check(len(encoded) < capacity, "rendezvous text exceeds fixed capacity")
    return encoded + b"\0" * (capacity - len(encoded))


def decode_fixed_wire_text(payload, offset, capacity):
    field = payload[offset : offset + capacity]
    check(len(field) == capacity, "truncated rendezvous text field")
    terminator = field.find(b"\0")
    check(terminator >= 0, "unterminated rendezvous text field")
    check(not any(field[terminator + 1 :]), "non-canonical rendezvous text padding")
    return field[:terminator].decode("ascii")


def recv_exact(stream, length):
    chunks = []
    remaining = length
    while remaining:
        chunk = stream.recv(remaining)
        check(chunk, "daemon closed during rendezvous response")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def probe_future_generation_rendezvous(
    socket_path, active_version, active_build, requested_version, requested_build
):
    """Use only the permanent envelope; future detailed wire bytes never run."""
    request = (
        struct.pack(">I", 1)
        + fixed_wire_text(requested_version, RENDEZVOUS_VERSION_CAP)
        + fixed_wire_text(requested_build, RENDEZVOUS_BUILD_CAP)
    )
    check(len(request) == RENDEZVOUS_REQUEST_SIZE, "bad rendezvous request fixture")
    header = struct.pack(
        ">4sBBHI", b"CBMD", 1, 1, 1, RENDEZVOUS_REQUEST_SIZE
    )
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
        stream.settimeout(10)
        stream.connect(str(socket_path))
        stream.sendall(header + request)
        response_header = recv_exact(stream, 12)
        magic, frame_version, frame_type, operation, length = struct.unpack(
            ">4sBBHI", response_header
        )
        check(magic == b"CBMD", "future probe received wrong frame magic")
        check(frame_version == 1, "future probe received unstable frame version")
        check(frame_type == 2 and operation == 1, "future probe received wrong response kind")
        check(length == RENDEZVOUS_RESPONSE_SIZE, "future probe received resized response")
        response = recv_exact(stream, length)

    connect_status, hello_status, client_id, process_id, conflict_status = struct.unpack_from(
        ">IIQQI", response, 0
    )
    check(connect_status == 2, "future generation was not rejected as a conflict")
    check(hello_status == 2 and conflict_status == 2, "future version conflict reason was lost")
    check(client_id == 0 and process_id == 0, "rejected generation received an identity")
    check(
        decode_fixed_wire_text(response, 28, RENDEZVOUS_VERSION_CAP) == active_version,
        "future probe active version mismatch",
    )
    check(
        decode_fixed_wire_text(response, 92, RENDEZVOUS_BUILD_CAP) == active_build,
        "future probe active build mismatch",
    )
    check(
        decode_fixed_wire_text(response, 157, RENDEZVOUS_VERSION_CAP) == requested_version,
        "future probe requested version mismatch",
    )
    check(
        decode_fixed_wire_text(response, 221, RENDEZVOUS_BUILD_CAP) == requested_build,
        "future probe requested build mismatch",
    )
    expected_message = (
        "CBM could not start because a conflicting CBM process is active "
        "(version; active version {}, build {}; requested version {}, build {}). "
        "Close all CBM sessions and commands, then retry."
    ).format(active_version, active_build, requested_version, requested_build)
    check(
        decode_fixed_wire_text(response, 286, RENDEZVOUS_MESSAGE_CAP) == expected_message,
        "future probe explicit diagnostic mismatch",
    )


class McpClient:
    def __init__(self, binary, env, stderr_path):
        self.stderr_path = stderr_path
        self.stderr_stream = stderr_path.open("w", encoding="utf-8")
        self.process = subprocess.Popen(
            [str(binary)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.stderr_stream,
            text=True,
            bufsize=1,
            env=env,
        )
        self.condition = threading.Condition()
        self.responses = {}
        self.other_output = []
        self.stdout_closed = False
        self.reader = threading.Thread(target=self._read_stdout, daemon=True)
        self.reader.start()

    def _read_stdout(self):
        assert self.process.stdout is not None
        for raw_line in self.process.stdout:
            line = raw_line.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                value = None
            with self.condition:
                if isinstance(value, dict) and "id" in value:
                    self.responses[value["id"]] = value
                else:
                    self.other_output.append(line)
                self.condition.notify_all()
        with self.condition:
            self.stdout_closed = True
            self.condition.notify_all()

    def send(self, value):
        check(self.process.stdin is not None, "client stdin is already closed")
        payload = json.dumps(value, separators=(",", ":"))
        try:
            self.process.stdin.write(payload + "\n")
            self.process.stdin.flush()
        except (BrokenPipeError, OSError) as exc:
            raise SmokeFailure("thin client closed while sending: " + str(exc)) from exc

    def wait_response(self, request_id, timeout=START_TIMEOUT):
        deadline = time.monotonic() + timeout
        with self.condition:
            while request_id not in self.responses:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or self.stdout_closed:
                    break
                self.condition.wait(remaining)
            if request_id in self.responses:
                return self.responses[request_id]
        raise SmokeFailure(
            "no response for request {} (exit={}, stderr={!r}, other={!r})".format(
                request_id,
                self.process.poll(),
                self.stderr_text(),
                self.other_output,
            )
        )

    def has_response(self, request_id):
        with self.condition:
            return request_id in self.responses

    def close_input(self):
        if self.process.stdin is not None and not self.process.stdin.closed:
            try:
                self.process.stdin.close()
            except OSError:
                pass

    def wait(self, timeout):
        return self.process.wait(timeout=timeout)

    def stderr_text(self):
        self.stderr_stream.flush()
        try:
            return self.stderr_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return ""

    def cleanup(self):
        self.close_input()
        try:
            self.process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)
        self.reader.join(timeout=1)
        self.stderr_stream.close()


def assert_rpc_success(response, label):
    check("error" not in response, label + " returned JSON-RPC error: " + repr(response))
    check("result" in response, label + " returned no result: " + repr(response))


def assert_rpc_cancelled(response, label):
    error = response.get("error")
    check(
        isinstance(error, dict) and error.get("code") == -32800,
        label + " did not return JSON-RPC request-cancelled: " + repr(response),
    )
    check("result" not in response, label + " returned both error and result")


def start_ready_mcp_client(
    binary, env, stderr_path, clients, initialize_params, request_base, label
):
    client = McpClient(binary, env, stderr_path)
    clients.append(client)
    client.send(
        {
            "jsonrpc": "2.0",
            "id": request_base,
            "method": "initialize",
            "params": initialize_params,
        }
    )
    assert_rpc_success(client.wait_response(request_base), label + " initialize")
    client.send(
        {
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {},
        }
    )
    client.send(
        {
            "jsonrpc": "2.0",
            "id": request_base + 1,
            "method": "tools/call",
            "params": {"name": "list_projects", "arguments": {}},
        }
    )
    assert_rpc_success(
        client.wait_response(request_base + 1), label + " list_projects"
    )
    return client


def mcp_result_json_payloads(result):
    content = result.get("content") if isinstance(result, dict) else None
    payloads = []
    if isinstance(content, list):
        for item in content:
            text = item.get("text") if isinstance(item, dict) else None
            if not isinstance(text, str):
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                payloads.append(payload)
    return payloads


def tool_json_payloads(response):
    return mcp_result_json_payloads(response.get("result"))


def assert_indexed_tool_response(response, label):
    assert_rpc_success(response, label)
    statuses = [payload.get("status") for payload in tool_json_payloads(response)]
    check(
        "indexed" in statuses,
        label + " did not complete a clean index: " + repr(response),
    )


def run_successful_activation(
    binary,
    env,
    tmpdir,
    activation_log,
    label,
    action,
    arguments,
    expected_source_build,
    minimum_daemon_clients=1,
):
    """Run one native activation and bind its durable audit trail to its PID."""
    records_before = len(json_records(activation_log))
    process = subprocess.Popen(
        [str(binary)] + arguments,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        stdout, stderr = process.communicate(timeout=SHUTDOWN_TIMEOUT)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            process.communicate(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=3)
        raise
    (tmpdir / (label + ".out")).write_text(stdout, encoding="utf-8")
    (tmpdir / (label + ".err")).write_text(stderr, encoding="utf-8")
    check(
        process.returncode == 0,
        label + " failed: stdout={!r}, stderr={!r}".format(stdout, stderr),
    )
    check(
        "Stopping active CBM sessions and operations for {}...".format(action)
        in stdout,
        label + " did not show coordination progress: " + repr(stdout),
    )
    check(
        RESTART_GUIDANCE in stdout,
        label + " did not print the exact restart guidance: " + repr(stdout),
    )

    records = json_records(activation_log)[records_before:]
    records = [
        record
        for record in records
        if record.get("event") == "cbm.activation"
        and record.get("requester_pid") == process.pid
    ]
    phases = [record.get("phase") for record in records]
    check(
        phases == ["requested", "daemon_stopped", "completed"],
        label + " activation audit sequence mismatch: " + repr(records),
    )
    for record in records:
        check(record.get("action") == action, label + " audit action mismatch")
        check(
            record.get("source_build") == expected_source_build,
            label + " audit source build mismatch",
        )
        check(
            isinstance(record.get("source_version"), str)
            and record["source_version"],
            label + " audit source version missing",
        )
        check(
            isinstance(record.get("timestamp_unix_s"), int)
            and record["timestamp_unix_s"] > 0,
            label + " audit timestamp missing",
        )
        check(
            record.get("restart_required") is True,
            label + " audit restart marker missing",
        )
        if action in ("install", "update"):
            target_build = record.get("target_build")
            check(
                isinstance(target_build, str)
                and len(target_build) == 64
                and all(character in "0123456789abcdef" for character in target_build),
                label + " did not audit the pre-staged target build",
            )
        if action == "install":
            check(
                record.get("target_version") == record.get("source_version"),
                label + " install target version audit mismatch",
            )
    stopped = records[1]
    check(
        stopped.get("daemon_active_clients", 0) >= minimum_daemon_clients,
        label + " audit did not record the drained daemon clients: " + repr(stopped),
    )
    check(
        stopped.get("daemon_active_connections", 0) >= minimum_daemon_clients,
        label + " audit did not record the drained daemon connections: "
        + repr(stopped),
    )

    log_status = os.lstat(activation_log)
    check(stat.S_ISREG(log_status.st_mode), "activation log is not a regular file")
    check(log_status.st_uid == os.geteuid(), "activation log has wrong owner")
    check(log_status.st_nlink == 1, "activation log has multiple hard links")
    check(stat.S_IMODE(log_status.st_mode) == 0o600, "activation log is not 0600")
    return records


def assert_coordination_idle(socket_path, lock_specs, description):
    def is_idle():
        return not os.path.lexists(str(socket_path)) and all(
            lock_status(path, record_lock) == "free"
            for path, record_lock in lock_specs
        )

    wait_until(is_idle, SHUTDOWN_TIMEOUT, description)
    for path, record_lock in lock_specs:
        status = lock_status(path, record_lock)
        check(status == "free", "{} remained {} after {}".format(path, status, description))


def create_local_update_release(release_dir, candidate):
    """Create the exact platform archive/checksum pair consumed by update."""
    release_dir.mkdir(parents=True, exist_ok=True)
    release_dir.chmod(0o700)
    if sys.platform == "darwin":
        os_name = "darwin"
        portable = ""
    else:
        os_name = "linux"
        portable = "-portable"
    machine = platform.machine().lower()
    arch = "arm64" if machine in ("arm64", "aarch64") else "amd64"
    asset_name = "codebase-memory-mcp-{}-{}{}.tar.gz".format(
        os_name, arch, portable
    )
    archive = release_dir / asset_name
    with tarfile.open(archive, "w:gz") as stream:
        stream.add(str(candidate), arcname="codebase-memory-mcp", recursive=False)
    digest = sha256_file(archive)
    (release_dir / "checksums.txt").write_text(
        "{}  {}\n".format(digest, asset_name), encoding="ascii"
    )
    return "file://" + str(release_dir)


def assert_no_activation_artifacts(directory, label):
    artifacts = (
        sorted(path.name for path in directory.iterdir() if path.name.startswith(".cbm-"))
        if directory.exists()
        else []
    )
    check(not artifacts, label + " left transaction artifacts: " + repr(artifacts))


def main():
    if os.name == "nt" or sys.platform.startswith(("cygwin", "msys")):
        if os.environ.get("CBM_DAEMON_SMOKE_REQUIRE_RUN") == "1":
            raise SmokeFailure(
                "required daemon smoke is POSIX-only; Windows needs a separate "
                "real-binary named-pipe/Job Object smoke"
            )
        print("SKIP: daemon smoke currently validates POSIX socket/lock lifecycle only")
        return 0

    root = Path(__file__).resolve().parent.parent
    binary = Path(sys.argv[1] if len(sys.argv) > 1 else root / "build/c/codebase-memory-mcp")
    binary = binary.resolve()
    check(binary.is_file() and os.access(binary, os.X_OK), "missing executable: " + str(binary))

    runtime_parent = Path("/private/tmp" if sys.platform == "darwin" else "/tmp")
    runtime_dir = runtime_parent / ("cbm-daemon-" + str(os.geteuid()))
    socket_path = runtime_dir / ("cbm-" + RENDEZVOUS_KEY + ".sock")
    startup_lock = runtime_dir / ("cbm-" + RENDEZVOUS_KEY + ".lock")
    lifetime_lock = runtime_dir / ("cbm-" + RENDEZVOUS_KEY + ".lifetime.lock")
    cohort_admission_lock = runtime_dir / "cbm-version-cohort-admission-v1.lock"
    cohort_lifetime_lock = runtime_dir / "cbm-version-cohort-lifetime-v1.lock"
    cohort_maintenance_lock = runtime_dir / "cbm-version-cohort-maintenance-v1.lock"
    cohort_daemon_lock = runtime_dir / "cbm-version-cohort-daemon-v1.lock"
    coordination_locks = (
        (startup_lock, False),
        (lifetime_lock, True),
        (cohort_admission_lock, True),
        (cohort_lifetime_lock, True),
        (cohort_maintenance_lock, True),
        (cohort_daemon_lock, True),
    )

    coordination_before = [
        (path, lock_status(path, record_lock))
        for path, record_lock in coordination_locks
    ]
    if any(status == "held" for _, status in coordination_before):
        if os.environ.get("CBM_DAEMON_SMOKE_REQUIRE_RUN") == "1":
            raise SmokeFailure(
                "another CBM process or activation is active or starting; "
                "required smoke needs a clean account-wide rendezvous"
            )
        print("SKIP: another CBM process or activation is active or starting")
        return 0
    for path, status in coordination_before:
        check(status == "free", "unsafe coordination lock {}: {}".format(path, status))

    clients = []
    with tempfile.TemporaryDirectory(prefix="cbm-daemon-smoke-") as raw_tmpdir:
        tmpdir = Path(raw_tmpdir)
        home = tmpdir / "home"
        cache = tmpdir / "cache"
        cache_alias = tmpdir / "cache-alias"
        mismatched_cache = tmpdir / "mismatched-cache"
        retargeted_cache = tmpdir / "retargeted-cache"
        repo = tmpdir / "repo"
        success_repo = tmpdir / "success-repo"
        target_dir = home / ".local/bin"
        for directory in (
            home,
            cache,
            mismatched_cache,
            retargeted_cache,
            repo,
            success_repo,
            target_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)
        # Exercise startup hardening of an existing user-selected root, not
        # merely secure creation of a previously absent directory.
        cache.chmod(0o755)
        cache_alias.symlink_to(cache, target_is_directory=True)
        (repo / "hang_me.py").write_text("def smoke():\n    return 1\n", encoding="utf-8")
        (success_repo / "tiny.py").write_text(
            "def daemon_index_smoke():\n    return 1\n", encoding="utf-8"
        )
        target_binary = target_dir / "codebase-memory-mcp"
        target_binary.write_bytes(b"installed binary sentinel\n")
        index_path = cache / "smoke-project.db"
        index_path.write_bytes(b"index sentinel\n")
        descendant_pid_path = tmpdir / "worker-descendant.pid"
        daemon_log = cache / "logs/cbm-daemon.log"
        conflict_log = cache / "logs/daemon-conflicts.ndjson"
        activation_log = cache / "logs/activation-events.ndjson"

        env = os.environ.copy()
        env.update(
            {
                "HOME": str(home),
                "CBM_CACHE_DIR": str(cache_alias),
                "CBM_LOG_LEVEL": "info",
                "CBM_LOG_FORMAT": "json",
                "CBM_TEST_HANG_ON": "hang_me",
                "CBM_TEST_WORKER_DESCENDANT_PID_FILE": str(descendant_pid_path),
            }
        )

        cli_first_process = None
        cli_first_descendant_pid = None
        cli_first_worker_pid = None
        cli_first_worker_pgid = None
        cli_first_worker_stopped = False
        cli_first_competitor = None
        cli_first_competitor_stderr = None
        activation_local_process = None
        activation_local_descendant_pid = None
        activation_local_worker_pid = None
        activation_local_worker_pgid = None
        try:
            version_result = subprocess.run(
                [str(binary), "--version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
                timeout=10,
                check=False,
            )
            check(version_result.returncode == 0, "--version failed: " + version_result.stderr)
            version_prefix = "codebase-memory-mcp "
            check(version_result.stdout.startswith(version_prefix), "unexpected --version output")
            semantic_version = version_result.stdout.strip()[len(version_prefix) :]

            local_cli = subprocess.run(
                [str(binary), "cli", "--progress", "--json", "list_projects"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
                timeout=15,
                check=False,
            )
            check(local_cli.returncode == 0, "local CLI failed: " + local_cli.stderr)
            try:
                json.loads(local_cli.stdout)
            except json.JSONDecodeError as exc:
                raise SmokeFailure("local CLI polluted JSON stdout: " + repr(local_cli.stdout)) from exc
            check(
                "Preparing one-shot local CBM command" in local_cli.stderr,
                "local CLI did not emit coordination feedback: "
                + repr(local_cli.stderr),
            )
            check(
                "Running list_projects locally" in local_cli.stderr,
                "local CLI did not emit immediate progress feedback: " + repr(local_cli.stderr),
            )
            check(
                "Completed list_projects" in local_cli.stderr,
                "local CLI did not emit completion feedback: " + repr(local_cli.stderr),
            )
            check(not socket_path.exists(), "standalone CLI created a daemon socket")
            check(
                lock_status(lifetime_lock, record_lock=True) == "free",
                "standalone CLI retained a daemon lifetime reservation",
            )
            check(not daemon_log.exists(), "standalone CLI started the coordination daemon")

            c1 = McpClient(binary, env, tmpdir / "client-1.err")
            clients.append(c1)
            c2 = McpClient(binary, env, tmpdir / "client-2.err")
            clients.append(c2)

            initialize_params = {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "cbm-daemon-smoke", "version": "1"},
            }
            c1.send(
                {"jsonrpc": "2.0", "id": 101, "method": "initialize", "params": initialize_params}
            )
            c2.send(
                {"jsonrpc": "2.0", "id": 201, "method": "initialize", "params": initialize_params}
            )
            assert_rpc_success(c1.wait_response(101), "client 1 initialize")
            assert_rpc_success(c2.wait_response(201), "client 2 initialize")
            c1.send({"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}})
            c2.send({"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}})
            c1.send(
                {
                    "jsonrpc": "2.0",
                    "id": 102,
                    "method": "tools/call",
                    "params": {"name": "list_projects", "arguments": {}},
                }
            )
            c2.send(
                {
                    "jsonrpc": "2.0",
                    "id": 202,
                    "method": "tools/call",
                    "params": {"name": "list_projects", "arguments": {}},
                }
            )
            assert_rpc_success(c1.wait_response(102), "client 1 list_projects")
            assert_rpc_success(c2.wait_response(202), "client 2 list_projects")

            # A cancellation notification is request-scoped control, not a
            # shorthand for closing the whole MCP session. A stale target
            # while idle must be ignored and the same frontend must continue
            # serving subsequent requests.
            c1.send(
                {
                    "jsonrpc": "2.0",
                    "method": "notifications/cancelled",
                    "params": {"requestId": 999999},
                }
            )
            c1.send({"jsonrpc": "2.0", "id": 104, "method": "ping", "params": {}})
            assert_rpc_success(
                c1.wait_response(104), "request after stale cancellation"
            )
            check(
                c1.process.poll() is None and c2.process.poll() is None,
                "thin client exited early",
            )

            wait_until(
                lambda: socket_path.exists() and lock_status(lifetime_lock, True) == "held",
                START_TIMEOUT,
                "daemon endpoint and lifetime reservation",
            )
            runtime_status = os.stat(runtime_dir)
            cache_status = os.stat(cache)
            socket_status = os.lstat(socket_path)
            check(runtime_status.st_uid == os.geteuid(), "runtime directory has wrong owner")
            check(stat.S_IMODE(runtime_status.st_mode) == 0o700, "runtime directory is not 0700")
            check(cache_status.st_uid == os.geteuid(), "cache root has wrong owner")
            check(stat.S_IMODE(cache_status.st_mode) == 0o700, "cache root is not 0700")
            check(stat.S_ISSOCK(socket_status.st_mode), "daemon endpoint is not a Unix socket")
            check(socket_status.st_uid == os.geteuid(), "daemon endpoint has wrong owner")
            wait_until(
                lambda: len(json_events(daemon_log, "daemon.start")) == 1,
                START_TIMEOUT,
                "one daemon.start event",
            )
            check(
                len(json_events(daemon_log, "daemon.start")) == 1,
                "two simultaneous clients spawned more than one daemon",
            )

            # One account daemon owns one canonical cache root. An exact-build
            # process configured with another root must fail before it joins
            # the daemon generation, while the active sessions remain healthy.
            mismatched_env = env.copy()
            mismatched_env["CBM_CACHE_DIR"] = str(mismatched_cache)
            mismatched_log = mismatched_cache / "logs/daemon-conflicts.ndjson"
            active_cache_fingerprint = hashlib.sha256(
                str(cache.resolve()).encode("utf-8")
            ).hexdigest()
            requested_cache_fingerprint = hashlib.sha256(
                str(mismatched_cache.resolve()).encode("utf-8")
            ).hexdigest()
            mismatched_result = subprocess.run(
                [str(binary)],
                input=json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 203,
                        "method": "initialize",
                        "params": initialize_params,
                    },
                    separators=(",", ":"),
                )
                + "\n",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=mismatched_env,
                timeout=15,
                check=False,
            )
            check(
                mismatched_result.returncode != 0,
                "mismatched CBM_CACHE_DIR unexpectedly joined the account daemon",
            )
            check(
                "active account daemon uses a different cache directory"
                in mismatched_result.stderr
                and "CBM_CACHE_DIR" in mismatched_result.stderr,
                "cache-root conflict did not emit visible remediation guidance: "
                + repr(mismatched_result.stderr),
            )
            wait_until(
                lambda: any(
                    item.get("reason") == "cache_root"
                    and item.get("active_cache") == active_cache_fingerprint
                    and item.get("requested_cache")
                    == requested_cache_fingerprint
                    for item in json_events(
                        mismatched_log, "daemon.version_conflict"
                    )
                ),
                10,
                "cache-root durable conflict log",
            )
            mismatched_log_text = mismatched_log.read_text(
                encoding="utf-8", errors="replace"
            )
            check(
                str(cache) not in mismatched_log_text
                and str(mismatched_cache) not in mismatched_log_text,
                "cache-root conflict log exposed a raw cache path",
            )
            c1.send({"jsonrpc": "2.0", "id": 105, "method": "ping", "params": {}})
            assert_rpc_success(
                c1.wait_response(105), "active session after cache-root rejection"
            )
            check(
                c2.process.poll() is None
                and len(json_events(daemon_log, "daemon.start")) == 1,
                "cache-root conflict disturbed the active daemon generation",
            )

            # A process must keep using the canonical cache root it admitted,
            # even if the original CBM_CACHE_DIR symlink is retargeted later.
            # Otherwise a daemon worker can silently move storage while the
            # cohort still advertises the old root fingerprint.
            cache_alias.unlink()
            cache_alias.symlink_to(retargeted_cache, target_is_directory=True)

            # A real daemon-backed index must cross the physical-worker
            # boundary and finish. This is the regression for the former
            # self-deadlock where the daemon parent held the same project lock
            # its worker was required to acquire.
            c2.send(
                {
                    "jsonrpc": "2.0",
                    "id": 204,
                    "method": "tools/call",
                    "params": {
                        "name": "index_repository",
                        "arguments": {
                            "repo_path": str(success_repo),
                            "name": "smoke-daemon-success",
                            "mode": "fast",
                        },
                    },
                }
            )
            assert_indexed_tool_response(
                c2.wait_response(204, timeout=OPERATION_TIMEOUT),
                "daemon-backed tiny index",
            )
            check(
                (cache / "smoke-daemon-success.db").is_file(),
                "daemon worker did not write through the admitted canonical cache root",
            )
            check(
                not any(retargeted_cache.iterdir()),
                "retargeting the original cache symlink moved daemon storage",
            )
            cache_alias.unlink()
            cache_alias.symlink_to(cache, target_is_directory=True)
            wait_until(
                descendant_pid_path.exists,
                OPERATION_TIMEOUT,
                "successful daemon worker containment probe",
            )
            successful_descendant_pid = int(
                descendant_pid_path.read_text(encoding="ascii").strip()
            )
            wait_until(
                lambda: process_gone_or_zombie(successful_descendant_pid),
                20,
                "successful daemon worker tree to quiesce",
            )
            descendant_pid_path.unlink(missing_ok=True)
            c2.send(
                {
                    "jsonrpc": "2.0",
                    "id": 207,
                    "method": "tools/call",
                    "params": {"name": "list_projects", "arguments": {}},
                }
            )
            persisted_projects = c2.wait_response(207)
            assert_rpc_success(
                persisted_projects, "daemon-backed persisted project listing"
            )
            check(
                any(
                    any(
                        isinstance(project, dict)
                        and project.get("name") == "smoke-daemon-success"
                        for project in payload.get("projects", [])
                    )
                    for payload in tool_json_payloads(persisted_projects)
                    if isinstance(payload.get("projects"), list)
                ),
                "daemon-backed index was not persisted in list_projects: "
                + repr(persisted_projects),
            )

            active_local_cli = subprocess.run(
                [str(binary), "cli", "--json", "list_projects"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
                timeout=15,
                check=False,
            )
            check(
                active_local_cli.returncode == 0,
                "same-build local CLI did not remain independent beside the daemon: "
                + active_local_cli.stderr,
            )
            try:
                json.loads(active_local_cli.stdout)
            except json.JSONDecodeError as exc:
                raise SmokeFailure(
                    "same-build local CLI polluted JSON stdout: "
                    + repr(active_local_cli.stdout)
                ) from exc
            check(
                len(json_events(daemon_log, "daemon.start")) == 1,
                "same-build local CLI restarted the daemon",
            )

            active_fingerprint = sha256_file(binary)
            future_version = "9.0.0-future-wire-v2"
            future_fingerprint = "c" * 64
            probe_future_generation_rendezvous(
                socket_path,
                semantic_version,
                active_fingerprint,
                future_version,
                future_fingerprint,
            )
            wait_until(
                lambda: any(
                    item.get("reason") == "version"
                    and item.get("requested_version") == future_version
                    and item.get("requested_build") == future_fingerprint
                    for item in json_events(conflict_log, "daemon.version_conflict")
                ),
                10,
                "future-generation stable-envelope conflict log",
            )

            conflict_binary = tmpdir / "codebase-memory-mcp-conflict"
            shutil.copy2(binary, conflict_binary)
            if sys.platform == "darwin":
                # Current Apple linkers ad-hoc sign arm64 executables. Appending
                # bytes corrupts that Mach-O before codesign can replace the
                # signature, so create a valid exact-build mismatch by signing
                # the copy with a distinct identifier instead.
                signed = subprocess.run(
                    [
                        "codesign",
                        "--force",
                        "--sign",
                        "-",
                        "--identifier",
                        "com.deusdata.cbm.daemon-smoke-conflict",
                        str(conflict_binary),
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=20,
                    check=False,
                )
                check(
                    signed.returncode == 0,
                    "could not ad-hoc sign conflict binary: " + signed.stderr,
                )
            else:
                # ELF permits trailing bytes, giving the fixture a different
                # exact build fingerprint while preserving executability.
                with conflict_binary.open("ab") as stream:
                    stream.write(b"\x00cbm-daemon-smoke-conflicting-build")
            requested_fingerprint = sha256_file(conflict_binary)
            check(
                active_fingerprint != requested_fingerprint,
                "conflict fixture hash did not change",
            )
            expected_conflict = (
                "CBM could not start because a conflicting CBM process is active "
                "(build; active version {}, build {}; requested version {}, build {}). "
                "Close all CBM sessions and commands, then retry."
            ).format(
                semantic_version,
                active_fingerprint,
                semantic_version,
                requested_fingerprint,
            )
            conflicts_before_cli = len(
                [
                    item
                    for item in json_events(conflict_log, "daemon.version_conflict")
                    if item.get("reason") == "build"
                    and item.get("requested_build") == requested_fingerprint
                ]
            )
            conflict_cli_result = subprocess.run(
                [str(conflict_binary), "cli", "--progress", "--json", "list_projects"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
                timeout=15,
                check=False,
            )
            check(
                conflict_cli_result.returncode != 0,
                "conflicting one-shot CLI unexpectedly ran beside the active daemon",
            )
            check(
                "codebase-memory-mcp: " + expected_conflict
                in conflict_cli_result.stderr,
                "conflicting one-shot CLI did not emit the exact visible diagnostic: "
                + repr(conflict_cli_result.stderr),
            )
            wait_until(
                lambda: len(
                    [
                        item
                        for item in json_events(conflict_log, "daemon.version_conflict")
                        if item.get("reason") == "build"
                        and item.get("requested_build") == requested_fingerprint
                    ]
                )
                > conflicts_before_cli,
                10,
                "one-shot CLI durable conflict log",
            )
            conflicts_before_mcp = len(
                [
                    item
                    for item in json_events(conflict_log, "daemon.version_conflict")
                    if item.get("reason") == "build"
                    and item.get("requested_build") == requested_fingerprint
                ]
            )
            conflict_result = subprocess.run(
                [str(conflict_binary)],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
                timeout=15,
                check=False,
            )
            check(conflict_result.returncode != 0, "conflicting build unexpectedly connected")
            check(
                expected_conflict in conflict_result.stderr,
                "conflicting build did not emit exact visible diagnostic: "
                + repr(conflict_result.stderr),
            )
            wait_until(
                lambda: len(
                    [
                        item
                        for item in json_events(
                            conflict_log, "daemon.version_conflict"
                        )
                        if item.get("reason") == "build"
                        and item.get("requested_build") == requested_fingerprint
                    ]
                )
                > conflicts_before_mcp,
                10,
                "MCP durable conflict log increment",
            )
            conflict_records = json_events(conflict_log, "daemon.version_conflict")
            check(
                any(
                    item.get("reason") == "build"
                    and item.get("active_build") == active_fingerprint
                    and item.get("requested_build") == requested_fingerprint
                    for item in conflict_records
                ),
                "conflict log did not bind the exact active/requested builds",
            )
            conflict_status = os.stat(conflict_log)
            check(conflict_status.st_uid == os.geteuid(), "conflict log has wrong owner")
            check(stat.S_IMODE(conflict_status.st_mode) == 0o600, "conflict log is not 0600")

            c1.send(
                {
                    "jsonrpc": "2.0",
                    "id": 103,
                    "method": "tools/call",
                    "params": {
                        "name": "index_repository",
                        "arguments": {"repo_path": str(repo), "mode": "fast"},
                    },
                }
            )
            wait_until(descendant_pid_path.exists, OPERATION_TIMEOUT, "session-owned index worker")
            descendant_pid = int(descendant_pid_path.read_text(encoding="ascii").strip())
            check(
                not process_gone_or_zombie(descendant_pid),
                "index worker descendant exited before cancellation",
            )
            check(
                not c1.has_response(103),
                "index request completed instead of reaching hang injector",
            )
            check(c2.process.poll() is None, "second session exited during first session operation")

            # A wrong request id must not cancel the active operation or close
            # the session. The exact request id must then promptly tear down
            # this session's supervised worker tree while c2 remains alive.
            c1.send(
                {
                    "jsonrpc": "2.0",
                    "method": "notifications/cancelled",
                    "params": {"requestId": 999999},
                }
            )
            # This black-box notification has no acknowledgement, so this is
            # only a production sanity window. Deterministic wrong-token
            # routing and completion barriers live in daemon_runtime tests.
            wrong_cancel_deadline = time.monotonic() + 0.5
            while time.monotonic() < wrong_cancel_deadline:
                check(
                    c1.process.poll() is None,
                    "wrong request-id cancellation closed the frontend",
                )
                check(
                    not process_gone_or_zombie(descendant_pid),
                    "wrong request-id cancellation stopped the worker",
                )
                time.sleep(0.02)
            c1.send(
                {
                    "jsonrpc": "2.0",
                    "method": "notifications/cancelled",
                    "params": {"requestId": 103},
                }
            )
            assert_rpc_cancelled(
                c1.wait_response(103, timeout=20),
                "matching request cancellation",
            )
            wait_until(
                lambda: process_gone_or_zombie(descendant_pid),
                20,
                "request-id-cancelled session worker tree to exit",
            )
            c1.send({"jsonrpc": "2.0", "id": 105, "method": "ping", "params": {}})
            assert_rpc_success(
                c1.wait_response(105), "request after matching cancellation"
            )
            check(
                c1.process.poll() is None,
                "matching request cancellation closed the whole MCP session",
            )

            # EOF remains the terminal ownership boundary. Start a second
            # session-owned operation, close this frontend, and require its
            # independent worker tree to be contained without affecting c2.
            descendant_pid_path.unlink(missing_ok=True)
            c1.send(
                {
                    "jsonrpc": "2.0",
                    "id": 106,
                    "method": "tools/call",
                    "params": {
                        "name": "index_repository",
                        "arguments": {
                            "repo_path": str(repo),
                            "name": "smoke-session-close",
                            "mode": "fast",
                        },
                    },
                }
            )
            wait_until(
                descendant_pid_path.exists,
                OPERATION_TIMEOUT,
                "session-close index worker",
            )
            close_descendant_pid = int(
                descendant_pid_path.read_text(encoding="ascii").strip()
            )
            check(
                not process_gone_or_zombie(close_descendant_pid),
                "session-close worker exited before EOF",
            )
            c1.close_input()
            check(
                c1.wait(timeout=15) == 0,
                "EOF-cancelled frontend exited nonzero",
            )
            wait_until(
                lambda: process_gone_or_zombie(close_descendant_pid),
                20,
                "EOF-cancelled session worker tree to exit",
            )
            c2.send(
                {
                    "jsonrpc": "2.0",
                    "id": 203,
                    "method": "tools/call",
                    "params": {"name": "list_projects", "arguments": {}},
                }
            )
            assert_rpc_success(c2.wait_response(203), "surviving client list_projects")
            check(
                lock_status(lifetime_lock, True) == "held",
                "daemon stopped while one client remained",
            )

            c2.close_input()
            check(c2.wait(timeout=15) == 0, "final generation-one frontend exited nonzero")

            # The final DISCONNECT acknowledgement is sent only after the old
            # runtime has entered STOPPING. Launch the next MCP frontend
            # immediately: the same bootstrap attempt must wait for cleanup,
            # serialize, and become the first client of generation two.
            turnover_client = McpClient(
                binary, env, tmpdir / "turnover-client.err"
            )
            clients.append(turnover_client)
            turnover_client.send(
                {
                    "jsonrpc": "2.0",
                    "id": 205,
                    "method": "initialize",
                    "params": initialize_params,
                }
            )
            assert_rpc_success(
                turnover_client.wait_response(205),
                "immediate post-shutdown client initialize",
            )
            turnover_client.send(
                {
                    "jsonrpc": "2.0",
                    "method": "notifications/initialized",
                    "params": {},
                }
            )
            turnover_client.send(
                {
                    "jsonrpc": "2.0",
                    "id": 206,
                    "method": "tools/call",
                    "params": {"name": "list_projects", "arguments": {}},
                }
            )
            assert_rpc_success(
                turnover_client.wait_response(206),
                "immediate post-shutdown client list_projects",
            )
            wait_until(
                lambda: len(json_events(daemon_log, "daemon.start")) == 2,
                START_TIMEOUT,
                "second daemon generation after immediate turnover",
            )
            turnover_client.close_input()
            check(
                turnover_client.wait(timeout=15) == 0,
                "turnover generation frontend exited nonzero",
            )
            wait_until(
                lambda: not socket_path.exists() and lock_status(lifetime_lock, True) == "free",
                SHUTDOWN_TIMEOUT,
                "turnover generation endpoint and reservation cleanup",
            )
            wait_until(
                lambda: len(json_events(daemon_log, "daemon.stop")) == 2,
                10,
                "two daemon.stop events",
            )
            check(
                len(json_events(daemon_log, "daemon.start")) == 2,
                "immediate turnover did not start exactly one replacement daemon",
            )
            check(
                len(json_events(daemon_log, "daemon.stop")) == 2,
                "turnover generations did not each stop exactly once",
            )
            check(
                daemon_lifecycle_sequence(daemon_log)[:4]
                == ["daemon.start", "daemon.stop", "daemon.start", "daemon.stop"],
                "daemon turnover lifecycle overlapped or reordered: "
                + repr(daemon_lifecycle_sequence(daemon_log)),
            )
            wait_until(
                lambda: lock_status(startup_lock, record_lock=False) == "free",
                SHUTDOWN_TIMEOUT,
                "turnover generation startup/participant guard release",
            )
            check(
                process_gone_or_zombie(descendant_pid),
                "cancelled worker tree survived daemon shutdown",
            )

            # Reverse the version-admission order: a one-shot CLI and its
            # supervised worker own the exact-build cohort first. A different
            # ordinary executable must fail before it can spawn/connect a
            # daemon. Native activation is exercised separately below because
            # it deliberately publishes maintenance and cancels this work.
            cli_first_pid_path = tmpdir / "cli-first-worker-descendant.pid"
            cli_first_lock_owner_path = (
                tmpdir / "cli-first-worker-project-lock.pid"
            )
            cli_first_env = env.copy()
            cli_first_env["CBM_TEST_WORKER_DESCENDANT_PID_FILE"] = str(
                cli_first_pid_path
            )
            cli_first_env["CBM_TEST_WORKER_PROJECT_LOCK_PID_FILE"] = str(
                cli_first_lock_owner_path
            )
            cli_first_process = subprocess.Popen(
                [
                    str(binary),
                    "cli",
                    "--progress",
                    "--json",
                    "index_repository",
                    "--repo-path",
                    str(repo),
                    "--name",
                    "smoke-cli-first",
                    "--mode",
                    "fast",
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=cli_first_env,
            )
            wait_until(
                lambda: read_private_pid_fields(cli_first_pid_path, 1)
                is not None,
                OPERATION_TIMEOUT,
                "CLI-first supervised worker",
            )
            wait_until(
                lambda: read_private_pid_fields(cli_first_lock_owner_path, 2)
                is not None,
                OPERATION_TIMEOUT,
                "physical worker to acquire the native project lock",
            )
            (cli_first_descendant_pid,) = read_private_pid_fields(
                cli_first_pid_path, 1
            )
            cli_first_worker_pid, cli_first_worker_pgid = (
                read_private_pid_fields(cli_first_lock_owner_path, 2)
            )
            check(
                cli_first_process.poll() is None
                and not process_gone_or_zombie(cli_first_descendant_pid)
                and worker_tree_identity_matches(
                    cli_first_process.pid,
                    cli_first_worker_pid,
                    cli_first_worker_pgid,
                    cli_first_descendant_pid,
                ),
                "CLI-first worker ownership marker did not identify the "
                "supervisor's isolated physical worker tree",
            )
            check(not socket_path.exists(), "CLI-first operation created a daemon socket")
            check(
                lock_status(lifetime_lock, True) == "free",
                "CLI-first operation acquired the daemon lifetime reservation",
            )
            check(
                lock_status(startup_lock, record_lock=False) == "held",
                "CLI-first operation did not retain the daemon-start transition",
            )

            cli_first_conflicts_before = len(
                [
                    item
                    for item in json_events(conflict_log, "daemon.version_conflict")
                    if item.get("requested_build") == requested_fingerprint
                ]
            )
            cli_first_conflict = subprocess.run(
                [str(conflict_binary)],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
                timeout=15,
                check=False,
            )
            check(
                cli_first_conflict.returncode != 0,
                "conflicting MCP started while a different CLI build was active",
            )
            check(
                "CBM could not start because a conflicting CBM process is active"
                in cli_first_conflict.stderr,
                "CLI-first conflict did not emit a visible diagnostic: "
                + repr(cli_first_conflict.stderr),
            )
            check(
                not socket_path.exists() and lock_status(lifetime_lock, True) == "free",
                "rejected CLI-first conflict spawned a daemon generation",
            )
            wait_until(
                lambda: len(
                    [
                        item
                        for item in json_events(conflict_log, "daemon.version_conflict")
                        if item.get("reason") == "build"
                        and item.get("requested_build") == requested_fingerprint
                    ]
                )
                > cli_first_conflicts_before,
                10,
                "CLI-first durable conflict log",
            )
            # A same-build MCP session must still be able to start the shared
            # daemon while an unrelated one-shot CLI operation is active. The
            # daemon then stops with its final MCP session; the standalone CLI
            # neither becomes a daemon session nor keeps that daemon alive.
            overlap_client = McpClient(
                binary, cli_first_env, tmpdir / "cli-first-overlap-client.err"
            )
            clients.append(overlap_client)
            overlap_client.send(
                {
                    "jsonrpc": "2.0",
                    "id": 301,
                    "method": "initialize",
                    "params": initialize_params,
                }
            )
            assert_rpc_success(
                overlap_client.wait_response(301),
                "CLI-first overlap client initialize",
            )
            overlap_client.send(
                {
                    "jsonrpc": "2.0",
                    "method": "notifications/initialized",
                    "params": {},
                }
            )
            overlap_client.send(
                {
                    "jsonrpc": "2.0",
                    "id": 302,
                    "method": "tools/call",
                    "params": {"name": "list_projects", "arguments": {}},
                }
            )
            assert_rpc_success(
                overlap_client.wait_response(302),
                "CLI-first overlap client list_projects",
            )
            wait_until(
                lambda: socket_path.exists()
                and lock_status(lifetime_lock, True) == "held",
                START_TIMEOUT,
                "same-build daemon startup during local CLI work",
            )
            wait_until(
                lambda: len(json_events(daemon_log, "daemon.start")) == 3,
                START_TIMEOUT,
                "third daemon generation during local CLI work",
            )
            check(
                cli_first_process.poll() is None
                and not process_gone_or_zombie(cli_first_descendant_pid),
                "daemon startup terminated the independent local CLI operation",
            )

            overlap_client.close_input()
            check(
                overlap_client.wait(timeout=15) == 0,
                "CLI-overlap daemon frontend exited nonzero",
            )
            wait_until(
                lambda: not socket_path.exists()
                and lock_status(lifetime_lock, True) == "free",
                SHUTDOWN_TIMEOUT,
                "overlap daemon shutdown after its final MCP session",
            )
            wait_until(
                lambda: len(json_events(daemon_log, "daemon.stop")) == 3,
                10,
                "third daemon.stop event",
            )
            check(
                daemon_lifecycle_sequence(daemon_log)[:6]
                == [
                    "daemon.start",
                    "daemon.stop",
                    "daemon.start",
                    "daemon.stop",
                    "daemon.start",
                    "daemon.stop",
                ],
                "three daemon generations overlapped or reordered: "
                + repr(daemon_lifecycle_sequence(daemon_log)),
            )
            check(
                cli_first_process.poll() is None
                and not process_gone_or_zombie(cli_first_descendant_pid),
                "final MCP session shutdown cancelled independent local CLI work",
            )
            check(
                lock_status(startup_lock, record_lock=False) == "held",
                "local CLI lost its legacy compatibility guard after daemon shutdown",
            )

            # The owner-only marker was published by the physical worker only
            # after it acquired the native lease. Freeze that verified worker
            # and require a same-project competitor to remain blocked, proving
            # the lease stays at the worker boundary rather than its polling
            # supervisor. Revalidate the complete ancestry/group immediately
            # before signaling so a stale or reparented PID is never stopped.
            check(
                cli_first_process.poll() is None
                and worker_tree_identity_matches(
                    cli_first_process.pid,
                    cli_first_worker_pid,
                    cli_first_worker_pgid,
                    cli_first_descendant_pid,
                ),
                "CLI-first physical worker identity changed before SIGSTOP",
            )
            try:
                os.kill(cli_first_worker_pid, signal.SIGSTOP)
            except OSError as exc:
                raise SmokeFailure(
                    "could not stop the verified CLI-first worker: " + str(exc)
                ) from exc
            cli_first_worker_stopped = True
            wait_until(
                lambda: worker_is_stopped(
                    cli_first_worker_pid, cli_first_worker_pgid
                ),
                5,
                "verified CLI-first worker to stop",
            )
            competitor_stderr_path = tmpdir / "cli-first-competitor.err"
            cli_first_competitor_stderr = competitor_stderr_path.open(
                "w", encoding="utf-8"
            )
            cli_first_competitor = subprocess.Popen(
                [
                    str(binary),
                    "cli",
                    "--progress",
                    "--json",
                    "delete_project",
                    "--project",
                    "smoke-cli-first",
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=cli_first_competitor_stderr,
                text=True,
                env=cli_first_env,
            )

            def competitor_waiting_or_exited():
                try:
                    text = competitor_stderr_path.read_text(
                        encoding="utf-8", errors="replace"
                    )
                except OSError:
                    text = ""
                return (
                    "Waiting for another CBM mutation of smoke-cli-first" in text
                    or cli_first_competitor.poll() is not None
                )

            wait_until(
                competitor_waiting_or_exited,
                15,
                "same-project competitor to reach worker-owned project lock",
            )
            cli_first_competitor_stderr.flush()
            competitor_stderr = competitor_stderr_path.read_text(
                encoding="utf-8", errors="replace"
            )
            check(
                cli_first_competitor.poll() is None
                and "Waiting for another CBM mutation of smoke-cli-first"
                in competitor_stderr,
                "worker did not retain same-project exclusion while frozen: "
                + competitor_stderr,
            )

            # SIGTERM would ask the CLI supervisor to perform an orderly
            # cancellation. SIGKILL models an abrupt supervisor crash. POSIX
            # may immediately SIGHUP/SIGCONT the newly orphaned stopped group;
            # if it has not, resume only the still-verified worker so its parent
            # watchdog can quiesce the group and release the competitor.
            cli_first_process.kill()
            try:
                cli_first_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                cli_first_process.kill()
                cli_first_process.wait(timeout=5)
            if worker_tree_identity_matches(
                None,
                cli_first_worker_pid,
                cli_first_worker_pgid,
                cli_first_descendant_pid,
            ):
                try:
                    os.kill(cli_first_worker_pid, signal.SIGCONT)
                except OSError:
                    pass
            cli_first_worker_stopped = False
            wait_until(
                lambda: process_gone_or_zombie(cli_first_descendant_pid),
                20,
                "CLI-first worker tree to exit after supervisor termination",
            )
            try:
                competitor_stdout, _ = cli_first_competitor.communicate(timeout=15)
            except subprocess.TimeoutExpired:
                cli_first_competitor.kill()
                competitor_stdout, _ = cli_first_competitor.communicate(timeout=5)
            try:
                competitor_result = json.loads(competitor_stdout)
            except json.JSONDecodeError as exc:
                raise SmokeFailure(
                    "post-worker mutation polluted JSON stdout: "
                    + repr(competitor_stdout)
                ) from exc
            competitor_statuses = {
                payload.get("status")
                for payload in mcp_result_json_payloads(competitor_result)
            }
            check(
                bool(competitor_statuses & {"deleted", "not_found"}),
                "same-project mutation did not acquire the released worker lock: "
                + repr(competitor_result)
                + " stderr: "
                + competitor_stderr_path.read_text(
                    encoding="utf-8", errors="replace"
                ),
            )
            if "deleted" in competitor_statuses:
                check(
                    cli_first_competitor.returncode == 0,
                    "successful post-worker deletion exited nonzero",
                )
            else:
                check(
                    cli_first_competitor.returncode != 0,
                    "post-worker not_found tool error reported success",
                )
            cli_first_competitor = None
            cli_first_competitor_stderr.close()
            cli_first_competitor_stderr = None
            wait_until(
                lambda: lock_status(startup_lock, record_lock=False) == "free",
                10,
                "CLI-first daemon-start transition to release",
            )

            # Once the final participant exits, the cohort is crash-released
            # and a different exact build may become the next generation.
            turnover = subprocess.run(
                [str(conflict_binary), "cli", "--json", "list_projects"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
                timeout=15,
                check=False,
            )
            check(
                turnover.returncode == 0,
                "exact-build cohort did not turn over after CLI exit: "
                + turnover.stderr,
            )
            try:
                json.loads(turnover.stdout)
            except json.JSONDecodeError as exc:
                raise SmokeFailure(
                    "post-turnover CLI polluted JSON stdout: " + repr(turnover.stdout)
                ) from exc
            check(
                not socket_path.exists()
                and lock_status(lifetime_lock, record_lock=True) == "free"
                and lock_status(startup_lock, record_lock=False) == "free",
                "post-turnover CLI left daemon/startup ownership behind",
            )
            check(
                len(json_events(daemon_log, "daemon.start")) == 3
                and len(json_events(daemon_log, "daemon.stop")) == 3,
                "final smoke state did not contain exactly three clean daemon generations",
            )
            check(
                daemon_lifecycle_sequence(daemon_log)
                == [
                    "daemon.start",
                    "daemon.stop",
                    "daemon.start",
                    "daemon.stop",
                    "daemon.start",
                    "daemon.stop",
                ],
                "unexpected final daemon lifecycle sequence: "
                + repr(daemon_lifecycle_sequence(daemon_log)),
            )

            # Native activation is the deliberate exception to exact-build
            # admission. Exercise that exception with a different-build
            # candidate while both kinds of coordinated work are live: one
            # MCP daemon session and one hanging local CLI worker tree. The
            # candidate must authenticate to the old daemon, publish
            # maintenance, cancel both, wait for their leases, and only then
            # install into the explicit temporary directory.
            source_binary_before = sha256_file(binary)
            conflict_binary_before = sha256_file(conflict_binary)
            check(
                source_binary_before == active_fingerprint,
                "source binary changed before activation smoke",
            )
            check(
                conflict_binary_before == requested_fingerprint,
                "different-build candidate changed before activation smoke",
            )
            install_dir = tmpdir / "activation-install-bin"
            install_dir.mkdir(mode=0o700)
            install_target = install_dir / "codebase-memory-mcp"
            install_pid_path = tmpdir / "activation-install-worker-descendant.pid"
            install_lock_owner_path = tmpdir / "activation-install-worker-lock.pid"
            activation_local_env = env.copy()
            activation_local_env["CBM_TEST_WORKER_DESCENDANT_PID_FILE"] = str(
                install_pid_path
            )
            activation_local_env["CBM_TEST_WORKER_PROJECT_LOCK_PID_FILE"] = str(
                install_lock_owner_path
            )

            install_starts = len(json_events(daemon_log, "daemon.start"))
            install_stops = len(json_events(daemon_log, "daemon.stop"))
            install_client = start_ready_mcp_client(
                binary,
                env,
                tmpdir / "activation-install-client.err",
                clients,
                initialize_params,
                401,
                "activation install client",
            )
            wait_until(
                lambda: len(json_events(daemon_log, "daemon.start"))
                == install_starts + 1,
                START_TIMEOUT,
                "install activation daemon generation",
            )
            activation_local_process = subprocess.Popen(
                [
                    str(binary),
                    "cli",
                    "--progress",
                    "--json",
                    "index_repository",
                    "--repo-path",
                    str(repo),
                    "--name",
                    "smoke-activation-install",
                    "--mode",
                    "fast",
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=activation_local_env,
            )
            wait_until(
                lambda: read_private_pid_fields(install_pid_path, 1) is not None,
                OPERATION_TIMEOUT,
                "activation-cancelled CLI worker descendant",
            )
            wait_until(
                lambda: read_private_pid_fields(install_lock_owner_path, 2)
                is not None,
                OPERATION_TIMEOUT,
                "activation-cancelled CLI worker lock owner",
            )
            (activation_local_descendant_pid,) = read_private_pid_fields(
                install_pid_path, 1
            )
            activation_local_worker_pid, activation_local_worker_pgid = (
                read_private_pid_fields(install_lock_owner_path, 2)
            )
            check(
                activation_local_process.poll() is None
                and worker_tree_identity_matches(
                    activation_local_process.pid,
                    activation_local_worker_pid,
                    activation_local_worker_pgid,
                    activation_local_descendant_pid,
                ),
                "activation CLI marker did not identify its isolated worker tree",
            )

            install_activation_records = run_successful_activation(
                conflict_binary,
                env,
                tmpdir,
                activation_log,
                "activation-install",
                "install",
                [
                    "install",
                    "--force",
                    "--yes",
                    "--skip-config",
                    "--dir",
                    str(install_dir),
                ],
                requested_fingerprint,
            )
            local_stdout, local_stderr = activation_local_process.communicate(
                timeout=10
            )
            (tmpdir / "activation-local-cli.out").write_text(
                local_stdout, encoding="utf-8"
            )
            (tmpdir / "activation-local-cli.err").write_text(
                local_stderr, encoding="utf-8"
            )
            check(
                activation_local_process.returncode != 0,
                "maintenance-cancelled local CLI reported success",
            )
            check(
                "active CLI command is stopping for install/update/uninstall"
                in local_stderr,
                "local CLI did not report maintenance cancellation: "
                + repr(local_stderr),
            )
            wait_until(
                lambda: process_gone_or_zombie(activation_local_descendant_pid),
                20,
                "maintenance-cancelled local CLI worker tree",
            )
            check(
                install_client.wait(timeout=15) >= 0,
                "install activation MCP frontend was killed by a signal",
            )
            wait_until(
                lambda: len(json_events(daemon_log, "daemon.stop"))
                == install_stops + 1,
                10,
                "install activation daemon.stop",
            )
            assert_coordination_idle(
                socket_path,
                coordination_locks,
                "install activation coordination cleanup",
            )
            assert_no_activation_artifacts(install_dir, "install activation")
            check(install_target.is_file(), "install activation did not publish target")
            install_status = os.lstat(install_target)
            check(stat.S_ISREG(install_status.st_mode), "installed target is not regular")
            check(install_status.st_uid == os.geteuid(), "installed target has wrong owner")
            check(install_status.st_nlink == 1, "installed target has multiple links")
            check(
                stat.S_IMODE(install_status.st_mode) & 0o022 == 0,
                "installed target is group/world writable",
            )
            installed_version = subprocess.run(
                [str(install_target), "--version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
                timeout=10,
                check=False,
            )
            check(
                installed_version.returncode == 0
                and installed_version.stdout.strip()
                == "codebase-memory-mcp " + semantic_version,
                "different-build install target is not executable: "
                + installed_version.stderr,
            )
            installed_build = sha256_file(install_target)
            check(
                installed_build != active_fingerprint,
                "different-build activation installed the old active build",
            )
            check(
                install_activation_records[-1].get("target_build")
                == installed_build,
                "install audit target build does not match the published binary",
            )
            check(
                target_binary.read_bytes() == b"installed binary sentinel\n",
                "custom-dir install mutated the canonical binary",
            )
            check(
                index_path.read_bytes() == b"index sentinel\n",
                "install without --reset-indexes removed an index",
            )
            check(
                sha256_file(binary) == source_binary_before
                and sha256_file(conflict_binary) == conflict_binary_before,
                "install activation modified a source executable",
            )

            # Build the complete update payload and checksum before starting
            # the next daemon. CBM_DOWNLOAD_URL points at this file:// fixture,
            # so the smoke cannot contact the network. Update must then drain
            # its live MCP generation before replacing only the temp HOME
            # installation and performing its documented index reset.
            update_release = tmpdir / "activation-update-release"
            update_env = env.copy()
            update_env["CBM_DOWNLOAD_URL"] = create_local_update_release(
                update_release, conflict_binary
            )
            update_starts = len(json_events(daemon_log, "daemon.start"))
            update_stops = len(json_events(daemon_log, "daemon.stop"))
            update_client = start_ready_mcp_client(
                binary,
                update_env,
                tmpdir / "activation-update-client.err",
                clients,
                initialize_params,
                501,
                "activation update client",
            )
            wait_until(
                lambda: len(json_events(daemon_log, "daemon.start"))
                == update_starts + 1,
                START_TIMEOUT,
                "update activation daemon generation",
            )
            update_activation_records = run_successful_activation(
                binary,
                update_env,
                tmpdir,
                activation_log,
                "activation-update",
                "update",
                ["update", "--force", "--standard", "--yes"],
                active_fingerprint,
            )
            check(
                update_client.wait(timeout=15) >= 0,
                "update activation MCP frontend was killed by a signal",
            )
            wait_until(
                lambda: len(json_events(daemon_log, "daemon.stop"))
                == update_stops + 1,
                10,
                "update activation daemon.stop",
            )
            assert_coordination_idle(
                socket_path,
                coordination_locks,
                "update activation coordination cleanup",
            )
            assert_no_activation_artifacts(target_dir, "update activation")
            updated_version = subprocess.run(
                [str(target_binary), "--version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
                timeout=10,
                check=False,
            )
            check(
                updated_version.returncode == 0
                and updated_version.stdout.strip()
                == "codebase-memory-mcp " + semantic_version,
                "updated target is not executable: " + updated_version.stderr,
            )
            updated_status = os.lstat(target_binary)
            check(stat.S_ISREG(updated_status.st_mode), "updated target is not regular")
            check(updated_status.st_uid == os.geteuid(), "updated target has wrong owner")
            check(updated_status.st_nlink == 1, "updated target has multiple links")
            check(
                stat.S_IMODE(updated_status.st_mode) & 0o022 == 0,
                "updated target is group/world writable",
            )
            updated_build = sha256_file(target_binary)
            check(
                updated_build != active_fingerprint,
                "update republished the old active build",
            )
            check(
                update_activation_records[-1].get("target_build")
                == updated_build,
                "update audit target build does not match the published binary",
            )
            check(not index_path.exists(), "update did not clear the opted-in index")
            check(
                sha256_file(install_target) == installed_build,
                "update mutated the separate custom-dir installation",
            )
            check(
                sha256_file(binary) == source_binary_before
                and sha256_file(conflict_binary) == conflict_binary_before,
                "update activation modified a source executable",
            )

            # Launch the activated target itself as the final daemon build,
            # then uninstall it from the original executable. This is another
            # authenticated cross-build drain and proves removal waits until
            # no process is still executing the target.
            uninstall_starts = len(json_events(daemon_log, "daemon.start"))
            uninstall_stops = len(json_events(daemon_log, "daemon.stop"))
            uninstall_client = start_ready_mcp_client(
                target_binary,
                env,
                tmpdir / "activation-uninstall-client.err",
                clients,
                initialize_params,
                601,
                "activation uninstall client",
            )
            wait_until(
                lambda: len(json_events(daemon_log, "daemon.start"))
                == uninstall_starts + 1,
                START_TIMEOUT,
                "uninstall activation daemon generation",
            )
            uninstall_activation_records = run_successful_activation(
                binary,
                env,
                tmpdir,
                activation_log,
                "activation-uninstall",
                "uninstall",
                ["uninstall", "--yes"],
                active_fingerprint,
            )
            check(
                all(
                    "target_build" not in record
                    for record in uninstall_activation_records
                ),
                "uninstall audit claimed a replacement target build",
            )
            check(
                uninstall_client.wait(timeout=15) >= 0,
                "uninstall activation MCP frontend was killed by a signal",
            )
            wait_until(
                lambda: len(json_events(daemon_log, "daemon.stop"))
                == uninstall_stops + 1,
                10,
                "uninstall activation daemon.stop",
            )
            assert_coordination_idle(
                socket_path,
                coordination_locks,
                "uninstall activation coordination cleanup",
            )
            assert_no_activation_artifacts(target_dir, "uninstall activation")
            assert_no_activation_artifacts(install_dir, "uninstall activation")
            check(not target_binary.exists(), "uninstall did not remove its target")
            check(
                install_target.exists()
                and sha256_file(install_target) == installed_build,
                "uninstall mutated the separate custom-dir installation",
            )
            check(
                sha256_file(binary) == source_binary_before
                and sha256_file(conflict_binary) == conflict_binary_before,
                "uninstall activation modified a source executable",
            )
            check(
                len(json_events(daemon_log, "daemon.start"))
                == len(json_events(daemon_log, "daemon.stop")),
                "activation smoke left an unmatched daemon generation",
            )

            print(
                "ok: shared daemon lifecycle, bidirectional version conflicts, "
                "coordinated install/update/uninstall, CLI maintenance cancellation, "
                "and session cancellation passed"
            )
            return 0
        finally:
            failed = sys.exc_info()[0] is not None
            if cli_first_competitor and cli_first_competitor.poll() is None:
                cli_first_competitor.terminate()
                try:
                    cli_first_competitor.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    cli_first_competitor.kill()
                    cli_first_competitor.wait(timeout=3)
            if cli_first_competitor_stderr:
                cli_first_competitor_stderr.close()
            if (
                cli_first_worker_stopped
                and cli_first_worker_pid
                and worker_tree_identity_matches(
                    cli_first_process.pid
                    if cli_first_process
                    and cli_first_process.poll() is None
                    else None,
                    cli_first_worker_pid,
                    cli_first_worker_pgid,
                    cli_first_descendant_pid,
                )
            ):
                try:
                    os.kill(cli_first_worker_pid, signal.SIGCONT)
                    cli_first_worker_stopped = False
                except OSError:
                    pass
            if cli_first_process and cli_first_process.poll() is None:
                cli_first_process.terminate()
                try:
                    cli_first_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    cli_first_process.kill()
                    cli_first_process.wait(timeout=5)
            if (
                cli_first_descendant_pid
                and not process_gone_or_zombie(cli_first_descendant_pid)
                and worker_group_contains_descendant(
                    cli_first_worker_pgid, cli_first_descendant_pid
                )
            ):
                try:
                    os.killpg(cli_first_worker_pgid, signal.SIGKILL)
                except OSError:
                    pass
            if activation_local_process and activation_local_process.poll() is None:
                activation_local_process.terminate()
                try:
                    activation_local_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    activation_local_process.kill()
                    activation_local_process.wait(timeout=5)
            if (
                activation_local_descendant_pid
                and not process_gone_or_zombie(activation_local_descendant_pid)
                and worker_group_contains_descendant(
                    activation_local_worker_pgid,
                    activation_local_descendant_pid,
                )
            ):
                try:
                    os.killpg(activation_local_worker_pgid, signal.SIGKILL)
                except OSError:
                    pass
            for client in clients:
                client.cleanup()
            if failed:
                diagnostics = []
                for path in (
                    daemon_log,
                    conflict_log,
                    tmpdir / "client-1.err",
                    tmpdir / "client-2.err",
                    tmpdir / "turnover-client.err",
                    tmpdir / "cli-first-overlap-client.err",
                    activation_log,
                    tmpdir / "activation-install-client.err",
                    tmpdir / "activation-update-client.err",
                    tmpdir / "activation-uninstall-client.err",
                    tmpdir / "activation-local-cli.err",
                    tmpdir / "activation-install.out",
                    tmpdir / "activation-install.err",
                    tmpdir / "activation-update.out",
                    tmpdir / "activation-update.err",
                    tmpdir / "activation-uninstall.out",
                    tmpdir / "activation-uninstall.err",
                ):
                    if not path.exists():
                        continue
                    try:
                        contents = path.read_text(
                            encoding="utf-8", errors="replace"
                        )
                    except OSError as exc:
                        contents = "<could not read: {}>".format(exc)
                    diagnostics.append("{}:\n{}".format(path.name, contents))
                if diagnostics:
                    print(
                        "daemon smoke diagnostics before cleanup:\n"
                        + "\n".join(diagnostics),
                        file=sys.stderr,
                    )
            # Closing all thin clients is the supported cleanup path. Give a
            # test-owned daemon a final chance to finish before the temp cache
            # directory disappears, without enumerating or killing processes.
            if lock_status(lifetime_lock, record_lock=True) == "held":
                deadline = time.monotonic() + SHUTDOWN_TIMEOUT
                while time.monotonic() < deadline:
                    if lock_status(lifetime_lock, record_lock=True) != "held":
                        break
                    time.sleep(0.1)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (SmokeFailure, subprocess.TimeoutExpired, ValueError) as exc:
        print("FAIL: " + str(exc), file=sys.stderr)
        raise SystemExit(1)
