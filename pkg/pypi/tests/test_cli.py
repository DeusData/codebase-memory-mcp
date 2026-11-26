import contextlib
import errno
import hashlib
import http.server
import io
import json
import os
import stat
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import unittest
import zipfile
from pathlib import Path, PureWindowsPath
from unittest import mock


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACKAGE_ROOT / "src"))

from codebase_memory_mcp import _cli  # noqa: E402


class BinarySelectionTests(unittest.TestCase):
    """One binary ships per platform: the cached file is the executed file."""

    def test_windows_executes_the_cached_binary(self):
        binary = Path("cache") / "0.8.1" / _cli._WINDOWS_BINARY_NAME

        self.assertEqual(_cli._execution_path(binary, "win32"), binary)

    def test_non_windows_executes_the_cached_binary(self):
        binary = Path("cache") / "0.8.1" / "codebase-memory-mcp"

        self.assertEqual(_cli._execution_path(binary, "linux"), binary)

    def test_cache_sensitive_mutation_classification_is_explicit(self):
        self.assertEqual(_cli._runtime_mutation_action(["update"]), "update")
        self.assertEqual(
            _cli._runtime_mutation_action(["uninstall", "--yes"]),
            "uninstall",
        )
        self.assertEqual(
            _cli._runtime_mutation_action(["install", "--yes"]), "install"
        )
        self.assertIsNone(_cli._runtime_mutation_action(["cli", "update"]))
        self.assertIsNone(_cli._runtime_mutation_action(["daemon", "update"]))

    def test_wrapper_uninstall_never_defaults_to_its_cache_binary(self):
        linux_home = Path("fixtures") / "linux-home"
        windows_local_app_data = Path("fixtures") / "windows-local-app-data"
        defaults = (
            (
                "linux",
                linux_home / ".local" / "bin",
                mock.patch.object(Path, "home", return_value=linux_home),
            ),
            (
                "win32",
                windows_local_app_data
                / "Programs"
                / "codebase-memory-mcp",
                mock.patch.dict(
                    os.environ,
                    {"LOCALAPPDATA": str(windows_local_app_data)},
                ),
            ),
        )
        for platform_name, expected, path_fixture in defaults:
            with self.subTest(platform=platform_name), mock.patch.object(
                _cli.sys, "platform", platform_name
            ), path_fixture:
                self.assertEqual(
                    _cli._native_args(["uninstall", "--yes"]),
                    ["uninstall", "--yes", "--dir", str(expected)],
                )

        explicit_dir = Path("fixtures") / "explicit-install"
        for platform_name in ("linux", "win32"):
            with self.subTest(
                platform=platform_name, explicit_dir=True
            ), mock.patch.object(_cli.sys, "platform", platform_name):
                self.assertEqual(
                    _cli._native_args(
                        ["uninstall", "--dir", str(explicit_dir)]
                    ),
                    ["uninstall", "--dir", str(explicit_dir)],
                )

    def test_release_archives_carry_no_sidecars(self):
        # The binary is self-contained; a returning sidecar would silently make
        # `pip install` reject every archive against this exact-set allowlist.
        for names in (_cli._WINDOWS_ARCHIVE_NAMES, _cli._UNIX_ARCHIVE_NAMES):
            self.assertNotIn("cbm-integrations.json", names)
            self.assertFalse([n for n in names if n.startswith("cbm-ui-")])
            self.assertEqual(len(names), 4)

    def test_update_guidance_uses_pip_and_names_managed_install(self):
        with mock.patch.object(_cli.sys, "argv", ["cbm", "update"]), \
             mock.patch.object(_cli, "_version", return_value="0.8.1"), \
             mock.patch("sys.stderr") as stderr:
            with self.assertRaisesRegex(SystemExit, "2"):
                _cli.main()

        output = "".join(call.args[0] for call in stderr.write.call_args_list)
        self.assertIn(
            "python -m pip install --upgrade codebase-memory-mcp", output
        )
        self.assertIn("codebase-memory-mcp install --yes", output)
        self.assertNotIn("install.sh", output)


class ProcessLivenessTests(unittest.TestCase):
    def test_windows_probe_never_calls_os_kill(self):
        with mock.patch.object(_cli.os, "name", "nt"), mock.patch.object(
            _cli,
            "_windows_process_is_alive",
            return_value=True,
            create=True,
        ) as windows_probe, mock.patch.object(
            _cli.os,
            "kill",
            side_effect=AssertionError("Windows must not call os.kill"),
        ):
            self.assertTrue(_cli._process_is_alive(1234))

        windows_probe.assert_called_once_with(1234)

    def test_windows_wait_result_distinguishes_dead_and_live_processes(self):
        cases = (
            (_cli._WINDOWS_WAIT_OBJECT_0, False),
            (_cli._WINDOWS_WAIT_TIMEOUT, True),
            (0xFFFFFFFF, True),
        )
        for wait_result, expected in cases:
            with self.subTest(wait_result=wait_result):
                kernel32 = mock.Mock()
                kernel32.OpenProcess.return_value = 123
                kernel32.WaitForSingleObject.return_value = wait_result
                with mock.patch.object(
                    _cli,
                    "_windows_process_api",
                    return_value=(kernel32, mock.Mock()),
                ):
                    self.assertEqual(
                        _cli._windows_process_is_alive(4567), expected
                    )

                kernel32.OpenProcess.assert_called_once_with(
                    _cli._WINDOWS_SYNCHRONIZE, False, 4567
                )
                kernel32.WaitForSingleObject.assert_called_once_with(123, 0)
                kernel32.CloseHandle.assert_called_once_with(123)

    def test_windows_open_failure_only_treats_missing_pid_as_dead(self):
        cases = (
            (_cli._WINDOWS_ERROR_INVALID_PARAMETER, False),
            (5, True),
        )
        for error, expected in cases:
            with self.subTest(error=error):
                kernel32 = mock.Mock()
                kernel32.OpenProcess.return_value = 0
                get_last_error = mock.Mock(return_value=error)
                with mock.patch.object(
                    _cli,
                    "_windows_process_api",
                    return_value=(kernel32, get_last_error),
                ):
                    self.assertEqual(
                        _cli._windows_process_is_alive(4567), expected
                    )

                get_last_error.assert_called_once_with()
                kernel32.WaitForSingleObject.assert_not_called()
                kernel32.CloseHandle.assert_not_called()

    def test_windows_api_failures_are_conservatively_live(self):
        kernel32 = mock.Mock()
        kernel32.OpenProcess.side_effect = OSError("injected API failure")
        with mock.patch.object(
            _cli,
            "_windows_process_api",
            return_value=(kernel32, mock.Mock()),
        ):
            self.assertTrue(_cli._windows_process_is_alive(4567))

    def test_posix_probe_behavior_is_unchanged(self):
        cases = (
            (None, True),
            (ProcessLookupError(), False),
            (PermissionError(), True),
            (OSError(errno.ESRCH, "missing"), False),
        )
        for error, expected in cases:
            with self.subTest(error=type(error).__name__):
                side_effect = error if error is not None else None
                with mock.patch.object(_cli.os, "name", "posix"), \
                     mock.patch.object(
                         _cli.os, "kill", side_effect=side_effect
                     ) as kill:
                    self.assertEqual(_cli._process_is_alive(4567), expected)
                kill.assert_called_once_with(4567, 0)


class RuntimeSetTests(unittest.TestCase):
    def test_candidate_probe_requires_the_binary_to_execute(self):
        candidates = (
            Path("/tmp/codebase-memory-mcp"),
            PureWindowsPath("/tmp/codebase-memory-mcp"),
        )

        for candidate in candidates:
            with self.subTest(candidate=candidate):
                with mock.patch.object(_cli.subprocess, "run") as run:
                    run.return_value.returncode = 0
                    _cli._verify_candidate(candidate)

                self.assertEqual(
                    run.call_args.args[0],
                    [str(candidate), "--version"],
                )

    def test_concurrent_publishers_preserve_the_first_complete_winner(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            binary_name = "codebase-memory-mcp"

            def stages(tag):
                staged = directory / f".stage-{tag}-{binary_name}"
                staged.write_bytes(f"binary:{tag}".encode())
                return {binary_name: staged}

            first_stages = stages("first")
            second_stages = stages("second")
            first_holds_lock = threading.Event()
            allow_first = threading.Event()
            second_observed_lock = threading.Event()
            errors = []
            replace = os.replace
            read_lock_owner = _cli._read_runtime_lock_owner

            def pause_first(source, destination):
                replace(source, destination)
                if Path(destination) == directory / binary_name:
                    first_holds_lock.set()
                    if not allow_first.wait(5):
                        raise RuntimeError("test did not release first publisher")

            def observe_waiter(lock_path):
                if threading.current_thread().name == "publisher-second":
                    second_observed_lock.set()
                return read_lock_owner(lock_path)

            def publish(label, staged_paths, replace_file=None):
                try:
                    _cli._publish_runtime_set(
                        staged_paths,
                        directory / binary_name,
                        binary_name,
                        replace_file=replace_file,
                    )
                except Exception as exc:  # surfaced on the test thread below
                    errors.append((label, exc))

            first = threading.Thread(
                name="publisher-first",
                target=publish,
                args=("first", first_stages, pause_first),
            )
            second = threading.Thread(
                name="publisher-second",
                target=publish,
                args=("second", second_stages),
            )
            with mock.patch.object(
                _cli,
                "_read_runtime_lock_owner",
                side_effect=observe_waiter,
            ):
                first.start()
                try:
                    self.assertTrue(first_holds_lock.wait(2))
                    second.start()
                    self.assertTrue(second_observed_lock.wait(2))
                finally:
                    allow_first.set()
                    first.join(5)
                    if second.ident is not None:
                        second.join(5)

            self.assertFalse(first.is_alive())
            self.assertFalse(second.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(
                (directory / binary_name).read_bytes(), b"binary:first"
            )
            self.assertFalse((directory / _cli._RUNTIME_LOCK_NAME).exists())

    def test_killed_publisher_is_reconciled_by_locked_readiness(self):
        helper_source = r"""
import os
import sys
import time
from pathlib import Path

source_root, directory_raw, staged_raw, marker_raw, crash_name = sys.argv[1:]
sys.path.insert(0, source_root)
from codebase_memory_mcp import _cli

directory = Path(directory_raw)
staged = Path(staged_raw)
marker = Path(marker_raw)
binary_name = "codebase-memory-mcp"
staged_paths = {
    binary_name: staged / binary_name,
}
replace = os.replace

def pausing_replace(source, destination):
    replace(source, destination)
    if Path(destination) == directory / crash_name:
        marker.write_text("reached\n")
        while True:
            time.sleep(60)

def verifier(binary):
    if not Path(binary).read_text().startswith("binary:"):
        raise RuntimeError("test binary failed verification")

_cli._publish_runtime_set(
    staged_paths,
    directory / binary_name,
    binary_name,
    replace_file=pausing_replace,
    verifier=verifier,
)
"""
        binary_name = "codebase-memory-mcp"
        for crash_name, expected_ready in (
            (binary_name, True),
        ):
            with self.subTest(crash_name=crash_name), tempfile.TemporaryDirectory() as root:
                root_path = Path(root)
                directory = root_path / "destination"
                staged = root_path / "staged"
                marker = root_path / "crash-reached"
                directory.mkdir()
                staged.mkdir()
                (directory / binary_name).write_text("corrupt:old")
                (staged / binary_name).write_text("binary:candidate")
                environment = os.environ.copy()
                environment["PYTHONPYCACHEPREFIX"] = str(root_path / "pycache")
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-c",
                        helper_source,
                        str(PACKAGE_ROOT / "src"),
                        str(directory),
                        str(staged),
                        str(marker),
                        crash_name,
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=environment,
                )
                try:
                    deadline = time.monotonic() + 10
                    while not marker.exists() and time.monotonic() < deadline:
                        if process.poll() is not None:
                            stderr = process.communicate()[1]
                            self.fail(
                                "publication helper exited before crash gate: "
                                f"{stderr}"
                            )
                        time.sleep(0.01)
                    self.assertTrue(
                        marker.exists(),
                        "publication helper did not reach crash gate",
                    )
                    backups = [
                        path
                        for path in directory.iterdir()
                        if path.name.startswith(".cbm-runtime-backup-")
                    ]
                    self.assertEqual(len(backups), 1)
                    self.assertTrue(backups[0].is_dir())
                    self.assertTrue(
                        (backups[0] / ".retirement-complete").is_file()
                    )

                    process.kill()
                    process.wait(5)
                    self.assertNotEqual(process.returncode, 0)

                    def verifier(binary):
                        if not Path(binary).read_text().startswith("binary:"):
                            raise RuntimeError("test binary failed verification")

                    with mock.patch.object(
                        _cli, "_bin_path", return_value=directory / binary_name
                    ), mock.patch.object(
                        _cli, "_verify_candidate", side_effect=verifier
                    ):
                        ready = _cli._runtime_set_ready_locked("test-version")
                    self.assertEqual(ready, expected_ready)
                    if not ready:
                        self.assertEqual(
                            (directory / binary_name).read_text(),
                            "corrupt:old",
                        )
                        retry_staged = root_path / "retry-staged"
                        retry_staged.mkdir()
                        retry_paths = {
                            binary_name: retry_staged / binary_name,
                        }
                        retry_paths[binary_name].write_text("binary:candidate")
                        _cli._publish_runtime_set(
                            retry_paths,
                            directory / binary_name,
                            binary_name,
                            verifier=verifier,
                        )

                    self.assertEqual(
                        (directory / binary_name).read_text(), "binary:candidate"
                    )
                    self.assertFalse(
                        any(
                            path.name.startswith(".cbm-runtime-backup-")
                            for path in directory.iterdir()
                        )
                    )
                    self.assertFalse(
                        (directory / _cli._RUNTIME_LOCK_NAME).exists()
                    )
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait(5)
                    if process.stderr is not None:
                        process.stderr.close()

    def test_publication_failure_restores_prior_complete_runtime_set(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            binary_name = "codebase-memory-mcp"
            (directory / binary_name).write_text("binary:old")
            staged_paths = {}
            for name, contents in (
                (binary_name, b"binary:candidate"),
            ):
                staged = directory / f"stage-{name}"
                staged.write_bytes(contents)
                staged_paths[name] = staged

            replace = os.replace
            failed = False
            retired = []

            def fail_binary_publish(source, destination):
                nonlocal failed
                source = Path(source)
                destination = Path(destination)
                if destination.parent.name.startswith(".cbm-runtime-backup-"):
                    retired.append(source.name)
                if destination == directory / binary_name and not failed:
                    failed = True
                    raise OSError("injected binary publication failure")
                replace(source, destination)

            with self.assertRaises(OSError):
                _cli._publish_runtime_set(
                    staged_paths,
                    directory / binary_name,
                    binary_name,
                    replace_file=fail_binary_publish,
                )

            self.assertTrue(failed)
            self.assertEqual(retired[0], binary_name)
            self.assertEqual((directory / binary_name).read_text(), "binary:old")
            self.assertFalse(
                any(
                    path.name.startswith(".cbm-runtime-backup-")
                    for path in directory.iterdir()
                )
            )

    def test_failure_never_deletes_a_complete_foreign_winner(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            binary_name = "codebase-memory-mcp"
            (directory / binary_name).write_bytes(b"binary:old")
            staged_paths = {}
            for name, contents in (
                (binary_name, b"binary:candidate"),
            ):
                staged = directory / f".stage-candidate-{name}"
                staged.write_bytes(contents)
                staged_paths[name] = staged

            replace = os.replace
            injected = False

            def install_foreign_winner(source, destination):
                nonlocal injected
                source = Path(source)
                destination = Path(destination)
                if destination == directory / binary_name and not injected:
                    injected = True
                    (directory / binary_name).write_bytes(b"binary:foreign")
                    raise OSError("simulated non-cooperating winner")
                replace(source, destination)

            _cli._publish_runtime_set(
                staged_paths,
                directory / binary_name,
                binary_name,
                replace_file=install_foreign_winner,
            )

            self.assertTrue(injected)
            self.assertEqual(
                (directory / binary_name).read_bytes(), b"binary:foreign"
            )
            self.assertFalse(
                any(
                    path.name.startswith(".cbm-runtime-backup-")
                    for path in directory.iterdir()
                )
            )

    def test_orphan_reconciliation_rejects_multiply_linked_backup_members(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            binary = directory / "codebase-memory-mcp"
            backup = directory / f".cbm-runtime-backup-{'a' * 32}"
            backup.mkdir()
            (backup / ".retirement-complete").write_bytes(b"")
            member = backup / binary.name
            member.write_text("binary:old")
            os.link(member, directory / "backup-hardlink-copy")

            with mock.patch.object(
                _cli, "_bin_path", return_value=binary
            ), self.assertRaisesRegex(
                RuntimeError, "unsafe package-cache backup member"
            ):
                _cli._runtime_set_ready_locked("test-version")

            self.assertTrue(backup.exists(), "unsafe backup was mutated")
            self.assertFalse((directory / _cli._RUNTIME_LOCK_NAME).exists())

    def test_stalled_lock_creator_never_deletes_successor(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            successor = None

            def publish_successor_then_abort():
                nonlocal successor
                _cli._runtime_lock_claim_observer = None
                successor, _ = _cli._acquire_runtime_lock(directory)
                raise RuntimeError("injected stalled creator abort")

            with mock.patch.object(
                _cli,
                "_runtime_lock_claim_observer",
                side_effect=publish_successor_then_abort,
            ), self.assertRaisesRegex(RuntimeError, "stalled creator abort"):
                _cli._acquire_runtime_lock(directory)

            self.assertIsNotNone(successor)
            self.assertTrue(successor[0].exists())
            _cli._release_runtime_lock(successor)

    def test_acquire_closes_claim_descriptor_before_unlink_and_reopens(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            captured = {}
            write_owner = _cli._write_runtime_lock_owner
            unlink = Path.unlink

            def capture_claim_descriptor(owner_fd, token):
                captured["claim_fd"] = owner_fd
                write_owner(owner_fd, token)

            def require_closed_claim_descriptor(path, *args, **kwargs):
                if f"{_cli._RUNTIME_LOCK_NAME}.claim-" in path.name:
                    with self.assertRaises(OSError):
                        os.fstat(captured["claim_fd"])
                    captured["closed_before_unlink"] = True
                return unlink(path, *args, **kwargs)

            with mock.patch.object(
                _cli,
                "_write_runtime_lock_owner",
                side_effect=capture_claim_descriptor,
            ), mock.patch.object(
                Path, "unlink", new=require_closed_claim_descriptor
            ):
                lock, _ = _cli._acquire_runtime_lock(directory)

            try:
                self.assertTrue(captured["closed_before_unlink"])
                self.assertTrue(
                    _cli._same_runtime_lock_object(
                        os.fstat(lock[2]), lock[0].lstat()
                    )
                )
            finally:
                _cli._release_runtime_lock(lock)

    def test_release_closes_descriptor_and_revalidates_each_lock_name(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            lock, _ = _cli._acquire_runtime_lock(directory)
            original_fd = lock[2]
            events = []
            read_owner = _cli._read_runtime_lock_owner
            rename = os.rename

            def record_owner_validation(path):
                events.append(("validate", Path(path).name))
                return read_owner(path)

            def require_closed_descriptor(source, destination):
                with self.assertRaises(OSError):
                    os.fstat(original_fd)
                events.append(("rename", Path(source).name))
                return rename(source, destination)

            with mock.patch.object(
                _cli,
                "_read_runtime_lock_owner",
                side_effect=record_owner_validation,
            ), mock.patch.object(
                _cli.os, "rename", side_effect=require_closed_descriptor
            ):
                _cli._release_runtime_lock(lock)

            self.assertEqual(
                events,
                [
                    ("validate", _cli._RUNTIME_LOCK_NAME),
                    ("validate", _cli._RUNTIME_LOCK_NAME),
                    ("rename", _cli._RUNTIME_LOCK_NAME),
                    (
                        "validate",
                        f"{_cli._RUNTIME_LOCK_NAME}.released-{lock[1]}",
                    ),
                ],
            )
            self.assertFalse(lock[0].exists())

    def test_release_rejects_owner_substitution_before_rename(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            lock, _ = _cli._acquire_runtime_lock(directory)
            read_owner = _cli._read_runtime_lock_owner
            canonical_reads = 0

            def substitute_owner_on_reopen(path):
                nonlocal canonical_reads
                owner = read_owner(path)
                if Path(path) == lock[0]:
                    canonical_reads += 1
                    if canonical_reads == 2:
                        return (os.getpid(), "f" * 32, owner[2])
                return owner

            with mock.patch.object(
                _cli,
                "_read_runtime_lock_owner",
                side_effect=substitute_owner_on_reopen,
            ):
                with self.assertRaisesRegex(RuntimeError, "ownership changed"):
                    _cli._release_runtime_lock(lock)

            self.assertTrue(lock[0].exists())
            lock[0].unlink()

    def test_release_rejects_object_substitution_before_rename(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            lock, _ = _cli._acquire_runtime_lock(directory)
            close = os.close
            substituted = False

            def substitute_object_after_owner_close(owner_fd):
                nonlocal substituted
                if (
                    owner_fd != lock[2]
                    or substituted
                    or not lock[0].exists()
                ):
                    close(owner_fd)
                    return
                owner_record = lock[0].read_bytes()
                replacement = directory / "replacement-lock"
                replacement.write_bytes(owner_record)
                try:
                    close(owner_fd)
                    os.replace(replacement, lock[0])
                    substituted = True
                finally:
                    replacement.unlink(missing_ok=True)

            with mock.patch.object(
                _cli.os,
                "close",
                side_effect=substitute_object_after_owner_close,
            ):
                with self.assertRaisesRegex(RuntimeError, "ownership changed"):
                    _cli._release_runtime_lock(lock)

            self.assertTrue(substituted)
            self.assertTrue(lock[0].exists())
            lock[0].unlink()

    def test_expired_lease_never_reclaims_live_owner(self):
        with tempfile.TemporaryDirectory() as root:
            lock_path = Path(root) / _cli._RUNTIME_LOCK_NAME
            owner = {
                "pid": os.getpid(),
                "token": "a" * 32,
                "lease_expires_ms": int((time.time() - 1) * 1000),
            }
            owner_record = f"{json.dumps(owner)}\n"
            lock_path.write_text(owner_record)
            before = lock_path.stat()

            with mock.patch.object(_cli, "_process_is_alive", return_value=True):
                self.assertEqual(
                    _cli._try_reclaim_runtime_lock(lock_path, "b" * 32),
                    (False, False),
                )
            after = lock_path.stat()
            self.assertEqual(lock_path.read_text(), owner_record)
            self.assertEqual(json.loads(lock_path.read_text())["token"], owner["token"])
            self.assertEqual(after.st_dev, before.st_dev)
            self.assertEqual(after.st_ino, before.st_ino)

    def test_stale_legacy_lock_never_reclaims_live_owner(self):
        with tempfile.TemporaryDirectory() as root:
            lock_path = Path(root) / _cli._RUNTIME_LOCK_NAME
            owner_record = json.dumps(
                {"pid": os.getpid(), "token": "a" * 32}
            ) + "\n"
            lock_path.mkdir()
            (lock_path / "owner.json").write_text(owner_record)
            stale = time.time() - _cli._RUNTIME_LEGACY_STALE_SECONDS - 1
            os.utime(lock_path, (stale, stale))
            before = lock_path.stat()

            with mock.patch.object(_cli, "_process_is_alive", return_value=True):
                self.assertEqual(
                    _cli._try_reclaim_runtime_lock(lock_path, "b" * 32),
                    (False, False),
                )
            after = lock_path.stat()
            self.assertEqual((lock_path / "owner.json").read_text(), owner_record)
            self.assertEqual(after.st_dev, before.st_dev)
            self.assertEqual(after.st_ino, before.st_ino)

    def test_stale_ownerless_lock_is_reclaimed(self):
        with tempfile.TemporaryDirectory() as root:
            lock_path = Path(root) / _cli._RUNTIME_LOCK_NAME
            lock_path.write_text("")
            stale = time.time() - _cli._RUNTIME_OWNERLESS_STALE_SECONDS - 1
            os.utime(lock_path, (stale, stale))

            self.assertEqual(
                _cli._try_reclaim_runtime_lock(lock_path, "b" * 32),
                (True, False),
            )
            self.assertFalse(lock_path.exists())

    def test_unix_tar_rejects_an_unexpected_root_member(self):
        with tempfile.TemporaryDirectory() as root:
            archive = Path(root) / "release.tar.gz"
            with tarfile.open(archive, "w:gz") as tf:
                for name in (*_cli._UNIX_ARCHIVE_NAMES, "unexpected-root-file"):
                    data_path = Path(root) / f"fixture-{len(tf.getmembers())}"
                    data_path.write_bytes(name.encode())
                    tf.add(data_path, arcname=name)
            destination = Path(root) / "extract"
            destination.mkdir()
            with tarfile.open(archive) as tf, self.assertRaises(SystemExit):
                _cli._safe_extract_tar(
                    tf,
                    str(destination),
                    _cli._UNIX_ARCHIVE_NAMES,
                    ("codebase-memory-mcp",),
                )

    def test_unix_tar_rejects_hardlink_members(self):
        with tempfile.TemporaryDirectory() as root:
            archive = Path(root) / "release.tar.gz"
            with tarfile.open(archive, "w:gz") as tf:
                info = tarfile.TarInfo("codebase-memory-mcp")
                info.type = tarfile.LNKTYPE
                info.linkname = "LICENSE"
                tf.addfile(info)
            destination = Path(root) / "extract"
            destination.mkdir()
            with tarfile.open(archive) as tf, self.assertRaises(SystemExit):
                _cli._safe_extract_tar(
                    tf,
                    str(destination),
                    ("codebase-memory-mcp",),
                    ("codebase-memory-mcp",),
                    False,
                )

    def test_windows_zip_rejects_symlink_metadata(self):
        with tempfile.TemporaryDirectory() as root:
            archive = Path(root) / "release.zip"
            with zipfile.ZipFile(archive, "w") as zf:
                info = zipfile.ZipInfo(_cli._WINDOWS_BINARY_NAME)
                info.create_system = 3
                info.external_attr = (stat.S_IFLNK | 0o777) << 16
                zf.writestr(info, "LICENSE")
            destination = Path(root) / "extract"
            destination.mkdir()
            with zipfile.ZipFile(archive) as zf, self.assertRaises(SystemExit):
                _cli._safe_extract_zip(
                    zf,
                    str(destination),
                    (_cli._WINDOWS_BINARY_NAME,),
                    (_cli._WINDOWS_BINARY_NAME,),
                    False,
                )


class _FakeHttpsResponse:
    """Minimal stand-in for the opener's response: headers, geturl, read."""

    def __init__(self, body: bytes, headers=None, fail_after_first_chunk=False):
        self._body = body
        self._offset = 0
        self._fail_after_first_chunk = fail_after_first_chunk
        self.headers = dict(headers or {})

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def geturl(self):
        return "https://example.invalid/release.tar.gz"

    def read(self, size=-1):
        if self._fail_after_first_chunk and self._offset:
            raise OSError("connection reset")
        if size is None or size < 0:
            size = len(self._body) - self._offset
        chunk = self._body[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk


class _ZeroSource:
    """Reader that yields `size` zero bytes without holding them."""

    def __init__(self, size: int):
        self._remaining = size

    def read(self, size=-1):
        if size is None or size < 0:
            size = self._remaining
        count = min(size, self._remaining)
        self._remaining -= count
        return bytes(count)


class _CountingSink:
    def __init__(self):
        self.written = 0

    def write(self, chunk):
        self.written += len(chunk)
        return len(chunk)


# The size cap the 0.12.0 wrappers enforced, which refused every release.
_FORMER_MEMBER_CAP_BYTES = 256 * 1024 * 1024


def _write_tar_gz(archive, entries):
    with tarfile.open(archive, "w:gz") as tf:
        for name, data in entries:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))


def _write_zip(archive, entries):
    with zipfile.ZipFile(archive, "w") as zf:
        for name, data in entries:
            zf.writestr(name, data)


def _release_entries(names):
    return [(name, f"payload of {name}".encode()) for name in names]


class ArchiveExtractionTests(unittest.TestCase):
    URL = "https://example.invalid/release.tar.gz"

    def _download(self, response, destination):
        with mock.patch.object(_cli._HTTPS_OPENER, "open", return_value=response):
            _cli._download_file(self.URL, destination)

    def test_download_stores_a_body_whose_declared_length_exceeds_the_former_cap(
        self,
    ):
        with tempfile.TemporaryDirectory() as root:
            destination = os.path.join(root, "release.tar.gz")
            response = _FakeHttpsResponse(
                b"payload", {"Content-Length": str(_FORMER_MEMBER_CAP_BYTES + 1)}
            )
            self._download(response, destination)
            with open(destination, "rb") as stored:
                self.assertEqual(stored.read(), b"payload")

    def test_download_removes_the_partial_file_when_the_body_fails(self):
        with tempfile.TemporaryDirectory() as root:
            destination = os.path.join(root, "release.tar.gz")
            response = _FakeHttpsResponse(b"x" * 200000, fail_after_first_chunk=True)
            with self.assertRaises(OSError):
                self._download(response, destination)
            self.assertFalse(os.path.exists(destination))

    def test_unix_tar_rejects_more_members_than_the_limit(self):
        with tempfile.TemporaryDirectory() as root:
            archive = Path(root) / "members.tar.gz"
            _write_tar_gz(archive, [("one", b"a"), ("two", b"b"), ("three", b"c")])
            destination = Path(root) / "extract"
            destination.mkdir()
            with tarfile.open(archive) as tf, self.assertRaises(SystemExit) as raised:
                _cli._safe_extract_tar(
                    tf,
                    str(destination),
                    ("one", "two", "three"),
                    ("one",),
                    max_members=2,
                )
            self.assertIn("2-member safety limit", str(raised.exception))
            self.assertFalse((destination / "one").exists())

    def test_unix_tar_extracts_only_the_executable(self):
        with tempfile.TemporaryDirectory() as root:
            archive = Path(root) / "release.tar.gz"
            _write_tar_gz(archive, _release_entries(_cli._UNIX_ARCHIVE_NAMES))
            destination = Path(root) / "extract"
            destination.mkdir()
            with tarfile.open(archive) as tf:
                extracted = _cli._safe_extract_tar(
                    tf,
                    str(destination),
                    _cli._UNIX_ARCHIVE_NAMES,
                    ("codebase-memory-mcp",),
                )
            self.assertEqual(extracted, ("codebase-memory-mcp",))
            self.assertEqual(
                (destination / "codebase-memory-mcp").read_bytes(),
                b"payload of codebase-memory-mcp",
            )
            self.assertEqual(os.listdir(destination), ["codebase-memory-mcp"])

    def test_archive_copy_accepts_a_member_larger_than_the_former_cap(self):
        size = _FORMER_MEMBER_CAP_BYTES + 1024 * 1024
        sink = _CountingSink()
        copied = _cli._copy_archive_member(
            _ZeroSource(size), sink, "codebase-memory-mcp", size
        )
        self.assertEqual(copied, size)
        self.assertEqual(sink.written, size)

    def test_archive_copy_rejects_a_member_that_yields_more_than_it_declares(self):
        with self.assertRaises(SystemExit) as raised:
            _cli._copy_archive_member(io.BytesIO(b"x" * 100), io.BytesIO(), "member", 10)
        self.assertIn(
            "'member' actual size 100 does not match declared size 10",
            str(raised.exception),
        )

    def test_archive_member_write_removes_the_partial_file_on_failure(self):
        with tempfile.TemporaryDirectory() as root:
            target = os.path.join(root, "codebase-memory-mcp")
            with self.assertRaises(SystemExit):
                _cli._write_archive_member(
                    io.BytesIO(b"x" * 100), target, "codebase-memory-mcp", 10
                )
            self.assertFalse(os.path.exists(target))

    def test_windows_zip_rejects_more_members_than_the_limit(self):
        with tempfile.TemporaryDirectory() as root:
            archive = Path(root) / "members.zip"
            _write_zip(archive, [("one", b"a"), ("two", b"b"), ("three", b"c")])
            destination = Path(root) / "extract"
            destination.mkdir()
            with zipfile.ZipFile(archive) as zf, self.assertRaises(SystemExit) as raised:
                _cli._safe_extract_zip(
                    zf,
                    str(destination),
                    ("one", "two", "three"),
                    ("one",),
                    max_members=2,
                )
            self.assertIn("2-member safety limit", str(raised.exception))

    def test_windows_zip_extracts_only_the_executable(self):
        with tempfile.TemporaryDirectory() as root:
            archive = Path(root) / "release.zip"
            _write_zip(archive, _release_entries(_cli._WINDOWS_ARCHIVE_NAMES))
            destination = Path(root) / "extract"
            destination.mkdir()
            with zipfile.ZipFile(archive) as zf:
                extracted = _cli._safe_extract_zip(
                    zf,
                    str(destination),
                    _cli._WINDOWS_ARCHIVE_NAMES,
                    (_cli._WINDOWS_BINARY_NAME,),
                )
            self.assertEqual(extracted, (_cli._WINDOWS_BINARY_NAME,))
            self.assertEqual(
                (destination / _cli._WINDOWS_BINARY_NAME).read_bytes(),
                b"payload of codebase-memory-mcp.exe",
            )
            self.assertEqual(os.listdir(destination), [_cli._WINDOWS_BINARY_NAME])


@contextlib.contextmanager
def _loopback_server(routes):
    """Serve `routes` ({path: bytes}) on 127.0.0.1; any other path redirects."""

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            body = routes.get(self.path)
            if body is None:
                self.send_response(302)
                self.send_header("Location", "https://example.invalid/elsewhere")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


class DownloadBaseTests(unittest.TestCase):
    GITHUB_BASE = "https://github.com/DeusData/codebase-memory-mcp/releases/download/v1.2.3"

    def test_unset_or_empty_uses_the_github_release_base(self):
        self.assertEqual(_cli._download_base("1.2.3", {}), (self.GITHUB_BASE, False))
        self.assertEqual(
            _cli._download_base("1.2.3", {"CBM_DOWNLOAD_URL": ""}),
            (self.GITHUB_BASE, False),
        )

    def test_https_override_is_accepted(self):
        for base in (
            "https://mirror.example/releases/v1",
            "https://127.0.0.1:8443/fixture",
        ):
            with self.subTest(base=base):
                self.assertEqual(
                    _cli._download_base("1.2.3", {"CBM_DOWNLOAD_URL": base}),
                    (base, False),
                )

    def test_exact_loopback_http_override_is_accepted(self):
        for base in (
            "http://localhost",
            "http://localhost:8080/fixture",
            "http://127.0.0.1:41234/standard",
            "http://[::1]:9/x?y#z",
        ):
            with self.subTest(base=base):
                self.assertEqual(
                    _cli._download_base("1.2.3", {"CBM_DOWNLOAD_URL": base}),
                    (base, True),
                )

    def test_non_loopback_http_and_other_schemes_are_refused(self):
        for base in (
            "http://example.com/releases",
            "http://127.0.0.1.example.com/",
            "http://localhost.example.com:8080/",
            "http://user@localhost/",
            "http://127.0.0.2/",
            "HTTP://localhost/",
            "file:///tmp/releases",
            "ftp://localhost/releases",
            "github.com/DeusData/codebase-memory-mcp",
        ):
            with self.subTest(base=base), self.assertRaises(SystemExit) as raised:
                _cli._download_base("1.2.3", {"CBM_DOWNLOAD_URL": base})
            self.assertIn(
                "refusing non-HTTPS download URL (CBM_DOWNLOAD_URL)",
                str(raised.exception),
            )

    def test_https_download_refuses_plain_http(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(SystemExit):
                _cli._download_file(
                    "http://127.0.0.1/release.tar.gz", os.path.join(root, "a")
                )

    def test_loopback_download_refuses_a_non_loopback_url(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(SystemExit) as raised:
                _cli._download_file(
                    "http://example.invalid/release.tar.gz",
                    os.path.join(root, "a"),
                    loopback=True,
                )
            self.assertIn("refusing non-loopback HTTP URL", str(raised.exception))

    def test_loopback_download_bypasses_proxies_and_refuses_redirects(self):
        routes = {"/fixture/archive": b"loopback payload"}
        # A configured proxy that is not listening: using it would fail.
        proxy_env = {"http_proxy": "http://127.0.0.1:9", "HTTP_PROXY": "http://127.0.0.1:9"}
        with _loopback_server(routes) as origin, tempfile.TemporaryDirectory() as root, \
                mock.patch.dict(os.environ, proxy_env):
            archive = os.path.join(root, "archive")
            _cli._download_file(f"{origin}/fixture/archive", archive, loopback=True)
            with open(archive, "rb") as stored:
                self.assertEqual(stored.read(), b"loopback payload")

            redirected = os.path.join(root, "redirected")
            with self.assertRaises(RuntimeError) as raised:
                _cli._download_file(
                    f"{origin}/fixture/checksums.txt", redirected, loopback=True
                )
            self.assertIn("refusing redirect from loopback download", str(raised.exception))
            self.assertFalse(os.path.exists(redirected))

    def test_download_fetches_archive_and_checksums_from_the_override(self):
        os_name = _cli._os_name()
        arch = _cli._arch()
        if os_name == "windows":
            ext, names = "zip", _cli._WINDOWS_ARCHIVE_NAMES
        else:
            ext, names = "tar.gz", _cli._UNIX_ARCHIVE_NAMES
        portable = "-portable" if os_name == "linux" else ""
        archive_name = f"codebase-memory-mcp-{os_name}-{arch}{portable}.{ext}"
        with tempfile.TemporaryDirectory() as root:
            fixture = Path(root) / archive_name
            if ext == "zip":
                _write_zip(fixture, _release_entries(names))
            else:
                _write_tar_gz(fixture, _release_entries(names))
            archive_bytes = fixture.read_bytes()
            digest = hashlib.sha256(archive_bytes).hexdigest()
            routes = {
                f"/fixture/{archive_name}": archive_bytes,
                "/fixture/checksums.txt": f"{digest}  {archive_name}\n".encode(),
            }
            cache = Path(root) / "cache"
            with _loopback_server(routes) as origin, mock.patch.dict(
                os.environ, {"CBM_DOWNLOAD_URL": f"{origin}/fixture"}
            ), mock.patch.object(_cli, "_cache_dir", return_value=cache), \
                    mock.patch.object(_cli, "_verify_candidate"), \
                    contextlib.redirect_stderr(io.StringIO()):
                installed = _cli._download("9.9.9")
            binary_name = (
                _cli._WINDOWS_BINARY_NAME
                if os_name == "windows"
                else "codebase-memory-mcp"
            )
            self.assertEqual(
                installed.read_bytes(), f"payload of {binary_name}".encode()
            )
