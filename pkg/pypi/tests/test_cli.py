import errno
import hashlib
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

    def test_standard_and_ui_cache_paths_do_not_collide(self):
        with mock.patch.dict("os.environ", {"CBM_VARIANT": "standard"}):
            standard = _cli._bin_path("0.8.1")
        with mock.patch.dict("os.environ", {"CBM_VARIANT": "ui"}):
            ui = _cli._bin_path("0.8.1")

        self.assertNotEqual(standard, ui)

    def test_release_archives_require_the_integrations_sidecar(self):
        self.assertIn("cbm-integrations.json", _cli._WINDOWS_ARCHIVE_NAMES)

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
    def test_candidate_probe_authenticates_adjacent_runtime_assets(self):
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
                    [str(candidate), "--verify-runtime-assets"],
                )

    def test_runtime_readiness_rejects_multiply_linked_cache_leaves(self):
        with tempfile.TemporaryDirectory() as root, mock.patch.object(
            _cli, "_cache_dir", return_value=Path(root)
        ):
            binary = _cli._bin_path("0.8.1", "standard")
            binary.parent.mkdir(parents=True)
            binary.write_text("binary")
            integrations = binary.parent / _cli._INTEGRATIONS_NAME
            integrations.write_text("integrations")
            os.link(integrations, binary.parent / "integrations-hardlink")

            self.assertFalse(_cli._runtime_set_ready("0.8.1"))

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
            ), mock.patch.object(
                _cli, "_variant", return_value="standard"
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

    def test_archive_namespace_is_exact_and_ui_has_one_pack(self):
        pack = f"cbm-ui-{'a' * 64}.pack"
        names = (*_cli._WINDOWS_ARCHIVE_NAMES, pack)

        self.assertEqual(
            _cli._validate_archive_names(names, _cli._WINDOWS_ARCHIVE_NAMES, True),
            pack,
        )
        with self.assertRaises(SystemExit):
            _cli._validate_archive_names(
                (*names, "unexpected-root-file"),
                _cli._WINDOWS_ARCHIVE_NAMES,
                True,
            )
        with self.assertRaises(SystemExit):
            _cli._validate_archive_names(
                (*names, f"cbm-ui-{'b' * 64}.pack"),
                _cli._WINDOWS_ARCHIVE_NAMES,
                True,
            )
        with self.assertRaises(SystemExit):
            _cli._validate_archive_names(
                names, _cli._WINDOWS_ARCHIVE_NAMES, False
            )

    def test_ui_zip_extracts_only_the_runtime_set(self):
        pack = f"cbm-ui-{'a' * 64}.pack"
        with tempfile.TemporaryDirectory() as root:
            archive = Path(root) / "release.zip"
            destination = Path(root) / "extract"
            destination.mkdir()
            with zipfile.ZipFile(archive, "w") as zf:
                for name in (*_cli._WINDOWS_ARCHIVE_NAMES, pack):
                    zf.writestr(name, name.encode())

            with zipfile.ZipFile(archive) as zf:
                names = _cli._safe_extract_zip(
                    zf,
                    str(destination),
                    _cli._WINDOWS_ARCHIVE_NAMES,
                    (_cli._WINDOWS_BINARY_NAME, _cli._INTEGRATIONS_NAME),
                    True,
                )

            self.assertEqual(
                names,
                (_cli._WINDOWS_BINARY_NAME, _cli._INTEGRATIONS_NAME, pack),
            )
            self.assertEqual(
                {path.name for path in destination.iterdir()}, set(names)
            )

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
                    ("codebase-memory-mcp", _cli._INTEGRATIONS_NAME),
                    False,
                )

    def test_unix_tar_rejects_hardlink_members(self):
        with tempfile.TemporaryDirectory() as root:
            archive = Path(root) / "release.tar.gz"
            with tarfile.open(archive, "w:gz") as tf:
                info = tarfile.TarInfo("codebase-memory-mcp")
                info.type = tarfile.LNKTYPE
                info.linkname = "cbm-integrations.json"
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
                zf.writestr(info, "cbm-integrations.json")
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

    def test_readiness_requires_the_complete_selected_variant(self):
        with tempfile.TemporaryDirectory() as root, mock.patch.object(
            _cli, "_cache_dir", return_value=Path(root)
        ):
            with mock.patch.dict(os.environ, {"CBM_VARIANT": "standard"}):
                standard_binary = _cli._bin_path("0.8.1")
                standard_binary.parent.mkdir(parents=True)
                standard_binary.write_text("binary")
                (standard_binary.parent / _cli._INTEGRATIONS_NAME).write_text(
                    "integrations"
                )
                self.assertTrue(_cli._runtime_set_ready("0.8.1"))
                (standard_binary.parent / _cli._INTEGRATIONS_NAME).unlink()
                self.assertFalse(_cli._runtime_set_ready("0.8.1"))

            with mock.patch.dict(os.environ, {"CBM_VARIANT": "ui"}):
                ui_binary = _cli._bin_path("0.8.1")
                ui_binary.parent.mkdir(parents=True)
                ui_binary.write_text("binary")
                (ui_binary.parent / _cli._INTEGRATIONS_NAME).write_text(
                    "integrations"
                )
                pack_bytes = b"pack"
                pack_name = (
                    f"cbm-ui-{hashlib.sha256(pack_bytes).hexdigest()}.pack"
                )
                (ui_binary.parent / pack_name).write_bytes(pack_bytes)
                self.assertTrue(_cli._runtime_set_ready("0.8.1"))
                second_pack = f"cbm-ui-{'b' * 64}.pack"
                (ui_binary.parent / second_pack).write_text("second pack")
                self.assertFalse(_cli._runtime_set_ready("0.8.1"))

    def test_ui_readiness_rejects_digest_mismatched_pack_bytes(self):
        with tempfile.TemporaryDirectory() as root, mock.patch.object(
            _cli, "_cache_dir", return_value=Path(root)
        ), mock.patch.dict(os.environ, {"CBM_VARIANT": "ui"}):
            binary = _cli._bin_path("0.8.1")
            binary.parent.mkdir(parents=True)
            binary.write_text("binary")
            (binary.parent / _cli._INTEGRATIONS_NAME).write_text("integrations")
            authenticated = b"authenticated pack"
            digest = hashlib.sha256(authenticated).hexdigest()
            pack = binary.parent / f"cbm-ui-{digest}.pack"
            pack.write_bytes(b"corrupt pack")

            self.assertFalse(
                _cli._runtime_set_ready("0.8.1"),
                "a well-shaped filename must not authenticate different bytes",
            )

    def test_ui_publication_rejects_digest_mismatched_staged_pack(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            binary_name = "codebase-memory-mcp"
            authenticated = b"authenticated pack"
            pack = (
                f"cbm-ui-{hashlib.sha256(authenticated).hexdigest()}.pack"
            )
            staged_paths = {}
            for name, contents in (
                (binary_name, b"binary"),
                (_cli._INTEGRATIONS_NAME, b"integrations"),
                (pack, b"corrupt pack"),
            ):
                staged = directory / f".stage-{name}"
                staged.write_bytes(contents)
                staged_paths[name] = staged

            with self.assertRaisesRegex(RuntimeError, "digest"):
                _cli._publish_runtime_set(
                    staged_paths, directory / binary_name, binary_name, "ui"
                )

            self.assertFalse((directory / binary_name).exists())
            self.assertFalse((directory / _cli._RUNTIME_LOCK_NAME).exists())

    def test_publication_commits_sidecars_before_binary(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            binary_name = "codebase-memory-mcp"
            pack_bytes = b"pack"
            pack = f"cbm-ui-{hashlib.sha256(pack_bytes).hexdigest()}.pack"
            staged_paths = {}
            for name, contents in (
                (binary_name, b"binary"),
                (_cli._INTEGRATIONS_NAME, b"integrations"),
                (pack, pack_bytes),
            ):
                staged = directory / f"stage-{name}"
                staged.write_bytes(contents)
                staged_paths[name] = staged
            committed = []
            replace = os.replace

            def record_replace(source, destination):
                committed.append(Path(destination).name)
                replace(source, destination)

            with mock.patch.object(_cli.os, "replace", side_effect=record_replace):
                _cli._publish_runtime_set(
                    staged_paths, directory / binary_name, binary_name, "ui"
                )

            self.assertEqual(
                committed, [_cli._INTEGRATIONS_NAME, pack, binary_name]
            )

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
    _cli._INTEGRATIONS_NAME: staged / _cli._INTEGRATIONS_NAME,
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
    "standard",
    replace_file=pausing_replace,
    verifier=verifier,
)
"""
        binary_name = "codebase-memory-mcp"
        for crash_name, expected_ready in (
            (_cli._INTEGRATIONS_NAME, False),
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
                (directory / _cli._INTEGRATIONS_NAME).write_text(
                    "integrations:old"
                )
                (staged / binary_name).write_text("binary:candidate")
                (staged / _cli._INTEGRATIONS_NAME).write_text(
                    "integrations:candidate"
                )
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
                        _cli, "_variant", return_value="standard"
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
                        self.assertEqual(
                            (directory / _cli._INTEGRATIONS_NAME).read_text(),
                            "integrations:old",
                        )
                        retry_staged = root_path / "retry-staged"
                        retry_staged.mkdir()
                        retry_paths = {
                            binary_name: retry_staged / binary_name,
                            _cli._INTEGRATIONS_NAME: retry_staged
                            / _cli._INTEGRATIONS_NAME,
                        }
                        retry_paths[binary_name].write_text("binary:candidate")
                        retry_paths[_cli._INTEGRATIONS_NAME].write_text(
                            "integrations:candidate"
                        )
                        _cli._publish_runtime_set(
                            retry_paths,
                            directory / binary_name,
                            binary_name,
                            "standard",
                            verifier=verifier,
                        )

                    self.assertEqual(
                        (directory / binary_name).read_text(), "binary:candidate"
                    )
                    self.assertEqual(
                        (directory / _cli._INTEGRATIONS_NAME).read_text(),
                        "integrations:candidate",
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
            old_pack_bytes = b"old pack"
            old_pack = f"cbm-ui-{hashlib.sha256(old_pack_bytes).hexdigest()}.pack"
            (directory / binary_name).write_text("binary:old")
            (directory / _cli._INTEGRATIONS_NAME).write_text("integrations:old")
            (directory / old_pack).write_bytes(old_pack_bytes)

            candidate_pack_bytes = b"candidate pack"
            candidate_pack = (
                f"cbm-ui-{hashlib.sha256(candidate_pack_bytes).hexdigest()}.pack"
            )
            staged_paths = {}
            for name, contents in (
                (binary_name, b"binary:candidate"),
                (_cli._INTEGRATIONS_NAME, b"integrations:candidate"),
                (candidate_pack, candidate_pack_bytes),
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
                    "ui",
                    replace_file=fail_binary_publish,
                )

            self.assertTrue(failed)
            self.assertEqual(retired[0], binary_name)
            self.assertEqual((directory / binary_name).read_text(), "binary:old")
            self.assertEqual(
                (directory / _cli._INTEGRATIONS_NAME).read_text(),
                "integrations:old",
            )
            self.assertEqual((directory / old_pack).read_bytes(), old_pack_bytes)
            self.assertFalse((directory / candidate_pack).exists())
            self.assertFalse(
                any(
                    path.name.startswith(".cbm-runtime-backup-")
                    for path in directory.iterdir()
                )
            )

    def test_concurrent_publishers_preserve_the_first_complete_winner(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            binary_name = "codebase-memory-mcp"

            def stages(tag):
                pack_bytes = f"pack:{tag}".encode()
                pack = (
                    f"cbm-ui-{hashlib.sha256(pack_bytes).hexdigest()}.pack"
                )
                result = {}
                for name, contents in (
                    (binary_name, f"binary:{tag}".encode()),
                    (_cli._INTEGRATIONS_NAME, f"integrations:{tag}".encode()),
                    (pack, pack_bytes),
                ):
                    staged = directory / f".stage-{tag}-{name}"
                    staged.write_bytes(contents)
                    result[name] = staged
                return result, pack

            first_stages, first_pack = stages("first")
            second_stages, second_pack = stages("second")
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
                        "ui",
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
            self.assertEqual(
                (directory / _cli._INTEGRATIONS_NAME).read_bytes(),
                b"integrations:first",
            )
            self.assertEqual(
                (directory / first_pack).read_bytes(), b"pack:first"
            )
            self.assertFalse((directory / second_pack).exists())
            self.assertFalse((directory / _cli._RUNTIME_LOCK_NAME).exists())

    def test_failure_never_deletes_a_complete_foreign_winner(self):
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            binary_name = "codebase-memory-mcp"
            old_pack_bytes = b"pack:old"
            old_pack = (
                f"cbm-ui-{hashlib.sha256(old_pack_bytes).hexdigest()}.pack"
            )
            (directory / binary_name).write_bytes(b"binary:old")
            (directory / _cli._INTEGRATIONS_NAME).write_bytes(
                b"integrations:old"
            )
            (directory / old_pack).write_bytes(old_pack_bytes)

            candidate_pack_bytes = b"pack:candidate"
            candidate_pack = (
                f"cbm-ui-{hashlib.sha256(candidate_pack_bytes).hexdigest()}.pack"
            )
            staged_paths = {}
            for name, contents in (
                (binary_name, b"binary:candidate"),
                (_cli._INTEGRATIONS_NAME, b"integrations:candidate"),
                (candidate_pack, candidate_pack_bytes),
            ):
                staged = directory / f".stage-candidate-{name}"
                staged.write_bytes(contents)
                staged_paths[name] = staged

            foreign_pack_bytes = b"pack:foreign"
            foreign_pack = (
                f"cbm-ui-{hashlib.sha256(foreign_pack_bytes).hexdigest()}.pack"
            )
            replace = os.replace
            injected = False

            def install_foreign_winner(source, destination):
                nonlocal injected
                source = Path(source)
                destination = Path(destination)
                if destination == directory / binary_name and not injected:
                    injected = True
                    for path in directory.iterdir():
                        if path.name.startswith("cbm-ui-") and path.name.endswith(
                            ".pack"
                        ):
                            path.unlink()
                    (directory / _cli._INTEGRATIONS_NAME).write_bytes(
                        b"integrations:foreign"
                    )
                    (directory / foreign_pack).write_bytes(foreign_pack_bytes)
                    (directory / binary_name).write_bytes(b"binary:foreign")
                    raise OSError("simulated non-cooperating winner")
                replace(source, destination)

            _cli._publish_runtime_set(
                staged_paths,
                directory / binary_name,
                binary_name,
                "ui",
                replace_file=install_foreign_winner,
            )

            self.assertTrue(injected)
            self.assertEqual(
                (directory / binary_name).read_bytes(), b"binary:foreign"
            )
            self.assertEqual(
                (directory / _cli._INTEGRATIONS_NAME).read_bytes(),
                b"integrations:foreign",
            )
            self.assertEqual(
                (directory / foreign_pack).read_bytes(), foreign_pack_bytes
            )
            self.assertFalse((directory / candidate_pack).exists())
            self.assertFalse((directory / old_pack).exists())
            self.assertFalse(
                any(
                    path.name.startswith(".cbm-runtime-backup-")
                    for path in directory.iterdir()
                )
            )


if __name__ == "__main__":
    unittest.main()
