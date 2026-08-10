import importlib.util
import os
import tempfile
import unittest
from unittest import mock


SCRIPT = os.path.join(os.path.dirname(__file__), "test_daemon_open_readiness.py")
SPEC = importlib.util.spec_from_file_location("test_daemon_open_readiness", SCRIPT)
READINESS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(READINESS)


class DaemonOpenReadinessPathTest(unittest.TestCase):
    def test_windows_extensionless_path_resolves_exe(self):
        with tempfile.TemporaryDirectory() as work:
            binary = os.path.join(work, "codebase-memory-mcp")
            executable = binary + ".exe"
            with open(executable, "wb"):
                pass

            self.assertEqual(READINESS.resolve_binary_path(binary, "nt"), executable)

    def test_stop_waits_for_daemon_process_exit(self):
        stop = READINESS.subprocess.CompletedProcess([], 0, b"", b"")
        absent = READINESS.subprocess.CompletedProcess([], 1, b"daemon: not running", b"")

        with mock.patch.object(READINESS.subprocess, "run", side_effect=[stop, absent]):
            with mock.patch.object(READINESS, "wait_for_process_exit",
                                   return_value=True) as wait:
                READINESS.stop_daemon("codebase-memory-mcp", {}, 1234)

        wait.assert_called_once_with(1234, 5)


if __name__ == "__main__":
    unittest.main()
