import importlib.util
import os
import tempfile
import unittest


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


if __name__ == "__main__":
    unittest.main()
