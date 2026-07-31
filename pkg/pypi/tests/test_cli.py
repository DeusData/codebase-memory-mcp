import sys
import unittest
from pathlib import Path


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

    def test_portable_mutation_guidance_classification_is_preserved(self):
        self.assertEqual(_cli._portable_mutation_action(["update"]), "update")
        self.assertEqual(
            _cli._portable_mutation_action(["uninstall", "--yes"]),
            "uninstall",
        )
        self.assertIsNone(_cli._portable_mutation_action(["install", "--yes"]))
        self.assertIsNone(_cli._portable_mutation_action(["cli", "update"]))


if __name__ == "__main__":
    unittest.main()
