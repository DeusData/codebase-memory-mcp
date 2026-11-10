"""Worker trace polling tolerates Windows cleanup races without hiding I/O errors."""
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

from test_daemon_stability import read_cleanup_trace


class WorkerTraceTest(unittest.TestCase):
    def test_worker_cleanup_race_preserves_trace_and_retries(self):
        for winerror in (5, 32):
            with self.subTest(winerror=winerror), tempfile.TemporaryDirectory() as cache:
                Path(cache, "logs").mkdir()
                worker = str(Path(cache, "logs", ".worker-log-fixture"))
                stderr = str(Path(cache, "stderr"))
                Path(stderr).write_bytes(b"cli")
                Path(worker).write_bytes(b"previous")
                traces = {}
                self.assertIn(b"previous", read_cleanup_trace(stderr, cache, traces))
                denied = PermissionError(13, "Permission denied", worker)
                denied.winerror = winerror
                real_open = open

                def read(path, *args, **kwargs):
                    if path == worker:
                        raise denied
                    return real_open(path, *args, **kwargs)

                with mock.patch("builtins.open", side_effect=read):
                    trace = read_cleanup_trace(stderr, cache, traces)
                    self.assertIn(b"previous", trace)
                    self.assertIn(b"temporarily unreadable", trace)
                Path(worker).write_bytes(b"current")
                self.assertIn(b"current", read_cleanup_trace(stderr, cache, traces))
                os.unlink(worker)
                self.assertIn(b"current", read_cleanup_trace(stderr, cache, traces))

    def test_stderr_and_other_permission_errors_propagate(self):
        with tempfile.TemporaryDirectory() as cache:
            stderr = str(Path(cache, "stderr"))
            denied = PermissionError(13, "Permission denied", stderr)
            denied.winerror = 5
            with mock.patch("builtins.open", side_effect=denied):
                with self.assertRaises(PermissionError):
                    read_cleanup_trace(stderr, cache, {})
            Path(stderr).write_bytes(b"cli")
            Path(cache, "logs").mkdir()
            worker = str(Path(cache, "logs", ".worker-log-fixture"))
            Path(worker).write_bytes(b"worker")
            real_open = open

            def read(path, *args, **kwargs):
                if path == worker:
                    raise PermissionError(13, "Permission denied", worker)
                return real_open(path, *args, **kwargs)

            with mock.patch("builtins.open", side_effect=read):
                with self.assertRaises(PermissionError):
                    read_cleanup_trace(stderr, cache, {})


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
