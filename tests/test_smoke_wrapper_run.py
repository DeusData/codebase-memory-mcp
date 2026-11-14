"""The smoke watchdog reports hangs separately and cleans the command tree."""

import importlib.util
import pathlib
import queue
import subprocess
import sys
import threading
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location(
    "smoke_wrapper_run",
    pathlib.Path(__file__).resolve().parents[1] / "scripts/smoke-wrapper-run.py",
)
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


class WrapperWatchdogTest(unittest.TestCase):
    def test_preserves_actual_exit(self):
        for code in (0, 7):
            with self.subTest(code=code):
                self.assertEqual(runner.run([sys.executable, "-c", f"raise SystemExit({code})"]), code)

    def test_start_failure(self):
        with self.assertRaises(FileNotFoundError):
            runner.run([str(pathlib.Path(__file__).resolve().parent / "missing-wrapper-2877")])

    def test_hang_stops_real_process(self):
        # Inject the outer expiry only after the child is ready, without sleeps.
        real_popen = subprocess.Popen
        child = None

        def start(command, **options):
            nonlocal child
            child = real_popen(command, stdout=subprocess.PIPE, text=True, **options)
            ready = queue.Queue()
            threading.Thread(target=lambda: ready.put(child.stdout.readline()), daemon=True).start()
            self.assertEqual(ready.get(timeout=30), "ready\n")
            real_wait = child.wait
            expired = False

            def wait(timeout):
                nonlocal expired
                if not expired:
                    expired = True
                    raise subprocess.TimeoutExpired(command, timeout)
                return real_wait(timeout=timeout)

            child.wait = wait
            return child

        try:
            # taskkill uses Popen too; let that separate command run normally.
            def spawn(command, **options):
                return real_popen(command, **options) if command[0] == "taskkill" else start(command, **options)
            with mock.patch.object(runner.subprocess, "Popen", side_effect=spawn), \
                    mock.patch.object(runner.sys, "stderr"):
                self.assertEqual(runner.run([
                    sys.executable, "-u", "-c",
                    "import sys, threading; sys.stdout.buffer.write(b'ready\\r\\n'); "
                    "sys.stdout.buffer.flush(); threading.Event().wait()",
                ]), 124)
                self.assertIsNotNone(child.poll())
        finally:
            if child is not None:
                if child.poll() is None:
                    child.kill()
                child.wait(timeout=30)
                child.stdout.close()

    def test_hang_kills_tree_and_reaps(self):
        for platform in ("posix", "nt"):
            with self.subTest(platform=platform), \
                    mock.patch.object(runner.os, "name", platform), \
                    mock.patch.object(runner.subprocess, "Popen") as popen, \
                    mock.patch.object(runner.subprocess, "run") as taskkill, \
                    mock.patch.object(runner.os, "killpg", create=True) as killpg, \
                    mock.patch.object(runner.signal, "SIGKILL", 9, create=True), \
                    mock.patch.object(runner.sys, "stderr") as diagnostic:
                process = popen.return_value
                process.pid = 123
                process.wait.side_effect = [subprocess.TimeoutExpired("candidate", 600), 0]
                self.assertEqual(runner.run(["candidate"]), 124)
                popen.assert_called_once_with(["candidate"], start_new_session=platform != "nt")
                self.assertEqual(process.wait.call_args_list, [mock.call(timeout=600), mock.call(timeout=30)])
                if platform == "nt":
                    taskkill.assert_called_once_with(
                        ["taskkill", "/PID", "123", "/T", "/F"], check=True, timeout=30)
                    killpg.assert_not_called()
                else:
                    killpg.assert_called_once_with(123, runner.signal.SIGKILL)
                    taskkill.assert_not_called()
                self.assertIn("HANG go", "".join(call.args[0] for call in diagnostic.write.call_args_list))


if __name__ == "__main__":
    unittest.main()
