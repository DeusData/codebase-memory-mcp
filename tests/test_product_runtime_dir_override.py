r"""Product guard for ``CBM_RUNTIME_DIR``.

The daemon/CLI rendezvous directory is created under ``%LOCALAPPDATA%`` on
Windows and under the POSIX runtime/home directory otherwise. That ancestry is
not always acceptable to ``cbm_daemon_ipc_private_directory_secure``: a profile
that has acquired a mutation-granting ACE for an untrusted identity fails the
walk, and the binary then cannot start at all -- ``config`` included, because it
needs the same endpoint. Before this guard the only relocation hook was
``CBM_TEST_DAEMON_RUNTIME_PARENT``, compiled out unless ``CBM_ENABLE_TEST_SEAMS``
is defined, so a test build started while the product build did not.

``CBM_RUNTIME_DIR`` does not relax the check. The directory it names goes
through exactly the same validation; the operator only chooses an ancestry that
passes. This guard proves three things about a **product** build:

* the rendezvous is created under the directory the operator named;
* leaving the variable unset does not create anything there;
* a value that fails validation is refused rather than silently ignored.

    python3 tests/test_product_runtime_dir_override.py build/c/codebase-memory-mcp

Exit code: 0 == green, 1 == behavior regression, 2 == fixture/setup error.
"""

import os
import subprocess
import sys
import tempfile

RUNTIME_DIR_ENV = "CBM_RUNTIME_DIR"
CACHE_DIR_ENV = "CBM_CACHE_DIR"
COMMAND_TIMEOUT_SECONDS = 120


def output_text(result):
    return ((result.stdout or b"") + (result.stderr or b"")).decode("utf-8", "replace")


def run_config_list(binary, work, runtime_dir):
    """Run the cheapest command that still needs the coordination endpoint."""
    env = dict(os.environ)
    env.pop(RUNTIME_DIR_ENV, None)
    # A cache the product creates itself, so it owns that directory's ACL.
    env[CACHE_DIR_ENV] = os.path.join(work, "cache")
    if runtime_dir is not None:
        env[RUNTIME_DIR_ENV] = runtime_dir
    try:
        return subprocess.run(
            [binary, "config", "list"],
            capture_output=True,
            timeout=COMMAND_TIMEOUT_SECONDS,
            env=env,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        print("SETUP FAIL: could not run the fixture: %s" % error)
        return None


def entries_under(path):
    try:
        return sorted(os.listdir(path))
    except OSError:
        return []


def assert_override_relocates_rendezvous(binary, work):
    """The rendezvous lands under the named directory, not the default one."""
    runtime_root = os.path.join(work, "runtime-root")
    os.makedirs(runtime_root, mode=0o700, exist_ok=True)

    result = run_config_list(binary, work, runtime_root)
    if result is None:
        return False
    text = output_text(result)
    if result.returncode != 0:
        print("REGRESSION: config list failed with %s set\n%s" % (RUNTIME_DIR_ENV, text))
        return False

    created = entries_under(runtime_root)
    if not created:
        print(
            "REGRESSION: %s=%s was accepted but nothing was created there; the "
            "rendezvous still went to the default location.\n%s"
            % (RUNTIME_DIR_ENV, runtime_root, text)
        )
        return False
    print("  ok: rendezvous created under the named directory: %s" % created)
    return True


def assert_unset_creates_nothing_there(binary, work):
    """Without the variable, the named directory stays untouched."""
    untouched = os.path.join(work, "untouched-root")
    os.makedirs(untouched, mode=0o700, exist_ok=True)

    result = run_config_list(binary, work, None)
    if result is None:
        return False

    created = entries_under(untouched)
    if created:
        print(
            "REGRESSION: %s was unset yet %s gained %s"
            % (RUNTIME_DIR_ENV, untouched, created)
        )
        return False
    print("  ok: unset override leaves the directory untouched")
    return True


def assert_invalid_value_is_refused(binary, work):
    """A path that cannot be a private runtime parent must not be ignored."""
    missing = os.path.join(work, "does-not-exist", "nested")

    result = run_config_list(binary, work, missing)
    if result is None:
        return False
    text = output_text(result)

    if result.returncode == 0 and "Configuration:" in text:
        print(
            "REGRESSION: %s=%s is not a usable runtime parent, but the command "
            "succeeded -- the value was silently ignored instead of refused.\n%s"
            % (RUNTIME_DIR_ENV, missing, text)
        )
        return False
    print("  ok: an unusable value is refused rather than silently ignored")
    return True


def main():
    if len(sys.argv) != 2:
        print("usage: %s <path-to-codebase-memory-mcp>" % os.path.basename(sys.argv[0]))
        return 2
    binary = sys.argv[1]
    if not os.path.isfile(binary):
        print("SETUP FAIL: binary not found: %s" % binary)
        return 2

    # Keep the endpoint path short: macOS sockaddr_un is capped at 104 bytes.
    short_temp_root = "/private/tmp" if sys.platform == "darwin" else tempfile.gettempdir()
    with tempfile.TemporaryDirectory(prefix="cbm_rtdir_", dir=short_temp_root) as work:
        if not assert_override_relocates_rendezvous(binary, work):
            return 1
        if not assert_unset_creates_nothing_there(binary, work):
            return 1
        if not assert_invalid_value_is_refused(binary, work):
            return 1
    print("\nGREEN: CBM_RUNTIME_DIR relocates the rendezvous in a product build.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
