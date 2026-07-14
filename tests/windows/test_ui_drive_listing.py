r"""GREEN regression guard — the UI directory picker enumerates all logical drives.

Guards the fix for issue #548 (landed on main). `handle_browse`
(src/ui/http_server.c) appends a `"roots"` array to every `/api/browse`
response; on Windows `append_roots_json` fills it from `GetLogicalDrives()` as
`"C:/"`, `"D:/"`, … so the directory picker can reach every drive (POSIX emits
a single `"/"`). This test asserts that user-level invariant against the running
embedded HTTP UI.

Before #548 the picker did `opendir("/")`, listing only the current drive's
subdirectories under `dirs` with no drive enumeration — non-system drives were
unreachable. The original red test asserted drives appeared in `dirs`; the fix
intentionally exposes them via the separate `roots` field, so this guard checks
`roots` (and that each advertised drive is actually browsable).

Requires a UI build (`make -f Makefile.cbm cbm-with-ui`) because the HTTP server
only starts when the frontend is embedded. Runs green with a single drive (C:/
must be advertised and browsable); a machine with D:/E: exercises the multi-drive
reach more fully.

Exit code: 0 == all drives advertised in roots and reachable (green),
1 == a drive is missing from roots or not browsable (regression),
2 == precondition not met (no UI build / server down).

Usage:
    python test_ui_drive_listing.py <path-to-codebase-memory-mcp-ui[.exe]> [port]
"""
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request


def list_fixed_drives():
    # Python 3.12+: os.listdrives(). Fall back to scanning A:..Z:.
    listdrives = getattr(os, "listdrives", None)
    if listdrives:
        try:
            return [d for d in listdrives()]
        except Exception:
            pass
    found = []
    for ch in "CDEFGHIJKLMNOPQRSTUVWXYZ":
        root = "%s:\\" % ch
        if os.path.isdir(root):
            found.append(root)
    return found


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def http_get_json(url, timeout=5):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


def browse(port, path):
    return http_get_json("http://127.0.0.1:%d/api/browse?path=%s" %
                         (port, urllib.parse.quote(path)))


def wait_for_server(port, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.3)
    return False


def norm(s):
    """Canonical drive key: 'D:\\', 'D:/', 'D:', 'D' -> 'D:'."""
    s = str(s).rstrip("\\/").upper()
    return s if s.endswith(":") else (s + ":" if len(s) == 1 else s)


def main():
    if len(sys.argv) < 2:
        print("usage: python test_ui_drive_listing.py <ui-binary> [port]")
        return 2
    binary = os.path.abspath(sys.argv[1])
    if not os.path.exists(binary):
        print("FAIL: binary not found: %s" % binary)
        return 2

    drives = list_fixed_drives()
    print("fixed drives: %s" % drives)
    if not drives:
        print("PRECONDITION: no fixed drives detected.")
        return 2

    work = tempfile.mkdtemp(prefix="cbm_win_uidrv_")
    port = int(sys.argv[2]) if len(sys.argv) > 2 else free_port()
    env = dict(os.environ)
    env["CBM_CACHE_DIR"] = os.path.join(work, "cache")
    os.makedirs(env["CBM_CACHE_DIR"], exist_ok=True)
    proc = subprocess.Popen([binary, "--ui=true", "--port=%d" % port],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, env=env)
    try:
        if not wait_for_server(port, timeout=25):
            print("PRECONDITION: HTTP server did not start on port %d. Is this a "
                  "UI build (make -f Makefile.cbm cbm-with-ui)?" % port)
            return 2

        # Control: browsing an explicit existing directory must return a payload
        # carrying the roots field, proving the endpoint works and isolating any
        # failure to drive enumeration itself. roots is appended to *every*
        # browse response, so any valid directory surfaces it.
        home = os.environ.get("USERPROFILE") or os.path.expanduser("~")
        home_fwd = home.replace("\\", "/")
        try:
            ctrl = browse(port, home_fwd)
        except Exception as ex:
            print("PRECONDITION: control /api/browse?path=%s failed: %r" %
                  (home_fwd, ex))
            return 2
        roots = ctrl.get("roots")
        print("control browse(%r) -> dirs(%d) roots=%s" %
              (home_fwd, len(ctrl.get("dirs", [])), roots))
        if roots is None:
            print("PRECONDITION: response has no 'roots' field; build predates "
                  "#548 or endpoint non-functional.")
            return 2

        # Invariant 1: every fixed drive is advertised in roots.
        adv = {norm(r) for r in roots}
        missing = [d for d in drives if norm(d) not in adv]
        if missing:
            print("\nRED: drives %s are not advertised in the picker's roots "
                  "array %s (handle_browse/append_roots_json did not enumerate "
                  "them)." % (missing, roots))
            return 1

        # Invariant 2: every advertised drive is actually browsable (a user can
        # select it). This is the user-level reach the fix promises.
        unreachable = []
        for d in drives:
            drive_root = norm(d) + "/"   # "D:/"
            try:
                resp = browse(port, drive_root)
                if resp.get("roots") is None and "dirs" not in resp:
                    unreachable.append(d)
            except Exception as ex:
                print("  browse(%r) failed: %r" % (drive_root, ex))
                unreachable.append(d)
        if unreachable:
            print("\nRED: drives advertised in roots but not browsable: %s" %
                  unreachable)
            return 1

        print("\nGREEN: all %d fixed drive(s) advertised in roots and reachable "
              "from the UI picker." % len(drives))
        return 0
    finally:
        try:
            proc.stdin.close()
        except Exception:
            pass
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
