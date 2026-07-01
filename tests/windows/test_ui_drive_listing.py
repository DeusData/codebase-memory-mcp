r"""RED integration test — the UI directory picker cannot reach non-system drives.

Reproduces issue #548 at the product surface (the embedded HTTP UI).

The UI directory picker calls `GET /api/browse?path=...` (handle_browse in
src/ui/http_server.c). For the filesystem root it does `opendir("/")`, which on
Windows resolves to the *current* drive's root and lists only that drive's
subdirectories. There is no `GetLogicalDriveStrings` drive enumeration, so when a
user opens the picker at root, drives other than the system drive (e.g. `D:\`,
`E:\`) never appear and cannot be selected.

This test requires a UI build (`make -f Makefile.cbm cbm-with-ui`) because the
HTTP server only starts when the frontend is embedded. It launches the server,
queries `/api/browse?path=/`, and asserts that every fixed drive on the machine
is reachable from the root listing. It is meaningful only on a machine with more
than one drive; with a single drive it reports a precondition error (exit 2).

Passes on a correct picker that enumerates drives; fails on native Windows until
handle_browse enumerates logical drives for the root path.

Exit code: 0 == all drives reachable (green), 1 == non-system drives missing
(red), 2 == precondition not met (single drive / no UI build / server down).

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


def wait_for_server(port, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.3)
    return False


def main():
    if len(sys.argv) < 2:
        print("usage: python test_ui_drive_listing.py <ui-binary> [port]")
        return 2
    binary = os.path.abspath(sys.argv[1])
    if not os.path.exists(binary):
        print("FAIL: binary not found: %s" % binary)
        return 2

    drives = list_fixed_drives()
    extra = [d for d in drives if not d.upper().startswith("C:")]
    print("fixed drives: %s" % drives)
    if not extra:
        print("PRECONDITION: only one drive present; cannot test multi-drive "
              "picker. Re-run on a machine with a D:/E: drive.")
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
            err = b""
            try:
                proc.stderr.settimeout = None
            except Exception:
                pass
            print("PRECONDITION: HTTP server did not start on port %d. Is this a "
                  "UI build (make cbm-with-ui)?" % port)
            return 2

        # Control: browsing an explicit existing directory must return entries,
        # proving the endpoint works and isolating the bug to root enumeration.
        import urllib.parse
        home = os.environ.get("USERPROFILE") or os.path.expanduser("~")
        home_fwd = home.replace("\\", "/")
        try:
            ctrl = http_get_json("http://127.0.0.1:%d/api/browse?path=%s" %
                                 (port, urllib.parse.quote(home_fwd)))
        except Exception as ex:
            print("PRECONDITION: control /api/browse?path=%s failed: %r" %
                  (home_fwd, ex))
            return 2
        print("control browse(%r) -> dirs(%d)" % (home_fwd, len(ctrl.get("dirs", []))))
        if not ctrl.get("dirs"):
            print("PRECONDITION: control browse returned no dirs; endpoint may be "
                  "non-functional in this build.")
            return 2

        # Browse the filesystem root.
        try:
            root = http_get_json("http://127.0.0.1:%d/api/browse?path=/" % port)
        except Exception as ex:
            print("PRECONDITION: /api/browse?path=/ failed: %r" % ex)
            return 2
        root_dirs = root.get("dirs", [])
        print("browse('/') -> path=%r dirs(%d)=%s" %
              (root.get("path"), len(root_dirs), root_dirs[:20]))

        # A correct root listing must let the user reach every drive. Accept a
        # match whether the API returns "D:", "D", or "D:\\"/"D:/".
        def reachable(drive_root):
            letter = drive_root[0].upper()
            cands = {letter, letter + ":", letter + ":\\", letter + ":/",
                     drive_root, drive_root.rstrip("\\/")}
            return any(str(d).rstrip("\\/").upper() in
                       {x.rstrip("\\/").upper() for x in cands} for d in root_dirs)

        missing = [d for d in extra if not reachable(d)]
        if not missing:
            print("\nGREEN: all non-system drives reachable from the root picker.")
            return 0
        print("\nRED: drives %s are not reachable from the UI root picker "
              "(/api/browse?path=/ lists only the current drive; handle_browse "
              "does not enumerate logical drives)." % missing)
        return 1
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
