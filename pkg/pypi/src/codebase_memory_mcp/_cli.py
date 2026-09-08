"""Downloads codebase-memory-mcp on first run, then runs its native entry point."""

import errno
import hashlib
import json
import os
import platform
import re
import secrets
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

REPO = "DeusData/codebase-memory-mcp"
_WINDOWS_BINARY_NAME = "codebase-memory-mcp.exe"
_UNIX_ARCHIVE_NAMES = (
    "codebase-memory-mcp",
    "LICENSE",
    "install.sh",
    "THIRD_PARTY_NOTICES.md",
)
_WINDOWS_ARCHIVE_NAMES = (
    _WINDOWS_BINARY_NAME,
    "LICENSE",
    "install.ps1",
    "THIRD_PARTY_NOTICES.md",
)

# Security: only permit https fetches. urllib's default handlers accept
# file://, ftp://, and custom schemes — a redirect or tainted URL source
# could otherwise turn a download into an arbitrary-local-file read.
_ALLOWED_SCHEMES = frozenset({"https"})
_MAX_REDIRECTS = 5
_NETWORK_TIMEOUT_SECONDS = 120
_CANDIDATE_TIMEOUT_SECONDS = 15
_MAX_CHECKSUM_MANIFEST_BYTES = 1024 * 1024
_REDIRECT_CODES = frozenset({301, 302, 303, 307, 308})
_RUNTIME_LOCK_NAME = ".codebase-memory-mcp-runtime.lock"
_RUNTIME_LOCK_WAIT_SECONDS = 45
_RUNTIME_OWNERLESS_STALE_SECONDS = 30
_RUNTIME_LOCK_POLL_SECONDS = 0.025
_RUNTIME_LOCK_LEASE_SECONDS = 300
_RUNTIME_LOCK_HEARTBEAT_SECONDS = 20
_RUNTIME_LEGACY_STALE_SECONDS = 3600
_RUNTIME_BACKUP_PREFIX = ".cbm-runtime-backup-"
_RUNTIME_BACKUP_RE = re.compile(r"\.cbm-runtime-backup-[0-9a-f]{32}\Z")
_RUNTIME_BACKUP_RETIRED_MARKER = ".retirement-complete"
_RUNTIME_BACKUP_CLEANUP_MARKER = ".cleanup-only"
_WINDOWS_SYNCHRONIZE = 0x00100000
_WINDOWS_WAIT_OBJECT_0 = 0x00000000
_WINDOWS_WAIT_TIMEOUT = 0x00000102
_WINDOWS_ERROR_INVALID_PARAMETER = 87
_runtime_lock_claim_observer = None


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Expose redirects to _download_https for explicit per-hop validation."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_HTTPS_OPENER = urllib.request.build_opener(_NoRedirectHandler())


def _validate_url_scheme(url: str) -> None:
    """Reject non-https URLs before any network fetch."""
    parsed = urllib.parse.urlparse(url)
    if (
        parsed.scheme not in _ALLOWED_SCHEMES
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
    ):
        sys.exit(
            f"codebase-memory-mcp: refusing to fetch invalid, credentialed, or "
            f"non-https URL: {url}"
        )


def _download_https(url: str, dest: str, max_bytes: int = 0) -> None:
    """Download through a bounded HTTPS-only opener into dest."""
    current_url = url
    for redirect_count in range(_MAX_REDIRECTS + 1):
        _validate_url_scheme(current_url)
        request = urllib.request.Request(
            current_url, headers={"User-Agent": "codebase-memory-mcp-installer"}
        )
        try:
            response = _HTTPS_OPENER.open(
                request, timeout=_NETWORK_TIMEOUT_SECONDS
            )
        except urllib.error.HTTPError as exc:
            if exc.code not in _REDIRECT_CODES:
                raise
            location = exc.headers.get("Location")
            exc.close()
            if not location:
                raise RuntimeError(f"redirect has no Location: {current_url}")
            if redirect_count == _MAX_REDIRECTS:
                raise RuntimeError("too many redirects")
            current_url = urllib.parse.urljoin(current_url, location)
            _validate_url_scheme(current_url)
            continue

        with response:
            _validate_url_scheme(response.geturl())
            deadline = time.monotonic() + _NETWORK_TIMEOUT_SECONDS
            total = 0
            with open(dest, "wb") as out:
                while True:
                    if time.monotonic() >= deadline:
                        raise TimeoutError(
                            f"download hop timed out: {current_url}"
                        )
                    chunk = response.read(65536)
                    if not chunk:
                        return
                    total += len(chunk)
                    if max_bytes and total > max_bytes:
                        raise RuntimeError(
                            f"download exceeds the {max_bytes}-byte safety limit"
                        )
                    out.write(chunk)

    raise RuntimeError("too many redirects")


def _verify_candidate(path: Path) -> None:
    """Require a staged native candidate to execute successfully."""
    try:
        subprocess.run(
            [str(path), "--version"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
            timeout=_CANDIDATE_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(f"downloaded binary failed to run: {exc}") from exc


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _files_equal_sha256(left: Path, right: Path) -> bool:
    try:
        return (
            _regular_file(left)
            and _regular_file(right)
            and left.stat().st_size == right.stat().st_size
            and _file_sha256(left) == _file_sha256(right)
        )
    except OSError:
        return False


def _validate_archive_names(names, archive_names, casefold: bool = False):
    """Require the exact release root namespace."""
    required = tuple(archive_names)
    required_set = set(required)
    found = set()
    seen = set()
    for name in names:
        key = name.casefold() if casefold else name
        if key in seen:
            sys.exit(
                f"codebase-memory-mcp: refusing unsafe archive entry "
                f"(duplicate or case conflict: {name!r})"
            )
        seen.add(key)
        if name not in required_set:
            sys.exit(
                f"codebase-memory-mcp: archive must contain only the exact "
                f"root files: {', '.join(required)}"
            )
        found.add(name)
    if found != required_set or len(seen) != len(required):
        sys.exit(
            f"codebase-memory-mcp: archive must contain exactly one of each "
            f"required root file: {', '.join(required)}"
        )


def _safe_extract_tar(
    tf, dest: str, archive_names=(), extract_names=(), ui: bool = False
):
    """Validate a tar namespace, then extract only its authenticated runtime set."""
    members = tf.getmembers()
    for member in members:
        if not member.isfile():
            sys.exit(
                f"codebase-memory-mcp: refusing unsafe tar entry "
                f"(not a regular root file: {member.name!r})"
            )
    _validate_archive_names([member.name for member in members], archive_names)
    runtime_names = list(extract_names)
    by_name = {member.name: member for member in members}
    dest_abs = os.path.abspath(dest)
    for name in runtime_names:
        source = tf.extractfile(by_name[name])
        if source is None:
            sys.exit(f"codebase-memory-mcp: could not read archive member: {name}")
        target = os.path.join(dest_abs, name)
        with source, open(target, "xb") as output:
            shutil.copyfileobj(source, output)
    return tuple(runtime_names)


def _safe_extract_zip(
    zf, dest: str, archive_names=(), extract_names=(), ui: bool = False
):
    """Extract a zipfile after validating its Windows-style namespace.

    Windows treats paths case-insensitively. Rejecting duplicate/case-conflicting
    members before extraction prevents archive order from selecting the binary
    that a portable package-manager shim eventually executes.
    """
    dest_abs = os.path.abspath(dest)
    names = []
    for info in zf.infolist():
        raw_name = info.filename
        name = raw_name.replace("\\", "/")
        segments_name = name[:-1] if name.endswith("/") else name
        segments = segments_name.split("/")
        if (
            not segments_name
            or name.startswith("/")
            or ":" in name
            or any(segment in ("", ".", "..") for segment in segments)
            or any(segment.endswith((".", " ")) for segment in segments)
        ):
            sys.exit(
                f"codebase-memory-mcp: refusing unsafe zip entry "
                f"(invalid path: {raw_name!r})"
            )
        unix_type = stat.S_IFMT((info.external_attr >> 16) & 0xFFFF)
        if info.is_dir() or unix_type not in (0, stat.S_IFREG):
            sys.exit(
                f"codebase-memory-mcp: refusing unsafe zip entry "
                f"(not a regular root file: {raw_name!r})"
            )
        names.append(name)
        member_abs = os.path.abspath(os.path.join(dest_abs, *segments))
        if not (member_abs == dest_abs or member_abs.startswith(dest_abs + os.sep)):
            sys.exit(
                f"codebase-memory-mcp: refusing unsafe zip entry "
                f"(escapes dest: {raw_name!r})"
            )
    _validate_archive_names(names, archive_names, casefold=True)

    # Extract only the validated runtime files. This avoids relying on
    # platform-specific zip path rewriting and always creates regular files.
    runtime_names = list(extract_names)
    for name in runtime_names:
        target = os.path.join(dest_abs, name)
        with zf.open(name) as source, open(target, "xb") as output:
            shutil.copyfileobj(source, output)
    return tuple(runtime_names)


def _verify_checksum(archive_path: str, archive_name: str, version: str) -> None:
    """Verify SHA256 checksum against checksums.txt from the release."""
    url = f"https://github.com/{REPO}/releases/download/v{version}/checksums.txt"
    tmp_path = None
    try:
        _validate_url_scheme(url)
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tmp:
            tmp_path = tmp.name
        _download_https(url, tmp_path, _MAX_CHECKSUM_MANIFEST_BYTES)
        expected = None
        with open(tmp_path, encoding="utf-8") as f:
            for line in f:
                fields = line.split()
                if len(fields) < 2 or fields[1] not in (
                    archive_name,
                    f"*{archive_name}",
                ):
                    continue
                digest = fields[0].lower()
                if len(digest) != 64 or any(
                    ch not in "0123456789abcdef" for ch in digest
                ):
                    sys.exit(
                        f"codebase-memory-mcp: invalid SHA256 checksum for "
                        f"{archive_name}"
                    )
                if expected is not None and expected != digest:
                    sys.exit(
                        f"codebase-memory-mcp: conflicting SHA256 checksums for "
                        f"{archive_name}"
                    )
                expected = digest
        if expected is None:
            sys.exit(
                f"codebase-memory-mcp: no checksum for {archive_name} in checksums.txt"
            )
        h = hashlib.sha256()
        with open(archive_path, "rb") as af:
            for chunk in iter(lambda: af.read(65536), b""):
                h.update(chunk)
        actual = h.hexdigest()
        if expected != actual:
            sys.exit(
                f"codebase-memory-mcp: CHECKSUM MISMATCH for {archive_name}\n"
                f"  expected: {expected}\n"
                f"  actual:   {actual}"
            )
        print("codebase-memory-mcp: checksum verified.", file=sys.stderr)
    except SystemExit:
        raise
    except Exception as exc:
        sys.exit(f"codebase-memory-mcp: checksum verification failed: {exc}")
    finally:
        if tmp_path is not None:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


def _version() -> str:
    try:
        from importlib.metadata import version
        return version("codebase-memory-mcp")
    except Exception:
        return "0.10.8"


def _os_name() -> str:
    p = sys.platform
    if p == "linux":
        return "linux"
    if p == "darwin":
        return "darwin"
    if p == "win32":
        return "windows"
    sys.exit(f"codebase-memory-mcp: unsupported platform: {p}")


def _arch() -> str:
    m = platform.machine().lower()
    if m in ("arm64", "aarch64"):
        return "arm64"
    if m in ("x86_64", "amd64"):
        return "amd64"
    sys.exit(f"codebase-memory-mcp: unsupported architecture: {m}")


def _cache_dir() -> Path:
    if sys.platform == "win32":
        base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
    elif sys.platform == "darwin":
        base = Path.home() / "Library" / "Caches"
    else:
        base = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
    return base / "codebase-memory-mcp"


def _runtime_dir(version: str) -> Path:
    return _cache_dir() / version


def _bin_path(version: str) -> Path:
    # One binary per platform: the cached file is the executed file.
    name = (
        _WINDOWS_BINARY_NAME
        if sys.platform == "win32"
        else "codebase-memory-mcp"
    )
    return _runtime_dir(version) / name


def _execution_path(binary: Path, target_platform: str) -> Path:
    """The cached binary is executed directly on every platform."""
    return binary


def _regular_file(path: Path) -> bool:
    try:
        status = path.lstat()
        return (
            stat.S_ISREG(status.st_mode)
            and not stat.S_ISLNK(status.st_mode)
            and status.st_nlink == 1
        )
    except OSError:
        return False


def _runtime_set_names(directory: Path, binary_name: str):
    if not _regular_file(directory / binary_name):
        return None
    return (binary_name,)


def _runtime_set_ready(version: str, verifier=None) -> bool:
    binary = _bin_path(version)
    if _runtime_set_names(binary.parent, binary.name) is None:
        return False
    if verifier is not None:
        try:
            verifier(binary)
        except RuntimeError:
            return False
    return True


def _require_safe_runtime_directory(directory: Path):
    status = directory.lstat()
    if not stat.S_ISDIR(status.st_mode) or stat.S_ISLNK(status.st_mode):
        raise RuntimeError(f"refusing unsafe package-cache directory: {directory}")


def _runtime_set_ready_locked(version: str) -> bool:
    binary = _bin_path(version)
    binary.parent.mkdir(parents=True, exist_ok=True)
    _require_safe_runtime_directory(binary.parent)
    lock, _ = _acquire_runtime_lock(binary.parent)
    try:
        _refresh_runtime_lock(lock)
        _reconcile_runtime_backups(
            binary.parent, binary.name, _verify_candidate, lock
        )
        return _runtime_set_ready(version, _verify_candidate)
    finally:
        _release_runtime_lock(lock)


def _runtime_set_fingerprint(directory: Path, binary_name: str):
    """Identify one complete set so a lock waiter can preserve its winner."""
    names = _runtime_set_names(directory, binary_name)
    if names is None:
        return None
    try:
        return tuple(
            (name, _file_sha256(directory / name)) for name in names
        )
    except OSError:
        return None


def _staged_runtime_names(staged_paths, binary_name: str):
    names = set(staged_paths)
    required = {binary_name}
    if names != required or not _regular_file(staged_paths[binary_name]):
        raise RuntimeError("staged package cache is incomplete")
    return (binary_name,)


def _windows_process_api():
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = (
        wintypes.DWORD,
        wintypes.BOOL,
        wintypes.DWORD,
    )
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.WaitForSingleObject.argtypes = (
        wintypes.HANDLE,
        wintypes.DWORD,
    )
    kernel32.WaitForSingleObject.restype = wintypes.DWORD
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL
    return kernel32, ctypes.get_last_error


def _windows_process_is_alive(pid: int) -> bool:
    if pid > 0xFFFFFFFF:
        # Avoid truncating an unprobeable owner identifier into another PID.
        return True
    try:
        kernel32, get_last_error = _windows_process_api()
        handle = kernel32.OpenProcess(_WINDOWS_SYNCHRONIZE, False, pid)
    except Exception:
        return True
    if not handle:
        try:
            error = get_last_error()
        except Exception:
            return True
        # OpenProcess documents ERROR_INVALID_PARAMETER for a PID that no
        # longer identifies a process. Access denial remains evidence of a
        # potentially live owner and must not authorize lock reclamation.
        return error != _WINDOWS_ERROR_INVALID_PARAMETER
    try:
        try:
            wait_result = kernel32.WaitForSingleObject(handle, 0)
        except Exception:
            return True
        if wait_result == _WINDOWS_WAIT_OBJECT_0:
            return False
        if wait_result == _WINDOWS_WAIT_TIMEOUT:
            return True
        return True
    finally:
        try:
            kernel32.CloseHandle(handle)
        except Exception:
            pass


def _process_is_alive(pid: int) -> bool:
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return False
    if os.name == "nt":
        return _windows_process_is_alive(pid)
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as exc:
        return exc.errno != errno.ESRCH


def _runtime_lock_status(lock_path: Path):
    try:
        return lock_path.lstat()
    except OSError:
        return None


def _same_runtime_lock_object(left, right):
    return (
        left is not None
        and right is not None
        and left.st_dev == right.st_dev
        and left.st_ino == right.st_ino
        and stat.S_IFMT(left.st_mode) == stat.S_IFMT(right.st_mode)
    )


def _read_runtime_lock_owner(lock_path: Path):
    try:
        status = lock_path.lstat()
        if stat.S_ISLNK(status.st_mode) or not (
            stat.S_ISREG(status.st_mode) or stat.S_ISDIR(status.st_mode)
        ):
            return None
        owner_path = lock_path / "owner.json" if stat.S_ISDIR(status.st_mode) else lock_path
        owner = json.loads(owner_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    pid = owner.get("pid")
    token = owner.get("token")
    if (
        not isinstance(pid, int)
        or isinstance(pid, bool)
        or pid <= 0
        or not isinstance(token, str)
        or re.fullmatch(r"[0-9a-f]{32}", token) is None
    ):
        return None
    lease_expires = owner.get("lease_expires_ms")
    if not isinstance(lease_expires, (int, float)) or isinstance(
        lease_expires, bool
    ):
        lease_expires = None
    return pid, token, lease_expires


def _restore_displaced_runtime_lock(reclaimed: Path, lock_path: Path, status):
    if status is None or not stat.S_ISREG(status.st_mode):
        return
    try:
        os.link(reclaimed, lock_path)
        reclaimed.unlink()
    except OSError:
        # A newer owner holds the canonical name. Preserve the displaced lock;
        # never delete a lock whose identity was not the one inspected.
        pass


def _try_reclaim_runtime_lock(lock_path: Path, contender_token: str):
    """Return whether to retry now and whether a contender released the lock."""
    observed_status = _runtime_lock_status(lock_path)
    if observed_status is None:
        return True, True
    if stat.S_ISLNK(observed_status.st_mode) or not (
        stat.S_ISREG(observed_status.st_mode)
        or stat.S_ISDIR(observed_status.st_mode)
    ):
        return False, False
    owner = _read_runtime_lock_owner(lock_path)
    reclaim = owner is not None and not _process_is_alive(owner[0])
    if owner is None:
        reclaim = (
            time.time() - observed_status.st_mtime
            >= _RUNTIME_OWNERLESS_STALE_SECONDS
        )
    if not reclaim:
        return False, False
    reclaimed = lock_path.with_name(
        f"{lock_path.name}.reclaimed-{contender_token}"
    )
    try:
        os.rename(lock_path, reclaimed)
    except FileNotFoundError:
        return True, True
    except OSError:
        return False, False
    moved_status = _runtime_lock_status(reclaimed)
    if not _same_runtime_lock_object(observed_status, moved_status):
        _restore_displaced_runtime_lock(reclaimed, lock_path, moved_status)
        return False, False
    moved_owner = _read_runtime_lock_owner(reclaimed)
    still_reclaimable = (
        time.time() - moved_status.st_mtime >= _RUNTIME_OWNERLESS_STALE_SECONDS
        if moved_owner is None
        else not _process_is_alive(moved_owner[0])
    )
    if not still_reclaimable:
        _restore_displaced_runtime_lock(reclaimed, lock_path, moved_status)
        return False, False
    if stat.S_ISDIR(moved_status.st_mode):
        shutil.rmtree(reclaimed)
    else:
        reclaimed.unlink()
    return True, False


def _write_runtime_lock_owner(owner_fd: int, token: str):
    record = (
        json.dumps(
            {
                "pid": os.getpid(),
                "token": token,
                "lease_expires_ms": int(
                    (time.time() + _RUNTIME_LOCK_LEASE_SECONDS) * 1000
                ),
            },
            separators=(",", ":"),
        )
        + "\n"
    ).encode()
    os.ftruncate(owner_fd, 0)
    os.lseek(owner_fd, 0, os.SEEK_SET)
    offset = 0
    while offset < len(record):
        offset += os.write(owner_fd, record[offset:])
    os.fsync(owner_fd)


def _assert_runtime_lock_owner(lock):
    lock_path, token, owner_fd = lock
    descriptor_status = os.fstat(owner_fd)
    canonical_status = _runtime_lock_status(lock_path)
    owner = _read_runtime_lock_owner(lock_path)
    if (
        not _same_runtime_lock_object(descriptor_status, canonical_status)
        or owner is None
        or owner[:2] != (os.getpid(), token)
    ):
        raise RuntimeError("package-cache publication lock ownership changed")


def _open_validated_runtime_lock(lock_path: Path, token: str, expected_status):
    """Reopen a lock name and prove it still names the expected owner object."""
    try:
        owner_fd = os.open(lock_path, os.O_RDWR)
    except OSError as exc:
        raise RuntimeError(
            "package-cache publication lock ownership changed"
        ) from exc
    try:
        descriptor_status = os.fstat(owner_fd)
        canonical_status = _runtime_lock_status(lock_path)
        owner = _read_runtime_lock_owner(lock_path)
        if (
            not _same_runtime_lock_object(expected_status, descriptor_status)
            or not _same_runtime_lock_object(
                descriptor_status, canonical_status
            )
            or owner is None
            or owner[:2] != (os.getpid(), token)
        ):
            raise RuntimeError(
                "package-cache publication lock ownership changed"
            )
    except BaseException:
        os.close(owner_fd)
        raise
    return owner_fd


def _refresh_runtime_lock(lock):
    _assert_runtime_lock_owner(lock)
    _write_runtime_lock_owner(lock[2], lock[1])
    _assert_runtime_lock_owner(lock)


def _acquire_runtime_lock(directory: Path):
    token = secrets.token_hex(16)
    lock_path = directory / _RUNTIME_LOCK_NAME
    claim_path = lock_path.with_name(f"{lock_path.name}.claim-{token}")
    deadline = time.monotonic() + _RUNTIME_LOCK_WAIT_SECONDS
    contended = False
    while True:
        owner_fd = None
        try:
            owner_fd = os.open(
                claim_path,
                os.O_RDWR | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            try:
                _write_runtime_lock_owner(owner_fd, token)
                if _runtime_lock_claim_observer is not None:
                    _runtime_lock_claim_observer()
                os.link(claim_path, lock_path)
                linked_status = os.fstat(owner_fd)
                if not _same_runtime_lock_object(
                    linked_status, _runtime_lock_status(lock_path)
                ):
                    raise RuntimeError(
                        "package-cache publication lock ownership changed"
                    )
                # Windows does not grant delete sharing to this descriptor.
                # Close it before removing the claim name, then reopen the
                # canonical name and revalidate identity plus owner record.
                os.close(owner_fd)
                owner_fd = None
                claim_path.unlink()
                owner_fd = _open_validated_runtime_lock(
                    lock_path, token, linked_status
                )
            except BaseException:
                if owner_fd is not None:
                    os.close(owner_fd)
                    owner_fd = None
                try:
                    claim_path.unlink()
                except OSError:
                    pass
                raise
            return (lock_path, token, owner_fd), contended
        except FileExistsError:
            retry, released_by_contender = _try_reclaim_runtime_lock(
                lock_path, token
            )
            contended = contended or released_by_contender
            if retry:
                continue
            contended = True
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "timed out waiting for package-cache publication lock"
                )
            time.sleep(_RUNTIME_LOCK_POLL_SECONDS)


def _release_runtime_lock(lock):
    lock_path, token, owner_fd = lock
    try:
        _assert_runtime_lock_owner(lock)
    except Exception:
        os.close(owner_fd)
        raise
    descriptor_status = os.fstat(owner_fd)
    # Path mutation while this descriptor is open fails on Windows. Close it,
    # then reopen the canonical name to prove no object or owner substitution
    # occurred before releasing the name.
    os.close(owner_fd)
    validated_fd = _open_validated_runtime_lock(
        lock_path, token, descriptor_status
    )
    os.close(validated_fd)
    released = lock_path.with_name(f"{lock_path.name}.released-{token}")
    os.rename(lock_path, released)
    released_status = _runtime_lock_status(released)
    try:
        validated_fd = _open_validated_runtime_lock(
            released, token, descriptor_status
        )
    except Exception:
        _restore_displaced_runtime_lock(released, lock_path, released_status)
        raise
    os.close(validated_fd)
    released.unlink()


# A grouped backup is a small crash journal. The retired marker separates a
# partial retirement from partial publication; cleanup-only makes deletion
# retryable after the destination has been accepted or restored.
def _runtime_backup_target_name(name: str, binary_name: str) -> bool:
    return name == binary_name


def _create_runtime_backup_directory(directory: Path) -> Path:
    for _ in range(128):
        backup = directory / f"{_RUNTIME_BACKUP_PREFIX}{secrets.token_hex(16)}"
        try:
            backup.mkdir(mode=0o700)
            return backup
        except FileExistsError:
            continue
    raise RuntimeError("could not reserve a package-cache backup transaction")


def _create_runtime_backup_marker(backup: Path, name: str):
    descriptor = os.open(backup / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _inspect_runtime_backup(directory: Path, entry_name: str, binary_name: str):
    backup_path = directory / entry_name
    try:
        directory_status = backup_path.lstat()
    except OSError as exc:
        raise RuntimeError(
            f"refusing unsafe package-cache backup: {backup_path}"
        ) from exc
    if not stat.S_ISDIR(directory_status.st_mode) or stat.S_ISLNK(
        directory_status.st_mode
    ):
        raise RuntimeError(f"refusing unsafe package-cache backup: {backup_path}")
    backup = {
        "path": backup_path,
        "retired": False,
        "cleanup_only": False,
        "files": {},
    }
    for candidate in sorted(backup_path.iterdir(), key=lambda path: path.name):
        status = candidate.lstat()
        if candidate.name in (
            _RUNTIME_BACKUP_RETIRED_MARKER,
            _RUNTIME_BACKUP_CLEANUP_MARKER,
        ):
            if (
                not stat.S_ISREG(status.st_mode)
                or stat.S_ISLNK(status.st_mode)
                or status.st_nlink != 1
                or status.st_size != 0
            ):
                raise RuntimeError(
                    f"refusing unsafe package-cache backup marker: {candidate}"
                )
            if candidate.name == _RUNTIME_BACKUP_RETIRED_MARKER:
                backup["retired"] = True
            else:
                backup["cleanup_only"] = True
            continue
        if (
            not _runtime_backup_target_name(candidate.name, binary_name)
            or not stat.S_ISREG(status.st_mode)
            or stat.S_ISLNK(status.st_mode)
            or status.st_nlink != 1
        ):
            raise RuntimeError(
                f"refusing unsafe package-cache backup member: {candidate}"
            )
        backup["files"][candidate.name] = (candidate, _file_sha256(candidate))
    return backup


def _list_runtime_backups(directory: Path, binary_name: str):
    names = sorted(
        path.name
        for path in directory.iterdir()
        if _RUNTIME_BACKUP_RE.fullmatch(path.name) is not None
    )
    return [
        _inspect_runtime_backup(directory, name, binary_name) for name in names
    ]


def _current_runtime_targets(directory: Path, binary_name: str):
    targets = {}
    for target in sorted(directory.iterdir(), key=lambda path: path.name):
        if not _runtime_backup_target_name(target.name, binary_name):
            continue
        status = target.lstat()
        if (
            not stat.S_ISREG(status.st_mode)
            or stat.S_ISLNK(status.st_mode)
            or status.st_nlink != 1
        ):
            raise RuntimeError(f"refusing unsafe package-cache target: {target}")
        targets[target.name] = target
    return targets


def _runtime_set_ready_at(directory: Path, binary_name: str, verifier=None) -> bool:
    if _runtime_set_names(directory, binary_name) is None:
        return False
    if verifier is not None:
        try:
            verifier(directory / binary_name)
        except (OSError, RuntimeError):
            return False
    return True


def _cleanup_runtime_backup(backup, lock):
    _assert_runtime_lock_owner(lock)
    if not backup["cleanup_only"]:
        _create_runtime_backup_marker(
            backup["path"], _RUNTIME_BACKUP_CLEANUP_MARKER
        )
        backup["cleanup_only"] = True
    for name in sorted(backup["files"]):
        _assert_runtime_lock_owner(lock)
        member, digest = backup["files"][name]
        if not _path_matches_digest(member, digest):
            raise RuntimeError(
                f"package-cache backup member changed during cleanup: {name}"
            )
        _assert_runtime_lock_owner(lock)
        member.unlink()
    if backup["retired"]:
        _assert_runtime_lock_owner(lock)
        (backup["path"] / _RUNTIME_BACKUP_RETIRED_MARKER).unlink()
    _assert_runtime_lock_owner(lock)
    (backup["path"] / _RUNTIME_BACKUP_CLEANUP_MARKER).unlink()
    backup["path"].rmdir()


def _copy_runtime_backup_file(
    member: Path, target: Path, expected_digest: str, executable: bool, lock
):
    descriptor, staged_name = tempfile.mkstemp(
        dir=str(target.parent), prefix=".cbm-runtime-recovery-"
    )
    staged = Path(staged_name)
    last_refresh = time.monotonic()
    try:
        with member.open("rb") as source, os.fdopen(descriptor, "wb") as output:
            descriptor = None
            while True:
                block = source.read(1024 * 1024)
                if not block:
                    break
                output.write(block)
                if time.monotonic() - last_refresh >= _RUNTIME_LOCK_HEARTBEAT_SECONDS:
                    _refresh_runtime_lock(lock)
                    last_refresh = time.monotonic()
            output.flush()
            os.fsync(output.fileno())
        staged.chmod(0o755 if executable else 0o644)
        if _file_sha256(staged) != expected_digest:
            raise RuntimeError(
                "package-cache backup changed while it was being restored"
            )
        _assert_runtime_lock_owner(lock)
        os.replace(staged, target)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            staged.unlink()
        except FileNotFoundError:
            pass


def _reconcile_runtime_backups(directory: Path, binary_name: str, verifier, lock):
    _refresh_runtime_lock(lock)
    backups = _list_runtime_backups(directory, binary_name)
    for backup in [item for item in backups if item["cleanup_only"]]:
        _cleanup_runtime_backup(backup, lock)
    backups = [item for item in backups if not item["cleanup_only"]]
    if not backups:
        return

    if _runtime_set_ready_at(directory, binary_name, verifier):
        for backup in backups:
            _cleanup_runtime_backup(backup, lock)
        return
    if not backups:
        return

    for backup in [
        item
        for item in backups
        if not item["retired"] and not item["files"]
    ]:
        _cleanup_runtime_backup(backup, lock)
    backups = [
        item for item in backups if item["retired"] or item["files"]
    ]
    if not backups:
        return
    if len(backups) != 1:
        raise RuntimeError(
            "multiple package-cache backup transactions require manual recovery"
        )

    backup = backups[0]
    targets = _current_runtime_targets(directory, binary_name)
    if not backup["retired"]:
        for name, (_, digest) in backup["files"].items():
            target = targets.get(name)
            if target is not None and not _path_matches_digest(target, digest):
                raise RuntimeError(
                    f"package-cache backup conflicts with current target: {name}"
                )
    else:
        binary_target = targets.pop(binary_name, None)
        if binary_target is not None:
            _assert_runtime_lock_owner(lock)
            binary_target.unlink()
        for name in sorted(targets):
            _assert_runtime_lock_owner(lock)
            targets[name].unlink()

    restore_names = sorted(
        name for name in backup["files"] if name != binary_name
    )
    if binary_name in backup["files"]:
        restore_names.append(binary_name)
    for name in restore_names:
        member, digest = backup["files"][name]
        target = directory / name
        if not _path_matches_digest(target, digest):
            _copy_runtime_backup_file(
                member, target, digest, name == binary_name, lock
            )
    for name, (_, digest) in backup["files"].items():
        if not _path_matches_digest(directory / name, digest):
            raise RuntimeError(
                f"package-cache backup restoration failed: {name}"
            )
    _cleanup_runtime_backup(backup, lock)


def _path_matches_digest(path: Path, expected: str) -> bool:
    try:
        return _regular_file(path) and _file_sha256(path) == expected
    except OSError:
        return False


def _publish_runtime_set(
    staged_paths,
    dest: Path,
    binary_name: str,
    replace_file=None,
    verifier=None,
):
    """Publish sidecars first, retaining exact-byte backups for recovery."""
    runtime_names = _staged_runtime_names(staged_paths, binary_name)
    if replace_file is None:
        replace_file = os.replace
    _require_safe_runtime_directory(dest.parent)
    initial_fingerprint = _runtime_set_fingerprint(dest.parent, binary_name)
    lock, contended = _acquire_runtime_lock(dest.parent)
    operation_error = None
    try:
        _refresh_runtime_lock(lock)
        _reconcile_runtime_backups(dest.parent, binary_name, verifier, lock)
        # Preserve a complete set that appeared or changed while this publisher
        # waited. An unchanged pre-existing set may still be repaired/replaced,
        # with its bytes retained below for rollback.
        locked_fingerprint = _runtime_set_fingerprint(dest.parent, binary_name)
        if (
            locked_fingerprint is not None
            and (contended or locked_fingerprint != initial_fingerprint)
        ):
            return

        backup_directory = None
        try:
            backup_directory = _create_runtime_backup_directory(dest.parent)
            retire_names = (binary_name,)
            retired = set()
            # Retire the executable first: until the final rename, readiness is false.
            for name in retire_names:
                _assert_runtime_lock_owner(lock)
                if name in retired:
                    continue
                retired.add(name)
                target = dest.parent / name
                try:
                    status = target.lstat()
                except FileNotFoundError:
                    continue
                if (
                    not stat.S_ISREG(status.st_mode)
                    or stat.S_ISLNK(status.st_mode)
                    or status.st_nlink != 1
                ):
                    raise RuntimeError(
                        f"refusing unsafe package-cache target: {target}"
                    )
                replace_file(target, backup_directory / name)
            _assert_runtime_lock_owner(lock)
            _create_runtime_backup_marker(
                backup_directory, _RUNTIME_BACKUP_RETIRED_MARKER
            )

            for name in runtime_names:
                _assert_runtime_lock_owner(lock)
                target = dest.parent / name
                try:
                    replace_file(staged_paths[name], target)
                    staged_paths.pop(name)
                except OSError as publish_error:
                    if not _files_equal_sha256(staged_paths[name], target):
                        raise publish_error
            if _runtime_set_names(dest.parent, binary_name) is None:
                raise RuntimeError("published package cache is incomplete")
            if verifier is not None:
                verifier(dest.parent / binary_name)
            _assert_runtime_lock_owner(lock)
        except Exception as publication_error:
            _assert_runtime_lock_owner(lock)
            # Preserve a complete non-cooperating winner; never delete its bytes.
            preserve_winner = _runtime_set_ready_at(dest.parent, binary_name, verifier)
            if backup_directory is not None:
                try:
                    _reconcile_runtime_backups(dest.parent, binary_name, verifier, lock)
                except Exception as recovery_error:
                    raise RuntimeError(
                        f"{publication_error}; package-cache recovery failed: "
                        f"{recovery_error}"
                    ) from recovery_error
            if preserve_winner:
                return
            raise
        else:
            backup = _inspect_runtime_backup(
                dest.parent, backup_directory.name, binary_name
            )
            _cleanup_runtime_backup(backup, lock)
    except Exception as exc:
        operation_error = exc
        raise
    finally:
        try:
            _release_runtime_lock(lock)
        except Exception:
            if operation_error is None:
                raise


def _download(version: str) -> Path:
    os_name = _os_name()
    arch = _arch()
    ext = "zip" if os_name == "windows" else "tar.gz"
    # Linux ships a fully-static "-portable" build; the standard linux binary
    # dynamically links glibc 2.38+ and fails on older distros. macOS/Windows
    # have no such variant. Keep in sync with install.sh / install.js / cli.c.
    portable = "-portable" if os_name == "linux" else ""
    archive = f"codebase-memory-mcp-{os_name}-{arch}{portable}.{ext}"
    url = f"https://github.com/{REPO}/releases/download/v{version}/{archive}"
    _validate_url_scheme(url)

    dest = _bin_path(version)
    dest.parent.mkdir(parents=True, exist_ok=True)

    print(
        f"codebase-memory-mcp: downloading v{version} for {os_name}/{arch}...",
        file=sys.stderr,
    )

    with tempfile.TemporaryDirectory() as tmp:
        tmp_archive = os.path.join(tmp, f"cbm.{ext}")
        try:
            _download_https(url, tmp_archive)
        except (OSError, RuntimeError, urllib.error.URLError) as e:
            sys.exit(
                f"codebase-memory-mcp: download failed ({e})\n"
                f"URL: {url}\n"
                f"See https://github.com/{REPO}/releases for available versions."
            )

        _verify_checksum(tmp_archive, archive, version)

        bin_name = (
            _WINDOWS_BINARY_NAME if os_name == "windows" else "codebase-memory-mcp"
        )
        extraction_names = (bin_name,)
        archive_names = (
            _WINDOWS_ARCHIVE_NAMES
            if os_name == "windows"
            else _UNIX_ARCHIVE_NAMES
        )
        if ext == "tar.gz":
            import tarfile
            with tarfile.open(tmp_archive) as tf:
                runtime_names = _safe_extract_tar(
                    tf,
                    tmp,
                    archive_names,
                    extraction_names,
                )
        else:
            import zipfile
            with zipfile.ZipFile(tmp_archive) as zf:
                runtime_names = _safe_extract_zip(
                    zf,
                    tmp,
                    archive_names,
                    extraction_names,
                )

        extracted_paths = {}
        for name in runtime_names:
            extracted_path = Path(tmp) / name
            if not _regular_file(extracted_path):
                sys.exit(
                    f"codebase-memory-mcp: required runtime member not found after "
                    f"extraction: {name}"
                )
            if name == bin_name:
                current = extracted_path.stat().st_mode
                extracted_path.chmod(
                    current | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
                )
            extracted_paths[name] = extracted_path
        if _runtime_set_names(Path(tmp), bin_name) is None:
            sys.exit(
                "codebase-memory-mcp: extracted runtime set failed content "
                "verification"
            )
        try:
            _verify_candidate(extracted_paths[bin_name])
        except (OSError, RuntimeError) as exc:
            sys.exit(f"codebase-memory-mcp: {exc}")

        staged_paths = {}
        try:
            for name in runtime_names:
                staged_suffix = (
                    ".tmp.exe"
                    if sys.platform == "win32" and name == bin_name
                    else ".tmp"
                )
                with tempfile.NamedTemporaryFile(
                    dir=str(dest.parent),
                    prefix=f".{name}.",
                    suffix=staged_suffix,
                    delete=False,
                ) as staged:
                    staged_path = Path(staged.name)
                shutil.copy2(extracted_paths[name], staged_path)
                if name == bin_name:
                    staged_mode = staged_path.stat().st_mode
                    staged_path.chmod(
                        staged_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
                    )
                staged_paths[name] = staged_path

            # A crash during publication leaves an incomplete cache, which
            # readiness rejects and the next invocation repairs.
            _publish_runtime_set(
                staged_paths,
                dest,
                bin_name,
                verifier=_verify_candidate,
            )
            _verify_candidate(dest)
        except (OSError, RuntimeError) as exc:
            sys.exit(f"codebase-memory-mcp: {exc}")
        finally:
            for staged_path in staged_paths.values():
                try:
                    staged_path.unlink()
                except OSError:
                    pass

    return dest


def main() -> None:
    version = _version()
    bin_path = _bin_path(version)
    mutation = _runtime_mutation_action(sys.argv[1:])
    if mutation == "update":
        print(
            "This PyPI copy is maintained by pip. Update it with "
            '"python -m pip install --upgrade codebase-memory-mcp". '
            "For a managed standalone installation, run "
            '"codebase-memory-mcp install --yes".',
            file=sys.stderr,
        )
        raise SystemExit(2)

    cache_ready = _runtime_set_ready_locked(version)
    if not cache_ready:
        bin_path = _download(version)

    execution_path = _execution_path(bin_path, sys.platform)

    # args is a list (not a shell string), so exec/subprocess treat each
    # element as a discrete argv entry — no shell interpretation, no
    # injection vector. sys.argv forwarding is the whole point of this
    # shim, so tainted-input suppression is intentional.
    forwarded = _native_args(sys.argv[1:])
    args = [str(execution_path)] + forwarded

    if mutation in ("install", "uninstall"):
        result = _run_cache_sensitive_mutation(version, execution_path, forwarded)
        if sys.platform == "win32" and result != 0 and mutation == "uninstall":
            print(
                'This PyPI Windows copy is portable. Use '
                '"python -m pip uninstall codebase-memory-mcp" for package maintenance. '
                "To create or repair a managed standalone installation, run "
                '"codebase-memory-mcp install --yes".',
                file=sys.stderr,
            )
        raise SystemExit(result)

    if sys.platform != "win32":
        os.execv(str(execution_path), args)  # noqa: S606 — list form, no shell
    else:
        result = subprocess.run(args)  # noqa: S603 — list form, no shell=True
        sys.exit(result.returncode)


def _runtime_mutation_action(args):
    for argument in args:
        if argument in ("--help", "-h", "--version"):
            return None
        if argument.startswith("-"):
            continue
        if argument in ("install", "update", "uninstall"):
            return argument
        if argument in ("cli", "hook-augment", "config"):
            return None
        return None
    return None


def _default_managed_install_dir() -> Path:
    if sys.platform != "win32":
        return Path.home() / ".local" / "bin"
    local_app_data = Path(
        os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local")
    )
    return local_app_data / "Programs" / "codebase-memory-mcp"


def _native_args(args):
    result = list(args)
    if _runtime_mutation_action(result) == "uninstall" and not any(
        argument == "--dir" or argument.startswith("--dir=")
        for argument in result
    ):
        result.extend(("--dir", str(_default_managed_install_dir())))
    return result


def _run_cache_sensitive_mutation(version: str, executable: Path, args) -> int:
    directory = executable.parent
    _require_safe_runtime_directory(directory)
    lock, _ = _acquire_runtime_lock(directory)
    stop = threading.Event()
    heartbeat_errors = []

    def heartbeat():
        while not stop.wait(_RUNTIME_LOCK_HEARTBEAT_SECONDS):
            try:
                _refresh_runtime_lock(lock)
            except Exception as exc:  # surfaced on the launcher thread
                heartbeat_errors.append(exc)
                return

    try:
        _refresh_runtime_lock(lock)
        if not _runtime_set_ready(version, _verify_candidate):
            raise RuntimeError(
                "cached runtime assets changed before mutation launch"
            )
        thread = threading.Thread(
            name="cbm-cache-lock-heartbeat", target=heartbeat, daemon=True
        )
        thread.start()
        process = subprocess.Popen(  # noqa: S603 — argv list, no shell
            [str(executable), *args]
        )
        while process.poll() is None:
            if heartbeat_errors:
                process.terminate()
                process.wait()
                raise RuntimeError(
                    f"package-cache lock lost during mutation: "
                    f"{heartbeat_errors[0]}"
                )
            time.sleep(0.05)
        return process.returncode
    finally:
        stop.set()
        if "thread" in locals():
            thread.join(timeout=1)
        _release_runtime_lock(lock)
