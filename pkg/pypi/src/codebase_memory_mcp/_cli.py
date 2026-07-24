"""Downloads codebase-memory-mcp on first run, then runs its native entry point."""

import hashlib
import os
import platform
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

REPO = "DeusData/codebase-memory-mcp"
_WINDOWS_LAUNCHER_NAME = "codebase-memory-mcp.exe"
_WINDOWS_PAYLOAD_NAME = "codebase-memory-mcp.payload.exe"
_WINDOWS_ARCHIVE_NAMES = (
    _WINDOWS_LAUNCHER_NAME,
    _WINDOWS_PAYLOAD_NAME,
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
            left.is_file()
            and right.is_file()
            and left.stat().st_size == right.stat().st_size
            and _file_sha256(left) == _file_sha256(right)
        )
    except OSError:
        return False


def _safe_extract_tar(tf, dest: str) -> None:
    """Extract a tarfile to dest, rejecting path-traversal entries.

    Uses the tarfile data filter on Python >=3.12 (PEP 706), falls back to
    manual per-member path validation on older Pythons. Mitigates the
    classic tar-slip / Zip Slip vulnerability (CWE-22).
    """
    if hasattr(tf, "extraction_filter") or sys.version_info >= (3, 12):
        tf.extractall(dest, filter="data")
        return

    dest_abs = os.path.abspath(dest)
    for member in tf.getmembers():
        if member.issym() or member.islnk():
            sys.exit(
                f"codebase-memory-mcp: refusing unsafe tar entry "
                f"(link: {member.name!r})"
            )
        member_abs = os.path.abspath(os.path.join(dest_abs, member.name))
        if not (member_abs == dest_abs or member_abs.startswith(dest_abs + os.sep)):
            sys.exit(
                f"codebase-memory-mcp: refusing unsafe tar entry "
                f"(escapes dest: {member.name!r})"
            )
    tf.extractall(dest)


def _safe_extract_zip(zf, dest: str, archive_names=(), extract_names=()) -> None:
    """Extract a zipfile after validating its Windows-style namespace.

    Windows treats paths case-insensitively. Rejecting duplicate/case-conflicting
    members before extraction prevents archive order from selecting the binary
    that a portable package-manager shim eventually executes.
    """
    dest_abs = os.path.abspath(dest)
    seen = set()
    required = tuple(archive_names)
    required_set = set(required)
    found = set()
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
        folded = segments_name.casefold()
        if folded in seen:
            sys.exit(
                f"codebase-memory-mcp: refusing unsafe zip entry "
                f"(duplicate or case conflict: {raw_name!r})"
            )
        seen.add(folded)
        if name not in required_set or info.is_dir():
            sys.exit(
                f"codebase-memory-mcp: archive must contain only the exact "
                f"root files: {', '.join(required)}"
            )
        found.add(name)
        member_abs = os.path.abspath(os.path.join(dest_abs, *segments))
        if not (member_abs == dest_abs or member_abs.startswith(dest_abs + os.sep)):
            sys.exit(
                f"codebase-memory-mcp: refusing unsafe zip entry "
                f"(escapes dest: {raw_name!r})"
            )
    missing = required_set - found
    if missing or len(seen) != len(required):
        sys.exit(
            f"codebase-memory-mcp: archive must contain exactly one of each "
            f"required root file: {', '.join(required)}"
        )

    # Extract only the two validated executable files. This avoids relying on
    # platform-specific zip path rewriting and always creates regular files.
    for name in extract_names:
        target = os.path.join(dest_abs, name)
        with zf.open(name) as source, open(target, "xb") as output:
            shutil.copyfileobj(source, output)


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
        return "0.8.1"


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


def _bin_path(version: str) -> Path:
    # The payload remains the immutable Windows cache pair's readiness signal.
    # _execution_path selects the adjacent launcher for process execution.
    name = (
        _WINDOWS_PAYLOAD_NAME
        if sys.platform == "win32"
        else "codebase-memory-mcp"
    )
    return _cache_dir() / version / name


def _windows_pair_paths(version: str):
    version_dir = _cache_dir() / version
    return (
        version_dir / _WINDOWS_LAUNCHER_NAME,
        version_dir / _WINDOWS_PAYLOAD_NAME,
    )


def _execution_path(payload: Path, target_platform: str) -> Path:
    """Select the permanent Windows launcher without changing other platforms."""
    if target_platform == "win32":
        return payload.with_name(_WINDOWS_LAUNCHER_NAME)
    return payload


def _windows_pair_ready(version: str) -> bool:
    launcher, payload = _windows_pair_paths(version)
    if not launcher.is_file() or not payload.is_file():
        return False
    try:
        _verify_candidate(launcher)
    except RuntimeError:
        return False
    return True


def _download(version: str) -> Path:
    os_name = _os_name()
    arch = _arch()
    ext = "zip" if os_name == "windows" else "tar.gz"
    # Linux ships a fully-static "-portable" build; the standard linux binary
    # dynamically links glibc 2.38+ and fails on older distros. macOS/Windows
    # have no such variant. Keep in sync with install.sh / install.js / cli.c.
    variant = "-portable" if os_name == "linux" else ""
    # Opt into the UI build (embedded graph visualization) with CBM_VARIANT=ui.
    # Default is the standard (headless) build. Mirrors install.sh --ui.
    ui = "ui-" if os.environ.get("CBM_VARIANT", "").lower() == "ui" else ""
    archive = f"codebase-memory-mcp-{ui}{os_name}-{arch}{variant}.{ext}"
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

        if os_name == "windows":
            bin_name = _WINDOWS_PAYLOAD_NAME
            extraction_names = (
                _WINDOWS_LAUNCHER_NAME,
                _WINDOWS_PAYLOAD_NAME,
            )
        else:
            bin_name = "codebase-memory-mcp"
            extraction_names = (bin_name,)
        cache_names = extraction_names
        publish_names = (
            (_WINDOWS_LAUNCHER_NAME, _WINDOWS_PAYLOAD_NAME)
            if os_name == "windows"
            else cache_names
        )
        if ext == "tar.gz":
            import tarfile
            with tarfile.open(tmp_archive) as tf:
                _safe_extract_tar(tf, tmp)
        else:
            import zipfile
            with zipfile.ZipFile(tmp_archive) as zf:
                _safe_extract_zip(
                    zf,
                    tmp,
                    _WINDOWS_ARCHIVE_NAMES,
                    extraction_names,
                )

        extracted_paths = {}
        for name in extraction_names:
            extracted_path = Path(tmp) / name
            if not extracted_path.is_file():
                sys.exit(
                    f"codebase-memory-mcp: required binary not found after "
                    f"extraction: {name}"
                )
            current = extracted_path.stat().st_mode
            extracted_path.chmod(
                current | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
            )
            extracted_paths[name] = extracted_path
        try:
            if os_name == "windows":
                # The portable launcher must resolve this adjacent payload.
                _verify_candidate(extracted_paths[_WINDOWS_LAUNCHER_NAME])
            else:
                _verify_candidate(extracted_paths[bin_name])
        except RuntimeError as exc:
            sys.exit(f"codebase-memory-mcp: {exc}")

        staged_paths = {}
        try:
            for name in cache_names:
                staged_suffix = ".tmp.exe" if sys.platform == "win32" else ".tmp"
                with tempfile.NamedTemporaryFile(
                    dir=str(dest.parent),
                    prefix=f".{name}.",
                    suffix=staged_suffix,
                    delete=False,
                ) as staged:
                    staged_path = Path(staged.name)
                shutil.copy2(extracted_paths[name], staged_path)
                staged_mode = staged_path.stat().st_mode
                staged_path.chmod(
                    staged_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
                )
                staged_paths[name] = staged_path

            if os_name != "windows":
                _verify_candidate(staged_paths[bin_name])
            for name in publish_names:
                target = dest.parent / name
                try:
                    os.replace(staged_paths[name], target)
                    staged_paths.pop(name)
                except OSError as publish_error:
                    # Never unlink a destination here: it may belong to a
                    # concurrent contender. A collision remains eligible only
                    # when its bytes match this authenticated stage; pair
                    # execution is checked after payload is published last.
                    if not _files_equal_sha256(staged_paths[name], target):
                        raise publish_error
            if os_name == "windows":
                _verify_candidate(dest.parent / _WINDOWS_LAUNCHER_NAME)
            else:
                _verify_candidate(dest)
        except RuntimeError as exc:
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

    cache_ready = (
        _windows_pair_ready(version)
        if sys.platform == "win32"
        else bin_path.is_file()
    )
    if not cache_ready:
        bin_path = _download(version)

    execution_path = _execution_path(bin_path, sys.platform)

    # args is a list (not a shell string), so exec/subprocess treat each
    # element as a discrete argv entry — no shell interpretation, no
    # injection vector. sys.argv forwarding is the whole point of this
    # shim, so tainted-input suppression is intentional.
    args = [str(execution_path)] + sys.argv[1:]

    if sys.platform != "win32":
        os.execv(str(execution_path), args)  # noqa: S606 — list form, no shell
    else:
        result = subprocess.run(args)  # noqa: S603 — list form, no shell=True
        mutation = _portable_mutation_action(sys.argv[1:])
        if result.returncode != 0 and mutation is not None:
            package_command = (
                "python -m pip install --upgrade codebase-memory-mcp"
                if mutation == "update"
                else "python -m pip uninstall codebase-memory-mcp"
            )
            print(
                f'This PyPI Windows copy is portable. Use "{package_command}" '
                f'for package maintenance, or run "codebase-memory-mcp '
                f'install --yes" once to create a managed launcher with '
                f'coordinated self-update/uninstall.',
                file=sys.stderr,
            )
        sys.exit(result.returncode)


def _portable_mutation_action(args):
    for argument in args:
        if argument in (
            "cli",
            "hook-augment",
            "config",
            "install",
            "--help",
            "-h",
            "--version",
        ):
            return None
        if argument in ("update", "uninstall"):
            return argument
    return None
