"""Downloads the codebase-memory-mcp binary on first run, then exec's it."""

import hashlib
import os
import sys
import platform
import stat
import shutil
import tempfile
import urllib.request
import urllib.error
import urllib.parse
from pathlib import Path

REPO = "DeusData/codebase-memory-mcp"

# Security: only permit https fetches. urllib's default handlers accept
# file://, ftp://, and custom schemes — a redirect or tainted URL source
# could otherwise turn a download into an arbitrary-local-file read.
_ALLOWED_SCHEMES = frozenset({"https"})


def _validate_url_scheme(url: str) -> None:
    """Reject non-https URLs before any network fetch."""
    scheme = urllib.parse.urlparse(url).scheme
    if scheme not in _ALLOWED_SCHEMES:
        sys.exit(
            f"codebase-memory-mcp: refusing to fetch non-https URL "
            f"(scheme={scheme!r}): {url}"
        )


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


def _safe_extract_zip(zf, dest: str) -> None:
    """Extract a zipfile to dest, rejecting path-traversal entries."""
    dest_abs = os.path.abspath(dest)
    for name in zf.namelist():
        member_abs = os.path.abspath(os.path.join(dest_abs, name))
        if not (member_abs == dest_abs or member_abs.startswith(dest_abs + os.sep)):
            sys.exit(
                f"codebase-memory-mcp: refusing unsafe zip entry "
                f"(escapes dest: {name!r})"
            )
    zf.extractall(dest)


def _verify_checksum(archive_path: str, archive_name: str, version: str) -> None:
    """Verify SHA256 checksum against checksums.txt from the release."""
    url = f"https://github.com/{REPO}/releases/download/v{version}/checksums.txt"
    try:
        _validate_url_scheme(url)
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tmp:
            tmp_path = tmp.name
        urllib.request.urlretrieve(url, tmp_path)  # noqa: S310 — scheme validated above
        with open(tmp_path) as f:
            for line in f:
                if archive_name in line:
                    expected = line.split()[0]
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
                    break
    except SystemExit:
        raise
    except Exception:
        pass  # Non-fatal: checksum unavailable
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


def _version() -> str:
    try:
        from importlib.metadata import version
        return version("codebase-memory-mcp")
    except Exception:
        return "0.6.0"


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
    name = "codebase-memory-mcp.exe" if sys.platform == "win32" else "codebase-memory-mcp"
    return _cache_dir() / version / name


def _download(version: str) -> Path:
    os_name = _os_name()
    arch = _arch()
    ext = "zip" if os_name == "windows" else "tar.gz"
    archive = f"codebase-memory-mcp-{os_name}-{arch}.{ext}"
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
            urllib.request.urlretrieve(url, tmp_archive)  # noqa: S310 — scheme validated above
        except urllib.error.HTTPError as e:
            sys.exit(
                f"codebase-memory-mcp: download failed ({e})\n"
                f"URL: {url}\n"
                f"See https://github.com/{REPO}/releases for available versions."
            )

        _verify_checksum(tmp_archive, archive, version)

        if ext == "tar.gz":
            import tarfile
            with tarfile.open(tmp_archive) as tf:
                _safe_extract_tar(tf, tmp)
        else:
            import zipfile
            with zipfile.ZipFile(tmp_archive) as zf:
                _safe_extract_zip(zf, tmp)

        bin_name = "codebase-memory-mcp.exe" if os_name == "windows" else "codebase-memory-mcp"
        extracted = os.path.join(tmp, bin_name)
        if not os.path.exists(extracted):
            sys.exit("codebase-memory-mcp: binary not found after extraction")

        shutil.copy2(extracted, dest)
        current = dest.stat().st_mode
        dest.chmod(current | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    return dest


def main() -> None:
    version = _version()
    bin_path = _bin_path(version)

    if not bin_path.exists():
        bin_path = _download(version)

    # args is a list (not a shell string), so exec/subprocess treat each
    # element as a discrete argv entry — no shell interpretation, no
    # injection vector. sys.argv forwarding is the whole point of this
    # shim, so tainted-input suppression is intentional.
    args = [str(bin_path)] + sys.argv[1:]

    if sys.platform != "win32":
        os.execv(str(bin_path), args)  # noqa: S606 — list form, no shell
    else:
        import subprocess
        result = subprocess.run(args)  # noqa: S603 — list form, no shell=True
        sys.exit(result.returncode)
