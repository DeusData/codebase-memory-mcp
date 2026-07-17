#!/usr/bin/env python3
"""Update or verify every active release-version and checksum surface."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SEMVER = re.compile(r"^\d+\.\d+\.\d+$")
CHECKSUM_FILE = ROOT / "pkg" / "release-checksums.json"
WINGET_ROOT = ROOT / "pkg" / "winget" / "manifests" / "d" / "DeusData" / "CodebaseMemoryMcp"
WINGET_MANIFEST_TYPES = {
    "DeusData.CodebaseMemoryMcp.installer.yaml": "installer",
    "DeusData.CodebaseMemoryMcp.locale.en-US.yaml": "defaultLocale",
    "DeusData.CodebaseMemoryMcp.yaml": "version",
}
MUTATED_FILES = (
    "VERSION",
    "flake.nix",
    "server.json",
    "pkg/npm/package.json",
    "pkg/pypi/pyproject.toml",
    "pkg/pypi/src/codebase_memory_mcp/_cli.py",
    "pkg/go/cmd/codebase-memory-mcp/main.go",
    "pkg/homebrew/Formula/codebase-memory-mcp.rb",
    "pkg/scoop/codebase-memory-mcp.json",
    "pkg/aur/PKGBUILD",
    "pkg/aur/.SRCINFO",
    "pkg/chocolatey/codebase-memory-mcp.nuspec",
    "pkg/chocolatey/tools/chocolateyInstall.ps1",
    "docs/index.html",
    ".github/ISSUE_TEMPLATE/bug_report.yml",
    "pkg/release-checksums.json",
)


def read(path: str | Path) -> str:
    return (ROOT / path).read_text(encoding="utf-8") if isinstance(path, str) else path.read_text(encoding="utf-8")


def write(path: str | Path, content: str) -> None:
    target = ROOT / path if isinstance(path, str) else path
    with target.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(content)


def replace_checked(path: str | Path, pattern: str, replacement: str, expected: int = 1) -> None:
    content = read(path)
    updated, count = re.subn(pattern, replacement, content, flags=re.MULTILINE)
    if count != expected:
        raise RuntimeError(f"{path}: replacement matched {count} times, expected {expected}")
    write(path, updated)


def parse_release_checksums(path: Path) -> dict[str, str]:
    assets: dict[str, str] = {}
    duplicates: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})\s+(.+)", line.strip())
        if match:
            if match.group(2) in assets:
                duplicates.add(match.group(2))
            assets[match.group(2)] = match.group(1)
    if duplicates:
        raise RuntimeError("checksum file contains duplicate assets: " + ", ".join(sorted(duplicates)))
    required = {
        "codebase-memory-mcp-darwin-amd64.tar.gz",
        "codebase-memory-mcp-darwin-arm64.tar.gz",
        "codebase-memory-mcp-linux-amd64.tar.gz",
        "codebase-memory-mcp-linux-arm64.tar.gz",
        "codebase-memory-mcp-windows-amd64.zip",
    }
    missing = sorted(required - assets.keys())
    if missing:
        raise RuntimeError(f"checksum file is missing required assets: {', '.join(missing)}")
    return dict(sorted(assets.items()))


def winget_shape_errors(winget_dir: Path) -> list[str]:
    errors: list[str] = []
    actual = {path.name for path in winget_dir.glob("*.yaml")}
    expected = set(WINGET_MANIFEST_TYPES)
    if actual != expected:
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if unexpected:
            details.append("unexpected " + ", ".join(unexpected))
        errors.append("Winget manifest set is invalid: " + "; ".join(details))
    for filename, expected_type in WINGET_MANIFEST_TYPES.items():
        manifest = winget_dir / filename
        if not manifest.is_file():
            continue
        content = manifest.read_text(encoding="utf-8")
        identifiers = re.findall(r"^PackageIdentifier:\s*(\S+)\s*$", content, flags=re.MULTILINE)
        manifest_types = re.findall(r"^ManifestType:\s*(\S+)\s*$", content, flags=re.MULTILINE)
        if identifiers != ["DeusData.CodebaseMemoryMcp"]:
            errors.append(f"{filename}: expected one CodebaseMemoryMcp PackageIdentifier")
        if manifest_types != [expected_type]:
            errors.append(f"{filename}: expected ManifestType {expected_type}")
    return errors


def workflow_step_blocks(workflow: str) -> dict[str, list[list[str]]]:
    """Return uncommented GitHub Actions step bodies grouped by step name."""
    lines = workflow.splitlines()
    blocks: dict[str, list[list[str]]] = {}
    for index, line in enumerate(lines):
        match = re.match(r'^(\s*)-\s+name:\s*["\']?(.+?)["\']?\s*$', line)
        if not match:
            continue
        indent = len(match.group(1))
        name = match.group(2).strip().strip("\"'")
        end = index + 1
        while end < len(lines):
            candidate = lines[end]
            leading = len(candidate) - len(candidate.lstrip())
            if candidate.strip() and leading < indent:
                break
            if re.match(rf"^\s{{{indent}}}-\s+", candidate):
                break
            end += 1
        active = [
            candidate.strip()
            for candidate in lines[index:end]
            if candidate.strip() and not candidate.lstrip().startswith("#")
        ]
        blocks.setdefault(name, []).append(active)
    return blocks


def update_versions(version: str, checksums_path: Path) -> None:
    assets = parse_release_checksums(checksums_path)
    current = (ROOT / "VERSION").read_text(encoding="utf-8").strip()

    replace_checked("flake.nix", r'^(\s*version = ")[0-9]+\.[0-9]+\.[0-9]+(";)$', rf'\g<1>{version}\g<2>')
    replace_checked("server.json", r'("version": ")[0-9]+\.[0-9]+\.[0-9]+(")', rf'\g<1>{version}\g<2>', 3)
    replace_checked("pkg/npm/package.json", r'("version": ")[0-9]+\.[0-9]+\.[0-9]+(")', rf'\g<1>{version}\g<2>')
    replace_checked("pkg/pypi/pyproject.toml", r'^(version = ")[0-9]+\.[0-9]+\.[0-9]+(")$', rf'\g<1>{version}\g<2>')
    replace_checked("pkg/pypi/src/codebase_memory_mcp/_cli.py", r'^(\s*return ")[0-9]+\.[0-9]+\.[0-9]+(")$', rf'\g<1>{version}\g<2>')
    replace_checked("pkg/go/cmd/codebase-memory-mcp/main.go", r'^(\s*version = ")[0-9]+\.[0-9]+\.[0-9]+(")$', rf'\g<1>{version}\g<2>')
    replace_checked("pkg/homebrew/Formula/codebase-memory-mcp.rb", r'^(\s*version ")[0-9]+\.[0-9]+\.[0-9]+(")$', rf'\g<1>{version}\g<2>')
    replace_checked("pkg/scoop/codebase-memory-mcp.json", r'("version": ")[0-9]+\.[0-9]+\.[0-9]+(")', rf'\g<1>{version}\g<2>')
    replace_checked("pkg/scoop/codebase-memory-mcp.json", r'(releases/download/v)[0-9]+\.[0-9]+\.[0-9]+(/codebase-memory)', rf'\g<1>{version}\g<2>')
    replace_checked("pkg/aur/PKGBUILD", r'^(pkgver=)[0-9]+\.[0-9]+\.[0-9]+$', rf'\g<1>{version}')
    replace_checked("pkg/aur/.SRCINFO", r'^(\s*pkgver = )[0-9]+\.[0-9]+\.[0-9]+$', rf'\g<1>{version}')
    replace_checked("pkg/aur/.SRCINFO", r'(codebase-memory-mcp-)[0-9]+\.[0-9]+\.[0-9]+(-linux)', rf'\g<1>{version}\g<2>', 2)
    replace_checked("pkg/aur/.SRCINFO", r'(releases/download/v)[0-9]+\.[0-9]+\.[0-9]+(/codebase-memory)', rf'\g<1>{version}\g<2>', 2)
    replace_checked("pkg/chocolatey/codebase-memory-mcp.nuspec", r'(<version>)[0-9]+\.[0-9]+\.[0-9]+(</version>)', rf'\g<1>{version}\g<2>')
    replace_checked("pkg/chocolatey/codebase-memory-mcp.nuspec", r'(releases/tag/v)[0-9]+\.[0-9]+\.[0-9]+(</releaseNotes>)', rf'\g<1>{version}\g<2>')
    replace_checked("pkg/chocolatey/tools/chocolateyInstall.ps1", r"^(\$version\s*= ')[0-9]+\.[0-9]+\.[0-9]+(')$", rf'\g<1>{version}\g<2>')
    replace_checked("docs/index.html", r'("softwareVersion": ")[0-9]+\.[0-9]+\.[0-9]+(")', rf'\g<1>{version}\g<2>')
    replace_checked(".github/ISSUE_TEMPLATE/bug_report.yml", r'^(\s*placeholder: codebase-memory-mcp )[0-9]+\.[0-9]+\.[0-9]+$', rf'\g<1>{version}')

    candidates = [p for p in WINGET_ROOT.iterdir() if p.is_dir() and SEMVER.fullmatch(p.name)]
    if len(candidates) != 1:
        raise RuntimeError(f"expected one Winget version directory, found {len(candidates)}")
    winget_dir = candidates[0]
    shape_errors = winget_shape_errors(winget_dir)
    if shape_errors:
        raise RuntimeError("; ".join(shape_errors))
    for manifest in winget_dir.glob("*.yaml"):
        content = manifest.read_text(encoding="utf-8")
        content = re.sub(r'^(PackageVersion: )[0-9]+\.[0-9]+\.[0-9]+$', rf'\g<1>{version}', content, flags=re.MULTILINE)
        content = re.sub(r'(releases/(?:download|tag)/v)[0-9]+\.[0-9]+\.[0-9]+', rf'\g<1>{version}', content)
        with manifest.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
    if winget_dir.name != version:
        target = winget_dir.with_name(version)
        if target.exists():
            raise RuntimeError(f"Winget target already exists: {target}")
        winget_dir.rename(target)
        winget_dir = target

    checksum_data = {
        "version": version,
        "source": f"https://github.com/DeusData/codebase-memory-mcp/releases/download/v{version}/checksums.txt",
        "assets": assets,
    }
    write(CHECKSUM_FILE, json.dumps(checksum_data, indent=2) + "\n")

    homebrew_path = "pkg/homebrew/Formula/codebase-memory-mcp.rb"
    homebrew = read(homebrew_path)
    for asset in (
        "codebase-memory-mcp-darwin-arm64.tar.gz",
        "codebase-memory-mcp-darwin-amd64.tar.gz",
        "codebase-memory-mcp-linux-arm64.tar.gz",
        "codebase-memory-mcp-linux-amd64.tar.gz",
    ):
        pattern = rf'(url "[^"]+/{re.escape(asset)}"\n\s*sha256 ")[0-9a-f]{{64}}("$)'
        homebrew, count = re.subn(pattern, rf"\g<1>{assets[asset]}\g<2>", homebrew, flags=re.MULTILINE)
        if count != 1:
            raise RuntimeError(f"{homebrew_path}: expected one checksum row for {asset}, found {count}")
    write(homebrew_path, homebrew)

    win = assets["codebase-memory-mcp-windows-amd64.zip"]
    linux_x64 = assets["codebase-memory-mcp-linux-amd64.tar.gz"]
    linux_arm = assets["codebase-memory-mcp-linux-arm64.tar.gz"]
    replace_checked("pkg/scoop/codebase-memory-mcp.json", r'("hash": ")[0-9a-f]{64}(")', rf'\g<1>{win}\g<2>')
    replace_checked("pkg/chocolatey/tools/chocolateyInstall.ps1", r"^(\$checksum64\s*= ')[0-9a-f]{64}(')$", rf'\g<1>{win}\g<2>')
    replace_checked("pkg/aur/PKGBUILD", r"^(sha256sums_x86_64=\(')[0-9a-f]{64}('\))$", rf'\g<1>{linux_x64}\g<2>')
    replace_checked("pkg/aur/PKGBUILD", r"^(sha256sums_aarch64=\(')[0-9a-f]{64}('\))$", rf'\g<1>{linux_arm}\g<2>')
    replace_checked("pkg/aur/.SRCINFO", r'^(\s*sha256sums_x86_64 = )[0-9a-f]{64}$', rf'\g<1>{linux_x64}')
    replace_checked("pkg/aur/.SRCINFO", r'^(\s*sha256sums_aarch64 = )[0-9a-f]{64}$', rf'\g<1>{linux_arm}')
    installer = winget_dir / "DeusData.CodebaseMemoryMcp.installer.yaml"
    content = installer.read_text(encoding="utf-8")
    content, count = re.subn(r'^(\s*InstallerSha256: )[0-9a-f]{64}$', rf'\g<1>{win}', content, flags=re.MULTILINE)
    if count != 1:
        raise RuntimeError("Winget installer checksum row is missing")
    with installer.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(content)

    write("VERSION", version + "\n")
    if current != version:
        print(f"updated version {current} -> {version}")


def update_versions_transactionally(version: str, checksums_path: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="cbm-version-update-") as temporary:
        backup_root = Path(temporary)
        for relative in MUTATED_FILES:
            source = ROOT / relative
            target = backup_root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
        winget_backup = backup_root / "winget"
        shutil.copytree(WINGET_ROOT, winget_backup)
        try:
            update_versions(version, checksums_path)
            if check() != 0:
                raise RuntimeError("updated version surfaces did not pass parity verification")
        except Exception:
            for relative in MUTATED_FILES:
                source = backup_root / relative
                target = ROOT / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)
            if WINGET_ROOT.exists():
                shutil.rmtree(WINGET_ROOT)
            shutil.copytree(winget_backup, WINGET_ROOT)
            raise


def collect_version_errors(version: str) -> list[str]:
    errors: list[str] = []

    def expect(path: str, pattern: str, count: int = 1) -> None:
        found = re.findall(pattern, read(path), flags=re.MULTILINE)
        if len(found) != count:
            errors.append(f"{path}: found {len(found)} version rows, expected {count}")
            return
        wrong = [value for value in found if value != version]
        if wrong:
            errors.append(f"{path}: expected {version}, found {', '.join(wrong)}")

    expect("flake.nix", r'^\s*version = "([0-9]+\.[0-9]+\.[0-9]+)";$')
    expect("server.json", r'"version": "([0-9]+\.[0-9]+\.[0-9]+)"', 3)
    expect("pkg/npm/package.json", r'"version": "([0-9]+\.[0-9]+\.[0-9]+)"')
    expect("pkg/pypi/pyproject.toml", r'^version = "([0-9]+\.[0-9]+\.[0-9]+)"$')
    expect("pkg/pypi/src/codebase_memory_mcp/_cli.py", r'^\s*return "([0-9]+\.[0-9]+\.[0-9]+)"$')
    expect("pkg/go/cmd/codebase-memory-mcp/main.go", r'^\s*version = "([0-9]+\.[0-9]+\.[0-9]+)"$')
    expect("pkg/homebrew/Formula/codebase-memory-mcp.rb", r'^\s*version "([0-9]+\.[0-9]+\.[0-9]+)"$')
    expect("pkg/scoop/codebase-memory-mcp.json", r'"version": "([0-9]+\.[0-9]+\.[0-9]+)"')
    expect("pkg/scoop/codebase-memory-mcp.json", r'releases/download/v([0-9]+\.[0-9]+\.[0-9]+)/codebase-memory')
    expect("pkg/aur/PKGBUILD", r'^pkgver=([0-9]+\.[0-9]+\.[0-9]+)$')
    expect("pkg/aur/.SRCINFO", r'^\s*pkgver = ([0-9]+\.[0-9]+\.[0-9]+)$')
    expect("pkg/aur/.SRCINFO", r'releases/download/v([0-9]+\.[0-9]+\.[0-9]+)/codebase-memory', 2)
    expect("pkg/chocolatey/codebase-memory-mcp.nuspec", r'<version>([0-9]+\.[0-9]+\.[0-9]+)</version>')
    expect("pkg/chocolatey/codebase-memory-mcp.nuspec", r'releases/tag/v([0-9]+\.[0-9]+\.[0-9]+)</releaseNotes>')
    expect("pkg/chocolatey/tools/chocolateyInstall.ps1", r"^\$version\s*= '([0-9]+\.[0-9]+\.[0-9]+)'$")
    expect("docs/index.html", r'"softwareVersion": "([0-9]+\.[0-9]+\.[0-9]+)"')
    expect(".github/ISSUE_TEMPLATE/bug_report.yml", r'^\s*placeholder: codebase-memory-mcp ([0-9]+\.[0-9]+\.[0-9]+)$')

    version_dirs = sorted(p.name for p in WINGET_ROOT.iterdir() if p.is_dir() and SEMVER.fullmatch(p.name))
    if version_dirs != [version]:
        errors.append(f"Winget version directories: expected [{version}], found {version_dirs}")
    else:
        winget_dir = WINGET_ROOT / version
        errors.extend(winget_shape_errors(winget_dir))
        manifests = [winget_dir / name for name in sorted(WINGET_MANIFEST_TYPES)]
        rows: list[str] = []
        for manifest in manifests:
            if manifest.is_file():
                rows.extend(re.findall(r'^PackageVersion: ([0-9]+\.[0-9]+\.[0-9]+)$', read(manifest), flags=re.MULTILINE))
        if rows != [version, version, version]:
            errors.append(f"Winget PackageVersion rows do not all match {version}: {rows}")
        installer = winget_dir / "DeusData.CodebaseMemoryMcp.installer.yaml"
        locale = winget_dir / "DeusData.CodebaseMemoryMcp.locale.en-US.yaml"
        expected_installer_url = (
            f"https://github.com/DeusData/codebase-memory-mcp/releases/download/v{version}/"
            "codebase-memory-mcp-windows-amd64.zip"
        )
        expected_notes_url = f"https://github.com/DeusData/codebase-memory-mcp/releases/tag/v{version}"
        if installer.is_file() and f"InstallerUrl: {expected_installer_url}" not in read(installer):
            errors.append("Winget installer URL does not match VERSION")
        if locale.is_file() and f"ReleaseNotesUrl: {expected_notes_url}" not in read(locale):
            errors.append("Winget release-notes URL does not match VERSION")

    release_workflow = read(".github/workflows/release.yml")
    steps = workflow_step_blocks(release_workflow)
    required_steps = {
        "Match release input to VERSION": {
            'expected="v$(tr -d \'\\r\\n\' < VERSION)"',
            'if [ "$RELEASE_VERSION" != "$expected" ]; then',
            "python3 scripts/sync-version.py",
        },
        "Verify registry package versions": {
            'test "$(jq -r \'.version\' pkg/npm/package.json)" = "$V"',
            'grep -q "^version = \\"$V\\"" pkg/pypi/pyproject.toml',
        },
        "Verify server.json version": {
            "'.version == $v and all(.packages[]; .version == $v)' server.json >/dev/null",
        },
    }
    for name, commands in required_steps.items():
        bodies = steps.get(name, [])
        if len(bodies) != 1:
            errors.append(f"release workflow requires exactly one active '{name}' step")
            continue
        missing_commands = sorted(commands - set(bodies[0]))
        if missing_commands:
            errors.append(f"release workflow step '{name}' is missing active commands: {', '.join(missing_commands)}")
    active_workflow_lines = [
        line.strip()
        for line in release_workflow.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if any("pkg/npm/package.tmp" in line or "server.tmp" in line for line in active_workflow_lines):
        errors.append("release workflow must verify checked-in versions without rewriting them")

    return errors


def collect_checksum_errors(version: str) -> list[str]:
    errors: list[str] = []
    data = json.loads(CHECKSUM_FILE.read_text(encoding="utf-8"))
    if data.get("version") != version:
        errors.append(f"pkg/release-checksums.json: expected version {version}")
    if data.get("source") != f"https://github.com/DeusData/codebase-memory-mcp/releases/download/v{version}/checksums.txt":
        errors.append("pkg/release-checksums.json: source URL does not match VERSION")
    assets = data.get("assets", {})
    if not isinstance(assets, dict):
        return errors + ["pkg/release-checksums.json: assets must be an object"]

    required = {
        "codebase-memory-mcp-darwin-amd64.tar.gz",
        "codebase-memory-mcp-darwin-arm64.tar.gz",
        "codebase-memory-mcp-linux-amd64.tar.gz",
        "codebase-memory-mcp-linux-arm64.tar.gz",
        "codebase-memory-mcp-windows-amd64.zip",
    }
    for name in sorted(required):
        checksum = assets.get(name)
        if not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum):
            errors.append(f"pkg/release-checksums.json: missing or invalid checksum for {name}")
    if errors:
        return errors

    def expect(label: str, actual: str | None, asset: str) -> None:
        expected = assets[asset]
        if actual != expected:
            errors.append(f"{label}: expected {asset} checksum {expected}, found {actual or 'missing'}")

    homebrew_path = "pkg/homebrew/Formula/codebase-memory-mcp.rb"
    homebrew = read(homebrew_path)
    homebrew_pairs = dict(
        re.findall(
            r'^\s*url "[^"]+/([^/"]+)"\s*\n\s*sha256 "([0-9a-f]{64})"$',
            homebrew,
            flags=re.MULTILINE,
        )
    )
    for asset in (
        "codebase-memory-mcp-darwin-arm64.tar.gz",
        "codebase-memory-mcp-darwin-amd64.tar.gz",
        "codebase-memory-mcp-linux-arm64.tar.gz",
        "codebase-memory-mcp-linux-amd64.tar.gz",
    ):
        expect(f"{homebrew_path} {asset}", homebrew_pairs.get(asset), asset)

    scoop_path = "pkg/scoop/codebase-memory-mcp.json"
    scoop = json.loads(read(scoop_path))
    scoop_64 = scoop.get("architecture", {}).get("64bit", {})
    expect(scoop_path, scoop_64.get("hash"), "codebase-memory-mcp-windows-amd64.zip")

    chocolatey_path = "pkg/chocolatey/tools/chocolateyInstall.ps1"
    chocolatey = read(chocolatey_path)
    chocolatey_match = re.search(r"^\$checksum64\s*= '([0-9a-f]{64})'$", chocolatey, flags=re.MULTILINE)
    expect(
        chocolatey_path,
        chocolatey_match.group(1) if chocolatey_match else None,
        "codebase-memory-mcp-windows-amd64.zip",
    )

    aur_path = "pkg/aur/PKGBUILD"
    aur = read(aur_path)
    for architecture, asset in (
        ("x86_64", "codebase-memory-mcp-linux-amd64.tar.gz"),
        ("aarch64", "codebase-memory-mcp-linux-arm64.tar.gz"),
    ):
        match = re.search(rf"^sha256sums_{architecture}=\('([0-9a-f]{{64}})'\)$", aur, flags=re.MULTILINE)
        expect(f"{aur_path} {architecture}", match.group(1) if match else None, asset)

    srcinfo_path = "pkg/aur/.SRCINFO"
    srcinfo = read(srcinfo_path)
    for architecture, asset in (
        ("x86_64", "codebase-memory-mcp-linux-amd64.tar.gz"),
        ("aarch64", "codebase-memory-mcp-linux-arm64.tar.gz"),
    ):
        match = re.search(rf"^\s*sha256sums_{architecture} = ([0-9a-f]{{64}})$", srcinfo, flags=re.MULTILINE)
        expect(f"{srcinfo_path} {architecture}", match.group(1) if match else None, asset)

    winget_path = (
        f"pkg/winget/manifests/d/DeusData/CodebaseMemoryMcp/{version}/"
        "DeusData.CodebaseMemoryMcp.installer.yaml"
    )
    if not (ROOT / winget_path).is_file():
        errors.append(f"{winget_path}: missing installer manifest")
        return errors
    winget = read(winget_path)
    winget_match = re.search(r"^\s*InstallerSha256: ([0-9a-f]{64})$", winget, flags=re.MULTILINE)
    expect(
        winget_path,
        winget_match.group(1) if winget_match else None,
        "codebase-memory-mcp-windows-amd64.zip",
    )
    return errors


def check() -> int:
    version = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
    if not SEMVER.fullmatch(version):
        print(f"VERSION is not semantic x.y.z: {version!r}", file=sys.stderr)
        return 1
    errors = collect_version_errors(version) + collect_checksum_errors(version)
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print(f"version parity: {version} across active metadata and release checksums")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", metavar="VERSION", help="update all surfaces to VERSION")
    parser.add_argument("--checksums", type=Path, help="official release checksums.txt for --write")
    args = parser.parse_args()
    if args.write:
        if not SEMVER.fullmatch(args.write):
            parser.error("--write requires a semantic x.y.z version")
        if not args.checksums:
            parser.error("--write requires --checksums from the published release")
        try:
            update_versions_transactionally(args.write, args.checksums.resolve())
        except (OSError, RuntimeError, ValueError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
    return check()


if __name__ == "__main__":
    raise SystemExit(main())
