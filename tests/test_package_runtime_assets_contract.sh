#!/usr/bin/env bash
# Runtime-sidecar contract for release archives, package managers and source
# installers. Release packaging executes the final staged binary against the
# exact adjacent files it will archive; downstream packages must retain those
# files either beside the executable or in ../share/codebase-memory-mcp.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python3 - "$ROOT" "$BASH" <<'PY'
from __future__ import annotations

import json
import hashlib
import os
import pathlib
import platform
import re
import shlex
import struct
import subprocess
import sys
import tarfile
import tempfile
import zipfile


root = pathlib.Path(sys.argv[1])
bash_executable = pathlib.Path(sys.argv[2])
failures: list[str] = []


def read(relative: str) -> str:
    return (root / relative).read_text(encoding="utf-8")


def require(condition: bool, message: str) -> None:
    if not condition:
        failures.append(message)


package_release = read("scripts/package-release.sh")
test_workflow = read(".github/workflows/_test.yml")
strip_position = package_release.find('strip_release_binary "$STAGED_BINARY"')
probe_position = package_release.find('"$STAGED_BINARY" --verify-runtime-assets')
tar_position = package_release.find('tar -czf "$OUT_DIR/$NAME.tar.gz"')
zip_position = package_release.find('zip -q "$OUT_DIR/$NAME.zip"')
require(
    'if ! "$STAGED_BINARY" --verify-runtime-assets; then' in package_release,
    "release packaging must execute the final staged binary's runtime-asset probe",
)
require(
    min(strip_position, probe_position, tar_position, zip_position) >= 0
    and strip_position < probe_position < tar_position
    and probe_position < zip_position,
    "Unix release packaging must probe after final binary transformation and before archive",
)
require(
    re.search(
        r'tar\s+-czf\s+"\$OUT_DIR/\$NAME\.tar\.gz"\s+-C\s+"\$PACK_DIR"',
        package_release,
    )
    is not None,
    "Unix release archive must be created from the same private directory that was probed",
)
require(
    'cd "$PACK_DIR"' in package_release
    and 'zip -q "$OUT_DIR/$NAME.zip" "${ARCHIVE_MEMBERS[@]}"' in package_release,
    "Windows release archive must be created from the same private directory that was probed",
)
windows_test_job = re.search(
    r"(?ms)^  test-windows:\n(?P<body>.*?)(?=^  [a-zA-Z0-9_-]+:\n)",
    test_workflow,
)
require(
    windows_test_job is not None
    and re.search(r"(?m)^\s+zip\s*$", windows_test_job.group("body")) is not None,
    "Windows test jobs that exercise package-release.sh must install zip",
)


homebrew = read("pkg/homebrew/Formula/codebase-memory-mcp.rb")
require(
    re.search(
        r'\(share\s*/\s*["\']codebase-memory-mcp["\']\)\.install\s+'
        r'["\']cbm-integrations\.json["\']',
        homebrew,
    )
    is not None,
    "Homebrew must install cbm-integrations.json in ../share/codebase-memory-mcp",
)
require(
    re.search(
        r'assert_path_exists\s+share\s*/\s*["\']codebase-memory-mcp/'
        r'cbm-integrations\.json["\']',
        homebrew,
    )
    is not None,
    "Homebrew formula test must assert the installed integration asset",
)
require(
    "codebase-memory-mcp install" in homebrew,
    "Homebrew must retain the opt-in agent-configuration caveat",
)

aur = read("pkg/aur/PKGBUILD")
require(
    re.search(
        r'install\s+-Dm644\s+["\']\$\{srcdir\}/cbm-integrations\.json["\']'
        r'\s*\\?\s*["\']\$\{pkgdir\}/usr/share/codebase-memory-mcp/'
        r'cbm-integrations\.json["\']',
        aur,
    )
    is not None,
    "AUR must install cbm-integrations.json in /usr/share/codebase-memory-mcp",
)

scoop = json.loads(read("pkg/scoop/codebase-memory-mcp.json"))
scoop_post_install = scoop.get("post_install", [])
if isinstance(scoop_post_install, str):
    scoop_post_install = [scoop_post_install]
require(
    not any(re.search(r'\binstall(?:\s|$)', command, re.IGNORECASE)
            for command in scoop_post_install),
    "Scoop must not invoke the full native install command after extracting the package",
)
require(
    scoop["architecture"]["64bit"].get("bin") == "codebase-memory-mcp.exe",
    "Scoop shim must target the executable inside the extracted package directory",
)

choco_install = read("pkg/chocolatey/tools/chocolateyInstall.ps1")
choco_uninstall = read("pkg/chocolatey/tools/chocolateyUninstall.ps1")
require(
    re.search(r'&\s*\$binPath\s+install\b', choco_install, re.IGNORECASE) is None,
    "Chocolatey must not invoke the full native install command after extraction",
)
require(
    re.search(r'-UnzipLocation\s+\$installDir\b', choco_install) is not None
    and re.search(
        r'\$binPath\s*=\s*Join-Path\s+\$installDir\s+'
        r'["\']codebase-memory-mcp\.exe["\']',
        choco_install,
        re.IGNORECASE,
    )
    is not None,
    "Chocolatey must shim the executable in the whole extracted package directory",
)
require(
    re.search(r'Remove-Item\s+\$installDir\s+-Recurse\s+-Force', choco_uninstall) is not None,
    "Chocolatey uninstall must remove the package-owned extraction directory",
)

setup_sh = read("scripts/setup.sh")
asset_helper = setup_sh.split("install_integration_asset() {", 1)[-1].split(
    "download_binary() {", 1
)[0]
download_body = setup_sh.split("download_binary() {", 1)[-1].split(
    "# --- Build from source ---", 1
)[0]
source_body = setup_sh.split("build_from_source() {", 1)[-1].split(
    "# --- MCP auto-configuration ---", 1
)[0]
require(
    re.search(
        r'cp\s+["\']\$source_path["\']\s+'
        r'["\']\$\{INSTALL_DIR\}/cbm-integrations\.json["\']',
        asset_helper,
    )
    is not None,
    "Unix integration-asset helper must copy the JSON beside the binary",
)
require(
    'install_integration_asset "$tmpdir"' in download_body,
    "Unix release download must install cbm-integrations.json beside the binary",
)
require(
    'install_integration_asset "${SOURCE_DIR}/build/c"' in source_body,
    "Unix source build must install cbm-integrations.json beside the binary",
)

setup_windows = read("scripts/setup-windows.ps1")
windows_source = setup_windows.split("if ($FromSource) {", 1)[-1].split(
    "} else {", 1
)[0]
windows_download = setup_windows.split("} else {", 1)[-1]
require(
    re.search(
        r'cp\s+build/c/codebase-memory-mcp\s+build/c/cbm-integrations\.json\s+'
        r'/home/\$wslUser/\.local/bin/',
        windows_source,
    )
    is not None,
    "Windows source build must copy cbm-integrations.json beside the WSL binary",
)
require(
    re.search(
        r'Expand-Archive\s+-Path\s+\$tmpZip\s+-DestinationPath\s+\$InstallDir\s+-Force',
        windows_download,
        re.IGNORECASE,
    )
    is not None,
    "Windows release download must retain the complete archive in the install directory",
)

glama = read("pkg/glama/Dockerfile")
require(
    re.search(r'tar\s+-xz\s+-C\s+/tmp[^\n]*cbm-integrations\.json', glama) is not None,
    "Glama package extraction must retain cbm-integrations.json",
)
require(
    re.search(
        r'COPY\s+--from=fetch\s+/tmp/cbm-integrations\.json\s+'
        r'/usr/local/share/codebase-memory-mcp/cbm-integrations\.json',
        glama,
    )
    is not None,
    "Glama must install cbm-integrations.json at the executable-relative share path",
)


# Exercise the packager itself with a tiny native executable that implements
# the same hidden command-line contract as the product. This is deliberately a
# native fixture (rather than a shell script): package-release still has to run
# its real strip and binary-composition gates before it reaches the association
# probe. The two negative cases distinguish self-integrity from association:
# the alternate UI pack is structurally valid and named by its own SHA-256, but
# it is not the pack the staged binary declares; likewise, the repository JSON
# is a valid manifest but is not the manifest expected by the mismatch fixture.
fixture_source_template = r'''
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define EXPECT_INTEGRATION __EXPECT_INTEGRATION__
#define EXPECT_UI_NAME __EXPECT_UI_NAME__
#define EXPECT_UI_MARKER __EXPECT_UI_MARKER__

static int file_contains(const char *path, const char *needle) {
    FILE *stream = fopen(path, "rb");
    if (!stream) return 0;
    const size_t needle_length = strlen(needle);
    size_t matched = 0;
    int byte = 0;
    while (needle_length > 0 && (byte = fgetc(stream)) != EOF) {
        if ((unsigned char)byte == (unsigned char)needle[matched]) {
            matched++;
            if (matched == needle_length) {
                fclose(stream);
                return 1;
            }
        } else {
            matched = ((unsigned char)byte == (unsigned char)needle[0]) ? 1U : 0U;
        }
    }
    fclose(stream);
    return 0;
}

static int adjacent_path(const char *argv0, const char *leaf, char *out, size_t out_size) {
    char directory[4096];
    const size_t length = strlen(argv0);
    if (length == 0 || length >= sizeof(directory)) return 0;
    memcpy(directory, argv0, length + 1U);
    char *slash = strrchr(directory, '/');
    char *backslash = strrchr(directory, '\\');
    if (backslash && (!slash || backslash > slash)) slash = backslash;
    if (!slash) return 0;
    *slash = '\0';
    const int written = snprintf(out, out_size, "%s/%s", directory, leaf);
    return written > 0 && (size_t)written < out_size;
}

int main(int argc, char **argv) {
    static const char product[] = "codebase-memory-mcp";
    char path[4096];
    if (argc != 2 || strcmp(argv[1], "--verify-runtime-assets") != 0) {
        fprintf(stderr, "%s fixture accepts only the runtime-asset probe\n", product);
        return 64;
    }
    if (!adjacent_path(argv[0], "cbm-integrations.json", path, sizeof(path)) ||
        !file_contains(path, EXPECT_INTEGRATION)) {
        fprintf(stderr, "%s fixture rejected integration assets\n", product);
        return 65;
    }
    if (EXPECT_UI_NAME[0] != '\0' &&
        (!adjacent_path(argv[0], EXPECT_UI_NAME, path, sizeof(path)) ||
         !file_contains(path, EXPECT_UI_MARKER))) {
        fprintf(stderr, "%s fixture rejected UI assets\n", product);
        return 66;
    }
    return 0;
}
'''


def valid_ui_pack(marker: str) -> bytes:
    path = b"/index.html"
    payload = ("<title>" + marker + "</title>").encode("ascii")
    header = bytearray(80)
    header[:8] = b"CBMUIPK\0"
    struct.pack_into("<HHIII", header, 8, 1, 80, 0, 1, 24)
    paths_offset = 80 + 24
    payload_offset = paths_offset + len(path)
    total = payload_offset + len(payload)
    struct.pack_into(
        "<QQQQQQQ",
        header,
        24,
        80,
        24,
        paths_offset,
        len(path),
        payload_offset,
        len(payload),
        total,
    )
    entry = struct.pack("<IHBBQQ", 0, len(path), 1, 1, 0, len(payload))
    return bytes(header) + entry + path + payload


host_system = platform.system().lower()
host_is_windows = (
    os.name == "nt"
    or bool(os.environ.get("MSYSTEM"))
    or host_system.startswith(("windows", "msys", "mingw", "cygwin"))
)
host_goos = "windows" if host_is_windows else (
    "darwin" if host_system == "darwin" else "linux"
)
binary_name = "codebase-memory-mcp.exe" if host_goos == "windows" else "codebase-memory-mcp"
archive_suffix = ".zip" if host_goos == "windows" else ".tar.gz"
compiler = shlex.split(os.environ.get("CC", "cc"))
arch_flags = shlex.split(os.environ.get("ARCHFLAGS", ""))
integration_marker = '\"format\":1,\"product\":\"codebase-memory-mcp\"'
pack_a = valid_ui_pack("fixture-pack-a")
pack_b = valid_ui_pack("fixture-pack-b")
pack_a_digest = hashlib.sha256(pack_a).hexdigest()
pack_b_digest = hashlib.sha256(pack_b).hexdigest()
pack_a_name = f"cbm-ui-{pack_a_digest}.pack"
pack_b_name = f"cbm-ui-{pack_b_digest}.pack"


def build_fixture(
    directory: pathlib.Path,
    expected_integration: str,
    expected_ui_name: str = "",
    expected_ui_marker: str = "",
) -> None:
    source = fixture_source_template
    source = source.replace("__EXPECT_INTEGRATION__", json.dumps(expected_integration))
    source = source.replace("__EXPECT_UI_NAME__", json.dumps(expected_ui_name))
    source = source.replace("__EXPECT_UI_MARKER__", json.dumps(expected_ui_marker))
    source_path = directory / "runtime_asset_fixture.c"
    source_path.write_text(source, encoding="utf-8")
    command = compiler + arch_flags + ["-std=c11", "-O2", str(source_path)]
    if host_goos == "linux":
        command.append("-Wl,-z,noexecstack")
    command += ["-o", str(directory / binary_name)]
    compiled = subprocess.run(
        command,
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    require(
        compiled.returncode == 0,
        "could not compile native release-packaging fixture: " + compiled.stdout.strip(),
    )


def package_fixture(
    work: pathlib.Path,
    case: str,
    variant: str,
    expected_integration: str,
    pack_name: str = "",
    pack_bytes: bytes = b"",
    expected_ui_name: str = "",
    expected_ui_marker: str = "",
) -> tuple[subprocess.CompletedProcess[str], pathlib.Path]:
    build_dir = work / f"{case}-build"
    output_dir = work / f"{case}-output"
    build_dir.mkdir()
    output_dir.mkdir()
    build_fixture(build_dir, expected_integration, expected_ui_name, expected_ui_marker)
    if pack_name:
        (build_dir / pack_name).write_bytes(pack_bytes)
    environment = os.environ.copy()
    environment["BUILD_DIR"] = str(build_dir)
    archive_name = f"codebase-memory-mcp{'-ui' if variant == 'ui' else ''}-{host_goos}-fixture{archive_suffix}"
    packaged = subprocess.run(
        [
            str(bash_executable),
            str(root / "scripts/package-release.sh"),
            host_goos,
            "fixture",
            "--variant",
            variant,
            "--out-dir",
            str(output_dir),
        ],
        cwd=root,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    return packaged, output_dir / archive_name


if compiler:
    with tempfile.TemporaryDirectory(prefix="cbm-package-association-") as temp:
        work = pathlib.Path(temp)
        standard, standard_archive = package_fixture(
            work, "standard-ok", "standard", integration_marker
        )
        require(
            standard.returncode == 0 and standard_archive.is_file(),
            "release packager rejected a matching standard fixture: " + standard.stdout.strip(),
        )
        if standard_archive.is_file():
            if host_goos == "windows":
                with zipfile.ZipFile(standard_archive) as archive:
                    standard_members = set(archive.namelist())
            else:
                with tarfile.open(standard_archive, "r:gz") as archive:
                    standard_members = set(archive.getnames())
            require(
                standard_members
                == {
                    binary_name,
                    "cbm-integrations.json",
                    "LICENSE",
                    "install.ps1" if host_goos == "windows" else "install.sh",
                    "THIRD_PARTY_NOTICES.md",
                },
                "standard archive must contain exactly the five verified/staged members "
                f"(got {sorted(standard_members)!r})",
            )

        ui, ui_archive = package_fixture(
            work,
            "ui-ok",
            "ui",
            integration_marker,
            pack_a_name,
            pack_a,
            pack_a_name,
            "fixture-pack-a",
        )
        require(
            ui.returncode == 0 and ui_archive.is_file(),
            "release packager rejected a matching UI fixture: " + ui.stdout.strip(),
        )
        if ui_archive.is_file():
            if host_goos == "windows":
                with zipfile.ZipFile(ui_archive) as archive:
                    ui_members = set(archive.namelist())
            else:
                with tarfile.open(ui_archive, "r:gz") as archive:
                    ui_members = set(archive.getnames())
            require(
                ui_members
                == {
                    binary_name,
                    "cbm-integrations.json",
                    "LICENSE",
                    "install.ps1" if host_goos == "windows" else "install.sh",
                    "THIRD_PARTY_NOTICES.md",
                    pack_a_name,
                },
                "UI archive must contain exactly the five base members plus its verified pack "
                f"(got {sorted(ui_members)!r})",
            )

        wrong_pack, wrong_pack_archive = package_fixture(
            work,
            "ui-wrong-pack",
            "ui",
            integration_marker,
            pack_b_name,
            pack_b,
            pack_a_name,
            "fixture-pack-a",
        )
        require(
            wrong_pack.returncode != 0
            and "staged binary rejected its adjacent runtime assets" in wrong_pack.stdout
            and not wrong_pack_archive.exists(),
            "release packager accepted a structurally valid, self-hashed UI pack for a different binary",
        )

        wrong_manifest, wrong_manifest_archive = package_fixture(
            work,
            "standard-wrong-manifest",
            "standard",
            "manifest-association-that-is-not-in-the-repository-asset",
        )
        require(
            wrong_manifest.returncode != 0
            and "staged binary rejected its adjacent runtime assets" in wrong_manifest.stdout
            and not wrong_manifest_archive.exists(),
            "release packager accepted a valid integration manifest that did not match the binary",
        )

if failures:
    print("FAIL: package runtime-asset contract", file=sys.stderr)
    for failure in failures:
        print(f"  - {failure}", file=sys.stderr)
    raise SystemExit(1)

print("PASS: release and package-manager runtime assets stay bound to their executable")
PY
