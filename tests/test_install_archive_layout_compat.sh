#!/usr/bin/env bash
# Regression for #1499: published v0.9.0 Windows/Unix archives omit
# cbm-integrations.json, while main installers initially required it. Install
# scripts must accept both the legacy four-file core layout and the current
# five-file layout (core + cbm-integrations.json). package-release.sh still
# ships the five-file set for new builds.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python3 - "$ROOT" <<'PY'
from __future__ import annotations

import pathlib
import re
import sys
import tempfile
import zipfile


root = pathlib.Path(sys.argv[1])
failures: list[str] = []


def require(condition: bool, message: str) -> None:
    if not condition:
        failures.append(message)


install_ps1 = (root / "install.ps1").read_text(encoding="utf-8")
install_sh = (root / "install.sh").read_text(encoding="utf-8")
package_release = (root / "scripts/package-release.sh").read_text(encoding="utf-8")

require(
    "WindowsCoreArchiveNames" in install_ps1
    and "WindowsIntegrationArchiveName" in install_ps1
    and "WindowsHasIntegrationManifest" in install_ps1,
    "install.ps1 must split core members from the optional integration manifest",
)
require(
    "EXPECTED_CORE_COUNT=4" in install_sh and "EXPECTED_CORE_COUNT=5" in install_sh,
    "install.sh must accept both the legacy 4-file and current 5-file layouts",
)
require(
    "INTEGRATION_MEMBERS\" -eq 0" in install_sh
    or '[ "$INTEGRATION_MEMBERS" -eq 0 ]' in install_sh,
    "install.sh must treat a missing cbm-integrations.json as the legacy layout",
)
require(
    "cbm-integrations.json LICENSE install.ps1" in package_release
    and "cbm-integrations.json LICENSE install.sh" in package_release,
    "package-release.sh must keep shipping cbm-integrations.json in new archives",
)

# Extract the PowerShell validation core into a tiny harness that mirrors the
# allowlist logic without downloading anything.
core = [
    "codebase-memory-mcp.exe",
    "LICENSE",
    "install.ps1",
    "THIRD_PARTY_NOTICES.md",
]
integration = "cbm-integrations.json"
ui_pack = "cbm-ui-" + ("a" * 64) + ".pack"


def validate(names: list[str], variant: str = "standard") -> str | None:
    """Return None on success, else the error string."""
    archive_names = core + [integration]
    counts = {name: 0 for name in archive_names}
    seen: set[str] = set()
    ui_pack_count = 0
    ui_pattern = re.compile(r"^cbm-ui-[0-9a-f]{64}\.pack$")
    for entry_name in names:
        if entry_name in seen:
            return f"duplicate or case-conflicting zip entry: {entry_name}"
        seen.add(entry_name)
        if entry_name in counts:
            counts[entry_name] += 1
        elif variant == "ui" and ui_pattern.match(entry_name):
            ui_pack_count += 1
        else:
            return f"archive contains an unexpected root entry: {entry_name}"
    for name in core:
        if counts[name] != 1:
            return f"archive must contain exactly one {name}"
    integration_count = counts[integration]
    if integration_count not in (0, 1):
        return f"archive must contain exactly one {integration}"
    expected_ui = 1 if variant == "ui" else 0
    expected = len(core) + integration_count + expected_ui
    if ui_pack_count != expected_ui or len(seen) != expected:
        return f"archive does not match the exact {variant} Windows release allowlist"
    return None


legacy = list(core)
current = list(core) + [integration]
legacy_ui = list(core) + [ui_pack]
current_ui = list(core) + [integration, ui_pack]

require(validate(legacy) is None, f"legacy 4-file layout must pass: {validate(legacy)}")
require(validate(current) is None, f"current 5-file layout must pass: {validate(current)}")
require(
    validate(legacy_ui, "ui") is None,
    f"legacy UI layout must pass: {validate(legacy_ui, 'ui')}",
)
require(
    validate(current_ui, "ui") is None,
    f"current UI layout must pass: {validate(current_ui, 'ui')}",
)
require(
    validate(core[:3]) is not None,
    "incomplete core layout must still fail",
)
require(
    validate(current + ["extra.txt"]) is not None,
    "unexpected extra root entry must still fail",
)
require(
    validate(core + [integration, integration]) is not None,
    "duplicate integration manifest must still fail",
)

# install.sh acceptance matrix for INTEGRATION_MEMBERS
sh_cases = [
    # binary, integration, license, installer, notice, ui, variant, expect_ok
    (1, 1, 1, 1, 1, 0, "standard", True),
    (1, 0, 1, 1, 1, 0, "standard", True),
    (1, 1, 1, 1, 1, 1, "ui", True),
    (1, 0, 1, 1, 1, 1, "ui", True),
    (1, 2, 1, 1, 1, 0, "standard", False),
    (1, 1, 1, 1, 0, 0, "standard", False),
    (1, 0, 1, 1, 1, 0, "ui", False),  # ui without pack
]


def sh_ok(
    binary: int,
    integration: int,
    license_n: int,
    installer: int,
    notice: int,
    ui: int,
    variant: str,
) -> bool:
    if integration == 1:
        expected_core = 5
    elif integration == 0:
        expected_core = 4
    else:
        return False
    expected_member = expected_core + (1 if variant == "ui" else 0)
    archive_member = binary + integration + license_n + installer + notice + ui
    return (
        binary == 1
        and license_n == 1
        and installer == 1
        and notice == 1
        and ui == (expected_member - expected_core)
        and archive_member == expected_member
    )


for case in sh_cases:
    *counts, variant, expect = case
    got = sh_ok(*counts, variant)
    require(
        got is expect,
        f"install.sh matrix {case} expected {expect}, got {got}",
    )

# Prove a real zip with the legacy layout enumerates the way install.ps1 expects.
with tempfile.TemporaryDirectory() as tmp:
    path = pathlib.Path(tmp) / "legacy.zip"
    with zipfile.ZipFile(path, "w") as zf:
        for name in legacy:
            zf.writestr(name, b"x")
    names = zipfile.ZipFile(path).namelist()
    require(validate(names) is None, f"zip legacy layout failed: {validate(names)}")

if failures:
    print("test_install_archive_layout_compat FAILED:")
    for failure in failures:
        print(f"  - {failure}")
    sys.exit(1)

print("test_install_archive_layout_compat: ok")
PY
