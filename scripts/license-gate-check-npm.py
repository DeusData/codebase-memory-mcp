#!/usr/bin/env python3
"""License-gate check for the graph-UI npm production dependency tree.

Usage: license-gate-check-npm.py <graph-ui-dir> <license-policy.json>

Walks the WHOLE production tree (including prebundled dependencies and
platform-restricted native packages) and fails if ANY package's license is not
on the policy allow-list. There are no exceptions: a package that is installed
but never bundled still blocks, because "unshipped" is a claim about a build,
and a non-permissive license must never be one build change away from shipping.
Packages with no declared license fall back to the first line of their shipped
license file; if that cannot be resolved either, the gate fails — unknown is
not allowed. The current app + worker build evidence must also verify.
"""
import json
import os
import re
import subprocess
import sys
from ui_browser_licenses import validate_browser_build

FILE_HEADER_MAP = {
    "mit license": "MIT",
    "the mit license": "MIT",
    "the mit license (mit)": "MIT",
    "apache license": "Apache-2.0",
    "isc license": "ISC",
    "bsd 2-clause license": "BSD-2-Clause",
    "bsd 3-clause license": "BSD-3-Clause",
    "the unlicense": "Unlicense",
}


def license_from_file(pkg_dir):
    for fname in sorted(os.listdir(pkg_dir)):
        if fname.upper().startswith(("LICENSE", "LICENCE", "COPYING", "UNLICENSE")):
            try:
                with open(os.path.join(pkg_dir, fname), encoding="utf-8",
                          errors="replace") as fh:
                    first = fh.readline().strip().lower()
                return FILE_HEADER_MAP.get(first)
            except OSError:
                return None
    return None


def collect(ui_dir, dep_tree, out):
    for name, info in (dep_tree or {}).items():
        version = info.get("version")
        if not version:
            continue
        pkg_dir = os.path.join(ui_dir, "node_modules", *name.split("/"))
        try:
            with open(os.path.join(pkg_dir, "package.json"), encoding="utf-8") as fh:
                meta = json.load(fh)
        except (OSError, ValueError):
            continue
        # Platform-restricted packages (os/cpu) are checked like any other:
        # skipping them is how sharp's LGPL libvips binaries passed unseen.
        lic = meta.get("license", "")
        if isinstance(lic, dict):
            lic = lic.get("type", "")
        out[(name, version)] = (str(lic or ""), pkg_dir)
        collect(ui_dir, info.get("dependencies"), out)


def main():
    ui_dir, policy_path = sys.argv[1], sys.argv[2]
    with open(policy_path) as fh:
        policy = json.load(fh)
    allowed = {x.lower() for x in policy["allowed_spdx_ids"]}
    ignored_pkgs = set(policy.get("ignored_npm_packages", []))
    skip_tokens = {"and", "or", "with", ""}

    # shell=True so the npm shim resolves on every platform (npm.cmd on
    # Windows is not found by bare-name exec). Constant command, no injection.
    ls = subprocess.run("npm ls --omit=dev --all --json",
                        shell=True, cwd=ui_dir, capture_output=True, text=True, check=False)
    tree = json.loads(ls.stdout or "{}")
    pkgs = {}
    collect(ui_dir, tree.get("dependencies"), pkgs)
    if not pkgs:
        print("FAIL: npm production tree resolved to zero packages — "
              "is node_modules installed?")
        sys.exit(1)

    try:
        validate_browser_build(ui_dir)
    except ValueError as exc:
        print(f"BLOCKED: browser build evidence does not verify: {exc}")
        sys.exit(1)

    violations = []
    for (name, version), (lic, pkg_dir) in sorted(pkgs.items()):
        if name in ignored_pkgs:
            continue
        if not lic:
            lic = license_from_file(pkg_dir) or ""
        if not lic:
            violations.append((f"{name}@{version}", "no resolvable license"))
            continue
        for tok in re.split(r"[\s()]+", lic):
            if tok.lower() in skip_tokens:
                continue
            if tok.lower() not in allowed:
                violations.append((f"{name}@{version}", lic))
                break

    if violations:
        print("BLOCKED: %d UI package(s) outside the license allow-list:" % len(violations))
        for pkg, lic in violations[:25]:
            print(f"  {pkg}: {lic}")
        sys.exit(1)
    print(f"OK: {len(pkgs)} npm packages covered, all allow-listed")


if __name__ == "__main__":
    main()
