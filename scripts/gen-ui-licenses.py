#!/usr/bin/env python3
"""Emit a markdown license appendix for the embedded graph-UI bundle.

Usage: gen-ui-licenses.py <graph-ui-dir>

Walks the PRODUCTION dependency tree (npm ls --omit=dev) — the packages
whose code is compiled into the UI bundle — and prints, for each unique
package, its declared license and the verbatim license file text found in
node_modules. Build-time tooling (vite, tailwind, lightningcss, ...) is
excluded: its code does not ship in the bundle.

Called by gen-third-party-notices.sh when node_modules is present (the
with-ui packaging path). Output is deterministic (sorted by name@version).
"""
import json
import os
import subprocess
import sys


def collect(ui_dir, dep_tree, out, excluded):
    for name, info in (dep_tree or {}).items():
        version = info.get("version")
        if not version:
            continue  # unmet optional peer — not installed, nothing shipped
        pkg_dir = os.path.join(ui_dir, "node_modules", *name.split("/"))
        try:
            with open(os.path.join(pkg_dir, "package.json"), encoding="utf-8") as fh:
                meta = json.load(fh)
        except (OSError, ValueError):
            continue  # not physically installed — nothing shipped
        # Platform-restricted packages (os/cpu fields) are native build
        # tooling (esbuild/rollup/lightningcss/oxide binaries). They cannot
        # be part of a browser bundle — exclude them and do not recurse into
        # their dependency subtrees (wasm runtime shims etc.).
        if meta.get("os") or meta.get("cpu"):
            excluded.add((name, version))
            continue
        out.add((name, version))
        collect(ui_dir, info.get("dependencies"), out, excluded)


def license_text(pkg_dir):
    if not os.path.isdir(pkg_dir):
        return None
    for fname in sorted(os.listdir(pkg_dir)):
        if fname.upper().startswith(("LICENSE", "LICENCE", "COPYING", "NOTICE")):
            try:
                with open(os.path.join(pkg_dir, fname), encoding="utf-8", errors="replace") as fh:
                    return fh.read().strip()
            except OSError:
                continue
    return None


def declared_license(pkg_dir):
    try:
        with open(os.path.join(pkg_dir, "package.json"), encoding="utf-8") as fh:
            d = json.load(fh)
    except (OSError, ValueError):
        return "UNKNOWN"
    lic = d.get("license", "UNKNOWN")
    if isinstance(lic, dict):
        lic = lic.get("type", "UNKNOWN")
    return str(lic)


def main():
    ui_dir = sys.argv[1]
    # shell=True so the npm shim resolves on every platform (npm.cmd on
    # Windows is not found by bare-name exec). Constant command, no injection.
    ls = subprocess.run(
        "npm ls --omit=dev --all --json",
        shell=True, cwd=ui_dir, capture_output=True, text=True, check=False,
    )
    tree = json.loads(ls.stdout or "{}")
    pkgs = set()
    excluded = set()
    collect(ui_dir, tree.get("dependencies"), pkgs, excluded)

    print("## Embedded Graph UI — bundled npm packages")
    print()
    print("The `-ui` binaries embed a compiled frontend bundle. The packages")
    print("below are its production dependency tree; their license texts are")
    print("reproduced verbatim from the packages as installed at build time.")
    print()

    for name, version in sorted(pkgs):
        pkg_dir = os.path.join(ui_dir, "node_modules", *name.split("/"))
        lic = declared_license(pkg_dir)
        text = license_text(pkg_dir)
        print(f"### {name}@{version} — {lic}")
        print()
        if text:
            print(text)
        else:
            print(f"(no license file shipped in the package; declared license: {lic})")
        print()

    if excluded:
        print("### Platform-specific build tooling (not part of the bundle)")
        print()
        print("Resolved in the dependency tree but excluded above: these are")
        print("native per-platform build binaries whose code does not ship in")
        print("the browser bundle.")
        print()
        for name, version in sorted(excluded):
            print(f"- {name}@{version}")
        print()


if __name__ == "__main__":
    main()
