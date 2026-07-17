#!/usr/bin/env python3
"""Fail when a direct C test file or suite is absent from the main runner."""

from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TESTS = ROOT / "tests"


def main() -> int:
    make_text = (ROOT / "Makefile.cbm").read_text(encoding="utf-8")
    main_text = (TESTS / "test_main.c").read_text(encoding="utf-8")

    files = {path.name for path in TESTS.glob("test_*.c")}
    make_entries = re.findall(r"tests/(test_[a-z0-9_]+\.c)", make_text)
    registered = set(make_entries)
    errors: list[str] = []

    missing = sorted(files - registered)
    stale = sorted(registered - files)
    duplicates = sorted(name for name, count in Counter(make_entries).items() if count > 1)
    if missing:
        errors.append("test files missing from Makefile.cbm: " + ", ".join(missing))
    if stale:
        errors.append("Makefile.cbm references missing test files: " + ", ".join(stale))
    if duplicates:
        errors.append("duplicate Makefile.cbm test entries: " + ", ".join(duplicates))

    suite_entries: list[str] = []
    suite_files: dict[str, list[str]] = {}
    for path in TESTS.glob("test_*.c"):
        content = path.read_text(encoding="utf-8")
        discovered = re.findall(r"^\s*SUITE\(([a-z0-9_]+)\)\s*\{", content, flags=re.MULTILINE)
        discovered.extend(
            re.findall(r"^\s*void\s+suite_([a-z0-9_]+)\s*\(void\)\s*\{", content, flags=re.MULTILINE)
        )
        if path.name != "test_main.c" and not discovered:
            errors.append(f"registered test file defines no suite: {path.name}")
        for suite in discovered:
            suite_entries.append(suite)
            suite_files.setdefault(suite, []).append(path.name)

    suites = set(suite_entries)
    duplicate_suites = sorted(suite for suite, count in Counter(suite_entries).items() if count > 1)
    if duplicate_suites:
        details = [f"{suite} ({', '.join(suite_files[suite])})" for suite in duplicate_suites]
        errors.append("suite definitions must be unique: " + ", ".join(details))

    declaration_entries = re.findall(r"extern void suite_([a-z0-9_]+)\(void\);", main_text)
    run_entries = re.findall(r"RUN_SELECTED_SUITE\(([a-z0-9_]+)\);", main_text)
    declarations = set(declaration_entries)
    runs = set(run_entries)
    duplicate_declarations = sorted(name for name, count in Counter(declaration_entries).items() if count > 1)
    duplicate_runs = sorted(name for name, count in Counter(run_entries).items() if count > 1)

    if suites - declarations:
        errors.append("suites missing declarations: " + ", ".join(sorted(suites - declarations)))
    if suites - runs:
        errors.append("suites missing runner calls: " + ", ".join(sorted(suites - runs)))
    if declarations - suites:
        errors.append("runner declares unknown suites: " + ", ".join(sorted(declarations - suites)))
    if runs - suites:
        errors.append("runner calls unknown suites: " + ", ".join(sorted(runs - suites)))
    if duplicate_declarations:
        errors.append("runner declares suites more than once: " + ", ".join(duplicate_declarations))
    if duplicate_runs:
        errors.append("runner calls suites more than once: " + ", ".join(duplicate_runs))

    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print(f"test registration: {len(files)} files, {len(suites)} suites, all registered")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
