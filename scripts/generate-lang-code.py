#!/usr/bin/env python3
"""Generate boilerplate code for new tree-sitter language support.

Reads scripts/new-languages.json and generates:
1. Grammar wrapper .c files (written directly)
2. Enum entries for cbm.h
3. Lang spec entries for lang_specs.c (designated initializer + factory)
4. Extension/filename/name entries for language.c
"""
import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
MANIFEST = os.path.join(SCRIPT_DIR, "new-languages.json")
GRAMMAR_DIR = os.path.join(PROJECT_DIR, "internal", "cbm")


def main():
    with open(MANIFEST) as f:
        langs = json.load(f)

    mode = sys.argv[1] if len(sys.argv) > 1 else "all"

    if mode in ("all", "wrappers"):
        generate_wrappers(langs)
    if mode in ("all", "enum"):
        generate_enum(langs)
    if mode in ("all", "specs"):
        generate_specs(langs)
    if mode in ("all", "language"):
        generate_language_c(langs)


def generate_wrappers(langs):
    """Create grammar_<name>.c wrapper files."""
    print("=== Grammar Wrapper Files ===")
    created = 0
    for lang in langs:
        path = os.path.join(GRAMMAR_DIR, f"grammar_{lang['name']}.c")
        if os.path.exists(path):
            continue
        lines = [
            f"// Vendored tree-sitter grammar: {lang['name']}",
            "// Each grammar compiled as separate unit (conflicting static symbols).",
            f"#include \"vendored/grammars/{lang['name']}/parser.c\"",
        ]
        if lang["has_scanner"]:
            lines.append(
                f"#include \"vendored/grammars/{lang['name']}/scanner.c\""
            )
        with open(path, "w") as f:
            f.write("\n".join(lines) + "\n")
        created += 1
    print(f"  Created {created} wrapper files")


def generate_enum(langs):
    """Print enum entries for cbm.h."""
    print("\n=== Enum Entries (paste into cbm.h before CBM_LANG_KUSTOMIZE) ===")
    for lang in langs:
        print(f"    CBM_LANG_{lang['enum']},")


def generate_specs(langs):
    """Print lang spec entries for lang_specs.c."""
    print("\n=== Extern Declarations (paste at top of lang_specs.c) ===")
    for lang in langs:
        print(f"extern const TSLanguage *{lang['ts_func']}(void);")

    print("\n=== Module Type Arrays (paste before spec table) ===")
    for lang in langs:
        arr = f"{lang['name']}_module_types"
        print(
            f'static const char *{arr}[] = {{"{lang["module_root"]}", NULL}};'
        )

    print("\n=== Spec Table Entries (paste into lang_specs[]) ===")
    for lang in langs:
        mod = f"{lang['name']}_module_types"
        print(f"    // CBM_LANG_{lang['enum']}")
        print(
            f"    [CBM_LANG_{lang['enum']}] = {{CBM_LANG_{lang['enum']}, "
            f"empty_types, empty_types, empty_types, {mod}, "
            f"empty_types, empty_types, empty_types, empty_types, "
            f"empty_types, empty_types, empty_types, NULL, empty_types, "
            f"NULL, NULL, {lang['ts_func']}}},"
        )
        print()


def generate_language_c(langs):
    """Print extension table and name entries for language.c."""
    print("\n=== EXT_TABLE Entries (paste into language.c, sorted by ext) ===")
    ext_entries = []
    for lang in langs:
        for ext in lang["extensions"]:
            ext_entries.append((ext, lang["enum"], lang["display"]))
    for ext, enum, display in sorted(ext_entries, key=lambda x: x[0].lower()):
        print(f'    /* {display} */')
        print(f'    {{"{ext}", CBM_LANG_{enum}}},')
        print()

    print("\n=== FILENAME_TABLE Entries ===")
    for lang in langs:
        for fn in lang["filenames"]:
            print(f'    {{"{fn}", CBM_LANG_{lang["enum"]}}},')

    print("\n=== LANG_NAMES Entries ===")
    for lang in langs:
        print(
            f'    [CBM_LANG_{lang["enum"]}] = "{lang["display"]}",'
        )


if __name__ == "__main__":
    main()
