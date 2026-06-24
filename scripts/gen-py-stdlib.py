#!/usr/bin/env python3.9
"""
gen-py-stdlib.py — Generate cbm_python_stdlib_register from typeshed stubs.

Phase 10 of Python LSP integration. Walks a typeshed/stdlib checkout,
parses each .pyi via the standard `ast` module, and emits a single C
source file (internal/cbm/lsp/generated/python_stdlib_data.c) that
populates a CBMTypeRegistry with classes + their method names and
free functions per module.

This is a v1 implementation — overload stacks collapse to the first
signature, ParamSpec/TypeVarTuple/Concatenate are skipped, version
guards `if sys.version_info >= (X, Y):` are flattened (we take the
union of all branches). Per-symbol min/max version guards from the
plan are recorded as comments only in v1; py_lsp consumers should
treat the generator output as the 3.12 baseline.

Usage:
    python3.9 scripts/gen-py-stdlib.py <typeshed-stdlib-path> <output.c>

Defaults to /tmp/python-lsp-references/typeshed/stdlib and
internal/cbm/lsp/generated/python_stdlib_data.c when run without args.
"""

from __future__ import annotations

import argparse
import ast
import dataclasses
import os
import sys
from pathlib import Path
from typing import Iterable

# v1 module allowlist — the plan's "top usage" set. Skip large / low-value
# modules (tkinter, turtle, curses, xml.*, email.*) to keep generated
# output manageable.
ALLOWED_MODULES = {
    "builtins",
    "typing",
    "typing_extensions",
    "os",
    "sys",
    "collections",
    "functools",
    "itertools",
    "pathlib",
    "json",
    "re",
    "dataclasses",
    "enum",
    "datetime",
    "subprocess",
    "logging",
    "asyncio",
    "unittest",
    "argparse",
    "contextlib",
    "io",
    "tempfile",
    "shutil",
    "inspect",
    "abc",
    "warnings",
    "weakref",
    "copy",
    "pickle",
    "time",
    "math",
    "string",
    "socket",
    "threading",
    "multiprocessing",
    "queue",
    "urllib",
    "http",
    "concurrent",
}


@dataclasses.dataclass
class StubMethod:
    name: str


@dataclasses.dataclass
class StubClass:
    qualified_name: str
    short_name: str
    methods: list[str]
    bases: list[str]


@dataclasses.dataclass
class StubFunction:
    qualified_name: str
    short_name: str
    module_qn: str


@dataclasses.dataclass
class ModuleStubs:
    module_qn: str
    classes: list[StubClass]
    functions: list[StubFunction]


def is_method(node: ast.AST) -> bool:
    return isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))


def class_methods(class_node: ast.ClassDef) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for child in class_node.body:
        if is_method(child):
            name = child.name  # type: ignore[attr-defined]
            if not name.startswith("_") or name in {"__init__", "__call__", "__enter__", "__exit__"}:
                if name not in seen:
                    seen.add(name)
                    out.append(name)
        elif isinstance(child, ast.If):
            # Flatten version guards: walk both branches.
            for sub in child.body + child.orelse:
                if is_method(sub):
                    name = sub.name  # type: ignore[attr-defined]
                    if name not in seen:
                        seen.add(name)
                        out.append(name)
    return out


def base_qns(class_node: ast.ClassDef, module_qn: str) -> list[str]:
    out: list[str] = []
    for base in class_node.bases:
        if isinstance(base, ast.Name):
            # Bare name — qualify within the same module by default.
            out.append(f"{module_qn}.{base.id}")
        elif isinstance(base, ast.Attribute):
            # mod.SubClass
            try:
                text = ast.unparse(base)  # type: ignore[attr-defined]
            except Exception:
                continue
            out.append(text)
    return out


@dataclasses.dataclass
class StarReExport:
    target_module: str   # e.g. "posixpath" / "ntpath"


def parse_module(path: Path, module_qn: str) -> tuple[ModuleStubs, list[StarReExport]]:
    src = path.read_text(encoding="utf-8", errors="replace")
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return ModuleStubs(module_qn=module_qn, classes=[], functions=[]), []

    classes: list[StubClass] = []
    functions: list[StubFunction] = []
    star_imports: list[StarReExport] = []
    seen_funcs: set[str] = set()

    def walk(body: list[ast.stmt], current_module_qn: str) -> None:
        for node in body:
            if isinstance(node, ast.ClassDef):
                qn = f"{current_module_qn}.{node.name}"
                classes.append(StubClass(
                    qualified_name=qn,
                    short_name=node.name,
                    methods=class_methods(node),
                    bases=base_qns(node, current_module_qn),
                ))
            elif is_method(node):
                name = node.name  # type: ignore[attr-defined]
                if name.startswith("_") and name not in {"__init__"}:
                    continue
                if name in seen_funcs:
                    continue
                seen_funcs.add(name)
                functions.append(StubFunction(
                    qualified_name=f"{current_module_qn}.{name}",
                    short_name=name,
                    module_qn=current_module_qn,
                ))
            elif isinstance(node, ast.ImportFrom):
                # Track `from X import *` for re-export resolution.
                if node.module and any(alias.name == "*" for alias in node.names):
                    star_imports.append(StarReExport(target_module=node.module))
            elif isinstance(node, ast.If):
                # Flatten version guards.
                walk(node.body, current_module_qn)
                walk(node.orelse, current_module_qn)

    walk(tree.body, module_qn)
    return ModuleStubs(module_qn=module_qn, classes=classes, functions=functions), star_imports


def collect_all_stubs(stdlib_root: Path) -> tuple[dict[str, ModuleStubs], dict[str, list[StarReExport]]]:
    """Gather every stub in the allowlist plus any re-export targets they
    pull in transitively (e.g. os.path -> posixpath -> genericpath).
    Returns (modules, star_imports_per_module). """
    modules: dict[str, ModuleStubs] = {}
    star_imports: dict[str, list[StarReExport]] = {}

    # First pass: walk allowlist
    queue: list[tuple[Path, str]] = []
    for path in sorted(stdlib_root.rglob("*.pyi")):
        rel = path.relative_to(stdlib_root)
        parts = list(rel.parts)
        if parts[-1] == "__init__.pyi":
            parts.pop()
        else:
            parts[-1] = parts[-1][:-len(".pyi")]
        if not parts:
            continue
        top = parts[0]
        if top.startswith("_"):
            continue
        if top not in ALLOWED_MODULES:
            continue
        module_qn = ".".join(parts)
        queue.append((path, module_qn))

    seen_targets: set[str] = set()
    while queue:
        path, mod_qn = queue.pop()
        if mod_qn in modules:
            continue
        ms, stars = parse_module(path, mod_qn)
        modules[mod_qn] = ms
        star_imports[mod_qn] = stars
        # Enqueue re-export targets (e.g. posixpath, ntpath, genericpath)
        for star in stars:
            tgt = star.target_module
            if tgt in seen_targets:
                continue
            seen_targets.add(tgt)
            tgt_path = stdlib_root / (tgt.replace(".", "/") + ".pyi")
            if tgt_path.is_file():
                queue.append((tgt_path, tgt))
            else:
                tgt_init = stdlib_root / tgt.replace(".", "/") / "__init__.pyi"
                if tgt_init.is_file():
                    queue.append((tgt_init, tgt))

    return modules, star_imports


def resolve_reexports(modules: dict[str, ModuleStubs],
                       star_imports: dict[str, list[StarReExport]]) -> None:
    """For each module with `from X import *`, copy X's classes/functions
    into the current module under that module's QN. Iterates to a fixed
    point to handle re-export chains. """
    changed = True
    iter_count = 0
    while changed and iter_count < 8:
        changed = False
        iter_count += 1
        for mod_qn, stars in star_imports.items():
            ms = modules[mod_qn]
            for star in stars:
                target = modules.get(star.target_module)
                if not target:
                    continue
                # Copy target's classes/functions under mod_qn
                existing_class_names = {c.short_name for c in ms.classes}
                existing_func_names = {f.short_name for f in ms.functions}
                for c in target.classes:
                    if c.short_name in existing_class_names:
                        continue
                    ms.classes.append(StubClass(
                        qualified_name=f"{mod_qn}.{c.short_name}",
                        short_name=c.short_name,
                        methods=list(c.methods),
                        bases=list(c.bases),
                    ))
                    existing_class_names.add(c.short_name)
                    changed = True
                for f in target.functions:
                    if f.short_name in existing_func_names:
                        continue
                    ms.functions.append(StubFunction(
                        qualified_name=f"{mod_qn}.{f.short_name}",
                        short_name=f.short_name,
                        module_qn=mod_qn,
                    ))
                    existing_func_names.add(f.short_name)
                    changed = True


def iter_module_stubs(stdlib_root: Path) -> Iterable[ModuleStubs]:
    modules, star_imports = collect_all_stubs(stdlib_root)
    resolve_reexports(modules, star_imports)
    for mod_qn in sorted(modules.keys()):
        yield modules[mod_qn]


C_HEADER = """\
// AUTO-GENERATED by scripts/gen-py-stdlib.py — DO NOT EDIT
// Python stdlib type information for LSP type resolver.
//
// Source: typeshed commit a7912d521e16ff63caf7a8b64b9072542be36777
// Module allowlist: see ALLOWED_MODULES in scripts/gen-py-stdlib.py
//
// v1 simplifications (per PYTHON_LSP_PLAN.md Phase 10):
//   - overload stacks collapse to first signature
//   - ParamSpec / TypeVarTuple / Concatenate skipped
//   - version guards flattened (union of all branches)
//   - per-symbol min/max version guards not yet emitted

#include "../type_rep.h"
#include "../type_registry.h"
#include <string.h>

#define CBM_PYTHON_STDLIB_GENERATED 1

void cbm_python_stdlib_register(CBMTypeRegistry* reg, CBMArena* arena) {
    if (!reg || !arena) return;

    CBMRegisteredType rt;
    CBMRegisteredFunc rf;
"""

C_FOOTER = """\
}
"""


def c_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace("\"", "\\\"")


def emit(stubs: list[ModuleStubs], output_path: Path) -> None:
    method_table_id = 0
    out_lines: list[str] = []

    for ms in stubs:
        if not ms.classes and not ms.functions:
            continue
        out_lines.append(f"\n    /* ===== module: {ms.module_qn} ===== */\n")

        for cls in ms.classes:
            method_array_name = None
            if cls.methods:
                method_table_id += 1
                method_array_name = f"py_methods_{method_table_id}"
                methods_c = ", ".join(f"\"{c_escape(m)}\"" for m in cls.methods)
                out_lines.append(
                    f"    static const char* {method_array_name}[] = {{ {methods_c}, NULL }};\n"
                )
            out_lines.append("    memset(&rt, 0, sizeof(rt));\n")
            out_lines.append(
                f"    rt.qualified_name = \"{c_escape(cls.qualified_name)}\";\n"
            )
            out_lines.append(
                f"    rt.short_name = \"{c_escape(cls.short_name)}\";\n"
            )
            if method_array_name:
                out_lines.append(f"    rt.method_names = {method_array_name};\n")
            if cls.bases:
                method_table_id += 1
                bases_array = f"py_bases_{method_table_id}"
                bases_c = ", ".join(f"\"{c_escape(b)}\"" for b in cls.bases)
                out_lines.append(
                    f"    static const char* {bases_array}[] = {{ {bases_c}, NULL }};\n"
                )
                out_lines.append(f"    rt.embedded_types = {bases_array};\n")
            out_lines.append("    cbm_registry_add_type(reg, rt);\n")

            # Also emit a registered method per class.method so registry
            # lookup_method(class_qn, name) works.
            for m in cls.methods:
                out_lines.append("    memset(&rf, 0, sizeof(rf));\n")
                out_lines.append(
                    f"    rf.qualified_name = \"{c_escape(cls.qualified_name)}.{c_escape(m)}\";\n"
                )
                out_lines.append(f"    rf.short_name = \"{c_escape(m)}\";\n")
                out_lines.append(
                    f"    rf.receiver_type = \"{c_escape(cls.qualified_name)}\";\n"
                )
                out_lines.append("    cbm_registry_add_func(reg, rf);\n")

        for fn in ms.functions:
            out_lines.append("    memset(&rf, 0, sizeof(rf));\n")
            out_lines.append(
                f"    rf.qualified_name = \"{c_escape(fn.qualified_name)}\";\n"
            )
            out_lines.append(f"    rf.short_name = \"{c_escape(fn.short_name)}\";\n")
            out_lines.append("    cbm_registry_add_func(reg, rf);\n")

    output_path.write_text(C_HEADER + "".join(out_lines) + C_FOOTER, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "stdlib_root",
        nargs="?",
        default="/tmp/python-lsp-references/typeshed/stdlib",
    )
    parser.add_argument(
        "output",
        nargs="?",
        default="internal/cbm/lsp/generated/python_stdlib_data.c",
    )
    args = parser.parse_args()

    root = Path(args.stdlib_root).resolve()
    if not root.is_dir():
        print(f"error: {root} is not a directory", file=sys.stderr)
        return 1

    stubs = list(iter_module_stubs(root))
    out_path = Path(args.output).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    emit(stubs, out_path)

    n_classes = sum(len(s.classes) for s in stubs)
    n_methods = sum(sum(len(c.methods) for c in s.classes) for s in stubs)
    n_funcs = sum(len(s.functions) for s in stubs)
    print(
        f"wrote {out_path}: {len(stubs)} modules, "
        f"{n_classes} classes ({n_methods} methods), {n_funcs} free functions"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
