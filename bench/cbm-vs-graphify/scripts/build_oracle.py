#!/usr/bin/env python3
"""Independent C oracle from `ctags -x` + ripgrep. No tool-under-test involved."""
import json, re, sys, subprocess, os
from collections import defaultdict, Counter

EVAL = "/private/tmp/claude-502/-Users-yf-jin-Desktop-03-Projects-codebase-memory-mcp-pro/fd757ee7-44e7-439c-ba81-f9b8dc43c4b4/scratchpad/eval"
CORPUS = f"{EVAL}/eval-corpus"
XFILE = f"{EVAL}/oracle_ctags_x.txt"

def norm_file(f):
    return f[2:] if f.startswith("./") else f

def classify(pattern):
    p = pattern.strip()
    if p.startswith("#define") or p.startswith("# define"):
        return "macro"
    if re.match(r'^\s*typedef\b', p):
        return "type"
    if re.match(r'^\s*(static\s+|const\s+)*(struct|union|enum)\b\s+\w+\s*\{?', p) or re.match(r'^\s*\}\s*\w+\s*;', p):
        return "type"
    # function definition: has a paren call-like signature, ends with { or ) and not a control kw
    if "(" in p and not p.lstrip().startswith(("if", "for", "while", "switch", "return")):
        # exclude obvious variable initializers like `int x = foo(...);`
        if re.search(r'\b\w+\s*\([^;]*\)\s*\{?\s*$', p) or p.rstrip().endswith("{"):
            return "function"
    return "other"

defs = []  # (name, file, line, kind, pattern)
with open(XFILE) as fh:
    for ln in fh:
        ln = ln.rstrip("\n")
        if not ln.strip():
            continue
        # name <pad> line file pattern...
        m = re.match(r'^(\S+)\s+(\d+)\s+(\S+)\s+(.*)$', ln)
        if not m:
            continue
        name, line, f, pat = m.group(1), int(m.group(2)), norm_file(m.group(3)), m.group(4)
        defs.append((name, f, line, classify(pat), pat))

KW = {"if", "for", "while", "switch", "return", "sizeof", "do", "else", "case"}
def is_real_func(n):
    return n not in KW and not n.startswith("__")
kinds = Counter(d[3] for d in defs)
functions = [(n, f, l) for (n, f, l, k, p) in defs if k == "function" and is_real_func(n)]
macros = [(n, f, l) for (n, f, l, k, p) in defs if k == "macro"]
types = [(n, f, l) for (n, f, l, k, p) in defs if k == "type"]

# include edges (exact, pure-python walk)
inc_re = re.compile(r'^\s*#\s*include\s*[<"]([^>"]+)[>"]')
include_edges = []
for root, _dirs, files in os.walk(CORPUS):
    if "/graphify-out" in root:
        continue
    for fn in files:
        if not fn.endswith((".c", ".h")):
            continue
        path = os.path.join(root, fn)
        src = os.path.relpath(path, CORPUS)
        try:
            with open(path, errors="ignore") as fh2:
                for line in fh2:
                    mm = inc_re.match(line)
                    if mm:
                        include_edges.append((src, mm.group(1)))
        except Exception:
            pass

oracle = {
    "corpus": CORPUS,
    "n_defs_total": len(defs),
    "kind_dist": dict(kinds),
    "functions": functions,
    "macros": macros,
    "types": types,
    "n_functions": len(functions),
    "n_macros": len(macros),
    "n_types": len(types),
    "include_edges": include_edges,
    "n_include_edges": len(include_edges),
}
with open(f"{EVAL}/oracle.json", "w") as fh:
    json.dump(oracle, fh)

print("=== ORACLE (independent, ctags -x + rg) ===")
print("total ctags defs:", len(defs))
print("kind classification:", dict(kinds))
print(f"functions={len(functions)} macros={len(macros)} types={len(types)}")
print(f"include edges (exact)={len(include_edges)}")
print("\nsample functions:", functions[:5])
print("sample macros:", macros[:3])
print("sample includes:", include_edges[:4])
