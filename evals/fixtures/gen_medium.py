#!/usr/bin/env python3
"""Generate `eval-medium`: a deterministic synthetic Java repository whose
graph shape is known in advance, for the scale, paging and index-plan evals.

    python3 evals/fixtures/gen_medium.py --out D:/cbm-evals/work/eval-medium \\
        [--files 20000] [--hub-callers 5000] [--seed 7]

Shape (all counts are exact and written to <out>/EXPECTED.json):
  - `files` Java classes in packages com.scale.p000 .. (100 classes per package).
  - Each class Cn has methods run(), helper() and step(int); run() calls helper()
    and step(); helper() calls the next class's run() (Cn -> Cn+1), so there is one
    long call chain and every run() has exactly one caller except C0.
  - com.scale.hub.Hub.hub() is called from the first `hub_callers` classes' step()
    method: one supernode with `hub_callers` inbound CALLS.
  - com.scale.dead.Dead has `dead_count` (= files // 100) methods nobody calls.
  - Every 1,000th class carries a docstring with the token EVALDOCTOKEN<k> so BM25
    search over docstrings has known hits.
"""
import argparse
import json
import os
import random
import sys


def generate(out, files=20000, hub_callers=5000, seed=7):
    rnd = random.Random(seed)
    os.makedirs(out, exist_ok=True)
    pkg_size = 100
    dead_count = max(1, files // 100)
    doc_every = 1000
    with open(os.path.join(out, "README.md"), "w", encoding="utf-8") as f:
        f.write("# eval-medium\n\nSynthetic Java repository for the codebase-memory-mcp evals.\n")
    with open(os.path.join(out, ".gitignore"), "w", encoding="utf-8") as f:
        f.write("target/\n")
    base = os.path.join(out, "src", "main", "java", "com", "scale")
    hub_dir = os.path.join(base, "hub")
    os.makedirs(hub_dir, exist_ok=True)
    with open(os.path.join(hub_dir, "Hub.java"), "w", encoding="utf-8") as f:
        f.write("package com.scale.hub;\n\npublic final class Hub {\n    private Hub() {}\n\n"
                "    /** The supernode: called by step() of the first hub_callers classes. */\n"
                "    public static int hub(int x) {\n        return x + 1;\n    }\n}\n")
    dead_dir = os.path.join(base, "dead")
    os.makedirs(dead_dir, exist_ok=True)
    with open(os.path.join(dead_dir, "Dead.java"), "w", encoding="utf-8") as f:
        f.write("package com.scale.dead;\n\npublic final class Dead {\n    private Dead() {}\n")
        for i in range(dead_count):
            f.write(f"\n    public static int dead{i:05d}(int x) {{\n        return x * {i + 2};\n    }}\n")
        f.write("}\n")
    doc_tokens = []
    for n in range(files):
        pkg = n // pkg_size
        pkg_dir = os.path.join(base, f"p{pkg:03d}")
        if n % pkg_size == 0:
            os.makedirs(pkg_dir, exist_ok=True)
        cls = f"C{n:05d}"
        nxt = f"C{n + 1:05d}"
        nxt_pkg = (n + 1) // pkg_size
        calls_hub = n < hub_callers
        doc = ""
        if n % doc_every == 0:
            tok = f"EVALDOCTOKEN{n // doc_every}"
            doc_tokens.append({"token": tok, "class": cls, "file": f"src/main/java/com/scale/p{pkg:03d}/{cls}.java"})
            doc = f"/** Generated class {cls}. Keyword {tok} marks it for BM25 search. */\n"
        body = [f"package com.scale.p{pkg:03d};\n"]
        if n + 1 < files and nxt_pkg != pkg:
            body.append(f"\nimport com.scale.p{nxt_pkg:03d}.{nxt};\n")
        if calls_hub:
            body.append("import com.scale.hub.Hub;\n")
        body.append(f"\n{doc}public class {cls} {{\n")
        body.append(f"    private int state = {rnd.randint(0, 1000)};\n\n")
        body.append("    public int run() {\n        helper();\n        return step(state);\n    }\n\n")
        if n + 1 < files:
            body.append(f"    void helper() {{\n        new {nxt}().run();\n    }}\n\n")
        else:
            body.append("    void helper() {\n        state++;\n    }\n\n")
        if calls_hub:
            body.append("    int step(int x) {\n        return Hub.hub(x);\n    }\n")
        else:
            body.append("    int step(int x) {\n        return x + 1;\n    }\n")
        body.append("}\n")
        with open(os.path.join(pkg_dir, f"{cls}.java"), "w", encoding="utf-8") as f:
            f.write("".join(body))
    expected = {
        "files_java": files + 2,
        "classes": files + 2,
        "methods_per_class": 3,
        "hub": {"qn_suffix": "com.scale.hub.Hub.hub", "inbound_calls": hub_callers},
        "chain": {"first": "C00000", "last": f"C{files - 1:05d}", "run_callers_each": 1},
        "dead_methods": dead_count,
        "doc_tokens": doc_tokens,
        "packages": (files + pkg_size - 1) // pkg_size + 2,
    }
    with open(os.path.join(out, "EXPECTED.json"), "w", encoding="utf-8") as f:
        json.dump(expected, f, indent=2)
    return {"files": files + 2, "hub_callers": hub_callers, "dead": dead_count}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--files", type=int, default=20000)
    ap.add_argument("--hub-callers", type=int, default=5000)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    print(generate(a.out, a.files, a.hub_callers, a.seed))
    return 0


if __name__ == "__main__":
    sys.exit(main())
