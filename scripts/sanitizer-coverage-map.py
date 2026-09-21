#!/usr/bin/env python3
"""
sanitizer-coverage-map.py — what our sanitizer lanes instrument, what they
actually execute, and every hole, vendored code included.

A sanitizer only checks code that runs under it. A function no suite reaches is
unchecked in every lane, and until now nobody could name those functions. This
tool answers three questions and keeps the answers from regressing:

  static    Which translation units does each lane COMPILE with its sanitizer,
            per the build's own compile commands (make -n)? Lists every TU no
            dynamic lane instruments and every TU the static lanes filter out.
  run       Build one lane with clang source coverage on top of its sanitizer
            flags, run its test runner (suites optional), and export per-function
            execution counts (lcov) -- vendored objects included, because the
            flags ride on the lane's own sanitizer variable, which the vendored
            rules already use.
  union     Merge the per-lane exports: the functions that ran under NO lane,
            grouped by file, vendored code flagged.
  ratchet   Compare a union against the checked-in baseline: a function that
            joins the never-executed list fails; one that leaves it is reported
            so the baseline can be lowered.
  map       Validate and print scripts/sanitizer-map.json: every defect class x
            platform cell must be covered, partial, or a named limit with a reason.

`run` executes test suites, so it belongs to the lane's own venue (local CI, the
CI job); `static`, `union`, `ratchet` and `map` run anywhere in seconds.

Usage:
    scripts/sanitizer-coverage-map.py static
    scripts/sanitizer-coverage-map.py run --lane asan --out build/covmap [SUITE...]
    scripts/sanitizer-coverage-map.py union --out build/covmap
    scripts/sanitizer-coverage-map.py ratchet --out build/covmap
    scripts/sanitizer-coverage-map.py map
"""
from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASELINE = os.path.join(ROOT, "scripts", "sanitizer-coverage-baseline.txt")
MAP_FILE = os.path.join(ROOT, "scripts", "sanitizer-map.json")
COVERAGE_FLAGS = "-fprofile-instr-generate -fcoverage-mapping"
SRC_EXT = (".c", ".cc", ".cpp")

# lane -> how the lane builds and runs. `var` is the make variable carrying the
# lane's sanitizer flags; the coverage flags are appended to it.
LANES = {
    "asan": {"target": "test-runner", "var": "SANITIZE",
             "value": "-fsanitize=address,undefined -fno-omit-frame-pointer", "env": {}},
    "tsan": {"target": "test-runner-tsan", "var": "TSAN_SANITIZE",
             "value": "-fsanitize=thread -fno-omit-frame-pointer",
             "env": {"TSAN_OPTIONS": "halt_on_error=1:report_thread_leaks=0"}},
    "lsan": {"target": "test-runner", "var": "SANITIZE",
             "value": "-fsanitize=address,undefined -fno-omit-frame-pointer",
             "env": {"ASAN_OPTIONS": "detect_leaks=1:halt_on_error=1"}},
}
DYNAMIC_FLAGS = ("-fsanitize=address", "-fsanitize=thread", "-fsanitize=memory", "-fsanitize=undefined",
                 "-fsanitize=address,undefined")


def is_vendored(path: str) -> bool:
    # grammar_<lang>.c and the runtime/unity wrappers only #include vendored sources
    base = os.path.basename(path)
    return "vendored/" in path or base.startswith(("grammar_", "ts_runtime")) or base in (
        "sqlite3.c", "mimalloc.c", "unixcoder_blob.c")


def dry_commands(target: str, extra: list[str]) -> list[list[str]]:
    res = subprocess.run(["make", "-f", "Makefile.cbm", "-n", "-B", target] + extra,
                         cwd=ROOT, capture_output=True, text=True, check=False)
    cmds = []
    for line in res.stdout.replace("\\\n", " ").splitlines():
        try:
            argv = shlex.split(line)
        except ValueError:
            continue
        if argv and any(a.endswith(SRC_EXT) for a in argv) and ("-c" in argv or "-o" in argv):
            cmds.append(argv)
    return cmds


def make_var(name: str) -> list[str]:
    """The fully expanded value of a Makefile.cbm variable (works with make 3.81: no --eval)."""
    with tempfile.NamedTemporaryFile("w", suffix=".mk", delete=False) as mk:
        mk.write("print-cbm-var-%:\n\t@echo $($*)\n")
        helper = mk.name
    try:
        res = subprocess.run(["make", "-s", "-f", "Makefile.cbm", "-f", helper, f"print-cbm-var-{name}"],
                             cwd=ROOT, capture_output=True, text=True, check=False)
    finally:
        os.unlink(helper)
    return res.stdout.split() if res.returncode == 0 else []


def cmd_static(args) -> int:
    instrumented: dict[str, set[str]] = defaultdict(set)
    compiled: set[str] = set()
    for lane, spec in {"asan": LANES["asan"], "tsan": LANES["tsan"]}.items():
        bdir = f"build/covmap-static-{lane}"
        for argv in dry_commands(f"{bdir}/{spec['target']}", [f"BUILD_DIR={bdir}"]):
            flags = " ".join(argv)
            for a in argv:
                if a.endswith(SRC_EXT) and os.path.isfile(os.path.join(ROOT, a)):
                    compiled.add(a)
                    if any(f in flags for f in ("-fsanitize=address", "-fsanitize=thread")):
                        instrumented[a].add(lane)
    uninstrumented = sorted(s for s in compiled if not instrumented.get(s))
    lint_srcs = set(make_var("LINT_SRCS"))
    static_skipped = sorted(s for s in compiled if s not in lint_srcs)
    out = {
        "translation_units": len(compiled),
        "not_instrumented_by_any_dynamic_lane": uninstrumented,
        "not_in_static_lanes": static_skipped,
        "per_tu": {s: sorted(instrumented.get(s, ())) for s in sorted(compiled)},
    }
    if args.json:
        json.dump(out, sys.stdout, indent=1)
        print()
        return 0
    print(f"translation units in the test builds: {len(compiled)}")
    print(f"  instrumented by ASan+UBSan: {sum(1 for v in instrumented.values() if 'asan' in v)}"
          f"   by TSan: {sum(1 for v in instrumented.values() if 'tsan' in v)}")
    print()
    print(f"  NOT instrumented by any dynamic lane ({len(uninstrumented)}):")
    for s in uninstrumented:
        print(f"    {s}{'  [vendored]' if is_vendored(s) else ''}")
    print()
    print(f"  outside the static lanes' source list (cppcheck, clang-analyzer) ({len(static_skipped)}):")
    vend = [s for s in static_skipped if is_vendored(s)]
    own = [s for s in static_skipped if not is_vendored(s) and not s.startswith("tests/")]
    print(f"    vendored: {len(vend)}   project: {len(own)}")
    for s in own:
        print(f"    {s}")
    return 0


def find_llvm_tool(name: str, cc: str) -> str:
    cc_path = shutil.which(cc)
    if cc_path:
        cand = os.path.join(os.path.dirname(os.path.realpath(cc_path)), name)
        if os.access(cand, os.X_OK):
            return cand
    p = shutil.which(name)
    if p:
        return p
    if shutil.which("xcrun"):
        res = subprocess.run(["xcrun", "--find", name], capture_output=True, text=True, check=False)
        if res.returncode == 0:
            return res.stdout.strip()
    sys.exit(f"sanitizer-coverage-map: {name} not found next to {cc} or on PATH")


def cmd_run(args) -> int:
    spec = LANES[args.lane]
    out = os.path.abspath(args.out)
    os.makedirs(out, exist_ok=True)
    bdir = f"build/covmap-{args.lane}"
    cxx = "clang++" if args.cc == "clang" else args.cc.replace("clang", "clang++")
    flags = f"{spec['value']} {COVERAGE_FLAGS}"
    subprocess.run(["make", "-f", "Makefile.cbm", f"-j{os.cpu_count() or 4}", f"{bdir}/{spec['target']}",
                    f"BUILD_DIR={bdir}", f"CC={args.cc}", f"CXX={cxx}", f"{spec['var']}={flags}"],
                   cwd=ROOT, check=True)
    raw_dir = os.path.join(out, f"{args.lane}-profraw")
    shutil.rmtree(raw_dir, ignore_errors=True)
    os.makedirs(raw_dir)
    env = dict(os.environ)
    env.update(spec["env"])
    env["LLVM_PROFILE_FILE"] = os.path.join(raw_dir, "%p-%m.profraw")
    runner = os.path.join(ROOT, bdir, spec["target"])
    rc = subprocess.run([runner] + args.suites, cwd=ROOT, env=env, check=False).returncode
    raws = [os.path.join(raw_dir, f) for f in os.listdir(raw_dir) if f.endswith(".profraw")]
    if not raws:
        sys.exit("sanitizer-coverage-map: the runner wrote no profile")
    profdata = find_llvm_tool("llvm-profdata", args.cc)
    cov = find_llvm_tool("llvm-cov", args.cc)
    merged = os.path.join(out, f"{args.lane}.profdata")
    subprocess.run([profdata, "merge", "-sparse", "-o", merged] + raws, check=True)
    with open(os.path.join(out, f"{args.lane}.lcov"), "w") as fh:
        subprocess.run([cov, "export", "-format=lcov", "-skip-expansions", f"-instr-profile={merged}", runner],
                       cwd=ROOT, stdout=fh, check=True)
    print(f"{args.lane}: runner rc={rc}, {len(raws)} profile(s) -> {out}/{args.lane}.lcov")
    return 0 if rc == 0 else rc


def read_lcov(path: str) -> dict[tuple[str, str], int]:
    counts: dict[tuple[str, str], int] = {}
    src = None
    for line in open(path, encoding="utf-8", errors="replace"):
        if line.startswith("SF:"):
            src = os.path.relpath(line[3:].strip(), ROOT)
        elif line.startswith("FNDA:") and src:
            count, name = line[5:].strip().split(",", 1)
            key = (src, name)
            counts[key] = max(counts.get(key, 0), int(count))
    return counts


def cmd_union(args) -> int:
    out = os.path.abspath(args.out)
    lanes = [f[:-5] for f in sorted(os.listdir(out)) if f.endswith(".lcov")]
    if not lanes:
        sys.exit(f"sanitizer-coverage-map: no <lane>.lcov under {out} (run `run` first)")
    total: dict[tuple[str, str], int] = {}
    for lane in lanes:
        for key, c in read_lcov(os.path.join(out, f"{lane}.lcov")).items():
            total[key] = max(total.get(key, 0), c)
    never = sorted(k for k, c in total.items() if c == 0 and not k[0].startswith(("tests/", "..")))
    with open(os.path.join(out, "never-executed.txt"), "w") as fh:
        for src, fn in never:
            fh.write(f"{src}\t{fn}\n")
    by_file = defaultdict(int)
    for src, _ in never:
        by_file[src] += 1
    functions = sum(1 for k in total if not k[0].startswith(("tests/", "..")))
    print(f"lanes merged: {', '.join(lanes)}")
    print(f"functions: {functions:,}   never executed under any lane: {len(never):,} "
          f"(vendored: {sum(1 for s, _ in never if is_vendored(s)):,})")
    for src, n in sorted(by_file.items(), key=lambda kv: (-kv[1], kv[0]))[: args.top]:
        print(f"  {n:>6}  {src}{'  [vendored]' if is_vendored(src) else ''}")
    print(f"list: {out}/never-executed.txt")
    return 0


def cmd_ratchet(args) -> int:
    current_path = os.path.join(os.path.abspath(args.out), "never-executed.txt")
    current = set(open(current_path).read().splitlines())
    if not os.path.exists(BASELINE):
        print(f"no baseline yet: copy {current_path} to {BASELINE} to arm the ratchet")
        return 0
    base = set(l for l in open(BASELINE).read().splitlines() if l and not l.startswith("#"))
    new = sorted(current - base)
    gone = sorted(base - current)
    for line in new:
        print(f"NEW never-executed: {line}")
    if gone:
        print(f"{len(gone)} function(s) now executed: lower the baseline (remove them from {BASELINE})")
    return 1 if new else 0


def cmd_map(args) -> int:
    data = json.load(open(MAP_FILE))
    platforms = data["platforms"]
    bad = []
    for cls in data["classes"]:
        cells = cls["cells"]
        for plat in platforms:
            cell = cells.get(plat)
            if not cell:
                bad.append(f"{cls['name']} / {plat}: missing")
            elif cell.get("status") not in ("covered", "partial", "limit"):
                bad.append(f"{cls['name']} / {plat}: status must be covered, partial or limit")
            elif cell["status"] != "covered" and not cell.get("why"):
                bad.append(f"{cls['name']} / {plat}: {cell['status']} needs a why")
    if not args.quiet:
        width = max(len(c["name"]) for c in data["classes"])
        print(f"{'defect class':<{width}}  " + "  ".join(f"{p:<14}" for p in platforms))
        for cls in data["classes"]:
            row = []
            for plat in platforms:
                cell = cls["cells"].get(plat, {})
                row.append(f"{cell.get('status', '?') + ':' + cell.get('lane', ''):<14}"[:14])
            print(f"{cls['name']:<{width}}  " + "  ".join(row))
        print()
        for cls in data["classes"]:
            for plat in platforms:
                cell = cls["cells"].get(plat, {})
                if cell.get("status") in ("partial", "limit"):
                    print(f"  {cell['status']:<7} {cls['name']} / {plat}: {cell.get('why', '')}")
    for b in bad:
        print(f"MAP DEFECT: {b}", file=sys.stderr)
    return 1 if bad else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("static")
    s.add_argument("--json", action="store_true")
    r = sub.add_parser("run")
    r.add_argument("--lane", choices=sorted(LANES), required=True)
    r.add_argument("--out", default="build/covmap")
    r.add_argument("--cc", default="clang")
    r.add_argument("suites", nargs="*")
    u = sub.add_parser("union")
    u.add_argument("--out", default="build/covmap")
    u.add_argument("--top", type=int, default=40)
    t = sub.add_parser("ratchet")
    t.add_argument("--out", default="build/covmap")
    m = sub.add_parser("map")
    m.add_argument("--quiet", action="store_true")
    args = ap.parse_args()
    return {"static": cmd_static, "run": cmd_run, "union": cmd_union,
            "ratchet": cmd_ratchet, "map": cmd_map}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
