#!/usr/bin/env python3
"""
memwaste-scaling.py — the CPU scaling lane of the waste sanitizer.

Work that grows faster than the input is the costliest CPU waste there is, and a
single run cannot see it: a function that burns 5 % of the time on the Go corpus
may burn 60 % on a corpus four times larger. This lane indexes the SAME real
source tree replicated K times and 2K times, with clang's execution counters in
every object (vendored ones included), and compares, function by function, how
much work each leg did. Linear work doubles; a function whose counters grow by
more than --threshold (default 2.3, i.e. 15 % above linear) while doing at least
--min-excess extra counter increments is reported as super-linear.

The counters are COUNTS, not time: the verdict is a function of the code and the
input, so it can gate (O9). Wall time is printed as information only.

Replicas are hard links (copies when the file system refuses) under
<out>/leg-<n>/r<i>/, so every replica is a separate directory -- a separate Go
package, a separate module -- and the registry, namespace and resolution paths
see N times the same names: exactly the coupling behind the #1669 class of bugs.

Usage:
    scripts/memwaste-scaling.py --corpus ~/perf-bench/go/src/net --replicas 2
    scripts/memwaste-scaling.py --corpus DIR --replicas 3 --no-build --json

Build: MEMWASTE_PROFILE=1 into build/memwaste-profile with clang (the driver does
it unless --no-build). llvm-profdata is taken from the directory of the clang
that built the binary, so the profile format always matches.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUILD_DIR = "build/memwaste-profile"


def find_profdata(cc: str) -> str:
    cc_path = shutil.which(cc)
    if cc_path:
        cand = os.path.join(os.path.dirname(os.path.realpath(cc_path)), "llvm-profdata")
        if os.access(cand, os.X_OK):
            return cand
    for name in ("llvm-profdata",) + tuple(f"llvm-profdata-{v}" for v in range(30, 13, -1)):
        p = shutil.which(name)
        if p:
            return p
    if shutil.which("xcrun"):
        res = subprocess.run(["xcrun", "--find", "llvm-profdata"], capture_output=True, text=True, check=False)
        if res.returncode == 0 and res.stdout.strip():
            return res.stdout.strip()
    sys.exit("memwaste-scaling: llvm-profdata not found next to the compiler or on PATH")


def replicate(src: str, dst: str, copies: int) -> int:
    """Hard-link `copies` replicas of src under dst/r<i>; returns the file count."""
    files = 0
    for i in range(copies):
        base = os.path.join(dst, f"r{i}")
        for dirpath, dirnames, filenames in os.walk(src):
            dirnames[:] = [d for d in dirnames if d not in (".git", ".hg", ".svn")]
            rel = os.path.relpath(dirpath, src)
            out_dir = os.path.join(base, rel)
            os.makedirs(out_dir, exist_ok=True)
            for name in filenames:
                s = os.path.join(dirpath, name)
                if os.path.islink(s) or not os.path.isfile(s):
                    continue
                d = os.path.join(out_dir, name)
                try:
                    os.link(s, d)
                except OSError:
                    shutil.copy2(s, d)
                files += 1
    return files


def run_leg(binary: str, profdata: str, corpus: str, work: str, copies: int) -> tuple[dict, dict]:
    leg = os.path.join(work, f"leg-{copies}")
    tree = os.path.join(leg, "corpus")
    os.makedirs(tree, exist_ok=True)
    nfiles = replicate(corpus, tree, copies)
    raw_dir = os.path.join(leg, "profraw")
    os.makedirs(raw_dir, exist_ok=True)
    runtime = tempfile.mkdtemp(prefix="cbm-sc.", dir="/tmp")
    os.chmod(runtime, 0o700)
    env = dict(os.environ)
    env.update({
        "LLVM_PROFILE_FILE": os.path.join(raw_dir, "cbm-%p.profraw"),
        "CBM_PROFILE_DIR": raw_dir,  # the index worker names its own: worker-<pid>.profraw
        "CBM_CACHE_DIR": os.path.join(leg, "cache"),
        "CBM_RUNTIME_DIR": runtime,
        "CBM_LOG_LEVEL": "error",
    })
    env.pop("CBM_MEMWASTE", None)  # production code paths: the event layer stays dormant
    t0 = time.monotonic()
    try:
        res = subprocess.run([binary, "cli", "index_repository", "--repo-path", tree, "--mode", "full"],
                             env=env, capture_output=True, text=True, check=False)
    finally:
        shutil.rmtree(runtime, ignore_errors=True)
    wall = time.monotonic() - t0
    if res.returncode != 0:
        sys.exit(f"memwaste-scaling: index of leg {copies} failed (rc={res.returncode}):\n{res.stderr[-2000:]}")
    raws = sorted(os.path.join(raw_dir, f) for f in os.listdir(raw_dir) if f.endswith(".profraw"))
    workers = [r for r in raws if os.path.basename(r).startswith("worker-")]
    if not workers:
        sys.exit("memwaste-scaling: the index worker wrote no profile -- is the binary built with "
                 "MEMWASTE_PROFILE=1, and does it call cbm_memev_process_exit before _Exit?")
    # The comparison is the WORKER's work: the daemon and CLI processes differ in
    # number between legs (and hash the binary at start-up), which is not scaling.
    info = {"copies": copies, "files": nfiles, "wall_s": round(wall, 1), "profiles": len(raws)}
    return merge_counts(profdata, workers, os.path.join(leg, "worker.profdata")), \
        dict(info, other=merge_counts(profdata, [r for r in raws if r not in workers],
                                      os.path.join(leg, "other.profdata")))


def merge_counts(profdata: str, raws: list[str], merged: str) -> dict[str, int]:
    if not raws:
        return {}
    subprocess.run([profdata, "merge", "-o", merged] + raws, check=True)
    shown = subprocess.run([profdata, "show", "--all-functions", "--counts", merged],
                           capture_output=True, text=True, check=True).stdout
    return parse_counts(shown)


FUNC_RE = re.compile(r"^  (\S.*):$")


def parse_counts(text: str) -> dict[str, int]:
    """Total counter increments per function from `llvm-profdata show --counts`."""
    out: dict[str, int] = {}
    name = None
    for line in text.splitlines():
        m = FUNC_RE.match(line)
        if m:
            name = m.group(1)
            continue
        if name is None:
            continue
        s = line.strip()
        if s.startswith("Function count:"):
            out[name] = out.get(name, 0) + int(s.split(":", 1)[1])
        elif s.startswith("Block counts:"):
            vals = s.split(":", 1)[1].strip().strip("[]")
            if vals:
                out[name] = out.get(name, 0) + sum(int(v) for v in vals.split(",") if v.strip())
    return out


def display_name(raw: str) -> str:
    # static functions are "file.c;name" (or "file.c:name" in older formats)
    for sep in (";", ":"):
        if sep in raw:
            file_part, fn = raw.rsplit(sep, 1)
            return f"{fn}  ({os.path.basename(file_part)})"
    return raw


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corpus", required=True, help="a real source tree to replicate")
    ap.add_argument("--replicas", type=int, default=2, help="K: legs index K and 2K replicas")
    ap.add_argument("--threshold", type=float, default=2.3, help="2K/K counter ratio that is super-linear")
    ap.add_argument("--min-excess", type=int, default=10_000_000,
                    help="ignore functions whose work beyond linear is below this many counter increments")
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--out", help="work directory (default: a new temp dir)")
    ap.add_argument("--no-build", action="store_true")
    ap.add_argument("--cc", default="clang")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    corpus = os.path.abspath(os.path.expanduser(args.corpus))
    if not os.path.isdir(corpus):
        sys.exit(f"memwaste-scaling: {corpus} is not a directory")
    binary = os.path.join(ROOT, BUILD_DIR, "codebase-memory-mcp")
    if not args.no_build:
        cxx = "clang++" if args.cc == "clang" else args.cc.replace("clang", "clang++")
        subprocess.run([os.path.join(ROOT, "scripts", "build.sh"), f"BUILD_DIR={BUILD_DIR}",
                        "MEMWASTE_PROFILE=1", f"CC={args.cc}", f"CXX={cxx}"],
                       check=True, stdout=subprocess.DEVNULL)
    if not os.access(binary, os.X_OK):
        sys.exit(f"memwaste-scaling: {binary} is missing (build it, or drop --no-build)")
    profdata = find_profdata(args.cc)
    work = os.path.abspath(args.out) if args.out else tempfile.mkdtemp(prefix="cbm-scaling.")
    os.makedirs(work, exist_ok=True)
    for sub in os.listdir(work):
        if sub.startswith("leg-"):
            shutil.rmtree(os.path.join(work, sub), ignore_errors=True)

    k = args.replicas
    small, info_k = run_leg(binary, profdata, corpus, work, k)
    large, info_2k = run_leg(binary, profdata, corpus, work, 2 * k)

    total_k = sum(small.values())
    total_2k = sum(large.values())
    rows = []
    for name, c2 in large.items():
        c1 = small.get(name, 0)
        excess = c2 - 2 * c1
        ratio = (c2 / c1) if c1 else float("inf")
        rows.append({"function": name, "k": c1, "2k": c2, "ratio": ratio, "excess": excess})
    flagged = [r for r in rows if r["ratio"] > args.threshold and r["excess"] >= args.min_excess]
    flagged.sort(key=lambda r: -r["excess"])
    heaviest = sorted(rows, key=lambda r: -r["2k"])[: args.top]

    for info in (info_k, info_2k):
        other = info.pop("other", {}) or {}
        info["other_total"] = sum(other.values())
        info["other_top"] = sorted(other.items(), key=lambda kv: -kv[1])[:10]
    report = {
        "corpus": corpus, "legs": [info_k, info_2k], "threshold": args.threshold,
        "min_excess": args.min_excess, "total_counts": {"k": total_k, "2k": total_2k},
        "total_ratio": (total_2k / total_k) if total_k else None,
        "superlinear": flagged,
        "work_dir": work,
    }
    if args.json:
        json.dump(report, sys.stdout, indent=1, default=lambda o: None if o == float("inf") else o)
        print()
        return 1 if flagged else 0

    print(f"corpus {corpus}")
    for info in (info_k, info_2k):
        print(f"  leg {info['copies']:>2} replicas: {info['files']:,} files, {info['profiles']} process profile(s), "
              f"{info['wall_s']} s wall (information only)")
    print(f"  worker counter increments: k={total_k:,}  2k={total_2k:,}  ratio={report['total_ratio']:.3f}")
    for info in (info_k, info_2k):
        other = info.get("other") or {}
        top = sorted(other.items(), key=lambda kv: -kv[1])[:3]
        print(f"  other processes at {info['copies']} replicas: {sum(other.values()):,} increments; heaviest: "
              + ", ".join(f"{display_name(n)} {c:,}" for n, c in top))
    print()
    print(f"  SUPER-LINEAR — ratio > {args.threshold} and >= {args.min_excess:,} increments beyond linear")
    if not flagged:
        print("    none")
    else:
        print(f"    {'ratio':>7} {'excess':>16} {'k':>16} {'2k':>16}  function")
        for r in flagged[: args.top]:
            ratio = "new" if r["ratio"] == float("inf") else f"{r['ratio']:.2f}"
            print(f"    {ratio:>7} {r['excess']:>16,} {r['k']:>16,} {r['2k']:>16,}  {display_name(r['function'])[:100]}")
    print()
    print(f"  HEAVIEST at 2k — top {args.top} by counter increments (share of all work, scaling ratio)")
    for r in heaviest:
        ratio = "new" if r["ratio"] == float("inf") else f"{r['ratio']:.2f}"
        print(f"    {100.0 * r['2k'] / total_2k:>6.2f}%  x{ratio:>5}  {display_name(r['function'])[:100]}")
    print()
    print(f"work dir: {work}")
    return 1 if flagged else 0


if __name__ == "__main__":
    sys.exit(main())
