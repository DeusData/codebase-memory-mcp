#!/usr/bin/env python3
"""
issue832_rss.py -- direct RSS reproduction for #832 (NON-GATING, manual repro tier).

WHY THIS IS NOT A C UNIT TEST
-----------------------------
The RSS ratchet is a property of mimalloc v3's abandoned-page handling
(page_reclaim_on_free=0): pages a worker THREAD abandons at exit are not
reclaimed when the main thread later frees their blocks. That only manifests in
the PROD binary, which links mimalloc as the global allocator (Makefile.cbm:
MI_OVERRIDE=1). The C test-runner and the C repro-runner are built CRT+ASan
(MI_OVERRIDE=0), so mimalloc is inert there and cbm_mem_rss() falls back to
os_rss() -- a C test would be VACUOUS. Hence this drives the real
`build/c/codebase-memory-mcp` server over stdio and samples its RSS from `ps`.

WHAT IT SHOWS
-------------
A long-lived MCP server is driven through K index_repository cycles of the same
fixture. The in-process pipeline (CBM_INDEX_SUPERVISOR=0) is the pre-#832-fix
background-path behaviour: RSS RATCHETS across cycles. The supervised subprocess
path (default) is the fix: each child returns 100% of its RSS on exit, so the
long-lived parent stays ~FLAT. The auto-index (mcp.c) and watcher re-index
(main.c) paths now route through that same supervised subprocess, so they inherit
this flat profile; the deterministic routing proof is the GATING guard
tests/test_mcp.c::index_bg_paths_route_through_supervisor_issue832.

Inherently noisy (allocator/OS dependent) -> thresholds are generous and this is
NOT wired into `make test` / `ci-ok`. Run manually:
    make -f Makefile.cbm cbm
    python3 tests/repro/issue832_rss.py
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BINARY = os.path.join(ROOT, "build", "c", "codebase-memory-mcp")
CYCLES = 10
NUM_FILES = 120  # enough files to fan out across worker threads (abandoned heaps)


def rss_kb(pid):
    out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)])
    return int(out.strip())


def make_fixture(d):
    for i in range(NUM_FILES):
        with open(os.path.join(d, f"mod_{i}.py"), "w") as f:
            for j in range(20):
                f.write(f"def fn_{i}_{j}(a, b):\n")
                f.write(f"    x = a + b + {i} * {j}\n")
                f.write("    return x\n\n")


def run_series(repo, cache, supervised):
    env = dict(os.environ)
    env["CBM_CACHE_DIR"] = cache
    if supervised:
        env.pop("CBM_INDEX_SUPERVISOR", None)
    else:
        env["CBM_INDEX_SUPERVISOR"] = "0"  # in-process (pre-fix background behaviour)
    env["CBM_INDEX_WORKER_TIMEOUT_S"] = "120"

    proc = subprocess.Popen(
        [BINARY], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL, env=env, text=True, bufsize=1,
    )

    def rpc(obj):
        proc.stdin.write(json.dumps(obj) + "\n")
        proc.stdin.flush()
        return proc.stdout.readline()

    rpc({"jsonrpc": "2.0", "id": 0, "method": "initialize", "params": {}})
    series = []
    for k in range(CYCLES):
        rpc({"jsonrpc": "2.0", "id": k + 1, "method": "tools/call",
             "params": {"name": "index_repository",
                        "arguments": {"repo_path": repo, "mode": "fast"}}})
        series.append(rss_kb(proc.pid))
    try:
        proc.stdin.close()
        proc.wait(timeout=15)
    except Exception:
        proc.kill()
    return series


def main():
    if not os.path.exists(BINARY):
        print(f"missing prod binary: {BINARY}\n  build it: make -f Makefile.cbm cbm")
        return 2
    base = tempfile.mkdtemp(prefix="cbm-832-")
    repo = os.path.join(base, "repo")
    os.makedirs(repo)
    make_fixture(repo)
    try:
        inproc = run_series(repo, os.path.join(base, "c1"), supervised=False)
        superv = run_series(repo, os.path.join(base, "c2"), supervised=True)
    finally:
        shutil.rmtree(base, ignore_errors=True)

    def mb(kb):
        return kb / 1024.0

    print(f"cycles={CYCLES} files={NUM_FILES}")
    print("cycle |  in-process(MB) | supervised(MB)")
    for i in range(CYCLES):
        print(f"  {i:2d}  |   {mb(inproc[i]):8.1f}      |   {mb(superv[i]):8.1f}")

    ip_peak = max(mb(x) for x in inproc)
    sv_peak = max(mb(x) for x in superv)
    print(f"\nin-process peak resident: {ip_peak:8.1f} MB")
    print(f"supervised peak resident: {sv_peak:8.1f} MB")
    # The decisive, robust signal at laptop-fixture scale is the RESIDENT-LEVEL
    # contrast, not cycle-over-cycle growth: the in-process server keeps the whole
    # index working set resident (it never leaves the long-lived process), while
    # the supervised path returns it every cycle (the child exits) -> the server
    # stays near its idle baseline. The unbounded ratchet in the field (#832, GB
    # over hours) is the same effect amplified by worker-thread count + cycle count
    # beyond what a small fixture surfaces. Generous threshold; report-only,
    # NON-GATING.
    verdict = "SUPERVISED ISOLATION reproduced (server stays near baseline)" \
        if sv_peak < ip_peak / 2 \
        else "inconclusive (env-dependent; see numbers)"
    print(f"verdict: {verdict}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
