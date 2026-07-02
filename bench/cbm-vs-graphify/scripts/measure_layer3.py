#!/usr/bin/env python3
"""Layer 3 — operational cost. cold-build time (median of 3), disk, query latency, incremental."""
import json, os, subprocess, time, shutil, statistics

EVAL = "/private/tmp/claude-502/-Users-yf-jin-Desktop-03-Projects-codebase-memory-mcp-pro/fd757ee7-44e7-439c-ba81-f9b8dc43c4b4/scratchpad/eval"
CORPUS = f"{EVAL}/eval-corpus"
CBM = "/Users/yf.jin/.local/bin/codebase-memory-mcp"
GFY = "/Users/yf.jin/.local/bin/graphify"
PROJ = "private-tmp-claude-502-Users-yf-jin-Desktop-03-Projects-codebase-memory-mcp-pro-fd757ee7-44e7-439c-ba81-f9b8dc43c4b4-scratchpad-eval-eval-corpus"

def t(): return time.time()
def med(xs): return round(statistics.median(xs), 2)

# ---------- cold build: cbm ----------
cbm_times = []
cache = f"{EVAL}/cbm-l3-cache"
for i in range(3):
    shutil.rmtree(cache, ignore_errors=True); os.makedirs(cache)
    env = dict(os.environ, CBM_CACHE_DIR=cache)
    a = t()
    subprocess.run([CBM, "cli", "--json", "index_repository", json.dumps({"repo_path": CORPUS, "mode": "full"})],
                   capture_output=True, text=True, env=env)
    cbm_times.append(t() - a)
# cbm db size
dbs = [f for f in os.listdir(cache) if f.endswith(".db")]
cbm_db_bytes = os.path.getsize(os.path.join(cache, dbs[0])) if dbs else 0

# ---------- cold build: graphify-AST ----------
gfy_times = []
for i in range(3):
    shutil.rmtree(f"{CORPUS}/graphify-out", ignore_errors=True)
    a = t()
    subprocess.run([GFY, "update", CORPUS], capture_output=True, text=True, cwd=CORPUS)
    gfy_times.append(t() - a)
def dir_size(p):
    tot = 0
    for r, _d, fs in os.walk(p):
        for f in fs:
            try: tot += os.path.getsize(os.path.join(r, f))
            except Exception: pass
    return tot
gfy_out_bytes = dir_size(f"{CORPUS}/graphify-out")
gfy_graphjson_bytes = os.path.getsize(f"{CORPUS}/graphify-out/graph.json")

# ---------- query latency ----------
env = dict(os.environ, CBM_CACHE_DIR=cache)
cbm_q = []
for i in range(3):
    a = t()
    subprocess.run([CBM, "cli", "--json", "query_graph",
                    json.dumps({"project": PROJ, "query": "MATCH (a)-[:CALLS]->(b) WHERE b.name='cbm_arena_alloc' RETURN a.name"})],
                   capture_output=True, text=True, env=env)
    cbm_q.append((t() - a) * 1000)
gfy_q = []
for i in range(3):
    a = t()
    subprocess.run([GFY, "query", "what calls cbm_arena_alloc", "--graph", f"{CORPUS}/graphify-out/graph.json", "--budget", "800"],
                   capture_output=True, text=True)
    gfy_q.append((t() - a) * 1000)

# ---------- incremental update ----------
victim = f"{CORPUS}/src/simhash/minhash.c"
backup = open(victim).read()
with open(victim, "a") as fh:
    fh.write("\n// eval touch\n")
a = t()
subprocess.run([GFY, "update", CORPUS], capture_output=True, text=True, cwd=CORPUS)
gfy_incr = t() - a
# cbm re-index cost after a 1-file change (cold full reindex is the comparator in this CLI harness)
a = t()
env2 = dict(os.environ, CBM_CACHE_DIR=cache)
subprocess.run([CBM, "cli", "--json", "index_repository", json.dumps({"repo_path": CORPUS, "mode": "full"})],
               capture_output=True, text=True, env=env2)
cbm_reindex = t() - a
# restore the touched file exactly
with open(victim, "w") as fh:
    fh.write(backup)

res = {
    "cold_build_s": {"cbm_median": med(cbm_times), "cbm_runs": [round(x, 2) for x in cbm_times],
                      "graphify_ast_median": med(gfy_times), "graphify_ast_runs": [round(x, 2) for x in gfy_times]},
    "disk_bytes": {"cbm_db": cbm_db_bytes, "graphify_out_total": gfy_out_bytes, "graphify_graph_json": gfy_graphjson_bytes},
    "query_latency_ms": {"cbm_median": med(cbm_q), "graphify_median": med(gfy_q)},
    "incremental_s": {"graphify_update": round(gfy_incr, 2), "cbm_reindex_full": round(cbm_reindex, 2)},
    "token_cost": {"cbm_index": 0, "graphify_ast": 0,
                   "graphify_full_subset_llm_naming_wallclock_s": 705.9,
                   "note": "cbm uses local nomic embeddings (0 API tokens); graphify-AST is deterministic (0 tokens); graphify-FULL semantic/community-naming uses an LLM backend (here local ollama gemma4:26b = 0 API $ but 11.8 min wall-clock for the 16K-LOC subset)."},
}
json.dump(res, open(f"{EVAL}/layer3_results.json", "w"), indent=2)
print(json.dumps(res, indent=2))
print("\nMB: cbm_db=%.1f  graphify_out=%.1f  graphify_json=%.1f" %
      (cbm_db_bytes/1e6, gfy_out_bytes/1e6, gfy_graphjson_bytes/1e6))
