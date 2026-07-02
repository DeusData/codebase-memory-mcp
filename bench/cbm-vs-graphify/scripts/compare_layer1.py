#!/usr/bin/env python3
"""Layer 1 — structural extraction accuracy. cbm vs graphify-AST vs independent oracle."""
import json, re, os, subprocess
from collections import defaultdict, Counter

EVAL = "/private/tmp/claude-502/-Users-yf-jin-Desktop-03-Projects-codebase-memory-mcp-pro/fd757ee7-44e7-439c-ba81-f9b8dc43c4b4/scratchpad/eval"
CORPUS = f"{EVAL}/eval-corpus"
CBM = "/Users/yf.jin/.local/bin/codebase-memory-mcp"
CBM_CACHE = f"{EVAL}/cbm-cache-smoke"
PROJ = "private-tmp-claude-502-Users-yf-jin-Desktop-03-Projects-codebase-memory-mcp-pro-fd757ee7-44e7-439c-ba81-f9b8dc43c4b4-scratchpad-eval-eval-corpus"
GJSON = f"{CORPUS}/graphify-out/graph.json"

oracle = json.load(open(f"{EVAL}/oracle.json"))

# corpus file set (relative paths + basenames) for include resolution
corpus_files = set()
basename_to_rel = defaultdict(list)
for root, _d, files in os.walk(CORPUS):
    if "/graphify-out" in root:
        continue
    for fn in files:
        if fn.endswith((".c", ".h")):
            rel = os.path.relpath(os.path.join(root, fn), CORPUS)
            corpus_files.add(rel)
            basename_to_rel[fn].append(rel)

def cbm_query(q):
    env = dict(os.environ, CBM_CACHE_DIR=CBM_CACHE)
    out = subprocess.run([CBM, "cli", "--json", "query_graph",
                          json.dumps({"project": PROJ, "query": q})],
                         capture_output=True, text=True, env=env).stdout
    try:
        inner = json.loads(out)["content"][0]["text"]
        return json.loads(inner).get("rows", [])
    except Exception as e:
        return []

# ---------- cbm graph ----------
def _i(x):
    try: return int(x)
    except Exception: return 0
cbm_nodes = cbm_query("MATCH (n) RETURN n.label, count(*)")
cbm_label_dist = {r[0]: _i(r[1]) for r in cbm_nodes}
cbm_edges = {r[0]: _i(r[1]) for r in cbm_query("MATCH ()-[r]->() RETURN type(r), count(*)")}
cbm_funcs_rows = cbm_query("MATCH (n:Function) RETURN n.name, n.file_path")
cbm_macros_rows = cbm_query("MATCH (n:Macro) RETURN n.name, n.file_path")
cbm_all_rows = cbm_query("MATCH (n) RETURN n.name, n.file_path, n.label")
# dup nodes: same (name,file) emitted as both Method and Function
byk = defaultdict(set)
for nm, f, lab in cbm_all_rows:
    if nm:
        byk[(nm, f)].add(lab)
cbm_dups = sum(1 for k, s in byk.items() if "Method" in s and "Function" in s)
cbm_funcs = set((r[0], r[1]) for r in cbm_funcs_rows if r[0] and r[1])
cbm_macros = set((r[0], r[1]) for r in cbm_macros_rows if r[0] and r[1])
cbm_any = set((r[0], r[1]) for r in cbm_all_rows if r[0] and r[1])
# cbm includes (IMPORTS edges) src->target file
cbm_imports = cbm_query("MATCH (a)-[:IMPORTS]->(b) RETURN a.file_path, b.file_path, b.name")

# ---------- graphify graph ----------
g = json.load(open(GJSON))
gnodes = g["nodes"]; glinks = g.get("links", [])
gfy_funcs = set()      # (name, file)
gfy_any = set()        # (name, file) any node
id_to_node = {}
for n in gnodes:
    lab = (n.get("label") or "")
    sf = n.get("source_file") or ""
    id_to_node[n["id"]] = n
    name = lab[:-2] if lab.endswith("()") else lab
    if sf:
        gfy_any.add((name, sf))
        if lab.endswith("()"):
            gfy_funcs.add((name, sf))
gfy_rel = Counter(l.get("relation", "?") for l in glinks)
# graphify includes: relation == 'imports'
gfy_imports = [(l["source"], l["target"]) for l in glinks if l.get("relation") == "imports"]

# ---------- oracle sets ----------
oracle_funcs = set((n, f) for (n, f, l) in oracle["functions"])
oracle_funcs_names = set(n for (n, f, l) in oracle["functions"])

def prf(tool_set, gold_set):
    tp = len(tool_set & gold_set)
    rec = tp / len(gold_set) if gold_set else 0
    prec = tp / len(tool_set) if tool_set else 0
    return tp, rec, prec

# function-definition recall (strict: function-typed node at name+file)
cbm_tp, cbm_rec, cbm_prec = prf(cbm_funcs, oracle_funcs)
gfy_tp, gfy_rec, gfy_prec = prf(gfy_funcs, oracle_funcs)
# loose recall: oracle function present as ANY node at (name,file)
cbm_tp_l, cbm_rec_l, _ = prf(cbm_any, oracle_funcs)
gfy_tp_l, gfy_rec_l, _ = prf(gfy_any, oracle_funcs)
# name-only recall (file-agnostic): does the tool know this function name anywhere?
cbm_names = set(n for (n, f) in cbm_any)
gfy_names = set(n for (n, f) in gfy_any)
cbm_name_rec = len(oracle_funcs_names & cbm_names) / len(oracle_funcs_names)
gfy_name_rec = len(oracle_funcs_names & gfy_names) / len(oracle_funcs_names)

# ---------- include-edge recall (in-corpus targets only) ----------
def resolve_inc(inc):
    base = os.path.basename(inc)
    if inc in corpus_files:
        return inc
    if base in basename_to_rel and len(basename_to_rel[base]) == 1:
        return basename_to_rel[base][0]
    # try suffix match
    for rel in corpus_files:
        if rel.endswith("/" + inc) or rel == inc:
            return rel
    return None

oracle_inc_incorpus = set()
for src, inc in oracle["include_edges"]:
    tgt = resolve_inc(inc)
    if tgt:
        oracle_inc_incorpus.add((src, tgt))

# cbm include edges as (src, tgt) by file_path
cbm_inc_set = set()
for r in cbm_imports:
    if r[0] and r[1]:
        cbm_inc_set.add((r[0], r[1]))
# graphify imports: source/target are node ids; resolve to source_file
gfy_inc_set = set()
for s, t in gfy_imports:
    sn = id_to_node.get(s); tn = id_to_node.get(t)
    if sn and tn and sn.get("source_file") and tn.get("source_file"):
        gfy_inc_set.add((sn["source_file"], tn["source_file"]))

cbm_inc_tp, cbm_inc_rec, cbm_inc_prec = prf(cbm_inc_set, oracle_inc_incorpus)
gfy_inc_tp, gfy_inc_rec, gfy_inc_prec = prf(gfy_inc_set, oracle_inc_incorpus)

# ---------- call-edge parity (top callees by fan-in) + grep upper bound ----------
def grep_callers(sym):
    # count call-sites (noisy upper bound): SYM( not preceded by ident char, excluding the def line is approximate
    cnt = 0
    pat = re.compile(r'(^|[^A-Za-z0-9_])' + re.escape(sym) + r'\s*\(')
    for rel in corpus_files:
        try:
            with open(os.path.join(CORPUS, rel), errors="ignore") as fh:
                for line in fh:
                    if pat.search(line):
                        cnt += 1
        except Exception:
            pass
    return cnt

cbm_top = cbm_query("MATCH (a)-[:CALLS]->(b) RETURN b.name, count(a) AS n ORDER BY n DESC LIMIT 8")
# graphify caller counts
gfy_call_targets = Counter()
for l in glinks:
    if l.get("relation") == "calls":
        tn = id_to_node.get(l["target"])
        if tn:
            nm = (tn.get("label") or "")
            nm = nm[:-2] if nm.endswith("()") else nm
            gfy_call_targets[nm] += 1

call_parity = []
for nm, cbm_n in cbm_top:
    short = nm.split(".")[-1] if nm else nm
    gfy_n = gfy_call_targets.get(short, 0)
    grep_n = grep_callers(short)
    call_parity.append({"sym": short, "cbm": _i(cbm_n), "gfy": gfy_n, "grep_ub": grep_n})

# ---------- output ----------
res = {
    "structural": {
        "cbm": {"nodes": sum(cbm_label_dist.values()), "node_labels": cbm_label_dist,
                "edges": sum(cbm_edges.values()), "edge_types": cbm_edges,
                "dup_nodes": cbm_dups, "kind_richness": len(cbm_label_dist)},
        "graphify_ast": {"nodes": len(gnodes), "links": len(glinks),
                          "relations": dict(gfy_rel), "kind_richness": 1},
    },
    "function_recall": {
        "oracle_functions": len(oracle_funcs),
        "cbm": {"function_nodes": len(cbm_funcs), "tp": cbm_tp, "recall_strict": round(cbm_rec, 3),
                "precision": round(cbm_prec, 3), "recall_loose_anynode": round(cbm_rec_l, 3),
                "recall_name_only": round(cbm_name_rec, 3)},
        "graphify": {"function_nodes": len(gfy_funcs), "tp": gfy_tp, "recall_strict": round(gfy_rec, 3),
                     "precision": round(gfy_prec, 3), "recall_loose_anynode": round(gfy_rec_l, 3),
                     "recall_name_only": round(gfy_name_rec, 3)},
    },
    "include_recall": {
        "oracle_include_edges_incorpus": len(oracle_inc_incorpus),
        "cbm": {"edges": len(cbm_inc_set), "tp": cbm_inc_tp, "recall": round(cbm_inc_rec, 3), "precision": round(cbm_inc_prec, 3)},
        "graphify": {"edges": len(gfy_inc_set), "tp": gfy_inc_tp, "recall": round(gfy_inc_rec, 3), "precision": round(gfy_inc_prec, 3)},
    },
    "call_parity": call_parity,
}
json.dump(res, open(f"{EVAL}/layer1_results.json", "w"), indent=2)

print("======== LAYER 1: STRUCTURAL EXTRACTION ACCURACY ========\n")
print("-- Structural stats --")
print(f"  cbm          : nodes={res['structural']['cbm']['nodes']:5} edges={res['structural']['cbm']['edges']:5} "
      f"kinds={res['structural']['cbm']['kind_richness']} dup_nodes={cbm_dups}")
print(f"  graphify-AST : nodes={len(gnodes):5} links={len(glinks):5} kinds=1(flat) communities given")
print(f"  cbm node labels: {dict(Counter(cbm_label_dist).most_common(8))}")
print(f"  cbm edge types : {dict(Counter(cbm_edges).most_common(8))}")
print(f"  gfy relations  : {dict(gfy_rel)}")
print()
print("-- Function-definition recall/precision (gold = ctags, N=%d) --" % len(oracle_funcs))
for tool, d in [("cbm", res["function_recall"]["cbm"]), ("graphify", res["function_recall"]["graphify"])]:
    print(f"  {tool:9}: fn_nodes={d['function_nodes']:5}  recall_strict={d['recall_strict']:.3f}  "
          f"recall_loose={d['recall_loose_anynode']:.3f}  recall_name={d['recall_name_only']:.3f}  precision={d['precision']:.3f}")
print()
print("-- Include-edge recall (gold = in-corpus #include, N=%d) --" % len(oracle_inc_incorpus))
for tool, d in [("cbm", res["include_recall"]["cbm"]), ("graphify", res["include_recall"]["graphify"])]:
    print(f"  {tool:9}: edges={d['edges']:5}  recall={d['recall']:.3f}  precision={d['precision']:.3f}")
print()
print("-- Call-graph parity (callers; grep = noisy upper bound) --")
print(f"  {'symbol':28} {'cbm':>5} {'gfy':>5} {'grep_ub':>8}")
for c in call_parity:
    print(f"  {c['sym']:28} {c['cbm']:>5} {c['gfy']:>5} {c['grep_ub']:>8}")
