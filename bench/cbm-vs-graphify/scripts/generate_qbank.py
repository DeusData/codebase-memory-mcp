#!/usr/bin/env python3
"""Generate oracle-seeded D1-D5 question bank with machine-checkable ground truth."""
import json, os, re
from collections import defaultdict

EVAL = "/private/tmp/claude-502/-Users-yf-jin-Desktop-03-Projects-codebase-memory-mcp-pro/fd757ee7-44e7-439c-ba81-f9b8dc43c4b4/scratchpad/eval"
CORPUS = f"{EVAL}/eval-corpus"
oracle = json.load(open(f"{EVAL}/oracle.json"))

funcs_by_file = defaultdict(list)
func_loc = {}
for n, f, l in oracle["functions"]:
    funcs_by_file[f].append(n)
    func_loc[n] = (f, l)

def sig_of(name):
    f, l = func_loc[name]
    try:
        lines = open(os.path.join(CORPUS, f), errors="ignore").read().splitlines()
        return lines[l-1].strip()
    except Exception:
        return ""

def callers_of(sym):
    """files+lines that reference SYM( as a call (noisy upper bound, excludes the def line)."""
    pat = re.compile(r'(^|[^A-Za-z0-9_])' + re.escape(sym) + r'\s*\(')
    deff, defl = func_loc.get(sym, (None, None))
    hits = defaultdict(int)
    for root, _d, files in os.walk(CORPUS):
        if "/graphify-out" in root:
            continue
        for fn in files:
            if not fn.endswith((".c", ".h")):
                continue
            rel = os.path.relpath(os.path.join(root, fn), CORPUS)
            for i, line in enumerate(open(os.path.join(root, fn), errors="ignore"), 1):
                if rel == deff and i == defl:
                    continue
                if pat.search(line) and "#define" not in line:
                    hits[rel] += 1
    return dict(sorted(hits.items(), key=lambda x: -x[1]))

Q = []
def add(qid, dim, question, gt):
    Q.append({"id": qid, "dim": dim, "question": question, "ground_truth": gt, "scope": "full"})

# ---- D1: definition / API discovery ----
d1_files = ["src/simhash/minhash.c", "internal/cbm/ac.c", "src/foundation/str_util.c",
            "src/git/git_context.c", "src/discover/discover.c"]
for i, f in enumerate(d1_files, 1):
    add(f"D1.{i}", "D1", f"List the names of all functions DEFINED in the file {f}. Return just the function names.",
        {"type": "set", "file": f, "functions": sorted(funcs_by_file[f]), "count": len(funcs_by_file[f])})

# ---- D2: relationship / call graph (callers) ----
d2_syms = ["cbm_arena_alloc", "cbm_minhash_compute", "cbm_split_camel_case", "cbm_sem_tokenize", "ac_build_trie"]
for i, s in enumerate(d2_syms, 1):
    c = callers_of(s)
    add(f"D2.{i}", "D2", f"Which functions/files call the function `{s}`? List the caller locations (files).",
        {"type": "callers_ub", "symbol": s, "caller_files_grep_ub": c, "n_caller_files": len(c)})

# ---- D3: targeted retrieval ----
d3_syms = ["ac_build_trie", "cbm_minhash_jaccard", "cbm_split_camel_case", "cbm_arena_alloc", "camel_should_split"]
for i, s in enumerate(d3_syms, 1):
    f, l = func_loc.get(s, ("?", 0))
    add(f"D3.{i}", "D3", f"In which file and at what line is the function `{s}` defined? Show its full signature.",
        {"type": "location", "symbol": s, "file": f, "line": l, "signature": sig_of(s)})

# ---- D4: architecture / structure (rubric-scored) ----
subsystems = sorted(set(p.split("/")[1] if p.startswith("src/") else "/".join(p.split("/")[:2])
                        for p in funcs_by_file if "/" in p))
add("D4.1", "D4", "What are the main subsystems / top-level modules of this codebase? Describe the overall architecture.",
    {"type": "rubric", "key_subsystems": ["src/cypher", "src/store", "src/semantic", "src/simhash",
     "src/pipeline", "internal/cbm/lsp", "src/mcp", "src/cli", "src/foundation", "src/discover",
     "src/watcher", "src/git", "src/graph_buffer", "src/traces", "src/ui"],
     "note": "credit for identifying the major subsystems and their roles"})
add("D4.2", "D4", "Which source file is the largest / most central hub by number of functions, and roughly how many functions does it have?",
    {"type": "value", "answer": ["src/store/store.c (~140)", "src/cypher/cypher.c (~134)"],
     "store_c": len(funcs_by_file["src/store/store.c"]), "cypher_c": len(funcs_by_file["src/cypher/cypher.c"])})
add("D4.3", "D4", "What is the program entry point, and which file contains main()?",
    {"type": "value", "answer": "src/main.c", "has_main": "main" in [n for n in funcs_by_file.get("src/main.c", [])]})
add("D4.4", "D4", "Which directory contains the per-language LSP type resolvers, and which languages have a hybrid LSP module?",
    {"type": "rubric", "dir": "internal/cbm/lsp/", "languages": ["go","python","typescript","java","c","csharp","php","kotlin","rust"]})
add("D4.5", "D4", "How is the MCP server layer organized — which file implements it and what does it expose?",
    {"type": "rubric", "file": "src/mcp/mcp.c", "note": "MCP JSON-RPC server exposing the graph query tools"})

# ---- D5: cross-cutting / semantic ----
def grep_concept(keywords):
    res = defaultdict(list)
    for n, f, l in oracle["functions"]:
        if any(k in n.lower() for k in keywords):
            res[f].append(n)
    return {k: sorted(v) for k, v in res.items()}

d5 = [
    ("D5.1", "Find the functions that implement near-duplicate / clone detection (minhash/simhash).", ["minhash", "simhash"]),
    ("D5.2", "Where is camelCase / identifier tokenization (splitting identifiers into words) implemented?", ["camel", "tokeniz"]),
    ("D5.3", "Find the arena / bump allocator implementation (allocation from a memory arena).", ["arena"]),
    ("D5.4", "Which code implements Aho-Corasick multi-pattern string matching?", ["ac_build", "ac_match", "ac_queue", "automaton"]),
    ("D5.5", "Where is the Cypher query language parsed and executed?", ["cypher", "parse_match", "eval_pattern"]),
]
for qid, q, kw in d5:
    add(qid, "D5", q, {"type": "concept_set", "keywords": kw, "reference_hits": grep_concept(kw)})

with open(f"{EVAL}/qbank.jsonl", "w") as fh:
    for q in Q:
        fh.write(json.dumps(q) + "\n")

print(f"generated {len(Q)} questions -> qbank.jsonl")
from collections import Counter
print("by dim:", dict(Counter(q["dim"] for q in Q)))
print("\n-- sample ground truths --")
for q in [Q[0], Q[5], Q[10], Q[15], Q[20]]:
    print(f"\n[{q['id']}] {q['question']}")
    print("  GT:", json.dumps(q["ground_truth"])[:200])
