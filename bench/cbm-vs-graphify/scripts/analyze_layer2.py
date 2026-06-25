#!/usr/bin/env python3
import json
from collections import defaultdict

d = json.load(open("layer2_results.json"))
res = d["result"]
results = res["results"]
CONDS = ["cbm", "graphify", "explorer"]

print("======== LAYER 2: per-question condition scores (mean of 2 judge passes, 0-4) ========")
print(f"{'id':6} {'dim':4} {'cbm':>5} {'gfy':>5} {'exp':>5}   notes")
no_answer = defaultdict(list)
low = []
toolcalls = defaultdict(list)
disagree = []
for r in results:
    cm = r["condMeanScore"]
    # detect no-answer / failures
    note = ""
    for a in r["answers"]:
        toolcalls[a["cond"]].append(a["tool_calls"])
        if a["answer"].strip() in ("(no answer)", "") or "StructuredOutput" in a["answer"]:
            no_answer[a["cond"]].append(r["id"])
            note += f" {a['cond']}=NOANS"
    # judge pass disagreement
    bypass = defaultdict(list)
    for p in r["judged"]:
        for s in p.get("scores", []):
            cond = r["slotToCond"].get(s["slot"])
            if cond:
                bypass[cond].append(s["correctness"])
    for c, arr in bypass.items():
        if len(arr) == 2 and abs(arr[0] - arr[1]) >= 2:
            disagree.append((r["id"], c, arr))
    def f(x): return f"{x:.1f}" if x is not None else " - "
    print(f"{r['id']:6} {r['dim']:4} {f(cm['cbm']):>5} {f(cm['graphify']):>5} {f(cm['explorer']):>5}  {note}")

print("\n-- no-answer / harness failures by condition --")
for c in CONDS:
    print(f"  {c}: {no_answer.get(c, [])}")

print("\n-- judge-pass disagreements (|p0-p1|>=2) --")
for did, c, arr in disagree:
    print(f"  {did} {c}: {arr}")
if not disagree:
    print("  none (judge passes consistent)")

print("\n-- mean tool calls per condition (effort) --")
for c in CONDS:
    arr = toolcalls.get(c, [])
    print(f"  {c}: mean={sum(arr)/len(arr):.1f}  total={sum(arr)}")

print("\n-- graphify notable failures (score<=1) --")
for r in results:
    if r["condMeanScore"]["graphify"] is not None and r["condMeanScore"]["graphify"] <= 1.0:
        gans = [a["answer"] for a in r["answers"] if a["cond"] == "graphify"][0]
        print(f"  [{r['id']}] gfy={r['condMeanScore']['graphify']}: {gans[:160]}")

print("\n-- cbm notable weak spots (D4, score<=2.5) --")
for r in results:
    if r["dim"] == "D4":
        print(f"  [{r['id']}] cbm={r['condMeanScore']['cbm']} gfy={r['condMeanScore']['graphify']} exp={r['condMeanScore']['explorer']}")

# recompute summary excluding harness-failed explorer answers (fairness)
print("\n-- explorer score EXCLUDING the harness no-answer failures (fairness-adjusted) --")
adj = defaultdict(list)
failed_ids = set(i for ids in no_answer.values() for i in ids)
for r in results:
    if r["id"] in [i for i in no_answer.get("explorer", [])]:
        continue
    if r["condMeanScore"]["explorer"] is not None:
        adj[r["dim"]].append(r["condMeanScore"]["explorer"])
import statistics
allv = []
for dim in ["D1", "D2", "D3", "D4", "D5"]:
    if adj[dim]:
        m = statistics.mean(adj[dim]); allv.extend(adj[dim])
        print(f"  {dim}: {m:.2f}")
print(f"  explorer OVERALL (excl failures): {statistics.mean(allv):.2f}")
