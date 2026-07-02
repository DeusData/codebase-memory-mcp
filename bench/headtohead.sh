#!/usr/bin/env bash
# headtohead.sh — deterministic head-to-head: codebase-memory-mcp (cbm) vs codegraph.
# Re-run after each workstream to MEASURE movement (no self-grading).
#
# Usage: bench/headtohead.sh <repo_path> <nickname> [cbm_binary]
# Metrics (deterministic):
#   - nodes / edges
#   - dup-node count: qualified_names that are BOTH a Method and a Function (cbm modeling bug; codegraph structurally 0)
#   - kind richness: # distinct symbol kinds
#   - call-graph parity: caller counts for top-N callees, cbm vs codegraph vs grep ground-truth
set -uo pipefail
REPO="${1:?repo path}"; NICK="${2:?nickname}"; CBM="${3:-/Users/charlesqin/.local/bin/codebase-memory-mcp}"
WORK="$(mktemp -d)/$NICK"; CACHE="$(mktemp -d)"
cp -R "$REPO" "$WORK"
echo "== head-to-head: $NICK ($(find "$WORK" -name '*.swift' -o -name '*.go' -o -name '*.ts' -o -name '*.py' 2>/dev/null | wc -l | tr -d ' ') src files) =="

# ---- cbm index ----
CBM_OUT=$(CBM_CACHE_DIR="$CACHE" "$CBM" cli index_repository "{\"repo_path\":\"$WORK\"}" 2>/dev/null | grep -v '^level=')
PROJ=$(echo "$CBM_OUT" | sed -n 's/.*"project":"\([^"]*\)".*/\1/p')
CBM_N=$(echo "$CBM_OUT" | sed -n 's/.*"nodes":\([0-9]*\).*/\1/p')
CBM_E=$(echo "$CBM_OUT" | sed -n 's/.*"edges":\([0-9]*\).*/\1/p')
qcbm(){ CBM_CACHE_DIR="$CACHE" "$CBM" cli query_graph "{\"project\":\"$PROJ\",\"query\":\"$1\"}" 2>/dev/null | grep -v '^level='; }

# cbm dup-node + kind richness: dup keyed on (name,file) since the bug emits the
# same source symbol as Method+Function with DIFFERENT qualified_names.
qcbm "MATCH (n) RETURN n.name AS nm, n.label AS l, n.file_path AS f" | python3 -c "
import sys,json
from collections import defaultdict,Counter
rows=json.load(sys.stdin).get('rows',[])
by=defaultdict(set); kinds=Counter()
for nm,l,f in rows:
    kinds[l]+=1
    if nm: by[(nm,f)].add(l)
dups=[k for k,s in by.items() if 'Method' in s and 'Function' in s]
# Swift type-kind fidelity: are struct/enum/protocol/extension distinct, or lumped into Class?
swiftkinds=sum(1 for k in kinds if k in ('Struct','Enum','Protocol','Extension','EnumCase','Actor','Component','Class'))
print(f'CBM_DUP={len(dups)}'); print(f'CBM_KINDS={len(kinds)}'); print(f'CBM_SWIFTKINDS={swiftkinds}')
print('CBM_KINDDIST='+','.join(f'{k}:{v}' for k,v in kinds.most_common(8)))
" > /tmp/_cbm_m
source /tmp/_cbm_m

# ---- codegraph index ----
CG_WORK="$(mktemp -d)/$NICK"; cp -R "$REPO" "$CG_WORK"
codegraph init "$CG_WORK" >/dev/null 2>&1
CG_STAT=$(codegraph status "$CG_WORK" 2>/dev/null)
CG_N=$(echo "$CG_STAT" | sed -n 's/.*Nodes:[[:space:]]*\([0-9]*\).*/\1/p' | head -1)
CG_E=$(echo "$CG_STAT" | sed -n 's/.*Edges:[[:space:]]*\([0-9]*\).*/\1/p' | head -1)
CG_KINDS=$(echo "$CG_STAT" | awk '/Nodes by Kind/{f=1;next} f&&/^  [a-z]/{c++} f&&/^$/{f=0} END{print c+0}')

# ---- call-graph parity (top-3 callees by fan-in) ----
echo "-- structural --"
printf "  %-10s nodes=%-5s edges=%-5s dup_nodes=%-3s kinds=%-3s\n" "cbm" "$CBM_N" "$CBM_E" "$CBM_DUP" "$CBM_KINDS"
printf "  %-10s nodes=%-5s edges=%-5s dup_nodes=%-3s kinds=%-3s\n" "codegraph" "$CG_N" "$CG_E" "0" "$CG_KINDS"
echo "  cbm kinds: $CBM_KINDDIST"
echo "-- call-graph parity (callers: cbm | codegraph | grep-truth) --"
CALLEES=$(qcbm "MATCH (a)-[:CALLS]->(b) RETURN b.name AS c, count(a) AS n ORDER BY n DESC LIMIT 5" | python3 -c "import sys,json;print(' '.join(r[0].split('.')[-1] for r in json.load(sys.stdin).get('rows',[]) if r[0].isidentifier() or '.' in r[0]))" 2>/dev/null)
for sym in $CALLEES; do
  cb=$(qcbm "MATCH (a)-[:CALLS]->(b) WHERE b.name='$sym' RETURN count(a) AS n" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d['rows'][0][0] if d.get('rows') else 0)" 2>/dev/null)
  cg=$(codegraph callers "$sym" -p "$CG_WORK" -j 2>/dev/null | python3 -c "import sys,json
try: d=json.load(sys.stdin); print(len(d) if isinstance(d,list) else len(d.get('callers',d.get('results',[]))))
except: print('?')" 2>/dev/null)
  gt=$(grep -rEo "[^a-zA-Z_]$sym\s*\(" "$WORK" --include='*.swift' 2>/dev/null | wc -l | tr -d ' ')
  printf "  %-28s cbm=%-3s codegraph=%-3s grep~%-3s\n" "$sym" "${cb:-?}" "${cg:-?}" "$gt"
done
rm -rf "$WORK" "$CG_WORK" "$CACHE"
