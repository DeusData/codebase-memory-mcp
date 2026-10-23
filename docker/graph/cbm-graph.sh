#!/bin/bash
# cbm-graph — index the repository mounted at /input/repo into Neo4j.
#
# Mounts:
#   /input/repo        the repository (read-only is fine; it is never modified)
#   /input/cbmignore   optional extra .cbmignore (e.g. a fleet-wide one)
#   /data              named volume: index cache + working copies (incremental)
# Environment:
#   CBM_NAME           project name (required)
#   CBM_FRESH=1        drop the project first and rebuild from scratch
#   NEO4J_URI          default http://host.docker.internal:7474
#   NEO4J_USER         default neo4j
#   NEO4J_PASSWORD     required
#   NEO4J_DATABASE     default neo4j
set -euo pipefail

die() { echo "cbm-graph: $*" >&2; exit 2; }

NAME=${CBM_NAME:-}
[ -n "$NAME" ] || die "CBM_NAME is required"
[[ "$NAME" =~ ^[A-Za-z0-9._-]+$ ]] || die "CBM_NAME may contain only letters, digits, '.', '_' and '-'"
[ -d /input/repo ] || die "mount the repository at /input/repo"
[ -n "${NEO4J_PASSWORD:-}" ] || die "NEO4J_PASSWORD is required"
NEO4J_URI=${NEO4J_URI:-http://host.docker.internal:7474}
NEO4J_USER=${NEO4J_USER:-neo4j}
NEO4J_DATABASE=${NEO4J_DATABASE:-neo4j}

WORK=/data/work/$NAME
mkdir -p "$HOME" "$WORK"

# ── Working copy ─────────────────────────────────────────────────────
# The indexer reads .cbmignore only from the repo root, and the source must
# stay untouched, so index a synced copy with the extra rules merged in.
# Paths an ignore rule excludes are also skipped by the copy (a jar-heavy
# repo is not copied for nothing) -- unless a rule uses '!' negation, whose
# gitignore meaning rsync cannot reproduce; then everything is copied and
# only the indexer applies the rules.
IGNORE_RULES=$(mktemp)
[ -f /input/repo/.cbmignore ] && cat /input/repo/.cbmignore >> "$IGNORE_RULES"
if [ -f /input/cbmignore ]; then
    printf '\n# --- merged from the mounted cbmignore ---\n' >> "$IGNORE_RULES"
    tr -d '\r' < /input/cbmignore >> "$IGNORE_RULES"
fi
RSYNC_ARGS=(-a --delete --delete-excluded --include='/.git/***' --exclude='/.cbmignore')
if ! grep -qE '^[[:space:]]*!' "$IGNORE_RULES"; then
    # gitignore trims trailing blanks ('target ' means 'target'); rsync would
    # take them literally.
    RSYNC_RULES=$(mktemp)
    sed 's/[[:space:]]*$//' "$IGNORE_RULES" > "$RSYNC_RULES"
    RSYNC_ARGS+=(--exclude='node_modules/' --exclude-from="$RSYNC_RULES")
fi
echo "== syncing working copy"
rsync "${RSYNC_ARGS[@]}" /input/repo/ "$WORK/"
cp "$IGNORE_RULES" "$WORK/.cbmignore"
rm -f "$IGNORE_RULES"
if [ -d "$WORK/.git" ]; then
    git config --global --add safe.directory "$WORK"
    grep -qx '.cbmignore' "$WORK/.git/info/exclude" 2>/dev/null ||
        { mkdir -p "$WORK/.git/info"; echo '.cbmignore' >> "$WORK/.git/info/exclude"; }
fi

# ── Index ────────────────────────────────────────────────────────────
if [ "${CBM_FRESH:-0}" = 1 ]; then
    echo "== fresh: dropping previous graph of $NAME"
    codebase-memory-mcp cli delete_project --project "$NAME" || true
fi

echo "== indexing $NAME into $NEO4J_URI (database $NEO4J_DATABASE)"
OUT=$(codebase-memory-mcp cli index_repository --repo-path "$WORK" --name "$NAME" \
      --graph-db "$NEO4J_URI" --graph-user "$NEO4J_USER" --graph-database "$NEO4J_DATABASE" 2>&1) || true
STATUS=$(printf '%s' "$OUT" | grep -o '"status":"[^"]*"' | tail -1 | cut -d'"' -f4)
if [ "$STATUS" != "indexed" ] && [ "$STATUS" != "degraded" ]; then
    printf '%s\n' "$OUT" >&2
    die "indexing failed (status: ${STATUS:-unknown})"
fi

echo "== done: $STATUS"
codebase-memory-mcp cli index_status --project "$NAME" 2>&1 |
    awk '/^[a-z_]+:/ { in_gb = ($1 == "graph_backend:") }
         /^(project|nodes|edges|status|indexed_at):/ || in_gb' | head -14
cat <<EOF

Open Neo4j Browser and run:
  MATCH (p:CbmProject {name:'$NAME'}) MATCH (n:CbmNode {db:p.name, gen:p.gen})-[r]->(m) RETURN n,r,m LIMIT 300
EOF
