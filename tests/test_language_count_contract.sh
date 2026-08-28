#!/usr/bin/env bash
# The published language count IS the number of vendored tree-sitter grammars,
# and every public surface must agree with it.
#
# Why this exists: the count was hand-maintained on seven separate surfaces, so
# it drifted apart from reality AND from itself — README/docs/npm said 158 while
# server.json/nuspec/package-release.sh said 159.
#
# Why grammars and not CBM_LANG_COUNT: the registry contains entries with no
# parser at all. CBM_LANG_NIM (grammar removed 2026-06-12) and
# CBM_LANG_OBJECTSCRIPT_EXPORT have no grammar directory, no tree_sitter
# function and no MANIFEST row, yet files still route to them by extension.
# Counting them would publish languages we do not parse. A vendored grammar
# directory, by contrast, demonstrably exists and is checksum-gated.
#
# Known and accepted: this UNDERCOUNTS languages that share one grammar
# (TypeScript/TSX, objectscript_udl/_routine). We publish the number we can
# prove rather than the larger number we cannot.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

GRAMMAR_DIR="internal/cbm/vendored/grammars"

# --- 1. The truth: count the vendored grammar directories. ---
expected=$(find "$GRAMMAR_DIR" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')

# A parse that silently returns nothing would make every check below vacuous.
if ((expected < 100 || expected > 500)); then
    echo "FAIL: counted $expected grammar directories under $GRAMMAR_DIR —" \
        "the layout changed and this contract can no longer read it" >&2
    exit 1
fi

# --- 2. MANIFEST.md must already agree; it is the human-facing ledger. ---
manifest_count=$(
    grep -oE '^- Grammars: \*\*[0-9]+\*\*' "$GRAMMAR_DIR/MANIFEST.md" |
        grep -oE '[0-9]+' || true
)
if [[ "$manifest_count" != "$expected" ]]; then
    echo "FAIL: $GRAMMAR_DIR/MANIFEST.md declares ${manifest_count:-no} grammars," \
        "but $expected directories exist" >&2
    exit 1
fi

# --- 3. Every published surface must claim exactly that number. ---
SURFACES=(
    README.md
    docs/index.html
    docs/llms.txt
    pkg/npm/README.md
    pkg/chocolatey/codebase-memory-mcp.nuspec
    scripts/package-release.sh
    server.json
)

# Files that legitimately say "<N> languages" about something OTHER than the
# product's registry size. Each entry states what its number actually counts,
# so an unexplained exemption cannot hide here.
EXEMPT=(
    'docs/EVALUATION_PLAN.md'                  # historical plan, pinned to its authoring date
    'docs/BENCHMARK.md'                        # benchmark corpus subsets (63/17 scored)
    'scripts/clone-bench-repos.sh'             # bench repo tiers (44 + 22)
    'tests/repro/repro_invariant_breadth.c'    # breadth of one invariant (26/27)
    'tests/repro/repro_grammar_scripting.c'    # scripting-grammar probe scope (12)
    'tests/test_grammar_probe_b.c'             # grammar probe scope (12)
    'tests/test_language_count_contract.sh'    # this file, which names the numbers above
    'pkg/winget/manifests/'                    # version-pinned published manifests: 0.8.1
                                               # really did ship 155, and rewriting a
                                               # released manifest would falsify it
)

failures=0
distinct=0

for path in "${SURFACES[@]}"; do
    if [[ ! -f "$path" ]]; then
        echo "FAIL: registered surface $path does not exist — update SURFACES" >&2
        failures=$((failures + 1))
        continue
    fi
    # Two claim forms. Prose says "<N> languages"; a shields.io badge says
    # "languages-<N>-colour". The badge was MISSED by the first version of this
    # contract: it gated only the prose form, so README's badge sat three behind
    # the prose in the same file while this test reported success. The lesson is
    # that the enumeration and the gate must not share a regex — if they do, the
    # gate can only ever confirm what the enumeration already saw.
    hits=$( { grep -oE '[0-9]{2,3} languages' "$path" | grep -oE '^[0-9]+'
              grep -oiE 'languages-[0-9]{2,3}' "$path" | grep -oE '[0-9]+$'
            } | sort -u || true)
    if [[ -z "$hits" ]]; then
        echo "FAIL: $path is registered as a language-count surface but states no" \
            "count — either it lost the claim (drop it from SURFACES) or the" \
            "wording changed and this check went vacuous" >&2
        failures=$((failures + 1))
        continue
    fi
    while IFS= read -r n; do
        distinct=$((distinct + 1))
        if [[ "$n" != "$expected" ]]; then
            echo "FAIL: $path claims $n languages, $expected grammars are vendored" >&2
            failures=$((failures + 1))
        fi
    done <<<"$hits"
done

# --- 4. No UNREGISTERED file may make the claim. ---
# Without this, adding an eighth surface reintroduces exactly the drift this
# contract exists to prevent, and every check above would still pass.
while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    for known in "${SURFACES[@]}"; do
        [[ "$path" == "$known" ]] && continue 2
    done
    for skip in "${EXEMPT[@]}"; do
        [[ "$path" == "$skip"* ]] && continue 2
    done
    echo "FAIL: $path states a language count but is not registered. Add it to" \
        "SURFACES so it stays in step with the registry, or to EXEMPT with a" \
        "note saying what its number actually counts." >&2
    failures=$((failures + 1))
done < <(git grep -IlE '[0-9]{2,3} languages|languages-[0-9]{2,3}' || true)

if ((failures > 0)); then
    echo "FAIL: $failures language-count contract violation(s)" >&2
    exit 1
fi

echo "Language-count contract passed (${#SURFACES[@]} surfaces, $distinct distinct value(s) checked, $expected grammars vendored)"
