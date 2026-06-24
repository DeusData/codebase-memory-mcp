#!/usr/bin/env bash
# check-lsp-originality.sh — Provenance guard for the hybrid LSP implementations.
#
# WHY THIS EXISTS
#   The LSP layer (internal/cbm/lsp/) is an original C implementation whose
#   behavior is "structurally inspired by and compatible with" the reference
#   language servers (pyright, gopls, tsserver, Roslyn, clangd, rust-analyzer,
#   …). We claim it contains NO source copied from those projects. This script
#   is the documented, runnable check that backs that claim — run it before
#   committing any NEW LSP implementation work, and review its report.
#
# WHAT IT CATCHES (the realistic copy "tells", and they survive a language port)
#   1. Verbatim string / error-message literals shared with a reference.
#   2. Verbatim comment phrases shared with a reference (the classic port tell:
#      people re-type the logic but keep the explanatory comments).
#   3. Same-language structural clones via jscpd (uses `jscpd` on PATH, else
#      `npx --yes jscpd`) — token-level clone detection, run only for clangd
#      (C++ ↔ our C), the one reference close enough to C to tokenize alike.
#
# WHAT IT CANNOT CATCH (be honest — these need a human)
#   An algorithm ported line-by-line with every identifier/comment rewritten is
#   NOT machine-detectable cheaply across C↔TS/Go/C#/Rust/Java. A clean report
#   is necessary, not sufficient: it means "no verbatim overlap found", not
#   "provably independent". The git history (incremental, test-driven authorship)
#   is the complementary evidence; keep it clean.
#
# USAGE
#   bash scripts/check-lsp-originality.sh [--lang NAME] [--refresh]
#                                         [--list-candidates] [--help]
#     --lang NAME        scan against ONE reference only (py|ts|go|cs|c|java|kotlin|rust)
#     --refresh          re-fetch reference sources even if cached
#     --list-candidates  print the extracted local tokens and exit (no fetch; self-test)
#   Exit 0 = no verbatim overlap found.  Exit 1 = overlap(s) to review by a human.
#
# Reference sources are shallow/sparse-cloned into $LSP_REFS_DIR (default
# .lsp-refs/, gitignored). Adding a new LSP language? Add its upstream repo to
# the REFS manifest below so this guard covers it too.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TAG="[lsp-orig]"
LSP_DIR="$ROOT/internal/cbm/lsp"
REFS_DIR="${LSP_REFS_DIR:-$ROOT/.lsp-refs}"

# Tunables (override via env).
MIN_STR="${LSP_ORIG_MIN_STR:-16}"        # min length of a string literal to consider
MIN_COMMENT="${LSP_ORIG_MIN_COMMENT:-30}" # min length of a comment phrase to consider
MIN_TOKENS="${LSP_ORIG_MIN_TOKENS:-50}"   # jscpd structural-clone min token run

# Reference manifest:  lang | git URL | sparse subpath (light checkout)
REFS=(
  "py|https://github.com/microsoft/pyright|packages/pyright-internal/src"
  "ts|https://github.com/microsoft/TypeScript|src/compiler,src/services"
  "go|https://github.com/golang/tools|gopls/internal"
  "cs|https://github.com/dotnet/roslyn|src/Compilers/CSharp/Portable"
  "c|https://github.com/llvm/llvm-project|clang-tools-extra/clangd"
  "java|https://github.com/eclipse-jdtls/eclipse.jdt.ls|org.eclipse.jdt.ls.core/src"
  "kotlin|https://github.com/fwcd/kotlin-language-server|server/src"
  "rust|https://github.com/rust-lang/rust-analyzer|crates"
  # PHP was implemented from specs (PHP language ref + PSR-4), with no upstream
  # LSP. We scan against phpactor (MIT) — the leading OSS PHP language server —
  # purely for defensive copy-detection. NOT Intelephense (proprietary).
  "php|https://github.com/phpactor/phpactor|lib"
)

ONLY_LANG=""; REFRESH=0; LIST_ONLY=0
while [ $# -gt 0 ]; do
  case "$1" in
    --lang) ONLY_LANG="${2:-}"; shift 2 ;;
    --refresh) REFRESH=1; shift ;;
    --list-candidates) LIST_ONLY=1; shift ;;
    --help|-h) sed -n '2,40p' "$0"; exit 0 ;;
    *) echo "$TAG unknown arg: $1" >&2; exit 2 ;;
  esac
done

if ! command -v rg >/dev/null 2>&1; then
  echo "$TAG ERROR: ripgrep (rg) is required." >&2; exit 2
fi

# --- 1. Extract distinctive local tokens from our LSP C sources ---------------
# Skip generated/ (machine-emitted stdlib tables, not hand-authored logic).
# bash 3.2 (macOS default) has no `mapfile` — build the array with a read loop.
LSP_SRC=()
while IFS= read -r f; do LSP_SRC+=("$f"); done < <(
  find "$LSP_DIR" -type f \( -name '*.c' -o -name '*.h' \) -not -path '*/generated/*' | sort)
if [ "${#LSP_SRC[@]}" -eq 0 ]; then
  echo "$TAG ERROR: no LSP sources under $LSP_DIR" >&2; exit 2
fi

CAND="$(mktemp)"; trap 'rm -f "$CAND" "$CAND.s" "$CAND.c" 2>/dev/null' EXIT

# String literals of interest: >= MIN_STR chars, contain a letter and a space
# (favours human-readable phrases / messages over format specifiers & symbols).
rg --no-filename -o "\"[^\"]{${MIN_STR},}\"" "${LSP_SRC[@]}" 2>/dev/null \
  | sed -E 's/^"//; s/"$//' \
  | rg '[A-Za-z].* ' \
  | rg -v '^(%|https?://|[0-9. ]+$)' \
  | sort -u > "$CAND.s"

# Comment phrases: // line comments and single-line /* ... */, >= MIN_COMMENT.
{ rg --no-filename -o '//[^\n]+' "${LSP_SRC[@]}" 2>/dev/null | sed -E 's#^//+##'
  rg --no-filename -o '/\*.*\*/' "${LSP_SRC[@]}" 2>/dev/null | sed -E 's#^/\*+##; s#\*+/$##'
} | sed -E 's/^[[:space:]*]+//; s/[[:space:]]+$//' \
  | awk -v n="$MIN_COMMENT" 'length($0) >= n' \
  | rg -v -i '^(todo|fixme|note|hack|xxx|copyright|spdx|clang-format|nolint|fallthrough)\b' \
  | sort -u > "$CAND.c"

# Keep only "meaningful" tokens: at least two real words (drops decorative
# dividers like ===== / ----- / box-drawing rules that collide with banners).
cat "$CAND.s" "$CAND.c" | grep -v '^[[:space:]]*$' \
  | rg -P '[A-Za-z]{3,}[^A-Za-z]+[A-Za-z]{2,}' \
  | sort -u > "$CAND"
n_str=$(wc -l < "$CAND.s" | tr -d ' '); n_com=$(wc -l < "$CAND.c" | tr -d ' ')
echo "$TAG extracted $(wc -l < "$CAND" | tr -d ' ') candidate tokens from ${#LSP_SRC[@]} LSP sources ($n_str strings, $n_com comments)"

if [ "$LIST_ONLY" -eq 1 ]; then
  echo "$TAG --- candidate tokens (no fetch performed) ---"
  cat "$CAND"
  exit 0
fi
if [ ! -s "$CAND" ]; then
  echo "$TAG no candidate tokens — nothing to compare."; exit 0
fi

# --- 2. Fetch reference sources (shallow + sparse) ----------------------------
fetch_ref() {
  local lang="$1" url="$2" subpath="$3" dst="$REFS_DIR/$lang"
  if [ -d "$dst/.git" ] && [ "$REFRESH" -eq 0 ]; then return 0; fi
  rm -rf "$dst"; mkdir -p "$dst"
  echo "$TAG fetching $lang ($url :: $subpath) ..."
  if ! git clone --depth 1 --filter=blob:none --sparse "$url" "$dst" >/dev/null 2>&1; then
    echo "$TAG WARN: clone failed for $lang — skipping"; rm -rf "$dst"; return 1
  fi
  ( cd "$dst" && git sparse-checkout set ${subpath//,/ } >/dev/null 2>&1 ) || true
}

# --- 3. Search candidates in each reference -----------------------------------
# Restrict to source code — license texts, changelogs and docs are not "copying".
REF_GLOBS=(-g '!**/LICENSE*' -g '!**/COPYING*' -g '!**/NOTICE*' -g '!**/CHANGELOG*'
           -g '!**/*.md' -g '!**/*.txt' -g '!**/*.json' -g '!**/*.yml' -g '!**/*.yaml')
total_hits=0
for entry in "${REFS[@]}"; do
  IFS='|' read -r lang url subpath <<< "$entry"
  [ -n "$ONLY_LANG" ] && [ "$ONLY_LANG" != "$lang" ] && continue
  fetch_ref "$lang" "$url" "$subpath" || continue
  dst="$REFS_DIR/$lang"
  [ -d "$dst" ] || continue

  # Fixed-string search of every candidate across this reference tree.
  hits=$(rg -F -l "${REF_GLOBS[@]}" -f "$CAND" "$dst" 2>/dev/null | wc -l | tr -d ' ')
  if [ "${hits:-0}" -gt 0 ]; then
    echo ""
    echo "$TAG ⚠ overlap with reference '$lang' ($url):"
    # Show which candidate matched where (cap output).
    rg -F -n "${REF_GLOBS[@]}" -f "$CAND" "$dst" 2>/dev/null | head -40
    total_hits=$((total_hits + hits))
  else
    echo "$TAG ok — no verbatim overlap with '$lang'"
  fi
done

# --- 4. Structural-clone pass (clangd C++ <-> our C; the only same-ish language)
# String search misses ports that kept the code structure but rewrote identifiers
# and strings. jscpd is a token clone detector; it tokenizes per format, so we
# stage both trees as .cpp (C tokenizes fine as C++) and report only clone pairs
# that SPAN the two trees.
structural_hits=0
if [ -z "$ONLY_LANG" ] || [ "$ONLY_LANG" = "c" ]; then
  JSCPD=""
  if command -v jscpd >/dev/null 2>&1; then JSCPD="jscpd"
  elif command -v npx >/dev/null 2>&1; then JSCPD="npx --yes jscpd"; fi
  cdir="$REFS_DIR/c"
  if [ -z "$JSCPD" ]; then
    echo "$TAG structural pass skipped — install jscpd (npm i -g jscpd) or node/npx to enable"
  elif [ ! -d "$cdir" ]; then
    echo "$TAG structural pass skipped — clangd ref not fetched (run without --lang, or --lang c)"
  else
    stage="$(mktemp -d)"; jout="$(mktemp -d)"
    for f in "${LSP_SRC[@]}"; do
      cp "$f" "$stage/ours__$(echo "${f#$LSP_DIR/}" | tr '/' '_').cpp"
    done
    while IFS= read -r f; do
      cp "$f" "$stage/clangd__$(echo "${f#$cdir/}" | tr '/' '_').cpp" 2>/dev/null
    done < <(find "$cdir" -type f \( -name '*.cpp' -o -name '*.cc' -o -name '*.cxx' \
                                      -o -name '*.h' -o -name '*.hpp' \))
    echo "$TAG structural pass (jscpd, >= ${MIN_TOKENS} tokens, clangd C++ <-> our C) ..."
    $JSCPD --silent --reporters json --output "$jout" --min-tokens "$MIN_TOKENS" "$stage" >/dev/null 2>&1 || true
    pairs=""
    if [ -f "$jout/jscpd-report.json" ]; then
      pairs=$(jq -r '.duplicates[]?
        | select((.firstFile.name|contains("ours__")) != (.secondFile.name|contains("ours__")))
        | "  \(.firstFile.name|gsub(".*/";"")) <-> \(.secondFile.name|gsub(".*/";"")) [\(.lines) lines]"' \
        "$jout/jscpd-report.json" 2>/dev/null | sort -u)
    fi
    if [ -n "$pairs" ]; then
      echo "$TAG ⚠ structural clones spanning our C and clangd:"; echo "$pairs" | head -20
      structural_hits=1
    else
      echo "$TAG ok — no structural clones (>= ${MIN_TOKENS} tokens) between our C and clangd"
    fi
    rm -rf "$stage" "$jout"
  fi
fi

echo ""
if [ "$total_hits" -gt 0 ] || [ "$structural_hits" -gt 0 ]; then
  if [ "$total_hits" -gt 0 ]; then
    echo "$TAG REVIEW NEEDED: $total_hits reference file(s) share a verbatim string/comment"
    echo "$TAG with internal/cbm/lsp/. A hit is NOT proof of copying (common phrases collide)"
    echo "$TAG — inspect each, confirm independent wording, reword genuinely-copied text."
  fi
  if [ "$structural_hits" -gt 0 ]; then
    echo "$TAG REVIEW NEEDED: jscpd found structurally-cloned block(s) between our C and"
    echo "$TAG clangd — inspect the pairs above and confirm independent implementation."
  fi
  echo "$TAG Re-run until clean before committing."
  exit 1
fi
echo "$TAG CLEAN — no verbatim string/comment overlap with any reference, and no"
echo "$TAG structural clones (jscpd) between our C and clangd."
echo "$TAG (Reminder: this proves no verbatim/structural overlap, not provable"
echo "$TAG  independence — the incremental git history is the complementary evidence.)"
exit 0
