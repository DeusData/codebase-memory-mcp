#!/usr/bin/env bash
# imba — one-shot setup. Configures the whole toolkit on a new machine.
#
#   ./setup.sh              # install everything that applies
#   ./setup.sh autoclaude   # only the Claude Code economy layer
#   ./setup.sh skills        # only the Claude Code skills
#   ./setup.sh hermes-plugin # only the Hermes economizer plugin
#   ./setup.sh --uninstall    # remove autoclaude bits (economy block + hook)
#
#   env: SKILLS_WITH_BROWSERS=1  also install Playwright Chromium (~150MB)
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET="${1:-all}"

req(){ command -v "$1" >/dev/null || { echo "! missing prerequisite: $1"; return 1; }; }

do_autoclaude(){
  echo "== autoclaude (Claude Code economy: terse output, narrow reads, quota guard) =="
  req python3 || return 0
  bash "$ROOT/autoclaude/install.sh" "${2:-$PWD}"
}
do_skills(){
  echo "== Claude Code skills (skill-creator, mcp-builder, webapp-testing, docx/pdf/pptx/xlsx) =="
  req git || return 0
  bash "$ROOT/autoclaude/install-skills.sh"
}
do_hermes_plugin(){
  echo "== Hermes economizer plugin =="
  if ! command -v hermes >/dev/null; then
    echo "• Hermes not installed — skipping plugin. Install Hermes Agent first:"
    echo "  https://github.com/NousResearch/hermes-agent  (then re-run: ./setup.sh hermes-plugin)"
    return 0
  fi
  local HH="${HERMES_HOME:-$HOME/.hermes}"
  mkdir -p "$HH/plugins/hermes-economizer"
  cp -R "$ROOT/hermes-economizer/." "$HH/plugins/hermes-economizer/"
  [ -f "$HH/plugins/hermes-economizer/config.yaml" ] || \
    cp "$ROOT/hermes-economizer/config.yaml.example" "$HH/plugins/hermes-economizer/config.yaml"
  echo "✓ plugin installed to $HH/plugins/hermes-economizer (verify: hermes plugins list)"
}

case "$TARGET" in
  all)
    do_autoclaude
    do_skills
    do_hermes_plugin
    ;;
  autoclaude)     do_autoclaude "$@" ;;
  skills)          do_skills ;;
  hermes-plugin)   do_hermes_plugin ;;
  --uninstall)     bash "$ROOT/autoclaude/install.sh" --uninstall ;;
  *) echo "usage: ./setup.sh [all|autoclaude|skills|hermes-plugin|--uninstall]"; exit 1 ;;
esac
echo
echo "imba setup ($TARGET) complete."
