#!/usr/bin/env bash
# autoclaude — install curated Claude Code skills globally (~/.claude/skills).
# Idempotent. Pulls only the needed skill folders from anthropics/skills via
# sparse-checkout (fast, small). Heavy deps are opt-in.
#
#   ./install-skills.sh                 # copy skills only (fast)
#   SKILLS_WITH_DEPS=1 ./install-skills.sh      # + pip install document/test deps
#   SKILLS_WITH_BROWSERS=1 ./install-skills.sh  # + Playwright Chromium (~150MB)
set -euo pipefail

SKILLS=(skill-creator mcp-builder webapp-testing docx pdf pptx xlsx)
DST="$HOME/.claude/skills"
CACHE="${ANTHROPIC_SKILLS_CACHE:-/tmp/anthropic-skills}"
REPO="https://github.com/anthropics/skills"

command -v git >/dev/null || { echo "! git required"; exit 1; }

# Sparse, shallow checkout of just the skill folders we want — not the whole repo.
if [ ! -d "$CACHE/skills" ]; then
  echo "• fetching ${#SKILLS[@]} skills (sparse) -> $CACHE"
  rm -rf "$CACHE"
  git clone --no-checkout --depth 1 --filter=blob:none "$REPO" "$CACHE" >/dev/null 2>&1
  git -C "$CACHE" sparse-checkout init --cone >/dev/null 2>&1
  git -C "$CACHE" sparse-checkout set "${SKILLS[@]/#/skills/}" >/dev/null 2>&1
  git -C "$CACHE" checkout >/dev/null 2>&1
fi

mkdir -p "$DST"
for s in "${SKILLS[@]}"; do
  if [ -d "$CACHE/skills/$s" ]; then
    rm -rf "$DST/$s"; cp -R "$CACHE/skills/$s" "$DST/"; echo "✓ skill: $s"
  else
    echo "! not found in repo: $s (skipped)"
  fi
done

# Deps are opt-in: the document/test skills self-install what they need on first
# use, so the default path stays fast and dependency-free.
if [ "${SKILLS_WITH_DEPS:-0}" = "1" ] || [ "${SKILLS_WITH_BROWSERS:-0}" = "1" ]; then
  echo "• installing python deps (document/testing skills)"
  python3 -m pip install --quiet --disable-pip-version-check \
    openpyxl pypdf pdfplumber python-pptx python-docx playwright 2>/dev/null \
    && echo "✓ python deps ready" || echo "• pip step skipped"
fi
if [ "${SKILLS_WITH_BROWSERS:-0}" = "1" ] && python3 -c "import playwright" 2>/dev/null; then
  if ! ls "$HOME"/{Library/Caches,.cache}/ms-playwright/chromium-* >/dev/null 2>&1; then
    echo "• installing Playwright Chromium (~150MB)"
    python3 -m playwright install chromium >/dev/null 2>&1 \
      && echo "✓ chromium installed" || echo "• run 'python3 -m playwright install chromium' manually"
  fi
fi

echo "Done. Skills in $DST (all sessions)."
[ "${SKILLS_WITH_DEPS:-0}" = "1" ] || echo "Tip: deps install on first use; or run with SKILLS_WITH_DEPS=1 / SKILLS_WITH_BROWSERS=1."
