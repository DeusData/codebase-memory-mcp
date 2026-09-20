#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

mkdir -p "$WORK/bin" "$WORK/home"
cat >"$WORK/bin/uname" <<'EOF'
case "$1" in
  -s) printf '%s\n' Linux ;;
  -m) printf '%s\n' aarch64 ;;
  -o) printf '%s\n' Android ;;
  *) exit 1 ;;
esac
EOF
cat >"$WORK/bin/curl" <<'EOF'
printf 'curl must not run for Android/Termux\n' >&2
exit 99
EOF
chmod +x "$WORK/bin/uname" "$WORK/bin/curl"

set +e
output=$(PATH="$WORK/bin:$PATH" HOME="$WORK/home" bash "$ROOT/install.sh" --skip-config 2>&1)
rc=$?
set -e

if [ "$rc" -eq 0 ]; then
  printf 'FAIL: installer accepted Android/Termux\n' >&2
  exit 1
fi
if ! grep -Fq 'Android/Termux' <<<"$output"; then
  printf 'FAIL: missing Android/Termux guidance\n%s\n' "$output" >&2
  exit 1
fi
if grep -Fq 'curl must not run' <<<"$output"; then
  printf 'FAIL: installer downloaded before rejecting Android/Termux\n' >&2
  exit 1
fi

printf 'PASS: Android/Termux is rejected before download\n'
