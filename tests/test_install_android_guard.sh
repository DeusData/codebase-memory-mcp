#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

mkdir -p "$WORK/bin" "$WORK/home"
cat >"$WORK/bin/uname" <<'EOF'
#!/usr/bin/env bash
case "$1" in
  -s) printf '%s\n' Linux ;;
  -m) printf '%s\n' aarch64 ;;
  -o) printf '%s\n' Android ;;
  *) exit 1 ;;
esac
EOF
cat >"$WORK/bin/curl" <<'EOF'
#!/usr/bin/env bash
printf 'curl must not run for Android/Termux\n' >&2
exit 99
EOF
chmod +x "$WORK/bin/uname" "$WORK/bin/curl"

# The stubs must be what install.sh sees, or this test proves nothing.
if [ "$(PATH="$WORK/bin:$PATH" uname -o)" != "Android" ]; then
  printf 'FAIL: the uname stub is not picked up from PATH\n' >&2
  exit 1
fi

set +e
output=$(PATH="$WORK/bin:$PATH" HOME="$WORK/home" \
  CBM_DOWNLOAD_URL="http://127.0.0.1:9/cbm-android-guard-test" \
  bash "$ROOT/install.sh" --skip-config 2>&1)
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
