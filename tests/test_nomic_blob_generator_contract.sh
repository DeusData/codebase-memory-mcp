#!/usr/bin/env bash
# Contract: scripts/extract_nomic_vectors.py must still emit the tracked
# vendored/nomic/code_vectors_blob.S byte for byte.
#
# WHY this exists: the .S is a GENERATED file that was hand-edited to add the
# ELF .note.GNU-stack section. Without that note GNU ld assumes the whole link
# needs an executable stack, and every Linux release binary shipped GNU_STACK
# RWE because of the omission. The generator kept emitting only the Mach-O
# branch, so the next regeneration would have silently reverted the hardening
# and reintroduced the defect. check-binary-composition.sh catches that at
# release time; this catches it at edit time.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GEN="$ROOT/scripts/extract_nomic_vectors.py"
TRACKED="$ROOT/vendored/nomic/code_vectors_blob.S"

for f in "$GEN" "$TRACKED"; do
    if [ ! -f "$f" ]; then
        echo "FAIL: missing $f" >&2
        exit 1
    fi
done

# The generator holds the .S as an f-string whose only placeholder is the
# incbin path, so the tracked file with that path re-parameterized must appear
# in it verbatim. Comparing the template rather than running the generator
# keeps this test free of the torch/transformers import the script needs.
python3 - "$GEN" "$TRACKED" <<'PYEOF'
import sys

gen_path, tracked_path = sys.argv[1], sys.argv[2]

with open(gen_path, encoding="utf-8") as fh:
    generator = fh.read()
with open(tracked_path, encoding="utf-8") as fh:
    tracked = fh.read()

INCBIN = "vendored/nomic/code_vectors.bin"
if INCBIN not in tracked:
    sys.exit("FAIL: %s no longer references %s" % (tracked_path, INCBIN))
if "{" in tracked or "}" in tracked:
    sys.exit("FAIL: %s gained a brace; the generator f-string would need "
             "escaping and this contract can no longer compare them directly"
             % tracked_path)

template = tracked.replace(INCBIN, "{incbin_path}")

if template not in generator:
    sys.exit(
        "FAIL: scripts/extract_nomic_vectors.py no longer emits the tracked\n"
        "      vendored/nomic/code_vectors_blob.S.\n"
        "      Regenerating the blob wrapper would overwrite the tracked file\n"
        "      with different content. Update write_blob_s() so its f-string\n"
        "      matches the tracked .S with the incbin path parameterized.")

# The note is the whole point; assert it explicitly so a future edit that keeps
# the files in sync while dropping the note still fails loudly.
NOTE = '.section .note.GNU-stack,"",@progbits'
for label, blob in (("tracked .S", tracked), ("generator template", generator)):
    if NOTE not in blob:
        sys.exit("FAIL: %s lost the ELF %s section -- Linux binaries would ship "
                 "an executable stack again" % (label, NOTE))

print("ok: generator template is byte-identical to the tracked blob wrapper")
PYEOF
