#!/usr/bin/env bash
# Exercise the real production Make rules with tiny C fixtures. CodeQL starts
# tracing after cbm-third-party, so every affected object must compile again,
# while unrelated objects must remain reused. No dry-run/timestamp assertions.
set -euo pipefail
ROOT="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
python3 - "$ROOT" "$BASH" <<'PY'
import os
import pathlib
import re
import subprocess
import sys
import tempfile

root, bash = pathlib.Path(sys.argv[1]), sys.argv[2]
failures = []
with tempfile.TemporaryDirectory(prefix="cbm-codeql-retrace-") as directory:
    work = pathlib.Path(directory)

    def write(path, text):
        dest = work / path
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(text, encoding="utf-8")

    for path in ("Makefile.cbm", "scripts/ci/codeql-retrace-vendored.sh",
                 "scripts/ci/select-lanes.sh", ".github/codeql/codeql-config.yml"):
        write(path, (root / path).read_text(encoding="utf-8"))

    # Nested and aliased includes deliberately escape the Makefile's shallow
    # wildcard dependencies. The compiler really compiles these translation units.
    write("vendored/mimalloc/src/static.c",
          '#include "mimalloc/internal.h"\n#include "prim/osx/prim.c"\n')
    write("vendored/mimalloc/include/mimalloc/internal.h", "#define MI_VALUE 7\n")
    write("vendored/mimalloc/src/prim/osx/prim.c", '#include "../unix/prim.c"\n')
    write("vendored/mimalloc/src/prim/unix/prim.c", "int mi_value(void) { return MI_VALUE; }\n")
    write("internal/cbm/vendored/common/scanner.h", "#define SHARED_VALUE 3\n")
    for language in ("go", "json"):
        write(f"internal/cbm/grammar_{language}.c",
              f'#include "vendored/grammars/{language}/parser.c"\n')
        write(f"internal/cbm/vendored/grammars/{language}/parser.c",
              '#include "nested/value.h"\n#include "../../common/scanner.h"\n'
              f'int grammar_{language}(void) {{ return VALUE + SHARED_VALUE; }}\n')
        write(f"internal/cbm/vendored/grammars/{language}/nested/value.h", "#define VALUE 2\n")
    write("internal/cbm/vendored/grammars/shared/shared.h", "/* future shared grammar helper */\n")
    write("internal/cbm/ts_runtime.c", '#include "vendored/ts_runtime/src/lib.c"\n')
    write("internal/cbm/vendored/ts_runtime/src/lib.c", "int runtime_value(void) { return 1; }\n")
    for path, symbol in (("vendored/sqlite3/sqlite3.c", "sqlite_value"),
                         ("vendored/tre/tre_all.c", "tre_value"),
                         ("internal/cbm/vendored/lz4/lz4.c", "lz4_value"),
                         ("internal/cbm/vendored/lz4/lz4hc.c", "lz4hc_value"),
                         ("internal/cbm/vendored/zstd/zstd.c", "zstd_value")):
        write(path, f"int {symbol}(void) {{ return 1; }}\n")
    write("internal/cbm/vendored/lz4/lz4.h", "/* shared LZ4 header */\n")
    write("vendored/sqlite3/sqlite3.h", "/* sqlite header */\n")
    write("internal/cbm/vendored/zstd/zstd.h", "/* zstd header */\n")
    write("vendored/nomic/code_vectors_blob.S", ".text\n")
    write("vendored/nomic/code_vectors.bin", "")
    write("src/main.c", "int main(void) { return 0; }\n")

    def run(args):
        proc = subprocess.run(args, cwd=work, capture_output=True, text=True)
        if proc.returncode:
            raise RuntimeError(f"{args}: exit {proc.returncode}\n{proc.stdout}\n{proc.stderr}")
        return proc.stdout

    make = ["make", "--no-print-directory", "-f", "Makefile.cbm", "cbm-third-party",
            "BUILD_DIR=build/c", "CC=" + os.environ.get("CC", "cc")]

    def compiled(output):
        return set(re.findall(r" -c -o build/c/([^\s]+\.o)(?:\s|$)", output))

    prebuilt = compiled(run(make))
    expected = {"prod_mimalloc.o", "prod_sqlite3.o", "prod_grammar_go.o",
                "prod_grammar_json.o", "prod_ts_runtime.o", "prod_lz4.o",
                "prod_lz4hc.o", "prod_zstd.o", "unixcoder_blob.o"}
    # TRE is a Windows-only production object. All other objects are mandatory.
    if prebuilt not in (expected, expected | {"prod_tre.o"}):
        raise RuntimeError(f"incomplete prebuild: {sorted(prebuilt)}")
    if compiled(run(make)):
        raise RuntimeError("unchanged prebuild is not reused")

    cases = [
        ("nested allocator header", ["vendored/mimalloc/include/mimalloc/internal.h"], {"prod_mimalloc.o"}),
        ("aliased allocator source", ["vendored/mimalloc/src/prim/unix/prim.c"], {"prod_mimalloc.o"}),
        ("nested grammar header", ["internal/cbm/vendored/grammars/go/nested/value.h"], {"prod_grammar_go.o"}),
        ("grammar wrapper", ["internal/cbm/grammar_go.c"], {"prod_grammar_go.o"}),
        ("runtime wrapper", ["internal/cbm/ts_runtime.c"], {"prod_ts_runtime.o"}),
        ("runtime source", ["internal/cbm/vendored/ts_runtime/src/lib.c"], {"prod_ts_runtime.o"}),
        ("shared grammar header", ["internal/cbm/vendored/common/scanner.h"], {"prod_grammar_go.o", "prod_grammar_json.o"}),
        ("unknown grammar family", ["internal/cbm/vendored/grammars/shared/shared.h"], {"prod_grammar_go.o", "prod_grammar_json.o"}),
        ("LZ4 header", ["internal/cbm/vendored/lz4/lz4.h"], {"prod_lz4.o", "prod_lz4hc.o"}),
        ("SQLite header", ["vendored/sqlite3/sqlite3.h"], {"prod_sqlite3.o"}),
        ("zstd header", ["internal/cbm/vendored/zstd/zstd.h"], {"prod_zstd.o"}),
        ("first-party only", ["src/main.c"], set()),
        ("duplicate changes", ["internal/cbm/grammar_go.c", "internal/cbm/grammar_go.c"], {"prod_grammar_go.o"}),
    ]
    for label, paths, expected_objects in cases:
        write("changed.txt", "\n".join(paths) + "\n")
        run([bash, "scripts/ci/codeql-retrace-vendored.sh", "--changed", "changed.txt", "retrace.txt"])
        retrace = (work / "retrace.txt").read_text(encoding="utf-8").splitlines()
        actual = compiled(run(make + [arg for path in retrace for arg in ("-W", path)]))
        if actual != expected_objects:
            failures.append(f"{label}: expected {sorted(expected_objects)}, compiled {sorted(actual)}")
        else:
            print(f"PASS {label}: {len(actual)}/{len(prebuilt)} third-party objects retraced")
if failures:
    print("CODEQL RETRACE CONTRACT VIOLATED:\n  " + "\n  ".join(failures))
    sys.exit(1)
print("CodeQL retrace contract OK (real compiler + production Make rules)")
PY
