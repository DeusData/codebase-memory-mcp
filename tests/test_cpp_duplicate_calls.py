"""Bounded real-MCP regression for duplicate C++ receiver definitions.

Run: python tests/test_cpp_duplicate_calls.py <binary> [--report <json-path>]
Fixtures and graph caches are fresh beneath the user's cache directory.
"""
import argparse
import hashlib
import json
import pathlib
import sys
import tempfile

sys.path.insert(0, str(pathlib.Path(__file__).parent / "windows"))
from mcp_stdio import McpServer

HEADER = "#pragma once\nclass FixtureLatch { public: bool Acquire(); void AcquireAgain(); };\n"
IMPL = '#include "fixture_latch.h"\nbool FixtureLatch::Acquire() { return true; }\nvoid FixtureLatch::AcquireAgain() { if (!Acquire()) return; }\n'
CALLS = "void UseRef(FixtureLatch& s) { if (!s.Acquire()) return; }\nvoid UsePtr(FixtureLatch* s) { if (!s->Acquire()) return; }\n"


def names(table):
    return {g["qn_prefix"] + "." + row[0] if g["qn_prefix"] else row[0]
            for g in table.get("groups", []) for row in g["rows"]}


def run(binary):
    parent = pathlib.Path.home() / ".cache"
    parent.mkdir(exist_ok=True)
    results = {"binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "cases": []}
    # Root include, explicit mirror, and negative controls for ambiguous visibility.
    cases = [("control", False, '#include "fixture_latch.h"\n', "fixture_latch"),
             ("duplicate", True, '#include "fixture_latch.h"\n', "fixture_latch"),
             ("mirror", True, '#include "mirror/fixture_latch.h"\n', "mirror.fixture_latch"),
             ("both", True, '#include "fixture_latch.h"\n#include "mirror/fixture_latch.h"\n', None),
             ("none", True, "", None),
             ("namespace", False, '#include "fixture_latch.h"\n', "fixture_latch.North"),
             ("namespace_missing", False, '#include "fixture_latch.h"\n', None)]
    # An upstream daemon may retain a log handle after its stdio client exits.
    # Leave locked temporary files in place rather than stopping other sessions.
    with tempfile.TemporaryDirectory(prefix="cpp-resolution-", dir=parent,
                                     ignore_cleanup_errors=True) as tmp:
        base = pathlib.Path(tmp)
        for label, duplicate, includes, target in cases:
            repo = base / label
            repo.mkdir()
            namespace_case = label.startswith("namespace")
            if namespace_case:
                header = "".join(
                    "namespace %s { class FixtureLatch { public: "
                    "bool Acquire() { return true; } void AcquireAgain() { Acquire(); } }; }\n" % ns
                    for ns in ("North", "South"))
                impl = '#include "fixture_latch.h"\n'
                calls = CALLS.replace("FixtureLatch", ("North" if target else "Absent") + "::FixtureLatch")
            else:
                header, impl, calls = HEADER, IMPL, CALLS
            (repo / "fixture_latch.h").write_text(header, encoding="utf-8")
            (repo / "fixture_latch.cpp").write_text(impl, encoding="utf-8")
            (repo / "caller.cpp").write_text(includes + calls, encoding="utf-8")
            if duplicate:
                (repo / "mirror").mkdir()
                for filename, text in [("fixture_latch.h", HEADER), ("fixture_latch.cpp", IMPL)]:
                    (repo / "mirror" / filename).write_text(text, encoding="utf-8")
            # Cross the extraction-worker threshold in one positive and one
            # ambiguity case; the small cases also cover the ordinary path.
            if label in ("duplicate", "both"):
                for i in range(51):
                    (repo / ("filler_%02d.cpp" % i)).write_text(
                        "int filler_%02d() { return %d; }\n" % (i, i), encoding="utf-8")
            cache, runtime = base / (label + "-cache"), base / (label + "-runtime")
            cache.mkdir()
            runtime.mkdir()
            with McpServer(str(binary), cache_dir=str(cache),
                           extra_env={"CBM_RUNTIME_DIR": str(runtime),
                                      "CBM_TEST_DAEMON_RUNTIME_PARENT": str(runtime),
                                      "CBM_MAX_THREADS": "2"},
                           cwd=str(base)) as server:
                initialized = server.initialize(timeout=40)
                info = initialized.get("result", {}).get("serverInfo", {})
                tool_names = ({"index": "index_repository", "trace": "trace_path"}
                              if info.get("name") == "codebase-memory-mcp" else {})

                def call(tool, args):
                    response = server.call_tool(tool_names.get(tool, tool),
                                                dict(args, format="json"), timeout=90)
                    text, error = server.tool_text(response)
                    assert not error and not response.get("result", {}).get("isError"), text
                    return json.loads(text)

                indexed = call("index", {"repo_path": str(repo)})
                project = indexed["project"]
                for key in ("not_indexed_files_count", "skipped_count",
                            "parse_partial_count", "parse_unusable_count"):
                    assert indexed[key] == 0, (label, key, indexed[key])

                def trace(qn, direction):
                    return call("trace", {"project": project, "function_name": project + "." + qn,
                                          "direction": direction, "depth": 1, "limit": 10})

                observed = {}
                modules = (["fixture_latch.North", "fixture_latch.South"] if namespace_case else
                           ["fixture_latch"] + (["mirror.fixture_latch"] if duplicate else []))
                for module in modules:
                    incoming = trace(module + ".FixtureLatch.Acquire", "inbound")
                    expected = {project + "." + module + ".FixtureLatch.AcquireAgain"}
                    if module == target:
                        expected |= {project + ".caller.UseRef", project + ".caller.UsePtr"}
                    actual = names(incoming.get("callers", {}))
                    assert actual == expected, (label, module, actual, expected)
                    assert incoming["callers_total"] == len(expected)
                    observed[module] = incoming["callers_total"]
                for caller in ("UseRef", "UsePtr"):
                    outgoing = trace("caller." + caller, "outbound")
                    actual = names(outgoing.get("callees", {}))
                    expected = {project + "." + target + ".FixtureLatch.Acquire"} if target else set()
                    assert actual == expected, (label, caller, actual, expected)
                results["cases"].append({"case": label, "callers": observed,
                                         "direct_target": target, "parse_failures": 0})
                print(label + ": PASS", flush=True)
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=pathlib.Path)
    parser.add_argument("--report", type=pathlib.Path)
    args = parser.parse_args()
    result = run(args.binary.resolve())
    if args.report:
        args.report.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(str(len(result["cases"])) + " cases passed")
