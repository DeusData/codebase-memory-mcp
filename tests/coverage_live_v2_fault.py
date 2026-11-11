"""Test-only invocation adapter: one fixed resource fault in the real native child."""

import json
import os
from pathlib import Path
import resource
import runpy
import subprocess
import sys


def main():
    collector, runner, suite, receipt, *arguments = sys.argv[1:]
    original = subprocess.Popen
    children = []

    def nofile():
        resource.setrlimit(resource.RLIMIT_NOFILE, (7, 7))

    def launch(args, *positional, **keywords):
        selected = (isinstance(args, (list, tuple)) and len(args) == 2
                    and os.fspath(args[0]) == runner and os.fspath(args[1]) == suite)
        if selected:
            if keywords.get("preexec_fn") is not None:
                raise RuntimeError("native fault boundary already has a pre-exec callback")
            keywords["preexec_fn"] = nofile
        process = original(args, *positional, **keywords)
        if selected:
            children.append((process, {"pid": process.pid, "argv": list(args),
                                       "rlimit_nofile": 7,
                                       "start_new_session": keywords.get("start_new_session", False)}))
        return process

    subprocess.Popen = launch
    sys.path.insert(0, str(Path(collector).parent))
    sys.argv = [collector] + arguments
    try:
        runpy.run_path(collector, run_name="__main__")
    finally:
        rows = []
        for process, row in children:
            row["exit"] = process.returncode
            rows.append(row)
        with open(receipt, "x", encoding="utf-8") as stream:
            json.dump({"fault": "native_child_fixed_nofile_7", "children": rows}, stream, indent=2)
            stream.write("\n")


if __name__ == "__main__":
    main()
