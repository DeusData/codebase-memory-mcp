#!/usr/bin/env python3
"""Drive a fixed sequence of MCP requests over stdio and wait for each reply.

Batching all requests and closing stdin does not work: the server treats EOF as
the client disconnecting and shuts down before answering, so the run produces a
census of a process that never did any work. Reading each response before
sending the next also makes the workload deterministic — every request is
serviced from the same steady state, which is what makes two runs comparable.
"""
import argparse
import json
import subprocess
import sys
import time


def rpc(proc, payload, timeout_note):
    proc.stdin.write(json.dumps(payload) + "\n")
    proc.stdin.flush()
    while True:
        line = proc.stdout.readline()
        if not line:
            raise RuntimeError(f"server closed stdout while waiting for {timeout_note}")
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if message.get("id") == payload.get("id"):
            return message


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("binary")
    parser.add_argument("corpus")
    parser.add_argument("requests", type=int)
    parser.add_argument("--stderr", help="file for the server's stderr; never discard it")
    parser.add_argument("--tool", default="search_graph",
                        help="tool to repeat; varying it isolates which path leaks")
    parser.add_argument("--idle-seconds", type=float, default=0.0,
                        help="pause midway; separates per-request growth from per-second growth")
    parser.add_argument("--skip-index", action="store_true",
                        help="omit the initial index, to separate store setup from the loop")
    args = parser.parse_args()

    proc = subprocess.Popen(
        [args.binary],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        # Never DEVNULL: when the server refuses to start, its stderr is the
        # only thing that says why.
        stderr=open(args.stderr, "w") if args.stderr else None,
        text=True,
        bufsize=1,
    )
    served = 0
    failures = 0
    try:
        rpc(proc, {"jsonrpc": "2.0", "id": 0, "method": "initialize",
                   "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                              "clientInfo": {"name": "memlab", "version": "1"}}},
            "initialize")
        if not args.skip_index:
            indexed = rpc(proc, {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                                 "params": {"name": "index_repository",
                                            "arguments": {"path": args.corpus}}},
                          "index_repository")
            if "error" in indexed:
                print(f"index failed: {indexed['error']}", file=sys.stderr)
                return 3

        halfway = 2 + args.requests // 2
        for i in range(2, args.requests + 2):
            if args.idle_seconds > 0 and i == halfway:
                # Nothing is requested during this gap. Any commit growth across
                # it belongs to a background thread, not the request path.
                print(f"idle-start id={i}", flush=True)
                time.sleep(args.idle_seconds)
                print(f"idle-end id={i}", flush=True)
            arguments = {"search_graph": {"name_pattern": ".*Widget.*", "limit": 10},
                         "list_projects": {},
                         "get_graph_schema": {},
                         "search_code": {"pattern": "Widget"}}.get(args.tool, {})
            reply = rpc(proc, {"jsonrpc": "2.0", "id": i, "method": "tools/call",
                               "params": {"name": args.tool, "arguments": arguments}},
                        f"request {i}")
            served += 1
            if "error" in reply:
                failures += 1
    finally:
        try:
            proc.stdin.close()
        except Exception:
            pass
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()

    print(f"served={served} failed={failures}")
    return 0 if served == args.requests and failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
