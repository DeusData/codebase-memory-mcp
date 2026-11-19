"""Minimal MCP stdio client for the evals.

One `Mcp` object is one MCP session (one `codebase-memory-mcp-neo4j` process
speaking JSON-RPC over stdin/stdout). Tools run in the per-account daemon the
binary starts on demand, so environment variables that the daemon must see
(for example CBM_NEO4J_DELTA_MAX_ROWS or NEO4J_PASSWORD) have to be set before
the daemon starts; `run_cases.py` handles that with its `session` step.

The client never logs the request bodies, so a password passed as a tool
argument stays out of the stderr file; `run_cases.py` masks it in results.
"""
import json
import os
import queue
import subprocess
import threading
import time

PROTOCOL_VERSION = "2025-06-18"


class McpError(Exception):
    pass


class Mcp:
    def __init__(self, binary, env=None, stderr_path=None, init_timeout=120):
        full_env = dict(os.environ)
        if env:
            full_env.update({k: str(v) for k, v in env.items()})
        self.stderr = open(stderr_path, "ab") if stderr_path else subprocess.DEVNULL
        self.p = subprocess.Popen(
            [binary],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.stderr,
            env=full_env,
        )
        self.q = queue.Queue()
        self.n = 0
        self.closed = False
        threading.Thread(target=self._reader, daemon=True).start()
        r = self.request(
            "initialize",
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "cbm-evals", "version": "1"},
            },
            init_timeout,
        )
        if r in ("HANG", "EXIT"):
            raise McpError(f"initialize {r}")
        self.server_info = (r.get("result") or {}).get("serverInfo", {})
        self.notify("notifications/initialized")

    def _reader(self):
        for raw in self.p.stdout:
            try:
                self.q.put(json.loads(raw.decode("utf-8", "replace")))
            except Exception:
                pass
        self.q.put(None)

    def _send(self, msg):
        data = (json.dumps(msg, ensure_ascii=False) + "\n").encode("utf-8")
        self.p.stdin.write(data)
        self.p.stdin.flush()

    def notify(self, method, params=None):
        msg = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            msg["params"] = params
        self._send(msg)

    def request(self, method, params, timeout):
        """Returns the JSON-RPC reply, or the strings HANG / EXIT."""
        self.n += 1
        rid = self.n
        self._send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
        deadline = time.time() + timeout
        while True:
            left = deadline - time.time()
            if left <= 0:
                return "HANG"
            try:
                m = self.q.get(timeout=left)
            except queue.Empty:
                return "HANG"
            if m is None:
                return "EXIT"
            if m.get("id") == rid:
                return m

    def list_tools(self, timeout=60):
        r = self.request("tools/list", {}, timeout)
        if r in ("HANG", "EXIT"):
            raise McpError(f"tools/list {r}")
        return (r.get("result") or {}).get("tools", [])

    def call(self, tool, args, timeout):
        """Call a tool. Returns (status, reply) where status is ok|error|HANG|EXIT."""
        r = self.request("tools/call", {"name": tool, "arguments": args}, timeout)
        if r in ("HANG", "EXIT"):
            return r, None
        if "error" in r:
            return "error", r
        res = r.get("result") or {}
        return ("error" if res.get("isError") else "ok"), r

    def close(self, timeout=60):
        if self.closed:
            return
        self.closed = True
        try:
            self.p.stdin.close()
            self.p.wait(timeout=timeout)
        except Exception:
            try:
                self.p.kill()
            except Exception:
                pass
        if self.stderr not in (subprocess.DEVNULL, None):
            try:
                self.stderr.close()
            except Exception:
                pass

    def kill(self):
        self.closed = True
        try:
            self.p.kill()
        except Exception:
            pass


def reply_text(reply):
    """All text content of a tools/call reply, joined; the JSON-RPC error if any."""
    if reply is None:
        return ""
    if "error" in reply:
        return json.dumps(reply["error"], ensure_ascii=False)
    res = reply.get("result") or {}
    return "".join(c.get("text", "") for c in res.get("content", []) if isinstance(c, dict))


def reply_structured(reply):
    if reply is None or "error" in reply:
        return None
    return (reply.get("result") or {}).get("structuredContent")
