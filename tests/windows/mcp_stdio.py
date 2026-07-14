"""Minimal MCP stdio client for the Windows red-test suite.

Drives a real codebase-memory-mcp(.exe) over a line-delimited JSON-RPC stdio
pipe. The pipe carries UTF-8 bytes, so a non-ASCII repo_path reaches the server
without passing through the Windows ANSI command-line code page (which mangles
argv for a binary whose main() is not wmain/GetCommandLineW). This isolates the
server's real path handling from CLI-argv encoding artifacts.

No third-party dependencies — standard library only.
"""
import json
import os
import subprocess
import threading
import time


class McpError(Exception):
    pass


class McpServer:
    def __init__(self, binary, cache_dir=None, extra_env=None, cwd=None):
        self.binary = binary
        self._id = 0
        self.proc = None
        self._stderr = []
        env = dict(os.environ)
        if cache_dir:
            env["CBM_CACHE_DIR"] = cache_dir  # isolate the graph DB location
        if extra_env:
            env.update(extra_env)
        self.env = env
        self.cwd = cwd

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *a):
        self.close()

    def start(self):
        self.proc = subprocess.Popen(
            [self.binary],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=self.env, cwd=self.cwd, bufsize=0)
        threading.Thread(target=self._drain_stderr, daemon=True).start()

    def _drain_stderr(self):
        try:
            for line in self.proc.stderr:
                self._stderr.append(line.decode("utf-8", "replace"))
        except Exception:
            pass

    def stderr_text(self):
        return "".join(self._stderr)

    def _send(self, obj):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.proc.stdin.write(data + b"\n")
        self.proc.stdin.flush()

    def _read_message(self, timeout=60):
        result = {}

        def reader():
            try:
                result["line"] = self.proc.stdout.readline()
            except Exception as ex:
                result["exc"] = ex

        th = threading.Thread(target=reader, daemon=True)
        th.start()
        th.join(timeout)
        if th.is_alive():
            raise McpError("timeout after %ss (hang)" % timeout)
        if "exc" in result:
            raise McpError("read error: %r" % result["exc"])
        line = result.get("line", b"")
        if not line:
            raise McpError("EOF / server closed stdout")
        # strict: an invalid-UTF-8 JSON-RPC response is itself a failure.
        return json.loads(line.decode("utf-8", "strict"))

    def request(self, method, params=None, timeout=60):
        self._id += 1
        rid = self._id
        self._send({"jsonrpc": "2.0", "id": rid, "method": method,
                    "params": params or {}})
        deadline = time.time() + timeout
        while True:
            msg = self._read_message(timeout=max(1, deadline - time.time()))
            if msg.get("id") == rid:
                return msg
            if time.time() > deadline:
                raise McpError("timeout waiting for id=%d" % rid)

    def notify(self, method, params=None):
        self._send({"jsonrpc": "2.0", "method": method, "params": params or {}})

    def initialize(self, timeout=60):
        resp = self.request("initialize", {
            "protocolVersion": "2024-11-05", "capabilities": {},
            "clientInfo": {"name": "windows-red-test", "version": "1.0"}}, timeout)
        if "error" in resp:
            raise McpError("initialize error: %r" % resp["error"])
        try:
            self.notify("notifications/initialized")
        except Exception:
            pass
        return resp

    def tools_list(self, timeout=60):
        resp = self.request("tools/list", {}, timeout=timeout)
        if "error" in resp:
            raise McpError("tools/list error: %r" % resp["error"])
        return resp["result"]["tools"]

    def call_tool(self, name, arguments, timeout=180):
        return self.request("tools/call",
                            {"name": name, "arguments": arguments}, timeout=timeout)

    @staticmethod
    def tool_text(resp):
        if "error" in resp:
            return None, resp["error"]
        parts = [c.get("text", "") for c in resp.get("result", {}).get("content", [])
                 if c.get("type") == "text"]
        return "".join(parts), None

    def close(self):
        if not self.proc:
            return
        try:
            self.proc.stdin.close()
        except Exception:
            pass
        try:
            self.proc.wait(timeout=10)
        except Exception:
            try:
                self.proc.kill()
            except Exception:
                pass
