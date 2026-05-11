#!/usr/bin/env python3
"""Unit tests for scripts/uq-publish.py.

Stdlib-only — no pytest. Run:
    python3 scripts/test_uq_publish.py
"""
from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

_SCRIPT = Path(__file__).resolve().parent / "uq-publish.py"
_spec = importlib.util.spec_from_file_location("uq_publish", _SCRIPT)
uq = importlib.util.module_from_spec(_spec)
sys.modules["uq_publish"] = uq
_spec.loader.exec_module(uq)


class StampMetadataTests(unittest.TestCase):
    def test_required_fields_set(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            subprocess.run(["git", "init", "-q", "-b", "main"], cwd=tmp, check=True)
            subprocess.run(["git", "config", "user.email", "t@t"], cwd=tmp, check=True)
            subprocess.run(["git", "config", "user.name", "t"], cwd=tmp, check=True)
            (tmp / "f").write_text("x")
            subprocess.run(["git", "add", "."], cwd=tmp, check=True)
            subprocess.run(["git", "commit", "-q", "-m", "i", "--no-gpg-sign"],
                           cwd=tmp, check=True)
            sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=tmp,
                                 capture_output=True, text=True, check=True
                                 ).stdout.strip()

            graph = {"schema": "gitnexus@1", "nodes": [], "links": []}
            md = uq.stamp_metadata(graph, repo_dir=tmp, tool_version="0.0.0-test")
            self.assertEqual(md["tool"], "codebase-memory-mcp")
            self.assertEqual(md["tool_version"], "0.0.0-test")
            self.assertEqual(md["commit"], sha)
            self.assertTrue(md["generated_at"].endswith("Z"))


class BuildGraphTests(unittest.TestCase):
    def test_projects_query_graph_rows_to_gitnexus_at_1(self) -> None:
        node_rows = [{"id": 1, "name": "AuthService", "kind": "Module"}]
        edge_rows = [{"source": 1, "target": 2, "kind": "DEPENDS_ON"}]

        def fake_query_rows(_binary: str, _project: str, query: str):
            return node_rows if "id(n)" in query else edge_rows

        with mock.patch.object(uq, "_query_rows", side_effect=fake_query_rows):
            graph = uq.build_graph("/fake/binary", "demo")
        self.assertEqual(graph["schema"], "gitnexus@1")
        self.assertEqual(len(graph["nodes"]), 1)
        self.assertEqual(graph["nodes"][0]["label"], "AuthService")
        self.assertEqual(graph["nodes"][0]["kind"], "Module")
        self.assertEqual(len(graph["links"]), 1)
        self.assertEqual(graph["links"][0]["source"], "1")
        self.assertEqual(graph["links"][0]["kind"], "DEPENDS_ON")


class McpCallUnwrapTests(unittest.TestCase):
    """`_mcp_call` must unwrap the MCP `{content:[{type:'text',text:'...'}]}` envelope."""

    def _mcp_response(self, payload: dict, is_error: bool = False) -> bytes:
        env = {
            "jsonrpc": "2.0", "id": 2,
            "result": {
                "content": [{"type": "text", "text": json.dumps(payload)}],
                "isError": is_error,
            },
        }
        return (json.dumps({"jsonrpc": "2.0", "id": 1, "result": {}}).encode()
                + b"\n" + json.dumps(env).encode() + b"\n")

    def test_unwraps_text_content_and_decodes_json(self) -> None:
        fake_proc = mock.MagicMock(stdout=self._mcp_response({"rows": [{"a": 1}]}))
        with mock.patch.object(uq.subprocess, "run", return_value=fake_proc):
            out = uq._mcp_call("/fake/bin", "query_graph", {"project": "p", "query": "q"})
        self.assertEqual(out, {"rows": [{"a": 1}]})

    def test_is_error_raises(self) -> None:
        fake_proc = mock.MagicMock(stdout=self._mcp_response({"msg": "boom"}, is_error=True))
        with mock.patch.object(uq.subprocess, "run", return_value=fake_proc):
            with self.assertRaises(RuntimeError):
                uq._mcp_call("/fake/bin", "query_graph", {"project": "p", "query": "q"})


if __name__ == "__main__":
    unittest.main()
