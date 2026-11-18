#!/usr/bin/env python3
"""Exercise install/update/uninstall beside a real upstream binary on Linux.

Usage: python3 tests/test_fork_coexistence.py --upstream /path/to/upstream
Run under a child subreaper in containers whose PID 1 does not reap orphans.
Uses only a temporary account, a checksum-verified local release, and loopback.
"""
import argparse
import functools
import hashlib
import http.server
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import threading
import tomllib

ROOT = Path(__file__).resolve().parents[1]
FORK = 'codebase-memory-mcp-neo4j'
ID = 'codebase-memory-neo4j'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fork', type=Path, default=ROOT / 'build/c' / FORK)
    parser.add_argument('--upstream', type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='cbm-coexist-') as directory:
        work = Path(directory)
        home = work / 'home'
        home.mkdir(mode=0o700)
        bindir = home / '.local/bin'
        bindir.mkdir(parents=True)
        runtime = work / 'run'
        runtime.mkdir(mode=0o700)
        env = os.environ.copy()
        for key in list(env):
            if key.startswith('CBM_'):
                del env[key]
        env.update(HOME=str(home), USERPROFILE=str(home), CODEX_HOME=str(home / '.codex'),
                   XDG_CONFIG_HOME=str(home / '.config'),
                   CBM_RUNTIME_DIR=str(runtime), CBM_NEO4J_RUNTIME_DIR=str(runtime),
                   CBM_CACHE_DIR=str(home / '.cache/codebase-memory-mcp'),
                   PATH=str(bindir) + os.pathsep + env['PATH'])
        env.pop('CLAUDE_CONFIG_DIR', None)
        # Deliberately no fork cache override: upstream's setting must be ignored.
        for client in ['.claude', '.codex', '.cursor', '.pi/agent', '.config/opencode']:
            (home / client).mkdir(parents=True, exist_ok=True)

        (home / '.claude.json').write_text('{"mcpServers":{}}\n')
        (home / '.codex/config.toml').write_text('')
        (home / '.cursor/mcp.json').write_text('{"mcpServers":{}}\n')

        def run(command, check=True):
            result = subprocess.run([str(v) for v in command], env=env, cwd=home,
                                    text=True, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, timeout=120)
            if check and result.returncode:
                raise AssertionError(f'{command} exited {result.returncode}:\n{result.stdout}')
            return result.stdout

        upstream = bindir / 'codebase-memory-mcp'
        fork = bindir / FORK
        run([args.upstream.resolve(), 'install', '-y', '--force', f'--dir={bindir}',
             '--clients=claude,codex,cursor,pi,opencode'])
        assert upstream.exists()
        upstream_hash = hashlib.file_digest(upstream.open('rb'), 'sha256').hexdigest()
        # Upstream updater and resources must never be overwritten by this fork.
        updater = bindir / 'install.sh'
        updater.write_text('# upstream updater sentinel\n')
        snapshots = {}
        for directory_name in ['.claude/hooks', '.claude/skills', '.claude/agents',
                               '.codex/skills', '.codex/agents', '.cursor/skills', '.cursor/agents',
                               '.pi/agent/extensions', '.config/opencode/plugins',
                               '.config/opencode/skills', '.config/opencode/agents']:
            for file in (home / directory_name).rglob('*'):
                if file.is_file():
                    snapshots[file] = file.read_bytes()
        claude = home / '.claude.json'
        codex = home / '.codex/config.toml'
        original_claude = json.loads(claude.read_text())['mcpServers']['codebase-memory-mcp']
        original_codex = tomllib.loads(codex.read_text())['mcp_servers']['codebase-memory-mcp']
        cursor = home / '.cursor/mcp.json'
        original_cursor = json.loads(cursor.read_text())['mcpServers']['codebase-memory-mcp']
        release = work / 'release'
        release.mkdir()
        archive = release / f'{FORK}-linux-amd64-portable.tar.gz'
        notices = work / 'THIRD_PARTY_NOTICES.md'
        notices.write_text('Local coexistence test fixture\n')
        with tarfile.open(archive, 'w:gz', compresslevel=1) as package:
            for source, name in [(args.fork.resolve(), FORK), (ROOT / 'install.sh', 'install.sh'),
                                 (ROOT / 'LICENSE', 'LICENSE'), (notices, notices.name)]:
                package.add(source, arcname=name)
        digest = hashlib.file_digest(archive.open('rb'), 'sha256').hexdigest()
        (release / 'checksums.txt').write_text(f'{digest}  {archive.name}\n')
        handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(release))
        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        env['CBM_DOWNLOAD_URL'] = f'http://127.0.0.1:{server.server_port}'
        try:
            for _ in range(2):  # repeat installation is also the update path
                run(['bash', ROOT / 'install.sh', f'--dir={bindir}',
                     '--clients=claude,codex,cursor,pi,opencode'])
                assert updater.read_text() == '# upstream updater sentinel\n'
                assert (bindir / 'install-neo4j.sh').read_bytes() == (ROOT / 'install.sh').read_bytes()
                c = json.loads(claude.read_text())['mcpServers']
                assert c['codebase-memory-mcp'] == original_claude
                assert c[ID]['command'] == str(fork)
                t = tomllib.loads(codex.read_text())['mcp_servers']
                assert t['codebase-memory-mcp'] == original_codex
                assert t[ID]['command'] == str(fork)
                assert json.loads(cursor.read_text())['mcpServers']['codebase-memory-mcp'] == original_cursor
                for file, data in snapshots.items():
                    assert file.read_bytes() == data, f'upstream resource changed: {file}'
            handoff = run([fork, 'update'])
            assert str(bindir / 'install-neo4j.sh') in handoff, handoff
            assert updater.read_text() == '# upstream updater sentinel\n'
            run([upstream, 'daemon', 'start'])
            run([fork, 'daemon', 'start'])
            assert 'active' in run([upstream, 'daemon', 'status'])
            assert 'active' in run([fork, 'daemon', 'status'])
            run([upstream, 'cli', 'list_projects'])
            run([fork, 'cli', 'list_projects'])
            assert (home / '.cache' / FORK).is_dir()
            run([fork, 'daemon', 'stop'])
            assert 'active' in run([upstream, 'daemon', 'status'])
            run([fork, 'uninstall', '-y'])
            assert not fork.exists()
            assert hashlib.file_digest(upstream.open('rb'), 'sha256').hexdigest() == upstream_hash
            assert json.loads(claude.read_text())['mcpServers']['codebase-memory-mcp'] == original_claude
            assert ID not in json.loads(claude.read_text())['mcpServers']
            assert tomllib.loads(codex.read_text())['mcp_servers']['codebase-memory-mcp'] == original_codex
            assert json.loads(cursor.read_text())['mcpServers']['codebase-memory-mcp'] == original_cursor
            for file, data in snapshots.items():
                assert file.read_bytes() == data, f'upstream resource removed: {file}'
            assert updater.read_text() == '# upstream updater sentinel\n'
            print('PASS: repeat install/update, distinct registrations/resources/cache/daemons, safe uninstall')
        finally:
            if fork.exists():
                run([fork, 'daemon', 'stop'], check=False)
            run([upstream, 'daemon', 'stop'], check=False)
            server.shutdown()
            server.server_close()


if __name__ == '__main__':
    main()
