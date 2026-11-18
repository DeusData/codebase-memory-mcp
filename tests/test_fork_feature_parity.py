#!/usr/bin/env python3
"""Compare the original and renamed MCP servers in isolated accounts.

Use --baseline with a trusted pre-rename fork binary to include Neo4j features.
No user account, project, or installed client configuration is modified.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import queue
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request


class Client:
    def __init__(self, binary, home, repo, launch_args=(), ui=False):
        home.mkdir(mode=0o700)
        self.home = home
        runtime = home / 'run'
        runtime.mkdir(mode=0o700)
        self.env = {k: v for k, v in os.environ.items()
                    if not k.startswith(('CBM_', 'NEO4J_'))}
        self.env.update(HOME=str(home), USERPROFILE=str(home), CODEX_HOME=str(home / '.codex'),
                        XDG_CONFIG_HOME=str(home / '.config'), CBM_ALLOWED_ROOT=str(repo.parent),
                        CBM_CACHE_DIR=str(home / 'upstream-cache'),
                        CBM_NEO4J_CACHE_DIR=str(home / 'fork-cache'),
                        CBM_RUNTIME_DIR=str(runtime), CBM_NEO4J_RUNTIME_DIR=str(runtime))
        self.binary = str(binary.resolve())
        self.ui_port = None
        if ui:
            with socket.socket() as listener:
                listener.bind(('127.0.0.1', 0))
                self.ui_port = listener.getsockname()[1]
            launch_args = ["--ui=true", f"--port={self.ui_port}"]
        else:
            launch_args = [*launch_args, '--ui=false']
        self.log = (home / 'stderr.log').open('wb')
        self.process = subprocess.Popen([self.binary, *launch_args], cwd=repo, env=self.env,
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=self.log, text=True, bufsize=1)
        self.responses = queue.Queue()
        self.counter = 0
        threading.Thread(target=self.reader, daemon=True).start()

    def reader(self):
        for line in self.process.stdout:
            try:
                self.responses.put(json.loads(line))
            except json.JSONDecodeError:
                if line.strip():
                    self.responses.put({'protocol_error': line.strip()})

    def send(self, message):
        self.process.stdin.write(json.dumps(message) + '\n')
        self.process.stdin.flush()

    def call(self, method, params=None, allow_error=False):
        self.counter += 1
        self.send(dict(jsonrpc='2.0', id=self.counter, method=method, params=params or {}))
        while True:
            response = self.responses.get(timeout=120)
            assert 'protocol_error' not in response, response
            if response.get('id') != self.counter:
                continue
            assert 'error' not in response, (method, response)
            result = response['result']
            assert allow_error or not result.get('isError'), (method, params, result)
            return result

    def tool(self, tool_name, **arguments):
        return self.call('tools/call', dict(name=tool_name, arguments=arguments))

    def close(self):
        if self.process.poll() is None:
            self.process.stdin.close()
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        subprocess.run([self.binary, 'daemon', 'stop'], env=self.env,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)
        self.log.close()


def normalize(value):
    if isinstance(value, str):
        return value.replace('.codebase-memory-neo4j/', '.codebase-memory/').replace(
            'codebase-memory-mcp-neo4j', 'codebase-memory-mcp').replace(
            'codebase-memory-neo4j', 'codebase-memory-mcp')
    if isinstance(value, list):
        return [normalize(v) for v in value]
    if isinstance(value, dict):
        return {normalize(k): normalize(v) for k, v in value.items()}
    return value


def catalog(client):
    init = client.call('initialize', dict(protocolVersion='2024-11-05', capabilities={},
                                        clientInfo=dict(name='fork-parity', version='1')))
    client.server_name = init['serverInfo']['name']
    client.send(dict(jsonrpc='2.0', method='notifications/initialized', params={}))
    client.call('ping')
    result = {'capabilities': init['capabilities']}
    result['tools'] = client.call('tools/list')['tools']
    result['prompts'] = client.call('prompts/list')['prompts']
    return result


def data(result):
    if 'structuredContent' in result:
        return result['structuredContent']
    texts = [c.get('text', '') for c in result.get('content', [])]
    try:
        return json.loads('\n'.join(texts))
    except json.JSONDecodeError:
        return '\n'.join(texts)


def strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for v in value.values():
            yield from strings(v)
    elif isinstance(value, list):
        for v in value:
            yield from strings(v)


def check_http(client, project):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    deadline = time.monotonic() + 30
    while True:
        try:
            with opener.open(f'http://127.0.0.1:{client.ui_port}/', timeout=5) as response:
                assert 'text/html' in response.headers['Content-Type']
                assert response.read()
            break
        except OSError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.05)
    message = dict(jsonrpc='2.0', id=1, method='tools/call',
                   params=dict(name='list_projects', arguments={'format': 'json'}))
    request = urllib.request.Request(f'http://127.0.0.1:{client.ui_port}/rpc',
                                    data=json.dumps(message).encode(),
                                    headers={'Content-Type': 'application/json'})
    with opener.open(request, timeout=15) as response:
        projects = json.load(response)
    assert project in json.dumps(projects), projects
    for endpoint in ('/api/ui-config', f'/api/project-health?name={project}'):
        with opener.open(f'http://127.0.0.1:{client.ui_port}{endpoint}', timeout=10) as response:
            assert response.status == 200
            json.load(response)


def exercise(client, repo):
    results = {}

    def tool(tool_name, **args):
        result = data(client.tool(tool_name, **args))
        results.setdefault(tool_name, []).append(result)
        return result

    project = 'parity'
    indexed = tool('index_repository', repo_path=str(repo), name=project,
                   mode='fast', persistence=True)
    assert indexed.get('project') == project, indexed
    artifact = '.codebase-memory-neo4j' if client.server_name.endswith('-neo4j') else '.codebase-memory'
    assert (repo / artifact / 'graph.db.zst').is_file(), 'Persisted graph artifact missing'
    listing = tool('list_projects', format='json')
    assert project in json.dumps(listing), listing
    if client.ui_port:
        check_http(client, project)
    tool('index_status', project=project, format='json')
    tool('check_index_coverage', project=project, paths=['sample.py'], format='json')
    discovered = tool('search_graph', project=project, name_pattern='parity_compute',
                      format='json')
    assert 'parity_compute' in json.dumps(discovered), discovered
    cli = subprocess.run([client.binary, 'cli', 'search_graph', '--project', project,
                          '--name-pattern', 'parity_compute', '--format', 'json'],
                         env=client.env, cwd=repo, text=True, capture_output=True, timeout=120)
    assert cli.returncode == 0 and 'parity_compute' in cli.stdout, (cli.stdout, cli.stderr)
    for command in (['set', 'watcher_enabled', 'false'], ['get', 'watcher_enabled'],
                    ['reset', 'watcher_enabled']):
        configured = subprocess.run([client.binary, 'config', *command], env=client.env,
                                    cwd=repo, text=True, capture_output=True, timeout=30)
        assert configured.returncode == 0, (command, configured.stdout, configured.stderr)
        if command[0] == 'get':
            assert configured.stdout.strip() == 'false', configured.stdout
    queried = tool('query_graph', project=project,
                   query='MATCH (n:Function) RETURN n.qualified_name AS qn ORDER BY qn',
                   format='json')
    qns = {s for s in strings(queried) if s.endswith('.parity_compute') or s.endswith('.parity_main')}
    assert len(qns) == 2, queried
    for marker in ('c_compute', 'GoCompute', 'javaCompute', 'tsCompute', 'rust_compute',
                   'CsCompute', 'ruby_compute'):
        # Class methods and free functions need not share the same graph label.
        symbol = tool('search_graph', project=project, name_pattern=marker, format='json')
        assert marker in json.dumps(symbol), (f'Missing language symbol: {marker}', symbol)
    paged = tool('query_graph', project=project,
                 query='MATCH (n:Function) RETURN n.qualified_name AS qn ORDER BY qn',
                 max_rows=1, format='json')
    assert paged.get('has_more') and paged.get('returned') == 1, paged
    cursor = paged.get('next_cursor')
    assert cursor, paged
    continued = tool('query_graph', project=project,
                     query='MATCH (n:Function) RETURN n.qualified_name AS qn ORDER BY qn',
                     cursor=cursor, max_rows=1, format='json')
    assert continued['rows'] == queried['rows'][1:2], continued
    compute = next(q for q in qns if q.endswith('.parity_compute'))
    caller = next(q for q in qns if q.endswith('.parity_main'))
    trace = tool('trace_path', project=project, function_name=caller,
                 direction='outbound', format='json')
    assert 'parity_compute' in json.dumps(trace), trace
    snippet = tool('get_code_snippet', project=project, qualified_name=compute, format='json')
    assert 'return x + 1' in json.dumps(snippet), snippet
    outline = tool('get_file_outline', project=project, file_path='sample.py', format='json')
    assert 'parity_compute' in json.dumps(outline), outline
    tool('get_graph_schema', project=project, format='json')
    tool('get_architecture', project=project, format='json')
    tool('index_repository', repo_path=str(repo.parent / 'comparison'), name='parity-comparison',
         mode='fast')
    tool('compare_graphs', base_project=project, target_project='parity-comparison')
    code = tool('search_code', project=project, pattern='parity_compute', format='json')
    assert 'parity_compute' in json.dumps(code), code
    (repo / 'sample.py').write_text((repo / 'sample.py').read_text().replace('x + 1', 'x + 2'))
    tool('detect_changes', project=project, scope='impact', base_branch='HEAD', format='json')
    (repo / 'sample.py').write_text((repo / 'sample.py').read_text().replace('x + 2', 'x + 1'))
    tool('manage_adr', project=project, mode='update', content='## PURPOSE\nParity test.\n')
    adr = tool('manage_adr', project=project, mode='get', format='json')
    assert 'Parity test.' in json.dumps(adr), adr
    tool('manage_adr', project=project, mode='outline', format='json')
    tool('manage_adr', project=project, mode='set_sections',
         section_updates={'PURPOSE': 'Updated parity test.'})
    adr = tool('manage_adr', project=project, mode='get', format='json')
    assert 'Updated parity test.' in json.dumps(adr), adr
    tool('ingest_traces', project=project,
         traces=[{'caller': caller, 'callee': compute, 'count': 3}], label='parity')
    for prompt, extra in [('explore_codebase', {'question': 'Where is parity_compute?'}),
                          ('review_change_impact', {'change': 'Change parity_compute'})]:
        rendered = client.call('prompts/get', dict(name=prompt, arguments={'project': project, **extra}))
        assert rendered.get('messages'), rendered
    for mode in ('moderate', 'full'):
        tool('index_repository', repo_path=str(repo), name=project, mode=mode)
    job = tool('index_repository', repo_path=str(repo), name=project, mode='fast', **{'async': True})
    assert job.get('state') in ('queued', 'running', 'succeeded'), job
    deadline = time.monotonic() + 120
    while True:
        job = tool('index_repository', repo_path=str(repo), name=project, status=True)
        if job.get('state') in ('succeeded', 'failed', 'cancelled'):
            assert job['state'] == 'succeeded', job
            break
        assert time.monotonic() < deadline, job
        time.sleep(0.05)
    tool('index_repository', repo_path=str(repo), name=project,
         mode='cross-repo-intelligence', target_projects=['*'])
    tool('delete_project', project=project)
    tool('delete_project', project='parity-comparison')
    empty = tool('list_projects', format='json')
    assert project not in json.dumps(empty), empty
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--fork', type=Path, default=Path('build/c/codebase-memory-mcp-neo4j'))
    parser.add_argument('--catalog-only', action='store_true')
    parser.add_argument('--ui-baseline', type=Path,
                        help='Optional trusted release binary with embedded UI assets')
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='cbm-feature-parity-') as folder:
        work = Path(folder)
        repo = work / 'fixture'
        repo.mkdir()
        (repo / 'sample.py').write_text('def parity_compute(x):\n    return x + 1\n\n'
                                      'def parity_main():\n    return parity_compute(41)\n')
        (repo / 'README.md').write_text('# Parity fixture\nCalls parity_compute.\n')
        fixtures = {
            'math_c.c': 'int c_compute(int x) { return x + 1; }\n',
            'math_go.go': 'package parity\nfunc GoCompute(x int) int { return x + 1 }\n',
            'MathJava.java': 'class MathJava { int javaCompute(int x) { return x + 1; } }\n',
            'math_ts.ts': 'export function tsCompute(x: number): number { return x + 1; }\n',
            'math_rust.rs': 'pub fn rust_compute(x: i32) -> i32 { x + 1 }\n',
            'MathCs.cs': 'class MathCs { public int CsCompute(int x) { return x + 1; } }\n',
            'math_ruby.rb': 'def ruby_compute(x)\n  x + 1\nend\n',
            'doc.rst': 'Parity documentation\n====================\n\nA fixture without Sphinx configuration.\n',
            'empty_package.pl': 'print "parity";\n',
            'Dockerfile': 'FROM python:3.12-slim\nCOPY sample.py /app/sample.py\nCMD ["python", "/app/sample.py"]\n',
            'compose.yaml': 'services:\n  app:\n    build: .\n    image: parity-app:local\n',
            'infra.tf': 'resource "aws_s3_bucket" "parity" { bucket = "parity-fixture" }\n',
        }
        for path, content in fixtures.items():
            (repo / path).write_text(content)
        (repo / '.gitignore').write_text('.codebase-memory/\n.codebase-memory-neo4j/\n')
        git_env = {k: v for k, v in os.environ.items()
                   if k not in ('GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE')}
        for command in (["init", "-q"], ["add", "."],
                        ['-c', 'user.name=Parity fixture', '-c', 'user.email=fixture@example.invalid',
                         '-c', 'commit.gpgsign=false', 'commit', '-qm', 'Fixture']):
            subprocess.run(['git', '-C', str(repo), *command], env=git_env, check=True)
        subprocess.run(['git', 'clone', '-q', str(repo), str(work / 'comparison')],
                       env=git_env, check=True)
        clients = []
        try:
            for name, binary in [('baseline', args.baseline), ('fork', args.fork)]:
                clients.append(Client(binary, work / name, repo))
            catalogs = [catalog(c) for c in clients]
            for field in ('capabilities', 'tools', 'prompts'):
                assert normalize(catalogs[0][field]) == normalize(catalogs[1][field]), (
                    f'MCP {field} differ; inspect the catalogs for missing or changed contracts')
            report = {'catalog': catalogs[1], 'checks': ['initialize', 'ping', 'tool schema parity']}
            report['binary_sha256'] = {}
            for label, binary in [('baseline', args.baseline), ('fork', args.fork)]:
                with binary.open('rb') as stream:
                    report['binary_sha256'][label] = hashlib.file_digest(stream, 'sha256').hexdigest()
            if not args.catalog_only:
                report['runs'] = [exercise(clients[0], repo)]
                upstream_artifacts = {p: p.read_bytes() for p in (repo / '.codebase-memory').rglob('*')
                                      if p.is_file()}
                assert upstream_artifacts, 'Baseline did not leave a persisted artifact fixture'
                report['runs'].append(exercise(clients[1], repo))
                for path, before in upstream_artifacts.items():
                    assert path.read_bytes() == before, f'Upstream artifact changed: {path}'
                assert clients[1].server_name == 'codebase-memory-mcp-neo4j'
                for c, expected_cache, foreign_cache in [
                        (clients[0], 'upstream-cache', 'fork-cache'),
                        (clients[1], 'fork-cache', 'upstream-cache')]:
                    assert (c.home / expected_cache).is_dir()
                    assert not (c.home / foreign_cache).exists(), 'Cache overrides are not isolated'
                expected = {t['name'] for t in catalogs[0]['tools']}
                for result in report['runs']:
                    assert set(result) == expected, ('Unexercised tools', expected - set(result))
                stable = ('list_projects', 'search_graph', 'trace_path',
                          'get_code_snippet', 'get_file_outline', 'get_graph_schema',
                          'get_architecture', 'detect_changes', 'manage_adr', 'ingest_traces',
                          'delete_project')
                for tool_name in stable:
                    assert normalize(report['runs'][0][tool_name]) == normalize(
                        report['runs'][1][tool_name]), f'{tool_name}: semantic result differs'
                assert report['runs'][0]['query_graph'][0] == report['runs'][1]['query_graph'][0]
                assert report['runs'][0]['query_graph'][1]['rows'] == report['runs'][1]['query_graph'][1]['rows']
                assert report['runs'][0]['query_graph'][2]['rows'] == report['runs'][1]['query_graph'][2]['rows']
                report['checks'].append('all advertised tools exercised on both executables')
                report['checks'].append('stable graph/source/ADR/trace results match exactly')
                report['checks'].extend(['CLI graph search', 'configuration set/get/reset',
                                         'query cursor pagination', 'all indexing modes',
                                         'background indexing and status polling'])
                report['checks'].append('separate persisted graph and cache overrides')
                report['profiles'] = {}
                for profile in ('analysis', 'scout'):
                    profiles = []
                    for label, binary in [('baseline', args.baseline), ('fork', args.fork)]:
                        restricted = Client(binary, work / (label + '-' + profile), repo,
                                            [f'--tool-profile={profile}'])
                        try:
                            profiles.append(catalog(restricted))
                            rejected = restricted.call('tools/call',
                                dict(name='index_repository', arguments={'repo_path': str(repo)}),
                                allow_error=True)
                            assert rejected.get('isError') is True, (profile, rejected)
                            projects = data(restricted.tool('list_projects', format='json'))
                            assert 'parity' not in json.dumps(projects), projects
                        finally:
                            restricted.close()
                    assert normalize(profiles[0]) == normalize(profiles[1]), profile
                    names = {t['name'] for t in profiles[1]['tools']}
                    assert not names & {'index_repository', 'delete_project', 'manage_adr',
                                        'ingest_traces'}, (profile, names)
                    report['profiles'][profile] = sorted(names)
                if args.ui_baseline:
                    for label, binary in [('upstream-ui', args.ui_baseline), ('fork-ui', args.fork)]:
                        ui_client = Client(binary, work / label, repo, ui=True)
                        try:
                            ui_catalog = catalog(ui_client)
                            if label == 'upstream-ui':
                                fork_tools = {t['name']: t for t in catalogs[1]['tools']}
                                for old in ui_catalog['tools']:
                                    assert old['name'] in fork_tools, old['name']
                                    new_schema = fork_tools[old['name']]['inputSchema']
                                    old_schema = old['inputSchema']
                                    assert set(new_schema.get('required', [])) <= set(old_schema.get('required', [])), old['name']
                                    for key, prop in old_schema.get('properties', {}).items():
                                        assert key in new_schema['properties'], (old['name'], key)
                                        replacement = new_schema['properties'][key]
                                        assert prop.get('type') == replacement.get('type'), (old['name'], key)
                                        if 'enum' in prop:
                                            assert set(prop['enum']) <= set(replacement.get('enum', [])), (old['name'], key)
                            ui_client.tool('index_repository', repo_path=str(repo),
                                           name='parity', mode='fast')
                            check_http(ui_client, 'parity')
                            ui_client.tool('delete_project', project='parity')
                        finally:
                            ui_client.close()
                    report['checks'].append('embedded UI HTTP projects/config/health')
                    report['checks'].append('official upstream tool/argument compatibility')
            if args.report:
                args.report.write_text(json.dumps(report, indent=2) + '\n')
            print(json.dumps(report, indent=2))
        finally:
            for c in reversed(clients):
                c.close()


if __name__ == '__main__':
    main()
