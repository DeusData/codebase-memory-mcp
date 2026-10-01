import { describe, expect, it, vi } from 'vitest';
import { isComposePath, loadContainerInventory, loadContainerTopology, readContainerSource, type ContainerInventory } from './container-source';
import type { RpcIntelligenceClient } from '../provider/rpc-client';

const inventory: ContainerInventory = { project: 'demo', rootPath: '/demo', files: new Map([
    ['compose.yml', 'demo.compose'], ['api/Dockerfile', 'demo.docker'], ['api/main.py', 'demo.main'], ['.env', 'demo.env'],
]), manifests: ['compose.yml'], warnings: [] };
const signal = () => new AbortController().signal;
const client = (methods: object) => methods as RpcIntelligenceClient;

describe('container source inspection', () => {
    it('recognizes Compose variants while requiring explicit selection', () => {
        expect(isComposePath('deploy/docker-compose.dev.yaml')).toBe(true);
        expect(isComposePath('compose.yml')).toBe(true);
        expect(isComposePath('package.json')).toBe(false);
        expect(isComposePath('somecompose.yml')).toBe(false);
    });
    it('uses indexed qualified identities and reports the file discovery bound', async () => {
        const queryRows = vi.fn(async (_project: string, _query: string) => Array.from({ length: 4001 }, (_, i) => ({ path: i ? `src/${i}.ts` : 'compose.yml', qn: `qn${i}` })));
        const result = await loadContainerInventory({ name: 'p', root_path: '/p' }, client({ queryRows }));
        expect(result.files.size).toBe(4000); expect(result.manifests).toEqual(['compose.yml']);
        expect(result.warnings).toHaveLength(1); expect(queryRows.mock.calls[0][0]).toBe('p');
    });
    it('rejects incomplete YAML instead of constructing a partial service declaration', async () => {
        const getCodeSnippet = vi.fn(async () => ({ source: 'services:\n  web:', start_line: 1, source_truncated: true }));
        await expect(readContainerSource(inventory, 'compose.yml', client({ getCodeSnippet }), signal())).rejects.toThrow('incomplete');
    });
    it('reads a verified module span rather than accepting an unflagged 51-line File preview', async () => {
        const queryRows = vi.fn(async (_project: string, query: string) => query.includes('f:Module')
            ? [{ path: 'compose.yml', qn: 'module', first_line: '1', last_line: '97' }]
            : [{ path: 'compose.yml', qn: 'file' }]);
        const result = await loadContainerInventory({ name: 'p', root_path: '/p' }, client({ queryRows }));
        expect(result.files.get('compose.yml')).toBe('module');
        const getCodeSnippet = vi.fn(async () => ({ source: 'services:', start_line: 1, end_line: 51 }));
        await expect(readContainerSource(result, 'compose.yml', client({ getCodeSnippet }), signal())).rejects.toThrow('incomplete');
        const withoutSpan = { ...result, sourceEnds: new Map<string, number>() };
        await expect(readContainerSource(withoutSpan, 'compose.yml', client({ getCodeSnippet }), signal())).rejects.toThrow('module span');
    });
    it('rejects a repeated source page and unknown source paths', async () => {
        const getCodeSnippet = vi.fn(async () => ({ source: 'services:', start_line: 1, end_line: 1, next_start_line: 1 }));
        await expect(readContainerSource(inventory, 'compose.yml', client({ getCodeSnippet }), signal())).rejects.toThrow('changed');
        await expect(readContainerSource(inventory, '../other/compose.yml', client({ getCodeSnippet }), signal())).rejects.toThrow('outside');
        expect(getCodeSnippet).toHaveBeenCalledTimes(1);
    });
    it('reads complete indexed class ranges when a whole-file module is unavailable, preserving source lines', async () => {
        const queryRows = vi.fn(async (_project: string, query: string) => query.includes('f:File')
            ? [{ path: 'api/Client.java', qn: 'file' }]
            : query.includes('f:Class') ? [
                { path: 'api/Client.java', qn: 'outer', first_line: '5', last_line: '8' },
                { path: 'api/Client.java', qn: 'inner', first_line: '6', last_line: '7' },
            ] : []);
        const result = await loadContainerInventory({ name: 'p', root_path: '/p' }, client({ queryRows }));
        const getCodeSnippet = vi.fn(async () => ({ source: 'class Client {\n  String host = "cache";\n  void send() {}\n}', start_line: 5, end_line: 8 }));
        const source = await readContainerSource(result, 'api/Client.java', client({ getCodeSnippet }), signal());
        expect(source.split('\n')[4]).toBe('class Client {');
        expect(source.split('\n')[5]).toContain('"cache"');
        expect(getCodeSnippet).toHaveBeenCalledTimes(1);
        expect(getCodeSnippet).toHaveBeenCalledWith('p', 'outer', { startLine: 5, maxLines: 500 });
        expect(result.sourceEnds?.has('api/Client.java')).toBe(false);
    });
    it('never uses a class-range fallback to accept an incomplete Compose document', async () => {
        const queryRows = vi.fn(async (_project: string, query: string) => query.includes('f:File')
            ? [{ path: 'compose.yml', qn: 'file' }]
            : query.includes('f:Class') ? [{ path: 'compose.yml', qn: 'misclassified', first_line: '5', last_line: '8' }] : []);
        const result = await loadContainerInventory({ name: 'p', root_path: '/p' }, client({ queryRows }));
        const getCodeSnippet = vi.fn();
        await expect(readContainerSource(result, 'compose.yml', client({ getCodeSnippet }), signal())).rejects.toThrow();
        expect(getCodeSnippet).not.toHaveBeenCalled();
    });
    it('shares the bounded source budget across services instead of starving later build contexts', async () => {
        const files = new Map<string, string>([['compose.yml', 'compose']]);
        for (const name of ['a', 'b', 'c']) for (let i = 0; i < (name === 'a' ? 80 : 2); i++) files.set(`${name}/${i}.py`, `${name}/${i}`);
        const getCodeSnippet = vi.fn(async (_project: string, qn: string) => ({
            source: qn === 'compose' ? 'services:\n  a:\n    build: ./a\n  b:\n    build: ./b\n  c:\n    build: ./c\n' : 'pass', start_line: 1,
        }));
        const queryGraph = vi.fn(async () => ({ columns: [], rows: [], total: 0 }));
        const result = await loadContainerTopology([{ inventory: { project: 'p', rootPath: '/p', files, manifests: ['compose.yml'], warnings: [] }, manifest: 'compose.yml' }], client({ getCodeSnippet, queryGraph }), signal());
        const inspected = getCodeSnippet.mock.calls.map(call => call[1]);
        expect(inspected).toContain('b/0'); expect(inspected).toContain('b/1');
        expect(inspected).toContain('c/0'); expect(inspected).toContain('c/1');
        expect(result.filesRead).toBe(81);
        expect(result.warnings.some(warning => warning.includes('80 of 84'))).toBe(true);
    });
    it('does not read host environment files and only reads code within mapped build inputs', async () => {
        const content: Record<string, string> = { 'demo.compose': 'services:\n  web:\n    build: ./api\n  cache:\n    image: redis:alpine\n', 'demo.docker': 'FROM python:3\nCOPY . /app', 'demo.main': 'import redis\nr = redis.Redis(host="cache")' };
        const getCodeSnippet = vi.fn(async (_project: string, qn: string) => ({ source: content[qn], start_line: 1 }));
        const queryGraph = vi.fn(async () => ({ columns: [], rows: [], total: 0 }));
        const result = await loadContainerTopology([{ inventory, manifest: 'compose.yml' }], client({ getCodeSnippet, queryGraph }), signal());
        expect(getCodeSnippet.mock.calls.map(call => call[1])).toEqual(['demo.compose', 'demo.docker', 'demo.main']);
        expect(result.topology.services).toHaveLength(2); expect(result.filesRead).toBe(3);
    });
    it('stops before requesting source after cancellation', async () => {
        const controller = new AbortController(); controller.abort(); const getCodeSnippet = vi.fn();
        await expect(readContainerSource(inventory, 'compose.yml', client({ getCodeSnippet }), controller.signal)).rejects.toMatchObject({ name: 'AbortError' });
        expect(getCodeSnippet).not.toHaveBeenCalled();
    });
});
