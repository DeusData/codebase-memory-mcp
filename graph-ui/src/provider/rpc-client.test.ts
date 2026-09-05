import { describe, expect, it } from 'vitest';
import { RpcIntelligenceClient } from './rpc-client';
import { EngineError, EngineUnavailableError } from './engine-errors';
import { isAmbiguousPath } from './rpc-schemas';
import { FakeRpc, RECORDED_PROJECT, listProjectsRoute, rowsText } from '../test-support/rpc-recordings';
import type { Route } from '../test-support/rpc-recordings';

/*
 * Was diese Datei prueft, ist die Uebersetzung zwischen dem, was der Server
 * schickt, und dem, was der portierte Provider erwartet. Die Nutzlasten unten
 * sind Ausschnitte der Antworten, die cbm/build/c/codebase-memory-mcp am
 * 2026-08-28 auf POST /rpc geliefert hat.
 */

function client(routes: Route[]): { client: RpcIntelligenceClient; rpc: FakeRpc } {
    const rpc = new FakeRpc(routes);
    return { client: new RpcIntelligenceClient({ fetch: rpc.fetch }), rpc };
}

describe('tabellarische Abfragen', () => {

    it('liest die kompakte Zeilenform als Spalten und Zeilen', async () => {
        const { client: c } = client([{ tool: 'query_graph', recording: 'calls-out' }]);
        const result = await c.queryGraph(RECORDED_PROJECT, 'MATCH ...');
        expect(result.columns).toEqual([
            'b.name', 'b.qualified_name', 'b.file_path', 'b.start_line', 'r.line',
        ]);
        expect(result.rows).toHaveLength(6);
        expect(result.total).toBe(6);
        expect(result.rows[0][0]).toBe('validateUser');
        // Zahlenspalten kommen gequotet und sollen ohne Anfuehrungszeichen
        // ankommen, damit die Koerzierung des Providers greift.
        expect(result.rows[0][4]).toBe('24');
    });

    it('macht aus dem Leermarker der kompakten Form eine leere Zelle', async () => {
        const { client: c } = client([{ tool: 'query_graph', recording: 'env-reads' }]);
        const rows = await c.queryRows(RECORDED_PROJECT, 'MATCH ...');
        // Der Server schreibt hier `-`; mit `format: "json"` schreibt er an
        // derselben Stelle "". Ein Bindestrich als Dateiname waere eine
        // Erfindung dieser Schicht.
        expect(rows[0]['b.file_path']).toBe('');
        expect(rows[0]['r.line']).toBe('');
        expect(rows[0]['b.env_key']).toBe('DB_URL');
    });

    it('benennt die Zeilen nach den Spalten, die die Abfrage verlangt hat', async () => {
        const { client: c } = client([{ tool: 'query_graph', recording: 'calls-in' }]);
        const rows = await c.queryRows(RECORDED_PROJECT, 'MATCH ...');
        expect(rows[0]['a.is_test']).toBe('true');
        expect(rows[0]['a.file_path']).toBe('test/userService.test.ts');
    });

    it('fragt query_graph ohne format, weil die kompakte Form die Vorgabe ist', async () => {
        const { client: c, rpc } = client([{ tool: 'query_graph', recording: 'calls-out' }]);
        await c.queryGraph(RECORDED_PROJECT, 'MATCH ...');
        expect(rpc.calls[0].args['format']).toBeUndefined();
    });

    it('meldet eine unlesbare Antwort als Engine-Fehler, nicht als leere Tabelle', async () => {
        const { client: c } = client([{ tool: 'query_graph', text: 'nodes: 3\n  a\n' }]);
        await expect(c.queryGraph(RECORDED_PROJECT, 'MATCH ...')).rejects.toBeInstanceOf(EngineError);
    });
});

describe('die Werkzeuge, die von sich aus JSON liefern', () => {

    it('liest die Projektliste mit ihrer Wurzel', async () => {
        const { client: c } = client([listProjectsRoute()]);
        const result = await c.listProjects();
        expect(result.projects).toHaveLength(1);
        expect(result.projects[0].name).toBe(RECORDED_PROJECT);
        expect(result.projects[0].root_path).toContain('fixtures/atlas-sample');
    });

    it('liest den Quelltext eines Symbols samt Spanne', async () => {
        const { client: c } = client([{
            tool: 'get_code_snippet',
            json: {
                name: 'createUser',
                qualified_name: `${RECORDED_PROJECT}.src.services.userService.createUser`,
                label: 'Function',
                file_path: '/tmp/atlas-sample/src/services/userService.ts',
                start_line: 23,
                end_line: 36,
                source: 'export function createUser(input: unknown): User {\n}\n',
                callers: 2,
                callees: 6,
            },
        }]);
        const snippet = await c.getCodeSnippet(RECORDED_PROJECT, 'x');
        expect(snippet.start_line).toBe(23);
        expect(snippet.end_line).toBe(36);
        expect(snippet.source).toContain('export function createUser');
    });

    it('liest den Indexzustand', async () => {
        const { client: c } = client([{
            tool: 'index_status',
            json: { project: RECORDED_PROJECT, nodes: 76, edges: 178, status: 'ready' },
        }]);
        const status = await c.indexStatus(RECORDED_PROJECT);
        expect(status.status).toBe('ready');
        expect(status.nodes).toBe(76);
    });

    it('reicht die Coverage-Beilage ungedeutet weiter', async () => {
        // Die Beilage wird im Join des Explorers gelesen. Dass sie hier
        // unangetastet ankommt, ist die Bedingung dafuer.
        const payload = {
            project: RECORDED_PROJECT,
            nodes: 76,
            parse_partial: { files: [{ path: 'src/a.ts', error_ranges: '4-9' }], count: 1, truncated: false },
        };
        const { client: c } = client([{ tool: 'index_status', json: payload }]);
        expect(await c.indexStatusPayload(RECORDED_PROJECT)).toEqual(payload);
    });

    it('fragt den Coverage-Store nach Pfaden und schreibt die Argumente so, wie er sie kennt', async () => {
        const { client: c, rpc } = client([{ tool: 'check_index_coverage', json: { paths: [] } }]);
        await c.checkIndexCoverage(RECORDED_PROJECT, { paths: ['src/a.ts'] });
        expect(rpc.calls[0].args['paths']).toEqual(['src/a.ts']);
        expect(rpc.calls[0].args['scopes']).toBeUndefined();
        expect(rpc.calls[0].args['scope_limit']).toBeUndefined();
    });

    it('fragt ihn nach Scopes mit Seitengrenze und Versatz', async () => {
        const { client: c, rpc } = client([{ tool: 'check_index_coverage', json: { scopes: [] } }]);
        await c.checkIndexCoverage(RECORDED_PROJECT, {
            scopes: ['.'],
            scopeLimit: 1000,
            scopeOffset: 200,
        });
        expect(rpc.calls[0].args['scopes']).toEqual(['.']);
        expect(rpc.calls[0].args['scope_limit']).toBe(1000);
        expect(rpc.calls[0].args['scope_offset']).toBe(200);
    });

    it('meldet einen gesperrten Coverage-Aufruf als nicht angebotene Faehigkeit', async () => {
        const { client: c } = client([{ tool: 'check_index_coverage', notAllowed: true }]);
        await expect(c.checkIndexCoverage(RECORDED_PROJECT, { scopes: ['.'] }))
            .rejects.toBeInstanceOf(EngineUnavailableError);
    });
});

describe('die Werkzeuge, die erst auf Bitte JSON liefern', () => {

    it('bittet um format json, statt eine Anzeigeform zu parsen', async () => {
        const { client: c, rpc } = client([{ tool: 'index_status', json: { status: 'ready' } }]);
        await c.indexStatus(RECORDED_PROJECT);
        expect(rpc.calls[0].args['format']).toBe('json');
    });

    it('flacht den gruppierten Pfadlauf zu Hops mit qualifizierten Namen ab', async () => {
        const { client: c } = client([{
            tool: 'trace_path',
            json: {
                function: 'createUser',
                direction: 'both',
                callees_total: 7,
                callees: {
                    cols: ['name', 'hop'],
                    groups: [
                        { qn_prefix: `${RECORDED_PROJECT}.src.types`, rows: [['UserEntity', 1]] },
                        { qn_prefix: `${RECORDED_PROJECT}.src.repo.db`, rows: [['insert', 1], ['query', 2]] },
                    ],
                },
                callers_total: 1,
                callers: {
                    cols: ['name', 'hop'],
                    groups: [
                        { qn_prefix: `${RECORDED_PROJECT}.src.routes.users`, rows: [['registerUserRoutes', 1]] },
                    ],
                },
            },
        }]);
        const result = await c.tracePath(RECORDED_PROJECT, 'createUser');
        expect(isAmbiguousPath(result)).toBe(false);
        if (isAmbiguousPath(result)) {
            return;
        }
        expect(result.callees?.map((hop) => hop.qualified_name)).toEqual([
            `${RECORDED_PROJECT}.src.types.UserEntity`,
            `${RECORDED_PROJECT}.src.repo.db.insert`,
            `${RECORDED_PROJECT}.src.repo.db.query`,
        ]);
        expect(result.callees?.[2].hop).toBe(2);
        expect(result.callers).toHaveLength(1);
    });

    it('gibt die Ablehnung wegen Mehrdeutigkeit als Antwort weiter, nicht als Fehler', async () => {
        const { client: c } = client([{
            tool: 'trace_path',
            json: {
                status: 'ambiguous',
                message: '2 matches for "create".',
                suggestions: [
                    { qualified_name: `${RECORDED_PROJECT}.src.services.orderService.create`, name: 'create', label: 'Function', file_path: 'src/services/orderService.ts' },
                    { qualified_name: `${RECORDED_PROJECT}.src.services.userService.create`, name: 'create', label: 'Function', file_path: 'src/services/userService.ts' },
                ],
            },
        }]);
        const result = await c.tracePath(RECORDED_PROJECT, 'create');
        expect(isAmbiguousPath(result)).toBe(true);
        if (!isAmbiguousPath(result)) {
            return;
        }
        expect(result.suggestions).toHaveLength(2);
        expect(result.suggestions[0].file_path).toBe('src/services/orderService.ts');
    });

    it('bringt die Aspekte der Uebersicht auf die Form, die der Provider liest', async () => {
        const { client: c } = client([{
            tool: 'get_architecture',
            json: {
                project: RECORDED_PROJECT,
                total_nodes: 76,
                total_edges: 178,
                node_labels: { cols: ['label', 'count'], rows: [['Function', 18], ['File', 11]] },
                languages: { cols: ['language', 'files'], rows: [['TypeScript', 10]] },
                packages: { cols: ['name', 'nodes', 'fan_in', 'fan_out'], rows: [['services', 6, 0, 0]] },
                entry_points: { cols: ['qn', 'file'], rows: [[`${RECORDED_PROJECT}.src.server.main`, 'src/server.ts']] },
                boundaries: { cols: ['from', 'to', 'calls'], rows: [['services', 'repo', 4]] },
                file_tree: { cols: ['path', 'type', 'children'], rows: [['src', 'dir', 7], ['src/types.ts', 'file', 0]] },
            },
        }]);
        const overview = await c.getArchitecture(RECORDED_PROJECT);
        expect(overview.total_nodes).toBe(76);
        expect(overview.node_labels[0]).toEqual({ label: 'Function', count: 18 });
        // Der Server sagt `files`, `nodes` und `calls`; der Provider liest
        // `file_count`, `node_count` und `call_count`. Umbenannt wird hier.
        expect(overview.languages[0]).toEqual({ language: 'TypeScript', file_count: 10 });
        expect(overview.packages[0].node_count).toBe(6);
        expect(overview.boundaries[0].call_count).toBe(4);
        expect(overview.entry_points[0]).toEqual({
            name: 'main',
            qualified_name: `${RECORDED_PROJECT}.src.server.main`,
            file: 'src/server.ts',
        });
        // Kein routes-Schluessel fuer TypeScript: leer, und die Faehigkeit
        // sagt, dass leer hier nicht "es gibt keine" heisst.
        expect(overview.routes).toEqual([]);
        expect(overview.file_tree).toHaveLength(2);
    });

    it('liest die Aenderungsmenge unter beiden Schreibweisen der Symbolliste', async () => {
        const { client: c } = client([{
            tool: 'detect_changes',
            json: {
                base: 'main',
                changed_files: ['src/services/userService.ts'],
                impacted: {
                    cols: ['name', 'label', 'hop'],
                    groups: [{ qn_prefix: `${RECORDED_PROJECT}.src.services.userService`, file: 'src/services/userService.ts', rows: [['createUser', 'Function', 0]] }],
                },
            },
        }]);
        const changes = await c.detectChanges(RECORDED_PROJECT, 'main');
        expect(changes.changed_files).toEqual(['src/services/userService.ts']);
        expect(changes.impacted_symbols).toHaveLength(1);
        expect(changes.impacted_symbols[0].qualified_name)
            .toBe(`${RECORDED_PROJECT}.src.services.userService.createUser`);
        expect(changes.impacted_symbols[0].file).toBe('src/services/userService.ts');
    });

    it('schickt den Vergleichspunkt als since, so wie das Werkzeug ihn deklariert', async () => {
        const { client: c, rpc } = client([{ tool: 'detect_changes', json: { changed_files: [] } }]);
        await c.detectChanges(RECORDED_PROJECT, 'HEAD~3', { depth: 2 });
        expect(rpc.calls[0].args['since']).toBe('HEAD~3');
        expect(rpc.calls[0].args['depth']).toBe(2);
    });
});

describe('Suche', () => {

    it('liest die flache Suchform mit Rang', async () => {
        const { client: c } = client([{ tool: 'search_graph', recording: 'search' }]);
        const result = await c.searchGraph(RECORDED_PROJECT, { namePattern: 'createUser', limit: 5 });
        expect(result.total).toBe(1);
        expect(result.results).toHaveLength(1);
        expect(result.results[0].name).toBe('createUser');
        expect(result.results[0].qualified_name)
            .toBe(`${RECORDED_PROJECT}.src.services.userService.createUser`);
        expect(result.results[0].file_path).toBe('src/services/userService.ts');
        // `lines` ist eine Spanne "23-36"; die Deklarationszeile ist ihr Anfang.
        expect(result.results[0].start_line).toBe(23);
    });

    it('behauptet nicht, was die Suchantwort nicht sagt', async () => {
        const { client: c } = client([{ tool: 'search_graph', recording: 'search' }]);
        const result = await c.searchGraph(RECORDED_PROJECT, { namePattern: 'createUser' });
        expect(result.results[0].is_test).toBeUndefined();
        expect(result.results[0].is_exported).toBeUndefined();
    });
});

describe('was schiefgehen kann', () => {

    it('macht aus einem stummen Server einen nicht erreichbaren, nicht aus einem leeren Ergebnis', async () => {
        const { client: c } = client([{ tool: 'list_projects', networkError: 'fetch failed' }]);
        await expect(c.listProjects()).rejects.toBeInstanceOf(EngineUnavailableError);
    });

    it('macht aus der Ablehnung der Allowlist einen nicht erreichbaren Server, keinen Engine-Fehler', async () => {
        const { client: c } = client([{ tool: 'get_architecture', notAllowed: true }]);
        await expect(c.getArchitecture(RECORDED_PROJECT)).rejects.toBeInstanceOf(EngineUnavailableError);
    });

    it('macht aus einem inhaltlichen Nein des Werkzeugs einen Engine-Fehler', async () => {
        const { client: c } = client([{ tool: 'get_code_snippet', toolError: 'Symbol not found: x' }]);
        await expect(c.getCodeSnippet(RECORDED_PROJECT, 'x')).rejects.toBeInstanceOf(EngineError);
    });

    it('lehnt die drei schreibenden Werkzeuge ab, ohne sie ueberhaupt zu fragen', async () => {
        const { client: c, rpc } = client([]);
        await expect(c.indexRepository()).rejects.toBeInstanceOf(EngineUnavailableError);
        await expect(c.deleteProject()).rejects.toBeInstanceOf(EngineUnavailableError);
        await expect(c.ingestTraces()).rejects.toBeInstanceOf(EngineUnavailableError);
        expect(rpc.calls).toEqual([]);
    });

    it('nennt in der Ablehnung, wo der schreibende Weg stattdessen laeuft', async () => {
        const { client: c } = client([]);
        await expect(c.ingestTraces()).rejects.toThrow(/CLI/);
    });

    it('liest eine leere Tabelle als leer und nicht als Fehler', async () => {
        const { client: c } = client([{ tool: 'query_graph', text: rowsText(['n.name'], []) }]);
        const result = await c.queryGraph(RECORDED_PROJECT, 'MATCH ...');
        expect(result.rows).toEqual([]);
        expect(result.total).toBe(0);
    });
});
