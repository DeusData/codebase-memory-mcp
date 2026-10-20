/*
 * Der Ladeweg des Readers: Name ableiten, gegen den Graphen halten, Quelltext
 * holen, Kappung benennen.
 *
 * Die Antworten unten haben die Form, die der gebaute Server am 2026-08-28
 * wirklich geliefert hat: die kompakte Zeilenform fuer query_graph (Zahlen
 * gequotet), und fuer get_code_snippet ein JSON-Objekt, das bei einer zu langen
 * Datei `source_clipped` und `clipped_at_lines` mitschickt. Der Lauf geht durch
 * rpc-transport und rpc-client hindurch; ersetzt ist nur die Leitung.
 */
import { describe, expect, it } from 'vitest';
import { observeErrors } from '../provider/error-observer';
import type { ErrorReport } from '../provider/error-observer';
import { RpcIntelligenceClient } from '../provider/rpc-client';
import { FakeRpc, queryContains, rowsText } from '../test-support/rpc-recordings';
import type { Route } from '../test-support/rpc-recordings';
import { FileNotReadableError, loadFileDocument, truncationNoteFor } from './file-source';

const PROJECT = 'probe-small';
const MODULE_COLUMNS = ['n.qualified_name', 'n.start_line', 'n.end_line'];
const FILE_COLUMNS = ['n.qualified_name'];

function clientFor(routes: Route[]): { client: RpcIntelligenceClient; rpc: FakeRpc } {
    const rpc = new FakeRpc(routes);
    return { client: new RpcIntelligenceClient({ fetch: rpc.fetch }), rpc };
}

function moduleRoute(qualifiedName: string, endLine: number): Route {
    return {
        tool: 'query_graph',
        when: queryContains('MATCH (n:Module)'),
        text: rowsText(MODULE_COLUMNS, [[qualifiedName, '"1"', `"${endLine}"`]]),
    };
}

const noModuleRoute: Route = {
    tool: 'query_graph',
    when: queryContains('MATCH (n:Module)'),
    text: rowsText(MODULE_COLUMNS, []),
};

function snippetRoute(json: Record<string, unknown>): Route {
    return { tool: 'get_code_snippet', json };
}

describe('loadFileDocument', () => {

    it('laedt ueber den Modul-Knoten und meldet die Ableitung als bestaetigt', async () => {
        const { client, rpc } = clientFor([
            moduleRoute('probe-small.src.services.userService', 43),
            snippetRoute({
                name: 'src/services/userService.ts',
                qualified_name: 'probe-small.src.services.userService',
                label: 'Module',
                start_line: 1,
                end_line: 43,
                source: 'export function createUser() {}\n',
            }),
        ]);

        const doc = await loadFileDocument(client, PROJECT, 'src/services/userService.ts');

        expect(doc.qualifiedName).toBe('probe-small.src.services.userService');
        expect(doc.derivedQualifiedName).toBe('probe-small.src.services.userService');
        expect(doc.qnSource).toBe('derived');
        expect(doc.source).toContain('createUser');
        expect(doc.truncated).toBe(false);
        expect(doc.truncationNote).toBe('');
        expect(doc.fileLastLine).toBe(43);
        // Der Quelltext kommt ueber get_code_snippet und ueber nichts sonst.
        expect(rpc.toolsCalled()).toEqual(['query_graph', 'get_code_snippet']);
        expect(rpc.callsTo('get_code_snippet')[0]?.args['qualified_name'])
            .toBe('probe-small.src.services.userService');
    });

    it('nimmt den Namen des Graphen, wenn er von der Ableitung abweicht', async () => {
        const { client, rpc } = clientFor([
            moduleRoute('probe-small.src.services.user_service', 43),
            snippetRoute({ start_line: 1, end_line: 43, source: 'x\n' }),
        ]);

        const doc = await loadFileDocument(client, PROJECT, 'src/services/userService.ts');

        expect(doc.qualifiedName).toBe('probe-small.src.services.user_service');
        expect(doc.derivedQualifiedName).toBe('probe-small.src.services.userService');
        expect(doc.qnSource).toBe('graph-module');
        expect(rpc.callsTo('get_code_snippet')[0]?.args['qualified_name'])
            .toBe('probe-small.src.services.user_service');
    });

    it('faellt auf den File-Knoten zurueck, wenn es keinen Modul-Knoten gibt', async () => {
        const { client } = clientFor([
            noModuleRoute,
            {
                tool: 'query_graph',
                when: queryContains('MATCH (n:File)'),
                text: rowsText(FILE_COLUMNS, [['probe-small.src.services.userService.ts.__file__']]),
            },
            snippetRoute({ start_line: 1, end_line: 43, source: 'x\n' }),
        ]);

        const doc = await loadFileDocument(client, PROJECT, 'src/services/userService.ts');

        expect(doc.qualifiedName).toBe('probe-small.src.services.userService');
        // Der Rueckfallweg bringt keine Spanne mit, also ist die Dateilaenge
        // unbekannt und wird nicht erfunden.
        expect(doc.fileLastLine).toBeUndefined();
        expect(doc.qnSource).toBe('derived');
    });

    it('benennt die fehlenden Zeilen, wenn der Server gekappt hat', async () => {
        const { client } = clientFor([
            moduleRoute('probe-large.src.big', 718),
            snippetRoute({
                qualified_name: 'probe-large.src.big',
                label: 'Module',
                start_line: 1,
                end_line: 500,
                source_clipped: true,
                clipped_at_lines: 500,
                source: 'line\n'.repeat(500),
            }),
        ]);

        const doc = await loadFileDocument(client, 'probe-large', 'src/big.ts');

        expect(doc.truncated).toBe(true);
        expect(doc.lastLine).toBe(500);
        expect(doc.fileLastLine).toBe(718);
        expect(doc.truncationNote).toContain('501-718');
        expect(doc.truncationNote).toContain('500 lines');
    });

    it('sagt, dass eine Datei ohne Modul-Knoten hier nicht lesbar ist', async () => {
        const { client } = clientFor([
            noModuleRoute,
            { tool: 'query_graph', when: queryContains('MATCH (n:File)'), text: rowsText(FILE_COLUMNS, []) },
        ]);

        await expect(loadFileDocument(client, PROJECT, 'assets/logo.svg'))
            .rejects.toBeInstanceOf(FileNotReadableError);
    });

    it('behauptet nicht, eine leere Antwort sei eine leere Datei', async () => {
        const { client } = clientFor([
            moduleRoute('probe-small.src.types', 54),
            snippetRoute({ start_line: 1, end_line: 54, source: '' }),
        ]);

        await expect(loadFileDocument(client, PROJECT, 'src/types.ts'))
            .rejects.toBeInstanceOf(FileNotReadableError);
    });
});

describe('loadFileDocument, seit dem schlanken Vertrag (#1597)', () => {

    /*
     * Auf main liefert get_code_snippet Quelltext in Seiten zu 500 Zeilen und
     * nennt die naechste in next_start_line; die wirkliche Dateilaenge steht
     * in original_end_line. Die Formen unten sind die, die der Server am
     * 2026-09-06 fuer src/ui/http_server.c (2244 Zeilen) geantwortet hat.
     */
    it('fordert vollen Quelltext an und fuegt die Seiten zusammen', async () => {
        const { client, rpc } = clientFor([
            moduleRoute('probe-small.src.ui.http_server', 2244),
            {
                tool: 'get_code_snippet',
                when: (args) => args['start_line'] === undefined,
                json: {
                    qualified_name: 'probe-small.src.ui.http_server', label: 'Module',
                    start_line: 1, end_line: 500, source_truncated: true, source_clipped: true,
                    next_start_line: 501, original_end_line: 1000, source_mode: 'full',
                    source: 'page one\n',
                },
            },
            {
                tool: 'get_code_snippet',
                when: (args) => args['start_line'] === 501,
                json: {
                    qualified_name: 'probe-small.src.ui.http_server', label: 'Module',
                    start_line: 501, end_line: 1000, source_mode: 'full', source: 'page two\n',
                },
            },
        ]);

        const doc = await loadFileDocument(client, PROJECT, 'src/ui/http_server.c');

        expect(doc.source).toBe('page one\npage two\n');
        expect(doc.firstLine).toBe(1);
        expect(doc.lastLine).toBe(1000);
        expect(doc.fileLastLine).toBe(1000);
        expect(doc.truncated).toBe(false);
        expect(doc.truncationNote).toBe('');
        const calls = rpc.callsTo('get_code_snippet');
        expect(calls).toHaveLength(2);
        expect(calls[0]?.args['source_mode']).toBe('full');
        expect(calls[1]?.args['start_line']).toBe(501);
        expect(calls[1]?.args['max_lines']).toBe(500);
    });

    it('benennt die fehlende Seite, wenn eine nicht ankommt', async () => {
        const { client } = clientFor([
            moduleRoute('probe-small.src.ui.http_server', 2244),
            {
                tool: 'get_code_snippet',
                when: (args) => args['start_line'] === undefined,
                json: {
                    start_line: 1, end_line: 500, source_clipped: true, next_start_line: 501,
                    original_end_line: 1000, source_mode: 'full', source: 'page one\n',
                },
            },
            { tool: 'get_code_snippet', when: (args) => args['start_line'] === 501, toolError: 'boom' },
        ]);

        const doc = await loadFileDocument(client, PROJECT, 'src/ui/http_server.c');

        expect(doc.source).toBe('page one\n');
        expect(doc.lastLine).toBe(500);
        expect(doc.truncated).toBe(true);
        expect(doc.truncationNote).toContain('501-1000');
        expect(doc.truncationNote).toContain('did not arrive');
    });

    it('nimmt den Platzhalter des Servers nicht als Quelltext', async () => {
        const { client } = clientFor([
            moduleRoute('probe-small.src.ui.http_server', 2244),
            snippetRoute({ start_line: 1, end_line: 500, source_mode: 'full', source: '(source not available)' }),
        ]);

        await expect(loadFileDocument(client, PROJECT, 'src/ui/http_server.c'))
            .rejects.toBeInstanceOf(FileNotReadableError);
        await expect(loadFileDocument(client, PROJECT, 'src/ui/http_server.c'))
            .rejects.toThrow(/could not read/);
    });
});

describe('loadFileDocument announces to the frontend log', () => {
    it('a file without a module node, and the server placeholder, before it throws', async () => {
        const reports: ErrorReport[] = [];
        const stop = observeErrors((report) => reports.push(report));
        try {
            const missing = clientFor([
                noModuleRoute,
                { tool: 'query_graph', when: queryContains('MATCH (n:File)'), text: rowsText(FILE_COLUMNS, []) },
            ]);
            await expect(loadFileDocument(missing.client, PROJECT, 'assets/logo.svg'))
                .rejects.toBeInstanceOf(FileNotReadableError);

            const gone = clientFor([
                moduleRoute('probe-small.src.ui.http_server', 2244),
                snippetRoute({ start_line: 1, end_line: 500, source_mode: 'full', source: '(source not available)' }),
            ]);
            await expect(loadFileDocument(gone.client, PROJECT, 'src/ui/http_server.c'))
                .rejects.toBeInstanceOf(FileNotReadableError);
        } finally {
            stop();
        }
        expect(reports.map((report) => [report.source, report.level])).toEqual([
            ['reader', 'warn'],
            ['reader', 'warn'],
        ]);
        expect(reports[0]?.message).toContain('The index has no module node for assets/logo.svg');
        expect(reports[1]?.message).toContain('could not read src/ui/http_server.c');
    });

    it('nothing when the file loads', async () => {
        const reports: ErrorReport[] = [];
        const stop = observeErrors((report) => reports.push(report));
        try {
            const { client } = clientFor([
                moduleRoute('probe-small.src.types', 3),
                snippetRoute({ start_line: 1, end_line: 3, source_mode: 'full', source: 'a\nb\nc' }),
            ]);
            await loadFileDocument(client, PROJECT, 'src/types.ts');
        } finally {
            stop();
        }
        expect(reports).toEqual([]);
    });
});

describe('truncationNoteFor', () => {

    it('nennt Zeilen, Deckel und den Grund, warum nicht nachgeladen wird', () => {
        const note = truncationNoteFor(500, 718, 500);
        expect(note).toContain('lines 501-718 not loaded');
        expect(note).toContain('server snippet cap of 500 lines');
        expect(note).toContain('ignored');
    });

    it('erfindet keine Dateilaenge, wenn der Graph keine kennt', () => {
        const note = truncationNoteFor(500, undefined, undefined);
        expect(note).toContain('lines after 500 not loaded');
        expect(note).toContain('did not record the file length');
    });
});
