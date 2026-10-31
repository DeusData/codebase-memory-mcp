/*
 * Der Weg von /api/layout in die Szene.
 *
 * Zwei Gruppen: was aus einer Antwort gelesen wird (und was nicht), und was
 * passiert, wenn niemand oder etwas Kaputtes antwortet. Die zweite ist die
 * wichtigere: eine Galaxie, die bei einem Fehler einfach leer bleibt,
 * behauptet, das Projekt habe keine Knoten.
 */

import { describe, expect, it, vi } from 'vitest';

import { LayoutError, loadLayout, readGraphData } from './layout-source';

const RESPONSE = {
    nodes: [
        {
            id: 51,
            x: -244.9,
            y: -453.5,
            z: -46.02,
            label: 'Function',
            name: 'createUser',
            file_path: 'src/services/userService.ts',
            qualified_name: 'atlas.src.services.userService.createUser',
            start_line: 23,
            end_line: 36,
            size: 7.6,
            color: '#ffe080',
            in_calls: 2,
            out_calls: 6,
            status: 'entry',
        },
        { id: 74, x: 1, y: 2, z: 3, label: 'EnvVar', name: 'DB_URL', size: 3, color: '#88aaff' },
    ],
    edges: [{ source: 51, target: 74, type: 'CONFIGURES' }],
    total_nodes: 76,
};

function jsonResponse(body: unknown, status = 200): Response {
    return new Response(JSON.stringify(body), { status });
}

describe('readGraphData', () => {

    it('liest die Felder, die die Szene und die Navigation brauchen', () => {
        const data = readGraphData(RESPONSE);
        expect(data.total_nodes).toBe(76);
        expect(data.nodes).toHaveLength(2);
        expect(data.edges).toEqual([{ source: 51, target: 74, type: 'CONFIGURES' }]);
        const first = data.nodes[0];
        expect(first.qualified_name).toBe('atlas.src.services.userService.createUser');
        expect(first.file_path).toBe('src/services/userService.ts');
        expect(first.start_line).toBe(23);
        expect(first.in_calls).toBe(2);
        expect(first.status).toBe('entry');
    });

    it('laesst fehlende Angaben fehlen, statt sie zu erfinden', () => {
        const envVar = readGraphData(RESPONSE).nodes[1];
        expect(envVar.file_path).toBeUndefined();
        expect(envVar.qualified_name).toBeUndefined();
        expect(envVar.start_line).toBeUndefined();
        expect(envVar.in_calls).toBeUndefined();
    });

    it('wirft Knoten ohne Ort weg, statt sie in den Ursprung zu legen', () => {
        const data = readGraphData({
            nodes: [{ id: 1, x: 1, y: 2, size: 1, color: '#fff', name: 'ohne z' }],
            edges: [],
        });
        expect(data.nodes).toHaveLength(0);
    });

    it('macht aus einer Antwort ohne Knoten eine leere Galaxie und keinen Absturz', () => {
        expect(readGraphData({}).nodes).toEqual([]);
        expect(readGraphData(null).edges).toEqual([]);
        expect(readGraphData('nope').total_nodes).toBe(0);
    });

    it('nimmt Zahlen auch als Zeichenkette an, so wie die Engine sie manchmal schickt', () => {
        const data = readGraphData({
            nodes: [{ id: '7', x: '1', y: '2', z: '3', size: '5', color: '#fff', name: 'sieben' }],
            edges: [{ source: '7', target: '8', type: 'CALLS' }],
        });
        expect(data.nodes[0].id).toBe(7);
        expect(data.edges[0].source).toBe(7);
    });
});

describe('loadLayout', () => {

    it('fragt genau die Route mit Projekt und Deckel', async () => {
        const fetchImpl = vi.fn(async () => jsonResponse(RESPONSE));
        await loadLayout('atlas-sample', { fetch: fetchImpl as unknown as typeof fetch });
        expect(fetchImpl).toHaveBeenCalledTimes(1);
        const [url] = fetchImpl.mock.calls[0] as unknown as [string];
        expect(url).toBe('/api/layout?project=atlas-sample&max_nodes=5000');
    });

    it('meldet einen HTTP-Fehler mit Status und Route, statt leer zu bleiben', async () => {
        const fetchImpl = vi.fn(async () => new Response('{"error":"unknown project"}', { status: 404 }));
        await expect(
            loadLayout('weg', { fetch: fetchImpl as unknown as typeof fetch }),
        ).rejects.toMatchObject({ name: 'LayoutError', status: 404 });
    });

    it('meldet einen unerreichbaren Server als solchen', async () => {
        const fetchImpl = vi.fn(async () => {
            throw new Error('connection refused');
        });
        const failure = await loadLayout('atlas', { fetch: fetchImpl as unknown as typeof fetch })
            .then(() => undefined)
            .catch((error: unknown) => error);
        expect(failure).toBeInstanceOf(LayoutError);
        expect((failure as LayoutError).status).toBe(0);
        expect((failure as LayoutError).message).toContain('/api/layout');
        expect((failure as LayoutError).message).toContain('connection refused');
    });

    it('meldet eine Antwort, die kein JSON ist, als solche', async () => {
        const fetchImpl = vi.fn(async () => new Response('<html>nope</html>', { status: 200 }));
        await expect(
            loadLayout('atlas', { fetch: fetchImpl as unknown as typeof fetch }),
        ).rejects.toThrow(/lieferte kein JSON/);
    });
});
