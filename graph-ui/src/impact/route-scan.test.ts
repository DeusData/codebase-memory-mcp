import { describe, expect, it, vi } from 'vitest';
import { MAX_ROUTE_SCAN_FILES, mergeRoutes, scanRoutes } from './route-scan';
import type { RouteRef } from '../core/intelligence-provider';

const USERS = `
import { createUser, listUsers } from '../services/userService';

export function registerUserRoutes(router: Router): void {
    router.get('/users', (req, res) => { res.json(listUsers()); });
    router.post('/users', (req, res) => { res.json(createUser(req.body)); });
}
`;

describe('scanRoutes', () => {

    it('reads registrations off the text and marks them as a source reading', async () => {
        const scan = await scanRoutes(['src/routes/users.ts'], async () => ({ source: USERS, truncated: false }));
        expect(scan.routes.map((route) => `${route.method} ${route.path}`)).toEqual([
            'GET /users',
            'POST /users',
        ]);
        expect(scan.routes.every((route) => route.origin === 'source')).toBe(true);
        expect(scan.routes.every((route) => route.filePath === 'src/routes/users.ts')).toBe(true);
        expect(scan.scanned).toBe(1);
        expect(scan.cappedAt).toBeUndefined();
        expect(scan.truncatedFiles).toBe(0);
    });

    it('never opens a file the reader has no pattern family for', async () => {
        const read = vi.fn(async () => ({ source: USERS, truncated: false }));
        const scan = await scanRoutes(['README.md', 'package.json', 'a.png'], read);
        expect(read).not.toHaveBeenCalled();
        expect(scan.scanned).toBe(0);
        expect(scan.routes).toEqual([]);
    });

    it('says the cap was reached instead of quietly reading fewer files', async () => {
        const files = Array.from({ length: 5 }, (_unused, index) => `src/r${index}.ts`);
        const scan = await scanRoutes(files, async () => ({ source: USERS, truncated: false }), 2);
        expect(scan.scanned).toBe(2);
        expect(scan.cappedAt).toBe(2);
    });

    it('counts a file whose text arrived incomplete', async () => {
        const scan = await scanRoutes(['a.ts', 'b.ts'], async (path) => ({
            source: USERS,
            truncated: path === 'b.ts',
        }));
        expect(scan.truncatedFiles).toBe(1);
    });

    it('skips a file it could not read without losing the rest', async () => {
        const scan = await scanRoutes(['a.ts', 'b.ts'], async (path) =>
            path === 'a.ts' ? undefined : { source: USERS, truncated: false });
        expect(scan.scanned).toBe(1);
        expect(scan.routes).toHaveLength(2);
    });

    it('survives a read that rejects', async () => {
        const scan = await scanRoutes(['a.ts'], async () => {
            throw new Error('no snippet');
        });
        expect(scan.scanned).toBe(0);
    });

    it('has a default cap so nobody has to remember one', () => {
        expect(MAX_ROUTE_SCAN_FILES).toBeGreaterThan(0);
    });
});

describe('mergeRoutes', () => {

    const scanned: RouteRef[] = [{ method: 'GET', path: '/users', filePath: 'src/routes/users.ts', line: 5, origin: 'source' }];
    const indexed: RouteRef[] = [
        { method: 'GET', path: '/users', origin: 'index' },
        { method: 'GET', path: '/orders/:id', origin: 'index' },
    ];

    it('keeps the reading that knows where the registration is written', () => {
        const merged = mergeRoutes(scanned, indexed);
        expect(merged).toHaveLength(2);
        expect(merged[0].origin).toBe('source');
        expect(merged[1].path).toBe('/orders/:id');
    });

    it('drops a route the index named with no path', () => {
        expect(mergeRoutes([], [{ path: '', origin: 'index' }])).toEqual([]);
    });
});
