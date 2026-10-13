import { describe, expect, it } from 'vitest';
import { connectionLoad } from './connection-load';

describe('visible connection load', () => {
    it('counts typed lines once, including reciprocal links and self loops', () => {
        const load = connectionLoad(['a', 'b', 'isolated'].map(id => ({ id })), [
            { source: 'a', target: 'b', type: 'RELATIONSHIPS', types: ['CALLS', 'IMPORTS', 'calls'] },
            { source: 'a', target: 'b', type: 'CALLS' },
            { source: 'b', target: 'a', type: 'CALLS' },
            { source: 'a', target: 'a', type: 'CALLS' },
            { source: 'a', target: 'missing', type: 'CALLS' },
        ]);
        expect(load.get('a')).toEqual({ links: 4, neighbors: 1, strength: 1 });
        expect(load.get('b')).toEqual({ links: 3, neighbors: 1, strength: Math.log1p(3) / Math.log1p(4) });
        expect(load.get('isolated')).toEqual({ links: 0, neighbors: 0, strength: 0 });
    });
    it('handles empty scopes and keeps results independent of input order', () => {
        expect(connectionLoad([], []).size).toBe(0);
        expect(connectionLoad([{ id: 'a' }], []).get('a')?.strength).toBe(0);
        const nodes = ['a', 'b', 'c'].map(id => ({ id }));
        const edges = [{ source: 'a', target: 'b', type: 'CALLS' }, { source: 'a', target: 'c', type: 'IMPORTS' }];
        const one = connectionLoad(nodes, edges), two = connectionLoad([...nodes].reverse(), [...edges].reverse());
        for (const node of nodes) expect(two.get(node.id)).toEqual(one.get(node.id));
    });
});
