import { describe, expect, it } from 'vitest';
import { choicesFromSearchHits, searchPriority } from './GalaxyNavigator';

/*
 * Handtest 04.10. (abends): In cbm fand "working" den Branch-Knoten nicht mehr.
 * Seit er "cbm · working tree" heisst, beginnt sein gezeigter Name nicht mit dem
 * Suchwort, und die Ordner der anderen Treffer schoben ihn aus den zwoelf Plaetzen.
 * Sein Name im Index ("working-tree") zaehlt wie der gezeigte.
 */
describe('search priority', () => {
    const [branch] = choicesFromSearchHits([{ qualified_name: 'cbm.__branch__.working-tree', name: 'working-tree', label: 'Branch', file_path: '{}' } as never]);

    it('ranks a hit whose name in the index starts with the search word with the name matches', () => {
        expect(branch.name).toBe('cbm · working tree');
        expect(searchPriority(branch, 'working')).toBe(1);
        expect(searchPriority(branch, 'working-tree')).toBe(0);
    });

    it('keeps folders behind name matches and other hits behind folders', () => {
        const folder = { key: 'docs/', name: 'docs/', detail: 'Indexed folder', kind: 'Folder', scope: { kind: 'folder' as const, path: 'docs', name: 'docs/' } };
        expect(searchPriority(folder, 'working')).toBe(2);
        expect(searchPriority({ ...branch, name: 'other', scope: { kind: 'symbol' as const, qualifiedName: 'a.b', name: 'other' } }, 'working')).toBe(3);
    });
});
