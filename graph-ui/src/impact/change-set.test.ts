import { describe, expect, it, vi } from 'vitest';
import { changedAreas, loadChangeSet, loadFileImpactSymbols, localDiffCommand, validChangeRef } from './change-set';

const page = (files: string[], total = files.length, extra = {}) => ({ base: 'HEAD', merge_base: 'a'.repeat(40),
    changed_files: files, changed_total: total, changed_has_more: false, ...extra });

describe('bounded local change-set reading', () => {
    it('uses an explicit HEAD baseline and files-only scope instead of the absent default main branch', async () => {
        const read = vi.fn().mockResolvedValue(page(['src/fixture.c']));
        expect(await loadChangeSet('fixture', 'HEAD', undefined, read)).toMatchObject({ files: ['src/fixture.c'], complete: true });
        expect(read.mock.calls[0]?.[0]).toMatchObject({ project: 'fixture', since: 'HEAD', scope: 'files', module_limit: 0, format: 'json' });
    });
    it('follows snapshot cursors and accounts for every changed path', async () => {
        const read = vi.fn().mockResolvedValueOnce(page(['a.c'], 2, { changed_has_more: true, changed_next_cursor: 'snapshot-page2' }))
            .mockResolvedValueOnce(page(['b.c'], 2));
        const data = await loadChangeSet('fixture', 'HEAD', undefined, read);
        expect(data).toMatchObject({ files: ['a.c', 'b.c'], totalFiles: 2, pages: 2, complete: true });
        expect(read.mock.calls[1]?.[0].changed_cursor).toBe('snapshot-page2');
        expect(read.mock.calls[1]?.[0].changed_offset).toBeUndefined();
    });
    it('reports stalled or unavailable continuation honestly and does not infer an empty change set', async () => {
        const read = vi.fn().mockResolvedValue(page([], 20, { changed_has_more: true, changed_continuation_requires_higher_budget: true }));
        const data = await loadChangeSet('fixture', 'HEAD', undefined, read);
        expect(read).toHaveBeenCalledTimes(2);
        expect(data).toMatchObject({ complete: false, files: [], totalFiles: 20 });
        expect(data.limitations.join(' ')).toContain('progressing snapshot cursor');
    });
    it('rejects a moved snapshot, malformed response and unsafe baseline before presenting findings', async () => {
        const read = vi.fn().mockResolvedValueOnce(page(['a.c'], 2, { changed_has_more: true, changed_next_cursor: 'page2' }))
            .mockResolvedValueOnce(page(['b.c'], 3));
        await expect(loadChangeSet('fixture', 'HEAD', undefined, read)).rejects.toThrow('moved');
        await expect(loadChangeSet('fixture', 'HEAD', undefined, async () => ({ changed_files: [] }))).rejects.toThrow('incomplete');
        const unused = vi.fn();
        await expect(loadChangeSet('fixture', '--exec=anything', undefined, unused)).rejects.toThrow('valid local Git revision');
        expect(unused).not.toHaveBeenCalled();
    });
    it('rejects absolute and traversal paths and accounts for a mismatching final total', async () => {
        await expect(loadChangeSet('fixture', 'HEAD', undefined, async () => page(['../private']))).rejects.toThrow('repository-relative');
        const data = await loadChangeSet('fixture', 'HEAD', undefined, async () => page(['a.c'], 2));
        expect(data.complete).toBe(false);
        expect(data.limitations.join(' ')).toContain('reported total');
    });
    it('keeps path-area navigation separate from any risk score and prefers the selected file', () => {
        const areas = changedAreas(['src/ui/a.c', 'src/ui/b.c', 'src/daemon/a.c', 'README.md'], 'src/daemon/a.c');
        expect(areas.map(area => area.path)).toEqual(['src/daemon', 'src/ui', '(root)']);
        expect(areas[1]?.files).toHaveLength(2);
    });
    it('reads actual indexed declarations and never re-labels them as changed lines', async () => {
        const client = { queryRows: vi.fn().mockResolvedValueOnce([
            { 'n.qualified_name': 'fixture.a', 'n.name': 'a', 'n.file_path': 'a.c', 'n.start_line': '10' },
            { 'n.qualified_name': 'unrelated', 'n.file_path': 'b.c' },
        ]).mockResolvedValueOnce([]) };
        expect(await loadFileImpactSymbols(client, 'fixture', 'a.c')).toEqual([{ qualifiedName: 'fixture.a', name: 'a', filePath: 'a.c', line: 10 }]);
        expect(client.queryRows.mock.calls[0]?.[1]).toContain('LIMIT 200');
    });
    it('provides safely quoted local commands without executing them', () => {
        expect(validChangeRef('HEAD~3')).toBe(true);
        expect(validChangeRef('feature/fix')).toBe(true);
        expect(validChangeRef('HEAD;curl secret')).toBe(false);
        expect(localDiffCommand('HEAD', "it's $(unsafe).c")).toBe("git diff 'HEAD' -- 'it'\\''s $(unsafe).c'");
    });
});
