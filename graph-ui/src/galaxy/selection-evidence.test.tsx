// @vitest-environment jsdom
import { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import { graphNodeEvidence, selectionEvidenceContext, useSelectionEvidence, type SelectionEvidence } from './selection-evidence';

const evidence: SelectionEvidence = { project: 'sample', view: 'galaxy', label: 'read', source: 'indexed graph',
    selected: { id: 7, qualifiedName: 'sample.read' }, limitations: 'Static evidence only.' };

it('bounds a large snapshot and reports exact omitted items and characters', () => {
    const context = selectionEvidenceContext({ ...evidence, selected: { name: 'a'.repeat(1300), members: Array.from({ length: 70 }, (_, id) => ({ id })) } });
    const snapshot = JSON.parse(context.text);
    expect(snapshot.evidence.selected.members).toHaveLength(24);
    expect(snapshot.omissions).toContainEqual({ path: '$.selected.members', kind: 'items', count: 46 });
    expect(snapshot.omissions).toContainEqual({ path: '$.selected.name', kind: 'characters', count: 100 });
    const large = selectionEvidenceContext({ ...evidence, selected: Array.from({ length: 50 }, () => ({ source: 'z'.repeat(4000) })) });
    expect(large.text.length).toBeLessThan(22_000);
    expect(JSON.parse(large.text).omissions.length).toBeGreaterThan(0);
    expect(snapshot.evidence.generation).toBe('unavailable');
});

it('retains source identity while excluding positions, visual size and color from stable context', () => {
    const node = { id: 7, name: 'read', label: 'Function', qualified_name: 'sample.read', file_path: 'read.ts', start_line: 2, end_line: 9, x: 0, y: 0, z: 0, size: 3, color: 'red' };
    const first = selectionEvidenceContext({ ...evidence, selected: graphNodeEvidence(node) });
    const moved = selectionEvidenceContext({ ...evidence, selected: graphNodeEvidence({ ...node, x: 500, y: -20, size: 90, color: 'blue' }) });
    expect(moved).toEqual(first);
    expect(JSON.parse(first.text).evidence.selected).toMatchObject({ filePath: 'read.ts', startLine: 2, endLine: 9 });
    expect(selectionEvidenceContext({ ...evidence, project: 'another' }).id).not.toBe(selectionEvidenceContext(evidence).id);
});

it('does not republish equivalent evidence or let hidden workspaces replace the active selection, and clears on background', async () => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    const host = document.createElement('div'), root = createRoot(host), listener = vi.fn();
    function Selection({ current, active = true }: { current?: SelectionEvidence; active?: boolean }) {
        useSelectionEvidence(listener, current, active); return null;
    }
    try {
        await act(async () => root.render(<Selection current={evidence} />));
        expect(listener).toHaveBeenCalledTimes(1);
        await act(async () => root.render(<Selection current={{ ...evidence }} />));
        expect(listener).toHaveBeenCalledTimes(1);
        await act(async () => root.render(<Selection current={{ ...evidence, project: 'hidden' }} active={false} />));
        expect(listener).toHaveBeenCalledTimes(1);
        await act(async () => root.render(<Selection />));
        expect(listener).toHaveBeenLastCalledWith(undefined);
    } finally { await act(async () => root.unmount()); }
});
