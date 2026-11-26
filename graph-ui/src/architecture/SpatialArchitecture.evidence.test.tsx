// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import SpatialArchitecture from './SpatialArchitecture';
import type { ArchitectureSceneProps } from './ArchitectureScene';
import type { GraphData, GraphNode } from '../galaxy/types';
import type { ArchitectureOverviewDto } from '../core/intelligence-provider';
import { architectureFacts } from '../browser-ai/architecture-evidence';

/*
 * K7: an area opened in the Overview with nothing selected inside it (the hand test opened
 * "django" at 17:53) is explained from readable facts about that area, not "Opened: django".
 */

vi.mock('./ArchitectureScene', () => ({ ArchitectureScene: (props: ArchitectureSceneProps) => <div data-testid="scene">{props.model.nodes.map(node => <button key={node.id} onClick={() => props.onSelect(node.id)}>{node.label}</button>)}</div> }));
vi.mock('./route-graph-source', () => ({ loadRouteGraph: vi.fn(async () => ({ relationships: [], truncated: false, warnings: [] })) }));

const symbol = (id: number, name: string, path: string, label = 'Function', start = 10, end = 20): GraphNode =>
    ({ id, label, name, qualified_name: `sample.${name}`, file_path: path, start_line: start, end_line: end, x: 0, y: 0, z: 0, size: 1, color: '' });
const graph: GraphData = {
    nodes: [
        symbol(1, 'options', 'django/contrib/admin/options.py', 'Module', 1, 300), symbol(2, 'ModelAdmin', 'django/contrib/admin/options.py', 'Class'),
        symbol(3, 'shortcuts', 'django/shortcuts.py', 'Module', 1, 120), symbol(4, 'render', 'django/shortcuts.py'),
        symbol(5, 'query', 'django/db/models/query.py', 'Module', 1, 2000), symbol(6, 'filter', 'django/db/models/query.py', 'Method', 1487, 1500),
        symbol(7, 'setup', 'setup.py', 'Module', 1, 40),
    ],
    edges: [{ source: 4, target: 6, type: 'CALLS' }, { source: 2, target: 6, type: 'CALLS' }, { source: 1, target: 5, type: 'IMPORTS' }],
    total_nodes: 7,
};
const overview: ArchitectureOverviewDto = { projectName: 'sample', totalSymbols: 7, totalRelations: 3, symbolKinds: [], relationKinds: [], languages: [], groups: [], boundaries: [], layers: [], clusters: [], entryPoints: [], routes: [],
    hotspots: [{ name: 'filter', qualifiedName: 'sample.filter', filePath: 'django/db/models/query.py', line: 1487, fanIn: 1224 }, { name: 'setup', qualifiedName: 'sample.setup', filePath: 'setup.py', line: 1, fanIn: 3 }],
    files: [] };
let host: HTMLDivElement;
let root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    host = document.createElement('div'); document.body.appendChild(host); root = createRoot(host);
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); });
async function click(text: string) {
    const button = [...host.querySelectorAll('button')].find(item => item.textContent === text);
    expect(button, text).toBeDefined(); await act(async () => button!.click());
}

it('explains an opened area with nothing selected from its files, lines, languages, parts, hotspots and connections', async () => {
    const onSelectionEvidence = vi.fn();
    await act(async () => root.render(<SpatialArchitecture project="sample" generation="g1" graph={graph} overview={overview} view="overview" filter="" active
        onSelectionEvidence={onSelectionEvidence} onNavigate={vi.fn()} onView={vi.fn()} />));
    await click('django'); await click('Open area →');
    const evidence = JSON.parse(onSelectionEvidence.mock.lastCall![0].text).evidence;
    expect(evidence.selected.label).toBeUndefined();
    const facts = architectureFacts(evidence)?.facts ?? [];
    const text = facts.join('\n');
    expect(facts[0]).toBe('Opened source area: `django`.');
    expect(text).toContain('2,420 indexed lines in 3 measured files of 3; files by language: Python 3.');
    expect(evidence.scope.visibleNodes).toBe(3);
    expect(text).toContain('Parts shown (3, largest first): `db` (area, 1 file, 2,000 indexed lines), `contrib` (area, 1 file, 300 indexed lines), `shortcuts.py` (file, 120 indexed lines).');
    expect(text).toContain('1 hotspot finding: `filter` (`django/db/models/query.py:1487`) fan-in 1,224.');
    expect(text).not.toContain('`setup`');
    expect(text).toMatch(/Connections of its parts: .*`shortcuts\.py` → `db`: CALLS ×1/);
    expect(text).toMatch(/`contrib` → `db`: (?:CALLS ×1, IMPORTS ×1|IMPORTS ×1, CALLS ×1)/);
    expect(text).not.toMatch(/^Opened: /m);
});

it('names the areas outside the opened area as outside in its connections', async () => {
    const onSelectionEvidence = vi.fn();
    const outside: GraphData = { ...graph, edges: [...graph.edges, { source: 7, target: 4, type: 'IMPORTS' }] };
    await act(async () => root.render(<SpatialArchitecture project="sample" generation="g1" graph={outside} overview={overview} view="overview" filter="" active
        onSelectionEvidence={onSelectionEvidence} onNavigate={vi.fn()} onView={vi.fn()} />));
    await click('django'); await click('Open area →');
    const evidence = JSON.parse(onSelectionEvidence.mock.lastCall![0].text).evidence;
    const text = (architectureFacts(evidence)?.facts ?? []).join('\n');
    expect(text).toMatch(/`\(root\)` \(outside\) → `shortcuts\.py`: IMPORTS ×1/);
    // The card counts the parts the view counts: the ones inside the area and the ones outside it, each named as such.
    expect(evidence.scope.visibleNodes).toBe(4);
    expect(text).toContain('Parts shown (4): 3 inside the area, largest first: `db` (area, 1 file, 2,000 indexed lines), `contrib` (area, 1 file, 300 indexed lines), '
        + '`shortcuts.py` (file, 120 indexed lines); 1 outside it: `(root)`.');
});

it('names the hotspot findings of an opened hotspot area', async () => {
    const onSelectionEvidence = vi.fn();
    await act(async () => root.render(<SpatialArchitecture project="sample" generation="g1" graph={graph} overview={overview} view="hotspots" filter="" active
        place={{ planar: false, hotspotArea: 'django' }} onPlace={vi.fn()} onSelectionEvidence={onSelectionEvidence} onNavigate={vi.fn()} onView={vi.fn()} />));
    const facts = architectureFacts(JSON.parse(onSelectionEvidence.mock.lastCall![0].text).evidence)?.facts ?? [];
    expect(facts[0]).toBe('Opened hotspot area: `django`.');
    expect(facts.join('\n')).toContain('1 hotspot finding: `filter` (`django/db/models/query.py:1487`) fan-in 1,224.');
});
