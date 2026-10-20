// @vitest-environment jsdom
/*
 * Review zu K29: seit G1 bildet die Huelle des Canvas ihren eigenen
 * Stapelkontext, damit die Namen der Szene unter den Bedienflaechen bleiben.
 * Dieselbe Regel legte die Hover-Karte eines Knotens unter jede Flaeche: neben
 * "Selection details" war sie abgeschnitten. Eine Hover-Karte ist fluechtig und
 * muss lesbar sein. Hier steht, dass sie in einer eigenen Ebene ausserhalb der
 * Huelle steht, ueber den Flaechen der Galaxie und unter ihren Menues und
 * Tooltips.
 */
import { act, useState } from 'react';
import type { ReactNode } from 'react';
import { readFileSync } from 'node:fs';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const html = vi.hoisted(() => ({ props: [] as Record<string, unknown>[] }));
vi.mock('@react-three/drei', async importOriginal => ({
    ...await importOriginal<typeof import('@react-three/drei')>(),
    Html: (props: Record<string, unknown> & { children?: ReactNode }) => { html.props.push(props); return <div data-testid="html">{props.children}</div>; },
}));

const { GraphScene } = await import('./GraphScene');
const { HoverLayerContext, HOVER_LAYER_Z_INDEX } = await import('./hover-layer');
const { NodeTooltipCard } = await import('./NodeTooltipCard');

let host: HTMLDivElement, root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    (globalThis as unknown as Record<string, unknown>).ResizeObserver ??= class { observe() {} unobserve() {} disconnect() {} };
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
    html.props = [];
});
afterEach(() => { act(() => root.unmount()); host.remove(); });

const node = { id: 1, x: 0, y: 0, z: 0, name: 'n1', label: 'Function', size: 3, color: '#abcdef', file_path: 'a.py' };

/** The z-index of the last rule whose selector list names `selector` exactly, across the given sheets. */
function zIndex(selector: string, ...files: string[]): number {
    let found: number | undefined;
    for (const file of files) {
        const css = readFileSync(new URL(file, import.meta.url), 'utf8').replace(/\/\*[\s\S]*?\*\//g, '');
        for (const match of css.matchAll(/([^{}]+)\{([^}]*)\}/g)) {
            if (!match[1]!.split(',').map(item => item.trim()).includes(selector)) continue;
            const value = /z-index:\s*(\d+)/.exec(match[2]!)?.[1];
            if (value !== undefined) found = Number(value);
        }
    }
    expect(found, selector).toBeDefined();
    return found!;
}
const SHEETS = ['./graph-exploration.css', '../styles/terminal.css', '../why/selection-context.css'];

describe('K29: the hover card stands above the Galaxy panels', () => {
    it('GraphScene keeps a hover layer beside the isolated canvas wrapper, not inside it', () => {
        act(() => root.render(<GraphScene active={false} data={{ nodes: [node], edges: [], total_nodes: 1 }} highlightedIds={null} cameraTarget={null}
            showLabels onNodeClick={() => {}} renderTooltip={() => null} />));
        const wrapper = host.querySelector('canvas')!.parentElement!.parentElement!;
        const layer = host.querySelector<HTMLElement>('[data-testid="atlas-galaxy-hover-layer"]');
        expect(layer).not.toBeNull();
        expect(layer!.parentElement).toBe(host);
        expect(wrapper.contains(layer)).toBe(false);
        expect(layer!.style.position).toBe('absolute');
        expect(layer!.style.pointerEvents).toBe('none');
        expect(Number(layer!.style.zIndex)).toBe(HOVER_LAYER_Z_INDEX);
    });

    it('lies over every Galaxy panel and under the toolbar menus and tooltips', () => {
        const panels = {
            'Selection details': zIndex('.galaxy-selection-evidence', ...SHEETS),
            'path panel': zIndex('.atlas-galaxy-path-panel', ...SHEETS),
            'hierarchy key': zIndex('.atlas-hierarchy-key', ...SHEETS),
            'fit view': zIndex('.atlas-galaxy-fit', ...SHEETS),
            'agents instrument': zIndex('.atlas-agents', ...SHEETS),
            'agents strip': zIndex('.atlas-galaxy-bottom', ...SHEETS),
        };
        const above = {
            'Path to menu': zIndex('.atlas-graph-path-menu', ...SHEETS),
            'Edge types menu': zIndex('.atlas-trace-edge-menu', ...SHEETS),
            'more menu': zIndex('.atlas-graph-limits-menu', ...SHEETS),
            'Recent menu': zIndex('.atlas-graph-recent-menu', ...SHEETS),
            'search results': zIndex('.atlas-galaxy-search-results', ...SHEETS),
            tooltip: zIndex('.atlas-hint', ...SHEETS),
        };
        for (const [name, value] of Object.entries(panels)) expect(HOVER_LAYER_Z_INDEX, name).toBeGreaterThan(value);
        for (const [name, value] of Object.entries(above)) expect(HOVER_LAYER_Z_INDEX, name).toBeLessThan(value);
    });

    it('the node card draws into that layer', () => {
        function Probe() {
            const [layer, setLayer] = useState<HTMLDivElement | null>(null);
            return <><div ref={setLayer} /><HoverLayerContext.Provider value={layer}><NodeTooltipCard node={node} /></HoverLayerContext.Provider></>;
        }
        act(() => root.render(<Probe />));
        const props = html.props.at(-1)!;
        expect((props.portal as { current: HTMLElement }).current).toBe(host.firstElementChild);
        expect(props.center).toBe(true);
    });
});
