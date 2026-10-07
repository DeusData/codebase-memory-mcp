/*
 * Handtest 2026-10-04, Review zu K29: die Namen der Szene liegen seit G1 unter
 * den Bedienflaechen der Galaxie, aber die Flaechen waren leicht durchsichtig
 * (.9 bis .98, die Schaltflaeche "fit view" 82 %). Der Name des Pfadziels und
 * sein Ring schienen darum durch "Selection details" hindurch. Hier steht, dass
 * jede Flaeche, die ueber der Szene liegt, deckend gemalt wird.
 */
import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const read = (path: string) => readFileSync(new URL(path, import.meta.url), 'utf8').replace(/\/\*[\s\S]*?\*\//g, '');
const SHEETS = [read('./graph-exploration.css'), read('../styles/terminal.css'), read('../styles/workspace.css')];

/** The background of the last rule, across the sheets, whose selector list names `selector` exactly. */
function background(selector: string): string | undefined {
    let found: string | undefined;
    for (const css of SHEETS) {
        for (const match of css.matchAll(/([^{}]+)\{([^}]*)\}/g)) {
            if (!match[1]!.split(',').map(item => item.trim().replace(/\s+/g, ' ')).includes(selector)) continue;
            for (const part of match[2]!.split(';')) {
                const [name, ...value] = part.split(':');
                if (name && ['background', 'background-color'].includes(name.trim())) found = value.join(':').trim();
            }
        }
    }
    return found;
}

/** True when the colour hides everything behind it: no alpha below one. Tokens of this project are plain hex colours. */
function opaque(value: string): boolean {
    if (/^var\(--atlas-(bg|panel|panel-raised)\)$/.test(value)) return true;
    if (/^#([0-9a-f]{3}|[0-9a-f]{6})$/i.test(value)) return true;
    const rgb = /^rgba?\(([^)]*)\)$/.exec(value);
    if (!rgb) return false;
    const parts = rgb[1]!.split(/[\s,/]+/).filter(Boolean);
    if (parts.length === 3) return true;
    const alpha = parts[3]!;
    return alpha.endsWith('%') ? Number.parseFloat(alpha) >= 100 : Number.parseFloat(alpha) >= 1;
}

const OVERLAYS = {
    'Selection details': '.galaxy-selection-evidence.atlas-galaxy-selection-details',
    'path panel': '.atlas-galaxy-path-panel',
    'hierarchy key': '.atlas-hierarchy-key',
    'fit view': '.atlas-galaxy-fit',
    'render progress': '.atlas-graph-render-progress',
    'search results': '.atlas-galaxy-search-results',
    'Path to menu': '.atlas-graph-path-menu',
    'Edge types menu': '.atlas-trace-edge-menu',
    'more and Limits menu': '.atlas-graph-limits-menu',
    'Recent menu': '.atlas-graph-recent-menu',
    'node picker': '.atlas-galaxy-node-picker',
    'agents instrument': '.atlas-agents',
    'agents follow line': '.atlas-agents-followline',
    'agents timeline': '.atlas-agents-timeline',
    'placeholder': '.atlas-galaxy-placeholder',
    'hover card': '.atlas-galaxy-card',
    'coverage hover card': '.atlas-coverage-tooltip',
};

describe('K29: nothing of the scene shows through a Galaxy overlay', () => {
    for (const [name, selector] of Object.entries(OVERLAYS)) {
        it(`${name} is painted opaque`, () => {
            const value = background(selector);
            expect(value, `${selector} has no background`).toBeDefined();
            expect(opaque(value!), `${selector}: ${value}`).toBe(true);
        });
    }

    it('reads alpha in every notation the sheets use', () => {
        expect(opaque('rgba(13, 25, 19, .95)')).toBe(false);
        expect(opaque('rgb(10 14 13 / 82%)')).toBe(false);
        expect(opaque('rgb(13, 25, 19)')).toBe(true);
        expect(opaque('#0d1913')).toBe(true);
        expect(opaque('#0d191380')).toBe(false);
    });
});
