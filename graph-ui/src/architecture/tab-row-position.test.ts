/*
 * Review of K40: the subtab row must stay in the same place across Overview,
 * Routes, Hotspots, System structure and Behavior. History now lives beside
 * each view's location, so those local controls may have their own layout.
 * No system-view rule may change the workspace frame or the tabs themselves.
 */
import { readFileSync } from 'node:fs';
import { expect, it } from 'vitest';

const SHEETS = ['architecture.css', 'system-architecture.css', 'behavior-journey.css', 'spatial-architecture.css', 'container-map.css', 'architecture-scene.css', 'source-evidence.css']
    .map((name) => ({ name, css: readFileSync(new URL(`./${name}`, import.meta.url), 'utf8').replace(/\/\*[\s\S]*?\*\//g, '') }));
const ROW = /\.atlas-arch-(tabrow|tabs|tab)\b/;
const BOX = /^(padding|margin)(-|$)/;

it('K40: System structure and Behavior keep the subtab row where the other subtabs have it', () => {
    const offending: string[] = [];
    for (const { name, css } of SHEETS) {
        for (const match of css.matchAll(/([^{}]+)\{([^}]*)\}/g)) {
            for (const selector of match[1]!.split(',').map((item) => item.trim())) {
                if (!selector.includes('[data-system-view')) continue;
                const properties = match[2]!.split(';').map((part) => part.split(':')[0]!.trim()).filter(Boolean);
                // Keep the workspace frame and the view-only tab row consistent.
                const frame = /^\.atlas-architecture\[data-system-view[^\]]*\]$/.test(selector) && properties.some((property) => BOX.test(property));
                if (frame || ROW.test(selector)) offending.push(`${name}: ${selector} { ${properties.join(', ')} }`);
            }
        }
    }
    expect(offending).toEqual([]);
});
