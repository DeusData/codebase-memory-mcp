/*
 * Review of K40: since A1 Back, Forward and the subtabs share one row, but the
 * row jumped between Overview, Routes and Hotspots on one side and System
 * structure and Behavior on the other: those two set the view's frame to 16 px
 * instead of 28 and 32 and made the tabs narrower. "← Back" stood at x 32, y 98
 * in the first three and at x 16, y 86 in the other two. No rule for the
 * system views may set the view's frame or the row differently, so the row
 * stands alike in every subtab.
 */
import { readFileSync } from 'node:fs';
import { expect, it } from 'vitest';

const SHEETS = ['architecture.css', 'system-architecture.css', 'behavior-journey.css', 'spatial-architecture.css', 'container-map.css', 'architecture-scene.css', 'source-evidence.css']
    .map((name) => ({ name, css: readFileSync(new URL(`./${name}`, import.meta.url), 'utf8').replace(/\/\*[\s\S]*?\*\//g, '') }));
const ROW = /\.atlas-arch-(tabrow|tabs|tab|history|recent)\b/;
const BOX = /^(padding|margin)(-|$)/;

it('K40: System structure and Behavior keep the subtab row where the other subtabs have it', () => {
    const offending: string[] = [];
    for (const { name, css } of SHEETS) {
        for (const match of css.matchAll(/([^{}]+)\{([^}]*)\}/g)) {
            for (const selector of match[1]!.split(',').map((item) => item.trim())) {
                if (!selector.includes('[data-system-view')) continue;
                const properties = match[2]!.split(';').map((part) => part.split(':')[0]!.trim()).filter(Boolean);
                // The view's own frame, or anything about the row with the history buttons and the subtabs.
                const frame = /^\.atlas-architecture\[data-system-view[^\]]*\]$/.test(selector) && properties.some((property) => BOX.test(property));
                if (frame || ROW.test(selector)) offending.push(`${name}: ${selector} { ${properties.join(', ')} }`);
            }
        }
    }
    expect(offending).toEqual([]);
});
