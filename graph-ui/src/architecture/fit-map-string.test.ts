/*
 * Review of K42: the line of the service map that got the shared Refresh
 * control still carried "Fit map" as text in the TSX. Visible words live in
 * architecture/strings.ts; both maps take the button's name from there.
 */
import { readFileSync } from 'node:fs';
import { expect, it } from 'vitest';
import { architectureText } from './strings';

it('K42: "Fit map" comes from the strings of Architecture, not from the TSX of the maps', () => {
    expect(architectureText.fitMap).toBe('Fit map');
    for (const file of ['ContainerMap.tsx', 'SpatialArchitecture.tsx']) {
        const source = readFileSync(new URL(`./${file}`, import.meta.url), 'utf8');
        expect(source, file).not.toMatch(/>\s*Fit map\s*</);
        expect(source, file).toContain('{text.fitMap}');
    }
});
