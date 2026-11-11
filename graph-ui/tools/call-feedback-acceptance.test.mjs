/*
 * Handtest K3: die Abnahme G7 mass die Leiste eines Ausschnitts nur bei
 * geschlossenem Chat, und genau dort brach sie nicht um. Diese Pruefung haelt
 * fest, dass G7 auch mit offenem Chat bei 1600 px misst und dort eine Zeile
 * verlangt. Das Skript selbst startet einen Browser; hier wird nur sein
 * G7-Abschnitt gelesen.
 */
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { expect, it } from 'vitest';

const source = readFileSync(fileURLToPath(new URL('./call-feedback-acceptance.mjs', import.meta.url)), 'utf8');
const g7 = source.slice(source.indexOf('// G7'), source.indexOf('// G3'));

it('K3: acceptance G7 measures the scoped toolbar with the chat closed and open at 1600 px and wants one row in both', () => {
    expect(g7.length).toBeGreaterThan(0);
    expect(g7).toContain("name: 'Open chat'");
    expect(g7).toContain("name: 'Hide chat'");
    // A measurement with the chat closed and one with it open, both in the result.
    expect(g7).toMatch(/\('closed'\)/);
    expect(g7).toMatch(/\('open'\)/);
    // One row for each measurement, not only for the first.
    expect(g7).toMatch(/\.every\(/);
    expect(g7).toMatch(/rows\s*===\s*1/);
});
