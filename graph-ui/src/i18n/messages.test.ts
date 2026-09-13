/**
 * Zwei Fragen an den Katalog, und beide sind an einem gruenen Bildschirm nicht
 * zu sehen.
 *
 * **Ist er vollstaendig?** Ein leerer Wert im Katalog ist ein Element, das im
 * Produkt als leerer Kasten dasteht, und niemand merkt es, weil ein leerer
 * Kasten wie Abstand aussieht. Der Test geht den Baum ab und verlangt von jedem
 * Blatt einen Inhalt: eine Zeichenkette mit Zeichen darin, oder eine Funktion,
 * die mit Beispielwerten aufgerufen etwas liefert.
 *
 * **Ist er die einzige Quelle?** Ein Katalog, neben dem die Komponenten weiter
 * ihre eigenen Saetze tragen, ist kein Katalog, sondern eine zweite Kopie. Der
 * Scan (tools/lib/chrome-scan.mjs) liest die Chrome-Dateien mit dem
 * TypeScript-Parser und meldet jede sichtbare Fundstelle, die noch ein Literal
 * traegt. Er laeuft bei jedem `npm run test:unit` mit, damit die naechste
 * hartkodierte Beschriftung sofort auffaellt und nicht erst im Beweislauf.
 */

import { describe, expect, it } from 'vitest';

import { messages } from './messages';
import { CHROME_FILES, scanChrome } from '../../tools/lib/chrome-scan.mjs';

/**
 * Beispielwerte fuer die Template-Funktionen des Katalogs.
 *
 * Nach Stelligkeit und nicht nach Namen: eine Funktion mit einem Parameter
 * bekommt einen Wert, eine mit zweien zwei. Die Werte sind absichtlich
 * unscheinbar; geprueft wird, dass die Funktion etwas zurueckgibt, nicht was.
 */
const sampleArguments = ['sample', 1, true, 'second'];

interface Leaf {
    path: string;
    value: string;
}

/** Den Baum abgehen und jedes Blatt zu einem Text machen. */
function leaves(node: unknown, path: string, out: Leaf[] = []): Leaf[] {
    if (typeof node === 'string') {
        out.push({ path, value: node });
        return out;
    }
    if (typeof node === 'function') {
        const arity = (node as (...args: unknown[]) => unknown).length;
        const args = sampleArguments.slice(0, Math.max(arity, 1));
        out.push({ path, value: String((node as (...args: unknown[]) => unknown)(...args)) });
        return out;
    }
    if (Array.isArray(node)) {
        node.forEach((entry, index) => leaves(entry, `${path}[${index}]`, out));
        return out;
    }
    if (node !== null && typeof node === 'object') {
        for (const [key, value] of Object.entries(node)) {
            leaves(value, path.length === 0 ? key : `${path}.${key}`, out);
        }
        return out;
    }
    out.push({ path, value: String(node) });
    return out;
}

describe('messages', () => {
    it('traegt die Bereiche, die der Contract nennt', () => {
        for (const area of [
            'menu', 'statusbar', 'why', 'tour', 'wizard', 'impact',
            'llm', 'chat', 'explorer', 'reader', 'galaxy',
        ] as const) {
            expect(messages, `Bereich ${area} fehlt`).toHaveProperty(area);
        }
    });

    it('hat keinen leeren Wert, in keinem Blatt', () => {
        const found = leaves(messages, '');
        // Ein Katalog mit fuenf Eintraegen waere vollstaendig und trotzdem
        // nichts: die untere Schranke haelt fest, dass wirklich der ganze
        // Rahmen hier steht.
        expect(found.length).toBeGreaterThan(60);
        const empty = found.filter((leaf) => leaf.value.trim().length === 0);
        expect(empty, `leere Eintraege: ${empty.map((leaf) => leaf.path).join(', ')}`)
            .toHaveLength(0);
    });

    it('schreibt keinen langen Strich in einen sichtbaren Satz', () => {
        // Dieselbe Regel wie im Stil-Gate (tools/style-gate.mjs), hier an der
        // Stelle, an der die Saetze entstehen: ein Katalog ist der eine Ort, an
        // dem ein Strich in die Oberflaeche kaeme.
        const dashed = leaves(messages, '').filter((leaf) => /[\u2013\u2014]/.test(leaf.value));
        expect(dashed.map((leaf) => leaf.path)).toEqual([]);
    });

    it('laesst in den Chrome-Komponenten keinen hartkodierten sichtbaren String stehen', () => {
        expect(CHROME_FILES.length).toBeGreaterThanOrEqual(11);
        const findings = scanChrome();
        const shown = findings
            .map((entry) => `${entry.file}:${entry.line} [${entry.rule}] ${entry.text}`)
            .join('\n');
        expect(findings, `hartkodierte Chrome-Strings:\n${shown}`).toHaveLength(0);
    });
});
