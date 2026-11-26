/**
 * Die aufgezeichneten IRs und die Presentations, durch die der Twin sie zeigt.
 *
 * Eine festgeschriebene IR statt eines Bauers je Test. Jede Tiefe muss eine
 * Sicht auf DIESELBEN Fakten sein, damit die Aussagen ueberhaupt etwas
 * bedeuten: eine je Test anders zusammengesetzte Fixture liesse zwei Tiefen
 * einander widersprechen, ohne dass ein Test das bemerkt, und genau das ist der
 * Fehler, gegen den das Render-Modell existiert.
 *
 * Die Dateien in src/twin/__fixtures__/ sind byte-identisch aus CodeAtlasIDE
 * uebernommen; woher genau, steht in der HERKUNFT.md daneben. Sie werden hier
 * gelesen und nicht importiert, damit ein Import in kein Bundle geraet: der
 * Twin des Produkts liest seine IR vom Server, nie aus einer Datei.
 *
 * Angelehnt an theia-extensions/codeatlas-views/test/support/presentations.ts.
 * Der Snapshot-Reduzierer `snapshotOf` ist NICHT mitgekommen: dieses Projekt
 * nagelt die zentralen Erwartungen als direkte Zusicherungen fest, statt eine
 * Snapshot-Datei zu fuehren, die nur im Referenzprojekt einen Reviewer hat.
 */

import { readFileSync, readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import type { SemanticIR } from '../core/semantic-ir';
import { Facet } from '../twin/presentation-profile';
import type {
    DepthLevel,
    ResolvedPresentation,
    TerminologyLevel,
} from '../twin/presentation-profile';
import type { TwinViewModel } from '../twin/render-model';

/*
 * Der Pfad wird aus `import.meta.url` zusammengesetzt und NICHT ueber
 * `new URL('...', import.meta.url)` gebildet. Das ist kein Stilfrage: Vite
 * erkennt genau dieses Muster als Asset-Verweis und schreibt es um, so dass in
 * der jsdom-Umgebung `http://localhost:3000/...` herauskaeme statt eines
 * Dateipfads. Gemessen, nicht vermutet.
 */
const FIXTURE_DIR = join(dirname(fileURLToPath(import.meta.url)), '..', 'twin', '__fixtures__');

function readFixture(name: string): SemanticIR {
    return JSON.parse(readFileSync(join(FIXTURE_DIR, name), 'utf8')) as SemanticIR;
}

export const CREATE_USER_IR: SemanticIR = readFixture('create-user-ir.json');

/** Der qualifizierte Name, den die Erzaehl-Tiefe nie zeigen darf. */
export const QUALIFIED_NAME = CREATE_USER_IR.symbol.qualifiedName ?? '';

/** Ein Punktpfad aus drei oder mehr Segmenten: die Form jedes qualifizierten Namens. */
export const QUALIFIED_SHAPE = /\w+\.\w+\.\w+/;

/** Alles, was das Produkt heute beantworten kann. */
export const CORE_FACETS: readonly Facet[] = [
    Facet.Logic,
    Facet.Calls,
    Facet.Data,
    Facet.Errors,
    Facet.Tests,
];

/** Dasselbe, plus die zwei Linsen, die Ueberschrift vor Fakt sind. */
export const ALL_FACETS: readonly Facet[] = [...CORE_FACETS, Facet.Runtime, Facet.Changes];

/** Errors und Data aus, damit ihre Sektionen ganz verschwinden muessen. */
export const NARROW_FACETS: readonly Facet[] = [Facet.Logic, Facet.Calls, Facet.Tests];

export function presentation(
    depth: DepthLevel,
    facets: readonly Facet[],
    terminology: TerminologyLevel = 'technical',
): ResolvedPresentation {
    return { depth, facets: new Set(facets), terminology, conceptCallouts: false };
}

/** Die Namen der Sektionen, die eine Presentation erzeugt hat, in ihrer Reihenfolge. */
export function sectionNames(model: TwinViewModel): string[] {
    return model.sections.map((section) => section.name);
}

/** Eine aufgezeichnete IR und der stabile Teil ihres Testnamens. */
export interface RecordedIr {
    /** `ir-getOrder` fuer `ir-getOrder.json`. */
    id: string;
    ir: SemanticIR;
}

/**
 * Jede aufgezeichnete IR, in fester Reihenfolge.
 *
 * Sortiert statt in Verzeichnisreihenfolge gelassen: ein Testbericht ist etwas,
 * das ein Mensch liest, und ein Block, der die Stelle wechselt, weil ein
 * Dateisystem anders aufzaehlt, kostet genau die Aufmerksamkeit, die den
 * echten Unterschieden gehoert.
 */
export const RECORDED_IRS: RecordedIr[] = readdirSync(FIXTURE_DIR)
    .filter((name) => name.startsWith('ir-') && name.endsWith('.json'))
    .sort()
    .map((name) => ({ id: name.replace(/\.json$/, ''), ir: readFixture(name) }));
