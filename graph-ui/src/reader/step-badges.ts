/**
 * Die Schritt-Nummern im Rand des Readers, als Monaco-Dekorationen.
 *
 * Das ist der Ersatz fuer `CodeAtlasStepBadgeDecorator` aus CodeAtlasIDE
 * (theia-extensions/codeatlas-core/src/browser/step-badge-decorator.ts, Klasse
 * ab Zeile 90). Die REGEL, welche Zeile welche Nummer bekommt, ist nicht hier:
 * sie steht portiert in src/core/step-badge-decorator.ts, und dieses Modul
 * malt nur, was dort entschieden wurde. Zwei Stellen, die numerieren duerfen,
 * waeren zwei Meinungen darueber, welcher Aufruf Schritt drei ist.
 *
 * Vier Entscheidungen der Vorlage sind hier unveraendert:
 *
 * 1. **`linesDecorationsClassName`, nicht `inlineClassName`.** Das Badge sitzt
 *    im Rand neben der Zeilennummer und verschiebt kein einziges Zeichen des
 *    gelesenen Quelltextes.
 * 2. **Die Ziffer kommt aus dem Stylesheet**, ueber die numerierte Klasse
 *    `codeatlas-step-badge-N`. Es gibt genau neun davon und es wird nie mehr
 *    geben, also kostet das nichts und macht das Badge umfaerbbar, ohne dass
 *    dieses Modul davon weiss.
 * 3. **Die Spanne ist leer und liegt auf EINER Zeile.** Eine Spanne, die am
 *    Anfang der Folgezeile endet, wuerde die Folgezeile mitdekorieren und jedes
 *    Badge zweimal zeichnen: einmal am Aufruf und einmal an dem, was zufaellig
 *    darunter steht.
 * 4. **Die Zeile klebt nicht am Text.** `stickiness` steht auf
 *    `NeverGrowsWhenTypingAtEdges`, wie in der Vorlage.
 *
 * Ein Unterschied zur Vorlage, und er ist eine Zeilenzaehlung, kein Verhalten:
 * Theias `EditorDecoration` nimmt 0-basierte Positionen, Monacos `IRange` nimmt
 * 1-basierte. `StepBadge.line` ist eine 1-basierte Graph-Zeile, geht hier also
 * direkt als `lineNumber` durch, wo die Vorlage `toEditorLine` davorsetzt. Das
 * Ergebnis ist dieselbe Zeile; nur der Nullpunkt der beiden Editoren ist ein
 * anderer.
 */

import { STEP_BADGE_CLASS, STEP_BADGE_PULSE_CLASS } from '../core/step-badge-decorator';
import type { StepBadge } from '../core/step-badge-decorator';

/**
 * Die Klasse der Zeile, auf die eine Schritt-Zeile im Panel gerade zeigt.
 *
 * Eine eigene Klasse und eine eigene Dekorationsmenge: die Badges und diese
 * Hervorhebung werden getrennt gesetzt, weil sie unterschiedlich oft wechseln
 * (die Badges einmal je Symbol, die Hervorhebung bei jeder Mausbewegung ueber
 * die Liste) und eine gemeinsame Menge die jeweils andere bei jedem Setzen
 * loeschen wuerde. Genau daran krankte die Vorlage, bevor sie zwei Decorator
 * bekam.
 */
export const STEP_LINE_HIGHLIGHT_CLASS = 'codeatlas-step-line';

/**
 * Monacos `TrackedRangeStickiness.NeverGrowsWhenTypingAtEdges`, als Zahl.
 *
 * Ausgeschrieben statt importiert, damit dieses Modul ohne Monaco geprueft
 * werden kann: ein Import von `monaco-editor` zoege den ganzen Editor samt
 * seiner CSS- und Worker-Importe in einen Unit-Test, der eine Zahl und zwei
 * Klassennamen prueft.
 *
 * Der Wert ist abgelesen, nicht geraten:
 * node_modules/monaco-editor/esm/vs/editor/editor.api.d.ts, Zeile 2007,
 * `enum TrackedRangeStickiness { ... NeverGrowsWhenTypingAtEdges = 1 ... }`
 * in monaco-editor 0.56, der Fassung, die package.json exakt pinnt.
 *
 * Was er hier bewirkt, ist ausserdem klein: der Reader ist read-only, es wird
 * nie an einer Kante getippt. Er steht da, weil die Vorlage ihn setzt und ein
 * stiller Unterschied zur Vorlage teurer waere als eine Zeile Gleichstand.
 */
export const NEVER_GROWS_WHEN_TYPING_AT_EDGES = 1;

/** Die Form, die `createDecorationsCollection` erwartet, ohne Monaco-Import. */
export interface LineDecoration {
    range: {
        startLineNumber: number;
        startColumn: number;
        endLineNumber: number;
        endColumn: number;
    };
    options: {
        linesDecorationsClassName?: string;
        className?: string;
        isWholeLine?: boolean;
        stickiness?: number;
        hoverMessage?: { value: string };
    };
}

/** Die Klassen eines Badges: die gemeinsame, die numerierte, und ggf. der Puls. */
export function stepBadgeClasses(ordinal: number, pulsing: boolean): string {
    const classes = [STEP_BADGE_CLASS, `${STEP_BADGE_CLASS}-${ordinal}`];
    if (pulsing) {
        classes.push(STEP_BADGE_PULSE_CLASS);
    }
    return classes.join(' ');
}

/**
 * Die Dekorationen fuer eine Badge-Menge, mit dem Puls auf der Caret-Zeile.
 *
 * `pulseLine` ist eine 1-basierte Graph-Zeile wie `badge.line`. Steht der Caret
 * auf keiner Aufrufstelle, pulst nichts; das ist kein Fehlerfall, sondern die
 * haeufigste Lage.
 */
export function badgeDecorations(
    badges: readonly StepBadge[],
    pulseLine?: number,
): LineDecoration[] {
    return badges.map((badge) => ({
        range: {
            startLineNumber: badge.line,
            startColumn: 1,
            endLineNumber: badge.line,
            endColumn: 1,
        },
        options: {
            linesDecorationsClassName: stepBadgeClasses(badge.ordinal, badge.line === pulseLine),
            // Das Badge gehoert zur Zeile, nicht zu einer Zeichenspanne.
            stickiness: NEVER_GROWS_WHEN_TYPING_AT_EDGES,
            ...(badge.label === undefined
                ? {}
                : { hoverMessage: { value: `Step ${badge.ordinal}: ${badge.label}` } }),
        },
    }));
}

/**
 * Die Hervorhebung EINER Zeile, oder gar keine.
 *
 * Ein Feld statt eines Wertes, damit der Aufrufer die Menge in beiden Faellen
 * gleich setzt: "nichts hervorheben" ist eine leere Menge und kein Sonderweg,
 * und ein Sonderweg waere die Stelle, an der eine alte Hervorhebung stehen
 * bleibt.
 */
export function highlightDecorations(line: number | undefined): LineDecoration[] {
    if (line === undefined || line < 1) {
        return [];
    }
    return [
        {
            range: { startLineNumber: line, startColumn: 1, endLineNumber: line, endColumn: 1 },
            options: {
                className: STEP_LINE_HIGHLIGHT_CLASS,
                isWholeLine: true,
                stickiness: NEVER_GROWS_WHEN_TYPING_AT_EDGES,
            },
        },
    ];
}
