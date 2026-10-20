/**
 * Welche Reiter der Erklaeren-Bereich hat, welcher davon gerade etwas zu zeigen
 * hat, und was der eingeklappte Streifen ueber ihn sagt.
 *
 * ## Warum ein Reiter, der nichts hat, trotzdem dasteht
 *
 * Bis W8 verschwand jede dieser Flaechen, sobald sie nichts hatte: kein Zug, kein
 * Chat-Panel; keine Fuehrung, keine Schrittkarte. Das ist die bequeme Loesung und
 * die falsche. Ein Bedienelement, das mal da ist und mal nicht, zwingt den Leser
 * bei jeder Frage zuerst zu der Frage, wo es hin ist, und die Antwort "es gibt
 * hier gerade nichts" bekommt er nie: er sieht nur eine Leere und muss raten, ob
 * das Produkt es nicht kann, es nicht hat oder er es falsch bedient.
 *
 * Also steht jeder Reiter immer da. Hat er nichts, ist er gedimmt UND er sagt
 * seinen Grund, und zwar im Feld darunter und nicht in einem Tooltip: dieselbe
 * Regel, mit der W7a die Attrappen aus der Menuezeile geworfen hat. Ein Grund,
 * den man nur mit der Maus findet, ist fuer den, der ihn braucht, nicht da.
 *
 * ## Warum die Reiter aus Tatsachen entstehen und nicht aus Zustandsschaltern
 *
 * `explainTabs` bekommt, was WAHR ist (steht ein Symbol im Twin, laeuft eine
 * Fuehrung, wie viele Fragen liegen im Chat), und nicht, was OFFEN ist. Damit
 * kann kein Reiter behaupten, er habe etwas, weil ihn jemand aufgeschlagen hat.
 * Die Umkehrung waere genau der Fehler, gegen den dieses Produkt gebaut ist:
 * eine Flaeche, die dasteht, weil sie geoeffnet wurde, und nicht, weil sie etwas
 * zu sagen hat.
 */

import { messages } from '../i18n/messages';

/** Die Reiter, in der Reihenfolge, in der sie in der Leiste stehen. */
export const EXPLAIN_TAB_IDS = ['flow', 'walk', 'chat', 'bug', 'change'] as const;

/** Der Name eines Reiters. Er steht auch in `data-tab` im DOM. */
export type ExplainTabId = (typeof EXPLAIN_TAB_IDS)[number];

/**
 * Was der Bereich ueber die Welt weiss.
 *
 * Absichtlich klein und absichtlich ohne einen einzigen Schalter: hier steht,
 * was es GIBT, und die Frage, was gerade offen ist, gehoert woanders hin.
 */
export interface ExplainFacts {
    /** Ob ueberhaupt ein Projekt geladen ist. Ohne das lesen zwei Reiter nichts. */
    hasProject: boolean;
    /** Der Name des Symbols im Twin. Leer heisst: keins, also kein Bild. */
    flowSubject: string;
    /** Wo der Flow steht. -1 heisst: noch kein Schritt gegangen. */
    flowStep: number;
    /** Ob eine Fuehrung laeuft. */
    walkRunning: boolean;
    /** Der Schritt der Fuehrung, nullbasiert. */
    walkStep: number;
    /** Wie viele Schritte die Fuehrung hat. */
    walkSteps: number;
    /** Wie viele Fragen in dieser Sitzung liegen. */
    chatTurns: number;
}

/** Ein Reiter, wie ihn die Leiste zeichnet. */
export interface ExplainTabState {
    id: ExplainTabId;
    /** Die Beschriftung in der Leiste. */
    label: string;
    /** Der Tooltip: was hinter dem Reiter liegt. Er ersetzt keinen Grund. */
    title: string;
    /** Ob dieser Reiter gerade etwas zu zeigen hat. */
    enabled: boolean;
    /** Der Satz, der im Feld steht, solange er nichts hat. Nie leer. */
    reason: string;
    /** Die eine Zeile, die der eingeklappte Streifen ueber ihn zeigt. Nie leer. */
    note: string;
}

/** Die Reiter fuer diese Tatsachen. Immer alle fuenf, immer in dieser Reihenfolge. */
export function explainTabs(facts: ExplainFacts): ExplainTabState[] {
    const label = messages.layout.tab;
    const title = messages.layout.tabTitle;
    const why = messages.layout.disabled;
    const note = messages.layout.note;
    return [
        {
            id: 'flow',
            label: label.flow,
            title: title.flow,
            enabled: facts.flowSubject.length > 0,
            reason: why.flow,
            note: facts.flowSubject.length === 0
                ? note.flowIdle
                : note.flow(facts.flowSubject),
        },
        {
            id: 'walk',
            label: label.walk,
            title: title.walk,
            enabled: facts.walkRunning && facts.walkSteps > 0,
            reason: why.walk,
            note: facts.walkRunning && facts.walkSteps > 0
                ? note.walk(Math.min(facts.walkStep + 1, facts.walkSteps), facts.walkSteps)
                : note.walkIdle,
        },
        {
            id: 'chat',
            label: label.chat,
            title: title.chat,
            enabled: facts.chatTurns > 0,
            reason: why.chat,
            note: facts.chatTurns > 0 ? note.chat(facts.chatTurns) : note.chatIdle,
        },
        {
            id: 'bug',
            label: label.bug,
            title: title.bug,
            enabled: facts.hasProject,
            reason: why.bug,
            note: note.bug,
        },
        {
            id: 'change',
            label: label.change,
            title: title.change,
            enabled: facts.hasProject,
            reason: why.change,
            note: note.change,
        },
    ];
}

/** Der Reiter mit diesem Namen, oder undefined. */
export function explainTabOf(tabs: readonly ExplainTabState[], id: ExplainTabId): ExplainTabState | undefined {
    return tabs.find((tab) => tab.id === id);
}
