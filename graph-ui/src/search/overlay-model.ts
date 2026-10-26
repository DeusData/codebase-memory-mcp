/**
 * Was das Suchfenster ueber der Fusszeile zeigt, als reine Rechnung.
 *
 * Die Zeilen, die Auswahl und die Tastenbedeutungen stehen hier und nicht in
 * der Komponente, weil sie die Stellen sind, an denen eine Suche sich falsch
 * anfuehlt, wenn sie falsch ist: eine Auswahl, die am Rand haengenbleibt oder
 * ueberlaeuft, eine Zeile, die eine Datei nennt, die es nicht gibt, ein Escape,
 * das die Anfrage stehen laesst. Alles davon ist hier ohne DOM pruefbar.
 */

import type { RankedHit } from './semantic-search';

/**
 * Wie viele Zeilen das Fenster zeigt.
 *
 * Zehn: mehr als ein Leser ueberfliegt, bevor er das Wort praezisiert, und
 * wenig genug, dass das Fenster die Fusszeile nicht zur Seite macht. Was
 * darunter liegt, ist nicht weg, sondern eine Anfrage entfernt, und die
 * Kopfzeile des Fensters sagt, wie viele es insgesamt waren.
 */
export const MAX_SEARCH_ROWS = 10;

/**
 * Woher eine Zeile kommt.
 *
 * Zwei Werte, und der Unterschied ist keine Nuance: `index` heisst "der Server
 * hat auf genau dieses Wort geantwortet", `loaded` heisst "das kennt dieses
 * Fenster schon, der Index ist noch dabei". Eine Zeile ohne diese Angabe waere
 * die Behauptung, die Suche sei fertig, waehrend sie laeuft.
 */
export type SearchRowSource = 'index' | 'loaded';

/** Eine Zeile des Fensters, fertig zum Zeichnen. */
export interface SearchRow {
    name: string;
    /** Workspace-relativer Pfad, oder leer, wenn der Treffer keinen traegt. */
    path: string;
    /**
     * Die Deklarationszeile als `L23`, oder leer.
     *
     * Nur der Anfang, nicht der Bereich: die flache Suchform des Servers
     * antwortet mit einer Spanne, aus der die Schemaschicht die erste Zahl
     * liest (src/provider/rpc-schemas.ts). Ein Bereich stuende hier also nur,
     * wenn ihn jemand erfindet.
     */
    line: string;
    /** Welche Woerter der Anfrage dieser Treffer beantwortet hat. */
    matched: string;
    /** Der qualifizierte Name, fuer die Identitaet der Zeile. */
    key: string;
    /** Ob der Index diese Zeile beantwortet hat, oder ob sie vorlaeufig ist. */
    source: SearchRowSource;
}

/**
 * Die Zeilen zu einer Rangliste, gekappt auf das, was das Fenster zeigt.
 *
 * `key` ist der qualifizierte Name und nicht die Position, und das ist seit
 * W7b eine tragende Entscheidung: eine vorlaeufige Zeile und die Zeile, die der
 * Index zum selben Symbol schickt, tragen denselben Schluessel. React laesst
 * ihr Element dann stehen, statt es wegzuwerfen und neu zu bauen, und der
 * Austausch der Liste ist ein Wechsel der Beschriftung statt eines Flackerns.
 */
export function searchRows(
    ranked: readonly RankedHit[],
    limit: number = MAX_SEARCH_ROWS,
    source: SearchRowSource = 'index',
): SearchRow[] {
    return ranked.slice(0, limit).map((entry) => ({
        name: entry.hit.name,
        path: entry.hit.filePath ?? '',
        line: entry.hit.line === undefined ? '' : `L${entry.hit.line}`,
        matched: entry.matched.join(' + '),
        key: entry.hit.qualifiedName ?? `${entry.hit.filePath ?? ''}#${entry.hit.name}`,
        source,
    }));
}

/** Die Kopfzeile des Fensters: was gefunden wurde und was davon zu sehen ist. */
export function searchHeadline(query: string, total: number, shown: number): string {
    if (total === 0) {
        return `no symbol answers "${query}"`;
    }
    if (total > shown) {
        return `${total} hits for "${query}", top ${shown}`;
    }
    return total === 1 ? `1 hit for "${query}"` : `${total} hits for "${query}"`;
}

/** Was eine Taste im Suchfenster bedeutet. */
export type OverlayIntent = 'up' | 'down' | 'choose' | 'close' | 'none';

/**
 * Die Tastenbedeutung, an einer Stelle.
 *
 * Nur diese vier. Alles andere gehoert der Kommandozeile selbst, denn ein
 * Fenster, das Buchstaben abfaengt, waere ein Fenster, in dem man nicht
 * weitertippen kann.
 */
export function overlayIntent(key: string): OverlayIntent {
    switch (key) {
        case 'ArrowUp':
            return 'up';
        case 'ArrowDown':
            return 'down';
        case 'Enter':
            return 'choose';
        case 'Escape':
            return 'close';
        default:
            return 'none';
    }
}

/**
 * Die neue Auswahl nach einem Schritt.
 *
 * Am Rand bleibt sie stehen, statt umzulaufen: ein Umlauf von der letzten auf
 * die erste Zeile liest sich als Sprung und nicht als Bewegung, und in einer
 * Liste von zehn Zeilen ist das ein Fehler, den man erst beim Enter bemerkt.
 */
export function moveSelection(rowCount: number, current: number, delta: number): number {
    if (rowCount <= 0) {
        return 0;
    }
    const next = current + delta;
    if (next < 0) {
        return 0;
    }
    if (next >= rowCount) {
        return rowCount - 1;
    }
    return next;
}

/** Ob eine Anfrage lang genug ist, um gestellt zu werden. */
export function isSearchable(query: string, minLength: number): boolean {
    return query.trim().length >= minLength;
}
