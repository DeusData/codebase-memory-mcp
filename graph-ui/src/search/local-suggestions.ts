/**
 * Vorschlaege aus dem, was ohnehin schon geladen ist.
 *
 * ## Warum es sie gibt
 *
 * Nutzerbefund vom 2026-08-29: die Suchvorschlaege erscheinen zu langsam. Der
 * Weg war bis dahin: tippen, 200 ms warten, ein Wort je Anfrage an den Server,
 * dann die erste Zeile. In dieser Zeit ist der Bildschirm leer, obwohl der
 * Browser die Antwort auf einen guten Teil der Frage schon im Speicher hat.
 *
 * Zwei Quellen liegen dort und kosten keinen Serverweg:
 *
 *  1. **Die Knoten des Layouts.** Die Galaxie laedt sie beim Start; jeder traegt
 *     Name, qualifizierten Namen, Datei und Deklarationszeile.
 *  2. **Die Dateien des Baums.** Der Explorer laedt sie beim Start ebenfalls,
 *     und ein Leser, der `userService` tippt, meint oft genau die Datei.
 *
 * ## Was diese Vorschlaege sind und was nicht
 *
 * Sie sind **vorlaeufig** und sagen das auch: die Zeile traegt eine Marke, und
 * sobald die Antwort des Index da ist, steht dort das Ergebnis der Suche. Ein
 * lokaler Vorschlag ist keine Behauptung ueber den Index, sondern eine Aussage
 * ueber das, was dieses Fenster schon weiss. Er ist trotzdem nie eine
 * Attrappe: jede Zeile fuehrt an eine Stelle, die dieses Fenster oeffnen kann.
 *
 * Gerankt wird mit **derselben** Funktion wie die Antwort des Servers
 * (`rankHits`). Eine eigene Ordnung fuer die vorlaeufigen Zeilen waere die
 * teuerste Art, den Sprung zu bauen, den dieser Zyklus abschaffen soll: die
 * Liste wuerde beim Eintreffen der Antwort nicht ersetzt, sondern umsortiert.
 *
 * ## Die eine Abweichung, und warum sie eine ist
 *
 * `isNavigable` (semantic-search.ts) wirft Treffer weg, die nach ihrer eigenen
 * Datei heissen, und Treffer mit der Art `module`. Der Grund dort ist richtig:
 * die Suchantwort des Servers enthaelt Datei- und Modulzeilen als Dubletten der
 * Symbolzeilen, und ein Modul hat keine Deklaration, zu der man springen
 * koennte. Hier ist die Lage eine andere: der Baum liefert Dateien, keine
 * Dubletten, und diese Oberflaeche oeffnet eine Datei mit einem Klick, genau so
 * wie der Explorer es tut. Eine Datei bekommt deshalb den Stamm ihres Namens
 * (`userService` fuer `src/services/userService.ts`) und die Art `unknown`: der
 * Index hat sie nicht als Symbol eingeordnet, und etwas anderes zu behaupten
 * waere eine erfundene Symbolart.
 */

import type { CodeAtlasSymbolKind } from '../core/focus-protocol';
import type { SymbolSearchHit } from '../core/intelligence-provider';
import type { RankedHit } from './semantic-search';
import { rankHits } from './semantic-search';

/** Wie viele Sofort-Vorschlaege hoechstens gerechnet werden. */
export const MAX_LOCAL_SUGGESTIONS = 10;

/** Was der Browser schon geladen hat, in der Form, die diese Datei braucht. */
export interface LocalIndex {
    /** Symbole aus dem Layout der Galaxie. */
    symbols: readonly SymbolSearchHit[];
    /** Dateipfade aus dem Baum, workspace-relativ. */
    files: readonly string[];
}

/** Nichts geladen. Als benannte Konstante, damit sie eine stabile Referenz ist. */
export const EMPTY_LOCAL_INDEX: LocalIndex = { symbols: [], files: [] };

/** Die Art, die eine Datei aus dem Baum bekommt. Siehe Kopf, letzter Abschnitt. */
export const FILE_CANDIDATE_KIND: CodeAtlasSymbolKind = 'unknown';

/**
 * Der Name, unter dem eine Datei als Kandidat auftritt: ihr Basisname ohne die
 * letzte Endung.
 *
 * Ohne Endung, weil ein Leser `userService` tippt und nicht `userService.ts`,
 * und weil ein Kandidat, der genau so heisst wie das Ende seines Pfades, von
 * `isNavigable` als Dublette verworfen wuerde.
 */
export function fileStem(path: string): string {
    const base = path.split('/').pop() ?? '';
    const dot = base.lastIndexOf('.');
    return dot > 0 ? base.slice(0, dot) : base;
}

/** Die Kandidaten, die die Dateien des Baums beitragen. */
export function fileCandidates(files: readonly string[]): SymbolSearchHit[] {
    const seen = new Set<string>();
    const out: SymbolSearchHit[] = [];
    for (const path of files) {
        const name = fileStem(path);
        if (name.length === 0 || seen.has(path)) {
            continue;
        }
        seen.add(path);
        out.push({ name, kind: FILE_CANDIDATE_KIND, filePath: path });
    }
    return out;
}

/**
 * Alles, was der Browser als Kandidat hergibt, ungerankt.
 *
 * Getrennt von {@link localSuggestions}, weil die Kommandozeile die Kandidaten
 * mit denen des Praefix-Caches in EINEN Rang wirft: zweimal zu ranken und dann
 * zwei Listen zu mischen waere eine zweite Ordnung neben der einen, die dieses
 * Produkt hat.
 */
export function localCandidates(index: LocalIndex): SymbolSearchHit[] {
    return [...index.symbols, ...fileCandidates(index.files)];
}

/**
 * Die Sofort-Vorschlaege zu einer Anfrage, ohne einen einzigen Serverweg.
 *
 * Leer heisst hier wirklich leer: nichts Geladenes beantwortet dieses Wort. Die
 * Kommandozeile zeigt dann, dass gesucht wird, statt eine Zeile zu erfinden.
 */
export function localSuggestions(
    index: LocalIndex,
    query: string,
    fanInOf: (hit: SymbolSearchHit) => number = () => 0,
    limit: number = MAX_LOCAL_SUGGESTIONS,
): RankedHit[] {
    if (index.symbols.length === 0 && index.files.length === 0) {
        return [];
    }
    return rankHits(localCandidates(index), query, fanInOf).slice(0, limit);
}
