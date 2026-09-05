/**
 * Woher die Kandidaten kommen, die die Rangfolge dann ordnet.
 *
 * Das Vorbild ist die Kommando-Haelfte des Referenzprojekts
 * (CodeAtlasIDE, theia-extensions/codeatlas-views/src/browser/search/search-command.ts).
 * Uebernommen ist ihre Regel: **eine Anfrage je Wort, nicht eine je Phrase.**
 * Kein Symbol in irgendeinem Repository heisst "user validation"; wer die
 * Phrase als eine Anfrage stellt, bekommt nichts und haelt das fuer eine
 * Aussage ueber den Code. Die Vereinigung der Wort-Antworten ist die Menge,
 * ueber die dann gerankt wird, damit ein Symbol, das nur das zweite Wort
 * traegt, ueberhaupt eine Chance hat.
 *
 * Drei Abweichungen vom Vorbild, alle drei erzwungen und alle drei benannt:
 *
 * 1. **BM25 statt Namensmuster.** Der Server beantwortet die flache,
 *    maschinenlesbare Suchform nur ueber `query` mit BM25-Rang
 *    (src/provider/rpc-client.ts). Die name_pattern-Form kommt als nach
 *    Modulen gruppierte Anzeigeform zurueck. Fuer die Rangfolge unten ist das
 *    unerheblich: sie rechnet ihre eigene Ordnung ueber Woerter und benutzt
 *    den Rang der Engine an keiner Stelle.
 * 2. **25 Kandidaten je Wort statt 40.** Ein Wort, das mehr als 25 Symbole
 *    trifft, grenzt nichts mehr ein; die BM25-Form liefert ihre besten zuerst,
 *    also ist der Schnitt der obere Rand und nicht ein zufaelliges Praefix.
 * 3. **Fan-in kommt aus dem Layout, wenn eines geladen ist.** Das
 *    Referenzprojekt fragt dafuer eine zweite Runde Komplexitaetswerte ab.
 *    Dieses Produkt hat die Zahl schon im Haus: `in_calls` steht an jedem
 *    Knoten der Galaxie. Ohne geladenes Layout hat jeder Kandidat kein
 *    Fan-in, und das aendert nur, wer bei Gleichstand vorne steht.
 */

import type { SymbolSearchHit } from '../core/intelligence-provider';
import type { RankedHit } from './semantic-search';
import { queryTerms, rankHits } from './semantic-search';

/**
 * Wie viele Kandidaten ein Wort beitragen darf.
 *
 * Siehe Abweichung 2 im Kopf.
 */
export const CANDIDATES_PER_TERM = 25;

/**
 * Wie lange die Kommandozeile still steht, bevor gefragt wird.
 *
 * Bis W7b waren es 200 ms, und die Begruendung war die Pause zwischen zwei
 * Woertern. Sie war falsch herum gedacht: der Serverweg kostet an diesem Index
 * wenige Millisekunden, die Wartezeit war also fast vollstaendig selbst
 * gemacht. Nutzerbefund vom 2026-08-29: die Vorschlaege erscheinen zu langsam.
 *
 * 90 ms sind kuerzer als die Pause zwischen zwei Tastendruecken beim fluessigen
 * Tippen (ueber 100 ms) und lang genug, dass ein Wort nicht Buchstabe fuer
 * Buchstabe gefragt wird. Der eigentliche Gewinn steht aber nicht hier, sondern
 * in App.tsx: waehrend dieser 90 ms ist die Liste nicht leer, sondern zeigt die
 * Sofort-Vorschlaege aus den Daten, die ohnehin schon geladen sind.
 */
export const SEARCH_DEBOUNCE_MS = 90;

/** Ab wann ueberhaupt gesucht wird. Ein Buchstabe grenzt nichts ein. */
export const SEARCH_MIN_QUERY = 2;

/**
 * Was diese Suche vom Provider braucht, und sonst nichts.
 *
 * Absichtlich ein Ausschnitt und kein Import des ganzen Providers: so laesst
 * sich die Beschaffung mit einer Handvoll erfundener Zeilen pruefen, ohne
 * Server, ohne Transport und ohne dass ein Test wissen muss, wie /rpc redet.
 */
export interface SymbolSearcher {
    searchSymbols(
        root: string,
        pattern: string,
        limit?: number,
        opts?: { projectName?: string; signal?: AbortSignal },
    ): Promise<SymbolSearchHit[]>;
}

/** Die Stellschrauben einer Anfrage: das Projekt und der Abbruch. */
export interface MeaningOptions {
    projectName?: string;
    /**
     * Abbruch, wenn diese Anfrage ueberholt wurde.
     *
     * Seit W7b, und nicht als Ersatz fuer die Ticketpruefung in App.tsx,
     * sondern davor: das Ticket verhindert, dass eine ueberholte Antwort die
     * neuere ueberschreibt, aber sie ist dann trotzdem gelaufen. Bei einem Wort
     * je Anfrage sind das bei sechs Woertern sechs Anfragen, die niemand mehr
     * lesen will, und sie stehen in der Warteschlange vor denen, die noch
     * jemand liest.
     */
    signal?: AbortSignal;
}

/** Was eine Runde ueber den Server erbracht hat. */
export interface MeaningAnswer {
    /** Die Treffer, geordnet. */
    hits: RankedHit[];
    /**
     * Die rohen Kandidaten dieser Runde.
     *
     * Sie bleiben erhalten, weil das Verlaengern eines Wortes sie noch einmal
     * brauchen kann (Praefix-Cache in App.tsx). Gerankt wird dabei neu, mit
     * derselben Funktion: eine zweite Ordnung fuer denselben Zweck gaebe es
     * hier nicht.
     */
    candidates: SymbolSearchHit[];
    /**
     * True, wenn kein Wort den Kandidatendeckel erreicht hat.
     *
     * Nur dann ist die Runde vollstaendig; hat ein Wort genau
     * {@link CANDIDATES_PER_TERM} Kandidaten geliefert, hat die Engine
     * moeglicherweise abgeschnitten, und eine Wiederverwendung waere eine
     * Antwort mit unbekannten Luecken.
     */
    complete: boolean;
    /** True, wenn die Runde abgebrochen wurde. Ihre Treffer sind dann nichts wert. */
    aborted: boolean;
}

/** Die Kandidaten eines Wortes, oder nichts, wenn die Engine das Wort nicht beantwortet. */
async function candidatesFor(
    searcher: SymbolSearcher,
    root: string,
    term: string,
    opts: MeaningOptions,
): Promise<SymbolSearchHit[]> {
    try {
        return await searcher.searchSymbols(root, term, CANDIDATES_PER_TERM, opts);
    } catch {
        // Ein Wort, das die Engine nicht beantwortet, traegt nichts bei. Die
        // anderen Woerter tun es weiter: das ist eine kuerzere Antwort und
        // niemals eine gescheiterte Suche.
        return [];
    }
}

/**
 * Die Treffer einer Anfrage, geordnet, samt dem, was der Praefix-Cache braucht.
 *
 * `fanInOf` ist von aussen, weil die Suchzeile des Servers keine Fan-in-Spalte
 * traegt. Wer keine Zahl hat, gibt keine, und ein Kandidat ohne Zahl hat null:
 * das entscheidet nur bei Gleichstand.
 */
export async function searchByMeaning(
    searcher: SymbolSearcher,
    root: string,
    query: string,
    opts: MeaningOptions = {},
    fanInOf: (hit: SymbolSearchHit) => number = () => 0,
): Promise<MeaningAnswer> {
    const terms = queryTerms(query);
    if (terms.length === 0) {
        return { hits: [], candidates: [], complete: true, aborted: false };
    }
    const candidates: SymbolSearchHit[] = [];
    let complete = true;
    for (const term of terms) {
        if (opts.signal?.aborted === true) {
            return { hits: [], candidates, complete: false, aborted: true };
        }
        const round = await candidatesFor(searcher, root, term, opts);
        if (round.length >= CANDIDATES_PER_TERM) {
            complete = false;
        }
        candidates.push(...round);
    }
    if (opts.signal?.aborted === true) {
        return { hits: [], candidates, complete: false, aborted: true };
    }
    return { hits: rankHits(candidates, query, fanInOf), candidates, complete, aborted: false };
}

/** Nur die Treffer. Der Weg, den alles ausser der Kommandozeile geht. */
export async function findByMeaning(
    searcher: SymbolSearcher,
    root: string,
    query: string,
    opts: MeaningOptions = {},
    fanInOf: (hit: SymbolSearchHit) => number = () => 0,
): Promise<RankedHit[]> {
    return (await searchByMeaning(searcher, root, query, opts, fanInOf)).hits;
}
