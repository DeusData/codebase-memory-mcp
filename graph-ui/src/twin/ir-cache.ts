/**
 * Die IR eines Symbols holen, aber nicht zweimal dieselbe.
 *
 * Der Twin haengt am Caret, und ein Caret bewegt sich. Ohne diese Schicht waere
 * jede Bewegung innerhalb desselben Symbols eine neue Runde ueber sechs
 * Graph-Anfragen, und ein Leser, der mit den Pfeiltasten durch eine Funktion
 * geht, wuerde den Server mit Fragen bewerfen, deren Antwort er schon hat.
 *
 * Drei Regeln, und jede loest ein anderes Problem:
 *
 * 1. **Geschluessel wird ueber den qualifizierten Namen.** Nicht ueber Datei
 *    plus Zeile: der Caret wandert durch dreizehn Zeilen desselben Symbols, und
 *    dreizehn Eintraege fuer eine Antwort waeren ein Cache, der genau dann
 *    nichts trifft, wenn es darauf ankommt. Ein Symbol ohne qualifizierten Namen
 *    wird nicht abgelegt: der Schluessel waere leer, und ein leerer Schluessel
 *    liefert die Antwort fuer irgendein anderes namenloses Symbol aus.
 * 2. **Was gleichzeitig laeuft, laeuft einmal.** Zwei Caret-Bewegungen kurz
 *    hintereinander auf dasselbe Symbol ergeben eine Anfrage und zwei Warter.
 *    Ohne das waere die Entprellung nur eine Verzoegerung und kein Deckel.
 * 3. **Gezaehlt wird nur, was wirklich gefragt wurde.** `onFetch` feuert genau
 *    dann, wenn eine Anfrage an den Server gestellt wird. Ein Treffer im Cache
 *    zaehlt nicht, und ein Warter, der sich an eine laufende Anfrage haengt,
 *    zaehlt auch nicht. Der Beweislauf misst daran, ob ein Caret-Wechsel
 *    nachgeladen hat, und ein Zaehler, der Wartende mitzaehlt, wuerde genau
 *    diese Messung wertlos machen.
 *
 * Eine gescheiterte Anfrage wird nicht abgelegt. Ein Fehler ist eine Aussage
 * ueber diesen Moment (der Server war weg, die Antwort war unlesbar) und keine
 * Aussage ueber das Symbol; ihn zu behalten hiesse, den Twin fuer den Rest der
 * Sitzung auf "geht nicht" festzunageln.
 */

import type { SymbolRef } from '../core/focus-protocol';
import type { SemanticIR } from '../core/semantic-ir';

/**
 * Wie viele IRs behalten werden.
 *
 * Sechzehn, weil das die Groessenordnung einer Lesesitzung ist: ein Leser folgt
 * einer Kette von Aufrufen, geht zurueck, und trifft dabei dieselben fuenf bis
 * zehn Symbole wieder. Mehr zu behalten waere Speicher fuer Symbole, zu denen
 * niemand zurueckkehrt, und der Index kann sich zwischendurch geaendert haben.
 */
export const IR_CACHE_CAPACITY = 16;

/** Was eine geladene IR mitbringt: den Bau und die Saetze, die dabei anfielen. */
export interface IrEntry {
    ir: SemanticIR;
    /** Ein Satz je Faktenfamilie, die nicht beantwortet werden konnte. */
    warnings: string[];
}

/** Was der Cache ruft, wenn er wirklich fragen muss. */
export type IrFetcher = (symbol: SymbolRef) => Promise<IrEntry>;

/**
 * Der Schluessel eines Symbols, oder undefined, wenn es keinen tragfaehigen gibt.
 *
 * Ausgelagert, damit der Aufrufer dieselbe Frage stellen kann wie der Cache
 * ("ist das noch dasselbe Symbol?"), ohne die Regel ein zweites Mal zu
 * schreiben.
 */
export function irCacheKey(symbol: SymbolRef): string | undefined {
    const qualifiedName = symbol.qualifiedName;
    return qualifiedName !== undefined && qualifiedName.length > 0 ? qualifiedName : undefined;
}

/**
 * Ein LRU-Cache mit Zusammenlegung gleichzeitiger Anfragen.
 *
 * Absichtlich eine Klasse und kein Hook: der Twin ist React, der Beweis dieser
 * Regeln muss es nicht sein. Was hier drinsteht, ist ohne DOM pruefbar.
 */
export class IrCache {
    private readonly entries = new Map<string, IrEntry>();
    private readonly inFlight = new Map<string, Promise<IrEntry>>();

    constructor(
        private readonly fetcher: IrFetcher,
        /** Wird genau einmal je echter Serveranfrage gerufen. */
        private readonly onFetch: () => void = () => undefined,
        private readonly capacity: number = IR_CACHE_CAPACITY,
    ) {}

    /** Was schon dasteht, ohne zu fragen. */
    peek(symbol: SymbolRef): IrEntry | undefined {
        const key = irCacheKey(symbol);
        if (key === undefined) {
            return undefined;
        }
        const hit = this.entries.get(key);
        if (hit !== undefined) {
            // Neu einsortieren: `Map` haelt die Einfuegereihenfolge, also macht
            // Loeschen-und-Setzen aus ihr eine Nutzungsreihenfolge.
            this.entries.delete(key);
            this.entries.set(key, hit);
        }
        return hit;
    }

    /** Wie viele IRs gerade abgelegt sind. Fuer die Tests, nicht fuer die Anzeige. */
    get size(): number {
        return this.entries.size;
    }

    /** Die abgelegten Schluessel, aelteste Nutzung zuerst. */
    keys(): string[] {
        return [...this.entries.keys()];
    }

    /**
     * Die IR fuer ein Symbol: aus dem Cache, aus einer laufenden Anfrage, oder
     * aus einer neuen.
     */
    async load(symbol: SymbolRef): Promise<IrEntry> {
        const key = irCacheKey(symbol);
        if (key === undefined) {
            // Kein Schluessel heisst kein Cache und keine Zusammenlegung, aber
            // sehr wohl eine Antwort: ein Symbol ohne qualifizierten Namen ist
            // ungewoehnlich, nicht unbeantwortbar.
            this.onFetch();
            return this.fetcher(symbol);
        }
        const cached = this.peek(symbol);
        if (cached !== undefined) {
            return cached;
        }
        const running = this.inFlight.get(key);
        if (running !== undefined) {
            return running;
        }
        this.onFetch();
        const pending = this.fetcher(symbol)
            .then((entry) => {
                this.remember(key, entry);
                return entry;
            })
            .finally(() => {
                this.inFlight.delete(key);
            });
        this.inFlight.set(key, pending);
        return pending;
    }

    private remember(key: string, entry: IrEntry): void {
        this.entries.delete(key);
        this.entries.set(key, entry);
        while (this.entries.size > this.capacity) {
            const oldest = this.entries.keys().next();
            if (oldest.done === true) {
                return;
            }
            this.entries.delete(oldest.value);
        }
    }
}
