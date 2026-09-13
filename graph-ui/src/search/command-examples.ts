/**
 * Was die Kommandozeile ueber sich selbst zeigt, und woher die Namen darin
 * kommen.
 *
 * ## Der Befund
 *
 * Nutzerbefund vom 2026-08-29, woertlich: "anstelle von 'type a command or ask
 * the atlas' explizite Beispiele, wie man den Chat nutzt, sonst weiss niemand,
 * wie man es nutzt."
 *
 * Der alte Platzhalter nannte eine GATTUNG ("ein Kommando oder eine Frage") und
 * kein Beispiel. Er ist damit die schriftliche Fassung von "hier kann man
 * etwas tun", was ein leeres Feld ohnehin schon sagt. Die Zeile hat zwei Jobs
 * (suchen und fragen), und beide sind an ihrer Form zu erkennen, sobald man sie
 * einmal gesehen hat: zwei Buchstaben suchen, ein `@` bindet die Frage an ein
 * Symbol, ein Fragezeichen macht aus einer Eingabe eine Frage. Das ist genau
 * das, was ein Beispiel zeigt und eine Gattungsbezeichnung nicht.
 *
 * ## Warum die Namen aus dem geladenen Index kommen muessen
 *
 * Ein Beispiel mit einem Symbol, das dieses Projekt nicht hat, waere eine
 * Einladung ins Leere: der Leser tippt es ab, bekommt nichts, und lernt daraus
 * das Falsche ueber die Zeile. Die Namen kommen darum aus dem, was der Browser
 * schon geladen hat (den Knoten des Layouts, siehe src/search/local-suggestions.ts),
 * und wenn nichts geladen ist, gibt es keine Beispiele: eine erfundene Liste
 * waere schlechter als keine.
 *
 * ## Die Wahl ist ordinal und damit wiederholbar
 *
 * Nicht "der erste Knoten der Antwort": die Reihenfolge des Layouts ist die des
 * Servers und kann sich zwischen zwei Laeufen aendern. Sortiert wird nach dem
 * Namen, und bevorzugt wird ein Symbol, das aufrufbar ist (eine Funktion oder
 * eine Methode), weil die beiden Fragen darunter von Aufrufen handeln. Ein
 * Beweislauf, der die Beispiele gegen den Index haelt, bekommt so bei jedem
 * Lauf dieselben.
 */

import type { CodeAtlasSymbolKind } from '../core/focus-protocol';

/** Wie viele Zeichen die Suche braucht, bevor sie antwortet. Dieselbe Zahl wie in find-by-meaning.ts. */
export const EXAMPLE_SEARCH_LETTERS = 2;

/** Ein Beispiel, wie die Zeile es anbietet. */
export interface CommandExample {
    /** Womit der Beweislauf und React es unterscheiden. */
    id: 'search' | 'at' | 'question';
    /** Was in die Zeile geschrieben wird, wenn man es anklickt. */
    text: string;
    /** Was daneben steht: welchen der beiden Jobs dieses Beispiel zeigt. */
    note: string;
    /** Der Name aus dem Index, an dem dieses Beispiel haengt. */
    symbol: string;
}

/** Ein Kandidat, so viel wie diese Datei davon braucht. */
export interface ExampleSymbol {
    name: string;
    kind: CodeAtlasSymbolKind;
}

/**
 * Die Arten, die eine Frage nach Aufrufen sinnvoll machen, in der Reihenfolge
 * ihrer Eignung.
 *
 * Eine Klasse ruft nichts, eine Datei erst recht nicht. Steht keine der drei im
 * Geladenen, wird trotzdem gewaehlt: ein Beispiel mit einem echten Namen dieses
 * Projekts ist besser als keins, und die Frage bleibt beantwortbar.
 */
const CALLABLE_KINDS: readonly CodeAtlasSymbolKind[] = ['function', 'method'];

/**
 * Namen, die im Index stehen und trotzdem kein gutes Beispiel abgeben.
 *
 * `constructor` ist der Fall, an dem es aufgefallen ist: der Index kennt ihn
 * (jede Klasse dieses Fixtures hat einen), er ist ordinal frueh, und
 * "Who calls constructor?" ist eine Frage, deren Antwort ein Leser nicht
 * gebrauchen kann. Ein Beispiel soll zeigen, wie die Zeile benutzt wird, und
 * dazu gehoert ein Name, der auf EIN Ding zeigt.
 *
 * Als benannte Liste mit Grund und nicht als Bedingung im Code, weil sie eine
 * Aussage ueber diese Sprache ist und nicht ueber diese Funktion.
 */
export const NOT_AN_EXAMPLE: readonly string[] = ['constructor', 'default', 'anonymous', 'module'];

/**
 * Das Symbol, an dem die Beispiele haengen.
 *
 * Ordinal sortiert und mit Vorzug fuer etwas Aufrufbares. `undefined`, wenn
 * nichts geladen ist: dann gibt es keine Beispiele.
 */
export function exampleSymbolOf(symbols: readonly ExampleSymbol[]): string | undefined {
    const usable = symbols
        .filter((symbol) => /^[A-Za-z_][A-Za-z0-9_]*$/.test(symbol.name))
        .filter((symbol) => symbol.name.length >= EXAMPLE_SEARCH_LETTERS)
        .filter((symbol) => !NOT_AN_EXAMPLE.includes(symbol.name))
        .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    if (usable.length === 0) {
        return undefined;
    }
    const callable = usable.find((symbol) => CALLABLE_KINDS.includes(symbol.kind));
    return (callable ?? usable[0]).name;
}

/**
 * Die drei Beispiele zu einem Namen.
 *
 * Beide Jobs der Zeile, in der Reihenfolge, in der ein Leser sie braucht:
 * erst finden, dann fragen, dann fragen ohne `@`. Das dritte ist dabei kein
 * Duplikat des zweiten: `@name` bindet die Frage an ein Symbol, ein blosser
 * Satz mit Fragezeichen laesst den Atlas das Subjekt selbst aufloesen, und
 * genau dieser Unterschied ist die Sache, die man an einem Beispiel lernt.
 */
export function commandExamplesFor(symbol: string): CommandExample[] {
    return [
        {
            id: 'search',
            text: symbol.slice(0, EXAMPLE_SEARCH_LETTERS),
            note: 'find by meaning',
            symbol,
        },
        {
            id: 'at',
            text: `@${symbol} what does it do?`,
            note: 'ask about one symbol',
            symbol,
        },
        {
            id: 'question',
            text: `Who calls ${symbol}?`,
            note: 'ask the atlas',
            symbol,
        },
    ];
}

/**
 * Der Platzhalter der Zeile: ein echtes Beispiel statt einer Gattung.
 *
 * Ohne geladenen Index bleibt die Gattung stehen, und das ist der ehrliche
 * Rueckfall: ein Platzhalter mit einem erfundenen Namen wuerde behaupten, es
 * gebe dieses Symbol.
 */
export function commandPlaceholderFor(symbol: string | undefined, fallback: string): string {
    return symbol === undefined ? fallback : `@${symbol} what does it do?`;
}

/** Was ueber der Liste steht. Eine Zeile, weil die Beispiele fuer sich sprechen. */
export const COMMAND_EXAMPLES_LABEL = 'this line does two things:';
