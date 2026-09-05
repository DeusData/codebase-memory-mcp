/**
 * Was ein Ereignis ist, und welche Art von Arbeit es zeigt.
 *
 * Die Ereignisse kommen von der Bruecke (tools/agent-bridge.mjs) und damit aus
 * einer Datei, die ein Hook geschrieben hat. Dieses Modul ist die einzige
 * Stelle, die aus einer fremden JSON-Zeile ein Ereignis dieses Produkts macht,
 * und es hat dabei genau zwei Aufgaben.
 *
 * ## 1. Nachsichtig lesen, ohne zu erfinden
 *
 * Eine Zeile, der ein Pflichtfeld fehlt, wird weggelassen. Eine Zeile mit
 * Feldern, die dieses Produkt nicht kennt, wird genommen und die Felder werden
 * uebergangen: eine Quelle, die mehr weiss, soll nicht daran scheitern, dass
 * sie es sagt. Was NICHT passiert, ist Ergaenzen: kein erfundener Pfad, kein
 * geratener Zeilenbereich, keine Absicht, die niemand geschrieben hat.
 *
 * ## 2. Die Art der Arbeit ablesen, nicht deuten
 *
 * Vier Arten stehen im Contract (lesen, schreiben, suchen, testen), und sie
 * werden am WERKZEUGNAMEN abgelesen. Ein Werkzeug, das keiner der vier
 * zugeordnet ist, bekommt `other` und sagt das auch so: "other" heisst hier
 * "der Werkzeugname sagt nicht, welche der vier es ist" und nicht "sonstiges".
 *
 * Die einzige Ausnahme ist die Kommandozeile. `Bash` ist kein Hinweis auf
 * irgendetwas, denn darin steckt alles: ein `ls`, ein Testlauf, ein Bau. Fuer
 * genau einen Fall ist der Befehl aussagekraeftig genug, und das ist der
 * Testlauf: `vitest`, `pytest`, `node --test` und ihre Geschwister sind
 * Programme, die nur eines tun. Alles andere an der Kommandozeile bleibt
 * `other`, weil eine feinere Deutung des Befehls Raten waere.
 */

/** Die Art der Arbeit, so wie das Instrument sie zeigt. */
export type WorkKind = 'read' | 'write' | 'search' | 'test' | 'other';

/** Ein Ereignis, wie dieses Produkt es fuehrt. */
export interface AgentEvent {
    /** Millisekunden seit 1970. */
    ts: number;
    /** Anzeigename des Agenten. */
    agent: string;
    /** Kennung des Laufs. Zusammen mit `seq` die Identitaet des Ereignisses. */
    run: string;
    /** Fortlaufend je Lauf. Eine Luecke darin ist eine gemeldete Luecke. */
    seq: number;
    phase: 'start' | 'end';
    tool: string;
    /** Repo-relativer Pfad. Leer heisst: die Zeile nennt keinen. */
    path: string;
    /** Der beruehrte Bereich, wenn die Quelle ihn kennt. */
    lines?: readonly [number, number];
    /** Der Befehl oder das Suchmuster. Leer heisst: die Zeile nennt keinen. */
    detail: string;
    /**
     * Was der Agent SELBST ueber seine Absicht gesagt hat.
     *
     * Optional, und das ist der ganze Punkt: fehlt es, gibt es keine
     * Absichtszeile. Steht es da, ist es eine Selbstauskunft und wird als
     * solche gekennzeichnet, nicht als Messung.
     */
    intent?: string;
    /** Woher das Ereignis kommt, wenn die Quelle es sagt (etwa `fs`). */
    source: string;
    /** Ob die Bruecke dieses Ereignis als Wiedergabe gekennzeichnet hat. */
    replay: boolean;
    /** Die aufgezeichnete Zeit, wenn die Wiedergabe sie verschoben hat. */
    recordedTs?: number;
}

/**
 * Die Werkzeuge, die lesen.
 *
 * `Open` steht darin, weil die eigene Navigation des Lesers so heisst: ein
 * geoeffnetes Symbol ist ein gelesenes Symbol, und eine eigene fuenfte Art
 * dafuer waere eine Art von Arbeit, die es nur fuer einen einzigen Akteur gibt.
 */
export const READ_TOOLS: readonly string[] = ['Read', 'ReadFile', 'NotebookRead', 'View', 'Open'];

/** Die Werkzeuge, die schreiben. */
export const WRITE_TOOLS: readonly string[] = [
    'Edit', 'MultiEdit', 'Write', 'NotebookEdit', 'Patch', 'Apply',
];

/** Die Werkzeuge, die suchen. */
export const SEARCH_TOOLS: readonly string[] = [
    'Grep', 'Glob', 'Search', 'ToolSearch', 'WebSearch', 'Find',
];

/**
 * Die Programme, an denen ein Testlauf zu erkennen ist.
 *
 * Eine Liste und kein Muster ueber "test", weil das Wort in einem Pfad steht,
 * ohne dass ein Test laeuft (`cat test/userService.test.ts` liest eine Datei).
 * Gesucht wird der AUFRUF, also das Programm am Anfang oder hinter einem
 * Laeufer wie `npx` oder `npm run`.
 */
export const TEST_COMMANDS: readonly RegExp[] = [
    /\bvitest\b/,
    /\bjest\b/,
    /\bpytest\b/,
    /\bmocha\b/,
    /\bnode\s+--test\b/,
    /\bnpm\s+(run\s+)?test\b/,
    /\byarn\s+test\b/,
    /\bpnpm\s+(run\s+)?test\b/,
    /\bgo\s+test\b/,
    /\bcargo\s+test\b/,
    /\bctest\b/,
];

/** Der Buchstabe einer Art. Er steht im Instrument neben dem Namen. */
export const WORK_KIND_LETTERS: Readonly<Record<WorkKind, string>> = {
    read: 'R',
    write: 'W',
    search: 'S',
    test: 'T',
    other: 'O',
};

/** Ob dieser Befehl einen Testlauf startet. */
export function looksLikeTestRun(command: string): boolean {
    return TEST_COMMANDS.some((pattern) => pattern.test(command));
}

/** Die Art der Arbeit, abgelesen am Werkzeugnamen und (nur fuer Tests) am Befehl. */
export function workKindOf(tool: string, detail = ''): WorkKind {
    if (READ_TOOLS.includes(tool)) {
        return 'read';
    }
    if (WRITE_TOOLS.includes(tool)) {
        return 'write';
    }
    if (SEARCH_TOOLS.includes(tool)) {
        return 'search';
    }
    if (looksLikeTestRun(detail)) {
        return 'test';
    }
    return 'other';
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function textOf(value: unknown): string {
    return typeof value === 'string' ? value : '';
}

function numberOf(value: unknown): number | undefined {
    if (typeof value === 'number' && Number.isFinite(value)) {
        return value;
    }
    if (typeof value === 'string' && value.trim().length > 0) {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : undefined;
    }
    return undefined;
}

/**
 * Der Zeilenbereich, wenn die Quelle einen nennt.
 *
 * Zwei Zahlen, aufsteigend sortiert. Alles andere (eine Zahl, drei Zahlen,
 * Text) ergibt nichts: ein halber Bereich waere eine Zuordnung, die genauer
 * aussieht, als sie ist.
 */
export function readLineSpan(value: unknown): readonly [number, number] | undefined {
    if (!Array.isArray(value) || value.length !== 2) {
        return undefined;
    }
    const from = numberOf(value[0]);
    const to = numberOf(value[1]);
    if (from === undefined || to === undefined || from < 1 || to < 1) {
        return undefined;
    }
    return from <= to ? [from, to] : [to, from];
}

/**
 * Eine Zeile der Bruecke als Ereignis, oder nichts.
 *
 * Nichts heisst: `ts`, `agent`, `run` oder `tool` fehlt. Ohne die vier ist ein
 * Ereignis weder einzuordnen noch zuzuordnen, und ein Platzhalter dafuer waere
 * ein Koerper auf dem Graphen, der niemandem gehoert.
 */
export function readAgentEvent(raw: unknown): AgentEvent | undefined {
    if (!isRecord(raw)) {
        return undefined;
    }
    const ts = numberOf(raw['ts']);
    const agent = textOf(raw['agent']);
    const run = textOf(raw['run']);
    const tool = textOf(raw['tool']);
    if (ts === undefined || agent.length === 0 || run.length === 0 || tool.length === 0) {
        return undefined;
    }
    const span = readLineSpan(raw['lines']);
    const intent = textOf(raw['intent']);
    const recorded = numberOf(raw['ts_recorded']);
    return {
        ts,
        agent,
        run,
        seq: numberOf(raw['seq']) ?? 0,
        phase: raw['phase'] === 'start' ? 'start' : 'end',
        tool,
        path: textOf(raw['path']),
        ...(span === undefined ? {} : { lines: span }),
        detail: textOf(raw['detail']),
        ...(intent.length === 0 ? {} : { intent }),
        source: textOf(raw['source']),
        replay: raw['replay'] === true,
        ...(recorded === undefined ? {} : { recordedTs: recorded }),
    };
}

/** Die Identitaet eines Ereignisses: sein Lauf und seine Nummer. */
export function eventKey(event: AgentEvent): string {
    return `${event.run}:${event.seq}`;
}

/**
 * Die Wiederaufnahme-Angabe fuer die Bruecke.
 *
 * `lauf:nummer`, durch Kommas getrennt, in der Ordnung der Laufkennungen, damit
 * zwei Aufrufe mit demselben Wissen dieselbe Zeichenkette ergeben.
 */
export function sinceParameter(seen: ReadonlyMap<string, number>): string {
    return [...seen.entries()]
        .sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))
        .map(([run, seq]) => `${run}:${seq}`)
        .join(',');
}
