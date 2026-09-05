/**
 * Parser fuer das kompakte Zeilenformat, das der C-Server im MCP-Text liefert.
 *
 * Der Server antwortet auf query_graph und Verwandte nicht mit JSON, sondern
 * mit einem Kopf, eingerueckten Datenzeilen und einem Fuss:
 *
 *     rows: 5  (cols: b.name r.line)
 *       validateUser "24"
 *     total: 5
 *
 * search_graph benutzt eine zweite, aehnliche Form mit `results:` statt
 * `rows:`. Beide werden getrennt geparst statt in einem Format vereinheitlicht,
 * weil sie verschiedene Dinge sagen: `total` in der Zeilenform ist die Zahl
 * der Treffer, `has_more` in der Suchform sagt zusaetzlich, ob der Server
 * abgeschnitten hat. Ein gemeinsamer Rueckgabetyp muesste eines der beiden
 * erfinden oder verschweigen.
 *
 * Der Parser rundet nichts ab: eine Zeile mit einer anderen Zellenzahl als der
 * Kopf ankuendigt ist ein Fehler und wird nicht mit Leerwerten aufgefuellt.
 */

/** Eine geparste Zeilenantwort. */
export interface CompactRows {
    /** Spaltennamen aus dem Kopf, zum Beispiel `b.name` und `r.line`. */
    columns: string[];
    /** Datenzeilen, Zellen in Spaltenreihenfolge und bereits entquotet. */
    rows: string[][];
    /** Der `total:`-Wert des Servers. Kann groesser sein als `rows.length`. */
    total: number;
    /** Der `hint:`-Text, den der Server bei leeren Ergebnissen mitschickt. */
    hint?: string;
}

/** Eine geparste search_graph-Antwort. */
export interface SearchResults {
    /** Der `total:`-Wert des Servers. */
    total: number;
    /** Der `search_mode:`-Wert, zum Beispiel `bm25`. */
    mode: string;
    /** Spaltennamen aus dem `results:`-Kopf. */
    columns: string[];
    /** Trefferzeilen, Zellen in Spaltenreihenfolge. */
    rows: string[][];
    /** True, wenn der Server abgeschnitten hat und mehr da waere. */
    hasMore: boolean;
}

const ROWS_HEAD = /^rows:\s*(\d+)\s*\(cols:\s*([^)]*)\)\s*$/;
const RESULTS_HEAD = /^results:\s*(\d+)\s*\(cols:\s*([^)]*)\)\s*$/;
const TOTAL_LINE = /^total:\s*(\d+)\s*$/;
const HINT_LINE = /^hint:\s*(.*)$/;
const MODE_LINE = /^search_mode:\s*(\S+)\s*$/;
const HAS_MORE_LINE = /^has_more:\s*(true|false)\s*$/;
const DATA_LINE = /^ {2}\S/;

/** Zerlegt eine Datenzeile in Zellen und nimmt Anfuehrungszeichen weg. */
export function splitCells(line: string): string[] {
    const cells: string[] = [];
    let i = 0;
    while (i < line.length) {
        while (i < line.length && (line[i] === ' ' || line[i] === '\t')) {
            i += 1;
        }
        if (i >= line.length) {
            break;
        }
        if (line[i] === '"') {
            i += 1;
            let buf = '';
            let closed = false;
            while (i < line.length) {
                const ch = line[i];
                if (ch === '\\' && i + 1 < line.length) {
                    buf += line[i + 1];
                    i += 2;
                    continue;
                }
                if (ch === '"') {
                    i += 1;
                    closed = true;
                    break;
                }
                buf += ch;
                i += 1;
            }
            if (!closed) {
                throw new Error(
                    `kompakte Zeile hat ein nicht geschlossenes Anfuehrungszeichen: ${line.trim()}`,
                );
            }
            cells.push(buf);
            continue;
        }
        const start = i;
        while (i < line.length && line[i] !== ' ' && line[i] !== '\t') {
            i += 1;
        }
        cells.push(line.slice(start, i));
    }
    return cells;
}

function unquote(value: string): string {
    const trimmed = value.trim();
    if (trimmed.length >= 2 && trimmed.startsWith('"') && trimmed.endsWith('"')) {
        return splitCells(trimmed)[0] ?? '';
    }
    return trimmed;
}

function readColumns(raw: string): string[] {
    const columns = raw.trim().split(/\s+/).filter((c) => c.length > 0);
    if (columns.length === 0) {
        throw new Error('kompakter Kopf nennt keine Spalten');
    }
    return columns;
}

function collectRows(
    lines: string[],
    from: number,
    columns: string[],
    label: string,
): { rows: string[][]; next: number } {
    const rows: string[][] = [];
    let i = from;
    while (i < lines.length && DATA_LINE.test(lines[i])) {
        const cells = splitCells(lines[i].slice(2));
        if (cells.length !== columns.length) {
            throw new Error(
                `${label}: Zeile ${i + 1} hat ${cells.length} Zellen, der Kopf nennt ` +
                    `${columns.length} Spalten (${columns.join(' ')}): ${lines[i].trim()}`,
            );
        }
        rows.push(cells);
        i += 1;
    }
    return { rows, next: i };
}

/**
 * Parst die Zeilenform (`rows:` / Datenzeilen / `total:` / optional `hint:`).
 *
 * Wirft, wenn der Kopf fehlt, eine Zeile nicht zur Spaltenzahl passt oder der
 * Kopf mehr oder weniger Zeilen ankuendigt, als dann kommen.
 */
export function parseCompactRows(text: string): CompactRows {
    const lines = text.split('\n');
    let i = 0;
    while (i < lines.length && lines[i].trim().length === 0) {
        i += 1;
    }
    if (i >= lines.length) {
        throw new Error('kompakte Antwort war leer');
    }

    const head = ROWS_HEAD.exec(lines[i]);
    if (head === null) {
        throw new Error(
            `unbekannter Kopf einer kompakten Antwort, erwartet "rows: N  (cols: ...)": ` +
                `${lines[i].trim()}`,
        );
    }
    const declared = Number.parseInt(head[1], 10);
    const columns = readColumns(head[2]);
    i += 1;

    const collected = collectRows(lines, i, columns, 'kompakte Antwort');
    const rows = collected.rows;
    i = collected.next;

    if (rows.length !== declared) {
        throw new Error(
            `kompakte Antwort kuendigt rows: ${declared} an, geliefert wurden ${rows.length}`,
        );
    }

    let total: number | undefined;
    let hint: string | undefined;
    for (; i < lines.length; i += 1) {
        const line = lines[i];
        if (line.trim().length === 0) {
            continue;
        }
        const totalMatch = TOTAL_LINE.exec(line);
        if (totalMatch !== null) {
            total = Number.parseInt(totalMatch[1], 10);
            continue;
        }
        const hintMatch = HINT_LINE.exec(line);
        if (hintMatch !== null) {
            hint = unquote(hintMatch[1]);
            continue;
        }
        throw new Error(`unbekannte Fusszeile einer kompakten Antwort: ${line.trim()}`);
    }

    if (total === undefined) {
        throw new Error('kompakte Antwort ohne total-Zeile');
    }

    const out: CompactRows = { columns, rows, total };
    if (hint !== undefined) {
        out.hint = hint;
    }
    return out;
}

/**
 * Parst die search_graph-Form (`total:` / `search_mode:` / `results:` /
 * Trefferzeilen / `has_more:`).
 */
export function parseSearchResults(text: string): SearchResults {
    const lines = text.split('\n');
    let total: number | undefined;
    let mode: string | undefined;
    let columns: string[] | undefined;
    let declared: number | undefined;
    let rows: string[][] | undefined;
    let hasMore: boolean | undefined;

    let i = 0;
    while (i < lines.length) {
        const line = lines[i];
        if (line.trim().length === 0) {
            i += 1;
            continue;
        }

        const totalMatch = TOTAL_LINE.exec(line);
        if (totalMatch !== null) {
            total = Number.parseInt(totalMatch[1], 10);
            i += 1;
            continue;
        }
        const modeMatch = MODE_LINE.exec(line);
        if (modeMatch !== null) {
            mode = modeMatch[1];
            i += 1;
            continue;
        }
        const hasMoreMatch = HAS_MORE_LINE.exec(line);
        if (hasMoreMatch !== null) {
            hasMore = hasMoreMatch[1] === 'true';
            i += 1;
            continue;
        }
        const head = RESULTS_HEAD.exec(line);
        if (head !== null) {
            declared = Number.parseInt(head[1], 10);
            columns = readColumns(head[2]);
            const collected = collectRows(lines, i + 1, columns, 'Suchantwort');
            rows = collected.rows;
            i = collected.next;
            continue;
        }
        throw new Error(`unbekannte Zeile in der Suchantwort: ${line.trim()}`);
    }

    if (columns === undefined || rows === undefined || declared === undefined) {
        throw new Error(
            'unbekannter Kopf einer Suchantwort, erwartet "results: N  (cols: ...)"',
        );
    }
    if (rows.length !== declared) {
        throw new Error(
            `Suchantwort kuendigt results: ${declared} an, geliefert wurden ${rows.length}`,
        );
    }
    if (total === undefined) {
        throw new Error('Suchantwort ohne total-Zeile');
    }
    if (mode === undefined) {
        throw new Error('Suchantwort ohne search_mode-Zeile');
    }
    if (hasMore === undefined) {
        throw new Error('Suchantwort ohne has_more-Zeile');
    }

    return { total, mode, columns, rows, hasMore };
}

/** Verbindet Spaltennamen und Zellen zu einem Objekt je Zeile. */
export function rowsAsObjects(parsed: CompactRows | SearchResults): Record<string, string>[] {
    return parsed.rows.map((cells) => {
        const record: Record<string, string> = {};
        parsed.columns.forEach((column, index) => {
            record[column] = cells[index];
        });
        return record;
    });
}
