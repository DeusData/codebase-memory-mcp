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
    /**
     * Die Fusszeilen, die der Server seit dem schlanken Ausgabevertrag
     * (#1597) mitschickt: wie viele Zeilen geliefert wurden, ob `total` exakt
     * oder eine Untergrenze ist (`eq` / `gte`), ob mehr da waere und ob
     * gekappt wurde. Optional, weil die Aufzeichnungen von vor dem Vertrag
     * sie nicht tragen; wo sie da sind, sind sie die ehrlichere Auskunft.
     */
    returned?: number;
    totalRelation?: string;
    hasMore?: boolean;
    truncated?: boolean;
    offset?: number;
    nextOffset?: number;
    nextCursor?: string;
    truncationReason?: string;
    warning?: string;
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
    /** Die weiteren Fusszeilen des schlanken Vertrags (#1597), siehe CompactRows. */
    returned?: number;
    totalRelation?: string;
    truncated?: boolean;
}

const ROWS_HEAD = /^rows:\s*(\d+)\s*\(cols:\s*([^)]*)\)\s*$/;
const RESULTS_HEAD = /^results:\s*(\d+)\s*\(cols:\s*([^)]*)\)\s*$/;
const TOTAL_LINE = /^total:\s*(\d+)\s*$/;
const HINT_LINE = /^hint:\s*(.*)$/;
const MODE_LINE = /^search_mode:\s*(\S+)\s*$/;
const HAS_MORE_LINE = /^has_more:\s*(true|false)\s*$/;
const RETURNED_LINE = /^returned:\s*(\d+)\s*$/;
const TOTAL_RELATION_LINE = /^total_relation:\s*(\S+)\s*$/;
const TRUNCATED_LINE = /^truncated:\s*(true|false)\s*$/;
const DATA_LINE = /^ {2}\S/;

/** Zerlegt eine Datenzeile in Zellen und nimmt Anfuehrungszeichen weg. */
export function splitCells(line: string): string[] {
    return cellTokens(line).map(cell => cell.value);
}

/** Quoted @N+ values are literals; retain that distinction until expansion. */
function cellTokens(line: string): { value: string; quoted: boolean }[] {
    const cells: { value: string; quoted: boolean }[] = [];
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
            cells.push({ value: buf, quoted: true });
            continue;
        }
        const start = i;
        while (i < line.length && line[i] !== ' ' && line[i] !== '\t') {
            i += 1;
        }
        cells.push({ value: line.slice(start, i), quoted: false });
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
    refs?: ReadonlyMap<number, string>,
): { rows: string[][]; next: number } {
    const rows: string[][] = [];
    let i = from;
    while (i < lines.length && DATA_LINE.test(lines[i])) {
        const cells = cellTokens(lines[i].slice(2)).map(cell => {
            const ref = !cell.quoted && refs ? /^@(\d+)\+(.*)$/.exec(cell.value) : null;
            if (!ref) return cell.value;
            const prefix = refs!.get(Number(ref[1]));
            if (prefix === undefined) throw new Error(`${label}: unknown reference @${ref[1]}`);
            // Prefix entries are declarations, never recursively expanded.
            return prefix + ref[2];
        });
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

function readReferences(lines: string[], from: number, key: 'rows' | 'results'):
    { refs?: ReadonlyMap<number, string>; next: number } {
    const head = new RegExp(`^${key}_refs:\\s*(\\d+)\\s*\\(cols:\\s*id\\s+prefix\\)\\s*$`).exec(lines[from]);
    if (!head) return { next: from };
    const count = Number(head[1]);
    const collected = collectRows(lines, from + 1, ['id', 'prefix'], `${key}_refs`);
    if (!Number.isSafeInteger(count) || collected.rows.length !== count)
        throw new Error(`${key}_refs: declared count does not match reference rows`);
    const refs = new Map<number, string>();
    for (const [id, prefix] of collected.rows) {
        const number = Number(id);
        if (!/^\d+$/.test(id) || !Number.isSafeInteger(number) || refs.has(number))
            throw new Error(`${key}_refs: invalid or duplicate reference id ${id}`);
        refs.set(number, prefix);
    }
    if (lines[collected.next]?.trim() !== `${key}_ref_rule: @N+suffix=prefix+suffix`)
        throw new Error(`${key}_refs: missing or unsupported reference rule`);
    return { refs, next: collected.next + 1 };
}

function validateCounts(parsed: CompactRows | SearchResults): void {
    for (const value of [parsed.total, parsed.returned, 'offset' in parsed ? parsed.offset : undefined,
        'nextOffset' in parsed ? parsed.nextOffset : undefined]) {
        if (value !== undefined && (!Number.isSafeInteger(value) || value < 0))
            throw new Error('Invalid compact response count');
    }
    if (parsed.total < parsed.rows.length || (parsed.returned !== undefined && parsed.returned !== parsed.rows.length))
        throw new Error('Compact response counts contradict returned rows');
    if (parsed.totalRelation !== undefined && parsed.totalRelation !== 'eq' && parsed.totalRelation !== 'gte')
        throw new Error('Unknown total_relation');
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

    const directory = readReferences(lines, i, 'rows');
    i = directory.next;
    const head = ROWS_HEAD.exec(lines[i] ?? '');
    if (head === null) {
        throw new Error(
            `unbekannter Kopf einer kompakten Antwort, erwartet "rows: N  (cols: ...)": ` +
                `${lines[i].trim()}`,
        );
    }
    const declared = Number.parseInt(head[1], 10);
    const columns = readColumns(head[2]);
    i += 1;

    const collected = collectRows(lines, i, columns, 'kompakte Antwort', directory.refs);
    const rows = collected.rows;
    i = collected.next;

    if (rows.length !== declared) {
        throw new Error(
            `kompakte Antwort kuendigt rows: ${declared} an, geliefert wurden ${rows.length}`,
        );
    }

    let total: number | undefined;
    let hint: string | undefined;
    let returned: number | undefined;
    let totalRelation: string | undefined;
    let hasMore: boolean | undefined;
    let truncated: boolean | undefined;
    const metadata: Pick<CompactRows, 'offset' | 'nextOffset' | 'nextCursor' | 'truncationReason' | 'warning'> = {};
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
        const returnedMatch = RETURNED_LINE.exec(line);
        if (returnedMatch !== null) {
            returned = Number.parseInt(returnedMatch[1], 10);
            continue;
        }
        const relationMatch = TOTAL_RELATION_LINE.exec(line);
        if (relationMatch !== null) {
            totalRelation = relationMatch[1];
            continue;
        }
        const hasMoreMatch = HAS_MORE_LINE.exec(line);
        if (hasMoreMatch !== null) {
            hasMore = hasMoreMatch[1] === 'true';
            continue;
        }
        const truncatedMatch = TRUNCATED_LINE.exec(line);
        if (truncatedMatch !== null) {
            truncated = truncatedMatch[1] === 'true';
            continue;
        }
        const extra = /^(offset|next_offset|next_cursor|truncation_reason|warning):\s*(.*)$/.exec(line);
        if (extra) {
            const value = unquote(extra[2]);
            if (extra[1] === 'offset' || extra[1] === 'next_offset') {
                if (!/^\d+$/.test(value)) throw new Error(`Invalid ${extra[1]}`);
                metadata[extra[1] === 'offset' ? 'offset' : 'nextOffset'] = Number(value);
            } else if (extra[1] === 'next_cursor') metadata.nextCursor = value;
            else if (extra[1] === 'truncation_reason') metadata.truncationReason = value;
            else metadata.warning = value;
            continue;
        }
        throw new Error(`unbekannte Fusszeile einer kompakten Antwort: ${line.trim()}`);
    }

    if (total === undefined) {
        throw new Error('kompakte Antwort ohne total-Zeile');
    }

    const out: CompactRows = { columns, rows, total, ...metadata };
    if (hint !== undefined) {
        out.hint = hint;
    }
    if (returned !== undefined) {
        out.returned = returned;
    }
    if (totalRelation !== undefined) {
        out.totalRelation = totalRelation;
    }
    if (hasMore !== undefined) {
        out.hasMore = hasMore;
    }
    if (truncated !== undefined) {
        out.truncated = truncated;
    }
    validateCounts(out);
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
    let returned: number | undefined;
    let totalRelation: string | undefined;
    let truncated: boolean | undefined;
    let refs: ReadonlyMap<number, string> | undefined;

    let i = 0;
    while (i < lines.length) {
        const line = lines[i];
        if (line.trim().length === 0) {
            i += 1;
            continue;
        }
        if (line.startsWith('results_refs:')) {
            if (refs || rows) throw new Error('Duplicate or misplaced results_refs');
            const directory = readReferences(lines, i, 'results');
            if (!directory.refs) throw new Error('Malformed results_refs');
            refs = directory.refs;
            i = directory.next;
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
        const returnedMatch = RETURNED_LINE.exec(line);
        if (returnedMatch !== null) {
            returned = Number.parseInt(returnedMatch[1], 10);
            i += 1;
            continue;
        }
        const relationMatch = TOTAL_RELATION_LINE.exec(line);
        if (relationMatch !== null) {
            totalRelation = relationMatch[1];
            i += 1;
            continue;
        }
        const truncatedMatch = TRUNCATED_LINE.exec(line);
        if (truncatedMatch !== null) {
            truncated = truncatedMatch[1] === 'true';
            i += 1;
            continue;
        }
        const head = RESULTS_HEAD.exec(line);
        if (head !== null) {
            declared = Number.parseInt(head[1], 10);
            columns = readColumns(head[2]);
            const collected = collectRows(lines, i + 1, columns, 'Suchantwort', refs);
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

    const out: SearchResults = { total, mode, columns, rows, hasMore };
    if (returned !== undefined) {
        out.returned = returned;
    }
    if (totalRelation !== undefined) {
        out.totalRelation = totalRelation;
    }
    if (truncated !== undefined) {
        out.truncated = truncated;
    }
    validateCounts(out);
    return out;
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
