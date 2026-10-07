/**
 * The operations the Behavior "Start" field offers (hand test 2026-10-04,
 * A4). For django-demo that were 62 entries in the order the server sent
 * them, with look-alikes ("database_backwards · …/models.py" six times), and
 * "handle · …/loaddata.py" could not be found. Now:
 *
 *  - the automatic start and the ranked suggestions come first, in a group
 *    of their own and in their order;
 *  - then every operation, alphabetically by name and then by path;
 *  - look-alikes carry their class ("database_backwards (CreateModel) · …",
 *    the name first so the list still reads alphabetically), their line when
 *    the class is the same too, and their position when the index names
 *    neither;
 *  - a filter keeps the operations whose name, class or path contain every
 *    word typed. The chosen one stays offered so the field can still show it,
 *    and when it does not match it stands apart (`current`): among the matches
 *    it made "5 of 61" a group of six (review of K43).
 */
import type { SystemSymbol } from './system-architecture-source';
import { architectureText } from './strings';

const text = architectureText.behaviorStart;

export interface OperationChoice { id: number; label: string; symbol: SystemSymbol }
export interface OperationChoices {
    /** The chosen start while a filter it does not match is set; it is in neither list then. */
    current?: OperationChoice;
    suggested: OperationChoice[];
    all: OperationChoice[];
    /** How many operations there are, and how many of them match the filter (the kept one only when it matches). */
    total: number;
    matching: number;
}

const place = (symbol: SystemSymbol) => symbol.file_path ?? symbol.qualified_name;
const lookAlike = (symbol: SystemSymbol) => `${symbol.name}\u0000${place(symbol) ?? ''}`;

/** The segment before the name in the qualified name: the class of a method, the module of a function. */
export function operationQualifier(symbol: SystemSymbol): string | undefined {
    const name = symbol.qualified_name;
    if (!name || !name.endsWith(`.${symbol.name}`)) return undefined;
    return name.slice(0, -symbol.name.length - 1).split('.').at(-1) || undefined;
}

const collator = new Intl.Collator('en', { sensitivity: 'base', numeric: true });
function compare(a: SystemSymbol, b: SystemSymbol): number {
    return collator.compare(a.name, b.name) || collator.compare(place(a) ?? '', place(b) ?? '')
        || collator.compare(operationQualifier(a) ?? '', operationQualifier(b) ?? '') || (a.start_line ?? 0) - (b.start_line ?? 0) || a.id - b.id;
}

/** One label per operation, the same with or without a filter, so the field never renames what it shows. */
export function operationLabels(entries: SystemSymbol[]): Map<number, string> {
    const groups = new Map<string, SystemSymbol[]>();
    for (const entry of entries) groups.set(lookAlike(entry), [...groups.get(lookAlike(entry)) ?? [], entry]);
    const labels = new Map<number, string>();
    for (const group of groups.values()) {
        const ordered = [...group].sort((a, b) => a.id - b.id);
        const qualifiers = ordered.map(operationQualifier);
        const distinct = (values: unknown[]) => values.every(value => value !== undefined) && new Set(values).size === values.length;
        const byQualifier = group.length > 1 && distinct(qualifiers);
        const byLine = group.length > 1 && !byQualifier && distinct(ordered.map(item => item.start_line));
        ordered.forEach((entry, index) => {
            const where = place(entry) ?? '';
            labels.set(entry.id, group.length === 1 ? text.option(entry.name, where)
                : byQualifier ? text.option(text.qualified(qualifiers[index]!, entry.name), where)
                    : byLine ? text.option(entry.name, text.line(where, entry.start_line!))
                        : `${text.option(entry.name, where)}${text.ordinal(index + 1, group.length)}`);
        });
    }
    return labels;
}

export function operationChoices(entries: SystemSymbol[], { suggested = [], query = '', keep }: { suggested?: number[]; query?: string; keep?: number }): OperationChoices {
    const labels = operationLabels(entries);
    const words = query.toLocaleLowerCase().split(/\s+/).filter(Boolean);
    const matches = (entry: SystemSymbol) => words.every(word => `${labels.get(entry.id)} ${entry.qualified_name}`.toLocaleLowerCase().includes(word));
    const choice = (symbol: SystemSymbol): OperationChoice => ({ id: symbol.id, label: labels.get(symbol.id)!, symbol });
    const byId = new Map(entries.map(entry => [entry.id, entry]));
    const offered = [...new Set(suggested)].filter(id => byId.has(id));
    const kept = keep === undefined ? undefined : byId.get(keep);
    return {
        ...(kept && !matches(kept) ? { current: choice(kept) } : {}),
        // Suggestions that name every operation would only repeat the list.
        suggested: offered.length >= entries.length ? [] : offered.flatMap(id => matches(byId.get(id)!) ? [choice(byId.get(id)!)] : []),
        all: entries.filter(matches).sort(compare).map(choice),
        total: entries.length,
        matching: entries.filter(matches).length,
    };
}

/** What `nameOperations` needs of the RPC client. */
export interface OperationNameClient { queryRows: (project: string, query: string) => Promise<Record<string, string>[]> }

/* Only plain identifiers and paths enter the query text; anything with a quote or a backslash stays unnamed. */
const PLAIN = /^[\w$<>@.\/\- ]+$/;

/**
 * Ranked flow starts carry no qualified name or line (/api/flows names only
 * the node, its file and its id). For the look-alikes among them the index is
 * asked once, by name and file, and the answer is matched by node id. Without
 * an answer the list stays as it was: the position still tells them apart.
 */
export async function nameOperations(project: string, entries: SystemSymbol[], client: OperationNameClient): Promise<SystemSymbol[]> {
    const groups = new Map<string, SystemSymbol[]>();
    for (const entry of entries) groups.set(lookAlike(entry), [...groups.get(lookAlike(entry)) ?? [], entry]);
    const unnamed = [...groups.values()].filter(group => group.length > 1).flat()
        .filter(entry => !entry.qualified_name && entry.file_path && PLAIN.test(entry.name) && PLAIN.test(entry.file_path));
    if (!unnamed.length) return entries;
    const list = (values: string[]) => `[${[...new Set(values)].map(value => `"${value}"`).join(', ')}]`;
    const query = `MATCH (n) WHERE n.name IN ${list(unnamed.map(entry => entry.name))} AND n.file_path IN ${list(unnamed.map(entry => entry.file_path!))} `
        + 'RETURN id(n) AS id, n.qualified_name AS qualified_name, n.start_line AS start_line LIMIT 1000';
    let rows: Record<string, string>[];
    try { rows = await client.queryRows(project, query); } catch { return entries; }
    const found = new Map(rows.map(row => [Number(row.id), row]));
    const wanted = new Set(unnamed.map(entry => entry.id));
    return entries.map(entry => {
        const row = wanted.has(entry.id) ? found.get(entry.id) : undefined;
        if (!row?.qualified_name) return entry;
        const line = Number(row.start_line);
        return { ...entry, qualified_name: row.qualified_name, ...(Number.isSafeInteger(line) && line > 0 ? { start_line: line } : {}) };
    });
}
