import type { BrowserChatReaderContext } from './chat-model';
import { fileKind, isDataFile, isGithubWorkflow } from './file-kind';
import { fileWords as words } from './strings';
import { workflowFactLines, yamlOutline } from './workflow-facts';

/** A configuration or text file as facts read from its text (K12). The automatic Explore card
 * of such a file is these facts and nothing else: a small model wrote "allows us to interact
 * with GitHub repositories from the command line" and "a simple shell script that greets"
 * next to the counted facts of a workflow. Each kind is read as far as its plain layout goes,
 * without a parser library; what cannot be read is left out, never guessed. */
export { isDataFile } from './file-kind';

const LISTED = 12;
const CHILDREN = 6;
const quote = (value: string) => `\`${value.replace(/`/g, "'").replace(/\s+/g, ' ').trim().slice(0, 120)}\``;
const extension = (path: string) => (path.split('/').pop() ?? path).split('.').slice(1).pop()?.toLowerCase() ?? '';
const lineCount = (text: string) => text ? text.split(/\r?\n/).length - (/\r?\n$/.test(text) ? 1 : 0) : 0;
const rows = (text: string) => text.split(/\r?\n/);

function yamlFacts(text: string): string[] {
    const outline = yamlOutline(text);
    if (!outline.length) return [];
    return [words.topKeys(outline.length, outline.slice(0, LISTED).map(item => `${quote(item.key)}${item.keys.length
        ? ` (${words.children(item.keys.length, item.keys.slice(0, CHILDREN).map(quote))})`
        : item.items ? ` (${words.list(item.items)}${item.itemKeys.length ? `; ${words.itemKeys(item.itemKeys.length, item.itemKeys.slice(0, CHILDREN).map(quote))}` : ''})` : ''}`))];
}

function jsonValue(value: unknown): string {
    if (Array.isArray(value)) return words.list(value.length);
    if (value !== null && typeof value === 'object') { const keys = Object.keys(value); return words.children(keys.length, keys.slice(0, CHILDREN).map(quote)); }
    return typeof value === 'string' ? words.value.text : typeof value === 'number' ? words.value.number : typeof value === 'boolean' ? words.value.boolean : words.value.empty;
}
function jsonFacts(text: string): string[] {
    let data: unknown;
    try { data = JSON.parse(text); } catch { return [words.invalidJson]; }
    if (Array.isArray(data)) return [words.items(data.length)];
    if (data === null || typeof data !== 'object') return [];
    const entries = Object.entries(data);
    return [words.topKeys(entries.length, entries.slice(0, LISTED).map(([key, value]) => `${quote(key)} (${jsonValue(value)})`))];
}

/** TOML: the keys before the first table and each table with its number of keys. */
function tomlFacts(text: string): string[] {
    const top: string[] = [];
    const tables: { name: string; keys: number }[] = [];
    let quoted: string | undefined;
    for (const raw of rows(text)) {
        const line = raw.trim();
        // A multi-line string may hold lines that look like keys.
        if (quoted) { if (line.split(quoted).length % 2 === 0) quoted = undefined; continue; }
        if (!line || line.startsWith('#')) continue;
        const table = /^(\[\[?)\s*([^\]]+?)\s*\]\]?\s*(?:#.*)?$/.exec(line);
        if (table) { tables.push({ name: `${table[1]}${table[2]}${table[1] === '[[' ? ']]' : ']'}`, keys: 0 }); continue; }
        const key = /^([A-Za-z0-9_\-."']+)\s*=/.exec(line);
        if (!key) continue;
        const delimiter = ['"""', "'''"].find(mark => line.split(mark).length === 2);
        if (delimiter) quoted = delimiter;
        if (tables.length) tables[tables.length - 1].keys += 1; else top.push(key[1]);
    }
    return [...top.length ? [words.topKeys(top.length, top.slice(0, LISTED).map(quote))] : [],
        ...tables.length ? [words.tables(tables.length, tables.slice(0, LISTED).map(table => `${quote(table.name)} (${words.keyCount(table.keys)})`))] : []];
}

/** One key of an INI file with its value; an indented line below it continues the value. */
export interface IniEntry { key: string; values: string[] }
/** INI and its relatives as they are written: the keys before the first section, then each
 * section with its keys. The automatic card counts them, the outline lists them (B6). */
export function iniSections(text: string): { loose: IniEntry[]; sections: { name: string; entries: IniEntry[] }[] } {
    const sections: { name: string; entries: IniEntry[] }[] = [];
    const loose: IniEntry[] = [];
    let last: IniEntry | undefined;
    for (const raw of rows(text)) {
        const line = raw.trim();
        if (!line || line.startsWith('#') || line.startsWith(';')) continue;
        const section = /^\[([^\]]+)\]$/.exec(line);
        if (section) { sections.push({ name: `[${section[1].trim()}]`, entries: [] }); last = undefined; continue; }
        if (/^\s/.test(raw)) { last?.values.push(line); continue; }
        const key = /^(?:export\s+)?([^=:\s][^=:]*?)\s*[=:]\s*(.*)$/.exec(line);
        last = key ? { key: key[1], values: key[2] ? [key[2]] : [] } : undefined;
        if (last) (sections.length ? sections[sections.length - 1].entries : loose).push(last);
    }
    return { loose, sections };
}

/** INI and its relatives: sections with their number of keys, or the keys of a file without sections. */
function iniFacts(text: string): string[] {
    const { loose, sections } = iniSections(text);
    if (sections.length) return [words.sections(sections.length, sections.slice(0, LISTED).map(section => `${quote(section.name)} (${words.keyCount(section.entries.length)})`))];
    return loose.length ? [words.keys(loose.length, loose.slice(0, LISTED).map(entry => quote(entry.key)))] : [];
}

/** Markdown: the title, the second-level sections and the code blocks; lines inside a code block are code. */
function markdownFacts(text: string): string[] {
    let title: string | undefined, fence: string | undefined, blocks = 0;
    const sections: string[] = [];
    const lines = rows(text);
    lines.forEach((raw, index) => {
        const mark = /^\s{0,3}(`{3,}|~{3,})/.exec(raw);
        if (mark) {
            if (!fence) { fence = mark[1][0]; blocks += 1; } else if (mark[1][0] === fence) fence = undefined;
            return;
        }
        if (fence) return;
        const heading = /^\s{0,3}(#{1,6})\s+(.+?)\s*#*\s*$/.exec(raw);
        const underline = /^\s{0,3}(=+|-+)\s*$/.exec(lines[index + 1] ?? '');
        const level = heading ? heading[1].length : underline && raw.trim() && !/^\s{0,3}[-*+>]/.test(raw) ? underline[1][0] === '=' ? 1 : 2 : 0;
        const name = heading ? heading[2] : raw.trim();
        if (level === 1 && !title) title = name;
        else if (level === 2) sections.push(name);
    });
    return [...title ? [words.title(quote(title))] : [], ...sections.length ? [words.sections(sections.length, sections.slice(0, LISTED).map(quote))] : [],
        ...blocks ? [words.codeBlocks(blocks)] : []];
}

/** reStructuredText: a title line is underlined with one repeated punctuation mark. */
function restructuredFacts(text: string): string[] {
    const lines = rows(text);
    const headings = lines.flatMap((line, index) => {
        const below = lines[index + 1] ?? '';
        return line.trim() && /^([=\-~^*#"'+`:.])\1+\s*$/.test(below) && below.trim().length >= line.trim().length ? [line.trim()] : [];
    });
    if (!headings.length) return [];
    const [first, ...rest] = headings;
    return [words.title(quote(first)), ...rest.length ? [words.sections(rest.length, rest.slice(0, LISTED).map(quote))] : []];
}

function patternFacts(text: string): string[] {
    const patterns = rows(text).map(line => line.trim()).filter(line => line && !line.startsWith('#'));
    return patterns.length ? [words.patterns(patterns.length, patterns.slice(0, LISTED).map(quote))] : [];
}

function xmlFacts(text: string): string[] {
    const root = /<([A-Za-z_][\w:.-]*)[\s>/]/.exec(text.replace(/<\?[\s\S]*?\?>|<!--[\s\S]*?-->|<![^>]*>/g, ''));
    return root ? [words.root(quote(`<${root[1]}>`))] : [];
}

/** The structure of the text, by the kind its name gives. */
function structureFacts(path: string, text: string): string[] {
    switch (extension(path)) {
        case 'yml': case 'yaml': return yamlFacts(text);
        case 'json': return jsonFacts(text);
        case 'toml': return tomlFacts(text);
        case 'ini': case 'cfg': case 'conf': case 'env': case 'properties': case 'editorconfig': case 'flake8': case 'coveragerc': case 'pylintrc': return iniFacts(text);
        case 'md': return markdownFacts(text);
        case 'rst': return restructuredFacts(text);
        case 'gitignore': case 'dockerignore': case 'gitattributes': return patternFacts(text);
        case 'xml': return xmlFacts(text);
        default: return [];
    }
}

/** The facts of a whole configuration or text file; a workflow keeps its own (jobs, triggers,
 * actions). Program code has none: there the source and the graph speak. */
export function fileFactLines(path: string, text: string): string[] {
    if (!isDataFile(path)) return [];
    const workflow = isGithubWorkflow(path) ? workflowFactLines(path, text) : [];
    if (workflow.length) return workflow;
    return [words.kind(fileKind(path)!, lineCount(text)), ...structureFacts(path, text)];
}

/** What the open file shows: a whole file in full, a marked part as that part, a cut
 * excerpt only by its kind and the note on what was left out. */
export function readerFacts(reader: BrowserChatReaderContext | undefined): string[] {
    const source = reader?.source;
    if (!source || !isDataFile(source.path)) return [];
    if (source.kind === 'selection') return [words.selected(source.startLine, source.endLine), ...structureFacts(source.path, source.text)];
    if (source.partial) return [words.kind(fileKind(source.path)!), source.partial];
    return fileFactLines(source.path, source.text);
}
