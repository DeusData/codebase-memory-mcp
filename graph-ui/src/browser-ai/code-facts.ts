import { chatRound3Text } from './strings';

/** What the selected code declares, read from its text and never written by the model (B2).
 *
 * "was macht diese klasse?" about JSONBAgg got four lines of counts: the model's sentence named
 * "Arrays" and was dropped every time, and nothing said what the class declares. These lines say
 * it: for a class its definition with its bases, its class-level assignments and then its
 * methods; for a function its signature, the first line of its docstring and, when it is short,
 * its body. Python is read by its indentation; other languages give their definition line. The
 * lines are the source's own, joined only where a signature runs over several lines. */

/** At most this many lines; the rest is counted. */
export const CODE_FACT_LINES = 6;
/** A function body up to this many lines is shown whole. */
const SHORT_BODY = 4;
/** Longer lines end in "…". */
const LINE = 120;

export interface CodeFacts {
    lines: string[];
    /** Members (`members`) or body lines (`lines`) that did not fit. */
    more: number;
    unit: 'members' | 'lines';
    /** The language of the Markdown code block. */
    fence: string;
}

const FENCES: Readonly<Record<string, string>> = { py: 'python', pyi: 'python', ts: 'typescript', tsx: 'typescript', js: 'javascript', mjs: 'javascript', jsx: 'javascript',
    go: 'go', rs: 'rust', java: 'java', c: 'c', h: 'c', cc: 'cpp', cpp: 'cpp', hpp: 'cpp', rb: 'ruby', sh: 'bash', kt: 'kotlin', cs: 'csharp', php: 'php', swift: 'swift' };
const extension = (path: string) => (path.split('/').pop() ?? path).replace(/-tpl$/, '').split('.').slice(1).pop()?.toLowerCase() ?? '';
const indentOf = (line: string) => line.length - line.trimStart().length;
const bounded = (line: string) => line.length > LINE ? `${line.slice(0, LINE - 1)}…` : line;
const escape = (name: string) => name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/** Brackets still open after `text`, outside strings. */
function openBrackets(text: string): number {
    let depth = 0, quote: string | undefined;
    for (let index = 0; index < text.length; index++) {
        const character = text[index];
        if (quote) { if (character === '\\') index++; else if (character === quote) quote = undefined; continue; }
        if (character === '#') break;
        if (character === '"' || character === "'") quote = character;
        else if ('([{'.includes(character)) depth++;
        else if (')]}'.includes(character)) depth--;
    }
    return depth;
}

/** The statement starting at `start`, joined over the lines its brackets keep open. */
function statement(lines: readonly string[], start: number): { text: string; end: number } {
    let text = lines[start].trim(), end = start;
    while (openBrackets(text) > 0 && end + 1 < lines.length && end - start < 8) text += ` ${lines[++end].trim()}`;
    return { text: text.replace(/([([{])\s+/g, '$1').replace(/\s+([)\]}])/g, '$1').replace(/,([)\]}])/g, '$1'), end };
}

/** The first line of a docstring right after line `after`, as it is written (one or two lines). */
function docstring(lines: readonly string[], after: number, indent: string): string[] {
    const at = lines.findIndex((line, index) => index > after && line.trim());
    const first = at >= 0 ? lines[at].trim() : '';
    if (!/^[rubRUB]{0,2}("""|''')/.test(first)) return [];
    const bare = /^[rubRUB]{0,2}("""|''')$/.test(first);
    const next = bare ? lines.slice(at + 1).find(line => line.trim())?.trim() : undefined;
    return bare && next ? [`${indent}${first}`, `${indent}${bounded(next)}`] : [`${indent}${bounded(first)}`];
}

/** Where a docstring after line `after` ends, or `after` without one. */
function docstringEnd(lines: readonly string[], after: number): number {
    const at = lines.findIndex((line, index) => index > after && line.trim());
    const match = at >= 0 ? /^[rubRUB]{0,2}("""|''')/.exec(lines[at].trim()) : null;
    if (!match) return after;
    const rest = lines[at].trim().slice(match[0].length);
    if (rest.includes(match[1])) return at;
    const close = lines.findIndex((line, index) => index > at && line.includes(match[1]));
    return close >= 0 ? close : lines.length - 1;
}

function pythonFacts(lines: readonly string[], name?: string): CodeFacts | undefined {
    const definition = new RegExp(`^\\s*(?:async\\s+)?(?:def|class)\\s+${name ? `${escape(name)}\\b` : '\\w'}`);
    let start = lines.findIndex(line => definition.test(line));
    if (start < 0) start = lines.findIndex(line => /^\s*(?:async\s+)?(?:def|class)\s+\w/.test(line));
    if (start < 0) return undefined;
    const base = indentOf(lines[start]);
    const shift = (line: string) => line.slice(Math.min(base, indentOf(line)));
    const header = statement(lines, start);
    const isClass = /^class\s/.test(header.text);
    const inner = lines.slice(header.end + 1).find(line => line.trim() && !line.trim().startsWith('#'));
    const pad = ' '.repeat(Math.max(4, inner ? indentOf(inner) - base : 4));
    const out = [bounded(header.text), ...docstring(lines, header.end, pad)];
    const bodyStart = docstringEnd(lines, header.end) + 1;
    const body = lines.slice(bodyStart).filter(line => line.trim() && !line.trim().startsWith('#') && indentOf(line) > base);
    if (!isClass) {
        const shown = body.length <= SHORT_BODY ? body.map(line => bounded(shift(line).trimEnd())) : [];
        const room = Math.max(0, CODE_FACT_LINES - out.length);
        return { lines: [...out, ...shown.slice(0, room)], more: body.length - Math.min(shown.length, room), unit: 'lines', fence: 'python' };
    }
    // Members at the class's own body indent: assignments first, then methods with their decorators.
    const memberIndent = inner ? indentOf(inner) : base + 4;
    const assignments: string[][] = [], methods: string[][] = [];
    let decorators: string[] = [];
    for (let index = bodyStart; index < lines.length; index++) {
        const line = lines[index];
        if (!line.trim() || line.trim().startsWith('#')) continue;
        if (indentOf(line) <= base) break;
        if (indentOf(line) !== memberIndent) continue;
        const { text, end } = statement(lines, index);
        if (text.startsWith('@')) { decorators.push(`${pad}${bounded(text)}`); index = end; continue; }
        if (/^(?:async\s+)?(?:def|class)\s/.test(text)) methods.push([...decorators, `${pad}${bounded(text)}`]);
        else if (/^[A-Za-z_]\w*(?:\s*:[^=]+)?\s*=[^=]/.test(text)) assignments.push([`${pad}${bounded(text)}`]);
        decorators = [];
        index = end;
    }
    let more = 0;
    for (const member of [...assignments, ...methods]) {
        if (out.length + member.length <= CODE_FACT_LINES) out.push(...member); else more++;
    }
    return { lines: out, more, unit: 'members', fence: 'python' };
}

/** The line that declares `name`, or the first line of code; without its opening brace. */
function definitionLine(lines: readonly string[], name: string | undefined, fence: string): CodeFacts | undefined {
    const code = lines.filter(line => line.trim() && !/^\s*(?:\/\/|\/\*|\*|#|--|<!--)/.test(line));
    const named = name ? code.find(line => new RegExp(`(?:^|[^\\w$])${escape(name)}(?:[^\\w$]|$)`).test(line)) : undefined;
    const line = (named ?? code[0])?.trim().replace(/\s*\{\s*$/, '');
    return line ? { lines: [bounded(line)], more: 0, unit: 'lines', fence } : undefined;
}

/** The facts of the code a selection stands for, or undefined without code. */
export function codeFacts(source: { text: string; path: string }, subject?: { name: string; kind?: string }): CodeFacts | undefined {
    const lines = source.text.replace(/\r\n?/g, '\n').split('\n');
    if (!source.text.trim()) return undefined;
    const fence = FENCES[extension(source.path)] ?? '';
    return (fence === 'python' ? pythonFacts(lines, subject?.name) : undefined) ?? definitionLine(lines, subject?.name, fence);
}

const LANGUAGES: Readonly<Record<string, string>> = { python: 'Python', typescript: 'TypeScript', javascript: 'JavaScript', go: 'Go', rust: 'Rust', java: 'Java', c: 'C', cpp: 'C++',
    ruby: 'Ruby', bash: 'Shell', kotlin: 'Kotlin', csharp: 'C#', php: 'PHP', swift: 'Swift' };
/** At most this many definitions are named; the rest is counted. */
const DEFINITIONS = 8;
const PYTHON_DEFINITION = /^(?:async\s+)?(def|class)\s+([A-Za-z_]\w*)/;
const DEFINITION = /^(?:export\s+)?(?:default\s+)?(?:declare\s+)?(?:abstract\s+)?(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(function\*?|class|interface|type|enum|struct|trait|fn|func|const|let|var)\s+([A-Za-z_$][\w$]*)/;

/** The definitions at the outermost indent of the code: "class ArrayAgg", "def ordered". */
function definitionsOf(lines: readonly string[], fence: string): string[] {
    const code = lines.filter(line => line.trim() && !/^\s*(?:#|\/\/|\/\*|\*)/.test(line));
    const outer = Math.min(...code.map(indentOf));
    return code.filter(line => indentOf(line) === outer).flatMap(line => {
        const match = (fence === 'python' ? PYTHON_DEFINITION : DEFINITION).exec(line.trim());
        return match ? [`${match[1]} ${match[2]}`] : [];
    });
}

/** The first line of a Python module docstring. */
function moduleDocstring(lines: readonly string[]): string | undefined {
    const at = lines.findIndex(line => line.trim() && !line.trim().startsWith('#'));
    const doc = at >= 0 ? docstring(lines, at - 1, '') : [];
    const text = doc.at(-1)?.replace(/^[rubRUB]{0,2}("""|''')/, '').replace(/("""|''')$/, '').trim();
    return text || undefined;
}

/** An open code file or marked code as facts read from it, for a short general question: what it
 * is and how long, a module's docstring, its definitions, and the code facts of a single marked
 * symbol (B3). `names` are the definitions' names, by which a question may ask about it. */
export function codeSourceFacts(source: { text: string; path: string; kind: 'file' | 'selection'; startLine: number; endLine: number }, language: 'en' | 'de'): { summary: string[]; code?: string; names: string[] } {
    const words = chatRound3Text[language];
    const fence = FENCES[extension(source.path)] ?? '';
    const kind = words.codeKind(LANGUAGES[fence] ?? '');
    const name = `\`${(source.path.split('/').pop() ?? source.path).replace(/`/g, "'")}\``;
    const lines = source.text.replace(/\r\n?/g, '\n').replace(/\n$/, '').split('\n');
    const defined = source.text.trim() ? definitionsOf(lines, fence) : [];
    const names = defined.map(item => item.split(' ').pop()!);
    const listed = (where: 'file' | 'marked') => defined.length ? [words.definitions(defined.length, defined.slice(0, DEFINITIONS).map(item => `\`${item}\``), where)] : [];
    if (source.kind === 'file') {
        const doc = fence === 'python' ? moduleDocstring(lines) : undefined;
        return { summary: [words.outlineHeading(name, kind, lines.length), ...doc ? [words.moduleDocstring(bounded(doc))] : [], ...listed('file')], names };
    }
    const marked = words.marked(source.startLine, source.endLine, name, kind);
    // One marked class or function: its own lines say what it declares (B2).
    const first = lines.find(line => line.trim() && !/^\s*(?:#|\/\/|@)/.test(line));
    const single = defined.length === 1 && first !== undefined && (fence === 'python' ? PYTHON_DEFINITION : DEFINITION).test(first.trim());
    const facts = single ? codeFacts(source, { name: names[0] }) : undefined;
    return { summary: [marked, ...single ? [] : listed('marked')], ...facts ? { code: codeFactsMarkdown(facts, language) } : {}, names };
}

/** "In the source:" with the lines as a code block and what did not fit, in the language of the question. */
export function codeFactsMarkdown(facts: CodeFacts, language: 'en' | 'de'): string {
    const words = chatRound3Text[language];
    const mark = facts.lines.some(line => line.includes('```')) ? '~~~' : '```';
    const more = facts.more ? `\n\n${facts.unit === 'members' ? words.moreMembers(facts.more) : words.moreLines(facts.more)}` : '';
    return `${words.inSource}\n\n${mark}${facts.fence}\n${facts.lines.join('\n')}\n${mark}${more}`;
}
