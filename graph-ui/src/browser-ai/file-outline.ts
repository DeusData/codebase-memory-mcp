import { isGithubWorkflow } from './file-kind';
import { iniSections, type IniEntry } from './file-facts';
import { chatRound3Text, fileOutlineWords, germanWorkflowWords, workflowWords } from './strings';
import { workflowFactLines, yamlTree, type YamlValue } from './workflow-facts';

/** "What does this file do?" about a configuration file, answered from the file (C7). The
 * small model wrote "YAML-Konfiguration für den Black-Commit-Tool" for django's pre-commit
 * file and, asked for detail, hung `additional_dependencies` on the wrong hooks. The outline
 * names what the file holds: every key, a list of mappings item by item with its fields, a
 * workflow by its counted facts. It is bounded, and says what it left out. */

const ENTRIES = 12;
const ITEMS = 10;
const FIELDS = 8;
const VALUE = 80;
const BUDGET = 3600;
/** The field that names an item of a nested list: "hooks (1): `black` (exclude ...)". */
const NAMING = ['id', 'name', 'key', 'uses', 'run', 'repo'];

type Words = typeof fileOutlineWords.en;
const code = (value: string) => `\`${(value.length > VALUE ? `${value.slice(0, VALUE - 1)}…` : value).replace(/`/g, "'")}\``;
const lineCount = (text: string) => text ? text.split(/\r?\n/).length - (/\r?\n$/.test(text) ? 1 : 0) : 0;
const extension = (path: string) => (path.split('/').pop() ?? path).split('.').slice(1).pop()?.toLowerCase() ?? '';

/** JSON as the same values as YAML. */
function jsonValue(value: unknown): YamlValue {
    if (Array.isArray(value)) return { kind: 'list', items: value.map(jsonValue) };
    if (value !== null && typeof value === 'object') return { kind: 'map', entries: Object.entries(value).map(([key, item]): [string, YamlValue] => [key, jsonValue(item)]) };
    return { kind: 'scalar', value: String(value) };
}

/** Brackets still open after `text`, outside strings and comments. */
function openBrackets(text: string): number {
    let depth = 0, quote: string | undefined;
    for (let index = 0; index < text.length; index++) {
        const character = text[index];
        if (quote) { if (character === '\\' && quote === '"') index++; else if (character === quote) quote = undefined; continue; }
        if (character === '#') break;
        if (character === '"' || character === "'") quote = character;
        else if (character === '[' || character === '{') depth++;
        else if (character === ']' || character === '}') depth--;
    }
    return depth;
}
/** The items of an array, split at its own commas, not at those inside strings or inline tables. */
function arrayItems(inner: string): string[] {
    const items: string[] = [];
    let depth = 0, quote: string | undefined, start = 0;
    for (let index = 0; index < inner.length; index++) {
        const character = inner[index];
        if (quote) { if (character === '\\' && quote === '"') index++; else if (character === quote) quote = undefined; continue; }
        if (character === '"' || character === "'") quote = character;
        else if (character === '[' || character === '{') depth++;
        else if (character === ']' || character === '}') depth--;
        else if (character === ',' && depth === 0) { items.push(inner.slice(start, index)); start = index + 1; }
    }
    return [...items, inner.slice(start)].map(item => item.trim()).filter(Boolean);
}

/** TOML: the keys before the first table, each `[table]` with its keys, each `[[table]]` as a list
 * item. An array may run over several lines ("dependencies = [" with one item per line). */
function tomlTree(text: string): YamlValue | undefined {
    const root: [string, YamlValue][] = [];
    let current = root, quoted: string | undefined;
    const unquote = (item: string) => item.replace(/^(["'])(.*)\1$/, '$2');
    const value = (raw: string): YamlValue => {
        const plain = raw.replace(/\s+#[^"']*$/, '').trim();
        if (/^\[[\s\S]*\]$/.test(plain)) return { kind: 'list', items: arrayItems(plain.slice(1, -1)).map(item => ({ kind: 'scalar', value: unquote(item) })) };
        return { kind: 'scalar', value: unquote(plain) };
    };
    const lines = text.split(/\r?\n/);
    for (let at = 0; at < lines.length; at++) {
        const line = lines[at].trim();
        if (quoted) { if (line.split(quoted).length % 2 === 0) quoted = undefined; continue; }
        if (!line || line.startsWith('#')) continue;
        const table = /^(\[\[?)\s*([^\]]+?)\s*\]\]?\s*(?:#.*)?$/.exec(line);
        if (table) {
            const entries: [string, YamlValue][] = [];
            if (table[1] === '[[') {
                const name = `[[${table[2]}]]`;
                const list = root.find(([key]) => key === name)?.[1];
                if (list?.kind === 'list') list.items.push({ kind: 'map', entries }); else root.push([name, { kind: 'list', items: [{ kind: 'map', entries }] }]);
            } else root.push([`[${table[2]}]`, { kind: 'map', entries }]);
            current = entries;
            continue;
        }
        const key = /^([A-Za-z0-9_\-."']+)\s*=\s*(.*)$/.exec(line);
        if (!key) continue;
        const delimiter = ['"""', "'''"].find(mark => line.split(mark).length === 2);
        if (delimiter) quoted = delimiter;
        let raw = key[2];
        // The lines an open array or inline table runs over, without their comments.
        while (!delimiter && openBrackets(raw) > 0 && at + 1 < lines.length) raw += ` ${lines[++at].trim().replace(/\s+#[^"']*$/, '')}`;
        current.push([key[1], delimiter ? { kind: 'scalar', value: '' } : value(raw)]);
    }
    return root.length ? { kind: 'map', entries: root } : undefined;
}

/** INI: the keys before the first section, then each `[section]` with its keys. A value
 * continued on indented lines is a list of those lines (`deps =` with one per line, B6). */
function iniTree(text: string): YamlValue | undefined {
    const { loose, sections } = iniSections(text);
    const value = (entry: IniEntry): YamlValue => entry.values.length > 1 ? { kind: 'list', items: entry.values.map(item => ({ kind: 'scalar', value: item })) }
        : { kind: 'scalar', value: entry.values[0] ?? '' };
    const entries = (list: readonly IniEntry[]) => list.map((entry): [string, YamlValue] => [entry.key, value(entry)]);
    const root = [...entries(loose), ...sections.map((section): [string, YamlValue] => [section.name, { kind: 'map', entries: entries(section.entries) }])];
    return root.length ? { kind: 'map', entries: root } : undefined;
}

const INI = new Set(['ini', 'cfg', 'flake8', 'coveragerc', 'pylintrc', 'editorconfig']);
function treeOf(path: string, text: string): YamlValue | undefined {
    const kind = extension(path);
    if (INI.has(kind)) return iniTree(text);
    switch (kind) {
        case 'yml': case 'yaml': return yamlTree(text);
        case 'json': try { return jsonValue(JSON.parse(text)); } catch { return undefined; }
        case 'toml': return tomlTree(text);
        default: return undefined;
    }
}

type Purpose = keyof typeof chatRound3Text.en.purposes;
const has = (tree: YamlValue | undefined, test: (key: string) => boolean) => tree?.kind === 'map' && tree.entries.some(([key]) => test(key));
/** What a well-known file is for, by its name, when its structure is that of such a file (B7). */
function purposeOf(path: string, tree: YamlValue | undefined): Purpose | undefined {
    const name = (path.split('/').pop() ?? path).toLowerCase();
    if (/^\.pre-commit-config\.ya?ml$/.test(name)) return has(tree, key => key === 'repos') ? 'preCommit' : undefined;
    if (name === 'package.json') return tree?.kind === 'map' ? 'npmPackage' : undefined;
    if (name === 'pyproject.toml') return has(tree, key => /^\[(?:project|build-system|tool\..+)\]$/.test(key)) ? 'pyproject' : undefined;
    if (name === 'tox.ini') return has(tree, key => /^\[(?:tox|testenv.*)\]$/.test(key)) ? 'tox' : undefined;
    if (name === 'setup.cfg') return tree?.kind === 'map' ? 'setupCfg' : undefined;
    if (/^(?:docker-)?compose(?:\.[\w-]+)?\.ya?ml$/.test(name)) return has(tree, key => key === 'services') ? 'compose' : undefined;
    if (/^\.readthedocs\.ya?ml$/.test(name)) return has(tree, key => key === 'version' || key === 'build' || key === 'sphinx') ? 'readTheDocs' : undefined;
    if (name === '.editorconfig') return has(tree, key => key.startsWith('[') || key === 'root') ? 'editorConfig' : undefined;
    if (/^tsconfig(?:\.[\w-]+)?\.json$/.test(name)) return has(tree, key => ['compilerOptions', 'include', 'files', 'extends', 'references'].includes(key)) ? 'tsconfig' : undefined;
    if (name === '.flake8') return has(tree, key => key === '[flake8]') ? 'flake8' : undefined;
    if (name === 'pytest.ini') return has(tree, key => key === '[pytest]') ? 'pytest' : undefined;
    if (name === '.coveragerc') return has(tree, key => key.startsWith('[')) ? 'coverage' : undefined;
    return undefined;
}

const isMap = (value: YamlValue): value is Extract<YamlValue, { kind: 'map' }> => value.kind === 'map';
const scalarText = (value: YamlValue, words: Words) => value.kind === 'scalar' ? value.value === '' ? words.empty : /^[|>][-+0-9]*$/.test(value.value) ? words.text : code(value.value) : '';
const more = (total: number, shown: number, words: Words, separator = '; ') => total > shown ? `${separator}${words.more(total - shown)}` : '';

/** A mapping on one line: "repo `x`; rev `y`; hooks (1): `black` (exclude `z`)". */
function inlineMap(value: Extract<YamlValue, { kind: 'map' }>, depth: number, words: Words): string {
    if (!value.entries.length) return words.empty;
    return value.entries.slice(0, FIELDS).map(([key, item]) => field(key, item, depth, words)).join('; ') + more(value.entries.length, FIELDS, words);
}
function field(key: string, value: YamlValue, depth: number, words: Words): string {
    if (value.kind === 'scalar') return `${key} ${scalarText(value, words)}`;
    if (value.kind === 'map') return depth < 2 ? `${key}: ${inlineMap(value, depth + 1, words)}` : `${key} (${words.keys(value.entries.length)})`;
    if (!value.items.some(isMap)) return `${key}: ${value.items.slice(0, ITEMS).map(item => scalarText(item, words) || words.list(0)).join(', ')}${more(value.items.length, ITEMS, words, ', ')}`;
    // A single item without a naming field needs no parentheses to stand apart from the next.
    const [only] = value.items;
    if (value.items.length === 1 && isMap(only) && !namingOf(only)) return `${key} (1): ${inlineMap(only, depth + 1, words)}`;
    return `${key} (${value.items.length}): ${value.items.slice(0, ITEMS).map(item => named(item, depth + 1, words)).join('; ')}${more(value.items.length, ITEMS, words)}`;
}
const namingOf = (value: Extract<YamlValue, { kind: 'map' }>) => NAMING.map(key => value.entries.find(([name, item]) => name === key && item.kind === 'scalar')).find(Boolean);
/** An item of a nested list by its naming field, the others in parentheses. */
function named(value: YamlValue, depth: number, words: Words): string {
    if (!isMap(value)) return scalarText(value, words) || words.list(value.kind === 'list' ? value.items.length : 0);
    const naming = namingOf(value);
    if (!naming) return depth < 3 ? `(${inlineMap(value, depth, words)})` : `(${words.keys(value.entries.length)})`;
    const rest = value.entries.filter(entry => entry !== naming);
    const details = rest.length && depth < 3 ? ` (${inlineMap({ kind: 'map', entries: rest }, depth, words)})` : '';
    return `${scalarText(naming[1], words)}${details}`;
}

/** One top-level key as bullet lines. */
function entryLines(key: string, value: YamlValue, words: Words): string[] {
    if (value.kind === 'scalar') return [`- ${code(key)}: ${scalarText(value, words)}`];
    if (value.kind === 'map') return [`- ${code(key)}: ${inlineMap(value, 1, words)}`];
    if (!value.items.some(isMap)) return [`- ${code(key)} (${words.list(value.items.length)}): ${value.items.slice(0, ITEMS).map(item => scalarText(item, words) || words.list(0)).join(', ')}${more(value.items.length, ITEMS, words, ', ')}`];
    return [`- ${code(key)} (${words.list(value.items.length)}):`, ...value.items.slice(0, ITEMS).map((item, at) => `  ${at + 1}. ${isMap(item) ? inlineMap(item, 1, words) : named(item, 1, words)}`),
        ...value.items.length > ITEMS ? [`  ${words.more(value.items.length - ITEMS)}`] : []];
}

/** The outline of a YAML, JSON, TOML or INI file in the language of the question, or undefined
 * for any other file and for text that does not parse. A well-known file says first what it
 * is for; "Read from the file" stands once, in the note at the end (B7). */
export function fileOutline(path: string, text: string, language: 'en' | 'de'): string | undefined {
    const words = fileOutlineWords[language], own = chatRound3Text[language];
    const name = code(path.split('/').pop() ?? path);
    if (isGithubWorkflow(path)) {
        const facts = workflowFactLines(path, text, language === 'de' ? germanWorkflowWords : workflowWords);
        if (facts.length) return [own.outlineHeading(name, words.kinds.workflow, lineCount(text)), own.purposes.workflow, facts.map(line => `- ${line}`).join('\n'), `_${words.note}_`].join('\n\n');
    }
    const tree = treeOf(path, text);
    if (!tree) return undefined;
    const kind = extension(path) === 'json' ? words.kinds.json : extension(path) === 'toml' ? words.kinds.toml : INI.has(extension(path)) ? own.iniKind : words.kinds.yaml;
    const purpose = purposeOf(path, tree);
    const entries: [string, YamlValue][] = tree.kind === 'map' ? tree.entries : [['', tree]];
    const lines: string[] = [];
    let used = 0, shown = 0;
    for (const [key, value] of entries.slice(0, ENTRIES)) {
        const next = key ? entryLines(key, value, words) : value.kind === 'list' ? entryLines(name.slice(1, -1), value, words) : [`- ${scalarText(value, words)}`];
        const size = next.join('\n').length;
        if (used + size > BUDGET && lines.length) { lines.push(words.cut); break; }
        lines.push(...next); used += size; shown++;
    }
    if (shown === Math.min(entries.length, ENTRIES) && entries.length > ENTRIES) lines.push(words.more(entries.length - ENTRIES));
    return [own.outlineHeading(name, kind, lineCount(text)), ...purpose ? [own.purposes[purpose]] : [], lines.join('\n'), `_${words.note}_`].join('\n\n');
}
