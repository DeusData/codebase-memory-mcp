import type { GraphData } from '../galaxy/types';
import type { SemanticNode } from './semantic-graph';
import { areaOf, MAP_RELATIONS } from './repository-map';

export interface LanguageMeasure { name: string; color: string; lines: number; files: number }
export interface SourceMeasure {
    /** Indexed whole-file line extent, including comments and blank lines. */
    lines?: number;
    measuredFiles: number;
    files: number;
    languages: LanguageMeasure[];
}
export interface FileMeasure { path: string; language: string; lines?: number; connection: 'connected' | 'none' | 'unknown' }
export interface SourceCatalog {
    files: Map<string, FileMeasure>;
    areas: Map<string, SourceMeasure>;
    total: SourceMeasure;
    referenceLines: number;
    referenceKind: 'folder' | 'file';
    referencePath?: string;
    partial: boolean;
}

const EXTENSIONS: Record<string, string> = {
    ts: 'TypeScript', tsx: 'TypeScript', mts: 'TypeScript', cts: 'TypeScript',
    js: 'JavaScript', jsx: 'JavaScript', mjs: 'JavaScript', cjs: 'JavaScript',
    py: 'Python', pyi: 'Python', c: 'C', h: 'C / C++', cc: 'C++', cpp: 'C++', cxx: 'C++', hpp: 'C++', hxx: 'C++', hh: 'C++',
    cs: 'C#', java: 'Java', kt: 'Kotlin', kts: 'Kotlin', go: 'Go', rs: 'Rust', rb: 'Ruby', php: 'PHP', swift: 'Swift',
    m: 'Objective-C / MATLAB', mm: 'Objective-C++', scala: 'Scala', sc: 'Scala', sh: 'Shell', bash: 'Shell', zsh: 'Shell', ps1: 'PowerShell',
    sql: 'SQL', html: 'HTML', htm: 'HTML', css: 'CSS', scss: 'SCSS', sass: 'Sass', vue: 'Vue', svelte: 'Svelte', astro: 'Astro',
    md: 'Markdown', mdx: 'MDX', json: 'JSON', jsonc: 'JSON', yml: 'YAML', yaml: 'YAML', toml: 'TOML', xml: 'XML', proto: 'Protobuf',
    r: 'R', hs: 'Haskell', ex: 'Elixir', exs: 'Elixir', erl: 'Erlang', ml: 'OCaml', fs: 'F#', clj: 'Clojure',
    dart: 'Dart', lua: 'Lua', jl: 'Julia', zig: 'Zig', sol: 'Solidity', tex: 'LaTeX', txt: 'Text', cmake: 'CMake',
};
const COLORS: Record<string, string> = {
    TypeScript: '#75bbef', JavaScript: '#e5cd73', Python: '#79b7aa', C: '#a7afdf', 'C / C++': '#b6b5d4', 'C++': '#e198bd',
    'C#': '#bb9be9', Java: '#e5aa78', Kotlin: '#c99be9', Go: '#73c9d5', Rust: '#d79e7e', Ruby: '#df8e9c', PHP: '#b2a9df', Swift: '#edab87',
    Vue: '#80d0ae', Svelte: '#e5a88d', HTML: '#dfa184', CSS: '#b8a0e2', SCSS: '#d9a0c4', SQL: '#d3be7e', Shell: '#a9c98c',
    Markdown: '#a3b2bf', JSON: '#b7bfaa', YAML: '#c3afb0', TOML: '#bbaea2', Unknown: '#81939e',
};
const pathKey = (path: string) => path.replaceAll('\\', '/').replace(/^\.\//, '').replace(/\/+/g, '/');
const compare = (a: string, b: string) => a < b ? -1 : a > b ? 1 : 0;

/** File-type classification, not a claim that an embedded language was parsed. */
export function sourceLanguage(path: string): string {
    const name = pathKey(path).split('/').at(-1) ?? '';
    if (/^dockerfile(?:\.|$)/i.test(name)) return 'Dockerfile';
    if (/^(?:gnumakefile|makefile)(?:\.|$)/i.test(name)) return 'Makefile';
    if (name === 'CMakeLists.txt') return 'CMake';
    if (name.endsWith('.C')) return 'C++';
    return EXTENSIONS[name.split('.').at(-1)?.toLowerCase() ?? ''] ?? 'Unknown';
}
export function languageColor(language: string): string {
    if (COLORS[language]) return COLORS[language];
    // Stable for less common languages, independent of repository or row order.
    let hash = 0;
    for (const char of language) hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
    return `hsl(${hash % 360}, 45%, 70%)`;
}
function summarize(files: FileMeasure[]): SourceMeasure {
    const languages = new Map<string, LanguageMeasure>();
    let measuredFiles = 0; let lines = 0;
    for (const file of files) {
        const language = languages.get(file.language) ?? { name: file.language, color: languageColor(file.language), lines: 0, files: 0 };
        language.files++;
        if (file.lines !== undefined) { lines += file.lines; measuredFiles++; language.lines += file.lines; }
        languages.set(file.language, language);
    }
    return { lines: measuredFiles ? lines : undefined, measuredFiles, files: files.length,
        languages: [...languages.values()].sort((a, b) => b.lines - a.lines || b.files - a.files || compare(a.name, b.name)) };
}

/** Build once from the unfiltered snapshot: scene caps and relations cannot resize a file. */
export function collectSourceMetrics(graph: GraphData, knownFiles: string[] = []): SourceCatalog {
    const files = new Map<string, FileMeasure>();
    const addFile = (path: string | undefined) => {
        if (!path || path === '{}') return undefined;
        const key = pathKey(path);
        if (!files.has(key)) files.set(key, { path: key, language: sourceLanguage(key), connection: 'unknown' });
        return files.get(key)!;
    };
    for (const path of knownFiles) addFile(path);
    const represented = new Set<string>();
    for (const node of graph.nodes) {
        if (['Project', 'Folder', 'Package', 'Branch'].includes(node.label)) continue;
        const file = addFile(node.file_path);
        if (file) represented.add(file.path);
        // File nodes often have 0/0 coordinates. Only a whole-file Module range
        // beginning at line 1 establishes this measure; symbol spans never do.
        if (file && node.label === 'Module' && node.start_line === 1 && Number.isSafeInteger(node.end_line) && node.end_line! > 0) {
            file.lines = Math.max(file.lines ?? 0, node.end_line!);
        }
    }
    const byId = new Map(graph.nodes.map(node => [node.id, node]));
    const relations = new Set<string>([...MAP_RELATIONS, 'HTTP_CALLS', 'ASYNC_CALLS', 'HANDLES']);
    const incomplete = graph.total_nodes > graph.nodes.length || ('edgesTruncated' in graph && graph.edgesTruncated !== false);
    for (const file of files.values()) if (!incomplete && represented.has(file.path)) file.connection = 'none';
    for (const edge of graph.edges) {
        if (!relations.has(edge.type)) continue;
        for (const id of [edge.source, edge.target]) {
            const path = byId.get(id)?.file_path; const file = path ? files.get(pathKey(path)) : undefined;
            if (file) file.connection = 'connected';
        }
    }
    const areaFiles = new Map<string, FileMeasure[]>();
    for (const file of files.values()) {
        const area = areaOf(file.path); const items = areaFiles.get(area) ?? [];
        items.push(file); areaFiles.set(area, items);
    }
    const areas = new Map([...areaFiles].map(([area, members]) => [area, summarize(members)]));
    const total = summarize([...files.values()]);
    const largest = (entries: [string, number | undefined][]) => {
        let result: { path: string; lines: number } | undefined;
        for (const [path, lines] of entries) {
            if (lines === undefined) continue;
            if (!result || lines > result.lines || (lines === result.lines && compare(path, result.path) < 0)) result = { path, lines };
        }
        return result;
    };
    // Root-direct files remain truthful measurements, but the root platform is
    // the visual baseline rather than a competing folder-size denominator.
    const folderReference = largest([...areas].filter(([path]) => path !== '(root)').map(([path, area]) => [path, area.lines]));
    const reference = folderReference ?? largest([...files.values()].map(file => [file.path, file.lines]));
    return { files, areas, total, referenceLines: Math.max(1, reference?.lines ?? 1),
        referenceKind: folderReference ? 'folder' : 'file', referencePath: reference?.path,
        partial: graph.total_nodes > graph.nodes.length || total.measuredFiles < total.files };
}

export function measureSourceNode(node: SemanticNode, catalog: SourceCatalog): SourceMeasure | undefined {
    if (node.kind === 'area') return node.areaPath ? catalog.areas.get(pathKey(node.areaPath)) : undefined;
    if (node.kind !== 'file' || !node.filePath) return undefined;
    const file = catalog.files.get(pathKey(node.filePath));
    return file ? summarize([file]) : undefined;
}

/** Raw relative size, independent of the compressed height used by the scene. */
export function sourceNodeSizePercent(node: SemanticNode, catalog: SourceCatalog): number | undefined {
    if (node.kind === 'area' && node.areaPath && pathKey(node.areaPath) === '(root)') return 0;
    const lines = measureSourceNode(node, catalog)?.lines;
    if (lines === undefined || !Number.isFinite(lines) || lines < 0 || catalog.referencePath === undefined) return undefined;
    const reference = Number.isFinite(catalog.referenceLines) ? Math.max(1, catalog.referenceLines) : 1;
    return Math.min(1, lines / reference) * 100;
}

/** Bounded square-root scale keeps smaller files visible with a distinct height spread. */
export function sourceBrickHeight(lines: number | undefined, referenceLines: number): number {
    if (lines === undefined || !Number.isFinite(lines) || lines < 0) return 0.65;
    const reference = Number.isFinite(referenceLines) ? Math.max(1, referenceLines) : 1;
    return 1.5 + 12.5 * Math.sqrt(Math.min(1, lines / reference));
}
