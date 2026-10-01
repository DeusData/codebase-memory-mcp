import { RpcIntelligenceClient } from '../provider/rpc-client';
import type { ProjectEntry } from '../provider/rpc-schemas';
import { loadRouteGraph } from './route-graph-source';
import { buildContainerTopology, normalizeSourcePath, type ContainerTopology, type ServiceProject } from './container-topology';

interface SourceSpan { qn: string; first: number; last: number }
export interface ContainerInventory {
    project: string; rootPath: string; files: Map<string, string>; manifests: string[]; warnings: string[];
    sourceEnds?: Map<string, number>;
    sourceSpans?: Map<string, SourceSpan[]>;
}
export interface ContainerSelection { inventory: ContainerInventory; manifest: string }
export interface ContainerReading { topology: ContainerTopology; projects: ServiceProject[]; warnings: string[]; filesRead: number }
export const isComposePath = (path: string) => /(^|\/)(?:docker-)?compose(?:[.-][\w.-]+)?\.ya?ml$/i.test(path);
const isDockerfile = (path: string) => /(^|\/)(?:Dockerfile(?:[.][\w.-]+)?|[\w.-]+\.Dockerfile)$/i.test(path);
const isCode = (path: string) => /\.(?:py|js|jsx|ts|tsx|cs|go|java|kt|rs|rb|php|c|cc|cpp|ex|exs)$/i.test(path)
    && !/(^|\/)(?:node_modules|vendor|vendored|test|tests|fixtures|dist|build)\//.test(path)
    && !/\.(?:min|test|spec)\.[^.]+$/.test(path);
const checkAbort = (signal: AbortSignal) => { if (signal.aborted) throw new DOMException('Aborted', 'AbortError'); };

/** Queries only indexed files. The caller explicitly selects each additional project. */
export async function loadContainerInventory(project: ProjectEntry, client: RpcIntelligenceClient): Promise<ContainerInventory> {
    const [rows, modules, classes, interfaces] = await Promise.all([
        client.queryRows(project.name, 'MATCH (f:File) RETURN f.file_path AS path, f.qualified_name AS qn ORDER BY f.file_path LIMIT 4001'),
        client.queryRows(project.name, 'MATCH (f:Module) RETURN f.file_path AS path, f.qualified_name AS qn, f.start_line AS first_line, f.end_line AS last_line ORDER BY f.file_path LIMIT 4001'),
        client.queryRows(project.name, 'MATCH (f:Class) RETURN f.file_path AS path, f.qualified_name AS qn, f.start_line AS first_line, f.end_line AS last_line ORDER BY f.file_path LIMIT 4001'),
        client.queryRows(project.name, 'MATCH (f:Interface) RETURN f.file_path AS path, f.qualified_name AS qn, f.start_line AS first_line, f.end_line AS last_line ORDER BY f.file_path LIMIT 4001'),
    ]);
    const files = new Map<string, string>();
    for (const row of rows.slice(0, 4000)) if (row.path && row.path !== '{}' && row.qn) files.set(row.path, row.qn);
    const sourceEnds = new Map<string, number>();
    for (const row of modules.slice(0, 4000)) if (files.has(row.path) && row.qn && Number(row.first_line) === 1
        && Number.isSafeInteger(Number(row.last_line)) && Number(row.last_line) >= 1) {
        files.set(row.path, row.qn); sourceEnds.set(row.path, Number(row.last_line));
    }
    // Java and some other languages expose class/interface ranges without a file-wide Module.
    // Keep exact ranges rather than claiming that a default File preview contains the whole file.
    const sourceSpans = new Map<string, SourceSpan[]>();
    for (const row of [...classes.slice(0, 4000), ...interfaces.slice(0, 4000)]) {
        const first = Number(row.first_line), last = Number(row.last_line);
        if (!files.has(row.path) || sourceEnds.has(row.path) || !isCode(row.path) || !row.qn
            || !Number.isSafeInteger(first) || !Number.isSafeInteger(last) || first < 1 || last < first) continue;
        const spans = sourceSpans.get(row.path) ?? [];
        spans.push({ qn: row.qn, first, last }); sourceSpans.set(row.path, spans);
    }
    for (const [path, spans] of sourceSpans) {
        const outer: SourceSpan[] = [];
        for (const span of spans.sort((a, b) => a.first - b.first || b.last - a.last || a.qn.localeCompare(b.qn))) {
            if (!outer.length || span.first > outer[outer.length - 1].last) outer.push(span);
        }
        sourceSpans.set(path, outer);
    }
    const manifests = [...files.keys()].filter(isComposePath).sort((a, b) =>
        Number(!/^(?:docker-)?compose\.ya?ml$/.test(a)) - Number(!/^(?:docker-)?compose\.ya?ml$/.test(b)) || a.localeCompare(b));
    return { project: project.name, rootPath: project.root_path ?? '', files, manifests, sourceEnds, sourceSpans,
        warnings: [rows, modules, classes, interfaces].some(result => result.length > 4000)
            ? ['File and symbol discovery is limited to 4,000 rows per kind for this project.'] : [] };
}

/** A partial YAML document must never masquerade as a complete deployment. */
export async function readContainerSource(inventory: ContainerInventory, path: string, client: RpcIntelligenceClient, signal: AbortSignal): Promise<string> {
    const qn = inventory.files.get(path);
    if (!qn) throw new Error('File is outside the indexed inventory.');
    // File nodes without a Module span can return a 51-line preview with no truncation flag.
    if (inventory.sourceEnds && !inventory.sourceEnds.has(path)) {
        const spans = isCode(path) ? inventory.sourceSpans?.get(path) : undefined;
        if (!spans?.length) throw new Error('No complete module span or indexed code range is available.');
        if (spans.length > 8 || spans[spans.length - 1].last > 20_000
            || spans.reduce((total, span) => total + span.last - span.first + 1, 0) > 2000) throw new Error('Indexed code exceeds the source limit.');
        const lines: string[] = [];
        for (const span of spans) {
            const code = await readSourceRange(inventory.project, span.qn, span.first, span.last, client, signal);
            const part = code.split('\n');
            if (part[part.length - 1] === '') part.pop();
            if (part.length !== span.last - span.first + 1) throw new Error('Indexed code range changed while reading.');
            while (lines.length < span.first - 1) lines.push('');
            lines.push(...part);
            if (lines.reduce((total, line) => total + line.length + 1, 0) > 160_000) throw new Error('Source exceeds the inspection limit.');
        }
        return lines.join('\n');
    }
    return readSourceRange(inventory.project, qn, 1, inventory.sourceEnds?.get(path), client, signal);
}

async function readSourceRange(project: string, qn: string, first: number, last: number | undefined, client: RpcIntelligenceClient, signal: AbortSignal): Promise<string> {
    let source = '', next = first;
    for (let page = 0; page < 4; page++) {
        checkAbort(signal);
        const snippet = await client.getCodeSnippet(project, qn, { startLine: next, maxLines: 500 });
        checkAbort(signal);
        if ((snippet.start_line ?? 1) !== next || snippet.source_mode === 'outline'
            || !snippet.source || snippet.source === '(source not available)') throw new Error('Source page is unavailable.');
        source += (page && !source.endsWith('\n') ? '\n' : '') + snippet.source;
        if (source.length > 160_000) throw new Error('Source exceeds the inspection limit.');
        if (snippet.next_start_line === undefined) {
            if (snippet.source_clipped || snippet.source_truncated
                || (last !== undefined && (snippet.end_line ?? 0) < last)) throw new Error('Source is incomplete.');
            return source;
        }
        if (snippet.next_start_line <= next || snippet.next_start_line !== (snippet.end_line ?? 0) + 1) throw new Error('Source pages changed while reading.');
        next = snippet.next_start_line;
    }
    throw new Error('Source exceeds the 2,000-line inspection limit.');
}

/** Round-robin build scopes so one large service cannot consume every inspected file. */
function fairSourcePaths(paths: string[], project: ServiceProject, services: ContainerTopology['services']): string[] {
    const available = new Map(paths.map(path => [normalizeSourcePath(project.rootPath, path), path]));
    const priority = (path: string) => {
        const name = path.split('/').at(-1) ?? '';
        if (/(?:impl|client|gateway|transport)\.[^.]+$/i.test(name)) return 0;
        return /(?:service|controller|repository)/i.test(name) ? 1 : 2;
    };
    const queues = [...services].sort((a, b) => a.id.localeCompare(b.id)).map(service => service.sourcePaths
        .flatMap(path => available.has(path) ? [available.get(path)!] : [])
        .sort((a, b) => priority(a) - priority(b) || a.localeCompare(b))).filter(queue => queue.length);
    const ordered = new Set<string>();
    for (let index = 0; queues.some(queue => index < queue.length); index++) {
        for (const queue of queues) if (queue[index]) ordered.add(queue[index]);
    }
    paths.forEach(path => ordered.add(path));
    return [...ordered];
}

/** Per-request lifetime; no ambient environment, shell, deployment, or external URL access. */
export async function loadContainerTopology(selections: readonly ContainerSelection[], client: RpcIntelligenceClient, signal: AbortSignal): Promise<ContainerReading> {
    const warnings: string[] = [];
    let filesRead = 0;
    const rangeFiles = new Set<string>();
    let sourceChars = 0;
    const projects: ServiceProject[] = selections.slice(0, 4).map(({ inventory, manifest }) => ({
        project: inventory.project, rootPath: inventory.rootPath, manifest, files: [...inventory.files.keys()], sources: new Map<string, string>(),
    }));
    if (selections.length > 4) warnings.push('Compare at most four indexed projects at once.');
    const read = async (index: number, paths: string[]) => {
        const inventory = selections[index].inventory;
        // Four requests at a time keep a large repo from monopolizing the daemon.
        for (let offset = 0; offset < paths.length; offset += 4) {
            checkAbort(signal);
            if (sourceChars >= 4_000_000) { warnings.push('Source inspection reached the four-million-character budget.'); break; }
            await Promise.all(paths.slice(offset, offset + 4).map(async path => {
                try {
                    const source = await readContainerSource(inventory, path, client, signal);
                    if (sourceChars + source.length > 4_000_000) { sourceChars = 4_000_000; warnings.push('Source inspection reached the four-million-character budget.'); return; }
                    sourceChars += source.length;
                    (projects[index].sources as Map<string, string>).set(path, source); filesRead++;
                    if (inventory.sourceSpans?.has(path) && !inventory.sourceEnds?.has(path)) rangeFiles.add(`${inventory.project}:${path}`);
                } catch {
                    checkAbort(signal);
                    warnings.push(`${inventory.project}: ${path} was unavailable, incomplete, or exceeded the source limit.`);
                }
            }));
        }
    };
    for (let index = 0; index < projects.length; index++) {
        warnings.push(...selections[index].inventory.warnings);
        if (projects[index].manifest) await read(index, [projects[index].manifest]);
        const dockerfiles = projects[index].files.filter(isDockerfile);
        if (dockerfiles.length > 32) warnings.push(`${projects[index].project}: Dockerfile inspection is limited to 32 files.`);
        await read(index, dockerfiles.slice(0, 32));
    }
    const declarations = buildContainerTopology(projects);
    for (let index = 0; index < projects.length; index++) {
        // Candidate ownership is established from declarations before reading application code.
        const candidates = new Set(declarations.services.flatMap(service => service.sourcePaths));
        const paths = fairSourcePaths(projects[index].files.filter(path => candidates.has(normalizeSourcePath(projects[index].rootPath, path)) && isCode(path)), projects[index], declarations.services);
        if (paths.length > 80) warnings.push(`${projects[index].project}: inspected 80 of ${paths.length} candidate source files.`);
        await read(index, paths.slice(0, 80));
        checkAbort(signal);
        projects[index].routeGraph = await loadRouteGraph(projects[index].project, { client, signal });
        warnings.push(...projects[index].routeGraph!.warnings);
    }
    checkAbort(signal);
    if (rangeFiles.size) warnings.push(`${rangeFiles.size} files were inspected through indexed class/interface ranges; imports and code outside those ranges were not analyzed.`);
    return { topology: buildContainerTopology(projects), projects, warnings: [...new Set(warnings)], filesRead };
}
