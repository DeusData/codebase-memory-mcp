/**
 * Woher die Imports-Gruppe ihre Daten bekommt.
 *
 * Im Referenzprojekt (CodeAtlasIDE) ist das eine Zeile: der Twin ruft
 * `pseudocode-service.importsFor(symbol, [ir])`, der Dienst ruft
 * `provider.fileImports(root, relativePath)`, und ein Theia-Backend setzt die
 * Antwort aus zwei Lesungen zusammen. Dieses Projekt hat kein solches Backend.
 * Der C-Server bietet auf /rpc kein Werkzeug an, das die Importe einer Datei mit
 * Namen und Zeilen zurueckgibt (INVENTAR.md Abschnitt 3, Ask 2 auf der
 * Upstream-Liste). Also wird derselbe DTO hier aus den zwei Lesungen gebaut,
 * die es schon gibt, und diese Datei ist genau diese duenne Beschaffung.
 *
 * **Die beiden Lesungen, und warum es zwei sein muessen.**
 *
 * Die erste ist die Analyse: `moduleDependencies` liefert Datei-zu-Datei-Kanten.
 * Das ist ein Befund des Index, und mehr als zwei Pfade steht nicht drin: kein
 * importierter Name, keine Zeile, und gar keine Kante fuer ein Modul, das
 * ausserhalb des Arbeitsbereichs aufloest.
 *
 * Die zweite ist der Text der Datei selbst, den der Reader ohnehin schon
 * geladen hat. Er traegt den Namen, die Zeile und die Form der Anweisung, und er
 * ist **keine** Aussage der Analyse. Darum traegt jeder so gewonnene Eintrag
 * `origin: 'source'` und eine Evidenz mit `source: 'source-text'`, bis auf den
 * Bildschirm durch.
 *
 * **Was hier ausdruecklich nicht passiert.** Es wird nicht geparst. Ein Scanner
 * ueber Text kann eine Import-Anweisung in einem Blockkommentar oder in einem
 * Template-String nicht von einer echten unterscheiden, und er kennt nur die
 * Schreibweisen, die unten stehen. Beides ist der Grund, warum die Gruppe
 * niemals "unused import" sagt: die Aussage ueber die Benutzung kommt aus den
 * Fakten des Index, nie aus diesem Scanner, und dieser Scanner liefert nur die
 * Liste der Namen, ueber die gefragt wird.
 *
 * Rein bis auf {@link fileImportsFor}, das eine Provider-Lesung braucht.
 */

import type { ModuleDependencyGraph, ProviderQueryOptions } from '../core/intelligence-provider';
import type { IrFactSource } from '../core/intelligence-provider';
import type { Evidence } from '../core/semantic-ir';
import type { SemanticIR } from '../core/semantic-ir';
import type { ResolveResult } from '../core/intelligence-provider';
import { buildIr } from '../ir/semantic-ir-builder';

import type { FileImportRef, FileImportsDto } from './imports-group';

/** Der Bezeichner, unter dem eine Text-Lesung dieser Datei zitiert wird. */
export const IMPORT_STATEMENT_RELATION = 'import-statement';

/** Die Strategie, die auf der Evidenz steht: gelesen, nicht analysiert. */
export const IMPORT_STATEMENT_STRATEGY = 'import-statement-read';

/**
 * Wie viele Anweisungen aus einer Datei gelesen werden.
 *
 * Weit ueber dem, was eine gewachsene Quelldatei importiert, und weit unter dem
 * Punkt, an dem ein Scanner ueber Text spuerbar kostet. Wird er erreicht, sagt
 * der DTO `truncated`, und die Gruppe sagt es weiter.
 */
export const MAX_IMPORTS_PER_FILE = 60;

/** Die Endungen, deren Import-Schreibweise dieser Scanner kennt. */
export const IMPORT_SOURCE_EXTENSIONS = ['.js', '.jsx', '.mjs', '.cjs', '.ts', '.tsx', '.mts', '.cts'];

/** Die Endungen, unter denen ein Spezifizierer auf eine indizierte Datei zeigen kann. */
const RESOLVE_EXTENSIONS = ['.ts', '.tsx', '.mts', '.cts', '.js', '.jsx', '.mjs', '.cjs'];

/** True when this reader has a pattern family for the file. */
export function isImportSource(filePath: string): boolean {
    const lower = filePath.toLowerCase();
    return IMPORT_SOURCE_EXTENSIONS.some((extension) => lower.endsWith(extension));
}

/**
 * Eine `import ... from '...'`-Anweisung, samt ihrer Klausel.
 *
 * Absichtlich eine Anweisung je Treffer und nicht ein Name: die Zeile gehoert
 * der Anweisung, und drei Namen aus einer Klausel stehen alle auf derselben.
 */
const IMPORT_STATEMENT = /(^|\n)[^\S\n]*import\s+(type\s+)?([^;\n]*?)\s*from\s*(['"])([^'"\n]+)\4/g;

/** `import './register';` und `import 'polyfill';`: bindet nichts. */
const SIDE_EFFECT_STATEMENT = /(^|\n)[^\S\n]*import\s*(['"])([^'"\n]+)\2/g;

/** `* as db` */
const NAMESPACE_CLAUSE = /^\*\s*as\s+([A-Za-z_$][\w$]*)$/;

/** `{ a, b as c }` */
const NAMED_CLAUSE = /\{([^}]*)\}/;

/** 1-based line the character at `index` sits on. */
function lineAt(source: string, index: number): number {
    let line = 1;
    for (let at = 0; at < index && at < source.length; at++) {
        if (source[at] === '\n') {
            line += 1;
        }
    }
    return line;
}

/** Die Evidenz einer Text-Lesung: die schwaechste, die dieses Produkt ausstellt. */
function statementCitation(filePath: string, line: number, providerId: string, generation: number): Evidence {
    return {
        source: 'source-text',
        relation: IMPORT_STATEMENT_RELATION,
        file: filePath,
        range: { startLine: line, endLine: line },
        strategy: IMPORT_STATEMENT_STRATEGY,
        engineGeneration: generation,
        providerId,
    };
}

/** Was eine Text-Lesung mitbekommt, damit ihre Zitate zuordenbar bleiben. */
export interface ImportReadContext {
    /** Workspace-relativer Pfad der gelesenen Datei, fuer das Zitat. */
    filePath: string;
    providerId: string;
    engineGeneration: number;
}

/**
 * Die Import-Anweisungen einer Datei, aus ihrem Text.
 *
 * Eine leere Liste heisst "in diesem Text stand keine Anweisung, die dieser
 * Scanner kennt", nie "diese Datei importiert nichts". Den Unterschied traegt
 * `sourceRead` im DTO.
 */
export function readImportStatements(source: string, context: ImportReadContext): FileImportRef[] {
    if (!isImportSource(context.filePath)) {
        return [];
    }
    const out: FileImportRef[] = [];
    const withStatement = new Set<number>();
    const cite = (line: number): Evidence[] => [
        statementCitation(context.filePath, line, context.providerId, context.engineGeneration),
    ];

    for (const match of source.matchAll(IMPORT_STATEMENT)) {
        const at = (match.index ?? 0) + (match[1]?.length ?? 0);
        const line = lineAt(source, at);
        withStatement.add(line);
        const clause = match[3].trim();
        const module = match[5];
        const evidence = cite(line);

        const namespace = NAMESPACE_CLAUSE.exec(clause);
        if (namespace !== null) {
            out.push({ alias: namespace[1], module, line, namespace: true, origin: 'source', evidence });
            continue;
        }

        const named = NAMED_CLAUSE.exec(clause);
        // Was vor der Klammer steht, ist die Vorgabe-Bindung. Sie wird unter dem
        // Namen gefuehrt, den die Datei ihr gibt, und nicht unter `default`: der
        // Index zeichnet einen Aufruf ueber sie unter genau diesem Namen auf,
        // und ein Eintrag namens `default` waere gegen nichts abgleichbar.
        const head = (named === null ? clause : clause.slice(0, named.index)).replace(/,\s*$/, '').trim();
        if (head.length > 0 && /^[A-Za-z_$][\w$]*$/.test(head)) {
            out.push({ name: head, module, line, origin: 'source', evidence });
        }
        if (named === null) {
            continue;
        }
        for (const part of named[1].split(',')) {
            const cleaned = part.replace(/^\s*type\s+/, '').trim();
            if (cleaned.length === 0) {
                continue;
            }
            const aliased = /^([A-Za-z_$][\w$]*)\s+as\s+([A-Za-z_$][\w$]*)$/.exec(cleaned);
            if (aliased !== null) {
                out.push({ name: aliased[1], alias: aliased[2], module, line, origin: 'source', evidence });
                continue;
            }
            if (/^[A-Za-z_$][\w$]*$/.test(cleaned)) {
                out.push({ name: cleaned, module, line, origin: 'source', evidence });
            }
        }
    }

    for (const match of source.matchAll(SIDE_EFFECT_STATEMENT)) {
        const at = (match.index ?? 0) + (match[1]?.length ?? 0);
        const line = lineAt(source, at);
        // Dieselbe Zeile hat schon eine Klausel geliefert: dann war das hier
        // nur der vordere Teil derselben Anweisung.
        if (withStatement.has(line)) {
            continue;
        }
        out.push({ module: match[3], line, origin: 'source', evidence: cite(line) });
    }

    return out.sort((a, b) => (a.line ?? 0) - (b.line ?? 0));
}

/** Der Verzeichnisteil eines workspace-relativen Pfades, ohne Schluss-Schraegstrich. */
function directoryOf(filePath: string): string {
    const at = filePath.lastIndexOf('/');
    return at < 0 ? '' : filePath.slice(0, at);
}

/** `a/b/../c` zu `a/c`. Rein textlich; es wird nichts am Dateisystem geprueft. */
function normalizePath(path: string): string {
    const out: string[] = [];
    for (const segment of path.split('/')) {
        if (segment === '' || segment === '.') {
            continue;
        }
        if (segment === '..') {
            out.pop();
            continue;
        }
        out.push(segment);
    }
    return out.join('/');
}

/**
 * Welche indizierte Datei ein Spezifizierer meint, wenn eine davon passt.
 *
 * Nur relative Spezifizierer, und nur ueber die Endungen oben. Ein Paketname
 * loest ausserhalb des Arbeitsbereichs auf; ihm einen Pfad zuzuordnen waere
 * geraten. Passt nichts, traegt der Eintrag keinen `targetPath`, und das ist
 * die richtige Antwort.
 */
export function resolveImportTarget(
    filePath: string,
    module: string,
    indexedTargets: readonly string[],
): string | undefined {
    if (!module.startsWith('.')) {
        return undefined;
    }
    const base = normalizePath(`${directoryOf(filePath)}/${module}`);
    const candidates = [base, ...RESOLVE_EXTENSIONS.map((extension) => `${base}${extension}`),
        ...RESOLVE_EXTENSIONS.map((extension) => `${base}/index${extension}`)];
    return candidates.find((candidate) => indexedTargets.includes(candidate));
}

/** Der Ausschnitt des Providers, den diese Beschaffung anfasst, und nichts sonst. */
export interface ImportsSource {
    readonly id: string;
    moduleDependencies(root: string, opts?: ProviderQueryOptions): Promise<ModuleDependencyGraph>;
    resolveSymbolAt?: (
        root: string,
        filePath: string,
        oneBasedLine: number,
        opts?: ProviderQueryOptions,
    ) => Promise<ResolveResult>;
    getFacts?: IrFactSource['getFacts'];
    getSnippet?: IrFactSource['getSnippet'];
}

/** Lines that can introduce a callable top-level declaration in supported source. */
function callableDeclarationLines(source: string): number[] {
    const lines: number[] = [];
    source.split('\n').forEach((line, index) => {
        if (/^\s*(?:export\s+)?(?:async\s+)?function\s+[A-Za-z_$][\w$]*/.test(line)) {
            lines.push(index + 1);
        }
    });
    return lines;
}

/** Fetch the same fact families for every callable in the file, or no file reading at all. */
async function fileIrsFor(
    source: ImportsSource,
    root: string,
    request: FileImportsRequest,
): Promise<SemanticIR[] | undefined> {
    if (request.source === undefined
        || source.resolveSymbolAt === undefined
        || source.getFacts === undefined
        || source.getSnippet === undefined) {
        return undefined;
    }
    const refs = await Promise.all(callableDeclarationLines(request.source).map(async (line) => {
        const answer = await source.resolveSymbolAt!(root, request.filePath, line, request.opts).catch(() => undefined);
        return answer?.kind === 'ok' ? answer.symbol : undefined;
    }));
    // A partial collection would make a negative statement about the file
    // unsound. Keep no `fileIrs` rather than silently judging the subset.
    if (refs.some((ref) => ref === undefined)) {
        return undefined;
    }
    const unique = [...new Map(refs
        .filter((ref): ref is NonNullable<typeof ref> => ref !== undefined)
        .map((ref) => [ref.qualifiedName ?? `${ref.name}:${ref.range.start.line}`, ref])).values()];
    const provider = source as ImportsSource & IrFactSource;
    const built = await Promise.all(unique.map((ref) => buildIr(provider, root, ref, request.opts).catch(() => undefined)));
    if (built.some((entry) => entry === undefined)) {
        return undefined;
    }
    return built.map((entry) => entry!.ir);
}

/** Was der Aufrufer beisteuert: den Text, falls er ihn hat. */
export interface FileImportsRequest {
    /** Workspace-relativer Pfad der Datei, ueber die gefragt wird. */
    filePath: string;
    /**
     * Der Quelltext der Datei, wenn der Reader ihn schon geladen hat.
     *
     * Undefined heisst "nicht gelesen", nicht "leer": der DTO sagt es mit
     * `sourceRead: false`, und die Gruppe zeigt dann die Kanten des Index und
     * behauptet ueber keinen Namen etwas.
     */
    source?: string | undefined;
    engineGeneration?: number;
    opts?: ProviderQueryOptions;
}

/**
 * Die Antwort, die im Referenzprojekt `provider.fileImports` gibt.
 *
 * Zuerst die Kanten des Index, dann der Text. Ein Ziel, das der Text nicht
 * benannt hat, bleibt als Eintrag mit `origin: 'index'` stehen: der Index hat
 * die Abhaengigkeit gesehen, und sie wegzulassen, weil der Scanner sie nicht
 * wiedererkannt hat, waere eine Liste, die stiller ist als der Befund.
 */
export async function fileImportsFor(
    source: ImportsSource,
    root: string,
    request: FileImportsRequest,
): Promise<FileImportsDto> {
    const graph = await source
        .moduleDependencies(root, request.opts)
        .catch(() => ({ edges: [], truncated: false }) as ModuleDependencyGraph);
    const indexedTargets = [...new Set(
        graph.edges.filter((edge) => edge.from === request.filePath).map((edge) => edge.to),
    )].sort();

    const text = request.source;
    const read = text === undefined
        ? []
        : readImportStatements(text, {
            filePath: request.filePath,
            providerId: source.id,
            engineGeneration: request.engineGeneration ?? 1,
        });
    const entries = read.slice(0, MAX_IMPORTS_PER_FILE).map((entry) => {
        const targetPath = resolveImportTarget(request.filePath, entry.module, indexedTargets);
        return targetPath === undefined ? entry : { ...entry, targetPath };
    });

    const named = new Set(entries.map((entry) => entry.targetPath).filter((path): path is string => path !== undefined));
    for (const target of indexedTargets) {
        if (named.has(target)) {
            continue;
        }
        entries.push({
            module: target,
            targetPath: target,
            origin: 'index',
            evidence: [{
                source: 'graph-edge',
                relation: 'IMPORTS',
                file: request.filePath,
                engineGeneration: request.engineGeneration ?? 1,
                providerId: source.id,
            }],
        });
    }

    const fileIrs = await fileIrsFor(source, root, request);
    return {
        entries,
        truncated: graph.truncated || read.length > MAX_IMPORTS_PER_FILE,
        indexedTargets,
        sourceRead: text !== undefined,
        ...(fileIrs === undefined ? {} : { fileIrs }),
    };
}
