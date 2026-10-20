/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-intelligence/src/node/provider/cypher.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert uebernommen:
 * jede Spalte, jeder Deckel und jede Escape-Regel ist hier dieselbe, weil eine
 * abgewandelte Frage eine andere Antwort ergibt und der Unterschied genau die
 * Sorte Abweichung waere, die erst in einer Zahl auf dem Bildschirm auffaellt.
 * Aenderungen gegenueber dem Original: keine. Die Datei hat keine Importe, der
 * Transport steht woanders.
 */
/**
 * Every graph query CodeAtlas sends, in one file.
 *
 * The frozen acceptance test asserts that no other source file contains a
 * query. That is not tidiness for its own sake: it is what makes the engine
 * replaceable. A second provider reimplements this file and nothing else in
 * the intelligence extension has to change shape.
 *
 * Three properties of the 0.9.0 query subset shape what can be written here:
 *
 *  - Supported clauses are MATCH, WHERE, RETURN, ORDER BY and LIMIT. There is
 *    no WITH, no COLLECT, no OPTIONAL MATCH and no labels() function, so a
 *    node's label cannot be returned as a value.
 *  - A pattern carries at most one label, so a query that needs several labels
 *    runs several times. That is why resolution issues one query per label.
 *  - An unknown property returns the empty string rather than failing, so a
 *    typo in a property name reads as "no data" instead of as an error. The
 *    property names below are therefore constants, never inline literals.
 */

/** Node properties read by the queries in this file. */
export const NODE_PROPS = {
    name: 'name',
    qualifiedName: 'qualified_name',
    filePath: 'file_path',
    startLine: 'start_line',
    endLine: 'end_line',
    isTest: 'is_test',
    isExported: 'is_exported',
    envKey: 'env_key',
    complexity: 'complexity',
    cognitive: 'cognitive',
    loopDepth: 'loop_depth',
    allocInLoop: 'alloc_in_loop',
    linearScanInLoop: 'linear_scan_in_loop',
    unguardedRecursion: 'unguarded_recursion'
} as const;

/** Relation properties read by the queries in this file. */
export const REL_PROPS = {
    /** Last recorded site of a relation, 1-based. Relations are deduplicated per pair. */
    line: 'line',
    strategy: 'strategy'
} as const;

/**
 * Relation names. Kept here so no other file has to spell them.
 *
 * There are two error relations rather than one because the engine records two.
 * `RAISES` is what the TypeScript and JavaScript analyses write for a `throw`;
 * `THROWS` is what the Java analysis writes for a declared `throws` clause. A
 * product that read only the first would tell a Java reader that a method
 * declaring `throws NoAuthorizationException` raises nothing, which is a
 * finding CodeAtlas never made. Both are read and the results are merged.
 */
export const RELATIONS = {
    invocation: 'CALLS',
    raise: 'RAISES',
    throwDeclaration: 'THROWS',
    environment: 'CONFIGURES',
    typeReference: 'USAGE',
    /**
     * One file importing another.
     *
     * Written by the 0.9.0 analysis for every language whose modules declare
     * their dependencies, and confirmed by the schema canary. The source of an
     * edge is the importing module and the target is the imported file; both
     * ends carry `file_path`, which is what makes a file-level dependency graph
     * readable without a second lookup per node.
     */
    moduleImport: 'IMPORTS'
} as const;

/**
 * Labels tried when resolving a caret position, ordered narrowest first. The
 * order is also the tie-break when two candidates span the same number of
 * lines: a method body wins over the function that shares its extent, which
 * wins over the class that contains it.
 */
export const ENCLOSING_LABELS = ['Method', 'Function', 'Class'] as const;
export type EnclosingLabel = typeof ENCLOSING_LABELS[number];

/** Label of the nodes that stand for indexed files. */
export const FILE_LABEL = 'File';

/** Label of the nodes that stand for environment values. */
export const ENV_LABEL = 'EnvVar';

/** Label whose call targets are constructions rather than plain invocations. */
export const CLASS_LABEL = 'Class';

/** Default row ceiling. Fact lists longer than this are truncated by the UI anyway. */
export const DEFAULT_LIMIT = 200;

/**
 * Make a value safe to embed in a double-quoted literal. Backslashes first,
 * then quotes, then the control characters that would end the statement early.
 */
export function escapeLiteral(value: string): string {
    let out = '';
    for (const char of value) {
        const code = char.codePointAt(0) ?? 0;
        if (code < 0x20 || code === 0x7f) {
            // Control characters have no meaning in a symbol name or a path and
            // would end the statement early, so they are dropped.
            continue;
        }
        if (char === '\\' || char === '"') {
            out += `\\${char}`;
            continue;
        }
        out += char;
    }
    return out;
}

/** Reject a line number that could not have come from an editor. */
function safeLine(oneBasedLine: number): number {
    if (!Number.isFinite(oneBasedLine)) {
        return 0;
    }
    return Math.max(0, Math.floor(oneBasedLine));
}

function safeLimit(limit: number): number {
    if (!Number.isFinite(limit) || limit <= 0) {
        return DEFAULT_LIMIT;
    }
    return Math.min(Math.floor(limit), 5000);
}

/** Columns each builder returns, in order. Consumers key rows by these. */
export const COLUMNS = {
    enclosing: [
        `n.${NODE_PROPS.name}`,
        `n.${NODE_PROPS.qualifiedName}`,
        `n.${NODE_PROPS.startLine}`,
        `n.${NODE_PROPS.endLine}`
    ],
    fileExists: [`n.${NODE_PROPS.name}`, `n.${NODE_PROPS.filePath}`],
    files: [`n.${NODE_PROPS.filePath}`],
    /**
     * Der Modul-Knoten einer Datei: sein Name, und die Spanne, die er
     * beansprucht. Die Spanne ist der Grund fuer diese Abfrage: sie ist die
     * einzige Stelle, an der steht, wie lang die Datei wirklich ist, und ohne
     * sie waere ein gekappter Schnipsel nicht von einer ganzen Datei zu
     * unterscheiden.
     */
    moduleForFile: [
        `n.${NODE_PROPS.qualifiedName}`,
        `n.${NODE_PROPS.startLine}`,
        `n.${NODE_PROPS.endLine}`
    ],
    /** Der qualifizierte Name des File-Knotens einer Datei. */
    fileNode: [`n.${NODE_PROPS.qualifiedName}`],
    callsOut: [
        `b.${NODE_PROPS.name}`,
        `b.${NODE_PROPS.qualifiedName}`,
        `b.${NODE_PROPS.filePath}`,
        `b.${NODE_PROPS.startLine}`,
        `r.${REL_PROPS.line}`
    ],
    classTargets: [`b.${NODE_PROPS.name}`, `b.${NODE_PROPS.qualifiedName}`],
    callsIn: [
        `a.${NODE_PROPS.name}`,
        `a.${NODE_PROPS.qualifiedName}`,
        `a.${NODE_PROPS.filePath}`,
        `a.${NODE_PROPS.startLine}`,
        `a.${NODE_PROPS.isTest}`,
        `r.${REL_PROPS.line}`
    ],
    raises: [
        `b.${NODE_PROPS.name}`,
        `b.${NODE_PROPS.filePath}`,
        `b.${NODE_PROPS.startLine}`,
        `r.${REL_PROPS.line}`
    ],
    envReads: [
        `b.${NODE_PROPS.name}`,
        `b.${NODE_PROPS.envKey}`,
        `b.${NODE_PROPS.filePath}`,
        `r.${REL_PROPS.line}`,
        `r.${REL_PROPS.strategy}`
    ],
    typeRefs: [
        `b.${NODE_PROPS.name}`,
        `b.${NODE_PROPS.qualifiedName}`,
        `b.${NODE_PROPS.filePath}`,
        `b.${NODE_PROPS.startLine}`,
        `r.${REL_PROPS.line}`
    ],
    /** One import edge, as two workspace-relative paths: the importer and the imported. */
    imports: [`a.${NODE_PROPS.filePath}`, `b.${NODE_PROPS.filePath}`],
    hotspots: [
        `n.${NODE_PROPS.name}`,
        `n.${NODE_PROPS.qualifiedName}`,
        `n.${NODE_PROPS.filePath}`,
        `n.${NODE_PROPS.startLine}`,
        `n.${NODE_PROPS.complexity}`,
        `n.${NODE_PROPS.cognitive}`,
        `n.${NODE_PROPS.loopDepth}`,
        `n.${NODE_PROPS.allocInLoop}`,
        `n.${NODE_PROPS.linearScanInLoop}`,
        `n.${NODE_PROPS.unguardedRecursion}`
    ],
    /**
     * Declarations of one label inside a set of files. The same columns as the
     * enclosing lookup plus the file, because the caller is asking about several
     * files at once and has to be able to tell the rows apart.
     */
    declarations: [
        `n.${NODE_PROPS.name}`,
        `n.${NODE_PROPS.qualifiedName}`,
        `n.${NODE_PROPS.filePath}`,
        `n.${NODE_PROPS.startLine}`,
        `n.${NODE_PROPS.endLine}`,
        `n.${NODE_PROPS.isTest}`
    ]
} as const;

const ret = (columns: readonly string[]): string => `RETURN ${columns.join(', ')}`;

/**
 * A disjunction over one property, or undefined when there is nothing to ask.
 *
 * `IN [...]` parses at 0.9.0, but an OR chain is what every other query in this
 * file is built from and it needs no assumption about list literal support in a
 * subset that has neither WITH nor COLLECT. The values are escaped exactly as a
 * single-valued predicate would escape them.
 */
function anyOf(alias: string, property: string, values: readonly string[]): string | undefined {
    const unique = [...new Set(values.filter(value => value.length > 0))];
    if (unique.length === 0) {
        return undefined;
    }
    return unique.map(value => `${alias}.${property} = "${escapeLiteral(value)}"`).join(' OR ');
}

/**
 * Symbols of one label whose declaration span covers `oneBasedLine` in
 * `filePath`. Ordered so the innermost candidate of that label comes first.
 */
export function enclosingByLabel(label: string, filePath: string, oneBasedLine: number): string {
    const line = safeLine(oneBasedLine);
    return `MATCH (n:${label}) WHERE n.${NODE_PROPS.filePath} = "${escapeLiteral(filePath)}"`
        + ` AND n.${NODE_PROPS.startLine} <= ${line} AND n.${NODE_PROPS.endLine} >= ${line}`
        + ` ${ret(COLUMNS.enclosing)}`
        + ` ORDER BY n.${NODE_PROPS.startLine} DESC LIMIT 10`;
}

/** How many files one listing returns. A repository past this is read partially, not wrongly. */
export const FILE_LIST_LIMIT = 5000;

/**
 * Every file the index holds, workspace-relative.
 *
 * The whole-project summary carries a file tree, but it is a shallow one: on a
 * large repository it names the top of the tree and stops, so a caller that
 * used it as a file list would silently read a fraction of the project. The
 * file nodes are the complete answer.
 */
export function indexedFiles(limit = FILE_LIST_LIMIT): string {
    return `MATCH (n:${FILE_LABEL}) ${ret(COLUMNS.files)} LIMIT ${safeLimit(limit)}`;
}

/**
 * How many import edges one sweep returns.
 *
 * The same ceiling as the file listing, which is also the highest a query in
 * this file may ask for. A repository past it is read partially rather than
 * wrongly: the consumer records how many edges it was given, so a dependency
 * order built from a truncated sweep says so instead of passing itself off as
 * the whole graph.
 */
export const IMPORT_EDGE_LIMIT = FILE_LIST_LIMIT;

/**
 * Every import edge in the project, as a pair of workspace-relative paths.
 *
 * One sweep rather than a reader per file, and that is the whole reason this is
 * the only import query in the file. The consumer is the tour generator, which
 * topologically sorts the entire dependency graph: it needs every edge before
 * it can place the first file, so a per-file `importsOf` would be one round
 * trip per file to assemble something a single query already returns. A
 * neighbourhood reader would be dead weight until something asks a
 * neighbourhood question.
 *
 * Unordered, and deliberately so. The 0.9.0 analysis writes one edge per import
 * *statement*, so a file that imports three names from one module appears three
 * times; the caller deduplicates on the pair and sorts, which makes the answer
 * independent of the order the engine happens to return rows in.
 */
export function importEdges(limit = IMPORT_EDGE_LIMIT): string {
    return `MATCH (a)-[r:${RELATIONS.moduleImport}]->(b) ${ret(COLUMNS.imports)} LIMIT ${safeLimit(limit)}`;
}

/**
 * Das Label des Knotens, der eine ganze Datei umspannt.
 *
 * Zwei Labels beschreiben dieselbe Datei und sie sind nicht dasselbe: `File`
 * ist der Eintrag im Datei-Inventar, `Module` ist die Datei als Symbol, mit
 * Spanne und mit Quelltext hinter `get_code_snippet`. Der Reader braucht das
 * zweite.
 */
export const MODULE_LABEL = 'Module';

/**
 * Der Modul-Knoten einer Datei, mit seiner Zeilenspanne.
 *
 * Diese Abfrage ist der Gegenbeweis zur Ableitung in src/app/module-qn.ts: der
 * Name laesst sich aus dem Pfad ausrechnen, aber ausgerechnet ist nicht
 * nachgesehen. Der Reader rechnet, fragt und nimmt die Antwort, wenn beide
 * auseinandergehen.
 */
export function moduleForFile(filePath: string): string {
    return `MATCH (n:${MODULE_LABEL}) WHERE n.${NODE_PROPS.filePath} = "${escapeLiteral(filePath)}"`
        + ` ${ret(COLUMNS.moduleForFile)} LIMIT 1`;
}

/**
 * Der File-Knoten einer Datei, nur sein qualifizierter Name.
 *
 * Der Rueckfallweg, wenn der Graph keinen Modul-Knoten fuehrt: sein Name ist
 * der des Moduls plus Endung plus `.__file__`, und daraus laesst sich der
 * Modul-Name zurueckrechnen (siehe moduleQnFromFileQn). Das ist eine schwaechere
 * Auskunft als der Modul-Knoten selbst, denn eine Zeilenspanne kommt so nicht
 * mit, und genau deshalb ist es der zweite Weg und nicht der erste.
 */
export function fileNodeForPath(filePath: string): string {
    return `MATCH (n:${FILE_LABEL}) WHERE n.${NODE_PROPS.filePath} = "${escapeLiteral(filePath)}"`
        + ` ${ret(COLUMNS.fileNode)} LIMIT 1`;
}

/** Whether the engine has a file node for `filePath`. Distinguishes "nothing here" from "not indexed". */
export function fileExists(filePath: string): string {
    return `MATCH (n:${FILE_LABEL}) WHERE n.${NODE_PROPS.filePath} = "${escapeLiteral(filePath)}"`
        + ` ${ret(COLUMNS.fileExists)} LIMIT 1`;
}

/** Outgoing invocations of a symbol, one row per distinct target. */
export function callsOut(qualifiedName: string, limit = DEFAULT_LIMIT): string {
    return `MATCH (a)-[r:${RELATIONS.invocation}]->(b)`
        + ` WHERE a.${NODE_PROPS.qualifiedName} = "${escapeLiteral(qualifiedName)}"`
        + ` ${ret(COLUMNS.callsOut)}`
        + ` ORDER BY r.${REL_PROPS.line} LIMIT ${safeLimit(limit)}`;
}

/**
 * Outgoing invocations whose target is a class. The engine records a
 * construction as an invocation of the class, so this second query is what
 * separates `new Foo()` from `foo()` without a labels() function.
 */
export function classCallTargets(qualifiedName: string, limit = DEFAULT_LIMIT): string {
    return `MATCH (a)-[r:${RELATIONS.invocation}]->(b:${CLASS_LABEL})`
        + ` WHERE a.${NODE_PROPS.qualifiedName} = "${escapeLiteral(qualifiedName)}"`
        + ` ${ret(COLUMNS.classTargets)} LIMIT ${safeLimit(limit)}`;
}

/**
 * Incoming invocations of a symbol. Returns the source's test flag so the
 * tested-by inference can run without a second query.
 */
export function callsIn(qualifiedName: string, limit = DEFAULT_LIMIT): string {
    return `MATCH (a)-[r:${RELATIONS.invocation}]->(b)`
        + ` WHERE b.${NODE_PROPS.qualifiedName} = "${escapeLiteral(qualifiedName)}"`
        + ` ${ret(COLUMNS.callsIn)}`
        + ` ORDER BY r.${REL_PROPS.line} LIMIT ${safeLimit(limit)}`;
}

/** Error types a symbol raises. */
export function raises(qualifiedName: string, limit = DEFAULT_LIMIT): string {
    return `MATCH (a)-[r:${RELATIONS.raise}]->(b)`
        + ` WHERE a.${NODE_PROPS.qualifiedName} = "${escapeLiteral(qualifiedName)}"`
        + ` ${ret(COLUMNS.raises)} LIMIT ${safeLimit(limit)}`;
}

/**
 * Error types a symbol declares it throws.
 *
 * The same columns as {@link raises}, on purpose: the two relations describe the
 * same product fact in two languages and the caller merges the rows. A single
 * query cannot cover both, because a pattern carries at most one relation type.
 */
export function throwsRelation(qualifiedName: string, limit = DEFAULT_LIMIT): string {
    return `MATCH (a)-[r:${RELATIONS.throwDeclaration}]->(b)`
        + ` WHERE a.${NODE_PROPS.qualifiedName} = "${escapeLiteral(qualifiedName)}"`
        + ` ${ret(COLUMNS.raises)} LIMIT ${safeLimit(limit)}`;
}

/**
 * Environment values a symbol reads. The engine models an environment read as
 * a configuration relation to a dedicated node, not as a variable reference,
 * which is why this is its own query rather than a filter on type references.
 */
export function envReads(qualifiedName: string, limit = DEFAULT_LIMIT): string {
    return `MATCH (a)-[r:${RELATIONS.environment}]->(b:${ENV_LABEL})`
        + ` WHERE a.${NODE_PROPS.qualifiedName} = "${escapeLiteral(qualifiedName)}"`
        + ` ${ret(COLUMNS.envReads)} LIMIT ${safeLimit(limit)}`;
}

/** Labels whose nodes carry the per-symbol complexity readings. */
export const CALLABLE_LABELS = ['Function', 'Method'] as const;

/** How many callables one hotspot sweep reads. Ranking happens in the provider. */
export const HOTSPOT_SCAN_LIMIT = 1500;

/**
 * Every callable of one label with the readings that make a symbol worth
 * reading first.
 *
 * Unordered on purpose. The 0.9.0 answer is all strings, so `ORDER BY` on a
 * numeric property sorts `"10"` before `"9"`; ranking is therefore done by the
 * caller on coerced numbers. The limit is what keeps this a bounded sweep on a
 * large repository rather than a download of the whole graph.
 */
export function hotspotCandidates(label: string, limit = HOTSPOT_SCAN_LIMIT): string {
    return `MATCH (n:${label}) ${ret(COLUMNS.hotspots)} LIMIT ${safeLimit(limit)}`;
}

/** How many symbols one batched complexity read asks about. */
export const COMPLEXITY_BATCH_LIMIT = 200;

/**
 * The complexity readings of a named set of symbols, one label at a time.
 *
 * The same columns as {@link hotspotCandidates}, because it is the same reading
 * asked of a set rather than of a label. Returns undefined when the set is
 * empty: a query with no predicate would match every callable in the project,
 * which is the sweep this function exists to avoid.
 */
export function complexityOf(
    label: string,
    qualifiedNames: readonly string[],
    limit = COMPLEXITY_BATCH_LIMIT
): string | undefined {
    const predicate = anyOf('n', NODE_PROPS.qualifiedName, qualifiedNames);
    if (predicate === undefined) {
        return undefined;
    }
    return `MATCH (n:${label}) WHERE ${predicate} ${ret(COLUMNS.hotspots)} LIMIT ${safeLimit(limit)}`;
}

/** How many declarations one batched file read returns. */
export const DECLARATION_BATCH_LIMIT = 1000;

/**
 * Symbols of one label declared in any of a set of files.
 *
 * This is the lookup that gives a change set its identities. The change tool
 * names an affected symbol and the file it sits in and nothing else, so a
 * product that wants to ask a follow-up question about that symbol, or to open
 * it, has to recover the qualified name and the declaration line from the index
 * itself. One query per label rather than one per symbol: a change set of forty
 * files is two round trips this way and eighty the other.
 */
export function declarationsInFiles(
    label: string,
    filePaths: readonly string[],
    limit = DECLARATION_BATCH_LIMIT
): string | undefined {
    const predicate = anyOf('n', NODE_PROPS.filePath, filePaths);
    if (predicate === undefined) {
        return undefined;
    }
    return `MATCH (n:${label}) WHERE ${predicate} ${ret(COLUMNS.declarations)} LIMIT ${safeLimit(limit)}`;
}

/** How many callers one batched inbound read returns. */
export const CALLER_BATCH_LIMIT = 500;

/**
 * Everything that calls any symbol in a set, in one query.
 *
 * The walk a change assessment needs is breadth first and unlabelled: what
 * calls this set, then what calls that set. Asking per symbol would be one round
 * trip per row of a list that grows with every step; asking per level is one
 * round trip per step. What is given up is which member of the set each caller
 * reached, and nothing above this needs it: the distance is a property of the
 * level, not of the edge.
 */
export function callersOfAny(qualifiedNames: readonly string[], limit = CALLER_BATCH_LIMIT): string | undefined {
    const predicate = anyOf('b', NODE_PROPS.qualifiedName, qualifiedNames);
    if (predicate === undefined) {
        return undefined;
    }
    return `MATCH (a)-[r:${RELATIONS.invocation}]->(b) WHERE ${predicate}`
        + ` ${ret(COLUMNS.callsIn)} LIMIT ${safeLimit(limit)}`;
}

/**
 * Types a symbol references. This relation covers type positions only; local
 * variable reads and writes are not recorded by the 0.9.0 engine at all, which
 * is why the provider declares no read/write capability.
 */
export function typeRefs(qualifiedName: string, limit = DEFAULT_LIMIT): string {
    return `MATCH (a)-[r:${RELATIONS.typeReference}]->(b)`
        + ` WHERE a.${NODE_PROPS.qualifiedName} = "${escapeLiteral(qualifiedName)}"`
        + ` ${ret(COLUMNS.typeRefs)} LIMIT ${safeLimit(limit)}`;
}
