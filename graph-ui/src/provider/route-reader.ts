/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-intelligence/src/node/provider/route-reader.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert uebernommen.
 * Die Datei ist rein: Text rein, Funde raus, kein Dateisystem und keine Engine.
 * Genau deshalb laeuft sie im Browser unveraendert; wer den Quelltext liefert,
 * entscheidet der Provider. Aenderungen gegenueber dem Original: keine.
 */
/**
 * Recovering HTTP routes from source text, for the languages the index does not.
 *
 * This file exists because of a gap, and the gap is worth stating plainly. The
 * 0.9.0 analysis writes route nodes for annotation-driven Java and reports them
 * under the `routes` aspect. For TypeScript, JavaScript and Python it writes
 * none at all: a file that registers three routes produces three call edges and
 * nothing that names a method or a path. A workspace map that showed only what
 * the index reports would therefore tell a Node or a Flask reader that their
 * application has no entry points, which is the confident-empty-list failure
 * this product exists to avoid.
 *
 * So the routes are read off the text instead, and every route recovered this
 * way is marked `source` rather than `index` all the way to the screen. Three
 * rules keep that reading honest rather than clever:
 *
 *  1. **A literal or nothing.** Only a registration whose path is a string
 *     literal is reported. `router.get(basePath + '/users', ...)` is invisible
 *     here, and inventing `basePath` would be a guess about a value.
 *  2. **A path, not a key.** A candidate is only a route when the literal
 *     starts with `/`. Without that rule `cache.get('user')` and
 *     `map.delete(id)` become routes, and a map full of imaginary endpoints is
 *     worse than a map with none.
 *  3. **No handler is named here.** The registration's line is reported and
 *     nothing else. Which symbol encloses that line is a question for the
 *     index, asked when a reader clicks the row, because that answer is a
 *     reading and this file only has text.
 *
 * Everything below is pure: text in, findings out. No file system, no engine.
 */

/** One route registration found in one file. */
export interface SourceRoute {
    /** HTTP method in upper case, absent when the registration does not name one. */
    method?: string;
    /** The path exactly as written, parameters and all. */
    path: string;
    /** 1-based line of the registration. */
    line: number;
}

/** Verbs a router object exposes as methods, in JavaScript, TypeScript and FastAPI. */
const VERB_CALLS = 'get|post|put|patch|delete|head|options|all';

/** Registration through a router method: `app.get('/users', handler)`. */
const VERB_CALL_PATTERN = new RegExp(
    `\\.(${VERB_CALLS})\\s*\\(\\s*(['"\`])(/[^'"\`\\n]*)\\2`,
    'g'
);

/** Flask and Django style decorator: `@bp.route('/users', methods=['POST'])`. */
const DECORATOR_ROUTE_PATTERN = /@\s*[\w.]*\.?route\s*\(\s*(['"])(\/[^'"\n]*)\1([^)\n]*)/g;

/** The `methods=[...]` argument of a decorator route, which may name several verbs. */
const DECORATOR_METHODS_PATTERN = /methods\s*=\s*\[([^\]]*)\]/;

/** Spring style annotation: `@GetMapping("/articles")`, `@RequestMapping(value = "/x")`. */
const ANNOTATION_PATTERN = /@(Get|Post|Put|Patch|Delete|Request)Mapping\s*\(([^)]*)\)/g;

/** The first string literal inside an annotation's arguments, which is its path. */
const ANNOTATION_PATH_PATTERN = /"([^"\n]*)"/;

/** The verb a `@RequestMapping` names, when it names one. */
const ANNOTATION_METHOD_PATTERN = /RequestMethod\.([A-Z]+)/;

/** Extensions whose route registrations look like method calls or decorators. */
const SCRIPT_EXTENSIONS = ['.js', '.jsx', '.mjs', '.cjs', '.ts', '.tsx', '.mts', '.cts', '.py'];

/** Extensions whose route registrations look like annotations. */
const ANNOTATION_EXTENSIONS = ['.java', '.kt'];

/** Every extension this reader knows how to look at. Anything else is skipped whole. */
export const ROUTE_SOURCE_EXTENSIONS = [...SCRIPT_EXTENSIONS, ...ANNOTATION_EXTENSIONS];

/** How many routes one file may contribute. A file past this is a generated table. */
export const MAX_ROUTES_PER_FILE = 40;

/** True when this reader has a pattern family for the file. */
export function isRouteSource(filePath: string): boolean {
    const lower = filePath.toLowerCase();
    return ROUTE_SOURCE_EXTENSIONS.some(extension => lower.endsWith(extension));
}

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

/**
 * Routes registered in one file.
 *
 * Returns an empty list for a file whose extension this reader does not know,
 * which is the same answer as a file it read and found nothing in. The
 * difference matters to the caller and not to the reader: the provider reports
 * both as "no route recovered here", and the capability is what says whether
 * that means anything.
 */
export function readRoutes(source: string, filePath: string): SourceRoute[] {
    if (!isRouteSource(filePath)) {
        return [];
    }
    const lower = filePath.toLowerCase();
    const found = ANNOTATION_EXTENSIONS.some(extension => lower.endsWith(extension))
        ? readAnnotationRoutes(source)
        : readScriptRoutes(source);
    return dedupe(found).slice(0, MAX_ROUTES_PER_FILE);
}

/** Method calls and decorators, which is how JavaScript, TypeScript and Python register. */
function readScriptRoutes(source: string): SourceRoute[] {
    const routes: SourceRoute[] = [];
    for (const match of source.matchAll(VERB_CALL_PATTERN)) {
        routes.push({
            method: match[1].toUpperCase(),
            path: match[3],
            line: lineAt(source, match.index ?? 0)
        });
    }
    for (const match of source.matchAll(DECORATOR_ROUTE_PATTERN)) {
        const line = lineAt(source, match.index ?? 0);
        const methods = DECORATOR_METHODS_PATTERN.exec(match[3] ?? '');
        if (!methods) {
            // A decorator with no `methods=` accepts GET in every framework that
            // spells it this way, but this reader is not the place to encode
            // that: the honest answer is that the registration names no verb.
            routes.push({ path: match[2], line });
            continue;
        }
        for (const verb of methods[1].split(',')) {
            const cleaned = verb.replace(/['"\s]/g, '').toUpperCase();
            if (cleaned.length > 0) {
                routes.push({ method: cleaned, path: match[2], line });
            }
        }
    }
    return routes;
}

/**
 * Annotations, which is how Spring and its relatives register.
 *
 * Two annotations are declined, and both for the same reason: what they carry
 * is not a route.
 *
 * A mapping that names no verb is a class-level prefix. `@RequestMapping(path =
 * "/articles")` above a controller is the first half of every route in the file
 * and an endpoint of nothing on its own, so putting `/articles` on the map as a
 * way in would be describing an address that does not answer.
 *
 * A mapping whose path does not start with `/` is the second half. `@GetMapping
 * (path = "feed")` is `/articles/feed` once the prefix above it is applied, and
 * this reader has only text: joining the two would mean deciding which of the
 * annotations in the file is the class-level one, which is a parse and not a
 * scan. Reporting `feed` on its own would be worse than reporting nothing.
 *
 * What both cases have in common is that the index reports these routes whole
 * for exactly the languages where they appear, so declining them loses nothing.
 */
function readAnnotationRoutes(source: string): SourceRoute[] {
    const routes: SourceRoute[] = [];
    for (const match of source.matchAll(ANNOTATION_PATTERN)) {
        const args = match[2] ?? '';
        const path = ANNOTATION_PATH_PATTERN.exec(args)?.[1];
        const verb = match[1] === 'Request'
            ? ANNOTATION_METHOD_PATTERN.exec(args)?.[1]
            : match[1].toUpperCase();
        if (path === undefined || !path.startsWith('/') || verb === undefined) {
            continue;
        }
        routes.push({ method: verb, path, line: lineAt(source, match.index ?? 0) });
    }
    return routes;
}

/** One registration reported once, even when two patterns both matched it. */
function dedupe(routes: SourceRoute[]): SourceRoute[] {
    const seen = new Set<string>();
    const out: SourceRoute[] = [];
    for (const route of routes) {
        const key = `${route.method ?? ''} ${route.path} ${route.line}`;
        if (seen.has(key)) {
            continue;
        }
        seen.add(key);
        out.push(route);
    }
    return out;
}
