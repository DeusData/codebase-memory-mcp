import { isAlias, isMap, isScalar, isSeq, LineCounter, parseDocument } from 'yaml';
import type { RouteGraphSnapshot } from './route-graph-source';

export interface ServiceProject {
    project: string;
    rootPath: string;
    manifest: string;
    sources: ReadonlyMap<string, string>;
    files: readonly string[];
    routeGraph?: RouteGraphSnapshot;
}
export interface ServiceEvidence { project: string; path: string; line: number; summary: string }
export interface ContainerService {
    id: string;
    name: string;
    project: string;
    manifest: string;
    line: number;
    image?: string;
    buildContext?: string;
    dockerfile?: string;
    sourcePaths: string[];
    networks: string[];
    ports: string[];
    profiles?: string[];
}
export interface ContainerConnection {
    id: string;
    source: string;
    target: string;
    kind: 'call' | 'configuration' | 'startup';
    protocol: string;
    evidence: ServiceEvidence[];
}
export interface ContainerTopology {
    services: ContainerService[];
    connections: ContainerConnection[];
    warnings: string[];
    unresolved: ServiceEvidence[];
}

const MAX_MANIFEST = 256 * 1024;
const MAX_SOURCE = 256 * 1024;
const MAX_TOTAL_SOURCE = 4 * 1024 * 1024;
const MAX_FILES = 4_000;
const MAX_SERVICES = 128;
const MAX_CONNECTIONS = 2048;
const NAME = /^[a-zA-Z0-9][a-zA-Z0-9_.-]*$/;
const ENDPOINT_KEY = /(?:HOST|HOSTNAME|ADDR|ADDRESS|ENDPOINT|URL|URI|SERVER|SERVERS|CONNECTION(?:_?STRING)?|BROKERS?|BOOTSTRAP_SERVERS)$/i;
const PRIVATE_KEY = /PASSWORD|SECRET|TOKEN|CREDENTIAL|API_?KEY/i;
const CODE_FILE = /\.(?:[cm]?[jt]sx?|py|go|rs|java|kt|kts|cs|fs|rb|php|[ch](?:pp|xx)?|scala|swift)$/i;
const str = (node: unknown): string | undefined => isScalar(node)
    && ['string', 'number', 'boolean'].includes(typeof node.value) ? String(node.value) : undefined;
const entries = (node: unknown): [string, unknown][] => isMap(node)
    ? node.items.flatMap(pair => { const key = str(pair.key); return key === undefined ? [] : [[key, pair.value]]; }) : [];
const field = (node: unknown, key: string): unknown => entries(node).find(entry => entry[0] === key)?.[1];
const list = (node: unknown): string[] => isSeq(node) ? node.items.flatMap(item => str(item) ?? []) : [];
const tidy = (value: string): string => value.replace(/[\u0000-\u001f\u007f]/g, '').slice(0, 180);
const unique = <T>(values: T[]): T[] => [...new Set(values)];
const idPart = (value: string): string => encodeURIComponent(value);

function normal(path: string): string {
    const absolute = path.startsWith('/') || /^[A-Za-z]:[\\/]/.test(path);
    const result: string[] = [];
    for (const part of path.replaceAll('\\', '/').split('/')) {
        if (!part || part === '.') continue;
        if (part === '..' && result.length && result.at(-1) !== '..') result.pop();
        else if (part !== '..' || !absolute) result.push(part);
    }
    return `${path.startsWith('/') ? '/' : ''}${result.join('/')}`;
}
const directory = (path: string): string => normal(path).slice(0, normal(path).lastIndexOf('/'));
export const normalizeSourcePath = (root: string, path: string): string => normal(path.startsWith('/') || /^[A-Za-z]:[\\/]/.test(path)
    ? path : `${root}/${path}`);
const absolute = normalizeSourcePath;
const inside = (path: string, parent: string): boolean => path === parent || path.startsWith(`${parent}/`);
const dynamic = (value: string): boolean => /\$|\{\{|\}\}|%[A-Z_]+%/.test(value);
function lineOf(node: unknown, counter: LineCounter): number {
    if (node && typeof node === 'object' && 'range' in node && Array.isArray(node.range)) {
        return counter.linePos(Number(node.range[0])).line;
    }
    return 1;
}
interface FileSource { project: ServiceProject; path: string; absolute: string; text?: string }
interface ParsedService {
    view: ContainerService;
    input: ServiceProject;
    node: unknown;
    counter: LineCounter;
    aliases: Map<string, string[]>;
    environment: Map<string, { value: string; line: number }>;
    prefixes: string[];
}
interface Endpoint { host: string; protocol: string }

function protocolFor(hint: string): string {
    if (/redis/i.test(hint)) return 'redis';
    if (/postgres|pgsql/i.test(hint)) return 'postgres';
    if (/mysql/i.test(hint)) return 'mysql';
    if (/mongo/i.test(hint)) return 'mongodb';
    if (/kafka|broker|bootstrap/i.test(hint)) return 'kafka';
    if (/amqp|rabbit/i.test(hint)) return 'amqp';
    if (/grpc/i.test(hint)) return 'grpc';
    if (/http|fetch|axios|urlopen|requests\./i.test(hint)) return 'http';
    return 'tcp';
}
/** Values are examined only in endpoint-bearing configuration or network-call arguments. */
function endpoints(value: string, hint: string, bare: boolean): Endpoint[] {
    if (dynamic(value) || value.length > 4096) return [];
    const result: Endpoint[] = [];
    for (const match of value.matchAll(/\b(https?|grpcs?|rediss?|postgres(?:ql)?|mysql|mongodb(?:\+srv)?|amqps?|kafka):\/\/(?:[^\s/@]*@)?([a-zA-Z0-9][a-zA-Z0-9_.-]*)(?=[:/?#\s]|$)/g)) {
        result.push({ host: match[2].toLowerCase(), protocol: match[1].replace('postgresql', 'postgres') });
    }
    if (result.length) return result;
    const connectionHost = /(?:^|;)\s*(?:Server|Host|Data Source)\s*=\s*([a-zA-Z0-9][a-zA-Z0-9_.-]*)(?:[:,]\d+)?\s*(?:;|$)/i.exec(value);
    if (connectionHost) return [{ host: connectionHost[1].toLowerCase(), protocol: protocolFor(hint) }];
    if (bare) for (const part of value.split(',')) {
        const match = /^\s*([a-zA-Z0-9][a-zA-Z0-9_.-]*)(?::\d+)?\s*$/.exec(part);
        if (match) result.push({ host: match[1].toLowerCase(), protocol: protocolFor(hint) });
    }
    return result;
}

function validateDocument(root: unknown): void {
    let count = 0;
    const walk = (node: unknown, depth: number): void => {
        if (++count > 15_000 || depth > 40) throw new Error('YAML structure exceeds the analysis limit');
        if (isAlias(node)) throw new Error('YAML aliases are not supported');
        if (isMap(node)) for (const pair of node.items) {
            if (str(pair.key) === '<<') throw new Error('YAML merge keys are not supported');
            if (!isScalar(pair.key) || typeof pair.key.value !== 'string') throw new Error('YAML mapping keys must be strings');
            walk(pair.value, depth + 1);
        }
        else if (isSeq(node)) for (const item of node.items) walk(item, depth + 1);
    };
    walk(root, 0);
}

function parseServices(input: ServiceProject, warnings: string[]): ParsedService[] {
    if (!input.manifest) return [];
    const manifestPath = absolute(input.rootPath, input.manifest);
    const source = [...input.sources].find(([path]) => absolute(input.rootPath, path) === manifestPath)?.[1];
    const warn = (message: string): void => { warnings.push(`${tidy(input.project)}/${tidy(input.manifest)}: ${message}.`); };
    if (source === undefined) { warn('Compose source is unavailable'); return []; }
    if (source.length > MAX_MANIFEST) { warn('Compose source exceeds the 256 KiB analysis limit'); return []; }
    const counter = new LineCounter();
    let document;
    try {
        document = parseDocument(source, { lineCounter: counter, uniqueKeys: true, merge: false, prettyErrors: false });
        // Parser diagnostics can contain source values. Never copy them into the public result.
        if (document.errors.length || document.warnings.length) { warn('Compose YAML is invalid or uses unsupported syntax'); return []; }
        validateDocument(document.contents);
    } catch (error) {
        warn(error instanceof Error && error.message.startsWith('YAML ') ? error.message : 'Compose YAML could not be safely parsed');
        return [];
    }
    if (field(document.contents, 'include') !== undefined) { warn('Compose include is unsupported; this manifest was omitted'); return []; }
    const serviceMap = field(document.contents, 'services');
    if (!isMap(serviceMap)) { warn('No services mapping was found'); return []; }
    const networkMap = field(document.contents, 'networks');
    const scope = `${idPart(input.project)}:${idPart(normal(input.manifest))}`;
    const services: ParsedService[] = [];
    for (const [name, node] of entries(serviceMap).slice(0, MAX_SERVICES)) {
        if (!NAME.test(name) || !isMap(node)) { warn('A service declaration has an invalid name or shape'); continue; }
        if (field(node, 'extends') !== undefined) { warn(`Service ${tidy(name)} uses unsupported extends and was omitted`); continue; }
        const networkMode = str(field(node, 'network_mode'));
        const networkNode = field(node, 'networks');
        const networkKeys = networkMode ? [] : networkNode === undefined ? ['default']
            : isSeq(networkNode) ? list(networkNode) : entries(networkNode).map(([key]) => key);
        const aliases = new Map<string, string[]>();
        const networks: string[] = [];
        for (const key of networkKeys) {
            const config = field(networkMap, key);
            const configuredName = str(field(config, 'name'));
            const external = str(field(config, 'external')) === 'true';
            const networkName = configuredName ?? key;
            if (dynamic(networkName) || !NAME.test(networkName)) { warn(`Service ${name} has an unresolved network name`); continue; }
            const identity = configuredName || external ? `shared:${networkName}` : `local:${scope}:${key}`;
            networks.push(identity);
            const containerName = str(field(node, 'container_name'));
            const declaredAliases = list(field(field(networkNode, key), 'aliases'));
            aliases.set(identity, unique([name, ...(containerName && NAME.test(containerName) ? [containerName] : []),
                ...declaredAliases.filter(alias => NAME.test(alias) && !dynamic(alias))].map(alias => alias.toLowerCase())));
        }
        if (networkMode) warn(`Service ${name} uses network_mode; its Docker DNS links are not inferred`);
        const build = field(node, 'build');
        const context = str(build) ?? str(field(build, 'context')) ?? (isMap(build) ? '.' : undefined);
        const localContext = context !== undefined && !dynamic(context) && !/^[a-zA-Z]+:\/\/|^git@/.test(context)
            ? absolute(directory(manifestPath), context) : undefined;
        if (context && !localContext) warn(`Service ${name} has a remote or unresolved build context`);
        const dockerfileName = str(field(build, 'dockerfile')) ?? 'Dockerfile';
        const dockerfile = localContext && !dynamic(dockerfileName) ? absolute(localContext, dockerfileName) : undefined;
        const environment = new Map<string, { value: string; line: number }>();
        const envNode = field(node, 'environment');
        for (const [key, value] of entries(envNode)) {
            const scalar = str(value);
            if (!PRIVATE_KEY.test(key) && scalar !== undefined) environment.set(key, { value: scalar, line: lineOf(value, counter) });
        }
        if (isSeq(envNode)) for (const item of envNode.items) {
            const value = str(item), index = value?.indexOf('=') ?? -1;
            if (value && index > 0 && !PRIVATE_KEY.test(value.slice(0, index))) {
                environment.set(value.slice(0, index), { value: value.slice(index + 1), line: lineOf(item, counter) });
            }
        }
        if (field(node, 'env_file') !== undefined) warn(`Service ${name} uses env_file; external environment values are not read`);
        const portsNode = field(node, 'ports');
        const ports = isSeq(portsNode) ? portsNode.items.flatMap(port => {
            const value = str(port);
            if (value && !dynamic(value) && /^[\d.:/a-z[\]-]+$/i.test(value)) return [value];
            const published = str(field(port, 'published')), target = str(field(port, 'target'));
            const protocol = str(field(port, 'protocol')) ?? 'tcp';
            return target && /^\d+$/.test(target) && (!published || /^\d+(?:-\d+)?$/.test(published)) && /^(tcp|udp)$/.test(protocol)
                ? [`${published ? `${published}:` : ''}${target}/${protocol}`] : [];
        }) : [];
        const image = str(field(node, 'image'));
        services.push({ input, node, counter, environment, aliases, prefixes: [], view: {
            id: `service:${scope}:${idPart(name)}`, name, project: input.project, manifest: input.manifest,
            line: lineOf(node, counter), image: image && !dynamic(image) ? tidy(image) : undefined,
            buildContext: localContext, dockerfile, sourcePaths: [], networks: unique(networks), ports,
            profiles: list(field(node, 'profiles')).filter(profile => NAME.test(profile)),
        } });
    }
    if (entries(serviceMap).length > MAX_SERVICES) warn(`Only the first ${MAX_SERVICES} service declarations were analyzed`);
    return services;
}

/** A root build context is ownership evidence only when COPY names a specific local subtree/file. */
function copyPrefixes(text: string, context: string): string[] {
    const result: string[] = [];
    for (const line of text.split('\n')) {
        const match = /^\s*COPY\s+(.+)$/i.exec(line);
        if (!match || /--from(?:=|\s)/i.test(match[1])) continue;
        const args = match[1].replace(/--[\w-]+=\S+\s*/g, '').trim();
        let words: string[];
        try {
            const parsed: unknown = args.startsWith('[') ? JSON.parse(args) : args.match(/"[^"]*"|'[^']*'|\S+/g);
            if (!Array.isArray(parsed) || !parsed.every(value => typeof value === 'string')) continue;
            words = (parsed as string[]).map(value => value.replace(/^["']|["']$/g, ''));
        } catch { continue; }
        for (const path of words.slice(0, -1)) {
            if (dynamic(path) || /[*?\[\]]|^\.\.?\/?$|^\/|^https?:/.test(path)) continue;
            const resolved = absolute(context, path);
            if (resolved !== context && inside(resolved, context)) result.push(resolved);
        }
    }
    return unique(result);
}

interface Literal { start: number; end: number; value: string }
/** Keep offsets, erase comments and literals from syntax matching, and never evaluate source. */
function sourceSyntax(source: string): { code: string; literals: Literal[] } {
    const output = source.split('');
    const literals: Literal[] = [];
    let index = 0;
    while (index < source.length) {
        const start = index;
        if (source[index] === '#' || source.slice(index, index + 2) === '//') {
            while (index < source.length && source[index] !== '\n') output[index++] = ' ';
        } else if (source.slice(index, index + 2) === '/*') {
            const end = source.indexOf('*/', index + 2);
            index = end < 0 ? source.length : end + 2;
            for (let i = start; i < index; i++) if (source[i] !== '\n') output[i] = ' ';
        } else if ('"\'`'.includes(source[index])) {
            const quote = source[index++];
            while (index < source.length) {
                if (source[index] === '\\') { index += 2; continue; }
                if (source[index++] === quote) break;
            }
            index = Math.min(index, source.length);
            if (source[index - 1] === quote) literals.push({ start, end: index, value: source.slice(start + 1, index - 1).replace(/\\(["'\\])/g, '$1') });
            for (let i = start; i < index; i++) if (source[i] !== '\n') output[i] = ' ';
        } else index++;
    }
    return { code: output.join(''), literals };
}
interface Call { name: string; start: number; open: number; end: number; firstEnd: number }
function callsIn(code: string): Call[] {
    const closing = new Map<number, number>(), stack: number[] = [];
    for (let i = 0; i < code.length; i++) {
        if (code[i] === '(') stack.push(i);
        else if (code[i] === ')' && stack.length) closing.set(stack.pop()!, i);
    }
    const calls: Call[] = [];
    for (const match of code.matchAll(/\b([A-Za-z_$][\w$]*(?:\s*\.\s*[A-Za-z_$][\w$]*)*)\s*\(/g)) {
        const open = match.index + match[0].lastIndexOf('('), end = closing.get(open);
        if (end === undefined || end - open > 8192) continue;
        let nesting = 0, firstEnd = end;
        for (let i = open + 1; i < end; i++) {
            if ('([{'.includes(code[i])) nesting++;
            else if (')]}'.includes(code[i])) nesting--;
            else if (code[i] === ',' && nesting === 0) { firstEnd = i; break; }
        }
        calls.push({ name: match[1].replaceAll(/\s/g, ''), start: match.index, open, end, firstEnd });
    }
    return calls;
}
function networkCall(name: string): boolean {
    return /^(?:fetch|axios|urlopen|Redis|StrictRedis|MongoClient|Pool|NpgsqlConnection|SqlConnection|Kafka|KafkaProducer|KafkaConsumer)$/i.test(name)
        || /^(?:redis\.(?:Redis|StrictRedis)|pg\.Pool|pymongo\.MongoClient)$/.test(name)
        || /^(?:requests|http|https|axios|urllib\.request|urllib3|grpc|redis|psycopg2?|pymysql|mysql|amqp|amqplib|net|socket|ConnectionMultiplexer|Dns)\./i.test(name)
            && /(?:get|post|put|patch|delete|request|urlopen|dial|newclient|connect|connectasync|createclient|createconnection|insecure_channel|secure_channel|gethostentry(?:async)?)$/i.test(name);
}
const wordIn = (code: string, name: string): boolean => new RegExp(`\\b${name.replace(/[$]/g, '\\$&')}\\b`).test(code);
interface Wrapper { parameter: number; hint: string }
/** Bounded local argument propagation to known transport APIs, not a function-name dictionary. */
function connectionWrappers(code: string, calls: Call[]): Map<string, Wrapper> {
    const result = new Map<string, Wrapper>();
    const definitions = new Set<string>(), ambiguous = new Set<string>();
    for (const candidate of calls) {
        const after = /^\s*\{/.exec(code.slice(candidate.end + 1));
        if (!after || candidate.name.includes('.') || /^(if|for|while|switch|catch)$/.test(candidate.name)) continue;
        if (definitions.has(candidate.name)) { ambiguous.add(candidate.name); result.delete(candidate.name); continue; }
        definitions.add(candidate.name);
        const bodyStart = candidate.end + 1 + after[0].length;
        let depth = 1, bodyEnd = bodyStart;
        while (bodyEnd < code.length && bodyEnd - bodyStart < 16_384 && depth) {
            if (code[bodyEnd] === '{') depth++;
            if (code[bodyEnd++] === '}') depth--;
        }
        if (depth) continue;
        const body = code.slice(bodyStart, bodyEnd);
        const sinks = calls.filter(call => call.start >= bodyStart && call.end < bodyEnd && networkCall(call.name));
        if (!sinks.length) continue;
        const parameters = code.slice(candidate.open + 1, candidate.end).split(',').map(part => part.trim().match(/[A-Za-z_$][\w$]*$/)?.[0]);
        for (let parameter = 0; parameter < parameters.length; parameter++) {
            const name = parameters[parameter];
            if (!name) continue;
            const tainted = new Set([name]);
            for (let pass = 0; pass < 3; pass++) {
                for (const assignment of body.matchAll(/\b([A-Za-z_$][\w$]*)\s*=\s*([^;\n]+)[;\n]/g)) {
                    if ([...tainted].some(value => wordIn(assignment[2], value))) tainted.add(assignment[1]);
                }
            }
            const sink = sinks.find(call => [...tainted].some(value => wordIn(code.slice(call.open + 1, call.firstEnd), value)));
            if (sink) { result.set(candidate.name, { parameter, hint: sink.name }); break; }
        }
    }
    for (const name of ambiguous) result.delete(name);
    return result;
}
interface SourceEndpoint { endpoint: Endpoint; line: number; via: string }

interface StaticString { prefix: string; complete: boolean }
interface JavaScope { start: number; end: number; parent?: JavaScope; method?: Call; parameters: string[] }
interface JavaBinding { name: string; start: number; expressionStart: number; end: number; declaration: boolean; scope: JavaScope }
interface JavaReceiver { name: string; type: string; start: number; end: number; scope: JavaScope }

/** Small static string interpreter for typed Spring call sites, never a general Java evaluator. */
function springEndpoints(text: string, code: string, literals: Literal[], calls: Call[]): SourceEndpoint[] {
    const literalAt = new Map(literals.map(literal => [literal.start, literal]));
    const literalEnding = new Set(literals.map(literal => literal.end));
    const delimiters = new Map<number, number>(), delimiterStack: number[] = [];
    for (let index = 0; index < code.length; index++) {
        if ('([{'.includes(code[index])) delimiterStack.push(index);
        else if (')]}'.includes(code[index]) && delimiterStack.length) delimiters.set(delimiterStack.pop()!, index);
    }
    const root: JavaScope = { start: -1, end: code.length, parameters: [] };
    const scopes: JavaScope[] = [root], stack = [root];
    const methodsAt = new Map<number, Call>();
    for (const call of calls) {
        const following = /^\s*\{/.exec(code.slice(call.end + 1));
        if (following && !call.name.includes('.') && !/^(?:if|for|while|switch|catch|synchronized)$/.test(call.name)) {
            methodsAt.set(call.end + following[0].length, call);
        }
    }
    for (let index = 0; index < code.length; index++) {
        if (code[index] === '{') {
            const method = methodsAt.get(index);
            const parameters = method ? code.slice(method.open + 1, method.end).split(',')
                .flatMap(parameter => parameter.trim().match(/([A-Za-z_$][\w$]*)\s*(?:\[\])?$/)?.[1] ?? []) : [];
            const scope: JavaScope = { start: index, end: delimiters.get(index) ?? code.length,
                parent: stack.at(-1), method, parameters };
            scopes.push(scope); stack.push(scope);
        } else if (code[index] === '}' && stack.length > 1) stack.pop();
    }
    const scopeAt = (position: number): JavaScope => {
        let result = root;
        for (const scope of scopes) if (scope.start < position && position < scope.end && scope.start > result.start) result = scope;
        return result;
    };
    const containsScope = (parent: JavaScope, child: JavaScope): boolean => parent.start <= child.start && child.end <= parent.end;
    const trim = (start: number, end: number): [number, number] => {
        while (start < end && /\s/.test(code[start]) && !literalAt.has(start)) start++;
        while (end > start && /\s/.test(code[end - 1]) && !literalEnding.has(end)) end--;
        return [start, end];
    };
    const split = (start: number, end: number, separator: string): [number, number][] => {
        const parts: [number, number][] = [];
        let beginning = start;
        for (let index = start; index < end; index++) {
            const close = delimiters.get(index);
            if (close !== undefined && close < end) { index = close; continue; }
            if (code[index] === separator) { parts.push([beginning, index]); beginning = index + 1; }
        }
        parts.push([beginning, end]);
        return parts;
    };
    const bindings: JavaBinding[] = [];
    for (const match of code.matchAll(/(?<![\w$.])(?:(?:final\s+)?(?:String|URI|URL|var)\s+)?(?:this\.)?([A-Za-z_$][\w$]*)\s*=(?!=)\s*/g)) {
        const expressionStart = match.index + match[0].lastIndexOf('=') + 1;
        const end = code.indexOf(';', expressionStart);
        if (end < 0 || end - expressionStart > 8192) continue;
        bindings.push({ name: match[1], start: match.index, expressionStart, end,
            declaration: /^(?:final\s+)?(?:String|URI|URL|var)\s/.test(match[0]), scope: scopeAt(match.index) });
    }
    const receivers: JavaReceiver[] = [];
    for (const match of code.matchAll(/\b(RestTemplate|RestOperations|WebClient)\s+([A-Za-z_$][\w$]*)\b/g)) {
        receivers.push({ type: match[1], name: match[2], start: match.index,
            end: match.index + match[0].length, scope: scopeAt(match.index) });
    }
    const receiverAt = (name: string, position: number): JavaReceiver | undefined => receivers
        .filter(receiver => receiver.name === name && receiver.start < position && containsScope(receiver.scope, scopeAt(position)))
        .sort((left, right) => right.scope.start - left.scope.start || right.start - left.start)[0];
    const helpers = new Map<string, { scope: JavaScope; start: number; end: number }>();
    const methodNames = new Set<string>(), ambiguousHelpers = new Set<string>();
    for (const scope of scopes) {
        const method = scope.method;
        if (!method) continue;
        if (methodNames.has(method.name)) ambiguousHelpers.add(method.name);
        methodNames.add(method.name);
        if (!/\bString\s+$/.test(code.slice(Math.max(0, method.start - 120), method.start))) continue;
        const body = code.slice(scope.start + 1, scope.end);
        const returned = /^\s*return\b([^;]*);\s*$/.exec(body);
        if (returned) {
            const start = scope.start + 1 + returned[0].indexOf('return') + 6;
            helpers.set(method.name, { scope, start, end: code.indexOf(';', start) });
        }
    }
    for (const name of ambiguousHelpers) helpers.delete(name);
    let budget = 20_000;
    const unknown = (): StaticString => ({ prefix: '', complete: false });
    const evaluate = (start: number, end: number, before: number, scope: JavaScope,
        parameters = new Map<string, StaticString>(), depth = 0): StaticString => {
        if (--budget < 0 || depth > 10 || end - start > 8192) return unknown();
        [start, end] = trim(start, end);
        if (start >= end) return unknown();
        if (code[start] === '(' && delimiters.get(start) === end - 1) return evaluate(start + 1, end - 1, before, scope, parameters, depth + 1);
        const parts = split(start, end, '+');
        if (parts.length > 1) {
            let prefix = '';
            for (const [partStart, partEnd] of parts) {
                const part = evaluate(partStart, partEnd, before, scope, parameters, depth + 1);
                prefix += part.prefix;
                if (prefix.length > 4096) return unknown();
                if (!part.complete) return { prefix, complete: false };
            }
            return { prefix, complete: true };
        }
        const literal = literalAt.get(start);
        if (literal?.end === end) return { prefix: literal.value, complete: true };
        const expression = code.slice(start, end);
        const variable = /^(?:this\.)?([A-Za-z_$][\w$]*)$/.exec(expression);
        if (variable) {
            const name = variable[1];
            if (parameters.has(name)) return parameters.get(name)!;
            const visible = bindings.filter(binding => binding.name === name && binding.start < before && containsScope(binding.scope, scope));
            const declaration = visible.filter(binding => binding.declaration)
                .sort((left, right) => right.scope.start - left.scope.start || right.start - left.start)[0];
            for (let current: JavaScope | undefined = scope; current; current = current.parent) {
                if (current.parameters.includes(name) && (!declaration || declaration.scope.start <= current.start)) return unknown();
            }
            if (!declaration) return unknown();
            // Writes in a conditional/loop cannot be ordered as one unconditional destination.
            if (bindings.some(binding => binding.name === name && !binding.declaration && binding.start > declaration.start
                && binding.start < before && containsScope(declaration.scope, binding.scope) && binding.scope !== declaration.scope)) return unknown();
            const binding = visible.filter(candidate => candidate.scope === declaration.scope && candidate.start >= declaration.start)
                .sort((left, right) => right.start - left.start)[0];
            return binding ? evaluate(binding.expressionStart, binding.end, binding.start, binding.scope, parameters, depth + 1) : unknown();
        }
        const invocation = /^(?:new\s+)?((?:this\.)?[A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)*)\s*\(/.exec(expression);
        if (invocation) {
            const open = start + invocation[0].lastIndexOf('(');
            if (delimiters.get(open) !== end - 1) return unknown();
            const argumentsList = split(open + 1, end - 1, ',');
            if (/^(?:URI\.create|URI|URL)$/.test(invocation[1]) && argumentsList.length === 1) {
                return evaluate(...argumentsList[0], before, scope, parameters, depth + 1);
            }
            const helper = helpers.get(invocation[1].replace(/^this\./, ''));
            if (helper && helper.scope.parameters.length === argumentsList.length) {
                const values = new Map<string, StaticString>();
                argumentsList.forEach(([a, b], index) => values.set(helper.scope.parameters[index], evaluate(a, b, before, scope, parameters, depth + 1)));
                return evaluate(helper.start, helper.end, helper.start, helper.scope, values, depth + 1);
            }
        }
        return unknown();
    };
    const result: SourceEndpoint[] = [];
    const emit = (value: StaticString, position: number, via: string): void => {
        // A runtime suffix is safe only after the entire URI authority is visibly delimited.
        if (!value.complete && !/^https?:\/\/[^/?#\s]+[/?#]/.test(value.prefix)) return;
        for (const endpoint of endpoints(value.prefix, 'http', false)) result.push({ endpoint,
            line: text.slice(0, position).split('\n').length, via });
    };
    for (const call of calls) {
        const method = /^(?:this\.)?([A-Za-z_$][\w$]*)\.(getForObject|getForEntity|postForObject|postForEntity|postForLocation|put|patchForObject|delete|exchange|execute|optionsForAllow|headForHeaders)$/.exec(call.name);
        if (!method) continue;
        const receiver = receiverAt(method[1], call.start);
        if (!receiver || !/^Rest(?:Template|Operations)$/.test(receiver.type)) continue;
        emit(evaluate(call.open + 1, call.firstEnd, call.start, scopeAt(call.start)), call.start, 'Spring HTTP call');
    }
    for (const match of code.matchAll(/\b((?:this\.)?[A-Za-z_$][\w$]*)\s*\.\s*(?:get|post|put|patch|delete|head|options)\s*\(\s*\)\s*\.\s*uri\s*\(/g)) {
        const receiver = receiverAt(match[1].replace(/^this\./, ''), match.index);
        if (receiver?.type !== 'WebClient') continue;
        const open = match.index + match[0].lastIndexOf('('), call = calls.find(candidate => candidate.open === open);
        if (!call) continue;
        let value = evaluate(open + 1, call.firstEnd, match.index, scopeAt(match.index));
        if (value.prefix.startsWith('/')) {
            const initializer = /^\s*=\s*WebClient\s*\.\s*create\s*\(/.exec(code.slice(receiver.end));
            if (!initializer) continue;
            const baseOpen = receiver.end + initializer[0].lastIndexOf('('), baseEnd = delimiters.get(baseOpen);
            if (baseEnd === undefined) continue;
            const base = evaluate(baseOpen + 1, baseEnd, receiver.start, receiver.scope);
            if (!base.complete) continue;
            value = { prefix: base.prefix.replace(/\/$/, '') + value.prefix, complete: value.complete };
        }
        emit(value, match.index, 'Spring WebClient HTTP request');
    }
    return result;
}
function codeEndpoints(text: string, service: ParsedService, path: string): SourceEndpoint[] {
    const { code, literals } = sourceSyntax(text);
    const calls = callsIn(code), wrappers = connectionWrappers(code, calls);
    const found: SourceEndpoint[] = /\.java$/i.test(path) ? springEndpoints(text, code, literals, calls) : [];
    for (const call of calls) {
        const wrapper = wrappers.get(call.name);
        if (!networkCall(call.name) && !wrapper) continue;
        // Function declarations are not executed calls. Only first-argument wrappers are supported.
        if (/^\s*\{/.test(code.slice(call.end + 1)) || (wrapper && wrapper.parameter !== 0)) continue;
        const hint = wrapper?.hint ?? call.name;
        const argument = text.slice(call.open + 1, call.firstEnd);
        let low = 0, high = literals.length;
        while (low < high) { const mid = (low + high) >>> 1; if (literals[mid].start <= call.open) low = mid + 1; else high = mid; }
        for (let index = low; index < literals.length && literals[index].start < call.firstEnd; index++) {
            const literal = literals[index];
            if (literal.end > call.firstEnd) continue;
            const prefix = code.slice(call.open + 1, literal.start);
            const hasField = /\b(?:host|hostname|address|addr|url|uri|endpoint|connectionString|connection_string|server)\s*[:=]\s*$/i.test(prefix);
            const firstValue = !prefix.trim();
            if (!firstValue && !hasField) continue;
            for (const endpoint of endpoints(literal.value, hint, hasField || Boolean(wrapper) || !/fetch|axios|requests|http|urlopen/i.test(hint))) {
                found.push({ endpoint, line: text.slice(0, literal.start).split('\n').length, via: wrapper ? 'local connection wrapper' : 'network call' });
            }
        }
        // Explicit Compose values can resolve a directly used environment variable; no shell expansion or defaults.
        for (const match of argument.matchAll(/(?:process\.env\.([A-Za-z_]\w*)|(?:getenv|GetEnvironmentVariable|Getenv|env::var|env\.var)\s*\(\s*["']([A-Za-z_]\w*)["']|process\.env\[\s*["']([A-Za-z_]\w*)["'])/g)) {
            const key = match[1] ?? match[2] ?? match[3], configured = service.environment.get(key);
            if (!configured || !ENDPOINT_KEY.test(key)) continue;
            for (const endpoint of endpoints(configured.value, key, true)) found.push({ endpoint,
                line: text.slice(0, call.start).split('\n').length, via: `network call using ${tidy(key)}` });
        }
    }
    return found;
}

/** Read-only static evidence. No Docker, shell expansion, .env loading, or runtime traffic is involved. */
export function buildContainerTopology(projects: readonly ServiceProject[]): ContainerTopology {
    const warnings: string[] = [], unresolved: ServiceEvidence[] = [];
    const parsed = projects.flatMap(project => parseServices(project, warnings));
    const services = parsed.slice(0, MAX_SERVICES);
    if (parsed.length > MAX_SERVICES) warnings.push(`Only the first ${MAX_SERVICES} services across selected projects were analyzed.`);
    const files = new Map<string, FileSource>();
    let bytes = 0, limited = false;
    for (const project of projects) {
        const sourceByPath = new Map([...project.sources].map(([path, text]) => [absolute(project.rootPath, path), text]));
        for (const path of unique([...project.files, ...project.sources.keys()])) {
        if (/(?:^|\/)\.env(?:\.|$)/i.test(path)) continue;
        const abs = absolute(project.rootPath, path);
        const key = `${project.project}\0${abs}`;
        if (files.has(key)) continue;
        if (files.size >= MAX_FILES) { limited = true; continue; }
        const value = sourceByPath.get(abs);
        const text = value !== undefined && value.length <= MAX_SOURCE && bytes + value.length <= MAX_TOTAL_SOURCE ? value : undefined;
        if (text !== undefined) bytes += text.length;
        else if (value !== undefined) limited = true;
        const root = normal(project.rootPath);
        files.set(key, { project, path: inside(abs, root) ? abs.slice(root.length + 1) : normal(path), absolute: abs, text });
        }
    }
    if (limited) warnings.push('Source analysis reached its file or size limit; some call evidence is unavailable.');
    const fileList = [...files.values()];
    for (const service of services) {
        const context = service.view.buildContext;
        if (!context) continue;
        const selectedRoots = unique(projects.map(project => normal(project.rootPath)));
        const contained = selectedRoots.some(root => inside(context, root));
        if (!contained) { warnings.push(`Service ${service.view.name}: build context is outside selected indexed projects.`); continue; }
        if (!selectedRoots.includes(context)) service.prefixes = [context];
        else {
            const dockerfile = fileList.find(file => file.absolute === service.view.dockerfile)?.text;
            service.prefixes = dockerfile ? copyPrefixes(dockerfile, context) : [];
            if (!service.prefixes.length) warnings.push(`Service ${service.view.name}: root build context does not identify a specific source subtree.`);
        }
    }
    const owners = new Map<string, ParsedService>();
    let ambiguous = 0;
    for (const file of fileList) {
        const candidates = services.filter(service => service.prefixes.some(prefix => inside(file.absolute, prefix)));
        if (candidates.length === 1) {
            owners.set(`${file.project.project}\0${file.absolute}`, candidates[0]);
            candidates[0].view.sourcePaths.push(file.absolute);
        } else if (candidates.length > 1) ambiguous++;
    }
    if (ambiguous) warnings.push(`${ambiguous} files belong to overlapping service build contexts; their call evidence was omitted.`);
    const edges = new Map<string, ContainerConnection>();
    const evidence = (service: ParsedService, line: number, summary: string): ServiceEvidence => ({
        project: service.input.project, path: service.input.manifest, line, summary,
    });
    const add = (source: ParsedService, target: ParsedService, kind: ContainerConnection['kind'], protocol: string, proof: ServiceEvidence): void => {
        if (source === target) return;
        const id = `${source.view.id}->${target.view.id}:${kind}:${protocol}`;
        const existing = edges.get(id);
        if (existing) {
            if (existing.evidence.length < 12 && !existing.evidence.some(item => item.project === proof.project && item.path === proof.path && item.line === proof.line && item.summary === proof.summary)) existing.evidence.push(proof);
        } else if (edges.size < MAX_CONNECTIONS) {
            edges.set(id, { id, source: source.view.id, target: target.view.id, kind, protocol, evidence: [proof] });
        } else if (!warnings.includes('Connection analysis reached its 2048-link limit.')) {
            warnings.push('Connection analysis reached its 2048-link limit.');
        }
    };
    const connectHost = (source: ParsedService, endpoint: Endpoint, kind: 'call' | 'configuration', proof: ServiceEvidence): void => {
        if (['localhost', '127.0.0.1', '0.0.0.0'].includes(endpoint.host)) return;
        const linkTarget = list(field(source.node, 'links')).find(link => link.split(':')[1]?.toLowerCase() === endpoint.host)?.split(':')[0];
        const targets = services.filter(target => source.view.networks.some(network => target.aliases.get(network)?.includes(endpoint.host)
            || (target.input === source.input && target.view.name === linkTarget && target.view.networks.includes(network))));
        if (targets.length === 1) {
            const protocol = endpoint.protocol === 'tcp' ? protocolFor(targets[0].view.image ?? '') : endpoint.protocol;
            add(source, targets[0], kind, protocol, proof);
        } else unresolved.push({ ...proof, summary: targets.length ? `Ambiguous Docker DNS destination ${tidy(endpoint.host)}` : `No selected service on a shared network resolves ${tidy(endpoint.host)}` });
    };
    for (const service of services) {
        const depends = field(service.node, 'depends_on');
        const dependencies: [string, unknown][] = isSeq(depends) ? depends.items.flatMap(item => str(item) ? [[str(item)!, item]] : []) : entries(depends);
        for (const [name, node] of dependencies) {
            const target = services.find(candidate => candidate.input === service.input && candidate.view.name === name);
            const proof = evidence(service, lineOf(node, service.counter), `Declared startup dependency on ${tidy(name)}`);
            if (target) add(service, target, 'startup', 'startup', proof);
            else unresolved.push({ ...proof, summary: `Startup dependency ${tidy(name)} is not present in this manifest` });
        }
        for (const link of list(field(service.node, 'links'))) {
            const name = link.split(':')[0];
            const target = services.find(candidate => candidate.input === service.input && candidate.view.name === name);
            const proof = evidence(service, lineOf(field(service.node, 'links'), service.counter), `Declared service link to ${tidy(name)}`);
            if (target && service.view.networks.some(network => target.view.networks.includes(network))) add(service, target, 'configuration', 'tcp', proof);
            else unresolved.push(proof);
        }
        for (const [key, configured] of service.environment) {
            if (!ENDPOINT_KEY.test(key) || /^(?:HOSTNAME|BIND_|LISTEN_|ADVERTISE_)/i.test(key)) continue;
            if (dynamic(configured.value)) {
                unresolved.push(evidence(service, configured.line, `Endpoint ${tidy(key)} contains unresolved variables`));
                continue;
            }
            for (const endpoint of endpoints(configured.value, key, true)) connectHost(service, endpoint, 'configuration',
                evidence(service, configured.line, `${tidy(key)} configures ${endpoint.protocol} destination ${tidy(endpoint.host)}`));
        }
    }
    for (const file of fileList) {
        const service = owners.get(`${file.project.project}\0${file.absolute}`);
        if (!service || !file.text || !CODE_FILE.test(file.path) || /(?:^|\/)(?:test|tests|__tests__)(?:\/|$)/i.test(file.path)) continue;
        for (const candidate of codeEndpoints(file.text, service, file.path)) connectHost(service, candidate.endpoint, 'call', {
            project: file.project.project, path: file.path, line: candidate.line,
            summary: `${candidate.via} references ${candidate.endpoint.protocol} destination ${tidy(candidate.endpoint.host)}`,
        });
    }
    // Graph identities are project-scoped; paths must independently map to unambiguous service ownership.
    for (const project of projects) {
        if (project.routeGraph?.truncated) warnings.push(`${tidy(project.project)}: route graph evidence is incomplete.`);
        if (project.routeGraph?.warnings.length) warnings.push(`${tidy(project.project)}: route graph reported unavailable or limited evidence.`);
        for (const relationship of project.routeGraph?.relationships ?? []) {
            if (relationship.type === 'HANDLES') continue;
            const sourcePath = relationship.source.file_path;
            if (!sourcePath) continue;
            const source = owners.get(`${project.project}\0${absolute(project.rootPath, sourcePath)}`);
            if (!source) continue;
            const targetPath = relationship.target.file_path;
            const target = targetPath ? owners.get(`${project.project}\0${absolute(project.rootPath, targetPath)}`) : undefined;
            const proof: ServiceEvidence = { project: project.project, path: sourcePath,
                line: relationship.source.start_line ?? 1, summary: `Indexed ${relationship.type} relationship` };
            if (target && source.view.networks.some(network => target.view.networks.includes(network))) {
                add(source, target, 'call', relationship.type === 'HTTP_CALLS' ? 'http' : 'async', proof);
            } else {
                // Relative URL paths are not a service identity and are deliberately never joined.
                const hosts = endpoints(relationship.routePath ?? '', relationship.type === 'HTTP_CALLS' ? 'http' : '', false);
                for (const endpoint of hosts) connectHost(source, endpoint, 'call', proof);
            }
        }
    }
    for (const service of services) service.view.sourcePaths = unique(service.view.sourcePaths).sort();
    const seenUnresolved = new Set<string>();
    return { services: services.map(service => service.view), connections: [...edges.values()], warnings: unique(warnings),
        unresolved: unresolved.filter(item => { const key = JSON.stringify(item); if (seenUnresolved.has(key)) return false; seenUnresolved.add(key); return true; }).slice(0, 100) };
}
