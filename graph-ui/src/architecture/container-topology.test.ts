import { describe, expect, it } from 'vitest';
import { buildContainerTopology, normalizeSourcePath } from './container-topology';
import type { ContainerTopology, ServiceProject } from './container-topology';
import type { RouteGraphSnapshot } from './route-graph-source';

function project(compose: string, sources: Record<string, string> = {}, overrides: Partial<ServiceProject> = {}): ServiceProject {
    return { project: 'sample', rootPath: '/repos/sample', manifest: 'compose.yaml',
        files: Object.keys(sources), sources: new Map(Object.entries({ 'compose.yaml': compose, ...sources })), ...overrides };
}
function links(topology: ContainerTopology, kind: 'call' | 'configuration' | 'startup' = 'call'): string[] {
    const names = new Map(topology.services.map(service => [service.id, service.name]));
    return topology.connections.filter(edge => edge.kind === kind).map(edge => `${names.get(edge.source)}>${names.get(edge.target)}:${edge.protocol}`).sort();
}

const voting = `services:
  vote:
    build: ./vote
    networks: [front, back]
    depends_on: [redis]
  result:
    build: ./result
    networks: [front, back]
  worker:
    build: ./worker
    networks: [back]
  redis:
    image: redis:alpine
    networks: [back]
  db:
    image: postgres:15
    networks: [back]
    environment:
      POSTGRES_PASSWORD: top-secret
  seed:
    build: ./seed
    profiles: [seed]
    networks: [front]
networks:
  front:
  back:
`;
const votingSources = {
    'vote/app.py': 'from redis import Redis\nclient = Redis(host="redis", db=0)\n',
    'result/server.js': "const pool = new Pool({\n  connectionString: 'postgres://user:credential@db/votes'\n});\n",
    'worker/Program.cs': `class Worker {
  static void Main() {
    var db = OpenDatabase("Server=db;Username=postgres;Password=credential;");
    var cache = OpenCache("redis");
  }
  static NpgsqlConnection OpenDatabase(string address) {
    var connection = new NpgsqlConnection(address);
    connection.Open();
    return connection;
  }
  static ConnectionMultiplexer OpenCache(string hostname) {
    var address = GetIp(hostname);
    return ConnectionMultiplexer.Connect(address);
  }
}`,
};

describe('Compose container topology', () => {
    it('recognizes qualified transport constructors without matching unrelated methods', () => {
        const result = buildContainerTopology([project(voting, {
            'vote/app.py': 'import redis\nclient = redis.Redis(host="redis")\n',
            'result/server.js': 'const pool = new pg.Pool({connectionString: "postgres://db/votes"});\nother.Pool("redis");',
        })]);
        expect(links(result)).toEqual(['result>db:postgres', 'vote>redis:redis']);
    });
    it('finds the four source connections in a voting microservice shape, retaining isolated/profiled declarations', () => {
        const result = buildContainerTopology([project(voting, votingSources)]);
        expect(result.services).toHaveLength(6);
        expect(links(result)).toEqual(['result>db:postgres', 'vote>redis:redis', 'worker>db:postgres', 'worker>redis:redis']);
        expect(links(result, 'startup')).toEqual(['vote>redis:startup']);
        expect(result.services.find(service => service.name === 'seed')?.profiles).toEqual(['seed']);
        expect(result.services.find(service => service.name === 'redis')?.sourcePaths).toEqual([]);
        const vote = result.connections.find(edge => edge.kind === 'call' && edge.protocol === 'redis'
            && result.services.find(service => service.id === edge.source)?.name === 'vote');
        expect(vote?.evidence[0]).toMatchObject({ project: 'sample', path: 'vote/app.py', line: 2 });
        expect(JSON.stringify(result)).not.toMatch(/credential|top-secret|Username|Password/);
        expect(result.services.find(service => service.name === 'worker')?.sourcePaths).toEqual(['/repos/sample/worker/Program.cs']);
    });

    it('never derives network traffic from a shared network, comments, log strings or route names', () => {
        const result = buildContainerTopology([project(voting, {
            'vote/app.py': '# Redis(host="redis")\nprint("Redis(host=redis)")\nlabel = "redis"\n',
            'worker/worker.js': 'console.log("http://db/users");\napp.get("/users", handler);\n',
        })]);
        expect(links(result)).toEqual([]);
        expect(result.services).toHaveLength(6);
    });

    it('scopes default DNS to each Compose manifest and project despite duplicate service names', () => {
        const compose = 'services:\n  web:\n    build: ./web\n    environment: {REDIS_HOST: redis}\n  redis:\n    image: redis:7\n';
        const one = project(compose, {}, { project: 'one', rootPath: '/repos/one' });
        const two = project(compose, {}, { project: 'two', rootPath: '/repos/two' });
        const result = buildContainerTopology([one, two]);
        expect(new Set(result.services.map(service => service.id)).size).toBe(4);
        expect(result.connections).toHaveLength(2);
        expect(result.connections.every(edge => result.services.find(service => service.id === edge.source)?.project
            === result.services.find(service => service.id === edge.target)?.project)).toBe(true);
        expect(result.unresolved).toEqual([]);
    });

    it('connects selected repositories only when the hostname is unique on an explicitly shared network', () => {
        const caller = project('services:\n  web:\n    build: ./web\n    networks: [mesh]\nnetworks:\n  mesh: {external: true, name: shared-mesh}\n', {
            'web/client.ts': 'fetch("http://api:8080/items");',
        }, { project: 'front', rootPath: '/repos/front' });
        const callee = project('services:\n  api:\n    image: example/api\n    networks:\n      mesh: {aliases: [catalog]}\nnetworks:\n  mesh: {external: true, name: shared-mesh}\n', {}, { project: 'back', rootPath: '/repos/back' });
        const result = buildContainerTopology([caller, callee]);
        expect(links(result)).toEqual(['web>api:http']);
        expect(result.connections[0].evidence[0]).toMatchObject({ project: 'front', path: 'web/client.ts', line: 1 });
        const collision = project('services:\n  api:\n    image: example/other\n    networks: [mesh]\nnetworks:\n  mesh: {external: true, name: shared-mesh}\n', {}, { project: 'other', rootPath: '/repos/other' });
        const ambiguous = buildContainerTopology([caller, callee, collision]);
        expect(links(ambiguous)).toEqual([]);
        expect(ambiguous.unresolved[0].summary).toContain('Ambiguous');
    });

    it('does not connect identical hostnames across separate networks or treat depends_on as a call', () => {
        const result = buildContainerTopology([project('services:\n  web:\n    build: ./web\n    networks: [front]\n    depends_on: [db]\n  db:\n    image: postgres\n    networks: [back]\nnetworks:\n  front:\n  back:\n', {
            'web/client.ts': 'fetch("http://db/query");',
        })]);
        expect(links(result)).toEqual([]);
        expect(links(result, 'startup')).toEqual(['web>db:startup']);
        expect(result.unresolved[0].summary).toContain('shared network');
    });

    it('resolves endpoint configuration separately and never expands variables or consumes secrets/.env', () => {
        const result = buildContainerTopology([project('services:\n  web:\n    build: ./web\n    environment:\n      - REDIS_HOST=redis\n      - DATABASE_URL=${DB_URL}\n      - ACCESS_TOKEN=http://db/secret\n    env_file: .env\n  redis: {image: redis}\n  db: {image: postgres}\n', {
            'web/client.ts': 'fetch(process.env.DATABASE_URL);\nredis.connect(process.env.REDIS_HOST);',
            '.env': 'DB_URL=http://db/private\n',
        })]);
        expect(links(result, 'configuration')).toEqual(['web>redis:redis']);
        expect(links(result)).toEqual(['web>redis:redis']);
        expect(result.unresolved.some(item => item.summary.includes('unresolved variables'))).toBe(true);
        expect(result.warnings.join(' ')).toContain('env_file');
        expect(JSON.stringify(result)).not.toContain('http://db/secret');
        expect(JSON.stringify(result)).not.toContain('private');
    });

    it('supports long port declarations and source-local links aliases without leaking an alias to unrelated services', () => {
        const result = buildContainerTopology([project('services:\n  web:\n    build: ./web\n    links: ["db:storage"]\n    ports:\n      - target: 8080\n        published: "80"\n        protocol: tcp\n  other:\n    build: ./other\n  db: {image: postgres}\n', {
            'web/client.py': 'psycopg.connect("host=storage")\npsycopg.connect("postgres://storage/data")',
            'other/client.py': 'psycopg.connect("postgres://storage/data")',
        })]);
        expect(links(result)).toEqual(['web>db:postgres']);
        expect(result.services.find(service => service.name === 'web')?.ports).toEqual(['80:8080/tcp']);
        expect(result.unresolved.some(item => item.path === 'other/client.py')).toBe(true);
    });

    it('uses selected sibling source roots for ../ builds without requiring a sibling manifest', () => {
        const owner = project('services:\n  api: {build: ../backend}\n  db: {image: postgres}\n');
        const sourceOnly = project('', { 'src/index.ts': 'fetch("http://db/items")', Dockerfile: 'FROM node\nCOPY src ./src\n' }, {
            project: 'backend', rootPath: '/repos/backend', manifest: '',
        });
        const result = buildContainerTopology([owner, sourceOnly]);
        expect(result.services).toHaveLength(2);
        expect(result.services[0].sourcePaths).toContain('/repos/backend/src/index.ts');
        expect(links(result)).toEqual(['api>db:http']);
        expect(result.connections[0].evidence[0]).toMatchObject({ project: 'backend', path: 'src/index.ts' });
        expect(result.warnings).toEqual([]);
    });

    it('refines root contexts with explicit COPY sources while excluding shared ownership and multi-stage copies', () => {
        const compose = 'services:\n  api:\n    build: {context: ., dockerfile: api.Dockerfile}\n  worker:\n    build: {context: ., dockerfile: worker.Dockerfile}\n  db: {image: postgres}\n';
        const result = buildContainerTopology([project(compose, {
            'api.Dockerfile': 'FROM node\nCOPY ["api", "/app"]\nCOPY shared /shared\nCOPY --from=build /generated /app/generated\n',
            'worker.Dockerfile': 'FROM node AS build\nCOPY worker /app\nCOPY shared /shared\nFROM node\nCOPY --from=build /app /app\n',
            'api/a.ts': 'fetch("http://db/a")', 'worker/w.ts': 'fetch("http://db/w")',
            'shared/s.ts': 'fetch("http://db/shared")', 'unowned/u.ts': 'fetch("http://db/no")',
        })]);
        expect(links(result)).toEqual(['api>db:http', 'worker>db:http']);
        expect(result.services).toHaveLength(3);
        expect(result.services.flatMap(service => service.sourcePaths)).not.toContain('/repos/sample/shared/s.ts');
        expect(result.warnings.join(' ')).toContain('overlapping');
        const noEvidence = buildContainerTopology([project('services:\n  api: {build: .}\n  db: {image: postgres}\n', {
            Dockerfile: 'FROM node\nCOPY . /app\n', 'client.ts': 'fetch("http://db/no")',
        })]);
        expect(links(noEvidence)).toEqual([]);
        expect(noEvidence.warnings.join(' ')).toContain('root build context');
    });

    it.each([
        ['duplicate keys', 'services:\n  api: {image: one}\n  api: {image: two}\n'],
        ['aliases', 'x-base: &base {image: example}\nservices:\n  api: *base\n'],
        ['merge', 'services:\n  api:\n    <<: {image: example}\n'],
        ['include', 'include: other.yaml\nservices:\n  api: {image: example}\n'],
        ['invalid syntax', 'services: [unclosed'],
        ['custom tags', 'services:\n  api: !custom value\n'],
        ['size', 'services:\n' + '#'.repeat(256 * 1024)],
        ['depth', 'services:\n  api:\n    x: ' + '['.repeat(45) + 'x' + ']'.repeat(45)],
    ])('fails closed with a visible warning for %s', (_name, manifest) => {
        const result = buildContainerTopology([project(manifest)]);
        expect(result.services).toEqual([]);
        expect(result.connections).toEqual([]);
        expect(result.warnings.length).toBeGreaterThan(0);
    });

    it('omits an unsupported extended service instead of guessing inherited settings', () => {
        const result = buildContainerTopology([project('services:\n  api:\n    extends: {file: other.yaml, service: base}\n  isolated: {image: busybox}\n')]);
        expect(result.services.map(service => service.name)).toEqual(['isolated']);
        expect(result.warnings.join(' ')).toContain('extends');
    });

    it('keeps graph IDs project-scoped and refuses to join relative route paths', () => {
        const node = (id: number, path: string) => ({ id, name: '/items', label: 'Function', file_path: path,
            start_line: 4, x: 0, y: 0, z: 0, size: 1, color: '' });
        const routeGraph: RouteGraphSnapshot = { truncated: false, warnings: [], relationships: [{
            source: node(1, 'web/client.ts'), target: node(2, ''), type: 'HTTP_CALLS', routePath: '/items',
        }] };
        const result = buildContainerTopology([
            project('services:\n  web: {build: ./web}\n', { 'web/client.ts': 'const x = 1;' }, { routeGraph }),
            project('services:\n  api: {build: ./api}\n', { 'api/server.ts': 'app.get("/items", handler)' }, { project: 'other', rootPath: '/repos/other',
                routeGraph: { truncated: false, warnings: [], relationships: [{ source: node(2, ''), target: node(1, 'api/server.ts'), type: 'HANDLES', routePath: '/items' }] } }),
        ]);
        expect(result.connections).toEqual([]);
    });

    it('bounds service declarations and preserves warnings for missing/oversized source evidence', () => {
        const declarations = Array.from({ length: 130 }, (_, index) => `  service${index}: {image: example}`).join('\n');
        const result = buildContainerTopology([project(`services:\n${declarations}\n`)]);
        expect(result.services).toHaveLength(128);
        expect(result.warnings.join(' ')).toContain('128');
        const oversized = buildContainerTopology([project('services:\n  api: {build: ./api}\n', { 'api/index.ts': 'x'.repeat(256 * 1024 + 1) })]);
        expect(oversized.warnings.join(' ')).toContain('size limit');
    });

    it('normalizes source identities without filesystem access', () => {
        expect(normalizeSourcePath('/repos/front', '../back/src/../main.ts')).toBe('/repos/back/main.ts');
        expect(normalizeSourcePath('/repos/front', '/repos/back/main.ts')).toBe('/repos/back/main.ts');
        expect(normalizeSourcePath('C:/repos/front', '../back/main.ts')).toBe('C:/repos/back/main.ts');
    });
});

describe('Spring service connection evidence', () => {
    const compose = 'services:\n  gateway: {build: ./gateway}\n  catalog: {image: example/catalog}\n  payment: {image: example/payment}\n';
    const model = (java: string, extra: Record<string, string> = {}) => buildContainerTopology([project(compose, {
        'gateway/src/main/java/Client.java': java, ...extra,
    })]);

    it('resolves typed RestTemplate URLs from literal concatenation and local URI bindings', () => {
        const result = model(`class Client {
  private RestTemplate transport;
  Response load(String id) {
    final String host = "catalog";
    String address = "http://" + host + ":8080/api/items/" + id;
    URI endpoint = URI.create(address);
    return transport.exchange(endpoint, HttpMethod.GET, null, Response.class);
  }
  void save(String id) {
    transport.postForEntity("http://payment:8081/api/pay/" + id, body, Response.class);
  }
}`);
        expect(links(result)).toEqual(['gateway>catalog:http', 'gateway>payment:http']);
        expect(result.connections.find(edge => edge.target.endsWith(':catalog'))?.evidence[0]).toMatchObject({
            path: 'gateway/src/main/java/Client.java', line: 7,
        });
    });

    it('substitutes literal arguments into a simple local string helper without repository-specific rules', () => {
        const result = model(`class Client {
  private RestTemplate transport;
  private String endpointFor(String host) { return "http://" + host; }
  Object load(String id) {
    String address = endpointFor("catalog");
    return transport.exchange(address + "/items/" + id, HttpMethod.GET, null, Object.class);
  }
  Object pay() {
    String address = endpointFor("payment");
    return transport.exchange(address + "/pay", HttpMethod.POST, null, Object.class);
  }
}`);
        expect(links(result)).toEqual(['gateway>catalog:http', 'gateway>payment:http']);
    });

    it('follows renamed RestOperations clients and same-scope sequential string assignments', () => {
        const result = model(`class Client {
  private RestOperations api;
  String load() {
    String endpoint = "http://catalog:8080";
    endpoint = endpoint + "/items";
    return api.getForObject(endpoint, String.class);
  }
}`);
        expect(links(result)).toEqual(['gateway>catalog:http']);
    });

    it('recognizes typed WebClient requests with explicit URLs and a statically configured base URL', () => {
        const result = model(`class Client {
  private WebClient catalogClient = WebClient.create("http://catalog:8080");
  private WebClient paymentClient;
  Object load(String id) {
    return catalogClient.get().uri("/items/" + id).retrieve().bodyToMono(String.class);
  }
  Object pay() {
    String url = "https://payment:8443/pay";
    return paymentClient.post().uri(url).retrieve().bodyToMono(String.class);
  }
}`);
        expect(links(result)).toEqual(['gateway>catalog:http', 'gateway>payment:https']);
    });

    it('does not borrow same-named locals from another method or guess a dynamic hostname', () => {
        const result = model(`class Client {
  private RestTemplate transport;
  void unused() { String address = "http://catalog:8080/items"; }
  Object dynamic(String hostname) {
    String address = "http://" + hostname + "/items";
    return transport.getForObject(address, String.class);
  }
  Object unrelated() { return transport.getForObject(address, String.class); }
}`);
        expect(links(result)).toEqual([]);
    });

    it('rejects conditional endpoint reassignment and method parameters shadowing a constant', () => {
        const result = model(`class Client {
  private RestTemplate transport;
  private static final String address = "http://catalog:8080/items";
  Object shadowed(String address) { return transport.getForObject(address, String.class); }
  Object conditional(boolean change) {
    String url = "http://catalog:8080/items";
    if (change) { url = "http://payment:8080/pay"; }
    return transport.getForObject(url, String.class);
  }
}`);
        expect(links(result)).toEqual([]);
    });

    it('ignores comments, logging, arbitrary exchange methods, client construction alone and test-only sources', () => {
        const result = model(`class Client {
  private RestTemplate transport;
  private OtherType fake;
  void observe() {
    // transport.getForObject("http://catalog", String.class);
    log.info("http://catalog:8080/items");
    fake.exchange("http://payment:8080/pay");
    WebClient setup = WebClient.create("http://catalog:8080");
  }
}`, {
            'gateway/src/test/java/ClientTest.java': 'class ClientTest { RestTemplate transport; void test() { transport.getForObject("http://payment", String.class); } }',
        });
        expect(links(result)).toEqual([]);
    });
});
