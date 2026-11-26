import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, readdirSync, realpathSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { isAbsolute, join, relative, resolve } from 'node:path';
import type { Plugin } from 'vite';

/** Evidence for the narrow browser-only license decision. Never shipped in dist. */
export function licenseBuildInventory(root: string): { app: Plugin; workers: () => Plugin[] } {
  const proofPath = join(root, '.vite/license-build.json');
  const modules = new Set<string>();
  const externals = new Set<string>();
  const chunks = new Set<string>();
  const generatedAssets: Record<string, string> = {};
  const inputs: Record<string, string> = {};
  let workerBuilds = 0;
  let failed = false;
  let outDir = join(root, 'dist');
  const hash = (path: string) => createHash('sha256').update(readFileSync(path)).digest('hex');
  const slash = (path: string) => path.replaceAll('\\', '/');
  const inside = (base: string, path: string) => {
    const rel = relative(base, path);
    return rel !== '..' && !rel.startsWith(`..${process.platform === 'win32' ? '\\' : '/'}`) && !isAbsolute(rel);
  };
  function localPath(path: string): string {
    const actual = realpathSync(path);
    const dependencies = realpathSync(join(root, 'node_modules'));
    if (inside(dependencies, actual)) return `node_modules/${slash(relative(dependencies, actual))}`;
    if (inside(realpathSync(root), actual)) return slash(relative(realpathSync(root), actual));
    throw new Error(`License inventory input outside the UI: ${path}`);
  }
  function record(path: string): string | undefined {
    const clean = path.split('?')[0];
    if (!clean || clean.includes('\0') || !isAbsolute(clean) || !existsSync(clean) || !statSync(clean).isFile()) return;
    const name = localPath(clean);
    inputs[name] = hash(clean);
    return name;
  }
  function files(dir: string): string[] {
    return readdirSync(dir, { withFileTypes: true }).flatMap(entry => {
      const path = join(dir, entry.name);
      return entry.isDirectory() ? files(path) : [path];
    });
  }
  const plugin = (worker: boolean): Plugin => ({
    name: `cbm-license-inventory-${worker ? 'worker' : 'app'}`,
    apply: 'build',
    configResolved(config) {
      if (!worker) outDir = resolve(config.root, config.build.outDir);
    },
    buildStart() {
      if (worker) { workerBuilds++; return; }
      rmSync(proofPath, { force: true });
      modules.clear(); externals.clear(); chunks.clear();
      for (const key of Object.keys(inputs)) delete inputs[key];
      for (const key of Object.keys(generatedAssets)) delete generatedAssets[key];
      workerBuilds = 0;
      failed = false;
      for (const file of ['vite.config.ts', 'tools/license-build-inventory.ts', 'package.json',
        'node_modules/@huggingface/transformers/package.json']) record(join(root, file));
    },
    buildEnd(error) { if (error) failed = true; },
    generateBundle: { order: 'post', handler(_options, bundle) {
      for (const id of this.getModuleIds()) {
        const info = this.getModuleInfo(id);
        if (info?.isExternal) externals.add(id);
        else modules.add(record(id) ?? id);
      }
      for (const path of this.getWatchFiles()) record(path);
      for (const [name, output] of Object.entries(bundle)) {
        if (output.type === 'chunk') chunks.add(name);
        else if (name === 'index.html' || name.endsWith('.css')) {
          // Only bundler outputs qualify. Files copied from public never
          // enter this hook; record bytes before inspecting the dist tree.
          generatedAssets[name] = createHash('sha256').update(output.source).digest('hex');
        }
      }
    } },
    closeBundle() {
      if (worker || failed || !existsSync(outDir)) return;
      const outputs: Record<string, string> = {};
      const runtimeInputs: Record<string, string> = {};
      // Vite emits ONNX's runtime through new URL(..., import.meta.url). Match
      // bytes to its installed input, rather than trusting an output filename.
      const ortDir = join(root, 'node_modules/onnxruntime-web/dist');
      const runtimeSources = existsSync(ortDir) ? files(ortDir).filter(path => /\.(wasm|m?js)$/.test(path)) : [];
      const runtimeHashes = new Map(runtimeSources.map(path => [hash(path), path]));
      for (const path of files(outDir).sort()) {
        const name = slash(relative(outDir, path));
        outputs[name] = hash(path);
        if (/\.(wasm|m?js)$/.test(name)) {
          const source = runtimeHashes.get(outputs[name]);
          if (source) runtimeInputs[name] = record(source)!;
        }
      }
      // Public copies are not Rollup modules. Record their inputs, while the
      // verifier still rejects copied executable payloads outside the graph.
      const publicDir = join(root, 'public');
      if (existsSync(publicDir)) for (const path of files(publicDir)) record(path);
      mkdirSync(join(root, '.vite'), { recursive: true });
      writeFileSync(proofPath, JSON.stringify({
        version: 1,
        lock_sha256: hash(join(root, 'package-lock.json')),
        inputs: Object.fromEntries(Object.entries(inputs).sort()),
        modules: [...modules].sort(), externals: [...externals].sort(),
        chunks: [...chunks].sort(), worker_builds: workerBuilds,
        outputs, runtime_inputs: runtimeInputs, generated_assets: generatedAssets,
      }, null, 2) + '\n');
    },
  });
  return { app: plugin(false), workers: () => [plugin(true)] };
}
