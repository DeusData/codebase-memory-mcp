/// <reference types="vitest/config" />
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { defineConfig, type ProxyOptions } from 'vite';
import react from '@vitejs/plugin-react';

const projectRoot = dirname(fileURLToPath(import.meta.url));

/**
 * Der Versions-Chip der Kopfzeile, zur Buildzeit festgeschrieben.
 *
 * Die Zahl kommt aus package.json, das Suffix aus dem Arbeitsbaum: ist beim
 * Bauen etwas uncommitted, heisst der Build `-dirty`. Das ist die einzige
 * Angabe, mit der ein Screenshot spaeter einer Fassung zugeordnet werden kann,
 * und ohne sie wuerde ein Bild aus einem schmutzigen Baum aussehen wie eins aus
 * einem Commit.
 *
 * Wenn git nicht da ist oder das Verzeichnis kein Repository ist, faellt das
 * Suffix weg. Es wird nichts erfunden: kein git heisst keine Aussage ueber den
 * Arbeitsbaum, nicht "sauber".
 */
function atlasVersion(): string {
    const pkg = JSON.parse(readFileSync(join(projectRoot, 'package.json'), 'utf8')) as {
        version?: string;
    };
    const version = typeof pkg.version === 'string' && pkg.version.length > 0 ? pkg.version : '0.0.0';
    let dirty = '';
    try {
        const porcelain = execFileSync('git', ['status', '--porcelain'], {
            cwd: projectRoot,
            encoding: 'utf8',
            stdio: ['ignore', 'pipe', 'ignore'],
        });
        dirty = porcelain.trim().length > 0 ? '-dirty' : '';
    } catch {
        dirty = '';
    }
    return `v${version}${dirty}`;
}

/**
 * Dev-Betrieb nach dem Vorbild des PR-Frontends (cbm/graph-ui/vite.config.ts):
 * /rpc und /api werden auf den C-Server geproxyt, der nur seine eigene
 * Loopback-Authority akzeptiert. Fremde Browser-Origins werden vor dem
 * Rewrite abgewiesen, damit der Proxy kein Loch in den Origin-Check reisst.
 *
 * The dev server listens on 5173, the port this repository's dev-proxy
 * contract test (tests/test_ui_dev_proxy_security.sh) pins together with
 * the two literal origins below.
 */
const uiBackendOrigin = 'http://127.0.0.1:9749';
const devPort = 5173;
const uiDevOrigins = new Set([
  'http://127.0.0.1:5173',
  'http://localhost:5173',
]);

const uiBackendProxy = (): ProxyOptions => ({
  target: uiBackendOrigin,
  changeOrigin: true,
  headers: { Origin: uiBackendOrigin },
  bypass(req, res) {
    const origin = req.headers.origin;
    if (res !== undefined && origin !== undefined && !uiDevOrigins.has(origin)) {
      res.writeHead(403, { 'Content-Type': 'application/json' });
      res.end('{"error":"forbidden origin"}');
      return false;
    }
    return undefined;
  },
});

export default defineConfig({
  plugins: [react()],
  define: {
    __ATLAS_VERSION__: JSON.stringify(atlasVersion()),
  },
  build: {
    outDir: 'dist',
    assetsDir: 'assets',
    sourcemap: false,
    // Monaco bringt viel mit. Der Deckel wird angehoben statt uebergangen,
    // damit eine echte Ueberraschung spaeter wieder auffaellt.
    chunkSizeWarningLimit: 4096,
  },
  server: {
    port: devPort,
    strictPort: true,
    proxy: {
      '/rpc': uiBackendProxy(),
      '/api': uiBackendProxy(),
    },
  },
  test: {
    // Standard ist node: Parser, Transport und IR sind reine Logik. Nur die
    // Render-Tests schalten per Docblock auf jsdom um.
    environment: 'node',
    include: ['src/**/*.test.ts', 'src/**/*.test.tsx', 'tools/**/*.test.mjs'],
  },
});
