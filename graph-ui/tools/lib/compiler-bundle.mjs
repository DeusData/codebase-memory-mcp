/**
 * The context compiler, made importable from a Node tool.
 *
 * The eval has to build cards the way the product builds them, and the product
 * is TypeScript compiled by Vite for a browser. Three ways out of that existed
 * and two were rejected:
 *
 *  - **Reimplement the compiler in the tool.** Rejected: the eval would then be
 *    scoring cards no reader ever sees, and the first drift between the two
 *    would be invisible.
 *  - **Drive the eval through a browser.** Rejected: six models times forty
 *    questions is 240 requests, and putting Playwright between a measurement and
 *    the process it measures adds a second thing that can be slow.
 *  - **Bundle the compiler for Node with esbuild.** Taken. esbuild is already a
 *    dependency of Vite, so nothing is installed and nothing is fetched; the
 *    entry is src/compiler/eval-entry.ts, which re-exports the product's own
 *    modules and adds nothing.
 *
 * The bundle is written under .eval-build/ and rebuilt on every run, because a
 * cached bundle of a compiler somebody just edited is the exact failure this
 * whole arrangement exists to avoid.
 */

import { mkdir, rm } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');

/** Where the bundle lands. Gitignored, rebuilt per run. */
export const BUNDLE_DIR = join(ROOT, '.eval-build');
export const BUNDLE_FILE = join(BUNDLE_DIR, 'compiler.mjs');

/**
 * Build the bundle and import it.
 *
 * `platform: 'neutral'` with an ESM format keeps Node's globals (fetch, crypto)
 * as globals instead of shimming them, which is what the provider layer already
 * expects: it reads `globalThis.fetch` and `globalThis.crypto.subtle`, both of
 * which Node has had since 18.
 */
export async function loadCompiler() {
    const esbuild = await import('esbuild');
    await rm(BUNDLE_DIR, { recursive: true, force: true });
    await mkdir(BUNDLE_DIR, { recursive: true });
    const result = await esbuild.build({
        entryPoints: [join(ROOT, 'src', 'compiler', 'eval-entry.ts')],
        outfile: BUNDLE_FILE,
        bundle: true,
        format: 'esm',
        platform: 'neutral',
        target: 'node20',
        mainFields: ['module', 'main'],
        conditions: ['node', 'import'],
        logLevel: 'silent',
        metafile: true,
    });
    const inputs = Object.keys(result.metafile?.inputs ?? {}).length;
    /*
     * esbuild keeps a helper process alive after a build and Node will not exit
     * while it is there. Measured, not guessed: without this line the eval sits
     * at 100 percent done with an idle `esbuild --service --ping` beside it and
     * never returns. A tool that has finished must end.
     */
    await esbuild.stop();
    const module = await import(pathToFileURL(BUNDLE_FILE).href);
    return { module, inputs };
}
