/**
 * One llama-server process, started and stopped by a proof run.
 *
 * llm/start.sh is the product's way in and it binds 4141, the one port the
 * product uses. The eval runs six models one after another and must not touch
 * that port: a measurement that occupies the product's port makes the product
 * unusable while it runs, and worse, a crashed eval would leave a model loaded
 * on the port the UI polls. So the eval starts the same binary on its own port
 * (4400 and up, agreed for this cycle) and stops it before the next model.
 *
 * The context size is not a knob here. It comes from the model class, exactly as
 * in llm/start.sh: 3072 for the roughly 1B candidates and 8192 for the roughly
 * 4B ones, which are the two windows PLAN paragraph 5 budgets against.
 */

import { spawn } from 'node:child_process';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');

/** The binary and where it came from is documented in vendor/llama/HERKUNFT.md. */
export const LLAMA_SERVER = join(ROOT, 'vendor', 'llama', 'llama-server');

/** The lowest port the eval may use. The product port 4141 stays free. */
export const EVAL_PORT_FLOOR = 4400;

const sleep = (ms) => new Promise((done) => setTimeout(done, ms));

/**
 * Start one model and wait until it answers `{"status":"ok"}` on /health.
 *
 * Waiting on the health route rather than on a timer, for the reason the C
 * server helper states: a sleep is a guess about the machine this runs on, and a
 * 4B model on a cold page cache takes a lot longer than one that was just read.
 */
export async function startLlama({ modelFile, contextTokens, port, timeoutMs = 300000, log = [] }) {
    const started = Date.now();
    const child = spawn(LLAMA_SERVER, [
        '--host', '127.0.0.1',
        '--port', String(port),
        '-m', join(ROOT, 'models', modelFile),
        '-c', String(contextTokens),
    ], { cwd: ROOT, stdio: ['ignore', 'pipe', 'pipe'] });
    child.stdout.on('data', (d) => log.push(`[llama stdout] ${d.toString().trim()}`));
    child.stderr.on('data', (d) => log.push(`[llama stderr] ${d.toString().trim()}`));

    let exited = null;
    child.on('exit', (code, signal) => { exited = { code, signal }; });

    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        if (exited !== null) {
            throw new Error(
                `llama-server for ${modelFile} exited early (code=${exited.code}, signal=${exited.signal})\n`
                + log.slice(-15).join('\n'),
            );
        }
        try {
            const response = await fetch(`http://127.0.0.1:${port}/health`);
            await response.arrayBuffer();
            if (response.status === 200) {
                return { child, readyMs: Date.now() - started };
            }
        } catch {
            // not listening yet
        }
        await sleep(400);
    }
    child.kill('SIGKILL');
    throw new Error(`llama-server for ${modelFile} was not ready within ${timeoutMs} ms`);
}

/** Stop it, politely and then not. Nothing may survive one model's turn. */
export async function stopLlama(child) {
    if (!child || child.exitCode !== null || child.killed) {
        return;
    }
    child.kill('SIGTERM');
    for (let i = 0; i < 100; i += 1) {
        if (child.exitCode !== null || child.signalCode !== null) {
            return;
        }
        await sleep(100);
    }
    child.kill('SIGKILL');
    await sleep(500);
}

/** What the running process says it loaded. Read, never assumed from the file name. */
export async function llamaProps(port) {
    try {
        const response = await fetch(`http://127.0.0.1:${port}/props`);
        if (!response.ok) {
            return {};
        }
        const payload = await response.json();
        return {
            modelPath: payload?.model_path ?? '',
            quantization: payload?.model_ftype ?? '',
            contextTokens: payload?.default_generation_settings?.n_ctx,
            slots: payload?.total_slots,
        };
    } catch {
        return {};
    }
}
